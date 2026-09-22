//! Document / messenger transports: Yandex Docs, Mail Docs, Max, OneME.
//!
//! These services expose a collaborative document (or chat) that syncs small
//! blobs between participants. `OpenFlux-core` turns that sync channel into a
//! byte pipe. We keep the same idea: frames are chunked into service-sized
//! payloads and shipped through a `DocumentChannel`.

use std::collections::VecDeque;

use async_trait::async_trait;

use crate::config::{Config, TransportKind};
use crate::error::{Error, Result};
use crate::transport::{Frame, Transport};
use crate::transports::{decode_frame, encode_frame};

/// Maximum payload we hand to a document service in one sync op.
pub const CHUNK: usize = 32 * 1024;

/// A collaborative channel: append and read opaque blobs.
#[async_trait]
pub trait DocumentChannel: Send + Sync {
    /// Open / attach to a document by id.
    async fn open(&mut self, doc: &str) -> Result<()>;
    /// Append a blob to the document.
    async fn append(&mut self, chunk: &[u8]) -> Result<()>;
    /// Read the next blob, if the peer appended one.
    async fn read(&mut self) -> Result<Option<Vec<u8>>>;
    /// Close the document.
    async fn close(&mut self) -> Result<()>;
}

/// Generic document transport parameterised by its channel.
pub struct DocumentTransport<C: DocumentChannel> {
    kind: TransportKind,
    doc: String,
    channel: C,
    connected: bool,
    pending: VecDeque<Frame>,
    reassembly: Vec<u8>,
    expect: Option<usize>,
}

impl<C: DocumentChannel> DocumentTransport<C> {
    pub fn new(kind: TransportKind, cfg: &Config, channel: C) -> Self {
        Self {
            kind,
            doc: cfg.room.clone().unwrap_or_else(|| "tots-doc".to_string()),
            channel,
            connected: false,
            pending: VecDeque::new(),
            reassembly: Vec::new(),
            expect: None,
        }
    }
}

#[async_trait]
impl<C: DocumentChannel> Transport for DocumentTransport<C> {
    fn kind(&self) -> TransportKind {
        self.kind
    }

    async fn connect(&mut self) -> Result<()> {
        if self.connected {
            return Ok(());
        }
        self.channel.open(&self.doc).await?;
        log::info!("[{}] opened document {}", self.kind.id(), self.doc);
        self.connected = true;
        Ok(())
    }

    async fn send(&mut self, frame: &Frame) -> Result<()> {
        let encoded = encode_frame(frame.seq, &frame.data);
        for chunk in encoded.chunks(CHUNK) {
            self.channel.append(chunk).await?;
        }
        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<Frame>> {
        if let Some(f) = self.pending.pop_front() {
            return Ok(Some(f));
        }
        while let Some(chunk) = self.channel.read().await? {
            self.reassembly.extend_from_slice(&chunk);
            // Try to decode a complete frame from the reassembly buffer.
            if self.reassembly.len() >= 8 {
                let len = u32::from_be_bytes([
                    self.reassembly[4],
                    self.reassembly[5],
                    self.reassembly[6],
                    self.reassembly[7],
                ]) as usize;
                if self.reassembly.len() >= 8 + len {
                    let buf = self.reassembly.split_off(8 + len);
                    let frame_buf = std::mem::replace(&mut self.reassembly, buf);
                    let (seq, data) = decode_frame(&frame_buf, self.kind)?;
                    return Ok(Some(Frame::new(data, seq)));
                }
            }
        }
        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        if self.connected {
            self.channel.close().await?;
            self.connected = false;
        }
        Ok(())
    }
}

/// Yandex Documents transport, cf. `OpenFlux-core`.
pub struct YandexDocsTransport(DocumentTransport<LoopbackChannel>);

/// Mail.ru Documents transport, cf. `OpenFlux-core`.
pub struct MailDocsTransport(DocumentTransport<LoopbackChannel>);

/// MAX messenger transport, cf. `OpenFlux-core`.
pub struct MaxTransport(DocumentTransport<LoopbackChannel>);

/// OneME messenger transport, cf. `OpenFlux-core`.
pub struct OneMeTransport(DocumentTransport<LoopbackChannel>);

impl YandexDocsTransport {
    pub fn new(cfg: Config) -> Self {
        Self(DocumentTransport::new(
            TransportKind::YandexDocs,
            &cfg,
            LoopbackChannel::default(),
        ))
    }
}

impl MailDocsTransport {
    pub fn new(cfg: Config) -> Self {
        Self(DocumentTransport::new(
            TransportKind::MailDocs,
            &cfg,
            LoopbackChannel::default(),
        ))
    }
}

impl MaxTransport {
    pub fn new(cfg: Config) -> Self {
        Self(DocumentTransport::new(
            TransportKind::Max,
            &cfg,
            LoopbackChannel::default(),
        ))
    }
}

impl OneMeTransport {
    pub fn new(cfg: Config) -> Self {
        Self(DocumentTransport::new(
            TransportKind::OneMe,
            &cfg,
            LoopbackChannel::default(),
        ))
    }
}

macro_rules! delegate_doc {
    ($ty:ty) => {
        #[async_trait]
        impl Transport for $ty {
            fn kind(&self) -> TransportKind {
                self.0.kind()
            }
            async fn connect(&mut self) -> Result<()> {
                self.0.connect().await
            }
            async fn send(&mut self, frame: &Frame) -> Result<()> {
                self.0.send(frame).await
            }
            async fn recv(&mut self) -> Result<Option<Frame>> {
                self.0.recv().await
            }
            async fn close(&mut self) -> Result<()> {
                self.0.close().await
            }
        }
    };
}

delegate_doc!(YandexDocsTransport);
delegate_doc!(MailDocsTransport);
delegate_doc!(MaxTransport);
delegate_doc!(OneMeTransport);

/// In-process channel used until real service clients are wired in.
#[derive(Default)]
pub struct LoopbackChannel {
    log: VecDeque<Vec<u8>>,
}

#[async_trait]
impl DocumentChannel for LoopbackChannel {
    async fn open(&mut self, doc: &str) -> Result<()> {
        if doc.is_empty() {
            return Err(Error::Config("empty document id".into()));
        }
        Ok(())
    }

    async fn append(&mut self, chunk: &[u8]) -> Result<()> {
        self.log.push_back(chunk.to_vec());
        Ok(())
    }

    async fn read(&mut self) -> Result<Option<Vec<u8>>> {
        Ok(self.log.pop_front())
    }

    async fn close(&mut self) -> Result<()> {
        self.log.clear();
        Ok(())
    }
}