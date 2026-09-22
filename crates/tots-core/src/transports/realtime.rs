//! Realtime transports: VK, Yandex Telemost, VK Stream.
//!
//! All three expose the same shape: a signalling HTTP call creates a
//! conference / broadcast, then media and data flow over a WebRTC session with
//! TURN relays for NAT traversal. That is exactly the machinery used by
//! `free-turn-proxy-core` (VK/TURN) and `olcrtc-core` (Telemost, VK Stream).
//!
//! Here we keep the state machine and framing, and expose the network side
//! behind a `Signaller` trait so the transport can be driven by a real client
//! or by an in-process loopback in tests.

use std::collections::VecDeque;

use async_trait::async_trait;

use crate::config::{Config, TransportKind};
use crate::error::{Error, Result};
use crate::transport::{Frame, Transport};
use crate::transports::{decode_frame, encode_frame};

/// Signalling backend: creates a session and pumps opaque frames.
#[async_trait]
pub trait Signaller: Send + Sync {
    /// Join / create the room and return the local session id.
    async fn join(&mut self, room: &str) -> Result<String>;
    /// Push one encoded frame to the peer.
    async fn push(&mut self, encoded: &[u8]) -> Result<()>;
    /// Poll for a frame from the peer.
    async fn poll(&mut self) -> Result<Option<Vec<u8>>>;
    /// Leave the room.
    async fn leave(&mut self) -> Result<()>;
}

/// Generic realtime transport parameterised by its signaller.
pub struct RealtimeTransport<S: Signaller> {
    kind: TransportKind,
    room: String,
    signaller: S,
    connected: bool,
    next_seq: u64,
    inbox: VecDeque<Frame>,
}

impl<S: Signaller> RealtimeTransport<S> {
    pub fn new(kind: TransportKind, cfg: &Config, signaller: S) -> Self {
        Self {
            kind,
            room: cfg.room.clone().unwrap_or_else(|| "tots".to_string()),
            signaller,
            connected: false,
            next_seq: 1,
            inbox: VecDeque::new(),
        }
    }
}

#[async_trait]
impl<S: Signaller> Transport for RealtimeTransport<S> {
    fn kind(&self) -> TransportKind {
        self.kind
    }

    async fn connect(&mut self) -> Result<()> {
        if self.connected {
            return Ok(());
        }
        let session = self.signaller.join(&self.room).await?;
        log::info!("[{}] joined room {} as {}", self.kind.id(), self.room, session);
        self.connected = true;
        Ok(())
    }

    async fn send(&mut self, frame: &Frame) -> Result<()> {
        let encoded = encode_frame(frame.seq, &frame.data);
        self.signaller.push(&encoded).await?;
        self.next_seq = self.next_seq.max(frame.seq + 1);
        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<Frame>> {
        if let Some(f) = self.inbox.pop_front() {
            return Ok(Some(f));
        }
        let Some(raw) = self.signaller.poll().await? else {
            return Ok(None);
        };
        let (seq, data) = decode_frame(&raw, self.kind)?;
        Ok(Some(Frame::new(data, seq)))
    }

    async fn close(&mut self) -> Result<()> {
        if self.connected {
            self.signaller.leave().await?;
            self.connected = false;
        }
        Ok(())
    }
}

/// VK calls transport (WebRTC + TURN), cf. `free-turn-proxy-core`.
pub struct VkTransport(RealtimeTransport<LoopbackSignaller>);

/// Yandex Telemost transport, cf. `olcrtc-core`.
pub struct TelemostTransport(RealtimeTransport<LoopbackSignaller>);

/// VK Stream transport, cf. `olcrtc-core`.
pub struct VkStreamTransport(RealtimeTransport<LoopbackSignaller>);

impl VkTransport {
    pub fn new(cfg: Config) -> Self {
        Self(RealtimeTransport::new(
            TransportKind::Vk,
            &cfg,
            LoopbackSignaller::default(),
        ))
    }
}

impl TelemostTransport {
    pub fn new(cfg: Config) -> Self {
        Self(RealtimeTransport::new(
            TransportKind::YandexTelemost,
            &cfg,
            LoopbackSignaller::default(),
        ))
    }
}

impl VkStreamTransport {
    pub fn new(cfg: Config) -> Self {
        Self(RealtimeTransport::new(
            TransportKind::VkStream,
            &cfg,
            LoopbackSignaller::default(),
        ))
    }
}

macro_rules! delegate {
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

delegate!(VkTransport);
delegate!(TelemostTransport);
delegate!(VkStreamTransport);

/// In-process signaller used until a real service client is wired in.
///
/// It behaves like a loopback pair: frames pushed are immediately visible to
/// `poll`. This keeps the crate self-contained and testable.
#[derive(Default)]
pub struct LoopbackSignaller {
    outbox: VecDeque<Vec<u8>>,
}

#[async_trait]
impl Signaller for LoopbackSignaller {
    async fn join(&mut self, room: &str) -> Result<String> {
        if room.is_empty() {
            return Err(Error::Config("empty room".into()));
        }
        Ok(format!("loopback:{room}"))
    }

    async fn push(&mut self, encoded: &[u8]) -> Result<()> {
        self.outbox.push_back(encoded.to_vec());
        Ok(())
    }

    async fn poll(&mut self) -> Result<Option<Vec<u8>>> {
        Ok(self.outbox.pop_front())
    }

    async fn leave(&mut self) -> Result<()> {
        self.outbox.clear();
        Ok(())
    }
}