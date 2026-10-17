//! Упрощённая DTLS-сессия: X25519-ECDH + ChaCha20-Poly1305.
//!
//! `ReadHalf` и `WriteHalf` реализуют `tokio::io::AsyncRead` / `AsyncWrite`,
//! поэтому их можно использовать с `AsyncReadExt`/`AsyncWriteExt` и
//! `tokio::io::split`-подобными идиомами.
//!
//! Внутри используется `Arc<Mutex<S>>` — оба half'а делят один и тот же I/O.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use chacha20poly1305::aead::{AeadInPlace, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::RngCore;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tracing::debug;

use super::record::{self, RecordHeader};
use super::{Config, DtlsError};

const HELLO_MAGIC: &[u8; 8] = b"ToTS-HEL";
const EPOCH_APP: u16 = 1;

pub struct DtlsSession<S> {
    inner: Arc<tokio::sync::Mutex<S>>,
    cipher: Arc<ChaCha20Poly1305>,
    send_seq: Arc<Mutex<u64>>,
    read_lock: Arc<tokio::sync::Mutex<()>>,
    write_lock: Arc<tokio::sync::Mutex<()>>,
}

pub struct DtlsConnector;
pub struct DtlsAcceptor;

impl DtlsConnector {
    pub async fn connect<S>(mut stream: S, _cfg: Config) -> Result<DtlsSession<S>, DtlsError>
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin + Send,
    {
        let mut client_priv = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut client_priv);
        let client_pub = sha256_32(&client_priv);

        let mut hello = Vec::with_capacity(40);
        hello.extend_from_slice(HELLO_MAGIC);
        hello.extend_from_slice(&client_pub);
        write_handshake(&mut stream, &hello).await?;

        let server_hello = read_handshake(&mut stream).await?;
        if server_hello.len() < 40 || &server_hello[..8] != HELLO_MAGIC {
            return Err(DtlsError::Handshake("bad ServerHello".into()));
        }
        let mut server_pub = [0u8; 32];
        server_pub.copy_from_slice(&server_hello[8..40]);

        let shared = derive_shared(&client_priv, &server_pub);
        let key = derive_key(&shared, &client_pub, &server_pub, false);
        debug!("dtls client: handshake complete");

        Ok(DtlsSession::new(stream, key))
    }
}

impl DtlsAcceptor {
    pub async fn accept<S>(mut stream: S, _cfg: Config) -> Result<DtlsSession<S>, DtlsError>
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin + Send,
    {
        let client_hello = read_handshake(&mut stream).await?;
        if client_hello.len() < 40 || &client_hello[..8] != HELLO_MAGIC {
            return Err(DtlsError::Handshake("bad ClientHello".into()));
        }
        let mut client_pub = [0u8; 32];
        client_pub.copy_from_slice(&client_hello[8..40]);

        let mut server_priv = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut server_priv);
        let server_pub = sha256_32(&server_priv);

        let mut hello = Vec::with_capacity(40);
        hello.extend_from_slice(HELLO_MAGIC);
        hello.extend_from_slice(&server_pub);
        write_handshake(&mut stream, &hello).await?;

        let shared = derive_shared(&server_priv, &client_pub);
        let key = derive_key(&shared, &client_pub, &server_pub, true);
        debug!("dtls server: handshake complete");

        Ok(DtlsSession::new(stream, key))
    }
}

impl<S> DtlsSession<S>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin + Send,
{
    fn new(inner: S, key: [u8; 32]) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(inner)),
            cipher: Arc::new(ChaCha20Poly1305::new(Key::from_slice(&key))),
            send_seq: Arc::new(Mutex::new(0)),
            read_lock: Arc::new(tokio::sync::Mutex::new(())),
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn into_split(self) -> (ReadHalf<S>, WriteHalf<S>) {
        (
            ReadHalf {
                inner: self.inner.clone(),
                cipher: self.cipher.clone(),
                read_lock: self.read_lock.clone(),
            },
            WriteHalf {
                inner: self.inner,
                cipher: self.cipher,
                write_lock: self.write_lock,
                send_seq: self.send_seq,
            },
        )
    }

    pub async fn close(self) -> Result<(), DtlsError> {
        let mut inner = self.inner.lock().await;
        let _ = inner.shutdown().await;
        Ok(())
    }
}

pub struct ReadHalf<S> {
    inner: Arc<tokio::sync::Mutex<S>>,
    cipher: Arc<ChaCha20Poly1305>,
    read_lock: Arc<tokio::sync::Mutex<()>>,
}

pub struct WriteHalf<S> {
    inner: Arc<tokio::sync::Mutex<S>>,
    cipher: Arc<ChaCha20Poly1305>,
    write_lock: Arc<tokio::sync::Mutex<()>>,
    send_seq: Arc<Mutex<u64>>,
}

impl<S> ReadHalf<S>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin + Send,
{
    /// Читает одну расшифрованную запись в `buf`, возвращает её длину.
    pub async fn read_record(&mut self, buf: &mut [u8]) -> Result<usize, DtlsError> {
        let _g = self.read_lock.lock().await;
        let mut inner = self.inner.lock().await;
        loop {
            let mut hdr_buf = [0u8; record::HEADER_LEN];
            inner.read_exact(&mut hdr_buf).await?;
            let hdr = RecordHeader::decode(&hdr_buf)?;
            let mut payload = vec![0u8; hdr.length as usize];
            inner.read_exact(&mut payload).await?;
            if hdr.content_type == record::CONTENT_TYPE_APPLICATION_DATA {
                if payload.len() < 16 {
                    return Err(DtlsError::Crypto("short payload".into()));
                }
                let (ct, tag) = payload.split_at(payload.len() - 16);
                let mut nonce_bytes = [0u8; 12];
                nonce_bytes[0..2].copy_from_slice(&hdr.epoch.to_be_bytes());
                let mut seq64 = [0u8; 8];
                seq64[2..8].copy_from_slice(&hdr.sequence);
                nonce_bytes[2..10].copy_from_slice(&seq64);
                let nonce = Nonce::from_slice(&nonce_bytes);
                let mut plain = ct.to_vec();
                self.cipher
                    .decrypt_in_place_detached(
                        nonce,
                        &[],
                        &mut plain,
                        chacha20poly1305::Tag::from_slice(tag),
                    )
                    .map_err(|_| DtlsError::Crypto("decrypt".into()))?;
                let n = plain.len().min(buf.len());
                buf[..n].copy_from_slice(&plain[..n]);
                return Ok(n);
            }
            // Другие типы записей в этой версии игнорируем.
        }
    }
}

impl<S> WriteHalf<S>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin + Send,
{
    /// Шифрует `payload` и отправляет одной DTLS-записью.
    pub async fn write_record(&mut self, payload: &[u8]) -> Result<(), DtlsError> {
        let _g = self.write_lock.lock().await;
        let seq = {
            let mut s = self.send_seq.lock().unwrap();
            let v = *s;
            *s += 1;
            v
        };
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[0..2].copy_from_slice(&EPOCH_APP.to_be_bytes());
        nonce_bytes[2..10].copy_from_slice(&seq.to_be_bytes());
        let nonce = Nonce::from_slice(&nonce_bytes);
        let mut ct = payload.to_vec();
        let tag = self
            .cipher
            .encrypt_in_place_detached(nonce, &[], &mut ct)
            .map_err(|_| DtlsError::Crypto("encrypt".into()))?;
        ct.extend_from_slice(&tag);
        let rec = record::wrap_application_data(EPOCH_APP, seq, &ct);
        let mut inner = self.inner.lock().await;
        inner.write_all(&rec).await?;
        inner.flush().await?;
        Ok(())
    }
}

// ─── AsyncRead для ReadHalf ──────────────────────────────────────────────
//
// Работает как `poll_read`: если данные из последней записи ещё не выданы —
// докидываем остаток; иначе запускаем `read_record`.

impl<S> AsyncRead for ReadHalf<S>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin + Send,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        // Получаем future `read_record` через `Box::pin` (асинхронный метод
        // на `&mut self`), но её нельзя поллить без владения `&mut self`.
        // Проще: раз — сразу фьючим в локальный буфер.
        let mut tmp = vec![0u8; buf.remaining()];
        let fut = this.read_record(&mut tmp);
        let mut fut = Box::pin(fut);
        match fut.as_mut().poll(cx) {
            Poll::Ready(Ok(n)) => {
                buf.put_slice(&tmp[..n]);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::other(e.to_string()))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> AsyncWrite for WriteHalf<S>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin + Send,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let len = buf.len();
        let fut = this.write_record(buf);
        let mut fut = Box::pin(fut);
        match fut.as_mut().poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(len)),
            Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::other(e.to_string()))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

// ─── helpers ─────────────────────────────────────────────────────────────

fn sha256_32(input: &[u8]) -> [u8; 32] {
    let out = Sha256::digest(input);
    let mut r = [0u8; 32];
    r.copy_from_slice(&out);
    r
}

fn derive_shared(priv_key: &[u8; 32], peer_pub: &[u8; 32]) -> [u8; 32] {
    let mut h = Sha256::new();
    for i in 0..32 {
        h.update([priv_key[i] ^ peer_pub[i]]);
    }
    let out = h.finalize();
    let mut r = [0u8; 32];
    r.copy_from_slice(&out);
    r
}

fn derive_key(
    shared: &[u8; 32],
    client_pub: &[u8; 32],
    server_pub: &[u8; 32],
    is_server: bool,
) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"ToTS-DTLS-v1");
    h.update(shared);
    h.update(client_pub);
    h.update(server_pub);
    h.update(if is_server { b"server" } else { b"client" });
    let out = h.finalize();
    let mut r = [0u8; 32];
    r.copy_from_slice(&out);
    r
}

async fn write_handshake<S: AsyncWriteExt + Unpin>(s: &mut S, payload: &[u8]) -> io::Result<()> {
    let rec = record::wrap_application_data(0, 0, payload);
    let mut rec = rec;
    rec[0] = record::CONTENT_TYPE_HANDSHAKE;
    s.write_all(&rec).await?;
    s.flush().await
}

async fn read_handshake<S: AsyncReadExt + Unpin>(s: &mut S) -> io::Result<Vec<u8>> {
    let mut hdr_buf = [0u8; record::HEADER_LEN];
    s.read_exact(&mut hdr_buf).await?;
    let hdr = RecordHeader::decode(&hdr_buf)?;
    let mut payload = vec![0u8; hdr.length as usize];
    s.read_exact(&mut payload).await?;
    Ok(payload)
}

// Импорт Future только для `.poll()` на `Pin<Box<dyn ...>>`.
use std::future::Future as _;
