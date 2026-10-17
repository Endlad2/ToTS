//! DTLS 1.2-совместимый транспорт поверх произвольного `AsyncRead + AsyncWrite`.
//!
//! Реализация использует X25519-ECDH + ChaCha20-Poly1305 и wire-совместима
//! по записям DTLS 1.2. Полная совместимость с DTLS 1.2 (retransmit,
//! cookie exchange, фрагментация handshake) не реализована — для внутреннего
//! протокола ToTS это не требуется, поскольку обе стороны наши.

pub mod record;
pub mod session;

pub use session::{DtlsAcceptor, DtlsConnector, DtlsSession};

use std::io;

#[derive(Debug, thiserror::Error)]
pub enum DtlsError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("handshake: {0}")]
    Handshake(String),
    #[error("crypto: {0}")]
    Crypto(String),
    #[error("closed")]
    Closed,
}

pub struct Config {
    pub handshake_timeout: std::time::Duration,
}
