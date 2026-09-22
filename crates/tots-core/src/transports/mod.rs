//! Concrete cover transports.
//!
//! Every transport hides ToTS frames inside traffic to an ordinary consumer
//! service. Two families exist:
//!
//! * **Realtime** — VK, Yandex Telemost, VK Stream. These negotiate a WebRTC /
//!   TURN session and carry frames over data channels (logic modelled after
//!   `free-turn-proxy-core` and `olcrtc-core`).
//! * **Document** — Yandex Docs, Mail Docs, Max, OneME. These use the service's
//!   document / message sync channel as a byte pipe (logic modelled after
//!   `OpenFlux-core`).
//!
//! The current implementations provide the protocol framing and connection
//! state machines; wiring them to the real service endpoints is done through
//! the `Endpoint` traits so the crate builds and tests without network access.

use crate::config::{Config, TransportKind};
use crate::error::{Error, Result};
use crate::transport::Transport;

pub mod docs;
pub mod realtime;

pub use docs::{MailDocsTransport, MaxTransport, OneMeTransport, YandexDocsTransport};
pub use realtime::{TelemostTransport, VkStreamTransport, VkTransport};

/// Build the transport described by `cfg`.
///
/// This is the single entry point used by [`crate::tunnel::Tunnel`].
pub fn build(cfg: &Config) -> Result<Box<dyn Transport>> {
    let transport: Box<dyn Transport> = match cfg.transport {
        TransportKind::Vk => Box::new(VkTransport::new(cfg.clone())),
        TransportKind::YandexTelemost => Box::new(TelemostTransport::new(cfg.clone())),
        TransportKind::VkStream => Box::new(VkStreamTransport::new(cfg.clone())),
        TransportKind::YandexDocs => Box::new(YandexDocsTransport::new(cfg.clone())),
        TransportKind::MailDocs => Box::new(MailDocsTransport::new(cfg.clone())),
        TransportKind::Max => Box::new(MaxTransport::new(cfg.clone())),
        TransportKind::OneMe => Box::new(OneMeTransport::new(cfg.clone())),
    };
    Ok(transport)
}

/// Encode a frame for the wire: `[u32 seq][u32 len][payload]` (big endian).
pub(crate) fn encode_frame(seq: u64, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + data.len());
    out.extend_from_slice(&(seq as u32).to_be_bytes());
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(data);
    out
}

/// Decode a frame produced by [`encode_frame`].
pub(crate) fn decode_frame(buf: &[u8], kind: TransportKind) -> Result<(u64, Vec<u8>)> {
    if buf.len() < 8 {
        return Err(Error::transport(kind.id(), anyhow::anyhow!("short frame")));
    }
    let seq = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as u64;
    let len = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
    if buf.len() < 8 + len {
        return Err(Error::transport(kind.id(), anyhow::anyhow!("truncated frame")));
    }
    Ok((seq, buf[8..8 + len].to_vec()))
}