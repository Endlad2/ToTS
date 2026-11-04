use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const PROTOCOL_VERSION: u16 = 2;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Frame {
    Probe { version: u16, nonce: u64 },
    ProbeAck { version: u16, nonce: u64 },

    /// SDP exchange used only to bootstrap relay-only WebRTC through the
    /// already-created provider TURN allocation.
    SignalOffer { sdp_json: String },
    SignalAnswer { sdp_json: String },

    Auth { device_id: Uuid },
    AuthResult { ok: bool, message: String },
    Data { payload: Vec<u8> },
    Ping { nonce: u64 },
    Pong { nonce: u64 },

    /// Explicit layer-3 TUN session. Appended to preserve the discriminants of
    /// all protocol-v1 frames while protocol version 2 negotiates support.
    AuthTun { device_id: Uuid },
}
