#![forbid(unsafe_code)]

pub mod config;
pub mod crypto;
pub mod error;
pub mod flow;
pub mod fragment;
pub mod protocol;
pub mod providers;
pub mod reassembly;
pub mod recovery;
pub mod session;
pub mod transport;

// Shared physical-egress policy and the WebSocket helper built on it. Both files
// are included by the bundled `jazz-session` crate as well, so client sockets and
// provider/WebRTC sockets follow one rule.
#[path = "../../Providers/egress.rs"]
pub mod egress;
#[path = "../../Providers/ws.rs"]
pub mod ws;

pub use config::{ConnectionConfig, ProviderName};
pub use error::{Error, Result};
pub use session::{DecodedPacket, SessionEngine};
