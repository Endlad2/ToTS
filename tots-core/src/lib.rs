//! ToTS core library.
//!
//! The library treats the local UDP socket as the KCP-facing side: each UDP
//! datagram is preserved as one ToTS `Data` frame.

mod client;
mod crypto;
mod provider;
mod transport;
mod wire;

pub use client::{run, ClientConfig, DEFAULT_LOCAL_PORT, DEFAULT_TURN_PORT, DEFAULT_WEBRTC_PORT};
pub use crypto::PacketCipher;
pub use provider::{TurnCredentials, TurnProvider, TurnServer};
pub use wire::{Frame, PROTOCOL_VERSION};
