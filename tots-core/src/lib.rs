//! ToTS core library.
//!
//! The library can expose an authenticated raw payload session. The default
//! `run` helper keeps the original KCP-facing local UDP mode, while other
//! front-ends (for example `tots-client --tun`) can attach a TUN interface.

mod client;
mod crypto;
mod provider;
mod transport;
mod wire;

pub use client::{
    connect, run, ClientConfig, ClientSession, DEFAULT_LOCAL_PORT, DEFAULT_TURN_PORT,
    DEFAULT_WEBRTC_PORT, MAX_DATA_PAYLOAD,
};
pub use crypto::PacketCipher;
pub use provider::{TurnCredentials, TurnProvider, TurnServer};
pub use wire::{Frame, PROTOCOL_VERSION};
