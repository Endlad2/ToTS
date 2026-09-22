//! ToTS — Tunnel over Transports.
//!
//! A VPN core that carries an encrypted payload (WireGuard datagrams over UDP,
//! or Xray/TCP streams) tunnelled through a set of "cover" transports that ride
//! on ordinary consumer web services:
//!
//! * VK (calls/TURN) — same idea as `free-turn-proxy-core`
//! * Yandex Telemost, VK Stream — realtime transports (cf. `olcrtc-core`)
//! * Yandex Docs, Mail Docs, Max, OneME — document/messaging transports (cf. `OpenFlux-core`)
//!
//! The core is platform agnostic; `tots-cli` wires it to a real TUN device.

pub mod config;
pub mod crypto;
pub mod error;
pub mod transport;
pub mod transports;
pub mod tunnel;

pub use config::{Config, Payload, TransportKind};
pub use error::{Error, Result};
pub use transport::{Frame, Transport};
pub use tunnel::Tunnel;

/// Human readable version string.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");