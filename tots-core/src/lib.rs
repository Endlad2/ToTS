//! ToTS core library.
//!
//! The library exposes authenticated raw payload sessions used by the current
//! SOCKS5-over-QUIC client as well as explicitly selected legacy UDP/TUN modes.
//! Provider selection and authentication stay below the nested QUIC layer.

mod client;
mod config_uri;
mod crypto;
mod provider;
mod quic_proxy;
mod transport;
mod wire;

pub use client::{
    connect, connect_proxy, connect_tun, run, ClientConfig, ClientSession, TransportPreference, DEFAULT_LOCAL_PORT,
    DEFAULT_TURN_PORT, DEFAULT_WEBRTC_PORT, MAX_DATA_PAYLOAD,
};
pub use config_uri::{TotsConfig, TOTS_CONFIG_SCHEME, TOTS_CONFIG_VERSION};
pub use crypto::PacketCipher;
pub use provider::{ProviderTransport, RelayScope, TurnCredentials, TurnProvider, TurnServer};
pub use quic_proxy::{
    bridge_client_session, quic_client_endpoint, read_proxy_request,
    read_proxy_status, read_udp_record, write_proxy_request, write_proxy_status, write_udp_record,
    ProxyTarget, VirtualDatagramBridge, VirtualUdpSocket, PROXY_CMD_CONNECT,
    PROXY_CMD_UDP_ASSOCIATE, QUIC_CLIENT_VIRTUAL_ADDR, QUIC_SERVER_NAME,
    QUIC_SERVER_VIRTUAL_ADDR,
};
pub use wire::{Frame, PROTOCOL_VERSION};
