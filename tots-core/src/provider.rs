use std::{net::Ipv4Addr, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayScope {
    /// Credentials may be used as ordinary RFC 5766 TURN credentials for an explicitly configured peer.
    GenericTurn,
    /// Credentials are bound to a provider call/session and must not be repurposed as a generic UDP relay.
    ProviderSession,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnServer {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnCredentials {
    pub username: String,
    pub password: String,
    pub servers: Vec<TurnServer>,
}

/// Optional provider-native packet transport. Providers such as Jazz are not
/// generic TURN proxies: their credentials are scoped to the provider's own
/// WebRTC/SFU session. A native transport lets a provider carry ToTS packets
/// through that real session instead of abusing TURN CreatePermission toward
/// an arbitrary VPS address.
#[async_trait]
pub trait ProviderTransport: Send + Sync {
    fn name(&self) -> &'static str;
    async fn send(&self, packet: Vec<u8>) -> Result<()>;
    async fn recv(&self) -> Result<Vec<u8>>;
    async fn close(&self) {}
    fn route_exception_ipv4(&self) -> Vec<Ipv4Addr> {
        Vec::new()
    }

    /// Optional live updates for endpoints that must bypass a client-side
    /// full-tunnel route. Provider-native transports can discover new SFU/TURN
    /// addresses after ICE restarts or DNS changes, so a one-shot list is not
    /// always sufficient.
    fn route_exception_updates(&self) -> Option<watch::Receiver<Vec<Ipv4Addr>>> {
        None
    }
}

#[async_trait]
pub trait TurnProvider: Send + Sync {
    fn name(&self) -> &'static str;

    /// Whether credentials from this provider are ordinary TURN credentials
    /// or are bound to the provider's own call/media session.
    fn relay_scope(&self) -> RelayScope {
        RelayScope::GenericTurn
    }

    /// Connect a provider-native transport when the service requires its own
    /// room/SFU signaling path. Generic TURN providers return None.
    async fn connect_transport(&self) -> Result<Option<Arc<dyn ProviderTransport>>> {
        Ok(None)
    }

    /// Obtain ordinary TURN credentials. Used only when connect_transport()
    /// returns None.
    async fn get_credentials(&self) -> Result<TurnCredentials>;

    /// Provider control-plane hosts that may still be needed while a full-tunnel
    /// route is active (token refresh, call/session APIs, WebSocket signaling).
    /// The client resolves these before installing the TUN default route and pins
    /// the resulting IPv4 addresses through the original physical gateway.
    fn route_exception_hosts(&self) -> Vec<(String, u16)> {
        Vec::new()
    }
}
