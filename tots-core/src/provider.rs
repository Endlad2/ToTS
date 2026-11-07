use std::{net::Ipv4Addr, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

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
}

#[async_trait]
pub trait TurnProvider: Send + Sync {
    fn name(&self) -> &'static str;

    /// Connect a provider-native transport when the service requires its own
    /// room/SFU signaling path. Generic TURN providers return None.
    async fn connect_transport(&self) -> Result<Option<Arc<dyn ProviderTransport>>> {
        Ok(None)
    }

    /// Obtain ordinary TURN credentials. Used only when connect_transport()
    /// returns None.
    async fn get_credentials(&self) -> Result<TurnCredentials>;
}
