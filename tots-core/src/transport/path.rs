use crate::{Error, Result};
use async_trait::async_trait;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::UdpSocket;

#[async_trait]
pub trait DatagramPath: Send + Sync {
    fn name(&self) -> &str;
    async fn send(&self, data: &[u8]) -> Result<()>;
    async fn recv(&self, buf: &mut [u8]) -> Result<usize>;
    fn route_ips(&self) -> Vec<std::net::Ipv4Addr> {
        Vec::new()
    }
    /// Periodic path-level maintenance (allocation refresh, keepalive media).
    ///
    /// The worker drives this from its own task so that a receive loop blocked on
    /// downstream back-pressure cannot starve the refresh and silently expire the
    /// transport.
    async fn keepalive(&self) -> Result<()> {
        Ok(())
    }
    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

pub struct DirectUdpPath {
    name: String,
    socket: Arc<UdpSocket>,
}

impl DirectUdpPath {
    pub async fn connect(name: impl Into<String>, target: SocketAddr) -> Result<Self> {
        // Pinned to the physical underlay so the direct provider path cannot be
        // routed back into the full-tunnel TUN device. `connect_udp_pinned` also
        // binds the underlay source address where the platform has no
        // device-bind socket option, and connects the socket.
        let s = super::socket::connect_udp_pinned(target).await?;
        Ok(Self {
            name: name.into(),
            socket: Arc::new(s),
        })
    }
}
#[async_trait]
impl DatagramPath for DirectUdpPath {
    fn route_ips(&self) -> Vec<std::net::Ipv4Addr> {
        match self.socket.peer_addr().ok().map(|a| a.ip()) {
            Some(std::net::IpAddr::V4(ip)) => vec![ip],
            _ => vec![],
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
    async fn send(&self, data: &[u8]) -> Result<()> {
        let n = self.socket.send(data).await?;
        if n != data.len() {
            return Err(Error::Transport("short UDP send".into()));
        }
        Ok(())
    }
    async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        Ok(self.socket.recv(buf).await?)
    }
}
