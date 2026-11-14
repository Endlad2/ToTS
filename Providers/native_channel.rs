use async_trait::async_trait;
use parking_lot::RwLock;
use std::{
    collections::BTreeSet,
    net::Ipv4Addr,
    sync::Arc,
};
use tokio::sync::{mpsc, watch, Mutex};
use crate::{transport::DatagramPath, Error, Result};

const QUEUE_CAPACITY: usize = 8192;

pub struct ChannelPath {
    name: String,
    outbound: mpsc::Sender<Vec<u8>>,
    inbound: Mutex<mpsc::Receiver<Vec<u8>>>,
    shutdown: watch::Sender<bool>,
    route_ips: Arc<RwLock<BTreeSet<Ipv4Addr>>>,
}

pub struct ChannelEndpoint {
    pub outbound: mpsc::Receiver<Vec<u8>>,
    pub inbound: mpsc::Sender<Vec<u8>>,
    pub shutdown: watch::Receiver<bool>,
    pub route_ips: Arc<RwLock<BTreeSet<Ipv4Addr>>>,
}

pub fn pair(name: impl Into<String>) -> (Arc<ChannelPath>, ChannelEndpoint) {
    let (outbound_tx, outbound_rx) = mpsc::channel(QUEUE_CAPACITY);
    let (inbound_tx, inbound_rx) = mpsc::channel(QUEUE_CAPACITY);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let route_ips = Arc::new(RwLock::new(BTreeSet::new()));
    (
        Arc::new(ChannelPath {
            name: name.into(),
            outbound: outbound_tx,
            inbound: Mutex::new(inbound_rx),
            shutdown: shutdown_tx,
            route_ips: route_ips.clone(),
        }),
        ChannelEndpoint {
            outbound: outbound_rx,
            inbound: inbound_tx,
            shutdown: shutdown_rx,
            route_ips,
        },
    )
}

pub async fn remember_url(route_ips: &Arc<RwLock<BTreeSet<Ipv4Addr>>>, raw: &str) {
    let Ok(url) = url::Url::parse(raw) else {
        return;
    };
    let Some(host) = url.host_str() else {
        return;
    };
    let port = url.port_or_known_default().unwrap_or(443);
    let Ok(addrs) = tokio::net::lookup_host((host, port)).await else {
        return;
    };
    let mut guard = route_ips.write();
    for addr in addrs {
        if let std::net::IpAddr::V4(ip) = addr.ip() {
            guard.insert(ip);
        }
    }
}

pub fn provider_error(context: &str, error: impl std::fmt::Display) -> Error {
    Error::Provider(format!("{context}: {error}"))
}


#[async_trait]
impl DatagramPath for ChannelPath {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, data: &[u8]) -> Result<()> {
        self.outbound
            .send(data.to_vec())
            .await
            .map_err(|_| Error::Transport(format!("{} send queue closed", self.name)))
    }

    async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        let mut inbound = self.inbound.lock().await;
        let data = inbound
            .recv()
            .await
            .ok_or_else(|| Error::Transport(format!("{} receive queue closed", self.name)))?;
        if data.len() > buf.len() {
            return Err(Error::Transport(format!(
                "{} received oversized datagram {} > {}",
                self.name,
                data.len(),
                buf.len()
            )));
        }
        buf[..data.len()].copy_from_slice(&data);
        Ok(data.len())
    }

    fn route_ips(&self) -> Vec<Ipv4Addr> {
        self.route_ips.read().iter().copied().collect()
    }

    async fn close(&self) -> Result<()> {
        let _ = self.shutdown.send(true);
        Ok(())
    }
}
