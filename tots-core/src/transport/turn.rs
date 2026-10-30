use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures::{SinkExt, StreamExt};
use tokio::net::{lookup_host, UdpSocket};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use turnclient::{
    ChannelUsage, MessageFromTurnServer, MessageToTurnServer, TurnClientBuilder,
};

use crate::TurnCredentials;

pub struct TurnTransport {
    outgoing: mpsc::Sender<Vec<u8>>,
    incoming: mpsc::Receiver<Vec<u8>>,
    close_tx: mpsc::Sender<()>,
}

impl TurnTransport {
    pub async fn connect(
        creds: &TurnCredentials,
        peer_host: &str,
        peer_port: u16,
        connect_timeout: Duration,
    ) -> Result<Self> {
        let peer = resolve_one(peer_host, peer_port).await?;
        let mut last_error = None;
        for server in &creds.servers {
            let turn_server = match resolve_one(&server.host, server.port).await {
                Ok(addr) => addr,
                Err(err) => {
                    last_error = Some(err);
                    continue;
                }
            };
            match Self::connect_one(
                turn_server,
                peer,
                creds.username.clone(),
                creds.password.clone(),
                connect_timeout,
            )
            .await
            {
                Ok(t) => return Ok(t),
                Err(err) => last_error = Some(err),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("provider returned no usable UDP TURN servers")))
    }

    async fn connect_one(
        turn_server: SocketAddr,
        peer: SocketAddr,
        username: String,
        password: String,
        connect_timeout: Duration,
    ) -> Result<Self> {
        let bind_addr = if turn_server.is_ipv6() { "[::]:0" } else { "0.0.0.0:0" };
        let udp = UdpSocket::bind(bind_addr)
            .await
            .with_context(|| format!("bind TURN UDP socket for {turn_server}"))?;
        let client = TurnClientBuilder::new(turn_server, username, password)
            .build_and_send_request(udp);
        let (mut sink, mut stream) = client.split();

        let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<Vec<u8>>(256);
        let (incoming_tx, incoming_rx) = mpsc::channel::<Vec<u8>>(256);
        let (ready_tx, ready_rx) = oneshot::channel::<Result<()>>();
        let (close_tx, mut close_rx) = mpsc::channel::<()>(1);

        tokio::spawn(async move {
            let mut ready_tx = Some(ready_tx);
            loop {
                tokio::select! {
                    evt = stream.next() => {
                        let Some(evt) = evt else {
                            if let Some(tx) = ready_tx.take() { let _ = tx.send(Err(anyhow!("TURN stream closed"))); }
                            break;
                        };
                        match evt {
                            Ok(MessageFromTurnServer::AllocationGranted { .. }) => {
                                if let Err(err) = sink.send(MessageToTurnServer::AddPermission(peer, ChannelUsage::WithChannel)).await {
                                    if let Some(tx) = ready_tx.take() { let _ = tx.send(Err(anyhow!(err))); }
                                    break;
                                }
                            }
                            Ok(MessageFromTurnServer::PermissionCreated(addr)) if addr == peer => {
                                if let Some(tx) = ready_tx.take() { let _ = tx.send(Ok(())); }
                            }
                            Ok(MessageFromTurnServer::PermissionNotCreated(addr)) if addr == peer => {
                                if let Some(tx) = ready_tx.take() { let _ = tx.send(Err(anyhow!("TURN permission/channel bind failed for {peer}"))); }
                                break;
                            }
                            Ok(MessageFromTurnServer::RecvFrom(addr, data)) if addr == peer => {
                                if incoming_tx.send(data).await.is_err() { break; }
                            }
                            Ok(MessageFromTurnServer::RedirectedToAlternateServer(addr)) => {
                                if let Some(tx) = ready_tx.take() { let _ = tx.send(Err(anyhow!("TURN redirected to unsupported alternate server {addr}"))); }
                                break;
                            }
                            Ok(MessageFromTurnServer::Disconnected) => break,
                            Ok(_) => {}
                            Err(err) => {
                                if let Some(tx) = ready_tx.take() { let _ = tx.send(Err(anyhow!(err))); }
                                break;
                            }
                        }
                    }
                    packet = outgoing_rx.recv() => {
                        let Some(packet) = packet else { break; };
                        if sink.send(MessageToTurnServer::SendTo(peer, packet)).await.is_err() { break; }
                    }
                    _ = close_rx.recv() => {
                        let _ = sink.send(MessageToTurnServer::Disconnect).await;
                        break;
                    }
                }
            }
        });

        timeout(connect_timeout, ready_rx)
            .await
            .context("TURN allocation/permission timeout")?
            .map_err(|_| anyhow!("TURN worker exited during setup"))??;

        Ok(Self {
            outgoing: outgoing_tx,
            incoming: incoming_rx,
            close_tx,
        })
    }

    pub async fn send(&self, packet: Vec<u8>) -> Result<()> {
        self.outgoing
            .send(packet)
            .await
            .map_err(|_| anyhow!("TURN worker closed"))
    }

    pub async fn recv(&mut self) -> Result<Vec<u8>> {
        self.incoming
            .recv()
            .await
            .ok_or_else(|| anyhow!("TURN worker closed"))
    }

    pub async fn close(&self) {
        let _ = self.close_tx.send(()).await;
    }
}

async fn resolve_one(host: &str, port: u16) -> Result<SocketAddr> {
    lookup_host((host, port))
        .await
        .with_context(|| format!("resolve {host}:{port}"))?
        .next()
        .ok_or_else(|| anyhow!("no address for {host}:{port}"))
}
