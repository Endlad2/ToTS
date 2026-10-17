//! UDP-режим сервера: принимает UDP-датаграммы и проксирует их в backend.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::net::UdpSocket;
use tracing::{debug, info, warn};

use tots_core::obf::Profile;

use crate::auth::Authorizer;

pub async fn serve(
    listen: SocketAddr,
    connect: SocketAddr,
    _profile: Profile,
    _obf_key: Option<Vec<u8>>,
    _obf_timing: Option<Duration>,
    _authorizer: Arc<Authorizer>,
) -> Result<()> {
    let sock = Arc::new(UdpSocket::bind(listen).await?);
    info!(%listen, "udp listener up");

    loop {
        let mut buf = vec![0u8; 65535];
        let (n, peer) = match sock.recv_from(&mut buf).await {
            Ok(x) => x,
            Err(e) => {
                warn!("recv_from: {e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        debug!(%peer, bytes = n, "datagram");
        let sock = sock.clone();
        let payload = buf[..n].to_vec();
        tokio::spawn(async move {
            if let Err(e) = handle(sock, peer, connect, payload).await {
                debug!("session: {e}");
            }
        });
    }
}

async fn handle(
    sock: Arc<UdpSocket>,
    peer: SocketAddr,
    connect: SocketAddr,
    _first: Vec<u8>,
) -> Result<()> {
    let backend = UdpSocket::bind("0.0.0.0:0").await?;
    backend
        .connect(connect)
        .await
        .with_context(|| format!("connect {connect}"))?;
    let backend = Arc::new(backend);

    let mut from_client = vec![0u8; 65535];
    let mut from_backend = vec![0u8; 65535];

    loop {
        tokio::select! {
            r = sock.recv_from(&mut from_client) => {
                match r {
                    Ok((n, p)) if p == peer => {
                        if let Err(e) = backend.send(&from_client[..n]).await {
                            debug!("backend send: {e}");
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
            r = backend.recv(&mut from_backend) => {
                match r {
                    Ok(n) => {
                        if let Err(e) = sock.send_to(&from_backend[..n], peer).await {
                            debug!("client send: {e}");
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
    Ok(())
}
