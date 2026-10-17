//! UDP-режим сервера: DTLS-терминация поверх UDP + форвардинг в backend.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::{Context as _, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

use tots_core::dtls::{Config as DtlsConfig, DtlsAcceptor};
use tots_core::obf::Profile;

use crate::auth::Authorizer;

const PEER_QUEUE_CAP: usize = 256;

pub async fn serve(
    listen: SocketAddr,
    connect: SocketAddr,
    _profile: Profile,
    _obf_key: Option<Vec<u8>>,
    _obf_timing: Option<Duration>,
    authorizer: Arc<Authorizer>,
) -> Result<()> {
    let sock = Arc::new(UdpSocket::bind(listen).await?);
    info!(%listen, "udp listener up");

    let peers: Arc<Mutex<HashMap<SocketAddr, mpsc::Sender<Vec<u8>>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let mut buf = vec![0u8; 65535];
    loop {
        let (n, peer) = match sock.recv_from(&mut buf).await {
            Ok(x) => x,
            Err(e) => {
                warn!("recv_from: {e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let payload = buf[..n].to_vec();

        let tx = {
            let mut map = peers.lock().await;
            if let Some(tx) = map.get(&peer) {
                tx.clone()
            } else {
                let (tx, rx) = mpsc::channel::<Vec<u8>>(PEER_QUEUE_CAP);
                map.insert(peer, tx.clone());

                let sock_cl = sock.clone();
                let peers_cl = peers.clone();
                let authorizer_cl = authorizer.clone();
                let connect_cl = connect;
                tokio::spawn(async move {
                    debug!(%peer, "peer: new session");
                    let res = handle_peer(sock_cl, peer, rx, authorizer_cl, connect_cl).await;
                    match res {
                        Ok(()) => debug!(%peer, "peer: session ended"),
                        Err(e) => debug!(%peer, error = %e, "peer: session error"),
                    }
                    peers_cl.lock().await.remove(&peer);
                });
                tx
            }
        };

        let _ = tx.send(payload).await;
    }
}

async fn handle_peer(
    sock: Arc<UdpSocket>,
    peer: SocketAddr,
    rx: mpsc::Receiver<Vec<u8>>,
    authorizer: Arc<Authorizer>,
    connect: SocketAddr,
) -> Result<()> {
    let adapter = UdpPeerStream::new(sock.clone(), peer, rx);

    let dtls = DtlsAcceptor::accept(
        adapter,
        DtlsConfig {
            handshake_timeout: Duration::from_secs(30),
        },
    )
    .await
    .context("DTLS accept")?;
    info!(%peer, "DTLS handshake complete");

    let (mut rd, mut wr) = dtls.into_split();
    let mut len_buf = [0u8; 1];
    tokio::time::timeout(Duration::from_secs(5), rd.read_exact(&mut len_buf))
        .await
        .map_err(|_| anyhow::anyhow!("client ID timeout"))??;
    let len = len_buf[0] as usize;
    let mut id_buf = vec![0u8; len];
    rd.read_exact(&mut id_buf).await?;
    let mut tail = [0u8; 2];
    let _ = tokio::time::timeout(Duration::from_secs(2), rd.read_exact(&mut tail)).await;
    let client_id = String::from_utf8_lossy(&id_buf).to_string();
    let version = tail[1];
    let mode = tail[0];

    info!(%peer, client_id = %client_id, mode, "client identified");

    if version >= 2 {
        wr.write_all(&[0x06]).await.context("ack")?;
        wr.flush().await?;
    }

    if !authorizer.is_authorized(&client_id).await {
        warn!(%peer, client_id = %client_id, "unauthorized");
        anyhow::bail!("unauthorized");
    }
    info!(%peer, "client authorized");

    let backend = UdpSocket::bind("0.0.0.0:0").await?;
    backend
        .connect(connect)
        .await
        .with_context(|| format!("connect backend {connect}"))?;
    info!(%peer, backend = %connect, "backend connected");

    let mut dtls_to_backend_buf = vec![0u8; 16 * 1024];
    let mut backend_to_dtls_buf = vec![0u8; 16 * 1024];

    loop {
        tokio::select! {
            r = rd.read(&mut dtls_to_backend_buf) => {
                match r {
                    Ok(0) => { debug!(%peer, "dtls closed"); break; }
                    Ok(n) => {
                        if let Err(e) = backend.send(&dtls_to_backend_buf[..n]).await {
                            warn!(%peer, "backend send: {e}");
                            break;
                        }
                    }
                    Err(e) => { debug!(%peer, "dtls read: {e}"); break; }
                }
            }
            r = backend.recv(&mut backend_to_dtls_buf) => {
                match r {
                    Ok(n) => {
                        if let Err(e) = wr.write_all(&backend_to_dtls_buf[..n]).await {
                            debug!(%peer, "dtls write: {e}");
                            break;
                        }
                    }
                    Err(e) => { debug!(%peer, "backend recv: {e}"); break; }
                }
            }
        }
    }

    Ok(())
}

// ─── Адаптер UdpPeerStream ────────────────────────────────────────────────

struct UdpPeerStream {
    sock: Arc<UdpSocket>,
    peer: SocketAddr,
    rx: mpsc::Receiver<Vec<u8>>,
    pending: Option<(Vec<u8>, usize)>,
}

impl UdpPeerStream {
    fn new(sock: Arc<UdpSocket>, peer: SocketAddr, rx: mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            sock,
            peer,
            rx,
            pending: None,
        }
    }
}

impl AsyncRead for UdpPeerStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some((data, offset)) = &mut self.pending {
            let remaining = data.len() - *offset;
            let n = remaining.min(buf.remaining());
            buf.put_slice(&data[*offset..*offset + n]);
            *offset += n;
            if *offset >= data.len() {
                self.pending = None;
            }
            return Poll::Ready(Ok(()));
        }

        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(data)) => {
                let n = data.len().min(buf.remaining());
                buf.put_slice(&data[..n]);
                if n < data.len() {
                    self.pending = Some((data, n));
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for UdpPeerStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let sock = this.sock.clone();
        let peer = this.peer;
        let data = buf.to_vec();
        let fut = async move { sock.send_to(&data, peer).await.map(|n| n.min(data.len())) };
        let mut fut = Box::pin(fut);
        match std::future::Future::poll(fut.as_mut(), cx) {
            Poll::Ready(r) => Poll::Ready(r),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
