//! TCP-режим сервера: DTLS → backend, простой двунаправленный копир.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

use tots_core::dtls::{Config as DtlsConfig, DtlsAcceptor};
use tots_core::obf::Profile;

use crate::auth::Authorizer;

pub async fn serve(
    listen: SocketAddr,
    connect: SocketAddr,
    _profile: Profile,
    _obf_key: Option<Vec<u8>>,
    _obf_timing: Option<Duration>,
    authorizer: Arc<Authorizer>,
) -> Result<()> {
    let listener = TcpListener::bind(listen).await?;
    info!(%listen, "tcp listener up");
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                warn!("accept: {e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        stream.set_nodelay(true).ok();
        debug!(%peer, "new tcp session");
        let authorizer = authorizer.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_session(stream, connect, authorizer).await {
                debug!(%peer, "session error: {e}");
            }
        });
    }
}

async fn handle_session(
    stream: tokio::net::TcpStream,
    connect: SocketAddr,
    _authorizer: Arc<Authorizer>,
) -> Result<()> {
    let dtls = DtlsAcceptor::accept(
        stream,
        DtlsConfig {
            handshake_timeout: Duration::from_secs(30),
        },
    )
    .await
    .context("DTLS accept")?;

    let (mut rd, mut wr) = dtls.into_split();

    let mut len_buf = [0u8; 1];
    read_exact(&mut rd, &mut len_buf).await?;
    let len = len_buf[0] as usize;
    let mut id_buf = vec![0u8; len];
    read_exact(&mut rd, &mut id_buf).await?;
    let mut tail = [0u8; 2];
    let _ = tokio::time::timeout(Duration::from_secs(2), read_exact(&mut rd, &mut tail)).await;
    let client_id = String::from_utf8_lossy(&id_buf).to_string();
    debug!(client_id, "client id read");

    if tail[1] >= 2 {
        wr.write(&[0x06]).await.context("write ack")?;
    }

    let backend = tokio::net::TcpStream::connect(connect)
        .await
        .with_context(|| format!("connect backend {connect}"))?;
    backend.set_nodelay(true).ok();
    let (mut br, mut bw) = backend.into_split();

    let up = async {
        loop {
            let mut buf = vec![0u8; 16 * 1024];
            match rd.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    use tokio::io::AsyncWriteExt;
                    if bw.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    };
    let down = async {
        use tokio::io::AsyncReadExt;
        loop {
            let mut buf = vec![0u8; 16 * 1024];
            match br.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if wr.write(&buf[..n]).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    };
    tokio::select! {
        _ = up => {}
        _ = down => {}
    }
    Ok(())
}

async fn read_exact<S>(rd: &mut tots_core::dtls::session::ReadHalf<S>, buf: &mut [u8]) -> Result<()>
where
    S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + Unpin + Send,
{
    let mut total = 0;
    while total < buf.len() {
        let n = rd
            .read(&mut buf[total..])
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        if n == 0 {
            anyhow::bail!("eof");
        }
        total += n;
    }
    Ok(())
}
