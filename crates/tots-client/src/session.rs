//! Управление сессией клиента: DTLS + Client ID + локальный listener.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::net::UdpSocket;
use tracing::{info, warn};

use tots_core::clientid::ClientMode;
use tots_core::dtls::{Config as DtlsConfig, DtlsConnector};
use tots_core::obf::Profile;
use tots_providers::vk::VkProvider;
use tots_providers::TurnProvider;

use crate::cli::{Mode, Transport as TransportFlag};
use crate::transport::{IoStream, TcpIo, TokioIoAdapter, TurnIo, Transport};

pub type BoxStream = Box<dyn IoStream + Send + Unpin>;

#[allow(clippy::too_many_arguments)]
pub async fn run_direct(
    peer: SocketAddr,
    listen: SocketAddr,
    _streams: u32,
    profile: Profile,
    obf_key: Option<Vec<u8>>,
    obf_timing: Option<Duration>,
    client_id: String,
    mode: Mode,
    _debug: bool,
) -> Result<()> {
    let io = Transport::direct(peer).await.context("direct connect")?;
    info!(%peer, "direct connection established");
    let stream: BoxStream = match io {
        Transport::Direct(s) => Box::new(TcpIo(s)),
        Transport::Turn(a) => Box::new(TurnIo::new(a, peer)),
    };
    run_single_session(stream, listen, profile, obf_key, obf_timing, client_id, mode).await
}

#[allow(clippy::too_many_arguments)]
pub async fn run_vk(
    peer: SocketAddr,
    listen: SocketAddr,
    streams: u32,
    profile: Profile,
    obf_key: Option<Vec<u8>>,
    obf_timing: Option<Duration>,
    client_id: String,
    mode: Mode,
    _transport: TransportFlag,
    provider: VkProvider,
) -> Result<()> {
    let creds = provider.get_credentials(1).await?;
    if creds.server_addrs.is_empty() {
        anyhow::bail!("no TURN servers returned by provider");
    }
    let turn_server: SocketAddr = creds.server_addrs[0]
        .parse()
        .context("parse TURN server addr")?;
    info!(%turn_server, "using TURN server from provider");

    let io = Transport::turn(
        turn_server,
        creds.username.clone(),
        creds.password.clone(),
    )
    .await
    .context("TURN allocate")?;

    let stream: BoxStream = match io {
        Transport::Direct(s) => Box::new(TcpIo(s)),
        Transport::Turn(a) => Box::new(TurnIo::new(a, peer)),
    };

    let _ = streams;
    run_single_session(stream, listen, profile, obf_key, obf_timing, client_id, mode).await
}

async fn run_single_session(
    stream: BoxStream,
    listen: SocketAddr,
    profile: Profile,
    obf_key: Option<Vec<u8>>,
    obf_timing: Option<Duration>,
    client_id: String,
    mode: Mode,
) -> Result<()> {
    let _ = (profile, obf_key, obf_timing);

    let adapter = TokioIoAdapter::new(stream);

    info!("starting DTLS handshake");
    let dtls = DtlsConnector::connect(adapter, DtlsConfig {
        handshake_timeout: Duration::from_secs(20),
    })
    .await
    .context("DTLS handshake")?;
    info!("DTLS handshake complete");

    let (mut rd, mut wr) = dtls.into_split();

    let wire_mode = match mode {
        Mode::Udp => ClientMode::Udp,
        Mode::Tcp => ClientMode::Tcp,
    };

    // Client ID с retransmit.
    send_client_id(&mut wr, &mut rd, &client_id, wire_mode)
        .await
        .context("send client ID")?;

    let sock = UdpSocket::bind(listen).await?;
    info!(%listen, "UDP relay listening");

    let mut from_client = vec![0u8; 2048];
    let mut from_dtls = vec![0u8; 2048];
    let mut last_peer: Option<SocketAddr> = None;

    loop {
        tokio::select! {
            r = sock.recv_from(&mut from_client) => {
                match r {
                    Ok((n, peer)) => {
                        last_peer = Some(peer);
                        if let Err(e) = wr.write(&from_client[..n]).await {
                            warn!("dtls write: {e}");
                            break;
                        }
                    }
                    Err(e) => { warn!("local read: {e}"); break; }
                }
            }
            r = rd.read(&mut from_dtls) => {
                match r {
                    Ok(0) => { info!("dtls closed"); break; }
                    Ok(n) => {
                        if let Some(peer) = last_peer {
                            if let Err(e) = sock.send_to(&from_dtls[..n], peer).await {
                                warn!("local send: {e}");
                            }
                        }
                    }
                    Err(e) => { warn!("dtls read: {e}"); break; }
                }
            }
        }
    }

    Ok(())
}

/// Отправляет Client ID с retransmit-подтверждением.
async fn send_client_id<R, W>(
    wr: &mut W,
    rd: &mut R,
    client_id: &str,
    mode: ClientMode,
) -> Result<()>
where
    R: ReadHalf,
    W: WriteHalf,
{
    let rec = build_client_id_record(client_id, mode);
    let mut ack = [0u8; 1];
    for wait in [1u64, 2, 4] {
        wr.write(&rec).await?;
        match tokio::time::timeout(Duration::from_secs(wait), rd.read_exact(&mut ack)).await {
            Ok(Ok(())) if ack[0] == 0x06 => {
                info!("client ID acknowledged");
                return Ok(());
            }
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(anyhow::anyhow!("{}", e)),
            Err(_) => {}
        }
    }
    anyhow::bail!("server did not acknowledge client ID")
}

fn build_client_id_record(client_id: &str, mode: ClientMode) -> Vec<u8> {
    let bytes = client_id.as_bytes();
    let len = bytes.len().min(255);
    let mut buf = Vec::with_capacity(len + 3);
    buf.push(len as u8);
    buf.extend_from_slice(&bytes[..len]);
    buf.push(mode as u8);
    buf.push(2);
    buf
}

/// Интерфейсы, чтобы отвязать `send_client_id` от конкретного типа half.
pub trait WriteHalf {
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::io::Result<()>> + Send + 'a>,
    >;
}

pub trait ReadHalf {
    fn read_exact<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::io::Result<()>> + Send + 'a>,
    >;
}

impl<S> WriteHalf for tots_core::dtls::session::WriteHalf<S>
where
    S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + Unpin + Send,
{
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::io::Result<()>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.write(buf)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))
        })
    }
}

impl<S> ReadHalf for tots_core::dtls::session::ReadHalf<S>
where
    S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + Unpin + Send,
{
    fn read_exact<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::io::Result<()>> + Send + 'a>,
    > {
        Box::pin(async move {
            let mut total = 0;
            while total < buf.len() {
                let n = self
                    .read(&mut buf[total..])
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "eof",
                    ));
                }
                total += n;
            }
            Ok(())
        })
    }
}
