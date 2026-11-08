use std::{net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr}, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use quinn::{
    rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    Connection, Endpoint, EndpointConfig, RecvStream, SendStream, ServerConfig, TokioRuntime,
};
use tokio::{
    io::AsyncWriteExt,
    net::{lookup_host, TcpStream, UdpSocket},
    task::JoinHandle,
};
use tracing::{info, warn};

use tots_core::{
    read_proxy_request, read_udp_record, write_proxy_status, write_udp_record,
    ProxyTarget, VirtualDatagramBridge, VirtualUdpSocket, PROXY_CMD_CONNECT,
    PROXY_CMD_UDP_ASSOCIATE, QUIC_CLIENT_VIRTUAL_ADDR, QUIC_SERVER_VIRTUAL_ADDR,
};

pub fn start_quic_proxy() -> Result<(VirtualDatagramBridge, JoinHandle<Result<()>>)> {
    let (socket, bridge) = VirtualUdpSocket::new(QUIC_SERVER_VIRTUAL_ADDR, QUIC_CLIENT_VIRTUAL_ADDR);
    let endpoint = build_server_endpoint(socket)?;
    let task = tokio::spawn(async move { run_endpoint(endpoint).await });
    Ok((bridge, task))
}

fn build_server_endpoint(socket: Arc<VirtualUdpSocket>) -> Result<Endpoint> {
    // This is an end-entity TLS certificate (CA=FALSE) signed by the
    // embedded ToTS nested-QUIC CA trusted by the client.
    const CERT_DER: &[u8] = include_bytes!("quic_cert.der");
    const KEY_DER: &[u8] = include_bytes!("quic_key.der");
    let cert = CertificateDer::from(CERT_DER);
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(KEY_DER.to_vec()));
    let server = ServerConfig::with_single_cert(vec![cert], key)
        .context("build nested QUIC server TLS config")?;
    Endpoint::new_with_abstract_socket(
        EndpointConfig::default(),
        Some(server),
        socket,
        Arc::new(TokioRuntime),
    )
    .context("create nested QUIC server endpoint")
}

async fn run_endpoint(endpoint: Endpoint) -> Result<()> {
    loop {
        let Some(incoming) = endpoint.accept().await else {
            return Ok(());
        };
        tokio::spawn(async move {
            match incoming.await {
                Ok(connection) => {
                    info!(remote = %connection.remote_address(), "nested QUIC proxy connection established");
                    if let Err(err) = serve_connection(connection).await {
                        warn!(%err, "nested QUIC proxy connection ended");
                    }
                }
                Err(err) => warn!(%err, "nested QUIC proxy handshake failed"),
            }
        });
    }
}

async fn serve_connection(connection: Connection) -> Result<()> {
    loop {
        match connection.accept_bi().await {
            Ok((send, recv)) => {
                tokio::spawn(async move {
                    if let Err(err) = handle_stream(send, recv).await {
                        warn!(%err, "QUIC proxy stream failed");
                    }
                });
            }
            Err(quinn::ConnectionError::ApplicationClosed(_))
            | Err(quinn::ConnectionError::LocallyClosed) => return Ok(()),
            Err(err) => return Err(err.into()),
        }
    }
}

async fn handle_stream(mut send: SendStream, mut recv: RecvStream) -> Result<()> {
    let (command, target) = read_proxy_request(&mut recv).await?;
    match command {
        PROXY_CMD_CONNECT => {
            let target = target.context("CONNECT request omitted target")?;
            handle_connect(send, recv, target).await
        }
        PROXY_CMD_UDP_ASSOCIATE => {
            write_proxy_status(&mut send, 0).await?;
            handle_udp(send, recv).await
        }
        other => {
            write_proxy_status(&mut send, 4).await?;
            bail!("unsupported proxy command {other}")
        }
    }
}

async fn handle_connect(mut send: SendStream, mut recv: RecvStream, target: ProxyTarget) -> Result<()> {
    let remote = match connect_target(&target).await {
        Ok(stream) => stream,
        Err(err) => {
            let status = match err.downcast_ref::<std::io::Error>().map(|e| e.kind()) {
                Some(std::io::ErrorKind::ConnectionRefused) => 2,
                Some(std::io::ErrorKind::NetworkUnreachable) => 1,
                _ => 3,
            };
            write_proxy_status(&mut send, status).await?;
            return Err(err.context(format!("connect proxy target {}:{}", target.display_host(), target.port())));
        }
    };
    write_proxy_status(&mut send, 0).await?;
    info!(host = %target.display_host(), port = target.port(), "QUIC proxy CONNECT opened");

    let (mut remote_read, mut remote_write) = remote.into_split();
    let upstream = async {
        tokio::io::copy(&mut recv, &mut remote_write).await?;
        remote_write.shutdown().await?;
        Result::<()>::Ok(())
    };
    let downstream = async {
        tokio::io::copy(&mut remote_read, &mut send).await?;
        send.finish()?;
        Result::<()>::Ok(())
    };
    let _ = tokio::try_join!(upstream, downstream)?;
    Ok(())
}

async fn connect_target(target: &ProxyTarget) -> Result<TcpStream> {
    match target {
        ProxyTarget::Ip(addr) => TcpStream::connect(addr).await.context("connect TCP target"),
        ProxyTarget::Domain { host, port } => TcpStream::connect((host.as_str(), *port))
            .await
            .with_context(|| format!("resolve/connect TCP target {host}:{port}")),
    }
}

async fn handle_udp(mut send: SendStream, mut recv: RecvStream) -> Result<()> {
    let udp4 = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)).await?;
    // IPv6 is optional: keep IPv4 UDP ASSOCIATE working on hosts where the
    // kernel/provider has IPv6 disabled instead of failing the whole session.
    let udp6 = UdpSocket::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0))
        .await
        .ok();
    let mut buf4 = vec![0u8; 65_535];
    let mut buf6 = vec![0u8; 65_535];

    loop {
        tokio::select! {
            record = read_udp_record(&mut recv) => {
                let (target, payload) = record?;
                let destination = resolve_udp_target(&target).await?;
                match destination {
                    SocketAddr::V4(_) => { udp4.send_to(&payload, destination).await?; }
                    SocketAddr::V6(_) => {
                        let socket = udp6.as_ref().context("IPv6 UDP egress is unavailable on this server")?;
                        socket.send_to(&payload, destination).await?;
                    }
                }
            }
            packet = udp4.recv_from(&mut buf4) => {
                let (n, source) = packet?;
                write_udp_record(&mut send, &ProxyTarget::Ip(source), &buf4[..n]).await?;
            }
            packet = recv_optional_udp(udp6.as_ref(), &mut buf6) => {
                let (n, source) = packet?;
                write_udp_record(&mut send, &ProxyTarget::Ip(source), &buf6[..n]).await?;
            }
        }
    }
}

async fn recv_optional_udp(
    socket: Option<&UdpSocket>,
    buf: &mut [u8],
) -> std::io::Result<(usize, SocketAddr)> {
    match socket {
        Some(socket) => socket.recv_from(buf).await,
        None => std::future::pending().await,
    }
}

async fn resolve_udp_target(target: &ProxyTarget) -> Result<SocketAddr> {
    match target {
        ProxyTarget::Ip(addr) => Ok(*addr),
        ProxyTarget::Domain { host, port } => {
            tokio::time::timeout(Duration::from_secs(8), lookup_host((host.as_str(), *port)))
                .await
                .context("UDP target DNS timeout")??
                .next()
                .with_context(|| format!("DNS returned no address for {host}:{port}"))
        }
    }
}
