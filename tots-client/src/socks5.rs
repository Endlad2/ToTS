use std::{net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr}, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use quinn::Connection;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
};
use tracing::{info, warn};

use tots_core::{
    bridge_client_session, connect_proxy, quic_client_endpoint, read_proxy_status, read_udp_record,
    write_proxy_request, write_udp_record, ClientConfig, ProxyTarget, TurnProvider,
    PROXY_CMD_CONNECT, PROXY_CMD_UDP_ASSOCIATE, QUIC_SERVER_NAME, QUIC_SERVER_VIRTUAL_ADDR,
};

pub async fn run_socks5_quic(
    config: ClientConfig,
    provider: Arc<dyn TurnProvider>,
    bind: SocketAddr,
) -> Result<()> {
    let session = connect_proxy(&config, provider).await?;
    let carrier_name = session.transport_name();
    let (socket, carrier_task) = bridge_client_session(session);
    let mut carrier_task = carrier_task;
    let endpoint = quic_client_endpoint(socket)?;
    let connecting = endpoint
        .connect(QUIC_SERVER_VIRTUAL_ADDR, QUIC_SERVER_NAME)
        .context("start nested QUIC connection over ToTS carrier")?;
    let connection = tokio::time::timeout(config.connect_timeout + Duration::from_secs(4), connecting)
        .await
        .context("nested QUIC handshake timeout")??;

    let listener = TcpListener::bind(bind)
        .await
        .with_context(|| format!("bind local SOCKS5 listener at {bind}"))?;
    info!(%bind, carrier = carrier_name, "ToTS SOCKS5-over-QUIC proxy is listening");

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted.context("accept SOCKS5 client")?;
                let connection = connection.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_socks_client(stream, peer, connection).await {
                        warn!(%peer, %err, "SOCKS5 client session ended with error");
                    }
                });
            }
            result = &mut carrier_task => {
                match result {
                    Ok(Ok(())) => bail!("ToTS carrier closed"),
                    Ok(Err(err)) => return Err(err.context("ToTS carrier failed")),
                    Err(err) => return Err(anyhow::Error::from(err).context("ToTS carrier task failed")),
                }
            }
            shutdown = tokio::signal::ctrl_c() => {
                shutdown.context("wait for Ctrl+C")?;
                endpoint.close(0u32.into(), b"client shutdown");
                return Ok(());
            }
        }
    }
}

async fn handle_socks_client(mut client: TcpStream, peer: SocketAddr, quic: Connection) -> Result<()> {
    negotiate_no_auth(&mut client).await?;
    let (command, target) = read_socks_request(&mut client).await?;

    match command {
        1 => handle_connect(client, target, quic).await,
        3 => handle_udp_associate(client, peer, quic).await,
        _ => {
            send_socks_reply(&mut client, 0x07, SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)).await?;
            bail!("unsupported SOCKS5 command {command}")
        }
    }
}

async fn negotiate_no_auth(client: &mut TcpStream) -> Result<()> {
    let version = client.read_u8().await?;
    if version != 5 {
        bail!("unsupported SOCKS version {version}");
    }
    let count = client.read_u8().await? as usize;
    if count == 0 {
        bail!("SOCKS5 client sent no authentication methods");
    }
    let mut methods = vec![0u8; count];
    client.read_exact(&mut methods).await?;
    if !methods.contains(&0x00) {
        client.write_all(&[5, 0xff]).await?;
        bail!("SOCKS5 client does not offer no-auth method");
    }
    client.write_all(&[5, 0x00]).await?;
    Ok(())
}

async fn read_socks_request(client: &mut TcpStream) -> Result<(u8, ProxyTarget)> {
    let version = client.read_u8().await?;
    if version != 5 {
        bail!("invalid SOCKS5 request version {version}");
    }
    let command = client.read_u8().await?;
    let reserved = client.read_u8().await?;
    if reserved != 0 {
        bail!("invalid SOCKS5 reserved byte {reserved}");
    }
    let target = read_socks_target(client).await?;
    Ok((command, target))
}

async fn read_socks_target<R: AsyncRead + Unpin>(r: &mut R) -> Result<ProxyTarget> {
    let atyp = r.read_u8().await?;
    Ok(match atyp {
        1 => {
            let mut octets = [0u8; 4];
            r.read_exact(&mut octets).await?;
            let port = r.read_u16().await?;
            ProxyTarget::Ip(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(octets)), port))
        }
        3 => {
            let len = r.read_u8().await? as usize;
            if len == 0 { bail!("empty SOCKS5 domain"); }
            let mut host = vec![0u8; len];
            r.read_exact(&mut host).await?;
            let port = r.read_u16().await?;
            ProxyTarget::Domain {
                host: String::from_utf8(host).context("SOCKS5 domain is not UTF-8")?,
                port,
            }
        }
        4 => {
            let mut octets = [0u8; 16];
            r.read_exact(&mut octets).await?;
            let port = r.read_u16().await?;
            ProxyTarget::Ip(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port))
        }
        other => bail!("unsupported SOCKS5 address type {other}"),
    })
}

async fn handle_connect(mut client: TcpStream, target: ProxyTarget, quic: Connection) -> Result<()> {
    let (mut send, mut recv) = quic.open_bi().await.context("open QUIC stream for SOCKS CONNECT")?;
    write_proxy_request(&mut send, PROXY_CMD_CONNECT, Some(&target)).await?;
    let status = read_proxy_status(&mut recv).await?;
    if status != 0 {
        send_socks_reply(&mut client, proxy_status_to_socks(status), SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)).await?;
        bail!("server rejected proxy CONNECT to {}:{} with status {status}", target.display_host(), target.port());
    }
    send_socks_reply(&mut client, 0, SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)).await?;

    let (mut client_read, mut client_write) = client.into_split();
    let upstream = async {
        tokio::io::copy(&mut client_read, &mut send).await?;
        send.finish()?;
        Result::<()>::Ok(())
    };
    let downstream = async {
        tokio::io::copy(&mut recv, &mut client_write).await?;
        client_write.shutdown().await?;
        Result::<()>::Ok(())
    };
    let _ = tokio::try_join!(upstream, downstream)?;
    Ok(())
}

async fn handle_udp_associate(mut control: TcpStream, peer: SocketAddr, quic: Connection) -> Result<()> {
    let (mut send, mut recv) = quic.open_bi().await.context("open QUIC stream for SOCKS UDP ASSOCIATE")?;
    write_proxy_request(&mut send, PROXY_CMD_UDP_ASSOCIATE, None).await?;
    let status = read_proxy_status(&mut recv).await?;
    if status != 0 {
        send_socks_reply(&mut control, proxy_status_to_socks(status), SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)).await?;
        bail!("server rejected UDP ASSOCIATE with status {status}");
    }

    let udp = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let bound = udp.local_addr()?;
    send_socks_reply(&mut control, 0, bound).await?;
    info!(%peer, udp = %bound, "SOCKS5 UDP association opened");

    let association = async {
        let mut local_peer: Option<SocketAddr> = None;
        let mut buf = vec![0u8; 65_535];
        loop {
            tokio::select! {
                incoming = udp.recv_from(&mut buf) => {
                    let (n, source) = incoming?;
                    if source.ip() != peer.ip() { continue; }
                    local_peer.get_or_insert(source);
                    let (target, payload) = parse_socks_udp_datagram(&buf[..n])?;
                    write_udp_record(&mut send, &target, payload).await?;
                }
                remote = read_udp_record(&mut recv) => {
                    let (source, payload) = remote?;
                    if let Some(local_peer) = local_peer {
                        let packet = build_socks_udp_datagram(&source, &payload)?;
                        udp.send_to(&packet, local_peer).await?;
                    }
                }
            }
        }
        #[allow(unreachable_code)]
        Result::<()>::Ok(())
    };

    let mut close_probe = [0u8; 1];
    tokio::select! {
        result = association => result,
        result = control.read(&mut close_probe) => {
            let _ = result?;
            Ok(())
        }
    }
}

async fn send_socks_reply(stream: &mut TcpStream, reply: u8, bound: SocketAddr) -> Result<()> {
    let mut out = vec![5, reply, 0];
    match bound {
        SocketAddr::V4(addr) => {
            out.push(1);
            out.extend_from_slice(&addr.ip().octets());
            out.extend_from_slice(&addr.port().to_be_bytes());
        }
        SocketAddr::V6(addr) => {
            out.push(4);
            out.extend_from_slice(&addr.ip().octets());
            out.extend_from_slice(&addr.port().to_be_bytes());
        }
    }
    stream.write_all(&out).await?;
    Ok(())
}

fn proxy_status_to_socks(status: u8) -> u8 {
    match status {
        1 => 0x04,
        2 => 0x05,
        3 => 0x03,
        _ => 0x01,
    }
}

fn parse_socks_udp_datagram(packet: &[u8]) -> Result<(ProxyTarget, &[u8])> {
    if packet.len() < 4 || packet[0] != 0 || packet[1] != 0 {
        bail!("invalid SOCKS5 UDP header");
    }
    if packet[2] != 0 {
        bail!("fragmented SOCKS5 UDP datagrams are not supported");
    }
    let atyp = packet[3];
    let mut p = 4usize;
    let target = match atyp {
        1 => {
            if packet.len() < p + 6 { bail!("truncated SOCKS5 UDP IPv4 header"); }
            let ip = Ipv4Addr::new(packet[p], packet[p+1], packet[p+2], packet[p+3]); p += 4;
            let port = u16::from_be_bytes([packet[p], packet[p+1]]); p += 2;
            ProxyTarget::Ip(SocketAddr::new(IpAddr::V4(ip), port))
        }
        3 => {
            if packet.len() < p + 1 { bail!("truncated SOCKS5 UDP domain header"); }
            let len = packet[p] as usize; p += 1;
            if len == 0 || packet.len() < p + len + 2 { bail!("truncated SOCKS5 UDP domain header"); }
            let host = String::from_utf8(packet[p..p+len].to_vec()).context("SOCKS5 UDP domain is not UTF-8")?; p += len;
            let port = u16::from_be_bytes([packet[p], packet[p+1]]); p += 2;
            ProxyTarget::Domain { host, port }
        }
        4 => {
            if packet.len() < p + 18 { bail!("truncated SOCKS5 UDP IPv6 header"); }
            let mut octets = [0u8; 16]; octets.copy_from_slice(&packet[p..p+16]); p += 16;
            let port = u16::from_be_bytes([packet[p], packet[p+1]]); p += 2;
            ProxyTarget::Ip(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port))
        }
        other => bail!("unsupported SOCKS5 UDP address type {other}"),
    };
    Ok((target, &packet[p..]))
}

fn build_socks_udp_datagram(source: &ProxyTarget, payload: &[u8]) -> Result<Vec<u8>> {
    let mut out = vec![0, 0, 0];
    match source {
        ProxyTarget::Ip(SocketAddr::V4(addr)) => {
            out.push(1);
            out.extend_from_slice(&addr.ip().octets());
            out.extend_from_slice(&addr.port().to_be_bytes());
        }
        ProxyTarget::Domain { host, port } => {
            let host = host.as_bytes();
            if host.is_empty() || host.len() > 255 { bail!("invalid UDP source domain length"); }
            out.push(3);
            out.push(host.len() as u8);
            out.extend_from_slice(host);
            out.extend_from_slice(&port.to_be_bytes());
        }
        ProxyTarget::Ip(SocketAddr::V6(addr)) => {
            out.push(4);
            out.extend_from_slice(&addr.ip().octets());
            out.extend_from_slice(&addr.port().to_be_bytes());
        }
    }
    out.extend_from_slice(payload);
    Ok(out)
}
