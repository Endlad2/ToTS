use std::{
    fmt,
    io::{self, IoSliceMut},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use anyhow::{bail, Context as _, Result};
use quinn::{
    udp::{RecvMeta, Transmit}, AsyncUdpSocket, ClientConfig as QuicClientConfig, Endpoint,
    EndpointConfig, TokioRuntime, UdpPoller,
};
use quinn::rustls::{pki_types::CertificateDer, RootCertStore};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc,
    task::JoinHandle,
};

use crate::ClientSession;

pub const QUIC_SERVER_NAME: &str = "tots-quic.internal";
pub const QUIC_CLIENT_VIRTUAL_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(100, 64, 0, 2)), 44330);
pub const QUIC_SERVER_VIRTUAL_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)), 44330);
pub const PROXY_PROTOCOL_VERSION: u8 = 1;
pub const PROXY_CMD_CONNECT: u8 = 1;
pub const PROXY_CMD_UDP_ASSOCIATE: u8 = 3;

const CERT_DER: &[u8] = include_bytes!("quic_cert.der");

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProxyTarget {
    Ip(SocketAddr),
    Domain { host: String, port: u16 },
}

impl ProxyTarget {
    pub fn port(&self) -> u16 {
        match self {
            Self::Ip(addr) => addr.port(),
            Self::Domain { port, .. } => *port,
        }
    }

    pub fn display_host(&self) -> String {
        match self {
            Self::Ip(addr) => addr.ip().to_string(),
            Self::Domain { host, .. } => host.clone(),
        }
    }
}

pub struct VirtualDatagramBridge {
    pub inbound: mpsc::UnboundedSender<Vec<u8>>,
    pub outbound: mpsc::UnboundedReceiver<Vec<u8>>,
}

pub struct VirtualUdpSocket {
    local: SocketAddr,
    peer: SocketAddr,
    outbound: mpsc::UnboundedSender<Vec<u8>>,
    inbound: Mutex<mpsc::UnboundedReceiver<Vec<u8>>>,
}

impl fmt::Debug for VirtualUdpSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VirtualUdpSocket")
            .field("local", &self.local)
            .field("peer", &self.peer)
            .finish_non_exhaustive()
    }
}

impl VirtualUdpSocket {
    pub fn new(local: SocketAddr, peer: SocketAddr) -> (Arc<Self>, VirtualDatagramBridge) {
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        (
            Arc::new(Self {
                local,
                peer,
                outbound: outbound_tx,
                inbound: Mutex::new(inbound_rx),
            }),
            VirtualDatagramBridge {
                inbound: inbound_tx,
                outbound: outbound_rx,
            },
        )
    }
}

#[derive(Debug)]
struct AlwaysWritable;

impl UdpPoller for AlwaysWritable {
    fn poll_writable(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncUdpSocket for VirtualUdpSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(AlwaysWritable)
    }

    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        if transmit.destination != self.peer {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                format!("virtual QUIC destination {} does not match peer {}", transmit.destination, self.peer),
            ));
        }
        if let Some(segment) = transmit.segment_size {
            if segment > 0 && segment < transmit.contents.len() {
                for part in transmit.contents.chunks(segment) {
                    self.outbound.send(part.to_vec()).map_err(|_| {
                        io::Error::new(io::ErrorKind::BrokenPipe, "virtual QUIC bridge closed")
                    })?;
                }
                return Ok(());
            }
        }
        self.outbound.send(transmit.contents.to_vec()).map_err(|_| {
            io::Error::new(io::ErrorKind::BrokenPipe, "virtual QUIC bridge closed")
        })
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        if bufs.is_empty() || meta.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let mut receiver = match self.inbound.lock() {
            Ok(receiver) => receiver,
            Err(_) => return Poll::Ready(Err(io::Error::other("virtual QUIC receive lock poisoned"))),
        };
        match receiver.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "virtual QUIC bridge closed",
            ))),
            Poll::Ready(Some(packet)) => {
                if packet.len() > bufs[0].len() {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("virtual QUIC datagram {} bytes exceeds receive buffer {}", packet.len(), bufs[0].len()),
                    )));
                }
                bufs[0][..packet.len()].copy_from_slice(&packet);
                let m = &mut meta[0];
                *m = RecvMeta::default();
                m.addr = self.peer;
                m.len = packet.len();
                m.stride = packet.len();
                m.dst_ip = Some(self.local.ip());
                Poll::Ready(Ok(1))
            }
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
    }

    fn may_fragment(&self) -> bool {
        true
    }
}

pub fn quic_client_endpoint(socket: Arc<VirtualUdpSocket>) -> Result<Endpoint> {
    let cert = CertificateDer::from(CERT_DER);
    let mut roots = RootCertStore::empty();
    roots.add(cert).context("trust embedded ToTS QUIC certificate")?;
    let client_config = QuicClientConfig::with_root_certificates(Arc::new(roots))
        .context("build ToTS QUIC client TLS config")?;
    let mut endpoint = Endpoint::new_with_abstract_socket(
        EndpointConfig::default(),
        None,
        socket,
        Arc::new(TokioRuntime),
    )
    .context("create ToTS virtual QUIC client endpoint")?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}


/// Move an authenticated ToTS proxy session into a virtual UDP socket used by Quinn.
/// The provider/WebRTC transport remains the actual carrier for every QUIC datagram.
pub fn bridge_client_session(mut session: ClientSession) -> (Arc<VirtualUdpSocket>, JoinHandle<Result<()>>) {
    let (socket, mut bridge) = VirtualUdpSocket::new(QUIC_CLIENT_VIRTUAL_ADDR, QUIC_SERVER_VIRTUAL_ADDR);
    let task = tokio::spawn(async move {
        loop {
            enum Event {
                Out(Option<Vec<u8>>),
                In(Result<Vec<u8>>),
            }
            let event = tokio::select! {
                outbound = bridge.outbound.recv() => Event::Out(outbound),
                inbound = session.recv_payload() => Event::In(inbound),
            };
            match event {
                Event::Out(Some(packet)) => session.send_payload(&packet).await?,
                Event::Out(None) => break,
                Event::In(Ok(packet)) => {
                    if bridge.inbound.send(packet).is_err() {
                        break;
                    }
                }
                Event::In(Err(err)) => return Err(err),
            }
        }
        session.close().await;
        Ok(())
    });
    (socket, task)
}

pub async fn write_proxy_request<W: AsyncWrite + Unpin>(
    w: &mut W,
    command: u8,
    target: Option<&ProxyTarget>,
) -> Result<()> {
    w.write_all(&[PROXY_PROTOCOL_VERSION, command]).await?;
    match target {
        Some(target) => write_target(w, target).await?,
        None => w.write_all(&[0]).await?,
    }
    w.flush().await?;
    Ok(())
}

pub async fn read_proxy_request<R: AsyncRead + Unpin>(r: &mut R) -> Result<(u8, Option<ProxyTarget>)> {
    let version = r.read_u8().await?;
    if version != PROXY_PROTOCOL_VERSION {
        bail!("unsupported ToTS proxy protocol version {version}");
    }
    let command = r.read_u8().await?;
    let atyp = r.read_u8().await?;
    let target = if atyp == 0 {
        None
    } else {
        Some(read_target_after_atyp(r, atyp).await?)
    };
    Ok((command, target))
}

pub async fn write_proxy_status<W: AsyncWrite + Unpin>(w: &mut W, status: u8) -> Result<()> {
    w.write_all(&[PROXY_PROTOCOL_VERSION, status]).await?;
    w.flush().await?;
    Ok(())
}

pub async fn read_proxy_status<R: AsyncRead + Unpin>(r: &mut R) -> Result<u8> {
    let version = r.read_u8().await?;
    if version != PROXY_PROTOCOL_VERSION {
        bail!("unsupported ToTS proxy response version {version}");
    }
    Ok(r.read_u8().await?)
}

pub async fn write_udp_record<W: AsyncWrite + Unpin>(
    w: &mut W,
    target: &ProxyTarget,
    payload: &[u8],
) -> Result<()> {
    let mut record = Vec::with_capacity(payload.len() + 300);
    encode_target(&mut record, target)?;
    record.extend_from_slice(payload);
    let len: u32 = record.len().try_into().context("UDP proxy record too large")?;
    w.write_u32(len).await?;
    w.write_all(&record).await?;
    w.flush().await?;
    Ok(())
}

pub async fn read_udp_record<R: AsyncRead + Unpin>(r: &mut R) -> Result<(ProxyTarget, Vec<u8>)> {
    let len = r.read_u32().await? as usize;
    if len == 0 || len > 65_800 {
        bail!("invalid UDP proxy record length {len}");
    }
    let mut record = vec![0u8; len];
    r.read_exact(&mut record).await?;
    decode_target_and_payload(&record)
}

async fn write_target<W: AsyncWrite + Unpin>(w: &mut W, target: &ProxyTarget) -> Result<()> {
    match target {
        ProxyTarget::Ip(SocketAddr::V4(addr)) => {
            w.write_u8(1).await?;
            w.write_all(&addr.ip().octets()).await?;
            w.write_u16(addr.port()).await?;
        }
        ProxyTarget::Domain { host, port } => {
            let bytes = host.as_bytes();
            if bytes.is_empty() || bytes.len() > 255 {
                bail!("domain length must be 1..=255 bytes");
            }
            w.write_u8(3).await?;
            w.write_u8(bytes.len() as u8).await?;
            w.write_all(bytes).await?;
            w.write_u16(*port).await?;
        }
        ProxyTarget::Ip(SocketAddr::V6(addr)) => {
            w.write_u8(4).await?;
            w.write_all(&addr.ip().octets()).await?;
            w.write_u16(addr.port()).await?;
        }
    }
    Ok(())
}

async fn read_target_after_atyp<R: AsyncRead + Unpin>(r: &mut R, atyp: u8) -> Result<ProxyTarget> {
    Ok(match atyp {
        1 => {
            let mut ip = [0u8; 4];
            r.read_exact(&mut ip).await?;
            let port = r.read_u16().await?;
            ProxyTarget::Ip(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(ip)), port))
        }
        3 => {
            let len = r.read_u8().await? as usize;
            if len == 0 {
                bail!("empty domain in proxy target");
            }
            let mut host = vec![0u8; len];
            r.read_exact(&mut host).await?;
            let host = String::from_utf8(host).context("proxy target domain is not UTF-8")?;
            let port = r.read_u16().await?;
            ProxyTarget::Domain { host, port }
        }
        4 => {
            let mut ip = [0u8; 16];
            r.read_exact(&mut ip).await?;
            let port = r.read_u16().await?;
            ProxyTarget::Ip(SocketAddr::new(IpAddr::V6(ip.into()), port))
        }
        other => bail!("unsupported proxy address type {other}"),
    })
}

fn encode_target(out: &mut Vec<u8>, target: &ProxyTarget) -> Result<()> {
    match target {
        ProxyTarget::Ip(SocketAddr::V4(addr)) => {
            out.push(1);
            out.extend_from_slice(&addr.ip().octets());
            out.extend_from_slice(&addr.port().to_be_bytes());
        }
        ProxyTarget::Domain { host, port } => {
            let bytes = host.as_bytes();
            if bytes.is_empty() || bytes.len() > 255 {
                bail!("domain length must be 1..=255 bytes");
            }
            out.push(3);
            out.push(bytes.len() as u8);
            out.extend_from_slice(bytes);
            out.extend_from_slice(&port.to_be_bytes());
        }
        ProxyTarget::Ip(SocketAddr::V6(addr)) => {
            out.push(4);
            out.extend_from_slice(&addr.ip().octets());
            out.extend_from_slice(&addr.port().to_be_bytes());
        }
    }
    Ok(())
}

fn decode_target_and_payload(record: &[u8]) -> Result<(ProxyTarget, Vec<u8>)> {
    if record.is_empty() {
        bail!("empty UDP proxy record");
    }
    let atyp = record[0];
    let (target, used) = match atyp {
        1 => {
            if record.len() < 7 { bail!("truncated IPv4 UDP proxy record"); }
            let ip = Ipv4Addr::new(record[1], record[2], record[3], record[4]);
            let port = u16::from_be_bytes([record[5], record[6]]);
            (ProxyTarget::Ip(SocketAddr::new(IpAddr::V4(ip), port)), 7)
        }
        3 => {
            if record.len() < 2 { bail!("truncated domain UDP proxy record"); }
            let len = record[1] as usize;
            if len == 0 || record.len() < 2 + len + 2 { bail!("truncated domain UDP proxy record"); }
            let host = String::from_utf8(record[2..2+len].to_vec()).context("UDP proxy domain is not UTF-8")?;
            let p = 2 + len;
            let port = u16::from_be_bytes([record[p], record[p+1]]);
            (ProxyTarget::Domain { host, port }, p + 2)
        }
        4 => {
            if record.len() < 19 { bail!("truncated IPv6 UDP proxy record"); }
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&record[1..17]);
            let port = u16::from_be_bytes([record[17], record[18]]);
            (ProxyTarget::Ip(SocketAddr::new(IpAddr::V6(octets.into()), port)), 19)
        }
        other => bail!("unsupported UDP proxy address type {other}"),
    };
    Ok((target, record[used..].to_vec()))
}
