use crate::{
    transport::{path::DatagramPath, rtp, stun},
    Error, Result,
};
use async_trait::async_trait;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};
use tokio::net::UdpSocket;

#[derive(Clone, Debug)]
pub struct TurnCredentials {
    pub urls: Vec<String>,
    pub username: String,
    pub password: String,
}
#[derive(Clone, Debug)]
struct Auth {
    username: String,
    realm: String,
    nonce: String,
    key: [u8; 16],
}

pub struct TurnUdpPath {
    socket: Arc<UdpSocket>,
    channel: u16,
    name: String,
    _relay: SocketAddr,
}
impl TurnUdpPath {
    pub async fn connect(credentials: &TurnCredentials, peer: SocketAddr) -> Result<Self> {
        let url = credentials
            .urls
            .iter()
            .find(|u| u.to_ascii_lowercase().starts_with("turn:"))
            .ok_or_else(|| Error::Transport("no TURN/UDP URL".into()))?;
        let server = parse_turn_udp(url)?;
        let bind: SocketAddr = if server.is_ipv4() {
            "0.0.0.0:0".parse().expect("static IPv4 bind")
        } else {
            "[::]:0".parse().expect("static IPv6 bind")
        };
        let socket = Arc::new(super::socket::bind_udp(bind)?);
        socket.connect(server).await?;
        let unauth = stun::encode(
            stun::ALLOCATE_REQUEST,
            stun::transaction(),
            &[stun::Attr {
                kind: stun::ATTR_REQUESTED_TRANSPORT,
                value: vec![17, 0, 0, 0],
            }],
            None,
        )?;
        socket.send(&unauth).await?;
        let first = recv_stun(&socket).await?;
        let realm = std::str::from_utf8(
            first
                .attr(stun::ATTR_REALM)
                .ok_or_else(|| Error::Transport("TURN response missing realm".into()))?,
        )
        .map_err(|_| Error::Transport("invalid TURN realm".into()))?
        .to_owned();
        let nonce = std::str::from_utf8(
            first
                .attr(stun::ATTR_NONCE)
                .ok_or_else(|| Error::Transport("TURN response missing nonce".into()))?,
        )
        .map_err(|_| Error::Transport("invalid TURN nonce".into()))?
        .to_owned();
        let auth = Auth {
            username: credentials.username.clone(),
            key: stun::long_term_key(&credentials.username, &realm, &credentials.password),
            realm,
            nonce,
        };
        let alloc = auth_request(
            stun::ALLOCATE_REQUEST,
            &auth,
            vec![stun::Attr {
                kind: stun::ATTR_REQUESTED_TRANSPORT,
                value: vec![17, 0, 0, 0],
            }],
        )?;
        socket.send(&alloc).await?;
        let allocated = recv_stun(&socket).await?;
        if let Some(code) = stun::error_code(&allocated) {
            return Err(Error::Transport(format!("TURN Allocate error {code}")));
        }
        let relay = stun::parse_xor_addr(
            allocated
                .attr(stun::ATTR_XOR_RELAYED_ADDRESS)
                .ok_or_else(|| Error::Transport("TURN missing relayed address".into()))?,
            allocated.tx,
        )?;
        let permission_tx = stun::transaction();
        let permission = auth_request_with_tx(
            stun::CREATE_PERMISSION_REQUEST,
            &auth,
            vec![stun::xor_addr_attr(
                stun::ATTR_XOR_PEER_ADDRESS,
                peer,
                permission_tx,
            )],
            permission_tx,
        )?;
        socket.send(&permission).await?;
        let p = recv_stun(&socket).await?;
        if let Some(code) = stun::error_code(&p) {
            return Err(Error::Transport(format!("TURN permission error {code}")));
        }
        let channel = 0x4000u16;
        let tx = stun::transaction();
        let bind = auth_request_with_tx(
            stun::CHANNEL_BIND_REQUEST,
            &auth,
            vec![
                stun::Attr {
                    kind: stun::ATTR_CHANNEL_NUMBER,
                    value: vec![(channel >> 8) as u8, channel as u8, 0, 0],
                },
                stun::xor_addr_attr(stun::ATTR_XOR_PEER_ADDRESS, peer, tx),
            ],
            tx,
        )?;
        socket.send(&bind).await?;
        let b = recv_stun(&socket).await?;
        if let Some(code) = stun::error_code(&b) {
            return Err(Error::Transport(format!("TURN channel bind error {code}")));
        }
        Ok(Self {
            socket,
            channel,
            name: format!("turn:{}", server),
            _relay: relay,
        })
    }
}

fn auth_request(ty: u16, a: &Auth, extra: Vec<stun::Attr>) -> Result<Vec<u8>> {
    auth_request_with_tx(ty, a, extra, stun::transaction())
}
fn auth_request_with_tx(
    ty: u16,
    a: &Auth,
    mut extra: Vec<stun::Attr>,
    tx: [u8; 12],
) -> Result<Vec<u8>> {
    let mut attrs = vec![
        stun::attr_text(stun::ATTR_USERNAME, &a.username),
        stun::attr_text(stun::ATTR_REALM, &a.realm),
        stun::attr_text(stun::ATTR_NONCE, &a.nonce),
    ];
    attrs.append(&mut extra);
    stun::encode(ty, tx, &attrs, Some(&a.key))
}
async fn recv_stun(socket: &UdpSocket) -> Result<stun::Message> {
    let mut buf = [0u8; 2048];
    let n = tokio::time::timeout(Duration::from_secs(4), socket.recv(&mut buf))
        .await
        .map_err(|_| Error::Transport("TURN timeout".into()))??;
    stun::decode(&buf[..n])
}
fn parse_turn_udp(value: &str) -> Result<SocketAddr> {
    let raw = value.trim();
    if raw.to_ascii_lowercase().starts_with("turns:") {
        return Err(Error::Transport(
            "TURN/TLS is not enabled in this build".into(),
        ));
    }
    let body = raw
        .strip_prefix("turn:")
        .or_else(|| raw.strip_prefix("TURN:"))
        .unwrap_or(raw);
    let authority = body.split('?').next().unwrap_or(body);
    if body.to_ascii_lowercase().contains("transport=tcp") {
        return Err(Error::Transport(
            "TURN/TCP URL cannot be used by UDP path".into(),
        ));
    }
    let with_port = if authority.contains(':') {
        authority.to_owned()
    } else {
        format!("{authority}:3478")
    };
    with_port
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| Error::Transport("TURN DNS produced no address".into()))
}

#[async_trait]
impl DatagramPath for TurnUdpPath {
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
        let data = rtp::encode_datagram(data)?;
        if data.len() > u16::MAX as usize {
            return Err(Error::Transport("TURN payload too large".into()));
        }
        let mut packet = Vec::with_capacity(4 + data.len());
        packet.extend_from_slice(&self.channel.to_be_bytes());
        packet.extend_from_slice(&(data.len() as u16).to_be_bytes());
        packet.extend_from_slice(&data);
        self.socket.send(&packet).await?;
        Ok(())
    }
    async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        let mut packet = vec![0u8; buf.len() + 64];
        loop {
            let n = self.socket.recv(&mut packet).await?;
            if n >= 4 {
                let ch = u16::from_be_bytes([packet[0], packet[1]]);
                let len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
                if ch == self.channel && 4 + len <= n {
                    let payload = &packet[4..4 + len];
                    let payload = rtp::decode(payload).unwrap_or(payload);
                    if payload.len() > buf.len() {
                        return Err(Error::Transport("receive buffer too small".into()));
                    }
                    buf[..payload.len()].copy_from_slice(payload);
                    return Ok(payload.len());
                }
            }
        }
    }
}
