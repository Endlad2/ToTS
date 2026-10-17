//! Минимальный TURN-клиент (RFC 5766) с long-term auth.

mod attr;
mod msg;

pub use attr::Attr;
pub use msg::{Class, Message, Method};

use std::net::SocketAddr;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use md5::Md5;
use rand::RngCore;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

const _MAGIC_COOKIE: u32 = 0x2112A442;
const HEADER_LEN: usize = 20;

#[derive(Debug, thiserror::Error)]
pub enum TurnError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("stun parse: {0}")]
    Parse(String),
    #[error("turn error {code}: {reason}")]
    Turn { code: u16, reason: String },
    #[error("authentication failed")]
    Auth,
    #[error("no free allocation: {0}")]
    Quota(String),
}

pub struct TurnAllocation {
    socket: UdpSocket,
    username: String,
    password: String,
    realm: String,
    nonce: String,
    pub relayed_addr: SocketAddr,
    pub mapped_addr: SocketAddr,
    channel: u16,
    peer: Option<SocketAddr>,
}

pub struct TurnClient {
    #[allow(dead_code)]
    server: SocketAddr,
    socket: UdpSocket,
    username: String,
    password: String,
    realm: Mutex<String>,
    nonce: Mutex<String>,
}

impl TurnClient {
    pub async fn connect(
        server: SocketAddr,
        username: String,
        password: String,
    ) -> Result<Self, TurnError> {
        let bind: SocketAddr = if server.is_ipv4() {
            "0.0.0.0:0".parse().unwrap()
        } else {
            "[::]:0".parse().unwrap()
        };
        let socket = UdpSocket::bind(bind).await?;
        socket.connect(server).await?;
        Ok(Self {
            server,
            socket,
            username,
            password,
            realm: Mutex::new(String::new()),
            nonce: Mutex::new(String::new()),
        })
    }

    pub async fn allocate(self, requested_family: u8) -> Result<TurnAllocation, TurnError> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            if attempts > 3 {
                return Err(TurnError::Auth);
            }

            let realm = self.realm.lock().await.clone();
            let nonce = self.nonce.lock().await.clone();

            let txid = random_txid();
            let mut msg = Message::new(Method::Allocate, Class::Request, txid);
            msg.add(Attr::RequestedTransport(17));
            if requested_family == 6 {
                msg.add(Attr::RequestedAddressFamily(0x02));
            }

            // Порядок важен: Username, Realm, Nonce идут ПЕРЕД MessageIntegrity,
            // а MI вычисляется по уже сериализованному сообщению без MI.
            if !realm.is_empty() && !nonce.is_empty() {
                msg.add(Attr::Username(self.username.clone()));
                msg.add(Attr::Realm(realm.clone()));
                msg.add(Attr::Nonce(nonce.clone()));
                let key = build_long_term_key(&self.username, &realm, &self.password);
                let integrity = msg.compute_integrity(&key);
                msg.add(Attr::MessageIntegrity(integrity));
            }

            let resp = self.request(msg).await?;

            match resp.class {
                Class::SuccessResponse => {
                    let relayed = resp.find_xor_relayed();
                    let mapped = resp.find_xor_mapped();
                    let (relayed_addr, mapped_addr) = match (relayed, mapped) {
                        (Some(r), Some(m)) => (r, m),
                        _ => return Err(TurnError::Parse("no XOR-RELAYED/XOR-MAPPED".into())),
                    };
                    return Ok(TurnAllocation {
                        socket: self.socket,
                        username: self.username,
                        password: self.password,
                        realm,
                        nonce,
                        relayed_addr,
                        mapped_addr,
                        channel: 0x4000,
                        peer: None,
                    });
                }
                Class::ErrorResponse => {
                    let code = resp.error_code().unwrap_or(0);
                    let reason = resp.error_reason().unwrap_or_default();
                    if code == 401 {
                        if let Some(r) = resp.find_string(0x0014) {
                            *self.realm.lock().await = r;
                        }
                        if let Some(n) = resp.find_string(0x0015) {
                            *self.nonce.lock().await = n;
                        }
                        continue;
                    }
                    if code == 486 || code == 508 {
                        return Err(TurnError::Quota(format!("{code}: {reason}")));
                    }
                    return Err(TurnError::Turn { code, reason });
                }
                _ => return Err(TurnError::Parse("unexpected class".into())),
            }
        }
    }

    async fn request(&self, msg: Message) -> Result<Message, TurnError> {
        let bytes = msg.encode();
        self.socket.send(&bytes).await?;
        let mut buf = vec![0u8; 2048];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let n = tokio::time::timeout_at(deadline, self.socket.recv(&mut buf))
                .await
                .map_err(|_| {
                    TurnError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "stun timeout",
                    ))
                })??;
            let mut slice = &buf[..n];
            if let Ok(resp) = Message::decode(&mut slice) {
                if resp.txid == msg.txid {
                    return Ok(resp);
                }
            }
        }
    }
}

impl TurnAllocation {
    pub async fn create_permission(&mut self, peer: SocketAddr) -> Result<(), TurnError> {
        self.peer = Some(peer);
        let realm = self.realm.clone();
        let nonce = self.nonce.clone();
        let txid = random_txid();
        let mut msg = Message::new(Method::CreatePermission, Class::Request, txid);
        msg.add(Attr::XorPeerAddress(peer));
        msg.add(Attr::Username(self.username.clone()));
        msg.add(Attr::Realm(realm.clone()));
        msg.add(Attr::Nonce(nonce.clone()));
        let key = build_long_term_key(&self.username, &realm, &self.password);
        let integrity = msg.compute_integrity(&key);
        msg.add(Attr::MessageIntegrity(integrity));

        let resp = self.stun_request(msg).await?;
        if resp.class == Class::SuccessResponse {
            Ok(())
        } else {
            Err(TurnError::Turn {
                code: resp.error_code().unwrap_or(0),
                reason: resp.error_reason().unwrap_or_default(),
            })
        }
    }

    pub async fn channel_bind(&mut self, peer: SocketAddr) -> Result<u16, TurnError> {
        self.peer = Some(peer);
        let channel = self.channel;
        let realm = self.realm.clone();
        let nonce = self.nonce.clone();
        let txid = random_txid();
        let mut msg = Message::new(Method::ChannelBind, Class::Request, txid);
        msg.add(Attr::ChannelNumber(channel));
        msg.add(Attr::XorPeerAddress(peer));
        msg.add(Attr::Username(self.username.clone()));
        msg.add(Attr::Realm(realm.clone()));
        msg.add(Attr::Nonce(nonce.clone()));
        let key = build_long_term_key(&self.username, &realm, &self.password);
        let integrity = msg.compute_integrity(&key);
        msg.add(Attr::MessageIntegrity(integrity));

        let resp = self.stun_request(msg).await?;
        if resp.class == Class::SuccessResponse {
            self.channel = self.channel.wrapping_add(1);
            if self.channel > 0x7FFF {
                self.channel = 0x4000;
            }
            Ok(channel)
        } else {
            Err(TurnError::Turn {
                code: resp.error_code().unwrap_or(0),
                reason: resp.error_reason().unwrap_or_default(),
            })
        }
    }

    pub async fn send_channel_data(
        &self,
        _peer: SocketAddr,
        payload: &[u8],
    ) -> Result<(), TurnError> {
        let mut buf = BytesMut::with_capacity(4 + payload.len());
        buf.put_u16(self.channel);
        buf.put_u16(payload.len() as u16);
        buf.put_slice(payload);
        self.socket.send(&buf).await?;
        Ok(())
    }

    pub async fn recv(&self) -> Result<(SocketAddr, Vec<u8>), TurnError> {
        let mut buf = vec![0u8; 65535];
        loop {
            let n = self.socket.recv(&mut buf).await?;
            let data = &buf[..n];
            if data.len() >= 4 && data[0] & 0xC0 == 0x40 {
                let len = u16::from_be_bytes([data[2], data[3]]) as usize;
                if data.len() >= 4 + len {
                    let payload = data[4..4 + len].to_vec();
                    let peer = self.peer.unwrap_or(self.relayed_addr);
                    return Ok((peer, payload));
                }
            } else if data.len() >= HEADER_LEN {
                let mut slice = data;
                if let Ok(msg) = Message::decode(&mut slice) {
                    if msg.method == Method::Data && msg.class == Class::Indication {
                        if let (Some(peer), Some(payload)) = (msg.find_xor_peer(), msg.find_data())
                        {
                            return Ok((peer, payload));
                        }
                    }
                }
            }
        }
    }

    pub async fn refresh(&mut self, lifetime: u32) -> Result<(), TurnError> {
        let realm = self.realm.clone();
        let nonce = self.nonce.clone();
        let txid = random_txid();
        let mut msg = Message::new(Method::Refresh, Class::Request, txid);
        msg.add(Attr::Lifetime(lifetime));
        msg.add(Attr::Username(self.username.clone()));
        msg.add(Attr::Realm(realm.clone()));
        msg.add(Attr::Nonce(nonce.clone()));
        let key = build_long_term_key(&self.username, &realm, &self.password);
        let integrity = msg.compute_integrity(&key);
        msg.add(Attr::MessageIntegrity(integrity));

        let resp = self.stun_request(msg).await?;
        if resp.class == Class::SuccessResponse {
            Ok(())
        } else {
            Err(TurnError::Turn {
                code: resp.error_code().unwrap_or(0),
                reason: resp.error_reason().unwrap_or_default(),
            })
        }
    }

    pub async fn deallocate(self) -> Result<(), TurnError> {
        let txid = random_txid();
        let mut msg = Message::new(Method::Refresh, Class::Request, txid);
        msg.add(Attr::Lifetime(0));
        msg.add(Attr::Username(self.username.clone()));
        msg.add(Attr::Realm(self.realm.clone()));
        msg.add(Attr::Nonce(self.nonce.clone()));
        let key = build_long_term_key(&self.username, &self.realm, &self.password);
        let integrity = msg.compute_integrity(&key);
        msg.add(Attr::MessageIntegrity(integrity));
        let bytes = msg.encode();
        let _ = self.socket.send(&bytes).await;
        Ok(())
    }

    async fn stun_request(&self, msg: Message) -> Result<Message, TurnError> {
        let bytes = msg.encode();
        self.socket.send(&bytes).await?;
        let mut buf = vec![0u8; 2048];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let n = tokio::time::timeout_at(deadline, self.socket.recv(&mut buf))
                .await
                .map_err(|_| {
                    TurnError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "stun timeout",
                    ))
                })??;
            let mut slice = &buf[..n];
            if let Ok(resp) = Message::decode(&mut slice) {
                if resp.txid == msg.txid {
                    return Ok(resp);
                }
            }
        }
    }
}

fn build_long_term_key(username: &str, realm: &str, password: &str) -> Vec<u8> {
    use md5::Digest;
    let mut hasher = Md5::new();
    hasher.update(format!("{username}:{realm}:{password}").as_bytes());
    hasher.finalize().to_vec()
}

fn random_txid() -> [u8; 12] {
    let mut txid = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut txid);
    txid
}
