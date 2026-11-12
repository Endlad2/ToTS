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
use tokio::{net::UdpSocket, sync::Mutex, time::Instant};

const REFRESH_INTERVAL: Duration = Duration::from_secs(180);
const CONTROL_RTO: Duration = Duration::from_millis(500);
const CONTROL_ATTEMPTS: u8 = 4;
const NONCE_RETRIES: u8 = 2;
const TURN_RX_BUFFER: usize = 65_599;
const MAINTENANCE_METHODS: [u16; 3] = [
    stun::REFRESH_REQUEST,
    stun::CREATE_PERMISSION_REQUEST,
    stun::CHANNEL_BIND_REQUEST,
];

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
    maintenance: Mutex<Maintenance>,
    send_buffer: Mutex<Vec<u8>>,
    receive_buffer: Mutex<Vec<u8>>,
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
        let first = exchange(&socket, &unauth).await?;
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
        let mut auth = Auth {
            username: credentials.username.clone(),
            key: stun::long_term_key(&credentials.username, &realm, &credentials.password),
            realm,
            nonce,
        };
        let channel = 0x4000u16;
        let allocated =
            authenticated_exchange(&socket, &mut auth, stun::ALLOCATE_REQUEST, peer, channel)
                .await?;
        let relay = stun::parse_xor_addr(
            allocated
                .attr(stun::ATTR_XOR_RELAYED_ADDRESS)
                .ok_or_else(|| Error::Transport("TURN missing relayed address".into()))?,
            allocated.tx,
        )?;
        authenticated_exchange(
            &socket,
            &mut auth,
            stun::CREATE_PERMISSION_REQUEST,
            peer,
            channel,
        )
        .await?;
        authenticated_exchange(
            &socket,
            &mut auth,
            stun::CHANNEL_BIND_REQUEST,
            peer,
            channel,
        )
        .await?;
        let interval = refresh_interval(&allocated)?;
        Ok(Self {
            socket,
            channel,
            name: format!("turn:{}", server),
            _relay: relay,
            maintenance: Mutex::new(Maintenance {
                auth,
                peer,
                channel,
                interval,
                next_refresh: Instant::now() + interval,
                phase: 0,
                pending: None,
            }),
            send_buffer: Mutex::new(Vec::with_capacity(2048)),
            receive_buffer: Mutex::new(vec![0u8; TURN_RX_BUFFER]),
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
async fn exchange(socket: &UdpSocket, request: &[u8]) -> Result<stun::Message> {
    let sent = stun::decode(request)?;
    let mut buffer = [0u8; 2048];
    for attempt in 0..CONTROL_ATTEMPTS {
        socket.send(request).await?;
        let deadline = Instant::now() + CONTROL_RTO * (1 << attempt);
        loop {
            let n = match tokio::time::timeout_at(deadline, socket.recv(&mut buffer)).await {
                Ok(result) => result?,
                Err(_) => break,
            };
            if let Ok(response) = stun::decode(&buffer[..n]) {
                if response.tx == sent.tx
                    && (response.ty == sent.ty | 0x0100 || response.ty == sent.ty | 0x0110)
                {
                    return Ok(response);
                }
            }
        }
    }
    Err(Error::Transport("TURN transaction timeout".into()))
}

fn operation_request(method: u16, auth: &Auth, peer: SocketAddr, channel: u16) -> Result<Vec<u8>> {
    let tx = stun::transaction();
    let attrs = match method {
        stun::ALLOCATE_REQUEST => vec![stun::Attr {
            kind: stun::ATTR_REQUESTED_TRANSPORT,
            value: vec![17, 0, 0, 0],
        }],
        stun::REFRESH_REQUEST => vec![stun::attr_u32(stun::ATTR_LIFETIME, 600)],
        stun::CREATE_PERMISSION_REQUEST => {
            vec![stun::xor_addr_attr(stun::ATTR_XOR_PEER_ADDRESS, peer, tx)]
        }
        stun::CHANNEL_BIND_REQUEST => vec![
            stun::Attr {
                kind: stun::ATTR_CHANNEL_NUMBER,
                value: vec![(channel >> 8) as u8, channel as u8, 0, 0],
            },
            stun::xor_addr_attr(stun::ATTR_XOR_PEER_ADDRESS, peer, tx),
        ],
        _ => return Err(Error::Transport("invalid TURN operation".into())),
    };
    auth_request_with_tx(method, auth, attrs, tx)
}

fn update_nonce(auth: &mut Auth, response: &stun::Message) -> Result<()> {
    if response
        .attr(stun::ATTR_REALM)
        .is_some_and(|realm| realm != auth.realm.as_bytes())
    {
        return Err(Error::Transport("TURN realm changed".into()));
    }
    let nonce = response
        .attr(stun::ATTR_NONCE)
        .filter(|nonce| !nonce.is_empty() && nonce.len() <= 1024)
        .and_then(|nonce| std::str::from_utf8(nonce).ok())
        .ok_or_else(|| Error::Transport("TURN invalid replacement nonce".into()))?;
    auth.nonce = nonce.to_owned();
    Ok(())
}

async fn authenticated_exchange(
    socket: &UdpSocket,
    auth: &mut Auth,
    method: u16,
    peer: SocketAddr,
    channel: u16,
) -> Result<stun::Message> {
    for attempt in 0..=NONCE_RETRIES {
        let response = exchange(socket, &operation_request(method, auth, peer, channel)?).await?;
        match stun::error_code(&response) {
            Some(438) if attempt < NONCE_RETRIES => update_nonce(auth, &response)?,
            Some(code) => return Err(Error::Transport(format!("TURN operation error {code}"))),
            None if response.ty == method | 0x0100 => return Ok(response),
            None => return Err(Error::Transport("TURN malformed error response".into())),
        }
    }
    unreachable!()
}

fn refresh_interval(response: &stun::Message) -> Result<Duration> {
    match response.attr(stun::ATTR_LIFETIME) {
        Some(bytes) if bytes.len() == 4 => {
            let lifetime = u32::from_be_bytes(bytes.try_into().unwrap());
            if lifetime == 0 {
                return Err(Error::Transport("TURN allocation expired".into()));
            }
            Ok((Duration::from_secs(u64::from(lifetime)) / 2)
                .max(Duration::from_millis(100))
                .min(REFRESH_INTERVAL))
        }
        Some(_) => Err(Error::Transport("TURN invalid allocation lifetime".into())),
        None => Ok(REFRESH_INTERVAL),
    }
}

struct Pending {
    wire: Vec<u8>,
    tx: [u8; 12],
    method: u16,
    attempts: u8,
    nonce_retries: u8,
    retry_at: Instant,
}

struct Maintenance {
    auth: Auth,
    peer: SocketAddr,
    channel: u16,
    interval: Duration,
    next_refresh: Instant,
    phase: usize,
    pending: Option<Pending>,
}

impl Maintenance {
    fn deadline(&self) -> Instant {
        self.pending
            .as_ref()
            .map_or(self.next_refresh, |pending| pending.retry_at)
    }

    fn poll(&mut self, now: Instant) -> Result<Option<Vec<u8>>> {
        if now < self.deadline() {
            return Ok(None);
        }
        if self.pending.is_none() {
            let method = MAINTENANCE_METHODS[self.phase];
            let wire = operation_request(method, &self.auth, self.peer, self.channel)?;
            self.pending = Some(Pending {
                tx: stun::decode(&wire)?.tx,
                wire,
                method,
                attempts: 0,
                nonce_retries: 0,
                retry_at: now,
            });
        }
        let pending = self.pending.as_mut().unwrap();
        if pending.attempts >= CONTROL_ATTEMPTS {
            return Err(Error::Transport("TURN maintenance timeout".into()));
        }
        pending.retry_at = now + CONTROL_RTO * (1 << pending.attempts);
        pending.attempts += 1;
        Ok(Some(pending.wire.clone()))
    }

    fn response(&mut self, response: &stun::Message) -> Result<()> {
        let Some(pending) = &mut self.pending else {
            return Ok(());
        };
        if response.tx != pending.tx
            || (response.ty != pending.method | 0x0100 && response.ty != pending.method | 0x0110)
        {
            return Ok(());
        }
        match stun::error_code(response) {
            Some(438) if pending.nonce_retries < NONCE_RETRIES => {
                update_nonce(&mut self.auth, response)?;
                pending.wire =
                    operation_request(pending.method, &self.auth, self.peer, self.channel)?;
                pending.tx = stun::decode(&pending.wire)?.tx;
                pending.nonce_retries += 1;
                pending.attempts = 0;
                pending.retry_at = Instant::now();
                return Ok(());
            }
            Some(code) => return Err(Error::Transport(format!("TURN maintenance error {code}"))),
            None if response.ty != pending.method | 0x0100 => {
                return Err(Error::Transport(
                    "TURN malformed maintenance response".into(),
                ))
            }
            None => {}
        }
        if pending.method == stun::REFRESH_REQUEST {
            self.interval = refresh_interval(response)?;
        }
        self.pending = None;
        self.phase = (self.phase + 1) % MAINTENANCE_METHODS.len();
        self.next_refresh = Instant::now()
            + if self.phase == 0 {
                self.interval
            } else {
                Duration::ZERO
            };
        Ok(())
    }
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
        let media_len = 12usize.saturating_add(data.len());
        if media_len > u16::MAX as usize {
            return Err(Error::Transport("TURN payload too large".into()));
        }
        // Reuse one packet buffer per TURN allocation. The mutex also serializes the
        // worker's data/control-feedback sends, avoiding allocator churn while preserving
        // datagram boundaries.
        let mut packet = self.send_buffer.lock().await;
        packet.clear();
        packet.reserve(4 + media_len);
        packet.extend_from_slice(&self.channel.to_be_bytes());
        packet.extend_from_slice(&(media_len as u16).to_be_bytes());
        rtp::encode_datagram_into(data, &mut packet)?;
        self.socket.send(packet.as_slice()).await?;
        Ok(())
    }
    async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        // Keep one allocation-sized receive buffer per TURN path instead of allocating
        // ~65 KiB for every datagram on the throughput hot path. Each path has exactly
        // one receive loop, so the mutex is uncontended in normal operation.
        let mut packet = self.receive_buffer.lock().await;
        loop {
            let (control, deadline) = {
                let mut maintenance = self.maintenance.lock().await;
                let control = maintenance.poll(Instant::now())?;
                (control, maintenance.deadline())
            };
            if let Some(control) = control {
                self.socket.send(&control).await?;
            }
            let n = match tokio::time::timeout_at(deadline, self.socket.recv(packet.as_mut_slice())).await {
                Ok(result) => result?,
                Err(_) => continue,
            };
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
            if let Ok(response) = stun::decode(&packet[..n]) {
                self.maintenance.lock().await.response(&response)?;
            }
        }
    }

    async fn close(&self) -> Result<()> {
        let auth = self.maintenance.lock().await.auth.clone();
        let request = auth_request(
            stun::REFRESH_REQUEST,
            &auth,
            vec![stun::attr_u32(stun::ATTR_LIFETIME, 0)],
        )?;
        self.socket.send(&request).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FakeTurn {
        credentials: TurnCredentials,
        refreshes: Arc<AtomicUsize>,
        binds: Arc<AtomicUsize>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for FakeTurn {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn fake_turn(drop_first: bool, maintain: bool) -> FakeTurn {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let refreshes = Arc::new(AtomicUsize::new(0));
        let binds = Arc::new(AtomicUsize::new(0));
        let refreshed = refreshes.clone();
        let bound = binds.clone();
        let task = tokio::spawn(async move {
            let mut buffer = [0u8; 4096];
            let mut dropped = false;
            loop {
                let (n, client) = socket.recv_from(&mut buffer).await.unwrap();
                let Ok(request) = stun::decode(&buffer[..n]) else {
                    continue;
                };
                if drop_first && !dropped {
                    dropped = true;
                    continue;
                }
                let method = request.ty;
                let mut ty = method | 0x0100;
                let mut attrs = Vec::new();
                if request.attr(stun::ATTR_USERNAME).is_none() {
                    ty = method | 0x0110;
                    attrs = vec![
                        stun::attr_text(stun::ATTR_REALM, "test"),
                        stun::attr_text(stun::ATTR_NONCE, "nonce-a"),
                        stun::Attr {
                            kind: stun::ATTR_ERROR_CODE,
                            value: vec![0, 0, 4, 1],
                        },
                    ];
                    if drop_first {
                        let unrelated = stun::encode(ty, [99; 12], &attrs, None).unwrap();
                        socket.send_to(&unrelated, client).await.unwrap();
                    }
                } else if method == stun::ALLOCATE_REQUEST {
                    attrs.push(stun::xor_addr_attr(
                        stun::ATTR_XOR_RELAYED_ADDRESS,
                        address,
                        request.tx,
                    ));
                    attrs.push(stun::attr_u32(
                        stun::ATTR_LIFETIME,
                        if maintain { 1 } else { 600 },
                    ));
                } else if method == stun::REFRESH_REQUEST {
                    let attempt = refreshed.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        let unrelated = stun::encode(method | 0x0100, [98; 12], &[], None).unwrap();
                        socket.send_to(&unrelated, client).await.unwrap();
                        socket
                            .send_to(
                                &[
                                    0x40, 0, 0, 8, b'T', b'O', b'T', b'S', b'd', b'a', b't', b'a',
                                ],
                                client,
                            )
                            .await
                            .unwrap();
                        ty = method | 0x0110;
                        attrs = vec![
                            stun::attr_text(stun::ATTR_NONCE, "nonce-b"),
                            stun::Attr {
                                kind: stun::ATTR_ERROR_CODE,
                                value: vec![0, 0, 4, 38],
                            },
                        ];
                    } else {
                        assert_eq!(request.attr(stun::ATTR_NONCE), Some(b"nonce-b".as_slice()));
                        attrs.push(stun::attr_u32(stun::ATTR_LIFETIME, 600));
                    }
                } else if method == stun::CHANNEL_BIND_REQUEST {
                    bound.fetch_add(1, Ordering::SeqCst);
                }
                let response = stun::encode(ty, request.tx, &attrs, None).unwrap();
                socket.send_to(&response, client).await.unwrap();
                if method == stun::CHANNEL_BIND_REQUEST && bound.load(Ordering::SeqCst) >= 2 {
                    socket
                        .send_to(
                            &[
                                0x40, 0, 0, 8, b'T', b'O', b'T', b'S', b'd', b'o', b'n', b'e',
                            ],
                            client,
                        )
                        .await
                        .unwrap();
                }
            }
        });
        FakeTurn {
            credentials: TurnCredentials {
                urls: vec![format!("turn:{address}")],
                username: "test-user".into(),
                password: "test-password".into(),
            },
            refreshes,
            binds,
            task,
        }
    }

    #[tokio::test]
    async fn startup_retries_loss_and_ignores_unmatched_transactions() {
        let relay = fake_turn(true, false).await;
        let result =
            TurnUdpPath::connect(&relay.credentials, "127.0.0.1:7801".parse().unwrap()).await;
        assert!(result.is_ok());
        assert_eq!(relay.binds.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn maintenance_refreshes_permissions_and_nonce_without_stalling_data() {
        let relay = fake_turn(false, true).await;
        let path = TurnUdpPath::connect(&relay.credentials, "127.0.0.1:7801".parse().unwrap())
            .await
            .unwrap();
        let mut buffer = [0u8; 100];
        let n = tokio::time::timeout(Duration::from_secs(2), path.recv(&mut buffer))
            .await
            .expect("maintenance must start before allocation expiry")
            .unwrap();
        assert_eq!(&buffer[..n], b"TOTSdata");
        assert_eq!(relay.binds.load(Ordering::SeqCst), 1);
        let n = tokio::time::timeout(Duration::from_secs(2), path.recv(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buffer[..n], b"TOTSdone");
        assert_eq!(relay.refreshes.load(Ordering::SeqCst), 2);
        assert_eq!(relay.binds.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn lost_maintenance_transactions_retry_identical_wire_and_stop() {
        let relay = fake_turn(false, false).await;
        let path = TurnUdpPath::connect(&relay.credentials, "127.0.0.1:7801".parse().unwrap())
            .await
            .unwrap();
        let mut maintenance = path.maintenance.lock().await;
        let mut now = maintenance.next_refresh;
        let mut first = None;
        for _ in 0..CONTROL_ATTEMPTS {
            let wire = maintenance.poll(now).unwrap().unwrap();
            if let Some(first) = &first {
                assert_eq!(&wire, first);
            } else {
                first = Some(wire);
            }
            now = maintenance.deadline();
        }
        assert!(maintenance.poll(now).is_err());
    }

    #[tokio::test]
    async fn repeated_stale_nonce_challenges_are_bounded() {
        let relay = fake_turn(false, false).await;
        let path = TurnUdpPath::connect(&relay.credentials, "127.0.0.1:7801".parse().unwrap())
            .await
            .unwrap();
        let mut maintenance = path.maintenance.lock().await;
        let now = maintenance.next_refresh;
        let mut wire = maintenance.poll(now).unwrap().unwrap();
        for attempt in 0..=NONCE_RETRIES {
            let request = stun::decode(&wire).unwrap();
            let response = stun::Message {
                ty: stun::REFRESH_REQUEST | 0x0110,
                tx: request.tx,
                attrs: vec![
                    stun::attr_text(stun::ATTR_NONCE, "replacement"),
                    stun::Attr {
                        kind: stun::ATTR_ERROR_CODE,
                        value: vec![0, 0, 4, 38],
                    },
                ],
            };
            let result = maintenance.response(&response);
            if attempt == NONCE_RETRIES {
                assert!(result.is_err());
            } else {
                result.unwrap();
                wire = maintenance.poll(Instant::now()).unwrap().unwrap();
                let next = stun::decode(&wire).unwrap();
                assert_ne!(next.tx, request.tx);
                assert_eq!(next.attr(stun::ATTR_NONCE), Some(b"replacement".as_slice()));
            }
        }
    }
}
