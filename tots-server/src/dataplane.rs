use crate::{config::Args, db::Db};
use anyhow::{Context, Result};
use chrono::Utc;
use dashmap::DashMap;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Weak,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UdpSocket,
    sync::{mpsc, Mutex, OwnedSemaphorePermit, Semaphore},
};
use tots_core::{
    config::ConnectionConfig,
    crypto::PacketCrypto,
    flow::{FlowManager, FlowScheduler, Reliability},
    protocol::peek_meta,
    recovery::{RecoveryController, RecoveryStats},
    session::{DecodedPacket, PreparedFrame, SessionEngine},
    transport::rtp,
};
use uuid::Uuid;

const SERVER_ACK_QUEUE: usize = 4096;
const SERVER_ACK_BATCH_MAX: usize = 32;
const SERVER_ACK_BATCH_DELAY: Duration = Duration::from_millis(2);
const TO_TUN_QUEUE: usize = 16_384;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PathKind {
    Direct,
    Turn,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Route {
    path: PathKind,
    addr: SocketAddr,
    media: bool,
}

struct Peer {
    device_id: Uuid,
    auth_fingerprint: [u8; 32],
    authorized: AtomicBool,
    engine: Arc<SessionEngine>,
    route: Mutex<Route>,
    virtual_ip: Ipv4Addr,
    last_seen_ms: AtomicU64,
    recovery: Mutex<RecoveryController>,
    outbound: mpsc::Sender<OutboundPacket>,
    ack_outbound: mpsc::Sender<AckRequest>,
    migrated: AtomicBool,
    ack_ranges: AtomicBool,
    _slot: OwnedSemaphorePermit,
}


type PeerMap = Arc<DashMap<(Uuid, u64), Arc<Peer>>>;
type DeviceSessions = Arc<DashMap<Uuid, Arc<Mutex<Vec<u64>>>>>;
type DeviceSchedulers = Arc<DashMap<Uuid, Arc<Mutex<FlowScheduler>>>>;

struct SelectedPeer {
    assoc_id: u16,
    session_id: u64,
    peer: Arc<Peer>,
    scheduler: Arc<Mutex<FlowScheduler>>,
}

struct OutboundPacket {
    assoc_id: u16,
    bytes: Vec<u8>,
    scheduler: Arc<Mutex<FlowScheduler>>,
    enqueued_at: tokio::time::Instant,
}

#[derive(Clone)]
struct AckRequest {
    packet_id: u32,
    route: Route,
}

struct CachedKey {
    password: String,
    crypto: Arc<PacketCrypto>,
}

struct KeyCache {
    devices: Mutex<HashMap<Uuid, Arc<Mutex<Option<CachedKey>>>>>,
    derivations: Semaphore,
}

impl Default for KeyCache {
    fn default() -> Self {
        Self {
            devices: Mutex::new(HashMap::new()),
            derivations: Semaphore::new(2),
        }
    }
}

impl KeyCache {
    async fn get(&self, cfg: &ConnectionConfig) -> Result<Arc<PacketCrypto>> {
        let key = {
            let mut devices = self.devices.lock().await;
            if !devices.contains_key(&cfg.device_id) && devices.len() >= 1024 {
                let idle = devices
                    .iter()
                    .find(|(_, key)| Arc::strong_count(key) == 1)
                    .map(|(id, _)| *id);
                let Some(idle) = idle else {
                    anyhow::bail!("device key cache is busy");
                };
                devices.remove(&idle);
            }
            devices
                .entry(cfg.device_id)
                .or_insert_with(|| Arc::new(Mutex::new(None)))
                .clone()
        };
        let mut cached = key.lock().await;
        if let Some(cached) = cached.as_ref().filter(|key| key.password == cfg.password) {
            return Ok(cached.crypto.clone());
        }
        let _permit = self.derivations.acquire().await?;
        let password = cfg.password.clone();
        let device = cfg.device_id;
        let crypto = Arc::new(
            tokio::task::spawn_blocking(move || PacketCrypto::derive(&password, device)).await??,
        );
        *cached = Some(CachedKey {
            password: cfg.password.clone(),
            crypto: crypto.clone(),
        });
        Ok(crypto)
    }
}

struct PeerRegistry {
    db: Db,
    peers: PeerMap,
    sessions_by_device: DeviceSessions,
    keys: KeyCache,
    slots: Arc<Semaphore>,
    registration: Mutex<()>,
    direct: Arc<UdpSocket>,
    turn: Arc<UdpSocket>,
}

impl PeerRegistry {
    fn new(db: Db, direct: Arc<UdpSocket>, turn: Arc<UdpSocket>) -> Self {
        Self {
            db,
            peers: Arc::new(DashMap::new()),
            sessions_by_device: Arc::new(DashMap::new()),
            keys: KeyCache::default(),
            slots: Arc::new(Semaphore::new(4096)),
            registration: Mutex::new(()),
            direct,
            turn,
        }
    }

    async fn get_peer(
        &self,
        id: Uuid,
        session: u64,
        data: &[u8],
    ) -> Result<Option<(Arc<Peer>, Option<DecodedPacket>)>> {
        let key = (id, session);
        if let Some(peer) = self.peers.get(&key).map(|peer| peer.value().clone()) {
            if !peer.authorized.load(Ordering::Acquire) {
                return Ok(None);
            }
            let decoded = peer.engine.decapsulate(data)?;
            return Ok(Some((peer, decoded)));
        }
        let Some(device) = self.db.get(id).await? else {
            return Ok(None);
        };
        if device.revoked || device.expires_at <= Utc::now() {
            return Ok(None);
        }
        let auth_fingerprint = device_auth_fingerprint(&device);
        let cfg = ConnectionConfig {
            version: 1,
            peer: "0.0.0.0".into(),
            webrtc_port: 1,
            turn_port: 1,
            password: device.password,
            device_id: id,
            provider: device.provider,
            provider_data: device.provider_data,
            mtu: 1200,
        };
        let virtual_ip = cfg.virtual_ipv4();
        let crypto = self.keys.get(&cfg).await?;
        let engine = Arc::new(SessionEngine::with_session_and_crypto(
            cfg,
            session,
            (*crypto).clone(),
        )?);
        // An envelope's clear session ID must never allocate a peer before AEAD verification.
        let decoded = engine.decapsulate(data)?;
        // Derivation may outlive a revoke or config replacement; check again before registration.
        let Some(current) = self.db.get(id).await? else {
            return Ok(None);
        };
        if current.revoked
            || current.expires_at <= Utc::now()
            || device_auth_fingerprint(&current) != auth_fingerprint
        {
            return Ok(None);
        }
        let _registration = self.registration.lock().await;
        let list = self
            .sessions_by_device
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(Vec::new())))
            .value()
            .clone();
        let mut sessions = list.lock().await;
        if let Some(peer) = self.peers.get(&key).map(|peer| peer.value().clone()) {
            let decoded = peer.engine.decapsulate(data)?;
            return Ok(Some((peer, decoded)));
        }
        if sessions.len() >= 256 {
            return Ok(None);
        }
        let Ok(slot) = self.slots.clone().try_acquire_owned() else {
            return Ok(None);
        };
        let (outbound, receiver) = mpsc::channel(128);
        let (ack_outbound, ack_receiver) = mpsc::channel(SERVER_ACK_QUEUE);
        let peer = Arc::new(Peer {
            device_id: id,
            auth_fingerprint,
            authorized: AtomicBool::new(true),
            engine,
            route: Mutex::new(Route {
                path: PathKind::Direct,
                addr: "0.0.0.0:0".parse().unwrap(),
                media: false,
            }),
            virtual_ip,
            last_seen_ms: AtomicU64::new(now_millis()),
            recovery: Mutex::new(RecoveryController::default()),
            outbound,
            ack_outbound,
            migrated: AtomicBool::new(false),
            ack_ranges: AtomicBool::new(false),
            _slot: slot,
        });
        self.peers.insert(key, peer.clone());
        sessions.push(session);
        sessions.sort_unstable();
        tracing::info!(device_id = %id, session_id = session, virtual_ip = %virtual_ip, "worker session registered");
        tokio::spawn(peer_send_loop(
            Arc::downgrade(&peer),
            self.direct.clone(),
            self.turn.clone(),
            receiver,
        ));
        tokio::spawn(peer_ack_loop(
            Arc::downgrade(&peer),
            self.direct.clone(),
            self.turn.clone(),
            ack_receiver,
        ));
        Ok(Some((peer, decoded)))
    }

    async fn refresh_auth(
        &self,
        schedulers: &DeviceSchedulers,
        active_by_ip: &DashMap<Ipv4Addr, Uuid>,
    ) {
        let devices: Vec<_> = self
            .sessions_by_device
            .iter()
            .map(|entry| *entry.key())
            .collect();
        for id in devices {
            let current = match self.db.get(id).await {
                Ok(Some(device)) if !device.revoked && device.expires_at > Utc::now() => {
                    Some(device_auth_fingerprint(&device))
                }
                Ok(_) => None,
                Err(_) => {
                    tracing::warn!(device_id = %id, "worker authorization refresh failed");
                    None
                }
            };
            let _registration = self.registration.lock().await;
            let Some(list) = self
                .sessions_by_device
                .get(&id)
                .map(|entry| entry.value().clone())
            else {
                continue;
            };
            let mut sessions = list.lock().await;
            let mut virtual_ip = None;
            let now = now_millis();
            sessions.retain(|session| {
                let key = (id, *session);
                let Some(peer) = self.peers.get(&key).map(|entry| entry.value().clone()) else {
                    return false;
                };
                virtual_ip = Some(peer.virtual_ip);
                let keep = current == Some(peer.auth_fingerprint)
                    && peer.authorized.load(Ordering::Acquire)
                    && now.saturating_sub(peer.last_seen_ms.load(Ordering::Relaxed)) <= 60_000;
                if !keep {
                    // Stop already-selected ingress/egress references as well as future lookups.
                    peer.authorized.store(false, Ordering::Release);
                    self.peers.remove(&key);
                    tracing::info!(device_id = %id, session_id = session, "worker session invalidated");
                }
                keep
            });
            let empty = sessions.is_empty();
            drop(sessions);
            if empty {
                self.sessions_by_device.remove(&id);
                schedulers.remove(&id);
                if let Some(ip) = virtual_ip {
                    active_by_ip.remove_if(&ip, |_, device| *device == id);
                }
            }
        }
    }
}

fn device_auth_fingerprint(device: &crate::db::Device) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update((device.password.len() as u64).to_be_bytes());
    hash.update(device.password.as_bytes());
    hash.update(device.provider.as_str().as_bytes());
    hash.update(device.provider_data.to_string().as_bytes());
    hash.finalize().into()
}

pub async fn run(args: Args, db: Db) -> Result<()> {
    let direct = Arc::new(tots_core::transport::socket::bind_udp(args.webrtc_listen)?);
    let turn = Arc::new(tots_core::transport::socket::bind_udp(args.turn_listen)?);
    let registry = Arc::new(PeerRegistry::new(db, direct.clone(), turn.clone()));
    let peers = registry.peers.clone();
    let sessions_by_device = registry.sessions_by_device.clone();
    let schedulers: DeviceSchedulers = Arc::new(DashMap::new());
    // One virtual IP maps to a device; a device may now have many worker sessions.
    let active_by_ip: Arc<DashMap<Ipv4Addr, Uuid>> = Arc::new(DashMap::new());
    let (to_tun_tx, mut to_tun_rx) = mpsc::channel::<Vec<u8>>(TO_TUN_QUEUE);
    let (from_tun_tx, from_tun_rx) = mpsc::channel::<Vec<u8>>(4096);

    if !args.no_tun {
        let mut c = tun::Configuration::default();
        c.tun_name(&args.tun_name)
            .address((10, 77, 0, 1))
            .netmask((255, 255, 0, 0))
            .mtu(1400)
            .up();
        let dev = tun::create_as_async(&c).context("create server TUN")?;
        let (mut rd, mut wr) = tokio::io::split(dev);
        let tx = from_tun_tx.clone();
        tokio::spawn(async move {
            let mut b = vec![0u8; 65535];
            loop {
                match rd.read(&mut b).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if tx.send(b[..n].to_vec()).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        tokio::spawn(async move {
            while let Some(p) = to_tun_rx.recv().await {
                if wr.write_all(&p).await.is_err() {
                    break;
                }
            }
        });
    } else {
        tokio::spawn(async move { while to_tun_rx.recv().await.is_some() {} });
    }

    let admissions = Arc::new(Semaphore::new(64));
    for (kind, sock) in [
        (PathKind::Direct, direct.clone()),
        (PathKind::Turn, turn.clone()),
    ] {
        let registry = registry.clone();
        let active_by_ip = active_by_ip.clone();
        let tx = to_tun_tx.clone();
        let admissions = admissions.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65535];
            loop {
                let Ok((n, addr)) = sock.recv_from(&mut buf).await else {
                    continue;
                };
                let data = &buf[..n];
                let media_payload = rtp::decode(data);
                let media = media_payload.is_some();
                let data = media_payload.unwrap_or(data);
                let Ok(meta) = peek_meta(data) else {
                    continue;
                };
                let known = registry
                    .peers
                    .get(&(meta.device_id, meta.session_id))
                    .map(|peer| peer.value().clone());
                if let Some(peer) = known {
                    if !peer.authorized.load(Ordering::Acquire) {
                        continue;
                    }
                    let Ok(decoded) = peer.engine.decapsulate(data) else {
                        continue;
                    };
                    handle_received(
                        &peer,
                        decoded,
                        &sock,
                        Route {
                            path: kind,
                            addr,
                            media,
                        },
                        &tx,
                        &active_by_ip,
                    )
                    .await;
                    continue;
                }
                // Admission may read SQLite and derive a key; it never stalls known workers.
                let Ok(permit) = admissions.clone().try_acquire_owned() else {
                    continue;
                };
                let registry = registry.clone();
                let sock = sock.clone();
                let tx = tx.clone();
                let active_by_ip = active_by_ip.clone();
                let data = data.to_vec();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Ok(Some((peer, decoded))) = registry
                        .get_peer(meta.device_id, meta.session_id, &data)
                        .await
                    {
                        handle_received(
                            &peer,
                            decoded,
                            &sock,
                            Route {
                                path: kind,
                                addr,
                                media,
                            },
                            &tx,
                            &active_by_ip,
                        )
                        .await;
                    }
                });
            }
        });
    }
    // Refresh authorization once per device, independently of packet and heartbeat traffic.
    {
        let registry = registry.clone();
        let schedulers = schedulers.clone();
        let active_by_ip = active_by_ip.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                registry.refresh_auth(&schedulers, &active_by_ip).await;
                let devices: Vec<_> = registry
                    .sessions_by_device
                    .iter()
                    .map(|entry| (*entry.key(), entry.value().clone()))
                    .collect();
                for (device, sessions) in devices {
                    let sessions = sessions.lock().await.clone();
                    let mut workers = 0usize;
                    let mut cwnd_sum = 0usize;
                    let mut bytes_in_flight_sum = 0usize;
                    let mut queued_packets = 0usize;
                    let mut recovery_stats = RecoveryStats::default();
                    let mut rtt_workers = 0usize;
                    let mut replay_rejections_sum = 0u64;
                    let mut best_effort_samples = 0usize;
                    let mut srtt_min: Option<Duration> = None;
                    let mut srtt_max: Option<Duration> = None;
                    let now = now_millis();
                    for session in sessions {
                        let Some(peer) = registry
                            .peers
                            .get(&(device, session))
                            .map(|entry| entry.value().clone())
                        else {
                            continue;
                        };
                        if !peer.authorized.load(Ordering::Acquire)
                            || now.saturating_sub(peer.last_seen_ms.load(Ordering::Relaxed))
                                > 30_000
                        {
                            continue;
                        }
                        workers += 1;
                        replay_rejections_sum =
                            replay_rejections_sum.saturating_add(peer.engine.replay_rejections());
                        queued_packets += peer
                            .outbound
                            .max_capacity()
                            .saturating_sub(peer.outbound.capacity());
                        let recovery = peer.recovery.lock().await;
                        let stats = recovery.stats();
                        recovery_stats.sent_packets += stats.sent_packets;
                        recovery_stats.acked_packets += stats.acked_packets;
                        recovery_stats.packet_threshold_losses += stats.packet_threshold_losses;
                        recovery_stats.time_threshold_losses += stats.time_threshold_losses;
                        recovery_stats.retransmits += stats.retransmits;
                        recovery_stats.late_acked_packets += stats.late_acked_packets;
                        recovery_stats.pto_expirations += stats.pto_expirations;
                        recovery_stats.pto_retransmits += stats.pto_retransmits;
                        recovery_stats.best_effort_evictions += stats.best_effort_evictions;
                        best_effort_samples += recovery.best_effort_samples();
                        cwnd_sum += recovery.cwnd();
                        bytes_in_flight_sum += recovery.bytes_in_flight();
                        if let Some(srtt) = recovery.srtt() {
                            rtt_workers += 1;
                            srtt_min = Some(srtt_min.map_or(srtt, |old| old.min(srtt)));
                            srtt_max = Some(srtt_max.map_or(srtt, |old| old.max(srtt)));
                        }
                    }
                    if workers != 0 {
                        tracing::info!(
                            device_id = %device,
                            workers, cwnd_sum, bytes_in_flight_sum, queued_packets, rtt_workers, replay_rejections_sum,
                            best_effort_samples, best_effort_evictions = recovery_stats.best_effort_evictions,
                            recovery_sent_packets = recovery_stats.sent_packets,
                            recovery_acked_packets = recovery_stats.acked_packets,
                            recovery_packet_threshold_losses = recovery_stats.packet_threshold_losses,
                            recovery_time_threshold_losses = recovery_stats.time_threshold_losses,
                            recovery_retransmits = recovery_stats.retransmits,
                            recovery_late_acked_packets = recovery_stats.late_acked_packets,
                            recovery_pto_expirations = recovery_stats.pto_expirations,
                            recovery_pto_retransmits = recovery_stats.pto_retransmits,
                            srtt_min_ms = srtt_min.map_or(0, |rtt| rtt.as_millis() as u64),
                            srtt_max_ms = srtt_max.map_or(0, |rtt| rtt.as_millis() as u64),
                            "transport summary"
                        );
                    }
                }
            }
        });
    }
    route_from_tun(
        from_tun_rx,
        &active_by_ip,
        &peers,
        &sessions_by_device,
        &schedulers,
    )
    .await;
    Ok(())
}

async fn handle_received(
    peer: &Peer,
    decoded: Option<DecodedPacket>,
    sock: &UdpSocket,
    incoming: Route,
    tx: &mpsc::Sender<Vec<u8>>,
    active_by_ip: &DashMap<Ipv4Addr, Uuid>,
) {
    if !peer.authorized.load(Ordering::Acquire) {
        return;
    }
    peer.last_seen_ms.store(now_millis(), Ordering::Relaxed);
    active_by_ip.insert(peer.virtual_ip, peer.device_id);
    let addr = incoming.addr;
    let media = incoming.media;
    {
        let mut route = peer.route.lock().await;
        if *route != incoming && route.addr.port() != 0 {
            peer.migrated.store(true, Ordering::Release);
            tracing::info!(device_id = %peer.device_id, session_id = peer.engine.session_id(), path = ?incoming.path, "worker route migrated");
        }
        *route = incoming.clone();
    }
    match decoded {
        Some(DecodedPacket::Ip {
            packet_id, bytes, ..
        }) => {
            // Never stall the socket receive loop on a TUN write or an ACK transport send.
            // A bounded TUN queue absorbs bursts; overload drops are preferable to blocking
            // every worker sharing this UDP socket. TCP/QUIC inside the tunnel can recover.
            if tx.try_send(bytes).is_ok() {
                let _ = peer.ack_outbound.try_send(AckRequest {
                    packet_id,
                    route: incoming.clone(),
                });
            }
        }
        Some(DecodedPacket::Duplicate { packet_id }) => {
            let _ = peer.ack_outbound.try_send(AckRequest {
                packet_id,
                route: incoming.clone(),
            });
        }
        Some(DecodedPacket::Ack { packet_id }) => {
            peer.recovery.lock().await.on_ack(packet_id);
        }
        Some(DecodedPacket::AckRanges { ranges }) => {
            // Only authenticated decoding for this Peer can advertise range support.
            peer.ack_ranges.store(true, Ordering::Release);
            peer.recovery.lock().await.on_ack_ranges(&ranges);
        }
        Some(DecodedPacket::Probe { token, sent_micros }) => {
            if let Ok(reply) = peer.engine.make_probe_ack(token, sent_micros) {
                let _ = send_reply(sock, &reply, addr, media).await;
            }
        }
        _ => {}
    }
}

async fn peer_ack_loop(
    weak: Weak<Peer>,
    direct: Arc<UdpSocket>,
    turn: Arc<UdpSocket>,
    mut receiver: mpsc::Receiver<AckRequest>,
) {
    let mut carry: Option<AckRequest> = None;
    let mut packet_ids = Vec::with_capacity(SERVER_ACK_BATCH_MAX);
    loop {
        let first = match carry.take() {
            Some(request) => request,
            None => match receiver.recv().await {
                Some(request) => request,
                None => return,
            },
        };
        let route = first.route.clone();
        packet_ids.push(first.packet_id);
        let deadline = tokio::time::Instant::now() + SERVER_ACK_BATCH_DELAY;
        while packet_ids.len() < SERVER_ACK_BATCH_MAX {
            match tokio::time::timeout_at(deadline, receiver.recv()).await {
                Ok(Some(request)) if request.route == route => packet_ids.push(request.packet_id),
                Ok(Some(request)) => {
                    carry = Some(request);
                    break;
                }
                Ok(None) | Err(_) => break,
            }
        }
        let Some(peer) = weak.upgrade() else {
            return;
        };
        if !peer.authorized.load(Ordering::Acquire) {
            return;
        }
        let socket = match route.path {
            PathKind::Direct => direct.as_ref(),
            PathKind::Turn => turn.as_ref(),
        };
        if peer.ack_ranges.load(Ordering::Acquire) {
            if let Ok(ack) = peer.engine.make_ack_batch(&packet_ids) {
                let _ = send_reply(socket, &ack, route.addr, route.media).await;
            }
        } else {
            // Preserve v1 compatibility until this authenticated session advertises ACK ranges.
            for &packet_id in &packet_ids {
                if let Ok(ack) = peer.engine.make_legacy_ack(packet_id) {
                    let _ = send_reply(socket, &ack, route.addr, route.media).await;
                }
            }
        }
        packet_ids.clear();
    }
}

async fn route_from_tun(
    mut from_tun_rx: mpsc::Receiver<Vec<u8>>,
    active_by_ip: &DashMap<Ipv4Addr, Uuid>,
    peers: &PeerMap,
    sessions_by_device: &DeviceSessions,
    schedulers: &DeviceSchedulers,
) {
    while let Some(packet) = from_tun_rx.recv().await {
        let Some(dst) = ipv4_dst(&packet) else {
            continue;
        };
        let Some(device_id) = active_by_ip.get(&dst).map(|v| *v.value()) else {
            continue;
        };
        let Some(SelectedPeer {
            assoc_id,
            session_id,
            peer,
            scheduler,
        }) = select_peer(device_id, &packet, peers, sessions_by_device, schedulers).await
        else {
            continue;
        };
        let outbound = OutboundPacket {
            assoc_id,
            bytes: packet,
            scheduler: scheduler.clone(),
            enqueued_at: tokio::time::Instant::now(),
        };
        if peer.outbound.try_send(outbound).is_err() {
            scheduler.lock().await.mark_congested(session_id);
        }
    }
}

async fn send_frames(
    peer: &Peer,
    frames: &[PreparedFrame],
    reliability: Reliability,
    direct: &UdpSocket,
    turn: &UdpSocket,
) -> Result<()> {
    let route = peer.route.lock().await.clone();
    let socket = match route.path {
        PathKind::Direct => direct,
        PathKind::Turn => turn,
    };
    for frame in frames {
        if !peer.authorized.load(Ordering::Acquire) {
            anyhow::bail!("worker authorization expired");
        }
        let delay = peer
            .recovery
            .lock()
            .await
            .reserve_pacing_for(frame.wire_len(), reliability);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if !peer.authorized.load(Ordering::Acquire) {
            anyhow::bail!("worker authorization expired");
        }
        let datagram = peer.engine.seal_prepared(frame)?;
        send_reply(socket, &datagram, route.addr, route.media).await?;
    }
    Ok(())
}

async fn recover_peer(peer: &Peer, direct: &UdpSocket, turn: &UdpSocket) {
    if !peer.authorized.load(Ordering::Acquire) {
        return;
    }
    let retries = if peer.migrated.swap(false, Ordering::AcqRel) {
        peer.recovery.lock().await.on_path_migration()
    } else {
        peer.recovery.lock().await.poll_losses()
    };
    for retry in retries {
        if let Ok(frames) =
            peer.engine
                .prepare_ip_with_id(retry.assoc_id, retry.packet_id, &retry.payload)
        {
            let _ = send_frames(peer, &frames, Reliability::Reliable, direct, turn).await;
        }
    }
}

async fn peer_send_loop(
    weak: Weak<Peer>,
    direct: Arc<UdpSocket>,
    turn: Arc<UdpSocket>,
    mut receiver: mpsc::Receiver<OutboundPacket>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(20));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let outbound = tokio::select! {
            packet = receiver.recv() => match packet { Some(packet) => packet, None => return },
            _ = tick.tick() => {
                let Some(peer) = weak.upgrade() else { return; };
                recover_peer(&peer, &direct, &turn).await;
                continue;
            }
        };
        let Some(peer) = weak.upgrade() else {
            return;
        };
        if !peer.authorized.load(Ordering::Acquire) {
            return;
        }
        let reliability = FlowManager::classify(&outbound.bytes);
        if reliability == tots_core::flow::Reliability::Unreliable
            && outbound.enqueued_at.elapsed() > Duration::from_millis(750)
        {
            continue;
        }
        let Ok((packet_id, frames)) = peer.engine.prepare_ip(outbound.assoc_id, &outbound.bytes)
        else {
            continue;
        };
        let bytes = frames.iter().map(PreparedFrame::wire_len).sum();
        let mut congestion_marked = false;
        loop {
            if !peer.authorized.load(Ordering::Acquire) {
                return;
            }
            if peer.migrated.load(Ordering::Acquire) {
                recover_peer(&peer, &direct, &turn).await;
            }
            if peer.recovery.lock().await.can_send_for(bytes, reliability) {
                break;
            }
            if !congestion_marked {
                outbound
                    .scheduler
                    .lock()
                    .await
                    .mark_congested(peer.engine.session_id());
                congestion_marked = true;
            }
            if now_millis().saturating_sub(peer.last_seen_ms.load(Ordering::Relaxed)) > 60_000 {
                return;
            }
            // Loss processing must continue while this worker is out of congestion credit.
            tokio::select! {
                _ = tick.tick() => recover_peer(&peer, &direct, &turn).await,
                _ = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
        }
        if send_frames(&peer, &frames, reliability, &direct, &turn)
            .await
            .is_ok()
        {
            peer.recovery.lock().await.on_sent(
                packet_id,
                outbound.assoc_id,
                outbound.bytes,
                bytes,
                reliability,
            );
        } else {
            outbound
                .scheduler
                .lock()
                .await
                .mark_congested(peer.engine.session_id());
        }
    }
}
async fn select_peer(
    device: Uuid,
    packet: &[u8],
    peers: &PeerMap,
    sessions_by_device: &DeviceSessions,
    schedulers: &DeviceSchedulers,
) -> Option<SelectedPeer> {
    let sessions = sessions_by_device.get(&device)?.value().clone();
    let sessions = sessions.lock().await.clone();
    let now = now_millis();
    let latency_sensitive = FlowScheduler::is_latency_sensitive(packet);
    let reliability = FlowManager::classify(packet);
    let estimated_wire_bytes = packet.len().saturating_add(256);
    let mut active = Vec::new();
    let mut sendable = Vec::new();

    for session in sessions {
        let Some(peer) = peers
            .get(&(device, session))
            .map(|entry| entry.value().clone())
        else {
            continue;
        };
        if !peer.authorized.load(Ordering::Acquire)
            || now.saturating_sub(peer.last_seen_ms.load(Ordering::Relaxed)) > 30_000
        {
            continue;
        }
        if peer.outbound.capacity() == 0 {
            continue;
        }
        let route_bound = match peer.route.try_lock() {
            Ok(route) => route.addr.port() != 0,
            Err(_) => peer.route.lock().await.addr.port() != 0,
        };
        if !route_bound {
            continue;
        }
        active.push(session);
        if !latency_sensitive
            && (reliability == Reliability::Unreliable
                || peer
                    .recovery
                    .lock()
                    .await
                    .can_send_for(estimated_wire_bytes, reliability))
        {
            sendable.push(session);
        }
    }

    if active.is_empty() {
        return None;
    }
    let candidates = if !latency_sensitive && !sendable.is_empty() {
        &sendable
    } else {
        &active
    };
    let scheduler = schedulers
        .entry(device)
        .or_insert_with(|| Arc::new(Mutex::new(FlowScheduler::default())))
        .value()
        .clone();
    let decision = scheduler
        .lock()
        .await
        .select_with_preferred(packet, &active, candidates)?;
    let peer = peers
        .get(&(device, decision.worker_id))
        .map(|entry| entry.value().clone())?;
    Some(SelectedPeer {
        assoc_id: decision.assoc_id,
        session_id: decision.worker_id,
        peer,
        scheduler,
    })
}

async fn send_reply(
    socket: &UdpSocket,
    packet: &[u8],
    addr: SocketAddr,
    media: bool,
) -> Result<()> {
    if media {
        socket.send_to(&rtp::encode_datagram(packet)?, addr).await?;
    } else {
        socket.send_to(packet, addr).await?;
    }
    Ok(())
}

fn ipv4_dst(packet: &[u8]) -> Option<Ipv4Addr> {
    if packet.len() < 20 || packet[0] >> 4 != 4 {
        return None;
    }
    Some(Ipv4Addr::new(
        packet[16], packet[17], packet[18], packet[19],
    ))
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ConnectionConfig {
        ConnectionConfig {
            version: 1,
            peer: "127.0.0.1".into(),
            webrtc_port: 1,
            turn_port: 2,
            password: "test-password".into(),
            device_id: Uuid::new_v4(),
            provider: tots_core::config::ProviderName::Vk,
            provider_data: serde_json::json!({"vk_call_links":["test"]}),
            mtu: 1200,
        }
    }

    #[tokio::test]
    async fn concurrent_worker_admission_reuses_one_device_key() {
        let keys = Arc::new(KeyCache::default());
        let cfg = config();
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..20 {
            let keys = keys.clone();
            let cfg = cfg.clone();
            tasks.spawn(async move { keys.get(&cfg).await.unwrap() });
        }
        let first = tasks.join_next().await.unwrap().unwrap();
        while let Some(key) = tasks.join_next().await {
            assert!(Arc::ptr_eq(&first, &key.unwrap()));
        }
    }

    async fn established_device() -> (
        std::path::PathBuf,
        ConnectionConfig,
        PeerRegistry,
        Vec<Arc<Peer>>,
    ) {
        let path = std::env::temp_dir().join(format!("tots-auth-refresh-{}.db", Uuid::new_v4()));
        let db = Db::open(&path).await.unwrap();
        let cfg = config();
        db.upsert(
            cfg.device_id,
            cfg.password.clone(),
            cfg.provider,
            cfg.provider_data.clone(),
            1,
        )
        .await
        .unwrap();
        let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let registry = PeerRegistry::new(db, sock.clone(), sock);
        let key = registry.keys.get(&cfg).await.unwrap();
        let mut peers = Vec::new();
        for session in 1..=2 {
            let engine =
                SessionEngine::with_session_and_crypto(cfg.clone(), session, (*key).clone())
                    .unwrap();
            let probe = engine.make_probe(1, 2).unwrap();
            let (peer, _) = registry
                .get_peer(cfg.device_id, session, &probe)
                .await
                .unwrap()
                .unwrap();
            peer.last_seen_ms.store(now_millis(), Ordering::Relaxed);
            peers.push(peer);
        }
        (path, cfg, registry, peers)
    }

    async fn ingest_ack_test_packet(
        registry: &PeerRegistry,
        packet: &[u8],
        addr: SocketAddr,
        tx: &mpsc::Sender<Vec<u8>>,
        active: &DashMap<Ipv4Addr, Uuid>,
    ) -> Result<()> {
        let meta = peek_meta(packet)?;
        let (peer, decoded) = registry
            .get_peer(meta.device_id, meta.session_id, packet)
            .await?
            .context("test peer rejected")?;
        handle_received(
            &peer,
            decoded,
            &registry.direct,
            Route {
                path: PathKind::Direct,
                addr,
                media: false,
            },
            tx,
            active,
        )
        .await;
        Ok(())
    }

    async fn receive_ack_test_packet(client: &SessionEngine, socket: &UdpSocket) -> DecodedPacket {
        let mut bytes = [0u8; 1200];
        let n = tokio::time::timeout(Duration::from_secs(1), socket.recv(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        client.decapsulate(&bytes[..n]).unwrap().unwrap()
    }

    #[tokio::test]
    async fn ack_capability_legacy_peer_keeps_single_acks() {
        let (path, cfg, registry, peers) = established_device().await;
        let crypto = registry.keys.get(&cfg).await.unwrap();
        let client = SessionEngine::with_session_and_crypto(cfg, 1, (*crypto).clone()).unwrap();
        // established_device already consumed this session's first probe sequence.
        client.make_probe(1, 2).unwrap();
        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (tx, _rx) = mpsc::channel(4);
        let active = DashMap::new();
        for _ in 0..3 {
            let (id, wire) = client.encapsulate_ip(1, &[7; 100]).unwrap();
            ingest_ack_test_packet(
                &registry,
                &wire[0],
                receiver.local_addr().unwrap(),
                &tx,
                &active,
            )
            .await
            .unwrap();
            assert!(
                matches!(receive_ack_test_packet(&client, &receiver).await, DecodedPacket::Ack { packet_id } if packet_id == id)
            );
        }
        drop(peers);
        drop(registry);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn ack_capability_requires_authenticated_same_session_and_survives_migration() {
        let (path, cfg, registry, peers) = established_device().await;
        let crypto = registry.keys.get(&cfg).await.unwrap();
        let client =
            SessionEngine::with_session_and_crypto(cfg.clone(), 1, (*crypto).clone()).unwrap();
        let other = SessionEngine::with_session_and_crypto(cfg, 2, (*crypto).clone()).unwrap();
        client.make_probe(1, 2).unwrap();
        other.make_probe(1, 2).unwrap();
        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = receiver.local_addr().unwrap();
        let (tx, _rx) = mpsc::channel(8);
        let active = DashMap::new();
        let mut corrupt = client.make_ack(9).unwrap();
        let end = corrupt.len() - 1;
        corrupt[end] ^= 1;
        assert!(
            ingest_ack_test_packet(&registry, &corrupt, address, &tx, &active)
                .await
                .is_err()
        );
        let (_, wire) = client.encapsulate_ip(1, &[7; 100]).unwrap();
        ingest_ack_test_packet(&registry, &wire[0], address, &tx, &active)
            .await
            .unwrap();
        assert!(matches!(
            receive_ack_test_packet(&client, &receiver).await,
            DecodedPacket::Ack { .. }
        ));

        ingest_ack_test_packet(
            &registry,
            &client.make_ack(10).unwrap(),
            address,
            &tx,
            &active,
        )
        .await
        .unwrap();
        let (id, wire) = client.encapsulate_ip(1, &[8; 100]).unwrap();
        ingest_ack_test_packet(&registry, &wire[0], address, &tx, &active)
            .await
            .unwrap();
        assert!(
            matches!(receive_ack_test_packet(&client, &receiver).await, DecodedPacket::AckRanges { ranges } if ranges.iter().any(|range| range.contains(id)))
        );

        let (_, wire) = other.encapsulate_ip(1, &[9; 100]).unwrap();
        ingest_ack_test_packet(&registry, &wire[0], address, &tx, &active)
            .await
            .unwrap();
        assert!(matches!(
            receive_ack_test_packet(&other, &receiver).await,
            DecodedPacket::Ack { .. }
        ));

        let migrated = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let wire = client.encapsulate_ip_with_id(1, id, &[8; 100]).unwrap();
        ingest_ack_test_packet(
            &registry,
            &wire[0],
            migrated.local_addr().unwrap(),
            &tx,
            &active,
        )
        .await
        .unwrap();
        assert!(matches!(
            receive_ack_test_packet(&client, &migrated).await,
            DecodedPacket::AckRanges { .. }
        ));
        drop(peers);
        drop(registry);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn best_effort_selection_does_not_wait_for_worker_recovery_lock() {
        let (path, cfg, registry, peers) = established_device().await;
        for peer in &peers {
            peer.route.lock().await.addr = "127.0.0.1:1".parse().unwrap();
        }
        let schedulers: DeviceSchedulers = Arc::new(DashMap::new());
        let mut packet = vec![0u8; 900];
        packet[0] = 0x45;
        packet[9] = 17;
        packet[16..20].copy_from_slice(&cfg.virtual_ipv4().octets());
        let held = peers[0].recovery.lock().await;
        let selected = tokio::time::timeout(
            Duration::from_millis(500),
            select_peer(
                cfg.device_id,
                &packet,
                &registry.peers,
                &registry.sessions_by_device,
                &schedulers,
            ),
        )
        .await;
        drop(held);
        let selected = selected.ok().flatten().is_some();
        drop(peers);
        drop(registry);
        std::fs::remove_file(path).unwrap();
        assert!(
            selected,
            "best-effort scheduling must not acquire worker recovery locks"
        );
    }

    #[tokio::test]
    async fn ready_route_selection_fits_multiple_packets_in_one_cooperative_budget() {
        let (path, cfg, registry, peers) = established_device().await;
        for peer in &peers {
            peer.route.lock().await.addr = "127.0.0.1:1".parse().unwrap();
        }
        let registry = Arc::new(registry);
        let selecting = registry.clone();
        let schedulers: DeviceSchedulers = Arc::new(DashMap::new());
        let mut packet = vec![0u8; 900];
        packet[0] = 0x45;
        packet[9] = 17;
        packet[16..20].copy_from_slice(&cfg.virtual_ipv4().octets());
        let completed = tokio::spawn(async move {
            let mut batch = Box::pin(async {
                for _ in 0..40 {
                    select_peer(
                        cfg.device_id,
                        &packet,
                        &selecting.peers,
                        &selecting.sessions_by_device,
                        &schedulers,
                    )
                    .await
                    .unwrap();
                }
            });
            std::future::poll_fn(|cx| {
                std::task::Poll::Ready(std::future::Future::poll(batch.as_mut(), cx).is_ready())
            })
            .await
        })
        .await
        .unwrap();
        drop(peers);
        drop(registry);
        std::fs::remove_file(path).unwrap();
        assert!(
            completed,
            "ready route metadata must not exhaust the cooperative budget"
        );
    }

    #[tokio::test]
    async fn busy_route_selection_waits_and_preserves_the_assigned_worker() {
        let (path, cfg, registry, peers) = established_device().await;
        for peer in &peers {
            peer.route.lock().await.addr = "127.0.0.1:1".parse().unwrap();
        }
        let schedulers: DeviceSchedulers = Arc::new(DashMap::new());
        let mut packet = vec![0u8; 900];
        packet[0] = 0x45;
        packet[9] = 17;
        packet[16..20].copy_from_slice(&cfg.virtual_ipv4().octets());
        let first = select_peer(
            cfg.device_id,
            &packet,
            &registry.peers,
            &registry.sessions_by_device,
            &schedulers,
        )
        .await
        .unwrap();
        let assigned = first.session_id;
        let held = first.peer.route.lock().await;
        let release = async {
            tokio::time::sleep(Duration::from_millis(25)).await;
            drop(held);
        };
        let selecting = tokio::time::timeout(
            Duration::from_millis(500),
            select_peer(
                cfg.device_id,
                &packet,
                &registry.peers,
                &registry.sessions_by_device,
                &schedulers,
            ),
        );
        let (selected, ()) = tokio::join!(selecting, release);
        let selected = selected.unwrap().unwrap();
        assert_eq!(selected.session_id, assigned);
        drop(selected);
        drop(first);
        drop(peers);
        drop(registry);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn reliable_selection_still_prefers_a_worker_with_congestion_credit() {
        let (path, cfg, registry, peers) = established_device().await;
        for peer in &peers {
            peer.route.lock().await.addr = "127.0.0.1:1".parse().unwrap();
        }
        peers[0]
            .recovery
            .lock()
            .await
            .on_sent(1, 1, vec![0; 900], 12_000, Reliability::Reliable);
        let schedulers: DeviceSchedulers = Arc::new(DashMap::new());
        let mut packet = vec![0u8; 900];
        packet[0] = 0x45;
        packet[9] = 1;
        packet[16..20].copy_from_slice(&cfg.virtual_ipv4().octets());
        let selected = select_peer(
            cfg.device_id,
            &packet,
            &registry.peers,
            &registry.sessions_by_device,
            &schedulers,
        )
        .await
        .unwrap();
        assert_eq!(selected.session_id, 2);
        drop(selected);
        drop(peers);
        drop(registry);
        std::fs::remove_file(path).unwrap();
    }

    async fn assert_device_disabled(
        cfg: &ConnectionConfig,
        registry: &PeerRegistry,
        peers: &[Arc<Peer>],
    ) {
        let schedulers: DeviceSchedulers = Arc::new(DashMap::new());
        schedulers.insert(
            cfg.device_id,
            Arc::new(Mutex::new(FlowScheduler::default())),
        );
        let active = DashMap::new();
        active.insert(cfg.virtual_ipv4(), cfg.device_id);
        registry.refresh_auth(&schedulers, &active).await;
        assert!(registry.peers.is_empty());
        assert!(registry.sessions_by_device.is_empty());
        assert!(schedulers.is_empty());
        assert!(active.is_empty());
        for peer in peers {
            assert!(!peer.authorized.load(Ordering::Acquire));
            let (tx, _rx) = mpsc::channel(1);
            handle_received(
                peer,
                Some(DecodedPacket::Heartbeat { counter: 9 }),
                &registry.turn,
                Route {
                    path: PathKind::Turn,
                    addr: "127.0.0.1:1".parse().unwrap(),
                    media: false,
                },
                &tx,
                &active,
            )
            .await;
            assert!(
                active.is_empty(),
                "an invalidated in-flight peer must not refresh its route"
            );
        }
    }

    #[tokio::test]
    async fn auth_refresh_keeps_current_sessions_when_expiry_is_extended() {
        let (path, cfg, registry, peers) = established_device().await;
        registry
            .db
            .upsert(
                cfg.device_id,
                cfg.password.clone(),
                cfg.provider,
                cfg.provider_data.clone(),
                2,
            )
            .await
            .unwrap();
        let schedulers: DeviceSchedulers = Arc::new(DashMap::new());
        let active = DashMap::new();
        active.insert(cfg.virtual_ipv4(), cfg.device_id);
        registry.refresh_auth(&schedulers, &active).await;
        assert_eq!(registry.peers.len(), 2);
        assert_eq!(registry.sessions_by_device.len(), 1);
        assert!(active.contains_key(&cfg.virtual_ipv4()));
        assert!(peers
            .iter()
            .all(|peer| peer.authorized.load(Ordering::Acquire)));
        drop(registry);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn auth_refresh_disables_all_revoked_sessions_despite_fresh_heartbeats() {
        let (path, cfg, registry, peers) = established_device().await;
        registry.db.revoke(cfg.device_id).await.unwrap();
        assert_device_disabled(&cfg, &registry, &peers).await;
        drop(registry);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn auth_refresh_disables_all_expired_sessions_despite_fresh_heartbeats() {
        let (path, cfg, registry, peers) = established_device().await;
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE configs SET expires_at=?1 WHERE device_id=?2",
                rusqlite::params![
                    (Utc::now() - chrono::Duration::seconds(1)).to_rfc3339(),
                    cfg.device_id.to_string()
                ],
            )
            .unwrap();
        assert_device_disabled(&cfg, &registry, &peers).await;
        drop(registry);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn auth_refresh_disables_old_sessions_after_password_rotation() {
        let (path, cfg, registry, peers) = established_device().await;
        registry
            .db
            .upsert(
                cfg.device_id,
                "rotated-test-password".into(),
                cfg.provider,
                cfg.provider_data.clone(),
                1,
            )
            .await
            .unwrap();
        assert_device_disabled(&cfg, &registry, &peers).await;
        drop(registry);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn auth_refresh_disables_old_sessions_after_provider_replacement() {
        let (path, cfg, registry, peers) = established_device().await;
        registry
            .db
            .upsert(
                cfg.device_id,
                cfg.password.clone(),
                tots_core::config::ProviderName::Jazz,
                serde_json::json!({"jazz":{}}),
                1,
            )
            .await
            .unwrap();
        assert_device_disabled(&cfg, &registry, &peers).await;
        drop(registry);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn invalid_first_packet_does_not_register_a_worker() {
        let path = std::env::temp_dir().join(format!("tots-admission-{}.db", Uuid::new_v4()));
        let db = Db::open(&path).await.unwrap();
        let cfg = config();
        db.upsert(
            cfg.device_id,
            cfg.password.clone(),
            cfg.provider,
            cfg.provider_data.clone(),
            1,
        )
        .await
        .unwrap();
        let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let registry = PeerRegistry::new(db, sock.clone(), sock);
        let engine = SessionEngine::new(cfg.clone()).unwrap();
        let mut packet = engine.make_probe(1, 2).unwrap();
        *packet.last_mut().unwrap() ^= 1;
        assert!(registry
            .get_peer(cfg.device_id, engine.session_id(), &packet)
            .await
            .is_err());
        assert!(registry.peers.is_empty());
        assert!(registry.sessions_by_device.is_empty());
        drop(registry);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn downlink_keeps_dispatching_when_one_worker_queue_is_stalled() {
        let cfg = config();
        let crypto = tots_core::crypto::PacketCrypto::derive(&cfg.password, cfg.device_id).unwrap();
        let peers: PeerMap = Arc::new(DashMap::new());
        let sessions: DeviceSessions = Arc::new(DashMap::new());
        let schedulers: DeviceSchedulers = Arc::new(DashMap::new());
        let active = DashMap::new();
        let virtual_ip = cfg.virtual_ipv4();
        active.insert(virtual_ip, cfg.device_id);
        sessions.insert(cfg.device_id, Arc::new(Mutex::new(vec![1, 2])));
        let permits = Arc::new(tokio::sync::Semaphore::new(2));
        let mut receivers = Vec::new();
        for session in 1..=2 {
            let (outbound, receiver) = mpsc::channel(1);
            let (ack_outbound, _ack_receiver) = mpsc::channel(1);
            receivers.push(receiver);
            peers.insert(
                (cfg.device_id, session),
                Arc::new(Peer {
                    device_id: cfg.device_id,
                    auth_fingerprint: [0; 32],
                    authorized: AtomicBool::new(true),
                    engine: Arc::new(
                        SessionEngine::with_session_and_crypto(
                            cfg.clone(),
                            session,
                            crypto.clone(),
                        )
                        .unwrap(),
                    ),
                    route: Mutex::new(Route {
                        path: PathKind::Turn,
                        addr: "127.0.0.1:1".parse().unwrap(),
                        media: false,
                    }),
                    virtual_ip,
                    last_seen_ms: AtomicU64::new(now_millis()),
                    recovery: Mutex::new(RecoveryController::default()),
                    outbound,
                    ack_outbound,
                    migrated: std::sync::atomic::AtomicBool::new(false),
                    ack_ranges: AtomicBool::new(false),
                    _slot: permits.clone().try_acquire_owned().unwrap(),
                }),
            );
        }
        let mut packet = vec![0u8; 1400];
        packet[0] = 0x45;
        packet[9] = 6;
        packet[16..20].copy_from_slice(&virtual_ip.octets());
        let (tx, rx) = mpsc::channel(2);
        tx.send(packet.clone()).await.unwrap();
        tx.send(packet).await.unwrap();
        drop(tx);
        tokio::time::timeout(
            Duration::from_millis(100),
            route_from_tun(rx, &active, &peers, &sessions, &schedulers),
        )
        .await
        .unwrap();
        assert!(receivers[0].try_recv().is_ok());
        assert!(receivers[1].try_recv().is_ok());
    }
}
