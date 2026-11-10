use crate::{config::Args, db::Db};
use anyhow::{Context, Result};
use chrono::Utc;
use dashmap::DashMap;
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UdpSocket,
    sync::{mpsc, Mutex},
};
use tots_core::{
    config::ConnectionConfig,
    flow::FlowManager,
    protocol::peek_meta,
    recovery::RecoveryController,
    session::{DecodedPacket, SessionEngine},
    transport::rtp,
};
use uuid::Uuid;

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
    engine: Arc<SessionEngine>,
    route: Mutex<Route>,
    virtual_ip: Ipv4Addr,
    last_seen_ms: AtomicU64,
    recovery: Mutex<RecoveryController>,
}

type PeerMap = Arc<DashMap<(Uuid, u64), Arc<Peer>>>;
type DeviceSessions = Arc<DashMap<Uuid, Arc<Mutex<Vec<u64>>>>>;

pub async fn run(args: Args, db: Db) -> Result<()> {
    let direct = Arc::new(tots_core::transport::socket::bind_udp(args.webrtc_listen)?);
    let turn = Arc::new(tots_core::transport::socket::bind_udp(args.turn_listen)?);
    let peers: PeerMap = Arc::new(DashMap::new());
    let sessions_by_device: DeviceSessions = Arc::new(DashMap::new());
    // One virtual IP maps to a device; a device may now have many worker sessions.
    let active_by_ip: Arc<DashMap<Ipv4Addr, Uuid>> = Arc::new(DashMap::new());
    let (to_tun_tx, mut to_tun_rx) = mpsc::channel::<Vec<u8>>(4096);
    let (from_tun_tx, mut from_tun_rx) = mpsc::channel::<Vec<u8>>(4096);

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

    for (kind, sock) in [
        (PathKind::Direct, direct.clone()),
        (PathKind::Turn, turn.clone()),
    ] {
        let db = db.clone();
        let peers = peers.clone();
        let sessions_by_device = sessions_by_device.clone();
        let active_by_ip = active_by_ip.clone();
        let tx = to_tun_tx.clone();
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
                let peer = match get_peer(
                    &db,
                    &peers,
                    &sessions_by_device,
                    meta.device_id,
                    meta.session_id,
                )
                .await
                {
                    Ok(Some(peer)) => peer,
                    _ => continue,
                };
                let Ok(decoded) = peer.engine.decapsulate(data) else {
                    continue;
                };
                peer.last_seen_ms.store(now_millis(), Ordering::Relaxed);
                active_by_ip.insert(peer.virtual_ip, meta.device_id);
                let new_route = Route {
                    path: kind,
                    addr,
                    media,
                };
                let migrated = {
                    let mut route = peer.route.lock().await;
                    let migrated = *route != new_route && route.addr.port() != 0;
                    *route = new_route.clone();
                    migrated
                };
                if migrated {
                    tracing::info!(
                        device_id = %meta.device_id,
                        session_id = meta.session_id,
                        path = ?kind,
                        "worker route migrated"
                    );
                    let retries = peer.recovery.lock().await.on_path_migration();
                    for retry in retries {
                        if let Ok(datagrams) = peer.engine.encapsulate_ip_with_id(
                            retry.assoc_id,
                            retry.packet_id,
                            &retry.payload,
                        ) {
                            for datagram in datagrams {
                                let delay = peer.recovery.lock().await.reserve_pacing(datagram.len());
                                if !delay.is_zero() {
                                    tokio::time::sleep(delay).await;
                                }
                                let _ = send_reply(&sock, &datagram, addr, media).await;
                            }
                        }
                    }
                }
                match decoded {
                    Some(DecodedPacket::Ip {
                        packet_id, bytes, ..
                    }) => {
                        let _ = tx.send(bytes).await;
                        if let Ok(ack) = peer.engine.make_ack(packet_id) {
                            let _ = send_reply(&sock, &ack, addr, media).await;
                        }
                    }
                    Some(DecodedPacket::Duplicate { packet_id }) => {
                        if let Ok(ack) = peer.engine.make_ack(packet_id) {
                            let _ = send_reply(&sock, &ack, addr, media).await;
                        }
                    }
                    Some(DecodedPacket::Ack { packet_id }) => {
                        peer.recovery.lock().await.on_ack(packet_id);
                    }
                    Some(DecodedPacket::AckRanges { ranges }) => {
                        peer.recovery.lock().await.on_ack_ranges(&ranges);
                    }
                    Some(DecodedPacket::Probe { token, sent_micros }) => {
                        if let Ok(reply) = peer.engine.make_probe_ack(token, sent_micros) {
                            let _ = send_reply(&sock, &reply, addr, media).await;
                        }
                    }
                    _ => {}
                }
            }
        });
    }

    // Recovery/loss detection is symmetric: replies from the server use the same
    // congestion controller and can retransmit bounded reliable packets.
    {
        let peers = peers.clone();
        let direct = direct.clone();
        let turn = turn.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(20));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let snapshot: Vec<_> = peers.iter().map(|entry| entry.value().clone()).collect();
                for peer in snapshot {
                    let retries = peer.recovery.lock().await.poll_losses();
                    if retries.is_empty() {
                        continue;
                    }
                    let route = peer.route.lock().await.clone();
                    if route.addr.port() == 0 {
                        continue;
                    }
                    let socket = match route.path {
                        PathKind::Direct => &direct,
                        PathKind::Turn => &turn,
                    };
                    for retry in retries {
                        let Ok(datagrams) = peer.engine.encapsulate_ip_with_id(
                            retry.assoc_id,
                            retry.packet_id,
                            &retry.payload,
                        ) else {
                            continue;
                        };
                        for datagram in datagrams {
                            let delay = peer.recovery.lock().await.reserve_pacing(datagram.len());
                            if !delay.is_zero() {
                                tokio::time::sleep(delay).await;
                            }
                            let _ = send_reply(socket, &datagram, route.addr, route.media).await;
                        }
                    }
                }
            }
        });
    }

    // Remove dead worker sessions so a failed worker never remains in scheduling forever.
    {
        let peers = peers.clone();
        let sessions_by_device = sessions_by_device.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            loop {
                tick.tick().await;
                let now = now_millis();
                let stale: Vec<_> = peers
                    .iter()
                    .filter_map(|entry| {
                        (now.saturating_sub(entry.value().last_seen_ms.load(Ordering::Relaxed))
                            > 60_000)
                            .then_some(entry.key().to_owned())
                    })
                    .collect();
                for (device, session) in stale {
                    peers.remove(&(device, session));
                    tracing::info!(device_id = %device, session_id = session, "worker session expired");
                    if let Some(list) = sessions_by_device.get(&device).map(|v| v.value().clone()) {
                        let mut list = list.lock().await;
                        list.retain(|id| *id != session);
                    }
                }
            }
        });
    }

    while let Some(packet) = from_tun_rx.recv().await {
        let Some(dst) = ipv4_dst(&packet) else {
            continue;
        };
        let Some(device_id) = active_by_ip.get(&dst).map(|v| *v.value()) else {
            continue;
        };
        let Some(peer) = select_peer(device_id, &packet, &peers, &sessions_by_device).await else {
            continue;
        };
        let route = peer.route.lock().await.clone();
        if route.addr.port() == 0 {
            continue;
        }
        let socket = match route.path {
            PathKind::Direct => &direct,
            PathKind::Turn => &turn,
        };
        let reliability = FlowManager::classify(&packet);
        let assoc_id = server_assoc_id(&packet);
        if let Ok((packet_id, frames)) = peer.engine.encapsulate_ip(assoc_id, &packet) {
            let bytes: usize = frames.iter().map(Vec::len).sum();
            loop {
                if peer.recovery.lock().await.can_send(bytes) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let mut sent_all = true;
            for frame in &frames {
                let delay = peer.recovery.lock().await.reserve_pacing(frame.len());
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                if send_reply(socket, frame, route.addr, route.media)
                    .await
                    .is_err()
                {
                    sent_all = false;
                    break;
                }
            }
            if sent_all {
                peer.recovery
                    .lock()
                    .await
                    .on_sent(packet_id, assoc_id, packet.clone(), bytes, reliability);
            }
        }
    }
    Ok(())
}

async fn get_peer(
    db: &Db,
    map: &PeerMap,
    sessions_by_device: &DeviceSessions,
    id: Uuid,
    session: u64,
) -> Result<Option<Arc<Peer>>> {
    let key = (id, session);
    if let Some(peer) = map.get(&key) {
        return Ok(Some(peer.clone()));
    }
    let Some(device) = db.get(id).await? else {
        return Ok(None);
    };
    if device.revoked || device.expires_at < Utc::now() {
        return Ok(None);
    }
    let cfg = ConnectionConfig {
        version: 1,
        peer: "0.0.0.0".into(),
        webrtc_port: 1,
        turn_port: 1,
        password: device.password.clone(),
        device_id: id,
        provider: device.provider,
        provider_data: device.provider_data,
        mtu: 1200,
    };
    let virtual_ip = cfg.virtual_ipv4();
    let peer = Arc::new(Peer {
        engine: Arc::new(SessionEngine::with_session(cfg, session)?),
        route: Mutex::new(Route {
            path: PathKind::Direct,
            addr: "0.0.0.0:0".parse().unwrap(),
            media: false,
        }),
        virtual_ip,
        last_seen_ms: AtomicU64::new(now_millis()),
        recovery: Mutex::new(RecoveryController::default()),
    });
    map.insert(key, peer.clone());
    tracing::info!(
        device_id = %id,
        session_id = session,
        virtual_ip = %virtual_ip,
        "worker session registered"
    );
    let list = sessions_by_device
        .entry(id)
        .or_insert_with(|| Arc::new(Mutex::new(Vec::new())))
        .value()
        .clone();
    let mut sessions = list.lock().await;
    if !sessions.contains(&session) {
        sessions.push(session);
        sessions.sort_unstable();
    }
    Ok(Some(peer))
}

async fn select_peer(
    device: Uuid,
    packet: &[u8],
    peers: &PeerMap,
    sessions_by_device: &DeviceSessions,
) -> Option<Arc<Peer>> {
    let sessions = sessions_by_device.get(&device)?.value().clone();
    let sessions = sessions.lock().await.clone();
    let now = now_millis();
    let mut active = Vec::new();
    for session in sessions {
        let Some(peer) = peers.get(&(device, session)).map(|entry| entry.value().clone()) else {
            continue;
        };
        if now.saturating_sub(peer.last_seen_ms.load(Ordering::Relaxed)) <= 30_000 {
            active.push((session, peer));
        }
    }
    if active.is_empty() {
        return None;
    }
    let flow = u64::from(server_assoc_id(packet));
    active
        .into_iter()
        .max_by_key(|(session, _)| rendezvous_score(flow, *session))
        .map(|(_, peer)| peer)
}

fn rendezvous_score(flow: u64, session: u64) -> u64 {
    // Rendezvous hashing keeps unaffected flows on their existing worker when one
    // session disappears or returns; only flows owned by that session need to move.
    let mut x = flow ^ session.rotate_left(17) ^ 0x9e37_79b9_7f4a_7c15;
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
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

fn server_assoc_id(packet: &[u8]) -> u16 {
    if packet.len() >= 20 && packet[0] >> 4 == 4 {
        let ihl = usize::from(packet[0] & 0x0f) * 4;
        if ihl >= 20 && packet.len() >= ihl {
            let mut hash = 0x9e37_79b9u32;
            for byte in &packet[12..20] {
                hash = hash.rotate_left(5) ^ u32::from(*byte);
                hash = hash.wrapping_mul(0x85eb_ca6b);
            }
            hash ^= u32::from(packet[9]);
            if matches!(packet[9], 6 | 17) && packet.len() >= ihl + 4 {
                for byte in &packet[ihl..ihl + 4] {
                    hash = hash.rotate_left(5) ^ u32::from(*byte);
                    hash = hash.wrapping_mul(0x85eb_ca6b);
                }
            }
            return ((hash >> 16) ^ hash) as u16;
        }
    }
    let mut hash = 0x9e37_79b9u32;
    for byte in packet.iter().take(32) {
        hash = hash.rotate_left(5) ^ u32::from(*byte);
        hash = hash.wrapping_mul(0x85eb_ca6b);
    }
    ((hash >> 16) ^ hash) as u16
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
