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
    flow::{FlowManager, FlowScheduler},
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
type DeviceSchedulers = Arc<DashMap<Uuid, Arc<Mutex<FlowScheduler>>>>;

struct SelectedPeer {
    assoc_id: u16,
    session_id: u64,
    peer: Arc<Peer>,
    scheduler: Arc<Mutex<FlowScheduler>>,
}

pub async fn run(args: Args, db: Db) -> Result<()> {
    let direct = Arc::new(tots_core::transport::socket::bind_udp(args.webrtc_listen)?);
    let turn = Arc::new(tots_core::transport::socket::bind_udp(args.turn_listen)?);
    let peers: PeerMap = Arc::new(DashMap::new());
    let sessions_by_device: DeviceSessions = Arc::new(DashMap::new());
    let schedulers: DeviceSchedulers = Arc::new(DashMap::new());
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
        let schedulers = schedulers.clone();
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
                        let empty = list.is_empty();
                        drop(list);
                        if empty {
                            sessions_by_device.remove(&device);
                            schedulers.remove(&device);
                        }
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
        let Some(selected) = select_peer(
            device_id,
            &packet,
            &peers,
            &sessions_by_device,
            &schedulers,
        )
        .await
        else {
            continue;
        };
        let SelectedPeer {
            assoc_id,
            session_id,
            peer,
            scheduler,
        } = selected;
        let route = peer.route.lock().await.clone();
        if route.addr.port() == 0 {
            continue;
        }
        let socket = match route.path {
            PathKind::Direct => &direct,
            PathKind::Turn => &turn,
        };
        let reliability = FlowManager::classify(&packet);
        if let Ok((packet_id, frames)) = peer.engine.encapsulate_ip(assoc_id, &packet) {
            let bytes: usize = frames.iter().map(Vec::len).sum();
            let mut congestion_marked = false;
            loop {
                if peer.recovery.lock().await.can_send(bytes) {
                    break;
                }
                if !congestion_marked {
                    scheduler.lock().await.mark_congested(session_id);
                    congestion_marked = true;
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
                    scheduler.lock().await.mark_congested(session_id);
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
    schedulers: &DeviceSchedulers,
) -> Option<SelectedPeer> {
    let sessions = sessions_by_device.get(&device)?.value().clone();
    let sessions = sessions.lock().await.clone();
    let now = now_millis();
    let latency_sensitive = FlowScheduler::is_latency_sensitive(packet);
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
        if now.saturating_sub(peer.last_seen_ms.load(Ordering::Relaxed)) > 30_000 {
            continue;
        }
        if peer.route.lock().await.addr.port() == 0 {
            continue;
        }
        active.push(session);
        if !latency_sensitive
            && peer
                .recovery
                .lock()
                .await
                .can_send(estimated_wire_bytes)
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
    let decision = scheduler.lock().await.select(packet, candidates)?;
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
