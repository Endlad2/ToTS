use anyhow::{anyhow, Context, Result};
use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tots_core::{
    config::{ConnectionConfig, ProviderName},
    flow::{FlowManager, FlowScheduler, Reliability},
    providers::{make_provider, Provider, ProviderBootstrap},
    recovery::RecoveryController,
    session::{DecodedPacket, SessionEngine},
    transport::{probe, turn::TurnUdpPath, DatagramPath, DirectUdpPath},
};
use tracing::{info, warn};

const PRIMARY_RETRIES_BEFORE_FALLBACK: usize = 3;
const WORKER_QUEUE: usize = 2048;
const INBOUND_QUEUE: usize = 4096;
const HEALTH_PROBE_INTERVAL: Duration = Duration::from_secs(5);
const HEALTH_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Debug)]
struct OutboundPacket {
    assoc_id: u16,
    bytes: Vec<u8>,
    reliability: Reliability,
    enqueued_at: Instant,
}

#[derive(Default)]
pub struct TrafficStats {
    pub tun_tx_bytes: AtomicU64,
    pub tun_rx_bytes: AtomicU64,
}

struct WorkerSlot {
    id: usize,
    healthy: AtomicBool,
    provider: RwLock<Option<ProviderName>>,
    sender: mpsc::Sender<OutboundPacket>,
    route_ips: RwLock<Vec<Ipv4Addr>>,
    tx_packets: AtomicU64,
    rx_packets: AtomicU64,
}

pub struct WorkerManager {
    cfg: ConnectionConfig,
    slots: Vec<Arc<WorkerSlot>>,
    scheduler: Mutex<FlowScheduler>,
    shutdown_tx: watch::Sender<bool>,
    handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    pub stats: Arc<TrafficStats>,
}

impl WorkerManager {
    pub async fn start(
        cfg: ConnectionConfig,
        workers: usize,
        relay_only: bool,
    ) -> Result<(Arc<Self>, mpsc::Receiver<Vec<u8>>)> {
        if workers == 0 {
            anyhow::bail!("--n must be at least 1");
        }
        if workers > 256 {
            anyhow::bail!("--n must not exceed 256");
        }
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (inbound_tx, inbound_rx) = mpsc::channel(INBOUND_QUEUE);
        let mut slots = Vec::with_capacity(workers);
        let mut receivers = Vec::with_capacity(workers);
        for id in 0..workers {
            let (sender, receiver) = mpsc::channel(WORKER_QUEUE);
            slots.push(Arc::new(WorkerSlot {
                id,
                healthy: AtomicBool::new(false),
                provider: RwLock::new(None),
                sender,
                route_ips: RwLock::new(Vec::new()),
                tx_packets: AtomicU64::new(0),
                rx_packets: AtomicU64::new(0),
            }));
            receivers.push(receiver);
        }
        let manager = Arc::new(Self {
            cfg: cfg.clone(),
            slots,
            scheduler: Mutex::new(FlowScheduler::default()),
            shutdown_tx,
            handles: Mutex::new(Vec::new()),
            stats: Arc::new(TrafficStats::default()),
        });
        let mut handles = Vec::with_capacity(workers);
        for (slot, receiver) in manager.slots.iter().cloned().zip(receivers) {
            handles.push(tokio::spawn(worker_supervisor(
                cfg.clone(),
                slot,
                receiver,
                inbound_tx.clone(),
                shutdown_rx.clone(),
                relay_only,
            )));
        }
        *manager.handles.lock().await = handles;
        Ok((manager, inbound_rx))
    }

    pub async fn wait_for_healthy(&self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.healthy_count() > 0 {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("no worker became healthy before timeout");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub fn desired_count(&self) -> usize {
        self.slots.len()
    }

    pub fn healthy_count(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.healthy.load(Ordering::Relaxed))
            .count()
    }

    pub async fn route_ips(&self) -> Vec<Ipv4Addr> {
        let mut out = BTreeSet::new();
        if let IpAddr::V4(peer) = self.cfg.peer_ip().unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED)) {
            if !peer.is_unspecified() {
                out.insert(peer);
            }
        }
        for slot in &self.slots {
            out.extend(slot.route_ips.read().await.iter().copied());
        }
        out.into_iter().collect()
    }

    pub async fn send_ip(&self, packet: &[u8]) -> Result<()> {
        let reliability = FlowManager::classify(packet);
        let mut candidates: Vec<u64> = self
            .slots
            .iter()
            .filter(|slot| slot.healthy.load(Ordering::Acquire))
            .map(|slot| slot.id as u64)
            .collect();

        while !candidates.is_empty() {
            let decision = {
                let mut scheduler = self.scheduler.lock().await;
                scheduler.select(packet, &candidates)
            };
            let Some(decision) = decision else {
                break;
            };
            let index = decision.worker_id as usize;
            let Some(slot) = self.slots.get(index) else {
                candidates.retain(|worker_id| *worker_id != decision.worker_id);
                continue;
            };
            let outbound = OutboundPacket {
                assoc_id: decision.assoc_id,
                bytes: packet.to_vec(),
                reliability,
                enqueued_at: Instant::now(),
            };
            match slot.sender.try_send(outbound) {
                Ok(()) => {
                    self.stats
                        .tun_tx_bytes
                        .fetch_add(packet.len() as u64, Ordering::Relaxed);
                    return Ok(());
                }
                Err(mpsc::error::TrySendError::Full(outbound)) => {
                    self.scheduler
                        .lock()
                        .await
                        .mark_congested(decision.worker_id);
                    // Keep the current flowlet ordered. Queue saturation lowers this
                    // worker's future scheduling weight, while this packet applies
                    // backpressure instead of being sprayed to another path.
                    slot.sender
                        .send(outbound)
                        .await
                        .map_err(|_| anyhow!("ToTS worker queue closed"))?;
                    self.stats
                        .tun_tx_bytes
                        .fetch_add(packet.len() as u64, Ordering::Relaxed);
                    return Ok(());
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    slot.healthy.store(false, Ordering::Release);
                    candidates.retain(|worker_id| *worker_id != decision.worker_id);
                }
            }
        }
        Err(anyhow!("no healthy ToTS worker"))
    }

    pub async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
        let handles = {
            let mut guard = self.handles.lock().await;
            std::mem::take(&mut *guard)
        };
        for handle in handles {
            let _ = handle.await;
        }
    }

    pub fn log_status(&self) {
        let tx = self.stats.tun_tx_bytes.load(Ordering::Relaxed);
        let rx = self.stats.tun_rx_bytes.load(Ordering::Relaxed);
        let total = tx.saturating_add(rx);
        let mut vk_workers = 0usize;
        let mut jazz_workers = 0usize;
        for slot in &self.slots {
            if !slot.healthy.load(Ordering::Relaxed) {
                continue;
            }
            if let Ok(provider) = slot.provider.try_read() {
                match *provider {
                    Some(ProviderName::Vk) => vk_workers += 1,
                    Some(ProviderName::Jazz) => jazz_workers += 1,
                    _ => {}
                }
            }
        }
        info!(
            tun_tx_mb = tx as f64 / 1_048_576.0,
            tun_rx_mb = rx as f64 / 1_048_576.0,
            tun_total_mb = total as f64 / 1_048_576.0,
            workers = self.healthy_count(),
            workers_target = self.desired_count(),
            workers_vk = vk_workers,
            workers_jazz = jazz_workers,
            vk_estimated_capacity_mbps = FlowScheduler::estimated_capacity_mbps(vk_workers),
            "ToTS status"
        );
    }
}

async fn worker_supervisor(
    cfg: ConnectionConfig,
    slot: Arc<WorkerSlot>,
    mut outbound_rx: mpsc::Receiver<OutboundPacket>,
    inbound_tx: mpsc::Sender<Vec<u8>>,
    mut shutdown: watch::Receiver<bool>,
    relay_only: bool,
) {
    let engine = match SessionEngine::new(cfg.clone()) {
        Ok(engine) => Arc::new(engine),
        Err(error) => {
            warn!(worker = slot.id, %error, "worker engine initialization failed");
            return;
        }
    };
    let recovery = Arc::new(Mutex::new(RecoveryController::default()));
    let providers = cfg.provider_order();
    let mut failures = 0usize;
    info!(worker = slot.id, session_id = engine.session_id(), "worker starting");
    // Avoid a provider API/ICE thundering herd when many workers start together.
    let startup_stagger = Duration::from_millis(((slot.id as u64) * 60).min(1_000));
    if !startup_stagger.is_zero() {
        tokio::select! {
            _ = tokio::time::sleep(startup_stagger) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    slot.healthy.store(false, Ordering::Release);
                    return;
                }
            }
        }
    }
    loop {
        if *shutdown.borrow() {
            break;
        }
        let provider = select_provider(&providers, failures);
        let Some(provider) = provider else {
            warn!(worker = slot.id, "worker has no configured provider");
            break;
        };
        *slot.provider.write().await = Some(provider);
        info!(
            worker = slot.id,
            provider = provider.as_str(),
            attempt = failures + 1,
            "worker connecting"
        );
        match connect_path(&cfg, provider, slot.id, engine.clone(), relay_only).await {
            Ok(connected) => {
                let ConnectedPath {
                    path,
                    route_ips,
                    provider_impl,
                    bootstrap,
                    provider_data,
                } = connected;
                *slot.route_ips.write().await = route_ips;
                slot.healthy.store(true, Ordering::Release);
                info!(
                    worker = slot.id,
                    provider = provider.as_str(),
                    path = path.name(),
                    session_id = engine.session_id(),
                    "worker healthy"
                );
                let migrated = recovery.lock().await.on_path_migration();
                for retry in migrated {
                    match engine.encapsulate_ip_with_id(retry.assoc_id, retry.packet_id, &retry.payload) {
                        Ok(datagrams) => {
                            for datagram in datagrams {
                                let delay = recovery.lock().await.reserve_pacing(datagram.len());
                                if !delay.is_zero() {
                                    tokio::time::sleep(delay).await;
                                }
                                if path.send(&datagram).await.is_err() {
                                    break;
                                }
                            }
                        }
                        Err(error) => warn!(worker = slot.id, %error, "recovery re-encapsulation failed"),
                    }
                }
                let connected_at = Instant::now();
                let result = run_worker(
                    slot.clone(),
                    engine.clone(),
                    path.clone(),
                    recovery.clone(),
                    &mut outbound_rx,
                    inbound_tx.clone(),
                    shutdown.clone(),
                )
                .await;
                slot.healthy.store(false, Ordering::Release);
                let _ = path.close().await;
                if let Err(error) = provider_impl.cleanup(&provider_data, &bootstrap).await {
                    warn!(worker = slot.id, provider = provider.as_str(), %error, "provider cleanup failed");
                }
                if *shutdown.borrow() {
                    break;
                }
                warn!(
                    worker = slot.id,
                    provider = provider.as_str(),
                    error = %result.err().unwrap_or_else(|| anyhow!("worker stopped")),
                    "worker lost; recovering"
                );
                failures = if connected_at.elapsed() >= Duration::from_secs(30) {
                    0
                } else {
                    failures.saturating_add(1)
                };
            }
            Err(error) => {
                slot.healthy.store(false, Ordering::Release);
                slot.route_ips.write().await.clear();
                warn!(
                    worker = slot.id,
                    provider = provider.as_str(),
                    %error,
                    "worker connection failed"
                );
                failures = failures.saturating_add(1);
            }
        }
        let jitter = ((slot.id as u64) * 37) % 250;
        let backoff = Duration::from_millis(
            (250u64 << failures.min(4)).min(5_000).saturating_add(jitter),
        );
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
        }
    }
    slot.healthy.store(false, Ordering::Release);
    info!(worker = slot.id, "worker stopped");
}

fn select_provider(providers: &[ProviderName], failures: usize) -> Option<ProviderName> {
    let primary = *providers.first()?;
    if providers.len() == 1 {
        return Some(primary);
    }
    let fallbacks = &providers[1..];
    let cycle = PRIMARY_RETRIES_BEFORE_FALLBACK + fallbacks.len();
    let position = failures % cycle;
    if position < PRIMARY_RETRIES_BEFORE_FALLBACK {
        Some(primary)
    } else {
        Some(fallbacks[position - PRIMARY_RETRIES_BEFORE_FALLBACK])
    }
}

struct ConnectedPath {
    path: Arc<dyn DatagramPath>,
    route_ips: Vec<Ipv4Addr>,
    provider_impl: Box<dyn Provider>,
    bootstrap: ProviderBootstrap,
    provider_data: serde_json::Value,
}

async fn connect_path(
    cfg: &ConnectionConfig,
    provider_name: ProviderName,
    worker_id: usize,
    engine: Arc<SessionEngine>,
    relay_only: bool,
) -> Result<ConnectedPath> {
    let mut data = cfg
        .provider_data_for(provider_name)
        .cloned()
        .with_context(|| format!("missing provider_data.{}", provider_name.as_str()))?;
    // Jazz assigns independent signalling/WebRTC identities per transport worker.
    // Session IDs already isolate ToTS packets; unique participant names also make
    // provider-side diagnostics unambiguous when many workers share one room.
    if provider_name == ProviderName::Jazz {
        if let Some(object) = data.as_object_mut() {
            object.insert(
                "name".into(),
                serde_json::Value::String(format!("ToTS worker {}", worker_id + 1)),
            );
        }
    }
    let provider = make_provider(&provider_name)?;
    let bootstrap = provider.bootstrap(&data).await?;
    let peer_ip = cfg.peer_ip()?;
    let mut candidates: Vec<Arc<dyn DatagramPath>> = Vec::new();
    if let Some(path) = provider.connect_native(&data).await? {
        candidates.push(path);
    } else {
        if !relay_only {
            let direct: Arc<dyn DatagramPath> = Arc::new(
                DirectUdpPath::connect(
                    format!("{}-webrtc", provider_name.as_str()),
                    SocketAddr::new(peer_ip, cfg.webrtc_port),
                )
                .await?,
            );
            candidates.push(direct);
        }
        for creds in &bootstrap.turn {
            for url in creds
                .urls
                .iter()
                .filter(|url| url.starts_with("turn:") && !url.contains("transport=tcp"))
            {
                let mut single = creds.clone();
                single.urls = vec![url.clone()];
                match TurnUdpPath::connect(&single, SocketAddr::new(peer_ip, cfg.turn_port)).await {
                    Ok(path) => candidates.push(Arc::new(path)),
                    Err(error) => warn!(provider=provider_name.as_str(), %error, "TURN candidate failed"),
                }
            }
        }
    }
    if candidates.is_empty() {
        let _ = provider.cleanup(&data, &bootstrap).await;
        anyhow::bail!("provider {} produced no usable path", provider_name.as_str());
    }
    let mut best: Option<(Duration, Arc<dyn DatagramPath>)> = None;
    for candidate in &candidates {
        match probe::probe(&engine, candidate.clone(), Duration::from_secs(3)).await {
            Ok(result) => {
                info!(provider=provider_name.as_str(), path=%result.path, rtt_ms=result.rtt.as_millis(), "worker path healthy");
                if best.as_ref().is_none_or(|(rtt, _)| result.rtt < *rtt) {
                    best = Some((result.rtt, candidate.clone()));
                }
            }
            Err(error) => warn!(provider=provider_name.as_str(), path=%candidate.name(), %error, "path probe failed"),
        }
    }
    let path = match best {
        Some((_, path)) => path,
        None => {
            for candidate in candidates {
                let _ = candidate.close().await;
            }
            let _ = provider.cleanup(&data, &bootstrap).await;
            anyhow::bail!("provider {} has no healthy ToTS path", provider_name.as_str());
        }
    };
    let mut route_ips = BTreeSet::new();
    for candidate in &candidates {
        route_ips.extend(candidate.route_ips());
    }
    for candidate in candidates {
        if !Arc::ptr_eq(&candidate, &path) {
            let _ = candidate.close().await;
        }
    }
    Ok(ConnectedPath {
        path,
        route_ips: route_ips.into_iter().collect(),
        provider_impl: provider,
        bootstrap,
        provider_data: data,
    })
}

async fn run_worker(
    slot: Arc<WorkerSlot>,
    engine: Arc<SessionEngine>,
    path: Arc<dyn DatagramPath>,
    recovery: Arc<Mutex<RecoveryController>>,
    outbound_rx: &mut mpsc::Receiver<OutboundPacket>,
    inbound_tx: mpsc::Sender<Vec<u8>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let last_rx = Arc::new(AtomicU64::new(now_millis()));
    let sender = send_loop(
        slot.clone(),
        engine.clone(),
        path.clone(),
        recovery.clone(),
        outbound_rx,
    );
    let receiver = recv_loop(
        slot.clone(),
        engine.clone(),
        path.clone(),
        recovery.clone(),
        inbound_tx,
        last_rx.clone(),
    );
    let recovery_loop = loss_loop(engine.clone(), path.clone(), recovery.clone());
    let health = health_loop(slot.id, engine, path, recovery, last_rx);
    tokio::select! {
        result = sender => result,
        result = receiver => result,
        result = recovery_loop => result,
        result = health => result,
        changed = shutdown.changed() => {
            match changed {
                Ok(()) if *shutdown.borrow() => Ok(()),
                Ok(()) => Err(anyhow!("worker shutdown state changed unexpectedly")),
                Err(_) => Ok(()),
            }
        }
    }
}

async fn send_loop(
    slot: Arc<WorkerSlot>,
    engine: Arc<SessionEngine>,
    path: Arc<dyn DatagramPath>,
    recovery: Arc<Mutex<RecoveryController>>,
    outbound_rx: &mut mpsc::Receiver<OutboundPacket>,
) -> Result<()> {
    while let Some(outbound) = outbound_rx.recv().await {
        // Do not replay stale latency-sensitive traffic after a provider outage.
        // Reliable/control packets remain queued and still use bounded recovery.
        if outbound.reliability == Reliability::Unreliable
            && outbound.enqueued_at.elapsed() > Duration::from_millis(750)
        {
            continue;
        }
        let (packet_id, datagrams) = engine.encapsulate_ip(outbound.assoc_id, &outbound.bytes)?;
        let bytes: usize = datagrams.iter().map(Vec::len).sum();
        loop {
            let can_send = recovery.lock().await.can_send(bytes);
            if can_send {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        for datagram in &datagrams {
            let delay = recovery.lock().await.reserve_pacing(datagram.len());
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            path.send(datagram).await?;
        }
        recovery.lock().await.on_sent(
            packet_id,
            outbound.assoc_id,
            outbound.bytes,
            bytes,
            outbound.reliability,
        );
        slot.tx_packets.fetch_add(1, Ordering::Relaxed);
    }
    Err(anyhow!("worker outbound queue closed"))
}

async fn recv_loop(
    slot: Arc<WorkerSlot>,
    engine: Arc<SessionEngine>,
    path: Arc<dyn DatagramPath>,
    recovery: Arc<Mutex<RecoveryController>>,
    inbound_tx: mpsc::Sender<Vec<u8>>,
    last_rx: Arc<AtomicU64>,
) -> Result<()> {
    let mut buf = vec![0u8; 65535];
    loop {
        let n = path.recv(&mut buf).await?;
        let decoded = match engine.decapsulate(&buf[..n]) {
            Ok(Some(decoded)) => decoded,
            Ok(None)
            | Err(tots_core::Error::Replay)
            | Err(tots_core::Error::Packet(_))
            | Err(tots_core::Error::Crypto) => continue,
            Err(error) => return Err(error.into()),
        };
        last_rx.store(now_millis(), Ordering::Relaxed);
        match decoded {
            DecodedPacket::Ip {
                packet_id, bytes, ..
            } => {
                let ack = engine.make_ack(packet_id)?;
                path.send(&ack).await?;
                slot.rx_packets.fetch_add(1, Ordering::Relaxed);
                inbound_tx
                    .send(bytes)
                    .await
                    .map_err(|_| anyhow!("TUN inbound queue closed"))?;
            }
            DecodedPacket::Duplicate { packet_id } => {
                path.send(&engine.make_ack(packet_id)?).await?;
            }
            DecodedPacket::Ack { packet_id } => {
                recovery.lock().await.on_ack(packet_id);
            }
            DecodedPacket::AckRanges { ranges } => {
                recovery.lock().await.on_ack_ranges(&ranges);
            }
            DecodedPacket::Probe { token, sent_micros } => {
                path.send(&engine.make_probe_ack(token, sent_micros)?).await?;
            }
            DecodedPacket::ProbeAck { sent_micros, .. } => {
                let now = now_micros();
                if now >= sent_micros {
                    recovery
                        .lock()
                        .await
                        .observe_rtt(Duration::from_micros(now - sent_micros));
                }
            }
            DecodedPacket::Heartbeat { .. } => {}
            DecodedPacket::Dissociate { .. } => {}
        }
    }
}

async fn loss_loop(
    engine: Arc<SessionEngine>,
    path: Arc<dyn DatagramPath>,
    recovery: Arc<Mutex<RecoveryController>>,
) -> Result<()> {
    let mut tick = tokio::time::interval(Duration::from_millis(20));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let retransmits = recovery.lock().await.poll_losses();
        for retransmit in retransmits {
            let datagrams = engine.encapsulate_ip_with_id(
                retransmit.assoc_id,
                retransmit.packet_id,
                &retransmit.payload,
            )?;
            for datagram in datagrams {
                let delay = recovery.lock().await.reserve_pacing(datagram.len());
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                path.send(&datagram).await?;
            }
        }
    }
}

async fn health_loop(
    worker_id: usize,
    engine: Arc<SessionEngine>,
    path: Arc<dyn DatagramPath>,
    recovery: Arc<Mutex<RecoveryController>>,
    last_rx: Arc<AtomicU64>,
) -> Result<()> {
    let mut tick = tokio::time::interval(HEALTH_PROBE_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut counter = 0u64;
    let mut best_srtt: Option<Duration> = None;
    let mut degraded_ticks = 0u8;
    loop {
        tick.tick().await;
        let now = now_millis();
        let last = last_rx.load(Ordering::Relaxed);
        if now.saturating_sub(last) > HEALTH_TIMEOUT.as_millis() as u64 {
            anyhow::bail!("worker {worker_id} path liveness timeout");
        }

        if let Some(srtt) = recovery.lock().await.srtt() {
            match best_srtt {
                None => best_srtt = Some(srtt),
                Some(best) if srtt < best => {
                    best_srtt = Some(srtt);
                    degraded_ticks = 0;
                }
                Some(best) => {
                    let degraded_at = best
                        .saturating_mul(4)
                        .saturating_add(Duration::from_millis(100));
                    if srtt > degraded_at {
                        degraded_ticks = degraded_ticks.saturating_add(1);
                        if degraded_ticks >= 3 {
                            anyhow::bail!(
                                "worker {worker_id} path RTT degraded from {} ms to {} ms",
                                best.as_millis(),
                                srtt.as_millis()
                            );
                        }
                    } else {
                        degraded_ticks = 0;
                    }
                }
            }
        }

        counter = counter.wrapping_add(1);
        let token = ((worker_id as u64) << 48) ^ counter ^ now;
        path.send(&engine.make_probe(token, now_micros())?)
            .await?;
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_is_retried_before_fallback() {
        let providers = [ProviderName::Vk, ProviderName::Jazz];
        assert_eq!(select_provider(&providers, 0), Some(ProviderName::Vk));
        assert_eq!(select_provider(&providers, 2), Some(ProviderName::Vk));
        assert_eq!(select_provider(&providers, 3), Some(ProviderName::Jazz));
        assert_eq!(select_provider(&providers, 4), Some(ProviderName::Vk));
    }
}
