use crate::bootstrap::{BootstrapPool, VK_WORKERS_PER_BOOTSTRAP};
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
use tokio::sync::{mpsc, watch, Mutex, RwLock, Semaphore};
use tots_core::{
    config::{ConnectionConfig, ProviderName},
    crypto::PacketCrypto,
    flow::{FlowManager, FlowScheduler, Reliability},
    providers::{make_provider, Provider, ProviderBootstrap},
    recovery::{RecoveryController, RecoveryStats},
    session::{DecodedPacket, PreparedFrame, SessionEngine},
    transport::{probe, turn::TurnUdpPath, DatagramPath, DirectUdpPath},
};
use tracing::{info, warn};

const PRIMARY_RETRIES_BEFORE_FALLBACK: usize = 3;
const WORKER_QUEUE: usize = 2048;
const INBOUND_QUEUE: usize = 4096;
const HEALTH_PROBE_INTERVAL: Duration = Duration::from_secs(5);
const HEALTH_TIMEOUT: Duration = Duration::from_secs(15);
const STARTUP_PARALLELISM: usize = VK_WORKERS_PER_BOOTSTRAP;
const ACK_QUEUE: usize = 4096;
const ACK_BATCH_MAX: usize = 32;
const ACK_BATCH_DELAY: Duration = Duration::from_millis(2);

fn validate_worker_request(cfg: &ConnectionConfig, workers: usize) -> Result<()> {
    if workers == 0 || workers > 256 {
        anyhow::bail!("--n must be between 1 and 256");
    }
    cfg.validate()?;
    if cfg.has_provider(ProviderName::Jazz) {
        let data = cfg
            .provider_data_for(ProviderName::Jazz)
            .context("missing Jazz configuration")?;
        let rooms = tots_core::providers::jazz::worker_room_count(data)?;
        if workers > rooms {
            anyhow::bail!("--n {workers} needs {workers} provisioned Jazz worker rooms because any worker may fail over to Jazz; config has {rooms}. Create the config with provider_data.jazz.worker_count={workers}");
        }
    }
    Ok(())
}

fn percent_value(value: &serde_json::Value, provider: &str) -> Result<usize> {
    let percent = match value {
        serde_json::Value::String(value) => value
            .parse::<usize>()
            .with_context(|| format!("workers_percent.{provider} must be an integer percent"))?,
        serde_json::Value::Number(value) => value
            .as_u64()
            .map(|value| value as usize)
            .with_context(|| format!("workers_percent.{provider} must be an integer percent"))?,
        _ => anyhow::bail!("workers_percent.{provider} must be a string or integer percent"),
    };
    if percent > 100 {
        anyhow::bail!("workers_percent.{provider} must be 0..=100");
    }
    Ok(percent)
}

fn worker_provider_orders(
    cfg: &ConnectionConfig,
    workers: usize,
    workers_percent: Option<&str>,
) -> Result<Vec<Vec<ProviderName>>> {
    let configured = cfg.provider_order();
    if configured.is_empty() {
        anyhow::bail!("no configured provider");
    }
    let Some(raw) = workers_percent else {
        return Ok((0..workers).map(|_| configured.clone()).collect());
    };
    let value: serde_json::Value = serde_json::from_str(raw).context("invalid --workers_percent JSON")?;
    let object = value
        .as_object()
        .context("--workers_percent must be a JSON object")?;
    if object.is_empty() {
        anyhow::bail!("--workers_percent cannot be empty");
    }

    let mut weighted = Vec::new();
    let mut total = 0usize;
    for (name, value) in object {
        let provider = ProviderName::parse(name)
            .map_err(|_| anyhow!("unsupported workers_percent provider {name}"))?;
        if provider == ProviderName::Unknown {
            anyhow::bail!("unsupported workers_percent provider {name}");
        }
        let percent = percent_value(value, name)?;
        if percent > 0 && !configured.contains(&provider) {
            anyhow::bail!("workers_percent requests {name}, but provider_data.{name} is not configured");
        }
        total = total.saturating_add(percent);
        if percent > 0 {
            weighted.push((provider, percent));
        }
    }
    if total != 100 {
        anyhow::bail!("--workers_percent values must sum to 100, got {total}");
    }
    if weighted.is_empty() {
        anyhow::bail!("--workers_percent must assign at least one provider");
    }

    // Largest-remainder apportionment gives an exact worker count even when --n is not
    // divisible by 100. Ties prefer the config's primary provider.
    let mut counts: Vec<(ProviderName, usize, usize)> = weighted
        .iter()
        .map(|(provider, percent)| {
            let scaled = workers.saturating_mul(*percent);
            (*provider, scaled / 100, scaled % 100)
        })
        .collect();
    let mut assigned: usize = counts.iter().map(|(_, count, _)| *count).sum();
    while assigned < workers {
        let index = counts
            .iter()
            .enumerate()
            .max_by_key(|(_, (provider, _, remainder))| {
                (*remainder, if *provider == cfg.provider { 1usize } else { 0usize })
            })
            .map(|(index, _)| index)
            .expect("positive worker distribution");
        counts[index].1 += 1;
        counts[index].2 = 0;
        assigned += 1;
    }

    // Smooth weighted round-robin spreads provider startup across the pool rather than
    // starting one large provider block and then another.
    let mut remaining: Vec<(ProviderName, usize, usize, isize)> = counts
        .into_iter()
        .map(|(provider, count, _)| (provider, count, count, 0))
        .collect();
    let total_workers = workers as isize;
    let mut preferences = Vec::with_capacity(workers);
    for _ in 0..workers {
        for (_, remaining_count, weight, score) in &mut remaining {
            if *remaining_count > 0 {
                *score += *weight as isize;
            }
        }
        let index = remaining
            .iter()
            .enumerate()
            .filter(|(_, (_, remaining_count, _, _))| *remaining_count > 0)
            .max_by_key(|(_, (provider, _, _, score))| {
                (*score, if *provider == cfg.provider { 1usize } else { 0usize })
            })
            .map(|(index, _)| index)
            .expect("worker distribution exhausted early");
        let preferred = remaining[index].0;
        remaining[index].1 -= 1;
        remaining[index].3 -= total_workers;
        let mut order = vec![preferred];
        order.extend(configured.iter().copied().filter(|provider| *provider != preferred));
        preferences.push(order);
    }
    Ok(preferences)
}

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

#[derive(Default)]
struct ReceiveDiagnostics {
    path_datagrams: AtomicU64,
    path_bytes: AtomicU64,
    crypto_errors: AtomicU64,
    packet_errors: AtomicU64,
    decode_none: AtomicU64,
    decoded_ip_packets: AtomicU64,
    decoded_ip_bytes: AtomicU64,
    duplicates: AtomicU64,
    ack_packets_queued: AtomicU64,
    ack_datagrams_sent: AtomicU64,
    ack_queue_wait_ns_sum: AtomicU64,
    ack_queue_wait_ns_max: AtomicU64,
    tun_inbound_wait_ns_sum: AtomicU64,
    tun_inbound_wait_ns_max: AtomicU64,
}

struct WorkerSlot {
    id: usize,
    preferred_provider: ProviderName,
    healthy: AtomicBool,
    provider: RwLock<Option<ProviderName>>,
    sender: mpsc::Sender<OutboundPacket>,
    route_ips: RwLock<Vec<Ipv4Addr>>,
    tx_packets: AtomicU64,
    rx_packets: AtomicU64,
    rx_replays: AtomicU64,
    receive: ReceiveDiagnostics,
    recovery: Arc<Mutex<RecoveryController>>,
}

pub struct WorkerManager {
    cfg: ConnectionConfig,
    slots: Vec<Arc<WorkerSlot>>,
    scheduler: Mutex<FlowScheduler>,
    shutdown_tx: watch::Sender<bool>,
    handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    bootstraps: Arc<BootstrapPool>,
    pub stats: Arc<TrafficStats>,
}

impl WorkerManager {
    pub async fn start(
        cfg: ConnectionConfig,
        workers: usize,
        relay_only: bool,
        workers_percent: Option<&str>,
    ) -> Result<(Arc<Self>, mpsc::Receiver<Vec<u8>>)> {
        validate_worker_request(&cfg, workers)?;
        let provider_orders = worker_provider_orders(&cfg, workers, workers_percent)?;
        let key_config = cfg.clone();
        let crypto = tokio::task::spawn_blocking(move || {
            PacketCrypto::derive(&key_config.password, key_config.device_id)
        })
        .await
        .context("packet key derivation task failed")??;
        let bootstraps = Arc::new(BootstrapPool::new());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (inbound_tx, inbound_rx) = mpsc::channel(INBOUND_QUEUE);
        let mut slots = Vec::with_capacity(workers);
        let mut receivers = Vec::with_capacity(workers);
        for id in 0..workers {
            let (sender, receiver) = mpsc::channel(WORKER_QUEUE);
            slots.push(Arc::new(WorkerSlot {
                id,
                preferred_provider: provider_orders[id][0],
                healthy: AtomicBool::new(false),
                provider: RwLock::new(None),
                sender,
                route_ips: RwLock::new(Vec::new()),
                tx_packets: AtomicU64::new(0),
                rx_packets: AtomicU64::new(0),
                rx_replays: AtomicU64::new(0),
                receive: ReceiveDiagnostics::default(),
                recovery: Arc::new(Mutex::new(RecoveryController::default())),
            }));
            receivers.push(receiver);
        }
        let manager = Arc::new(Self {
            cfg: cfg.clone(),
            slots,
            scheduler: Mutex::new(FlowScheduler::default()),
            shutdown_tx,
            handles: Mutex::new(Vec::new()),
            bootstraps: bootstraps.clone(),
            stats: Arc::new(TrafficStats::default()),
        });
        let startup_gate = Arc::new(Semaphore::new(STARTUP_PARALLELISM));
        let mut handles = Vec::with_capacity(workers);
        for ((slot, receiver), providers) in manager
            .slots
            .iter()
            .cloned()
            .zip(receivers)
            .zip(provider_orders)
        {
            handles.push(tokio::spawn(worker_supervisor(
                cfg.clone(),
                crypto.clone(),
                bootstraps.clone(),
                startup_gate.clone(),
                providers,
                slot,
                receiver,
                inbound_tx.clone(),
                shutdown_rx.clone(),
                relay_only,
            )));
        }
        let vk_target = manager
            .slots
            .iter()
            .filter(|slot| slot.preferred_provider == ProviderName::Vk)
            .count();
        let jazz_target = manager
            .slots
            .iter()
            .filter(|slot| slot.preferred_provider == ProviderName::Jazz)
            .count();
        info!(
            workers,
            workers_vk_target = vk_target,
            workers_jazz_target = jazz_target,
            startup_parallelism = STARTUP_PARALLELISM,
            "worker supervisors launched"
        );
        *manager.handles.lock().await = handles;
        Ok((manager, inbound_rx))
    }

    pub async fn wait_for_healthy(&self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.healthy_count() == self.desired_count() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!(
                    "only {}/{} workers became healthy before timeout",
                    self.healthy_count(),
                    self.desired_count()
                );
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
        if let IpAddr::V4(peer) = self
            .cfg
            .peer_ip()
            .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
        {
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

    pub fn stop(&self) {
        self.bootstraps.stop();
        let _ = self.shutdown_tx.send(true);
    }

    pub async fn shutdown(&self) {
        self.stop();
        let handles = {
            let mut guard = self.handles.lock().await;
            std::mem::take(&mut *guard)
        };
        for handle in handles {
            let _ = handle.await;
        }
        if let Ok(provider) = make_provider(&ProviderName::Vk) {
            if let Err(error) = self.bootstraps.cleanup(provider.as_ref()).await {
                warn!(%error, "VK bootstrap cleanup failed");
            }
        }
    }

    pub fn log_status(&self) {
        let tx = self.stats.tun_tx_bytes.load(Ordering::Relaxed);
        let rx = self.stats.tun_rx_bytes.load(Ordering::Relaxed);
        let total = tx.saturating_add(rx);
        let mut vk_workers = 0usize;
        let mut jazz_workers = 0usize;
        let vk_target = self
            .slots
            .iter()
            .filter(|slot| slot.preferred_provider == ProviderName::Vk)
            .count();
        let jazz_target = self
            .slots
            .iter()
            .filter(|slot| slot.preferred_provider == ProviderName::Jazz)
            .count();
        let mut cwnd_bytes = 0usize;
        let mut inflight_bytes = 0usize;
        let mut rtt_min_us = u128::MAX;
        let mut rtt_max_us = 0u128;
        let mut queued = 0usize;
        let mut recovery_stats = RecoveryStats::default();
        let mut recovery_sampled_workers = 0usize;
        let mut replay_rejections = 0u64;
        let mut best_effort_samples = 0usize;
        let mut path_rx_datagrams = 0u64;
        let mut path_rx_bytes = 0u64;
        let mut decode_crypto_errors = 0u64;
        let mut decode_packet_errors = 0u64;
        let mut decode_none = 0u64;
        let mut decoded_ip_packets = 0u64;
        let mut decoded_ip_bytes = 0u64;
        let mut duplicate_frames = 0u64;
        let mut rx_packets_after_ack = 0u64;
        let mut ack_packets_queued = 0u64;
        let mut ack_datagrams_sent = 0u64;
        let mut ack_queue_wait_ns_sum = 0u64;
        let mut ack_queue_wait_ns_max = 0u64;
        let mut tun_inbound_wait_ns_sum = 0u64;
        let mut tun_inbound_wait_ns_max = 0u64;
        for slot in &self.slots {
            let receive = &slot.receive;
            path_rx_datagrams += receive.path_datagrams.load(Ordering::Relaxed);
            path_rx_bytes += receive.path_bytes.load(Ordering::Relaxed);
            decode_crypto_errors += receive.crypto_errors.load(Ordering::Relaxed);
            decode_packet_errors += receive.packet_errors.load(Ordering::Relaxed);
            decode_none += receive.decode_none.load(Ordering::Relaxed);
            decoded_ip_packets += receive.decoded_ip_packets.load(Ordering::Relaxed);
            decoded_ip_bytes += receive.decoded_ip_bytes.load(Ordering::Relaxed);
            duplicate_frames += receive.duplicates.load(Ordering::Relaxed);
            rx_packets_after_ack += slot.rx_packets.load(Ordering::Relaxed);
            ack_packets_queued += receive.ack_packets_queued.load(Ordering::Relaxed);
            ack_datagrams_sent += receive.ack_datagrams_sent.load(Ordering::Relaxed);
            ack_queue_wait_ns_sum += receive.ack_queue_wait_ns_sum.load(Ordering::Relaxed);
            ack_queue_wait_ns_max =
                ack_queue_wait_ns_max.max(receive.ack_queue_wait_ns_max.load(Ordering::Relaxed));
            tun_inbound_wait_ns_sum += receive.tun_inbound_wait_ns_sum.load(Ordering::Relaxed);
            tun_inbound_wait_ns_max = tun_inbound_wait_ns_max
                .max(receive.tun_inbound_wait_ns_max.load(Ordering::Relaxed));
            replay_rejections += slot.rx_replays.load(Ordering::Relaxed);
            if !slot.healthy.load(Ordering::Relaxed) {
                continue;
            }
            queued += WORKER_QUEUE - slot.sender.capacity();
            if let Ok(recovery) = slot.recovery.try_lock() {
                recovery_sampled_workers += 1;
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
                cwnd_bytes += recovery.cwnd();
                inflight_bytes += recovery.bytes_in_flight();
                if let Some(rtt) = recovery.srtt() {
                    rtt_min_us = rtt_min_us.min(rtt.as_micros());
                    rtt_max_us = rtt_max_us.max(rtt.as_micros());
                }
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
            workers_vk_target = vk_target,
            workers_jazz_target = jazz_target,
            turn_credentials_seen = self.bootstraps.credential_count(),
            cwnd_bytes,
            inflight_bytes,
            queued,
            recovery_sampled_workers,
            replay_rejections,
            path_rx_datagrams,
            path_rx_bytes,
            decode_crypto_errors,
            decode_packet_errors,
            decode_none,
            decoded_ip_packets,
            decoded_ip_bytes,
            duplicate_frames,
            rx_packets_after_ack,
            ack_packets_queued,
            ack_datagrams_sent,
            ack_queue_wait_ms_sum = ack_queue_wait_ns_sum as f64 / 1_000_000.0,
            ack_queue_wait_ms_max = ack_queue_wait_ns_max as f64 / 1_000_000.0,
            tun_inbound_wait_ms_sum = tun_inbound_wait_ns_sum as f64 / 1_000_000.0,
            tun_inbound_wait_ms_max = tun_inbound_wait_ns_max as f64 / 1_000_000.0,
            best_effort_samples,
            best_effort_evictions = recovery_stats.best_effort_evictions,
            recovery_sent_packets = recovery_stats.sent_packets,
            recovery_acked_packets = recovery_stats.acked_packets,
            recovery_packet_threshold_losses = recovery_stats.packet_threshold_losses,
            recovery_time_threshold_losses = recovery_stats.time_threshold_losses,
            recovery_retransmits = recovery_stats.retransmits,
            recovery_late_acked_packets = recovery_stats.late_acked_packets,
            recovery_pto_expirations = recovery_stats.pto_expirations,
            recovery_pto_retransmits = recovery_stats.pto_retransmits,
            rtt_min_ms = if rtt_min_us == u128::MAX {
                0.0
            } else {
                rtt_min_us as f64 / 1000.0
            },
            rtt_max_ms = rtt_max_us as f64 / 1000.0,
            vk_estimated_capacity_mbps = FlowScheduler::estimated_capacity_mbps(vk_workers),
            "ToTS status"
        );
    }
}

#[allow(clippy::too_many_arguments)] // Keep each worker's owned setup and channels explicit.
async fn worker_supervisor(
    cfg: ConnectionConfig,
    crypto: PacketCrypto,
    bootstraps: Arc<BootstrapPool>,
    startup_gate: Arc<Semaphore>,
    providers: Vec<ProviderName>,
    slot: Arc<WorkerSlot>,
    mut outbound_rx: mpsc::Receiver<OutboundPacket>,
    inbound_tx: mpsc::Sender<Vec<u8>>,
    mut shutdown: watch::Receiver<bool>,
    relay_only: bool,
) {
    let engine = match SessionEngine::new_with_crypto(cfg.clone(), crypto) {
        Ok(engine) => Arc::new(engine),
        Err(error) => {
            warn!(worker = slot.id, %error, "worker engine initialization failed");
            return;
        }
    };
    let recovery = slot.recovery.clone();
    let mut failures = 0usize;
    info!(
        worker = slot.id,
        session_id = engine.session_id(),
        "worker starting"
    );
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
        let permit = tokio::select! {
            permit = startup_gate.clone().acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break,
            },
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
                continue;
            }
        };
        let connected = connect_path(
            &cfg,
            provider,
            slot.id,
            failures,
            engine.clone(),
            relay_only,
            &bootstraps,
        )
        .await;
        drop(permit);
        match connected {
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
                    match engine.prepare_ip_with_id(retry.assoc_id, retry.packet_id, &retry.payload)
                    {
                        Ok(frames) => {
                            if let Err(error) = send_prepared_frames(
                                &engine,
                                path.as_ref(),
                                &recovery,
                                &frames,
                                Reliability::Reliable,
                            )
                            .await
                            {
                                warn!(worker = slot.id, %error, "migration resend failed");
                                break;
                            }
                        }
                        Err(error) => {
                            warn!(worker = slot.id, %error, "recovery re-encapsulation failed")
                        }
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
                if provider != ProviderName::Vk {
                    if let Err(error) = provider_impl.cleanup(&provider_data, &bootstrap).await {
                        warn!(worker = slot.id, provider = provider.as_str(), %error, "provider cleanup failed");
                    }
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
            (250u64 << failures.min(4))
                .min(5_000)
                .saturating_add(jitter),
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
    attempt: usize,
    engine: Arc<SessionEngine>,
    relay_only: bool,
    bootstraps: &BootstrapPool,
) -> Result<ConnectedPath> {
    let mut data = cfg
        .provider_data_for(provider_name)
        .cloned()
        .with_context(|| format!("missing provider_data.{}", provider_name.as_str()))?;
    // Jazz assigns independent signalling/WebRTC identities per transport worker.
    // Session IDs already isolate ToTS packets; unique participant names also make
    // provider-side diagnostics unambiguous when many workers share one room.
    if provider_name == ProviderName::Jazz {
        data = tots_core::providers::jazz::worker_room_data(&data, worker_id)?;
        if let Some(object) = data.as_object_mut() {
            object.insert(
                "name".into(),
                serde_json::Value::String(format!("ToTS worker {}", worker_id + 1)),
            );
        }
    }
    let provider = make_provider(&provider_name)?;
    let bootstrap = if provider_name == ProviderName::Vk {
        bootstraps.acquire(provider.as_ref(), &data).await?
    } else {
        provider.bootstrap(&data).await?
    };
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
        let allocations: Vec<_> = bootstrap
            .turn
            .iter()
            .flat_map(|creds| {
                creds
                    .urls
                    .iter()
                    .filter(|url| url.starts_with("turn:") && !url.contains("transport=tcp"))
                    .map(move |url| (creds, url))
            })
            .collect();
        for (index, &(creds, url)) in allocations.iter().enumerate() {
            // One allocation attempt consumes one credential lease. Rotate endpoints across workers.
            if provider_name == ProviderName::Vk
                && index != (worker_id + attempt) % allocations.len()
            {
                continue;
            }
            let mut single = creds.clone();
            single.urls = vec![url.clone()];
            match TurnUdpPath::connect(&single, SocketAddr::new(peer_ip, cfg.turn_port)).await {
                Ok(path) => candidates.push(Arc::new(path)),
                Err(error) => {
                    warn!(provider=provider_name.as_str(), %error, "TURN candidate failed")
                }
            }
        }
    }
    if candidates.is_empty() {
        if provider_name != ProviderName::Vk {
            let _ = provider.cleanup(&data, &bootstrap).await;
        }
        anyhow::bail!(
            "provider {} produced no usable path",
            provider_name.as_str()
        );
    }
    // Probe all paths for this worker concurrently. Worker-level setup is already bounded by
    // STARTUP_PARALLELISM, so this removes serial 3-second probe tails without creating an
    // unbounded provider thundering herd.
    let mut probes = tokio::task::JoinSet::new();
    for candidate in &candidates {
        let candidate = candidate.clone();
        let engine = engine.clone();
        probes.spawn(async move {
            let result = probe::probe(&engine, candidate.clone(), Duration::from_secs(3)).await;
            (candidate, result)
        });
    }
    let mut best: Option<(Duration, Arc<dyn DatagramPath>)> = None;
    while let Some(joined) = probes.join_next().await {
        match joined {
            Ok((candidate, Ok(result))) => {
                info!(provider=provider_name.as_str(), path=%result.path, rtt_ms=result.rtt.as_millis(), "worker path healthy");
                if best.as_ref().is_none_or(|(rtt, _)| result.rtt < *rtt) {
                    best = Some((result.rtt, candidate));
                }
            }
            Ok((candidate, Err(error))) => {
                warn!(provider=provider_name.as_str(), path=%candidate.name(), %error, "path probe failed")
            }
            Err(error) => warn!(provider=provider_name.as_str(), %error, "path probe task failed"),
        }
    }
    let path = match best {
        Some((_, path)) => path,
        None => {
            for candidate in candidates {
                let _ = candidate.close().await;
            }
            if provider_name != ProviderName::Vk {
                let _ = provider.cleanup(&data, &bootstrap).await;
            }
            anyhow::bail!(
                "provider {} has no healthy ToTS path",
                provider_name.as_str()
            );
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
    let (ack_tx, ack_rx) = mpsc::channel(ACK_QUEUE);
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
        ack_tx,
    );
    let acknowledgements = ack_loop(slot.clone(), engine.clone(), path.clone(), ack_rx);
    let recovery_loop = loss_loop(engine.clone(), path.clone(), recovery.clone());
    let health = health_loop(slot.id, engine, path, recovery, last_rx);
    tokio::select! {
        result = sender => result,
        result = receiver => result,
        result = acknowledgements => result,
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

async fn send_prepared_frames(
    engine: &SessionEngine,
    path: &dyn DatagramPath,
    recovery: &Mutex<RecoveryController>,
    frames: &[PreparedFrame],
    reliability: Reliability,
) -> Result<()> {
    for frame in frames {
        let delay = recovery
            .lock()
            .await
            .reserve_pacing_for(frame.wire_len(), reliability);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        // ACK traffic may advance the replay window while a data frame waits.
        let datagram = engine.seal_prepared(frame)?;
        path.send(&datagram).await?;
    }
    Ok(())
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
        let (packet_id, frames) = engine.prepare_ip(outbound.assoc_id, &outbound.bytes)?;
        let bytes: usize = frames.iter().map(PreparedFrame::wire_len).sum();
        loop {
            let can_send = recovery
                .lock()
                .await
                .can_send_for(bytes, outbound.reliability);
            if can_send {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        send_prepared_frames(
            &engine,
            path.as_ref(),
            &recovery,
            &frames,
            outbound.reliability,
        )
        .await?;
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
    ack_tx: mpsc::Sender<u32>,
) -> Result<()> {
    let mut buf = vec![0u8; 65535];
    loop {
        let n = path.recv(&mut buf).await?;
        slot.receive.path_datagrams.fetch_add(1, Ordering::Relaxed);
        slot.receive
            .path_bytes
            .fetch_add(n as u64, Ordering::Relaxed);
        let decoded = match engine.decapsulate(&buf[..n]) {
            Ok(Some(decoded)) => decoded,
            Err(tots_core::Error::Replay) => {
                slot.rx_replays.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            Ok(None) => {
                slot.receive.decode_none.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            Err(tots_core::Error::Packet(_)) => {
                slot.receive.packet_errors.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            Err(tots_core::Error::Crypto) => {
                slot.receive.crypto_errors.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        last_rx.store(now_millis(), Ordering::Relaxed);
        match decoded {
            DecodedPacket::Ip {
                packet_id, bytes, ..
            } => {
                slot.receive
                    .decoded_ip_packets
                    .fetch_add(1, Ordering::Relaxed);
                slot.receive
                    .decoded_ip_bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                queue_ack(&slot, &ack_tx, packet_id).await?;
                slot.rx_packets.fetch_add(1, Ordering::Relaxed);
                let started = Instant::now();
                let sent = inbound_tx.send(bytes).await;
                let waited = started.elapsed().as_nanos() as u64;
                slot.receive
                    .tun_inbound_wait_ns_sum
                    .fetch_add(waited, Ordering::Relaxed);
                slot.receive
                    .tun_inbound_wait_ns_max
                    .fetch_max(waited, Ordering::Relaxed);
                sent.map_err(|_| anyhow!("TUN inbound queue closed"))?;
            }
            DecodedPacket::Duplicate { packet_id } => {
                slot.receive.duplicates.fetch_add(1, Ordering::Relaxed);
                queue_ack(&slot, &ack_tx, packet_id).await?;
            }
            DecodedPacket::Ack { packet_id } => {
                recovery.lock().await.on_ack(packet_id);
            }
            DecodedPacket::AckRanges { ranges } => {
                recovery.lock().await.on_ack_ranges(&ranges);
            }
            DecodedPacket::Probe { token, sent_micros } => {
                path.send(&engine.make_probe_ack(token, sent_micros)?)
                    .await?;
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

async fn queue_ack(slot: &WorkerSlot, ack_tx: &mpsc::Sender<u32>, packet_id: u32) -> Result<()> {
    slot.receive
        .ack_packets_queued
        .fetch_add(1, Ordering::Relaxed);
    match ack_tx.try_send(packet_id) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(packet_id)) => {
            let started = Instant::now();
            let sent = ack_tx.send(packet_id).await;
            let waited = started.elapsed().as_nanos() as u64;
            slot.receive
                .ack_queue_wait_ns_sum
                .fetch_add(waited, Ordering::Relaxed);
            slot.receive
                .ack_queue_wait_ns_max
                .fetch_max(waited, Ordering::Relaxed);
            sent.map_err(|_| anyhow!("ACK queue closed"))
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(anyhow!("ACK queue closed")),
    }
}

async fn ack_loop(
    slot: Arc<WorkerSlot>,
    engine: Arc<SessionEngine>,
    path: Arc<dyn DatagramPath>,
    mut ack_rx: mpsc::Receiver<u32>,
) -> Result<()> {
    let mut pending = Vec::with_capacity(ACK_BATCH_MAX);
    loop {
        let first = ack_rx
            .recv()
            .await
            .ok_or_else(|| anyhow!("ACK queue closed"))?;
        pending.push(first);
        let deadline = tokio::time::Instant::now() + ACK_BATCH_DELAY;
        while pending.len() < ACK_BATCH_MAX {
            match tokio::time::timeout_at(deadline, ack_rx.recv()).await {
                Ok(Some(packet_id)) => pending.push(packet_id),
                Ok(None) | Err(_) => break,
            }
        }
        let ack = engine.make_ack_batch(&pending)?;
        path.send(&ack).await?;
        slot.receive
            .ack_datagrams_sent
            .fetch_add(1, Ordering::Relaxed);
        pending.clear();
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
            let frames = engine.prepare_ip_with_id(
                retransmit.assoc_id,
                retransmit.packet_id,
                &retransmit.payload,
            )?;
            send_prepared_frames(
                &engine,
                path.as_ref(),
                &recovery,
                &frames,
                Reliability::Reliable,
            )
            .await?;
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
        path.send(&engine.make_probe(token, now_micros())?).await?;
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
    fn jazz_worker_request_cannot_exceed_provisioned_rooms() {
        let mut cfg = ConnectionConfig {
            version: 1,
            peer: "127.0.0.1".into(),
            webrtc_port: 1,
            turn_port: 2,
            password: "test-password".into(),
            device_id: uuid::Uuid::new_v4(),
            provider: ProviderName::Jazz,
            provider_data: serde_json::json!({"jazz":{"join_link":"https://example.test/room", "connector_url":"wss://example.test/connector"},"vk":{"vk_call_links":["test"]}}),
            mtu: 1200,
        };
        assert!(validate_worker_request(&cfg, 20).is_err());
        validate_worker_request(&cfg, 1).unwrap();
        cfg.provider = ProviderName::Vk;
        // Jazz is still configured as a fallback, so every slot must have a unique room.
        assert!(validate_worker_request(&cfg, 20).is_err());
        cfg.provider_data = serde_json::json!({"vk":{"vk_call_links":["test"]}});
        validate_worker_request(&cfg, 20).unwrap();
    }

    #[test]
    fn workers_percent_splits_preferred_providers_exactly() {
        let cfg = ConnectionConfig {
            version: 1,
            peer: "127.0.0.1".into(),
            webrtc_port: 1,
            turn_port: 2,
            password: "test-password".into(),
            device_id: uuid::Uuid::new_v4(),
            provider: ProviderName::Vk,
            provider_data: serde_json::json!({
                "vk":{"vk_call_links":["test"]},
                "jazz":{"join_link":"https://example.test/room", "connector_url":"wss://example.test/connector"}
            }),
            mtu: 1200,
        };
        let orders = worker_provider_orders(&cfg, 20, Some(r#"{"jazz":"50","vk":"50"}"#)).unwrap();
        assert_eq!(orders.len(), 20);
        assert_eq!(orders.iter().filter(|order| order[0] == ProviderName::Vk).count(), 10);
        assert_eq!(orders.iter().filter(|order| order[0] == ProviderName::Jazz).count(), 10);
        assert!(orders.iter().all(|order| order.len() == 2));
    }

    #[test]
    fn workers_percent_is_optional_and_validated() {
        let cfg = ConnectionConfig {
            version: 1,
            peer: "127.0.0.1".into(),
            webrtc_port: 1,
            turn_port: 2,
            password: "test-password".into(),
            device_id: uuid::Uuid::new_v4(),
            provider: ProviderName::Vk,
            provider_data: serde_json::json!({"vk":{"vk_call_links":["test"]}}),
            mtu: 1200,
        };
        let default_orders = worker_provider_orders(&cfg, 3, None).unwrap();
        assert!(default_orders.iter().all(|order| order[0] == ProviderName::Vk));
        assert!(worker_provider_orders(&cfg, 10, Some(r#"{"vk":90}"#)).is_err());
        assert!(worker_provider_orders(&cfg, 10, Some(r#"{"vk":50,"jazz":50}"#)).is_err());
        worker_provider_orders(&cfg, 10, Some(r#"{"vk":100}"#)).unwrap();
    }

    #[tokio::test]
    async fn pool_readiness_waits_for_every_requested_worker() {
        let (shutdown_tx, _) = watch::channel(false);
        let slots = (0..20)
            .map(|id| {
                let (sender, _) = mpsc::channel(1);
                Arc::new(WorkerSlot {
                    id,
                    preferred_provider: ProviderName::Vk,
                    healthy: AtomicBool::new(id < 10),
                    provider: RwLock::new(None),
                    sender,
                    route_ips: RwLock::new(Vec::new()),
                    tx_packets: AtomicU64::new(0),
                    rx_packets: AtomicU64::new(0),
                    rx_replays: AtomicU64::new(0),
                    receive: ReceiveDiagnostics::default(),
                    recovery: Arc::new(Mutex::new(RecoveryController::default())),
                })
            })
            .collect();
        let manager = WorkerManager {
            cfg: ConnectionConfig {
                version: 1,
                peer: "127.0.0.1".into(),
                webrtc_port: 1,
                turn_port: 2,
                password: "test-password".into(),
                device_id: uuid::Uuid::new_v4(),
                provider: ProviderName::Vk,
                provider_data: serde_json::json!({"vk_call_links":["test"]}),
                mtu: 1200,
            },
            slots,
            scheduler: Mutex::new(FlowScheduler::default()),
            shutdown_tx,
            handles: Mutex::new(Vec::new()),
            stats: Arc::new(TrafficStats::default()),
            bootstraps: Arc::new(BootstrapPool::new()),
        };
        assert!(manager
            .wait_for_healthy(Duration::from_millis(10))
            .await
            .is_err());
        for slot in &manager.slots {
            slot.healthy.store(true, Ordering::Release);
        }
        manager
            .wait_for_healthy(Duration::from_millis(10))
            .await
            .unwrap();
    }

    #[test]
    fn primary_is_retried_before_fallback() {
        let providers = [ProviderName::Vk, ProviderName::Jazz];
        assert_eq!(select_provider(&providers, 0), Some(ProviderName::Vk));
        assert_eq!(select_provider(&providers, 2), Some(ProviderName::Vk));
        assert_eq!(select_provider(&providers, 3), Some(ProviderName::Jazz));
        assert_eq!(select_provider(&providers, 4), Some(ProviderName::Vk));
    }
}
