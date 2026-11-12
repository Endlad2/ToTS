use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    net::Ipv4Addr,
    time::{Duration, Instant},
};

/// Scheduling estimate for one provider allocation. 250 kB/s is roughly 2 Mbit/s.
/// This is a balancing weight, not a hard bandwidth cap: when every worker is busy,
/// the scheduler still sends and lets the per-worker recovery controller pace the path.
pub const ALLOCATION_RATE_BYTES_PER_SEC: f64 = 247_000.0;
const ALLOCATION_TOKEN_CAPACITY: f64 = 12_000.0;
const MAX_TOKEN_DEFICIT: f64 = -6_000.0;
const MIN_ADAPTIVE_RATE: f64 = 150_000.0;
const RATE_DECREASE: f64 = 0.85;
const RATE_RECOVERY_STEP: f64 = 5_000.0;
const RATE_ADJUST_INTERVAL: Duration = Duration::from_millis(100);
const FLOWLET_BYTES: usize = 16 * 1024;
const FLOW_IDLE_TTL: Duration = Duration::from_secs(90);
const FLOW_TABLE_SOFT_LIMIT: usize = 4096;
const SMALL_UDP_MAX_LEN: usize = 300;
const SMALL_UDP_STICKY: Duration = Duration::from_secs(1);
const LOAD_WINDOW: Duration = Duration::from_millis(250);
const SMALL_UDP_WINDOW_LIMIT: usize = 46_312; // ~75% of one ~2 Mbit/s allocation for 250 ms.
const BULK_HOLD: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reliability {
    Reliable,
    Unreliable,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FlowKey {
    pub src: Ipv4Addr,
    pub dst: Ipv4Addr,
    pub protocol: u8,
    pub src_port: u16,
    pub dst_port: u16,
}

#[derive(Clone, Debug)]
struct FlowState {
    assoc_id: u16,
    worker_hint: usize,
    last_seen: Instant,
}

/// Legacy stable association helper retained for callers that need a single pinned worker.
#[derive(Debug, Default)]
pub struct FlowManager {
    flows: HashMap<FlowKey, FlowState>,
}

impl FlowManager {
    pub fn classify(packet: &[u8]) -> Reliability {
        let Some((protocol, _, _)) = ip_transport(packet) else {
            return Reliability::Reliable;
        };
        match protocol {
            // TCP already owns retransmission. Duplicating reliability below it can create HOL.
            6 => Reliability::Unreliable,
            // UDP is latency-sensitive or freshness-sensitive by default.
            17 => Reliability::Unreliable,
            // ICMP and other control protocols benefit from bounded recovery.
            _ => Reliability::Reliable,
        }
    }

    pub fn association(&mut self, packet: &[u8], workers: usize) -> (u16, usize) {
        self.expire();
        let key = flow_key(packet).unwrap_or_else(|| fallback_key(packet));
        if let Some(state) = self.flows.get_mut(&key) {
            state.last_seen = Instant::now();
            if workers != 0 {
                state.worker_hint %= workers;
            }
            return (state.assoc_id, state.worker_hint);
        }
        let hash = stable_hash(&key);
        let assoc_id = fold_hash(hash);
        let worker_hint = if workers == 0 {
            0
        } else {
            (hash as usize) % workers
        };
        self.flows.insert(
            key,
            FlowState {
                assoc_id,
                worker_hint,
                last_seen: Instant::now(),
            },
        );
        (assoc_id, worker_hint)
    }

    pub fn rebind(&mut self, packet: &[u8], worker: usize) {
        let key = flow_key(packet).unwrap_or_else(|| fallback_key(packet));
        if let Some(state) = self.flows.get_mut(&key) {
            state.worker_hint = worker;
            state.last_seen = Instant::now();
        }
    }

    fn expire(&mut self) {
        if self.flows.len() < FLOW_TABLE_SOFT_LIMIT {
            return;
        }
        self.flows
            .retain(|_, state| state.last_seen.elapsed() < Duration::from_secs(120));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScheduleDecision {
    pub assoc_id: u16,
    pub worker_id: u64,
    pub latency_sensitive: bool,
}

#[derive(Clone, Debug)]
struct StripeState {
    worker_id: u64,
    last_seen: Instant,
    flowlet_left: usize,
    sticky_until: Option<Instant>,
    window_start: Instant,
    window_bytes: usize,
    bulk_until: Option<Instant>,
}

impl StripeState {
    fn new(worker_id: u64, now: Instant, packet_len: usize, small_udp: bool) -> Self {
        Self {
            worker_id,
            last_seen: now,
            flowlet_left: FLOWLET_BYTES.saturating_sub(packet_len),
            sticky_until: small_udp.then_some(now + SMALL_UDP_STICKY),
            window_start: now,
            window_bytes: packet_len,
            bulk_until: None,
        }
    }

    fn observe(&mut self, now: Instant, packet_len: usize, small_udp: bool) -> bool {
        self.last_seen = now;
        if now.saturating_duration_since(self.window_start) >= LOAD_WINDOW {
            self.window_start = now;
            self.window_bytes = 0;
        }
        self.window_bytes = self.window_bytes.saturating_add(packet_len);
        if self.window_bytes > SMALL_UDP_WINDOW_LIMIT {
            self.bulk_until = Some(now + BULK_HOLD);
            self.sticky_until = None;
        }
        let forced_bulk = self.bulk_until.is_some_and(|until| now < until);
        if small_udp && !forced_bulk {
            self.sticky_until = Some(now + SMALL_UDP_STICKY);
        }
        self.sticky_until.is_some_and(|until| now < until) && !forced_bulk
    }
}

#[derive(Clone, Debug)]
struct WorkerBudget {
    tokens: f64,
    rate: f64,
    last_update: Instant,
    last_adjust: Instant,
    congested: bool,
}

impl WorkerBudget {
    fn new(now: Instant) -> Self {
        Self {
            tokens: ALLOCATION_TOKEN_CAPACITY,
            rate: ALLOCATION_RATE_BYTES_PER_SEC,
            last_update: now,
            last_adjust: now,
            congested: false,
        }
    }

    fn update(&mut self, now: Instant) {
        let elapsed = now
            .saturating_duration_since(self.last_update)
            .as_secs_f64();
        self.last_update = now;
        self.tokens = (self.tokens + elapsed * self.rate).min(ALLOCATION_TOKEN_CAPACITY);

        if now.saturating_duration_since(self.last_adjust) >= RATE_ADJUST_INTERVAL {
            if self.congested {
                self.rate = (self.rate * RATE_DECREASE).max(MIN_ADAPTIVE_RATE);
                self.congested = false;
            } else {
                self.rate = (self.rate + RATE_RECOVERY_STEP).min(ALLOCATION_RATE_BYTES_PER_SEC);
            }
            self.last_adjust = now;
        }
    }

    fn debit(&mut self, bytes: usize) {
        self.tokens = (self.tokens - bytes as f64).max(MAX_TOKEN_DEFICIT);
    }
}

/// Flowlet scheduler for multiple independent TURN/WebRTC workers.
///
/// * short/small UDP stays on one worker for low jitter;
/// * if that UDP exceeds roughly 75% of one estimated allocation, it becomes bulk;
/// * TCP and bulk UDP move only on 16 KiB flowlet boundaries;
/// * workers are selected by a deficit/token model whose nominal weight is about 2 Mbit/s each;
/// * congestion reduces a worker's scheduling weight temporarily without imposing a hard cap.
#[derive(Debug, Default)]
pub struct FlowScheduler {
    flows: HashMap<FlowKey, StripeState>,
    budgets: HashMap<u64, WorkerBudget>,
    cursor: usize,
    evict_counter: u32,
}

impl FlowScheduler {
    pub fn select(&mut self, packet: &[u8], workers: &[u64]) -> Option<ScheduleDecision> {
        self.select_with_preferred(packet, workers, workers)
    }

    /// Credit preference applies at flowlet boundaries; only dead workers break a flowlet.
    /// `preferred` must be a subset of `alive`; an empty preference uses every alive worker.
    pub fn select_with_preferred(
        &mut self,
        packet: &[u8],
        alive: &[u64],
        preferred: &[u64],
    ) -> Option<ScheduleDecision> {
        if alive.is_empty() {
            return None;
        }
        let now = Instant::now();
        let candidates = if preferred.is_empty() {
            alive
        } else {
            preferred
        };
        self.update_budgets(alive, now);
        self.evict_flows(now);

        let key = flow_key(packet).unwrap_or_else(|| fallback_key(packet));
        let assoc_id = fold_hash(stable_hash(&key));
        let small_udp = is_small_udp_key(&key, packet.len());

        if let Some(mut state) = self.flows.remove(&key) {
            let sticky = state.observe(now, packet.len(), small_udp);
            let assigned_alive = alive.contains(&state.worker_id);
            if assigned_alive && (sticky || state.flowlet_left >= packet.len()) {
                state.flowlet_left = state.flowlet_left.saturating_sub(packet.len());
                let worker_id = state.worker_id;
                let latency_sensitive = sticky;
                self.debit(worker_id, packet.len());
                self.flows.insert(key, state);
                return Some(ScheduleDecision {
                    assoc_id,
                    worker_id,
                    latency_sensitive,
                });
            }

            let avoid = (candidates.len() > 1 && assigned_alive).then_some(state.worker_id);
            let worker_id = self.choose_worker(candidates, avoid, now)?;
            state.worker_id = worker_id;
            state.flowlet_left = FLOWLET_BYTES.saturating_sub(packet.len());
            let latency_sensitive = sticky;
            self.debit(worker_id, packet.len());
            self.flows.insert(key, state);
            return Some(ScheduleDecision {
                assoc_id,
                worker_id,
                latency_sensitive,
            });
        }

        let worker_id = self.choose_worker(candidates, None, now)?;
        self.debit(worker_id, packet.len());
        self.flows.insert(
            key,
            StripeState::new(worker_id, now, packet.len(), small_udp),
        );
        Some(ScheduleDecision {
            assoc_id,
            worker_id,
            latency_sensitive: small_udp,
        })
    }

    pub fn mark_congested(&mut self, worker_id: u64) {
        let now = Instant::now();
        let budget = self
            .budgets
            .entry(worker_id)
            .or_insert_with(|| WorkerBudget::new(now));
        budget.congested = true;
    }

    pub fn estimated_capacity_mbps(worker_count: usize) -> f64 {
        worker_count as f64 * ALLOCATION_RATE_BYTES_PER_SEC * 8.0 / 1_000_000.0
    }

    pub fn is_latency_sensitive(packet: &[u8]) -> bool {
        flow_key(packet).is_some_and(|key| is_small_udp_key(&key, packet.len()))
    }

    fn update_budgets(&mut self, workers: &[u64], now: Instant) {
        for worker_id in workers {
            self.budgets
                .entry(*worker_id)
                .or_insert_with(|| WorkerBudget::new(now))
                .update(now);
        }
        if self.budgets.len() > workers.len().saturating_mul(4).max(64) {
            self.budgets
                .retain(|worker_id, _| workers.contains(worker_id));
        }
    }

    fn choose_worker(&mut self, workers: &[u64], avoid: Option<u64>, now: Instant) -> Option<u64> {
        if workers.len() == 1 {
            return workers.first().copied();
        }
        let start = self.cursor % workers.len();
        let mut best: Option<(u64, f64)> = None;
        for offset in 0..workers.len() {
            let worker_id = workers[(start + offset) % workers.len()];
            if Some(worker_id) == avoid {
                continue;
            }
            let budget = self
                .budgets
                .entry(worker_id)
                .or_insert_with(|| WorkerBudget::new(now));
            budget.update(now);
            if best.is_none_or(|(_, tokens)| budget.tokens > tokens) {
                best = Some((worker_id, budget.tokens));
            }
        }
        let chosen = best
            .or_else(|| workers.first().copied().map(|worker_id| (worker_id, 0.0)))?
            .0;
        if let Some(position) = workers.iter().position(|worker_id| *worker_id == chosen) {
            self.cursor = (position + 1) % workers.len();
        }
        Some(chosen)
    }

    fn debit(&mut self, worker_id: u64, bytes: usize) {
        let now = Instant::now();
        self.budgets
            .entry(worker_id)
            .or_insert_with(|| WorkerBudget::new(now))
            .debit(bytes);
    }

    fn evict_flows(&mut self, now: Instant) {
        self.evict_counter = self.evict_counter.wrapping_add(1);
        if self.flows.len() < FLOW_TABLE_SOFT_LIMIT && !self.evict_counter.is_multiple_of(1024) {
            return;
        }
        self.flows
            .retain(|_, state| now.saturating_duration_since(state.last_seen) < FLOW_IDLE_TTL);
        if self.flows.len() > FLOW_TABLE_SOFT_LIMIT * 2 {
            self.flows.clear();
        }
    }
}

pub fn association_id(packet: &[u8]) -> u16 {
    let key = flow_key(packet).unwrap_or_else(|| fallback_key(packet));
    fold_hash(stable_hash(&key))
}

fn flow_key(packet: &[u8]) -> Option<FlowKey> {
    if packet.len() < 20 || packet[0] >> 4 != 4 {
        return None;
    }
    let ihl = usize::from(packet[0] & 0x0f) * 4;
    if ihl < 20 || packet.len() < ihl {
        return None;
    }
    let protocol = packet[9];
    let src = Ipv4Addr::new(packet[12], packet[13], packet[14], packet[15]);
    let dst = Ipv4Addr::new(packet[16], packet[17], packet[18], packet[19]);
    let fragment_offset = u16::from_be_bytes([packet[6], packet[7]]) & 0x1fff;
    let (src_port, dst_port) =
        if fragment_offset == 0 && matches!(protocol, 6 | 17) && packet.len() >= ihl + 4 {
            (
                u16::from_be_bytes([packet[ihl], packet[ihl + 1]]),
                u16::from_be_bytes([packet[ihl + 2], packet[ihl + 3]]),
            )
        } else {
            (0, 0)
        };
    Some(FlowKey {
        src,
        dst,
        protocol,
        src_port,
        dst_port,
    })
}

fn is_small_udp_key(key: &FlowKey, packet_len: usize) -> bool {
    key.protocol == 17 && packet_len <= SMALL_UDP_MAX_LEN
}

fn ip_transport(packet: &[u8]) -> Option<(u8, u16, u16)> {
    let key = flow_key(packet)?;
    Some((key.protocol, key.src_port, key.dst_port))
}

fn fallback_key(packet: &[u8]) -> FlowKey {
    let mut acc = 0u32;
    for b in packet.iter().take(32) {
        acc = acc.rotate_left(5) ^ u32::from(*b);
        acc = acc.wrapping_mul(0x85eb_ca6b);
    }
    FlowKey {
        src: Ipv4Addr::UNSPECIFIED,
        dst: Ipv4Addr::UNSPECIFIED,
        protocol: 255,
        src_port: (acc >> 16) as u16,
        dst_port: acc as u16,
    }
}

fn stable_hash(key: &FlowKey) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

fn fold_hash(hash: u64) -> u16 {
    let mixed = hash ^ (hash >> 32) ^ (hash >> 16);
    mixed as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ipv4_packet(protocol: u8, src_port: u16, dst_port: u16, len: usize, marker: u8) -> Vec<u8> {
        let header = if protocol == 6 { 20 } else { 8 };
        let mut p = vec![0u8; len.max(20 + header)];
        p[0] = 0x45;
        p[9] = protocol;
        p[12..16].copy_from_slice(&[10, 0, 0, 2]);
        p[16..20].copy_from_slice(&[1, 1, 1, 1]);
        p[20..22].copy_from_slice(&src_port.to_be_bytes());
        p[22..24].copy_from_slice(&dst_port.to_be_bytes());
        let last = p.len() - 1;
        p[last] = marker;
        p
    }

    #[test]
    fn payload_changes_do_not_change_association() {
        let mut flows = FlowManager::default();
        let a = flows.association(&ipv4_packet(17, 1000, 53, 64, 1), 8);
        let b = flows.association(&ipv4_packet(17, 1000, 53, 64, 2), 8);
        assert_eq!(a, b);
    }

    #[test]
    fn small_udp_stays_on_one_worker() {
        let mut scheduler = FlowScheduler::default();
        let workers = [10, 20, 30, 40];
        let first = scheduler
            .select(&ipv4_packet(17, 40000, 3478, 120, 1), &workers)
            .unwrap();
        for marker in 2..20 {
            let next = scheduler
                .select(&ipv4_packet(17, 40000, 3478, 120, marker), &workers)
                .unwrap();
            assert_eq!(next.worker_id, first.worker_id);
            assert!(next.latency_sensitive);
        }
    }

    #[test]
    fn tcp_bulk_rotates_on_flowlet_boundaries() {
        let mut scheduler = FlowScheduler::default();
        let workers = [1, 2, 3, 4];
        let mut seen = std::collections::BTreeSet::new();
        for marker in 0..40u8 {
            let decision = scheduler
                .select(&ipv4_packet(6, 50000, 443, 1400, marker), &workers)
                .unwrap();
            seen.insert(decision.worker_id);
        }
        assert!(
            seen.len() >= 3,
            "bulk flow should use several workers: {seen:?}"
        );
    }

    #[test]
    fn credit_preference_does_not_interrupt_a_tcp_flowlet() {
        let mut scheduler = FlowScheduler::default();
        let packet = ipv4_packet(6, 50000, 443, 1400, 1);
        let first = scheduler
            .select_with_preferred(&packet, &[1, 2], &[1])
            .unwrap();
        assert_eq!(first.worker_id, 1);
        for _ in 1..11 {
            let decision = scheduler
                .select_with_preferred(&packet, &[1, 2], &[2])
                .unwrap();
            assert_eq!(
                decision.worker_id, 1,
                "credit shortage interrupted the existing flowlet"
            );
        }
        let next = scheduler
            .select_with_preferred(&packet, &[1, 2], &[2])
            .unwrap();
        assert_eq!(
            next.worker_id, 2,
            "the next flowlet ignored its preferred worker"
        );
    }

    #[test]
    fn credit_preference_keeps_small_udp_sticky() {
        let mut scheduler = FlowScheduler::default();
        let packet = ipv4_packet(17, 50000, 3478, 120, 1);
        let first = scheduler
            .select_with_preferred(&packet, &[1, 2], &[1])
            .unwrap();
        for _ in 0..20 {
            let decision = scheduler
                .select_with_preferred(&packet, &[1, 2], &[2])
                .unwrap();
            assert_eq!(decision.worker_id, first.worker_id);
            assert!(decision.latency_sensitive);
        }
    }

    #[test]
    fn empty_credit_preference_falls_back_to_alive_workers() {
        let mut scheduler = FlowScheduler::default();
        let packet = ipv4_packet(6, 50000, 443, 1400, 1);
        assert!(scheduler
            .select_with_preferred(&packet, &[1, 2], &[])
            .is_some());
        assert!(scheduler.select_with_preferred(&packet, &[], &[]).is_none());
    }

    #[test]
    fn high_rate_small_udp_is_promoted_to_bulk() {
        let mut scheduler = FlowScheduler::default();
        let workers = [1, 2, 3];
        let mut seen = std::collections::BTreeSet::new();
        for marker in 0..220u16 {
            let packet = ipv4_packet(17, 50000, 443, 280, marker as u8);
            let decision = scheduler.select(&packet, &workers).unwrap();
            seen.insert(decision.worker_id);
        }
        assert!(seen.len() > 1, "high-rate UDP should stop being pinned");
    }

    #[test]
    fn missing_worker_rebinds_flow() {
        let mut scheduler = FlowScheduler::default();
        let packet = ipv4_packet(17, 2222, 3333, 100, 1);
        let first = scheduler.select(&packet, &[7, 8]).unwrap();
        let surviving = if first.worker_id == 7 { 8 } else { 7 };
        let next = scheduler.select(&packet, &[surviving]).unwrap();
        assert_eq!(next.worker_id, surviving);
    }

    #[test]
    fn tcp_and_udp_default_to_unreliable_transport_recovery() {
        assert_eq!(
            FlowManager::classify(&ipv4_packet(17, 1000, 53, 64, 1)),
            Reliability::Unreliable
        );
        assert_eq!(
            FlowManager::classify(&ipv4_packet(6, 1000, 443, 64, 1)),
            Reliability::Unreliable
        );
    }
}
