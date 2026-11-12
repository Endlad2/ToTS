use crate::{flow::Reliability, protocol::AckRange};
use std::{collections::BTreeMap, time::Duration};
use tokio::time::Instant;

const MSS: usize = 1200;
const INITIAL_CWND: usize = 10 * MSS;
const MIN_CWND: usize = 4 * MSS;
const MAX_CWND: usize = 4 * 1024 * 1024;
const MAX_RETRIES: u8 = 4;
const BEST_EFFORT_SAMPLE_LIMIT: usize = 512;
const MIN_PTO: Duration = Duration::from_secs(1);
const MAX_PTO: Duration = Duration::from_secs(30);
const PACING_BURST_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug)]
pub struct Retransmit {
    pub packet_id: u32,
    pub assoc_id: u16,
    pub payload: Vec<u8>,
}

/// Cumulative IP-packet diagnostics; retransmits count scheduled retry attempts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveryStats {
    pub sent_packets: u64,
    /// Newly ACKed tracked packets; late_acked_packets is a separate count.
    pub acked_packets: u64,
    /// Packet threshold takes precedence when both loss thresholds hold.
    pub packet_threshold_losses: u64,
    pub time_threshold_losses: u64,
    pub retransmits: u64,
    /// Tail timer events, separate from ACK-evidenced congestion losses.
    pub pto_expirations: u64,
    pub pto_retransmits: u64,
    /// Old diagnostic samples displaced by the fixed best-effort metadata cap.
    pub best_effort_evictions: u64,
    /// Retired unreliable packets ACKed later; no congestion credit is restored.
    pub late_acked_packets: u64,
}

#[derive(Clone, Debug)]
struct SentPacket {
    assoc_id: u16,
    payload: Vec<u8>,
    wire_bytes: usize,
    last_sent: Instant,
    retries: u8,
}

#[derive(Clone, Debug)]
struct BestEffortSample {
    sent_at: Instant,
    retired: bool,
}

/// QUIC-inspired, packet-number based recovery for ToTS packets.
/// It intentionally does not implement QUIC wire format; it provides congestion,
/// pacing and bounded recovery for reliable control over any DatagramPath.
/// Best-effort IP datagrams use bounded ACK diagnostics, without a second delivery window.
#[derive(Debug)]
pub struct RecoveryController {
    in_flight: BTreeMap<u32, SentPacket>,
    best_effort: BTreeMap<u32, BestEffortSample>,
    stats: RecoveryStats,
    bytes_in_flight: usize,
    cwnd: usize,
    ssthresh: usize,
    srtt: Option<Duration>,
    rttvar: Duration,
    largest_acked: Option<u32>,
    recovery_started: Option<Instant>,
    pacing_credit: f64,
    pacing_updated: Instant,
}

impl Default for RecoveryController {
    fn default() -> Self {
        Self {
            in_flight: BTreeMap::new(),
            best_effort: BTreeMap::new(),
            stats: RecoveryStats::default(),
            bytes_in_flight: 0,
            cwnd: INITIAL_CWND,
            ssthresh: 64 * MSS,
            srtt: None,
            rttvar: Duration::from_millis(50),
            largest_acked: None,
            recovery_started: None,
            pacing_credit: PACING_BURST_BYTES as f64,
            pacing_updated: Instant::now(),
        }
    }
}

impl RecoveryController {
    pub fn stats(&self) -> RecoveryStats {
        self.stats
    }

    pub fn can_send(&self, bytes: usize) -> bool {
        self.can_send_for(bytes, Reliability::Reliable)
    }

    pub fn can_send_for(&self, bytes: usize, reliability: Reliability) -> bool {
        reliability == Reliability::Unreliable
            || self.bytes_in_flight.saturating_add(bytes) <= self.cwnd
    }

    pub fn best_effort_samples(&self) -> usize {
        self.best_effort.len()
    }

    pub fn reserve_pacing(&mut self, bytes: usize) -> Duration {
        self.reserve_pacing_for(bytes, Reliability::Reliable)
    }

    pub fn reserve_pacing_for(&mut self, bytes: usize, reliability: Reliability) -> Duration {
        if reliability == Reliability::Unreliable {
            Duration::ZERO
        } else {
            self.reserve_pacing_at(bytes, Instant::now())
        }
    }

    fn reserve_pacing_at(&mut self, bytes: usize, now: Instant) -> Duration {
        let srtt = self.srtt.unwrap_or(Duration::from_millis(80));
        let rate = self.cwnd.max(MSS) as f64 / srtt.as_secs_f64().max(0.000_001);
        let capacity = self.cwnd.min(PACING_BURST_BYTES) as f64;
        let elapsed = now
            .saturating_duration_since(self.pacing_updated)
            .as_secs_f64();
        // Credit permits one bounded burst; negative credit preserves all pacing debt.
        self.pacing_credit = (self.pacing_credit + elapsed * rate).min(capacity);
        self.pacing_updated = now;
        self.pacing_credit -= bytes.max(1) as f64;
        Duration::from_secs_f64((-self.pacing_credit / rate).max(0.0))
    }

    pub fn on_sent(
        &mut self,
        packet_id: u32,
        assoc_id: u16,
        payload: Vec<u8>,
        wire_bytes: usize,
        reliability: Reliability,
    ) {
        let now = Instant::now();
        self.stats.sent_packets = self.stats.sent_packets.saturating_add(1);
        if let Some(old) = self.in_flight.remove(&packet_id) {
            self.bytes_in_flight = self.bytes_in_flight.saturating_sub(old.wire_bytes);
        }
        self.best_effort.remove(&packet_id);
        if reliability == Reliability::Unreliable {
            // Inner TCP/QUIC owns delivery and congestion; retain no best-effort payloads.
            self.best_effort.insert(
                packet_id,
                BestEffortSample {
                    sent_at: now,
                    retired: false,
                },
            );
            if self.best_effort.len() > BEST_EFFORT_SAMPLE_LIMIT {
                self.best_effort.pop_first();
                self.stats.best_effort_evictions =
                    self.stats.best_effort_evictions.saturating_add(1);
            }
            return;
        }
        self.in_flight.insert(
            packet_id,
            SentPacket {
                assoc_id,
                payload,
                wire_bytes,
                last_sent: now,
                retries: 0,
            },
        );
        self.bytes_in_flight = self.bytes_in_flight.saturating_add(wire_bytes);
    }

    pub fn on_ack(&mut self, packet_id: u32) -> bool {
        self.on_ack_ranges(&[AckRange {
            start: packet_id,
            end: packet_id,
        }]) > 0
    }

    pub fn on_ack_ranges(&mut self, ranges: &[AckRange]) -> usize {
        if ranges.is_empty() {
            return 0;
        }
        let now = Instant::now();
        if let Some(largest) = ranges.iter().map(|range| range.end).max() {
            self.largest_acked = Some(self.largest_acked.map_or(largest, |old| old.max(largest)));
        }
        let mut acked = Vec::new();
        for packet_id in self.in_flight.keys().copied() {
            if ranges.iter().any(|range| range.contains(packet_id)) {
                acked.push(packet_id);
            }
        }
        let mut newest_rtt = None;
        let mut acked_bytes = 0usize;
        for packet_id in &acked {
            if let Some(sent) = self.in_flight.remove(packet_id) {
                self.bytes_in_flight = self.bytes_in_flight.saturating_sub(sent.wire_bytes);
                acked_bytes = acked_bytes.saturating_add(sent.wire_bytes);
                // Only the newest ACKed packet gives RTT; older ranges may repeat lost ACKs.
                // Packet IDs survive retransmission, making its ACK's RTT ambiguous.
                if Some(*packet_id) == self.largest_acked && sent.retries == 0 {
                    newest_rtt = Some(now.saturating_duration_since(sent.last_sent));
                }
            }
        }
        self.stats.acked_packets = self.stats.acked_packets.saturating_add(acked.len() as u64);
        let mut best_effort_acked = 0;
        for range in ranges.iter().filter(|range| range.start <= range.end) {
            let samples: Vec<_> = self
                .best_effort
                .range(range.start..=range.end)
                .map(|(&packet_id, _)| packet_id)
                .collect();
            for packet_id in samples {
                if let Some(sent) = self.best_effort.remove(&packet_id) {
                    if sent.retired {
                        self.stats.late_acked_packets =
                            self.stats.late_acked_packets.saturating_add(1);
                    } else {
                        best_effort_acked += 1;
                        if Some(packet_id) == self.largest_acked {
                            newest_rtt = Some(now.saturating_duration_since(sent.sent_at));
                        }
                    }
                }
            }
        }
        self.stats.acked_packets = self
            .stats
            .acked_packets
            .saturating_add(best_effort_acked as u64);
        if let Some(sample) = newest_rtt {
            self.update_rtt(sample);
        }
        if acked_bytes != 0 {
            if self.cwnd < self.ssthresh {
                self.cwnd = self.cwnd.saturating_add(acked_bytes.min(2 * MSS));
            } else {
                let inc = (MSS * acked_bytes / self.cwnd.max(1)).max(1);
                self.cwnd = self.cwnd.saturating_add(inc);
            }
            self.cwnd = self.cwnd.min(MAX_CWND);
        }
        acked.len() + best_effort_acked
    }

    pub fn observe_rtt(&mut self, sample: Duration) {
        self.update_rtt(sample);
    }

    pub fn poll_losses(&mut self) -> Vec<Retransmit> {
        let now = Instant::now();
        let loss_delay = self.loss_delay();
        let mut expired = Vec::new();
        for (&packet_id, sent) in &self.in_flight {
            // Packet IDs stay unchanged on retry, so an old ACK cannot prove the retry lost.
            let later_ack = sent.retries == 0
                && self
                    .largest_acked
                    .is_some_and(|largest| largest > packet_id);
            let packet_threshold = later_ack
                && self
                    .largest_acked
                    .is_some_and(|largest| largest - packet_id >= 3);
            let age = now.saturating_duration_since(sent.last_sent);
            let time_threshold = later_ack && age >= loss_delay;
            if packet_threshold || time_threshold {
                if packet_threshold {
                    self.stats.packet_threshold_losses =
                        self.stats.packet_threshold_losses.saturating_add(1);
                } else {
                    self.stats.time_threshold_losses =
                        self.stats.time_threshold_losses.saturating_add(1);
                }
                expired.push((packet_id, false));
            } else if age >= self.pto_delay(sent.retries) {
                // A tail timeout probes/retires bounded credit; it is not proof of congestion.
                self.stats.pto_expirations = self.stats.pto_expirations.saturating_add(1);
                expired.push((packet_id, true));
            }
        }
        let best_effort_pto = self.pto_delay(0);
        for (&packet_id, sent) in &mut self.best_effort {
            if sent.retired {
                continue;
            }
            let later_ack = self
                .largest_acked
                .is_some_and(|largest| largest > packet_id);
            let age = now.saturating_duration_since(sent.sent_at);
            if later_ack
                && self
                    .largest_acked
                    .is_some_and(|largest| largest - packet_id >= 3)
            {
                self.stats.packet_threshold_losses =
                    self.stats.packet_threshold_losses.saturating_add(1);
            } else if later_ack && age >= loss_delay {
                self.stats.time_threshold_losses =
                    self.stats.time_threshold_losses.saturating_add(1);
            } else if age >= best_effort_pto {
                self.stats.pto_expirations = self.stats.pto_expirations.saturating_add(1);
            } else {
                continue;
            }
            sent.retired = true;
        }
        if expired.is_empty() {
            return Vec::new();
        }
        let new_flight_lost = expired.iter().any(|(packet_id, pto)| {
            !pto && self.in_flight.get(packet_id).is_some_and(|sent| {
                self.recovery_started
                    .is_none_or(|started| sent.last_sent > started)
            })
        });
        if new_flight_lost {
            self.ssthresh = (self.cwnd / 2).max(MIN_CWND);
            self.cwnd = self.ssthresh;
            self.recovery_started = Some(now);
        }
        let mut retry = Vec::new();
        for (packet_id, pto) in expired {
            let remove = match self.in_flight.get_mut(&packet_id) {
                Some(sent) if sent.retries < MAX_RETRIES => {
                    sent.retries += 1;
                    self.stats.retransmits = self.stats.retransmits.saturating_add(1);
                    if pto {
                        self.stats.pto_retransmits = self.stats.pto_retransmits.saturating_add(1);
                    }
                    sent.last_sent = now;
                    retry.push(Retransmit {
                        packet_id,
                        assoc_id: sent.assoc_id,
                        payload: sent.payload.clone(),
                    });
                    false
                }
                Some(_) => true,
                None => false,
            };
            if remove {
                if let Some(sent) = self.in_flight.remove(&packet_id) {
                    self.bytes_in_flight = self.bytes_in_flight.saturating_sub(sent.wire_bytes);
                }
            }
        }
        retry
    }

    /// Reliable in-flight packets are eligible for immediate resend on a new path.
    /// Fresh encryption sequence numbers are generated by SessionEngine on resend.
    pub fn on_path_migration(&mut self) -> Vec<Retransmit> {
        // A new provider/path has different capacity and RTT. Start conservatively instead
        // of carrying an inflated old-path window into TURN/Jazz, but preserve reliable data.
        self.cwnd = INITIAL_CWND;
        self.ssthresh = 64 * MSS;
        self.srtt = None;
        self.rttvar = Duration::from_millis(50);
        self.largest_acked = None;
        // Late ACK timestamps from the old path cannot estimate the new path's RTT.
        self.best_effort.clear();
        self.recovery_started = None;
        self.pacing_credit = INITIAL_CWND.min(PACING_BURST_BYTES) as f64;
        self.pacing_updated = Instant::now();

        let now = Instant::now();
        let retries: Vec<_> = self
            .in_flight
            .iter_mut()
            .filter_map(|(&packet_id, sent)| {
                if sent.retries < MAX_RETRIES {
                    sent.retries += 1;
                    sent.last_sent = now;
                    Some(Retransmit {
                        packet_id,
                        assoc_id: sent.assoc_id,
                        payload: sent.payload.clone(),
                    })
                } else {
                    None
                }
            })
            .collect();
        self.stats.retransmits = self.stats.retransmits.saturating_add(retries.len() as u64);
        retries
    }

    pub fn srtt(&self) -> Option<Duration> {
        self.srtt
    }

    pub fn cwnd(&self) -> usize {
        self.cwnd
    }

    pub fn bytes_in_flight(&self) -> usize {
        self.bytes_in_flight
    }

    pub fn loss_delay(&self) -> Duration {
        let srtt = self.srtt.unwrap_or(Duration::from_millis(100));
        let variance = self.rttvar.saturating_mul(4);
        (srtt + variance).max(Duration::from_millis(80))
    }

    fn pto_delay(&self, retries: u8) -> Duration {
        self.loss_delay()
            .saturating_add(Duration::from_millis(25))
            .max(MIN_PTO)
            .saturating_mul(1 << retries.min(MAX_RETRIES))
            .min(MAX_PTO)
    }

    fn update_rtt(&mut self, sample: Duration) {
        match self.srtt {
            None => {
                self.srtt = Some(sample);
                self.rttvar = sample / 2;
            }
            Some(old) => {
                let diff = old.abs_diff(sample);
                self.rttvar = self.rttvar.mul_f64(0.75) + diff.mul_f64(0.25);
                self.srtt = Some(old.mul_f64(0.875) + sample.mul_f64(0.125));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn best_effort_ack_blackout_cannot_consume_reliable_credit_or_stall_forwarding() {
        let mut recovery = RecoveryController::default();
        recovery.on_sent(1, 1, vec![1; 4000], 4000, Reliability::Unreliable);
        recovery.on_sent(2, 1, vec![2], 100, Reliability::Reliable);
        assert_eq!(recovery.bytes_in_flight(), 100);
        assert!(recovery.can_send_for(usize::MAX, Reliability::Unreliable));
        assert!(recovery.can_send_for(INITIAL_CWND - 100, Reliability::Reliable));
        assert!(!recovery.can_send_for(INITIAL_CWND, Reliability::Reliable));
        assert_eq!(recovery.best_effort_samples(), 1);
    }

    #[test]
    fn best_effort_pacing_does_not_spend_or_reset_reliable_debt() {
        let mut recovery = RecoveryController::default();
        recovery.reserve_pacing(INITIAL_CWND);
        assert!(recovery.reserve_pacing(MSS) > Duration::ZERO);
        let credit = recovery.pacing_credit;
        assert_eq!(
            recovery.reserve_pacing_for(64 * 1024, Reliability::Unreliable),
            Duration::ZERO
        );
        assert_eq!(recovery.pacing_credit, credit);
    }

    #[test]
    fn best_effort_ack_and_proven_loss_neither_grow_nor_cut_reliable_window() {
        let mut recovery = RecoveryController::default();
        recovery.on_sent(1, 1, vec![1], 100, Reliability::Unreliable);
        recovery.on_sent(4, 1, vec![4], 100, Reliability::Unreliable);
        let window = recovery.cwnd();
        assert!(recovery.on_ack(4));
        assert_eq!(recovery.cwnd(), window);
        assert!(recovery.poll_losses().is_empty());
        assert_eq!(recovery.cwnd(), window);
        assert_eq!(recovery.bytes_in_flight(), 0);
        assert_eq!(recovery.stats().packet_threshold_losses, 1);
        assert_eq!(recovery.stats().retransmits, 0);
    }

    #[test]
    fn best_effort_diagnostic_samples_are_strictly_bounded() {
        let mut recovery = RecoveryController::default();
        for id in 1..=600 {
            recovery.on_sent(id, 1, vec![1; 1000], 1000, Reliability::Unreliable);
        }
        assert_eq!(recovery.best_effort_samples(), 512);
        assert_eq!(recovery.stats().best_effort_evictions, 88);
        assert_eq!(recovery.bytes_in_flight(), 0);
        assert!(recovery.in_flight.is_empty());
        assert!(recovery.can_send_for(INITIAL_CWND, Reliability::Reliable));
        recovery.on_path_migration();
        assert_eq!(recovery.best_effort_samples(), 0);
    }

    #[test]
    fn pacing_initial_burst_is_limited_by_window_and_sixteen_kib() {
        let mut recovery = RecoveryController {
            srtt: Some(Duration::from_millis(200)),
            ..Default::default()
        };
        let now = Instant::now();
        for _ in 0..INITIAL_CWND / MSS {
            assert_eq!(recovery.reserve_pacing_at(MSS, now), Duration::ZERO);
        }
        assert!(recovery.reserve_pacing_at(MSS, now) > Duration::ZERO);
        let mut recovery = RecoveryController {
            cwnd: 64 * 1024,
            srtt: Some(Duration::from_millis(200)),
            ..Default::default()
        };
        for _ in 0..13 {
            assert_eq!(recovery.reserve_pacing_at(MSS, now), Duration::ZERO);
        }
        assert_eq!(recovery.reserve_pacing_at(784, now), Duration::ZERO);
        assert!(recovery.reserve_pacing_at(MSS, now) > Duration::ZERO);
    }

    #[test]
    fn pacing_busy_worker_preserves_the_window_over_rtt_byte_rate() {
        let mut recovery = RecoveryController {
            cwnd: 32 * 1024,
            srtt: Some(Duration::from_millis(200)),
            ..Default::default()
        };
        let now = Instant::now();
        for _ in 0..16 {
            assert_eq!(recovery.reserve_pacing_at(1024, now), Duration::ZERO);
        }
        for packet in 1..=32 {
            let delay = recovery.reserve_pacing_at(1024, now);
            assert_eq!(delay, Duration::from_micros(6250 * packet));
        }
        assert_eq!(
            recovery.reserve_pacing_at(1024, now + Duration::from_millis(200)),
            Duration::from_micros(6250)
        );
    }

    #[test]
    fn pacing_idle_refill_is_capped_and_pays_existing_debt() {
        let mut recovery = RecoveryController {
            cwnd: 32 * 1024,
            srtt: Some(Duration::from_millis(200)),
            ..Default::default()
        };
        let now = Instant::now();
        recovery.reserve_pacing_at(16 * 1024, now);
        assert_eq!(
            recovery.reserve_pacing_at(16 * 1024, now),
            Duration::from_millis(100)
        );
        let later = now + Duration::from_secs(1);
        assert_eq!(recovery.reserve_pacing_at(16 * 1024, later), Duration::ZERO);
        assert_eq!(
            recovery.reserve_pacing_at(16 * 1024, later),
            Duration::from_millis(100)
        );
    }

    #[test]
    fn pacing_migration_discards_debt_and_refills_only_the_initial_window() {
        let mut recovery = RecoveryController {
            cwnd: 32 * 1024,
            srtt: Some(Duration::from_millis(200)),
            ..Default::default()
        };
        let now = Instant::now();
        recovery.reserve_pacing_at(32 * 1024, now);
        recovery.on_path_migration();
        let now = Instant::now();
        assert_eq!(
            recovery.reserve_pacing_at(INITIAL_CWND, now),
            Duration::ZERO
        );
        assert_eq!(
            recovery.reserve_pacing_at(MSS, now),
            Duration::from_millis(8)
        );
    }

    #[test]
    fn pacing_window_decrease_caps_credit_and_rate_decrease_keeps_debt() {
        let mut recovery = RecoveryController {
            cwnd: 64 * 1024,
            srtt: Some(Duration::from_millis(200)),
            ..Default::default()
        };
        let now = Instant::now();
        recovery.reserve_pacing_at(8 * 1024, now);
        recovery.cwnd = MIN_CWND;
        assert_eq!(recovery.reserve_pacing_at(MIN_CWND, now), Duration::ZERO);
        assert_eq!(
            recovery.reserve_pacing_at(MSS, now),
            Duration::from_millis(50)
        );
        recovery.srtt = Some(Duration::from_millis(400));
        assert_eq!(
            recovery.reserve_pacing_at(MSS, now),
            Duration::from_millis(200)
        );
    }

    #[test]
    fn pacing_zero_rtt_keeps_both_credit_and_delay_finite() {
        let mut recovery = RecoveryController {
            srtt: Some(Duration::ZERO),
            ..Default::default()
        };
        let now = Instant::now();
        assert_eq!(
            recovery.reserve_pacing_at(INITIAL_CWND, now),
            Duration::ZERO
        );
        assert!(recovery.reserve_pacing_at(MSS, now) > Duration::ZERO);
    }

    #[test]
    fn delayed_tail_ack_survives_the_rtt_timer_without_a_window_cut() {
        let mut recovery = RecoveryController::default();
        recovery.observe_rtt(Duration::from_millis(100));
        recovery.on_sent(1, 1, vec![1], 100, Reliability::Reliable);
        recovery.in_flight.get_mut(&1).unwrap().last_sent =
            Instant::now() - Duration::from_millis(400);
        let window = recovery.cwnd();
        assert!(recovery.poll_losses().is_empty());
        assert_eq!(recovery.bytes_in_flight(), 100);
        assert_eq!(recovery.cwnd(), window);
        assert_eq!(recovery.stats().time_threshold_losses, 0);
        assert!(recovery.on_ack(1));
        assert!(recovery.srtt().unwrap() > Duration::from_millis(100));
    }

    #[test]
    fn an_aged_gap_before_a_later_ack_is_still_a_congestion_loss() {
        let mut recovery = RecoveryController::default();
        recovery.observe_rtt(Duration::from_millis(100));
        recovery.on_sent(1, 1, vec![1], 100, Reliability::Reliable);
        recovery.on_sent(2, 1, vec![2], 100, Reliability::Reliable);
        recovery.on_ack(2);
        recovery.in_flight.get_mut(&1).unwrap().last_sent =
            Instant::now() - Duration::from_millis(400);
        let window = recovery.cwnd();
        assert_eq!(recovery.poll_losses().len(), 1);
        assert_eq!(recovery.bytes_in_flight(), 100);
        assert!(recovery.cwnd() < window);
        assert_eq!(recovery.stats().time_threshold_losses, 1);
    }

    #[test]
    fn ack_blackout_retries_and_credit_cleanup_are_bounded_without_false_loss() {
        let mut recovery = RecoveryController::default();
        recovery.observe_rtt(Duration::from_millis(100));
        recovery.on_sent(1, 1, vec![1], 100, Reliability::Unreliable);
        recovery.on_sent(2, 1, vec![2], 100, Reliability::Reliable);
        let window = recovery.cwnd();
        for sent in recovery.in_flight.values_mut() {
            sent.last_sent = Instant::now() - Duration::from_millis(400);
        }
        for sent in recovery.best_effort.values_mut() {
            sent.sent_at = Instant::now() - Duration::from_millis(400);
        }
        assert!(recovery.poll_losses().is_empty());
        assert_eq!(recovery.bytes_in_flight(), 100);
        for attempt in 0..=MAX_RETRIES {
            for sent in recovery.in_flight.values_mut() {
                sent.last_sent = Instant::now() - Duration::from_secs(31);
            }
            for sent in recovery.best_effort.values_mut() {
                sent.sent_at = Instant::now() - Duration::from_secs(31);
            }
            let retries = recovery.poll_losses();
            assert_eq!(retries.len(), usize::from(attempt < MAX_RETRIES));
            assert_eq!(recovery.cwnd(), window);
        }
        assert_eq!(recovery.bytes_in_flight(), 0);
        assert!(recovery.can_send(INITIAL_CWND));
        assert_eq!(recovery.stats().retransmits, u64::from(MAX_RETRIES));
        assert_eq!(recovery.stats().packet_threshold_losses, 0);
        assert_eq!(recovery.stats().time_threshold_losses, 0);
        assert_eq!(recovery.stats().pto_expirations, u64::from(MAX_RETRIES) + 2);
        assert_eq!(recovery.stats().pto_retransmits, u64::from(MAX_RETRIES));
    }

    #[test]
    fn a_late_ack_after_tail_credit_expiry_only_updates_diagnostics() {
        let mut recovery = RecoveryController::default();
        recovery.observe_rtt(Duration::from_millis(100));
        recovery.on_sent(9, 1, vec![9], 100, Reliability::Unreliable);
        recovery.best_effort.get_mut(&9).unwrap().sent_at = Instant::now() - Duration::from_secs(2);
        let window = recovery.cwnd();
        let rtt = recovery.srtt();
        recovery.poll_losses();
        assert_eq!(recovery.bytes_in_flight(), 0);
        assert!(!recovery.on_ack(9));
        assert_eq!(recovery.stats().late_acked_packets, 1);
        assert_eq!(recovery.srtt(), rtt);
        assert_eq!(recovery.cwnd(), window);
    }

    #[test]
    fn cumulative_ack_after_idle_only_samples_the_newest_packet_rtt() {
        let mut recovery = RecoveryController::default();
        recovery.observe_rtt(Duration::from_millis(100));
        recovery.on_sent(1, 1, vec![1], 100, Reliability::Unreliable);
        // Packet 1 arrived, but its first ACK was lost before a long idle period.
        recovery.best_effort.get_mut(&1).unwrap().sent_at =
            Instant::now() - Duration::from_secs(60);
        recovery.poll_losses();
        recovery.on_sent(2, 1, vec![2], 100, Reliability::Unreliable);
        recovery.best_effort.get_mut(&2).unwrap().sent_at =
            Instant::now() - Duration::from_millis(100);

        assert_eq!(recovery.on_ack_ranges(&[AckRange { start: 1, end: 2 }]), 1);
        assert_eq!(recovery.stats().late_acked_packets, 1);
        assert!(recovery.srtt().unwrap() < Duration::from_millis(200));
        assert!(recovery.loss_delay() < Duration::from_secs(1));
    }

    #[test]
    fn an_older_ack_does_not_replace_a_newer_rtt_measurement() {
        let mut recovery = RecoveryController::default();
        recovery.observe_rtt(Duration::from_millis(100));
        recovery.on_sent(1, 1, vec![1], 100, Reliability::Unreliable);
        recovery.on_sent(2, 1, vec![2], 100, Reliability::Reliable);
        recovery.best_effort.get_mut(&1).unwrap().sent_at =
            Instant::now() - Duration::from_millis(900);
        recovery.in_flight.get_mut(&2).unwrap().last_sent =
            Instant::now() - Duration::from_millis(100);
        assert!(recovery.on_ack(2));
        let rtt = recovery.srtt();

        assert!(recovery.on_ack(1));
        assert_eq!(recovery.srtt(), rtt);
    }

    #[test]
    fn retransmitted_packets_do_not_supply_ambiguous_rtt_samples() {
        let mut recovery = RecoveryController::default();
        recovery.on_sent(1, 1, vec![1], 100, Reliability::Reliable);
        recovery.on_sent(4, 1, vec![4], 100, Reliability::Unreliable);
        recovery.on_ack(4);
        assert_eq!(recovery.poll_losses().len(), 1);
        recovery.observe_rtt(Duration::from_millis(200));
        let rtt = recovery.srtt();
        assert!(recovery.on_ack(1));
        assert_eq!(recovery.srtt(), rtt);
    }

    #[test]
    fn diagnostics_count_new_acks_and_distinct_loss_triggers() {
        let mut recovery = RecoveryController::default();
        recovery.on_sent(1, 1, vec![1], 100, Reliability::Reliable);
        recovery.on_sent(3, 1, vec![3], 100, Reliability::Unreliable);
        recovery.on_sent(4, 1, vec![4], 100, Reliability::Unreliable);
        recovery.on_ack(4);
        assert_eq!(recovery.poll_losses().len(), 1);
        assert!(recovery.poll_losses().is_empty());
        assert_eq!(
            recovery.stats(),
            RecoveryStats {
                sent_packets: 3,
                acked_packets: 1,
                packet_threshold_losses: 1,
                time_threshold_losses: 0,
                retransmits: 1,
                late_acked_packets: 0,
                ..Default::default()
            }
        );
        recovery.best_effort.get_mut(&3).unwrap().sent_at = Instant::now() - recovery.loss_delay();
        assert!(recovery.poll_losses().is_empty());
        recovery.on_ack(1);
        recovery.on_ack(1);
        assert_eq!(
            recovery.stats(),
            RecoveryStats {
                sent_packets: 3,
                acked_packets: 2,
                packet_threshold_losses: 1,
                time_threshold_losses: 1,
                retransmits: 1,
                late_acked_packets: 0,
                ..Default::default()
            }
        );
    }

    #[test]
    fn diagnostics_count_a_delayed_ack_once_without_restoring_credit() {
        let mut recovery = RecoveryController::default();
        recovery.on_sent(9, 1, vec![9], 100, Reliability::Unreliable);
        recovery.best_effort.get_mut(&9).unwrap().sent_at = Instant::now() - recovery.pto_delay(0);
        assert!(recovery.poll_losses().is_empty());
        let window = recovery.cwnd();
        assert!(!recovery.on_ack(9));
        assert!(!recovery.on_ack(9));
        assert_eq!(recovery.stats().late_acked_packets, 1);
        assert_eq!(recovery.stats().acked_packets, 0);
        assert_eq!(recovery.stats().time_threshold_losses, 0);
        assert_eq!(recovery.stats().pto_expirations, 1);
        assert_eq!(recovery.cwnd(), window);
        assert_eq!(recovery.bytes_in_flight(), 0);
    }

    #[test]
    fn diagnostics_keep_a_bounded_lost_packet_tail_and_deduplicate_ranges() {
        let mut recovery = RecoveryController::default();
        for id in 1..=600 {
            recovery.on_sent(id, 1, vec![1], 100, Reliability::Unreliable);
        }
        let expired = Instant::now() - recovery.pto_delay(0);
        for sent in recovery.best_effort.values_mut() {
            sent.sent_at = expired;
        }
        recovery.poll_losses();
        recovery.on_ack_ranges(&[AckRange { start: 1, end: 88 }]);
        assert_eq!(recovery.stats().late_acked_packets, 0);
        recovery.on_ack_ranges(&[
            AckRange {
                start: 89,
                end: 600,
            },
            AckRange {
                start: 100,
                end: 600,
            },
        ]);
        recovery.on_ack_ranges(&[AckRange {
            start: 89,
            end: 600,
        }]);
        assert_eq!(recovery.stats().late_acked_packets, 512);
    }

    #[test]
    fn ranges_ack_multiple_packets() {
        let mut recovery = RecoveryController::default();
        for id in 1..=4 {
            recovery.on_sent(id, 1, vec![id as u8], 100, Reliability::Reliable);
        }
        assert_eq!(recovery.on_ack_ranges(&[AckRange { start: 2, end: 4 }]), 3);
        assert!(recovery.bytes_in_flight() > 0);
    }
    #[test]
    fn packet_threshold_marks_older_packet_lost() {
        let mut recovery = RecoveryController::default();
        recovery.on_sent(1, 7, vec![1], 100, Reliability::Reliable);
        recovery.on_sent(4, 7, vec![4], 100, Reliability::Reliable);
        recovery.on_ack(4);
        let lost = recovery.poll_losses();
        assert_eq!(lost.len(), 1);
        assert_eq!(lost[0].packet_id, 1);
    }

    #[test]
    fn repeated_loss_polls_do_not_reuse_the_same_ack_to_exhaust_retries() {
        let mut recovery = RecoveryController::default();
        recovery.on_sent(1, 7, vec![1], 100, Reliability::Reliable);
        recovery.on_sent(4, 7, vec![4], 100, Reliability::Reliable);
        recovery.on_ack(4);
        assert_eq!(recovery.poll_losses().len(), 1);
        let window_after_loss = recovery.cwnd();
        for _ in 0..10 {
            assert!(recovery.poll_losses().is_empty());
        }
        assert_eq!(recovery.bytes_in_flight(), 100);
        assert_eq!(recovery.cwnd(), window_after_loss);
    }

    #[test]
    fn packets_from_one_flight_reduce_the_window_once() {
        let mut recovery = RecoveryController::default();
        for id in [1, 2, 4, 5] {
            recovery.on_sent(id, 7, vec![id as u8], 100, Reliability::Reliable);
        }
        recovery.on_ack(4);
        assert_eq!(recovery.poll_losses()[0].packet_id, 1);
        recovery.on_ack(5);
        let window_before_next_poll = recovery.cwnd();
        let lost = recovery.poll_losses();
        assert_eq!(lost.len(), 1);
        assert_eq!(lost[0].packet_id, 2);
        assert_eq!(recovery.cwnd(), window_before_next_poll);
    }

    #[test]
    fn timed_retries_remain_bounded_after_fast_retransmission() {
        let mut recovery = RecoveryController::default();
        recovery.on_sent(1, 7, vec![1], 100, Reliability::Reliable);
        recovery.on_sent(4, 7, vec![4], 100, Reliability::Reliable);
        recovery.on_ack(4);
        assert_eq!(recovery.poll_losses().len(), 1);
        for _ in 1..MAX_RETRIES {
            let delay = recovery.pto_delay(recovery.in_flight[&1].retries);
            recovery.in_flight.get_mut(&1).unwrap().last_sent = Instant::now() - delay;
            assert_eq!(recovery.poll_losses().len(), 1);
        }
        let delay = recovery.pto_delay(recovery.in_flight[&1].retries);
        recovery.in_flight.get_mut(&1).unwrap().last_sent = Instant::now() - delay;
        assert!(recovery.poll_losses().is_empty());
        assert_eq!(recovery.bytes_in_flight(), 0);
    }

    #[test]
    fn migration_keeps_only_reliable_in_flight_data() {
        let mut recovery = RecoveryController::default();
        recovery.on_sent(1, 1, vec![1], 100, Reliability::Reliable);
        recovery.on_sent(2, 2, vec![2], 100, Reliability::Unreliable);
        let retry = recovery.on_path_migration();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].packet_id, 1);
        assert_eq!(recovery.bytes_in_flight(), 100);
    }

    #[test]
    fn pto_backoff_is_conservative_and_capped() {
        let mut recovery = RecoveryController::default();
        recovery.observe_rtt(Duration::from_millis(100));
        for retries in 0..=MAX_RETRIES {
            assert_eq!(
                recovery.pto_delay(retries),
                Duration::from_secs(1 << retries)
            );
        }
        recovery.observe_rtt(Duration::from_secs(10));
        assert_eq!(recovery.pto_delay(MAX_RETRIES), MAX_PTO);
    }

    #[test]
    fn migration_does_not_import_a_late_rtt_sample_from_the_old_path() {
        let mut recovery = RecoveryController::default();
        recovery.on_sent(1, 1, vec![1], 100, Reliability::Unreliable);
        recovery.best_effort.get_mut(&1).unwrap().sent_at = Instant::now() - Duration::from_secs(2);
        recovery.poll_losses();
        recovery.on_path_migration();
        assert!(!recovery.on_ack(1));
        assert_eq!(recovery.srtt(), None);
        assert_eq!(recovery.stats().late_acked_packets, 0);
    }
}
