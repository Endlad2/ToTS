use crate::{flow::Reliability, protocol::AckRange};
use std::{collections::BTreeMap, time::Duration};
use tokio::time::Instant;

const MSS: usize = 1200;
const INITIAL_CWND: usize = 10 * MSS;
const MIN_CWND: usize = 4 * MSS;
const MAX_CWND: usize = 4 * 1024 * 1024;
const MAX_RETRIES: u8 = 4;

#[derive(Clone, Debug)]
pub struct Retransmit {
    pub packet_id: u32,
    pub assoc_id: u16,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug)]
struct SentPacket {
    assoc_id: u16,
    payload: Vec<u8>,
    wire_bytes: usize,
    reliability: Reliability,
    last_sent: Instant,
    retries: u8,
}

/// QUIC-inspired, packet-number based recovery for ToTS packets.
/// It intentionally does not implement QUIC wire format; it provides congestion,
/// pacing, loss detection and bounded recovery over any DatagramPath.
#[derive(Debug)]
pub struct RecoveryController {
    in_flight: BTreeMap<u32, SentPacket>,
    bytes_in_flight: usize,
    cwnd: usize,
    ssthresh: usize,
    srtt: Option<Duration>,
    rttvar: Duration,
    largest_acked: Option<u32>,
    next_send: Instant,
}

impl Default for RecoveryController {
    fn default() -> Self {
        Self {
            in_flight: BTreeMap::new(),
            bytes_in_flight: 0,
            cwnd: INITIAL_CWND,
            ssthresh: 64 * MSS,
            srtt: None,
            rttvar: Duration::from_millis(50),
            largest_acked: None,
            next_send: Instant::now(),
        }
    }
}

impl RecoveryController {
    pub fn can_send(&self, bytes: usize) -> bool {
        self.bytes_in_flight.saturating_add(bytes) <= self.cwnd
    }

    pub fn reserve_pacing(&mut self, bytes: usize) -> Duration {
        let now = Instant::now();
        let srtt = self.srtt.unwrap_or(Duration::from_millis(80));
        let nanos = ((srtt.as_nanos() * bytes.max(1) as u128) / self.cwnd.max(MSS) as u128)
            .clamp(50_000, 10_000_000) as u64;
        let spacing = Duration::from_nanos(nanos);
        let send_at = self.next_send.max(now);
        self.next_send = send_at + spacing;
        send_at.saturating_duration_since(now)
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
        if let Some(old) = self.in_flight.insert(
            packet_id,
            SentPacket {
                assoc_id,
                payload,
                wire_bytes,
                reliability,
                last_sent: now,
                retries: 0,
            },
        ) {
            self.bytes_in_flight = self.bytes_in_flight.saturating_sub(old.wire_bytes);
        }
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
        for packet_id in acked.iter().copied() {
            if let Some(sent) = self.in_flight.remove(&packet_id) {
                self.bytes_in_flight = self.bytes_in_flight.saturating_sub(sent.wire_bytes);
                acked_bytes = acked_bytes.saturating_add(sent.wire_bytes);
                newest_rtt = Some(now.saturating_duration_since(sent.last_sent));
            }
        }
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
        acked.len()
    }

    pub fn observe_rtt(&mut self, sample: Duration) {
        self.update_rtt(sample);
    }

    pub fn poll_losses(&mut self) -> Vec<Retransmit> {
        let now = Instant::now();
        let loss_delay = self.loss_delay();
        let mut lost = Vec::new();
        for (&packet_id, sent) in &self.in_flight {
            let packet_threshold = self
                .largest_acked
                .is_some_and(|largest| largest.saturating_sub(packet_id) >= 3);
            let time_threshold = now.saturating_duration_since(sent.last_sent) >= loss_delay;
            if packet_threshold || time_threshold {
                lost.push(packet_id);
            }
        }
        if lost.is_empty() {
            return Vec::new();
        }
        self.ssthresh = (self.cwnd / 2).max(MIN_CWND);
        self.cwnd = self.ssthresh;
        let mut retry = Vec::new();
        for packet_id in lost {
            let remove = match self.in_flight.get_mut(&packet_id) {
                Some(sent) if sent.reliability == Reliability::Reliable && sent.retries < MAX_RETRIES => {
                    sent.retries += 1;
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
        self.next_send = Instant::now();

        // Unreliable packets belong to the old path and should not block the new cwnd.
        let unreliable: Vec<_> = self
            .in_flight
            .iter()
            .filter_map(|(&packet_id, sent)| {
                (sent.reliability == Reliability::Unreliable).then_some(packet_id)
            })
            .collect();
        for packet_id in unreliable {
            if let Some(sent) = self.in_flight.remove(&packet_id) {
                self.bytes_in_flight = self.bytes_in_flight.saturating_sub(sent.wire_bytes);
            }
        }

        let now = Instant::now();
        self.in_flight
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
            .collect()
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
    fn ranges_ack_multiple_packets() {
        let mut recovery = RecoveryController::default();
        for id in 1..=4 {
            recovery.on_sent(id, 1, vec![id as u8], 100, Reliability::Reliable);
        }
        assert_eq!(
            recovery.on_ack_ranges(&[AckRange { start: 2, end: 4 }]),
            3
        );
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
    fn migration_keeps_only_reliable_in_flight_data() {
        let mut recovery = RecoveryController::default();
        recovery.on_sent(1, 1, vec![1], 100, Reliability::Reliable);
        recovery.on_sent(2, 2, vec![2], 100, Reliability::Unreliable);
        let retry = recovery.on_path_migration();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].packet_id, 1);
        assert_eq!(recovery.bytes_in_flight(), 100);
    }

}
