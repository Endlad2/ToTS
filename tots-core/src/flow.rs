use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    net::Ipv4Addr,
    time::{Duration, Instant},
};

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

/// Stable 5-tuple associations avoid reordering a flow across workers.
/// Entries are refreshed lazily and old associations are evicted.
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
            // UDP is latency-sensitive by default.
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
        let assoc_id = ((hash >> 16) ^ hash) as u16;
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

    /// Pins an existing association to the worker that actually accepted the packet.
    /// This keeps a long-lived flow on its failover worker instead of bouncing back
    /// immediately when an older worker becomes healthy again.
    pub fn rebind(&mut self, packet: &[u8], worker: usize) {
        let key = flow_key(packet).unwrap_or_else(|| fallback_key(packet));
        if let Some(state) = self.flows.get_mut(&key) {
            state.worker_hint = worker;
            state.last_seen = Instant::now();
        }
    }

    fn expire(&mut self) {
        if self.flows.len() < 4096 {
            return;
        }
        let cutoff = Duration::from_secs(120);
        self.flows.retain(|_, state| state.last_seen.elapsed() < cutoff);
    }
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
    let (src_port, dst_port) = if matches!(protocol, 6 | 17) && packet.len() >= ihl + 4 {
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
    // DefaultHasher is process-local but deterministic for an explicitly constructed instance.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn udp(src: u16, dst: u16, marker: u8) -> Vec<u8> {
        let mut p = vec![0u8; 28];
        p[0] = 0x45;
        p[9] = 17;
        p[12..16].copy_from_slice(&[10, 0, 0, 2]);
        p[16..20].copy_from_slice(&[1, 1, 1, 1]);
        p[20..22].copy_from_slice(&src.to_be_bytes());
        p[22..24].copy_from_slice(&dst.to_be_bytes());
        p[27] = marker;
        p
    }

    #[test]
    fn payload_changes_do_not_change_association() {
        let mut flows = FlowManager::default();
        let a = flows.association(&udp(1000, 53, 1), 8);
        let b = flows.association(&udp(1000, 53, 2), 8);
        assert_eq!(a, b);
    }

    #[test]
    fn failover_rebind_keeps_flow_on_new_worker() {
        let packet = udp(1000, 53, 1);
        let mut flows = FlowManager::default();
        let (_, original) = flows.association(&packet, 8);
        let replacement = (original + 1) % 8;
        flows.rebind(&packet, replacement);
        assert_eq!(flows.association(&packet, 8).1, replacement);
    }

    #[test]
    fn tcp_and_udp_default_to_unreliable_transport_recovery() {
        assert_eq!(FlowManager::classify(&udp(1000, 53, 1)), Reliability::Unreliable);
    }
}
