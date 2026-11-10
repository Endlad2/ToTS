use crate::{
    config::ConnectionConfig,
    crypto::PacketCrypto,
    fragment::fragment_ip,
    protocol::{open_datagram, seal_datagram, AckRange, EnvelopeMeta, Frame, ReplayWindow, MAX_ACK_RANGES},
    reassembly::Reassembler,
    Result,
};
use parking_lot::Mutex;
use rand::{rngs::OsRng, RngCore};
use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
};

#[derive(Debug)]
pub enum DecodedPacket {
    Ip {
        assoc_id: u16,
        packet_id: u32,
        bytes: Vec<u8>,
    },
    Duplicate {
        packet_id: u32,
    },
    Ack {
        packet_id: u32,
    },
    AckRanges {
        ranges: Vec<AckRange>,
    },
    Heartbeat {
        counter: u64,
    },
    Probe {
        token: u64,
        sent_micros: u64,
    },
    ProbeAck {
        token: u64,
        sent_micros: u64,
    },
    Dissociate {
        assoc_id: u16,
    },
}

#[derive(Debug, Default)]
struct AckTracker {
    received: BTreeSet<u32>,
}

impl AckTracker {
    fn record(&mut self, packet_id: u32) -> Vec<AckRange> {
        self.received.insert(packet_id);
        // Keep a bounded tail. Packet IDs are monotonically increasing within a session.
        while self.received.len() > 512 {
            let Some(first) = self.received.first().copied() else {
                break;
            };
            self.received.remove(&first);
        }
        let mut ranges = Vec::new();
        let mut end = None;
        let mut start = 0u32;
        for packet in self.received.iter().rev().copied() {
            match end {
                None => {
                    start = packet;
                    end = Some(packet);
                }
                Some(current_end) if packet.saturating_add(1) == start => {
                    start = packet;
                    end = Some(current_end);
                }
                Some(current_end) => {
                    ranges.push(AckRange {
                        start,
                        end: current_end,
                    });
                    if ranges.len() == MAX_ACK_RANGES {
                        return ranges;
                    }
                    start = packet;
                    end = Some(packet);
                }
            }
        }
        if let Some(current_end) = end {
            ranges.push(AckRange {
                start,
                end: current_end,
            });
        }
        ranges
    }
}

pub struct SessionEngine {
    config: ConnectionConfig,
    crypto: PacketCrypto,
    session_id: u64,
    next_sequence: AtomicU64,
    next_packet: AtomicU32,
    replay: Mutex<ReplayWindow>,
    reassembly: Mutex<Reassembler>,
    delivered: Mutex<BTreeSet<u32>>,
    ack_tracker: Mutex<AckTracker>,
}

impl SessionEngine {
    pub fn new(config: ConnectionConfig) -> Result<Self> {
        config.validate()?;
        let crypto = PacketCrypto::derive(&config.password, config.device_id)?;
        let mut r = OsRng;
        let session_id = r.next_u64();
        Ok(Self {
            config,
            crypto,
            session_id,
            next_sequence: AtomicU64::new(1),
            next_packet: AtomicU32::new(1),
            replay: Mutex::new(ReplayWindow::default()),
            reassembly: Mutex::new(Reassembler::default()),
            delivered: Mutex::new(BTreeSet::new()),
            ack_tracker: Mutex::new(AckTracker::default()),
        })
    }

    pub fn with_session(config: ConnectionConfig, session_id: u64) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            crypto: PacketCrypto::derive(&config.password, config.device_id)?,
            config,
            session_id,
            next_sequence: AtomicU64::new(1),
            next_packet: AtomicU32::new(1),
            replay: Mutex::new(ReplayWindow::default()),
            reassembly: Mutex::new(Reassembler::default()),
            delivered: Mutex::new(BTreeSet::new()),
            ack_tracker: Mutex::new(AckTracker::default()),
        })
    }

    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    fn wrap(&self, frame: &Frame) -> Result<Vec<u8>> {
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        seal_datagram(
            &self.crypto,
            EnvelopeMeta {
                device_id: self.config.device_id,
                session_id: self.session_id,
                sequence,
            },
            frame,
        )
    }

    pub fn encapsulate_ip(&self, assoc_id: u16, packet: &[u8]) -> Result<(u32, Vec<Vec<u8>>)> {
        let id = self.next_packet.fetch_add(1, Ordering::Relaxed);
        Ok((id, self.encapsulate_ip_with_id(assoc_id, id, packet)?))
    }

    /// Re-encapsulates the same packet number with fresh envelope sequence numbers.
    /// This is required for recovery because replay protection must remain enabled.
    pub fn encapsulate_ip_with_id(
        &self,
        assoc_id: u16,
        packet_id: u32,
        packet: &[u8],
    ) -> Result<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        for frame in fragment_ip(assoc_id, packet_id, packet, self.config.mtu)? {
            out.push(self.wrap(&frame)?);
        }
        Ok(out)
    }

    /// Emits compact ACK ranges. The old single ACK frame is still accepted on receive.
    pub fn make_ack(&self, packet_id: u32) -> Result<Vec<u8>> {
        let ranges = self.ack_tracker.lock().record(packet_id);
        self.wrap(&Frame::AckRanges { ranges })
    }

    pub fn make_probe(&self, token: u64, sent_micros: u64) -> Result<Vec<u8>> {
        self.wrap(&Frame::Probe { token, sent_micros })
    }

    pub fn make_probe_ack(&self, token: u64, sent_micros: u64) -> Result<Vec<u8>> {
        self.wrap(&Frame::ProbeAck { token, sent_micros })
    }

    pub fn make_heartbeat(&self, counter: u64) -> Result<Vec<u8>> {
        self.wrap(&Frame::Heartbeat { counter })
    }

    pub fn decapsulate(&self, datagram: &[u8]) -> Result<Option<DecodedPacket>> {
        let (meta, frame) = open_datagram(&self.crypto, datagram)?;
        // Multiple workers of one device share a key but use distinct sessions. This check
        // prevents provider broadcast media (notably a shared Jazz room) from cross-feeding workers.
        if meta.session_id != self.session_id {
            return Ok(None);
        }
        if !self.replay.lock().accept(meta.sequence) {
            return Err(crate::Error::Replay);
        }
        match &frame {
            Frame::Data {
                assoc_id,
                packet_id,
                ..
            } => {
                if self.delivered.lock().contains(packet_id) {
                    return Ok(Some(DecodedPacket::Duplicate {
                        packet_id: *packet_id,
                    }));
                }
                let completed = self.reassembly.lock().push(meta.session_id, &frame)?;
                if let Some(bytes) = completed {
                    let mut delivered = self.delivered.lock();
                    delivered.insert(*packet_id);
                    while delivered.len() > 1024 {
                        let Some(first) = delivered.first().copied() else {
                            break;
                        };
                        delivered.remove(&first);
                    }
                    Ok(Some(DecodedPacket::Ip {
                        assoc_id: *assoc_id,
                        packet_id: *packet_id,
                        bytes,
                    }))
                } else {
                    Ok(None)
                }
            },
            Frame::Ack { packet_id, .. } => Ok(Some(DecodedPacket::Ack {
                packet_id: *packet_id,
            })),
            Frame::AckRanges { ranges } => Ok(Some(DecodedPacket::AckRanges {
                ranges: ranges.clone(),
            })),
            Frame::Heartbeat { counter } => Ok(Some(DecodedPacket::Heartbeat { counter: *counter })),
            Frame::Probe { token, sent_micros } => Ok(Some(DecodedPacket::Probe {
                token: *token,
                sent_micros: *sent_micros,
            })),
            Frame::ProbeAck { token, sent_micros } => Ok(Some(DecodedPacket::ProbeAck {
                token: *token,
                sent_micros: *sent_micros,
            })),
            Frame::Dissociate { assoc_id } => Ok(Some(DecodedPacket::Dissociate {
                assoc_id: *assoc_id,
            })),
        }
    }
}
