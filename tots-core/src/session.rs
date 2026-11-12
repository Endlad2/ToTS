use crate::{
    config::ConnectionConfig,
    crypto::PacketCrypto,
    fragment::fragment_ip,
    protocol::{
        open_datagram, seal_encoded_datagram, AckRange, EnvelopeMeta, Frame, ReplayWindow,
        MAX_ACK_RANGES, SEALED_ENVELOPE_OVERHEAD,
    },
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
            device_id: uuid::Uuid::new_v4(),
            provider: crate::config::ProviderName::Vk,
            provider_data: serde_json::json!({"vk_call_links":["test"]}),
            mtu: 1200,
        }
    }

    #[test]
    fn legacy_ack_is_authenticated_fresh_and_preserves_range_history() {
        let cfg = config();
        let crypto = PacketCrypto::derive(&cfg.password, cfg.device_id).unwrap();
        let engine = SessionEngine::with_session_and_crypto(cfg, 1, crypto.clone()).unwrap();
        for (packet_id, sequence) in [(7, 1), (8, 2)] {
            let wire = engine.make_legacy_ack(packet_id).unwrap();
            let (meta, frame) = open_datagram(&crypto, &wire).unwrap();
            assert_eq!(meta.sequence, sequence);
            assert!(matches!(frame, Frame::Ack { packet_id: id, .. } if id == packet_id));
            let mut corrupt = wire;
            let end = corrupt.len() - 1;
            corrupt[end] ^= 1;
            assert!(open_datagram(&crypto, &corrupt).is_err());
        }
        let (meta, frame) = open_datagram(&crypto, &engine.make_ack(9).unwrap()).unwrap();
        assert_eq!(meta.sequence, 3);
        assert!(
            matches!(frame, Frame::AckRanges { ranges } if ranges == vec![AckRange { start: 7, end: 9 }])
        );
    }

    #[test]
    fn shared_key_keeps_session_sequences_and_replay_independent() {
        let cfg = config();
        let crypto = PacketCrypto::derive(&cfg.password, cfg.device_id).unwrap();
        let first = SessionEngine::with_session_and_crypto(cfg.clone(), 1, crypto.clone()).unwrap();
        let second =
            SessionEngine::with_session_and_crypto(cfg.clone(), 2, crypto.clone()).unwrap();
        let receiver = SessionEngine::with_session_and_crypto(cfg, 1, crypto).unwrap();
        let probe = first.make_probe(7, 9).unwrap();
        assert!(matches!(
            receiver.decapsulate(&probe),
            Ok(Some(DecodedPacket::Probe { token: 7, .. }))
        ));
        assert_eq!(receiver.replay_rejections(), 0);
        assert!(receiver.decapsulate(&probe).is_err());
        assert_eq!(receiver.replay_rejections(), 1);
        assert!(second.decapsulate(&probe).unwrap().is_none());
        assert_eq!(
            crate::protocol::peek_meta(&second.make_probe(8, 9).unwrap())
                .unwrap()
                .sequence,
            1
        );
    }

    #[test]
    fn prepared_fragments_survive_128_ack_sequences_before_each_send() {
        let cfg = config();
        let crypto = PacketCrypto::derive(&cfg.password, cfg.device_id).unwrap();
        let sender =
            SessionEngine::with_session_and_crypto(cfg.clone(), 1, crypto.clone()).unwrap();
        let receiver = SessionEngine::with_session_and_crypto(cfg, 1, crypto).unwrap();
        let packet = vec![7; 1400];
        let (packet_id, frames) = sender.prepare_ip(3, &packet).unwrap();
        assert_eq!(frames.len(), 2);
        let mut delivered = None;
        for (fragment, frame) in frames.iter().enumerate() {
            // Both a congestion wait and each fragment's pacing wait may emit many ACKs.
            for ack in 0..128 {
                let ack = sender.make_ack((fragment * 128 + ack + 1) as u32).unwrap();
                assert!(matches!(
                    receiver.decapsulate(&ack),
                    Ok(Some(DecodedPacket::AckRanges { .. }))
                ));
            }
            let sealed = sender.seal_prepared(frame).unwrap();
            assert_eq!(frame.wire_len(), sealed.len());
            assert_eq!(
                crate::protocol::peek_meta(&sealed).unwrap().sequence,
                ((fragment + 1) * 129) as u64
            );
            if let Some(DecodedPacket::Ip {
                packet_id: received_id,
                bytes,
                ..
            }) = receiver.decapsulate(&sealed).unwrap()
            {
                assert_eq!(received_id, packet_id);
                delivered = Some(bytes);
            }
            assert_eq!(receiver.replay_rejections(), fragment as u64);
            assert!(matches!(
                receiver.decapsulate(&sealed),
                Err(crate::Error::Replay)
            ));
            assert_eq!(receiver.replay_rejections(), fragment as u64 + 1);
        }
        assert_eq!(delivered.unwrap(), packet);
    }

    #[test]
    fn prepared_retransmit_preserves_packet_id_and_uses_fresh_sequence() {
        let cfg = config();
        let crypto = PacketCrypto::derive(&cfg.password, cfg.device_id).unwrap();
        let sender =
            SessionEngine::with_session_and_crypto(cfg.clone(), 1, crypto.clone()).unwrap();
        let receiver = SessionEngine::with_session_and_crypto(cfg, 1, crypto).unwrap();
        let frames = sender.prepare_ip_with_id(3, 42, &[9; 100]).unwrap();
        for id in 1..=128 {
            receiver.decapsulate(&sender.make_ack(id).unwrap()).unwrap();
        }
        let sealed = sender.seal_prepared(&frames[0]).unwrap();
        assert_eq!(frames[0].wire_len(), sealed.len());
        assert!(matches!(
            receiver.decapsulate(&sealed),
            Ok(Some(DecodedPacket::Ip { packet_id: 42, .. }))
        ));
    }
}

#[derive(Debug, Default)]
struct AckTracker {
    received: BTreeSet<u32>,
}

impl AckTracker {
    fn observe(&mut self, packet_id: u32) {
        self.received.insert(packet_id);
        // Keep a bounded tail. Packet IDs are monotonically increasing within a session.
        while self.received.len() > 512 {
            let Some(first) = self.received.first().copied() else {
                break;
            };
            self.received.remove(&first);
        }
    }

    fn record(&mut self, packet_id: u32) -> Vec<AckRange> {
        self.observe(packet_id);
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
    replay_rejections: AtomicU64,
    replay: Mutex<ReplayWindow>,
    reassembly: Mutex<Reassembler>,
    delivered: Mutex<BTreeSet<u32>>,
    ack_tracker: Mutex<AckTracker>,
}

/// An encoded frame without an authenticated envelope or allocated sequence number.
pub struct PreparedFrame {
    encoded: Vec<u8>,
}

impl PreparedFrame {
    pub fn wire_len(&self) -> usize {
        SEALED_ENVELOPE_OVERHEAD + self.encoded.len()
    }
}

impl SessionEngine {
    pub fn new(config: ConnectionConfig) -> Result<Self> {
        config.validate()?;
        let crypto = PacketCrypto::derive(&config.password, config.device_id)?;
        Self::new_with_crypto(config, crypto)
    }

    /// Reuses a device key while keeping every worker's sequence and replay state separate.
    pub fn new_with_crypto(config: ConnectionConfig, crypto: PacketCrypto) -> Result<Self> {
        let mut r = OsRng;
        Self::with_session_and_crypto(config, r.next_u64(), crypto)
    }

    pub fn with_session(config: ConnectionConfig, session_id: u64) -> Result<Self> {
        config.validate()?;
        let crypto = PacketCrypto::derive(&config.password, config.device_id)?;
        Self::with_session_and_crypto(config, session_id, crypto)
    }

    pub fn with_session_and_crypto(
        config: ConnectionConfig,
        session_id: u64,
        crypto: PacketCrypto,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            crypto,
            config,
            session_id,
            next_sequence: AtomicU64::new(1),
            next_packet: AtomicU32::new(1),
            replay_rejections: AtomicU64::new(0),
            replay: Mutex::new(ReplayWindow::default()),
            reassembly: Mutex::new(Reassembler::default()),
            delivered: Mutex::new(BTreeSet::new()),
            ack_tracker: Mutex::new(AckTracker::default()),
        })
    }

    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    pub fn replay_rejections(&self) -> u64 {
        self.replay_rejections.load(Ordering::Relaxed)
    }

    fn wrap(&self, frame: &Frame) -> Result<Vec<u8>> {
        self.seal_prepared(&PreparedFrame {
            encoded: frame.encode(),
        })
    }

    /// Allocate the envelope sequence only after congestion/pacing waits, immediately before send.
    pub fn seal_prepared(&self, frame: &PreparedFrame) -> Result<Vec<u8>> {
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        seal_encoded_datagram(
            &self.crypto,
            EnvelopeMeta {
                device_id: self.config.device_id,
                session_id: self.session_id,
                sequence,
            },
            &frame.encoded,
        )
    }

    pub fn prepare_ip(&self, assoc_id: u16, packet: &[u8]) -> Result<(u32, Vec<PreparedFrame>)> {
        let id = self.next_packet.fetch_add(1, Ordering::Relaxed);
        Ok((id, self.prepare_ip_with_id(assoc_id, id, packet)?))
    }

    pub fn prepare_ip_with_id(
        &self,
        assoc_id: u16,
        packet_id: u32,
        packet: &[u8],
    ) -> Result<Vec<PreparedFrame>> {
        Ok(fragment_ip(assoc_id, packet_id, packet, self.config.mtu)?
            .into_iter()
            .map(|frame| PreparedFrame {
                encoded: frame.encode(),
            })
            .collect())
    }

    pub fn encapsulate_ip(&self, assoc_id: u16, packet: &[u8]) -> Result<(u32, Vec<Vec<u8>>)> {
        let (id, frames) = self.prepare_ip(assoc_id, packet)?;
        Ok((
            id,
            frames
                .iter()
                .map(|frame| self.seal_prepared(frame))
                .collect::<Result<_>>()?,
        ))
    }

    /// Re-encapsulates the same packet number with fresh envelope sequence numbers.
    /// This is required for recovery because replay protection must remain enabled.
    pub fn encapsulate_ip_with_id(
        &self,
        assoc_id: u16,
        packet_id: u32,
        packet: &[u8],
    ) -> Result<Vec<Vec<u8>>> {
        self.prepare_ip_with_id(assoc_id, packet_id, packet)?
            .iter()
            .map(|frame| self.seal_prepared(frame))
            .collect()
    }

    /// Emits compact ACK ranges. The old single ACK frame is still accepted on receive.
    pub fn make_ack(&self, packet_id: u32) -> Result<Vec<u8>> {
        let ranges = self.ack_tracker.lock().record(packet_id);
        self.wrap(&Frame::AckRanges { ranges })
    }

    /// Emits a v1 single ACK while retaining history for a later range-capable peer.
    pub fn make_legacy_ack(&self, packet_id: u32) -> Result<Vec<u8>> {
        self.ack_tracker.lock().observe(packet_id);
        self.wrap(&Frame::Ack {
            packet_id,
            recv_micros: 0,
        })
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
            self.replay_rejections.fetch_add(1, Ordering::Relaxed);
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
            }
            Frame::Ack { packet_id, .. } => Ok(Some(DecodedPacket::Ack {
                packet_id: *packet_id,
            })),
            Frame::AckRanges { ranges } => Ok(Some(DecodedPacket::AckRanges {
                ranges: ranges.clone(),
            })),
            Frame::Heartbeat { counter } => {
                Ok(Some(DecodedPacket::Heartbeat { counter: *counter }))
            }
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
