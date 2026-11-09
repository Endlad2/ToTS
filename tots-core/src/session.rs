use crate::{
    config::ConnectionConfig,
    crypto::PacketCrypto,
    fragment::fragment_ip,
    protocol::{open_datagram, seal_datagram, EnvelopeMeta, Frame, ReplayWindow},
    reassembly::Reassembler,
    Result,
};
use parking_lot::Mutex;
use rand::{rngs::OsRng, RngCore};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

#[derive(Debug)]
pub enum DecodedPacket {
    Ip {
        assoc_id: u16,
        packet_id: u32,
        bytes: Vec<u8>,
    },
    Ack {
        packet_id: u32,
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

pub struct SessionEngine {
    config: ConnectionConfig,
    crypto: PacketCrypto,
    session_id: u64,
    next_sequence: AtomicU64,
    next_packet: AtomicU32,
    replay: Mutex<ReplayWindow>,
    reassembly: Mutex<Reassembler>,
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
        let mut out = Vec::new();
        for f in fragment_ip(assoc_id, id, packet, self.config.mtu)? {
            out.push(self.wrap(&f)?);
        }
        Ok((id, out))
    }
    pub fn make_ack(&self, packet_id: u32) -> Result<Vec<u8>> {
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
        if !self.replay.lock().accept(meta.sequence) {
            return Err(crate::Error::Replay);
        }
        match &frame {
            Frame::Data {
                assoc_id,
                packet_id,
                ..
            } => Ok(self
                .reassembly
                .lock()
                .push(meta.session_id, &frame)?
                .map(|bytes| DecodedPacket::Ip {
                    assoc_id: *assoc_id,
                    packet_id: *packet_id,
                    bytes,
                })),
            Frame::Ack { packet_id, .. } => Ok(Some(DecodedPacket::Ack {
                packet_id: *packet_id,
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
