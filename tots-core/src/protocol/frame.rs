use crate::{Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    Data {
        assoc_id: u16,
        packet_id: u32,
        frag_total: u16,
        frag_id: u16,
        original_len: u16,
        payload: Bytes,
    },
    Ack {
        packet_id: u32,
        recv_micros: u32,
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

impl Frame {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = BytesMut::with_capacity(
            64 + match self {
                Self::Data { payload, .. } => payload.len(),
                _ => 0,
            },
        );
        match self {
            Self::Data {
                assoc_id,
                packet_id,
                frag_total,
                frag_id,
                original_len,
                payload,
            } => {
                out.put_u8(0x01);
                out.put_u16(*assoc_id);
                out.put_u32(*packet_id);
                out.put_u16(*frag_total);
                out.put_u16(*frag_id);
                out.put_u16(*original_len);
                out.put_u16(payload.len() as u16);
                out.extend_from_slice(payload);
            }
            Self::Ack {
                packet_id,
                recv_micros,
            } => {
                out.put_u8(0x02);
                out.put_u32(*packet_id);
                out.put_u32(*recv_micros);
            }
            Self::Heartbeat { counter } => {
                out.put_u8(0x03);
                out.put_u64(*counter);
            }
            Self::Probe { token, sent_micros } => {
                out.put_u8(0x04);
                out.put_u64(*token);
                out.put_u64(*sent_micros);
            }
            Self::ProbeAck { token, sent_micros } => {
                out.put_u8(0x05);
                out.put_u64(*token);
                out.put_u64(*sent_micros);
            }
            Self::Dissociate { assoc_id } => {
                out.put_u8(0x06);
                out.put_u16(*assoc_id);
            }
        }
        out.to_vec()
    }

    pub fn decode(input: &[u8]) -> Result<Self> {
        if input.is_empty() {
            return Err(Error::Packet("empty frame".into()));
        }
        let mut b = input;
        let kind = b.get_u8();
        macro_rules! need {
            ($n:expr) => {
                if b.remaining() < $n {
                    return Err(Error::Packet("truncated frame".into()));
                }
            };
        }
        match kind {
            0x01 => {
                need!(14);
                let assoc_id = b.get_u16();
                let packet_id = b.get_u32();
                let frag_total = b.get_u16();
                let frag_id = b.get_u16();
                let original_len = b.get_u16();
                let n = b.get_u16() as usize;
                need!(n);
                if frag_total == 0 || frag_id >= frag_total {
                    return Err(Error::Packet("bad fragment coordinates".into()));
                }
                if n > 1400 {
                    return Err(Error::Packet("fragment too large".into()));
                }
                Ok(Self::Data {
                    assoc_id,
                    packet_id,
                    frag_total,
                    frag_id,
                    original_len,
                    payload: Bytes::copy_from_slice(&b[..n]),
                })
            }
            0x02 => {
                need!(8);
                Ok(Self::Ack {
                    packet_id: b.get_u32(),
                    recv_micros: b.get_u32(),
                })
            }
            0x03 => {
                need!(8);
                Ok(Self::Heartbeat {
                    counter: b.get_u64(),
                })
            }
            0x04 => {
                need!(16);
                Ok(Self::Probe {
                    token: b.get_u64(),
                    sent_micros: b.get_u64(),
                })
            }
            0x05 => {
                need!(16);
                Ok(Self::ProbeAck {
                    token: b.get_u64(),
                    sent_micros: b.get_u64(),
                })
            }
            0x06 => {
                need!(2);
                Ok(Self::Dissociate {
                    assoc_id: b.get_u16(),
                })
            }
            _ => Err(Error::Packet(format!("unknown frame type {kind:#x}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn data_roundtrip() {
        let f = Frame::Data {
            assoc_id: 7,
            packet_id: 9,
            frag_total: 2,
            frag_id: 1,
            original_len: 2000,
            payload: Bytes::from_static(b"abc"),
        };
        assert_eq!(Frame::decode(&f.encode()).unwrap(), f);
    }
}
