use crate::{protocol::Frame, Error, Result};
use bytes::Bytes;

/// Raw encrypted ToTS datagrams stay below 1200 bytes. TURN channel-data + RTP +
/// UDP/IPv4 then remains comfortably below common 1280/1500 path MTUs, avoiding
/// IP fragmentation without forcing an unsafe 1420-byte media payload.
pub const SAFE_DATAGRAM_MTU: usize = 1200;
const ENVELOPE_OVERHEAD: usize = 4 + 1 + 16 + 8 + 8 + 24 + 16;
const DATA_FRAME_OVERHEAD: usize = 1 + 2 + 4 + 2 + 2 + 2 + 2;
const MAX_FRAGMENT_PAYLOAD: usize = SAFE_DATAGRAM_MTU - ENVELOPE_OVERHEAD - DATA_FRAME_OVERHEAD;

pub fn fragment_ip(
    assoc_id: u16,
    packet_id: u32,
    packet: &[u8],
    _tun_mtu: usize,
) -> Result<Vec<Frame>> {
    if packet.len() > u16::MAX as usize {
        return Err(Error::Packet("IP packet too large".into()));
    }
    let total = packet.len().div_ceil(MAX_FRAGMENT_PAYLOAD).max(1);
    if total > u16::MAX as usize {
        return Err(Error::Packet("too many fragments".into()));
    }
    let mut out = Vec::with_capacity(total);
    if packet.is_empty() {
        out.push(Frame::Data {
            assoc_id,
            packet_id,
            frag_total: 1,
            frag_id: 0,
            original_len: 0,
            payload: Bytes::new(),
        });
        return Ok(out);
    }
    for (i, part) in packet.chunks(MAX_FRAGMENT_PAYLOAD).enumerate() {
        out.push(Frame::Data {
            assoc_id,
            packet_id,
            frag_total: total as u16,
            frag_id: i as u16,
            original_len: packet.len() as u16,
            payload: Bytes::copy_from_slice(part),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypted_datagram_budget_is_bounded() {
        assert_eq!(
            MAX_FRAGMENT_PAYLOAD + ENVELOPE_OVERHEAD + DATA_FRAME_OVERHEAD,
            1200
        );
    }
}
