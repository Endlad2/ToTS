use crate::{protocol::Frame, Error, Result};
use bytes::Bytes;

pub fn fragment_ip(
    assoc_id: u16,
    packet_id: u32,
    packet: &[u8],
    mtu: usize,
) -> Result<Vec<Frame>> {
    const OVERHEAD: usize = 15;
    if packet.len() > u16::MAX as usize {
        return Err(Error::Packet("IP packet too large".into()));
    }
    if mtu <= OVERHEAD + 64 {
        return Err(Error::Packet("MTU too small".into()));
    }
    let chunk = mtu - OVERHEAD;
    let total = packet.len().div_ceil(chunk).max(1);
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
    for (i, part) in packet.chunks(chunk).enumerate() {
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
