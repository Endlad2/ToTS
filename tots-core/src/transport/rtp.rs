/// The TURN service forwards media packets but drops bare ToTS datagrams.
fn append(payload: &[u8], sequence: u16, ssrc: u32, packet: &mut Vec<u8>) {
    packet.extend_from_slice(&[0x80, 96]);
    packet.extend_from_slice(&sequence.to_be_bytes());
    packet.extend_from_slice(&(u32::from(sequence) * 960).to_be_bytes());
    packet.extend_from_slice(&ssrc.to_be_bytes());
    packet.extend_from_slice(payload);
}

pub fn encode(payload: &[u8], sequence: u16, ssrc: u32) -> Vec<u8> {
    let mut packet = Vec::with_capacity(12 + payload.len());
    append(payload, sequence, ssrc, &mut packet);
    packet
}

pub fn decode(packet: &[u8]) -> Option<&[u8]> {
    if packet.len() >= 16
        && packet[0] == 0x80
        && packet[1] & 0x7f == 96
        && packet[12..].starts_with(b"TOTS")
    {
        Some(&packet[12..])
    } else {
        None
    }
}

pub fn encode_datagram_into(packet: &[u8], out: &mut Vec<u8>) -> crate::Result<()> {
    let meta = crate::protocol::peek_meta(packet)?;
    append(packet, meta.sequence as u16, meta.session_id as u32, out);
    Ok(())
}

pub fn encode_datagram(packet: &[u8]) -> crate::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(12 + packet.len());
    encode_datagram_into(packet, &mut out)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_encrypted_datagram_inside_rtp() {
        let payload = b"TOTS encrypted bytes";
        let packet = encode(payload, 17, 42);
        assert_eq!(&packet[..2], &[0x80, 96]);
        assert_eq!(&packet[2..4], &17u16.to_be_bytes());
        assert_eq!(&packet[8..12], &42u32.to_be_bytes());
        assert_eq!(decode(&packet), Some(payload.as_slice()));
    }

    #[test]
    fn encode_into_matches_encode() {
        let payload = b"TOTS encrypted bytes";
        let mut out = Vec::new();
        append(payload, 17, 42, &mut out);
        assert_eq!(out, encode(payload, 17, 42));
    }

    #[test]
    fn rejects_truncated_or_other_rtp_streams() {
        for size in 0..12 {
            assert!(decode(&vec![0x80; size]).is_none());
        }
        let mut packet = encode(b"TOTS encrypted bytes", 1, 1);
        packet[1] = 97;
        assert!(decode(&packet).is_none());
        assert!(decode(b"TOTS raw datagram").is_none());
    }
}
