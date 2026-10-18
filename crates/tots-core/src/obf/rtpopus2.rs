//! Профиль rtpopus2: RTP + RFC 8285 one-byte extensions.

use std::io;

use chacha20poly1305::aead::{AeadInPlace, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::RngCore;

pub const KEY_LEN: usize = 32;
const RTP_HDR_LEN: usize = 12;
const RTP_EXT_LEN: usize = 12;
const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
pub const HEADER_LEN: usize = RTP_HDR_LEN + RTP_EXT_LEN + NONCE_LEN;
pub const OVERHEAD: usize = HEADER_LEN + TAG_LEN;

const RTP_VERSION: u8 = 0x90;
const RTP_PT: u8 = 0x6F;
const RTP_MARKER: u8 = 0x80;
const TS_STEP: u32 = 960;

pub struct Conn {
    aead: ChaCha20Poly1305,
    session_id: [u8; 4],
    ssrc: [u8; 4],
    counter: u64,
    seq: u32,
    timestamp: u32,
    tcc: u16,
    first: bool,
}

impl Conn {
    pub fn new(key: &[u8], is_server: bool) -> io::Result<Self> {
        if key.len() != KEY_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rtpopus2: bad key len",
            ));
        }
        let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
        let mut rng = rand::thread_rng();
        let mut rnd = [0u8; 16];
        rng.fill_bytes(&mut rnd);
        let mut session_id = [0u8; 4];
        let mut ssrc = [0u8; 4];
        session_id.copy_from_slice(&rnd[0..4]);
        ssrc.copy_from_slice(&rnd[4..8]);
        if is_server {
            session_id[0] |= 0x80;
        } else {
            session_id[0] &= !0x80;
        }
        let mut cb = [0u8; 8];
        rng.fill_bytes(&mut cb);

        Ok(Self {
            aead: cipher,
            session_id,
            ssrc,
            counter: u64::from_be_bytes(cb),
            seq: u16::from_be_bytes([rnd[8], rnd[9]]) as u32,
            timestamp: u32::from_be_bytes([rnd[10], rnd[11], rnd[12], rnd[13]]),
            tcc: u16::from_be_bytes([rnd[14], rnd[15]]),
            first: true,
        })
    }

    pub fn wrap_into_mut(&mut self, dst: &mut [u8], payload: &[u8]) -> io::Result<usize> {
        let wire_len = OVERHEAD + payload.len();
        if dst.len() < wire_len {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "dst too small"));
        }

        self.seq = self.seq.wrapping_add(1);
        let seq = (self.seq & 0xFFFF) as u16;
        self.timestamp = self.timestamp.wrapping_add(TS_STEP);

        let mut pt = RTP_PT;
        if self.first {
            pt |= RTP_MARKER;
            self.first = false;
        }

        // 1. Заголовок.
        dst[0] = RTP_VERSION;
        dst[1] = pt;
        dst[2..4].copy_from_slice(&seq.to_be_bytes());
        dst[4..8].copy_from_slice(&self.timestamp.to_be_bytes());
        dst[8..12].copy_from_slice(&self.ssrc);

        dst[12] = 0xBE;
        dst[13] = 0xDE;
        dst[14..16].copy_from_slice(&2u16.to_be_bytes());
        dst[16] = 0x10;
        dst[17] = 0x80 | ((seq & 0x3F) as u8);
        dst[18] = 0x21;
        self.tcc = self.tcc.wrapping_add(1);
        dst[19..21].copy_from_slice(&self.tcc.to_be_bytes());
        dst[21] = 0;
        dst[22] = 0;
        dst[23] = 0;

        // 2. Nonce пишем в dst[24..HEADER_LEN].
        dst[24..28].copy_from_slice(&self.session_id);
        self.counter = self.counter.wrapping_add(1);
        dst[28..HEADER_LEN].copy_from_slice(&self.counter.to_be_bytes());

        // 3. Копируем nonce и AAD в собственные буферы.
        let mut nonce_bytes = [0u8; NONCE_LEN];
        nonce_bytes.copy_from_slice(&dst[24..HEADER_LEN]);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let mut aad = [0u8; HEADER_LEN];
        aad.copy_from_slice(&dst[..HEADER_LEN]);

        // 4. Payload.
        dst[HEADER_LEN..HEADER_LEN + payload.len()].copy_from_slice(payload);

        // 5. Шифрование.
        let (ct, tag_slot) = dst[HEADER_LEN..wire_len].split_at_mut(payload.len());
        let tag = self
            .aead
            .encrypt_in_place_detached(nonce, &aad, ct)
            .map_err(|_| io::Error::other("rtpopus2: encrypt failed"))?;
        tag_slot[..TAG_LEN].copy_from_slice(&tag);
        Ok(wire_len)
    }

    pub fn unwrap_into_mut(&mut self, wire: &[u8], dst: &mut [u8]) -> io::Result<usize> {
        if wire.len() < OVERHEAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "packet too short",
            ));
        }
        let payload_len = wire.len() - OVERHEAD;
        if dst.len() < payload_len {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "dst too small"));
        }

        let mut nonce_bytes = [0u8; NONCE_LEN];
        nonce_bytes.copy_from_slice(&wire[24..HEADER_LEN]);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let mut aad = [0u8; HEADER_LEN];
        aad.copy_from_slice(&wire[..HEADER_LEN]);

        let (ct, tag) = wire[HEADER_LEN..].split_at(payload_len);
        dst[..payload_len].copy_from_slice(ct);

        self.aead
            .decrypt_in_place_detached(
                nonce,
                &aad,
                &mut dst[..payload_len],
                chacha20poly1305::Tag::from_slice(tag),
            )
            .map_err(|_| io::Error::other("rtpopus2: decrypt failed"))?;

        Ok(payload_len)
    }
}
