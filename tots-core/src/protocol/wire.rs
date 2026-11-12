use super::Frame;
use crate::{crypto::PacketCrypto, Error, Result};
use bytes::{Buf, BufMut, BytesMut};
use uuid::Uuid;

const MAGIC: &[u8; 4] = b"TOTS";
const VERSION: u8 = 1;
const AAD_LEN: usize = 4 + 1 + 16 + 8 + 8;
const HEADER_LEN: usize = AAD_LEN + 24;
pub(crate) const SEALED_ENVELOPE_OVERHEAD: usize = HEADER_LEN + 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnvelopeMeta {
    pub device_id: Uuid,
    pub session_id: u64,
    pub sequence: u64,
}

pub fn peek_meta(datagram: &[u8]) -> Result<EnvelopeMeta> {
    if datagram.len() < HEADER_LEN + 16 {
        return Err(Error::Packet("short ToTS datagram".into()));
    }
    if &datagram[..4] != MAGIC || datagram[4] != VERSION {
        return Err(Error::Packet("bad ToTS magic/version".into()));
    }
    let device_id =
        Uuid::from_slice(&datagram[5..21]).map_err(|_| Error::Packet("bad uuid".into()))?;
    let mut b = &datagram[21..37];
    let session_id = b.get_u64();
    let sequence = b.get_u64();
    Ok(EnvelopeMeta {
        device_id,
        session_id,
        sequence,
    })
}

pub fn seal_datagram(crypto: &PacketCrypto, meta: EnvelopeMeta, frame: &Frame) -> Result<Vec<u8>> {
    seal_encoded_datagram(crypto, meta, &frame.encode())
}

pub(crate) fn seal_encoded_datagram(
    crypto: &PacketCrypto,
    meta: EnvelopeMeta,
    encoded_frame: &[u8],
) -> Result<Vec<u8>> {
    let mut aad = BytesMut::with_capacity(AAD_LEN);
    aad.extend_from_slice(MAGIC);
    aad.put_u8(VERSION);
    aad.extend_from_slice(meta.device_id.as_bytes());
    aad.put_u64(meta.session_id);
    aad.put_u64(meta.sequence);
    let (nonce, ciphertext) = crypto.seal(&aad, encoded_frame)?;
    let mut out = Vec::with_capacity(aad.len() + 24 + ciphertext.len());
    out.extend_from_slice(&aad);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

pub fn open_datagram(crypto: &PacketCrypto, datagram: &[u8]) -> Result<(EnvelopeMeta, Frame)> {
    let meta = peek_meta(datagram)?;
    let aad = &datagram[..AAD_LEN];
    let nonce: &[u8; 24] = datagram[AAD_LEN..HEADER_LEN]
        .try_into()
        .map_err(|_| Error::Packet("bad nonce".into()))?;
    let plain = crypto.open(aad, nonce, &datagram[HEADER_LEN..])?;
    Ok((meta, Frame::decode(&plain)?))
}

#[derive(Clone, Debug, Default)]
pub struct ReplayWindow {
    max: Option<u64>,
    bits: u128,
}
impl ReplayWindow {
    pub fn accept(&mut self, sequence: u64) -> bool {
        match self.max {
            None => {
                self.max = Some(sequence);
                self.bits = 1;
                true
            }
            Some(max) if sequence > max => {
                let shift = (sequence - max).min(128) as u32;
                self.bits = if shift >= 128 {
                    1
                } else {
                    (self.bits << shift) | 1
                };
                self.max = Some(sequence);
                true
            }
            Some(max) => {
                let delta = max - sequence;
                if delta >= 128 {
                    return false;
                }
                let mask = 1u128 << delta;
                if self.bits & mask != 0 {
                    return false;
                }
                self.bits |= mask;
                true
            }
        }
    }
}
