use anyhow::{anyhow, Context, Result};
use chacha20poly1305::{
    aead::{Aead, KeyInit, OsRng},
    ChaCha20Poly1305, Nonce,
};
use rand::RngCore;
use sha2::{Digest, Sha256};

use crate::Frame;

const NONCE_LEN: usize = 12;
const AAD: &[u8] = b"TOTS/1";

#[derive(Clone)]
pub struct PacketCipher {
    cipher: ChaCha20Poly1305,
}

impl PacketCipher {
    pub fn new(password: &str) -> Self {
        let digest = Sha256::digest(password.as_bytes());
        let cipher = ChaCha20Poly1305::new_from_slice(&digest)
            .expect("SHA-256 always produces a 32-byte key");
        Self { cipher }
    }

    pub fn seal(&self, frame: &Frame) -> Result<Vec<u8>> {
        let plaintext = postcard::to_allocvec(frame).context("serialize ToTS frame")?;
        let mut nonce_bytes = [0u8; NONCE_LEN];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = self
            .cipher
            .encrypt(
                nonce,
                chacha20poly1305::aead::Payload {
                    msg: &plaintext,
                    aad: AAD,
                },
            )
            .map_err(|_| anyhow!("ChaCha20-Poly1305 encryption failed"))?;

        let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    pub fn open(&self, packet: &[u8]) -> Result<Frame> {
        if packet.len() <= NONCE_LEN {
            return Err(anyhow!("encrypted packet too short"));
        }
        let (nonce_bytes, ciphertext) = packet.split_at(NONCE_LEN);
        let nonce = Nonce::from_slice(nonce_bytes);
        let plaintext = self
            .cipher
            .decrypt(
                nonce,
                chacha20poly1305::aead::Payload {
                    msg: ciphertext,
                    aad: AAD,
                },
            )
            .map_err(|_| anyhow!("ChaCha20-Poly1305 authentication failed"))?;
        postcard::from_bytes(&plaintext).context("decode ToTS frame")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let cipher = PacketCipher::new("01234567890123456789012345678901");
        let frame = Frame::Data {
            payload: vec![1, 2, 3, 4],
        };
        let packet = cipher.seal(&frame).unwrap();
        assert_eq!(cipher.open(&packet).unwrap(), frame);
    }

    #[test]
    fn tamper_is_rejected() {
        let cipher = PacketCipher::new("secret");
        let mut packet = cipher.seal(&Frame::Ping { nonce: 7 }).unwrap();
        *packet.last_mut().unwrap() ^= 1;
        assert!(cipher.open(&packet).is_err());
    }
}
