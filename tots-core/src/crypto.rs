use crate::{Error, Result};
use argon2::{Algorithm, Argon2, Params, Version};
use chacha20poly1305::{
    aead::{Aead, Payload},
    KeyInit, XChaCha20Poly1305, XNonce,
};
use rand::{rngs::OsRng, RngCore};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Clone)]
pub struct PacketCrypto {
    key: [u8; 32],
}

impl PacketCrypto {
    pub fn derive(password: &str, device: Uuid) -> Result<Self> {
        let mut salt = Sha256::new();
        salt.update(b"ToTS v1 packet key");
        salt.update(device.as_bytes());
        let salt = salt.finalize();
        let mut key = [0u8; 32];
        let params = Params::new(64 * 1024, 3, 1, Some(32)).map_err(|_| Error::Crypto)?;
        Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
            .hash_password_into(password.as_bytes(), &salt[..16], &mut key)
            .map_err(|_| Error::Crypto)?;
        Ok(Self { key })
    }

    pub fn seal(&self, aad: &[u8], plaintext: &[u8]) -> Result<([u8; 24], Vec<u8>)> {
        let mut nonce = [0u8; 24];
        OsRng.fill_bytes(&mut nonce);
        let cipher = XChaCha20Poly1305::new((&self.key).into());
        let ciphertext = cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|_| Error::Crypto)?;
        Ok((nonce, ciphertext))
    }

    pub fn open(&self, aad: &[u8], nonce: &[u8; 24], ciphertext: &[u8]) -> Result<Vec<u8>> {
        let cipher = XChaCha20Poly1305::new((&self.key).into());
        cipher
            .decrypt(
                XNonce::from_slice(nonce),
                Payload {
                    msg: ciphertext,
                    aad,
                },
            )
            .map_err(|_| Error::Crypto)
    }
}
