//! Session encryption: X25519 key agreement + HKDF-SHA256 + ChaCha20-Poly1305.
//!
//! This mirrors the encryption shape used by `free-turn-proxy-core`: the cover
//! transport only ever sees opaque encrypted frames, so a passive observer of
//! VK / Telemost / Docs traffic cannot distinguish VPN payload from noise.

use chacha20poly1305::aead::{Aead, KeyInit, Payload as AeadPayload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use hkdf::Hkdf;
use rand::rngs::OsRng;
use sha2::Sha256;
use x25519_dalek::{EphemeralSecret, PublicKey, StaticSecret};

use crate::error::{Error, Result};

const INFO: &[u8] = b"tots/v1/session";
const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;

/// Long-term identity derived from the connection password.
pub struct Identity {
    secret: StaticSecret,
    /// Public half, sent to the peer during the handshake.
    pub public: PublicKey,
}

impl Identity {
    /// Derive a deterministic identity from the shared password so both peers
    /// arrive at the same X25519 key material without extra configuration.
    pub fn from_password(password: &str) -> Result<Self> {
        let hk = Hkdf::<Sha256>::new(None, password.as_bytes());
        let mut seed = [0u8; 32];
        hk.expand(b"tots/v1/identity", &mut seed)
            .map_err(|e| Error::Crypto(format!("hkdf identity: {e}")))?;
        let secret = StaticSecret::from(seed);
        let public = PublicKey::from(&secret);
        Ok(Self { secret, public })
    }

    /// Perform a one-shot ECDH against the peer's public key.
    pub fn agree(&self, peer: &PublicKey) -> [u8; 32] {
        *self.secret.diffie_hellman(peer).as_bytes()
    }

    /// Fresh ephemeral key pair for an initiation message.
    pub fn ephemeral() -> (EphemeralSecret, PublicKey) {
        let s = EphemeralSecret::random_from_rng(OsRng);
        let p = PublicKey::from(&s);
        (s, p)
    }
}

/// Symmetric session used to seal and open frames.
pub struct Session {
    cipher: ChaCha20Poly1305,
    counter: u64,
}

impl Session {
    /// Build a session from a shared secret.
    pub fn new(shared: &[u8; 32]) -> Result<Self> {
        let hk = Hkdf::<Sha256>::new(None, shared);
        let mut key = [0u8; 32];
        hk.expand(INFO, &mut key)
            .map_err(|e| Error::Crypto(format!("hkdf session: {e}")))?;
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
        Ok(Self { cipher, counter: 0 })
    }

    fn next_nonce(&mut self) -> Nonce {
        let mut n = [0u8; NONCE_LEN];
        n[4..].copy_from_slice(&self.counter.to_be_bytes());
        self.counter = self.counter.wrapping_add(1);
        *Nonce::from_slice(&n)
    }

    /// Seal a plaintext frame. The counter guarantees nonce uniqueness.
    pub fn seal(&mut self, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        let nonce = self.next_nonce();
        let ct = self
            .cipher
            .encrypt(&nonce, AeadPayload { msg: plaintext, aad })
            .map_err(|e| Error::Crypto(format!("seal: {e}")))?;
        let mut out = Vec::with_capacity(NONCE_LEN + ct.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ct);
        Ok(out)
    }

    /// Open a frame produced by [`Session::seal`].
    pub fn open(&mut self, aad: &[u8], framed: &[u8]) -> Result<Vec<u8>> {
        if framed.len() < NONCE_LEN + TAG_LEN {
            return Err(Error::Crypto("frame too short".into()));
        }
        let (nonce, ct) = framed.split_at(NONCE_LEN);
        let pt = self
            .cipher
            .decrypt(Nonce::from_slice(nonce), AeadPayload { msg: ct, aad })
            .map_err(|e| Error::Crypto(format!("open: {e}")))?;
        Ok(pt)
    }
}