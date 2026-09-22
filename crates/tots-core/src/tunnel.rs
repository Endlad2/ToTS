//! The tunnel engine: ties config, crypto and the cover transport together.
//!
//! The tunnel is payload agnostic. A [`Payload::WireGuard`] tunnel expects UDP
//! datagrams (typically handed over by the platform TUN device) and preserves
//! datagram boundaries. A [`Payload::Xray`] tunnel expects a byte stream (TCP)
//! and simply relays it.

use crate::config::{Config, Payload, TransportKind};
use crate::crypto::{Identity, Session};
use crate::error::{Error, Result};
use crate::transport::{Frame, Transport};
use crate::transports;

/// A live ToTS tunnel.
pub struct Tunnel {
    cfg: Config,
    transport: Box<dyn Transport>,
    session: Session,
    seq: u64,
}

impl Tunnel {
    /// Build a tunnel from configuration.
    ///
    /// Performs the X25519 handshake derivation locally: both peers derive the
    /// same identity from the shared password, so the symmetric session can be
    /// created without an online handshake round trip.
    pub fn new(cfg: Config) -> Result<Self> {
        cfg.validate()?;

        let identity = Identity::from_password(&cfg.password)?;
        // In a full deployment the peer's public key arrives via signalling.
        // Here we derive a symmetric secret from our own identity so the two
        // deterministic peers agree on the same key material.
        let shared = identity.agree(&identity.public);
        let session = Session::new(&shared)?;

        let transport = transports::build(&cfg)?;

        Ok(Self {
            cfg,
            transport,
            session,
            seq: 1,
        })
    }

    /// Which cover transport is in use.
    pub fn transport_kind(&self) -> TransportKind {
        self.transport.kind()
    }

    /// Which payload the tunnel carries.
    pub fn payload(&self) -> Payload {
        self.cfg.payload
    }

    /// Establish the transport session.
    pub async fn connect(&mut self) -> Result<()> {
        self.transport.connect().await
    }

    /// Encrypt a local packet and ship it over the cover transport.
    pub async fn send(&mut self, plaintext: &[u8]) -> Result<()> {
        let aad = self.aad();
        let sealed = self.session.seal(&aad, plaintext)?;
        let frame = Frame::new(sealed, self.seq);
        self.seq += 1;
        self.transport.send(&frame).await
    }

    /// Fetch one remote packet and decrypt it.
    pub async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        let Some(frame) = self.transport.recv().await? else {
            return Ok(None);
        };
        let aad = self.aad();
        let plaintext = self.session.open(&aad, &frame.data)?;
        Ok(Some(plaintext))
    }

    /// Shut the tunnel down.
    pub async fn close(&mut self) -> Result<()> {
        self.transport.close().await
    }

    /// Additional authenticated data binds each frame to the connection.
    fn aad(&self) -> Vec<u8> {
        let mut aad = Vec::with_capacity(64);
        aad.extend_from_slice(self.cfg.transport.id().as_bytes());
        aad.push(b':');
        aad.extend_from_slice(self.cfg.server.as_bytes());
        aad
    }
}

/// Driver for a [`Payload::WireGuard`] tunnel: UDP in, UDP out.
///
/// The real binary binds this to a TUN device; the logic is kept here so it can
/// be unit-tested without elevated privileges.
pub struct WireGuardDriver {
    tunnel: Tunnel,
}

impl WireGuardDriver {
    pub fn new(cfg: Config) -> Result<Self> {
        if cfg.payload != Payload::WireGuard {
            return Err(Error::Config("WireGuardDriver requires payload=wireguard".into()));
        }
        Ok(Self {
            tunnel: Tunnel::new(cfg)?,
        })
    }

    /// Relay a single datagram and return the reply, if any.
    pub async fn exchange(&mut self, datagram: &[u8]) -> Result<Option<Vec<u8>>> {
        self.tunnel.send(datagram).await?;
        self.tunnel.recv().await
    }
}

/// Driver for a [`Payload::Xray`] tunnel: TCP byte stream in, TCP byte stream out.
pub struct XrayDriver {
    tunnel: Tunnel,
}

impl XrayDriver {
    pub fn new(cfg: Config) -> Result<Self> {
        if cfg.payload != Payload::Xray {
            return Err(Error::Config("XrayDriver requires payload=xray".into()));
        }
        Ok(Self {
            tunnel: Tunnel::new(cfg)?,
        })
    }

    /// Push a chunk of the TCP stream and read whatever comes back.
    pub async fn pump(&mut self, chunk: &[u8]) -> Result<Option<Vec<u8>>> {
        self.tunnel.send(chunk).await?;
        self.tunnel.recv().await
    }
}