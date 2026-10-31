use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use rand::Rng;
use tokio::net::UdpSocket;
use tokio::time::timeout;
use tracing::{info, warn};
use uuid::Uuid;

use crate::transport::{Transport, TurnTransport, WebRtcTransport};
use crate::{Frame, PacketCipher, TurnProvider, PROTOCOL_VERSION};

pub const DEFAULT_LOCAL_PORT: u16 = 7000;
pub const DEFAULT_WEBRTC_PORT: u16 = 8443;
pub const DEFAULT_TURN_PORT: u16 = 50000;
pub const MAX_DATA_PAYLOAD: usize = 16_000;

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub server: String,
    pub local_bind: SocketAddr,
    pub webrtc_port: u16,
    pub turn_port: u16,
    pub password: String,
    pub device_id: Uuid,
    pub connect_timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server: "127.0.0.1".to_string(),
            local_bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), DEFAULT_LOCAL_PORT),
            webrtc_port: DEFAULT_WEBRTC_PORT,
            turn_port: DEFAULT_TURN_PORT,
            password: String::new(),
            device_id: Uuid::nil(),
            connect_timeout: Duration::from_secs(8),
        }
    }
}

/// An authenticated ToTS data session. It exposes raw payload datagrams so
/// front-ends can attach either the legacy local UDP/KCP socket or a real TUN
/// interface without re-implementing transport selection/authentication.
pub struct ClientSession {
    transport: Transport,
    cipher: PacketCipher,
}

impl ClientSession {
    pub fn transport_name(&self) -> &'static str {
        self.transport.name()
    }

    pub async fn send_payload(&self, payload: &[u8]) -> Result<()> {
        if payload.len() > MAX_DATA_PAYLOAD {
            bail!(
                "payload is {} bytes, exceeding ToTS DataChannel limit of {} bytes",
                payload.len(),
                MAX_DATA_PAYLOAD
            );
        }
        let packet = self
            .cipher
            .seal(&Frame::Data { payload: payload.to_vec() })?;
        self.transport.send(packet).await
    }

    /// Wait for the next application payload. Ping frames are handled in-band
    /// and never leak into the attached local interface.
    pub async fn recv_payload(&mut self) -> Result<Vec<u8>> {
        loop {
            let packet = self.transport.recv().await?;
            match self.cipher.open(&packet)? {
                Frame::Data { payload } => return Ok(payload),
                Frame::Ping { nonce } => {
                    self.transport
                        .send(self.cipher.seal(&Frame::Pong { nonce })?)
                        .await?;
                }
                _ => {}
            }
        }
    }

    pub async fn close(&self) {
        self.transport.close().await;
    }
}

/// Establishes both transport probes, chooses the preferred working transport,
/// and authorizes device_id. No local UDP or TUN interface is opened here.
pub async fn connect(config: &ClientConfig, provider: Arc<dyn TurnProvider>) -> Result<ClientSession> {
    if config.password.is_empty() {
        bail!("password must not be empty");
    }
    if config.device_id.is_nil() {
        bail!("device_id must be a non-nil UUIDv4");
    }

    let cipher = PacketCipher::new(&config.password);

    info!("probing direct WebRTC transport");
    let direct = match WebRtcTransport::connect(
        &config.server,
        config.webrtc_port,
        config.connect_timeout,
    )
    .await
    {
        Ok(t) => {
            let mut tr = Transport::WebRtc(t);
            match probe(&mut tr, &cipher, config.connect_timeout).await {
                Ok(()) => Some(tr),
                Err(err) => {
                    warn!(error = %err, "direct WebRTC probe failed");
                    tr.close().await;
                    None
                }
            }
        }
        Err(err) => {
            warn!(error = %err, "direct WebRTC connection failed");
            None
        }
    };

    info!(provider = provider.name(), "requesting TURN credentials");
    let turn = match provider.get_credentials().await {
        Ok(creds) => {
            match TurnTransport::connect(
                &creds,
                &config.server,
                config.turn_port,
                config.connect_timeout,
            )
            .await
            {
                Ok(mut signaling) => {
                    match WebRtcTransport::connect_via_turn(
                        &creds,
                        &mut signaling,
                        &cipher,
                        config.connect_timeout,
                    )
                    .await
                    {
                        Ok(t) => {
                            let mut tr = Transport::TurnWebRtc(t);
                            match probe(&mut tr, &cipher, config.connect_timeout).await {
                                Ok(()) => Some(tr),
                                Err(err) => {
                                    warn!(error = %err, "WebRTC-over-TURN probe failed");
                                    tr.close().await;
                                    None
                                }
                            }
                        }
                        Err(err) => {
                            signaling.close().await;
                            warn!(error = %err, "WebRTC-over-TURN setup failed");
                            None
                        }
                    }
                }
                Err(err) => {
                    warn!(error = %err, "TURN signaling allocation failed");
                    None
                }
            }
        }
        Err(err) => {
            warn!(error = %err, "TURN provider failed");
            None
        }
    };

    let mut transport = match (direct, turn) {
        (Some(direct), Some(turn)) => {
            info!("both transports passed handshake; selecting direct WebRTC");
            turn.close().await;
            direct
        }
        (Some(direct), None) => {
            info!("selecting direct WebRTC");
            direct
        }
        (None, Some(turn)) => {
            info!("selecting WebRTC DataChannel over TURN");
            turn
        }
        (None, None) => bail!("neither WebRTC nor WebRTC-over-TURN passed the probe"),
    };

    authorize(
        &mut transport,
        &cipher,
        config.device_id,
        config.connect_timeout,
    )
    .await?;
    info!(transport = transport.name(), device_id = %config.device_id, "device authorized");

    Ok(ClientSession { transport, cipher })
}

/// Legacy KCP-facing local UDP mode.
pub async fn run(config: ClientConfig, provider: Arc<dyn TurnProvider>) -> Result<()> {
    let mut session = connect(&config, provider).await?;

    let socket = UdpSocket::bind(config.local_bind)
        .await
        .with_context(|| format!("bind local KCP-facing UDP socket at {}", config.local_bind))?;
    info!(local = %config.local_bind, "ToTS local UDP port is listening");

    let mut buf = vec![0u8; MAX_DATA_PAYLOAD];
    let mut last_local_peer: Option<SocketAddr> = None;
    loop {
        tokio::select! {
            local = socket.recv_from(&mut buf) => {
                let (n, peer) = local.context("read local UDP datagram")?;
                last_local_peer = Some(peer);
                session.send_payload(&buf[..n]).await?;
            }
            remote = session.recv_payload() => {
                let payload = remote?;
                if let Some(peer) = last_local_peer {
                    socket.send_to(&payload, peer).await.context("write local UDP response")?;
                }
            }
        }
    }
}

async fn probe(transport: &mut Transport, cipher: &PacketCipher, wait: Duration) -> Result<()> {
    let nonce: u64 = rand::thread_rng().gen();
    transport
        .send(cipher.seal(&Frame::Probe {
            version: PROTOCOL_VERSION,
            nonce,
        })?)
        .await?;
    let packet = timeout(wait, transport.recv())
        .await
        .context("transport probe timeout")??;
    match cipher.open(&packet)? {
        Frame::ProbeAck { version, nonce: n }
            if version == PROTOCOL_VERSION && n == nonce =>
        {
            Ok(())
        }
        other => Err(anyhow!("unexpected probe reply: {other:?}")),
    }
}

async fn authorize(
    transport: &mut Transport,
    cipher: &PacketCipher,
    device_id: Uuid,
    wait: Duration,
) -> Result<()> {
    transport
        .send(cipher.seal(&Frame::Auth { device_id })?)
        .await?;
    let packet = timeout(wait, transport.recv())
        .await
        .context("device authorization timeout")??;
    match cipher.open(&packet)? {
        Frame::AuthResult { ok: true, .. } => Ok(()),
        Frame::AuthResult { ok: false, message } => bail!("device rejected by server: {message}"),
        other => bail!("unexpected device authorization reply: {other:?}"),
    }
}
