use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use rand::Rng;
use tokio::{net::{lookup_host, UdpSocket}, sync::watch};
use tokio::time::{timeout, Instant};
use tracing::{info, warn};
use uuid::Uuid;

use crate::transport::{Transport, TurnTransport, WebRtcTransport};
use crate::{Frame, PacketCipher, RelayScope, TurnCredentials, TurnProvider, PROTOCOL_VERSION};

pub const DEFAULT_LOCAL_PORT: u16 = 7000;
pub const DEFAULT_WEBRTC_PORT: u16 = 8443;
pub const DEFAULT_TURN_PORT: u16 = 50000;
pub const MAX_DATA_PAYLOAD: usize = 16_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportPreference {
    Auto,
    ProviderRelay,
}

impl Default for TransportPreference {
    fn default() -> Self {
        Self::Auto
    }
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub server: String,
    pub local_bind: SocketAddr,
    pub webrtc_port: u16,
    pub turn_port: u16,
    pub password: String,
    pub device_id: Uuid,
    pub connect_timeout: Duration,
    pub transport_preference: TransportPreference,
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
            transport_preference: TransportPreference::Auto,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SessionMode {
    LegacyPayload,
    Tun,
}

/// An authenticated ToTS data session. It exposes raw payload datagrams so
/// front-ends can attach either the legacy local UDP/KCP socket or a real TUN
/// interface without re-implementing transport selection/authentication.
pub struct ClientSession {
    transport: Transport,
    cipher: PacketCipher,
    route_exception_ipv4: Vec<Ipv4Addr>,
    route_exception_updates: Option<watch::Receiver<Vec<Ipv4Addr>>>,
    // Provider-native transports may retain signaling/session state in the
    // provider; keep it alive for the lifetime of this authenticated session.
    _provider: Arc<dyn TurnProvider>,
}

impl ClientSession {
    pub fn transport_name(&self) -> &'static str {
        self.transport.name()
    }

    /// IPv4 endpoints that must stay on the original physical route when a
    /// client installs a full-tunnel default route. This is non-empty for
    /// provider-relayed transports such as TURN and Jazz.
    pub fn route_exception_ipv4(&self) -> &[Ipv4Addr] {
        &self.route_exception_ipv4
    }

    pub fn route_exception_updates(&self) -> Option<watch::Receiver<Vec<Ipv4Addr>>> {
        self.route_exception_updates.clone()
    }

    pub async fn send_payload(&self, payload: &[u8]) -> Result<()> {
        if payload.len() > MAX_DATA_PAYLOAD {
            bail!(
                "payload is {} bytes, exceeding ToTS transport limit of {} bytes",
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

/// Establishes transport probes, chooses the preferred working transport, and
/// authorizes a legacy payload session.
pub async fn connect(config: &ClientConfig, provider: Arc<dyn TurnProvider>) -> Result<ClientSession> {
    connect_mode(config, provider, SessionMode::LegacyPayload).await
}

/// Establishes an authenticated layer-3 TUN session. The explicit AuthTun
/// handshake prevents raw IP packets from ever being mistaken for legacy UDP
/// backend payloads on the server.
pub async fn connect_tun(
    config: &ClientConfig,
    provider: Arc<dyn TurnProvider>,
) -> Result<ClientSession> {
    connect_mode(config, provider, SessionMode::Tun).await
}

fn host_is_not_publicly_relayable(host: &str) -> bool {
    let host = host.trim();
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    let unwrapped = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);
    let Ok(ip) = unwrapped.parse::<IpAddr>() else {
        return false;
    };
    match ip {
        IpAddr::V4(ip) => {
            ip.is_unspecified() || ip.is_loopback() || ip.is_private() || ip.is_link_local()
        }
        IpAddr::V6(ip) => {
            ip.is_unspecified() || ip.is_loopback() || ip.is_unique_local() || ip.is_unicast_link_local()
        }
    }
}

async fn connect_mode(
    config: &ClientConfig,
    provider: Arc<dyn TurnProvider>,
    mode: SessionMode,
) -> Result<ClientSession> {
    if config.password.is_empty() {
        bail!("password must not be empty");
    }
    if config.device_id.is_nil() {
        bail!("device_id must be a non-nil UUIDv4");
    }

    let cipher = PacketCipher::new(&config.password);

    if config.transport_preference == TransportPreference::Auto {
        info!("probing direct WebRTC transport");
        match WebRtcTransport::connect(
            &config.server,
            config.webrtc_port,
            config.connect_timeout,
        )
        .await
        {
            Ok(t) => {
                let mut direct = Transport::WebRtc(t);
                match probe(&mut direct, &cipher, config.connect_timeout).await {
                    Ok(()) => {
                        info!("selecting direct WebRTC");
                        authorize(
                            &mut direct,
                            &cipher,
                            config.device_id,
                            config.connect_timeout,
                            mode,
                        )
                        .await?;
                        info!(transport = direct.name(), device_id = %config.device_id, "device authorized");
                        return Ok(ClientSession {
                            transport: direct,
                            cipher,
                            route_exception_ipv4: Vec::new(),
                            route_exception_updates: None,
                            _provider: provider,
                        });
                    }
                    Err(err) => {
                        warn!(error = %err, "direct WebRTC probe failed; trying provider relay");
                        direct.close().await;
                    }
                }
            }
            Err(err) => {
                warn!(error = %err, "direct WebRTC connection failed; trying provider relay");
            }
        }
    } else {
        info!(provider = provider.name(), "provider relay forced; skipping direct WebRTC probe");
    }

    info!(provider = provider.name(), "connecting provider-native transport if available");
    if let Some(native) = provider
        .connect_transport()
        .await
        .context("provider-native transport failed")?
    {
        let route_exception_ipv4 = native.route_exception_ipv4();
        let route_exception_updates = native.route_exception_updates();
        let mut transport = Transport::Provider(native);
        if let Err(err) = probe(&mut transport, &cipher, config.connect_timeout).await {
            transport.close().await;
            return Err(err).context("provider-native transport probe failed");
        }
        info!(transport = transport.name(), "selecting provider-native transport");
        authorize(
            &mut transport,
            &cipher,
            config.device_id,
            config.connect_timeout,
            mode,
        )
        .await?;
        info!(transport = transport.name(), device_id = %config.device_id, "device authorized");
        return Ok(ClientSession {
            transport,
            cipher,
            route_exception_ipv4,
            route_exception_updates,
            _provider: provider,
        });
    }

    if provider.name() == "jazz" && host_is_not_publicly_relayable(&config.server) {
        bail!(
            "Jazz provider did not expose its native room transport and fallback cannot target {}",
            config.server
        );
    }
    if provider.relay_scope() == RelayScope::ProviderSession
        && provider.name() != "jazz"
        && host_is_not_publicly_relayable(&config.server)
    {
        bail!(
            "provider '{}' uses relay-only WebRTC ICE, but server '{}' is not a publicly relayable address; set tots-server --public-host to its public IP/DNS and regenerate the config",
            provider.name(),
            config.server
        );
    }

    info!(provider = provider.name(), "requesting provider TURN/ICE credentials");
    let creds = provider
        .get_credentials()
        .await
        .context("TURN/ICE provider failed")?;
    let route_exception_ipv4 = resolve_provider_ipv4(provider.as_ref(), &creds).await;

    let relay_webrtc = if provider.relay_scope() == RelayScope::ProviderSession {
        info!(
            provider = provider.name(),
            "using provider session credentials as WebRTC ICE relay only"
        );
        WebRtcTransport::connect_via_turn_ice_http(
            &config.server,
            config.webrtc_port,
            &creds,
            config.connect_timeout,
        )
        .await
        .context("provider-session WebRTC ICE relay setup failed")?
    } else {
        let mut signaling = TurnTransport::connect(
            &creds,
            &config.server,
            config.turn_port,
            config.connect_timeout,
        )
        .await
        .context("provider TURN signaling allocation failed")?;

        match WebRtcTransport::connect_via_turn(
            &creds,
            &mut signaling,
            &cipher,
            config.connect_timeout,
        )
        .await
        {
            Ok(transport) => transport,
            Err(err) => {
                signaling.close().await;
                return Err(err).context("provider-relayed WebRTC setup failed");
            }
        }
    };
    let mut transport = Transport::TurnWebRtc(relay_webrtc);
    if let Err(err) = probe(&mut transport, &cipher, config.connect_timeout).await {
        transport.close().await;
        return Err(err).context("provider-relayed WebRTC probe failed");
    }
    info!(transport = transport.name(), "selecting provider-relayed transport");

    authorize(
        &mut transport,
        &cipher,
        config.device_id,
        config.connect_timeout,
        mode,
    )
    .await?;
    info!(transport = transport.name(), device_id = %config.device_id, "device authorized");

    Ok(ClientSession {
        transport,
        cipher,
        route_exception_ipv4,
        route_exception_updates: None,
        _provider: provider,
    })
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
    let probe_packet = cipher.seal(&Frame::Probe {
        version: PROTOCOL_VERSION,
        nonce,
    })?;
    let deadline = Instant::now() + wait;
    const RETRY: Duration = Duration::from_millis(600);

    // Provider-native media transports are datagram-like. In particular, the
    // first RTP packet can race the remote subscriber track becoming active.
    // Re-send the idempotent Probe until the overall connection deadline rather
    // than turning one dropped first packet into a full connection failure.
    loop {
        transport.send(probe_packet.clone()).await?;
        let now = Instant::now();
        if now >= deadline {
            bail!("transport probe timeout");
        }
        let slice = (deadline - now).min(RETRY);
        match timeout(slice, transport.recv()).await {
            Err(_) => continue,
            Ok(Err(error)) => return Err(error),
            Ok(Ok(packet)) => match cipher.open(&packet)? {
                Frame::ProbeAck { version, nonce: n }
                    if version == PROTOCOL_VERSION && n == nonce =>
                {
                    return Ok(());
                }
                Frame::ProbeAck { version, .. } => bail!(
                    "ToTS protocol mismatch: client version {}, server version {}",
                    PROTOCOL_VERSION,
                    version
                ),
                other => return Err(anyhow!("unexpected probe reply: {other:?}")),
            },
        }
    }
}

async fn authorize(
    transport: &mut Transport,
    cipher: &PacketCipher,
    device_id: Uuid,
    wait: Duration,
    mode: SessionMode,
) -> Result<()> {
    let frame = match mode {
        SessionMode::LegacyPayload => Frame::Auth { device_id },
        SessionMode::Tun => Frame::AuthTun { device_id },
    };
    transport.send(cipher.seal(&frame)?).await?;
    let packet = timeout(wait, transport.recv())
        .await
        .context("device authorization timeout")??;
    match cipher.open(&packet)? {
        Frame::AuthResult { ok: true, .. } => Ok(()),
        Frame::AuthResult { ok: false, message } => bail!("device rejected by server: {message}"),
        other => bail!("unexpected device authorization reply: {other:?}"),
    }
}

async fn resolve_provider_ipv4(
    provider: &dyn TurnProvider,
    creds: &TurnCredentials,
) -> Vec<Ipv4Addr> {
    let mut endpoints: Vec<(String, u16)> = creds
        .servers
        .iter()
        .map(|server| (server.host.clone(), server.port))
        .collect();
    endpoints.extend(provider.route_exception_hosts());

    let mut unique = HashSet::new();
    let mut seen_hosts = HashSet::new();
    for (host, port) in endpoints {
        if !seen_hosts.insert((host.clone(), port)) {
            continue;
        }
        if let Ok(ip) = host.parse::<IpAddr>() {
            if let IpAddr::V4(ip) = ip {
                if is_route_exception(ip) {
                    info!(provider = provider.name(), %ip, "provider direct-route IPv4 discovered");
                    unique.insert(ip);
                }
            }
            continue;
        }

        info!(provider = provider.name(), domain = %host, port, "provider direct-route domain discovered");
        match lookup_host((host.as_str(), port)).await {
            Ok(addrs) => {
                for addr in addrs {
                    if let IpAddr::V4(ip) = addr.ip() {
                        if is_route_exception(ip) && unique.insert(ip) {
                            info!(provider = provider.name(), %ip, "provider direct-route IPv4 discovered");
                        }
                    }
                }
            }
            Err(err) => warn!(
                provider = provider.name(),
                host = %host,
                port,
                %err,
                "could not pre-resolve provider endpoint for full-tunnel route pinning"
            ),
        }
    }
    let mut result: Vec<_> = unique.into_iter().collect();
    result.sort();
    result
}

fn is_route_exception(ip: Ipv4Addr) -> bool {
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_multicast())
}
#[cfg(test)]
mod relay_host_tests {
    use super::host_is_not_publicly_relayable;

    #[test]
    fn jazz_preflight_detects_unreachable_literal_targets() {
        assert!(host_is_not_publicly_relayable("127.0.0.1"));
        assert!(host_is_not_publicly_relayable("10.0.0.5"));
        assert!(host_is_not_publicly_relayable("localhost"));
        assert!(!host_is_not_publicly_relayable("203.0.113.10"));
        assert!(!host_is_not_publicly_relayable("vpn.example.test"));
    }
}

