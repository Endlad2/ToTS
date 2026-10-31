mod stun;

use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use axum::{
    extract::{Path as AxumPath, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use bytes::BytesMut;
use clap::Parser;
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    net::{TcpListener, UdpSocket},
    sync::{mpsc, Mutex, RwLock},
};
use tracing::{error, info, warn};
use tots_core::{
    Frame, PacketCipher, PROTOCOL_VERSION, DEFAULT_LOCAL_PORT, DEFAULT_TURN_PORT,
    DEFAULT_WEBRTC_PORT,
};
use uuid::Uuid;
use webrtc::{
    data_channel::{DataChannel, DataChannelEvent},
    peer_connection::{
        PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCConfigurationBuilder,
        RTCIceGatheringState, RTCIceServer, RTCPeerConnectionState, RTCSessionDescription,
    },
};

#[derive(Parser, Debug)]
#[command(name = "tots-server", version, about = "ToTS VPN relay/server")]
struct Cli {
    /// Address written into generated client configs when --stun is not used.
    #[arg(long, default_value = "127.0.0.1")]
    public_host: String,

    /// Discover the externally visible server IP with STUN and publish that IP
    /// in generated client configs instead of --public-host.
    #[arg(long, default_value_t = false)]
    stun: bool,

    /// RFC 5389 STUN server used by --stun.
    #[arg(long, default_value = "stun.l.google.com:19302")]
    stun_server: String,

    /// Timeout for the STUN Binding request.
    #[arg(long, default_value_t = 5)]
    stun_timeout_secs: u64,

    /// Local-only management API bind address.
    #[arg(long, default_value = "127.0.0.1")]
    api_bind: IpAddr,

    #[arg(long, default_value_t = 8787)]
    api_port: u16,

    /// TCP HTTP signaling port for native WebRTC. ICE uses SDP-advertised UDP candidates.
    #[arg(long, default_value_t = DEFAULT_WEBRTC_PORT)]
    webrtc_port: u16,

    /// UDP target port reached by a client through its provider TURN relay.
    #[arg(long, default_value_t = DEFAULT_TURN_PORT)]
    turn_port: u16,

    /// Local UDP destination that receives decrypted tunnel payloads.
    #[arg(long, default_value = "127.0.0.1:40000")]
    backend: SocketAddr,

    #[arg(long, default_value = "tots-server-state.json")]
    state_file: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistentState {
    master_password: String,
    devices: HashSet<Uuid>,
}

impl PersistentState {
    fn fresh() -> Self {
        Self {
            master_password: random_password(),
            devices: HashSet::new(),
        }
    }
}

#[derive(Clone)]
struct AppState {
    cipher: PacketCipher,
    master_password: String,
    devices: Arc<RwLock<HashSet<Uuid>>>,
    state_file: PathBuf,
    public_host: String,
    stun_server: Option<String>,
    webrtc_port: u16,
    turn_port: u16,
    backend: SocketAddr,
    peers: Arc<Mutex<Vec<Arc<dyn PeerConnection>>>>,
}

#[derive(Debug, Deserialize)]
struct CreateConfigRequest {
    #[serde(default = "default_provider")]
    provider: String,
    #[serde(default)]
    provider_data: Value,
    #[serde(default = "default_local_port")]
    local_port: u16,
}

fn default_provider() -> String {
    "vk".to_string()
}

fn default_local_port() -> u16 {
    DEFAULT_LOCAL_PORT
}

#[derive(Debug, Serialize)]
struct ClientConfigResponse {
    server: String,
    device_id: Uuid,
    password: String,
    provider: String,
    provider_data: Value,
    local_port: u16,
    webrtc_port: u16,
    turn_port: u16,
}

#[derive(Debug, Serialize)]
struct DevicesResponse {
    devices: Vec<Uuid>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    ok: bool,
    protocol_version: u16,
    public_host: String,
    webrtc_port: u16,
    turn_port: u16,
    stun: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tots_server=info,tots_core=info".into()),
        )
        .init();

    let cli = Cli::parse();

    let public_host = if cli.stun {
        let public_ip = stun::discover_public_ip(
            &cli.stun_server,
            std::time::Duration::from_secs(cli.stun_timeout_secs),
        )
        .await
        .context("STUN public IP discovery failed")?;
        info!(
            public_ip = %public_ip,
            stun_server = %cli.stun_server,
            "STUN discovered server public IP"
        );
        public_ip.to_string()
    } else {
        cli.public_host.clone()
    };

    let persisted = load_or_create_state(&cli.state_file)?;
    persist_state(&cli.state_file, &persisted)?;

    let state = AppState {
        cipher: PacketCipher::new(&persisted.master_password),
        master_password: persisted.master_password,
        devices: Arc::new(RwLock::new(persisted.devices)),
        state_file: cli.state_file,
        public_host,
        stun_server: cli.stun.then(|| cli.stun_server.clone()),
        webrtc_port: cli.webrtc_port,
        turn_port: cli.turn_port,
        backend: cli.backend,
        peers: Arc::new(Mutex::new(Vec::new())),
    };

    let management_state = state.clone();
    let management_addr = SocketAddr::new(cli.api_bind, cli.api_port);
    let management = tokio::spawn(async move {
        if let Err(err) = run_management_api(management_state, management_addr).await {
            error!(%err, "management API exited");
        }
    });

    let signaling_state = state.clone();
    let signaling = tokio::spawn(async move {
        if let Err(err) = run_signaling(signaling_state).await {
            error!(%err, "WebRTC signaling exited");
        }
    });

    let turn_state = state.clone();
    let turn_ingress = tokio::spawn(async move {
        if let Err(err) = run_turn_ingress(turn_state).await {
            error!(%err, "TURN ingress exited");
        }
    });

    info!(
        api = %management_addr,
        public_host = %state.public_host,
        stun = cli.stun,
        webrtc_signaling_port = state.webrtc_port,
        turn_target_port = state.turn_port,
        backend = %state.backend,
        "ToTS server started"
    );

    tokio::signal::ctrl_c().await?;
    info!("shutdown requested");

    management.abort();
    signaling.abort();
    turn_ingress.abort();

    let peers = std::mem::take(&mut *state.peers.lock().await);
    for peer in peers {
        let _ = peer.close().await;
    }

    Ok(())
}

async fn run_management_api(state: AppState, addr: SocketAddr) -> Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/configs", post(create_config).get(list_devices))
        .route("/v1/configs/{device_id}", delete(delete_config))
        .with_state(state);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        protocol_version: PROTOCOL_VERSION,
        public_host: state.public_host.clone(),
        webrtc_port: state.webrtc_port,
        turn_port: state.turn_port,
        stun: state.stun_server.is_some(),
    })
}

async fn create_config(
    State(state): State<AppState>,
    Json(req): Json<CreateConfigRequest>,
) -> Result<Json<ClientConfigResponse>, (StatusCode, String)> {
    let device_id = Uuid::new_v4();
    state.devices.write().await.insert(device_id);
    save_devices(&state).await.map_err(internal_server_error)?;

    Ok(Json(ClientConfigResponse {
        server: state.public_host.clone(),
        device_id,
        password: state.master_password.clone(),
        provider: req.provider,
        provider_data: req.provider_data,
        local_port: req.local_port,
        webrtc_port: state.webrtc_port,
        turn_port: state.turn_port,
    }))
}

async fn list_devices(State(state): State<AppState>) -> Json<DevicesResponse> {
    let mut devices: Vec<_> = state.devices.read().await.iter().copied().collect();
    devices.sort();
    Json(DevicesResponse { devices })
}

async fn delete_config(
    State(state): State<AppState>,
    AxumPath(device_id): AxumPath<Uuid>,
) -> Result<StatusCode, (StatusCode, String)> {
    let removed = state.devices.write().await.remove(&device_id);
    if !removed {
        return Err((StatusCode::NOT_FOUND, "unknown device_id".to_string()));
    }
    save_devices(&state).await.map_err(internal_server_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn save_devices(state: &AppState) -> Result<()> {
    let snapshot = PersistentState {
        master_password: state.master_password.clone(),
        devices: state.devices.read().await.clone(),
    };
    let json = serde_json::to_vec_pretty(&snapshot)?;
    tokio::fs::write(&state.state_file, json).await?;
    Ok(())
}

fn internal_server_error(err: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

async fn run_turn_ingress(state: AppState) -> Result<()> {
    let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), state.turn_port);
    let socket = Arc::new(UdpSocket::bind(bind).await?);
    let sessions: Arc<Mutex<HashMap<SocketAddr, Arc<UdpSocket>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let mut buf = vec![0u8; 65_535];

    loop {
        let (n, source) = socket.recv_from(&mut buf).await?;
        let frame = match state.cipher.open(&buf[..n]) {
            Ok(frame) => frame,
            Err(err) => {
                warn!(%source, %err, "discarding undecryptable TURN datagram");
                continue;
            }
        };

        match frame {
            Frame::Probe { version, nonce } => {
                if version == PROTOCOL_VERSION {
                    send_udp_frame(
                        &socket,
                        source,
                        &state.cipher,
                        Frame::ProbeAck {
                            version: PROTOCOL_VERSION,
                            nonce,
                        },
                    )
                    .await?;
                }
            }
            Frame::Auth { device_id } => {
                let valid = state.devices.read().await.contains(&device_id);
                if valid {
                    let backend = UdpSocket::bind("0.0.0.0:0").await?;
                    backend.connect(state.backend).await?;
                    let backend = Arc::new(backend);
                    sessions.lock().await.insert(source, backend.clone());
                    spawn_backend_to_turn(
                        backend,
                        socket.clone(),
                        source,
                        state.cipher.clone(),
                    );
                }
                send_udp_frame(
                    &socket,
                    source,
                    &state.cipher,
                    Frame::AuthResult {
                        ok: valid,
                        message: if valid {
                            "ok".to_string()
                        } else {
                            "invalid device_id".to_string()
                        },
                    },
                )
                .await?;
            }
            Frame::Data { payload } => {
                if let Some(backend) = sessions.lock().await.get(&source).cloned() {
                    if let Err(err) = backend.send(&payload).await {
                        if err.kind() == std::io::ErrorKind::ConnectionRefused {
                            warn!(%source, %err, "backend temporarily refused TURN payload; keeping session alive");
                            continue;
                        }
                        return Err(err.into());
                    }
                }
            }
            Frame::SignalOffer { sdp_json } => {
                let offer: RTCSessionDescription = match serde_json::from_str(&sdp_json) {
                    Ok(v) => v,
                    Err(err) => {
                        warn!(%source, %err, "invalid TURN-carried WebRTC offer");
                        continue;
                    }
                };
                match create_webrtc_answer(state.clone(), offer).await {
                    Ok(answer) => {
                        let sdp_json = serde_json::to_string(&answer)?;
                        send_udp_frame(
                            &socket,
                            source,
                            &state.cipher,
                            Frame::SignalAnswer { sdp_json },
                        )
                        .await?;
                    }
                    Err(err) => warn!(%source, %err, "failed to answer TURN-carried WebRTC offer"),
                }
            }
            Frame::Ping { nonce } => {
                send_udp_frame(&socket, source, &state.cipher, Frame::Pong { nonce }).await?;
            }
            Frame::ProbeAck { .. }
            | Frame::SignalAnswer { .. }
            | Frame::AuthResult { .. }
            | Frame::Pong { .. } => {}
        }
    }
}

fn spawn_backend_to_turn(
    backend: Arc<UdpSocket>,
    relay_socket: Arc<UdpSocket>,
    client: SocketAddr,
    cipher: PacketCipher,
) {
    tokio::spawn(async move {
        let mut buf = vec![0u8; 65_535];
        loop {
            match backend.recv(&mut buf).await {
                Ok(n) => {
                    let frame = Frame::Data {
                        payload: buf[..n].to_vec(),
                    };
                    match cipher.seal(&frame) {
                        Ok(packet) => {
                            if let Err(err) = relay_socket.send_to(&packet, client).await {
                                warn!(%client, %err, "failed to send TURN response");
                                break;
                            }
                        }
                        Err(err) => {
                            warn!(%client, %err, "failed to encrypt TURN response");
                            break;
                        }
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::ConnectionRefused => {
                    warn!(%client, %err, "backend temporarily refused UDP; keeping TURN return path alive");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
                Err(err) => {
                    warn!(%client, %err, "backend receive failed");
                    break;
                }
            }
        }
    });
}

async fn send_udp_frame(
    socket: &UdpSocket,
    destination: SocketAddr,
    cipher: &PacketCipher,
    frame: Frame,
) -> Result<()> {
    let packet = cipher.seal(&frame)?;
    socket.send_to(&packet, destination).await?;
    Ok(())
}

async fn run_signaling(state: AppState) -> Result<()> {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), state.webrtc_port);
    let app = Router::new()
        .route("/webrtc/offer", post(webrtc_offer))
        .with_state(state);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn webrtc_offer(
    State(state): State<AppState>,
    Json(offer): Json<RTCSessionDescription>,
) -> Result<Json<RTCSessionDescription>, (StatusCode, String)> {
    create_webrtc_answer(state, offer)
        .await
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn create_webrtc_answer(
    state: AppState,
    offer: RTCSessionDescription,
) -> Result<RTCSessionDescription> {
    let (gather_tx, mut gather_rx) = mpsc::channel::<()>(1);
    let handler = Arc::new(ServerPeerHandler {
        state: state.clone(),
        gather_tx,
    });

    let config = if let Some(stun_server) = state.stun_server.as_deref() {
        RTCConfigurationBuilder::default()
            .with_ice_servers(vec![RTCIceServer {
                urls: vec![format!("stun:{stun_server}")],
                username: String::new(),
                credential: String::new(),
            }])
            .build()
    } else {
        RTCConfigurationBuilder::default().build()
    };
    let peer: Arc<dyn PeerConnection> = Arc::new(
        PeerConnectionBuilder::new()
            .with_configuration(config)
            .with_handler(handler)
            .with_udp_addrs(vec!["0.0.0.0:0".to_string()])
            .build()
            .await
            .context("build server WebRTC peer")?,
    );

    peer.set_remote_description(offer)
        .await
        .context("set server remote WebRTC offer")?;
    let answer = peer.create_answer(None).await.context("create WebRTC answer")?;
    peer.set_local_description(answer)
        .await
        .context("set server local WebRTC answer")?;

    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), gather_rx.recv()).await;
    let local = peer
        .local_description()
        .await
        .context("missing server WebRTC local description")?;

    state.peers.lock().await.push(peer);
    Ok(local)
}

#[derive(Clone)]
struct ServerPeerHandler {
    state: AppState,
    gather_tx: mpsc::Sender<()>,
}

#[async_trait]
impl PeerConnectionEventHandler for ServerPeerHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_tx.try_send(());
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        info!(?state, "WebRTC peer state changed");
        if matches!(state, RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed) {
            warn!(?state, "WebRTC peer unavailable");
        }
    }

    async fn on_data_channel(&self, data_channel: Arc<dyn DataChannel>) {
        let state = self.state.clone();
        tokio::spawn(async move {
            if let Err(err) = serve_data_channel(state, data_channel).await {
                warn!(%err, "WebRTC data channel ended");
            }
        });
    }
}

async fn serve_data_channel(state: AppState, data_channel: Arc<dyn DataChannel>) -> Result<()> {
    let backend: Arc<Mutex<Option<Arc<UdpSocket>>>> = Arc::new(Mutex::new(None));

    while let Some(event) = data_channel.poll().await {
        match event {
            DataChannelEvent::OnOpen => info!("ToTS WebRTC data channel opened"),
            DataChannelEvent::OnMessage(message) => {
                let frame = match state.cipher.open(&message.data) {
                    Ok(frame) => frame,
                    Err(err) => {
                        warn!(%err, "discarding undecryptable WebRTC packet");
                        continue;
                    }
                };

                match frame {
                    Frame::Probe { version, nonce } if version == PROTOCOL_VERSION => {
                        send_data_channel_frame(
                            &data_channel,
                            &state.cipher,
                            Frame::ProbeAck {
                                version: PROTOCOL_VERSION,
                                nonce,
                            },
                        )
                        .await?;
                    }
                    Frame::Auth { device_id } => {
                        let valid = state.devices.read().await.contains(&device_id);
                        if valid {
                            let sock = UdpSocket::bind("0.0.0.0:0").await?;
                            sock.connect(state.backend).await?;
                            let sock = Arc::new(sock);
                            *backend.lock().await = Some(sock.clone());
                            spawn_backend_to_data_channel(
                                sock,
                                data_channel.clone(),
                                state.cipher.clone(),
                            );
                        }
                        send_data_channel_frame(
                            &data_channel,
                            &state.cipher,
                            Frame::AuthResult {
                                ok: valid,
                                message: if valid {
                                    "ok".to_string()
                                } else {
                                    "invalid device_id".to_string()
                                },
                            },
                        )
                        .await?;
                    }
                    Frame::Data { payload } => {
                        if let Some(sock) = backend.lock().await.clone() {
                            if let Err(err) = sock.send(&payload).await {
                                if err.kind() == std::io::ErrorKind::ConnectionRefused {
                                    warn!(%err, "backend temporarily refused WebRTC payload; keeping data channel alive");
                                    continue;
                                }
                                return Err(err.into());
                            }
                        }
                    }
                    Frame::Ping { nonce } => {
                        send_data_channel_frame(
                            &data_channel,
                            &state.cipher,
                            Frame::Pong { nonce },
                        )
                        .await?;
                    }
                    Frame::Probe { .. }
                    | Frame::ProbeAck { .. }
                    | Frame::SignalOffer { .. }
                    | Frame::SignalAnswer { .. }
                    | Frame::AuthResult { .. }
                    | Frame::Pong { .. } => {}
                }
            }
            DataChannelEvent::OnClose => break,
            _ => {}
        }
    }

    Ok(())
}

fn spawn_backend_to_data_channel(
    backend: Arc<UdpSocket>,
    data_channel: Arc<dyn DataChannel>,
    cipher: PacketCipher,
) {
    tokio::spawn(async move {
        let mut buf = vec![0u8; 16_000];
        loop {
            match backend.recv(&mut buf).await {
                Ok(n) => {
                    let frame = Frame::Data {
                        payload: buf[..n].to_vec(),
                    };
                    if let Err(err) = send_data_channel_frame(&data_channel, &cipher, frame).await {
                        warn!(%err, "failed to return backend packet over WebRTC");
                        break;
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::ConnectionRefused => {
                    warn!(%err, "backend temporarily refused UDP; keeping WebRTC return path alive");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
                Err(err) => {
                    warn!(%err, "backend receive failed");
                    break;
                }
            }
        }
    });
}

async fn send_data_channel_frame(
    data_channel: &Arc<dyn DataChannel>,
    cipher: &PacketCipher,
    frame: Frame,
) -> Result<()> {
    let packet = cipher.seal(&frame)?;
    data_channel.send(BytesMut::from(packet.as_slice())).await?;
    Ok(())
}

fn load_or_create_state(path: &Path) -> Result<PersistentState> {
    if path.exists() {
        let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        let state: PersistentState =
            serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))?;
        if state.master_password.chars().count() != 32 {
            anyhow::bail!("master_password in {} must be 32 characters", path.display());
        }
        Ok(state)
    } else {
        Ok(PersistentState::fresh())
    }
}

fn persist_state(path: &Path, state: &PersistentState) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(state)?)
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

fn random_password() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .map(char::from)
        .collect()
}

