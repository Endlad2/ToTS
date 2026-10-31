use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;
use tots_core::{
    ClientConfig, TurnProvider, DEFAULT_LOCAL_PORT, DEFAULT_TURN_PORT, DEFAULT_WEBRTC_PORT,
    MAX_DATA_PAYLOAD,
};
use tots_provider_vk::{VkProvider, VkProviderData};
use tracing::info;
use tracing_subscriber::EnvFilter;
use tun_rs::DeviceBuilder;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "tots-client", version, about = "ToTS VPN client")]
struct Cli {
    /// Public IP or DNS name of tots-server.
    #[arg(long)]
    server: String,

    #[arg(long)]
    device_id: Uuid,

    #[arg(long)]
    password: String,

    #[arg(long, default_value = "vk")]
    provider: String,

    /// Provider-specific JSON object.
    #[arg(long, default_value = "{}")]
    provider_data: String,

    /// Legacy local UDP/KCP-facing port. Ignored when --tun is enabled.
    #[arg(long, default_value_t = DEFAULT_LOCAL_PORT)]
    local_port: u16,

    /// Create a real layer-3 TUN interface and tunnel raw IP packets instead of
    /// opening the legacy local UDP/KCP-facing socket.
    #[arg(long, default_value_t = false)]
    tun: bool,

    /// TUN interface name. On Windows this is the Wintun adapter name.
    #[arg(long, default_value = "tots0")]
    tun_name: String,

    /// IPv4 address assigned to the client TUN interface.
    #[arg(long, default_value = "10.77.0.2")]
    tun_address: Ipv4Addr,

    /// IPv4 prefix length assigned to the TUN interface.
    #[arg(long, default_value_t = 24)]
    tun_prefix: u8,

    /// TUN MTU. Keep this below the ToTS DataChannel message ceiling.
    #[arg(long, default_value_t = 1300)]
    tun_mtu: u16,

    #[arg(long, default_value_t = DEFAULT_WEBRTC_PORT)]
    webrtc_port: u16,

    #[arg(long, default_value_t = DEFAULT_TURN_PORT)]
    turn_port: u16,

    #[arg(long, default_value_t = 8)]
    connect_timeout_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cli = Cli::parse();
    let provider: Arc<dyn TurnProvider> = match cli.provider.as_str() {
        "vk" => {
            let data: VkProviderData = serde_json::from_str(&cli.provider_data)
                .context("parse --provider-data as VK JSON")?;
            Arc::new(VkProvider::new(data)?)
        }
        other => bail!("unsupported provider '{other}'; this build contains only 'vk'"),
    };

    let config = ClientConfig {
        server: cli.server.clone(),
        local_bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), cli.local_port),
        webrtc_port: cli.webrtc_port,
        turn_port: cli.turn_port,
        password: cli.password.clone(),
        device_id: cli.device_id,
        connect_timeout: Duration::from_secs(cli.connect_timeout_secs),
    };

    if cli.tun {
        run_tun(config, provider, &cli).await
    } else {
        tots_core::run(config, provider).await
    }
}

async fn run_tun(
    config: ClientConfig,
    provider: Arc<dyn TurnProvider>,
    cli: &Cli,
) -> Result<()> {
    if cli.tun_prefix == 0 || cli.tun_prefix > 32 {
        bail!("--tun-prefix must be between 1 and 32");
    }
    if cli.tun_mtu < 576 || cli.tun_mtu > 1500 {
        bail!("--tun-mtu must be between 576 and 1500");
    }
    if usize::from(cli.tun_mtu) > MAX_DATA_PAYLOAD {
        bail!(
            "--tun-mtu {} exceeds the ToTS payload limit {}",
            cli.tun_mtu,
            MAX_DATA_PAYLOAD
        );
    }

    // Connect/authenticate first. This preserves the original invariant that
    // no local ingress is opened before the device_id has been accepted.
    let mut session = tots_core::connect(&config, provider).await?;

    let tun = DeviceBuilder::new()
        .name(cli.tun_name.clone())
        .mtu(cli.tun_mtu)
        .ipv4(cli.tun_address, cli.tun_prefix, None)
        .build_async()
        .with_context(|| {
            #[cfg(windows)]
            {
                format!(
                    "create Wintun interface '{}'; run as Administrator and place wintun.dll next to tots-client.exe",
                    cli.tun_name
                )
            }
            #[cfg(not(windows))]
            {
                format!(
                    "create TUN interface '{}'; root/CAP_NET_ADMIN is normally required",
                    cli.tun_name
                )
            }
        })?;

    info!(
        name = %cli.tun_name,
        address = %cli.tun_address,
        prefix = cli.tun_prefix,
        mtu = cli.tun_mtu,
        transport = session.transport_name(),
        "ToTS TUN interface is up"
    );
    info!(
        "route the destination networks you want to tunnel through this interface; ToTS does not replace the host default route automatically"
    );

    // No GSO/offload is enabled, so one read is one L3 packet. The configured
    // MTU is intentionally far below MAX_DATA_PAYLOAD.
    let mut buf = vec![0u8; usize::from(cli.tun_mtu) + 256];
    loop {
        tokio::select! {
            packet = tun.recv(&mut buf) => {
                let n = packet.context("read IP packet from TUN")?;
                if n == 0 {
                    continue;
                }
                session.send_payload(&buf[..n]).await?;
            }
            remote = session.recv_payload() => {
                let payload = remote?;
                if payload.len() > usize::from(cli.tun_mtu) + 256 {
                    bail!("remote IP packet too large for configured TUN MTU: {} bytes", payload.len());
                }
                tun.send(&payload).await.context("inject IP packet into TUN")?;
            }
        }
    }
}
