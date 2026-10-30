use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;
use tots_core::{ClientConfig, TurnProvider, DEFAULT_LOCAL_PORT, DEFAULT_TURN_PORT, DEFAULT_WEBRTC_PORT};
use tots_provider_vk::{VkProvider, VkProviderData};
use tracing_subscriber::EnvFilter;
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

    #[arg(long, default_value_t = DEFAULT_LOCAL_PORT)]
    local_port: u16,

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
        server: cli.server,
        local_bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), cli.local_port),
        webrtc_port: cli.webrtc_port,
        turn_port: cli.turn_port,
        password: cli.password,
        device_id: cli.device_id,
        connect_timeout: Duration::from_secs(cli.connect_timeout_secs),
    };

    tots_core::run(config, provider).await
}
