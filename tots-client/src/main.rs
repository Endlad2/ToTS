mod routes;
mod tun_io;
use anyhow::{Context, Result};
use clap::Parser;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tots_core::{
    config::{ConnectionConfig, ProviderName},
    providers::make_provider,
    session::SessionEngine,
    transport::{probe, turn::TurnUdpPath, DatagramPath, DirectUdpPath},
};
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "tots-client", version, about = "ToTS desktop/Android client")]
struct Args {
    #[arg(long, conflicts_with = "config_file")]
    config: Option<String>,
    #[arg(long)]
    config_file: Option<std::path::PathBuf>,
    #[arg(long)]
    peer: Option<String>,
    #[arg(long)]
    webrtc_port: Option<u16>,
    #[arg(long)]
    turn_port: Option<u16>,
    #[arg(long)]
    password: Option<String>,
    #[arg(long)]
    device_id: Option<Uuid>,
    #[arg(long, default_value = "vk")]
    provider: String,
    #[arg(long)]
    provider_data: Option<String>,
    #[arg(long)]
    tun_fd: Option<i32>,
    #[arg(long, default_value = "tots0")]
    tun_name: String,
    #[arg(long, default_value_t = 1200)]
    mtu: usize,
    /// Exclude direct VPS traffic from path selection.
    #[arg(long)]
    relay_only: bool,
    /// Check the encrypted provider path without opening a TUN device.
    #[arg(long)]
    probe_only: bool,
    /// Route all IPv4 Internet traffic through the TUN (Windows).
    #[arg(long)]
    tun_default_route: bool,
    /// Stop gracefully after this many seconds (for unattended testing).
    #[arg(long)]
    run_for_seconds: Option<u64>,
}

fn config_from_args(a: &Args) -> Result<ConnectionConfig> {
    if let Some(file) = &a.config_file {
        return Ok(ConnectionConfig::decode_uri(
            std::fs::read_to_string(file)?.trim(),
        )?);
    }
    if let Some(uri) = &a.config {
        return Ok(ConnectionConfig::decode_uri(uri)?);
    }
    Ok(ConnectionConfig {
        version: 1,
        peer: a.peer.clone().context("--peer required")?,
        webrtc_port: a.webrtc_port.context("--webrtc-port required")?,
        turn_port: a.turn_port.context("--turn-port required")?,
        password: a.password.clone().context("--password required")?,
        device_id: a.device_id.context("--device-id required")?,
        provider: ProviderName::parse(&a.provider)?,
        provider_data: serde_json::from_str(a.provider_data.as_deref().unwrap_or("{}"))?,
        mtu: a.mtu,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("tots_client=info".parse()?),
        )
        .init();
    let args = Args::parse();
    let cfg = config_from_args(&args)?;
    cfg.validate()?;
    let provider = make_provider(&cfg.provider)?;
    info!("fetching provider bootstrap");
    let boot = provider.bootstrap(&cfg.provider_data).await?;
    let run_result: Result<()> = async {
        let engine = Arc::new(SessionEngine::new(cfg.clone())?);
        let peer_ip = cfg.peer_ip()?;
        let mut candidates: Vec<Arc<dyn DatagramPath>> = Vec::new();
        if let Some(path) = provider.connect_native(&cfg.provider_data).await? {
            candidates.push(path);
        } else {
            if !args.relay_only {
                let direct: Arc<dyn DatagramPath> = Arc::new(
                    DirectUdpPath::connect("direct-ice", SocketAddr::new(peer_ip, cfg.webrtc_port))
                        .await?,
                );
                candidates.push(direct);
            }
            for creds in &boot.turn {
                for url in creds
                    .urls
                    .iter()
                    .filter(|url| url.starts_with("turn:") && !url.contains("transport=tcp"))
                {
                    let mut single = creds.clone();
                    single.urls = vec![url.clone()];
                    match TurnUdpPath::connect(&single, SocketAddr::new(peer_ip, cfg.turn_port))
                        .await
                    {
                        Ok(p) => {
                            candidates.push(Arc::new(p));
                        }
                        Err(e) => warn!("TURN candidate failed: {e}"),
                    }
                }
            }
        }
        let mut best: Option<(Duration, Arc<dyn DatagramPath>)> = None;
        for p in candidates {
            match probe::probe(&engine, p.clone(), Duration::from_secs(3)).await {
                Ok(r) => {
                    info!(path=%r.path,rtt_ms=r.rtt.as_millis(),"path healthy");
                    if best.as_ref().is_none_or(|(d, _)| r.rtt < *d) {
                        best = Some((r.rtt, p));
                    }
                }
                Err(e) => warn!(path=%p.name(),"probe failed: {e}"),
            }
        }
        let path = best.context("no healthy ToTS path")?.1;
        info!(path=%path.name(),virtual_ip=%cfg.virtual_ipv4(),"selected path");
        if args.probe_only {
            path.close().await?;
            return Ok(());
        }
        let mut tun = tun_io::open(&cfg, &args.tun_name, args.tun_fd).await?;
        let routes = if args.tun_default_route {
            let mut protected = path.route_ips();
            if let std::net::IpAddr::V4(ip) = peer_ip {
                protected.push(ip);
            }
            Some(routes::install(tun_io::index(&tun)?, protected).await?)
        } else {
            None
        };
        info!(path=%path.name(), "ToTS TUN connected");
        let deadline = async {
            if let Some(seconds) = args.run_for_seconds {
                tokio::time::sleep(Duration::from_secs(seconds)).await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        let result = tokio::select! {
            result = tun_io::run(&mut tun, engine, path.clone()) => result,
            result = tokio::signal::ctrl_c() => result.map_err(Into::into),
            _ = deadline => Ok(()),
        };
        if let Some(routes) = routes {
            routes.cleanup().await?;
        }
        path.close().await?;
        result
    }
    .await;
    if let Err(e) = provider.cleanup(&cfg.provider_data, &boot).await {
        warn!("provider cleanup failed: {e}");
    }
    run_result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_relay_probe_and_bounded_full_tunnel_flags() {
        let args = Args::try_parse_from([
            "tots-client",
            "--relay-only",
            "--probe-only",
            "--tun-default-route",
            "--run-for-seconds",
            "30",
        ])
        .unwrap();
        assert!(args.relay_only && args.probe_only && args.tun_default_route);
        assert_eq!(args.run_for_seconds, Some(30));
    }
}
