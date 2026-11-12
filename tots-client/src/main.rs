mod bootstrap;
mod routes;
mod tun_io;
mod workers;

use anyhow::{Context, Result};
use clap::Parser;
use std::{sync::Arc, time::Duration};
use tots_core::config::{ConnectionConfig, ProviderName};
use tracing::info;
use uuid::Uuid;
use workers::WorkerManager;

#[derive(Parser, Debug)]
#[command(name = "tots-client", version, about = "ToTS desktop/Android client")]
struct Args {
    /// Server-issued tots:// URI.
    #[arg(long)]
    config: Option<String>,
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
    /// Preferred provider. provider_data may also contain fallback providers.
    #[arg(long, default_value = "vk")]
    provider: String,
    /// JSON object, preferably {"vk":{...},"jazz":{...}}.
    #[arg(long)]
    provider_data: Option<String>,
    /// Number of independent transport workers.
    #[arg(long = "n", default_value_t = 1)]
    n: usize,
    /// Optional provider proportions, e.g. {"jazz":"50","vk":"50"}.
    /// Underscore spelling is canonical; --workers-percent is accepted as an alias.
    #[arg(long = "workers_percent", visible_alias = "workers-percent")]
    workers_percent: Option<String>,
    #[arg(long)]
    tun_fd: Option<i32>,
    #[arg(long, default_value = "tots0")]
    tun_name: String,
    /// Override the URI's IPv4 TUN MTU (576..=1400); explicit fields default to 1200.
    #[arg(long)]
    mtu: Option<usize>,
    /// Exclude the direct VPS path and use provider relay/native transport only.
    #[arg(long)]
    relay_only: bool,
    /// Check that all requested encrypted workers are healthy without opening a TUN device.
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
    if let Some(uri) = &a.config {
        let mut cfg = ConnectionConfig::decode_uri(uri)?;
        if let Some(mtu) = a.mtu {
            cfg.mtu = mtu;
            cfg.validate()?;
        }
        return Ok(cfg);
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
        mtu: a.mtu.unwrap_or(1200),
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
    let (manager, inbound) = WorkerManager::start(
        cfg.clone(),
        args.n,
        args.relay_only,
        args.workers_percent.as_deref(),
    )
    .await?;
    if let Err(error) = manager
        .wait_for_healthy(Duration::from_secs((45 + args.n as u64 * 5).min(300)))
        .await
    {
        manager.shutdown().await;
        return Err(error);
    }
    info!(
        workers = manager.healthy_count(),
        workers_target = manager.desired_count(),
        virtual_ip = %cfg.virtual_ipv4(),
        "ToTS worker pool ready"
    );
    if args.probe_only {
        manager.log_status();
        manager.shutdown().await;
        return Ok(());
    }

    let mut tun = match tun_io::open(&cfg, &args.tun_name, args.tun_fd).await {
        Ok(tun) => tun,
        Err(error) => {
            manager.shutdown().await;
            return Err(error);
        }
    };
    let routes = if args.tun_default_route {
        match routes::install(tun_io::index(&tun)?, manager.route_ips().await).await {
            Ok(routes) => Some(routes),
            Err(error) => {
                manager.shutdown().await;
                return Err(error);
            }
        }
    } else {
        None
    };
    info!(workers = manager.healthy_count(), "ToTS TUN connected");

    let status_manager = Arc::clone(&manager);
    let status_task = tokio::spawn(async move {
        let mut tick = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(5),
            Duration::from_secs(5),
        );
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            status_manager.log_status();
        }
    });
    let deadline = async {
        if let Some(seconds) = args.run_for_seconds {
            tokio::time::sleep(Duration::from_secs(seconds)).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    let result = tokio::select! {
        result = tun_io::run(&mut tun, manager.clone(), inbound) => result,
        result = tokio::signal::ctrl_c() => result.map_err(Into::into),
        _ = deadline => Ok(()),
    };
    status_task.abort();
    manager.stop();
    let cleanup = if let Some(routes) = routes {
        routes.cleanup().await
    } else {
        Ok(())
    };
    manager.shutdown().await;
    cleanup?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_mtu_overrides_uri_and_preserves_validation() {
        let cfg = ConnectionConfig {
            version: 1,
            peer: "203.0.113.2".into(),
            webrtc_port: 7800,
            turn_port: 7801,
            password: "test-password".into(),
            device_id: Uuid::new_v4(),
            provider: ProviderName::Vk,
            provider_data: serde_json::json!({"vk":{"vk_call_links":["test"]}}),
            mtu: 1200,
        };
        let uri = cfg.encode_uri().unwrap();
        let plain = Args::try_parse_from(["tots-client", "--config", &uri]).unwrap();
        assert_eq!(config_from_args(&plain).unwrap().mtu, 1200);
        let overridden =
            Args::try_parse_from(["tots-client", "--config", &uri, "--mtu", "900"]).unwrap();
        assert_eq!(config_from_args(&overridden).unwrap().mtu, 900);
        let invalid =
            Args::try_parse_from(["tots-client", "--config", &uri, "--mtu", "500"]).unwrap();
        assert!(config_from_args(&invalid).is_err());
    }

    #[test]
    fn accepts_worker_count_and_runtime_flags() {
        let args = Args::try_parse_from([
            "tots-client",
            "--relay-only",
            "--probe-only",
            "--tun-default-route",
            "--run-for-seconds",
            "30",
            "--n",
            "12",
            "--workers_percent",
            r#"{"jazz":"50","vk":"50"}"#,
        ])
        .unwrap();
        assert!(args.relay_only && args.probe_only && args.tun_default_route);
        assert_eq!(args.run_for_seconds, Some(30));
        assert_eq!(args.n, 12);
        assert_eq!(args.workers_percent.as_deref(), Some(r#"{"jazz":"50","vk":"50"}"#));
    }
}
