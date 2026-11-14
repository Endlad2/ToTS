mod bootstrap;
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
#[command(name = "tots-android", version, about = "ToTS Android VpnService worker binary")]
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
    /// JSON object keyed by provider.
    #[arg(long)]
    provider_data: Option<String>,
    /// Number of independent transport workers.
    #[arg(long = "n", default_value_t = 1)]
    n: usize,
    /// Optional provider proportions, e.g. {"jazz":"50","vk":"50"}.
    #[arg(long = "workers_percent", visible_alias = "workers-percent")]
    workers_percent: Option<String>,
    /// Inherited Android VpnService TUN file descriptor. Required unless --probe-only.
    #[arg(long)]
    tun_fd: Option<i32>,
    /// Override the URI's IPv4 TUN MTU (576..=1400); the VpnService must use the same MTU.
    #[arg(long)]
    mtu: Option<usize>,
    /// Exclude the direct VPS path and use provider relay/native transport only.
    #[arg(long)]
    relay_only: bool,
    /// Check that all requested encrypted workers are healthy without consuming a TUN fd.
    #[arg(long)]
    probe_only: bool,
    /// Stop gracefully after this many seconds (for tests/app lifecycle integration).
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
                .add_directive("tots_android=info".parse()?),
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

    let route_endpoint_count = manager.route_ips().await.len();
    info!(
        workers = manager.healthy_count(),
        workers_target = manager.desired_count(),
        route_endpoints = route_endpoint_count,
        virtual_ip = %cfg.virtual_ipv4(),
        "ToTS Android worker pool ready"
    );

    if args.probe_only {
        manager.log_status();
        manager.shutdown().await;
        return Ok(());
    }

    let mut tun = match tun_io::open(args.tun_fd).await {
        Ok(tun) => tun,
        Err(error) => {
            manager.shutdown().await;
            return Err(error);
        }
    };

    info!(
        tun_fd = tun.original_fd(),
        workers = manager.healthy_count(),
        virtual_ip = %cfg.virtual_ipv4(),
        mtu = cfg.mtu,
        "ToTS Android TUN connected"
    );

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
        result = shutdown_signal() => result,
        _ = deadline => Ok(()),
    };

    status_task.abort();
    manager.stop();
    manager.shutdown().await;
    result
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .context("install SIGTERM handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("install Ctrl+C handler"),
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context("install Ctrl+C handler")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_android_worker_flags() {
        let args = Args::try_parse_from([
            "tots-android",
            "--probe-only",
            "--relay-only",
            "--run-for-seconds",
            "30",
            "--n",
            "12",
            "--workers_percent",
            r#"{"jazz":"50","vk":"50"}"#,
        ])
        .unwrap();
        assert!(args.relay_only && args.probe_only);
        assert_eq!(args.run_for_seconds, Some(30));
        assert_eq!(args.n, 12);
        assert_eq!(args.workers_percent.as_deref(), Some(r#"{"jazz":"50","vk":"50"}"#));
    }

    #[test]
    fn explicit_mtu_overrides_uri() {
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
        let args =
            Args::try_parse_from(["tots-android", "--config", &uri, "--mtu", "900"]).unwrap();
        assert_eq!(config_from_args(&args).unwrap().mtu, 900);
    }
}
