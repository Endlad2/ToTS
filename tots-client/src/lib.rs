mod bootstrap;
mod routes;
mod tun_io;
mod workers;

use anyhow::{Context, Result};
use clap::Parser;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tots_core::config::{ConnectionConfig, ProviderName};
use tracing::{info, warn};
use uuid::Uuid;
use workers::WorkerManager;

#[derive(Parser, Debug)]
#[command(name = "tots-client", version, about = "ToTS desktop/Android client")]
struct Args {
    /// Server-issued tots:// URI.
    #[arg(long, conflicts_with = "config_file")]
    config: Option<String>,
    /// Read the server-issued tots:// URI from a protected file (OpenWrt service mode).
    #[arg(long, value_name = "PATH", conflicts_with = "config")]
    config_file: Option<PathBuf>,
    /// Atomically write runtime state and traffic counters for OpenWrt/LuCI integrations.
    #[arg(long, value_name = "PATH")]
    status_file: Option<PathBuf>,
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
    /// Route all supported Internet traffic through the TUN (enabled by default on Windows).
    #[arg(long)]
    tun_default_route: bool,
    /// Keep Windows system routes unchanged (disables automatic full-tunnel routing).
    #[arg(long)]
    no_tun_default_route: bool,
    /// Stop gracefully after this many seconds (for unattended testing).
    #[arg(long)]
    run_for_seconds: Option<u64>,
    /// Force the physical adapter that carries ToTS transport and provider
    /// signalling (default: detected from the IPv4 default route).
    #[arg(long, value_name = "NAME")]
    egress_interface: Option<String>,
    /// Disable physical-adapter pinning and use the ordinary route table.
    #[arg(long)]
    no_egress_pinning: bool,
}

fn full_tunnel_enabled(args: &Args, is_windows: bool) -> bool {
    args.tun_default_route || (is_windows && !args.no_tun_default_route)
}

fn config_from_args(a: &Args) -> Result<ConnectionConfig> {
    let config_uri = if let Some(path) = &a.config_file {
        Some(
            std::fs::read_to_string(path)
                .with_context(|| format!("read config file {}", path.display()))?,
        )
    } else {
        a.config.clone()
    };
    if let Some(uri) = config_uri.as_deref() {
        let mut cfg = ConnectionConfig::decode_uri(uri.trim())?;
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

fn write_status_file(
    path: &Path,
    state: &str,
    manager: Option<&WorkerManager>,
    cfg: &ConnectionConfig,
    tun_name: &str,
    started_at: Option<Instant>,
) -> Result<()> {
    let tx = manager
        .map(|m| {
            m.stats
                .tun_tx_bytes
                .load(std::sync::atomic::Ordering::Relaxed)
        })
        .unwrap_or(0);
    let rx = manager
        .map(|m| {
            m.stats
                .tun_rx_bytes
                .load(std::sync::atomic::Ordering::Relaxed)
        })
        .unwrap_or(0);
    let healthy = manager.map(WorkerManager::healthy_count).unwrap_or(0);
    let target = manager.map(WorkerManager::desired_count).unwrap_or(0);
    let payload = serde_json::json!({
        "state": state,
        "connected": state == "connected",
        "interface": tun_name,
        "virtualIp": cfg.virtual_ipv4().to_string(),
        "mtu": cfg.mtu,
        "workersHealthy": healthy,
        "workersTarget": target,
        "txBytes": tx,
        "rxBytes": rx,
        "uptimeSec": started_at.map(|t| t.elapsed().as_secs()).unwrap_or(0),
        "updatedAt": chrono_free_unix_seconds(),
    });
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create status directory {}", parent.display()))?;
    }
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    std::fs::write(&temporary, serde_json::to_vec(&payload)?)
        .with_context(|| format!("write status file {}", temporary.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(&temporary, path)
        .with_context(|| format!("publish status file {}", path.display()))?;
    Ok(())
}

fn chrono_free_unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Resolve the physical underlay for every socket this client opens itself.
///
/// Once the full tunnel is installed the system default route points at the ToTS
/// TUN device. Any transport or provider socket created afterwards would be
/// carried by the tunnel it is building, which is why provider traffic is pinned
/// to a real adapter instead of following the route table.
fn configure_egress(args: &Args, cfg: &ConnectionConfig) {
    tots_core::egress::set_tunnel(Some(&args.tun_name), Some(cfg.virtual_ipv4()));
    if args.no_egress_pinning {
        std::env::set_var(tots_core::egress::EGRESS_DISABLE_ENV, "1");
        info!(
            "physical-adapter pinning disabled; ToTS transport follows the system route table"
        );
        return;
    }
    match tots_core::egress::configure(args.egress_interface.as_deref()) {
        Some(egress) => info!(
            interface = egress.name(),
            interface_index = egress.index().unwrap_or(0),
            address = %egress.address().map(|a| a.to_string()).unwrap_or_default(),
            "ToTS transport is pinned to the physical adapter"
        ),
        None if cfg!(windows) => info!(
            "Windows underlay is not captured yet; ToTS transport will be pinned to the physical adapter as soon as Wintun setup captures it"
        ),
        None => warn!(
            "no physical egress adapter selected; ToTS transport will follow the system route table (set --egress-interface or TOTS_EGRESS_INTERFACE to force one)"
        ),
    }
}

async fn wait_for_shutdown() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut terminate = signal(SignalKind::terminate()).context("register SIGTERM handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("wait for Ctrl-C"),
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context("wait for Ctrl-C")
    }
}

pub async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("tots_client=info".parse()?),
        )
        .init();
    let args = Args::parse();
    let cfg = config_from_args(&args)?;
    cfg.validate()?;
    configure_egress(&args, &cfg);
    let (manager, inbound) = WorkerManager::start(
        cfg.clone(),
        args.n,
        args.relay_only,
        args.workers_percent.as_deref(),
    )
    .await?;
    if let Some(path) = args.status_file.as_deref() {
        if let Err(error) = write_status_file(
            path,
            "connecting",
            Some(&manager),
            &cfg,
            &args.tun_name,
            None,
        ) {
            warn!(%error, "could not write initial OpenWrt status file");
        }
    }
    if args.probe_only {
        if let Err(error) = manager
            .wait_for_healthy(Duration::from_secs((45 + args.n as u64 * 5).min(300)))
            .await
        {
            if let Some(path) = args.status_file.as_deref() {
                let _ = write_status_file(
                    path,
                    "error",
                    Some(&manager),
                    &cfg,
                    &args.tun_name,
                    None,
                );
            }
            manager.shutdown().await;
            return Err(error);
        }
        manager.log_status();
        if let Some(path) = args.status_file.as_deref() {
            if let Err(error) = write_status_file(
                path,
                "ready",
                Some(&manager),
                &cfg,
                &args.tun_name,
                None,
            ) {
                warn!(%error, "could not write OpenWrt probe status file");
            }
        }
        manager.shutdown().await;
        return Ok(());
    }
    info!(
        workers_healthy = manager.healthy_count(),
        workers_target = manager.desired_count(),
        virtual_ip = %cfg.virtual_ipv4(),
        "ToTS worker supervisors launched; full worker-pool health is not a startup gate"
    );

    let full_tunnel = full_tunnel_enabled(&args, cfg!(windows));
    let underlay = if full_tunnel {
        match routes::capture_underlay().await {
            Ok(underlay) => Some(underlay),
            Err(error) => {
                if let Some(path) = args.status_file.as_deref() {
                    let _ = write_status_file(
                        path,
                        "error",
                        Some(&manager),
                        &cfg,
                        &args.tun_name,
                        None,
                    );
                }
                manager.shutdown().await;
                return Err(error);
            }
        }
    } else {
        None
    };

    let mut tun = match tun_io::open(&cfg, &args.tun_name, args.tun_fd).await {
        Ok(tun) => tun,
        Err(error) => {
            if let Some(path) = args.status_file.as_deref() {
                let _ = write_status_file(
                    path,
                    "error",
                    Some(&manager),
                    &cfg,
                    &args.tun_name,
                    None,
                );
            }
            manager.shutdown().await;
            return Err(error);
        }
    };

    // Create Wintun immediately. Do not make the adapter itself wait for slow
    // provider health checks; keep system default routes untouched until the
    // first usable transport path is ready.
    if full_tunnel {
        info!(
            "Windows TUN created; waiting for the first healthy transport before switching system routes"
        );
        // Long enough for one worst-case native media join (signalling + SDP +
        // ICE); a 45 s gate aborted Jazz-only startups while the join was still
        // in progress.
        let gate = (45 + args.n as u64 * 5).min(300);
        if let Err(error) = manager
            .wait_for_first_route_ready(Duration::from_secs(gate))
            .await
        {
            if let Some(path) = args.status_file.as_deref() {
                let _ = write_status_file(
                    path,
                    "error",
                    Some(&manager),
                    &cfg,
                    &args.tun_name,
                    None,
                );
            }
            manager.shutdown().await;
            return Err(error.context(
                "no initial transport route became ready; system routes were left unchanged",
            ));
        }
    }

    let mut routes = if let Some(underlay) = underlay {
        match routes::install(tun_io::index(&tun)?, manager.route_ips().await, underlay).await {
            Ok(routes) => Some(routes),
            Err(error) => {
                if let Some(path) = args.status_file.as_deref() {
                    let _ = write_status_file(
                        path,
                        "error",
                        Some(&manager),
                        &cfg,
                        &args.tun_name,
                        None,
                    );
                }
                manager.shutdown().await;
                return Err(error);
            }
        }
    } else {
        None
    };
    if routes.is_some() {
        info!(
            workers = manager.healthy_count(),
            workers_target = manager.desired_count(),
            full_tunnel = true,
            "ToTS TUN connected; Windows full-tunnel routes point to Wintun"
        );
    } else {
        info!(
            workers = manager.healthy_count(),
            "ToTS TUN connected without changing system default routes"
        );
    }

    let connected_at = Instant::now();
    if let Some(path) = args.status_file.as_deref() {
        if let Err(error) = write_status_file(
            path,
            "connected",
            Some(&manager),
            &cfg,
            &args.tun_name,
            Some(connected_at),
        ) {
            warn!(%error, "could not write connected OpenWrt status file");
        }
    }
    let status_manager = Arc::clone(&manager);
    let status_path = args.status_file.clone();
    let status_cfg = cfg.clone();
    let status_tun_name = args.tun_name.clone();
    let status_task = tokio::spawn(async move {
        let mut tick = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        );
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut log_tick = 0u8;
        loop {
            tick.tick().await;
            if let Some(path) = status_path.as_deref() {
                if let Err(error) = write_status_file(
                    path,
                    "connected",
                    Some(&status_manager),
                    &status_cfg,
                    &status_tun_name,
                    Some(connected_at),
                ) {
                    warn!(%error, "could not update OpenWrt status file");
                }
            }
            log_tick = log_tick.wrapping_add(1);
            if log_tick >= 5 {
                status_manager.log_status();
                log_tick = 0;
            }
        }
    });
    let deadline = async {
        if let Some(seconds) = args.run_for_seconds {
            tokio::time::sleep(Duration::from_secs(seconds)).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    let route_refresh_manager = Arc::clone(&manager);
    let route_refresh = async {
        if let Some(route_guard) = routes.as_mut() {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let endpoints = route_refresh_manager.route_ips().await;
                if let Err(error) = route_guard.refresh_protected(endpoints).await {
                    tracing::warn!(%error, "Windows provider escape-route refresh failed");
                }
            }
        } else {
            std::future::pending::<()>().await;
        }
    };
    let result = tokio::select! {
        result = tun_io::run(&mut tun, manager.clone(), inbound) => result,
        _ = route_refresh => Ok(()),
        result = wait_for_shutdown() => result,
        _ = deadline => Ok(()),
    };
    status_task.abort();
    manager.stop();
    if let Some(path) = args.status_file.as_deref() {
        let _ = write_status_file(
            path,
            "disconnected",
            Some(&manager),
            &cfg,
            &args.tun_name,
            None,
        );
    }
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
    fn config_file_and_status_file_flags_are_available() {
        let args = Args::try_parse_from([
            "tots-client",
            "--config-file",
            "/etc/tots/config.uri",
            "--status-file",
            "/var/run/tots/status.json",
        ])
        .unwrap();
        assert_eq!(args.config_file.as_deref(), Some(Path::new("/etc/tots/config.uri")));
        assert_eq!(args.status_file.as_deref(), Some(Path::new("/var/run/tots/status.json")));
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
        assert!(full_tunnel_enabled(&args, true));
        assert_eq!(args.run_for_seconds, Some(30));
        assert_eq!(args.n, 12);
        assert_eq!(args.workers_percent.as_deref(), Some(r#"{"jazz":"50","vk":"50"}"#));

        let defaults = Args::try_parse_from(["tots-client"]).unwrap();
        assert!(full_tunnel_enabled(&defaults, true));
        assert!(!full_tunnel_enabled(&defaults, false));

        let split = Args::try_parse_from(["tots-client", "--no-tun-default-route"]).unwrap();
        assert!(!full_tunnel_enabled(&split, true));
    }
}
