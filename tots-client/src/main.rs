mod socks5;

#[cfg(windows)]
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;
use serde_json::Value;
use tots_core::{
    ClientConfig, TotsConfig, TransportPreference, TurnProvider, DEFAULT_LOCAL_PORT,
    DEFAULT_TURN_PORT, DEFAULT_WEBRTC_PORT, MAX_DATA_PAYLOAD,
};
use tots_provider_jazz::{JazzProvider, JazzProviderData};
use tots_provider_max::{MaxProvider, MaxProviderData};
use tots_provider_vk::{VkProvider, VkProviderData};
use tracing::info;
#[cfg(windows)]
use tracing::warn;
use tracing_subscriber::EnvFilter;
use tun_rs::DeviceBuilder;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "tots-client", version, about = "ToTS SOCKS5-over-QUIC client")]
struct Cli {
    /// Self-contained server-issued config: tots://<base64-json>.
    /// Values explicitly supplied on the command line override the URI values.
    #[arg(long, value_name = "TOTS_URI")]
    config: Option<String>,

    /// Public IP or DNS name of tots-server.
    #[arg(long)]
    server: Option<String>,

    #[arg(long)]
    device_id: Option<Uuid>,

    #[arg(long)]
    password: Option<String>,

    #[arg(long)]
    provider: Option<String>,

    /// Transport selection: auto prefers direct WebRTC, turn forces the selected
    /// provider relay. vk/max force their provider-scoped WebRTC-over-TURN path; jazz forces the SaluteJazz room/SFU data transport.
    #[arg(long, value_parser = ["auto", "turn", "vk", "jazz", "max"])]
    transport: Option<String>,

    /// Provider-specific JSON object. VK accepts join_link or access_token; MAX accepts access_token + group_id; Jazz accepts {"join_link":"...","name":"..."} and can auto-create a temporary guest call.
    #[arg(long)]
    provider_data: Option<String>,

    /// Local SOCKS5 TCP port (127.0.0.1:<port>). In explicit --tun compatibility mode this is unused.
    #[arg(long, visible_alias = "socks-port")]
    local_port: Option<u16>,

    /// Legacy compatibility only: create the old layer-3 TUN interface.
    /// New configs run SOCKS5-over-QUIC by default and ignore the URI tun flag.
    #[arg(long, default_value_t = false)]
    tun: bool,

    /// TUN interface name. On Windows this is the Wintun adapter name.
    #[arg(long)]
    tun_name: Option<String>,

    /// IPv4 address assigned to the client TUN interface.
    #[arg(long)]
    tun_address: Option<Ipv4Addr>,

    /// IPv4 prefix length assigned to the TUN interface.
    #[arg(long)]
    tun_prefix: Option<u8>,

    /// TUN MTU. Keep this below the ToTS encrypted transport payload ceiling.
    #[arg(long)]
    tun_mtu: Option<u16>,

    /// Windows full-tunnel mode: pin the ToTS server through the original gateway and
    /// install 0.0.0.0/1 + 128.0.0.0/1 on the TUN interface. Requires Administrator.
    #[arg(long, default_value_t = false)]
    tun_default_route: bool,

    /// DNS servers assigned to the Windows TUN adapter in --tun-default-route mode.
    #[arg(long, value_delimiter = ',')]
    tun_dns: Vec<Ipv4Addr>,

    #[arg(long)]
    webrtc_port: Option<u16>,

    #[arg(long)]
    turn_port: Option<u16>,

    #[arg(long)]
    connect_timeout_secs: Option<u64>,
}

#[derive(Debug, Clone)]
struct ResolvedCli {
    server: String,
    device_id: Uuid,
    password: String,
    provider: String,
    transport: String,
    provider_data: Value,
    local_port: u16,
    tun: bool,
    tun_name: String,
    tun_address: Ipv4Addr,
    tun_prefix: u8,
    tun_mtu: u16,
    tun_default_route: bool,
    tun_dns: Vec<Ipv4Addr>,
    webrtc_port: u16,
    turn_port: u16,
    connect_timeout_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cli = resolve_cli(Cli::parse())?;
    let transport_preference = match cli.transport.as_str() {
        "auto" => TransportPreference::Auto,
        "turn" | "vk" | "jazz" | "max" => TransportPreference::ProviderRelay,
        other => bail!("unsupported transport '{other}'"),
    };
    let effective_provider = match cli.transport.as_str() {
        "vk" => "vk",
        "jazz" => "jazz",
        "max" => "max",
        _ => cli.provider.as_str(),
    };
    let provider: Arc<dyn TurnProvider> = match effective_provider {
        "vk" => {
            let data: VkProviderData = serde_json::from_value(cli.provider_data.clone())
                .context("parse provider_data as VK JSON")?;
            Arc::new(VkProvider::new(data)?)
        }
        "jazz" => {
            let data: JazzProviderData = serde_json::from_value(cli.provider_data.clone())
                .context("parse provider_data as Jazz JSON")?;
            Arc::new(JazzProvider::new(data)?)
        }
        "max" => {
            let data: MaxProviderData = serde_json::from_value(cli.provider_data.clone())
                .context("parse provider_data as MAX JSON")?;
            Arc::new(MaxProvider::new(data)?)
        }
        other => bail!("unsupported provider '{other}'; supported providers are 'vk', 'jazz', and 'max'"),
    };

    let config = ClientConfig {
        server: cli.server.clone(),
        local_bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), cli.local_port),
        webrtc_port: cli.webrtc_port,
        turn_port: cli.turn_port,
        password: cli.password.clone(),
        device_id: cli.device_id,
        connect_timeout: Duration::from_secs(cli.connect_timeout_secs),
        transport_preference,
    };

    if cli.tun {
        run_tun(config, provider, &cli).await
    } else {
        let socks_bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), cli.local_port);
        socks5::run_socks5_quic(config, provider, socks_bind).await
    }
}

fn resolve_cli(cli: Cli) -> Result<ResolvedCli> {
    let uri_config = cli
        .config
        .as_deref()
        .map(TotsConfig::from_uri)
        .transpose()
        .context("decode --config")?;

    let server = cli
        .server
        .or_else(|| uri_config.as_ref().map(|c| c.server.clone()))
        .context("--server is required unless --config is supplied")?;
    let device_id = cli
        .device_id
        .or_else(|| uri_config.as_ref().map(|c| c.device_id))
        .context("--device-id is required unless --config is supplied")?;
    let password = cli
        .password
        .or_else(|| uri_config.as_ref().map(|c| c.password.clone()))
        .context("--password is required unless --config is supplied")?;

    let provider = cli
        .provider
        .or_else(|| uri_config.as_ref().map(|c| c.provider.clone()))
        .unwrap_or_else(|| "vk".to_string());
    let transport = cli
        .transport
        .or_else(|| uri_config.as_ref().map(|c| c.transport.clone()))
        .unwrap_or_else(|| "auto".to_string());
    let provider_data = match cli.provider_data {
        Some(json) => serde_json::from_str(&json).context("parse --provider-data as JSON")?,
        None => uri_config
            .as_ref()
            .map(|c| c.provider_data.clone())
            .unwrap_or_else(|| Value::Object(Default::default())),
    };

    let local_port = cli
        .local_port
        .or_else(|| uri_config.as_ref().map(|c| c.local_port))
        .unwrap_or(DEFAULT_LOCAL_PORT);
    // `tun` in older tots:// URIs is intentionally ignored. TUN is now an
    // explicit compatibility switch; the default data plane is SOCKS5 over QUIC.
    let tun = cli.tun;
    let tun_name = cli
        .tun_name
        .or_else(|| uri_config.as_ref().map(|c| c.tun_name.clone()))
        .unwrap_or_else(|| "tots0".to_string());
    let tun_address = cli
        .tun_address
        .or_else(|| uri_config.as_ref().map(|c| c.tun_address))
        .unwrap_or_else(|| Ipv4Addr::new(10, 77, 0, 2));
    let tun_prefix = cli
        .tun_prefix
        .or_else(|| uri_config.as_ref().map(|c| c.tun_prefix))
        .unwrap_or(24);
    let tun_mtu = cli
        .tun_mtu
        .or_else(|| uri_config.as_ref().map(|c| c.tun_mtu))
        .unwrap_or(1300);
    let tun_default_route =
        cli.tun_default_route || uri_config.as_ref().map(|c| c.tun_default_route).unwrap_or(false);
    let tun_dns = if cli.tun_dns.is_empty() {
        uri_config
            .as_ref()
            .map(|c| c.tun_dns.clone())
            .unwrap_or_else(|| vec![Ipv4Addr::new(1, 1, 1, 1), Ipv4Addr::new(8, 8, 8, 8)])
    } else {
        cli.tun_dns
    };
    let webrtc_port = cli
        .webrtc_port
        .or_else(|| uri_config.as_ref().map(|c| c.webrtc_port))
        .unwrap_or(DEFAULT_WEBRTC_PORT);
    let turn_port = cli
        .turn_port
        .or_else(|| uri_config.as_ref().map(|c| c.turn_port))
        .unwrap_or(DEFAULT_TURN_PORT);
    let connect_timeout_secs = cli
        .connect_timeout_secs
        .or_else(|| uri_config.as_ref().map(|c| c.connect_timeout_secs))
        .unwrap_or(8);

    Ok(ResolvedCli {
        server,
        device_id,
        password,
        provider,
        transport,
        provider_data,
        local_port,
        tun,
        tun_name,
        tun_address,
        tun_prefix,
        tun_mtu,
        tun_default_route,
        tun_dns,
        webrtc_port,
        turn_port,
        connect_timeout_secs,
    })
}

async fn run_tun(
    config: ClientConfig,
    provider: Arc<dyn TurnProvider>,
    cli: &ResolvedCli,
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
    let mut session = tots_core::connect_tun(&config, provider).await?;

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
    let route_guard = if cli.tun_default_route {
        Some(FullTunnelRouteGuard::install(
            &cli.server,
            &cli.tun_name,
            cli.webrtc_port,
            &cli.tun_dns,
            session.route_exception_ipv4(),
            session.route_exception_updates(),
        ).await?)
    } else {
        info!(
            "no default route takeover requested; add destination routes manually or use --tun-default-route on Windows"
        );
        None
    };

    // No GSO/offload is enabled, so one read is one L3 packet. The configured
    // MTU is intentionally far below MAX_DATA_PAYLOAD.
    let mut buf = vec![0u8; usize::from(cli.tun_mtu) + 256];
    let run_result: Result<()> = async {
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
                shutdown = tokio::signal::ctrl_c() => {
                    shutdown.context("wait for Ctrl+C")?;
                    return Ok(());
                }
            }
        }
    }.await;

    if let Some(guard) = route_guard {
        guard.cleanup().await;
    }
    session.close().await;
    run_result
}

#[cfg(windows)]
struct FullTunnelRouteGuard {
    tun_index: u32,
    original_index: u32,
    added_host_routes: Arc<tokio::sync::Mutex<HashSet<Ipv4Addr>>>,
    route_update_task: Option<tokio::task::JoinHandle<()>>,
    added_low_half: bool,
    added_high_half: bool,
}

#[cfg(not(windows))]
struct FullTunnelRouteGuard;

impl FullTunnelRouteGuard {
    #[cfg(windows)]
    async fn install(
        server: &str,
        tun_name: &str,
        server_port: u16,
        dns_servers: &[Ipv4Addr],
        route_exceptions: &[Ipv4Addr],
        route_updates: Option<tokio::sync::watch::Receiver<Vec<Ipv4Addr>>>,
    ) -> Result<Self> {
        use tokio::net::lookup_host;
        use tokio::process::Command;

        let server_ip = if let Ok(ip) = server.parse::<Ipv4Addr>() {
            ip
        } else {
            lookup_host((server, server_port))
                .await
                .with_context(|| format!("resolve ToTS server '{server}' before route takeover"))?
                .find_map(|addr| match addr.ip() {
                    IpAddr::V4(ip) => Some(ip),
                    IpAddr::V6(_) => None,
                })
                .with_context(|| format!("ToTS server '{server}' has no IPv4 address"))?
        };

        let default = run_powershell(
            r#"$r = Get-NetRoute -AddressFamily IPv4 -DestinationPrefix '0.0.0.0/0' -ErrorAction Stop | Where-Object { $_.NextHop -ne '0.0.0.0' } | Sort-Object RouteMetric | Select-Object -First 1; if (-not $r) { throw 'No IPv4 default route found' }; Write-Output ($r.NextHop + '|' + $r.InterfaceIndex)"#,
        )
        .await?;
        let (gateway, original_index) = default
            .trim()
            .split_once('|')
            .context("unexpected PowerShell default-route output")?;
        let original_index: u32 = original_index
            .trim()
            .parse()
            .context("parse original interface index")?;

        let alias = tun_name.replace('\'', "''");
        let tun_index_text = run_powershell(&format!(
            "$i = Get-NetIPInterface -AddressFamily IPv4 -InterfaceAlias '{alias}' -ErrorAction Stop | Select-Object -First 1; Write-Output $i.InterfaceIndex"
        ))
        .await?;
        let tun_index: u32 = tun_index_text
            .trim()
            .parse()
            .context("parse TUN interface index")?;

        let mut protected_ips = vec![server_ip];
        protected_ips.extend_from_slice(route_exceptions);
        if let Some(receiver) = route_updates.as_ref() {
            let latest = receiver.borrow().clone();
            protected_ips.extend(latest);
        }
        protected_ips.sort();
        protected_ips.dedup();

        let added_host_routes = Arc::new(tokio::sync::Mutex::new(HashSet::new()));
        let known_host_routes = Arc::new(tokio::sync::Mutex::new(HashSet::new()));
        for ip in &protected_ips {
            let prefix = format!("{ip}/32");
            if add_windows_route(&prefix, original_index, gateway.trim(), 1).await? {
                added_host_routes.lock().await.insert(*ip);
            }
            known_host_routes.lock().await.insert(*ip);
        }

        let added_low_half = add_windows_route("0.0.0.0/1", tun_index, "0.0.0.0", 5).await?;
        let added_high_half =
            add_windows_route("128.0.0.0/1", tun_index, "0.0.0.0", 5).await?;

        let dns = if dns_servers.is_empty() {
            vec![Ipv4Addr::new(1, 1, 1, 1), Ipv4Addr::new(8, 8, 8, 8)]
        } else {
            dns_servers.to_vec()
        };
        let dns_array = dns
            .iter()
            .map(|ip| format!("'{}'", ip))
            .collect::<Vec<_>>()
            .join(",");
        run_powershell(&format!(
            "Set-NetIPInterface -InterfaceIndex {tun_index} -InterfaceMetric 5 -ErrorAction Stop; Set-DnsClientServerAddress -InterfaceIndex {tun_index} -ServerAddresses @({dns_array}) -ErrorAction Stop"
        ))
        .await?;

        info!(
            %server_ip,
            original_gateway = %gateway.trim(),
            original_interface = original_index,
            tun_interface = tun_index,
            dns = ?dns,
            pinned_ipv4 = ?protected_ips,
            "Windows full-tunnel IPv4 routes installed"
        );

        // Make sure the helper binary exists; this also gives a clearer failure early on
        // for stripped-down Windows images.
        let _ = Command::new("route.exe").arg("PRINT").output().await;

        let route_update_task = route_updates.map(|mut receiver| {
            let gateway = gateway.trim().to_string();
            let added_host_routes = added_host_routes.clone();
            let known_host_routes = known_host_routes.clone();
            tokio::spawn(async move {
                loop {
                    // Use the current full snapshot. The Jazz provider republishes
                    // it periodically, so failed New-NetRoute calls are retried.
                    let latest = receiver.borrow_and_update().clone();
                    for ip in latest {
                        let already_known = known_host_routes.lock().await.contains(&ip);
                        if already_known {
                            continue;
                        }
                        let prefix = format!("{ip}/32");
                        match add_windows_route(&prefix, original_index, &gateway, 1).await {
                            Ok(added) => {
                                known_host_routes.lock().await.insert(ip);
                                if added {
                                    added_host_routes.lock().await.insert(ip);
                                }
                                info!(
                                    %ip,
                                    original_gateway = %gateway,
                                    original_interface = original_index,
                                    created = added,
                                    "Jazz direct bypass route installed"
                                );
                            }
                            Err(error) => {
                                warn!(%ip, %error, "failed to install Jazz direct bypass route; will retry");
                            }
                        }
                    }
                    if receiver.changed().await.is_err() {
                        break;
                    }
                }
            })
        });

        Ok(Self {
            tun_index,
            original_index,
            added_host_routes,
            route_update_task,
            added_low_half,
            added_high_half,
        })
    }

    #[cfg(not(windows))]
    async fn install(
        _server: &str,
        _tun_name: &str,
        _server_port: u16,
        _dns_servers: &[Ipv4Addr],
        _route_exceptions: &[Ipv4Addr],
        _route_updates: Option<tokio::sync::watch::Receiver<Vec<Ipv4Addr>>>,
    ) -> Result<Self> {
        bail!("--tun-default-route is currently implemented for Windows only")
    }

    #[cfg(windows)]
    async fn cleanup(mut self) {
        if let Some(task) = self.route_update_task.take() {
            task.abort();
            let _ = task.await;
        }
        let _ = run_powershell(&format!(
            "Set-DnsClientServerAddress -InterfaceIndex {} -ResetServerAddresses -ErrorAction SilentlyContinue",
            self.tun_index
        ))
        .await;
        if self.added_low_half {
            let _ = remove_windows_route("0.0.0.0/1", self.tun_index).await;
        }
        if self.added_high_half {
            let _ = remove_windows_route("128.0.0.0/1", self.tun_index).await;
        }
        let added_host_routes: Vec<_> = self.added_host_routes.lock().await.iter().copied().collect();
        for ip in added_host_routes {
            let prefix = format!("{ip}/32");
            let _ = remove_windows_route(&prefix, self.original_index).await;
        }
        info!("Windows full-tunnel routes removed");
    }

    #[cfg(not(windows))]
    async fn cleanup(self) {}
}

#[cfg(windows)]
async fn run_powershell(script: &str) -> Result<String> {
    use tokio::process::Command;
    let output = Command::new("powershell.exe")
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .output()
        .await
        .context("start PowerShell for Windows route configuration")?;
    if !output.status.success() {
        bail!(
            "PowerShell route command failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[cfg(windows)]
async fn add_windows_route(
    prefix: &str,
    interface_index: u32,
    next_hop: &str,
    metric: u32,
) -> Result<bool> {
    let script = format!(
        "$e = Get-NetRoute -AddressFamily IPv4 -DestinationPrefix '{prefix}' -InterfaceIndex {interface_index} -ErrorAction SilentlyContinue | Select-Object -First 1; if ($e) {{ Write-Output 'EXISTS' }} else {{ New-NetRoute -DestinationPrefix '{prefix}' -InterfaceIndex {interface_index} -NextHop '{next_hop}' -RouteMetric {metric} -PolicyStore ActiveStore -ErrorAction Stop | Out-Null; Write-Output 'ADDED' }}"
    );
    Ok(run_powershell(&script).await?.trim() == "ADDED")
}

#[cfg(windows)]
async fn remove_windows_route(prefix: &str, interface_index: u32) -> Result<()> {
    let script = format!(
        "Get-NetRoute -AddressFamily IPv4 -DestinationPrefix '{prefix}' -InterfaceIndex {interface_index} -ErrorAction SilentlyContinue | Remove-NetRoute -Confirm:$false -ErrorAction SilentlyContinue"
    );
    let _ = run_powershell(&script).await?;
    Ok(())
}
