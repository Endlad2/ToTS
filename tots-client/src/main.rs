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

    /// Windows full-tunnel mode: pin the ToTS server through the original gateway and
    /// install 0.0.0.0/1 + 128.0.0.0/1 on the TUN interface. Requires Administrator.
    #[arg(long, default_value_t = false)]
    tun_default_route: bool,

    /// DNS servers assigned to the Windows TUN adapter in --tun-default-route mode.
    #[arg(long, value_delimiter = ',', default_value = "1.1.1.1,8.8.8.8")]
    tun_dns: Vec<Ipv4Addr>,

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
    let route_guard = if cli.tun_default_route {
        if session.transport_name() == "webrtc-over-turn" {
            bail!(
                "--tun-default-route currently supports the direct WebRTC transport only; TURN relay endpoints also need pinned host routes before full-tunnel takeover"
            );
        }
        Some(FullTunnelRouteGuard::install(&cli.server, &cli.tun_name, cli.webrtc_port, &cli.tun_dns).await?)
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
    server_ip: Ipv4Addr,
    added_server_route: bool,
    added_low_half: bool,
    added_high_half: bool,
}

#[cfg(not(windows))]
struct FullTunnelRouteGuard;

impl FullTunnelRouteGuard {
    #[cfg(windows)]
    async fn install(server: &str, tun_name: &str, server_port: u16, dns_servers: &[Ipv4Addr]) -> Result<Self> {
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

        let server_prefix = format!("{server_ip}/32");
        let added_server_route = add_windows_route(
            &server_prefix,
            original_index,
            gateway.trim(),
            1,
        )
        .await?;
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
            "Windows full-tunnel IPv4 routes installed"
        );

        // Make sure the helper binary exists; this also gives a clearer failure early on
        // for stripped-down Windows images.
        let _ = Command::new("route.exe").arg("PRINT").output().await;

        Ok(Self {
            tun_index,
            original_index,
            server_ip,
            added_server_route,
            added_low_half,
            added_high_half,
        })
    }

    #[cfg(not(windows))]
    async fn install(_server: &str, _tun_name: &str, _server_port: u16, _dns_servers: &[Ipv4Addr]) -> Result<Self> {
        bail!("--tun-default-route is currently implemented for Windows only")
    }

    #[cfg(windows)]
    async fn cleanup(self) {
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
        if self.added_server_route {
            let prefix = format!("{}/32", self.server_ip);
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
