#[cfg(any(windows, test))]
use anyhow::Context;
use anyhow::{bail, Result};
use std::net::Ipv4Addr;

/// Owns only routes created by this client. Drop restores routing and TUN DNS.
pub struct FullTunnelRoutes {
    #[cfg(windows)]
    changes: OwnedChanges,
}

/// Physical/default underlay captured before Wintun is opened.
///
/// Capturing this first is deliberate: once Windows starts treating the ToTS
/// adapter as Internet-capable, route/metric changes must not be able to make
/// the VPN transport choose its own TUN as the underlay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UnderlayRoute {
    index: u32,
    gateway: Ipv4Addr,
    metric: u64,
}

pub(crate) async fn capture_underlay() -> Result<UnderlayRoute> {
    #[cfg(windows)]
    {
        tokio::task::spawn_blocking(capture_underlay_sync)
            .await
            .context("Windows underlay discovery task failed")?
    }
    #[cfg(not(windows))]
    {
        bail!("automatic full-tunnel routing is supported only on Windows")
    }
}

pub async fn install(
    tun_index: i32,
    protected: Vec<Ipv4Addr>,
    underlay: UnderlayRoute,
) -> Result<FullTunnelRoutes> {
    #[cfg(windows)]
    {
        // A cancelled caller still lets this task finish; its returned guard is then dropped.
        tokio::task::spawn_blocking(move || install_sync(tun_index, protected, underlay))
            .await
            .context("Windows route installation task failed")?
    }
    #[cfg(not(windows))]
    {
        let _ = (tun_index, protected, underlay);
        bail!("automatic full-tunnel routing is supported only on Windows")
    }
}

impl FullTunnelRoutes {
    pub async fn cleanup(mut self) -> Result<()> {
        #[cfg(windows)]
        {
            tokio::task::spawn_blocking(move || self.cleanup_sync())
                .await
                .context("Windows route cleanup task failed")?
        }
        #[cfg(not(windows))]
        {
            let _ = &mut self;
            Ok(())
        }
    }

    #[cfg(windows)]
    fn cleanup_sync(&mut self) -> Result<()> {
        let mut failures = Vec::new();
        // Remove split defaults before the endpoint escape routes.
        for i in (0..self.changes.routes.len()).rev() {
            let route = self.changes.routes[i];
            match powershell(&format!(
                "{} | Remove-NetRoute -Confirm:$false -ErrorAction Stop | Out-Null",
                route.query()
            )) {
                Ok(_) => {
                    self.changes.routes.remove(i);
                }
                Err(error) => failures.push(format!("{}: {error:#}", route.destination())),
            }
        }
        if let Some(index) = self.changes.dns_index {
            match powershell(&format!(
                "Set-DnsClientServerAddress -InterfaceIndex {index} -ResetServerAddresses -ErrorAction Stop"
            )) {
                Ok(_) => self.changes.dns_index = None,
                Err(error) => failures.push(format!("reset TUN DNS: {error:#}")),
            }
        }
        if !failures.is_empty() {
            bail!("Windows route cleanup failed: {}", failures.join("; "));
        }
        Ok(())
    }
}

impl Drop for FullTunnelRoutes {
    fn drop(&mut self) {
        #[cfg(windows)]
        if let Err(error) = self.cleanup_sync() {
            tracing::warn!("{error:#}");
        }
    }
}

#[cfg(any(windows, test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Route {
    address: Ipv4Addr,
    prefix: u8,
    index: u32,
    gateway: Ipv4Addr,
}

#[cfg(any(windows, test))]
fn parse_default_route(json: &str, excluded_index: u32) -> Result<UnderlayRoute> {
    let value: serde_json::Value = serde_json::from_str(json.trim_start_matches('\u{feff}'))
        .context("parse physical default-route JSON")?;
    let rows = match &value {
        serde_json::Value::Array(rows) => rows.as_slice(),
        serde_json::Value::Object(_) => std::slice::from_ref(&value),
        _ => bail!("expected physical default-route JSON objects"),
    };
    let mut candidates = Vec::new();
    for row in rows {
        let number = |key: &str| -> Result<u32> {
            let value = row
                .get(key)
                .and_then(serde_json::Value::as_u64)
                .with_context(|| format!("invalid route field {key}"))?;
            u32::try_from(value).with_context(|| format!("route field {key} is out of range"))
        };
        let index = number("InterfaceIndex")?;
        if index == 0 || index > i32::MAX as u32 {
            bail!("invalid default-route interface index");
        }
        let gateway: Ipv4Addr = row
            .get("NextHop")
            .and_then(serde_json::Value::as_str)
            .context("missing default-route IPv4 gateway")?
            .parse()
            .context("invalid default-route IPv4 gateway")?;
        if gateway.is_unspecified()
            || gateway.is_loopback()
            || gateway.is_multicast()
            || gateway.is_broadcast()
        {
            bail!("default route has no usable IPv4 gateway");
        }
        let physical_rank = row
            .get("PhysicalRank")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let metric = physical_rank
            .saturating_mul(1_000_000)
            .saturating_add(u64::from(number("RouteMetric")?))
            .saturating_add(u64::from(number("InterfaceMetric")?));
        if index != excluded_index {
            candidates.push(UnderlayRoute {
                index,
                gateway,
                metric,
            });
        }
    }
    candidates
        .into_iter()
        .min_by_key(|r| (r.metric, r.index, r.gateway))
        .context("no connected physical IPv4 default route found")
}

#[cfg(any(windows, test))]
fn route_plan(tun_index: u32, default: UnderlayRoute, mut protected: Vec<Ipv4Addr>) -> Vec<Route> {
    protected.sort_unstable();
    protected.dedup();
    let mut routes: Vec<_> = protected
        .into_iter()
        .map(|address| Route {
            address,
            prefix: 32,
            index: default.index,
            gateway: default.gateway,
        })
        .collect();
    for address in [Ipv4Addr::UNSPECIFIED, Ipv4Addr::new(128, 0, 0, 0)] {
        routes.push(Route {
            address,
            prefix: 1,
            index: tun_index,
            gateway: Ipv4Addr::UNSPECIFIED,
        });
    }
    routes
}

#[cfg(any(windows, test))]
#[derive(Default)]
struct OwnedChanges {
    routes: Vec<Route>,
    dns_index: Option<u32>,
}

#[cfg(any(windows, test))]
impl OwnedChanges {
    fn record(&mut self, route: Route, created: bool) {
        if created {
            self.routes.push(route);
        }
    }
}

#[cfg(windows)]
impl Route {
    fn destination(self) -> String {
        format!("{}/{}", self.address, self.prefix)
    }

    fn query(self) -> String {
        // Every interpolated field is a checked number or an Ipv4Addr, never an interface name.
        format!(
            "Get-NetRoute -AddressFamily IPv4 -PolicyStore ActiveStore -ErrorAction Stop | Where-Object {{ $_.InterfaceIndex -eq {} -and $_.DestinationPrefix -eq '{}' -and $_.NextHop -eq '{}' }}",
            self.index, self.destination(), self.gateway
        )
    }
}

#[cfg(windows)]
fn capture_underlay_sync() -> Result<UnderlayRoute> {
    let forced = match std::env::var("TOTS_PHYSICAL_IFINDEX") {
        Ok(raw) if !raw.trim().is_empty() => {
            let index: u32 = raw
                .trim()
                .parse()
                .context("TOTS_PHYSICAL_IFINDEX must be a positive interface index")?;
            if index == 0 || index > i32::MAX as u32 {
                bail!("TOTS_PHYSICAL_IFINDEX is out of range");
            }
            Some(index)
        }
        _ => None,
    };

    let script = if let Some(index) = forced {
        format!(
            r#"
            $rows = @()
            $adapter = Get-NetAdapter -InterfaceIndex {index} -IncludeHidden -ErrorAction SilentlyContinue | Select-Object -First 1
            if ($null -eq $adapter -or $adapter.Status -ne 'Up') {{
                throw 'TOTS_PHYSICAL_IFINDEX does not name an Up adapter'
            }}
            $ip = Get-NetIPInterface -AddressFamily IPv4 -InterfaceIndex {index} -ErrorAction SilentlyContinue |
                Where-Object {{ $_.ConnectionState -eq 'Connected' }} | Select-Object -First 1
            if ($null -eq $ip) {{
                throw 'TOTS_PHYSICAL_IFINDEX has no connected IPv4 interface'
            }}
            $rows = @(Get-NetRoute -AddressFamily IPv4 -DestinationPrefix '0.0.0.0/0' -InterfaceIndex {index} -PolicyStore ActiveStore -ErrorAction SilentlyContinue |
                Where-Object {{ $_.NextHop -ne '0.0.0.0' }} |
                ForEach-Object {{
                    [PSCustomObject]@{{
                        InterfaceIndex = [uint32]$_.InterfaceIndex
                        NextHop = [string]$_.NextHop
                        RouteMetric = [uint32]$_.RouteMetric
                        InterfaceMetric = [uint32]$ip.InterfaceMetric
                        PhysicalRank = [uint32]0
                    }}
                }})
            if ($rows.Count -eq 0) {{
                throw 'TOTS_PHYSICAL_IFINDEX has no usable active IPv4 default route'
            }}
            ConvertTo-Json -InputObject $rows -Compress
            "#
        )
    } else {
        r#"
        $rows = @()
        $virtualPattern = 'Wintun|(^|[^A-Za-z])TUN([^A-Za-z]|$)|(^|[^A-Za-z])TAP([^A-Za-z]|$)|Virtual|VPN|WireGuard|ZeroTier|Radmin|Hamachi|Loopback|Hyper-V|Tailscale|OpenVPN|ToTS'
        $routes = @(Get-NetRoute -AddressFamily IPv4 -DestinationPrefix '0.0.0.0/0' -PolicyStore ActiveStore -ErrorAction Stop |
            Where-Object { $_.NextHop -ne '0.0.0.0' })
        foreach ($route in $routes) {
            $adapter = Get-NetAdapter -InterfaceIndex $route.InterfaceIndex -IncludeHidden -ErrorAction SilentlyContinue | Select-Object -First 1
            if ($null -eq $adapter -or $adapter.Status -ne 'Up') {
                continue
            }
            $identity = [string]$adapter.Name + ' ' + [string]$adapter.InterfaceDescription
            if ($identity -match $virtualPattern) {
                continue
            }
            $ip = Get-NetIPInterface -AddressFamily IPv4 -InterfaceIndex $route.InterfaceIndex -ErrorAction SilentlyContinue |
                Where-Object { $_.ConnectionState -eq 'Connected' } | Select-Object -First 1
            if ($null -eq $ip) {
                continue
            }

            # Prefer adapters that Windows marks as real hardware. Some Wi-Fi/USB
            # drivers report this incorrectly, so non-blacklisted candidates remain
            # as a lower-priority fallback rather than making discovery fail.
            $rank = 1
            if (($adapter.HardwareInterface -eq $true) -and ($adapter.Virtual -ne $true)) {
                $rank = 0
            }
            $rows += [PSCustomObject]@{
                InterfaceIndex = [uint32]$route.InterfaceIndex
                NextHop = [string]$route.NextHop
                RouteMetric = [uint32]$route.RouteMetric
                InterfaceMetric = [uint32]$ip.InterfaceMetric
                PhysicalRank = [uint32]$rank
            }
        }
        ConvertTo-Json -InputObject $rows -Compress
        "#
        .to_owned()
    };

    let output = powershell(&script).context("read physical IPv4 default route")?;
    let json = String::from_utf8(output.stdout).context("default-route JSON was not UTF-8")?;
    let underlay = parse_default_route(&json, 0)?;
    tracing::info!(
        physical_interface_index = underlay.index,
        physical_gateway = %underlay.gateway,
        forced = forced.is_some(),
        "captured Windows transport underlay"
    );
    Ok(underlay)
}

#[cfg(any(windows, test))]
fn validate_underlay(tun_index: u32, underlay: UnderlayRoute) -> Result<()> {
    if underlay.index == 0 || underlay.index > i32::MAX as u32 {
        bail!("invalid physical underlay interface index");
    }
    if underlay.index == tun_index {
        bail!("refusing to route ToTS transport through its own TUN interface");
    }
    if underlay.gateway.is_unspecified()
        || underlay.gateway.is_loopback()
        || underlay.gateway.is_multicast()
        || underlay.gateway.is_broadcast()
    {
        bail!("physical underlay has no usable IPv4 gateway");
    }
    Ok(())
}

#[cfg(windows)]
fn install_sync(
    tun_index: i32,
    protected: Vec<Ipv4Addr>,
    underlay: UnderlayRoute,
) -> Result<FullTunnelRoutes> {
    if tun_index <= 0 {
        bail!("invalid TUN interface index");
    }
    validate_underlay(tun_index as u32, underlay)?;
    if protected.is_empty() {
        bail!("full tunnel requires at least one protected transport endpoint");
    }
    if protected
        .iter()
        .any(|ip| ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() || ip.is_broadcast())
    {
        bail!("protected transport endpoints must be usable IPv4 addresses");
    }

    let mut guard = FullTunnelRoutes {
        changes: OwnedChanges::default(),
    };
    let result = (|| -> Result<()> {
        let plan = route_plan(tun_index as u32, underlay, protected);
        let protected_count = plan.iter().filter(|route| route.prefix == 32).count();
        tracing::info!(
            physical_interface_index = underlay.index,
            physical_gateway = %underlay.gateway,
            protected_endpoints = protected_count,
            "installing Windows full-tunnel escape routes"
        );

        // The /32 transport routes are deliberately first. Once the two /1 routes
        // exist, every provider/VPS socket must already have a more-specific route
        // through the captured physical gateway.
        for route in plan {
            let output = powershell(&format!(
                "$existing = @({}); if ($existing.Count -eq 0) {{ New-NetRoute -AddressFamily IPv4 -DestinationPrefix '{}' -InterfaceIndex {} -NextHop '{}' -RouteMetric 1 -PolicyStore ActiveStore -ErrorAction Stop | Out-Null; exit 10 }}",
                route.query(), route.destination(), route.index, route.gateway
            ))
            .with_context(|| format!("install IPv4 route {}", route.destination()))?;
            // An exit code cannot be confused by localized warnings on stdout.
            guard
                .changes
                .record(route, output.status.code() == Some(10));
        }

        // Mark before the call so a partially successful DNS update is also reset.
        guard.changes.dns_index = Some(tun_index as u32);
        powershell(&format!(
            "Set-DnsClientServerAddress -InterfaceIndex {tun_index} -ServerAddresses @('1.1.1.1','8.8.8.8') -ErrorAction Stop"
        ))
        .context("set TUN DNS servers")?;
        Ok(())
    })();
    if let Err(error) = result {
        return match guard.cleanup_sync() {
            Ok(()) => Err(error),
            Err(cleanup) => Err(error.context(format!("rollback also failed: {cleanup:#}"))),
        };
    }
    Ok(guard)
}

#[cfg(windows)]
fn powershell(script: &str) -> Result<std::process::Output> {
    use std::os::windows::process::CommandExt;
    use std::process::{Command, Stdio};
    let system_root = std::env::var_os("SystemRoot").context("SystemRoot is missing")?;
    let executable = std::path::PathBuf::from(system_root)
        .join(r"System32\WindowsPowerShell\v1.0\powershell.exe");
    let script = format!(
        "$ErrorActionPreference='Stop'; $ProgressPreference='SilentlyContinue'; [Console]::OutputEncoding=[System.Text.UTF8Encoding]::new($false); try {{ & {{ {script} }} }} catch {{ [Console]::Error.WriteLine($_.Exception.Message); exit 1 }}"
    );
    let output = Command::new(executable)
        .args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-WindowStyle",
            "Hidden",
            "-Command",
            &script,
        ])
        .creation_flags(0x08000000) // CREATE_NO_WINDOW
        .stdin(Stdio::null())
        .output()
        .context("run Windows network configuration command")?;
    // 10 is the private success code for a newly created route.
    if !output.status.success() && output.status.code() != Some(10) {
        bail!(
            "PowerShell failed ({}): {} {}",
            output.status,
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_route() -> UnderlayRoute {
        UnderlayRoute {
            index: 12,
            gateway: "192.168.1.1".parse().unwrap(),
            metric: 30,
        }
    }

    #[test]
    fn selects_default_by_combined_metric_and_excludes_tun() {
        let rows = r#"[
          {"InterfaceIndex":7,"NextHop":"10.0.0.1","RouteMetric":1,"InterfaceMetric":1},
          {"InterfaceIndex":11,"NextHop":"192.168.2.1","RouteMetric":1,"InterfaceMetric":100},
          {"InterfaceIndex":12,"NextHop":"192.168.1.1","RouteMetric":20,"InterfaceMetric":10}
        ]"#;
        assert_eq!(parse_default_route(rows, 7).unwrap(), default_route());
    }

    #[test]
    fn rejects_untrusted_default_route_values() {
        for row in [
            r#"{"InterfaceIndex":-1,"NextHop":"192.168.1.1","RouteMetric":1,"InterfaceMetric":1}"#,
            r#"{"InterfaceIndex":4294967296,"NextHop":"192.168.1.1","RouteMetric":1,"InterfaceMetric":1}"#,
            r#"{"InterfaceIndex":12,"NextHop":"192.168.1.1';bad","RouteMetric":1,"InterfaceMetric":1}"#,
            r#"{"InterfaceIndex":12,"NextHop":"0.0.0.0","RouteMetric":1,"InterfaceMetric":1}"#,
            r#"{"InterfaceIndex":12,"NextHop":"192.168.1.1","RouteMetric":"1;bad","InterfaceMetric":1}"#,
            "[]",
        ] {
            assert!(parse_default_route(row, 7).is_err(), "accepted {row}");
        }
    }

    #[test]
    fn protects_unique_endpoints_before_split_defaults() {
        let endpoint: Ipv4Addr = "203.0.113.9".parse().unwrap();
        let routes = route_plan(7, default_route(), vec![endpoint, endpoint]);
        assert_eq!(routes.len(), 3);
        assert_eq!(
            routes[0],
            Route {
                address: endpoint,
                prefix: 32,
                index: 12,
                gateway: default_route().gateway
            }
        );
        assert_eq!(
            routes[1],
            Route {
                address: Ipv4Addr::UNSPECIFIED,
                prefix: 1,
                index: 7,
                gateway: Ipv4Addr::UNSPECIFIED
            }
        );
        assert_eq!(routes[2].address, Ipv4Addr::new(128, 0, 0, 0));
    }

    #[test]
    fn guard_does_not_claim_preexisting_routes() {
        let routes = route_plan(7, default_route(), vec![]);
        let mut owned = OwnedChanges::default();
        owned.record(routes[0], false);
        owned.record(routes[1], true);
        assert_eq!(owned.routes, vec![routes[1]]);
        assert!(owned.dns_index.is_none());
    }

    #[test]
    fn physical_rank_beats_a_virtual_default_with_lower_metric() {
        let rows = r#"[
          {"InterfaceIndex":11,"NextHop":"10.10.0.1","RouteMetric":1,"InterfaceMetric":1,"PhysicalRank":1},
          {"InterfaceIndex":12,"NextHop":"192.168.1.1","RouteMetric":20,"InterfaceMetric":10,"PhysicalRank":0}
        ]"#;
        assert_eq!(parse_default_route(rows, 0).unwrap(), default_route());
    }

    #[test]
    fn rejects_tun_as_captured_underlay() {
        assert!(validate_underlay(12, default_route()).is_err());
        assert!(validate_underlay(7, default_route()).is_ok());
    }
}
