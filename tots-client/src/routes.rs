#[cfg(any(windows, test))]
use anyhow::Context;
use anyhow::{bail, Result};
use std::net::Ipv4Addr;

/// Owns only routes created by this client. Drop restores routing and TUN DNS.
pub struct FullTunnelRoutes {
    #[cfg(windows)]
    changes: OwnedChanges,
}

pub async fn install(tun_index: i32, protected: Vec<Ipv4Addr>) -> Result<FullTunnelRoutes> {
    #[cfg(windows)]
    {
        // A cancelled caller still lets this task finish; its returned guard is then dropped.
        tokio::task::spawn_blocking(move || install_sync(tun_index, protected))
            .await
            .context("Windows route installation task failed")?
    }
    #[cfg(not(windows))]
    {
        let _ = (tun_index, protected);
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
struct DefaultRoute {
    index: u32,
    gateway: Ipv4Addr,
    metric: u64,
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
fn parse_default_route(json: &str, tun_index: u32) -> Result<DefaultRoute> {
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
        let metric = u64::from(number("RouteMetric")?) + u64::from(number("InterfaceMetric")?);
        if index != tun_index {
            candidates.push(DefaultRoute {
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
fn route_plan(tun_index: u32, default: DefaultRoute, mut protected: Vec<Ipv4Addr>) -> Vec<Route> {
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
fn install_sync(tun_index: i32, protected: Vec<Ipv4Addr>) -> Result<FullTunnelRoutes> {
    if tun_index <= 0 {
        bail!("invalid TUN interface index");
    }
    if protected.is_empty() {
        bail!("full tunnel requires at least one protected transport endpoint");
    }
    if protected
        .iter()
        .any(|ip| ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() || ip.is_broadcast())
    {
        bail!("protected transport endpoints must be usable IPv4 addresses");
    }
    let json = powershell(r#"
        $physical = @(Get-NetAdapter -Physical -ErrorAction Stop | Where-Object { $_.Status -eq 'Up' } | ForEach-Object { $_.ifIndex })
        $rows = @(Get-NetRoute -AddressFamily IPv4 -DestinationPrefix '0.0.0.0/0' -PolicyStore ActiveStore -ErrorAction Stop | ForEach-Object {
            $route = $_
            if ($physical -contains $route.InterfaceIndex) {
                $ip = Get-NetIPInterface -AddressFamily IPv4 -InterfaceIndex $route.InterfaceIndex -ErrorAction Stop | Where-Object { $_.ConnectionState -eq 'Connected' } | Select-Object -First 1
                if ($null -ne $ip) {
                    [PSCustomObject]@{ InterfaceIndex = [uint32]$route.InterfaceIndex; NextHop = [string]$route.NextHop; RouteMetric = [uint32]$route.RouteMetric; InterfaceMetric = [uint32]$ip.InterfaceMetric }
                }
            }
        })
        ConvertTo-Json -InputObject $rows -Compress
    "#).context("read original physical IPv4 default route")?;
    let json = String::from_utf8(json.stdout).context("default-route JSON was not UTF-8")?;
    let default = parse_default_route(&json, tun_index as u32)?;
    let mut guard = FullTunnelRoutes {
        changes: OwnedChanges::default(),
    };
    let result = (|| -> Result<()> {
        for route in route_plan(tun_index as u32, default, protected) {
            let output = powershell(&format!(
                "$existing = @({}); if ($existing.Count -eq 0) {{ New-NetRoute -AddressFamily IPv4 -DestinationPrefix '{}' -InterfaceIndex {} -NextHop '{}' -RouteMetric 1 -PolicyStore ActiveStore -ErrorAction Stop | Out-Null; exit 10 }}",
                route.query(), route.destination(), route.index, route.gateway
            )).with_context(|| format!("install IPv4 route {}", route.destination()))?;
            // An exit code cannot be confused by localized warnings on stdout.
            guard
                .changes
                .record(route, output.status.code() == Some(10));
        }
        // Mark before the call so a partially successful DNS update is also reset.
        guard.changes.dns_index = Some(tun_index as u32);
        powershell(&format!(
            "Set-DnsClientServerAddress -InterfaceIndex {tun_index} -ServerAddresses @('1.1.1.1','8.8.8.8') -ErrorAction Stop"
        )).context("set TUN DNS servers")?;
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

    fn default_route() -> DefaultRoute {
        DefaultRoute {
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
}
