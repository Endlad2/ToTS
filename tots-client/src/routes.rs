#[cfg(any(windows, test))]
use anyhow::Context;
use anyhow::{bail, Result};
use std::net::Ipv4Addr;

/// Owns only routes created by this client. Drop restores routing and TUN DNS.
pub struct FullTunnelRoutes {
    #[cfg(windows)]
    changes: OwnedChanges,
    #[cfg(windows)]
    underlay: UnderlayRoute,
    #[cfg(windows)]
    protected: std::collections::BTreeSet<Ipv4Addr>,
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
    /// Add escape routes for provider endpoints discovered after the first
    /// transport became healthy. This lets slow/fallback providers reconnect
    /// directly after the system default route has moved to Wintun.
    pub async fn refresh_protected(&mut self, endpoints: Vec<Ipv4Addr>) -> Result<()> {
        #[cfg(windows)]
        {
            let candidates: Vec<Ipv4Addr> = endpoints
                .into_iter()
                .filter(|ip| {
                    !ip.is_unspecified()
                        && !ip.is_loopback()
                        && !ip.is_multicast()
                        && !ip.is_broadcast()
                        && !self.protected.contains(ip)
                })
                .collect();
            if candidates.is_empty() {
                return Ok(());
            }
            let underlay = self.underlay;
            let (created, installed, failures) = tokio::task::spawn_blocking(move || {
                let mut created = Vec::new();
                let mut installed = Vec::new();
                let mut failures = Vec::new();
                for address in candidates {
                    let route = Route {
                        address,
                        prefix: 32,
                        index: underlay.index,
                        gateway: underlay.gateway,
                    };
                    let script = format!(
                        "$existing = @({}); if ($existing.Count -eq 0) {{ New-NetRoute -AddressFamily IPv4 -DestinationPrefix '{}' -InterfaceIndex {} -NextHop '{}' -RouteMetric 1 -PolicyStore ActiveStore -ErrorAction Stop | Out-Null; exit 10 }}",
                        route.query(), route.destination(), route.index, route.gateway
                    );
                    match powershell(&script) {
                        Ok(output) => {
                            if output.status.code() == Some(10) {
                                created.push(route);
                            }
                            installed.push(address);
                        }
                        Err(error) => failures.push(format!("{address}: {error:#}")),
                    }
                }
                (created, installed, failures)
            })
            .await
            .context("Windows dynamic endpoint-route task failed")?;
            self.changes.routes.extend(created);
            self.protected.extend(installed);
            for failure in failures {
                tracing::warn!(error = %failure, "could not add a Windows provider escape route; will retry");
            }
        }
        #[cfg(not(windows))]
        {
            let _ = endpoints;
        }
        Ok(())
    }

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
                "$existing = @({}); if ($existing.Count -gt 0) {{ $existing | Remove-NetRoute -Confirm:$false -ErrorAction Stop | Out-Null }}",
                route.query()
            )) {
                Ok(_) => {
                    self.changes.routes.remove(i);
                }
                Err(error) => failures.push(format!("{}: {error:#}", route.destination())),
            }
        }
        for i in (0..self.changes.dns.len()).rev() {
            let snapshot = self.changes.dns[i].clone();
            let restore = restore_dns_script(&snapshot);
            match powershell(&restore) {
                Ok(_) => {
                    self.changes.dns.remove(i);
                }
                Err(error) => failures.push(format!("restore DNS on interface {}: {error:#}", snapshot.index)),
            }
        }
        if let Some(rule) = self.changes.ipv6_rule.clone() {
            let script = format!(
                "$existing = Get-NetFirewallRule -Name '{}' -ErrorAction SilentlyContinue; if ($null -ne $existing) {{ Remove-NetFirewallRule -InputObject $existing -Confirm:$false -ErrorAction Stop | Out-Null }}",
                rule
            );
            match powershell(&script) {
                Ok(_) => self.changes.ipv6_rule = None,
                Err(error) => failures.push(format!("remove IPv6 leak-protection rule: {error:#}")),
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
    // Two /1 routes win over every ordinary IPv4 default route. Keep a /0 route
    // as well so Windows NCSI and applications that explicitly inspect the default
    // route see the Wintun adapter as the active IPv4 egress.
    for (address, prefix) in [
        (Ipv4Addr::UNSPECIFIED, 1),
        (Ipv4Addr::new(128, 0, 0, 0), 1),
        (Ipv4Addr::UNSPECIFIED, 0),
    ] {
        routes.push(Route {
            address,
            prefix,
            index: tun_index,
            gateway: Ipv4Addr::UNSPECIFIED,
        });
    }
    routes
}

#[cfg(windows)]
fn install_route_plan(plan: &[Route]) -> Result<Vec<Route>> {
    if plan.is_empty() {
        bail!("Windows route plan is empty");
    }
    let rows = plan
        .iter()
        .map(|route| {
            format!(
                "[PSCustomObject]@{{Address='{}'; Prefix={}; InterfaceIndex={}; NextHop='{}'}}",
                route.address, route.prefix, route.index, route.gateway
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let script = format!(
        r#"
$plan = @({rows})
$created = @()
try {{
    $active = @(Get-NetRoute -AddressFamily IPv4 -PolicyStore ActiveStore -ErrorAction Stop)
    for ($i = 0; $i -lt $plan.Count; $i++) {{
        $route = $plan[$i]
        $prefix = [string]$route.Address + '/' + [string]$route.Prefix
        $existing = @($active | Where-Object {{
            $_.InterfaceIndex -eq $route.InterfaceIndex -and
            $_.DestinationPrefix -eq $prefix -and
            $_.NextHop -eq $route.NextHop
        }})
        if ($existing.Count -eq 0) {{
            New-NetRoute -AddressFamily IPv4 -DestinationPrefix $prefix -InterfaceIndex $route.InterfaceIndex -NextHop $route.NextHop -RouteMetric 1 -PolicyStore ActiveStore -ErrorAction Stop | Out-Null
            $created += $i
        }}
    }}
    [Console]::Out.WriteLine(($created -join ','))
}} catch {{
    for ($j = $created.Count - 1; $j -ge 0; $j--) {{
        $route = $plan[$created[$j]]
        $prefix = [string]$route.Address + '/' + [string]$route.Prefix
        Get-NetRoute -AddressFamily IPv4 -PolicyStore ActiveStore -ErrorAction SilentlyContinue |
            Where-Object {{ $_.InterfaceIndex -eq $route.InterfaceIndex -and $_.DestinationPrefix -eq $prefix -and $_.NextHop -eq $route.NextHop }} |
            Remove-NetRoute -Confirm:$false -ErrorAction SilentlyContinue
    }}
    throw
}}
"#
    );
    let output = powershell(&script).context("install Windows IPv4 route plan")?;
    let raw = String::from_utf8(output.stdout).context("route plan response was not UTF-8")?;
    let created: Vec<usize> = if raw.trim().is_empty() {
        Vec::new()
    } else {
        raw.trim()
            .split(',')
            .map(|value| value.parse::<usize>().context("invalid created route index"))
            .collect::<Result<Vec<_>>>()?
    };
    let mut routes = Vec::with_capacity(created.len());
    for index in created {
        let route = plan
            .get(index)
            .copied()
            .context("PowerShell returned an invalid created-route index")?;
        routes.push(route);
    }
    Ok(routes)
}

#[cfg(any(windows, test))]
#[derive(Clone, Debug)]
struct DnsSnapshot {
    index: u32,
    servers: Vec<Ipv4Addr>,
}

#[cfg(any(windows, test))]
#[derive(Default)]
struct OwnedChanges {
    routes: Vec<Route>,
    dns: Vec<DnsSnapshot>,
    ipv6_rule: Option<String>,
}

#[cfg(any(windows, test))]
impl OwnedChanges {
    fn record(&mut self, route: Route, created: bool) {
        if created {
            self.routes.push(route);
        }
    }

    fn record_dns(&mut self, snapshot: DnsSnapshot) {
        if !self.dns.iter().any(|existing| existing.index == snapshot.index) {
            self.dns.push(snapshot);
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

/// Build the Windows Defender Firewall rule used to prevent IPv6 Internet
/// traffic from bypassing this IPv4-only tunnel. NetSecurity's address filter
/// accepts the `Internet6` keyword; `::/0` is not accepted consistently by
/// Windows Firewall's address-prefix parser and fails on some Windows builds.
#[cfg(any(windows, test))]
fn ipv6_block_rule_script(rule: &str) -> String {
    // The rule name is generated locally from the process id and contains only
    // ASCII letters, digits and hyphens. Keep the PowerShell body raw so its
    // braces are not interpreted as Rust format placeholders.
    let template = r#"
$service = Get-Service -Name MpsSvc -ErrorAction Stop
if ($service.Status -ne 'Running') {
    throw 'Windows Defender Firewall service is not running; refusing an IPv6-leaking full tunnel'
}

$activeProfiles = @(Get-NetConnectionProfile -ErrorAction SilentlyContinue | ForEach-Object {
    switch ([string]$_.NetworkCategory) {
        'DomainAuthenticated' { 'Domain' }
        'Private' { 'Private' }
        default { 'Public' }
    }
} | Select-Object -Unique)
if ($activeProfiles.Count -eq 0) { $activeProfiles = @('Public') }
$disabled = @(Get-NetFirewallProfile -PolicyStore ActiveStore | Where-Object {
    $activeProfiles -contains $_.Name -and $_.Enabled -ne $true
})
if ($disabled.Count -gt 0) {
    throw 'Windows Firewall is disabled for an active network profile; refusing an IPv6-leaking full tunnel'
}
$stale = Get-NetFirewallRule -Name '__TOTS_RULE__' -ErrorAction SilentlyContinue
if ($null -ne $stale) {
    Remove-NetFirewallRule -InputObject $stale -Confirm:$false -ErrorAction Stop | Out-Null
}
New-NetFirewallRule -Name '__TOTS_RULE__' -DisplayName 'ToTS IPv6 leak protection' -Group 'ToTS Full Tunnel' -Description 'Temporary IPv6 Internet egress block while the IPv4-only ToTS tunnel is active' -Direction Outbound -Action Block -RemoteAddress 'Internet6' -Profile Any -Enabled True -PolicyStore PersistentStore -ErrorAction Stop | Out-Null
$created = Get-NetFirewallRule -Name '__TOTS_RULE__' -ErrorAction Stop
$addressFilter = $created | Get-NetFirewallAddressFilter -ErrorAction Stop
if (@($addressFilter.RemoteAddress) -notcontains 'Internet6') {
    throw 'ToTS IPv6 leak-protection rule was created without the Internet6 address filter'
}
"#;
    template.replace("__TOTS_RULE__", rule)
}

// These helpers must be Rust items, not part of the PowerShell raw string above.
// Set-DnsClientServerAddress infers IPv4 from ServerAddresses and does not accept
// the -AddressFamily switch supported by Get-DnsClientServerAddress.
#[cfg(any(windows, test))]
fn restore_dns_script(snapshot: &DnsSnapshot) -> String {
    if snapshot.servers.is_empty() {
        format!(
            "Set-DnsClientServerAddress -InterfaceIndex {} -ResetServerAddresses -ErrorAction Stop",
            snapshot.index
        )
    } else {
        let servers = snapshot
            .servers
            .iter()
            .map(|ip| format!("'{}'", ip))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "Set-DnsClientServerAddress -InterfaceIndex {} -ServerAddresses @({servers}) -ErrorAction Stop",
            snapshot.index
        )
    }
}

#[cfg(any(windows, test))]
fn set_ipv4_dns_script(indices: &[u32]) -> String {
    indices
        .iter()
        .map(|index| {
            format!(
                "Set-DnsClientServerAddress -InterfaceIndex {index} -ServerAddresses @('1.1.1.1','8.8.8.8') -ErrorAction Stop"
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
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
        underlay,
        protected: protected.iter().copied().collect(),
    };
    let result = (|| -> Result<()> {
        // ToTS carries IPv4 only. Block IPv6 egress while the tunnel is up
        // rather than silently letting IPv6-capable applications bypass Wintun.
        let rule = format!("ToTS-IPv6-Block-{}", std::process::id());
        guard.changes.ipv6_rule = Some(rule.clone());
        powershell(&ipv6_block_rule_script(&rule))
            .context("enable temporary IPv6 leak protection")?;

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
        let created_routes = install_route_plan(&plan)?;
        for route in created_routes {
            guard.changes.record(route, true);
        }

        // Verify the active route table instead of treating a successful PowerShell
        // process exit as proof that Windows accepted the full-tunnel routes.
        powershell(&format!(
            "Set-NetIPInterface -InterfaceIndex {tun_index} -AddressFamily IPv4 -AutomaticMetric Disabled -InterfaceMetric 1 -ErrorAction Stop; $required = @('0.0.0.0/1','128.0.0.0/1','0.0.0.0/0'); foreach ($prefix in $required) {{ $found = @(Get-NetRoute -AddressFamily IPv4 -DestinationPrefix $prefix -InterfaceIndex {tun_index} -PolicyStore ActiveStore -ErrorAction SilentlyContinue | Where-Object {{ $_.NextHop -eq '0.0.0.0' }}); if ($found.Count -eq 0) {{ throw ('missing ToTS route ' + $prefix + ' on interface {tun_index}') }} }}; $adapter = Get-NetAdapter -InterfaceIndex {tun_index} -IncludeHidden -ErrorAction Stop | Select-Object -First 1; if ($adapter.Status -ne 'Up') {{ throw 'ToTS Wintun adapter is not Up' }}"
        ))
        .context("verify Windows full-tunnel routes")?;

        // Snapshot and replace DNS on both the physical underlay and Wintun. This
        // prevents Windows' multi-homed DNS client from continuing to query the
        // router/ISP DNS server on the physical adapter.
        let dns_indices = [underlay.index, tun_index as u32];
        for index in dns_indices {
            let snapshot = snapshot_dns(index)?;
            guard.changes.record_dns(snapshot);
        }
        // One PowerShell invocation for both adapters. Set-DnsClientServerAddress
        // infers the family from the supplied IPv4 addresses and has no
        // -AddressFamily parameter (unlike Get-DnsClientServerAddress).
        powershell(&set_ipv4_dns_script(&dns_indices))
        .context("set IPv4 DNS servers on physical and Wintun interfaces")?;
        tracing::info!(
            tun_interface_index = tun_index,
            physical_interface_index = underlay.index,
            ipv6_egress_blocked = true,
            "Windows full-tunnel routes, DNS and IPv6 leak protection are active"
        );
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
fn snapshot_dns(index: u32) -> Result<DnsSnapshot> {
    let output = powershell(&format!(
        "$servers = @(Get-DnsClientServerAddress -InterfaceIndex {index} -AddressFamily IPv4 -ErrorAction Stop | ForEach-Object {{ $_.ServerAddresses }} | Where-Object {{ $_ }}); ConvertTo-Json -InputObject $servers -Compress"
    ))
    .with_context(|| format!("snapshot IPv4 DNS servers on interface {index}"))?;
    let raw = String::from_utf8(output.stdout).context("DNS snapshot was not UTF-8")?;
    let value: serde_json::Value = serde_json::from_str(raw.trim())
        .context("parse IPv4 DNS snapshot")?;
    let entries: Vec<&serde_json::Value> = match &value {
        serde_json::Value::Array(values) => values.iter().collect(),
        serde_json::Value::String(_) => vec![&value],
        serde_json::Value::Null => Vec::new(),
        _ => bail!("unexpected IPv4 DNS snapshot format"),
    };
    let mut servers = Vec::with_capacity(entries.len());
    for entry in entries {
        let raw = entry.as_str().context("DNS snapshot entry is not a string")?;
        let ip = raw.parse::<Ipv4Addr>().context("invalid IPv4 DNS server in snapshot")?;
        if !servers.contains(&ip) {
            servers.push(ip);
        }
    }
    Ok(DnsSnapshot { index, servers })
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
    fn ipv6_firewall_rule_uses_supported_internet6_keyword() {
        let script = ipv6_block_rule_script("ToTS-IPv6-Block-1234");
        assert!(script.contains("-RemoteAddress 'Internet6'"));
        assert!(script.contains("-Action Block"));
        assert!(script.contains("ToTS-IPv6-Block-1234"));
        assert!(!script.contains("::/0"));
    }

    #[test]
    fn dns_set_and_restore_commands_do_not_use_unsupported_address_family_parameter() {
        let set_dns = set_ipv4_dns_script(&[7, 12]);
        let reset_dns = restore_dns_script(&DnsSnapshot {
            index: 7,
            servers: Vec::new(),
        });
        let restore_dns = restore_dns_script(&DnsSnapshot {
            index: 12,
            servers: vec!["192.168.1.1".parse().unwrap()],
        });
        assert!(set_dns.contains("-InterfaceIndex 7"));
        assert!(set_dns.contains("-InterfaceIndex 12"));
        assert!(!set_dns.contains("-AddressFamily"));
        assert!(!reset_dns.contains("-AddressFamily"));
        assert!(!restore_dns.contains("-AddressFamily"));
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
        assert_eq!(routes.len(), 4);
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
        assert_eq!(routes[2].prefix, 1);
        assert_eq!(routes[3].address, Ipv4Addr::UNSPECIFIED);
        assert_eq!(routes[3].prefix, 0);
    }

    #[test]
    fn full_tunnel_route_plan_covers_all_ipv4_destinations_and_default() {
        let routes = route_plan(7, default_route(), vec!["203.0.113.8".parse().unwrap()]);
        assert!(routes.iter().any(|route| route.prefix == 32 && route.index == 12));
        assert!(routes.iter().any(|route| route.address == Ipv4Addr::UNSPECIFIED && route.prefix == 1 && route.index == 7));
        assert!(routes.iter().any(|route| route.address == Ipv4Addr::new(128, 0, 0, 0) && route.prefix == 1 && route.index == 7));
        assert!(routes.iter().any(|route| route.address == Ipv4Addr::UNSPECIFIED && route.prefix == 0 && route.index == 7));
    }

    #[test]
    fn owned_dns_snapshot_is_deduplicated_by_interface() {
        let mut owned = OwnedChanges::default();
        owned.record_dns(DnsSnapshot {
            index: 12,
            servers: vec!["1.1.1.1".parse().unwrap()],
        });
        owned.record_dns(DnsSnapshot {
            index: 12,
            servers: vec!["8.8.8.8".parse().unwrap()],
        });
        assert_eq!(owned.dns.len(), 1);
        assert_eq!(owned.dns[0].servers, vec!["1.1.1.1".parse::<Ipv4Addr>().unwrap()]);
    }

    #[test]
    fn guard_does_not_claim_preexisting_routes() {
        let routes = route_plan(7, default_route(), vec![]);
        let mut owned = OwnedChanges::default();
        owned.record(routes[0], false);
        owned.record(routes[1], true);
        assert_eq!(owned.routes, vec![routes[1]]);
        assert!(owned.dns.is_empty());
        assert!(owned.ipv6_rule.is_none());
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
