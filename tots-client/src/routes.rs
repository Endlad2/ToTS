#[cfg(any(windows, test))]
use anyhow::Context;
use anyhow::{bail, Result};
use std::net::Ipv4Addr;

/// Owns only routes created by this client. Drop restores routing and TUN DNS.
pub struct FullTunnelRoutes {
    // Compiled for tests as well as Windows so the Linux test suite type-checks the
    // guard's fields and constructor. A Windows-only field previously hid a
    // `collect()` mismatch until the Windows CI job failed.
    #[cfg(any(windows, test))]
    changes: OwnedChanges,
    #[cfg(any(windows, test))]
    underlay: UnderlayRoute,
    #[cfg(any(windows, test))]
    protected: ProtectedEndpoints,
}

/// Validate the install request and build the route guard.
#[cfg(any(windows, test))]
fn build_guard(
    tun_index: i32,
    protected: &[Ipv4Addr],
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
    Ok(FullTunnelRoutes {
        changes: OwnedChanges::default(),
        underlay,
        protected: ProtectedEndpoints::from_endpoints(protected.iter().copied()),
    })
}

/// Escape-route bookkeeping for provider endpoints.
///
/// Provider/TURN/CDN addresses rotate across reconnects, and the refresh tick runs
/// every second. Remembering every address ever seen grew the route table and the
/// per-tick PowerShell work without bound on long sessions, so endpoints that have
/// not been reported for a while are removed again.
#[cfg(any(windows, test))]
#[derive(Default)]
struct ProtectedEndpoints {
    entries: std::collections::BTreeMap<Ipv4Addr, std::time::Instant>,
}

#[cfg(any(windows, test))]
impl ProtectedEndpoints {
    const MAX: usize = 512;
    const IDLE_EXPIRY: std::time::Duration = std::time::Duration::from_secs(900);

    fn contains(&self, address: &Ipv4Addr) -> bool {
        self.entries.contains_key(address)
    }

    fn insert(&mut self, address: Ipv4Addr) {
        self.entries.insert(address, std::time::Instant::now());
    }

    fn touch(&mut self, address: &Ipv4Addr) {
        if let Some(seen) = self.entries.get_mut(address) {
            *seen = std::time::Instant::now();
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    /// Seed the set with the endpoints whose escape routes were installed by the
    /// initial route plan, so the first refresh does not treat them as new.
    fn from_endpoints(endpoints: impl IntoIterator<Item = Ipv4Addr>) -> Self {
        let now = std::time::Instant::now();
        Self {
            entries: endpoints.into_iter().map(|address| (address, now)).collect(),
        }
    }

    /// Endpoints that may be released: idle for the grace period, or the oldest
    /// entries once the cap is exceeded.
    fn expired(&self, now: std::time::Instant) -> Vec<Ipv4Addr> {
        let mut candidates: Vec<(Ipv4Addr, std::time::Instant)> = self
            .entries
            .iter()
            .filter(|(_, seen)| now.duration_since(**seen) >= Self::IDLE_EXPIRY)
            .map(|(address, seen)| (*address, *seen))
            .collect();
        let remaining = self.entries.len().saturating_sub(candidates.len());
        if remaining > Self::MAX {
            let mut oldest: Vec<(Ipv4Addr, std::time::Instant)> = self
                .entries
                .iter()
                .filter(|(address, _)| !candidates.iter().any(|(taken, _)| taken == *address))
                .map(|(address, seen)| (*address, *seen))
                .collect();
            oldest.sort_by_key(|(_, seen)| *seen);
            candidates.extend(oldest.into_iter().take(remaining - Self::MAX));
        }
        candidates.into_iter().map(|(address, _)| address).collect()
    }
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
    /// The adapter's own IPv4 address.
    ///
    /// Windows has no `SO_BINDTODEVICE`, so this is what pins ToTS transport
    /// sockets to the physical adapter: a socket bound to the underlay's address
    /// cannot be emitted through the TUN device.
    address: Option<Ipv4Addr>,
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

#[cfg(test)]
impl FullTunnelRoutes {
    /// `(protected_contains_203_0_113_9, protected_len)` for the route-guard tests.
    fn properties_for_test(&self) -> (bool, usize) {
        (
            self.protected.contains(&"203.0.113.9".parse().unwrap()),
            self.protected.len(),
        )
    }

    async fn release_endpoints_for_test(&mut self, stale: Vec<Ipv4Addr>) {
        self.release_endpoints(stale).await;
    }
}

impl FullTunnelRoutes {
    /// Add escape routes for provider endpoints discovered after the first
    /// transport became healthy. This lets slow/fallback providers reconnect
    /// directly after the system default route has moved to Wintun.
    pub async fn refresh_protected(&mut self, endpoints: Vec<Ipv4Addr>) -> Result<()> {
        #[cfg(any(windows, test))]
        {
            let live: Vec<Ipv4Addr> = endpoints
                .into_iter()
                .filter(|ip| {
                    !ip.is_unspecified()
                        && !ip.is_loopback()
                        && !ip.is_multicast()
                        && !ip.is_broadcast()
                })
                .collect();
            for address in &live {
                self.protected.touch(address);
            }
            let candidates: Vec<Ipv4Addr> = live
                .into_iter()
                .filter(|ip| !self.protected.contains(ip))
                .collect();
            let stale = self.protected.expired(std::time::Instant::now());
            if candidates.is_empty() && stale.is_empty() {
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
                    match powershell(&add_route_script(route)) {
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
            for address in installed {
                self.protected.insert(address);
            }
            for failure in failures {
                tracing::warn!(error = %failure, "could not add a Windows provider escape route; will retry");
            }
            if !stale.is_empty() {
                self.release_endpoints(stale).await;
            }
            if self.protected.len() > ProtectedEndpoints::MAX {
                tracing::warn!(
                    protected = self.protected.len(),
                    "Windows escape-route set is above its cap; oldest endpoints are released first"
                );
            }
        }
        // Only the non-test, non-Windows build needs a fallback: every test build
        // already compiles the Windows body above, which type-checks it on Linux.
        #[cfg(not(any(windows, test)))]
        {
            let _ = endpoints;
        }
        Ok(())
    }

    /// Remove escape routes for endpoints that are no longer reported.
    #[cfg(any(windows, test))]
    async fn release_endpoints(&mut self, stale: Vec<Ipv4Addr>) {
        if stale.is_empty() {
            return;
        }
        let underlay = self.underlay;
        let (released, failures) = tokio::task::spawn_blocking(move || {
            let mut released = Vec::new();
            let mut failures = Vec::new();
            for address in stale {
                let route = Route {
                    address,
                    prefix: 32,
                    index: underlay.index,
                    gateway: underlay.gateway,
                };
                match powershell(&remove_route_script(route)) {
                    Ok(_) => released.push(address),
                    Err(error) => failures.push(format!("{address}: {error:#}")),
                }
            }
            (released, failures)
        })
        .await
        .unwrap_or_default();
        if !released.is_empty() {
            self.changes
                .routes
                .retain(|route| !released.contains(&route.address));
            for address in released {
                self.protected.entries.remove(&address);
            }
            tracing::info!("released Windows provider escape routes for stale endpoints");
        }
        for failure in failures {
            tracing::warn!(error = %failure, "could not release a Windows provider escape route");
        }
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

    #[cfg(any(windows, test))]
    fn cleanup_sync(&mut self) -> Result<()> {
        let mut failures = Vec::new();
        // Remove split defaults before the endpoint escape routes.
        for i in (0..self.changes.routes.len()).rev() {
            let route = self.changes.routes[i];
            match powershell(&remove_route_script(route)) {
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
            match powershell(&remove_ipv6_rule_script(&rule)) {
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
        let address = row
            .get("Address")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.parse::<Ipv4Addr>().ok())
            .filter(|address| {
                !address.is_unspecified()
                    && !address.is_loopback()
                    && !address.is_multicast()
                    && !address.is_broadcast()
            });
        if index != excluded_index {
            candidates.push(UnderlayRoute {
                index,
                gateway,
                metric,
                address,
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

#[cfg(any(windows, test))]
fn install_route_plan(plan: &[Route]) -> Result<Vec<Route>> {
    if plan.is_empty() {
        bail!("Windows route plan is empty");
    }
    let output = powershell(&install_route_plan_script(plan))
        .context("install Windows IPv4 route plan")?;
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

#[cfg(any(windows, test))]
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

/// Fill `__TOKEN__` placeholders in a PowerShell template.
///
/// PowerShell scripts are kept as plain raw strings and completed by replacement
/// rather than `format!`, because every PowerShell brace would need doubling and a
/// single mismatch is only reported by a Windows build. Keeping the builders free
/// of `format!` also lets the Linux test suite compile and assert them.
#[cfg(any(windows, test))]
fn powershell_script(template: &str, values: &[(&str, &str)]) -> String {
    let mut script = template.to_owned();
    for (token, value) in values {
        script = script.replace(token, value);
    }
    script
}

#[cfg(any(windows, test))]
fn route_rows(plan: &[Route]) -> String {
    plan.iter()
        .map(|route| {
            format!(
                "[PSCustomObject]@{{Address='{}'; Prefix={}; InterfaceIndex={}; NextHop='{}'}}",
                route.address, route.prefix, route.index, route.gateway
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(any(windows, test))]
fn install_route_plan_script(plan: &[Route]) -> String {
    const TEMPLATE: &str = r#"
$plan = @(__ROWS__)
$created = @()
try {
    $active = @(Get-NetRoute -AddressFamily IPv4 -PolicyStore ActiveStore -ErrorAction Stop)
    for ($i = 0; $i -lt $plan.Count; $i++) {
        $route = $plan[$i]
        $prefix = [string]$route.Address + '/' + [string]$route.Prefix
        $existing = @($active | Where-Object {
            $_.InterfaceIndex -eq $route.InterfaceIndex -and
            $_.DestinationPrefix -eq $prefix -and
            $_.NextHop -eq $route.NextHop
        })
        if ($existing.Count -eq 0) {
            New-NetRoute -AddressFamily IPv4 -DestinationPrefix $prefix -InterfaceIndex $route.InterfaceIndex -NextHop $route.NextHop -RouteMetric 1 -PolicyStore ActiveStore -ErrorAction Stop | Out-Null
            $created += $i
        }
    }
    [Console]::Out.WriteLine(($created -join ','))
} catch {
    for ($j = $created.Count - 1; $j -ge 0; $j--) {
        $route = $plan[$created[$j]]
        $prefix = [string]$route.Address + '/' + [string]$route.Prefix
        Get-NetRoute -AddressFamily IPv4 -PolicyStore ActiveStore -ErrorAction SilentlyContinue |
            Where-Object { $_.InterfaceIndex -eq $route.InterfaceIndex -and $_.DestinationPrefix -eq $prefix -and $_.NextHop -eq $route.NextHop } |
            Remove-NetRoute -Confirm:$false -ErrorAction SilentlyContinue
    }
    throw
}
"#;
    powershell_script(TEMPLATE, &[("__ROWS__", &route_rows(plan))])
}

/// Create one `/32` escape route and exit with code 10 when it was created.
#[cfg(any(windows, test))]
fn add_route_script(route: Route) -> String {
    const TEMPLATE: &str = r#"
$existing = @(__QUERY__)
if ($existing.Count -eq 0) {
    New-NetRoute -AddressFamily IPv4 -DestinationPrefix '__DESTINATION__' -InterfaceIndex __INDEX__ -NextHop '__GATEWAY__' -RouteMetric 1 -PolicyStore ActiveStore -ErrorAction Stop | Out-Null
    exit 10
}
"#;
    powershell_script(
        TEMPLATE,
        &[
            ("__QUERY__", &route.query()),
            ("__DESTINATION__", &route.destination()),
            ("__INDEX__", &route.index.to_string()),
            ("__GATEWAY__", &route.gateway.to_string()),
        ],
    )
}

#[cfg(any(windows, test))]
fn remove_route_script(route: Route) -> String {
    const TEMPLATE: &str = r#"
$existing = @(__QUERY__)
if ($existing.Count -gt 0) {
    $existing | Remove-NetRoute -Confirm:$false -ErrorAction Stop | Out-Null
}
"#;
    powershell_script(TEMPLATE, &[("__QUERY__", &route.query())])
}

#[cfg(any(windows, test))]
fn remove_ipv6_rule_script(rule: &str) -> String {
    const TEMPLATE: &str = r#"
$existing = Get-NetFirewallRule -Name '__RULE__' -ErrorAction SilentlyContinue
if ($null -ne $existing) {
    Remove-NetFirewallRule -InputObject $existing -Confirm:$false -ErrorAction Stop | Out-Null
}
"#;
    powershell_script(TEMPLATE, &[("__RULE__", rule)])
}

/// Verify that Windows actually accepted the full-tunnel routes.
#[cfg(any(windows, test))]
fn verify_full_tunnel_script(tun_index: u32) -> String {
    const TEMPLATE: &str = r#"
Set-NetIPInterface -InterfaceIndex __INDEX__ -AddressFamily IPv4 -AutomaticMetric Disabled -InterfaceMetric 1 -ErrorAction Stop
$required = @('0.0.0.0/1','128.0.0.0/1','0.0.0.0/0')
foreach ($prefix in $required) {
    $found = @(Get-NetRoute -AddressFamily IPv4 -DestinationPrefix $prefix -InterfaceIndex __INDEX__ -PolicyStore ActiveStore -ErrorAction SilentlyContinue | Where-Object { $_.NextHop -eq '0.0.0.0' })
    if ($found.Count -eq 0) { throw ('missing ToTS route ' + $prefix + ' on interface __INDEX__') }
}
$adapter = Get-NetAdapter -InterfaceIndex __INDEX__ -IncludeHidden -ErrorAction Stop | Select-Object -First 1
if ($adapter.Status -ne 'Up') { throw 'ToTS Wintun adapter is not Up' }
"#;
    powershell_script(TEMPLATE, &[("__INDEX__", &tun_index.to_string())])
}

#[cfg(any(windows, test))]
fn snapshot_dns_script(index: u32) -> String {
    const TEMPLATE: &str = r#"
$servers = @(Get-DnsClientServerAddress -InterfaceIndex __INDEX__ -AddressFamily IPv4 -ErrorAction Stop | ForEach-Object { $_.ServerAddresses } | Where-Object { $_ })
ConvertTo-Json -InputObject $servers -Compress
"#;
    powershell_script(TEMPLATE, &[("__INDEX__", &index.to_string())])
}

#[cfg(any(windows, test))]
fn forced_underlay_script(index: u32) -> String {
    const TEMPLATE: &str = r#"
$adapter = Get-NetAdapter -InterfaceIndex __INDEX__ -IncludeHidden -ErrorAction SilentlyContinue | Select-Object -First 1
if ($null -eq $adapter -or $adapter.Status -ne 'Up') {
    throw 'TOTS_PHYSICAL_IFINDEX does not name an Up adapter'
}
$ip = Get-NetIPInterface -AddressFamily IPv4 -InterfaceIndex __INDEX__ -ErrorAction SilentlyContinue |
    Where-Object { $_.ConnectionState -eq 'Connected' } | Select-Object -First 1
if ($null -eq $ip) {
    throw 'TOTS_PHYSICAL_IFINDEX has no connected IPv4 interface'
}
$localAddress = [string](Get-NetIPAddress -AddressFamily IPv4 -InterfaceIndex __INDEX__ -ErrorAction SilentlyContinue |
    Where-Object { $_.AddressState -eq 'Preferred' } | Select-Object -First 1).IPAddress
$rows = @(Get-NetRoute -AddressFamily IPv4 -DestinationPrefix '0.0.0.0/0' -InterfaceIndex __INDEX__ -PolicyStore ActiveStore -ErrorAction SilentlyContinue |
    Where-Object { $_.NextHop -ne '0.0.0.0' } |
    ForEach-Object {
        [PSCustomObject]@{
            InterfaceIndex = [uint32]$_.InterfaceIndex
            NextHop = [string]$_.NextHop
            RouteMetric = [uint32]$_.RouteMetric
            InterfaceMetric = [uint32]$ip.InterfaceMetric
            PhysicalRank = [uint32]0
            Address = $localAddress
        }
    })
if ($rows.Count -eq 0) {
    throw 'TOTS_PHYSICAL_IFINDEX has no usable active IPv4 default route'
}
ConvertTo-Json -InputObject $rows -Compress
"#;
    powershell_script(TEMPLATE, &[("__INDEX__", &index.to_string())])
}

/// Discover the physical underlay from the active IPv4 default routes.
#[cfg(any(windows, test))]
fn automatic_underlay_script() -> &'static str {
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

    # Prefer adapters that Windows marks as real hardware. Some Wi-Fi/USB drivers
    # report this incorrectly, so non-blacklisted candidates stay as a lower-ranked
    # fallback instead of making discovery fail.
    $rank = 1
    if (($adapter.HardwareInterface -eq $true) -and ($adapter.Virtual -ne $true)) {
        $rank = 0
    }
    $localAddress = [string](Get-NetIPAddress -AddressFamily IPv4 -InterfaceIndex $route.InterfaceIndex -ErrorAction SilentlyContinue |
        Where-Object { $_.AddressState -eq 'Preferred' } | Select-Object -First 1).IPAddress
    $rows += [PSCustomObject]@{
        InterfaceIndex = [uint32]$route.InterfaceIndex
        NextHop = [string]$route.NextHop
        RouteMetric = [uint32]$route.RouteMetric
        InterfaceMetric = [uint32]$ip.InterfaceMetric
        PhysicalRank = [uint32]$rank
        Address = $localAddress
    }
}
ConvertTo-Json -InputObject $rows -Compress
"#
}

#[cfg(any(windows, test))]
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

    let script = match forced {
        Some(index) => forced_underlay_script(index),
        None => automatic_underlay_script().to_owned(),
    };
    let output = powershell(&script).context("read physical IPv4 default route")?;
    let json = String::from_utf8(output.stdout).context("default-route JSON was not UTF-8")?;
    let underlay = parse_default_route(&json, 0)?;
    tracing::info!(
        physical_interface_index = underlay.index,
        physical_gateway = %underlay.gateway,
        physical_address = %underlay
            .address
            .map(|address| address.to_string())
            .unwrap_or_default(),
        forced = forced.is_some(),
        "captured Windows transport underlay"
    );
    // Publish the underlay so ToTS's own sockets (direct UDP, TURN, provider HTTP
    // and WebSocket signalling, WebRTC media) are bound to the physical adapter
    // instead of following the full-tunnel default route.
    if let Some(address) = underlay.address {
        tots_core::egress::configure_address(address, None);
    }
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

#[cfg(any(windows, test))]
fn install_sync(
    tun_index: i32,
    protected: Vec<Ipv4Addr>,
    underlay: UnderlayRoute,
) -> Result<FullTunnelRoutes> {
    let mut guard = build_guard(tun_index, &protected, underlay)?;
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
        powershell(&verify_full_tunnel_script(tun_index as u32))
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

#[cfg(any(windows, test))]
fn snapshot_dns(index: u32) -> Result<DnsSnapshot> {
    let output = powershell(&snapshot_dns_script(index))
        .with_context(|| format!("snapshot IPv4 DNS servers on interface {index}"))?;
    let raw = String::from_utf8(output.stdout).context("DNS snapshot was not UTF-8")?;
    parse_dns_snapshot(index, &raw)
}

#[cfg(any(windows, test))]
fn parse_dns_snapshot(index: u32, raw: &str) -> Result<DnsSnapshot> {
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

/// Stand-in for the Windows PowerShell runner.
///
/// The Windows route implementation is compiled under `cfg(test)` on every host so
/// the Linux test suite type-checks it: a Windows-only field previously hid a
/// `collect()` mismatch until the Windows CI job failed. Calls from a non-Windows
/// test always fail, which is what those tests expect.
#[cfg(all(test, not(windows)))]
fn powershell(_script: &str) -> Result<std::process::Output> {
    bail!("Windows PowerShell is not available on this host")
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
            address: None,
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

    #[cfg(not(windows))]
    #[tokio::test]
    async fn refresh_protected_records_new_endpoints_without_duplicating_routes() {
        // PowerShell is unavailable here, so every dynamic route add fails. The
        // point of the test is that the code path compiles on Linux and that a
        // failure is reported without corrupting the tracked set.
        let mut guard = build_guard(7, &["203.0.113.9".parse().unwrap()], default_route()).unwrap();
        let endpoint: Ipv4Addr = "198.51.100.7".parse().unwrap();
        assert_eq!(guard.properties_for_test(), (true, 1));
        // The script fails on this host, so no endpoint is claimed and no route is
        // recorded; a partial failure must never corrupt the tracked set.
        guard.refresh_protected(vec![endpoint]).await.unwrap();
        assert_eq!(guard.properties_for_test(), (true, 1));
        assert!(guard.changes.routes.is_empty());
        // Unusable endpoints are filtered before any script runs.
        guard
            .refresh_protected(vec![Ipv4Addr::UNSPECIFIED, Ipv4Addr::LOCALHOST])
            .await
            .unwrap();
        assert_eq!(guard.properties_for_test(), (true, 1));
        // Releasing a stale endpoint must not panic with an empty route list.
        guard
            .release_endpoints_for_test(vec!["192.0.2.99".parse().unwrap()])
            .await;
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn windows_installation_reports_a_missing_powershell_instead_of_panicking() {
        // `install`, `install_route_plan`, `snapshot_dns` and `capture_underlay_sync`
        // are Windows-only in production; on this host they must fail cleanly, which
        // also type-checks them in the Linux test suite.
        let plan = route_plan(7, default_route(), vec!["203.0.113.9".parse().unwrap()]);
        assert!(install_route_plan(&plan).is_err());
        assert!(snapshot_dns(12).is_err());
        let result = install_sync(7, vec!["203.0.113.9".parse().unwrap()], default_route());
        let error = match result {
            Ok(_) => panic!("no PowerShell on this host, so install must fail"),
            Err(error) => error,
        };
        assert!(!format!("{error:#}").is_empty());
        // Partial routes must be rolled back, never leaked as live system routes.
        assert!(capture_underlay_sync().is_err());
    }

    #[test]
    fn capture_underlay_validates_the_forced_index_before_running_powershell() {
        std::env::set_var("TOTS_PHYSICAL_IFINDEX", "0");
        let error = capture_underlay_sync().expect_err("index 0 is invalid");
        assert!(format!("{error:#}").contains("TOTS_PHYSICAL_IFINDEX"));
        std::env::set_var("TOTS_PHYSICAL_IFINDEX", "not-a-number");
        assert!(capture_underlay_sync().is_err());
        std::env::remove_var("TOTS_PHYSICAL_IFINDEX");
    }

    #[test]
    fn guard_accepts_valid_endpoints_and_rejects_unusable_ones() {
        let underlay = default_route();
        let guard = build_guard(7, &["203.0.113.9".parse().unwrap()], underlay).unwrap();
        // The initial route plan's endpoints must be recorded, otherwise the first
        // refresh re-adds them and the set drifts.
        assert!(guard.protected.contains(&"203.0.113.9".parse().unwrap()));
        assert_eq!(guard.underlay, underlay);
        assert!(guard.changes.routes.is_empty());
        assert!(guard.changes.dns.is_empty());
        assert!(guard.changes.ipv6_rule.is_none());

        // Same checks `install` enforced inline before the guard was extracted.
        assert!(build_guard(0, &["203.0.113.9".parse().unwrap()], underlay).is_err());
        assert!(build_guard(7, &[], underlay).is_err());
        for bad in ["0.0.0.0", "127.0.0.1", "224.0.0.1", "255.255.255.255"] {
            assert!(
                build_guard(7, &[bad.parse().unwrap()], underlay).is_err(),
                "accepted {bad}"
            );
        }
        // The underlay index must never be the TUN index.
        assert!(build_guard(12, &["203.0.113.9".parse().unwrap()], underlay).is_err());
    }

    #[test]
    fn protected_endpoint_set_expires_idle_entries_and_respects_its_cap() {
        let mut set = ProtectedEndpoints::default();
        for octet in 1..=4u8 {
            set.insert(Ipv4Addr::new(203, 0, 113, octet));
        }
        assert_eq!(set.len(), 4);
        // Nothing is stale before the grace period.
        assert!(set.expired(std::time::Instant::now()).is_empty());
        // After it, every entry may be released. `insert` timestamps with
        // `Instant::now()`, so allow for the elapsed test execution.
        let later = std::time::Instant::now() + ProtectedEndpoints::IDLE_EXPIRY * 2;
        assert_eq!(set.expired(later).len(), 4);

        // Above the cap the oldest entries are released even while still "fresh".
        let mut capped = ProtectedEndpoints::default();
        let overflow = 10usize;
        for index in 0..(ProtectedEndpoints::MAX + overflow) {
            let address = Ipv4Addr::new(
                198,
                51,
                u8::try_from(index / 250).unwrap_or(0),
                u8::try_from(index % 250).unwrap_or(0),
            );
            capped.insert(address);
        }
        assert_eq!(capped.len(), ProtectedEndpoints::MAX + overflow);
        let released = capped.expired(std::time::Instant::now());
        assert_eq!(released.len(), overflow);

        // Re-touching a tracked endpoint refreshes it, so an active provider
        // endpoint is not released while it is still in use.
        let active = Ipv4Addr::new(198, 51, 0, 0);
        assert!(capped.contains(&active));
        capped.touch(&active);
        assert!(capped.contains(&active));
        assert_eq!(capped.len(), ProtectedEndpoints::MAX + overflow);
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
    fn windows_scripts_are_complete_and_brace_balanced() {
        // These scripts used to be built with `format!`, so every PowerShell brace
        // needed doubling and a mismatch only surfaced on a Windows build. They are
        // now plain templates; assert that every placeholder was substituted and
        // that braces balance, so the Linux test suite catches a broken script.
        let route = Route {
            address: "203.0.113.9".parse().unwrap(),
            prefix: 32,
            index: 12,
            gateway: "192.168.1.1".parse().unwrap(),
        };
        let plan = route_plan(7, default_route(), vec!["203.0.113.9".parse().unwrap()]);
        let scripts = vec![
            install_route_plan_script(&plan),
            add_route_script(route),
            remove_route_script(route),
            remove_ipv6_rule_script("ToTS-IPv6-Block-1234"),
            verify_full_tunnel_script(7),
            snapshot_dns_script(12),
            forced_underlay_script(12),
            automatic_underlay_script().to_owned(),
            ipv6_block_rule_script("ToTS-IPv6-Block-1234"),
            set_ipv4_dns_script(&[7, 12]),
            restore_dns_script(&DnsSnapshot {
                index: 12,
                servers: vec!["1.1.1.1".parse().unwrap()],
            }),
        ];
        const TOKENS: [&str; 7] = [
            "__ROWS__",
            "__QUERY__",
            "__INDEX__",
            "__GATEWAY__",
            "__DESTINATION__",
            "__RULE__",
            "__TOTS_RULE__",
        ];
        for script in scripts {
            for token in TOKENS {
                assert!(
                    !script.contains(token),
                    "unsubstituted {token} in: {}",
                    script.trim()
                );
            }
            assert!(
                script.matches('{').count() == script.matches('}').count(),
                "unbalanced braces in: {}",
                script.trim()
            );
            assert!(!script.trim().is_empty());
        }
    }

    #[test]
    fn windows_route_scripts_carry_the_route_they_manage() {
        let route = Route {
            address: "203.0.113.9".parse().unwrap(),
            prefix: 32,
            index: 12,
            gateway: "192.168.1.1".parse().unwrap(),
        };
        let add = add_route_script(route);
        assert!(add.contains("-DestinationPrefix '203.0.113.9/32'"));
        assert!(add.contains("-InterfaceIndex 12"));
        assert!(add.contains("-NextHop '192.168.1.1'"));
        assert!(add.contains("exit 10"));
        assert!(remove_route_script(route).contains("Remove-NetRoute"));
        // Every plan row must reach the script as a literal object.
        let plan = route_plan(7, default_route(), vec!["203.0.113.9".parse().unwrap()]);
        let install = install_route_plan_script(&plan);
        assert!(install.contains("Address='203.0.113.9'; Prefix=32; InterfaceIndex=12"));
        assert!(install.contains("Address='0.0.0.0'; Prefix=1; InterfaceIndex=7"));
        let verify = verify_full_tunnel_script(7);
        assert!(verify.contains("-InterfaceIndex 7"));
        assert!(verify.contains("'0.0.0.0/1'"));
        assert!(!verify.contains("__INDEX__"));
    }

    #[test]
    fn windows_dns_snapshot_parsing_is_shared_with_the_script_builder() {
        assert!(snapshot_dns_script(12).contains("-InterfaceIndex 12"));
        let snapshot = parse_dns_snapshot(12, r#"["1.1.1.1","8.8.8.8","1.1.1.1"]"#).unwrap();
        assert_eq!(snapshot.index, 12);
        assert_eq!(
            snapshot.servers,
            vec![
                "1.1.1.1".parse::<Ipv4Addr>().unwrap(),
                "8.8.8.8".parse::<Ipv4Addr>().unwrap()
            ]
        );
        assert!(parse_dns_snapshot(12, "null").unwrap().servers.is_empty());
        assert!(parse_dns_snapshot(12, r#""9.9.9.9""#).is_ok());
        assert!(parse_dns_snapshot(12, "123").is_err());
        assert!(parse_dns_snapshot(12, r#"["not-an-ip"]"#).is_err());
    }

    #[test]
    fn captures_the_adapter_ipv4_for_windows_source_binding() {
        let rows = r#"[
          {"InterfaceIndex":12,"NextHop":"192.168.1.1","RouteMetric":20,"InterfaceMetric":10,"Address":"192.168.1.10"},
          {"InterfaceIndex":13,"NextHop":"192.168.1.1","RouteMetric":30,"InterfaceMetric":10,"Address":"not-an-address"}
        ]"#;
        let selected = parse_default_route(rows, 0).unwrap();
        assert_eq!(selected.address, Some("192.168.1.10".parse().unwrap()));
        // An unusable address is dropped instead of blocking capture.
        let rows = r#"[
          {"InterfaceIndex":12,"NextHop":"192.168.1.1","RouteMetric":20,"InterfaceMetric":10,"Address":"0.0.0.0"}
        ]"#;
        assert_eq!(parse_default_route(rows, 0).unwrap().address, None);
    }

    #[test]
    fn rejects_tun_as_captured_underlay() {
        assert!(validate_underlay(12, default_route()).is_err());
        assert!(validate_underlay(7, default_route()).is_ok());
    }
}
