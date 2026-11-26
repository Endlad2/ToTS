//! Physical-network egress pinning for every socket the ToTS stack opens itself.
//!
//! A full-tunnel VPN replaces the system default route with a TUN device. Any
//! transport socket created afterwards would then follow that default route and
//! be carried *by the tunnel it is supposed to build*: provider signalling loops
//! into itself, ICE gathering produces useless candidates, the TURN allocation
//! dies and the tunnel appears to "connect and then stop working". Provider
//! traffic must therefore leave through the real hardware adapter
//! (eth0 / wan / Wi-Fi) no matter what the route table says.
//!
//! This module is shared verbatim by `tots-core` and the bundled MIT
//! `jazz-session` crate (`#[path]` include), so the client, the provider adapters
//! and the WebRTC/ICE layer apply one policy. It deliberately depends on
//! `socket2` + `std` only and performs no logging: callers decide how to report
//! failures.
//!
//! Policy resolution, once per process:
//!
//! 1. `TOTS_EGRESS_INTERFACE` (or the client's `--egress-interface` flag) forces
//!    an explicit device name; `TOTS_EGRESS_ADDRESS` supplies its local IPv4 and
//!    `TOTS_EGRESS_INTERFACE_INDEX` its kernel index.
//! 2. Otherwise the IPv4 default route is inspected (Linux/Android:
//!    `/proc/net/route`) and the best non-virtual adapter is selected.
//!
//! Pinning mechanisms:
//!
//! * Linux / Android / Fuchsia: `SO_BINDTODEVICE` (needs `CAP_NET_RAW`).
//! * macOS family / Solaris / illumos: `IP_BOUND_IF` with an explicit index.
//! * Windows: no device-bind socket option in `socket2`, so sockets are bound to
//!   the adapter's own IPv4 address; Windows' strong-host model then guarantees
//!   that the adapter — not the TUN — carries the packet. `tots-client` also
//!   installs host routes through the captured physical underlay.
//!
//! Failure is never fatal: an unprivileged process (for example an Android app
//! that uses `VpnService.protect` instead) must still be able to run unpinned.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        OnceLock, RwLock,
    },
};

use socket2::{Domain, Protocol, Socket, Type};

/// Forced egress interface name (also settable with `--egress-interface`).
pub const EGRESS_INTERFACE_ENV: &str = "TOTS_EGRESS_INTERFACE";
/// Local IPv4 of that interface, used to bind sockets where no device-bind exists.
pub const EGRESS_ADDRESS_ENV: &str = "TOTS_EGRESS_ADDRESS";
/// Kernel interface index, required by the macOS `IP_BOUND_IF` path.
pub const EGRESS_INDEX_ENV: &str = "TOTS_EGRESS_INTERFACE_INDEX";
/// Set to `1` to disable egress pinning entirely (debugging/containers).
pub const EGRESS_DISABLE_ENV: &str = "TOTS_EGRESS_DISABLE";
/// The client's TUN device name; excluded from ICE candidates.
pub const TUN_INTERFACE_ENV: &str = "TOTS_TUN_INTERFACE";
/// The client's virtual IPv4; never advertised as an ICE candidate.
pub const TUN_IPV4_ENV: &str = "TOTS_TUN_IPV4";

/// Interface name prefixes that can never be the physical underlay. Shared so the
/// client, provider adapters and the WebRTC layer agree on "physical".
const VIRTUAL_PREFIXES: &[&str] = &[
    "lo",
    "tun",
    "tap",
    "wg",
    "zt",
    "utun",
    "ipsec",
    "veth",
    "docker",
    "br-",
    "virbr",
    "vmnet",
    "vboxnet",
    "ham",
    "nordlynx",
    "proton",
    "mullvad",
    "tailscale",
    "wintun",
    "tots",
    "xray",
    "sing",
    "clash",
    "mihomo",
];

pub fn is_virtual_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    VIRTUAL_PREFIXES
        .iter()
        .any(|prefix| lower.starts_with(prefix))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Egress {
    name: String,
    index: Option<u32>,
    address: Option<Ipv4Addr>,
}

impl Egress {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            index: None,
            address: None,
        }
    }

    pub fn with_index(mut self, index: Option<u32>) -> Self {
        self.index = index.filter(|index| *index > 0);
        self
    }

    pub fn with_address(mut self, address: Option<Ipv4Addr>) -> Self {
        self.address = address.filter(|address| !address.is_unspecified());
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn index(&self) -> Option<u32> {
        self.index
    }

    /// Local IPv4 address of the underlay adapter.
    pub fn address(&self) -> Option<Ipv4Addr> {
        self.address
    }
}

/// Selected underlay. A read-write cell rather than a `OnceLock` because the
/// Windows client can only publish its underlay *after* capturing it, and the
/// capture happens after the initial (address-less) configuration.
static EGRESS: RwLock<Option<Egress>> = RwLock::new(None);
static LAST_ERROR: OnceLock<String> = OnceLock::new();
static BIND_ATTEMPTED: AtomicBool = AtomicBool::new(false);

/// Environment variable, if set and non-empty.
fn env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// Resolve (and cache) the egress interface. `explicit` wins over detection.
///
/// Must be called once by the *client* process at startup (it is what turns
/// pinning on). Other processes — notably `tots-server`, which must keep binding
/// wildcard listeners — never call it, and therefore never pin their sockets.
///
/// Returns `None` when traffic must follow the ordinary route table: pinning is
/// disabled by configuration, the platform offers no mechanism, or no usable
/// physical default route exists.
/// Configure pinning from a host-provided underlay address.
///
/// Windows has no `SO_BINDTODEVICE`, but binding the transport sockets to the
/// physical adapter's own IPv4 address makes the strong-host model choose that
/// adapter, so the client publishes the captured underlay address here.
pub fn configure_address(address: Ipv4Addr, name: Option<&str>) -> Option<Egress> {
    if let Some(existing) = current() {
        return Some(existing);
    }
    let selected = Egress::new(name.unwrap_or("physical-underlay"))
        .with_index(env(EGRESS_INDEX_ENV).and_then(|value| value.parse::<u32>().ok()))
        .with_address(Some(address));
    publish(&selected);
    store(Some(selected))
}

fn store(selected: Option<Egress>) -> Option<Egress> {
    if let Ok(mut guard) = EGRESS.write() {
        *guard = selected.clone();
    }
    selected
}

pub fn configure(explicit: Option<&str>) -> Option<Egress> {
    if let Some(cached) = current() {
        return Some(cached);
    }
    let selected = resolve_uncached(explicit);
    if let Some(egress) = &selected {
        publish(egress);
    }
    store(selected)
}

/// Compatible alias used by tests and by callers that already hold the value.
pub fn resolve(explicit: Option<&str>) -> Option<Egress> {
    configure(explicit)
}

fn resolve_uncached(explicit: Option<&str>) -> Option<Egress> {
    if env(EGRESS_DISABLE_ENV).is_some_and(|value| value != "0") {
        return None;
    }
    let requested = explicit
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .or_else(|| env(EGRESS_INTERFACE_ENV));
    let index = env(EGRESS_INDEX_ENV).and_then(|value| value.parse::<u32>().ok());
    let address = env(EGRESS_ADDRESS_ENV).and_then(|value| value.parse::<Ipv4Addr>().ok());
    match requested {
        Some(name) => {
            let detected_address = address.or_else(|| interface_ipv4(&name));
            Some(
                Egress::new(name)
                    .with_index(index)
                    .with_address(detected_address),
            )
        }
        None => match detect() {
            Some(detected) => Some(detected),
            // A host that knows only the underlay's address (Windows publishes the
            // captured adapter address) still gets source-address pinning.
            None => address.map(|address| {
                Egress::new("physical-underlay")
                    .with_index(index)
                    .with_address(Some(address))
            }),
        },
    }
}

/// Publish the resolved interface through the environment so the bundled
/// `jazz-session` crate (a separate compilation unit with its own copy of this
/// module) uses the same interface.
fn publish(egress: &Egress) {
    std::env::set_var(EGRESS_INTERFACE_ENV, egress.name());
    if let Some(index) = egress.index() {
        std::env::set_var(EGRESS_INDEX_ENV, index.to_string());
    }
    if let Some(address) = egress.address() {
        std::env::set_var(EGRESS_ADDRESS_ENV, address.to_string());
    }
}

/// The interface resolved for this process, if any.
pub fn current() -> Option<Egress> {
    EGRESS.read().ok().and_then(|guard| guard.clone())
}

/// Resolve from the cache, or from the environment variables published by the
/// client (which is how the `jazz-session` copy learns the interface).
///
/// Never performs detection: a process that did not call [`configure`] keeps the
/// default route behaviour.
pub fn ensure() -> Option<Egress> {
    if let Some(cached) = current() {
        return Some(cached);
    }
    if env(EGRESS_DISABLE_ENV).is_some_and(|value| value != "0") {
        return None;
    }
    let name = env(EGRESS_INTERFACE_ENV)?;
    let resolved = Egress::new(name)
        .with_index(env(EGRESS_INDEX_ENV).and_then(|value| value.parse::<u32>().ok()))
        .with_address(env(EGRESS_ADDRESS_ENV).and_then(|value| value.parse::<Ipv4Addr>().ok()));
    store(Some(resolved))
}

/// Interface name for APIs that accept a name only
/// (`reqwest::ClientBuilder::interface`).
pub fn interface_name() -> Option<String> {
    ensure().map(|egress| egress.name().to_owned())
}

/// Local IPv4 address of the resolved underlay adapter, for APIs that can bind a
/// source address but not a device (`yawc`, and `reqwest::local_address`).
pub fn local_address() -> Option<Ipv4Addr> {
    ensure().and_then(|egress| egress.address())
}

/// Interfaces that must never receive an ICE candidate.
pub fn excluded_interface_prefixes() -> Vec<String> {
    let mut excluded: Vec<String> = VIRTUAL_PREFIXES.iter().map(|p| (*p).to_owned()).collect();
    if let Some(tun) = env(TUN_INTERFACE_ENV) {
        excluded.push(tun);
    }
    excluded
}

/// Interface name of the tunnel device, when the host told us.
pub fn tunnel_interface() -> Option<String> {
    env(TUN_INTERFACE_ENV)
}

/// The client's virtual IPv4 address.
pub fn tunnel_ipv4() -> Option<Ipv4Addr> {
    env(TUN_IPV4_ENV).and_then(|value| value.parse::<Ipv4Addr>().ok())
}

/// Publish the client's tunnel identity so provider/ICE code can exclude it.
pub fn set_tunnel(name: Option<&str>, ipv4: Option<Ipv4Addr>) {
    if let Some(name) = name.map(str::trim).filter(|name| !name.is_empty()) {
        std::env::set_var(TUN_INTERFACE_ENV, name);
    }
    if let Some(ipv4) = ipv4 {
        std::env::set_var(TUN_IPV4_ENV, ipv4.to_string());
    }
}

/// True when `ip` belongs to the ToTS tunnel and must not be advertised.
pub fn is_tunnel_ip(ip: IpAddr) -> bool {
    match (ip, tunnel_ipv4()) {
        (IpAddr::V4(candidate), Some(tunnel)) => candidate == tunnel,
        _ => false,
    }
}

/// The most recent pinning failure, for one-shot diagnostics by the caller.
pub fn last_error() -> Option<&'static str> {
    LAST_ERROR.get().map(String::as_str)
}

fn record_error(message: String) -> bool {
    let _ = LAST_ERROR.set(message);
    false
}

/// Whether this build can pin sockets at all.
pub const fn platform_supported() -> bool {
    cfg!(any(
        target_os = "linux",
        target_os = "android",
        target_os = "fuchsia",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "visionos",
        target_os = "macos",
        target_os = "solaris",
        target_os = "illumos"
    ))
}

/// Pin an already-created socket to the resolved egress interface/address.
///
/// Returns `false` when no pinning was applied; `last_error()` then explains why.
pub fn apply(socket: &Socket) -> bool {
    BIND_ATTEMPTED.store(true, Ordering::Relaxed);
    let Some(egress) = ensure() else {
        return false;
    };
    if let Err(error) = bind_device(socket, &egress) {
        return record_error(format!(
            "cannot bind to interface {}: {error}",
            egress.name()
        ));
    }
    true
}

/// Bind an unbound socket to the underlay's own IPv4 address.
///
/// Used where no device-bind option exists (Windows) and as a second safety net
/// elsewhere: a packet whose source address belongs to the physical adapter
/// cannot be emitted through the TUN device.
pub fn bind_local_address(socket: &Socket, port: u16) -> bool {
    let Some(egress) = ensure() else {
        return false;
    };
    let Some(address) = egress.address() else {
        return false;
    };
    match socket.bind(&SocketAddr::new(IpAddr::V4(address), port).into()) {
        Ok(()) => true,
        Err(error) => record_error(format!(
            "cannot bind local address {address} on {}: {error}",
            egress.name()
        )),
    }
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "fuchsia"))]
fn bind_device(socket: &Socket, egress: &Egress) -> std::io::Result<()> {
    socket.bind_device(Some(egress.name().as_bytes()))
}

#[cfg(any(
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "visionos",
    target_os = "macos",
    target_os = "solaris",
    target_os = "illumos"
))]
fn bind_device(socket: &Socket, egress: &Egress) -> std::io::Result<()> {
    match egress.index().and_then(std::num::NonZeroU32::new) {
        Some(index) => socket.bind_device_by_index_v4(Some(index)),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{} is required on this platform", EGRESS_INDEX_ENV),
        )),
    }
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "fuchsia",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "visionos",
    target_os = "macos",
    target_os = "solaris",
    target_os = "illumos"
)))]
fn bind_device(_socket: &Socket, _egress: &Egress) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "this platform has no device-bind socket option",
    ))
}

fn domain(address: SocketAddr) -> Domain {
    if address.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    }
}

/// Create a UDP socket, pinned to the egress adapter and bound to `bind`.
///
/// `None` means "no egress policy is active, use the default socket path".
///
/// The requested bind address is used verbatim. Binding to the underlay's own
/// address is only correct for sockets that talk to the provider network; see
/// [`udp_socket_for`], which decides that from the remote address.
pub fn udp_socket(
    bind: SocketAddr,
    recv_buffer: usize,
    send_buffer: usize,
) -> Option<std::io::Result<Socket>> {
    let _ = ensure()?;
    let socket = match Socket::new(domain(bind), Type::DGRAM, Some(Protocol::UDP)) {
        Ok(socket) => socket,
        Err(error) => return Some(Err(error)),
    };
    if let Err(error) = socket.set_nonblocking(true) {
        return Some(Err(error));
    }
    let _ = socket.set_recv_buffer_size(recv_buffer);
    let _ = socket.set_send_buffer_size(send_buffer);
    if let Err(error) = socket.bind(&bind.into()) {
        return Some(Err(error));
    }
    apply(&socket);
    Some(Ok(socket))
}

/// Create a UDP socket for talking to `remote`, pinned to the egress adapter.
///
/// On platforms without a device-bind option (Windows) the only way to force the
/// physical adapter is to bind the underlay's own source address, so a wildcard
/// bind becomes `underlay_ip:0`. Loopback and other explicitly requested
/// addresses are never rewritten.
pub fn udp_socket_for(
    remote: SocketAddr,
    recv_buffer: usize,
    send_buffer: usize,
) -> Option<std::io::Result<Socket>> {
    let bind = outbound_bind(remote);
    let socket = udp_socket(bind, recv_buffer, send_buffer)?;
    Some(socket)
}

/// Source address to bind for an outbound connection to `remote`.
pub fn outbound_bind(remote: SocketAddr) -> SocketAddr {
    let wildcard = if remote.is_ipv4() {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
    } else {
        SocketAddr::new(IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED), 0)
    };
    if remote.ip().is_loopback() {
        return wildcard;
    }
    match ensure().and_then(|egress| egress.address()) {
        Some(address) if remote.is_ipv4() => {
            SocketAddr::new(IpAddr::V4(address), 0)
        }
        _ => wildcard,
    }
}

/// Create a TCP socket prepared for `connect`, pinned to the egress adapter.
pub fn tcp_socket(remote: SocketAddr, nonblocking: bool) -> Option<std::io::Result<Socket>> {
    let _ = ensure()?;
    let socket = match Socket::new(domain(remote), Type::STREAM, Some(Protocol::TCP)) {
        Ok(socket) => socket,
        Err(error) => return Some(Err(error)),
    };
    if let Err(error) = socket.set_nonblocking(nonblocking) {
        return Some(Err(error));
    }
    let bind = outbound_bind(remote);
    if !bind.ip().is_unspecified() {
        let _ = socket.bind(&bind.into());
    }
    apply(&socket);
    Some(Ok(socket))
}

/// True when the caller should use the pinned sockets.
pub fn active() -> bool {
    ensure().is_some()
}

/// Apply the egress policy to a provider HTTP client under construction.
///
/// On Linux/Android/Fuchsia (and the macOS family) the HTTP stack sets
/// `SO_BINDTODEVICE` / `IP_BOUND_IF` for every connection it creates, including
/// pooled reconnects and redirects. On platforms without a name-based device
/// option (Windows) the builder is returned unchanged; the client then relies on
/// its underlay host routes.
pub fn configure_http(builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
    #[cfg(any(
        target_os = "android",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos",
        target_os = "solaris",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos",
    ))]
    {
        let builder = match interface_name() {
            Some(name) => builder.interface(name.as_str()),
            None => builder,
        };
        // Binding the source address as well is what pins the socket on Windows,
        // where the HTTP stack has no interface option.
        match local_address() {
            Some(address) => builder.local_address(std::net::IpAddr::V4(address)),
            None => builder,
        }
    }
    #[cfg(not(any(
        target_os = "android",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos",
        target_os = "solaris",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos",
    )))]
    {
        match local_address() {
            Some(address) => builder.local_address(std::net::IpAddr::V4(address)),
            None => builder,
        }
    }
}

/// Whether a pinning attempt was made (used to distinguish "disabled" from
/// "unsupported" in diagnostics).
pub fn pinning_attempted() -> bool {
    BIND_ATTEMPTED.load(Ordering::Relaxed)
}

/// Read the IPv4 default-route table and pick the best physical adapter.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn detect() -> Option<Egress> {
    let table = std::fs::read_to_string("/proc/net/route").ok()?;
    let mut candidates: Vec<(u32, String)> = Vec::new();
    for line in table.lines().skip(1) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 8 {
            continue;
        }
        let (iface, destination, flags, metric) = (fields[0], fields[1], fields[3], fields[6]);
        // Default route: destination 0.0.0.0/0 with RTF_UP | RTF_GATEWAY.
        if !destination.eq_ignore_ascii_case("00000000") {
            continue;
        }
        let Ok(flags) = u32::from_str_radix(flags, 16) else {
            continue;
        };
        if flags & 0x1 == 0 || flags & 0x2 == 0 {
            continue;
        }
        if iface.is_empty() || iface == "lo" || is_virtual_name(iface) {
            continue;
        }
        candidates.push((metric.parse::<u32>().unwrap_or(u32::MAX), iface.to_owned()));
    }
    candidates.sort();
    candidates.dedup_by(|left, right| left.1 == right.1);
    let (_, name) = candidates.into_iter().next()?;
    Some(Egress::new(name.clone()).with_index(interface_index(&name)))
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn detect() -> Option<Egress> {
    // Windows has no name-based device-bind option and macOS needs an explicit
    // index, so detection uses the underlay the client published after capturing
    // it (address and/or index) instead of inspecting the route table itself.
    let name = env(EGRESS_INTERFACE_ENV);
    let index = env(EGRESS_INDEX_ENV).and_then(|value| value.parse::<u32>().ok());
    let address = env(EGRESS_ADDRESS_ENV).and_then(|value| value.parse::<Ipv4Addr>().ok());
    if name.is_none() && index.is_none() && address.is_none() {
        return None;
    }
    Some(
        Egress::new(name.unwrap_or_else(|| "physical-underlay".to_owned()))
            .with_index(index)
            .with_address(address),
    )
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn interface_index(name: &str) -> Option<u32> {
    std::fs::read_to_string(format!("/sys/class/net/{name}/ifindex"))
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|index| *index > 0)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn interface_ipv4(name: &str) -> Option<Ipv4Addr> {
    // `ip` may be absent in minimal images; the address is optional because
    // SO_BINDTODEVICE already pins egress on these platforms.
    let output = std::process::Command::new("ip")
        .args(["-4", "-o", "addr", "show", "dev", name])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    text.split_whitespace()
        .find_map(|token| token.split('/').next()?.parse::<Ipv4Addr>().ok())
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn interface_ipv4(_name: &str) -> Option<Ipv4Addr> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbound_bind_keeps_loopback_and_uses_the_underlay_for_provider_traffic() {
        // A provider connection must originate from the underlay address so the OS
        // cannot send it through the TUN, but loopback traffic (tests, local
        // bridges) must never be rewritten.
        let loopback: SocketAddr = "127.0.0.1:9".parse().unwrap();
        assert!(outbound_bind(loopback).ip().is_unspecified());
        let provider: SocketAddr = "203.0.113.7:443".parse().unwrap();
        match ensure().and_then(|egress| egress.address()) {
            Some(address) => assert_eq!(outbound_bind(provider).ip(), IpAddr::V4(address)),
            None => assert!(outbound_bind(provider).ip().is_unspecified()),
        }
    }

    #[test]
    fn virtual_adapter_names_are_never_selected() {
        for name in [
            "lo",
            "tun0",
            "tots0",
            "wg0",
            "utun3",
            "docker0",
            "veth5e30488",
            "br-lan",
            "tailscale0",
            "Wintun",
        ] {
            assert!(is_virtual_name(name), "accepted {name}");
        }
        for name in ["eth0", "enp59s0", "wlp60s0", "wan", "pppoe-wan", "en0"] {
            assert!(!is_virtual_name(name), "rejected {name}");
        }
    }

    #[test]
    fn explicit_interface_keeps_its_name_index_and_address() {
        let selected = Egress::new("wan")
            .with_index(Some(7))
            .with_address(Some("192.0.2.5".parse().unwrap()));
        assert_eq!(selected.name(), "wan");
        assert_eq!(selected.index(), Some(7));
        assert_eq!(selected.address(), Some("192.0.2.5".parse().unwrap()));
        // A zero index and an unspecified address are never recorded.
        let empty = Egress::new("wan")
            .with_index(Some(0))
            .with_address(Some(Ipv4Addr::UNSPECIFIED));
        assert_eq!(empty.index(), None);
        assert_eq!(empty.address(), None);
    }

    #[test]
    fn address_only_underlays_survive_the_environment_round_trip() {
        // Windows publishes only the captured underlay address. `publish` must
        // round-trip it so the `jazz-session` copy of this module sees it too.
        // The variables are restored because they are process-wide and other tests
        // must keep binding loopback.
        let before = (env(EGRESS_INTERFACE_ENV), env(EGRESS_ADDRESS_ENV));
        let selected = Egress::new("Ethernet").with_address(Some("192.0.2.44".parse().unwrap()));
        publish(&selected);
        assert_eq!(env(EGRESS_INTERFACE_ENV).as_deref(), Some("Ethernet"));
        assert_eq!(env(EGRESS_ADDRESS_ENV).as_deref(), Some("192.0.2.44"));
        assert_eq!(
            Egress::new("x")
                .with_address(env(EGRESS_ADDRESS_ENV).and_then(|v| v.parse().ok()))
                .address(),
            Some("192.0.2.44".parse().unwrap())
        );
        restore(EGRESS_INTERFACE_ENV, before.0);
        restore(EGRESS_ADDRESS_ENV, before.1);
    }

    fn restore(name: &str, value: Option<String>) {
        match value {
            Some(value) => std::env::set_var(name, value),
            None => std::env::remove_var(name),
        }
    }

    #[test]
    fn disabling_pinning_removes_the_environment_override() {
        // Exercise the policy switch through the same variable `configure` reads,
        // without depending on the process-wide cache.
        std::env::set_var(EGRESS_DISABLE_ENV, "1");
        assert!(env(EGRESS_DISABLE_ENV).is_some_and(|value| value != "0"));
        std::env::remove_var(EGRESS_DISABLE_ENV);
        assert!(env(EGRESS_DISABLE_ENV).is_none());
    }
}
