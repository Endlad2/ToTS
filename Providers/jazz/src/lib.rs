use std::{collections::HashSet, net::{IpAddr, Ipv4Addr}, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use jazz_session::{join_call, CallSession, JazzDataPlane};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{net::lookup_host, sync::Mutex};
use tots_core::{ProviderTransport, TurnCredentials, TurnProvider, TurnServer};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JazzProviderData {
    /// SaluteJazz invite URL issued by tots-server for this device. Jazz is a
    /// room/SFU transport, not a generic TURN proxy, so a server-issued room is
    /// required for the native transport.
    #[serde(default)]
    pub join_link: String,

    /// Guest display name used for the Jazz session.
    #[serde(default = "default_name")]
    pub name: String,
}

impl Default for JazzProviderData {
    fn default() -> Self {
        Self {
            join_link: String::new(),
            name: default_name(),
        }
    }
}

fn default_name() -> String {
    "ToTS client".to_string()
}

pub struct JazzProvider {
    data: JazzProviderData,
    session: Arc<Mutex<Option<CallSession>>>,
}

impl JazzProvider {
    pub fn new(mut data: JazzProviderData) -> Result<Self> {
        data.name = data.name.trim().to_string();
        if data.name.is_empty() {
            bail!("Jazz provider_data.name must not be empty");
        }
        Ok(Self {
            data,
            session: Arc::new(Mutex::new(None)),
        })
    }

    async fn connect_native(&self) -> Result<JazzNativeTransport> {
        let join_link = self.data.join_link.trim();
        if join_link.is_empty() {
            bail!(
                "Jazz transport requires provider_data.join_link from a server-issued tots:// config; regenerate the config with provider/transport=jazz"
            );
        }

        let session = join_call(join_link, &self.data.name)
            .await
            .context("join server-created SaluteJazz room")?;
        let data = session.data_plane().await.context("open Jazz data plane")?;
        data.wait_ready(Duration::from_secs(20))
            .await
            .context("wait for Jazz reliable data channel")?;
        // Capture route exceptions only after ICE/SCTP is actually usable so
        // trickled SFU candidates are present in the snapshot before Windows
        // installs its full-tunnel routes.
        let snapshot = session.json().await;
        let route_exception_ipv4 = resolve_jazz_ipv4(&snapshot).await;

        // The CallSession owns signaling and both PeerConnections. Keep it in
        // the provider for at least as long as the returned data plane exists.
        *self.session.lock().await = Some(session);

        Ok(JazzNativeTransport {
            data,
            route_exception_ipv4,
            session: self.session.clone(),
        })
    }
}

struct JazzNativeTransport {
    data: JazzDataPlane,
    route_exception_ipv4: Vec<Ipv4Addr>,
    session: Arc<Mutex<Option<CallSession>>>,
}

#[async_trait]
impl ProviderTransport for JazzNativeTransport {
    fn name(&self) -> &'static str {
        "jazz"
    }

    async fn send(&self, packet: Vec<u8>) -> Result<()> {
        self.data.send(&packet).await.context("send Jazz DataPacket")
    }

    async fn recv(&self) -> Result<Vec<u8>> {
        self.data.recv().await.context("receive Jazz DataPacket")
    }

    async fn close(&self) {
        let session = { self.session.lock().await.take() };
        if let Some(session) = session {
            let _ = session.close().await;
        }
    }

    fn route_exception_ipv4(&self) -> Vec<Ipv4Addr> {
        self.route_exception_ipv4.clone()
    }
}

#[async_trait]
impl TurnProvider for JazzProvider {
    fn name(&self) -> &'static str {
        "jazz"
    }

    async fn connect_transport(&self) -> Result<Option<Arc<dyn ProviderTransport>>> {
        Ok(Some(Arc::new(self.connect_native().await?)))
    }

    async fn get_credentials(&self) -> Result<TurnCredentials> {
        bail!("Jazz uses its native room/SFU data transport; raw TURN credentials are not a generic ToTS relay")
    }
}

async fn resolve_jazz_ipv4(snapshot: &Value) -> Vec<Ipv4Addr> {
    let mut endpoints: Vec<(String, u16)> = Vec::new();

    if let Some(raw) = snapshot.pointer("/call/connector_url").and_then(Value::as_str) {
        if let Ok(url) = Url::parse(raw) {
            if let Some(host) = url.host_str() {
                endpoints.push((host.to_string(), url.port_or_known_default().unwrap_or(443)));
            }
        }
    }

    if let Some(servers) = snapshot.pointer("/webrtc/ice_servers").and_then(Value::as_array) {
        for entry in servers {
            for raw in ice_urls(entry) {
                if let Some((host, port)) = parse_ice_endpoint(raw) {
                    endpoints.push((host, port));
                }
            }
        }
    }

    let mut result = HashSet::new();
    for (host, port) in endpoints {
        if let Ok(addrs) = lookup_host((host.as_str(), port)).await {
            for addr in addrs {
                if let IpAddr::V4(ip) = addr.ip() {
                    if is_route_exception(ip) {
                        result.insert(ip);
                    }
                }
            }
        }
    }

    // Once Windows installs the ToTS /1 routes, the already-established Jazz
    // ICE sockets must keep reaching the SFU on the original gateway. The SFU
    // addresses are carried in trickle candidates/SDP and are not necessarily
    // the same addresses as the configured TURN hosts.
    collect_candidate_ipv4(snapshot, &mut result);

    let mut result: Vec<_> = result.into_iter().collect();
    result.sort();
    result
}


fn is_route_exception(ip: Ipv4Addr) -> bool {
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_multicast())
}

fn collect_candidate_ipv4(value: &Value, result: &mut HashSet<Ipv4Addr>) {
    match value {
        Value::String(text) => {
            for line in text.lines() {
                let line = line.trim();
                let candidate = line
                    .strip_prefix("a=candidate:")
                    .or_else(|| line.strip_prefix("candidate:"));
                if let Some(candidate) = candidate {
                    // ICE candidate: foundation component transport priority
                    // address port typ ...
                    if let Some(address) = candidate.split_whitespace().nth(4) {
                        if let Ok(ip) = address.parse::<Ipv4Addr>() {
                            if is_route_exception(ip) {
                                result.insert(ip);
                            }
                        }
                    }
                }
                if let Some(address) = line.strip_prefix("c=IN IP4 ") {
                    if let Ok(ip) = address.trim().parse::<Ipv4Addr>() {
                        if is_route_exception(ip) {
                            result.insert(ip);
                        }
                    }
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_candidate_ipv4(item, result);
            }
        }
        Value::Object(map) => {
            for item in map.values() {
                collect_candidate_ipv4(item, result);
            }
        }
        _ => {}
    }
}

fn ice_urls(entry: &Value) -> Vec<&str> {
    let mut result = Vec::new();
    if let Some(url) = entry.get("url").and_then(Value::as_str) {
        result.push(url);
    }
    match entry.get("urls") {
        Some(Value::String(url)) => result.push(url),
        Some(Value::Array(urls)) => result.extend(urls.iter().filter_map(Value::as_str)),
        _ => {}
    }
    result
}

fn parse_ice_endpoint(raw: &str) -> Option<(String, u16)> {
    let raw = raw.trim();
    let (scheme, rest) = raw.split_once(':')?;
    let default_port = match scheme.to_ascii_lowercase().as_str() {
        "stun" | "turn" => 3478,
        "stuns" | "turns" => 5349,
        _ => return None,
    };
    let authority = rest.split_once('?').map(|(a, _)| a).unwrap_or(rest);
    if let Some(ipv6) = authority.strip_prefix('[') {
        let end = ipv6.find(']')?;
        let host = ipv6[..end].to_string();
        let tail = &ipv6[end + 1..];
        let port = if tail.is_empty() {
            default_port
        } else {
            tail.strip_prefix(':')?.parse().ok()?
        };
        return Some((host, port));
    }
    if let Some((host, port)) = authority.rsplit_once(':') {
        if !host.is_empty() {
            if let Ok(port) = port.parse() {
                return Some((host.to_string(), port));
            }
        }
    }
    (!authority.is_empty()).then(|| (authority.to_string(), default_port))
}

// Kept for source/API compatibility with callers that consumed the previous
// helper in tests. Jazz itself no longer uses this as a ToTS transport.
#[allow(dead_code)]
fn parse_turn_credentials(snapshot: &Value) -> Result<TurnCredentials> {
    let entries = snapshot
        .pointer("/webrtc/ice_servers")
        .and_then(Value::as_array)
        .context("Jazz session did not expose ICE servers")?;
    let mut selected_username: Option<String> = None;
    let mut selected_password: Option<String> = None;
    let mut servers = Vec::new();
    for entry in entries {
        let Some(username) = entry.get("username").and_then(Value::as_str) else { continue; };
        let Some(password) = entry.get("credential").and_then(Value::as_str) else { continue; };
        if username.is_empty() || password.is_empty() { continue; }
        match (&selected_username, &selected_password) {
            (None, None) => {
                selected_username = Some(username.to_string());
                selected_password = Some(password.to_string());
            }
            (Some(u), Some(p)) if u == username && p == password => {}
            _ => continue,
        }
        for raw in ice_urls(entry) {
            if let Some((host, port)) = parse_udp_turn_url(raw) {
                if !servers.iter().any(|existing: &TurnServer| existing.host == host && existing.port == port) {
                    servers.push(TurnServer { host, port });
                }
            }
        }
    }
    if servers.is_empty() { bail!("Jazz returned no UDP TURN endpoints"); }
    Ok(TurnCredentials {
        username: selected_username.context("Jazz TURN username is missing")?,
        password: selected_password.context("Jazz TURN credential is missing")?,
        servers,
    })
}

fn parse_udp_turn_url(raw: &str) -> Option<(String, u16)> {
    let raw = raw.trim();
    if raw.starts_with("turns:") { return None; }
    let rest = raw.strip_prefix("turn:")?;
    let (authority, query) = rest.split_once('?').unwrap_or((rest, ""));
    if !query.is_empty() {
        let transport = query.split('&').find_map(|part| part.strip_prefix("transport="));
        if transport.is_some_and(|value| !value.eq_ignore_ascii_case("udp")) { return None; }
    }
    parse_ice_endpoint(&format!("turn:{authority}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_jazz_turn_configuration_without_using_it_as_generic_relay() {
        let snapshot = json!({
            "webrtc": {"ice_servers": [{
                "urls": ["turn:relay.example.test:3478?transport=udp", "turn:relay.example.test:3478?transport=tcp"],
                "username": "u", "credential": "p"
            }]}
        });
        let creds = parse_turn_credentials(&snapshot).unwrap();
        assert_eq!(creds.servers.len(), 1);
        assert_eq!(creds.servers[0].host, "relay.example.test");
        assert_eq!(creds.servers[0].port, 3478);
    }

    #[test]
    fn extracts_public_sfu_candidates_for_full_tunnel_pinning() {
        let snapshot = json!({
            "webrtc": {
                "publisher": {
                    "remote_description": {
                        "sdp": "v=0\r\na=candidate:1 1 udp 2122260223 203.0.113.44 50000 typ host\r\na=candidate:2 1 udp 2122260223 169.254.10.2 50001 typ host\r\n"
                    }
                },
                "subscriber": {
                    "remote_ice_candidates": [{
                        "candidate": "candidate:3 1 udp 2122260223 198.51.100.7 40000 typ host"
                    }]
                }
            }
        });
        let mut found = HashSet::new();
        collect_candidate_ipv4(&snapshot, &mut found);
        assert!(found.contains(&"203.0.113.44".parse().unwrap()));
        assert!(found.contains(&"198.51.100.7".parse().unwrap()));
        assert!(!found.contains(&"169.254.10.2".parse().unwrap()));
    }

}
