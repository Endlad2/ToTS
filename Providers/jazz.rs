use async_trait::async_trait;
use jazz_session::{CallSession, JazzDataPlane};
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::Duration,
};
use tokio::{net::lookup_host, sync::Mutex};
use url::Url;

use crate::{
    providers::{Provider, ProviderBootstrap},
    transport::path::DatagramPath,
    Error, Result,
};

const READY_TIMEOUT: Duration = Duration::from_secs(20);
const KEEPALIVE: &[u8] = b"ToTS/Jazz transport keepalive v1";

fn is_beacon(data: &[u8]) -> bool {
    data == KEEPALIVE
}

#[derive(Default)]
pub struct JazzProvider;

#[derive(Deserialize)]
struct JazzData {
    join_link: String,
    #[serde(default = "client_name")]
    name: String,
    connector_url: Option<String>,
}

fn client_name() -> String {
    "ToTS client".into()
}

fn parse_data(data: &Value) -> Result<JazzData> {
    let parsed: JazzData = serde_json::from_value(data.clone())
        .map_err(|_| Error::Provider("invalid Jazz provider_data".into()))?;
    if parsed.join_link.trim().is_empty()
        || parsed.name.trim().is_empty()
        || parsed.name.chars().count() > 128
        || parsed.name.chars().any(char::is_control)
        || parsed
            .connector_url
            .as_ref()
            .is_some_and(|url| url.trim().is_empty())
    {
        return Err(Error::Provider("invalid Jazz provider_data".into()));
    }
    Ok(parsed)
}

#[async_trait]
impl Provider for JazzProvider {
    async fn bootstrap(&self, _data: &Value) -> Result<ProviderBootstrap> {
        Ok(ProviderBootstrap::default())
    }

    async fn connect_native(&self, data: &Value) -> Result<Option<Arc<dyn DatagramPath>>> {
        let data = parse_data(data)?;
        let session = match data.connector_url {
            Some(connector) => {
                jazz_session::join_preconnected(&data.join_link, &data.name, &connector).await
            }
            None => jazz_session::join_call(&data.join_link, &data.name).await,
        }
        .map_err(|error| Error::Provider(format!("Jazz session join failed: {error}")))?;
        Ok(Some(JazzPath::connect(session, true).await?))
    }
}

pub struct JazzRoom {
    pub join_link: String,
    pub path: Arc<dyn DatagramPath>,
}

pub async fn create_room() -> Result<JazzRoom> {
    let session = jazz_session::create_call("ToTS server")
        .await
        .map_err(|error| Error::Provider(format!("Jazz room creation failed: {error}")))?;
    server_room(session).await
}

pub async fn join_room(link: &str) -> Result<JazzRoom> {
    let session = jazz_session::join_call(link, "ToTS server")
        .await
        .map_err(|error| Error::Provider(format!("Jazz server session join failed: {error}")))?;
    server_room(session).await
}

async fn server_room(session: CallSession) -> Result<JazzRoom> {
    let join_link = session.join_url().to_owned();
    let path = JazzPath::connect(session, false).await?;
    Ok(JazzRoom { join_link, path })
}

/// Returns a secret connector URL for client provisioning.
pub async fn preconnect(link: &str) -> Result<String> {
    jazz_session::preconnect_call(link)
        .await
        .map_err(|error| Error::Provider(format!("Jazz client provisioning failed: {error}")))
}

struct JazzPath {
    session: Mutex<Option<CallSession>>,
    plane: JazzDataPlane,
    route_ips: Vec<Ipv4Addr>,
    keepalive: tokio::task::JoinHandle<()>,
}

impl JazzPath {
    async fn connect(session: CallSession, receive: bool) -> Result<Arc<dyn DatagramPath>> {
        let mut keepalive = None;
        let result = async {
            let plane = session
                .data_plane()
                .await
                .map_err(|_| Error::Provider("Jazz media initialization failed".into()))?;
            plane
                .wait_ready(READY_TIMEOUT)
                .await
                .map_err(|_| Error::Provider("Jazz media connection timed out".into()))?;
            let sender = plane.clone();
            keepalive = Some(tokio::spawn(async move {
                let mut ticks = tokio::time::interval(Duration::from_secs(1));
                loop {
                    ticks.tick().await;
                    if sender.send(KEEPALIVE).await.is_err() {
                        break;
                    }
                }
            }));
            if receive {
                plane.wait_receive_ready(READY_TIMEOUT).await.map_err(|_| {
                    Error::Provider("Jazz remote media subscription timed out".into())
                })?;
            }
            let route_ips = collect_route_ips(&session.json().await, session.join_url()).await?;
            Ok((plane, route_ips))
        }
        .await;
        match result {
            Ok((plane, route_ips)) => Ok(Arc::new(Self {
                session: Mutex::new(Some(session)),
                plane,
                route_ips,
                keepalive: keepalive.expect("keepalive started after media readiness"),
            })),
            Err(error) => {
                if let Some(keepalive) = keepalive {
                    keepalive.abort();
                }
                let _ = session.close().await;
                Err(error)
            }
        }
    }
}

impl Drop for JazzPath {
    fn drop(&mut self) {
        self.keepalive.abort();
    }
}

#[async_trait]
impl DatagramPath for JazzPath {
    fn name(&self) -> &str {
        "jazz-rtp"
    }

    fn route_ips(&self) -> Vec<Ipv4Addr> {
        self.route_ips.clone()
    }

    async fn send(&self, data: &[u8]) -> Result<()> {
        self.plane
            .send(data)
            .await
            .map_err(|_| Error::Provider("Jazz media send failed".into()))
    }

    async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        loop {
            let data = self
                .plane
                .recv()
                .await
                .map_err(|_| Error::Provider("Jazz media receive failed".into()))?;
            if is_beacon(&data) {
                continue;
            }
            if data.len() > buf.len() {
                return Err(Error::Provider(
                    "Jazz datagram exceeds receive buffer".into(),
                ));
            }
            buf[..data.len()].copy_from_slice(&data);
            return Ok(data.len());
        }
    }

    async fn close(&self) -> Result<()> {
        self.keepalive.abort();
        let session = self.session.lock().await.take();
        if let Some(session) = session {
            session
                .close()
                .await
                .map_err(|_| Error::Provider("Jazz session close failed".into()))?;
        }
        Ok(())
    }
}

async fn collect_route_ips(snapshot: &Value, join_link: &str) -> Result<Vec<Ipv4Addr>> {
    let (mut ips, endpoints) = route_sources(snapshot, join_link);
    for (host, port) in endpoints {
        if let Ok(ip) = host.parse::<Ipv4Addr>() {
            if is_public_ipv4(ip) {
                ips.insert(ip);
            }
            continue;
        }
        let addresses =
            tokio::time::timeout(Duration::from_secs(5), lookup_host((host.as_str(), port)))
                .await
                .map_err(|_| Error::Provider("Jazz endpoint DNS resolution timed out".into()))?
                .map_err(|_| Error::Provider("Jazz endpoint DNS resolution failed".into()))?;
        for address in addresses {
            if let IpAddr::V4(ip) = address.ip() {
                if is_public_ipv4(ip) {
                    ips.insert(ip);
                }
            }
        }
    }
    Ok(ips.into_iter().collect())
}

fn route_sources(
    snapshot: &Value,
    join_link: &str,
) -> (BTreeSet<Ipv4Addr>, BTreeSet<(String, u16)>) {
    let mut ips = BTreeSet::new();
    let mut endpoints = BTreeSet::new();
    let mut values = vec![snapshot];
    while let Some(value) = values.pop() {
        match value {
            Value::Array(items) => values.extend(items),
            Value::Object(items) => values.extend(items.values()),
            Value::String(value) => add_route_source(value, &mut ips, &mut endpoints),
            _ => {}
        }
    }
    add_route_source(join_link, &mut ips, &mut endpoints);
    (ips, endpoints)
}

fn add_route_source(
    value: &str,
    ips: &mut BTreeSet<Ipv4Addr>,
    endpoints: &mut BTreeSet<(String, u16)>,
) {
    if let Some((scheme, rest)) = value.split_once(':') {
        let default_port = match scheme.to_ascii_lowercase().as_str() {
            "https" | "wss" => Some(443),
            "turn" | "stun" => Some(3478),
            "turns" | "stuns" => Some(5349),
            _ => None,
        };
        if let Some(default_port) = default_port {
            let normalized = format!("{scheme}://{}", rest.trim_start_matches('/'));
            if let Ok(url) = Url::parse(&normalized) {
                if let Some(host) = url.host_str() {
                    if !matches!(url.host(), Some(url::Host::Ipv6(_)))
                        && url.username().is_empty()
                        && url.password().is_none()
                    {
                        endpoints.insert((host.to_owned(), url.port().unwrap_or(default_port)));
                    }
                }
            }
        }
    }
    for line in value.lines().map(str::trim) {
        let candidate = line
            .strip_prefix("a=candidate:")
            .or_else(|| line.strip_prefix("candidate:"));
        let fields: Vec<_> = line.split_whitespace().collect();
        let address = if candidate.is_some() {
            fields.get(4).copied()
        } else if line.starts_with("c=IN IP4 ") {
            fields.get(2).copied()
        } else {
            None
        };
        for address in address.into_iter().chain(
            fields
                .windows(2)
                .filter(|fields| candidate.is_some() && fields[0] == "raddr")
                .map(|fields| fields[1]),
        ) {
            if let Ok(ip) = address.parse::<Ipv4Addr>() {
                if is_public_ipv4(ip) {
                    ips.insert(ip);
                }
            }
        }
    }
}

fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, _, _] = ip.octets();
    !ip.is_private()
        && !ip.is_loopback()
        && !ip.is_link_local()
        && !ip.is_documentation()
        && a != 0
        && a < 224
        && !(a == 100 && (64..=127).contains(&b))
        && !(a == 198 && (18..=19).contains(&b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn only_the_exact_public_keepalive_is_filtered() {
        assert!(is_beacon(b"ToTS/Jazz transport keepalive v1"));
        assert!(!is_beacon(b"ToTS/Jazz transport keepalive"));
        assert!(!is_beacon(b"ToTS/Jazz transport keepalive v1\0"));
        assert!(!is_beacon(b"encrypted ToTS packet"));
    }

    #[test]
    fn provider_data_requires_a_link_and_rejects_invalid_field_types() {
        for data in [
            json!({}),
            json!({"join_link": ""}),
            json!({"join_link": "secret", "name": 42}),
            json!({"join_link": "secret", "name": "  "}),
            json!({"join_link": "secret", "connector_url": true}),
            json!({"join_link": "secret", "connector_url": ""}),
        ] {
            let error = parse_data(&data).err().expect("invalid data accepted");
            assert!(matches!(error, Error::Provider(_)));
            assert!(!error.to_string().contains("secret"));
        }
        let data =
            parse_data(&json!({"join_link": "https://salutejazz.ru/calls/example?psw=secret"}))
                .unwrap();
        assert_eq!(data.name, "ToTS client");
        assert!(data.connector_url.is_none());
    }

    #[test]
    fn route_sources_include_public_candidates_and_all_provider_url_forms() {
        let snapshot = json!({
            "call": {"connector_url": "wss://ws.example/connect?token=secret"},
            "webrtc": {
                "ice_servers": [{"urls": [
                    "turn:relay.example:3478?transport=udp",
                    "turns:relay.example:5349?transport=tcp",
                    "stun:stun.example",
                    "stuns://stun.example",
                    "turn:1.0.0.1:3478?transport=tcp"
                ]}],
                "publisher": {"local_description": {"sdp": concat!(
                    "v=0\r\nc=IN IP4 8.8.8.8\r\n",
                    "a=candidate:1 1 udp 123 192.168.1.2 1234 typ host\r\n",
                    "a=candidate:2 1 udp 123 1.1.1.1 1234 typ srflx raddr 192.168.1.2 rport 1234\r\n"
                )}},
                "subscriber": {"remote_ice_candidates": [
                    {"candidate": "candidate:3 1 udp 123 9.9.9.9 443 typ relay raddr 8.8.8.8 rport 12"},
                    {"candidate": "candidate:4 1 udp 123 2001:4860:4860::8888 443 typ host"}
                ]}
            }
        });
        let (ips, endpoints) = route_sources(&snapshot, "https://join.example/calls/id?psw=secret");
        assert_eq!(
            ips,
            ["1.1.1.1", "8.8.8.8", "9.9.9.9"]
                .into_iter()
                .map(|ip| ip.parse().unwrap())
                .collect()
        );
        assert_eq!(
            endpoints,
            [
                ("1.0.0.1", 3478),
                ("join.example", 443),
                ("relay.example", 3478),
                ("relay.example", 5349),
                ("stun.example", 3478),
                ("stun.example", 5349),
                ("ws.example", 443),
            ]
            .into_iter()
            .map(|(host, port)| (host.to_owned(), port))
            .collect()
        );
    }

    #[test]
    fn public_candidate_filter_excludes_local_and_reserved_addresses() {
        for ip in [
            "0.1.2.3",
            "10.0.0.1",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.1.1",
            "172.16.0.1",
            "192.168.1.1",
            "192.0.2.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
        ] {
            assert!(!is_public_ipv4(ip.parse().unwrap()), "accepted {ip}");
        }
        assert!(is_public_ipv4("178.248.233.18".parse().unwrap()));
    }
}
