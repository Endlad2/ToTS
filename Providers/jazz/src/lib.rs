use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use jazz_session::{create_call, join_call, CallSession};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;
use tots_core::{TurnCredentials, TurnProvider, TurnServer};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JazzProviderData {
    /// Optional existing SaluteJazz invite URL. When omitted, the provider
    /// creates a temporary guest call and keeps it alive while ToTS is running.
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
    "ToTS relay".to_string()
}

pub struct JazzProvider {
    data: JazzProviderData,
    session: Mutex<Option<CallSession>>,
}

impl JazzProvider {
    pub fn new(mut data: JazzProviderData) -> Result<Self> {
        data.name = data.name.trim().to_string();
        if data.name.is_empty() {
            bail!("Jazz provider_data.name must not be empty");
        }
        Ok(Self {
            data,
            session: Mutex::new(None),
        })
    }

    async fn acquire(&self) -> Result<TurnCredentials> {
        let session = if self.data.join_link.trim().is_empty() {
            create_call(&self.data.name)
                .await
                .context("create SaluteJazz guest call")?
        } else {
            join_call(self.data.join_link.trim(), &self.data.name)
                .await
                .context("join SaluteJazz guest call")?
        };

        let snapshot = session.json().await;
        let credentials = parse_turn_credentials(&snapshot)?;

        // Keep the Jazz guest session alive. The service can rotate or revoke
        // credentials together with the signaling/media session lifecycle.
        let mut slot = self.session.lock().await;
        *slot = Some(session);
        Ok(credentials)
    }
}

#[async_trait]
impl TurnProvider for JazzProvider {
    fn name(&self) -> &'static str {
        "jazz"
    }

    async fn get_credentials(&self) -> Result<TurnCredentials> {
        self.acquire().await
    }
}

fn parse_turn_credentials(snapshot: &Value) -> Result<TurnCredentials> {
    let entries = snapshot
        .pointer("/webrtc/ice_servers")
        .and_then(Value::as_array)
        .context("Jazz session did not expose ICE servers")?;

    let mut selected_username: Option<String> = None;
    let mut selected_password: Option<String> = None;
    let mut servers = Vec::new();

    for entry in entries {
        let Some(username) = entry.get("username").and_then(Value::as_str) else {
            continue;
        };
        let Some(password) = entry.get("credential").and_then(Value::as_str) else {
            continue;
        };
        if username.is_empty() || password.is_empty() {
            continue;
        }

        match (&selected_username, &selected_password) {
            (None, None) => {
                selected_username = Some(username.to_string());
                selected_password = Some(password.to_string());
            }
            (Some(u), Some(p)) if u == username && p == password => {}
            _ => continue,
        }

        for raw in ice_urls(entry) {
            if let Some(server) = parse_udp_turn_url(raw) {
                if !servers.iter().any(|existing: &TurnServer| {
                    existing.host == server.host && existing.port == server.port
                }) {
                    servers.push(server);
                }
            }
        }
    }

    if servers.is_empty() {
        bail!("Jazz returned no UDP TURN endpoints usable by ToTS");
    }

    Ok(TurnCredentials {
        username: selected_username.context("Jazz TURN username is missing")?,
        password: selected_password.context("Jazz TURN credential is missing")?,
        servers,
    })
}

fn ice_urls(entry: &Value) -> Vec<&str> {
    let mut result = Vec::new();
    if let Some(url) = entry.get("url").and_then(Value::as_str) {
        result.push(url);
    }
    match entry.get("urls") {
        Some(Value::String(url)) => result.push(url),
        Some(Value::Array(urls)) => {
            result.extend(urls.iter().filter_map(Value::as_str));
        }
        _ => {}
    }
    result
}

fn parse_udp_turn_url(raw: &str) -> Option<TurnServer> {
    let raw = raw.trim();
    if raw.starts_with("turns:") {
        return None;
    }
    let rest = raw.strip_prefix("turn:")?;
    let (authority, query) = rest.split_once('?').unwrap_or((rest, ""));
    if !query.is_empty() {
        let transport = query
            .split('&')
            .find_map(|part| part.strip_prefix("transport="));
        if transport.is_some_and(|value| !value.eq_ignore_ascii_case("udp")) {
            return None;
        }
    }

    if let Some(ipv6) = authority.strip_prefix('[') {
        let end = ipv6.find(']')?;
        let host = &ipv6[..end];
        let tail = &ipv6[end + 1..];
        let port = if tail.is_empty() {
            3478
        } else {
            tail.strip_prefix(':')?.parse().ok()?
        };
        return Some(TurnServer {
            host: host.to_string(),
            port,
        });
    }

    if let Some((host, port)) = authority.rsplit_once(':') {
        if !host.is_empty() {
            if let Ok(port) = port.parse() {
                return Some(TurnServer {
                    host: host.to_string(),
                    port,
                });
            }
        }
    }

    if authority.is_empty() {
        None
    } else {
        Some(TurnServer {
            host: authority.to_string(),
            port: 3478,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_jazz_turn_configuration() {
        let snapshot = json!({
            "webrtc": {
                "ice_servers": [{
                    "urls": [
                        "stun:stun.example.test:3478",
                        "turn:relay.example.test:3478?transport=udp",
                        "turn:relay.example.test:3478?transport=tcp",
                        "turns:relay.example.test:5349?transport=tcp"
                    ],
                    "username": "u",
                    "credential": "p"
                }]
            }
        });
        let creds = parse_turn_credentials(&snapshot).unwrap();
        assert_eq!(creds.username, "u");
        assert_eq!(creds.password, "p");
        assert_eq!(creds.servers.len(), 1);
        assert_eq!(creds.servers[0].host, "relay.example.test");
        assert_eq!(creds.servers[0].port, 3478);
    }
}
