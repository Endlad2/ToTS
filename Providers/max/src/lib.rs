use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};
use tots_core::{RelayScope, TurnCredentials, TurnProvider, TurnServer};
use tracing::{debug, warn};
use uuid::Uuid;

const ONEME_WS_URL: &str = "wss://ws-api.oneme.ru/websocket";
const MAX_CALLS_API: &str = "https://calls.okcdn.ru/fb.do";
const MAX_ORIGIN: &str = "https://web.max.ru";
const MAX_API_KEY: &str = "CNHIJPLGDIHBABABA";
const MAX_APP_VERSION: &str = "26.10.3";
const MAX_UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MaxProviderData {
    /// MAX web access token (`__oneme_auth` value obtained by the user).
    #[serde(default, alias = "token")]
    pub access_token: String,
    /// Group/chat id in which the call is created.
    #[serde(default, alias = "chat_id", alias = "max_group_id")]
    pub group_id: String,
    /// Optional existing Calls conversation id. When set, no startConversation
    /// request is made and credentials are requested for this conversation.
    #[serde(default)]
    pub conversation_id: Option<String>,

    /// Optional direct TURN override for diagnostics/offline tests.
    #[serde(default)]
    pub turn_username: Option<String>,
    #[serde(default)]
    pub turn_password: Option<String>,
    #[serde(default)]
    pub turn_urls: Option<Vec<String>>,
}

pub struct MaxProvider {
    data: MaxProviderData,
    http: Client,
}

#[derive(Debug, Clone)]
struct CallsSession {
    session_key: String,
}

impl MaxProvider {
    pub fn new(data: MaxProviderData) -> Result<Self> {
        let has_override = data.turn_username.is_some()
            && data.turn_password.is_some()
            && data.turn_urls.as_ref().is_some_and(|urls| !urls.is_empty());
        if !has_override && data.access_token.trim().is_empty() {
            bail!("MAX provider_data.access_token is required unless a TURN override is supplied");
        }
        if !has_override
            && data.conversation_id.as_deref().unwrap_or_default().trim().is_empty()
            && data.group_id.trim().is_empty()
        {
            bail!("MAX provider_data.group_id is required when conversation_id is not supplied");
        }

        let http = Client::builder()
            .cookie_store(true)
            .user_agent(MAX_UA)
            .timeout(Duration::from_secs(20))
            .build()
            .context("build MAX HTTP client")?;
        Ok(Self { data, http })
    }

    async fn get_via_max(&self) -> Result<TurnCredentials> {
        let conversation_id = if let Some(id) = self
            .data
            .conversation_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
        {
            id.to_string()
        } else {
            self.start_conversation().await?
        };

        // A fresh Calls session mirrors the web client/account flow and keeps
        // credential issuance independent from the creator session.
        let credential_session = self.open_calls_session().await?;
        let value = self
            .post_calls(vec![
                ("conversationId", conversation_id),
                ("method", "vchat.getConversationParams".to_string()),
                ("format", "JSON".to_string()),
                ("application_key", MAX_API_KEY.to_string()),
                ("session_key", credential_session.session_key),
            ])
            .await
            .context("MAX vchat.getConversationParams")?;
        ensure_api_ok(&value, "MAX vchat.getConversationParams")?;
        parse_turn_credentials(&value).context("parse MAX TURN credentials")
    }

    async fn start_conversation(&self) -> Result<String> {
        let creator = self.open_calls_session().await?;

        let info = self
            .post_calls(vec![
                ("method", "system.getInfo".to_string()),
                ("format", "JSON".to_string()),
                ("application_key", MAX_API_KEY.to_string()),
                ("session_key", creator.session_key.clone()),
            ])
            .await
            .context("MAX system.getInfo")?;
        ensure_api_ok(&info, "MAX system.getInfo")?;

        let proposed = Uuid::new_v4().to_string();
        let payload = json!({
            "is_video": false,
            "chat_id": self.data.group_id.trim()
        })
        .to_string();

        let started = self
            .post_calls(vec![
                ("conversationId", proposed.clone()),
                ("isVideo", "false".to_string()),
                ("protocolVersion", "5".to_string()),
                ("payload", payload),
                ("method", "vchat.startConversation".to_string()),
                ("format", "JSON".to_string()),
                ("application_key", MAX_API_KEY.to_string()),
                ("session_key", creator.session_key),
            ])
            .await;

        match started {
            Ok(value) => {
                if let Err(error) = ensure_api_ok(&value, "MAX vchat.startConversation") {
                    // Some MAX deployments still expose conversation parameters
                    // under the client-generated UUID even when starting the call
                    // reports a detailed API error. Preserve that interoperability
                    // behavior while making it visible in logs.
                    warn!(%error, "MAX startConversation returned an API error; trying generated conversation id");
                    return Ok(proposed);
                }
                let id = value
                    .get("id")
                    .or_else(|| value.get("conversationId"))
                    .and_then(Value::as_str)
                    .filter(|id| !id.trim().is_empty())
                    .unwrap_or(&proposed)
                    .to_string();
                debug!(conversation_id = %id, "MAX call conversation created");
                Ok(id)
            }
            Err(error) => {
                warn!(%error, "MAX startConversation request failed; trying generated conversation id");
                Ok(proposed)
            }
        }
    }

    async fn open_calls_session(&self) -> Result<CallsSession> {
        let call_token = fetch_call_token(&self.data.access_token).await?;
        let session_data = json!({
            "auth_token": call_token,
            "client_type": "SDK_JS",
            "client_version": "1.1",
            "device_id": Uuid::new_v4().to_string(),
            "version": 3
        })
        .to_string();

        let value = self
            .post_calls(vec![
                ("session_data", session_data),
                ("method", "auth.anonymLogin".to_string()),
                ("format", "JSON".to_string()),
                ("application_key", MAX_API_KEY.to_string()),
            ])
            .await
            .context("MAX auth.anonymLogin")?;
        ensure_api_ok(&value, "MAX auth.anonymLogin")?;
        let session_key = value
            .get("session_key")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("MAX auth.anonymLogin response has no session_key"))?
            .to_string();
        Ok(CallsSession { session_key })
    }

    async fn post_calls(&self, form: Vec<(&str, String)>) -> Result<Value> {
        let response = self
            .http
            .post(MAX_CALLS_API)
            .header("Accept", "*/*")
            .header("Origin", MAX_ORIGIN)
            .header("Referer", format!("{MAX_ORIGIN}/"))
            .form(&form)
            .send()
            .await
            .context("MAX Calls HTTP request failed")?;
        parse_json(response).await
    }

    fn override_credentials(&self) -> Result<Option<TurnCredentials>> {
        let (Some(username), Some(password), Some(urls)) = (
            &self.data.turn_username,
            &self.data.turn_password,
            &self.data.turn_urls,
        ) else {
            return Ok(None);
        };
        let servers: Vec<_> = urls.iter().filter_map(|url| parse_turn_url(url)).collect();
        if servers.is_empty() {
            bail!("MAX turn_urls override contains no usable UDP TURN URL");
        }
        Ok(Some(TurnCredentials {
            username: username.clone(),
            password: password.clone(),
            servers,
        }))
    }
}

#[async_trait]
impl TurnProvider for MaxProvider {
    fn name(&self) -> &'static str {
        "max"
    }

    fn relay_scope(&self) -> RelayScope {
        // VK/MAX Calls credentials are used by the reference flow as ordinary
        // RFC TURN allocations to the ToTS peer. Keep them in the generic TURN
        // path; Jazz remains provider-session scoped because its TURN service
        // rejects arbitrary peer permissions.
        RelayScope::GenericTurn
    }

    async fn get_credentials(&self) -> Result<TurnCredentials> {
        if let Some(creds) = self.override_credentials()? {
            return Ok(creds);
        }
        self.get_via_max().await
    }

    fn route_exception_hosts(&self) -> Vec<(String, u16)> {
        vec![
            ("ws-api.oneme.ru".to_string(), 443),
            ("web.max.ru".to_string(), 443),
            ("calls.okcdn.ru".to_string(), 443),
        ]
    }
}

async fn fetch_call_token(access_token: &str) -> Result<String> {
    let mut request = ONEME_WS_URL
        .into_client_request()
        .context("build MAX WebSocket request")?;
    request
        .headers_mut()
        .insert("Origin", MAX_ORIGIN.parse().context("MAX Origin header")?);
    request
        .headers_mut()
        .insert("User-Agent", MAX_UA.parse().context("MAX User-Agent header")?);

    let (stream, _) = tokio::time::timeout(Duration::from_secs(10), connect_async(request))
        .await
        .context("MAX WebSocket connect timeout")?
        .context("connect MAX WebSocket")?;
    let (mut writer, mut reader) = stream.split();

    let init = json!({
        "ver": 11,
        "cmd": 0,
        "seq": 1,
        "opcode": 6,
        "payload": {
            "deviceId": Uuid::new_v4().to_string(),
            "userAgent": {
                "deviceType": "WEB",
                "pushDeviceType": "WEBPUSH",
                "locale": "ru",
                "deviceLocale": "ru",
                "osVersion": "Windows",
                "deviceName": "Chrome",
                "headerUserAgent": MAX_UA,
                "appVersion": MAX_APP_VERSION,
                "screen": "1920x1080 1.0x",
                "timezone": "Europe/Moscow"
            }
        }
    });
    writer
        .send(Message::Text(init.to_string().into()))
        .await
        .context("send MAX INIT")?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut seq = 1u64;
    loop {
        let item = tokio::time::timeout_at(deadline, reader.next())
            .await
            .context("MAX WebSocket response timeout")?
            .ok_or_else(|| anyhow!("MAX WebSocket closed before call token"))??;

        let text = match item {
            Message::Text(text) => text,
            Message::Close(frame) => {
                bail!("MAX WebSocket closed before call token: {frame:?}");
            }
            _ => continue,
        };
        let value: Value = serde_json::from_str(text.as_ref())
            .context("decode MAX WebSocket JSON")?;
        match value.get("cmd").and_then(Value::as_i64) {
            Some(3) => bail!("MAX WebSocket protocol error: {:?}", value.get("payload")),
            Some(1) => {}
            _ => continue,
        }

        match value.get("opcode").and_then(Value::as_i64) {
            Some(6) => {
                seq += 1;
                let login = json!({
                    "ver": 11,
                    "cmd": 0,
                    "seq": seq,
                    "opcode": 19,
                    "payload": {
                        "token": access_token,
                        "interactive": true,
                        "chatsCount": 0,
                        "chatsSync": 0,
                        "contactsSync": 0,
                        "presenceSync": 0,
                        "draftsSync": 0
                    }
                });
                writer
                    .send(Message::Text(login.to_string().into()))
                    .await
                    .context("send MAX LOGIN")?;
            }
            Some(19) => {
                seq += 1;
                let token_request = json!({
                    "ver": 11,
                    "cmd": 0,
                    "seq": seq,
                    "opcode": 158,
                    "payload": {}
                });
                writer
                    .send(Message::Text(token_request.to_string().into()))
                    .await
                    .context("request MAX call token")?;
            }
            Some(158) => {
                return value
                    .pointer("/payload/token")
                    .and_then(Value::as_str)
                    .filter(|token| !token.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| anyhow!("MAX opcode 158 response has no payload.token"));
            }
            _ => {}
        }
    }
}

async fn parse_json(response: Response) -> Result<Value> {
    let status = response.status();
    let text = response.text().await.context("read MAX HTTP body")?;
    if !status.is_success() {
        bail!("MAX HTTP status {status}: {text}");
    }
    serde_json::from_str(&text).with_context(|| format!("decode MAX JSON: {text}"))
}

fn ensure_api_ok(value: &Value, action: &str) -> Result<()> {
    if let Some(error) = value.get("error") {
        if !error.is_null() && error != &Value::Bool(false) {
            bail!("{action} error: {error}");
        }
    }
    if let Some(code) = value.get("error_code") {
        let nonzero = match code {
            Value::Number(number) => number.as_i64().unwrap_or_default() != 0,
            Value::String(text) => text != "0" && !text.is_empty(),
            _ => false,
        };
        if nonzero {
            bail!("{action} error_code={code}: {}", value.get("error_msg").and_then(Value::as_str).unwrap_or("API error"));
        }
    }
    Ok(())
}

fn parse_turn_credentials(value: &Value) -> Result<TurnCredentials> {
    let turn = value
        .get("turn_server")
        .or_else(|| value.pointer("/response/turn_server"))
        .or_else(|| value.get("turn_servers").and_then(Value::as_array).and_then(|items| items.first()))
        .or_else(|| value.get("iceServers").and_then(Value::as_array).and_then(|items| items.first()))
        .ok_or_else(|| anyhow!("MAX response has no TURN/ICE server"))?;
    let username = turn
        .get("username")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("MAX turn_server.username missing"))?
        .to_string();
    let password = turn
        .get("credential")
        .or_else(|| turn.get("password"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("MAX turn_server credential missing"))?
        .to_string();

    let mut servers = Vec::new();
    if let Some(urls) = turn.get("urls").and_then(Value::as_array) {
        servers.extend(urls.iter().filter_map(Value::as_str).filter_map(parse_turn_url));
    } else if let Some(url) = turn.get("urls").and_then(Value::as_str) {
        if let Some(server) = parse_turn_url(url) {
            servers.push(server);
        }
    } else if let Some(url) = turn.get("url").and_then(Value::as_str) {
        if let Some(server) = parse_turn_url(url) {
            servers.push(server);
        }
    }

    if servers.is_empty() {
        bail!("MAX returned no UDP TURN URLs usable by this build");
    }
    debug!(count = servers.len(), "MAX TURN/ICE credentials acquired");
    Ok(TurnCredentials {
        username,
        password,
        servers,
    })
}

fn parse_turn_url(raw: &str) -> Option<TurnServer> {
    let raw = raw.trim();
    if raw.starts_with("turns:") {
        return None;
    }
    let rest = raw.strip_prefix("turn:")?;
    let (authority, query) = rest.split_once('?').unwrap_or((rest, ""));
    if !query.is_empty() && !query.split('&').any(|part| part == "transport=udp") {
        return None;
    }

    if let Some(ipv6) = authority.strip_prefix('[') {
        let end = ipv6.find(']')?;
        let host = &ipv6[..end];
        let tail = &ipv6[end + 1..];
        let port = tail.strip_prefix(':')?.parse().ok()?;
        return Some(TurnServer {
            host: host.to_string(),
            port,
        });
    }

    let (host, port) = authority.rsplit_once(':')?;
    Some(TurnServer {
        host: host.to_string(),
        port: port.parse().ok()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_udp_turn_only() {
        assert!(parse_turn_url("turn:relay.example:3478?transport=udp").is_some());
        assert!(parse_turn_url("turn:relay.example:3478?transport=tcp").is_none());
        assert!(parse_turn_url("turns:relay.example:443?transport=tcp").is_none());
    }
}
