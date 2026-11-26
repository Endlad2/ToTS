use crate::providers::{Provider, ProviderBootstrap};
use crate::transport::{turn::TurnCredentials, DatagramPath};
use crate::{Error, Result};
use async_trait::async_trait;
use kolibri_net::calls::{parse_connection, ws2_url_from_endpoint, ConversationParams, Ws2ClientInfo, Ws2Signaling};
use kolibri_net::protocol::opcodes;
use kolibri_net::session::{HandshakeConfig, Session, SessionConfig, UserAgent};
use kolibri_net::transport::ClientConfig;
use rmpv::Value as MpValue;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tracing::warn;
use uuid::Uuid;

const MAX_API_HOST: &str = "api.oneme.ru";
const MAX_API_PORT: u16 = 443;
const CALLS_API_URL: &str = "https://calls.okcdn.ru/fb.do";
const CALLS_APPLICATION_KEY: &str = "CNHIJPLGDIHBABABA";
const CALLS_PROTOCOL_VERSION: &str = "5";
const MAX_APP_VERSION: &str = "26.41.0";
const CALLS_CLIENT_VERSION: &str = "1.1";
const HTTP_USER_AGENT: &str = "OKMessages/26.41.0 (14; Android; 360x780 3.0x)";

#[derive(Default)]
pub struct MaxProvider {
    state: Mutex<Option<MaxSessionState>>,
}

struct MaxSessionState {
    session: Session,
    http: reqwest::Client,
    signaling: Option<Ws2Signaling>,
    session_key: Option<String>,
    conversation_id: Option<String>,
    group_id: Option<i64>,
}

impl MaxSessionState {
    async fn cleanup(&mut self) {
        if let Some(signaling) = self.signaling.take() {
            let _ = signaling.hangup("HUNGUP").await;
            signaling.close();
        }

        if let (Some(session_key), Some(conversation_id)) =
            (self.session_key.as_deref(), self.conversation_id.as_deref())
        {
            let _ = calls_api(
                &self.http,
                vec![
                    ("method", "vchat.hangupConversation".to_owned()),
                    ("conversationId", conversation_id.to_owned()),
                    ("session_key", session_key.to_owned()),
                    ("reason", "HUNGUP".to_owned()),
                ],
            )
            .await;
        }

        if let Some(group_id) = self.group_id {
            let payload = mp_map(vec![("chatId", MpValue::from(group_id))]);
            let _ = request_is_ok(&self.session, opcodes::CHAT_LEAVE, payload, "MAX leave group").await;
        }

        self.session.disconnect();
    }
}

#[async_trait]
impl Provider for MaxProvider {
    async fn bootstrap(&self, data: &Value) -> Result<ProviderBootstrap> {
        let mut state_guard = self.state.lock().await;
        if state_guard.is_some() {
            return Err(Error::Provider("MAX provider session is already active".into()));
        }

        let access_token = data
            .get("access_token")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .ok_or_else(|| Error::Provider("provider_data.max.access_token is required".into()))?
            .to_owned();

        let session = Session::new(max_session_config());
        let http = crate::egress::configure_http(reqwest::Client::builder())
            .timeout(Duration::from_secs(20))
            .user_agent(HTTP_USER_AGENT)
            .build()
            .map_err(|_| Error::Provider("MAX HTTP client initialization failed".into()))?;
        let mut state = MaxSessionState {
            session,
            http,
            signaling: None,
            session_key: None,
            conversation_id: None,
            group_id: None,
        };

        let result = build_bootstrap(&mut state, &access_token).await;
        match result {
            Ok(bootstrap) => {
                *state_guard = Some(state);
                Ok(bootstrap)
            }
            Err(error) => {
                drop(state_guard);
                state.cleanup().await;
                Err(error)
            }
        }
    }

    async fn cleanup(&self, _data: &Value, _bootstrap: &ProviderBootstrap) -> Result<()> {
        if let Some(mut state) = self.state.lock().await.take() {
            state.cleanup().await;
        }
        Ok(())
    }

    async fn connect_native(&self, _data: &Value) -> Result<Option<Arc<dyn DatagramPath>>> {
        Ok(None)
    }
}

fn max_session_config() -> SessionConfig {
    let user_agent = UserAgent {
        device_type: "ANDROID".into(),
        app_version: MAX_APP_VERSION.into(),
        os_version: "14".into(),
        timezone: "UTC".into(),
        screen: "360x780 3.0x".into(),
        push_device_type: "ANDROID".into(),
        arch: "arm64".into(),
        locale: "ru".into(),
        build_number: 6931,
        device_name: "Android".into(),
        device_locale: "ru".into(),
        is_pwa: None,
        header_user_agent: None,
    };
    let handshake = HandshakeConfig {
        instance_id: Uuid::new_v4().to_string(),
        device_id: Uuid::new_v4().to_string(),
        client_session_id: now_millis(),
        user_agent,
    };
    let mut config = SessionConfig::new(ClientConfig::new(MAX_API_HOST, MAX_API_PORT), handshake);
    config.auto_reconnect = false;
    config.ping_interactive = true;
    config
}

async fn build_bootstrap(state: &mut MaxSessionState, access_token: &str) -> Result<ProviderBootstrap> {
    state
        .session
        .connect()
        .await
        .map_err(|_| Error::Provider("MAX session handshake failed".into()))?;

    let login = request_json(
        &state.session,
        opcodes::LOGIN,
        mp_map(vec![
            ("interactive", MpValue::from(true)),
            ("token", MpValue::from(access_token)),
            ("chatsSync", MpValue::from(0)),
            ("contactsSync", MpValue::from(0)),
            ("presenceSync", MpValue::from(-1)),
            ("draftsSync", MpValue::from(0)),
            ("chatsCount", MpValue::from(40)),
        ]),
        "MAX login",
    )
    .await?;

    let max_user_id = login
        .pointer("/profile/contact/id")
        .and_then(value_as_string)
        .ok_or_else(|| Error::Provider("MAX login response has no account id".into()))?;

    let call_token_response = request_json(
        &state.session,
        opcodes::OK_TOKEN,
        mp_map(Vec::new()),
        "MAX call token",
    )
    .await?;
    let call_token = call_token_response
        .get("token")
        .or_else(|| call_token_response.get("call_token"))
        .or_else(|| call_token_response.get("callToken"))
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| Error::Provider("MAX did not return a call token".into()))?;

    let call_device_id = Uuid::new_v4().to_string();
    let session_data = json!({
        "auth_token": call_token,
        "version": 3,
        "device_id": call_device_id,
        "client_type": "SDK_JS",
        "client_version": CALLS_CLIENT_VERSION,
    })
    .to_string();
    let auth = calls_api(
        &state.http,
        vec![
            ("method", "auth.anonymLogin".to_owned()),
            ("session_data", session_data),
        ],
    )
    .await?;
    let session_key = auth
        .get("session_key")
        .or_else(|| auth.get("sessionKey"))
        .and_then(value_as_string)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::Provider("MAX call authentication did not return a session key".into()))?;
    let external_user_id = auth
        .get("external_user_id")
        .or_else(|| auth.get("externalUserId"))
        .and_then(value_as_string)
        .unwrap_or(max_user_id);
    state.session_key = Some(session_key.clone());

    // Create a private one-member group. The creator is added by MAX; an empty
    // userIds list avoids inviting or calling any third-party contact.
    let group_name = format!("ToTS-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let cid = (rand::random::<u32>() & 0x7fff_ffff).max(1);
    let group_result = request_json(
        &state.session,
        opcodes::MSG_SEND,
        mp_map(vec![
            (
                "message",
                mp_map(vec![
                    ("text", MpValue::from("")),
                    ("cid", MpValue::from(cid)),
                    ("elements", MpValue::Array(Vec::new())),
                    (
                        "attaches",
                        MpValue::Array(vec![mp_map(vec![
                            ("_type", MpValue::from("CONTROL")),
                            ("event", MpValue::from("new")),
                            ("chatType", MpValue::from("CHAT")),
                            ("title", MpValue::from(group_name.as_str())),
                            ("userIds", MpValue::Array(Vec::new())),
                        ])]),
                    ),
                ]),
            ),
            ("notify", MpValue::from(true)),
        ]),
        "MAX group creation",
    )
    .await;
    let group_id = group_result.ok().and_then(|group| {
        group
            .get("chatId")
            .or_else(|| group.pointer("/chat/id"))
            .or_else(|| group.pointer("/message/chatId"))
            .and_then(value_as_i64)
    });
    let group_created = group_id.is_some();
    state.group_id = group_id;
    if !group_created {
        warn!("MAX did not create a self-only group; continuing with the account call for TURN credentials");
    }

    let conversation_id = Uuid::new_v4().to_string();
    state.conversation_id = Some(conversation_id.clone());
    let call = calls_api(
        &state.http,
        vec![
            ("method", "vchat.startConversation".to_owned()),
            ("conversationId", conversation_id.clone()),
            ("externalIds", external_user_id),
            ("session_key", session_key),
            ("isVideo", "false".to_owned()),
            ("protocolVersion", CALLS_PROTOCOL_VERSION.to_owned()),
            ("payload", json!({"is_video": false}).to_string()),
        ],
    )
    .await?;

    // Attach the active call to the freshly-created group. MAX may reject a
    // self-only group on some account/privacy configurations; the call itself
    // still supplies the TURN/WebRTC session needed by the transport.
    let group_call_response = if let Some(group_id) = group_id {
        let group_call_payload = mp_map(vec![
            ("conversationId", MpValue::from(conversation_id.as_str())),
            ("isVideo", MpValue::from(false)),
            ("internalParams", MpValue::from("")),
            ("chatId", MpValue::from(group_id)),
        ]);
        request_json(
            &state.session,
            opcodes::VIDEO_CHAT_START_ACTIVE,
            group_call_payload,
            "MAX attach call to group",
        )
        .await
        .ok()
    } else {
        None
    };
    let group_call_attached = group_call_response.is_some();
    if !group_call_attached {
        warn!(group_created, "MAX call could not be attached to the temporary group; continuing with the call TURN session");
    }

    let vcp = group_call_response
        .as_ref()
        .and_then(extract_vcp)
        .or_else(|| extract_vcp(&call));
    let endpoint = vcp
        .as_ref()
        .map(|params| params.ws_endpoint.clone())
        .or_else(|| {
            call.get("endpoint")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        })
        .ok_or_else(|| Error::Provider("MAX call response has no WebRTC endpoint".into()))?;
    validate_webrtc_endpoint(&endpoint)?;

    let mut turn = parse_http_turn(&call);
    let mut stun_urls = parse_http_stun(&call);
    let mut signaling_host = None;

    let client_info = Ws2ClientInfo {
        capabilities: "3c03f".into(),
        device: "Android".into(),
        platform: "ANDROID".into(),
        client_type: "ONE_ME".into(),
        app_version: MAX_APP_VERSION.into(),
        os_version: "14".into(),
    };
    let ws2_url = if let Some(params) = vcp.as_ref() {
        params.ws2_url(&conversation_id, &client_info)
    } else {
        ws2_url_from_endpoint(&endpoint, &client_info)
    };
    let signaling_result = tokio::time::timeout(
        Duration::from_secs(18),
        Ws2Signaling::connect(&ws2_url, None),
    )
    .await;
    if let Ok(Ok(signaling)) = signaling_result {
        let mut notifications = signaling.notifications();
        let connection_result = tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                let notification = notifications
                    .recv()
                    .await
                    .map_err(|_| Error::Provider("MAX WebRTC signaling stream closed".into()))?;
                if notification.get("notification").and_then(Value::as_str) == Some("connection") {
                    return Ok(parse_connection(&notification));
                }
                if notification.get("notification").and_then(Value::as_str) == Some("hungup") {
                    return Err(Error::Provider("MAX call ended before WebRTC setup completed".into()));
                }
            }
        })
        .await;
        if let Ok(Ok(connection)) = connection_result {
            let mut signal_turn = Vec::new();
            let mut signal_stun = Vec::new();
            for server in &connection.ice_servers {
                let turn_urls = server
                    .urls
                    .iter()
                    .filter(|url| is_udp_turn_url(url))
                    .cloned()
                    .collect::<Vec<_>>();
                if !turn_urls.is_empty() {
                    if let (Some(username), Some(password)) =
                        (server.username.as_deref(), server.credential.as_deref())
                    {
                        if !username.is_empty() && !password.is_empty() {
                            signal_turn.push(TurnCredentials {
                                urls: turn_urls,
                                username: username.to_owned(),
                                password: password.to_owned(),
                            });
                        }
                    }
                }
                signal_stun.extend(
                    server
                        .urls
                        .iter()
                        .filter(|url| url.starts_with("stun:"))
                        .cloned(),
                );
            }
            signal_stun.sort();
            signal_stun.dedup();
            if !signal_turn.is_empty() {
                turn = signal_turn;
            }
            if !signal_stun.is_empty() {
                stun_urls = signal_stun;
            }
            signaling_host = endpoint_host(&endpoint);
            state.signaling = Some(signaling);
        } else {
            signaling.close();
        }
    }

    if turn.is_empty() {
        return Err(Error::Provider("MAX call did not provide usable UDP TURN credentials".into()));
    }

    let endpoint_host = signaling_host.or_else(|| endpoint_host(&endpoint));
    Ok(ProviderBootstrap {
        turn,
        stun_urls,
        signalling: json!({
            "provider": "max",
            "group_id": group_id,
            "group_name": group_name.clone(),
            "group_created": group_created,
            "conversation_id": conversation_id.clone(),
            "webrtc_endpoint_host": endpoint_host,
            "group_call_attached": group_call_attached,
            "vcp_available": vcp.is_some(),
        }),
    })
}

async fn request_json(
    session: &Session,
    opcode: u16,
    payload: MpValue,
    step: &str,
) -> Result<Value> {
    let bytes = encode_payload(payload, step)?;
    let packet = session
        .request_raw(opcode, &bytes)
        .await
        .map_err(|_| Error::Provider(format!("{step} request failed")))?;
    if !packet.is_ok() {
        return Err(Error::Provider(format!("{step} was rejected by MAX")));
    }
    packet
        .json()
        .map_err(|_| Error::Provider(format!("{step} returned an invalid MessagePack payload")))
}

async fn request_is_ok(
    session: &Session,
    opcode: u16,
    payload: MpValue,
    step: &str,
) -> Result<bool> {
    let bytes = encode_payload(payload, step)?;
    let packet = session
        .request_raw(opcode, &bytes)
        .await
        .map_err(|_| Error::Provider(format!("{step} request failed")))?;
    Ok(packet.is_ok())
}

fn encode_payload(payload: MpValue, step: &str) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    rmpv::encode::write_value(&mut bytes, &payload)
        .map_err(|_| Error::Provider(format!("{step} payload encoding failed")))?;
    Ok(bytes)
}

fn mp_map(fields: Vec<(&str, MpValue)>) -> MpValue {
    MpValue::Map(
        fields
            .into_iter()
            .map(|(key, value)| (MpValue::from(key), value))
            .collect(),
    )
}

async fn calls_api(http: &reqwest::Client, fields: Vec<(&str, String)>) -> Result<Value> {
    let mut form = vec![
        ("format", "JSON".to_owned()),
        ("application_key", CALLS_APPLICATION_KEY.to_owned()),
    ];
    form.extend(fields);
    let response = http
        .post(CALLS_API_URL)
        .form(&form)
        .send()
        .await
        .map_err(|_| Error::Provider("MAX calls API request failed".into()))?;
    if !response.status().is_success() {
        return Err(Error::Provider(format!(
            "MAX calls API returned HTTP {}",
            response.status().as_u16()
        )));
    }
    let value = response
        .json::<Value>()
        .await
        .map_err(|_| Error::Provider("MAX calls API returned invalid JSON".into()))?;
    let explicit_error = value.get("error").is_some_and(|error| {
        !error.is_null()
            && error != &Value::Bool(false)
            && error.as_str().is_none_or(|message| !message.is_empty())
    });
    let nonzero_error_code = value.get("error_code").is_some_and(|code| {
        code.as_i64().is_some_and(|code| code != 0)
            || code.as_str().is_some_and(|code| !code.is_empty() && code != "0")
    });
    if explicit_error || nonzero_error_code {
        return Err(Error::Provider("MAX calls API rejected the request".into()));
    }
    Ok(value.get("response").cloned().unwrap_or(value))
}

fn extract_vcp(value: &Value) -> Option<ConversationParams> {
    let encoded = value
        .get("vcp")
        .or_else(|| value.pointer("/payload/vcp"))
        .or_else(|| value.pointer("/call/vcp"))
        .or_else(|| value.pointer("/conversation/vcp"))
        .and_then(Value::as_str)?;
    ConversationParams::decode(encoded).into_iter().next()
}

fn is_udp_turn_url(url: &str) -> bool {
    url.starts_with("turn:") && !url.to_ascii_lowercase().contains("transport=tcp")
}

fn parse_http_turn(call: &Value) -> Vec<TurnCredentials> {
    let Some(server) = call.get("turn_server") else {
        return Vec::new();
    };
    let urls = json_string_list(server.get("urls"))
        .into_iter()
        .filter(|url| is_udp_turn_url(url))
        .collect::<Vec<_>>();
    let username = server.get("username").and_then(Value::as_str).unwrap_or("");
    let password = server
        .get("credential")
        .or_else(|| server.get("password"))
        .and_then(Value::as_str)
        .unwrap_or("");
    if urls.is_empty() || username.is_empty() || password.is_empty() {
        return Vec::new();
    }
    vec![TurnCredentials {
        urls,
        username: username.to_owned(),
        password: password.to_owned(),
    }]
}

fn parse_http_stun(call: &Value) -> Vec<String> {
    let Some(server) = call.get("stun_server") else {
        return Vec::new();
    };
    let urls = server
        .get("urls")
        .map(|value| json_string_list(Some(value)))
        .unwrap_or_else(|| json_string_list(Some(server)));
    urls.into_iter()
        .filter(|url| url.starts_with("stun:"))
        .collect()
}

fn json_string_list(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::String(value)) if !value.is_empty() => vec![value.clone()],
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

fn validate_webrtc_endpoint(endpoint: &str) -> Result<()> {
    let url = url::Url::parse(endpoint)
        .map_err(|_| Error::Provider("MAX WebRTC endpoint is malformed".into()))?;
    let trusted_host = url
        .host_str()
        .is_some_and(|host| host == "okcdn.ru" || host.ends_with(".okcdn.ru"));
    if url.scheme() != "wss" || !trusted_host {
        return Err(Error::Provider("MAX returned an unexpected WebRTC endpoint".into()));
    }
    Ok(())
}

fn endpoint_host(endpoint: &str) -> Option<String> {
    let url = url::Url::parse(endpoint).ok()?;
    Some(format!("{}://{}", url.scheme(), url.host_str()?))
}

fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turn_parser_keeps_only_udp_turn_urls() {
        let call = json!({
            "turn_server": {
                "urls": [
                    "turn:relay.example.net:3478?transport=udp",
                    "turn:relay.example.net:3478?transport=tcp",
                    "turns:relay.example.net:5349?transport=tcp",
                    "stun:relay.example.net:3478"
                ],
                "username": "user",
                "credential": "secret"
            }
        });
        let turn = parse_http_turn(&call);
        assert_eq!(turn.len(), 1);
        assert_eq!(turn[0].urls.len(), 1);
        assert!(turn[0].urls[0].starts_with("turn:"));
    }

    #[test]
    fn endpoint_validation_rejects_untrusted_hosts() {
        assert!(validate_webrtc_endpoint("wss://videowebrtc.okcdn.ru/ws2").is_ok());
        assert!(validate_webrtc_endpoint("wss://example.com/ws2").is_err());
        assert!(validate_webrtc_endpoint("https://videowebrtc.okcdn.ru/ws2").is_err());
    }
}
