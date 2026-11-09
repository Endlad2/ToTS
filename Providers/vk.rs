use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;
use url::Url;
use uuid::Uuid;

use crate::{
    providers::{IceServer, Provider, ProviderBootstrap},
    transport::turn::TurnCredentials,
    Error, Result,
};

const VK_CALLS_API: &str = "https://api.vk.me/method";
const VK_API: &str = "https://api.vk.ru/method";
const OK_CALLS: &str = "https://calls.okcdn.ru/fb.do";
const VK_CONNECT_CLIENT_ID: &str = "8093730";
const OK_APPLICATION_KEY: &str = "CGMMEJLGDIHBABABA";

#[derive(Default)]
pub struct VkProvider;

#[derive(Debug, Deserialize)]
struct VkData {
    #[serde(default)]
    vk_call_links: Vec<String>,
    vk_token: Option<String>,
    #[serde(default)]
    ice_servers: Vec<IceServer>,
}

#[async_trait]
impl Provider for VkProvider {
    async fn bootstrap(&self, data: &Value) -> Result<ProviderBootstrap> {
        let parsed: VkData = serde_json::from_value(data.clone())
            .map_err(|e| Error::Provider(format!("bad VK provider_data: {e}")))?;
        if parsed.vk_call_links.is_empty()
            && parsed.vk_token.as_deref().unwrap_or("").trim().is_empty()
        {
            return Err(Error::Provider(
                "vk_call_links or vk_token is required".into(),
            ));
        }
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .user_agent("ToTS/0.1")
            .build()
            .map_err(|e| Error::Provider(e.to_string()))?;
        let mut links = parsed.vk_call_links;
        let mut created_call_ids = Vec::new();
        if links.is_empty() {
            if let Some(token) = parsed.vk_token.as_deref() {
                let (call_id, join_link) = start_call(&client, token).await?;
                created_call_ids.push(call_id);
                links.push(join_link);
            }
        }
        let mut ice = parsed.ice_servers;
        let mut signalling = Vec::new();
        for link in &links {
            let hash = join_hash(link)
                .ok_or_else(|| Error::Provider("invalid VK call link".into()))?;
            let (servers, meta) = ice_from_join_link(&client, hash).await?;
            ice.extend(servers);
            signalling.push(meta);
        }
        let mut turn = Vec::new();
        let mut stun = Vec::new();
        for server in ice {
            let mut turn_urls = Vec::new();
            for u in server.urls {
                let low = u.to_ascii_lowercase();
                if low.starts_with("turn:") || low.starts_with("turns:") {
                    turn_urls.push(u);
                } else if low.starts_with("stun:") {
                    stun.push(u);
                }
            }
            if !turn_urls.is_empty() {
                if let (Some(username), Some(password)) = (server.username, server.credential) {
                    turn.push(TurnCredentials {
                        urls: turn_urls,
                        username,
                        password,
                    });
                }
            }
        }
        if turn.is_empty() {
            return Err(Error::Provider(
                "VK returned no usable TURN credentials".into(),
            ));
        }
        Ok(ProviderBootstrap {
            turn,
            stun_urls: stun,
            signalling: json!({"provider":"vk","calls":signalling,"created_call_ids":created_call_ids}),
        })
    }

    async fn cleanup(&self, data: &Value, bootstrap: &ProviderBootstrap) -> Result<()> {
        let token = data
            .get("vk_token")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if token.is_empty() {
            return Ok(());
        }
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("ToTS/0.1")
            .build()
            .map_err(|e| Error::Provider(e.to_string()))?;
        if let Some(ids) = bootstrap
            .signalling
            .get("created_call_ids")
            .and_then(Value::as_array)
        {
            for id in ids.iter().filter_map(Value::as_str) {
                let _ = client
                    .post(format!("{VK_API}/calls.forceFinish"))
                    .bearer_auth(token)
                    .form(&[("v", "5.285"), ("call_id", id)])
                    .send()
                    .await;
            }
        }
        Ok(())
    }
}

async fn start_call(client: &Client, token: &str) -> Result<(String, String)> {
    let response: Value = client
        .post(format!("{VK_API}/calls.start"))
        .bearer_auth(token)
        .form(&[("v", "5.285")])
        .send()
        .await
        .map_err(|e| Error::Provider(e.to_string()))?
        .error_for_status()
        .map_err(|e| Error::Provider(e.to_string()))?
        .json()
        .await
        .map_err(|e| Error::Provider(e.to_string()))?;
    if let Some(e) = response.get("error") {
        return Err(Error::Provider(format!("calls.start failed: {e}")));
    }
    let join = response
        .pointer("/response/join_link")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Provider("calls.start returned no join_link".into()))?
        .to_owned();
    let call_id = response
        .pointer("/response/call_id")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Provider("calls.start returned no call_id".into()))?
        .to_owned();
    Ok((call_id, join))
}

fn join_hash(value: &str) -> Option<&str> {
    let v = value.trim().trim_end_matches('/');
    if v.is_empty() {
        return None;
    }
    if let Some(i) = v.to_ascii_lowercase().find("/call/join/") {
        let h = &v[i + "/call/join/".len()..];
        return h.split(['?', '#']).next().filter(|s| !s.is_empty());
    }
    Some(v)
}

async fn get_json(client: &Client, url: Url) -> Result<Value> {
    client
        .get(url)
        .send()
        .await
        .map_err(|e| Error::Provider(e.to_string()))?
        .error_for_status()
        .map_err(|e| Error::Provider(e.to_string()))?
        .json()
        .await
        .map_err(|e| Error::Provider(e.to_string()))
}

fn str_at<'a>(v: &'a Value, p: &str) -> Result<&'a str> {
    v.pointer(p)
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Provider(format!("missing {p}")))
}

async fn ice_from_join_link(client: &Client, hash: &str) -> Result<(Vec<IceServer>, Value)> {
    let device = Uuid::new_v4().to_string();
    let name = format!("tots-{}", &device[..8]);
    let link = format!("https://vk.ru/call/join/{hash}");
    let mut u = Url::parse(&format!("{VK_CALLS_API}/auth.getAnonymToken")).unwrap();
    u.query_pairs_mut()
        .append_pair("v", "5.276")
        .append_pair("client_id", VK_CONNECT_CLIENT_ID)
        .append_pair("link", &link)
        .append_pair("device_id", &device)
        .append_pair("anonymName", &name)
        .append_pair("lang", "en");
    let r1 = get_json(client, u).await?;
    let anonymous = str_at(&r1, "/response/token")?.to_owned();
    let mut u = Url::parse(&format!("{VK_CALLS_API}/messages.getCallPreview")).unwrap();
    u.query_pairs_mut()
        .append_pair("v", "5.276")
        .append_pair("anonymous_token", &anonymous)
        .append_pair("device_id", &device)
        .append_pair("extended", "1")
        .append_pair("fields", "first_name,last_name,photo_200")
        .append_pair("lang", "en")
        .append_pair("link", &link);
    let r2 = get_json(client, u).await?;
    let user_id = r2
        .pointer("/response/user_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| Error::Provider("missing user_id".into()))?
        .to_string();
    let secret = str_at(&r2, "/response/secret")?.to_owned();
    let mut u = Url::parse(&format!("{VK_CALLS_API}/messages.getAnonymCallToken")).unwrap();
    u.query_pairs_mut()
        .append_pair("v", "5.276")
        .append_pair("anonymous_token", &anonymous)
        .append_pair("device_id", &device)
        .append_pair("link", &link)
        .append_pair("name", &name)
        .append_pair("user_id", &user_id)
        .append_pair("secret", &secret)
        .append_pair("lang", "en");
    let r3 = get_json(client, u).await?;
    let ok_token = str_at(&r3, "/response/token")?.to_owned();
    let session_data = json!({"version":2,"device_id":Uuid::new_v4().to_string(),"client_version":"1.0.1"}).to_string();
    let login: Value = client
        .post(OK_CALLS)
        .form(&[
            ("session_data", session_data.as_str()),
            ("method", "auth.anonymLogin"),
            ("format", "JSON"),
            ("application_key", OK_APPLICATION_KEY),
        ])
        .send()
        .await
        .map_err(|e| Error::Provider(e.to_string()))?
        .error_for_status()
        .map_err(|e| Error::Provider(e.to_string()))?
        .json()
        .await
        .map_err(|e| Error::Provider(e.to_string()))?;
    let session_key = str_at(&login, "/session_key")?.to_owned();
    let joined: Value = client
        .post(OK_CALLS)
        .form(&[
            ("joinLink", hash),
            ("isVideo", "false"),
            ("protocolVersion", "5"),
            ("anonymToken", ok_token.as_str()),
            ("method", "vchat.joinConversationByLink"),
            ("format", "JSON"),
            ("application_key", OK_APPLICATION_KEY),
            ("session_key", session_key.as_str()),
        ])
        .send()
        .await
        .map_err(|e| Error::Provider(e.to_string()))?
        .error_for_status()
        .map_err(|e| Error::Provider(e.to_string()))?
        .json()
        .await
        .map_err(|e| Error::Provider(e.to_string()))?;
    let servers = parse_ice_servers(&joined)?;
    Ok((servers, json!({"join_link":link,"hash":hash})))
}

fn parse_ice_servers(v: &Value) -> Result<Vec<IceServer>> {
    let candidates: Vec<&Value> = if let Some(a) = v.get("iceServers").and_then(Value::as_array) {
        a.iter().collect()
    } else if let Some(a) = v.get("turn_servers").and_then(Value::as_array) {
        a.iter().collect()
    } else if let Some(one) = v.get("turn_server") {
        vec![one]
    } else {
        vec![]
    };
    let mut out = Vec::new();
    for s in candidates {
        let username = s
            .get("username")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let credential = s
            .get("credential")
            .or_else(|| s.get("password"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let urls = match s.get("urls").or_else(|| s.get("url")) {
            Some(Value::String(x)) => vec![x.clone()],
            Some(Value::Array(a)) => a
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
            _ => vec![],
        };
        if !urls.is_empty() {
            out.push(IceServer {
                urls,
                username,
                credential,
            });
        }
    }
    if out.is_empty() {
        return Err(Error::Provider(
            "provider response contained no ICE servers".into(),
        ));
    }
    Ok(out)
}
