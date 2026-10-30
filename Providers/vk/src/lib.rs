use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tots_core::{TurnCredentials, TurnProvider, TurnServer};
use tracing::{debug, warn};
use url::Url;
use uuid::Uuid;

const API_VERSION: &str = "5.282";

// Public application credentials present in the repository supplied by the user.
// Prefer explicitly passing client_id/client_secret in provider_data so these can
// be rotated without changing this crate.
const FALLBACK_APPS: &[(&str, &str)] = &[
    ("6287487", "QbYic1K3lEV5kTGiqlq2"),
    ("7879029", "aR5NKGmm03GYrCiNKsaw"),
    ("2274003", "hHbZxrka2uZ6jB1inYsH"),
    ("51453752", "4UyuCUsdK8pVCNoeQuGi"),
    ("3140623", "VeWdmVclDCtn6ihuP1nt"),
];

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VkProviderData {
    /// Full `https://vk.ru/call/join/<code>` URL or just `<code>`.
    #[serde(default)]
    pub join_link: String,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub client_secret: Option<String>,

    /// User-obtained VK success token. The crate never auto-solves CAPTCHA.
    #[serde(default)]
    pub captcha_success_token: Option<String>,

    /// Optional direct TURN credential override. If all three are supplied,
    /// no VK HTTP requests are made.
    #[serde(default)]
    pub turn_username: Option<String>,
    #[serde(default)]
    pub turn_password: Option<String>,
    #[serde(default)]
    pub turn_urls: Option<Vec<String>>,
}

pub struct VkProvider {
    data: VkProviderData,
    http: Client,
    link_code: String,
}

#[derive(Debug, Clone, Default)]
struct CaptchaChallenge {
    redirect_uri: String,
    session_token: String,
    captcha_sid: String,
    captcha_ts: String,
    captcha_attempt: String,
}

impl VkProvider {
    pub fn new(data: VkProviderData) -> Result<Self> {
        let link_code = extract_join_code(&data.join_link)?;
        let http = Client::builder()
            .cookie_store(true)
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36")
            .build()
            .context("build VK HTTP client")?;
        Ok(Self { data, http, link_code })
    }

    async fn get_via_vk(&self) -> Result<TurnCredentials> {
        let apps: Vec<(String, String)> = match (&self.data.client_id, &self.data.client_secret) {
            (Some(id), Some(secret)) => vec![(id.clone(), secret.clone())],
            (None, None) => FALLBACK_APPS
                .iter()
                .map(|(id, secret)| ((*id).to_string(), (*secret).to_string()))
                .collect(),
            _ => bail!("VK provider_data must set both client_id and client_secret, or neither"),
        };

        let mut last = None;
        for (client_id, client_secret) in apps {
            match self.fetch_chain(&client_id, &client_secret).await {
                Ok(creds) => return Ok(creds),
                Err(err) => {
                    let text = format!("{err:#}");
                    if text.contains("CAPTCHA_REQUIRED") {
                        return Err(err);
                    }
                    warn!(client_id, error = %err, "VK app credential attempt failed");
                    last = Some(err);
                }
            }
        }
        Err(last.unwrap_or_else(|| anyhow!("no VK application credentials available")))
    }

    async fn fetch_chain(&self, client_id: &str, client_secret: &str) -> Result<TurnCredentials> {
        // Warm the join page/cookie jar like the reference implementation.
        let _ = self
            .http
            .get(format!("https://vk.ru/call/join/{}", self.link_code))
            .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
            .send()
            .await;

        let token1 = self.fetch_anon_token(client_id, client_secret).await?;

        let preview_url = format!(
            "https://api.vk.ru/method/calls.getCallPreview?v={API_VERSION}&client_id={client_id}"
        );
        let _ = self
            .post_form(
                preview_url,
                vec![
                    ("vk_join_link", format!("https://vk.ru/call/join/{}", self.link_code)),
                    ("fields", "photo_200".to_string()),
                    ("access_token", token1.clone()),
                ],
            )
            .await;

        let token2 = self.fetch_call_token(client_id, &token1).await?;
        let session_key = self.fetch_ok_session().await?;
        self.fetch_turn_credentials(&token2, &session_key).await
    }

    async fn fetch_anon_token(&self, client_id: &str, client_secret: &str) -> Result<String> {
        let value = self
            .post_form(
                "https://login.vk.ru/?act=get_anonym_token".to_string(),
                vec![
                    ("client_secret", client_secret.to_string()),
                    ("client_id", client_id.to_string()),
                    ("scopes", "audio_anonymous,video_anonymous,photos_anonymous,profile_anonymous".to_string()),
                    ("isApiOauthAnonymEnabled", "false".to_string()),
                    ("version", "1".to_string()),
                    ("app_id", client_id.to_string()),
                ],
            )
            .await?;
        value
            .pointer("/data/access_token")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| anyhow!("VK anonymous token missing: {value}"))
    }

    async fn fetch_call_token(&self, client_id: &str, token1: &str) -> Result<String> {
        let endpoint = format!(
            "https://api.vk.ru/method/calls.getAnonymousToken?v={API_VERSION}&client_id={client_id}"
        );
        let name = format!("Guest-{}", &Uuid::new_v4().simple().to_string()[..8]);
        let mut form = vec![
            ("vk_join_link", format!("https://vk.ru/call/join/{}", self.link_code)),
            ("name", name),
            ("access_token", token1.to_string()),
        ];

        let first = self.post_form(endpoint.clone(), form.clone()).await?;
        if let Some(token) = first.pointer("/response/token").and_then(Value::as_str) {
            return Ok(token.to_string());
        }

        let err = first
            .get("error")
            .ok_or_else(|| anyhow!("unexpected calls.getAnonymousToken response: {first}"))?;
        let code = err.get("error_code").and_then(Value::as_i64).unwrap_or_default();
        if code != 14 {
            bail!("VK calls.getAnonymousToken error: {err}");
        }

        let challenge = parse_captcha(err);
        let Some(success_token) = self.data.captcha_success_token.as_deref() else {
            bail!(
                "CAPTCHA_REQUIRED redirect_uri={} session_token={} captcha_sid={}; solve it interactively and pass provider_data.captcha_success_token",
                challenge.redirect_uri,
                challenge.session_token,
                challenge.captcha_sid
            );
        };

        if challenge.captcha_sid.is_empty() {
            form.push(("success_token", success_token.to_string()));
        } else {
            form.extend([
                ("captcha_key", String::new()),
                ("captcha_sid", challenge.captcha_sid.clone()),
                ("is_sound_captcha", "0".to_string()),
                ("success_token", success_token.to_string()),
                ("captcha_ts", challenge.captcha_ts.clone()),
                (
                    "captcha_attempt",
                    if challenge.captcha_attempt.is_empty() {
                        "1".to_string()
                    } else {
                        challenge.captcha_attempt.clone()
                    },
                ),
            ]);
        }

        let retry = self.post_form(endpoint, form).await?;
        retry
            .pointer("/response/token")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| anyhow!("VK token retry after user CAPTCHA token failed: {retry}"))
    }

    async fn fetch_ok_session(&self) -> Result<String> {
        let session_data = serde_json::json!({
            "version": 2,
            "device_id": Uuid::new_v4().to_string(),
            "client_version": 1.1,
            "client_type": "SDK_JS"
        })
        .to_string();
        let value = self
            .post_form(
                "https://calls.okcdn.ru/fb.do".to_string(),
                vec![
                    ("session_data", session_data),
                    ("method", "auth.anonymLogin".to_string()),
                    ("format", "JSON".to_string()),
                    ("application_key", "CGMMEJLGDIHBABABA".to_string()),
                ],
            )
            .await?;
        value
            .get("session_key")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| anyhow!("OK Calls session_key missing: {value}"))
    }

    async fn fetch_turn_credentials(&self, token2: &str, session_key: &str) -> Result<TurnCredentials> {
        let value = self
            .post_form(
                "https://calls.okcdn.ru/fb.do".to_string(),
                vec![
                    ("joinLink", self.link_code.clone()),
                    ("isVideo", "false".to_string()),
                    ("protocolVersion", "5".to_string()),
                    ("capabilities", "2F7F".to_string()),
                    ("anonymToken", token2.to_string()),
                    ("method", "vchat.joinConversationByLink".to_string()),
                    ("format", "JSON".to_string()),
                    ("application_key", "CGMMEJLGDIHBABABA".to_string()),
                    ("session_key", session_key.to_string()),
                ],
            )
            .await?;

        let ts = value
            .get("turn_server")
            .ok_or_else(|| anyhow!("turn_server missing from VK/OK response: {value}"))?;
        let username = ts
            .get("username")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("turn_server.username missing"))?
            .to_string();
        let password = ts
            .get("credential")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("turn_server.credential missing"))?
            .to_string();
        let urls = ts
            .get("urls")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("turn_server.urls missing"))?;
        let mut servers = Vec::new();
        for value in urls {
            if let Some(raw) = value.as_str() {
                if let Some(server) = parse_turn_url(raw) {
                    servers.push(server);
                }
            }
        }
        if servers.is_empty() {
            bail!("VK returned no UDP TURN URLs usable by this build");
        }
        debug!(count = servers.len(), "VK TURN credentials acquired");
        Ok(TurnCredentials {
            username,
            password,
            servers,
        })
    }

    async fn post_form(&self, url: String, form: Vec<(&str, String)>) -> Result<Value> {
        let response = self
            .http
            .post(url)
            .header("Accept", "*/*")
            .header("Origin", "https://vk.ru")
            .header("Referer", "https://vk.ru/")
            .form(&form)
            .send()
            .await
            .context("VK HTTP request failed")?;
        parse_json(response).await
    }
}

#[async_trait]
impl TurnProvider for VkProvider {
    fn name(&self) -> &'static str {
        "vk"
    }

    async fn get_credentials(&self) -> Result<TurnCredentials> {
        if let (Some(username), Some(password), Some(urls)) = (
            &self.data.turn_username,
            &self.data.turn_password,
            &self.data.turn_urls,
        ) {
            let servers: Vec<TurnServer> = urls.iter().filter_map(|u| parse_turn_url(u)).collect();
            if servers.is_empty() {
                bail!("turn_urls override contains no usable UDP TURN URL");
            }
            return Ok(TurnCredentials {
                username: username.clone(),
                password: password.clone(),
                servers,
            });
        }
        self.get_via_vk().await
    }
}

async fn parse_json(response: Response) -> Result<Value> {
    let status = response.status();
    let text = response.text().await.context("read VK HTTP body")?;
    if !status.is_success() {
        bail!("VK HTTP status {status}: {text}");
    }
    serde_json::from_str(&text).with_context(|| format!("decode VK JSON: {text}"))
}

fn extract_join_code(input: &str) -> Result<String> {
    let trimmed = input.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        bail!("VK provider_data.join_link is required");
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        let url = Url::parse(trimmed).context("parse VK join_link URL")?;
        let parts: Vec<_> = url.path_segments().into_iter().flatten().collect();
        if let Some(pos) = parts.iter().position(|p| *p == "join") {
            if let Some(code) = parts.get(pos + 1) {
                if !code.is_empty() {
                    return Ok((*code).to_string());
                }
            }
        }
        bail!("VK join_link must look like https://vk.ru/call/join/<code>");
    }
    Ok(trimmed.to_string())
}

fn parse_captcha(err: &Value) -> CaptchaChallenge {
    let redirect_uri = err
        .get("redirect_uri")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let session_token = Url::parse(&redirect_uri)
        .ok()
        .and_then(|url| {
            url.query_pairs()
                .find(|(k, _)| k == "session_token")
                .map(|(_, v)| v.into_owned())
        })
        .or_else(|| err.get("session_token").and_then(Value::as_str).map(str::to_string))
        .unwrap_or_default();
    CaptchaChallenge {
        redirect_uri,
        session_token,
        captcha_sid: value_to_string(err.get("captcha_sid")),
        captcha_ts: value_to_string(err.get("captcha_ts")),
        captcha_attempt: value_to_string(err.get("captcha_attempt")),
    }
}

fn value_to_string(v: Option<&Value>) -> String {
    match v {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        _ => String::new(),
    }
}

fn parse_turn_url(raw: &str) -> Option<TurnServer> {
    let raw = raw.trim();
    if raw.starts_with("turns:") {
        return None; // turnclient 0.5 is UDP-only; no TURN/TLS in this MVP.
    }
    let rest = raw.strip_prefix("turn:")?;
    let (authority, query) = rest.split_once('?').unwrap_or((rest, ""));
    if !query.is_empty() && !query.split('&').any(|p| p == "transport=udp") {
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
    fn join_code_from_url() {
        assert_eq!(
            extract_join_code("https://vk.ru/call/join/abc123").unwrap(),
            "abc123"
        );
    }

    #[test]
    fn turn_url_parsing() {
        let s = parse_turn_url("turn:turn.example.org:3478?transport=udp").unwrap();
        assert_eq!(s.host, "turn.example.org");
        assert_eq!(s.port, 3478);
        assert!(parse_turn_url("turns:turn.example.org:5349?transport=tcp").is_none());
    }
}
