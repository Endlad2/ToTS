use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{process::Command, time::timeout};
use tots_core::{RelayScope, TurnCredentials, TurnProvider, TurnServer};
use tracing::{debug, info, warn};
use url::Url;
use uuid::Uuid;

const API_VERSION: &str = "5.282";
const ACCOUNT_API_VERSION: &str = "5.285";
const DEFAULT_ACCOUNT_APP_ID: &str = "6287487";

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

const CAPTCHA_HELPER_BYTES: &[u8] = include_bytes!(env!("VK_CAPTCHA_HELPER_PATH"));
const CAPTCHA_HELPER_TOKEN_PREFIX: &str = "TOTS_CAPTCHA_SUCCESS_TOKEN=";
const CAPTCHA_HELPER_TIMEOUT: Duration = Duration::from_secs(620);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VkProviderData {
    /// Full `https://vk.ru/call/join/<code>` URL or just `<code>`.
    #[serde(default)]
    pub join_link: String,

    /// Optional authenticated VK access token. When present, ToTS can create
    /// (or attach to) a Calls conversation and request its ICE/TURN parameters
    /// without relying on the anonymous join-link flow.
    #[serde(default, alias = "token")]
    pub access_token: Option<String>,
    /// Optional existing Calls conversation UUID for the authenticated path.
    #[serde(default)]
    pub conversation_id: Option<String>,
    /// VK application id used by messages.getCallToken (defaults to 6287487).
    #[serde(default)]
    pub app_id: Option<String>,

    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub client_secret: Option<String>,

    /// Optional pre-obtained VK success token. If absent and VK requests a
    /// challenge, ToTS launches the embedded interactive helper and waits for
    /// the user to complete the CAPTCHA in its browser window.
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
        let has_override = data.turn_username.is_some()
            && data.turn_password.is_some()
            && data.turn_urls.as_ref().is_some_and(|urls| !urls.is_empty());
        let has_account = data
            .access_token
            .as_deref()
            .is_some_and(|token| !token.trim().is_empty());
        let link_code = if data.join_link.trim().is_empty() {
            if !has_override && !has_account {
                bail!("VK provider_data.join_link or provider_data.access_token is required");
            }
            String::new()
        } else {
            extract_join_code(&data.join_link)?
        };
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
                    if text.contains("CAPTCHA") || text.contains("captcha helper") {
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
        if self.link_code.is_empty() {
            bail!("VK anonymous flow requires provider_data.join_link");
        }
        // Warm the join page/cookie jar like the browser flow.
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
        validate_captcha_redirect(&challenge)?;
        let success_token = match self
            .data
            .captcha_success_token
            .as_deref()
            .map(str::trim)
            .filter(|token| !token.is_empty())
        {
            Some(token) => token.to_string(),
            None => self.solve_captcha_interactively(&challenge).await?,
        };

        if challenge.captcha_sid.is_empty() {
            form.push(("success_token", success_token));
        } else {
            form.extend([
                ("captcha_key", String::new()),
                ("captcha_sid", challenge.captcha_sid.clone()),
                ("is_sound_captcha", "0".to_string()),
                ("success_token", success_token),
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

    async fn solve_captcha_interactively(&self, challenge: &CaptchaChallenge) -> Result<String> {
        validate_captcha_redirect(challenge)?;
        let helper = TempCaptchaHelper::materialize()?;
        info!(
            host = %Url::parse(&challenge.redirect_uri).ok().and_then(|url| url.host_str().map(str::to_string)).unwrap_or_default(),
            "VK CAPTCHA required; opening embedded interactive helper"
        );

        let mut command = Command::new(helper.path());
        command
            .env("TOTS_VK_CAPTCHA_URL", &challenge.redirect_uri)
            .arg("--timeout")
            .arg("600")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);

        let output = timeout(CAPTCHA_HELPER_TIMEOUT, command.output())
            .await
            .context("VK captcha helper timed out")?
            .context("launch embedded VK captcha helper")?;
        if !output.status.success() {
            bail!("VK captcha helper exited with status {}", output.status);
        }

        let token = parse_captcha_helper_token(&output.stdout)
            .context("VK captcha helper returned no success token")?;
        info!("VK CAPTCHA completed; resuming anonymous call-token request");
        Ok(token)
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
        if self.link_code.is_empty() {
            bail!("VK anonymous flow requires provider_data.join_link");
        }
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
        parse_turn_credentials(&value, "VK")
    }

    async fn get_via_account(&self, access_token: &str) -> Result<TurnCredentials> {
        let app_id = self
            .data
            .app_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(DEFAULT_ACCOUNT_APP_ID);

        let conversation_id = if let Some(existing) = self
            .data
            .conversation_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            existing.to_string()
        } else {
            let creator = self.open_account_session(access_token, app_id).await?;
            self.start_account_conversation(&creator.0, &creator.1, app_id)
                .await?
        };

        // Obtain ICE/TURN parameters through a fresh Calls session.
        let (api_url, session_key) = self.open_account_session(access_token, app_id).await?;
        let value = self
            .post_form(
                api_url,
                vec![
                    ("conversationId", conversation_id),
                    ("method", "vchat.getConversationParams".to_string()),
                    ("format", "JSON".to_string()),
                    ("application_key", "CGMMEJLGDIHBABABA".to_string()),
                    ("session_key", session_key),
                ],
            )
            .await
            .context("VK vchat.getConversationParams")?;
        ensure_api_ok(&value, "VK vchat.getConversationParams")?;
        parse_turn_credentials(&value, "VK")
    }

    async fn open_account_session(
        &self,
        access_token: &str,
        app_id: &str,
    ) -> Result<(String, String)> {
        let token_url = format!(
            "https://api.vk.ru/method/messages.getCallToken?v={ACCOUNT_API_VERSION}&client_id={app_id}"
        );
        let token_value = self
            .post_form(
                token_url,
                vec![
                    ("env", "production".to_string()),
                    ("access_token", access_token.to_string()),
                ],
            )
            .await
            .context("VK messages.getCallToken")?;
        ensure_api_ok(&token_value, "VK messages.getCallToken")?;
        let response = token_value
            .get("response")
            .ok_or_else(|| anyhow!("VK messages.getCallToken response missing"))?;
        let call_token = response
            .get("token")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("VK messages.getCallToken token missing"))?;
        let api_url = validate_calls_api_url(
            response
                .get("api_base_url")
                .and_then(Value::as_str)
                .unwrap_or("https://calls.okcdn.ru/fb.do"),
        )?;

        let device_id = Uuid::new_v4().to_string();
        let session_data = serde_json::json!({
            "auth_token": call_token,
            "version": 3,
            "device_id": device_id,
            "client_version": "1.0.1"
        })
        .to_string();
        let login = self
            .post_form(
                api_url.clone(),
                vec![
                    ("session_data", session_data),
                    ("device_id", device_id),
                    ("verification_supported", "true".to_string()),
                    ("verification_supported_v", "1".to_string()),
                    ("client", "test".to_string()),
                    ("gen_token", "true".to_string()),
                    ("method", "auth.anonymLogin".to_string()),
                    ("format", "JSON".to_string()),
                    ("application_key", "CGMMEJLGDIHBABABA".to_string()),
                ],
            )
            .await
            .context("VK Calls auth.anonymLogin")?;
        ensure_api_ok(&login, "VK Calls auth.anonymLogin")?;
        let session_key = login
            .get("session_key")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("VK Calls session_key missing"))?
            .to_string();
        Ok((api_url, session_key))
    }

    async fn start_account_conversation(
        &self,
        api_url: &str,
        session_key: &str,
        app_id: &str,
    ) -> Result<String> {
        let conversation_id = Uuid::new_v4().to_string();
        let payload = serde_json::json!({
            "is_video": true,
            "with_join_link": true,
            "join_by_link": true,
            "community_user_id": 0,
            "caller_app_id": app_id.parse::<u64>().unwrap_or(6287487)
        })
        .to_string();
        let value = self
            .post_form(
                api_url.to_string(),
                vec![
                    ("conversationId", conversation_id.clone()),
                    ("isVideo", "true".to_string()),
                    ("protocolVersion", "5".to_string()),
                    ("createJoinLink", "true".to_string()),
                    ("payload", payload),
                    ("onlyAdminCanShareMovie", "false".to_string()),
                    ("externalIds", String::new()),
                    ("method", "vchat.startConversation".to_string()),
                    ("format", "JSON".to_string()),
                    ("application_key", "CGMMEJLGDIHBABABA".to_string()),
                    ("session_key", session_key.to_string()),
                ],
            )
            .await
            .context("VK vchat.startConversation")?;
        ensure_api_ok(&value, "VK vchat.startConversation")?;
        Ok(value
            .get("id")
            .or_else(|| value.get("conversationId"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or(&conversation_id)
            .to_string())
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

    fn relay_scope(&self) -> RelayScope {
        // VK Calls credentials belong to a provider call session. Route them
        // through the WebRTC ICE stack so allocation/permission/channel state
        // is established before DTLS/SCTP/DataChannel traffic starts. This
        // avoids sending ToTS application frames directly through a fresh TURN
        // allocation.
        RelayScope::ProviderSession
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
        if let Some(access_token) = self
            .data
            .access_token
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return self.get_via_account(access_token).await;
        }
        self.get_via_vk().await
    }

    fn route_exception_hosts(&self) -> Vec<(String, u16)> {
        vec![
            ("vk.ru".to_string(), 443),
            ("api.vk.ru".to_string(), 443),
            ("login.vk.ru".to_string(), 443),
            ("id.vk.ru".to_string(), 443),
            ("calls.okcdn.ru".to_string(), 443),
        ]
    }
}

struct TempCaptchaHelper {
    path: PathBuf,
}

impl TempCaptchaHelper {
    fn materialize() -> Result<Self> {
        if CAPTCHA_HELPER_BYTES.is_empty() {
            bail!("embedded VK captcha helper is empty");
        }
        let suffix = if cfg!(windows) { ".exe" } else { "" };
        let path = std::env::temp_dir().join(format!(
            "tots-vk-captcha-{}-{}{}",
            std::process::id(),
            Uuid::new_v4().simple(),
            suffix
        ));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .with_context(|| format!("create temporary VK captcha helper {}", path.display()))?;
        file.write_all(CAPTCHA_HELPER_BYTES)
            .context("write embedded VK captcha helper")?;
        file.flush().context("flush embedded VK captcha helper")?;
        drop(file);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                .context("mark VK captcha helper executable")?;
        }

        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempCaptchaHelper {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_file(&self.path) {
            debug!(path = %self.path.display(), %error, "could not remove temporary VK captcha helper");
        }
    }
}

fn parse_captcha_helper_token(stdout: &[u8]) -> Option<String> {
    String::from_utf8_lossy(stdout)
        .lines()
        .filter_map(|line| line.trim().strip_prefix(CAPTCHA_HELPER_TOKEN_PREFIX))
        .map(str::trim)
        .find(|token| !token.is_empty())
        .map(str::to_string)
}

fn validate_captcha_redirect(challenge: &CaptchaChallenge) -> Result<()> {
    if challenge.redirect_uri.trim().is_empty() || challenge.session_token.trim().is_empty() {
        bail!("VK CAPTCHA response is missing redirect_uri/session_token");
    }
    let url = Url::parse(&challenge.redirect_uri).context("parse VK CAPTCHA redirect_uri")?;
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    let allowed = host == "id.vk.ru"
        || host == "vk.ru"
        || host == "vk.com"
        || host.ends_with(".vk.ru")
        || host.ends_with(".vk.com");
    if url.scheme() != "https"
        || !allowed
        || url.username() != ""
        || url.password().is_some()
        || url.port_or_known_default() != Some(443)
    {
        bail!("VK CAPTCHA redirect_uri is not an allowed HTTPS VK endpoint");
    }
    Ok(())
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

fn validate_calls_api_url(raw: &str) -> Result<String> {
    let mut url = Url::parse(raw).context("parse VK Calls api_base_url")?;
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    let allowed_host = host == "okcdn.ru" || host.ends_with(".okcdn.ru") || host == "api.ok.ru";
    if url.scheme() != "https"
        || !allowed_host
        || url.username() != ""
        || url.password().is_some()
        || url.port_or_known_default() != Some(443)
    {
        bail!("VK Calls api_base_url is not an allowed HTTPS Calls endpoint");
    }
    url.set_query(None);
    url.set_fragment(None);
    if url.path().is_empty() || url.path() == "/" {
        url.set_path("/fb.do");
    }
    if url.path() != "/fb.do" {
        bail!("VK Calls api_base_url path must be /fb.do");
    }
    Ok(url.to_string())
}

fn parse_turn_credentials(value: &Value, provider: &str) -> Result<TurnCredentials> {
    let ts = value
        .get("turn_server")
        .or_else(|| value.pointer("/response/turn_server"))
        .or_else(|| value.get("turn_servers").and_then(Value::as_array).and_then(|items| items.first()))
        .or_else(|| value.get("iceServers").and_then(Value::as_array).and_then(|items| items.first()))
        .ok_or_else(|| anyhow!("{provider} turn_server missing: {value}"))?;
    let username = ts
        .get("username")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("{provider} turn_server.username missing"))?
        .to_string();
    let password = ts
        .get("credential")
        .or_else(|| ts.get("password"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("{provider} turn_server.credential missing"))?
        .to_string();

    let mut servers = Vec::new();
    if let Some(urls) = ts.get("urls").and_then(Value::as_array) {
        servers.extend(urls.iter().filter_map(Value::as_str).filter_map(parse_turn_url));
    } else if let Some(url) = ts.get("urls").and_then(Value::as_str) {
        if let Some(server) = parse_turn_url(url) {
            servers.push(server);
        }
    } else if let Some(url) = ts.get("url").and_then(Value::as_str) {
        if let Some(server) = parse_turn_url(url) {
            servers.push(server);
        }
    }

    if servers.is_empty() {
        bail!("{provider} returned no UDP TURN URLs usable by this build");
    }
    debug!(count = servers.len(), provider = provider, "provider TURN/ICE credentials acquired");
    Ok(TurnCredentials {
        username,
        password,
        servers,
    })
}

fn parse_turn_url(raw: &str) -> Option<TurnServer> {
    let raw = raw.trim();
    if raw.starts_with("turns:") {
        return None; // This transport currently pins provider WebRTC relay to TURN/UDP.
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

    #[test]
    fn captcha_helper_token_is_machine_readable() {
        let out = b"noise\nTOTS_CAPTCHA_SUCCESS_TOKEN=abc123\n";
        assert_eq!(parse_captcha_helper_token(out).as_deref(), Some("abc123"));
    }

    #[test]
    fn captcha_redirect_is_restricted_to_vk_https() {
        let challenge = CaptchaChallenge {
            redirect_uri: "https://id.vk.ru/captcha?session_token=abc".to_string(),
            session_token: "abc".to_string(),
            ..Default::default()
        };
        assert!(validate_captcha_redirect(&challenge).is_ok());
        let mut bad = challenge.clone();
        bad.redirect_uri = "https://example.org/captcha?session_token=abc".to_string();
        assert!(validate_captcha_redirect(&bad).is_err());
    }
}
