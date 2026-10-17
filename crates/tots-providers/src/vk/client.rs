//! HTTP-клиент провайдера VK.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use rand::Rng;
use reqwest::cookie::Jar;
use reqwest::Client;
use serde_json::Value;
use tokio::sync::Mutex;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::vk::cache::CredentialStore;
use crate::vk::creds::{AnonToken, CallToken, OkSession};
use crate::vk::error::VkApiError;
use crate::{ProviderError, TurnCredentials, TurnProvider};

use super::consts;

#[derive(Debug, Clone)]
pub struct VkConfig {
    pub link: String,
    pub streams_per_cache: u32,
    pub restrict_to_first_client: bool,
    pub request_timeout: Duration,
}

impl VkConfig {
    pub fn new(link: impl Into<String>) -> Self {
        Self {
            link: link.into(),
            streams_per_cache: consts::DEFAULT_STREAMS_PER_CACHE,
            restrict_to_first_client: false,
            request_timeout: Duration::from_secs(20),
        }
    }
}

pub struct VkProvider {
    cfg: VkConfig,
    call_code: String,
    http: Client,
    store: CredentialStore,
    lockout_until: AtomicI64,
    fetch_lock: Mutex<()>,
    last_fetch: Mutex<Option<std::time::Instant>>,
}

impl VkProvider {
    pub fn new(cfg: VkConfig) -> Result<Self, ProviderError> {
        let call_code = parse_call_code(&cfg.link)?;

        let cookie_jar = std::sync::Arc::new(Jar::default());
        let http = Client::builder()
            .timeout(cfg.request_timeout)
            .user_agent(user_agent())
            .cookie_provider(cookie_jar)
            .gzip(true)
            .brotli(true)
            .build()?;

        let store = CredentialStore::new(cfg.streams_per_cache);

        Ok(Self {
            cfg,
            call_code,
            http,
            store,
            lockout_until: AtomicI64::new(0),
            fetch_lock: Mutex::new(()),
            last_fetch: Mutex::new(None),
        })
    }

    async fn fetch_or_cached(&self, stream_id: u32) -> Result<TurnCredentials, ProviderError> {
        let slot = self.store.slot(stream_id);
        if let Some(creds) = slot.get_valid(consts::CACHE_SAFETY_MARGIN) {
            debug!(stream_id, "vk: cached");
            return Ok(creds);
        }
        let _guard = self.fetch_lock.lock().await;
        self.throttle().await?;
        if let Some(creds) = slot.get_valid(consts::CACHE_SAFETY_MARGIN) {
            return Ok(creds);
        }
        let creds = self.fetch_fresh(stream_id).await?;
        slot.store(creds.clone());
        *self.last_fetch.lock().await = Some(std::time::Instant::now());
        Ok(creds)
    }

    async fn throttle(&self) -> Result<(), ProviderError> {
        const MIN_INTERVAL: Duration = Duration::from_secs(3);
        let last = *self.last_fetch.lock().await;
        if let Some(when) = last {
            let elapsed = when.elapsed();
            if elapsed < MIN_INTERVAL {
                tokio::time::sleep(MIN_INTERVAL - elapsed).await;
            }
        }
        Ok(())
    }

    async fn fetch_fresh(&self, stream_id: u32) -> Result<TurnCredentials, ProviderError> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        if now < self.lockout_until.load(Ordering::Relaxed) {
            return Err(ProviderError::RateLimited {
                retry_after: Duration::from_secs(30),
            });
        }

        let mut last_err: Option<ProviderError> = None;
        let pairs: Vec<_> = if self.cfg.restrict_to_first_client {
            consts::DEFAULT_CLIENT_PAIRS.iter().take(1).collect()
        } else {
            consts::DEFAULT_CLIENT_PAIRS.iter().collect()
        };

        for (client_id, client_secret) in pairs {
            debug!(stream_id, client_id, "vk: trying pair");
            match self.fetch_with_pair(stream_id, client_id, client_secret).await {
                Ok(creds) => return Ok(creds),
                Err(e) => {
                    warn!(stream_id, client_id, error = %e, "vk: pair failed");
                    let terminal = matches!(
                        e,
                        ProviderError::CaptchaRequired { .. }
                            | ProviderError::InvalidLink(_)
                            | ProviderError::Unavailable(_)
                    );
                    last_err = Some(e);
                    if terminal {
                        break;
                    }
                }
            }
        }

        self.lockout_until.store(
            (SystemTime::now() + Duration::from_secs(60))
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            Ordering::Relaxed,
        );
        Err(last_err.unwrap_or(ProviderError::NoCredentials))
    }

    async fn fetch_with_pair(
        &self,
        stream_id: u32,
        client_id: &str,
        client_secret: &str,
    ) -> Result<TurnCredentials, ProviderError> {
        let anon = self.fetch_anon_token(client_id, client_secret).await?;
        debug!(stream_id, "vk: anon token (len={})", anon.0.len());
        let call_token = self.fetch_call_token(&anon.0, client_id).await?;
        debug!(stream_id, "vk: call token");
        let session = self.fetch_ok_session().await?;
        debug!(stream_id, "vk: ok session");
        let turn = self.fetch_turn_creds(&call_token, &session).await?;
        debug!(stream_id, "vk: turn creds user={}", turn.username);
        Ok(turn)
    }

    async fn fetch_anon_token(
        &self,
        client_id: &str,
        client_secret: &str,
    ) -> Result<AnonToken, ProviderError> {
        let form = [
            ("client_secret", client_secret),
            ("client_id", client_id),
            ("scopes", consts::ANON_SCOPES),
            ("isApiOauthAnonymEnabled", "false"),
            ("version", "1"),
            ("app_id", client_id),
        ];
        let resp: Value = self
            .http
            .post(consts::ANON_TOKEN_URL)
            .header("Origin", "https://vk.ru")
            .header("Referer", "https://vk.ru/")
            .form(&form)
            .send()
            .await?
            .json()
            .await?;
        let token = resp
            .get("data")
            .and_then(|d| d.get("access_token"))
            .and_then(|t| t.as_str())
            .ok_or_else(|| ProviderError::Malformed(format!("no access_token: {resp}")))?;
        Ok(AnonToken(token.to_string()))
    }

    async fn fetch_call_token(
        &self,
        anon_token: &str,
        client_id: &str,
    ) -> Result<CallToken, ProviderError> {
        let url = format!(
            "{}/method/calls.getAnonymousToken?v={}&client_id={}",
            consts::API_BASE,
            consts::API_VERSION,
            client_id
        );
        let name = random_name();
        let form = [
            (
                "vk_join_link",
                format!("https://vk.ru/call/join/{}", self.call_code),
            ),
            ("name", name),
            ("access_token", anon_token.to_string()),
        ];
        let resp: Value = self
            .http
            .post(&url)
            .header("Origin", "https://vk.ru")
            .header("Referer", "https://vk.ru/")
            .form(&form)
            .send()
            .await?
            .json()
            .await?;
        if let Some(err) = resp.get("error") {
            let parsed = VkApiError::from_value(err)
                .ok_or_else(|| ProviderError::Malformed(format!("unparsed error: {err}")))?;
            return Err(parsed.into_provider_error());
        }
        let token = resp
            .get("response")
            .and_then(|r| r.get("token"))
            .and_then(|t| t.as_str())
            .ok_or_else(|| ProviderError::Malformed(format!("no token: {resp}")))?;
        Ok(CallToken(token.to_string()))
    }

    async fn fetch_ok_session(&self) -> Result<OkSession, ProviderError> {
        let session_data = format!(
            r#"{{"version":2,"device_id":"{}","client_version":1.1,"client_type":"SDK_JS"}}"#,
            Uuid::new_v4()
        );
        let url = format!("{}/fb.do", consts::CALLS_BASE);
        let form = [
            ("session_data", session_data),
            ("method", "auth.anonymLogin".to_string()),
            ("format", "JSON".to_string()),
            ("application_key", consts::OK_APP_KEY.to_string()),
        ];
        let resp: Value = self
            .http
            .post(&url)
            .header("Origin", "https://vk.ru")
            .header("Referer", "https://vk.ru/")
            .form(&form)
            .send()
            .await?
            .json()
            .await?;
        let key = resp
            .get("session_key")
            .and_then(|k| k.as_str())
            .ok_or_else(|| ProviderError::Malformed(format!("no session_key: {resp}")))?;
        Ok(OkSession(key.to_string()))
    }

    async fn fetch_turn_creds(
        &self,
        call_token: &CallToken,
        session: &OkSession,
    ) -> Result<TurnCredentials, ProviderError> {
        let url = format!("{}/fb.do", consts::CALLS_BASE);
        let form = [
            ("joinLink", self.call_code.clone()),
            ("isVideo", "false".to_string()),
            ("protocolVersion", "5".to_string()),
            ("capabilities", "2F7F".to_string()),
            ("anonymToken", call_token.0.clone()),
            ("method", "vchat.joinConversationByLink".to_string()),
            ("format", "JSON".to_string()),
            ("application_key", consts::OK_APP_KEY.to_string()),
            ("session_key", session.0.clone()),
        ];
        let resp: Value = self
            .http
            .post(&url)
            .header("Origin", "https://vk.ru")
            .header("Referer", "https://vk.ru/")
            .form(&form)
            .send()
            .await?
            .json()
            .await?;
        let turn = resp
            .get("turn_server")
            .ok_or_else(|| ProviderError::Malformed(format!("no turn_server: {resp}")))?;
        let username = turn
            .get("username")
            .and_then(|u| u.as_str())
            .ok_or_else(|| ProviderError::Malformed("no username".into()))?
            .to_string();
        let password = turn
            .get("credential")
            .and_then(|c| c.as_str())
            .ok_or_else(|| ProviderError::Malformed("no credential".into()))?
            .to_string();
        let urls = turn
            .get("urls")
            .and_then(|u| u.as_array())
            .ok_or_else(|| ProviderError::Malformed("no urls".into()))?;
        let server_addrs = urls
            .iter()
            .filter_map(|v| v.as_str())
            .map(strip_turn_scheme)
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        if server_addrs.is_empty() {
            return Err(ProviderError::Malformed("empty turn urls".into()));
        }
        Ok(TurnCredentials {
            username,
            password,
            server_addrs,
            expires_at: SystemTime::now() + consts::CREDENTIAL_LIFETIME,
        })
    }
}

#[async_trait]
impl TurnProvider for VkProvider {
    async fn get_credentials(&self, stream_id: u32) -> Result<TurnCredentials, ProviderError> {
        self.fetch_or_cached(stream_id).await
    }
    fn is_auth_error(&self, err: &ProviderError) -> bool {
        matches!(err, ProviderError::Auth(_))
    }
    fn handle_auth_error(&self, stream_id: u32) -> bool {
        let slot = self.store.slot(stream_id);
        let count = slot.error_count.fetch_add(1, Ordering::Relaxed) + 1;
        slot.last_error_unix.store(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            Ordering::Relaxed,
        );
        if count >= 3 {
            slot.invalidate();
            return true;
        }
        false
    }
    fn reset_errors(&self, stream_id: u32) {
        self.store.slot(stream_id).error_count.store(0, Ordering::Relaxed);
    }
    fn drop_credentials(&self, stream_id: u32) {
        self.store.slot(stream_id).invalidate();
    }
    fn backoff_until_unix(&self) -> i64 {
        self.lockout_until.load(Ordering::Relaxed)
    }
    fn name(&self) -> &'static str {
        "vk"
    }
}

fn parse_call_code(input: &str) -> Result<String, ProviderError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ProviderError::InvalidLink("empty link".into()));
    }
    if let Ok(url) = url::Url::parse(trimmed) {
        let segments: Vec<_> = url.path_segments().map(|s| s.collect()).unwrap_or_default();
        let code = segments
            .iter()
            .rev()
            .find(|s| !s.is_empty())
            .ok_or_else(|| ProviderError::InvalidLink("no code in URL".into()))?;
        return Ok(code.to_string());
    }
    Ok(trimmed.to_string())
}

fn strip_turn_scheme(s: &str) -> String {
    let no_scheme = s
        .strip_prefix("turns:")
        .or_else(|| s.strip_prefix("turn:"))
        .unwrap_or(s);
    no_scheme.split('?').next().unwrap_or(no_scheme).to_string()
}

fn random_name() -> String {
    const FIRST: &[&str] = &["Ivan", "Petr", "Anna", "Maria", "Alex", "Olga", "Nikita"];
    const LAST: &[&str] = &["Ivanov", "Petrov", "Sidorov", "Smirnov", "Kuznetsov"];
    let mut rng = rand::thread_rng();
    format!(
        "{} {}",
        FIRST[rng.gen_range(0..FIRST.len())],
        LAST[rng.gen_range(0..LAST.len())]
    )
}

fn user_agent() -> &'static str {
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
     (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_full_url() {
        assert_eq!(parse_call_code("https://vk.ru/call/join/ABC123").unwrap(), "ABC123");
    }
    #[test]
    fn parse_bare_code() {
        assert_eq!(parse_call_code("ABC123").unwrap(), "ABC123");
    }
    #[test]
    fn strip_scheme() {
        assert_eq!(strip_turn_scheme("turn:1.2.3.4:3478"), "1.2.3.4:3478");
    }
}
