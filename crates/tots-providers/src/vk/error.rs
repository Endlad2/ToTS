//! Разбор ошибок VK API.

use serde_json::Value;

use crate::ProviderError;

use super::consts;

#[derive(Debug, Clone)]
pub struct VkApiError {
    pub code: i64,
    pub message: String,
    pub redirect_uri: Option<String>,
    pub session_token: Option<String>,
}

impl VkApiError {
    pub fn from_value(v: &Value) -> Option<Self> {
        let obj = v.as_object()?;
        let code = obj.get("error_code")?.as_i64()?;
        let message = obj
            .get("error_msg")
            .and_then(|m| m.as_str())
            .unwrap_or_default()
            .to_string();

        let str_or_num = |key: &str| -> Option<String> {
            obj.get(key).and_then(|v| match v {
                Value::String(s) => Some(s.clone()),
                Value::Number(n) => Some(n.to_string()),
                _ => None,
            })
        };

        let redirect_uri = str_or_num("redirect_uri");
        let session_token = redirect_uri
            .as_deref()
            .and_then(extract_session_token)
            .or_else(|| str_or_num("session_token"));

        Some(Self {
            code,
            message,
            redirect_uri,
            session_token,
        })
    }

    pub fn is_captcha(&self) -> bool {
        self.code == consts::CAPTCHA_ERROR_CODE
            && self.redirect_uri.is_some()
            && self.session_token.is_some()
    }

    pub fn into_provider_error(self) -> ProviderError {
        if self.is_captcha() {
            return ProviderError::CaptchaRequired {
                session_token: self.session_token.unwrap_or_default(),
                redirect_uri: self.redirect_uri.unwrap_or_default(),
            };
        }
        if self.code == consts::RATE_LIMIT_ERROR_CODE {
            return ProviderError::RateLimited {
                retry_after: std::time::Duration::from_secs(60),
            };
        }
        if consts::TERMINAL_LINK_ERROR_CODES.contains(&self.code) {
            return ProviderError::InvalidLink(self.message);
        }
        if self.code == consts::AUTH_ERROR_CODE {
            return ProviderError::Auth(self.message);
        }

        let lower = self.message.to_lowercase();
        if lower.contains("not valid") || lower.contains("not found") {
            return ProviderError::InvalidLink(self.message);
        }
        if lower.contains("anonym") {
            return ProviderError::Auth(format!("anonymous access blocked: {}", self.message));
        }
        if lower.contains("full") {
            return ProviderError::Auth(format!("call is full: {}", self.message));
        }

        ProviderError::Malformed(format!("vk error {}: {}", self.code, self.message))
    }
}

fn extract_session_token(uri: &str) -> Option<String> {
    let parsed = url::Url::parse(uri).ok()?;
    parsed
        .query_pairs()
        .find(|(k, _)| k == "session_token")
        .map(|(_, v)| v.into_owned())
}
