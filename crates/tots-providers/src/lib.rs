//! ToTS providers — источники TURN-креденшелов.

pub mod vk;

use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use thiserror::Error;

pub use vk::VkProvider;

#[derive(Debug, Clone)]
pub struct TurnCredentials {
    pub username: String,
    pub password: String,
    pub server_addrs: Vec<String>,
    pub expires_at: SystemTime,
}

impl TurnCredentials {
    pub fn is_valid(&self, margin: Duration) -> bool {
        SystemTime::now() + margin < self.expires_at
    }
}

#[derive(Debug, Error)]
pub enum ProviderError {
    #[error("network error: {0}")]
    Network(#[from] reqwest::Error),

    #[error("invalid link: {0}")]
    InvalidLink(String),

    #[error("captcha required (session_token={session_token})")]
    CaptchaRequired {
        session_token: String,
        redirect_uri: String,
    },

    #[error("captcha solving failed: {0}")]
    CaptchaFailed(String),

    #[error("rate limited, retry after {retry_after:?}")]
    RateLimited { retry_after: Duration },

    #[error("authentication failed: {0}")]
    Auth(String),

    #[error("provider returned malformed response: {0}")]
    Malformed(String),

    #[error("provider is temporarily unavailable: {0}")]
    Unavailable(String),

    #[error("no credentials available")]
    NoCredentials,
}

#[async_trait]
pub trait TurnProvider: Send + Sync {
    async fn get_credentials(&self, stream_id: u32) -> Result<TurnCredentials, ProviderError>;

    fn is_auth_error(&self, err: &ProviderError) -> bool {
        matches!(err, ProviderError::Auth(_))
    }

    fn handle_auth_error(&self, _stream_id: u32) -> bool {
        false
    }

    fn reset_errors(&self, _stream_id: u32) {}

    fn drop_credentials(&self, _stream_id: u32) {}

    fn backoff_until_unix(&self) -> i64 {
        0
    }

    fn name(&self) -> &'static str;
}

pub mod consts {
    pub const DEFAULT_CREDENTIAL_LIFETIME: std::time::Duration =
        std::time::Duration::from_secs(600);
    pub const CACHE_SAFETY_MARGIN: std::time::Duration = std::time::Duration::from_secs(60);
}
