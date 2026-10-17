//! Провайдер VK Calls — получение TURN-креденшелов из ссылки на звонок,
//! включая авторешение Smart Captcha.

mod cache;
mod captcha;
mod client;
mod creds;
mod error;
mod persona;

pub use client::{VkConfig, VkProvider};

pub mod consts {
    use std::time::Duration;

    pub const API_VERSION: &str = "5.282";
    pub const ANON_TOKEN_URL: &str = "https://login.vk.ru/?act=get_anonym_token";
    pub const API_BASE: &str = "https://api.vk.ru";
    pub const CALLS_BASE: &str = "https://calls.okcdn.ru";
    pub const OK_APP_KEY: &str = "CGMMEJLGDIHBABABA";
    pub const ANON_SCOPES: &str =
        "audio_anonymous,video_anonymous,photos_anonymous,profile_anonymous";
    pub const CREDENTIAL_LIFETIME: Duration = Duration::from_secs(600);
    pub const CACHE_SAFETY_MARGIN: Duration = Duration::from_secs(60);
    pub const DEFAULT_STREAMS_PER_CACHE: u32 = 12;

    pub const DEFAULT_CLIENT_PAIRS: &[(&str, &str)] = &[
        ("6287487", "QbYic1K3lEV5kTGiqlq2"),
        ("7879029", "aR5NKGmm03GYrCiNKsaw"),
        ("2274003", "hHbZxrka2uZ6jB1inYsH"),
        ("51453752", "4UyuCUsdK8pVCNoeQuGi"),
        ("3140623", "VeWdmVclDCtn6ihuP1nt"),
    ];

    pub const TERMINAL_LINK_ERROR_CODES: &[i64] = &[9000, 9008];
    pub const CAPTCHA_ERROR_CODE: i64 = 14;
    pub const RATE_LIMIT_ERROR_CODE: i64 = 29;
    pub const AUTH_ERROR_CODE: i64 = 5;
}
