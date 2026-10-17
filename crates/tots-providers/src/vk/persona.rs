//! Браузерная персона: UA, sec-ch-ua, TLS-отпечаток.

use reqwest::header::{HeaderMap, HeaderValue, ACCEPT_LANGUAGE, USER_AGENT};

/// Chrome 146 sec-ch-ua строка.
pub const SEC_CH_UA: &str =
    r#""Google Chrome";v="146", "Chromium";v="146", "Not)A;Brand";v="24""#;

/// Chrome 146 sec-ch-ua-mobile (desktop).
pub const SEC_CH_UA_MOBILE: &str = "?0";

/// Chrome 146 sec-ch-ua-platform (Windows).
pub const SEC_CH_UA_PLATFORM: &str = r#""Windows""#;

/// Полный User-Agent Chrome 146 Windows.
pub const USER_AGENT_CHROME_WINDOWS: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
     (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36";

/// Значение Accept-Language как у русского пользователя.
pub const ACCEPT_LANGUAGE_VALUE: &str = "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7";

/// Базовые заголовки персоны, которые обязаны совпадать у всех запросов к VK.
pub fn base_headers() -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(USER_AGENT, HeaderValue::from_static(USER_AGENT_CHROME_WINDOWS));
    h.insert("sec-ch-ua", HeaderValue::from_static(SEC_CH_UA));
    h.insert(
        "sec-ch-ua-mobile",
        HeaderValue::from_static(SEC_CH_UA_MOBILE),
    );
    h.insert(
        "sec-ch-ua-platform",
        HeaderValue::from_static(SEC_CH_UA_PLATFORM),
    );
    h.insert(
        ACCEPT_LANGUAGE,
        HeaderValue::from_static(ACCEPT_LANGUAGE_VALUE),
    );
    h
}
