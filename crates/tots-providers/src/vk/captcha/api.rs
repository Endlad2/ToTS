//! HTTP-запросы к API captchaNotRobot.

use std::time::Duration;

use reqwest::header::CONTENT_TYPE;
use serde_json::Value;
use tracing::debug;

use super::{encode_form, SolveContext, API_HOST, API_VERSION};
use crate::vk::persona;

#[derive(Debug, thiserror::Error)]
pub enum CaptchaError {
    #[error("network: {0}")]
    Network(#[from] reqwest::Error),
    #[error("captcha unavailable: {0}")]
    Unavailable(String),
    #[error("rate limit reached")]
    RateLimit,
    #[error("bot challenge failed")]
    Bot,
    #[error("slider required (content: {0:?})")]
    ShowTypeSlider(super::slider::ContentRef),
    #[error("unsupported captcha type: {0}")]
    Unsupported(String),
    #[error("no attempts left")]
    NoAttempts,
    #[error("pow parse: {0}")]
    PowParse(String),
    #[error("api error: {0}")]
    Api(String),
}

pub type SolveResult = Result<String, CaptchaError>;

/// GET страницы капчи по redirect_uri.
pub async fn fetch_html(ctx: &SolveContext) -> Result<String, CaptchaError> {
    let resp = ctx
        .http
        .get(&ctx.redirect_uri)
        .headers(persona::base_headers())
        .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        .header("Upgrade-Insecure-Requests", "1")
        .header("Sec-Fetch-Dest", "document")
        .header("Sec-Fetch-Mode", "navigate")
        .header("Sec-Fetch-User", "?1")
        .header("Sec-Fetch-Site", "cross-site")
        .header("Referer", "https://vk.ru/")
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() {
        return Err(CaptchaError::Unavailable(format!("http {}", status)));
    }
    let body = resp.text().await?;
    Ok(body)
}

/// `captchaNotRobot.initSession` — получает show_type и content settings.
pub async fn init_session(ctx: &SolveContext) -> Result<(String, super::slider::ContentRef), CaptchaError> {
    let url = format!("https://{}/method/captchaNotRobot.initSession?v={}", API_HOST, API_VERSION);
    let body = encode_form(&[
        ("session_token", ctx.session_token.clone()),
        ("domain", "vk.ru".to_string()),
        ("lang", "0".to_string()),
        ("access_token", String::new()),
    ]);
    let resp: Value = ctx
        .http
        .post(&url)
        .headers(persona::base_headers())
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header("Origin", "https://vk.ru")
        .header("Referer", "https://vk.ru/")
        .body(body)
        .send()
        .await?
        .json()
        .await?;

    let response = resp
        .get("response")
        .ok_or_else(|| CaptchaError::Api(format!("no response: {resp}")))?;

    let show_type = response
        .get("show_captcha_type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let content = parse_content_ref(response.get("content_settings"));
    Ok((show_type, content))
}

pub fn parse_content_ref(raw: Option<&Value>) -> super::slider::ContentRef {
    let Some(arr) = raw.and_then(|v| v.as_array()) else {
        return super::slider::ContentRef::default();
    };
    for item in arr {
        if item.get("type").and_then(|v| v.as_str()) == Some("slider") {
            if let Some(key) = item.get("settings_key").and_then(|v| v.as_str()) {
                if !key.is_empty() {
                    return super::slider::ContentRef {
                        source: "settings_key".into(),
                        value: key.to_string(),
                    };
                }
            }
            if let Some(s) = item.get("settings").and_then(|v| v.as_str()) {
                if !s.is_empty() {
                    return super::slider::ContentRef {
                        source: "captcha_settings".into(),
                        value: s.to_string(),
                    };
                }
            }
        }
    }
    super::slider::ContentRef::default()
}

/// `captchaNotRobot.settings`.
pub async fn settings(ctx: &SolveContext) -> Result<Value, CaptchaError> {
    let url = format!("https://{}/method/captchaNotRobot.settings?v={}", API_HOST, API_VERSION);
    let body = encode_form(&[
        ("session_token", ctx.session_token.clone()),
        ("domain", "vk.ru".to_string()),
        ("adFp", String::new()),
        ("access_token", String::new()),
    ]);
    let resp: Value = ctx
        .http
        .post(&url)
        .headers(persona::base_headers())
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header("Origin", "https://vk.ru")
        .header("Referer", "https://vk.ru/")
        .body(body)
        .send()
        .await?
        .json()
        .await?;
    Ok(resp)
}

/// `captchaNotRobot.endSession`.
pub async fn end_session(ctx: &SolveContext) -> Result<(), CaptchaError> {
    post_simple(ctx, "captchaNotRobot.endSession", &[]).await
}

/// `captchaNotRobot.leaveCaptcha`.
pub async fn leave_captcha(ctx: &SolveContext) -> Result<(), CaptchaError> {
    post_simple(ctx, "captchaNotRobot.leaveCaptcha", &[]).await
}

pub async fn post_simple(
    ctx: &SolveContext,
    method: &str,
    extra: &[(&str, String)],
) -> Result<(), CaptchaError> {
    let url = format!("https://{}/method/{}?v={}", API_HOST, method, API_VERSION);
    let mut form: Vec<(&str, String)> = vec![
        ("session_token", ctx.session_token.clone()),
        ("domain", "vk.ru".to_string()),
        ("adFp", String::new()),
        ("access_token", String::new()),
    ];
    form.extend(extra.iter().cloned());
    let body = encode_form(&form);
    let resp = ctx
        .http
        .post(&url)
        .headers(persona::base_headers())
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header("Origin", "https://vk.ru")
        .header("Referer", "https://vk.ru/")
        .body(body)
        .send()
        .await?;
    if !resp.status().is_success() {
        debug!(status = %resp.status(), "captcha: {} http error", method);
    }
    Ok(())
}

/// `captchaNotRobot.check` — отправляет ответ и получает success_token.
pub async fn check(
    ctx: &SolveContext,
    pow_hash: &str,
    debug_info: &str,
    answer_json: &str,
    sensors_elapsed: Duration,
    browser_fp: &str,
) -> Result<CheckResult, CaptchaError> {
    let url = format!("https://{}/method/captchaNotRobot.check?v={}", API_HOST, API_VERSION);
    let analytics = super::telemetry::build_analytics(sensors_elapsed);
    let mut form: Vec<(&str, String)> = vec![
        ("session_token", ctx.session_token.clone()),
        ("domain", "vk.ru".to_string()),
        ("adFp", String::new()),
    ];
    for (k, v) in analytics {
        form.push((k, v));
    }
    form.push(("browser_fp", browser_fp.to_string()));
    form.push(("hash", pow_hash.to_string()));
    form.push((
        "answer",
        base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            answer_json.as_bytes(),
        ),
    ));
    form.push(("debug_info", debug_info.to_string()));
    form.push(("access_token", String::new()));
    let body = encode_form(&form);
    let resp: Value = ctx
        .http
        .post(&url)
        .headers(persona::base_headers())
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header("Origin", "https://vk.ru")
        .header("Referer", "https://vk.ru/")
        .body(body)
        .send()
        .await?
        .json()
        .await?;

    let response = resp
        .get("response")
        .ok_or_else(|| CaptchaError::Api(format!("no response: {resp}")))?;

    let status = response
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let success_token = response
        .get("success_token")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let show_type = response
        .get("show_captcha_type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let content = parse_content_ref(response.get("content_settings"));

    Ok(CheckResult {
        status,
        success_token,
        show_type,
        content,
    })
}

/// `captchaNotRobot.componentDone`.
pub async fn component_done(ctx: &SolveContext, browser_fp: &str) -> Result<(), CaptchaError> {
    post_simple(
        ctx,
        "captchaNotRobot.componentDone",
        &[("browser_fp", browser_fp.to_string())],
    )
    .await
}

#[derive(Debug)]
pub struct CheckResult {
    pub status: String,
    pub success_token: String,
    pub show_type: String,
    pub content: super::slider::ContentRef,
}
