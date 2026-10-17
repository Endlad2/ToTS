//! Автоматическое решение VK Smart Captcha (captchaNotRobot).

mod api;
mod assets;
mod checkbox;
mod html;
mod pow;
mod slider;
mod telemetry;

pub use api::{CaptchaError, SolveResult};

use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use reqwest::cookie::Jar;
use reqwest::Client;
use tracing::{debug, warn};

const MAX_ATTEMPTS: u32 = 2;
pub(crate) const API_HOST: &str = "api.vk.ru";
pub(crate) const API_VERSION: &str = "5.131";

pub struct SolveContext {
    pub http: Client,
    pub cookie_jar: Arc<Jar>,
    pub session_token: String,
    pub redirect_uri: String,
    pub attempt: u32,
}

/// Пробует решить капчу. Возвращает success_token или ошибку.
pub async fn solve(ctx: SolveContext) -> SolveResult {
    let mut last_err = None;
    for attempt in 1..=MAX_ATTEMPTS {
        debug!(attempt, "captcha: solve attempt");
        match solve_once(&ctx).await {
            Ok(token) => return Ok(token),
            Err(e) => {
                warn!(attempt, error = %e, "captcha: attempt failed");
                let fatal = matches!(e, CaptchaError::RateLimit | CaptchaError::Bot);
                last_err = Some(e);
                if fatal {
                    break;
                }
                let backoff = Duration::from_millis(500 * attempt as u64);
                tokio::time::sleep(backoff).await;
            }
        }
    }
    Err(last_err.unwrap_or(CaptchaError::NoAttempts))
}

async fn solve_once(ctx: &SolveContext) -> Result<String, CaptchaError> {
    let html = api::fetch_html(ctx).await?;
    debug!(len = html.len(), "captcha: html loaded");

    let page = html::parse_page(&html).map_err(|e| CaptchaError::Unavailable(e.to_string()))?;

    let assets_task = assets::load_assets_async(
        ctx.http.clone(),
        ctx.cookie_jar.clone(),
        &html,
        &ctx.redirect_uri,
    );

    let pow_hash = pow::solve_and_wrap(
        &page.pow,
        page.debug_info.clone(),
        &page.origin,
        &page.page_origin,
        &ctx.session_token,
    )
    .await?;
    debug!("captcha: pow ready");

    let _ = assets_task.await;

    let (show_type, slider_content) = if page.init.found {
        (page.init.show_type.clone(), page.init.content.clone())
    } else {
        api::init_session(ctx).await?
    };
    debug!(show_type = %show_type, "captcha: challenge type");

    let settings = api::settings(ctx).await?;
    let sensors_delay = settings
        .get("response")
        .and_then(|r| r.get("sensors_delay"))
        .and_then(|v| v.as_f64())
        .map(|ms| Duration::from_millis(ms.max(20.0) as u64))
        .unwrap_or(Duration::from_millis(100));
    debug!(delay = ?sensors_delay, "captcha: sensors config");

    dwell(250, 400).await;

    let check_result = match show_type.as_str() {
        "slider" => slider::solve(ctx, &pow_hash, &page, slider_content, sensors_delay).await,
        "checkbox" | "" => checkbox::solve(ctx, &pow_hash, &page, sensors_delay).await,
        other => {
            warn!(show_type = other, "captcha: unsupported type");
            Err(CaptchaError::Unsupported(other.to_string()))
        }
    };

    let success_token = match check_result {
        Ok(tok) => tok,
        Err(CaptchaError::ShowTypeSlider(content)) => {
            debug!("captcha: escalating to slider");
            dwell(500, 1100).await;
            slider::solve(ctx, &pow_hash, &page, content, sensors_delay).await?
        }
        Err(e) => {
            let _ = api::leave_captcha(ctx).await;
            return Err(e);
        }
    };

    if let Err(e) = api::end_session(ctx).await {
        warn!(error = %e, "captcha: endSession failed (non-fatal)");
    }

    Ok(success_token)
}

pub(crate) async fn dwell(min: u64, max: u64) {
    let ms = {
        let mut rng = rand::thread_rng();
        min + rng.gen_range(0..(max.saturating_sub(min)).max(1))
    };
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

pub(crate) fn encode_form(values: &[(&str, String)]) -> String {
    let mut out = String::new();
    for (i, (k, v)) in values.iter().enumerate() {
        if i > 0 {
            out.push('&');
        }
        out.push_str(&urlencode(k));
        out.push('=');
        out.push_str(&urlencode(v));
    }
    out
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match *b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char)
            }
            b' ' => out.push('+'),
            _ => {
                out.push('%');
                out.push_str(&format!("{:02X}", b));
            }
        }
    }
    out
}
