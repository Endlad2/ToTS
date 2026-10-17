//! Checkbox-капча: показываем виджет, ждём, отправляем check.

use std::time::{Duration, Instant};

use tracing::debug;

use super::api::{self, CaptchaError, CheckResult};
use super::{dwell, html::Page, SolveContext};

pub async fn solve(
    ctx: &SolveContext,
    pow_hash: &str,
    page: &Page,
    sensors_delay: Duration,
) -> Result<String, CaptchaError> {
    // 1. componentDone.
    let browser_fp = generate_browser_fp();
    api::component_done(ctx, &browser_fp).await?;
    debug!("captcha: componentDone sent");

    // 2. Пауза (пользователь смотрит на виджет).
    dwell(450, 950).await;

    // 3. check с пустым ответом.
    let sensors_start = Instant::now();
    let check = api::check(
        ctx,
        pow_hash,
        &page.debug_info,
        "{}",
        sensors_start.elapsed() + sensors_delay,
        &browser_fp,
    )
    .await?;

    // 4. Обработка результата.
    interpret_check(check)
}

fn interpret_check(check: CheckResult) -> Result<String, CaptchaError> {
    let status = check.status.to_lowercase();
    if status == "error_limit" {
        return Err(CaptchaError::RateLimit);
    }
    if status == "bot" {
        return Err(CaptchaError::Bot);
    }
    if !check.show_type.is_empty() && check.show_type.to_lowercase() != "checkbox" {
        return Err(CaptchaError::ShowTypeSlider(check.content));
    }
    if status == "ok" {
        if check.success_token.is_empty() {
            return Err(CaptchaError::Api("success_token empty".into()));
        }
        debug!("captcha: checkbox accepted");
        return Ok(check.success_token);
    }
    Err(CaptchaError::Api(format!(
        "checkbox status: {}",
        check.status
    )))
}

fn generate_browser_fp() -> String {
    let bytes: [u8; 16] = rand::random();
    hex::encode(bytes)
}
