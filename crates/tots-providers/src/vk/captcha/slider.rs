//! Slider-капча: getContent → анализ изображения → check с лучшей попыткой.

use std::time::{Duration, Instant};

use base64::Engine;
use image::{GenericImageView, RgbImage};
use serde_json::Value;
use tracing::debug;

use super::api::{self, CaptchaError};
use super::{dwell, html::Page, SolveContext};

#[derive(Debug, Clone, Default)]
pub struct ContentRef {
    pub source: String,
    pub value: String,
}

#[derive(Debug)]
struct Puzzle {
    image: RgbImage,
    grid: usize,
    swaps: Vec<usize>,
    attempts: u32,
}

#[derive(Debug, Clone)]
struct Guess {
    index: usize,
    swaps: Vec<usize>,
    consensus: i64,
    luma: i64,
}

pub async fn solve(
    ctx: &SolveContext,
    pow_hash: &str,
    page: &Page,
    content: ContentRef,
    sensors_delay: Duration,
) -> Result<String, CaptchaError> {
    if content.value.is_empty() {
        return Err(CaptchaError::Api("slider content empty".into()));
    }
    debug!(source = %content.source, len = content.value.len(), "captcha: slider getContent");

    // 1. getContent.
    let raw = get_content(ctx, &content).await?;

    // 2. Парсим puzzle.
    let puzzle = parse_puzzle(&raw)?;
    debug!(
        grid = puzzle.grid,
        swaps = puzzle.swaps.len() / 2,
        attempts = puzzle.attempts,
        "captcha: slider puzzle decoded"
    );

    // 3. Ранжируем.
    let guesses = rank_guesses(&puzzle)?;
    let limit = (puzzle.attempts as usize).min(guesses.len());
    if limit == 0 {
        return Err(CaptchaError::Api("no slider attempts".into()));
    }
    let attempts = pick_attempts(&guesses, limit);
    debug!(total = guesses.len(), limit, "captcha: slider ranked");

    // 4. componentDone.
    let browser_fp = generate_browser_fp();
    api::component_done(ctx, &browser_fp).await?;

    // 5. Перебираем попытки.
    for (i, guess) in attempts.iter().enumerate() {
        debug!(
            attempt = i + 1,
            of = attempts.len(),
            guess = guess.index,
            "captcha: slider try"
        );

        let answer = serde_json::json!({ "value": guess.swaps });
        let answer_json = answer.to_string();

        dwell(650, 1750).await;

        let sensors_start = Instant::now();
        let check = api::check(
            ctx,
            pow_hash,
            &page.debug_info,
            &answer_json,
            sensors_start.elapsed() + sensors_delay,
            &browser_fp,
        )
        .await?;

        let status = check.status.to_lowercase();
        debug!(attempt = i + 1, status = %status, "captcha: slider check");
        if status == "ok" {
            if check.success_token.is_empty() {
                return Err(CaptchaError::Api("success_token empty".into()));
            }
            return Ok(check.success_token);
        }
        if status == "error_limit" {
            return Err(CaptchaError::RateLimit);
        }
        dwell(700, 1600).await;
    }
    Err(CaptchaError::Api("slider attempts exhausted".into()))
}

async fn get_content(ctx: &SolveContext, content: &ContentRef) -> Result<Value, CaptchaError> {
    let url = "https://api.vk.ru/method/captchaNotRobot.getContent?v=5.131";
    let form = super::encode_form(&[
        ("session_token", ctx.session_token.clone()),
        ("domain", "vk.ru".to_string()),
        ("adFp", String::new()),
        ("access_token", String::new()),
        ("captcha_settings", content.value.clone()),
    ]);
    let resp: Value = ctx
        .http
        .post(url)
        .headers(crate::vk::persona::base_headers())
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Origin", "https://vk.ru")
        .header("Referer", "https://vk.ru/")
        .body(form)
        .send()
        .await?
        .json()
        .await?;
    Ok(resp)
}

fn parse_puzzle(raw: &Value) -> Result<Puzzle, CaptchaError> {
    let response = raw
        .get("response")
        .ok_or_else(|| CaptchaError::Api(format!("no response: {raw}")))?;
    let status = response.get("status").and_then(|v| v.as_str()).unwrap_or("");
    if status.to_lowercase() != "ok" {
        return Err(CaptchaError::Api(format!("getContent status: {status}")));
    }
    let image_b64 = response
        .get("image")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CaptchaError::Api("no image".into()))?;
    let steps = response
        .get("steps")
        .and_then(|v| v.as_array())
        .ok_or_else(|| CaptchaError::Api("no steps".into()))?;

    let mut nums: Vec<usize> = steps
        .iter()
        .filter_map(|v| v.as_i64().map(|n| n.max(0) as usize))
        .collect();
    if nums.len() < 3 {
        return Err(CaptchaError::Api("steps too short".into()));
    }
    let grid = nums[0];
    if grid == 0 {
        return Err(CaptchaError::Api("grid is zero".into()));
    }
    let mut tail = nums.split_off(1);
    let mut attempts: u32 = 4;
    if tail.len() % 2 != 0 {
        attempts = tail.pop().unwrap_or(4) as u32;
    }
    if tail.is_empty() || tail.len() % 2 != 0 {
        return Err(CaptchaError::Api("bad swap payload".into()));
    }

    let bytes = base64::engine::general_purpose::STANDARD
        .decode(image_b64)
        .map_err(|e| CaptchaError::Api(format!("image b64: {e}")))?;
    let dynimg = image::load_from_memory(&bytes)
        .map_err(|e| CaptchaError::Api(format!("image decode: {e}")))?;
    let rgb = dynimg.to_rgb8();
    let _ = dynimg.dimensions();

    Ok(Puzzle {
        image: rgb,
        grid,
        swaps: tail,
        attempts,
    })
}

fn rank_guesses(p: &Puzzle) -> Result<Vec<Guess>, CaptchaError> {
    let count = p.swaps.len() / 2;
    if count == 0 {
        return Err(CaptchaError::Api("no candidates".into()));
    }

    let mut guesses: Vec<Guess> = (1..=count)
        .map(|idx| {
            let active: Vec<usize> = p.swaps[..idx * 2].to_vec();
            let mapping = apply_swaps(p.grid, &active).unwrap_or_else(|| identity(p.grid));
            let luma = score_luma(&p.image, p.grid, &mapping);
            Guess {
                index: idx,
                swaps: active,
                consensus: 0,
                luma,
            }
        })
        .collect();

    let mut luma_rank: Vec<usize> = (0..count).collect();
    luma_rank.sort_by_key(|&i| guesses[i].luma);

    let mut rgb_scores: Vec<(usize, i64)> = Vec::with_capacity(count);
    for (i, g) in guesses.iter().enumerate() {
        let mapping = apply_swaps(p.grid, &g.swaps).unwrap_or_else(|| identity(p.grid));
        let rgb = score_rgb(&p.image, p.grid, &mapping);
        rgb_scores.push((i, rgb));
    }
    let mut rgb_rank: Vec<usize> = (0..count).collect();
    rgb_rank.sort_by_key(|&i| rgb_scores[i].1);

    let mut luma_rank_of = vec![0usize; count];
    for (pos, &orig) in luma_rank.iter().enumerate() {
        luma_rank_of[orig] = pos;
    }
    let mut rgb_rank_of = vec![0usize; count];
    for (pos, &orig) in rgb_rank.iter().enumerate() {
        rgb_rank_of[orig] = pos;
    }
    for (i, g) in guesses.iter_mut().enumerate() {
        g.consensus = (luma_rank_of[i] + rgb_rank_of[i] * 2) as i64;
    }

    guesses.sort_by_key(|g| (g.consensus, g.luma));
    Ok(guesses)
}

fn pick_attempts(guesses: &[Guess], limit: usize) -> Vec<Guess> {
    const MIN_GAP: usize = 3;
    let mut out: Vec<Guess> = Vec::with_capacity(limit);

    let far = |out: &[Guess], idx: usize, gap: usize| -> bool {
        out.iter().all(|g| g.index.abs_diff(idx) >= gap)
    };

    for gap in [MIN_GAP, 1] {
        for g in guesses {
            if out.len() == limit {
                return out;
            }
            if far(&out, g.index, gap) {
                out.push(g.clone());
            }
        }
    }
    out
}

fn identity(grid: usize) -> Vec<usize> {
    (0..grid * grid).collect()
}

fn apply_swaps(grid: usize, swaps: &[usize]) -> Option<Vec<usize>> {
    let total = grid * grid;
    if total == 0 || swaps.len() % 2 != 0 {
        return None;
    }
    let mut map: Vec<usize> = (0..total).collect();
    for pair in swaps.chunks(2) {
        let (a, b) = (pair[0], pair[1]);
        if a >= total || b >= total {
            return None;
        }
        map.swap(a, b);
    }
    Some(map)
}

fn tile_rect(w: u32, h: u32, grid: usize, idx: usize) -> (u32, u32, u32, u32) {
    let row = idx / grid;
    let col = idx % grid;
    let x0 = (col as u32 * w) / grid as u32;
    let x1 = ((col as u32 + 1) * w) / grid as u32;
    let y0 = (row as u32 * h) / grid as u32;
    let y1 = ((row as u32 + 1) * h) / grid as u32;
    (x0, y0, x1, y1)
}

fn score_luma(img: &RgbImage, grid: usize, mapping: &[usize]) -> i64 {
    let (w, h) = img.dimensions();
    let mut total: i64 = 0;
    for row in 0..grid {
        for col in 0..grid.saturating_sub(1) {
            let left = row * grid + col;
            let right = left + 1;
            let lr = tile_rect(w, h, grid, left);
            let rr = tile_rect(w, h, grid, right);
            let ls = tile_rect(w, h, grid, mapping[left]);
            let rs = tile_rect(w, h, grid, mapping[right]);
            for y in 0..(lr.3 - lr.1).min(rr.3 - rr.1) {
                let yy = lr.1 + y;
                let a = sample_luma(img, &lr, &ls, lr.2 - 1, yy);
                let b = sample_luma(img, &rr, &rs, rr.0, yy);
                total += (a as i64 - b as i64).abs();
            }
        }
    }
    for row in 0..grid.saturating_sub(1) {
        for col in 0..grid {
            let top = row * grid + col;
            let bot = top + grid;
            let tr = tile_rect(w, h, grid, top);
            let br = tile_rect(w, h, grid, bot);
            let ts = tile_rect(w, h, grid, mapping[top]);
            let bs = tile_rect(w, h, grid, mapping[bot]);
            for x in 0..(tr.2 - tr.0).min(br.2 - br.0) {
                let xx = tr.0 + x;
                let a = sample_luma(img, &tr, &ts, xx, tr.3 - 1);
                let b = sample_luma(img, &br, &bs, xx, br.1);
                total += (a as i64 - b as i64).abs();
            }
        }
    }
    total
}

fn score_rgb(img: &RgbImage, grid: usize, mapping: &[usize]) -> i64 {
    let (w, h) = img.dimensions();
    let mut total: i64 = 0;
    for row in 0..grid {
        for col in 0..grid.saturating_sub(1) {
            let left = row * grid + col;
            let right = left + 1;
            let lr = tile_rect(w, h, grid, left);
            let rr = tile_rect(w, h, grid, right);
            let ls = tile_rect(w, h, grid, mapping[left]);
            let rs = tile_rect(w, h, grid, mapping[right]);
            for y in 0..(lr.3 - lr.1).min(rr.3 - rr.1) {
                let yy = lr.1 + y;
                let a = sample_rgb(img, &lr, &ls, lr.2 - 1, yy);
                let b = sample_rgb(img, &rr, &rs, rr.0, yy);
                total += color_diff(a, b);
            }
        }
    }
    total
}

fn sample_luma(
    img: &RgbImage,
    dst: &(u32, u32, u32, u32),
    src: &(u32, u32, u32, u32),
    x: u32,
    y: u32,
) -> u8 {
    let dx = dst.2 - dst.0;
    let dy = dst.3 - dst.1;
    let sx = if dx > 0 {
        src.0 + (x.saturating_sub(dst.0)) * (src.2 - src.0) / dx
    } else {
        src.0
    };
    let sy = if dy > 0 {
        src.1 + (y.saturating_sub(dst.1)) * (src.3 - src.1) / dy
    } else {
        src.1
    };
    let p = img.get_pixel(sx, sy);
    ((299 * p[0] as u32 + 587 * p[1] as u32 + 114 * p[2] as u32) / 1000).min(255) as u8
}

fn sample_rgb(
    img: &RgbImage,
    dst: &(u32, u32, u32, u32),
    src: &(u32, u32, u32, u32),
    x: u32,
    y: u32,
) -> [u8; 3] {
    let dx = dst.2 - dst.0;
    let dy = dst.3 - dst.1;
    let sx = if dx > 0 {
        src.0 + (x.saturating_sub(dst.0)) * (src.2 - src.0) / dx
    } else {
        src.0
    };
    let sy = if dy > 0 {
        src.1 + (y.saturating_sub(dst.1)) * (src.3 - src.1) / dy
    } else {
        src.1
    };
    let p = img.get_pixel(sx, sy);
    [p[0], p[1], p[2]]
}

fn color_diff(a: [u8; 3], b: [u8; 3]) -> i64 {
    (a[0] as i64 - b[0] as i64).abs()
        + (a[1] as i64 - b[1] as i64).abs()
        + (a[2] as i64 - b[2] as i64).abs()
}

fn generate_browser_fp() -> String {
    let bytes: [u8; 16] = rand::random();
    hex::encode(bytes)
}
