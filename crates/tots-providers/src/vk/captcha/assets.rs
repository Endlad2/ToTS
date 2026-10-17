//! Загрузка CSS/JS/шрифтов страницы капчи — VK сверяет наличие запросов.

use std::sync::Arc;

use rand::Rng;
use regex::Regex;
use reqwest::cookie::Jar;
use reqwest::Client;
use tracing::debug;

use crate::vk::persona;

const ASSET_MAX: usize = 40;

const ASSET_HOSTS: &[&str] = &[
    "static.vk.ru",
    "static.vk.com",
    "st.vk.ru",
    "st.vk.com",
    "sun9-",
    "userapi.com",
    "okcdn.ru",
    "mycdn.me",
];

static RE_SCRIPT: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
static RE_LINK: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
static RE_HREF: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
static RE_IMG: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();

fn re_script() -> &'static Regex {
    RE_SCRIPT.get_or_init(|| Regex::new(r#"<script[^>]+src="([^"]+)""#).unwrap())
}
fn re_link() -> &'static Regex {
    RE_LINK.get_or_init(|| Regex::new(r"<link[^>]+>").unwrap())
}
fn re_href() -> &'static Regex {
    RE_HREF.get_or_init(|| Regex::new(r#"href="([^"]+)""#).unwrap())
}
fn re_img() -> &'static Regex {
    RE_IMG.get_or_init(|| Regex::new(r#"<img[^>]+src="([^"]+)""#).unwrap())
}

fn is_allowed(url: &str) -> bool {
    for host in ASSET_HOSTS {
        if url.contains(host) {
            return true;
        }
    }
    false
}

fn absolutize(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.starts_with("https://") {
        Some(raw.to_string())
    } else if raw.starts_with("//") {
        Some(format!("https:{}", raw))
    } else {
        None
    }
}

pub fn collect_assets(html: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();

    let mut push = |url: &str| {
        if let Some(abs) = absolutize(url) {
            if is_allowed(&abs) && seen.insert(abs.clone()) && out.len() < ASSET_MAX {
                out.push(abs);
            }
        }
    };

    for cap in re_script().captures_iter(html) {
        push(&cap[1]);
    }
    for m in re_link().find_iter(html) {
        if let Some(href) = re_href().captures(m.as_str()) {
            push(&href[1]);
        }
    }
    for cap in re_img().captures_iter(html) {
        push(&cap[1]);
    }

    out
}

pub async fn load_assets_async(
    http: Client,
    cookie_jar: Arc<Jar>,
    html: &str,
    page_url: &str,
) -> Result<(), ()> {
    let assets = collect_assets(html);
    if assets.is_empty() {
        return Ok(());
    }
    debug!(count = assets.len(), "captcha: loading assets");

    for asset in assets {
        let referer = format!("{}/", page_url.trim_end_matches('/'));
        let req = http
            .get(&asset)
            .headers(persona::base_headers())
            .header("Referer", referer)
            .header("Accept", "*/*")
            .header("Sec-Fetch-Site", "same-site")
            .header("Sec-Fetch-Mode", "no-cors")
            .header("Sec-Fetch-Dest", "image");
        let _ = req.send().await;
        // Небольшой джиттер между запросами.
        tokio::time::sleep(std::time::Duration::from_millis(
            rand::thread_rng().gen_range(20..80),
        ))
        .await;
    }
    let _ = cookie_jar;
    Ok(())
}
