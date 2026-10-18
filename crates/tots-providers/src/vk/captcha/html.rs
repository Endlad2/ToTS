//! Парсинг HTML страницы капчи.

use regex::Regex;
use serde_json::Value;
use std::sync::OnceLock;

use super::slider::ContentRef;

#[derive(Debug)]
pub struct PowParams {
    pub input: String,
    pub difficulty: u32,
    pub prefix: String,
}

#[derive(Debug)]
pub struct InitData {
    pub found: bool,
    pub show_type: String,
    pub content: ContentRef,
}

#[derive(Debug)]
pub struct Page {
    pub pow: PowParams,
    pub debug_info: String,
    pub init: InitData,
    pub origin: String,
    pub page_origin: String,
}

static RE_POW_ARGS: OnceLock<Regex> = OnceLock::new();
static RE_POW_PREFIX: OnceLock<Regex> = OnceLock::new();
static RE_VK_GLOBAL: OnceLock<Regex> = OnceLock::new();
static RE_INIT_GLOBAL: OnceLock<Regex> = OnceLock::new();
static RE_DEBUG_INFO: OnceLock<Regex> = OnceLock::new();
static RE_ORIGIN: OnceLock<Regex> = OnceLock::new();

fn re_pow_args() -> &'static Regex {
    RE_POW_ARGS.get_or_init(|| {
        Regex::new(r#"\}\(\s*["']([A-Za-z0-9_-]{8,})["']\s*,\s*(\d+)\s*,\s*["'][^"']*["']\s*(?:,|\)\s*\))"#)
            .unwrap()
    })
}

fn re_pow_prefix() -> &'static Regex {
    RE_POW_PREFIX.get_or_init(|| {
        Regex::new(r#"captchaPowResult["'\]]{0,3}\s*=\s*["']([A-Za-z0-9._-]{0,8})["']\s*\+"#)
            .unwrap()
    })
}

fn re_vk_global() -> &'static Regex {
    RE_VK_GLOBAL.get_or_init(|| Regex::new(r"window\.vk\s*=\s*\{").unwrap())
}

fn re_init_global() -> &'static Regex {
    RE_INIT_GLOBAL.get_or_init(|| Regex::new(r"window\.init\s*=\s*\{").unwrap())
}

fn re_debug_info() -> &'static Regex {
    RE_DEBUG_INFO.get_or_init(|| {
        Regex::new(r#"[A-Za-z_$][\w$]*:\s*"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})""#)
            .unwrap()
    })
}

fn re_origin() -> &'static Regex {
    RE_ORIGIN.get_or_init(|| Regex::new(r#"apiDomain["']?\s*:\s*["']([^"']+)["']"#).unwrap())
}

pub fn parse_page(html: &str) -> Result<Page, String> {
    // Проверяем, что это действительно страница капчи (а не 429/5xx заглушка).
    if !re_vk_global().is_match(html) && !re_init_global().is_match(html) {
        return Err("not a captcha page".into());
    }

    let pow = parse_pow(html)?;
    let debug_info =
        parse_debug_info(html).ok_or_else(|| "captcha debug_info not found".to_string())?;
    let init = parse_init(html);
    let origin = re_origin()
        .captures(html)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
        .unwrap_or_else(|| "api.vk.ru".into());

    Ok(Page {
        pow,
        debug_info,
        init,
        origin: origin.clone(),
        page_origin: format!("https://{}", origin),
    })
}

fn parse_pow(html: &str) -> Result<PowParams, String> {
    let m = re_pow_args()
        .captures(html)
        .ok_or_else(|| "pow args not found".to_string())?;
    let input = m.get(1).unwrap().as_str().to_string();
    let difficulty: u32 = m
        .get(2)
        .unwrap()
        .as_str()
        .parse()
        .map_err(|_| "bad difficulty".to_string())?;
    if difficulty == 0 {
        return Err("zero difficulty".into());
    }
    let prefix = re_pow_prefix()
        .captures(html)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
        .ok_or_else(|| "pow envelope not recognized".to_string())?;
    Ok(PowParams {
        input,
        difficulty,
        prefix,
    })
}

fn parse_debug_info(html: &str) -> Option<String> {
    let m = re_vk_global().find(html)?;
    let block = balanced_json_object(&html[m.end() - 1..])?;
    let found = re_debug_info().captures(&block)?;
    Some(found.get(1)?.as_str().to_string())
}

fn parse_init(html: &str) -> InitData {
    let Some(m) = re_init_global().find(html) else {
        return InitData {
            found: false,
            show_type: String::new(),
            content: ContentRef::default(),
        };
    };
    let Some(raw) = balanced_json_object(&html[m.end() - 1..]) else {
        return InitData {
            found: false,
            show_type: String::new(),
            content: ContentRef::default(),
        };
    };
    let Ok(parsed) = serde_json::from_str::<Value>(&raw) else {
        return InitData {
            found: false,
            show_type: String::new(),
            content: ContentRef::default(),
        };
    };
    let show_type = parsed
        .get("data")
        .and_then(|d| d.get("show_captcha_type"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let content = parsed
        .get("data")
        .and_then(|d| d.get("captcha_settings"))
        .map(|v| super::api::parse_content_ref(Some(v)))
        .unwrap_or_default();
    InitData {
        found: !show_type.is_empty(),
        show_type,
        content,
    }
}

/// Возвращает первый сбалансированный JSON-объект с учётом строк.
pub fn balanced_json_object(s: &str) -> Option<String> {
    let mut depth = 0;
    let mut in_str = false;
    let mut esc = false;
    for (i, c) in s.char_indices() {
        if in_str {
            if esc {
                esc = false;
            } else if c == '\\' {
                esc = true;
            } else if c == '"' {
                in_str = false;
            }
            continue;
        }
        match c {
            '"' => in_str = true,
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(s[..=i].to_string());
                }
            }
            _ => {}
        }
    }
    None
}
