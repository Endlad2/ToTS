//! Proof-of-work: SHA-256 перебор + телеметрия + base64url-конверт.

use base64::Engine;
use rand::Rng;
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::api::CaptchaError;
use super::html::PowParams;

#[derive(Serialize)]
struct Telemetry {
    globals: Globals,
    ua: UA,
    frame: Frame,
    match_media: MatchMedia,
    plugins: Plugins,
    nav_tamper: NavTamper,
    referrer: Referrer,
    devtools: DevTools,
    css: CSS,
    native_integrity: NativeIntegrity,
    cookie_test: CookieTest,
    ancestor_origins: AncestorOrigins,
    sandbox_behavior: SandboxBehavior,
    max_touch_points: MaxTouchPoints,
    timezone_locale: TimezoneLocale,
    device_pixel_ratio: DevicePixelRatio,
}

#[derive(Serialize)]
struct Globals {
    doc: bool,
    win: bool,
    nav: bool,
    webdriver: bool,
    subtle: bool,
    secure: bool,
    gcs: bool,
    raf: bool,
    wasm: bool,
    plugins_len: u32,
    languages_len: u32,
    hw: u32,
    mem: u32,
}

#[derive(Serialize)]
struct UA {
    #[serde(rename = "userAgent")]
    user_agent: String,
    #[serde(rename = "userAgentData")]
    user_agent_data: UserAgentData,
}

#[derive(Serialize)]
struct UserAgentData {
    brands: Vec<Brand>,
    platform: String,
    mobile: bool,
    architecture: Option<String>,
}

#[derive(Serialize)]
struct Brand {
    brand: String,
    version: String,
}

#[derive(Serialize)]
struct Frame {
    #[serde(rename = "frameElement")]
    frame_element: Option<String>,
    #[serde(rename = "ancestorOriginsLen")]
    ancestor_origins_len: u32,
    #[serde(rename = "parentAccessible")]
    parent_accessible: bool,
}

#[derive(Serialize)]
struct MatchMedia {
    #[serde(rename = "prefersDark")]
    prefers_dark: bool,
    #[serde(rename = "prefersLight")]
    prefers_light: bool,
    #[serde(rename = "reducedMotion")]
    reduced_motion: bool,
    #[serde(rename = "pointerFine")]
    pointer_fine: bool,
}

#[derive(Serialize)]
struct Plugins {
    length: u32,
    names: Vec<String>,
    descriptions: Vec<String>,
    #[serde(rename = "mimeTypes")]
    mime_types: Vec<Vec<String>>,
    #[serde(rename = "isChrome")]
    is_chrome: bool,
}

#[derive(Serialize)]
struct NavTamper {
    tampered: bool,
    #[serde(rename = "el_ctor")]
    el_ctor: String,
    #[serde(rename = "style_ctor")]
    style_ctor: String,
    #[serde(rename = "nav_ctor")]
    nav_ctor: String,
    #[serde(rename = "alert_native")]
    alert_native: bool,
    #[serde(rename = "to_string_native")]
    to_string_native: bool,
}

#[derive(Serialize)]
struct Referrer {
    referrer: String,
    #[serde(rename = "inIframe")]
    in_iframe: bool,
    domain: String,
}

#[derive(Serialize)]
struct DevTools {
    open: bool,
    #[serde(rename = "delay_ms")]
    delay_ms: u32,
}

#[derive(Serialize)]
struct CSS {
    #[serde(rename = "expectedMissing")]
    expected_missing: u32,
}

#[derive(Serialize)]
struct NativeIntegrity {
    #[serde(rename = "protoMatch")]
    proto_match: bool,
    #[serde(rename = "xhrNative")]
    xhr_native: bool,
    #[serde(rename = "xhrSendNative")]
    xhr_send_native: bool,
    #[serde(rename = "addEventListenerNative")]
    add_event_listener_native: bool,
    #[serde(rename = "alertNative")]
    alert_native: bool,
    #[serde(rename = "toStringNative")]
    to_string_native: bool,
}

#[derive(Serialize)]
struct CookieTest {
    write: bool,
}

#[derive(Serialize)]
struct AncestorOrigins {
    #[serde(rename = "ancestorOrigin")]
    ancestor_origin: Option<String>,
}

#[derive(Serialize)]
struct SandboxBehavior {
    #[serde(rename = "originIsNull")]
    origin_is_null: bool,
    #[serde(rename = "localStorage")]
    local_storage: bool,
    #[serde(rename = "sessionStorage")]
    session_storage: bool,
}

#[derive(Serialize)]
struct MaxTouchPoints {
    #[serde(rename = "maxTouchPoints")]
    max_touch_points: u32,
}

#[derive(Serialize)]
struct TimezoneLocale {
    timezone: String,
    languages: Vec<String>,
}

#[derive(Serialize)]
struct DevicePixelRatio {
    dpr: f64,
    orientation: String,
    #[serde(rename = "orientationAngle")]
    orientation_angle: u32,
}

const CHROME_PDF_PLUGINS: &[&str] = &[
    "PDF Viewer",
    "Chrome PDF Viewer",
    "Chromium PDF Viewer",
    "Microsoft Edge PDF Viewer",
    "WebKit built-in PDF",
];

pub async fn solve_and_wrap(
    pow: &PowParams,
    debug_info: String,
    _api_origin: &str,
    page_origin: &str,
    domain: &str,
) -> Result<String, CaptchaError> {
    let _ = debug_info;

    // 1. Решаем PoW.
    let (hash, nonce) = solve_pow(&pow.input, pow.difficulty)
        .ok_or_else(|| CaptchaError::PowParse("pow failed".into()))?;

    // 2. Собираем телеметрию.
    let telemetry = build_telemetry(page_origin, domain);
    let telemetry_json =
        serde_json::to_vec(&telemetry).map_err(|e| CaptchaError::PowParse(e.to_string()))?;

    // 3. Хешируем канонизированную телеметрию.
    let tel_hash = {
        let canonical =
            canonical_json(&telemetry_json).map_err(|e| CaptchaError::PowParse(e.to_string()))?;
        let sum = Sha256::digest(&canonical);
        hex::encode(sum)
    };

    // 4. Формируем конверт.
    let envelope = format!(
        r#"{{"hash":"{}","nonce":{},"duration_ms":{},"telemetry":{},"tel_hash":"{}"}}"#,
        hash,
        nonce,
        pow_duration_ms(nonce),
        std::str::from_utf8(&telemetry_json).unwrap_or("{}"),
        tel_hash
    );

    // 5. Base64.
    let encoded = base64::engine::general_purpose::STANDARD.encode(envelope.as_bytes());
    Ok(format!("{}{}", pow.prefix, encoded))
}

fn solve_pow(input: &str, difficulty: u32) -> Option<(String, u32)> {
    let target = "0".repeat(difficulty as usize);
    let mut buf = Vec::with_capacity(input.len() + 20);
    buf.extend_from_slice(input.as_bytes());
    for nonce in 0..10_000_000u32 {
        buf.truncate(input.len());
        buf.extend_from_slice(nonce.to_string().as_bytes());
        let sum = Sha256::digest(&buf);
        let hex_hash = hex::encode(sum);
        if hex_hash.starts_with(&target) {
            return Some((hex_hash, nonce));
        }
    }
    None
}

fn pow_duration_ms(nonce: u32) -> u64 {
    ((nonce as f64 + 1.0) * 0.015).round().max(1.0) as u64
}

fn canonical_json(data: &[u8]) -> Result<Vec<u8>, serde_json::Error> {
    let v: serde_json::Value = serde_json::from_slice(data)?;
    let canonical = sort_value(&v);
    serde_json::to_vec(&canonical)
}

fn sort_value(v: &serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match v {
        Value::Object(m) => {
            let mut sorted: Vec<_> = m.iter().collect();
            sorted.sort_by_key(|(k, _)| *k);
            let mut out = serde_json::Map::new();
            for (k, v) in sorted {
                out.insert(k.clone(), sort_value(v));
            }
            Value::Object(out)
        }
        Value::Array(a) => Value::Array(a.iter().map(sort_value).collect()),
        other => other.clone(),
    }
}

fn build_telemetry(page_origin: &str, domain: &str) -> Telemetry {
    let mut rng = rand::thread_rng();
    let hw: u32 = [8, 12, 16, 24][rng.gen_range(0..4)];
    let mem: u32 = [4, 8, 8, 16][rng.gen_range(0..4)];
    let dpr: f64 = [1.0, 1.25, 1.5, 2.0][rng.gen_range(0..4)];
    let prefers_dark = rng.gen_bool(0.5);

    Telemetry {
        globals: Globals {
            doc: true,
            win: true,
            nav: true,
            webdriver: false,
            subtle: true,
            secure: true,
            gcs: true,
            raf: true,
            wasm: true,
            plugins_len: CHROME_PDF_PLUGINS.len() as u32,
            languages_len: 2,
            hw,
            mem,
        },
        ua: UA {
            user_agent: crate::vk::persona::USER_AGENT_CHROME_WINDOWS.to_string(),
            user_agent_data: UserAgentData {
                brands: vec![
                    Brand {
                        brand: "Google Chrome".into(),
                        version: "146".into(),
                    },
                    Brand {
                        brand: "Chromium".into(),
                        version: "146".into(),
                    },
                    Brand {
                        brand: "Not)A;Brand".into(),
                        version: "24".into(),
                    },
                ],
                platform: "Windows".into(),
                mobile: false,
                architecture: None,
            },
        },
        frame: Frame {
            frame_element: None,
            ancestor_origins_len: 0,
            parent_accessible: true,
        },
        match_media: MatchMedia {
            prefers_dark,
            prefers_light: !prefers_dark,
            reduced_motion: false,
            pointer_fine: true,
        },
        plugins: Plugins {
            length: CHROME_PDF_PLUGINS.len() as u32,
            names: CHROME_PDF_PLUGINS.iter().map(|s| s.to_string()).collect(),
            descriptions: CHROME_PDF_PLUGINS
                .iter()
                .map(|_| "Portable Document Format".into())
                .collect(),
            mime_types: CHROME_PDF_PLUGINS
                .iter()
                .map(|_| vec!["application/pdf".to_string(), "text/pdf".to_string()])
                .collect(),
            is_chrome: true,
        },
        nav_tamper: NavTamper {
            tampered: false,
            el_ctor: "HTMLDivElement".into(),
            style_ctor: "CSSStyleDeclaration".into(),
            nav_ctor: "Navigator".into(),
            alert_native: true,
            to_string_native: true,
        },
        referrer: Referrer {
            referrer: format!("https://{}/", domain),
            in_iframe: false,
            domain: page_origin.trim_start_matches("https://").to_string(),
        },
        devtools: DevTools {
            open: false,
            delay_ms: 0,
        },
        css: CSS {
            expected_missing: 0,
        },
        native_integrity: NativeIntegrity {
            proto_match: true,
            xhr_native: true,
            xhr_send_native: true,
            add_event_listener_native: true,
            alert_native: true,
            to_string_native: true,
        },
        cookie_test: CookieTest { write: true },
        ancestor_origins: AncestorOrigins {
            ancestor_origin: None,
        },
        sandbox_behavior: SandboxBehavior {
            origin_is_null: false,
            local_storage: true,
            session_storage: true,
        },
        max_touch_points: MaxTouchPoints { max_touch_points: 0 },
        timezone_locale: TimezoneLocale {
            timezone: "Europe/Moscow".into(),
            languages: vec!["ru-RU".into(), "en-US".into()],
        },
        device_pixel_ratio: DevicePixelRatio {
            dpr,
            orientation: "landscape-primary".into(),
            orientation_angle: 0,
        },
    }
}
