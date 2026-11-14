use super::native_channel::{pair, provider_error, remember_url, ChannelEndpoint};
use crate::{
    providers::{Provider, ProviderBootstrap},
    transport::DatagramPath,
    Error, Result,
};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use flate2::{write::GzEncoder, Compression};
use futures_util::{SinkExt, StreamExt};
use rand::{rngs::OsRng, RngCore};
use reqwest::{
    cookie::{CookieStore, Jar},
    redirect::Policy,
    Client,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    io::Write,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::oneshot;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue, Message},
};
use tracing::{debug, warn};

const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/137 Safari/537.36";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
const KEEPALIVE: Duration = Duration::from_secs(10);

pub struct YaDocsProvider;

#[derive(Clone)]
struct DocInfo {
    token: String,
    doc_id: String,
    origin: String,
    ws_url: String,
    cookie: String,
    permissions: Value,
    open_cmd: Value,
}

pub fn configured_urls(data: &Value) -> Result<Vec<String>> {
    if let Some(values) = data.get("urls") {
        let values = values
            .as_array()
            .ok_or_else(|| Error::Provider("ya-docs.urls must be an array".into()))?;
        let mut urls = Vec::with_capacity(values.len());
        for value in values {
            let url = value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| Error::Provider("ya-docs.urls contains an invalid URL".into()))?;
            urls.push(url.to_owned());
        }
        if urls.is_empty() {
            return Err(Error::Provider("ya-docs.urls cannot be empty".into()));
        }
        return Ok(urls);
    }
    let url = data
        .get("url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::Provider("ya-docs requires url or urls".into()))?;
    Ok(vec![url.to_owned()])
}

pub fn worker_data(data: &Value, worker_id: usize) -> Result<Value> {
    let urls = configured_urls(data)?;
    let mut selected = data.clone();
    let object = selected
        .as_object_mut()
        .ok_or_else(|| Error::Provider("ya-docs data must be an object".into()))?;
    object.remove("urls");
    object.insert("url".into(), Value::String(urls[worker_id % urls.len()].clone()));
    Ok(selected)
}

#[async_trait]
impl Provider for YaDocsProvider {
    async fn bootstrap(&self, _data: &Value) -> Result<ProviderBootstrap> {
        Ok(ProviderBootstrap::default())
    }

    async fn connect_native(&self, data: &Value) -> Result<Option<Arc<dyn DatagramPath>>> {
        let url = configured_urls(data)?
            .into_iter()
            .next()
            .expect("validated non-empty ya-docs urls");
        let (path, endpoint) = pair("ya-docs");
        let data = data.clone();
        let (ready_tx, ready_rx) = oneshot::channel();
        tokio::spawn(async move {
            if let Err(error) = run(data, url, endpoint, ready_tx).await {
                warn!(%error, "ya-docs carrier stopped");
            }
        });
        match tokio::time::timeout(CONNECT_TIMEOUT, ready_rx).await {
            Ok(Ok(Ok(()))) => Ok(Some(path)),
            Ok(Ok(Err(message))) => {
                let _ = path.close().await;
                Err(Error::Provider(message))
            }
            Ok(Err(_)) => {
                let _ = path.close().await;
                Err(Error::Provider("ya-docs startup task ended".into()))
            }
            Err(_) => {
                let _ = path.close().await;
                Err(Error::Provider("ya-docs connection timed out".into()))
            }
        }
    }
}

async fn run(
    data: Value,
    url: String,
    mut endpoint: ChannelEndpoint,
    ready: oneshot::Sender<std::result::Result<(), String>>,
) -> Result<()> {
    remember_url(&endpoint.route_ips, &url).await;
    let jar = Arc::new(Jar::default());
    seed_cookies(&jar, &url, &data)?;
    let client = Client::builder()
        .cookie_provider(jar.clone())
        .redirect(Policy::none())
        .timeout(Duration::from_secs(20))
        .build()
        .map_err(|e| provider_error("ya-docs HTTP client", e))?;

    let mut ready = Some(ready);
    let mut pending: Option<Vec<u8>> = None;
    let mut attempt = 0u32;
    loop {
        if *endpoint.shutdown.borrow() {
            return Ok(());
        }
        match connect_and_serve(
            &client,
            jar.clone(),
            &url,
            &mut endpoint,
            &mut pending,
            &mut ready,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(error) => {
                if let Some(ready) = ready.take() {
                    let _ = ready.send(Err(error.to_string()));
                    return Err(error);
                }
                attempt = attempt.saturating_add(1);
                warn!(attempt, %error, "ya-docs disconnected; reconnecting");
                let delay = Duration::from_millis((500u64 << attempt.min(5)).min(15_000));
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    changed = endpoint.shutdown.changed() => {
                        if changed.is_err() || *endpoint.shutdown.borrow() { return Ok(()); }
                    }
                }
            }
        }
    }
}

async fn connect_and_serve(
    client: &Client,
    jar: Arc<Jar>,
    doc_url: &str,
    endpoint: &mut ChannelEndpoint,
    pending: &mut Option<Vec<u8>>,
    ready: &mut Option<oneshot::Sender<std::result::Result<(), String>>>,
) -> Result<()> {
    let user_id = random_user_id();
    let info = fetch_doc_info(client, jar, doc_url, &user_id).await?;
    remember_url(&endpoint.route_ips, &info.ws_url).await;

    let mut request = info
        .ws_url
        .clone()
        .into_client_request()
        .map_err(|e| provider_error("ya-docs WebSocket request", e))?;
    request.headers_mut().insert(
        "User-Agent",
        HeaderValue::from_static(USER_AGENT),
    );
    request.headers_mut().insert(
        "Origin",
        HeaderValue::from_str(&info.origin)
            .map_err(|e| provider_error("ya-docs Origin header", e))?,
    );
    if !info.cookie.is_empty() {
        request.headers_mut().insert(
            "Cookie",
            HeaderValue::from_str(&info.cookie)
                .map_err(|e| provider_error("ya-docs Cookie header", e))?,
        );
    }

    let (mut socket, _) = connect_async(request)
        .await
        .map_err(|e| provider_error("ya-docs WebSocket connect", e))?;

    // Engine.IO open may be the first frame. It is harmless if the endpoint accepts auth sooner,
    // but consuming it first avoids racing the Socket.IO namespace handshake.
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = socket.next().await {
            let message = message.map_err(|e| provider_error("ya-docs handshake read", e))?;
            if let Message::Text(text) = message {
                if text.starts_with("0{") {
                    break;
                }
            }
        }
        Ok::<_, Error>(())
    })
    .await;

    socket
        .send(Message::Text(format!("40{{\"token\":\"{}\"}}", info.token).into()))
        .await
        .map_err(|e| provider_error("ya-docs Socket.IO connect", e))?;
    let auth = json!([
        "message",
        {
            "type":"auth",
            "docid":info.doc_id,
            "token":"fghhfgsjdgfjs",
            "user":{"id":user_id},
            "editorType":0,
            "lastOtherSaveTime":-1,
            "permissions":info.permissions,
            "openCmd":info.open_cmd,
            "coEditingMode":"fast",
            "jwtOpen":info.token
        }
    ]);
    socket
        .send(Message::Text(format!("42{}", auth).into()))
        .await
        .map_err(|e| provider_error("ya-docs auth", e))?;

    let (write, mut read) = socket.split();
    let (socket_tx, mut socket_rx) = tokio::sync::mpsc::channel::<Message>(4096);
    let mut writer = tokio::spawn(async move {
        let mut write = write;
        while let Some(message) = socket_rx.recv().await {
            write.send(message).await?;
        }
        Ok::<(), tokio_tungstenite::tungstenite::Error>(())
    });
    let mut keepalive = tokio::time::interval(KEEPALIVE);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    if let Some(data) = pending.take() {
        if socket_tx.send(cursor_message(&data)).await.is_err() {
            *pending = Some(data);
            return Err(Error::Transport("ya-docs writer stopped".into()));
        }
    }

    if let Some(ready) = ready.take() {
        let _ = ready.send(Ok(()));
    }
    debug!(url=%doc_url, "ya-docs carrier connected");
    loop {
        tokio::select! {
            outgoing = endpoint.outbound.recv() => {
                let Some(data) = outgoing else { return Ok(()); };
                if socket_tx.send(cursor_message(&data)).await.is_err() {
                    *pending = Some(data);
                    return Err(Error::Transport("ya-docs writer stopped".into()));
                }
            }
            incoming = read.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if text.as_str() == "2" {
                            let _ = socket_tx.send(Message::Text("3".into())).await;
                            continue;
                        }
                        for payload in cursor_payloads(text.as_str()) {
                            if let Ok(bytes) = STANDARD.decode(payload) {
                                let _ = endpoint.inbound.try_send(bytes);
                            }
                        }
                    }
                    Some(Ok(Message::Binary(_))) | Some(Ok(Message::Pong(_))) | Some(Ok(Message::Ping(_))) => {}
                    Some(Ok(Message::Close(_))) | None => return Err(Error::Transport("ya-docs WebSocket closed".into())),
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(provider_error("ya-docs WebSocket read", error)),
                }
            }
            _ = keepalive.tick() => {
                let _ = socket_tx.send(Message::Text("42[\"message\",{\"type\":\"cursor\",\"cursor\":\"18;---KA---\"}]".into())).await;
            }
            result = &mut writer => {
                match result {
                    Ok(Ok(())) => return Err(Error::Transport("ya-docs writer ended".into())),
                    Ok(Err(error)) => return Err(provider_error("ya-docs WebSocket write", error)),
                    Err(error) => return Err(provider_error("ya-docs writer task", error)),
                }
            }
            changed = endpoint.shutdown.changed() => {
                if changed.is_err() || *endpoint.shutdown.borrow() {
                    writer.abort();
                    return Ok(());
                }
            }
        }
    }
}

fn cursor_message(data: &[u8]) -> Message {
    let payload = STANDARD.encode(data);
    Message::Text(
        format!(
            "42[\"message\",{{\"type\":\"cursor\",\"cursor\":\"{};{}\"}}]",
            data.len(), payload
        )
        .into(),
    )
}

fn cursor_payloads(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let marker = "\"cursor\":\"";
    let mut rest = text;
    while let Some(start) = rest.find(marker) {
        let value = &rest[start + marker.len()..];
        let Some(end) = value.find('"') else {
            break;
        };
        let value = &value[..end];
        if let Some((_, payload)) = value.split_once(';') {
            if payload != "---KA---" && !payload.is_empty() {
                out.push(payload);
            }
        }
        rest = &rest[start + marker.len() + end + 1..];
    }
    out
}

async fn fetch_doc_info(client: &Client, jar: Arc<Jar>, raw_url: &str, user_id: &str) -> Result<DocInfo> {
    let mut current = reqwest::Url::parse(raw_url)
        .map_err(|e| provider_error("invalid ya-docs URL", e))?;
    let mut final_url = None;
    let mut html = None;
    for _ in 0..10 {
        let response = client
            .get(current.clone())
            .header("User-Agent", USER_AGENT)
            .send()
            .await
            .map_err(|e| provider_error("ya-docs document request", e))?;
        if response.status().is_success() {
            final_url = Some(response.url().clone());
            html = Some(
                response
                    .text()
                    .await
                    .map_err(|e| provider_error("ya-docs document body", e))?,
            );
            break;
        }
        if response.status().is_redirection() {
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| Error::Provider("ya-docs redirect without Location".into()))?;
            if location.contains("showcaptchafast") {
                let captcha_url = current
                    .join(location)
                    .or_else(|_| reqwest::Url::parse(location))
                    .map_err(|e| provider_error("ya-docs captcha URL", e))?;
                let next = solve_pow_captcha(client, captcha_url).await?;
                current = next.unwrap_or_else(|| reqwest::Url::parse(raw_url).expect("validated ya-docs URL"));
                continue;
            }
            if location.contains("showcaptcha") {
                return Err(Error::Provider("ya-docs SmartCaptcha requires browser cookies".into()));
            }
            if location.contains("passport.yandex") {
                return Err(Error::Provider("ya-docs document requires Yandex login".into()));
            }
            current = current
                .join(location)
                .or_else(|_| reqwest::Url::parse(location))
                .map_err(|e| provider_error("ya-docs redirect URL", e))?;
            continue;
        }
        return Err(Error::Provider(format!(
            "ya-docs document returned HTTP {}",
            response.status()
        )));
    }
    let html = html.ok_or_else(|| Error::Provider("ya-docs redirect limit exceeded".into()))?;
    let final_url = final_url.unwrap_or(current);
    let config_json = extract_client_config(&html)
        .ok_or_else(|| Error::Provider("ya-docs client-config not found".into()))?;
    let config: Value = serde_json::from_str(config_json)
        .map_err(|e| provider_error("ya-docs client-config JSON", e))?;
    let office = config
        .get("officeActionData")
        .and_then(Value::as_object)
        .ok_or_else(|| Error::Provider("ya-docs officeActionData missing".into()))?;
    let editor = office
        .get("editor_config")
        .and_then(Value::as_object)
        .ok_or_else(|| Error::Provider("ya-docs editor_config missing".into()))?;
    let document = editor
        .get("document")
        .and_then(Value::as_object)
        .ok_or_else(|| Error::Provider("ya-docs document metadata missing".into()))?;
    let token = editor
        .get("token")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::Provider("ya-docs token missing".into()))?;
    let doc_id = document
        .get("key")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::Provider("ya-docs document key missing".into()))?;
    let balancer = office
        .get("balancer_url")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::Provider("ya-docs balancer_url missing".into()))?;
    let balancer_url = reqwest::Url::parse(balancer)
        .map_err(|e| provider_error("ya-docs balancer URL", e))?;
    let host = balancer_url
        .host_str()
        .ok_or_else(|| Error::Provider("ya-docs balancer host missing".into()))?;
    let ws_url = format!(
        "wss://{host}/2024.1.1-375/doc/{doc_id}/c/?EIO=4&transport=websocket"
    );
    let cookie = jar
        .cookies(&final_url)
        .and_then(|value| value.to_str().ok().map(str::to_owned))
        .unwrap_or_default();
    let permissions = document
        .get("permissions")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let open_cmd = json!({
        "c":"open",
        "id":doc_id,
        "userid":user_id,
        "format":document.get("fileType").cloned().unwrap_or(Value::Null),
        "url":document.get("url").cloned().unwrap_or(Value::Null),
        "title":document.get("title").cloned().unwrap_or(Value::Null),
        "lcid":25
    });
    Ok(DocInfo {
        token: token.to_owned(),
        doc_id: doc_id.to_owned(),
        origin: balancer.to_owned(),
        ws_url,
        cookie,
        permissions,
        open_cmd,
    })
}

fn seed_cookies(jar: &Jar, raw_url: &str, data: &Value) -> Result<()> {
    let Some(values) = data.get("cookies") else {
        return Ok(());
    };
    let values = values
        .as_object()
        .ok_or_else(|| Error::Provider("ya-docs.cookies must be an object".into()))?;
    let url = reqwest::Url::parse(raw_url).map_err(|e| provider_error("ya-docs cookie URL", e))?;
    for (name, value) in values {
        let value = value
            .as_str()
            .ok_or_else(|| Error::Provider("ya-docs cookie values must be strings".into()))?;
        jar.add_cookie_str(&format!("{name}={value}; Path=/"), &url);
    }
    Ok(())
}

fn extract_client_config(html: &str) -> Option<&str> {
    let id = html.find("id=\"client-config\"")?;
    let start = html[id..].find('>')? + id + 1;
    let end = html[start..].find("</script>")? + start;
    Some(html[start..end].trim())
}

fn random_user_id() -> String {
    let mut bytes = [0u8; 8];
    OsRng.fill_bytes(&mut bytes);
    let value = u64::from_le_bytes(bytes) % 1_000_000_000;
    format!("{value:010}")
}


#[derive(serde::Deserialize)]
struct PowChallenge {
    #[serde(rename = "uniqueKey")]
    unique_key: String,
    pow: PowSpec,
}

#[derive(serde::Deserialize)]
struct PowSpec {
    complexity: usize,
    prefix: String,
}

async fn solve_pow_captcha(client: &Client, captcha_url: reqwest::Url) -> Result<Option<reqwest::Url>> {
    let response = client
        .get(captcha_url.clone())
        .header("User-Agent", USER_AGENT)
        .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        .header("Accept-Language", "en-US,en;q=0.9")
        .send()
        .await
        .map_err(|e| provider_error("ya-docs captcha page", e))?;
    if !response.status().is_success() {
        return Err(Error::Provider(format!(
            "ya-docs proof-of-work page returned HTTP {}",
            response.status()
        )));
    }
    let html = response
        .text()
        .await
        .map_err(|e| provider_error("ya-docs captcha body", e))?;
    let encoded = extract_between(&html, "JSON.parse(atob(\"", "\"))")
        .ok_or_else(|| Error::Provider("ya-docs captcha challenge missing".into()))?;
    let raw = STANDARD
        .decode(encoded)
        .map_err(|e| provider_error("ya-docs captcha challenge base64", e))?;
    let challenge: PowChallenge = serde_json::from_slice(&raw)
        .map_err(|e| provider_error("ya-docs captcha challenge JSON", e))?;
    if challenge.pow.complexity > 256 {
        return Err(Error::Provider("ya-docs captcha complexity is invalid".into()));
    }
    let action = captcha_form_action(&html, &captcha_url)?;
    let prefix = decode_hex(&challenge.pow.prefix)
        .unwrap_or_else(|| challenge.pow.prefix.as_bytes().to_vec());
    let complexity = challenge.pow.complexity;
    let nonce = tokio::task::spawn_blocking(move || solve_pow(prefix, complexity))
        .await
        .map_err(|e| provider_error("ya-docs captcha worker", e))??;
    let fingerprint = encode_fingerprint(&nonce)?;
    let body = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("version", "1.5.0")
        .append_pair("uniquekey", &challenge.unique_key)
        .append_pair("chstate", "ok")
        .append_pair("fingerprint", &fingerprint)
        .finish();
    let response = client
        .post(action.clone())
        .header("User-Agent", USER_AGENT)
        .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        .header("Accept-Language", "en-US,en;q=0.9")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Origin", "https://docs.yandex.ru")
        .header("Referer", captcha_url.as_str())
        .body(body)
        .send()
        .await
        .map_err(|e| provider_error("ya-docs captcha submit", e))?;
    if !response.status().is_redirection() {
        return Err(Error::Provider(format!(
            "ya-docs proof-of-work submit returned HTTP {}",
            response.status()
        )));
    }
    let Some(location) = response
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(None);
    };
    let next = action
        .join(location)
        .or_else(|_| reqwest::Url::parse(location))
        .map_err(|e| provider_error("ya-docs captcha return URL", e))?;
    debug!("ya-docs proof-of-work captcha solved");
    Ok(Some(next))
}

fn solve_pow(prefix: Vec<u8>, complexity: usize) -> Result<String> {
    let mut nonce = [0u8; 16];
    for _ in 0..10_000_000u32 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        nonce[..8].copy_from_slice(&now.to_le_bytes());
        OsRng.fill_bytes(&mut nonce[8..]);
        let mut hasher = Sha256::new();
        hasher.update(nonce);
        hasher.update(&prefix);
        let digest = hasher.finalize();
        if has_leading_zero_bits(&digest, complexity) {
            return Ok(hex_encode(&nonce));
        }
    }
    Err(Error::Provider("ya-docs proof-of-work search exhausted".into()))
}

fn has_leading_zero_bits(bytes: &[u8], bits: usize) -> bool {
    if bits > bytes.len() * 8 {
        return false;
    }
    let whole = bits / 8;
    if bytes.iter().take(whole).any(|byte| *byte != 0) {
        return false;
    }
    let remainder = bits % 8;
    remainder == 0 || bytes.get(whole).is_some_and(|byte| byte & (0xff << (8 - remainder)) == 0)
}

fn encode_fingerprint(nonce: &str) -> Result<String> {
    let fingerprint = json!({
        "b6":8,"b7":8,"b9":["en-US","en"],"c2":"","c4":"Win32","c5":[],"c9":USER_AGENT,
        "f4":1080,"f5":1920,"f6":24,"f7":1080,"f8":true,"f9":[1920,1080],"g1":1920,
        "g2":"Europe/Moscow","g3":-180,"j5":true,"m2":{"mTP":0,"tE":false,"tS":false},
        "n6":false,"o2":0,"o3":"srgb","o4":0,"o5":"en-US","o8":null,"o9":null,
        "p1":null,"p2":0,"p3":null,"p4":null,"p5":null,"p6":null,"p8":[],"p9":"111111111",
        "j6":48000,"a1":"","a2":{"w":false,"d":""},
        "a3":{"acos":1.4444399284962483,"asin":0.12349655394506357,"atan":0.4636476090008061,
              "cos":-0.8390715290095377,"exp":2.718281828459045,"log1p":2.3978952727983707,
              "sin":-0.9917788534431158,"tan":-0.23206847684369653},
        "a4":{"minDelta":0.1,"maxDelta":1.2},"a5":null,"k4":[],
        "j1":{"vn":"WebKit","vr":"WebKit WebGL","vU":"","r":"Mozilla","rU":"","sLV":"WebGL GLSL ES 1.0 (1.0)"},
        "j2":{"cA":[],"p":[],"sP":[],"e":[],"eP":[]},"m10":nonce,"version":"1.5.0"
    });
    let raw = serde_json::to_vec(&fingerprint)
        .map_err(|e| provider_error("ya-docs captcha fingerprint JSON", e))?;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&raw)
        .map_err(|e| provider_error("ya-docs captcha fingerprint gzip", e))?;
    let compressed = encoder
        .finish()
        .map_err(|e| provider_error("ya-docs captcha fingerprint gzip", e))?;
    Ok(format!("~{}~", STANDARD.encode(compressed)))
}

fn captcha_form_action(html: &str, base: &reqwest::Url) -> Result<reqwest::Url> {
    let marker = "id=\"tmgrdfrend-form\"";
    let id = html
        .find(marker)
        .ok_or_else(|| Error::Provider("ya-docs captcha form missing".into()))?;
    let start = html[..id]
        .rfind("<form")
        .ok_or_else(|| Error::Provider("ya-docs captcha form start missing".into()))?;
    let end = html[id..]
        .find('>')
        .map(|offset| id + offset)
        .ok_or_else(|| Error::Provider("ya-docs captcha form end missing".into()))?;
    let tag = &html[start..=end];
    let action = html_attr(tag, "action")
        .ok_or_else(|| Error::Provider("ya-docs captcha action missing".into()))?
        .replace("&amp;", "&");
    base.join(&action)
        .or_else(|_| reqwest::Url::parse(&action))
        .map_err(|e| provider_error("ya-docs captcha action URL", e))
}

fn html_attr<'a>(tag: &'a str, name: &str) -> Option<&'a str> {
    let marker = format!("{name}=\"");
    let start = tag.find(&marker)? + marker.len();
    let rest = &tag[start..];
    let end = rest.find('"')?;
    Some(&rest[..end])
}

fn extract_between<'a>(text: &'a str, start_marker: &str, end_marker: &str) -> Option<&'a str> {
    let start = text.find(start_marker)? + start_marker.len();
    let rest = &text[start..];
    let end = rest.find(end_marker)?;
    Some(&rest[..end])
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    if text.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(text.len() / 2);
    let bytes = text.as_bytes();
    for index in (0..bytes.len()).step_by(2) {
        let high = hex_value(bytes[index])?;
        let low = hex_value(bytes[index + 1])?;
        out.push((high << 4) | low);
    }
    Some(out)
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
