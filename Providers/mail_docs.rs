use super::native_channel::{pair, provider_error, remember_url, ChannelEndpoint};
use crate::{
    providers::{Provider, ProviderBootstrap},
    transport::DatagramPath,
    Error, Result,
};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use rand::{rngs::OsRng, RngCore};
use reqwest::{cookie::{CookieStore, Jar}, Client};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;
use yawc::{Frame, HttpRequestBuilder, OpCode, Options, TcpWebSocket, WebSocket};
use tracing::{debug, warn};

const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:155.0) Gecko/20100101 Firefox/155.0";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
const KEEPALIVE: Duration = Duration::from_secs(10);

pub struct MailDocsProvider;

#[derive(Clone)]
struct DocInfo {
    token: String,
    doc_key: String,
    ws_url: String,
    file_type: String,
    doc_url: String,
    title: String,
    permissions: Value,
    callback_url: String,
    editor_user_id: String,
    cookie: String,
}

pub fn configured_urls(data: &Value) -> Result<Vec<String>> {
    if let Some(values) = data.get("urls") {
        let values = values
            .as_array()
            .ok_or_else(|| Error::Provider("mail-docs.urls must be an array".into()))?;
        let mut urls = Vec::with_capacity(values.len());
        for value in values {
            let url = value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| Error::Provider("mail-docs.urls contains an invalid URL".into()))?;
            urls.push(url.to_owned());
        }
        if urls.is_empty() {
            return Err(Error::Provider("mail-docs.urls cannot be empty".into()));
        }
        return Ok(urls);
    }
    let url = data
        .get("url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::Provider("mail-docs requires url or urls".into()))?;
    Ok(vec![url.to_owned()])
}

pub fn worker_data(data: &Value, worker_id: usize) -> Result<Value> {
    let urls = configured_urls(data)?;
    let mut selected = data.clone();
    let object = selected
        .as_object_mut()
        .ok_or_else(|| Error::Provider("mail-docs data must be an object".into()))?;
    object.remove("urls");
    object.insert("url".into(), Value::String(urls[worker_id % urls.len()].clone()));
    Ok(selected)
}

#[async_trait]
impl Provider for MailDocsProvider {
    async fn bootstrap(&self, _data: &Value) -> Result<ProviderBootstrap> {
        Ok(ProviderBootstrap::default())
    }

    async fn connect_native(&self, data: &Value) -> Result<Option<Arc<dyn DatagramPath>>> {
        let raw = configured_urls(data)?
            .into_iter()
            .next()
            .expect("validated mail-docs urls");
        let weblink = normalize_weblink(&raw);
        if weblink.is_empty() {
            return Err(Error::Provider("mail-docs public link is empty".into()));
        }
        let (path, endpoint) = pair("mail-docs");
        let (ready_tx, ready_rx) = oneshot::channel();
        tokio::spawn(async move {
            if let Err(error) = run(weblink, endpoint, ready_tx).await {
                warn!(%error, "mail-docs carrier stopped");
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
                Err(Error::Provider("mail-docs startup task ended".into()))
            }
            Err(_) => {
                let _ = path.close().await;
                Err(Error::Provider("mail-docs connection timed out".into()))
            }
        }
    }
}

async fn run(
    weblink: String,
    mut endpoint: ChannelEndpoint,
    ready: oneshot::Sender<std::result::Result<(), String>>,
) -> Result<()> {
    remember_url(&endpoint.route_ips, "https://cloud.mail.ru/").await;
    let jar = Arc::new(Jar::default());
    let client = Client::builder()
        .cookie_provider(jar.clone())
        .timeout(Duration::from_secs(20))
        .build()
        .map_err(|e| provider_error("mail-docs HTTP client", e))?;
    let mut ready = Some(ready);
    let mut pending = None;
    let mut attempt = 0u32;
    loop {
        if *endpoint.shutdown.borrow() {
            return Ok(());
        }
        match connect_and_serve(
            &client,
            jar.clone(),
            &weblink,
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
                warn!(attempt, %error, "mail-docs disconnected; reconnecting");
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
    weblink: &str,
    endpoint: &mut ChannelEndpoint,
    pending: &mut Option<Vec<u8>>,
    ready: &mut Option<oneshot::Sender<std::result::Result<(), String>>>,
) -> Result<()> {
    let info = fetch_doc_info(client, jar, weblink).await?;
    remember_url(&endpoint.route_ips, &info.ws_url).await;
    let mut request = HttpRequestBuilder::new()
        .header("User-Agent", USER_AGENT)
        .header("Accept-Language", "en-US,en;q=0.9")
        .header("Origin", "https://docs.datacloudmail.ru");
    if !info.cookie.is_empty() {
        request = request.header("Cookie", info.cookie.as_str());
    }
    let ws_url = url::Url::parse(&info.ws_url)
        .map_err(|e| provider_error("mail-docs WebSocket URL", e))?;
    let mut socket = WebSocket::connect(ws_url)
        .with_options(Options::default().with_balanced_compression())
        .with_request(request)
        .await
        .map_err(|e| provider_error("mail-docs WebSocket connect", e))?;

    wait_for_prefix(&mut socket, "0{").await?;
    socket
        .send(Frame::text(format!("40{{\"token\":\"{}\"}}", info.token)))
        .await
        .map_err(|e| provider_error("mail-docs Socket.IO connect", e))?;
    wait_for_prefix(&mut socket, "40").await?;

    let user_id = if info.editor_user_id.is_empty() {
        random_user_id()
    } else {
        info.editor_user_id.clone()
    };
    let auth = json!([
        "message",
        {
            "type":"auth",
            "docid":info.doc_key,
            "documentCallbackUrl":info.callback_url,
            "token":"fghhfgsjdgfjs",
            "user":{"id":user_id,"username":random_user_id(),"indexUser":-1},
            "editorType":0,
            "lastOtherSaveTime":-1,
            "block":[],
            "documentFormatSave":65,
            "view":false,
            "isCloseCoAuthoring":false,
            "openCmd":{
                "c":"open",
                "id":info.doc_key,
                "userid":user_id,
                "format":info.file_type,
                "url":info.doc_url,
                "title":info.title,
                "lcid":25,
                "nobase64":true,
                "convertToOrigin":".pdf.xps.oxps.djvu"
            },
            "lang":"ru",
            "mode":"edit",
            "permissions":info.permissions,
            "IsAnonymousUser":false,
            "timezoneOffset":-180,
            "coEditingMode":"fast",
            "jwtOpen":info.token,
            "time":1000,
            "supportAuthChangesAck":true
        }
    ]);
    socket
        .send(Frame::text(format!("42{}", auth)))
        .await
        .map_err(|e| provider_error("mail-docs auth", e))?;

    let (write, mut read) = socket.split();
    let (socket_tx, mut socket_rx) = tokio::sync::mpsc::channel::<Frame>(4096);
    let mut writer = tokio::spawn(async move {
        let mut write = write;
        while let Some(message) = socket_rx.recv().await {
            write.send(message).await?;
        }
        Ok::<(), yawc::WebSocketError>(())
    });
    let mut keepalive = tokio::time::interval(KEEPALIVE);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    if let Some(data) = pending.take() {
        if socket_tx.send(cursor_message(&data)).await.is_err() {
            *pending = Some(data);
            return Err(Error::Transport("mail-docs writer stopped".into()));
        }
    }
    if let Some(ready) = ready.take() {
        let _ = ready.send(Ok(()));
    }
    debug!(weblink, "mail-docs carrier connected");

    loop {
        tokio::select! {
            outgoing = endpoint.outbound.recv() => {
                let Some(data) = outgoing else { return Ok(()); };
                if socket_tx.send(cursor_message(&data)).await.is_err() {
                    *pending = Some(data);
                    return Err(Error::Transport("mail-docs writer stopped".into()));
                }
            }
            incoming = read.next() => {
                match incoming {
                    Some(frame) if frame.opcode() == OpCode::Text => {
                        let text = String::from_utf8_lossy(frame.payload());
                        if text.as_ref() == "2" {
                            let _ = socket_tx.send(Frame::text("3")).await;
                            continue;
                        }
                        for payload in cursor_payloads(text.as_ref()) {
                            if let Ok(bytes) = STANDARD.decode(payload) {
                                let _ = endpoint.inbound.try_send(bytes);
                            }
                        }
                    }
                    Some(frame) if frame.opcode() == OpCode::Close => return Err(Error::Transport("mail-docs WebSocket closed".into())),
                    Some(_) => {}
                    None => return Err(Error::Transport("mail-docs WebSocket closed".into())),
                }
            }
            _ = keepalive.tick() => {
                let _ = socket_tx.send(Frame::text("42[\"message\",{\"type\":\"cursor\",\"cursor\":\"18;---KA---\"}]")).await;
            }
            result = &mut writer => {
                match result {
                    Ok(Ok(())) => return Err(Error::Transport("mail-docs writer ended".into())),
                    Ok(Err(error)) => return Err(provider_error("mail-docs WebSocket write", error)),
                    Err(error) => return Err(provider_error("mail-docs writer task", error)),
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

async fn wait_for_prefix(socket: &mut TcpWebSocket, prefix: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(frame) = socket.next().await {
            if frame.opcode() != OpCode::Text {
                if frame.opcode() == OpCode::Close {
                    break;
                }
                continue;
            }
            let text = String::from_utf8_lossy(frame.payload());
            if text.as_ref() == "2" {
                socket
                    .send(Frame::text("3"))
                    .await
                    .map_err(|e| provider_error("mail-docs handshake pong", e))?;
                continue;
            }
            if text.starts_with(prefix) {
                return Ok(());
            }
        }
        Err(Error::Provider("mail-docs handshake closed".into()))
    })
    .await
    .map_err(|_| Error::Provider("mail-docs handshake timed out".into()))?
}

fn cursor_message(data: &[u8]) -> Frame {
    let payload = STANDARD.encode(data);
    Frame::text(format!(
        "42[\"message\",{{\"type\":\"cursor\",\"cursor\":\"{};{}\"}}]",
        data.len(), payload
    ))
}

fn cursor_payloads(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let marker = "\"cursor\":\"";
    let mut rest = text;
    while let Some(start) = rest.find(marker) {
        let value = &rest[start + marker.len()..];
        let Some(end) = value.find('"') else { break; };
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

async fn fetch_doc_info(client: &Client, jar: Arc<Jar>, weblink: &str) -> Result<DocInfo> {
    let response = client
        .post("https://cloud.mail.ru/api/v4/r7/edit")
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/plain, */*")
        .header("Accept-Language", "en-US,en;q=0.9")
        .header("User-Agent", USER_AGENT)
        .header("X-Api-Version", "4")
        .header(
            "Referer",
            format!("https://cloud.mail.ru/public/{weblink}?weblink={weblink}"),
        )
        .json(&json!({"x-email":"anonym","public":format!("/{weblink}"),"platform":"desktop_web"}))
        .send()
        .await
        .map_err(|e| provider_error("mail-docs metadata request", e))?;
    if !response.status().is_success() {
        return Err(Error::Provider(format!(
            "mail-docs metadata returned HTTP {}",
            response.status()
        )));
    }
    let value: Value = response
        .json()
        .await
        .map_err(|e| provider_error("mail-docs metadata JSON", e))?;
    let api = value
        .get("api")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::Provider("mail-docs api missing".into()))?;
    let token = value
        .get("token")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::Provider("mail-docs token missing".into()))?;
    let document = value
        .get("document")
        .and_then(Value::as_object)
        .ok_or_else(|| Error::Provider("mail-docs document missing".into()))?;
    let editor = value
        .get("editorConfig")
        .and_then(Value::as_object)
        .ok_or_else(|| Error::Provider("mail-docs editorConfig missing".into()))?;
    let doc_key = document
        .get("key")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::Provider("mail-docs document key missing".into()))?;
    let ws_base = if let Some(rest) = api.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = api.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        api.to_owned()
    };
    let ws_url = format!("{}/doc/{}/c/?EIO=4&transport=websocket", ws_base.trim_end_matches('/'), doc_key);
    let cookie_url = reqwest::Url::parse(api).map_err(|e| provider_error("mail-docs api URL", e))?;
    let cookie = jar
        .cookies(&cookie_url)
        .and_then(|value| value.to_str().ok().map(str::to_owned))
        .unwrap_or_default();
    Ok(DocInfo {
        token: token.to_owned(),
        doc_key: doc_key.to_owned(),
        ws_url,
        file_type: document.get("fileType").and_then(Value::as_str).unwrap_or("docx").to_owned(),
        doc_url: document.get("url").and_then(Value::as_str).unwrap_or("").to_owned(),
        title: document.get("title").and_then(Value::as_str).unwrap_or("ToTS").to_owned(),
        permissions: document.get("permissions").cloned().unwrap_or_else(|| json!({})),
        callback_url: editor.get("callbackUrl").and_then(Value::as_str).unwrap_or("").to_owned(),
        editor_user_id: editor.get("user").and_then(Value::as_object).and_then(|user| user.get("id")).and_then(Value::as_str).unwrap_or("").to_owned(),
        cookie,
    })
}

fn normalize_weblink(raw: &str) -> String {
    let value = raw.trim().trim_matches('/');
    for prefix in [
        "https://cloud.mail.ru/public/",
        "http://cloud.mail.ru/public/",
        "https://cloud.mail.ru/",
        "http://cloud.mail.ru/",
    ] {
        if let Some(rest) = value.strip_prefix(prefix) {
            return rest.trim_matches('/').to_owned();
        }
    }
    value.to_owned()
}

fn random_user_id() -> String {
    let mut bytes = [0u8; 8];
    OsRng.fill_bytes(&mut bytes);
    let value = u64::from_le_bytes(bytes) % 1_000_000_000;
    format!("{value:010}")
}
