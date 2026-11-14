use super::native_channel::{pair, provider_error, remember_url, ChannelEndpoint};
use crate::{
    providers::{Provider, ProviderBootstrap},
    transport::DatagramPath,
    Error, Result,
};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use reqwest::{cookie::{CookieStore, Jar}, Client};
use serde_json::{json, Value};
use std::{sync::Arc, time::{Duration, Instant}};
use tokio::sync::oneshot;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue, Message},
};
use tracing::{debug, warn};

pub const MAX_WORKER_ROOMS: usize = 50;
const BASE_ROOM_URL: &str = "https://interview.cups.online/live-coding/";
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/137 Safari/537.36";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(25);
const KEEPALIVE: Duration = Duration::from_secs(20);
const SEND_INTERVAL: Duration = Duration::from_millis(12);
const BYTES_PER_NUMBER: usize = 6;

pub struct CupsProvider;

#[derive(Clone)]
struct Auth {
    room_id: String,
    user_id: String,
    conn_token: String,
    conn_url: String,
    sub_token: String,
    channel: String,
    cookie: String,
}

pub struct CupsRoom {
    pub room_id: String,
    pub path: Arc<dyn DatagramPath>,
}

pub fn worker_room_count(data: &Value) -> Result<usize> {
    if let Some(rooms) = data.get("worker_rooms") {
        let rooms = rooms
            .as_array()
            .ok_or_else(|| Error::Provider("cups.worker_rooms must be an array".into()))?;
        if rooms.is_empty() || rooms.len() > MAX_WORKER_ROOMS {
            return Err(Error::Provider("cups.worker_rooms must contain 1..=50 rooms".into()));
        }
        if let Some(count) = data.get("worker_count").and_then(Value::as_u64) {
            if count as usize != rooms.len() {
                return Err(Error::Provider("cups.worker_count does not match worker_rooms".into()));
            }
        }
        return Ok(rooms.len());
    }
    if data.get("room_id").and_then(Value::as_str).is_some() {
        return Ok(1);
    }
    match data.get("worker_count") {
        Some(value) => value
            .as_u64()
            .filter(|count| (1..=MAX_WORKER_ROOMS as u64).contains(count))
            .map(|count| count as usize)
            .ok_or_else(|| Error::Provider("cups.worker_count must be 1..=50".into())),
        None => Ok(1),
    }
}

pub fn worker_room_data(data: &Value, worker_id: usize) -> Result<Value> {
    if let Some(rooms) = data.get("worker_rooms") {
        let rooms = rooms
            .as_array()
            .ok_or_else(|| Error::Provider("cups.worker_rooms must be an array".into()))?;
        let room = rooms.get(worker_id).ok_or_else(|| {
            Error::Provider("cups worker has no room; provision a larger worker_count".into())
        })?;
        let room_id = room
            .get("room_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::Provider("cups worker room has no room_id".into()))?;
        return Ok(json!({"room_id":room_id}));
    }
    if worker_id == 0 {
        if data.get("room_id").and_then(Value::as_str).is_some() {
            return Ok(data.clone());
        }
    }
    Err(Error::Provider(
        "cups worker has no provisioned room; create the config with cups.worker_count >= --n".into(),
    ))
}

#[async_trait]
impl Provider for CupsProvider {
    async fn bootstrap(&self, _data: &Value) -> Result<ProviderBootstrap> {
        Ok(ProviderBootstrap::default())
    }

    async fn connect_native(&self, data: &Value) -> Result<Option<Arc<dyn DatagramPath>>> {
        let room_id = data
            .get("room_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::Provider("cups requires a server-issued room_id".into()))?;
        Ok(Some(join_room(room_id).await?.path))
    }
}

pub async fn create_room() -> Result<CupsRoom> {
    spawn_room(None).await
}

pub async fn join_room(room_id: &str) -> Result<CupsRoom> {
    spawn_room(Some(room_id.to_owned())).await
}

async fn spawn_room(requested_room: Option<String>) -> Result<CupsRoom> {
    let (path, endpoint) = pair("cups");
    let (ready_tx, ready_rx) = oneshot::channel();
    tokio::spawn(async move {
        if let Err(error) = run(requested_room, endpoint, ready_tx).await {
            warn!(%error, "cups carrier stopped");
        }
    });
    match tokio::time::timeout(CONNECT_TIMEOUT, ready_rx).await {
        Ok(Ok(Ok(room_id))) => Ok(CupsRoom { room_id, path }),
        Ok(Ok(Err(message))) => {
            let _ = path.close().await;
            Err(Error::Provider(message))
        }
        Ok(Err(_)) => {
            let _ = path.close().await;
            Err(Error::Provider("cups startup task ended".into()))
        }
        Err(_) => {
            let _ = path.close().await;
            Err(Error::Provider("cups connection timed out".into()))
        }
    }
}

async fn run(
    mut requested_room: Option<String>,
    mut endpoint: ChannelEndpoint,
    ready: oneshot::Sender<std::result::Result<String, String>>,
) -> Result<()> {
    remember_url(&endpoint.route_ips, BASE_ROOM_URL).await;
    let jar = Arc::new(Jar::default());
    let client = Client::builder()
        .cookie_provider(jar.clone())
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| provider_error("cups HTTP client", e))?;
    let mut ready = Some(ready);
    let mut pending = None;
    let mut attempt = 0u32;
    loop {
        if *endpoint.shutdown.borrow() {
            return Ok(());
        }
        let room_url = requested_room
            .as_ref()
            .map(|room| format!("{BASE_ROOM_URL}?room={room}"))
            .unwrap_or_else(|| BASE_ROOM_URL.to_owned());
        let auth = match authorize(&client, jar.clone(), &room_url).await {
            Ok(auth) => auth,
            Err(error) => {
                if let Some(ready) = ready.take() {
                    let _ = ready.send(Err(error.to_string()));
                    return Err(error);
                }
                attempt = attempt.saturating_add(1);
                reconnect_wait(&mut endpoint, attempt).await?;
                continue;
            }
        };
        if requested_room.is_none() {
            requested_room = Some(auth.room_id.clone());
        }
        if let Some(expected) = requested_room.as_ref() {
            if &auth.room_id != expected {
                let error = Error::Provider(format!(
                    "cups room {expected} no longer exists (service returned {})",
                    auth.room_id
                ));
                if let Some(ready) = ready.take() {
                    let _ = ready.send(Err(error.to_string()));
                }
                return Err(error);
            }
        }
        remember_url(&endpoint.route_ips, &auth.conn_url).await;
        match connect_and_serve(auth.clone(), &mut endpoint, &mut pending, &mut ready).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                if let Some(ready) = ready.take() {
                    let _ = ready.send(Err(error.to_string()));
                    return Err(error);
                }
                attempt = attempt.saturating_add(1);
                warn!(attempt, %error, room=%auth.room_id, "cups disconnected; reconnecting");
                reconnect_wait(&mut endpoint, attempt).await?;
            }
        }
    }
}

async fn reconnect_wait(endpoint: &mut ChannelEndpoint, attempt: u32) -> Result<()> {
    let delay = Duration::from_millis((250u64 << attempt.min(5)).min(10_000));
    tokio::select! {
        _ = tokio::time::sleep(delay) => Ok(()),
        changed = endpoint.shutdown.changed() => {
            if changed.is_err() || *endpoint.shutdown.borrow() { Ok(()) } else { Ok(()) }
        }
    }
}

async fn authorize(client: &Client, jar: Arc<Jar>, room_url: &str) -> Result<Auth> {
    let response = client
        .get(room_url)
        .header("User-Agent", USER_AGENT)
        .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        .header("Accept-Language", "ru-RU,ru;q=0.9")
        .send()
        .await
        .map_err(|e| provider_error("cups room request", e))?;
    if !response.status().is_success() {
        return Err(Error::Provider(format!(
            "cups room returned HTTP {}",
            response.status()
        )));
    }
    let final_url = response.url().clone();
    let html = response
        .text()
        .await
        .map_err(|e| provider_error("cups room body", e))?;
    let room_id = extract_html_uuid(&html, "data-room")
        .ok_or_else(|| Error::Provider("cups room uuid missing".into()))?;
    let user_id = extract_html_uuid(&html, "data-user")
        .ok_or_else(|| Error::Provider("cups user uuid missing".into()))?;
    let conn_token = meta_content(&html, "centrifuge-connection-token")
        .ok_or_else(|| Error::Provider("cups connection token missing".into()))?;
    let conn_url = meta_content(&html, "centrifuge-connection-url")
        .ok_or_else(|| Error::Provider("cups connection URL missing".into()))?;
    let sub_url = meta_content(&html, "centrifuge-subscription-token-url")
        .ok_or_else(|| Error::Provider("cups subscription URL missing".into()))?;
    let cookie = jar
        .cookies(&final_url)
        .and_then(|value| value.to_str().ok().map(str::to_owned))
        .unwrap_or_default();
    let csrf = cookie
        .split(';')
        .find_map(|item| item.trim().strip_prefix("csrftoken=").map(str::to_owned))
        .ok_or_else(|| Error::Provider("cups csrftoken missing".into()))?;
    let channel = format!("$shared_editor:room-{room_id}");
    let origin = origin_of(&sub_url)?;
    let response = client
        .post(&sub_url)
        .header("User-Agent", USER_AGENT)
        .header("Content-Type", "application/json")
        .header("X-CSRFToken", csrf)
        .header("Origin", origin)
        .header("Referer", room_url)
        .json(&json!({"channel":channel}))
        .send()
        .await
        .map_err(|e| provider_error("cups subscription token request", e))?;
    if !response.status().is_success() {
        return Err(Error::Provider(format!(
            "cups subscription token returned HTTP {}",
            response.status()
        )));
    }
    let value: Value = response
        .json()
        .await
        .map_err(|e| provider_error("cups subscription token JSON", e))?;
    let sub_token = value
        .get("token")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::Provider("cups subscription token missing".into()))?;
    Ok(Auth {
        room_id,
        user_id,
        conn_token,
        conn_url,
        sub_token: sub_token.to_owned(),
        channel,
        cookie,
    })
}

async fn connect_and_serve(
    auth: Auth,
    endpoint: &mut ChannelEndpoint,
    pending: &mut Option<Vec<u8>>,
    ready: &mut Option<oneshot::Sender<std::result::Result<String, String>>>,
) -> Result<()> {
    let ws_base = if let Some(rest) = auth.conn_url.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = auth.conn_url.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        auth.conn_url.clone()
    };
    let ws_url = format!("{}/websocket", ws_base.trim_end_matches('/'));
    let mut request = ws_url
        .into_client_request()
        .map_err(|e| provider_error("cups WebSocket request", e))?;
    request.headers_mut().insert("User-Agent", HeaderValue::from_static(USER_AGENT));
    request.headers_mut().insert(
        "Origin",
        HeaderValue::from_str(&origin_of(&auth.conn_url)?)
            .map_err(|e| provider_error("cups Origin header", e))?,
    );
    if !auth.cookie.is_empty() {
        request.headers_mut().insert(
            "Cookie",
            HeaderValue::from_str(&auth.cookie)
                .map_err(|e| provider_error("cups Cookie header", e))?,
        );
    }
    let (mut socket, _) = connect_async(request)
        .await
        .map_err(|e| provider_error("cups WebSocket connect", e))?;
    socket
        .send(Message::Text(
            json!({"id":1,"connect":{"token":auth.conn_token,"name":"js"}})
                .to_string()
                .into(),
        ))
        .await
        .map_err(|e| provider_error("cups connect command", e))?;
    wait_reply(&mut socket, 1).await?;
    socket
        .send(Message::Text(
            json!({"id":2,"subscribe":{"channel":auth.channel,"token":auth.sub_token}})
                .to_string()
                .into(),
        ))
        .await
        .map_err(|e| provider_error("cups subscribe command", e))?;
    wait_reply(&mut socket, 2).await?;

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
    let mut rpc_id = 2u64;
    let mut last_send = Instant::now() - SEND_INTERVAL;

    if let Some(data) = pending.take() {
        rpc_id += 1;
        pace(&mut last_send).await;
        if socket_tx.send(cups_data_message(&auth, rpc_id, &data)).await.is_err() {
            *pending = Some(data);
            return Err(Error::Transport("cups writer stopped".into()));
        }
    }
    if let Some(ready) = ready.take() {
        let _ = ready.send(Ok(auth.room_id.clone()));
    }
    debug!(room=%auth.room_id, "cups carrier connected");

    loop {
        tokio::select! {
            outgoing = endpoint.outbound.recv() => {
                let Some(data) = outgoing else { return Ok(()); };
                rpc_id = rpc_id.saturating_add(1);
                pace(&mut last_send).await;
                if socket_tx.send(cups_data_message(&auth, rpc_id, &data)).await.is_err() {
                    *pending = Some(data);
                    return Err(Error::Transport("cups writer stopped".into()));
                }
            }
            incoming = read.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => handle_cups_frame(text.as_bytes(), &auth, endpoint, &socket_tx).await,
                    Some(Ok(Message::Binary(data))) => handle_cups_frame(&data, &auth, endpoint, &socket_tx).await,
                    Some(Ok(Message::Close(_))) | None => return Err(Error::Transport("cups WebSocket closed".into())),
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(provider_error("cups WebSocket read", error)),
                }
            }
            _ = keepalive.tick() => {
                rpc_id = rpc_id.saturating_add(1);
                let message = json!({"rpc":{"method":"shared_editor_ping","data":{"room":auth.room_id,"user":auth.user_id}},"id":rpc_id});
                let _ = socket_tx.send(Message::Text(message.to_string().into())).await;
            }
            result = &mut writer => {
                match result {
                    Ok(Ok(())) => return Err(Error::Transport("cups writer ended".into())),
                    Ok(Err(error)) => return Err(provider_error("cups WebSocket write", error)),
                    Err(error) => return Err(provider_error("cups writer task", error)),
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

async fn pace(last_send: &mut Instant) {
    let elapsed = last_send.elapsed();
    if elapsed < SEND_INTERVAL {
        tokio::time::sleep(SEND_INTERVAL - elapsed).await;
    }
    *last_send = Instant::now();
}

fn cups_data_message(auth: &Auth, id: u64, data: &[u8]) -> Message {
    let mut payload = Vec::with_capacity(data.len() + 2);
    payload.extend_from_slice(&(data.len() as u16).to_be_bytes());
    payload.extend_from_slice(data);
    let numbers = pack_numbers(&payload);
    let mut cursors = Vec::with_capacity((numbers.len() + 1) / 2);
    for chunk in numbers.chunks(2) {
        cursors.push(json!({
            "row":chunk[0],
            "column":chunk.get(1).copied().unwrap_or(0)
        }));
    }
    Message::Text(
        json!({
            "rpc":{
                "method":"shared_editor_change_cursors",
                "data":{
                    "cursors":cursors,
                    "ranges":[],
                    "room":auth.room_id,
                    "user":auth.user_id
                }
            },
            "id":id
        })
        .to_string()
        .into(),
    )
}

async fn wait_reply<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>, id: u64) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(message) = socket.next().await {
            let message = message.map_err(|e| provider_error("cups handshake read", e))?;
            let bytes = match message {
                Message::Text(text) => text.as_bytes().to_vec(),
                Message::Binary(data) => data.to_vec(),
                _ => continue,
            };
            for line in bytes.split(|byte| *byte == b'\n') {
                let line = line.strip_suffix(b"\r").unwrap_or(line);
                if line == b"{}" {
                    socket
                        .send(Message::Text("{}".into()))
                        .await
                        .map_err(|e| provider_error("cups handshake pong", e))?;
                    continue;
                }
                let Ok(value) = serde_json::from_slice::<Value>(line) else { continue; };
                if value.get("id").and_then(Value::as_u64) != Some(id) {
                    continue;
                }
                if let Some(error) = value.get("error") {
                    return Err(Error::Provider(format!("cups handshake rejected: {error}")));
                }
                return Ok(());
            }
        }
        Err(Error::Provider("cups handshake closed".into()))
    })
    .await
    .map_err(|_| Error::Provider("cups handshake timed out".into()))?
}

async fn handle_cups_frame(
    bytes: &[u8],
    auth: &Auth,
    endpoint: &ChannelEndpoint,
    socket_tx: &tokio::sync::mpsc::Sender<Message>,
) {
    for line in bytes.split(|byte| *byte == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line == b"{}" {
            let _ = socket_tx.send(Message::Text("{}".into())).await;
            continue;
        }
        let Ok(value) = serde_json::from_slice::<Value>(line) else { continue; };
        let Some(data) = value
            .get("push")
            .and_then(|value| value.get("pub"))
            .and_then(|value| value.get("data"))
        else { continue; };
        if data.get("type").and_then(Value::as_str) != Some("cursors_update") {
            continue;
        }
        let Some(payload) = data.get("payload") else { continue; };
        if payload.get("user_uuid").and_then(Value::as_str) == Some(auth.user_id.as_str()) {
            continue;
        }
        let Some(cursors) = payload.get("cursors").and_then(Value::as_array) else { continue; };
        let mut numbers = Vec::with_capacity(cursors.len() * 2);
        let mut valid = true;
        for cursor in cursors {
            let Some(row) = cursor.get("row").and_then(Value::as_u64) else { valid = false; break; };
            let column = cursor.get("column").and_then(Value::as_u64).unwrap_or(0);
            numbers.push(row);
            numbers.push(column);
        }
        if !valid {
            continue;
        }
        let decoded = unpack_numbers(&numbers);
        if decoded.len() < 2 {
            continue;
        }
        let len = u16::from_be_bytes([decoded[0], decoded[1]]) as usize;
        if len == 0 || len + 2 > decoded.len() {
            continue;
        }
        let _ = endpoint.inbound.try_send(decoded[2..2 + len].to_vec());
    }
}

fn pack_numbers(bytes: &[u8]) -> Vec<u64> {
    bytes
        .chunks(BYTES_PER_NUMBER)
        .map(|chunk| {
            let mut value = 0u64;
            for index in 0..BYTES_PER_NUMBER {
                value <<= 8;
                if let Some(byte) = chunk.get(index) {
                    value |= *byte as u64;
                }
            }
            value
        })
        .collect()
}

fn unpack_numbers(numbers: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(numbers.len() * BYTES_PER_NUMBER);
    for value in numbers {
        let mut value = *value;
        let mut chunk = [0u8; BYTES_PER_NUMBER];
        for index in (0..BYTES_PER_NUMBER).rev() {
            chunk[index] = value as u8;
            value >>= 8;
        }
        out.extend_from_slice(&chunk);
    }
    out
}

fn meta_content(html: &str, name: &str) -> Option<String> {
    let needle = format!("name=\"{name}\"");
    let pos = html.find(&needle)?;
    let tag_start = html[..pos].rfind("<meta")?;
    let tag_end = html[pos..].find('>')? + pos;
    let tag = &html[tag_start..=tag_end];
    attr_value(tag, "content").map(str::to_owned)
}

fn extract_html_uuid(html: &str, attr: &str) -> Option<String> {
    let needle = format!("{attr}=\"");
    let pos = html.find(&needle)?;
    let value = &html[pos + needle.len()..];
    let uuid = value.find("uuid&quot;")? + "uuid&quot;".len();
    let after_uuid = &value[uuid..];
    let colon = after_uuid.find(':')? + 1;
    let after_colon = after_uuid[colon..].trim_start();
    let quote = after_colon.find("&quot;")? + "&quot;".len();
    let rest = &after_colon[quote..];
    let end = rest.find("&quot;")?;
    Some(rest[..end].to_owned())
}

fn attr_value<'a>(tag: &'a str, attr: &str) -> Option<&'a str> {
    let needle = format!("{attr}=\"");
    let start = tag.find(&needle)? + needle.len();
    let rest = &tag[start..];
    let end = rest.find('"')?;
    Some(&rest[..end])
}

fn origin_of(raw: &str) -> Result<String> {
    let url = url::Url::parse(raw).map_err(|e| provider_error("invalid cups URL", e))?;
    let host = url
        .host_str()
        .ok_or_else(|| Error::Provider("cups URL has no host".into()))?;
    let port = url.port().map(|port| format!(":{port}")).unwrap_or_default();
    Ok(format!("{}://{}{}", url.scheme(), host, port))
}
