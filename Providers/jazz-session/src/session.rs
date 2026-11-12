use crate::{
    carrier::{MediaCarrier, VIDEO_HEIGHT, VIDEO_WIDTH},
    rtc::{DataPlane, Media},
    snapshot::sdp_parameters,
    Error, Result,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::{HashSet, VecDeque},
    fmt,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, RwLock},
    task::JoinHandle,
    time::{self, Instant},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{client::IntoClientRequest, protocol::WebSocketConfig, Message},
    MaybeTlsStream, WebSocketStream,
};
use url::Url;

pub(crate) struct CallInfo {
    pub name: String,
    pub room_id: String,
    pub password: String,
    pub join_url: String,
    pub connector_url: Url,
    pub carrier: MediaCarrier,
}

type MediaSlot = Arc<RwLock<Option<Arc<Media>>>>;

/// Живой звонок. `json()` явно раскрывает секреты подключения; Debug их скрывает.
#[must_use = "Сохраните сессию до завершения работы, затем вызовите close().await"]
pub struct CallSession {
    state: Arc<RwLock<Value>>,
    media: MediaSlot,
    join_url: String,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<()>>>,
}

/// Bidirectional ToTS carrier over the SaluteJazz media plane. Encrypted ToTS
/// frames are fragmented into RTP payloads on the published audio track and
/// reassembled from the subscribed remote track.
#[derive(Clone)]
pub struct JazzDataPlane {
    inner: DataPlane,
}

impl JazzDataPlane {
    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        self.inner.wait_ready(timeout).await.map_err(Into::into)
    }

    /// Wait until a remote participant media track is actually subscribed.
    /// Unlike wait_ready(), this is intentionally not used by create_call():
    /// the server must be able to issue an invite before the client joins.
    pub async fn wait_receive_ready(&self, timeout: Duration) -> Result<()> {
        self.inner
            .wait_receive_ready(timeout)
            .await
            .map_err(Into::into)
    }

    /// Experimental video queues bounded datagrams for the next 60 fps media frame.
    /// Success means accepted by the queue; a failed writer rejects later sends.
    pub async fn send(&self, payload: &[u8]) -> Result<()> {
        self.inner.send(payload).await.map_err(Into::into)
    }

    pub async fn recv(&self) -> Result<Vec<u8>> {
        self.inner.recv().await.map_err(Into::into)
    }
}

impl fmt::Debug for CallSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CallSession { credentials: [REDACTED] }")
    }
}

impl CallSession {
    /// Ссылка-приглашение этой комнаты. Это секрет: она содержит пароль комнаты.
    pub fn join_url(&self) -> &str {
        &self.join_url
    }

    /// Получить двунаправленный Jazz media data plane. Сессия должна оставаться живой, пока
    /// используется возвращённый объект.
    pub async fn data_plane(&self) -> Result<JazzDataPlane> {
        let media = self
            .media
            .read()
            .await
            .clone()
            .ok_or(Error::Protocol("WebRTC media ещё не настроена"))?;
        Ok(JazzDataPlane {
            inner: media.data_plane(),
        })
    }

    /// Текущий JSON-снимок, включая исходные SDP и TURN credentials.
    pub async fn json(&self) -> Value {
        let mut result = self.state.read().await.clone();
        let media = self.media.read().await.clone();
        if let Some(media) = media {
            let data = media.snapshot().await;
            result["webrtc"]["native_receive"] = data["native_receive"].clone();
            for key in ["publisher", "subscriber"] {
                result["webrtc"][key] = data[key].clone();
                for description in ["local_description", "remote_description"] {
                    if let Some(sdp) = result["webrtc"][key][description]["sdp"]
                        .as_str()
                        .map(str::to_owned)
                    {
                        result["webrtc"][key][description]["transport_parameters"] =
                            sdp_parameters(&sdp);
                    }
                }
            }
        }
        result
    }

    /// Отправить leave и дождаться закрытия WebSocket и обоих PeerConnection.
    pub async fn close(mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(mut task) = self.task.take() {
            match time::timeout(Duration::from_secs(3), &mut task).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(Error::Protocol("задача сессии остановлена")),
                Err(_) => {
                    task.abort();
                    if let Some(media) = self.media.read().await.clone() {
                        let _ = time::timeout(Duration::from_secs(3), media.close()).await;
                    }
                    Err(Error::Timeout("закрытие сессии"))
                }
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for CallSession {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

pub(crate) async fn establish(call: CallInfo, timeout: Duration) -> Result<CallSession> {
    let url = &call.connector_url;
    let test_local = cfg!(test)
        && url.scheme() == "ws"
        && url
            .host_str()
            .and_then(|host| host.parse::<std::net::IpAddr>().ok())
            .is_some_and(|ip| ip.is_loopback());
    if (url.scheme() != "wss" && !test_local)
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || timeout.is_zero()
    {
        return Err(Error::Protocol("недопустимый сигнальный адрес"));
    }
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|_| Error::WebSocket)?;
    let origin = Url::parse(&call.join_url)
        .map_err(|_| Error::Protocol("некорректный адрес приглашения"))?
        .origin()
        .ascii_serialization();
    request
        .headers_mut()
        .insert("Origin", origin.parse().map_err(|_| Error::WebSocket)?);
    request.headers_mut().insert(
        "User-Agent",
        "jazz-session/0.1".parse().expect("fixed user agent"),
    );
    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(2 * 1024 * 1024);
    config.max_frame_size = Some(2 * 1024 * 1024);
    let (socket, _) = time::timeout(
        timeout,
        connect_async_with_config(request, Some(config), false),
    )
    .await
    .map_err(|_| Error::Timeout("WebSocket handshake"))?
    .map_err(|_| Error::WebSocket)?;
    let join_url = call.join_url.clone();
    let state = Arc::new(RwLock::new(json!({"schema_version":1,"status":"joining",
        "call":{"name":call.name,"room_id":call.room_id,"join_url":call.join_url,"password":call.password,
            "connector_url":call.connector_url.as_str()},
        "session":null,"webrtc":{"ice_servers":[],"publisher":null,"subscriber":null}})));
    let media: MediaSlot = Arc::new(RwLock::new(None));
    let (stop_tx, stop_rx) = oneshot::channel();
    let (ready_tx, ready_rx) = oneshot::channel();
    let (outgoing, outgoing_rx) = mpsc::unbounded_channel();
    let mut worker = Worker {
        socket,
        call,
        timeout,
        state: state.clone(),
        media: media.clone(),
        outgoing,
        ready: Some(ready_tx),
        group_id: String::new(),
        join_id: uuid::Uuid::new_v4().to_string(),
        joined: false,
        track_requested: false,
        track_cid: uuid::Uuid::new_v4().to_string(),
        publisher_answer: false,
        subscriber_answer: false,
        turn_configured: false,
        rtc_joined: false,
        local_identity: String::new(),
        track_subscriptions: HashSet::new(),
        pending: VecDeque::new(),
        servers: None,
        ping_interval: Duration::ZERO,
        ping_timeout: Duration::ZERO,
        next_ping: Instant::now(),
        last_rtc: Instant::now(),
        last_socket_ping: Instant::now(),
        rtt: 0,
    };
    let task = tokio::spawn(async move {
        let result = worker.drive(stop_rx, outgoing_rx).await;
        worker.cleanup().await;
        let mut state = worker.state.write().await;
        state["status"] = json!(if result.is_ok() { "closed" } else { "failed" });
        if let Err(error) = &result {
            state["error"] = json!(error.to_string());
        }
        result
    });
    let mut session = CallSession {
        state,
        media,
        join_url,
        stop: Some(stop_tx),
        task: Some(task),
    };
    match time::timeout(timeout, ready_rx).await {
        Ok(Ok(())) => Ok(session),
        Ok(Err(_)) => {
            let result = session
                .task
                .take()
                .expect("worker task")
                .await
                .map_err(|_| Error::Protocol("задача сессии остановлена"))?;
            Err(result
                .err()
                .unwrap_or(Error::Protocol("сессия закрыта до получения SDP")))
        }
        Err(_) => {
            let _ = session.close().await;
            Err(Error::Timeout("вход и получение SDP/ICE"))
        }
    }
}

struct Worker {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    call: CallInfo,
    timeout: Duration,
    state: Arc<RwLock<Value>>,
    media: MediaSlot,
    outgoing: mpsc::UnboundedSender<Value>,
    ready: Option<oneshot::Sender<()>>,
    group_id: String,
    join_id: String,
    joined: bool,
    track_requested: bool,
    track_cid: String,
    publisher_answer: bool,
    subscriber_answer: bool,
    turn_configured: bool,
    rtc_joined: bool,
    local_identity: String,
    track_subscriptions: HashSet<String>,
    pending: VecDeque<Value>,
    servers: Option<Value>,
    ping_interval: Duration,
    ping_timeout: Duration,
    next_ping: Instant,
    last_rtc: Instant,
    last_socket_ping: Instant,
    rtt: u64,
}

impl Worker {
    async fn send(&mut self, value: Value) -> Result<()> {
        time::timeout(
            self.timeout,
            self.socket.send(Message::Text(value.to_string().into())),
        )
        .await
        .map_err(|_| Error::Timeout("отправка signaling"))?
        .map_err(|_| Error::WebSocket)
    }

    async fn send_media(&mut self, payload: Value) -> Result<()> {
        self.send(
            json!({"event":"media-in","requestId":uuid::Uuid::new_v4().to_string(),
            "roomId":self.call.room_id,"groupId":self.group_id,"payload":payload}),
        )
        .await
    }

    async fn drive(
        &mut self,
        mut stop: oneshot::Receiver<()>,
        mut outgoing: mpsc::UnboundedReceiver<Value>,
    ) -> Result<()> {
        self.send(
            json!({"event":"join","requestId":self.join_id,"roomId":self.call.room_id,
            "payload":{"password":self.call.password,"participantName":self.call.name,
            "jazzToken":null,"sessionId":null,"supportedFeatures":{
                "nickname":true,"sessionGroups":true,"attachedRooms":true
            },"isSilent":false}}),
        )
        .await?;
        let mut timer = time::interval(Duration::from_millis(200));
        loop {
            tokio::select! {
                _ = &mut stop => return Ok(()),
                payload = outgoing.recv() => {
                    if let Some(payload) = payload { self.send_media(payload).await?; }
                }
                message = self.socket.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            let packet = serde_json::from_str::<Value>(&text).map_err(|_| Error::Protocol("ожидался JSON signaling"))?;
                            self.packet(packet).await?;
                        }
                        Some(Ok(Message::Ping(data))) => {
                            time::timeout(self.timeout, self.socket.send(Message::Pong(data))).await
                                .map_err(|_| Error::Timeout("WebSocket pong"))?.map_err(|_| Error::WebSocket)?;
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(_))) | None => return Err(Error::Protocol("сервер закрыл сессию")),
                        Some(Ok(_)) => return Err(Error::Protocol("неподдерживаемый WebSocket frame")),
                        Some(Err(_)) => return Err(Error::WebSocket),
                    }
                }
                _ = timer.tick() => {
                    let now = Instant::now();
                    if !self.ping_timeout.is_zero() && now.duration_since(self.last_rtc) > self.ping_timeout {
                        return Err(Error::Timeout("ответ медиасервера"));
                    }
                    if !self.ping_interval.is_zero() && now >= self.next_ping && self.joined {
                        self.next_ping = now + self.ping_interval;
                        self.send_media(json!({"method":"rtc:ping","ping_req":{"timestamp":timestamp(),"rtt":self.rtt}})).await?;
                    }
                    if now.duration_since(self.last_socket_ping) >= Duration::from_secs(45) {
                        self.last_socket_ping = now;
                        time::timeout(self.timeout,self.socket.send(Message::Ping(Vec::new().into()))).await
                            .map_err(|_| Error::Timeout("WebSocket ping"))?.map_err(|_| Error::WebSocket)?;
                    }
                }
            }
        }
    }

    async fn packet(&mut self, packet: Value) -> Result<()> {
        let event = packet["event"]
            .as_str()
            .ok_or(Error::Protocol("нет event в signaling"))?;
        if packet["requestId"] == self.join_id {
            if event == "error" || packet.get("error").is_some_and(|value| !value.is_null()) {
                return Err(Error::Protocol("сервер отклонил вход"));
            }
            if event == "join-response" {
                let payload = &packet["payload"];
                self.group_id = payload
                    .pointer("/participantGroup/groupId")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or(Error::Protocol("нет группы участника"))?
                    .to_owned();
                for path in ["/participant/sessionId", "/participant/participantId"] {
                    if payload.pointer(path).and_then(Value::as_str).is_none() {
                        return Err(Error::Protocol("неполный ответ входа"));
                    }
                }
                self.state.write().await["session"] = payload.clone();
                self.joined = true;
                self.state.write().await["status"] = json!("negotiating");
            }
        }
        if event == "error" || event == "evicted" || event == "meeting-stopped" {
            return Err(Error::Protocol("сервер завершил или отклонил операцию"));
        }
        if event == "media-out" {
            self.last_rtc = Instant::now();
            let payload = packet["payload"].clone();
            match payload["method"].as_str() {
                Some("rtc:config") => {
                    let servers = payload
                        .pointer("/configuration/iceServers")
                        .ok_or(Error::Protocol("нет ICE servers"))?;
                    if let Some(previous) = &self.servers {
                        if previous != servers {
                            return Err(Error::Protocol(
                                "ICE-конфигурация изменилась; требуется новый вход",
                            ));
                        }
                        if let Some(media) = self.media.read().await.clone() {
                            media.configure(servers).await?;
                        }
                    } else {
                        // The async WebRTC engine binds the initial ICE servers to its gatherer.
                        let media = Arc::new(
                            Media::new(servers, self.outgoing.clone(), self.call.carrier).await?,
                        );
                        *self.media.write().await = Some(media);
                        self.turn_configured = has_turn_credentials(servers);
                        self.servers = Some(servers.clone());
                        self.state.write().await["webrtc"]["ice_servers"] = servers.clone();
                    }
                }
                Some("rtc:join") => {
                    let join = &payload["join"];
                    self.local_identity = join
                        .pointer("/participant/identity")
                        .and_then(Value::as_str)
                        .filter(|identity| !identity.is_empty())
                        .ok_or(Error::Protocol("нет identity участника media"))?
                        .to_owned();
                    self.ping_interval = seconds(&join["pingInterval"])?;
                    self.ping_timeout = seconds(&join["pingTimeout"])?;
                    self.next_ping = Instant::now() + self.ping_interval;
                    self.state.write().await["rtc_join"] = join.clone();
                    self.rtc_joined = true;
                    if self.pending.len() >= 256 {
                        return Err(Error::Protocol("слишком много ранних media events"));
                    }
                    self.pending.push_back(payload);
                }
                Some("rtc:pong") => {
                    if let Some(value) = payload.pointer("/pong_resp/lastPingTimestamp") {
                        let timestamp = value
                            .as_u64()
                            .or_else(|| value.as_str().and_then(|v| v.parse().ok()));
                        if let Some(sent) = timestamp {
                            self.rtt = self::timestamp().saturating_sub(sent);
                        }
                    }
                }
                Some(_) => {
                    if self.joined && self.media.read().await.is_some() {
                        self.process_media(payload).await?;
                    } else {
                        if self.pending.len() >= 256 {
                            return Err(Error::Protocol("слишком много ранних media events"));
                        }
                        self.pending.push_back(payload);
                    }
                }
                None => return Err(Error::Protocol("нет media method")),
            }
        }
        if self.joined && self.media.read().await.is_some() {
            // The current web client publishes an explicitly unmuted track after rtc:join.
            if self.rtc_joined && !self.track_requested {
                self.track_requested = true;
                self.send_media(json!({"method":"rtc:track:add","addTrackRequest":
                    self.call.carrier.track_request(&self.track_cid)}))
                    .await?;
            }
            while let Some(payload) = self.pending.pop_front() {
                self.process_media(payload).await?;
            }
            if self.rtc_joined
                && self.publisher_answer
                && self.subscriber_answer
                && self.turn_configured
            {
                if let Some(ready) = self.ready.take() {
                    self.state.write().await["status"] = json!("ready");
                    let _ = ready.send(());
                }
            }
        }
        Ok(())
    }

    async fn process_media(&mut self, payload: Value) -> Result<()> {
        let media = self
            .media
            .read()
            .await
            .clone()
            .ok_or(Error::Protocol("ICE ещё не настроен"))?;
        match payload["method"].as_str() {
            Some("rtc:join") | Some("rtc:participants:update") => {
                let participants = if payload["method"] == "rtc:join" {
                    &payload["join"]["otherParticipants"]
                } else {
                    &payload["update"]["participants"]
                };
                for command in subscription_changes(
                    &self.local_identity,
                    &mut self.track_subscriptions,
                    participants,
                    self.call.carrier,
                )? {
                    self.send_media(command).await?;
                }
            }
            Some("rtc:track:published") => {
                let published = &payload["trackPublishedResponse"];
                if published["cid"] == self.track_cid {
                    let sid = published
                        .pointer("/track/sid")
                        .and_then(Value::as_str)
                        .ok_or(Error::Protocol("нет server track id"))?;
                    let offer = media.publish(&self.track_cid, sid).await?;
                    self.send_media(offer).await?;
                }
            }
            Some("rtc:offer") | Some("rtc:answer") => {
                let answer = media.description(&payload).await?;
                if payload["method"] == "rtc:answer" {
                    self.publisher_answer = true;
                }
                if let Some(answer) = answer {
                    self.send_media(answer).await?;
                    self.subscriber_answer = true;
                }
            }
            Some("rtc:ice") => {
                let items = payload["rtcIceCandidates"]
                    .as_array()
                    .ok_or(Error::Protocol("некорректный список ICE"))?;
                media.candidates(items).await?;
            }
            Some("rtc:error") => return Err(Error::Protocol("медиасервер отклонил операцию")),
            _ => {}
        }
        Ok(())
    }

    async fn cleanup(&mut self) {
        let leave_id = uuid::Uuid::new_v4().to_string();
        let leave = json!({"event":"leave","requestId":leave_id,"roomId":self.call.room_id,"groupId":self.group_id});
        let _ = time::timeout(
            Duration::from_millis(500),
            self.socket.send(Message::Text(leave.to_string().into())),
        )
        .await;
        // Drain in-flight ICE and wait for leave acknowledgement before closing the transport.
        let _ = time::timeout(Duration::from_millis(500), async {
            while let Some(message) = self.socket.next().await {
                match message {
                    Ok(Message::Text(text)) => {
                        if let Ok(packet) = serde_json::from_str::<Value>(&text) {
                            if packet["requestId"] == leave_id {
                                break;
                            }
                        }
                    }
                    Ok(Message::Ping(data)) => {
                        let _ = self.socket.send(Message::Pong(data)).await;
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }
        })
        .await;
        if let Some(media) = self.media.read().await.clone() {
            media.close().await;
        }
        let _ = time::timeout(Duration::from_secs(1), self.socket.close(None)).await;
    }
}

fn subscription_changes(
    local_identity: &str,
    subscribed: &mut HashSet<String>,
    participants: &Value,
    carrier: MediaCarrier,
) -> Result<Vec<Value>> {
    let mut commands = Vec::new();
    let Some(participants) = participants.as_array() else {
        return Ok(commands);
    };
    for participant in participants {
        let Some(identity) = participant["identity"].as_str() else {
            continue;
        };
        if identity == local_identity || local_identity.is_empty() {
            continue;
        }
        let Some(tracks) = participant["tracks"].as_array() else {
            continue;
        };
        for track in tracks {
            if track["type"] != carrier.track_type() {
                continue;
            }
            let sid = track["sid"]
                .as_str()
                .filter(|sid| !sid.is_empty() && sid.len() <= 256)
                .ok_or(Error::Protocol("некорректный remote media track sid"))?;
            let subscribe = track["muted"] != true && participant["state"] != "DISCONNECTED";
            let changed = if subscribe {
                if !subscribed.contains(sid) && subscribed.len() >= 4096 {
                    return Err(Error::Protocol("слишком много media subscriptions"));
                }
                subscribed.insert(sid.to_owned())
            } else {
                subscribed.remove(sid)
            };
            if changed {
                commands.push(json!({"method":"rtc:track:subscription:change",
                    "subscription":{"trackSids":[sid],"subscribe":subscribe}}));
                if subscribe && carrier.is_video() {
                    commands.push(json!({"method":"rtc:track:settings:update",
                        "trackSetting":{"trackSids":[sid],"disabled":false,"width":VIDEO_WIDTH,"height":VIDEO_HEIGHT}}));
                }
            }
        }
    }
    Ok(commands)
}

fn has_turn_credentials(servers: &Value) -> bool {
    servers.as_array().is_some_and(|servers| {
        servers.iter().any(|server| {
            let populated = |key: &str| server[key].as_str().is_some_and(|value| !value.is_empty());
            let is_turn = |value: &Value| {
                value.as_str().is_some_and(|url| {
                    let url = url.trim();
                    if !url.starts_with("turn:") {
                        return false;
                    }
                    let transport = url
                        .split_once('?')
                        .map(|(_, query)| query)
                        .unwrap_or("")
                        .split('&')
                        .find_map(|part| part.strip_prefix("transport="));
                    match transport {
                        None => true,
                        Some(value) => value.eq_ignore_ascii_case("udp"),
                    }
                })
            };
            populated("username")
                && populated("credential")
                && (is_turn(&server["url"])
                    || is_turn(&server["urls"])
                    || server["urls"]
                        .as_array()
                        .is_some_and(|urls| urls.iter().any(is_turn)))
        })
    })
}

fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn seconds(value: &Value) -> Result<Duration> {
    if value.is_null() {
        return Ok(Duration::ZERO);
    }
    let seconds = value
        .as_f64()
        .filter(|value| value.is_finite() && *value >= 0.0 && *value <= 86400.0)
        .ok_or(Error::Protocol("некорректный ping interval"))?;
    Ok(Duration::from_secs_f64(seconds))
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
