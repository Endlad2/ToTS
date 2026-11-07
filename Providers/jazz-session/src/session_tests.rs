use super::{establish, CallInfo};
use crate::rtc::Media;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};
use url::Url;

const NAME: &str = "Fixture participant";
const PASSWORD: &str = "fixture-private-password";
const JOIN_URL: &str = "https://salutejazz.ru/calls/fixture-room?psw=fixture-private-invite";

#[derive(Clone, Copy)]
enum Mode {
    Empty,
    Bidirectional,
    NoTurn,
    Refuse,
    Timeout,
}

struct Fixture {
    connector: Url,
    events: mpsc::UnboundedReceiver<&'static str>,
    task: Option<JoinHandle<Result<(), String>>>,
}

impl Fixture {
    async fn start(mode: Mode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = Url::parse(&format!(
            "ws://{}/connector",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let (events_tx, events) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.map_err(|_| "accept failed")?;
            let socket = accept_async(stream)
                .await
                .map_err(|_| "WebSocket handshake failed")?;
            serve(socket, mode, events_tx).await
        });
        Self {
            connector,
            events,
            task: Some(task),
        }
    }

    fn call(&self) -> CallInfo {
        CallInfo {
            name: NAME.to_owned(),
            room_id: "fixture-room".to_owned(),
            password: PASSWORD.to_owned(),
            join_url: JOIN_URL.to_owned(),
            connector_url: self.connector.clone(),
        }
    }

    async fn event(&mut self, expected: &'static str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(event) = self.events.recv().await {
                if event == expected {
                    return;
                }
            }
            panic!("fixture closed before {expected}");
        })
        .await
        .expect("fixture event timeout");
    }

    async fn finish(mut self) {
        tokio::time::timeout(Duration::from_secs(5), self.task.take().unwrap())
            .await
            .expect("fixture task timeout")
            .expect("fixture task panicked")
            .expect("fixture protocol failed");
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn ice_servers() -> Value {
    json!([{"urls":["turn:127.0.0.1:9?transport=udp"],"username":"fixture-user","credential":"fixture-pass"}])
}

async fn send(socket: &mut WebSocketStream<TcpStream>, value: Value) -> Result<(), String> {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .map_err(|_| "fixture write failed".to_owned())
}

async fn media_out(socket: &mut WebSocketStream<TcpStream>, payload: Value) -> Result<(), String> {
    send(socket, json!({"event":"media-out","roomId":"fixture-room","groupId":"fixture-group","payload":payload})).await
}

fn swap_targets(payload: &mut Value) {
    if let Some(items) = payload["rtcIceCandidates"].as_array_mut() {
        for item in items {
            item["target"] = match item["target"].as_str() {
                Some("PUBLISHER") => json!("SUBSCRIBER"),
                Some("SUBSCRIBER") => json!("PUBLISHER"),
                _ => panic!("fixture received invalid ICE target"),
            };
        }
    }
}

async fn serve(
    mut socket: WebSocketStream<TcpStream>,
    mode: Mode,
    events: mpsc::UnboundedSender<&'static str>,
) -> Result<(), String> {
    let (ice_tx, mut ice_rx) = mpsc::unbounded_channel();
    let media = Media::new(&json!([]), ice_tx)
        .await
        .map_err(|_| "fixture Media init failed")?;
    let result = async {
        let mut subscriber_offer_sent = false;
        loop {
            tokio::select! {
                ice = ice_rx.recv() => {
                    if let Some(mut ice) = ice {
                        swap_targets(&mut ice);
                        media_out(&mut socket, ice).await?;
                    }
                }
                incoming = socket.next() => {
                    let message = match incoming {
                        Some(Ok(Message::Text(text))) => serde_json::from_str::<Value>(&text).map_err(|_| "client did not send JSON")?,
                        Some(Ok(Message::Ping(payload))) => {
                            socket.send(Message::Pong(payload)).await.map_err(|_| "fixture pong failed")?;
                            continue;
                        }
                        Some(Ok(Message::Close(_))) | None => return Ok(()),
                        Some(Ok(_)) => continue,
                        Some(Err(_)) if matches!(mode, Mode::Refuse | Mode::Timeout) => return Ok(()),
                        Some(Err(_)) => return Err("client transport failed".to_owned()),
                    };
                    match message["event"].as_str() {
                        Some("join") => {
                            if message["payload"]["participantName"] != NAME || message["payload"]["password"] != PASSWORD || message["payload"]["isSilent"] != false {
                                return Err("invalid join fields".to_owned());
                            }
                            let request_id = message["requestId"].as_str().ok_or("missing join requestId")?;
                            let _ = events.send("join");
                            if matches!(mode, Mode::Timeout) { continue; }
                            if matches!(mode, Mode::Refuse) {
                                send(&mut socket, json!({"event":"join-response","requestId":request_id,"roomId":"fixture-room","error":{"code":"FORBIDDEN","message":"fixture-private-password fixture-pass fixture-private-invite"}})).await?;
                                continue;
                            }
                            let servers = if matches!(mode, Mode::NoTurn) { json!([]) } else { ice_servers() };
                            media_out(&mut socket, json!({"method":"rtc:config","configuration":{"iceServers":servers}})).await?;
                            media_out(&mut socket, json!({"method":"rtc:join","join":{"participant":{"sid":"fixture-rtc-participant","identity":"fixture-participant"},"otherParticipants":[],"pingInterval":1,"pingTimeout":3}})).await?;
                            send(&mut socket, json!({"event":"join-response","requestId":request_id,"roomId":"fixture-room","groupId":"fixture-group","payload":{
                                "participant":{"sessionId":"fixture-session","participantId":"fixture-participant"},
                                "participantGroup":{"groupId":"fixture-group","participants":[]},
                                "meetingId":"fixture-meeting"
                            }})).await?;
                        }
                        Some("media-in") => {
                            let payload = &message["payload"];
                            match payload["method"].as_str() {
                                Some("rtc:track:add") => {
                                    let cid = payload["addTrackRequest"]["cid"].as_str().ok_or("missing track cid")?;
                                    if payload["addTrackRequest"]["type"] != "AUDIO"
                                        || payload["addTrackRequest"]["source"] != "MICROPHONE"
                                        || !payload["addTrackRequest"]["muted"].is_null()
                                    {
                                        return Err("invalid published audio track".to_owned());
                                    }
                                    media_out(&mut socket, json!({"method":"rtc:track:published","trackPublishedResponse":{"cid":cid,"track":{"sid":"server-track","type":"AUDIO","source":"MICROPHONE","muted":true}}})).await?;
                                }
                                Some("rtc:offer") => {
                                    let answer = media.description(payload).await.map_err(|_| "fixture failed to answer client SDP")?.ok_or("fixture answer missing")?;
                                    media_out(&mut socket, answer).await?;
                                    let _ = events.send("publisher-answer");
                                    if matches!(mode, Mode::Bidirectional | Mode::NoTurn) && !subscriber_offer_sent {
                                        subscriber_offer_sent = true;
                                        let offer = media.publish("fixture-server-audio", "fixture-server-stream").await.map_err(|_| "fixture offer failed")?;
                                        media_out(&mut socket, offer).await?;
                                    }
                                }
                                Some("rtc:answer") => {
                                    media.description(payload).await.map_err(|_| "fixture could not apply client answer")?;
                                    let _ = events.send("subscriber-answer");
                                }
                                Some("rtc:ice") => {
                                    let mut swapped = payload.clone();
                                    swap_targets(&mut swapped);
                                    let candidates = swapped["rtcIceCandidates"].as_array().ok_or("invalid client ICE list")?;
                                    media.candidates(candidates).await.map_err(|_| "fixture could not apply client ICE")?;
                                }
                                Some("rtc:ping") => {
                                    media_out(&mut socket, json!({"method":"rtc:pong","pong_resp":{"lastPingTimestamp":payload["ping_req"]["lastPingTimestamp"]}})).await?;
                                }
                                _ => {}
                            }
                        }
                        Some("leave") => {
                            let _ = events.send("leave");
                            send(&mut socket, json!({"event":"leave-response","requestId":message["requestId"],"roomId":"fixture-room","payload":{}})).await?;
                            return Ok(());
                        }
                        _ => {}
                    }
                }
            }
        }
    }.await;
    media.close().await;
    result
}

fn assert_real_description(description: &Value, expected_type: &str) {
    assert_eq!(description["type"], expected_type);
    let sdp = description["sdp"].as_str().expect("real SDP missing");
    for attribute in [
        "v=0",
        "m=audio",
        "a=ice-ufrag:",
        "a=ice-pwd:",
        "a=fingerprint:sha-256",
    ] {
        assert!(sdp.contains(attribute), "missing SDP attribute {attribute}");
    }
}

#[tokio::test]
async fn successful_session_contains_real_bidirectional_sdp_and_turn_credentials() {
    let mut fixture = Fixture::start(Mode::Bidirectional).await;
    let session = establish(fixture.call(), Duration::from_secs(8))
        .await
        .expect("session should negotiate with real fixture peers");
    fixture.event("subscriber-answer").await;
    let data = session.json().await;
    assert_eq!(data["status"], "ready");
    assert_eq!(data["call"]["name"], NAME);
    assert_eq!(data["call"]["room_id"], "fixture-room");
    assert_eq!(data["call"]["join_url"], JOIN_URL);
    assert_eq!(
        data["session"]["participant"]["sessionId"],
        "fixture-session"
    );
    assert_eq!(
        data["session"]["participant"]["participantId"],
        "fixture-participant"
    );
    assert_eq!(
        data["session"]["participantGroup"]["groupId"],
        "fixture-group"
    );
    assert_eq!(data["session"]["meetingId"], "fixture-meeting");
    assert_eq!(data["webrtc"]["ice_servers"], ice_servers());
    assert_real_description(&data["webrtc"]["publisher"]["local_description"], "offer");
    assert_real_description(&data["webrtc"]["publisher"]["remote_description"], "answer");
    assert_real_description(&data["webrtc"]["subscriber"]["local_description"], "answer");
    assert_real_description(&data["webrtc"]["subscriber"]["remote_description"], "offer");
    session.close().await.unwrap();
    fixture.event("leave").await;
    fixture.finish().await;
}

#[tokio::test]
async fn missing_subscriber_sdp_times_out_and_sends_leave() {
    let mut fixture = Fixture::start(Mode::Empty).await;
    let result = establish(fixture.call(), Duration::from_millis(700)).await;
    assert!(
        matches!(result, Err(crate::Error::Timeout(_))),
        "incomplete SDP must not be ready"
    );
    fixture.event("publisher-answer").await;
    fixture.event("leave").await;
    fixture.finish().await;
}

#[tokio::test]
async fn missing_turn_credentials_times_out_and_sends_leave() {
    let mut fixture = Fixture::start(Mode::NoTurn).await;
    let result = establish(fixture.call(), Duration::from_millis(700)).await;
    assert!(matches!(result, Err(crate::Error::Timeout(_))));
    fixture.event("subscriber-answer").await;
    fixture.event("leave").await;
    fixture.finish().await;
}

#[tokio::test]
async fn dropping_a_session_sends_leave() {
    let mut fixture = Fixture::start(Mode::Bidirectional).await;
    let session = establish(fixture.call(), Duration::from_secs(8))
        .await
        .unwrap();
    drop(session);
    fixture.event("leave").await;
    fixture.finish().await;
}

#[tokio::test]
async fn refused_join_returns_an_error_without_credentials() {
    let mut fixture = Fixture::start(Mode::Refuse).await;
    let result = establish(fixture.call(), Duration::from_secs(3)).await;
    fixture.event("join").await;
    let error = match result {
        Err(error) => error,
        Ok(session) => {
            session.close().await.unwrap();
            panic!("refused join was accepted");
        }
    };
    assert!(
        !matches!(error, crate::Error::Timeout(_)),
        "refusal must not wait for timeout"
    );
    let rendered = format!("{error} {error:?}");
    for secret in [PASSWORD, "fixture-pass", "fixture-private-invite", JOIN_URL] {
        assert!(!rendered.contains(secret));
    }
    fixture.finish().await;
}

#[tokio::test]
async fn incomplete_join_times_out_without_credentials() {
    let mut fixture = Fixture::start(Mode::Timeout).await;
    let started = tokio::time::Instant::now();
    let result = establish(fixture.call(), Duration::from_millis(150)).await;
    fixture.event("join").await;
    let error = match result {
        Err(error) => error,
        Ok(session) => {
            session.close().await.unwrap();
            panic!("incomplete join was accepted");
        }
    };
    assert!(matches!(error, crate::Error::Timeout(_)));
    assert!(started.elapsed() < Duration::from_secs(2));
    let rendered = format!("{error} {error:?}");
    for secret in [PASSWORD, "fixture-pass", JOIN_URL] {
        assert!(!rendered.contains(secret));
    }
    fixture.finish().await;
}
