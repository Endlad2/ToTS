use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::{mpsc::UnboundedSender, Mutex};
use webrtc::{
    api::{
        media_engine::{MediaEngine, MIME_TYPE_OPUS},
        setting_engine::SettingEngine,
        APIBuilder,
    },
    ice::{
        candidate::candidate_base::unmarshal_candidate,
        network_type::NetworkType,
        url::{ProtoType, SchemeType, Url},
    },
    ice_transport::{ice_candidate::RTCIceCandidateInit, ice_server::RTCIceServer},
    peer_connection::{
        configuration::RTCConfiguration, sdp::session_description::RTCSessionDescription,
        RTCPeerConnection,
    },
    rtp_transceiver::{
        rtp_codec::RTCRtpCodecCapability, rtp_transceiver_direction::RTCRtpTransceiverDirection,
        RTCRtpTransceiverInit,
    },
    track::track_local::track_local_static_rtp::TrackLocalStaticRTP,
};

#[derive(Debug, thiserror::Error)]
#[error("Не удалось обработать медиасессию")]
pub(crate) struct MediaError;

struct Peer {
    pc: Arc<RTCPeerConnection>,
    local: Arc<Mutex<Vec<Value>>>,
    remote: Mutex<Vec<Value>>,
    pending: Mutex<Vec<RTCIceCandidateInit>>,
    operation: Mutex<()>,
}

impl Peer {
    async fn new(
        servers: Vec<RTCIceServer>,
        target: &'static str,
        outgoing: UnboundedSender<Value>,
    ) -> Result<Self, MediaError> {
        let mut engine = MediaEngine::default();
        engine.register_default_codecs().map_err(|_| MediaError)?;
        let mut settings = SettingEngine::default();
        // The Android Jazz client can use more transports, but webrtc-rs 0.17
        // cannot gather TURN/TCP or TURN/TLS candidates reliably on Windows.
        // Jazz always exposes UDP relay endpoints as well; keeping the ICE
        // agent on UDP/IPv4 avoids bogus IPv6/link-local adapters (10049) and
        // matches the transport we later reuse for the ToTS relay.
        settings.set_network_types(vec![NetworkType::Udp4]);
        let api = APIBuilder::new()
            .with_media_engine(engine)
            .with_setting_engine(settings)
            .build();
        let pc = Arc::new(
            api.new_peer_connection(RTCConfiguration {
                ice_servers: servers,
                ..Default::default()
            })
            .await
            .map_err(|_| MediaError)?,
        );
        let local = Arc::new(Mutex::new(Vec::new()));
        let saved = Arc::clone(&local);
        pc.on_ice_candidate(Box::new(move |candidate| {
            let saved = Arc::clone(&saved);
            let outgoing = outgoing.clone();
            Box::pin(async move {
                if let Some(candidate) = candidate {
                    if let Ok(candidate) = candidate.to_json() {
                        if let Ok(mut item) = serde_json::to_value(candidate) {
                            item["target"] = json!(target);
                            saved.lock().await.push(item.clone());
                            let _ = outgoing
                                .send(json!({"method":"rtc:ice", "rtcIceCandidates":[item]}));
                        }
                    }
                }
            })
        }));
        Ok(Self {
            pc,
            local,
            remote: Mutex::new(Vec::new()),
            pending: Mutex::new(Vec::new()),
            operation: Mutex::new(()),
        })
    }

    async fn set_remote(&self, description: RTCSessionDescription) -> Result<(), MediaError> {
        let _operation = self.operation.lock().await;
        self.pc
            .set_remote_description(description)
            .await
            .map_err(|_| MediaError)?;
        let mut pending = self.pending.lock().await;
        while let Some(candidate) = pending.first() {
            self.pc
                .add_ice_candidate(candidate.clone())
                .await
                .map_err(|_| MediaError)?;
            pending.remove(0);
        }
        Ok(())
    }

    async fn candidate(
        &self,
        item: Value,
        candidate: RTCIceCandidateInit,
    ) -> Result<(), MediaError> {
        let _operation = self.operation.lock().await;
        if self.pc.remote_description().await.is_some() {
            self.pc
                .add_ice_candidate(candidate)
                .await
                .map_err(|_| MediaError)?;
        } else {
            self.pending.lock().await.push(candidate);
        }
        self.remote.lock().await.push(item);
        Ok(())
    }

    async fn snapshot(&self) -> Value {
        json!({
            "local_description": self.pc.local_description().await,
            "remote_description": self.pc.remote_description().await,
            "local_ice_candidates": *self.local.lock().await,
            "remote_ice_candidates": *self.remote.lock().await,
            "connection_state": self.pc.connection_state().to_string(),
        })
    }
}

fn configuration(value: &Value) -> Result<Vec<RTCIceServer>, MediaError> {
    let entries = value.as_array().ok_or(MediaError)?;
    let mut result = Vec::new();

    for item in entries {
        let object = item.as_object().ok_or(MediaError)?;
        let mut raw_urls = match object.get("urls") {
            Some(Value::Array(items)) => items
                .iter()
                .map(|v| v.as_str().map(str::to_owned).ok_or(MediaError))
                .collect::<Result<Vec<_>, _>>()?,
            Some(Value::String(value)) => vec![value.clone()],
            None | Some(Value::Null) => Vec::new(),
            _ => return Err(MediaError),
        };
        if let Some(value) = object.get("url").filter(|v| !v.is_null()) {
            let url = value.as_str().ok_or(MediaError)?;
            if !raw_urls.iter().any(|item| item == url) {
                raw_urls.push(url.to_owned());
            }
        }

        let string = |key: &str| -> Result<String, MediaError> {
            match object.get(key) {
                None | Some(Value::Null) => Ok(String::new()),
                Some(Value::String(value)) => Ok(value.clone()),
                _ => Err(MediaError),
            }
        };
        let username = string("username")?;
        let credential = string("credential")?;

        let mut urls = Vec::new();
        for raw in raw_urls {
            let parsed = Url::parse_url(&raw).map_err(|_| MediaError)?;
            let supported = if parsed.scheme == SchemeType::Stun {
                true
            } else if parsed.scheme == SchemeType::Turn {
                parsed.proto == ProtoType::Udp
            } else {
                // webrtc-rs 0.17 logs "Unable to handle URL" for TURN/TCP and
                // TURN/TLS. Jazz provides UDP TURN alongside these URLs.
                false
            };
            if !supported {
                continue;
            }
            if parsed.scheme == SchemeType::Turn
                && (username.is_empty() || credential.is_empty())
            {
                return Err(MediaError);
            }
            urls.push(raw);
        }

        if !urls.is_empty() {
            result.push(RTCIceServer {
                urls,
                username,
                credential,
            });
        }
    }

    Ok(result)
}

pub(crate) struct Media {
    publisher: Peer,
    subscriber: Peer,
}

impl Media {
    pub(crate) async fn new(
        ice_servers: &Value,
        outgoing: UnboundedSender<Value>,
    ) -> Result<Self, MediaError> {
        let servers = configuration(ice_servers)?;
        let publisher = Peer::new(servers.clone(), "PUBLISHER", outgoing.clone()).await?;
        let subscriber = match Peer::new(servers, "SUBSCRIBER", outgoing).await {
            Ok(peer) => peer,
            Err(error) => {
                let _ = publisher.pc.close().await;
                return Err(error);
            }
        };
        Ok(Self {
            publisher,
            subscriber,
        })
    }

    pub(crate) async fn configure(&self, ice_servers: &Value) -> Result<(), MediaError> {
        let servers = configuration(ice_servers)?;
        // webrtc 0.17 cannot replace its ICE gatherer's servers; a changed config needs a new session.
        for peer in [&self.publisher, &self.subscriber] {
            if peer.pc.get_configuration().await.ice_servers != servers {
                return Err(MediaError);
            }
        }
        Ok(())
    }

    pub(crate) async fn publish(&self, cid: &str, sid: &str) -> Result<Value, MediaError> {
        if cid.is_empty() || sid.is_empty() {
            return Err(MediaError);
        }
        let _operation = self.publisher.operation.lock().await;
        let track = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: MIME_TYPE_OPUS.to_owned(),
                clock_rate: 48_000,
                channels: 2,
                ..Default::default()
            },
            cid.to_owned(),
            sid.to_owned(),
        ));
        self.publisher
            .pc
            .add_transceiver_from_track(
                track,
                Some(RTCRtpTransceiverInit {
                    direction: RTCRtpTransceiverDirection::Sendonly,
                    send_encodings: Vec::new(),
                }),
            )
            .await
            .map_err(|_| MediaError)?;
        let offer = self
            .publisher
            .pc
            .create_offer(None)
            .await
            .map_err(|_| MediaError)?;
        self.publisher
            .pc
            .set_local_description(offer.clone())
            .await
            .map_err(|_| MediaError)?;
        Ok(json!({"method":"rtc:offer", "description":offer}))
    }

    pub(crate) async fn description(&self, payload: &Value) -> Result<Option<Value>, MediaError> {
        let description = payload.get("description").ok_or(MediaError)?;
        let sdp = description
            .get("sdp")
            .and_then(Value::as_str)
            .ok_or(MediaError)?
            .to_owned();
        match (
            payload.get("method").and_then(Value::as_str),
            description.get("type").and_then(Value::as_str),
        ) {
            (Some("rtc:offer"), Some("offer")) => {
                let offer = RTCSessionDescription::offer(sdp).map_err(|_| MediaError)?;
                self.subscriber.set_remote(offer).await?;
                let answer = self
                    .subscriber
                    .pc
                    .create_answer(None)
                    .await
                    .map_err(|_| MediaError)?;
                self.subscriber
                    .pc
                    .set_local_description(answer.clone())
                    .await
                    .map_err(|_| MediaError)?;
                Ok(Some(json!({"method":"rtc:answer", "description":answer})))
            }
            (Some("rtc:answer"), Some("answer")) => {
                let answer = RTCSessionDescription::answer(sdp).map_err(|_| MediaError)?;
                self.publisher.set_remote(answer).await?;
                Ok(None)
            }
            _ => Err(MediaError),
        }
    }

    pub(crate) async fn candidates(&self, items: &[Value]) -> Result<(), MediaError> {
        for item in items {
            let peer = match item.get("target").and_then(Value::as_str) {
                Some("PUBLISHER") => &self.publisher,
                Some("SUBSCRIBER") => &self.subscriber,
                _ => return Err(MediaError),
            };
            let candidate: RTCIceCandidateInit =
                serde_json::from_value(item.clone()).map_err(|_| MediaError)?;
            let text = candidate
                .candidate
                .strip_prefix("candidate:")
                .unwrap_or(&candidate.candidate);
            if !text.is_empty() {
                unmarshal_candidate(text).map_err(|_| MediaError)?;
            }
            peer.candidate(item.clone(), candidate).await?;
        }
        Ok(())
    }

    pub(crate) async fn snapshot(&self) -> Value {
        json!({"publisher": self.publisher.snapshot().await, "subscriber": self.subscriber.snapshot().await})
    }

    pub(crate) async fn close(&self) {
        let _ = self.publisher.pc.close().await;
        let _ = self.subscriber.pc.close().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    async fn media() -> (Media, mpsc::UnboundedReceiver<Value>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Media::new(&json!([]), tx).await.unwrap(), rx)
    }

    #[tokio::test]
    async fn real_local_peers_negotiate_without_capture_devices() {
        let (publisher, _publisher_ice) = media().await;
        let (subscriber, mut subscriber_ice) = media().await;
        let offer = publisher
            .publish("local-audio", "server-stream")
            .await
            .unwrap();
        assert_eq!(offer["method"], "rtc:offer");
        let sdp = offer["description"]["sdp"].as_str().unwrap();
        assert!(sdp.contains("a=ice-ufrag:"));
        assert!(sdp.contains("a=ice-pwd:"));
        assert!(sdp.contains("a=fingerprint:"));
        assert!(sdp.contains("a=sendonly"));
        assert!(sdp.contains("server-stream"));
        let answer = subscriber.description(&offer).await.unwrap().unwrap();
        assert_eq!(answer["method"], "rtc:answer");
        let candidates =
            tokio::time::timeout(std::time::Duration::from_secs(5), subscriber_ice.recv())
                .await
                .unwrap()
                .unwrap();
        let mut candidate = candidates["rtcIceCandidates"][0].clone();
        assert_eq!(candidate["target"], "SUBSCRIBER");
        candidate["target"] = json!("PUBLISHER");
        publisher.candidates(&[candidate.clone()]).await.unwrap();
        assert!(publisher.description(&answer).await.unwrap().is_none());
        let snapshot = publisher.snapshot().await;
        assert_eq!(snapshot["publisher"]["local_description"]["type"], "offer");
        assert_eq!(
            snapshot["publisher"]["remote_description"]["type"],
            "answer"
        );
        assert_eq!(snapshot["publisher"]["remote_ice_candidates"][0], candidate);
        assert!(snapshot["subscriber"]["remote_description"].is_null());
        publisher.close().await;
        subscriber.close().await;
        assert_eq!(
            publisher.snapshot().await["publisher"]["connection_state"],
            "closed"
        );
    }

    #[tokio::test]
    async fn candidates_arriving_before_remote_sdp_are_retained_and_applied() {
        let (remote, mut remote_ice) = media().await;
        let (local, _local_ice) = media().await;
        let offer = remote.publish("audio", "stream").await.unwrap();
        let payload = tokio::time::timeout(std::time::Duration::from_secs(5), remote_ice.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(payload["method"], "rtc:ice");
        let mut item = payload["rtcIceCandidates"][0].clone();
        assert_eq!(item["target"], "PUBLISHER");
        item["target"] = json!("SUBSCRIBER");
        local.candidates(&[item.clone()]).await.unwrap();
        let before = local.snapshot().await;
        assert_eq!(before["subscriber"]["remote_ice_candidates"][0], item);
        assert!(before["subscriber"]["remote_description"].is_null());
        assert!(local.description(&offer).await.unwrap().is_some());
        assert_eq!(
            local.snapshot().await["subscriber"]["remote_ice_candidates"][0],
            item
        );
        remote.close().await;
        local.close().await;
    }

    #[test]
    fn jazz_ice_configuration_keeps_only_webrtc_rs_supported_urls() {
        let servers = configuration(&json!([{
            "urls": [
                "stun:stun.example.test:3478",
                "turn:relay.example.test:3478?transport=udp",
                "turn:relay.example.test:3478?transport=tcp",
                "turns:relay.example.test:443?transport=tcp"
            ],
            "username": "u",
            "credential": "p"
        }]))
        .unwrap();
        assert_eq!(servers.len(), 1);
        assert_eq!(
            servers[0].urls,
            vec![
                "stun:stun.example.test:3478".to_string(),
                "turn:relay.example.test:3478?transport=udp".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn malformed_ice_configuration_is_rejected_without_exposing_credentials() {
        for config in [
            json!({"urls": ["turn:example.invalid"]}),
            json!([{"urls": 7, "username": "secret-user", "credential": "secret-pass"}]),
            json!([{"urls": ["https://secret.example.invalid"], "credential": "secret-pass"}]),
            json!([{"urls": ["turn:example.invalid"], "username": "secret-user"}]),
        ] {
            let (tx, _rx) = mpsc::unbounded_channel();
            let error = match Media::new(&config, tx).await {
                Err(error) => error,
                Ok(media) => {
                    media.close().await;
                    panic!("invalid configuration accepted")
                }
            };
            let rendered = format!("{error} {error:?}");
            assert!(!rendered.contains("secret"));
            assert!(!rendered.contains("example.invalid"));
        }
        let (local, _rx) = media().await;
        assert!(local.configure(&json!([{"urls": false}])).await.is_err());
        local.close().await;
    }

    #[tokio::test]
    async fn turn_credential_changes_require_a_new_media_session() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let initial = json!([{"urls":["turn:localhost:3478"],"username":"secret-user","credential":"secret-original"}]);
        let local = Media::new(&initial, tx).await.unwrap();
        local.configure(&initial).await.unwrap();
        let mut changed = initial.clone();
        changed[0]["credential"] = json!("secret-rotated");
        let error = local.configure(&changed).await.unwrap_err();
        assert!(!format!("{error} {error:?}").contains("secret"));
        local.configure(&initial).await.unwrap();
        local.close().await;
    }

    #[tokio::test]
    async fn rejects_bad_signaling_and_accepts_legacy_ice_url() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let local = Media::new(&json!([{"url":"stun:localhost:3478"}]), tx)
            .await
            .unwrap();
        let error = local.description(&json!({"method":"rtc:offer","description":{"type":"offer","sdp":"secret invalid sdp"}})).await.unwrap_err();
        assert!(!format!("{error} {error:?}").contains("secret"));
        assert!(local
            .candidates(&[json!({"candidate":"secret invalid candidate","target":"OTHER"})])
            .await
            .is_err());
        assert!(local
            .candidates(&[json!({"candidate":"secret invalid candidate","target":"PUBLISHER"})])
            .await
            .is_err());
        local.close().await;
    }
}
