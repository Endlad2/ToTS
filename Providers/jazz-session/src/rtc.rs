use bytes::Bytes;
use serde_json::{json, Value};
use std::{net::IpAddr, sync::Arc};
use tokio::sync::{mpsc, Mutex, Notify, RwLock};
use webrtc::{
    api::{
        media_engine::{MediaEngine, MIME_TYPE_OPUS},
        setting_engine::SettingEngine,
        APIBuilder,
    },
    data_channel::{
        data_channel_init::RTCDataChannelInit,
        data_channel_state::RTCDataChannelState,
        RTCDataChannel,
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

const RELIABLE_LABEL: &str = "_reliable";
const LOSSY_LABEL: &str = "_lossy";
const MAX_USER_PACKET: usize = 15 * 1024;

#[derive(Debug, thiserror::Error)]
#[error("Не удалось обработать медиасессию")]
pub(crate) struct MediaError;

#[derive(Clone)]
pub(crate) struct DataPlane {
    publisher_channel: Arc<RwLock<Option<Arc<RTCDataChannel>>>>,
    subscriber_channel: Arc<RwLock<Option<Arc<RTCDataChannel>>>>,
    incoming: Arc<Mutex<mpsc::Receiver<Vec<u8>>>>,
    ready: Arc<Notify>,
}

impl DataPlane {
    pub(crate) async fn wait_ready(&self, timeout: std::time::Duration) -> Result<(), MediaError> {
        let wait = async {
            loop {
                // Register the waiter before checking channel state so an
                // on_open notification cannot be lost between the check and await.
                let notified = self.ready.notified();
                if self.open_send_channel().await.is_some() {
                    return Ok(());
                }
                notified.await;
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| MediaError)?
    }

    async fn open_send_channel(&self) -> Option<Arc<RTCDataChannel>> {
        // Match the LiveKit/Jazz client: prefer the publisher-side _reliable
        // channel. The subscriber channel is a fallback when the SFU exposes a
        // bidirectional incoming channel before the publisher one is open.
        for slot in [&self.publisher_channel, &self.subscriber_channel] {
            if let Some(channel) = slot.read().await.clone() {
                if channel.ready_state() == RTCDataChannelState::Open {
                    return Some(channel);
                }
            }
        }
        None
    }

    pub(crate) async fn send(&self, payload: &[u8]) -> Result<(), MediaError> {
        if payload.len() > MAX_USER_PACKET {
            return Err(MediaError);
        }
        let channel = self.open_send_channel().await.ok_or(MediaError)?;
        let packet = Bytes::from(encode_livekit_data_packet(payload)?);
        channel.send(&packet).await.map_err(|_| MediaError)?;
        Ok(())
    }

    pub(crate) async fn recv(&self) -> Result<Vec<u8>, MediaError> {
        match self.incoming.lock().await.recv().await {
            Some(payload) if !payload.is_empty() => Ok(payload),
            _ => Err(MediaError),
        }
    }
}

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
        outgoing: mpsc::UnboundedSender<Value>,
        subscriber_channel: Arc<RwLock<Option<Arc<RTCDataChannel>>>>,
        publisher_channel: Arc<RwLock<Option<Arc<RTCDataChannel>>>>,
        incoming: mpsc::Sender<Vec<u8>>,
        ready: Arc<Notify>,
    ) -> Result<Self, MediaError> {
        let mut engine = MediaEngine::default();
        engine.register_default_codecs().map_err(|_| MediaError)?;
        let mut settings = SettingEngine::default();
        // Keep Jazz ICE on UDP/IPv4. webrtc-rs 0.17 does not implement the
        // relay gather path needed for TURN/TCP/TLS and Windows commonly
        // exposes unusable link-local/IPv6 adapters that otherwise produce 10049.
        settings.set_network_types(vec![NetworkType::Udp4]);
        settings.set_ip_filter(Box::new(|ip| match ip {
            IpAddr::V4(ip) => !ip.is_link_local() && !ip.is_unspecified(),
            IpAddr::V6(_) => false,
        }));
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

        let channel_slot = subscriber_channel.clone();
        let publisher_slot = publisher_channel.clone();
        let incoming_tx = incoming.clone();
        let ready_notify = ready.clone();
        pc.on_data_channel(Box::new(move |dc| {
            attach_data_channel(
                dc,
                channel_slot.clone(),
                publisher_slot.clone(),
                incoming_tx.clone(),
                ready_notify.clone(),
            );
            Box::pin(async {})
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
        while !pending.is_empty() {
            let candidate = pending.remove(0);
            self.pc
                .add_ice_candidate(candidate)
                .await
                .map_err(|_| MediaError)?;
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

fn attach_data_channel(
    dc: Arc<RTCDataChannel>,
    slot: Arc<RwLock<Option<Arc<RTCDataChannel>>>>,
    other_slot: Arc<RwLock<Option<Arc<RTCDataChannel>>>>,
    incoming: mpsc::Sender<Vec<u8>>,
    ready: Arc<Notify>,
) {
    if dc.label() != RELIABLE_LABEL {
        return;
    }

    let opened = dc.clone();
    let opened_slot = slot.clone();
    let opened_ready = ready.clone();
    dc.on_open(Box::new(move || {
        Box::pin(async move {
            *opened_slot.write().await = Some(opened);
            opened_ready.notify_waiters();
        })
    }));

    let closed_incoming = incoming.clone();
    dc.on_message(Box::new(move |message| {
        let incoming = incoming.clone();
        Box::pin(async move {
            if let Ok(payload) = decode_livekit_data_packet(&message.data) {
                let _ = incoming.send(payload).await;
            }
        })
    }));

    let closed_slot = slot;
    dc.on_close(Box::new(move || {
        let closed_slot = closed_slot.clone();
        let other_slot = other_slot.clone();
        let closed_incoming = closed_incoming.clone();
        Box::pin(async move {
            *closed_slot.write().await = None;
            let other_open = other_slot
                .read()
                .await
                .as_ref()
                .is_some_and(|channel| channel.ready_state() == RTCDataChannelState::Open);
            if !other_open {
                // Encrypted ToTS packets are never empty, so an empty item is
                // a private close sentinel once both reliable paths are gone.
                let _ = closed_incoming.send(Vec::new()).await;
            }
        })
    }));
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
                false
            };
            if !supported {
                continue;
            }
            if parsed.scheme == SchemeType::Turn && (username.is_empty() || credential.is_empty()) {
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
    data: DataPlane,
}

impl Media {
    pub(crate) async fn new(
        ice_servers: &Value,
        outgoing: mpsc::UnboundedSender<Value>,
    ) -> Result<Self, MediaError> {
        let servers = configuration(ice_servers)?;
        let publisher_channel = Arc::new(RwLock::new(None));
        let subscriber_channel = Arc::new(RwLock::new(None));
        let ready = Arc::new(Notify::new());
        let (incoming_tx, incoming_rx) = mpsc::channel(2048);
        let data = DataPlane {
            publisher_channel: publisher_channel.clone(),
            subscriber_channel: subscriber_channel.clone(),
            incoming: Arc::new(Mutex::new(incoming_rx)),
            ready: ready.clone(),
        };

        let publisher = Peer::new(
            servers.clone(),
            "PUBLISHER",
            outgoing.clone(),
            subscriber_channel.clone(),
            publisher_channel.clone(),
            incoming_tx.clone(),
            ready.clone(),
        )
        .await?;
        let subscriber = match Peer::new(
            servers,
            "SUBSCRIBER",
            outgoing,
            subscriber_channel.clone(),
            publisher_channel.clone(),
            incoming_tx.clone(),
            ready.clone(),
        )
        .await
        {
            Ok(peer) => peer,
            Err(error) => {
                let _ = publisher.pc.close().await;
                return Err(error);
            }
        };

        // Jazz/LiveKit uses these publisher-side channels. The reliable one is
        // our actual ToTS carrier; the lossy channel is created because the
        // Jazz publisher SDP is expected to advertise both standard channels.
        let lossy = publisher
            .pc
            .create_data_channel(
                LOSSY_LABEL,
                Some(RTCDataChannelInit {
                    ordered: Some(false),
                    max_retransmits: Some(1),
                    ..Default::default()
                }),
            )
            .await
            .map_err(|_| MediaError)?;
        // Ignore payloads arriving over _lossy. It only makes the publisher
        // negotiation match Jazz/LiveKit clients.
        lossy.on_message(Box::new(|_| Box::pin(async {})));

        let reliable = publisher
            .pc
            .create_data_channel(
                RELIABLE_LABEL,
                Some(RTCDataChannelInit {
                    ordered: Some(true),
                    ..Default::default()
                }),
            )
            .await
            .map_err(|_| MediaError)?;
        attach_data_channel(
            reliable,
            publisher_channel,
            subscriber_channel,
            incoming_tx,
            ready,
        );

        Ok(Self {
            publisher,
            subscriber,
            data,
        })
    }

    pub(crate) fn data_plane(&self) -> DataPlane {
        self.data.clone()
    }

    pub(crate) async fn configure(&self, ice_servers: &Value) -> Result<(), MediaError> {
        let servers = configuration(ice_servers)?;
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

fn write_varint(mut value: usize, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn encode_livekit_data_packet(payload: &[u8]) -> Result<Vec<u8>, MediaError> {
    if payload.len() > MAX_USER_PACKET {
        return Err(MediaError);
    }
    // LiveKit protobuf:
    // DataPacket.user = field 2 (wire type 2)
    // UserPacket.payload = field 2 (wire type 2)
    let mut user = Vec::with_capacity(payload.len() + 8);
    user.push(0x12);
    write_varint(payload.len(), &mut user);
    user.extend_from_slice(payload);

    let mut packet = Vec::with_capacity(user.len() + 8);
    packet.push(0x12);
    write_varint(user.len(), &mut packet);
    packet.extend_from_slice(&user);
    Ok(packet)
}

fn read_varint(data: &[u8], cursor: &mut usize) -> Result<u64, MediaError> {
    let mut value = 0u64;
    let mut shift = 0u32;
    while *cursor < data.len() && shift <= 63 {
        let byte = data[*cursor];
        *cursor += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
    Err(MediaError)
}

fn find_length_delimited<'a>(data: &'a [u8], wanted: u64) -> Result<&'a [u8], MediaError> {
    let mut cursor = 0usize;
    while cursor < data.len() {
        let key = read_varint(data, &mut cursor)?;
        let field = key >> 3;
        let wire = key & 7;
        match wire {
            0 => {
                let _ = read_varint(data, &mut cursor)?;
            }
            1 => {
                cursor = cursor.checked_add(8).ok_or(MediaError)?;
                if cursor > data.len() {
                    return Err(MediaError);
                }
            }
            2 => {
                let len = usize::try_from(read_varint(data, &mut cursor)?).map_err(|_| MediaError)?;
                let end = cursor.checked_add(len).ok_or(MediaError)?;
                if end > data.len() {
                    return Err(MediaError);
                }
                if field == wanted {
                    return Ok(&data[cursor..end]);
                }
                cursor = end;
            }
            5 => {
                cursor = cursor.checked_add(4).ok_or(MediaError)?;
                if cursor > data.len() {
                    return Err(MediaError);
                }
            }
            _ => return Err(MediaError),
        }
    }
    Err(MediaError)
}

fn decode_livekit_data_packet(data: &[u8]) -> Result<Vec<u8>, MediaError> {
    let user = find_length_delimited(data, 2)?;
    let payload = find_length_delimited(user, 2)?;
    if payload.len() > MAX_USER_PACKET {
        return Err(MediaError);
    }
    Ok(payload.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    async fn media() -> (Media, mpsc::UnboundedReceiver<Value>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Media::new(&json!([]), tx).await.unwrap(), rx)
    }

    #[test]
    fn livekit_user_packet_round_trip() {
        let payloads = vec![Vec::new(), b"hello".to_vec(), vec![7u8; 1300]];
        for payload in payloads {
            let encoded = encode_livekit_data_packet(&payload).unwrap();
            assert_eq!(decode_livekit_data_packet(&encoded).unwrap(), payload);
        }
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
        assert!(sdp.contains("m=application"));
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
        assert_eq!(snapshot["publisher"]["remote_description"]["type"], "answer");
        assert_eq!(snapshot["publisher"]["remote_ice_candidates"][0], candidate);
        publisher.close().await;
        subscriber.close().await;
    }
}
