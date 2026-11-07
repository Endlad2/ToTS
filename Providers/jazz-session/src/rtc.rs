use bytes::Bytes;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, Mutex, Notify, RwLock};
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
        peer_connection_state::RTCPeerConnectionState,
        RTCPeerConnection,
    },
    rtp::{header::Header as RtpHeader, packet::Packet as RtpPacket},
    rtp_transceiver::{
        rtp_codec::RTCRtpCodecCapability, rtp_transceiver_direction::RTCRtpTransceiverDirection,
        RTCRtpTransceiverInit,
    },
    track::{
        track_local::{track_local_static_rtp::TrackLocalStaticRTP, TrackLocalWriter},
        track_remote::TrackRemote,
    },
};

// SaluteJazz' Android client does not use WebRTC DataChannel for participant
// data; its PeerConnection observer leaves onDataChannel() empty. The SFU may
// therefore accept SCTP locally without forwarding those messages to the other
// participant. Carry ToTS bytes over the media plane that Jazz actually
// forwards: the published AUDIO/MICROPHONE RTP stream.
const RTP_TOTS_MAGIC: &[u8; 4] = b"TOTS";
const RTP_TOTS_VERSION: u8 = 1;
const RTP_TOTS_HEADER: usize = 23;
const RTP_TOTS_CHUNK: usize = 1000;
const MAX_MEDIA_MESSAGE: usize = 32 * 1024;
const MAX_MEDIA_FRAGMENTS: usize = 64;
const REASSEMBLY_TTL: Duration = Duration::from_secs(15);
const RTP_CLOCK_STEP: u32 = 960; // 20 ms at 48 kHz

#[derive(Debug, thiserror::Error)]
#[error("Не удалось обработать медиасессию")]
pub(crate) struct MediaError;

#[derive(Clone)]
pub(crate) struct DataPlane {
    outgoing_track: Arc<RwLock<Option<Arc<TrackLocalStaticRTP>>>>,
    incoming: Arc<Mutex<mpsc::Receiver<Vec<u8>>>>,
    ready: Arc<Notify>,
    publisher_connected: Arc<AtomicBool>,
    subscriber_connected: Arc<AtomicBool>,
    incoming_track_ready: Arc<AtomicBool>,
    source_id: u64,
    next_message_id: Arc<AtomicU32>,
    tx_clock: Arc<Mutex<(u16, u32)>>,
}

impl DataPlane {
    pub(crate) async fn wait_ready(&self, timeout: std::time::Duration) -> Result<(), MediaError> {
        let wait = async {
            loop {
                // Register the waiter before checking readiness so a state
                // notification cannot be lost between the check and await.
                let notified = self.ready.notified();
                if self.publisher_connected.load(Ordering::SeqCst)
                    && self.subscriber_connected.load(Ordering::SeqCst)
                    && self.outgoing_track.read().await.is_some()
                {
                    return Ok(());
                }
                notified.await;
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| MediaError)?
    }

    pub(crate) async fn wait_receive_ready(&self, timeout: std::time::Duration) -> Result<(), MediaError> {
        let wait = async {
            loop {
                let notified = self.ready.notified();
                if self.publisher_connected.load(Ordering::SeqCst)
                    && self.subscriber_connected.load(Ordering::SeqCst)
                    && self.outgoing_track.read().await.is_some()
                    && self.incoming_track_ready.load(Ordering::SeqCst)
                {
                    return Ok(());
                }
                notified.await;
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| MediaError)?
    }

    pub(crate) async fn send(&self, payload: &[u8]) -> Result<(), MediaError> {
        if payload.is_empty() || payload.len() > MAX_MEDIA_MESSAGE {
            return Err(MediaError);
        }
        let track = self
            .outgoing_track
            .read()
            .await
            .clone()
            .ok_or(MediaError)?;
        let message_id = self.next_message_id.fetch_add(1, Ordering::Relaxed);
        let fragments = encode_rtp_fragments(self.source_id, message_id, payload)?;
        let mut clock = self.tx_clock.lock().await;
        let timestamp = clock.1;
        for (index, fragment) in fragments.into_iter().enumerate() {
            let packet = RtpPacket {
                header: RtpHeader {
                    version: 2,
                    marker: index + 1 == fragments_len(payload),
                    sequence_number: clock.0,
                    timestamp,
                    ..Default::default()
                },
                payload: Bytes::from(fragment),
            };
            clock.0 = clock.0.wrapping_add(1);
            let written = track.write_rtp(&packet).await.map_err(|_| MediaError)?;
            if written == 0 {
                return Err(MediaError);
            }
        }
        clock.1 = clock.1.wrapping_add(RTP_CLOCK_STEP);
        Ok(())
    }

    pub(crate) async fn recv(&self) -> Result<Vec<u8>, MediaError> {
        match self.incoming.lock().await.recv().await {
            Some(payload) if !payload.is_empty() => Ok(payload),
            _ => Err(MediaError),
        }
    }
}

fn fragments_len(payload: &[u8]) -> usize {
    payload.len().div_ceil(RTP_TOTS_CHUNK)
}

fn encode_rtp_fragments(
    source_id: u64,
    message_id: u32,
    payload: &[u8],
) -> Result<Vec<Vec<u8>>, MediaError> {
    let count = fragments_len(payload);
    if count == 0 || count > MAX_MEDIA_FRAGMENTS || payload.len() > u16::MAX as usize {
        return Err(MediaError);
    }
    let total_len = payload.len() as u16;
    let mut result = Vec::with_capacity(count);
    for index in 0..count {
        let start = index * RTP_TOTS_CHUNK;
        let end = (start + RTP_TOTS_CHUNK).min(payload.len());
        let mut out = Vec::with_capacity(RTP_TOTS_HEADER + end - start);
        out.extend_from_slice(RTP_TOTS_MAGIC);
        out.push(RTP_TOTS_VERSION);
        out.extend_from_slice(&source_id.to_be_bytes());
        out.extend_from_slice(&message_id.to_be_bytes());
        out.extend_from_slice(&(index as u16).to_be_bytes());
        out.extend_from_slice(&(count as u16).to_be_bytes());
        out.extend_from_slice(&total_len.to_be_bytes());
        out.extend_from_slice(&payload[start..end]);
        result.push(out);
    }
    Ok(result)
}

struct Reassembly {
    created: Instant,
    total_len: usize,
    fragments: Vec<Option<Vec<u8>>>,
    received: usize,
}

#[derive(Default)]
struct Reassembler {
    messages: HashMap<(u64, u32), Reassembly>,
}

impl Reassembler {
    fn push(&mut self, local_source_id: u64, packet: &[u8]) -> Option<Vec<u8>> {
        let now = Instant::now();
        self.messages
            .retain(|_, value| now.duration_since(value.created) <= REASSEMBLY_TTL);
        if self.messages.len() > 128 {
            self.messages.clear();
        }
        if packet.len() < RTP_TOTS_HEADER || &packet[..4] != RTP_TOTS_MAGIC {
            return None;
        }
        if packet[4] != RTP_TOTS_VERSION {
            return None;
        }
        let source_id = u64::from_be_bytes(packet[5..13].try_into().ok()?);
        if source_id == local_source_id {
            return None;
        }
        let message_id = u32::from_be_bytes(packet[13..17].try_into().ok()?);
        let index = u16::from_be_bytes(packet[17..19].try_into().ok()?) as usize;
        let count = u16::from_be_bytes(packet[19..21].try_into().ok()?) as usize;
        let total_len = u16::from_be_bytes(packet[21..23].try_into().ok()?) as usize;
        if count == 0
            || count > MAX_MEDIA_FRAGMENTS
            || index >= count
            || total_len == 0
            || total_len > MAX_MEDIA_MESSAGE
            || packet.len() - RTP_TOTS_HEADER > RTP_TOTS_CHUNK
        {
            return None;
        }
        let key = (source_id, message_id);
        let entry = self.messages.entry(key).or_insert_with(|| Reassembly {
            created: now,
            total_len,
            fragments: vec![None; count],
            received: 0,
        });
        if entry.total_len != total_len || entry.fragments.len() != count {
            self.messages.remove(&key);
            return None;
        }
        if entry.fragments[index].is_none() {
            entry.fragments[index] = Some(packet[RTP_TOTS_HEADER..].to_vec());
            entry.received += 1;
        }
        if entry.received != count {
            return None;
        }
        let entry = self.messages.remove(&key)?;
        let mut result = Vec::with_capacity(entry.total_len);
        for fragment in entry.fragments {
            result.extend_from_slice(fragment.as_deref()?);
        }
        if result.len() != entry.total_len {
            return None;
        }
        Some(result)
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
        incoming: mpsc::Sender<Vec<u8>>,
        reassembler: Arc<Mutex<Reassembler>>,
        ready: Arc<Notify>,
        connected: Arc<AtomicBool>,
        incoming_track_ready: Arc<AtomicBool>,
        local_source_id: u64,
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

        let ready_notify = ready.clone();
        pc.on_peer_connection_state_change(Box::new(move |state| {
            let connected = connected.clone();
            let ready_notify = ready_notify.clone();
            Box::pin(async move {
                let is_connected = state == RTCPeerConnectionState::Connected;
                connected.store(is_connected, Ordering::SeqCst);
                if is_connected {
                    ready_notify.notify_waiters();
                }
            })
        }));

        if target == "SUBSCRIBER" {
            pc.on_track(Box::new(move |track: Arc<TrackRemote>, _, _| {
                let incoming = incoming.clone();
                let reassembler = reassembler.clone();
                let incoming_track_ready = incoming_track_ready.clone();
                let ready = ready.clone();
                Box::pin(async move {
                    incoming_track_ready.store(true, Ordering::SeqCst);
                    ready.notify_waiters();
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 2048];
                        loop {
                            let packet = match track.read(&mut buf).await {
                                Ok((packet, _)) => packet,
                                Err(_) => break,
                            };
                            let completed = reassembler
                                .lock()
                                .await
                                .push(local_source_id, &packet.payload);
                            if let Some(payload) = completed {
                                if incoming.send(payload).await.is_err() {
                                    break;
                                }
                            }
                        }
                    });
                })
            }));
        }

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
        let ready = Arc::new(Notify::new());
        let publisher_connected = Arc::new(AtomicBool::new(false));
        let subscriber_connected = Arc::new(AtomicBool::new(false));
        let incoming_track_ready = Arc::new(AtomicBool::new(false));
        let outgoing_track = Arc::new(RwLock::new(None));
        let (incoming_tx, incoming_rx) = mpsc::channel(2048);
        let reassembler = Arc::new(Mutex::new(Reassembler::default()));
        let source_id = (uuid::Uuid::new_v4().as_u128() & u128::from(u64::MAX)) as u64;
        let data = DataPlane {
            outgoing_track: outgoing_track.clone(),
            incoming: Arc::new(Mutex::new(incoming_rx)),
            ready: ready.clone(),
            publisher_connected: publisher_connected.clone(),
            subscriber_connected: subscriber_connected.clone(),
            incoming_track_ready: incoming_track_ready.clone(),
            source_id,
            next_message_id: Arc::new(AtomicU32::new(1)),
            tx_clock: Arc::new(Mutex::new((1, 0))),
        };

        let publisher = Peer::new(
            servers.clone(),
            "PUBLISHER",
            outgoing.clone(),
            incoming_tx.clone(),
            reassembler.clone(),
            ready.clone(),
            publisher_connected,
            incoming_track_ready.clone(),
            source_id,
        )
        .await?;
        let subscriber = match Peer::new(
            servers,
            "SUBSCRIBER",
            outgoing,
            incoming_tx,
            reassembler,
            ready.clone(),
            subscriber_connected,
            incoming_track_ready,
            source_id,
        )
        .await
        {
            Ok(peer) => peer,
            Err(error) => {
                let _ = publisher.pc.close().await;
                return Err(error);
            }
        };

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
        *self.data.outgoing_track.write().await = Some(track.clone());
        self.data.ready.notify_waiters();
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

    #[test]
    fn rtp_tots_fragment_round_trip() {
        let source_id = 0x1020_3040_5060_7080;
        let payload = vec![7u8; 2501];
        let fragments = encode_rtp_fragments(source_id, 42, &payload).unwrap();
        assert_eq!(fragments.len(), 3);
        let mut reassembler = Reassembler::default();
        assert!(reassembler.push(99, &fragments[2]).is_none());
        assert!(reassembler.push(99, &fragments[0]).is_none());
        assert_eq!(reassembler.push(99, &fragments[1]).unwrap(), payload);
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
        assert!(sdp.contains("m=audio"));
        assert!(!sdp.contains("m=application"));
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
