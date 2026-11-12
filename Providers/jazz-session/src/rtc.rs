use crate::carrier::{wallclock_timestamp, MediaCarrier};
use bytes::Bytes;
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, oneshot, Mutex, Notify, RwLock},
    task::JoinHandle,
};
use webrtc::{
    api::{
        interceptor_registry::register_default_interceptors,
        media_engine::{MediaEngine, MIME_TYPE_OPUS, MIME_TYPE_VP8},
        setting_engine::SettingEngine,
        APIBuilder,
    },
    ice::{
        candidate::candidate_base::unmarshal_candidate,
        network_type::NetworkType,
        url::{ProtoType, SchemeType, Url},
    },
    ice_transport::{ice_candidate::RTCIceCandidateInit, ice_server::RTCIceServer},
    interceptor::registry::Registry,
    peer_connection::{
        configuration::RTCConfiguration, peer_connection_state::RTCPeerConnectionState,
        sdp::session_description::RTCSessionDescription, RTCPeerConnection,
    },
    rtcp::{
        packet::Packet as RtcpPacket, receiver_report::ReceiverReport, sender_report::SenderReport,
    },
    rtp::{header::Header as RtpHeader, packet::Packet as RtpPacket},
    rtp_transceiver::{
        rtp_codec::{RTCRtpCodecCapability, RTPCodecType},
        rtp_transceiver_direction::RTCRtpTransceiverDirection,
        RTCRtpTransceiverInit,
    },
    stats::{StatsReport, StatsReportType},
    track::{
        track_local::{track_local_static_rtp::TrackLocalStaticRTP, TrackLocalWriter},
        track_remote::TrackRemote,
    },
    util::MarshalSize,
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
const VIDEO_FRAME_INTERVAL: Duration = Duration::from_micros(16_667);
const VIDEO_QUEUE_MESSAGES: usize = 64;
const MAX_VIDEO_FRAME_BYTES: usize = 64 * 1024;
const VIDEO_CLOSE_TIMEOUT: Duration = Duration::from_millis(250);

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
    tx_clock: Arc<Mutex<(u16, u64)>>,
    clock_started: Instant,
    carrier: MediaCarrier,
    video: Option<Arc<VideoBatcher>>,
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

    pub(crate) async fn wait_receive_ready(
        &self,
        timeout: std::time::Duration,
    ) -> Result<(), MediaError> {
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
        if let Some(video) = &self.video {
            if self.outgoing_track.read().await.is_none() {
                return Err(MediaError);
            }
            return video.send(payload).await;
        }
        let message_id = self.next_message_id.fetch_add(1, Ordering::Relaxed);
        let fragments = encode_rtp_fragments(self.source_id, message_id, payload)?;
        self.write_frame(fragments, message_id as u16 & 0x7fff)
            .await
    }

    async fn write_frame(&self, fragments: Vec<Vec<u8>>, picture: u16) -> Result<(), MediaError> {
        let track = self.outgoing_track.read().await.clone().ok_or(MediaError)?;
        let frame_len = fragments.iter().map(Vec::len).sum();
        let packet_count = fragments.len();
        let mut clock = self.tx_clock.lock().await;
        if self.carrier.uses_wallclock() {
            clock.1 = wallclock_timestamp(
                self.clock_started.elapsed(),
                self.carrier.clock_rate(),
                clock.1,
            );
        }
        let timestamp = clock.1 as u32;
        for (index, fragment) in fragments.into_iter().enumerate() {
            let packet = RtpPacket {
                header: RtpHeader {
                    version: 2,
                    marker: index + 1 == packet_count,
                    sequence_number: clock.0,
                    timestamp,
                    ..Default::default()
                },
                payload: if self.carrier.is_video() {
                    Bytes::from(self.carrier.wrap_fragment(
                        &fragment,
                        index == 0,
                        frame_len,
                        picture,
                    ))
                } else {
                    Bytes::from(fragment)
                },
            };
            clock.0 = clock.0.wrapping_add(1);
            let written = track.write_rtp(&packet).await.map_err(|_| MediaError)?;
            if written == 0 {
                return Err(MediaError);
            }
        }
        if !self.carrier.uses_wallclock() {
            clock.1 = clock.1.wrapping_add(u64::from(RTP_CLOCK_STEP));
        }
        Ok(())
    }

    pub(crate) async fn recv(&self) -> Result<Vec<u8>, MediaError> {
        match self.incoming.lock().await.recv().await {
            Some(payload) if !payload.is_empty() => Ok(payload),
            _ => Err(MediaError),
        }
    }
}

enum VideoCommand {
    Packet(Vec<u8>),
    Close(oneshot::Sender<()>),
}

struct VideoBatcher {
    outgoing: mpsc::Sender<VideoCommand>,
    closed: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl VideoBatcher {
    fn new(writer: DataPlane) -> Self {
        let (outgoing, incoming) = mpsc::channel(VIDEO_QUEUE_MESSAGES);
        let closed = Arc::new(AtomicBool::new(false));
        let failed = closed.clone();
        let task = tokio::spawn(async move {
            let _ = Self::run(writer, incoming).await;
            failed.store(true, Ordering::SeqCst);
        });
        Self {
            outgoing,
            closed,
            task,
        }
    }

    async fn send(&self, payload: &[u8]) -> Result<(), MediaError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(MediaError);
        }
        let permit = self.outgoing.reserve().await.map_err(|_| MediaError)?;
        if self.closed.load(Ordering::SeqCst) {
            return Err(MediaError);
        }
        permit.send(VideoCommand::Packet(payload.to_vec()));
        Ok(())
    }

    async fn run(
        writer: DataPlane,
        mut incoming: mpsc::Receiver<VideoCommand>,
    ) -> Result<(), MediaError> {
        let mut pending = None;
        let mut picture = 1;
        loop {
            let first = match pending.take() {
                Some(payload) => payload,
                None => match incoming.recv().await {
                    Some(VideoCommand::Packet(payload)) => payload,
                    Some(VideoCommand::Close(done)) => {
                        let _ = done.send(());
                        return Ok(());
                    }
                    None => return Ok(()),
                },
            };
            let deadline = tokio::time::Instant::now() + VIDEO_FRAME_INTERVAL;
            let mut bytes = first.len() + fragments_len(&first) * RTP_TOTS_HEADER;
            let mut batch = vec![first];
            let mut closing = None;
            loop {
                if batch.len() == VIDEO_QUEUE_MESSAGES || bytes == MAX_VIDEO_FRAME_BYTES {
                    tokio::time::sleep_until(deadline).await;
                    break;
                }
                match tokio::time::timeout_at(deadline, incoming.recv()).await {
                    Ok(Some(VideoCommand::Packet(payload))) => {
                        let size = payload.len() + fragments_len(&payload) * RTP_TOTS_HEADER;
                        if bytes + size > MAX_VIDEO_FRAME_BYTES {
                            pending = Some(payload);
                            tokio::time::sleep_until(deadline).await;
                            break;
                        }
                        bytes += size;
                        batch.push(payload);
                    }
                    Ok(Some(VideoCommand::Close(done))) => {
                        incoming.close();
                        closing = Some(done);
                        break;
                    }
                    Ok(None) | Err(_) => break,
                }
            }
            let mut fragments = Vec::new();
            for payload in batch {
                let id = writer.next_message_id.fetch_add(1, Ordering::Relaxed);
                fragments.extend(encode_rtp_fragments(writer.source_id, id, &payload)?);
            }
            writer.write_frame(fragments, picture).await?;
            picture = picture.wrapping_add(1) & 0x7fff;
            if let Some(done) = closing {
                let _ = done.send(());
                return Ok(());
            }
        }
    }

    async fn close(&self) {
        if !self.closed.swap(true, Ordering::SeqCst) {
            // Flush accepted packets in FIFO order, but never hold session shutdown indefinitely.
            let flush = async {
                let (done, finished) = oneshot::channel();
                self.outgoing
                    .send(VideoCommand::Close(done))
                    .await
                    .map_err(|_| MediaError)?;
                finished.await.map_err(|_| MediaError)
            };
            let _ = tokio::time::timeout(VIDEO_CLOSE_TIMEOUT, flush).await;
        }
        self.task.abort();
    }

    fn abort(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.task.abort();
    }
}

impl Drop for VideoBatcher {
    fn drop(&mut self) {
        self.abort();
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

#[derive(Default, Serialize)]
struct NativeReceiveCounters {
    rtp_packets: AtomicU64,
    rtp_payload_bytes: AtomicU64,
    rtp_header_bytes: AtomicU64,
    padding_packets: AtomicU64,
    empty_payload_packets: AtomicU64,
    unwrap_rejects: AtomicU64,
    fragment_rejects: AtomicU64,
    self_fragments: AtomicU64,
    duplicate_fragments: AtomicU64,
    reassembly_expired: AtomicU64,
    reassembly_evicted: AtomicU64,
    pending_assemblies: AtomicU64,
    completed_messages: AtomicU64,
    completed_bytes: AtomicU64,
    track_read_errors: AtomicU64,
    rtcp_packets: AtomicU64,
    rtcp_sender_reports: AtomicU64,
    rtcp_receiver_reports: AtomicU64,
    rtcp_read_errors: AtomicU64,
}

impl NativeReceiveCounters {
    fn rtcp(&self, packets: &[Box<dyn RtcpPacket + Send + Sync>]) {
        self.rtcp_packets
            .fetch_add(packets.len() as u64, Ordering::Relaxed);
        for packet in packets {
            if packet.as_any().is::<SenderReport>() {
                self.rtcp_sender_reports.fetch_add(1, Ordering::Relaxed);
            }
            if packet.as_any().is::<ReceiverReport>() {
                self.rtcp_receiver_reports.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Default)]
struct Reassembler {
    messages: HashMap<(u64, u32), Reassembly>,
    counters: Arc<NativeReceiveCounters>,
}

impl Reassembler {
    fn push(&mut self, local_source_id: u64, packet: &[u8]) -> Option<Vec<u8>> {
        let result = self.push_inner(local_source_id, packet);
        self.counters
            .pending_assemblies
            .store(self.messages.len() as u64, Ordering::Relaxed);
        if let Some(payload) = &result {
            self.counters
                .completed_messages
                .fetch_add(1, Ordering::Relaxed);
            self.counters
                .completed_bytes
                .fetch_add(payload.len() as u64, Ordering::Relaxed);
        }
        result
    }

    fn reject(&self) -> Option<Vec<u8>> {
        self.counters
            .fragment_rejects
            .fetch_add(1, Ordering::Relaxed);
        None
    }

    fn push_inner(&mut self, local_source_id: u64, packet: &[u8]) -> Option<Vec<u8>> {
        let now = Instant::now();
        let before = self.messages.len();
        self.messages
            .retain(|_, value| now.duration_since(value.created) <= REASSEMBLY_TTL);
        self.counters
            .reassembly_expired
            .fetch_add((before - self.messages.len()) as u64, Ordering::Relaxed);
        if self.messages.len() > 128 {
            self.counters
                .reassembly_evicted
                .fetch_add(self.messages.len() as u64, Ordering::Relaxed);
            self.messages.clear();
        }
        if packet.len() < RTP_TOTS_HEADER || &packet[..4] != RTP_TOTS_MAGIC {
            return self.reject();
        }
        if packet[4] != RTP_TOTS_VERSION {
            return self.reject();
        }
        let source_id = u64::from_be_bytes(packet[5..13].try_into().ok()?);
        if source_id == local_source_id {
            self.counters.self_fragments.fetch_add(1, Ordering::Relaxed);
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
            return self.reject();
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
            return self.reject();
        }
        if entry.fragments[index].is_none() {
            entry.fragments[index] = Some(packet[RTP_TOTS_HEADER..].to_vec());
            entry.received += 1;
        } else {
            self.counters
                .duplicate_fragments
                .fetch_add(1, Ordering::Relaxed);
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
            return self.reject();
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
        carrier: MediaCarrier,
    ) -> Result<Self, MediaError> {
        let mut engine = MediaEngine::default();
        engine.register_default_codecs().map_err(|_| MediaError)?;
        let registry = if carrier.is_video() {
            register_default_interceptors(Registry::new(), &mut engine).map_err(|_| MediaError)?
        } else {
            Registry::new()
        };
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
            .with_interceptor_registry(registry)
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
            let counters = reassembler.lock().await.counters.clone();
            pc.on_track(Box::new(move |track: Arc<TrackRemote>, receiver, _| {
                let incoming = incoming.clone();
                let reassembler = reassembler.clone();
                let incoming_track_ready = incoming_track_ready.clone();
                let ready = ready.clone();
                let counters = counters.clone();
                Box::pin(async move {
                    let expected = if carrier.is_video() {
                        RTPCodecType::Video
                    } else {
                        RTPCodecType::Audio
                    };
                    if track.kind() != expected {
                        return;
                    }
                    incoming_track_ready.store(true, Ordering::SeqCst);
                    ready.notify_waiters();
                    if carrier.is_video() {
                        let counters = counters.clone();
                        tokio::spawn(async move {
                            while let Ok((packets, _)) = receiver.read_rtcp().await {
                                counters.rtcp(&packets);
                            }
                            counters.rtcp_read_errors.fetch_add(1, Ordering::Relaxed);
                        });
                    }
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 2048];
                        loop {
                            let packet = match track.read(&mut buf).await {
                                Ok((packet, _)) => packet,
                                Err(_) => {
                                    counters.track_read_errors.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                            };
                            counters.rtp_packets.fetch_add(1, Ordering::Relaxed);
                            counters
                                .rtp_payload_bytes
                                .fetch_add(packet.payload.len() as u64, Ordering::Relaxed);
                            counters
                                .rtp_header_bytes
                                .fetch_add(packet.header.marshal_size() as u64, Ordering::Relaxed);
                            if packet.header.padding {
                                counters.padding_packets.fetch_add(1, Ordering::Relaxed);
                            }
                            if packet.payload.is_empty() {
                                counters
                                    .empty_payload_packets
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                            let Some(fragment) = carrier.unwrap_fragment(&packet.payload) else {
                                counters.unwrap_rejects.fetch_add(1, Ordering::Relaxed);
                                continue;
                            };
                            let completed =
                                reassembler.lock().await.push(local_source_id, fragment);
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
        let stats = self.pc.get_stats().await;
        let selected = self
            .pc
            .dtls_transport()
            .ice_transport()
            .get_selected_candidate_pair()
            .await;
        let selected = selected.map(|pair| json!({
            "local":ice_candidate_summary(&pair.local.address,pair.local.port,&pair.local.typ.to_string()),
            "remote":ice_candidate_summary(&pair.remote.address,pair.remote.port,&pair.remote.typ.to_string())}));
        json!({
            "local_description": self.pc.local_description().await,
            "remote_description": self.pc.remote_description().await,
            "local_ice_candidates": *self.local.lock().await,
            "remote_ice_candidates": *self.remote.lock().await,
            "connection_state": self.pc.connection_state().to_string(),
            "ice_state":self.pc.ice_connection_state().to_string(),
            "gathering_state":self.pc.ice_gathering_state().to_string(),
            "selected_pair":selected,
            "ice_diagnostics":ice_stats_summary(&stats),
            "rtp_diagnostics":rtp_stats_summary(&stats),
        })
    }
}

fn rtp_stats_summary(stats: &StatsReport) -> Value {
    let (mut transport_received, mut transport_sent) = (0u64, 0u64);
    let (mut packets_received, mut payload_received, mut headers_received, mut nacks_sent) =
        (0u64, 0u64, 0u64, 0u64);
    let (mut packets_sent, mut payload_sent, mut headers_sent, mut nacks_received) =
        (0u64, 0u64, 0u64, 0u64);
    for report in stats.reports.values() {
        match report {
            StatsReportType::Transport(report) => {
                transport_received += report.bytes_received as u64;
                transport_sent += report.bytes_sent as u64;
            }
            StatsReportType::InboundRTP(report) => {
                packets_received += report.packets_received;
                payload_received += report.bytes_received;
                headers_received += report.header_bytes_received;
                nacks_sent += report.nack_count;
            }
            StatsReportType::OutboundRTP(report) => {
                packets_sent += report.packets_sent;
                payload_sent += report.bytes_sent;
                headers_sent += report.header_bytes_sent;
                nacks_received += report.nack_count;
            }
            _ => {}
        }
    }
    json!({"transport_received":transport_received,"transport_sent":transport_sent,
        "rtp_packets_received":packets_received,"rtp_payload_received":payload_received,"rtp_headers_received":headers_received,"nacks_sent":nacks_sent,
        "rtp_packets_sent":packets_sent,"rtp_payload_sent":payload_sent,"rtp_headers_sent":headers_sent,"nacks_received":nacks_received})
}

fn ice_candidate_summary(address: &str, port: u16, kind: &str) -> Value {
    let kind = match kind {
        "host" => "host",
        "srflx" => "srflx",
        "prflx" => "prflx",
        "relay" => "relay",
        _ => "unknown",
    };
    let class = match address.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => {
            let [a, b, _, _] = ip.octets();
            if ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_documentation()
                || a == 0
                || a >= 224
                || (a == 100 && (64..=127).contains(&b))
                || (a == 198 && (18..=19).contains(&b))
            {
                "private_or_reserved"
            } else {
                "public"
            }
        }
        Ok(IpAddr::V6(ip)) => {
            if ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || (ip.segments()[0] == 0x2001 && ip.segments()[1] == 0xdb8)
            {
                "private_or_reserved"
            } else {
                "public"
            }
        }
        Err(_) => "unparsed",
    };
    json!({"candidate_type":kind,"ip_class":class,"port":port})
}

fn ice_stats_summary(stats: &StatsReport) -> Value {
    let mut states = json!({"waiting":0,"in-progress":0,"succeeded":0,"failed":0,"unspecified":0});
    let mut nominated = Vec::new();
    let candidate = |id: &str| match stats.reports.get(id) {
        Some(StatsReportType::LocalCandidate(value))
        | Some(StatsReportType::RemoteCandidate(value)) => {
            ice_candidate_summary(&value.ip, value.port, &value.candidate_type.to_string())
        }
        _ => Value::Null,
    };
    for report in stats.reports.values() {
        if let StatsReportType::CandidatePair(pair) = report {
            let state = pair.state.to_string();
            states[&state] = json!(states[&state].as_u64().unwrap_or(0) + 1);
            if pair.nominated {
                nominated.push(
                    json!({"state":state,"local":candidate(&pair.local_candidate_id),
                    "remote":candidate(&pair.remote_candidate_id)}),
                );
            }
        }
    }
    // webrtc-rs currently leaves request/response/RTT counters at zero defaults.
    json!({"pair_states":states,"nominated_pairs":nominated})
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
    receive_counters: Arc<NativeReceiveCounters>,
}

impl Media {
    pub(crate) async fn new(
        ice_servers: &Value,
        outgoing: mpsc::UnboundedSender<Value>,
        carrier: MediaCarrier,
    ) -> Result<Self, MediaError> {
        let servers = configuration(ice_servers)?;
        let ready = Arc::new(Notify::new());
        let publisher_connected = Arc::new(AtomicBool::new(false));
        let subscriber_connected = Arc::new(AtomicBool::new(false));
        let incoming_track_ready = Arc::new(AtomicBool::new(false));
        let outgoing_track = Arc::new(RwLock::new(None));
        let (incoming_tx, incoming_rx) = mpsc::channel(2048);
        let reassembler = Reassembler::default();
        let receive_counters = reassembler.counters.clone();
        let reassembler = Arc::new(Mutex::new(reassembler));
        let source_id = (uuid::Uuid::new_v4().as_u128() & u128::from(u64::MAX)) as u64;
        let mut data = DataPlane {
            outgoing_track: outgoing_track.clone(),
            incoming: Arc::new(Mutex::new(incoming_rx)),
            ready: ready.clone(),
            publisher_connected: publisher_connected.clone(),
            subscriber_connected: subscriber_connected.clone(),
            incoming_track_ready: incoming_track_ready.clone(),
            source_id,
            next_message_id: Arc::new(AtomicU32::new(1)),
            tx_clock: Arc::new(Mutex::new((1, 0))),
            clock_started: Instant::now(),
            carrier,
            video: None,
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
            carrier,
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
            carrier,
        )
        .await
        {
            Ok(peer) => peer,
            Err(error) => {
                let _ = publisher.pc.close().await;
                return Err(error);
            }
        };

        if carrier.is_video() {
            // The writer clone has no batcher, so it cannot retain its own task through an Arc cycle.
            data.video = Some(Arc::new(VideoBatcher::new(data.clone())));
        }

        Ok(Self {
            publisher,
            subscriber,
            data,
            receive_counters,
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
                mime_type: if self.data.carrier.is_video() {
                    MIME_TYPE_VP8
                } else {
                    MIME_TYPE_OPUS
                }
                .to_owned(),
                clock_rate: self.data.carrier.clock_rate(),
                channels: if self.data.carrier.is_video() { 0 } else { 2 },
                ..Default::default()
            },
            cid.to_owned(),
            sid.to_owned(),
        ));
        *self.data.outgoing_track.write().await = Some(track.clone());
        self.data.ready.notify_waiters();
        let transceiver = self
            .publisher
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
        if self.data.carrier.is_video() {
            let sender = transceiver.sender().await;
            let counters = self.receive_counters.clone();
            tokio::spawn(async move {
                while let Ok((packets, _)) = sender.read_rtcp().await {
                    counters.rtcp(&packets);
                }
                counters.rtcp_read_errors.fetch_add(1, Ordering::Relaxed);
            });
        }
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
        json!({"publisher": self.publisher.snapshot().await, "subscriber": self.subscriber.snapshot().await,
            "native_receive":self.receive_counters.as_ref()})
    }

    pub(crate) async fn close(&self) {
        if let Some(video) = &self.data.video {
            video.close().await;
        }
        let _ = self.publisher.pc.close().await;
        let _ = self.subscriber.pc.close().await;
    }
}

impl Drop for Media {
    fn drop(&mut self) {
        if let Some(video) = &self.data.video {
            video.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    async fn media() -> (Media, mpsc::UnboundedReceiver<Value>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Media::new(&json!([]), tx, MediaCarrier::Audio)
                .await
                .unwrap(),
            rx,
        )
    }

    #[tokio::test]
    async fn experimental_video_negotiates_default_receiver_feedback_without_changing_audio() {
        for carrier in [
            MediaCarrier::Audio,
            MediaCarrier::AudioWallclock,
            MediaCarrier::Vp8Experimental,
        ] {
            let (tx, _rx) = mpsc::unbounded_channel();
            let media = Media::new(&json!([]), tx, carrier).await.unwrap();
            let offer = media
                .publish("feedback-track", "feedback-stream")
                .await
                .unwrap();
            let sdp = offer["description"]["sdp"].as_str().unwrap();
            let twcc =
                sdp.contains("transport-cc") && sdp.contains("transport-wide-cc-extensions-01");
            media.close().await;
            assert_eq!(
                twcc,
                carrier.is_video(),
                "video must configure the default feedback interceptors"
            );
        }
    }

    #[tokio::test]
    async fn experimental_video_exchanges_rtcp_reports_and_closes_its_feedback_tasks() {
        let (left_tx, mut left_ice) = mpsc::unbounded_channel();
        let (right_tx, mut right_ice) = mpsc::unbounded_channel();
        let left = Media::new(&json!([]), left_tx, MediaCarrier::Vp8Experimental)
            .await
            .unwrap();
        let right = Media::new(&json!([]), right_tx, MediaCarrier::Vp8Experimental)
            .await
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            connect_local_pair(&left, &right, &mut left_ice, &mut right_ice).await?;
            left.data.send(b"opaque forward").await?;
            assert_eq!(right.data.recv().await?, b"opaque forward");
            right.data.send(b"opaque reverse").await?;
            assert_eq!(left.data.recv().await?, b"opaque reverse");
            loop {
                let ready = [&left, &right].iter().all(|media| {
                    media
                        .receive_counters
                        .rtcp_sender_reports
                        .load(Ordering::Relaxed)
                        > 0
                        && media
                            .receive_counters
                            .rtcp_receiver_reports
                            .load(Ordering::Relaxed)
                            > 0
                });
                if ready {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<(), MediaError>(())
        })
        .await;
        tokio::time::timeout(Duration::from_secs(2), async {
            left.close().await;
            right.close().await;
        })
        .await
        .expect("feedback tasks prevented media shutdown");
        result
            .expect("local RTCP report exchange timed out")
            .unwrap();
        let before = left.receive_counters.rtcp_packets.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(
            left.receive_counters.rtcp_packets.load(Ordering::Relaxed),
            before
        );
    }

    #[test]
    fn native_reassembly_counters_separate_rejection_pending_and_complete_messages() {
        let mut reassembler = Reassembler::default();
        let fragments = encode_rtp_fragments(100, 1, &vec![7; 1100]).unwrap();
        assert!(reassembler.push(0, &fragments[0]).is_none());
        assert!(reassembler.push(0, &fragments[0]).is_none());
        assert!(reassembler.push(0, b"malformed encrypted bytes").is_none());
        assert!(reassembler.push(100, &fragments[1]).is_none());
        assert!(reassembler.push(0, &fragments[1]).is_some());
        let snapshot = json!(reassembler.counters.as_ref());
        assert_eq!(snapshot["completed_messages"], 1);
        assert_eq!(snapshot["completed_bytes"], 1100);
        assert_eq!(snapshot["pending_assemblies"], 0);
        assert_eq!(snapshot["fragment_rejects"], 1);
        assert_eq!(snapshot["self_fragments"], 1);
        assert_eq!(snapshot["duplicate_fragments"], 1);
        assert!(snapshot.as_object().unwrap().values().all(Value::is_u64));
    }

    async fn connect_local_pair(
        left: &Media,
        right: &Media,
        left_ice: &mut mpsc::UnboundedReceiver<Value>,
        right_ice: &mut mpsc::UnboundedReceiver<Value>,
    ) -> Result<(), MediaError> {
        let offer = left.publish("left-track", "left-stream").await?;
        let answer = right.description(&offer).await?.ok_or(MediaError)?;
        left.description(&answer).await?;
        let offer = right.publish("right-track", "right-stream").await?;
        let answer = left.description(&offer).await?.ok_or(MediaError)?;
        right.description(&answer).await?;
        let ready = async {
            left.data.wait_ready(Duration::from_secs(10)).await?;
            right.data.wait_ready(Duration::from_secs(10)).await
        };
        tokio::pin!(ready);
        loop {
            let (event, destination) = tokio::select! {
                result = &mut ready => return result,
                event = left_ice.recv() => (event, right),
                event = right_ice.recv() => (event, left),
            };
            let mut event = event.ok_or(MediaError)?;
            for candidate in event["rtcIceCandidates"].as_array_mut().ok_or(MediaError)? {
                candidate["target"] = if candidate["target"] == "PUBLISHER" {
                    json!("SUBSCRIBER")
                } else {
                    json!("PUBLISHER")
                };
            }
            destination
                .candidates(event["rtcIceCandidates"].as_array().ok_or(MediaError)?)
                .await?;
        }
    }

    #[tokio::test]
    async fn video_batches_serial_and_concurrent_datagrams_into_real_rtp_frames() {
        let carrier = MediaCarrier::Vp8Experimental;
        let (left_tx, mut left_ice) = mpsc::unbounded_channel();
        let (right_tx, mut right_ice) = mpsc::unbounded_channel();
        let left = Media::new(&json!([]), left_tx, carrier).await.unwrap();
        let right = Media::new(&json!([]), right_tx, carrier).await.unwrap();
        let (packet_tx, mut packets) = mpsc::channel(64);
        right.subscriber.pc.on_track(Box::new(move |track, _, _| {
            let packet_tx = packet_tx.clone();
            Box::pin(async move {
                tokio::spawn(async move {
                    let mut buf = [0; 2048];
                    while let Ok((packet, _)) = track.read(&mut buf).await {
                        if packet_tx.send(packet).await.is_err() {
                            break;
                        }
                    }
                });
            })
        }));
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            connect_local_pair(&left, &right, &mut left_ice, &mut right_ice).await?;
            let mut payloads: Vec<Vec<u8>> = [2300, 300, 1800, 2501, 1100, 1]
                .into_iter()
                .enumerate()
                .map(|(id, size)| vec![id as u8 + 1; size])
                .collect();
            for payload in &payloads[..3] {
                left.data.send(payload).await?;
            }
            let (a, b, c) = tokio::join!(
                left.data.send(&payloads[3]),
                left.data.send(&payloads[4]),
                left.data.send(&payloads[5])
            );
            a?;
            b?;
            c?;
            let mut received = Vec::new();
            for _ in 0..payloads
                .iter()
                .map(|payload| fragments_len(payload))
                .sum::<usize>()
            {
                received.push(packets.recv().await.ok_or(MediaError)?);
            }
            assert!(
                received
                    .iter()
                    .all(|packet| packet.header.timestamp == received[0].header.timestamp),
                "serial sends must share one video frame timestamp"
            );
            let picture = &received[0].payload[2..4];
            let mut reassembler = Reassembler::default();
            let mut decoded = Vec::new();
            for (index, packet) in received.iter().enumerate() {
                assert_eq!(&packet.payload[2..4], picture);
                assert_eq!(packet.payload[0] & 0x10 != 0, index == 0);
                assert_eq!(packet.header.marker, index == received.len() - 1);
                if let Some(payload) =
                    reassembler.push(0, carrier.unwrap_fragment(&packet.payload).unwrap())
                {
                    decoded.push(payload);
                }
            }
            decoded.sort();
            payloads.sort();
            assert_eq!(decoded, payloads);

            // Two maximum messages cannot fit one bounded frame; close must flush both in order.
            let closing_payloads = [vec![41; MAX_MEDIA_MESSAGE], vec![42; MAX_MEDIA_MESSAGE]];
            for payload in &closing_payloads {
                left.data.send(payload).await?;
            }
            left.close().await;
            assert!(left.data.send(b"after close").await.is_err());
            let mut frame_bytes = 0;
            let mut frame_count = 0;
            let mut decoded = Vec::new();
            for _ in 0..closing_payloads
                .iter()
                .map(|payload| fragments_len(payload))
                .sum::<usize>()
            {
                let packet = packets.recv().await.ok_or(MediaError)?;
                let fragment = carrier.unwrap_fragment(&packet.payload).unwrap();
                assert_eq!(packet.payload[0] & 0x10 != 0, frame_bytes == 0);
                frame_bytes += fragment.len();
                assert!(frame_bytes <= MAX_VIDEO_FRAME_BYTES);
                if let Some(payload) = reassembler.push(0, fragment) {
                    decoded.push(payload);
                }
                if packet.header.marker {
                    frame_count += 1;
                    frame_bytes = 0;
                }
            }
            assert_eq!(frame_count, 2);
            assert_eq!(frame_bytes, 0);
            assert_eq!(decoded, closing_payloads);
            Ok::<(), MediaError>(())
        })
        .await;
        left.close().await;
        right.close().await;
        result.expect("local batched video timed out").unwrap();
    }

    #[tokio::test]
    async fn video_queue_bounds_backpressure_and_surfaces_writer_failure() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let media = Media::new(&json!([]), tx, MediaCarrier::Vp8Experimental)
            .await
            .unwrap();
        media
            .publish("unbound-track", "unbound-stream")
            .await
            .unwrap();
        let video = media.data.video.as_ref().unwrap();
        let clock = media.data.tx_clock.lock().await;
        tokio::time::timeout(
            Duration::from_secs(1),
            media.data.send(b"first queued packet"),
        )
        .await
        .unwrap()
        .unwrap();
        tokio::time::sleep(VIDEO_FRAME_INTERVAL * 2).await;
        for _ in 0..VIDEO_QUEUE_MESSAGES {
            media.data.send(b"bounded packet").await.unwrap();
        }
        assert_eq!(video.outgoing.capacity(), 0);
        assert!(tokio::time::timeout(
            Duration::from_millis(10),
            media.data.send(b"queue overflow")
        )
        .await
        .is_err());
        drop(clock);
        // The unbound WebRTC track writes zero bytes, which must terminate the queued writer.
        tokio::time::timeout(Duration::from_secs(1), async {
            while !video.closed.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(media.data.send(b"after writer failure").await.is_err());
        media.close().await;
    }

    #[tokio::test]
    async fn dropping_media_stops_video_writer_even_with_an_external_data_plane() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let media = Media::new(&json!([]), tx, MediaCarrier::Vp8Experimental)
            .await
            .unwrap();
        let data = media.data_plane();
        let video = data.video.as_ref().unwrap().clone();
        let peers = [media.publisher.pc.clone(), media.subscriber.pc.clone()];
        drop(media);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !video.task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(video.closed.load(Ordering::SeqCst));
        assert!(data.send(b"after media drop").await.is_err());
        for peer in peers {
            peer.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn video_close_aborts_a_blocked_flush_within_its_deadline() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let media = Media::new(&json!([]), tx, MediaCarrier::Vp8Experimental)
            .await
            .unwrap();
        media
            .publish("blocked-track", "blocked-stream")
            .await
            .unwrap();
        let clock = media.data.tx_clock.lock().await;
        media.data.send(b"queued before close").await.unwrap();
        tokio::time::sleep(VIDEO_FRAME_INTERVAL * 2).await;
        let video = media.data.video.as_ref().unwrap();
        tokio::time::timeout(VIDEO_CLOSE_TIMEOUT + Duration::from_secs(1), video.close())
            .await
            .unwrap();
        assert!(media.data.send(b"after closed flush").await.is_err());
        drop(clock);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !video.task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        media.close().await;
    }

    #[test]
    fn video_reassembly_rejects_malformed_inner_message_metadata() {
        let carrier = MediaCarrier::Vp8Experimental;
        let original = encode_rtp_fragments(100, 1, b"opaque bytes")
            .unwrap()
            .remove(0);
        for offset in [4, 17, 19, 21] {
            let mut fragment = original.clone();
            fragment[offset] = 0xff;
            let wrapped = carrier.wrap_fragment(&fragment, true, fragment.len(), 1);
            let mut reassembler = Reassembler::default();
            let recovered = carrier
                .unwrap_fragment(&wrapped)
                .and_then(|fragment| reassembler.push(0, fragment));
            assert!(recovered.is_none());
        }
    }

    #[tokio::test]
    async fn optional_carriers_preserve_datagrams_over_real_local_webrtc() {
        for carrier in [MediaCarrier::AudioWallclock, MediaCarrier::Vp8Experimental] {
            let (left_tx, mut left_ice) = mpsc::unbounded_channel();
            let (right_tx, mut right_ice) = mpsc::unbounded_channel();
            let left = Media::new(&json!([]), left_tx, carrier).await.unwrap();
            let right = Media::new(&json!([]), right_tx, carrier).await.unwrap();
            let result=tokio::time::timeout(Duration::from_secs(10),async {
                let offer=left.publish("left-track","left-stream").await?;
                if carrier.is_video() { assert!(offer["description"]["sdp"].as_str().unwrap().contains("m=video")); }
                let answer=right.description(&offer).await?.unwrap();
                left.description(&answer).await?;
                let offer=right.publish("right-track","right-stream").await?;
                let answer=left.description(&offer).await?.unwrap();
                right.description(&answer).await?;
                let ready=async {
                    left.data.wait_ready(Duration::from_secs(10)).await?;
                    right.data.wait_ready(Duration::from_secs(10)).await
                };
                tokio::pin!(ready);
                loop {
                    tokio::select! {
                        result=&mut ready => { result?; break; }
                        event=left_ice.recv() => {
                            let mut event=event.ok_or(MediaError)?;
                            for candidate in event["rtcIceCandidates"].as_array_mut().unwrap() {
                                candidate["target"]=if candidate["target"]=="PUBLISHER" {json!("SUBSCRIBER")}else{json!("PUBLISHER")};
                            }
                            right.candidates(event["rtcIceCandidates"].as_array().unwrap()).await?;
                        }
                        event=right_ice.recv() => {
                            let mut event=event.ok_or(MediaError)?;
                            for candidate in event["rtcIceCandidates"].as_array_mut().unwrap() {
                                candidate["target"]=if candidate["target"]=="PUBLISHER" {json!("SUBSCRIBER")}else{json!("PUBLISHER")};
                            }
                            left.candidates(event["rtcIceCandidates"].as_array().unwrap()).await?;
                        }
                    }
                }
                for size in [1,1000,1100,2501,MAX_MEDIA_MESSAGE] {
                    let payload:Vec<_>=(0..size).map(|i|(i%251) as u8).collect();
                    left.data.send(&payload).await?;
                    assert_eq!(right.data.recv().await?,payload,"local forward datagram changed");
                    right.data.send(&payload).await?;
                    assert_eq!(left.data.recv().await?,payload,"local reverse datagram changed");
                }
                let snapshot = left.snapshot().await;
                assert_eq!(snapshot["native_receive"]["completed_messages"], 5,
                    "native snapshot must count recovered datagrams");
                assert_eq!(snapshot["native_receive"]["completed_bytes"], 37_370);
                assert_eq!(snapshot["native_receive"]["unwrap_rejects"], 0);
                Ok::<(),MediaError>(())
            }).await;
            left.close().await;
            right.close().await;
            result.expect("local WebRTC carrier timed out").unwrap();
        }
    }

    #[test]
    fn candidate_diagnostics_never_reveal_addresses_or_untrusted_labels() {
        for (address, class) in [
            ("8.8.8.8", "public"),
            ("172.17.250.101", "private_or_reserved"),
            ("100.64.0.1", "private_or_reserved"),
            ("secret-address", "unparsed"),
        ] {
            let summary = ice_candidate_summary(address, 3478, "secret-kind");
            assert_eq!(summary["ip_class"], class);
            assert_eq!(summary["candidate_type"], "unknown");
            assert_eq!(summary["port"], 3478);
            assert!(!summary.to_string().contains(address));
            assert!(!summary.to_string().contains("secret"));
        }
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
        assert_eq!(
            snapshot["publisher"]["remote_description"]["type"],
            "answer"
        );
        assert_eq!(snapshot["publisher"]["remote_ice_candidates"][0], candidate);
        assert!(snapshot["publisher"]["ice_state"].is_string());
        assert!(snapshot["publisher"]["gathering_state"].is_string());
        assert!(snapshot["publisher"]["ice_diagnostics"]["nominated_pairs"].is_array());
        publisher.close().await;
        subscriber.close().await;
    }
}
