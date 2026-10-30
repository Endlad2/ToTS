use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelInit};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCConfiguration,
    RTCConfigurationBuilder, RTCIceGatheringState, RTCIceServer, RTCIceTransportPolicy,
    RTCSessionDescription,
};

use crate::{Frame, PacketCipher, TurnCredentials};
use super::TurnTransport;

#[derive(Clone)]
struct Handler {
    gather_tx: mpsc::Sender<()>,
}

#[async_trait]
impl PeerConnectionEventHandler for Handler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_tx.try_send(());
        }
    }
}

pub struct WebRtcTransport {
    pc: Arc<dyn PeerConnection>,
    dc: Arc<dyn DataChannel>,
    incoming: mpsc::Receiver<Vec<u8>>,
}

struct PreparedPeer {
    pc: Arc<dyn PeerConnection>,
    dc: Arc<dyn DataChannel>,
    incoming: mpsc::Receiver<Vec<u8>>,
    open_rx: oneshot::Receiver<()>,
    gather_rx: mpsc::Receiver<()>,
}

impl WebRtcTransport {
    pub async fn connect(server: &str, port: u16, connect_timeout: Duration) -> Result<Self> {
        let config = RTCConfigurationBuilder::default().build();
        let mut prepared = prepare_peer(config).await?;
        let local = make_local_offer(&prepared.pc, &mut prepared.gather_rx, connect_timeout).await?;

        let url_host = match server.parse::<IpAddr>() {
            Ok(IpAddr::V6(_)) => format!("[{server}]"),
            _ => server.to_string(),
        };
        let url = format!("http://{url_host}:{port}/webrtc/offer");
        let answer: RTCSessionDescription = timeout(
            connect_timeout,
            reqwest::Client::new().post(url).json(&local).send(),
        )
        .await
        .context("WebRTC signaling timeout")??
        .error_for_status()
        .context("WebRTC signaling HTTP error")?
        .json()
        .await
        .context("decode WebRTC answer")?;

        finish_peer(prepared, answer, connect_timeout).await
    }

    /// Builds a genuine WebRTC DataChannel whose ICE candidates are forced to
    /// the provider TURN servers. SDP signaling itself is carried over a small
    /// temporary raw TURN allocation to the server's `turn_port`, so the
    /// direct HTTP signaling path is not required for fallback mode.
    pub async fn connect_via_turn(
        creds: &TurnCredentials,
        signaling: &mut TurnTransport,
        cipher: &PacketCipher,
        connect_timeout: Duration,
    ) -> Result<Self> {
        let urls: Vec<String> = creds
            .servers
            .iter()
            .map(|s| {
                if s.host.contains(':') {
                    format!("turn:[{}]:{}?transport=udp", s.host, s.port)
                } else {
                    format!("turn:{}:{}?transport=udp", s.host, s.port)
                }
            })
            .collect();
        if urls.is_empty() {
            return Err(anyhow!("provider returned no TURN servers for WebRTC ICE"));
        }

        let config = RTCConfigurationBuilder::default()
            .with_ice_servers(vec![RTCIceServer {
                urls,
                username: creds.username.clone(),
                credential: creds.password.clone(),
            }])
            .with_ice_transport_policy(RTCIceTransportPolicy::Relay)
            .build();

        let mut prepared = prepare_peer(config).await?;
        let local = make_local_offer(&prepared.pc, &mut prepared.gather_rx, connect_timeout).await?;
        let sdp_json = serde_json::to_string(&local).context("encode relay WebRTC offer")?;
        signaling
            .send(cipher.seal(&Frame::SignalOffer { sdp_json })?)
            .await?;

        let answer_packet = timeout(connect_timeout, signaling.recv())
            .await
            .context("TURN-carried WebRTC signaling timeout")??;
        let answer_json = match cipher.open(&answer_packet)? {
            Frame::SignalAnswer { sdp_json } => sdp_json,
            other => return Err(anyhow!("unexpected TURN signaling reply: {other:?}")),
        };
        let answer: RTCSessionDescription =
            serde_json::from_str(&answer_json).context("decode relay WebRTC answer")?;

        let transport = finish_peer(prepared, answer, connect_timeout).await?;
        signaling.close().await;
        Ok(transport)
    }

    pub async fn send(&self, packet: Vec<u8>) -> Result<()> {
        self.dc
            .send(BytesMut::from(packet.as_slice()))
            .await
            .context("send WebRTC DataChannel packet")
    }

    pub async fn recv(&mut self) -> Result<Vec<u8>> {
        self.incoming
            .recv()
            .await
            .ok_or_else(|| anyhow!("WebRTC DataChannel closed"))
    }

    pub async fn close(&self) {
        let _ = self.dc.close().await;
        let _ = self.pc.close().await;
    }
}

async fn prepare_peer(config: RTCConfiguration) -> Result<PreparedPeer> {
    let (gather_tx, gather_rx) = mpsc::channel(1);
    let handler = Arc::new(Handler { gather_tx });
    let pc: Arc<dyn PeerConnection> = Arc::new(
        PeerConnectionBuilder::new()
            .with_configuration(config)
            .with_handler(handler)
            .with_udp_addrs(vec!["0.0.0.0:0".to_string()])
            .build()
            .await
            .context("build WebRTC peer connection")?,
    );

    let dc = pc
        .create_data_channel(
            "tots",
            Some(RTCDataChannelInit {
                ordered: false,
                max_retransmits: Some(3),
                ..Default::default()
            }),
        )
        .await
        .context("create ToTS data channel")?;

    let (incoming_tx, incoming) = mpsc::channel(256);
    let (open_tx, open_rx) = oneshot::channel::<()>();
    let poll_dc = dc.clone();
    tokio::spawn(async move {
        let mut open_tx = Some(open_tx);
        while let Some(event) = poll_dc.poll().await {
            match event {
                DataChannelEvent::OnOpen => {
                    if let Some(tx) = open_tx.take() {
                        let _ = tx.send(());
                    }
                }
                DataChannelEvent::OnMessage(msg) => {
                    if incoming_tx.send(msg.data.to_vec()).await.is_err() {
                        break;
                    }
                }
                DataChannelEvent::OnClose => break,
                _ => {}
            }
        }
    });

    Ok(PreparedPeer {
        pc,
        dc,
        incoming,
        open_rx,
        gather_rx,
    })
}

async fn make_local_offer(
    pc: &Arc<dyn PeerConnection>,
    gather_rx: &mut mpsc::Receiver<()>,
    connect_timeout: Duration,
) -> Result<RTCSessionDescription> {
    let offer = pc.create_offer(None).await.context("create WebRTC offer")?;
    pc.set_local_description(offer)
        .await
        .context("set local WebRTC description")?;
    timeout(connect_timeout, gather_rx.recv())
        .await
        .context("WebRTC ICE gathering timeout")?
        .ok_or_else(|| anyhow!("WebRTC ICE gathering channel closed"))?;
    pc.local_description()
        .await
        .ok_or_else(|| anyhow!("missing WebRTC local description"))
}

async fn finish_peer(
    prepared: PreparedPeer,
    answer: RTCSessionDescription,
    connect_timeout: Duration,
) -> Result<WebRtcTransport> {
    prepared
        .pc
        .set_remote_description(answer)
        .await
        .context("set remote WebRTC description")?;
    timeout(connect_timeout, prepared.open_rx)
        .await
        .context("WebRTC DataChannel open timeout")?
        .map_err(|_| anyhow!("WebRTC DataChannel task exited before open"))?;

    Ok(WebRtcTransport {
        pc: prepared.pc,
        dc: prepared.dc,
        incoming: prepared.incoming,
    })
}
