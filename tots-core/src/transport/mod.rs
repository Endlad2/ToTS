mod turn;
mod webrtc;

use anyhow::Result;

pub use turn::TurnTransport;
pub use webrtc::WebRtcTransport;

pub enum Transport {
    WebRtc(WebRtcTransport),
    TurnWebRtc(WebRtcTransport),
}

impl Transport {
    pub fn name(&self) -> &'static str {
        match self {
            Self::WebRtc(_) => "webrtc",
            Self::TurnWebRtc(_) => "webrtc-over-turn",
        }
    }

    pub async fn send(&self, packet: Vec<u8>) -> Result<()> {
        match self {
            Self::WebRtc(t) | Self::TurnWebRtc(t) => t.send(packet).await,
        }
    }

    pub async fn recv(&mut self) -> Result<Vec<u8>> {
        match self {
            Self::WebRtc(t) | Self::TurnWebRtc(t) => t.recv().await,
        }
    }

    pub async fn close(&self) {
        match self {
            Self::WebRtc(t) | Self::TurnWebRtc(t) => t.close().await,
        }
    }
}
