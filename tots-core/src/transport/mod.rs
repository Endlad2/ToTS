mod turn;
mod webrtc;

use std::sync::Arc;

use anyhow::Result;

use crate::ProviderTransport;

pub use turn::TurnTransport;
pub use webrtc::WebRtcTransport;

pub enum Transport {
    WebRtc(WebRtcTransport),
    TurnWebRtc(WebRtcTransport),
    Provider(Arc<dyn ProviderTransport>),
}

impl Transport {
    pub fn name(&self) -> &'static str {
        match self {
            Self::WebRtc(_) => "webrtc",
            Self::TurnWebRtc(_) => "webrtc-over-turn",
            Self::Provider(t) => t.name(),
        }
    }

    pub async fn send(&self, packet: Vec<u8>) -> Result<()> {
        match self {
            Self::WebRtc(t) | Self::TurnWebRtc(t) => t.send(packet).await,
            Self::Provider(t) => t.send(packet).await,
        }
    }

    pub async fn recv(&mut self) -> Result<Vec<u8>> {
        match self {
            Self::WebRtc(t) | Self::TurnWebRtc(t) => t.recv().await,
            Self::Provider(t) => t.recv().await,
        }
    }

    pub async fn close(&self) {
        match self {
            Self::WebRtc(t) | Self::TurnWebRtc(t) => t.close().await,
            Self::Provider(t) => t.close().await,
        }
    }
}
