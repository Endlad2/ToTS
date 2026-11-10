use crate::{
    session::{DecodedPacket, SessionEngine},
    transport::DatagramPath,
    Error, Result,
};
use rand::{rngs::OsRng, RngCore};
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug)]
pub struct ProbeResult {
    pub path: String,
    pub rtt: Duration,
}

fn micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}
pub async fn probe(
    engine: &SessionEngine,
    path: Arc<dyn DatagramPath>,
    timeout: Duration,
) -> Result<ProbeResult> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut retry = tokio::time::interval(Duration::from_millis(500));
    retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut pending = std::collections::VecDeque::new();
    let mut buf = vec![0u8; 2048];
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                return Err(Error::Transport(format!("{} probe timeout", path.name())));
            }
            _ = retry.tick() => {
                let token = OsRng.next_u64();
                if pending.len() == 64 { pending.pop_front(); }
                pending.push_back((token, Instant::now()));
                // Fresh sequence numbers allow a retry through the replay-protected server.
                let packet = engine.make_probe(token, micros())?;
                tokio::time::timeout_at(deadline, path.send(&packet)).await
                    .map_err(|_| Error::Transport(format!("{} probe timeout", path.name())))??;
            }
            received = path.recv(&mut buf) => {
                let n = received?;
                if let Ok(Some(DecodedPacket::ProbeAck { token, .. })) = engine.decapsulate(&buf[..n]) {
                    if let Some((_, sent)) = pending.iter().find(|(id, _)| *id == token) {
                        return Ok(ProbeResult { path: path.name().to_owned(), rtt: sent.elapsed() });
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{mpsc, Mutex};

    struct DropFirst {
        peer: SessionEngine,
        sent: AtomicUsize,
        outgoing: mpsc::Sender<Vec<u8>>,
        incoming: Mutex<mpsc::Receiver<Vec<u8>>>,
        noise: bool,
    }

    #[async_trait]
    impl DatagramPath for DropFirst {
        fn name(&self) -> &str {
            "lossy-test"
        }
        async fn send(&self, data: &[u8]) -> Result<()> {
            let attempt = self.sent.fetch_add(1, Ordering::SeqCst);
            if self.noise {
                self.outgoing
                    .send(b"invalid packet".to_vec())
                    .await
                    .unwrap();
            } else if attempt > 0 {
                if let Some(DecodedPacket::Probe { token, sent_micros }) =
                    self.peer.decapsulate(data)?
                {
                    self.outgoing
                        .send(self.peer.make_probe_ack(token, sent_micros)?)
                        .await
                        .unwrap();
                }
            }
            Ok(())
        }
        async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
            let data = self.incoming.lock().await.recv().await.unwrap();
            buf[..data.len()].copy_from_slice(&data);
            Ok(data.len())
        }
    }

    fn fixture(noise: bool) -> (SessionEngine, Arc<DropFirst>) {
        let cfg = crate::config::ConnectionConfig {
            version: 1,
            peer: "127.0.0.1".into(),
            webrtc_port: 1,
            turn_port: 2,
            password: "test-password".into(),
            device_id: uuid::Uuid::new_v4(),
            provider: crate::config::ProviderName::Vk,
            provider_data: serde_json::json!({"vk_call_links":["test"]}),
            mtu: 1200,
        };
        let engine = SessionEngine::new(cfg.clone()).unwrap();
        let peer = SessionEngine::with_session(cfg, engine.session_id()).unwrap();
        let (outgoing, incoming) = mpsc::channel(8);
        (
            engine,
            Arc::new(DropFirst {
                peer,
                sent: AtomicUsize::new(0),
                outgoing,
                incoming: Mutex::new(incoming),
                noise,
            }),
        )
    }

    #[tokio::test]
    async fn retries_a_lost_initial_probe_with_a_fresh_sequence() {
        let (engine, path) = fixture(false);
        probe(&engine, path.clone(), Duration::from_secs(2))
            .await
            .unwrap();
        assert!(path.sent.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn malformed_reply_does_not_replace_the_overall_probe_deadline() {
        let (engine, path) = fixture(true);
        let error = probe(&engine, path, Duration::from_millis(100))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Transport(message) if message.contains("probe timeout")));
    }
}
