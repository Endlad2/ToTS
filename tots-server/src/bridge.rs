use anyhow::Result;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::task::JoinHandle;
use tots_core::transport::{socket::bind_udp, DatagramPath};

/// Keep provider media and the existing authenticated UDP/TUN dataplane joined.
///
/// The bridge used to exit on the first transport error and stay in the room map
/// forever: the config still existed, the client could join the room, and every
/// ToTS datagram vanished. Errors are now retried, and the owning task exposes
/// whether the bridge is finished so the API can rebuild or drop it.
pub struct NativeBridge {
    path: Arc<dyn DatagramPath>,
    task: JoinHandle<()>,
}

impl NativeBridge {
    pub async fn start(path: Arc<dyn DatagramPath>, mut target: SocketAddr) -> Result<Self> {
        if target.ip().is_unspecified() {
            target.set_ip(if target.is_ipv4() {
                std::net::Ipv4Addr::LOCALHOST.into()
            } else {
                std::net::Ipv6Addr::LOCALHOST.into()
            });
        }
        let socket = bind_udp(if target.is_ipv4() {
            "0.0.0.0:0".parse()?
        } else {
            "[::]:0".parse()?
        })?;
        socket.connect(target).await?;
        let transport = path.clone();
        let task = tokio::spawn(async move {
            let mut from_provider = vec![0; 65535];
            let mut from_server = vec![0; 65535];
            let mut consecutive_errors = 0u32;
            loop {
                let step: Result<()> = async {
                    tokio::select! {
                        n = transport.recv(&mut from_provider) => {
                            socket.send(&from_provider[..n?]).await?;
                        }
                        n = socket.recv(&mut from_server) => {
                            transport.send(&from_server[..n?]).await?;
                        }
                    }
                    Ok(())
                }
                .await;
                match step {
                    Ok(()) => consecutive_errors = 0,
                    Err(error) => {
                        consecutive_errors = consecutive_errors.saturating_add(1);
                        // A single ICMP port-unreachable (the dataplane may still be
                        // binding) or one media hiccup must not destroy the room.
                        tracing::warn!(%error, consecutive_errors, "native provider bridge step failed; retrying");
                        if consecutive_errors >= 16 {
                            tracing::error!("native provider bridge keeps failing; stopping");
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(
                            100 * u64::from(consecutive_errors.min(10)),
                        ))
                        .await;
                    }
                }
            }
        });
        Ok(Self { path, task })
    }

    /// True when the bridge task has stopped and the room must be rebuilt.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    pub async fn close(self) {
        self.task.abort();
        let _ = self.path.close().await;
    }
}

impl Drop for NativeBridge {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::UdpSocket;
    use tots_core::transport::DirectUdpPath;

    /// Path whose first receives fail, like a transient media/ICMP error.
    struct FlakyPath {
        socket: tokio::net::UdpSocket,
        failures: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl DatagramPath for FlakyPath {
        fn name(&self) -> &str {
            "flaky"
        }

        async fn send(&self, data: &[u8]) -> tots_core::Result<()> {
            self.socket.send(data).await?;
            Ok(())
        }

        async fn recv(&self, buf: &mut [u8]) -> tots_core::Result<usize> {
            if self.failures.fetch_add(1, Ordering::SeqCst) < 2 {
                return Err(tots_core::Error::Transport("transient media error".into()));
            }
            Ok(self.socket.recv(buf).await?)
        }
    }

    #[tokio::test]
    async fn bridge_survives_transient_transport_errors() {
        let ingress = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let provider = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let media = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let media_addr = media.local_addr().unwrap();
        media.connect(provider.local_addr().unwrap()).await.unwrap();
        let path: Arc<dyn DatagramPath> = Arc::new(FlakyPath {
            socket: media,
            failures: AtomicUsize::new(0),
        });
        let bridge = NativeBridge::start(path, ingress.local_addr().unwrap())
            .await
            .unwrap();
        let mut buffer = [0u8; 128];
        // The path's first receive attempts fail with a transient error; media must
        // still reach the dataplane afterwards.
        provider.send_to(b"encrypted-request", media_addr).await.unwrap();
        let (n, bridge_addr) = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            ingress.recv_from(&mut buffer),
        )
        .await
        .expect("a transient error must not stop the bridge")
        .unwrap();
        assert_eq!(&buffer[..n], b"encrypted-request");
        // And the reverse direction (dataplane -> provider media) keeps working.
        ingress.send_to(b"encrypted-reply", bridge_addr).await.unwrap();
        let (n, _) = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            provider.recv_from(&mut buffer),
        )
        .await
        .expect("the reverse direction must survive too")
        .unwrap();
        assert_eq!(&buffer[..n], b"encrypted-reply");
        assert!(!bridge.is_finished(), "bridge stopped after a retryable error");
        bridge.close().await;
    }

    #[tokio::test]
    async fn bridge_preserves_datagrams_in_both_directions() {
        let ingress = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let path = Arc::new(
            DirectUdpPath::connect("test", remote.local_addr().unwrap())
                .await
                .unwrap(),
        );
        path.send(b"ready").await.unwrap();
        let mut buffer = [0u8; 128];
        let (_, native_addr) = remote.recv_from(&mut buffer).await.unwrap();
        let bridge = NativeBridge::start(path, ingress.local_addr().unwrap())
            .await
            .unwrap();
        remote
            .send_to(b"encrypted-request", native_addr)
            .await
            .unwrap();
        let (n, sender) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            ingress.recv_from(&mut buffer),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&buffer[..n], b"encrypted-request");
        ingress.send_to(b"encrypted-reply", sender).await.unwrap();
        let (n, _) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            remote.recv_from(&mut buffer),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&buffer[..n], b"encrypted-reply");
        assert!(!bridge.is_finished());
        bridge.close().await;
    }
}
