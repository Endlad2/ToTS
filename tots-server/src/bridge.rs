use anyhow::Result;
use std::{net::SocketAddr, sync::Arc};
use tokio::{net::UdpSocket, task::JoinHandle};
use tots_core::transport::DatagramPath;

/// Keep provider media and the existing authenticated UDP/TUN dataplane joined.
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
        let socket = UdpSocket::bind(if target.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        })
        .await?;
        socket.connect(target).await?;
        let transport = path.clone();
        let task = tokio::spawn(async move {
            let result: Result<()> = async {
                let mut from_provider = vec![0; 65535];
                let mut from_server = vec![0; 65535];
                loop {
                    tokio::select! {
                        n = transport.recv(&mut from_provider) => {
                            socket.send(&from_provider[..n?]).await?;
                        }
                        n = socket.recv(&mut from_server) => {
                            transport.send(&from_server[..n?]).await?;
                        }
                    }
                }
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(%error, "native provider bridge stopped");
            }
        });
        Ok(Self { path, task })
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
    use tots_core::transport::DirectUdpPath;

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
        bridge.close().await;
    }
}
