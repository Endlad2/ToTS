//! Проверка/отправка Client ID — первая запись после DTLS-handshake.

use std::io;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use tracing::{debug, warn};

/// Режим работы клиента (wire-тег).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ClientMode {
    Unset = 0,
    Udp = 1,
    Tcp = 2,
    TcpBond = 3,
}

impl ClientMode {
    pub fn from_byte(b: u8) -> Self {
        match b {
            1 => ClientMode::Udp,
            2 => ClientMode::Tcp,
            3 => ClientMode::TcpBond,
            _ => ClientMode::Unset,
        }
    }
}

const VERSION_ACK: u8 = 2;
const ID_ACK: u8 = 0x06;
const RETRANSMIT: [Duration; 3] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
];

#[derive(Debug, thiserror::Error)]
pub enum ClientIdError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("server did not acknowledge client ID (server outdated?)")]
    NoAck,
}

fn build_record(client_id: &str, mode: ClientMode) -> Vec<u8> {
    let bytes = client_id.as_bytes();
    let len = bytes.len().min(255);
    let mut buf = Vec::with_capacity(len + 3);
    buf.push(len as u8);
    buf.extend_from_slice(&bytes[..len]);
    buf.push(mode as u8);
    buf.push(VERSION_ACK);
    buf
}

/// Отправляет Client ID и ждёт подтверждения (retransmit по таймеру).
pub async fn write_client_id<W, R>(
    mut writer: W,
    mut reader: R,
    client_id: &str,
    mode: ClientMode,
) -> Result<(), ClientIdError>
where
    W: AsyncWriteExt + Unpin,
    R: AsyncReadExt + Unpin,
{
    let rec = build_record(client_id, mode);
    let mut ack_buf = [0u8; 1];

    for wait in RETRANSMIT {
        writer.write_all(&rec).await?;
        writer.flush().await?;
        match timeout(wait, reader.read_exact(&mut ack_buf)).await {
            Ok(Ok(_)) if ack_buf[0] == ID_ACK => {
                debug!("client ID acknowledged");
                return Ok(());
            }
            Ok(Ok(_)) => {
                warn!("unexpected ack byte: {:02x}", ack_buf[0]);
            }
            Ok(Err(e)) => return Err(ClientIdError::Io(e)),
            Err(_) => {
                debug!("client ID ack timeout, retransmitting");
            }
        }
    }
    Err(ClientIdError::NoAck)
}

/// Результат чтения записи Client ID на сервере.
#[derive(Debug, Clone)]
pub struct IncomingClientId {
    pub client_id: String,
    pub mode: ClientMode,
    pub version: u8,
}

/// Читает запись Client ID, отправляет ACK при version >= 2.
pub async fn read_client_id<R, W>(
    mut reader: R,
    mut writer: W,
    read_timeout: Duration,
) -> Result<IncomingClientId, ClientIdError>
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut len_buf = [0u8; 1];
    timeout(read_timeout, reader.read_exact(&mut len_buf))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "client ID read timeout"))??;

    let len = len_buf[0] as usize;
    let mut id_buf = vec![0u8; len];
    reader.read_exact(&mut id_buf).await?;

    let mut tail = [0u8; 2];
    let tail_read = timeout(Duration::from_secs(2), reader.read_exact(&mut tail)).await;
    let (mode, version) = match tail_read {
        Ok(Ok(_)) => (ClientMode::from_byte(tail[0]), tail[1]),
        _ => (ClientMode::Unset, 0),
    };

    let client_id = String::from_utf8_lossy(&id_buf).to_string();
    debug!(client_id, ?mode, version, "client ID read");

    if version >= VERSION_ACK {
        writer.write_all(&[ID_ACK]).await?;
        writer.flush().await?;
    }

    Ok(IncomingClientId {
        client_id,
        mode,
        version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn roundtrip() {
        let (a, b) = duplex(1024);
        let (ar, aw) = tokio::io::split(a);
        let (br, bw) = tokio::io::split(b);

        let writer =
            tokio::spawn(
                async move { write_client_id(aw, ar, "test-client", ClientMode::Udp).await },
            );
        let reader =
            tokio::spawn(async move { read_client_id(br, bw, Duration::from_secs(5)).await });

        let w = writer.await.unwrap().unwrap();
        let r = reader.await.unwrap().unwrap();
        let _ = w;
        assert_eq!(r.client_id, "test-client");
        assert_eq!(r.mode, ClientMode::Udp);
        assert_eq!(r.version, VERSION_ACK);
    }
}
