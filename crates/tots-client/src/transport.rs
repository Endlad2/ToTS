//! Транспорт до сервера: TURN-релей либо прямое подключение.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tracing::{debug, info};

use tots_core::turn::{TurnAllocation, TurnClient, TurnError};

/// Соединение до сервера.
pub enum Transport {
    Direct(TcpStream),
    Turn(TurnAllocation),
}

impl Transport {
    pub async fn direct(peer: SocketAddr) -> io::Result<Self> {
        debug!(%peer, "transport: direct TCP");
        let stream = TcpStream::connect(peer).await?;
        stream.set_nodelay(true)?;
        Ok(Transport::Direct(stream))
    }

    pub async fn turn(
        server: SocketAddr,
        username: String,
        password: String,
    ) -> Result<Self, TurnError> {
        info!(%server, "transport: TURN allocate");
        let client = TurnClient::connect(server, username, password).await?;
        let alloc = client.allocate(4).await?;
        info!(relayed = %alloc.relayed_addr, "TURN allocation established");
        Ok(Transport::Turn(alloc))
    }
}

/// Общий интерфейс потока.
pub trait IoStream: Send + Unpin {
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>>;

    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;

    fn close<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// Обёртка TcpStream как IoStream.
pub struct TcpIo(pub TcpStream);

impl IoStream for TcpIo {
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
        Box::pin(self.0.read(buf))
    }
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.0.write_all(buf).await?;
            self.0.flush().await
        })
    }
    fn close<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let _ = self.0.shutdown().await;
        })
    }
}

/// IoStream поверх TURN-аллокации.
pub struct TurnIo {
    alloc: TurnAllocation,
    peer: SocketAddr,
}

impl TurnIo {
    pub fn new(alloc: TurnAllocation, peer: SocketAddr) -> Self {
        Self { alloc, peer }
    }
}

impl IoStream for TurnIo {
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
        Box::pin(async move {
            let (_peer, payload) = self
                .alloc
                .recv()
                .await
                .map_err(|e| io::Error::other(e.to_string()))?;
            let n = payload.len().min(buf.len());
            buf[..n].copy_from_slice(&payload[..n]);
            Ok(n)
        })
    }
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.alloc
                .send_channel_data(self.peer, buf)
                .await
                .map_err(|e| io::Error::other(e.to_string()))?;
            Ok(())
        })
    }
    fn close<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {})
    }
}

/// Оборачивает `Box<dyn IoStream>` в tokio-совместимый `AsyncRead + AsyncWrite`.
pub struct TokioIoAdapter {
    inner: Box<dyn IoStream + Send + Unpin>,
}

impl TokioIoAdapter {
    pub fn new(inner: Box<dyn IoStream + Send + Unpin>) -> Self {
        Self { inner }
    }
}

impl AsyncRead for TokioIoAdapter {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let remaining = buf.remaining();
        if remaining == 0 {
            return Poll::Ready(Ok(()));
        }
        // Область жизни `fut` ограничена блоком, чтобы `tmp` освободился до
        // чтения из него.
        let mut tmp = vec![0u8; remaining];
        let poll_result = {
            let mut fut = self.inner.read(&mut tmp);
            fut.as_mut().poll(cx)
        };
        match poll_result {
            Poll::Ready(Ok(n)) => {
                buf.put_slice(&tmp[..n]);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for TokioIoAdapter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let len = buf.len();
        let poll_result = {
            let mut fut = self.inner.write(buf);
            fut.as_mut().poll(cx)
        };
        match poll_result {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(len)),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let poll_result = {
            let mut fut = self.inner.close();
            fut.as_mut().poll(cx)
        };
        match poll_result {
            Poll::Ready(()) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}
