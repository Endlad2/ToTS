//! RelayConn — единый интерфейс над произвольным асинхронным потоком.

use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct RelayConn<S> {
    inner: S,
}

impl<S> RelayConn<S>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin + Send,
{
    pub fn new(inner: S) -> Self {
        Self { inner }
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf).await
    }

    pub async fn write(&mut self, buf: &[u8]) -> io::Result<()> {
        self.inner.write_all(buf).await?;
        self.inner.flush().await
    }

    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.inner.shutdown().await
    }
}
