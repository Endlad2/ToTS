//! Yamux-мультиплексор поверх произвольного `AsyncRead + AsyncWrite`.
//!
//! yamux работает с futures-io трейтами и предоставляет не Stream, а набор
//! poll-методов на `Connection`. Для использования из tokio оборачиваем его
//! в `poll_fn`, а I/O мостим через `tokio_util::compat`.

use std::io;
use std::pin::Pin;

use futures::future::poll_fn;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use tracing::debug;

/// Открывает клиентскую yamux-сессию и первый подпоток.
pub async fn client<S>(stream: S) -> io::Result<tokio_util::compat::Compat<yamux::Stream>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let compat = stream.compat();
    let mut conn = yamux::Connection::new(compat, yamux::Config::default(), yamux::Mode::Client);
    // Ждём первый входящий stream (в Mode::Client это ответ сервера на OpenStream,
    // но чтобы открыть stream, сервер должен принять соединение — для нашего
    // протокола клиент сразу открывает поток).
    let stream = poll_fn(|cx| Pin::new(&mut conn).poll_next_inbound(cx))
        .await
        .ok_or_else(|| io::Error::other("yamux: connection closed before stream"))?
        .map_err(|e| io::Error::other(e.to_string()))?;
    debug!("yamux: client stream accepted");
    Ok(stream.compat())
}

/// Принимает входящую yamux-сессию и первый подпоток.
pub async fn server<S>(stream: S) -> io::Result<tokio_util::compat::Compat<yamux::Stream>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let compat = stream.compat();
    let mut conn = yamux::Connection::new(compat, yamux::Config::default(), yamux::Mode::Server);
    let stream = poll_fn(|cx| Pin::new(&mut conn).poll_next_inbound(cx))
        .await
        .ok_or_else(|| io::Error::other("yamux: connection closed before stream"))?
        .map_err(|e| io::Error::other(e.to_string()))?;
    debug!("yamux: server stream accepted");
    Ok(stream.compat())
}

/// Заглушка для трейт-объектов.
pub type BoxStream = Box<dyn AsyncReadWrite + Send + Unpin>;

pub trait AsyncReadWrite: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite> AsyncReadWrite for T {}
