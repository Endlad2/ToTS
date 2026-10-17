//! KCP + smux поверх произвольного `AsyncRead + AsyncWrite`.

use std::io;

use kcp::Kcp;
use smux::{Config, Session};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Создаёт клиентскую smux-сессию поверх KCP.
pub async fn client<S>(stream: S, config: Config) -> io::Result<Session>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin + Send + 'static,
{
    let kcp = Kcp::new(stream);
    smux::client(kcp, config).await
}

/// Создаёт серверную smux-сессию поверх KCP.
pub async fn server<S>(stream: S, config: Config) -> io::Result<Session>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin + Send + 'static,
{
    let kcp = Kcp::new(stream);
    smux::server(kcp, config).await
}

/// Параметры smux.
pub fn default_smux_config() -> Config {
    let mut cfg = Config::default();
    cfg.max_stream_buffer = 1024 * 1024;
    cfg.keep_alive_interval = Some(std::time::Duration::from_secs(10));
    cfg.keep_alive_timeout = std::time::Duration::from_secs(30);
    cfg
}
