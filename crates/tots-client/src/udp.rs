//! Заглушка UDP-релея; основной релей — в `session.rs`.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use crate::transport::IoStream;

pub async fn run(
    _listen: SocketAddr,
    _stream: Arc<dyn IoStream + Send + Sync>,
) -> anyhow::Result<()> {
    Ok(())
}
