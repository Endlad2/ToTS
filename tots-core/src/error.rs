#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid config: {0}")]
    Config(String),
    #[error("invalid packet: {0}")]
    Packet(String),
    #[error("crypto error")]
    Crypto,
    #[error("replayed or stale packet")]
    Replay,
    #[error("provider error: {0}")]
    Provider(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
