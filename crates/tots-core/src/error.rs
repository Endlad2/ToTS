use thiserror::Error;

/// Convenience alias used across the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// All failures that ToTS can produce.
#[derive(Debug, Error)]
pub enum Error {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("unsupported transport: {0}")]
    UnsupportedTransport(String),

    #[error("crypto error: {0}")]
    Crypto(String),

    #[error("transport error ({transport}): {source}")]
    Transport {
        transport: &'static str,
        #[source]
        source: anyhow::Error,
    },

    #[error("tunnel error: {0}")]
    Tunnel(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Error {
    /// Wrap any error coming from a concrete transport implementation.
    pub fn transport(transport: &'static str, source: impl Into<anyhow::Error>) -> Self {
        Error::Transport {
            transport,
            source: source.into(),
        }
    }
}