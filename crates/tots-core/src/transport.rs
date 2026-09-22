//! The transport abstraction every cover service implements.

use async_trait::async_trait;

use crate::config::TransportKind;
use crate::error::Result;

/// A frame moving over a cover transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Opaque bytes (already encrypted by the session).
    pub data: Vec<u8>,
    /// Monotonic sequence number, used for ordering and replay protection.
    pub seq: u64,
}

impl Frame {
    pub fn new(data: Vec<u8>, seq: u64) -> Self {
        Self { data, seq }
    }
}

/// Common behaviour of a cover transport.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Which kind this is (for logging and diagnostics).
    fn kind(&self) -> TransportKind;

    /// Establish the underlying session (join a call, open a document, ...).
    async fn connect(&mut self) -> Result<()>;

    /// Send one encrypted frame.
    async fn send(&mut self, frame: &Frame) -> Result<()>;

    /// Receive the next frame, if any.
    async fn recv(&mut self) -> Result<Option<Frame>>;

    /// Tear the session down.
    async fn close(&mut self) -> Result<()>;
}