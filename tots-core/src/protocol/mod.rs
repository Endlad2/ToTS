pub mod frame;
pub mod wire;

pub use frame::{AckRange, Frame, MAX_ACK_RANGES};
pub use wire::{open_datagram, peek_meta, seal_datagram, EnvelopeMeta, ReplayWindow};
pub(crate) use wire::{seal_encoded_datagram, SEALED_ENVELOPE_OVERHEAD};
