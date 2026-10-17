//! ToTS core — общие примитивы протокола: TURN, DTLS, обфускация, Client ID.

pub mod clientid;
pub mod dtls;
pub mod framed;
pub mod obf;
pub mod stats;
pub mod turn;

pub use clientid::{read_client_id, write_client_id, ClientIdError, ClientMode};
pub use turn::{TurnAllocation, TurnClient, TurnError};
