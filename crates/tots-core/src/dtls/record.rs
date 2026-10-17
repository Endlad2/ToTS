//! Формирование и разбор DTLS-записей.
//!
//! Упрощённая реализация для внутреннего использования: только
//! ApplicationData-записи с AEAD-шифрованием.

use std::io;

/// DTLS 1.2 версия в заголовке записи.
pub const DTLS_VERSION: [u8; 2] = [0xFE, 0xFD];

pub const CONTENT_TYPE_CHANGE_CIPHER_SPEC: u8 = 20;
pub const CONTENT_TYPE_ALERT: u8 = 21;
pub const CONTENT_TYPE_HANDSHAKE: u8 = 22;
pub const CONTENT_TYPE_APPLICATION_DATA: u8 = 23;

pub const HEADER_LEN: usize = 13;

/// Заголовок DTLS-записи.
#[derive(Debug, Clone, Copy)]
pub struct RecordHeader {
    pub content_type: u8,
    pub version: [u8; 2],
    pub epoch: u16,
    pub sequence: [u8; 6],
    pub length: u16,
}

impl RecordHeader {
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut buf = [0u8; HEADER_LEN];
        buf[0] = self.content_type;
        buf[1..3].copy_from_slice(&self.version);
        buf[3..5].copy_from_slice(&self.epoch.to_be_bytes());
        buf[5..11].copy_from_slice(&self.sequence);
        buf[11..13].copy_from_slice(&self.length.to_be_bytes());
        buf
    }

    pub fn decode(buf: &[u8]) -> io::Result<Self> {
        if buf.len() < HEADER_LEN {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short record"));
        }
        Ok(Self {
            content_type: buf[0],
            version: [buf[1], buf[2]],
            epoch: u16::from_be_bytes([buf[3], buf[4]]),
            sequence: [buf[5], buf[6], buf[7], buf[8], buf[9], buf[10]],
            length: u16::from_be_bytes([buf[11], buf[12]]),
        })
    }
}

/// Собирает ApplicationData-запись с уже готовым (зашифрованным) payload.
pub fn wrap_application_data(epoch: u16, sequence: u64, payload: &[u8]) -> Vec<u8> {
    let seq_bytes = {
        let mut b = [0u8; 6];
        b[0] = ((sequence >> 40) & 0xFF) as u8;
        b[1] = ((sequence >> 32) & 0xFF) as u8;
        b[2] = ((sequence >> 24) & 0xFF) as u8;
        b[3] = ((sequence >> 16) & 0xFF) as u8;
        b[4] = ((sequence >> 8) & 0xFF) as u8;
        b[5] = (sequence & 0xFF) as u8;
        b
    };
    let header = RecordHeader {
        content_type: CONTENT_TYPE_APPLICATION_DATA,
        version: DTLS_VERSION,
        epoch,
        sequence: seq_bytes,
        length: payload.len() as u16,
    };
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&header.encode());
    out.extend_from_slice(payload);
    out
}
