//! STUN/TURN атрибуты.

use std::net::SocketAddr;

use bytes::{Buf, BufMut, BytesMut};

pub const MAGIC_COOKIE: u32 = 0x2112A442;

#[derive(Debug, Clone)]
pub enum Attr {
    Username(String),
    Realm(String),
    Nonce(String),
    MessageIntegrity(Vec<u8>),
    Fingerprint,
    RequestedTransport(u8),
    RequestedAddressFamily(u8),
    Lifetime(u32),
    XorPeerAddress(SocketAddr),
    XorRelayedAddress(SocketAddr),
    XorMappedAddress(SocketAddr),
    ChannelNumber(u16),
    Data(Vec<u8>),
    ErrorCode(u16, String),
    Unknown(u16, Vec<u8>),
}

impl Attr {
    pub fn encode(&self, buf: &mut BytesMut, txid: &[u8; 12]) {
        match self {
            Attr::Username(s) => write_str(buf, 0x0006, s),
            Attr::Realm(s) => write_str(buf, 0x0014, s),
            Attr::Nonce(s) => write_str(buf, 0x0015, s),
            Attr::MessageIntegrity(d) => {
                write_header(buf, 0x0008, 20);
                buf.put_slice(d);
            }
            Attr::Fingerprint => write_header(buf, 0x8028, 4),
            Attr::RequestedTransport(v) => {
                write_header(buf, 0x0019, 4);
                buf.put_u8(*v);
                buf.put_slice(&[0, 0, 0]);
            }
            Attr::RequestedAddressFamily(v) => {
                write_header(buf, 0x0017, 4);
                buf.put_u8(*v);
                buf.put_slice(&[0, 0, 0]);
            }
            Attr::Lifetime(v) => {
                write_header(buf, 0x000D, 4);
                buf.put_u32(*v);
            }
            Attr::XorPeerAddress(a) => write_xor_addr(buf, 0x0012, *a, txid),
            Attr::XorRelayedAddress(a) => write_xor_addr(buf, 0x0016, *a, txid),
            Attr::XorMappedAddress(a) => write_xor_addr(buf, 0x0020, *a, txid),
            Attr::ChannelNumber(v) => {
                write_header(buf, 0x000C, 4);
                buf.put_u16(*v);
                buf.put_u16(0);
            }
            Attr::Data(d) => {
                write_header(buf, 0x0013, d.len());
                buf.put_slice(d);
                write_padding(buf, d.len());
            }
            Attr::ErrorCode(code, reason) => {
                let reason_bytes = reason.as_bytes();
                let len = 4 + reason_bytes.len();
                write_header(buf, 0x0009, len);
                buf.put_u16(0);
                buf.put_u8(((*code / 100) as u8) & 0x07);
                buf.put_u8((*code % 100) as u8);
                buf.put_slice(reason_bytes);
                write_padding(buf, len);
            }
            Attr::Unknown(t, d) => {
                write_header(buf, *t, d.len());
                buf.put_slice(d);
                write_padding(buf, d.len());
            }
        }
    }
}

pub(crate) fn write_header(buf: &mut BytesMut, typ: u16, len: usize) {
    buf.put_u16(typ);
    buf.put_u16(len as u16);
}

pub(crate) fn write_str(buf: &mut BytesMut, typ: u16, s: &str) {
    write_header(buf, typ, s.len());
    buf.put_slice(s.as_bytes());
    write_padding(buf, s.len());
}

pub(crate) fn write_padding(buf: &mut BytesMut, len: usize) {
    let pad = (4 - (len % 4)) % 4;
    for _ in 0..pad {
        buf.put_u8(0);
    }
}

pub(crate) fn write_xor_addr(buf: &mut BytesMut, typ: u16, addr: SocketAddr, txid: &[u8; 12]) {
    write_header(buf, typ, if addr.is_ipv4() { 8 } else { 20 });
    buf.put_u8(0);
    match addr {
        SocketAddr::V4(v4) => {
            buf.put_u8(0x01);
            let port = v4.port() ^ ((MAGIC_COOKIE >> 16) as u16);
            buf.put_u16(port);
            let ip = u32::from(*v4.ip()) ^ MAGIC_COOKIE;
            buf.put_u32(ip);
        }
        SocketAddr::V6(v6) => {
            buf.put_u8(0x02);
            let port = v6.port() ^ ((MAGIC_COOKIE >> 16) as u16);
            buf.put_u16(port);
            let ip_bytes = v6.ip().octets();
            let cookie_bytes = MAGIC_COOKIE.to_be_bytes();
            let mut key = [0u8; 16];
            key[0..4].copy_from_slice(&cookie_bytes);
            key[4..16].copy_from_slice(txid);
            let mut xored = [0u8; 16];
            for i in 0..16 {
                xored[i] = ip_bytes[i] ^ key[i];
            }
            buf.put_slice(&xored);
        }
    }
}

pub(crate) fn decode_attr(
    typ: u16,
    value: &[u8],
    txid: &[u8; 12],
) -> Result<Attr, super::TurnError> {
    Ok(match typ {
        0x0006 => Attr::Username(String::from_utf8_lossy(value).to_string()),
        0x0014 => Attr::Realm(String::from_utf8_lossy(value).to_string()),
        0x0015 => Attr::Nonce(String::from_utf8_lossy(value).to_string()),
        0x0008 => Attr::MessageIntegrity(value.to_vec()),
        0x8028 => Attr::Fingerprint,
        0x0019 => Attr::RequestedTransport(value.first().copied().unwrap_or(0)),
        0x0017 => Attr::RequestedAddressFamily(value.first().copied().unwrap_or(0)),
        0x000D => {
            if value.len() < 4 {
                return Err(super::TurnError::Parse("lifetime too short".into()));
            }
            let mut b = value;
            Attr::Lifetime(b.get_u32())
        }
        0x0012 => Attr::XorPeerAddress(decode_xor_addr(value, txid)?),
        0x0016 => Attr::XorRelayedAddress(decode_xor_addr(value, txid)?),
        0x0020 => Attr::XorMappedAddress(decode_xor_addr(value, txid)?),
        0x000C => {
            if value.len() < 4 {
                return Err(super::TurnError::Parse("channel too short".into()));
            }
            let mut b = value;
            Attr::ChannelNumber(b.get_u16())
        }
        0x0013 => Attr::Data(value.to_vec()),
        0x0009 => {
            if value.len() < 4 {
                return Err(super::TurnError::Parse("error-code too short".into()));
            }
            let class = (value[2] & 0x07) as u16;
            let number = value[3] as u16;
            let code = class * 100 + number;
            Attr::ErrorCode(code, String::from_utf8_lossy(&value[4..]).to_string())
        }
        other => Attr::Unknown(other, value.to_vec()),
    })
}

fn decode_xor_addr(value: &[u8], txid: &[u8; 12]) -> Result<SocketAddr, super::TurnError> {
    if value.len() < 4 {
        return Err(super::TurnError::Parse("xor-addr too short".into()));
    }
    let family = value[1];
    let mut b = &value[2..];
    let port = b.get_u16() ^ ((MAGIC_COOKIE >> 16) as u16);
    match family {
        0x01 => {
            if value.len() < 8 {
                return Err(super::TurnError::Parse("ipv4 addr too short".into()));
            }
            let ip = b.get_u32() ^ MAGIC_COOKIE;
            let ip = std::net::Ipv4Addr::from(ip);
            Ok(SocketAddr::from((ip, port)))
        }
        0x02 => {
            if value.len() < 20 {
                return Err(super::TurnError::Parse("ipv6 addr too short".into()));
            }
            let mut key = [0u8; 16];
            key[0..4].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
            key[4..16].copy_from_slice(txid);
            let mut ip_bytes = [0u8; 16];
            for i in 0..16 {
                ip_bytes[i] = value[4 + i] ^ key[i];
            }
            let ip = std::net::Ipv6Addr::from(ip_bytes);
            Ok(SocketAddr::from((ip, port)))
        }
        _ => Err(super::TurnError::Parse(format!(
            "unknown address family {family}"
        ))),
    }
}
