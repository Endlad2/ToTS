use crate::{Error, Result};
use bytes::{BufMut, BytesMut};
use crc32fast::Hasher as Crc32;
use hmac::{Hmac, Mac};
use md5::{Digest as _, Md5};
use rand::{rngs::OsRng, RngCore};
use sha1::Sha1;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

pub const MAGIC_COOKIE: u32 = 0x2112_A442;
pub const ATTR_USERNAME: u16 = 0x0006;
pub const ATTR_MESSAGE_INTEGRITY: u16 = 0x0008;
pub const ATTR_ERROR_CODE: u16 = 0x0009;
pub const ATTR_CHANNEL_NUMBER: u16 = 0x000C;
pub const ATTR_LIFETIME: u16 = 0x000D;
pub const ATTR_XOR_PEER_ADDRESS: u16 = 0x0012;
pub const ATTR_REALM: u16 = 0x0014;
pub const ATTR_NONCE: u16 = 0x0015;
pub const ATTR_XOR_RELAYED_ADDRESS: u16 = 0x0016;
pub const ATTR_REQUESTED_TRANSPORT: u16 = 0x0019;
pub const ATTR_FINGERPRINT: u16 = 0x8028;
pub const ALLOCATE_REQUEST: u16 = 0x0003;
pub const REFRESH_REQUEST: u16 = 0x0004;
pub const CREATE_PERMISSION_REQUEST: u16 = 0x0008;
pub const CHANNEL_BIND_REQUEST: u16 = 0x0009;

#[derive(Clone, Debug)]
pub struct Attr {
    pub kind: u16,
    pub value: Vec<u8>,
}
#[derive(Clone, Debug)]
pub struct Message {
    pub ty: u16,
    pub tx: [u8; 12],
    pub attrs: Vec<Attr>,
}
impl Message {
    pub fn attr(&self, k: u16) -> Option<&[u8]> {
        self.attrs
            .iter()
            .find(|a| a.kind == k)
            .map(|a| a.value.as_slice())
    }
}

pub fn transaction() -> [u8; 12] {
    let mut x = [0u8; 12];
    OsRng.fill_bytes(&mut x);
    x
}
pub fn attr_text(kind: u16, value: &str) -> Attr {
    Attr {
        kind,
        value: value.as_bytes().to_vec(),
    }
}
pub fn attr_u32(kind: u16, value: u32) -> Attr {
    Attr {
        kind,
        value: value.to_be_bytes().to_vec(),
    }
}
pub fn long_term_key(username: &str, realm: &str, password: &str) -> [u8; 16] {
    let mut d = Md5::new();
    d.update(username);
    d.update(b":");
    d.update(realm);
    d.update(b":");
    d.update(password);
    d.finalize().into()
}

fn push_attr(out: &mut Vec<u8>, a: &Attr) {
    out.extend_from_slice(&a.kind.to_be_bytes());
    out.extend_from_slice(&(a.value.len() as u16).to_be_bytes());
    out.extend_from_slice(&a.value);
    while !out.len().is_multiple_of(4) {
        out.push(0);
    }
}
fn set_len(out: &mut [u8]) {
    let n = (out.len() - 20) as u16;
    out[2..4].copy_from_slice(&n.to_be_bytes());
}

pub fn encode(ty: u16, tx: [u8; 12], attrs: &[Attr], key: Option<&[u8; 16]>) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(256);
    out.extend_from_slice(&ty.to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
    out.extend_from_slice(&tx);
    for a in attrs {
        push_attr(&mut out, a);
    }
    if let Some(key) = key {
        let mi_final = out.len() + 24;
        let len = (mi_final - 20) as u16;
        out[2..4].copy_from_slice(&len.to_be_bytes());
        let mut mac =
            Hmac::<Sha1>::new_from_slice(key).map_err(|_| Error::Packet("HMAC key".into()))?;
        mac.update(&out);
        let tag = mac.finalize().into_bytes();
        push_attr(
            &mut out,
            &Attr {
                kind: ATTR_MESSAGE_INTEGRITY,
                value: tag.to_vec(),
            },
        );
    }
    let fp_final = out.len() + 8;
    let len = (fp_final - 20) as u16;
    out[2..4].copy_from_slice(&len.to_be_bytes());
    let mut c = Crc32::new();
    c.update(&out);
    let fp = c.finalize() ^ 0x5354_554e;
    push_attr(&mut out, &attr_u32(ATTR_FINGERPRINT, fp));
    set_len(&mut out);
    Ok(out)
}

pub fn decode(data: &[u8]) -> Result<Message> {
    if data.len() < 20 {
        return Err(Error::Packet("short STUN".into()));
    }
    let ty = u16::from_be_bytes([data[0], data[1]]);
    let len = u16::from_be_bytes([data[2], data[3]]) as usize;
    if data.len() < 20 + len || u32::from_be_bytes(data[4..8].try_into().unwrap()) != MAGIC_COOKIE {
        return Err(Error::Packet("bad STUN header".into()));
    }
    let mut tx = [0u8; 12];
    tx.copy_from_slice(&data[8..20]);
    let mut attrs = Vec::new();
    let mut p = 20;
    let end = 20 + len;
    while p + 4 <= end {
        let k = u16::from_be_bytes([data[p], data[p + 1]]);
        let n = u16::from_be_bytes([data[p + 2], data[p + 3]]) as usize;
        p += 4;
        if p + n > end {
            return Err(Error::Packet("truncated STUN attr".into()));
        }
        attrs.push(Attr {
            kind: k,
            value: data[p..p + n].to_vec(),
        });
        p += n;
        p = (p + 3) & !3;
    }
    Ok(Message { ty, tx, attrs })
}

pub fn xor_addr_attr(kind: u16, addr: SocketAddr, tx: [u8; 12]) -> Attr {
    let mut v = BytesMut::new();
    v.put_u8(0);
    match addr.ip() {
        IpAddr::V4(ip) => {
            v.put_u8(1);
            v.put_u16(addr.port() ^ ((MAGIC_COOKIE >> 16) as u16));
            let raw = ip.octets();
            let cookie = MAGIC_COOKIE.to_be_bytes();
            for i in 0..4 {
                v.put_u8(raw[i] ^ cookie[i]);
            }
        }
        IpAddr::V6(ip) => {
            v.put_u8(2);
            v.put_u16(addr.port() ^ ((MAGIC_COOKIE >> 16) as u16));
            let raw = ip.octets();
            let mut mask = [0u8; 16];
            mask[..4].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
            mask[4..].copy_from_slice(&tx);
            for i in 0..16 {
                v.put_u8(raw[i] ^ mask[i]);
            }
        }
    }
    Attr {
        kind,
        value: v.to_vec(),
    }
}

pub fn parse_xor_addr(value: &[u8], tx: [u8; 12]) -> Result<SocketAddr> {
    if value.len() < 8 {
        return Err(Error::Packet("short XOR address".into()));
    }
    let port = u16::from_be_bytes([value[2], value[3]]) ^ ((MAGIC_COOKIE >> 16) as u16);
    match value[1] {
        1 => {
            let c = MAGIC_COOKIE.to_be_bytes();
            let ip = Ipv4Addr::new(
                value[4] ^ c[0],
                value[5] ^ c[1],
                value[6] ^ c[2],
                value[7] ^ c[3],
            );
            Ok(SocketAddr::new(ip.into(), port))
        }
        2 if value.len() >= 20 => {
            let mut mask = [0u8; 16];
            mask[..4].copy_from_slice(&MAGIC_COOKIE.to_be_bytes());
            mask[4..].copy_from_slice(&tx);
            let mut raw = [0u8; 16];
            for i in 0..16 {
                raw[i] = value[4 + i] ^ mask[i];
            }
            Ok(SocketAddr::new(Ipv6Addr::from(raw).into(), port))
        }
        _ => Err(Error::Packet("bad XOR address family".into())),
    }
}

pub fn error_code(msg: &Message) -> Option<u16> {
    let v = msg.attr(ATTR_ERROR_CODE)?;
    if v.len() < 4 {
        return None;
    }
    Some((v[2] as u16 & 0x7) * 100 + v[3] as u16)
}
