//! STUN/TURN-сообщение.

use bytes::{BufMut, BytesMut};
use hmac::{Hmac, Mac};
use sha1::Sha1;

use super::attr::{decode_attr, Attr, MAGIC_COOKIE};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum Method {
    Allocate = 0x003,
    Refresh = 0x004,
    Send = 0x006,
    Data = 0x007,
    CreatePermission = 0x008,
    ChannelBind = 0x009,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Request = 0,
    Indication = 1,
    SuccessResponse = 2,
    ErrorResponse = 3,
}

#[derive(Debug)]
pub struct Message {
    pub method: Method,
    pub class: Class,
    pub txid: [u8; 12],
    pub attrs: Vec<Attr>,
}

impl Message {
    pub fn new(method: Method, class: Class, txid: [u8; 12]) -> Self {
        Self {
            method,
            class,
            txid,
            attrs: Vec::new(),
        }
    }

    pub fn add(&mut self, attr: Attr) {
        self.attrs.push(attr);
    }

    pub fn find_string(&self, typ: u16) -> Option<String> {
        self.attrs.iter().find_map(|a| match (typ, a) {
            (0x0014, Attr::Realm(s)) => Some(s.clone()),
            (0x0015, Attr::Nonce(s)) => Some(s.clone()),
            (0x0006, Attr::Username(s)) => Some(s.clone()),
            _ => None,
        })
    }

    pub fn find_xor_relayed(&self) -> Option<std::net::SocketAddr> {
        self.attrs.iter().find_map(|a| match a {
            Attr::XorRelayedAddress(a) => Some(*a),
            _ => None,
        })
    }

    pub fn find_xor_mapped(&self) -> Option<std::net::SocketAddr> {
        self.attrs.iter().find_map(|a| match a {
            Attr::XorMappedAddress(a) => Some(*a),
            _ => None,
        })
    }

    pub fn find_xor_peer(&self) -> Option<std::net::SocketAddr> {
        self.attrs.iter().find_map(|a| match a {
            Attr::XorPeerAddress(a) => Some(*a),
            _ => None,
        })
    }

    pub fn find_data(&self) -> Option<Vec<u8>> {
        self.attrs.iter().find_map(|a| match a {
            Attr::Data(d) => Some(d.clone()),
            _ => None,
        })
    }

    pub fn error_code(&self) -> Option<u16> {
        self.attrs.iter().find_map(|a| match a {
            Attr::ErrorCode(c, _) => Some(*c),
            _ => None,
        })
    }

    pub fn error_reason(&self) -> Option<String> {
        self.attrs.iter().find_map(|a| match a {
            Attr::ErrorCode(_, r) => Some(r.clone()),
            _ => None,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(256);
        let msg_type = encode_msg_type(self.method, self.class);
        buf.put_u16(msg_type);
        let len_pos = buf.len();
        buf.put_u16(0);
        buf.put_u32(MAGIC_COOKIE);
        buf.put_slice(&self.txid);

        for attr in &self.attrs {
            attr.encode(&mut buf, &self.txid);
        }

        let len = (buf.len() - 20) as u16;
        buf[len_pos..len_pos + 2].copy_from_slice(&len.to_be_bytes());
        buf.to_vec()
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Self, super::TurnError> {
        if buf.len() < 20 {
            return Err(super::TurnError::Parse("short message".into()));
        }
        let typ = u16::from_be_bytes([buf[0], buf[1]]);
        let len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
        let cookie = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        if cookie != MAGIC_COOKIE {
            return Err(super::TurnError::Parse("bad magic cookie".into()));
        }
        let mut txid = [0u8; 12];
        txid.copy_from_slice(&buf[8..20]);

        let (method, class) = decode_msg_type(typ);

        let mut rest = &buf[20..];
        if rest.len() < len {
            return Err(super::TurnError::Parse("truncated".into()));
        }
        rest = &rest[..len];

        let mut attrs = Vec::new();
        while rest.len() >= 4 {
            let at = u16::from_be_bytes([rest[0], rest[1]]);
            let al = u16::from_be_bytes([rest[2], rest[3]]) as usize;
            if rest.len() < 4 + al {
                break;
            }
            let value = &rest[4..4 + al];
            if let Ok(a) = decode_attr(at, value, &txid) {
                attrs.push(a);
            }
            let pad = (4 - (al % 4)) % 4;
            let advance = 4 + al + pad;
            if rest.len() < advance {
                break;
            }
            rest = &rest[advance..];
        }

        Ok(Message {
            method,
            class,
            txid,
            attrs,
        })
    }

    /// Вычисляет MessageIntegrity (HMAC-SHA1 по RFC 5389).
    ///
    /// Длина сообщения в заголовке должна быть увеличена на 24 байта:
    /// 4 байта заголовка атрибута + 20 байт HMAC-SHA1.
    pub fn compute_integrity(&self, key: &[u8]) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(256);
        let msg_type = encode_msg_type(self.method, self.class);
        buf.put_u16(msg_type);
        let len_pos = buf.len();
        buf.put_u16(0);
        buf.put_u32(MAGIC_COOKIE);
        buf.put_slice(&self.txid);

        for attr in &self.attrs {
            attr.encode(&mut buf, &self.txid);
        }

        // По RFC 5389 §15.4: длина в заголовке = текущая длина + 24
        // (заголовок MESSAGE-INTEGRITY 4 байта + HMAC-SHA1 20 байт).
        let fake_len = (buf.len() - 20 + 24) as u16;
        buf[len_pos..len_pos + 2].copy_from_slice(&fake_len.to_be_bytes());

        let mut mac = Hmac::<Sha1>::new_from_slice(key).expect("hmac");
        mac.update(&buf);
        mac.finalize().into_bytes().to_vec()
    }
}

fn encode_msg_type(method: Method, class: Class) -> u16 {
    let m = method as u16;
    let c = class as u16;
    ((m & 0x0F80) << 2) | ((m & 0x0070) << 1) | (m & 0x000F) | ((c & 0x01) << 4) | ((c & 0x02) << 7)
}

fn decode_msg_type(typ: u16) -> (Method, Class) {
    let method = ((typ >> 2) & 0x0F80) | ((typ >> 1) & 0x0070) | (typ & 0x000F);
    let class = ((typ >> 4) & 0x01) | ((typ >> 7) & 0x02);
    let method = match method {
        0x003 => Method::Allocate,
        0x004 => Method::Refresh,
        0x006 => Method::Send,
        0x007 => Method::Data,
        0x008 => Method::CreatePermission,
        0x009 => Method::ChannelBind,
        _ => Method::Allocate,
    };
    let class = match class {
        0 => Class::Request,
        1 => Class::Indication,
        2 => Class::SuccessResponse,
        _ => Class::ErrorResponse,
    };
    (method, class)
}
