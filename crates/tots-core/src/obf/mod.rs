//! Обфускация wire-трафика — мимикрия под RTP/Opus.

pub mod rtpopus;
pub mod rtpopus2;
pub mod rtpopus3;
pub mod shape;

use std::io;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    None,
    Rtpopus,
    Rtpopus2,
    Rtpopus3,
}

impl Profile {
    /// Разбирает имя профиля (без конфликта с `std::str::FromStr`).
    #[allow(clippy::should_implement_trait)]
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "none" => Some(Self::None),
            "rtpopus" => Some(Self::Rtpopus),
            "rtpopus2" => Some(Self::Rtpopus2),
            "rtpopus3" => Some(Self::Rtpopus3),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Rtpopus => "rtpopus",
            Self::Rtpopus2 => "rtpopus2",
            Self::Rtpopus3 => "rtpopus3",
        }
    }
}

/// Кодек обфускации — обёртка с внутренним Mutex.
///
/// `Rtpopus3` заметно крупнее остальных вариантов, поэтому боксован,
/// чтобы размер `Codec` не тянул всю enum на 480 байт.
pub enum Codec {
    Plain,
    Rtpopus(Mutex<rtpopus::Conn>),
    Rtpopus2(Mutex<rtpopus2::Conn>),
    Rtpopus3(Box<Mutex<rtpopus3::Conn>>),
}

impl Codec {
    pub fn client(profile: Profile, key: Option<&[u8]>) -> io::Result<Self> {
        match profile {
            Profile::None => Ok(Codec::Plain),
            Profile::Rtpopus => {
                let k = key.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "obf key required")
                })?;
                Ok(Codec::Rtpopus(Mutex::new(rtpopus::Conn::new(k, false)?)))
            }
            Profile::Rtpopus2 => {
                let k = key.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "obf key required")
                })?;
                Ok(Codec::Rtpopus2(Mutex::new(rtpopus2::Conn::new(k, false)?)))
            }
            Profile::Rtpopus3 => {
                let k = key.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "obf key required")
                })?;
                Ok(Codec::Rtpopus3(Box::new(Mutex::new(rtpopus3::Conn::new(
                    k, false,
                )?))))
            }
        }
    }

    pub fn server(profile: Profile, key: Option<&[u8]>) -> io::Result<Self> {
        match profile {
            Profile::None => Ok(Codec::Plain),
            Profile::Rtpopus => {
                let k = key.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "obf key required")
                })?;
                Ok(Codec::Rtpopus(Mutex::new(rtpopus::Conn::new(k, true)?)))
            }
            Profile::Rtpopus2 => {
                let k = key.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "obf key required")
                })?;
                Ok(Codec::Rtpopus2(Mutex::new(rtpopus2::Conn::new(k, true)?)))
            }
            Profile::Rtpopus3 => {
                let k = key.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "obf key required")
                })?;
                Ok(Codec::Rtpopus3(Box::new(Mutex::new(rtpopus3::Conn::new(
                    k, true,
                )?))))
            }
        }
    }

    /// Оборачивает payload. Возвращает wire-байты.
    pub fn wrap(&self, payload: &[u8]) -> io::Result<Vec<u8>> {
        match self {
            Codec::Plain => Ok(payload.to_vec()),
            Codec::Rtpopus(c) => {
                let mut conn = c.lock().unwrap();
                let mut buf = vec![0u8; rtpopus::OVERHEAD + payload.len()];
                let n = conn.wrap_into_mut(&mut buf, payload)?;
                buf.truncate(n);
                Ok(buf)
            }
            Codec::Rtpopus2(c) => {
                let mut conn = c.lock().unwrap();
                let mut buf = vec![0u8; rtpopus2::OVERHEAD + payload.len()];
                let n = conn.wrap_into_mut(&mut buf, payload)?;
                buf.truncate(n);
                Ok(buf)
            }
            Codec::Rtpopus3(c) => {
                let mut conn = c.lock().unwrap();
                let mut buf = vec![0u8; rtpopus3::OVERHEAD + payload.len()];
                let n = conn.wrap_into_mut(&mut buf, payload)?;
                buf.truncate(n);
                Ok(buf)
            }
        }
    }

    /// Разбирает wire-байты.
    pub fn unwrap(&self, wire: &[u8]) -> io::Result<Vec<u8>> {
        match self {
            Codec::Plain => Ok(wire.to_vec()),
            Codec::Rtpopus(c) => {
                let mut conn = c.lock().unwrap();
                let mut buf = vec![0u8; wire.len()];
                let n = conn.unwrap_into_mut(wire, &mut buf)?;
                buf.truncate(n);
                Ok(buf)
            }
            Codec::Rtpopus2(c) => {
                let mut conn = c.lock().unwrap();
                let mut buf = vec![0u8; wire.len()];
                let n = conn.unwrap_into_mut(wire, &mut buf)?;
                buf.truncate(n);
                Ok(buf)
            }
            Codec::Rtpopus3(c) => {
                let mut conn = c.lock().unwrap();
                let mut buf = vec![0u8; wire.len()];
                let n = conn.unwrap_into_mut(wire, &mut buf)?;
                buf.truncate(n);
                Ok(buf)
            }
        }
    }
}
