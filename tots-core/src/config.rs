use crate::{Error, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::net::IpAddr;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProviderName {
    Vk,
    #[serde(other)]
    Unknown,
}

impl ProviderName {
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "vk" => Ok(Self::Vk),
            other => Err(Error::Config(format!("unsupported provider {other:?}"))),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub version: u8,
    pub peer: String,
    pub webrtc_port: u16,
    pub turn_port: u16,
    pub password: String,
    pub device_id: Uuid,
    pub provider: ProviderName,
    #[serde(default)]
    pub provider_data: Value,
    #[serde(default = "default_mtu")]
    pub mtu: usize,
}

const fn default_mtu() -> usize {
    1200
}

impl ConnectionConfig {
    pub fn validate(&self) -> Result<()> {
        if self.version != 1 {
            return Err(Error::Config("unsupported config version".into()));
        }
        if self.peer.trim().is_empty() {
            return Err(Error::Config("peer is empty".into()));
        }
        if self.webrtc_port == 0 || self.turn_port == 0 {
            return Err(Error::Config("ports must be non-zero".into()));
        }
        if self.password.len() < 8 {
            return Err(Error::Config("password must be at least 8 bytes".into()));
        }
        if !(576..=1400).contains(&self.mtu) {
            return Err(Error::Config("mtu must be 576..=1400".into()));
        }
        match self.provider {
            ProviderName::Vk => {
                let links = self
                    .provider_data
                    .get("vk_call_links")
                    .and_then(Value::as_array)
                    .map_or(0, |v| v.len());
                let token = self
                    .provider_data
                    .get("vk_token")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if links == 0 && token.trim().is_empty() {
                    return Err(Error::Config(
                        "VK provider_data requires vk_call_links or vk_token".into(),
                    ));
                }
            }
            ProviderName::Unknown => return Err(Error::Config("unknown provider".into())),
        }
        Ok(())
    }

    pub fn encode_uri(&self) -> Result<String> {
        self.validate()?;
        let json = serde_json::to_vec(self)?;
        Ok(format!("tots://{}", URL_SAFE_NO_PAD.encode(json)))
    }

    pub fn decode_uri(uri: &str) -> Result<Self> {
        let raw = uri
            .strip_prefix("tots://")
            .ok_or_else(|| Error::Config("config must start with tots://".into()))?;
        let bytes = URL_SAFE_NO_PAD
            .decode(raw)
            .map_err(|e| Error::Config(format!("invalid base64: {e}")))?;
        let cfg: Self = serde_json::from_slice(&bytes)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn peer_ip(&self) -> Result<IpAddr> {
        self.peer
            .parse()
            .map_err(|_| Error::Config("peer must currently be an IP address".into()))
    }

    pub fn virtual_ipv4(&self) -> std::net::Ipv4Addr {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(self.device_id.as_bytes());
        let a = 1 + (digest[0] % 253);
        let b = 1 + (digest[1] % 253);
        std::net::Ipv4Addr::new(10, 77, a, b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn uri_roundtrip() {
        let c = ConnectionConfig {
            version: 1,
            peer: "203.0.113.2".into(),
            webrtc_port: 7800,
            turn_port: 7801,
            password: "12345678".into(),
            device_id: Uuid::nil(),
            provider: ProviderName::Vk,
            provider_data: serde_json::json!({"vk_call_links":["abc"]}),
            mtu: 1200,
        };
        let uri = c.encode_uri().unwrap();
        let d = ConnectionConfig::decode_uri(&uri).unwrap();
        assert_eq!(d.peer, c.peer);
    }
}
