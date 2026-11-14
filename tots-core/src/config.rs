use crate::{Error, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::net::IpAddr;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ProviderName {
    #[serde(rename = "vk")]
    Vk,
    #[serde(rename = "jazz")]
    Jazz,
    #[serde(rename = "ya-docs")]
    YaDocs,
    #[serde(rename = "mail-docs")]
    MailDocs,
    #[serde(rename = "cups")]
    Cups,
    #[serde(other)]
    Unknown,
}

impl ProviderName {
    pub const ALL: [Self; 5] = [Self::Vk, Self::Jazz, Self::YaDocs, Self::MailDocs, Self::Cups];

    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "vk" => Ok(Self::Vk),
            "jazz" => Ok(Self::Jazz),
            "ya-docs" | "yadocs" | "yandex-docs" => Ok(Self::YaDocs),
            "mail-docs" | "maildocs" | "mailru" => Ok(Self::MailDocs),
            "cups" | "cupsonline" | "cups-online" => Ok(Self::Cups),
            other => Err(Error::Config(format!("unsupported provider {other:?}"))),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Vk => "vk",
            Self::Jazz => "jazz",
            Self::YaDocs => "ya-docs",
            Self::MailDocs => "mail-docs",
            Self::Cups => "cups",
            Self::Unknown => "unknown",
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
    /// Preferred provider. Additional providers can be present in `provider_data`.
    pub provider: ProviderName,
    /// Multi-provider layout keyed by provider: vk/jazz/ya-docs/mail-docs/cups.
    /// Legacy single-provider objects are still accepted for old server DB rows/config URIs.
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
        if self.provider == ProviderName::Unknown {
            return Err(Error::Config("unknown provider".into()));
        }
        if !self.provider_data.is_object() {
            return Err(Error::Config("provider_data must be an object".into()));
        }

        let order = self.provider_order();
        if !order.contains(&self.provider) {
            return Err(Error::Config(format!(
                "provider_data is missing primary provider {}",
                self.provider.as_str()
            )));
        }
        for provider in order {
            let data = self.provider_data_for(provider).ok_or_else(|| {
                Error::Config(format!("provider_data is missing {}", provider.as_str()))
            })?;
            validate_provider(provider, data)?;
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

    /// Returns provider-specific data. New configs are keyed by provider name.
    /// Old configs with a flat provider_data object continue to work for the primary provider.
    pub fn provider_data_for(&self, provider: ProviderName) -> Option<&Value> {
        if let Some(value) = self.provider_data.get(provider.as_str()) {
            return Some(value);
        }
        if provider == self.provider && !self.has_keyed_provider_data() {
            return Some(&self.provider_data);
        }
        None
    }

    /// Primary provider first, then configured fallbacks in a deterministic order.
    pub fn provider_order(&self) -> Vec<ProviderName> {
        let mut out = Vec::with_capacity(ProviderName::ALL.len());
        if self.provider != ProviderName::Unknown
            && (self.provider_data.get(self.provider.as_str()).is_some()
                || !self.has_keyed_provider_data())
        {
            out.push(self.provider);
        }
        for provider in ProviderName::ALL {
            if provider != self.provider && self.provider_data.get(provider.as_str()).is_some() {
                out.push(provider);
            }
        }
        out
    }

    pub fn has_provider(&self, provider: ProviderName) -> bool {
        self.provider_data_for(provider).is_some()
    }

    pub fn has_keyed_provider_data(&self) -> bool {
        ProviderName::ALL
            .iter()
            .any(|provider| self.provider_data.get(provider.as_str()).is_some())
    }
}

fn validate_provider(provider: ProviderName, data: &Value) -> Result<()> {
    if !data.is_object() {
        return Err(Error::Config(format!(
            "provider_data.{} must be an object",
            provider.as_str()
        )));
    }
    match provider {
        ProviderName::Vk => {
            let links = data
                .get("vk_call_links")
                .and_then(Value::as_array)
                .map_or(0, |v| v.len());
            let token = data.get("vk_token").and_then(Value::as_str).unwrap_or("");
            if links == 0 && token.trim().is_empty() {
                return Err(Error::Config(
                    "provider_data.vk requires vk_call_links or vk_token".into(),
                ));
            }
        }
        ProviderName::Jazz => {
            if data
                .get("join_link")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .is_empty()
            {
                return Err(Error::Config(
                    "provider_data.jazz requires a server-issued join_link".into(),
                ));
            }
        }
        ProviderName::YaDocs => validate_url_provider("ya-docs", data)?,
        ProviderName::MailDocs => validate_url_provider("mail-docs", data)?,
        ProviderName::Cups => {
            let room = data.get("room_id").and_then(Value::as_str).unwrap_or("").trim();
            let rooms = data.get("worker_rooms").and_then(Value::as_array);
            if room.is_empty() && rooms.is_none_or(|rooms| rooms.is_empty()) {
                return Err(Error::Config(
                    "provider_data.cups requires server-issued room_id/worker_rooms".into(),
                ));
            }
        }
        ProviderName::Unknown => return Err(Error::Config("unknown provider".into())),
    }
    Ok(())
}


fn validate_url_provider(name: &str, data: &Value) -> Result<()> {
    let one = data
        .get("url")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty());
    let many = data
        .get("urls")
        .and_then(Value::as_array)
        .is_some_and(|values| {
            !values.is_empty()
                && values.iter().all(|value| {
                    value
                        .as_str()
                        .is_some_and(|value| !value.trim().is_empty())
                })
        });
    if !one && !many {
        return Err(Error::Config(format!(
            "provider_data.{name} requires url or non-empty urls"
        )));
    }
    if let Some(cookies) = data.get("cookies") {
        let Some(cookies) = cookies.as_object() else {
            return Err(Error::Config(format!("provider_data.{name}.cookies must be an object")));
        };
        if cookies.values().any(|value| !value.is_string()) {
            return Err(Error::Config(format!(
                "provider_data.{name}.cookies values must be strings"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(provider_data: Value) -> ConnectionConfig {
        ConnectionConfig {
            version: 1,
            peer: "203.0.113.2".into(),
            webrtc_port: 7800,
            turn_port: 7801,
            password: "12345678".into(),
            device_id: Uuid::nil(),
            provider: ProviderName::Vk,
            provider_data,
            mtu: 1200,
        }
    }

    #[test]
    fn uri_roundtrip_multi_provider() {
        let c = base(serde_json::json!({
            "vk":{"vk_call_links":["abc"]},
            "jazz":{"join_link":"https://example.invalid/call"}
        }));
        let uri = c.encode_uri().unwrap();
        let d = ConnectionConfig::decode_uri(&uri).unwrap();
        assert_eq!(d.peer, c.peer);
        assert_eq!(
            d.provider_order(),
            vec![ProviderName::Vk, ProviderName::Jazz]
        );
    }

    #[test]
    fn old_flat_provider_data_is_still_accepted() {
        let c = base(serde_json::json!({"vk_call_links":["abc"]}));
        c.validate().unwrap();
        assert!(c.provider_data_for(ProviderName::Vk).is_some());
    }
}
