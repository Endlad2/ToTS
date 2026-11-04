use std::net::Ipv4Addr;

use anyhow::{bail, Context, Result};
use base64::{
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD},
    Engine as _,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{DEFAULT_LOCAL_PORT, DEFAULT_TURN_PORT, DEFAULT_WEBRTC_PORT};

pub const TOTS_CONFIG_VERSION: u8 = 1;
pub const TOTS_CONFIG_SCHEME: &str = "tots://";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TotsConfig {
    #[serde(default = "default_config_version")]
    pub version: u8,
    pub server: String,
    pub device_id: Uuid,
    pub password: String,
    #[serde(default = "default_provider")]
    pub provider: String,
    #[serde(default = "default_provider_data")]
    pub provider_data: Value,
    #[serde(default = "default_transport")]
    pub transport: String,
    #[serde(default = "default_local_port")]
    pub local_port: u16,
    #[serde(default)]
    pub tun: bool,
    #[serde(default = "default_tun_name")]
    pub tun_name: String,
    #[serde(default = "default_tun_address")]
    pub tun_address: Ipv4Addr,
    #[serde(default = "default_tun_prefix")]
    pub tun_prefix: u8,
    #[serde(default = "default_tun_mtu")]
    pub tun_mtu: u16,
    #[serde(default)]
    pub tun_default_route: bool,
    #[serde(default = "default_tun_dns")]
    pub tun_dns: Vec<Ipv4Addr>,
    #[serde(default = "default_webrtc_port")]
    pub webrtc_port: u16,
    #[serde(default = "default_turn_port")]
    pub turn_port: u16,
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
}

impl TotsConfig {
    /// Encode this configuration as a copy/paste friendly `tots://` URI.
    /// URL-safe base64 without padding keeps the payload free of URI delimiters.
    pub fn to_uri(&self) -> Result<String> {
        if self.version != TOTS_CONFIG_VERSION {
            bail!(
                "cannot encode unsupported ToTS config version {}; expected {}",
                self.version,
                TOTS_CONFIG_VERSION
            );
        }
        let json = serde_json::to_vec(self).context("serialize ToTS config JSON")?;
        Ok(format!(
            "{TOTS_CONFIG_SCHEME}{}",
            URL_SAFE_NO_PAD.encode(json)
        ))
    }

    /// Decode a `tots://<base64-json>` URI. URL-safe and standard base64,
    /// padded or unpadded, are accepted for interoperability.
    pub fn from_uri(uri: &str) -> Result<Self> {
        let payload = uri
            .trim()
            .strip_prefix(TOTS_CONFIG_SCHEME)
            .context("config must start with 'tots://'")?;
        if payload.is_empty() {
            bail!("ToTS config URI has an empty payload");
        }

        let json = URL_SAFE_NO_PAD
            .decode(payload)
            .or_else(|_| URL_SAFE.decode(payload))
            .or_else(|_| STANDARD_NO_PAD.decode(payload))
            .or_else(|_| STANDARD.decode(payload))
            .context("decode base64 payload from ToTS config URI")?;
        let config: Self =
            serde_json::from_slice(&json).context("parse JSON from ToTS config URI")?;

        if config.version != TOTS_CONFIG_VERSION {
            bail!(
                "unsupported ToTS config version {}; this client supports version {}",
                config.version,
                TOTS_CONFIG_VERSION
            );
        }
        if config.server.trim().is_empty() {
            bail!("ToTS config server must not be empty");
        }
        if config.device_id.is_nil() {
            bail!("ToTS config device_id must be a non-nil UUID");
        }
        if config.password.is_empty() {
            bail!("ToTS config password must not be empty");
        }

        Ok(config)
    }
}

fn default_config_version() -> u8 {
    TOTS_CONFIG_VERSION
}
fn default_provider() -> String {
    "vk".to_string()
}
fn default_provider_data() -> Value {
    Value::Object(Default::default())
}
fn default_transport() -> String {
    "auto".to_string()
}
fn default_local_port() -> u16 {
    DEFAULT_LOCAL_PORT
}
fn default_tun_name() -> String {
    "tots0".to_string()
}
fn default_tun_address() -> Ipv4Addr {
    Ipv4Addr::new(10, 77, 0, 2)
}
fn default_tun_prefix() -> u8 {
    24
}
fn default_tun_mtu() -> u16 {
    1300
}
fn default_tun_dns() -> Vec<Ipv4Addr> {
    vec![Ipv4Addr::new(1, 1, 1, 1), Ipv4Addr::new(8, 8, 8, 8)]
}
fn default_webrtc_port() -> u16 {
    DEFAULT_WEBRTC_PORT
}
fn default_turn_port() -> u16 {
    DEFAULT_TURN_PORT
}
fn default_connect_timeout_secs() -> u64 {
    8
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> TotsConfig {
        TotsConfig {
            version: TOTS_CONFIG_VERSION,
            server: "vpn.example.test".to_string(),
            device_id: Uuid::parse_str("8a4b7574-09f5-45fa-bcba-4522c3f70a24").unwrap(),
            password: "test-password".to_string(),
            provider: "jazz".to_string(),
            provider_data: serde_json::json!({"name": "ToTS relay"}),
            transport: "jazz".to_string(),
            local_port: DEFAULT_LOCAL_PORT,
            tun: true,
            tun_name: "tots0".to_string(),
            tun_address: Ipv4Addr::new(10, 77, 0, 2),
            tun_prefix: 24,
            tun_mtu: 1300,
            tun_default_route: true,
            tun_dns: default_tun_dns(),
            webrtc_port: DEFAULT_WEBRTC_PORT,
            turn_port: DEFAULT_TURN_PORT,
            connect_timeout_secs: 8,
        }
    }

    #[test]
    fn uri_round_trip() {
        let source = sample_config();
        let uri = source.to_uri().unwrap();
        assert!(uri.starts_with(TOTS_CONFIG_SCHEME));
        let decoded = TotsConfig::from_uri(&uri).unwrap();
        assert_eq!(decoded.server, source.server);
        assert_eq!(decoded.device_id, source.device_id);
        assert_eq!(decoded.provider_data, source.provider_data);
        assert!(decoded.tun);
        assert!(decoded.tun_default_route);
    }

    #[test]
    fn accepts_standard_padded_base64() {
        let source = sample_config();
        let json = serde_json::to_vec(&source).unwrap();
        let uri = format!("{TOTS_CONFIG_SCHEME}{}", STANDARD.encode(json));
        let decoded = TotsConfig::from_uri(&uri).unwrap();
        assert_eq!(decoded.server, source.server);
        assert_eq!(decoded.device_id, source.device_id);
    }
}
