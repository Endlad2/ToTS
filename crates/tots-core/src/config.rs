use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Which underlying protocol the tunnel carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Payload {
    /// Amnezia/WireGuard datagrams over UDP.
    WireGuard,
    /// Xray VLESS streams over TCP.
    Xray,
}

impl Payload {
    pub fn is_datagram(self) -> bool {
        matches!(self, Payload::WireGuard)
    }
}

impl std::str::FromStr for Payload {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "wireguard" | "wg" | "udp" => Ok(Payload::WireGuard),
            "xray" | "vless" | "tcp" => Ok(Payload::Xray),
            other => Err(Error::Config(format!("unknown payload `{other}`"))),
        }
    }
}

/// A cover transport — the consumer service that hides the traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TransportKind {
    /// VK calls / TURN (free-turn-proxy-core logic).
    Vk,
    /// Yandex Telemost conferences (olcrtc-core logic).
    YandexTelemost,
    /// VK Stream broadcasts (olcrtc-core logic).
    VkStream,
    /// Yandex Documents collaborative editing (OpenFlux-core logic).
    YandexDocs,
    /// Mail.ru Documents (OpenFlux-core logic).
    MailDocs,
    /// MAX messenger (OpenFlux-core logic).
    Max,
    /// OneME messenger (OpenFlux-core logic).
    OneMe,
}

impl TransportKind {
    /// All transports, in canonical order.
    pub const ALL: [TransportKind; 7] = [
        TransportKind::Vk,
        TransportKind::YandexTelemost,
        TransportKind::VkStream,
        TransportKind::YandexDocs,
        TransportKind::MailDocs,
        TransportKind::Max,
        TransportKind::OneMe,
    ];

    pub fn id(self) -> &'static str {
        match self {
            TransportKind::Vk => "vk",
            TransportKind::YandexTelemost => "telemost",
            TransportKind::VkStream => "vkstream",
            TransportKind::YandexDocs => "yadocs",
            TransportKind::MailDocs => "maildocs",
            TransportKind::Max => "max",
            TransportKind::OneMe => "oneme",
        }
    }

    /// Realtime (WebRTC-like) transports behave differently from document ones.
    pub fn is_realtime(self) -> bool {
        matches!(
            self,
            TransportKind::Vk | TransportKind::YandexTelemost | TransportKind::VkStream
        )
    }
}

impl std::str::FromStr for TransportKind {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "vk" => Ok(TransportKind::Vk),
            "telemost" | "yandex-telemost" => Ok(TransportKind::YandexTelemost),
            "vkstream" | "vk-stream" => Ok(TransportKind::VkStream),
            "yadocs" | "yandex-docs" => Ok(TransportKind::YandexDocs),
            "maildocs" | "mail-docs" => Ok(TransportKind::MailDocs),
            "max" => Ok(TransportKind::Max),
            "oneme" | "one-me" => Ok(TransportKind::OneMe),
            other => Err(Error::UnsupportedTransport(other.to_string())),
        }
    }
}

/// Full ToTS configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Cover transport to use.
    pub transport: TransportKind,
    /// What the tunnel carries.
    pub payload: Payload,
    /// Server endpoint in `host:port` form.
    pub server: String,
    /// Pre-shared secret used to derive the tunnel session keys.
    pub password: String,
    /// Optional room / document / channel identifier required by some transports.
    #[serde(default)]
    pub room: Option<String>,
    /// Optional SOCKS5 listener for Xray mode (`127.0.0.1:1080`).
    #[serde(default)]
    pub socks_listen: Option<String>,
    /// Local TUN name (informational; the CLI decides the device).
    #[serde(default = "default_tun")]
    pub tun_name: String,
}

fn default_tun() -> String {
    "tots0".to_string()
}

impl Config {
    /// Parse a `tots://connect?...` link into a [`Config`].
    pub fn from_link(link: &str) -> Result<Self> {
        let url = url::Url::parse(link)
            .map_err(|e| Error::Config(format!("invalid tots link: {e}")))?;
        if url.scheme() != "tots" {
            return Err(Error::Config(format!(
                "unexpected scheme `{}`, expected `tots`",
                url.scheme()
            )));
        }

        let mut transport = None;
        let mut payload = None;
        let mut server = None;
        let mut password = None;
        let mut room = None;
        let mut socks = None;

        for (k, v) in url.query_pairs() {
            match k.as_ref() {
                "transport" | "t" => transport = Some(v.parse()?),
                "payload" | "p" => payload = Some(v.parse()?),
                "server" | "host" => server = Some(v.to_string()),
                "password" | "pw" => password = Some(v.to_string()),
                "room" | "r" => room = Some(v.to_string()),
                "socks" => socks = Some(v.to_string()),
                _ => {}
            }
        }

        Ok(Config {
            transport: transport
                .ok_or_else(|| Error::Config("missing `transport`".into()))?,
            payload: payload.unwrap_or(Payload::WireGuard),
            server: server.ok_or_else(|| Error::Config("missing `server`".into()))?,
            password: password.ok_or_else(|| Error::Config("missing `password`".into()))?,
            room,
            socks_listen: socks,
            tun_name: default_tun(),
        })
    }

    /// Validate invariants that the rest of the crate relies on.
    pub fn validate(&self) -> Result<()> {
        if !self.server.contains(':') {
            return Err(Error::Config(format!(
                "server `{}` must be host:port",
                self.server
            )));
        }
        if self.password.is_empty() {
            return Err(Error::Config("password must not be empty".into()));
        }
        Ok(())
    }
}