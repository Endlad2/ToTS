//! Разбор аргументов командной строки.

use std::net::SocketAddr;
use std::time::Duration;

use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Provider {
    Vk,
    Direct,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ObfProfile {
    None,
    Rtpopus,
    Rtpopus2,
    Rtpopus3,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Mode {
    Udp,
    Tcp,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Transport {
    Tcp,
    Udp,
}

#[derive(Debug, Parser)]
#[command(name = "tots-client", version, about = "ToTS VPN client")]
pub struct Cli {
    #[arg(long)]
    pub peer: SocketAddr,

    #[arg(long, value_enum, default_value_t = Provider::Vk)]
    pub provider: Provider,

    #[arg(long)]
    pub link: Option<String>,

    #[arg(long, default_value = "127.0.0.1:9000")]
    pub listen: SocketAddr,

    #[arg(long, default_value_t = 12)]
    pub streams: u32,

    #[arg(long, value_enum, default_value_t = Transport::Tcp)]
    pub transport: Transport,

    #[arg(long, value_enum, default_value_t = Mode::Udp)]
    pub mode: Mode,

    #[arg(long, value_enum, default_value_t = ObfProfile::Rtpopus3)]
    pub obf_profile: ObfProfile,

    #[arg(long)]
    pub obf_key: Option<String>,

    #[arg(long, value_parser = humantime_parse)]
    pub obf_timing: Option<Duration>,

    #[arg(long)]
    pub client_id: Option<String>,

    #[arg(long, default_value_t = 12)]
    pub streams_per_cred: u32,

    #[arg(long)]
    pub debug: bool,
}

fn humantime_parse(s: &str) -> Result<Duration, String> {
    if let Some(v) = s.strip_suffix("ms") {
        return v.parse::<u64>().map(Duration::from_millis).map_err(|e| e.to_string());
    }
    if let Some(v) = s.strip_suffix('s') {
        return v.parse::<u64>().map(Duration::from_secs).map_err(|e| e.to_string());
    }
    s.parse::<u64>().map(Duration::from_secs).map_err(|e| e.to_string())
}
