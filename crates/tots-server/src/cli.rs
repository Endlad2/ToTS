//! CLI сервера.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};

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

#[derive(Debug, Parser)]
#[command(name = "tots-server", version, about = "ToTS VPN server")]
pub struct Cli {
    #[arg(long, default_value = "0.0.0.0:56000")]
    pub listen: SocketAddr,

    #[arg(long)]
    pub connect: SocketAddr,

    #[arg(long, value_enum, default_value_t = Mode::Udp)]
    pub mode: Mode,

    #[arg(long, value_enum, default_value_t = ObfProfile::Rtpopus3)]
    pub obf_profile: ObfProfile,

    #[arg(long)]
    pub obf_key: Option<String>,

    #[arg(long, value_parser = humantime_parse)]
    pub obf_timing: Option<Duration>,

    #[arg(long)]
    pub clients_file: Option<PathBuf>,

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
