use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
#[derive(Parser, Clone, Debug)]
#[command(name = "tots-server", version)]
pub struct Args {
    #[arg(long, env = "TOTS_DB", default_value = "/var/lib/tots/tots.db")]
    pub db: PathBuf,
    #[arg(long, env = "TOTS_API_LISTEN", default_value = "127.0.0.1:9080")]
    pub api_listen: SocketAddr,
    #[arg(long, env = "TOTS_WEBRTC_LISTEN", default_value = "0.0.0.0:7800")]
    pub webrtc_listen: SocketAddr,
    #[arg(long, env = "TOTS_TURN_LISTEN", default_value = "0.0.0.0:7801")]
    pub turn_listen: SocketAddr,
    #[arg(long, env = "TOTS_PUBLIC_HOST")]
    pub public_host: String,
    #[arg(long, env = "TOTS_TUN_NAME", default_value = "tots0")]
    pub tun_name: String,
    #[arg(long, env = "TOTS_ADMIN_TOKEN")]
    pub admin_token: Option<String>,
    #[arg(long, env = "TOTS_NO_TUN", default_value_t = false)]
    pub no_tun: bool,
}
