mod tun_io;
use anyhow::{Context,Result};
use clap::Parser;
use std::{net::SocketAddr,sync::Arc,time::Duration};
use tots_core::{config::{ConnectionConfig,ProviderName},providers::make_provider,session::{DecodedPacket,SessionEngine},transport::{DatagramPath,DirectUdpPath,probe,turn::TurnUdpPath}};
use tracing::{info,warn};
use uuid::Uuid;

#[derive(Parser,Debug)]
#[command(name="tots-client",version,about="ToTS desktop/Android client")]
struct Args{
    #[arg(long)] config:Option<String>, #[arg(long)] peer:Option<String>, #[arg(long)] webrtc_port:Option<u16>, #[arg(long)] turn_port:Option<u16>, #[arg(long)] password:Option<String>, #[arg(long)] device_id:Option<Uuid>, #[arg(long,default_value="vk")] provider:String, #[arg(long)] provider_data:Option<String>,
    #[arg(long)] tun_fd:Option<i32>, #[arg(long,default_value="tots0")] tun_name:String, #[arg(long,default_value_t=1200)] mtu:usize,
}

fn config_from_args(a:&Args)->Result<ConnectionConfig>{if let Some(uri)=&a.config{return Ok(ConnectionConfig::decode_uri(uri)?);}Ok(ConnectionConfig{version:1,peer:a.peer.clone().context("--peer required")?,webrtc_port:a.webrtc_port.context("--webrtc-port required")?,turn_port:a.turn_port.context("--turn-port required")?,password:a.password.clone().context("--password required")?,device_id:a.device_id.context("--device-id required")?,provider:ProviderName::parse(&a.provider)?,provider_data:serde_json::from_str(a.provider_data.as_deref().unwrap_or("{}"))?,mtu:a.mtu})}

#[tokio::main]
async fn main()->Result<()> {
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("tots_client=info".parse()?)).init();
    let args=Args::parse();let cfg=config_from_args(&args)?;cfg.validate()?;let provider=make_provider(&cfg.provider)?;info!("fetching provider bootstrap");let boot=provider.bootstrap(&cfg.provider_data).await?;let engine=Arc::new(SessionEngine::new(cfg.clone())?);let peer_ip=cfg.peer_ip()?;
    let direct:Arc<dyn DatagramPath>=Arc::new(DirectUdpPath::connect("direct-ice",SocketAddr::new(peer_ip,cfg.webrtc_port)).await?);
    let mut candidates:Vec<Arc<dyn DatagramPath>>=vec![direct.clone()];
    for creds in &boot.turn { match TurnUdpPath::connect(creds,SocketAddr::new(peer_ip,cfg.turn_port)).await {Ok(p)=>{candidates.push(Arc::new(p));break},Err(e)=>warn!("TURN candidate failed: {e}")} }
    let mut best:Option<(Duration,Arc<dyn DatagramPath>)>=None;for p in candidates{match probe::probe(&engine,p.clone(),Duration::from_secs(3)).await{Ok(r)=>{info!(path=%r.path,rtt_ms=r.rtt.as_millis(),"path healthy");if best.as_ref().map_or(true,|(d,_)|r.rtt<*d){best=Some((r.rtt,p));}},Err(e)=>warn!(path=%p.name(),"probe failed: {e}")}}
    let path=best.context("no healthy ToTS path")?.1;info!(path=%path.name(),virtual_ip=%cfg.virtual_ipv4(),"selected path");
    let tun=tun_io::open(&cfg,&args.tun_name,args.tun_fd).await?;
    let run_result=tun_io::run(tun,engine,path).await;
    if let Err(e)=provider.cleanup(&cfg.provider_data,&boot).await{warn!("provider cleanup failed: {e}");}
    run_result
}
