use anyhow::{bail,Context,Result};
use std::sync::Arc;
use tokio::io::{AsyncReadExt,AsyncWriteExt};
use tots_core::{config::ConnectionConfig,session::{DecodedPacket,SessionEngine},transport::DatagramPath};

pub enum TunHandle { Device(tun::AsyncDevice), #[cfg(unix)] Fd(tokio::fs::File) }

pub async fn open(cfg:&ConnectionConfig,name:&str,fd:Option<i32>)->Result<TunHandle>{
    #[cfg(unix)] if let Some(fd)=fd { use std::os::fd::FromRawFd; if fd<0{bail!("invalid --tun-fd");} let f=unsafe{std::fs::File::from_raw_fd(fd)};return Ok(TunHandle::Fd(tokio::fs::File::from_std(f))); }
    let mut c=tun::Configuration::default();c.tun_name(name).address(cfg.virtual_ipv4()).netmask((255,255,0,0)).mtu(cfg.mtu as u16).up();
    #[cfg(target_os="linux")] c.platform_config(|p|{p.packet_information(false);});
    Ok(TunHandle::Device(tun::create_as_async(&c).context("create TUN")?))
}

pub async fn run(tun:TunHandle,engine:Arc<SessionEngine>,path:Arc<dyn DatagramPath>)->Result<()> {
    match tun { TunHandle::Device(dev)=>run_io(dev,engine,path).await, #[cfg(unix)] TunHandle::Fd(file)=>run_io(file,engine,path).await }
}

async fn run_io<T>(io:T,engine:Arc<SessionEngine>,path:Arc<dyn DatagramPath>)->Result<()> where T:tokio::io::AsyncRead+tokio::io::AsyncWrite+Unpin+Send+'static {
    let (mut rd,mut wr)=tokio::io::split(io);let tx_engine=engine.clone();let tx_path=path.clone();let tx=tokio::spawn(async move{let mut buf=vec![0u8;65535];loop{let n=rd.read(&mut buf).await?;if n==0{break;}let assoc=flow_hash(&buf[..n]);let(_,packets)=tx_engine.encapsulate_ip(assoc,&buf[..n])?;for p in packets{tx_path.send(&p).await?;}}Ok::<_,anyhow::Error>(())});
    let rx=tokio::spawn(async move{let mut buf=vec![0u8;65535];loop{let n=path.recv(&mut buf).await?;if let Some(decoded)=engine.decapsulate(&buf[..n])?{match decoded{DecodedPacket::Ip{packet_id,bytes,..}=>{wr.write_all(&bytes).await?;let ack=engine.make_ack(packet_id)?;path.send(&ack).await?;},DecodedPacket::Probe{token,sent_micros}=>{path.send(&engine.make_probe_ack(token,sent_micros)?).await?;},_=>{}}}}#[allow(unreachable_code)]Ok::<_,anyhow::Error>(())});
    tokio::select!{r=tx=>r??,r=rx=>r??};Ok(())
}
fn flow_hash(packet:&[u8])->u16{let mut x:u32=0x9e37_79b9;for b in packet.iter().take(64){x=x.rotate_left(5)^(*b as u32);x=x.wrapping_mul(0x85eb_ca6b);}((x>>16)^x) as u16}
