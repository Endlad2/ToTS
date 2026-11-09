use anyhow::{Context,Result};
use chrono::Utc;
use dashmap::DashMap;
use std::{net::{Ipv4Addr,SocketAddr},sync::Arc};
use tokio::{io::{AsyncReadExt,AsyncWriteExt},net::UdpSocket,sync::{mpsc,Mutex}};
use tots_core::{config::ConnectionConfig,protocol::peek_meta,session::{DecodedPacket,SessionEngine}};
use uuid::Uuid;
use crate::{config::Args,db::Db};

#[derive(Clone,Copy,Debug)]enum PathKind{Direct,Turn}
#[derive(Clone,Debug)]struct Route{path:PathKind,addr:SocketAddr}
struct Peer{engine:Arc<SessionEngine>,route:Mutex<Route>,virtual_ip:Ipv4Addr}

pub async fn run(args:Args,db:Db)->Result<()> {
 let direct=Arc::new(UdpSocket::bind(args.webrtc_listen).await?);let turn=Arc::new(UdpSocket::bind(args.turn_listen).await?);let peers:Arc<DashMap<(Uuid,u64),Arc<Peer>>>=Arc::new(DashMap::new());
 let active_by_ip:Arc<DashMap<Ipv4Addr,(Uuid,u64)>>=Arc::new(DashMap::new());
 let (to_tun_tx,mut to_tun_rx)=mpsc::channel::<Vec<u8>>(2048);let (from_tun_tx,mut from_tun_rx)=mpsc::channel::<Vec<u8>>(2048);
 if !args.no_tun{let mut c=tun::Configuration::default();c.tun_name(&args.tun_name).address((10,77,0,1)).netmask((255,255,0,0)).mtu(1400).up();#[cfg(target_os="linux")]c.platform_config(|p|{p.packet_information(false);});let dev=tun::create_as_async(&c).context("create server TUN")?;let(mut rd,mut wr)=tokio::io::split(dev);let tx=from_tun_tx.clone();tokio::spawn(async move{let mut b=vec![0u8;65535];loop{match rd.read(&mut b).await{Ok(0)|Err(_)=>break,Ok(n)=>{if tx.send(b[..n].to_vec()).await.is_err(){break;}}}}});tokio::spawn(async move{while let Some(p)=to_tun_rx.recv().await{if wr.write_all(&p).await.is_err(){break;}}});} else {tokio::spawn(async move{while to_tun_rx.recv().await.is_some(){}});}
 for(kind,sock)in[(PathKind::Direct,direct.clone()),(PathKind::Turn,turn.clone())]{let db=db.clone();let peers=peers.clone();let active_by_ip=active_by_ip.clone();let tx=to_tun_tx.clone();tokio::spawn(async move{let mut buf=vec![0u8;65535];loop{let Ok((n,addr))=sock.recv_from(&mut buf).await else{continue};let data=&buf[..n];let Ok(meta)=peek_meta(data) else{continue};let peer=match get_peer(&db,&peers,meta.device_id,meta.session_id).await{Ok(Some(p))=>p,_=>continue};active_by_ip.insert(peer.virtual_ip,(meta.device_id,meta.session_id));*peer.route.lock().await=Route{path:kind,addr};match peer.engine.decapsulate(data){Ok(Some(DecodedPacket::Ip{packet_id,bytes,..}))=>{let _=tx.send(bytes).await;let _=sock.send_to(&peer.engine.make_ack(packet_id).unwrap_or_default(),addr).await;},Ok(Some(DecodedPacket::Probe{token,sent_micros}))=>{if let Ok(a)=peer.engine.make_probe_ack(token,sent_micros){let _=sock.send_to(&a,addr).await;}},_=>{}}}});}
 while let Some(packet)=from_tun_rx.recv().await{let Some(dst)=ipv4_dst(&packet)else{continue};let Some(key)=active_by_ip.get(&dst).map(|v|*v.value()) else{continue};let Some(peer)=peers.get(&key).map(|p|p.value().clone()) else{continue};let route=peer.route.lock().await.clone();if let Ok((_,frames))=peer.engine.encapsulate_ip(0,&packet){let socket=match route.path{PathKind::Direct=>&direct,PathKind::Turn=>&turn};for f in frames{let _=socket.send_to(&f,route.addr).await;}}}
 Ok(())
}

async fn get_peer(db:&Db,map:&DashMap<(Uuid,u64),Arc<Peer>>,id:Uuid,session:u64)->Result<Option<Arc<Peer>>>{let key=(id,session);if let Some(p)=map.get(&key){return Ok(Some(p.clone()));}let Some(d)=db.get(id).await? else{return Ok(None)};if d.revoked||d.expires_at<Utc::now(){return Ok(None)};let cfg=ConnectionConfig{version:1,peer:"0.0.0.0".into(),webrtc_port:1,turn_port:1,password:d.password.clone(),device_id:id,provider:d.provider,provider_data:d.provider_data,mtu:1200};let virtual_ip=cfg.virtual_ipv4();let p=Arc::new(Peer{engine:Arc::new(SessionEngine::with_session(cfg,session)?),route:Mutex::new(Route{path:PathKind::Direct,addr:"0.0.0.0:0".parse().unwrap()}),virtual_ip});map.insert(key,p.clone());Ok(Some(p))}
fn ipv4_dst(p:&[u8])->Option<Ipv4Addr>{if p.len()<20||p[0]>>4!=4{return None;}Some(Ipv4Addr::new(p[16],p[17],p[18],p[19]))}
