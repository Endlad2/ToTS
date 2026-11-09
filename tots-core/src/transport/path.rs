use crate::{Error, Result};
use async_trait::async_trait;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::UdpSocket;

#[async_trait]
pub trait DatagramPath: Send + Sync {
    fn name(&self)->&str;
    async fn send(&self,data:&[u8])->Result<()>;
    async fn recv(&self,buf:&mut [u8])->Result<usize>;
}

pub struct DirectUdpPath { name:String, socket:Arc<UdpSocket> }
impl DirectUdpPath { pub async fn connect(name:impl Into<String>, target:SocketAddr)->Result<Self>{let bind=if target.is_ipv4(){"0.0.0.0:0"}else{"[::]:0"};let s=UdpSocket::bind(bind).await?;s.connect(target).await?;Ok(Self{name:name.into(),socket:Arc::new(s)})} }
#[async_trait]
impl DatagramPath for DirectUdpPath { fn name(&self)->&str{&self.name} async fn send(&self,data:&[u8])->Result<()>{let n=self.socket.send(data).await?;if n!=data.len(){return Err(Error::Transport("short UDP send".into()));}Ok(())} async fn recv(&self,buf:&mut[u8])->Result<usize>{Ok(self.socket.recv(buf).await?)} }
