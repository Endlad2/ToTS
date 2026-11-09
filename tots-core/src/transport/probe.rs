use crate::{session::{DecodedPacket,SessionEngine}, transport::DatagramPath, Error, Result};
use rand::{rngs::OsRng,RngCore};
use std::{sync::Arc,time::{Duration,Instant,SystemTime,UNIX_EPOCH}};

#[derive(Clone,Debug)] pub struct ProbeResult{pub path:String,pub rtt:Duration}
fn micros()->u64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64}
pub async fn probe(engine:&SessionEngine,path:Arc<dyn DatagramPath>,timeout:Duration)->Result<ProbeResult>{let token=OsRng.next_u64();let pkt=engine.make_probe(token,micros())?;let start=Instant::now();path.send(&pkt).await?;let mut buf=vec![0u8;2048];loop{let n=tokio::time::timeout(timeout,path.recv(&mut buf)).await.map_err(|_|Error::Transport(format!("{} probe timeout",path.name())))??;if let Some(DecodedPacket::ProbeAck{token:t,..})=engine.decapsulate(&buf[..n])?{if t==token{return Ok(ProbeResult{path:path.name().to_owned(),rtt:start.elapsed()});}}}}
