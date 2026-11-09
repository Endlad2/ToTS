use crate::{protocol::Frame, Error, Result};
use std::{collections::HashMap, time::{Duration, Instant}};

struct Partial { created:Instant, total:u16, original_len:u16, parts:Vec<Option<Vec<u8>>>, bytes:usize }

pub struct Reassembler { entries:HashMap<(u64,u32),Partial>, total_bytes:usize, max_entries:usize, max_bytes:usize, ttl:Duration }
impl Default for Reassembler { fn default()->Self{Self{entries:HashMap::new(),total_bytes:0,max_entries:256,max_bytes:4*1024*1024,ttl:Duration::from_secs(4)}} }
impl Reassembler {
    pub fn push(&mut self, session:u64, frame:&Frame)->Result<Option<Vec<u8>>> {
        self.gc();
        let Frame::Data{packet_id,frag_total,frag_id,original_len,payload,..}=frame else{return Ok(None)};
        if *frag_total==1 { if payload.len()!=*original_len as usize{return Err(Error::Packet("length mismatch".into()));} return Ok(Some(payload.to_vec())); }
        if self.entries.len()>=self.max_entries && !self.entries.contains_key(&(session,*packet_id)){ return Err(Error::Packet("reassembly table full".into())); }
        let key=(session,*packet_id);
        let p=self.entries.entry(key).or_insert_with(||Partial{created:Instant::now(),total:*frag_total,original_len:*original_len,parts:vec![None;*frag_total as usize],bytes:0});
        if p.total!=*frag_total || p.original_len!=*original_len { return Err(Error::Packet("fragment metadata changed".into())); }
        let slot=&mut p.parts[*frag_id as usize];
        if slot.is_none(){ p.bytes+=payload.len(); self.total_bytes+=payload.len(); *slot=Some(payload.to_vec()); }
        if self.total_bytes>self.max_bytes { self.entries.remove(&key); self.recount(); return Err(Error::Packet("reassembly byte budget exceeded".into())); }
        if p.parts.iter().all(Option::is_some) { let p=self.entries.remove(&key).unwrap(); self.total_bytes=self.total_bytes.saturating_sub(p.bytes); let mut packet=Vec::with_capacity(p.original_len as usize); for part in p.parts.into_iter().flatten(){packet.extend_from_slice(&part);} if packet.len()!=p.original_len as usize{return Err(Error::Packet("reassembled length mismatch".into()));} return Ok(Some(packet)); }
        Ok(None)
    }
    fn gc(&mut self){ let now=Instant::now(); self.entries.retain(|_,p|now.duration_since(p.created)<=self.ttl); self.recount(); }
    fn recount(&mut self){self.total_bytes=self.entries.values().map(|p|p.bytes).sum();}
}
