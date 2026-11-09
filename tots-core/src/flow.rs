use std::{collections::HashMap, time::{Duration, Instant}};

#[derive(Debug)]
pub struct FlowController { cwnd:usize, max_cwnd:usize, min_cwnd:usize, sent:HashMap<u32,Instant>, srtt:Option<Duration> }
impl Default for FlowController { fn default()->Self{Self{cwnd:64,max_cwnd:1024,min_cwnd:16,sent:HashMap::new(),srtt:None}} }
impl FlowController {
    pub fn can_send(&self)->bool{self.sent.len()<self.cwnd}
    pub fn on_sent(&mut self, packet_id:u32){self.sent.insert(packet_id,Instant::now());}
    pub fn on_ack(&mut self, packet_id:u32)->Option<Duration>{let rtt=self.sent.remove(&packet_id)?.elapsed(); self.srtt=Some(match self.srtt{None=>rtt,Some(old)=>old.mul_f64(0.875)+rtt.mul_f64(0.125)}); if self.cwnd<self.max_cwnd{self.cwnd+=1;} Some(rtt)}
    pub fn on_timeout(&mut self){self.cwnd=(self.cwnd/2).max(self.min_cwnd); let limit=self.srtt.unwrap_or(Duration::from_millis(250))*4; self.sent.retain(|_,t|t.elapsed()<limit);}
    pub fn srtt(&self)->Option<Duration>{self.srtt}
    pub fn cwnd(&self)->usize{self.cwnd}
}
