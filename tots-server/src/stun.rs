use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use rand::RngCore;
use tokio::{
    net::{lookup_host, UdpSocket},
    time::timeout,
};

const STUN_BINDING_REQUEST: u16 = 0x0001;
const STUN_BINDING_SUCCESS: u16 = 0x0101;
const STUN_MAGIC_COOKIE: u32 = 0x2112_A442;
const ATTR_MAPPED_ADDRESS: u16 = 0x0001;
const ATTR_XOR_MAPPED_ADDRESS: u16 = 0x0020;

/// Discover the externally visible IP address with an RFC 5389 STUN Binding request.
///
/// Only the IP is returned intentionally. STUN's mapped UDP port belongs to this
/// short-lived discovery socket and is not the public TCP/UDP port used by ToTS.
pub async fn discover_public_ip(server: &str, wait: Duration) -> Result<IpAddr> {
    let mut addrs: Vec<SocketAddr> = lookup_host(server)
        .await
        .with_context(|| format!("resolve STUN server {server}"))?
        .collect();
    if addrs.is_empty() {
        bail!("STUN server {server} resolved to no addresses");
    }
    // Prefer IPv4 because provider TURN relays and home NATs are more commonly
    // reachable over IPv4; fall back to IPv6 when it is the only working path.
    addrs.sort_by_key(|addr| addr.is_ipv6());

    let mut last_error = None;
    for server_addr in addrs {
        match discover_from(server_addr, wait).await {
            Ok(addr) => return Ok(addr.ip()),
            Err(err) => last_error = Some(err),
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("STUN discovery failed for {server}")))
}

async fn discover_from(server: SocketAddr, wait: Duration) -> Result<SocketAddr> {
    let bind_addr = if server.is_ipv6() { "[::]:0" } else { "0.0.0.0:0" };
    let socket = UdpSocket::bind(bind_addr)
        .await
        .with_context(|| format!("bind STUN discovery socket for {server}"))?;
    socket
        .connect(server)
        .await
        .with_context(|| format!("connect STUN discovery socket to {server}"))?;

    let mut transaction_id = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut transaction_id);
    let request = binding_request(transaction_id);

    let response = timeout(wait, async {
        socket.send(&request).await.context("send STUN Binding request")?;
        let mut buf = [0u8; 2048];
        let n = socket.recv(&mut buf).await.context("receive STUN Binding response")?;
        Ok::<Vec<u8>, anyhow::Error>(buf[..n].to_vec())
    })
    .await
    .with_context(|| format!("STUN timeout talking to {server}"))??;

    parse_binding_response(&response, transaction_id)
        .with_context(|| format!("parse STUN Binding response from {server}"))
}

fn binding_request(transaction_id: [u8; 12]) -> [u8; 20] {
    let mut packet = [0u8; 20];
    packet[0..2].copy_from_slice(&STUN_BINDING_REQUEST.to_be_bytes());
    packet[2..4].copy_from_slice(&0u16.to_be_bytes());
    packet[4..8].copy_from_slice(&STUN_MAGIC_COOKIE.to_be_bytes());
    packet[8..20].copy_from_slice(&transaction_id);
    packet
}

fn parse_binding_response(packet: &[u8], transaction_id: [u8; 12]) -> Result<SocketAddr> {
    if packet.len() < 20 {
        bail!("STUN response is shorter than its 20-byte header");
    }

    let msg_type = u16::from_be_bytes([packet[0], packet[1]]);
    if msg_type != STUN_BINDING_SUCCESS {
        bail!("unexpected STUN message type 0x{msg_type:04x}");
    }

    let msg_len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
    let magic = u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]);
    if magic != STUN_MAGIC_COOKIE {
        bail!("invalid STUN magic cookie 0x{magic:08x}");
    }
    if packet[8..20] != transaction_id[..] {
        bail!("STUN transaction id mismatch");
    }

    let end = 20usize
        .checked_add(msg_len)
        .ok_or_else(|| anyhow!("invalid STUN message length"))?;
    if end > packet.len() {
        bail!("truncated STUN response: header says {msg_len} attribute bytes");
    }

    let mut pos = 20usize;
    let mut mapped_fallback = None;
    while pos + 4 <= end {
        let attr_type = u16::from_be_bytes([packet[pos], packet[pos + 1]]);
        let attr_len = u16::from_be_bytes([packet[pos + 2], packet[pos + 3]]) as usize;
        let value_start = pos + 4;
        let value_end = value_start
            .checked_add(attr_len)
            .ok_or_else(|| anyhow!("invalid STUN attribute length"))?;
        if value_end > end {
            bail!("truncated STUN attribute 0x{attr_type:04x}");
        }
        let value = &packet[value_start..value_end];

        match attr_type {
            ATTR_XOR_MAPPED_ADDRESS => {
                return parse_address(value, true, transaction_id)
                    .context("parse XOR-MAPPED-ADDRESS");
            }
            ATTR_MAPPED_ADDRESS => {
                mapped_fallback = Some(
                    parse_address(value, false, transaction_id)
                        .context("parse MAPPED-ADDRESS")?,
                );
            }
            _ => {}
        }

        let padded = (attr_len + 3) & !3;
        pos = value_start
            .checked_add(padded)
            .ok_or_else(|| anyhow!("invalid STUN attribute padding"))?;
    }

    mapped_fallback.ok_or_else(|| anyhow!("STUN response has no mapped address attribute"))
}

fn parse_address(value: &[u8], xor: bool, transaction_id: [u8; 12]) -> Result<SocketAddr> {
    if value.len() < 4 {
        bail!("mapped-address attribute is too short");
    }

    let family = value[1];
    let mut port = u16::from_be_bytes([value[2], value[3]]);
    if xor {
        port ^= (STUN_MAGIC_COOKIE >> 16) as u16;
    }

    match family {
        0x01 => {
            if value.len() < 8 {
                bail!("IPv4 mapped-address attribute is too short");
            }
            let mut raw = u32::from_be_bytes([value[4], value[5], value[6], value[7]]);
            if xor {
                raw ^= STUN_MAGIC_COOKIE;
            }
            Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(raw)), port))
        }
        0x02 => {
            if value.len() < 20 {
                bail!("IPv6 mapped-address attribute is too short");
            }
            let mut raw = [0u8; 16];
            raw.copy_from_slice(&value[4..20]);
            if xor {
                let cookie = STUN_MAGIC_COOKIE.to_be_bytes();
                for i in 0..4 {
                    raw[i] ^= cookie[i];
                }
                for i in 0..12 {
                    raw[4 + i] ^= transaction_id[i];
                }
            }
            Ok(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(raw)), port))
        }
        other => bail!("unsupported STUN address family 0x{other:02x}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_xor_mapped_ipv4() {
        let txid = [0x11u8; 12];
        let expected_ip = Ipv4Addr::new(203, 0, 113, 10);
        let expected_port = 54_321u16;

        let xport = expected_port ^ ((STUN_MAGIC_COOKIE >> 16) as u16);
        let xaddr = u32::from(expected_ip) ^ STUN_MAGIC_COOKIE;

        let mut packet = Vec::new();
        packet.extend_from_slice(&STUN_BINDING_SUCCESS.to_be_bytes());
        packet.extend_from_slice(&12u16.to_be_bytes());
        packet.extend_from_slice(&STUN_MAGIC_COOKIE.to_be_bytes());
        packet.extend_from_slice(&txid);
        packet.extend_from_slice(&ATTR_XOR_MAPPED_ADDRESS.to_be_bytes());
        packet.extend_from_slice(&8u16.to_be_bytes());
        packet.extend_from_slice(&[0, 0x01]);
        packet.extend_from_slice(&xport.to_be_bytes());
        packet.extend_from_slice(&xaddr.to_be_bytes());

        let got = parse_binding_response(&packet, txid).unwrap();
        assert_eq!(got, SocketAddr::new(IpAddr::V4(expected_ip), expected_port));
    }
}
