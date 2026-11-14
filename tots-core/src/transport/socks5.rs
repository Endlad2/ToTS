use crate::{Error, Result};
use async_trait::async_trait;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{lookup_host, TcpStream, UdpSocket},
};

use super::DatagramPath;

/// A datagram path carried through a local SOCKS5 UDP ASSOCIATE proxy.
///
/// ToTS uses this for external carrier sidecars such as OpenFlux. The SOCKS5
/// control connection is intentionally kept open for the lifetime of the UDP
/// association, as required by RFC 1928.
pub struct Socks5UdpPath {
    name: String,
    _control: TcpStream,
    udp: UdpSocket,
    target: SocketAddr,
    route_ips: Vec<Ipv4Addr>,
}

impl Socks5UdpPath {
    pub async fn connect(
        name: impl Into<String>,
        proxy: SocketAddr,
        target: SocketAddr,
        route_ips: Vec<Ipv4Addr>,
    ) -> Result<Self> {
        let mut control = TcpStream::connect(proxy).await?;
        control.write_all(&[0x05, 0x01, 0x00]).await?;
        let mut greeting = [0u8; 2];
        control.read_exact(&mut greeting).await?;
        if greeting != [0x05, 0x00] {
            return Err(Error::Transport(format!(
                "SOCKS5 proxy rejected no-authentication mode: {:02x?}",
                greeting
            )));
        }

        let request = [0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0];
        control.write_all(&request).await?;
        let relay = read_reply(&mut control, proxy.ip()).await?;
        let bind = match relay {
            SocketAddr::V4(_) => "0.0.0.0:0",
            SocketAddr::V6(_) => "[::]:0",
        };
        let udp = UdpSocket::bind(bind).await?;
        udp.connect(relay).await?;

        Ok(Self {
            name: name.into(),
            _control: control,
            udp,
            target,
            route_ips,
        })
    }
}

#[async_trait]
impl DatagramPath for Socks5UdpPath {
    fn name(&self) -> &str {
        &self.name
    }

    fn route_ips(&self) -> Vec<Ipv4Addr> {
        self.route_ips.clone()
    }

    async fn send(&self, data: &[u8]) -> Result<()> {
        let mut packet = Vec::with_capacity(data.len() + 22);
        packet.extend_from_slice(&[0, 0, 0]);
        append_addr(&mut packet, self.target);
        packet.extend_from_slice(data);
        let sent = self.udp.send(&packet).await?;
        if sent != packet.len() {
            return Err(Error::Transport("short SOCKS5 UDP send".into()));
        }
        Ok(())
    }

    async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        let mut packet = vec![0u8; buf.len().saturating_add(300)];
        let received = self.udp.recv(&mut packet).await?;
        let payload = udp_payload(&packet[..received])?;
        if payload.len() > buf.len() {
            return Err(Error::Transport(
                "SOCKS5 UDP payload exceeds receive buffer".into(),
            ));
        }
        buf[..payload.len()].copy_from_slice(payload);
        Ok(payload.len())
    }
}

async fn read_reply(stream: &mut TcpStream, proxy_ip: IpAddr) -> Result<SocketAddr> {
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).await?;
    if header[0] != 0x05 {
        return Err(Error::Transport("invalid SOCKS5 reply version".into()));
    }
    if header[1] != 0x00 {
        return Err(Error::Transport(format!(
            "SOCKS5 UDP ASSOCIATE failed with status 0x{:02x}",
            header[1]
        )));
    }
    if header[2] != 0x00 {
        return Err(Error::Transport("invalid SOCKS5 reserved byte".into()));
    }

    let ip = match header[3] {
        0x01 => {
            let mut bytes = [0u8; 4];
            stream.read_exact(&mut bytes).await?;
            IpAddr::V4(Ipv4Addr::from(bytes))
        }
        0x04 => {
            let mut bytes = [0u8; 16];
            stream.read_exact(&mut bytes).await?;
            IpAddr::V6(std::net::Ipv6Addr::from(bytes))
        }
        0x03 => {
            let mut len = [0u8; 1];
            stream.read_exact(&mut len).await?;
            let mut name = vec![0u8; len[0] as usize];
            stream.read_exact(&mut name).await?;
            let name = std::str::from_utf8(&name)
                .map_err(|_| Error::Transport("invalid SOCKS5 relay hostname".into()))?;
            let mut port_bytes = [0u8; 2];
            stream.read_exact(&mut port_bytes).await?;
            let port = u16::from_be_bytes(port_bytes);
            let mut resolved = lookup_host((name, port)).await?;
            return resolved
                .next()
                .ok_or_else(|| Error::Transport("SOCKS5 relay hostname resolved empty".into()));
        }
        _ => return Err(Error::Transport("unsupported SOCKS5 relay address type".into())),
    };
    let mut port = [0u8; 2];
    stream.read_exact(&mut port).await?;
    let mut address = SocketAddr::new(ip, u16::from_be_bytes(port));
    if address.ip().is_unspecified() {
        address.set_ip(proxy_ip);
    }
    Ok(address)
}

fn append_addr(out: &mut Vec<u8>, address: SocketAddr) {
    match address.ip() {
        IpAddr::V4(ip) => {
            out.push(0x01);
            out.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            out.push(0x04);
            out.extend_from_slice(&ip.octets());
        }
    }
    out.extend_from_slice(&address.port().to_be_bytes());
}

fn udp_payload(packet: &[u8]) -> Result<&[u8]> {
    if packet.len() < 4 || packet[0] != 0 || packet[1] != 0 || packet[2] != 0 {
        return Err(Error::Transport("invalid or fragmented SOCKS5 UDP packet".into()));
    }
    let address_len = match packet[3] {
        0x01 => 4,
        0x04 => 16,
        0x03 => {
            let Some(len) = packet.get(4) else {
                return Err(Error::Transport("truncated SOCKS5 UDP hostname".into()));
            };
            1 + *len as usize
        }
        _ => return Err(Error::Transport("unsupported SOCKS5 UDP address type".into())),
    };
    let offset = 4usize
        .checked_add(address_len)
        .and_then(|v| v.checked_add(2))
        .ok_or_else(|| Error::Transport("SOCKS5 UDP header overflow".into()))?;
    packet
        .get(offset..)
        .ok_or_else(|| Error::Transport("truncated SOCKS5 UDP packet".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ipv4_udp_payload() {
        let packet = [0, 0, 0, 1, 127, 0, 0, 1, 0x1e, 0x61, b'o', b'k'];
        assert_eq!(udp_payload(&packet).unwrap(), b"ok");
    }

    #[test]
    fn rejects_fragmented_udp_packet() {
        let packet = [0, 0, 1, 1, 127, 0, 0, 1, 0, 1, b'x'];
        assert!(udp_payload(&packet).is_err());
    }
}
