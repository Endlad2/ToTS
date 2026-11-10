use crate::Result;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::SocketAddr;
use tokio::net::UdpSocket;

const UDP_BUFFER: usize = 4 * 1024 * 1024;

/// Creates a nonblocking UDP socket with larger kernel buffers. The larger buffer
/// requests are best-effort because the OS may clamp them to sysctl/system limits.
pub fn bind_udp(addr: SocketAddr) -> Result<UdpSocket> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_nonblocking(true)?;
    let _ = socket.set_recv_buffer_size(UDP_BUFFER);
    let _ = socket.set_send_buffer_size(UDP_BUFFER);
    socket.bind(&addr.into())?;
    let std_socket: std::net::UdpSocket = socket.into();
    Ok(UdpSocket::from_std(std_socket)?)
}
