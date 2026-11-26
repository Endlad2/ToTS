use crate::Result;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::SocketAddr;
use tokio::net::UdpSocket;

const UDP_BUFFER: usize = 4 * 1024 * 1024;

fn tune(socket: &Socket) {
    let _ = socket.set_recv_buffer_size(UDP_BUFFER);
    let _ = socket.set_send_buffer_size(UDP_BUFFER);
}

fn plain_udp(addr: SocketAddr) -> Result<UdpSocket> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_nonblocking(true)?;
    tune(&socket);
    socket.bind(&addr.into())?;
    let std_socket: std::net::UdpSocket = socket.into();
    Ok(UdpSocket::from_std(std_socket)?)
}

/// Creates a nonblocking UDP socket with larger kernel buffers. The larger buffer
/// requests are best-effort because the OS may clamp them to sysctl/system limits.
///
/// This is the *unpinned* form used by listeners (server ingress, loopback
/// bridges) where binding to a physical interface would be wrong.
pub fn bind_udp(addr: SocketAddr) -> Result<UdpSocket> {
    plain_udp(addr)
}

/// Create an outbound UDP socket pinned to the physical underlay adapter.
///
/// Client transport (direct VPS ingress, TURN/UDP) must never follow the
/// full-tunnel default route back into the ToTS TUN device. When no egress
/// interface was configured for this process this is exactly [`bind_udp`].
pub fn bind_udp_pinned(addr: SocketAddr) -> Result<UdpSocket> {
    match crate::egress::udp_socket(addr, UDP_BUFFER, UDP_BUFFER) {
        Some(socket) => {
            let socket = socket?;
            let std_socket: std::net::UdpSocket = socket.into();
            Ok(UdpSocket::from_std(std_socket)?)
        }
        None => plain_udp(addr),
    }
}

/// Create a UDP socket pinned to the underlay and connected to `remote`.
pub async fn connect_udp_pinned(remote: SocketAddr) -> Result<UdpSocket> {
    // Bind the underlay's own source address where the platform needs one
    // (Windows); loopback and explicit addresses are left untouched.
    let bind = crate::egress::outbound_bind(remote);
    let socket = match crate::egress::udp_socket_for(remote, UDP_BUFFER, UDP_BUFFER) {
        Some(socket) => {
            let socket = socket?;
            let std_socket: std::net::UdpSocket = socket.into();
            UdpSocket::from_std(std_socket)?
        }
        None => plain_udp(bind)?,
    };
    socket.connect(remote).await?;
    Ok(socket)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn binds_and_reports_the_local_address() {
        let socket = bind_udp("127.0.0.1:0".parse().unwrap()).unwrap();
        let local = socket.local_addr().unwrap();
        assert_eq!(local.ip().to_string(), "127.0.0.1");
        assert_ne!(local.port(), 0);
    }

    #[tokio::test]
    async fn pinned_socket_still_delivers_datagrams_without_an_interface() {
        // With no egress interface configured (the default in tests) the pinned
        // helper must behave as a plain socket and loopback must keep working.
        let listener = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender = connect_udp_pinned(listener.local_addr().unwrap())
            .await
            .unwrap();
        sender.send(b"ping").await.unwrap();
        let mut buffer = [0u8; 16];
        let (n, _) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            listener.recv_from(&mut buffer),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&buffer[..n], b"ping");
    }
}
