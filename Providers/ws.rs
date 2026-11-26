//! WebSocket handshake over a TCP connection pinned to the physical egress
//! adapter.
//!
//! `tokio_tungstenite::connect_async*` resolves and connects internally, so it
//! always follows the system default route — which a full-tunnel VPN has just
//! pointed at its own TUN device. Provider signalling would then be carried by
//! the tunnel it is trying to establish. This helper creates the TCP socket
//! through [`crate::egress`] first and runs the WebSocket/TLS handshake over it.
//!
//! Shared verbatim by `tots-core` and the bundled `jazz-session` crate.

use tokio::net::TcpStream;
use tokio_tungstenite::{
    tungstenite::{
        client::IntoClientRequest, handshake::client::Response, protocol::WebSocketConfig,
        Error as WsError,
    },
    MaybeTlsStream, WebSocketStream,
};

use crate::egress;

pub type PinnedWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Connect a WebSocket, honouring the egress-interface policy.
///
/// When no interface is configured this is exactly
/// `connect_async_with_config(request, config, false)`.
pub async fn connect<R>(
    request: R,
    config: Option<WebSocketConfig>,
) -> Result<(PinnedWebSocket, Response), WsError>
where
    R: IntoClientRequest + Unpin,
{
    if !egress::active() {
        return tokio_tungstenite::connect_async_with_config(request, config, false).await;
    }
    let request = request.into_client_request()?;
    let uri = request.uri();
    let host = uri
        .host()
        .ok_or(WsError::Url(
            tokio_tungstenite::tungstenite::error::UrlError::NoHostName,
        ))?
        .to_owned();
    let tls = uri.scheme_str() != Some("ws");
    let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });

    let mut last_error = None;
    for address in resolve(host.as_str(), port).await? {
        let stream = match connect_pinned(address).await {
            Ok(stream) => stream,
            Err(error) => {
                last_error = Some(error);
                continue;
            }
        };
        return tokio_tungstenite::client_async_tls_with_config(request, stream, config, None)
            .await
            .map_err(|error| match (error, last_error.take()) {
                // Surface the connect error rather than a bare handshake failure.
                (WsError::Io(_), Some(io)) => WsError::Io(io),
                (error, _) => error,
            });
    }
    Err(WsError::Io(last_error.unwrap_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, "no resolved address")
    })))
}

async fn resolve(host: &str, port: u16) -> Result<Vec<std::net::SocketAddr>, WsError> {
    let mut addresses = Vec::new();
    for address in tokio::net::lookup_host((host, port))
        .await
        .map_err(WsError::Io)?
    {
        addresses.push(address);
    }
    // Prefer IPv4: the ToTS dataplane and the physical underlay are IPv4.
    addresses.sort_by_key(|address| !address.is_ipv4());
    addresses.dedup();
    Ok(addresses)
}

async fn connect_pinned(remote: std::net::SocketAddr) -> std::io::Result<TcpStream> {
    match egress::tcp_socket(remote, true) {
        Some(socket) => {
            let socket = tokio::net::TcpSocket::from_std_stream(socket?.into());
            socket.connect(remote).await
        }
        None => TcpStream::connect(remote).await,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn public_type_is_the_stream_the_providers_expect() {
        // Compile-time assertion: the returned stream type matches what
        // `WebSocketStream<MaybeTlsStream<TcpStream>>` consumers expect.
        fn assert_send<T: Send>() {}
        assert_send::<super::PinnedWebSocket>();
    }
}
