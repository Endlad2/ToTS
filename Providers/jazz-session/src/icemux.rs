//! ICE UDP mux pinned to the physical underlay adapter.
//!
//! `webrtc-rs` creates the sockets used for ICE host candidates itself, always
//! binding them to wildcard addresses. Inside a full-tunnel VPN the wildcard
//! route points at the ToTS TUN device, so the media socket would send the media
//! stream into the tunnel that is supposed to be carrying it.
//!
//! The one supported hook in `webrtc 0.17` / `webrtc-ice 0.17.2` is
//! [`webrtc::ice::udp_network::UDPNetwork::Muxed`]: ICE then performs all local
//! candidate traffic over a single socket that *we* create. This module creates
//! that socket with the same physical-egress policy as the rest of the client
//! (`SO_BINDTODEVICE` on Linux/Android, adapter address binding elsewhere).
//!
//! When no egress interface is configured (server-side use, or a platform without
//! a usable device option) this returns `None` and the SDK keeps its default
//! ephemeral sockets.

use std::sync::Arc;

pub(crate) use webrtc::ice::udp_network::UDPNetwork;
use webrtc::ice::udp_mux::{UDPMux, UDPMuxDefault, UDPMuxParams};

use crate::egress;

/// Build a pinned UDP mux, or `None` when the default behaviour should be kept.
pub(crate) fn pinned_udp_mux() -> Option<Arc<dyn UDPMux + Send + Sync>> {
    let selected = egress::ensure()?;
    let bind = match selected.address() {
        Some(address) => std::net::SocketAddr::new(address.into(), 0),
        None => "0.0.0.0:0".parse().expect("static IPv4 bind"),
    };
    let socket = match egress::udp_socket(bind, 4 * 1024 * 1024, 4 * 1024 * 1024) {
        Some(Ok(socket)) => socket,
        Some(Err(error)) => {
            tracing::warn!(
                %error,
                interface = selected.name(),
                "ICE mux socket could not be created; falling back to ephemeral sockets"
            );
            return None;
        }
        None => return None,
    };
    let std_socket: std::net::UdpSocket = socket.into();
    let socket = match tokio::net::UdpSocket::from_std(std_socket) {
        Ok(socket) => socket,
        Err(error) => {
            tracing::warn!(%error, "ICE mux socket registration failed");
            return None;
        }
    };
    tracing::info!(
        interface = selected.name(),
        "media (ICE) socket pinned to the physical adapter"
    );
    Some(UDPMuxDefault::new(UDPMuxParams::new(socket)))
}

/// Interface filter that keeps ICE candidate gathering on real adapters only.
///
/// With a pinned mux only the selected adapter's addresses are reachable through
/// that socket, so advertising any other interface would only add dead
/// candidates.
pub(crate) fn interface_filter() -> Box<dyn Fn(&str) -> bool + Send + Sync> {
    match egress::ensure() {
        Some(selected) => {
            let name = selected.name().to_owned();
            Box::new(move |candidate: &str| candidate == name)
        }
        None => {
            let excluded = egress::excluded_interface_prefixes();
            Box::new(move |candidate: &str| {
                let lower = candidate.to_ascii_lowercase();
                !excluded.iter().any(|prefix| lower.starts_with(prefix))
            })
        }
    }
}

/// Drop the client's own tunnel address from ICE candidate gathering.
pub(crate) fn ip_filter(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ip) => {
            !ip.is_link_local()
                && !ip.is_unspecified()
                && !egress::is_tunnel_ip(std::net::IpAddr::V4(ip))
        }
        std::net::IpAddr::V6(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tunnel_addresses_are_never_gathered() {
        egress::set_tunnel(Some("tots-test1"), Some("10.55.0.7".parse().unwrap()));
        assert!(!ip_filter("10.55.0.7".parse().unwrap()));
        assert!(ip_filter("192.0.2.10".parse().unwrap()));
        assert!(!ip_filter("2001:db8::1".parse().unwrap()));
    }

    #[test]
    fn loopback_is_never_a_media_candidate_adapter() {
        let filter = interface_filter();
        assert!(!filter("lo"));
        assert!(filter("eth0"));
    }
}
