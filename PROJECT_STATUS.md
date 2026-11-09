# Project status

This repository is a clean-room ToTS 0.1 source implementation.

Implemented:
- four-crate Rust workspace (`tots-core`, `tots-client`, `tots-ios`, `tots-server`);
- original encrypted packet format, packet IDs, fragmentation/reassembly, replay window, heartbeat/probe frames and RTT path selection;
- VK provider bootstrap and parsing of provider ICE/TURN data;
- STUN codec and TURN/UDP Allocate -> authentication -> CreatePermission -> ChannelBind -> ChannelData path;
- separate server UDP ingress ports for the direct/provider-assisted path and TURN-relayed path;
- TUN dataplane, Android raw VPN-fd support and iOS C ABI/staticlib surface;
- loopback config API, SQLite persistence, `tots://BASE64URL(JSON)` configs, expiry/revoke handling;
- GitHub Actions CI/release workflow, systemd unit and preserving/upgrading install script.

Known gap before calling the WebRTC side production-complete:
- the direct `webrtc_port` path currently performs ToTS transport directly over UDP after provider bootstrap/path probing. A full RFC 5764 DTLS-SRTP media layer / PeerConnection termination is not yet wired into the dataplane. `ProviderBootstrap.signalling` and ICE/STUN data are retained specifically so that layer can be added without changing the public config format.
- TURN/TCP and TURN/TLS fallback are not implemented; TURN/UDP is implemented.

Do not represent these two items as implemented until they are actually wired and covered by integration tests.
