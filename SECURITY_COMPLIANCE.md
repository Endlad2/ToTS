# Transport and license boundary

ToTS does not implement DPI-specific camouflage. In particular this tree does
not intentionally generate orphan SRTP, fake STUN consent traffic, one-byte
`0xFF` keepalives, or protocol signatures designed around a specific filtering
rule.


## Default application data plane

The default application-facing mode is a local SOCKS5 proxy. TCP CONNECT and
UDP ASSOCIATE traffic is multiplexed through a nested standards-based QUIC
session, whose packets are carried by the already authenticated provider/WebRTC
transport. Domain-form SOCKS5 destinations are resolved by `tots-server`. The
normal proxy mode does not install a TUN interface or replace the operating
system default route.

## Transport model

- Direct ToTS WebRTC uses normal ICE, DTLS and SCTP/DataChannel negotiation.
- Jazz uses the provider-native room/media implementation already present in
  ToTS and keeps its dynamic endpoint bypass watcher.
- VK supports both the anonymous join-link credential flow and the authenticated
  `messages.getCallToken` / Calls flow.
- MAX supports the web token flow (`6 -> 19 -> 158`) and the Calls conversation
  flow used to obtain TURN parameters.
- VK call-scoped TURN credentials are fed to relay-only WebRTC ICE. The ICE
  implementation owns TURN allocation/permission/consent, then WebRTC performs
  DTLS and SCTP before the ToTS DataChannel opens. HTTP offer/answer signaling
  uses the configured `webrtc_port`.
- MAX keeps the existing generic RFC TURN bootstrap toward the configured ToTS
  `turn_port`, followed by the normal WebRTC DataChannel setup.

For VK, the ToTS server therefore needs its WebRTC signaling port reachable and
a public ICE candidate reachable from the provider TURN service. MAX still
requires the configured UDP `turn_port` to be publicly reachable.

## Clean-room boundary

The supplied MRTT reference is licensed under PolyForm Noncommercial 1.0.0.
ToTS remains independently implemented under its existing MIT OR Apache-2.0
workspace licensing. The reference was consulted for high-level, externally
observable interoperability facts (API endpoints, method/opcode sequence,
field names and transport architecture); its implementation source is not copied
into this project.

## VK CAPTCHA helper and TURN ordering

The VK CAPTCHA helper is a user-interactive browser bridge only: it opens an allow-listed HTTPS VK challenge and returns the success token produced after the user completes the provider challenge. It contains no automatic CAPTCHA solver or browser-fingerprint spoofing.

VK TURN credentials are treated as provider-session ICE credentials. ToTS uses relay-only WebRTC so TURN allocation/permission establishment precedes DTLS/SCTP/DataChannel traffic. The VK path does not use the legacy one-byte `0xFF` keepalive or emit raw ToTS application frames as the first packet of a TURN allocation.
