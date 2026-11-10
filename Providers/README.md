# Providers

Adapters are compiled into `tots-core` through `src/providers/mod.rs`. They return neutral bootstrap data or an owned native `DatagramPath`; the shared ToTS codec and encryption remain in core.

| Adapter | Transport |
| --- | --- |
| `vk.rs` | VK Calls bootstrap and TURN/UDP. A 12-byte RTP envelope carries each encrypted ToTS datagram; the server still accepts raw packets. |
| `jazz.rs` | Real Jazz WebSocket signalling and WebRTC RTP media through the bundled `jazz-session` library. |

Public config data is keyed by provider: `provider_data.vk` and/or `provider_data.jazz`. `provider` remains the preferred provider. VK data requires `vk_call_links` or a `vk_token`; `ice_servers` can supplement bootstrap. A `jazz: {}` entry makes the config API create a room and provision a secret client connector even when Jazz is only a fallback. Optional `join_link` uses an existing room. The database retains the invite so restart can restore active server sessions; revoke, expiry and replacement close the old bridge.

Jazz's `preconnect_call` separates HTTP provisioning from `join_preconnected`, which connects directly to the supplied WSS endpoint and media servers. This allows VPS-side HTTP provisioning when the PC cannot reach Jazz's HTTP bootstrap. SSH carries no user traffic. Both ends send public RTP keepalives; the adapter filters that exact marker before returning datagrams to ToTS.

The session explicitly publishes audio with `muted:false` and subscribes to remote unmuted AUDIO track SIDs from initial participant data and later updates. It skips local tracks, deduplicates subscriptions and unsubscribes muted tracks. Shared encrypted path probes retry at 500 ms intervals with fresh sequence numbers within the client's overall three-second deadline, tolerating startup loss during SFU renegotiation.

Paths provide endpoint IPv4 addresses for the Windows route guard and close their sessions/tasks explicitly. Keep invites, connectors and ICE credentials out of logs and documentation. TURN/TCP/TLS and IPv6 full-tunnel routing are not implemented.

`jazz-session` remains MIT-licensed with its own [LICENSE](jazz-session/LICENSE); it is a path dependency excluded from the four-member workspace. Preserve its license and root [NOTICE](../NOTICE) in the delivery archive. [Test results](../docs/TEST-RESULTS.md) record verified Windows VPN egress and graceful-exit route cleanup through both providers, plus a successful reconnect probe with the same Jazz config. The bundled library has 26 passing unit tests and one passing documentation test.
