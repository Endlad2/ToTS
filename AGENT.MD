# AGENT.MD — ToTS architecture contract

This file records the architecture and ownership rules for repository changes.

## Workspace and ownership

Keep exactly four workspace crates:

1. `tots-core`: original packet protocol, AEAD, replay protection, fragmentation, flow control, provider interfaces and transport paths. No UI or server-storage dependencies.
2. `tots-client`: CLI/config loading, TUN integration and owned Windows route/DNS changes.
3. `tots-ios`: C ABI/static library calling core logic for a NetworkExtension host.
4. `tots-server`: SQLite, management API, Linux TUN, provider-room bridges and system integration.

Provider adapters live in top-level `Providers/` and are included by `tots-core/src/providers/mod.rs`. `Providers/jazz-session` is the bundled MIT path dependency, excluded from workspace membership. Preserve its license and attribution when redistributing source or binaries. Workspace/Jazz Rust-version metadata is `1.89`; stable `1.99.1` was tested.

## Packet flow

Client TUN → stable five-tuple classifier → `FlowScheduler` → per-worker `SessionEngine` → congestion/pacing/recovery → encrypted ToTS datagram → worker `DatagramPath` → server authenticated multi-session dataplane → symmetric `FlowScheduler` → server TUN → Internet. Small UDP stays pinned for latency; bulk TCP/UDP rotates only at flowlet boundaries.

Keep separate UDP ingress ports:

- `webrtc_port` (default 7800): direct/provider-assisted UDP path; this path itself is not a PeerConnection.
- `turn_port` (default 7801): provider-relayed ingress, also used locally by the Jazz room bridge.

VK TURN/UDP requires the 12-byte RTP envelope from `transport/rtp.rs` around encrypted ToTS bytes. The server recognizes RTP-wrapped packets and still accepts raw ToTS packets. Jazz uses a real WebRTC RTP media session and bridges its recovered datagrams to the existing server ingress; do not duplicate the packet codec, encryption or TUN routing there.

Each worker probes healthy candidates and selects by measured RTT. Retry probes every 500 ms with fresh authenticated sequence numbers under the client's overall three-second deadline; packet loss or malformed replies must not reset that deadline. A five-second liveness probe detects dead paths; sustained severe RTT degradation also restarts the worker so direct/WebRTC-side and TURN candidates are re-probed. Reconnect keeps the SessionEngine/sequence space so the first authenticated datagram can immediately rebind the server route (0-RTT-like path resumption). `--relay-only` excludes direct VPS/WebRTC-side UDP; `--probe-only` verifies that at least one worker is healthy without TUN. Native Jazz connections use the same `DatagramPath` boundary.

## Provider and session contract

`Provider::bootstrap` returns provider-neutral TURN/STUN/signalling data. `Provider::connect_native` optionally returns an owned `Arc<dyn DatagramPath>`. Each path supplies its provider endpoint `route_ips` and an asynchronous `close` operation.

- `Providers/vk.rs`: anonymous call bootstrap, optional call creation, ICE/TURN acquisition and provider cleanup.
- `Providers/jazz.rs`: `create_room`, `join_room`, `preconnect`, native client connection, endpoint discovery and session ownership.
- `Providers/jazz-session`: guest room/signalling/WebRTC implementation, including `preconnect_call` and `join_preconnected`.

Jazz provisioning HTTP may run on the VPS while the PC connects directly to Jazz WSS/media. SSH carries setup/configuration only, never VPN data. The public RTP keepalive marker is filtered before returning ToTS datagrams. Abort its task and close the call on setup failure, normal close and drop.

The client waits for media readiness and a remote track; server room creation waits only for media readiness so a room can exist before a client joins. Keep sessions alive while their paths are in use.

Match current Jazz signalling: publish microphone audio with `muted:false`. Discover remote AUDIO track SIDs from `rtc:join.join.otherParticipants` and `rtc:participants:update.update.participants`; skip the local identity, deduplicate subscriptions, and unsubscribe muted tracks. Bound subscription state. Send the confirmed `rtc:track:subscription:change` command rather than relying on automatic subscription.

`provider_data` is provider-keyed JSON: `{"vk": {...}, "jazz": {...}}`. `provider` names the preferred provider; additional keys are fallbacks. VK requires `vk_call_links` or `vk_token`; explicit `ice_servers` supplements bootstrap. `jazz: {}` makes the API create a room, and optional `join_link` joins an existing room. The API inserts the invite and secret `connector_url` into `provider_data.jazz`. Legacy flat single-provider rows are accepted only as a migration path.

`--n` controls 1..256 independent client workers. Every worker gets a distinct `session_id`; the server stores multiple sessions per `device_id` and schedules return traffic without the old last-route-wins conflict. Do not replace flowlet scheduling with permanent 5-tuple pinning: bulk traffic must be able to use multiple healthy sessions, while small UDP must remain sticky unless its measured short-window load exceeds one worker-class capacity. A failed worker retries the preferred provider first, then configured fallbacks, then cycles back to the preferred provider. Do not make workers share a SessionEngine.

## Storage and lifecycle

`tots-server/src/db.rs` owns SQLite. The default database is `/var/lib/tots/tots.db`; preserve it and `/etc/tots/server.env` on upgrades. The management API defaults to loopback.

Persist Jazz invites in provider data. Restart restores active rooms. Revoke or provider replacement closes the previous bridge; periodic expiry maintenance closes expired room bridges. Each bridge owns its task and native path. Serialize lifecycle changes so a delayed restore cannot replace a newer configuration.

The client accepts a server-issued URI with `--config tots://...` or explicit CLI fields; it does not load secret configuration from a local config file. Windows `--tun-default-route` uses a guard that owns only its route/DNS changes. Pin VPS/provider IPv4 endpoints through the physical gateway before installing full-tunnel routes. Normal exit, Ctrl+C and `--run-for-seconds` perform cleanup. Do not claim cleanup after force-killing a process. IPv6 routes are not managed; TURN/TCP/TLS is not implemented.

## Security invariants

- Never send the config password or derived key as transport payload.
- AEAD-protect all ToTS `Frame` bytes before they leave a process.
- Keep UUID/session/sequence envelope metadata authenticated; enforce replay checks before applying packets to TUN.
- Do not log passwords, config URIs, invites, connector tokens, TURN credentials, VK tokens or decrypted packet bytes.
- Use static/sanitized provider errors; provider response bodies and URLs can contain secrets.
- Keep management API exposure and bearer-token configuration explicit.
- Bound queues, maps, fragmentation and reassembly for untrusted input.

## File map

| File | Responsibility |
| --- | --- |
| `tots-core/src/config.rs` | Public config and URI codec |
| `tots-core/src/crypto.rs`, `protocol/` | Key derivation, AEAD, original frame/envelope format |
| `tots-core/src/fragment.rs`, `reassembly.rs`, `flow.rs` | Safe datagram sizing, bounded reassembly, five-tuple associations and reliability classification |
| `tots-core/src/recovery.rs` | Congestion window, pacing, RTT, ACK processing, loss detection and bounded retransmission |
| `tots-core/src/flow.rs` | Five-tuple classification, small-UDP pinning, ~16 KiB bulk flowlets and adaptive ~2 Mbit/s-per-allocation scheduling weights |
| `tots-core/src/session.rs` | Per-worker packet engine, ACK-range tracking and replay/delivery deduplication |
| `tots-core/src/transport/` | Datagram paths, tuned UDP sockets, STUN/TURN, RTP envelope and probes |
| `tots-client/src/main.rs`, `tun_io.rs`, `workers.rs`, `routes.rs` | Client lifecycle, TUN, worker pool/failover and Windows routing |
| `tots-ios/src/lib.rs` | NetworkExtension C ABI |
| `tots-server/src/api.rs`, `db.rs`, `bridge.rs`, `dataplane.rs` | Config lifecycle, persistence, provider bridge and UDP/TUN routing |
| `scripts/`, `systemd/` | Installation, Linux NAT and service integration |

Keep public structs serde-compatible unless the config protocol version changes. Test wire formats, config/lifecycle changes and route ownership at their actual boundaries. Record live provider evidence in `docs/TEST-RESULTS.md`; a direct UDP probe or unit test does not prove provider VPN egress. Current recorded checks are 22 workspace tests (5 client, 10 core, 7 server), a separately verified elevated MTU test, and 26 Jazz unit tests plus one doc test. Keep release packaging reproducible: GitHub Actions publishes the Linux server and Windows client assets, while source archives keep instructions, reports and licenses. A PR is not required for local source delivery.
