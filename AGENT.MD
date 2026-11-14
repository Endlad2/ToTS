# AGENT.MD — ToTS architecture contract

This file records the architecture and ownership rules for repository changes.

## Workspace and ownership

Keep exactly four workspace crates:

1. `tots-core`: original packet protocol, AEAD, replay protection, fragmentation, flow control, provider interfaces and transport paths. No UI or server-storage dependencies.
2. `tots-client`: CLI/config loading, TUN integration and owned Windows route/DNS changes.
3. `tots-ios`: C ABI/static library calling core logic for a NetworkExtension host.
4. `tots-server`: SQLite, management API, Linux TUN, provider-room bridges and system integration.

Provider adapters live in top-level `Providers/` and are included by `tots-core/src/providers/mod.rs`. `Providers/jazz-session` is the bundled MIT path dependency, excluded from workspace membership. Preserve its license and attribution when redistributing source or binaries. Workspace/Jazz Rust-version metadata is `1.89`; stable `1.99.1` was tested. Retain `webrtc-ice 0.17.2` in the workspace lock and build with `--locked`: 0.17.3 applies a 100 ms STUN write deadline to relay writes that may still be awaiting a TURN CreatePermission round trip. A physical-network A/B changing only ICE restored both Jazz PeerConnections; keep this lock until upstream makes TURN permission setup cancellation-safe.

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

`--n` controls 1..256 independent client workers. `--workers_percent` optionally apportions each slot's *preferred* configured provider (values sum to 100); the rest of the configured providers remain that slot's failover order. Omitting it keeps primary-first behavior. Every worker gets a distinct `session_id`; the server stores multiple sessions per `device_id` and schedules return traffic without the old last-route-wins conflict. Do not replace flowlet scheduling with permanent 5-tuple pinning: bulk traffic must be able to use multiple healthy sessions, while small UDP must remain sticky unless its measured short-window load exceeds one worker-class capacity. A failed worker retries its preferred provider first, then configured fallbacks, then cycles back. Do not make workers share a SessionEngine.

Derive the Argon2 device key once, off the async runtime, and reuse it across independent SessionEngines. Server admission must authenticate before registering a bounded session; key derivation must not block shared UDP ingress. Keep per-session bounded outbound queues and independent pacing/recovery tasks. Congestion credit may prefer a worker at a new flowlet boundary; it does not make a live worker disappear midway through a flowlet. Established server sessions revalidate device authorization every ten seconds.

VK bootstrap is single-flight and generations remain spaced by three seconds, but one generation may lease credentials to up to seven worker allocations. Each leased worker still owns a distinct UDP socket, TURN allocation and SessionEngine. Connection/probe setup is bounded to seven workers concurrently and candidates inside one worker are probed concurrently. Retrying rotates endpoints; generation fetches rotate supplied call links. Repeated flood failures back off up to five minutes. Cleanup owns created calls at pool level, after workers stop. Jazz API input `worker_count` is 1..50 (default 1). Persist ordered `worker_rooms` with distinct invite/connector pairs; legacy fields alias slot zero. Because every slot may fail over to Jazz, provision `worker_count >= --n` whenever Jazz is in a multi-provider config. Provision at most four rooms concurrently, with at most three attempts per room; roll back every opened bridge on failure. Restore, revoke, replacement and expiry operate on the complete pool. A one-room legacy config supports one Jazz worker. Client startup waits for all requested workers before installing full-tunnel routes, within `min(45 + 5*N, 300)` seconds.

Best-effort TCP/UDP forwarding bypasses the outer ACK credit and pacing budget; inner transports retain congestion control. Bound its queues and keep at most 512 payload-free ACK diagnostic samples per worker. Range ACKs are delayed only briefly (currently up to 2 ms) and batched (currently up to 32 packet IDs) so RX hot paths do not await one transport ACK send per IP packet. On the server, UDP ingress must not await a TUN enqueue for every packet; use the bounded ingress queue and keep probe/control replies explicit. TURN datapaths reuse send/receive packet buffers; do not reintroduce per-datagram large receive allocations. Reliable control retains congestion control, pacing, loss recovery and retransmission. Neither mode changes AEAD or replay checks. Prepare plaintext frames locally, wait for any reliable send credit, then seal immediately before each send so intervening ACKs cannot age a queued sequence out of the replay window.

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

Keep public structs serde-compatible unless the config protocol version changes. Test wire formats, config/lifecycle changes and route ownership at their actual boundaries. Record current multi-worker provider evidence in `docs/THROUGHPUT-RESULTS.md`; `docs/TEST-RESULTS.md` preserves earlier single-worker evidence. A direct UDP probe or unit test does not prove provider VPN egress. Keep release packaging reproducible: GitHub Actions publishes the Linux server and Windows client assets, while source archives keep instructions, reports and licenses. A PR is not required for local source delivery.

## OpenFlux carrier boundary

`Providers/openflux.rs` and `tots-core/src/transport/socks5.rs` implement an Apache-2.0 adapter around a separately installed OpenFlux executable. Supported provider names are `yandex`, `vyandex`, `boards`, `cupsonline`, and `mailru`. Do not copy OpenFlux GPLv3 transport implementation files into this workspace. The adapter launches/uses an OpenFlux client in SOCKS5 mode and carries ToTS encrypted UDP datagrams through SOCKS5 UDP ASSOCIATE. Keep ToTS session IDs, crypto, recovery, flow scheduling and provider fallback outside the sidecar.

OpenFlux provider data requires `url`. Optional local runtime fields are `socks5_addr`, `target`, `auto_launch`, `openflux_binary`, `yandex_cookies_file` (`vyandex` only), `cookie_store`, and `route_hosts`. Prefer CLI `--openflux-binary` and `--yandex-cookies-file` for machine-local paths so server-issued `tots://` URIs stay portable. `target` defaults to the ToTS server `peer:turn_port`; do not assume loopback is valid for every OpenFlux exit mode.

`--workers_percent` is generic across every `ProviderName`, not just VK/Jazz. When adding a provider, update `ProviderName::ALL`, config validation, provider construction, server DB persistence, worker telemetry and any keyed-provider migration detection. Jazz remains the only provider that requires one pre-provisioned room per possible worker fallback.
