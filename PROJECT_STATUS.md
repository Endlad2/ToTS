# Project status

## Implemented in this source snapshot

- Multi-provider `provider_data` (`vk`, `jazz`, `yandex`, `vyandex`, `boards`, `cupsonline`, `mailru`) with a preferred `provider` and fallback order.
- Client `--n` worker pool, optional `--workers_percent` preferred-provider apportionment, per-worker session IDs, health logs and five-second TUN traffic/worker telemetry (including preferred and actual provider splits).
- OpenFlux carrier adapter for Yandex.Docs, Yandex Volga, Yandex Board, Cups.online and Mail.ru Docs. It uses a separate GPLv3 OpenFlux process over SOCKS5 UDP ASSOCIATE; no OpenFlux source is copied into the Apache workspace.
- Worker recovery: preferred-provider retries, fallback-provider cycling, path re-probing on liveness/sustained-RTT degradation and authenticated session/path resumption.
- Server multi-session routing per device so workers do not overwrite one another.
- ACK ranges and RTT diagnostics; congestion control, pacing and bounded retransmission for reliable control. Best-effort TCP/UDP forwarding has no duplicate outer ACK window or pacer.
- Symmetric client/server flowlet scheduling: small UDP pinning, automatic promotion of high-rate UDP to bulk, ~16 KiB bulk striping, and adaptive per-allocation scheduling weights based on ~247 kB/s (~2 Mbit/s).
- Reliable/unreliable transport policy (TCP/UDP default to low-latency unreliable recovery; stale queued unreliable packets are dropped after outages; control traffic can be recovered).
- 1200-byte encrypted datagram budget and best-effort 4 MiB UDP socket buffers.
- Client configuration is passed as `--config tots://...` or explicit CLI fields; local secret-config loading is not part of the CLI.
- Shared off-runtime device key derivation, bounded authenticated admission, independent server egress queues and periodic authorization refresh.
- Paced VK credential acquisition with seven-worker credential cohorts, seven-way bounded concurrent worker connection/probing, and independent Jazz room/bridge pools for up to 50 workers; startup waits for the complete requested pool.
- Lazy frame sealing preserves the existing 128-packet replay check when data waits behind control packets.
- Delayed ACK-range batching (up to 32 packet IDs / ~2 ms) on client and server, a non-blocking server UDP→TUN ingress hot path with a 16K-packet bounded queue, and reusable TURN send/receive buffers to remove per-datagram allocator churn.

## Validation status

The Sasha baseline recorded a workspace run on 2026-11-12 that passed 109 tests (64 core, 14 client, 31 server), with one Administrator/Wintun test ignored in the ordinary run; Workspace Clippy passed with warnings denied. Jazz SDK checks passed 42 unit tests, two example tests and one doc test; ten provider tests passed. The `--workers_percent`, seven-way startup and ACK hot-path changes in this archive were added after that recorded run. This packaging environment has no Rust toolchain and no live VK/Jazz endpoint, so those new changes are structurally checked here but must still pass GitHub CI and a real throughput run before their speed gain is treated as measured.

Both VK and Jazz have connected 50 independent encrypted workers in earlier bounded Windows runs and returned the Linux server's public IPv4 address. Later starts also timed out with incomplete pools, so repeatable startup is still unresolved. The operator subsequently reported a continuously enabled background VPN, and running WireSock services were observed. Speed measurements are provisional until that additional path is disabled or a ToTS bypass is independently verified. The prior 40 Mbit/s acceptance target was not achieved in the recorded baseline. This revision specifically removes serial startup tails and per-packet ACK/TUN awaits, but no fixed “+10 Mbit/s” improvement is claimed without a clean live benchmark because VK relay capacity, RTT and loss dominate the result.

IPv6 routing is not managed. Reconnected provider endpoints can require a client restart because bypass routes are installed from the initial pool. Permanently exited native bridges are not automatically replaced. Bounded tests do not establish long-duration stability.

The new OpenFlux carrier adapter was added after the recorded Sasha baseline and is not live-tested in this packaging environment. It requires a separately installed OpenFlux binary and a matching OpenFlux exit. Full-tunnel route bypass resolves configured/known carrier hosts before installing the TUN route; dynamic service balancer changes may require explicit `route_hosts` or a platform-specific sidecar bypass.
