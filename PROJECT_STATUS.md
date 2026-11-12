# Project status

## Implemented in this source snapshot

- Multi-provider `provider_data` (`vk` / `jazz`) with a preferred `provider` and fallback order.
- Client `--n` worker pool, per-worker session IDs, health logs and five-second TUN traffic/worker telemetry.
- Worker recovery: preferred-provider retries, fallback-provider cycling, path re-probing on liveness/sustained-RTT degradation and authenticated session/path resumption.
- Server multi-session routing per device so workers do not overwrite one another.
- ACK ranges and RTT diagnostics; congestion control, pacing and bounded retransmission for reliable control. Best-effort TCP/UDP forwarding has no duplicate outer ACK window or pacer.
- Symmetric client/server flowlet scheduling: small UDP pinning, automatic promotion of high-rate UDP to bulk, ~16 KiB bulk striping, and adaptive per-allocation scheduling weights based on ~247 kB/s (~2 Mbit/s).
- Reliable/unreliable transport policy (TCP/UDP default to low-latency unreliable recovery; stale queued unreliable packets are dropped after outages; control traffic can be recovered).
- 1200-byte encrypted datagram budget and best-effort 4 MiB UDP socket buffers.
- Client configuration is passed as `--config tots://...` or explicit CLI fields; local secret-config loading is not part of the CLI.
- Shared off-runtime device key derivation, bounded authenticated admission, independent server egress queues and periodic authorization refresh.
- Paced VK credential acquisition and independent Jazz room/bridge pools for up to 50 workers; startup waits for the complete requested pool.
- Lazy frame sealing preserves the existing 128-packet replay check when data waits behind control packets.

## Validation status

The recorded workspace run on 2026-11-12 passed 109 tests (64 core, 14 client, 31 server), with one Administrator/Wintun test ignored in the ordinary run. Workspace Clippy passed with warnings denied. Jazz SDK checks passed 42 unit tests, two example tests and one doc test; ten provider tests passed. These recorded checks precede draft packaging; packaging does not launch another live VPN or Administrator test.

Both VK and Jazz have connected 50 independent encrypted workers in earlier bounded Windows runs and returned the Linux server's public IPv4 address. Later starts also timed out with incomplete pools, so repeatable startup is still unresolved. The operator subsequently reported a continuously enabled background VPN, and running WireSock services were observed. Speed measurements are provisional until that additional path is disabled or a ToTS bypass is independently verified. The 40 Mbit/s acceptance target is not achieved; consult [the throughput report](docs/THROUGHPUT-RESULTS.md) for measurements and limitations. Historical single-worker results remain in [the earlier report](docs/TEST-RESULTS.md).

IPv6 routing is not managed. Reconnected provider endpoints can require a client restart because bypass routes are installed from the initial pool. Permanently exited native bridges are not automatically replaced. Bounded tests do not establish long-duration stability.
