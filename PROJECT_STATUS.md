# Project status

## Implemented in this source snapshot

- Multi-provider `provider_data` (`vk` / `jazz`) with a preferred `provider` and fallback order.
- Client `--n` worker pool, per-worker session IDs, health logs and five-second TUN traffic/worker telemetry.
- Worker recovery: preferred-provider retries, fallback-provider cycling, path re-probing on liveness/sustained-RTT degradation and authenticated session/path resumption.
- Server multi-session routing per device so workers do not overwrite one another.
- ACK ranges, congestion control, pacing, RTT estimation, loss detection and bounded reliable retransmission.
- Symmetric client/server flowlet scheduling: small UDP pinning, automatic promotion of high-rate UDP to bulk, ~16 KiB bulk striping, and adaptive per-allocation scheduling weights based on ~247 kB/s (~2 Mbit/s).
- Reliable/unreliable transport policy (TCP/UDP default to low-latency unreliable recovery; stale queued unreliable packets are dropped after outages; control traffic can be recovered).
- 1200-byte encrypted datagram budget and best-effort 4 MiB UDP socket buffers.
- Client configuration is passed as `--config tots://...` or explicit CLI fields; local secret-config loading is not part of the CLI.

## Validation status

The live VK/Jazz/TUN results in `docs/TEST-RESULTS.md` predate the new worker/recovery changes. This archive was structurally checked in the generation environment, but no Rust toolchain was available there, so `cargo check`, `cargo test`, Clippy and live provider throughput tests for this exact snapshot were not run locally. GitHub CI remains the authoritative compile/test gate for the snapshot.
