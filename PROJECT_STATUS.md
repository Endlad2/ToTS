## Android API 24 linker fix (2026-11-14)

Android arm64 builds now target API 24 by default and CI/Release pin `ANDROID_API=24`. The WebRTC interface-enumeration dependency references bionic `getifaddrs`/`freeifaddrs`, which are unavailable when linking against Android API 23. `scripts/build-android.sh` rejects API levels below 24 with a direct diagnostic so the undefined-symbol linker failure cannot silently return.

## Android client crate (2026-11-14)

Added a separate `tots-android` workspace crate based on the current desktop client worker/provider dataplane. It preserves multi-provider workers, `--workers_percent`, failover/recovery, flowlet scheduling, telemetry, relay/probe modes and config handling. Android TUN is supplied only through an inherited `VpnService` descriptor (`--tun-fd`); the crate does not create a Linux/Windows TUN or manage Android routes/DNS.

`tots-android` is built as a normal PIE executable. `tots-android/build.rs` supports standalone builder mode used by `scripts/build-android.sh`: it cross-builds the executable and then moves the final artifact to `dist/android/<abi>/libtots.so`. Cargo-mode execution of the same build script only stamps the Android soname, avoiding recursive Cargo builds. CI/release now build `arm64-v8a`, `armeabi-v7a`, and `x86_64`; the release packages all three under `jniLibs/<abi>/libtots.so`.

This packaging environment still has no Rust toolchain or Android NDK, so the new Android target could not be compiled locally. Static manifest/script/source checks are performed here; GitHub Android CI is the compile/link gate. Future APK integration must ensure ToTS's own transport sockets bypass the `VpnService` full-tunnel path, e.g. by excluding the hosting package/UID or an equivalent protected-socket mechanism.

## Native provider expansion (2026-11-14)

Implemented native `ya-docs`, `mail-docs`, and `cups` provider modules directly in `Providers/`, integrated with `ProviderName`, `--workers_percent`, worker fallback/recovery, server bridge provisioning, DB restoration, and five-second provider counts. `cups` provisions independent worker rooms. `ya-docs` includes in-process Yandex proof-of-work handling. No OpenFlux binary, subprocess, SOCKS sidecar, or OpenFlux runtime dependency is present.

This environment does not contain a Rust toolchain and outbound package installation is unavailable, so the new Rust code could not be validated with `cargo clippy/test/build` locally. Static structure/config/script/archive checks are performed before delivery; GitHub CI remains the compile/type-check gate. The Mail.ru edge requires a permessage-deflate WebSocket offer according to the reference implementation; the current carrier sends that offer, but `tokio-tungstenite 0.28` does not itself implement RFC7692 decompression, so live Mail.ru verification is still required and compressed inbound frames may require a follow-up WebSocket backend change.

# Project status

## Implemented in this source snapshot

- Multi-provider `provider_data` (`vk` / `jazz`) with a preferred `provider` and fallback order.
- Client `--n` worker pool, optional `--workers_percent` preferred-provider apportionment, per-worker session IDs, health logs and five-second TUN traffic/worker telemetry (including preferred and actual provider splits).
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
