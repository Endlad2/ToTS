## Android API 24 linker fix (2026-11-14)

Android arm64 builds now target API 24 by default and CI/Release pin `ANDROID_API=24`. The WebRTC interface-enumeration dependency references bionic `getifaddrs`/`freeifaddrs`, which are unavailable when linking against Android API 23. `scripts/build-android.sh` rejects API levels below 24 with a direct diagnostic so the undefined-symbol linker failure cannot silently return.

## Android client crate (2026-11-14)

Added a separate `tots-android` workspace crate based on the current desktop client worker/provider dataplane. It preserves multi-provider workers, `--workers_percent`, failover/recovery, flowlet scheduling, telemetry, relay/probe modes and config handling. Android TUN is supplied through `VpnService` with `SCM_RIGHTS` over an abstract local socket (`--tun-fd-socket`); legacy inherited `--tun-fd` remains for launchers that guarantee descriptor inheritance. The crate does not create a Linux/Windows TUN or manage Android routes/DNS.

`tots-android` is built as a normal PIE executable. `tots-android/build.rs` supports standalone builder mode used by `scripts/build-android.sh`: it cross-builds the executable and then moves the final artifact to `dist/android/<abi>/libtots.so`. Cargo-mode execution of the same build script only stamps the Android soname, avoiding recursive Cargo builds. CI/release now build `arm64-v8a`, `armeabi-v7a`, and `x86_64`; the release packages all three under `jniLibs/<abi>/libtots.so`.

This packaging environment still has no Rust toolchain or Android NDK, so the new Android target could not be compiled locally. Static manifest/script/source checks are performed here; GitHub Android CI is the compile/link gate. Future APK integration must ensure ToTS's own transport sockets bypass the `VpnService` full-tunnel path, e.g. by excluding the hosting package/UID or an equivalent protected-socket mechanism.

## Native provider expansion (2026-11-14)

Implemented native `ya-docs`, `mail-docs`, and `cups` provider modules directly in `Providers/`, integrated with `ProviderName`, `--workers_percent`, worker fallback/recovery, server bridge provisioning, DB restoration, and five-second provider counts. `cups` provisions independent worker rooms. `ya-docs` includes in-process Yandex proof-of-work handling. No OpenFlux binary, subprocess, SOCKS sidecar, or OpenFlux runtime dependency is present.

This environment does not contain a Rust toolchain and outbound package installation is unavailable, so the new Rust code could not be validated with `cargo clippy/test/build` locally. Static structure/config/script/archive checks are performed before delivery; GitHub CI remains the compile/type-check gate. The Mail.ru edge requires a permessage-deflate WebSocket offer according to the reference implementation; the current carrier sends that offer, but `tokio-tungstenite 0.28` does not itself implement RFC7692 decompression, so live Mail.ru verification is still required and compressed inbound frames may require a follow-up WebSocket backend change.

# Project status

## Implemented in this source snapshot

- Multi-provider `provider_data` (`vk` / `jazz` / `ya-docs` / `mail-docs` / `cups` / `max`) with a preferred `provider` and fallback order.
- Client `--n` worker pool, optional `--workers_percent` preferred-provider apportionment, per-worker session IDs, health logs and five-second TUN traffic/worker telemetry (including preferred and actual provider splits).
- Worker recovery: preferred-provider retries, fallback-provider cycling, path re-probing on liveness/sustained-RTT degradation and authenticated session/path resumption.
- Server multi-session routing per device so workers do not overwrite one another.
- ACK ranges and RTT diagnostics; congestion control, pacing and bounded retransmission for reliable control. Best-effort TCP/UDP forwarding has no duplicate outer ACK window or pacer.
- Symmetric client/server flowlet scheduling: small UDP pinning, automatic promotion of high-rate UDP to bulk, ~16 KiB bulk striping, and adaptive per-allocation scheduling weights based on ~247 kB/s (~2 Mbit/s).
- Reliable/unreliable transport policy (TCP/UDP default to low-latency unreliable recovery; stale queued unreliable packets are dropped after outages; control traffic can be recovered).
- 1200-byte encrypted datagram budget and best-effort 4 MiB UDP socket buffers.
- Client configuration is passed as `--config tots://...` or explicit CLI fields; local secret-config loading is not part of the CLI.
- Shared off-runtime device key derivation, bounded authenticated admission, independent server egress queues and periodic authorization refresh.
- Paced VK credential acquisition with seven-worker credential cohorts, seven-way bounded concurrent worker connection/probing, and independent Jazz room/bridge pools for up to 50 workers; normal startup is not gated on complete-pool health, while `--probe-only` retains the explicit full-pool check.
- Lazy frame sealing preserves the existing 128-packet replay check when data waits behind control packets.
- Delayed ACK-range batching (up to 32 packet IDs / ~2 ms) on client and server, a non-blocking server UDP→TUN ingress hot path with a 16K-packet bounded queue, and reusable TURN send/receive buffers to remove per-datagram allocator churn.

## MAX transport and startup/TUN fixes (2026-11-20)

Added a MAX provider using KometTeam's `kolibri-net` protocol/signaling library and Android-style binary TCP/TLS session: access-token login, call-token exchange, attempted random private group creation, audio self-call setup, best-effort group attachment, VCP decoding, ws2 WebRTC signaling, and TURN/STUN extraction. The provider does not invite contacts. MAX may reject attaching a self-only group call; when that happens it still uses the call's TURN credentials and records `group_call_attached: false`. The supplied `access_token` remains in server `provider_data` and therefore in the config database; derived call/session/turn secrets are kept in memory.

Removed the blocking all-workers-health barrier from normal Android and desktop startup. Worker supervisors connect in the background, and the TUN sender waits on a readiness watch for the first available worker. Android TUN I/O now uses a duplicated non-blocking fd registered with epoll, handles `EAGAIN` asynchronously, reports EOF explicitly, and uses SIGTERM as the Android process shutdown signal. Direct-UDP candidate failure no longer prevents trying TURN. Fixed silent inbound packet drops in Yandex Docs, Mail.ru Docs and Cups queues; tightened the Yandex Engine.IO/Socket.IO handshake; corrected Mail.ru numeric editor IDs and dynamic Origin; normalized Cups websocket URL/Origin and relative metadata URLs.

No live MAX token or Android APK host was available, and this archive has no Rust toolchain. `Cargo.lock` could not be regenerated here; the first normal Cargo build must resolve the new `kolibri-net` git dependency before using `--locked`. Compile/live-provider verification remains outstanding.

## Validation status

The Sasha baseline recorded a workspace run on 2026-11-12 that passed 109 tests (64 core, 14 client, 31 server), with one Administrator/Wintun test ignored in the ordinary run; Workspace Clippy passed with warnings denied. Jazz SDK checks passed 42 unit tests, two example tests and one doc test; ten provider tests passed. The `--workers_percent`, seven-way startup and ACK hot-path changes in this archive were added after that recorded run. This packaging environment has no Rust toolchain and no live VK/Jazz endpoint, so those new changes are structurally checked here but must still pass GitHub CI and a real throughput run before their speed gain is treated as measured.

Both VK and Jazz have connected 50 independent encrypted workers in earlier bounded Windows runs and returned the Linux server's public IPv4 address. Later starts also timed out with incomplete pools, so repeatable startup is still unresolved. The operator subsequently reported a continuously enabled background VPN, and running WireSock services were observed. Speed measurements are provisional until that additional path is disabled or a ToTS bypass is independently verified. The prior 40 Mbit/s acceptance target was not achieved in the recorded baseline. This revision specifically removes serial startup tails and per-packet ACK/TUN awaits, but no fixed “+10 Mbit/s” improvement is claimed without a clean live benchmark because VK relay capacity, RTT and loss dominate the result.

IPv6 is not transported by the dataplane; the Windows full-tunnel guard now blocks it while connected. Reconnected provider endpoints can still require a client restart because bypass routes are installed from the initial ready-path pool. Permanently exited native bridges are not automatically replaced. Bounded tests do not establish long-duration stability.

## Windows full-tunnel routing fix (2026-11-20)

Windows now enables full-tunnel mode by default unless `--no-tun-default-route` is passed. Wintun is created immediately; before switching system routes, the client waits only for the first healthy worker, not every selected provider group or the complete worker pool. The route plan installs protected `/32` underlay routes, both IPv4 `/1` routes, and an IPv4 `/0` route; the active route table and Wintun adapter state are verified before reporting full-tunnel readiness. The Wintun interface metric is set to 1. Additional provider endpoints are pinned to the captured physical underlay as workers discover them.

While the full tunnel is active, the client snapshots and replaces IPv4 DNS on the selected physical underlay and Wintun with public IPv4 resolvers, then restores the original server lists on normal shutdown. Because the dataplane is IPv4-only, a Windows Defender Firewall outbound rule blocks IPv6 Internet egress using the Windows Firewall `Internet6` address keyword during the session and is removed during normal cleanup. If the process is force-killed, the persistent firewall rule may remain; the README includes the exact elevated-PowerShell cleanup command. A diagnostic PowerShell script reports adapter, route, firewall and DNS state.

This environment has no Rust toolchain and cannot run a Windows/Wintun integration test. The code and scripts were statically inspected, but Windows compilation and live tests on the user's PC remain required before calling the fix verified.

## Windows and Android startup regression fixes (2026-11-21)

Android ToTS now supports explicit TUN descriptor transfer over an abstract AF_UNIX socket with `SCM_RIGHTS`, avoiding Android `ProcessBuilder`'s unreliable inheritance of arbitrary file descriptors. LaLune creates the local server before launching ToTS, sends the `VpnService` descriptor, and only marks the TUN ready after ToTS reports successful epoll registration.

Windows now creates Wintun before waiting for provider health and waits for the first healthy worker rather than one worker per selected provider group. Full-tunnel routes are installed in a batched PowerShell operation; newly discovered provider endpoints are added to the physical underlay escape routes in the background. Removed the unsupported `-AddressFamily` argument from every `Set-DnsClientServerAddress` call, which was the failure shown in the user's log and caused route rollback.

Compilation and live Android/Windows tests remain outstanding because this environment does not have Cargo/Rust, Android SDK/NDK or a Windows host. SCM_RIGHTS semantics were smoke-tested on a local abstract Unix socket; the app-side Android handoff and Windows routing still require device/host verification.

## Windows DNS helper compile fix (2026-11-21)

Moved `restore_dns_script` and `set_ipv4_dns_script` out of the IPv6 PowerShell raw string and back to module scope. They remain gated with `#[cfg(any(windows, test))]`, so the Windows implementation and its unit tests can resolve them. Removed the now-unused `HashSet` import from `tots-client/src/workers.rs`. The source-level checks confirm each helper is defined once, outside the raw PowerShell string, and that `Set-DnsClientServerAddress` calls do not pass the unsupported `-AddressFamily` parameter. A Cargo build/test could not be run in this environment because Rust/Cargo are unavailable.
