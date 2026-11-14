# ToTS — Traffic over Turn Server

**Development draft, 2026-11-12:** the 40 Mbit/s target is not achieved. Live speed measurements were made with a background VPN reportedly enabled and need an isolated repeat. Worker startup has both successful 30/50-worker runs and later incomplete-pool timeouts. See [current status](PROJECT_STATUS.md) and [the throughput investigation](docs/THROUGHPUT-RESULTS.md) before treating this snapshot as a release.

ToTS carries encrypted L3 VPN packets through VK Calls TURN/UDP, the direct WebRTC-side ingress, SaluteJazz RTP media, Yandex Docs WebSocket, Mail.ru Docs WebSocket, or Cups.online Centrifugo rooms. The client runs an independent worker pool, keeps latency-sensitive small UDP pinned to one healthy worker, stripes bulk TCP/large UDP across workers in bounded flowlets, and can rebuild a failed worker on the preferred provider or a configured fallback. Windows IPv4 Internet egress through both providers has been verified against three IP services, with clean timed exits and full-tunnel route removal. The same Jazz config also passed a reconnect probe. See [test results](docs/TEST-RESULTS.md).

## Workspace

- `tots-core`: packet protocol, encryption, fragmentation, probing and transport abstractions.
- `tots-client`: Windows/Linux CLI client with managed desktop TUN/routing support.
- `tots-android`: Android CLI worker binary with `VpnService` TUN-fd support; release packaging renames the executable to `libtots.so` for APK `jniLibs` delivery.
- `tots-ios`: C ABI/static library for a NetworkExtension host.
- `tots-server`: Linux TUN, UDP ingress, SQLite and the local management API.
- `Providers/`: native VK, Jazz, `ya-docs`, `mail-docs`, and `cups` adapters. No OpenFlux executable/sidecar is used. The bundled MIT-licensed `jazz-session` library remains a path dependency excluded from workspace membership.

## Server and configuration

The management API defaults to `127.0.0.1:9080`. Create a configuration with `POST /v1/configs`, using a private JSON request file:

```bash
curl -sS http://127.0.0.1:9080/v1/configs \
  -H 'content-type: application/json' \
  --data-binary @request.private.json
```

The request contains `provider` (the preferred provider), `provider_data`, a password of at least eight bytes, `device_id` (UUID), and optional `valid_days` (default 30). `provider_data` is now keyed by provider so one config can contain fallbacks, for example:

```json
{
  "provider": "vk",
  "provider_data": {
    "vk": {"vk_call_links": ["https://vk.ru/call/join/..."]},
    "jazz": {}
  }
}
```

`vk` accepts `vk_call_links` or `vk_token`; optional `ice_servers` supplements bootstrap. VK bootstrap generations remain paced by three seconds to avoid anonymous API flood control, but one generation may lease credentials to a cohort of up to seven workers. Those workers still create independent UDP sockets, TURN allocations and encrypted sessions. Worker setup/path probing is bounded to seven concurrent workers and candidate paths inside each worker are probed in parallel. Multiple call links rotate between bootstrap generations. Repeated flood-control failures impose a shared 30/60/120/240/300-second cooldown.

For parallel Jazz workers, request `"jazz": {"worker_count": 30}` and run the returned URI with `--n 30`. The count is 1–50, default 1. The server provisions independent rooms and bridges, with four concurrent setup operations, and persists an ordered secret `worker_rooms` array. Each worker uses its own room so media is not broadcast to the other workers. The first room's invite and `connector_url` remain in the legacy fields. An optional `join_link` supplies room zero; further rooms are created independently. Existing one-room configs support one Jazz worker and must be provisioned again for a larger pool. This also applies when Jazz is configured as a fallback.

Jazz also accepts the diagnostic `media_carrier` option: `audio` (default), `audio-wallclock` (same audio transport with timestamps derived from elapsed time), or `vp8-experimental` (opaque video RTP envelope). The API applies it to both server bridges and client worker rooms. The video variant adds VP8 framing metadata for forwarding tests; it is not a VP8 video encoder. Check the throughput report for physical-provider evidence before choosing an experimental carrier.

Native document/room providers use the same worker/fallback machinery:

```json
{
  "provider": "vk",
  "provider_data": {
    "vk": {"vk_call_links": ["https://vk.ru/call/join/..."]},
    "ya-docs": {"urls": ["https://docs.yandex.ru/docs/view?...", "https://docs.yandex.ru/docs/view?..."]},
    "mail-docs": {"urls": ["AbCdEfGh1/IjKlMnOp2"]},
    "cups": {"worker_count": 20}
  }
}
```

`ya-docs` accepts `url` or `urls`; optional `cookies` is a JSON object of cookie name/value pairs. The provider follows Yandex document redirects and solves the `showcaptchafast` proof-of-work challenge in-process. A browser-only SmartCaptcha/login still needs reusable cookies. `mail-docs` accepts a bare public weblink (`AbCdEfGh1/IjKlMnOp2`) or a full `https://cloud.mail.ru/public/...` link, also as `url`/`urls`. For both document carriers, workers select URLs round-robin; providing multiple URLs avoids concentrating every worker in one collaborative document.

`cups` accepts `{"worker_count": N}` in the server API. The server provisions `N` independent Cups.online rooms, stores them in `worker_rooms`, and starts one native bridge per room. As with Jazz, if Cups is available as a fallback, provision `worker_count >= --n` so any failed slot can move to a non-conflicting room. Existing `room_id`/`worker_rooms` can also be reused on server restart.

If `TOTS_ADMIN_TOKEN` is configured, include its bearer authorization header. Store the returned `tots://` URI privately; it contains credentials. The database preserves the invite; server restart rejoins active rooms. Revoke, expiry and provider replacement close the old room bridge. Revoke a device with `DELETE /v1/configs/{device_id}`. The server bridges Jazz/Yandex Docs/Mail.ru Docs/Cups packets into its existing authenticated UDP/TUN dataplane.

The server uses `10.77.0.0/16` for virtual addresses. `scripts/setup-network.sh` configures Linux forwarding/NAT; set `TOTS_UPLINK` if automatic uplink detection is unsuitable. Keep the management API on loopback unless remote access is explicitly configured.

## Windows client

Extract the delivery archive and keep `wintun.dll` beside the Windows `tots-client.exe`. Run TUN operations as Administrator. Pass the server-returned URI directly:

```powershell
.\tots-client.exe --config 'tots://...' --n 10 --relay-only --tun-default-route
```

To request a preferred startup split across configured providers, add `--workers_percent`. Values may be JSON strings or integers and must total 100:

```powershell
.\tots-client.exe --config 'tots://...' --n 20 --workers_percent '{"vk":"40","jazz":"20","ya-docs":"20","mail-docs":"10","cups":"10"}' --relay-only --tun-default-route
```

The split only controls each slot's preferred provider. Every slot still keeps the other configured providers in its fallback order, so the actual `workers_vk`, `workers_jazz`, `workers_ya_docs`, `workers_mail_docs`, and `workers_cups` counts can move away from the requested percentages after failures. Omitting `--workers_percent` preserves the existing primary-provider-first behavior. If Jazz is present as a fallback, provision at least `worker_count = --n`, because any slot may eventually fail over to Jazz and must have a non-conflicting room.

Useful options:

| Option | Behavior |
| --- | --- |
| `--config URI` | Decode one server-issued `tots://` config. |
| `--n N` | Run `N` independent transport workers (1..256). |
| `--workers_percent JSON` | Optional preferred provider split across any configured `vk`/`jazz`/`ya-docs`/`mail-docs`/`cups`; values must sum to 100. |
| `--mtu N` | Override the IPv4 TUN MTU, including with `--config`; valid range 576..1400. |
| `--relay-only` | Exclude the direct VPS/WebRTC-side UDP path; use provider relay/native transport. |
| `--probe-only` | Wait for every requested encrypted worker without opening TUN or installing routes. |
| `--tun-default-route` | Install Windows IPv4 full-tunnel routing and TUN DNS. |
| `--run-for-seconds N` | End the connected TUN run gracefully after a bounded interval. |

For example, add `--run-for-seconds 75` for a bounded test. The route guard pins the VPS and provider endpoint IPv4 addresses to the physical gateway, then removes its owned routes and restores TUN DNS during normal exit, including Ctrl+C and the timer. Cleanup after force-killing the process is not guaranteed.

The client waits for the complete requested pool before creating the full-tunnel routes. Startup/reconnect connection work is capped at seven workers concurrently rather than serially staggering every worker. Any request that configures Jazz or Cups (primary or fallback) and exceeds its provisioned room count fails immediately.

IPv6 routing is not managed. Automatic full-tunnel routing is currently Windows-only. Android uses the separate `tots-android` binary below; iOS requires a NetworkExtension host.

## Android client

`tots-android` carries the same worker/provider dataplane as `tots-client`: `--n`, `--workers_percent`, relay-only mode, provider fallback/recovery, flowlet scheduling and five-second traffic/worker telemetry are preserved. Android TUN ownership is intentionally different: the APK's `VpnService` creates/configures the interface, and the native binary receives the inherited descriptor with `--tun-fd`.

The host `VpnService` must configure the same virtual IPv4 address and MTU contained in the `tots://` configuration. It also owns Android route/DNS/app-selection policy. To avoid the VPN process tunnelling its own provider/WebSocket/TURN sockets back into its TUN, the future APK should exclude its own package/UID from the VPN or otherwise protect the transport sockets before installing a full-tunnel route.

Example process arguments inside an APK host:

```text
libtots.so --config tots://... --n 20 --workers_percent {"vk":50,"jazz":50} --relay-only --tun-fd 42
```

`libtots.so` is deliberately **not** a Rust `cdylib`: it is the normal PIE executable produced from `tots-android`, renamed after linking so Android packaging can place it under `jniLibs/<abi>/libtots.so`. The package `build.rs` has two modes. Under Cargo it stamps the Android ELF soname; when compiled/run standalone by `scripts/build-android.sh`, the same `build.rs` invokes the real Android Cargo build and moves the finished executable to `dist/android/<abi>/libtots.so`.

Build any supported Android ABI locally with an Android NDK installed:

```bash
export ANDROID_NDK_HOME=/path/to/android-ndk
./scripts/build-android.sh aarch64-linux-android
./scripts/build-android.sh armv7-linux-androideabi
./scripts/build-android.sh x86_64-linux-android
```

The minimum/default Android API level is 24 and can be overridden with `ANDROID_API` to a newer level. API 23 is not supported because the WebRTC networking stack uses `getifaddrs`/`freeifaddrs`, which Android exposes from API 24. The build helper also understands `i686-linux-android` for local legacy builds. GitHub CI/Release build the three APK ABIs `arm64-v8a`, `armeabi-v7a`, and `x86_64` in parallel and package them together under `jniLibs/<abi>/libtots.so`.

For a probe without consuming a TUN fd:

```text
libtots.so --config tots://... --n 20 --relay-only --probe-only
```

## Transport and encryption

VK uses TURN/UDP allocation, authentication, permissions and channel binding. Its relay requires a 12-byte RTP envelope around each encrypted ToTS datagram. The server recognizes that envelope while continuing to accept raw ToTS datagrams for the direct/legacy path.

Jazz uses actual WebSocket signalling and WebRTC RTP media through the restored `jazz-session` library. `preconnect_call` performs HTTP provisioning on the VPS; `join_preconnected` lets the PC connect directly to Jazz WSS/media endpoints. SSH is used for setup or private configuration retrieval, never for user traffic. Public media keepalives establish the remote track and are filtered before ToTS packet processing.

The Jazz session publishes audio with `muted:false` and explicitly subscribes to remote unmuted AUDIO tracks, deduplicating updates and unsubscribing when tracks are muted. Encrypted path probes retry every 500 ms with fresh sequence numbers within an overall three-second deadline, allowing for initial packet loss during SFU renegotiation.

ToTS frames use XChaCha20-Poly1305 with an Argon2id-derived key, authenticated envelope metadata and replay protection. Each worker has its own session and sequence space; workers reuse the device key without repeating Argon2. Data frames receive their sequence number immediately before encrypted transmission. The replay window remains 128 packets. ACK-range feedback is delayed for at most about 2 ms and batched up to 32 received packet IDs per datagram, reducing control traffic and transport sends without changing authenticated packet numbering.

TCP/UDP IP traffic uses best-effort datagram forwarding, relying on the inner transport's congestion control rather than adding a second ACK window, pacer or retransmission layer. Bounded queues and socket backpressure still apply. Reliable control traffic retains congestion control, burst pacing, loss detection and bounded recovery. Best-effort ACK diagnostics retain at most 512 payload-free samples per worker.

Bulk traffic moves between workers at roughly 16 KiB flowlet boundaries. Small UDP (up to 300-byte IP packets) stays sticky unless its short-window load rises above about 75% of one estimated allocation, at which point it is temporarily treated as bulk. Worker selection uses an adaptive token/deficit weight of about 247 kB/s (~2 Mbit/s) per allocation; this is a balancing estimate rather than a hard rate cap. The server receive hot path uses a larger bounded TUN ingress queue and no longer awaits one TUN enqueue plus one ACK network send per incoming IP packet; ACK transmission is handled by a separate per-session batcher. TURN paths also reuse their receive/send packet buffers instead of allocating a ~65 KiB receive Vec and multiple transmit Vecs per datagram. These changes follow the same hot-path separation/buffer-reuse principles as MRTT without importing its source code.

Every worker owns a distinct authenticated `session_id`, so workers from one device cannot overwrite each other on the server. If a path dies, the same SessionEngine/sequence space is retained while the worker re-probes direct/WebRTC-side and TURN candidates; after repeated preferred-provider failures it tries configured fallback providers. This gives authenticated 0-RTT-like path resumption without importing QUIC wire format. A five-second status line reports TUN MB, healthy/target worker counts, per-provider worker split and the approximate aggregate VK scheduling capacity. TURN/TCP and TURN/TLS are not implemented.

## Build and release delivery

This source tree contains no private configuration or credentials. GitHub Actions builds the Linux server, Linux/macOS/Windows clients and Android arm64-v8a/armeabi-v7a/x86_64 client release assets; local rebuilds use stable Rust. This revision was tested with Rust `1.99.1`; workspace/Jazz minimum-version metadata is `1.89`. The workspace lockfile predates the added native WebSocket/cookie dependencies in this snapshot. Android's standalone builder therefore lets Cargo refresh the lock during the build. Jazz still pins `webrtc-ice = 0.17.2` explicitly, so refreshing the lock cannot silently move that relay-sensitive dependency to 0.17.3. See `PROJECT_STATUS.md` and `docs/THROUGHPUT-RESULTS.md` for measured validation and remaining limitations.

```bash
cargo test --workspace
cargo test --manifest-path Providers/jazz-session/Cargo.toml --locked
cargo build --workspace --release
```

Distribute the built Windows executable with its matching official AMD64 `wintun.dll`, and include `LICENSE`, `NOTICE` and the Jazz MIT license. The Android release archive contains `jniLibs/arm64-v8a/libtots.so`, `jniLibs/armeabi-v7a/libtots.so`, and `jniLibs/x86_64/libtots.so`; each is an executable PIE despite the `.so` filename. The Linux server package also needs the systemd unit and network setup script. Preserve `/var/lib/tots/tots.db` and `/etc/tots/server.env` when upgrading. Archive delivery does not require a pull request or published release.

[AGENT.MD](AGENT.MD) describes architecture and ownership; [PROJECT_STATUS.md](PROJECT_STATUS.md) separates implementation from verified behavior. ToTS is a reference implementation and has not received an independent security audit.
