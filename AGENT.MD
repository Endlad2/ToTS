# AGENT.MD — ToTS architecture contract

This file is the source-of-truth for agents modifying the repository.

## Non-negotiable architecture

ToTS is split into exactly four workspace crates plus the top-level `Providers/` adapter directory:

1. `tots-core`: reusable library. All protocol, packet codec, crypto, path selection, TURN/STUN and provider abstractions belong here. It must never depend on UI or server storage.
2. `tots-client`: executable for desktop and Android. It parses CLI/config URI, opens/accepts a TUN fd and wires it to `tots-core`.
3. `tots-ios`: static library / C ABI. Do not duplicate protocol logic; call `tots-core` only.
4. `tots-server`: server executable. Owns SQLite, management API, Linux TUN, peer routing and system integration.
5. `Providers/`: provider-specific acquisition logic. `tots-core/src/providers/mod.rs` includes these adapters. Provider code may fetch signalling/ICE/TURN data but must return provider-neutral structures.

## Wire/data flow

Client TUN -> `SessionEngine::encapsulate_ip` -> encrypted ToTS datagram -> selected `DatagramPath` -> server UDP ingress -> `SessionEngine::decapsulate` -> server TUN -> Internet. Return traffic reverses the flow.

Server has two distinct UDP entry ports:

- `webrtc_port` (default 7800): direct/provider-assisted ICE/STUN candidate path.
- `turn_port` (default 7801): target address for packets relayed through a provider TURN allocation.

Do not merge these ports. Path selection must probe both when provider data allows it and prefer the lowest measured RTT among healthy paths.

## Security invariants

- Never transmit the config password or derived key on the wire.
- All `Frame` bytes are AEAD-protected before leaving a process.
- `device_id`, session ID and sequence remain in the authenticated cleartext envelope for demux/replay handling.
- `ReplayWindow` must run before an accepted packet is applied to TUN state.
- Do not log passwords, TURN credentials, VK access tokens or decrypted packet bytes.
- Management API defaults to loopback; remote exposure requires an explicit operator change and should use an admin bearer token/reverse proxy.

## TUIC-inspired behavior (not TUIC wire compatibility)

`protocol/frame.rs` owns the original ToTS commands. Data frames include association ID, packet ID and fragment coordinates. `fragment.rs` and `reassembly.rs` are responsible for bounded fragmentation/reassembly. `flow.rs` owns the adaptive in-flight window and RTT estimator. Do not import or copy TUIC source code.

## TURN/STUN

`transport/stun.rs` is a small RFC-style codec. `transport/turn.rs` performs UDP Allocate -> long-term auth -> CreatePermission -> ChannelBind and then carries ToTS datagrams as TURN ChannelData. If TURN/TCP/TLS is added, implement it behind the same `DatagramPath` trait.

## Provider contract

`providers::Provider` returns `ProviderBootstrap` containing TURN credentials, STUN URLs and signalling metadata. `Providers/vk.rs` is the VK implementation. New providers get a new file in `Providers/` and one registry branch in `tots-core/src/providers/mod.rs`.

`provider_data` is opaque JSON in the public config so adding providers does not change the URI schema. VK currently accepts:

```json
{
  "vk_call_links": ["https://vk.ru/call/join/..."],
  "vk_token": "optional-token",
  "ice_servers": []
}
```

At least one of `vk_call_links` or `vk_token` must be present. `ice_servers` is an optional explicit bootstrap useful for tests/self-hosted provider gateways.

## Server storage/API

`server/db.rs` is the only module that speaks SQLite. DB path defaults to `/var/lib/tots/tots.db`. Configs persist across binary upgrades.

`server/api.rs` owns HTTP routes. Creation returns the config URI and derived virtual address. Avoid leaking the password in list endpoints.

## File map

- `/Cargo.toml` — workspace and dependency versions.
- `/Providers/vk.rs` — VK Calls/ICE/TURN acquisition adapter.
- `/tots-core/src/config.rs` — public config schema and `tots://` codec.
- `/tots-core/src/crypto.rs` — Argon2id + XChaCha20-Poly1305.
- `/tots-core/src/protocol/frame.rs` — frame types and serialization.
- `/tots-core/src/protocol/wire.rs` — authenticated envelope.
- `/tots-core/src/fragment.rs` — MTU-aware data fragmentation.
- `/tots-core/src/reassembly.rs` — bounded fragment reassembly.
- `/tots-core/src/flow.rs` — ACK/RTT/in-flight controller.
- `/tots-core/src/session.rs` — high-level packet engine.
- `/tots-core/src/transport/path.rs` — `DatagramPath` trait + direct UDP path.
- `/tots-core/src/transport/stun.rs` — STUN/TURN message codec.
- `/tots-core/src/transport/turn.rs` — TURN/UDP path.
- `/tots-core/src/transport/probe.rs` — path RTT/health probes.
- `/tots-client/src/main.rs` — CLI orchestration.
- `/tots-client/src/tun_io.rs` — Linux TUN and Android fd loops.
- `/tots-ios/src/lib.rs` — C ABI for NetworkExtension hosts.
- `/tots-server/src/main.rs` — server lifecycle.
- `/tots-server/src/api.rs` — local config API.
- `/tots-server/src/db.rs` — SQLite schema/queries.
- `/tots-server/src/dataplane.rs` — UDP/TUN packet routing.
- `/scripts/install-server.sh` — release installer/upgrader preserving DB/config.
- `/scripts/setup-network.sh` — idempotent Linux forwarding/NAT setup.
- `/systemd/tots-server.service` — systemd unit.
- `/.github/workflows/ci.yml` — fmt/clippy/test/build.
- `/.github/workflows/release.yml` — Linux release artifact build.

## Change rules

Keep public structs serde-compatible unless bumping config protocol version. Add tests for every frame format and config migration. Prefer bounded queues/maps; untrusted network input must not grow memory without limits. New transport paths implement `DatagramPath` and are selected by probe results rather than special-casing the client loop.
