# ToTS — Traffic over Turn Server

ToTS is a Rust workspace for carrying L3 VPN traffic over provider-assisted UDP paths. The first provider is VK Calls. A client obtains ICE/TURN material, probes two server entry ports, chooses the healthy/lowest-RTT path, encrypts ToTS packets with the config password, and moves TUN packets through the selected path.

## Workspace

- `tots-core` — reusable protocol, crypto, provider, TURN/STUN, probing and session code.
- `tots-client` — desktop/Linux and Android-friendly CLI binary. Android can pass an already-open VPN fd.
- `tots-ios` — `staticlib`/`cdylib` C ABI around config parsing and packet codec; intended for a NetworkExtension host.
- `tots-server` — Linux server, local management API, SQLite config DB and dual UDP ingress.
- `Providers/` — provider adapters compiled into `tots-core`; currently VK.

## Config URI

The server returns:

```text
tots://BASE64URL(JSON)
```

The JSON carries `peer`, `webrtc_port`, `turn_port`, `password`, `device_id`, `provider` and `provider_data`. The client can also receive the same values as explicit CLI flags.

## Server management API

By default the API listens only on `127.0.0.1:9080`.

```bash
curl -sS http://127.0.0.1:9080/v1/configs \
  -H 'content-type: application/json' \
  -d '{
    "provider":"vk",
    "provider_data":{"vk_call_links":["https://vk.ru/call/join/..."]},
    "password":"change-me",
    "device_id":"00000000-0000-4000-8000-000000000001",
    "valid_days":30
  }'
```

If `TOTS_ADMIN_TOKEN` is set, add `Authorization: Bearer <token>`.

## Client

```bash
tots-client --config 'tots://...'
```

or:

```bash
tots-client \
  --peer 203.0.113.10 \
  --webrtc-port 7800 \
  --turn-port 7801 \
  --password 'secret' \
  --device-id 00000000-0000-4000-8000-000000000001 \
  --provider vk \
  --provider-data '{"vk_call_links":["https://vk.ru/call/join/..."]}'
```

On Android, create the VPN interface in `VpnService` and pass its fd with `--tun-fd`.

## Network model

The TURN path is implemented as a TURN/UDP allocation + permission + channel binding to the ToTS server's TURN ingress port. The direct entry port is an ICE/STUN-reachability path used by the ToTS session selector. `Providers/vk.rs` also exposes signalling metadata/ICE URLs so a full WebRTC media transport can be enabled without changing the config schema. The current 0.1 implementation intentionally keeps the direct data plane as provider-assisted UDP rather than pretending to be a browser PeerConnection.

ToTS itself adds a TUIC-inspired *design* (not wire compatibility): association IDs, packet IDs, fragmentation/reassembly, heartbeat/probe frames, replay protection and an adaptive in-flight window. Its wire format is original.

## Encryption

Each ToTS datagram is protected by XChaCha20-Poly1305. A 256-bit key is derived from `password` + `device_id` with Argon2id. The envelope contains the UUID/session/sequence needed for demultiplexing; payload and frame metadata are encrypted and authenticated.

## Server routing

The server creates a TUN interface for `10.77.0.0/16`. A stable client address is derived from `device_id`. `scripts/setup-network.sh` enables forwarding/NAT with `nft` when run by systemd. Change `TOTS_UPLINK` if the default route detection is unsuitable.

## Build

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo build --workspace --release
```

## Production notes

This is a reference implementation, not an audited VPN. Before exposing it publicly: pin and audit dependencies, add rate limits to the management/API and UDP ingress, rotate provider credentials, add TURN/TCP+TLS fallback if required, and run an independent cryptography/network review.

See `AGENT.MD` for the architecture and file ownership map.
