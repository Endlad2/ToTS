# ToTS — Tunnel over Transports

ToTS is a Rust implementation of a tunnel that carries a **WireGuard (UDP)** or
**Xray (TCP)** payload over ordinary messenger / document channels. It is the
successor idea of CSQTT / FreeTurnProxy / OlcRTC / OpenFlux, unified into one
crate with pluggable transports.

## Supported transports

| Kind           | `TransportKind` | Notes                                   |
|----------------|-----------------|-----------------------------------------|
| VK             | `vk`            | real-time, WebRTC/TURN-style channel    |
| Yandex Telemost| `telemost`      | real-time                               |
| VK Stream      | `vk_stream`     | real-time                               |
| Yandex Docs    | `yandex_docs`   | document sync channel                   |
| Mail Docs      | `mail_docs`     | document sync channel                   |
| MAX            | `max`           | messenger sync channel                  |
| OneME          | `oneme`         | messenger sync channel                  |

Payload layer: `wireguard` (UDP) or `xray` (TCP).
Crypto: X25519 key agreement + HKDF-SHA256 + ChaCha20-Poly1305 framed AEAD.

## Workspace layout

```
crates/
  tots-core/   # library: config, crypto, transport trait + impls, tunnel engine
  tots-cli/    # `tots` binary
docs/
  install-server.md
```

## Build

```bash
cargo build --release -p tots-cli
./target/release/tots --help
```

Mobile / FFI artifacts are produced by `crates/tots-core` with
`crate-type = ["rlib", "cdylib", "staticlib"]`:

- Android: `libtots_core.so`
- iOS: `libtots_core.a` (+ generated header)
- Desktop FFI: `tots_core.dll` / `libtots_core.so` / `libtots_core.dylib`

## CLI

```bash
tots info                       # print build / config info
tots transports                 # list available transports
tots run --config server.toml   # start the tunnel
tots parse "tots://vk?room=r1&transport_key=...&payload=wireguard"
```

## Link format

```
tots://<transport>?room=<id>&transport_key=<hex32>&payload=<wireguard|xray>&relay=<host:port>
```

## Server installation

See **[docs/install-server.md](docs/install-server.md)** for a step-by-step
guide: release download / source build, `/etc/tots/server.toml`, the systemd
unit, WireGuard `iptables` forwarding, and Xray TCP wiring.

## Releases

CI publishes, on every `v*.*.*` tag:

- desktop binaries — linux (amd64, arm64), windows (amd64), macos (amd64, arm64)
- Android shared libraries — arm64-v8a, armeabi-v7a, x86_64
- iOS XCFramework
- `sha256` checksums for every artifact

## License

See [LICENSE](LICENSE).