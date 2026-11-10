# ToTS — Traffic over Turn Server

ToTS carries encrypted L3 VPN packets through VK Calls TURN/UDP or a live SaluteJazz RTP media session. The client probes the available paths, selects a healthy path by RTT, and connects it to a TUN interface. Windows IPv4 Internet egress through both providers has been verified against three IP services, with clean timed exits and full-tunnel route removal. The same Jazz config also passed a reconnect probe. See [test results](docs/TEST-RESULTS.md).

## Workspace

- `tots-core`: packet protocol, encryption, fragmentation, probing and transport abstractions.
- `tots-client`: Windows/Linux CLI and Android VPN-fd support.
- `tots-ios`: C ABI/static library for a NetworkExtension host.
- `tots-server`: Linux TUN, UDP ingress, SQLite and the local management API.
- `Providers/`: VK and Jazz adapters. The bundled MIT-licensed `jazz-session` library is a path dependency excluded from workspace membership, keeping four workspace crates.

## Server and configuration

The management API defaults to `127.0.0.1:9080`. Create a configuration with `POST /v1/configs`, using a private JSON request file:

```bash
curl -sS http://127.0.0.1:9080/v1/configs \
  -H 'content-type: application/json' \
  --data-binary @request.private.json
```

The request contains `provider`, `provider_data`, a password of at least eight bytes, `device_id` (UUID), and optional `valid_days` (default 30). If `TOTS_ADMIN_TOKEN` is configured, include its bearer authorization header. Store the returned config URI privately; its contents include credentials.

| Provider | `provider_data` |
| --- | --- |
| `vk` | Supply `vk_call_links`, or `vk_token` to create a call. Optional `ice_servers` supplements bootstrap. |
| `jazz` | `{}` creates a room and provisions a client connector on the VPS. Optional `join_link` joins an existing room. |

For Jazz, the API adds the room invite and secret `connector_url` to the returned configuration. The database preserves the invite; server restart rejoins active rooms. Revoke, expiry and provider replacement close the old room bridge. Revoke a device with `DELETE /v1/configs/{device_id}`. The server bridges Jazz packets into its existing authenticated UDP/TUN dataplane.

The server uses `10.77.0.0/16` for virtual addresses. `scripts/setup-network.sh` configures Linux forwarding/NAT; set `TOTS_UPLINK` if automatic uplink detection is unsuitable. Keep the management API on loopback unless remote access is explicitly configured.

## Windows client

Extract the delivery archive and keep `wintun.dll` beside the Windows `tots-client.exe`. Run TUN operations as Administrator. Save only the server-returned config URI in `client.private.txt`, then run:

```powershell
.\tots-client.exe --config-file .\client.private.txt --relay-only --tun-default-route
```

Useful options:

| Option | Behavior |
| --- | --- |
| `--config-file PATH` | Read the private URI from a file instead of command-line arguments. |
| `--relay-only` | Exclude the direct VPS UDP path; use provider transport. |
| `--probe-only` | Verify an encrypted provider path without opening TUN or installing routes. |
| `--tun-default-route` | Install Windows IPv4 full-tunnel routing and TUN DNS. |
| `--run-for-seconds N` | End the connected TUN run gracefully after a bounded interval. |

For example, add `--run-for-seconds 75` for a bounded test. The route guard pins the VPS and provider endpoint IPv4 addresses to the physical gateway, then removes its owned routes and restores TUN DNS during normal exit, including Ctrl+C and the timer. Cleanup after force-killing the process is not guaranteed.

IPv6 routing is not managed. Automatic full-tunnel routing is currently Windows-only. On Android, an application can supply its `VpnService` fd with `--tun-fd`; iOS requires a NetworkExtension host.

## Transport and encryption

VK uses TURN/UDP allocation, authentication, permissions and channel binding. Its relay requires a 12-byte RTP envelope around each encrypted ToTS datagram. The server recognizes that envelope while continuing to accept raw ToTS datagrams for the direct/legacy path.

Jazz uses actual WebSocket signalling and WebRTC RTP media through the restored `jazz-session` library. `preconnect_call` performs HTTP provisioning on the VPS; `join_preconnected` lets the PC connect directly to Jazz WSS/media endpoints. SSH is used for setup or private configuration retrieval, never for user traffic. Public media keepalives establish the remote track and are filtered before ToTS packet processing.

The Jazz session publishes audio with `muted:false` and explicitly subscribes to remote unmuted AUDIO tracks, deduplicating updates and unsubscribing when tracks are muted. Encrypted path probes retry every 500 ms with fresh sequence numbers within an overall three-second deadline, allowing for initial packet loss during SFU renegotiation.

ToTS frames use XChaCha20-Poly1305 with an Argon2id-derived key, authenticated envelope metadata and replay protection. Its association IDs, fragmentation, probes and flow control are TUIC-inspired design choices; the wire format is original. TURN/TCP and TURN/TLS are not implemented.

## Build and archive delivery

The delivery is one archive containing source, Windows and Linux release binaries, instructions and the test report, with no private configuration or credentials. To rebuild, use stable Rust. The tested toolchain is Rust `1.99.1`; workspace/Jazz minimum-version metadata is `1.89`.

```bash
cargo test --workspace --locked
cargo test --manifest-path Providers/jazz-session/Cargo.toml --locked
cargo build --workspace --release --locked
```

Distribute the built Windows executable with its matching official AMD64 `wintun.dll`, and include `LICENSE`, `NOTICE` and the Jazz MIT license. The Linux server package also needs the systemd unit and network setup script. Preserve `/var/lib/tots/tots.db` and `/etc/tots/server.env` when upgrading. Archive delivery does not require a pull request or published release.

[AGENT.MD](AGENT.MD) describes architecture and ownership; [PROJECT_STATUS.md](PROJECT_STATUS.md) separates implementation from verified behavior. ToTS is a reference implementation and has not received an independent security audit.
