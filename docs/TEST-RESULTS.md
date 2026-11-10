# Test results — 2026-11-10

Date zone: Europe/Moscow (UTC+03:00). These results distinguish automated checks, elevated TUN checks and actual provider VPN egress. No private config URI, invite or credential is included.

## Build and automated checks

| Check | Recorded result |
| --- | --- |
| Stable Rust | 1.99.1; workspace/Jazz minimum-version metadata 1.89 |
| Main workspace tests | 22 passed: 5 client, 10 core, 7 server; one Windows TUN test ignored in the ordinary run |
| Bundled Jazz session tests | 26 unit tests and one documentation test passed |
| Workspace Clippy | Passed with `--workspace --all-targets --locked -- -D warnings` |

The Jazz session package is excluded from workspace membership, so its tests run separately:

```bash
cargo test --workspace --locked
cargo test --manifest-path Providers/jazz-session/Cargo.toml --locked
```

## Elevated Windows TUN test

The initial MTU 1200 attempt failed with Windows OS error 87. The generic MTU setter also applied an IPv6 MTU below its minimum. Windows setup now creates the interface first and sets the IPv4 MTU with `netsh`, leaving IPv6 MTU management outside this path.

After that fix, the ignored TUN test was run elevated and passed all three cases: **1200, 576 and 1400**. Windows TUN operations require Administrator and the matching `wintun.dll` beside the executable.

## Live VK VPN — passed

The Windows client ran with `--relay-only --tun-default-route --run-for-seconds 75`, using a private config file. Direct VPS UDP was excluded from path selection.

- Selected VK TURN endpoint: `90.156.236.96:19302`.
- Windows TUN virtual address: `10.77.190.210`.
- TUN connected: `2026-11-09T21:03:58Z`, equivalent to **2026-11-10 00:03:58 Europe/Moscow**.
- Each request used `curl -4 --noproxy '*'` to force IPv4 and bypass configured HTTP proxies.

| IP service | Response while connected |
| --- | --- |
| `api.ipify.org` | `201.58.191.113` |
| `checkip.amazonaws.com` | `201.58.191.113` |
| `ipv4.icanhazip.com` | `201.58.191.113` |

All three responses matched the VPS address. The timed run exited with code **0**, its owned routes were removed, and the original external IP was restored. This establishes provider-relayed IPv4 Internet egress and normal-exit routing cleanup for that VK run.

## Live Jazz VPN — passed

The final Windows release client and deployed Linux release server passed the Jazz Internet-egress check. The client used a private config file with `--relay-only --tun-default-route --run-for-seconds 90`; direct VPS UDP was excluded.

- Selected path: `jazz-rtp`, healthy RTT **662 ms** at `2026-11-10T09:19:47Z`.
- Windows TUN virtual address: `10.77.125.188`.
- TUN connected: `2026-11-10T09:20:09.350436Z`, equivalent to **2026-11-10 12:20:09 Europe/Moscow**.
- Each request used `curl.exe -4 --noproxy '*' --max-time 20` and exited with code **0**.

| IP service | Response while connected |
| --- | --- |
| `api.ipify.org` | `201.58.191.113` |
| `checkip.amazonaws.com` | `201.58.191.113` |
| `ipv4.icanhazip.com` | `201.58.191.113` |

All three responses matched the VPS address, establishing real provider-carried IPv4 Internet egress. The timed client exited with code **0**, stderr was empty, and both full-tunnel `/1` routes were absent after exit.

The same private Jazz config was then reused with the release client and `--relay-only --probe-only`. It exited with code **0** at `2026-11-10T09:22:40Z` (**12:22:40 Europe/Moscow**), reporting healthy `jazz-rtp` RTT **698 ms**. This confirms config reuse for a fresh provider connection after the full VPN run.

The PC's direct Jazz HTTP bootstrap initially failed before TLS with Windows TCP connection timeout 10060. Jazz WSS/media endpoints were reachable. The implementation therefore supports HTTP provisioning on the VPS and direct PC WSS/media connections using the provisioned connector. SSH is used only for setup/config retrieval, never for user packets.

The final session publishes audio explicitly unmuted and sends remote AUDIO subscription changes matching the official client. Initial probes could be lost while the SFU applied a new track's SDP. The shared probe now retries every **500 ms**, generating fresh authenticated sequence numbers under an unchanged **three-second overall deadline**. Regression tests cover a dropped first probe and malformed replies that must not extend the deadline.

## Scope

IPv6 routing, TURN/TCP/TLS, mobile-device egress and long-running throughput/stability were not verified. Cleanup after force-killing the process is not guaranteed; both recorded VPN runs used graceful timers.
