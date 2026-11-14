# OpenFlux carrier adapter

ToTS supports the carrier names `yandex`, `vyandex`, `boards`, `cupsonline`, and `mailru` without importing GPL code into the Apache-2.0 workspace. The ToTS client talks to a separately installed OpenFlux process through SOCKS5 UDP ASSOCIATE. ToTS encryption, sessions, worker recovery, ACK/recovery, flow scheduling, and multi-provider failover remain unchanged.

OpenFlux itself is **not bundled** in this repository. Put `openflux.exe` next to `tots-client.exe`, put `openflux` in `PATH`, or use `--openflux-binary PATH`. The OpenFlux program remains under its own GPLv3 license.

## Carrier configuration

Each carrier lives inside the normal multi-provider `provider_data` object:

```json
{
  "yandex": {
    "url": "https://docs.yandex.ru/..."
  },
  "vyandex": {
    "url": "https://docs.yandex.ru/..."
  },
  "boards": {
    "url": "https://..."
  },
  "cupsonline": {
    "url": "BASE64_ROOM_LIST_FROM_EXIT"
  },
  "mailru": {
    "url": "AbCdEfGh1/IjKlMnOp2"
  }
}
```

`target` is the UDP address that the OpenFlux **exit** dials. If omitted, ToTS uses the server `peer:turn_port` from the `tots://` URI. A colocated exit may use an explicit local target only after verifying that the chosen OpenFlux exit mode handles that address correctly.

Optional per-carrier fields:

- `socks5_addr`: local OpenFlux client SOCKS5 listener. Defaults: yandex `127.0.0.1:11801`, vyandex `:11802`, boards `:11803`, cupsonline `:11804`, mailru `:11805`.
- `auto_launch`: defaults to `true`. When false, ToTS expects an already-running OpenFlux SOCKS5 client.
- `route_hosts`: extra hostnames/IPs that ToTS should bypass when `--tun-default-route` is used.
- `cookie_store`: local OpenFlux cookie-store path.
- `openflux_binary`: local executable override. Prefer the CLI `--openflux-binary` so machine-local paths do not have to be stored in server-issued config URIs.
- `yandex_cookies_file`: `vyandex` only. Prefer CLI `--yandex-cookies-file`.

## Exit side

Run one OpenFlux exit for every carrier URL used by the ToTS config. A helper is included:

```bash
./scripts/openflux-exit.sh yandex 'https://docs.yandex.ru/...'
./scripts/openflux-exit.sh boards 'https://...'
./scripts/openflux-exit.sh mailru 'AbCdEfGh1/IjKlMnOp2'
```

For `vyandex`:

```bash
YANDEX_COOKIES_FILE=/root/yandex-cookies.txt \
  ./scripts/openflux-exit.sh vyandex 'https://docs.yandex.ru/...'
```

For `cupsonline`, the exit may create the room set. Start it without a URL, copy the base64 room list it prints, and place that value in `provider_data.cupsonline.url`:

```bash
./scripts/openflux-exit.sh cupsonline
```

The OpenFlux exit must use L3 mode for ToTS's SOCKS5 UDP carrier path.

## Windows client

With `openflux.exe` next to `tots-client.exe`:

```powershell
.\tots-client.exe --config 'tots://...' --n 20 --relay-only --tun-default-route
```

For `vyandex` with a logged-in Yandex session:

```powershell
.\tots-client.exe --config 'tots://...' --n 20 --relay-only --tun-default-route `
  --yandex-cookies-file 'C:\path\yandex-cookies.txt'
```

The OpenFlux sidecar is started once per configured carrier and shared by all ToTS workers for that carrier. Every ToTS worker still creates its own SOCKS5 UDP association/session, so worker IDs do not collide.

## Full-tunnel bypass note

The adapter resolves the carrier URL host plus known service hosts and exposes those IPs to ToTS's route installer. Some services can move to dynamic balancer hosts. If a carrier reconnects after the default route is installed and loops into the TUN, add the service's current balancer hostname/IP to `route_hosts` or run the OpenFlux client sidecar with platform-specific bypass routing outside ToTS.
