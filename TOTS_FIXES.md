# ToTS fixes: TUN/NAT, `tots://` configs and native Jazz room transport

## What changed

- Protocol version is `2`; TUN clients use the explicit `AuthTun` handshake.
- Linux server TUN/NAT (`tots-gw0`) is separate from the legacy UDP backend.
- Windows full-tunnel keeps the transport endpoints on the original physical route.
- `POST /v1/configs` returns `tots://<base64-json>` and the client accepts either
  the URI or the old individual CLI arguments.
- Jazz is now a **provider-native room/SFU transport**. It no longer takes Jazz
  TURN credentials and tries `TURN CreatePermission` toward the ToTS VPS.

## Why the previous Jazz build failed

The previous implementation joined SaluteJazz only to obtain TURN credentials,
then called the generic ToTS TURN transport with `server:turn_port` as the peer.
That produced errors such as:

```text
TURN permission/channel bind failed for YOUR_PUBLIC_IP:50000
```

Those TURN credentials are scoped to the Jazz WebRTC media session. They are
not a general-purpose UDP proxy to arbitrary Internet peers.

## New Jazz data path

For every server-issued Jazz config:

1. `tots-server` creates a dedicated SaluteJazz room and remains joined as the
   server endpoint.
2. The room invite is stored in server state and placed in
   `provider_data.join_link` inside the returned `tots://` URI.
3. `tots-client` joins that same room.
4. Both endpoints perform Jazz `rtc:config`, `rtc:join`, track publication,
   publisher/subscriber SDP and trickle ICE.
5. ToTS encrypted frames are fragmented into RTP payloads and sent through the
   real Jazz `AUDIO/MICROPHONE` media track. The decompiled Jazz client leaves
   `onDataChannel()` empty, so the SFU data-channel path is not used.
6. Jazz signaling/ICE/TURN/SFU domains and public IPv4 candidates are monitored
   continuously. On Windows full-tunnel mode, newly discovered IPv4 endpoints
   receive `/32` routes through the original physical gateway so Jazz cannot
   route recursively through `tots0`.
7. The server decrypts the ToTS frame and forwards `Frame::Data` either to the
   Linux TUN gateway (`AuthTun`) or to the legacy UDP backend (`Auth`).

`UDP/50000` is therefore **not part of the Jazz path anymore**. It is kept for
providers that really expose an ordinary TURN relay and for backward-compatible
server functionality.

## Server

Run on Linux as root or with the privileges needed for TUN, routes and NAT:

```sh
sudo ./tots-server --public-host YOUR_PUBLIC_IP_OR_DNS --tun-gateway
```

`--public-host` is still used by direct WebRTC / generic provider transports and
is kept in the config schema. The native Jazz data path itself does not create a
TURN permission to this address.

## Create a new Jazz config

Jazz URIs from the old generic-TURN implementation must be regenerated because
they do not contain the server-created room invite. URIs from the immediately
previous native-room build already contain `provider_data.join_link` and remain
schema-compatible with this RTP carrier.

```sh
curl -sS -X POST http://127.0.0.1:8787/v1/configs \
  -H 'content-type: application/json' \
  -d '{"provider":"jazz","transport":"jazz","provider_data":{"name":"ToTS client"},"tun":true,"tun_default_route":true}'
```

The response is a secret URI:

```text
tots://<url-safe-base64-json>
```

Its JSON contains the usual ToTS credentials plus a private
`provider_data.join_link`. Do not publish or log the URI unnecessarily.

## Windows client

Run PowerShell as Administrator and keep `wintun.dll` next to the executable:

```powershell
.\tots-client.exe --config="tots://NEW_CONFIG_HERE"
```

The old CLI form is still supported, but native Jazz requires the server-issued
`provider_data.join_link`; creating a config through `/v1/configs` is the normal
workflow.

The client resolves and pins Jazz signaling/ICE/SFU IPv4 endpoints before the
full-tunnel `/1` routes are installed, then refreshes the Jazz signaling/SDP/ICE
snapshot every two seconds. Newly discovered public IPv4 endpoints receive `/32`
routes through the original gateway. Domains and public routeable addresses are
logged without printing the secret invite URL or TURN credentials. This prevents
the live Jazz connection from routing into `tots0` after takeover. Jazz ICE is
restricted to UDP/IPv4 and filters Windows APIPA (`169.254.0.0/16`) addresses so
disconnected/link-local adapters are not handed to the ICE socket binder.

## Expected healthy logs

Server (after config creation / client join):

```text
Jazz room data transport is ready device_id=...
```

Client:

```text
provider relay forced; skipping direct WebRTC probe provider="jazz"
connecting provider-native transport if available provider="jazz"
Jazz direct-route domain discovered domain=...
Jazz direct-route IPv4 discovered ip=...
selecting provider-native transport transport="jazz"
device authorized transport="jazz" device_id=...
ToTS TUN interface is up ... transport="jazz"
Windows full-tunnel IPv4 routes installed ...
```

The Jazz path must no longer emit:

```text
provider TURN signaling allocation failed
TURN permission/channel bind failed for YOUR_PUBLIC_IP:50000
```

because it no longer calls the generic `TurnTransport::connect()` path.

## Compatibility / persistence

- Update `tots-server` and `tots-client` together.
- Existing non-Jazz configs continue to use the existing direct/generic paths.
- Existing server state is readable; the new `jazz_rooms` field has a serde
  default.
- A newly created Jazz room is associated with its `device_id` and persisted in
  the server state file. The server attempts to rejoin persisted rooms after a
  process restart. If the third-party Jazz service has already expired/closed a
  room while the server was offline, issue a new config for that device.

## Build verification

Run these on a machine with the Rust toolchain installed:

```sh
cargo test --locked --workspace
cargo build --locked --release
```

`jazz-session` intentionally uses its pinned `webrtc = 0.17` API, while the main
ToTS workspace continues to use its existing WebRTC dependency for the direct
transport.
