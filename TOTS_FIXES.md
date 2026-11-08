# Current default: SOCKS5 over QUIC

The current client defaults to SOCKS5 over a nested QUIC session carried by the
selected ToTS WebRTC/provider transport. See [`QUIC_SOCKS5.md`](QUIC_SOCKS5.md).
The TUN/full-tunnel material below is retained as historical/legacy compatibility
documentation and is no longer the default data path. Wire protocol version is
now `3`.

---

# ToTS fixes: TUN/NAT, `tots://` configs and native Jazz room transport

## What changed

- Historical TUN protocol version was `2`; current wire version `3` adds `AuthProxy`.
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




## VK and MAX provider TURN transports

VK and MAX now have complete credential acquisition paths instead of the former
fail-closed MAX placeholder / incomplete VK path. The implementation is clean-room:
the PolyForm reference was used only to identify externally observable request
sequences and response fields; no source file from it is included in this tree.

Both providers obtain call-scoped TURN credentials, but their ToTS bootstrap now
differs. VK is `RelayScope::ProviderSession`: credentials are passed to relay-only
WebRTC ICE, so ICE performs TURN allocation/permission/consent before DTLS, SCTP
and the ToTS DataChannel. The SDP offer/answer is exchanged over the ordinary
ToTS WebRTC signaling endpoint. MAX keeps the existing generic RFC TURN bootstrap
toward `turn_port`, followed by the normal WebRTC DataChannel setup. There is no
synthetic SRTP, fake STUN consent, 0xFF keepalive, or DPI-specific camouflage.

VK packet path:

```text
tots-client
  -> HTTP SDP offer to ToTS webrtc_port
  -> VK TURN as relay-only ICE candidate
  -> TURN Allocate/CreatePermission + ICE checks/consent
  -> DTLS -> SCTP -> WebRTC DataChannel
  -> tots-server -> TUN gateway / Internet
```

MAX packet path remains:

```text
tots-client
  -> MAX TURN allocation
  -> ToTS server UDP turn_port
  -> encrypted SDP exchange
  -> WebRTC ICE -> DTLS -> SCTP -> DataChannel
```

Jazz stays `ProviderSession` with its provider-native RTP room transport.

### VK provider data

Anonymous join-link mode remains supported:

```json
{
  "join_link": "https://vk.ru/call/join/..."
}
```

Authenticated account mode is also supported:

```json
{
  "access_token": "VK_ACCESS_TOKEN"
}
```

Optionally set `conversation_id` to reuse an existing Calls conversation or
`app_id` to override the default VK application id. `captcha_success_token` is
still accepted as an override for the anonymous join-link flow; when omitted,
the embedded interactive helper is launched on error 14.

### MAX provider data

MAX requires an access token and either a group id (ToTS creates a Calls
conversation) or an existing conversation id:

```json
{
  "access_token": "MAX_ONEME_TOKEN",
  "group_id": "-12345678"
}
```

or:

```json
{
  "access_token": "MAX_ONEME_TOKEN",
  "conversation_id": "00000000-0000-0000-0000-000000000000"
}
```

The MAX bootstrap performs the observable web flow `opcode 6 -> 19 -> 158`,
then Calls `auth.anonymLogin`, `system.getInfo`, `vchat.startConversation`, and
`vchat.getConversationParams` to obtain the UDP TURN set.

`--transport vk` and `--transport max` force the corresponding provider relay.
The older `--transport turn --provider vk|max` form remains valid.

### Server reachability

For VK, TCP `webrtc_port` (default `8443`) must be reachable for SDP signaling and
the server must advertise a public ICE candidate that VK TURN can reach. For MAX,
UDP `turn_port` (default `50000`) remains publicly reachable because its generic
TURN bootstrap creates a permission/channel to that address. `--public-host` must
therefore still point at the real public server address.

### Full-tunnel bypass

Before installing the Windows `/1` full-tunnel routes, the client resolves and
logs the provider control/TURN endpoints and pins their public IPv4 addresses
through the original physical gateway. Jazz keeps its continuous route watcher;
VK/MAX pin the control and TURN endpoint set established during bootstrap; VK
also includes `id.vk.ru` for an interactive CAPTCHA that occurs before TUN setup.

Provider access tokens, TURN username/password and the full config URI are not
printed by these route logs.

## VK embedded interactive CAPTCHA helper

`Providers/vk/build.rs` now builds `Providers/vk/captcha-helper` with Go for the active Rust target and embeds the resulting helper into `tots-provider-vk` at compile time. When VK returns error 14 and no `provider_data.captcha_success_token` was supplied, the provider validates the VK redirect URL, writes the embedded helper to the OS temp directory, runs it, waits for the user to complete the visible VK challenge, consumes the machine-readable `TOTS_CAPTCHA_SUCCESS_TOKEN=...` result, deletes the temporary executable, and resumes `calls.getAnonymousToken` in the same ToTS process.

The helper is deliberately interactive. It does not inject stealth/fingerprint overrides, auto-click the checkbox, synthesize CAPTCHA answers, or retry against VK in a tight loop.

VK call TURN credentials are marked provider-session scoped again. The core therefore feeds them to the relay-only WebRTC ICE configuration instead of sending ToTS frames directly over a fresh generic TURN allocation. ICE performs allocation/permission handling first; normal DTLS/SCTP/DataChannel establishment follows before ToTS Probe/Auth/Data frames are sent.
