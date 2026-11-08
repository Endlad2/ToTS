# ToTS SOCKS5 over QUIC

The default ToTS client data plane is now a local SOCKS5 proxy instead of a
layer-3 TUN/full-tunnel interface.

## Data path

```text
application
  -> SOCKS5 127.0.0.1:7000
  -> nested QUIC connection
  -> authenticated ToTS provider carrier
     (direct WebRTC / VK WebRTC+TURN / MAX / Jazz native carrier)
  -> tots-server
  -> remote TCP or UDP destination
```

The QUIC endpoint uses a virtual UDP socket. Quinn's QUIC datagrams are carried
inside the already selected ToTS provider transport, so the nested QUIC layer
does not need a separate public UDP port. The outer provider transport remains
responsible for ICE/TURN/WebRTC/provider signaling.

SOCKS5 domain targets are preserved as host names and resolved by the server.
This gives remote DNS for clients that send domain-form SOCKS5 requests.

## Protocol

ToTS wire protocol version is now `3`. `Frame::AuthProxy` selects the proxy data
plane. Update the client and server together.

Each SOCKS5 TCP CONNECT maps to its own QUIC bidirectional stream. SOCKS5 UDP
ASSOCIATE uses a dedicated QUIC bidirectional stream carrying framed UDP
records. SOCKS5 UDP fragmentation (`FRAG != 0`) is intentionally not supported.

## Server

The normal proxy mode does not require Linux TUN, NAT, `CAP_NET_ADMIN`, or
`--tun-gateway`:

```sh
./tots-server --public-host YOUR_PUBLIC_IP_OR_DNS
```

Provider-specific requirements still apply to the outer transport. For
example, the configured WebRTC signaling endpoint must remain reachable when a
provider path uses it.

## Config creation

Example for VK:

```sh
curl -sS -X POST http://127.0.0.1:8787/v1/configs \
  -H 'content-type: application/json' \
  -d '{
    "provider":"vk",
    "transport":"vk",
    "provider_data":{
      "join_link":"https://vk.ru/call/join/YOUR_CALL_CODE"
    },
    "local_port":7000
  }'
```

New server-issued configs set `tun=false` and `tun_default_route=false`.

## Client

```powershell
.\tots-client.exe --config="tots://..."
```

A healthy startup ends with a line similar to:

```text
ToTS SOCKS5-over-QUIC proxy is listening bind=127.0.0.1:7000 carrier="vk"
```

Point the application at SOCKS5 `127.0.0.1:7000`. Administrator privileges,
Wintun, default-route takeover, and per-provider bypass routes are not required
for the normal proxy mode.

The old `--tun` code path remains only as an explicit compatibility mode. The
`tun` flag embedded in older `tots://` URIs is ignored by the client unless
`--tun` is explicitly supplied on the command line.

## Security boundary

Nested QUIC uses TLS 1.3. The client embeds a dedicated ToTS nested-QUIC root
CA, while the server presents a separate `CA=FALSE` end-entity certificate for
`tots-quic.internal` with the `serverAuth` EKU. This avoids treating a CA
certificate as a TLS server leaf, which current rustls/webpki rejects with
`CaUsedAsEndEntity`.

The authenticated/encrypted outer ToTS session remains the main trust boundary;
the nested QUIC certificate/key are bundled protocol credentials. Client and server
must be updated together when this embedded CA/leaf pair changes. This change does
not add DPI-specific camouflage, fake STUN traffic, orphan SRTP, or
protocol-signature avoidance logic.
