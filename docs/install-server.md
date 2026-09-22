# ToTS — server installation guide

ToTS (**T**unnel **o**ver **T**ransports) is a Rust workspace that carries a
WireGuard/Xray payload over messenger / document channels (VK, Yandex Telemost,
VK Stream, Yandex Docs, Mail Docs, MAX, OneME).

This guide installs the **exit node** (server) that terminates the tunnel.

## 1. Requirements

- Linux server (x86_64 or aarch64), root or `sudo`
- A public IP, UDP `51820` open for the WireGuard side
- A ToTS config link (`tots://...`) issued by the client / operator

## 2. Install the binary

### Option A — download a release build

```bash
VERSION=v0.1.0
ARCH=$(uname -m)   # x86_64 | aarch64
curl -L -o tots.tar.gz \
  "https://github.com/Endlad2/ToTS/releases/download/${VERSION}/tots-linux-${ARCH}.tar.gz"
tar xzf tots.tar.gz
sudo install -m0755 tots /usr/local/bin/tots
tots --version
```

### Option B — build from source

```bash
sudo apt-get update && sudo apt-get install -y build-essential pkg-config
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
git clone https://github.com/Endlad2/ToTS.git
cd ToTS
cargo build --release -p tots-cli
sudo install -m0755 target/release/tots /usr/local/bin/tots
```

## 3. Configure

Create the exit-node config:

```bash
sudo mkdir -p /etc/tots
sudo tee /etc/tots/server.toml >/dev/null <<'EOF'
# ToTS exit-node configuration
link          = "tots://REPLACE_WITH_YOUR_LINK"
transport     = "yandex_docs"   # vk | telemost | vk_stream | yandex_docs | mail_docs | max | oneme
payload       = "wireguard"     # wireguard (UDP) | xray (TCP)
listen        = "0.0.0.0:51820"
room          = "tots-server"
log_level     = "info"
EOF
```

Verify the config parses:

```bash
tots info --config /etc/tots/server.toml
tots transports
```

## 4. Run as a systemd service

```bash
sudo tee /etc/systemd/system/tots.service >/dev/null <<'EOF'
[Unit]
Description=ToTS tunnel exit node
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/tots run --config /etc/tots/server.toml
Restart=on-failure
RestartSec=3
LimitNOFILE=1048576
AmbientCapabilities=CAP_NET_ADMIN CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_ADMIN CAP_NET_BIND_SERVICE
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now tots
sudo systemctl status tots --no-pager
journalctl -u tots -f
```

## 5. WireGuard wiring (UDP payload)

When `payload = "wireguard"`, ToTS presents a UDP endpoint that the client's
WireGuard peer dials. On the server:

```bash
sudo sysctl -w net.ipv4.ip_forward=1
sudo iptables -t nat -A POSTROUTING -o eth0 -j MASQUERADE
```

Persist forwarding:

```bash
echo 'net.ipv4.ip_forward=1' | sudo tee /etc/sysctl.d/99-tots.conf
sudo sysctl --system
```

## 6. Xray wiring (TCP payload)

When `payload = "xray"`, ToTS exposes a TCP listener that Xray/VMess/VLESS can
use as its outbound transport. Point the Xray client's `address`/`port` at the
ToTS listener and route traffic through the resulting SOCKS/HTTP inbound.

## 7. Link format

```
tots://<transport>?room=<id>&transport_key=<hex32>&payload=<wireguard|xray>&relay=<host:port>
```

- `transport` — one of the 7 supported transports
- `room` — channel / document id shared by both peers
- `transport_key` — 32-byte hex pre-shared key (X25519 + ChaCha20-Poly1305)
- `payload` — `wireguard` (UDP) or `xray` (TCP)
- `relay` — optional signalling relay `host:port`

## 8. Health check

```bash
systemctl is-active tots        # -> active
ss -lunp | grep 51820           # WireGuard listener
tots info --config /etc/tots/server.toml
```

## 9. Updating

```bash
sudo systemctl stop tots
# re-run the download or build steps, then:
sudo install -m0755 tots /usr/local/bin/tots
sudo systemctl start tots
```

## 10. Uninstall

```bash
sudo systemctl disable --now tots
sudo rm -f /etc/systemd/system/tots.service /usr/local/bin/tots
sudo rm -rf /etc/tots
sudo systemctl daemon-reload
```