ToTS OpenWrt binary
===================

This archive contains the dedicated OpenWrt executable `tots-openwrt` and an
optional procd service. The binary is the same ToTS client dataplane and
provider implementation used by desktop clients; it is not a separate protocol
implementation.

Supported release targets:
  - aarch64: aarch64-unknown-linux-musl
  - x86_64:  x86_64-unknown-linux-musl
  - armv7:   armv7-unknown-linux-musleabihf

Install the archive as root:
  tar -xzf tots-openwrt-<abi>.tar.gz -C /
  mkdir -p /etc/tots
  chmod 700 /etc/tots
  printf '%s\n' 'tots://YOUR_SERVER_ISSUED_URI' > /etc/tots/config.uri
  chmod 600 /etc/tots/config.uri
  uci set tots.main.workers='8'
  uci commit tots
  /etc/init.d/tots-openwrt enable
  /etc/init.d/tots-openwrt start

The service creates `tots0`, writes runtime status and counters to
`/var/run/tots/status.json`, logs to the OpenWrt system log, and handles SIGTERM
for a clean shutdown. The ToTS client does not alter OpenWrt firewall or
policy-routing configuration; configure routing separately if the router is
intended to forward LAN traffic through the TUN.

Manual run:
  /usr/bin/tots-openwrt --config-file /etc/tots/config.uri --n 8 --relay-only

Required runtime:
  /dev/net/tun (kmod-tun), root/CAP_NET_ADMIN, iproute2 and a working provider
  configuration. The TUN interface name defaults to `tots0`.
