#!/usr/bin/env bash
set -euo pipefail
UPLINK="${TOTS_UPLINK:-$(ip route show default | awk '/default/{print $5; exit}')}"
[[ -n "$UPLINK" ]] || { echo "ToTS: cannot determine uplink" >&2; exit 1; }
sysctl -w net.ipv4.ip_forward=1 >/dev/null
nft list table inet tots >/dev/null 2>&1 || nft add table inet tots
nft list chain inet tots forward >/dev/null 2>&1 || nft 'add chain inet tots forward { type filter hook forward priority 0; policy accept; }'
nft list table ip tots_nat >/dev/null 2>&1 || nft add table ip tots_nat
nft list chain ip tots_nat postrouting >/dev/null 2>&1 || nft 'add chain ip tots_nat postrouting { type nat hook postrouting priority 100; }'
if ! nft list chain ip tots_nat postrouting | grep -q '10.77.0.0/16'; then nft add rule ip tots_nat postrouting ip saddr 10.77.0.0/16 oifname "$UPLINK" masquerade; fi
