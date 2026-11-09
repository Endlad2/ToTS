#!/usr/bin/env bash
set -euo pipefail
REPO="Endlad2/ToTS"
ASSET="${TOTS_ASSET:-tots-server-x86_64-unknown-linux-gnu.tar.gz}"
PREFIX="${TOTS_PREFIX:-/usr/local}"
STATE=/var/lib/tots
ETC=/etc/tots
SERVICE=/etc/systemd/system/tots-server.service

if [[ "$REPO" == YOUR_GITHUB_ORG/* ]]; then echo "Set TOTS_REPO=owner/repo (or edit the default in this script)." >&2; exit 2; fi
[[ $EUID -eq 0 ]] || { echo "run as root" >&2; exit 1; }
for x in curl tar install systemctl python3; do command -v "$x" >/dev/null || { echo "missing $x" >&2; exit 1; }; done
mkdir -p "$STATE" "$ETC" "$PREFIX/lib/tots"
chmod 700 "$STATE"
# Preserve DB/config across upgrades. Backup before replacement if they already exist.
ts=$(date +%Y%m%d-%H%M%S)
[[ -f "$STATE/tots.db" ]] && cp -a "$STATE/tots.db" "$STATE/tots.db.backup-$ts"
[[ -f "$ETC/server.env" ]] && cp -a "$ETC/server.env" "$ETC/server.env.backup-$ts"

tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
api="https://api.github.com/repos/${REPO}/releases/latest"
url=$(curl -fsSL "$api" | ASSET="$ASSET" python3 -c 'import json,sys,os; d=json.load(sys.stdin); a=os.environ["ASSET"]; print(next(x["browser_download_url"] for x in d["assets"] if x["name"]==a))')
curl -fL "$url" -o "$tmp/pkg.tar.gz"
tar -xzf "$tmp/pkg.tar.gz" -C "$tmp"
systemctl stop tots-server.service 2>/dev/null || true
install -m 0755 "$tmp/tots-server" "$PREFIX/bin/tots-server.new"
mv -f "$PREFIX/bin/tots-server.new" "$PREFIX/bin/tots-server"
install -m 0755 "$tmp/setup-network.sh" "$PREFIX/lib/tots/setup-network.sh"
install -m 0644 "$tmp/tots-server.service" "$SERVICE"
if [[ ! -f "$ETC/server.env" ]]; then install -m 0600 "$tmp/config.example.env" "$ETC/server.env"; echo "Edit $ETC/server.env and set TOTS_PUBLIC_HOST before starting."; fi
systemctl daemon-reload
systemctl enable tots-server.service >/dev/null
if grep -q '^TOTS_PUBLIC_HOST=203\.0\.113\.10$' "$ETC/server.env"; then echo "Installed, but service not started because TOTS_PUBLIC_HOST is still the example address."; exit 0; fi
systemctl restart tots-server.service
systemctl --no-pager --full status tots-server.service || true
