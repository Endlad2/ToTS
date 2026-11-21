#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ABI="${1:-}"
case "$ABI" in
  aarch64) TARGET="aarch64-unknown-linux-musl" ;;
  x86_64)  TARGET="x86_64-unknown-linux-musl" ;;
  armv7)   TARGET="armv7-unknown-linux-musleabihf" ;;
  *)
    echo "Usage: $0 {aarch64|x86_64|armv7}" >&2
    exit 2
    ;;
esac

command -v cargo >/dev/null 2>&1 || { echo "cargo is required" >&2; exit 1; }
command -v cargo-zigbuild >/dev/null 2>&1 || { echo "cargo-zigbuild is required (cargo install cargo-zigbuild --locked)" >&2; exit 1; }
command -v zig >/dev/null 2>&1 || { echo "zig is required on PATH" >&2; exit 1; }

cd "$ROOT"
rustup target add "$TARGET"
cargo zigbuild -p tots-openwrt --release --target "$TARGET"

BIN="$ROOT/target/$TARGET/release/tots-openwrt"
[ -x "$BIN" ] || { echo "Missing OpenWrt binary: $BIN" >&2; exit 1; }

DIST="$ROOT/dist/openwrt/$ABI"
rm -rf "$DIST"
mkdir -p "$DIST/root/usr/bin" "$DIST/root/usr/share/licenses/tots-openwrt" "$DIST/root/usr/share/doc/tots-openwrt" "$DIST/root/etc/init.d" "$DIST/root/etc/config"
install -m 0755 "$BIN" "$DIST/root/usr/bin/tots-openwrt"
install -m 0755 "$ROOT/openwrt/tots-openwrt.init" "$DIST/root/etc/init.d/tots-openwrt"
install -m 0644 "$ROOT/openwrt/tots.config" "$DIST/root/etc/config/tots"
install -m 0644 "$ROOT/LICENSE" "$DIST/root/usr/share/licenses/tots-openwrt/LICENSE"
install -m 0644 "$ROOT/NOTICE" "$DIST/root/usr/share/licenses/tots-openwrt/NOTICE"
cp "$ROOT/openwrt/README-OPENWRT.txt" "$DIST/README-OPENWRT.txt"
install -m 0644 "$ROOT/openwrt/README-OPENWRT.txt" "$DIST/root/usr/share/doc/tots-openwrt/README-OPENWRT.txt"

ARCHIVE="$ROOT/dist/tots-openwrt-$ABI.tar.gz"
mkdir -p "$ROOT/dist"
tar -C "$DIST/root" -czf "$ARCHIVE" .
(cd "$ROOT/dist" && sha256sum "tots-openwrt-$ABI.tar.gz" > "tots-openwrt-$ABI.tar.gz.sha256")
file "$BIN"
readelf -h "$BIN" | grep -E 'Class:|Machine:'
echo "Created $ARCHIVE"
