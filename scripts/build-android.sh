#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TARGET="${1:-aarch64-linux-android}"
API="${ANDROID_API:-24}"

if ! [[ "$API" =~ ^[0-9]+$ ]]; then
  echo "ANDROID_API must be a numeric Android API level (minimum: 24), got: $API" >&2
  exit 2
fi
if (( API < 24 )); then
  echo "ANDROID_API=$API is too old: ToTS WebRTC dependencies use getifaddrs/freeifaddrs, available on Android from API 24." >&2
  exit 2
fi

case "$TARGET" in
  aarch64-linux-android)
    ABI="arm64-v8a"
    CLANG_PREFIX="aarch64-linux-android"
    ;;
  armv7-linux-androideabi)
    ABI="armeabi-v7a"
    CLANG_PREFIX="armv7a-linux-androideabi"
    ;;
  x86_64-linux-android)
    ABI="x86_64"
    CLANG_PREFIX="x86_64-linux-android"
    ;;
  i686-linux-android)
    ABI="x86"
    CLANG_PREFIX="i686-linux-android"
    ;;
  *)
    echo "unsupported Android Rust target: $TARGET" >&2
    exit 2
    ;;
esac

NDK="${ANDROID_NDK_HOME:-${ANDROID_NDK_ROOT:-${ANDROID_NDK_LATEST_HOME:-}}}"
if [[ -z "$NDK" && -n "${ANDROID_HOME:-}" && -d "$ANDROID_HOME/ndk" ]]; then
  NDK="$(find "$ANDROID_HOME/ndk" -mindepth 1 -maxdepth 1 -type d | sort -V | tail -n 1)"
fi
if [[ -z "$NDK" || ! -d "$NDK/toolchains/llvm/prebuilt" ]]; then
  echo "Android NDK not found. Set ANDROID_NDK_HOME/ANDROID_NDK_ROOT/ANDROID_NDK_LATEST_HOME." >&2
  exit 2
fi

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) HOST_TAG="linux-x86_64" ;;
  Darwin-x86_64) HOST_TAG="darwin-x86_64" ;;
  Darwin-arm64) HOST_TAG="darwin-arm64" ;;
  *)
    echo "unsupported NDK host: $(uname -s)-$(uname -m)" >&2
    exit 2
    ;;
esac

TOOLCHAIN="$NDK/toolchains/llvm/prebuilt/$HOST_TAG/bin"
LINKER="$TOOLCHAIN/${CLANG_PREFIX}${API}-clang"
CXX="$TOOLCHAIN/${CLANG_PREFIX}${API}-clang++"
AR="$TOOLCHAIN/llvm-ar"

for tool in "$LINKER" "$CXX" "$AR"; do
  [[ -x "$tool" ]] || { echo "missing NDK tool: $tool" >&2; exit 2; }
done

rustup target add "$TARGET"

TARGET_KEY="${TARGET^^}"
TARGET_KEY="${TARGET_KEY//-/_}"
TARGET_KEY_LOWER="${TARGET//-/_}"

export "CARGO_TARGET_${TARGET_KEY}_LINKER=$LINKER"
export "CC_${TARGET_KEY_LOWER}=$LINKER"
export "CXX_${TARGET_KEY_LOWER}=$CXX"
export "AR_${TARGET_KEY_LOWER}=$AR"

mkdir -p target/host-tools
rustc tots-android/build.rs -O -o target/host-tools/tots-android-builder

unset OUT_DIR

TOTS_REPO_ROOT="$ROOT" \
TOTS_ANDROID_TARGET="$TARGET" \
TOTS_ANDROID_ABI="$ABI" \
TOTS_ANDROID_OUTPUT="$ROOT/dist/android/$ABI/libtots.so" \
  target/host-tools/tots-android-builder

test -f "dist/android/$ABI/libtots.so"
echo "Android artifact: dist/android/$ABI/libtots.so"
