#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  openflux-exit.sh <yandex|vyandex|boards|cupsonline|mailru> [URL]

Environment:
  OPENFLUX_BIN           OpenFlux executable (default: openflux)
  YANDEX_COOKIES_FILE    Netscape cookies.txt for vyandex
  OPENFLUX_COOKIE_STORE  Optional OpenFlux cookie-store path

Examples:
  ./scripts/openflux-exit.sh yandex 'https://docs.yandex.ru/...'
  YANDEX_COOKIES_FILE=/root/yandex-cookies.txt ./scripts/openflux-exit.sh vyandex 'https://docs.yandex.ru/...'
  ./scripts/openflux-exit.sh cupsonline

For cupsonline, OpenFlux can create rooms when URL is omitted; copy the base64
room list printed by the exit into provider_data.cupsonline.url.
EOF
}

[[ $# -ge 1 ]] || { usage >&2; exit 2; }
transport="$1"
url="${2:-}"
case "$transport" in
  yandex|vyandex|boards|cupsonline|mailru) ;;
  *) echo "unsupported OpenFlux transport: $transport" >&2; exit 2 ;;
esac

bin="${OPENFLUX_BIN:-openflux}"
args=("--role=exit" "--mode=l3" "--transport=$transport")
if [[ -n "$url" ]]; then
  args+=("--url=$url")
elif [[ "$transport" != "cupsonline" ]]; then
  echo "$transport requires URL" >&2
  exit 2
fi
if [[ "$transport" == "vyandex" && -n "${YANDEX_COOKIES_FILE:-}" ]]; then
  args+=("--yandex-cookies-file=$YANDEX_COOKIES_FILE")
fi
if [[ -n "${OPENFLUX_COOKIE_STORE:-}" ]]; then
  args+=("--cookie-store=$OPENFLUX_COOKIE_STORE")
fi

exec "$bin" "${args[@]}"
