#!/bin/sh
set -eu

set -- mcp-serve \
  --allow-network-bind \
  --bind 0.0.0.0 \
  --port 8130 \
  --data-root /var/lib/spot-lab

if [ -n "${PUBLIC_DOMAIN:-}" ]; then
  set -- "$@" --public-no-auth --allow-host "$PUBLIC_DOMAIN"
fi

exec /usr/local/bin/spot-lab "$@"

