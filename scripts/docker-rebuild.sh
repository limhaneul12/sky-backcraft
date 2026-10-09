#!/bin/sh
# Build the spot-lab image and (re)create the app container.
#
# Public exposure is operator-managed: set PUBLIC_DOMAIN to the hostname your
# own Cloudflare tunnel (or equivalent) routes to this container's local port.
# No tunnel containers are started here.
#
# Usage: scripts/docker-rebuild.sh
# Env:   PUBLIC_DOMAIN (optional), SPOT_LAB_HOST_PORT (default 8130)
set -eu

project=${COMPOSE_PROJECT_NAME:-sky-backcraft}
export COMPOSE_PROJECT_NAME=$project

validate_domain() {
  candidate=$1
  if ! printf '%s\n' "$candidate" | grep -Eq '^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?$' \
    || printf '%s\n' "$candidate" | grep -q '\.\.'; then
    printf 'invalid PUBLIC_DOMAIN: expected a lowercase hostname without scheme, port, path, or wildcard\n' >&2
    exit 2
  fi
}

if [ -n "${PUBLIC_DOMAIN:-}" ]; then
  validate_domain "$PUBLIC_DOMAIN"
fi

revision=$(git rev-parse HEAD)
if ! printf '%s\n' "$revision" | grep -Eq '^[0-9a-f]{40}$'; then
  printf 'unable to resolve a full lowercase Git revision for this build\n' >&2
  exit 2
fi
git_state=$(git status --porcelain --untracked-files=normal)
if [ -n "$git_state" ]; then
  revision="${revision}-dirty"
fi
SKY_BACKCRAFT_BUILD_REVISION=$revision
export SKY_BACKCRAFT_BUILD_REVISION

docker compose build \
  --build-arg "SKY_BACKCRAFT_BUILD_REVISION=$SKY_BACKCRAFT_BUILD_REVISION" \
  --build-arg "SKY_BACKCRAFT_VERIFICATION_RECEIPT=${SKY_BACKCRAFT_VERIFICATION_RECEIPT:-}" \
  app

if [ -n "${PUBLIC_DOMAIN:-}" ]; then
  docker compose up --detach app --remove-orphans
  printf 'Local MCP:  http://127.0.0.1:%s/mcp\n' "${SPOT_LAB_HOST_PORT:-8130}"
  printf 'Public MCP: https://%s/mcp\n' "$PUBLIC_DOMAIN"
  printf 'Route your own Cloudflare tunnel (or equivalent) to 127.0.0.1:%s and\n' "${SPOT_LAB_HOST_PORT:-8130}"
  printf 're-run this script after the tunnel hostname changes.\n'
else
  docker compose up --detach app --remove-orphans
  printf 'Local MCP:  http://127.0.0.1:%s/mcp\n' "${SPOT_LAB_HOST_PORT:-8130}"
  printf 'PUBLIC_DOMAIN not set: local only (no public exposure).\n'
fi
