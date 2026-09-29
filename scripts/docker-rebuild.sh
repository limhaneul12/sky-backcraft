#!/bin/sh
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

docker compose build app

if [ -n "${PUBLIC_DOMAIN:-}" ]; then
  validate_domain "$PUBLIC_DOMAIN"
  if [ -z "${CLOUDFLARE_TUNNEL_TOKEN:-}" ]; then
    printf 'PUBLIC_DOMAIN requires CLOUDFLARE_TUNNEL_TOKEN for a remotely-managed tunnel\n' >&2
    exit 2
  fi
  docker compose --profile quick stop quick-tunnel
  docker compose --profile managed pull managed-tunnel
  docker compose --profile managed up --detach app managed-tunnel
  printf 'Local MCP:  http://127.0.0.1:%s/mcp\n' "${SPOT_LAB_HOST_PORT:-8130}"
  printf 'Public MCP: https://%s/mcp\n' "$PUBLIC_DOMAIN"
  exit 0
fi

docker compose --profile managed stop managed-tunnel

quick_url=
quick_container=$(docker compose --profile quick ps --quiet quick-tunnel)
if [ -n "$quick_container" ] \
  && [ "$(docker inspect --format '{{.State.Running}}' "$quick_container")" = "true" ]; then
  quick_url=$(docker compose logs --no-color quick-tunnel 2>&1 \
    | grep -Eo 'https://[a-z0-9-]+\.trycloudflare\.com' \
    | tail -n 1 || true)
fi

if [ -z "$quick_url" ]; then
  docker compose --profile quick rm --force --stop quick-tunnel
  docker compose --profile quick pull quick-tunnel
  docker compose --profile quick up --detach quick-tunnel
fi

attempt=0
while [ -z "$quick_url" ] && [ "$attempt" -lt 60 ]; do
  quick_url=$(docker compose logs --no-color quick-tunnel 2>&1 \
    | grep -Eo 'https://[a-z0-9-]+\.trycloudflare\.com' \
    | tail -n 1 || true)
  if [ -n "$quick_url" ]; then
    break
  fi
  attempt=$((attempt + 1))
  sleep 1
done

if [ -z "$quick_url" ]; then
  printf 'cloudflared did not issue a Quick Tunnel URL within 60 seconds\n' >&2
  docker compose --profile quick logs quick-tunnel >&2
  docker compose --profile quick stop quick-tunnel
  exit 1
fi

PUBLIC_DOMAIN=${quick_url#https://}
validate_domain "$PUBLIC_DOMAIN"
export PUBLIC_DOMAIN
docker compose up --detach app

printf 'Local MCP:  http://127.0.0.1:%s/mcp\n' "${SPOT_LAB_HOST_PORT:-8130}"
printf 'Public MCP: %s/mcp\n' "$quick_url"
printf 'Quick Tunnel is transient, has no SLA, and does not support SSE. Re-run make rebuild after it exits.\n'
