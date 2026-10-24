#!/bin/sh
# Container CI: retain dependency artifacts, rebuild both workspace crates.
set -eu

case "${1:-}" in
  --help|-h) echo "Usage: ci.sh [--cold] (inside the CI container)"; exit 0 ;;
  ''|--cold) ;;
  *) echo "Unknown CI option: $1" >&2; exit 2 ;;
esac
if [ "$#" -gt 1 ]; then
  echo "Usage: ci.sh [--cold]" >&2
  exit 2
fi

REPOSITORY_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$REPOSITORY_ROOT"
CACHE_ROOT=${SKY_BACKCRAFT_CI_CACHE_DIR:-/ci-cache}
mkdir -p "$CACHE_ROOT"

# The lock FD survives exec and remains held by the actual build pipeline.
# Cold outputs live only in the disposable container, outside the warm cache.
exec flock --no-fork "$CACHE_ROOT/run.lock" sh -ec '
  if [ "$2" = "--cold" ]; then
    CARGO_TARGET_DIR=$(mktemp -d /tmp/sky-backcraft-ci-cold.XXXXXX)
  else
    CARGO_TARGET_DIR="$1/target"
  fi
  export CARGO_TARGET_DIR
  cargo clean --locked --package spot-lab --package xtask
  exec make check
' ci "$CACHE_ROOT" "${1:-}"
