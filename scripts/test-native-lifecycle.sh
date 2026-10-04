#!/bin/sh
set -eu

REPOSITORY_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TEST_DIRECTORY=$(mktemp -d "${TMPDIR:-/tmp}/sky-backcraft-native-lifecycle.XXXXXX")
trap 'rm -rf "$TEST_DIRECTORY"' EXIT HUP INT TERM
OUTPUT="$TEST_DIRECTORY/sky-backcraft-native-lifecycle-test"

xcrun swiftc -parse-as-library -swift-version 5 -warnings-as-errors \
  -target "$(uname -m)-apple-macos12.0" -framework Security \
  "$REPOSITORY_ROOT/macos/Config.swift" \
  "$REPOSITORY_ROOT/macos/RuntimeLifecycle.swift" \
  "$REPOSITORY_ROOT/macos/tests/LifecycleRegression.swift" \
  -o "$OUTPUT"
"$OUTPUT"
