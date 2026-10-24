#!/bin/sh
set -eu

REPOSITORY_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
TEST_DIRECTORY=$(mktemp -d "${TMPDIR:-/tmp}/sky-backcraft-native-realtime.XXXXXX")
trap 'rm -rf "$TEST_DIRECTORY"' EXIT HUP INT TERM
OUTPUT="$TEST_DIRECTORY/realtime-regression"

xcrun swiftc -parse-as-library -swift-version 5 -warnings-as-errors \
  -target "$(uname -m)-apple-macos12.0" \
  "$REPOSITORY_ROOT/macos/NativeMCPClient.swift" \
  "$REPOSITORY_ROOT/macos/ReadTimestampParser.swift" \
  "$REPOSITORY_ROOT/macos/RealtimeFeedMCPService.swift" \
  "$REPOSITORY_ROOT/macos/tests/RealtimeFeedMCPServiceRegression.swift" \
  -o "$OUTPUT"
"$OUTPUT"

# Compile the disposable appearance fixture without launching a user window.
xcrun swiftc -parse-as-library -swift-version 5 -warnings-as-errors \
  -target "$(uname -m)-apple-macos12.0" -framework AppKit \
  "$REPOSITORY_ROOT/macos/NativeMCPClient.swift" \
  "$REPOSITORY_ROOT/macos/ReadTimestampParser.swift" \
  "$REPOSITORY_ROOT/macos/RealtimeFeedMCPService.swift" \
  "$REPOSITORY_ROOT/macos/RealtimeConnectionStatusView.swift" \
  "$REPOSITORY_ROOT/macos/TerminalTheme.swift" \
  "$REPOSITORY_ROOT/macos/tests/RealtimeStatusGUIFixture.swift" \
  -o "$TEST_DIRECTORY/realtime-gui-fixture"
echo "PASS native realtime appearance fixture compile (visual review separate)"
