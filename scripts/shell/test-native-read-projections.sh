#!/bin/sh
set -eu

REPOSITORY_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
TEST_DIRECTORY=$(mktemp -d "${TMPDIR:-/tmp}/sky-backcraft-native-read-projections.XXXXXX")
trap 'rm -rf "$TEST_DIRECTORY"' EXIT HUP INT TERM
OUTPUT="$TEST_DIRECTORY/native-read-projection-regression"

xcrun swiftc -parse-as-library -swift-version 5 -warnings-as-errors \
  -target "$(uname -m)-apple-macos12.0" -lsqlite3 \
  "$REPOSITORY_ROOT/macos/DatabaseReader.swift" \
  "$REPOSITORY_ROOT/macos/ReadTimestampParser.swift" \
  "$REPOSITORY_ROOT/macos/LedgerDetailMCPReader.swift" \
  "$REPOSITORY_ROOT/macos/PolicyDetailMCPReader.swift" \
  "$REPOSITORY_ROOT/macos/tests/NativeReadProjectionRegression.swift" \
  -o "$OUTPUT"

"$OUTPUT"
for ROW_COUNT in 1000 25000; do
  SUPPORT_DIRECTORY="$TEST_DIRECTORY/marks-$ROW_COUNT"
  "$OUTPUT" create-marks-db "$SUPPORT_DIRECTORY" "$ROW_COUNT"
  "$OUTPUT" measure-marks "$SUPPORT_DIRECTORY" 1000
done
"$OUTPUT" measure-ledger 100
"$OUTPUT" measure-policy 30
