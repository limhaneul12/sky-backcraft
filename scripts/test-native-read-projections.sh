#!/bin/sh
set -eu

REPOSITORY_ROOT=$(CDPATH= cd -- "$(rtk proxy dirname -- "$0")/.." && rtk proxy pwd)
TEST_DIRECTORY=$(rtk proxy mktemp -d "${TMPDIR:-/tmp}/sky-backcraft-native-read-projections.XXXXXX")
trap 'rtk proxy rm -rf "$TEST_DIRECTORY"' EXIT HUP INT TERM
OUTPUT="$TEST_DIRECTORY/native-read-projection-regression"

rtk xcrun swiftc -parse-as-library -swift-version 5 -warnings-as-errors \
  -target "$(rtk uname -m)-apple-macos12.0" -lsqlite3 \
  "$REPOSITORY_ROOT/macos/DatabaseReader.swift" \
  "$REPOSITORY_ROOT/macos/ReadTimestampParser.swift" \
  "$REPOSITORY_ROOT/macos/LedgerDetailMCPReader.swift" \
  "$REPOSITORY_ROOT/macos/PolicyDetailMCPReader.swift" \
  "$REPOSITORY_ROOT/macos/tests/NativeReadProjectionRegression.swift" \
  -o "$OUTPUT"

rtk proxy "$OUTPUT"
for ROW_COUNT in 1000 25000; do
  SUPPORT_DIRECTORY="$TEST_DIRECTORY/marks-$ROW_COUNT"
  rtk proxy "$OUTPUT" create-marks-db "$SUPPORT_DIRECTORY" "$ROW_COUNT"
  rtk proxy "$OUTPUT" measure-marks "$SUPPORT_DIRECTORY" 1000
done
rtk proxy "$OUTPUT" measure-ledger 100
rtk proxy "$OUTPUT" measure-policy 30
