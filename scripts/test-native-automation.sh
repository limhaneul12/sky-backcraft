#!/bin/sh
set -eu

REPOSITORY_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TEST_DIRECTORY=$(mktemp -d "${TMPDIR:-/tmp}/sky-backcraft-native-automation.XXXXXX")
trap 'rm -rf "$TEST_DIRECTORY"' EXIT HUP INT TERM
TARGET="$(uname -m)-apple-macos12.0"

PROTOCOL_OUTPUT="$TEST_DIRECTORY/automation-protocol-regression"
xcrun swiftc -parse-as-library -swift-version 5 -warnings-as-errors \
  -target "$TARGET" \
  "$REPOSITORY_ROOT/macos/NativeMCPClient.swift" \
  "$REPOSITORY_ROOT/macos/PaperSessionMCPService.swift" \
  "$REPOSITORY_ROOT/macos/AutomationSessionMCPService.swift" \
  "$REPOSITORY_ROOT/macos/tests/AutomationProtocolRegression.swift" \
  -o "$PROTOCOL_OUTPUT"
SKY_BACKCRAFT_AUTOMATION_WIRE_FIXTURE="$REPOSITORY_ROOT/macos/tests/fixtures/AutomationWire.json" \
SKY_BACKCRAFT_PAPER_EVENT_FIXTURE="$REPOSITORY_ROOT/macos/tests/fixtures/PaperSessionEventPageActualMock.json" \
  "$PROTOCOL_OUTPUT"

POLICY_DELETE_OUTPUT="$TEST_DIRECTORY/policy-delete-protocol-regression"
rg --null --files "$REPOSITORY_ROOT/macos" -g '*.swift' -g '!main.swift' -g '!**/tests/**' \
  | xargs -0 xcrun swiftc -parse-as-library -D POLICY_DELETION_REGRESSION -swift-version 5 -warnings-as-errors \
      -target "$TARGET" -framework AppKit -framework Security -lsqlite3 \
      "$REPOSITORY_ROOT/macos/tests/AutomationProtocolRegression.swift" \
      -o "$POLICY_DELETE_OUTPUT"
SKY_BACKCRAFT_AUTOMATION_WIRE_FIXTURE="$REPOSITORY_ROOT/macos/tests/fixtures/AutomationWire.json" \
SKY_BACKCRAFT_PAPER_EVENT_FIXTURE="$REPOSITORY_ROOT/macos/tests/fixtures/PaperSessionEventPageActualMock.json" \
SKY_BACKCRAFT_POLICY_DELETION_WIRE_FIXTURE="$REPOSITORY_ROOT/.omx/evidence/p13/policy-delete-backend/wire-examples.json" \
SKY_BACKCRAFT_POLICY_DELETE_PREVIEW_FIXTURE="$REPOSITORY_ROOT/macos/tests/fixtures/PolicyDeletePreviewActualUnreferenced.json" \
  "$POLICY_DELETE_OUTPUT"
echo "PASS native policy deletion protocol regression"

APP_OUTPUT="$TEST_DIRECTORY/SkyBackcraft"
xcrun swiftc -parse-as-library -swift-version 5 -warnings-as-errors \
  -target "$TARGET" -framework AppKit -framework Security -lsqlite3 \
  "$REPOSITORY_ROOT"/macos/*.swift -o "$APP_OUTPUT"
echo "PASS native automation app compile"

FIXTURE_OUTPUT="$TEST_DIRECTORY/SkyBackcraftAutomationGUIFixture"
rg --null --files "$REPOSITORY_ROOT/macos" -g '*.swift' -g '!main.swift' -g '!**/tests/**' \
  | xargs -0 xcrun swiftc -parse-as-library -swift-version 5 -warnings-as-errors \
      -target "$TARGET" -framework AppKit -framework Security -lsqlite3 \
      "$REPOSITORY_ROOT/macos/tests/AutomationGUIFixture.swift" \
      -o "$FIXTURE_OUTPUT"
echo "PASS isolated automation GUI fixture compile"

LIVE_ORDER_OUTPUT="$TEST_DIRECTORY/live-order-switch-regression"
xcrun swiftc -parse-as-library -swift-version 5 -warnings-as-errors \
  -target "$TARGET" -framework Security \
  "$REPOSITORY_ROOT/macos/Config.swift" \
  "$REPOSITORY_ROOT/macos/RuntimeLifecycle.swift" \
  "$REPOSITORY_ROOT/macos/NativeMCPClient.swift" \
  "$REPOSITORY_ROOT/macos/UpbitLiveService.swift" \
  "$REPOSITORY_ROOT/macos/RuntimeController.swift" \
  "$REPOSITORY_ROOT/macos/tests/LiveOrderSwitchRegression.swift" \
  -o "$LIVE_ORDER_OUTPUT"
SKY_BACKCRAFT_LIVE_ORDER_WIRE_FIXTURE="${SKY_BACKCRAFT_LIVE_ORDER_WIRE_FIXTURE:-$REPOSITORY_ROOT/macos/tests/fixtures/LiveOrderControlActualMock.json}" \
  "$LIVE_ORDER_OUTPUT"
echo "PASS native live order control protocol regression"
