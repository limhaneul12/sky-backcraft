#!/bin/sh
set -eu

REPOSITORY_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
TEST_DIRECTORY=$(mktemp -d "${TMPDIR:-/tmp}/sky-backcraft-native-decision-analysis.XXXXXX")
trap 'rm -rf "$TEST_DIRECTORY"' EXIT HUP INT TERM
OUTPUT="$TEST_DIRECTORY/decision-analysis-regression"
TARGET="$(uname -m)-apple-macos12.0"
if [ "${SKY_BACKCRAFT_DECISION_ANALYSIS_WIRE_DIRECTORY+x}" = x ]; then
  WIRE_DIRECTORY="$SKY_BACKCRAFT_DECISION_ANALYSIS_WIRE_DIRECTORY"
  if [ -z "$WIRE_DIRECTORY" ] || [ ! -d "$WIRE_DIRECTORY" ]; then
    echo "Explicit decision-analysis wire directory does not exist: $WIRE_DIRECTORY" >&2
    exit 2
  fi
else
  WIRE_DIRECTORY="$TEST_DIRECTORY/wire"
  mkdir -p "$WIRE_DIRECTORY"

  run_wire_test() {
    TEST_NAME=$1
    TEST_LOG="$TEST_DIRECTORY/$(printf '%s' "$TEST_NAME" | tr '/:' '__').log"
    if (
      cd "$REPOSITORY_ROOT"
      SKY_BACKCRAFT_DECISION_ANALYSIS_WIRE_DIR="$WIRE_DIRECTORY" \
        cargo test --color never --locked --manifest-path "$REPOSITORY_ROOT/Cargo.toml" \
          -p spot-lab --lib "$TEST_NAME" -- --exact
    ) >"$TEST_LOG" 2>&1; then
      cat "$TEST_LOG"
    else
      cat "$TEST_LOG" >&2
      exit 1
    fi
    if ! rg -q "^test $TEST_NAME \.\.\. ok$" "$TEST_LOG"; then
      echo "Expected exactly the targeted Rust fixture test to pass: $TEST_NAME" >&2
      exit 1
    fi
  }

  run_wire_test "mcp::tests::saved_decision_analysis_mcp_workflow_preserves_both_source_families"
  run_wire_test "reporting::decision_analysis::tests::captured_condition_wire_corpus_preserves_enum_evidence_and_decimal_text"
  run_wire_test "reporting::decision_analysis::tests::missing_times_capture_states_and_numeric_tie_identity_stay_distinct"
fi

for WIRE_FILE in \
  automation-all-markets-compare.json automation-no-previous-compare.json \
  automation-preperiod-all-markets-compare.json \
  automation-buy-decisions.json automation-compare.json automation-decision.json \
  automation-decisions.json automation-overview.json automation-selected-compare.json \
  paper-all-markets-compare.json paper-no-previous-compare.json \
  paper-buy-decisions.json paper-captured-compare.json paper-captured-decision.json \
  paper-captured-compare-missing-time.json paper-captured-compare-no-previous.json \
  paper-captured-decisions.json paper-captured-overview.json paper-compare.json \
  paper-decision.json paper-decisions.json paper-overview.json paper-selected-compare.json; do
  if [ ! -s "$WIRE_DIRECTORY/$WIRE_FILE" ]; then
    echo "Required decision-analysis Rust wire fixture is missing or empty: $WIRE_DIRECTORY/$WIRE_FILE" >&2
    exit 1
  fi
done
SKY_BACKCRAFT_DECISION_ANALYSIS_WIRE_DIRECTORY="$WIRE_DIRECTORY"
export SKY_BACKCRAFT_DECISION_ANALYSIS_WIRE_DIRECTORY
if [ -n "${SKY_BACKCRAFT_DECISION_ANALYSIS_OUTPUT:-}" ]; then
  OUTPUT="$SKY_BACKCRAFT_DECISION_ANALYSIS_OUTPUT"
  if [ -e "$OUTPUT" ]; then
    echo "Refusing to overwrite existing decision-analysis fixture output: $OUTPUT" >&2
    exit 2
  fi
  OUTPUT_DIRECTORY=$(dirname -- "$OUTPUT")
  mkdir -p "$OUTPUT_DIRECTORY"
fi

rg --null --files "$REPOSITORY_ROOT/macos" -g '*.swift' -g '!main.swift' -g '!**/tests/**' \
  | xargs -0 xcrun swiftc -parse-as-library -swift-version 5 -warnings-as-errors \
      -target "$TARGET" -framework AppKit -framework Security -lsqlite3 \
      "$REPOSITORY_ROOT/macos/tests/DecisionAnalysisRegression.swift" \
      "$REPOSITORY_ROOT/macos/tests/DecisionAnalysisGUIFixture.swift" \
      -o "$OUTPUT"
echo "PASS native decision-analysis regression and appearance fixture compile"

"$OUTPUT" "$@"
