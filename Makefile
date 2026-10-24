SHELL := /bin/sh
HELPER ?= $(if $(CARGO_TARGET_DIR),$(CARGO_TARGET_DIR),target)/debug/spot-lab

.PHONY: ci ci-cold check scripts-check rebuild package verify-install native-check verify-helper

ci ci-cold:
	@set -eu; image_id_file=$$(mktemp "$${TMPDIR:-/tmp}/sky-backcraft-ci-image.XXXXXX"); \
	trap 'rm -f "$$image_id_file"' EXIT; \
	trap 'exit 129' HUP; trap 'exit 130' INT; trap 'exit 143' TERM; \
	revision=$$(git rev-parse HEAD); \
	if ! printf '%s\n' "$$revision" | grep -Eq '^[0-9a-f]{40}$$'; then \
	  printf 'unable to resolve a full lowercase Git revision for this build\n' >&2; exit 2; \
	fi; \
	git_state=$$(git status --porcelain --untracked-files=normal); \
	if [ -n "$$git_state" ]; then revision="$$revision-dirty"; fi; \
	docker build --target ci --iidfile "$$image_id_file" \
	  --build-arg "SKY_BACKCRAFT_BUILD_REVISION=$$revision" \
	  --build-arg "SKY_BACKCRAFT_VERIFICATION_RECEIPT=" .; \
	image_id=$$(cat "$$image_id_file"); \
	if ! printf '%s\n' "$$image_id" | grep -Eq '^sha256:[0-9a-f]{64}$$'; then \
	  printf 'unable to resolve the exact CI image ID\n' >&2; exit 2; \
	fi; \
	if [ "$@" = "ci-cold" ]; then \
	  SPOT_LAB_CI_IMAGE="$$image_id" docker compose --profile ci run --rm --no-deps --pull never ci sh scripts/shell/ci.sh --cold; \
	else \
	  SPOT_LAB_CI_IMAGE="$$image_id" docker compose --profile ci run --rm --no-deps --pull never ci; \
	fi

check:
	cargo xtask ci
	$(MAKE) scripts-check
	cargo build --locked --bin spot-lab
	$(MAKE) verify-helper

scripts-check:
	python3 -m compileall -q scripts/python
	@set -eu; for script in scripts/shell/*.sh; do sh -n "$$script"; done
	python3 -B -m unittest discover -s scripts/python/tests -p 'test_*.py'

rebuild:
	./scripts/shell/docker-rebuild.sh

package:
	./scripts/shell/package.sh

verify-install:
	python3 scripts/python/dist_outputs.py "$(CURDIR)/dist" --verify-installed "$$HOME/Applications/Sky Backcraft.app" --launch-app "/Applications/Sky Backcraft.app"

native-check:
	./scripts/shell/test-native-lifecycle.sh
	./scripts/shell/test-native-runtime-integration.sh
	./scripts/shell/test-native-realtime.sh
	./scripts/shell/test-native-automation.sh
	./scripts/shell/test-native-read-projections.sh
	./scripts/shell/test-native-decision-analysis.sh

# Verify a built helper without installing or restarting the operating app.
verify-helper:
	python3 scripts/python/verify_helper.py --binary "$(HELPER)"
