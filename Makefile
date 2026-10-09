SHELL := /bin/sh

.PHONY: ci rebuild package native-check

ci:
	@revision=$$(git rev-parse HEAD); \
	if ! printf '%s\n' "$$revision" | grep -Eq '^[0-9a-f]{40}$$'; then \
	  printf 'unable to resolve a full lowercase Git revision for this build\n' >&2; exit 2; \
	fi; \
	git_state=$$(git status --porcelain --untracked-files=normal) || exit $$?; \
	if [ -n "$$git_state" ]; then revision="$$revision-dirty"; fi; \
	SKY_BACKCRAFT_BUILD_REVISION="$$revision" docker compose --profile ci build \
	  --build-arg "SKY_BACKCRAFT_BUILD_REVISION=$$revision" \
	  --build-arg "SKY_BACKCRAFT_VERIFICATION_RECEIPT=" ci
	docker compose --profile ci run --rm --no-deps ci

rebuild:
	./scripts/docker-rebuild.sh

package:
	./scripts/package.sh

native-check:
	./scripts/test-native-lifecycle.sh
	./scripts/test-native-runtime-integration.sh
