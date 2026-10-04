SHELL := /bin/sh

.PHONY: ci rebuild package native-check

ci:
	docker compose --profile ci build ci
	docker compose --profile ci run --rm --no-deps ci

rebuild:
	./scripts/docker-rebuild.sh

package:
	./scripts/package.sh

native-check:
	./scripts/test-native-lifecycle.sh
	./scripts/test-native-runtime-integration.sh
