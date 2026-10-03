SHELL := /bin/sh

.PHONY: ci rebuild package

ci:
	docker compose --profile ci build ci
	docker compose --profile ci run --rm --no-deps ci

rebuild:
	./scripts/docker-rebuild.sh

package:
	cargo build --release --bin spot-lab --bin sky-backcraft-setup
	./scripts/package.sh

