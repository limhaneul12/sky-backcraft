SHELL := /bin/sh

.PHONY: ci rebuild

ci:
	docker compose --profile ci build ci
	docker compose --profile ci run --rm --no-deps ci

rebuild:
	./scripts/docker-rebuild.sh

