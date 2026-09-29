# syntax=docker/dockerfile:1.7
FROM rust:1.98.0-bookworm AS source

WORKDIR /workspace
COPY Cargo.toml Cargo.lock rust-toolchain.toml clippy.toml ./
COPY .cargo .cargo
COPY crates crates
COPY xtask xtask

FROM source AS ci
RUN rustup component add --toolchain 1.98.0 rustfmt clippy \
    && cargo fetch --locked
CMD ["cargo", "xtask", "ci"]

FROM source AS builder
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo build --locked --release --package spot-lab

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 spotlab \
    && useradd --uid 10001 --gid spotlab --no-create-home --shell /usr/sbin/nologin spotlab \
    && install --directory --owner spotlab --group spotlab /var/lib/spot-lab

COPY --from=builder /workspace/target/release/spot-lab /usr/local/bin/spot-lab
COPY --chmod=755 scripts/container-entrypoint.sh /usr/local/bin/spot-lab-entrypoint

USER 10001:10001
WORKDIR /var/lib/spot-lab
EXPOSE 8130
ENTRYPOINT ["/usr/local/bin/spot-lab-entrypoint"]
