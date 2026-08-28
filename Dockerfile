# AnimusDB container image.
#
# Multi-stage: a `rust:1.96` builder (matching the toolchain pinned in
# rust-toolchain.toml / the workspace `rust-version`) release-builds the two
# runnable binaries, then a `debian:bookworm-slim` runtime stage carries only
# those binaries plus the glibc/libssl userland they link against. Distroless
# was considered and rejected here: bookworm-slim keeps a shell + package
# manager available for interactive debugging inside a running container,
# which matters more for an early, pre-alpha, ops-heavy database than the
# extra few MB distroless would save.
#
# No ENTRYPOINT flags are baked in beyond the binary itself — every
# deployment mode (`--cluster`, `--config --node`, `join`, `control`,
# `data`, ...; see crates/animusd/CLAUDE.md) is selected by whoever runs the
# image, via `docker run ... animusd <args>`. In particular `--dir` (the
# on-disk data path) is never defaulted here: it must be passed explicitly
# and should point at the mounted /var/lib/animus volume.

# syntax=docker/dockerfile:1

# Overridable only for local/sandboxed verification where Docker Hub's blob
# CDN is unreachable (e.g. `--build-arg BASE_REGISTRY=mirror.gcr.io/library`).
# The committed default is plain Docker Hub, which is what CI and every real
# build use.
ARG BASE_REGISTRY=docker.io/library

FROM ${BASE_REGISTRY}/rust:1.96 AS builder
WORKDIR /build

# Cargo.lock is deliberately gitignored in this workspace (see repo
# .gitignore), so only the manifest is copied ahead of the source — `cargo
# build` mints its own lock file from the registry cache mount below.
COPY Cargo.toml ./
COPY crates ./crates

# BuildKit cache mounts persist the cargo registry/git checkouts and the
# incremental target/ directory across builds without baking them into any
# image layer (unlike a plain COPY of target/, which would defeat layer
# caching entirely since the whole workspace rebuilds on every source
# change anyway). `cargo-chef` was the other documented option here but
# adds a second tool + an extra recipe/cook pass for the same effect a
# cache mount gets in one step.
#
# The optional `ccrca` secret below is for building behind a TLS-
# intercepting egress proxy (some sandboxed CI/dev hosts) where crates.io
# re-terminates with a private CA the base image doesn't trust — e.g.
# `--secret id=ccrca,src=/path/to/ca-bundle.crt`. Unset (the normal case:
# real CI, a developer's own machine) it mounts nothing, the `-f` test
# short-circuits, and cargo uses the image's normal trust store; the cert
# is never baked into a layer either way.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    --mount=type=secret,id=ccrca,target=/tmp/ccrca.crt \
    sh -c '\
      if [ -s /tmp/ccrca.crt ]; then \
        export SSL_CERT_FILE=/tmp/ccrca.crt CARGO_HTTP_CAINFO=/tmp/ccrca.crt; \
      fi; \
      cargo build --release -p animusd -p animus-cli \
    ' \
    && cp target/release/animusd /build/animusd \
    && cp target/release/animus /build/animus

FROM ${BASE_REGISTRY}/debian:bookworm-slim AS runtime

LABEL org.opencontainers.image.source="https://github.com/animus-db/animus-db" \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      org.opencontainers.image.title="animusd" \
      org.opencontainers.image.description="AnimusDB node server and operator CLI"

# No extra runtime packages: the binaries are dynamically linked against
# glibc only, which bookworm-slim already ships. (An outbound-TLS backup
# target such as an S3 `SegmentStore`, ADR 0059, would need ca-certificates
# added here — not yet required by anything v1 ships.)
RUN groupadd --system --gid 1000 animus \
    && useradd --system --uid 1000 --gid animus --home-dir /var/lib/animus --shell /usr/sbin/nologin animus \
    && mkdir -p /var/lib/animus \
    && chown -R animus:animus /var/lib/animus

COPY --from=builder /build/animusd /usr/local/bin/animusd
COPY --from=builder /build/animus /usr/local/bin/animus

USER animus:animus
WORKDIR /var/lib/animus
VOLUME ["/var/lib/animus"]

ENTRYPOINT ["animusd"]
