# AnimusDB container image(s).
#
# Multi-stage: one `rust:1.96` builder (matching the toolchain pinned in
# rust-toolchain.toml / the workspace `rust-version`) release-builds every
# runnable binary — `animusd`, `animus` (the CLI), and `animus-operator` —
# then two separate `debian:bookworm-slim` runtime stages each carry only
# the binaries their image needs plus the glibc/libssl userland they link
# against. Distroless was considered and rejected here: bookworm-slim keeps
# a shell + package manager available for interactive debugging inside a
# running container, which matters more for an early, pre-alpha, ops-heavy
# database than the extra few MB distroless would save.
#
# `docker build .` (no `--target`) produces the `runtime` stage — the
# animusd node image, unchanged default. `docker build --target
# runtime-operator .` produces the `animus-operator` controller image (see
# `.github/workflows/image.yml`'s `build` job matrix and
# `deploy/operator/deployment.yaml`).
#
# No ENTRYPOINT flags are baked into the `animusd` image beyond the binary
# itself — every deployment mode (`--cluster`, `--config --node`, `join`,
# `control`, `data`, ...; see crates/animusd/CLAUDE.md) is selected by
# whoever runs the image, via `docker run ... animusd <args>`. In
# particular `--dir` (the on-disk data path) is never defaulted here: it
# must be passed explicitly and should point at the mounted /var/lib/animus
# volume.

# syntax=docker/dockerfile:1

# Overridable only for local/sandboxed verification where Docker Hub's blob
# CDN is unreachable (e.g. `--build-arg BASE_REGISTRY=mirror.gcr.io/library`).
# The committed default is plain Docker Hub, which is what CI and every real
# build use.
ARG BASE_REGISTRY=docker.io/library

FROM ${BASE_REGISTRY}/rust:1.96 AS builder
WORKDIR /build

# Cargo.lock is committed (S-07a, 2026-09-02 — see docs/adr/0060-kubernetes-
# operator.md's Part 2 amendment) so every image build resolves the exact
# dependency set CI tested, not whatever `cargo build` happens to pick up
# fresh from the registry that day. `--locked` below makes that authoritative:
# the build fails loudly instead of silently re-resolving if the manifest and
# lock ever drift apart.
COPY Cargo.toml Cargo.lock ./
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
# Built once, all three binaries in one pass — a single cache-mounted
# compile that both runtime stages below draw `COPY --from=builder` from,
# rather than a separate build per image.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    --mount=type=secret,id=ccrca,target=/tmp/ccrca.crt \
    sh -c '\
      if [ -s /tmp/ccrca.crt ]; then \
        export SSL_CERT_FILE=/tmp/ccrca.crt CARGO_HTTP_CAINFO=/tmp/ccrca.crt; \
      fi; \
      cargo build --release --locked -p animusd -p animus-cli -p animus-operator \
    ' \
    && cp target/release/animusd /build/animusd \
    && cp target/release/animus /build/animus \
    && cp target/release/animus-operator /build/animus-operator

# --- animus-operator image (docker build --target runtime-operator .) ---
#
# Placed BEFORE the default `runtime` stage below deliberately: with no
# `--target`, `docker build .` builds whichever stage is declared LAST in
# the file, and that default has to stay `animusd` (every existing caller —
# `scripts/e2e-kind.sh`, this file's own header comment, a developer running
# a bare `docker build .`) — so `runtime-operator` cannot be the final
# `FROM` even though it is the newer of the two.
#
# The `animus-operator` controller (crates/animus-operator, ADR 0060 Part
# 3): a `kube-rs` controller that runs as a single `Deployment` replica
# in-cluster (deploy/operator/deployment.yaml), never as a StatefulSet
# workload with a data volume of its own — it only talks to the Kubernetes
# API server and, for scale-down drains, to an `animusd` pod's admin port.
FROM ${BASE_REGISTRY}/debian:bookworm-slim AS runtime-operator

LABEL org.opencontainers.image.source="https://github.com/animus-db/animus-db" \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      org.opencontainers.image.title="animus-operator" \
      org.opencontainers.image.description="AnimusDB Kubernetes operator (AnimusCluster controller)"

# ca-certificates is required here (unlike the animusd runtime stage below):
# `animus-operator` talks to the Kubernetes API server over TLS via `kube`'s
# rustls-platform-verifier stack, which needs a real trust store to verify
# that connection.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 1000 animus-operator \
    && useradd --system --uid 1000 --gid animus-operator --shell /usr/sbin/nologin animus-operator

COPY --from=builder /build/animus-operator /usr/local/bin/animus-operator

USER animus-operator:animus-operator

ENTRYPOINT ["animus-operator"]
CMD ["run"]

# --- animusd image (default target — docker build . with no --target) ---
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
