# `Cargo.lock` is deliberately gitignored (`.gitignore`, present since the repo's initial commit, no lockfile ever tracked in history) — a container build or any other recipe that assumes a committed lockfile breaks (ADR 0060 e2e work).

**`Cargo.lock` is deliberately gitignored (`.gitignore`, present since the
repo's initial commit, no lockfile ever tracked in history) — a container
build or any other recipe that assumes a committed lockfile breaks
(ADR 0060 e2e work).** The Dockerfile's own builder stage says as much
(`COPY Cargo.toml` only, never `Cargo.lock` — "cargo build mints its own
lock file from the registry cache mount"), and this is exactly what a
real build does: `docker build`'s first line is `Locking N packages to
latest Rust <version>-compatible versions`, freshly resolved every build,
not reproduced from a checked-in file. The repo carries no separate
written rationale for the choice beyond that comment; treat it as "this
workspace resolves dependencies per-checkout, not pinned across commits"
and design any new build/CI recipe around that — never add a `COPY
Cargo.lock` step or a lockfile-presence check expecting one to exist, and
don't be surprised when two builds an hour apart pull a different patch
version of some transitive dependency.
