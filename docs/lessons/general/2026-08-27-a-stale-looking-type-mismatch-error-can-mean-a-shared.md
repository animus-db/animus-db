# A stale-looking type-mismatch error can mean a shared `CARGO_TARGET_DIR` served an rlib from before this session's own edit (ADR 0060 advertise/dial split)

Mid-refactor, `cargo build -p animusd` reported `expected BTreeMap<NodeId,
String>, found BTreeMap<NodeId, SocketAddr>` at an `env.set_peers(...)` call
site whose surrounding code — both the caller's `.collect()` and
`ProdEnv::set_peers`'s own signature — had already been edited to agree on
`String`, moments earlier in the same session, with `animus-env` untouched
since. The error made no sense read literally. Cause: this environment's
mandatory `CARGO_TARGET_DIR=/home/user/shared-cargo-target` is a directory
shared across concurrent agent sessions/worktrees, and the build was
linking against an `animus-env` rlib compiled from a *different* worktree's
copy of `prod.rs` (pre-dating this session's own `set_peers` signature
change) rather than picking up the edit just made in *this* worktree — a
`touch crates/animus-env/src/prod.rs && cargo build -p animus-env` forced a
fresh compile from this worktree's own source, and the identical `animusd`
build then succeeded with no further code changes. **The tell**: a type
error whose two sides both look correct in the code you can see, especially
one naming a crate two or more levels below the one you're editing and that
you haven't touched THIS session. Before hunting for a bug in code that
reads right, force a rebuild of the suspect lower-level crate on its own
(`touch` the file, or `cargo build -p <crate>`) and re-check — cheaper than
a wrong-theory debugging detour, and the fix leaves no trace once the real
build lands.
