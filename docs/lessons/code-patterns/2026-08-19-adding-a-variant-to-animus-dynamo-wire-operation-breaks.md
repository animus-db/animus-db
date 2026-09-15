# Adding a variant to `animus-dynamo::wire::Operation` breaks `animusd`'s exhaustive `match op { .. }` dispatch by construction — that's a downstream crate's job to fix, not a sign your pure-crate slice failed (2026-08-19).

**Adding a variant to `animus-dynamo::wire::Operation` breaks `animusd`'s
exhaustive `match op { .. }` dispatch by construction — that's a downstream
crate's job to fix, not a sign your pure-crate slice failed (2026-08-19).**
Building ADR 0051 TTL's `Operation::UpdateTimeToLive`/
`DescribeTimeToLive` in `animus-dynamo` (a crate deliberately kept
dependency-free of `animusd`) left `cargo build --workspace --all-targets`
failing on `crates/animusd/src/dynamo.rs`'s non-exhaustive `match` with a
clear `E0004` naming exactly the two new variants. This is the expected,
narrow shape of the split: a pure-crate agent adds the wire vocabulary,
a separate `animusd`-owning agent wires it up; a compile error whose
*only* content is "new match arm needed" in a crate you were told not to
touch is a handoff marker, not a regression to chase — confirm the error
names only your new variants (nothing pre-existing broke) and report it
rather than reaching into the other crate. The reusable check: `git status`
before troubleshooting a cross-crate build break in a parallel-agent tree
— if the crate the error is in shows as untouched while sibling crates
show real diffs, the owning agent for that crate simply hasn't landed the
consuming change yet.
