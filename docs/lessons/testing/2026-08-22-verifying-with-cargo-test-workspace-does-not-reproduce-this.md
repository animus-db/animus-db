# Verifying with `cargo test --workspace` does not reproduce this repo's CI, and manufactures failures CI would never see

**Verifying with `cargo test --workspace` does not reproduce this repo's
CI, and manufactures failures CI would never see** (2026-08-22, the
`ScanIndexForward` rung). CI deliberately splits animusd out:
`cargo test --workspace --exclude animusd -- --test-threads=2`, then
`cargo test -p animusd --lib --tests -- --test-threads=1`, because that
crate's ~66 real-thread multi-node integration tests starve each other at
default parallelism. A local `cargo test --workspace` runs exactly the
configuration CI was restructured to avoid — every heavyweight cluster
test concurrently — so a failure there is not yet evidence about the code.
It cost a full investigation of `schema_ddl_on_a_follower_is_relayed_to_the_leader`
that passed both in isolation and serially. The investigation was still
right to run (the diff touched forwarded request enums, and a missed
relay match site is exactly the bimodal per-process flake this log warns
about) — but the *first* step should be re-running the way `ci.yml` does,
before reading anything into the parallel sweep. Corollary: when a local
gate disagrees with CI, check how CI actually invokes it before debugging
the code; the invocation is part of the gate.
