# A runtime `assert!` on a type-level trait constant is a reachability claim the compiler will not check for you — grep the impls (2026-08-19).

**A runtime `assert!` on a type-level trait constant is a reachability claim
the compiler will not check for you — grep the impls (2026-08-19).**
`animus-control`'s `RaftCore::encoded_wal_image` /
`PersistedState::encode_snapshot_record_from_blob` existed to serialize
`Metadata` once per compaction instead of twice, guarded by
`assert!(!S::DRIVER_APPLIED, ..)`. ADR 0038 then made both real state
machines in the workspace (`Metadata`, `KvState`) `DRIVER_APPLIED = true`,
which quietly made the pair unreachable — yet nothing flagged it: the
functions still compiled, were still `pub`, and still had a passing unit test
that constructed its own toy implementor with the "wrong" constant. Two
cheap detectors: `grep -rn "DRIVER_APPLIED"` across every `impl` (including
test files) settles reachability faster than tracing call sites forward, and
when a doc comment cites a specific guard test as evidence a mechanism is
exercised, **check the test exists** — this one cited
`wal_compaction.rs::encoded_image_matches_wal_image_encoding`, which had
never existed anywhere in the repo. A cited-but-phantom test is worse than no
citation: it buys a reader's trust for free.
