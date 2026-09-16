# A "bytes" accessor's own scope is part of its contract, not an implementation detail — check which `StorageScope`/row-kind it measures before reusing it for a new trigger.

**A "bytes" accessor's own scope is part of its contract, not an
implementation detail — check which `StorageScope`/row-kind it measures
before reusing it for a new trigger.** `RaftKvNode::approx_bytes` was
deliberately narrowed to the **base** kind scope by ADR 0034's own fix
(so auto-split stops reacting to change-log churn) — a fact stated
plainly in that method's doc and this crate's own `CLAUDE.md`, and easy
to miss when reaching for "the byte estimate" to build a *different*
trigger. The Streams sealer's size trigger needs `KIND_CHANGE`'s own
bytes specifically (ADR 0043 §A3's "When": "`KIND_CHANGE` scope
`approx_bytes`") — calling the existing `approx_bytes()` compiled, ran,
and even passed several tests (small test tables happen to write base
rows and change records of comparable size, so the wrong scope's number
still crossed the same threshold at roughly the same time), until an
end-to-end auto-split test on a *streamed* table exposed the mismatch
indirectly. Fixed by adding a kind-scoped sibling
(`RaftKvNode::approx_bytes_kind(kind)`/`CpGroup::approx_bytes_kind`) that
takes the row-kind's own `StorageScope` instead of assuming the base one
— never widen an existing narrowly-scoped accessor back out, add a
sibling with the same shape over a different scope. General rule: before
wiring an existing "cheap estimate" accessor into a new caller, re-read
its own doc for *which* scope/kind/range it was deliberately narrowed to
and *why* — a byte/count estimator that looks generic by name can be
pinned to one specific scope for a reason that has nothing to do with
your new use case. (`crates/animus-cp-data/src/lib.rs`,
`crates/animusd/src/index_drain.rs`, ADR 0042/0043 round-3 PR5,
2026-08-14.)
