# When a CLI's identity scheme changes from "operator-picked index" to "explicit-or-self-minted string id," a test-support helper that used to take the index can usually keep its own signature unchanged — just have it derive the new explicit id *from* the index it already takes (`config::node_id(index)`) at the one internal call site, rather than propagating the new `Option<NodeId>` parameter out through every test file that calls it.

**When a CLI's identity scheme changes from "operator-picked index" to
"explicit-or-self-minted string id," a test-support helper that used to
take the index can usually keep its own signature unchanged — just have it
derive the new explicit id *from* the index it already takes
(`config::node_id(index)`) at the one internal call site, rather than
propagating the new `Option<NodeId>` parameter out through every test file
that calls it.** ADR 0040 PR4 replaced `run_node_join(seeds, index: usize,
..)` with `run_node_join(seeds, id: Option<NodeId>, .., labels)` — a
breaking signature change — but `animusd/tests/support/mod.rs`'s
`join_fresh_deadline(seeds, index: usize, ..)` (called from
`seed_join.rs`/`decommission.rs`/`cluster_growth.rs`) kept its own
`index: usize` parameter and just changed its one-line internal call to
`run_node_join(seeds, Some(config::node_id(index)), ..)`. Every caller
outside `support/mod.rs` — which mostly just wants "a distinct, readable,
deterministic test id for slot N," not "test the new CLI surface itself" —
compiled and passed completely unchanged; only the two direct,
bypass-the-helper call sites in `seed_join.rs` (a rejoin helper and an
explicit collision-guard test) needed updating to pass the id directly.
**General rule: when a lower-layer API's identity parameter type changes,
look for the test-support layer that already had a stable, semantically-
named wrapper around the old parameter before touching every call site —
the wrapper is usually the one and only place that needs to translate the
old convention into the new one, and preserving its signature is what
keeps a large, otherwise-unrelated test suite's diff to nearly nothing.**
(ADR 0040 PR4.)
