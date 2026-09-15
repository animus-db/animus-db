# In a real `ProdEnv` test, "the tablet map shows the tablet" and "this node has actually started hosting its `CpGroup`" are two different, separately-converging facts — polling only the first before reaching for `ClusterEdgeState::local_cp` is a real (if usually narrow) race, not paranoia.

**In a real `ProdEnv` test, "the tablet map shows the tablet" and "this
node has actually started hosting its `CpGroup`" are two different,
separately-converging facts — polling only the first before reaching for
`ClusterEdgeState::local_cp` is a real (if usually narrow) race, not
paranoia.** A split or merge test that fetches a fresh child's/survivor's
`CpGroup` handle immediately after `Metadata` shows the new tablet count
can hit `local_cp` returning `None` — the per-node tablet-host reconciler
(ADR 0031) still needs its own tick to stand the group up locally.
Poll-for-`Some` (`local_cp(tablet).is_some()`) before ever unwrapping it,
the same way every other eventually-true fact in these tests is awaited,
rather than chaining an `.expect(..)` straight off a `Metadata` poll.
Separately: a merge survivor's *widened* `StorageScope` — needed before an
absorbed sibling's own physically-still-present rows (e.g. its own cursor
row, ADR 0042 §7) become visible through the survivor's scans — is
*also* a distinct, later-converging fact from "the tablet map shows one
tablet again"; assert on it with its own poll, not a single check right
after the merge's own convergence. (`crates/animusd/src/index_drain.rs`,
`gsi_drain_cursor_tests`, 2026-08-14.)
