# A module doc's "this never happens under this fixture" claim needs its own expiry check whenever the fixture it describes gains a capability — not just when a scenario finally exercises it

**A module doc's "this never happens under this fixture" claim needs its
own expiry check whenever the fixture it describes gains a capability —
not just when a scenario finally exercises it** (2026-09-09, ADR 0061
rung K, C-11 PR 2). `sim_cluster_throttle.rs`'s own module doc asserted,
from ADR 0061 rung D2 PR 1 onward, that `ThrottledWrites`/`ThrottledReads`
"never increment under `SimCluster`" because every node's `data` field
was `None`. That was true when written — but D2 PR 1, landing in the
*same* rung, gave every `SimCluster` node a real `DataRole` (`data:
Some(DataRole { raftkv_metrics: node_metrics[i].clone(), .. })`) so the
metric-recording sites' `self.data.as_ref()` gate resolved to `Some`
from that point on. The claim silently went stale the moment the
capability landed, three rungs before any scenario tried to prove it —
and a second file (`sim_cluster_dynamo_update_table.rs`, D3 PR 2b) later
cited the same stale claim as its own justification for staying
`ProdEnv`-only, propagating the error instead of catching it. The
general fix: when a PR removes a fixture's limitation (a `None` becomes
`Some`, a stubbed loop becomes real, a `pub(crate)` surface widens),
grep every sibling module's doc comments for the specific claim the
limitation justified — not just update the one file whose own scenario
now depends on the fix — before treating the rung as closed. A "why this
stays `ProdEnv`-only" doc comment is a claim with a truth condition, not
decoration; it needs the same staleness suspicion as a comment describing
code, per this log's existing "verify a documented gap by grepping the
code" convention (root `CLAUDE.md`'s own "Before implementing a
'close this documented gap' task, grep the code" rule) — the difference
here is the gap being described is in a *test fixture's own capability*,
not the product code the fixture exercises.
