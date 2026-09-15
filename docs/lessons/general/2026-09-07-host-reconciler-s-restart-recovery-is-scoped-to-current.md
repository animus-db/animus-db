# `host::Reconciler`'s restart recovery is scoped to CURRENT `Metadata` only — a node offline across a drop leaks its own tablet engine forever (ADR 0061 rung D4 PR 3, 2026-09-07)

Building deterministic `SimCluster` coverage for dropped-table GC (ADR
0024) surfaced a real, previously-uncharacterized gap in `animus-cp-data::
host::Reconciler` — not a fixture artifact, and not fixed in that PR (out
of its own "driver plus assertions, not new mechanism" scope), but real
enough to record here rather than let a green test quietly paper over it.

**The mechanism.** `Reconciler::gather_facts` builds every `TabletFacts`
entry from exactly two sources: the tablets this reconciler is *currently*
driving (`self.hosted`, in-process state) and the tablets `Metadata`
*currently* names (`view.tablets.iter()`, both for the already-hosted
branch and the join-candidate/restart-upgrade branch). Nothing else ever
seeds a fact. `DropTableTablets` removes a table's tablet rows from
`Metadata` **synchronously** at apply (ADR 0024) — so once a drop has
committed and propagated, a dropped tablet id is simply gone from
`view.tablets`, permanently, with no tombstone or residual row to notice
later. A `Reconciler` is rebuilt from scratch (`Reconciler::new`, empty
`LocalState`) on every real process restart — nothing persists it, by
design (see `crates/animusd/CLAUDE.md`'s drop-table-GC entry: "there is no
more durable `cp-hosted` marker... a restart just re-discovers every
tablet to host from replicated `Metadata`"). Put together: a node that was
hosting a tablet, goes offline (process down, not merely network-
partitioned) for the whole window from before a table's drop commits
through after `Metadata` has converged everywhere else to "table absent,"
and only then restarts, comes back with a fresh, empty `LocalState` and a
`Metadata` that never shows the dropped tablet at all — `gather_facts`
produces **no fact whatsoever** for that tablet id, `plan()` never places
it in `next.hosted`, and `HostAction::Reclaim` (which only ever fires for
a tablet this reconciler's own `LocalState` currently claims — see
`plan`'s own Phase 3 doc in `host.rs`) can never target it. The tablet's
own private engine — real data, written before the crash — is a
permanent, silent leak. This is the SAME mechanism on real disk: the
`LsmEngine` production backend's `LsmTabletFactory::probe`/`destroy`
(`crates/animusd/src/lib.rs`) is only ever called for tablet ids
`gather_facts` already decided to ask about, so nothing there closes the
gap either.

**This regressed a real guarantee the pre-reconciler design had.**
`docs/adr/0024-drop-table-data-gc.md`'s own text (lines 94-99, predating
ADR 0031/0050's reconciler rewrite) describes it explicitly: "a replica
that was down during the drop restarts, re-hosts the tablet from its
marker/engine, then its GC loop reclaims it once its control replica
catches up." That guarantee depended on a durable per-node marker
surviving the restart and forcing a re-host attempt FIRST (so the tablet
would land in the node's own hosted-set again, from which a later-observed
drop could then legitimately trigger `Release`/`Reclaim`) — a mechanism
ADR 0050 removed as part of moving to the reconciler's simpler
`Metadata`-only design, without anything replacing its restart-time "ask
about what I used to host, not just what `Metadata` currently says" role.
Nobody signed off on dropping that guarantee — it was an unstated,
unnoticed casualty of an unrelated simplification, only surfaced when new
deterministic coverage finally exercised the exact restart-across-a-drop
combination for the first time.

**Confirmed empirically, not left as a plausible-sounding static-analysis
claim.** The test was first written as a POSITIVE convergence assertion
(crash a non-leader replica hosting the table, issue `DeleteTable` from a
live node, restart the crashed node once the drop has committed, poll for
the same three observables every other scenario in the same file
converges on) — and it reliably failed, at every one of 6 seeds tried,
never once converging within a 15s virtual-time budget, while the
metadata/hosted-set observables (purged/re-derived independent of
`gather_facts` entirely — `SimCluster::restart`'s own fixture code purges
`ClusterEdgeState` registrations directly, mirroring a real process's
driver tasks simply ceasing to exist) converged fine every time. Only the
physical engine — the actual GC proof, not the bookkeeping — stayed
non-empty forever. This is the general form of a lesson this repo already
half-states elsewhere (a fixture built to prove one property can reveal a
completely different one it never set out to test): **new coverage of a
crash/restart × any-other-fault combination that nothing has driven before
is exactly where an unstated regression from an unrelated refactor hides**
— when a "should obviously converge" test doesn't, on the first honest
try, the right response is to trust the failure over the intuition, run it
at a handful more seeds to rule out a one-off, and only then decide
whether it's a real gap (record it, seed and all, as an `#[ignore]`d
regression a future fix can run against — never assert around it, and
never silently narrow the scenario until it happens to pass) or a fixture
bug (fix the fixture). Landing a green test that quietly avoids the exact
combination its own name promises to test is worse than either — it reads
as proof of a property that was never actually checked.

**A structural fix, if this is ever picked up, needs new capability, not a
tweak.** `gather_facts` has no way to ask "what tablet ids does this node
physically have engine data for, regardless of what `Metadata` currently
says" — that would need a new `EngineFactory::list()`-shaped method (or
equivalent) across the trait and both real implementors
(`MemoryTabletEngines`, `LsmTabletFactory`), consulted once at reconciler
start (or periodically) to seed facts for tablet ids `Metadata` doesn't
currently name at all, with its own reclaim decision once satisfied there
is no live claim to worry about racing. That is real new mechanism, which
is exactly why ADR 0061 rung D4 PR 3 (a driver-plus-assertions PR) reported
it rather than building it.

**Update (2026-09-07, issue #722, closed)**: fixed almost exactly as the
paragraph above predicted. `EngineFactory` gained `local_tablets(&self) ->
BTreeSet<TabletId>` (default-empty, so the widened trait stays
source-compatible for any other implementor); `Reconciler::tick` consults
it exactly once, on its very first tick after construction. See ADR 0024's
and ADR 0061's matching 2026-09-07 amendments, and
`crates/animus-cp-data/CLAUDE.md`'s host-module entry, for the mechanism
as shipped — this entry stays as the discovery record; that is where the
fix itself is documented.
