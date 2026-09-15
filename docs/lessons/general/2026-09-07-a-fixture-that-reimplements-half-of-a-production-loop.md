# A fixture that reimplements half of a production loop inherits the other half's hazards — `SimCluster`'s add-only tablet-host watcher (ADR 0061 rung D4 PR 1, closing issue #715)

D3 PR 2a needed *some* mechanism to host a wire-provisioned tablet under
`SimCluster` (nothing in that fixture ran a real `animus_cp_data::host::
Reconciler`), so it wrote the smallest thing that would make `CreateTable`
work: a per-node loop that hosts a `RaftKvNode` for any tablet whose
`Metadata` replica set names this node. That watcher deliberately
implemented exactly one half of what a real reconciler does — *add* a
missing replica's own hosting — and left the other half out: it never
reacted to a `MetaCommand::CasTabletReplicas` that dropped a replica the
same tablet used to have. The control plane's own `rebalance_placement`
pass doesn't know or care whether anything is physically reconciling its
decisions; it runs unconditionally, so the instant `node_count exceeded
MAX_REPLICATION_FACTOR`, it rebalanced a wire-provisioned tablet's
replica set exactly as designed — and the watcher left a stale, live
`RaftKvNode` running on the node the CAS just removed, genuinely
split-brain-shaped, reachable with a plain `CreateTable` and no fault
injection at all. This was found, characterized precisely (`sim_cluster_
dynamo_table_ops.rs::reconciler_hazard_fires_deterministically_when_
node_count_exceeds_replication`, 25 seeds, byte-identical every time), and
left deliberately unfixed for two whole rungs (D3 PR 2a through 3b) behind
a documented `node_count <= 3` restriction on every test that issued a
real wire `CreateTable` — the honest and correct call at the time, since a
proper fix needed materially more fixture machinery than any single one
of those PRs' own briefs asked for.

**The general lesson**: a fixture that stands in for a production
event-driven loop by hand-coding *only the code paths the fixture's
current tests happen to exercise* will silently reproduce every hazard
the loop's *other* code paths exist to prevent, the moment something
else in the system (here: an entirely separate, correct, unconditional
background process — the control plane's own rebalancer) starts
exercising the path the stand-in never implemented. The stand-in doesn't
need to be buggy in isolation for this to bite — `spawn_policy_tablet_
host_loop` did exactly what its own doc said it did, correctly, forever;
the gap was never a defect in the code that existed, only in the code
that didn't. Two ways this generalizes: (1) when scoping a stand-in for
a real subsystem, name explicitly which of the real thing's own
responsibilities you are and are not implementing, and write that
omission down as a load-bearing constraint on every caller (the
`node_count <= 3` restriction was exactly this — a real, if narrow,
safety net) rather than as an implicit assumption a future test can
silently violate; (2) the actual fix, when it eventually lands (D4 PR 1,
this same rung), is almost always cheaper than re-deriving the missing
half by hand a second time — `animus_cp_data::host::Reconciler` already
existed, was already `SimEnv`-generic and sim-proven
(`reconciler_corpus.rs`), and slotting it in (one `Reconciler` per node,
`on_host`/`on_teardown` hooks mirroring hosting into the fixture's
existing routing registry) needed zero production signature changes at
all — the "more machinery than this PR's own brief asks for" that
justified deferring the fix originally was true of THAT PR's own scope,
not a statement that the real fix was actually hard. Don't let "the
minimal fix for today's ticket" become "the permanent shape of the
fixture" without an explicit, re-visitable decision to that effect — a
`node_count <= 3` comment that outlives the investigation that produced
it is a standing invitation for the next engineer to either violate it by
accident or avoid a whole class of otherwise-useful test shapes forever.
