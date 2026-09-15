# A fixture's own bookkeeping map can silently exclude an entire creation path — a "does this tablet have a leader" helper scoped to one map answered `None` for every wire-created table (ADR 0061 rung D3 PR 3b)

`SimClusterHandle::leader_index_of` (`sim_cluster.rs`) resolved a tablet's
current leader by scanning `Self::replicas_of(tablet)` — a lookup into
`self.tablets`, the `BTreeMap<TabletId, TabletInfo>` that exactly one call
site, `SimCluster::create_table_with_replication` (the fixture's own
*hand-hosted* table path), ever inserts into. Every `SimCluster::drain_gsi`
caller this PR added creates its table through the real DynamoDB wire
instead (`cluster.dynamo(0, "..CreateTable", ..)`, the path PR 2a/3a
built) — a path that never touches `self.tablets` at all, since it hosts
tablets via `spawn_policy_tablet_host_loop`'s own `Metadata::policies`
watcher, not this method's bookkeeping. The result was not a compile error
or an obviously-wrong value: `leader_index_of` simply, silently, returned
`None` for every wire-created table's tablet — `replicas_of` came back
`Some(vec![])`-shaped empty, so `.find(..)` over an empty iterator is a
completely unremarkable `None`, indistinguishable at the call site from
"this tablet genuinely has no leader yet."

**The general lesson**: a helper that resolves "the current state of X"
by scanning a fixture's own bookkeeping map, rather than the live
domain state that map is meant to summarize, silently stops working the
moment a second creation path populates the live state without also
updating that same map — and the failure mode is a clean, plausible-looking
`None`/empty result, not a panic or a type error, so it will not surface
until a *test* exercises the second path and gets an unexpected failure.
The fix here was to stop trusting the bookkeeping shortcut and scan the
authoritative, always-current signal instead (`ClusterEdgeState::
local_cp(tablet).is_some_and(|g| g.is_leader())`, checked across every
node id rather than only the subset one creation path happens to record) —
strictly more general, and provably no less correct for the original path
either, since the underlying per-node check already answers `false` for
any node that was never given a reason to host that tablet. When a fixture
grows a second way to create the same kind of object, audit every helper
that reads the *first* way's own private bookkeeping before assuming it
still answers correctly for the second.
