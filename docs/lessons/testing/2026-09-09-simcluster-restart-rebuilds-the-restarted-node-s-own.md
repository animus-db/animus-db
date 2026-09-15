# `SimCluster::restart` rebuilds the restarted node's own control-plane Raft log from scratch (`RaftNode::start(.., MemoryEngine::new())`) and relies on ordinary peer catch-up to repopulate it — a 1-node cluster has no peer to catch up from, so restarting node 0 on a `SimCluster::new( seed, 1, 1)` cluster loses ALL replicated `Metadata`, including every table's own schema

**`SimCluster::restart` rebuilds the restarted node's own control-plane
Raft log from scratch (`RaftNode::start(.., MemoryEngine::new())`) and
relies on ordinary peer catch-up to repopulate it — a 1-node cluster has
no peer to catch up from, so restarting node 0 on a `SimCluster::new(
seed, 1, 1)` cluster loses ALL replicated `Metadata`, including every
table's own schema** (ADR 0061 rung J, C-10 PR 3). Only the DATA-plane
tablet engines survive a restart (`self.engines[node]`, reused
deliberately, per that method's own doc); the control-plane log is not
durable across a restart the way a real `ProdEnv` node's on-disk WAL is.
A real-socket test converted to this fixture that crashes-and-restarts a
**single** node and expects its own prior schema/catalog state to still
be there afterward needs a multi-node cluster instead (so the restarted
node recovers via replication, exactly like production), even when the
original test used one real process. Found converting `tests/
update_table_drop_index.rs::a_crash_and_retry_mid_cascade_still_
converges` (a real single-node crash/restart test) — the fix was simply
running the equivalent `SimCluster` scenario at `(seed, 3, 3)` instead of
`(seed, 1, 1)`, not a fixture change.
