# Polling every node address does not prove every replica caught up

**Context.** `dynamo_index_scan::gsi_scan_paginates_and_drains_all_rows` (real-thread `ProdEnv`)
flaked on `main` (~0.3-0.7% under CPU contention): a `Limit=2` walk's second page came back with
`a0,b1` (skipping `a2`) and no `LastEvaluatedKey`. Issue #559 had "fixed" an earlier instance by
polling the unpaginated scan against every node address. That was not sufficient.

**Mechanism.** An eventually-consistent read (ADR 0055) is served by a replica that passes a
purely local gate: `engine_applied >= that replica's own commit_index`. A follower learns the commit
index only from the leader's next AppendEntries/heartbeat, so right after the last write it can be
`applied == commit == N-1` while the leader is at `N` and pass the gate one full entry behind.
Traced at the failing run: polls saw `applied=7` replicas, the walk's replica was
`applied=6 commit=6`, and its short page (2 rows where 3 were expected) made `LastEvaluatedKey`
absent, which is correct behaviour for the data that replica held. Which replica answers also
varies per request (local if ready, else the first other replica in NodeId order), so a per-address
poll samples whoever answered that instant, not every replica.

**Lesson.**
- For an eventual read to be stable, prove convergence per replica: every replica's own engine
  applied index >= the highest commit index of its tablet. `support::await_replicas_caught_up`
  does this from `/admin/raftkv` (`engine_applied_index`, `commit_index`, `voters`).
- "Count:N on every address" proves the rows are committed, not that no replica lags.
- An empty/short page with no LEK is not a pagination bug: LEK is emitted only when the `Limit+1`
  probe row exists on the serving replica.
- Do not widen the poll, retry the walk or relax the assertion: the walk is meaningful only over
  converged replicas. Only an *eventual data read* has this hazard: `*_everywhere` helpers that poll
  replicated control-plane `Metadata` (e.g. `dynamo_streams`'s stream-label poll) do not. The
  SimCluster twin is `SimCluster::await_replicas_caught_up` (#1129), called by
  `src/sim_cluster_dynamo_query_pagination.rs` between the per-node poll and the walk.
