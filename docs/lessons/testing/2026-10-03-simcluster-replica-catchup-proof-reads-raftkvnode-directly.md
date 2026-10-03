# A SimCluster replica catch-up proof reads each RaftKvNode directly, and the race may not reproduce there

**Context.** #1129: `sim_cluster_dynamo_query_pagination`'s GSI walk rotated nodes after only a
per-node eventual poll (the shape #1128 fixed for real sockets). Added
`SimCluster::await_replicas_caught_up`.

**Lessons.**
- In `SimCluster` there is no need for `/admin/raftkv` (which burns an `OP_BUDGET` jump per call):
  `edge.hosted_groups()` gives every node's `CpGroup`, and `commit_index()` / `engine_applied_index()`
  / `config()` give the per-replica proof directly. Poll it with `run_for(50ms)` steps, never a
  one-shot, and dump per-replica progress in the timeout panic.
- Index rows (GSI/LSI) live in their base tablet's own engine, so "every tablet" already covers them.
- Honest limit: with the wait removed, 200 seeds of the GSI walk did not fail and the helper never
  saw a lagging replica at entry (0/100 seeds), because `drain_gsi` plus the per-node `dynamo` calls
  already burn enough virtual time for AppendEntries to propagate. The wait is a guard against a
  latent race, not a fix for an observed one; do not claim it was reproduced.
- Audit result: other rotating walks (`query_filter`, `scan_index_forward`, `select`, `parallel_scan`,
  `upgrade_corpus`) use `ConsistentRead: true` (ReadIndex) so the replica-local gate does not apply.
