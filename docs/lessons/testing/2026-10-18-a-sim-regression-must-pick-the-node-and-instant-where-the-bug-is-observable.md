# A sim regression must pick the node and the instant where the bug is observable

Seven past bugs (confirm-loop false supersede #911/#967/#971, propose pacing
#268/#42, CreateTable ack #265, half-staged 2PC #162, RF growth #149, stale admin
add #712, readiness on quorum loss #1274) were re-pinned in
`sim_cluster_regressions.rs`. Several first drafts passed with the fix reverted,
for reasons that recur:

- **Reverting the guard is the only proof the test sees the bug.** Run the revert,
  read the failing assertion, then restore. The propose-pacing test for
  `propose_and_await` passed with the pacing removed because it ran on the control
  leader: there the call holds its single pending entry and never re-proposes at
  all. The bug is only observable on a *relaying* node (a data-only node), so the
  fixture needs one. Ask "which node runs the code path that has the bug".
- **Fixture helpers hide the window.** `SimCluster::dynamo`/`run_with_ctx` run a
  fixed virtual budget after the op returns, so by the time the caller looks, group
  formation, election and apply have all finished. To test "at the instant of the
  ack", spawn the op as a task and sample the cluster in the same task step the
  response lands (no `await` in between).
- **A confirm-loop duplicate is only visible through a side effect, not a
  counter.** A genuinely lost accepted entry legitimately costs a second proposal,
  so "proposals == writes" is not an oracle under leadership flaps. Count change
  records per key instead (each key written once, never retried by the test): more
  than one record is a double apply. Sweep the fault delay (cut the leader 1..24 ms
  after the accept) so the cut lands before, during and after replication of the
  accepted entry; only the middle slice is the bug.
- **The fixture spawns no background loops.** `txn_resolver_loop` does not run, so
  a read of an anchor key whose coordinator died blocks forever on its own intent;
  drive `ClientCtx::txn_recover` from a survivor instead, once per attempt.
  Likewise a lone former control leader keeps leading (no check-quorum step-down) and
  its `/admin/health` stays 200: for a quorum-loss readiness test the survivor must
  be a follower.
- **Measure noise, then subtract it.** The idle control leader appends a log entry
  per heartbeat (240 per 12 s), so a pacing bound is on `growth - idle_baseline`,
  and a `>= 1` floor stops the test passing vacuously when nothing is accepted.
- **Know which races SimEnv cannot reach.** Issue #1308's two `engine_applied_index`
  samples are consecutive synchronous calls; the simulator is cooperative, so no
  apply can land between them. That bug stays covered by the pure `kind_poll`
  unit tests, not by a sim cell.
- **When several agents share a worktree and a scratchpad, name backups uniquely.**
  Two agents both used `schema.rs.orig` for their revert-and-restore bite checks
  and overwrote each other's backup. Use a per-agent prefix, or `git stash`-free
  `git diff` patches, and re-check `git diff` for the file afterwards.
