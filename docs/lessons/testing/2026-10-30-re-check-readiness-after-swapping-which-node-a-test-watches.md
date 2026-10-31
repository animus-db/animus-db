# Re-check readiness after changing which node a test watches

`animusd` `tests/admin_endpoint.rs` `admin_ready_is_200_while_leaderless_with_metadata`
waited for node 0 to report `/admin/health` and `/admin/ready` 200, then swapped a
control-plane follower into slot 0 (so the survivor of the quorum loss is not the
leader) and stopped the other two nodes. The convergence wait had only proved
the *original* node 0 ready. The swapped-in follower could still be at
`last_applied == 0` (it had not yet learned the commit index), and once its two
peers were gone it never would, so `/admin/ready` stayed 503 with
`metadata_synced: false`. It failed in the merge queue for PR #1440.

**Why it generalizes:** a precondition proved on one node does not carry over
when the test later picks a different node to observe. Whenever a test re-selects
its subject (by role, leadership, index), re-run the convergence wait on the new
subject before injecting the fault, especially a fault (quorum loss) that freezes
the state the precondition depends on.
