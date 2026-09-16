# `await_leader`/`is_control_leader()` proves *a* leader exists, not that *this specific node's* own self-registration has landed in the ADR 0038 `DRIVER_APPLIED` apply task's published cache yet — a single-shot assert right after it is exposed to the apply task's inherent one-hop lag.

**`await_leader`/`is_control_leader()` proves *a* leader exists, not that
*this specific node's* own self-registration has landed in the ADR 0038
`DRIVER_APPLIED` apply task's published cache yet — a single-shot assert
right after it is exposed to the apply task's inherent one-hop lag.**
Adding a `GET /admin/system-table` smoke-check to
`control_only.rs::control_only_cluster_elects_leader_and_serves_status`
(iterates every node right after `await_leader`), a first draft asserted
each node's own system-keyspace browse already showed its
`RegisterNodeAddrs` row — and flaked under `cargo test --workspace`-level
contention (passed every isolated run, failed once mixed with the rest of
the suite's load): a freshly-elected leader's own election no-op can be
the *only* command the async apply task (`meta_apply_loop`) has drained
and mirrored so far (`applied_index: 1, count: 0` in the failure), with
this node's own `RegisterNodeAddrs` proposal still sitting in the Raft log
or the apply task's queue, not yet in the engine it mirrors into. Same
family as the `PutOk`-doesn't-mean-committed entry above, but one layer
further downstream: even a *committed* command isn't necessarily
*mirrored into the system-keyspace engine and published* yet, because ADR
0038 PR3 put an async apply task between "committed" and "visible in
`metadata()`/the system-keyspace browse". Fixed by turning the single-shot
assert into a bounded poll (10s) that re-fetches `/admin/system-table`
until a `node_addrs` row appears, matching this codebase's standing
converged-or-timeout discipline for eventual properties — the general
form: *any* assertion against a freshly-elected cluster's *replicated,
apply-task-mirrored* state needs the same poll, not just assertions
against explicitly-slow operations.
