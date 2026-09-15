# Superseded by ADR 0028

**Superseded by ADR 0028**: `claim_auto_split`/`release_auto_split` and the
cluster-wide contention guard this entry describes are deleted — a
same-tick redundant `SplitTablet` from multiple nodes is now just a normal
epoch-CAS race with one clean winner, no orphan risk to guard against.
Retained for historical record. **`auto_split_loop`'s "only the leader's host triggers" gate
(`ctx.edge.cp_leader(tablet)`) is *not* actually node-scoped — it is scoped
to the shared registry, and under `--cluster N` that registry is shared by
every node.** `ClusterEdgeState::cp_leader` scans **every** registered CP
group handle for a tablet (across the whole cluster's shared `raftkv` map in
`--cluster N` dev mode) and returns whichever one `is_leader()` — it has no
concept of "which node is asking." So in a 3-node `--cluster N` run, all 3
nodes' independent `auto_split_loop` tasks see `Some(leader)` simultaneously
whenever *any* replica leads (always true), and all 3 can independently
compute a (possibly different) median and propose a fresh `SplitTablet` for
the *same* source tablet in the same tick — live-observed as 3 near-
simultaneous `step 1 (split metadata) did not commit` warnings for the same
tablet, and as a tablet's split id churning upward forever
(8→10→12…, each new metadata-only tablet permanently orphaned once a
different key wins the source group's one-time data-plane split). Same root
cause as the documented "per-node decision must dedup on per-node state,
never the shared `ClusterEdgeState`" gotcha (`cp_join_host_loop`,
`/admin/raftkv`'s node-local caveat) — just not yet swept into this loop.
Unlike those, `local_cp`-style per-node scoping isn't available here without
threading this node's own id through the loop, so the fix instead adds a
cluster-wide (not per-node) claim set,
`ClusterEdgeState::claim_auto_split`/`release_auto_split`, that a loop must
win before proposing a fresh split for a tablet — held for the whole
attempt (step 1, step 2, any pending retry) and released only on a
terminal outcome (success, step-1 failure, or abandonment). **General
check when adding a new "only the owning node acts" gate near
`ClusterEdgeState`: does the underlying registry actually distinguish
callers by node, or does it just answer "does *anyone* in the cluster
satisfy this" — those are silently identical in `--cluster N` and only
diverge (bimodally) in a real one-process-per-node deployment.**
