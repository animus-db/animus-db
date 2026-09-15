# The in-process `--cluster N` shared edge state masks cross-process leader-routing gaps — test cross-process paths *per-process*.

**The in-process `--cluster N` shared edge state masks cross-process leader-routing
gaps — test cross-process paths *per-process*.** In `--cluster N` every node shares
one `ClusterEdgeState`, so an operation that needs to reach *both* the control
leader **and** a per-tablet CP-group leader (e.g. the tablet-split trigger:
`SplitTablet` metadata on the control leader + `propose_split` on the CP leader)
works from any node, because the shared edge reaches both in-process. **Per-process**
(one `ClusterEdgeState` each) those two leaderships can sit on *different* nodes, so
the same call silently fails on every node unless the trigger is forwarded
cross-process. The split-over-`ProdEnv` and re-host tests therefore drive the split
*in-process* (`cp_rehost.rs`) and the reconfigure/failure tests run *per-process*
(`cp_reconfigure.rs`) to exercise the node-local admin views + real failure
detection. When a path resolves a leader, ask "which leader, and is it the same node
as the other leader this path needs?" — and add a per-process test if not.
**Update (ADR 0031 PR2, 2026-08-07): the shared `ClusterEdgeState` root cause
this entry describes is gone** — `--cluster N`'s in-process bring-up
(`start_cluster_with`) now creates a distinct edge-state set **per node**,
exactly like one-process-per-node, and populates `client_route` the same way
`run_node_with` does, so an in-process node genuinely forwards/relays to
reach a leader hosted elsewhere rather than finding it locally via a shared
registry. `--cluster N` and one-process-per-node are now the same code path
in every way that matters to this class of bug. (`cp_rehost.rs`, referenced
above as the in-process split test, no longer exists — split is now a
single control-plane command with no data-plane half to rehost, ADR 0028 —
but the general lesson stands as a *pattern to watch for*: any future
process-scoped convenience shortcut (a shared registry, a shared cache, a
shared claim set) that an in-process multi-node test harness introduces for
convenience can silently mask the same class of cross-process gap, so audit
new shared state the same way.) The general "which leader, is it the same
node" question, and "test cross-process paths per-process," remain sound
advice for any *new* multi-leader coordination this repo adds.
