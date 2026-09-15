# A crashed-but-muted `SimCluster` node's own stale "I still lead" belief generalizes past the CP-data tablet leader it was first found for — check EVERY leader-index accessor a fault scenario calls after a crash (ADR 0061 rung L, C-12 PR 4b)

`sim_cluster_auto_split.rs`'s own module doc already records this for a
CP-data tablet leader's `SimCluster::leader_index_of`: `SimCluster::crash`
**mutes** a node (its tasks keep running, nothing is delivered to or from
it) rather than stopping it, so a crashed former leader's own `RaftCore`
never receives the higher-term vote that would tell it to step down from a
term it already won — its `is_leader()` keeps answering `true` forever,
and an unfiltered leader-index scan that includes the crashed node's own
id can keep returning that same crashed node, never noticing the
survivors' real election.

Writing `sim_cluster_split_cluster.rs`'s own control-leader-failover
scenarios found the **identical** gotcha one layer up, for the
**control-plane** leader: `SimCluster::control_leader_index()` (unfiltered)
has the exact same failure mode after `cluster.crash(leader)`, for the
exact same reason — nothing control-plane-specific about the mechanism,
it's a property of `SimCluster::crash` itself, and applies to *any*
leader-shaped accessor scanning node state after a crash of the node it
would have returned. The fix generalizes identically too: a new sibling
accessor, `SimCluster::control_leader_index_excluding(exclude)`, mirrors
`leader_index_of`'s own filtered-scan shape (skip `exclude` on every poll
iteration) rather than trying to make the crashed node's own belief
correct.

**The general rule, stated once so it doesn't need re-discovering a third
time**: any `SimCluster` fault scenario that (a) crashes a leader and (b)
then needs to find out who leads *now* must use a filtered accessor that
excludes the crashed node's own id — never the plain, unfiltered
leader-index accessor, for the control plane, a CP-data tablet, or any
future leader-shaped state this fixture grows. Check for this the moment
a scenario's own shape is "crash the leader, then ask who leads."

**Confirmed a third time anyway (ADR 0061 rung N, C-14 PR 2)** —
`sim_cluster_control_growth.rs`'s own crash/transfer/serve helper hit the
identical mistake despite this entry already predicting it: a plain
`control_leader_index()` call issued right after `cluster.crash(leader)`
kept returning the crashed node's own frozen `is_leader()==true` belief
(its vec index sorted before the real new leader's), silently misrouting
both a `/admin/control/transfer` retry loop (timing out against a dead
node) and a post-transfer "did it land?" check (reporting the crashed
node's stale belief instead of the grown node's real, live leadership).
Fixed the same way — route every post-crash lookup through
`control_leader_index_excluding`. No new mechanism was added; this is
recorded here only as confirmation that "check for this the moment a
scenario crashes a leader" is worth restating to whoever writes the next
one, not as a new lesson.
