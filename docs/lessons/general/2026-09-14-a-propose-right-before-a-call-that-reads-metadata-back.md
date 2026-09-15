# A `propose()` right before a call that reads `Metadata` back needs virtual time in between, not just between a propose and a crash (ADR 0061 rung N, C-14 PR 4)

Every prior instance of this lesson in this crate was framed around a
propose racing a *crash* (`sim_cluster_growth.rs`'s own scenario (d),
`sim_cluster_backup_janitor.rs`'s own finding): `propose()` only appends to
the leader's own local Raft log, so crashing immediately afterward can lose
an entry that never left the leader — fixed by advancing virtual time
before the crash. `SimCluster::grow_combined()`'s own first draft hit the
identical root cause in a shape with no crash anywhere in it: it proposed
`RegisterNode`+`UpsertMember{Active}` directly on the current control
leader (the fixture's own control-plane bypass), then immediately called
the real `POST /admin/control/member/add` admin route with zero virtual
time advanced in between. `admin_add_control_member`'s own read-your-writes
barrier (the issue #406/#450 fix) bound-waits on `engine_applied_index() >=
commit_index()`, where `commit_index()` is captured **at the barrier's own
call start** — with nothing advancing the simulator between the proposes
and the admin call, that captured commit index can itself be read before
the two just-proposed entries have committed, so the barrier catches up to
a stale commit index, `Metadata::node_addrs` reads as not-yet-containing
the node, and the admission call's own bounded retry-on-collision loop
races the real (eventually-committing) registration to a permanent `409`:
"node n3 is already claimed by a different registration." The general
form: the lesson is not "advance time before a crash" specifically — it is
"a `propose()` commits and applies only once the Raft driver actually
runs, so ANY subsequent call in the same scenario that reads `Metadata`
back (a crash, an admin action, a wire read, an admission route with its
own read-your-writes barrier) needs real virtual time between the propose
and that read," and the fix is the same `self.sim.run_for(..)` margin
either way. No production code was at fault — `admin_add_control_member`'s
barrier behaved exactly as designed against a caller that gave it no time
to observe anything.
