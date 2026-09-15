# A node-local lock cannot protect a cross-node invariant — move the evaluation to the node that already serializes the data (its leader), don't try to distribute the lock.

**A node-local lock cannot protect a cross-node invariant — move the
evaluation to the node that already serializes the data (its leader),
don't try to distribute the lock.** `index_aware_write`'s DynamoDB write
path read a table's prior item and diffed its LSI/change-record image at
whichever edge node received the request, serialized only by a
**node-local** `rmw_lock`. Two edge nodes writing the same item never
contended on that lock — each could read → diff against the same stale
prior value, and the loser's stale LSI row orphaned forever (nothing
reconciles a stale LSI row once written; only the GSI drain, being a full
re-derivation, self-heals). No amount of making the lock "smarter" at the
edge fixes this, because the edge is fundamentally the wrong place to
hold it — a lock only serializes callers that actually contend on the
*same instance* of it, and two different processes never share one. The
fix (ADR 0046 U3, "evaluate at leader") moves the read-diff-propose
sequence onto the item's own tablet leader — the one node every write of
that item reaches regardless of which edge node the client connected to
— so the identical lock, now held on the leader, actually serializes
what it was always meant to. General form: before adding or hardening a
lock to close a race, ask whether every racing caller can actually reach
the *same* lock instance; if the callers are different processes/nodes
and the lock is per-process, the lock is decorative, and the fix is to
move the guarded work to a single point of serialization that already
exists for other reasons (a leader, a single writer, an owning shard) —
not to invent a new distributed lock. **A repro for this class of bug
needs genuine cross-node contention, and unrelated setup noise can mask
the actual assertion before it ever runs — the fix is to get past the
noise, not to accept a differently-shaped failure as good enough.** The
first version of this stack's hammer test hard-panicked
(`assert_eq!(200)`) on any non-200 write reply; against the unfixed
baseline it reliably (8/8 runs) failed via a request-level
`InternalServerError` ("CP kind write did not commit in time" / "relay to
peer node failed") on the very first write of the very first iteration,
never by surviving to the intended "assert exactly one live LSI row"
check. The actual root cause turned out to be a genuine but **separate**
t=0 race: a brand-new tablet's own Raft group needs its own leader
election, and every node's tablet-host reconciler needs to observe it
should host/relay for it, before either edge node's very first write can
resolve a route — `CreateTable`'s own success only guarantees the
*catalog* entry committed, not that every node has already reconciled
hosting for it. The unfixed design's extra edge-to-leader hop (a separate
forwarded read before the forwarded propose, versus the fix's single
forwarded RPC) doubled this window's exposure, which is exactly why the
fix *looked* like it closed the race outright rather than merely
shrinking a pre-existing, unrelated one. Two changes closed the gap
between "a failure" and "the failure this test is for": (1) a few
retried, sequential warm-up writes against a key the race never touches,
settling routing before the concurrent hammer starts, and (2) making the
hammer loops tolerate a write's own transient non-200 (counting
acked/failed rather than asserting on any one outcome — a write that
times out at *confirm* is not known to have failed at *propose*, so
asserting on it asserts against an unknown) while gating on a run-health
minimum (most writes must still land) so a vacuously "healthy" run
doesn't silently masquerade as a real one. With both in place, the
**same** unfixed baseline failed differently and far more informatively:
every one of 7 runs reached the actual orphan-row assertion and failed it
outright (7/7, 100%), with 2–6 live LSI rows (vs. the expected 1) each
time, containing verifiably stale `alt` values from iterations neither
loop's own latest write named. **The general lesson: when a repro's failure
mode doesn't match the mechanism it's supposed to demonstrate, don't
rationalize the mismatch as "still legitimate evidence" and move on —
the mismatch is itself a signal that something upstream of the intended
assertion is misfiring, and it is usually cheap to isolate and fix
(here: settle routing first, tolerate transient noise, then assert).**
(ADR 0046 U3, `crates/animusd/tests/dynamo_index_writes.rs::
cross_node_racing_unconditional_puts_never_orphan_an_lsi_row`,
2026-08-16.)
