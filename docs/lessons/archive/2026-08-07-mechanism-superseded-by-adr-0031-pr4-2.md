# Mechanism superseded by ADR 0031 PR4

**Mechanism superseded by ADR 0031 PR4**: `cp_gc_tablet`'s `current_range`
parameter (the by-convention fix this entry describes) is deleted — the
planner's `HostAction::Release` now *carries* the erase bound
(`erase_bound`, always the tablet's current replicated range, never a
scope fact), and the executor narrows to it immediately before erasing, so
the narrow-before-erase ordering is a structural property of one function's
output rather than a convention between two unrelated loops. Lesson
retained for the record. **A teardown that erases "my own scope" must re-derive the scope from
replicated state at the point of irreversible action — not trust an
in-memory cache that a *different* code path is responsible for keeping
current.** The fix directly above (`cp_join_host_loop` re-narrowing an
already-hosted tablet's `StorageScope` every tick) has a gap the very next
feature exposed: that re-narrow is **permanently skipped** the instant this
node leaves the tablet's replica set (`plan_join_host` returns `None` →
the loop's `continue` — the pure join-host decision, correctly, never touches
a tablet this node no longer replicates). So a node dropped from a
just-split tablet's replica set *within one join-host tick* (~250ms) of the
split — exactly the shape ADR 0029's rebalancer produces, since a split
raises the hosting nodes' replica counts, which is what makes the
rebalancer target that tablet next — is left with a **permanently
stale-wide** `StorageScope` for the tablet it's being removed from. The
removed-replica release GC (`cp_gc_tablet`) used to call `erase_scope()`
straight off that group's own (possibly stale-wide) scope; since ADR
0026/0028 put every tablet a node hosts on **one shared engine**, an
unbounded erase there doesn't just fail to reclaim less than it should — it
actively **tombstones a co-hosted sibling's live keys** (the split's new
child, which the departing node is typically still a replica of, since the
replica-set change only touched the *parent*), at a version high enough to
beat the child's own fresh writes under per-key LWW: silent, permanent
corruption of a tablet this node was never even asked to release. The fix:
the release phase now passes the tablet's **current** `Metadata`-replicated
range into `cp_gc_tablet`, which calls `narrow_scope` on it immediately
before `erase_scope` — bounding the erase to what the tablet's replicated
state says right now, not to whatever the group's own cache happened to
freeze at. **General check when a fix makes some cached/derived value
"usually current": ask what happens at the one moment that value is about
to be used for something irreversible (here, an engine-wide tombstone
erase) if the normal refresh path was never given a chance to run — refresh
it again, from the authoritative source, right at that point, rather than
trusting the ambient cache is fresh enough.** Same family as the
`RaftKvNode` read-barrier `all_nodes`-vs-`config()` entry below, and as the
"prefer a live read of the durable layer over observation-built in-memory
state" entry. Caught by design review before it ever shipped, not by a live
incident. (`animusd` `cp_gc_tablet`'s `current_range` parameter,
`cp_gc_release_phase`; `animus-cp-data/tests/
narrow_scope.rs::narrow_then_erase_scope_spares_a_co_hosted_siblings_data`
deterministically proves the mechanism at the primitive level;
`animusd/tests/cp_rebalance_gc.rs::
split_then_immediate_release_spares_the_new_siblings_data` is the end-to-end
regression — worth noting **how** it forces the race: proposing the split
and the follow-up replica-set CAS back-to-back on the control leader's own
Raft log, rather than round-tripping the split through the client wire
protocol first, is what makes the race reproducible at all — the
wire-protocol version gave the ~250ms join-host tick enough real time to
win and self-heal the scope before the drop landed, catching the pre-fix
bug 0 times in 5 runs; the back-to-back-propose version caught it in ~3 of
5. Even so this remains a genuine timing race, not a deterministic repro —
the primitive-level test is what actually proves the fix, the E2E test is
corroborating evidence.)
