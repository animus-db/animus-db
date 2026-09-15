# A test asserting that a transient intermediate state occurred must read it from a history the system records itself, never from an external poll — the intermediate's DURATION is an implementation artifact that load can shrink below any sampling interval (2026-09-05, issue #596, `split_placing_two_replica_diff_e2e.rs`).

**A test asserting that a transient intermediate state occurred must read
it from a history the system records itself, never from an external poll
— the intermediate's DURATION is an implementation artifact that load can
shrink below any sampling interval (2026-09-05, issue #596,
`split_placing_two_replica_diff_e2e.rs`).** `reconfigure_step`'s
learner-phased sequencing only guarantees a two-of-three-replica-diff swap
*reaches* the over-replicated 5-voter intermediate (add both learners,
promote both, only then remove the two stale voters) — it says nothing
about how long that intermediate survives before the next reconciler tick
removes an extra. This test proved the intermediate occurred by sampling
`/admin/raftkv` externally every 200ms and asserting on the observed max
voter count; on a 2-core-pinned run under background load it failed about
1 in 3, because consecutive reconciler ticks can legitimately remove both
extras faster than a 200ms poll can land a sample in between — the bug
was in the TEST's assumption that a poll interval bounds a mechanism's own
timing, not in `reconfigure_step`. Fixed by giving `RaftKvNode` its own
durable, in-process `voter_history()` (a small bounded ring, recorded once
per consensus-loop iteration, the same "recompute live, same lock
acquisition" cadence `state_machine_behind`/the quiesce veto already use)
and asserting on that instead — nothing external has to catch the
transient state while it's happening, because the node already recorded
it durably for the length of its own uptime. The general rule: **when a
test needs to prove a system passed through a specific transient state,
and the system doesn't already expose a history of the states it visited,
add that history as a first-class (if minimal) accessor rather than
tightening a poll interval or widening a flaky window** — a poll can only
ever prove "the state existed for at least the sampling interval," which
is exactly the property this kind of mechanism does not promise. **Even a
system-recorded history is only as fine-grained as its own recording
cadence**: the `ProdEnv` e2e sibling records once per consensus-loop
iteration, and a CPU-starved follower's `handle_append_entries` can adopt
two config-change entries in a single batch (the leader only needs a
majority of the OTHER voters to commit the first before proposing the
second), so that one replica's own history can skip an intermediate step
it never got a scheduling chance to observe between the two — the e2e
test asserts the floor/ceiling/5-voter-presence on that replica but keeps
the exact `[3,4,5,4,3]` sequence assertion only in the `SimEnv` regression
(`voter_history_reconfigure_diff.rs`), which has no such starvation.

**A second, orthogonal wrinkle found building the fix, worth its own
callout**: naively taking the union of every currently-hosting replica's
own `voter_history()` and asserting it never drops below the floor
(`RF`) is unsound, because a replica that bootstraps into an
ALREADY-LED group as a quiet non-voter (`host::Reconciler::host`'s
`initial_formation: false` branch, `crates/animus-cp-data/src/host.rs`)
seeds its OWN local `RaftCore` from `Metadata`'s CURRENT `t.replicas`
**minus itself** — pure scaffolding so it knows initial peer addresses
before it has ever heard from the real leader, never a value any quorum
agreed on. Since `Metadata::tablets[..].replicas` already reflects the
DIRECTED-PLACING final target the instant `split_placing` computes it
(ADR 0062 §3) — well before the live Raft swap catches up to it — a
replica bootstrapping through this path can record a transient,
structurally-nonsensical FIRST entry that excludes itself and is smaller
than any value the group's real quorum ever adopted (observed live:
`["m1", "n0"]`, two entries, on a replica whose real join sequence was
the same clean 3→4→5→4→3 every other replica saw). It self-corrects the
moment real sync begins. **The fix is not to touch the recording
mechanism** (it is faithfully recording exactly what `core.config()`
said, which is the contract) **but to filter each replica's OWN history
to start from its first entry that actually includes itself** before
computing anything across replicas — no real committed configuration
ever excludes a member that hasn't joined yet, and Raft's one-member-at-
a-time discipline means a later legitimate "no longer includes me" entry
(this replica's own eventual removal) can only ever follow a genuine
self-inclusive one, never precede it, so the filter cannot hide a real
regression. General form: when unioning several independent observers'
own histories of "what changed," a bootstrap/join artifact specific to
one observer's own start-up path can look identical to a real value in
isolation — the same "does this observer's own reported value make sense
FROM ITS OWN vantage point" question (here: can a replica meaningfully
report a configuration that doesn't include itself, this early?) that
catches a stale/duplicate signal elsewhere in this log applies to a
freshly-added observer's very first report too.
