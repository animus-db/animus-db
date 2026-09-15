# Superseded by ADR 0031 PR4 for this specific pair

**Superseded by ADR 0031 PR4 for this specific pair**: `cp_reconfigure_loop`
and its `jitter`/150ms-cadence mitigation are deleted — the tablet-host
reconciler reacts to a `metadata_watch` wake (event-driven), so it observes a
replica-set change on the commit that made it and there is no cadence ratio
left to tune against `reconcile_loop`. Retained for historical record; the
*general* lesson (two independent fixed-period pollers racing a one-shot
outcome) still applies to any future pair of loops. **Two independent, un-jittered fixed-period polling loops that can each "win"
a one-shot outcome are a real, silent flake source — not just theoretical.**
Rewiring tablet split to a single control-plane command surfaced this in
`animusd::cp_reconfigure_loop` (steps a CP group's Raft voters toward a
tablet's replicated replica set) racing `animus-control`'s `reconcile_loop`
(re-CASes a replica set back to satisfy its placement *policy*): a manual
replica-set drop is a **one-shot** race — whichever loop observes it first
decides the outcome, because the loser's own next tick sees an
already-equal-to-desired state and never retries. Both loops polled a fixed
500ms, so once one loop happened to start ticking with an unlucky phase
offset relative to the other (here: an eager, synchronous, awaited
`LsmEngine::open` inserted before the loop's spawn point, versus the
other side's non-blocking, self-spawning `RaftNode::start`), it lost *every*
time, deterministically, for the life of the process — reproduced as
`animusd::tests::cp_reconfigure::cp_group_follows_tablet_replica_set` timing
out 100% of runs in isolation. Re-rolling a random jitter each tick only
turned "always loses" into "wins about half the time across separate test
runs" (a one-shot race stays a coin flip no matter how much you jitter one
side — jitter decorrelates *repeated* ticks, but there's only one tick that
matters here). What actually fixed it: making the loop that must react to an
operator-driven change poll **meaningfully faster** than the policy
loop it's racing (`CP_RECONFIGURE_INTERVAL` cut from 500ms to 150ms, a third
of `RECONCILE_INTERVAL`), so it overwhelmingly observes the change first.
When two independent pollers can produce a stable-but-wrong equilibrium by
one of them winning a single race, check not just "is each one individually
correct" but "does either one systematically have first-mover advantage,
and does that matter" — and if a manual/operator action must reliably beat
an automatic policy-enforcement loop, poll for it faster, don't just add
jitter. (`animusd` `cp_reconfigure_loop`/`jitter`.)
