# A prior "delete it" recommendation can conflate two different costs that share a symptom — measure the specific one before trusting it (roadmap C-05, `SharedWal`)

`animus-control::shared_wal.rs` (ADR 0028) is a built-and-unit-tested,
never-wired multi-tenant WAL I/O coordinator, and `docs/roadmap.md`'s C-05
carried a "delete" recommendation resting on ADR 0048's quiescence
finding — "the apply-poll term dominated" the per-group idle cost. Read
at face value that sounds like it settles the WAL question too: if
per-group overhead isn't the bottleneck, why would coalescing per-group
WAL files be? But ADR 0048's finding was about **idle** cost (heartbeat
ticking, the apply task's own idle poll) for a group with nothing to do —
a cost quiescence genuinely closes, no fsync involved at all until a
group wakes. `SharedWal`'s whole reason to exist is a completely
different cost: **active-write** fsync count when *multiple groups have
something to persist at the same moment*. Quiescence doesn't touch that
case — a quiesced group isn't fsyncing regardless, and an active one
fsyncs exactly as often whether or not its idle siblings are ticking.

A cheap, exact `SimEnv` measurement (single-voter tablet groups so the
count isolates WAL persistence from network/quorum effects — see the
task write-up for the harness shape, not committed) settled it directly:
a burst across `K` concurrently-active groups on one node costs `K`
separate `fsync`s (`K=1` → 1 sync, `K=32` → 32 syncs, zero cross-group
coalescing), while a burst to the *same* group already coalesces for
free (32 writes to one group before its persist round runs → 1 sync —
the existing `persist_round.rs` group-commit batching working exactly as
designed, just scoped to one group). The amplification is real, it is
specifically cross-group under concurrent load, and quiescence's own
finding says nothing about it either way — `persist_round.rs`'s own
module doc already named the live version of this (a split multiplying
"the concurrently `fsync`ing groups") as the actual root cause behind a
real production livelock (issue #279), which should have been the
first place to check before trusting the idle-cost finding as a proxy.

**The general lesson**: when a prior recommendation to delete unwired
infrastructure cites evidence from a *related* optimization, check
whether that evidence is about the *same* cost the infrastructure
addresses, or merely a *neighboring* one that happens to share a
symptom (here: "per-group overhead on a busy tablet fleet"). Idle-time
cost and active-load cost are different failure modes with different
fixes even when they show up in the same subsystem; a fix for one is not
evidence against the other, and the cheap way to tell them apart is
usually a small, targeted, throwaway measurement rather than trusting
the adjacent finding's prose to generalize. See `docs/roadmap.md`'s C-05
entry (2026-09-02) for the numbers and the resulting "keep, defer wiring"
decision.
