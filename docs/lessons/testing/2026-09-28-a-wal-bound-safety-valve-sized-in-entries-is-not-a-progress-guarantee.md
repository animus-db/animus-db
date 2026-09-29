# A WAL-retention safety valve sized in log entries is not a bound on progress, and can livelock a genuinely completable transfer under a continuous writer

**Context**: issue #1064's own heavier-load finding, investigated and fixed
alongside issue #1061. `apply_and_compact`'s `COMPACT_DEFER_EMERGENCY_
CEILING` (`crates/animus-cp-data/src/lib.rs`) exists to bound worst-case WAL
retention when a peer's chunked `InstallSnapshot` transfer is genuinely
in-flight but has gone idle just often enough to dodge the primary,
idle-progress-based defer check (`COMPACT_DEFER_IDLE_CEILING`) — a
last-resort escape hatch sized in raw log-entry count (`behind`), deliberately
independent of whether the transfer is actually progressing.

**What went wrong**: under a CONTINUOUS writer, `behind` is driven entirely
by the write rate, which has no relationship to how close a given transfer
is to landing. A joining learner's transfer can be fully in flight and
genuinely advancing — never idle long enough to trip the primary check — and
still get forced out by the emergency ceiling the moment enough writes pile
up, because the ceiling only ever asked "has this quantity crossed a fixed
number," never "is the thing it's meant to protect actually failing to
progress." `RaftCore::snapshot_upto` unconditionally invalidates the
in-flight transfer's blob and every peer's offset bookkeeping the instant
the base moves — required for correctness (shipping old-base bytes labeled
with a new base would corrupt the receiver) — so every forced override is a
full restart from chunk 0, against a strictly larger image (more has been
written since). A write rate the learner's own disk could genuinely sustain
in steady state (proven by an AppendEntries-only control at the identical
rate) still starved a from-scratch snapshot forever: the target it needed to
reach kept moving away from it, at the write rate, on every forced restart.

**The generalizable lesson**: a quantity that is *itself driven by the same
process it's supposed to be a fallback check against* — here, `behind` is
driven by the ordinary write stream, the exact thing racing a lagging
peer's catch-up — cannot honestly answer "has progress stalled." It can only
answer "has this fixed budget of ordinary activity elapsed," which is a
completely different question once the write rate is high enough. A defer/
retry mechanism needs (at most) two independent signals: a *progress* signal
(has the tracked position advanced recently — the correct primary check) and
a *time* or *resource* signal that is NOT itself proportional to the load the
progress signal is racing (a real elapsed-time bound, or — as done here — a
narrower scope exclusion, since the population that genuinely needs
protecting here, a joining learner, has an already-bounded, already-accepted
catch-up contract that makes the entries-sized ceiling redundant with the
idle check for that population specifically). Reusing "how much of the
thing driving the problem has happened" as the safety valve for "is this
still a problem" quietly turns the valve into the bug.

**Where the scope-narrowing fix landed**: rather than retuning the ceiling
(a bigger number just moves the same livelock to a higher, still-reachable
write rate) or removing it outright (it is a genuine, still-needed backstop
for an ordinary VOTER falling into the snapshot path, PR #1047's own flood
scenario), the fix excludes only a transfer in flight to a **learner** from
the override — a learner's own catch-up contract is already "one
`InstallSnapshot`, then promote," so the only question worth asking about
its transfer is whether it's genuinely stalled, which the pre-existing
idle-progress check already answers correctly. A voter's in-flight transfer
is untouched. This is the same shape as the pre-existing "follower-aware
compaction, voters only" decision (`compaction_floor`) for a structurally
different reason at a different layer — both times, "which population is
this mechanism actually meant to protect" turned out to be narrower than
"every in-flight transfer," and widening either one blindly reopened a
different livelock.

**The testing lesson (mirrors the sibling #1064 baseline-metric entry)**: a
bounded-burst-then-drain test proves a different, weaker claim than a
continuous-writer test and cannot exercise this class of bug at all — the
regression needed to keep the writer running for the entire test, at a rate
first validated sustainable via an AppendEntries-only control (a learner
that joins before any compaction has happened, so it never needs a snapshot
at all), before ever exercising the actual from-scratch-snapshot scenario at
the identical rate.
