# A fix must cover every path to a dangerous primitive's sink, not just the caller that surfaced it.

**A fix must cover every path to a dangerous primitive's sink, not just
the caller that surfaced it.** ADR 0018 §2/PR2b's own
`next_ceiling_candidate` doc named the hazard precisely: never call
`Hlc::witness` on a value deliberately shifted `HLC_MAX_OFFSET` into the
future, because witnessing permanently ratchets the group's shared clock
toward it, poisoning every later ordinary mint. That fix built a
separate CAS ratchet for the one call site that surfaced the bug
(`ensure_ceiling_above`'s ceiling-candidate disambiguation) — but
`RaftKvNode::mint_pushed` (`crates/animus-cp-data/src/lib.rs`) had its
*own*, independent call to `self.hlc.witness(floor, ..)` on a floor that
folded in the same future-shifted ceiling, unconditionally, on every
write. Nothing caught this for weeks: the two call sites don't call each
other, so grepping "does `next_ceiling_candidate`'s caller still do this
right" finds nothing, and the existing amortization test only exercised
reads, never interleaved reads-and-writes, so it never drove the second
sink. The bug was a live, self-sustaining feedback loop (a write
witnesses the ceiling forward, the next read mints near the poisoned
clock and exceeds it almost immediately, forcing a fresh ceiling
proposal, which the next write folds in and witnesses again) — a
k×`HLC_MAX_OFFSET` runaway roughly one window per round, independent of
real elapsed time, plus propose-path starvation from the resulting
`ReadCeiling` churn. The general move: when a postmortem or doc comment
names a primitive as dangerous in a specific way ("never call X with a
value shaped like Y"), **grep every caller of that primitive**, not just
the one under investigation — a doc that explains *why* a fix works for
one call site is not evidence every other call site got the same fix.
The regression this time had to be a genuinely different shape from the
existing amortization test (interleaved reads *and* writes on a tight
loop, asserting the group's clock never diverges from real elapsed time
by more than a small bounded multiple of `HLC_MAX_OFFSET`) — a read-only
workload structurally cannot reach a write-side sink. (`crates/
animus-cp-data/src/lib.rs::mint_pushed`; ADR 0018 §2 amendment,
`tests/ts_cache.rs::interleaved_reads_and_writes_never_let_minted_
timestamps_outrun_real_time`, 2026-08-15.)
