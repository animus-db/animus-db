# A change that makes previously-permanent state transient must revisit every earlier test that observed that state live — and the fix is an erasure-proof accounting signal, never a race-tolerant wait

**A change that makes previously-permanent state transient must revisit
every earlier test that observed that state live — and the fix is an
erasure-proof accounting signal, never a race-tolerant wait** (ADR 0049
Train A rung 4, 2026-08-16). Rung 4 extended the hot-trim arm to every
table, which turned plain-table marker records from
accumulate-forever into trimmed-within-a-tick — and silently broke (or
made load-flaky) three earlier rungs' own marker-*emission* regressions,
which counted live `pending_changes()` rows: under suite load the trim
tick could win the race and delete the evidence between the write's ack
and the test's read. No sleep/poll tuning can fix that shape — the
window is real and the test's observable is genuinely transient. The
sound fix is a signal the eraser itself maintains:
`Metric::ChangeLogTrimmedTotal` counts every record the trim deletes, so
the tests assert `live + trimmed-delta == N` — a union a racing trim
cannot erase, and one that still fails on a genuine emission regression
(both terms zero). General form: when rung N+1 adds a deleter for rung
N's observable, rung N's tests must switch from observing the state to
observing state-plus-deletions, and the deleter should export the
deletion count as a real metric (it is genuine operational observability,
not test scaffolding).
