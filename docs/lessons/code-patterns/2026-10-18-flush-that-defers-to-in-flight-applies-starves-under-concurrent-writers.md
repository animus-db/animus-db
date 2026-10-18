# A flush that defers to in-flight applies starves under concurrent writers

`LsmEngine::flush` is a no-op while a durable-but-unapplied write is in flight
(`applies_in_flight > 0`), and the writer re-attempts from its own
`maybe_flush_and_compact`. With one writer per engine that is a few microseconds
of deferral. With 8 concurrent writers per engine on a slow disk (ADR 0081
characterisation, `e8_w8_5ms`) there is always some write in flight, flush
attempts keep returning without flushing, the memtable climbs to its hard cap
and the engine stalls on the **memtable** for about 2,000 s of stall time in 60
s of virtual time, with no L0 build-up at all. It looks like a stall bug and is a
flush-liveness defect that the stall merely exposes.

Rules:

- When a stall appears with L0 low, read `storage_flush_blocked_micros` and the
  memtable cause counters before touching the L0 levels.
- Measure with one writer per engine first; add concurrency as a separate axis,
  or the rig measures the starvation instead of the mechanism.
- Followed up as an issue rather than folded into the stall change: fixing flush
  to wait for the in-flight set to drain (or to snapshot a memtable prefix) touches
  the part the crash corpora guard hardest.

Resolved (issue #1341): `flush` now closes an entry gate (`Inner::flush_pending`,
cleared by an RAII `FlushPendingGuard`) so new writers park before logging,
waits for `applies_in_flight` to drain (already-admitted applies need nothing
from the flush, so the wait is one commit + apply), and snapshots in the same
critical section that sees zero. A "skip if busy" attempt against a live
producer is a starvation; a "close the door, drain, then act" is not. Two
pitfalls the fix avoids: set the gate only *after* taking the maintenance lock
(else writers block for a whole compaction), and make the gate RAII (a dropped
flush future must not leave it shut). Regression: `tests/it/lsm_flush_starvation.rs`.
Second half (same issue): fixing the skip alone doubled the flush count but not goodput,
because a flush still waited for a running compaction on the one maintenance lock, and the
background mode ran flush and compaction in one task, so even a free lock could not help. The
rule: **a lane is the lock plus the task that drives it.** Splitting only the lock changed
nothing in the measurement (identical numbers); splitting the task as well removed the memtable
stall entirely (190,000 ms of stall to 0) and moved the bound to L0, i.e. compaction cost. To run
flush beside compaction: serialize them only on a short manifest-install section that builds the
new manifest from the *current* one; allocate file seqs from a shared in-memory counter, not the
manifest; keep per-lane failure state so one lane's success cannot clear the other's ENOSPC
verdict. When a measurement does not move after a fix, trace timestamps before trusting the
hypothesis: here the first model (the lock) was only half the answer.
