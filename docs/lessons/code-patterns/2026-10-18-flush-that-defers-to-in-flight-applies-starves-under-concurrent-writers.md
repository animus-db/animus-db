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
Caveat found while measuring: the 1-engine x 8-writer rig is mostly bounded by a
long compaction holding the maintenance lock (0.3 s growing to 2 s at a 5 ms
sync), during which the memtable fills to its cap; the flush fix doubled the flush
count (23 to 44) but does not by itself restore goodput there.
