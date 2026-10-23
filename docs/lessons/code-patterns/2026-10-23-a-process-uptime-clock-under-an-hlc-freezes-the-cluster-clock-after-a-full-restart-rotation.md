# A process-uptime clock under an HLC freezes the cluster clock after one full restart rotation

**Found:** 2026-10-23, real-process soak throughput decay (`/home/claude/soak/decay-rootcause.md`).

`ProdEnv::now()` used to be process uptime. The HLC takes its wall component
from `env.now()` and is monotone (a restarted node recovers its floor from the
engine and from witnessed timestamps). After every original process has been
restarted once, no live node's uptime exceeds the cluster's HLC floor, so the
HLC wall stops advancing and only `logical` increments. Everything measured in
HLC wall time then stops elapsing:

- the LSM tombstone/version GC grace (`DEFAULT_TOMBSTONE_GRACE_VERSIONS`, 5 s
  of HLC wall): nothing is ever reclaimable, so read amplification and
  compaction bytes grow with history;
- the read-ceiling lease (`ensure_ceiling_above`, ts + 500 ms): the committed
  ceiling stays above every ts, so ceiling proposals stop.

Symptom: acked writes per epoch fell from ~4000 to ~2000 (and to ~650 in a 2 h
run) while a no-restart control stayed flat and a calendar-anchored clock
restored flat throughput. `SimEnv` never showed it: all nodes share one virtual
clock that a restart does not reset.

Rules:

- A clock that feeds a **persisted, monotone** timestamp must share a time base
  across restarts. `ProdEnv::now()` is now boot calendar time (ns since the Unix
  epoch at bind) plus elapsed monotonic time; it is still monotone within a
  process and only reads the calendar once, at boot.
- A comparison between a persisted timestamp and `env.now()` (issue #1204's
  `RECOVERY_GRACE`) is a symptom of the same mismatch; patching the comparison
  hides the rest.
- To model it in a sim, use `Simulator::set_clock_skew_for(node, -(now - 1s))`
  on a restart; a plain restart in `SimEnv` does not reproduce it.
- When throughput decays with restart count but not with time, compare a
  no-restart control and the per-request counters (tombstones reclaimed,
  block reads per request, compaction bytes) early vs late before reading code.
