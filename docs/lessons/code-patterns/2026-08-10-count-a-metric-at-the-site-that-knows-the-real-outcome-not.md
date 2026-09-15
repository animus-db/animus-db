# Count a metric at the site that knows the *real* outcome, not the attempt.

**Count a metric at the site that knows the *real* outcome, not the attempt.**
A counter recorded where an op is *requested* over-counts when a downstream
helper silently no-ops (e.g. `HintStore::record` drops a hint on a residency or
LWW-supersede miss). Have the helper *return* whether it acted and count on that
(`data_hints_stored` counts only hints actually stored). This keeps the closed
`Metric` enum (ADR 0015) append-only/byte-reproducible and the seam observe-only
— instrumenting must never change the path it measures. **When a metric is the
*delta* of a pre-existing monotonic counter** (e.g. recording WAL rotations from
`GroupCommit::rotation_count` around each `commit`, or block reads off a shared
introspection `AtomicU64`), it is easy to wire the source increment yet forget the
`metrics.incr_by(delta)` — the source counter moves but the metric stays 0. A
"counter moved under a known workload" sim test catches exactly this (it did:
`storage_wal_segment_rotations` read 0 while the WAL had rotated 349 times).
