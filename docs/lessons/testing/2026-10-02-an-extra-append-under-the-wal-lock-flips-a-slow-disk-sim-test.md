# An extra `append` under the WAL lock can flip a slow-disk SimEnv test

`SimEnv`'s `append` sleeps the node's configured `sync_delay`, just like
`sync` (issue #1092 already collapsed per-record appends for this reason). The
CWL v2 sync marker (#1132) was a second `append` per persist round, held under
`wal_lock`, so a slow-disk learner's round cost 3 latencies instead of 2.
`snapshot_transfer_lands_under_sustained_writes_and_a_slow_learner` (200ms
disk, tuned right at the edge of PR #1047's flood signature) went from 1.7s to
a 90s real-time-watchdog failure (36 ships/advance, limit 20) — deterministic,
and invisible to every other gate.

Fix: markers are cumulative, so skip the marker while the core still
`has_unflushed_wal()`; the next round's marker covers this round, and the last
round of a burst (WAL fully flushed) still gets one. Lesson: any new per-round
I/O on the persist path must be justified against the slow-disk sim tests, and
"after every fsync" claims should be read as "when the WAL goes quiescent".
