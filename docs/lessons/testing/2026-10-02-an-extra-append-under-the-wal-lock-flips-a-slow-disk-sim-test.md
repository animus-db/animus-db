# An extra `append` under the WAL lock can flip a slow-disk SimEnv test

`SimEnv`'s `append` sleeps the node's configured `sync_delay`, just like
`sync` (issue #1092 already collapsed per-record appends for this reason). The
CWL v2 sync marker (#1132) was first written as a standalone `append` after each
fsync, held under `wal_lock`, so a slow-disk learner's round cost 3 latencies
instead of 2. `snapshot_transfer_lands_under_sustained_writes_and_a_slow_learner`
(200ms disk, tuned right at the edge of PR #1047's flood signature) went from
1.7s to a 90s real-time-watchdog failure (36 ships/advance, limit 20) —
deterministic, and invisible to every other gate.

The first fix ("skip the marker while `has_unflushed_wal()`") was wrong: under
sustained writes that predicate stays true for whole bursts, so acked records
got no marker and mid-file damage there silently degraded to the torn-tail
path — exactly what #1132 closes. The right shape is **piggybacking**: remember
that the previous round's fsync succeeded and prepend the `!sync:<N>` marker to
the NEXT round's single `append` (N = the file's live length, the marker's own
start; compaction rewrites and failed rounds clear the pending state). Same
guarantee, zero extra appends. Lessons: any new per-round I/O on the persist
path must be justified against the slow-disk sim tests; and when a cost-saving
tweak weakens a safety property, look for a way to make the work free instead
of conditional.
