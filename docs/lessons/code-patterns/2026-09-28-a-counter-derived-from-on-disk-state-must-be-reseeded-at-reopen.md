# A counter derived from on-disk state must be re-seeded from that state at reopen

`GroupCommit::active_seg_bytes` (the WAL rotation counter) was initialised to 0
on every open even though the reopened active segment already held bytes. A
workload that restarts between small writes therefore never rotated, and one
segment grew without bound, delaying WAL GC. Nothing failed functionally, so no
crash or recovery test caught it.

Why: a resumable counter mirrors a file's real size; "fresh" is only correct
for a fresh file. Seed it from the post-repair length (after torn-tail
truncation), which recovery already knows.

How to catch it: a SimEnv test that reopens repeatedly between sub-threshold
writes and asserts the resource bound (`reopen_reseeds_active_segment_bytes` in
`crates/animus-storage/tests/lsm_wal_rotation.rs`). Crash-safety corpora do not
check size bounds.
