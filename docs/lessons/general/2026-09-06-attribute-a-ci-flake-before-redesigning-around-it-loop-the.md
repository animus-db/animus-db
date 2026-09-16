# Attribute a CI flake before redesigning around it — loop the test under a *yielding* contention source at the change and at its parent, and compare rates (PR #666, `split_placing_two_replica_diff_e2e`)

`ProdEnv::send_stream`'s head-of-line fix (#666) spawns one task per send
and so no longer preserves per-destination FIFO between back-to-back
sends. When `two_of_three_replica_diff_placing_target_converges_end_to_
end` failed on that PR's `prod-liveness` shard, the tempting story was
"reordering broke placement convergence". Measuring first showed
otherwise: 6/60 failures at the fix, 5/60 at its parent, with the same
four failure shapes on both sides (issue #670). The design that would
have "fixed" it — a per-destination bounded queue drained by one task —
is a legitimate future improvement but was not the cause, and building
it on that story would have hidden a pre-existing, contention-sensitive
test behind an unrelated change.

Two method points: (1) the `Network` seam's own contract already permits
reordering and Raft/snapshot chunking tolerates it (`raft.rs`'s
"reordered/duplicate chunk is ignored and re-driven"), so a reordering
hypothesis needs evidence, not plausibility; (2) a non-yielding CPU spin
loop is the wrong "pressure": it starves the test's own tokio runtime so
hard that an unrelated bootstrap assertion fires first. Use a real,
yielding contention source (another integration-test binary looping on
the same pinned cores) to reach the code path under test.
