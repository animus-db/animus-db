# An operator/admin action that calls straight into an engine bypasses the single-writer contract the normal path establishes — audit every admin surface against the layer's concurrency assumptions.

**An operator/admin action that calls straight into an engine bypasses the
single-writer contract the normal path establishes — audit every admin surface
against the layer's concurrency assumptions.** `LsmEngine` is safe on the client
path because the per-tablet Raft apply loop is its sole writer, but
`POST /admin/storage/flush|compact` call `flush_now`/`compact_now` from the admin
connection's task, racing that loop — and `flush()` (snapshot → unlocked build →
unconditional `memtable.clear()`, no flush-in-progress flag) then erases an acked
concurrent write, whose WAL segment a *later* flush GCs: permanent loss. The
concurrency tests miss the quadrant (the concurrent-writer test never flushes;
the flushing test has one writer) — test "forced maintenance under live load"
explicitly. (2026-08-06 audit; ADR 0008/0020 notes; fix = serialize
flush-vs-apply and flush-vs-flush.)
