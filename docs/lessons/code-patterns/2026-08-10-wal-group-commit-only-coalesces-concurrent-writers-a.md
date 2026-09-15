# WAL group commit only coalesces *concurrent* writers — a *sequential* apply loop pays one `fsync` per op and needs an explicit batch primitive.

**WAL group commit only coalesces *concurrent* writers — a *sequential* apply
loop pays one `fsync` per op and needs an explicit batch primitive.** The
per-tablet CP-data Raft apply loop (`flush_and_apply`) applies a run of committed
commands from **one** task, `await`ing each `merge`/`merge_tombstone` in turn; the
WAL group commit (which amortizes the `fsync` across writers *ready in the same
drain cycle*) sees exactly one in-flight writer, so every command paid a full
`fsync` (~5-9ms on real disk; ~180ms for a 20-40 command batch). The fix is a
`StorageEngine::merge_batch(Vec<MergeOp>)` that logs the whole run as **one WAL
record + one `fsync`** then applies it under one lock (defaulted to a per-op loop
so `MemoryEngine`/others are unaffected; `LsmEngine` overrides it). The apply loop
accumulates a run of `Put`/`Delete` into a batch and drains it before any command
that must *read* committed state (`Cas`, `Split`) so the read still sees prior
writes. Measured ~9.7x apply throughput (1851→17918 puts/s), fsyncs 30x fewer.
**Lesson: "we have group commit" does not make sequential writes cheap — group
commit is a concurrency optimization; a single-task write run needs a batch API.**
(`animus-storage` `merge_batch`; `animus-cp-data` `flush_and_apply`.)
