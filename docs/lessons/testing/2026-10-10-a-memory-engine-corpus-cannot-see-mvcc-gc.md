# A `MemoryEngine` corpus cannot see MVCC GC; test the retention contract on the LSM with a control

Issue #1206: `LsmEngine` compaction dropped history ~1 ms behind the newest
write (`1 << 20` raw versions of an HLC-packed `wall_ms << 20`), so snapshot
reads at an older timestamp and the multi-tick backup capture could lose the
version they needed. Every sim corpus passed because it ran on `MemoryEngine`.

- **Check that a corpus actually reaches the code path you think it does.** The
  txn corpus' LSM tier looked like the right place for a `read_at` cell, but
  its reader had been rewritten to latest-read rounds (`quiescent_multi_read`),
  so a hold-disabled negative control never failed. A control that cannot fail
  proves nothing: write it first and see it go red.
- **A dedicated cell beats a bolt-on to a big corpus** when the property is
  narrow: `animus-test` `lsm_read_holds.rs` writes, takes a timestamp / cut,
  churns for longer than the grace in virtual time (flush threshold of a few
  hundred bytes), and compares. Each positive cell has a control (old 1 ms
  grace; no hold) that must diverge, and the control asserts the read was
  *served* (a refused `read_at` is `None` and would pass a `!= expected` check
  vacuously).
- **A hold only protects versions that still exist when it is taken.** The time
  grace is the floor under a late hold; do not rely on a per-call hold for a
  timestamp that is already older than the grace.
- **Grace units depend on the version space.** A default expressed in HLC wall
  time is ~5e9 entries for an engine versioned by Raft index (the control
  syskv), i.e. never reclaim; such engines open with `LsmOptions::raw_versions()`.
