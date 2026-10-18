# A retry loop that waits for space must probe with a fixed-size canary, never rebuild its payload per attempt; and an async fn's locals outlive the failure they were for.

**Issue #1331: with 64 KiB values the disk filled and RSS then climbed 4 GB to
13 GB (OOM at 14 GB) while accepted writes stayed flat.** Two things were
multiplying the log, neither of them the log itself (a massif run showed the
Raft log at ~4 percent of the heap):

- `recover_suspect_wal`'s free-space probe *was* the whole-WAL rewrite: every
  50 ms to 2 s tick, on every stalled group, it cloned the entire in-memory log
  (`wal_image`) and JSON-encoded it (commands are number arrays, 3-6x), only
  for `replace` to fail with ENOSPC again. Probe cost scaled with the log, not
  with what a probe needs to learn ("is there any space?").
- `persist_wal` called `recover_kv_wal(..).await` from inside its ENOSPC branch.
  That future parks for as long as the disk is full, and an `async fn` keeps
  every local alive until it returns, so the failed round's `records`,
  `record_bytes` and `buf` (the same bytes encoded twice) were pinned per
  stalled group for the whole outage.

What to do:

- A "wait until the resource returns" loop probes with the smallest operation
  that answers the question (`probe_free_space`: a fixed 64 KiB `replace`, then
  `remove`) and builds the expensive payload only after it succeeds.
- Before `.await`-ing a long wait from a failure branch, `drop(..)` every local
  the wait no longer needs (or move the work into a helper that returns only the
  error). Check the frame, not the intent.
- Prove it deterministically by counting the amplification in the sim trace, not
  RSS: `raftkv_disk_full` now asserts zero non-canary `replace` ENOSPC faults
  while the disk is entirely full (18 before the fix in `disk_full_early_3`).
  `ProdEnv` RSS confirms but cannot gate.
- When the symptom is "RSS grows, the obvious queue is flat", `valgrind
  --tool=massif` on the debug test binary at a small scale (20 groups) names the
  retaining allocation sites in minutes; `mallinfo`/`malloc_stats` via `gdb`
  separates fragmentation (in-use << system) from live data.
- Never fill a real disk to reproduce: `mount -t tmpfs -o size=256m` and point
  the rig's `TMPDIR` at it.

A leader that is `StorageFull` must also refuse client proposals itself
(`gated_propose`), not rely on the wire edge: with the apply-backlog bound off
the sim accepted 7 980 4 KiB writes into memory on a disk that could persist none.
