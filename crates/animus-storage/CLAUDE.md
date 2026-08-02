# CLAUDE.md — animus-storage

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The `StorageEngine` trait and its backing implementations. The trait is driven
by what the distributed layer needs, not by any one engine (ADR 0004, 0008).

## Entry points

- `lib.rs` — `StorageEngine` and `Snapshot` traits; `WriteBatch`, `WriteOp`,
  `VersionedValue`, `StorageError`.
- `memory.rs` — `MemoryEngine`: `BTreeMap` MVCC store, deterministic; the engine
  used under simulation.
- `lsm.rs` (+ `lsm/sstable.rs`) — `LsmEngine<E: Env>`: a **real on-disk LSM**
  doing all I/O through the `Env` `Disk` seam, so it is deterministically
  crash-testable under `SimEnv` (ADR 0008). `LsmOptions` tunes the flush
  threshold + compaction trigger.

## What's non-obvious

- **The traits are `async`** (`#[async_trait::async_trait]`). The I/O-ish
  methods — `put`/`merge`/`merge_tombstone`/`delete`/`delete_range`/
  `write_batch`/`get`/`get_at`/`scan`/`entries`/`entries_with_tombstones` on
  `StorageEngine`, and `get`/`scan` on `Snapshot` — are `async fn`; callers
  `.await` them. This is so an on-disk LSM can reach the async `Disk` seam
  (SSTable block reads/flushes) behind the same trait. `snapshot()` and
  `latest_version()` (and `Snapshot::version()`) stay **synchronous** — pinning
  a version and reading the floor are cheap, in-memory on every backend.
  `MemoryEngine` awaits nothing real (its bodies are unchanged logic inside
  `async fn`); `LsmEngine` is the one that actually reaches the disk. Storage-only
  tests with no `Env` drive the futures with `futures::executor::block_on`; code
  already inside a `SimEnv` task just `.await`s.
- **Versions are MVCC commit timestamps supplied by the caller and must be
  strictly increasing** (enforced via `StorageError::NonMonotonicVersion`).
  Given that, a `Snapshot` taken at version `v` is isolated from later writes —
  snapshots are version-pinned read views, not copies.
- `merge(key, value, version)` is the **leaderless-replication** primitive (ADR
  0010): per-key LWW that applies iff `version` is newer for *that key*,
  bypassing the engine-wide monotonic floor `put` enforces (so a repair can
  re-apply a value at its original, below-floor version). Idempotent and
  commutative ⇒ convergence regardless of delivery order. `merge_tombstone(key,
  version)` is its delete counterpart: same per-key LWW, applying a tombstone.
  `entries()` returns the full *live* digest; `entries_with_tombstones()`
  returns each key's latest record including tombstones (`(key, Option<value>,
  version)`), which anti-entropy uses so deletes propagate too. `put` keeps its
  global contract for single-writer callers (control plane, dynamo adapter).
- Per key, history is `version -> Some(value) | None`; `None` is a tombstone, so
  `delete`/`delete_range` preserve older versions for `get_at`.
- **`LsmEngine` is the simulation-testable on-disk engine.**
  `LsmEngine::open(env, prefix)` (or `open_with(.., LsmOptions)`) opens at a
  filename `prefix` over the node-scoped `Env` disk. Files: `MANIFEST` (durable
  source of truth, swapped **atomically** via `Disk::replace` — the single
  flush/compaction linearization point; encoded with a **compact hand-rolled
  binary codec** — `CMF1` magic + version, big-endian ints + length-prefixed
  byte strings — not JSON; a legacy JSON manifest, which starts with `{`, is
  still decoded for forward-compat — see `encode_manifest`/`decode_manifest` in
  `lsm.rs`; the manifest also records the **live WAL segment numbers**, format v2),
  `wal-NNNNNN` (the WAL split into **rotating numbered segments** — each write
  `append`+`sync`ed *before* it returns, so an ack means durable; mirrors the Raft
  WAL pattern), and `sst-NNNNNN` (immutable, sorted, **per-block CRC32** via `crc32fast`, with
  an in-file block index + footer, plus a per-table **Bloom filter** in the
  manifest; point reads fetch one block with `read_at`, never the whole file).
  Each data block is **LZ4-compressed** (`lz4_flex`, pure-Rust/MIT, safe-only
  build) when that is smaller, else stored verbatim — framed `tag(u8) || payload
  || crc`, the CRC covering `tag || payload`. The table **format version** (v2 =
  compression-capable; v1 = legacy uncompressed) lives in `SsTableMeta::format`,
  and `read_block` decodes either, so old tables still read.
  Writes go to the WAL then the in-memory memtable (`BTreeMap` MVCC, same shape as
  `MemoryEngine`); a size threshold flushes the memtable to an SSTable, then swaps
  the manifest (recording the surviving WAL segments) and `remove`s the WAL
  segments the flush fully covered. **Leveled compaction** (`lsm.rs`):
  tables carry a `level`; L0 is the overlapping flush tier, L1+ hold
  non-overlapping runs (re-partitioned on key boundaries to ≈`target_table_bytes`),
  so read amplification is bounded by level count. L0→L1 fires at
  `compaction_trigger`; deeper levels cascade over a fanout-scaled budget. Reads
  merge the memtable (newest) with SSTables newest→oldest by version, matching
  `MemoryEngine` exactly (a differential proptest in `lsm_semantics.rs` pins this).
- **Two read gates skip an SSTable before any disk read** (`sstable.rs`
  `SsTableMeta::may_contain`): the key range `[min_key, max_key]`, then the
  per-table **Bloom filter** (`lsm/bloom.rs` — a hand-rolled FNV-1a
  double-hashing bit vector, deterministic, no external dep). A legacy table from
  a pre-Bloom manifest (`has_bloom == false`) is range-gated only, so an upgrade
  stays correct. Built over the table's distinct keys on flush/compaction.
- **The `std::sync::Mutex` guard is never held across an `.await`** in
  `LsmEngine`: every op does its disk I/O (await) lock-free — snapshotting the
  cheap `SsTableReader` clones (metadata + block index, no block bytes) under a
  brief lock first — then takes the lock again only to mutate in-memory state.
  This keeps futures `Send` and ordering deterministic (ADR 0003). Block bytes
  are read from disk outside any lock.
- **WAL segment rotation** (`lsm/wal.rs`): the WAL is a sequence of numbered
  segment files `wal-NNNNNN`, not one growing file. The `GroupCommit` leader
  appends each batch to the **active** segment and rolls to a fresh one past
  `LsmOptions::wal_segment_bytes`, sealing the old segment with the highest
  `wal_seq` it holds. A flush computes the segments fully covered by its watermark
  (`segments_covered_by`), records the **survivors** in the manifest *before* the
  swap, then `remove`s the covered files and `forget_segments`. The old
  single-file truncation path (`begin_truncate`/`finish_truncate`/WAL `replace`) is
  gone. Recovery (`discover_wal_segments` + replay in `open_with`) replays the
  manifest's live segments plus any contiguous segment files present beyond the
  highest recorded one (acked writes since the last flush, or a crash mid-GC), so
  it reconstructs the memtable exactly as the single-file replay did; a directory
  with no recorded segments falls back to replaying a legacy single-file
  `<prefix>wal` (upgrade path). The seq space is monotonic for the engine's life
  (rotation/GC never reset it).
- **Crash safety** holds at the manifest swap: a crash mid-flush or
  mid-compaction recovers the last durable manifest + the intact WAL segments — the
  orphan SSTable (un-synced, never manifest-referenced, at a seq beyond the
  manifest's `next_seq`) is ignored; no torn-table read is possible because a table
  is only read once a *synced* manifest names it. WAL GC is safe at the same swap:
  a segment is `remove`d only after a manifest not naming it is durable, so a crash
  mid-GC recovers a manifest that still lists it (intact) or an orphan covered
  segment *below* the live set (ignored — its data is in the SSTable). Argued in
  `lsm.rs` module docs, exercised in `lsm_crash.rs` + `lsm_wal_rotation.rs`.

## Tests & benchmark

`cargo test -p animus-storage` (proptest semantics + units). Library unit tests
also cover the new perf formats: `sstable::tests` round-trips a compressible and
an incompressible block (asserting LZ4 shrinks the former and never inflates the
latter), and `manifest_tests` round-trips the binary manifest codec, checks it is
smaller than JSON, and confirms a legacy JSON manifest still decodes.

`LsmEngine` tests run under `SimEnv` via `Simulator` (a dev-dep): `lsm_semantics.rs`
mirrors the `MemoryEngine` units + a differential proptest, plus a Bloom test
asserting a point-miss inside a table's key range reads **zero** blocks; and
`lsm_crash.rs` fault-injects crashes (synced writes survive; a flushed SSTable
survives; mid-flush and mid-compaction crashes lose nothing) and asserts
flush + leveled compaction actually happen (L0 bounded by the trigger, L1+
non-overlapping) — all seed-reproducible. `lsm_wal_rotation.rs` covers segment
rotation specifically: writes spanning multiple segments, a flush removing the
covered segment files, multi-segment recovery restoring all acked data (across
SSTables + live WAL segments), and a crash mid-rotation recovering correctly
(including idempotent re-recovery). `LsmEngine` exposes `#[doc(hidden)]`
`sstable_count`/`flush_count`/`compaction_count`/`block_read_count`/
`reset_block_reads`/`level_table_counts`/`levels_non_overlapping`/
`wal_segment_count`/`wal_segments`/`wal_batch_sync_count`/
`test_write_orphan_sstable` introspection helpers for these tests.

`lsm_concurrent.rs` is a **real multi-threaded** regression (`#[tokio::test(flavor
= "multi_thread")]` over `ProdEnv`, timeout-guarded): the deterministic single-
threaded `SimEnv` cannot exercise a preemptive interleaving, so the WAL group-
commit's liveness under genuine parallelism is covered here. (A writer that
enqueued its record while the leader was mid-`fsync` once parked forever; the
`DurableUpTo` future now resolves as soon as `!flushing` so it re-leads — see
`wal.rs`.) **Lesson:** concurrency primitives need a `ProdEnv` multi-thread test;
the sim proves logic/order, not real-thread races.

`cargo bench -p animus-storage` runs `benches/engine_bench.rs`: a hand-rolled
(no criterion) macro-benchmark over **`ProdEnv`** comparing `LsmEngine` vs
`MemoryEngine` on put/get/scan throughput + latency and reporting flush/
compaction counts. Workload is tunable via `ANIMUS_BENCH_KEYS` /
`ANIMUS_BENCH_GETS` / `ANIMUS_BENCH_VALUE_BYTES` / `ANIMUS_BENCH_SCAN`. The
default run shows the per-put WAL `fsync` is the dominant write cost (group-commit
batching is the next deferred win).
