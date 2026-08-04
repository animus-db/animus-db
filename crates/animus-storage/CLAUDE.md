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
  || crc`, the CRC covering `tag || payload`. Records inside a block use
  **shared-prefix key encoding** (v3): each record stores `shared(u32)` (leading
  bytes its key shares with the previous record's key in the block) + only its
  differing suffix; the block's first record stores its full key. Since records
  are sorted by key, adjacent keys share long prefixes (every key in a table
  shares the `escape(table) || …` prefix), so this shrinks the key bytes *before*
  LZ4 and shrinks the decoded footprint. There is a **single on-disk format**
  (pre-alpha, no older tables exist — ADR 0008); `SsTableMeta::format` is kept as a
  per-table version tag for operator introspection (`/admin/storage/lsm`) and a
  future-evolution hook, not a read-time switch. (Restart-point in-block
  binary-search seek — the other half of LevelDB's block format — is a deliberate
  follow-up: the reader decodes whole blocks, gated by the block index + Bloom, so
  restart points would be unused machinery today.)
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
- **Tombstone GC happens during compaction** (`gc_obsolete_records` in `lsm.rs`):
  a tombstone (and the versions it shadows) is reclaimed once it is at/below the
  **GC floor** = `max_version - LsmOptions::tombstone_grace_versions` AND no
  deeper, uncompacted level overlaps the key (the deeper-level guard — else an
  older value would resurface). Nothing in the retained window
  `(gc_floor, max_version]` is touched, so every `get_at` above the floor is
  unchanged; the differential proptest stays green for that window and now asserts
  the only `entries_with_tombstones` difference vs `MemoryEngine` is below-floor
  reclaimed tombstones (`MemoryEngine` never GCs). Set the grace **above the max
  anti-entropy lag** so a delete propagates before its tombstone is reclaimed (ADR
  0010). `lsm_gc.rs` is the dedicated test.
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
  (rotation/GC never reset it). **Orphan WAL segments below the live set** (covered
  files a crash-after-manifest-swap-before-`remove` leaked) are deleted on open by
  `remove_orphan_wal_segments` — recovery already ignored them (it only probes
  *forward*), this also reclaims them so they don't leak.
- **Crash safety** holds at the manifest swap: a crash mid-flush or
  mid-compaction recovers the last durable manifest + the intact WAL segments — the
  orphan SSTable (un-synced, never manifest-referenced, at a seq beyond the
  manifest's `next_seq`) is ignored; no torn-table read is possible because a table
  is only read once a *synced* manifest names it. WAL GC is safe at the same swap:
  a segment is `remove`d only after a manifest not naming it is durable, so a crash
  mid-GC recovers a manifest that still lists it (intact) or an orphan covered
  segment *below* the live set (ignored — its data is in the SSTable — and removed
  on the next open). **Tombstone GC is crash-safe at the same swap**: it runs
  inside a compaction whose merged output is an orphan until the manifest swap, so
  a crash mid-GC recovers the pre-GC inputs. Argued in `lsm.rs` module docs,
  exercised in `lsm_crash.rs` + `lsm_wal_rotation.rs` + `lsm_gc.rs`.

- **Observability (ADR 0015) is observe-only and deterministic.** `LsmEngine`
  records `storage_*` counters through the `Env` metrics seam at the real LSM site
  that knows the outcome: a flush *after* its manifest swap (`storage_flushes`); a
  compaction *after* its swap (`storage_compactions` + `_tables_merged` +
  `_bytes_merged` from the consumed inputs' `file_size`, + `_tombstones_reclaimed`
  from the GC record-count drop); a block fetched in `SsTableReader::read_block`
  (`storage_sstable_block_reads`); the per-table Bloom verdict at the point-read gate
  in `may_contain_observed` (`storage_bloom_hits`/`_misses` — a miss is counted
  *before* any block read, so a proven-absent in-range key reads zero blocks); and a
  WAL rotation at the group-commit site (`storage_wal_segment_rotations`, via the
  coordinator's monotonic `rotation_count` whose delta `log_and_apply` records around
  each `commit`). The handle defaults to `env.metrics()` (no-op under `SimEnv`); a
  sim test reads counters back via the additive `LsmEngine::open_with_metrics`
  (`SsTableReader::with_metrics` carries it to the readers). Counters only — never
  read the wall clock; recording changes no engine behavior or signatures.

## Tests & benchmark

`cargo test -p animus-storage` (proptest semantics + units). Library unit tests
also cover the perf formats: `sstable::tests` round-trips a compressible and
an incompressible block (asserting LZ4 shrinks the former and never inflates the
latter), round-trips the **shared-prefix codec** across every prefix relation +
rejects a malformed `shared` length, and asserts shared-prefix encoding is far
smaller than naive full-key encoding (isolated from LZ4 by comparing the raw
buffer against the arithmetic full-key cost); and `manifest_tests` round-trips the binary manifest codec,
checks it is smaller than JSON, and confirms a legacy JSON manifest still decodes.

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
`test_write_orphan_sstable`/`test_write_orphan_wal_segment`/`test_disk_versions_of`
introspection helpers for these tests. It also exposes a **documented** read-only
introspection API for the admin/debug interface (ADR 0020, consumed by `animusd`):
`sstable_views()` (a lean `SsTableView` per live table — key/version range, size,
level, bloom), `memtable_len`/`memtable_bytes`, `wal_segment_sizes`/
`wal_durable_seq`/`wal_rotation_count`, and `wal_segment_records(seg)` (decoded
`WalRecordView`s via the existing `decode_wal`); plus the **admin actions**
`flush_now`/`compact_now` (force a flush+compaction / a compaction pass — `flush_now`
keeps the `applies_in_flight == 0` WAL-GC invariant). Pure reads or explicit
actions; they record nothing and change no engine behavior. `lsm_gc.rs` covers tombstone GC: an aged
tombstone (and its shadowed value) is physically reclaimed while a within-grace
tombstone is preserved, and GC never resurrects a key with a deeper old value.

`lsm_metrics.rs` covers the ADR 0015 storage counters: a write workload forces
several flushes, an L0→L1 compaction (asserting tables/bytes merged), WAL segment
rotations, and on-disk point reads (block reads + Bloom hits); a proven-absent
in-range key is a Bloom *miss* that reads **zero** blocks; an aged tombstone is
counted reclaimed; and the recorded snapshot is asserted byte-identical across two
runs of the same seed (determinism). All under `SimEnv` via `open_with_metrics`.

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
