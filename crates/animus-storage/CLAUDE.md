# CLAUDE.md — animus-storage

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The `StorageEngine` trait and its backing implementations. The trait is driven
by what the distributed layer needs, not by any one engine (ADR 0004, 0008).

## Entry points

- `lib.rs` — `StorageEngine` and `Snapshot` traits; `WriteBatch`, `WriteOp`,
  `MergeOp`, `VersionedValue`, `StorageError`.
- `memory.rs` — `MemoryEngine`: `BTreeMap` MVCC store, deterministic; the engine
  used under simulation.
- `lsm.rs` (+ `lsm/sstable.rs`) — `LsmEngine<E: Env>`: a **real on-disk LSM**
  doing all I/O through the `Env` `Disk` seam, so it is deterministically
  crash-testable under `SimEnv` (ADR 0008). `LsmOptions` tunes the flush
  threshold + compaction trigger.

## What's non-obvious

- **The traits are `async`** (`#[async_trait::async_trait]`). The I/O-ish
  methods — `put`/`merge`/`merge_tombstone`/`delete`/`delete_range`/
  `write_batch`/`get`/`get_at`/`scan`/`scan_at`/`entries`/`entries_at`/
  `entries_with_tombstones` on `StorageEngine`, and `get`/`scan` on
  `Snapshot` — are `async fn`; callers
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
- `merge(key, value, version)` is the **per-key LWW** primitive: it applies iff
  `version` is newer for *that key*, bypassing the engine-wide monotonic floor
  `put` enforces (so a re-apply can land a value at its original, below-floor
  version). Idempotent and commutative ⇒ convergence regardless of delivery
  order. Born for leaderless replication (ADR 0010 — that AP plane is deleted,
  ADR 0019); today its main consumer is the **CP-data Raft apply loop**
  (re-applying a recovered log tail must be idempotent). `merge_tombstone(key,
  version)` is its delete counterpart: same per-key LWW, applying a tombstone.
  `entries()` returns the full *live* digest; `entries_with_tombstones()`
  returns each key's latest record including tombstones (`(key, Option<value>,
  version)`). `put` keeps its global contract for single-writer callers.
- **`scan_at(start, end, version)`/`entries_at(version)`** (ADR 0018 §2/PR2b)
  are `scan`/`entries`'s as-of-a-past-version counterparts — the range/
  whole-keyspace analogues of `get_at`, and the primitive the CP data
  plane's MVCC snapshot reads (`RaftKvNode::read_at`/`scan_at`) are built
  on. Unlike `get_at` (a required method with no useful default) and
  `merge_batch`/`approx_bytes_in_range` (which *do* have a correct, if not
  cheap, default derivable from the rest of the trait), these have **no
  default**: `entries_with_tombstones` only ever exposes each key's
  *latest* record, not enough history to answer "what did this key look
  like as of an earlier version." Both engines already carried the exact
  logic internally (`MemoryEngine`'s `Inner::scan_at` already retains full
  per-key history and already backed `scan`/`snapshot`'s own `Snapshot::
  scan`; `LsmEngine`'s private `scan_at`/`merged_at` already backed `scan`
  at `version = Version::MAX`), so exposing them on the trait was a thin,
  direct addition in each impl, not new logic.
- **`merge_batch(Vec<MergeOp>)` coalesces a *sequential* run of merges into one WAL
  `fsync`.** Each `MergeOp` carries its **own** version (unlike `write_batch`, which
  stamps one version and uses `put`/monotonic-floor semantics) and applies with the
  same per-key LWW rule as `merge`/`merge_tombstone` — the decision considers both
  engine state **and** earlier winners for the same key in the batch, and only the
  winners are logged, so WAL replay reconstructs the memtable byte-identically to a
  run of individual merges (idempotent, crash-safe at the same `fsync` boundary).
  This exists because WAL group commit only amortizes the `fsync` across
  *concurrent* writers; the CP-data Raft apply loop is a single sequential task, so
  it needs an explicit batch API to avoid one `fsync` per applied command (ADR 0008;
  ~9.7x on the apply-path bench). The trait method is **defaulted** (per-op loop) so
  `MemoryEngine` and any other backend need no change; `LsmEngine` overrides it
  (`WalRecord::MergeBatch`). Regression: `lsm_group_commit.rs::merge_batch_coalesces_one_fsync_and_is_durable`.
- **`approx_bytes_in_range(start, end)` (ADR 0034: byte-based auto-split)
  follows the same defaulted-trait-method shape as `merge_batch`**: the
  default implementation is *exact* (scan `[start, end)`, or filter
  `entries()` by `start` when `end` is `None`, and sum `key.len() +
  value.len()`) — correct and cheap enough for `MemoryEngine` (and any
  future engine) with zero code. `LsmEngine` overrides it with a **cheap,
  non-materializing** estimate: an exact range-scoped `BTreeMap::range` sum
  over the memtable, plus every SSTable whose own `[min_key, max_key]`
  overlaps the query range at all contributing its **whole** `file_size`
  (deliberately over-estimating — a table's overlap can include a sibling
  tenant's bytes on a shared engine, ADR 0026/0028, particularly at L0). No
  disk read. `tests/lsm_approx_bytes.rs`: known SSTables at known ranges
  give a sane, never-under, tightly-scoped estimate; a partial overlap
  proves the whole-`file_size` bias directly; the default-trait-impl path is
  proven exact on `MemoryEngine`.
- **`LsmOptions::trust_monotonic_versions`** (opt-in, default `false`) skips
  `merge`/`merge_tombstone`/`merge_batch`'s cross-SSTable `latest_version_of`
  point read — under the CP plane's monotonic Raft-log-index versions that read
  is structurally always a winner, so it's pure overhead. `merge_batch` still
  dedupes multiple ops for the *same* key within one batch (cheap, in-memory —
  the skipped read is only against already-durable engine state). Proven by
  asserting **zero** SSTable block reads for a merge whose key already lives in
  a flushed table (`lsm_merge_fast_path.rs`), not just that the result is
  correct — a correct result alone wouldn't prove the read was skipped.
- **Adding a field to `LsmOptions` is a wide mechanical edit, not a local
  one:** every test/bench constructs it via a full struct literal (no
  `..Default::default()`), so a new field means touching every call site in
  the crate (~20+, across `tests/*.rs` and `benches/*.rs`) — nothing outside
  `animus-storage` uses it yet, so the blast radius stays crate-local. A
  `sed -E 's/^(\s*)tombstone_grace_versions: (.*),$/&\n\1new_field: default,/'`
  over every site (matching indentation via a backreference) is faster and
  less error-prone than hand-editing each one; verify with a build immediately
  after (a missed site is a compile error, not a silent bug — Rust won't let a
  struct literal omit a field).
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
- **Flushes and compactions (maintenance) are mutually exclusive** via a
  hand-rolled async `MaintenanceLock` (`lsm.rs`) whose guard *is* held across the
  whole operation's awaits (it's a waker-based async mutex, not a `std` guard, so
  the futures stay `Send`). Both allocate SSTable seqs from `manifest.next_seq`
  (only advanced at the final swap) and both swap the manifest + readers, so an
  overlap — the admin `flush_now`/`compact_now` racing the write path's
  `maybe_flush_and_compact` from another task — used to allocate **duplicate
  seqs** and clobber manifests/double-GC WAL segments. Writers never take this
  lock (group-commit liveness untouched). Two companion invariants in `flush`:
  the memtable clear is **surgical** (only the exact `(key, version)` slots the
  snapshot folded into the SSTable — a blanket `clear()` erased any write applied
  during the lock-free SSTable build, an acked-write loss once a later flush GC'd
  its WAL segment); and the `applies_in_flight == 0` gate + WAL watermark are
  (re-)checked/sampled **atomically with the snapshot** inside `flush` (the
  callers' decision-time check is only a hint), so a WAL segment is GC'd only
  when every durable record it holds is provably in the new SSTable.
  Regressions: `lsm_concurrent.rs` (`concurrent_writers_with_flushes_…`,
  `forced_flush_under_live_load_…`, `overlapping_flush_now_…` — real
  multi-thread; `SimEnv` disk ops complete without yielding, so a flush runs in
  one poll under sim and this race is *only* reachable under `ProdEnv`).
  - **Reads must be consistent against concurrent flush/compaction** (the lock-free
    window has two races, both fixed; surfaced by bulk-seed → auto-split). (1)
    **Compaction removes files**: it swaps `inner.readers` under the lock then
    `remove`s the superseded files, so a lock-free read of a just-removed file gets
    an empty/short read (`read_at` maps a `NotFound` file to `Ok(empty)`). Reads now
    capture the **compaction generation** (`inner.compactions`) with the readers
    snapshot and **retry** (`raced_compaction` / `READ_COMPACTION_RETRIES`) when a
    compaction raced — re-reading the merged tables (same data) instead of erroring.
    (2) **Flush moves keys memtable→SSTable**: `merged_at` used to read the memtable
    *after* the readers under a separate lock, so a flush in between dropped the
    just-flushed keys from the result (silent loss — a split handoff could seed the
    child short). It now snapshots the memtable range **atomically with the readers**
    (one lock). `read_at`/`latest_version_of` already snapshot the memtable
    atomically; only `merged_at` (scan/`entries`) had the flush gap.
    `tests/lsm_concurrent.rs::scans_survive_concurrent_compaction` is the regression.
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
  *forward*), this also reclaims them so they don't leak. Uses **`Disk::list`**
  (one directory listing, filtered by filename) rather than probing segment
  numbers `0..lowest_live` one `env.size` call at a time — the probe loop cost
  one I/O call per *ever-rotated* segment number on every open, unbounded over
  the engine's lifetime; a listing is one call regardless of history.
- **Each WAL record is framed `len(u32 BE) | crc32(u32 BE) | payload`** (a
  compact hand-rolled binary encoding, replacing an older newline-delimited
  `serde_json` line — a `Vec<u8>` value serialized as a decimal-number JSON
  array is 3-6x bigger than the raw bytes on the fsync-critical path). The CRC
  turns at-rest corruption of a durable record into a loud error instead of
  silently dropping it. **Distinguishing a legitimate crash-torn trailing
  record from real corruption is not a magnitude check on the frame — it's
  positional.** A crash can only ever tear the physical *end* of a file (prior
  synced content is untouched), so `decode_wal`'s rule is: a frame that fails
  to parse (short read *or* CRC mismatch) is tolerated **only if no valid,
  checksummed frame exists anywhere later in the buffer**
  (`wal_resync_point`) — proof nothing recoverable follows, i.e. this really is
  the tail. A bad frame *followed* by more valid frames can only be corruption
  of previously-durable data (a crash cannot reach past the tear point), so
  that's a hard error, never a silent truncation of everything after it. Magnitude-based
  heuristics ("declared length looks implausible") don't work here: both a
  genuinely torn-and-then-bit-flipped tail *and* real mid-file corruption can
  produce an equally implausible declared length — the position (is there
  provably-valid data after the bad frame?) is the only sound signal. On open,
  a torn tail found on the segment that becomes **active** is truncated via
  `Disk::replace` before further appends ride it — left in place, the next
  acked write would concatenate onto the garbage with no frame boundary, and a
  *second* recovery would then lose it (regression:
  `lsm_disk_faults.rs::acked_writes_after_torn_tail_recovery_survive_second_restart`).
  Corruption regression: `lsm_disk_faults.rs::corrupted_durable_wal_record_surfaces_loudly`.
- **A `snapshot()`'s pinned version must floor compaction's tombstone-GC
  window, not just its own read path.** `LsmSnapshot` used to be a bare
  `(engine, version)` pair with no registration anywhere — a long-held snapshot
  could read torn/reclaimed history once a background compaction aged past
  `tombstone_grace_versions`. Fixed with a refcounted watermark
  (`Inner::held_snapshots`, registered on `snapshot()`/every `Clone`, released
  on `Drop`) that caps compaction's `gc_floor` at `min_held_version - 1`,
  sampled under the *same* lock as `max_version` so a concurrently-registered
  snapshot can't be missed. `LsmSnapshot` therefore couldn't stay
  `#[derive(Clone)]` — a hand-written `Clone` re-registers the hold so a clone
  and the original release independently. Regression: `lsm_gc.rs::held_snapshot_survives_compaction_gc`.
- **`env.spawn_task` + this crate's `block_on`-only `SimEnv` test convention
  don't mix — gate genuine backgrounding behind an opt-in flag, don't retrofit
  every test.** Moving `maybe_flush_and_compact` off the write-path ack
  (`LsmOptions::background_maintenance`) needs a task that outlives the
  triggering `put`/`merge` call. But `SimEnv`'s `Spawner::spawn` only
  *registers* a task on the `Simulator`'s ready queue — polling it needs
  `Simulator::run_for`/`run_until_quiescent`, which none of this crate's
  `LsmEngine` tests call (they drive everything with a bare
  `futures::executor::block_on`, deliberately, per the module docs). A
  `block_on`'d write that spawns a background flush would leave that task
  *permanently unpolled* in every such test — not flaky, structurally dead —
  and dozens of tests assert `sstable_count()`/`compaction_count()` right after
  a write loop with no driving step. Retrofitting all of them (or their
  `Simulator` handle, which several helpers discard) to drive the simulator
  would be a sprawling, high-risk rewrite for a change whose only real
  consumer is a different crate's async, already-`Simulator`-driven apply
  loop. Instead the feature is fully implemented and tested, but **opt-in**
  (`LsmOptions::background_maintenance: bool`, default `false` = the old
  fully-synchronous behavior, byte-for-byte what every existing test expects).
  Its own tests (`lsm_maintenance.rs`) follow the pattern real consumers need:
  spawn the *write workload itself* as a task, then
  `Simulator::run_until_quiescent` to drive both the writer and the
  maintenance task it triggers — the same shape `animus-control`'s Raft tests
  already use for spawned protocol loops. **Generalizable check before adding
  `env.spawn_task` to any component this crate's style of test exercises:** does
  the test suite already drive `Simulator::run_for`/`run_until_quiescent`, or
  only `block_on`? A bare `block_on` harness makes newly-spawned background
  work invisible, not merely hard to observe.
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

`cargo test -p animus-storage` (proptest semantics + units). The
`MemoryEngine`-level suites are `tests/storage_basic.rs`/`storage_props.rs`;
`LsmEngine` gets a much larger battery under `SimEnv` via `Simulator` (a
dev-dep) — semantics/differential, crash/fault-injection (including an
opt-in `SimEnv` disk fault model: torn WAL tails, at-rest corruption,
injected I/O errors), WAL rotation, tombstone GC + snapshot pinning, the
`trust_monotonic_versions`/`background_maintenance` opt-ins, and the ADR
0015 metrics counters. `LsmEngine` exposes a `#[doc(hidden)]` introspection
API for these tests, plus a **documented** read-only introspection API for
the admin/debug interface (ADR 0020, consumed by `animusd`) —
`sstable_views()`, `memtable_len`/`memtable_bytes`, `wal_segment_sizes`/
`wal_durable_seq`/`wal_rotation_count`, `wal_segment_records(seg)` — and
the **admin actions** `flush_now`/`compact_now`. `open_with_metrics`/
`with_metrics` is the sim-readable seam every metrics test opens the
engine through (`SimEnv`'s metrics sink, not `ProdEnv`'s).

`lsm_concurrent.rs` is a **real multi-threaded** regression
(`#[tokio::test(flavor = "multi_thread")]` over `ProdEnv`, timeout-guarded):
the deterministic single-threaded `SimEnv` cannot exercise a preemptive
interleaving, so the WAL group-commit's liveness under genuine parallelism,
scans racing concurrent compaction, and flush-concurrency (maintenance
exclusion + surgical clear) are covered only here. **Lesson:** concurrency
primitives — and lock-free reads racing flush/compaction — need a `ProdEnv`
multi-thread test; the sim proves logic/order, not real-thread races.
**Second lesson:** a concurrency test only has teeth against the code paths
its workload actually reaches — drive the *maintenance* machinery (cross
the flush threshold), not just the write path.

`cargo bench -p animus-storage` runs `benches/engine_bench.rs`: a
hand-rolled (no criterion) macro-benchmark over **`ProdEnv`** comparing
`LsmEngine` vs `MemoryEngine` on put/get/scan throughput + latency and
reporting flush/compaction counts. Workload is tunable via
`ANIMUS_BENCH_KEYS`/`ANIMUS_BENCH_GETS`/`ANIMUS_BENCH_VALUE_BYTES`/
`ANIMUS_BENCH_SCAN`/`ANIMUS_BENCH_APPLY_BATCH` (default 30, the
sequential-apply section comparing per-op `merge` against `merge_batch` —
the CP-data Raft apply pattern).
