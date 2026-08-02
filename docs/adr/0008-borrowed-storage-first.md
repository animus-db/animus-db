# ADR 0008 — Borrowed storage engine first, then a custom on-disk LSM

- **Status:** Accepted (custom-engine half now implemented)
- **Date:** 2026-08-01 (revised 2026-08-02)

## Context

A production storage engine (an LSM tree with compaction, bloom filters, a WAL,
and crash recovery) is a multi-year effort and a solved problem with mature Rust
options (RocksDB via `rust-rocksdb`, or the pure-Rust `fjall`). The novel risk
and differentiation of AnimusDB live in the **distributed** layer — quorums,
tablets, placement, consensus — not in local storage.

## Decision

We hide storage behind a `StorageEngine` trait (ADR 0004). The first backing
implementation is a simple, fully deterministic **in-memory `BTreeMap`** engine,
sufficient to exercise the distributed layer under simulation. A real persistent
backend was then borrowed behind the same trait, feature-gated, without touching
the distributed code: the pure-Rust **`fjall` LSM** (`FjallEngine`, feature
`fjall`). It proved the trait was portable to a real on-disk engine, but it could
not be driven by `SimEnv` (it does its own real I/O outside the `Env` seam), so
once the custom `LsmEngine` below landed and was wired into `animusd`, the
borrowed `FjallEngine` and the `fjall` dependency were **removed** — the project
relies only on its own engines now.

The custom engine, originally deferred "possibly indefinitely", is now **built**:
`LsmEngine<E: Env>` is a real on-disk log-structured merge tree implemented
against the `StorageEngine` trait, doing **all** of its I/O through the `Env`
`Disk` seam (`append`/`sync`/`read`/`read_at`/`size`/`remove`/`replace`). Because
it touches the disk only through that seam, it is **deterministically
crash-testable under `SimEnv`** — the differentiator a borrowed engine cannot
offer, since RocksDB/`fjall` do their own nondeterministic real I/O outside the
seam and so cannot be driven by the simulator.

`LsmEngine` is a textbook LSM, correctness-first:

- a **memtable** (`BTreeMap` MVCC store, identical shape to `MemoryEngine`);
- a **write-ahead log** split into **rotating numbered segments**
  (`<prefix>wal-NNNNNN`): each mutation is appended + `sync`ed before the call
  returns, so an ack means durable (mirroring the control-plane Raft WAL pattern,
  ADR 0009). The group-commit coordinator appends to the active segment and rolls
  to a fresh segment once it passes `wal_segment_bytes`; a flush then `remove`s the
  segments it fully covers (their records all folded into the new SSTable) — so WAL
  size is bounded and a flush never rewrites a growing single file. The live
  segment set is recorded in the MANIFEST so recovery knows which segments to
  replay;
- immutable, sorted, checksummed **SSTables** (`<prefix>sst-NNNNNN`) with a block
  layout, an in-file block index, a footer, and a per-table **Bloom filter** over
  the table's keys — point reads fetch one block via `read_at`, never the whole
  file (`crc32fast` per block), and skip a table entirely when its Bloom proves
  the key absent (tighter than the key-range gate). Each data block is
  **LZ4-compressed** (pure-Rust `lz4_flex`) when that shrinks it, stored verbatim
  otherwise; the CRC covers the framed `tag || payload`. The table format is
  versioned so legacy uncompressed tables still read;
- a **MANIFEST** (`<prefix>MANIFEST`): the durable source of truth listing live
  SSTables + metadata (including each table's LSM level and Bloom filter) **and the
  live WAL segment numbers**, encoded with a **compact hand-rolled binary codec**
  (manifest format v2 adds the segment list; v1 binary and legacy JSON manifests
  are still read for forward-compat) and written **atomically** via `Disk::replace`,
  the single linearization point for flush and compaction;
- **leveled compaction**: tables carry a level; **L0** is the (overlapping) flush
  tier, **L1+** hold non-overlapping runs (re-partitioned on a key boundary to
  ≈`target_table_bytes`), so read amplification is bounded by the number of levels
  rather than the total table count. L0→L1 fires at `compaction_trigger`; deeper
  levels cascade when over a fanout-scaled table budget. Every distinct
  `(key, version)` record above the GC floor is preserved across a compaction,
  keeping the retained-window view observationally identical to `MemoryEngine`;
- **tombstone GC** (during compaction): a tombstone (and the older versions it
  shadows) is reclaimed once it has aged below the **GC floor** =
  `max_version - LsmOptions::tombstone_grace_versions` **and** no deeper,
  uncompacted level could still hold an older value for the key (which would
  otherwise resurface). Below the floor, a key's history is also compacted to its
  floor anchor — pure space reclamation. Nothing in the retained window
  `(gc_floor, max_version]` is touched, so every `get_at` above the floor is
  unchanged and the differential proptest stays green for it. The grace should
  exceed the maximum anti-entropy lag so a delete still propagates before its
  tombstone is reclaimed (ADR 0010);
- **recovery** on open: read the manifest, open the named SSTables, **reclaim
  orphan WAL segments below the live set** (covered files a crash-after-swap-
  before-`remove` leaked — data-safe to delete, since their records are already in
  an SSTable), replay the live WAL segments (in order) into the memtable, restore
  the monotonic floor.

Crash safety is argued at the manifest swap: a crash mid-flush or mid-compaction
(new SSTable written but the manifest not yet swapped) recovers the last durable
manifest plus the intact WAL — no loss, no torn-table read, the orphan file
ignored. WAL segment GC is safe at the same swap: a segment file is `remove`d only
after a manifest that no longer names it is durable, so a crash mid-GC recovers a
manifest that still lists the segment (intact) or an orphan covered segment below
the live set whose records are already in the SSTable (ignored, and now **removed
on the next open** so it does not leak — `remove_orphan_wal_segments`). Recovery
also replays any segment file present beyond the manifest's recorded set — writes
acked after the last flush — so an un-flushed segment is never lost. Tombstone GC
is likewise crash-safe: it happens inside a compaction, whose merged output is an
orphan until the single manifest swap, so a crash mid-GC recovers the pre-GC
inputs (still named + intact). These are tested under fault injection in
`animus-storage/tests/lsm_crash.rs`, `animus-storage/tests/lsm_wal_rotation.rs`
(including orphan-segment cleanup), and `animus-storage/tests/lsm_gc.rs`.

## Consequences

- We can build and test the entire distributed stack against an in-memory engine
  that is trivially deterministic — ideal for simulation testing (ADR 0003).
- The `StorageEngine` trait must be driven by what the distributed layer needs
  (snapshots, MVCC, range delete, atomic batches), not by what any one engine
  happens to offer, so the trait stays portable across backends.
- The custom `LsmEngine` lands as a purely additive change behind the existing
  trait boundary, exactly as this ADR anticipated — no rewrite of the distributed
  code, which remains generic over `StorageEngine`.
- `LsmEngine` provides on-disk durability that is **simulation-testable**: unlike
  the borrowed `fjall`, its crash-recovery story is exercised deterministically
  under `SimEnv`, closing the gap where durability could previously only be
  asserted at the in-memory simulation layer.
- **The runnable node (`animusd`) now backs its data-plane replicas with
  `LsmEngine` over `ProdEnv` by default**, so the data plane is durable
  end-to-end — a value acked to a client survives a process restart (the engine
  recovers from its on-disk WAL/SSTables/manifest on reopen), matching the
  control plane, which already persists its Raft WAL. The volatile `MemoryEngine`
  remains the simulator's engine and is selectable for ephemeral runs via
  `animusd --ephemeral`. The data role's `ProdEnv` dir is dedicated to the engine,
  so its files use a flat filename prefix (`db-…`), not a subdirectory (`ProdEnv`
  opens files without creating intermediate directories — though `ProdEnv` now
  `create_dir_all`s a file's parent on `append`/`replace`, so a slash-bearing
  prefix would also work). End-to-end durability across a real restart is
  asserted in `animusd/tests/durable_restart.rs`, which restarts the node in the
  same runtime via `Node::shutdown()` (a clean teardown → rebind → recover cycle).
- **An ack must mean the write durably applied.** The data replica
  (`animus-data`'s `serve_replica`) now propagates the storage result into its
  reply: a `WriteAck`/`DeleteAck` is `ok: true` only when
  `StorageEngine::merge`/`merge_tombstone` returned `Ok` (a superseded no-op,
  `Ok(false)`, still counts — the durable state already reflects a newer write),
  and `ok: false` on a storage `Err`. Previously the replica swallowed the result
  (`let _ = storage.merge(..)`) and always acked `true`, so with the durable LSM
  a write that failed to persist would still be counted toward the W quorum and
  falsely reported as committed. The coordinator only counts `ok` acks, so a
  quorum write/delete now fails rather than lying when fewer than W replicas
  could persist (asserted under `SimEnv` with a failing-engine test double in
  `animus-data/tests/ack_durability.rs`).
- **Done since (performance work that does not touch the trait or correctness):**
  per-SSTable **Bloom filters** (hand-rolled FNV-1a double-hashing bit vector,
  persisted in the manifest; a point-miss inside a table's key range now reads
  zero blocks — asserted via a block-read introspection counter in
  `lsm_semantics.rs`) and **leveled compaction** (L0 overlapping flush tier; L1+
  non-overlapping runs; crash-safe at the same single manifest swap; the
  differential proptest and all crash tests stay green). A **benchmark harness**
  (`benches/engine_bench.rs`, `cargo bench -p animus-storage`) measures
  put/get/scan throughput + latency and flush/compaction cost of `LsmEngine` over
  `ProdEnv` against `MemoryEngine` — no new runtime dependency (hand-rolled
  timing; `tokio` is a dev-dependency only).
- **SSTable block compression** (`lsm/sstable.rs`): each data block is now framed
  `tag(u8) || payload || crc`, where the payload is **LZ4-compressed** (via
  `lz4_flex`, a pure-Rust, MIT-licensed compressor built in its safe-only mode,
  so it honours `unsafe_code = "forbid"`) when that is strictly smaller, and
  stored verbatim otherwise — so an incompressible block is never inflated. The
  CRC32 covers `tag || payload`, so it validates the tag and the (possibly
  compressed) bytes; the in-file block index + footer geometry is unchanged. The
  table **format version** is bumped to `2` (footer magic `MAGIC_V2`, mirrored
  into `SsTableMeta::format`); a `read_block` decodes v2 or the **legacy v1**
  framing (`record_bytes || crc`, no tag/compression) per `format`, so SSTables
  written by an older engine still read after an upgrade. Round-trips (including
  an incompressible block) are unit-tested in `sstable.rs`.
- **Compact binary MANIFEST** (`lsm.rs`): the manifest is now encoded with a
  hand-rolled, dependency-free binary codec (a `CMF1` magic + 1-byte version,
  then big-endian fixed ints and `u32`-length-prefixed byte strings for each
  table's `SsTableMeta`) instead of JSON — smaller and cheaper to parse, written
  on every flush/compaction. It remains **crash-safe**: still written atomically
  via `Disk::replace` (the single linearization point), so a crash sees the whole
  old or whole new manifest. Reading is **forward-compatible**: a legacy JSON
  manifest (which begins with `{`, never the `CMF1` magic) is detected and
  decoded via `serde_json`, so an existing on-disk directory still opens. The
  codec is round-trip + legacy-JSON-decode + size unit-tested in `lsm.rs`; all
  crash tests (which reopen through the binary decoder) stay green.
- **WAL segment rotation** (`lsm/wal.rs`, `lsm.rs`): the WAL is now a sequence of
  numbered segment files (`<prefix>wal-NNNNNN`) instead of one growing file. The
  group-commit leader appends each batch to the active segment and rolls to a fresh
  one past `wal_segment_bytes`, sealing the old segment with the highest `wal_seq`
  it holds. On a flush, segments fully covered by the flush watermark are `remove`d
  (rather than the whole WAL rewritten via `replace`), bounding WAL size; the
  surviving segment set is recorded in the manifest (format v2) before the swap, so
  GC is crash-safe at the same single linearization point. Recovery replays the
  manifest's live segments plus any present-on-disk segments beyond them (writes
  acked since the last flush), reconstructing the memtable exactly as the old
  single-file replay did; a legacy single-file `<prefix>wal` is still replayed when
  no segments are recorded (upgrade path). The manifest's old single-file
  truncation machinery (`begin_truncate`/`finish_truncate`/WAL `replace`) is gone.
  Covered by the differential proptest + all crash tests (now multi-segment) and a
  new `lsm_wal_rotation.rs` (rotation, covered-segment GC, multi-segment recovery,
  crash mid-rotation). The group-commit liveness invariant (no mutex guard across
  `.await`, `DurableUpTo` re-leads) is unchanged and still covered by
  `lsm_concurrent.rs`.
- **Tombstone GC + orphan WAL cleanup** (`lsm.rs`): the two tail items of the
  LSM are done. (1) **Tombstone GC during compaction**: `run_compaction` now
  reclaims a tombstone — and the versions it shadows — once it sits at/below the
  **GC floor** (`max_version - LsmOptions::tombstone_grace_versions`) and no
  deeper, uncompacted level overlaps the key (the deeper-level guard prevents
  resurrecting an older value). `gc_obsolete_records` does this per key over the
  merged record stream; versions in the retained window `(gc_floor, max_version]`
  are untouched, so reads above the floor are unchanged and the differential
  proptest stays green for that window (it now asserts the only digest difference
  is below-floor reclaimed tombstones). New `lsm_gc.rs` asserts an aged tombstone
  is physically gone (key + shadowed value) while a within-grace one is preserved,
  and that GC never resurrects a key with a deeper old value. (2) **Orphan WAL
  cleanup**: `open` calls `remove_orphan_wal_segments` to delete WAL segment files
  below the live set — the covered files a crash-after-manifest-swap-before-
  `remove` can leak (recovery already ignored them; now it also reclaims them).
  Tested in `lsm_wal_rotation.rs`. Both are crash-safe at the existing single
  manifest swap; no trait change, no new dependency.
- **Still deferred within `LsmEngine`** (correctness-first, performance later):
  none of the remaining ideas affect the trait or correctness.
