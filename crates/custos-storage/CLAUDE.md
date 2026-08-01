# CLAUDE.md — custos-storage

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
  flush/compaction linearization point), `wal` (each write `append`+`sync`ed
  *before* it returns, so an ack means durable; mirrors the Raft WAL pattern),
  and `sst-NNNNNN` (immutable, sorted, **per-block CRC32** via `crc32fast`, with
  an in-file block index + footer; point reads fetch one block with `read_at`,
  never the whole file). Writes go to the WAL then the in-memory memtable
  (`BTreeMap` MVCC, same shape as `MemoryEngine`); a size threshold flushes the
  memtable to an SSTable, then swaps the manifest and starts a fresh WAL.
  Size-tiered compaction merges accumulated SSTables. Reads merge the memtable
  (newest) with SSTables newest→oldest by version, matching `MemoryEngine`
  exactly (a differential proptest in `lsm_semantics.rs` pins this).
- **The `std::sync::Mutex` guard is never held across an `.await`** in
  `LsmEngine`: every op does its disk I/O (await) lock-free — snapshotting the
  cheap `SsTableReader` clones (metadata + block index, no block bytes) under a
  brief lock first — then takes the lock again only to mutate in-memory state.
  This keeps futures `Send` and ordering deterministic (ADR 0003). Block bytes
  are read from disk outside any lock.
- **Crash safety** holds at the manifest swap: a crash mid-flush or
  mid-compaction recovers the last durable manifest + the intact WAL — the orphan
  SSTable (un-synced, never manifest-referenced, at a seq beyond the manifest's
  `next_seq`) is ignored; no torn-table read is possible because a table is only
  read once a *synced* manifest names it. Argued in `lsm.rs` module docs,
  exercised in `lsm_crash.rs`.

## Tests

`cargo test -p custos-storage` (proptest semantics + units).

`LsmEngine` tests run under `SimEnv` via `Simulator` (a dev-dep): `lsm_semantics.rs`
mirrors the `MemoryEngine` units + a differential proptest, and `lsm_crash.rs`
fault-injects crashes (synced writes survive; a flushed SSTable survives; mid-flush
and mid-compaction crashes lose nothing) and asserts flush+compaction actually
happen — all seed-reproducible. `LsmEngine` exposes `#[doc(hidden)]`
`sstable_count`/`flush_count`/`compaction_count`/`test_write_orphan_sstable`
introspection helpers for these tests.
