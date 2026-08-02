# ADR 0008 — Borrowed storage engine first, then a custom on-disk LSM

- **Status:** Accepted (custom-engine half now implemented)
- **Date:** 2026-08-01 (revised 2026-08-01)

## Context

A production storage engine (an LSM tree with compaction, bloom filters, a WAL,
and crash recovery) is a multi-year effort and a solved problem with mature Rust
options (RocksDB via `rust-rocksdb`, or the pure-Rust `fjall`). The novel risk
and differentiation of CustosDB live in the **distributed** layer — quorums,
tablets, placement, consensus — not in local storage.

## Decision

We hide storage behind a `StorageEngine` trait (ADR 0004). The first backing
implementation is a simple, fully deterministic **in-memory `BTreeMap`** engine,
sufficient to exercise the distributed layer under simulation. A real persistent
backend was then borrowed behind the same trait, feature-gated, without touching
the distributed code: the pure-Rust **`fjall` LSM** (`FjallEngine`, feature
`fjall`). It proved the trait was portable to a real on-disk engine, but it could
not be driven by `SimEnv` (it does its own real I/O outside the `Env` seam), so
once the custom `LsmEngine` below landed and was wired into `custosd`, the
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
- a **write-ahead log** (`<prefix>wal`): each mutation is appended + `sync`ed
  before the call returns, so an ack means durable (mirroring the control-plane
  Raft WAL pattern, ADR 0009);
- immutable, sorted, checksummed **SSTables** (`<prefix>sst-NNNNNN`) with a block
  layout, an in-file block index, a footer, and a per-table **Bloom filter** over
  the table's keys — point reads fetch one block via `read_at`, never the whole
  file (`crc32fast` per block), and skip a table entirely when its Bloom proves
  the key absent (tighter than the key-range gate);
- a **MANIFEST** (`<prefix>MANIFEST`): the durable source of truth listing live
  SSTables + metadata (including each table's LSM level and Bloom filter), written
  **atomically** via `Disk::replace`, the single linearization point for flush and
  compaction;
- **leveled compaction**: tables carry a level; **L0** is the (overlapping) flush
  tier, **L1+** hold non-overlapping runs (re-partitioned on a key boundary to
  ≈`target_table_bytes`), so read amplification is bounded by the number of levels
  rather than the total table count. L0→L1 fires at `compaction_trigger`; deeper
  levels cascade when over a fanout-scaled table budget. Every distinct
  `(key, version)` record is preserved across a compaction, keeping the merged
  view observationally identical to `MemoryEngine`;
- **recovery** on open: read the manifest, open the named SSTables, replay the
  WAL into the memtable, restore the monotonic floor.

Crash safety is argued at the manifest swap: a crash mid-flush or mid-compaction
(new SSTable written but the manifest not yet swapped) recovers the last durable
manifest plus the intact WAL — no loss, no torn-table read, the orphan file
ignored. This is tested under fault injection in `custos-storage/tests/lsm_crash.rs`.

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
- **The runnable node (`custosd`) now backs its data-plane replicas with
  `LsmEngine` over `ProdEnv` by default**, so the data plane is durable
  end-to-end — a value acked to a client survives a process restart (the engine
  recovers from its on-disk WAL/SSTables/manifest on reopen), matching the
  control plane, which already persists its Raft WAL. The volatile `MemoryEngine`
  remains the simulator's engine and is selectable for ephemeral runs via
  `custosd --ephemeral`. The data role's `ProdEnv` dir is dedicated to the engine,
  so its files use a flat filename prefix (`db-…`), not a subdirectory (`ProdEnv`
  opens files without creating intermediate directories). End-to-end durability
  across a real restart is asserted in `custosd/tests/durable_restart.rs`.
- **Done since (performance work that does not touch the trait or correctness):**
  per-SSTable **Bloom filters** (hand-rolled FNV-1a double-hashing bit vector,
  persisted in the manifest; a point-miss inside a table's key range now reads
  zero blocks — asserted via a block-read introspection counter in
  `lsm_semantics.rs`) and **leveled compaction** (L0 overlapping flush tier; L1+
  non-overlapping runs; crash-safe at the same single manifest swap; the
  differential proptest and all crash tests stay green). A **benchmark harness**
  (`benches/engine_bench.rs`, `cargo bench -p custos-storage`) measures
  put/get/scan throughput + latency and flush/compaction cost of `LsmEngine` over
  `ProdEnv` against `MemoryEngine` — no new runtime dependency (hand-rolled
  timing; `tokio` is a dev-dependency only).
- **Still deferred within `LsmEngine`** (correctness-first, performance later):
  WAL segment rotation / fsync group-commit batching (the benchmark shows the
  per-put WAL fsync is the dominant write cost), block compression, and a more
  compact binary manifest (JSON today). None affect the trait or correctness.
