# ADR 0008 — Borrowed storage engine first, custom LSM deferred

- **Status:** Accepted
- **Date:** 2026-08-01

## Context

A production storage engine (an LSM tree with compaction, bloom filters, a WAL,
and crash recovery) is a multi-year effort and a solved problem with mature Rust
options (RocksDB via `rust-rocksdb`, or the pure-Rust `fjall`). The novel risk
and differentiation of CustosDB live in the **distributed** layer — quorums,
tablets, placement, consensus — not in local storage.

## Decision

We will hide storage behind a `StorageEngine` trait (ADR 0004) and not write a
storage engine yet. The first backing implementation is a simple, fully
deterministic **in-memory `BTreeMap`** engine, sufficient to exercise the
distributed layer under simulation. A real persistent backend (RocksDB or
`fjall`) can be added later behind the same trait, feature-gated, without
touching the distributed code. A custom LSM engine is explicitly deferred,
possibly indefinitely.

## Consequences

- We can build and test the entire distributed stack against an in-memory engine
  that is trivially deterministic — ideal for simulation testing (ADR 0003).
- The `StorageEngine` trait must be driven by what the distributed layer needs
  (snapshots, MVCC, range delete, atomic batches), not by what any one engine
  happens to offer, so the trait stays portable across backends.
- Until a persistent backend lands, the system is not durable across process
  restarts on real hardware; that is acceptable for the current milestones,
  whose durability is defined and tested at the simulation layer.
- If we ever do need a custom engine, the trait boundary means it is an additive
  change, not a rewrite.
