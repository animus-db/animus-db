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
- `fjall_engine.rs` — `FjallEngine`: persistent LSM behind feature `fjall`.

## What's non-obvious

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
- `FjallEngine` layers MVCC over a plain ordered KV store: physical key =
  `escape(user_key) || (u64::MAX - version)`. `escape` (0x00→0x00 0x01, 0x00
  0x00 terminator) is **order-preserving and prefix-free** so a key's versions
  are contiguous and no key prefixes another; the inverted suffix sorts newest
  first. The monotonic floor is persisted to a `meta` partition (survives
  reopen). The same escape scheme is mirrored in `custos-dynamo`.
- The distributed layer must never depend on `fjall` — it's an additive,
  feature-gated backend, off by default.

## Tests

`cargo test -p custos-storage` (proptest semantics + units). The fjall backend:
`cargo test -p custos-storage --features fjall`. CI lints `--all-features`, so
keep the fjall path clippy-clean too.
