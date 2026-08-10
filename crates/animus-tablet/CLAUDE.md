# CLAUDE.md — animus-tablet

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The tablet model: the unit of placement and migration (ADR 0002). Shared types
used by both planes; no behavior of its own beyond range/epoch helpers.

## Entry points

- `TabletId`, `Epoch` (`INITIAL`, `next()`), `KeyRange`, `Tablet` (incl.
  `version_floor: u64` — the cross-group MVCC version-floor fix, see below).
- `KeyRange`: `whole()`, `contains`, `contains_range` (subset containment —
  the shared primitive behind the reconciler's narrow-only/widen-only checks
  and `animusd`'s read-path scope pre-checks, ADR 0031/0033), `split_at`
  (strictly-inside split into two half-open ranges), `abuts` (contiguity test
  — the primitives behind split/merge).
- `Tablet::new` normalizes (sorts/dedups) the replica set.

## What's non-obvious

- `KeyRange` is half-open `[start, end)`; `end == None` means unbounded above
  (`whole()` is `start = []`, `end = None`). `abuts` is false for an
  unbounded-above range (nothing follows it).
- `Epoch` is the **data-plane fencing token**: every placement change bumps it.
  The actual split/merge *state transitions* live in `animus-control`'s
  `Metadata::apply`; this crate only provides the range primitives.
- Serializable (`serde`) because tablets travel inside control-plane Raft log
  entries and data-plane routing views.
- **`Tablet::version_floor` (cross-group LWW version-floor fix, confirmed
  real — root `CLAUDE.md` has the full writeup) closes a hazard where a
  fresh/widened `animus-cp-data` group's own local Raft log index (its MVCC
  version) could collide with a version a *different* group already stamped
  for the same key on the node-shared `StorageEngine`.** `0` by default
  (`#[serde(default)]`, and every existing `Tablet::new`/`new_for_table`/
  `with_table` constructor) — byte-identical to using the raw log index,
  so a tablet that has never been split/merged is completely unaffected.
  Only `animus-control`'s `SplitTablet`/`MergeTablets` apply ever set it to
  something else (`source.version_floor + 1` for a fresh sibling,
  `max(left, right) + 1` for a merge survivor) — a pure function of
  already-replicated state, computed once by the control plane's own
  deterministic apply, so every data replica reads the identical value.
  `animus-cp-data::RaftKvNode` is what actually consumes it
  (`start_hosted_with_floor`/`bump_version_floor`/`effective_version`); this
  crate just carries the field.

## Tests

`cargo test -p animus-tablet` — inline unit tests for `contains`, `split_at`
bounds, and `abuts`.
