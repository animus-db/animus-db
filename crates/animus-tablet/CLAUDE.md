# CLAUDE.md — animus-tablet

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The tablet model: the unit of placement and migration (ADR 0002). Shared types
used by both planes; no behavior of its own beyond range/epoch helpers.

## Entry points

- `TabletId`, `Epoch` (`INITIAL`, `next()`), `KeyRange`, `Tablet`.
- `KeyRange`: `whole()`, `contains`, `split_at` (strictly-inside split into two
  half-open ranges), `abuts` (contiguity test — the primitives behind split/merge).
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

## Tests

`cargo test -p animus-tablet` — inline unit tests for `contains`, `split_at`
bounds, and `abuts`.
