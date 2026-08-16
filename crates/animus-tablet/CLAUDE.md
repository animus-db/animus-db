# CLAUDE.md — animus-tablet

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The tablet model — the unit of placement and migration (ADR 0002) — **plus the
per-table hash-ring key layout** (ADR 0022/0023): this crate owns the Murmur3
partitioner and the order-preserving key-escape primitives every data-plane key
is built from. Shared types used across the control plane, the CP data plane,
and the wire edges. Mostly one file (`src/lib.rs`), plus `split_basis.rs`
(below).

## Split-inheritance combinator (ADR 0046 principle 3)

- `split_basis::effective<T: Clone>(own: Option<T>, frozen_basis: Option<&T>)
  -> Option<T>` — the one generic form of "a split is a log cut; every
  consumer offset crossing it inherits from a basis frozen at the cut, never
  a live re-derivation from the parent's later state." `own.or_else(||
  frozen_basis.cloned())`, nothing more — the call site still owns what
  "own"/"frozen" mean for its own offset convention. First caller:
  `animus-control::meta::Metadata::effective_stream_shard_watermark`.

## Key layout (ADR 0022/0023)

- `TOKEN_BYTES` + `partition_token(&[u8]) -> [u8; TOKEN_BYTES]` — the
  Cassandra-compatible **Murmur3 x64_128** partition token (helpers
  `murmur3_x64_128`, `fmix64`) that leads every data-plane key.
  **Load-bearing invariant (from the code's own doc):** every node and every
  restart must agree on the token — it is fixed, seedless, no RNG/host state;
  **do not change the function without a data migration**, the bytes are baked
  into stored keys.
- `escape(&[u8]) -> Vec<u8>` — the order-preserving, **prefix-free** escape
  (no key's encoding prefixes another's). It is **deliberately duplicated**
  from the wire adapters (`animus-dynamo`/`animus-cql` carry their own copies)
  and must match them **byte-for-byte** — the duplication keeps this crate
  dependency-light while the adapters stay pure.
- `table_key_block(&str) -> KeyRange` — a table's whole key block
  `[escape(table), block_end)`, used where a table-scoped range is needed.

## Tablets & ranges

- `TabletId`, `Epoch` (`INITIAL`, `next()`), `TableName` (type alias),
  `KeyRange`, `Tablet`.
- `Tablet` is **table-scoped** (ADR 0023): field `table: Option<TableName>`
  (`None` = a legacy whole-keyspace tablet). Constructors `new` /
  `new_for_table` / `with_table` (all normalize — sort/dedup — the replica
  set); predicates `serves_table`, `has_replica`.
- `KeyRange`: `whole()`, `contains`, `contains_range` (subset containment —
  the shared primitive behind the reconciler's narrow-only check and
  `animusd`'s read-path scope pre-checks, ADR 0031/0028), `split_at`
  (strictly-inside split into two half-open ranges), `abuts` (contiguity
  test — originally the adjacency check `MergeTablets` required of its two
  tablets; tablets are split-only now (ADR 0044), so `abuts` has **no
  production caller today**, exercised only by this crate's own unit
  tests).

## What's non-obvious

- `KeyRange` is half-open `[start, end)`; `end == None` means unbounded above
  (`whole()` is `start = []`, `end = None`). `abuts` is false for an
  unbounded-above range (nothing follows it).
- `Epoch` is the **data-plane fencing token**: every placement change bumps it.
  The actual split *state transitions* live in `animus-control`'s
  `Metadata::apply`; this crate provides the range primitives. (Tablets are
  split-only, ADR 0044 — there is no merge state transition anymore.)
- **`Tablet::version_floor` (the cross-group LWW version-floor fix) is
  retired (ADR 0018 §2 amendment, PR2)**, replaced by HLC witnessing plus a
  range seal in `animus-cp-data` — see that crate's `CLAUDE.md` and
  `docs/engineering-lessons.md` for the design and the full writeup of the
  hazard it used to close. `Tablet` no longer carries this field. The split
  provenance the seal design's reconciler gating needs (`split_parents`)
  lives entirely in `animus-control`'s `Metadata`, not on `Tablet` itself —
  this crate has nothing to say about it.
- Serializable (`serde`) because tablets travel inside control-plane Raft log
  entries and data-plane routing views.
- Dependency direction: `animus-control`, `animus-cql`, `animus-cp-data`, and
  `animusd` all depend on this crate — never the reverse. That reverse-dep ban
  is exactly why `escape`/`TableName` are duplicated here rather than imported.

## Tests

`cargo test -p animus-tablet` — inline unit tests for `contains`/
`contains_range`, `split_at` bounds, `abuts`, token determinism + fixed width,
the Murmur3 empty-input spec anchor, token spread across ring octants,
table-scoped `serves_table`, and `table_key_block` isolation.
