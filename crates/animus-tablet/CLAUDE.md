# CLAUDE.md — animus-tablet

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The tablet model — the unit of placement and migration (ADR 0002) — **plus the
per-table hash-ring key layout** (ADR 0022/0023): this crate owns the Murmur3
partitioner and the order-preserving key-escape primitives every data-plane key
is built from. Shared types used across the control plane, the CP data plane,
and the wire edges. One file (`src/lib.rs`) — the `split_basis` module
(ADR 0046 principle 3's frozen-basis combinator) was deleted in the ADR
0050 Train B rung-7 sweep: copy-based split children are born with empty
logs, so no consumer offset crosses a split at all (the strictly stronger
successor invariant).

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

## Tablets & ranges

- `TabletId`, `Epoch` (`INITIAL`, `next()`), `TableName` (type alias),
  `KeyRange`, `Tablet`.
- `Tablet` is **table-scoped** (ADR 0023): field `table: Option<TableName>`
  (`None` = a legacy whole-keyspace tablet). Constructors `new` /
  `new_for_table` / `with_table` (all normalize — sort/dedup — the replica
  set); predicates `serves_table`, `has_replica`, `is_routable`.
- `Tablet.state: TabletState {Active, Building, Splitting}` (ADR 0050 Train
  B rung 3, serde-default `Active`) — the copy-based split's lifecycle. A
  `Building` split child's range **overlaps its still-serving parent's**
  (the parent is never narrowed; it is removed whole at cutover), so
  `is_routable()` (`!Building`) is load-bearing for every routing/scan
  consumer: without the filter, map-iteration order could serve a
  half-copied engine.
- `KeyRange`: `whole()`, `contains`, `contains_range` (subset containment),
  `split_at` (strictly-inside split into two half-open ranges). `abuts` —
  merge's contiguity test, production-caller-less since ADR 0044 — was
  deleted in the ADR 0050 rung-7 sweep.

## What's non-obvious

- `KeyRange` is half-open `[start, end)`; `end == None` means unbounded above
  (`whole()` is `start = []`, `end = None`).
- `Epoch` is the **data-plane fencing token**: every placement change bumps it.
  The actual split *state transitions* live in `animus-control`'s
  `Metadata::apply`; this crate provides the range primitives. (Tablets are
  split-only, ADR 0044 — there is no merge state transition anymore.)
- **`Tablet::version_floor` (the cross-group LWW version-floor fix) is
  retired (ADR 0018 §2 amendment, PR2)**, replaced by HLC witnessing (plus,
  historically, the zero-copy range seal — itself deleted with that split
  design in ADR 0050 Train B rung 7; per-tablet private engines make the
  cross-group shared-row hazard unrepresentable). Split provenance lives in
  `animus-control`'s `Metadata::split_lineage`, not on `Tablet` itself —
  this crate has nothing to say about it.
- Serializable (`serde`) because tablets travel inside control-plane Raft log
  entries and data-plane routing views.
- Dependency direction: `animus-control`, `animus-cql`, `animus-cp-data`, and
  `animusd` all depend on this crate — never the reverse. That reverse-dep ban
  is exactly why `escape`/`TableName` are duplicated here rather than imported.

## Tests

`cargo test -p animus-tablet` — inline unit tests for `contains`/
`contains_range`, `split_at` bounds, token determinism + fixed width,
the Murmur3 empty-input spec anchor, token spread across ring octants,
and table-scoped `serves_table`.
