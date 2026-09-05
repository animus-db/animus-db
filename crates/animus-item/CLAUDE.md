# CLAUDE.md — animus-item

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The pure DynamoDB-style **item model** — extracted from `animus-dynamo` (ADR
0054 step 1) to sit **below both** `animus-dynamo` (the wire adapter) and
`animus-cp-data` (the CP data plane's KV state machine). `animus-cp-data` is
protocol-agnostic and cannot depend on a wire crate; `animus-dynamo` keeps
HTTP/JSON wire decoding. This crate is where they can both meet: it holds
`AttributeValue`/`Item`/`TableSchema`, the order-preserving key encodings, the
`ConditionExpression`/`SortKeyCondition` evaluator, the `UpdateExpression`
data model and its apply-time evaluator, the stored-item codec, the item-size
formula, and the materialized secondary-index key/footprint/change-record
derivation. **As of this step, `animus-cp-data` does not yet depend on this
crate** — ADR 0054's step 2 adds the schema slice to `KindBatch` and wires
this crate in there; this step is a pure, no-behaviour-change relocation.

## What must stay pure, and why

Every module here is **pure**: no I/O, no storage engine, no network, no
`Env` (ADR 0003) — `BTreeMap`/`BTreeSet` only, and **no `animus-env`
dependency at all**. This is not incidental style — it is the property ADR
0054's plan depends on. `animus-cp-data`'s apply path is meant to run
identically on every replica from the same committed Raft log entry
(deterministic by construction, not by coincidence of catalog/clock timing —
see the ADR's "The entry is self-contained" mechanism). If evaluation here
ever reached into a clock, an RNG, a `HashMap`, or any other non-deterministic
seam, replicas applying the same entry could diverge. Never add an
`animus-env`/`tokio` dependency to this crate to "just get the time" or "just
spawn a task" — that would reopen exactly the hole ADR 0054 exists to close.
`cargo tree -p animus-item` should always show only `serde`/`serde_json` (plus
`bigdecimal`/`proptest` in `[dev-dependencies]`).

## Entry points

- `AttributeValue`/`Item`/`TableSchema` (`lib.rs`) — the DynamoDB type system
  and key schema, unchanged from their pre-move shape in `animus-dynamo`.
  `escape`/`key_bytes` stay `pub(crate)` (only `storage_key` and the `index`
  module need them); `storage_key(pk, sk)` is the public entry point for the
  base-table key.
- `numkey` — the order-preserving byte encoding for DynamoDB `N` values (ADR
  0063), used by `AttributeValue::key_bytes` and `condition::matches_raw`.
  Fully self-contained (no `crate::` dependencies of its own).
- `condition` — `SortKeyCondition`/`ConditionExpression`/`Comparator`, the
  decimal bignum helpers (`add_numeric`/`negate_numeric`/`compare_numeric`),
  and `ConditionError`. Unchanged from `animus-dynamo::condition`.
- `index` (ADR 0041) — the GSI/LSI row-key builders, `IndexFootprint`,
  `ChangeRecord`, and every other byte-layout primitive the write path, the
  GSI drain, and the native index read path agree on. Unchanged from
  `animus-dynamo::index`.
- `update` — the `UpdateExpression` **data model** (`PathSegment`/
  `UpdateOperand`/`UpdateExpr`/`UpdateAction`) and its apply-time evaluator
  (`apply_update` and everything it calls: `eval_update_expr`/
  `eval_update_arithmetic`/`get_document_path`/`eval_update_operand`/
  `set_document_path`/`set_into_container`/`document_path_parent_exists`/
  `remove_document_path`/`remove_from_container`, plus `union_sorted`/
  `difference_sorted`/`set_is_empty`/`type_name`/`format_update_path`). Its
  own doc comment explains the boundary in detail: the **tokenizer/parser**
  that turns a request's `UpdateExpression` string into a `Vec<UpdateAction>`
  stays in `animus-dynamo::wire` (it needs `ExpressionAttributeNames`/
  `ExpressionAttributeValues`, genuinely JSON/wire-decode concerns); only the
  already-resolved data types and the pure evaluator moved here. `UpdateError`
  is this module's own error type (`code`/`message`, mirroring
  `condition::ConditionError`'s shape) — `animus_dynamo::wire::WireError`
  converts from it (`impl From<UpdateError> for WireError`).
- `size` — `item_size`/`value_size`/`MAX_ITEM_SIZE_BYTES`: DynamoDB's
  published item/value size formula. Moved here because it has two
  independent callers now — `animus-dynamo::capacity`'s `ConsumedCapacity`
  accounting (re-exported unchanged, `capacity::item_size` still resolves)
  and `update::apply_update`'s own post-fold size cap — and this is the one
  copy both share.
- `stored` — `encode_stored_item`/`decode_stored_item`/`encode_tombstone`:
  the serialized form of an item as the data plane stores it at its key
  (`{"item": {..}}` / `{"tombstone": true}`). `decode_stored_item` returns
  `Result<Option<Item>, String>` here (a plain message) rather than a
  `WireError` — `animus_dynamo::wire::decode_stored_item` is the thin wrapper
  that turns it into a `WireError::serialization` with the exact same message
  text every existing caller already sees.

## The escape duplication (ADR 0023)

`animus-tablet` defines its own `escape`/token primitives, deliberately
duplicated rather than imported, because `animus-tablet` sits below
`animus-dynamo` (now: below this crate too) in the dependency graph and a
reverse dependency would invert it — see `crates/animus-tablet/CLAUDE.md`'s
own note. This crate's `escape` (in `lib.rs`) is a **relocation** of
`animus-dynamo`'s pre-existing copy, not a new duplicate: the reasoning is
unchanged, there are still exactly two independent copies (this crate's and
`animus-tablet`'s), and `animus-tablet`'s is untouched by this move.

## What changed vs. what didn't (ADR 0054 step 1)

This was a **pure move**: every symbol animus-dynamo re-exports still
resolves at its old `animus_dynamo::X` (or `animus_dynamo::wire::X`) path, and
no test's assertion changed. The two visibility widenings this crossing
required: `format_update_path` went from a private `fn` in `wire.rs` to a
`pub fn` here (`wire.rs`'s own `validate_no_overlapping_targets` — which
stays in `wire.rs` since it runs at parse time — needs it across the crate
boundary now), and `item_size`/`value_size` went from `animus-dynamo::
capacity`-owned `pub fn`s to this crate's, re-exported by `capacity`
unchanged. Everything else that used to be `pub(crate)` in `animus-dynamo`
either stayed `pub(crate)` here (`escape`, `AttributeValue::key_bytes`,
`condition::add_numeric`/`negate_numeric`) or was already `pub`.

## `write_schema` module (ADR 0054 step 2)

The second consumer of this crate — `animus-cp-data`'s apply path — landed:
`animus-cp-data` now depends on `animus-item` directly (no
`animus-dynamo`/`animus-env` pulled in transitively). A new module,
`write_schema`, holds:

- **`WriteSchema`** — the frozen schema slice a `KvCommand::KindEval` entry
  carries: `key: TableSchema`, `lsis: Vec<LsiDef>` (name/sort-attribute/
  projection — **no** GSI list, since a write never commits a GSI row
  directly; the asynchronous drain derives those later from the change-log
  record), and `change_records_carry_images: bool`. See the type's own doc
  for the "apply cannot read `Metadata`" rationale — the same reasoning
  this crate's own module doc gives for why `animus-cp-data`'s apply path
  needs a pure, dependency-free item model at all, one level more specific
  (a live catalog read at apply, not just an `animus-dynamo` dependency,
  would let two replicas of one entry derive different index rows).
- **`Projection`**/**`LsiDef`** — a small, pure, `animus-item`-local copy of
  `animus_control::schema::IndexProjection`/a narrowed `IndexDef`,
  duplicated rather than imported for the identical layering reason this
  crate's own "escape duplication" section gives for `animus-tablet`'s copy:
  the control plane sits *above* this crate (it names `TableSchema`/
  `AttributeValue` itself), so a reverse dependency would invert it.
- **`derive_kind_writes`** — the pure core of what used to be
  `animusd::dynamo::kind_writes_for_item`'s whole body (moved verbatim,
  byte-identical output): given a `WriteSchema`, an item's identity, its own
  ADR 0022 partition token (passed in — this crate still has no
  `animus-tablet` dependency), the old/new item, and the caller's own
  `KIND_BASE`/`KIND_LSI` byte constants (also passed in, for the identical
  reason — this crate sits below `animus-cp-data`, which defines them),
  derives the base/LSI writes and the one change-log record. `animusd::
  dynamo::kind_writes_for_item` is now a thin wrapper: build a
  `WriteSchema` (`write_schema_for`, next to the pre-existing `schema_for`),
  call this, return its two fields as the pre-existing tuple — proven
  byte-identical by every pre-existing `animusd` index/stream test staying
  green unmodified (`dynamo_indexes`, `dynamo_index_writes`,
  `dynamo_streams`, `dynamo_update_add_delete`).

**`animusd` depends on `animus-item` directly too** (not only via
`animus-dynamo`'s re-export list, which stays scoped to wire-adjacent item
types) — `write_schema_for`/`kind_writes_for_item`'s wrapper body need
`WriteSchema`/`LsiDef`/`Projection`/`derive_kind_writes` directly, and these
are apply-evaluator machinery a wire-protocol consumer of `animus-dynamo`
has no reason to see re-exported.

See `crates/animus-cp-data/CLAUDE.md`'s own ADR 0054 step 2 entry for how
`derive_kind_writes`'s output is consumed at apply (`KvCommand::KindEval`,
`evaluate_kind_eval`), the outcome mapping, and the leader-local result
slots — none of that lives in this crate, which stays pure and knows
nothing about Raft, apply order, or outcomes.

## Tests

`cargo test -p animus-item` — every unit test and proptest that moved with
its code: `numkey`'s differential proptests against `bigdecimal`, `condition`'s
own decimal differential tests and `matches_raw`/evaluator tests, `index`'s
row-key/footprint/change-record tests, `size`'s value/item-size tests
(moved from `animus-dynamo::capacity`), `stored`'s round-trip test, and
`update`'s `apply_update` tests (the ones that construct `UpdateAction`s
directly, as opposed to `animus-dynamo::wire`'s tests that exercise the
`UpdateExpression` string parser end to end — those stayed in `wire.rs`
since the parser did). See `crates/animus-dynamo/CLAUDE.md`'s Tests section
for what stayed there and why. `write_schema`'s own tests cover
`derive_kind_writes` directly (a plain insert with no index, an LSI diff
that removes the stale row and writes the new one, an unchanged sort
attribute that does not delete-then-reput the same row, and a delete) — the
apply-path integration coverage (`KvCommand::KindEval` end to end) lives in
`animus-cp-data`'s own `tests/kind_eval.rs` instead, since this crate has no
Raft/apply machinery to integration-test against.
