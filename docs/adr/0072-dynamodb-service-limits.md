# ADR 0072 — DynamoDB service limits are AWS-faithful and compiled-in; there is no "unleashed" mode

- **Status:** Accepted
- **Date:** 2026-09-22
- **Origin:** a maintainer question — should AnimusDB build an "unleashed"
  mode that forgoes DynamoDB's service limits? — plus a code sweep (this
  ADR's own groundwork PR) that found a real, unrelated gap: the `Query`/
  `Scan` 1 MB page cap was never enforced at all, a resource hazard rather
  than a mere compatibility gap.
- **Amends:** none. **Depends on:** ADR 0006 (wire adapter), ADR 0018
  (2PC transactions — why per-transaction limits are coupled to engine
  internals, §3), ADR 0019's 2026-08-23 amendment (why the DynamoDB wire
  cannot express a per-table configuration knob, §3(d)), ADR 0051 (TTL's
  5-year past-expiry guard, one of the limits catalogued here), ADR 0054
  step 1 (`animus_item::MAX_ITEM_SIZE_BYTES`), ADR 0063 (order-preserving
  `N` key encoding — number precision is ADR 0063's territory, never a
  knob, §4), ADR 0065 (per-table throttling, ADR 0065's `Provisioned-
  Throughput` floor is one of the limits catalogued here), ADR 0071
  (`BATCH_EXECUTE_STATEMENT_MAX_STATEMENTS`/`EXECUTE_TRANSACTION_MAX_
  STATEMENTS`, catalogued here unchanged).

## Context

Every DynamoDB limit AnimusDB enforced before this series was a hard-coded
constant with no config field, CLI flag, or compat/strict mode to loosen it.
A sweep across `crates/animus-dynamo`, `crates/animus-item`, and
`crates/animusd` found:

**Enforced today:**

- 400 KB item size (`animus_item::MAX_ITEM_SIZE_BYTES`,
  `crates/animus-item/src/size.rs`) — checked on `PutItem`, `BatchWriteItem`
  `Put`, `TransactWriteItems` `Put`, and post-fold on `UpdateItem`
  (`crates/animus-item/src/update.rs`).
- `BatchWriteItem` 25 items, `BatchGetItem` 100 keys,
  `TransactWriteItems` 100 actions, `TransactGetItems` 100 items,
  `BatchExecuteStatement`/`ExecuteTransaction` 25 statements each (ADR
  0071) — all in `crates/animus-dynamo/src/wire.rs`
  (`BATCH_WRITE_MAX_ITEMS`, `BATCH_GET_MAX_KEYS`,
  `TRANSACT_WRITE_MAX_ACTIONS`, `TRANSACT_GET_MAX_ITEMS`,
  `BATCH_EXECUTE_STATEMENT_MAX_STATEMENTS`,
  `EXECUTE_TRANSACTION_MAX_STATEMENTS`).
- 20 GSI / 5 LSI per table (`MAX_GSI_PER_TABLE`/`MAX_LSI_PER_TABLE`,
  `crates/animus-dynamo/src/wire.rs`).
- 38 significant digits for a numeric key (`animus_item::numkey::
  MAX_SIGNIFICANT_DIGITS`, ADR 0063's encoding choke point,
  `crates/animus-item/src/numkey.rs`).
- `ClientRequestToken` 1–36 characters
  (`decode_client_request_token`, `crates/animus-dynamo/src/wire.rs`).
- `BackupName` 3–255 characters, `[a-zA-Z0-9_.-]`
  (`validate_backup_name`, `crates/animus-dynamo/src/wire.rs`).
- `ProvisionedThroughput` ≥ 1 read/write unit
  (`decode_provisioned_throughput`, `crates/animus-dynamo/src/wire.rs`; ADR
  0065's own throttling machinery is what a provisioned table's request
  rate is checked against once accepted).
- `DescribeLimits` reports the account/table capacity ceilings AWS's
  on-demand default publishes — 80,000 account-level, 40,000 per-table
  (`ACCOUNT_MAX_READ_CAPACITY_UNITS`/`ACCOUNT_MAX_WRITE_CAPACITY_UNITS`/
  `TABLE_MAX_READ_CAPACITY_UNITS`/`TABLE_MAX_WRITE_CAPACITY_UNITS`,
  `crates/animus-dynamo/src/wire.rs`) — **reported only**, since this
  adapter has no capacity-billing meter to enforce them against (`capacity.
  rs`'s own doc comment says so plainly).
- TTL's 5-year past-expiry guard (`MAX_PAST_EXPIRY_SECS`,
  `crates/animus-dynamo/src/ttl.rs`, ADR 0051) — the safety window against
  a client writing milliseconds instead of seconds.

**Not enforced before this series** (found by the same sweep, and the
reason this ADR exists rather than just answering the maintainer's
question in prose):

- `Query`/`Scan`'s 1 MB per-page evaluation cap. Pagination was purely
  item-count based (`Limit`) — a `Scan` with no `Limit` materialized the
  *whole tablet* into one response. This is not just a compatibility gap;
  it is a live resource hazard on a large table, independent of whether
  AWS-fidelity is a goal at all.
- `BatchGetItem`'s 16 MB response cap.
- `BatchWriteItem`'s 16 MB request cap.
- `TransactWriteItems`/`TransactGetItems`'s 4 MB aggregate cap.
- Partition key 2048 B / sort key 1024 B attribute-value size limits.
- Attribute name length (1–64 KB general; key attribute names ≤ 255
  characters).
- Nesting depth (32 levels of `List`/`Map`).
- Expression length (4 KB per `ConditionExpression`/`UpdateExpression`/
  `ProjectionExpression`/`FilterExpression`/`KeyConditionExpression`).
- Table/index name shape (3–255 characters, `[a-zA-Z0-9_.-]`).
- `ProvisionedThroughput`'s upper ceiling (40,000 units) — only the floor
  (≥ 1) was checked.

The maintainer asked whether to build an "unleashed" mode that forgoes
DynamoDB's limits entirely, given that this is a from-scratch database with
no billing story (root `CLAUDE.md`: "no RCUs/WCUs to plan around" outside
ADR 0065's own opt-in throttling) and no obvious reason to inherit AWS's
1980s-shaped ceilings. That question is what this ADR answers.

## Decision

### 1. AWS-faithful enforcement: same error class, same non-error behavior

AnimusDB enforces DynamoDB's documented service limits **AWS-faithfully**:

- Where DynamoDB errors, this adapter errors with the **same error class**
  — `ValidationException`, matching every other decode-time rejection this
  crate already gives (`wire.rs`'s existing convention, see
  `crates/animus-dynamo/CLAUDE.md`).
- Where DynamoDB has a **non-error** behavior at a limit, this adapter
  matches that shape rather than substituting an error: `Query`/`Scan`
  stop at 1 MB of evaluated items and return a `LastEvaluatedKey` exactly
  as if `Limit` had been reached, not a thrown exception; `BatchGetItem`
  returns the overflow past 16 MB as `UnprocessedKeys` (the same shape it
  already uses for a throttled or over-count key, ADR 0065), not a partial
  failure.

This is the same posture ADR 0065 took for throttling (`Provisioned-
ThroughputExceededException`, `UnprocessedItems`/`UnprocessedKeys`, a
`ThrottlingError` cancellation reason) and ADR 0071 took for PartiQL
(`ValidationException` naming what was rejected) — AnimusDB's wire
contract is "behaves like DynamoDB", and a limit is part of that contract,
not an implementation footnote.

### 2. One catalogue: `animus_dynamo::limits`

Every limit constant lives in exactly one module,
`crates/animus-dynamo/src/limits.rs`. A pre-existing constant is
**re-exported from where it already lives** (`crate::wire`, `animus_item`,
`animus_item::numkey`) — unmoved, so nothing that already depends on its
old path breaks — and every *new* constant this series adds lives in
`limits.rs` itself. A limit check anywhere in `animus-dynamo` or `animusd`
names its constant from `animus_dynamo::limits`, never a bare numeric
literal restated at the call site. See the table below for the full
catalogue as it stands after this ADR's groundwork PR.

### 3. No "unleashed" mode, no per-limit configuration

There is no `--unleashed` flag, no compat/strict mode, and no per-limit
config field, for four reasons:

**(a) The high-level SDKs already chunk client-side.** The AWS SDKs'
document/high-level clients split a batch at 25 (`BatchWriteItem`) or 100
(`BatchGetItem`/`TransactWriteItems`/`TransactGetItems`) before a request
ever reaches the wire. Raising those caps server-side only reaches a
client using the *raw* API directly — a narrow, low-value target for the
complexity a config knob adds.

**(b) The limits users actually want lifted are coupled to engine
internals, not free-standing numbers.** Item size and transaction size are
the two limits most likely to actually constrain a real workload, and
neither is a flag flip:

- Item size (400 KB) bounds a single Raft log entry's payload (every
  hosted tablet has its own private engine per ADR 0050, but the entry
  itself still has to serialize, replicate, and fsync as one WAL record),
  the forwarding message size between nodes (ADR 0047's intra port), the
  HTTP body limit at the DynamoDB wire edge, `capacity.rs`'s RCU/WCU
  arithmetic (ADR 0065's throttling divides a table's provisioned
  capacity by its tablet count and meters against exactly this unit), and
  backup/restore chunking (ADR 0059's BASE/LSI/FOOTPRINT object chunking
  is sized around it).
- Transaction size (100 actions / 4 MB) bounds 2PC participant count and
  the intent-hold window across however many tablet Raft groups a
  transaction spans (ADR 0018) — a larger transaction holds intents open
  longer, across more groups, which is a liveness and contention question,
  not a validation-constant question.

Raising either would need its own ADR-level ceiling (what *is* safe, and
why) and its own fault-injection corpus at the raised value (root
`CLAUDE.md`: "every distributed behavior lands with a fault-injecting
simulation test") — it cannot be a config default with no engineering
behind the new number.

**(c) A mode doubles the test matrix.** "Every limit, at both its normal
value and its unleashed value" is not a one-line change to any existing
corpus; it is a second corpus dimension for every limit and every corpus
that exercises one, forever, for a feature with no user-visible identity
beyond "a knob was left on."

**(d) The DynamoDB wire cannot express a per-table limit set anyway.**
This is the same structural fact that closed ADR 0019's AP long shot: once
CQL is dropped (ADR 0053), the DynamoDB wire is the only client-facing
surface, and it has no request field for "run this table with a different
limit." Any future knob that did materialize would have to be **cluster-
wide operator configuration** (a node/cluster flag, not a per-request or
per-table wire field) — which is exactly the shape named as the only
acceptable one below, if the day comes.

### 4. Number precision is never a knob

`MAX_SIGNIFICANT_DIGITS` (38) is not a service-limit validation constant
in the same sense as the others — it is ADR 0063's own key-encoding choke
point (`AttributeValue::key_bytes`/`numkey::encode_checked`): the byte
layout that makes stored-key order equal DynamoDB numeric order is
*defined* in terms of a fixed digit budget. Raising it is a wire-format
change to every numeric key already written, not a limit relaxation — ADR
0063 is the authority on this constant, and this ADR does not attempt to
re-open it. It is catalogued below (re-exported, unmoved) for
completeness only.

## Alternatives considered

- **A single `--unleashed` switch that disables every check.** Rejected —
  it bundles unrelated knobs (item size, batch counts, name-length rules,
  transaction size) behind one bit with no way to loosen just the one a
  workload actually needs, and the item-size/transaction-size limits
  cannot safely move without their own engineering (§3(b)) regardless of
  how the switch is spelled.
- **Per-limit, cluster-wide configuration with DynamoDB's own values as
  defaults.** Deferred, not rejected outright — this is the only shape
  recorded here as acceptable *if* real demand materializes: cluster-wide
  (an operator/node flag, never per-table or per-request — §3(d)), each
  limit independently configurable, DynamoDB's published value as the
  floor a config can only raise from, and explicitly excluding the
  engine-coupled limits (item size, transaction size/action count) until
  each has its own ADR pinning a safe ceiling and its own corpus. No such
  demand exists today; this ADR does not build the config surface.
- **Leave the gaps unenforced.** Rejected. The unenforced 1 MB `Query`/
  `Scan` page cap was found to be a live resource hazard during this ADR's
  own groundwork sweep, not merely a documentation gap — an unbounded
  `Scan` can materialize an entire tablet. Beyond that one hazard, silent
  divergence from DynamoDB's documented behavior breaks the drop-in
  promise `website/compatibility.html` makes to a client written against
  real DynamoDB.

## Consequences

This ADR ships as the bottom of a 4-layer stacked series:

1. **This ADR + `animus_dynamo::limits`** (`crates/animus-dynamo/src/
   limits.rs`) — the catalogue module, re-exporting every pre-existing
   limit constant unchanged and adding the constants for limits not yet
   enforced. An unused new constant at this layer is expected, not a bug
   (the module's own doc comment says so) — wiring lands in the layers
   below.
2. **Wire-decode validation limits** — key value lengths (partition 2048
   B / sort 1024 B), key/attribute name lengths (`MAX_KEY_ATTRIBUTE_
   NAME_CHARS`/`MAX_ATTRIBUTE_NAME_BYTES`), nesting depth
   (`MAX_NESTING_DEPTH`), expression length (`MAX_EXPRESSION_BYTES`),
   table/index name shape (`is_valid_table_or_index_name`), and the
   `ProvisionedThroughput` ceiling (40,000 units, the upper half of what
   `decode_provisioned_throughput` already checks the floor of).
3. **The `Query`/`Scan` 1 MB page cap** (`MAX_QUERY_SCAN_PAGE_BYTES`) —
   closing the resource hazard this ADR's own sweep found.
4. **The aggregate byte caps** — `BatchGetItem` 16 MB
   (`MAX_BATCH_GET_RESPONSE_BYTES`) folded into `UnprocessedKeys`,
   `BatchWriteItem` 16 MB (`MAX_BATCH_WRITE_REQUEST_BYTES`),
   `TransactWriteItems`/`TransactGetItems` 4 MB aggregate
   (`MAX_TRANSACT_BYTES`) — plus the `website/compatibility.html` update
   (root `CLAUDE.md`'s Conventions: "the website is part of the
   documentation... a change that alters something the site claims
   updates `website/` in the same change") listing every limit this
   series newly enforces.

**Explicitly deferred**, listed here rather than silently dropped:

- DynamoDB Streams' `ShardIterator` 15-minute expiry
  (`ExpiredIteratorException`) — no shard-iterator TTL exists in this
  adapter today; a future ADR's territory, not this one's.
- The LSI 10 GB item-collection size limit (a whole partition's LSI rows
  bounded in aggregate) — a different shape of limit (aggregate over a
  partition's rows, not a single request/item), left for its own design.
- `ExpressionAttributeNames`/`ExpressionAttributeValues` aggregate size
  rules (as opposed to a single expression string's own 4 KB cap, which
  layer 2 above does cover).
- The 80,000 account-level capacity ceiling as an actually-*enforced*
  number — AnimusDB has no account concept at all (every table is
  cluster-wide), so there is nothing to divide 80,000 across; `Describe-
  Limits` keeps reporting it as a static, honest, unenforced number
  exactly as it does today.

### Limit catalogue

| Limit | Constant (`animus_dynamo::limits`) | Value | Enforced |
|---|---|---|---|
| Item size | `MAX_ITEM_SIZE_BYTES` (from `animus_item`) | 400 KB (409,600 B) | Today |
| `BatchWriteItem` item count | `BATCH_WRITE_MAX_ITEMS` | 25 | Today |
| `BatchGetItem` key count | `BATCH_GET_MAX_KEYS` | 100 | Today |
| `TransactWriteItems` action count | `TRANSACT_WRITE_MAX_ACTIONS` | 100 | Today |
| `TransactGetItems` item count | `TRANSACT_GET_MAX_ITEMS` | 100 | Today |
| `BatchExecuteStatement` statement count | `BATCH_EXECUTE_STATEMENT_MAX_STATEMENTS` | 25 | Today |
| `ExecuteTransaction` statement count | `EXECUTE_TRANSACTION_MAX_STATEMENTS` | 25 | Today |
| GSIs per table | `MAX_GSI_PER_TABLE` | 20 | Today |
| LSIs per table | `MAX_LSI_PER_TABLE` | 5 | Today |
| Numeric key significant digits | `MAX_SIGNIFICANT_DIGITS` (from `animus_item::numkey`) | 38 | Today (ADR 0063's encoding, never a knob — §4) |
| `ClientRequestToken` length | (`decode_client_request_token`, `wire.rs`) | 1–36 chars | Today |
| `BackupName` shape | (`validate_backup_name`, `wire.rs`) | 3–255 chars, `[a-zA-Z0-9_.-]` | Today |
| `ProvisionedThroughput` floor | (`decode_provisioned_throughput`, `wire.rs`) | ≥ 1 unit | Today |
| `DescribeLimits` account ceiling | `ACCOUNT_MAX_READ_CAPACITY_UNITS`/`ACCOUNT_MAX_WRITE_CAPACITY_UNITS` | 80,000 | Reported only — no account concept to enforce against |
| `DescribeLimits` table ceiling | `TABLE_MAX_READ_CAPACITY_UNITS`/`TABLE_MAX_WRITE_CAPACITY_UNITS` | 40,000 | Reported only |
| TTL past-expiry safety window | `MAX_PAST_EXPIRY_SECS` (`ttl.rs`) | 5 years | Today (ADR 0051) |
| Partition key attribute-value size | `MAX_PARTITION_KEY_BYTES` | 2048 B | Layer 2 |
| Sort key attribute-value size | `MAX_SORT_KEY_BYTES` | 1024 B | Layer 2 |
| Key attribute name length | `MAX_KEY_ATTRIBUTE_NAME_CHARS` | 255 chars | Layer 2 |
| Attribute name length | `MAX_ATTRIBUTE_NAME_BYTES` | 65,536 B (64 KB) | Layer 2 |
| Nesting depth | `MAX_NESTING_DEPTH` | 32 levels | Layer 2 |
| Expression length | `MAX_EXPRESSION_BYTES` | 4096 B | Layer 2 |
| Table/index name length | `MIN_TABLE_NAME_CHARS`/`MAX_TABLE_NAME_CHARS` | 3–255 chars, `[a-zA-Z0-9_.-]` | Layer 2 |
| `ProvisionedThroughput` ceiling | (extends `decode_provisioned_throughput`) | 40,000 units | Layer 2 |
| `Query`/`Scan` page size | `MAX_QUERY_SCAN_PAGE_BYTES` | 1 MiB (1,048,576 B) | Layer 3 |
| `BatchGetItem` response size | `MAX_BATCH_GET_RESPONSE_BYTES` | 16 MiB (16,777,216 B) | Layer 4 |
| `BatchWriteItem` request size | `MAX_BATCH_WRITE_REQUEST_BYTES` | 16 MiB (16,777,216 B) | Layer 4 |
| `TransactWriteItems`/`TransactGetItems` aggregate size | `MAX_TRANSACT_BYTES` | 4 MiB (4,194,304 B) | Layer 4 |
| Streams `ShardIterator` expiry | — | 15 min | Deferred, not scheduled |
| LSI item-collection size | — | 10 GB | Deferred, not scheduled |
| `ExpressionAttributeNames`/`Values` aggregate size | — | AWS's own rules | Deferred, not scheduled |
| Account-level capacity ceiling (enforced) | — | — | Deferred — no account concept |

## Testing

`limits.rs` itself carries unit tests for its one non-trivial function,
`is_valid_table_or_index_name` (boundary lengths, invalid characters, the
full accepted charset) — see the module for the current list. Layers 2–4
each add their own decode-time/execution-time unit tests plus, per root
`CLAUDE.md`'s "every distributed behavior lands with a fault-injecting
simulation test" convention, end-to-end coverage in `crates/animusd/
tests/` for the layers that touch the write/read/batch paths (the
`Query`/`Scan` page cap and the aggregate byte caps in particular, since
both change response *shape*, not just accept/reject).
