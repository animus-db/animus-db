# ADR 0023 — Table-scoped tablets on a per-table hash ring

- **Status:** Accepted
- **Date:** 2026-08-05
- **Builds on:** [ADR 0022](0022-hash-ring-partitioning.md) (the Murmur3 partition
  token), [ADR 0002](0002-tablets-unit-of-placement.md) (the tablet model)

## Context

ADR 0022 makes the keyspace a hash ring via a Murmur3 partition token. The
remaining question is *where the ring lives*: one global ring with every table's
rows interleaved across it, or one ring **per table**. A global ring scatters a
table's rows across every tablet, so a full-table scan must touch the whole
cluster, dropping a table requires sweeping the entire keyspace, and one table's
placement/replication can't be tuned independently. AnimusDB's adapters (DynamoDB,
CQL) are table-oriented, so the table is the natural unit of locality, placement,
and lifecycle.

We also want a clean invariant: **no key is unowned**. Today a tablet could be the
legacy whole-keyspace range that silently absorbs any key, which masks a
"table not provisioned yet" bug as a successful write into a stranger tablet.

## Decision

A tablet is **scoped to exactly one table**, and each table is its own hash ring.
Because each tablet is its own Raft group with its **own engine** (ADR 0017) and a
tablet holds exactly one table, the table is **not encoded in the key** — it is
carried as tablet metadata and as an explicit routing argument. Storing the table
name in every key would re-state what the engine already implies.

- **Key layout (within a tablet's engine — no table prefix):**
  ```
  partition_token(pk) || escape(pk) || rk
  ```
  The ADR 0022 token leads, spreading partitions across the table's ring; the
  partition key follows the token (prefix-free, so a token collision between a pk
  and a pk-prefix can't tear a partition's `Query`); the row/clustering key (`rk`)
  keeps a partition sort-ordered. `escape` doubles every `0x00`, terminated by
  `0x00 0x00`. (Considered and deferred: a cheaper length-prefixed `pk` framing —
  the pk no longer needs *order-preserving* encoding once it is hashed, only a
  self-delimiting one — kept as `escape` for uniformity with `rk`.)
- **`Tablet` carries its table** (`animus-tablet`): a tablet owns a pure
  **token sub-range** `[tok_lo, tok_hi)` of one table's ring; the table is the
  `table` field, not key bytes. Two tables' tablets may share a token range — they
  are distinguished by `table`, never by a global range scan.
- **Routing is `tablet_for(table, key)`**: the table is passed explicitly, then
  the key's leading token selects the tablet within `tablets_for_table(table)`.
  Routing never compares ranges across tables. **The table is enforced everywhere,
  not defaulted**: the data-plane primitives (`cp_read`/`cp_write`/`cp_delete`/
  `cp_scan`, `cp_route`, `tablet_for`) take a non-optional `&str`, and the entry
  types make it a **required field** — `ClientRequest::{Put,Get,Delete,Scan}` and
  the admin bulk-seed request all carry a required `table` (a table-less frame fails
  to decode), the CLI takes it as a positional arg, and the Dynamo/CQL edges read it
  from their own protocols. No path invents or defaults a table (there is no
  `__system` fallback) — so an unscoped key is unconstructable, not merely rejected. A split inherits the parent's table scope (it never crosses a table
  boundary). The replicated map stays in stable base node ids (ADR 0017).
- **One tablet per table at `CreateTable`, split on demand.** A table is
  provisioned with a single tablet covering its whole block and splits at the
  median token as it grows — not pre-split into V, because the hash already
  spreads partitions and a median-token split bisects load evenly.
- **No unscoped keys.** There is no whole-keyspace catch-all in the data plane:
  routing a key to a table with no provisioned tablet returns `None` and the
  caller **waits** (never absorbed by a stranger tablet). Raw / internal keys
  (the plain client, future system metadata) route under a reserved `__system`
  table scope. `Tablet.table` remains an `Option<TableName>` in the *type* only
  for the table-agnostic Accord/consensus shard router (`animus-consensus`, which
  routes by raw key and has no table concept) and for old-snapshot serde — the CP
  data plane never produces an unscoped tablet.
- **Scan is per-table fan-out.** A full-table `Scan` fans out across only that
  table's (bounded) tablet set and merges in token order, rather than sweeping the
  whole keyspace.

## Consequences

- **Per-table locality and lifecycle.** A table's scan touches only its own
  tablets; `DropTable` can reclaim a bounded, identifiable set of tablets + CP
  groups; a table can get its own replication factor and residency policy.
- **A clean "no orphan keys" invariant** at the data-plane boundary — a missing
  table is a visible wait, not a silent mis-route.
- **Splits and placement reason per table**, matching how operators think about a
  table-oriented store.
- **Type still admits `None`** for the consensus plane and legacy snapshots; the
  guarantee is enforced by the data-plane bootstrap/routing, not the type. (A
  later hard migration to a required `TableName` is possible if the consensus
  router is made table-aware.)

## Rollout

Staged (one logical change per PR), on top of ADR 0022's token:

1. **Tablet model + control plane** — `Tablet.table`, scoped `CreateTablet`,
   table-inheriting splits, `tablets_for_table` / `has_table_tablet`. *(done)*
2. **Routing** — `tablet_for` routes a table key only to its scoped tablet; no
   catch-all. *(done)*
3. **Key wiring** — emit table-less `token(pk) || escape(pk) || rk` keys in the
   DynamoDB / CQL / plain-client builders, thread `table` into the routing call
   (`tablet_for(table, key)`), and update the `Query`/`Scan` range math.
4. **Provision-at-create** — `CreateTable` stands up the table's own hosted Raft
   group (one tablet, splits on demand); `__system` scope for raw keys.
5. **Per-table scan fan-out**; **drop-table teardown** *(done — ADR 0024:
   `DropTableTablets` + the per-node GC loop reclaim the table's groups and
   on-disk data)*.

## Amendment (2026-08-17): create acks wait for serveability

Provision-at-create originally acked once the `CreateTableSchema` +
`CreateTablet` (+ policy) commits were observed — but the tablet's Raft group
forms and elects **asynchronously** (each replica's tablet-host reconciler,
ADR 0031), so the ack raced the formation window: a client's
immediately-following first write only landed via the election-wait machinery
(`cp_forward`'s backoff pass / the local `RouteDecision::Wait`) and, under
unlucky timing, burned much of `CLIENT_TIMEOUT` or failed outright.

**The create-ack contract is now: a success reply from `CreateTable`
(DynamoDB) / `CREATE TABLE` (CQL) additionally means the table's tablet group
is elected and serving.** Both edges call `ClientCtx::await_table_serveable`
before replying — a linearizable probe read routed through the ordinary
`cp_read` machinery, converged-or-timeout (a ReadIndex success requires an
elected leader with confirmed quorum contact, so it also implies a first
write commits promptly). First-*write* auto-provision paths are unchanged:
their own op already rides `cp_route`'s wait, so they need no extra gate.
Regression: `crates/animusd/tests/create_table_ready.rs` (the readiness
assertion is one-shot at ack time, deliberately — the property is "already
true when the 200 arrives").
