# A write path's siblings can silently diverge in feature coverage, and nothing but a grep of every writer catches it.

**A write path's siblings can silently diverge in feature coverage, and
nothing but a grep of every writer catches it.** ADR 0041's index-maintaining
write path (`animusd::dynamo::index_aware_write` → `kind_writes_for_item`,
committing an item's LSI rows + GSI change-log record atomically with the
base row) was wired into the single-item `PutItem`/`DeleteItem` handlers
only. `UpdateItem` (`run_update_item`), `BatchWriteItem`, and
`TransactWriteItems` (`run_transact`) all still commit through the plain
`quorum_write`/`cp_batch_write`/`cp_txn` primitives that predate ADR 0041 —
so a table's secondary indexes silently never see a write made exclusively
through any of those three ops, with no error, no warning, and no test
currently exercising that combination (every existing GSI/LSI test happens
to write through `PutItem`). This was found while replacing the old
edge-local in-memory index (which every write path *did* feed, via a
shared `note_put`/`note_delete` post-write hook) with the native
replicated-row read path (ADR 0041 §5) — deleting that shared hook removed
the one place all four write paths' index-maintenance obligations were
unified, which is what made the pre-existing gap visible instead of merely
latent. The general lesson (the same shape as this file's
`ClientCtx::cp_write`/`cp_put` entry above): when a family of write
operations is supposed to share one piece of derived-data maintenance,
check *every member of the family* against the primitive that actually
does it, not just the one the feature's own tests happen to exercise — a
new mechanism wired into only the "obvious" call site looks complete right
up until a sibling silently doesn't participate. Left unfixed here
(deliberately out of scope for the read-path PR that found it — see
"Separate PRs for incidental bugs"); tracked in `animusd/CLAUDE.md`'s and
`animus-dynamo/CLAUDE.md`'s ADR 0041 entries.

**Resolution (2026-08-13).** `UpdateItem` and `BatchWriteItem` now route
through `index_aware_write` (the latter per-item, and only for a table
that actually has an index — an unindexed table keeps its no-read
`cp_batch_write` fast path unchanged). `TransactWriteItems` did **not**
get the same treatment — it is rejected outright (`ValidationException`)
whenever any write action targets an indexed table, because closing this
gap for real would mean giving `cp_txn`'s `KvCommand::TxnStage` a
multi-kind-write extension (staging LSI rows + a change record atomically
with a transactional base-row write), which is a genuine `animus-cp-data`
protocol change, not a `dynamo.rs`-local fix — named as a follow-up in ADR
0041's as-built note under §2. The generalizable half of this entry
stands on its own; the corollary worth keeping: **when the honest fix for
one sibling in a "family of write operations" reaches into a lower layer's
protocol, a loud rejection of that one combination is a legitimate
interim closure of a correctness gap** — better than either leaving it
silently wrong or scope-creeping a wire-edge fix into a data-plane
protocol change. Regression: `animusd/tests/dynamo_index_writes.rs`.
