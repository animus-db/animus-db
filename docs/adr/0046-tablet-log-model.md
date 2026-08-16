# ADR 0046 — The tablet log model

- **Status:** Accepted
- **Date:** 2026-08-16
- **Amends:** [ADR 0018](0018-cross-tablet-transactions.md) (names the
  serialization-point discipline its apply-time write-key conditions and
  `TxnStage` already practice), [ADR 0028](0028-shared-storage-single-command-split.md)
  (the fence-at-apply/CAS-at-apply idiom this ADR generalizes), [ADR
  0041](0041-materialized-secondary-indexes.md) (names the drain's
  consumer/producer shape and flags §2's intent-staging design as rejected —
  see Decision 2), [ADR 0042](0042-dynamo-streams.md)/[ADR
  0043](0043-stream-shard-subsystem.md) (names the seal arm and its
  frozen-basis split inheritance as instances of the same model), [ADR
  0045](0045-updatetable-gsi-backfill.md) (names the backfill seeder as a
  producer of the same change-record stream, and flags its own §16 reference
  to intent-staging as superseded by Decision 2 below).
- **Depends on:** [ADR 0022](0022-hash-ring-partitioning.md)/[ADR
  0023](0023-table-scoped-tablets.md) (the per-tablet log this ADR names is
  scoped per table, per tablet — never global), [ADR
  0044](0044-split-only-tablets.md) (split-only is what makes "log cut,
  never log merge" the only lineage shape this model has to reason about).
  ADR 0044's own cheap-groups roadmap (quiescence first) is a distinct,
  later concern — an idle Raft group's liveness cost, not this ADR's
  apply-time materialization model — and gets its own follow-up ADR rather
  than being folded in here.

## Context

Nothing in this ADR is new mechanism. Five separate features — cross-tablet
transactions (ADR 0018), the shared-storage split (ADR 0028), materialized
secondary indexes (ADR 0041), DynamoDB Streams (ADR 0042/0043), and
`UpdateTable` backfill (ADR 0045) — each independently arrived at the same
shape: **a tablet is a Raft log, and everything that isn't the base row is
something computed from that log, deterministically, at the point the log's
own order becomes fixed.** No ADR named that shape as a shape. Each one
solved its own slice of it, in its own vocabulary, and the seams between
those slices are exactly where the last month's production bugs lived.

Concretely, two seams recur:

**(a) Three independent consumer-offset implementations.** The GSI drain's
`"gsi"` cursor watermark (ADR 0041 §4a), the stream sealer's per-tablet
watermark (ADR 0043 §A6), and the backfill seeder's partition-prefix cursor
(ADR 0045 §3/§5) each re-derived, on their own, what a consumer's offset
means across a split. Each got it wrong at least once, in a different way:
the split-watermark data-loss bug (PR #216 — a *live*-derived watermark let a
parent's later seal retroactively raise a child's own, silently skipping a
backlog the child had physically inherited but not yet sealed itself); the
backfill-cursor's fence rejection on split children and the same-named-index
recreation poisoning it (ADR 0045 §5/§6); and the split-seal duplication race
(PR #220 — the mirror of #216: a parent re-sealing records that, per
`Metadata`, already belong to a split-off sibling, because the physical
scope and the metadata snapshot the seal arm reads can both lag the same
committed split at once). Three offset trackers, three bug families, from
one un-named shared invariant.

**(b) Derived state computed at the wrong place.** An LSI diff
(`kind_writes_for_item`, `crates/animusd/src/dynamo.rs:2839`) and a
change-record image are both derived at the edge node handling a request,
under that node's own local `rmw_lock` (`crates/animusd/src/dynamo.rs`,
e.g. lines 447/495/596/651/1517) — while the fact that actually decides
whether a write happened at all lives at apply, on whichever node leads the
tablet's Raft group. Those two places coincide when one node handles a
partition's writes exclusively; they do not coincide across nodes, and the
gap is a real, still-open bug: two nodes racing a write to the same item each
diff against the same stale "before" image under their own local lock, one
commits, and the loser's now-stale LSI row is never revisited — the GSI side
self-heals for free (the drain always reconciles toward the base row's
*current* value, ADR 0041 §4), but an LSI row is written once, synchronously,
with no reconciler of its own.

Naming the shape does two things this ADR is for: it gives the three
offset-tracking implementations one invariant to be checked against instead
of three ad hoc arguments, and it turns "where should derived-state
computation live" from a per-feature question into one decision (Decision 1
below), settled once.

## Decision

**A tablet is a Raft log whose apply deterministically materializes every
piece of state colocated with it.** Five principles, each already load-
bearing somewhere in the tree; this ADR is what makes them one model rather
than five coincidences.

1. **One log entry, one atomic materialization.** `KvCommand::KindBatch`'s
   apply arm (`crates/animus-cp-data/src/lib.rs:4591`) writes a write's base
   row, every LSI row, and its change-log record in the same Raft entry,
   gated as one unit exactly like `KvCommand::Batch`. The change-log key's
   HLC suffix is completed *at apply*, "with THIS entry's commit
   timestamp — the only one that agrees with the entry's position in the
   log, and so the only one that makes the log readable in commit order"
   (the apply arm's own comment, `lib.rs:4628-4631`; ADR 0041 §4a).
2. **Everything asynchronous is a consumer or producer of that same stream.**
   The GSI drain, the Streams sealer, and the backfill seeder are not three
   mechanisms; they are three offset positions over one change-record
   stream. The abstraction is real, not aspirational: ADR 0045's backfill
   seeder populates a newly declared index by *injecting* synthetic change
   records so the unmodified GSI drain materializes them with zero new code
   in `reconcile_partition` (ADR 0045 §2/§3); and the round-3 Streams pivot
   (ADR 0043) deleted a whole parallel copying/resharding tier once it
   recognized a stream shard as nothing but a seal epoch of the log the
   table already had.
3. **A split is a log cut, and every consumer offset crossing it must be
   inherited from a basis frozen at the cut** — never re-derived live from
   the parent's later state. This is the generalized form of the #216
   lesson: `Metadata::stream_split_basis` (ADR 0043 §A4) exists specifically
   because a *live* walk of the parent's chain lets the parent's later
   activity retroactively change what the child inherited.
4. **Authority lives at the serialization point, never at the edge.** The
   edge proposes intent; apply (or, for the control plane, the equivalent
   serialization point) decides. This is `KvCommand::Cas`, ADR 0018's
   apply-time write-key conditions, `TxnStage`'s own apply-time fence/seal
   checks (`crates/animus-cp-data/src/lib.rs` around `KvCommand::TxnStage`),
   ADR 0028's per-entry range fence, and PR #220's `SealStreamShard`
   range-CAS (ADR 0043 §A4's "Split-seal range-fence CAS amendment") all
   independently arriving at the same shape: a proposal is a claim, apply is
   the ruling.
5. **The log stays physical and replay/snapshot-stable.** An entry's effect
   at apply must never depend on the binary version that replays it — the
   same reason `KindBatch`'s apply arm skips, rather than interprets, a row
   kind it doesn't recognize (`lib.rs:4614-4616`), and the same reason
   Decision 1 below rejects deriving state from a carried specification at
   apply time.

### Decision 1 — the derived-state evaluation point: evaluate-at-leader

Where a derived write (an LSI diff, a change-record image) gets *computed*
was never actually settled by ADR 0041 — it shipped answering it
implicitly, at the edge, under a node-local lock, which is §(b) above's open
bug. Three options were weighed:

- **U1 — proposer-evaluated, kept as an interim seatbelt.** The edge node
  keeps evaluating the diff (as it does today), and an implicit
  own-key condition at apply rejects a diff proposed against an image apply
  can prove is stale — the shape CockroachDB calls proposer-evaluated KV.
  This narrows the race but does not close it (a diff computed against a
  stale image can still be the *first* to commit, landing a wrong LSI row
  that nothing then corrects) and is not adopted as the standing design —
  only as the cheap fallback the chosen option below still keeps for its
  own narrow residual window.
- **U2 — derive-at-apply. Rejected.** Apply reads the old image itself and
  computes the derived rows from a carried specification (e.g. "recompute
  this item's LSI rows"), rather than from already-physical row writes.
  This directly violates principle 5: a log entry's materialized effect
  would depend on the version of `kind_writes_for_item`-equivalent logic
  that happens to run it, which is exactly the replay/snapshot divergence
  hazard this model exists to rule out — and it is the identical reason
  CockroachDB itself abandoned apply-time evaluation in favor of
  proposer-evaluated writes.
- **U3 — evaluate-at-leader. Decided.** The edge forwards the *logical*
  operation to the tablet's own current group-leader node; that leader —
  and only that leader, for that tablet, at that moment — reads the old
  image and runs the derivation (`kind_writes_for_item`) under its own
  latch, then proposes the resulting *physical* rows. Every evaluation for
  one tablet funnels through one node, so the two-node race in §(b) above
  cannot arise: there is only ever one node evaluating against the "current"
  image at a time, for a given tablet, and the log still only ever carries
  physical writes (principle 5 stays satisfied). U1's apply-time own-key
  condition is retained underneath U3 anyway, as a cheap seatbelt for the
  narrow evaluate-then-propose sliver (a leadership change between the read
  and the propose) — belt-and-suspenders, not the load-bearing mechanism.
  Transactional writes take the identical shape: a kind payload is
  evaluated at each participant's own leader at *stage* time, and the staged
  intent then blocks a conflicting write until resolution, rather than being
  evaluated anywhere else.

### Decision 2 — intent-staging of derived rows is rejected

Older ADR prose (ADR 0041 §2, ADR 0042 §16, ADR 0045's named follow-up)
recorded the obvious-looking fix for `TransactWriteItems` on an indexed or
streamed table as "stage LSI rows and the change record as intents in their
own kind scopes, the way a base row is staged today." That design is
**formally rejected**, not merely deferred:

- **A change record staged as an intent can be resolved after a consumer's
  cursor has already passed its timestamp.** Every consumer here — the GSI
  drain, the Streams sealer, the backfill seeder — only ever scans *forward*
  from an HLC watermark, and a kind-scope scan is defined to skip intents
  (only a base scope's readers ever resolve one). A record staged at `ts=10`
  and resolved at `ts=40`, after a consumer's watermark has already passed
  10, is silently skipped forever: a permanently lost GSI update, or a
  permanently lost stream event, with no error.
- **It breaks a load-bearing invariant every non-base reader relies on**:
  that a kind scope only ever holds committed values. `RaftKvNode::
  linearizable_scan_kind` (ADR 0041 §5's as-built note) and every drain/
  sealer/seeder read a kind scope with no intent-resolution step at all,
  specifically because that invariant was true. Staging a kind-scope row as
  an intent would require giving every one of those readers a resolution
  path it does not have and was never designed to need.

The replacement — **materialize-at-resolve**: a derived row and its change
record ride inside the *base* row's own intent envelope, and `TxnResolve`'s
commit branch writes them at its own, locally-minted commit timestamp, the
same way `KindBatch`'s apply arm mints one today — is the subject of the
upcoming `TxnStage` kind-writes delivery, not this ADR. This ADR records
only the model-level reason the older design is wrong; ADR 0041 §2, ADR
0042 §16, and ADR 0045's "Named follow-ups" entry will each be amended in
place once that stack ships.

## Consequences

**Binding, going forward.**

- **One shared materialization function.** `KindBatch`'s apply arm and
  `TxnResolve`'s commit branch must call one common "materialize derived
  writes at this timestamp" helper — byte-identical output for identical
  payloads — never two independently-maintained copies. Two copies is
  exactly the kind of drift principle 5 exists to prevent: they would start
  identical and diverge the first time either is touched without the other.
- **One consumer-offset concept, not three.** The GSI cursor's HLC
  watermark, the stream sealer's HLC watermark, and the backfill seeder's
  raw-key cursor keep their two different *value* conventions — an HLC
  watermark is not interchangeable with a raw last-seeded-key prefix — but
  a planned consolidation gives all three exactly one lineage rule and one
  inheritance implementation for what a split does to an offset (principle
  3), rather than three call sites each re-deriving it and each getting a
  chance to re-introduce #216 or #220's bug in a new place.
- **What this model does not, and cannot, change.** LSI must stay
  same-entry-synchronous — DynamoDB's own strong-consistency contract for a
  local secondary index (ADR 0041's "consistency question") is not
  negotiable, and Decision 1 only changes *who* computes an LSI diff and
  *when* within one atomic write, never whether it is synchronous. Per-
  tablet logs stay per-tablet — a single global log would defeat the whole
  point of a masterless, linearly-scalable data plane (ADR 0001/0019) — so
  split-lineage complexity can be centralized into one shared rule (above)
  but never removed outright; every tablet still has its own log, its own
  cut points, and its own basis to freeze at each cut. And the change log
  is trimmed behind its slowest consumer (ADR 0041 §4a/ADR 0042 §8) — it is
  the *conduit* changes travel through, not a second copy of the database.
  Base state, not the log, remains the authority for what the data actually
  is; a query never answers itself from the log, only from the materialized
  base/index rows the log's own consumers produced.

**Left open, explicitly.** The cross-node LSI orphan-row race described in
Context §(b) is not closed by this ADR — Decision 1 (U3, evaluate-at-leader)
is the fix, and it ships as its own change, not folded into this document.
This ADR's job is to record *why* U3 rather than U1 or U2, so that change can
be reviewed against a stated model instead of a bare diff.

## As-built amendment (2026-08-16) — the `TxnStage` kind-writes delivery

Both items this ADR left as forward references have now shipped.

**Decision 2's replacement, materialize-at-resolve, is principle 1's own
transactional extension.** Principle 1 states "one log entry, one atomic
materialization" for `KindBatch`; the shipped mechanism is the identical
claim for a transaction's own commit point. A transactional write's derived
kind-scope rows and change-log record ride inside the base write's own
intent envelope (opaque, never written to a kind scope directly — kind
scopes still only ever hold committed values, unchanged), and
`KvCommand::TxnResolve`'s commit branch materializes them in the same
atomic apply that finalizes the base value, at that entry's own commit
timestamp. The only thing that differs from `KindBatch`'s own case is
*which* log entry does the materializing — `KindBatch` materializes at the
entry that proposed the write; a transaction defers materialization to the
entry that resolves it, since only that entry's position is guaranteed
monotone across every consumer's own watermark (this document's own §(a)
argument, restated: a consumer offset must never be assigned from a moment
earlier than the entry that actually fixes it in commit order). Full
mechanism, forks, and incidental bugs: `docs/adr/0018-cross-tablet-
transactions.md`'s 2026-08-16 amendment.

**The shared-helper rule (Consequences, "One shared materialization
function") is enforced exactly as written, not merely aspired to.**
`materialize_derived` is the one function both `KindBatch`'s apply arm and
`TxnResolve`'s commit branch call — `KvCommand::KindBatch`'s own arm was
refactored to call it too, rather than gaining a sibling copy, in the same
change that added `TxnResolve`'s call. `animus-cp-data/tests/
txn_kind_writes.rs::kind_batch_and_txn_resolve_materialize_byte_identical_
rows_for_identical_payloads` is the regression: an identical `(kind, key,
value)` payload staged through each of the two paths produces byte-
identical stored rows. This is also the concrete instance of principle 5
(replay/snapshot-stability) the shared-helper rule protects: two
independently-maintained copies are exactly the kind of drift that would
let one code path's apply-time effect diverge from the other's after either
was touched alone, even though both replay the identical logical operation.

Decision 1 (U3) itself: the cross-node LSI orphan-row race this ADR left
open is closed by `animusd::dynamo::kind_write_item_at_leader`
(non-transactional path) and `eval_kind_txn_write`/`ClientCtx::
txn_stage_local` (transactional path, the same U3 shape applied to a
write staged inside a 2PC) — both funnel every write of one item through
one node's own `rmw_lock`, regardless of which edge node received the
request.

## As-built amendment (2026-08-16) — the consumer-offset consolidation

Context §(a)'s "three independent consumer-offset implementations" is now
one named rule with one shared implementation and one explicit
classification, delivered as the small, scoped-down version the approved
plan called for — not the deeper storage unification the plan explicitly
rejected as over-unification (dependency direction, `animus-cp-data` →
`animus-control` never the reverse, and plane separation — replicated
`Metadata` vs per-tablet engine rows — forbid a shared storage home; what's
shared is the *rule*, not the implementation).

**Principle 3, factored out.** `animus_tablet::split_basis::effective(own,
frozen_basis)` (`own.or_else(|| frozen_basis.cloned())`) is the one generic
form of "a split is a log cut; every consumer offset crossing it inherits
from a basis frozen at the cut, never a live re-derivation from the
parent's later state." `Metadata::effective_stream_shard_watermark`
(`animus-control`) is its first caller, ported to a one-line wrapper around
its own two lookups with no behavior change (regression:
`meta::tests::effective_stream_shard_watermark_inherits_through_split_
provenance`, `stream_shard_parent_id_is_frozen_at_split_time_not_the_
parents_current_chain`, and `animus-test`'s `stream_lineage_corpus::
split_then_parent_seals_first`).

**One value type, still two conventions.** `animus_cp_data::cursor::
ConsumerOffset { Watermark(HlcTimestamp), KeyPos(Vec<u8>) }` is an
additive wrapper over the two byte conventions the module doc already
documented side by side (the packed-HLC watermark and the raw
last-scanned-base-key) — for a future generic consumer (a per-CQL-CDC
cursor is the concrete case the module doc names) that wants to hold
either shape without hard-coding its own tag's convention. It delegates to
the pre-existing `encode_watermark`/`decode_watermark`/
`encode_backfill_cursor`/`decode_backfill_cursor` free functions rather
than duplicating either encoding; those functions and every existing
caller are untouched, consistent with principle 5 (no second,
independently-maintained copy of an encoding to drift from the first).

**Split-policy classification, made explicit.** `animus_cp_data::cursor::
SplitPolicy { RestartFromScratch, InheritFrozenBasis }` names the two
outcomes principle 3 leaves open for a consumer offset, plus a
`classify_tag` function and a module-doc table pairing every known
`KIND_CURSOR` tag with its policy: `"gsi"` and `"backfill:{index_name}"`
both classify `RestartFromScratch` (their cursor row simply reads empty
for a split's right child — `cursor_key` embeds `range.start`,
`narrow_scope` never moves rows — and each consumer's own idempotent
reconciliation makes "restart from scratch" unconditionally safe, ADR 0045
§5 Fork A/F1); the stream seal watermark is the one
`InheritFrozenBasis` case, but it is a doc-level table entry only — it
lives in the control plane's replicated `Metadata`, not a `KIND_CURSOR`
row, so `classify_tag` (a data-plane crate) has no row of its own to
classify. This is **not** a runtime cross-plane registry — the plan's own
non-goal — precisely because the dependency direction that forbids a
shared storage home (above) equally forbids a shared runtime classifier;
the table is the one place a human reads both halves together. The
regression is `cursor`'s own `every_known_cursor_tag_prefix_is_classified`
test: it enumerates every tag this crate constructs today and asserts each
maps to a policy, with a failure message that tells whoever adds a third
tag to classify it — deliberately by hand, since a new tag is exactly the
moment ADR 0046 principle 3 needs a conscious answer, not a default.
`animusd::index_drain`'s own module doc cross-references this table from
the two arms (the GSI drain, the backfill seeder) that own those tags.
