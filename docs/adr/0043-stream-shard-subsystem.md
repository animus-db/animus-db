# ADR 0043 — In-place stream sealing and the `SegmentStore`

- **Status:** Accepted — implemented (round-3 stack). PR map: PR0 salvage →
  PR1 ADR 0042 + this ADR's rewrite → PR2 `SegmentStore` trait/`Sim`/`Fs` →
  PR3 `ClusterSegmentStore` → PR4 segment codec + shard catalog + merge
  guard → PR5 the sealer → PR6 read path + wire API → PR7 segment janitor →
  PR8 lineage corpus + `ProdEnv` e2e + nightly (this PR).
- **Date:** 2026-08-14 (round-3 rewrite, retitled from "The stream-shard
  subsystem" — that title described round 2's separate shard-tablet design,
  which this text replaces in place. Round 1/2 text is retrievable from git
  history and the `adr-0042/5`/`adr-0042/6` archive branches, kept for
  exactly that reason.)
- **Amends:** [ADR 0028](0028-shared-storage-single-command-split.md) (no new
  kind scope: the hot shard is the existing `KIND_CHANGE` scope, unchanged;
  the kind set stays at five), [ADR 0033](0033-tablet-merge.md)/
  [ADR 0044](0044-split-only-tablets.md) and
  [ADR 0034](0034-byte-based-auto-split.md) (a streamed base table's own
  tablets are ordinary tablets — merge was rejected outright as a v1 stopgap
  (ADR 0042 §12), then removed entirely by ADR 0044; auto-split is what
  drives shard lineage, token-aligned per ADR 0042 §14),
  [ADR 0024](0024-drop-table-data-gc.md) (drop cascade removes a
  streamed table's segment catalog rows and objects, not a hidden table's
  tablets), [ADR 0013](0013-replicated-schemas.md) (`StreamSpec` replicates
  in the catalog; this ADR adds the segment catalog rows alongside it),
  [ADR 0035](0035-control-plane-separate-deployment.md) (round 3 needs **no**
  dedicated streams node role — superseded, see that ADR's amendment).
- **Depends on:** [ADR 0042](0042-dynamo-streams.md) (the contract this
  subsystem serves), [ADR 0018](0018-cross-tablet-transactions.md) (HLC
  timestamps, apply-time monotonicity, the packed-HLC MVCC version), [ADR
  0031](0031-tablet-host-reconciler.md) (the per-node tablet-host reconciler
  this subsystem still needs zero new code from — nothing here is a new
  kind of tablet), [ADR 0041](0041-materialized-secondary-indexes.md) (the
  `KIND_CHANGE` log and the unified per-node change-consumer loop this ADR's
  seal arm joins).

## Context: three rounds, each one closer to what AWS actually built

**Round 1** (the original plan, never fully built) and **round 2** (shipped,
then deliberately dropped) both modeled a DynamoDB stream shard as the
tablet of a **separate, hidden per-stream table** — `T$streams$<label>`, a
fixed number of equal token-range tablets, placed on a dedicated **streams**
node role, populated by a **stream copier**: a third arm alongside the ADR
0041 GSI drain that read a source tablet's change log and forwarded batches
into the shard tablets via two new `KvCommand`s
(`StreamAppend`/`StreamTrim`), three new row kinds, and a per-partition-key
dedupe row guarding the copier's own retries. This was a coherent design —
every one of those pieces is preserved, commits intact, on the
`adr-0042/5`/`adr-0042/6` archive branches — and it inherited a lot for
free (placement, snapshotting, membership change, drop-GC all "just tablet
machinery"). But it was also **a bigger structural departure from what
DynamoDB itself does than the feature needed**: real DynamoDB Streams has no
separate copying tier at all — a table partition (and hence its shard)
*splits* under exactly the same trigger that grows the base table, and a
shard closes and reopens in place, not by forwarding records somewhere else.

**Round 2's own committed roadmap item (a)** — "automatic shard growth via
generation-cut resharding" — was already trying to reproduce, by hand, the
exact lineage behavior a tablet's own auto-split already gives for free.
That was the tell. The owner's round-3 pivot (2026-08-14/06, this ADR's own
decisions) collapses the copying tier entirely: **a stream shard is not a
separate thing the data plane copies records *into* — it is what the data
plane's own change log *becomes* once sealed.** The transactional data plane
creates shards (by writing to `KIND_CHANGE`, which it already does); a
per-tablet seal arm, triggered on size or age, closes the current epoch in
place and ships it to an external store; the tablet's own leader just keeps
writing to a fresh epoch. No copier, no second Raft group, no dedupe row —
because there is no second write path for any of those to guard.

This also changes *where* sealed records live: round 2 kept everything,
hot and sealed, in the same `StorageEngine`-backed kind scopes every other
row lives in (rejected a "bespoke segment store" explicitly, in its own
Rejected Alternatives). **Round 3 reverses that specific call for sealed
data only** — a real, versioned segment format behind a new `SegmentStore`
trait, with a cluster-replicated default implementation that upholds this
database's own durability bar (K-way replication, not "however durable the
object store happens to be"). Hot data stays exactly where ADR 0041 already
put it: the tablet's own Raft-replicated `KIND_CHANGE` scope, untouched.

Verified against `main` at `65cd9d8` (the round-3 salvage boundary — round
2's `36884c1`/`3a7e0ac` were reset out, keeping only the `$`-name-rejection
guard they also touched, `RESERVED_TABLE_NAME_SEPARATOR` in
`animus-control/src/meta.rs`):

- `KvCommand::KindBatch` (`animus-cp-data/src/lib.rs`) already completes a
  change-record's key with `hlc::pack(ts)` **at apply**, from the entry's
  own commit timestamp — "structural, apply-assigned suffix" is not new
  machinery this ADR introduces; it is the existing mechanism this ADR's
  seal step simply reads back out in bulk.
- `assert_ts_monotonic` (same file) is the hard invariant that makes a
  tablet's own `KIND_CHANGE` scope a genuinely ordered log in the first
  place — every apply strictly increases the group's own `ts`, so "sort by
  the log's own key order" and "sort by commit order" already agree within
  one partition (not across partitions — `pending_changes`' own doc is
  explicit that its key order is token-then-pk-then-HLC, not global commit
  order, which is exactly why a seal must re-sort by the HLC suffix, §A3).
- `table_takes_kind_write_path` (`animusd/src/dynamo.rs`) already routes a
  streamed-but-unindexed table's writes through `KindBatch`, producing
  exactly base row + change record — the round-3 hot shard's content, with
  no code left to write for that half.
- `cursor.rs`'s own module doc (`animus-cp-data`) already states the plan
  this ADR executes: *"Round 3 has no separate stream copier or `"copier"`
  cursor row: the eventual sealer reads a table's own `KIND_CHANGE` change
  log directly."* `index_drain.rs`'s `expected_consumer_tags` carries a
  `// round-3 sealer PR:` marker at the exact line this ADR's sealer PR
  (PR 5) replaces.
- `host.rs` (the ADR 0031 per-node tablet-host reconciler) needs **zero new
  code** from this ADR — unlike round 2, there is no new kind of tablet at
  all. A streamed table's tablets are ordinary tablets; the only new
  mechanism is what a tablet's own leader does with its own `KIND_CHANGE`
  scope on a background tick.

## Decision

**A stream shard is a seal epoch of its source tablet's own `KIND_CHANGE`
scope — sealed in place by that tablet's leader, on size/age triggers,
shipped to a `SegmentStore` and recorded in the replicated segment catalog.
No new row kind, no new tablet, no copier. Auto-split is the only mechanism
that ever creates a new tablet, and hence a new shard lineage branch; the
per-tablet change-consumer loop (ADR 0041's GSI drain) gains a second arm,
the **sealer**, and its existing hot-log trim arm is generalized to derive
a streamed table's watermark from the segment catalog instead of the
retired `"copier"` cursor tag (ADR 0042 §8). A separate, control-plane-
leader **segment-janitor** loop (§A9) handles already-sealed data —
retention and replica repair — a cluster-wide catalog concern distinct from
any one tablet's own hot-log housekeeping.**

### A1. Hot shard = the source tablet's change log

The ADR 0041 change log *is* the hot stream: `KvCommand::KindBatch` writes
one non-collapsing record per mutation into `KIND_CHANGE`, key
`token || escape(pk) || hlc::pack(ts)`, `ts` completed at apply from the
entry's own commit timestamp, both images (`ChangeRecord`).
`assert_ts_monotonic` guarantees per-tablet HLC order equals arrival order
equals per-item order. `table_takes_kind_write_path` already routes every
streamed table here, indexed or not. No copier, no second Raft group, no
dedupe rows — exactly-once is structural, inherited straight from the
apply-time atomicity `KindBatch` already provides. A hot poll's cost is
O(the tablet's own hot scope), bounded by the seal knobs (ADR 0042 §13).

### A2. Shard = seal epoch

Epoch is the chain length — catalog-derived, so a crash mid-seal that
retries recomputes the identical epoch rather than skipping or duplicating
one. `ParentShardId`: a routine seal's child names the same tablet's own
previous epoch; a split child's epoch-0 shard names the *parent tablet's*
last shard (§A4). Sealing does not invalidate an open-shard iterator:
`GetRecords` resolves the shard id against the catalog at serve time — a
sealed shard is fetched from the store, an open one is scanned hot; an
iterator that was open when minted simply drains the resulting segment and
nulls, and the consumer walks to the child per the ordinary lineage
discipline (ADR 0042 §2).

### A3. Seal mechanics

**Who**: the source tablet's own leader, via a second arm of the per-node
`change_consumer_loop` (the renamed `index_drain_loop`): ADR 0041's GSI
drain arm is unchanged, joined by this ADR's **seal arm**; that same loop's
existing **hot-trim arm** is generalized to derive a streamed table's
watermark from the segment catalog instead of the retired `"copier"`
cursor-tag row (ADR 0042 §8). The **segment-janitor** (§A9) is a distinct,
control-plane-leader loop — retention and repair of already-*sealed*
segments are cluster-wide catalog concerns, not any one tablet's own
hot-log housekeeping, and are never performed by this per-tablet loop.

**When**: `KIND_CHANGE` scope `approx_bytes` exceeds `--stream-seal-bytes`,
**or** the oldest unsealed record's age exceeds `--stream-seal-age` (the
loop's own `env` clock, never wall-clock directly — ADR 0003), **or** a
disable-triggered final seal fires (ADR 0042 §11's F12-b grace).

**Sequence (durable-before-visible, mirroring every other apply-then-durable
discipline in this crate):**

1. Let `W` be the tablet's current effective watermark (ADR 0042 §8: its
   own shard chain's last sealed end-HLC, or its parent chain's for a fresh
   split child, or absent). Scan `pending_changes()`, keep every record with
   `hlc > W`, and **sort by the 8-byte HLC key suffix** — `pending_changes`'
   own key order is token-then-pk-then-HLC, *not* commit order, so this sort
   is load-bearing, not a formality. Encode the segment (§"Segment format,"
   below).
2. `SegmentStore::put(id)` at the deterministic id
   `{table}/{label}/{tablet}/{epoch}`. With the default `ClusterSegmentStore`
   (§A5) this is the K-replica push; `Ok` means every replica fsynced.
3. Propose `MetaCommand::SealStreamShard { table, label, tablet, epoch,
   hlc_range, count, wall_ms, replicas }` — apply-guarded
   **first-committer-wins on `(tablet, epoch)`** (mirroring `CreateTablet`'s
   own race-safety shape, generalized from "first tablet" to "first seal of
   this epoch"). Commit is what makes the shard visible and advances the
   trim watermark — never the `put` alone (ADR 0042 §9).
4. Hot records `≤` the new watermark are deleted **later**, by the segment
   janitor (§A6/A9), never by the seal step itself.

**Recovery**: a crash before step 3 simply re-runs steps 1–3 on the next
tick — the id is deterministic, so the retried `put` overwrites the same
object; a **superset** overwrite (the retried scan sees a couple more
records than the first attempt did) is safe precisely because readers slice
to the *committed* `hlc_range`, never the raw object (ADR 0042 §10, and see
"the superset-slice rule" test scenario below). A crash after step 3 simply
means the janitor is what trims, on its own next tick. If the store is
unavailable, the hot scope keeps growing (bounded by disk, metered loudly)
until it heals, then seals normally — never a stuck or lost write, per the
durability invariant (ADR 0042 §9).

**Segment format** (`animus-cp-data/src/segment.rs`, new in the sealer PR):
a versioned header — `{version, table, label, shard id, tablet, epoch,
parent shard id, hlc_range, count, seal wall-ms}` — followed by a body of
length-prefixed `{source_key, packed hlc, change_record bytes}` triples in
HLC order. `change_record` stays opaque to `animus-cp-data` (the same
`ChangeRecord` type `animus-dynamo`/`animusd` already own); this crate only
ever moves its bytes.

### A4. Split lineage — records never move

Kind scopes share the tablet's live `KeyRange`: after `SplitTablet`, the
left child (the same tablet id, its own already-open shard continuing
uninterrupted — **the split never closes or re-seals anything**) keeps
left-range records in place; the right sibling's fresh epoch-0 open shard
exposes right-range records, likewise in place — nothing is copied. Each
child's **epoch-0** shard carries `ParentShardId` = the parent's own
last-*sealed* shard, and each child's **initial watermark** is the parent
chain's own last sealed end-HLC (mirroring exactly how the GSI cursor's
min-over-rows rule already treats a fresh split child at `W = 0` over an
empty row set — here it's `W = parent's last sealed end-HLC`, not zero,
because the parent's own sealed segments are shared history both children
inherit, not each child's own to re-derive) — **both frozen once, at the
split's own apply, into `Metadata::stream_split_basis`** (PR1 amendment
below), not re-derived from the parent's live chain on every later read.

**PR1 bugfix, 2026-08-15 (a live-derivation data-loss bug, found and fixed
after PR8 shipped).** The two values above used to be derived *live*
instead of frozen: `effective_stream_shard_watermark`/
`stream_shard_parent_id` walked `Metadata::split_parents` to the parent's
**current** chain on every call. That is correct only so long as the
parent never seals again before the child does. The moment it does, the
parent's later seal's end-HLC — necessarily higher, since a tablet's own
`KIND_CHANGE` scope only ever advances — became the child's own effective
watermark too, retroactively appearing to have already sealed a pre-split
backlog the child had physically inherited in place (this section's own
"records never move" design) but had not yet sealed itself. The child's
own first seal then silently filtered that backlog out (`hlc <=
watermark`), and the same inflated watermark blocked it from the open-tail
read path as well — a permanent, silent loss, invisible unless the child
happened to seal before the parent did (the race that let this ship
undetected). The fix freezes both values once, at the instant
`MetaCommand::SplitTablet` applies, into a new sibling map,
`Metadata::stream_split_basis: BTreeMap<TabletId, StreamSplitBasis>` —
`split_parents` itself is untouched (ADR 0018's range-seal and the
tablet-host reconciler still consume it). No new `MetaCommand` (§A8's
"exactly two commands" claim is about `SealStreamShard`/
`ExpireStreamShards`, and still holds). Regression:
`animus-test`'s `stream_lineage_corpus.rs::split_then_parent_seals_first`
(the deliberate inverse of `split_mid_stream`'s ordering, confirmed to
reproduce the loss on the unfixed code) and `animusd`'s `streams_e2e.rs::
manual_split_with_unsealed_backlog_under_production_seal_knobs`.

**Cross-group HLC safety**: a child group's start witnesses the shared
engine's own `latest_version()` (the pre-existing witnessing chain ADR
0018 §2's amendment already establishes) — nothing stream-specific to add
here. **Auto-scaling is tablet topology, full stop**: auto-split is the
only event that ever creates a new shard-lineage branch; stream
parallelism *is* tablet count, with no separate resharding mechanism, knob,
or command. ADR 0042 §14's F11 rounds a streamed table's split key down to
its own token boundary, preserving the partition-key/shard affinity a
change record's own token-leading key already assumes.

### A5. Merge (removed, ADR 0044)

Rejected on a streamed base table in v1 (ADR 0042 §12's F1 stopgap) — the
same class of apply-time guard `MergeTablets`'s own state-machine arm got
for other invariants it protected. Tablet merge — `MergeTablets` and this
guard along with it — was then removed entirely (ADR 0044, tablets are
split-only, decided 2026-08-14, shipped 2026-08-14): there is no merge to
reject anymore, on a streamed table or any other. ADR 0044 also closes the
door on reviving it: any future count-reduction story for a streamed (or
any) table is a from-scratch redesign, never a merge revival, so the
escape hatch this section used to sketch (an `AdjacentParentShardId`-style
lineage extension plus a range-aware survivor watermark, "if merge is ever
revived") is moot and not carried forward.

### A6. Watermark + trim (F10)

The stream half of the trim computation (ADR 0042 §8) is entirely
catalog-derived: a tablet's watermark is its own chain's last sealed
end-HLC, or — for a fresh split child with no rows of its own yet — its
parent's, **frozen at split time** (§A4's PR1 amendment; never re-derived
from the parent's chain as it stands *now*), or absent if it has never
sealed. The GSI half (`"gsi"` cursor tag, min-over-rows) is
**completely unchanged** — ADR 0041's drain still owns it, still advances
it in its own trailing write, still generalizes correctly across a
split/merge. `expected_consumer_tags` drops `"copier"` and the row it used
to gate on; nothing ever writes a `"copier"` `KIND_CURSOR` row again. A
`DISABLED`-but-not-yet-reaped stream (ADR 0042 §11's F12-b grace) does
**not** block hot-scope trim on the table's still-live records — its own
records are already sealed by the final-seal step, so the only term left
for the hot-trim arm to honor on that table is the GSI's, if any. Sealing
itself never deletes hot records; the same per-tablet loop's **hot-trim
arm** (generalized from ADR 0041's own trim janitor) is the only deleter of
hot records, and only once the segment + catalog row it trims behind are
both durably committed — a distinct action from the control-leader
**segment-janitor** (§A9), which only ever touches already-sealed segment
objects and rows, never a tablet's own hot `KIND_CHANGE` scope.

### A7. The `SegmentStore` trait

```rust
#[async_trait] pub trait SegmentStore: Send + Sync {
    async fn put(&self, id: &str, bytes: &[u8]) -> io::Result<()>; // durable per impl contract on Ok; idempotent overwrite
    async fn get(&self, id: &str) -> io::Result<Option<Vec<u8>>>;  // None = deleted → TrimmedDataAccess
    async fn delete(&self, id: &str) -> io::Result<()>;            // idempotent
    async fn list(&self, prefix: &str) -> io::Result<Vec<String>>; // debug/sweep only, never load-bearing for reads
}
```

Lives in `animus-env` beside the other seams (`Clock`/`Rng`/`Network`/
`Disk`/`Spawner`), but is **not** folded into the `Env` supertrait — every
call site threads an explicit handle, the same way a `StorageEngine`
handle is threaded rather than made part of `Env` itself. **Consistency
contract**: read-after-put for every reader once `put` returns `Ok`;
immutable once cataloged, modulo the superset-slice rule (ADR 0042 §10);
`get` returning `None` after a `delete` is a defined, expected outcome
(`TrimmedDataAccess` to a client), never an error.

**Implementations**:

- **`SimSegmentStore`** (`animus-sim`) — seeded, fault-injectable (ack-lost
  puts, unavailability windows, partial-K delivery for the cluster variant
  below) — the corpus's own store.
- **`ClusterSegmentStore`** — **the default** (§ below).
- **`FsSegmentStore`** — a single local directory, temp-write + rename +
  fsync; demoted to explicit opt-in (`--segment-store=dir:...`) for dev or a
  shared mount, and reused internally as `ClusterSegmentStore`'s own
  per-node local building block.

### A7b. The default store must uphold this database's own durability bar (F5)

**`ClusterSegmentStore` is the default, deliberately, because a stream's
sealed records are exactly as much "the database's own data" as anything
else it holds — a default store any less durable than the rest of this
system would be a durability *regression* for users who enable Streams,
not a neutral convenience.** Concretely:

- **K-way replication**, `K = RF` (default 3) — an immutable segment is
  pushed to K nodes' own local segment directories (each backed by the same
  `FsSegmentStore` building block).
- **Placement** is chosen via the *existing* placement policy machinery
  (`animus-placement`'s failure-domain spread, ADR 0005) — no new policy
  engine — and the chosen replica set is **recorded in the `SealStreamShard`
  catalog row itself** (the `replicas` field, §A3 step 3), so any future
  reader or repair sweep knows exactly where to look without a discovery
  round.
- **`put` returns `Ok` only once all K nodes have fsynced** (temp-write +
  rename, the same discipline `FsSegmentStore` already uses per-node) — this
  is what makes the durability invariant (ADR 0042 §9) actually hold, not
  merely a documented aspiration.
- **`get`** serves from any recorded replica, preferring a local fetch when
  the requesting node happens to be one.
- **Repair** is a control-leader sweep re-replicating an under-replicated
  segment from a surviving replica — "a dumb copy," since a segment is
  immutable once cataloged; it folds into the same segment-janitor loop
  retention already needs (§A9, F9's own merge).
- **Delete is idempotent** and reaped by the same sweep, never a
  synchronous part of the retention decision itself.
- **`ClusterSegmentStore` is cluster code written over the `Env`
  network/disk seams** (ADR 0003) — like every other distributed mechanism
  in this codebase, it is fully `SimEnv`-fault-injectable (partial-K
  delivery, node death mid-put, network partition during repair), not a
  production-only integration surface tested by hand.
- **S3 is a future trait swap, not a requirement** — a durability
  *upgrade* over the cluster-replicated default for an operator who wants
  it, never a dependency this ADR's own correctness relies on.

### A8. The segment catalog lives in replicated `Metadata` (F3/F7)

`MetaCommand::SealStreamShard` (the seal's own commit, §A3 step 3) and
`MetaCommand::ExpireStreamShards` (the janitor's own reclaim, §A9) are the
only two new `MetaCommand`s this whole subsystem needs — no new
`syskv::EntityKind`, since a `StreamSpec` already rides inside the existing
table-schema catalog entry (ADR 0013) and a segment row is a small,
self-contained addition beside it. `SealStreamShard` is
**first-committer-wins on `(tablet, epoch)`'s content** (round-3 PR7
amendment — see §A9's own "replicas-update decision" for the full
argument) — the same race-safety shape `CreateTablet`'s own apply arm
already established for "the first proposal for this identity wins,"
generalized from a tablet id to a `(tablet, epoch)` pair, with one
deliberate carve-out: a second proposal whose content matches exactly but
whose `replicas` differs is a genuine in-place update (the segment
janitor's own repair sweep), never treated as a conflicting second
committer. `DescribeStream`/`ListStreams` (ADR 0042 §3) are
pure functions of this catalog plus the tablet map — **the store is never
load-bearing for a metadata read** (F7), matching how `ListStreams` never
touched a shard tablet's own state even in round 2.

**Why the catalog, and not a per-tablet manifest object or the store's own
`list()`**: a manifest living *in* the store would make metadata reads pay
a store round trip for something `Metadata`'s replicated, always-consistent
state already answers for free, and would need its own separate consistency
story across K replicas. `list()` is explicitly documented as "debug/sweep
only, never load-bearing" (§A7) precisely so no code path is ever tempted to
treat an object listing as authoritative over what the catalog says exists —
an object store's listing consistency is, in general, weaker than a
replicated Raft log's, and this design never needs to lean on it.

### A9. The segment-janitor loop (F9, merging retention with replica repair — round-3 PR7)

A **distinct loop, run on the control-plane leader** (`animusd::
segment_janitor::segment_janitor_loop`) — not an arm of the per-tablet
`change_consumer_loop` (§A3), since retention and repair are cluster-wide
concerns over the catalog as a whole, not per-source-tablet ones. Spawned
unconditionally on every node shape that can ever become the control-plane
leader (combined, and control-only — ADR 0035), self-gating every tick on
whether *this* node currently holds a live control `RaftNode` believed to
be leader — the identical pattern `detect_loop`/`orphan_sweep_loop`
(`animus-control`) already use, generalized one layer up because this loop
also needs a `SegmentStore` handle those two never do.

**Two-phase retention**, over every catalog row: mark expired
(`ExpireStreamShards{remove: false}`) once past `--stream-retention` (age
from the row's own `seal_wall_ms`) *or* once its table has been dropped
entirely (the drop-table cascade's own rule, below); then, for every marked
row, delete the segment object at every recorded replica still present in
the cluster's own membership and, once that succeeds (or there was nothing
reachable left to delete), physically remove the row
(`ExpireStreamShards{remove: true}`). Every step is idempotent, so a crash
mid-sweep simply resumes on the next tick with no special recovery logic.

**The epoch-derivation guard (a correctness-preserving exception the
retention rule above must never violate).** A tablet's next seal epoch
(`index_drain::seal_now`'s `next_epoch`) and its current open epoch
(`dynamo_streams::current_open_epoch`) are both *derived*, not counted from
an independent monotonic source — "the chain's own highest-numbered
existing row, plus one" (§A2's own "epoch = the chain length"). That was a
safe design for a catalog that only ever *grows*; retention's whole point
is to physically *shrink* it. If the janitor ever removed a tablet's own
**current highest-epoch row** while that tablet still exists (still in the
tablet map — a table drop is the one case it doesn't), a future seal would
silently recompute the *identical* epoch number for genuinely new data —
two different segments both claiming to be `shardId-<tablet>-<epoch>` at
different points in time, with no way for a reader to tell them apart. The
janitor therefore **never physically removes a tablet's own current
highest-epoch row while that tablet still exists** — only its *object* may
be deleted (safe unconditionally: nothing about epoch derivation reads
object bytes, only the catalog row's own numeric fields), and the row stays
marked `expired` (already invisible to `DescribeStream`'s enumeration, so
this is externally indistinguishable from full reclaim) until either the
tablet seals past it — no longer the max, now ordinarily reclaimable — or
the tablet itself is dropped. A quiet, idle tail shard can therefore sit
"expired, object gone, row retained" indefinitely; this is an accepted,
self-healing residual (see `docs/engineering-lessons.md`), not a leak a
future seal ever needs to clean up by hand.

**The dead-replica deletion rule.** A recorded replica counts as
**confirmed-absent** (no delete owed) only once it has been **removed from
the cluster's own membership entirely** (decommissioned) — a merely `Down`
member still gets a genuine delete attempt on every tick, since it might
come back with its copy intact and "confirmed" must mean confirmed, not
assumed. The corollary, accepted deliberately: a member that crashes
**permanently but is never decommissioned** blocks that row's *physical*
row-removal forever (its object delete can never succeed, since
`ClusterSegmentStore::delete_from` is all-or-error across every reachable
recorded replica) — the row stays marked `expired` (so already correctly
invisible/inaccessible to readers) but never reaches `remove: true`. This
is a durability-over-availability tradeoff, not a bug: the operational
remedy is decommissioning the dead node, which is the honest signal that
its data is truly gone for good, not an inference this loop should ever
make unilaterally from a mere heartbeat timeout.

**Replica repair (F5's own mandate) runs in the same loop, over every
still-*live* (unexpired) row with a non-empty `replicas`** (a
`ClusterSegmentStore`-backed row — the single-directory `FsSegmentStore`
opt-in always records an empty list and has no per-replica concept to
repair, §A7b): verify every recorded replica is a current `Active` member;
for however many are not, fetch a live copy from whichever recorded
replicas *are* `Active` (`ClusterSegmentStore::get_from`) and push it
(`ClusterSegmentStore::repair`) to enough freshly-chosen candidates —
excluding every id already recorded — to restore the row's own original
replica count, degrading gracefully if fewer candidates exist (mirroring
`choose_targets`'s own degraded-mode philosophy) and re-attempting on a
later tick once membership recovers. F9's original plan (retention alone)
and F5's own repair mandate turn out to be the same loop once the catalog
exists to drive both from one snapshot, so they ship together rather than
as two competing background tasks. Repair never touches an expired row
(reclaimed by this same loop anyway) and never resurrects a genuinely
deleted object.

**The replicas-update decision (round-3 PR7 amendment to §A8's
`SealStreamShard` apply arm).** Repair needs to commit an updated
`replicas` set for an *already-committed* `(tablet, epoch)` — a shape
`SealStreamShard`'s original "first-committer-wins on `(tablet, epoch)`"
design (§A8) treated as an unconditional no-op. Rather than adding a third
`MetaCommand` (rejected: this subsystem's whole design goal is exactly two
commands, and a replicas-only update is the *same* underlying fact
`SealStreamShard` already records, just refreshed), the apply arm now
evaluates first-committer-wins on the row's **content** (everything but
`replicas`): a second proposal for an already-recorded identity whose
content matches exactly is `Applied` — a genuine in-place `replicas`
update — if `replicas` differs, or a true no-op if it doesn't (the sealer's
own crash-retry, unchanged); a proposal whose *content* genuinely conflicts
is still rejected as a no-op, exactly as originally designed. This is safe
for every reader because both `GetRecords` and this loop always re-fetch
the row fresh before consulting `replicas` — an in-place update is observed
atomically, never a torn read of a half-updated set — and the repair sweep
is the *only* production caller that ever proposes a genuinely different
`replicas` for an existing identity, so there is no other writer to race
against.

**Disable (F12-b, ADR 0042 §11) needs no dedicated janitor path** — the
disable-triggered final seal (§A3) already moves every record to the
sealed tier before the write gate closes, so a `DISABLED` stream's rows and
objects simply age out through this same ordinary sweep, on the same
timeline as any other stream's retention.

**Drop table is a convergent design, not a dedicated code path.**
`ExpireStreamShards` is deliberately **not relayable** (§A8) — its only
sanctioned caller is this control-plane-leader-only loop, which already
holds a live `RaftNode` handle. `ClientCtx::drop_table` runs on whichever
node a client happens to connect to, essentially never guaranteed to be
that leader, so it cannot reliably propose `ExpireStreamShards` itself.
Rather than adding a leader-only special case to `drop_table` (reachable
only when lucky, and a second copy of this loop's own two-phase decision
when it *is*), the janitor's own retention-expiry rule treats a row whose
table's schema no longer exists at all (`Metadata::table_schema(&row.table)
.is_none()`) as **immediately due**, regardless of age — "retention `0`"
for a table that no longer exists to protect. `drop_table`'s existing
cascade (dropping the schema, then the tablets) is exactly what flips this
condition; no new command, no new code path in `drop_table` at all, and the
epoch-derivation guard above never blocks this case specifically, because a
dropped table's tablets have also left the tablet map by the time this
condition is ever true (`!meta.tablets.contains_key(tablet)`), so even a
tablet's own last epoch is safely, fully reclaimable once its table is
truly gone.

**The control-only-leader scope gap.** Retention *marking* and the
drop-table rule above need only `Metadata` — cheap on any node that can
become control leader. Object deletion and replica repair need a
`SegmentStoreHandle`, which today only exists on a node with a data role.
A **control-only** leader (a genuine ADR 0035 split deployment) therefore
marks rows and reacts to drops correctly, but cannot physically delete
objects or repair replicas for as long as it leads — rows accumulate,
marked but un-reclaimed, until a data-role node takes over leadership
instead. In a **pure** split deployment (control-only nodes are the *only*
control voters), this never happens at all today. This is a real,
deliberate deferral — extending `SegmentStoreHandle` provisioning to a
control-only node is its own follow-up, out of this PR's scope — not a
correctness bug: a marked row is already invisible to `DescribeStream` and,
once its object happens to be gone, inaccessible via `GetRecords` too; the
residual is purely "this specific node shape's own leadership stint cannot
finish the physical reclaim," never a stale or incorrect read.

### Rejected alternatives

**Round 2's whole architecture** (a separate hidden per-stream table, a
dedicated streams node role, a copier, per-partition dedupe rows,
`StreamAppend`/`StreamTrim`, `KIND_STREAM`/`KIND_STREAM_META`). Not wrong,
exactly — it worked, and its code is preserved on the `adr-0042/5`/
`adr-0042/6` archive branches precisely because it was a genuine, tested
design. Superseded because it was **more distributed-systems machinery than
the feature actually needs**, once seen through the lens of what DynamoDB
Streams itself is: not a separate service with its own copying tier, but a
view of the table's own replication log that seals in place. Round 2's own
committed growth roadmap (generation-cut resharding, reproducing
auto-split's lineage by hand) was the concrete symptom that motivated
revisiting the whole shape rather than patching that one item.

**Keeping sealed data in `StorageEngine` kind scopes** (round 2's own
choice, and this ADR's one deliberate reversal of it). Round 2 rejected a
bespoke segment store because durability/crash-recovery/snapshot-shipping
"come for free" from the engine. That argument is sound for **hot** data —
and this ADR keeps it there, unchanged. It is a worse fit for **sealed,
immutable, potentially-large, rarely-read** data: an engine's compaction
and snapshot-shipping machinery is built for mutable, actively-read state,
not a write-once object a retention sweep eventually deletes wholesale — a
real segment store's replication/repair model (§A5) is a better-fitting
tool for exactly that shape, and the durability mandate (F5) means "simpler
to implement" was never on the table as the deciding factor either way.

**A dedicated streams node role** (round 2's own placement decision, ADR
0035's amendment). No longer needed: there is no separate shard tablet to
place at all, so the whole payload-profile-separation argument that
motivated it is moot. See ADR 0035's own amendment for the retraction.

**Slot-indirection / Kafka-style add-only partitions for growth** (round
2's own rejected alternatives to its generation-cut design). Both are now
moot for the same reason round 2's whole architecture is superseded — there
is no fixed shard count to grow at all, since a shard is simply a seal
epoch of whatever tablet exists at the time.

## Consequences

**Easier.**

- **No new distributed machinery of any kind.** A streamed table's tablets
  are ordinary tablets — hosted, split, placed, snapshotted, and reclaimed
  by mechanisms this codebase already has fully tested. The only genuinely
  new code is a background loop arm and a segment codec/store.
- **Growth is free and automatic**, inherited entirely from auto-split —
  no resharding command, no shard-count knob, no generation-cut lineage
  rule to design or test separately from the tablet lifecycle itself.
- **Exactly-once needs no dedupe state** — it was never really a stream
  property to enforce; it was always the underlying `KindBatch` atomicity
  ADR 0041 already built and tested.

**Harder, and knowingly accepted.**

- **A real external store is now a load-bearing dependency for a stream's
  sealed data** — `ClusterSegmentStore`'s own K-replica put/repair
  machinery is genuinely new distributed code, fault-injection-tested from
  scratch (PR 3), not inherited from an existing primitive the way
  everything else in this ADR is.
- **The durability invariant and the superset-slice rule are subtle,
  cross-cutting properties** (ADR 0042 §9/§10) that a future change to the
  seal sequence, the catalog apply arm, or a reader's slicing logic could
  silently violate — the corpus (the Testing plan below) exists because this
  class of bug is a genuine data-loss or torn-read hazard, not a cosmetic one.
- **Two background loops now run on different roles** — the per-tablet
  change-consumer loop (GSI drain + seal arm + hot-trim arm, unchanged
  leader-per-tablet placement) and the control-leader segment-janitor
  (retention + repair,
  a new *cluster-wide* responsibility for whichever node currently leads
  the control group) — a new class of "which node is responsible for this"
  reasoning this codebase's admin surface and dashboard will need to
  surface honestly.

## Testing plan

House corpus discipline throughout (ADR 0014's doctrine: a frozen,
seed-reproducible scenario list, a depth knob, `ANIMUS_STREAM_SEEDS`,
corpus-deep nightly):

1. **Seal crash-safety**: crash between the segment `put` and the catalog
   commit, re-seal to the same id, assert no loss/duplication against the
   write journal; a leader kill mid-seal; an old, deposed leader's late
   superset put racing the winning leader's committed row, asserting a
   reader's slice is still correct.
2. **Store fault injection**: an acknowledged-but-lost put; a window of
   store unavailability (hot scope grows, trim blocks, sealing/draining
   resumes on heal); `ClusterSegmentStore`-specific partial-K delivery,
   node death mid-put, and repair convergence after a replica loss.
3. **Lineage walk**: a model consumer (`DescribeStream` → parent-before-
   child → iterate → null-advance) driven under concurrent seals,
   auto-splits, leader kills, and restarts, asserting exactly-once and
   per-item order against the generating write history.
4. **Retention vs. in-flight reads**: `TrimmedDataAccess` on a reclaimed
   shard is a defined outcome, never silently treated as an empty success;
   an iterator straddling the retention horizon at the moment of a sweep.
5. **GSI coexistence**: GSI + stream trim min-rule; GSI-only; stream-only;
   a split child's watermark inheritance for the stream side vs. the GSI's
   own `W = 0` — proving the two halves of ADR 0042 §8 genuinely coexist.
6. **Merge stopgap** (removed, ADR 0044): a unit-level apply-arm rejection
   plus an end-to-end check through the client relay path — both deleted
   along with `MergeTablets` itself once tablet merge was removed globally.
7. **Disable grace (F12-b)**: write → disable (final seal) → read
   through the grace window → ordinary retention reaps → `ResourceNotFound`;
   a re-enable during the grace window listing two coexisting streams, the
   new one accumulating independently.
8. **`ProdEnv` end-to-end**: a real multi-process cluster, the default
   `ClusterSegmentStore`, small knobs; an auto-split mid-stream with a live
   consumer; a full restart's recovery; every-node-in-turn reads (the house
   forwarded-command regression pattern); an `FsSegmentStore` opt-in smoke
   test.
9. **The durability invariant, directly**: at arbitrary kill points across
   the whole scenario space, assert every acknowledged write is recoverable
   from either hot Raft state or a committed, K-replicated segment — never
   from neither.
