# ADR 0059 — Backup and restore: on-demand backups + point-in-time recovery

- **Status:** Accepted — Train 1 (catalog, backup-store plumbing, capture
  driver, wire surface, janitor) and Train 2 (restore) implemented; Train 3
  (PITR) pending.
- **Date:** 2026-08-26
- **Amends:** [ADR 0013](0013-replicated-schemas.md) (a table's manifest
  records its schema shape — partition/clustering keys, columns, GSI/LSI
  definitions, `StreamSpec`/`TtlSpec` — as a `SourceTableFeatureDetails`
  snapshot, mirroring how a stream shard row carries its own `view_type`
  copy rather than a live reference), [ADR 0024](0024-drop-table-data-gc.md)
  (a backup catalog entry deliberately **outlives** `DropTableSchema`/
  `DropTableTablets` — an explicit carve-out from that ADR's convergent
  reclaim, stated and justified below), [ADR
  0043](0043-stream-shard-subsystem.md) (reuses the `SegmentStore` trait,
  the write-once object discipline, and the catalog-not-`list()` authority
  rule verbatim; PITR's sealing consumer is a fifth, independent arm beside
  the stream sealer, sharing its segment codec machinery but writing to a
  distinct object namespace with its own lifecycle), [ADR
  0049](0049-universal-kind-write-path.md) (capture reads
  KIND_BASE/KIND_LSI/KIND_FOOTPRINT exactly as the split-build driver does;
  PITR's log consumer holds a trim term on KIND_CHANGE exactly as the
  streams sealer and the split driver do), [ADR
  0050](0050-per-tablet-storage-copy-based-splits.md) (capture pins an
  `engine_image` at the tablet's own applied watermark and restore rebuilds
  a table through fresh `Building` tablets seeded via `propose_seed_batch`
  — both mechanisms this ADR reuses without change), [ADR
  0045](0045-updatetable-gsi-backfill.md) (a restored table's GSIs are
  rebuilt through the existing `Creating`→backfill→`Active` lifecycle, never
  captured), [ADR 0051](0051-dynamodb-ttl.md) (the manifest's creation
  timestamp is `wall_now()`-stamped at propose time by the wire-serving
  node — the identical "calendar time enters through one seam, at one
  documented site" discipline, never used for internal timing).
- **Depends on:** ADR 0043 (`SegmentStore`, the segment janitor's two-phase
  retention shape), ADR 0049 (the universal change log PITR seals), ADR
  0050 (`engine_image`/`install_engine_image`/`SeedBatch`, the tablet
  lifecycle states capture and restore both drive), ADR 0038 (the syskv
  mirroring pattern the backup catalog rides), ADR 0044 (split-only
  tablets — the backup-vs-split race below rests on `split_lineage` being
  the sole, immutable provenance record a retired parent leaves behind).

## Context

DynamoDB exposes three things under this feature area — on-demand backups
(`CreateBackup`/`RestoreTableFromBackup`), point-in-time recovery
(`UpdateContinuousBackups`/`RestoreTableToPointInTime`), and S3 export/import
(`ExportTableToPointInTime`/`ImportTable`) — as three separate-looking API
surfaces. They are not three mechanisms. Every one of them answers the same
underlying question — "give me the table's state as of some moment" — from
the same two ingredients: a full snapshot of the data, and a log of changes
since that snapshot. An on-demand backup is a snapshot with no log replay
needed (the moment *is* the snapshot). PITR is a snapshot plus a log replay
to an arbitrary second. Export is a snapshot (or a replay) rendered to a
customer-owned S3 bucket instead of this database's own store. **We build
one internal mechanism — snapshots plus a change-log feed — and let the wire
surfaces be different faces of it**, the same design move ADR 0043 made for
streams (a shard is a seal epoch of the change log; on-demand backup and
PITR are, respectively, a snapshot and a snapshot-plus-replay of that same
log).

This ADR designs and delivers **on-demand backups and PITR**. `ExportTable-
ToPointInTime`/`ImportTable` (the S3-facing pair) are **explicitly deferred**
— they need an S3 `SegmentStore` backend (below) and a distinct wire
contract (a customer bucket, IAM-shaped permissions this database does not
model), and neither blocks the mechanism this ADR builds. The **S3
`SegmentStore` backend** itself is scoped as a follow-up trait-swap, not
part of any train here — but this ADR is designed against it existing
later, because the whole point of reusing `SegmentStore` is that a future
S3 implementation changes nothing about capture, the catalog, or restore.

### Why this reuses `SegmentStore`, not a new store

ADR 0043 already solved "where does write-once, potentially-large, rarely-
read, replicated-durably data live that isn't ordinary row data." A backup
artifact is exactly that shape — closer to a sealed stream segment than to
a live tablet's engine — so building a second store for it would be the
same mistake ADR 0043's own Rejected Alternatives called out for keeping
sealed stream data in `StorageEngine` kind scopes: a bespoke mechanism the
existing one already fits.

One difference matters enough to change the wiring, not the trait: a
backup store commonly wants to be handed a *different* durability/locality
policy than the cluster's own stream segments — an operator backing up to
a separate filesystem mount or (once the follow-up lands) an S3 bucket in
another region, deliberately outside the blast radius of "this cluster's
own storage." So backups get a **separately configured `SegmentStore`
handle** (`--backup-store cluster|fs:PATH`, defaulting to `cluster` — the
existing `ClusterSegmentStore`, so a fresh install needs nothing extra
configured) rather than sharing the stream sealer's store outright, and a
**backup-specific object namespace** so the two feeds can never collide
even when an operator *does* point both at the same store. Said plainly,
because it would be dishonest not to: **the default (`ClusterSegmentStore`)
does not survive a whole-cluster loss** — it replicates within the same
cluster the backups protect data *from* operator or application mistakes
in, not from a total cluster failure. `fs:PATH` pointed at separately
backed-up or replicated storage — and, later, an S3 backend — is the actual
disaster-recovery story. This is stated here once, plainly, rather than
left to be discovered the hard way; the admin surface names it in the
config's own help text.

## Decision

**One internal mechanism: a per-table catalog of point-in-time artifacts
(snapshots and, for PITR, sealed change-log segments), keyed by a durable
backup identity that outlives the source table.** Three wire-facing
capabilities sit on top: `CreateBackup`/`DescribeBackup`/`ListBackups`/
`DeleteBackup`/`RestoreTableFromBackup` (on-demand), and
`UpdateContinuousBackups`/`DescribeContinuousBackups`/
`RestoreTableToPointInTime` (PITR).

### 1. Store: a separately configured `SegmentStore` handle

Reuses the ADR 0043 `SegmentStore` trait unchanged (`put`/`get`/`delete`/
`list`, write-once, `list()` debug/sweep-only) — no new trait, no new
`animus-env` seam. `animusd` gains a second `SegmentStoreHandle` alongside
the streams one, built the same way (`SimSegmentStore` under test,
`ClusterSegmentStore` or `FsSegmentStore` in production) but from its own
CLI knob, `--backup-store cluster|fs:PATH`. Object ids live under a
namespace the stream sealer never writes (`backup/{backup_id}/...` vs. the
stream sealer's `{table}/{label}/{tablet}/{epoch}`), so even an operator
who deliberately points both stores at the same underlying directory or
cluster gets no collision — belt-and-suspenders on top of the namespace
separation being sufficient on its own.

**The S3 backend is a future trait-swap, exactly as ADR 0043 §A7b framed
it for streams**, with one operationally important wrinkle worth naming
now even though it ships later: the intended production shape (root
`CLAUDE.md`'s Kubernetes-operator target, ADR 0047) keeps node-to-node and
seed traffic cluster-internal with only the DynamoDB wire edge exposed
outside the cluster. An S3-backed `SegmentStore` is a deliberate,
narrowly-scoped **exception** to that isolation posture — egress to the
object-storage endpoint, and nothing else, opened for the nodes that
capture/restore backups — not a reason to widen the operator's general
network posture. This ADR does not implement that backend; it is recorded
here so the follow-up's design starts from the right constraint instead of
rediscovering it.

### 2. Backup format: a manifest plus chunked per-tablet data objects

A backup is one **manifest object** plus, per tablet pinned into the
backup, one or more **chunked data objects**.

**Data objects** carry `(kind, logical_key, value_or_tombstone, version)`
tuples — the exact `ImageEntry` shape `engine_image`/`install_engine_image`
already use for split-build snapshot transfer (ADR 0050) — restricted to
**`KIND_BASE`/`KIND_LSI`/`KIND_FOOTPRINT` only**. This is the identical
copy-kinds rule the split-build driver already enforces (never
`KIND_CHANGE`: a restored table's own change log starts empty, exactly
like a split child's, and copying change records forward would be the
#220 duplication class replayed in a new context; never `KIND_CURSOR`:
consumer cursors are per-tablet-identity bookkeeping that means nothing on
a newly-minted tablet id). Chunking follows the split driver's own
`SEED_CHUNK_BYTES`-budget convention rather than inventing a second one.

**The manifest** records everything restore needs without touching the
source table again (which may no longer exist by restore time, §7):

- The table's schema shape — partition key, clustering keys, columns, GSI
  and LSI definitions — captured as a `SourceTableFeatureDetails` snapshot
  (a plain owned copy, not a reference into the live `Metadata::schemas`
  entry, mirroring `StreamShardRow`'s own `view_type` copy-not-reference
  convention from ADR 0043 §A8). Stream and TTL settings are recorded in
  the same snapshot **for descriptive fidelity only** — `DescribeBackup`
  can report "this table had a stream/TTL when backed up," matching AWS —
  but restore deliberately never re-enables either (§6).
- The pinned tablet list and each tablet's key range, at capture time.
- Each tablet's **cut version** — the packed-HLC watermark its capture
  pinned (§4).
- Per-tablet and total object sizes, for `DescribeBackup`.
- A wall-clock creation timestamp, **stamped at propose time by the
  wire-serving node** via `env.wall_now()` — the ADR 0051 precedent
  exactly: the pure state machine has no clock, so calendar time rides the
  command the same way `SealStreamShard::seal_wall_ms` and
  `CutoverSplit::cutover_wall_ms` already do. Never used to make any
  internal decision — retention math (§9) and PITR's cutoff selection
  (§10) both key off it as *data*, not as a timing input.

### 3. Catalog: replicated `Metadata`, keyed by backup identity — never by table name

A new family of `MetaCommand`s, following the `SealStreamShard`/
`ExpireStreamShards` shape exactly:

- `BeginBackup { backup_id, table, manifest_stub, ... }` — mints a catalog
  row in `Creating`, epoch-CAS-free (a backup id is freshly minted per
  request, so there is nothing to CAS against — the collision case is
  "this id already exists," rejected outright, mirroring `CreateTablet`'s
  first-committer-wins shape generalized to a fresh identity rather than a
  race on an existing one).
- One **per-tablet completion record**, proposed by each tablet's capture
  driver as it finishes (§4) — mirroring `MarkIndexBackfilled`'s per-
  tablet-report shape exactly, including its identity convention: keyed by
  `(backup_id, tablet)`, not `(table, backup_id, tablet)`, for the same
  reason `stream_shards` and `index_backfill` key by `(tablet, ...)` alone
  — a tablet id already implies its table.
- `CompleteBackup { backup_id }` / `FailBackup { backup_id, reason }` —
  proposed by a control-plane-leader aggregator once every pinned tablet
  has reported (or has been waiting past a bounded timeout, in which case
  it fails the backup rather than leaving it `Creating` forever) —
  mirroring `index_backfill_loop`'s own convergence-aggregator shape.
- `DeleteBackup { backup_id }` — an operator/retention action, distinct
  from the two above.

**Keyed by backup id (an opaque, freshly-minted identity — an ARN-shaped
string at the wire), never by table name.** This is not a stylistic
choice; it is a scar. The lessons log already carries the general form of
this mistake (name-keyed convergent state that a delete-then-recreate of
the same name silently poisons — see `index_drain.rs`'s backfill-cursor
entry and `docs/engineering-lessons.md`'s note on it) and this ADR's own
catalog is *more* exposed to it than that one, because a backup's defining
property is that it must survive the exact operation — dropping the
source table — that a name-keyed design would treat as "the name is free
again." Keying by a fresh, never-reused identity sidesteps the whole class
outright: two backups of tables that happen to share a name across time
(one dropped, one recreated) are simply two unrelated catalog rows, never
contending for one key.

**Catalog entries outlive the source table** — an explicit, named
exception to ADR 0024's convergent drop-table GC. `DropTableSchema`/
`DropTableTablets` do not touch `Metadata::backups` at all; a backup (and,
for PITR, its sealed segments) is reclaimed only by this feature's own
retention janitor (§9), never as a side effect of dropping the table it
was taken from. This is what makes "restore a table I dropped three days
ago, within the retention window" (§7) possible at all, and it is the same
shape DynamoDB itself commits to.

**Mirrored durably via the syskv pattern**, like every other `Metadata`
collection (ADR 0038): a new `syskv::EntityKind::Backup` (and, for PITR,
`EntityKind::PitrSegment`) with the usual typed key/decode helper pair,
picked up by `mirror::apply_and_derive_mirror`'s exhaustive per-variant
match (no wildcard arm — a future backup-related `MetaCommand` fails to
compile here until its mirror behavior is a deliberate decision, the same
discipline every other collection gets) and by the bulk-rebuild path.
`Metadata::backups`, like `stream_shards`, needs the same
`#[serde(with = "..._codec")]` tuple-key workaround wherever its map key
is not a bare string.

**Retention, failed-backup cleanup, and orphaned-object reaping** run in a
control-plane-leader janitor, in the **ADR 0043 §A9 two-phase mold**: mark
(a `MetaCommand` transition to an `Expired`-shaped state, or an outright
`FailBackup` past a stuck-`Creating` timeout) then reclaim (delete every
recorded object, then remove the row) — never collapsed into one step, so
a crash mid-sweep just resumes on the next tick exactly like the stream
janitor's own retention phase. `SegmentStore::list()` stays **debug/sweep
only, never load-bearing** — the catalog is the sole authority for what
backup data exists, for the identical reason ADR 0043 §A8 gives: an object
store's listing consistency is weaker than a replicated Raft log's, and a
manifest living only in the store would make an ordinary `DescribeBackup`
pay a store round trip for something `Metadata` already answers for free.

**This janitor inherits ADR 0043 §A9's own open control-only-leader scope
gap, named rather than silently re-created.** Retention *marking* needs
only `Metadata`, cheap on any control-plane leader; object deletion needs
a `SegmentStoreHandle`, which today exists only on a node with a data
role. A control-only leader (a genuine ADR 0035 split deployment) marks
backups expired and reacts to a stuck `Creating` correctly, but cannot
physically reclaim objects for as long as it leads — rows accumulate,
marked but un-reclaimed, until a data-role node takes the lead instead.
Exactly the same deferred, documented residual as the stream janitor's;
extending `SegmentStoreHandle` provisioning to a control-only node would
close both gaps at once, and remains its own follow-up either way.

### 4. Capture (on-demand): per-tablet, leader-side, event-driven

`CreateBackup` proposes `BeginBackup`, pinning the table's *current*
tablet list into the manifest stub. From there, capture is **per-tablet**,
running on each pinned tablet's own leader — event-driven off the
metadata watch, the same trigger shape the split-build driver uses to
notice a `Splitting` parent it should be draining. Per tablet:

1. **Pin a read view at the tablet's own current applied watermark** —
   the same snapshot-pinned, kind-classified sweep `engine_image`
   (`animus-cp-data::lib.rs`, ADR 0050) is built on, filtered to the
   three copy kinds, but *not* that function verbatim: capture reads
   **through intent resolution** (§5) rather than copying physical bytes,
   and emits **chunk by chunk** rather than materializing one whole
   in-memory image, since its output is a sequence of store objects, not
   a single snapshot-transfer blob. The scan/classification primitive is
   shared; the byte-verbatim, single-blob packaging is not.
2. **Sweep via a durable, resumable cursor** — the backfill-seeder shape
   (`index_drain.rs`'s per-index `KIND_CURSOR` row convention): a capture
   in progress survives a leader crash by resuming from its own recorded
   cursor rather than restarting the whole tablet, exactly as a backfill
   resumes.
3. **Write chunked data objects** to the backup store, at the object
   naming convention `backup/{backup_id}/{tablet}/{chunk}`.
4. **Report completion through Raft** — the per-tablet completion record
   (§3), carrying the tablet's own cut version for the manifest.

**Consistency: a per-tablet cut, not a cluster-wide one.** Each tablet
captures at its *own* applied watermark, independently of every other
pinned tablet's watermark. This is a deliberate rejection of a
cluster-wide HLC cut for this tier — considered and set aside because (a)
it matches what DynamoDB's own on-demand backup actually guarantees (AWS
documents on-demand backup as *not* being a single consistent
cross-partition snapshot instant either), and (b) PITR is where
time-addressability genuinely matters, and PITR gets it from the change
log's own packed-HLC ordering (§10), not from forcing every tablet's
*base* snapshot to agree on one instant. Manufacturing a cluster-wide cut
for the on-demand tier would mean either a blocking barrier across every
tablet (a real write-availability cost this ADR's own "writes never
blocked" rule forbids, next paragraph) or a second, PITR-shaped replay
mechanism duplicated into the on-demand path for no wire-visible benefit —
paying full PITR machinery cost to serve a guarantee real DynamoDB itself
doesn't make.

**Writes never blocked.** Capture reads a `engine_image` snapshot of
already-committed state and sweeps forward from a cursor; nothing about it
proposes a freeze, holds a lock across a commit, or otherwise slows a
concurrent write — the same non-blocking posture the split-build driver's
own bulk pass has against its parent's ongoing writes.

**Durable-before-visible.** A backup is `AVAILABLE` (DynamoDB's terminal
status) only once its **manifest object is durably stored** — the last
write in the capture sequence, proposed as `CompleteBackup` only after
every pinned tablet's completion record is in and the manifest itself has
been `put` to the store. A crash before that point simply leaves the
catalog row `Creating`, exactly as `SealStreamShard`'s own crash-before-
commit story resumes on the next tick.

### 5. Settled decision — capture reads through intent resolution

**A backup contains only committed values — never a verbatim byte copy of
physical rows.** Capture reads through the existing 2PC intent-resolution
machinery (`read_resolved`'s discipline: an unresolved intent restores to
its prior committed value, per ADR 0018 §2), not a raw scan of engine
bytes. The alternative — copying physical bytes verbatim, envelope tag
included, the way `SeedBatch` deliberately does for a split (ADR 0050
fork F7) — is correct *there* because a split child and its parent are
part of the same live transaction's blast radius: an in-flight intent
copies forward, and the transaction's own coordinator/resolver machinery
(still running, against the same anchor) eventually resolves it wherever
it lands. A backup has no such continuation. Its anchor record may live
in a **different table entirely**, one this backup knows nothing about
and may itself be dropped, restored, or simply gone by the time anyone
ever restores from this artifact. An intent envelope surviving into a
restored table would be a **dangling reference with no possible resolver**
— worse than merely stale data, an unresolvable one. Reading through
resolution at capture time means a restored table starts from exactly the
same kind of state a fresh table with those committed values would have:
no envelope, no in-doubt anything.

### 6. Settled decision — the backup-vs-split race

The manifest's pinned tablet list is a snapshot of `Metadata::tablets` at
`BeginBackup` time. If a split cuts over on one of those tablets while
capture is still in flight, that tablet id retires from the map entirely
(ADR 0050 stage 4/5: `CutoverSplit` removes the parent; the reconciler
reclaims its engine). A capture driver still holding a lease on that
retired tablet id would otherwise stall forever waiting for a group that
no longer exists.

**The capture driver re-plans a retired tablet's range onto its
children**, via `Metadata::split_lineage` — the same frozen, immutable
provenance map ADR 0050 fork F9 writes once at `CutoverSplit`'s own apply,
already load-bearing for stream shard lineage (`stream_shard_parent_id`)
and reconciler book-keeping. When the aggregator (or the per-tablet
capture arm itself, on next tick) observes a pinned tablet id no longer
present in `Metadata::tablets`, it looks up every **live** tablet whose
`split_lineage` chain traces back to it (a tablet can be an arbitrarily-
many-generations-removed descendant if splits cascade during a long
capture — `split_lineage` is transitive through retired ancestors purely
on wire data, exactly as ADR 0050's rung-6 as-built note establishes for
stream lineage) and substitutes those descendants for the retired parent
in the backup's own pinned-tablet bookkeeping, each capturing its own
share of the original range independently from there. This closes the
race with **zero new control-plane state and no GC veto on parent
retirement** — the rejected alternative.

**Why not a GC veto instead** (the naive fix: block a parent's retirement
until every in-flight backup capturing it has finished). Rejected for the
same reason ADR 0050's own Decision 2 exists at all: a live tablet
blocking on an unrelated background process's own pace reintroduces
exactly the coupled-teardown hazard class ADR 0050 spent a whole train
closing (the "two live things must coordinate a handoff, observed through
independently-lagging caches" root cause named in that ADR's own Context
section). A capture that has to wait indefinitely for a slow or wedged
backup is a liveness hazard for the *split*, not just for the backup — and
splits are exactly the mechanism this database leans on to relieve a hot
tablet, the worst possible thing to have silently stall. Re-planning onto
`split_lineage` costs nothing at split time (the map write already
happens, unconditionally, for stream lineage) and pushes all of the
re-planning cost onto the (already slower-paced, already-resumable)
capture path, which is where it belongs.

**Correctness argument.** A capture that has not yet read past the
tablet's declared range boundary when the split lineage substitution
happens simply continues against the two (or more) children instead of
one parent — its own cursor-based resumability (§4 step 2) means "the
tablet I was reading is now three tablets" is handled exactly like any
other leader-change-triggered resume, just against a different, wider set
of leaders. Because a copy-based split child is born with an **empty
change log and its own base rows copied via `SeedBatch`** (ADR 0050), and
capture reads through the identical `engine_image`/cursor-sweep primitive
regardless of which tablet id it targets, no row is ever double-counted
or dropped purely because the underlying tablet identity changed mid-
capture — the range the manifest ultimately records is the union of what
each live descendant actually captured, which by construction covers
exactly the same key space the original parent owned.

### 7. Restore (`RestoreTableFromBackup`)

**Always to a new table name — fails if the target already exists**,
matching AWS exactly (this is not a merge or an in-place restore
operation). Restore replays the table's own creation machinery rather
than inventing a parallel bootstrap path:

1. **`CreateTableSchema` from the manifest's `SourceTableFeatureDetails`**
   — partition key, clustering keys, columns, and GSI/LSI definitions
   carried forward. A restore request may supply a
   `GlobalSecondaryIndexOverride` (AWS's own knob, letting a caller change
   a GSI's projection or omit it entirely on the restored table) — applied
   at this step, before the schema ever commits, exactly the shape
   `create_index`'s existing `schema_bridge::index_to_control` bridging
   already provides for an ordinary `CreateTable`-declared index.
2. **TTL and stream settings are deliberately NOT re-enabled** — AWS-
   faithful. The manifest records what the source table *had* purely for
   descriptive parity (`DescribeBackup` can report it), but a restored
   table starts with no stream and no TTL regardless of the source's
   configuration, mirroring the exact "streams begin at enable, never
   retroactively" posture ADR 0049 §1 already established for a
   differently-shaped case. An operator who wants either back re-enables
   them explicitly on the new table, same as on real DynamoDB.
3. **Mint fresh `Building` tablets matching the backup's key ranges** —
   the manifest's pinned-tablet-and-range list (already re-planned onto
   live descendants if a split raced the original capture, §6) becomes
   the restore driver's own tablet-provisioning plan. Tablet ids are
   **never reused** (the existing monotonic allocator floor already
   guarantees this for every other tablet-minting path; restore adds no
   new allocator logic).
4. **A restore driver seeds each new tablet from its backup objects via
   `propose_seed_batch`** — the identical `KvCommand::SeedBatch` primitive
   the split-build driver already uses to seed a `Building` child, applied
   the same way: version-carrying merges, so a row lands at its
   **original captured HLC version**, not a freshly-minted one, and
   re-proposing the same chunk on retry is an idempotent no-op exactly as
   it is for a split. This is what makes restore's own crash recovery
   free: a driver that dies mid-seed simply re-runs from wherever its
   resumable progress marker says, and every already-applied chunk
   merges as a no-op.
5. **Activation, then the converged-or-timeout serveable gate** — once
   every tablet has been fully seeded, the driver flips the tablets
   `Active` (mirroring `CutoverSplit`'s activation, minus the "retire a
   parent" half, since restore has no parent to retire) and the table
   goes through the same `await_table_serveable` linearizable-probe gate
   `CreateTable` already uses before acking a client. `TableStatus` reads
   `CREATING` for the whole duration, matching AWS's own reported status
   during a real restore.

### 8. Settled decision — GSIs are rebuilt, not captured

**Restore seeds base, LSI, and footprint rows only — never a GSI's own
hidden-table rows.** A GSI is a *separate, hidden table* (ADR 0041,
`index_table_name`) with its own tablets; nothing in the manifest's
copy-kinds rule (§2) captures it, by construction. Once the restored base
table's rows are seeded and activated, each declared GSI goes through the
**existing** ADR 0045 `Creating` → backfill → `Active` lifecycle exactly
as an `UpdateTable`-added GSI on a live, already-populated table does: the
backfill seeder sweeps the restored table's own `KIND_BASE` rows, seeding
synthetic change-log records the ordinary GSI drain then materializes.

This was checked against the real interaction it could plausibly break —
**a GSI's completion signal races the base table's own footprint
materialization** — and holds for the same reason a live `UpdateTable`
GSI add already works over a populated table: the backfill seeder's own
completion condition (`index_backfill_loop`, aggregating per-tablet
`MarkIndexBackfilled` reports) is keyed off the table's **current** live
tablet map, read fresh every tick, not off any restore-specific state — a
restored table's tablets are ordinary `Active` tablets the instant restore
activates them, indistinguishable to the backfill machinery from tablets
that have always existed. No new interaction to design: restore's only
job is to get the base table into an ordinary, fully-seeded, `Active`
state, and everything downstream of that (GSI backfill included) is
mechanism this ADR reuses unmodified.

### 9. PITR: continuous backups over the change log

`UpdateContinuousBackups { Enabled: true }` registers a **backup
consumer** on each of the table's tablets' `KIND_CHANGE` logs — a
**fifth** consumer arm alongside the existing GSI drain, seal arm,
backfill seeder, and hot-trim arm in `change_consumer_loop`, holding a
trim term exactly like the streams sealer and the split-build driver's
own tail cursor do (ADR 0049 §4's "a consumer holds a term for exactly as
long as it needs it"). It:

- **Reads locally without waking a quiesced group**, exactly the TTL
  reaper's own quiescence contract (ADR 0051 §6) — a PITR-enabled table
  with no write traffic costs one idle local read per sweep and stays
  quiesced; the group wakes only to actually ship a sealed segment (a
  Raft-adjacent action — proposing the sealed segment's catalog row —
  requires an awake group regardless, mirroring exactly why the reaper's
  delete does but its scan doesn't).
- **Seals change records into the backup store continuously**, sharing
  the stream sealer's own segment codec and sealing mechanics
  (`segment.rs`'s encode/decode, the superset-slice rule, the
  ledger-named-object write-once-id discipline) — but as a genuinely
  **separate consumer, writing to a distinct object namespace with its
  own lifecycle**, never the same objects a table's own DynamoDB Streams
  feature seals. A table can have a live stream, PITR, both, or neither,
  independently — a stream's disable/re-enable cycle and PITR's own
  enable/disable cycle (§10) must never interact with or gate each other,
  since they answer different questions for different consumers.
- **Plus periodic base snapshots**, via the *identical* on-demand capture
  mechanism (§4) — an internally-triggered `BeginBackup` a PITR-enabled
  table's own leader proposes on a schedule, not a client-visible
  `CreateBackup` call. These bound PITR's own replay length (a restore
  never has to walk further back than the nearest preceding base snapshot)
  and are what let the change-log side of the retention janitor (below)
  trim segments — a PITR base snapshot is retained at least as long as any
  segment sealed after it might still need it as a replay base.

**Retention window: 35 days by default, configurable, janitor-enforced.**
`EarliestRestorableDateTime` is the retention floor; the same two-phase
janitor (§3) marks and reclaims a PITR base snapshot or sealed segment
once retention has passed it, subject to the identical "never remove a
tablet's own current highest-epoch row while the tablet still exists"
epoch-derivation guard ADR 0043 §A9 already established for stream
segments — PITR segments derive their own epoch numbering the same
chain-length way, so the same hazard applies and the same fix does too.
`LatestRestorableDateTime` **trails "now" by apply/seal lag** — a PITR
consumer is a background sweep like every other consumer arm, not a
synchronous part of the write path, so the most recent few seconds of
writes are honestly reported as not-yet-restorable rather than silently
claimed as covered.

**Enable starts the clock at now; disable then re-enable resets the
window.** This is deliberately AWS-faithful and mirrors a precedent this
codebase already has: ADR 0042's stream re-enable mints a fresh `label`
rather than resuming the old one, and a re-enabled PITR consumer likewise
starts a fresh retention window from its own new enable moment rather
than pretending continuity with whatever coverage existed before the gap
— a gap in coverage is real and must not be papered over as if the
disabled interval had been captured.

### 10. `RestoreTableToPointInTime`

Given a target wall-clock second `T` (AWS's own 1-second granularity):

1. **Pick the newest base snapshot at or before `T`** from the table's
   PITR base-snapshot history (§9) — the replay starting point.
2. **Replay change records from that snapshot forward**, per tablet, up
   to **the per-tablet packed-HLC cutoff corresponding to wall-clock
   second `T`** — not a single global HLC value, since different tablets'
   own logs advance independently and each tablet's own replay must stop
   at its own record nearest `T`, mirroring the same per-tablet
   independence the on-demand tier already embraces (§4) rather than
   inventing a cross-tablet synchronization point PITR doesn't actually
   need either.
3. **Seed the result into a new table via the same restore path as
   `RestoreTableFromBackup`** (§7) — `propose_seed_batch` against fresh
   `Building` tablets, GSIs rebuilt through backfill (§8), TTL/stream not
   re-enabled. PITR's replay produces exactly the same
   `(kind, logical_key, value, version)` shape an on-demand backup's data
   objects carry, so the seeding mechanism does not know or care whether
   its input came from a snapshot alone or a snapshot-plus-replay.

**Deleted-table restore within the retention window works** — a
deliberate, explicit override of the streams rule that a dropped table's
shards are retention-zeroed **immediately** (ADR 0043 §A9's own drop-table
convergent rule: `Metadata::table_schema(&row.table).is_none()` makes a
stream segment "retention 0, immediately due"). PITR's catalog rows and
segments are **not** gated on the source table's schema still existing —
they follow the backup catalog's own outlives-the-source-table rule (§3),
because DynamoDB's own PITR contract is explicitly that a dropped table
stays restorable for the remainder of its retention window. The two rules
look like a contradiction only if read as "how does this codebase treat a
dropped table's change-log artifacts," and they are not: streams and PITR
are two different consumers of the identical log with two different,
independently-chosen retention contracts, exactly as §9 states they must
never gate each other.

## Testing plan

House corpus discipline throughout (ADR 0014's doctrine, carried forward
by every subsequent ADR in this line): a frozen, seed-reproducible
scenario list, a depth knob, nightly deep tier in `corpus-deep.yml`.

- **`ANIMUS_BACKUP_SEEDS`** (`animus-test`): on-demand backup under fault
  injection — concurrent writes racing capture, a split racing capture
  (§6's re-planning), a leader kill mid-capture, a crash-restart of the
  capture driver, `SegmentStore` faults (the existing `SimSegmentStore`
  ack-lost-put/partial-delivery injection, reused verbatim rather than
  built anew) — then a full restore from the resulting backup, compared
  against a model of what the source table held at capture completion.
- **`ANIMUS_PITR_SEEDS`** (`animus-test`): random restore-to-second
  requests against a table under continuous mixed load (writes, splits,
  index add/drop), each compared against an independent model's state at
  that exact wall-clock second — proving the per-tablet cutoff selection
  (§10) reconstructs the right row set even when different tablets'
  own logs advanced at different rates.
- Both corpora run in the nightly deep tier (`.github/workflows/
  corpus-deep.yml`), matching every other named `ANIMUS_*_SEEDS` knob in
  the house table (root `CLAUDE.md`).
- **`SimSegmentStore` fault injection already exists** (ADR 0043) and is
  reused rather than reimplemented — ack-lost puts and partial-K-delivery
  windows drive both corpora's store-fault scenarios directly.
- **`ProdEnv` end-to-end**: a real multi-process cluster backing up to
  the default `ClusterSegmentStore` and to an `fs:` opt-in, a full
  on-demand backup/restore round trip, and a PITR enable → write → restore
  round trip across a real restart.

## Delivery plan

Three trains plus a follow-up, each independently reviewable and mergeable
as its own stacked series (root `CLAUDE.md`'s stacked-PR default):

- **Train 1 — capture.** The catalog `MetaCommand`s and this ADR's own
  acceptance (§3); the backup-store plumbing (`--backup-store`, the second
  `SegmentStoreHandle`, the namespace convention, §1); the capture driver
  and its fault-injection corpus (§4/§6, `ANIMUS_BACKUP_SEEDS`); the wire
  surface for `CreateBackup`/`DescribeBackup`/`ListBackups`/`DeleteBackup`.
  Ships alone — a backup exists and can be inspected, but nothing can
  restore from one yet.
- **Train 2 — restore.** `RestoreTableFromBackup` (§7), the GSI-rebuild
  interaction (§8), and the corpus's restore-and-compare half.
- **Train 3 — PITR.** The fifth consumer arm (§9), the retention janitor's
  PITR-segment phase, `RestoreTableToPointInTime` (§10), and
  `ANIMUS_PITR_SEEDS`.
- **Follow-up — the S3 `SegmentStore` backend.** Out of every train above;
  a trait-swap this ADR designs for (§1) but does not implement, gated on
  the Kubernetes-operator egress exception being an explicit, reviewed
  decision rather than an incidental widening.

## As-built amendment (2026-08-26, Train 1 PR③ — capture driver)

Two deviations from this ADR's own text, found building the capture
driver and completion aggregator, recorded here rather than left for a
reader to discover by diffing prose against code:

- **§4's chunking is row-count-capped, not byte-budgeted.** The text calls
  for "the split driver's own `SEED_CHUNK_BYTES`-budget convention" —
  `animusd::backup_capture::CHUNK_ROWS` instead caps each data-chunk object
  at a fixed row count (200). `SEED_CHUNK_BYTES` is a `const` private to
  `index_drain.rs`'s own module (not this ADR's concern — a Rust module-
  privacy fact, not a deliberate divergence), and porting the split
  driver's byte-accounting helper into a second module for one PR was
  judged not worth the duplication risk at Train 1's correctness-first
  scope. A row cap still bounds object size well under any real DynamoDB
  item's ~400 KB limit in practice. Matching the byte-budgeted convention
  exactly (or sharing one implementation) is a named follow-up, not a
  correctness gap.
- **§6/§7's "re-planned pinned-tablet-and-range list" is `tablet_progress`,
  not a rewritten `pinned_tablets`.** §7 step 3 describes restore reading
  "the manifest's pinned-tablet-and-range list (already re-planned onto
  live descendants... §6)" as if `BackupManifest::pinned_tablets` itself
  gets updated when a split re-plans a tablet's capture. As built,
  `pinned_tablets` is a **frozen historical stub**, written once at
  `BeginBackup` and never rewritten (PR①'s own explicit design, kept
  unchanged by PR③) — the re-planning instead surfaces through
  `Metadata::backup_manifest_tablet_progress`, whose entries name whichever
  tablets are **currently authoritative** (a live descendant, when a split
  raced capture) and are what the completion aggregator actually writes
  into `BackupManifestObject::tablet_progress`. That list carries each
  tablet's `(cut_version, bytes)` but **no key range** — unlike
  `BackupPinnedTablet`, `BackupManifestTabletEntry` has no `range` field.
  This is a real, open question for Train 2: `RestoreTableFromBackup` does
  not strictly need each historical reporting tablet's own range to
  reconstruct the table correctly (`propose_seed_batch` merges each row by
  its own logical key regardless of which tablet the restore driver mints
  to receive it, so restore is free to choose an entirely fresh tablet
  layout for the whole table — e.g. one tablet per the placement engine's
  own preference — rather than mirroring the capture-time split topology),
  but if Train 2's design instead wants to reproduce that topology
  one-for-one, `BackupManifestTabletEntry` will need a `range` field added
  first. Left for that train's own design pass rather than pre-emptively
  widened here with no consumer.

## As-built amendment (2026-08-27, Train 1 PR④ — wire surface + janitor)

Four deviations/additions from this ADR's own text, found building
`CreateBackup`/`DescribeBackup`/`ListBackups`/`DeleteBackup` and the backup
janitor, recorded here rather than left for a reader to discover by diffing
prose against code:

- **A new `MetaCommand::MarkBackupDeleted`, not a widened `DeleteBackup`.**
  §3's text describes `DeleteBackup { backup_id }` as "an operator/retention
  action" without separating a mark step from a finalize step. As built, the
  wire `DeleteBackup` operation (`animusd::dynamo::delete_backup`) proposes
  the new `MarkBackupDeleted { backup_id }` — transitioning `Available`/
  `Failed` to `Expired` (idempotent once `Expired`; rejects a still-`Creating`
  row as a defense-in-depth seatbelt behind the wire edge's own
  `BackupInUseException` check) — and the **existing, unmodified**
  `MetaCommand::DeleteBackup` (PR①'s own row-plus-progress removal) becomes
  the janitor's own **finalizing** command, proposed only once every one of
  a marked backup's objects has been reclaimed. `BackupStatus::Expired`
  already existed for exactly this purpose (PR①'s own doc: "no `MetaCommand`
  in this PR ever transitions a row into this state... so a later PR's
  janitor-mark command doesn't need to widen this enum") — no enum change
  was needed, only the one new command to drive the transition.
- **`BackupRow` gained two fields the wire surface needs and PR①/PR③ never
  carried: `backup_name: String` and `total_bytes: u64`.** `BackupName` is a
  client-supplied, AWS-remembered attribute `CreateBackup`/`DescribeBackup`/
  `ListBackups` must echo back identically — recorded verbatim on
  `MetaCommand::BeginBackup` (a new field, threaded through every existing
  construction site) and stored on the row, never interpreted. `total_bytes`
  is **frozen exactly once**, by `CompleteBackup`'s own apply arm, from
  `Metadata::backup_total_bytes` at the moment every pinned tablet's live
  descendant is still resolvable — **not** re-derived live by
  `DescribeBackup`/`ListBackups`, which would silently collapse to zero the
  instant this backup's source table (and with it every one of its tablets)
  is ever dropped, breaking this ADR's own §3 "outlives the source table"
  promise for the *reported size* specifically, even though the row and its
  progress records themselves already survived the drop correctly. Found by
  reasoning through `backup_total_bytes`'s own doc (a live re-derivation
  over `Metadata::tablets`) against the "works after the source table is
  dropped" requirement below, not by a failing test — `docs/engineering-
  lessons.md` records the general lesson.
- **The wire ARN *is* the catalog's `BackupId`, not a separate wrapper.**
  §3's "an ARN-shaped string at the wire" is realized literally:
  `animusd::dynamo::create_backup` mints `wire::backup_arn(table,
  random_suffix)` and proposes it as `BeginBackup`'s own `backup_id` — so
  every lookup (`DescribeBackup`/`DeleteBackup`/`ListBackups`'s pagination
  cursor) is a direct `Metadata::backups` key lookup, with no ARN-parsing
  function anywhere in this adapter (none was needed once the ARN and the
  key are the same string).
- **Reclaim is local-only — a deliberate, named Train 1 simplification, not
  the cataloged-replica reclaim §3 might suggest by analogy to the segment
  janitor.** No backup object carries a recorded `replicas` list the way a
  `StreamShardRow` does (`backup_capture.rs`/`backup_completion.rs` both
  discard `BackupStoreHandle::put`'s own returned replica set), and a
  tablet's completion record carries total bytes, not a chunk count, so
  there is no way to enumerate a backup's own object ids without asking the
  store. The janitor (`animusd::backup_janitor`) therefore does what §3
  explicitly licenses for exactly this situation — `SegmentStore::list()`
  as a debug/sweep tool, scoped to `backup/{backup_id}/`, on **this node's
  own local** backup directory only — the identical shape the segment
  janitor's own orphan sweep already uses, generalized here from "extra,
  uncataloged objects" to "this backup's objects" outright. **Named
  residual**: on a `Cluster`-backed store whose control-plane leader never
  happens to be one of the `K` (`ClusterSegmentStore::DEFAULT_K` = 3) nodes
  actually holding a given backup's objects, this loop's local sweep finds
  nothing and finalizes (removes the row) on the very first tick it
  observes the mark, before a node that *does* hold a copy ever gets to
  sweep its own — those copies become permanent, uncataloged orphans. Below
  or at `DEFAULT_K` cluster size (every node is always a target) the gap
  does not manifest; above it, closing it needs either a per-object
  `replicas` list or a cluster-wide list primitive for `ClusterSegmentStore`
  (neither exists today), both out of this PR's scope. See
  `backup_janitor.rs`'s own module doc and `docs/engineering-lessons.md`
  for the fuller note.

## As-built amendment (2026-08-27, Train 2 — restore)

Six deviations/decisions from this ADR's own text, found building
`RestoreTableFromBackup` and the restore driver, recorded here rather than
left for a reader to discover by diffing prose against code.

- **Settled: pinned-tablets-vs-fresh-layout (PR③'s own open question) —
  restore mints exactly ONE fresh `Building` tablet over the whole ring,
  never one per the backup's original (possibly many) pinned/reporting
  tablets.** §7 step 3's text ("mint fresh `Building` tablets matching the
  backup's key ranges") describes mirroring the historical topology; PR③'s
  as-built note explicitly left the door open to the alternative it names
  itself: "restore is free to choose an entirely fresh tablet layout for
  the whole table — e.g. one tablet per the placement engine's own
  preference." Taken at its simplest. This sidesteps the open question
  entirely: `BackupManifestTabletEntry` needed **no** new `range` field,
  because nothing in the restore driver ever needs to know which physical
  tablet originally captured a given row — every data object across every
  one of the manifest's `tablet_progress` entries seeds into the SAME
  single destination tablet, verbatim, with no per-row key routing at all.
  This matches ordinary `CreateTable`'s own "one tablet over the whole
  ring" provisioning convention exactly (`ClientCtx::provision_tablet`),
  and the existing auto-split machinery reshapes the restored table's
  tablet count afterward, exactly as it would for any freshly-populated
  table. **Acknowledged tradeoff**: every one of a backup's original
  tablets funnels through one Raft group during the seed phase, so
  restore's own write throughput on a large, many-tablet source table is
  bounded by a single group until auto-split (if enabled) kicks in
  afterward. A Train 2.5 follow-up could mint N tablets up front (e.g. one
  per `pinned_tablets` entry, reusing its already-recorded `range` — no new
  field needed even for that) and route each captured row to whichever
  fresh tablet's range contains it — a real, but narrow and diagnosable
  (not correctness-affecting), performance improvement rather than a gap.
  See `animusd::backup_restore`'s own module doc for the mechanism this
  decision produced.
- **A real bug found building the seeder, not by design review: captured
  values must be re-wrapped in the engine's committed envelope before
  `SeedBatch` merges them, or a read panics.** ADR §5 states plainly that
  capture "reads through intent resolution" and stores each row's
  already-*resolved* value — correct, and unchanged. What §7 step 4's text
  glossed over is that `KvCommand::SeedBatch` (reused verbatim from ADR
  0050) is a **raw envelope-tag-included byte passthrough** — sound for the
  split-build driver, whose child rows are still-enveloped physical bytes
  from the same live transaction blast radius, but not sound for a
  captured, already-resolved value, which carries no envelope tag at all.
  Feeding one straight into `SeedBatch` merges an unwrapped value the
  read path's envelope decoder cannot parse — the byte a real read
  interprets as the envelope tag is instead the value's own first content
  byte, an "unknown envelope tag" panic reachable from an ordinary
  `ConsistentRead` `GetItem` on a restored row. Caught immediately by this
  train's own first end-to-end test run, not by review. Fixed by
  `animus_cp_data::backup::encode_restored_value` (a thin, well-documented
  wrapper the restore driver calls on every `Some` value before seeding) —
  see that function's own doc for the full mechanism, and
  `docs/engineering-lessons.md` for the generalized lesson.
- **A restored table's GSIs are declared on the schema only AFTER
  activation, not up front alongside the base schema/LSIs — narrower
  sequencing than §7's own text implies.** §7 doesn't explicitly order GSI
  declaration against tablet activation; §8 does say GSIs are rebuilt
  "once the restored base table's rows are seeded and activated," but
  doesn't say *when the `CreateTableIndex` proposal itself happens*. Doing
  it early (alongside the base schema, mirroring how `create_table` itself
  declares every index up front) is actively wrong here: the backfill
  seeder would observe the still-empty/`Building` destination tablet,
  find its `KIND_BASE` scan already exhausted, and mark it backfilled
  before this restore ever seeds a single row — silently dropping every
  restored row from the GSI forever. The restore driver (`animusd::
  backup_restore::complete_restore`) proposes each of `RestoreRow::
  gsi_defs`'s `CreateTableIndex` calls **immediately after, in the same
  step as,** `CompleteRestore` — the earliest point the tablet is
  genuinely `Active` and fully seeded, so the backfill seeder's very next
  sweep finds real data. `RestoreRow::gsi_defs` itself (a new `Vec<IndexDef>`
  field, resolved once client-side by the wire handler from
  `GlobalSecondaryIndexOverride` or the manifest's own captured GSIs,
  forced to `IndexStatus::Creating` regardless of the source's own status)
  is what carries this plan from propose time to the driver, since the two
  steps can run on different nodes at very different times.
- **A visible, deliberate AWS deviation this ordering choice produces**:
  the `RestoreTableFromBackup` response, and any `DescribeTable` call
  before the restore completes, do not show the target's GSIs **at all**
  — not even as `CREATING`/backfilling, which is what real DynamoDB shows
  from the very first response. They appear only once the base table
  finishes seeding and activates, at which point they follow the ordinary
  `Creating` → backfill → `Active` lifecycle a client would recognize from
  any `UpdateTable`-added GSI. Named here rather than silently shipped as
  if AWS-faithful; closing it (showing a synthetic pre-declared `Creating`
  GSI in the response before the schema itself carries one) is a
  wire-layer-only follow-up, not a data-model change.
- **`TableStatus` needed no new persisted state at all** — derived
  (`animusd::dynamo::table_status`) purely from whether every one of a
  table's *current* tablets is `Active`: `CREATING` while any is
  `Building` (true for a restore's own single tablet until activation;
  structurally unreachable for an ordinary `CreateTable`, which blocks on
  `await_table_serveable` before ever returning 200), `ACTIVE` otherwise.
  This is the same "derive from live tablet state, never a redundant
  status field" discipline the codebase already applies to a GSI's own
  `IndexStatus` and to `BackupStatus`'s relationship to capture progress.
- **A restore does not pin/lock its source backup against a concurrent
  `DeleteBackup`** — a narrow, accepted residual, not a defended property.
  If a backup is marked deleted (and, rarer still, actually reclaimed by
  the janitor) while a restore reading from it is still in flight, the
  restore driver's own defensive check (re-reading the backup's live
  status each tick) fails the restore outright (`FailRestore`) rather than
  serving a half-seeded table — never a correctness violation, but a
  liveness one an operator could hit by deleting a backup at an unlucky
  moment mid-restore. Closing it (a reference count, or refusing
  `DeleteBackup` while any `Seeding` restore names the backup) is a named
  follow-up, not implemented in this train.

**Corpus** (`crates/animus-test/tests/backup_fault_corpus.rs`,
`ANIMUS_BACKUP_SEEDS`): five restore cells, the identical self-contained-
reimplementation technique the capture half already established —
`restore_round_trip_matches_model_at_capture_cut_version` (including a
staged-and-never-resolved intent, proving restore only ever sees resolved
values), `restore_driver_crash_restart_resumes` (a true process restart of
the destination leader mid-seed), `restore_leader_kill_mid_seed_converges`
(a live leader kill/failover mid-seed), `restore_store_faults_still_converge`
(the backup store genuinely unavailable partway through the sweep, healing
later — `SegmentFaultConfig`'s own ack-lost thresholds are `put`/`delete`-
only, checked directly against `animus-sim`'s source, so a read fault for
restore's `get`-only workload is `SimSegmentStore::set_unavailable_until`,
not `SegmentFaultConfig`), and `restore_after_source_drop`. GSI-rebuild
convergence is deliberately not reimplemented a third time in this corpus —
it is the exact `index_backfill.rs`/`index_drain.rs` machinery
`backfill_fault_corpus.rs` already proves at depth, applied to an ordinary
`Active` tablet indistinguishable from any other (§8's own point); the real
production stack's end-to-end GSI-after-restore convergence is covered by
`animusd/tests/dynamo_restore.rs` instead, alongside the full
`CreateBackup` → `AVAILABLE` → write-more-data → `RestoreTableFromBackup` →
converged round trip, restore-after-source-drop, and the AWS-faithful error
shapes.

**Open questions carried into Train 3 (PITR)**: none of Train 2's own
decisions above constrain PITR's design — `RestoreTableToPointInTime` (§10)
reuses "the same restore path as `RestoreTableFromBackup`" for its own
seeding, so PITR inherits both the single-fresh-tablet layout decision and
the GSI-after-activation sequencing unchanged. The one item worth a future
PITR author's attention: whether PITR's own replay (a snapshot plus a
change-log walk, producing the identical `SeedRow` shape per §10) needs the
same `encode_restored_value` envelope re-wrap — very likely yes, since its
output is described as "exactly the same `(kind, logical_key, value,
version)` shape an on-demand backup's data objects carry," which is the
exact shape this train found needed the wrap.
