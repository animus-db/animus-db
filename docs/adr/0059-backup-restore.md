# ADR 0059 — Backup and restore: on-demand backups + point-in-time recovery

- **Status:** Accepted — Train 1 (catalog, backup-store plumbing, capture
  driver, wire surface, janitor), Train 2 (restore), Train 3 PR① (PITR
  mechanism: the fifth consumer arm, periodic base snapshots, retention,
  `UpdateContinuousBackups`/`DescribeContinuousBackups`), and Train 3 PR②
  (`RestoreTableToPointInTime`) all implemented. The backup/restore/PITR
  feature train is complete.
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

**This janitor used to inherit ADR 0043 §A9's own control-only-leader scope
gap — closed, along with that gap, by W-10 (2026-09-04; see this doc's own
As-built amendment below).** Retention *marking* needs only `Metadata`,
cheap on any control-plane leader; object deletion needs a
`SegmentStoreHandle`, which — as originally shipped — existed only on a
node with a data role, so a control-only leader (a genuine ADR 0035 split
deployment) marked backups expired and reacted to a stuck `Creating`
correctly but could not physically reclaim objects for as long as it led.
Both this janitor's and the stream janitor's gaps closed at once, exactly
as this paragraph originally anticipated: `SegmentStoreHandle`/
`BackupStoreHandle` provisioning was extended to the control-only assembly
path (`animusd::BoundControlNode::start_control_with`).

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

## As-built amendment (2026-08-27, Train 3 PR① — PITR mechanism)

Nine deviations/decisions from this ADR's own text, found building the
fifth consumer arm, periodic base snapshots, the retention janitor's PITR
phase, and the `UpdateContinuousBackups`/`DescribeContinuousBackups` wire
surface, recorded here rather than left for a reader to discover by
diffing prose against code.

- **A `PitrSpec.generation: u64` counter, not a bare boolean, and a
  separate never-rewound `Metadata::pitr_generation: BTreeMap<TableName,
  u64>` allocator** — mirroring `StreamSpec.label`'s own identity role
  (§9's text doesn't specify PITR's own coverage-epoch identity mechanism
  beyond "a fresh window"). `SealPitrSegment`'s generation-licensing rule
  mirrors `SealStreamShard`'s label rule verbatim (licensed by the table's
  *current* generation, or an existing catalog row's own, for the
  disable-triggered final seal). The allocator survives `DropTableSchema`
  and a same-named table's later recreation, closing the identical
  "convergent per-name state a delete-then-recreate can poison" scar §3
  already names for `BackupId` — applied here to a per-table counter
  instead of an opaque identity, since a `(table, generation)` pair is
  already enough to disambiguate two eras of a same-named table's PITR
  history without needing a wholly opaque id.
- **`MetaCommand::MarkBackupPitrBase { backup_id }`, a side-tag command, not
  a `BackupRow.pitr_base`/`BeginBackup.pitr_base` field.** Widening
  `BeginBackup`'s own signature would have touched every one of its ~30
  existing construction sites across `animus-control`, `animus-test`, and
  `animusd` (the exact compiler-enumerated fan-out class root `CLAUDE.md`'s
  engineering-practices log already documents for `backup_name`'s own
  addition) for a fact only the PITR janitor and `DescribeContinuousBackups`
  ever need. `Metadata::pitr_base_backups: BTreeSet<BackupId>` is populated
  by a second proposal (`pitr_janitor::pitr_snapshot_loop`) immediately
  after its own `BeginBackup` is observed to land, with a **self-healing
  sweep** (matching any untagged row whose `backup_name` carries a
  recognizable internal marker prefix) closing the gap left by a dropped
  ack between the two proposals on every subsequent tick — a named,
  accepted residual (a vanishingly brief window where an internal snapshot
  looks like an ordinary on-demand one), not a defended two-phase-commit
  property. `DeleteBackup`'s existing apply arm prunes the tag alongside
  the row it tags. **Superseded 2026-09-04 (issue #593) — see this ADR's
  own amendment of that date**: the "vanishingly brief window" above turned
  out to be a real, observable product gap (a `ListBackups` default `USER`
  filter, or the console's per-table backups projection, could show the
  untagged row), so `MarkBackupPitrBase` was deleted outright in favor of a
  `BeginBackup.pitr_base: bool` field applied atomically with the mint.
- **A PITR base snapshot is a `SYSTEM`-type backup for `ListBackups`
  purposes**, realizing `wire::BackupTypeFilter::System`'s own doc comment
  from Train 1 PR④ ("PITR base snapshots — never produced yet") literally:
  the default (`USER`) filter excludes `pitr_base_backups`-tagged rows;
  `SYSTEM`/`ALL` include them. `DescribeBackup`/`DeleteBackup` by ARN are
  unchanged (a caller already holding the specific ARN is not filtered by
  type, matching real DynamoDB) — a client can still delete a PITR base
  snapshot directly if it discovers the ARN via `ListBackups(SYSTEM)`; this
  is not specially guarded (a narrow, accepted residual: doing so can
  orphan a still-needed replay base for segments sealed after it, the same
  class of self-inflicted narrowing a user deleting any relevant backup
  already risks). The JSON body's own `BackupType` field still always
  renders `"USER"` (cosmetic; threading the real value through
  `wire::BackupDetails` end to end is a named follow-up, not a correctness
  gap for the mechanism this PR builds).
- **The disable-triggered final seal is wired identically to F12-b's own
  stream precedent**: a new `ClientRequest::ForcePitrSeal` RPC and
  `ClientCtx::force_pitr_seal_tablet`, mirroring `ForceSeal`/
  `force_seal_tablet` exactly, called for every one of a table's tablets by
  `dynamo.rs::update_continuous_backups`'s disable path **before** it
  proposes `MetaCommand::UpdateContinuousBackups { enabled: false, .. }` —
  so `trim_janitor`'s PITR term (which, mirroring the stream term, applies
  only while `pitr_enabled` is currently true) never has an unprotected
  window the instant the flag flips.
- **PITR shares the stream sealer's own `seal_bytes`/`seal_age` trigger
  knobs (`ctx.data().stream_seal_knobs`) — no separate PITR-specific
  threshold, and no CLI-configurable retention/snapshot-cadence knob at
  all yet.** Both are deliberate Train 3 PR① simplifications, not gaps
  discovered after the fact: `DEFAULT_PITR_RETENTION` (35 days) and
  `DEFAULT_PITR_SNAPSHOT_CADENCE` (6 hours) are hardcoded production
  defaults at every spawn site, the identical "no CLI flag exists yet"
  shape `ttl_reaper.rs`'s own sweep interval already carries in this
  codebase. Both loops' own functions take the relevant `Duration` as a
  parameter (so a test passes a tiny value directly), and threading a real
  `--pitr-retention-days`/reusing `--stream-seal-bytes`-shaped knobs is a
  named follow-up — `start_with_growth`'s parameter list already carries
  enough same-shaped `Duration`s that adding another was judged not worth
  the ~40-call-site mechanical fan-out under this PR's own time budget.
- **No replica-repair phase for PITR segments** — a deliberate, named Train
  3 simplification mirroring Train 1 PR④'s own "reclaim is local-only"
  acceptance for on-demand backups: `pitr_janitor`'s retention phase marks,
  deletes via the cataloged `replicas` list (unlike Train 1 PR④'s backup
  janitor, a PITR segment *does* carry a real `replicas` list, mirroring
  `StreamShardRow`'s own, so reclaim already uses `BackupStoreHandle::
  delete` against it rather than a local-only sweep), and removes — but
  never re-replicates a segment that has lost a copy to cluster churn the
  way `segment_janitor.rs`'s own phase 2 does for streams. A future train
  can add it by copying that phase verbatim.
- **`EarliestRestorableDateTime`/`LatestRestorableDateTime` derivation**:
  `Earliest = max(retention floor, this generation's own enabled_wall_ms)`;
  `Latest = min, over every one of the table's CURRENT tablets, of that
  tablet's own last-PITR-seal wall time (or `enabled_wall_ms` for a tablet
  that has never sealed yet)` — the minimum, not the maximum, since a
  client's own restore-to-a-second request must be coherent across every
  tablet, and reporting later than the slowest tablet's own actual coverage
  would claim a restore this adapter cannot yet reconstruct for that
  tablet's range. Both read `ctx.env.wall_now()` directly at serve time
  (a pure read handler, not a proposed command, so there is no propose-time
  stamping site to ride) — consistent with the "wall clock enters through
  one seam" discipline, just at a read rather than a write.
- **The split endgame's PITR final seal is a straight second copy of the
  streams final-seal step, not a shared helper** — `split_driver_tick`/
  `inplace_split_driver_tick` each gained one more `while pitr_seal_now(..)
  .is_some() {}` loop, gated on `meta.table_pitr(&table).is_some()`,
  immediately after the existing streams one. Not factored into one
  parameterized function: the two write to different stores and catalog
  collections (`crate::SegmentStoreHandle`+`stream_shards` vs.
  `crate::BackupStoreHandle`+`pitr_segments`), and forcing them through a
  shared abstraction was judged more likely to obscure that distinction
  than to save the ~10 lines of duplication.
- **Corpus scoping**: `ANIMUS_PITR_SEEDS` (`animus-test/tests/
  pitr_fault_corpus.rs`) reimplements `pitr_seal_now` directly (the
  `stream_lineage_corpus.rs` precedent) but deliberately does **not**
  re-simulate the periodic-base-snapshot loop or the janitor's full tick —
  both reuse Train 1 machinery `backup_fault_corpus.rs` already proves at
  depth, so this corpus instead proves the janitor's own **new** logic (the
  keep-anchor retention predicate) as a pure function under randomized
  interleavings. See `crates/animus-test/CLAUDE.md`'s own corpus section
  for the full scenario list and the split-scenario engine-sharing bug this
  corpus's own first run found in its test harness (not production code).

**Open questions carried into Train 3 PR② (`RestoreTableToPointInTime`)**:

- §10's per-tablet cutoff replay needs to walk a PITR segment chain the
  same way `DescribeStream`/`GetRecords` walks a stream shard chain
  (`Metadata::pitr_segments`, ascending epoch, `segment::decode_and_slice`)
  — no new primitive, but the replay driver itself (locate the newest base
  snapshot at-or-before `T`, then replay each tablet's own segments/hot
  tail up to the packed-HLC position nearest `T`) is unwritten.
  `encode_restored_value`'s envelope re-wrap (Train 2's own as-built note)
  very likely applies unchanged, per that note's own prediction.
- The single-fresh-tablet restore layout decision (Train 2's own as-built
  note) is inherited unchanged; PITR's replay produces the identical
  `SeedRow` shape an on-demand backup's data objects carry, so
  `propose_seed_batch` doesn't know or care whether its input came from a
  snapshot alone or a snapshot-plus-replay.
- **Not yet decided**: how `RestoreTableToPointInTime` selects which base
  snapshot is "newest at or before `T`" when a table has both PITR base
  snapshots (tagged `pitr_base_backups`) and unrelated on-demand backups a
  client separately created via `CreateBackup` — the ADR's own §10 text
  implies only PITR's own base-snapshot history is eligible, which
  `Metadata::pitr_base_backups_for_table` already scopes correctly; PR②
  should use that accessor directly rather than scanning `Metadata::
  backups` by table name alone.
- **Not yet decided**: whether a restore-to-a-second request against a
  table whose PITR was disabled-then-re-enabled (crossing a generation
  boundary) should be rejected outright for any `T` inside the gap, or
  simply find no coverage there and fail with whatever "no restorable data
  at this point" error DynamoDB itself defines — this ADR's §9 text
  establishes that the gap is real and uncovered, but doesn't specify the
  wire-facing error shape PR② should surface for it.

## As-built amendment (2026-08-27, Train 3 PR② — `RestoreTableToPointInTime`)

Resolves the three open questions above and records five deviations/bugs
found while building the point-in-time restore path — the final PR of the
backup/restore/PITR train.

- **Base-snapshot selection answers the first open question directly**:
  `restore_table_to_point_in_time` (`animusd::dynamo`) calls
  `Metadata::pitr_base_backups_for_table` — never a bare scan of
  `Metadata::backups` by table name — and picks the newest `Available` row
  at or before the resolved cutoff. An unrelated on-demand backup a client
  created via plain `CreateBackup` is structurally invisible to this path
  regardless of its own timestamp, exactly as §10's text implies.
- **The generation-gap question answers "reject, with `InvalidRestoreTimeException`,
  the same code the ADR already names for an out-of-window `T`"** — no new
  error shape. `Metadata::pitr_restore_window` (built in the PR① follow-on
  commit) scopes `Earliest`/`Latest` to the table's *current* generation
  only, so a `T` that falls inside an earlier generation's own window (a
  disable/re-enable gap, or before the current generation's own enable) is
  simply outside `[earliest_floor_ms, latest_ms]` and rejected by the same
  bounds check as any other out-of-range request — the corpus's
  `pitr_restore_window_scopes_to_the_latest_generation_under_random_cycles`
  cell proves this holds under randomized disable/re-enable cycling, not
  just the hand-picked case.
- **Replay-integration shape: extend Train 2's own restore driver, not a
  parallel one** — `backup_restore.rs`'s existing per-tablet, leader-side,
  event-driven `backup_restore_loop`/`restore_tick` gained one more phase:
  after the pre-existing base-manifest chunk sweep, a `Seeding` restore
  carrying a `PitrRestorePlan` runs `replay_pitr_segments`, which walks the
  plan's own resolved `PitrReplaySegmentRef`s (already epoch-ordered by
  construction), fetches + decodes each segment
  (`animus_cp_data::segment::decode_and_slice`), decodes every
  `ChangeRecord`, skips `consumer_hidden()` ones (markers/seeded/staged —
  never real content), and re-derives `KIND_BASE`/`KIND_LSI` writes via
  `dynamo::kind_writes_for_item` — the *same* pure function a live write's
  own leader-side evaluation already uses, so LSI-derivation logic is never
  duplicated. `KIND_CHANGE`/`KIND_FOOTPRINT` are never replayed (a
  footprint is rebuilt fresh by the post-activation GSI backfill
  regardless of how base content arrived, exactly like an on-demand
  restore's own seeded rows). This reuses `BeginRestore` → the existing
  activation → GSI-declare sequence verbatim; `PitrRestorePlan` is carried
  on `RestoreRow`/`MetaCommand::BeginRestore` exactly like `gsi_defs`
  already is, so nothing downstream of `BeginRestore` needed to learn a new
  restore "kind" exists.
- **Confirmed: replayed values need the identical `encode_restored_value`
  re-wrap Train 2's own seeds do** — Train 2's as-built note had predicted
  this "very likely" without proving it; this PR's own first end-to-end run
  proved it the hard way (a raw resolved value merged via `SeedBatch`
  without the envelope corrupts the engine's later reads, identical to the
  Train 2 incident). `replay_pitr_segments` re-wraps every derived value
  before batching it into a `SeedRow`.
- **A real, previously-shipped bug found and fixed ahead of this PR's own
  validation gate being meaningful**: `pitr_seal_now`/`pitr_tick`
  (`index_drain.rs`) stamped `seal_wall_ms` from `ctx.env.now()`
  (monotonic-since-process-start `Nanos`), not `ctx.env.wall_now()` (real
  epoch milliseconds, ADR 0051's one calendar-time seam) — since
  `PitrSpec::enabled_wall_ms` is genuinely wall-clock,
  `LatestRestorableDateTime` silently collapsed to
  `EarliestRestorableDateTime` forever the instant any tablet ever sealed,
  making every non-trivial restore window empty. Fixed to `wall_now()`; the
  never-sealed age-trigger bootstrap fallback (which used to scan for the
  true oldest pending record's own packed-HLC `wall_ms`) switched to
  seeding at the now-`wall_now()`-based `now_ms` directly, since a packed
  HLC's `wall_ms` component is monotonic-since-start and no longer
  comparable to it. A second, narrower gate had the same class of bug:
  `table_change_records_carry_images` didn't account for PITR at all — a
  table with PITR enabled but no GSI/LSI/stream wrote only image-less
  markers, which PITR replay cannot reconstruct row content from. PITR now
  joins the same `!indexes.is_empty() || stream.is_some()` gate a stream or
  index already trips.
- **A real bug in this PR's own new code, found by its own first
  end-to-end test, not by review**: the first `Metadata::
  pitr_replay_segments` was built on `live_split_descendants` (ADR 0059
  §6's on-demand-capture re-planning accessor), which answers "this
  pinned tablet's currently-*live* descendants" and returns **empty** for
  a tablet retired by an ordinary `DropTableTablets` — no `split_lineage`
  entry exists for a drop, only for a split — so a deleted-table PITR
  restore silently replayed nothing. Rewritten as a direct forward DFS
  over the `split_lineage` tree, starting from each of the base snapshot's
  own pinned tablets: every tablet visited (root or descendant) contributes
  its own `pitr_segments` rows regardless of current liveness, since a
  table drop never touches `split_lineage` or `pitr_segments` at all —
  they are ADR 0024 carve-outs exactly like the backup catalog itself. The
  root's own floor is the base snapshot's recorded cut version (never
  replay below what the snapshot already covers); a descendant's floor is
  0 (its own change log starts empty at birth, ADR 0050's copy-based split
  design). Regression:
  `pitr_replay_segments_still_finds_a_dropped_never_split_tablets_own_segments`
  (`animus-control`'s own unit tests) plus the corpus's
  `deleted_table_pitr_restore_matches_the_model` cell.
- **A second production fix the same end-to-end run found**:
  `RestoreDateTime` is truncated to the second (§10's own contract), but
  `pitr_restore_window`'s `earliest_ms` is millisecond-precise (derived
  from `PitrSpec::enabled_wall_ms`, a real wall-clock timestamp with no
  reason to land on a whole second), so a `T` naming the very same
  wall-clock second PITR was enabled in could be rejected purely because
  enabling didn't happen to land on that second's own first millisecond.
  Fixed by flooring `earliest_ms` to its own second
  (`earliest_floor_ms = (earliest_ms / 1000) * 1000`) before the bounds
  comparison — the comparison itself still stays in whole milliseconds
  internally; only the floor moved.
- **Testing technique worth keeping**: the e2e suite
  (`animusd/tests/dynamo_pitr_restore.rs`) does not race the real wall
  clock to decide when a write is "definitely covered by the next seal" —
  polling `LatestRestorableDateTime` for "≥ target second" doesn't
  guarantee the target write is *included*, since Latest can advance past
  the target second before the segment covering it is actually sealed.
  Instead it reads the sealed segment's own `seal_wall_ms` directly off
  `node.metadata()` (`await_next_pitr_seal`), so the test always restores
  to a second the harness *knows* the relevant write landed in, not one it
  merely observed the clock reach.
- **`admin.rs`'s `/admin/restores` view** gained a `source` field
  (`"POINT_IN_TIME"` vs. `"BACKUP"`, keyed on `RestoreRow.pitr.is_some()`)
  plus `pitr_target_wall_ms`/`pitr_segments_planned`, for operator
  visibility into which restores are PITR-driven and how large their
  resolved replay plan is — no wire-facing consequence, purely
  observability.
- **Residuals carried forward, none blocking**: the "not yet decided"
  ARN-scoped-delete-of-a-PITR-base-snapshot residual from PR①'s own
  amendment is unchanged by this PR (restore never touches deletion); no
  replica-repair phase exists for PITR segments (PR①'s own named gap,
  unaffected by adding a reader of those segments); and — as with every
  restore in this train — a restored table's stream/TTL config is never
  re-enabled from the source's own history, matching Train 2's identical
  choice for `RestoreTableFromBackup`. Corpus:
  `crates/animus-test/tests/pitr_fault_corpus.rs`, `ANIMUS_PITR_SEEDS`
  (held green at 300 in ~8s), covering restore-to-a-random-second against
  mixed load with a leader kill, the same property across a split's
  independently-sealing children, the generation-gap scoping property,
  the deleted-table regression, and `UseLatestRestorableTime` reproducing
  the full model. E2e: `animusd/tests/dynamo_pitr_restore.rs` — a full
  enable → timed writes → restore-to-a-mid-point-second round trip
  (verifying exactly the rows as of `T`, via both `RestoreDateTime` and
  `UseLatestRestorableTime`), a deleted-table PITR restore within the
  window, and the `TableNotFoundException`/
  `PointInTimeRecoveryUnavailableException`/`InvalidRestoreTimeException`
  error shapes.

## As-built amendment (2026-09-04, W-10 — the control-only-leader scope gap)

Closes the control-only-leader scope gap §3's own "This janitor inherits
ADR 0043 §A9's own open control-only-leader scope gap" paragraph named
(above) and §9's own PITR-janitor twin — the roadmap item this amendment
documents (`docs/roadmap.md` W-10, filed against ADR 0043 §A9 since that's
where the gap was first named; this ADR inherited it identically for its
own backup/PITR janitors).

- **The fix: `SegmentStoreHandle`/`BackupStoreHandle` provisioning extended
  to the control-only assembly path.** `animusd::BoundControlNode::
  start_control_with` now builds both handles (`build_segment_store`/
  `build_backup_store`, the identical functions the combined and data-only
  paths already used) and threads `--segment-store`/`--backup-store`
  through `animusd control`'s own CLI parsing (`main.rs::run_control` →
  a new `animusd::run_node_control_with_stores`), exactly as those flags
  already thread through `--config`/`--node` and `--cluster N`.
- **The handles moved off `DataRole` onto `ClientCtx` itself** —
  `ClientCtx::segment_store`/`backup_store`, no longer `Option`-wrapped
  behind a data role at all, since every node shape now provisions a real
  value (the `Fs` variant costs nothing to construct where it's never
  chosen as a placement candidate — see below). `DataRole` now holds only
  genuinely data-role-specific fields (`rmw_lock`, `raftkv_metrics`,
  `base_id`, `stream_seal_knobs`, `change_rates`). Every existing call
  site that read `ctx.data().segment_store`/`ctx.data().backup_store`
  (`backup_capture.rs`, `backup_restore.rs`, `dynamo.rs`,
  `dynamo_streams.rs`, `index_drain.rs`, `segment_janitor.rs`) now reads
  `ctx.segment_store`/`ctx.backup_store` directly; `client_ctx_host.rs`'s
  `BackupObjectStore` impl for `ClientCtx` now always answers `Some(..)`
  (the trait itself stays `Option`-returning for a genuinely store-less
  host — `animus_node::backup_janitor`'s own `ControlOnlyStore` test
  double). `ClientCtx::data_opt()` — the non-panicking accessor
  `segment_janitor.rs` was the sole caller of — is deleted as dead code;
  `ClientCtx::data()` (panicking) is unchanged and still gates the
  genuinely data-role-only fields.
- **A control-only node is never chosen as a segment/backup-store replica
  target**, so this fix changes nothing about *what* gets replicated where
  — only *who can drive reclaim*. `MetaCommand::RegisterNode`'s own apply
  arm (`animus-control::meta.rs`) has always excluded a `role == "control"`
  registration from claiming `Metadata::members` (the placement
  candidate pool `ClusterSegmentStore`'s `ControlPlacementView` reads);
  this predates and is unaffected by this fix.
- **Effect on each consumer**: the segment janitor's phases 2/3 (object
  deletion, replica repair) and phase 1b's physical delete now run
  identically on a control-only leader; the backup completion aggregator
  can now durably `put` a completed backup's manifest object there too;
  the backup janitor can now physically reclaim a marked-deleted or failed
  backup's objects there too; the PITR janitor's retention loop can now
  reclaim PITR segment objects there too. **Unaffected, for a structural
  reason this fix cannot address**: `pitr_snapshot_loop`'s own `BeginBackup`
  *capture* step, and the on-demand capture driver (`backup_capture.rs`)
  generally — capture is per-tablet, leader-side, and a control-only node
  never hosts (so never leads) a CP-data tablet at all; a control-only
  leader still correctly proposes `BeginBackup`/tags rows, it simply never
  has a tablet of its own to capture from. This is not a residual of this
  fix — it was never in scope, since no `SegmentStoreHandle`-shaped
  provisioning gap was ever the reason capture couldn't run there.
- **Regression**: `crates/animusd/tests/stream_janitor.rs::
  segment_janitor_reclaims_objects_from_a_genuinely_control_only_leader` —
  a genuine split deployment (3 control-only + 2 data-only nodes, no
  combined-mode node anywhere) whose control leader (necessarily one of
  the control-only trio, since a data-only node never registers a local
  control `RaftNode`) reclaims a sealed stream's segment objects on its
  own, including the physical on-disk delete at every recorded (data-only)
  replica. See `crates/animusd/CLAUDE.md`'s matching entries for the full
  per-module account.

## As-built amendment (2026-09-04, issue #593 — atomic PITR base tag)

Closes the "vanishingly brief window" §9's own Train 3 PR① amendment
(2026-08-27) named as an accepted residual for `MetaCommand::
MarkBackupPitrBase`. It was not vanishing in practice: right after
`UpdateContinuousBackups(Enabled: true)`, `pitr_snapshot_loop` proposes its
first base snapshot via `BeginBackup` and only afterwards proposes
`MarkBackupPitrBase` once it observes that row exist. Between the two
commits — a real committed window, not merely a scheduling artifact — the
row is an ordinary `Creating` `BackupRow` with no PITR tag, so any consumer
that hides PITR base snapshots by consulting `Metadata::pitr_base_backups`
(`ListBackups`'s default `USER` filter, `animusd`'s console per-table
backups projection) briefly showed it as a user backup.
`console_table_config::table_detail_shows_pitr_status_and_backups`
reproduced it end to end (previously tolerated with a converged-or-timeout
poll; now a strict poll that fails on the very first sighting of a second
row — see its own updated doc).

- **The fix: fold the tag into the mint.** `MetaCommand::BeginBackup` gained
  a `pitr_base: bool` field (`false` for every existing proposer — an
  explicit `CreateBackup`, the admin seeder, every pre-existing test
  fixture — `true` only for `pitr_janitor::pitr_snapshot_loop`'s own
  internally-triggered proposal). `Metadata::apply`'s `BeginBackup` arm
  inserts `backup_id` into `Metadata::pitr_base_backups` in the SAME apply
  that mints the row, whenever `pitr_base` is set — so every replica that
  ever observes the row observes it already tagged. There is no longer a
  committed state in which a PITR base snapshot exists untagged, by
  construction rather than by convergence.
- **`MetaCommand::MarkBackupPitrBase` is deleted outright** — its own apply
  arm, mirror arm, `is_relayable_command` classification
  (`animus-node::wire`), and every construction site (across
  `animus-control`'s own tests, `animus-node::backup_janitor`'s test
  fixture) are gone. It was never wire-visible and had exactly one
  legitimate proposer (`pitr_snapshot_loop`), so nothing else needed a
  migration path — this repo makes no back-compat promise across
  revisions anyway (root `CLAUDE.md`).
- **The self-healing sweep is gone too, not just narrowed** — it existed
  solely to close the gap a dropped `MarkBackupPitrBase` ack could leave;
  with the tag riding the same command as the mint, `BeginBackup` either
  commits fully tagged or doesn't commit at all, so there is nothing left
  for a sweep to heal. `pitr_snapshot_tick` (`animus-node::pitr_janitor`)
  no longer scans `Metadata::backups` at all on its periodic-snapshot path.
- **`BackupRow.backup_name`'s internal-marker prefix
  (`__pitr_base__{table}`) survives as a purely cosmetic label** — it was
  never the source of truth even before this fix (`Metadata::
  pitr_base_backups` always was), and now has no self-healing-sweep
  consumer at all; it remains solely for a human reading `DescribeBackup`/
  `ListBackups`/the console.
- **Every one of `MetaCommand::BeginBackup`'s ~30-plus existing
  construction sites needed the new field** — the exact compiler-enumerated
  fan-out class root `CLAUDE.md`'s engineering-practices log already
  documents for `backup_name`'s own addition (Train 1 PR④'s as-built
  amendment, above): `cargo build`'s `error[E0063]` enumerated every site
  across `animus-control`, `animus-test`, and `animusd`, rather than
  trusting a grep pass to have found them all.
- **Regression**: `animus-control::meta::tests::
  begin_backup_pitr_base_tags_atomically_with_the_mint` (a PITR-enabled
  table's `BeginBackup { pitr_base: true, .. }` is tagged immediately, in
  every reachable `BackupStatus` — `Creating` at mint, then `Available`
  after `CompleteBackup`) and `begin_backup_without_pitr_base_is_never_
  tagged` (an ordinary on-demand backup is never tagged); the mirror-level
  twins in `animus-control::mirror::tests` prove the row and the tag
  mirror to the system keyspace in the same write batch. At the wire
  layer, `animusd::tests::console_table_config::
  table_detail_shows_pitr_status_and_backups` now polls the console's own
  per-table backups projection repeatedly through several of
  `pitr_snapshot_loop`'s own 200ms tick intervals and fails on the first
  sighting of more than the user's own one backup, rather than tolerating
  eventual convergence to it.
- **Generalizable lesson** (recorded in `docs/engineering-lessons.md`): a
  piece of state that must never be observable in an intermediate,
  half-formed shape has to be minted by ONE replicated command, not a
  mint-then-tag pair — even when the gap between the two commits is
  expected to be small in practice, "small" is not "never," and a
  consumer polling fast enough (or simply unlucky) will eventually observe
  it. Prefer widening the minting command's own signature (a bool/enum
  field, `error[E0063]`-enumerated across every construction site) over a
  second side-tag command, even when the side-tag avoids touching more
  call sites — the atomicity is the point, and a "self-healing sweep" is a
  tell that the two-command design's own gap was already known to be real,
  not merely theoretical.

## As-built amendment (2026-09-06, roadmap U-07 — `GET /admin/backup-store`)

A new observability route, `GET /admin/backup-store`, surfaces this
subsystem's own store config, a bounded live object-count/byte scan, and
the on-demand backup janitor's (§3) own live phase/counters — the first of
docs/roadmap.md's U-07 batch, chosen deliberately as the template the
other three (`/admin/ttl`, `/admin/gc`, `/admin/segment-store`) copy. The
janitor loop (`animus_node::backup_janitor::backup_janitor_loop`) now
publishes a small `JanitorProgress` snapshot (phase, last tick, cumulative
`backups_seen`/`objects_reclaimed`, the last error, the backup id currently
being worked) through a new capability trait,
`animus_node::host::BackupJanitorProgressHost`, into an
`Arc<std::sync::Mutex<JanitorProgress>>` `ClientCtx` holds — no change to
the janitor's own reclaim decisions, only instrumentation layered on top of
each existing phase transition. See ADR 0020's own matching as-built note
for the full route design (redaction, the bounded-scan rationale, and the
test references) — this amendment exists only to record that the route
lives in this subsystem's own territory too.

## Amendment (2026-09-06): S-04 — S3 `SegmentStore` backend — design

`docs/roadmap.md`'s S-04 picks up exactly the follow-up this ADR's
"Why this reuses `SegmentStore`, not a new store" section and §1's "The S3
backend is a future trait-swap" paragraph deferred. This amendment records
the three-PR plan and the design decisions PR 1 (`animus-s3`, merged
alongside this amendment) already had to make; it does not change anything
about capture, the catalog, or restore — exactly the promise §1 made.

### The three-PR plan

1. **Client** (this PR) — a new crate, `animus-s3`: a pure AWS Signature
   Version 4 request signer and a minimal `put`/`get`/`delete`/`head`/
   `list_objects_v2` S3 client generic over an explicit `Transport` seam.
   No `SegmentStore` impl, no `s3:` URI, no `animus-env` dependency —
   `crates/animus-s3/CLAUDE.md` has the full design write-up. Ships alone
   because it's independently testable (an in-process fake transport with
   real signature verification) and independently reviewable (the signing
   math is its own concern, separate from how a `SegmentStore` wraps it).
2. **Backend + wiring** (not yet started) — `S3SegmentStore` implementing
   `animus_env::SegmentStore` over `animus_s3::client::S3Client` (the
   `prod::HyperRustlsTransport` in production, `fake::FakeS3` under
   `assert_segment_store_contract`), plus `s3://bucket/optional/prefix`
   URIs on both `--segment-store` and `--backup-store`. This is where
   `env.wall_now()` enters (`animus-s3`'s own `S3Client` methods take
   `now_epoch_ms` as a parameter — see that crate's CLAUDE.md — so the
   wrapper's only new "impure" surface is reading the clock once per call
   and threading a retry loop around transient transport failures,
   `SegmentStore`'s own layering decision per its trait doc).
3. **Operator egress + credentials** (not yet started) — `animus-operator`
   restricts/documents egress in `desired/networkpolicy.rs` (today ingress-
   only, egress unrestricted by omission — §1's own "deliberate, narrowly-
   scoped exception" framing) for exactly the object-storage endpoint, plus
   a credential `Secret` the CRD references (mirroring `spec.tls`'s
   pre-existing/cert-manager `Secret` precedent, ADR 0064) rather than
   inlining a secret into the generated `ConfigMap`.

### Object layout

One S3 bucket, optionally scoped by a key prefix (`s3://bucket[/prefix]`).
`SegmentStore`'s existing namespace convention maps onto S3 keys 1:1, with
no reshaping: an id `{table}/{label}/{tablet}/{epoch}/{attempt-suffix}`
(stream segments, ADR 0043 §A3) or `backup/{backup_id}/...` (this ADR's own
§1) becomes the S3 object key `[prefix/]{table}/{label}/{tablet}/{epoch}/
{attempt-suffix}` — S3's own `/`-delimited key namespace already matches
the shape `SegmentStore::list`'s prefix filter expects, so `list(prefix)`
is a direct `ListObjectsV2 {Prefix: "[prefix/]" + prefix}` call, no
translation layer.

### Consistency assumptions

S3 (and every credible S3-compatible target: MinIO, localstack) has been
strongly (read-after-write) consistent for both new-object PUTs and
overwrite PUTs since 2020 — this backend assumes that, not S3's old
2006-era eventual-consistency model, matching `SegmentStore`'s own
write-once/read-after-put contract (ADR 0043 §A7) exactly: a `put` that
returns success is immediately visible to a `get`/`head` on any node. Two
narrower assumptions this backend does add, both already true of every
`SegmentStore` implementor's `list`: it is **debug/sweep-only, never
load-bearing for a read** (a reader/sweep resolves an object's id from the
replicated catalog, never from a `list` result), and it must **paginate**
rather than assume one page — `ListObjectsV2`'s own 1000-key page cap is a
hard S3 API limit `animus_s3::client::S3Client::list_objects_v2`'s
`continuation` parameter already threads through (PR 1); the `SegmentStore`
wrapper (PR 2) is responsible for looping pages, never truncating silently.

### Credential sourcing

Static access-key-id + secret-access-key, from node config or an
environment variable — the identical posture ADR 0057's `dynamo_auth`
static credential map already established for this codebase (`SecretKey`-
shaped redaction, never logged, never returned by `/admin/*`; see
`animus_s3::sigv4::Credentials`'s own redacting `Debug`, PR 1). **Instance-
role/IMDS credential resolution is explicitly out of scope** — it would add
a second, EC2-specific credential-discovery code path this codebase has no
other reason to carry, and every target this backend needs to support
today (real S3 with a bucket-scoped IAM user, MinIO, localstack) works with
static credentials. Revisit only if a deployment genuinely requires it.

### Region and endpoint

**Path-style addressing with an explicit, required endpoint** (PR 1's
`S3Config::endpoint`) — not virtual-hosted-style, and not "derive the
endpoint from the region" the way the official AWS SDKs default to. This
is deliberate, not a shortcut: an explicit endpoint is what makes MinIO and
localstack work at all (they have no `s3.<region>.amazonaws.com`-shaped
DNS name), and it costs nothing against real AWS S3, which accepts
path-style addressing for any bucket. Virtual-hosted-style stays a
documented, unimplemented option (`crates/animus-s3/CLAUDE.md`) for a
later PR if a real deployment ever needs it (e.g. a proxy that only
recognizes the virtual-hosted form).

### TLS

Reuses the rustls trust stack ADR 0064 established for this codebase (the
`ring` crypto provider, the same "no back-compat, no bespoke abstraction
per crate unless the trait shape genuinely doesn't fit" posture) — PR 1's
`animus_s3::prod::HyperRustlsTransport` builds its `RootCertStore` from
`rustls-native-certs` (the platform trust bundle) rather than ADR 0064's
own explicit-PEM-file `TlsConfig`, since this is an *outbound* client
verifying a well-known public/operator-supplied CA, not a peer-to-peer
mutual-TLS handshake with no ambient trust root to lean on. A plain
`http://` endpoint is allowed only via an explicit, opt-in transport
constructor (`HyperRustlsTransport::new_allow_insecure_http`) — never
inferred from the endpoint string itself — reserved for a loopback MinIO
dev/test target; a real deployment's `s3://` config always negotiates TLS.

### Testing

- **Signing**: pure known-answer tests against AWS's own published
  `aws-sig-v4-test-suite` vectors (the same suite ADR 0057 vendors for the
  DynamoDB verifier), plus a dev-dependency-only proof that this crate's
  copied HMAC signing-key chain and `animus_dynamo::sigv4`'s
  independently-implemented one agree byte-for-byte on identical inputs
  (`crates/animus-s3/tests/sigv4_chain_matches_dynamo.rs` — the one place
  `animus-s3` names `animus-dynamo` at all, and only in
  `[dev-dependencies]`; see that crate's `CLAUDE.md` for why the chain was
  copied rather than depended on).
- **Contract**: an in-process fake S3 (`animus_s3::fake::FakeS3`) with real
  SigV4 signature verification, exercising the client end to end with no
  sockets (PR 1). PR 2 runs `animus_env::test_support::
  assert_segment_store_contract` against an `S3SegmentStore` built on this
  same fake, exactly like `FsSegmentStore`'s own contract test does today.
- **Opt-in real endpoint**: a `prod`-feature-gated test that skips (prints
  a line, does nothing) unless `ANIMUS_S3_TEST_ENDPOINT` is set — so the
  workspace gates stay green with no MinIO/localstack infrastructure, but a
  session with one available can drive a real round trip. Never `#[ignore]`
  — see `crates/animus-s3/CLAUDE.md`'s Testing section for the exact
  environment variables and a MinIO invocation.

None of this changes capture, the catalog, or restore (§1's own promise) —
PR 2, when it lands, is a pure trait-swap: an operator who configures
`--backup-store s3://bucket/prefix` gets a backup pipeline identical in
every other respect to one configured with `fs:PATH` or the default
`cluster`.

### As-built (2026-09-06): PR 2 — `S3SegmentStore` and `s3:` URIs

PR 2 landed as designed above, with the following as-built specifics:

- **Where it lives**: `animus_env::S3SegmentStore<T: animus_s3::client::
  Transport>` (`crates/animus-env/src/s3_store.rs`), gated behind the same
  `prod` Cargo feature as `FsSegmentStore` — `animus-env` may depend on
  `animus-s3` after all (the dev-dependency `animus-s3` → `animus-dynamo`
  edge PR 1's own `CLAUDE.md` flagged as needing confirmation is a
  `[dev-dependencies]`-only edge of `animus-s3`, never built when
  `animus-s3` is used as a plain library dependency, so no cycle exists).
  Generic over `T: Transport` rather than concretely typed to
  `HyperRustlsTransport` specifically so the same store type is exercised
  against `animus_s3::fake::FakeS3` in tests and the real transport in
  production, with no second implementation of the write-once/list logic.
- **Object layout**: exactly as designed — `[prefix/]{id}` verbatim, no
  escaping (every character a production id can contain is already a
  literal-safe S3 key byte, and `animus_s3::client::S3Client` percent-
  encodes the wire/signing forms independently and exactly once already).
- **Write-once**: enforced with a `GET`-then-compare-then-`PUT` (real S3 has
  no built-in conditional "put only if absent" this client sends), matching
  `FsSegmentStore::put`'s own local read-then-compare-then-write shape
  exactly — an identical-content re-put skips the network `PUT` entirely; a
  differing-content re-put is a hard `Err` with the stored bytes untouched.
- **Retry**: a small, fixed, non-configurable bounded retry (3 attempts,
  linear 100ms/attempt backoff) on a transport failure or a `5xx` — never on
  a `4xx` or `NotFound`/`AccessDenied`. Since this store is not `Env`-generic
  (mirrors `FsSegmentStore`'s own concrete shape), the backoff sleep is a
  plain `tokio::time::sleep` under a module-level `#[allow(clippy::
  disallowed_methods)]`, the identical justification `animus-env`'s own
  `prod.rs` module carries.
- **Config surface, as built**: `s3://<bucket>[/<prefix>]?endpoint=<scheme://
  host[:port]>&region=<region>[&path_style=true][&insecure_http=true]` on
  both `--segment-store`/`--backup-store` (`animusd::S3StoreConfig`,
  `main.rs`'s `parse_s3_uri`). `endpoint`'s own `http://`/`https://` prefix
  must agree with `insecure_http` (an `http://` endpoint always needs
  `insecure_http=true`, and vice versa) — this deliberately never infers a
  TLS decision from the scheme string alone, only from the explicit query
  key, so a `http://` typo can't silently downgrade a production config.
  `insecure_http=true` against a non-loopback host is refused unless
  `--allow-insecure-s3` is also given. `path_style=false` (virtual-hosted
  addressing) is rejected as unimplemented rather than silently ignored,
  since the underlying client only ever addresses path-style.
- **Credentials, as built**: deliberately **not** a new `ClusterConfig`
  field — this codebase's own history with adding a `ClusterConfig` field
  (documented in `crates/animusd/CLAUDE.md`'s `config.rs` entry: each of
  `dynamo_auth`/`cluster_settings` triggered a ~55-60-call-site `error
  [E0063]` fan-out across every `ClusterConfig { .. }` literal) made that
  the wrong shape for a feature whose own design already calls for static,
  file/env-sourced credentials with no cluster-wide semantics of their own.
  Instead: a standalone `--s3-credentials PATH` JSON file (`{"access_key_id":
  "...", "secret_access_key_file": "..."}` or `{"access_key_id": "...",
  "secret_access_key_env": "VAR"}, exactly one of the two secret sources),
  mirroring ADR 0064's own `tls` section's cert/key-**path** precedent
  (never an inline secret) more closely than `dynamo_auth`'s in-`ClusterConfig`
  static map — falling back to the `ANIMUS_S3_ACCESS_KEY_ID`/
  `ANIMUS_S3_SECRET_ACCESS_KEY` environment variables when the flag is
  omitted. A missing credential is a startup error naming the field/flag,
  never a panic; an `s3://` store configured with no credential resolvable
  anywhere is also a startup error, naming both sourcing options.
- **The insecure-HTTP gate, as built**: `parse_s3_uri` performs the whole
  gate at parse time (loopback-host check via a conservative literal
  `localhost`/`127.0.0.0/8`/`::1` match, never a DNS resolution), not at
  store-construction time — a config that will be refused is refused before
  any other startup work happens.
- **Testing, as built**: `assert_segment_store_contract` against
  `S3SegmentStore<animus_s3::fake::FakeS3>` (`animus-env`'s own
  `s3_store::tests`, `cargo test -p animus-env --all-features`) is the
  load-bearing, no-network proof — plus a pagination-specific test proving
  the store's own `list` loop follows `next_continuation_token` across more
  than one page. The opt-in real-endpoint counterpart
  (`crates/animus-env/tests/s3_segment_store_minio.rs`) mirrors PR 1's own
  `ANIMUS_S3_TEST_ENDPOINT`/`_BUCKET`/`_ACCESS_KEY_ID`/`_SECRET_ACCESS_KEY`
  environment variables exactly, driving the same contract assertion
  instead of raw client calls. `animusd`'s own end-to-end proof is an
  in-crate test (`s3_store_handle_tests`, `lib.rs`, needing
  `BackupStoreHandle`/`SegmentStoreHandle`'s `pub(crate)` visibility) that
  builds a real `BackupStoreHandle::S3`/`SegmentStoreHandle::S3` directly
  over `animus_s3::fake::FakeS3` and drives the exact `put`/`put_sealed`/
  `list_local`/`get_local`/`get_any`/`delete_local` methods the capture
  driver, backup janitor, and `/admin/backup-store`/`/admin/segment-store`
  routes call in production — **deliberately not** a full running `Node`
  with an injected transport driving `CreateBackup` over the real DynamoDB
  wire, which would need either widening `BackupStoreHandle` to `pub` (a
  public-API change beyond this PR's scope) or a second, parallel
  node-construction entry point; a live-node fake-transport e2e is a
  reasonable follow-up, not required for this PR's own correctness claim.
  `main.rs`'s own `parse_segment_store`/`parse_backup_store` unit tests
  cover the URI-shape/credential/insecure-http-gate matrix directly.
- **Admin surface**: `GET /admin/segment-store`/`GET /admin/backup-store`
  render `"kind": "s3"` with a `location` of `s3://bucket[/prefix]@host`
  (host only — no query string, no credentials) via the pre-existing
  `StoreView`/`redact_store_location` machinery — no new admin-side code
  beyond the `S3` arm of `StoreView`'s two `From` impls, since both routes
  already project through that one type.
- **PR 3 (operator egress + credential secret)** remains, unchanged in
  scope from the plan above.

### As-built (2026-09-06): PR 3 — operator egress + credential secret (closes S-04)

PR 3 landed as designed, closing `docs/roadmap.md`'s S-04 item entirely
(all three PRs now done — see that file's own maintenance rule: the S-04
section is removed from the roadmap in the same change). The full
as-built account lives in [ADR 0060](0060-kubernetes-operator.md)'s own
"Amendment (2026-09-06): S-04 PR 3" (`animus-operator`'s crate, not this
one) — this note only records the piece that touches this ADR's own
scope: `AnimusClusterSpec.s3`'s two store fields are the identical
`s3://...` URI shape this ADR's PR 2 amendment specified for
`--backup-store`/`--segment-store`, unchanged; the operator only *routes*
that string onto the flag (plus a generated `--s3-credentials` file
pointing at a mounted `Secret`) rather than reinterpreting it. Nothing
about capture, the catalog, restore, or the `S3SegmentStore`/`S3StoreConfig`
shapes PR 2 built changed to accommodate this — the operator is a pure
consumer of the same command-line contract every other deployment shape
(bare-metal `--config FILE --node I`, `animusd control`) already used.

## As-built amendment (2026-09-07, ADR 0061 rung D4 PR 5 — deterministic coverage for the backup janitor's own loop)

The backup janitor (§3, `animus_node::backup_janitor::backup_janitor_loop`)
now has its own deterministic, `SimEnv`-driven regression, riding on
`animusd`'s multi-node `SimCluster` fixture (ADR 0061 Phase D) rather than
only the primitive-level `RaftNode<SimEnv>` unit tests the loop's own
module already carried since rung C2. Nothing about the janitor's own
decisions changed — this closes a coverage gap, not a behavior gap.

**What moved (mechanical, not a redesign)**: two things were still pinned
to the concrete `ClientCtx` alias (`E = ProdEnv, R = AnimusdRelayClient`)
and are now `<E: Env, R: RelayClient>`-generic — `animusd::client_ctx_
host.rs`'s four `ClientCtx` implementations of `animus_node::host`'s
`ControlLeaderHost`/`BackupObjectStore`/`BackupJanitorProgressHost`/
`TtlScanHost` traits, and `animusd::backup_janitor`'s own thin wrapper
around `animus_node::backup_janitor::backup_janitor_loop`. Every field and
method each impl delegates to (`self.edge`, `self.backup_store`,
`self.backup_janitor_progress`, `dynamo::kind_write_item_at_leader::<E,
R>`) was already `E`/`R`-agnostic or already generic — this rung is a pure
signature widening. `animus_node::host`'s own trait definitions needed no
change at all.

**Store choice**: `SimCluster` now builds every node's `ClientCtx::
backup_store` as `BackupStoreHandle::S3` wrapping a clone of ONE shared
`SimSegmentStore` (`animus-sim`'s own deterministic `SegmentStore` corpus
implementor), not a per-node placeholder `Fs` directory the way every
other `SimCluster`-driven module's own fixture still does for the fields
nothing else reads. This is the faithful choice, not a simplification: a
real `--backup-store s3://...` bucket has no per-node locality at all
(`BackupStoreHandle::S3` already holds `Arc<dyn SegmentStore>` specifically
so it can be substituted this way, the same seam `lib.rs`'s own
`s3_store_handle_tests` uses over `animus_s3::fake::FakeS3`), and sharing
one store is what makes the leader-gating scenario below meaningful — a
per-node-local store would make "did a follower's janitor touch the store"
trivially true by construction. `backup_janitor_loop` is now spawned
unconditionally on every node (mirroring `heartbeat_loop`'s own always-on
D4 PR 1 spawn, not `auto_split_loop`'s opt-in one — this loop's own leader
gate already makes a non-leader tick a cheap idle no-op).

**Five scenarios** (`crates/animusd/src/sim_cluster_backup_janitor.rs`,
each replayed at 5 seeds): (a) a completed (`Available`) backup marked
deleted is reclaimed — its manifest and data-chunk objects, seeded
directly into the shared store, are gone, and the catalog row itself is
gone (not merely `Expired`), with the leader's own `JanitorProgress`
ending `Idle` having seen the backup and reclaimed both objects; (b) a
`Failed` backup (the completion aggregator's own stuck-timeout shape) is
reclaimed the identical way, proving the janitor's `Expired`-or-`Failed`
admission gate rather than only the `DeleteBackup`-driven path; (c) leader
gating in two parts — a follower's own `JanitorProgress` never leaves
`Idle` while the leader alone reclaims a deleted backup, and a real
`RaftCore::transfer_leadership` handoff issued immediately after
`MarkBackupDeleted` commits still converges to exactly one reclaim with no
error recorded on any node's own progress (the mark/reclaim/finalize
sequence is idempotent regardless of which of the old or new leader's own
tick actually does the work); (d) the control-plane leader itself crashes
right after `MarkBackupDeleted` commits, and is restarted only once the
surviving two nodes have already reclaimed the backup on their own — the
restarted node's own view converges too, with no stale error; (e) a
backup that stays `Available` (never marked deleted, never failed) is
never touched over a long window — every node's own `JanitorProgress.
backups_seen` stays 0 and the store keeps every object.

**No janitor bug found.** All five scenarios (and their `_over_seeds`
siblings) hold at every seed tried; the two real findings this rung's own
build produced were both test-harness timing gaps, not production
defects — see `crates/animusd/CLAUDE.md`'s matching D4 PR 5 entry and
`docs/engineering-lessons.md` for the general lesson each generalizes to
(a `propose` must be given time to replicate and commit before the node
that issued it is crashed, or the entry is stranded and lost rather than
inherited by the survivors).

**What stayed on `ProdEnv`**: `crates/animusd/tests/dynamo_backup.rs`'s
own `create_backup_round_trip_survives_table_drop_and_janitor_reclaims`
fuses the janitor's own row-removal convergence (its very last assertion)
into one long test that also proves the DynamoDB wire shapes
(`CreateBackup`/`DescribeBackup`/`ListBackups`/`DeleteBackup`'s JSON
responses, the `BackupSizeBytes` freeze across a table drop, the
immediate-`DELETED`-then-`BackupNotFoundException` wire contract) —
`SimCluster` drives no DynamoDB backup/restore operations at all (a named
residual since ADR 0061 rung D2), so none of that wire-shape machinery is
reachable through it regardless of how far this rung widens the janitor's
own host seam. Splitting the janitor's own tail assertion out into a
second, separate test would only duplicate the setup this one test already
does once; nothing was removed from `dynamo_backup.rs`. What D4 PR 5 adds
is a companion, not a replacement: the primitive-level proof
(`animus_node::backup_janitor::tests`, single-voter `RaftNode<SimEnv>`,
synthetic `BackupObjectStore`), the wire-level end-to-end proof
(`dynamo_backup.rs`, real `ProdEnv`/sockets/disk), and now this
multi-node, fault-injecting, deterministic middle tier
(`sim_cluster_backup_janitor.rs`) that neither of the other two reaches:
leader gating, a real leadership handoff, and a crashed-leader/restart
recovery, all replayable from a bare seed.
