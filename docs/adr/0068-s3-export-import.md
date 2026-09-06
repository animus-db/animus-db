# ADR 0068 — S3 export and import (S-05)

- **Status:** Accepted
- **Date:** 2026-09-06
- **Origin:** `docs/roadmap.md`'s S-05, itself the follow-up ADR 0059 §1's
  "S3 export/import... explicitly deferred" line named from the start, and
  ADR 0059's own 2026-09-06 S-04 amendment's "future PR" closing note.
- **Amends:** ADR 0059 (adds a sixth wire-facing capability family, reusing
  its capture/catalog conventions); ADR 0066 (extends the `OpClass`
  classification table with three more `Backup`-class operations).
- **Depends on:** ADR 0059 (the on-demand backup catalog/capture
  conventions this reuses), S-04 (`animus-s3`, `animus_env::S3SegmentStore`,
  `s3:` store URIs — landed 2026-09-06, unblocking this item per the
  roadmap's own sequencing note).

## Context

ADR 0059 built AnimusDB's own internal backup/restore/PITR mechanism —
snapshots and sealed change-log segments in a cluster-operated
`SegmentStore`, restorable only back into this same cluster. It explicitly
deferred a second, DynamoDB-shaped capability: exporting a table's data into
a **customer-owned** S3 bucket, in DynamoDB's own JSON export layout, for
consumption by tools outside this database entirely (Athena, Spark, a data
lake, `ImportTable` into an actual AWS DynamoDB table, or — this roadmap
item's own second half, not built here — `ImportTable` back into this
adapter). The reason to defer was stated plainly in ADR 0059 §1: the target
store is not this cluster's own operational object store, so the design
needs its own answer for *whose* S3 credentials/endpoint are used and how a
request names an arbitrary customer bucket — "a distinct wire model."

S-04 (2026-09-06) built exactly the missing piece: `animus-s3` (a pure SigV4
signer + minimal S3 client over an explicit `Transport` seam), a real
`animus_env::S3SegmentStore<T: Transport>`, and `s3://bucket/prefix?...`
URIs on `--segment-store`/`--backup-store`. That backend was built for this
node's **own** operational store — a fixed bucket, chosen once at process
startup, credentials resolved once at startup. S-05's job is to reuse that
same client/store machinery for a **per-request**, customer-named bucket
instead, and to define the DynamoDB wire surface and object layout on top.

This ADR originally covered the S-05 PR (1) scope only — the export trio
(`ExportTableToPointInTime`/`DescribeExport`/`ListExports`). **The import
trio (`ImportTable`/`DescribeImport`/`ListImports`, PR (2)) landed
2026-09-06 too** — see this ADR's own "As-built amendment (2026-09-06, S-05
PR 2 — the import trio)" section, below §9, for what §6's sketch became.
The deterministic simulation corpus (PR (3)) is still sketched in §8 only;
`docs/roadmap.md`'s S-05 entry stays in place until it lands.

## Decision

### 1. Export is a leader-driven job, not a per-tablet distributed capture

ADR 0059's on-demand backup capture is deliberately **per-tablet,
leader-side, and crash-resumable**: every pinned tablet's own leader
independently sweeps its share into chunked objects, reports completion
through a `(backup_id, tablet)`-keyed catalog, and a control-plane-leader
aggregator declares the whole backup `Available` once every tablet has
reported. That design earns its complexity because a backup's own object
namespace is per-tablet and a leader crash mid-capture must resume from
exactly where it left off without re-deriving different bytes.

S-05 makes a different, deliberately simpler call: **one export job, run
once, on whichever node's DynamoDB wire edge received the
`ExportTableToPointInTime` call.** The job reads the *whole* table through
the existing native quorum range scan (`ClientCtx::cp_scan` — the identical
primitive an ordinary `Scan` operation already uses, which itself fans out
across every tablet a table currently has, in token order, with no
tablet-by-tablet bookkeeping the caller has to manage) rather than
orchestrating capture per pinned tablet. This is the right trade for this
feature specifically:

- An export's payload is customer-facing DynamoDB JSON of **base rows
  only** — no LSI/footprint rows, no per-tablet chunk objects to
  reassemble, nothing that benefits from a per-tablet split the way a
  restorable backup's own internal chunked format does.
- `cp_scan` already reads through intent resolution and already tolerates
  a concurrent split (a retiring tablet's range is simply covered by
  whichever tablet(s) currently own it at scan time) — the exact two
  properties ADR 0059 §5/§6 had to build bespoke machinery for
  (`traces_to_pinned`/`live_split_descendants`) are free here.
- The genuine cost of this simplicity is **no crash-resumability** — see
  §9's "Known residuals."

### 2. The customer bucket store: a per-request `S3SegmentStore`, node-level credentials

An export names an arbitrary customer bucket (`S3Bucket`/`S3Prefix` on the
wire request) that cannot be known at node startup, so it cannot reuse
`--backup-store`/`--segment-store`'s own fixed-at-startup `SegmentStoreHandle`
directly. Instead:

- **New CLI flags, `--export-s3-endpoint`/`--export-s3-region`**, resolved
  once at node startup into an `ExportS3Config` (endpoint/region/
  insecure-http/credentials — everything **except** bucket/prefix, which
  arrive per-request). **Credentials are reused, not duplicated**: the same
  `--s3-credentials PATH` / `ANIMUS_S3_ACCESS_KEY_ID`+
  `ANIMUS_S3_SECRET_ACCESS_KEY` resolution S-04 already established for
  `--segment-store`/`--backup-store s3://` is the one and only S3 credential
  source this node ever has — a node authenticates to S3 as one identity
  regardless of which bucket (its own operational store, or a customer's
  export target) it's writing to. A second, independent credential flag for
  exports would add configuration surface with no real security benefit
  (this was the ADR's own "pick one, justify" decision point — reusing wins
  because nothing about *whose* bucket is being written changes *which*
  credential the node authenticates with).
- **A per-request `S3SegmentStore` is built fresh, per export**, from that
  fixed endpoint/region/credentials plus the request's own bucket/prefix —
  never cached, since two different exports can name two different buckets.
- **`ClientCtx::export_store_factory` is the seam this is built behind**: a
  `Fn(&str, Option<&str>) -> io::Result<Arc<dyn SegmentStore>>`, held in an
  `Arc<Mutex<_>>` so it can be swapped in place. The production default
  (`default_export_store_factory`) builds a real
  `S3SegmentStore<HyperRustlsTransport>` from the node's `ExportS3Config`,
  or a clear "not configured" error when the node has none.
  `Node::set_export_store_factory` is a **public, always-compiled** hook
  (not `#[cfg(test)]`, unlike the crate's existing `test_ctx` field — an
  external `tests/*.rs` integration binary needs a real, non-test-cfg-gated
  entry point) that a test uses to install a factory built over
  `animus_s3::fake::FakeS3` instead — no real sockets, no MinIO.
- **`S3BucketOwner`/`S3SseAlgorithm`/`S3SseKmsKeyId` are accepted and
  ignored** — documented here rather than left for a reader to discover:
  this adapter neither verifies S3 bucket ownership (no cross-account
  concept at all) nor applies server-side encryption of its own (an
  operator wanting encryption-at-rest for the export bucket configures it
  on the bucket itself, outside this adapter's knowledge).
- **CLI reach is deliberately partial, a named and precedented gap**: the
  two new flags reach `--config FILE --node I` only (`main.rs::run` →
  `run_single` → `run_node_with_cluster_settings` →
  `run_node_with_streams_quiesce_and_ttl_sweep_interval` →
  `BoundNode::start_with_growth` → `spawn_common_tail`), mirroring
  `--segment-store`/`--backup-store`'s own well-established partial CLI
  reach on `--cluster N`/`--cluster-control`+`--cluster-data`/`animusd
  control`/`animusd data`/`animusd join`. A cluster started any other way
  has no CLI-driven route to real S3 export credentials yet — it still
  serves every export wire operation, but a real `ExportTableToPointInTime`
  fails with the "not configured" error until either this gap closes or the
  process is restarted under `--config`/`--node`.

### 3. A replicated export catalog in `Metadata`

`Metadata::exports: BTreeMap<ExportId, ExportRow>` — `ExportId = String`,
an ARN-shaped opaque identity exactly like `BackupId`
(`<TableArn>/export/<id>`, [`wire::export_arn`]), for the identical "never a
table name" reason `Metadata::backups`' own doc states (a drop-then-recreate
of the source table name must never poison a live or completed export row).

`ExportRow` carries: `table`/`table_arn` (data only — the catalog is keyed
by `ExportId`), `s3_bucket`/`s3_prefix`, `format`/`export_type` (always
`DynamoDbJson`/`Full` in practice — see §4), `export_time_ms` (the requested
`ExportTime`, `None` for "current committed state"), `client_token`,
`status` (`InProgress`/`Completed`/`Failed{reason}`), `created_wall_ms`/
`completed_wall_ms`, `item_count`/`billed_size_bytes` (frozen once, by
`CompleteExport`'s own apply arm — mirroring `BackupRow::total_bytes`'s
identical "freeze once, never re-derive" discipline and its own documented
reason: a live re-derivation would collapse to zero the instant something
downstream of the row changed), and `export_manifest` (the written
`manifest-summary.json` object's S3 key, `None` until `Completed`).

Three commands, modelled directly on `BeginBackup`/`CompleteBackup`/
`FailBackup`'s own shapes:

- `MetaCommand::BeginExport` — mints an `InProgress` row. Rejected if
  `export_id` already exists (first-committer-wins on a freshly-minted
  identity, the same shape `BeginBackup` uses) or if `table` has no schema.
  Deterministic apply: every field including `created_wall_ms` travels in
  the command, stamped by the wire-serving node's own `env.wall_now()` at
  propose time (ADR 0051's discipline) — the pure state machine reads no
  clock.
- `MetaCommand::CompleteExport` — rejected if `export_id` is unknown or not
  `InProgress`. Freezes `item_count`/`billed_size_bytes`/`export_manifest`/
  `completed_wall_ms` and flips to `Completed`.
- `MetaCommand::FailExport` — rejected if `export_id` is unknown or already
  `Completed`. Idempotent on an identical repeated `reason`; a genuinely
  different `reason` is a real transition (mirroring `FailBackup`'s
  identical shape).

**All three are on the `is_relayable_command` allowlist — unlike
`CompleteBackup`/`FailBackup`, which are not.** This is the one deliberate
divergence from the backup catalog's own relay classification, and it
follows directly from §1's design: `CompleteBackup`/`FailBackup` are
proposed only by the control-plane-leader-only completion aggregator, which
already holds a live `RaftNode` handle and so never needs to relay.
`CompleteExport`/`FailExport` are proposed by the **export job**, which runs
on whichever node happened to receive the wire request — not necessarily
(and, on any cluster with more than one node, usually not) the control-plane
leader. Missing this on the allowlist would be the exact bimodal
per-process-flake root `CLAUDE.md`'s "grep every gating match site" warning
names; `tests/dynamo_export.rs`'s own `export_full_flow_...` test issues its
`ExportTableToPointInTime` against a deliberately-follower-connected node as
the regression.

**No delete/reclaim command, unlike `Metadata::backups`.** Real DynamoDB's
`ExportTableToPointInTime` has no `DeleteExport` API — an export artifact
lives in the customer's own bucket under the customer's own lifecycle
policy, and the catalog row that describes it is likewise permanent (never
touched by `DropTableSchema`/`DropTableTablets`, the identical ADR 0024
carve-out `Metadata::backups` already gets). This also means there is no
backup-janitor-shaped reclaim loop to build for this feature at all.

Mirrored via the usual `syskv::EntityKind::Export`/`export_key` pair,
following `Backup`'s plain-string-key convention exactly — one new
`EntityKind` variant, one new arm in `mirror.rs`'s exhaustive
`apply_key_write`/`apply_delete`/`apply_and_derive_mirror` matches.

### 4. `ExportFormat`/`ExportType`: only one real value each, for now

DynamoDB's own `ExportTableToPointInTime` accepts `ExportFormat:
DYNAMODB_JSON|ION` and `ExportType: FULL_EXPORT|INCREMENTAL_EXPORT`. This PR
implements exactly one value of each — `DYNAMODB_JSON`/`FULL_EXPORT` — and
**rejects the other two at wire-decode time**, before anything is ever
proposed: `ExportFormat: "ION"` and `ExportType: "INCREMENTAL_EXPORT"` are
both `ValidationException`, with a message naming the value as documented-
unimplemented rather than merely "invalid." `animus_control::ExportFormat`/
`ExportType` are still modelled as full two-variant enums (not
single-variant types) purely for wire/catalog shape fidelity with real
DynamoDB — the state machine and catalog never actually observe the
unimplemented variants in practice, since the wire edge never proposes them.

### 5. `ExportArn` and idempotency

`ExportArn = <TableArn>/export/<id>` ([`wire::export_arn`]), the identical
shape and convention `wire::backup_arn` already established
(`<TableArn>/backup/<id>`) — the whole string **is** the catalog's
`ExportId` key, so no ARN-parsing function exists anywhere in this adapter
to recover an export id from it (mirroring `backup_arn`'s own documented
reason).

`ClientToken` idempotency: `Metadata::export_by_client_token(table,
token)` resolves a matching in-flight-or-terminal export for the same
`(table, token)` pair; `create_export` (`animusd::dynamo`) checks this
**before** ever minting a fresh id, so a retried call with the same token
and table returns the existing export's `ExportDescription` rather than
starting a second job. A different token (or a different table) always
mints a genuinely new export — there is no cross-table token collision
handling, since a token is only ever compared within the scope of the table
it named.

### 6. Import (PR (2) — landed 2026-09-06, see this ADR's own as-built amendment below §9)

`ImportTable` reads the identical layout §7 defines back into this adapter
through the ADR 0059 restore driver's own `KvCommand::SeedBatch` merge
primitive — the same "restore always creates a brand-new table" discipline
ADR 0059 §7 already established for `RestoreTableFromBackup`, sourcing rows
from a customer-supplied S3 prefix (this adapter's own export layout, or
real AWS's identical one) instead of this cluster's own backup catalog.
`DescribeImport`/`ListImports` mirror `DescribeExport`/`ListExports`'s own
read shape over a new, symmetric `Metadata::imports` catalog. This section
is left as the original sketch (below) for historical context; the "As-built
amendment (2026-09-06, S-05 PR 2 — the import trio)" section near the end of
this document is the current, authoritative account of what actually
shipped — read that one first.

### 7. Object layout

Exactly DynamoDB's own documented `DYNAMODB_JSON` export layout, rooted at
`[S3Prefix/]AWSDynamoDB/<export id suffix>/` inside the customer's bucket
(`<export id suffix>` is the random hex tail of the minted `ExportId`, never
the whole ARN — a `/`-bearing string has no business inside an S3 key
segment):

- `_started` — an empty marker object, written **first**, before any other
  object — the durable "an export began at this id" signal (mirrors, in
  spirit, ADR 0059 §4's own durable-before-visible discipline, though this
  marker's own absence is not itself load-bearing for correctness the way a
  missing `manifest-summary.json` is — see below).
- `data/<NNNN>.json.gz` — one gzip-compressed file per
  `EXPORT_CHUNK_ROWS` (1000) items scanned, each line a standalone
  `{"Item": {...DynamoDB JSON...}}` object (`animus_dynamo::wire::
  encode_item`, the exact encoder `GetItem`/`Query`/`Scan` responses
  already use). A DynamoDB tombstone value (a `DeleteItem`'s own
  representation) is skipped — never exported, matching real DynamoDB.
- `manifest-files.json` — one JSON line per data file:
  `{"itemCount", "md5Checksum", "etag", "dataFileS3Key"}`.
- `manifest-summary.json` — written **last**, after every data file and
  `manifest-files.json` itself: `exportArn`/`s3Bucket`/`s3Prefix`/
  `manifestFilesS3Key`/`itemCount`/`billedSizeBytes`/`outputFormat`, plus a
  `version` field. **This ordering is the one genuine durable-before-visible
  guarantee this PR makes**: a reader (real AWS export/import tooling, or
  this adapter's own future `ImportTable`) that finds `manifest-summary.json`
  present can trust every data file and `manifest-files.json` it references
  is already fully written — the identical rule ADR 0059 §4 states for a
  backup's own manifest object.

**`md5Checksum`/`etag` are CRC32, not real MD5** — a deliberate, named
simplification: adding a real MD5 implementation (a new crypto dependency)
purely to populate two fields this adapter's own `DescribeExport`/
`ImportTable` (future) never re-verifies was judged not worth a new
dependency for this PR; `crc32fast` is already a workspace dependency
(`animus-storage`'s SSTable block checksums). A consumer that specifically
re-verifies these hashes against a real MD5 of the object bytes would need
this closed as a follow-up.

### 8. Observability

No new admin/dashboard route in this PR — the export catalog is visible the
same way the backup catalog was before U-02/U-07 built dedicated views for
it: `GET /admin/system-table?kind=export` (the generic system-keyspace
browse, `EntityKind::Export`'s JSON-passthrough render) already renders
every row. A dedicated `GET /admin/exports` route (mirroring `/admin/
backups`) and a dashboard card are reasonable follow-ups, sized the same as
U-02's own original backups-tab PR, not required for this PR's own
correctness claim.

### 9. Testing, and known residuals

**PR (1)'s own test coverage** (`tests/dynamo_export.rs`, real `ProdEnv`
sockets, no MinIO): a full end-to-end flow (create a table, write items,
force a real data-driven split so the export genuinely spans more than one
tablet, issue `ExportTableToPointInTime` against a deliberately
follower-connected node, poll `DescribeExport` to `COMPLETED`, then read
the fake bucket directly — `manifest-summary.json`'s shape,
`manifest-files.json`'s lines, every data file gunzips to `{"Item": ...}`
lines that round-trip through the real `animus_dynamo::wire::decode_item`
decoder back to the exact items written, `_started`'s presence, and
`ListExports`' filtered/unfiltered listing), `ClientToken` idempotency,
unknown-table/unknown-export error shapes, `ExportFormat`/`ExportType`
rejection, and an `ExportTime` with no PITR history rejected as
`InvalidExportTimeException`. Control-crate unit tests
(`crates/animus-control/src/meta.rs`) cover the catalog's own apply logic
in isolation. **PR (3)'s planned `SimEnv` corpus** (reusing
`ANIMUS_BACKUP_SEEDS`'s fault-injection shape against the object store) is
not built in this PR.

**Named residuals, stated plainly rather than silently shipped:**

1. **No crash-resumability.** If the node running an export job crashes
   mid-export, the row is left permanently `InProgress` — there is no
   per-tablet durable cursor the way on-demand backup capture has, and no
   janitor built yet to time out a stuck `InProgress` row the way the
   backup completion aggregator's own `STUCK_CREATING_TIMEOUT` does. A
   follow-up needs either a resumable per-chunk cursor (mirroring
   `backup_capture.rs`'s own `CaptureCursor`) or, more simply for this
   single-job design, a control-plane-leader-driven timeout-and-fail sweep.
2. **`ExportTime` content fidelity is not implemented.** `create_export`
   validates a given `ExportTime` against the table's PITR restore window
   (`validate_export_time`, reusing `Metadata::pitr_restore_window`
   exactly like `RestoreTableToPointInTime` does) and rejects anything
   outside it with `InvalidExportTimeException` — but the payload the job
   actually renders is always the table's *current* committed state,
   never a true point-in-time reconstruction as of the requested instant.
   This is a real gap against this ADR's own §1 "served through the PITR
   machinery" framing, tracked here rather than silently claimed; closing
   it means adapting `backup_restore.rs`'s own PITR-segment-replay
   machinery (base snapshot + change-log replay up to a cutoff, decoded
   in memory) into the export job instead of a live `cp_scan`.
3. **No S3-object reclaim on export failure.** A `Failed` export's
   partially-written data files are never cleaned up — left in the
   customer's bucket as the customer's own responsibility (matching real
   DynamoDB's own contract: nothing about `ExportTableToPointInTime`
   promises the customer's bucket is left pristine on failure either).

## Consequences

- A customer can pull a full snapshot of any table into their own bucket in
  DynamoDB's documented layout, consumable by real AWS tooling (Athena
  table definitions, `ImportTable` into a real DynamoDB table) without this
  adapter needing to implement any of those consumers itself.
- The export job's simplicity (one job, one node, a plain `cp_scan`) is a
  deliberate trade against on-demand backup's own per-tablet distributed
  design — cheaper to build and reason about, at the cost of the two named
  residuals above. Revisit if a real deployment's export sizes or failure
  rate make residual (1) a genuine operational problem.
- `OpClass::Backup` now also classifies all three export operations (ADR
  0066's own classification table gains three rows, no new `OpClass`
  variant) — a credential's policy scoped to `read`/`write`/`ddl` alone,
  with no `backup` class, cannot export a table either, matching the
  identical scoping backup/restore already had.

## Alternatives considered

- **Per-tablet distributed export capture, mirroring on-demand backup
  exactly.** Rejected for this PR: real complexity (a per-tablet catalog,
  a completion aggregator, split-vs-capture race handling) for no benefit
  an export's own base-rows-only, non-restorable-back-into-this-cluster
  payload actually needs — see §1.
- **A second, export-specific S3 credential flag.** Rejected — see §2's
  "pick one, justify" note.
- **Reusing `--backup-store`'s own endpoint/region when it happens to be
  `s3://`-configured**, instead of dedicated `--export-s3-*` flags.
  Rejected: it would make export's own configurability depend on an
  unrelated feature's own store choice (an operator using `fs:`/`cluster`
  for backups would have no way to configure export at all), and it
  conflates two conceptually distinct buckets (this cluster's own
  operational store vs. an arbitrary customer target) behind one flag pair
  that happens to share a shape.

## As-built amendment (2026-09-06, S-05 PR 2 — the import trio)

§6's sketch is now built: `ImportTable`/`DescribeImport`/`ListImports`,
reading the identical DynamoDB-JSON export layout §7 defines (this
adapter's own, or real AWS's) back into a **brand-new** table. The shape
that shipped follows the sketch's own "reuse the restore driver's
`KvCommand::SeedBatch` primitive" instruction, but the *orchestration*
around it is a new, dedicated driver rather than a literal reuse of
`backup_restore.rs`'s existing loop — see below for why.

### The import catalog: `Metadata::imports`, modeled on both `exports` and `restores`

`ImportRow`/`MetaCommand::{BeginImport, CompleteImport, FailImport}` (all
three on the `is_relayable_command` allowlist, the identical §3 reasoning:
the import driver runs on whichever node hosts/leads the destination
tablet, not necessarily the control-plane leader) borrow from **both**
existing catalogs, deliberately:

- **From `exports`**: the identity is the import's own ARN
  (`<TableArn-of-the-freshly-created-target>/import/<id>`,
  [`wire::import_arn`]) — unlike a restore's opaque internally-minted id,
  real DynamoDB's `ImportTable` has an `ImportArn` a client can
  `DescribeImport`/`ListImports` by by name, so the ARN itself is the
  catalog key, `export_arn`'s exact precedent. No delete/reclaim command —
  real DynamoDB's `ImportTable` has no `DeleteImport` API either, and (like
  an export row) an import row is a small, permanent record of "this table
  was imported from here."
- **From `restores`**: the target-table provisioning shape — exactly one
  fresh `Building` destination tablet (`BeginImport` mints it directly,
  the identical `BeginRestore` convention), GSIs resolved at
  `IndexStatus::Creating` but recorded on the row rather than declared
  until `CompleteImport` (the identical backfill-race avoidance
  `RestoreRow::gsi_defs` documents), and — the one deliberate departure
  from restore — **a failed import drops its own half-created target
  table** (`FailImport`'s own doc) rather than leaving it for manual
  cleanup, matching real DynamoDB's documented "a failed `ImportTable`
  rolls back the table it was creating" contract. This is the reason
  `FailImport`'s apply arm freezes the observed counts but never touches
  the tablet map itself — the table drop is the import **driver**'s own
  follow-up call (`ClientCtx::drop_table`, the ordinary path, best-effort:
  a drop failure is logged, not retried, since the target stays a clean,
  state-agnostic `DeleteTable` away from full cleanup either way), not
  something the pure state machine could safely do atomically with the
  fail transition (the drop is itself a multi-step commit sequence, not a
  single `MetaCommand`).
- **`TableCreationParameters` support is real `CreateTable`-shaped, not a
  stub**: `KeySchema`/`AttributeDefinitions`/`GlobalSecondaryIndexes`/
  `BillingMode`+`ProvisionedThroughput` decode through the **identical**
  helpers `CreateTable` itself uses (`decode_key_schema`/
  `decode_attribute_types`/`decode_indexes`/
  `decode_create_table_throughput`/`check_attribute_definitions`), so
  schema validation and throughput behave identically — and once the
  target activates, the ordinary throughput-derived min-tablet-count
  auto-split trigger (ADR 0067) picks it up exactly like any other
  provisioned table, with no import-specific logic needed for that at all
  (min-tablet sizing is a background auto-split concern, never part of the
  synchronous create path, so nothing about a `Building`-then-`Active`
  target changes that). **`LocalSecondaryIndexes` is rejected outright** —
  real DynamoDB's own `TableCreationParameters` object has no such field.

### The import driver: `animusd::import`, `backup_restore.rs`'s twin — not a literal reuse

§6's own sketch imagined this landing inside the *existing* restore
driver; it shipped as a **new, sibling module** (`crates/animusd/src/
import.rs`) instead, structurally identical (same "run everywhere,
self-gate per tablet on `group.is_leader()`" discovery, same "no durable
cursor — re-sweep the whole thing every tick" resumability, same bounded
stuck-timeout) but genuinely a different job:

- **Every seeded row carries a fixed, constant version**
  (`IMPORT_SEED_VERSION = 1`), not a captured-at-real-version number the
  way a backup's own data objects do — an import's source is DynamoDB JSON
  text with no native version concept at all, and a constant is exactly
  as idempotent under `SeedBatch`'s merge-at-carried-version semantics as
  a real one (`animus-storage`'s `merge` applies only strictly-newer
  versions, so a re-seeded, already-applied row at the same version is a
  verified no-op, not merely "probably fine").
- **Item → row derivation reuses `kind_writes_for_item`** (the same
  function a live write's leader-side evaluation, and PITR segment replay,
  already call) rather than trusting captured physical bytes verbatim the
  way restore's own base-chunk sweep does — an import's source rows are
  customer-supplied DynamoDB JSON *text*, not this cluster's own
  previously-captured `KIND_BASE`/`KIND_LSI` physical values, so they have
  to be derived, not merely re-wrapped. `KIND_FOOTPRINT`/`KIND_CHANGE` are
  never produced, the identical PITR-replay/restore convention (a GSI is
  rebuilt fresh by the post-activation backfill regardless of how base
  content arrived; an imported table's own change log starts empty).
- **Key-schema validation, and a bounded malformed-item budget**: each
  decoded item's key attributes are checked against the target's declared
  `AttributeDefinitions` (presence + `S`/`N`/`B` type match); a failure
  increments `ErrorCount` and is skipped, up to
  [`MAX_MALFORMED_ITEMS`] (10,000) — crossing it fails the import
  immediately (never retried: malformed content can't be fixed by trying
  again, unlike every I/O fault in this driver, which is retried until the
  stuck-timeout).
- **Object resolution supports both of DynamoDB's own `S3KeyPrefix`
  shapes**: a prefix naming the level directly above the export's own
  `AWSDynamoDB/<id>/` folder (list `AWSDynamoDB/*/manifest-summary.json`
  under it), or a prefix naming the export folder itself
  (`manifest-summary.json` at the resolved store's own root). The second
  shape needs a path rewrite (`rebase_recorded_key`) — this adapter's own
  export job writes every recorded `manifestFilesS3Key`/`dataFileS3Key`
  relative to whatever store *it* was built over (ADR 0068 §1's own
  simplification, not a bucket-absolute key), so a caller resolving the
  manifest one level deeper than the export job's own prefix has to strip
  the now-redundant leading `AWSDynamoDB/<id>/` segment before using those
  recorded keys against its own, more-narrowly-scoped store.
- **`ClientRequestToken` idempotency is table-unscoped** (unlike export's
  `(table, token)` pair): a repeated `ImportTable` call with the same
  token always names the same target table by construction (a different
  name would already collide with the just-created target's own schema
  before the token lookup could ever matter), so `Metadata::
  import_by_client_token` matches on the token alone.
- **The customer-bucket seam is shared verbatim with export, not
  duplicated**: `ClientCtx::export_store_factory`/`ExportStoreFactory`
  (the name kept as-is rather than renamed to something export/import-
  neutral — a rename was considered and rejected as churn with no
  behavioral benefit for a field only ever spelled out inside this crate)
  is reused directly by the import driver to reach the identical
  per-request `S3SegmentStore`-over-`--export-s3-*`-credentials
  construction §2 already built.

### Named residuals (PR 2), stated plainly

1. **`ION`/`CSV` input formats and `ZSTD` compression are not
   implemented** — rejected with `ValidationException` at wire-decode
   time, the identical §4 treatment `ExportFormat::Ion`/
   `ExportType::Incremental` already get.
2. **No `CancelImport`** — real DynamoDB's `ImportTable` can be
   cancelled mid-flight; this adapter has no such command, and
   `ImportStatus` never produces `CANCELLING`/`CANCELLED`.
3. **No crash-resumability**, the identical §9 residual #1 for export: a
   node crash mid-import leaves the row `InProgress` until the driver's
   own stuck-timeout (10 minutes) fails it — there is no per-chunk durable
   cursor.
4. **`TableId` is a synthetic, freshly-minted hex string**, not a real
   DynamoDB-shaped GUID — this adapter never needed a `TableId` concept
   before `ImportTableDescription`'s own echoed field asked for one, and a
   hex string is diagnostic-only, never interpreted.
5. **PR 3's planned `SimEnv` corpus** (reusing `ANIMUS_BACKUP_SEEDS`'s
   fault-injection shape) is not built in this PR, matching export's own
   PR 1 residual — `docs/roadmap.md`'s S-05 entry stays in place pointing
   at it.

**Tests** (`crates/animusd/tests/dynamo_import.rs`, real `ProdEnv`
sockets, no MinIO — reuses the S-04 `FakeS3` fake, the identical
`ExportStoreFactory` injection seam PR 1's own test harness established):
a full export→import round trip (a real forced split on the source table,
import issued against a follower-connected node) converging to
`COMPLETED` with exact `ProcessedItemCount`/`ImportedItemCount`/
`ErrorCount` and every item reading back through `GetItem`/`Scan`; a
`NONE`-compressed export written by hand (an input shape this adapter's
own export job never produces, but real DynamoDB export tooling can);
two deliberately malformed items counted in `ErrorCount` with the rest
still imported; `ImportConflictException` for both an already-existing
target table name and a second import racing an already-claimed one;
`ImportNotFoundException` for an unknown ARN; `ListImports` pagination
and `TableArn` filtering; `ValidationException` for `ION`/`ZSTD`; and
`ClientRequestToken` idempotency.
