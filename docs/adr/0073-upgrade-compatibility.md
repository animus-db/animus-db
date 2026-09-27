# ADR 0073 — Upgrade compatibility: a phased plan, not yet a promise

- **Status:** Proposed — a skeleton/inventory for a future decision, not
  the decision itself. Nothing in this document lifts root `CLAUDE.md`'s
  "No back-compat until further notice" rule. That rule stays in force
  until Phase 4 below is actually accepted and its preceding phases have
  landed; until then, every format inventoried here may still change
  incompatibly between any two revisions, exactly as it does today.
- **Date:** 2026-09-27
- **Amends:** none. **Depends on:** [ADR 0003](0003-deterministic-simulation.md)
  (the `Env` seam any new sim corpus below is built on), [ADR 0008](0008-borrowed-storage-first.md)
  (the on-disk formats inventoried in §2), [ADR 0028](0028-shared-storage-single-command-split.md)
  (`SharedWal`), [ADR 0038](0038-control-metadata-system-keyspace.md)
  (`Metadata`'s system-keyspace mirror), [ADR 0059](0059-backup-restore.md)
  (backup/PITR objects, the format most likely to actually outlive a
  cluster today), [ADR 0060](0060-kubernetes-operator.md) ("Upgrades: None,
  by design" — this ADR is the plan for eventually revisiting that),
  [ADR 0035](0035-control-plane-separate-deployment.md) (the one existing
  precedent for real compatibility discipline — its "Rolling upgrade /
  mixed-version compatibility" section).

## Context

Root `CLAUDE.md` states the current rule plainly: *"There are no migration
paths and no wire/WAL/on-disk-format compatibility guarantees between
revisions — assume clusters are recreated from scratch."* ADR 0060 restates
it for the operator specifically ("Upgrades: None, by design ... Changing
`spec.image` or any topology field beyond plain scale-up/down is either
rejected by the operator's own validation or requires recreating the
`AnimusCluster` from scratch"). This was the right call for a pre-alpha
codebase that is still routinely making breaking changes to its own
on-disk and wire formats (ADR 0054's apply-time evaluation, ADR 0058/0062's
split redesigns, ADR 0063's key-encoding change, ADR 0069's encryption
envelope — each shipped without a migration path, exactly per the rule) —
paying for compatibility machinery before the formats stabilize would have
been waste, and would have slowed down exactly the changes that made the
system correct.

That calculus changes once there is a production user. A production
DynamoDB-compatible database that requires "delete the cluster, reload
every table from a backup" for every point release is not offering the
operational posture DynamoDB customers are used to (`website/index.html`
already lists "On-disk format stability, then rolling upgrades" as
**Planned** — this ADR is the plan that heading currently has no ADR or
roadmap entry behind). Two things stay true regardless of when this is
picked up:

1. **ADR 0035 already proves the shape of the easy 80%.** Its "Rolling
   upgrade / mixed-version compatibility" section shows that additive
   `#[serde(default)]` fields on config/wire structs, plus a matching
   additive-only discipline on request/response enums, get you safe
   mixed-version behavior for free, with zero new machinery. Most of the
   inventory below just needs that discipline applied and *verified*, not
   redesigned.
2. **The hard 20% — the Raft log/snapshot codec, the LSM WAL/SSTable/
   manifest formats, and true rolling restarts without downtime — needs a
   real version-gating mechanism**, because those are exactly the formats
   ADR 0054/0058/0062/0063/0069 changed incompatibly, on purpose, mid-life.
   A production user needs to know *when* it's safe to do that again.

This ADR is a **proposal skeleton**: an honest inventory of every format
that would need to participate, and a phased decision for how compatibility
would be built if and when the project takes it on. It intentionally does
not commit to timing (see roadmap section 3, C-16) — no phase here is sized
or scheduled ahead of it being picked up.

## Inventory of every persisted and wire format

Three lifetime classes matter, because they set how much backward-reading
range a format needs:

- **transient wire** — lives only for the duration of one RPC; both ends
  are always the *same build* today, so no compatibility currently exists
  or is assumed.
- **durable, node-local** — written and read back only by the same node's
  own process across a restart; never needs to be read by a different
  build unless that build is the upgrade target for that exact node.
- **durable, outlives the cluster** — written once, potentially read back
  much later, possibly by a different cluster or a different major version
  entirely (backups above all; also PITR change-log segments and S3
  exports). These need the widest support window of anything in this table.

| Format | Location | Version tag today | Lifetime | Notes |
|---|---|---|---|---|
| LSM WAL records | `crates/animus-storage/src/lsm/wal.rs` | **none found** — no magic/version constant in the module | durable, node-local | Record framing has no explicit version byte; crash-safety relies on length-prefixed/checksummed records and torn-tail detection, not on version negotiation. Any format change today is a silent break on reopen with an older build, or a loud one only if the length/checksum fields happen to mismatch. |
| LSM SSTable data blocks + footer | `crates/animus-storage/src/lsm/sstable.rs` | `MAGIC: u64 = 0x4355_5354_4F53_5333` in the footer (file identification only — the reader takes the actual format from the manifest, not by re-reading the footer) plus a real per-table version tag, `SsTableMeta::format: u32` (`FORMAT_CURRENT = 3`), recorded in the manifest for each table | durable, node-local | Better covered than it first looks: the format version lives one level up, in the manifest's per-table metadata, not in the footer bytes themselves. The block index has its own separate magic+version too: `INDEX_MAGIC = b"SSIX"`, `INDEX_VERSION: u8 = 1`. All three (footer magic, manifest-recorded table format, index magic+version) currently describe a single supported format — there is no decoder path for `format != FORMAT_CURRENT` yet, so the tag would need a real N-1 branch added before Phase 1 could rely on it. |
| LSM manifest | `crates/animus-storage/src/lsm.rs` (`encode_manifest`/`decode_manifest`) | `MANIFEST_MAGIC = b"CMF1"`, `MANIFEST_VERSION: u8 = 2` | durable, node-local | The best-covered format in `animus-storage`: magic + version + a documented legacy fallback (`decode_manifest` still reads a pre-binary-codec JSON manifest when the magic is absent). This is the pattern the rest of the inventory should converge on. |
| Control-plane Raft WAL (`RaftCore` log entries + membership records) | `crates/animus-control/src/persist.rs` | **none** — `serde_json`-per-line (`Line<C, S>`, comment at persist.rs:31 notes this replaced an even older plain-newline-JSON format with no version marker either) | durable, node-local | Two generations of this format have already existed with no version discriminator between them; a reader distinguishes them structurally (parse success), not by a tag. Works only because both are JSON and one is a strict superset shape. |
| Control-plane snapshot / `InstallSnapshot` payload | `crates/animus-control/src/raft.rs` (`RaftCore::snapshot`, the `S` state-machine type) | **none** — the snapshot image is just `S`'s own `serde_json`/`serde` encoding, chunked by `SNAPSHOT_CHUNK_BYTES`; no envelope version | durable, node-local (persisted) **and** transient wire (`InstallSnapshot` RPC) | Same gap as the WAL: whatever `Metadata`'s own serde shape is *is* the wire format, with no independent version byte wrapping it. |
| `Metadata` + its system-keyspace mirror (ADR 0038) | `crates/animus-control/src/meta.rs`, `syskv.rs` | **none dedicated** — individual fields get `#[serde(default)]` on addition (ADR 0035's pattern, e.g. `RoleAddrs.role`, `ClusterSettings`), but there is no single schema-version field on `Metadata` itself | durable, node-local (mirror) **and** replicated over the control Raft log | The field-by-field-default discipline is real and documented (ADR 0035 §"Rolling upgrade") but it is a convention, not a checked invariant — nothing fails loudly if a new field is added without the default, or if a field's *meaning* changes without renaming it. |
| CP-data Raft command codec (`KvCommand` etc.) | `crates/animus-cp-data/src/codec.rs` | `const VERSION: u8 = 31` | durable, node-local (in the WAL/SharedWal) and transient wire (Raft replication) | The single best-versioned format in the whole system — a version byte is written by every encode and checked by every decode, loud `Err` on mismatch (no cross-version decoding attempted). The `VERSION` bump cadence (31 already) shows how often this layer has changed; each bump is currently an unconditional breaking change, by design. |
| Segment codec (streams, ADR 0042/0043) | `crates/animus-cp-data/src/segment.rs` | `pub const VERSION: u8 = 2` | durable, node-local (per-tablet change-log segments) and consumed by backup/PITR/export as a data source | Same magic+version+loud-error discipline as the RaftKV codec, explicitly modeled on it (module doc: "mirroring `codec.rs`'s own ... discipline"). |
| `SharedWal` record framing (ADR 0028, C-05) | `crates/animus-control/src/shared_wal.rs` (record shape via `persist::encode_tagged_record`) | **none of its own** — the *outer* `Line{tablet, record}` envelope is unversioned `serde_json`, same as `persist.rs`'s single-tablet WAL; the inner `record` bytes are the already-versioned `codec.rs` payload | durable, node-local | Two-layer format: inner payload versioned, outer multi-tenant tagging envelope not. |
| Per-tablet engine key layout (ADR 0050: `kind \|\| logical`) | `crates/animus-cp-data/src/host.rs` and callers | **none** — this is a *key-space convention*, not a length-prefixed codec, so there is no version byte to check; a layout change is a data migration, not a decode-time rejection | durable, node-local (defines what every stored key means) | Explicitly called out in the task framing: changing this is equivalent to a full re-encode of every stored row, categorically different from bumping `codec.rs::VERSION`. |
| Hash-ring token / key encoding (ADR 0022/0023, ADR 0063's `N`-key layout) | `crates/animus-tablet/src/lib.rs` (`murmur3_x64_128`, `key_bytes()` in `animus-item`) | **none** — no version byte; correctness here means *every* node, every table, every historical row agrees on one encoding | durable, outlives the cluster (any row's placement and every index/backup/export/stream record derived from its key depend on it) | The highest-blast-radius unversioned format in the system: ADR 0063's own note is explicit that changing this is "no migration owed per root `CLAUDE.md`" today — i.e. it is currently *understood* to be a breaking, no-compat-path change, same category as the key layout above, but with the widest reach (base keys, GSI/LSI keys, stream `base_sk`, backup/export objects all inherit it). |
| Internal `Network` message enums (control Raft `RaftMsg`, CP-data Raft messages, `ClientRequest`/`ClientResponse` in `animus-node::wire`) | `crates/animus-control/src/raft.rs` (`RaftMsg<C>`), `crates/animus-node/src/wire.rs` (`ClientRequest`/`ClientResponse`) | **none** — `serde_json` enums with no schema version; `wire.rs`'s own doc comment says outright: *"this repo's pre-alpha stance ... both sides of a cluster are the same build ... so no version negotiation is needed"* | transient wire (internal/intra ports) | The one format in this table whose *design comment* already states the exact assumption this ADR would need to break: same-build-on-both-ends. Any mixed-version rolling upgrade needs this to become additive-only (ADR 0035's config/wire pattern), immediately, before anything else here matters — a rolling restart is, definitionally, a period where two builds talk to each other over this wire. |
| Client-port `ClientRequest`/`ClientResponse` (DynamoDB wire is separate — see next row) | same file, `Surface::Client`/`Surface::Intra` variants | same as above | transient wire | No DynamoDB-specific versioning issue here; this is the internal admin/relay protocol, not the public API. |
| DynamoDB JSON/HTTP wire (`animus-dynamo`) | `crates/animus-dynamo/src/wire.rs` | **externally versioned by AWS's own API, not by this codebase** — this repo tracks a fixed slice of DynamoDB's API surface; there is no AnimusDB-specific wire version | transient wire, public | Out of scope for internal version-gating — compatibility here means "faithful to AWS's wire," already the project's stated goal (root `CLAUDE.md`'s wire-adapter section). |
| Cluster config JSON (`animusd::config::ClusterConfig`) | `crates/animusd/src/config.rs` | **none** — no schema-version field on `ClusterConfig` itself; individual additions use `#[serde(default)]` (`dynamo_auth`, `cluster_settings`), exactly ADR 0035's pattern | durable, node-local (read at process start) | Same convention-not-invariant gap as `Metadata` above — works today because every addition so far has, by discipline, remembered the default. |
| Kubernetes operator CRD (`AnimusCluster`) | `crates/animus-operator/src/crd.rs` | `version = "v1alpha1"` (a real Kubernetes API version, with no `v1alpha1`→next conversion webhook — ADR 0060's deferred list: *"the CRD ships with no webhook of any kind; `v1alpha1` has no prior version to convert from"*) | durable, outlives a single reconcile but is itself the operator's config, not cluster data | Kubernetes' own CRD versioning mechanism exists and is unused beyond the label; a `v1alpha1`→`v1beta1` bump today would be a hard break for existing `AnimusCluster` objects. |
| Backup manifest (JSON) + chunked data objects (ADR 0059) | `crates/animus-cp-data/src/backup.rs` | Manifest: **JSON body, no explicit schema-version field noted** (the module doc calls out that the manifest reuses the magic+version *chunk* envelope for consistency but is a JSON body); data chunks: `pub const DATA_VERSION: u8 = 1`, checked with a loud, named error on mismatch | **durable, outlives the cluster** — a backup id survives its source table's deletion, by design (ADR 0059) | The module's own doc is unusually explicit that this discipline is deliberate *despite* the no-back-compat rule: "pre-alpha 'no back-compat' notwithstanding, a version bump here should [get the magic+version treatment]" — i.e. backups already understand they need a longer memory than everything else in this table. This is the natural Phase 1 starting point. |
| PITR change-log segments | reuses the segment codec above (`segment.rs`, `VERSION = 2`) as a fifth change-log consumer | versioned (inherits `segment.rs`) | durable, outlives the cluster (a PITR restore window can span a long retention period, default 35 days, but the *segments themselves* are read back by a restore that could run against a much later binary) | Inherits the same version discipline as streams; the open question is support *window* length, not mechanism. |
| S3 export/import objects (ADR 0068) | `crates/animusd/src/import.rs`, export path (JSON-lines + manifest, gzip'd, DynamoDB's own format) | **externally defined by AWS's export/import format, not internally versioned by this repo**; `IMPORT_SEED_VERSION: u64 = 1` is an MVCC seed-version constant (a `merge` precondition), not a *format* version | durable, outlives the cluster | Because the on-wire *content* format is AWS's own documented export/import layout, cross-version compatibility here is largely inherited "for free," as long as this repo keeps emitting/consuming that fixed external shape rather than an internal one. |
| Encryption envelope (ADR 0069) | `crates/animus-env/src/encrypted.rs` | `MAGIC = b"ADE1"`, `VERSION: u8 = 1` | durable, node-local (wraps every other on-disk format transparently at the `Disk` seam) | Versioned, magic-guarded, same discipline as the LSM manifest and the RaftKV/segment codecs — this is the second format in the table (after the LSM manifest) that already meets the Phase 0 bar. |

**Summary — formats with *no* version tag at all today:** the LSM WAL, the
control-plane Raft WAL and snapshot/`InstallSnapshot` payload, `Metadata`'s
own schema (as opposed to its individual fields), `SharedWal`'s outer
tagging envelope, the per-tablet engine key layout, the hash-ring token/key
encoding, every internal `Network` message enum (`RaftMsg`,
`ClientRequest`/`ClientResponse`), `ClusterConfig`, the operator CRD's
actual schema (beyond the static `v1alpha1` label), and the backup
manifest's own JSON body. That is a majority of the table. The formats that
already meet a reasonable Phase-0 bar are: the LSM manifest, the LSM
SSTable (via `SsTableMeta::format`, though with only one format ever
implemented so far), the RaftKV command codec, the segment codec, the
backup data-chunk codec, and the encryption envelope — these follow the
same magic + version byte (or, for the SSTable, a manifest-recorded version
tag) + loud named error pattern, which is the template Phase 0 below
generalizes.

## Decision (proposed), phased

No phase below is committed to a timeline; see roadmap C-16. Each phase is
gated on the previous one landing and on a maintainer decision to proceed —
this ADR fixes the *shape* of the plan so that whenever the project does
pick it up, the inventory and design work above doesn't need redoing.

- **Phase 0 — version-tag everything that lacks it, plus golden fixtures.**
  Add a magic + version byte (or, where a byte-oriented envelope doesn't
  exist yet, a leading version field in the existing `serde_json` envelope)
  to every "none" row above, using the LSM-manifest/RaftKV-codec/segment-
  codec pattern already proven in this codebase: loud, named `Err` on an
  unrecognized version, never silent misdecoding. Pair every format with a
  **golden-fixture test** — a checked-in binary/JSON fixture encoded by the
  current version, decoded by the current code, asserting byte-for-byte (or
  structurally, for JSON) round-trip — so a future incompatible change is
  caught by CI the moment it breaks a fixture, not discovered in the field.
  This phase changes no runtime behavior and needs no ADR amendment beyond
  bumping the version constants it touches; it is pure defensive plumbing.
- **Phase 1 — on-disk format stability, N-1 window.** Define a support
  window (open question below) and make each durable, node-local format's
  decoder accept both its current version and the immediately preceding
  one (or the whole declared window), translating on read where the shapes
  differ enough to require it. Backups and PITR/export objects are
  prioritized first, since they are the one class that already outlives a
  cluster today (ADR 0059's own manifest doc already anticipates this).
  The Raft command codec and the LSM formats are the highest-value, highest-
  risk targets — they change often (codec.rs is already at version 31) and
  sit under every other layer.
- **Phase 2 — mixed-version wire compatibility via a replicated cluster
  version.** Add a `Metadata`-resident cluster version / feature-gate,
  Cockroach's "cluster version" / etcd/Kubernetes' "feature gate" shape:
  new on-wire or on-disk behavior stays dormant until every member of the
  control quorum (and, for data-plane-visible gates, every hosting node)
  reports support for it, then a `Finalize` step commits the new floor.
  **No downgrade after finalize** — mirrors etcd/Cockroach precedent and
  keeps the gate a one-way ratchet, avoiding the far harder problem of a
  node needing to un-support a feature it already used. This phase is what
  makes the internal `Network` enums' current "same build on both ends"
  assumption (`wire.rs`'s own doc comment) go away: every message becomes
  additive-only under the gate, exactly ADR 0035's config/wire discipline,
  generalized cluster-wide instead of per-field.
- **Phase 3 — rolling-upgrade orchestration.** A documented per-node
  drain/restart runbook (reusing the existing drain primitive, ADR 0032),
  an admin API/CLI surface to report each node's running version and to
  trigger Phase 2's `Finalize` once every node has rolled (extending ADR
  0037's admin surface), and operator support for `spec.image` changes
  (ADR 0060 currently rejects any image change outright — this phase is
  what would let the operator drive a real rolling restart node by node
  instead of requiring cluster recreation).
- **Phase 4 — lift the root `CLAUDE.md` rule, partially.** Once Phases 0–3
  have landed and been exercised, revise root `CLAUDE.md`'s no-back-compat
  rule to state which formats are now frozen (carry a real compatibility
  contract, e.g. "N-1 on-disk, N-1 wire") versus which remain free to break
  (anything not yet reached by the phases above, or anything the project
  explicitly decides isn't worth the ongoing cost — see Open Questions).
  This phase is itself a maintainer decision, not an automatic consequence
  of the others landing.

## Testing

Every phase must stay provable under ADR 0003's determinism guarantee, the
same way every other distributed behavior in this codebase is:

- **Golden fixtures of N-1 (and, once Phase 1 lands, N-2 etc.) encodings**,
  checked into the repo per format — the LSM manifest, the RaftKV codec, the
  segment codec, the backup manifest/data chunks, the encryption envelope,
  and, once Phase 0 adds them, the newly-versioned WAL/snapshot/`Metadata`/
  key-layout/wire formats. A fixture is regenerated only when a version is
  deliberately bumped, never silently.
- **A sim corpus for mixed-version upgrade/restart/finalize**, modeled on
  the existing `SimCluster` corpora (`ANIMUS_SIMCLUSTER_SEEDS` et al.): one
  binary, with each simulated node's advertised/behavioral version
  independently selectable (an `Env`-visible knob, not a second binary —
  ADR 0003's single-binary-many-configurations precedent), driven through
  upgrade → mixed-version operation → `Finalize` with the existing fault
  vocabulary (crashes, partitions, restarts) layered on top, seed-
  reproducible via `ANIMUS_SEED`. A new depth knob, proposed name
  `ANIMUS_UPGRADE_SEEDS`, following the existing `ANIMUS_*_SEEDS` convention
  (see root `CLAUDE.md`'s table).
- **A `ProdEnv`/`kind` e2e for the operator path** (Phase 3), extending
  `scripts/e2e-kind.sh`: bring up a cluster at version N, apply a `spec.image`
  bump to N+1, and confirm the rolling restart converges to `Ready` with no
  availability gap, mirroring the existing scale-up-and-reconverge smoke.

## Alternatives considered

- **Stop-the-world upgrade via backup/restore only.** Simplest possible
  answer: never support mixed-version operation; an upgrade is "back up,
  tear down, stand up the new version, restore." Rejected as the *sole*
  answer because it still requires full downtime and a full data
  reload for every upgrade, which is exactly the operational burden a
  production user is trying to avoid — but it remains the honest fallback
  this ADR's Phase 0/1 alone would provide if Phases 2/3 are never picked
  up, and is why backups/PITR are prioritized first in Phase 1 regardless
  of what happens with the rest of the plan.
- **Logical dump/reload (a `Scan`-everything-and-`PutItem`-it-back tool).**
  Simpler to build than binary format stability, format-agnostic by
  construction, but slow at scale, doesn't preserve exact key encodings if
  ADR 0063-style layout changes happen concurrently, and doesn't solve
  wire-protocol compatibility during the dump/reload window itself. Not
  rejected outright — it may still be a useful escape hatch alongside
  Phase 1, not a substitute for it.
- **Never supporting mixed-version operation; recreate the cluster every
  time, forever.** The status quo. Rejected as a permanent stance (though
  it remains correct as the *current* stance) because it does not scale to
  a production user's expectations once the project leaves pre-alpha.

## Consequences

- **Before this is accepted, nothing changes.** No format in the inventory
  above gains a compatibility guarantee by virtue of this ADR existing;
  Status stays Proposed until a maintainer decides to fund Phase 0.
- **Phase 0 is cheap and has no downside** — it is defensive plumbing
  (version bytes, loud errors, golden fixtures) that costs nothing today
  and pays off the moment Phase 1 is picked up, or even if it never is
  (a version-tagged format that never needs to be read cross-version is
  still strictly better at catching a corrupt or truncated read).
- **Phases 1–3 are real, ongoing engineering cost** — every future
  breaking change to a covered format now needs either a translation path
  or a deliberate decision to bump the cluster version and force a
  Finalize-gated jump, exactly the kind of design-and-review budget the
  pre-alpha stance has been correctly avoiding. This is the trade this ADR
  proposes making, not one it assumes is obviously worth it yet.
- **The hash-ring/key-encoding layer is the hardest case and may never be
  a pure "version-tag it" fix** — a layout change there (as ADR 0063 was)
  changes the *meaning* of every stored byte, not just its framing, so
  supporting it is closer to a live data migration than a format
  negotiation. Phase 1's inventory should treat it as a named exception
  requiring its own design, not assume the generic N-1 mechanism covers it.

## Open questions

- **Support window length.** N-1 only, or a longer window (N-2, or a time-
  boxed "supported upgrade path" like Kubernetes' skip-version policy)?
  Longer windows cost more ongoing translation-path maintenance.
- **Which formats go first inside Phase 1?** This ADR's inventory
  prioritizes backups/PITR/export (already outlive the cluster) and the
  RaftKV codec + LSM formats (highest change frequency, sit under
  everything else) — but the actual sequencing is a sizing decision for
  whoever picks this up, not fixed here.
- **Freeze the Raft codec, or keep translating it?** `codec.rs` is already
  at `VERSION = 31` after a comparatively short project history — an
  N-1 translation requirement on a format that changes this often may
  cost more than it's worth. An alternative is to *freeze* the wire/log
  codec shape earlier and push future evolution into an envelope
  (a schema-versioned inner payload the codec itself doesn't need to
  understand), rather than requiring `codec.rs` to grow an N-1 decode path
  for every future bump.
- **The WAL back-compat fields ADR 0032/0040 already keep.** Those two
  ADRs already carry ad hoc "old field still accepted" provisions for their
  own narrow cases (node id / address-book history) — should Phase 0/1
  formalize those into the same generic version-gate mechanism, or are
  they narrow enough to stay as-is? Worth revisiting once Phase 0's
  generic mechanism exists, rather than deciding now.
- **Does the hash-ring/key-encoding layer belong in this plan at all**, or
  is it better served by staying a documented, deliberate breaking-change
  category forever (i.e. explicitly *not* Phase-4-frozen), with Phase 1
  covering everything else? See the Consequences section's note above.
