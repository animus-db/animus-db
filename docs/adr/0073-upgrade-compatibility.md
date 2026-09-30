# ADR 0073 — Upgrade compatibility: a phased plan, not yet a promise

- **Status:** Accepted (2026-09-27) — the phased plan itself, and the
  Phase 0 conventions below, are binding from this date. This is a policy
  decision, not a claim that any phase is implemented yet: see the "Phase
  status" list and the "Maintainer decision" section below for what is
  actually in force. Root `CLAUDE.md`'s no-back-compat paragraph has been
  rewritten to match (see that file); it no longer states an unconditional
  "no compat, ever" rule — it now points here.
- **Date:** 2026-09-27 (Proposed); accepted 2026-09-27 (see Maintainer
  decision)
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
- **2026-09-27 maintainer decision (verbatim):** *"you can reset format now,
  there is no production cluster yet. But from now on we operate as if
  there was one. Start phases."* This accepts the plan below and makes it
  binding immediately, ahead of any phase actually landing — see
  "Maintainer decision" and "Phase status" below for exactly what changes
  today versus what each phase still has to build.

## Maintainer decision (2026-09-27)

The maintainer has decided the timing question the original "Proposed"
status above left open (see the 2026-09-27 note): **there is no production
cluster yet, so one last incompatible reset is free — take it now, as part
of Phase 0 — and from the moment that reset lands, operate as if a
production cluster existed.** That is a deliberate ratchet, not a claim that
Phases 1–3's machinery already exists. What follows is the precise, binding
interpretation.

1. **One final format reset, as part of Phase 0.** Phase 0 (below) is the
   **last permitted incompatible change** to any format in this ADR's
   inventory. During Phase 0, and only during Phase 0, a format may be
   freely redesigned: every format gains a version tag per the Phase 0
   conventions below, every version counter **restarts at 1** (e.g. the
   RaftKV codec's `VERSION = 31` becomes the Phase 0 baseline's `VERSION =
   1`; the segment codec's `VERSION = 2` likewise resets to `1`), and
   legacy back-compat baggage that exists only to read pre-baseline data
   (e.g. the `cp_member_addrs` field and the other ad hoc "old field still
   accepted" provisions ADR 0032/0040 carry, the LSM manifest's own
   pre-binary-codec JSON fallback) may be dropped outright rather than
   folded into the new generic version-gate mechanism. Each Phase 0
   workstream (see the table below) identifies its own instances of this
   as it does the work — this ADR does not enumerate every one in advance.
2. **The baseline.** The baseline is **the merge commit on `main` where the
   last Phase 0 workstream (A–E below) lands**. This ADR records the actual
   commit SHA and date here once that happens:
   - **Baseline commit:** `9a9f972fb38a766be4a4b044379540ded533eefc`
     (`9a9f972f`), "Merge pull request #1104" (Workstream E's last PR),
     merged 2026-09-29 22:50 UTC. Set 2026-09-30; see the "Phase 0
     complete" amendment at the end of this ADR for the PR list and
     verification.
   Before the baseline, every format may still change incompatibly *as
   part of Phase 0 work only* — Phase 0 is not itself covered by the rule
   it exists to establish. From the baseline onward, the "as if production"
   rule in point 3 applies in full, to every format Phase 0 touched.
3. **The "as if production" rule, staged as an honest ratchet.** The
   mechanism to enforce full compatibility does not exist yet as one piece
   — it is built incrementally by Phases 1–3. The rule below is staged to
   match exactly what each phase actually makes enforceable, so nobody
   reads "operate as if there was one" as a promise the current tooling
   can't back up:
   - **From the baseline (Phase 0 done) — durable formats are compatible.**
     Every **durable** format (anything written to disk or to the backup
     `SegmentStore`: WALs, SSTables/manifest, snapshots, `Metadata`'s
     system-keyspace mirror, the per-tablet key layout, the hash-ring/key
     encoding, backup/PITR/export/stream objects, the encryption envelope,
     cluster config on disk) must be readable by every later binary: a
     full-cluster stop → upgrade → restart must work with no data loss and
     no manual conversion step. Mechanically: **a format change is a new
     version tag, a decoder that still accepts every older post-baseline
     version, and a new golden fixture for the new version — an existing
     checked-in fixture is never edited or deleted** (the CI guard below
     enforces the "never deleted" half mechanically; the "decoder still
     accepts it" half is reviewed by hand until Phase 1's N-1 requirement
     is actually implemented per format — see Phase status). **Support
     window: every post-baseline version stays readable forever**
     (maintainer decision, 2026-09-30 — see the "Phase 1 design"
     amendment at the end of this ADR; this replaces the earlier
     "until the first tagged release" default).
   - **Once Phase 2 lands (a replicated cluster version / feature gate in
     `Metadata`) — wire formats join the same rule.** Internal `Network`
     message enums (control-plane Raft, CP-data Raft, the client/intra
     wire) become mixed-version compatible: new messages or behavior stay
     dormant until the whole quorum/hosting set reports support, gated the
     same way Cockroach/etcd gate a cluster version. **Before Phase 2
     lands, wire changes stay free** (no compat obligation) **but should be
     additive-by-design wherever it is cheap** — new optional fields/
     variants rather than renamed/removed ones — since that costs nothing
     today and is exactly the discipline Phase 2 will require anyway.
   - **Once Phase 3 lands — rolling upgrades are supported and tested,
     full stop.** A documented drain/restart runbook, an admin surface to
     report per-node version and trigger Finalize, and operator support for
     `spec.image` changes (today rejected outright).
   - **The escape hatch stays available at every stage:** anything that
     genuinely cannot be made compatible needs an explicit ADR amendment
     naming the break and the migration path (backup/restore, at minimum)
     — the same bar a production database would hold itself to for a
     breaking change, not a ban on ever breaking anything again.
4. **Phase status** (updated as phases land; this is the live status this
   ADR's own header should be read against):
   - **Phase 0 — version-tag everything + golden fixtures + the final
     reset:** **done** (2026-09-30). Tracked as five
     independent workstreams (A–E) — see the table below. All five PR
     series merged; the baseline commit above is `9a9f972f`.
   - **Phase 1 — on-disk N-1 stability:** **in progress** (design
     accepted 2026-09-30, see the "Phase 1 design" amendment: window =
     every post-baseline version forever; per-crate implementation
     workstreams P1-A..P1-D).
   - **Phase 2 — replicated cluster version / wire feature-gate:** planned,
     blocked on Phase 1.
   - **Phase 3 — rolling-upgrade orchestration:** planned, blocked on
     Phase 2.
   - **Phase 4 — lift/rewrite the root `CLAUDE.md` no-back-compat rule:**
     **effectively decided now, ahead of Phases 1–3, by this same
     maintainer decision** — root `CLAUDE.md` is rewritten in this PR to
     state the staged ratchet in point 3 above rather than an unconditional
     "no compat, ever" rule. What Phase 4 in the original plan treated as a
     single future flip is, as of this decision, already in force *as a
     ratchet whose strength grows with each later phase* — there is no
     separate future "Phase 4 PR" left to do; this is it. The original
     Phase 4 bullet below is kept for historical record of what was
     proposed before this decision, not as a still-open step.

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

**As-built refresh (2026-09-30, Phase 0 complete):** the *Location* and *Version tag today* cells below are corrected to match `main` at the baseline. The *Notes* cells, the Summary paragraph that follows, and the workstream rows keep their original pre-Phase-0 wording as a historical record of the starting inventory; where they say a format has "no version tag", read the corrected cell and the as-built notes instead.

| Format | Location | Version tag today | Lifetime | Notes |
|---|---|---|---|---|
| LSM WAL records | `crates/animus-storage/src/lsm/wal.rs` | `WAL_MAGIC = b"LWL1"`, `WAL_VERSION: u8 = 1` | durable, node-local | Record framing has no explicit version byte; crash-safety relies on length-prefixed/checksummed records and torn-tail detection, not on version negotiation. Any format change today is a silent break on reopen with an older build, or a loud one only if the length/checksum fields happen to mismatch. |
| LSM SSTable data blocks + footer | `crates/animus-storage/src/lsm/sstable.rs` | `MAGIC: u64 = 0x4355_5354_4F53_5333` in the footer (file identification only — the reader takes the actual format from the manifest, not by re-reading the footer) plus a real per-table version tag, `SsTableMeta::format: u32` (`FORMAT_CURRENT = 1`), recorded in the manifest for each table | durable, node-local | Better covered than it first looks: the format version lives one level up, in the manifest's per-table metadata, not in the footer bytes themselves. The block index has its own separate magic+version too: `INDEX_MAGIC = b"SSIX"`, `INDEX_VERSION: u8 = 1`. All three (footer magic, manifest-recorded table format, index magic+version) currently describe a single supported format — there is no decoder path for `format != FORMAT_CURRENT` yet, so the tag would need a real N-1 branch added before Phase 1 could rely on it. |
| LSM manifest | `crates/animus-storage/src/lsm.rs` (`encode_manifest`/`decode_manifest`) | `MANIFEST_MAGIC = b"CMF1"`, `MANIFEST_VERSION: u8 = 1` | durable, node-local | The best-covered format in `animus-storage`: magic + version + a documented legacy fallback (`decode_manifest` still reads a pre-binary-codec JSON manifest when the magic is absent). This is the pattern the rest of the inventory should converge on. |
| Control-plane Raft WAL (`RaftCore` log entries + membership records) | `crates/animus-control/src/persist.rs` | `CONTROL_WAL`: `CWL1` + version 1 (`format::encode_line`) | durable, node-local | Two generations of this format have already existed with no version discriminator between them; a reader distinguishes them structurally (parse success), not by a tag. Works only because both are JSON and one is a strict superset shape. |
| Control-plane snapshot / `InstallSnapshot` payload | `crates/animus-control/src/raft.rs` (`RaftCore::snapshot`, the `S` state-machine type) | `CONTROL_SNAPSHOT`: `CSN1` + version 1 (`persist.rs`), wrapping `S`'s own serde shape | durable, node-local (persisted) **and** transient wire (`InstallSnapshot` RPC) | Same gap as the WAL: whatever `Metadata`'s own serde shape is *is* the wire format, with no independent version byte wrapping it. |
| `Metadata` + its system-keyspace mirror (ADR 0038) | `crates/animus-control/src/meta.rs`, `syskv.rs` | `METADATA_VERSION: u32 = 1` (`meta.rs`, serialized as `"v"`); the system-keyspace mirror carries `SYSKV_MIRROR_VERSION: u32 = 1` (`mirror.rs`). Individual fields still use ADR 0035's `#[serde(default)]` pattern | durable, node-local (mirror) **and** replicated over the control Raft log | The field-by-field-default discipline is real and documented (ADR 0035 §"Rolling upgrade") but it is a convention, not a checked invariant — nothing fails loudly if a new field is added without the default, or if a field's *meaning* changes without renaming it. |
| CP-data Raft command codec (`KvCommand` etc.) | `crates/animus-cp-data/src/codec.rs` | `MAGIC = 0xCB`, `const VERSION: u8 = 1` | durable, node-local (in the WAL/SharedWal) and transient wire (Raft replication) | The single best-versioned format in the whole system — a version byte is written by every encode and checked by every decode, loud `Err` on mismatch (no cross-version decoding attempted). The `VERSION` bump cadence (31 already) shows how often this layer has changed; each bump is currently an unconditional breaking change, by design. |
| Segment codec (streams, ADR 0042/0043) | `crates/animus-cp-data/src/segment.rs` | `MAGIC = b"SEGF"`, `pub const VERSION: u8 = 1` | durable, node-local (per-tablet change-log segments) and consumed by backup/PITR/export as a data source | Same magic+version+loud-error discipline as the RaftKV codec, explicitly modeled on it (module doc: "mirroring `codec.rs`'s own ... discipline"). |
| `SharedWal` record framing (ADR 0028, C-05) | `crates/animus-control/src/persist.rs` (`SHARED_WAL_TAG`, `encode_tagged_record`) | `SHARED_WAL_TAG`: `SWL1` + version 1 on the outer `Line{tablet, record}` envelope; the inner `record` bytes are the already-versioned `codec.rs` payload | durable, node-local | Two-layer format: inner payload versioned, outer multi-tenant tagging envelope not. |
| Per-tablet engine key layout (ADR 0050: `kind \|\| logical`) | `crates/animus-cp-data/src/layout.rs` (and `host.rs` callers) | a reserved-namespace marker key per engine: `LAYOUT_MAGIC = b"KLY1"`, `LAYOUT_EPOCH: u8 = 1` (the layout is still a key-space convention; the marker stamps its epoch) | durable, node-local (defines what every stored key means) | Explicitly called out in the task framing: changing this is equivalent to a full re-encode of every stored row, categorically different from bumping `codec.rs::VERSION`. |
| Hash-ring token / key encoding (ADR 0022/0023, ADR 0063's `N`-key layout) | `crates/animus-tablet/src/lib.rs` (`murmur3_x64_128`, `key_bytes()` in `animus-item`) | **none** — no version byte; correctness here means *every* node, every table, every historical row agrees on one encoding | durable, outlives the cluster (any row's placement and every index/backup/export/stream record derived from its key depend on it) | The highest-blast-radius unversioned format in the system: ADR 0063's own note is explicit that changing this is "no migration owed per root `CLAUDE.md`" today — i.e. it is currently *understood* to be a breaking, no-compat-path change, same category as the key layout above, but with the widest reach (base keys, GSI/LSI keys, stream `base_sk`, backup/export objects all inherit it). |
| Internal `Network` message enums (control Raft `RaftMsg`, CP-data Raft messages, `ClientRequest`/`ClientResponse` in `animus-node::wire`) | `crates/animus-control/src/raft.rs` (`RaftMsg<C>`), `crates/animus-node/src/wire.rs` (`ClientRequest`/`ClientResponse`) | the connection handshake carries `NETWORK_PROTOCOL` (`NHS1`) / `CLIENT_PROTOCOL` (`CHS1`), each version 1 (`animus-env/src/handshake.rs`); the message enums themselves are still unversioned `serde_json`, and `wire.rs`'s (pre-Phase-0) doc comment said outright: *"this repo's pre-alpha stance ... both sides of a cluster are the same build ... so no version negotiation is needed"* | transient wire (internal/intra ports) | The one format in this table whose *design comment* already states the exact assumption this ADR would need to break: same-build-on-both-ends. Any mixed-version rolling upgrade needs this to become additive-only (ADR 0035's config/wire pattern), immediately, before anything else here matters — a rolling restart is, definitionally, a period where two builds talk to each other over this wire. |
| Client-port `ClientRequest`/`ClientResponse` (DynamoDB wire is separate — see next row) | same file, `Surface::Client`/`Surface::Intra` variants | same as above | transient wire | No DynamoDB-specific versioning issue here; this is the internal admin/relay protocol, not the public API. |
| DynamoDB JSON/HTTP wire (`animus-dynamo`) | `crates/animus-dynamo/src/wire.rs` | **externally versioned by AWS's own API, not by this codebase** — this repo tracks a fixed slice of DynamoDB's API surface; there is no AnimusDB-specific wire version | transient wire, public | Out of scope for internal version-gating — compatibility here means "faithful to AWS's wire," already the project's stated goal (root `CLAUDE.md`'s wire-adapter section). |
| Cluster config JSON (`animusd::config::ClusterConfig`) | `crates/animusd/src/config.rs` | `CLUSTER_CONFIG_VERSION: u32 = 1` (`config.rs`, serialized as `"v"`); individual additions still use `#[serde(default)]`, ADR 0035's pattern | durable, node-local (read at process start) | Same convention-not-invariant gap as `Metadata` above — works today because every addition so far has, by discipline, remembered the default. |
| Kubernetes operator CRD (`AnimusCluster`) | `crates/animus-operator/src/crd.rs` | `version = "v1alpha1"` plus a content-schema `CONTENT_SCHEMA_VERSION: u32 = 1` (`spec.schemaVersion`, distinct from the Kubernetes API version) (a real Kubernetes API version, with no `v1alpha1`→next conversion webhook — ADR 0060's deferred list: *"the CRD ships with no webhook of any kind; `v1alpha1` has no prior version to convert from"*) | durable, outlives a single reconcile but is itself the operator's config, not cluster data | Kubernetes' own CRD versioning mechanism exists and is unused beyond the label; a `v1alpha1`→`v1beta1` bump today would be a hard break for existing `AnimusCluster` objects. |
| Backup manifest (JSON) + chunked data objects (ADR 0059) | `crates/animus-cp-data/src/backup.rs` | Manifest: `BKMF` + `u8` version (`MANIFEST_VERSION = 1`) envelope around a JSON body — the envelope is the version tag, so the body needs no `"v"` field; data chunks: `pub const DATA_VERSION: u8 = 1`, checked with a loud, named error on mismatch | **durable, outlives the cluster** — a backup id survives its source table's deletion, by design (ADR 0059) | The module's own doc is unusually explicit that this discipline is deliberate *despite* the no-back-compat rule: "pre-alpha 'no back-compat' notwithstanding, a version bump here should [get the magic+version treatment]" — i.e. backups already understand they need a longer memory than everything else in this table. This is the natural Phase 1 starting point. |
| PITR change-log segments | reuses the segment codec above (`segment.rs`, `SEGF` + `segment::VERSION`) as a fifth change-log consumer | versioned (inherits `segment.rs`) | durable, outlives the cluster (a PITR restore window can span a long retention period, default 35 days, but the *segments themselves* are read back by a restore that could run against a much later binary) | Inherits the same version discipline as streams (Workstream E confirmed the inheritance with a test that reads the sealed object's raw bytes); the open question is support *window* length, not mechanism. |
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
  of the others landing. **Superseded by the 2026-09-27 maintainer decision
  above**: the decision to "operate as if there was one" from the baseline
  on is itself the Phase 4 call, taken now rather than after Phases 1–3 —
  see point 4 there.

## Phase 0 conventions (binding, 2026-09-27)

These conventions are concrete enough that each Phase 0 workstream (table
below) can implement independently and land a consistent result. A
workstream that needs to deviate does so via a review comment on its own
PR, not silently — if a real format genuinely can't fit this shape, that is
itself worth recording as an amendment here.

### Version tag shape

- **Binary/byte-oriented formats** (anything with its own framing, not a
  `serde_json` document): a **4-byte ASCII magic, format-specific**,
  immediately followed by a **`u8` version**, exactly the shape already
  proven by the LSM manifest (`CMF1` + `u8`) and the encryption envelope
  (`ADE1` + `u8`). `u8` (not `u16`) is chosen for consistency with every
  existing precedent in this codebase (manifest, envelope, RaftKV codec,
  segment codec all use `u8`) and because 255 versions is nowhere close to
  a binding constraint — the RaftKV codec is the fastest-moving format in
  the system and only reached 31 over the project's entire history to
  date; restarting it at 1 under Phase 0 buys another multi-year runway.
  Suggested magics for the formats that don't have one yet (pick a 4-byte
  ASCII value that doesn't collide with an existing one in the inventory —
  `CMF1`/`ADE1`/`SSIX` are taken):
  | Format | Suggested magic |
  |---|---|
  | LSM WAL record header | `LWL1` |
  | Control-plane Raft WAL line envelope | `CWL1` |
  | Control-plane snapshot / InstallSnapshot envelope | `CSN1` |
  | `SharedWal` outer `Line{tablet, record}` envelope | `SWL1` |
  | Per-tablet engine key layout marker (a leading key-space byte/prefix, not a framed record — see workstream C's own design task) | n/a — versioned differently, see below |
  Each workstream may substitute a better magic during implementation; the
  only hard constraint is *no collision* with another format's magic and
  *no reuse* of a magic across versions (a version bump keeps the same
  magic, `ADE1`/`CMF1` already establish that).
- **`serde_json` formats** (`Metadata`, `ClusterConfig`, the backup
  manifest, the operator CRD's actual schema): a **top-level `"v": <u32>`
  field**, not a wrapping envelope object — added as a plain field on the
  existing top-level struct (`#[serde(default)] version: Option<u32>`
  becomes, at the Phase 0 reset, a required `version: u32` with no
  default, since Phase 0 is the one point where dropping the default is
  safe). Chosen over a `{ "v": N, "data": ... }` wrapper because every
  format in this group is already a single top-level struct with
  `#[serde(default)]`-annotated fields (ADR 0035's pattern) — adding one
  more required field costs nothing and needs no restructuring, while a
  wrapper would touch every call site that constructs or matches the
  value.
- **Untagged pre-baseline input**: a `serde_json` document with no `"v"`
  field, or a byte-oriented file with no recognized magic, read by
  post-baseline code is a **named, loud `Err`** (e.g.
  `Error::PreBaselineFormat { format: &'static str }` — one variant shared
  across formats, not one per format), never a silent best-effort parse
  and never a panic. This is intentional: Phase 0 is the reset, so nothing
  written before it is owed compatibility, but silently misreading it (as
  opposed to refusing it by name) is exactly the failure mode this whole
  ADR exists to close off.
- **Unknown/future version**: same treatment — a named, loud `Err` (e.g.
  `Error::UnsupportedFormatVersion { format, found, max_supported }`),
  never silent misdecoding, never a panic. This is already the discipline
  the RaftKV codec and segment codec follow; Phase 0 generalizes it to
  every format in the inventory.

### Golden fixtures

- **Location:** `crates/<crate>/tests/fixtures/formats/<format>/v<N>.<ext>`
  — one directory per format (`<format>` a short stable slug, e.g.
  `raftkv-codec`, `lsm-manifest`, `metadata`, `cluster-config`,
  `backup-manifest`), one file per version (`<ext>` = `bin` for
  byte-oriented formats, `json` for `serde_json` ones). Example:
  `crates/animus-cp-data/tests/fixtures/formats/raftkv-codec/v1.bin`.
- **Decode test:** each format gets one test (co-located with its codec,
  e.g. `codec.rs`'s own `#[cfg(test)] mod` or a `tests/format_fixtures.rs`
  in the owning crate) that iterates every fixture file under its
  `tests/fixtures/formats/<format>/` directory, decodes each with the
  *current* code, and asserts the decoded value structurally (field-by-
  field `assert_eq!` against a hand-written expected value, not just
  "decodes without error" — a decoder that silently drops a field would
  otherwise pass). Today, right after the Phase 0 reset, this is a single
  `v1.<ext>` fixture per format; the test is written to iterate the
  directory (`v1`, `v2`, ...) rather than name `v1` literally, so Phase 1
  additions need no test-code change, only a new fixture file.
- **Round-trip test:** a separate test that encodes a representative value
  with the *current* version, decodes it back, and asserts equality —
  catches an encoder/decoder asymmetry that a static fixture alone (which
  only exercises decode) would miss.
- **Fixture generation:** a fixture is never hand-written byte-for-byte.
  Each format's test module gets a `#[ignore]`d generator test (run
  explicitly, e.g. `cargo test -p <crate> --lib generate_fixture_<format>
  -- --ignored`) that encodes a representative value with the *current*
  version and writes it to
  `tests/fixtures/formats/<format>/v<CURRENT>.<ext>` — but **refuses to
  overwrite a file that already exists** (checks with `std::fs::metadata`
  first and panics with a clear message telling the author to bump the
  version instead). This, plus the CI guard below, is what makes "a
  fixture is regenerated only when a version is deliberately bumped, never
  silently" (this ADR's Testing section) an enforced property instead of a
  convention.
- **Determinism:** fixture *content* must not embed wall-clock time or
  unseeded randomness (ADR 0003) — a fixture that encodes, say, a
  `Metadata` value with a timestamp field uses a fixed, hand-chosen
  constant (e.g. epoch 0, or a readable fixed date past 2000-01-01),
  never `SystemTime::now()`, so the checked-in bytes are stable across
  every future run and every future contributor's clock.

### CI guard

`scripts/check-format-fixtures.sh` fails the build if any file already
present under `**/tests/fixtures/formats/**` at the merge base with
`origin/main` has been modified or deleted on the current branch (a new
file — a genuinely new version — is fine; that is the *only* legitimate way
forward once a version has fixtures). It is a no-op pass when no such
directory exists yet anywhere in the tree (true today, until the first
Phase 0 workstream adds one). Wired into `.github/workflows/ci.yml`'s
existing `gates` job as one more named step, after `rustfmt` and before
`clippy` (cheapest gate first, matching that job's own step ordering
rationale) — no restructuring of the job.

### Phase 0 workstreams

Five independent sessions, one PR series each, per the root `CLAUDE.md`
"Session operating mode" rule that independent work runs in a separate
session. Ordered by dependency (a later workstream reads an earlier one's
landed convention, never its in-flight code); within a "wave" they can run
concurrently since they touch disjoint crates. This PR (docs/policy +
`scripts/check-format-fixtures.sh`) is the prerequisite all of them build
on — none should start implementing before it merges, since it fixes the
tag shape and fixture layout every workstream conforms to.

| Workstream | Crate(s) | Formats reset | Depends on | Touches (do not touch, for the others) |
|---|---|---|---|---|
| **A** | `animus-storage` | LSM WAL record header (add magic+version, `LWL1`+v1); SSTable/manifest re-baseline (`MANIFEST_VERSION` → 1, keep the existing `CMF1` magic since it's already correct — only the version counter and the "read pre-binary-codec JSON" legacy fallback are in scope for removal, since that fallback is exactly the pre-baseline legacy path point 1 above says may be dropped); `SsTableMeta::format` reset to 1 with a real decode path so a *second* format value becomes meaningful before Phase 1 needs it; encryption envelope fixture (`ADE1` already has a version — no reset needed, just add its golden fixture) | none (self-contained crate) | `crates/animus-storage/**` only. Do not touch `animus-cp-data`'s or `animus-control`'s own WAL code even though they call into `animus-storage`'s `StorageEngine` trait — this workstream owns the trait's implementations, not its callers. |
| **B** | `animus-control` | Raft WAL `Line<C, S>` envelope (add `CWL1`+v1, replacing the two-generation structural-sniff scheme in `persist.rs`); control-plane snapshot/`InstallSnapshot` payload envelope (`CSN1`+v1, wrapping `S`'s own serde shape); `Metadata`'s schema gets a top-level `"v": 1` field (`syskv.rs`'s mirror inherits it for free since it serializes the same struct); drop the `cp_member_addrs`/address-book legacy fields ADR 0032/0040 kept for pre-existing-cluster back-compat (Phase 0 is the point where "no production cluster yet" makes that safe) | none (self-contained crate, though B and C should coordinate on the shared `Line`-style envelope shape so they don't diverge — see C's own note) | `crates/animus-control/**` only. Do not touch `crates/animus-cp-data/src/shared_wal.rs`'s own `Line{tablet, record}` envelope — it looks similar but is workstream C's, not B's, even though both reuse `persist::encode_tagged_record`. |
| **C** | `animus-cp-data` | RaftKV command codec re-baseline (`codec.rs::VERSION` 31 → 1); `SharedWal` outer `Line{tablet, record}` envelope (`SWL1`+v1 — the *inner* `record` bytes are already the versioned codec payload, unchanged shape, only the outer tag is new); segment codec re-baseline (`segment.rs::VERSION` 2 → 1); a version marker on the per-tablet engine key layout (ADR 0050's `kind \|\| logical` convention; **as built: a reserved-namespace marker key, not a kind byte — see the layer 4 as-built paragraph**) — this one is a design task, not a mechanical tag-and-fixture: recommend a single reserved leading byte in the `kind` namespace itself (a `layout` epoch) rather than a per-record magic, since the layout is a key-space convention, not a framed record; document the chosen approach directly in `crates/animus-cp-data/CLAUDE.md`, this ADR does not prescribe the exact bit layout | B (for the shared `encode_tagged_record`/`Line`-shape convention, so C's `SharedWal` envelope and B's Raft WAL envelope don't independently invent incompatible tagging shapes for what is structurally the same "envelope wraps an inner payload" problem) | `crates/animus-cp-data/**` only. Do not touch `crates/animus-control/src/persist.rs` — read it for the shared convention, don't edit it. |
| **D** | wire (`animus-node`, `animus-control`'s `RaftMsg`, `animus-cp-data`'s Raft messages) | A version field in the connection handshake/envelope for internal `Network` message enums (`RaftMsg<C>`, `ClientRequest`/`ClientResponse`) — **prep for Phase 2, not a Phase 1 compatibility mechanism yet**: today this only needs to *exist* and be checked for equality (both ends same build, as today), not gate anything, since real mixed-version wire compatibility is Phase 2's job. Keep changes additive (new field, `#[serde(default)]`-shaped where the enum crosses a version boundary) so Phase 2 can build on it without another reset. | none | `crates/animus-node/src/wire.rs`, the `RaftMsg`/Raft-message enum definitions in `animus-control`/`animus-cp-data`. Do not touch those crates' *storage* formats (A/B/C's territory) even though they live in the same files in some cases — a wire enum and a WAL record type in the same crate are still disjoint concerns; grep for the specific type names above before editing anything else nearby. |
| **E** | `animusd`, `animus-operator` | `ClusterConfig` gets `"v": 1`; operator CRD real schema gets an explicit internal version marker (distinct from the Kubernetes `v1alpha1` API-version label, which stays as-is — this is about the CRD's *content* schema, not its API group version); backup manifest JSON body gets `"v": 1` (the manifest already reuses the chunk envelope's magic+version for its *data chunks*, `DATA_VERSION`; this is about the manifest's own top-level JSON body, a separate thing per the inventory table); PITR/export objects inherit the segment codec's reset from C, so E's own scope here is just confirming that inheritance holds, not re-versioning them independently | C (backup/PITR/export read the segment codec — E must land after C's `segment.rs` reset, or build against C's PR series directly if timing requires overlap) | `crates/animusd/**`, `crates/animus-operator/**`. Do not touch `crates/animus-cp-data/src/segment.rs` — read C's landed version, don't reset it a second time. |

Wave 1 (no cross-workstream dependency): A, B, D. Wave 2: C (after B lands,
for the shared envelope convention). Wave 3: E (after C lands, for the
segment-codec reset it inherits). A, B, D can run fully concurrently in
separate sessions from the start; C should start once B's convention is
settled (a quick read of B's landed PR, not a long wait); E starts once C's
`segment.rs` reset is on `main`. Each workstream session gets: this ADR
(read in full), its own row above, root `CLAUDE.md`'s Phase 0 conventions
section, the relevant crate `CLAUDE.md`(s), and the explicit do-not-touch
list from its own row plus the others' "touches" columns.

### Workstream D as-built (2026-09-28)

Landed across four layers (`animus-env` codec, `ProdEnv` wiring, `animusd`'s
client/intra port, `SimEnv` modeling) as a stacked PR series. Recorded here
as one addition, not a rewrite of the row above.

- **Preamble layout**: `magic[4] | version:u8 | ext_len:u16 (LE) |
  ext[ext_len]` (`animus_env::handshake`). `ext` is reserved for Phase 2
  (feature bits / a supported-version range) — a v1 `encode` always writes
  it empty, and a v1 `decode`/`check_peer` carries a peer's `ext` bytes
  through without ever rejecting a non-empty one, so Phase 2 can start
  using the field for real with no further format reset.
- **Two magics, two independently-versioned counters**: `NHS1` for the
  internal `Network` transport (control-plane Raft, every per-tablet
  CP-data Raft group, anything multiplexed over `(node, stream)` — ADR
  0026) and `CHS1` for the client/intra length-prefixed JSON-RPC wire
  (`animus-node::wire`'s `ClientRequest`/`ClientResponse`, used both
  client-to-node and node-to-node for forwarding). A version bump on one
  never requires a bump on the other — they are unrelated wire formats
  that happen to share one handshake mechanism. Both checked for no
  collision against every magic already in use elsewhere in this codebase.
- **Per-connection handshake, not a per-message field — why**: internal
  connections are pooled (one outbound TCP connection per destination) and
  ADR 0026 multiplexes many `(node, stream)` protocol instances over each
  one, so a check paid once per connection costs nothing against the
  connection's whole lifetime, where a per-message field would be paid on
  every single Raft heartbeat for as long as the connection lives.
- **Symmetric write-first exchange, magic-first check**: both sides write
  their own preamble immediately on connect/accept, then each reads and
  checks the peer's — no separate client/server preamble shape, no extra
  round trip to negotiate who goes first. `check_peer` checks magic before
  version, so a pre-baseline peer (a raw, unversioned frame predating this
  handshake) is refused with a named `BadMagic`, distinct from a same-magic
  version mismatch (`UnsupportedVersion`).
- **Refusal semantics**: `warn`-level log naming the peer/role/cause, a
  refusal-only metric (`Metric::NetworkHandshakeRefused` /
  `Metric::ClientHandshakeRefused` — a plain I/O failure like EOF or a
  reset is not counted here, only a genuine protocol refusal is), then the
  connection is closed. Never cached (every new connection re-runs the full
  exchange) and never a panic (a refusal is an ordinary, logged, counted
  connection-setup failure, not a defect).
- **No extra round trip on the unpooled relay/join path**: the same
  write-then-check sequence, pipelined rather than a request/response
  round trip, so a one-shot dial (`animus-node`'s relay/join callers) pays
  exactly one network round trip total, not one for the handshake plus one
  for the first real frame.
- **`SimEnv` model**: `SimEnv` has no real connections at all (ADR 0003 —
  no sockets, just `send`/`recv` over an in-memory timeline), so the
  identical check is modeled at **message**, not connection, granularity —
  each refused message stands in for one refused connection. A per-node
  override (`Simulator::set_network_protocol_for`, defaulting to this
  build's own `NETWORK_PROTOCOL`) is checked in both directions on every
  `Event::Deliver`, mirroring `ProdEnv::perform_handshake` running on both
  the accept and dial side of a real connection; a refusal is never
  delivered, is traced as a `Drop` with the named reason
  `"protocol-refused"`, and is counted (`Simulator::protocol_refusals`).
  Pure comparison of already-known values — no RNG draw, no timeline
  event, no ordering change — so every pre-existing seed's execution and
  trace stays byte-identical when every node is at its default.
  `animus-node`'s `SimRelayClient` (the `CLIENT_PROTOCOL` wire, in sim)
  rides this same `Network`, so this one check covers it too at the
  network-transport level; `SimEnv` does not separately model a `CHS1`
  exchange on top. See `crates/animus-control/tests/
  protocol_version_refusal.rs` for the end-to-end proof (a 3-node control
  cluster where one node's protocol version is mismatched: the v1 majority
  still elects and commits, the odd node never receives or applies
  anything and never leads, the refusal is observable, and resetting the
  odd node's protocol lets it converge).
- **Fixtures**: `crates/animus-env/tests/fixtures/formats/{network,
  client}-handshake/v1.bin`, golden-fixture-tested per ADR 0073 Phase 0's
  own convention (`scripts/check-format-fixtures.sh`).
- **What Phase 2 builds on this**: the extension area starts carrying the
  supported version range and feature bits for real; `check_peer` relaxes
  from exact equality to a supported-range intersection; a replicated
  cluster version / feature gate (`Metadata`-resident) decides what a node
  may *use* once every member supports it — none of this needs another
  format reset, since the preamble already carries an `ext` area no v1
  peer rejects for being non-empty.
- **Caveat (dial-side counting on the client/intra wire)**: every
  `animusd` *dialer* of the client/intra wire — `connect_client`, and the
  pipelined, unpooled relay/join path (`client_request_pipelined`, used by
  `relay_request_with_timeout`/`join_request`) — passes a no-op
  `MetricsHandle`, simply because none of those call sites has the node's
  metrics handle plumbed through today. A refusal there is still logged at
  `warn` with the named error and surfaces as a transport failure to the
  caller (`RELAY_TRANSPORT_FAILURE` on the relay path), never a panic or a
  silent drop; and because the handshake is symmetric, the *accepting*
  node always counts it in `Metric::ClientHandshakeRefused`, so every
  mismatch is visible in at least one side's metrics. The internal
  `Network` transport (`ProdEnv`) counts on both sides. Threading a real
  handle into the client/intra dial paths is a small follow-up, not a
  Phase 2 prerequisite.

**Workstream B as-built (2026-09-28), PR 1 of its own stacked series —
the shared convention + `CWL1`.** Landed `crates/animus-control/src/
format.rs` (`pub mod format`, re-exported from `lib.rs`): `FormatTag`
(magic/version/name, const-constructible), one shared `FormatError`
(`PreBaselineFormat`/`UnsupportedFormatVersion`/`Malformed`, manual
`Display`/`std::error::Error` — this crate carries no `thiserror`
dependency, so the ADR's "thiserror if already a dependency, otherwise
manual" instruction took the manual branch), the binary `wrap`/`unwrap`
envelope, and the line `encode_line`/`decode_lines` pair. `persist::
CONTROL_WAL` (`CWL1`, v1) is now what `PersistedState::encode_record`/
`decode` use; `decode` returns `Result<_, FormatError>` (previously an
infallible `Vec`). See `crates/animus-control/CLAUDE.md`'s new "Versioned
formats" section for the full shape and the exact torn-tail/`Err`
semantics.

Three notes for whoever reads this row next:

1. **This PR's own scope is narrower than the row above implies** — it is
   the shared convention plus `CWL1` only. The row's other two items
   (`CSN1` snapshot/`InstallSnapshot` envelope, `Metadata`'s top-level
   `"v"` field + the `cp_member_addrs` legacy-field drop) are later PRs in
   this same stacked series, not done yet as of this note.
2. **The row's own "replacing the two-generation structural-sniff scheme
   in `persist.rs`" description was already stale by the time this PR read
   it.** `persist.rs` had already moved past that scheme onto a
   checksummed-but-untagged `<crc32>:<json>` line (issue #495, landed
   before this ADR existed) — there was no structural sniff left in the
   code to remove; this PR's actual predecessor state was "checksummed,
   not tagged," not "two structurally-disambiguated generations." Recorded
   here as the general reminder this ADR's own workstream table already
   asks for elsewhere: verify an inventory claim against the code before
   relying on it, rather than propagating stale prose forward.
3. **One deliberate deviation from the Version tag shape convention's
   plain raw-`u8` byte, confined to the *line* shape only**: `encode_line`/
   `decode_lines` render the version as two lowercase hex digits rather
   than a raw byte, because a raw version byte can equal `\n` (version 10)
   and corrupt the line's own delimiter. The value is still a `u8`
   (`0..=255`); only its line-format rendering differs. `wrap`/`unwrap`
   (the binary envelope, no line delimiter to protect) keeps the plain raw
   byte this ADR's convention describes. Workstream C inherits this
   unchanged when it adopts `encode_line`/`decode_lines` for its own
   `SWL1` envelope.

**Workstream B as-built, PR 2 — `CSN1` on the control-plane snapshot/
`InstallSnapshot` payload.** Landed `persist::CONTROL_SNAPSHOT` (magic
`CSN1`, version 1, `format::wrap`/`format::unwrap`'s binary shape). One tag
covers **both** producers of "the bytes a control-plane `InstallSnapshot`
transfer carries": the real, `DRIVER_APPLIED` control plane's actual
payload — `node.rs`'s `syskv_image`/`install_syskv_image`, wrapping/
unwrapping the system-keyspace image (a `serde_json` `Vec<(key,
value-or-tombstone, version)>`, **not** a serialized `Metadata` blob, per
this ADR's own investigation note on `Metadata` being `DRIVER_APPLIED`) —
and `RaftCore`'s own generic `!S::DRIVER_APPLIED` fallback in `raft.rs`
(`snapshot_upto`/`recovered`/`handle_install_snapshot`), exercised in this
workspace only by the toy test state machine, wrapped for consistency
rather than because production traffic reaches it. `install_syskv_image`'s
decode-failure handling is a **new, unconditional** path, deliberately
distinct from its pre-existing `merge_batch`-failure handling: a
`CONTROL_SNAPSHOT` envelope failure is logged at `error`, installs nothing
and halts the node (the core has already adopted the snapshot by then, so
skipping would silently diverge engine from Raft state), whatever the driver's halted state, never
a panic — a format-decode failure is not the same claim as a real engine
I/O fault, which keeps its original halted-gated-panic discipline
untouched. `raft.rs`'s own generic-path decode failure keeps its
pre-existing behavior (log, then `InstallSnapshotResp { last_index: 0,
next_offset: 0 }` to restart the transfer) unchanged, now just logged by
name. Golden fixture: `tests/fixtures/formats/control-snapshot/v1.bin`,
following `control-wal`'s established pattern in the same `tests/
format_fixtures.rs`. See `crates/animus-control/CLAUDE.md`'s "Versioned
formats" section for the full account. The row's remaining items —
the `cp_member_addrs` legacy-field drop (PR 3) and `Metadata`'s top-level
`"v"` field (PR 4) — are later PRs in this same stacked series.

**Workstream B as-built, PR 3 (legacy drop) and PR 4 (`Metadata`'s `"v"`
field + the system-keyspace mirror's version row).** PR 3 removed the
legacy CP-member address book (`Metadata::cp_member_addrs`/
`cp_member_tablets`, `MetaCommand::RegisterCpAddr`,
`syskv::EntityKind::CpMemberAddr`) outright, before any `Metadata` field
was versioned, so the `Metadata` golden fixture never contains those keys.
PR 4 landed `meta::METADATA_VERSION` (currently `1`) and `#[serde(rename =
"v")] pub version: u32` on `Metadata`, plus `Metadata::from_json(bytes) ->
Result<Metadata, format::FormatError>` as the named-error decode path for
untrusted standalone documents. `Metadata`'s derived `Default` became a
manual impl (every field but `version` still `Default::default()`;
`version` is always `METADATA_VERSION`, never `0`).

**Deviation from the "required, no default" wording above.** The `"v"` field
*does* carry a serde default (`metadata_v1`, the literal `1`) and a
validator that rejects a present `0`/`> METADATA_VERSION` for plain serde
decoding. Why: pre-baseline detection is the *envelope's* job — the CWL1/CSN1
magic already rejects untagged pre-baseline bytes — so a `Metadata` found
inside a tagged v1 envelope is post-reset by construction, and v1 is the only
`Metadata` schema that ever existed without the tag; an absent `"v"` there
unambiguously means v1. It is also forced by the frozen
`tests/fixtures/formats/control-wal/v1.bin`, which embeds a `Metadata`
serialized before the field existed and may never be edited.
`Metadata::from_json` stays **strict** (missing `"v"` is
`PreBaselineFormat`, unknown is `UnsupportedFormatVersion`) for any bare
`Metadata` document, e.g. the admin `Status` response.

**The one surprise this work's investigation turned up, confirming the ADR's
own "verify an inventory claim against the code" reminder from PR 1's note
above**: `Metadata`'s own `"v"` field versions *less* than it looks like it
should — `Metadata` is `DRIVER_APPLIED` (ADR 0038), so its own
`Serialize`/`Deserialize` impl is never what actually reaches the real
system-keyspace engine on the production path; a type's version field can
only version storage that actually serializes the type. The mirror needed
its **own** independent signal instead:
`mirror::SYSKV_FORMAT_VERSION_COUNTER`/`mirror::SYSKV_MIRROR_VERSION`, an
ordinary `EntityKind::Counter` row written unconditionally (idempotent) on
every durable apply-task batch alongside the pre-existing `_applied_index`
watermark, but excluded from the delta ring.
`mirror::rebuild_metadata_from_engine` now returns `Result<Metadata,
mirror::RebuildError>` (`Storage`/`Format` — a non-empty reserved keyspace
missing the row is `FormatError::PreBaselineFormat { format:
"syskv-mirror" }`, an unrecognized version is
`FormatError::UnsupportedFormatVersion`, an empty keyspace is `Ok`); both
real call sites (`node.rs`'s `meta_apply_seed` and
`meta_apply_and_compact`'s post-`InstallSnapshot` rebuild) give a
`RebuildError::Format` the established halt-not-panic treatment, while
`RebuildError::Storage` keeps the pre-existing hard panic. No separate
`syskv-mirror` golden fixture — that row is mirror-internal and unit-tested
directly. Golden fixture: `tests/fixtures/formats/metadata/v1.json` (a
`Metadata` built by applying real `MetaCommand`s). Lessons recorded under
`docs/lessons/code-patterns/`. See `crates/animus-control/CLAUDE.md`'s
"Versioned formats" section for the full account.

**Workstream C as-built (2026-09-29), layer 1 — segment codec.**
`crates/animus-cp-data/src/segment.rs`'s `VERSION` is reset 2 → 1 (magic
`SEGF` unchanged; the baseline layout is the one with the per-record
`ordinal`, issue #852). The codec had no legacy decode branches — an
unrecognised version was already a hard error — so the reset is the counter
plus the error type: `SegmentError` (formerly a plain `String`) is now
`animus_control::format::FormatError`, so a missing/foreign magic (or input
too short to hold `magic + version`) is `PreBaselineFormat { format:
"segment" }`, a version of `0` or above `VERSION` is
`UnsupportedFormatVersion { format: "segment", found, max_supported }`, and
any other framing damage (truncation, trailing bytes, `shard_id`
mismatch, bad presence flag) is `Malformed { format, detail }`. Every
existing caller only `Display`s the error, so none changed. Golden
fixture `crates/animus-cp-data/tests/fixtures/formats/segment/v1.bin`
(decode + structural asserts, decode-then-encode round-trip, an
`#[ignore]`d refuse-to-overwrite generator) lives in
`crates/animus-cp-data/tests/format_fixtures.rs`, the file the later
workstream C layers (RaftKV codec, `SharedWal` envelope, key-layout marker)
extend. Backup/PITR/export segment objects reuse this codec unchanged, so
they inherit the reset; workstream E confirms rather than re-versions.

**Workstream C as-built — `SWL1` on the `SharedWal` outer line envelope.**
Landed `persist::SHARED_WAL_TAG` (magic `SWL1`, version 1,
`format::encode_line`/`decode_lines`'s line shape), converting
`PersistedState::encode_tagged_record`/`decode_tagged` off the private
untagged `<crc32>:<json>` helpers (deleted). Two corrections to this
table's row C prose: (a) **the `SharedWal` code lives in `animus-control`**
(`persist.rs`/`shared_wal.rs`), not `animus-cp-data`, so this layer
necessarily edits those files despite the row's crate list and its "do not
touch `persist.rs`" note — that note was written on the assumption the
envelope lived in cp-data; (b) **the inner `record` is not a `codec.rs`
payload**: it is the generic `WalRecord<KvCommand, KvState>` as `serde_json`
— the same shape `CWL1` carries — so the envelope is `{"tablet":..,
"record":..}` JSON inside the tagged line, and there is no separate
versioned inner codec to coordinate with. Decode semantics match `CWL1`'s:
a torn/CRC-failed tail is a silent stop (`Ok` with the valid prefix — the
crash-recovery contract is unchanged); a CRC-valid line with no `SWL1`
magic (a pre-baseline untagged line) is `PreBaselineFormat`, an unknown
version `UnsupportedFormatVersion`, and a CRC-valid `SWL1` line whose JSON
does not parse is `Malformed` — loud, where the old decoder silently
truncated. `SharedWal::open` maps any of these to an `InvalidData`
`io::Error`, so a node refuses to start on a pre-baseline shared WAL
rather than recovering an empty/truncated one. Golden fixture:
`crates/animus-control/tests/fixtures/formats/shared-wal/v1.bin`
(three tablets interleaved, every `WalRecord` variant), with the decode,
round-trip and `#[ignore]`d generator tests in the same
`tests/format_fixtures.rs`.

**Workstream C as-built (2026-09-29), layer 3 — RaftKV codec.**
`crates/animus-cp-data/src/codec.rs`'s `VERSION` is reset **32** → 1 (row C
and the inventory table said 31; the counter had moved on). The magic stays
the single byte `0xCB` + a `u8` version (not widened to a 4-byte magic — it is
a wire/image frame, and the shape already had strict-equality version checks and
no legacy decode arms). **Scope clarification:** the binary codec covers only
`encode_wire`/`decode_wire` (`KvWire`: Raft, ReadProbe, ReadProbeAck,
HeartbeatBatch) and `encode_image`/`decode_image` (the `InstallSnapshot` engine
image with its `max_ts` header). The durable `KvCommand` lives in the Raft WAL
as `serde_json` inside `WalRecord<KvCommand, KvState>`, carried by the `CWL1`
(per-group) / `SWL1` (shared) line envelopes from layer 2 and workstream B —
that is the compatibility-relevant format, and it gets its own fixture. Errors
are now `FormatError` (formats `raftkv-wire`, `raftkv-image`): empty input or a
magic mismatch is `PreBaselineFormat`, version `0` or above `VERSION` is
`UnsupportedFormatVersion`, all other framing damage is `Malformed`; every call
site (`warn!` and drop) only `Display`s it. Pre-baseline serde compat removed
inside `animus-cp-data`: `TxnWrite.stage_marker` and `TxnWrite.pending` no
longer `#[serde(default)]` (and, because serde defaults a missing `Option`
field to `None` regardless, use a `deserialize_with = "required_option"` so a
missing field really is a decode error). Golden fixtures:
`crates/animus-cp-data/tests/fixtures/formats/{raftkv-wire,raftkv-image,raftkv-wal}/v1.bin`;
their tests are in-crate (`src/format_fixture_tests.rs`) because the wire/image
codec is `pub(crate)`. **Open items for other workstreams' owners** — pre-baseline
compat `#[serde(default)]`s outside C's crates, left untouched: `animus-item`
(`index.rs`: `ChangeRecord` and related fields), `animus-tablet` (`lib.rs`:
`table`, lifecycle state and split fields on the tablet descriptor),
`animus-control` (`persist.rs` `WalRecord::Snapshot.config`/`learners`;
`raft.rs` `LogEntry.config`/`learners`, `AppendEntries`/`InstallSnapshot` wire
fields; `schema.rs` and `meta.rs` catalog/`Metadata` fields, including
`IndexStatus::active` and `default_node_role`), each of which is a persisted or
wire shape a baselined format should either require or document as genuine
semantics.

**Workstream C as-built (2026-09-29), layer 4 — engine layout marker.**
Row C's fourth item (a version marker on the per-tablet engine key layout) is
implemented as `crates/animus-cp-data/src/layout.rs`, **deviating from this
ADR's recommendation of "a single reserved leading byte in the `kind`
namespace"**. A new kind byte is not free: the kind bytes (`KIND_BASE` `0x00`
.. `KIND_CURSOR` `0x04`) double as indexes into `ALL_KINDS`' scope table
(`install_engine_image` does `kind_scopes.get(kind as usize)`), so a new byte
would have to be excluded from every all-kinds scan, snapshot image and
classifier — a wide change to hot code with a silent-misclassification failure
mode. A per-record magic was rejected for changing every key/value and
breaking the `kind || logical` range-scan convention. Instead the marker
reuses the **engine-global marker family** (`applied`/`hwm`/`seal`/`ceiling`/
`split`/`trim_marker`), keyed `escape(RESERVED_NAMESPACE) || escape("cp_layout")
|| tablet_be` — a `0x5F`-leading prefix already disjoint from every kind scope,
so `engine_image`, `has_data` and the seal/ceiling/applied scans skip it with
no change. Value `b"KLY1" || epoch(u8)`, `LAYOUT_EPOCH = 1`; errors are
`FormatError` (format `cp-engine-layout`: wrong/missing magic
`PreBaselineFormat`, epoch `0` or above the build's `UnsupportedFormatVersion`,
wrong length `Malformed`). `Reconciler::ensure_engine` checks it after
`factory.open`: valid marker for *this* tablet id → ok; marker absent on an
empty engine → stamp (first write, before any Raft/`InstallSnapshot`/
`SeedBatch`/applied write); absent on a non-empty engine (pre-baseline, or only
another tablet's marker) or present-but-unusable → **refuse — logged at error,
tablet not hosted, engine never destroyed** (deliberately *not* the
issue-#554 destroy-and-rebuild path, which would erase a pre-baseline or
newer-version engine). A split child is stamped by `trim_split_child` in the
same write batch as its trim-completion marker, so trim completion implies a
layout marker. Golden fixture
`crates/animus-cp-data/tests/fixtures/formats/cp-engine-layout/v1.bin`
(`u32`-BE length-prefixed key then value; in `src/format_fixture_tests.rs`).
The stamp is isolated from kind rows: the reconciler follows it (and a split
child's trim batch) with `EngineFactory::flush_engine`, an `LsmEngine`-backed
factory's `flush_now()`, so the `0x5F` marker lands in its own SSTable rather
than widening the first table of kind rows up to the reserved namespace (which
would defeat `clone_to_filtered`'s whole-file exclusion on a split until
compaction).

### Workstream E as-built (2026-09-29)

Landed as a four-layer stacked series (`animusd` + `animus-operator`).
Recorded here as one addition, not a rewrite of the row above.

1. **`ClusterConfig` required `"v"`** (PR #1099): a missing/unsupported
   `"v"` is a typed `FormatError` (via a new `ConfigError`), checked
   before serde runs in `from_json`; golden fixture
   `cluster-config/v1.json`. The operator's `cluster.json` mirror emits
   `"v"`; its own re-parse of previously applied ConfigMaps keeps a serde
   default for it, a deliberate, commented exception.
2. **Operator CRD required `spec.schemaVersion`** (PR #1100):
   `CONTENT_SCHEMA_VERSION = 1`, required in the CRD schema with
   `minimum: 1`. `validate_spec` and the admission webhook reject 0 and
   future values; the controller sets a `SchemaVersionInvalid` condition
   and applies nothing. Fixture `animuscluster-spec/v1.json`. The
   Kubernetes API version (`v1alpha1`) is unchanged.
3. **Backup manifest/data** (PR #1101): both were already `BKMF`/`BKDT` +
   a `u8` version-1 envelope, so **no `"v"` was added inside the JSON
   body** — a deliberate deviation from the row's wording, since the
   envelope is this ADR's binary tag shape and a second in-body counter
   would be redundant. Typed `FormatError`; fixtures
   `backup-manifest/v1.bin` and `backup-data/v1.bin`; the inventory row
   was corrected.
4. **PITR inheritance** (this PR, the last of Workstream E): sealed PITR
   segments are written by `segment.rs`'s own codec (`index_drain`'s
   seal path), so they carry Workstream C's `VERSION` reset with no
   independent tag. `pitr_seal_happy_path_and_disable_reenable_continues_
   epoch_chain` now reads the raw stored object and asserts it begins
   `SEGF` followed by `segment::VERSION` (never a literal, so it is
   correct before and after C lands), that the restore read path
   (`decode_and_slice`) accepts it, and that a bumped version byte is a
   loud error. S3 export/import (ADR 0068) does not use the segment
   codec at all — it emits/consumes AWS's own JSON-lines/gzip layout — so
   there is nothing in-repo to version. **Follow-up once C's PR #1089
   merges:** add a fixture-decode test against C's
   `segment/v1.bin` (it does not exist on `main` yet).

This is the last PR of Workstream E. **Baseline: `9a9f972f`** (full SHA in
Maintainer decision point 2 and the "Phase 0 complete" amendment below) —
set once all of A/B/C/D/E had merged.

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

- **As of the 2026-09-27 maintainer decision, this is accepted and Phase 0
  is funded and in progress.** The paragraph below describes the original
  proposal's assumption (before acceptance, nothing changes); it is kept
  for record. What actually changes today: every format in the inventory
  is queued for the Phase 0 reset (workstreams A–E above); nothing gains an
  N-1 *compatibility* guarantee yet (that is Phase 1), but every format
  does gain a version tag, a golden fixture, and a CI guard that a fixture
  is never silently edited — a real, if partial, tightening starting now.
- ~~Before this is accepted, nothing changes.~~ No format in the inventory
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

- **Support window length, post-baseline.** **Resolved 2026-09-30:**
  every post-baseline format version stays readable forever; old decoders
  and fixtures are never deleted (see the "Phase 1 design" amendment).
  The N-1 / N-2 / skip-version-policy candidates listed here earlier are
  moot; the only remaining lever is an explicit ADR amendment naming a
  break and its migration path (the escape hatch in Maintainer decision
  point 3).
- **Which formats go first inside Phase 1?** **Resolved 2026-09-30** by the
  "Phase 1 design" amendment (workstreams P1-A..P1-D, backups/PITR/export
  first). Original text, kept for record — narrowed:
  Phase 0's own workstream split (A–E above) already sequences the *reset*
  by crate; Phase 1's own sequencing (which reset format gets its N-1
  decode path first) still prioritizes backups/PITR/export (already
  outlive the cluster) and the RaftKV codec + LSM formats (highest change
  frequency, sit under everything else), but the actual order is a sizing
  decision for whoever picks Phase 1 up, not fixed here.
- **Freeze the Raft codec, or keep translating it?** Still open, sharper
  still under the 2026-09-30 forever-readable window (every bump now costs a
  permanent `legacy` decoder + encoder, so bump cadence is a real budget).
  Earlier text: sharper: Phase 0 resets `codec.rs::VERSION` to 1, so this question is
  really "how many more times will Phase 1 let it bump before freezing the
  wire/log shape and pushing further evolution into a schema-versioned
  inner envelope the codec itself doesn't need to understand." Not decided
  here — a Phase 1 design question.
- **The WAL back-compat fields ADR 0032/0040 already keep.** Resolved for
  Phase 0: per the Maintainer decision point 1, Phase 0 workstream B drops
  these outright rather than folding them into the generic version-gate
  mechanism — "no production cluster yet" makes the pre-baseline case they
  exist for moot. Still open for *anything of the same shape discovered
  after the baseline*: a future one would need Phase 1's real N-1 decode
  path, not another ad hoc field.
- **Does the hash-ring/key-encoding layer belong in this plan at all**, or
  is it better served by staying a documented, deliberate breaking-change
  category forever (i.e. explicitly *not* frozen even after Phase 1), with
  Phase 1 covering everything else? Still open — see the Consequences
  section's note above. Phase 0 does not resolve this either way: it is
  reset like everything else (no version tag is added to it in this pass,
  since it is a key-space convention, not a framed record — see the
  inventory table's own note), but whether it ever gets a Phase 1
  compatibility mechanism at all remains undecided.

## Amendment 2026-09-30 — Phase 0 complete; baseline set

Phase 0 is **done**. The baseline is `9a9f972fb38a766be4a4b044379540ded533eefc`
(`9a9f972f`), "Merge pull request #1104", merged 2026-09-29 22:50 UTC — the
merge commit where the last workstream landed. The workstream rows' as-built
notes above are unchanged.

- **Workstream PRs.** D (wire handshake): #1056, #1059, #1066, #1069. B
  (`animus-control`): #1058, #1063, #1087, #1088. A (`animus-storage`): #1079,
  #1082, #1086 (plus the related fix #1081). C (`animus-cp-data`): #1089,
  #1090, #1091, #1097. E (`animusd`/operator/backup): #1099, #1100, #1101,
  #1104.
- **Verification.** The baseline commit's own `main` CI run was cancelled by
  the workflow's concurrency setting (a newer push superseded it), so it has
  no green run of its own. The covering verification is `main` CI at
  `516759d4` (= `9a9f972f` plus #1108, a test-only flake fix): every gate and
  every prod-liveness job green. The `kind` e2e smoke passed on `main` at
  `0fa0cf47` (the #1100 operator merge).
- **What applies from the baseline on.**
  - Durable formats must stay readable by newer builds (a full-cluster stop,
    upgrade, restart with no data loss and no manual conversion).
  - Golden fixtures are append-only: a format change is a new version tag, a
    decoder that accepts the older versions, and a new fixture; an existing
    fixture is never edited or deleted (`scripts/check-format-fixtures.sh`
    enforces this in CI).
  - Wire compatibility applies once Phase 2 lands; before that, wire changes
    stay free but additive-by-design where cheap.
  - Rolling upgrades are supported once Phase 3 lands. Until then an upgrade
    is still a full-cluster stop and restart.
- **Phase status.** Phase 0 done. Phases 1 (on-disk N-1 readability, backups
  and PITR first), 2 and 3 remain planned; the "decoder still accepts every
  older version" half of the durable-format rule is reviewed by hand until
  Phase 1 implements a real N-1 decode path per format.
- **Inventory table.** The *Location* and *Version tag today* cells were
  refreshed to the as-built constants (see the note above the table).

## Amendment 2026-09-30 — Phase 1 design

Maintainer decisions, 2026-09-30:

1. **Support window: every post-baseline format version stays readable
   forever.** A newer binary must decode every version written by any
   post-baseline binary. Old decoders and old fixtures are never deleted.
   This resolves the "Support window length" open question (struck above)
   and replaces the earlier "until the first tagged release" default.
2. **Phase 1 starts with a design PR** (this amendment), then per-crate
   implementation sessions fan out (workstreams P1-A..P1-D below).

This amendment changes no format, no code and no fixture. It records the
audit that sizes Phase 1, the decoder pattern, the format-change checklist,
the upgrade-restart harness design, the workstreams, and the definition of
"Phase 1 done".

### Audit: is "forever readable" already mechanically enforced?

All formats are at v1, so nothing has yet had to keep a second decoder. The
hypothesis was that the fixture layer already enforces most of the rule:
every format's fixture test iterates its `tests/fixtures/formats/<format>/`
directory, and `scripts/check-format-fixtures.sh` forbids editing or
deleting a checked-in fixture, so dropping the v1 decoder fails CI.
Verified format by format against the code at `cbd23d05`:

| Format (fixture dir) | Crate | Fixture test | Per-version expected values? | Version-dispatch shape | Gap |
|---|---|---|---|---|---|
| `lsm-wal` | `animus-storage` | `src/lsm.rs`, `wal_format_fixture_tests::decodes_every_checked_in_fixture`; round trip present | **No** — every file is asserted equal to the one `representative_records()` | File-header gate `version == 0 \|\| > WAL_VERSION` → named error; body is one decode path, version not passed on | Same expected value for all files (a v2 with new information cannot be expressed); no dispatch seam |
| `lsm-manifest` | `animus-storage` | `src/lsm.rs`, `decodes_every_checked_in_fixture`; round trip `binary_manifest_round_trips` | **No** — same `representative_manifest()` for every file | Gate on `1..=MANIFEST_VERSION`, then a single inline field-by-field decode | Same as above |
| `lsm-sstable` | `animus-storage` | `src/lsm/sstable.rs`, `decodes_every_checked_in_fixture`; round trip `writer_output_round_trips_the_fixture_records` | **No** — same `fixture_records()`; every file is opened with `FORMAT_CURRENT`, not the version its file name says (the format lives in the manifest, not the file) | Best shaped: `check_format(meta.format)` at open and again in `read_block`, decode arm commented "Format 1" | A v2 fixture needs its format supplied from the file name; `read_block` has no `match` yet |
| `encryption-envelope` | `animus-env` | `src/encrypted.rs`, `decode_every_fixture_matches_current_code`; round trip present | **Only v1** gets the known-value asserts; any other file silently gets the weaker checks (magic, version range, decrypts) and **does not panic** | `scan` returns `UnsupportedVersion` for `0`/`> VERSION`; one frame decoder | A v2 fixture would pass with almost no content check |
| `network-handshake`, `client-handshake` (wire, Phase 2) | `animus-env` | `tests/format_fixtures.rs`; round trips present | Asserts *every* fixture has `version == spec.version` (the **current** version) and an empty `ext` | `decode` reads any version; `check_peer` is exact equality (by design until Phase 2) | The test itself would fail on the retained v1 file the moment `spec.version` becomes 2, i.e. it forces the fixture to be deleted or the test rewritten |
| `control-wal`, `control-snapshot`, `shared-wal`, `metadata` | `animus-control` | `tests/format_fixtures.rs`; round trips present | **Yes** — `match` on the file name, hand-written expectation, `panic!` on an unrecognised fixture | `format::decode_lines`/`unwrap` gate on `1..=tag.version` and **return the version**, but every caller (`persist.rs` x2, `node.rs`, `raft.rs`) discards it (`_version`) and parses one serde shape. `Metadata`'s `"v"` has a serde default (deliberate, see Workstream B PR 4) and one struct. **P1-C decision: the default stays** — the frozen `control-wal`/`shared-wal`/`control-snapshot` fixtures embed a `Metadata` with no `"v"` and may never be edited, and the outer `CWL1`/`CSN1` envelope versions the record; the default is the literal `1`. `Metadata::from_json` now dispatches with `match` on the peeked `"v"` (strict: missing is pre-baseline, non-1 unsupported) and the fixture test is per-version (version from the file name, per-version expected value, `panic!` on an unrecognised one) | Gate, not dispatch: bumping the tag and changing the payload shape in place would be accepted for v1 bytes and misread unless a serde default happens to cover it |
| `mirror-entities` (one directory per `syskv::EntityKind`, 18 at `9a9f972f`+) | `animus-control` | `tests/format_fixtures.rs`; round trip present; scenario test proves every kind is produced by the real encoder | **Yes** (`match` on the file-name version, hand-derived expected `Metadata` per kind, `panic!` on an unrecognised version; every kind directory must hold a fixture for `SYSKV_MIRROR_VERSION`) | Untagged per-entity values (JSON via `put_json`, 8-byte big-endian counters, empty presence markers); `apply_put` decodes with one serde shape per kind and is not given a version | Dispatch at keyspace level (`mirror-version`, `rebuild_metadata_from_engine`), no per-entity dispatch: a value-shape change must bump `SYSKV_MIRROR_VERSION`, keep the old `apply_put` arm under `legacy`, and add a `v2` fixture per changed kind |
| `mirror-version` | `animus-control` | `tests/format_fixtures.rs`; decodes through `rebuild_metadata_from_engine` over a `MemoryEngine`; current-encoder equality test | **Yes** (`match`, `panic!` otherwise; current version required) | `rebuild_metadata_from_engine` **dispatches** (P1-C, 2026-09-30): `match found_version { 1 => rebuild_metadata_v1(..), v => UnsupportedFormatVersion }`; missing row on a non-empty engine is `PreBaselineFormat`; `0`, `2` and `u64::MAX` are refused by name (test `rebuild_from_engine_dispatches_on_the_version_row`) | Real dispatch at keyspace level (was a range gate) |
| `raftkv-wire`, `raftkv-image`, `raftkv-wal`, `cp-engine-layout` | `animus-cp-data` | `src/format_fixture_tests.rs`; round trips present | **Yes** (`match version`, `panic!` otherwise; `raftkv-wal` asserts all 16 `KvCommand` variants) | `check_header` (`codec.rs`) and `decode_layout_value` gate `1..=CURRENT`; single body decoder. `raftkv-wal` shares `control-wal`'s line gate | Gate, not dispatch (as above) |
| `segment` (also PITR objects) | `animus-cp-data` | `tests/format_fixtures.rs`; round trip present; `animusd/src/index_drain.rs` also reads `segment/v1.bin` through the PITR restore path | **Yes** | `decode` gates `1..=VERSION`, then a single `decode_body` | Gate, not dispatch. Test hardcodes `assert_eq!(segment::VERSION, 1, "Phase 0 baseline")` (fails on bump: acceptable forcing function, but it should assert "fixture for `VERSION` exists" instead) |
| `backup-manifest`, `backup-data` | `animus-cp-data` | `tests/backup_format_fixtures.rs`; round trips present | **Yes** (`match`, `panic!` otherwise) | Via `format::unwrap`; `_version` discarded at the call | Gate, not dispatch. Outlives the cluster, so highest priority |
| `cluster-config` | `animusd` | `tests/format_fixtures.rs`; round trip present | **Yes** (`match` on the file name; comment says a new arm is required before a new fixture) | `from_json` peeks `"v"` and `match`es on it (P1-C: `1` => current type, else a named error); `"v"` stays required with no serde default (the config is only a standalone document, and its fixture carries `"v"`); fixture test is per-version | None |
| `animuscluster-spec` | `animus-operator` | `tests/format_fixtures.rs`; lossless round trip present | **No** — `assert_v1_structure` is applied to every `.json`, including asserting `schema_version == 1` | `validate_spec` rejects `0`/future `schemaVersion`; one serde type | A v2 fixture would fail the v1 asserts; no dispatch seam |

Outside the fixture directories, three more durable groups matter:

- **Hash-ring token / key encoding** (`animus-tablet` murmur3, `animus-item`
  `numkey` and `key_bytes`): no tag by design (a key-space convention, see
  the inventory). It *is* pinned, but by in-source reference-vector and
  differential tests, not by `tests/fixtures/formats/`, so
  `check-format-fixtures.sh` does not cover it.
- **Row values that were never in the inventory**: the stored-item codec
  (`animus-item/src/stored.rs`, `{"item": ..}` / `{"tombstone": true}`
  JSON in every base-row value), `ChangeRecord` (`animus-item/src/index.rs`,
  serde JSON with `#[serde(default)]` additions), and the per-entity JSON
  values of the `Metadata` system-keyspace mirror (`syskv.rs` /
  `mirror.rs`). They are untagged, and no fixture pins their exact shape
  (they appear only opaquely inside `raftkv-wal`, `segment`, `backup-data`
  and the `control-snapshot` fixture, whose values are placeholder JSON).
  Two of them are read back out of backups and PITR segments, so they are
  the real contents of the objects that outlive a cluster. This is the most
  consequential audit finding.
- **S3 export/import**: AWS's own format, no in-repo version, no fixture.
  Nothing to add beyond keeping the item JSON stable (previous bullet).

**Verdict.** The hypothesis holds for the *deletion* half and is weaker for
the *misread* half:

- *A v1 decoder cannot be dropped silently.* All 19 fixture directories
  decode their v1 file in CI, and the guard forbids deleting or editing it.
  Even the weak tests (same expected value for every file) still force v1 to
  keep decoding.
- *A v1 file can still be silently misread after a v2 change.* Every
  decoder is a range gate (`1..=CURRENT`, named error outside it, correct
  and loud) followed by **one** body decoder; the version that was just
  checked is dropped. Adding v2 therefore means editing that one body in
  place, and v1 survives only if the single hand-written v1 fixture happens
  to exercise whatever changed. The seven formats marked "No"/"Only v1"/
  "would fail" in the table also cannot express a per-version expectation
  yet. The fixture layer is a safety net, not a structure that makes v2
  natural; Phase 1 has to add the structure.
- Twelve of nineteen fixture tests already fail loudly on a fixture with no
  expectation (`panic!` on an unrecognised version), which is the desired
  forcing function for checklist step 4 below. The other seven do not.

This is a small amount of implementation work (test hardening plus a
dispatch seam per format, no format changes), plus the row-value gap and
the restart harness, which is the bulk of Phase 1.

### The decoder pattern: upgrade on read

1. **Decoders dispatch on the version and translate every older version
   into the current in-memory type.** Concretely, after the existing gate
   (which stays: `0` and `> CURRENT` are named errors), the caller matches
   on the version it was just handed (`format::unwrap`/`decode_lines`
   already return it; the header checks in `codec.rs`, `segment.rs`,
   `lsm.rs` and `layout.rs` need to start returning it):

   ```rust
   match version {
       1 => legacy::v1::decode(body).map(Into::into),   // frozen
       2 => decode_v2(body),                            // current
       found => Err(UnsupportedFormatVersion { found, max_supported: CURRENT, .. }),
   }
   ```

   An `_version` that is bound and dropped is a review failure from now on.
   Callers never see an older shape: there is exactly one in-memory type per
   format, and `From<legacy::vN::T>` is the whole translation.
2. **Writers always write the current version.** There is no "write in
   version N-1 for compatibility" mode. Old-version files persist until the
   normal lifecycle rewrites them (LSM compaction, WAL rotation, snapshot);
   an engine therefore legitimately holds several versions at once (SSTables
   of different `format` in one manifest, WAL segments of different versions
   in one directory), and every reader must handle the mix. Backups, PITR
   segments and exports are immutable, so they stay in their writer's
   version for their whole life and are read by every later binary.
3. **A newer-than-supported version stays a named error**
   (`UnsupportedFormatVersion { found, max_supported }`), never a silent
   misread and never a panic. **There is no downgrade**: after a node has
   restarted on the new binary and written new-version data, an older
   binary refuses it by name. The rollback path is restore from a backup
   taken before the upgrade (readable by the new binary, and by the old one
   only if nothing newer was written to it).
4. **Where legacy code lives:** a `legacy` module beside each format's
   current codec, one submodule per retired version
   (`codec/legacy/v1.rs`, or an inline `mod legacy { pub mod v1 { .. } }`
   for a small format). It holds the frozen decoder, the frozen shape type
   the decoder produces (`V1Foo`), and the `From<V1Foo> for Foo`
   translation. Kept forever; edited only by mechanical compile fixes, never
   to change behaviour. **A legacy module must not reuse a type whose
   serialization can change without a version bump**; if it embeds
   `AttributeValue`/`Item`, that type's serde shape is itself a frozen
   format (see P1-A's row-value fixtures), so a change to it is a format
   change with its own tag and checklist run, not a refactor.
5. **Legacy encoders are test-only** (see the harness section): a
   `legacy::vN::encode`, compiled under `#[cfg(any(test, feature =
   "legacy-encoders"))]` (feature off by default, enabled only by test
   crates), that is anchored to the checked-in fixture by a byte-equality
   test.

### The format-change checklist

Adding version N+1 of any durable format (every item is part of the same
PR series; a PR missing one is not mergeable):

1. Bump the version constant.
2. Keep the vN decode path: move it to `legacy::vN` (decoder, frozen shape,
   `From` translation), and route the version `match` to it.
3. Add a `vN+1` fixture with the format's `#[ignore]`d no-overwrite
   generator (`generate_fixture_<format>`). Never regenerate vN.
4. Add a per-version expected value to the decode test (the `match` arm for
   `vN+1`; an unrecognised fixture panics, so the test fails until this is
   done).
5. Add a round-trip test at the new version (encode with the current
   writer, decode, compare).
6. Add a test that vN input (the retained fixture, and the legacy encoder's
   output) decodes to the correct *current* value, i.e. that the
   translation fills new fields with what an old writer implicitly meant.
7. Add `legacy::vN::encode` (test-only) with the byte-equality test against
   `vN`'s fixture, and register the new pair in the upgrade-restart
   harness's per-format transcode table.
8. Update this ADR's inventory row (version tag today, fixture, legacy
   module) and, if the change is not purely additive to a wire enum, say so.

Steps 1-7 are mechanical once the seam exists; step 8 is review.

**What is enforced mechanically once Phase 1 lands, and what stays
review.** Mechanical: fixtures are never edited or deleted (CI script);
every fixture must decode and match a hand-written per-version value, and an
unrecognised fixture panics (all formats after P1-A..P1-C); the legacy
encoder reproduces the fixture byte-for-byte; the harness restarts on
version N-k state built by the legacy encoders and verifies every
acknowledged write (P1-D). **Still review:** that a legacy module was not
altered in behaviour; that a type embedded by a legacy shape did not change
serialization; that a new `#[serde(default)]` on a JSON format really means
"what the old writer meant" (the audit's most fragile area: `Metadata`,
`ClusterConfig`, `ChangeRecord`, the mirror entities); that step 8 was done.
Until P1-A..P1-C land, the "decoder still accepts every older version" half
is enforced only by the v1 fixtures still decoding (see the verdict above)
and by review.

### The upgrade-restart harness

**Property.** A node or cluster writes durable state in the format versions
of an older post-baseline build, stops or crashes, and restarts on the
current code. Every acknowledged write survives and reads back identically;
no named format error is produced for a supported version; the restarted
cluster keeps accepting writes and rewrites data in the current version.

**What exists today, and why it is not enough.** `SimCluster`'s restart
keeps a node's engine as an in-memory `MemoryEngine` (`sim_cluster.rs`
module doc), so a "restart" never re-reads any encoded file: the formats
Phase 0 tagged are exactly what it does not exercise. The `raftkv` corpus's
`ANIMUS_RAFTKV_LSM=1` and `ANIMUS_RAFTKV_WAL_FAULTS=1` tiers, and the LSM
crash corpora, do restart over `LsmEngine<SimEnv>` and real WAL bytes, so
disk-backed restarts on `SimEnv` are an established pattern; they only ever
read what the current binary wrote.

**Old encoders: decision.** Two options:

- *Restore from checked-in fixtures.* Free (they exist), and a fixture is
  frozen forever by construction. But a fixture is one small representative
  value per format, not a seed-driven workload; it cannot hold thousands of
  acknowledged writes, interleavings, torn tails or several tablets, so it
  can only prove "this file decodes", which the fixture tests already prove.
- *Test-only legacy encoders beside the forever-kept legacy decoders.*

**Recommendation: test-only legacy encoders (`legacy::vN::encode`), anchored
to the fixtures, used by the harness through a transcode-at-rest step, with
fixtures kept as the cheap tier-0.** Justification:
(a) the property to prove is about *workload-shaped* state (acked writes
across restarts), which only an encoder can produce at scale; (b) the
fixture byte-equality test (checklist step 7) is what makes the encoder
trustworthy: an encoder that drifts from the frozen bytes fails CI, so we
never test against a fictional "old format"; (c) the cost is one encoder per
retired version per format, added at exactly the moment the format changes
and its author knows the old layout best, which is far cheaper than
resurrecting an old binary later; (d) it needs no second binary or
old-release build in CI. Harness mechanics: run the workload at the
*current* version on `SimEnv` disks, stop or crash the node, **transcode**
each durable file to version N-k with the legacy encoders (decode current →
re-encode as N-k, record by record, per format), restart on the current
code, and verify. Because an older version can only carry what it could
express, the workload is drawn from the feature set the target version
supports (a per-version "capability mask" recorded next to the legacy
encoder; a version that lacks a field simply omits it from the workload).
Fixture-seeded restarts stay as tier-0: seed a `SimEnv` disk with the
checked-in whole-file fixtures (`lsm-wal`, `lsm-manifest`,
`lsm-sstable`, `control-wal`, `shared-wal`) and open a real engine on it,
a fast per-format smoke that needs no encoder and exists from day one.

**Design.**

- Two tiers, both deterministic `SimEnv`, seed-reproducible via
  `ANIMUS_SEED`, fault-injecting per the repo convention (crash at random
  points including mid-transcode-window, torn-tail and corrupt-on-crash as
  in `ANIMUS_RAFTKV_WAL_FAULTS`, partitions during the post-upgrade
  catch-up), and mixed-version *files* (a fraction of files left in the
  current version, so a single engine holds several versions).
  - **Tier 1, `animus-test`: `upgrade_restart_corpus`.** Single tablet
    group and control group over `LsmEngine<SimEnv>` with the real
    WAL/manifest/SSTable/`CWL1`/`SWL1` paths: cells = {leader restart,
    follower restart, whole-group stop} x {clean stop, crash, torn tail} x
    target version k back. Oracle: the raftkv corpus's linearizability
    checker over pre- and post-restart histories, plus an "every acked
    write present" check.
  - **Tier 2, `animusd`: `sim_cluster_upgrade_corpus`.** Whole-cluster stop
    → transcode → restart through `SimCluster` with an `LsmEngine`-backed
    engine factory (a new option; `SimCluster` is `MemoryEngine`-only today)
    and the DynamoDB wire on top, so control `Metadata`, the mirror, tablet
    hosting, streams cursors and a backup taken *before* the upgrade
    (restored after it) are all in the loop. Second wave: it depends on the
    factory option and on tier 1 being stable.
- **Knob: `ANIMUS_UPGRADE_RESTART_SEEDS=K`** (default 1), added to root
  `CLAUDE.md`'s table when tier 1 lands; the corpus is also added to
  `corpus-deep.yml`. `ANIMUS_UPGRADE_SEEDS` (proposed in the Testing section
  above for Phase 2's mixed-version *running* cluster) stays reserved for
  that later corpus, so the two are never conflated.
- **Degenerate today, by design.** With only v1 in existence the transcode
  step is the identity, so the first landing proves the machinery (restart
  over real bytes, the oracle, seed replay) and every later format bump
  activates it by adding one table entry (checklist step 7). It must have
  teeth without a real v2: a **negative control** (as in
  `animus-test/tests/negative_control.rs`) transcodes through a
  deliberately broken translation (a legacy decoder that drops a field, a
  legacy encoder that emits a torn record) and the corpus must fail.
- **Lives in:** the corpus in `animus-test` (tier 1) and `animusd`
  (`src/sim_cluster_upgrade_corpus.rs`, tier 2); the legacy encoders in each
  format's own crate behind the `legacy-encoders` feature; the per-format
  transcode table in a small `animus-test` module that depends on those
  features (dev-dependencies only, never in a production build).

### Phase 1 workstreams

Four sessions, one PR series each (root `CLAUDE.md`, "independent work runs
in a separate session"). The audit shows little decode-plumbing work, so the
crate workstreams are deliberately small and only P1-D is large. Every
workstream produces **no format change**: v1 stays v1, no fixture is added,
no fixture is touched. Each implementation session gets this ADR in full,
its own row, the crate guides, the lessons index, and the do-not-touch
lists below.

| Workstream | Crate(s) | Scope | Depends on | Do not touch |
|---|---|---|---|---|
| **P1-A** (backups/PITR/export first; outlives the cluster) | `animus-cp-data`, `animus-item` | Dispatch seam (`match version`, `legacy` module scaffolding) for `backup-manifest`, `backup-data`, `segment` (PITR), then `raftkv-wire`/`raftkv-image`/`raftkv-wal`/`cp-engine-layout`; `segment.rs` fixture test asserts "fixture for `VERSION` exists" instead of `== 1`. **Row values**: add golden fixtures (new files under `animus-item/tests/fixtures/formats/`) for `stored-item` and `change-record`, decide tag-vs-freeze for them (proposal: freeze the serde shape, additive-only with fixtures; an untagged JSON `{` document is v1, so a later tagged v2 is unambiguous to sniff) and add the inventory rows; pin key-encoding vectors (`numkey`, `key_bytes`, murmur3) as fixtures in `animus-tablet`/`animus-item` (the guard then covers them) | none | `animus-control/**` (incl. `format.rs` — consume, do not change signatures), `animus-storage/**`, `animusd/**`, `animus-env/**` |
| **P1-B** | `animus-storage`, `animus-env` | Per-version expected values for `lsm-wal`, `lsm-manifest`, `lsm-sstable` (format derived from the file name; unrecognised fixture panics), `encryption-envelope` (panic instead of silently weaker checks), `network-handshake`/`client-handshake` (assert `version == <file version>`, not the current one); dispatch seam in `decode_wal`, `decode_manifest`, `read_block`, `EncryptedDisk::scan`; `legacy` scaffolding | none | `animus-cp-data/**`, `animus-control/**` |
| **P1-C** | `animus-control`, `animusd`, `animus-operator` | `control-wal`/`control-snapshot`/`shared-wal`: callers use the returned version and dispatch (kills the `_version` discards in `persist.rs`, `node.rs`, `raft.rs`); `Metadata`/`ClusterConfig`: per-version decode entry points; system-keyspace mirror entity-value fixtures (`mirror-entities`, one per `EntityKind`) and a mirror-version fixture; `animuscluster-spec` per-version test | none | `animus-cp-data/**`, `animus-storage/**`; do not edit `format.rs` helper signatures (P1-A consumes them as they are) |
| **P1-D** | `animus-test`, `animusd`, plus `legacy-encoders` feature plumbing in the crates above | The upgrade-restart harness: tier 0 fixture-seeded restarts, tier 1 `upgrade_restart_corpus`, negative control, `ANIMUS_UPGRADE_RESTART_SEEDS`, `corpus-deep.yml`; then tier 2 (`sim_cluster_upgrade_corpus`, `SimCluster` `LsmEngine` factory). Adds the per-format transcode table skeleton (identity today) | A, B, C for the per-format plug-ins and the `legacy-encoders` feature convention; tier 0/1 skeleton may start earlier against the convention in this ADR | The format crates' non-test source apart from the feature gate; `sim_cluster.rs` beyond the factory option |

**P1-B step 1 as-built (tests only, every format still v1).** The fixture
decode tests for `lsm-wal`, `lsm-manifest`, `lsm-sstable` and
`encryption-envelope` now take the version under test from the fixture's
**file name** (`animus-storage`'s `#[cfg(test)] fixture_file_version`; the
envelope test matches the header byte and asserts it agrees with the name)
and `match` it to a per-version expected value; a fixture whose version has
no arm **panics** instead of being skipped or weakly checked, and each test
also asserts a fixture exists for the *current* version constant.
`network-handshake`/`client-handshake` assert `version == <file-name version>`
rather than `spec.version`. The dispatch seam in the decoders is step 2 (a
stacked PR).

**P1-B step 2 as-built (dispatch seam, every format still v1; P1-B done).**
`decode_wal`, `decode_manifest`, `SsTableReader::read_block` (on
`meta.format`) and the envelope `scan` (`EncryptedDisk`) now read the version
tag and `match` it exactly: `1 => <v1 decode fn>` (`decode_wal_v1`,
`decode_manifest_v1`, `decode_block_v1`, `scan_v1` — the former bodies, moved
verbatim), `v => <the same unsupported-version error as before>`; no range
check remains. Each of the three files carries an empty `mod legacy {}` whose
doc states the upgrade-on-read contract (a `legacy::vN` decoder returns the
*current* in-memory type; never deleted). Behavior is byte-for-byte unchanged:
the existing fixture tests and the existing version-0 / newer-version tests
per decoder pass untouched, and no fixture was edited.

**Waves.** Wave 1: P1-A, P1-B, P1-C, fully concurrent (disjoint crates; the
only shared surface is `animus_control::format`, which nobody changes).
Wave 2: P1-D tier 0/1 (starts once one of A/B/C has landed its
`legacy-encoders` convention, or earlier against this ADR's text). Wave 3:
P1-D tier 2. Phase 1 is done when all four have merged.

**P1-D as-built, tiers 0 and 1 (2026-09-30; tier 2 pending; every format still
v1).** Tier 0 (#1130) seeds disks from the checked-in fixtures and restarts
the real readers; tier 1 is `animus-test/tests/upgrade_restart_corpus.rs`
(21 cells: Data / Control / SharedWal x leader / follower / whole-group x
clean / crash / torn-tail, strict engine opens, list-append oracle with a
post-restart probe, a 300s wall-clock watchdog, and for Control a check on the
driver-applied `engine_applied_index`; knobs `ANIMUS_UPGRADE_RESTART_SEEDS`,
`ANIMUS_UPGRADE_RESTART_CELL`; wired into `corpus-deep.yml` at K=100). The
transcode step is the **identity** today. **Negative controls** prove it has
teeth: dropping the final WAL record is caught for `Data` (and benign for
`Control`/`SharedWal`, which hold redundant copies); halving the log and
wiping the engine, wiping the `SharedWal` engines, and wiping everything are
each caught as lost acknowledged appends; a truncated SSTable fails the strict
open. **Three real bugs the harness found:** (1) CWL1/SWL1 treated a CRC
failure anywhere as a torn tail (handed off to its own session); (2) the
control `wal_lock` starved the ADR 0038 apply task (#1133); (3) an `LsmEngine`
WAL torn-header open failure (three TornTail seeds at K=50; fix in progress,
PR to come). Tier 2 (`sim_cluster_upgrade_corpus`) is pending. See
`animus-test/CLAUDE.md`.

### What Phase 1 "done" means, and what users can rely on

Done, when every one of these holds on `main`:

- Every durable format in the inventory (plus the row-value formats added by
  P1-A and P1-C) has: a version-dispatching decoder with a `legacy` seam, a
  per-version-asserting fixture test that panics on a fixture with no
  expectation, a round-trip test, and a fixture directory covered by the CI
  guard.
- The format-change checklist above is in root `CLAUDE.md` (pointer) and
  followed by review.
- The upgrade-restart corpus (tiers 0-2) is in the per-push gate at its
  default depth and in `corpus-deep.yml`, and its negative control fails as
  it should.

**What users can then rely on:** a **full-cluster stop → upgrade → restart
across any post-baseline versions is supported and tested**: no data loss,
no manual conversion, backups/PITR/export objects written by any
post-baseline version stay readable by every later one, and a downgrade is
refused by name (rollback is restore from backup). What they still cannot
rely on until later phases: **mixed-version running** (wire compatibility,
Phase 2), **rolling upgrades** and operator `spec.image` changes (Phase 3),
and a stable hash-ring/key encoding beyond "frozen by the vectors" (open
question below, unchanged).

**Documents that claim otherwise today, and change only when Phase 1
lands, not in this PR:** `website/architecture.html` (lines ~277 and ~292:
"on-disk formats change between revisions ... upgrading means recreating
the cluster"), `website/docs.html` (~536 and ~635), `website/how-it-works.html`
(~222), and `website/index.html`'s "On-disk format stability, then rolling
upgrades" Planned entry; also `docs/roadmap.md` C-16's status line and ADR
0060's "Upgrades: None, by design". The website copy stays true as a
statement of what is *tested and supported* (an upgrade is still cluster
recreation until the restart corpus exists) even though the durable-format
rule already binds the code; the Phase 1 close-out PR rewrites all of them
in one change and updates this ADR's status header.

**Phase status after this amendment:** Phase 0 done; **Phase 1 in progress**
(design accepted, P1-A..P1-D not started); Phases 2 and 3 planned, blocked
on Phase 1 as before.
