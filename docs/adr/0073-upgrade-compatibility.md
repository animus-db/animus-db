# ADR 0073 — Upgrade compatibility: a phased plan, not yet a promise

- **Status:** Accepted (2026-09-27) — the phased plan itself, and the
  Phase 0 conventions below, are binding from this date. This is a policy
  decision, not a claim that any phase is implemented yet: see the "Phase
  status" list and the "Maintainer decision" section below for what is
  actually in force. Root `CLAUDE.md`'s no-back-compat paragraph has been
  rewritten to match (see that file); it no longer states an unconditional
  "no compat, ever" rule — it now points here.
  **Update 2026-10-03: Phase 0 and Phase 1 are done** (see "Phase 1 as
  built (2026-10-03)" below); Phase 2 (wire/feature gate) is next, and
  Phase 3 (rolling upgrades) after it. **Phase 2 design in review
  (2026-10-03):** see "Amendment 2026-10-03 — Phase 2 design: cluster version
  and feature gates" at the end (decided: no rollback once a node has run the new binary; Phase 2 itself
  is rolling-installable over live Phase 1 binaries, no restart ever needed).
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
   - **Phase 1 — on-disk N-1 stability:** **done** (2026-10-03). Design
     accepted 2026-09-30 (window = every post-baseline version forever);
     workstreams P1-A..P1-D all merged. See "Phase 1 as built
     (2026-10-03)" below for what is enforced and what is not.
   - **Phase 2 — replicated cluster version / wire feature-gate:** planned;
     unblocked by Phase 1 (next).
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
| LSM WAL records | `crates/animus-storage/src/lsm/wal.rs` | `WAL_MAGIC = b"LWL1"`, `WAL_VERSION: u8 = 2` (file-level header; **v2** adds sync-marker frames, #1142; v1 kept as `lsm.rs` `legacy::v1`, fixture `lsm-wal/v1.bin`) | durable, node-local | Record framing has no per-record version byte; the file header carries it and `decode_wal` dispatches on it. Crash-safety relies on length-prefixed/checksummed records plus, from v2, durable sync markers that bound the provably-synced prefix; a v1 file keeps the lenient "no valid frame follows" torn-tail rule. |
| LSM SSTable data blocks + footer | `crates/animus-storage/src/lsm/sstable.rs` | `MAGIC: u64 = 0x4355_5354_4F53_5333` in the footer (file identification only — the reader takes the actual format from the manifest, not by re-reading the footer) plus a real per-table version tag, `SsTableMeta::format: u32` (`FORMAT_CURRENT = 1`), recorded in the manifest for each table | durable, node-local | Better covered than it first looks: the format version lives one level up, in the manifest's per-table metadata, not in the footer bytes themselves. The block index has its own separate magic+version too: `INDEX_MAGIC = b"SSIX"`, `INDEX_VERSION: u8 = 1`. All three (footer magic, manifest-recorded table format, index magic+version) currently describe a single supported format — there is no decoder path for `format != FORMAT_CURRENT` yet, so the tag would need a real N-1 branch added before Phase 1 could rely on it. |
| LSM manifest | `crates/animus-storage/src/lsm.rs` (`encode_manifest`/`decode_manifest`) | `MANIFEST_MAGIC = b"CMF1"`, `MANIFEST_VERSION: u8 = 1` | durable, node-local | The best-covered format in `animus-storage`: magic + version + a documented legacy fallback (`decode_manifest` still reads a pre-binary-codec JSON manifest when the magic is absent). This is the pattern the rest of the inventory should converge on. |
| Control-plane Raft WAL (`RaftCore` log entries + membership records) | `crates/animus-control/src/persist.rs` | `CONTROL_WAL`: `CWL1` + version **2** (`format::encode_line`); v1 retained as `persist::dispatch::legacy::v1` (fixture `control-wal/v1.bin`; v2 adds sync-marker lines, fixture `control-wal/v2.bin`, issue #1132) | durable, node-local | Two generations of this format have already existed with no version discriminator between them; a reader distinguishes them structurally (parse success), not by a tag. Works only because both are JSON and one is a strict superset shape. |
| Control-plane snapshot / `InstallSnapshot` payload | `crates/animus-control/src/raft.rs` (`RaftCore::snapshot`, the `S` state-machine type) | `CONTROL_SNAPSHOT`: `CSN1` + version 1 (`persist.rs`), wrapping `S`'s own serde shape | durable, node-local (persisted) **and** transient wire (`InstallSnapshot` RPC) | Same gap as the WAL: whatever `Metadata`'s own serde shape is *is* the wire format, with no independent version byte wrapping it. |
| `Metadata` + its system-keyspace mirror (ADR 0038) | `crates/animus-control/src/meta.rs`, `syskv.rs` | `METADATA_VERSION: u32 = 1` (`meta.rs`, serialized as `"v"`); the system-keyspace mirror carries `SYSKV_MIRROR_VERSION: u32 = 1` (`mirror.rs`). Individual fields still use ADR 0035's `#[serde(default)]` pattern | durable, node-local (mirror) **and** replicated over the control Raft log | The field-by-field-default discipline is real and documented (ADR 0035 §"Rolling upgrade") but it is a convention, not a checked invariant — nothing fails loudly if a new field is added without the default, or if a field's *meaning* changes without renaming it. |
| CP-data Raft command codec (`KvCommand` etc.) | `crates/animus-cp-data/src/codec.rs` | `MAGIC = 0xCB`, `const VERSION: u8 = 1` | durable, node-local (in the WAL/SharedWal) and transient wire (Raft replication) | The single best-versioned format in the whole system — a version byte is written by every encode and checked by every decode, loud `Err` on mismatch (no cross-version decoding attempted). The `VERSION` bump cadence (31 already) shows how often this layer has changed; each bump is currently an unconditional breaking change, by design. |
| Segment codec (streams, ADR 0042/0043) | `crates/animus-cp-data/src/segment.rs` | `MAGIC = b"SEGF"`, `pub const VERSION: u8 = 1` | durable, node-local (per-tablet change-log segments) and consumed by backup/PITR/export as a data source | Same magic+version+loud-error discipline as the RaftKV codec, explicitly modeled on it (module doc: "mirroring `codec.rs`'s own ... discipline"). |
| `SharedWal` record framing (ADR 0028, C-05) | `crates/animus-control/src/persist.rs` (`SHARED_WAL_TAG`, `encode_tagged_record`) | `SHARED_WAL_TAG`: `SWL1` + version **2** (sync markers, #1132; v1 kept as `dispatch::legacy_shared::v1`, fixtures `shared-wal/v1.bin`+`v2.bin`) on the outer `Line{tablet, record}` envelope; the inner `record` bytes are the already-versioned `codec.rs` payload | durable, node-local | Two-layer format: inner payload versioned, outer multi-tenant tagging envelope not. |
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
| Stored-item codec (base-row value; ADR 0054 step 1) | `crates/animus-item/src/stored.rs` (`encode_stored_item`/`decode_stored_item`/`encode_tombstone`/`stored_item_version`) | **untagged JSON, frozen serde shape** (2026-09-30 decision, see "Row-value formats: freeze, don't tag"): `{"item": {..}}` / the bare JSON string `"tombstone"` over `AttributeValue`'s derived serde form (externally tagged, e.g. `{"S":"x"}`, `"Null"`, `{"B":[0,255]}`); fixture `crates/animus-item/tests/fixtures/formats/stored-item/v1.json`; v1 is identified by a first non-whitespace byte of `{` (live item) or `\"` (tombstone) | **durable, outlives the cluster** (every base row, and therefore every backup, PITR segment and export carrying one) | Opaque inside `raftkv-wal`/`segment`/`backup-data` payloads until the P1-A fixture. `AttributeValue`/`Item` are part of this format: changing their serde shape is a format change. |
| Change record (`ChangeRecord`, change-log value; ADR 0041/0049) | `crates/animus-item/src/index.rs` (`ChangeRecord::encode`/`decode`/`version_of`) | **untagged JSON, frozen serde shape** (same decision): `{"base_sk":[..],"old_image":..,"new_image":..,"seeded":..,"marker":..,"staged":..,"ttl_expired":..}`; additions are `#[serde(default)]` only; fixture `crates/animus-item/tests/fixtures/formats/change-record/v1.json`; v1 identified by a leading `{` | **durable, outlives the cluster** (PITR segments and backups carry change records; Streams reads them) | A new `#[serde(default)]` must mean "what the old writer meant" (review-enforced; the fixture test also pins a pre-flag record). |
| Txn value envelope (`txn-envelope`, every base-row value; ADR 0018 §2) | `crates/animus-cp-data/src/txn.rs` (`encode_committed`/`encode_intent`/`decode_envelope`, `legacy::v1`) | the leading tag byte is the version: `0` committed (every version), `1` v1 intent (retired; decoded by `txn::legacy::v1` to `IntentPrior::Unknown`), `2` v2 intent (**current**, 2026-10-04: the v1 body plus a trailing `prior`, the committed value the intent shadows); fixtures `crates/animus-cp-data/tests/fixtures/formats/txn-envelope/v1.bin` + `v2.bin` (`src/format_fixture_tests.rs`) | durable, node-local (engine row values; also inside `raftkv-image` snapshot images). Backups/PITR/export carry only resolved committed values | Added to the inventory with its first bump (ADR 0018's 2026-10-04 amendment: aborted transactions lost acked writes because the abort read the prior value out of MVCC history). Engine-resident, so no carrier transcode re-encodes it yet: the v1 encoder is `cfg(test)` only, and the upgrade harness lists the format in `EMBEDDED` (carrier `lsm-sstable`, v2). Purely additive: v1 bytes decode unchanged. |
| `numkey` (DynamoDB `N` key encoding, ADR 0063) — **pinned vectors, not a tagged format** | `crates/animus-item/src/numkey.rs` | **none by design**; pinned by fixture `crates/animus-item/tests/fixtures/formats/numkey/v1.json` (input → `encode`/`encode_checked`/`decode`, edge cases from the unit tests: zero forms, ordering regressions, range extremes, 38-digit cap, malformed text), checked by `tests/numkey_vectors.rs` | durable, outlives the cluster (inside every stored `N` key) | The fixture is the compatibility pin; a vector change is a breaking key-space change needing an ADR amendment. See the open question on the hash-ring/key-encoding layer. |
| `AttributeValue::key_bytes` — **pinned vectors, not a tagged format** | `crates/animus-item/src/lib.rs` (`pub(crate)`; test in `src/key_bytes_vectors.rs`) | **none by design**; fixture `crates/animus-item/tests/fixtures/formats/key-bytes/v1.json` (every `AttributeValue` variant → key bytes, incl. the malformed-`N` raw-text fallback and the empty encoding of non-key types) | durable, outlives the cluster | As above; the fixture's `value` side also pins `AttributeValue`'s serde shape. |
| Hash-ring token + key `escape` — **pinned vectors, not a tagged format** | `crates/animus-tablet/src/lib.rs` (`partition_token`/`murmur3_x64_128`, `escape`) | **none by design**; fixtures `crates/animus-tablet/tests/fixtures/formats/partition-token/v1.json` and `.../escape/v1.json` (every murmur3 tail length and block boundary, embedded `0x00`s), checked by `tests/format_fixtures.rs`; the canonical-reference unit test stays | durable, outlives the cluster | Refines the "Hash-ring token / key encoding" row above: the pin now lives in checked-in fixtures covered by the append-only guard, not only in in-source vectors. Whether the layer ever gets a version tag remains the open question below. |

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
directly. *(Superseded by P1-C, 2026-09-30: the mirror entity values and the
version row now have golden fixtures, `mirror-entities` and `mirror-version`;
see "P1-C as-built" under Phase 1 workstreams.)* Golden fixture: `tests/fixtures/formats/metadata/v1.json` (a
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
| `lsm-wal` | `animus-storage` | `src/lsm.rs`, `wal_format_fixture_tests::decodes_every_checked_in_fixture`; round trip present; v1-reframe test | **Yes (#1142)** — per-version arm (`1 \| 2`, same records) | `decode_wal` exact `match` on the header version: `1 => legacy::v1::decode`, `2 => decode_wal_v2` | v1 and v2 share one expected value (markers carry no record data) |
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
| `animuscluster-spec` | `animus-operator` | `tests/format_fixtures.rs`; lossless round trip present | **Yes (P1-C)** — version from the file name, per-version arm, panics on an unrecognised version | `crd::decode_cluster` dispatches on `spec.schemaVersion` (`1` => current type until v2 adds `legacy::v1`); `validate_spec` rejects `0`/future | None |

Outside the fixture directories, three more durable groups matter:

- **Hash-ring token / key encoding** (`animus-tablet` murmur3, `animus-item`
  `numkey` and `key_bytes`): no tag by design (a key-space convention, see
  the inventory). It *is* pinned, but by in-source reference-vector and
  differential tests, not by `tests/fixtures/formats/`, so
  `check-format-fixtures.sh` does not cover it.
- **Row values that were never in the inventory**: the stored-item codec
  (`animus-item/src/stored.rs`, `{"item": ..}` / `"tombstone"` JSON in
  every base-row value; this audit originally repeated the module doc's
  wrong `{"tombstone": true}`, corrected by P1-A's fixture), `ChangeRecord` (`animus-item/src/index.rs`,
  serde JSON with `#[serde(default)]` additions), and the per-entity JSON
  values of the `Metadata` system-keyspace mirror (`syskv.rs` /
  `mirror.rs`). They are untagged, and no fixture pins their exact shape
  (they appear only opaquely inside `raftkv-wal`, `segment`, `backup-data`
  and the `control-snapshot` fixture, whose values are placeholder JSON).
  Two of them are read back out of backups and PITR segments, so they are
  the real contents of the objects that outlive a cluster. This is the most
  consequential audit finding.
  *(Partly superseded by P1-C, 2026-09-30: the `Metadata` mirror entity
  values are now pinned by the 18 `mirror-entities` fixtures and the version
  row by `mirror-version`; see "P1-C as-built" under Phase 1 workstreams.
  The stored-item codec and `ChangeRecord` remain P1-A's.)*
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
| **P1-D** | `animus-test`, `animusd`, plus `legacy-encoders` feature plumbing in the crates above | The upgrade-restart harness: tier 0 fixture-seeded restarts, tier 1 `upgrade_restart_corpus`, negative control, `ANIMUS_UPGRADE_RESTART_SEEDS`, `corpus-deep.yml`; then tier 2 (`sim_cluster_upgrade_corpus`, `SimCluster` `LsmEngine` factory; landed 2026-10-01, per-format plug-ins still to come as P1-A/P1-C land). Adds the per-format transcode table skeleton (identity today) | A, B, C for the per-format plug-ins and the `legacy-encoders` feature convention; tier 0/1 skeleton may start earlier against the convention in this ADR | The format crates' non-test source apart from the feature gate; `sim_cluster.rs` beyond the factory option |

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

**P1-C as-built (2026-09-30).** Landed as a four-PR stack on the baseline
`9a9f972f`; every change is additive, no existing fixture is edited, no
format changes (v1 stays v1).

- **Layer 1, control dispatch.** `persist::dispatch` holds one body decoder per
  format, `wal_record` (`control-wal`), `shared_wal_line` (`shared-wal`) and
  `snapshot_body` (`control-snapshot`), each `match version { 1 => .., found =>
  format::unsupported_version(tag, found) }`. The `_version` discards in
  `persist.rs`, `node.rs` and `raft.rs` are gone. `format::unsupported_version`
  is a new helper (an addition; the `wrap`/`unwrap`/`decode_lines` signatures are
  unchanged).
- **Layer 2, `Metadata` and `ClusterConfig`.** Each has a per-version entry
  point (`Metadata::from_json`, `ClusterConfig::from_json` match on the peeked
  `"v"`) and a per-version fixture test. **Decision: `Metadata`'s `"v"` serde
  default is kept**, because the frozen `control-wal`, `shared-wal` and
  `control-snapshot` v1 fixtures embed a `Metadata` with no `"v"` and may never
  be edited; standalone `Metadata::from_json` stays strict (missing is
  pre-baseline, non-1 unsupported). `ClusterConfig`'s `"v"` stays required.
  Lesson: `docs/lessons/code-patterns/2026-09-30-a-serde-default-on-the-version-field-is-decided-by-frozen-fixtures.md`.
- **Layer 3, mirror.** 18 `mirror-entities` fixtures (one per `EntityKind`) plus
  one `mirror-version` fixture, each with a per-version expected value.
  `rebuild_metadata_from_engine` is now a real dispatch on the version row
  (`1 => rebuild_metadata_v1`, else a named `UnsupportedFormatVersion`; empty
  engine and a missing row behave as before), refusing `0`, `2` and `u64::MAX`
  by name. It had been a range gate that read the version and ignored it.
- **Layer 4, operator.** A per-version `animuscluster-spec` fixture test
  (version from the file name, panics on an unrecognised one) and
  `crd::decode_cluster`, which dispatches on `spec.schemaVersion`.

**Waves.** Wave 1: P1-A, P1-B, P1-C, fully concurrent (disjoint crates; the
only shared surface is `animus_control::format`, which nobody changes).
Wave 2: P1-D tier 0/1 (starts once one of A/B/C has landed its
`legacy-encoders` convention, or earlier against this ADR's text). Wave 3:
P1-D tier 2. Phase 1 is done when all four have merged.

**P1-D as-built, tiers 0 and 1 (2026-09-30; tier 2 pending; every format still
v1).** Tier 0 (#1130) seeds disks from the checked-in fixtures and restarts
the real readers; tier 1 is `animus-test/tests/it/upgrade_restart_corpus.rs`
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
PR to come). Tier 2 (`sim_cluster_upgrade_corpus`) landed afterwards, see
the next note. See `animus-test/CLAUDE.md`.

**P1-D as-built, tier 2 (2026-10-01; every format still v1).**
`animusd/src/sim_cluster_upgrade_corpus.rs` restarts a whole `SimCluster`
(roles `[Both, Both, Both, Data]`, RF 3) over a new `LsmEngine<SimEnv>` backend
option (`SimEngineBackend::Lsm`, `SimCluster::new_with_lsm_engines`; the default
stays `MemoryEngine`, every existing constructor unchanged): every node is
stopped (after a `Simulator::crash`, optionally with torn tail and
corrupt-on-crash armed together, on the crash cells), `transcode_disk` runs on
every node's disk (identity today; seeded mid-transcode stop and
mixed-version fraction), and every node restarts through `SimCluster::restart`
with **strict** engine opens (the sim tablet factory panics rather than return
`Err`, because the reconciler answers an `Err` open with destroy-and-reopen).
In the loop: control `Metadata` and its system-keyspace LSM mirror, the
data-only node's mirror, tablet hosting and rebalancing, DynamoDB streams
(enabled, written, optionally sealed, then read back through
`GetRecords` over sealed and open shards), a backup catalog row, and the
DynamoDB wire clients. **Not drivable under `SimCluster`, so not faked:** a
backup *captured* before the upgrade and *restored* after it
(`backup_capture_loop`/`backup_restore_loop` take the concrete
`ClientCtx<ProdEnv, ..>` and `SimCluster` spawns only the janitor) and stream
cursor advancement (no `change_consumer_loop` runs under `SimCluster`); the
backup row is a catalog row only, and the sealed segments live in a shared
`SimSegmentStore` outside any node's disk. Oracle: `check_cycles`, a wire probe
that every acknowledged append reads back in order before any phase-2 write
(equality with a pre-stop snapshot on clean cells), the control apply frontier,
metadata/backup/hosting agreement across all four nodes, the stream check,
`check_durability`/`check_convergence`, non-vacuity. Knob
`ANIMUS_UPGRADE_RESTART_SEEDS` (shared with tier 1); `corpus-deep.yml` runs it
at K=50 (K=20 measured at about 150s wall in debug). **Negative controls:**
logs halved with engines wiped, and every disk wiped, are each caught as lost
acknowledged appends; a truncated LSM file fails the strict open. **Remaining
for P1-D:** per-format plug-ins arrive as P1-A and P1-C land (one
`transcode::TABLE` entry each; the cells grow with `supported_back()`); nothing
in tier 2 names a format version.

**P1-D as-built, step 4: per-format plug-ins and coverage (2026-10-01; every
format still v1).** Formats fall into two harness classes. **Whole-file**
(a file on a node's disk one format owns): `lsm-wal`, `lsm-manifest`,
`lsm-sstable`, `control-wal` (also `raftkv.wal*`), `shared-wal`,
`encryption-envelope`; each is a `transcode::TABLE` entry and a bump edits that
entry. **Embedded** (everything else with a fixture directory): listed in the new
`transcode::EMBEDDED` registry with a carrier, and a bump edits the *carrier's*
transcode so it re-encodes the embedded records through the legacy encoder.
Carriers: `control-wal` for `control-snapshot`, `metadata` and the `raftkv-wal`
payload; `lsm-sstable` for the engine-resident `mirror-version`,
`mirror-entities`, `cp-engine-layout`, `stored-item`, `change-record`,
`key-bytes`, `numkey`, `escape`, `partition-token`; **off-disk** (never touched
by the disk pass; covered by their own per-version fixture tests) for `segment`,
`backup-manifest`, `backup-data`, `raftkv-wire`, `raftkv-image`,
`network-handshake`, `client-handshake`, `cluster-config`, `animuscluster-spec`.
Legacy encoders the harness calls must be `pub`, gated
`#[cfg(any(test, feature = "legacy-encoders"))]`, not `cfg(test)`-private (the
latter is invisible to `animus-test`). Tier 0 now iterates `TABLE` for the
newest-fixture check (which includes `encryption-envelope`, previously skipped),
checks `EMBEDDED` versions against their newest fixture, and has a completeness
test: every crate's `tests/fixtures/formats/<dir>` must be named in `TABLE` or
`EMBEDDED`, every registration must have a directory, and every carrier must be
a `TABLE` entry; a negative control feeds the pure check a synthetic
unregistered name and asserts it is flagged.

**Coordination with #1140/#1141.** Those PRs bump `control-wal` (CWL) and
`shared-wal` (SWL) to v2. Whichever of them lands second against this stack
must register v2 in `TABLE`: `current_version` 2, `versions` `[v1, v2]` both
`CapabilityMask::ALL`, and a `transcode` that goes through a `pub`,
`legacy-encoders`-gated, type-erased v2 -> v1 line reframer in `animus-control`
(it drops the sync-marker lines and re-frames each payload under the v1 tag).
Until that is done the tier-0 `transcode_table_matches_the_checked_in_fixtures`
test is red, by design: it sees a v2 fixture with no `VersionSpec`. This stack
deliberately does **not** pre-register v2, which would be red against `main`.

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

**Documents that claimed otherwise** (`website/architecture.html`,
`website/docs.html`, `website/how-it-works.html`, `website/install.html`,
`website/index.html`, `docs/roadmap.md` C-16, ADR 0060's "Upgrades") were
rewritten by the Phase 1 close-out PR (2026-10-03) to the contract above.

**Phase status after this amendment (historical, as of 2026-09-30):** Phase 0
done; Phase 1 in progress; Phases 2 and 3 planned. Superseded by the next
section.

### Phase 1 as built (2026-10-03)

Phase 1 is **done**: P1-A (#1124-#1127), P1-B (#1117, #1118), P1-C
(#1119-#1122) and P1-D (#1130, #1137, #1139, #1143) have all merged, and
#1144 moved the `animus-test` integration tests under `tests/it/`. The
per-workstream as-built notes are above ("P1-A as built", "P1-B step 1/2
as-built", "P1-C as-built", "P1-D as-built" tiers 0/1/2 and step 4); this
section only states the resulting contract.

**Enforced now:**

- **Per-version fixture tests and `legacy` seams** for every durable format
  in the inventory plus the row-value formats P1-A/P1-C added: the decoder
  matches the version exactly, a fixture with no per-version expected value
  panics, and an unsupported version is a named error (a downgrade is
  refused by name, not misread).
- **The 8-step format-change checklist** ("Phase 1 design" above) is the
  review gate for any format change; it is pointed to from root
  `CLAUDE.md`.
- **`scripts/check-format-fixtures.sh`** (per-push CI): an existing fixture
  is never edited or deleted.
- **The upgrade-restart harness**, which restarts on state transcoded to an
  older version with the test-only legacy encoders and verifies every
  acknowledged write:
  - *Tier 0* (`animus-test` `tests/it/upgrade_restart_tier0.rs`): seeds
    disks from the checked-in fixtures and opens the real readers. Runs in
    `cargo test --workspace`, i.e. per-push. Includes #1143's
    registration/completeness check: every crate's
    `tests/fixtures/formats/<dir>` must be named in `transcode::TABLE` or
    `transcode::EMBEDDED`, every registration must have a directory, every
    carrier must be a `TABLE` entry, and `current_version` must equal the
    newest checked-in fixture. A new format therefore cannot land without
    being registered with the harness.
  - *Tier 1* (`animus-test` `tests/it/upgrade_restart_corpus.rs`, 21
    cells): per-push at the default `ANIMUS_UPGRADE_RESTART_SEEDS=1`,
    nightly at K=100 (`corpus-deep.yml`, step `upgrade_restart`).
  - *Tier 2* (`animusd` `sim_cluster_upgrade_corpus`, whole `SimCluster`
    over `LsmEngine<SimEnv>` plus the DynamoDB wire, 3 cells): per-push at
    K=1 (it is a `--lib` test of `animusd`), nightly at K=50
    (`corpus-deep.yml`, step `upgrade_restart_cluster`).
  - Negative controls (a deliberately lossy transcode must fail the
    corpus) are part of both tiers; see the P1-D as-built notes.

**What Phase 1 does not give:**

- **No wire compatibility.** Internal `Network` messages, forwarded client
  requests and handshakes are only version-tagged (Phase 0 workstream D),
  not negotiated; two builds with different wire formats cannot run in one
  cluster. That is Phase 2.
- **No rolling upgrades and no mixed-version running cluster.** The only
  supported upgrade is a **whole-cluster stop, upgrade, restart**. Phase 3
  (including operator `spec.image` orchestration) comes after Phase 2.
- No stability guarantee for the hash-ring/key encoding beyond "pinned by
  the key-vector fixtures" (open question below, unchanged).

**State of the formats on main (2026-10-03, after #1140/#1141 merged; `lsm-wal`
bumped to v2 by #1142, see the 2026-10-03 amendment):**
the first real format bumps have landed — `control-wal` (CWL v2) and
`shared-wal` (SWL v2), both adding WAL sync markers, with new
`control-wal/v2.bin`/`shared-wal/v2.bin` fixtures, and `raftkv-wal` v2
(`raftkv-wal/v2.bin`, an `EMBEDDED` format in the control-wal carrier).
Their `transcode::TABLE` entries have `current_version: 2` and transcode to
v1 for real (`animus_control::format::reframe_to_v1`, `legacy-encoders`-
gated: drop marker lines, re-frame each record under the v1 tag), so the
older-version path is now exercised by a real bump, not only the identity.
Every other format is still v1 and transcodes as the identity. (Before
#1140/#1141 merged, this section correctly stated that no v2 existed.)

**Open questions resolved by Phase 1:** the support window (every
post-baseline version, forever; decided 2026-09-30) and the Phase 1
ordering (backups/PITR/export first) are resolved above. Still open: the
Raft-codec freeze-or-keep-translating question and the hash-ring/key-
encoding question.

### Amendment 2026-10-01: LSM WAL segment header is synced before any record (P1-B / P1-D follow-up)

The P1-D tier-1 upgrade-restart corpus (strict `LsmEngine` open straight
after `Simulator::crash` with `torn_tail_on_crash` + `corrupt_on_crash`)
found that a crash during WAL segment creation could leave a node that
cannot restart: the 5-byte header (`LWL1` + version) shared one `sync` with
the segment's first records, so a torn-and-bit-flipped un-synced header
decoded as `UnsupportedFormatVersion { found: 254 }` or `PreBaselineFormat`.
**No encoding or fixture changes, no version bump.** Two coupled changes:
(1) the write side appends and `sync`s the header on its own before any
record is appended (one extra `fsync` per segment creation);
(2) `decode_wal` now treats any file shorter than the header, whatever its
bytes, as an empty torn tail (previously only a true prefix of `WAL_MAGIC`).
Soundness: after (1) every file longer than the header has a durable header,
so a bad header there is real corruption and stays loud (the version-dispatch
seam is untouched for >= 5 bytes); a shorter file never had a synced header
and holds no acked data. Regression: `animus-storage/tests/lsm_crash.rs`
`crash_during_segment_header_creation`.

## Amendment 2026-10-03 — `LWL1` v2: sync markers in the LSM WAL (issue #1142)

The LSM WAL decoder used the rule the #1132 amendment below declined to copy:
a frame that fails to parse is a torn tail only if no valid frame follows it.
That is sound only if at most one frame can be un-synced, but `GroupCommit`
coalesces several writers' frames into one append + one fsync, and
`corrupt_on_crash` flips a byte anywhere in the kept prefix of that un-synced
region (never in already-synced bytes). A correct writer plus a crash therefore
left "bad frame, then valid frame" and recovery was wrongly refused: 76 of 300
seeds of the `lsm_wal_sync_markers` probe (1..=7 coalesced writers). The
existing `lsm_crash`/`lsm_disk_faults` corpora never exposed it because they
buffer exactly one un-synced frame.

- **Format v2** (checklist step 1): `WAL_VERSION = 2` (file-level header, as
  before). A *sync marker* frame is `len=9 | crc32 | tag 5 | offset u64` and
  claims "every byte before `offset` is fsynced", where `offset` is the marker's
  own start (the file offset, header included). Markers are **piggybacked on
  the next batch's own `append`**, mirroring #1132: prepended only when the
  previous batch's append+sync on that segment both returned `Ok` (flag cleared
  when a leader claims a batch, set again only on success; also cleared on
  rotation and at open), with `offset` read from `env.size` at flush time. No
  separate append or fsync (one extra fsync per round would double commit
  latency); the marker is itself durable only with the next sync, so the newest
  batch has no durable marker until the next one — that only shrinks the
  provable region. `fsync_lie`: a lied-to sync leaves its bytes buffered; a
  later marker may then vouch for bytes a crash tore, and recovery fails loudly
  — the same, correct outcome as #1132 (acked bytes were lost by the disk).
- **Decoder** (`decode_wal_v2`): a frame that fails to parse and starts before
  a valid marker located *at the offset it claims* is a hard error (durable
  data corrupted); with no such marker after it, it is a torn tail even if
  valid frames follow. A marker met in sequence at the wrong offset is a hard
  error; one merely found by the resync scan at the wrong offset is not a
  boundary. (A forged marker inside a record value can only cause a spurious
  refusal, never an accepted corruption.)
- **Legacy** (step 2): the v1 decoder moved unchanged to `legacy::v1::decode`
  (commit 1); a test-only `legacy::v1::encode_file` and the
  `legacy-encoders`-gated `reframe_wal_to_v1` build v1 images. A recovered v1
  *active* segment is appended to **without** markers (its header still says
  v1, so a v1 reader keeps parsing it) until it rotates; new segments are v2.
- **Fixtures** (steps 3-4): `lsm-wal/v2.bin` (records interleaved with
  markers, expected value shared with v1: the same `representative_records()`),
  `v1.bin` untouched; a reframe of the v2 image is byte-identical to `v1.bin`.
- **Harness** (step 7): `lsm-wal` `TABLE` entry `current_version: 2`,
  `[v1, v2]`, transcode through `animus_storage::reframe_wal_to_v1`.
- **Residual**: as #1132 — corruption confined to the one newest, not yet
  marker-vouched round is indistinguishable from a torn tail.

## Amendment 2026-10-01 — `CWL1`/`SWL1` v2: sync markers (issue #1132)

A decoder-behaviour change that needed a format bump. Both line-framed WALs
(`CWL1`, the control-plane Raft WAL, and `SWL1`, the `SharedWal`; the
per-group `raftkv.wal` shares `CWL1`) treated a CRC failure on *any* line as a
crash-torn tail, so rot in an early line silently dropped acked term/vote/log
history. The LSM WAL's resync proof ("a valid frame follows, so it is not a
torn tail") cannot be copied: a persist round appends N lines then syncs once,
and `animus-sim`'s `corrupt_on_crash` flips a byte anywhere in the kept part
of that un-synced region, so a correct writer plus a crash produces "bad line,
then valid line" (72 of 300 seeds measured). The reader needs a durable sync
boundary.

- **Format v2** (checklist step 1): after every `fsync` that returns `Ok`, the
  writer records that the file is durable and **piggybacks** the marker on its
  *next* persist round: it is prepended to that round's single `append` (never
  a separate append under the WAL lock, which a slow disk charges a full extra
  latency per round), with `N` read from the file's live length at that moment.
  The claim is true when written (the earlier fsync completed first); a
  compaction rewrite or any failed round clears the pending state. Residual:
  the latest round has no durable marker until the next round syncs. The
  marker is a line `!sync:<N>` (an ordinary CRC-checked line carrying the
  tag's version) where `N` is the file length at that moment, which is the
  marker's own start offset. Never written before the sync it vouches for (a
  pre-sync marker could survive a kept-prefix tear next to a flipped byte in
  the same un-synced round). Record payloads are unchanged. Harness: the
  `control-wal` transcode reframes v2 to v1 through `format::reframe_to_v1`
  (`legacy-encoders`-gated), and the `shared-wal` transcode does the same for `SWL1`, and the `raftkv-wal` EMBEDDED row is at v2.
- **Decoder** (`format::decode_lines_extent`, documented there): a bad line
  that starts before the greatest valid marker is
  `FormatError::MidFileCorruption { offset, durable_to }`; at or after it, a
  torn tail as before. The decoder keeps scanning past the first bad line for
  markers. A valid marker whose claimed offset is not its own start is
  `FormatError::BadSyncMarker`. A marker-less file (every v1 file) keeps the
  lenient rule. The forged-future-version error is unchanged.
- **Legacy** (step 2): the v1 arm of `persist::dispatch` is
  `legacy::v1::wal_record`, frozen; a test-only v1 encoder anchors the v1
  fixture (step 7; `cfg(test)`, no `legacy-encoders` feature exists on this
  base). **Not done here:** registering the v2 pair in the upgrade-restart
  harness's transcode table (`animus-test/src/upgrade/`, owned by another
  workstream).
- **Fixtures** (steps 3-4): `control-wal/v2.bin`, `raftkv-wal/v2.bin` (and
  `shared-wal/v2.bin` on the SWL branch), each with a per-version expected
  value; v1 files untouched.
- **v1 reopen**: version is per line, so a v1 file reopened by a v2 build is
  appended to with v2 lines and markers (no rewrite, no mixed-file hazard: the
  marker offset is absolute). A marker then also protects the v1 prefix.
- **Writers also repair on open**: a torn tail is cut back (`Disk::replace`)
  at recovery, otherwise the next appends would sit after garbage and the
  next recovery would correctly refuse the file. This also closes a latent
  v1-era bug (acked records appended after a torn tail were lost at the next
  recovery).
- **Residual**: the latest round has no durable marker until the next sync, so
  corruption of that one round alone is indistinguishable from a torn tail.
  A disk that lied about `fsync` and lost acked bytes before a surviving
  marker now fails loudly.

### Row-value formats: freeze, don't tag (2026-09-30 decision, P1-A)

The audit above found two durable row values that were in no inventory and
pinned by no fixture: the stored-item codec and `ChangeRecord`. Decision
(adopting the Phase 1 proposal):

- **Freeze the serde shape; do not add a tag now.** Both are untagged JSON
  written by `serde_json`. Their shape, *including* `AttributeValue`/`Item`
  (externally tagged enum: `{"S":".."}`, `{"Bool":true}`, `"Null"`,
  `{"B":[..]}` as a number array, sets as arrays, `M` as an object), is a
  frozen format. Changes are **additive-only** (a new `#[serde(default)]`
  field or a new variant) and each ships with a new golden fixture; anything
  else is a new *tagged* version via the normal checklist. A `legacy` module
  that embeds `AttributeValue`/`Item` (decoder pattern, point 4) can rely on
  this. The types carry a doc comment saying so.
- **v1 is sniffable.** v1 is untagged JSON. A stored item is `{"item":{..}}`
  (first non-whitespace byte `{`) or the tombstone, which is the bare JSON
  *string* `"tombstone"` (serde serializes a unit variant as a string, not as
  the `{"tombstone":true}` object the module doc used to claim; first byte
  `"`). A `ChangeRecord` is always an object (`{`). The writers emit no
  leading whitespace; it is skipped only to accept what the parser always
  accepted. `animus_item::stored::stored_item_version` (`{` or `"`) and
  `ChangeRecord::version_of` (`{`) return `Some(1)` for that form and `None`
  otherwise, and the decoders dispatch through them. A later tagged v2 must
  start with a byte that is neither `{` nor `"` nor whitespace, so the sniff
  stays unambiguous with no migration of existing rows.
- **Fixtures** (`crates/animus-item/tests/fixtures/formats/{stored-item,
  change-record}/v1.json`) are produced by the current writer (no format
  change), with `#[ignore]`d no-overwrite generators, a per-version expected
  value that panics on a fixture with no expectation, and round-trip tests.
  The tombstone (`"tombstone"`) is pinned by an inline byte assertion.

### P1-A as built (2026-09-30)

One four-PR stack, no format change (every tag is still v1, no existing
fixture edited):

1. **Backup/PITR dispatch seam** (`animus-cp-data`): `backup-data`
   (`backup::decode_data_chunk`), `backup-manifest`
   (`backup::decode_manifest_object`) and `segment` (`segment::decode`) keep
   the header gate's named errors for `0` and `> CURRENT`, then
   `match version { 1 => decode_<fmt>_v1(body), found =>
   Err(UnsupportedFormatVersion { .. }) }`. The segment fixture test asserts
   a fixture exists for `segment::VERSION` instead of `VERSION == 1`.
2. **raftkv/layout dispatch seam** (`animus-cp-data`): the same for
   `raftkv-wire` (`codec::decode_wire`), `raftkv-image`
   (`codec::decode_image`; `codec::check_header` now returns the version it
   checked) and `cp-engine-layout` (`layout::decode_layout_value`, whose
   version *is* the epoch). `raftkv-wal` has no cp-data decoder to dispatch:
   it is read by `animus-control`'s `PersistedState::decode` through
   `format::decode_lines` (P1-C's); only its fixture test lives here, and it
   was hardened.
3. **Row values** (`animus-item`): `stored-item` and `change-record`
   fixtures, the freeze decision above, and the v1 sniffs.
4. **Key vectors** (`animus-item`, `animus-tablet`): `numkey`, `key-bytes`,
   `partition-token` (murmur3) and `escape` pinned as fixtures.

**The `legacy` convention (what P1-B/C/D copy).** Each format file has a
private `mod legacy {}` directly after its public decoder, documented as the
home of `legacy::vN` (frozen decoder, frozen `VNFoo` shape, `From<VNFoo>`
translation), empty while the format is at v1. The current body decoder is
a private `decode_<fmt>_v1(body)`; bumping to v2 moves it into
`legacy::v1` and adds a `2 =>` arm. Every fixture test's per-version
expectation is a `match` whose fallback arm panics with "fixture v{N} has
no expected value — add one (ADR 0073 checklist step 4)". The
`legacy-encoders` feature is not added here: nothing is retired yet, so
there is no encoder to gate; P1-D introduces the feature with its first
consumer.

## Amendment 2026-10-03 — Phase 2 design: cluster version and feature gates

Design only; no code. Status: **in review**. Phase 2 makes a mixed-version
cluster *safe on the wire and in replicated state*, which is what turns
"restart every node by hand, one at a time" from unsupported into supported.
Phase 3 (orchestration) stays later. Claims below were checked against `main`
at `ac57d56a`.

**Maintainer rule (2026-10-03): an upgrade never needs a restart of the whole
cluster. That includes the very first one, from today's Phase 1 binaries
to the Phase 2 release ("B2"), and every future release.** Section 2 is the
design that satisfies it; the rest is built on it.

### What the code does today

| Fact | Evidence |
|---|---|
| Control Raft messages are `serde_json` of `RaftMsg<C>`; an undecodable message is **logged and dropped** | `animus-control/src/node.rs` (`serde_json::from_slice::<RaftMsg>`, `undecodable raft message dropped`) |
| CP-data Raft messages are the binary `codec.rs` (`0xCB` + `VERSION`, one version for wire, image and WAL payloads); a bad version/tag/trailing byte is a `FormatError` and the receiver **drops the message** | `animus-cp-data/src/codec.rs`; `lib.rs` (`undecodable raftkv message dropped`) |
| `ClientRequest`/`ClientResponse` are length-prefixed `serde_json`; an undecodable frame is an `InvalidData` error that **tears down the connection** | `animus-node/src/codec.rs`; `animusd/src/lib.rs` `read_frame` |
| **No `deny_unknown_fields` anywhere in the workspace.** An unknown *field* is ignored by an older reader; an unknown enum *variant* fails the whole enclosing value. One new `MetaCommand` inside an `AppendEntries` fails the whole message, so the follower never gets that batch: replication wedges silently | grep; serde defaults |
| **A non-empty handshake `ext` is accepted and ignored today (verified).** `handshake::decode` (`animus-env/src/handshake.rs:235`) copies `ext` (<= `MAX_EXTENSION_LEN` = 1024) into `Preamble.extensions` and never inspects it; `handshake::check_peer` (`:272`) compares **only magic and version**; `prod::read_preamble` (`animus-env/src/prod.rs:1360`) reads `ext_len` extra bytes before returning; pinned by the unit test `round_trip_nonempty_extensions_is_carried_through_unrejected` (`handshake.rs:332`). `animusd`'s client/intra port uses the same functions | code |
| `Metadata` is `"v": 1` JSON; `Metadata::from_json` and the embedded `v` deserializer refuse any `v != 1` by name; `Member` has `labels`/`status`/`has_activated` | `animus-control/src/meta.rs` |
| The system-keyspace mirror **`expect`s** every entity decode, so a new entity kind or shape is a node-local panic on a reader that does not know it | `animus-control/src/mirror.rs` |
| Data-only nodes read `Metadata` through a mirror (`ControlHandle::Remote`, `WatchMetadata` -> `MetadataDelta { writes: Vec<KeyWrite> }`); their view lags the leader | ADR 0035; `animus-node/src/wire.rs` |
| Every member heartbeats to the control nodes (`heartbeat_loop_live`, spawned in both node assemblies, `animusd/src/lib.rs:6350`, `:8328`) | code |
| A tablet replica's state machine cannot read `Metadata` | `animus-cp-data` |
| CWL/SWL v2 and `raftkv-wal` v2 are written unconditionally (node-local) | #1140/#1141 |

### Decisions

| # | Decision | Status |
|---|---|---|
| 1 | Version model: a binary has `[min_supported, max_supported]` over one integer cluster version; `Metadata.cluster_version` is the active one (absent = 1) | design |
| 2 | **B2 is rolling-installable over live Phase 1 binaries**: no handshake version bump, nothing new on any replicated/shipped surface until a leader-local precondition proves every member is B2 | design (maintainer rule) |
| 3 | The handshake version is **never bumped incompatibly again**; handshake changes go in `ext` TLVs, unknown TLVs ignored | rule |
| 4 | Gates cover cross-node surface only; decoders accept every version; apply never branches on a gate | design |
| 5 | Finalize is **manual** (admin API/CLI), one version at a time; auto-finalize may come as a Phase 3 option | **DECIDED** |
| 6 | Down / removed-pending members **block** Finalize; remedy: bring the node back on the new binary or remove it (ADR 0032); no override | **DECIDED** |
| 7 | Floor policy: `min_supported = max_supported - 1`, raised only by an ADR amendment naming the stepping-stone release; durable readability stays forever | **DECIDED** |
| 8 | Rollback: **Option B**, no rollback once a node has run the new binary | **DECIDED** |
| 9 | Skew: N-1 and N only | design |

### 1. Version model

- **`ClusterVersion`** is a `u32`. **Version 1 is "everything in Phase 1 and B2"**:
  the surfaces that exist today, plus B2's handshake `ext` and the era
  machinery of section 2. Real gates start at 2.
- **A binary's range.** `max_supported` = the highest cluster version it can run
  at; `min_supported` = the lowest it can still *emit for* (`max - 1`). Both are
  `const`s in one module, `animus_control::version` (`ClusterVersion`,
  `VersionRange`, `Gate`, the registry). `animus-env` must not depend on it, so
  the handshake codec carries opaque `ext` bytes.
- **Handshake `ext` (NHS1 and CHS1), version stays 1.** A TLV list: tag 1 =
  `min:u32, max:u32` LE, tag 2 = build string (display only); unknown tags are
  ignored (rule 3). **Empty `ext` means a Phase 1 binary, range [1,1].** A B2
  binary always sends tag 1, even at [1,1]: *presence* is what distinguishes
  B2 from Phase 1. `check_peer` keeps magic+version equality and additionally
  refuses **disjoint** ranges, by name.
- **Observation.** The `Network` seam hands the control layer the `ext` of the
  connection each envelope arrived on (`Envelope` gains `peer_ext`, default
  empty; `ProdEnv` stamps it from the connection's preamble, `SimEnv` from the
  per-node override). A control node's receive loop records `from -> (range,
  observed_at)` leader-locally. Every member heartbeats to the control nodes, so
  the leader sees every live member without any new message. (P2-A confirms
  each role does, with a test.)
- **Replicated record.** Once the era has started (section 2): `Member.version_range:
  Option<VersionRange>` and `Member.build`, `Metadata.cluster_version: u32`.
  All are `#[serde(default, skip_serializing_if = ...)]` **additions inside
  `Metadata` `"v": 1`**: with default values they are not serialized, so era-0
  bytes are identical to Phase 1's. Why no `"v": 2`: a v2 tag would make every
  Phase 1 reader refuse the snapshot, and an additive field is exactly what the
  gate (not the tag) protects. New fixture `metadata/v1-era.json`, existing
  fixtures untouched (checklist steps 3-4); the checklist's "new version tag"
  step is replaced by "gated additive field" for this one change, and the
  per-version expected value records both shapes.
- **Safe target** = `min` over every member row of `version_range.max`, capped
  at the leader's `max_supported`; a row with `None` blocks. Published on the
  admin API `{active, safe_target, blockers}`; the leader does not act on it.
- **Startup rule (loud).** After reading `Metadata`, a node whose range does not
  contain `cluster_version` exits with a named error (`cluster version A is above
  this binary's max M (downgrade is not supported)` / `below this binary's min m
  (upgrade through release R first)`). Until a node has read `cluster_version`
  it uses the **floor** (`min_supported`) for every gate decision.

### 2. B2 is rolling-installable over a live Phase 1 cluster

**Principle.** While any Phase 1 binary can be in the cluster, B2 emits nothing a
Phase 1 binary cannot decode, and replicates nothing new. Only the era-start
step (below) changes that, and only after the leader has *proven* every member
is B2.

**What B2 holds back until the era starts (every Phase 1 surface, checked one by one).**

| Phase 1 surface a Phase 1 node reads | B2 behavior before the era |
|---|---|
| NHS1/CHS1 preamble | version 1; `ext` now non-empty, which Phase 1 `decode`/`check_peer`/`read_preamble` accept and ignore (verified above) |
| Control `RaftMsg` JSON (incl. `Heartbeat`, `AppendEntries` entries, `InstallSnapshot` chunks) | no new variant, no new field |
| `MetaCommand` in control log entries and in `ProposeSchema` | only existing variants; `ReportNodeVersion`/`FinalizeClusterVersion` are new variants and are era-only |
| `Metadata` JSON in `CSN1` snapshots, `InstallSnapshot`, `Status`/`JoinInfo` carriers | byte-identical to Phase 1 (`"v": 1`; new fields skipped at default) |
| `MetadataDelta`/`KeyWrite` mirror stream read by Phase 1 data-only nodes; mirror entity kinds | no new entity kind (the `cluster` entity is era-only); `Member` entity value unchanged |
| `ClientRequest`/`ClientResponse` | no new variant; the admin Finalize rides the existing `ProposeSchema` relay and exists only post-era; `JoinInfo` may carry an additive `#[serde(default)]` field, which Phase 1 ignores |
| `KvWire`, `KvCommand`, `raftkv-image`, `codec.rs` `VERSION` | unchanged; B2 adds nothing here |
| `control-wal`/`shared-wal`/`raftkv-wal`/LSM files | unchanged (v2 already shipped on `main`; node-local anyway) |
| Admin/dashboard HTTP, DynamoDB wire | admin gets new routes (additive); Dynamo unchanged |

A B2 build with no Phase 1 peer ever observed still behaves this way until the
era starts. A test pins it: B2 pre-era encodings equal the existing Phase 1
golden fixtures byte for byte.

**Chicken and egg.** `ReportNodeVersion` is a new `MetaCommand` a Phase 1 voter
cannot decode, so it cannot be how B2 learns versions before every voter is B2.
Bootstrap instead:

- `cluster_version` is implicitly 1 when absent, and the era is *off*;
- the control leader learns live ranges from handshake `ext` (passive, above);
- **Precondition P (leader-local):** every control voter and learner **and every
  row in `Metadata.members`** has been observed by *this* leader with a B2 range
  (tag 1 present) within the last `T` (default 3 heartbeat intervals), and the
  leader itself is B2. **It must be every member, not only the control voters:**
  data-only Phase 1 nodes read `Metadata` through `MetadataDelta`, whose entity
  kinds the era changes, so a Phase 1 data-only node would panic in the mirror
  `expect`. A member never observed (down, partitioned) counts as [1,1] and
  blocks P, consistent with decision 6;
- **Era start:** when P holds, the leader proposes `ReportNodeVersion` for each
  member (its first and only new-variant emission). The era is *on* once any
  member row has `version_range: Some` (`Metadata::versioning_active()`, read
  from replicated state, so followers and data nodes agree);
- **Era on:** B2 nodes (a) self-report `ReportNodeVersion` at every boot,
  (b) **refuse any peer whose handshake `ext` is empty** (a Phase 1 binary) at
  the handshake, with a `warn` naming `peer is a Phase 1 binary; cluster has
  versioning enabled`, a refusal metric, and connection close, (c) refuse
  `RegisterNode`/`UpsertMember`/`change_membership` for a peer with no observed
  range, (d) allow `FinalizeClusterVersion`. `Gate::Era` is the registry's one
  gate whose open condition is `versioning_active()` rather than `cluster_version`.
- **A Phase 1 binary joining or rejoining after the era** cannot read `ext`
  ranges and is not asked to: the **B2 side refuses it** (b)/(c). The Phase 1
  node sees only a closed connection; the loud error is on the B2 side. A late
  Phase 1 joiner racing P (registered between P and the first report) gets a row
  with `version_range: None`, is refused at every connection, never becomes
  `Active`, and blocks Finalize until removed.

**Ordered sequence, Phase 1 -> R (R's `max_supported` = 2), then Finalize.**

1. Cluster is all Phase 1, `cluster_version` implicit 1, era off.
2. Operator restarts **one node** on R (any role, any order; keep a control
   quorum). R speaks NHS1/CHS1 v1 with `ext` and emits only Phase 1 bytes, so the
   Phase 1 peers carry on. Repeat per node, waiting for `Active` and no
   under-replicated tablet each time.
3. After the **last** member is on R (a down member never observed holds this
   at step 3, indefinitely and harmlessly: era-0 is a fully working cluster;
   remove it per ADR 0032 or bring it back on R), P holds on the leader.
4. Leader proposes the initial `ReportNodeVersion`s; the era starts; Phase 1 peers
   are refused from now on (there are none).
5. Every node self-reports at each later boot; `cluster version` shows `active 1,
   safe_target 2, blockers []`.
6. Operator runs `cluster finalize`; `FinalizeClusterVersion { expected: 1,
   target: 2 }` applies; `Gate(2)` opens everywhere as each node applies it.

No step requires a stop: each is one node's ordinary restart, and at no moment
does any node receive bytes it cannot decode (before step 4 nothing new exists;
after step 4 every reader is R). If R's `max_supported` is 1 (no gate yet), the
roll ends at step 5.

**A B2 leader steps down to a Phase 1 leader mid-roll.** Safe: before P holds B2
has proposed and replicated nothing a Phase 1 node cannot decode, so the Phase 1
leader and replicas read every entry, snapshot and delta. After P holds there is
no Phase 1 voter to become leader. A Phase 1 binary reinstalled on a node after the
era (violating Option B) is refused at the handshake by every B2 peer, so it
looks partitioned instead of wedging on a variant it cannot decode.

**No future stop-the-world step (rules).**
1. The handshake version is never bumped incompatibly; handshake evolution is
   `ext` TLVs, unknown ones ignored.
2. A release's only way to change a cross-node surface is a gate. A change that
   cannot be a gate is split into two releases (expand, then contract); it never
   ships as "restart everything".
3. Every release runs the mixed-version corpus against the **previous release
   profile** (section 7); a release that fails it does not merge.

### 3. Gates

- **Registry.** `Gate` is an enum, one variant per behavior change, with an
  exhaustive `const fn version(self)` table (gate -> cluster version; `Gate::Era`
  excepted, above) naming the PR/ADR that introduced each. `ClusterFeatures` (a
  cheap clone, updated by the `Metadata` apply task / mirror, `is_open(Gate)`) is
  injected into every emitter. Never a global: `SimEnv` runs many nodes in one
  process and a per-node handle is what lets two nodes be different binaries.
- **Emit.** Anything **another node or binary reads** is emitted in its new form
  only when `is_open(gate)`, otherwise in the previous form. Scope: a new wire
  message/variant/field (`RaftMsg`, `KvWire`, `KvCommand`, `ClientRequest`/
  `ClientResponse`, `MetaCommand`); a new replicated command shape; any change to
  snapshot/`InstallSnapshot` content (`raftkv-image`, `CSN1`, `MetadataDelta`);
  any change to replicated `Metadata`/mirror content; a client-visible feature
  that only some nodes could serve (refused by name on every node until open).
- **Decode.** Every decoder accepts every version it has ever known.
- **Apply never branches on a gate.** A tablet replica cannot read `Metadata` and a
  data node's view lags; replicas branching on their own view would diverge.
  Apply is a pure function of the entry bytes and the state machine. A semantic
  change ships as a new variant or flag, gated at the **proposer**; the proposer's
  view can only be behind the truth, which is the safe direction.
- **JSON enums.** Fields stay `#[serde(default)]` and are safe only when absence
  means the old behavior; the gate keeps them at default until open. **Variants
  are not safe by being additive** (whole-message failure above). Enforcement: each
  of `RaftMsg`, `KvWire`, `KvCommand`, `MetaCommand`, `ClientRequest`,
  `ClientResponse` gets `fn required_gate(&self) -> Gate`, an **exhaustive match
  with no `_` arm**, so a new variant does not compile until it names its gate.
  The send choke points (`to_vec` sites in `node.rs`, `encode_client_frame`,
  `codec::encode_wire`, the propose path) `debug_assert!(required_gate <= open)`
  and bump a metric in release builds; never a production panic.
- **Binary codecs.** The frame version byte is the dispatch; the encoder picks it
  from the gate for wire and image frames, the decoder accepts all. The WAL
  payload of the same codec follows "local" (section 5).
- **Gate-off equals the previous release, byte for byte,** asserted against the
  previous release's golden fixture; the open side gets a new fixture.

### 4. Finalize

- `MetaCommand::FinalizeClusterVersion { expected: u32, target: u32 }` (era-only).
  Apply rejects unless `expected == cluster_version`, `target == cluster_version + 1`,
  and **every** `Member` row has `version_range == Some(r)` with `r.min <= target <= r.max`.
- **Down / removed-pending members block (DECIDED).** Every row in
  `Metadata.members` counts: `Down`, `Leaving`, a never-activated `Joining`. Such a
  node still holds replicas and maybe a vote; returning on an old binary after
  Finalize it would be refused, a permanent capacity loss. Remedy: bring it back
  on the new binary or remove it (ADR 0032). No override.
- **Manual (DECIDED).** `animus-cli cluster version` (reads `/admin/cluster-version`:
  active, per-node range+build, safe target, blockers) and `animus-cli cluster
  finalize [--to N]` (admin API -> control leader; role-gated like ADR 0037's
  membership actions). Finalize is the step that cannot be undone. Auto-finalize
  may come as a Phase 3 option once an orchestrator knows the roll is complete.

### 5. Rollback policy: Option B (DECIDED, maintainer, 2026-10-03)

**No rollback once a node has run the new binary.** Gating covers only what another
node or binary reads. What a node writes **only for itself** switches to the new
version the first time the node starts the new binary; no old-format writer is kept
in production code. The already-landed CWL/SWL v2 and `raftkv-wal` v2 need no
retrofit. **Consequence:** a bad release mid-roll is **fix-forward only** (ship a
fixed binary, wipe and rebuild the node from peers, or restore from backup); each
node passes its own point of no return at its first start of the new binary. Docs
and the Phase 3 runbook must say so before the first node is touched.

- **Classification rule and its hazard.** A format is *local* only if **no other
  node and no other binary ever reads its bytes**. Traps: snapshots built from
  local engine state are **shipped to peers** (`raftkv-image`, `CSN1`): gated;
  `Metadata` and mirror entity shapes are replicated content: gated; backup
  manifests/data, PITR segments and S3 export objects outlive the cluster and are
  read by future binaries: they keep Phase 1's forever-readable rule regardless
  (each change is a new version + fixture), never gated on cluster version;
  **anything not obviously local defaults to gated** (a wrong "local" is silent
  data loss on a peer; a wrong "gated" costs a release of delay).
- **Option A (rollback allowed until Finalize; new binaries keep writing old
  formats, wire and durable, until Finalize; CockroachDB's model) may be preferred
  in the future.** The design does not preclude it: the `Gate` registry can name
  durable formats, and the floor rule already covers boot-time writes. Switching
  later costs: gating every durable write site (WAL framing, LSM files, manifest,
  key layout); shipping and testing an N-1 *writer* for each (today's test-only
  `legacy-encoders` become production code); deciding what to do with formats that
  already switched under B (grandfather, as CWL/SWL v2 is, or declare the first A
  release a baseline); and a "write old, restart new, write new" axis in the
  upgrade-restart harness. The wire/gate design is unchanged.

### 6. Supported skew and floor policy (DECIDED)

**N-1 <-> N only**, one cluster version at a time. A release at `max_supported = R`
runs at cluster versions R-1 and R (`min_supported = R-1`); R+1 refuses a cluster at
R-1 by name. Why: the emit-old-form code for each gate must be kept and tested for
as long as it can be selected; one release of window bounds that cost and the
mixed-version matrix to one pair. `min_supported` rises only by an ADR amendment
naming the required stepping-stone release; until the first gate ships it is 1.
Durable *readability* stays forever (Phase 1); only the *upgrade path* across a
raised floor needs the intermediate release, and it is still a rolling upgrade.

### 7. Testing

Mixed-version corpus on `SimCluster`/`SimEnv`, reusing `check_cycles`,
`check_durability`, `check_convergence`, the `raftkv_linearizable` checker and the
`sim_cluster_dynamo_corpus` workload.

- **Binary profiles.** A node's "binary" is a per-node `BinaryProfile` handed to
  `ClusterFeatures`, with: `Phase1` (empty `ext`, today's encodings,
  **decode-only of today's variants**), `B2` (range [1,1]), and later `Release(N)`.
  A node capped below a gate (a) emits only forms with `gate <= cap` and (b) under
  `cfg(any(test, feature = "sim-versions"))` **rejects on decode** exactly what the
  real older binary would (`required_gate(msg) > cap` -> the same unknown-variant /
  `UnsupportedFormatVersion` error, then the receiver's "dropped"/teardown
  behavior). `SimEnv` gains a per-node `ext` override beside
  `set_network_protocol_for`, so `check_peer` and the leader's observation see the
  profile. Phase 1 behavior is real code, not a mock: B2 pre-era emits Phase 1 bytes.
- **Synthetic ladder.** Test-only gates (one variant per layer, one default field,
  one codec frame version, one snapshot-content change) model B2 -> B3, since Phase 2
  ships no real gate. Real gates join via `Gate::ALL`.
- **Knob `ANIMUS_UPGRADE_SEEDS=K`** (default 1; nightly deep in `corpus-deep.yml`);
  `ANIMUS_UPGRADE_CELL=<substring>`, `ANIMUS_SEED=<seed>` replay. Distinct from
  `ANIMUS_UPGRADE_RESTART_SEEDS`. Home: `animusd` `sim_cluster_mixed_version_corpus`
  (`--lib`, like tier 2) plus a pure per-layer tier in `animus-control`/`animus-cp-data`.

| Cell | Asserts |
|---|---|
| **Phase 1 -> B2 under load**: all nodes `Phase1`, rolled one at a time to `B2` with a linearizable workload, **leader changes mid-roll** (kill leader at random points, incl. while P is about to hold), crashes, torn-tail, partitions | no client-visible error beyond retries; `check_cycles` + linearizability; no Phase 1 profile node ever receives a byte with `required_gate > Phase 1` (a delivery assertion); the era starts only after the last member is `B2`; a Phase 1 leader and a B2 leader both appear in some seeds |
| era start, then Finalize to 2 with the synthetic ladder | gate 2 opens only after every row reports; `check_cycles`; per-node mirrors agree on `cluster_version` |
| a member down (never observed) at P | era does not start; cluster serves normally; era starts once it is back on `B2` or removed |
| Phase 1 binary (re)joins after the era; Phase 1 binary against a cluster at 2 | refused by the B2 side at the handshake; counted; never a member; Finalize blocked until removed |
| out-of-range binary (R+1 vs cluster at R-1; R-1 vs cluster at R) | refused at startup or handshake by name |
| Finalize early (blocker present) | rejected by name; cluster unaffected |
| N-1 -> N for every later release (previous-release profile) | same assertions; this cell is mandatory for every release (rule 3) |
| **negative controls** | (1) a premature new `MetaCommand` variant emitted while a `Phase1` voter exists **wedges that replica** (its `AppendEntries` is dropped; commit never reaches it) and the corpus must fail; (2) an ungated variant, (3) an ungated field, (4) a gate opened on a stale view, each fail the corpus |

- **Per-gate unit tests (both sides):** closed -> bytes equal the previous golden
  fixture and an older-profile decoder accepts them; open -> new fixture round-trips and
  an older-profile decoder refuses by name; an exhaustiveness test that every `Gate`
  has a version and a test row.
- **Later (Phase 3):** a CI job that builds the previous release tag's real `animusd`
  and rolls a `ProdEnv`/`kind` cluster across it, since the profile models the gate
  discipline of the *current* tree, not the old bytes.

### 8. Inventory: surfaces under the gate discipline

Class: **G** gated on `cluster_version` (or `Gate::Era`); **L** node-local, switches at
first start of the new binary (Option B); **F** outlives the cluster, Phase 1 rules only.

| Surface | Code | Class | Mechanism |
|---|---|---|---|
| Control `RaftMsg` (JSON) incl. `Heartbeat` | `animus-control/src/raft.rs`, `node.rs` | G | `required_gate`; variants gated, fields `#[serde(default)]` |
| `MetaCommand` (log entries, `ProposeSchema`) | `animus-control/src/meta.rs`; `animus-node/src/wire.rs` `is_relayable_command` | G | `required_gate`; relay allowlist updated with every new variant (root CLAUDE.md rule) |
| `Metadata` content, `Member` fields | `meta.rs` | G | additive skipped-at-default fields; populated only after the gate |
| Control snapshot / `InstallSnapshot` (`CSN1`) | `raft.rs`, `persist.rs` | G | persisted *and* shipped |
| `MetadataDelta`/`WatchMetadata`, mirror entity values | `animus-node/src/wire.rs`, `mirror.rs`, `delta_ring.rs` | G | entity shapes gated (mirror `expect`s) |
| `KvWire` (`Raft`, `ReadProbe`, `HeartbeatBatch`) | `animus-cp-data/src/lib.rs`, `codec::encode_wire` | G | frame version from the gate |
| `KvCommand` shapes | `animus-cp-data/src/lib.rs`, `codec.rs` | G | new variant/flag gated at the proposer |
| Tablet `InstallSnapshot` image (`ImageEntry`) | `animus-cp-data/src/lib.rs`, `codec.rs` | G | built from the leader's engine, read by peers |
| `ClientRequest`/`ClientResponse` | `animus-node/src/wire.rs`, `codec.rs`, `animusd` `read_frame` | G | `required_gate`; an unknown variant tears down the connection |
| Client-visible DynamoDB behavior | `animusd/src/dynamo.rs`, `animus-dynamo` | G | refused by name on every node until open |
| Admin/dashboard/console HTTP JSON | `animusd/src/admin.rs`, `dashboard*`, `console.rs` | G (additive) | new fields only; new action routes gated |
| Handshakes `NHS1`/`CHS1` | `animus-env/src/handshake.rs`, `prod.rs`, `animusd/src/lib.rs` | version fixed at 1 forever | `ext` TLVs; `check_peer` + disjoint-range refusal; `Envelope.peer_ext` |
| `ReportNodeVersion`, `FinalizeClusterVersion` | new | `Gate::Era` | era-only variants |
| `control-wal`/`shared-wal`/`raftkv-wal`, LSM WAL/SSTable/manifest, key-layout marker, `ADE1` | `animus-control/src/persist.rs`, `animus-storage`, `animus-cp-data/src/layout.rs`, `animus-env/src/encrypted.rs` | L | next bump at first start; checklist applies |
| Mirror files, `ClusterConfig` | `mirror.rs`, `animusd/src/config.rs` | L | only that node reads them |
| Backup manifest/data, PITR/stream segments, S3 export | `animus-cp-data/src/backup.rs`, `segment.rs`, `animusd/src/import.rs` | F | forever-readable; no gate |
| Hash-ring token, `numkey`, key bytes, stored item, `ChangeRecord` | `animus-tablet`, `animus-item` | frozen | pinned by vectors; a change is an ADR amendment |
| Operator CRD | `animus-operator/src/crd.rs` | out of scope | Phase 3 |

### 9. Workstreams (one session/PR series each)

| WS | Crates | Scope | Depends on | Do not touch |
|---|---|---|---|---|
| **P2-A** version core + bootstrap | `animus-control`, `animus-env`, `animus-sim` | `version` module (`ClusterVersion`, `VersionRange`, `Gate` incl. `Gate::Era`, `ClusterFeatures`); `Metadata` additive fields + `versioning_active()` + fixture; `ReportNodeVersion`/`FinalizeClusterVersion` + apply checks (era-only); handshake `ext` TLV codec, disjoint-range refusal, `Envelope.peer_ext` in `ProdEnv`/`SimEnv`, per-node `SimEnv` ext override; the leader's observation table and **precondition P / era start**; era-on handshake refusal of empty-`ext` peers; startup range check | none | `animus-cp-data`, `animus-node`, `animusd`, operator, `website/` |
| **P2-B** gate enforcement | `animus-control`, `animus-cp-data`, `animus-node` | `required_gate` exhaustive tables for the six enums; send-site asserts and metric; gate-selected codec frame version; `is_relayable_command` for the new commands; G/L/F review of the inventory; **byte-identity test: B2 pre-era encodings equal the Phase 1 fixtures** | P2-A (types) | `animusd` wiring, `animus-test`, `animus-env` |
| **P2-C** node wiring + admin | `animusd`, `animus-cli` | `ClusterFeatures` fed from the apply task/mirror and injected into every emitter; boot-time self-report (era on); `/admin/cluster-version`, `cluster version`/`cluster finalize`; joiner and `change_membership` range checks; additive `JoinInfo` field | P2-A (P2-B for full gating) | `animus-env`, `animus-control` internals, operator |
| **P2-D** corpus | `animus-test`, `animusd`, test-only cfg elsewhere | `BinaryProfile` incl. `Phase1`, capped decode, delivery assertion, synthetic ladder, `sim_cluster_mixed_version_corpus`, pure tier, per-gate tests, negative controls, `ANIMUS_UPGRADE_SEEDS`, `corpus-deep.yml` step | P2-A, P2-B; P2-C for the cluster tier | production emit/apply logic (report bugs, do not fix inline) |

Waves: P2-A first (largest now: it owns the bootstrap); P2-B and P2-C concurrent; P2-D
starts its pure tier with P2-B and its cluster tier with P2-C. **P2-A must not merge
alone into a release**: until P2-B's enforcement and P2-D's Phase 1 cell land, nothing
proves B2 is Phase 1-safe, so the three merge as one release train.

### 10. What Phase 2 "done" means, and what is supported

Done when: B2 merged; every G row has `required_gate` or a per-gate test; the corpus
(both tiers, negative controls, the Phase 1 -> B2 cell) is in the per-push gate at K=1
and nightly deep; root `CLAUDE.md`'s format-change checklist gains the "class G/L/F,
which gate" step; the website and ADR 0060 "Upgrades" are updated in that final PR.

**Supported after Phase 2:** a **manual node-by-node rolling upgrade with no stop**,
**from today's Phase 1 binaries to B2** and from release R-1 to R afterwards: restart
one node at a time, wait until it is `Active` and no tablet is under-replicated
(control quorum kept), then run `cluster finalize` once every member, down ones too,
reports the new range. **Not supported:** skipping a release; running N-1 with N+1;
rolling a node back after it ran the new binary (fix forward, rebuild from peers,
restore from backup); a Phase 1 binary joining or rejoining once the era has started
(refused by name on the B2 side); operator-driven `spec.image` changes (the operator
is untouched; ADR 0060 "Upgrades" stands until Phase 3).

**Phase 3 sketch (not a design):** a per-node roll runbook/CLI (drain via ADR 0032
where useful, restart, wait-healthy), a roll status view, optional auto-finalize, operator
`spec.image` orchestration (OnDelete, one pod at a time, gated on the same health
signal), and the previous-release `ProdEnv`/`kind` cross-version CI job.

**Residual risks (not closed by design):**
1. *Observation is leader-local and time-bounded.* A Phase 1 node installed after P
   but before the first report is applied is handled by the refusal rules, but P
   itself trusts heartbeats' `ext`; P2-A must test that every role's heartbeats carry
   it.
2. *An operator violating Option B* (reinstalling a Phase 1 binary after the era) is
   made safe by the B2 handshake refusal, but the Phase 1 node's own logs only show a
   closed connection.
3. *The profile models gate discipline, not old bytes*; the previous-release `ProdEnv`
   job (Phase 3) is the real proof.

(All maintainer questions on this amendment are decided; none remain open.)

### Amendment 2026-10-03 — P2-A implementation notes (corrections found in the code)

These notes record where the Phase 2 design above was wrong or incomplete
against `main` at `2e28aeb`, and how P2-A does it instead. They belong to the
P2-A stack. Like the rest of P2-A, they ship in the P2-A + P2-B + P2-D
release train.

1. **The version record is a separate map, not `Member` fields.**
   `Metadata.node_versions: BTreeMap<NodeId, NodeVersion { range:
   VersionRange, build: String }>` (`#[serde(default, skip_serializing_if =
   "BTreeMap::is_empty")]`) and `Metadata.cluster_version: u32`
   (`#[serde(default, skip_serializing_if = ...)]`, where `0` or absent reads
   as `1`). These replace `Member.version_range`/`Member.build`. There are
   three reasons:
   - `UpsertMember`'s apply replaces the whole `Member`, so every
     failure-detector flip would erase a version field stored there.
   - **Control-only voters have no `Member` row at all.** `RegisterNode`
     with `role == "control"` creates only a `node_addrs` entry, so "every
     row in `members`" missed them. A Phase 1 control-only voter would then
     not block Finalize, yet it would wedge on the first gated variant.
   - A new `Member` field touches ~120 struct literals across crates
     outside P2-A.

   **The required set** for precondition P, the era-start reports and
   Finalize's apply check is therefore `members` ∪ `node_addrs` keys (every
   registered node of any role). `RemoveMember` already prunes `node_addrs`,
   and now prunes `node_versions` too.

   `versioning_active()` is `!node_versions.is_empty()`. Era-0 bytes stay
   identical to Phase 1's `"v": 1` (fixture `metadata/v1-era.json` is new,
   and `v1.json` is untouched). The fixture test learns the `vN-<shape>.json`
   naming, with one expected value per shape.
2. **Observation covers every inbound control envelope, not only
   `Heartbeat`.** Control-only nodes do not run `heartbeat_loop_live`.
   - Each role does reach the leader on a connection it dialed itself, over
     NHS1, never relayed:
     - combined (incl. `--cluster N`, which uses real sockets between
       in-process nodes) and data-only (incl. `--seed`/`join`): via
       heartbeats;
     - control voters and learners: via their Raft traffic.
   - The leader keys its table by `Envelope.from`. A SimEnv test per role
     pins this (residual risk #1).
3. **The mirror does not panic on an unknown entity kind.**
   `syskv::decode_key` returns `None` for an unknown kind and `apply_put`
   skips it, and an unknown counter id is ignored as well. Only a changed
   *shape* of an existing kind hits the `expect`s. P still covers every
   registered node, because data-only nodes also decode control-plane
   relays.
4. **Plumbing the design did not name** (all in `animus-env`/`animus-sim`):
   - `ProdEnv` gets a setter for its own `ext` (today
     `Preamble::for_protocol` hardcodes it empty).
   - The accept path keeps the peer preamble and stamps
     `Envelope.peer_ext` (one shared buffer per connection).
   - A `check_peer_ext(spec, own_ext, peer, require_peer_ext)` beside
     `check_peer` adds the disjoint-range refusal and the era-on refusal of
     an empty `ext`, both as named `HandshakeError` variants. It uses a TLV
     parser local to `animus-env`, so `animus-env` still does not depend on
     `animus-control`.
   - A `Network::set_require_peer_ext(bool)` hook (default no-op;
     `ProdEnv`, `SimEnv` and `EncryptedEnv` implement or forward it) is
     what the control apply task flips when `versioning_active()` becomes
     true. That is the era-on refusal of empty-`ext` peers. In `ProdEnv` it
     refuses new handshakes and closes an already-accepted empty-`ext`
     connection on its next frame.
   - `ProdEnv::set_own_ext` applies to connections handshaken after the call
     (pooled dialed connections keep what they advertised), so `animusd`
     sets it at bind, before any traffic.
   - `SimEnv` gains `set_network_ext_for(node, ext)`.
   - **Wiring `animusd` to give each `ProdEnv` its `ext`, and its CHS1
     sites, is P2-C**, so P2-A on its own changes no byte on any wire.
5. **`animus-node` gets one minimal edit.** `is_relayable_command` is an
   exhaustive match, so the two new `MetaCommand` variants need an arm for
   the workspace to compile. P2-A adds them as **not relayable**. P2-B/P2-C
   decide relay (boot-time self-report from data-only nodes; admin Finalize).
6. **The startup range check halts the node, not the process.** In
   `animus-control`, a node whose range excludes `cluster_version` (at the
   seed and on snapshot install) halts its `RaftNode` with a named
   `halt_reason`. Turning that into the named process exit is `animusd`
   wiring (P2-C).

### Amendment 2026-10-03 — P2-A notes from PRs 4 and 5

1. **A `RaftNode` defaults to the Phase 1 profile** (`own range = None`).
   The assembler opts in with `set_own_version_range(Some(own_range()))`
   in the same step that gives the `Env` its `ext` (`set_own_ext`). That
   step is P2-C wiring. With a B2 default, a lone voter (required set =
   itself) satisfies P at once and starts the era. The era-on refusal then
   refuses every peer whose `ext` is still empty, which broke 13 existing
   single-node tests and would break a one-node production cluster before
   P2-C. So until P2-C lands, P2-A never starts an era in production.
2. **A leader whose required set is only itself satisfies P immediately.**
   This is intended. Because the era then refuses empty-`ext` peers, the
   `Env` must carry its `ext` before the node can lead.
3. **Cache lag.** P is evaluated against the leader's applied cache. The
   loop waits for `engine_applied >= commit_index` and for one observation
   window of leadership. That narrows the late-joiner race but does not
   close it: a Phase 1 node may register between the last P evaluation and
   the first applied report. The **era-on refusal** closes that race, not P.
4. **The era-on upkeep records late registrants.** After the era starts,
   the leader's upkeep (`era_on_proposals`) also reports any required node
   that has no record yet, or whose observed range or build changed (rate
   limited). Boot-time self-report (P2-C) remains, and this leader path is
   an additional safety net.
5. **`halt()` does not stop the driver loops** (existing semantics). The
   out-of-range halt is observable through `RaftNode::halt_reason()`, and
   the named process exit is P2-C. The era-on flag flip on data-only nodes
   (`ControlHandle::Remote`) is P2-C too.

### Amendment 2026-10-04 — P2-D as built (mixed-version corpus)

Built against `main` plus P2-A; P2-B (gate enforcement, `required_gate` tables)
and P2-C (node wiring, `ClusterFeatures` fed into emitters) are not on `main`, so
this is the part of section 7 that does not depend on them.

**Landed.**
- `sim-versions` feature on `animus-control` (enabled via `animus-test`) with
  `BinaryProfile {Phase1, B2, Release(N)}`, `RaftNode::set_binary_profile` (own
  range + build + decode cap in one call) and a receive-site **rejection log**
  (`CapLog`). The delivery assertion is read at the production drop point, not in
  a `SimEnv` tap, because an `InstallSnapshot` payload is a chunk, not a parseable
  message; a **state assertion** (a Phase 1 node's applied `Metadata` never shows
  versioning fields) covers what the cap cannot see. Capped decode classifies with
  a provisional single-call-site `provisional_required_gate` (era variants =>
  `Gate::Era`): **P2-B replaces it with `required_gate`**.
- Pure tier (`animus-control/tests/it/version_mixed_corpus.rs`): Phase1 -> B2 rolls
  in several orders with leader kills, deterministic Phase1-leader and
  B2-leader cells, kills swept around precondition P, member-down, a Phase 1
  binary after the era (refused, counted, never recorded, Finalize blocked by
  name), early Finalize and out-of-range halts, and negative control N1.
- Cluster tier (`animusd` `sim_cluster_mixed_version_corpus`): rolling over
  `SimCluster` under the linearizable DynamoDB-wire workload with control-leader
  kills and a partition mid-roll, the same deterministic leader-by-profile cells,
  a data-only node down at P, and N1. `SimCluster::set_binary_profile` applies a
  profile through one `apply_profile` helper that `restart` also calls.
- Knobs `ANIMUS_UPGRADE_SEEDS` (default 1), `ANIMUS_UPGRADE_CELL`, `ANIMUS_SEED`;
  per-push at K=1, nightly deep in `corpus-deep.yml` (fixed depths, no new
  `workflow_dispatch` input).
- Mutation checks recorded in the PR: M1 (cap logs but delivers), M2 (P accepts a
  `range: None` peer), M3 (the require-peer-ext flag never flipped), M4 (observe
  only heartbeats) each fail the corpus; M4 only in the pure tier, because every
  `SimCluster` control node is `Both` and heartbeats.

**Pending, not registered as tests (nothing passes vacuously).**
- Synthetic gate ladder, `required_gate`-exhaustiveness and byte-identity
  per-gate tests, `ungated variant/field` and `stale view` negative controls
  (N2-N4), the data-plane and `animus-node` tiers: **P2-B**.
- Phase 1 joiners after the era, the data-only node's era flag
  (`ControlHandle::Remote`), `RegisterNode` joiner range checks, and
  `Release(N-1) -> Release(N)` cells over real gates: **P2-C** and the first
  real gate.

**Observed, not fixed (P2-A, unchanged).** `EraWatch::sync` sets the require flag
even for an own range of `None`; the sim keeps `require_peer_ext` across
`Simulator::stop`, so the restart window ProdEnv has is not modelled; the sim's
disjoint-range check reads the sender's *current* ext. None blocks a cell here.
