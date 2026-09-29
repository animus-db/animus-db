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
   - **Baseline commit:** *(not yet reached — filled in when Phase 0's last
     workstream merges)*.
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
     window:** proposed default is *every post-baseline version stays
     readable until the project's first tagged release defines an explicit
     window* — left as an open question below since pre-alpha has no
     tagged releases yet to hang a window off of.
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
     reset:** **in progress**, this PR starts it. Tracked as five
     independent workstreams (A–E) — see the table below. Not done until
     every workstream's PR series has merged and the baseline commit above
     is filled in.
   - **Phase 1 — on-disk N-1 stability:** planned, blocked on Phase 0's
     baseline.
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
| **C** | `animus-cp-data` | RaftKV command codec re-baseline (`codec.rs::VERSION` 31 → 1); `SharedWal` outer `Line{tablet, record}` envelope (`SWL1`+v1 — the *inner* `record` bytes are already the versioned codec payload, unchanged shape, only the outer tag is new); segment codec re-baseline (`segment.rs::VERSION` 2 → 1); a version marker on the per-tablet engine key layout (ADR 0050's `kind \|\| logical` convention) — this one is a design task, not a mechanical tag-and-fixture: recommend a single reserved leading byte in the `kind` namespace itself (a `layout` epoch) rather than a per-record magic, since the layout is a key-space convention, not a framed record; document the chosen approach directly in `crates/animus-cp-data/CLAUDE.md`, this ADR does not prescribe the exact bit layout | B (for the shared `encode_tagged_record`/`Line`-shape convention, so C's `SharedWal` envelope and B's Raft WAL envelope don't independently invent incompatible tagging shapes for what is structurally the same "envelope wraps an inner payload" problem) | `crates/animus-cp-data/**` only. Do not touch `crates/animus-control/src/persist.rs` — read it for the shared convention, don't edit it. |
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

- **Support window length, post-baseline.** Still open. Proposed default
  (see Maintainer decision, point 3): *every post-baseline version stays
  readable until the project's first tagged release defines an explicit
  window*, since pre-alpha has no releases yet to size a window against.
  Revisit at the first tagged release — N-1 only, N-2, or a time-boxed
  "supported upgrade path" (Kubernetes' skip-version policy) are the
  candidates, and longer windows cost more ongoing translation-path
  maintenance.
- **Which formats go first inside Phase 1?** Still open, but narrowed:
  Phase 0's own workstream split (A–E above) already sequences the *reset*
  by crate; Phase 1's own sequencing (which reset format gets its N-1
  decode path first) still prioritizes backups/PITR/export (already
  outlive the cluster) and the RaftKV codec + LSM formats (highest change
  frequency, sit under everything else), but the actual order is a sizing
  decision for whoever picks Phase 1 up, not fixed here.
- **Freeze the Raft codec, or keep translating it?** Still open, and now
  sharper: Phase 0 resets `codec.rs::VERSION` to 1, so this question is
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
