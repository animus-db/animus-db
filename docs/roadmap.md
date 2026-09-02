# Roadmap: every known feature gap, with a plan

**Status: living document.** Produced 2026-09-02 from an audit of the code
against the ADRs, the per-crate guides, the website, and the issue tracker.
Each item below is a gap that was **verified against the code**, not just
against prose (the audit found several "gaps" the docs still describe that
the code has long since closed — those are listed first, as doc fixes).

How to maintain this file:

- When an item lands, delete it here and record the decision where it
  belongs (the ADR, the crate guide, `website/`). This file is a queue, not
  a changelog.
- When a new gap is found, add it with the same fields: **Gap**, **Plan**,
  **Reuse**, **Files**, **Tests**, **ADR**, **PRs**, **Size**, **Depends**.
- Sizes are S (hours), M (a day or two), L (a week), XL (multi-week). They
  are estimates for a stacked PR series delivered per `CLAUDE.md`'s session
  operating mode, gates green throughout.
- "PRs" is the suggested `gh-stack` shape. Anything with more than one
  reviewable step stacks by default.

The next free ADR number at the time of writing is **0063**.

---

## 0. Corrections: the docs say "missing", the code says "built"

These are not feature gaps. They are prose that lags the code. Most of the
rows this section used to carry were fixed by the stale-prose sweep; what's
left is the one row below with nowhere in the docs to point a fix at, plus
the still-true paragraph after the table.

| Prose claim | Where | Reality |
|---|---|---|
| Read-path counters for ReadIndex vs eventual reads "missing" | (this audit's first pass) | `CpReadBarriersServed/TimedOut`, `CpEventualReads{Local,Forwarded,FellBack}` exist and surface in `/admin/metrics` |

Still true, and worth stating so nobody re-audits: the operator's **own**
container image is not published (`.github/workflows/image.yml` publishes
`animusd` only); `Cargo.lock` is gitignored (`.gitignore:3`) yet that
workflow lists it as a trigger path.

---

## 1. Wire surface (DynamoDB API)

### W-01 UpdateExpression extensions (issue #375)

- **Gap:** only `SET a = :v`, `REMOVE a`, `ADD`, `DELETE`. No
  `if_not_exists()`, `list_append()`, arithmetic `SET a = a + :x`, or
  nested-path targets (`a.b.c`, `a[0]`). The most likely thing to break an
  existing app.
- **Plan:** extend the tokenizer (already emits `LParen`/`RParen` with
  depth tracking, forward-built for this) and `parse_update_clauses`;
  evaluate function calls and arithmetic at the leader against `raw_old`
  under the same `rmw_lock` scope ADD already uses; make `apply_update`
  path-aware.
- **Reuse:** `condition::add_numeric` (decimal bignum) for `+`/`-`;
  `resolve_attr_name`'s alias logic for path targets; `Projection::apply`'s
  path reconstruction for nested writes.
- **Files:** `crates/animus-dynamo/src/wire.rs` (`tokenize_update_expression`
  ~1651, `parse_update_clauses` ~1834, `resolve_attr_name` ~1973,
  `apply_update` ~3962); `crates/animusd/src/dynamo.rs`
  (`kind_write_item_at_leader` ~5733, both `apply_update` call sites).
- **Tests:** `wire.rs` inline `decode_update_expression` cases (~6380);
  `crates/animusd/tests/dynamo_updated_return_values.rs`,
  `dynamo_documents.rs`.
- **ADR:** none (additive grammar within a documented subset).
- **PRs:** (1) function calls `if_not_exists`/`list_append`, SET only;
  (2) arithmetic operands, `+`/`-` as first-class tokens; (3) nested-path
  SET/REMOVE targets (touches `apply_update`'s data model).
- **Size:** M + S + L.
- **Depends:** do W-02 first so PR3 reuses its `Field | Index` path
  segment type.

### W-02 ProjectionExpression list-index paths

- **Gap:** `a[0]` is rejected (`reject_list_index`, `wire.rs:2883`).
- **Plan:** change the projection path segment from `String` to
  `enum { Field(String), Index(usize) }`; extend `Projection::apply`'s
  reconstruction to build `L` elements at the indexed slot.
- **Files:** `wire.rs` `resolve_projection_name` ~2847, `reject_list_index`
  ~2883, `Projection::apply`.
- **Tests:** `wire.rs` projection unit tests; `dynamo_documents.rs`.
- **ADR:** none. **PRs:** one. **Size:** S.

### W-03 Order-preserving encoding for `N` sort keys

- **Gap:** `AttributeValue::key_bytes()` writes `N` as raw decimal text, so
  Query ordering across magnitudes and signs is bytewise, not numeric.
  Range predicates are correct; `ScanIndexForward` order is not.
- **Plan:** canonical sign + exponent + digit-run encoding with inversion
  for negatives, applied at the single choke point `key_bytes()` and
  mirrored byte-for-byte in `animus-tablet`'s escape/token primitives;
  `matches_raw` must decode the new encoding instead of UTF-8 text.
- **Reuse:** `condition.rs`'s `decimal_parts`/`add_digits`/`sub_digits`
  for canonicalisation.
- **Files:** `crates/animus-dynamo/src/lib.rs:84-97`;
  `crates/animus-tablet/src/lib.rs:44,134`; `index.rs:140-249` (GSI/LSI
  row keys); `condition.rs:118-131`.
- **Tests:** new differential proptest against `bigdecimal` next to
  `condition.rs`'s `decimal_differential_tests`; rewrite
  `matches_raw_reinterprets_bytes_by_the_conditions_own_declared_type`
  (`condition.rs:870`); explicit regressions for GSI, LSI, streams, and
  backup/restore reading the same bytes.
- **ADR:** **yes, 0063** — amends ADR 0022/0023's key layout, which those
  ADRs mark "do not change without a data migration". No back-compat is
  owed (root `CLAUDE.md`), but the decision is recorded.
- **PRs:** (1) ADR; (2) encode/decode + proptest, unwired; (3) wire
  `key_bytes` + `animus-tablet` mirror + `matches_raw`; (4) index, stream,
  backup regressions.
- **Size:** L (small algorithm, large blast radius).
- **Depends:** land W-05 first to avoid rebasing `schema.rs`'s bridge.

### W-04 UpdateTable strictness

- **Gap:** `BillingMode`, `SSESpecification`, `ReplicaUpdates`,
  `ProvisionedThroughput` are never parsed, so a body carrying only those
  may be silently ignored instead of rejected; GSI `Update` element
  rejection wording needs confirming.
- **Plan:** explicit `ValidationException` for each unsupported top-level
  key in `decode_update_table` (`wire.rs:2585-2626`), matching the existing
  GSI-`Update` rejection idiom at `decode_index_updates` ~2670.
- **Tests:** `wire.rs` UpdateTable cases (~6074-6180).
- **ADR:** none. **PRs:** one. **Size:** S.

### W-05 Durable index key attribute types (issue #319)

- **Gap:** `IndexDef` carries bare attribute names; `DescribeTable`'s
  `AttributeDefinitions` defaults every index-only attribute to `"S"`.
- **Plan:** add `#[serde(default)]` type fields to `IndexDef` and the
  registry's `GlobalSecondaryIndex`/`LocalSecondaryIndex`, threaded from
  the `AttributeDefinitions` array `CreateTable`/`UpdateTable` already
  require; both bridge directions; console Add-GSI type picker.
- **Files:** `crates/animus-control/src/schema.rs:244-262`;
  `crates/animus-dynamo/src/registry.rs:67-88`, `schema.rs:146,172`,
  `wire.rs` `decode_index_entry` ~2462 and `attribute_definitions`
  ~4382; console `AddGsiRequest` (`crates/animusd/src/lib.rs:2697-2723`).
- **Tests:** flip
  `describe_table_response_attribute_definitions_cover_index_key_attributes`
  (`wire.rs:5864`).
- **ADR:** none. **PRs:** (1) fields + CreateTable; (2) UpdateTable
  CreateIndex + console; (3) response fix + test flip. **Size:** M.

### W-06 Cheap missing ops: tags, DescribeLimits, DescribeEndpoints

- **Gap:** all return `UnknownOperationException`. Some SDK tooling probes
  them.
- **Plan:** `tags: BTreeMap<String,String>` on `TableSchema`
  (`#[serde(default)]`, same precedent as `stream`/`ttl`/`pitr`), mutated by
  new `MetaCommand::{TagResource,UntagResource}` modelled on `SetTableTtl`
  (`meta.rs:1412`, apply arm ~3082). `DescribeLimits` is static;
  `DescribeEndpoints` reads `ClusterConfig` listen addresses.
- **Gotcha:** a new `MetaCommand` variant must be added at every gating
  match site (`is_relayable_command`, `mirror.rs`, `syskv.rs`) — the
  "missed allowlist" bimodal flake in root `CLAUDE.md`.
- **Files:** `crates/animus-control/src/{schema.rs:298-338,meta.rs}`;
  `wire.rs` decode table ~1287; `crates/animusd/src/dynamo.rs` dispatch
  ~568.
- **Tests:** new `crates/animusd/tests/dynamo_tags.rs`; `meta::tests`
  next to `SetTableTtl`; a follower-connected regression for relay.
- **ADR:** none. **PRs:** (1) control-plane plumbing; (2) three tag ops;
  (3) DescribeLimits + DescribeEndpoints (independent). **Size:** M + S.

### W-07 PartiQL (`ExecuteStatement`, `BatchExecuteStatement`, `ExecuteTransaction`)

- **Gap:** absent. Deliberately sized honestly: a new parser, a new error
  surface, and a WHERE-to-key-bound-or-filter compiler that must not
  diverge from `ConditionExpression` semantics.
- **Plan:** new `crates/animus-dynamo/src/partiql.rs`, minimal grammar
  (SELECT/INSERT/UPDATE/DELETE with WHERE on key attributes, `?`
  placeholders only, never string interpolation), lowering onto the
  existing `Operation` variants so `animusd` needs no new primitive.
- **Reuse:** `SortKeyCondition` comparators for key WHERE clauses;
  `decode_condition` for non-key WHERE as a filter; `TransactWriteItems`
  for `ExecuteTransaction`.
- **Tests:** new `dynamo_partiql.rs` end-to-end; unit tests in the module.
- **ADR:** **yes** — pins the supported subset and the placeholder-only
  discipline.
- **PRs:** (1) ADR; (2) SELECT → Query/Scan; (3) INSERT/UPDATE/DELETE;
  (4) Batch; (5) ExecuteTransaction. **Size:** XL.
- **Depends:** soft: after W-01 if its tokenizer generalises.

### W-08 Per-table throttling

- **Gap:** `capacity.rs` computes `ConsumedCapacity` but nothing gates a
  request; no protection against a noisy tenant.
- **Plan:** token bucket keyed per tablet (shape copied from
  `ChangeRateTracker`, `crates/animusd/src/lib.rs:6123-6203`, including its
  `retain_existing(&Metadata)` GC), checked in `kind_write_item_at_leader`
  and the read-path entry before proposing; returns
  `ProvisionedThroughputExceededException`. Configure first with a
  cluster flag, then per table via a replicated `TableSchema` field set by
  `UpdateTable ProvisionedThroughput`.
- **Seam:** `write_path.rs` is `#[deny(clippy::disallowed_methods)]` —
  the bucket must use `env.now()`, never `tokio::time::Instant`.
- **Tests:** new `dynamo_throttling.rs` over `SimEnv`'s virtual clock.
- **ADR:** **yes** — per-table vs per-tablet scope, per-node vs
  coordinated, interaction with transaction atomicity.
- **PRs:** (1) ADR; (2) bucket + tracker; (3) enforcement + error mapping;
  (4) config surface. **Size:** L.
- **Depends:** W-09 first (shares the per-tablet tracker).

### W-09 Hot-but-small auto-split signal (ADR 0034 deferred item)

- **Gap:** the byte trigger cannot see a small tablet under heavy load.
  ADR 0034 says no per-tablet rate signal exists. **Partly stale:**
  `ChangeRateTracker` exists but is fed only from `KIND_CHANGE` bytes, so
  only streamed/indexed tables get a rate.
- **Plan:** sibling `RequestRateTracker` (ops/sec) observed unconditionally
  from `kind_write_item_at_leader`; new `auto_split_loop` gate
  (`lib.rs:8485-8520`) and `--auto-split-ops-rate` knob.
- **ADR:** amendment note closing ADR 0034's deferred bullet.
- **PRs:** one. **Size:** M. **Depends:** S-06 so the knob is reachable
  from real deployments.

### W-10 Stream sealing from a control-only leader (ADR 0043)

- **Gap:** a control-only leader can mark catalog rows but has no
  `SegmentStoreHandle` (`DataRole`, `lib.rs:6231`), so it cannot delete,
  reclaim, or repair segment objects.
- **Plan:** provision `SegmentStoreHandle`/`BackupStoreHandle` on the
  control-only assembly path (`main.rs` role assembly, ADR 0035) instead
  of gating on data role.
- **Tests:** extend the segment-janitor tests with a control-only
  topology.
- **ADR:** none (ADR 0043 names the fix). **PRs:** one. **Size:** M.

---

## 2. Security, storage, and deployment

### S-01 TLS on every port

- **Gap:** none anywhere: client, intra-node, admin, console.
- **Plan:** `rustls` (already in the workspace via `kube`, `ring`
  provider). Wrap `TcpListener::accept()` streams in a `TlsAcceptor` at
  `Node::bind`/`bind_control`/`bind_data` (`lib.rs:4184,4237,4275`) and
  `serve_requests` (~8612); wrap `ProdEnv`'s outbound dial in a
  `TlsConnector`. The intra wire sits inside `ProdEnv`, so `Network` does
  not change; the client/admin/console listeners live in `animusd` outside
  the seam. Operator: cert-manager `Certificate`/`Issuer` + volume mounts +
  `ClusterConfig` cert-path fields.
- **Tests:** `prod::tests` loopback for intra; a TLS variant of each
  real-listener `animusd` test (`prod`-feature-gated real-thread tests).
- **ADR:** **yes, 0063 (or next)** — crosses ADR 0047, 0057, 0060.
- **PRs:** (1) intra/`ProdEnv` with file certs; (2) client/admin/console
  listeners; (3) operator cert-manager wiring; (4) ADR closing note +
  website "Planned" pill. **Size:** L.

### S-02 SigV4 hardening (ADR 0057 follow-on)

- **Gap:** one static key map from config; no rotation, no replication,
  no dynamic API, no per-table policy, no multi-tenancy.
- **Plan:** `Credentials: BTreeMap<AccessKeyId, CredentialRow>` in
  `Metadata`, mutated by new `MetaCommand`s (template: backup/PITR rows,
  `meta.rs:326-421`); each row carries a minimal allow list
  (`tables`, `ops`); dual-secret grace window for rotation; admin CRUD
  route; static config becomes bootstrap merged at the `dynamo.rs` gate.
- **Files:** `crates/animusd/src/config.rs:105-150`,
  `crates/animus-dynamo/src/sigv4.rs` (unchanged verifier),
  `crates/animus-control/src/meta.rs:1148`, `crates/animus-node/src/admin.rs`.
- **Tests:** `sigv4_vectors_test.rs` untouched; a `credentials` corpus in
  `animus-control` modelled on the backup-catalog one; `animusd` auth
  integration test.
- **ADR:** **yes** — materially changes ADR 0057's "not IAM" stance.
- **PRs:** (1) replicated map + commands; (2) admin CRUD + rotation;
  (3) allow-list enforcement at dispatch. **Size:** L.

### S-03 Encryption at rest

- **Gap:** not mentioned anywhere in the docs.
- **Plan:** decide `Disk`-seam byte-level AES-GCM in `ProdEnv`
  (`animus-env/src/lib.rs:435`) versus LSM block-level in
  `animus-storage`; per-node key file (same pattern as `--dynamo-auth
  PATH`); `SegmentStore`/`FsSegmentStore` (`lib.rs:581`) get the same
  treatment for backup, PITR, and stream objects.
- **Tests:** `assert_segment_store_contract` (`animus-env/src/test_support.rs`)
  encrypted round-trip; LSM crash corpus under `SimEnv` faults (a torn
  write must never partially decrypt).
- **ADR:** **yes** — new number; seam choice and key management.
- **PRs:** (1) key loading + `Disk` wrapper for WAL/engine; (2)
  `SegmentStore`; (3) operator key secret mount. **Size:** XL (interacts
  with the `Disk` seam's fsync/durability contract).
- **Depends:** sequence after S-02 to avoid three crypto ADRs in review at
  once.

### S-04 S3 `SegmentStore` backend (ADR 0059 deferred)

- **Gap:** `parse_segment_store`/`parse_backup_store`
  (`main.rs:575-601`) know only `dir:`, `fs:`, `cluster`.
- **Plan:** spike whether `sigv4.rs`'s signing-key chain can be exposed as
  a `sign_request` for a minimal PUT/GET/DELETE/LIST client (no
  `aws-sdk-s3` in tree today); `S3SegmentStore` behind the `prod` feature;
  `s3:` URIs on both flags; operator: document/restrict egress in
  `desired/networkpolicy.rs:99` (currently ingress-only, egress
  unrestricted by omission) plus credential secret.
- **Tests:** `assert_segment_store_contract` against minio, real-thread,
  `prod`-gated.
- **ADR:** amend 0059 in place (it reserves this exact follow-up).
- **PRs:** (1) client; (2) backend + flags; (3) operator egress +
  secrets. **Size:** L. **Blocks:** S-05.

### S-05 S3 export/import

- **Gap:** `ExportTableToPointInTime`, `DescribeExport`, `ListExports`,
  `ImportTable`, `DescribeImport`, `ListImports` absent.
- **Plan:** reuse the capture driver (`backup_restore.rs`, `dynamo.rs`
  `create_backup` ~1328, restore ~1672/~1978) against a customer
  `S3SegmentStore` handle; new wire handlers and manifest shape.
- **Tests:** extend `ANIMUS_BACKUP_SEEDS`/`ANIMUS_PITR_SEEDS` corpora.
- **ADR:** **yes** (0059 defers it as needing "a distinct wire model").
- **PRs:** (1) export trio; (2) import trio; (3) corpus. **Size:** L.
- **Depends:** S-04.

### S-06 Cluster knobs reachable from real deployments

- **Gap:** `--auto-split-bytes` and `--auto-split-change-rate` reach only
  `run_in_process_cluster`/`run_in_process_split_cluster`
  (`main.rs:1081,1154`); `run_single` (~684), `run_control` (~736),
  `run_data` (~804) never see them. The operator CRD has the fields but
  never emits them. `ClusterConfig` has no settings section at all.
- **Plan:** `ClusterConfig::cluster_settings: Option<ClusterSettings>`
  (auto-split, quiesce, orphan-sweep, stream-seal knobs) loaded on all
  three real paths, CLI flag conflict is a hard error exactly like
  `apply_dynamo_auth_flag`; operator mirror emits the section.
- **Files:** `crates/animusd/src/config.rs:143,150,316-330`,
  `main.rs`; `crates/animus-operator/src/desired/cluster_config.rs`,
  `crd.rs:107-121`.
- **Tests:** `config.rs` round-trip + conflict tests (~469-506); operator
  golden JSON.
- **ADR:** amendment notes on 0034, 0040, 0048.
- **PRs:** (1) config section + wiring; (2) operator mirror; (3) guide
  table sweep. **Size:** M. **Blocks:** W-09, S-07b.

### S-07 Operator hardening (ADR 0060 deferred list)

- **a. Operator image publish + `Cargo.lock`.** `image.yml` publishes
  `animusd` only. Add an operator image job; commit `Cargo.lock`, drop it
  from `.gitignore`, so the Docker build pins from it. Size S.
- **b. `backupStore`/`segmentStore` CRD fields** mirrored into the
  ConfigMap/entrypoint. Size M. Depends S-06.
- **c. `PodDisruptionBudget` builder** (`desired/poddisruptionbudget.rs`,
  pure-builder pattern + golden test). Size S.
- **d. `controlNodes` growth via the CRD**: controller drives ADR 0037
  `control/member/add` against a pod's admin port, mirroring
  `drain_and_remove_node`; extend `scripts/e2e-kind.sh` with a
  control-grow step. Size L.
- **e. Admission webhook** validating the CRD. Needs a webhook TLS cert,
  so after S-01. Size L.
- **ADR:** amend 0060 for a–c; d and e get their own section or a new
  number if the webhook design grows.

---

## 3. Core design items still proposed

### C-01 Evaluate writes at apply (ADR 0054)

- **Gap:** `rmw_lock` and evaluate-at-leader remain
  (`dynamo.rs:3154,5760`, `txn_coordinator.rs:57`, `lib.rs:6218`);
  contention-induced write refusals persist.
- **Plan:** follow ADR 0054's Decision and Sequencing sections: a
  self-contained entry evaluated in the `KindBatch` apply arm of
  `animus-cp-data`, results returned via a leader-local payload; keep the
  leader seatbelt as a double-check for one PR; then remove `rmw_lock` and
  the OCC precondition machinery.
- **Tests:** flip the concurrent-increment regression ADR 0054 cites;
  `animus-cp-data` apply-path sims; run C-04's `SimCluster` corpus first.
- **ADR:** none new; flip 0054 to Accepted on landing.
- **PRs:** (1) entry shape + apply-side single-item; (2) ADD/conditional
  cutover; (3) remove `rmw_lock`. **Size:** L. Land in isolation from the
  listener work in S-01.

### C-02 Heartbeat amortization (ADR 0044 phase 2)

- **Plan:** a per-node-pair heartbeat batcher below each `RaftCore` tick
  (precedent: `ProdEnv` pools one TCP connection per destination). First
  PR is investigation only: map every heartbeat send site
  (`HEARTBEAT_INTERVAL` users across `animus-control`'s driver and
  `animus-cp-data`'s host module).
- **Tests:** a `SimEnv` corpus asserting heartbeat count scales with
  node pairs, not groups.
- **ADR:** amendment on 0044. **PRs:** (1) map; (2) batcher behind a
  flag; (3) cutover. **Size:** L.

### C-03 Log-only replicas (ADR 0044 phase 3)

- Prerequisite only: needs its own ADR after C-02 ships and shows whether
  it is still needed. Not sized.

### C-04 Testability phases D and E (ADR 0061)

- **D1:** a `SimCluster` fixture over `ClientCtx<SimEnv>`
  (`lib.rs:6275`, already generic), reusing the existing multi-node
  `SimEnv` setup from `animus-control`/`animus-cp-data` tests and the B1
  seed harness in `animus-test`; then a first cycles/durability corpus.
  Size M. Land before C-01.
- **E1:** fake-kube-client harness for `controller.rs`'s `reconcile`,
  `control_nodes_changed`, `drain_and_remove_node`, reusing
  `desired/test_support.rs::test_cluster`. Size S–M.
- **ADR:** amendment notes on 0061.

### C-05 `SharedWal` (built, unwired)

- **Plan:** one benchmark PR (fsync count, many groups per node,
  post-quiescence). If no motivation remains, delete
  `crates/animus-control/src/shared_wal.rs` and its export
  (`lib.rs:67`), amend ADR 0028/0044. Otherwise wire it into
  `animus-cp-data`'s persist path with a cross-tablet ordering corpus.
- **Size:** S (delete) or L (wire). Recommendation: delete.

---

## 4. Operator surfaces: admin API, dashboard, console, CLI

Conventions (verified): a new admin route needs a match arm in
`crates/animus-node/src/admin.rs:36-79`, an `AdminHost` trait method, the
`FakeHost` stub in that file's tests, and a handler in
`crates/animusd/src/admin.rs`. A new dashboard tab needs a section in
`dashboard.html`, a `dashboard_X.js`, an `include_str!` in `dashboard.rs`,
a `<script>` tag, and a `ROLE_TABS` entry (`dashboard_core.js:538`).
Dashboard tests: `crates/animusd/tests/dashboard_endpoint.rs`; admin
tests: `admin_endpoint.rs`. CLI has no dedicated tests. Console widening:
`ConsoleBackend` in `crates/animus-node/src/console.rs` plus the
`animusd` impl, tests in `tests/console_*.rs`. The dashboard's only
mutation idiom is `postJSON("/admin/data/dynamo", {op, payload})` with a
`window.confirm` guard.

### U-01 Render-only dashboard fixes (no backend change)

- Transactions tab over `/admin/txns` (`CpTxnView`, `admin.rs:141-197`).
- Full per-group Raft detail (commit, durable, snapshot index, log len,
  voters, learners) in `renderTabletDetail` (`dashboard_tablets.js:133`).
- `believes_alive` badge in `renderOverview` (`dashboard_overview.js:12`).
- Sparklines from `/admin/metrics/history` (720-sample ring,
  `lib.rs:8264-8325`) as a shared component in `dashboard_core.js`, on
  Overview stat tiles; chart the six read-path counters there.
- `SYSTEM_TABLE_KINDS` (`dashboard_storage.js:56-66`) extended to all 16
  `EntityKind` variants (`syskv.rs:100-198`).
- **PRs:** one per bullet or one series. **Size:** M total.

### U-02 Backups tab

- View over `/admin/backups` + `/admin/restores` (`admin.rs:1265,1352`);
  gated Create/Delete/Restore/PITR enable-disable through the dynamo
  proxy (wire ops at `dynamo.rs:1030-1090`). Role-gated to control +
  combined like `placement`.
- **Tests:** `dashboard_endpoint.rs`; backend nets already exist.
- **PRs:** one. **Size:** M.

### U-03 Console per-table backup/PITR status

- `TableDetail` (`console.rs:182-190`) gains `pitr` and a trimmed
  `backups` list (id, status, created only — table-scoped, so allowed by
  ADR 0052's no-cluster-shape rule). One or two `ConsoleBackend` methods.
- **Tests:** `tests/console_table_config.rs`. **PRs:** one. **Size:** M.

### U-04 Data Browser: TTL row + one-call create-table form

- `#br-dy-ttl` next to `#br-dy-stream` (`dashboard.html:201`) via
  `Describe/UpdateTimeToLive`; `#br-dy-table-form` (`dashboard.html:162`)
  sends GSIs, LSIs, stream, TTL following `console::CreateTableRequest`'s
  two-call shape.
- **PRs:** two. **Size:** S + M.

### U-05 Control-plane visibility, lineage, and infrastructure actions

- Render `/admin/control/members` (`admin.rs:1754`) on the Node tab
  (near `#nd-mirror`, `dashboard.html:352`). Size S. Prerequisite for the
  control-member buttons.
- Lineage panel on the Tablets tab from `system-table?kind=split_lineage`
  / `split_placing`, keyed by the selected tablet. Size M.
- Gated buttons over existing POST routes: split/flush/compact/reconfigure
  on the tablet detail; drain/decommission/member add-remove on Node;
  control member add-remove on the new members panel. Document that the
  only gate is `window.confirm` plus tab visibility (ADR 0020 admin port
  is trusted-network). Three PRs by action family. Size L total.
- **Verify then add:** there is no standalone leadership-transfer admin
  route (`raft_view.transfer_target` is read-only; the CLI's
  `control-remove` arms a transfer internally). If confirmed, add
  `POST /admin/control/transfer` before its button. Size S.

### U-06 Config view fields (folds in auth and tracing views)

- Thread `backup_store`, `segment_store`, `quiesce_after`, `auth_enabled`
  + access key ids (never secrets), resolved OTLP endpoint into
  `AdminInfo` (precedent `auto_split_bytes_threshold`, `lib.rs:2178`) and
  `config_view` (`admin.rs:478-514`). Expect a compiler-driven fan-out
  across every `AdminInfo` literal.
- **Tests:** `admin_endpoint.rs`/`dashboard_endpoint.rs` config
  assertions. **PRs:** one. **Size:** M.

### U-07 New observability routes

- `GET /admin/backup-store`: store config, object counts (live `list`
  scan, debug posture, or a maintained counter in `BackupStoreHandle`),
  janitor phase via an `Arc<Mutex<JanitorProgress>>` on `ClientCtx`
  updated by `animus_node::backup_janitor`. Size L. Do first as the
  template.
- `GET /admin/ttl`: reaper cursor and deletes per tick from
  `animus_node::ttl_reaper`. Size L.
- `GET /admin/gc`: orphan-sweep phase from `segment_janitor_loop`
  (ADR 0024/0040). Size L.
- `GET /admin/segment-store`: placement per shard (already inside
  `ClusterSegmentStore`) plus counts. Size M.
- Each renders on the Backups tab (U-02) or Storage tab.

### U-08 CLI parity

- **(i) Flat GETs:** `backups`, `restores`, `txns`, `peers`,
  `control-members`, `system-table [--kind]`, `storage-scan`,
  `storage-control` as new arms in `run_admin` (`main.rs:185-266`) plus
  `ADMIN_USAGE`. Size S.
- **(ii) Dynamo-proxy wrappers:** `backup create|delete`, `restore`,
  `pitr enable|disable`, `ttl`, `stream`, each a `run_*` helper posting
  to `/admin/data/dynamo`. Size M.
- Trailing PRs add coverage for U-07's routes as they land.

---

## 5. Documentation

Root `CLAUDE.md`'s ADR 0060 sentence stays (operator image still
unpublished) until S-07a lands; `website/index.html` "Planned" pills stay
until S-01.

---

## 6. Deliberately not planned

- **Shrink / dilution of tablet count.** ADR 0044 records this as an
  accepted permanent cost; any fix is a from-scratch redesign that must
  first re-litigate the merge rejection in a new ADR. No PR without that.
- **Metadata as a real tablet (ADR 0039).** Gated on operational evidence
  that control-plane scale is the bottleneck and on ADR 0018 stability.
  Not a sizing question yet.
- **Global tables, on-demand/provisioned billing, Lambda triggers, DAX,
  CloudWatch, Kinesis destinations, Contributor Insights, replica
  auto-scaling.** Properties of the managed service, declared out of scope
  on `website/compatibility.html`. W-08 gives throttling without a
  billing meter.

---

## 7. Sequencing

Waves are ordered by risk and dependency, not importance. Items within a
wave are independent and can run in parallel.

| Wave | Items | Why here |
|---|---|---|
| 0 | C-05 (delete path), S-07a | Cheapest, unblock accurate PR descriptions and reproducible images |
| 1 | W-02, W-04, W-05, W-06, U-01, U-08(i), C-04 E1 | Small, ADR-free, no cross-deps |
| 2 | W-01, W-10, S-06, U-02, U-03, U-04, U-06 | Depends only on wave 1 |
| 3 | W-03 (ADR first), W-09, U-05, U-07, U-08(ii), C-04 D1 | W-03 after W-05; U-05 after its members panel; D1 before C-01 |
| 4 | C-01, S-01, S-02, W-08 | Highest blast radius; C-01 in isolation from S-01's listener changes |
| 5 | S-04 → S-05, S-07b–d, C-02 | S-05 strictly after S-04; S-07b after S-06 |
| 6 | S-03, S-07e, W-07, C-03 | XL or gated on earlier waves (webhook needs S-01) |

Open issues mapped: #375 → W-01, #319 → W-05. The flaky-test issues
(#280, #298, #418, #447, #539) are correctness work under the green
invariant, not roadmap items, and take precedence over any wave.
