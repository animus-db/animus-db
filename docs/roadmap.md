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

The next free ADR number at the time of writing is **0065** (0064 is
[TLS on every port](adr/0064-tls-on-every-port.md), S-01 — landed in full
2026-09-05, no longer carried as a section below).

---

## 0. Corrections: the docs say "missing", the code says "built"

These are not feature gaps. They are prose that lags the code. Most of the
rows this section used to carry were fixed by the stale-prose sweep; what's
left is the one row below with nowhere in the docs to point a fix at, plus
the still-true paragraph after the table.

| Prose claim | Where | Reality |
|---|---|---|
| Read-path counters for ReadIndex vs eventual reads "missing" | (this audit's first pass) | `CpReadBarriersServed/TimedOut`, `CpEventualReads{Local,Forwarded,FellBack}` exist and surface in `/admin/metrics` |

---

## 1. Wire surface (DynamoDB API)

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
- **Depends:** W-05 landed 2026-09-04; `schema.rs`'s bridge now carries
  index key attribute types, so this can start at any time.

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
- **Depends:** soft: reuse W-01's `UpdateExpression` tokenizer (landed
  2026-09-04) if it generalises.

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
- **PRs:** one. **Size:** M. **Depends:** none (S-06 landed 2026-09-04: a new knob
  goes in `cluster_settings`).

---

## 2. Security, storage, and deployment

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

### S-07 Operator hardening (ADR 0060 deferred list)

- **b. `backupStore`/`segmentStore` CRD fields** mirrored into the
  ConfigMap/entrypoint. Size M. (S-06 landed 2026-09-04.)
- **c. `PodDisruptionBudget` builder** (`desired/poddisruptionbudget.rs`,
  pure-builder pattern + golden test). Size S.
- **d. `controlNodes` growth via the CRD**: controller drives ADR 0037
  `control/member/add` against a pod's admin port, mirroring
  `drain_and_remove_node`; extend `scripts/e2e-kind.sh` with a
  control-grow step. Size L.
- **e. Admission webhook** validating the CRD. Needs a webhook TLS cert —
  the prerequisite this used to be sequenced behind is done: TLS on every
  port ([ADR 0064](adr/0064-tls-on-every-port.md)) shipped in full,
  including this crate's own cert-manager `Certificate` builder and CRD
  shape (`spec.tls.certManager`) a webhook's own cert-issuance can reuse
  directly. No longer blocked; open to pick up on its own schedule. Size L.
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
  cutover; (3) remove `rmw_lock`. **Size:** L. S-01 (the listener work
  this used to be sequenced apart from) landed 2026-09-05 (ADR 0064) — no
  ongoing overlap to avoid any more, but this item itself is still open.

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
- **E1 landed 2026-09-04** (`ClusterApi`/`AdminOps` seams in
  `animus-operator`, fake-driven `controller::tests`; ADR 0061's
  2026-09-04 amendment). E2 (`animus-cli` coverage) stays folded into
  U-08's trailing PRs.
- **ADR:** amendment notes on 0061.

### C-05 `SharedWal` (built, unwired): keep, wire later

- **Measured 2026-09-02** (`SimEnv`, exact `Disk::sync` count,
  single-voter groups, throwaway harness not committed): a burst of one
  write to each of K active groups on one node costs K fsyncs, one per
  group's own WAL file, with no cross-group coalescing (K=1 → 1, K=32 →
  32). A burst of 32 writes to one group costs 1 fsync, so
  `persist_round.rs`'s group commit works but is scoped per group.
- **Why the earlier delete recommendation was wrong:** ADR 0048's
  "apply-poll term dominated" finding is about idle cost, which quiescence
  closes. `SharedWal` targets active-load cross-group fsync count, which
  quiescence never touches. `persist_round.rs`'s own doc names this
  shape (a split multiplying concurrently fsyncing groups) as the root
  of the issue #279 livelock.
- **Plan:** wire `SharedWal` into `animus-cp-data`'s persist path
  (`persist_wal` and the apply task's compaction rewrite) with a
  cross-tablet ordering corpus, segment GC, and crash-mid-roll fault
  injection. Gate the work on a `ProdEnv` wall-clock benchmark at
  realistic tablet density first: concurrent fsyncs to different files
  may already be cheap on some media.
- **Files:** `crates/animus-control/src/shared_wal.rs` (stays as is),
  `crates/animus-cp-data/src/lib.rs` persist path.
- **ADR:** amend 0028 on wiring. **PRs:** (1) `ProdEnv` benchmark;
  (2) wire behind a flag + corpus; (3) cutover. **Size:** L.

---

## 4. Operator surfaces: admin API, dashboard, console, CLI

Conventions (verified): a new admin route needs a match arm in
`crates/animus-node/src/admin.rs:36-79`, an `AdminHost` trait method, the
`FakeHost` stub in that file's tests, and a handler in
`crates/animusd/src/admin.rs`. A new dashboard tab needs a section in
`dashboard.html`, a `dashboard_X.js`, an `include_str!` in `dashboard.rs`,
a `<script>` tag, and a `ROLE_TABS` entry (`dashboard_core.js:538`).
Dashboard tests: `crates/animusd/tests/dashboard_endpoint.rs`; admin
tests: `admin_endpoint.rs`. CLI arg parsing is unit-tested via `admin_request`
(`crates/animus-cli/src/main.rs`); nothing opens a socket. Console widening:
`ConsoleBackend` in `crates/animus-node/src/console.rs` plus the
`animusd` impl, tests in `tests/console_*.rs`. The dashboard's only
mutation idiom is `postJSON("/admin/data/dynamo", {op, payload})` with a
`window.confirm` guard.

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
- Each renders on the Backups tab (landed with U-02) or Storage tab.

### U-08 CLI parity

- **(i) landed 2026-09-04** (`admin_request` pure arg parser + eight flat
  GET arms in `animus-cli`).
- **(ii) Dynamo-proxy wrappers:** `backup create|delete`, `restore`,
  `pitr enable|disable`, `ttl`, `stream`, each a `run_*` helper posting
  to `/admin/data/dynamo`. Size M.
- Trailing PRs add coverage for U-07's routes as they land.

---

## 5. Documentation

D-01 (the stale-prose sweep) and S-07a landed 2026-09-02; waves 1 and 2
landed 2026-09-04; S-01's own website update (moving its "Planned" pill to
"Works today" and correcting the "no TLS"/"trusted network" statements
across `index.html`, `architecture.html`, `how-it-works.html`, `docs.html`,
`install.html`) landed 2026-09-05 alongside ADR 0064's closing amendment.
Nothing outstanding here at present — add a row when the next
documentation-lagging-code gap turns up.

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
| 1 | *landed 2026-09-04* (W-02, W-04, W-05, W-06, U-01, U-08(i), C-04 E1) | Small, ADR-free, no cross-deps |
| 2 | *landed 2026-09-04* (W-01, W-10, W-11, S-06, U-02, U-03, U-04, U-06) | Depends only on wave 1 |
| 3 | W-03 (ADR first), W-09, U-05, U-07, U-08(ii), C-04 D1 | W-03 after W-05; U-05 after its members panel; D1 before C-01 |
| 4 | *S-01 landed 2026-09-05* (ADR 0064); C-01, S-02, W-08 remain | Highest blast radius |
| 5 | S-04 → S-05, S-07b–d, C-02, C-05 | S-05 strictly after S-04 |
| 6 | S-03, S-07e, W-07, C-03 | XL or gated on earlier waves (S-07e's webhook-TLS prerequisite is satisfied now that S-01 landed; no longer a hard gate, just unscheduled) |

Open issues mapped: none left (#375 closed by W-01, #319 by W-05). Filed
from wave 2's own findings: #590 (the operator still emits the deleted
`--split-mode` flag) and #591 (a `control_only` relay-budget flake). The flaky-test issues
(#280, #298, #418, #447, #539) are correctness work under the green
invariant, not roadmap items, and take precedence over any wave.
