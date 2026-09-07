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

The next free ADR number at the time of writing is **0069** (0065 is
[Per-table throttling](adr/0065-per-table-throttling.md), W-08's design of
record; 0066 is [SigV4 hardening](adr/0066-sigv4-hardening.md), S-02's;
0067 is [Throughput-derived minimum tablet count](adr/0067-throughput-derived-minimum-tablet-count.md),
W-08b's — all three landed 2026-09-05 and their roadmap sections are removed
per this document's own maintenance rule above; 0068 is
[S3 export and import](adr/0068-s3-export-import.md), S-05's design of
record — all three PRs (export trio, import trio, the `SimEnv` corpus)
landed 2026-09-06 and its roadmap section is removed the same way).

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
- **Status (2026-09-07):** PR 1, PR 2, and PR 3 landed. ADR
  [0071](adr/0071-partiql-subset.md) pins the grammar, the placeholder-only
  discipline, and the key-versus-filter lowering rule; concludes the W-01
  tokenizer does *not* generalise, a hand-written lexer instead. PR 2 adds
  `ExecuteStatement` with a PartiQL `SELECT` subset
  (`crates/animus-dynamo/src/partiql.rs`), lowered onto `Operation::Query`/
  `Operation::Scan` by building `animus-item::condition` types directly
  from the parsed AST (not by round-tripping through `wire.rs`'s string
  decoders), with an opaque versioned/statement-hashed `NextToken`. **PR 3**
  adds `INSERT`/`UPDATE`/`DELETE`, lowered onto real `Operation::PutItem`/
  `UpdateItem`/`DeleteItem` values and run through the exact same
  `run_operation` dispatcher a client-built request of that shape already
  uses (`animusd::dynamo::execute_statement`) — conditions, index
  maintenance, streams, and throttling are all inherited with no new
  write-path code. `INSERT`'s implicit `attribute_not_exists(pk)` maps a
  `ConditionalCheckFailedException` to a new `DuplicateItemException` (or
  swallows it under `ON CONFLICT DO NOTHING`); `UPDATE`'s implicit
  `attribute_exists(pk)` fails a missing item; `DELETE` has no implicit
  condition (matching plain `DeleteItem`'s own silent-no-op-on-missing-key
  semantics). **A conservative widening past PR 1's own original scope**:
  `RETURNING ALL OLD *`/`ALL NEW *` (`UPDATE`, both; `DELETE`, `ALL OLD`
  only) is implemented in PR 3 rather than deferred, since it costs nothing
  beyond wiring the statement's own clause onto the already-existing
  `ReturnValues`/`UpdateReturnValues` fields — see ADR 0071's "As-built: PR
  3" amendment. **PR 4** adds `BatchExecuteStatement`: 1 to 25 statements
  (AWS's own cap), each the identical `parse_statement`/per-kind lowering
  PR 2/3 already built, run independently through `execute_statement`
  (`INSERT`/`UPDATE`/`DELETE`) or a restricted `SELECT` path
  (`partiql::select_is_exact_key` — AWS limits a batch statement to a
  single-item operation, so a range/filtered `SELECT` is a per-statement
  error rather than lowering to a real `Query`/`Scan`) — no cross-statement
  atomicity, mirroring `BatchWriteItem`/`BatchGetItem`'s own contract, with
  a per-statement `AccessDenied` (not a whole-request rejection) on a
  denied table. See ADR 0071's "As-built: PR 4" amendment for the full
  response shape and error-code mapping. **PR 5** (`ExecuteTransaction`) is
  implemented too: 1–25 statements parsed with the identical grammar, all-
  `SELECT` lowered to one `TransactGet` per statement and run as one
  `TransactGetItems` (each `SELECT` must be an exact-key read — no index,
  no `ORDER BY`, no non-key `WHERE` term, since `TransactGetItems` has
  nothing else to lower onto), all-`INSERT`/`UPDATE`/`DELETE` lowered to
  one `TransactAction` per statement and run as one atomic
  `TransactWriteItems` (`ClientRequestToken` idempotency and per-statement
  `CancellationReasons` inherited unchanged from that existing machinery);
  mixing `SELECT` with a mutation is a `ValidationException`; a
  transaction statement's own `RETURNING`/`ON CONFLICT DO NOTHING` is
  rejected rather than honored, since `TransactWriteItems` reports no item
  image on a successful action and a transaction's condition failure
  always cancels the whole transaction — see ADR 0071's "As-built: PR 5"
  amendment. **The W-07 PartiQL train is now fully implemented and
  complete** — PRs 1–5 all landed; this gap entry is closed.

---

## 2. Security, storage, and deployment

### S-03 Encryption at rest

- **Status: complete — all 3 PRs landed** ([ADR 0069](adr/0069-encryption-at-rest.md),
  PR 1/2 2026-09-06, PR 3 2026-09-07) — key loading + a generic AEAD `Disk`-seam wrapper
  (`EncryptedDisk<D: Disk, R: Rng>`/`EncryptedEnv<E: Env>`,
  `crates/animus-env/src/encrypted.rs`), a per-node `--encryption-key
  PATH`/`RoleAddrs::encryption_key_path` key file (the `--dynamo-auth`/
  `--tls-cert` pattern), and a marker-file loud refusal on a key/directory
  mismatch. `ProdEnv` composes the same primitives internally rather than
  becoming `EncryptedEnv<ProdEnv>` — see the ADR's "Crate placement"
  section for why. **PR 2** adds the `SegmentStore`-seam sibling
  (`EncryptedSegmentStore<S: SegmentStore, R: Rng>`,
  `crates/animus-env/src/encrypted_segment_store.rs`), sealing each
  object as a whole standalone frame (reusing PR 1's frame codec) rather
  than an `EncryptedDisk`-style incremental one, wired into
  `--backup-store`/`--segment-store fs:PATH`/`s3://...` via `animusd`'s
  `build_segment_store`/`build_backup_store` — but under a
  **cluster-wide, not per-node, key** (every node sharing a `fs:`/`s3://`
  store must configure the identical key file), since a backup/PITR/
  stream-segment object is routinely read by a *different* node than the
  one that wrote it, unlike a `Disk` file. Both PRs reach `--config FILE
  --node I` and `--cluster N`; `--cluster-control`/`--cluster-data`,
  `animusd control`, `animusd data`, and `animusd join` do not yet accept
  the flag (a documented reach gap, the same shape several other per-node
  flags already have on those entry points).
- **PR 3 — operator key-secret mount, landed 2026-09-07**:
  `spec.encryptionKeySecretName: Option<String>` (`crates/animus-operator`)
  names a pre-existing `Secret` holding the raw key under one well-known
  data key (`"key"`), mounted read-only (`defaultMode` restricted) at
  `/etc/animus/encryption` on every pod and threaded into every node's
  `cluster.json` as `RoleAddrs::encryption_key_path` — mirroring
  `spec.tls`'s own `RoleAddrs.tls` wiring. Checked live against the API
  server (a `Secret` reference can't be validated from the spec alone,
  unlike `spec.tls`/`spec.s3`); a missing/malformed `Secret` surfaces an
  `EncryptionKeySecretInvalid` condition without stripping the field —
  see ADR 0069's own "As-built: PR 3" amendment for why falling back to
  "as if unset" here would be actively dangerous, not merely inert. No
  key rotation support (matching ADR 0069's own v1 scope): the config-hash
  restart annotation rolls pods on the field's *presence* changing, never
  on the same-named `Secret`'s content changing. **S-03 is now complete.**
- **Still open (tracked separately, neither closable from S-03's own
  scope):**
  - **Issue #680** — the default replicated `cluster` segment/backup store
    is not covered by PR 2 — its per-node local building block does its
    own raw filesystem I/O outside the `Disk` seam, so PR 1 never touched
    it either; only the `fs:`/`s3://` opt-in stores are sealed. Encrypting
    it would mean widening `ClusterSegmentStore`'s own concrete type
    parameter — a separate, structurally larger change than PR 2's own
    scope, tracked here rather than silently assumed done.
  - **Issue #676** — `animusd join`/`data --seed`/`--cluster-control`+
    `--cluster-data` don't thread `--encryption-key` (among several other
    per-node knobs) through to those entry points; a real gap for a
    hand-run cluster using them, irrelevant to the operator (which never
    generates those invocations).
- **Tests (PR 1):** `crates/animus-sim/tests/encrypted_disk.rs` (14 direct
  unit tests over `SimEnv`); `crates/animus-storage/tests/
  lsm_crash_encrypted.rs` (the crash/fault corpus sibling of
  `lsm_crash.rs`, depth knob `ANIMUS_LSM_ENCRYPTED_SEEDS`); `crates/
  animusd/tests/encryption_at_rest_e2e.rs` (real `ProdEnv`/disk/DynamoDB
  wire); `crates/animus-env/src/prod.rs`'s own real-filesystem unit tests.
- **Tests (PR 2):** the shared `SegmentStore` contract against
  `EncryptedSegmentStore<SimSegmentStore, SimEnv>`
  (`crates/animus-sim/src/segment_store.rs`),
  `EncryptedSegmentStore<FsSegmentStore, DiskSaltRng>`
  (`crates/animus-env/src/prod.rs`), and
  `EncryptedSegmentStore<S3SegmentStore<FakeS3>, R>`
  (`crates/animus-env/src/s3_store.rs`); a new fault-injection corpus,
  `crates/animus-test/tests/segment_store_encrypted_fault_corpus.rs`
  (depth knob `ANIMUS_SEGMENT_STORE_ENCRYPTED_SEEDS`, held at 20 — proves
  the wrapper tolerates every fault the backup/PITR/export-import domain
  corpora already inject through `SimSegmentStore`, without converting
  those three ~2,400-line corpora themselves, a named follow-up); and
  `crates/animusd/tests/encryption_at_rest_segment_store_e2e.rs` (real
  `ProdEnv`, a real 2-node cluster, `CreateBackup` on node 0 →
  `RestoreTableFromBackup` on node 1 with the same key, plus both
  mismatch directions refused at node startup).
- **Tests (PR 3):** unit tests over the operator's own fakes, mirroring the
  `dynamo_auth`/`tls` precedents (`crates/animus-operator/src/desired/
  cluster_config.rs`, `desired/statefulset.rs`, `controller.rs`); the
  `scripts/e2e-kind.sh` `E2E_ENCRYPTION=1` leg
  (`.github/workflows/e2e-kind.yml`'s `e2e-kind-encryption` job) — see
  ADR 0069's "As-built: PR 3" amendment for the full list and its own
  unverified-in-this-sandbox note.
- **ADR:** [0069](adr/0069-encryption-at-rest.md) — seam choice, key
  management, threat model, the positional torn-tail-vs-corruption rule,
  (PR 2 amendment) the cluster-wide key-scope decision, and (PR 3
  amendment) the operator's CRD field shape, mount, and failure semantics.
- **PRs:** (1) key loading + `Disk` wrapper for WAL/engine — **landed**;
  (2) `SegmentStore` — **landed**; (3) operator key secret mount —
  **landed 2026-09-07**. **Size:** XL (interacts with the `Disk` seam's
  fsync/durability contract). **S-03 is complete.**

### S-07 Operator hardening (ADR 0060 deferred list) — landed 2026-09-07, complete

- **e. Admission webhook** validating the CRD — landed 2026-09-07: a pure,
  shared validator (`crate::validate::validate_spec`) the reconciler's own
  condition-based fallback and a new opt-in `ValidatingWebhookConfiguration`
  both call, an HTTPS webhook server in the same `animus-operator` binary
  (`--webhook-addr`/`--webhook-cert`/`--webhook-key`), and its own TLS cert
  via a generalized cert-manager `Certificate` builder (`animus-operator
  webhook-cert`) or a hand-issued `Secret` — see [ADR 0070](
  adr/0070-operator-admission-webhook.md).
- **ADR:** e got its own number, [ADR 0070](
  adr/0070-operator-admission-webhook.md), per this section's own "or a new
  number if the webhook design grows" allowance — a shared validator, a
  TLS server, two cert paths, and a static manifest was more than a
  section-in-0060 could carry cleanly. (Item b — `backupStore`/
  `segmentStore` CRD fields for the non-S3 `cluster`/`fs:`/`dir:` forms —
  landed 2026-09-06, see ADR 0060's own "Amendment (2026-09-06): S-07b"
  section; `spec.s3` already covers the `s3://...` form, ADR 0060's S-04
  PR 3 amendment. Item c — quorum-derived `PodDisruptionBudget` builder, no
  CRD field added — landed 2026-09-06, see ADR 0060's own "Amendment
  (2026-09-06): S-07c" section. Item d — `controlNodes` growth via the
  CRD, driving ADR 0037 `control/member/add` one voter at a time and a
  config-hash pod-template restart mechanism — landed 2026-09-06, see ADR
  0060's own "Amendment (2026-09-06): S-07d" section.) **This closes
  S-07's whole item list (b/c/d/e all landed).**

---

## 3. Core design items still proposed

### C-03 Log-only replicas (ADR 0044 phase 3)

- C-02 (ADR 0044 phase 2, heartbeat amortization) landed 2026-09-06 —
  investigation, batcher, and default-on cutover all shipped (see that
  ADR's phase-2 and phase-2-cutover amendments). Whether phase 3 is still
  needed on top of it hasn't been assessed; prerequisite only, not sized.

### C-04 Testability phases D and E (ADR 0061)

- **D1 landed 2026-09-05** (`SimCluster`, a multi-node `ClientCtx<SimEnv,
  SimRelayClient<SimEnv>>` fixture with a real fault surface, plus
  `sim_cluster_corpus`, its first cycles/durability corpus over
  `animus-test`'s `check_cycles`/`check_durability`/`check_convergence`
  oracle, depth knob `ANIMUS_SIMCLUSTER_SEEDS`; ADR 0061's 2026-09-05
  amendments). D2-D4 remain open (an end-to-end DynamoDB-wire corpus, the
  `animusd` integration-suite migration, and deterministic coverage for
  auto-split/GC/join/backup-janitor).
- **E1 landed 2026-09-04** (`ClusterApi`/`AdminOps` seams in
  `animus-operator`, fake-driven `controller::tests`; ADR 0061's
  2026-09-04 amendment). **E2 not yet fully closed**, even though U-08 (its
  own planned home) landed in full 2026-09-06: U-08(i)'s eight flat GET
  arms and U-08(ii)'s six dynamo-proxy wrappers all now have their own
  `admin_request` unit tests, but several pre-existing one-shot mutating
  arms predating both — `drain`/`drain-status`/`remove`/`reconfigure`/
  `flush`/`compact`/`stream-grow` — still have none, so ADR 0061's own
  "741 currently-untested lines" framing isn't fully retired. A follow-up
  PR adding `admin_request` tests for exactly those arms (same shape as
  every test U-08 already added) would close it; not sized here.
- **ADR:** amendment notes on 0061.

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

U-08 CLI parity landed in full 2026-09-06: (i) landed 2026-09-04
(`admin_request` pure arg parser + eight flat GET arms in `animus-cli`);
(ii) landed 2026-09-06 (six dynamo-proxy wrappers — `backup-create`/
`backup-delete`/`restore`/`pitr-enable`/`pitr-disable`/`ttl`/`stream`,
each an `admin_request` arm posting `{op, payload}` to `/admin/data/dynamo`
with the exact wire shapes the dashboard already sends for these actions;
see ADR 0020's matching 2026-09-06 as-built note). Nothing outstanding
here.

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
  on `website/compatibility.html`. W-08 (landed 2026-09-05, ADR 0065) gives
  real per-table throttling in DynamoDB capacity units without a billing
  meter — `BillingMode`/`ProvisionedThroughput` are supported and enforced,
  but nothing here meters or invoices.

---

## 7. Sequencing

Waves are ordered by risk and dependency, not importance. Items within a
wave are independent and can run in parallel.

| Wave | Items | Why here |
|---|---|---|
| 1 | *landed 2026-09-04* (W-02, W-04, W-05, W-06, U-01, U-08(i), C-04 E1) | Small, ADR-free, no cross-deps |
| 2 | *landed 2026-09-04* (W-01, W-10, W-11, S-06, U-02, U-03, U-04, U-06) | Depends only on wave 1 |
| 2.5 | *landed 2026-09-05* (C-04 D1) | No cross-deps; run before C-01 |
| — | *landed 2026-09-05* (W-09) | Closed ADR 0034's deferred bullet ahead of wave 3 |
| — | *landed 2026-09-05* (W-08) | Per-table throttling (ADR 0065), all four steps |
| — | *landed 2026-09-05* (W-08b) | Throughput-derived minimum tablet count (ADR 0067), a direct W-08 follow-up |
| 3 | *U-05, U-07, U-08(ii) landed 2026-09-06* | No ordering constraint remains |
| 4 | *landed 2026-09-05* (S-02) | Highest blast radius (C-01 landed 2026-09-05 — see ADR 0054; S-01 landed 2026-09-05 — see ADR 0064; S-02 — see ADR 0066) |
| 5 | *S-04, S-05, S-07b–d, C-02, C-05 all landed 2026-09-06* | S-05 strictly after S-04 |
| 6 | *S-03 complete 2026-09-07 (all 3 PRs, ADR 0069)*; *S-07e/S-07 complete 2026-09-07 (ADR 0070)*; W-07, C-03 | XL or gated on earlier waves |

Open issues mapped: none left (#375 closed by W-01, #319 by W-05). Filed
from wave 2's own findings: #590 (the operator still emits the deleted
`--split-mode` flag) and #591 (a `control_only` relay-budget flake). The flaky-test issues
(#280, #298, #418, #447, #539) are correctness work under the green
invariant, not roadmap items, and take precedence over any wave.
