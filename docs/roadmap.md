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
- **Issue #680 — closed 2026-09-07** (ADR 0069's "As-built: cluster
  store" amendment): the default replicated `cluster` segment/backup
  store — left uncovered by PR 2, since its per-node local building block
  did its own raw filesystem I/O outside the `Disk` seam — is now sealed
  under `--encryption-key` too, under the identical cluster-wide key PR 2
  already established. The widening PR 2's own scope-cut paragraph
  expected (a second generic parameter threaded through
  `animus-cp-data`'s own `cluster_segment_store` module) turned out
  unnecessary: `ClusterSegmentStore<E, S: SegmentStore>` was already
  generic over its local building block, so closing this was a single new
  `animusd`-local type (`LocalSegmentStore`) occupying that existing
  parameter, with zero changes to `animus-cp-data`. Tests: `crates/
  animusd/tests/encryption_at_rest_default_cluster_store_e2e.rs` (real
  `ProdEnv`, mirroring the `fs:` store's own e2e — a restore across
  nodes with no plaintext anywhere, and both loud-refusal directions).
- **Issue #676 — closed 2026-09-07** — `animusd join`/`data --seed` now
  thread `--encryption-key` (plus `--shared-wal`/`--heartbeat-batch`/
  `--quiesce-after`/`--segment-store`/`--backup-store`), and `animusd
  control` now threads `--encryption-key` too; `--cluster-control`+
  `--cluster-data` now threads `--quiesce-after`/`--heartbeat-batch`/
  `--shared-wal` (still rejects `--encryption-key` outright, the same
  posture `--tls-*` already has there — a real gap for a hand-run cluster
  using it, irrelevant to the operator, which never generates that
  invocation). See ADR 0028/0044/0048/0069's own 2026-09-07 amendments and
  `crates/animusd/CLAUDE.md`'s CLI reference for the full per-entry-point
  account.
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

### C-03 Log-only replicas (ADR 0044 phase 3) — assessed 2026-09-07: defer, not sized

- **Assessed 2026-09-07, recommendation: defer** (ADR 0044's matching
  2026-09-07 amendment has the full evidence). C-02 (heartbeat
  amortization) and C-05 (`SharedWal`) both landed and defaulted on
  2026-09-06, closing two of phase 3's three named per-group costs
  (heartbeat timers/frames, one WAL file per group) outright. The third —
  a per-tablet storage engine's idle footprint (ADR 0050) — measured at
  ~8.1 KB/engine this session (`cargo test -p animus-storage --test
  idle_engine_cost --features prod-heavy -- --ignored --nocapture`),
  under 0.4% of that test's own 2 MiB/engine gating ceiling: already
  negligible. The one unmeasured piece (per-group `RaftCore`/`RaftKvNode`
  in-memory bookkeeping and its one `drive` task, no RSS/CPU harness
  exists) is sized at roughly a day (M) to build and was not built here.
- **Not a "no," a design mismatch**: [ADR 0055](adr/0055-eventually-consistent-reads.md)
  (2026-08-23, after this ADR's original text) depends on *every* replica
  of a tablet carrying a full applied engine to serve `ConsistentRead:
  false` reads locally — the fix for v1's own "no read scaling at all"
  gap. A log-only replica, converted from an ordinary RF3 voter as phase
  3's own "asymmetric replicas" framing implies, would shrink exactly that
  read-scaling fan-out. If ever revisited, phase 3 needs re-scoping as
  extra log-only voters added *beyond* a full-copy read-serving quorum
  (RF > 3 for failure-domain spread, not a conversion of an existing
  replica), plus an ADR 0055 amendment excluding log-only members from its
  "any replica" read fan-out by construction.
- **Reopens on**: (a) the unmeasured per-group `RaftCore`/task cost, once
  measured, showing a real bite at a realistic per-node tablet density
  (hundreds to thousands of hosted groups — untested at that scale
  anywhere in this codebase); or (b) a deployment need for RF > 3 driven
  by failure-domain spread rather than read scaling. Neither holds today.
  No PRs planned.

### C-04 Testability phases D and E (ADR 0061)

- **D1 landed 2026-09-05** (`SimCluster`, a multi-node `ClientCtx<SimEnv,
  SimRelayClient<SimEnv>>` fixture with a real fault surface, plus
  `sim_cluster_corpus`, its first cycles/durability corpus over
  `animus-test`'s `check_cycles`/`check_durability`/`check_convergence`
  oracle, depth knob `ANIMUS_SIMCLUSTER_SEEDS`; ADR 0061's 2026-09-05
  amendments). **D2 landed 2026-09-07 (both PRs)**: PR 1 put six item
  operations (`PutItem`/`DeleteItem`/`GetItem`/`BatchGetItem`/`UpdateItem`/
  `BatchWriteItem`) plus a base-table-only `Query`/`Scan` through a new
  generic `dynamo::dispatch_item_op<E, R>` core, reachable against
  `SimCluster` via `SimClusterHandle::dynamo`/`SimCluster::dynamo`
  (`animus_dynamo::wire::decode_request` in, the same production dispatch
  handlers out) — proven by a first small `sim_cluster_dynamo.rs` smoke
  (five seed-parameterized scenarios). PR 2 built the actual corpus on top:
  `sim_cluster_dynamo_corpus.rs`, the same `Recorder`/`History`/
  `check_cycles`/`check_durability`/`check_convergence` model
  `sim_cluster_corpus` uses, driven entirely through the real DynamoDB JSON
  wire (list-append via `UpdateItem`'s `list_append`, `ConsistentRead:
  true`/`false` both exercised — only `true` feeds `check_cycles`, `false`
  is checked as a prefix of the converged final state — `DeleteItem`/
  `BatchWriteItem` via their own direct probes, base-table `Query`/`Scan`
  feeding multi-key reads into the same history), an `ANIMUS_DYNAMO_
  WIRE_SEEDS` depth knob (held green at `=25`), and a `corpus-deep.yml`
  tier (ADR 0061's 2026-09-07 amendments — see the second amendment for
  the full design and the read-consistency modeling decision). GSI/LSI
  `Query`/`Scan`, `TransactWriteItems`/`TransactGetItems`, and PartiQL
  remain out of scope, named as D2's own residuals for whichever rung
  generalizes those operations next. **D3 (the `animusd` integration-suite
  migration) is in progress: PR 1 landed 2026-09-07** — a planning pass
  classified `crates/animusd/tests/`'s 120 real-socket `ProdEnv` binaries
  (524 tests); this PR converted the "B class" (base-table DynamoDB logic
  reachable through `dispatch_item_op`, ~30 tests across ten
  `tests/dynamo_*.rs` binaries plus `kind_batch_outcome.rs`) to eleven
  `SimCluster`-driven `sim_cluster_dynamo_*`/`sim_cluster_kind_batch_
  outcome` modules in `src/` (`SimCluster::dynamo_concurrent` is this PR's
  own new fixture primitive, for racing several wire requests at one key —
  see `crates/animusd/CLAUDE.md`'s Tests section for the full module list
  and what stayed on `ProdEnv`). **The roadmap's own success criterion
  above ("the `prod-liveness` job shrinks enough to drop its 2-attempt
  retry") is stale and corrected here**: `.github/workflows/ci.yml`'s own
  comments show the retry loop was already replaced by nextest sharding
  (`prod-liveness-animusd` 4 partitions, plus `prod-liveness-hammer-pair`/
  `prod-liveness-scattered`) before this rung started — there is no retry
  left to drop. D3's real goal, and the one this and future PRs measure
  against, is shrinking the real-thread tier's own test count / wall time /
  flake surface. A parallel redundancy audit folded in two more removals
  from `dynamo_wire.rs`/`dynamo_throttling.rs` whose tests duplicated
  existing sim coverage outright (see those files' own doc comments).
  **PR 2a landed 2026-09-07**: base-table DDL (`CreateTable`/`DeleteTable`/
  `ListTables`/`DescribeTable`, still without `UpdateTable` — PR 2b) is now
  drivable through `SimCluster` over the real wire, via a new `dynamo::
  dispatch_table_op<E, R>` generic core (`dispatch_item_op`'s DDL sibling)
  and two `SimCluster` fixes this PR needed to make it actually work:
  `Metadata::members` population (`SimCluster::seed_members`, closing the
  gap PR 1's own amendment named) and `ClusterEdgeState::control` widened
  from a fixed `RaftNode<ProdEnv>` to `RaftNode<E>` (restoring
  `propose_schema`'s real leader-local fast path, and for the first time
  genuinely exercising its non-leader relay branch — see `animus-env`'s new
  `Env::merge_peer` default method, added to keep one existing `animusd`
  call site compiling generically). Two real, previously-unreachable fixture
  bugs were found and fixed along the way (a member seeded `Active` was
  flipping back to `Down` within 500ms with no heartbeat loop running; two
  independent tablet-id allocators could collide once both the hand-hosted
  and wire-provisioned paths were used in the same cluster) — see ADR
  0061's 2026-09-07 "D3 PR 2a" amendment for both incidents and `docs/
  engineering-lessons.md`'s matching entries. The reconciler-hazard
  investigation this PR's own task named found a **real, deterministic,
  documented gap**, left open rather than fixed: on a cluster with more
  nodes than `MAX_REPLICATION_FACTOR` (3), the control plane's own
  (correct) `rebalance_placement` can move a wire-provisioned tablet's
  replica set out from under `SimCluster`'s own minimal tablet-hosting
  watcher, which only ever adds a newly-named replica's own hosting and
  never tears down one a rebalance dropped — genuinely split-brain-shaped,
  reachable with a plain `CreateTable`, no fault injection needed, whenever
  `node_count > 3`. Every test added by this PR stays at `node_count <= 3`
  to avoid it; a proper fix needs `SimCluster` to grow real
  `Reconciler`-shaped teardown, named as this PR's own follow-up rather
  than attempted here. **PR 2b landed 2026-09-07**: `UpdateTable`'s own
  **throughput-only** change (`BillingMode`/`ProvisionedThroughput`, ADR
  0065) is now drivable through `SimCluster` too, via a widened
  `dynamo::update_table_throughput<E, R>` and a new `UpdateTable` arm on
  `dispatch_table_op` (still deliberately narrow — a stream/index change
  stays `unsupported_by_generic_dispatch`, exactly as PR 2a left it;
  `update_table`'s own production dispatch, and its stream/index-carrying
  callees, are untouched). Five of `dynamo_throttling.rs`'s eleven tests
  moved to a new `sim_cluster_dynamo_update_table.rs` (`CreateTable`/
  `UpdateTable` provisioning and lifting a throttle limit, raising units,
  `DescribeTable`'s `BillingMode`/`ProvisionedThroughput` rendering, and
  the follower-relay regression for `MetaCommand::SetTableThroughput`); the
  other six (batch shedding, `TransactWriteItems`, a forwarded-write
  throttle check, the `/admin/metrics` counter regression, and the
  cluster-wide `cluster_settings` config-surface test) stay on `ProdEnv` —
  none is reachable through `dispatch_item_op`/`dispatch_table_op` yet, and
  the metric-counter tests specifically need real counters this fixture's
  own `sim_cluster_throttle.rs` module doc already documents as never
  incrementing under `SimCluster` (every metric-recording site gates on
  `self.data.as_ref()`). No new fixture bugs found this time — both
  `SimCluster` fixes PR 2a needed (member-liveness heartbeating, the
  tablet-id-allocator collision) were sufficient. `crates/animusd/tests/
  auto_split_min_tablets.rs` was checked and deliberately left on `ProdEnv`
  — its `UpdateTable` call feeds the real auto-split-min-tablets
  background loop and tablet-forking reconciler, neither of which
  `SimCluster` has (it hand-hosts tablets, ADR 0061 rung D1's own design
  choice), so it is real-thread-liveness-shaped, not base-table-DDL-shaped.
  **PR 3a landed 2026-09-07**: GSI/LSI `Query`/`Scan` dispatch through
  `SimCluster`, plus `CreateTable` with a declared GSI/LSI. Eight functions
  (`run_index_query`/`run_gsi_query`/`run_lsi_query`/`run_index_scan`/
  `run_gsi_scan`/`run_lsi_scan`/`paginated_kind_examine`/`paginated_kind_
  examine_one`) widened to `<E, R>` — their only `ProdEnv`-binding was the
  concrete `&ClientCtx` parameter, every callee already generic;
  `dispatch_item_op`'s `Query`/`Scan` arms now route a named index through
  them instead of rejecting it, and `dispatch_table_op`'s `CreateTable` arm
  drops its GSI/LSI rejection (`create_table`/`index_to_control` already
  mint a declared index `Active` generically, nothing to backfill at create
  time; a stream declaration is still rejected). 42 new tests across nine
  `sim_cluster_dynamo_*` sibling modules — base/LSI `Query`/`Scan` filter,
  pagination, range comparators, `ScanIndexForward`, `Select`,
  `ConsistentRead` fidelity, consumed-capacity, item-collection-metrics —
  converted from `dynamo_query_filter.rs`/`dynamo_query_pagination.rs`/
  `dynamo_query_range.rs`/`dynamo_scan_index_forward.rs`/`dynamo_
  consistent_read.rs`/`dynamo_select.rs`/`dynamo_consumed_capacity.rs`/
  `dynamo_item_collection_metrics.rs`/`dynamo_indexes.rs`. **A GSI row is
  still never materialized under `SimCluster`** (no `index_drain::
  change_consumer_loop`), pinned by a new regression
  (`sim_cluster_dynamo_table_ops.rs::gsi_query_reads_empty_under_the_
  fixture_until_the_drain_generalizes`); every GSI-data test named above
  stays on `ProdEnv`, and `cross_index_cursor_mismatch_is_rejected`'s own
  `SimCluster` version drops one of its four sub-cases for the identical
  underlying reason one level earlier (`run_gsi_query`'s own empty-page
  gate fires before cursor validation and is unconditionally true here) —
  see ADR 0061's 2026-09-07 "D3 PR 3a" amendment and `crates/animusd/
  CLAUDE.md`'s Tests section.
  **PR 3b landed 2026-09-07, closing the GSI-drain gap PR 3a left
  open**: `index_drain::drain_tablet`/`reconcile_partition` widened to
  `<E, R>` (a pure signature change — every callee was already generic;
  proven behavior-identical by running the full pre-existing real-socket
  regression net against the widened code before trimming anything), plus
  a new `SimCluster::drain_gsi` fixture helper (`sim_cluster.rs`) that
  materializes a GSI's hidden table on demand by replicating `change_
  consumer_loop`'s own per-tablet guard sequence by hand (its `is_
  quiesced()`/`Building`-child skips are structurally unreachable under
  this fixture and are documented as such rather than replicated). Flips
  PR 3a's own boundary regression (`gsi_query_reads_empty_under_the_
  fixture_until_the_drain_generalizes` → `gsi_query_materializes_rows_
  after_a_drain`) and converts every GSI-data test PR 3a had to leave on
  `ProdEnv` — 12 tests across nine sibling modules (two new: `sim_cluster_
  dynamo_documents.rs`, `sim_cluster_dynamo_schema.rs`), plus a sim twin of
  `dynamo_indexes.rs::gsi_write_then_query` that does not replace the
  original (D2 PR 1's own real-socket proof of `run_operation`'s
  independent path). Seven `tests/dynamo_*.rs` files deleted whole
  (`dynamo_query_filter.rs`/`dynamo_query_pagination.rs`/`dynamo_query_
  range.rs`/`dynamo_scan_index_forward.rs`/`dynamo_select.rs`/`dynamo_
  documents.rs`/`dynamo_update_add_delete.rs`), one trimmed (`dynamo_
  schema.rs`, one of three tests removed). A small, unplanned fixture fix
  was needed too: `SimClusterHandle::leader_index_of` used to scan only
  the hand-hosted-table bookkeeping `SimCluster::create_table_with_
  replication` populates, which every wire-created table (every table this
  PR's own tests use) never gets an entry in — fixed to scan every node id
  instead. See ADR 0061's 2026-09-07 "D3 PR 3b" amendment and `crates/
  animusd/CLAUDE.md`'s SimCluster/Tests sections.
  D3's remaining classes (`UpdateTable`'s stream/index changes, Transact,
  PartiQL, Streams, TTL, admin/console/dashboard HTTP, TLS, SigV4,
  restart-durability, wall-clock timing) remain open. The GSI-drain gap
  specifically is closed as of PR 3b.
  **D3 closed 2026-09-07** (PRs #711, #716, #717, #718, #719, plus this
  CI/docs closing PR): the real-thread `tests/*.rs` tier shrank from 120
  files/521 tests to 100 files/418 tests while the deterministic
  `SimCluster` sim tier it fed grew from 5 to 29 modules (35 to 140
  tests); CI's `prod-liveness-animusd` sharding stays at 4 partitions (a
  per-shard cold-compile floor of roughly five minutes means fewer
  partitions would raise, not lower, the max shard wall time) and the sim
  tier now runs inside `gates` instead of riding along in the real-thread
  shards. See ADR 0061's 2026-09-07 "D3 closed" amendment for the full
  before/after numbers and the residual `tests/*.rs` inventory by class.
- **D4 PR 1 landed 2026-09-07, closing issue #715**: `SimCluster` is now
  hosted by a real per-node `animus_cp_data::host::Reconciler` — the exact
  gap D3 PR 2a found and left open (a rebalanced-away replica's
  `RaftKvNode` was never torn down, genuinely split-brain-shaped for
  `node_count > MAX_REPLICATION_FACTOR`). `spawn_policy_tablet_host_loop`
  (the add-only stand-in watcher) is deleted outright; `SimCluster::
  create_table_with_replication` no longer builds a `RaftKvNode` by hand
  either — both the hand-hosted and wire-provisioned paths now provision a
  tablet (`CreateTableSchema`/`CreateTablet`/`SetTabletPolicy`) and let the
  same real reconciler discover and host it, exactly like production.
  `SimCluster::restart` reuses the same node's own `MemoryTabletEngines`
  handle across a restart rather than wiping it, mirroring `reconciler_
  corpus.rs`'s own "durable engine survives a process crash" modeling — a
  deliberate behavior change from the pre-D4 restart. Zero production
  signature changes were needed (`ClusterEdgeState::unregister_raftkv`
  already existed). The former hazard test (`reconciler_hazard_fires_
  deterministically_when_node_count_exceeds_replication`) is now `every_
  node_hosts_exactly_its_replica_set_after_rebalance`, a convergence proof
  run at the original pinned seed plus ten more; one existing DDL test
  (`create_table_issued_on_a_control_follower_relays_and_converges`) was
  bumped from 3 to 4 nodes to prove the `node_count <= 3` restriction no
  longer applies; a general "no zombie groups" invariant was added to
  `sim_cluster_corpus.rs`'s end-of-scenario checks. No existing scenario
  changed behavior — every pre-existing cell's own tablet-hosting counts
  already stayed within ADR 0029's max−min ≤ 1 balanced band, so the real
  reconciler's own rebalance pass never had anything to move for them.
  `cargo test -p animusd --lib`: 306 passed / 118.4s before this rung, 307
  passed / 113.4s after (net +1: +2 new, −1 renamed, zero regressions — wall
  time within ordinary run-to-run noise of the baseline, once the
  reconciler's own fallback poll interval was tuned from an initial 50ms to
  200ms after a corpus-level measurement — see the ADR amendment).
  What remains for D4 (PRs 2-5): auto-split's own byte trigger needs
  `auto_split_loop`'s `tokio::time` conversion to run under `SimEnv`; GC
  reclaim is already a reconciler action, awaiting only a `drop_table`
  driver reachable from this fixture; join/growth needs an add-node
  capability; the backup janitor needs `client_ctx_host.rs`'s impls
  widened. **PR 1 landed 2026-09-07, closing issue #715; PRs 2-5 remain
  pending.** See ADR 0061's matching 2026-09-07 "D4 PR 1" amendment and
  `crates/animusd/CLAUDE.md`'s own entry for the full account.
- **E1 landed 2026-09-04** (`ClusterApi`/`AdminOps` seams in
  `animus-operator`, fake-driven `controller::tests`; ADR 0061's
  2026-09-04 amendment). **E2 landed 2026-09-07**: the seven pre-existing
  one-shot mutating arms predating both U-08(i) and U-08(ii) —
  `drain`/`drain-status`/`remove`/`reconfigure`/`flush`/`compact`/
  `stream-grow` — now each have their own `admin_request` unit tests
  (happy path + the argument-error paths the parser already has, same
  shape as every test U-08 already added); all seven already built their
  request through `admin_request`, so no dispatch refactor was needed.
  `cargo test -p animus-cli` went from 61 to 78 passing (ADR 0061's
  2026-09-07 amendment).
- **ADR:** amendment notes on 0061.

### C-06 Transact/PartiQL SimCluster dispatch

- **Gap:** the two named D2 residuals — `TransactWriteItems`/
  `TransactGetItems` and `ExecuteStatement`/`BatchExecuteStatement`/
  `ExecuteTransaction` (PartiQL) — are unreachable through the generic
  `dispatch_item_op`/`dispatch_table_op` core `SimCluster` drives, so their
  own `crates/animusd/tests/` binaries stay real-socket `ProdEnv` forever
  unless a future rung generalizes them. Per the D3-closing residual
  inventory (`docs/adr/0061-*.md`), that's 6 files/32 tests for Transact
  and 2 files/37 tests for PartiQL — the largest and third-largest of
  Class D's thirteen unowned groups, and, unlike the other eight groups
  D4 left open, both were named as out-of-scope *by design* from D2 PR 1
  onward rather than newly discovered.
- **Plan:** the identical widen-to-`<E: Env, R: RelayClient>`-then-add-a-
  parallel-generic-entry-point template D3/D4 already validated four times
  (D3 PR 2a/2b/3a, D4 PR 2/5), applied to Transact's handlers/idempotency
  helpers and to new PartiQL siblings of `execute_statement`/
  `execute_transaction`/`run_batch_execute_statement`. See
  [ADR 0061](adr/0061-testability-node-crate-simulator.md)'s 2026-09-07
  "Rung F" amendment for the full account, including the two real
  wrinkles (`ensure_txn_idempotency_table`'s six wall-clock sites; the
  PartiQL write path's recursion into concrete production dispatchers,
  which forces new parallel `_as` siblings rather than a widening of
  `run_operation` itself).
- **PRs:** a seven-PR series — (1) this docs opener; (2) Transact
  groundwork (widen nine functions, convert the six wall-clock sites);
  (3) Transact reachable from `SimCluster` (`execute_item_op_as` routes
  both operations; a new `sim_cluster_dynamo_transact.rs`, ~6-8 scenarios
  including the idempotency-table bootstrap race between two first
  callers); (4) Transact in the wire corpus (`sim_cluster_dynamo_corpus.rs`
  gains a list-append `TransactWriteItems` op and a `TransactGetItems`
  probe, `ANIMUS_DYNAMO_WIRE_SEEDS=25` once); (5) PartiQL siblings
  (`execute_statement_as`/`execute_transaction_as`/
  `run_batch_execute_statement_as` over the pure lowering + generic
  dispatch, ~4 signatures); (6) PartiQL sim tests (~8-10 scenarios,
  SELECT/INSERT/UPDATE/DELETE/batch/ExecuteTransaction, plus an optional
  corpus equivalence cell); (7) docs close-out. Every production dispatch
  path (`run_operation`, `execute_statement`, `execute_transaction`,
  `run_batch_execute_statement`, `execute_one_batch_statement`) stays
  byte-identical throughout — strictly additive, parallel new paths only,
  per the D2 PR 1 lesson (`docs/engineering-lessons.md`).
- **Status:** PR 1 (docs) and PR 2 (Transact groundwork) landed
  2026-09-07. **PR 3 (Transact reachable from `SimCluster`) landed
  2026-09-07**: `dispatch_item_op` gained the two match arms, and a new
  `sim_cluster_dynamo_transact.rs` covers 7 scenarios (12 of 14 tests
  green — commit + `ConditionCheck` across two tables, cancellation
  reasons, `ClientRequestToken` idempotency, a `TransactGetItems` snapshot
  against a concurrent writer, forwarding from a non-participant node, and
  the idempotency-table bootstrap race this section's own Plan named up
  front, all with no product bug found). **One real finding**: the
  scenario proving atomic recovery after a coordinator crash
  (`coordinator_never_finished_past_prepare_recovers_atomically`) found a
  structural deadlock in `animus_node::sim_relay::SimRelayClient` (a
  shared testing primitive, a different crate) when a forwarded request's
  own handler needs a nested outbound relay call, not a bug in the
  Transact dispatch or coordinator logic itself. See ADR 0061's matching
  2026-09-07 "C-06 PR 3" amendment for the full diagnosis. **Issue #731,
  fixed 2026-09-07**: `SimRelayClient::serve_loop` now dispatches each
  inbound forwarded request onto its own task instead of awaiting it
  inline (`crates/animus-node/src/sim_relay.rs`, mirroring
  `AnimusdRelayClient`'s own one-task-per-connection shape) — confirmed
  directly: the scenario's own poll loop no longer returns the relay
  timeout text at any seed tried. **A second, distinct, pre-existing bug
  the fix itself uncovered — in `ClientCtx::txn_recover`'s non-local
  grace-check, unrelated to and unmodified by the relay fix — used to
  still block the scenario from converging; filed as issue #737 and fixed
  2026-09-07**: both `txn_recover` call sites now share one clock-read
  helper (`recovery_grace_now_ms`, `crates/animusd/src/txn_coordinator.rs`)
  that reads an absolute `env.now()` on every route, never the near-zero
  elapsed-duration read the non-local branch used to compute. `coordinator_
  crash_after_prepare_recovers_atomically_to_commit` (renamed from
  `coordinator_never_finished_past_prepare_recovers_atomically`) and its
  `_over_seeds` sibling are un-ignored and green at the pinned seed
  `0xC06F_0007` (= `3228499975`) and the five-seed loop. See ADR 0061's
  "#731 closed" and "#737 closed" addenda and `docs/engineering-lessons.md`'s
  matching entries. Both issues #731 and #737 are now closed. **PR 4
  (Transact in the wire corpus) landed 2026-09-08**: `sim_cluster_dynamo_
  corpus.rs`'s randomized op mix gained `TransactWriteItems` (two owned
  keys, `Update`+`list_append`, plus an always-passing `ConditionCheck`,
  sometimes tokened) and `TransactGetItems` (any two keys, always feeding
  the shared history — real DynamoDB gives it no `ConsistentRead`), riding
  the identical 8-cell fault matrix every other op already does; atomicity
  is checked by construction (both appends enter history as one atomic
  entry) and isolation with single-item ops falls out of the shared
  history for free. A dedicated deterministic probe
  (`run_transact_probe`) proves the two shapes the randomized workload
  can't safely produce: a genuinely failing `ConditionCheck` (all-or-
  nothing) and `ClientRequestToken` idempotency/mismatch. **Three real
  `SimCluster`/`ClientCtx` fixture bugs found and fixed, all classified
  (b) — a fixture/model gap, never a defect in the transact protocol**:
  (A) a stale `SimClusterHandle::replicas_of` snapshot going stale the
  moment a tokened transact write's idempotency-table bootstrap shifts
  global tablet rebalance, fixed via a live `hosted_tablets`-backed
  `live_replicas` query; (B) a same-tablet ("self-transaction") abandoned
  transact write permanently masked under `local_get`'s raw-peek
  semantics with no resolver loop in this fixture, fixed by issuing one
  covering `TransactGetItems` after drain (`force_resolve_all_keys`,
  which resolves a local intent the same way `TransactGetItems`'s own
  read path resolves a foreign one); (C) `SimCluster::restart` never
  updating a restarted node's own `ClusterEdgeState::control` registry,
  permanently breaking that node's `propose_schema` fast path for any
  *new* schema after a restart — fixed via a new `replace_control` (not
  `register_control`, which only appends and, tried first, left the
  identical scenario failing with a stale handle shadowing the fresh one).
  See `sim_cluster_dynamo_corpus.rs`'s own new "Corpus-fixture findings"
  module-doc section for the full account of each, including the exact
  seeds. **A resource-scale finding was filed here as "not fixed, not
  investigated further"; it has since been root-caused and fixed by PR
  #753, outside this PR's own scope at the time.** The OOM was never
  glibc allocator retention (this PR's own original framing) — it was a
  genuine per-test reference-cycle leak in `animus-sim`'s task queue plus a
  second, independent one in `animus-node`'s `SimRelayClient` handler slot,
  both fixed 2026-09-08 (PR #753) ahead of PR 6's own full-suite gate,
  which depended on them. See ADR 0061's two 2026-09-08 leak-fix
  amendments and this section's own PR 7 close-out below for the fix and
  its measured numbers. **PR 5 (PartiQL siblings) landed 2026-09-08**:
  four new `<E: Env, R: RelayClient>`-generic siblings —
  `execute_statement_as`/`execute_transaction_as`/
  `run_batch_execute_statement_as`/`execute_one_batch_statement_as` — plus
  three new `dispatch_item_op` match arms routing
  `ExecuteStatement`/`BatchExecuteStatement`/`ExecuteTransaction` to them.
  `run_operation`, `execute_statement`, `execute_transaction`,
  `run_batch_execute_statement`, and `execute_one_batch_statement` stayed
  byte-identical throughout (confirmed by the unmodified 37-test
  `dynamo_partiql.rs`/`dynamo_execute_transaction.rs` real-socket suite
  staying green against the widened code). A second mutual-recursion
  cycle, distinct from `execute_statement`'s own pre-existing one, was
  found by the compiler once `dispatch_item_op` gained its new
  `ExecuteStatement` arm — closed with one `Box::pin` in the new
  `dispatch_lowered_write_as` helper. A new `sim_cluster_dynamo_partiql.rs`
  covers 5 reachability scenarios (10 tests): `INSERT` then `SELECT`,
  `UPDATE`/`DELETE` with `RETURNING`, a mixed `BatchExecuteStatement`
  running in order, an `ExecuteTransaction` commit across two tables, and
  a condition-failed cancel — each issued from a non-leader node. No
  product bug found. See ADR 0061's matching 2026-09-08 "Rung F, PR 5"
  amendment for the full account. **PR 6 (PartiQL sim tests) landed
  2026-09-08**: 27 more `SimCluster` siblings, one per named real-socket
  LOGIC test in `dynamo_partiql.rs`/`dynamo_execute_transaction.rs`
  (statement semantics, error mapping, pagination shape, index routing,
  cancellation reasons), each issued from a non-leader node of a 3-node
  RF3 `SimCluster` with an `_over_seeds` sibling at 5 seeds (54 tests
  total) — pure test authorship over PR 5's own dispatch, no new
  `dynamo.rs` mechanism. No product bug found; the real-socket 37-test
  suite stays untouched and green throughout. Deliberately not converted:
  `throttled_table_throttles_a_partiql_insert` (throttle-window timing),
  two tests already subsumed by PR 5's own scenarios (`insert_then_
  select_sees_it`, `delete_with_returning_all_old`), and every
  `batch_execute_statement_*`/`delete_*` test beyond PR 5's own scenario
  (c) — not named in this PR's own candidate list, left for a future pass.
  See ADR 0061's matching 2026-09-08 "Rung F, PR 6" amendment for the full
  account.

  **PR 7 (this docs close-out) landed 2026-09-08, closing C-06.** The full
  seven-PR series: PR 1 docs opener (#728); PR 2 Transact groundwork
  (#729); PR 3 Transact reachable from `SimCluster` (#732), which surfaced
  and closed two issues of its own — the `SimRelayClient` nested-relay
  deadlock (#731, fixed by #738) and `txn_recover`'s non-local grace-check
  wall-clock bug (#737, fixed by #740); PR 4 Transact in the wire corpus
  (#748); PR 5 PartiQL siblings (#750); PR 6 PartiQL sim tests (#756); PR 7
  this close-out. Across PRs 3-6, `cargo test -p animusd --lib` grew from
  353 passed (pre-PR-3 baseline) to 438 passed / 3 ignored (post-PR-6),
  entirely additive `SimCluster` coverage with zero regressions at any
  step. Every production dispatch path (`run_operation`, `execute_
  statement`, `execute_transaction`, `run_batch_execute_statement`,
  `execute_one_batch_statement`) stayed byte-identical throughout, per this
  section's own non-goals — confirmed the whole way by running the
  unmodified real-socket suites against the widened code.

  **`crates/animusd/tests/dynamo_partiql.rs` and `dynamo_execute_
  transaction.rs` are kept in full, not trimmed or deleted.** They remain
  the `ProdEnv` proof that `run_operation`'s own production dispatch path —
  never replaced, only paralleled by the new generic siblings — actually
  matches what the generic `SimCluster` siblings exercise; deleting them
  would leave the byte-identical claim above unverifiable on every future
  change. This mirrors D3 PR 3b's own decision to keep `dynamo_indexes.
  rs::gsi_write_then_query` whole rather than convert or delete it. See ADR
  0061's "Rung F closed" amendment for the full account.

  **What remains unowned after C-06**, per the D3-closing residual
  inventory (`crates/animusd/CLAUDE.md`'s Tests section) with Transact and
  PartiQL now removed from it: admin/console/dashboard HTTP, **Streams
  (now owned by C-07, opened 2026-09-08 — see that entry below)**, TTL,
  the control/data role split, `--config` bring-up, index DDL beyond
  plain `CreateTable`, node assembly/raw `ClientRequest`, and the
  throttle-metric counters. None of the other seven groups has a rung
  against it today.
- **ADR:** [0061](adr/0061-testability-node-crate-simulator.md) — the
  2026-09-07 "Rung F" amendment (see its 2026-09-08 "Rung F, PR 4"/"Rung F,
  PR 5"/"Rung F, PR 6" addenda for PR 4/5/6's own accounts, and its "Rung F
  closed" amendment for PR 7's own close-out).
- **Size:** L (seven PRs, two real production functions' worth of
  `ProdEnv`-only surface to widen plus two new fault-injecting sim
  suites).
- **Depends:** C-04 (closed 2026-09-07 — D4 PR 1's real per-node
  `Reconciler` and D3's `dispatch_item_op`/`dispatch_table_op` cores are
  both load-bearing prerequisites this rung builds directly on).
- **Status (2026-09-08):** closed. All seven PRs landed; see the PR 7
  close-out above for the full accounting.

### C-07 Streams SimCluster dispatch

- **Gap:** Streams is the residual the D3-closing inventory and the C-06
  close-out both name as next in line — the largest remaining unowned
  group after admin/console/dashboard HTTP. A read-only pass over this
  tree finds four real-socket `crates/animusd/tests/*.rs` files touching
  it: `dynamo_streams.rs` (15 tests), `stream_janitor.rs` (11 tests),
  `console_stream.rs` (4 tests), and `stream_backfill_seed_filter.rs` (2
  tests). The D3-closing inventory's own "Streams (3/28)" figure is three
  of these four (`dynamo_streams.rs` + `stream_janitor.rs` +
  `stream_backfill_seed_filter.rs` = 28); `console_stream.rs`'s 4 tests
  are already filed under the separate admin/console/dashboard HTTP
  group. `tests/streams_e2e.rs` (12 tests) is explicitly out of scope
  throughout — frozen behind the open flake issue #298 (joined by #745
  for this file), owned by whichever agent/issue closes those, never this
  series.
- **Plan:** the identical widen-to-`<E: Env, R: RelayClient>`-then-add-a-
  parallel-generic-entry-point template D3/D4/C-06 already validated five
  times, applied to `dynamo_streams.rs`'s eight read-path functions, the
  stream-only `UpdateTable` sub-arm of `dispatch_table_op`, on-demand
  shard sealing, and the segment janitor's tick/loop. The key reachability
  finding: `dynamo.rs::execute_routed_as` forks on the `X-Amz-Target`
  prefix *before* decoding into an `Operation` at all, so the generic
  `execute_item_op_as`/`dispatch_item_op` core `SimCluster` already drives
  can never reach a `DynamoDBStreams_20120810.*` target — a parallel
  `dynamo_streams::execute_streams_op_as`/`SimClusterHandle::
  dynamo_streams` pair is needed, mirroring the existing
  `SimClusterHandle::dynamo`. See
  [ADR 0061](adr/0061-testability-node-crate-simulator.md)'s 2026-09-08
  "Rung G" amendment for the full account, including the precise
  file/function blockers and the per-file scenario disposition (what
  converts, what stays `ProdEnv` and why).
- **PRs:** a six-PR series — (1) this docs opener; (2) groundwork (widen
  `disable_stream`, a stream-only `UpdateTable` arm on
  `dispatch_table_op`, `SegmentStoreHandle::S3` over a shared
  `SimSegmentStore` in `SimCluster`, a new `SimCluster::
  drive_stream_seal(node)`); (3) the Streams read API reachable (widen
  `dynamo_streams.rs`'s eight functions, `execute_streams_op_as` +
  `SimClusterHandle::dynamo_streams`, a new `sim_cluster_dynamo_
  streams.rs` with ~8 scenarios); (4) `dynamo_streams.rs`'s 12
  sim-convertible tests converted to `sim_cluster_dynamo_streams*.rs`
  siblings, source trimmed to the two that stay `ProdEnv`; (5) the
  segment janitor widened to `<E, R>` and spawned unconditionally under
  `SimCluster` (the D4 PR 5 backup-janitor shape), covered by
  `SimCluster`'s own `Drop`/`restart` shutdown path per issue #753, a new
  `sim_cluster_stream_janitor.rs` with ~9 scenarios; (6) docs close-out.
  Every production dispatch path (`execute_routed_as`, `execute_as`,
  `run_operation`, `dynamo_streams::execute_as`) stays byte-identical
  throughout — strictly additive, parallel new paths only, per the D2
  PR 1 lesson (`docs/engineering-lessons.md`).
- **Size:** M (six PRs, one real production dispatcher's worth of
  `ProdEnv`-only surface to widen plus two new fault-injecting sim
  suites).
- **Depends:** C-04 (closed), C-06 (closed) — the D3/D4 generic dispatch
  cores this rung builds directly on, plus C-06's own Transact widening,
  which is what makes `dynamo_streams.rs`'s Transact-on-a-streamed-table
  pair convertible at all.
- **Status:** open 2026-09-08, PR 1 (this opener).

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
| 6 | *S-03 complete 2026-09-07 (all 3 PRs, ADR 0069)*; *S-07e/S-07 complete 2026-09-07 (ADR 0070)*; *C-03 assessed 2026-09-07 — deferred, no PRs planned (see ADR 0044's matching amendment)*; W-07 | XL or gated on earlier waves |
| 7 | C-06 (closed 2026-09-08 — all seven PRs landed: #728, #729, #732, #748, #750, #756, plus this PR; issues #731 and #737 both fixed 2026-09-07) | Gated on C-04 (closed 2026-09-07) — the D4 `Reconciler` and D3 generic dispatch cores it builds on |
| 8 | C-07 (open 2026-09-08 — PR 1, this docs opener, landed) | Gated on C-04 (closed) and C-06 (closed) — the same generic dispatch cores, plus C-06's own Transact widening |

Open issues mapped: none left (#375 closed by W-01, #319 by W-05). Filed
from wave 2's own findings: #590 (the operator still emits the deleted
`--split-mode` flag) and #591 (a `control_only` relay-budget flake). The flaky-test issues
(#280, #298, #418, #447, #539) are correctness work under the green
invariant, not roadmap items, and take precedence over any wave.

---

**Addendum, 2026-09-07 (C-04 D4 PR 3 landed)**: dropped-table GC (ADR 0024)
now has deterministic `SimCluster` coverage —
`crates/animusd/src/sim_cluster_dynamo_drop_table.rs`, driven through the
real `DeleteTable` wire operation (already `<E, R>`-generic since D3 PR 2a,
no new widening needed). Four of five scenarios converge cleanly (base
table, GSI cascade, create-then-immediately-drop race, drop off a
rebalanced replica set); the fifth (a node crashed while hosting the table,
restarted after the drop) found a real, previously-uncharacterized reclaim
gap — `host::Reconciler::gather_facts` derives every fact solely from
`Metadata`'s current tablet map, so a node offline across a drop's whole
commit-and-converge window can never rediscover, and therefore never
reclaims, its own leftover tablet engine on restart. Kept as one
`#[ignore]`d regression rather than fixed (out of this PR's own
driver-plus-assertions scope); see ADR 0061's matching 2026-09-07 "D4 PR 3"
amendment for the full account and `crates/animusd/CLAUDE.md`'s SimCluster
section for the pointer. `crates/animusd/tests/drop_table_gc.rs` and
`drop_table_index_cascade.rs` stay whole and unconverted — every test in
both interleaves real-disk WAL-file assertions with metadata/hosting
convergence in one body, which this fixture's `MemoryEngine` tier cannot
stand in for. What remains open for D4 (PRs 2, 4, 5, unchanged): auto-split
needs `auto_split_loop`'s own `tokio::time` conversion; join/growth needs
an add-node capability; the backup janitor needs `client_ctx_host.rs`'s
impls widened.

---

**Addendum, 2026-09-07 (issue #722 closed, D4 PR 3's own finding fixed)**:
the crashed-during-the-drop reclaim gap the addendum above reports (a node
offline across a table drop's whole commit-and-converge window never
rediscovering, and therefore never reclaiming, its own leftover tablet
engine on restart) is fixed — `animus_cp_data::host::EngineFactory` gained
`local_tablets()`, a second, restart-surviving fact source the reconciler
consults exactly once per process lifetime (its very first tick), folded
into `plan`'s existing reclaim path via a `known`-set safety argument (an
engine only ever exists locally for a tablet id this node has itself
observed as real, so an id absent from both the current tablet map and
every live in-place-split intent's own children is always a genuine
leftover). The formerly-`#[ignore]`d `SimCluster` regression is now a
positive assertion, and a matching real-disk regression landed in
`crates/animusd/tests/drop_table_gc.rs`. See ADR 0024's and ADR 0061's
matching 2026-09-07 amendments for the full account, and
`crates/animus-cp-data/CLAUDE.md`'s host-module entry for the mechanism as
shipped. This closes the one open item D4 PR 3 itself deliberately left
outstanding; D4 PRs 2, 4, 5 remain open, unchanged by this fix.

---

**Addendum, 2026-09-07 (C-04 D4 PR 2 landed)**: the auto-split BYTE
trigger's own `tokio::time` residual D4 PR 1 named is closed —
`auto_split_loop` is now `<E: Env, R: RelayClient>`-generic (`Nanos`-keyed
cooldown/confirm maps instead of `tokio::time::Instant`), `index_drain::
{inplace_split_driver_tick, gsi_caught_up}` widened the same way so
`SimCluster::drive_inplace_split_cutover` can manually drive the fork's
own `MetaCommand::CutoverSplit` (this fixture still never spawns
`change_consumer_loop` as a background loop), and `SimCluster::
set_auto_split_thresholds` (defaulted off, mirroring D4 PR 1's own
`heartbeat_loop` spawn) is the new opt-in knob. Five deterministic
scenarios in the new `sim_cluster_auto_split.rs`, replayed at 5 seeds each
(10 tests): a byte-threshold crossing forking exactly once with every
pre-split key still wire-readable; staying below threshold over a long
window; a regrown child forking again after modest writes don't; a
leadership move mid-window still yielding exactly one fork; a crashed-and-
restarted node converging via the issue #722 fix. `cargo test -p animusd
--lib`: 321 → 331 (+10, 0 regressions). Two `cp_plane.rs` tests removed in
favor of the new sim scenarios (`tablet_auto_splits_when_it_grows`,
`already_split_tablet_splits_again_once_it_regrows`); four ProdEnv
tests/files stay for stated reasons (a skewed-value-size quantitative-
balance claim the sim scenarios don't reproduce, the manual raw-key split
path, two Streams-specific tests, one real-thread-paced-writer-timing
test) — see ADR 0034's and ADR 0061's matching 2026-09-07 amendments for
the full account. What remains open for D4: PR 4 (join/growth needs an
add-node capability) and PR 5 (the backup janitor needs `client_ctx_
host.rs`'s impls widened).

---

**Addendum, 2026-09-07 (C-04 D4 PR 5 landed)**: the backup janitor's own
async loop (`animus_node::backup_janitor::backup_janitor_loop`) now has
deterministic `SimCluster` coverage — `client_ctx_host.rs`'s four
`ClientCtx` host-capability impls and `backup_janitor.rs`'s own thin
wrapper widened to `<E: Env, R: RelayClient>` (previously concrete
`ClientCtx` = `ClientCtx<ProdEnv, AnimusdRelayClient>`), zero new
mechanism (every field/method each impl delegates to was already
`E`/`R`-agnostic or already generic). `SimCluster` now builds every node's
`backup_store` as a `BackupStoreHandle::S3` wrapping a clone of ONE shared
`SimSegmentStore` (a real S3 bucket has no per-node locality, so this is
the faithful choice, not a placeholder) and spawns the janitor loop
unconditionally on every node, mirroring D4 PR 1's own `heartbeat_loop`
spawn. Five scenarios in the new `sim_cluster_backup_janitor.rs` (12 tests
including `_over_seeds` siblings): a deleted backup reclaimed, a failed
backup reclaimed, leader gating (a follower never touches the store) plus
a real leadership-transfer handoff mid-reclaim converging cleanly, a
crashed-and-restarted control leader converging via the survivors' own
reclaim, and an untouched `Available` backup left alone. `cargo test -p
animusd --lib`: 331 → 343 (+12, 0 regressions). No janitor bug found —
one harness-only gotcha was found and fixed (a `propose`-then-immediate-
`crash` scenario must let the entry replicate before crashing its own
proposer, or the entry is stranded and lost rather than inherited by the
survivors; see ADR 0061's and `docs/engineering-lessons.md`'s matching
entries). `dynamo_backup.rs`'s own janitor-convergence assertion stays on
`ProdEnv`, fused into one long wire-shape test `SimCluster` cannot
reach — nothing separable to move. **This closes D4 PR 5. What remains
open for D4: PR 4 (join/growth needs an add-node capability).**

---

**Addendum, 2026-09-07 (C-04 D4 PR 4 landed, closing D4 and C-04)**: the
last of the four D4 rungs — join/growth/decommission sequencing (ADR
0030/0032) — now has deterministic `SimCluster` coverage. `SimCluster`
gains a `grow(role)`/`drain(node)`/`remove(node)` fixture surface (five new
signatures total, `crates/animusd/src/sim_cluster.rs`): `grow` adds a
data-only node after construction, installing `ControlHandle::Remote`
(a `RemoteControlClient` mirroring against the existing control quorum) for
the first time under `SimEnv` — a new `SimEnv`-native reimplementation of
`animusd`'s own `remote_metadata_watch_loop`'s long-poll protocol
(`spawn_remote_mirror_sync_loop`, since the production function's own
`ClientCtx<ProdEnv>`-bound signature and real `tokio::time::sleep` can't run
under `SimEnv` at all) drives the identical wire round trip against the
identical `RemoteControlClient` type production uses; `drain`/`remove` drive
the REAL `ClientCtx::admin_drain`/`admin_remove_member` primitives, already
`<E, R>`-generic since rung C5. `role = "combined"` (a new control-plane
voter) was scoped and deferred — it needs `self.controls` itself to grow, a
materially different mechanism than a data-only node's mirror; documented
as a named follow-up, not attempted. Five scenarios in the new
`crates/animusd/src/sim_cluster_growth.rs`, replayed at 5 seeds each: grow
converges and serves a genuinely forwarded write/read; growth then a real
rebalance places (and tears down the moved-away replica of) a table onto
the new node; grow-then-drain-then-remove re-homes every replica with no
zombie group and never reuses the removed node's id; a control-plane leader
crash between `grow`'s own two registration proposes still converges once a
new leader takes over; and the mirror's long-poll recovers from a full
partition of the new node from every control voter. No product bug found —
the `ControlHandle::Remote`-under-`SimEnv` path (new surface, and the
likeliest place for one) held at every seed tried. See ADR 0061's "D4 PR 4"
amendment, and ADR 0030's/ADR 0032's matching 2026-09-07 amendments, for
the full account.

**This closes D4, and with it C-04.** All four D4 rungs (auto-split byte
trigger, dropped-table GC, the backup janitor, and this PR's join/growth/
decommission) now have deterministic `SimCluster` coverage; issues #715 and
#722 (found and fixed along the way) are both closed. What remains
unowned by C-04 or any other planned rung, per the D3-closing residual
inventory above: Transact/PartiQL (D2's own named residuals), Streams, TTL,
admin/console/dashboard HTTP, the control/data role split, and `--config`
bring-up — none scoped to a future C-04 rung as of this close.
