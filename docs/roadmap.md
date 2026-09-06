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

---

## 2. Security, storage, and deployment

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
- **Depends:** was sequenced after S-02 specifically to avoid three crypto
  ADRs in review at once; S-02 ([ADR 0066](adr/0066-sigv4-hardening.md))
  landed 2026-09-05, so this item is unblocked.

### S-07 Operator hardening (ADR 0060 deferred list)

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
- **ADR:** amend 0060 for c; d and e get their own section or a new
  number if the webhook design grows. (Item b — `backupStore`/
  `segmentStore` CRD fields for the non-S3 `cluster`/`fs:`/`dir:` forms —
  landed 2026-09-06, see ADR 0060's own "Amendment (2026-09-06): S-07b"
  section; `spec.s3` already covers the `s3://...` form, ADR 0060's S-04
  PR 3 amendment.)

---

## 3. Core design items still proposed

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
| 5 | *S-04, S-05, and S-07b landed 2026-09-06* → S-07c–d, C-02, C-05 | S-05 strictly after S-04 |
| 6 | S-03, S-07e, W-07, C-03 | XL or gated on earlier waves (S-07e's webhook-TLS prerequisite is satisfied now that S-01 landed; no longer a hard gate, just unscheduled) |

Open issues mapped: none left (#375 closed by W-01, #319 by W-05). Filed
from wave 2's own findings: #590 (the operator still emits the deleted
`--split-mode` flag) and #591 (a `control_only` relay-budget flake). The flaky-test issues
(#280, #298, #418, #447, #539) are correctness work under the green
invariant, not roadmap items, and take precedence over any wave.
