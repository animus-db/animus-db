# Production-readiness: beta exit criteria

This is the checklist that decides when AnimusDB stops being **pre-alpha**
and may call itself **beta**. It is ratified by
[ADR 0074](adr/0074-production-readiness-exit-criteria.md); the work plan is
`docs/roadmap.md` R-01.

**Beta means: every criterion below is `Met`, or carries an explicit,
signed-off waiver listed in the [Waivers](#waivers) table.** Nothing else
defines beta. `Pending-dependency` blocks beta exactly like `Not met`
unless waived.

How to read a row:

- **Status** — `Met` (evidence exists in the tree and is green),
  `Not met`, or `Pending-dependency` (cannot start or finish until the
  named item lands).
- **Evidence** — a path that exists (test, workflow, script, ADR). For a
  `Not met` row it names what exists today and what is missing. Prose is
  not evidence; a row flips to `Met` in the PR that adds its evidence.
- **Owner** — the R-01 sub-track (a soak, b chaos, c fuzzing, d resource
  bounds/overload, e runbook, f observability, g release engineering) or
  `X` for cross-cutting.

Last verified against the tree: 2026-11-14.

## Dependencies

| Needs | Blocks |
|---|---|
| B-01 (benchmark harness / workload generator: `crates/animus-bench`, ADR 0076; harness landed, **no published numbers**, `docs/benchmarks.md`) | (a) soak workload; (e) capacity-planning numbers; (f) per-op-class p99 SLO targets |
| C-17 (scale/density, `docs/roadmap.md`; measurements in `docs/density-measurements.md`, sizing guidance not yet derived) | (d) default connection/in-flight bounds sizing; (e) disk and node sizing |
| ADR 0073 Phase 2 (replicated cluster version / feature gate) and Phase 3 (rolling upgrades): **both done** (2026-10-04, 2026-10-05) | (e) the rolling-upgrade chapter (landed, E-7); criterion G-7 mixed-version support statement (moot, X-11 is Met) |

## Cross-cutting criteria

| ID | Criterion | Status | Evidence | Owner |
|---|---|---|---|---|
| X-1 | Correctness is established by deterministic simulation: linearizability and transaction-serializability oracles run seeded corpora per push, deeper tiers nightly | Met | `crates/animus-test/tests/it/raftkv_linearizable.rs`, `crates/animus-test/tests/it/txn_serializable.rs`, `.github/workflows/corpus-deep.yml`, `.github/workflows/ci.yml`, `docs/adr/0003-deterministic-simulation.md` | X |
| X-2 | Every persisted format is version-tagged and pinned by golden fixtures; an edited or deleted fixture fails CI (ADR 0073 Phase 0, baseline `9a9f972f`) | Met | `scripts/check-format-fixtures.sh`, the `format fixtures are append-only` step in `.github/workflows/ci.yml`, `crates/*/tests/fixtures/formats/`, `docs/adr/0073-upgrade-compatibility.md` | X |
| X-3 | A whole-cluster stop, upgrade and restart across format versions is supported and tested (ADR 0073 Phase 1) | Met | `crates/animus-test/tests/it/upgrade_restart_corpus.rs`, `crates/animusd/src/sim_cluster_upgrade_corpus.rs`, `.github/workflows/corpus-deep.yml` | X |
| X-4 | TLS available on every port (mutual on internal, server-only on client/admin/console), config-gated | Met | `docs/adr/0064-tls-on-every-port.md`, `crates/animus-env/src/tls.rs` | X |
| X-5 | Encryption at rest for WAL, engine files and backup/stream-segment objects | Met | `docs/adr/0069-encryption-at-rest.md` | X |
| X-6 | Client authentication: SigV4 with replicated credentials, rotation and a per-key allow list | Met | `docs/adr/0066-sigv4-hardening.md` | X |
| X-7 | Per-table throttling in DynamoDB capacity units with the AWS error | Met | `docs/adr/0065-per-table-throttling.md` | X |
| X-8 | Backup, restore and PITR implemented and fault-injection tested | Met | `docs/adr/0059-backup-restore.md` | X |
| X-9 | The Kubernetes operator is smoke-tested on a real `kind` cluster in CI (create, bootstrap, scale, delete; plain and TLS) | Met | `scripts/e2e-kind.sh`, `.github/workflows/e2e-kind.yml`, `docs/adr/0060-kubernetes-operator.md` | X |
| X-10 | Dependency licences and advisories are gated per push | Met | `deny.toml`, the `cargo-deny check` step in `.github/workflows/ci.yml` | X |
| X-11 | Rolling (mixed-version) upgrades are supported and tested | Met | Phase 2 and 3 are done (ADR 0073): replicated cluster version and gates, mixed-version corpus `crates/animus-control/tests/it/version_mixed_corpus.rs`, `crates/animusd/src/sim_cluster_mixed_version_corpus.rs`, roll orchestrator corpus `crates/animusd/src/sim_cluster_roll_orchestrator.rs`, `crates/animus-roll`, and a real-process roll from the previous release (`crates/animusd/tests/upgrade_previous_release.rs`, CI job `upgrade-previous-release` in `.github/workflows/ci.yml`); procedure `docs/runbook/upgrade.md`. Limits (documented, not gaps in the criterion): no rollback after a node ran the new binary, no skipped release, and a roll from a tree older than `f97451d58` with transactions is unsupported (#1355) | X |

## (a) Soak

Harness: [`docs/soak.md`](soak.md). (It uses the chaos history recorder, not B-01's `animus-bench` generator, which yields latency rather than an oracle-checkable history.)

| ID | Criterion | Status | Evidence | Owner |
|---|---|---|---|---|
| A-1 | A multi-day soak harness runs real `animusd` processes (bare multi-process and/or the operator on `kind`) under a continuous recorded workload | Partially met | Bare multi-process harness `crates/animusd/tests/soak.rs` (opt-in `soak` feature, knobs up to `ANIMUS_SOAK_DURATION=7d`), [`docs/soak.md`](soak.md), and a weekly 45 minute leg in `.github/workflows/soak.yml` (green 2026-10-25, 11-01, 11-08). Missing: no run longer than the 45 minute CI leg exists; the operator-on-`kind` leg is not built; it uses the chaos recorder rather than `animus-bench` | a |
| A-2 | A 7-consecutive-day soak completes with zero violations from the `animus-test` oracles (`check_cycles`, `check_durability`, `check_convergence`) over the recorded history | Not met | Oracles run per epoch inside the harness (`docs/soak.md`); only short legs run so far, no 7-day run | a |
| A-3 | Resource trends stay bounded over the soak: RSS, open fds, disk, WAL and compaction backlog show no monotone growth | Not met | Trend detector `crates/animus-test/src/soak.rs` (unit-tested) and per-node sampling in `tests/soak.rs`; no 7-day run | a |
| A-4 | Soak is re-runnable from CI or one documented command | Met | `.github/workflows/soak.yml` (short leg, weekly + dispatch) and the one command in `docs/soak.md` | a |

## (b) Real-cluster chaos

| ID | Criterion | Status | Evidence | Owner |
|---|---|---|---|---|
| B-1 | Chaos scenarios run against real processes: process kill, network partition, clock skew, slow disk, disk full | Not met | Real-process harness `crates/animusd/tests/chaos.rs` covers process kill (incl. control leader, full power cut), network partition (incl. one-way), delay and SIGSTOP stall, see `docs/chaos.md`. Disk full now runs against real processes on real size-limited tmpfs mounts (`chaos_disk_full`, #1221; needs `CAP_SYS_ADMIN`, CI job `chaos-disk-full`, skips cleanly where mounting is not permitted); it found two real defects (F-1 and F-2 in `docs/chaos.md`, both since fixed). Still missing from the criterion: clock skew and slow disk (Kubernetes-only designs in `deploy/chaos/`, unvalidated) | b |
| B-2 | Each scenario records a client history and passes the `animus-test` oracles | Partially met | History capture and oracle feed (`crates/animusd/tests/chaos_support/workload.rs`, oracles `crates/animus-test/src/check.rs`); the nightly `.github/workflows/chaos.yml` (scenarios plus `chaos-disk-full`) has been green on every scheduled run since 2026-10-31 after the txn/split root causes in `docs/chaos.md` F-2 were fixed. Missing: `docs/chaos.md` still lists Finding 1 as not confirmed end to end and Finding 2 (wrong error class for an unconditional write racing an intent) open, and the criterion names scenarios (clock skew, slow disk) that do not exist (B-1) | b |
| B-3 | A failure reproducible from a seed is converted into a seeded sim corpus cell | Partially met | Policy in ADR 0074 section 1. The root causes the chaos runs exposed became red-before sim cells: `sim_cluster_auto_split` scenarios, `animus-cp-data` `txn_id_across_groups`, `split_tablet::a_txn_decision_ordered_after_the_fork_is_a_sealed_no_op`, `sim_cluster_dynamo_transact::coordinator_crash_recovers_when_the_survivors_clock_restarted_behind_the_record` (`docs/chaos.md`). Missing: Finding 1 itself (not seed-reproducible, ~1 in 12 smoke runs before the fixes) was never reduced to a cell; its tombstone-GC engine repro is "kept with the report, not committed" | b |
| B-4 | A chaos leg runs in CI or nightly beside the `kind` e2e | Met | `.github/workflows/chaos.yml` (PR smoke + nightly, non-required) beside `.github/workflows/e2e-kind.yml` | b |

## (c) Fuzzing

| ID | Criterion | Status | Evidence | Owner |
|---|---|---|---|---|
| C-1 | A `cargo-fuzz` target exists for every untrusted parser: DynamoDB JSON request decode, UpdateExpression/ConditionExpression/projection parsers, PartiQL lexer/parser, SigV4 header/credential parsing, the HTTP request parser | Met | `fuzz/fuzz_targets/{dynamo_request,dynamo_expressions,partiql,http_sigv4,net_frames}.rs`; stable smoke `fuzz/tests/smoke.rs`; see `fuzz/README.md` | c |
| C-2 | A fuzz target exists for each durable-format decoder with a `legacy` seam (LSM WAL/SSTable/manifest, Raft WAL/snapshot, RaftKV codec, segment and backup chunk codecs, encryption envelope), seeded from the golden fixtures | Met | `fuzz/fuzz_targets/{lsm_formats,control_formats,cp_data_formats,encryption_envelope,item_codecs}.rs`, seeded in place from `crates/*/tests/fixtures/formats/` via `fuzz/seeds.tsv` | c |
| C-3 | Property held: never panic, never allocate unboundedly, decode-or-named-error | Partially met | The LZ4 size-prefix allocation is fixed in `animus-storage`: `decompress_lz4_block` in `crates/animus-storage/src/lsm/sstable.rs` bounds the declared size by 255x the compressed length. Missing: the fenced guard in `fuzz_shims::block_v1` (same file) and the matching note in `fuzz/known-issues.tsv` are still in place, so no fuzz run has exercised the property unguarded; the property flips to held when the guard is removed and a long run is clean | c |
| C-4 | A short fuzz smoke (about 60 s per target) runs per push and long runs nightly | Met | `.github/workflows/fuzz.yml`: 60 s per target on push (green on `main` 2026-11-14) and a long nightly matrix (scheduled runs green 2026-11-08 to 2026-11-14); `fuzz` is part of the `gates` aggregate in `ci.yml` | c |
| C-5 | Every crash found becomes a regression test (format-decoder crashes are filed as bugs under the green invariant) | Met | The one finding so far (the LZ4 size-prefix allocation) has its regression test, `lz4_block_with_hostile_size_prefix_is_rejected_without_allocating` (+ `maximal_legit_lz4_block_round_trips_under_the_bound`) in `crates/animus-storage/src/lsm/sstable.rs`; the policy continues to apply to future crashes | c |

## (d) Resource bounds and overload

Contract fixed by ADR 0074 section 2 (bounded queues; `ServiceUnavailable`
503 on node overload; `ProvisionedThroughputExceededException` stays
per-table; disk-full refuses writes, never acks un-fsynced). Default sizing
uses C-17.

| ID | Criterion | Status | Evidence | Owner |
|---|---|---|---|---|
| D-1 | Per-table throttling returns `ProvisionedThroughputExceededException` | Met | `docs/adr/0065-per-table-throttling.md`, `crates/animus-dynamo/src/wire.rs` | d |
| D-2 | Request bodies are size-bounded | Met | `MAX_BODY` (1 MiB) in `crates/animus-node/src/http.rs` | d |
| D-3 | AWS-faithful service limits catalogued and enforced (item size, batch sizes, transaction size) | Met | ADR 0072 layers 1 to 4 are all landed: catalogue `crates/animus-dynamo/src/limits.rs` and enforcement for item size, key/attribute/name/expression limits, the 1 MiB `Query`/`Scan` page, batch sizes and the 16 MiB batch / 4 MiB transaction byte caps, with wire tests `crates/animusd/src/sim_cluster_dynamo_wire_limits.rs`, `sim_cluster_dynamo_page_size_cap.rs`, `sim_cluster_dynamo_byte_caps.rs`. The ADR keeps a short explicit list of deferred limits (stream iterator expiry, LSI collection size, expression-attribute aggregate size), none named by the criterion | d |
| D-4 | Every client-facing listener has a finite connection cap; an excess connection is refused with 503 `ServiceUnavailable`, never queued | Met | `overload.max_connections` (4096), `max_admin_connections` (256) and `max_peer_connections` (16384) on the dynamo, admin, console, client and intra listeners; an excess connection gets a prompt 503 from a bounded refusal task (plain TCP) or is closed (TLS). `crates/animusd/tests/overload.rs` (real TCP); `docs/resource-bounds.md` section 1 | d |
| D-5 | A node-wide in-flight request bound and a per-connection pipelining bound shed with `ServiceUnavailable`, before queuing | Met | `overload.max_inflight_requests` (2048) sheds the next request with 503 at once (`overload::CountGate`, never queued); pipelining is bounded at 1 by construction (`handle_conn` answers one request before reading the next). `crates/animusd/tests/overload.rs`; `docs/resource-bounds.md` section 1 | d |
| D-6 | Memory-bound audit done: per-connection buffers, scan/batch result sizes, snapshot streaming buffers and channels on untrusted paths are bounded and documented, each with a test | Partially met | Audit recorded in `docs/resource-bounds.md` section 2 (path, bound, status per row), with the peer-frame cap (`check_frame_len`, `crates/animus-env/src/prod.rs`, tests `read_frames_rejects_oversized_*`), the client-listener frame bound, the bounded accept channel and listener timeouts landed (ADR 0080). Still open in its section 4: item 3 (the per-tablet scan byte cap applies at the coordinator, after the RPC), the intra listener keeps a 64 MiB frame bound, and the console listener is covered only by its connection cap; not every audited row names a test | d |
| D-7 | Disk-full: a write that cannot be fsynced is refused with a named `StorageFull` error and never acked, reads continue, and the node recovers without restart when space returns; proven in sim and on a real size-limited filesystem | Met | Sim: `ANIMUS_DISK_FULL_SEEDS` corpus (`crates/animus-test/tests/it/raftkv_linearizable.rs`, over `MemoryEngine` and `LsmEngine<SimEnv>`), named 503 `StorageFull`, `storage_full` on `/admin/health`, WAL rewrite without restart (#1185, #1218, #1219, #1228). Real filesystem: `chaos_disk_full` (`crates/animusd/tests/chaos.rs`, #1221) on size-limited tmpfs mounts, run in the `chaos-disk-full` job of `.github/workflows/chaos.yml` (per PR and nightly, `ANIMUS_CHAOS_REQUIRE_MOUNT=1` so it fails rather than skips; green nightly). It asserts the refusal, no ack, reads continuing, and recovery with no restart. Details `docs/resource-bounds.md` section 3, `docs/chaos.md`. Documented decision: a leader that dies while every replica is full is not replaced until space returns (ADR 0074 amendment) | d |
| D-8 | Each overload refusal is counted by reason in the metrics seam | Met | `overload_shed_conn_cap`, `overload_shed_admission`, `overload_shed_admin_conn_cap`, `overload_shed_peer_conn_cap` (and `overload_storage_full`) in `crates/animus-env/src/metrics.rs`; asserted in `crates/animusd/tests/overload.rs` | d |
| D-9 | A storage engine that falls behind on flush or compaction slows its writers and, through the propose bound, sheds with `ThrottlingException`; it never fails a committed entry or panics the tablet | Partially met | ADR 0081 (#1300): per-engine L0 slowdown and stop before the log append, delay only; node-wide compaction write-bytes budget (default off, urgent bypass); metrics and `lsm_stall` on `/admin/health`. Sim corpus `ANIMUS_LSM_STALL_SEEDS` (`lsm_write_stall.rs`, `lsm_stall_admission.rs`, `compaction_io_budget_corpus.rs`, `sim_cluster_lsm_stall`), flush starvation corpus (`lsm_flush_starvation.rs`, #1341), real-thread `crates/animus-storage/tests/lsm_stall_prod.rs`. Missing: the budget default is unmeasured on a real volume (ADR 0081 open item) and no soak has run with the stall on | d |

## (e) Operations runbook

Capacity planning needs B-01 and C-17; the upgrade chapter's rolling part
landed with ADR 0073 Phase 3 (E-7).

| ID | Criterion | Status | Evidence | Owner |
|---|---|---|---|---|
| E-1 | `docs/runbook/` exists with node replace and decommission (ADR 0032 drain) | Met | `docs/runbook/` exists (index `docs/runbook/README.md`) with `node-replace.md` and `node-decommission.md` (ADR 0032 drain), plus `node-down.md`, `tablet-unavailable.md` and the rest. Maturity: written from the code and ADRs, not executed on a real multi-node cluster (see E-9) | e |
| E-2 | Control-plane quorum-loss procedure written, including whether an unsafe-recovery tool is needed | Met (control plane) | ADR 0077: `animusd recover-control` (offline force-new-configuration, ack flag, simulation corpus `force_new_configuration`); runbook `control-plane-quorum-loss.md`. Not drilled on a real cluster; tablet-group form is phase 2 | e |
| E-3 | Backup, restore and PITR drill documented and executed once | Partially met | Procedure and drill written in `docs/runbook/backup-restore-pitr.md`, executed once with `curl` on a local dev cluster on in-memory engines on 2026-10-04. Missing: the page itself says this is "not a real drill": no S3 backend, real disks or restore into a fresh cluster; game-day drill 11 (`docs/runbook/game-day.md`) is unexecuted | e |
| E-4 | Certificate rotation procedure (restart-time `TlsConfig::load()`) | Met | `docs/runbook/cert-rotation.md` (leaf and CA rotation, restart-time `TlsConfig::load()`) and `docs/runbook/cert-expiry.md`; mechanism ADR 0064. The page states it was not executed on a real cluster | e |
| E-5 | Encryption key rotation procedure | Met | `docs/runbook/encryption-key-rotation.md`; mechanism ADR 0069. Not executed on a real cluster | e |
| E-6 | Upgrade chapter: whole-cluster procedure per ADR 0073 | Met | `docs/runbook/upgrade.md` whole-cluster procedure (ADR 0073 Phase 1; X-3). One stale sentence in it (version `0.0.0`, no `--version`) is corrected in this change | e |
| E-7 | Upgrade chapter: rolling-upgrade procedure | Met | `docs/runbook/upgrade.md` (manual CLI roll and operator-orchestrated roll); exercised on real processes by the `upgrade-previous-release` CI job (`crates/animusd/tests/upgrade_previous_release.rs`) and on `kind` by the nightly `.github/workflows/upgrade-kind-nightly.yml` (`E2E_UPGRADE=1`), which went green on `main` 2026-11-14 after ten red nights (one green scheduled run so far; watch it). Documented limit: rolling from a tree older than `f97451d58` with transactions is unsupported (#1355) | e |
| E-8 | Capacity planning and disk sizing with published numbers | Pending-dependency | `docs/runbook/capacity-planning.md` has the model and worksheet but states "Numbers are PENDING"; `docs/benchmarks.md` states there are no published results (B-01 curated run not done); C-17 measurements exist in `docs/density-measurements.md` but are not turned into sizing guidance | e |
| E-9 | A game-day drill checklist is executed once on `kind` | Not met | Checklist `docs/runbook/game-day.md` and substrate `scripts/e2e-kind.sh` (`E2E_KEEP_CLUSTER=1`) exist; the checklist is marked "NOT EXECUTED" and its execution log is empty. The nightly `kind` runs are the e2e smoke, not the drill | e |

## (f) Observability completeness

SLO latency targets need B-01.

| ID | Criterion | Status | Evidence | Owner |
|---|---|---|---|---|
| F-1 | A metrics endpoint, admin API and dashboard exist | Met | `crates/animusd/src/admin.rs` (`/admin/metrics`, `/admin/health`, `/admin/live`), `crates/animus-env/src/metrics.rs`, `docs/adr/0015-observability.md` | f |
| F-2 | SLOs defined (availability; p99 latency per op class) | Pending-dependency | Availability SLO defined in `deploy/observability/README.md` (99.9% over 30 days, multiwindow burn-rate alerts and recording rules in `deploy/observability/animus-alerts.yml`). Missing: every p99 latency target per op class is "provisional pending B-01"; animusd exports no latency histogram, so latency is unmeasurable from the exposition (gap 1 there) | f |
| F-3 | Alert rules shipped as Prometheus YAML plus a dashboard JSON | Met | `deploy/observability/animus-alerts.yml` (33 alerts plus recording rules), `deploy/observability/animus-dashboard.json`, `deploy/observability/README.md` | f |
| F-4 | A CI test asserts every metric name referenced in `docs/` and `website/` exists in `crates/animus-env/src/metrics.rs` and appears in `/admin/metrics` output | Not met | No test greps `docs/` or `website/` for metric names. Related tests only cover individual metrics (`crates/animusd/tests/metrics_endpoint.rs`); nothing checks the alert rules' metric names either | f |
| F-5 | Each alert links to a runbook page | Met | All 33 alerts in `deploy/observability/animus-alerts.yml` carry a `runbook_url` and every target page (and the `node-down.md` anchor) exists under `docs/runbook/`, checked 2026-11-14. Nothing in CI keeps the links resolving | f |

## (g) Release engineering

Policy fixed by ADR 0074 section 3.

| ID | Criterion | Status | Evidence | Owner |
|---|---|---|---|---|
| G-1 | The binary has a real SemVer version, bumped per policy | Met | Workspace `version = "0.1.0-alpha.0"` in `Cargo.toml`, bump policy in `docs/release.md`, `animusd --version` / `animus --version`, tag-must-equal-version check in `.github/workflows/release.yml` | g |
| G-2 | A `CHANGELOG.md` entry exists for every tagged release; a tag without one fails the release workflow | Partially met | `CHANGELOG.md` (Keep a Changelog, `Unreleased` only), `cliff.toml`, and release notes generated by git-cliff in `release.yml`. Missing: no tag has been cut, and `release.yml` verifies only that the tag equals the workspace version; it does not fail a tag whose `CHANGELOG.md` section is missing | g |
| G-3 | Release images and binaries are signed with cosign keyless | Partially met | Implemented, never exercised: `.github/workflows/release.yml` (cosign keyless `sign-blob` of every artefact) and `.github/workflows/image.yml` (cosign keyless on the manifest digest). Both workflows state they have not run on a real tag, and the repository has no `v*` tag | g |
| G-4 | An SBOM is generated and attached to each release and image | Partially met | Implemented, never exercised: CycloneDX SBOMs via `cargo cyclonedx` attached to the release (`release.yml`), and an SBOM attestation on each image digest (`image.yml`, `anchore/sbom-action` + `actions/attest-sbom`). No tag has run them | g |
| G-5 | A build-provenance attestation is published for each release artefact | Partially met | Implemented, never exercised: `actions/attest-build-provenance` in `release.yml` (tarballs, `SHA256SUMS`) and `image.yml` (image digests). No tag has run them | g |
| G-6 | `SECURITY.md` with a disclosure process exists | Met | `SECURITY.md` (supported versions, private GitHub vulnerability reporting, process) | g |
| G-7 | A supported-platform matrix (OS, kernel, filesystem, architectures, Kubernetes versions) is published and gates a release; mixed-version clusters are stated as unsupported until X-11 | Partially met | Matrix published in `docs/release.md` "Supported platforms" (Linux glibc, kernel 5.10+, amd64/arm64, ext4/xfs, Kubernetes 1.32 to 1.34), with CI legs for Kubernetes versions (`e2e-kind-k8s-compat` in `.github/workflows/e2e-kind.yml`) and an arm64 build and smoke on tags. Missing: nothing makes a release depend on those legs (`release.yml` has no gate on them; only `upgrade-previous-release` is a documented pre-tag step). The mixed-version clause is moot now that rolling upgrades are supported (X-11); `docs/release.md` states the one-release-step limit | g |
| G-8 | Deprecation policy for wire and format changes is written | Met | `docs/adr/0074-production-readiness-exit-criteria.md` section 3.6, `docs/adr/0073-upgrade-compatibility.md` | g |
| G-9 | Release images are published for every supported architecture (multi-arch, at least amd64 and arm64) | Partially met | `.github/workflows/image.yml` builds `linux/amd64` always and `linux/arm64` natively on `ubuntu-24.04-arm` on `v*` tags, then joins them into one signed manifest list; `release.yml` builds amd64 and arm64 binaries. Missing: no tag has run it, so no multi-arch image has been published or verified | g |

## Waivers

A waiver lists the criterion, why it is waived, the risk accepted, who
signed it off (a maintainer, by name, in the PR that adds the row) and the
date. Waivers are re-reviewed at every release.

| Criterion | Reason | Risk accepted | Signed off by | Date |
|---|---|---|---|---|
| _(none)_ | | | | |

## Summary (2026-11-14)

Met: X-1 to X-11, A-4, B-4, C-1, C-2, C-4, C-5, D-1 to D-5, D-7, D-8, E-1, E-2 (control plane),
E-4 to E-7, F-1, F-3, F-5, G-1, G-6, G-8.

Partially met: A-1, B-2, B-3, C-3, D-6, D-9, E-3, G-2, G-3, G-4, G-5, G-7, G-9.

Pending-dependency: E-8 (needs B-01 published numbers and C-17 sizing guidance), F-2 (latency
targets need B-01 and a latency histogram).

Not met: A-2 and A-3 (no 7-day soak run), B-1 (no clock-skew or slow-disk scenario against real
processes), E-9 (the game-day drill has never been executed), F-4 (no test that documented metric
names exist).

A `Partially met` row blocks beta exactly like `Not met`. The project is therefore **pre-alpha**.
Rows that need an external event rather than more code: G-2 to G-5 and G-9 flip with the first
real `v*` tag run (and a changelog-presence check in `release.yml` for G-2); A-2, A-3 need the
7-day run; E-3 and E-9 need a real drill.
