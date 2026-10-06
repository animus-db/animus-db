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

Last verified against the tree: 2026-10-04.

## Dependencies

| Needs | Blocks |
|---|---|
| B-01 (benchmark harness / workload generator: `crates/animus-bench`, ADR 0076; published numbers still pending) | (a) soak workload; (e) capacity-planning numbers; (f) per-op-class p99 SLO targets |
| C-17 (scale/density, `docs/roadmap.md`) | (d) default connection/in-flight bounds sizing; (e) disk and node sizing |
| ADR 0073 Phase 2 (replicated cluster version / feature gate) and Phase 3 (rolling upgrades) | (e) the rolling-upgrade chapter; criterion G-7 mixed-version support statement |

## Cross-cutting criteria (already met)

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
| X-11 | Rolling (mixed-version) upgrades are supported and tested | Pending-dependency | ADR 0073 Phase 2 (P2-A and P2-C landed: era live, admin/CLI present; P2-B and P2-D not yet) then Phase 3 (planned); rolling upgrade is not yet supported, only whole-cluster restart | X |

## (a) Soak

Harness: [`docs/soak.md`](soak.md). (It uses the chaos history recorder, not B-01's `animus-bench` generator, which yields latency rather than an oracle-checkable history.)

| ID | Criterion | Status | Evidence | Owner |
|---|---|---|---|---|
| A-1 | A multi-day soak harness runs real `animusd` processes (bare multi-process and/or the operator on `kind`) under a continuous recorded workload | Not met | Bare multi-process harness `crates/animusd/tests/soak.rs` (opt-in `soak` feature), [`docs/soak.md`](soak.md); operator-on-`kind` leg not built; uses the chaos recorder rather than `animus-bench` | a |
| A-2 | A 7-consecutive-day soak completes with zero violations from the `animus-test` oracles (`check_cycles`, `check_durability`, `check_convergence`) over the recorded history | Not met | Oracles run per epoch inside the harness (`docs/soak.md`); only short legs run so far, no 7-day run | a |
| A-3 | Resource trends stay bounded over the soak: RSS, open fds, disk, WAL and compaction backlog show no monotone growth | Not met | Trend detector `crates/animus-test/src/soak.rs` (unit-tested) and per-node sampling in `tests/soak.rs`; no 7-day run | a |
| A-4 | Soak is re-runnable from CI or one documented command | Met | `.github/workflows/soak.yml` (short leg, weekly + dispatch) and the one command in `docs/soak.md` | a |

## (b) Real-cluster chaos

| ID | Criterion | Status | Evidence | Owner |
|---|---|---|---|---|
| B-1 | Chaos scenarios run against real processes: process kill, network partition, clock skew, slow disk, disk full | Not met | Real-process harness `crates/animusd/tests/chaos.rs` covers process kill (incl. control leader, full power cut), network partition (incl. one-way), delay and SIGSTOP stall, see `docs/chaos.md`. Disk full now runs against real processes on real size-limited tmpfs mounts (`chaos_disk_full`, #1221; needs `CAP_SYS_ADMIN`, CI job `chaos-disk-full`, skips cleanly where mounting is not permitted); it found two real defects (F-1 and F-2 in `docs/chaos.md`). Still missing from the criterion: clock skew and slow disk (Kubernetes-only designs in `deploy/chaos/`, unvalidated) | b |
| B-2 | Each scenario records a client history and passes the `animus-test` oracles | Not met | History capture and oracle feed exist (`crates/animusd/tests/chaos_support/workload.rs`, oracles `crates/animus-test/src/check.rs`) but the first runs found a violation (`docs/chaos.md`, Findings), so the scenarios do not pass | b |
| B-3 | A failure reproducible from a seed is converted into a seeded sim corpus cell | Not met | Policy in ADR 0074 section 1. The first finding is timing-dependent (not seed-reproducible, ~1 in 12 smoke runs) and has not been reduced to a sim cell yet; the engine-level mechanism is reproducible deterministically (`docs/chaos.md`, Findings) | b |
| B-4 | A chaos leg runs in CI or nightly beside the `kind` e2e | Met | `.github/workflows/chaos.yml` (PR smoke + nightly, non-required) beside `.github/workflows/e2e-kind.yml` | b |

## (c) Fuzzing

| ID | Criterion | Status | Evidence | Owner |
|---|---|---|---|---|
| C-1 | A `cargo-fuzz` target exists for every untrusted parser: DynamoDB JSON request decode, UpdateExpression/ConditionExpression/projection parsers, PartiQL lexer/parser, SigV4 header/credential parsing, the HTTP request parser | Met | `fuzz/fuzz_targets/{dynamo_request,dynamo_expressions,partiql,http_sigv4,net_frames}.rs`; stable smoke `fuzz/tests/smoke.rs`; see `fuzz/README.md` | c |
| C-2 | A fuzz target exists for each durable-format decoder with a `legacy` seam (LSM WAL/SSTable/manifest, Raft WAL/snapshot, RaftKV codec, segment and backup chunk codecs, encryption envelope), seeded from the golden fixtures | Met | `fuzz/fuzz_targets/{lsm_formats,control_formats,cp_data_formats,encryption_envelope,item_codecs}.rs`, seeded in place from `crates/*/tests/fixtures/formats/` via `fuzz/seeds.tsv` | c |
| C-3 | Property held: never panic, never allocate unboundedly, decode-or-named-error | Not met | Held on every target except one known violation: an LZ4 SSTable block's untrusted size prefix allocates ~4 GiB (`animus-storage` `decode_block_v1`), fenced by a guard in `fuzz_shims::block_v1` and listed in `fuzz/known-issues.tsv`; flips when its fix PR lands | c |
| C-4 | A short fuzz smoke (about 60 s per target) runs per push and long runs nightly | Not met | `.github/workflows/fuzz.yml` (60 s per target per push, long nightly matrix) is in the tree; flips on its first green run | c |
| C-5 | Every crash found becomes a regression test (format-decoder crashes are filed as bugs under the green invariant) | Not met | One finding so far (the LZ4 size-prefix allocation above); its regression test lands with the fix | c |

## (d) Resource bounds and overload

Contract fixed by ADR 0074 section 2 (bounded queues; `ServiceUnavailable`
503 on node overload; `ProvisionedThroughputExceededException` stays
per-table; disk-full refuses writes, never acks un-fsynced). Default sizing
uses C-17.

| ID | Criterion | Status | Evidence | Owner |
|---|---|---|---|---|
| D-1 | Per-table throttling returns `ProvisionedThroughputExceededException` | Met | `docs/adr/0065-per-table-throttling.md`, `crates/animus-dynamo/src/wire.rs` | d |
| D-2 | Request bodies are size-bounded | Met | `MAX_BODY` (1 MiB) in `crates/animus-node/src/http.rs` | d |
| D-3 | AWS-faithful service limits catalogued and enforced (item size, batch sizes, transaction size) | Not met | Catalogue: `crates/animus-dynamo/src/limits.rs`; ADR 0072 is PR 1 of 4, validation limits, `Query`/`Scan` page cap and aggregate byte caps pending | d |
| D-4 | Every client-facing listener has a finite connection cap; an excess connection is refused with 503 `ServiceUnavailable`, never queued | Not met | The accept loop in `crates/animusd/src/dynamo.rs` spawns a task per connection with no cap; no cap in `crates/animus-node/src/http.rs` | d |
| D-5 | A node-wide in-flight request bound and a per-connection pipelining bound shed with `ServiceUnavailable`, before queuing | Not met | None; error plumbing exists (`error_status`, `WireError::service_unavailable`) | d |
| D-6 | Memory-bound audit done: per-connection buffers, scan/batch result sizes, snapshot streaming buffers and channels on untrusted paths are bounded and documented, each with a test | Not met | No audit recorded | d |
| D-7 | Disk-full: a write that cannot be fsynced is refused with a named `StorageFull` error and never acked, reads continue, and the node recovers without restart when space returns; proven in sim and on a real size-limited filesystem | Partially met | Handled (R-01 (d), #1185): ENOSPC marks the WAL suspect, refuses writes with a named 503 `StorageFull` (`overload_storage_full`), reports `storage_full` on `/admin/health`, and rewrites the WAL onto free space without a restart; sim corpus `ANIMUS_DISK_FULL_SEEDS` (`raftkv_linearizable.rs`). LSM-engine ENOSPC (#1218) is handled too: the apply task pauses and retries, flush/compaction fail cleanly, the group reports `storage_full` while paused, and the corpus runs over `LsmEngine<SimEnv>`. A StorageFull tablet leader hands leadership to a replica with free disk (#1219), so a leader-only disk-full window keeps the group writable (probe writes acked in-window in the corpus). The real-filesystem leg now exists (#1221): `chaos_disk_full` (`crates/animusd/tests/chaos.rs`) puts each node's data dir on its own 64 MiB tmpfs, fills one node then all three with a ballast file under a recorded workload, asserts the named 503 `StorageFull` refusal, `storage_full` on `/admin/health`, writes continuing through the other replicas in the one-node window, recovery with no restart (same pids) after the ballast is deleted, and runs the oracles. A panicked consensus task now fails `/admin/health` and is exported as `consensus_task_panics` (#1220). **Not met yet:** reads are *not* reliably served while every node is full (F-1, `docs/chaos.md`), and with the 2PC workload on a disk-full window leaves an unresolved intent that blocks a key (F-2); the scenario runs with 2PC off by default until F-2 is fixed | d |
| D-8 | Each overload refusal is counted by reason in the metrics seam | Not met | None; metrics seam `crates/animus-env/src/metrics.rs` | d |

## (e) Operations runbook

Capacity planning needs B-01 and C-17; the upgrade chapter's rolling part
landed with ADR 0073 Phase 3 (E-7, partially met).

| ID | Criterion | Status | Evidence | Owner |
|---|---|---|---|---|
| E-1 | `docs/runbook/` exists with node replace and decommission (ADR 0032 drain) | Not met | Directory absent; mechanism in `docs/adr/0032-seed-join-membership.md` | e |
| E-2 | Control-plane quorum-loss procedure written, including whether an unsafe-recovery tool is needed | Not met | Only lesson files mention it; no tool exists | e |
| E-3 | Backup, restore and PITR drill documented and executed once | Not met | Mechanism: `docs/adr/0059-backup-restore.md`; no drill | e |
| E-4 | Certificate rotation procedure (restart-time `TlsConfig::load()`) | Not met | Mechanism: `docs/adr/0064-tls-on-every-port.md` | e |
| E-5 | Encryption key rotation procedure | Not met | Mechanism: `docs/adr/0069-encryption-at-rest.md` | e |
| E-6 | Upgrade chapter: whole-cluster procedure per ADR 0073 | Not met | Supported and tested (X-3); no runbook page | e |
| E-7 | Upgrade chapter: rolling-upgrade procedure | Partially met | `docs/runbook/upgrade.md` (manual and operator paths); exercised on real processes by the `upgrade-previous-release` CI job (`crates/animusd/tests/upgrade_previous_release.rs`); the operator path's nightly `kind` leg (`E2E_UPGRADE=1`) has not yet had a verified run; **not met yet:** rolling from a release older than `efcaa6cb` with transactions carries that release's own bug (#1238; #1237 is fixed) and the `kind` leg is unverified | e |
| E-8 | Capacity planning and disk sizing with published numbers | Pending-dependency | B-01 and C-17 | e |
| E-9 | A game-day drill checklist is executed once on `kind` | Not met | `scripts/e2e-kind.sh` is the substrate; no checklist | e |

## (f) Observability completeness

SLO latency targets need B-01.

| ID | Criterion | Status | Evidence | Owner |
|---|---|---|---|---|
| F-1 | A metrics endpoint, admin API and dashboard exist | Met | `crates/animusd/src/admin.rs` (`/admin/metrics`, `/admin/health`, `/admin/live`), `crates/animus-env/src/metrics.rs`, `docs/adr/0015-observability.md` | f |
| F-2 | SLOs defined (availability; p99 latency per op class) | Pending-dependency | None; latency numbers need B-01 | f |
| F-3 | Alert rules shipped as Prometheus YAML plus a dashboard JSON | Not met | `deploy/` holds only `deploy/operator/`; no `deploy/observability/` | f |
| F-4 | A CI test asserts every metric name referenced in `docs/` and `website/` exists in `crates/animus-env/src/metrics.rs` and appears in `/admin/metrics` output | Not met | None | f |
| F-5 | Each alert links to a runbook page | Pending-dependency | Needs (e) pages | f |

## (g) Release engineering

Policy fixed by ADR 0074 section 3.

| ID | Criterion | Status | Evidence | Owner |
|---|---|---|---|---|
| G-1 | The binary has a real SemVer version, bumped per policy | Not met | Workspace `version = "0.0.0"` in `Cargo.toml` | g |
| G-2 | A `CHANGELOG.md` entry exists for every tagged release; a tag without one fails the release workflow | Not met | No `CHANGELOG*` in the tree | g |
| G-3 | Release images and binaries are signed with cosign keyless | Not met | `.github/workflows/image.yml` builds and pushes only (triggers `main` and `v*` tags), no signing step | g |
| G-4 | An SBOM is generated and attached to each release and image | Not met | None | g |
| G-5 | A build-provenance attestation is published for each release artefact | Not met | None | g |
| G-6 | `SECURITY.md` with a disclosure process exists | Not met | No `SECURITY.md` | g |
| G-7 | A supported-platform matrix (OS, kernel, filesystem, architectures, Kubernetes versions) is published and gates a release; mixed-version clusters are stated as unsupported until X-11 | Not met | None | g |
| G-8 | Deprecation policy for wire and format changes is written | Met | `docs/adr/0074-production-readiness-exit-criteria.md` section 3.6, `docs/adr/0073-upgrade-compatibility.md` | g |
| G-9 | Release images are published for every supported architecture (multi-arch, at least amd64 and arm64) | Not met | `.github/workflows/image.yml` builds one image per matrix entry and sets no `platforms:`, so it publishes the runner architecture only | g |

## Waivers

A waiver lists the criterion, why it is waived, the risk accepted, who
signed it off (a maintainer, by name, in the PR that adds the row) and the
date. Waivers are re-reviewed at every release.

| Criterion | Reason | Risk accepted | Signed off by | Date |
|---|---|---|---|---|
| _(none)_ | | | | |

## Summary (2026-10-04)

Met: X-1 to X-10, A-4, B-4, D-1, D-2, F-1, G-8. Pending-dependency: X-11,
E-8, F-2, F-5. Partially met: D-7, E-7 (ADR 0073 Phase 3 landed 2026-10-05). Not met: every remaining row (A-1 to A-3, B-1 to B-3, all of C, D-3
to D-8, E-1 to E-6 and E-9, F-3, F-4, G-1 to G-7, G-9). The project is therefore
**pre-alpha**.
