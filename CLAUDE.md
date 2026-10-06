# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

It is deliberately a **thin, method-focused entry point**: how to work here, the
load-bearing constraints, and a map of where things live. It does **not** restate
design *rationale* (that lives in the ADRs, `docs/adr/`), per-crate *mechanism*
(that lives in each crate's `CLAUDE.md`), or the accumulated *lessons log*
(that lives in [`docs/engineering-lessons.md`](docs/engineering-lessons.md)).
Those are the source of truth — keep *them* current on decisions and details,
not this file.

## Session operating mode (binding defaults)

Every agent session on this repo boots into this posture — it is a maintainer
standing instruction, not a preference to rediscover mid-task. The session-start
hook re-injects a summary at boot; treat a violation like a failed gate.

1. **The main thread orchestrates; Sonnet subagents do the heavy lifting.**
   Delegate to a Sonnet subagent any work that would pull substantial file
   content, build output, or test output into the main context: code
   exploration, multi-file implementation, gate runs. One investigation agent
   per issue, one implementation agent per change. Brief each subagent with
   the relevant crate guides, the applicable lessons-log sections, and an
   explicit validation gate; verify its committed state on completion rather
   than trusting its report alone. Inline main-thread work is for **trivial
   tasks only**: a one-liner, a doc tweak, a targeted read or grep.

2. **Subagents run in the background; the main thread stays responsive.**
   Never park the conversation behind a foreground subagent. While agents
   work, the main thread remains available to the maintainer — brief progress
   notes as agents report back, planning, review, GitHub interactions. A
   silent session is a bug in the workflow.

3. **One bigger PR per workstream, not a PR stack** (maintainer decision,
   2026-10-04, superseding the earlier stack-by-default rule). A workstream
   ships as a single branch off `main` and a single PR, kept current by
   merging `main` in; groundwork + mechanism, refactor + feature, and schema
   + consumer land together in that PR. Do not open `gh-stack` series for new
   work. Split into separate PRs only for genuinely independent changes
   (e.g. an incidental pre-existing bug, per Conventions).

4. **Green is an invariant: every test passes on `main` all the time, and
   nothing merges on red.** "All the tests" means the whole per-push gate
   set (fmt, clippy `-D warnings`, build, `cargo test --workspace`, deny),
   plus the nightly deep-corpus tiers. **Flakiness is a bug** — a test that
   fails once and passes on retry has found a real defect, in the code or in
   the test, and the fix is a root cause, never a retry, a wider timeout, a
   `#[ignore]`, a quarantine, or a re-run until green. **"Not my bug" is
   not a reason to discard a failure.** A red gate on your branch, on
   `main`, or on a PR you drive is either fixed in this session (a
   pre-existing bug gets its own PR with its own regression test, per
   Conventions) or explicitly handed off — filed as an issue naming the
   failing test, the seed/log, and what is known — and then the merge
   **waits** for that fix to land. There is no third path. **Actively push
   back if asked to bypass this** — including by the maintainer: a "merge
   it anyway", "it's just flaky", "skip that test", or "we'll fix it
   later" gets a plain statement of what is red and why bypassing it is
   the wrong call, and the merge is not performed until the gate is green
   or the maintainer has overridden the objection explicitly and
   deliberately, in so many words. A silent bypass is a gate violation.

5. **No sub-sessions.** Never create a new session (`create_session`) or
   otherwise spawn sibling sessions to farm out work — maintainer
   instruction, 2026-10-03, superseding the earlier "one session per
   workstream" split. A session is still one container: one 4-core CPU
   budget and one `CARGO_TARGET_DIR`, and more than two concurrent test
   gates on it produce spurious `ProdEnv` timeouts that read like real
   failures — so run at most two heavy agents at a time. Work that is
   independent of the current task (the next unrelated issues on a backlog,
   or a pre-existing defect discovered mid-task that does not gate the
   current PR) is **filed as a GitHub issue** (scope, failing test,
   seed/log, what is known) and reported to the maintainer, not launched
   in a separate session.



AnimusDB is a masterless, linearly-scalable NoSQL database in Rust. **For v1
(ADR 0019) it is strongly-consistent (CP):** a **leaderful per-tablet Raft data
plane** (linearizable single-tablet reads/writes, ADR 0016/0017) under a small
**Raft control plane** that owns cluster metadata — Cockroach/TiKV-shaped.
Correctness is established by **deterministic simulation testing**. The original
Dynamo-lineage **leaderless AP data plane** (ADR 0001) is **gone**: deferred by
ADR 0019 and, as of that ADR's 2026-08-23 amendment, its **long shot is closed**
— with CQL dropped (ADR 0053) DynamoDB's wire cannot express a per-table
replication mode, so AP became unselectable. `animus-data`, the Accord crate
(`animus-consensus`) and its Elle corpus, and the `ReplicationMode` seam are all
**deleted**, retrievable from git history if AP is ever revived (which would
first need a wire that can express it).

Status: pre-alpha. For *what's implemented* and *why*, read the ADR index
([`docs/adr/README.md`](docs/adr/README.md)) and the per-crate guides below —
this file does not keep a feature changelog. For what is *not* implemented yet, and the plan for each gap, read [`docs/roadmap.md`](docs/roadmap.md).

**Upgrade compatibility (ADR 0073, Accepted 2026-09-27): a staged ratchet, not
a blanket "no back-compat" any more.** Phase 0 — the last permitted
incompatible reset (every persisted/wire format gained a version tag and a
golden fixture; version counters restarted at 1) — is **done, and the
baseline is set: `9a9f972f` (2026-09-29, see ADR 0073)**. From it on:
**durable formats must be compatible** — a newer binary reads everything an
older post-baseline binary wrote, an existing golden fixture is never edited
or deleted (`scripts/check-format-fixtures.sh` enforces this in CI), and a
format change is a new version tag plus a new fixture, never a rewrite of an
old one. **Wire formats** follow the same rule as of Phase 2 (done: a
replicated cluster version and feature gates, below); **rolling upgrades**
are supported as of Phase 3 (done, below); **Support window: every post-baseline version stays readable forever**
(2026-09-30) — old decoders and fixtures are never deleted. **Phase 1 is done (2026-10-03):** every durable format has a
version-dispatching decoder with a `legacy` seam and a per-version fixture
test, and the upgrade-restart harness (`animus-test` tiers 0/1,
`animusd` `sim_cluster_upgrade_corpus` tier 2; per-push at K=1, nightly in
`corpus-deep.yml`) restarts on state transcoded to older versions. So a
**whole-cluster stop → upgrade → restart is supported and tested**;
**Cluster version is 2 since 2026-10-05** (`MAX_SUPPORTED = 2`, first real gate `Gate::GlobalTables` for `ConvertTableToGlobal`; B2 is pinned to `[1,1]`, ADR 0073's 2026-10-05 amendment). **Phase 2 is done (2026-10-04):** a replicated cluster version and feature
gates (`Gate`, `ClusterFeatures`, `GatedCommand::required_gate`, ADR 0073
sections 1-4 and 8), a mixed-version corpus (both tiers, negative controls,
`ANIMUS_UPGRADE_SEEDS`), and a **manual node-by-node rolling upgrade with no
stop, from today's Phase 1 binaries to B2 and from release R-1 to R**
(`animus cluster version` / `animus cluster finalize`; skipping a release,
rolling a node back and a Phase 1 binary joining after the era started are
not supported). **Phase 3 is done (2026-10-05): rolling-upgrade orchestration**
(ADR 0073's Phase 3 design and as-built amendments). Supported: a **manual roll
with the CLI** (`animus cluster roll plan|wait|status`, `GET /admin/roll-health`,
`animus cluster finalize`; one node at a time, control leader last after a
leadership transfer, no drain) and an **operator-orchestrated roll** (a
`spec.image` or other pod-template edit rolls an `AnimusCluster` of at least
three nodes one pod at a time behind an operator-owned `StatefulSet` partition and
the shared `animus-roll` gate; finalize manual by default, `spec.upgrade.finalize:
Auto` opt-in; `docs/runbook/upgrade.md`). Still no rollback once a node has run the
new binary, no skipped release, no mid-roll image revert. **Open:** issues
#1237 (ungated `txn-envelope` v2 intent can panic an N-1 replica) and #1238 (acked
writes lost across a roll with transactions), found by the `upgrade-previous-release`
CI job and fixed separately — until they land, do not roll while multi-key
transactions are in use; #1235 (a `SimCluster` Memory-backend restart oddity,
test-only); the nightly `kind` operator-roll leg (`E2E_UPGRADE=1`) has not had a
verified run; D4(b), a replicated maintenance mark that suppresses repair churn during
a roll, is a pending maintainer decision (measured: ADR 0073 as-built). The
first real bumps have landed (2026-10-03, #1140/#1141/#1142): `control-wal`,
`shared-wal` and the LSM WAL (`lsm-wal`, `LWL1`) are v2 (WAL sync markers) and
the harness transcodes them to v1 for real; `raftkv-wal` is v2 too (embedded
in the control-wal carrier); `txn-envelope` (the value-envelope intent tag) is v2
since 2026-10-04 (an intent carries its prior value, ADR 0018; an engine-row value, so the harness
down-converts it to v1 with a row transcode, `animus-test` `ROW_TABLE`; class G as well since
2026-10-05: the snapshot sender ships v1 until `Gate::GlobalTables` opens, #1237); every other format is still v1,
transcoded as the identity. **A format change follows ADR 0073's "Phase 1 design"
checklist** ([`docs/adr/0073-upgrade-compatibility.md`](docs/adr/0073-upgrade-compatibility.md):
bump, keep the vN decoder under `legacy`, new no-overwrite fixture,
per-version expected value, round-trip and old-input tests, test-only legacy
encoder, inventory row) **plus, for any cross-node surface (a wire or
replicated shape), the Phase 2 step: classify it G (gated on the cluster
version or the era), L (node-local) or F (outlives the cluster), and for G
name its `Gate` (an exhaustive `required_gate` row, no `_` arm; emit sites
check it, a per-gate test and a mixed-version corpus cell cover it); a new
cross-node variant or field without a gate wedges older replicas**. See ADR 0073 for the phase
plan, the conventions (tag shape, fixture layout), and the open questions
(support window length, the hash-ring/key-encoding layer). A break that
can't be made compatible needs an explicit ADR amendment naming it and its
migration path.

## Per-crate guides

Each crate has its own `CLAUDE.md` with local entry points and gotchas — read
the relevant one before working in a crate:

| Crate | Guide |
|-------|-------|
| `animus-env` | [crates/animus-env/CLAUDE.md](crates/animus-env/CLAUDE.md) |
| `animus-sim` | [crates/animus-sim/CLAUDE.md](crates/animus-sim/CLAUDE.md) |
| `animus-storage` | [crates/animus-storage/CLAUDE.md](crates/animus-storage/CLAUDE.md) |
| `animus-tablet` | [crates/animus-tablet/CLAUDE.md](crates/animus-tablet/CLAUDE.md) |
| `animus-control` | [crates/animus-control/CLAUDE.md](crates/animus-control/CLAUDE.md) |
| `animus-cp-data` | [crates/animus-cp-data/CLAUDE.md](crates/animus-cp-data/CLAUDE.md) |
| `animus-item` | [crates/animus-item/CLAUDE.md](crates/animus-item/CLAUDE.md) |
| `animus-roll` | [crates/animus-roll/CLAUDE.md](crates/animus-roll/CLAUDE.md) |
| `animus-test` | [crates/animus-test/CLAUDE.md](crates/animus-test/CLAUDE.md) |
| `animus-dynamo` | [crates/animus-dynamo/CLAUDE.md](crates/animus-dynamo/CLAUDE.md) |
| `animus-s3` | [crates/animus-s3/CLAUDE.md](crates/animus-s3/CLAUDE.md) |
| `animus-placement` | [crates/animus-placement/CLAUDE.md](crates/animus-placement/CLAUDE.md) |
| `animus-node` | [crates/animus-node/CLAUDE.md](crates/animus-node/CLAUDE.md) |
| `animusd` | [crates/animusd/CLAUDE.md](crates/animusd/CLAUDE.md) |
| `animus-cli` | [crates/animus-cli/CLAUDE.md](crates/animus-cli/CLAUDE.md) |
| `animus-bench` | [crates/animus-bench/CLAUDE.md](crates/animus-bench/CLAUDE.md) |
| `animus-operator` | [crates/animus-operator/CLAUDE.md](crates/animus-operator/CLAUDE.md) |

## Commands

```sh
cargo build --workspace --all-targets
cargo test --workspace
cargo test -p animus-control                       # one crate
cargo test -p animus-control --test it             # the crate's merged SimEnv/pure binary (tests/it/main.rs)
cargo test -p animus-control --test it control_raft::   # one former test file (now a module of `it`)
cargo test -p animus-control survives_leader_kill  # one test by name substring
cargo nextest run --workspace --exclude animusd    # what CI's sharded `gates` tier runs (+ `cargo test --workspace --exclude animusd --doc`)
cargo nextest run -p animusd --lib --profile gates # animusd's SimCluster `--lib` tier
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all --check
cargo deny check                                   # licenses + advisories (cargo install cargo-deny)
scripts/check-format-fixtures.sh                   # ADR 0073 Phase 0: fails if a checked-in format fixture was edited/deleted
(cd fuzz && cargo test --release --test smoke)    # R-01 (c): deterministic stable smoke of every fuzz target (real libFuzzer: fuzz/README.md)
cargo bench -p animus-storage                      # ProdEnv smoke of the write/IO path
cargo bench -p animusd                             # cluster wire benchmark: latency percentiles + degraded phase
cargo run --release -p animus-bench -- --help      # open-loop YCSB A-F load generator over the DynamoDB wire (docs/benchmarks.md, ADR 0076)
cargo bench -p animus-cp-data --bench wal_fsync_bench  # ProdEnv WAL fsync bench gating SharedWal wiring (ADR 0028, C-05)
```

**Integration-test layout (consolidated 2026-10-02).** Each crate's
SimEnv/pure integration tests are modules of ONE binary, `tests/it/main.rs`
(`mod foo;` per former `tests/foo.rs`, which now lives at `tests/it/foo.rs`),
so each crate links once instead of once per file. Select a former file with
`cargo test -p <crate> --test it foo::` (a name filter on the module path —
check it with `-- --list` when adding a CI step: a filter that matches nothing
passes green). A new SimEnv/pure test goes in `tests/it/` plus a `mod` line;
a test that needs real threads / `ProdEnv` / real sockets / process-global
state (env vars, cwd, `/etc/hosts`, signal handlers) stays its own
`tests/<name>.rs` target (`animusd`'s 100 `tests/*.rs` all are, and
`prod-heavy` ones keep their `[[test]] required-features`) — merging those
raises in-binary concurrency and brings back the documented spurious
`ProdEnv` timeouts. Rationale: `docs/lessons/testing/2026-10-02-integration-test-binary-consolidation.md`.

All five gates (fmt, clippy `-D warnings`, build, test, deny) must be green; CI
runs them. Green is a standing invariant, not a per-PR aspiration — see
Session operating mode item 4 (a flaky test is a bug; nothing merges on red;
a failure you didn't cause is still yours to fix or hand off). Commits require a DCO sign-off (`git commit -s`); this repo is also
set up for GPG-signed commits.

### Replaying a failed simulation

Every simulation run is a pure function of its seed. Tests print the seed in
assertion messages; replay with `ANIMUS_SEED=<seed> cargo test <name>`. The
`Simulator` is driven by `Simulator::new(seed)`.

### Test-scaling and bench knobs

| Env var | Default | Effect |
|---------|---------|--------|
| `ANIMUS_SEED` | unset | replay one sim run from its printed seed |
| `ANIMUS_RAFTKV_SEEDS=K` | 1 | raftkv-corpus depth (`animus-test`) |
| `ANIMUS_RAFTKV_LSM=1` | off | run the whole raftkv corpus over `LsmEngine<SimEnv>` |
| `ANIMUS_RAFTKV_WAL_FAULTS=1` | off | run a second pass of the raftkv corpus's crash-based cells (`LeaderKill`/`FollowerKill`) with `torn_tail_on_crash`+`corrupt_on_crash` armed for the whole run |
| `ANIMUS_UPGRADE_RESTART_SEEDS=K` | 1 | upgrade-restart corpus depth, tiers 1 and 2 (ADR 0073 P1-D): tier 1 `animus-test` `tests/it/upgrade_restart_corpus.rs` — K seeds per cell (21 cells); tier 2 `animusd` `sim_cluster_upgrade_corpus` (whole-cluster restart over `SimCluster`'s `LsmEngine` backend + the DynamoDB wire, 3 cells; `cargo test -p animusd --lib sim_cluster_upgrade_corpus`); `ANIMUS_UPGRADE_RESTART_CELL=<substring>` narrows to matching cells (combine with `ANIMUS_SEED=<seed>` to replay one). |
| `ANIMUS_UPGRADE_SEEDS=K` | 1 | mixed-version corpus depth (ADR 0073 Phase 2, P2-D; distinct from `ANIMUS_UPGRADE_RESTART_SEEDS`): pure tier `animus-control` `tests/it/version_mixed_corpus.rs` (`cargo test -p animus-control --test it version_mixed_corpus::`, 8 cells) and `animusd` `sim_cluster_mixed_version_corpus` (rolling Phase 1 -> B2 over `SimCluster`, `cargo test -p animusd --lib sim_cluster_mixed_version`) and, since ADR 0073 Phase 3 P3-C, `animusd` `sim_cluster_roll_orchestrator` (the `animus-roll` roll driver over `SimCluster`'s LSM backend, 12 cells, `cargo test -p animusd --lib sim_cluster_roll_orchestrator`); `ANIMUS_UPGRADE_CELL=<substring>` narrows cells, `ANIMUS_SEED=<seed>` replays one. Needs `animus-control`'s `sim-versions` feature (enabled via `animus-test`) |
| `E2E_UPGRADE`, `ANIMUSD_IMAGE_PREV`, `E2E_UPGRADE_ROLL_TIMEOUT`, `UPGRADE_CLIENT_IMAGE` | 0 / `animusd:e2e-prev` / 1500 / `curlimages/curl:8.10.1` | `kind` operator-driven rolling-upgrade leg of `scripts/e2e-kind.sh` (ADR 0073 Phase 3, D10; nightly `.github/workflows/upgrade-kind-nightly.yml`, not per-push): `E2E_UPGRADE=1` bootstraps on `ANIMUSD_IMAGE_PREV` (the previous release, built from `scripts/upgrade-from.txt`) with `spec.upgrade.finalize: Auto`, starts an in-cluster retrying write client, edits `spec.image` to `ANIMUSD_IMAGE`, and asserts a gated roll (one pod unavailable at most), finalize, and no lost/stalled acked write; plain-TCP only; **unverified** (kind cannot run in the sandbox) |
| `ANIMUS_UPGRADE_FROM_BIN` / `_REF` / `_REPORT_DIR` / `_RESTART_GAP_SECS` / `_TXN` / `_CONTROL`, `ANIMUS_CLI_BIN` | required / — / — / 0 (12 on the 4-node variant) / unset | previous-release rolling-upgrade `ProdEnv` test (ADR 0073 Phase 3 P3-E, D10; `animusd` `tests/upgrade_previous_release.rs`, `upgrade-from` feature, CI job `upgrade-previous-release`): `_BIN` = the pinned R-1 `animusd` (`scripts/build-upgrade-from.sh`, pin in `scripts/upgrade-from.txt`; **a missing binary fails the test, it never skips**); `_REF` labels it in the report; `_REPORT_DIR` receives the per-variant JSON (rolling steps, D4 repair churn) and, on a failure, history/op-trace/node logs; `_RESTART_GAP_SECS` keeps each node down that long before it restarts (past the 5 s repair dwell the D4 rebuild traffic shows); `_TXN=1` turns multi-key transactions on (off by default: against the pinned `ac57d56a` they expose three known defects, see the test's "Known findings"); `_CONTROL=same-binary\|current-only` runs the same roll with no binary change (triage: mixed-version vs restart/repair defect); `ANIMUS_CLI_BIN` overrides the `animus` binary (default: next to `animusd`). Run: `ANIMUS_UPGRADE_FROM_BIN=$(scripts/build-upgrade-from.sh) cargo test -p animusd --features upgrade-from --test upgrade_previous_release -- --nocapture --test-threads=1` |
| `ANIMUS_MRSC_SEEDS=K` | 1 | **two corpora share this knob** (ADR 0075, G-01 G-c; each corpus step in `corpus-deep.yml` sets it separately): (1) pure tier, `animus-cp-data` `preferred_leader_corpus` (M2) — 3 regions x 1 node over WAN links, real `host::Reconciler` with `MetadataView.preferred_leader`; cells + negative control (empty preferred map leaves the leader outside the preferred region) — `cargo test -p animus-cp-data --test it preferred_leader_corpus::`; (2) cluster tier, `animusd` `sim_cluster_mrsc` (M4) — 6 nodes, 3 regions x 2, WAN links, LSM engine, table converted over the DynamoDB wire; one `sim_cluster_mrsc_corpus_<cell>` test per cell (steady, region loss leader/follower, partition heal, split, in-region replacement, witness form, decommission guard) + 3 negative controls — `cargo test -p animusd --lib sim_cluster_mrsc`; one seed is ~8 CPU-minutes in a debug build. `ANIMUS_MRSC_CELL=<substring>` narrows (both), `ANIMUS_SEED=<seed>` replays one |
| `ANIMUS_RECONCILER_SEEDS=K` | 1 | reconciler-corpus depth (`animus-cp-data`) |
| `ANIMUS_TXN_SEEDS=K` | 1 | multi-tablet cross-transaction corpus depth (`animus-test`, ADR 0018) |
| `ANIMUS_STREAM_SEEDS=K` | 1 | DynamoDB Streams lineage-walk corpus depth (`animus-test`, ADR 0042/0043) |
| `ANIMUS_BACKFILL_SEEDS=K` | 1 | secondary-index backfill fault-injection corpus depth (`animus-test`, ADR 0045) |
| `ANIMUS_QUIESCE_SEEDS=K` | 1 | idle-tablet-group quiescence corpus depth (`animus-cp-data`, ADR 0044 phase 1) |
| `ANIMUS_SPLIT_SEEDS=K` | 1 | `KvCommand::SeedBatch` corpus depth (`animus-cp-data`) — the version-carrying row-merge command originally built for the now-deleted copy-based split driver (ADR 0050 Train B), its sole surviving consumer is the restore driver (ADR 0059 §7) |
| `ANIMUS_LEARNER_PENDING_CHECK_SEEDS=K` | 8 (floor) | grown-group-elects-after-leader-loss corpus depth with a learner whose boot-time cluster check is pending (`animus-control`, `tests/learner_promotion_pending_check.rs`, issue #1131) |
| `ANIMUS_LEARNER_SEEDS=K` | 1 | learner (non-voting) membership-class fault-injection corpus depth (`animus-control`, ADR 0058 Train 1) |
| `ANIMUS_RECONFIGURE_DROP_SEEDS=K` | 1 | healthy-voter-drop reconfigure corpus depth (`animus-cp-data`, issue #781) — a live follower, and separately the leader, dropped via a direct `CasTabletReplicas` through the real `spawn_reconfigure_loop`/`reconfigure_step` |
| `ANIMUS_CONTROL_SEEDS=K` | 1 | control-plane machinery (apply task, schema-catalog exclusivity) fault-injection corpus depth (`animus-control`) |
| `ANIMUS_INPLACE_SPLIT_SEEDS=K` | 1 | in-place split group-mint-at-apply fault-injection corpus depth (`animus-cp-data`, ADR 0058 Train 2 rung 3) |
| `ANIMUS_BACKUP_SEEDS=K` | 1 | on-demand backup capture fault-injection corpus depth (`animus-test`, ADR 0059 Train 1) |
| `ANIMUS_PITR_SEEDS=K` | 1 | PITR sealing fault-injection corpus depth (`animus-test`, ADR 0059 Train 3) |
| `ANIMUS_LSM_CRASH_SEEDS=K` | 1 | `LsmEngine` crash-safety corpus depth (`animus-storage`, `tests/lsm_crash.rs`) |
| `ANIMUS_LSM_DISK_FAULT_SEEDS=K` | 1 | `LsmEngine` `DiskConfig` fault-injection corpus depth (`animus-storage`, `tests/lsm_disk_faults.rs`) |
| `ANIMUS_LSM_ENCRYPTED_SEEDS=K` | 1 | `LsmEngine<EncryptedEnv<SimEnv>>` crash/fault-injection corpus depth (`animus-storage`, `tests/lsm_crash_encrypted.rs`, ADR 0069) |
| `ANIMUS_SEGMENT_STORE_ENCRYPTED_SEEDS=K` | 1 | `EncryptedSegmentStore` fault-injection corpus depth (`animus-test`, `tests/segment_store_encrypted_fault_corpus.rs`, ADR 0069 S-03 PR 2) |
| `ANIMUS_SIMCLUSTER_SEEDS=K` | 1 | multi-node/multi-tablet `SimCluster` cycles/durability corpus depth (`animusd`, ADR 0061 rung D1) — run via `cargo test -p animusd --lib sim_cluster_corpus` |
| `ANIMUS_DYNAMO_WIRE_SEEDS=K` | 1 | end-to-end DynamoDB-wire cycles/durability corpus depth over `SimCluster` (`animusd`, ADR 0061 rung D2 PR 2) — run via `cargo test -p animusd --lib sim_cluster_dynamo_corpus` |
| `ANIMUS_SHAREDWAL_SEEDS=K` | 1 | `SharedWal` cross-tablet ordering/crash-safety/GC fault-injection corpus depth (`animus-cp-data`, ADR 0028, C-05 — on by default since PR 3's cutover) — `cargo test -p animus-cp-data --test it sharedwal_fault_corpus::` |
| `ANIMUS_WAL_REWRITE_CRASH_SEEDS=K` | 1 | staged WAL-compaction-rewrite crash corpus depth (`animus-cp-data`, issue #1116) — whole-cluster power-cut at 12 offsets into a stalled rewrite, plain and torn/corrupt tails; every acked write must survive — `cargo test -p animus-cp-data --test it wal_rewrite_crash::` |
| `ANIMUS_ZONE_PLACEMENT_SEEDS=K` | 1 | zone-labelled placement + whole-zone-loss corpus depth over `SimCluster` (`animusd`, G-01 stage G-a) — 6 nodes, 2 per zone, RF 3: every tablet spans 3 zones, kill a zone, no acked write lost, repair re-converges — `cargo test -p animusd --lib sim_cluster_zone_placement` |
| `ANIMUS_SPLIT_RELOCATION_SEEDS=K` | 1 | split-child wholesale-relocation corpus depth over `SimCluster` (`animusd`, issue #1229) — a child moved entirely off the parent's replicas by directed Placing must keep its pre-split rows; two cells (`MemoryEngine`; `LsmEngine` + rotating node restarts) — `cargo test -p animusd --lib sim_cluster_split_relocation` |
| `ANIMUS_EXPORT_IMPORT_SEEDS=K` | 1 | S3 export/import fault-injection corpus depth (`animus-test`, ADR 0068, S-05 PR 3) |
| `ANIMUS_S3_FAULT_SEEDS=K` | 1 | S3 store retry/backoff fault-injection corpus depth (`animus-test`, `tests/it/s3_fault_corpus.rs`, S-08 M3) — `S3SegmentStore<FaultyTransport<FakeS3>, SimEnv>`: 5xx/429/timeout/lost-ack bursts, multipart part/Complete failures, expiring and failing credential providers; `ANIMUS_SEED=<seed>` replays one seed per cell — `cargo test -p animus-test --test it s3_fault_corpus::` |
| `ANIMUS_HEARTBEAT_SEEDS=K` | 1 | per-node heartbeat-batcher fault-injection corpus depth (`animus-cp-data`, ADR 0044 phase 2, C-02 PR 2) — run via `cargo test -p animus-cp-data --test it heartbeat_batch_corpus::` |
| `ANIMUS_WAN_TIMING_SEEDS=K` | 1 | per-group WAN Raft timing profile corpus depth (`animus-cp-data`, ADR 0075 section 3.4, G-01 stage G-c groundwork) — 3 regions at 60-90ms one-way with tail latency: steady, leader-node kill and leader-region partition+heal must keep bounded term churn and commit durably under the WAN profile, and a LAN-forced negative control must show election churn — `cargo test -p animus-cp-data --test it wan_timing_corpus::` |
| `ANIMUS_DIRECTED_PLACING_LOAD_SEEDS=K` | 1 | directed-Placing (2-of-3 replica diff) learner-promotion-under-a-continuous-writer corpus depth (`animus-cp-data`, issue #1064) — `cargo test -p animus-cp-data --test it directed_placing_under_sustained_load::` |
| `ANIMUS_LEARNER_SNAPSHOT_LIVELOCK_SEEDS=K` | 1 | late-joining-learner-needing-a-real-InstallSnapshot-under-a-continuous-writer corpus depth (`animus-cp-data`, issue #1064 part 2) — `cargo test -p animus-cp-data --test it learner_snapshot_livelock_under_continuous_writer::` |
| `ANIMUS_RELEASE_RACE_SEEDS=K` | 1 | release-vs-promote race corpus depth (`animus-cp-data`, `tests/release_race_corpus.rs`, ADR 0031's 2026-09-30 amendment) — a mid-catch-up learner must never be released/erased by the host reconciler nor refused as a voter on re-host |
| `ANIMUS_RESTAGE_SEEDS=K` | 2 | stale-restage-after-resolve replica-determinism corpus depth (`animus-cp-data`, `tests/it/resolved_restage_replica_determinism.rs`, issue #1243) — a duplicate `TxnStage` for an already-resolved txn must apply identically on a restarted replica and on a snapshot-installed one; `ANIMUS_SEED=<seed>` replays one — `cargo test -p animus-cp-data --test it resolved_restage` |
| `ANIMUS_CHAOS_SEED=S` | per-scenario name hash | seed of the real-cluster chaos harness's **fault schedule** (`animusd`, `tests/chaos.rs`, R-01 b, `docs/chaos.md`); the processes are real, so a replay is the same faults against a similar, not identical, execution. Opt-in: `cargo test -p animusd --features chaos --test chaos -- --test-threads=1` |
| `ANIMUS_CHAOS_SECS=N` | 90 (smoke) / 150 | length of the chaos fault window in seconds (the long run: `ANIMUS_CHAOS_SECS=900 … chaos_mixed`); also `ANIMUS_CHAOS_NODES` (3), `ANIMUS_CHAOS_TABLETS` (4), `ANIMUS_CHAOS_RECOVERY_SECS` (60), `ANIMUS_CHAOS_TXN=0`, `ANIMUS_CHAOS_DIR`, `ANIMUS_CHAOS_OUT` |
| `ANIMUS_SOAK_DURATION=D` | `10m` | total length of the real-process soak (`animusd`, `tests/soak.rs`, R-01 (a), opt-in `soak` feature, `docs/soak.md`); also `ANIMUS_SOAK_{SEED,NODES,TABLETS,EPOCH_SECS,SAMPLE_SECS,WARMUP,PACE_MS,RESTART_EVERY,DIR,OUT}` |
| `ANIMUS_DISK_FULL_SEEDS=K` | 1 | disk-full (ENOSPC) corpus depth (`animus-test`, `tests/it/raftkv_linearizable.rs`, R-01 (d), issue #1185) — 8 cells of ENOSPC windows (every replica, leader only, flaky) over `MemoryEngine` only (LSM-engine ENOSPC is unhandled, never combine with `ANIMUS_RAFTKV_LSM=1`); asserts no acked write lost/duplicated and resume without restart — `cargo test -p animus-test --test it raftkv_linearizable::raftkv_disk_full`; `ANIMUS_SEED=<seed>` replays one |
| `ANIMUS_SHRINK=1` | off | when a corpus scenario fails, delta-debug it to a minimal reproducing case and print a replayable handle (`animus-test::shrink`, ADR 0061 rung B4) |
| `ANIMUS_SHRINK_MAX_CHECKS=N` | 500 | iteration budget for `ANIMUS_SHRINK`'s search (a plain check count, not wall-clock time — see `animus-test/CLAUDE.md`) |
| `ANIMUS_SHRINK_REPLAY=<json>` | unset | replay a minimized scenario a shrink run printed (per-corpus entry point, e.g. `raftkv_shrink_replay` in `raftkv_linearizable.rs`) |
| `ANIMUS_BENCH_{KEYS,GETS,SCAN,VALUE_BYTES,APPLY_BATCH}` | — | `animus-storage`'s `engine_bench` workload tuning |
| `ANIMUS_BENCH_{NODES,ITEMS,OPS,VALUE_BYTES,CLIENTS,JSON}` | — | `animusd`'s `cluster_bench` workload tuning (node count, preload size, measured ops/class, item size, concurrent-client sweep, JSON output path) |
| `ANIMUS_BENCH_GROUPS` | `1,8,32,128` | `animus-cp-data`'s `wal_fsync_bench` active-tablet-count sweep (per-group-files vs. `SharedWal` fsync/latency comparison, C-05 PR 1) |
| `ANIMUS_BENCH_ROUNDS`/`ANIMUS_BENCH_VALUE_BYTES`/`ANIMUS_BENCH_JSON` | `20`/`96`/unset | `wal_fsync_bench`'s own round count, per-write payload size, and JSON output path (same knob name/shape as the other two benches above) |

The deep corpus tiers run nightly in CI
(`.github/workflows/corpus-deep.yml`), not per-push.

## The load-bearing constraint: determinism

This is the single most important rule (ADR 0003). **All nondeterminism flows
through the `Env` seam.** In every crate except `animus-env`'s `ProdEnv` and
test code:

`ProdEnv`/`FsSegmentStore` live behind `animus-env`'s default-off `prod`
Cargo feature (ADR 0061 rung C0): a crate that depends on `animus-env` with
`default-features = false` cannot name `ProdEnv` at all — it fails to
compile, not just fails review. Only a crate whose own library really
constructs one (currently `animusd`) enables `prod` on its normal
dependency; a crate that only needs it for a real-thread test/bench enables
it on a separate `[dev-dependencies]` entry instead. See
`crates/animus-env/CLAUDE.md` for the full breakdown.

- No wall clock — use `env.now()` / `env.sleep()`, never `std::time` or
  `tokio::time`. The **one** exception is `env.wall_now()` (ADR 0051), which
  returns calendar time for interpreting externally-supplied absolute
  timestamps — a DynamoDB TTL attribute and nothing else so far. It is still
  inside the seam (`SimEnv` derives it from virtual time, so it stays
  seed-reproducible), but it is **never** for timing: every deadline,
  timeout, election, and backoff keeps using `env.now()`, which cannot step
  backwards. **Lint-enforced** (`Instant::now`/`SystemTime::now`/
  `tokio::time::{sleep,timeout}`, ADR 0061 rung B5).
- No raw task spawning — use `env.spawn_task(..)`, never `tokio::spawn`.
  **Lint-enforced** (`tokio::spawn`, ADR 0061 rung B5).
- No real I/O — use `env.send`/`recv` and `env.append`/`sync`/`read`, never
  `std::net`/`std::fs`/`tokio::{net,fs}`. **Not** lint-enforced (ADR 0061
  rung B5 judged it impractical — no single small replacement to name in a
  `reason` string, and `animusd`'s listener binding alone would need dozens
  of individually-meaningless allows); reviewed by hand.
- No unseeded randomness — use `env.next_u64()` / `env.gen_below(..)`, never
  `thread_rng`/`OsRng`. **Lint-enforced** (`thread_rng` via
  `disallowed-methods`, `OsRng` via `disallowed-types` since it's a type not
  a function; ADR 0061 rung B5).
- **No `HashMap`/`HashSet` in logic** — their iteration order is
  nondeterministic. Use `BTreeMap`/`BTreeSet`. This is lint-enforced via
  `clippy.toml`.

Every lint-enforced item above is `clippy.toml`'s `disallowed-methods`/
`disallowed-types`, workspace-wide via `[workspace.lints.clippy]` — a
legitimate exception (`animus-env`'s `ProdEnv`, a real-thread `ProdEnv`
liveness test, `animus-cli`/`animus-operator`'s real-socket process
boundaries) carries an individually-justified `#[allow(clippy::
disallowed_{methods,types}, reason = "...")]`. **`animusd` is exempted at
the package level instead** (`crates/animusd/Cargo.toml`'s `[lints.clippy]`
override) — it is ADR 0061's own pre-Phase-C process boundary, with ~600
real call sites the ADR judged genuinely unreasonable to hand-annotate;
`disallowed_types` stays enforced there, only the methods half is off. **That
exemption is not crate-wide any more**: ADR 0061 Phase C's closing rung put
an explicit `#[deny(clippy::disallowed_methods)]` on the `mod` declarations
of `animusd`'s five `E: Env`-generic client-path modules (`schema`,
`read_path`, `write_path`, `txn_coordinator`, `forwarding`) in `lib.rs`, so
a reintroduced `Instant::now`/`tokio::spawn`/`tokio::time::{sleep,timeout}`
there is a build failure. The package-level allow now covers only the code
that genuinely is the process boundary — `lib.rs`, `dynamo.rs`, the wire
edges, the remaining loops, and the test/bench targets. Narrow it further as
more of `animusd` becomes seam-clean; never widen it back to make a change
compile. See ADR 0003's 2026-08-28 note (3), ADR 0061 Decision 4's as-built
note, and ADR 0061's seventh 2026-08-28 amendment (why this deny is the
enforcement the planned crate boundary was going to provide) for the full
account.

Components are generic over `E: Env` (monomorphized, never `dyn`). `Env` is a
supertrait combining `Clock + Rng + Network + Disk + Spawner`, scoped to one
node id. Production wiring uses `ProdEnv` (the only place real time/IO/RNG
live); tests use `animus-sim`'s `SimEnv`.

When a design decision changes, update the relevant ADR in `docs/adr/` in the
same change.

## Architecture map (where things live)

One line of orientation per subsystem — *what it is, its ADR (the why), and the
crate (the mechanism)*. The per-crate `CLAUDE.md` and the ADRs are the source of
truth; this map is just for navigation.

- **`Env` seam + simulator** — `animus-env`, `animus-sim` (ADR 0003). The single
  boundary for time/rng/network/disk/spawn. `SimEnv` is the deterministic
  single-threaded executor; `ProdEnv` the real one. Drive sims with
  `run_for`/`run_until`, never `run()` for protocols with perpetual timers.
  The `Network` is multiplexed `(node, stream)` (ADR 0026) so one node id can
  host several protocol instances; a node's inbox-per-stream is single-consumer.
- **Two planes** — ADR 0001 / ADR 0019. A consistent Raft **control plane**
  (`animus-control`, owns `Metadata` = membership + tablet map + schema catalog)
  vs a **CP data plane** (`animus-cp-data`, leaderful per-tablet Raft,
  linearizable). v1 is CP-only; the original leaderless-AP data plane
  (`animus-data`) is **deleted** (retrievable from git history, ADR 0019).
- **Control-plane Raft** — `animus-control` (ADR 0009; in-house, not openraft, so
  `SimEnv` can drive it). Sync I/O-free `RaftCore<C, S>` + thin `RaftNode<E>`
  driver; pre-vote; leadership transfer; WAL + truncating/chunked-
  `InstallSnapshot` snapshots; epoch-CAS placement; heartbeat **failure
  detection** (ADR 0012); a replicated **table-schema catalog** in `Metadata`
  (ADR 0013); `metadata_watch()` change notification (ADR 0031); the control
  group itself can grow/shrink/replace voters **at runtime** via
  `change_membership`/`transfer_leadership` + an admin API/CLI (ADR 0037).
  `Metadata` is itself `DRIVER_APPLIED` (ADR 0038): a per-node async apply
  task, not the sync core, owns it and durably mirrors it into a per-node
  system-keyspace `StorageEngine`.
- **CP data plane** — `animus-cp-data` (ADR 0016, 0017). Each tablet is its own
  Raft group with a single leader serving **linearizable** single-tablet
  reads/writes/scans, durable on a real `StorageEngine`; reuses the control
  plane's sync `RaftCore` with a KV state machine; ReadIndex reads, compaction +
  streaming `InstallSnapshot`, single-server membership change. **Reads have a
  second, weaker path since ADR 0055**: a `ConsistentRead: false` read (the
  DynamoDB wire default) is served from *any* replica's own applied engine
  state — no read barrier, no leadership, no wake of a quiesced group — behind
  a purely local freshness gate, falling back to the ReadIndex path whenever no
  replica can serve it cheaply. Each hosted
  tablet has its **own private engine** (ADR 0050; keys `kind || logical`,
  identity in the engine's file namespace — the shared-engine
  `StorageScope`/fence machinery of ADR 0028 is gone). The per-node
  **tablet-host reconciler** (`host` module, ADR 0031) is the one
  event-driven loop that hosts/reconfigures/releases/reclaims tablet
  groups (and their engines) from replicated `Metadata`.
- **Partitioning & keys** — `animus-tablet` (ADR 0022, 0023). Every data-plane
  key leads with a Murmur3 **hash-ring token** over the partition key; tablets
  are **table-scoped** (a table's tablets partition its own ring; no table
  prefix in keys). The escape/token primitives live here and must match the
  wire edges byte-for-byte.
- **Tablet lifecycle** — split is, by default, an **in-place atomic fork**
  (ADR 0058, default since rung 4 layer 2): a single Raft entry on the
  parent's own log mints both children directly `Active`, materialized on
  every fork participant from the committed entry, with no separate
  build/freeze phase. **Since ADR 0062 the fork is placement-blind**: both
  children inherit the parent's own current replicas verbatim, and a
  child's actual final home is a separate, directed **Placing** decision
  (`Metadata::split_placing`, computed once at cutover) driven, after
  cutover, by the same replica-rebalancing convergence machinery
  (`reconfigure_step`/`CasTabletReplicas`) that already moves any other
  tablet's placement — never fused into the fork itself. The original
  **copy-based background workflow** (ADR
  0050 — `BeginSplit` mints two `Building` children at placement-chosen
  homes, a driver on the parent's leader copies + tails, a terminal
  `Freeze` stops writes, `CutoverSplit` activates the children and retires
  the parent) — and the `--split-mode {copy,inplace}` selector that used to
  choose between it and the in-place workflow above — was **deleted
  2026-09-01** (the copy-split-deletion stack, ADR 0058's rung 4 layer),
  retrievable from git history if ever needed again; in-place fork is now
  the only split. Lineage is still frozen in `split_lineage`. Auto-split
  triggers on
  **bytes** (ADR 0034, `animusd`), change-rate/ops-rate (ADR 0042 §14/W-09),
  and a provisioned table's own throughput-derived minimum tablet count
  (ADR 0067, W-08b — DynamoDB's own `ceil(RCU/3000 + WCU/1000)` formula,
  on by default); **tablets are split-only** — merge has
  been removed entirely (ADR 0044, supersedes ADR 0033); dropped tables'
  data is reclaimed by a convergent **GC** (ADR 0024). Tablet ids are never
  reused. An idle CP-data group **quiesces** (ADR 0048, phase 1 of ADR
  0044's cheap-groups roadmap): no local activity for `--quiesce-after`
  (default on, 5s; floor 2s, the auto-split sweep period, so a bursty
  tablet is always observed awake by at least one bytes-split sweep — ADR
  0048's issue #992 amendment) stops its Raft timers/heartbeats/apply-poll
  entirely until a write, a peer message, or the reconciler's proactive
  wake (a replica marked `Down`) touches it again — data-plane only (the
  control group never quiesces), remains leader while quiesced, and
  admin/dashboard reads never wake a group (`quiesced` is a pure
  diagnostic).
- **Placement, rebalancing & growth** — `animus-placement` (ADR 0005): pure
  policy engine (RF + residency labels + failure-domain spread), `replan`'s
  growth-only best-effort sibling `replan_repair` (the control plane's own
  repair pass, issue #957 — a policy RF the current candidate pool can't
  fully satisfy still gets grown as far as it genuinely can, e.g. an RF-3
  policy on a 2-node cluster still repairs a 1-replica tablet up to 2,
  rather than refusing to make any progress at all; never shrinks an
  already-at-capacity set) + `rebalance_step` (ADR 0029: one balance-driven move per
  call; converges to max−min ≤ 1 when the policy sets no `SpreadPolicy` — with
  a spread constraint the domain guard can legally block every improving move,
  so only monotonic non-worsening and termination hold, see the property tests
  in `animus-placement/tests/placement_props.rs`). The control-plane leader reconciles
  placement event-driven (ADR 0031). Clusters grow online: new nodes
  self-register and mirror `Metadata` (ADR 0030), join via seed addresses, and
  are decommissioned via drain → remove (ADR 0032).
- **Global tables (MRSC stretch tables)** — ADR 0075 (G-01 stage G-c; MREC and
  federation are G-d/G-e, not built). One cluster whose nodes carry
  `topology.kubernetes.io/region` labels; `UpdateTable` `ReplicaUpdates` +
  `MultiRegionConsistency: STRONG` on an empty table (three Regions, or two plus
  a witness) becomes one `ConvertTableToGlobal` (`animus-control`, behind
  `Gate::GlobalTables`, cluster version 2): every tablet gets a region-pinned,
  one-replica-per-Region policy (`animus-placement` `allowed_values`,
  `replan_pinned`; a lost Region's replica waits, repair happens only inside a
  Region), the host reconciler's **preferred-leader step** keeps leaders in the
  preferred Region and off the witness (`animus-cp-data` `host`), witness
  replicas never serve eventual reads, and `animusd::global_tables` is the wire
  edge + `/admin/global-tables` + the decommission guard. Corpora:
  `preferred_leader_corpus` (pure) and `sim_cluster_mrsc` (cluster), knob
  `ANIMUS_MRSC_SEEDS`. Known gap: #1226 (WAN groups never quiesce).
- **Transaction consensus** — 2PC/HLC over the per-tablet Raft groups (ADR
  0018), the only transaction story. The Accord slice that used to sit here
  (`animus-consensus`, ADR 0011) is **deleted** — rejected for CP by ADR 0018 in
  favour of 2PC-over-Raft, and deferred with AP by ADR 0019, whose 2026-08-23
  amendment removed it outright along with its Elle corpus (ADR 0014).
- **Storage** — `animus-storage` (ADR 0004, 0008). The **async** `StorageEngine`
  trait; `MemoryEngine` (deterministic, for sim) and a custom on-disk
  `LsmEngine<E>` (WAL/SSTable/leveled compaction, all I/O via the `Env` disk seam
  so its crash recovery is sim-tested).
- **Item model** — `animus-item` (ADR 0054 step 1). The pure DynamoDB item
  model — `AttributeValue`/`Item`/`TableSchema`, key encoding, `condition`
  and `index` (GSI/LSI row derivation), the `UpdateExpression` data model and
  its apply-time evaluator, the stored-item codec — extracted from
  `animus-dynamo` to sit **below both** it and `animus-cp-data`, since the
  latter is a protocol-agnostic KV state machine that cannot depend on a wire
  crate; `animus-dynamo` re-exports everything unchanged. No `animus-env`
  dependency, by design — see `crates/animus-item/CLAUDE.md`.
- **Wire adapter** — `animus-dynamo` (ADR 0006; a CQL adapter, `animus-cql`,
  also shipped for a time but was dropped, ADR 0053 — v1 is DynamoDB-only).
  **Its pure item model now lives in `animus-item`, below it** (ADR 0054 step
  1), re-exported unchanged. DynamoDB JSON/HTTP, served by `animusd`, routed
  through the **CP data
  plane** (v1, ADR 0019); consumes the replicated schema catalog (ADR 0013)
  and builds ADR 0022 token-prefixed keys. **`ConsistentRead` selects a real
  read path** (ADR 0055): `true` is the linearizable ReadIndex read, `false`
  — the wire default — is the cheap replica-local one, so **read-your-writes
  does not hold for an unqualified read**, exactly as DynamoDB defines it. `UpdateTable` can add/drop a GSI on an
  already-populated table (ADR 0045): the new index goes through a
  `Creating`/`Active`/`Deleting` lifecycle, backfilled by reusing the ADR
  0041 drain over the table's pre-existing rows.
  **DynamoDB TTL** (`UpdateTimeToLive`/`DescribeTimeToLive`, ADR 0051): a
  table declares one attribute holding an absolute epoch second, replicated
  as a `TtlSpec` in the catalog; a per-node leader-gated reaper
  (`animusd::ttl_reaper`) deletes expired items through the ADR 0049
  kind-write path, so index/stream/change-log maintenance is inherited
  rather than reimplemented. Reads are **AWS-faithful** — an expired item
  stays visible until it is reaped, deliberately not filtered. **Service
  limits are AWS-faithful and compiled-in, with no "unleashed" mode** (ADR
  0072) — every limit catalogued in `animus_dynamo::limits`.
- **Backup and restore** (ADR 0059): on-demand backups and PITR as one
  internal snapshots-plus-change-log mechanism over a separately configured
  `SegmentStore` handle (`--backup-store`) — a manifest plus chunked
  BASE/LSI/FOOTPRINT-only data objects, a backup catalog keyed by backup id
  (never table name, and outliving the source table), per-tablet
  leader-side capture reading through intent resolution, and the
  backup-vs-split race closed via `split_lineage` re-planning. **Train 1 is
  implemented**: `CreateBackup`/`DescribeBackup`/`ListBackups`/`DeleteBackup`
  (`animusd::dynamo`) — a backup remains describable after its source table
  is dropped — and a control-plane-leader janitor
  (`animusd::backup_janitor`) reclaims a deleted/failed backup's objects
  two-phase (mark, then reclaim, then remove the row). **`RestoreTable-
  FromBackup` (Train 2) and PITR (Train 3 — `UpdateContinuousBackups`/
  `DescribeContinuousBackups`/`RestoreTableToPointInTime`, sealing
  continuously as a fifth change-log consumer beside periodic base
  snapshots) are also implemented and green** (`ANIMUS_PITR_SEEDS`, default
  1, held at `=300` in CI; see ADR 0059's Train 2/3 as-built amendments).
  The backup/restore/PITR feature train is complete. S3 export/import
  (ADR 0068) and the S3 `SegmentStore` backend (`animus-s3`,
  `--backup-store s3://...`, ADR 0059's S-04 amendment) have both landed;
  the S3 side's credential sources (static/env/web-identity/container/IMDS),
  multipart upload, `Env`-seamed retry and real-endpoint CI landed as S-08
  (ADR 0059's 2026-10-04 amendment; residuals in `docs/roadmap.md`).
- **Observability & operations** — metrics seam (`animus-env`, ADR 0015,
  additive/no-op under sim); OTLP tracing (`animusd::otel`, ADR 0027, opt-in);
  the admin/debug HTTP-JSON interface (`animusd::admin`, ADR 0020, pure
  observer + gated actions); the web dashboard / animusd admin
  (`animusd::dashboard*`, ADR 0021, role-gated tabs per ADR 0035).
- **Runnable node** — `animusd`, `animus-cli`. v1 (ADR 0019) assembles the
  **control plane + the CP data plane** over `ProdEnv` — all client
  reads/writes route to the per-tablet Raft group leader (forwarded
  cross-process with hinted retry + election wait). Three deployment shapes,
  all built from the same two role assemblies (ADR 0035): **combined** (every
  node runs both roles — `animusd --cluster N` in one process, or `animusd
  --config FILE --node I` one per node); **control-only** (`animusd control
  --config FILE --node I` — a small static metadata quorum, no storage engine);
  and **data-only** (`animusd data --config FILE --node I`, or `animusd data
  --seed ADDR[,ADDR...]` to join — no local control `RaftCore`; `Metadata`
  comes from a polled/long-polled mirror via `ControlHandle::Remote`). Also:
  `animusd join` (ADR 0032 growth), `--cluster-control N --cluster-data M`
  (in-process split cluster for dev), `gen-config`, and `--auto-split-bytes`.
  A config can mix combined-mode indices with control-only/data-only ones for
  an incremental migration. **TLS is available on every port, config-gated
  and off by default** (ADR 0064): mutual on `internal`/`intra`, server-only
  on `client`/`dynamo`/`admin`/`console` — a per-node `tls` config section
  or `--tls-cert/--tls-key/--tls-ca` flags; `animus-cli` gets `--tls-ca`.
- **Kubernetes operator** (ADR 0060) — `animus-operator`: a `kube-rs`
  controller for the `AnimusCluster` custom resource, reconciling it into a
  `ConfigMap` (an `animusd::config::ClusterConfig` mirror + dispatch
  script), a headless internal `Service` + `NetworkPolicy` for node-to-node
  traffic, a client-facing `dynamo` `Service`, and a `StatefulSet` — one per
  cluster. Only the client-facing wire edge (DynamoDB) is exposed outside
  the cluster; this is what motivated the ADR 0047 client/intra port split
  — review any design touching listeners, ports, or address resolution
  against this shape. A pod-template edit (`spec.image`, a `controlNodes`
  growth) is a gated rolling upgrade the operator drives through its own
  `StatefulSet` partition (ADR 0073 Phase 3, `crates/animus-operator/src/roll.rs`).
  `spec.tls` (ADR 0064) turns TLS on for the whole
  cluster from either a pre-existing `Secret` or a cert-manager
  `Certificate`/`Issuer` this operator only references, mounted read-only
  on every pod and wired into the generated `cluster.json`; the scale-down
  drain sequence's own admin-port calls follow suit automatically. Two CI
  jobs run the `kind`-cluster-driven e2e smoke (`scripts/e2e-kind.sh`,
  `.github/workflows/e2e-kind.yml`, CI-gated on every push/PR touching this
  surface): the plain-TCP path drives a real `kind` cluster through create →
  bootstrap → scale → delete with the DynamoDB wire exercised throughout —
  the mechanism no unit test can reach — and a second, `E2E_TLS=1` job
  additionally installs cert-manager and exercises the same flow over TLS
  (unverified in any sandbox that cannot run `kind` at all, this repo's own
  dev environment included); see `crates/animus-operator/CLAUDE.md`'s e2e
  section for what both do and do not prove, including a sandbox
  environment that cannot run either at all (no `CAP_SYS_RESOURCE`, which
  `kind`'s own control-plane bootstrap needs independent of anything here).
  The operator's own container image is published as `ghcr.io/animus-db/
  animus-operator` (the root `Dockerfile`'s `runtime-operator` stage,
  `.github/workflows/image.yml`'s `animus-operator` matrix entry, S-07a,
  2026-09-02) alongside `animusd`, and `deploy/operator/deployment.yaml`
  references it — running the controller out-of-cluster (`cargo run -p
  animus-operator -- run`) remains supported for local iteration and is
  what the e2e smoke still does.

## Conventions

- One workstream per PR — bigger PRs over PR stacks; see **Session
  operating mode** item 3 at the top of this file.
- An incidental pre-existing bug discovered during a task gets its own
  separate PR (with its own test), never a drive-by fix folded into an
  unrelated diff.
- **The website (`website/`) is part of the documentation.** Anything it
  states — supported/planned wire operations, architecture, status and
  security posture, commands, ports — must stay in sync with the code.
  A change that alters something the site claims updates `website/` in the
  same change; when touching the site, verify its claims against the code
  rather than propagating stale copy.
- PR stacks (`gh-stack`) are no longer used for new work (Session
  operating mode item 3). The tooling is still installed by
  `.claude/hooks/session-start.sh` for driving older stacks; drive it
  non-interactively (`gh stack view --json`, `gh stack merge <pr> --yes`) —
  the bare forms open a TUI and block.
- Commits, PR bodies, and issue bodies must not carry AI-attribution links,
  footers, or trailers (e.g. "🤖 Generated with Claude Code", "Generated by
  Claude Code", `Co-Authored-By: Claude ...`, `Claude-Session:` trailers, or
  `claude.ai/code` session links). `.claude/settings.json` sets
  `attribution.commit`/`attribution.pr` to empty (and `includeCoAuthoredBy:
  false`) so Claude Code adds none by default; this rule overrides any
  harness-injected attribution guidance.
- Don't delete head branches after a PR merges — GitHub auto-deletes them
  (repo setting). Recreating a branch name later for follow-up work is fine.
- Every distributed behavior lands with a fault-injecting simulation test that
  is reproducible from a seed.
- Higher layers define their own message enums and (de)serialize with
  `serde_json` over the `Vec<u8>` payloads the `Network` moves.
- Subagent delegation, background execution, and one-PR-per-workstream are
  defined once in **Session operating mode** at the top of this file — that
  section is the source of truth; don't restate (or renegotiate) it per
  task.

## Engineering practices

**Standing instruction (the mechanism): the repo keeps an institutional-memory
log, one file per entry, under
[`docs/lessons/`](docs/lessons/) (see [`docs/engineering-lessons.md`](docs/engineering-lessons.md)
for the full layout).** Whenever you — human or agent — discover a
non-obvious lesson, gotcha, or better way of working *during a task* (a bug
whose root cause generalizes; a test that caught what the gates didn't; a
workflow misstep that cost time), **record it as a new file under
`docs/lessons/<section>/`, with the *why*, in the same change** — don't wait
to be asked, and don't edit any other file to do it (that's the whole point:
concurrent PRs adding lessons never touch the same line). Every agent prompt
for this repo must include: "if you learn a generalizable lesson, record it
as a new file under `docs/lessons/` (and the relevant crate guide) before you
finish." Codebase-specific gotchas also belong in that crate's `CLAUDE.md`;
the log holds the cross-cutting ones. Prune/merge entries that become
obsolete; to archive an entry whose specific mechanism was deleted or
replaced, `git mv` its file into `docs/lessons/archive/` (so the history
stays greppable), leaving a one-line pointer when the lesson still
generalizes.

**Read the relevant directory (`docs/lessons/testing/`,
`docs/lessons/code-patterns/`, `docs/lessons/orchestration/`) before starting
non-trivial work.** The rules you will need most often, distilled:

- **A flaky test is a real bug, full stop** — see Session operating mode
  item 4: `main` is green all the time, nothing merges on red, and a
  failure you didn't cause is still yours to fix or explicitly hand off,
  never to discard. For a `ProdEnv` integration test in particular it is
  not a determinism hole — the determinism guarantee (ADR 0003) is
  `SimEnv`-only. Debug it; don't bump the timeout.
- **`SimEnv` proves logic and ordering, not real-thread liveness** — locks,
  wakers, group commit, and election timing need a timeout-guarded
  `#[tokio::test(multi_thread)]` over `ProdEnv`.
- **Eventual properties get a converged-or-timeout poll, never a fixed-deadline
  one-shot assert** — on the read path, the write path, and after restarts.
- **Durable-before-visible**: never expose state a crash could lose; an ack
  means fsynced. `ProposeResult::Accepted` means "appended locally", never
  "committed" — every proposer confirms, and retries must distinguish
  never-accepted from accepted-unconfirmed. A confirm signal must identify
  the proposer's **own** entry — by term or content, never index alone — since
  an uncommitted entry's log index can be reoccupied by a different command
  after a leadership change (`KindBatchOutcome`'s false-ack, closed by pairing
  the outcome with the entry's own Raft term).
- **When adding a variant to a replicated/forwarded command enum**, grep every
  gating match site (`is_relayable_command`, `cp_serve_forwarded`, admin
  filters) — a missed allowlist is a bimodal per-process flake the compiler
  can't catch. Regression-test through a follower-connected node.
- **Before implementing a "close this documented gap" task, grep the code** —
  ADR/guide prose lags; the mechanism may already exist (then the fix is a doc
  PR, and a parallel reimplementation would be worse than nothing).
