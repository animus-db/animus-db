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

3. **A stacked PR series is the default shape for delivered work.** Any
   change with more than one reviewable logical step (groundwork + mechanism,
   refactor + feature, schema + consumer) ships as a `gh-stack` series —
   tooling in Conventions below. A single flat PR is the *exception*, and its
   description says why it wasn't stacked.

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

**No back-compat until further notice.** There are no migration paths and no
wire/WAL/on-disk-format compatibility guarantees between revisions — assume
clusters are recreated from scratch. Don't spend design or review budget on
upgrade paths or compat shims; where a cheap compat measure exists anyway (a
serde default, a codec version bump) it is an implementation convenience, not
a promise.

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
| `animus-test` | [crates/animus-test/CLAUDE.md](crates/animus-test/CLAUDE.md) |
| `animus-dynamo` | [crates/animus-dynamo/CLAUDE.md](crates/animus-dynamo/CLAUDE.md) |
| `animus-placement` | [crates/animus-placement/CLAUDE.md](crates/animus-placement/CLAUDE.md) |
| `animus-node` | [crates/animus-node/CLAUDE.md](crates/animus-node/CLAUDE.md) |
| `animusd` | [crates/animusd/CLAUDE.md](crates/animusd/CLAUDE.md) |
| `animus-cli` | [crates/animus-cli/CLAUDE.md](crates/animus-cli/CLAUDE.md) |
| `animus-operator` | [crates/animus-operator/CLAUDE.md](crates/animus-operator/CLAUDE.md) |

## Commands

```sh
cargo build --workspace --all-targets
cargo test --workspace
cargo test -p animus-control                       # one crate
cargo test -p animus-control --test control_raft   # one test binary
cargo test -p animus-control survives_leader_kill  # one test by name substring
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all --check
cargo deny check                                   # licenses + advisories (cargo install cargo-deny)
cargo bench -p animus-storage                      # ProdEnv smoke of the write/IO path
cargo bench -p animusd                             # cluster wire benchmark: latency percentiles + degraded phase
```

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
| `ANIMUS_RECONCILER_SEEDS=K` | 1 | reconciler-corpus depth (`animus-cp-data`) |
| `ANIMUS_TXN_SEEDS=K` | 1 | multi-tablet cross-transaction corpus depth (`animus-test`, ADR 0018) |
| `ANIMUS_STREAM_SEEDS=K` | 1 | DynamoDB Streams lineage-walk corpus depth (`animus-test`, ADR 0042/0043) |
| `ANIMUS_BACKFILL_SEEDS=K` | 1 | secondary-index backfill fault-injection corpus depth (`animus-test`, ADR 0045) |
| `ANIMUS_QUIESCE_SEEDS=K` | 1 | idle-tablet-group quiescence corpus depth (`animus-cp-data`, ADR 0044 phase 1) |
| `ANIMUS_SPLIT_SEEDS=K` | 1 | `KvCommand::SeedBatch` corpus depth (`animus-cp-data`) — the version-carrying row-merge command originally built for the now-deleted copy-based split driver (ADR 0050 Train B), its sole surviving consumer is the restore driver (ADR 0059 §7) |
| `ANIMUS_LEARNER_SEEDS=K` | 1 | learner (non-voting) membership-class fault-injection corpus depth (`animus-control`, ADR 0058 Train 1) |
| `ANIMUS_CONTROL_SEEDS=K` | 1 | control-plane machinery (apply task, schema-catalog exclusivity) fault-injection corpus depth (`animus-control`) |
| `ANIMUS_INPLACE_SPLIT_SEEDS=K` | 1 | in-place split group-mint-at-apply fault-injection corpus depth (`animus-cp-data`, ADR 0058 Train 2 rung 3) |
| `ANIMUS_BACKUP_SEEDS=K` | 1 | on-demand backup capture fault-injection corpus depth (`animus-test`, ADR 0059 Train 1) |
| `ANIMUS_PITR_SEEDS=K` | 1 | PITR sealing fault-injection corpus depth (`animus-test`, ADR 0059 Train 3) |
| `ANIMUS_LSM_CRASH_SEEDS=K` | 1 | `LsmEngine` crash-safety corpus depth (`animus-storage`, `tests/lsm_crash.rs`) |
| `ANIMUS_LSM_DISK_FAULT_SEEDS=K` | 1 | `LsmEngine` `DiskConfig` fault-injection corpus depth (`animus-storage`, `tests/lsm_disk_faults.rs`) |
| `ANIMUS_SHRINK=1` | off | when a corpus scenario fails, delta-debug it to a minimal reproducing case and print a replayable handle (`animus-test::shrink`, ADR 0061 rung B4) |
| `ANIMUS_SHRINK_MAX_CHECKS=N` | 500 | iteration budget for `ANIMUS_SHRINK`'s search (a plain check count, not wall-clock time — see `animus-test/CLAUDE.md`) |
| `ANIMUS_SHRINK_REPLAY=<json>` | unset | replay a minimized scenario a shrink run printed (per-corpus entry point, e.g. `raftkv_shrink_replay` in `raftkv_linearizable.rs`) |
| `ANIMUS_BENCH_{KEYS,GETS,SCAN,VALUE_BYTES,APPLY_BATCH}` | — | `animus-storage`'s `engine_bench` workload tuning |
| `ANIMUS_BENCH_{NODES,ITEMS,OPS,VALUE_BYTES,CLIENTS,JSON}` | — | `animusd`'s `cluster_bench` workload tuning (node count, preload size, measured ops/class, item size, concurrent-client sweep, JSON output path) |

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
  **bytes** (ADR 0034, `animusd`); **tablets are split-only** — merge has
  been removed entirely (ADR 0044, supersedes ADR 0033); dropped tables'
  data is reclaimed by a convergent **GC** (ADR 0024). Tablet ids are never
  reused. An idle CP-data group **quiesces** (ADR 0048, phase 1 of ADR
  0044's cheap-groups roadmap): no local activity for `--quiesce-after`
  (default on, 5s) stops its Raft timers/heartbeats/apply-poll entirely
  until a write, a peer message, or the reconciler's proactive wake (a
  replica marked `Down`) touches it again — data-plane only (the control
  group never quiesces), remains leader while quiesced, and admin/
  dashboard reads never wake a group (`quiesced` is a pure diagnostic).
- **Placement, rebalancing & growth** — `animus-placement` (ADR 0005): pure
  policy engine (RF + residency labels + failure-domain spread), `replan`
  (failure repair) + `rebalance_step` (ADR 0029: one balance-driven move per
  call; converges to max−min ≤ 1 when the policy sets no `SpreadPolicy` — with
  a spread constraint the domain guard can legally block every improving move,
  so only monotonic non-worsening and termination hold, see the property tests
  in `animus-placement/tests/placement_props.rs`). The control-plane leader reconciles
  placement event-driven (ADR 0031). Clusters grow online: new nodes
  self-register and mirror `Metadata` (ADR 0030), join via seed addresses, and
  are decommissioned via drain → remove (ADR 0032).
- **Transaction consensus** — 2PC/HLC over the per-tablet Raft groups (ADR
  0018), the only transaction story. The Accord slice that used to sit here
  (`animus-consensus`, ADR 0011) is **deleted** — rejected for CP by ADR 0018 in
  favour of 2PC-over-Raft, and deferred with AP by ADR 0019, whose 2026-08-23
  amendment removed it outright along with its Elle corpus (ADR 0014).
- **Storage** — `animus-storage` (ADR 0004, 0008). The **async** `StorageEngine`
  trait; `MemoryEngine` (deterministic, for sim) and a custom on-disk
  `LsmEngine<E>` (WAL/SSTable/leveled compaction, all I/O via the `Env` disk seam
  so its crash recovery is sim-tested).
- **Wire adapter** — `animus-dynamo` (ADR 0006; a CQL adapter, `animus-cql`,
  also shipped for a time but was dropped, ADR 0053 — v1 is DynamoDB-only).
  DynamoDB JSON/HTTP, served by `animusd`, routed through the **CP data
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
  stays visible until it is reaped, deliberately not filtered.
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
  The backup/restore/PITR feature train is complete. S3 export/import and
  an S3 `SegmentStore` backend are deferred follow-ups.
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
  an incremental migration.
- **Kubernetes operator** (ADR 0060) — `animus-operator`: a `kube-rs`
  controller for the `AnimusCluster` custom resource, reconciling it into a
  `ConfigMap` (an `animusd::config::ClusterConfig` mirror + dispatch
  script), a headless internal `Service` + `NetworkPolicy` for node-to-node
  traffic, a client-facing `dynamo` `Service`, and a `StatefulSet` — one per
  cluster. Only the client-facing wire edge (DynamoDB) is exposed outside
  the cluster; this is what motivated the ADR 0047 client/intra port split
  — review any design touching listeners, ports, or address resolution
  against this shape. An e2e smoke (`scripts/e2e-kind.sh`,
  `.github/workflows/e2e-kind.yml`, CI-gated on every push/PR touching this
  surface) drives a real `kind` cluster through create → bootstrap → scale
  → delete with the DynamoDB wire exercised throughout — the mechanism no
  unit test can reach; see `crates/animus-operator/CLAUDE.md`'s e2e section
  for what it does and does not prove, including a sandbox environment that
  cannot run it at all (no `CAP_SYS_RESOURCE`, which `kind`'s own
  control-plane bootstrap needs independent of anything here). The
  operator's own container image is not yet published — it runs
  out-of-cluster (or from a locally built image) until that lands.

## Conventions

- One milestone / logical change per PR; keep diffs reviewable. Work with
  more than one reviewable logical step stacks by default — see **Session
  operating mode** at the top of this file.
- An incidental pre-existing bug discovered during a task gets its own
  separate PR (with its own test), never a drive-by fix folded into an
  unrelated diff.
- **The website (`website/`) is part of the documentation.** Anything it
  states — supported/planned wire operations, architecture, status and
  security posture, commands, ports — must stay in sync with the code.
  A change that alters something the site claims updates `website/` in the
  same change; when touching the site, verify its claims against the code
  rather than propagating stale copy.
- Larger work ships as a stacked PR series (managed with
  [`gh-stack`](https://github.com/github/gh-stack), a `gh` CLI extension),
  reviewed per-PR and merged as one stack. **Web sessions get `gh`, the
  extension and its agent skill installed automatically** by
  `.claude/hooks/session-start.sh`; locally, install them once with:
  ```sh
  gh extension install github/gh-stack   # the extension
  gh skill install github/gh-stack       # the agent skill (gh >= 2.8x)
  git config rerere.enabled true         # remember conflict resolutions
  ```
  Drive it non-interactively — `gh stack view --json`, `gh stack submit
  --auto`, `gh stack merge <pr> --yes`; the bare forms open a TUI and block.
  `gh stack merge` is an **atomic** all-or-nothing merge of the whole stack,
  which is what makes it safer than merging a hand-rolled stack bottom-up
  (see `docs/engineering-lessons.md` on issue #279, where doing that by hand
  landed the groundwork on `main` without the fix it was groundwork for while
  both PRs reported "Merged").
- PR and issue bodies must not carry AI-attribution links or footers (e.g.
  "🤖 Generated with Claude Code", "Generated by Claude Code", or
  `claude.ai/code` session links).
- Don't delete head branches after a PR merges — GitHub auto-deletes them
  (repo setting). Recreating a branch name later for follow-up work is fine.
- Every distributed behavior lands with a fault-injecting simulation test that
  is reproducible from a seed.
- Higher layers define their own message enums and (de)serialize with
  `serde_json` over the `Vec<u8>` payloads the `Network` moves.
- Subagent delegation, background execution, and stack-by-default are
  defined once in **Session operating mode** at the top of this file — that
  section is the source of truth; don't restate (or renegotiate) it per
  task.

## Engineering practices

**Standing instruction (the mechanism): the repo keeps an append-only
institutional-memory log at
[`docs/engineering-lessons.md`](docs/engineering-lessons.md).** Whenever you —
human or agent — discover a non-obvious lesson, gotcha, or better way of
working *during a task* (a bug whose root cause generalizes; a test that caught
what the gates didn't; a workflow misstep that cost time), **add an entry
there, with the *why*, in the same change** — don't wait to be asked. Every
agent prompt for this repo must include: "if you learn a generalizable lesson,
record it in `docs/engineering-lessons.md` (and the relevant crate guide)
before you finish." Codebase-specific gotchas also belong in that crate's
`CLAUDE.md`; the log holds the cross-cutting ones. Prune/merge entries that
become obsolete; entries whose specific mechanism was deleted or replaced move
verbatim to
[`docs/engineering-lessons-archive.md`](docs/engineering-lessons-archive.md)
(so the history stays greppable), leaving a one-line pointer when the lesson
still generalizes.

**Read the log's relevant section (Testing / Code patterns / Parallel-agent
orchestration) before starting non-trivial work.** The rules you will need
most often, distilled:

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
