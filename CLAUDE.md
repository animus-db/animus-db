# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

It is deliberately a **thin, method-focused entry point**: how to work here, the
load-bearing constraints, and a map of where things live. It does **not** restate
design *rationale* (that lives in the ADRs, `docs/adr/`), per-crate *mechanism*
(that lives in each crate's `CLAUDE.md`), or the accumulated *lessons log*
(that lives in [`docs/engineering-lessons.md`](docs/engineering-lessons.md)).
Those are the source of truth — keep *them* current on decisions and details,
not this file.

## What this is

AnimusDB is a masterless, linearly-scalable NoSQL database in Rust. **For v1
(ADR 0019) it is strongly-consistent (CP):** a **leaderful per-tablet Raft data
plane** (linearizable single-tablet reads/writes, ADR 0016/0017) under a small
**Raft control plane** that owns cluster metadata — Cockroach/TiKV-shaped.
Correctness is established by **deterministic simulation testing**. The original
Dynamo-lineage **leaderless AP data plane** (ADR 0001) is **deferred** — a
long-shot future improvement (ADR 0019); its crate (`animus-data`) and the Accord
data-plane "frontier" that depended on it were **deleted**, retrievable from git
history if both-planes is revived.

Status: pre-alpha. For *what's implemented* and *why*, read the ADR index
([`docs/adr/README.md`](docs/adr/README.md)) and the per-crate guides below —
this file does not keep a feature changelog.

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
| `animus-consensus` | [crates/animus-consensus/CLAUDE.md](crates/animus-consensus/CLAUDE.md) |
| `animus-cql` | [crates/animus-cql/CLAUDE.md](crates/animus-cql/CLAUDE.md) |
| `animusd` | [crates/animusd/CLAUDE.md](crates/animusd/CLAUDE.md) |
| `animus-cli` | [crates/animus-cli/CLAUDE.md](crates/animus-cli/CLAUDE.md) |

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
```

All five gates (fmt, clippy `-D warnings`, build, test, deny) must be green; CI
runs them. Commits require a DCO sign-off (`git commit -s`); this repo is also
set up for GPG-signed commits.

### Replaying a failed simulation

Every simulation run is a pure function of its seed. Tests print the seed in
assertion messages; replay with `ANIMUS_SEED=<seed> cargo test <name>`. The
`Simulator` is driven by `Simulator::new(seed)`.

### Test-scaling and bench knobs

| Env var | Default | Effect |
|---------|---------|--------|
| `ANIMUS_SEED` | unset | replay one sim run from its printed seed |
| `ANIMUS_CORPUS_SEEDS=K` | 1 | Accord Elle-corpus depth: K seed variants per cell (`animus-test`) |
| `ANIMUS_CORPUS_FULL=1` | off | Accord Elle-corpus breadth: extended dimensions (`animus-test`) |
| `ANIMUS_RAFTKV_SEEDS=K` | 1 | raftkv-corpus depth (`animus-test`) |
| `ANIMUS_RAFTKV_LSM=1` | off | run the whole raftkv corpus over `LsmEngine<SimEnv>` |
| `ANIMUS_RECONCILER_SEEDS=K` | 1 | reconciler-corpus depth (`animus-cp-data`) |
| `ANIMUS_TXN_SEEDS=K` | 1 | multi-tablet cross-transaction corpus depth (`animus-test`, ADR 0018) |
| `ANIMUS_STREAM_SEEDS=K` | 1 | DynamoDB Streams lineage-walk corpus depth (`animus-test`, ADR 0042/0043) |
| `ANIMUS_BACKFILL_SEEDS=K` | 1 | secondary-index backfill fault-injection corpus depth (`animus-test`, ADR 0045) |
| `ANIMUS_QUIESCE_SEEDS=K` | 1 | idle-tablet-group quiescence corpus depth (`animus-cp-data`, ADR 0044 phase 1) |
| `ANIMUS_SPLIT_SEEDS=K` | 1 | split-build `SeedBatch` corpus depth (`animus-cp-data`, ADR 0050 Train B) |
| `ANIMUS_BENCH_{KEYS,GETS,SCAN,VALUE_BYTES,APPLY_BATCH}` | — | `engine_bench` workload tuning |

The deep corpus tier (`ANIMUS_CORPUS_SEEDS=40 ANIMUS_CORPUS_FULL=1`) runs
nightly in CI (`.github/workflows/corpus-deep.yml`), not per-push.

### Dashboard

The "AnimusDB Console" (ADR 0021) is served by `animusd` on each node's
**admin** address. Its assets (`crates/animusd/src/dashboard.{html,css}` +
`dashboard_*.js`) are self-contained vanilla JS embedded via `include_str!` —
no bundler, no build step: edit, `cargo build`, reload.

## The load-bearing constraint: determinism

This is the single most important rule (ADR 0003). **All nondeterminism flows
through the `Env` seam.** In every crate except `animus-env`'s `ProdEnv` and
test code:

- No wall clock — use `env.now()` / `env.sleep()`, never `std::time` or
  `tokio::time`.
- No raw task spawning — use `env.spawn_task(..)`, never `tokio::spawn`.
- No real I/O — use `env.send`/`recv` and `env.append`/`sync`/`read`, never
  `std::net`/`std::fs`/`tokio::{net,fs}`.
- No unseeded randomness — use `env.next_u64()` / `env.gen_below(..)`, never
  `thread_rng`/`OsRng`.
- **No `HashMap`/`HashSet` in logic** — their iteration order is
  nondeterministic. Use `BTreeMap`/`BTreeSet`. This is lint-enforced via
  `clippy.toml`.

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
  streaming `InstallSnapshot`, single-server membership change. Each hosted
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
- **Tablet lifecycle** — split is a **copy-based background workflow**
  (ADR 0050): `BeginSplit` mints two `Building` children at
  placement-chosen homes, a driver on the parent's leader copies + tails,
  a terminal `Freeze` stops writes, `CutoverSplit` activates the children
  and retires the parent (children born with empty change logs; lineage
  frozen in `split_lineage`); auto-split triggers on
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
  call, converges to max−min ≤ 1). The control-plane leader reconciles
  placement event-driven (ADR 0031). Clusters grow online: new nodes
  self-register and mirror `Metadata` (ADR 0030), join via seed addresses, and
  are decommissioned via drain → remove (ADR 0032).
- **Transaction consensus** — `animus-consensus` (ADR 0011). An Accord slice in
  the same shape as the Raft core (sync `AccordCore` + `AccordNode<E>`).
  **Testbed-only** (ADR 0018/0019): no production consumer — it exists as the
  known-serializable system the Elle checkers are proven against; ADR 0018
  chose 2PC-over-Raft (unbuilt) for future CP transactions.
- **Storage** — `animus-storage` (ADR 0004, 0008). The **async** `StorageEngine`
  trait; `MemoryEngine` (deterministic, for sim) and a custom on-disk
  `LsmEngine<E>` (WAL/SSTable/leveled compaction, all I/O via the `Env` disk seam
  so its crash recovery is sim-tested).
- **Wire adapters** — `animus-dynamo`, `animus-cql` (ADR 0006). DynamoDB JSON/HTTP
  and CQL v4, served by `animusd`, routed through the **CP data plane** (v1,
  ADR 0019); both consume the replicated schema catalog (ADR 0013) and build
  ADR 0022 token-prefixed keys. `UpdateTable` can add/drop a GSI on an
  already-populated table (ADR 0045): the new index goes through a
  `Creating`/`Active`/`Deleting` lifecycle, backfilled by reusing the ADR
  0041 drain over the table's pre-existing rows.
- **Observability & operations** — metrics seam (`animus-env`, ADR 0015,
  additive/no-op under sim); OTLP tracing (`animusd::otel`, ADR 0027, opt-in);
  the admin/debug HTTP-JSON interface (`animusd::admin`, ADR 0020, pure
  observer + gated actions); the web dashboard / AnimusDB Console
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
  (in-process split cluster for dev), `gen-config`, and `--auto-split[-bytes]`.
  A config can mix combined-mode indices with control-only/data-only ones for
  an incremental migration.
- **Deployment target (Kubernetes operator)** — the intended production shape:
  a K8s operator runs the cluster with seed/intra node-to-node traffic kept
  cluster-internal (never on an externally-reachable Service); only the
  client-facing wire edges (DynamoDB/CQL) are exposed outside the cluster.
  This is what motivated the ADR 0047 client/intra port split — review any
  design touching listeners, ports, or address resolution against this shape.

## Conventions

- One milestone / logical change per PR; keep diffs reviewable.
- An incidental pre-existing bug discovered during a task gets its own
  separate PR (with its own test), never a drive-by fix folded into an
  unrelated diff.
- Larger work ships as a stacked PR series (managed with `gh-stack`),
  reviewed per-PR and merged bottom-up as one stack.
- PR and issue bodies must not carry AI-attribution links or footers (e.g.
  "🤖 Generated with Claude Code", "Generated by Claude Code", or
  `claude.ai/code` session links).
- Every distributed behavior lands with a fault-injecting simulation test that
  is reproducible from a seed.
- Higher layers define their own message enums and (de)serialize with
  `serde_json` over the `Vec<u8>` payloads the `Network` moves.

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

- **A flaky `ProdEnv` integration test is a real bug**, not a determinism hole
  — the determinism guarantee (ADR 0003) is `SimEnv`-only. Debug it; don't bump
  the timeout.
- **`SimEnv` proves logic and ordering, not real-thread liveness** — locks,
  wakers, group commit, and election timing need a timeout-guarded
  `#[tokio::test(multi_thread)]` over `ProdEnv`.
- **Eventual properties get a converged-or-timeout poll, never a fixed-deadline
  one-shot assert** — on the read path, the write path, and after restarts.
- **Durable-before-visible**: never expose state a crash could lose; an ack
  means fsynced. `ProposeResult::Accepted` means "appended locally", never
  "committed" — every proposer confirms, and retries must distinguish
  never-accepted from accepted-unconfirmed.
- **When adding a variant to a replicated/forwarded command enum**, grep every
  gating match site (`is_relayable_command`, `cp_serve_forwarded`, admin
  filters) — a missed allowlist is a bimodal per-process flake the compiler
  can't catch. Regression-test through a follower-connected node.
- **Before implementing a "close this documented gap" task, grep the code** —
  ADR/guide prose lags; the mechanism may already exist (then the fix is a doc
  PR, and a parallel reimplementation would be worse than nothing).
