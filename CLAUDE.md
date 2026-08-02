# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

It is deliberately a **thin, method-focused entry point**: how to work here, the
load-bearing constraints, and a map of where things live. It does **not** restate
design *rationale* (that lives in the ADRs, `docs/adr/`) or per-crate *mechanism*
(that lives in each crate's `CLAUDE.md`). Those are the source of truth — keep
*them* current on decisions and details, not this file.

## What this is

AnimusDB is a masterless, linearly-scalable NoSQL database in Rust (Dynamo
lineage). It pairs a **leaderless AP data plane** (tunable quorum consistency)
with a small **strongly-consistent Raft control plane** that owns cluster
metadata. Correctness is established by **deterministic simulation testing**.

Status: pre-alpha. For *what's implemented* and *why*, read the ADR index
([`docs/adr/README.md`](docs/adr/README.md)) and the per-crate guides below —
this file does not keep a feature changelog.

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
| `animus-data` | [crates/animus-data/CLAUDE.md](crates/animus-data/CLAUDE.md) |
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
```

All five (fmt, clippy `-D warnings`, build, test, deny) must be green; CI runs
them. Commits require a DCO sign-off (`git commit -s`); this repo is also set up
for GPG-signed commits.

### Replaying a failed simulation

Every simulation run is a pure function of its seed. Tests print the seed in
assertion messages; replay with `ANIMUS_SEED=<seed> cargo test <name>`. The
`Simulator` is driven by `Simulator::new(seed)`.

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
- **Two planes** — ADR 0001. A consistent Raft **control plane** (`animus-control`,
  owns `Metadata` = membership + tablet map) vs a leaderless-AP **data plane**
  (`animus-data`, serving reads/writes from a *cached* `TabletView` so a
  control-plane outage doesn't stop I/O). See `animus-data/tests/two_plane.rs`.
- **Control-plane Raft** — `animus-control` (ADR 0009; in-house, not openraft, so
  `SimEnv` can drive it). Sync I/O-free `RaftCore` + thin `RaftNode<E>` driver;
  WAL + truncating/chunked-`InstallSnapshot` snapshots; epoch-CAS placement;
  heartbeat **failure detection** (ADR 0012); a replicated **table-schema
  catalog** in `Metadata` (ADR 0013).
- **Data plane** — `animus-data` (ADR 0001). `serve_replica` + the `DataClient`
  quorum coordinator; per-tablet **epoch fencing** (ADR 0002); pick `R+W>N`.
  Convergence via read-repair + segment-digest anti-entropy + hinted handoff,
  all **residency-bounded** (ADR 0010, 0005). An ack means the write durably
  applied.
- **Placement & residency** — `animus-placement` (ADR 0005). Pure policy engine
  (RF + residency labels + failure-domain spread); the leader auto-reconciles
  tablet placement via `reconcile_loop`.
- **Transaction consensus** — `animus-consensus` (ADR 0011). An Accord slice in
  the same shape as the Raft core: sync `AccordCore` + `AccordNode<E>`; durable
  execution, coordinator failover, message retry, read / data-plane-backed /
  sharded / interactive transactions.
- **Storage** — `animus-storage` (ADR 0004, 0008). The **async** `StorageEngine`
  trait; `MemoryEngine` (deterministic, for sim) and a custom on-disk
  `LsmEngine<E>` (WAL/SSTable/leveled compaction, all I/O via the `Env` disk seam
  so its crash recovery is sim-tested).
- **Wire adapters** — `animus-dynamo`, `animus-cql` (ADR 0006). DynamoDB JSON/HTTP
  and CQL v4, served by `animusd`, routed through the same `DataClient`; both
  consume the replicated schema catalog (ADR 0013).
- **Runnable node** — `animusd`, `animus-cli`. Assembles the three planes over
  `ProdEnv`; runs as one process (`animusd --cluster N`) or one per node
  (`animusd --config FILE --node I`).

**Cross-cutting gotcha — a node's inbox is single-consumer.** `Network::recv` for
a node id has exactly one consumer; never run two protocols (e.g. a control
`RaftNode` and a data replica) on the same node id (tests give control nodes
`0..3` and data replicas `3..6` distinct ids).

## Conventions

- One milestone / logical change per PR; keep diffs reviewable.
- Every distributed behavior lands with a fault-injecting simulation test that
  is reproducible from a seed.
- Higher layers define their own message enums and (de)serialize with
  `serde_json` over the `Vec<u8>` payloads the `Network` moves.

## Engineering practices (living — keep this current)

**Standing instruction (the mechanism): this section is append-only institutional
memory.** Whenever you — human or agent — discover a non-obvious lesson, gotcha,
or better way of working *during a task* (a bug whose root cause generalizes; a
test that caught what the gates didn't; a workflow misstep that cost time), **add
a one-line entry here, with the *why*, in the same change** — don't wait to be
asked. Every agent prompt for this repo must include: "if you learn a
generalizable lesson, record it in the root `CLAUDE.md` Engineering-practices
section (and the relevant crate guide) before you finish." Codebase-specific
gotchas also belong in that crate's `CLAUDE.md`; entries here are the
cross-cutting ones. Prune/merge entries that become obsolete.

### Testing
- **Determinism (ADR 0003) proves logic and ordering, not real-thread liveness.**
  `SimEnv` is single-threaded + cooperative, so a `Mutex` guard held across an
  `.await`, a lost waker, or a leader-election/group-commit deadlock can pass
  every sim test and only hang under the real multi-threaded `ProdEnv`. Any
  concurrency primitive (locks, waker handoffs, group commit, leader election)
  needs a **real `#[tokio::test(flavor = "multi_thread")]` over `ProdEnv`,
  timeout-guarded** so a deadlock fails loudly. (Found via the WAL group-commit
  deadlock; pattern in `animus-storage/tests/lsm_concurrent.rs`.)
- **`cargo bench -p animus-storage` (real `ProdEnv`) is a smoke test the
  deterministic suite is not** — it surfaced that same deadlock. Run it when
  touching the write/IO path.
- **A property checker only has teeth under the workload that can exercise it.**
  An Elle serializability check over *disjoint keys / single-writer-per-key* is
  near-trivial (no cross-transaction conflicts → no cycles). Point a
  serializability checker at the layer that *claims* it (Accord), drive
  **conflicting** transactions, and include a **negative control** (a known
  non-serializable history the checker must reject) so a passing run means
  something. The AP/LWW data plane should be checked for what it offers
  (read-your-writes, convergence), not serializability.
- **Prefer a frozen, *generated* scenario corpus over a live-randomized test.**
  Generate scenarios (cluster + workload + an explicit fault schedule) with
  randomness for breadth, but **materialize them into a committed, named set** so
  the suite is reproducible and a failure maps to a specific scenario — not a
  one-off RNG state. Aim for structured/combinatorial coverage of the fault
  matrix (fault type × target class × timing × workload); keep bug-finding
  scenarios in the corpus forever as regressions. (Done: ADR 0014 / `animus-test`
  `corpus.rs` — ~119 frozen, name-seeded scenarios over Accord.)
- **For a *true black-box* Elle check, store the datatype and observe it — don't
  reconstruct it from the ordering layer's log.** Reconstructing each read's list
  from `AccordNode::applied_order` (the old register modelling) limits the
  checker's teeth to cross-replica *divergence*: a single globally-agreed but
  non-serializable order can't show as a cycle, because the lists are derived from
  the very order under test. With **arbitrary write values** (ADR 0011) each key
  now stores a real list and reads observe stored bytes
  (`AccordNode::read_value_result`), so `check_cycles` is genuinely black-box
  (`animus-test/tests/support/mod.rs`). Read "final state" straight from stored
  values on **two distinct replicas** (a real cross-replica agreement check), and
  use **single-writer-per-key** so per-key LWW doesn't lose appends — and build
  each append on the client's own authoritative list, not a begin-time quorum read
  (the apply flips `is_applied` before its fire-and-forget data-plane write lands,
  so a begin-time read can be stale and lose the client's own earlier appends).
- **Never `let _ = storage.merge(...)` on the write path** — an ack must mean the
  write durably applied; surface storage errors so a non-durable write isn't
  counted toward the quorum (`animus-data` `ack_durability.rs`).
- **A timeout-based failure detector can't tell *slow* from *dead* — its bound is
  load-bearing, and the frozen corpus is what catches an over-aggressive one.** A
  replica watching a transaction it doesn't coordinate only sees *phase* changes,
  not the coordinator slowly gathering a quorum, so "recover after N quiet ticks"
  will recover a live-but-slow (or transiently-partitioned-then-healing)
  coordinator if N is too small. Worse, recovering a transaction that *would* have
  committed re-orders it after every conflict committed meanwhile (Accord recovery
  bumps the timestamp), and where execution is **LWW-by-execution-timestamp** that
  silently loses later same-key writes. The bound must exceed a realistic
  slow-commit / partition-and-heal window; safety also wants the recovered commit
  ballot-fenced so a healed coordinator's late commit can't revert it. An
  over-aggressive 600ms bound passed every targeted consensus test but failed the
  `animus-test` Elle corpus (`wide_write`/`isolate_one`) — **run the frozen corpus
  (`cargo test -p animus-test`) after any change to recovery/execution timing**, it
  exercises interactions a single-feature test never will. (ADR 0011 failure-detector
  slice.)

### Code patterns
- **No process-global mutable state (`OnceLock`/`static`) for per-instance
  concerns.** It leaks across tests in one binary (multiple in-process clusters
  share it) and conflates instances in any multi-tenant context. Thread state
  through a per-instance context instead (the wire edges' `ClusterEdgeState` via
  `ClientCtx`, not process statics). If you must keep a static, make sure tests
  tear instances down (`Node::shutdown()`) and use unique names/keys per test.
- **Never hold a `std::sync::Mutex` guard across an `.await`** in `<E: Env>`
  code — it breaks `Send` (often a *compile* error via `spawn_task`'s bound) and
  risks nondeterminism. Take the lock, mutate, drop it; do I/O lock-free.
- **Count a metric at the site that knows the *real* outcome, not the attempt.**
  A counter recorded where an op is *requested* over-counts when a downstream
  helper silently no-ops (e.g. `HintStore::record` drops a hint on a residency or
  LWW-supersede miss). Have the helper *return* whether it acted and count on that
  (`data_hints_stored` counts only hints actually stored). This keeps the closed
  `Metric` enum (ADR 0015) append-only/byte-reproducible and the seam observe-only
  — instrumenting must never change the path it measures.
- **A per-instance observability seam (e.g. ADR 0015 metrics) has *one sink per
  `Env`/role*, so the integration layer must aggregate, not pick one.** A node
  runs several `ProdEnv` roles on distinct ids (control/data/coord), each with its
  **own** `metrics()` sink — `RaftNode::start` records into the *control* env's,
  the replica/coordinator into theirs. A `/metrics` handler that read only one
  (e.g. `node.raft.metrics()`) would silently drop the others' counters. Capture
  every role's handle and sum the snapshots **at request time** (live, not cached);
  capture the soon-to-be-moved handles before the envs are consumed. (`animusd`
  `ClientCtx::metrics_text`.)
- **Don't react to "I was superseded" by *immediately* re-proposing higher** —
  that is the classic duelling-proposers **livelock** (two recoverers ratchet each
  other's ballot forever within one logical instant, an unbounded message storm).
  Break ties **deterministically** (e.g. only the higher-id contender retries; the
  other stands down and adopts the winner's result) or back the retry off in time.
  This also hangs a `SimEnv` test rather than failing it: the single-threaded
  cooperative executor just spins at one virtual instant (100%+ CPU, no progress,
  no panic), so **run new sim tests under a `timeout`** the first time — a hang
  there is a same-instant unbounded-work loop, not slowness. (Found wiring Accord
  recovery ballots; `animus-consensus` `core.rs::handle_superseded`.)
- **Prefer a live read of the durable layer over observation-built in-memory
  state.** The DynamoDB edge once tracked written item keys in-memory to fake a
  range scan; that set is lost on restart and stale on a follower that never saw a
  write. Replacing it with the data plane's native quorum range scan
  (`DataClient::scan`, reading live storage in key order) made `Query`/`Scan`
  correct after a restart — and the *regression that proves it* is a scan **after a
  node restart wipes the registry** (`animusd/tests/dynamo_schema.rs`), not just a
  same-process scan. When you delete a derived cache, test the path that the cache
  used to mask.
- **A cross-cutting seam (metrics, tracing) must be *additive* and observable
  without touching `SimEnv`.** Add it to `Env` as a method with a **no-op default**
  (a real shared no-op handle, not an `Option`, so record sites need no guard) —
  the supertrait and every `E: Env` impl stay untouched. Keep it deterministic:
  no wall clock (timestamps come from `Clock::now`), no I/O, no `HashMap` (snapshot
  into a `BTreeMap`). To let a *sim test* read what a component records, thread a
  recording handle into the component (e.g. `start_with_metrics`) rather than
  overriding `SimEnv` — so `animus-sim` needs no change. (ADR 0015 / `animus-env`
  `metrics.rs`.)

### Merge / integration workflow
- **Run `cargo test --workspace` after *each* merge, not just at the end of a
  batch.** Batching the gate run let a regression onto master via an earlier
  merge before it was caught. All five gates (fmt, clippy `--all-features
  -D warnings`, build, test, `cargo deny`) green per merge.
- **`cargo deny` can be silently broken** (e.g. the repo's own `AGPL-3.0-only`
  missing from the allow-list) and it can't run in every local env — CI runs it;
  treat it as a real gate, not optional.
- **Don't `git add -A` while resolving a merge** — it can sweep agent worktree
  dirs in as embedded git repos. Stage explicit paths; `.claude/worktrees/` is
  gitignored to prevent it.
- **Doc files (`CLAUDE.md`, ADRs) conflict predictably** when parallel changes
  each edit the "what remains" lists — resolve by *unioning the done-states*
  (each side is usually stale only for the *other* change's feature).

### Parallel-agent orchestration
- **Partition work by disjoint crate ownership — exactly one owner per shared
  crate/file.** The assembly points (`animusd`, `animus-control`) are
  chokepoints; if several agents must touch `animusd`, split by *file*
  (`dynamo.rs` / `cql.rs` / `lib.rs`) and expect a small `lib.rs` merge.
- **Verify agent output yourself** (build + gates), don't trust the report —
  especially for safety-critical changes (a `SimEnv`/determinism edit) and after
  any agent died mid-run.
- **When an agent dies (API overload/stall/error), inspect its worktree before
  re-launching** — its partial work is often intact and finishable (or resumable
  via `SendMessage`); a lost worktree means redo. **Don't thrash re-launches
  during an API overload** — wait for it to ease.
- **Tell agents to keep public signatures stable** when a sibling depends on them
  (additive changes only), and to **stop-and-report rather than loop** on a
  transient API error.
