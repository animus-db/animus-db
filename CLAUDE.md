# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

It is deliberately a **thin, method-focused entry point**: how to work here, the
load-bearing constraints, and a map of where things live. It does **not** restate
design *rationale* (that lives in the ADRs, `docs/adr/`) or per-crate *mechanism*
(that lives in each crate's `CLAUDE.md`). Those are the source of truth — keep
*them* current on decisions and details, not this file.

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
- **Two planes** — ADR 0001 / ADR 0019. A consistent Raft **control plane**
  (`animus-control`, owns `Metadata` = membership + tablet map) vs a **CP data
  plane** (`animus-cp-data`, leaderful per-tablet Raft, linearizable). v1 is
  CP-only; the original leaderless-AP data plane (`animus-data`) is **deleted**
  (retrievable from git history, ADR 0019).
- **Control-plane Raft** — `animus-control` (ADR 0009; in-house, not openraft, so
  `SimEnv` can drive it). Sync I/O-free `RaftCore` + thin `RaftNode<E>` driver;
  WAL + truncating/chunked-`InstallSnapshot` snapshots; epoch-CAS placement;
  heartbeat **failure detection** (ADR 0012); a replicated **table-schema
  catalog** in `Metadata` (ADR 0013).
- **CP data plane** — `animus-cp-data` (ADR 0016, 0017). Each tablet is its own
  Raft group with a single leader serving **linearizable** single-tablet
  reads/writes, durable on a real `StorageEngine`; reuses the control plane's sync
  `RaftCore` with a `DRIVER_APPLIED` KV state machine; ReadIndex reads, compaction +
  streaming `InstallSnapshot`, single-server membership change, tablet split.
- **Placement & residency** — `animus-placement` (ADR 0005). Pure policy engine
  (RF + residency labels + failure-domain spread); the leader auto-reconciles
  tablet placement via `reconcile_loop`.
- **Transaction consensus** — `animus-consensus` (ADR 0011). An Accord slice in
  the same shape as the Raft core: sync `AccordCore` + `AccordNode<E>`; durable
  local execution, coordinator failover, message retry, read / interactive
  transactions, and per-shard consensus (one group per tablet).
- **Storage** — `animus-storage` (ADR 0004, 0008). The **async** `StorageEngine`
  trait; `MemoryEngine` (deterministic, for sim) and a custom on-disk
  `LsmEngine<E>` (WAL/SSTable/leveled compaction, all I/O via the `Env` disk seam
  so its crash recovery is sim-tested).
- **Wire adapters** — `animus-dynamo`, `animus-cql` (ADR 0006). DynamoDB JSON/HTTP
  and CQL v4, served by `animusd`, routed through the **CP data plane** (v1,
  ADR 0019); both consume the replicated schema catalog (ADR 0013).
- **Runnable node** — `animusd`, `animus-cli`. v1 (ADR 0019) assembles the
  **control plane + the CP data plane** (`animus-cp-data`) over `ProdEnv` — all
  client reads/writes route to the per-tablet Raft group leader (forwarded
  cross-process); the leaderless AP plane is dropped. Runs as one process
  (`animusd --cluster N`) or one per node (`animusd --config FILE --node I`).

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

**Entries whose specific mechanism has since been deleted or replaced live in
[`docs/engineering-lessons-archive.md`](docs/engineering-lessons-archive.md),
not here** — moved verbatim, not deleted, so the history stays greppable.
Where the underlying lesson is still generally applicable, a one-line pointer
to the archive stays in place below.

### Testing
- **A cluster-bring-up test helper that gates on `any(is_control_leader)` is
  wrong for a test that restarts a single node of a multi-node cluster** — the
  restarted node rejoins as a follower (the majority never went down), so it
  never reports itself leader and the helper times out waiting for a
  leadership signal that was never the actual readiness condition. A
  single-node cluster's own restart test hides this (a 1-of-1 group is always
  its own leader). For a restart-one-node-of-N test, wait for the node to
  *catch up* instead — poll its admin/Raft view until `last_applied ==
  commit_index && commit_index >= snapshot_index + log_len` (no leadership
  requirement) — which is also the correct replay-completion gate before any
  convergent post-restart assertion. (`animusd` ADR 0029 release-GC restart
  test.)
- **Adding a new heavy multi-node `ProdEnv` integration test raises CPU/IO
  contention on every *other* test binary running in parallel under `cargo
  test`, and a pre-existing hard latency-bound assertion (e.g. a median write
  latency under some millisecond ceiling) can flake purely from that added
  load — no code regression in either test.** Confirm such a failure by
  re-running the victim *in isolation* before treating it as real; a
  release/GC-style loop that is a genuine no-op on a steady cluster (its
  predicate returns empty, then iterates nothing) cannot be the cause. Same
  family as the documented "a flaky ProdEnv test is a real bug" rule, with the
  refinement that a *newly-added* heavy test can itself be the load source —
  so the right move is isolate-and-reconfirm, not loosen the victim's bound.
- **The in-process `--cluster N` shared edge state masks cross-process leader-routing
  gaps — test cross-process paths *per-process*.** In `--cluster N` every node shares
  one `ClusterEdgeState`, so an operation that needs to reach *both* the control
  leader **and** a per-tablet CP-group leader (e.g. the tablet-split trigger:
  `SplitTablet` metadata on the control leader + `propose_split` on the CP leader)
  works from any node, because the shared edge reaches both in-process. **Per-process**
  (one `ClusterEdgeState` each) those two leaderships can sit on *different* nodes, so
  the same call silently fails on every node unless the trigger is forwarded
  cross-process. The split-over-`ProdEnv` and re-host tests therefore drive the split
  *in-process* (`cp_rehost.rs`) and the reconfigure/failure tests run *per-process*
  (`cp_reconfigure.rs`) to exercise the node-local admin views + real failure
  detection. When a path resolves a leader, ask "which leader, and is it the same node
  as the other leader this path needs?" — and add a per-process test if not.
  **Update (ADR 0031 PR2, 2026-08-07): the shared `ClusterEdgeState` root cause
  this entry describes is gone** — `--cluster N`'s in-process bring-up
  (`start_cluster_with`) now creates a distinct edge-state set **per node**,
  exactly like one-process-per-node, and populates `client_route` the same way
  `run_node_with` does, so an in-process node genuinely forwards/relays to
  reach a leader hosted elsewhere rather than finding it locally via a shared
  registry. `--cluster N` and one-process-per-node are now the same code path
  in every way that matters to this class of bug. (`cp_rehost.rs`, referenced
  above as the in-process split test, no longer exists — split is now a
  single control-plane command with no data-plane half to rehost, ADR 0028 —
  but the general lesson stands as a *pattern to watch for*: any future
  process-scoped convenience shortcut (a shared registry, a shared cache, a
  shared claim set) that an in-process multi-node test harness introduces for
  convenience can silently mask the same class of cross-process gap, so audit
  new shared state the same way.) The general "which leader, is it the same
  node" question, and "test cross-process paths per-process," remain sound
  advice for any *new* multi-leader coordination this repo adds.
- **Match a consistency-checker harness to what the layer *offers*; don't shoehorn
  a transactional workload onto a non-transactional layer — build a sibling harness
  that reuses the *checkers*, not the workload.** Adding an Elle corpus for the
  leaderful Raft KV plane (ADR 0017), the obvious move was a `Topology` variant of
  the Accord corpus — but that harness drives **multi-key transactions** and the
  Raft plane is **single-tablet, non-transactional KV** (one key per op), so the
  workload simply doesn't map; forcing it would mean an enum fork through every
  method *and* a workload that misrepresents the plane. Instead a self-contained
  `raftkv_linearizable.rs` reuses just the proven `check_cycles`/durability/
  convergence + `Recorder` model over a single-key list-append workload. And note
  the counter-intuitive soundness: **serializability is a sound, meaningful check
  on a single linearizable Raft group** (not only on Accord) — the group *is* the
  serialization authority, so a forked/stale read shows as a cycle; there's no
  eventually-consistent read path to manufacture torn-read false positives (the
  hazard that bans `check_cycles` on the AP `Frontier`). The teeth-proof
  (`negative_control.rs`) is shared because the checker is.
- **A flaky `ProdEnv` integration test is a real-world bug, not a determinism
  hole — the determinism guarantee (ADR 0003) is `SimEnv`-only.** The `animusd`
  tests run over `ProdEnv` (real sockets/time/threads) and *poll with timeouts,
  not deterministic assertions* — so an intermittent failure there means a
  genuine timing/durability race, exactly the class `SimEnv` can't catch. Debug it
  (don't just bump the timeout): `create_table_survives_node_restart` flaked
  because (a) its post-restart probe raced the Raft **catalog recovery** — gate on
  the recovered artifact (`await_table_schema` polls `has_table_schema`), the
  pattern the sibling GSI test already used; and (b) deeper, the control plane
  **applied + acked a proposal before its WAL was fsynced** (apply-before-fsync),
  so an abrupt teardown lost the acked schema. Both are now fixed — see the
  durable-before-visible pattern below. **A real-time restart test must wait for
  the recovered state and tear down gracefully.**
- **Durable-before-visible: never expose state a crash could lose.** A node must
  not make a committed entry client-visible (readable / ack-returnable) until it is
  fsynced. The control plane enforces this with a `durable_index` watermark the
  driver advances *after* `env.sync(WAL)`, gating `apply`
  (`min(commit_index, durable_index)`) — so `metadata()`, and any proposer waiting
  on it, only sees durable state (ADR 0009; mirrors `animus-data` `ack_durability`
  and `animus-consensus` `persist_then_ship`). Two consequences worth remembering:
  a core/component driven by hand must **simulate the fsync** (advance the
  watermark) or its applied state never moves; and gating *follower* visibility on
  the follower's own fsync **widens cross-node replication races** — a read on a
  follower right after a create on the leader must wait for the definition to
  replicate to that node (`await_table_*`), not assume the leader's ack made it
  visible everywhere.
- **Two independent, un-jittered fixed-period polling loops that can each "win" a one-shot outcome are a real, silent flake source.** (Found in `cp_reconfigure_loop`'s cadence race with `reconcile_loop`; that mechanism is superseded by ADR 0031 PR4 — the reconciler is event-driven now, no cadence to tune. Full entry archived in `docs/engineering-lessons-archive.md`.)
- **Determinism (ADR 0003) proves logic and ordering, not real-thread liveness.**
  `SimEnv` is single-threaded + cooperative, so a `Mutex` guard held across an
  `.await`, a lost waker, or a leader-election/group-commit deadlock can pass
  every sim test and only hang under the real multi-threaded `ProdEnv`. Any
  concurrency primitive (locks, waker handoffs, group commit, leader election)
  needs a **real `#[tokio::test(flavor = "multi_thread")]` over `ProdEnv`,
  timeout-guarded** so a deadlock fails loudly. (Found via the WAL group-commit
  deadlock; pattern in `animus-storage/tests/lsm_concurrent.rs`.)
- **Don't do slow, non-consensus work on the single task that must service Raft
  liveness — a per-loop stall past the election timeout becomes a self-sustaining
  leader-election storm, invisible to `SimEnv`.** The CP-data driver ran engine
  apply (a batch of LSM `merge`s) + compaction *inline* on the same loop as
  `select(recv, timer)`; under bulk-write load that block took ~180–300ms — longer
  than the 150ms election timeout — so the leader couldn't heartbeat and followers
  couldn't process AppendEntries in time → they campaigned → the deposed leader's
  in-flight writes were truncated → those writes hit the 10s client timeout and
  retried → term climbed continuously and throughput collapsed to ~15/s (a fixed
  count, `≈ concurrency / one-election-cycle`, that *looks* like a per-write latency
  floor but is churn). `SimEnv`'s virtual time never trips a wall-clock election
  timeout, so the whole suite was green. Fix: move apply + compaction to a **separate
  task**, leaving the consensus loop to only persist + step + send (→ term flat,
  ~15–20× throughput). Two split invariants worth remembering: (1) once apply is
  async, the core's `last_applied` **leads** the engine, so anything that reads the
  engine after gating on an index (linearizable ReadIndex) must gate on a separate
  *engine-applied* watermark, not `last_applied`; (2) if two tasks write one WAL
  file, serialize them (async lock) and make the compaction rewrite bounded by
  engine progress + discard the other task's pending records (WAL `replay` is
  push-based → duplicates otherwise). **The guard is a `ProdEnv` load test asserting
  the term barely moves under a bulk seed** (`animusd`
  `seed_load_does_not_storm_cp_elections`) — the exact liveness property `SimEnv`
  can't see. Same family as the group-commit-deadlock entry above: real-time,
  timeout/assertion-guarded. (`animus-cp-data` `drive`/`apply_loop`.)
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
- **Split assertions by *property class*, not just by layer: safety scales to
  adversarial depth, eventual/liveness properties do not.** Serializability is a
  *safety* property — it must hold on every interleaving, so it is sound to assert
  as a hard check across a deep, fault-heavy, many-seed corpus (it held 7,560/7,560
  in the Elle deep tier). Convergence + durability are *eventual* properties
  (anti-entropy + coordinator retry) — "did it converge within the test's fixed
  post-heal drain?" is only sound on a bounded, non-pathological set. At seed-depth
  a compound fault (`lossy`+`stop_restart`) can legitimately leave convergence in
  flight when the drain ends — observed on **both** the pure-Accord and
  data-plane-frontier topologies (opposite seeds), with **no** safety violation. So
  scale the safety check to depth; keep the eventual checks bounded (or, later,
  give them a *converged-or-timeout* poll instead of a fixed-drain snapshot). A
  fixed-deadline assertion on an eventual property reads as a flaky test, not a
  bug. (ADR 0014 deep-tier findings.)
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
- **Judge an *eventual* property (convergence, durability) with a converged-or-timeout
  poll, not a fixed-drain snapshot — then it scales to depth like a safety property.**
  A fixed post-heal `run_for(N)` then a one-shot check imposes a false deadline: at
  adversarial seed-depth a compound fault can leave anti-entropy still in flight when
  the drain ends, so the check flakes without revealing a bug — which is why the
  frontier corpus was once pinned to the bounded base set. Instead drive a *bounded*
  poll (`run_for` an increment, re-read, re-check; stop early once it holds) up to a
  generous budget; only budget exhaustion is a genuine failure. Keep it a pure
  function of the seed (`run_for`/`run_until` only, no wall clock). This let
  `frontier_corpus_converges_and_is_durable` scale to the full deep tier. (ADR 0014;
  `animus-test` `support/mod.rs::run_scenario_with`.)
- **A test helper that binds `:0`, reads `local_addr()`, then *drops* the listener
  has a port TOCTOU — retry the (allocate-fresh-ports + start) as a unit.** The
  freed ephemeral port can be stolen by another test binary before the real bind,
  so the subsequent `run_node` rebind fails `AddrInUse` intermittently under
  `cargo test --workspace` (it flaked the `animusd` restart tests' *first* bring-up).
  Wrap the bring-up in a bounded retry that re-allocates fresh ports each attempt
  (`start_single_node` → `(Node, ClusterConfig)`). A same-address **restart** must
  reuse the captured config (it's testing same-address recovery), so it can't
  re-allocate — retry the *rebind in time* instead (the thief is another binary's
  momentary `free_addrs` probe): `tests/support/mod.rs::restart_same_addrs`. The
  window was once "acceptably tiny", but every retried bring-up added to the suite
  raises probe pressure on everyone else — under `--workspace` load the restart
  tests flaked ~2 in 5 full runs until retried. Both retries are bounded, so a
  genuinely occupied port still fails. (`animusd` tests.)
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
- **A serializability checker must observe the layer that *claims* serializability,
  not an eventually-consistent projection of it.** The Elle corpus observed Accord
  through the **AP data-plane frontier** (a current quorum read); under a
  data-replica fault a committed multi-key write is acked *before* it is
  quorum-durable (fire-and-forget), so a later read can see one key's new value but
  not the other's — a torn read that `check_cycles` correctly flags as a cycle,
  even though **Accord's order is fine**. The signature is unmistakable: cycle-only
  failures, **never** no-fault, convergence + durability always green. Fix is *not*
  to weaken the checker — point it at the serialization authority (pure Accord:
  local execution + versioned-snapshot reads, `Topology::Authoritative`) and check
  the AP frontier for **convergence + durability** only. (ADR 0014 topology split.)
- **A frozen corpus is broad but shallow — one cell × one seed misses
  schedule-dependent bugs; scale *depth* (seeds/cell), env-gated and tiered.** The
  119-cell corpus explored each structural configuration down a single
  name-hashed interleaving; multiplying seeds per cell
  (`ANIMUS_CORPUS_SEEDS=K`, default 1, nightly 40) is what surfaced the
  frontier-read unsoundness above on the *first* deep run. Keep variant 0 = the
  canonical frozen name+seed (so `K=1` is byte-identical and no regression seed
  moves) and `_sNN`-suffix the rest; gate the cost so default `cargo test` stays at
  the frozen base while the deep tier (`ANIMUS_CORPUS_FULL=1` too) runs in a nightly
  CI job, not per-push. (ADR 0014 coverage-expansion increment.)
- **A harness's client poll granularity bounds which timing windows it can catch —
  a "passing" corpus proves nothing about sub-poll windows.** The 2026-08-06 audit
  confirmed a ReadIndex linearizability hole (a new leader serves reads before its
  current-term no-op commits, ADR 0017 §3) that `raftkv_linearizable.rs` structurally
  cannot fire: the stale window is ~one message round-trip after an election, but the
  client polls at 100ms, so it never samples the sliver — and the single-writer
  re-propose model heals the evidence. When a protocol has a known
  narrow-window rule (ReadIndex no-op, lease expiry, config overlap), write a
  targeted sim test that *drives into the window* (sub-poll granularity, read
  immediately on the new leader), don't rely on corpus luck.
- **An adversarial-verify pass is worth it before acting on audit/review findings —
  and re-verify against the branch you'll edit.** Of the audit's 6 highest-stakes
  claims all 6 confirmed, but two materially changed shape under verification (the
  storage flush bug's trigger is admin flush/compact, not client writes; the
  ~15/s seed throughput was primarily the 50ms confirm-poll cap, not the election
  storm) — and several perf findings were already fixed on `main`, which had moved
  past the audited checkout (pre-vote, single-write-latency, cp-batch-put). A
  finding is (claim × trigger path × branch); verify all three.
- **File reads taken shortly after a branch checkout can transiently disagree
  across tool families (Read/Edit vs Bash) — verify you're actually at HEAD
  before trusting either.** An agent building against `origin/perf/cp-data-
  snapshots-codec` initially saw a stale 1053-line `animus-cp-data/src/lib.rs`
  via `Read` while the true tip was 1482 lines with a materially different
  architecture (split consensus-loop/apply-task, wake-on-propose, a binary wire
  codec) — caught only because a test file referenced methods the "current"
  file didn't have. `git show HEAD:path` gave a third answer on repeated calls.
  Recovery: `git status --short` + `git diff --stat HEAD -- path` both empty is
  the only trustworthy "am I at HEAD" check; for a file where Bash-side
  build/test is the actual gate, `git checkout -- path` + direct Bash
  edits (sed/perl) are safer than Read/Edit if this is suspected. (PR #31.)
- **A `SimEnv` test must never `block_on` an operation that internally polls
  `env.sleep()` (e.g. `linearizable_get`/`linearizable_scan`)** — those only
  resolve while `Simulator::run_for` is advancing virtual time; calling one
  directly under `block_on` hangs forever with no panic, burning wall-clock
  silently. Spawn it as a task and drive it via `sim.run_for` instead (the
  `lin_read`-style helper pattern in `tests/read_index.rs`). (PR #31.)
- **Distinguishing "crash-torn tail" from "mid-file corruption" needs a
  positional proof, not a magnitude heuristic — scan forward for the next
  valid checksummed frame; if one exists, the failure is real corruption.**
  A torn-and-happens-to-look-corrupted tail and genuine mid-file corruption
  can produce equally implausible declared lengths, so "does the length look
  sane" can't tell them apart. The WAL's binary frame decoder resolves a
  parse failure by resyncing forward: tolerate it as a crash-torn tail only
  if NO later valid frame is found in the buffer; otherwise it's a hard
  error. (`wal_resync_point`, PR #32.)
- **A test suite built entirely on bare `block_on` cannot observe a
  `env.spawn_task`-backed background feature — check the harness before
  defaulting a new async-offload feature on.** Storage's tests never drive
  `Simulator::run_for`/`run_until`, so a new "move maintenance to a spawned
  task" feature would silently never run under the existing suite. Shipped
  correctly as additive and default-OFF rather than rewriting the test
  harness to flip it on. Corollary of "SimEnv proves logic, not real-thread
  liveness" — but also a warning to CHECK the harness shape before assuming a
  feature can default on. (PR #32.)
- **Extracting a "pure decision" from a method that intentionally short-circuits
  an expensive call must preserve that laziness explicitly, or the refactor
  silently becomes a hot-path perf regression.** `resolve_cp_route` avoided
  `RaftNode::metadata()`'s full deep-clone on the common "local leader" /
  "known hint" paths by checking cheap facts first; pulling the branching out
  as a pure `decide_cp_route` function required the wrapper to keep gathering
  metadata-derived facts lazily (only in the one branch that needs them)
  rather than eagerly computing everything before calling the pure function.
  When extracting logic mechanically, check what expensive input the original
  short-circuited around, not just what it decided. (PR #33.)
- **Before extracting a flagged "untested pure function," check whether it's
  already a thin call-through to a pure/tested implementation elsewhere.**
  `next_free_tablet_id` looked like animusd's problem (the audit flagged the
  *caller*, `trigger_split`) but the allocator itself was already pure and
  unit-tested in `animus-control::Metadata` — nothing to extract, just a
  caller that wasn't using it (fixed separately in PR #21). (PR #33.)
- **Extending a shared trait's addressing with a new axis: make the primitive
  methods the ones every implementor must write, and re-derive the old surface
  as *default* methods over a well-known constant.** Adding multiplexed
  `(node, stream)` addressing to `Network` (ADR 0026, replacing the
  `Coresident` sibling-pool escape hatch's rationale) needed every existing
  call site (`env.send(to, payload)` / `env.recv()`, nearly the whole
  codebase) to keep compiling and behaving identically. Making `send_stream`/
  `recv_stream` the trait's required methods and `send`/`recv` **default**
  methods that forward to them with a `PRIMARY_STREAM` constant meant the only
  code that had to change was the *three* concrete `Network` implementors
  (`SimEnv`, `ProdEnv`, and one test double) — every caller was untouched,
  because a default method is in scope exactly like a required one once the
  trait is in scope. Grep every `impl <Trait> for` site *before* estimating
  blast radius; it is often far smaller than "everywhere the trait's methods
  are called." (PR #34.)
- **In a worktree session, an absolute-path tool call (Read/Edit/Write) is not
  scoped by the shell's `cd` — pin every path under the worktree root
  explicitly, every time.** A `Bash` `cd /path/to/main/repo && ...` changes the
  *shell's* cwd for subsequent Bash calls, but Read/Edit/Write take literal
  absolute paths and don't care what the shell's cwd is — so it is easy to
  `cd` into the main checkout for one command (e.g. to run cargo from a
  familiar path) and then keep handing Read/Edit/Write paths that *look*
  worktree-rooted but are actually bare `/repo/...` paths resolving into the
  main checkout, silently editing a different working tree than intended.
  The tell was a `git status` on what should have been the worktree suddenly
  reporting the *main repo's* branch name, and a test binary not picking up an
  edit that Read/Edit had just reported succeeding — both mean the tool and the
  build are looking at two different files. Recovery: `git diff` the
  suspect-wrong checkout, confirm which hunks are genuinely new (not
  pre-existing unrelated dirty state) before touching anything, revert only
  those, and re-apply them (a filtered `git apply --include=<path>` off a saved
  patch is faster and safer than re-doing every edit by hand) in the correct
  location. Never `git checkout --`/reset a dirty file without first diffing
  it to confirm every hunk is yours. (PR #34.)
- **A fault-schedule runner that heals immediately after the last fault gives
  single-fault scenarios a zero-length outage — give scenarios an explicit fault
  window.** The raftkv corpus healed partitions the instant the last fault landed,
  so its partition cells were near-vacuous (nothing was ever asked of the cluster
  *while* partitioned). New cells carry `Scenario::window` (outage duration with
  traffic spanning it); old cells keep window 0 for byte-identity. Check any new
  fault harness for this: "did traffic actually run during the fault?" (PR #23.)
- **A "recovery tolerates X" claim must be tested through the NEXT write cycle,
  not just one reopen.** The LSM tolerated a torn WAL tail on replay (skipped the
  torn line) but reused the un-truncated active segment, so the next acked record
  was appended after garbage and a SECOND restart silently dropped it — the
  crash-recovery instance of the "prove recursive ops at depth ≥ 2" rule: recover,
  write, recover again, then assert. (PR #24's fault injection; fix = seal the
  recovered segment.)
- **A test asserting data LOSS can be load-bearing on a consensus bug — when a
  correctness fix flips it, invert the test, don't weaken the fix.** A restart
  test asserted acked data on the memory backend is lost across restart; that
  "expected loss" actually depended on a sole recovered voter never re-advancing
  commit over its WAL tail (a real bug). The ReadIndex-gate fix surfaced it; the
  test now asserts survival via Raft-WAL replay. (PR #25.)
- **When a process-scoped convenience shortcut is removed, grep for tests that
  quietly relied on it to *assert something the removed shortcut made trivially
  true* — not just tests that time out.** Making `--cluster N`'s in-process
  `ClusterEdgeState` per-node (ADR 0031 PR2, closing the gotcha above) broke
  exactly one of ~90 `animusd` tests: `cql_wire.rs`'s cross-connection
  `EXECUTE` assertion, which `PREPARE`d a statement via node 0 then
  `EXECUTE`d it via a connection to node 1 to "prove the prepared store is
  shared across connections" — true only because the old shared edge made
  every node's `CqlState` the same object. Per-node, that's not a bug to fix,
  it's the **correct, intended new behavior** (a real one-process-per-node
  deployment never shared this either) — so the honest fix is to change what
  the test proves: reuse a **second connection to the same node** (`conn0b`)
  for the cross-connection assertion, and keep the cross-*node* connection
  for what's actually still cross-node-safe (reading committed CP-plane
  data). The signature to watch for isn't a hang/timeout (this failed with a
  clean, immediate `Error` response) — it's an assertion whose comment
  literally describes the removed shortcut's own guarantee ("shared across
  connections/nodes/processes"); grep test comments for the word "shared" (or
  "cluster-wide", "any node") near the specific state you're scoping down,
  not just the obvious call sites. Every other test that exercised
  cross-node behavior already did so through a *real* mechanism (replicated
  `Metadata`, `cp_route` forwarding, `propose_schema` relay), so removing the
  shortcut made those tests exercise more real code, not less — 100% of the
  rest of the workspace suite passed unmodified, including several
  (`cp_plane.rs`, `cp_rebalance.rs`, `cp_reconfigure.rs`) that now genuinely
  drive cross-process-style forwarding in-process for the first time instead
  of resolving everything locally through the shared registry. (`animusd`
  `tests/cql_wire.rs::cql_wire_prepare_execute_typed_round_trip`.)
- **A test that hand-drives a real Raft transfer/membership change must retry
  the whole arm/propose sequence on a poll tick, never assert success on one
  attempt — even when the immediately-preceding `is_leader()` check was
  synchronous and just returned true.** Building the ADR 0031 PR5 reconciler
  lifecycle corpus, two hand-rolled "force a real membership removal" test
  helpers passed every run at low seed depth and then hit
  `NotLeader{leader: Some(<the exact node just confirmed as leader>)}` from
  `change_membership`/`transfer_leadership` at `ANIMUS_RECONCILER_SEEDS=60`
  and `=150` — a real, already-documented core behavior (`propose`/
  `change_membership` **freeze**, returning `NotLeader` with the *transfer
  target* as the "leader" hint, while a leadership transfer is armed
  elsewhere in the group; see this file's "two-layer gate" entry) that a
  single-shot assert cannot distinguish from a genuine failure. No amount of
  sleeping between the `is_leader()` check and the propose call closes this,
  because the freeze can arm *after* the check — the fix is to fold the
  whole "check → act" sequence into the body of a bounded retry poll (`check
  condition; if not met, attempt the action; return false; poll again`) and
  only fail once the bound is exhausted, exactly like every production retry
  loop in this codebase already must (`ProposeResult::Accepted` isn't
  `committed`, and `NotLeader` isn't necessarily permanent). This is the same
  discipline as the standing "a retry loop over a Raft write must distinguish
  never-accepted from accepted-unconfirmed" entry, just showing up inside a
  *test's* orchestration code instead of production code — seed depth is what
  surfaced it, at low depth every run happened to avoid the race window.
  (`animus-cp-data/tests/reconciler_corpus.rs::remove_replica_for_real`,
  `scenario_partition_blocks_release`.)
- **`RaftKvNode::linearizable_get`/`linearizable_scan` only ever serve on the
  confirmed leader — calling them on a follower returns `None` unconditionally
  (the ReadIndex ban unconditionally fails for a non-leader), not a slow or
  stale read.** A test that wants to confirm a write *replicated* to a
  follower (as opposed to confirming linearizability) must read that
  follower with `local_get` (a raw, non-linearizable engine read), not
  `linearizable_get` — calling the latter on whichever handle isn't currently
  leading is not "testing the follower," it's testing a guaranteed `None`,
  and asserting `Some(value)` against it fails deterministically regardless
  of how long you wait. Caught immediately (first run) by the ADR 0031 PR5
  reconciler corpus's 2-replica scenarios asserting both replicas' handles
  via `linearizable_get` — fixed by reading the leader linearizably and
  polling the follower with `local_get`.
- **Making a simulator/executor handle `Clone`-able (when its fields are
  already `Arc`-backed shared state) is a small, safe, additive change worth
  making the moment a test needs to carry fault-injection capability *into* a
  spawned async task** — don't route around the missing `Clone` with an
  awkward workaround (a channel back to the outer synchronous scope, a
  second parallel handle type, restructuring every scenario to interleave
  fault injection from the outside). `animus-sim::Simulator` held only an
  `Arc<Shared>` + a `u64` seed, had no `Drop`, and its per-node handle
  (`SimEnv`) was already `Clone` for exactly this reason — so adding
  `#[derive(Clone)]` to `Simulator` itself cost nothing and immediately
  unblocked a harness where each scenario's own spawned "driver" task needs
  to call `&self` fault methods (`stop`/`crash`/`partition_pair`/`heal`/
  `env`) while the outer test thread keeps a separate handle for the `&mut
  self` `run_for`/`run_until` driving loop. Check for a `Drop` impl and
  whether every field is itself cheaply `Clone`-able before assuming a type
  wasn't made `Clone` for a real reason — here it clearly wasn't, it just
  hadn't been needed yet. (`animus-sim::Simulator`;
  `animus-cp-data/tests/reconciler_corpus.rs`.)
- **The rebalancer converges the *global* imbalance to `max − min ≤ 1` and
  stops — it makes no per-table promise, so a test must not route an op
  through ONLY a just-grown node for an *arbitrary* table.** Building the ADR
  0032 PR2 seed/join test, "the joined node hosts a replica of *some* tablet"
  is the stable rebalancing signal, but writing through only that node's
  client address for a table it does *not* replicate flakes bimodally
  (~40%): `resolve_cp_route`'s no-local-replica branch forwards blindly to
  *some known replica* of the tablet — not its leader — and the receiving
  `cp_serve_forwarded` never re-forwards (routing is bounded to one hop), so
  a forward that lands on a follower errors "not the leader here" on every
  retry with the same first-listed replica. Two sound test shapes: gate on
  the *specific table* the node actually replicates (poll `/admin/status`'s
  per-tablet `table` + `replicas` and pick that table for the
  through-only-this-node ops), or give the client every node's address
  (`cluster_growth.rs`'s round-robin `put`). The one-hop-blind-forward
  behavior itself is a known production shape (the client is expected to
  retry with fresh routing), not a bug this test should have papered over
  with a longer timeout. (`animusd` `tests/seed_join.rs::table_with_replica`.)
- **Adding an automatic background registration/bring-up step makes any
  test's "not yet registered" pre-assertion a race, not an invariant — sweep
  for assertions on the *absence* of state the new automation now
  establishes.** Folding growth-node membership self-registration into
  `start_with` (ADR 0032 PR2) broke `cluster_growth.rs`'s sanity check that
  a freshly-started growth node "should not be a member before admin-add" —
  intermittently (the self-registration + heartbeat promotion can complete
  before the test's first poll, or not), the worst kind of breakage. The
  dual of the documented "removed shortcut → grep for tests that relied on
  it" lesson: an *added* automation invalidates assertions about the
  pre-automation quiescent state. The honest fix is to delete the stale
  pre-assertion and let the convergent post-state assertion (it *does*
  become `Active`) carry the proof. (`animusd` `tests/cluster_growth.rs`.)

### Code patterns
- **A health/status rollup that gates on a *proxy* signal (a member's `Down`
  status) rather than the actual risk that signal stands in for (a tablet
  under-replicated/leaderless) can diverge from reality forever, because the
  two clear on different triggers.** The dashboard's `computeHealth()`
  (ADR 0021) treated any `Down` member as itself "degraded" — but a `Down`
  member only clears on manual decommission (ADR 0032 PR3) or the node
  rejoining, while the actual data-loss risk it represents is cleared much
  sooner, automatically, once the placement reconciler repairs every tablet
  the dead node used to replicate onto a spare (`failure_auto_replaces_
  replica_onto_spare`). So a cluster whose data was fully re-replicated
  within seconds could show "Degraded" indefinitely, until someone
  remembered to decommission the long-dead node. Fixed by keying "degraded"
  on the tablets' own derived status (`leaderlessCount`/`underReplicatedCount`,
  already computed per-tablet for the "Under-replicated" stat tile) instead
  of the member roster; `downCount` is kept as informational context in the
  banner/tiles, not a health-gating input. **General check for any rollup
  built from "X is down/unhealthy ⇒ overall is unhealthy": does the thing
  being protected (data replication, request-serving capacity) actually
  recover on a faster/different path than the raw signal does — and if so,
  gate on the protected property, not the signal.**
- **A quorum primitive's "who do I need acks from" and "how many acks do I
  need" must both read the group's *live* Raft config — never a peer set
  captured once at construction, even one that looks read-only/immutable.**
  Building automatic replica rebalancing (ADR 0029) needed a *healthy* replica
  move (not just failure repair), which for the first time could rotate a
  majority of a tablet's Raft group onto nodes that were never in any
  surviving replica's original peer set. `RaftKvNode`'s ReadIndex barrier
  (`animus-cp-data`) had silently keyed both its ack-quorum threshold
  (`majority()`) and its probe fanout on `all_nodes` — the group's peer set at
  *hosting time* — instead of `RaftCore::config()` (the live, dynamically
  updated voter set already used everywhere else in the same crate). This was
  invisible for the entire life of the feature it was built for: every
  membership change before ADR 0029 was a same-size, pre-known swap (a
  failure-repair spare was already listed in every replica's `all_nodes` from
  the moment the group formed), so the stale and live sets never actually
  diverged. The break only showed up once a *different* feature (rebalancing)
  exercised a membership shape (a full rotation) the original code was never
  tested against — a stale-quorum leader could only ever self-ack, so every
  linearizable read on that tablet timed out and reported the key **absent**,
  indistinguishable from real data loss from outside. A second, compounding
  bug in the same feature made it worse: `animusd`'s CP-routing short-circuit
  (`resolve_cp_route`) trusted "I have a locally registered group handle" as
  proof of being a *current* replica — true before ADR 0029, false during the
  new removed-replica GC's deliberate grace window — so a node that had just
  been rebalanced off a tablet, but not yet GC'd, waited forever instead of
  forwarding to the tablet's actual current replicas. **General check when
  adding a new way an existing invariant can change** (here: "a group's peer
  set can evolve after hosting," where before it was fixed for a group's whole
  lifetime): grep every place that invariant's *original* form was cached or
  assumed stable, not just the one mechanism you're adding to change it — an
  optimization that skips re-deriving a fact from live state ("no `Metadata`
  clone needed, I already have a local handle") is exactly where this hides,
  because it was correct on every input anyone had tried before. Caught by
  building a genuine end-to-end integration test (`animusd/tests/
  cp_rebalance.rs`, a 5-node cluster with tables provisioned before growth) —
  no unit or sim test at either layer alone exercised a *full* replica-set
  rotation through a *linearizable read*, only through `local_get`/config
  equality. When writing a regression test for "a stale peer keeps
  responding," make the peer actually stop responding (`shutdown()`), not
  just remove it from the current config — a still-live departed peer can
  accidentally still ack on a bare term match and mask the very bug the test
  exists to catch. (`animus-cp-data::RaftKvNode::majority`/read-barrier probe
  fanout; `animusd::resolve_cp_route`'s `has_local_replica` gate.)
- **A two-layer gate where the selector and the actuator use different
  thresholds fails silently — and a primitive's `bool`/`Result` return value
  that encodes "did this actually take effect" must never be discarded, however
  statement-shaped the call looks.** ADR 0029's leadership-transfer primitive
  had exactly this shape: `RaftKvNode::reconfigure_step`'s step 4 *selected* a
  transfer target with `peer_match(n) >= commit_index()`, but
  `RaftCore::transfer_leadership` only *armed* at `peer_match(target) ==
  last_log_index()` — a stricter threshold the selector never checked — and
  the caller wrote `self.transfer_leadership(target);` with the returned
  `bool` dropped on the floor. `propose` is fire-and-forget (it appends to the
  leader's local log and returns before any replication round trip), so on a
  write-hot tablet `last_log_index` moves the instant a write is accepted
  while every peer's `peer_match` still reflects the *previous* entry — the
  two thresholds disagreed at essentially every sampling instant, so the arm
  failed *forever*, and nothing ever surfaced it: no error, no log, no metric,
  just a rebalance move that silently never completed for any tablet whose
  move needed to relocate its leader. The correct fix is standard Raft §3.10
  semantics, not just threshold alignment: relax the arm gate to match the
  selector (`>= commit_index`), but that alone reintroduces the original
  danger (arming to a target that isn't actually at `last_log_index` yet), so
  **freeze `propose`/`change_membership`** while a transfer is armed (return
  `NotLeader`, hinting the target) so the log stops growing and replication
  can close the remaining gap, gate the actual `TimeoutNow` send on the
  target *reaching* `last_log_index`, and **abort** (clear the arm, resume
  proposing) if a deadline passes with no step-down — else a target that
  crashes right after arming strands the group frozen forever. A related,
  narrower bug in the same function compounded it: the down-extra search
  reused a generic "lowest non-self extra" helper and only *then* filtered it
  on down-ness, so a `Down` extra sorting after a healthy one was invisible —
  the step fell through to a *different*, catch-up-gated removal path, which
  could stall behind an unrelated survivor's lag. **General checks:** (1) when
  a value is computed once to pick a candidate and re-derived/re-checked
  inside the primitive that acts on the candidate, diff the two conditions —
  "selects X" and "arms X" must agree on what "eligible" means, or the
  narrower one silently wins every time; (2) grep for every call to a
  bool/Result-returning mutator where the result is bound to `let _ =` or not
  bound at all — if the primitive's doc says "returns whether it took effect,"
  a discarded result is a designed-in blind spot; (3) a "search for the first
  match of predicate P" helper reused with an *unrelated* predicate applied
  only to the first result (`extra().filter(down.contains)`) is a common way
  to accidentally scope a search to "the first element of the base sequence,"
  not "the first element satisfying the actual predicate" — write the combined
  predicate into the search itself. (`animus-control` `RaftCore::
  transfer_leadership`/`propose`/`change_membership`/`broadcast_append`;
  `animus-cp-data::RaftKvNode::reconfigure_step`; regressions in
  `animus-control/tests/leadership_transfer.rs`,
  `animus-cp-data/tests/leader_transfer_reconfigure.rs` — the hand-driven
  variant is the one proven to fail against the pre-fix source — and
  `animus-cp-data/tests/reconfigure_down_extra_priority.rs`.)
- **`tokio::fs::File` writes are not ordered or durable until `flush().await` —
  a dropped handle completes its write in the background, so two sequential
  appends via separate handles can land INVERTED on disk, and a later `sync` on a
  fresh fd can fsync before the buffered write reaches the page cache.** This
  broke "ack means durable" under ProdEnv and was the long-standing
  `lsm_concurrent::scans_survive_concurrent_compaction` flake (an SSTable
  recovered with its index at offset 0). Always `flush().await` before dropping a
  write handle; found independently twice (PRs #26, #27). Corollary of the
  documented "a flaky ProdEnv test is a real bug" rule.
- **Commit the election no-op in `become_leader` itself (`maybe_advance_commit`
  after the append)** — a leader that only advances commit on propose/ack strands
  a sole voter's recovered WAL tail (nothing re-drives commit until the next
  propose), and any gate on "current-term entry committed" (ReadIndex §6.4, the
  membership-change gate) would deadlock a single-node group. (PR #25.)
- **Metadata-level dedup of a proposal only picks one *winner* — it does not stop other legitimate callers from invoking a side-effecting state-machine command, which must therefore be idempotent at APPLY time, not just deduped at the propose layer.** (Found in the pre-ADR-0028 two-phase split; superseded by ADR 0028's single-command split. Archived in `docs/engineering-lessons-archive.md`.)
- **An operator/admin action that calls straight into an engine bypasses the
  single-writer contract the normal path establishes — audit every admin surface
  against the layer's concurrency assumptions.** `LsmEngine` is safe on the client
  path because the per-tablet Raft apply loop is its sole writer, but
  `POST /admin/storage/flush|compact` call `flush_now`/`compact_now` from the admin
  connection's task, racing that loop — and `flush()` (snapshot → unlocked build →
  unconditional `memtable.clear()`, no flush-in-progress flag) then erases an acked
  concurrent write, whose WAL segment a *later* flush GCs: permanent loss. The
  concurrency tests miss the quadrant (the concurrent-writer test never flushes;
  the flushing test has one writer) — test "forced maintenance under live load"
  explicitly. (2026-08-06 audit; ADR 0008/0020 notes; fix = serialize
  flush-vs-apply and flush-vs-flush.)
- **One id space must have one allocator — a second allocation path silently breaks
  the invariant the first one carries.** Tablet ids are never-reused *because*
  provisioning allocates via `next_free_tablet_id()` (folds in the monotonic
  `next_tablet_id`); `trigger_split` allocated `max(live ids)+1` instead, so
  drop-highest-table-then-split re-mints the freed id — and a replica still holding
  the dropped tablet's files re-hosts them as the new tablet (ADR 0024 violation;
  GC can never reclaim them since the id is live again). The apply-side validation
  only rejected collisions with *present* tablets, so nothing self-healed. Route
  every mint through the one allocator, and make the replicated apply reject ids
  below the monotonic counter so a divergent client can't reintroduce it.
- **A new state-mutating replicated command needs the *same* CAS/precondition
  discipline as its sibling commands on the same resource — a missing guard is
  invisible until two proposers race.** `MetaCommand::SplitTablet` applied
  unconditionally as long as its `split_key` fell inside the source tablet's
  *current* range, with no check on the tablet's epoch — unlike its sibling
  `CasTabletReplicas`, which already gates on `expected_epoch`. Two proposers
  racing to split the same tablet at the same epoch (two independent
  `animusd::auto_split_loop` instances, or an auto-trigger racing a manual one)
  could each compute a different median from an equally-stale range view and
  **both commit**: each `SplitTablet` mutates the source's range and mints a new
  child id, and neither commit's precondition ever looks at the other's. But the
  tablet's own per-tablet CP-data Raft group can only ever apply **one** real
  `Split`, ever (an at-most-once apply-time guard there) — so the losing
  metadata-level split's `new_id` becomes a permanent, leaderless,
  metadata-only orphan tablet: present in `Metadata.tablets` with a real
  range/replica set, but with no CP group anywhere in the cluster and no code
  path that ever revisits it (the `auto_split_loop`'s existing "abandoned"
  detection correctly stops *retrying* the losing key, but never cleans up the
  `new_id` it already minted). Found live on a `--cluster 3 --auto-split 2000`
  bulk-seed run (two orphaned tablets, `/admin/status` showed real ranges,
  `/admin/raftkv` showed no group for either). Fixed by adding
  `expected_epoch: Epoch` to `SplitTablet`, gated identically to
  `CasTabletReplicas` — so the loser's step 1 (`propose_split_metadata`) now
  fails cleanly (`Rejected("epoch mismatch")`), which `auto_split_loop` already
  handles as "nothing was allocated, no orphan to track." **When adding a
  command that mutates a resource another command already CASes, check whether
  the new command needs the same guard — "my precondition happens to still
  hold" is not the same as "no one else committed a conflicting change since I
  read this."** (`animus-control` `meta.rs::split_rejects_a_stale_epoch_racing_a_concurrent_split`,
  `tablet_split_merge.rs::racing_splits_at_the_same_epoch_only_one_applies`.)
- **A CAS guard closes the *concurrent* instance of a race; a *sequential* instance of the same race needs its own answer — usually cleanup, not another precondition.** (Found in the pre-ADR-0028 orphan-tablet cleanup, now structurally impossible; archived in `docs/engineering-lessons-archive.md`.)
- **A retry loop keyed on a resource id must recheck the resource still exists — a precondition that only checks its own transient state silently assumes the resource itself is immortal.** (Found in the pre-ADR-0028 `auto_split_loop` pending-retry map, since deleted; archived in `docs/engineering-lessons-archive.md`.)
- **Before reaching for "remember everything" to disambiguate an edge case, check whether a cheap, independent check at the point of irreversible action can bound the state to O(1) instead.** (Found in the pre-ADR-0028 `current_split_bound`, since the data-plane half of split was removed entirely; archived in `docs/engineering-lessons-archive.md`.)
- **To wake a `select`-parked `<E: Env>` driver loop from another task, race a
  `futures::task::AtomicWaker` + `AtomicBool` future — never a tokio-only primitive
  (`Notify`/`watch`), which SimEnv can't drive.** The CP data-plane driver used to
  leave a freshly-proposed Raft entry parked until the next ~50ms heartbeat tick; the
  fix (single-write latency, ADR 0017) has the proposer raise a flag + `wake()` and
  the consensus loop race a third `select` arm that resolves on it, then
  `replicate_now` immediately. `AtomicWaker` is executor-agnostic: under `SimEnv` the
  synchronous `wake()` marks the driver task ready for the next run-loop poll (fully
  deterministic, no wall clock); under tokio `ProdEnv` it resolves the register/wake
  race. Two disciplines keep it correct: the waiting future **registers the waker
  *before* checking the flag** (else a wake between check and park is lost), and
  **consumes the flag** (`swap(false)`) on resolve so it doesn't busy-spin. Pair the
  wake with the *consumer-side* poll it unblocks: a fast propose is pointless if the
  ack path still polls on a coarse fixed interval — `animusd`'s `cp_put_local` confirm
  loop was cut from a fixed 50ms to a ~200µs→5ms adaptive back-off in the same change
  (median lone-write latency 52ms → 11ms, `cp_plane.rs::single_write_latency_is_low`,
  a `multi_thread` `ProdEnv` liveness test — the sim can't measure real-thread
  latency). (`animus-cp-data` `ProposeSignal`; `RaftCore::replicate_now`.)
- **A new `ClientRequest` variant that can be *forwarded* must be handled in BOTH
  the main serve loop AND `cp_serve_forwarded` — a single-node test can't catch the
  missing half.** `animusd` CP ops route locally or **forward one hop** to the
  leader's node wrapped in `ClientRequest::Forwarded`; the receiver dispatches the
  inner request through `cp_serve_forwarded`, a *separate* match from the top-level
  serve loop. A batch (`PutBatch`) added only to the serve loop works whenever the
  connected node happens to host the tablet leader and silently errors ("unexpected
  forwarded request") when it must forward — the same bimodal per-process failure
  shape as the `is_relayable_command` allowlist gap. When adding a forwardable
  variant, grep for the request enum's name across *both* match sites and add the
  arm to each; regression-test it through a **follower/non-leader-connected** node
  in a per-process cluster. (`animusd` `cp_serve_forwarded`; batch put, ADR 0017.)
- **WAL group commit only coalesces *concurrent* writers — a *sequential* apply
  loop pays one `fsync` per op and needs an explicit batch primitive.** The
  per-tablet CP-data Raft apply loop (`flush_and_apply`) applies a run of committed
  commands from **one** task, `await`ing each `merge`/`merge_tombstone` in turn; the
  WAL group commit (which amortizes the `fsync` across writers *ready in the same
  drain cycle*) sees exactly one in-flight writer, so every command paid a full
  `fsync` (~5-9ms on real disk; ~180ms for a 20-40 command batch). The fix is a
  `StorageEngine::merge_batch(Vec<MergeOp>)` that logs the whole run as **one WAL
  record + one `fsync`** then applies it under one lock (defaulted to a per-op loop
  so `MemoryEngine`/others are unaffected; `LsmEngine` overrides it). The apply loop
  accumulates a run of `Put`/`Delete` into a batch and drains it before any command
  that must *read* committed state (`Cas`, `Split`) so the read still sees prior
  writes. Measured ~9.7x apply throughput (1851→17918 puts/s), fsyncs 30x fewer.
  **Lesson: "we have group commit" does not make sequential writes cheap — group
  commit is a concurrency optimization; a single-task write run needs a batch API.**
  (`animus-storage` `merge_batch`; `animus-cp-data` `flush_and_apply`.)
- **A "send X" path that falls back to a *default* when X is absent can ship a
  silently-corrupt value — make the absent case impossible (set X at every state
  transition that needs it), not `unwrap_or_default()`.** The per-tablet CP Raft
  ships its engine image as `snapshot_blob`, set by the driver only on *compaction*;
  the leader's `snapshot_chunk_for` did `snapshot_blob.unwrap_or_default()`. A node
  that caught up via a *received* `InstallSnapshot` advanced `snapshot_index > 0` but
  never set its blob, so when it later became the source it shipped **0 bytes** — the
  receiver decoded an empty image (`EOF while parsing a value, line 1 column 0`),
  dropped it, and never caught up (surfaced as "CP split: new tablet never appeared",
  a *leaderless* split child). The fix sets `snapshot_blob` on the *install* path too,
  so the invariant `snapshot_index > 0 ⟹ blob.is_some()` holds and no ship is ever
  empty — far better than a 0-byte default that *looks* like a valid transfer. **A
  recursive/relay protocol must hold its invariant at the *second hop*: A→B works
  off A's freshly-built state; B→C is what exposes that B never retained what it
  received.** A unit test that drives only one hop (`A→B`) misses it — drive the
  re-ship (`A→B`, then `B`-as-leader→`C`).
  (`animus-control` `raft.rs::handle_install_snapshot`; regression
  `driver_applied_sm.rs::caught_up_node_reships_non_empty_snapshot`.)
- **A per-message O(state) serialize on a Raft consensus loop is a latent
  election-storm hazard, and a *cache* to fix it must not double the work it replaces
  — reuse the one serialization everywhere the state is needed.** The control-plane
  `snapshot_chunk_for` re-serialized the whole `Metadata` **per 1KB InstallSnapshot
  chunk**; on a multi-MB metadata a follower catch-up shipped ~thousands of chunks
  (~50ms serialize each), pinning the loop far past the 150ms election timeout — a
  self-sustaining storm during any large-state catch-up (the control-plane twin of
  PR #16's CP-data apply/compaction storm). Fix: **cache the serialized image once
  when `snapshot_index` advances and slice it per chunk** (O(chunk)). But the naive
  cache *doubled* compaction cost — the blob serialize **plus** the WAL `Snapshot`
  record's own metadata serialize — so reuse the cached bytes for the WAL too
  (`serde_json` `RawValue` embeds the pre-serialized image verbatim; byte-identical,
  guarded by a round-trip test). Two morals: (1) the cache must be pinned to
  `snapshot_index`'s state, serialized **eagerly at snapshot time** (in-core
  `metadata` advances past the base between compactions, so lazy-at-ship would ship
  a state *ahead of* its claimed index → the follower double-applies its log tail);
  (2) **this hazard is invisible to `SimEnv`** (virtual time never trips the
  wall-clock election timeout) — the teeth is a wall-clock-timed transfer
  (`install_snapshot.rs::large_snapshot_ships_in_o_chunk_time_not_o_state`: fix ~ms
  vs regression ~46s), because a *live* `ProdEnv` cluster catch-up races
  leadership/AppendEntries and won't reliably traverse a long chunk-stream.
  (`animus-control` `raft.rs::snapshot_chunk_for`/`snapshot_upto`/`encoded_wal_image`,
  `persist.rs::encode_snapshot_record_from_blob`.)
- **When mirroring a fix onto a *sibling* subsystem, assess honestly — the sibling
  may have a *different-shaped* version of the hazard, or a bounded one not worth the
  same risky refactor.** PR #16 moved CP-data's async **engine apply + compaction**
  off its Raft loop (a >150ms self-sustaining stall). The control plane applies its
  state machine **in-core, synchronously** — no async apply to move — so its only
  loop-blocking O(state) work is snapshot-shipping (fixed above, cheaply) and the
  compaction WAL-rewrite serialize. The latter is a *single* stall (~50ms at ~1MB,
  ~120ms at ~3MB), under the election timeout at realistic scale and **not**
  self-sustaining (once per 64 applied entries). Moving it fully off the loop would
  couple the install→WAL-rewrite ordering into a second task on the most
  safety-critical Raft (real risk) for a bounded, rare, extreme-scale stall — so it
  was **measured, documented, and deferred**, not force-fit. A well-reasoned "the
  sibling's hazard is smaller; here's the measurement" is a valid outcome.
- **A recursive operation that "works" once may be relying on a depth-1 coincidence — prove it at depth ≥ 2.** (Found in the pre-ADR-0028 split-hook/member-id derivation, since deleted by ADR 0026 Stage B / ADR 0028; archived in `docs/engineering-lessons-archive.md`.)
- **Distinguish "seed a fresh child" from "join an existing group empty" by a durable monotonic signal, not a race.** (Found in the pre-ADR-0028 split-handoff design, since removed — a fresh split child needs no handoff seeding at all; archived in `docs/engineering-lessons-archive.md`.)
- **Which physical engines a node hosts is *local* durable state, not derivable from replicated `Metadata`.** (Found in the pre-ADR-0028 `cp-hosted` marker, since removed — every tablet on a node now shares one engine opened once at start; archived in `docs/engineering-lessons-archive.md`.)
- **Keep a replicated map in stable canonical ids; translate to any locally-derived ids at the edge, not in the replicated state itself.** (Found in the pre-ADR-0028 base/member id split, since removed — a tablet's CP group member id is now always its base id; archived in `docs/engineering-lessons-archive.md`.)
- **Drive cross-plane reconfiguration by *pull from replicated state*, not a new
  push command — it keeps the dependency edge one-way and the seam testable.**
  Wiring the control plane to reconfigure a per-tablet Raft KV group on a node
  failure (ADR 0017 C), a control→data "reconfigure now" message would have forced
  `animus-control` to depend on `animus-cp-data` (a cycle — data already depends on
  control for `RaftCore`) and to track each group's leader. Instead the decision
  already lives in replicated `Metadata` (the placement reconciler's epoch-CAS), so
  each group **leader pulls** its tablet's desired voter set and reconfigures
  *itself* (`reconfigure_step` + `spawn_reconfigure_loop`) — no reverse dependency,
  no leader-reporting needed for the trigger, and the data side takes the metadata
  source as a **closure** (`Fn() -> Option<BTreeSet<NodeId>>`) so the crate stays
  decoupled from the control-plane driver type. Mirrors the proven `reconcile_loop`
  split: decision pure + elsewhere, timing in the loop. Reconfigure toward a target
  **one single-server step per tick** (the `change_membership` contract), letting a
  multi-server move converge over successive ticks rather than failing.
- **Extend the `Env` seam with a *sub-trait* bound only where used, not by widening the supertrait — capabilities not every env has stay opt-in.** (Found building `Coresident`/`sibling()`, since superseded by ADR 0026 Stage B / ADR 0028; the pattern itself remains the right one for a future capability. Archived in `docs/engineering-lessons-archive.md`.)
- **To store a generic type behind one registry/handle field, fix the concrete
  type parameter when the variation isn't needed at the call site — don't reach for
  a trait object.** Routing a CP-mode table to a hosted `RaftKvNode<E, S>` (ADR 0017
  #3a) needed the `animusd` edge state to hold the group handle and call
  `put`/`linearizable_get`/`is_leader` on it. `RaftKvNode` is generic over its
  engine `S`, so a `Vec<RaftKvNode<ProdEnv, _>>` field would need an
  `async_trait` object (the methods are async) — extra machinery for variation that
  doesn't exist here: the CP plane is *always* durable, so `S = LsmEngine<ProdEnv>`
  is the only instantiation. Fixing it (a `type CpGroup = RaftKvNode<ProdEnv,
  LsmEngine<ProdEnv>>` alias — also silences `clippy::type_complexity`) kept the
  edge registry a plain `Vec<CpGroup>`, no trait object, no async-trait dep. The
  AP data replica *is* type-erased (`Box<dyn Any>`) because its backend genuinely
  varies (LSM vs Memory); the CP group's does not.
- **Adding an Nth internal role to a fixed-stride multi-role node is a wide but
  mechanical ripple — change the stride, every literal, and the arity together.**
  `animusd` packs each node's roles into consecutive ports (`base + stride*i`); the
  CP `raftkv` role bumped the stride 6→7 and touched every `RoleAddrs` literal
  (config gen + 5 test sites), `peer_book`, `Node::bind`'s arity, the `[ProdEnv; N]`
  shutdown array, and the conventional id base (`300+i`). A `#[serde(default)]` on
  the new addr field keeps *older configs* loading, but struct **literals** still
  need the field — so the compiler walks you through the sites; expect it and do
  them in one pass.
- **Generalizing a type over a state machine: prefer *two plain type params*
  (`<C, S>`) over *one param with an associated type* (`<SM: Trait<Command=C>>`) —
  `#[derive]` can't see through associated types.** Making `RaftCore` generic over
  its command + state machine (ADR 0016 step 2), a one-param `RaftCore<SM>` would
  force manual `Clone`/`Debug`/`Serialize` impls on every container holding
  `SM::Command` (the derive generates `impl<SM: Clone>`, which does *not* imply
  `SM::Command: Clone`). Two plain params (`C = MetaCommand`, `S = Metadata`) let
  every derive Just Work, and **defaulted** params keep all existing references
  source- and serialization-compatible (the generic is erased in JSON, so the WAL
  bytes are unchanged). One residual gotcha: `#[derive(Default)]` still adds a
  spurious `C: Default` bound — hand-write `Default` where a field is
  `Vec<_>`/`Option<_>` (needs no inner `Default`). And a no-arg constructor like
  `RaftCore::new()` needs a type annotation at call sites that don't otherwise pin
  the params (bare `let x: RaftCore = …` or `Vec<WalRecord>` on a `decode`).
- **No process-global mutable state (`OnceLock`/`static`) for per-instance
  concerns.** It leaks across tests in one binary (multiple in-process clusters
  share it) and conflates instances in any multi-tenant context. Thread state
  through a per-instance context instead (the wire edges' `ClusterEdgeState` via
  `ClientCtx`, not process statics). If you must keep a static, make sure tests
  tear instances down (`Node::shutdown()`) and use unique names/keys per test.
- **Never hold a `std::sync::Mutex` guard across an `.await`** in `<E: Env>`
  code — it breaks `Send` (often a *compile* error via `spawn_task`'s bound) and
  risks nondeterminism. Take the lock, mutate, drop it; do I/O lock-free.
- **`serde_json` cannot serialize a map keyed by a struct** — it fails at
  *runtime*, not compile time (`expect("...serializes")` panics). A
  `BTreeMap<Timestamp, _>` (or any non-string/non-integer key) in a `Serialize`
  type must ride as a `Vec<(K, V)>` instead. Bit when adding a WAL `Snapshot`
  record carrying `BTreeMap<TxnId, _>` (animus-consensus); integer-keyed maps
  (`BTreeMap<u64, _>`) are fine (stringified), struct-keyed are not.
- **Tightening a quorum/threshold can *expose* a latent ordering bug elsewhere —
  re-derive the safe bound from first principles and check the whole pipeline.**
  Making Accord's fast quorum precise (`N-1`, down from `ceil(3N/4)`) let two
  *conflicting* txns legitimately commit at the same `logical` timestamp (ordered
  by the node tiebreak); the downstream MVCC `version` was `logical` alone, so
  per-key LWW kept the wrong (first-applied) winner. Encode the *full* order
  (`(logical<<16)|node`) wherever a total order is collapsed to one `u64`. Also:
  pair a quorum bound with its *recovery* procedure — the smaller "optimized"
  Accord/EPaxos fast quorum needs the full witness-recovery; the simplified
  slow-path recovery requires the larger `N-1` bound.
- **Replicate the *definition*, keep the *bulk data* at the edge — and split them
  cleanly.** When promoting per-process state to the control plane (ADR 0013), move
  only the small, must-agree *shape* (e.g. a secondary-index definition: name/keys/
  projection) into replicated `Metadata`; leave the large derived *data* (the index
  entries) edge-local, rebuilt from observed writes. Make the edge reconcile its
  in-memory machinery *from* the replicated definitions (a `sync_indexes`-style
  method that preserves entries on an unchanged shape, clears on a changed one) so a
  restart recovers the shape from Raft, not local memory. Additive `MetaCommand`
  variants + a `#[serde(default)]` new field keep older snapshots/consumers working.
  (Found replicating DynamoDB GSI/LSI definitions; `animus-control` `schema.rs`.)
- **Count a metric at the site that knows the *real* outcome, not the attempt.**
  A counter recorded where an op is *requested* over-counts when a downstream
  helper silently no-ops (e.g. `HintStore::record` drops a hint on a residency or
  LWW-supersede miss). Have the helper *return* whether it acted and count on that
  (`data_hints_stored` counts only hints actually stored). This keeps the closed
  `Metric` enum (ADR 0015) append-only/byte-reproducible and the seam observe-only
  — instrumenting must never change the path it measures. **When a metric is the
  *delta* of a pre-existing monotonic counter** (e.g. recording WAL rotations from
  `GroupCommit::rotation_count` around each `commit`, or block reads off a shared
  introspection `AtomicU64`), it is easy to wire the source increment yet forget the
  `metrics.incr_by(delta)` — the source counter moves but the metric stays 0. A
  "counter moved under a known workload" sim test catches exactly this (it did:
  `storage_wal_segment_rotations` read 0 while the WAL had rotated 349 times).
- **A per-instance observability seam (e.g. ADR 0015 metrics) has *one sink per
  `Env`/role*, so the integration layer must aggregate, not pick one.** A node
  runs several `ProdEnv` roles on distinct ids (control/data/coord), each with its
  **own** `metrics()` sink — `RaftNode::start` records into the *control* env's,
  the replica/coordinator into theirs. A `/metrics` handler that read only one
  (e.g. `node.raft.metrics()`) would silently drop the others' counters. Capture
  every role's handle and sum the snapshots **at request time** (live, not cached);
  capture the soon-to-be-moved handles before the envs are consumed. (`animusd`
  `ClientCtx::metrics_text`.)
- **An admin/introspection surface is a pure *observer* over per-instance handles,
  aggregated live — and per-instance state makes it meaningful *per node*, not
  cluster-wide.** The admin interface (ADR 0020) only *reads* node state (Raft
  accessors, promoted `LsmEngine` introspection — kept snapshot-shaped so it can't
  perturb the measured path, like the metrics seam) or drives an explicit gated
  action; it never changes the path it inspects. Two consequences bit during the
  build: (1) **metrics/Raft counters are per-node sinks** — a *follower's*
  leader-only counters (`elections_won`, `append_entries_sent`) are legitimately 0,
  so an admin/metrics endpoint is sound only *per node* (scrape the leader for
  leader-only state; the test asserts election counters only on the control leader).
  (2) **the in-process `--cluster` shared `ClusterEdgeState` lists *every* node's CP
  group handle**, so one node's `/admin/raftkv` shows all replicas, while a
  one-process-per-node deployment (separate edge each) is node-local — match the
  test's bring-up (`run_node` per node) to the semantics you assert. Reuse the
  documented port-TOCTOU retry for any `free_addrs`-style `ProdEnv` bring-up.
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
- **A new orthogonal capability often *composes* existing single-instance pieces —
  don't reshape the proven core to add it.** Per-shard Accord consensus (one group
  per tablet) landed as a thin driver layer (`ShardedOwner` hosting one untouched
  `AccordNode` per local shard, routed by a `ShardRouter` *derived from the existing
  tablet map* — no new control-plane state), leaving the sync `AccordCore`
  byte-for-byte unchanged and the whole prior suite green. Look for the
  by-composition path before editing a load-bearing state machine; and **a node
  hosting several protocol instances needs one `Env`/inbox/WAL *per instance*** (the
  inbox is single-consumer) — allocate a distinct id per (node, instance) and let
  the caller own that allocation policy. (`animus-consensus` `shard.rs`.)
- **A *per-node* decision must dedup on *per-node* state, never on shared registry state — a registry that doesn't distinguish callers by node silently answers "does anyone in the cluster satisfy this," not "do I."** (Found in the pre-ADR-0031 `cp_join_host_loop`/`minted`/shared `ClusterEdgeState`; both halves are superseded by ADR 0031 PR2+PR4. Archived in `docs/engineering-lessons-archive.md`.)
- **A Raft group *forming or re-forming* (no live leader) needs the full voter config;
  only a *new spare joining a led group* starts as a non-voter — and the restart
  signal is on-disk data, not the epoch.** WAL recovery does **not** restore voter
  status from a non-voter `all_nodes` start, so a node re-hosting a tablet it already
  has data for must pass the **full** config explicitly. Gating on epoch misfired: a
  split bumps the original replicas' epoch, so a post-restart re-host of a split
  parent looked like a "join" → non-voter → no election. Use `latest_version() > 0`
  (engine has data ⟹ re-forming) as the signal. (ADR 0023, originally `animusd`
  `cp_join_host`; since ADR 0031 PR4 the decision lives on unchanged as
  `TabletFacts::has_data` in `animus_cp_data::host` — gathered by
  `Reconciler::gather_facts` via `StorageScope::has_data`, the shared-engine
  successor to `latest_version()`.)
- **With provisioning in band (a tablet's group forms on first access, not at
  startup), a node that *is* a replica of a not-yet-hosted tablet must WAIT, not
  forward.** Routing's "I host no replica → forward to any route" fallback misfires
  during the formation window when a replica-to-be hasn't stood its group up yet — it
  forwards to a node that doesn't host the leader → "forwarded CP op: not the leader
  here". Gate the forward on "this node is **not** in the tablet's replica set"; a
  replica waits for its own election. And **don't paper over formation latency with a
  synchronous serve-wait on the provisioning path** — it made the first write block on
  full formation (regressing a restart test); `cp_route` already waits, so provisioning
  returns once the tablet is in `Metadata`. (ADR 0023, `animusd` `resolve_cp_route`.)
- **An id-translation seam must be applied in *both* directions — the identity case masks the missing one.** (Found in `cp_member_id`/`cp_base_id`, since removed by ADR 0026 Stage B / ADR 0028; archived in `docs/engineering-lessons-archive.md`.)
- **When the key format changes (e.g. ADR 0022's token prefix), sweep *every*
  key-building write path, not just the wire edges — a path that bypasses the shared
  layout partitions a different keyspace.** The admin bulk-seed endpoint kept writing
  raw `prefix+index` bytes via `cp_write` after the DynamoDB/CQL builders gained the
  Murmur3 token prefix, so seeded tables split at raw-key medians (readable ranges in
  the dashboard — the visible symptom), sequential seeds piled into one tablet's tail
  (the exact skew the token removes), and a mixed seed+edge table would interleave two
  key layouts in one engine. `cp_write`/`cp_read` take the key verbatim, so nothing
  below the edge catches this — grep for `cp_write` callers and check each builds the
  ADR 0022 layout. (`animusd` `admin.rs::seed_key`.)
- **The token prefix is a *wire-edge/seeder* convention, not a storage invariant —
  a transform that renders/parses a stored key must detect the layout by content,
  not assume it.** The ADR 0022 `token || escape(pk) || rk` layout is built *above*
  `cp_write`; the DynamoDB/CQL edges and the bulk seeder add the token, but the
  plain-client `Put` stores its key **verbatim** (`cp_put_local`, un-prefixed). So a
  dashboard key view that hex-formats "the first `TOKEN_BYTES` bytes as the token"
  mangles a plain key (`admin-key` → `61646d696e2d6b65:y`). Gate the split on the
  leading run actually being **non-printable** (a Murmur3 token almost always has a
  non-printable byte; a printable key is shown as text) so both key populations
  render correctly. Same "keys aren't uniform below the edge" root as the seed-key
  entry above. (`animusd` `admin.rs::key_display`/`parse_key_display`; the
  `admin_endpoint` test writes a *plain-client* `admin-key` — it caught exactly this.)
- **A restarted Raft replica re-applies its recovered log from the start, so any
  consumer keyed on replicated state passes through *historical* states — a loop
  acting on *absence* (a GC/teardown) must be convergent, and its post-restart
  assertions must poll.** The drop-table GC (ADR 0024) keys on "tablet no longer in
  the map"; during post-restart replay the map transiently *contains* the dropped
  tablet again, so the join-host loop briefly re-hosts an empty zombie group — then
  replay reaches the drop and the GC reclaims it. That round-trip is correct
  (convergent, ids never reused), but a test that one-shot-asserts "files still
  gone" after a fixed post-restart sleep flakes bimodally: it catches the zombie
  mid-flight. Wait for replay to complete (`last_applied == commit_index` ≥ the
  full log via `/admin/raft`), then poll to the converged state — the restart
  instance of the standing "eventual properties get a converged-or-timeout poll"
  rule. (`animusd` `tests/drop_table_gc.rs`.)
- **A new variant in a replicated command enum must be added to every *gating*
  match, not just `apply` — a missed relay allowlist is a bimodal per-process
  flake.** `animusd`'s cross-process proposal path gates on `is_relayable_command`;
  a `MetaCommand` variant missing there **works whenever the connected node happens
  to be the control leader** (proposed locally) and silently times out ("did not
  commit") when it must relay to another node's leader. The compiler can't catch a
  `matches!` allowlist, and single-node tests never exercise the relay. When adding
  a variant, grep the enum's name for gating `matches!`/match sites (allowlists,
  admin filters) and update them in the same change; regression-test the new
  command through a **follower-connected** node in a per-process cluster.
  (`DropTableTablets`; caught by `drop_table_gc.rs`'s 3-node test going bimodal.)
- **Adding a Raft *pre-vote* step changes what a single hand-driven `tick`
  produces — update the election-driving tests, but single-node `tick` stays
  a leader.** Pre-vote (ADR 0009) makes an election-timeout `tick` yield a
  `PreCandidate` + `PreVote` (no term bump), *not* a `Candidate` + `RequestVote`;
  every test that hand-drives a *multi-node* election (`tick` then feed
  `RequestVoteResp`) must now also feed a `PreVoteResp` grant to reach the pre-vote
  quorum first — but a *single-node* group still elects on one `tick` (self is a
  pre-vote majority, which short-circuits straight to the real election), so those
  tests are unchanged. The correctness invariant a pre-vote must hold: it **never**
  mutates a node's term/vote/role (both `PreVote` and `PreVoteResp` bypass the
  step-down-on-higher-term rule) — the sole exception is a *rejecting* `PreVoteResp`
  carrying a higher real term, which reverts a stale pre-candidate to a follower at
  that term. Assert this directly (`pre_vote.rs`: a live-leader lease rejects and
  the term is untouched); the multi-node `SimEnv` teeth is that an *isolated*
  follower's repeated pre-vote rounds leave the stable leader's term unchanged
  (without pre-vote it would ratchet the term every timeout and disrupt on heal).
- **A two-step operation where step 1 is a cheap, always-visible write and step 2 is the expensive, failure-prone "make it real" step must never let a background loop discard a step-2 failure — that silently strands step 1's effect forever.** (Found in the pre-ADR-0028 two-phase `auto_split_loop`; superseded by ADR 0028 — split has no step 2 anymore. Archived in `docs/engineering-lessons-archive.md`.)
- **An `opentelemetry-otlp` exporter's `.with_endpoint(url)` takes `url` as the
  exact, final request URL — it does *not* append the OTLP signal path
  (`/v1/traces`) the way the SDK's own env-var resolution does for the generic
  `OTEL_EXPORTER_OTLP_ENDPOINT`.** Reading that env var by hand and forwarding it
  straight into `.with_endpoint(..)` (ADR 0027's `animusd::otel` seam) silently
  posted every span export to the endpoint's bare root (`POST /`) instead of
  `POST /v1/traces` — a real collector would 404 this with zero indication it was
  a config bug, not a network one, since the exporter reports one generic
  `HttpClient.NetworkError` regardless of cause. Either let the builder resolve
  the endpoint itself (don't call `.with_endpoint(..)` at all — it then reads
  `OTEL_EXPORTER_OTLP_ENDPOINT`/`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and appends
  the signal path correctly), or reproduce the append by hand if the endpoint must
  be threaded explicitly for testability (`animus_db` did the latter, so a test
  seam could pass an arbitrary receiver address without `unsafe`-mutating process
  env). Caught by decoding the exporter's actual protobuf payload in
  `animusd/tests/otel_tracing.rs`, not by the exporter reporting success.
- **`SdkTracerProvider::force_flush`/`shutdown` block the calling OS thread until
  the exporter's HTTP call completes — call them via `spawn_blocking`, never
  directly inside an async fn on a `#[tokio::test]`'s default current-thread
  runtime.** The default runtime has exactly one worker thread; blocking it
  synchronously starves every other task scheduled on it, including a test's own
  in-process receiver task waiting to `accept()`/`read()` the very HTTP request
  the flush is trying to send — a same-process instance of the "don't hold a lock
  across `.await`" deadlock family, just with a blocking call standing in for the
  lock. The symptom is a flush that hangs for its full timeout and then reports a
  generic network error, which reads exactly like a broken exporter rather than a
  starved runtime. (`animusd/tests/otel_tracing.rs`.)
- **A tracing seam wired at one client-facing entry point doesn't cover every
  caller of the primitives underneath it — an internal path that "emulates a
  client" by calling the same primitives directly, bypassing the wrapped entry
  point, needs its own span or context-propagating calls inside it become
  silent no-ops.** `handle_client` wraps every accepted request in a
  `client_request` span (ADR 0027), so `cp_forward`'s
  `otel::current_traceparent()` has an active span to inject when a *client*
  write forwards to another node. The admin bulk seeder
  (`admin.rs::action_data_seed`) calls `ctx.cp_batch_write` directly — never
  through `handle_client` — so it wrote real data with zero spans exported no
  matter how much it wrote, a gap invisible from the code (no error, no
  warning — `current_traceparent()` just returned `None`, its documented
  no-op-when-there's-nothing-to-propagate behavior, indistinguishable from
  "export is disabled"). Fixed by giving it its own `admin_seed` root span
  (mirroring `client_request`'s granularity) with per-chunk `admin_seed_batch`
  children. The general check: when adding a new internal caller of a
  primitive whose forwarding path reads ambient span context, ask "does this
  caller sit under a span at all" — not just "does the primitive still work."
- **Run `cargo test --workspace` after *each* merge, not just at the end of a
  batch.** Batching the gate run let a regression onto main via an earlier
  merge before it was caught. All five gates (fmt, clippy `--all-features
  -D warnings`, build, test, `cargo deny`) green per merge.
- **`cargo deny` can be silently broken** (e.g. the repo's own `AGPL-3.0-only`
  missing from the allow-list) and it can't run in every local env — CI runs it;
  treat it as a real gate, not optional.
- **When a CI check fails on your PR, first check whether it also failed on
  already-merged PRs — a check that has *never* passed is misconfigured, not a
  verdict on your change.** The DCO check failed identically on every PR
  (including merged ones) because the workflow invoked `tim-actions/dco` without
  its *required* `commits` input (instant `Unexpected end of JSON input`), and —
  second layer, only visible after the first fix — the repo's restricted default
  token lacked `pull-requests: read` for the commit-listing step (`Resource not
  accessible by integration`). Two generalizable checks: (1) an action's
  README/`action.yml` lists required inputs — a bare `uses:` of an action that
  needs inputs fails on every event; (2) a workflow calling the REST API needs
  its permissions declared explicitly under restricted default-token settings.
  Verify a workflow fix by letting the PR that changes it exercise itself
  (`pull_request` workflows run from the PR's merge ref). (PR #83.)
- **Don't `git add -A` while resolving a merge** — it can sweep agent worktree
  dirs in as embedded git repos. Stage explicit paths; `.claude/worktrees/` is
  gitignored to prevent it.
- **Doc files (`CLAUDE.md`, ADRs) conflict predictably** when parallel changes
  each edit the "what remains" lists — resolve by *unioning the done-states*
  (each side is usually stale only for the *other* change's feature).
- **`ProposeResult::Accepted` means "appended to the leader's local log," never "committed" — every proposer must confirm, and a bare boolean flag isn't always enough to confirm the caller's *specific* request.** (Found in the pre-ADR-0028 `propose_split_data`/`applied_split_key`, since removed; `cp_put_local`'s confirm-by-index is the still-live instance of this lesson. Archived in `docs/engineering-lessons-archive.md`.)
- **A retry loop over a Raft write must distinguish "never accepted, retry is
  free" from "accepted, unconfirmed" before resubmitting — the latter doubles
  outstanding work under exactly the conditions that caused the timeout.**
  Diagnosing `--auto-split 2000` failures that looked like a runaway/election
  storm, a live reproduction (isolated cluster, sustained bulk-seed under
  load) showed every Raft term — control plane and every per-tablet CP group
  — stayed flat the whole time; `commit_index` kept climbing well past
  individual write attempts already reported as failed. So the writes weren't
  stuck, just slower than the 10s client timeout (measured ~12-27ms fsyncs on
  this host vs. sub-ms on real NVMe — a slow/virtualized disk under a growing
  number of independent per-tablet Raft WALs). The admin bulk-seeder's retry
  loop (`action_data_seed`) turned that slowness into a pile-up: on **any**
  `cp_batch_write` error, including a bare confirm-timeout, it resubmitted the
  same entries — but `ProposeResult::Accepted` only means appended to the
  leader's local log, not committed, so a confirm-timeout after `Accepted`
  almost always means "still committing," and resubmitting appends a
  **second, fully duplicate** Raft entry for the same data on top of one that
  was probably going to land anyway — safe by per-key LWW, but it doubles
  fsync/replication load, compounding under the very slowness that caused the
  timeout. Fixed by splitting propose from confirm
  (`cp_batch_propose`/`poll_probe` in `animusd`) so a patient retry
  (`cp_batch_write_patient`) can poll an already-accepted entry a second time
  instead of re-proposing, while still proposing fresh on a genuine routing
  failure (leader moved — e.g. a tablet split mid-seed, where `cp_route`
  re-resolving on each attempt is exactly what's needed). General check for
  any retry loop wrapping a Raft write: does a bare timeout distinguish
  "definitely not accepted anywhere" from "accepted, just slow"? If not, a
  slow/contended commit path gets a retry storm instead of patience.
  **This recurred immediately in a sibling code path** (superseded by ADR
  0028 — `auto_split_loop`'s `pending` map and `propose_split_data`/
  `propose_and_confirm_split`/`cp_split_here` no longer exist; retained for
  historical record) — worth treating as a
  *pattern* to sweep for, not a one-off: `auto_split_loop`'s `pending` map
  (the step-2 `propose_split` retry) has the identical shape, just already
  half-fixed — `confirm_split` was already a poll-only primitive (propose and
  confirm were never fused there the way `cp_batch_local` fused them), but the
  retry loop still called `propose_split_data` (propose **and** confirm)
  fresh on every ~2s tick regardless of whether the prior attempt reached
  `Accepted`. `Split` apply is idempotent (a group splits once; re-application
  is a no-op) so this was never a correctness bug, purely a wasted-work one —
  same fsync/replication doubling, same live-repro signature (flat Raft terms,
  `commit_index` still climbing). Fixed the same way: `propose_and_confirm_split`
  takes a `confirm_rounds` count, and the pending-retry call (plus
  `cp_split_here`, the cross-process counterpart, which can't tell if its
  caller is about to retry) passes 2 instead of 1 — poll the already-accepted
  entry a second time before the *next* tick would otherwise re-propose.
  Lesson beyond the original one: when a retry-amplification bug is found and
  fixed in one place, grep for the same *shape* (propose-then-poll, called
  again from a loop on bare timeout) elsewhere in the same subsystem — it is
  rarely truly a one-off.
- **The "sweep for the pattern" advice above was followed for two known sites
  and still missed the actual common root: the shared helper both of them (and
  most other schema proposals) sit on top of.** `ClientCtx::propose_and_await`
  — the generic "propose a `MetaCommand`, poll `Metadata` for its commit"
  helper backing `propose_split_metadata`, `register_node_addrs` (formerly
  `register_cp_addr`, superseded by ADR 0032 PR1),
  `create_table_schema`, `replace_table_schema`, `drop_table`, and
  `drop_table_schema`'s own hand-rolled copy of the same loop — called
  `propose_schema` unconditionally on **every** `SCHEMA_POLL_INTERVAL` (50ms)
  tick regardless of whether the previous call had already reached a leader's
  log, for up to `SCHEMA_COMMIT_TIMEOUT` (10s) ⇒ up to ~200 duplicate proposals
  per call. `SplitTablet` apply's `new_id`-exists guard makes a duplicate
  harmless (cleanly rejected), so this was pure wasted WAL/replication work —
  but under `--cluster N`'s auto-split loop running on every node concurrently
  (see the sibling cross-node-contention entry), that waste compounds directly
  into the 10-minute-long "split metadata did not commit in time" stalls seen
  live: three nodes' independent retry storms flooding the control-plane log
  fast enough that nothing drains within any single 10s window. Fixed by
  having `propose_schema` report whether it has reason to believe the command
  reached a leader's log (a local `Accepted`, or a relay that didn't visibly
  error) and having `propose_and_await` only resubmit immediately when it
  knows the prior attempt went nowhere, otherwise backing off
  `SCHEMA_PROPOSE_PATIENCE` (1s) before trying again — mirroring
  `propose_and_confirm_split`'s confirm-before-resubmit shape one level up the
  call graph. **Corollary: "sweep for the pattern" means grep the shared
  primitives a retry loop calls into, not just the two sites a bug report
  named** — the pattern's most common instance was hiding one layer below
  where it had already been fixed twice.
- **A "only the owning node acts" gate near a shared registry must check whether the registry actually distinguishes callers by node — or it silently answers "does anyone in the cluster satisfy this," not "do I."** (The sibling cross-node-contention bug referenced above: `auto_split_loop`'s `ctx.edge.cp_leader(tablet)` gate, scoped to the shared `--cluster N` registry rather than per-node. Superseded by ADR 0028, which removed the two-phase split contention this guarded; archived in `docs/engineering-lessons-archive.md`.)
- **An "abandon and forget" exit from a retry loop must still leave the cooldown state a *fresh* attempt would have set — otherwise the resource is eligible again on the very next tick, not after backing off.** (Found in the pre-ADR-0028 `auto_split_loop` abandon path, since removed; archived in `docs/engineering-lessons-archive.md`.)
- **A `spawn_task`'d background disk-I/O task must be gracefully joined via its
  own `is_stopped()`-style contract before `Env`/runtime teardown — an outer
  `AbortHandle::abort()` is not enough, because it races the runtime's own
  blocking-pool teardown and can surface as a raw runtime-internal panic
  instead of a clean stop.** `animusd`'s Ctrl-C path (`shutdown_graceful`)
  flushed the control-plane WAL, then called `Node::shutdown()`, which
  `ProdEnv::shutdown()`-aborts every task the node's two internal envs own —
  including the CP-data apply task (`animus-cp-data`'s `apply_and_compact`).
  `RaftKvNode::shutdown()`/`CpGroup::shutdown()` already document "a graceful
  driver halt, not a kill" (a flag observed *between* full apply passes), and
  the drop-table GC path (`cp_gc_tablet`) already uses the correct
  shutdown-then-poll-`is_stopped()` pattern before touching files — but
  process-level teardown skipped it and went straight to the hard abort. If
  the apply task was mid-`storage.merge(..).await` (a `tokio::fs` op, which
  internally runs on tokio's blocking thread pool), aborting the task while
  its blocking op was still in flight surfaced as a `tokio`-internal panic —
  `Backend("background task failed")` / `Backend("task was cancelled")` — on
  every real `animusd` shutdown, harmless to durability (an un-acked write
  just isn't durable yet) but a noisy, uncontrolled crash instead of a clean
  exit. Fixed by adding `ClusterEdgeState::shutdown_all_cp_groups` (snapshot
  the registered handles out of the lock, call `.shutdown()` on each, then
  poll `.is_stopped()` bounded by `CP_GC_STOP_TIMEOUT` — the exact
  `cp_gc_tablet` shape) and calling it from `shutdown_graceful` before the
  hard-abort `shutdown()`. **General check: when a component documents its own
  graceful-stop contract, make sure every caller that tears it down — not just
  the one call site the contract was originally written for — actually uses
  it**, especially a process-exit path that looks unconditionally safe because
  "the process is exiting anyway." [`cp_gc_tablet` itself is gone as of ADR
  0031 PR4 — its shutdown-then-poll-`is_stopped()` shape now lives in
  `animus_cp_data::host::Reconciler`'s own `Release`/`Reclaim` teardown; the
  lesson and `ClusterEdgeState::shutdown_all_cp_groups` this entry added are
  otherwise unaffected and still current.]
- **A cached per-node handle derived from replicated state needs an explicit re-sync step for every way that state can change in place — "it was correct when constructed" is not "it stays correct."** (Mechanism superseded by ADR 0031 PR4 — the reconciler's planner now emits an explicit `NarrowScope` action instead of a per-tick patch-up. Archived in `docs/engineering-lessons-archive.md`.)
- **A feature whose only enabling *registration* path is shaped by the startup
  config silently caps the cluster at its born size — and a test that "proves"
  growth by starting every node up front only proves the planner, never the
  actual growth path.** ADR 0029's rebalancer worked perfectly in
  `cp_rebalance.rs` (5 nodes started together, `Active` from bootstrap) — but a
  cluster grown *after* bring-up had no path in at all: `bootstrap` computes
  the raftkv ids it registers from `control_ids.len()` at the process's own
  start, so a node added later is never proposed as a member by anyone, ever.
  The tell was in the problem statement, not the code: "the passing grow-test
  starts all 5 nodes up front by its own admission" is a giveaway that the
  test exercises the *decision* (given a balanced-vs-imbalanced membership,
  does the planner converge) but never the *registration* that would put a
  genuinely-new node into that membership in the first place. General check
  when auditing "does X actually support growth/scale-out": find the one
  function that turns "a node exists" into "the system knows about it," and
  ask whether it can only ever run at the size the system started at.
  Delivering online growth (ADR 0030) then surfaced a second-order version of
  the same lesson: hardening a *different* gap (a declared-but-never-booted
  node staying a permanent placement-eligible phantom, since the failure
  detector only judges members it has heard from) by making the detector treat
  an untracked `Active` member as demotable broke several *existing*
  `animus-control` sim tests that had, for years, modeled "Active data members"
  by proposing `UpsertMember` directly with **no heartbeat simulated at all** —
  a fine way to test placement logic in isolation right up until a change
  makes "declared but silent" meaningfully different from "declared and about
  to heartbeat." A change to shared detection/liveness semantics needs its
  blast radius checked against every test that manages membership *without*
  wiring up the corresponding liveness mechanism, not just the tests for the
  feature being changed. (`animusd::bootstrap`,
  `animus-control::node::detect_loop`; `animusd/tests/cluster_growth.rs`;
  `animus-control/tests/{placement_auto_reconcile,placement_rebalance,
  placement_reconcile,prod_liveness}.rs`.)
- **A teardown that erases "my own scope" must re-derive the scope from replicated state at the point of irreversible action — not trust an in-memory cache that a *different* code path is responsible for keeping current.** (Mechanism superseded by ADR 0031 PR4 — `HostAction::Release` now carries the erase bound directly, computed by the one planner. Archived in `docs/engineering-lessons-archive.md`.)
- **A safety mechanism that exists and is unit-tested but has zero production
  callers is dead code with a green suite — second instance of the
  `narrow_scope` pattern above, on the *write* side this time.** ADR 0028's
  crossover-window write fences (`RaftKvNode::put_fenced`/`delete_fenced`/
  `put_batch_fenced`, a `fence: KeyRange` embedded in the proposed command
  and checked at apply time) landed additively with a thorough sim suite
  (`animus-cp-data/tests/fenced_commands.rs`) — but `grep -rn "_fenced"
  crates/animusd/src` found **zero** callers: `cp_put_local`/
  `cp_delete_local`/`cp_batch_propose` (reached by every client write,
  including every `cp_serve_forwarded` counterpart) all called the
  *unfenced* `put`/`delete`/`put_batch`, which stamp `fence =
  KeyRange::whole()` — so the apply-time check was a permanent no-op in the
  one place it needed to matter. The trigger: a node whose `Metadata` view
  hasn't yet observed a `SplitTablet` commit still resolves a child-range
  key to the parent's (now too-wide) group via `cp_route`'s `Local` branch
  (no re-resolution once routed); the unfenced write then applies onto the
  shared engine's physical key the child now logically owns, shadowing or
  corrupting it via LWW — invisible to every existing test because nothing
  drove a write into that specific crossover window. Fixed by adding an
  additive `RaftKvNode::scope_range()` accessor (a `StorageScope::range()`
  getter underneath) and stamping it as the fence on every real proposal.
  **The sharper lesson is the second half of the fix, not the wiring
  itself:** the fence alone is not sufficient, because `cp_put_local`/
  `cp_delete_local` confirm success by polling for the proposed value (or
  its absence) to read back from **local** storage — and a fenced-out entry
  still commits and applies as a deterministic no-op, silently advancing
  any coarser "did this commit" signal (e.g. `engine_applied_index()`
  alone) right along with it. Had the confirm loop been keyed on such a
  signal instead of exact value equality, wiring the fence alone would have
  turned "silently corrupts the child" into "silently falsely-acks a write
  that never happened" — a *different* silent-failure mode, not a fix. The
  actual fix pairs the fence with a **pre-propose range check**: reject
  before ever proposing if a key falls outside the group's current
  `scope_range()`, returning the same error shape a routing failure already
  produces so the caller's retry re-resolves `cp_route`; the embedded fence
  then only has to cover the much smaller residual race between that check
  and the entry's actual apply. **General checks this generalizes to:** (1)
  when auditing a safety mechanism for "is it wired in," also ask "does the
  *confirmation* path downstream of it use a signal precise enough that a
  mechanism turning a write into a no-op is distinguishable from the write
  actually succeeding" — a coarse confirm signal can convert a newly-fixed
  correctness bug into a new, differently-shaped one; (2) a regression test
  for this class of bug needs access to the private routing internals (here,
  a specific tablet's `CpGroup` handle) to *force* the stale-routing shape
  deterministically, since the real race is not reliably reproducible over
  wall-clock timing — when the integration crate under `tests/` can't reach
  what's needed (its types are only `pub(crate)`/private), an **in-crate**
  `#[cfg(test)] mod` (a child module of the module holding the private
  items, hence able to see them) is the right tool, not a workaround.
  (`animus-cp-data::RaftKvNode::scope_range`; `animusd`
  `cp_put_local`/`cp_delete_local`/`cp_batch_propose`,
  `split_fence_tests::stale_routed_write_for_a_split_childs_key_is_rejected_not_lost`.)
- **A change-notification primitive built on a monotonic watermark re-checked
  fresh on every poll, instead of a one-shot consumed flag, eliminates the
  wake-before-park race class by construction — no special-case handling
  needed.** `animus-cp-data`'s `ProposeSignal` (wake-on-propose) is a flag: it
  registers the waker, checks-and-swaps an `AtomicBool`, and — like any
  consumed-flag design — depends on the register-before-check ordering to
  avoid losing a wake that lands between "check" and "park." Building
  `animus-control::RaftNode::metadata_watch()` (ADR 0031 §trigger, a *caller*-
  facing "has the applied index moved past what I last saw" notification
  rather than a single internal consumer's wake), the natural shape is instead
  an `AtomicU64` watermark: `changed(last_seen)`'s `poll` just checks
  `current > last_seen` — true state, not a consumed edge — so a change that
  already happened before the future was ever created or polled resolves
  immediately on the very first poll, with no dependence on registration
  timing at all. The register-before-check discipline is still followed (for
  the case where the change happens *after* the first poll, before a
  subsequent one), but the *design* no longer has a race to reason about for
  the "already happened" case — it isn't consuming evidence that could be
  consumed by nobody. General rule when building a wake primitive: if the
  "did the awaited thing happen" question can be phrased as a comparison
  against a monotonically increasing counter/index/version (not just "did an
  edge fire"), prefer that framing — it is strictly more robust than a
  one-shot flag and costs nothing extra (a `fetch_max` instead of a `swap`).
  Keep the flag-consuming shape only when the event genuinely has no ordered
  value to compare against (a bare "something happened, go do your own
  re-check" nudge, which is what `ProposeSignal` actually needs — the
  consensus loop doesn't care *how many* proposals queued, only that it should
  wake up and drain). (`animus-control::node::MetadataWatch`.)
- **When extracting a pure planner over a retry-until-success async teardown
  loop, the planner must NOT eagerly mutate its own successor state to reflect
  "the action I just emitted will succeed" — because the real execution is
  async and can fail/time out, and the planner has no way to know.** Porting
  `animusd`'s `cp_gc_tablet`-driven reclaim/release teardown into
  `animus-cp-data::host::plan` (ADR 0031 PR3), the real code only removes a
  tablet from its `minted` claim set *after* shutdown + erase + WAL deletion
  all actually succeed (a timeout re-registers the handle and leaves `minted`
  untouched, so the next tick retries the whole teardown). A naive pure
  `plan(state) -> (actions, next_state)` that removes the tablet from
  `LocalState::hosted` the moment it emits a `Reclaim`/`Release` action would
  silently break that retry contract the instant the caller wires it in: a
  timed-out teardown's tablet would vanish from `hosted` in the *returned*
  state regardless, and if the caller trusts that as ground truth for its next
  `plan` call, the tablet is never revisited again — a permanent leak with no
  error, indistinguishable from a successful teardown from the planner's own
  point of view. Fixed by leaving `hosted` untouched for `Reclaim`/`Release`
  and giving the caller an explicit `LocalState::confirm_torn_down` to call
  **only** once its own async teardown has actually completed — so an
  un-confirmed action is simply re-planned identically on the next call,
  mirroring the real loop's tick-based retry exactly. General check when
  extracting a pure "decide what to do" function out of a loop that also does
  fallible I/O for the same resource: does the loop's *bookkeeping* removal
  happen at decision time or at confirmed-completion time in the original code
  — if the latter, the pure function's successor state must preserve that
  asymmetry (add eagerly is fine when the action can't practically fail;
  remove must wait for a caller-reported confirmation). Verified with a
  dedicated unit test that drives `plan` twice without confirming and asserts
  the identical action re-appears, then confirms and asserts it stops.
  (`animus-cp-data::host::{LocalState::confirm_torn_down, plan}`;
  `host::tests::a_pending_reclaim_is_replanned_until_confirmed_torn_down`.)
- **When replacing N polling loops with one event-driven watch, inventory the
  consumers whose watch source structurally never fires before deleting the
  polls — the periodic fallback arm is load-bearing for them, not a safety
  net; and any guard that gates the new unified loop must be keyed on *every*
  node-type's own signal, or it permanently blocks the type it wasn't written
  for.** Wiring the ADR 0031 PR4 reconciler trigger
  (`select!(metadata_watch.changed(..), sleep(500ms))`), two growth-node (ADR
  0030) hazards were only visible by asking "for which consumer does the
  watch never fire": (1) a growth node's own control raft never advances (a
  permanent non-voter of a group it never replicates), so `metadata_watch`
  never wakes it — only the fallback tick ever drives its reconciler, reading
  the `remote_metadata_sync_loop` mirror via `effective_metadata()`; deleting
  the old fixed-period loops without the fallback would have silently frozen
  every grown node's tablet hosting forever, with zero errors. (2) The
  pre-recovery guard the old GC loop used (`raft.last_applied() == 0` → skip,
  so a default-empty pre-recovery `Metadata` doesn't read as "everything
  dropped") is keyed on exactly the signal a growth node never raises — so
  the unified loop's guard had to become `last_applied() == 0 && remote
  mirror is empty`, or the same guard that protects a normal node's restart
  would have blocked a growth node's reconciler from ever ticking at all.
  Also: after any watch-arm wake, coalesce to the source's freshest value
  (`watch.latest()`) rather than the value the future resolved with — a
  burst of commits under bulk load must collapse into one reconcile tick,
  not one per applied entry. (`animusd::tablet_host_reconciler_loop`,
  `RECONCILE_FALLBACK_INTERVAL`; `tests/cluster_growth.rs` is the regression
  that proves the growth node still functions.)
- **Widening a process-start-immutable field into a live, periodically
  re-synced one: change the field's *type* first, then let the compiler
  enumerate every consumer — don't grep for call sites by hand.** Making
  `ClientCtx.client_route` (a plain `BTreeMap`, filled once at node start)
  live (ADR 0032 PR1, closing ADR 0030's `client_route`-staleness gap) meant
  wrapping it in `Arc<Mutex<_>>` and adding a `route_sync_loop` sibling to the
  already-proven `peer_sync_loop` (same static-seed-∪-replicated-overlay
  shape, same cadence). Every direct `.get()`/`.values()` access to the old
  plain-map field (`cp_forward_target`, `propose_schema`'s relay + broadcast
  fallback, a routing fallback search, the growth-node
  `remote_metadata_sync_loop` seed computation) became a **type error** the
  moment the field's type changed, so the compiler itself produced the exact
  call-site list — a mechanical, self-auditing sweep, unlike the many
  documented gaps in this codebase that are *silent* to the compiler (a
  missing `is_relayable_command`/`cp_serve_forwarded` match arm, a stale
  cached invariant). Route every such access through small
  lock-scoped accessor methods (`route_addr`/`route_snapshot`, cloning out
  under the lock) so no caller can end up holding the guard across an
  `.await`. The *test* fallout from the same change was not compiler-caught,
  though: a test asserting directly on the **superseded** state
  (`cp_member_addrs`, no longer populated by `animusd`'s own startup path
  once `RegisterNodeAddrs` replaced `RegisterCpAddr` as the self-registration
  command) failed at runtime, not compile time — when retiring a producer in
  favor of a superset command that keeps the old command only for WAL
  back-compat, grep tests for direct field/assertion checks on the
  old-producer's output, not just callers of the old propose function.
  (`animus-control::meta::NodeAddrs`/`RegisterNodeAddrs`; `animusd`
  `route_sync_loop`; `tests/cp_plane.rs::cp_member_addresses_register_and_replicate`.)
- **In a multi-refusal admin action that is deliberately local-leader-only,
  check leadership FIRST — every other refusal that reads local `Metadata`
  is only trustworthy once leadership is confirmed, since a follower's
  replica can genuinely lag the leader's own just-committed state.**
  `ClientCtx::admin_remove_member` (ADR 0032 PR3 decommission) originally
  checked "is the member drained" (via `self.raft.metadata()`) before
  checking "am I the leader" (`self.edge.leader_handle()`) — reads that
  happened to agree on the *leader* node (where `self.raft` and the leader
  handle are the same underlying core), but on a **follower** under load a
  just-converged release-GC move can still be in flight over Raft
  replication, so the follower's own stale metadata reported "still
  referenced by 1 tablet" instead of the intended "not the control-plane
  leader; retry on the leader" routing error — the wrong refusal reaching the
  operator, not a wrong *decision* (the follower correctly refused, just for
  a misleading reason). Invisible in an isolated single-test run (no
  contention, replication is near-instant); it flaked exactly once under
  `cargo test --workspace`'s parallel load, the same class of timing hazard
  the "flaky ProdEnv test is a real bug" rule already covers, just showing up
  as a wrong error string rather than a wrong outcome. Fix: check leadership
  before any metadata-dependent refusal, mirroring "resolve the authority
  first, then ask it questions" — the same shape as checking `is_leader()`
  before trusting a quorum-derived fact elsewhere in this codebase.
  (`admin_remove_member`; `tests/decommission.rs`'s follower-refusal
  assertion.)
- **A rebalance-dependent test needs enough independent tablets that the
  pre-growth cluster is *not already balanced* — one table can leave a
  joined/grown node with zero replicas forever, not just an ambiguous
  choice of which table to route through.** `rebalance_step` only proposes a
  move while it improves the *global* `max − min` imbalance; with exactly
  one table (one tablet, RF = the pre-growth node count) every pre-growth
  node already holds exactly one replica and the joined node holds zero —
  `max − min == 1`, already at the stopping condition, so the rebalancer
  never moves anything and a test polling for "the joined node gained a
  replica" times out completely (not flakily — every run). This is a
  sharper version of the already-documented "the rebalancer converges the
  *global* imbalance and makes no per-table promise" lesson (which is about
  which table a test must route through once *some* replica has moved) —
  the additional wrinkle is that with too few tablets, the imbalance can be
  zero from the start and *no* replica ever moves. Fix: seed several
  independent tables (`tests/decommission.rs` uses three, mirroring
  `tests/seed_join.rs`'s `TABLES`), so the pre-growth distribution is
  imbalanced enough to guarantee at least one move onto the new node.

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
