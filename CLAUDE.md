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
| `animus-raftdata` | [crates/animus-raftdata/CLAUDE.md](crates/animus-raftdata/CLAUDE.md) |
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
  (`start_single_node` → `(Node, ClusterConfig)`). Only the *first* bring-up can do
  this — a same-address **restart** must reuse the captured config (it's testing
  same-address recovery), so it keeps a tiny irreducible window; that's acceptable
  because the first bring-up is the dominant contention. (`animusd` tests.)
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

### Code patterns
- **Drive cross-plane reconfiguration by *pull from replicated state*, not a new
  push command — it keeps the dependency edge one-way and the seam testable.**
  Wiring the control plane to reconfigure a per-tablet Raft KV group on a node
  failure (ADR 0017 C), a control→data "reconfigure now" message would have forced
  `animus-control` to depend on `animus-raftdata` (a cycle — data already depends on
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
- **Extend the `Env` seam with a *sub-trait* bound only where used, not by widening
  the supertrait — capabilities not every env has stay opt-in.** In-band tablet
  split (ADR 0017 D) needs a node to mint a second inbox at runtime
  (`sibling(id) -> Self`). Adding that to the `Env` supertrait would force *every*
  env — `ProdEnv` included — to implement runtime inbox-minting (an unsolved
  production-network problem) just to compile. Instead it's a separate
  `Coresident: Env` trait that only the split path bounds on (`impl<E: Coresident,
  S> RaftKvNode<E, S>`), so `SimEnv` implements it, `ProdEnv` doesn't yet, and
  nothing else changes. Same shape as the metrics seam (additive, default-off) but
  via a trait bound rather than a defaulted method, because it returns `Self`. Keep
  the consumer generic over `Env` and inject the capability where needed (here, a
  `SplitHook` closure built with a `Coresident` env), so the driver stays
  `<E: Env>` and existing call paths (`split.rs`, hook = `None`) are byte-identical.
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

### Merge / integration workflow
- **Run `cargo test --workspace` after *each* merge, not just at the end of a
  batch.** Batching the gate run let a regression onto main via an earlier
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
