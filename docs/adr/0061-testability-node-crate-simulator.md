# ADR 0061 — Testability: the `animus-node` split, simulator extensions, and a shared corpus harness

- **Status:** Proposed — plan of record for the testability work; Phases A–E
  below are the delivery order.
- **Date:** 2026-08-28
- **Amends:** [ADR 0003](0003-deterministic-simulation.md) (extends the `Env`
  seam's fault vocabulary; refreshes its stale "known fidelity limits"
  section; finally builds the shrinking its Consequences promised),
  [ADR 0020](0020-admin-interface.md) and
  [ADR 0052](0052-data-console-port.md) (their HTTP edges move behind a
  byte-level seam), [ADR 0026](0026-multiplexed-node-stream-addressing.md) (node-to-node
  relay and the `ControlHandle::Remote` mirror move onto the multiplexed
  `Network` instead of raw `TcpStream`), [ADR 0035](0035-control-plane-separate-deployment.md)
  (the three deployment-shape assemblies split across the new crate
  boundary).
- **Depends on:** [ADR 0003](0003-deterministic-simulation.md) (the `Env`
  seam this generalizes), [ADR 0009](0009-in-house-raft-over-env.md) and
  [ADR 0017](0017-per-tablet-raft-data-plane.md) (already `E: Env`-generic —
  the pattern this applies one layer up).

## Context

The determinism constraint (ADR 0003) is the repo's load-bearing correctness
mechanism, and it works: `animus-control` and `animus-cp-data` are generic
over `E: Env`, and ten fault-injecting corpora (`ANIMUS_*_SEEDS`) explore
them nightly. `animus-test::check.rs` carries a real Adya G1c/G2
serializability checker with negative-control teeth. `animus-sim` already
supports directional partitions, per-node disk faults, torn-tail-on-crash,
deliberate at-rest corruption, per-node clock skew, and a trace log proven
byte-identical across runs of a seed.

The problem is where that machinery *stops*.

**1. `animusd` has no `Env` at all.** `grep -rn "E: Env" crates/animusd/src`
returns zero hits. The crate that assembles the actual node — 44k lines of
source, 18,690 of them in `lib.rs` — monomorphizes `RaftNode<ProdEnv>` and
`RaftKvNode<ProdEnv, LsmEngine<ProdEnv>>` at the top of the stack
(`lib.rs:538-541`, `control_handle.rs:75`) and everything above that line is
real sockets, `tokio::spawn` (50+ sites in `lib.rs` alone), and
`tokio::time`. The mechanisms it wraps are sim-proven one layer down; the
*assembly, routing, wire, and background-loop* layer it adds on top is
proven only by wall-clock re-run.

That layer is not thin. `impl ClientCtx` (`lib.rs:7437–13006`, 5,569 lines)
is the node's brain: route selection, the read path, the write path, 2PC
coordination, forwarding/relay, schema propose. Pure decision logic
(`confirm_wait_is_futile`, `read_should_retry`, hinted-retry target
selection) is interleaved line-by-line with network calls, so none of it can
be exercised without a socket.

**2. The cost is already visible in CI.** All 100 files in
`crates/animusd/tests/` use `#[tokio::test]` over real `ProdEnv`; none
instantiate `SimEnv`. Ten of them carry doc comments explaining why they
*can't* (e.g. `advertise_host.rs:9-10`, `cp_rebalance.rs:8-11`). They
contain 1,583 real-time `sleep`/`timeout`/`Duration::from` calls. CI runs
them in a dedicated `prod-liveness` job pinned to `--test-threads=1` with a
built-in 2-attempt retry, commented as "a stopgap for runner-class
starvation, not a license to ignore failures" (`.github/workflows/ci.yml:150-165`).
Workspace-wide: 434 `#[tokio::test(multi_thread)]` functions against 248
`Simulator::new` sites.

The doctrine that `animusd`'s tests prove *wiring* while the protocols are
proven under `SimEnv` elsewhere is sound. But it leaves the wiring itself —
routing, forwarding, retry, hinted-retry, join/growth sequencing, the
auto-split trigger, the GC reclaim loop, backup/PITR driver ticking — with
no deterministic coverage *anywhere*, only slow real-time re-proof. Those
are ordering-sensitive distributed behaviours, exactly the class ADR 0003
exists for.

**3. The simulator has capability gaps.** Network fault config is a single
global `NetConfig` (`lib.rs:453-455`) — no per-link or even per-node
override, though disk already has one (`set_disk_config_for`, lib.rs:467).
Absent entirely: message duplication; wire-payload corruption; delay
distributions beyond uniform jitter; a process-pause primitive (alive but
frozen — GC pause, cgroup throttle, VM stall); clock *drift* as opposed to
static skew; an ENOSPC-distinguishable `ErrorKind`; and the
fsync-acked-but-lost fault. And there is **no shrinking or minimization
facility at all** — ADR 0003's Consequences promised "shrinking a failure to
a minimal seed becomes possible" and it was never built, so triage of a
failing corpus seed is manual re-running.

**4. Enforcement has a hole.** Only the `HashMap`/`HashSet` half of the
determinism rule is lint-enforced (`clippy.toml` `disallowed-types`).
Nothing blocks `tokio::spawn`, `std::time::Instant::now()`, or
`thread_rng`/`OsRng` from appearing inside `E: Env`-generic logic. The
discipline holds today by review alone.

**5. Some pure logic is untested because it is trapped in async code**, and
some pure logic that *is* free is under-tested. `LsmEngine::next_compaction`
(`animus-storage/src/lsm.rs:1218`) is a pure policy behind a `&self`
lock, exercised only indirectly. `animus-placement`'s `replan` and
`rebalance_step` are already pure and `Env`-free — the positive example —
but have no `proptest` dependency and no randomized-topology test, so the
"converges to max−min ≤ 1" claim in the root `CLAUDE.md` rests on one fixed
scenario. `murmur3_x64_128` (`animus-tablet`) has no canonical reference
vectors. `animus-operator`'s `controller.rs` (436 lines: `reconcile`,
`apply_children`, `drain_and_remove_node` scale-down) has **zero** tests of
any kind, and `animus-cli` (741 lines) has none either.

**6. The corpus harness is copy-pasted.** `name_seed`, `seeds_per_cell`, and
`seed_expand` are independently reimplemented in at least 11 corpus files
across three crates (~400–600 lines of duplication), while the *checkers* in
`check.rs` are properly shared. Every new corpus pays that tax again.

Two facts make this tractable. There is **no `hyper`/`axum`** — every wire
edge is hand-rolled HTTP/1.1 over `tokio::net`, so there is no framework
lock-in between request parsing and transport; the seam is clean. And
`topology.rs` already demonstrates the target shape inside `animusd`: pure
functions, 17 plain `#[test]`s, no bring-up.

## Decision

Push the `Env` seam up one layer, extend the simulator's fault vocabulary
and failure triage, and consolidate the corpus scaffolding — delivered in
five phases in dependency order.

### Decision 1 — the generic core becomes a new crate, `animus-node`

The `E: Env`-generic node logic moves into a **new crate that does not
depend on `tokio::net`, `ProdEnv`, or `std::time` at all**. `animusd`
retains the binary: `main.rs`, config, listener binding, process lifecycle,
signal handling, and the single `ProdEnv` construction site.

Genericizing in place was considered and rejected. The whole failure mode
here is nondeterminism creeping back into logic that ought to be pure, and
in-place genericization leaves nothing to stop it — the same review-only
enforcement that already left the hole in item 4 above. A crate boundary
with no `tokio::net` dependency in its manifest makes the constraint
**compiler-enforced**: a `TcpStream::connect` in node logic is a build
failure, not a review miss. That is the same argument that makes the `Env`
seam work in the first place, applied to the layer that currently escapes
it.

A single core crate is preferred over several smaller ones
(`animus-node-wire`, `-txn`, `-loops`) for now: the dependency graph among
those concerns is not yet understood well enough to freeze into manifests,
and module boundaries inside one crate can be moved cheaply while the carve
is in progress. Splitting further is a follow-up once the seams have
settled.

### Decision 2 — the HTTP edges split at a byte-level seam

The hand-rolled HTTP parsing (`http.rs`, and the request handling in
`dynamo.rs`, `admin.rs`, `console.rs`) moves into `animus-node` as pure
bytes-in/bytes-out functions. `animusd` keeps only the socket accept loop
feeding them. This is what makes the **DynamoDB wire path itself**
sim-drivable end-to-end — today the entire wire surface, including SigV4
(ADR 0057), expression evaluation, and error mapping, can only be tested
through a real socket.

### Decision 3 — the simulator gains the missing fault vocabulary and a shrinker

Per-link and per-node network fault configuration (mirroring the per-node
disk config that already exists), message duplication, wire-payload
corruption, delay distributions, a process-pause primitive, clock drift,
ENOSPC-distinguishable disk errors, and the fsync-acked-but-lost fault.
Plus **failure minimization**: given a failing seed, automatically reduce
the fault schedule and operation count to a minimal reproducing case, driven
by the existing trace log. This is the single highest-leverage item in the
plan — it changes the cost of every future corpus failure from an afternoon
to a minute.

### Decision 4 — determinism becomes lint-enforced, not review-enforced

`clippy.toml` gains `disallowed-methods` entries for `std::time::Instant::now`,
`SystemTime::now`, `tokio::spawn`, `tokio::time::{sleep,timeout}`,
`thread_rng`, and `OsRng`, with narrow, individually-justified `#[allow]`s at
the process-boundary sites in `animusd` and inside `animus-env`'s `ProdEnv`.
Every allow is a documented exception rather than an invisible default.

**As built (rung B5, 2026-08-28) — one refinement found by actually doing
the survey.** `OsRng` (`rand::rngs::OsRng`) is a unit struct, not a
function — `disallowed-methods` can't name it (clippy rejects the config
entry outright); it lives in `disallowed-types` instead, alongside
`HashMap`/`HashSet`, with the same lint-level plumbing. More significantly,
the survey this rung actually ran (grep every crate's `src`/`tests`/
`benches` for the six method patterns, real code only) found the crates
this ADR's `Env` seam actually targets — `animus-control`, `animus-cp-data`,
`animus-storage`, `animus-tablet`, `animus-placement`, `animus-dynamo`,
`animus-sim`, `animus-test` — already **clean in their `src/`**: zero real
call sites. The discipline this decision converts from review-enforced to
lint-enforced had, in fact, already held. What the survey did find:
- **`animus-env/src/prod.rs`** (~32 sites) — exactly the sanctioned
  `ProdEnv` implementation Decision 4 already named; one module-level
  allow (plus two narrower `impl Rng` block-level `disallowed_types`
  allows for the two `OsRng` sites, kept separate from the file-level
  methods allow so an accidental future `HashMap` in this file still
  trips the lint).
- **A handful of real-thread `ProdEnv` liveness tests/benches** in
  `animus-storage` (3 test files + 1 bench), `animus-control` (2 test
  files + one single test function), and `animus-cp-data` (2 test files)
  — every one already carried a module doc explaining, independently of
  this rung, exactly why it must run on real threads/time (the "`SimEnv`
  proves logic, not liveness" class the root `CLAUDE.md` documents). Each
  got one file- or function-level allow citing that existing doc rather
  than restating it.
- **`animus-cli`** (one file, 7 sites) and **`animus-operator`** (one call
  site) — real process-boundary tools outside the `Env` seam entirely (a
  network client CLI; a Kubernetes reconcile loop polling a real pod).
  File-level and call-site allows respectively.
- **`animusd`** (~600 real call sites across 84 files: `src/lib.rs` alone
  has ~174, `dynamo.rs` ~57, `index_drain.rs` ~33, plus ~70 `tests/*.rs`
  files that are, by this crate's own documented design, **all**
  real-socket `ProdEnv` integration tests). This is exactly the case this
  Decision anticipated needing judgment on, and the honest answer is the
  one this Decision's own text undersold: per-site `#[allow]`s here would
  be hundreds of near-identical copies of the same one reason ("this is
  the process boundary Phase C hasn't carved out yet"), which is worse
  than no lint — a wall of allows nobody reads is not review-enforced
  either, just review-enforced with extra steps. `crates/animusd/Cargo.toml`
  gets a package-level `[lints.clippy] disallowed_methods = "allow"`
  override instead (Cargo's `[lints]` table applies to every target in
  the package — lib, bin, every integration test, the bench — so this one
  entry, not 84 file edits, is the actual scope of the exemption),
  documented in that file's own comment and cross-referenced from ADR
  0003. `disallowed_types` (HashMap/HashSet) is untouched by this
  override and stays fully enforced in `animusd` — it has nothing to do
  with the process boundary. This is intentional, tracked debt: it goes
  away when Phase C's `animus-node` extraction gives the newly-carved
  Env-generic core the workspace default back, unmodified.

**Deliberately not lint-enforced**: raw I/O entry points
(`std::fs`/`std::net`/`tokio::fs`/`tokio::net`). Unlike the six methods
above, none has a single, small, always-correct replacement a `reason`
string can name — `animus-env`'s `Disk`/`Network` traits wrap specific
framing/durability contracts, not a drop-in substitute for e.g. a raw
`TcpListener::bind` at a process's one accept-loop boundary — and
`animusd`'s listener binding alone owns dozens of such sites. Enumerating
each would be exactly the wall-of-allows this rung exists to avoid;
reviewed by hand instead (see `clippy.toml`'s own comment).

Full validation: `cargo fmt --all --check`, `cargo clippy --workspace
--all-targets --all-features -- -D warnings`, and `cargo build --workspace`
all green.

## Delivery plan

Each phase is a `gh-stack` series. Phases are ordered by dependency: A
requires nothing and is pure gain; B makes every corpus (existing and
future) more powerful; C is the structural carve that A's extractions have
already de-risked; D is the payoff that C unlocks; E closes the untested
crates.

### Phase A — extract and property-test the pure logic (no structural change)

No moves, no genericization; every rung is additive and independently
mergeable. This both banks immediate coverage and pre-factors the hardest
part of Phase C.

| Rung | Work |
|---|---|
| A1 | `animus-placement`: add `proptest`; randomized-topology property tests for `replan` and for `rebalance_step` convergence (bounded steps, monotonic non-worsening of residency/spread) — currently one fixed scenario backs a general claim |
| A2 | `animus-tablet`: canonical `murmur3_x64_128` reference vectors + a distribution property test for `partition_token` |
| A3 | `animus-storage`: extract `next_compaction_plan(tables, opts) -> Option<CompactionPlan>` out of `LsmEngine::next_compaction` (`lsm.rs:1218`); property-test cascade termination and the trigger floor |
| A4 | `animus-cp-data`: table-driven unit tests for `stale_read_ready` (`lib.rs:4028`) — already pure, currently only reached through full integration tests |
| A5 | `animus-dynamo`: differential proptest for the decimal bignum ops (`condition.rs:368/393/412`) against a reference implementation |
| A6 | `animusd`: extract the pure decision predicates out of `impl ClientCtx` into a `decide` module beside `topology.rs` — `confirm_wait_is_futile`, `read_should_retry`, `frozen_refusal`, `not_leader_refusal`, route resolution, hinted-retry target selection — with direct unit tests and no `&self`/`ProdEnv` |

A6 is the keystone: it is the first cut into the 5,569-line brain, it is
mechanical, and it produces exactly the module that Phase C moves first.

### Phase B — simulator capability and corpus harness

| Rung | Work |
|---|---|
| B1 | Shared `animus-test::corpus` module — `name_seed`, `seeds_from_env`, `seed_expand`; migrate all 11 corpora onto it |
| B2 | Per-node and per-link network fault config, mirroring `set_disk_config_for`'s shape |
| B3 | New faults: message duplication, wire-payload corruption, delay distributions, `pause(node, dur)` process-pause, ENOSPC `ErrorKind`, clock drift rate, fsync-acked-but-lost |
| B4 | **Failure minimization**: shrink a failing seed's fault schedule and op count to a minimal reproducing case, replayable by a printed handle |
| B5 | `clippy.toml` `disallowed-methods` per Decision 4, with justified allows — **done**, see Decision 4's as-built note for what the survey found and where `animusd` was exempted instead of hand-annotated |
| B6 | Refresh ADR 0003's "known fidelity limits" — it undersells what shipped (clock skew, disk faults) and still lists gaps B2/B3 close |

B1 lands first so that everything after it (including every Phase D corpus)
is written against the shared harness rather than adding a twelfth copy.

### Phase C — the `animus-node` carve-out

The long pole. Ordered so each rung compiles and ships green, leaf-first,
brain-last.

| Rung | Work |
|---|---|
| C0 | **Prerequisite (added 2026-08-28, see the amendment below).** Feature-gate `animus-env`'s `prod` module — `ProdEnv`/`FsSegmentStore` behind a default-off `prod` feature; every current consumer opts in explicitly. Without this, C1's boundary is decorative |
| C1 | Create `animus-node` depending on `animus-env` with `default-features = false` — so `ProdEnv` genuinely does not exist in that build — and with no `tokio::net`. Move the wire types (`ClientRequest`/`ClientResponse`/`Surface`/`is_relayable_command`), `topology.rs`, and A6's `decide` module. Also harden `is_relayable_command` from `matches!` to an exhaustive `match` (see below). Boundary established and compiler-enforced from the first commit |
| C2 | Genericize and move the leaf background loops: `ttl_reaper`, `backup_janitor`, `pitr_janitor`, `segment_janitor`, `backup_completion`, `index_backfill` — paced by `env.sleep()`/`env.now()`/`env.spawn_task()` instead of `tokio::time`. **Requires a host-capability trait first (see the second 2026-08-28 amendment): every one of these takes `ClientCtx`, which does not move until C5.** |
| C3 | `ControlHandle<E, R>` and a `RelayClient` capability trait, split into **C3a–C3d** (see the third 2026-08-28 amendment). The literal "move relay onto `Network`" is **rejected**: it would collapse ADR 0047's `intra` port into `internal`, a production wire-topology change this ADR disclaims. The goal — a cluster that talks inside `SimEnv` — is met by a second, sim-only `RelayClient` implementor instead |
| C4 | The HTTP edges per Decision 2, split into **C4a–C4d** (see the fourth 2026-08-28 amendment). `dynamo.rs`'s `run_operation`/DDL handlers are **not** a C4 rung — they fold into C5 |
| C5 | The brain: split `impl ClientCtx` into `read_path`, `write_path`, `txn_coordinator`, `forwarding`, `schema`; genericize over `E: Env`. The heaviest rung, but A6 has already lifted the pure predicates out of it |
| C6 | Node assembly: `Node<E>`/`BoundNode<E>`/`BoundControlNode<E>`/`BoundDataNode<E>` move; `animusd` shrinks to binary, config, listeners, lifecycle, and the one `ProdEnv` construction. The `start_cluster*`/`run_node*` harness functions move to test support where they belong |

#### 2026-08-28 amendment — C0, and why C1 alone would not have worked

Scoping C1 against the code turned up that **C1 as originally written would
not have delivered Decision 1's central claim.** Recorded here rather than
silently corrected, because the claim is this ADR's main argument for
choosing a new crate over genericizing in place.

The wire types themselves are clean: `ClientRequest`/`ClientResponse` and
everything they transitively reference (`KindWriteOp`, `PendingKindWrite`,
`TxnTableWrite`, plus plain-data types from `animus-control`,
`animus-cp-data`, `animus-tablet`, `animus-dynamo`) are ordinary serde data.
None embeds `ProdEnv`, `CpGroup`, `RaftNode`, `RaftKvNode`, `LsmEngine`, or a
tokio type. `topology.rs` and `decide.rs` are pure as claimed. That half of C1
is as easy as this ADR assumed.

The manifest is the problem. `animus-node` needs `animus-env` for `NodeId`.
But `animus-env/Cargo.toml` has **no `[features]` section at all**, declares
`tokio` unconditionally, and `lib.rs` exports `pub mod prod; pub use
prod::{FsSegmentStore, ProdEnv};` with no `cfg` guard. Every crate in the
graph depends on `animus-env` unconditionally, and there is no path to the
plain data types that avoids it. So `animus_node` could write
`animus_env::ProdEnv::new(..)` and it would compile — real sockets, no error.

"No `ProdEnv` in the manifest, therefore compiler-enforced" would then have
been **decorative**: moving the types into a crate that still drags `ProdEnv`
in unguarded is a relabeling, not a boundary. Hence C0 as a prerequisite:
gate `prod` behind a default-off feature so `animus-node` can depend on
`animus-env` with `default-features = false` and the type genuinely is not
in its build.

The rejected alternative was splitting `prod.rs` into a separate
`animus-env-prod` crate. Cleaner in principle, but a much larger diff for the
same guarantee; the feature gate matches the "narrow, individually justified"
spirit of Decision 4 and is mechanical for consumers (the compiler enumerates
every site that needs `features = ["prod"]`).

**One related hardening, folded into C1.** `is_relayable_command` is written
with `matches!`, which — unlike `surface_of` and `request_kind`, both real
`match`es with no wildcard arm — has no exhaustiveness requirement. A new
`MetaCommand` variant therefore silently defaults to "not relayable" with no
compiler signal: exactly the bimodal per-process flake the root `CLAUDE.md`
warns about, currently unguarded. Rewriting it as an exhaustive `match` costs
nothing and C1 already moves that function.

Note also that after C1, `cp_serve_forwarded`'s match still lives in
`animusd` while its input type lives in `animus-node`, so the repo's
"grep every gating site" discipline spans a crate boundary until C5.

#### 2026-08-28 amendment (second) — there is no leaf; C2 needs a capability trait

This ADR's Phase C ordering is described as "leaf-first, brain-last", with C2's
background loops as the easy first movers because each is "small,
self-contained". Scoping C2 found that **premise is wrong**: all six loops take
`ClientCtx` by value or reference, and four also take `&RaftNode<ProdEnv>`.
`ClientCtx` is the 5,569-line brain that C5 moves last. On the ADR's own
ordering, nothing in C2 can move.

Stated plainly because it is the second time Phase C's plan has not survived
contact with the code (the first being C0), and because the naive reactions —
pulling C5 forward, or moving the loops together with the brain — would both
undo the leaf-first property that makes this phase reviewable.

The loops turn out to depend on a **very small slice** of `ClientCtx`:

| Loop | Capabilities used |
|---|---|
| `ttl_reaper` | `effective_metadata()` |
| `index_backfill` | the leader's `metadata()`, `propose()` |
| `backup_completion` | `data_opt()` |

So the fix is dependency inversion, not reordering. C2 gains a prerequisite
step: define a narrow **host-capability trait** in `animus-node` naming just
the operations the loops need, implement it for `animusd`'s `ClientCtx`, and
move each loop generic over `E: Env` plus that trait. `ClientCtx` stays where
it is until C5; the loops stop depending on it as a concrete type.

This is better for the ADR's actual goal than the original plan, not merely a
workaround. A loop generic over a capability trait can be driven in `SimEnv`
against a **fake host** — no cluster, no sockets, no `ClientCtx` at all — which
is precisely the deterministic coverage Phase D wants for the janitor and
reaper arms. Moving the loops while still coupled to a concrete `ClientCtx`
would have produced code inside `animus-node` that still could not be
sim-tested until C5 landed.

Expect the same shape at C3 and C4: the question at each rung is not "can this
file move" but "what narrow capability does it actually need from its host".

#### 2026-08-28 amendment (third) — C3 splits, and the literal reading is rejected

Scoping C3 found two things the one-line plan did not anticipate. The pattern
from the second amendment held a third time, so it is now stated as standing
guidance below rather than rediscovered per rung.

**1. `Network` does not fit relay, and the literal move would change the wire.**
ADR 0026's `Network` is fire-and-forget `send_stream`/single-consumer
`recv_stream` with **no request/response correlation**. Relay is synchronous
call/await RPC. Two concurrent relay calls to one peer on a shared stream
cannot match replies to callers without a `req_id`. That machinery would have
to be built — the codebase already has the shape twice
(`animus-cp-data::cluster_segment_store`'s `req_id` + `Pending` slots polled
via `env.sleep()`, deliberately not `tokio::sync::oneshot` because `SimEnv`
callers have no tokio runtime; and `RaftKvNode`'s `ReadProbe`/`ReadProbeAck`).

Worse, `Network`'s `ProdEnv` impl dials the **`internal`** port (raw Raft/
`KvWire` frames), while relay dials **`intra`** (or `client`). ADR 0047 split
`intra` off `client` precisely so internal `ClientRequest` traffic never rides
the client edge; `internal` is a third, orthogonal port. Literally riding relay
on `Network` therefore **collapses `intra` into `internal`** — a production
wire-topology change, contradicting this ADR's own "explicitly not in scope:
any behaviour change" and fighting ADR 0047's separation rationale.

So the literal reading is rejected. The *spirit* — a multi-node cluster that
talks inside `SimEnv` — is achieved by making relay a capability and giving it
a second, sim-only implementor. Production keeps its existing transport and
ports, byte for byte. Anyone wanting the production merge should propose it as
its own change with its own ADR amendment; it is not a testability rung's to
smuggle in.

**2. C3 is four rungs, not one.**

| Sub-rung | Work |
|---|---|
| C3a | Move the **pure** half of `write_frame`/`read_frame` — framing arithmetic, `MAX_FRAME_LEN` bound, serde calls — into `animus-node` as functions over `&[u8]`. ~90% of those two functions; no socket. Trivial, independently shippable |
| C3b | A `RelayClient` capability trait in `animus-node::host`, beside C2's three. `animusd` implements it over the **unchanged** `relay_request` — still raw `TcpStream`, still on `intra`/`client`, zero wire change |
| C3c | `ControlHandle<E, R: RelayClient>` / `RemoteControlClient<R>`. Mechanical once C3b exists: every `Local` arm is already a synchronous passthrough to an `E`-generic `RaftNode<E>` accessor, and `metadata_fresh` is the single method doing real I/O |
| C3d | A `Network`-backed `RelayClient` implementor, **sim-only**, with the `req_id` correlation above and a reserved stream constant. This is what actually lets a cluster talk inside `SimEnv`, and it feeds Phase D's `SimCluster` |

Deferred to C5 as `ClientCtx`-entangled: `ClientCtx::relay` and every call site
reaching relay through it (`propose_schema`, `cp_serve_forwarded`'s
forwarding). C3 only frees the free functions and `control_handle.rs`, neither
of which touches `ClientCtx`.

**Standing guidance for C4 and C5.** Three rungs in, the same question has been
load-bearing every time, and asking "can this file move?" has been wrong every
time:

> Ask what **narrow capability** the code needs from its host, name that as a
> trait in `animus-node`, implement it thinly in `animusd`, and let the
> production implementation keep whatever concrete machinery it already has.
> A second, sim-only implementor of the same trait is what buys deterministic
> coverage — not relocating the production one.

#### 2026-08-28 amendment (fourth) — C4 splits, and `dynamo.rs`'s handlers belong to C5

Scoping C4 confirmed Decision 2's seam is sound — no edge streams a response, so
the bytes-in/bytes-out buffering assumption holds cleanly — but found the rung
is four shippable pieces plus one that is misfiled.

**C4a** move `http.rs`'s pure halves (header-block splitting, `Content-Length`
handling, `query_param`/`percent_decode`, response formatting) into
`animus-node`; `read_http_request`/`write_response_with` become thin wrappers
doing only `stream.read`/`write_all`. Mirrors C3a exactly. **C4b** factor the
SigV4 gate out of `dynamo.rs::handle_conn` — `animus_dynamo::sigv4::verify` is
already pure (and `animus-dynamo` has no `tokio` dependency at all); only the
build-request/read-`wall_now()`/map-error sequence is entangled. Cheap,
security-relevant, and closes the "only testable through a socket" gap this ADR
names for SigV4 specifically. **C4c** `console.rs`'s `route` is *already* at the
target shape — it takes `&HttpRequest` plus a `ConsoleBackend` trait and returns
a buffered tuple, with `TcpStream` confined to `handle_conn`. It moves nearly
verbatim. **C4d** `admin.rs`'s dispatch and its ~50 handlers behind an
`AdminHost` capability trait (a 15-method cluster-shape slice: raft, placement,
membership, metrics history).

**What does not move: `dynamo.rs`'s `run_operation` and the DDL handlers.**
Roughly **40** `tokio::time::Instant::now()`/`sleep` schema-commit poll loops
live **inside the handler bodies** — one per `create_table`/`update_table`/
`create_index`/`drop_index`/backup/restore/PITR handler, plus
`run_transact_get`'s poll — not wrapped around them. They cannot become
`env.sleep()`/`env.now()` without an `E: Env` bound, which needs the ~21-method
`ClientCtx` slice those handlers reach (schema propose, tablet provisioning,
kind-write, scan/read, the RMW lock) to be generic first.

A capability trait wide enough to cover that slice would simply be `ClientCtx`
under another name — **the exact failure mode the second amendment named for
C2**, resurfacing one rung later. So these handlers are not a C4 rung at all:
they fold into C5, where `ClientCtx` is split and genericized properly.

**On the pattern.** Four consecutive rungs have now needed re-planning on
contact with the code (C0's `prod` gate, C2's missing leaf, C3's port collapse,
C4's inline poll loops). The ADR's Phase C table was written at a granularity
that reads well but does not survive implementation. Treat every remaining rung
description as a *hypothesis to scope*, not a plan to execute — scoping has paid
for itself four times and has never yet been wasted.

### Phase D — the payoff

| Rung | Work |
|---|---|
| D1 | `SimCluster` harness: a multi-node `animus-node` cluster driven by `SimEnv`, on B1's shared corpus scaffolding |
| D2 | An end-to-end DynamoDB-wire corpus — requests in at the wire edge, faults injected, resulting history checked by the existing `check_cycles`/`check_durability`/`check_convergence` |
| D3 | Migrate the `animusd` integration suite: **keep** the tests that genuinely prove real-thread liveness (group commit, lock contention, election timing — per the engineering-lessons rule that `SimEnv` does not prove thread liveness), convert the rest. Success is measured by the `prod-liveness` CI job shrinking enough to drop its 2-attempt retry |
| D4 | Deterministic coverage for the behaviours that have none today: the auto-split byte trigger (`lib.rs:14397`), the dropped-table GC reclaim loop, join/growth sequencing, and the backup-janitor async loop (its replicated state machine is already sim-tested in `animus-control/tests/backup_catalog.rs`; the loop driving it is not) |

Note that the copy-based split driver (ADR 0050) is deliberately **not** on
this list: ADR 0058 rung 4's remaining layer deletes it. Writing a corpus
for code slated for removal would be waste — if that deletion slips, it gets
covered then.

### Phase E — the untested crates

| Rung | Work |
|---|---|
| E1 | `animus-operator`: a fake-kube-client harness and tests for `controller.rs` — `reconcile`, `apply_children`, `control_nodes_changed`, and the ADR 0032-driven `drain_and_remove_node` scale-down sequencing, which is precisely the stateful, ordering-sensitive logic this codebase otherwise insists gets a fault-injected test |
| E2 | `animus-cli`: argument/dispatch coverage for its 741 currently-untested lines |

## Consequences

**Good.** The node's own logic — routing, forwarding, retry, 2PC
coordination, the wire surface, the background loops — becomes reachable
from seed-reproducible simulation for the first time, which is where the
remaining correctness risk actually lives. CI's wall-clock burden and its
runner-starvation retry shrink. Failure triage stops being manual once B4
lands. The determinism rule becomes mechanically enforced rather than
review-enforced, which matters more as the codebase grows. Extracting pure
functions (Phase A) is a permanent readability win independent of everything
downstream.

**Costs and risks.** Phase C is a large mechanical refactor of the repo's
most complex file, and C5 in particular touches the write and transaction
paths. Mitigations: A6 lifts the pure logic out first; each rung ships green
under the full five-gate set; the existing 100-file `animusd` integration
suite stays in place *throughout* Phase C as the regression net, and is only
thinned in D3 once deterministic equivalents exist. Threading `E: Env`
through the node adds a generic parameter to a lot of signatures — noisy in
diff, but it is exactly the noise `animus-control` and `animus-cp-data`
already absorbed. The no-back-compat posture means no migration or
compat-shim work is owed for the wire-type moves.

**Explicitly not in scope.** No behaviour changes: this is a testability
refactor, and any bug it uncovers gets its own separate PR with its own
test, per the repo's convention on incidental discoveries. No further
crate-splitting of `animus-node` until its seams settle. No replacement of
the `ProdEnv` integration tests that prove real-thread liveness — ADR 0003's
guarantee is `SimEnv`-only and that boundary is deliberate.
