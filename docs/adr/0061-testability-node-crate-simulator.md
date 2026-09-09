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
| C5 | The brain, **minus admin/metrics** (see the fifth 2026-08-28 amendment): genericize `CpGroup`/`SharedEngine`/`ClientCtx` over `E: Env` **in place** first, then split into modules. The heaviest rung. **The move itself is dropped** — the seventh 2026-08-28 amendment closes Phase C with the brain staying in `animusd`, generic, `tokio`-free, and lint-enforced in place; step 3c and the `dynamo.rs` absorption go with it |
| C6 | ~~Node assembly moves~~ — **dropped** by the seventh 2026-08-28 amendment: the assembly was only in the plan because the brain was. Replaced by Phase C's closing rung: a `SimEnv`-driven `ClientCtx` harness in `animusd`'s own tests, and narrowing rung B5's package-level `disallowed_methods` exemption to the files that still need it |

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

#### 2026-08-28 amendment (fifth) — C5's shape, and what it deliberately leaves behind

Scoping C5 found the rung larger than recorded and differently shaped. `impl
ClientCtx` is now **6,287 lines across 97 methods** (it grew during C2–C4), and
the clusters do not separate the way this ADR assumed.

**Read and write are line-interleaved**, not regionally separable — `cp_read`
ends where `cp_kind_write_item` begins. Read, write and 2PC share `poll_probe`;
`cp_serve_forwarded` calls into all three by name. So "move the read path first"
silently drags most of the rung with it: the false-leaf trap the second
amendment already named once.

**Three things genuinely resist the move**, and each needs a decision rather
than effort:

- `DataRole::rmw_lock: Arc<tokio::sync::Mutex<()>>`. `animus-node` has no
  `tokio` dependency and `Env` exposes no async-lock primitive. Either add one
  to the seam, or keep RMW serialization behind a `with_rmw_lock` host
  capability implemented concretely in `animusd`.
- `SegmentStoreHandle`/`BackupStoreHandle` hardcode `FsSegmentStore` — the
  `prod`-gated type that **cannot exist** in `animus-node`'s build (C0). They go
  behind a capability trait, as C2 already did for `BackupObjectStore`.
- Only **16** of the folded-in poll loops share the mechanical deadline shape.
  `run_transact_get`'s is round-bounded, `poll_probe` wraps a three-tier
  term-checked confirm, and `cp_txn`'s retry allowlist carries a comment
  recording **two reverted attempts** that caused double-materialization.
  A shared `poll_until` helper may absorb the 16; the other three keep their
  bespoke logic. Flattening them would reintroduce proven bugs.

**Decision — the minimal cut.** C5 excludes **admin/metrics** (9 methods, ~612
lines). Nothing in the DynamoDB wire path or Phase D's `SimCluster` reaches
`admin_add_member`/`metrics_history`/`admin_drain`; they are served through C4d's
`AdminHost` with `ClientCtx` as one concrete implementor, and they have their own
real-socket coverage. They stay `ProdEnv`-hardcoded in `animusd` indefinitely.
That trims ~10% of the rung for near-zero loss against Phase D's goals.

**Decision — sequencing.** Genericize `CpGroup<E>`/`SharedEngine<E>`/
`ControlHandle` binding/`ClientCtx` over `E: Env` **in place inside `animusd`**
first, with `cargo build` as the check; then split into modules; then move. This
is deliberately the "genericize in place" approach Decision 1 rejects as a
*standing state* — but as a transient first step immediately before the physical
move it separates type-signature churn from boundary enforcement, which is the
same C0→C1 sequencing that has now worked twice. The step-1 diff touches every
read/write/txn call site at once and is not meaningfully green midway, so it
needs internal checkpoints (`CpGroup<E>` green before `ClientCtx`'s own
signature).

**Hazards for review**, from the repo's own hard-won rules — a mechanical-looking
refactor can silently undo any of these: `poll_probe`'s term-checked confirm
exists because index-alone confirmation false-acked; `cp_txn`'s narrow retry
allowlist exists because wider ones double-materialized; `ProposeResult::Accepted`
means appended, never committed, at every `CpGroup` call site the genericization
touches; and `rmw_lock` must stay held across exactly the read-modify-write span,
neither narrower nor wider.

#### 2026-08-28 amendment (sixth) — the orphan rule blocks moving `ClientCtx`, and 91 poll sites remain

Scoping C5 step 3 found two things, one mechanical and one architectural. The
architectural one needs a maintainer decision before any of the brain moves.

**1. `ClientCtx` the struct probably cannot move to `animus-node` at all.**
Four impls already exist *because* the type is local to `animusd` while the
traits are foreign: `ControlLeaderHost`, `BackupObjectStore`, `TtlScanHost`
(`client_ctx_host.rs`) and `AdminHost` (`admin.rs`). Move the struct and all
four become foreign-trait-for-foreign-type — an orphan-rule violation. Worse,
`BackupObjectStore`'s impl reaches `DataRole.backup_store: BackupStoreHandle`,
which names the `prod`-gated `FsSegmentStore` and so categorically cannot exist
in `animus-node`'s build. "Move the impls too" is dead for at least that one.

Two ways out, and they differ in kind, not degree:

- **(i)** `ClientCtx` stays in `animusd` permanently, and the five clusters
  become **default methods on capability traits** defined in `animus-node`,
  with `animusd` supplying thin accessors for `control`/`edge`/`env`/`data`/
  routing. This is the standing "narrow capability" guidance scaled up to the
  whole brain — consistent with C2/C3/C4, but a much wider trait surface than
  any of those.
- **(ii)** Split `ClientCtx` into a movable pure-state struct plus an
  `animusd`-local wrapper that re-implements the four host traits by
  delegating. A genuine architecture change.

Neither is "move the file". **This is recorded as open**; C5 step 3 must not
guess at it.

**2. Steps 1 and 2 were signature-and-location only, so the bodies still hold
91 raw `tokio` sites** across the five modules (`schema` 40, `write_path` 25,
`read_path` 12, `forwarding` 8, `txn_coordinator` 6), plus a bare
`tokio::select!` in `schema`'s `WatchMetadata` long-poll and a `tokio::spawn`
and `tokio::time::timeout` in `txn_coordinator`. `animus-node` has no `tokio`
dependency at all, so every one must become `env.now()`/`env.sleep()` — or, for
the `select!` and `timeout`, a hand-rolled race against `env.sleep()`, the same
shape C3's amendment used for relay correlation — **before any of these files
can compile there**, independent of the lint.

**3. `dynamo.rs` is a hidden fourth dependency.** `kind_write_item_at_leader`,
`eval_kind_txn_write`, `item_key`, `KindWriteOutcome`, `kind_write_is_idempotent`,
`encode_relayed_error` are free functions there — some already `E`-generic from
step 1, but physically in a file that is not moving. `write_path`,
`txn_coordinator` and `forwarding` cannot be portable until these are re-homed.

**4. Two resisters resolve cheaply.** `SegmentStoreHandle`/`BackupStoreHandle`
turn out to block nothing here — a grep of all five modules returns **zero**
references; they are touched only by files step 3 does not move, so they become
a later rung's problem (`index_drain`, `dynamo_streams`), where C2's
`BackupObjectStore` pattern applies directly. And `rmw_lock` wants option (a),
a narrow `with_rmw_lock` host capability implemented concretely in `animusd` —
**not** a new `Env` async-lock primitive. The lock is a same-node
collision-rate optimization, not a correctness mechanism (the OCC seatbelt is
what makes the path safe, per `dynamo.rs`'s own note on issue #285), so it does
not belong in the seam every `Env`-generic component shares. The capability
method also keeps the guarded span exactly where the caller puts it, which
matters: it must cover the local read, condition check and new-value
computation, and drop **before** the propose/confirm poll.

**Decomposition.** Step 3 splits: **3a** add `R: RelayClient` to `ClientCtx` so
`schema`/`forwarding` can use the generic `ControlHandle<E, R>`; **3b** convert
the 91 `tokio` sites in place, still inside `animusd`; **3c** move `read_path`
(least entangled — 12 sites, no `self.control`) once the open question above is
settled. `write_path`, `forwarding` and `txn_coordinator` stay put until the
`rmw_lock` capability, the `dynamo.rs` re-homing, and the `ClientCtx`-location
decision all land. 3a and 3b are unblocked and ship now.

#### 2026-08-28 amendment (seventh) — Decision 1 was right for the leaves and wrong for the brain

The sixth amendment recorded that the orphan rule blocks moving `ClientCtx` into
`animus-node`, and left the way forward open. This settles it, and in doing so
revises **Decision 1** for the remainder of Phase C.

**The realisation.** Sim-testability of the read/write/txn/forwarding/schema
paths does not require the crate move at all. `ClientCtx<E>` is already generic
(step 1) and its module bodies are `tokio`-free (step 3b). A
`ClientCtx<SimEnv>` can therefore be constructed and driven **in `animusd`'s own
tests**, with `animus-sim` as a dev-dependency. The deterministic coverage
Phase D wants is reachable without moving a line.

What the move was actually buying was **compiler-enforced determinism** —
Decision 1's entire argument for a crate boundary over genericizing in place.
And that is now obtainable another way. `animusd` is package-level exempt from
`disallowed_methods` (rung B5, because it then had ~600 real call sites). Once
the five modules are `tokio`-free, that exemption narrows: `#[deny(clippy::
disallowed_methods)]` on `schema`/`read_path`/`write_path`/`txn_coordinator`/
`forwarding`, with the package exemption retained only for `lib.rs` and
`dynamo.rs`. That is compiler enforcement, scoped precisely where it matters.

**Decision 1's premise has weakened, and this ADR should say so rather than
quietly keep spending rungs against it.** It rejected genericize-in-place
because doing so "leaves nothing to stop it — the same review-only enforcement
that already left the hole." True when written. No longer: rung B5 built the
lint infrastructure that did not exist then, and lint scope is a real boundary,
not a review convention.

**Decision.** Phase C stops moving code at the brain. Concretely:

- `ClientCtx` and the five clusters **stay in `animusd`**, generic over
  `E: Env`, `tokio`-free, and lint-enforced in place.
- **Rung 3c is dropped.** So is **C6** (node assembly) — the assembly never
  needed to move either; it was only in the plan because the brain was.
- A `SimEnv`-driven test harness lands in `animusd`'s own tests, giving the
  read/write/txn paths their first deterministic coverage. This is what Phase D
  builds `SimCluster` on.

**What `animus-node` keeps** is everything that moved *cleanly*, because those
pieces genuinely were leaves: the wire types, `topology`, `decide`, the frame
codec, the SigV4 gate, HTTP parsing, console routing, admin dispatch,
`ControlHandle`, five capability traits, and five background loops — with 107
unit tests and 5 sim tests running in under two seconds, none of which existed
before this ADR.

**The general lesson, for the next architecture ADR in this repo.** A crate
boundary is an excellent enforcement mechanism for code that is already a leaf,
and a poor one for code that is load-bearing in its own crate's type graph. The
orphan rule is the specific mechanism, but the shape is general: a type with
foreign-trait impls cannot leave the crate that owns those impls without taking
them with it, and they cannot come if they touch anything the destination crate
excludes. Ask that question *before* choosing a crate split as the enforcement
strategy — it is cheap to check and expensive to discover six rungs in.

#### 2026-08-28 amendment (eighth) — Phase C's closing rung landed; the claim held for reads/writes, with one precise DDL boundary

Scoping and building the closing rung (a `SimEnv`-driven `ClientCtx` harness
in `animusd`'s own tests, per the seventh amendment's decision) confirmed the
core claim exactly as written: `ClientCtx<SimEnv, _>` is constructible and
drivable in `animusd`'s own tests, with `animus-sim` as a `[dev-dependencies]`
entry and **no visibility widened anywhere** (the harness is an in-crate
`#[cfg(test)] mod`, reachable via Rust's ordinary "descendant module" privacy
rule — see `crates/animusd/CLAUDE.md`'s own section on it for the full
design). A real write (`ClientCtx::cp_kind_write_raw`) and a real read
(`ClientCtx::cp_get`) — the exact methods `handle_request`'s `ClientRequest::
Put`/`Get` arms call in production — both run end to end through a real
one-voter control `RaftNode<SimEnv>` and a real one-voter CP data-plane
`RaftKvNode<SimEnv, MemoryEngine>`, seed-reproducibly, with no sockets and no
`ProdEnv`. This is a full outcome against the rung's stated goal, not a
partial one requiring an unwinding later.

**One thing the seventh amendment's phrasing didn't anticipate, found while
building rather than while scoping.** That amendment listed "the read/write/
txn/forwarding/schema paths" as what the harness would prove reachable. The
first four are: this rung's harness drives read/write directly, and nothing
about txn/forwarding's own genericity (steps 1/3a/3b already made both
`tokio`-free and `E`-generic, unchanged by this rung) is in question. **Schema
is different in kind, not degree.** `ClientCtx::propose_schema`'s
local-propose fast path — the thing every schema-DDL call
(`provision_tablet`, `trigger_split`, `drop_table*`, and `propose_schema`
itself) ultimately needs — reads `ClusterEdgeState::control: Arc<Mutex<
Vec<RaftNode<ProdEnv>>>>`, a field that is concretely `ProdEnv`-typed
regardless of the enclosing `ClientCtx<E, R>`'s own `E`. This is not new
technical debt this rung introduced: it is a pre-existing, deliberate design
choice rung C3c already made and documented on `ControlHandle`'s own doc
(`animus-node::control_handle`) — proposing a `MetaCommand` is "inherently a
local-Raft-log operation," so `ControlHandle::propose`/`flush` were
deliberately never added to that seam, and every proposal instead goes
through this one concrete, `ProdEnv`-bound handle. The closing rung's harness
therefore cannot drive `propose_schema` under `SimEnv` at all — it seeds the
schema catalog by proposing directly on the control `RaftNode` instead
(bypassing `ClientCtx` for setup, the same thing `animus-node/tests/
index_backfill_sim.rs` already does for the identical reason), which is
sufficient for read/write coverage but means DDL stays undriven here.

**A second, independent blocker, also found by building rather than
scoping**: `DataRole`'s `SegmentStoreHandle`/`BackupStoreHandle` hardcode
`FsSegmentStore`/`ClusterSegmentStore<ProdEnv, FsSegmentStore>` regardless of
`E` — not a C0 feature-gate issue (`animus-env`'s `prod` feature is
unconditionally on for `animusd`), simply that neither handle type takes an
`E` parameter at all. Not exercised by this rung (`cp_kind_write_raw`/
`cp_get` never call `self.data()`), so `data: None` sufficed — but it is the
next thing a follow-on rung driving the DynamoDB-shaped write path
(`cp_kind_write_item`) or any of the TTL/backup/stream loops under a real
`DataRole` will hit.

**Neither blocker is treated as something to fix in this rung.** Both are
narrow, precisely located, and — per the second and fourth 2026-08-28
amendments' standing guidance — routing around either with a new capability
trait purely to make DDL/`DataRole` sim-drivable would be exactly the
"contorted trait built to make a move happen" failure mode those amendments
warn against, not a genuine leaf capability. They are recorded here as the
scope boundary Phase D's `SimCluster` (D1) needs to know about before it
tries to seed schema or exercise `DataRole`-dependent paths the same way:
`SimCluster` will need its own answer to "how does a multi-node `SimEnv`
cluster propose schema and reach quorum on it," which is a real design
question, not a rerun of this rung's single-node bypass.

**On the pattern.** This is the first rung in Phase C's delivery whose
closing scoping pass did *not* find the plan needed re-ordering or
re-splitting — the six earlier amendments (C0, C2, C3, C4, C5's fifth, C5's
sixth) each corrected the *shape* of a rung before or during work. This one
confirms the seventh amendment's redirection (stop moving code, prove
`SimEnv`-drivability where the code already stands) was the right call, and
the residual findings above are refinements to *what the proof covers*, not
corrections to *how it should be built*.

#### 2026-09-04 amendment — C3d landed: a sim-only `Network`-backed `RelayClient`, and the relay seam threaded through `ClientCtx`

C3d (the third 2026-08-28 amendment's own table) is done — the piece the
eighth amendment's own closing note flagged as still missing before a
`SimCluster` could talk to itself: `animus_node::sim_relay::SimRelayClient
<E: Env>`, plus threading `ClientCtx`'s own relay call sites through the
`R: RelayClient` field this rung adds, so the seam C3b/C3c built is
actually load-bearing end to end rather than reachable only by
`AnimusdRelayClient`.

**Stream allocation.** `SimRelayClient` reserves `RELAY_STREAM = u64::MAX -
2` (`animus_node::sim_relay`, whose module doc carries the full table
gathered by grepping every existing reserved-stream constant in the
workspace: `PRIMARY_STREAM` = 0, a CP data-plane tablet's own group =
`tablet.0`, `BACKUP_SEGMENT_STREAM` = `u64::MAX - 1`, `SEGMENT_STREAM` =
`u64::MAX`) — disjoint from all three, and from every plausible `tablet.0`
(small, sequential, nowhere near `u64::MAX`).

**Address convention.** A `SimEnv` node has no host:port, so
`RelayClient::relay`'s `addr: String` is defined to be exactly
`NodeId::to_string()` under this implementor — `SimRelayClient::relay`
parses it back via `NodeId::new_unchecked` (the literal inverse of
`Display`), never a separate `String -> NodeId` lookup table. A fixture
that wants a sim node's `client_route`/`intra_route` entry writes
`id.to_string()` as the address, precisely the shape `SimCluster` (D1)
must use for its own route tables.

**One stream, two roles.** `(node, stream)` is single-consumer (ADR 0026),
and a node acting as a relay is both the *client* sending `relay()` calls
out and the *server* answering another node's calls, on the identical
stream — a reply to this node's own outbound call and an inbound request
both arrive on `RELAY_STREAM`. `SimRelayClient` follows `animus_cp_data::
cluster_segment_store::serve_loop`'s own precedent rather than a
direction-demultiplexed pair of streams: one wire enum (`RelayWire::
{Request, Reply}`, `req_id`-correlated exactly like that module's own
`Pending` slots — a monotonic per-client counter, not an `Rng` draw, so it
never perturbs a test's other seeded draws), one receive loop dispatching
on which variant arrived. `SimRelayClient::new` spawns that loop
unconditionally (not `serve`, which only *installs a handler* into an
`Arc<Mutex<Option<Handler>>>` the already-running loop reads) — a node
that never calls `serve` still needs the loop running to receive its own
outbound calls' replies, the opposite of what the eighth amendment's own
"answers none until `serve` is called" phrasing (written before this rung
built the thing) implied.

**The generic relayed-request dispatcher.** `forwarding::
handle_relayed_request<E: Env, R: RelayClient>(ctx: &ClientCtx<E, R>, req:
ClientRequest) -> ClientResponse` covers exactly the three `ClientRequest`
variants a `ClientCtx<E, R>` method actually relays today —
`Forwarded` (`forward_to_tablet_leader`/`read_path.rs`'s
`relay_stale_read`, delegating to `cp_serve_forwarded`), `ProposeSchema`
(`schema.rs`'s single-hint relay and its ADR 0030 broadcast fallback,
gated on `is_relayable_command`), and `Status` (`RemoteControlClient::
metadata_fresh`, rung C3c) — everything else answers a plain
`ClientResponse::Error("not relayable under sim")`, deliberately not an
attempt at `ClientRequest`'s full surface (the plain client-facing ops
never reach a node-to-node relay at all). **Production's `handle_request`
now delegates its `Status`/`Forwarded`/`ProposeSchema` arms to this exact
function** — a pure refactor (each arm's body moved verbatim), so there is
one dispatch table for the relayed set, never two independently-maintained
copies; every other arm (`Put`/`Get`/`SplitTablet`/`JoinInfo`/
`WatchMetadata`/`Txn`/the internal tablet-addressed RPCs) stays exactly
where it was, unmoved and unmodified.

**Relay threading.** `ClientCtx<E, R>` gains a `relay: R` field (alongside
the pre-existing `control: GenericControlHandle<E, R>`, which already
carried its own `R` for `RemoteControlClient`'s `Status` fetch, rung C3c —
this is every *other* relay call `ClientCtx`'s own methods make directly).
`forward_to_tablet_leader`, `ClientCtx::relay`, `read_path.rs`'s
`relay_stale_read`, and `schema.rs`'s `propose_schema` broadcast fallback
all now call `self.relay.relay(..)` instead of the free `relay_request`/
`relay_request_with_timeout` functions. **Production behavior is
byte-for-byte unchanged**: `AnimusdRelayClient::relay` is the same
unmodified wrapper over `relay_request_with_timeout` it always was
(rung C3b); `spawn_common_tail`'s `ClientCtx` struct literal sets `relay:
AnimusdRelayClient` — a zero-sized `Default` value — and every other field
and call site is untouched. `relay_request`/`relay_request_with_timeout`
themselves are unchanged and still exist, now called from exactly two
places (`AnimusdRelayClient::relay`, and `remote_metadata_watch_loop`,
which sits outside the five seam-clean modules and keeps calling the free
function directly — unrelated to this rung's scope).

**Proof.** `animus-node`'s own tests (`sim_relay::tests`, four of them:
request/reply round trip, a partitioned peer timing out cleanly, a late
reply after timeout never matching a later request's `req_id`, and several
concurrent outstanding requests to one peer each resolving to their own
caller) exercise the implementor in isolation. `animusd`'s
`two_node_relay_tests` (sibling to the eighth amendment's own single-node
`simenv_client_ctx_tests`) is the end-to-end proof this rung's own brief
asked for: two `ClientCtx<SimEnv, SimRelayClient<SimEnv>>`s, one per
`SimEnv` node id, node B (no local tablet replica) forwarding a real
`cp_kind_write_raw`/`cp_get` round trip to node A's own locally-led tablet
through the real relay wire — `forward_to_tablet_leader` resolving
`CpRoute::Forward`, carrying it over `SimRelayClient`, `forwarding::
handle_relayed_request` serving it via `cp_serve_forwarded` against the
real local leader — with a third, direct `cp_get` on node A confirming the
write actually landed on its own engine, not merely echoed back through
the relay's own bookkeeping.

**What this does not attempt.** Schema DDL through `ClientCtx::
propose_schema`'s *local-propose fast path* is still unreachable under
`SimEnv` for the identical, pre-existing reason the eighth amendment
recorded: `ClusterEdgeState::control` is concretely `RaftNode<ProdEnv>`-
typed regardless of `E`. What changed is that `propose_schema`'s *relay*
branches are now reachable (they always fall through to them under
`SimEnv`, since the fast path's own field can never hold a `SimEnv`
handle) — sufficient for `two_node_relay_tests`' own write/read proof,
which never calls `propose_schema` at all (it seeds schema by proposing
directly on the shared control `RaftNode`, the same bypass every `SimEnv`
`ClientCtx` fixture in this crate uses). A `SimCluster` that needs a
genuine multi-voter control quorum reaching agreement on a `ProposeSchema`
call still needs its own answer to that question — this rung does not
supply one, and isn't trying to.

### Phase D — the payoff

| Rung | Work |
|---|---|
| D1 | `SimCluster` harness: a multi-node cluster driven by `SimEnv`, on B1's shared corpus scaffolding. Built on `ClientCtx<SimEnv>` in `animusd`'s own tests, per the seventh 2026-08-28 amendment — not on a moved `animus-node` assembly |
| D2 | An end-to-end DynamoDB-wire corpus — requests in at the wire edge, faults injected, resulting history checked by the existing `check_cycles`/`check_durability`/`check_convergence`. **Landed 2026-09-07 (both PRs)**: PR 1 (six item operations generic, `SimClusterHandle::dynamo`, a first small smoke) and PR 2 (the actual `Recorder`/`History` corpus over the wire, see the amendments below); GSI/LSI, transact, and PartiQL remain out of scope, named as D2's own residuals |
| D3 | Migrate the `animusd` integration suite: **keep** the tests that genuinely prove real-thread liveness (group commit, lock contention, election timing — per the engineering-lessons rule that `SimEnv` does not prove thread liveness), convert the rest. **Success criterion corrected 2026-09-07** (see that date's own "D3 PR 1" amendment): the `prod-liveness` job's 2-attempt retry was already replaced by nextest sharding before D3 started, so there is no retry to drop — success is measured by the real-thread tier's own shrinking test count / wall time / flake surface instead. **PR 1 landed 2026-09-07**: the base-table-only "B class" (~30 tests across ten `dynamo_*.rs` binaries plus `kind_batch_outcome.rs`) converted to `SimCluster`. **PR 2a landed 2026-09-07**: `Metadata::members` population + `ClusterEdgeState::control` widened to `RaftNode<E>` make base-table DDL (`CreateTable`/`DeleteTable`/`ListTables`/`DescribeTable`, via new `dynamo::dispatch_table_op`) drivable over the real wire; two real fixture bugs found and fixed (a liveness-detector heartbeat gap, a tablet-id-allocator collision) and one genuine, documented `SimCluster` gap found and left open (a rebalanced-away replica's `RaftKvNode` is never torn down — see that date's own "D3 PR 2a" amendment). **PR 2b landed 2026-09-07**: `UpdateTable`'s own throughput-only change (`BillingMode`/`ProvisionedThroughput`, ADR 0065) is now drivable too, via a widened `dynamo::update_table_throughput` and a new `UpdateTable` arm on `dispatch_table_op` — five more `dynamo_throttling.rs` tests converted, no new fixture bugs (see that date's own "D3 PR 2b" amendment). **PR 3a landed 2026-09-07**: GSI/LSI `Query`/`Scan` dispatch through `SimCluster`, plus `CreateTable` with a declared GSI/LSI — eight functions widened to `<E, R>` (`run_index_query`/`run_gsi_query`/`run_lsi_query`/`run_index_scan`/`run_gsi_scan`/`run_lsi_scan`/`paginated_kind_examine`/`paginated_kind_examine_one`), 42 tests converted across nine new sibling modules; a GSI row is still never materialized under `SimCluster` (no drain loop spawned), pinned by its own new regression, so every GSI-*data* test stays on `ProdEnv` (see that date's own "D3 PR 3a" amendment). **PR 3b landed 2026-09-07, closing D3's own GSI-drain boundary**: `index_drain::drain_tablet`/`reconcile_partition` widened to `<E, R>` and a new `SimCluster::drain_gsi` fixture helper materialize a GSI's hidden table on demand, flipping PR 3a's own boundary regression positive and converting every GSI-data test it had to leave on `ProdEnv` (12 tests across nine sibling modules, two of them new: `sim_cluster_dynamo_documents.rs`, `sim_cluster_dynamo_schema.rs`) plus a sim twin of `dynamo_indexes.rs::gsi_write_then_query` that does not replace the original; seven `tests/dynamo_*.rs` files deleted whole, one trimmed (see that date's own "D3 PR 3b" amendment). D3 is now closed for the GSI-drain gap specifically — remaining `ProdEnv` binaries are there for real-thread-liveness or not-yet-generic-operation reasons. **D3 closed 2026-09-07 (PRs #711 #716 #717 #718 #719 + this)** — see the dated "D3 closing" amendment below for the full before/after numbers, the reframed success criterion's verdict, and the residual `tests/*.rs` inventory by class |
| D4 | Deterministic coverage for the behaviours that have none today: the auto-split byte trigger (`lib.rs:14397`), the dropped-table GC reclaim loop, join/growth sequencing, and the backup-janitor async loop (its replicated state machine is already sim-tested in `animus-control/tests/backup_catalog.rs`; the loop driving it is not) |
| F | Post-C-04: Transact/PartiQL `SimCluster` dispatch (C-06) — the two named D2 residuals (Transact, PartiQL), never claimed by any D3/D4 rung. **Closed 2026-09-08 (PRs #728, #729, #732, #748, #750, #756, plus PR 7)** — both residuals now reachable through `dispatch_item_op`, the real-socket `dynamo_partiql.rs`/`dynamo_execute_transaction.rs` kept in full as the `ProdEnv` equivalence proof, `cargo test -p animusd --lib` 353 → 438 passed across PRs 3-6. See the 2026-09-07 "Rung F" amendment and the "Rung F closed" amendment below, and `docs/roadmap.md`'s C-06 entry |
| G | Post-C-06: Streams `SimCluster` dispatch (C-07) — the largest remaining unowned residual group named by Rung F's own close-out (`dynamo_streams.rs`/`stream_janitor.rs`/`stream_backfill_seed_filter.rs`, 3 files/28 tests, plus `console_stream.rs`'s own 4 tests filed under the console/dashboard group). **Closed 2026-09-08 (PRs #758, #759, #760, #761, #762, plus PR 6)** — `dynamo_streams.rs` (15 tests: 12 converted, 3 kept `ProdEnv`) and `stream_janitor.rs` (11 tests: 9 converted, 2 kept `ProdEnv`) both closed; the read API, stream enable/disable, on-demand sealing, and the segment janitor's two-phase retention sweep are all `SimCluster`-reachable. `stream_backfill_seed_filter.rs` (2 tests) stays `ProdEnv`, filed under the separate "index DDL beyond plain `CreateTable`" residual, per the rung's own plan. `console_stream.rs` (4 tests) stays filed under admin/console/dashboard HTTP. `tests/streams_e2e.rs` stayed out of scope throughout, frozen behind #298/#745. `cargo test -p animusd --lib` 315 passed / 2 ignored at the sim tier after PR 5, no leak trajectory. See the 2026-09-08 "Rung G" amendments below (including the "Rung G closed" amendment) and `docs/roadmap.md`'s C-07 entry |
| H | Post-C-07: admin/console/dashboard HTTP `SimCluster` dispatch (C-08) — the group Rung G's own close-out named as what remains unowned, 10 files/67 tests (`admin_endpoint.rs` 23, `dashboard_endpoint.rs` 16, `console_endpoint.rs` 3, `console_create_table.rs` 4, `console_items.rs` 4, `console_stream.rs` 4, `console_table_config.rs` 9, `console_tables.rs` 1, `metrics_endpoint.rs` 1, `system_table.rs` 2). **Closed 2026-09-08 (PRs #764, #765, #766, #767, #773, #776, #777, plus this PR 8)** — 42 of the 67 tests now have a deterministic `SimCluster` sibling across six new modules (`sim_cluster_console.rs`, `sim_cluster_console_stream.rs`, `sim_cluster_console_table_config.rs`, `sim_cluster_admin.rs`, `sim_cluster_admin_actions.rs`, `sim_cluster_dashboard.rs`; 89 tests with `_over_seeds`), `console_tables.rs`/`console_create_table.rs`/`console_items.rs` deleted whole; 25 tests stay `ProdEnv` with a documented reason each (`admin_endpoint.rs` 10, `dashboard_endpoint.rs` 4, `console_endpoint.rs` 3, `console_stream.rs` 1, `console_table_config.rs` 4, `metrics_endpoint.rs` 1, `system_table.rs` 2). Two real, previously-latent seam bugs found and fixed (a shared `MetricsHandle::noop()` corrupting `/admin/metrics`'s `is_leader` gauge cluster-wide, PR 5; `ClientCtx::admin_transfer_control_leadership`'s commit-wait loop still reading the real clock despite an already-generic signature, PR 6, the third recorded recurrence of that lesson), plus one same-day-corrected design mistake (PR 2's blanket `impl Trait for ClientCtx` narrowing production, fixed with the `GenericAdminHost`/`GenericConsoleBackend` newtype pair). `cargo test -p animusd --lib sim_cluster` ran 317 → 337 → 353 → 367 → 383 → **406 passed, 0 failed, 2 ignored** (#777's own real gate run, 752.46s, anchored-sampler RSS first ~99 MB / peak ~816 MB / last ~133 MB). See the 2026-09-08 "Rung H" amendments below (including "Rung H, PR 2 landed" through "Rung H, PR 7 landed", and "Rung H closed") and `docs/roadmap.md`'s C-08 entry |
| I | Post-C-07: TTL reaper `SimCluster` dispatch (C-09) — the `TTL (1/9)` residual Rung H's own close-out named and recommended first, of the six groups left unowned after C-08. `animus_node::ttl_reaper::{ttl_reaper_loop, ttl_sweep_one_tablet}` was already `<E: Env, H: TtlScanHost + TtlReaperProgressHost>`-generic (rung C2); the one remaining concrete surface was `crates/animusd/src/ttl_reaper.rs`'s thin wrapper plus `impl TtlReaperProgressHost for ClientCtx` (bare defaults) in `client_ctx_host.rs:98` — `TtlScanHost` was already `<E, R>`-generic there (D4 PR5). No primitive drove the loop under `SimEnv` before this rung (`SimCluster::new`/`restart` never spawned it), so `tests/dynamo_ttl.rs`'s 9 tests plus one residue test apiece in `admin_endpoint.rs` and `console_stream.rs` all stayed `ProdEnv`. **Closed 2026-09-09 (PRs #780, #782, #785, #786, #787, plus this PR 6)** — PR 2 (groundwork: `TtlReaperProgressHost`/the `ttl_reaper.rs` wrapper widened, `SimCluster`'s always-on per-node reaper spawn at a 200ms sim interval, `drive_ttl_sweep`), PR 3 (`sim_cluster_ttl.rs` extended with 5 more scenarios, 5/8 of the remaining tests converted), PR 4 (admin/console residue: `sim_cluster_admin.rs`'s reaper-progress scenario, `sim_cluster_console_stream.rs`'s TTL-identity scenario replacing `console_stream.rs`, which is deleted whole), and PR 5 (the `DescribeTimeToLive` dispatch gap PR 3 found, closed, restoring its own two reverted scenarios) all landed. `cargo test -p animusd --lib sim_cluster` ran 406 → 408 → 420 → 424 → **428 passed, 0 failed, 2 ignored**. Final residue: exactly one test, `tests/dynamo_ttl.rs::expired_item_is_still_readable_immediately` (every `SimCluster` wire call drains the fixed 12s `OP_BUDGET` while the always-on 200ms reaper ticks, so an already-expired item can never be observed pre-reap through this fixture) — `admin_endpoint.rs` keeps 9 tests, `console_stream.rs` is gone. See the matching 2026-09-08/09 "Rung I" amendments below (including "Rung I closed") and `docs/roadmap.md`'s C-09 entry |
| J | Post-C-09: index DDL beyond plain `CreateTable` `SimCluster` dispatch (C-10) — blocker (d) named by both Rung H's and Rung I's own close-outs, the largest remaining `SimCluster`-dispatch group (9 files/30 tests in the D3-closing class-D breakdown). `dynamo.rs:1725-1774`'s `dispatch_table_op`'s `UpdateTable` arm rejects any call with `index_update.is_some()` via `unsupported_by_generic_dispatch`; `create_index`/`drop_index`/`set_index_status`/`drop_table_index` (`dynamo.rs:4509/4627/4680/4716`) are concrete (`&ClientCtx`, `tokio::time::Instant::now()`/`sleep`) even though every `ClientCtx` method they call (`propose_schema`/`drop_table_tablets`/`clear_backfill_cursor_for_table`, `schema.rs`'s own `impl<E: Env, R: RelayClient> ClientCtx<E, R>` block) is already generic — pure signature/clock-call widening, no new capability. `index_drain::drain_tablet`/`reconcile_partition`/`gsi_caught_up` are already `<E, R>`/`<E, R>`/`<E>`-generic (D3 PR 3b); `animus_node::index_backfill::index_backfill_loop<E, H: ControlLeaderHost<E>>` is already fully generic and `ControlLeaderHost` is already implemented generically for `ClientCtx<E, R>` (`client_ctx_host.rs:47`) — only `crates/animusd/src/index_backfill.rs`'s 18-line wrapper is concrete. `index_drain::backfill_seed_tick`/`advance_backfill_cursor`/`seed_change_log_record` (`index_drain.rs:1412/1539/1651`) stay concrete (`&ClientCtx`/`&CpGroup`, one `tokio::time::Instant`), called only from `change_consumer_loop`, which `SimCluster` never spawns (`sim_cluster.rs:2987`'s own doc). `SimCluster::drain_gsi` (`sim_cluster.rs:2802`) already exists and hand-drives `drain_tablet` directly; no primitive drives `backfill_seed_tick`. `GenericConsoleBackend::add_gsi`/`drop_gsi` (`lib.rs:4107/4123`) already route through `execute_routed_as_generic` — zero console/`lib.rs` change needed once the dispatch sub-arm exists. `CreateTableIndex`/`DropTableIndex`/`SetIndexStatus`/`MarkIndexBackfilled` are already on `is_relayable_command`'s allowlist (`wire.rs:764-781`). **Closed 2026-09-09 (PRs #789, #790, #791, #792, #793, #794, plus this PR 7)** — PR 2 (groundwork: the four `dynamo.rs` functions plus the `index_drain` seeder trio widened, the `UpdateTable` index sub-arm, the always-on `index_backfill_loop` spawn, `drive_backfill_seed`), PR 3 (`sim_cluster_dynamo_update_table_index.rs`, 9 tests from `update_table_create_index.rs`/`update_table_drop_index.rs`/`dynamo_gsi_drain.rs`, all three deleted whole), PR 4 (`sim_cluster_backfill_seeder.rs`, 4 of `backfill_seeder.rs`'s 5 scenarios, `split_during_backfill_converges_with_correct_final_gsi` kept `ProdEnv` per its own license, `SimCluster::restart` now respawns `index_backfill_loop`), PR 5 (`sim_cluster_stream_backfill_seed_filter.rs`, 2 tests, file deleted whole), and PR 6 (3 GSI-DDL `console_table_config.rs` scenarios into `sim_cluster_console_table_config.rs`, file trimmed to its 1 out-of-scope PITR test) all landed. `cargo test -p animusd --lib sim_cluster` ran 428 → 432 → 458 → 462 → **468 passed, 0 failed, 2 ignored**. Final residue: the three frozen files (`dynamo_index_scan.rs` #418, `index_backfill.rs` #592, `dynamo_index_writes.rs` #610, 14 tests, untouched throughout), `backfill_seeder.rs`'s one licensed `ProdEnv` residual, and `console_table_config.rs`'s one PITR residual — 16 tests total, `schema_ddl_relay.rs`'s 7 tests untouched by design. See the 2026-09-09 "Rung J (post-C-09)" through "Rung J, PR 6 landed" amendments below (including "Rung J closed") and `docs/roadmap.md`'s C-10 entry |
| K | Post-C-10: throttle-metric counters `SimCluster` dispatch (C-11, open) — the smallest of the four groups Rung J's own close-out left unowned (`ThrottledWrites`/`ThrottledReads`, 1 file/6 tests in the D3-closing class-D breakdown, unchanged since). `tests/dynamo_throttling.rs` still holds all 6: `batch_write_item_sheds_throttled_rows_into_unprocessed_items` (:445), `batch_get_item_sheds_throttled_keys_into_unprocessed_keys` (:492), `transact_write_items_cancels_with_throttling_error` (:538), `a_forwarded_write_is_throttled_on_the_leader` (:605), `admin_metrics_reports_nonzero_throttled_counters` (:652), and `cluster_wide_throttle_default_is_overridden_by_a_tables_own_throughput` (:740, a `run_node_with_cluster_settings`-only config-parse test expected to stay a permanent residual). `dynamo::kind_write_item_at_leader<E: Env, R: RelayClient>` and `dynamo::run_transact<E, R>` are already generic and already increment `Metric::ThrottledWrites` via `ctx.data().raftkv_metrics.incr(..)` (`dynamo.rs:9069`, `write_path.rs:327`, `txn_coordinator.rs:134`); the read-path `ThrottledReads` site (`read_path.rs:99`) is generic too. `dispatch_item_op` already routes `BatchGetItem`/`BatchWriteItem`/`TransactWriteItems`. **The rung's own distinguishing finding, corrected from two stale module docs**: `SimCluster::new`/`::restart` build every node with a real `DataRole` (`data: Some(DataRole { raftkv_metrics: node_metrics[i].clone(), .. })`, `sim_cluster.rs:1460`, since rung D2 PR 1) and a `ThrottleTracker` (`:1504`) — `ThrottledWrites`/`ThrottledReads` **do** increment under `SimCluster` today, contradicting `sim_cluster_throttle.rs`'s and `sim_cluster_dynamo_update_table.rs`'s own module docs, both of which said the counters never increment here; this PR corrects both. `SimCluster::set_throttle_defaults`/`::set_throttle_defaults_all` (`sim_cluster.rs:2121`/`:2134`) and `SimCluster::admin` (`:933`) already exist and already drive `GET /admin/metrics` in `sim_cluster_admin.rs`; `metrics_view<E, R>` (`admin.rs:1982`) is generic, dispatched from `animus-node/src/admin.rs:52`. Wire-level sim siblings already exist for the same operation family (`sim_cluster_dynamo_batch_get.rs`, `sim_cluster_kind_batch_outcome.rs`, `sim_cluster_dynamo_transact.rs`). **Not yet sequenced by the maintainer** — this opener proceeds on C-10's own close-out recommendation as a stated assumption (see that amendment below). **Size S.** See the 2026-09-09 "Rung K (post-C-10)" opener amendment below and `docs/roadmap.md`'s C-11 entry |

Note that the copy-based split driver (ADR 0050) is deliberately **not** on
this list: ADR 0058 rung 4's remaining layer deletes it. Writing a corpus
for code slated for removal would be waste — if that deletion slips, it gets
covered then.

#### 2026-09-05 amendment — D1 landed: `SimCluster`, hand-hosted, over a real multi-voter control quorum

D1 is done: `animusd::sim_cluster::SimCluster` (`crates/animusd/src/
sim_cluster.rs`, an in-crate `#[cfg(test)] mod` declared from `lib.rs` —
kept in its own file rather than inline, unlike `simenv_client_ctx_tests`/
`two_node_relay_tests`, purely so `lib.rs` doesn't keep growing, with no
change to the "descendant of the crate root, private fields reachable"
privacy property those two siblings rely on). One `Simulator`, `nodes`
node ids, a real **multi-voter** control `RaftNode<SimEnv>` quorum (every
node id is a voter — `animus-control/tests/control_raft.rs::cluster`'s own
shape, generalizing `two_node_relay_tests`' single-voter/shared-`Arc`
stand-in to N independent voters that actually replicate over the
`Network` seam), a `SimRelayClient<SimEnv>` per node with the rung C3d
generic relayed-request dispatcher installed, and a `ClientCtx<SimEnv,
SimRelayClient<SimEnv>>` per node whose `client_route`/`intra_route` name
every node id by `NodeId::to_string()` up front (rung C3d's address
convention) — since this fixture's whole node set is known at
construction, pre-populating once is sufficient; no
`route_sync_loop`/`intra_route_sync_loop` equivalent was needed.

**Design decisions, as scoped in the eighth 2026-08-28 amendment's own
"what `SimCluster` will need its own answer to" note:**

- **Tablets are hand-hosted, not reconciler-hosted.** `SimCluster::
  create_table` proposes `CreateTableSchema`/`CreateTablet` directly on
  the control group's current leader, then constructs a
  `RaftKvNode<SimEnv, MemoryEngine>` on each chosen replica node directly
  and registers it into that node's own `ClusterEdgeState` — mirroring
  `animus-test::raftkv_linearizable`'s `Group::start`, not
  `animus-cp-data::host::Reconciler`. `Metadata`'s tablet row and each
  hosting node's edge registration are built from the exact same replica
  list in the same call, so they can never disagree — the "as long as
  `Metadata` and the edge agree" bar this ADR's own D1 rung description
  set. A reconciler-hosted `SimCluster` (lifting `animus-cp-data/tests/
  reconciler_corpus.rs`'s `Cluster`/`ClusterNode` shape) is a legitimate
  future rung — it would additionally prove the reconciler's own
  event-driven hosting loop, which this rung's client-path-focused brief
  does not need — but is more machinery than D1 asked for.
- **DDL stays a control-plane-Raft bypass, exactly like every other
  `SimEnv` `ClientCtx` fixture in this crate.** `ClientCtx::propose_schema`'s
  local-propose fast path is still `ProdEnv`-locked (the eighth amendment's
  own finding, unchanged by C3d — C3d only made `propose_schema`'s *relay*
  branches reachable, never its local fast path). `SimCluster` seeds every
  table by proposing `CreateTableSchema`/`CreateTablet` directly on
  whichever control `RaftNode` handle it holds for the current leader —
  the identical bypass `simenv_client_ctx_tests`/`two_node_relay_tests`
  use for their own single- and two-node setups, now against a genuine
  multi-voter quorum. **What remains unexercised**: a `ProposeSchema`
  **relayed** through `ClientCtx` to reach a multi-voter control leader —
  this fixture never calls `ClientCtx::propose_schema` at all, so C3d's
  relay branches for that specific command are still only proven by
  `two_node_relay_tests`' own single-voter setup, not by a real
  multi-voter quorum. A future rung wanting that proof needs either a
  `SimCluster::propose_schema_via_client` entry point or to keep waiting on
  a genuine `ClientCtx`-genericized DDL path (the pre-existing, larger,
  separately-scoped blocker).
- **Restart is a true process restart, on `MemoryEngine`.** `SimCluster::
  restart` mirrors `raftkv_linearizable.rs`'s own `StopRestart` nemesis:
  `Simulator::stop` (drops every task the node owns) followed by fresh
  `RaftNode::start`/`RaftKvNode::start_hosted` calls on the same node id,
  each on a brand-new `MemoryEngine`. Since this fixture only uses
  `MemoryEngine` (matching every sibling `SimEnv` harness in this crate), a
  restart is a wipe-and-rejoin, not a WAL replay — recovery is via
  ordinary peer catch-up / chunked `InstallSnapshot` (the same mechanism
  `animus-cp-data/tests/engine_wipe_needs_snapshot.rs` proves at the
  primitive level). A durable (`LsmEngine`) `SimCluster` tier, proving the
  WAL-replay recovery path this rung's restart does NOT exercise, is a
  natural follow-on mirroring `raftkv_linearizable.rs`'s own two-tier
  design, not built here.
- **What is still `ProdEnv`-only**, unchanged from every prior rung's
  findings: `ClientCtx::propose_schema`'s local-propose fast path (above);
  `SegmentStoreHandle`/`BackupStoreHandle`'s `Cluster` variant (this
  fixture only ever uses the `Fs` placeholder, like `simenv_client_ctx_
  tests`/`two_node_relay_tests` — nothing this fixture drives reads either
  field); and a `DataRole` (`data: None` on every node — no DynamoDB wire
  edge, no TTL reaper, no stream/backup loops; this fixture drives the
  plain `cp_kind_write_raw`/`cp_get`/`cp_scan` client-protocol methods
  only, the same surface the eighth amendment's own harness proved
  single-node).

**The fault surface**: thin wrappers over `Simulator`, mirroring
`raftkv_linearizable.rs`'s own `Nemesis::apply` — `crash`/`restart` (the
distinction above)/`partition`/`heal_all`/`run_for`, plus `leader_of`/
`leader_index_of`/`metadata`/`tablet_of` accessors for assertions. Five
scenarios ship with it (`sim_cluster::tests`, a `#[cfg(test)] mod` nested
inside `sim_cluster.rs`, each seed-parameterized): (1) 3 nodes, RF 3 — a
write on the leader reads back, via a real `ConsistentRead: true`-
equivalent (`consistent: true`) linearizable read, from every node
including a non-leader that must forward over the real `SimRelayClient`
wire (plus a `scan`/`delete` pass over the same write, proving those two
methods too); (2) RF 2 of 3 — a write issued from the one node hosting no
replica at all succeeds through the relay and is readable everywhere; (3)
crash the tablet leader, hold the fault open for an election window,
write through a surviving node, restart the crashed node, and confirm the
whole group converges — a converged-or-timeout retry loop
(`poll_until_get_eq`), never a one-shot assert; (4) a 1-node minority
isolated from the other two cannot ack a write issued from it (a
majority of 3 survives at 2), a write on the majority side succeeds, and
the minority catches up after `heal_all`; (5) a second `create_table`
works after the first, with both tables' schema/tablet visible on every
node and both independently writable/readable (proving the two tablets'
distinct `stream = tablet.0` Raft addressing, ADR 0026 Stage B, never
cross-talks — the exact hazard `animus-test/CLAUDE.md`'s stream-corpus
entry documents for `RaftKvNode::start_scoped`, which is why
`SimCluster::create_table` always uses `start_hosted` with the tablet id
as the stream instead).

**Gates**: `cargo fmt --all`, `cargo clippy --workspace --all-targets
--all-features -- -D warnings`, `cargo test -p animusd --lib` (128 passed,
1 pre-existing ignored), `cargo test -p animus-node` (119 unit + 5 sim, all
green — proof this rung touched nothing in that crate), `cargo test -p
animus-control --test control_raft` (proof the multi-voter control quorum
shape this rung leans on is unaffected), and — as the real-socket
sanity check that production relay stayed untouched —
`cargo test -p animusd --test control_only` and `--test schema_ddl_relay`
(10 tests, all green).

**What commit 3 (a cycles/durability corpus modelled on
`raftkv_linearizable.rs`, gated by `ANIMUS_SIMCLUSTER_SEEDS`) needs that
this rung does not supply**: `sim_cluster.rs` is `#[cfg(test)] mod
sim_cluster;` — compiled only when building `animusd`'s own unit tests,
and reachable from nowhere outside that build (not even `animusd`'s own
`tests/*.rs` integration binaries, which are separate crates that link
against the *library* target, not its `#[cfg(test)]` tree). A `tests/
sim_cluster_corpus.rs` file modelled literally on `raftkv_linearizable.rs`
therefore **cannot** `use animusd::sim_cluster::SimCluster` — that path
does not exist outside `cfg(test)`. The corpus has to be a second in-crate
`#[cfg(test)] mod` instead (e.g. `crates/animusd/src/
sim_cluster_corpus.rs`, declared from `lib.rs` next to `sim_cluster`,
`use super::sim_cluster::SimCluster;`), run via `cargo test -p animusd
--lib` like every other in-crate harness in this file — not a `tests/*.rs`
binary despite the naming precedent `raftkv_linearizable.rs` sets. Beyond
that placement question, the corpus needs: (a) a way to record a
`Recorder`/`History` of `put`/`get`/`delete` operations against
`SimCluster` (the fixture's own `put`/`get`/`delete` return plain
`Result`s today, with no invoke/ok/fail/info hook — the corpus will need
to wrap each call, mirroring `raftkv_linearizable.rs`'s own `client_loop`);
(b) `SimCluster::restart`/`crash` are today only ever called against a
node this fixture itself tracks the tablet-hosting map for — a corpus
driving concurrent client tasks that call `SimCluster` methods from
`env.spawn_task`'d futures will need `SimCluster` (or a thin wrapper
around it) to be safely shareable that way (today every method takes
`&mut self`, fine for this rung's own sequential scenario scripts, but a
concurrent-client corpus needs either an `Arc<Mutex<SimCluster>>` wrapper
or a redesign of the mutable surface — `raftkv_linearizable.rs`'s own
`Nodes<S> = Arc<Mutex<Vec<Arc<Node<S>>>>>` shape is the precedent to
follow); and (c) `SimCluster::create_table`'s hand-hosted design means a
corpus wanting to prove convergence *through* a reconfiguration (a replica
moved, not just crashed/restarted in place) has no primitive to call —
this rung's `restart` always reconstructs a tablet on the SAME node id
with the SAME replica set, never a different one, so a "replica set
change mid-corpus" scenario needs either a new `SimCluster::move_replica`
method or the reconciler-hosted design point noted above.

**No product bug found while building this rung** — every scenario proved
correct on the first fully-wired attempt (the seed-round-trip failures hit
along the way were fixture-construction mistakes: `nid`'s concrete
`"n{n}"` string encoding being round-tripped through `NodeId::to_string()`
+ `.parse::<u64>()` instead of using the plain-index accessor this rung
added specifically to avoid that fragility — `SimCluster::leader_index_of`
alongside the spec-shaped `leader_of -> Option<NodeId>`).

#### 2026-09-05 amendment (2) — D1 step 3 landed: `sim_cluster_corpus`, the SimCluster cycles/durability corpus

D1's own commit-3 residual (the previous amendment's "what commit 3
needs" note) is closed, gaps (a) and (b) both. `crates/animusd/src/
sim_cluster_corpus.rs` (`#[cfg(test)] mod sim_cluster_corpus;`, declared
from `lib.rs` next to `sim_cluster` exactly as that note predicted — not a
`tests/*.rs` binary, since `sim_cluster.rs` compiles only under `cfg(test)`
and is unreachable from a separate integration-test crate) is a
list-append `Recorder`/`History` corpus over the fixture, checked with
`animus_test::check::{check_cycles, check_durability, check_convergence}`
— the identical oracle `raftkv_linearizable.rs` uses, run via `cargo test
-p animusd --lib sim_cluster_corpus`, depth knob `ANIMUS_SIMCLUSTER_SEEDS`
(default 1 = 8 frozen cells, ~14s; nightly tier at 25).

**Gap (b) — making `SimCluster` safely shareable with concurrently
spawned client tasks — closed by `SimClusterHandle`, not the `Arc<Mutex<
SimCluster>>` wrapper the previous amendment's note suggested as the
precedent to follow.** Wrapping the *whole* fixture in one mutex would
have serialized every concurrent client task's op behind a single lock
held for that op's entire (potentially `CLIENT_TIMEOUT`-bounded) duration
— exactly the "several concurrent client tasks" property a corpus needs
would have been defeated by the naive version of the suggested fix. The
actual design instead moves only the two fields client-issued ops ever
read/write (`ctxs: Vec<ClientCtx<..>>`, `tablets: BTreeMap<TabletId,
TabletInfo>`) into a `Clone`-able `SimClusterHandle` (`Arc<Mutex<..>>`
each), whose `put`/`get`/`delete`/`scan` methods take `&self` and briefly
lock only to clone out the target node's own `ClientCtx` before
`.await`ing on the owned clone — never holding the lock across an
`.await`. `SimCluster` itself keeps `sim: Simulator`/`controls: Vec<
RaftNode<SimEnv>>`/`crashed: BTreeSet<u64>` unshared (nothing but the
driver ever touches them), and its own `put`/`get`/`delete`/`scan`/
`restart` now delegate to the identical handle methods. This is a cheaper
answer to "make it shareable" than a single coarse lock, and follows
naturally once the actual read/write surface a concurrent task needs is
named explicitly instead of wrapping the whole struct.

**Gap (a) — the `Recorder`/`History` wrapper — turned out to need no
extra "wrap each call" plumbing beyond that handle**, because `SimCluster::
put`/`get`/`delete`/`cp_kind_write_raw`/`cp_get` are already internally
`CLIENT_TIMEOUT`-bounded (`cp_route`'s own deadline, `forward_to_tablet_
leader`'s own hint-chasing deadline) — a client task can simply `.await`
`SimClusterHandle::put`/`get` directly inside its own spawned loop and
get back `Ok`/a timeout-shaped `Err` well within the corpus's own poll
window, with no wrapper `spawn_and_capture`/`OP_BUDGET` of its own (that
wrapper is still needed, and kept, on `SimCluster`'s own synchronous
`put`/`get`/`delete`/`scan` — the ones a plain, non-spawned test-thread
caller uses, which is exactly what `run_delete_probe`, below, needs).

**Gap (c) (replica-set-change-mid-corpus) was skipped, per the task's own
brief** ("optionally a replica-move primitive, skip unless cheap") — no
`SimCluster::move_replica` was added; every cell in `corpus_cells` uses
`SimCluster::create_table`'s existing hand-hosted, fixed-replica-set
shape.

**`delete` is exercised but deliberately NOT fed into `check_cycles`.**
The shared, cross-crate `Mop`/`History` model (`animus-test::history`) is
list-append-only — every observed read must be a *prefix* of its key's
one recovered order, an invariant a tombstoning `delete` cannot satisfy
without either teaching that model a semantics no other corpus needs or
manufacturing a workload-artifact false positive (the exact class of trap
`raftkv_linearizable.rs`'s own doc names for value reuse). Instead
`run_delete_probe` puts a real DynamoDB API to actual verification
directly and separately: after every scenario's fault schedule heals and
drains, it round-trips put→get(present)→delete→get(absent) **from every
node in the cluster in turn**, so a cell with more nodes than replicas
(`forward_heavy`) proves a forwarded delete, not just a forwarded put/get.

**A real hang, found and fixed while building this rung — not a product
bug, a harness mistake with a lesson that generalizes past this file**:
the first version of `run_delete_probe` called `SimClusterHandle::put`/
`get`/`delete` directly via a bare `futures::executor::block_on`, mirroring
`raftkv_linearizable.rs`'s own `final_state`'s `block_on(node.local_get(..))`
idiom. `local_get` is a pure local-engine read with no internal `.await`
on simulated time, so `block_on` alone resolves it on the very first poll
— sound. `SimClusterHandle::put`/`get`, unlike `local_get`, internally
`.await` real `env.sleep()`-paced route/confirm-poll loops; `block_on`ing
one of those directly, with no `Simulator::run_for`/`run_until` anywhere
concurrently advancing virtual time, hangs **forever** (the future's own
`sleep` never wakes, since nothing is driving `SimEnv`'s cooperative
executor). The very first `cargo test -p animusd --lib sim_cluster` run of
this file never returned. Fixed by driving `run_delete_probe` through
`SimCluster`'s own synchronous `put`/`get`/`delete` (which each spawn +
`run_for` internally, exactly the shape `SimCluster::put`/`get`/`delete`/
`scan` already used before this rung) instead of the handle + bare
`block_on`. **The general rule, added to `docs/engineering-lessons.md`**:
`block_on`ing a `SimEnv`-driven future is only sound when that future is
known to resolve without needing the simulator's own clock to advance
(a pure local read); a future that itself `.await`s `env.sleep()` needs
`spawn_task` + `Simulator::run_for`/`run_until` somewhere in its call
chain, or it hangs silently rather than erroring — and a hang under
`cargo test` with no timeout looks identical to a slow compile until
someone kills it, so this is worth checking explicitly whenever a new
`block_on` call is added against a `SimEnv`-backed future.

**Non-vacuity teeth**: every cell asserts `ok_writes > 0`; a cell with
`nodes > replication` (`forward_heavy`) additionally asserts at least one
acked write was issued from a node hosting no local replica of the
tablet — tracked per-issuing-node in `Shared::ok_writes_by_node`, so a
regression that silently stopped exercising the forward path (not just
one that broke it) would also be caught. `delete_probe.is_ok()` (every
node, including a non-hosting one) is asserted for every cell.

**No product bug found** — every cell passed on the first fully-wired
attempt once the `block_on` hang above was fixed.

**Gates**: `cargo fmt --all`, `cargo clippy --workspace --all-targets
--all-features -- -D warnings` (clean — required one new named type alias,
`SimNodeCtx`, to satisfy `clippy::type_complexity` on `SimClusterHandle`'s
own `Arc<Mutex<Vec<ClientCtx<SimEnv, SimRelayClient<SimEnv>>>>>` field —
the same "name it instead of nesting it inline" convention `ScanRows`
already set in this file), `cargo test -p animusd --lib` (131 passed, 2
pre-existing/new ignored replay entry points), `ANIMUS_SIMCLUSTER_SEEDS=5
cargo test -p animusd --lib sim_cluster_corpus_is_consistent` (40
scenarios, ~70s), and `cargo test -p animus-test --test raftkv_linearizable`
(unchanged, 10 passed / 1 ignored — proof this rung touched nothing in
the model it copied).

#### 2026-09-07 amendment — D2 PR 1 landed: the DynamoDB wire path is drivable from `SimCluster`, first small smoke

D2 PR 1 is done: a DynamoDB JSON request (`X-Amz-Target` + body) decoded by
`animus_dynamo::wire::decode_request` now executes through the exact same
generic core `dynamo::run_operation`'s own item-op arms call in production,
reachable against a `SimEnv`-backed `ClientCtx` inside `SimCluster` for the
first time. **Not yet the full nemesis corpus** — that is PR 2, sized below.

**The genericization, scoped down from "all of `dynamo.rs`'s dispatch" to a
reviewable slice.** `dynamo::run_operation`/`execute_as` themselves stay
concrete (`ClientCtx<ProdEnv, AnimusdRelayClient>`) top-level entry
points — genericizing them wholesale would have meant genericizing every
DDL/backup/export/import/PartiQL/transact handler they also dispatch to
(`create_table`/`update_table`/the backup family/the S3 export-import
family/`execute_statement`/`run_transact`/`run_transact_get`, several of
which are genuinely `ProdEnv`-only — `tokio::spawn` for the export job's own
async task, real `TcpStream` process-boundary code), pushing the signature
count into the 60s and well past a single reviewable PR. Instead, a new
generic function, `dynamo::dispatch_item_op<E: Env, R: RelayClient>`,
holds exactly the six operations that had no such entanglement — `PutItem`,
`DeleteItem`, `GetItem`, `BatchGetItem`, `UpdateItem`, `BatchWriteItem` — as
a **pure move** of their existing bodies (calling the exact same
`ClientCtx::cp_kind_write_item`/`cp_get`/`cp_scan` methods, themselves
already `E`/`R`-generic since rung C5), never a rewrite. A new sibling
entry point, `dynamo::execute_item_op_as<E, R>`, runs that function's own
decode/meta/reject-internal-table/authorize prelude (mirroring `execute_as`
production runs, just against a generic `ClientCtx`) and is the one
`SimClusterHandle::dynamo` calls. `run_operation`'s own arms for those six
operations now delegate to `dispatch_item_op` too (`op @ (Operation::
PutItem { .. } | ... ) => dispatch_item_op(ctx, principal, meta, op)
.await`), monomorphized at `E = ProdEnv, R = AnimusdRelayClient` — so
production behavior for them is byte-identical, not merely equivalent.

**`Query`/`Scan` are the one operation pair with a real, deliberate
duplication, not a delegation.** `dispatch_item_op`'s own `Query`/`Scan`
arms cover only the **base-table** path (`index: None`) — genericizing the
real `run_query`/`run_scan` would also require genericizing their own
`run_index_query`/`run_gsi_query`/`run_lsi_query`/`run_index_scan`/
`run_gsi_scan`/`run_lsi_scan`/`paginated_kind_examine`/
`paginated_kind_examine_one` siblings (the GSI/LSI dispatch tree), which
alone would have pushed this PR's signature count from ~23 into the
mid-30s. **`run_operation`'s own `Query`/`Scan` arms are deliberately NOT
in the delegated set** — they still call the full, unmodified, concrete
`run_query`/`run_scan` (complete GSI/LSI dispatch), exactly as before this
rung. This was found the hard way, not designed correctly the first time:
the first cut of this PR *did* route `run_operation`'s `Query`/`Scan`
through `dispatch_item_op`, and `cargo test -p animusd --lib` caught it
immediately — four `index_drain::gsi_drain_cursor_tests` failures, each
timing out on `"a GSI/LSI Query is not yet supported by the generic
(SimEnv-capable) dispatch path"`, since those tests exercise real GSI
queries through production's own `run_operation`. **The general lesson,
worth restating beyond this file**: when splitting one dispatcher into "a
generic core" plus "a concrete production entry point that also handles
the ProdEnv-only remainder," a narrowed generic core must not become the
sole path an *unrelated, still-full-featured* production caller uses for
the cases the narrowing dropped — verify by running the full existing test
suite for the touched dispatcher, not just the new corpus, before trusting
a "the production call site is monomorphized so it's unaffected" argument
that turns out to be more subtle than it looks (here, `run_operation`
genuinely called the narrowed function directly, not merely the same
generic *machinery* at a different type — the two are not the same
guarantee).

**`ClientCtx::data()`'s panic on a `None` `DataRole`** (ADR 0035 PR3's
control-only-node guard) was the other real gap the eighth/D1 amendments'
own "still `ProdEnv`-only" finding left unresolved for this rung:
`write_path::kind_write_item_at_leader` (already generic since rung C5) and
`dynamo::fast_marker_write`/`authz::record_denied` all call `ctx.data()` on
their hot paths (`raftkv_metrics.incr`/`request_rates.observe`). Every
`DataRole` field turned out to be a plain, `Env`-free, `Default`-able
handle (`MetricsHandle::noop()`, `StreamSealKnobs::default()`,
`ChangeRateTracker`/`RequestRateTracker`, both already `#[derive(Default)]`
— none of the four touch `E` at all), so `SimCluster::new` now builds a
real one per node (`base_id` the node's own id) instead of `data: None` —
cheap, and needs no `ProdEnv`. This was the "construct a SimEnv-safe
`DataRole`" branch of the two options the task brief posed, not the "gate
the reads" one — simpler, and it means every generic item-op handler this
rung and any future one adds can call `ctx.data()` freely without a second
audit.

**`SimClusterHandle::dynamo`/`SimCluster::dynamo`** (mirroring `put`/`get`/
`scan`'s own two-tier async-handle/sync-wrapper split) run a decoded
request against one node's own `ClientCtx`, exactly like `admin.rs::
action_data_dynamo`'s own unauthenticated `execute_routed` proxy — an
unrestricted `Principal`, since this fixture has no SigV4 listener to
resolve a scoped one from.

**The smoke** (`crates/animusd/src/sim_cluster_dynamo.rs`, `#[cfg(test)]
mod sim_cluster_dynamo;` from `lib.rs`, a sibling of `sim_cluster`/
`sim_cluster_corpus`/`sim_cluster_throttle` for the identical privacy
reason): five scenarios, each seed-parameterized with an `ANIMUS_SEED`
replay entry point on the first — PutItem → GetItem(`ConsistentRead:
true`) through the wire on a 3-node RF3 cluster, both issued from a
non-leader node (proving the generic dispatch path forwards over the real
`SimRelayClient` wire); `UpdateItem` with a `ConditionExpression`, both a
satisfied and a failing one (`ConditionalCheckFailedException`, proving
`cp_kind_write_item`'s evaluate-at-leader path, not the unconditioned fast
arm); `Query` over a composite `(pk, sk)` table (proving
`dispatch_item_op`'s base-table `Query` arm returns exactly one partition's
rows); `BatchWriteItem` (proving the `marker_batch_write` single-Raft-
entry-per-tablet path); and one leader crash + write-through-a-survivor +
restart, with a converged-or-timeout wire read from every node afterward.
Run via `cargo test -p animusd --lib sim_cluster_dynamo`; example seed
`0xD2C1_0001`.

**A harness gotcha found building the crash/restart scenario**: a crashed
(muted, not stopped) node's own `is_leader_local` read stays frozen at
whatever it last locally believed — nothing tells a muted node its peers
re-elected, since its inbox is cleared, not its internal Raft state — so
calling `SimCluster::leader_index_of` again right after `crash` + an
election window can return the **crashed** node's own id, not the new
leader's. The fix (already the shape `sim_cluster.rs`'s own scenario 3
uses) is to route the follow-up write through any *survivor* node index
picked without consulting `leader_index_of` at all — `cp_kind_write_item`'s
own `forward_to_tablet_leader` hint-chasing loop finds the real new leader
internally. Added to `docs/engineering-lessons.md`: `leader_index_of`
answers "who does this replica locally believe leads," which is exactly
wrong to ask of a node you just crashed.

**Gates**: `cargo fmt --all --check`; `cargo clippy -p animusd -p
animus-dynamo --all-targets --all-features -- -D warnings` (clean —
`execute_item_op_as`'s only caller is `#[cfg(test)]`-gated, so it carries
the identical precise `#[cfg_attr(not(test), allow(dead_code))]`
`RaftKvNode::local_scan_kind_bounded` already established in this crate);
`cargo test -p animusd --lib` (198 passed, 2 ignored — the two pre-existing
opt-in shrink-replay entry points — including `sim_cluster_corpus` at its
default depth and all five new `sim_cluster_dynamo` scenarios); `cargo test
-p animus-dynamo` (unchanged, proof this rung touched nothing in the wire
codec itself); and, as the real-socket sanity check that production's
`dynamo.rs` dispatch stayed behaviorally unchanged, `cargo test -p animusd
--test dynamo_wire --test dynamo_txn --test dynamo_indexes --test
dynamo_update_add_delete --test dynamo_partiql --test
dynamo_execute_transaction` (58 tests, all green — `dynamo_indexes`' own
`gsi_write_then_query` in particular is the real-socket proof the
`index_drain.rs` regression above is actually fixed, not just no-longer-
caught by a narrower rerun).

**PR 2's plan** (not built here): (a) generic GSI/LSI `Query`/`Scan` —
`run_index_query`/`run_gsi_query`/`run_lsi_query`/`run_index_scan`/
`run_gsi_scan`/`run_lsi_scan`/`paginated_kind_examine`/
`paginated_kind_examine_one` made `<E, R>`-generic, folded into
`dispatch_item_op`'s own `Query`/`Scan` arms in place of today's
`index.is_some()` rejection; (b) `TransactWriteItems`/`TransactGetItems` —
blocked on a **new** proof this crate has never built: `ClientCtx::
propose_schema`'s *relayed* path (the only branch reachable under `SimEnv`,
per the eighth/D1 amendments) successfully auto-provisioning the internal
`__animus_txn_idempotency` table against a genuine multi-voter `SimEnv`
control quorum — `ensure_txn_idempotency_table`'s own `propose_schema` +
commit-wait loop, first exercised by whichever scenario in PR 2 issues a
`TransactWriteItems` with a `ClientRequestToken`; once that path is proven,
`run_transact`/`run_transact_get`/`transact_write_idempotency_preflight`/
`ensure_txn_idempotency_table`/`idempotency_claim_put`/
`idempotency_record_item`/`read_idempotency_record` become generic the same
mechanical way this PR's six operations did; (c) `ExecuteStatement`/
`BatchExecuteStatement`/`ExecuteTransaction` — PartiQL lowers onto (a)/(b)'s
own operations, so these need no new logic once those are generic, only
their own thin generic wrapper; (d) **the actual corpus** — a list-append
`Recorder`/`History` model over `SimClusterHandle::dynamo` (mirroring
`sim_cluster_corpus.rs`'s own `Recorder` over `put`/`get` exactly), checked
with `animus_test::check::{check_cycles, check_durability,
check_convergence}`, the Nemesis matrix `sim_cluster_corpus.rs` already
has (`leader_crash`/`follower_crash`/`stop_restart`/`leader_partition`/
`split_brain`/`forward_heavy`/`two_tables`) generalized to DynamoDB-wire
ops instead of raw `put`/`get`, an `ANIMUS_DYNAMO_WIRE_SEEDS` depth knob,
and a `corpus-deep.yml` nightly tier entry alongside `ANIMUS_SIMCLUSTER_
SEEDS`.

**No product bug found in the six delegated operations themselves** — the
one real bug this rung found (the `Query`/`Scan` production regression
above) was caught before it ever reached a committed state, by the
existing `cargo test -p animusd --lib` run this rung's own gate list
requires anyway.

#### 2026-09-07 amendment — D2 PR 2 landed: the actual end-to-end DynamoDB-wire corpus

D2 PR 2 is done: `crates/animusd/src/sim_cluster_dynamo_corpus.rs`
(`#[cfg(test)] mod sim_cluster_dynamo_corpus;` from `lib.rs`, a sibling of
`sim_cluster_corpus`/`sim_cluster_dynamo` for the identical privacy reason
those two already document) — a list-append `Recorder`/`History` model
over `SimClusterHandle::dynamo` (PR 1's own generic entry point), checked
with the identical `animus_test::check::{check_cycles, check_durability,
check_convergence}` oracle `sim_cluster_corpus.rs` already uses, driven
through the **real** DynamoDB JSON wire (`PutItem`/`GetItem`/`UpdateItem`/
`DeleteItem`/`BatchWriteItem`/base-table `Query`/`Scan`) instead of the raw
`cp_kind_write_raw`/`cp_get` primitives that corpus calls directly. Same 8
cells, same fault matrix, same converged-or-timeout durability/convergence
poll — the fault dimension this rung exercises was already proven by D1;
what's new here is that every op crosses the wire codec
(`animus_dynamo::wire::decode_request`/`dynamo::dispatch_item_op`) first.

**The list-append mapping: a server-evaluated `UpdateItem`, not a
client-tracked list.** `sim_cluster_corpus.rs`'s own write is a plain
`put` of a client-maintained, locally-extended list — sound only because a
raw `put` is an idempotent whole-value overwrite. This corpus's one write
mechanism is instead

```
SET items = list_append(if_not_exists(items, :empty), :v)
```

— a **server-evaluated**, genuinely non-idempotent operation (a
duplicated apply of the same entry would append the appended value twice,
unlike a plain `Put`), which is exactly the `SET`-plus-`list_append`
non-idempotent write the task brief for this rung asked for; no separate
`ADD` op was needed to get that property, since `list_append` already has
it by construction. `if_not_exists(items, :empty)` means the very first
write to a key needs no separate provisioning step.

**The read-consistency modeling decision (ADR 0055).** `GetItem`/`Query`/
`Scan` all decode `ConsistentRead`, and this corpus issues both values —
but only `ConsistentRead: true` observations feed the shared
`Recorder`/`History` `check_cycles` runs against (a linearizable
ReadIndex read is a real, ordered observation, the same discipline
`sim_cluster_corpus.rs`'s own ADR 0055 testing-gotcha entry states).
`ConsistentRead: false` reads are excluded from `check_cycles`'s history
entirely — feeding a replica-local, un-barriered observation into the same
`wr`/`rw` graph a linearizable read builds would manufacture false-positive
"divergence" violations the instant a stale-but-legal read landed during a
fault window, since `check_cycles`'s shared, cross-crate model has no
weaker-read flag to distinguish "legitimately stale" from "actually
forked." Every `ConsistentRead: false` observation is instead recorded
separately and checked directly against the scenario's own **converged
final state** once the fault schedule has healed and drained: each
observed list must be a prefix of the converged state — sound under this
corpus's single-writer-per-key discipline, since a lagging/un-barriered
replica can only ever have observed an *earlier* state of the one writer's
own strictly-ordered commit sequence. This is the same kind of exclusion
`sim_cluster_corpus.rs` already made for `delete` (workload-shape excluded
from a checker whose model doesn't fit, not a defect in either side),
applied to a second dimension the wire corpus newly introduces. No
violation was ever observed across the depth runs below — the exclusion
is a modeling decision proven sound by this corpus's own passing runs, not
an untested hope.

**`DeleteItem`/`BatchWriteItem` are exercised but kept out of
`check_cycles`**, for the identical reason `delete` already was in
`sim_cluster_corpus.rs`: a tombstoning `DeleteItem` cannot satisfy the
list-append prefix invariant, and a `BatchWriteItem` `PutRequest` is a
whole-item overwrite, not an append. Both get their own direct correctness
probes (`run_delete_probe`/`run_batch_write_probe`), run from every node
in the cluster in turn after the fault schedule heals and drains, on key
namespaces (`delete-probe-*`/`batch-probe-*`) disjoint from the
list-append model's own keys.

**Base-table `Query`/`Scan` feed the same history, as multiple `Mop::Read`s
per transaction.** Every table's items live under exactly two fixed
partition keys, so a `Query` (`pk = :p`) returns a real, strict subset of
what a `Scan` returns — genuinely exercising `dispatch_item_op`'s
base-table `Query` arm rather than a `Scan` synonym — and both decode every
returned item into one `Mop::Read`, all recorded as one history entry
(`Recorder::ok` already takes a `Vec<Mop>` for exactly this shape).

**Depth knob**: `ANIMUS_DYNAMO_WIRE_SEEDS` (default 1 = the 8 frozen
cells). Held green at `=25` locally — **default depth ~25s (8 scenarios);
depth 25 ~10m2s wall / 601.29s test-binary time (200 scenarios)** — see the
Gates section below for the exact commands and counts.
`ANIMUS_SEED`/`ANIMUS_SHRINK`/`ANIMUS_SHRINK_REPLAY` wiring
(`sim_cluster_dynamo_shrink_replay`) mirrors `sim_cluster_corpus.rs`'s own
exactly. A structural `dynamo_wire_corpus_covers_the_fault_matrix` guard
(mirroring `sim_cluster_corpus_covers_every_cell_shape`) pins the 8-cell
set and its fault-class/forward-heavy/multi-table coverage. `corpus-deep.
yml` gained a `dynamo_wire` tier entry at the same depth
`ANIMUS_SIMCLUSTER_SEEDS` uses (`=25`), and root `CLAUDE.md`'s env-var
table gained the matching row.

**Residuals, named rather than silently skipped** (identical to D2 PR 1's
own "PR 2's plan" list, now pushed one rung further out): GSI/LSI
`Query`/`Scan`, `TransactWriteItems`/`TransactGetItems`, and PartiQL
(`ExecuteStatement`/`BatchExecuteStatement`/`ExecuteTransaction`) are all
still unreachable through the generic `dispatch_item_op` core this corpus
drives (`execute_item_op_as` returns a clean `InternalServerError` for any
of them) — see `dynamo.rs`'s own `dispatch_item_op` doc for the full
"what's `ProdEnv`-only, why" account. Deferred to D3/D4, not attempted
here; this rung's own scope was "the actual corpus," not widening the
generic dispatch surface further.

**No product bug found.** Every cell held green at both the frozen and
`=25` depths on the first complete run; the `ConsistentRead: false`
prefix check and both direct probes never found a violation either.

**Gates**: `cargo fmt --all --check`; `cargo clippy -p animusd
--all-targets --all-features -- -D warnings` (clean); `cargo test -p
animusd --lib` (green, including `sim_cluster_corpus` and the new
`sim_cluster_dynamo_corpus` at default depth); `ANIMUS_DYNAMO_WIRE_
SEEDS=25 cargo test -p animusd --lib sim_cluster_dynamo_corpus` (24 cells,
green — 200 scenarios, `test result: ok. 3 passed`, `finished in 601.29s`, wall `10m2.300s`); `cargo test -p animusd --test dynamo_wire` (real-socket
sanity, unchanged, green — proof this rung touched no production dispatch
code, only added a new test module).

#### 2026-09-07 amendment — D3 PR 1 landed: the first `ProdEnv`-to-`SimCluster` test conversion, and a corrected success criterion

D3 is "keep the `crates/animusd/tests/` binaries that genuinely prove
real-thread liveness or real-disk durability on `ProdEnv`; convert the
rest to `SimCluster`." A planning pass classified all 120 real-socket
integration binaries (524 tests) into classes; this PR converts only
class **B** — base-table DynamoDB logic tests that `dynamo::
dispatch_item_op` (D2 PR 1's own generic core) can already drive, needing
no widening of that function or `execute_item_op_as`.

**The roadmap's own stated D3 success criterion is stale, corrected
here.** `docs/roadmap.md`'s C-04 entry (and this ADR's own Delivery-plan
table row for D3) framed success as "the `prod-liveness` job shrinks
enough to drop its 2-attempt retry." Checked against `.github/workflows/
ci.yml` directly before starting this rung: there is no 2-attempt retry
to drop — that job was already replaced by nextest sharding
(`prod-liveness-animusd`, 4 partitions by test count, plus
`prod-liveness-hammer-pair` and `prod-liveness-scattered` as separate
parallel jobs) before D3 began, per that workflow's own comments on the
sharding rationale. **D3's real, corrected goal: shrink the real-thread
tier's own test count / wall time / flake surface** — there is no retry
metric left to watch.

**What moved.** Eleven new `#[cfg(test)]` sibling modules in
`crates/animusd/src/` (`sim_cluster_dynamo_batch_get`, `_boolean_
composition`, `_eventual_read`, `_expression_surface`, `_extended`,
`_item_size_cap`, `_parallel_scan`, `_predicate_bugs`, `_update_add_
delete`, `_updated_return_values`, and `sim_cluster_kind_batch_outcome`),
each replacing some or all of one `tests/dynamo_*.rs`/`tests/
kind_batch_outcome.rs` binary — ~30 tests total, driven through
`SimCluster::dynamo`/`SimCluster::dynamo_concurrent` instead of real
sockets/threads. Every converted test preserves its original assertions;
where the original exercised a forwarded/non-leader path, the converted
version keeps at least one op issued from a node that does not host the
tablet's leader. `SimCluster::dynamo_concurrent` (new,
`crates/animusd/src/sim_cluster.rs`) is this PR's own fixture addition —
it spawns several DynamoDB wire requests onto their own nodes' envs
*before* one shared `Simulator::run_for`, so two requests genuinely race
the same key/tablet the way the original tests' `tokio::spawn`/
`tokio::join!` pairs did, rather than resolving one at a time.

**What stayed on `ProdEnv`, and why** (each noted in its own module's
doc, not silently dropped): a wire-level `CreateTable` test
(`dynamo_extended.rs`'s `create_table_query_and_conditional_writes` —
`dispatch_item_op` has no `CreateTable` arm; `SimCluster::create_table`
seeds a table by proposing `CreateTableSchema`/`CreateTablet` directly on
the control leader, bypassing the wire); a `TransactWriteItems` test
(`dynamo_item_size_cap.rs`'s transact half — Transact is still an
unreached D2 residual); and a GSI-query test (`dynamo_update_add_
delete.rs`'s `an_add_that_changes_an_indexed_attribute_reindexes` — GSI/
LSI dispatch is likewise still an unreached D2 residual).

**A second `SimCluster` capability gap found live, distinct from the D2
residuals above**: `dynamo_extended.rs`'s `concurrent_conditional_puts_
one_wins` relies on `dynamo::legacy_register`'s auto-registration of an
unrecognized table on its first `PutItem`, with no `CreateTable` at all.
`ClientCtx::provision_tablet` (the auto-provision every first write to a
brand-new table goes through) picks the tablet's initial replica set from
`Metadata::members` — the node-registration catalog a real deployment's
`MetaCommand::RegisterNode` populates, which `SimCluster::new` never
proposes (its own nodes are wired directly into `ClusterEdgeState`, never
registered into `Metadata`). A legacy-registered table's auto-provision
therefore mints a tablet with an **empty** replica set that nobody ever
hosts, and the write times out waiting for a group that will never form.
Worked around, not fixed: the converted test uses `SimCluster::
create_table` (the fixture's own supported path, which picks replicas
directly) instead of relying on legacy auto-registration — a fixture
data-shape change (a real `pk`/`sk` composite schema instead of a legacy
one), not a change to what the race itself proves. Widening
`SimCluster::new` to also register nodes into `Metadata::members` is left
for a future fixture PR, not attempted here.

**A parallel, read-only redundancy audit** (run independently of this
classification pass) found two more `tests/dynamo_*.rs` tests that
duplicated existing sim coverage outright, with no rewrite needed:
`dynamo_wire.rs::dynamo_wire_put_get_delete_round_trip` (proven by
`sim_cluster_dynamo.rs::put_then_consistent_get_through_wire_from_a_
non_leader_node`) and `dynamo_throttling.rs`'s
`put_item_is_throttled_once_the_write_budget_is_exhausted`/`get_item_is_
throttled_once_the_read_budget_is_exhausted` (proven by `sim_cluster_
throttle.rs`'s `write_admits_a_burst_then_refuses_then_recovers_after_a_
full_refill`/`read_admits_a_burst_then_refuses_then_recovers_after_a_
full_refill`). Folded into this same PR's removal commit; every other
test in both files stays (each proves something no sim test covers —
fresh combined-mode `Status.control_voters`, `UnprocessedItems`/
`UnprocessedKeys` shedding, forwarded-write throttling, the throttled-
metric counters, `CreateTable`/`UpdateTable`/`DescribeTable` wire ops).

**Two commits, in equivalence order**: commit 1 adds every new sim test
with the `ProdEnv` files untouched (so a reviewer can diff the sim tests
directly against their `ProdEnv` originals before anything is deleted);
commit 2 deletes/trims the now-redundant `ProdEnv` tests and updates this
ADR, `docs/roadmap.md`, and `crates/animusd/CLAUDE.md`.

**Gates**: `cargo fmt --all --check`; `cargo clippy -p animusd
--all-targets --all-features -- -D warnings` (clean); `cargo test -p
animusd --lib` (green both before and after the removal commit, 240
passed — the same count either side, since only test *location* moved);
`cargo build -p animusd --all-targets` (green — proves no deleted file is
still referenced); `cargo test -p animusd --test dynamo_extended --test
dynamo_item_size_cap --test dynamo_update_add_delete --test dynamo_wire
--test dynamo_throttling` (the five partially-edited `ProdEnv` binaries,
green); a converted test replayed with an explicit `ANIMUS_SEED`,
confirming determinism; `Cargo.lock` unchanged.

**Left for the fixture PR the task named, not attempted here**: widening
`SimCluster`/`dispatch_item_op` for GSI/LSI query dispatch, wire-level
`CreateTable`, and `TransactWriteItems` — the three capability gaps every
"stayed on `ProdEnv`" test above is blocked on — plus the `Metadata::
members` registration gap this PR's own workaround sidesteps. D3's
remaining test classes (DDL beyond plain `CreateTable`, Streams, TTL,
admin/console/dashboard HTTP, TLS, SigV4, restart-durability, wall-clock
timing) are unclassified-by-this-PR follow-on work.

#### 2026-09-07 amendment — D3 PR 2a landed: base-table DDL drivable through `SimCluster` over the real DynamoDB wire, and two real findings that had to be fixed first

PR 2a is "make base-table DDL — `CreateTable`/`DeleteTable`/`ListTables`/
`DescribeTable`, deliberately **without** `UpdateTable` (PR 2b) —
drivable through `SimCluster` over the real DynamoDB wire codec." It
closes the `Metadata::members` gap D3 PR 1's own amendment named as "left
for the fixture PR the task named."

**`Metadata::members` population (`sim_cluster.rs`,
`SimCluster::seed_members`)**: for every node, `SimCluster::new` now
proposes `MetaCommand::RegisterNode { addrs: NodeAddrs { role: "combined",
.. }, .. }` (role **must** be `"combined"` — `RegisterNode`'s own
`claims_membership = addrs.role != "control"` gate, `animus-control::
meta`, never inserts into `members` for a control-only registration) then
`MetaCommand::UpsertMember { status: Active, .. }` (`RegisterNode` alone
inserts `Down`), converged-or-timeout polled the same shape
`create_table_with_replication`'s own tail already used. Confirmed against
`animus-control`'s own source, not assumed: `reconcile_placement`/
`rebalance_placement` iterate `Metadata::policies`, never `tablets`
directly, and `create_table_with_replication`'s hand-hosted tablets never
attach a policy — so this population is provably inert for every
pre-existing hand-hosted scenario. `cargo test -p animusd --lib` holds its
exact pre-PR pass count (240) with this change alone, before any new test
is added.

**`ClusterEdgeState<E>::control` widened `Arc<Mutex<Vec<RaftNode<
ProdEnv>>>>` → `Arc<Mutex<Vec<RaftNode<E>>>>`** (`register_control`/
`leader_handle` widened to match), and `SimCluster::new` now calls
`ctxs[i].edge.register_control(controls[i].clone())` for every node.
Before this, `ClientCtx::propose_schema`'s local-propose fast path
(`self.edge.leader_handle()`) was structurally `ProdEnv`-only regardless
of the enclosing `ClientCtx<E, R>`'s own `E`: under `SimEnv` `control` was
always empty, so `leader_handle()` always answered `None` and **every**
schema proposal — even one issued on the node genuinely leading the
control group — took the relay branch, which under `SimEnv` meant
relaying to **itself**; `forwarding::handle_relayed_request`'s own
`ProposeSchema` arm re-resolves the leader the identical way and
re-relays, recursing until the caller's own timeout. This is what D2 PR 1
called "never yet exercised." The widening restores the real leader-local
fast path and, for the first time, makes the non-leader one-hop relay
branch genuinely exercised (a leader-issued call no longer takes it at
all) — proved directly by `sim_cluster_dynamo_table_ops.rs`'s
`create_table_issued_on_a_control_follower_relays_and_converges`/
`delete_table_through_a_follower_connected_node_is_relayed_to_the_leader`,
both of which issue their DDL against a node `control_leader_index` does
not currently name, so the relay branch is the *only* branch that can
possibly succeed there.

**One real production-code blocker this widening surfaced, fixed in
`animus-env` (not `animusd`)**: `ClientCtx::admin_add_control_member`
(`lib.rs`) calls `leader.env().merge_peer(..)` — `ProdEnv::merge_peer` is
an inherent method with no trait equivalent, and before this widening
`leader: RaftNode<ProdEnv>` was concrete regardless of the enclosing `E`
(the same escape hatch this whole PR closes), so the call compiled
incidentally. Fixed by adding a **default no-op** `Env::merge_peer(&self,
_id: NodeId, _addr: String) {}` to the trait itself (`animus-env/src/
lib.rs`) — the identical "additive default, no existing implementor
changes" shape `Env::metrics()` already established — with `ProdEnv`
overriding it to delegate to the pre-existing inherent method (Rust's
inherent-impl method-resolution priority means the delegation call
inside the trait impl reaches the inherent method, not itself). `SimEnv`
needs no override: it has no peer-book concept to begin with, so a no-op
is the *correct* behavior there, not a stand-in. `cargo clippy -p
animus-env --all-targets --all-features -- -D warnings` and `cargo test
-p animusd --test dynamo_table_ops --test dynamo_schema --test
dynamo_extended --test schema_ddl_relay --test create_table_ready` (the
real `ProdEnv` binaries) both stayed green, confirming this is additive
only.

**`dynamo::dispatch_table_op<E: Env, R: RelayClient>`** (new, `dynamo.rs`)
is `dispatch_item_op`'s DDL sibling, covering exactly four operations:
`CreateTable` (base table only — a declared GSI/LSI or a stream is
rejected with the identical `unsupported_by_generic_dispatch` shape
`dispatch_item_op`'s own `Query`/`Scan` arms use for a named index),
`DeleteTable`, `ListTables`, `DescribeTable`. Five functions widened to
`<E: Env, R: RelayClient>` to let it compile and to keep `run_operation`'s
own production dispatch calling them monomorphized: `create_table`,
`enable_stream` (compiles generically though unreached via the generic
path today — a `CreateTable` carrying a stream is rejected before ever
calling it that way; `update_table`'s own unmodified `ProdEnv`-only call
site is unaffected), `mint_stream_label`, `delete_table`, `describe_table`.
The ~9 `tokio::time::Instant::now()`/`tokio::time::sleep` sites inside
`create_table`/`enable_stream` converted to `ctx.env.now().saturating_add(
..)`/`ctx.env.sleep(..)`, the identical rung-C5-step-3b shape — no
`use animus_env::EnvExt;` needed (`Clock`, whose methods these are, was
already imported in `dynamo.rs`, and its methods are reachable through the
`E: Env` bound regardless). `run_operation`'s own `CreateTable`/
`DescribeTable`/`DeleteTable`/`ListTables` arms stay **byte-for-byte**
unchanged, still calling those five functions directly, never
`dispatch_table_op` — the D2 lesson ("a narrowed generic split of a
dispatcher must not become the production dispatcher's ONLY path")
deliberately repeated rather than re-derived. `execute_item_op_as`
(`SimCluster`'s own entry point) now routes the four DDL operations to
`dispatch_table_op` and everything else to `dispatch_item_op`, in one
`matches!`-gated branch ahead of the existing call.

**Two real findings, both fixed in `sim_cluster.rs`, neither in the
original design pass — found empirically, exactly as this rung's own
brief anticipated might happen, and both load-bearing for wire
`CreateTable` to work under `SimCluster` at all:**

1. **The design pass's claim "a directly-activated member cannot flip
   back to `Down`" is wrong for the liveness detector** (it is correct
   only for the *orphan sweep*, a different mechanism gated on
   `has_activated`). `RaftNode::start` spawns `detect_loop`
   unconditionally (not something `animusd` opts into) — its "phantom-
   member hardening" (ADR 0030) gives an `Active`-but-untracked member
   exactly one **synthetic** `FailureDetector::observe` the first tick it
   sees one; with no *real* heartbeat ever following (production nodes
   heartbeat via `animusd`'s own `BoundNode`-spawned loop, which
   `SimCluster` never ran), that synthetic timestamp ages out after
   `DETECT_TIMEOUT` (500ms) like any other and the member is judged dead.
   Confirmed live: every member seeded by `seed_members` flipped to
   `Down` well before a second wire `CreateTable` call (each burning
   `OP_BUDGET` = 12s of virtual time) ever reached `provision_tablet`,
   which then found zero `Active` candidates and spun until its own
   commit-wait deadline. **Fixed** by spawning `animus_control::node::
   heartbeat_loop` (the exact loop a real deployment runs) on every node
   in `SimCluster::new`, re-spawned on `SimCluster::restart` too (a
   restarted node's `Simulator::stop` drops its previous instance along
   with everything else it owned).
2. **A pre-existing, previously-unreachable tablet-id collision between
   this fixture's two independent allocators.** `create_table_with_
   replication` (the hand-hosted path) minted tablet ids from its own
   fixture-local counter (`SimCluster::next_tablet_id`, starting at 1),
   entirely independent of `Metadata::next_free_tablet_id()` (the
   allocator `ClientCtx::provision_tablet`, the wire path, already reads
   fresh every attempt). Every `SimCluster` test before this rung used
   *either* path, never both in the same cluster, so the two counters
   never had occasion to collide. This rung's own `list_tables_sorts_
   paginates_and_excludes_gsi_hidden_tables` test is the first to mix
   them (three wire-created tables, then one hand-hosted one for the
   GSI-hidden-name check) and reproduced it immediately: the hand-hosted
   call proposed `TabletId(1)`, already claimed by the first wire-created
   table, was rejected ("tablet already exists"), and `create_table_
   with_replication`'s own `poll_until` timed out at 5s. **Fixed** by
   deleting the fixture-local counter outright and deriving `create_table_
   with_replication`'s own tablet id from `self.controls[leader].
   metadata().next_free_tablet_id()` — the identical live allocator the
   wire path already uses — so the two paths can never disagree again.

**The reconciler-hazard investigation (item 5), and its real, deterministic
finding.** Once `Metadata::members` is populated and a wire-provisioned
table's tablet carries a real placement policy (`SetTabletPolicy`,
`provision_tablet`), the control leader's own `reconcile_loop`/
`rebalance_placement` — unconditional, spawned by `RaftNode::start`
itself — is live over it, while `SimCluster`'s own tablet-hosting
mechanism (`spawn_policy_tablet_host_loop`, this rung's own minimal
`Reconciler` stand-in, added so `await_table_serveable` has a real group
to find at all) only ever **adds** a replica newly named in
`Metadata.tablets[t].replicas`, never tears down one a `CasTabletReplicas`
just dropped. Investigated with a dedicated 4-node (`node_count >
MAX_REPLICATION_FACTOR = 3`, so one member is always left un-provisioned
and every table's fixed-RF-3 pick is genuinely imbalanced) cluster,
`sim_cluster_dynamo_table_ops.rs::
reconciler_hazard_fires_deterministically_when_node_count_exceeds_
replication`: **it fires, deterministically, on every one of 25 seeds
tried** — the exact same two `CasTabletReplicas` moves every time (table
1's tablet `n0`/`n1`/`n2` → `n1`/`n2`/`n3`; table 2's `n0`/`n1`/`n2` →
`n0`/`n2`/`n3`; a third table created after those two already balance
every member at 2 tablets apiece never moves). This is
`rebalance_placement` doing exactly its documented job — an entirely
correct, ordinary control-plane decision, not a bug in `animus-control` —
and the real, uncovered bug is the `SimCluster` fixture gap it exposes: a
dropped-but-not-torn-down `RaftKvNode` left on the node the CAS removed,
genuinely split-brain-shaped (two different-membership `RaftKvNode`s for
one tablet id, no fault injection needed), reachable with a plain
`CreateTable` whenever `node_count > MAX_REPLICATION_FACTOR`. **Left as a
documented finding, not fixed here**: a correct fix needs `SimCluster` to
grow an actual `Reconciler`-shaped teardown mechanism, materially more
fixture machinery than this PR's own "base-table DDL drivable through the
wire" brief asks for — ADR 0061 rung D1's own module doc already named a
reconciler-hosted `SimCluster` as "a legitimate future rung," not this
one. The practical mitigation for every test in this file and any future
one: keep `node_count <= MAX_REPLICATION_FACTOR` (3) for any scenario that
issues a real wire `CreateTable` — every other regression in this module
does exactly that (1- or 3-node clusters only) and is unaffected. See
`docs/engineering-lessons.md`'s matching entries for both fixes and this
finding, and `docs/roadmap.md`'s C-04 entry for the follow-up this leaves
named.

**Tests converted** (`crates/animusd/src/sim_cluster_dynamo_table_ops.rs`,
new sibling module): `tests/dynamo_table_ops.rs` whole (all three tests —
`list_tables_sorts_paginates_and_excludes_gsi_hidden_tables`,
`delete_table_removes_it_and_a_repeat_delete_is_not_found`,
`delete_table_through_a_follower_connected_node_is_relayed_to_the_leader`),
`dynamo_schema.rs::create_table_rejects_reserved_namespace`, and
`dynamo_extended.rs::create_table_query_and_conditional_writes` (its own
sibling test had already moved in D3 PR 1, leaving this the file's only
test — emptying it, so the file is deleted rather than left as an empty
shell). Two new tests beyond the direct conversions:
`create_table_issued_on_the_control_leader_converges`/`create_table_
issued_on_a_control_follower_relays_and_converges`, the direct proof pair
for item 2's own widening. Every `dynamo_schema.rs` test beyond the one
converted (restarts, GSI index tests, `extended_surface`) is untouched, per
this task's own explicit instruction.

**Gates**: `cargo fmt --all --check`; `cargo clippy -p animusd
--all-targets --all-features -- -D warnings` and `cargo clippy -p
animus-env --all-targets --all-features -- -D warnings` (both clean);
`cargo test -p animusd --lib` — 240 before this PR's changes (member
population + control widening alone), 247 after the new module lands (7
new tests, zero regressions, ~68-70s wall); `cargo build -p animusd
--all-targets` (green, proves no deleted file is still referenced);
`cargo test -p animusd --test dynamo_table_ops --test dynamo_schema
--test dynamo_extended --test schema_ddl_relay --test create_table_ready`
— run in full **before** commit B (18 tests, all green — production DDL
behavior byte-identical after the widening) and the four surviving
binaries (`dynamo_table_ops.rs`/`dynamo_extended.rs` deleted, `dynamo_
schema.rs` trimmed by one test) re-run green after; an `ANIMUS_SEED`
replay of a converted test confirmed deterministic; `Cargo.lock`
unchanged.

**Deferred to PR 2b, unchanged from this rung's own brief**:
`UpdateTable` in any shape (stream/index/throughput changes, `TagResource`/
`UntagResource`, `UpdateTimeToLive`) — `dynamo_throttling.rs` (its own
`UpdateTable`-provisioned-throughput coverage) is untouched.

#### 2026-09-07 amendment — D3 PR 2b landed: `UpdateTable`'s throughput change drivable through `SimCluster`

PR 2b is "`UpdateTable`'s **throughput-only** change (ADR 0065's
`BillingMode`/`ProvisionedThroughput`) drivable through `SimCluster` over
the real DynamoDB wire, and the matching `dynamo_throttling.rs` tests
converted" — the half of `UpdateTable` PR 2a's own `dispatch_table_op`
doc named as deferred, deliberately still without a stream or index
change (unchanged scope cut from PR 2a — those need the GSI-drain/
stream-sealer machinery this rung does not generalize).

**`dynamo::update_table_throughput` widened to `<E: Env, R: RelayClient>`**
— the identical `enable_stream`/`create_table` shape PR 2a already used:
its three `tokio::time::Instant::now()`/`tokio::time::sleep` sites became
`ctx.env.now().saturating_add(..)`/`ctx.env.sleep(..)`. `update_table`
itself (and its other two branches' callees — `enable_stream`/
`disable_stream`/`create_index`/`drop_index`) stays completely untouched,
still `ProdEnv`-only, still calling `update_table_throughput` at its one
existing call site (now simply monomorphized, exactly as `create_table`'s
five widened callees already were in PR 2a).

**`dynamo::dispatch_table_op` gained a fifth arm, `UpdateTable`** —
narrower than `update_table`'s own three-way dispatch: it decodes
`Operation::UpdateTable { table, stream, index_update, throughput_update,
.. }` and only ever proceeds when both `stream` and `index_update` are
`None` (mirroring `update_table`'s own `(None, None, Some(spec))` match
arm exactly) and `throughput_update` is `Some(spec)`, calling
`update_table_throughput(ctx, &table, spec)` then re-describing the table.
A stream or index change (or no change at all — unreachable via the wire
decoder, but handled explicitly rather than assumed, the same defensive
posture `update_table`'s own catch-all arm already takes) returns the
identical `unsupported_by_generic_dispatch` shape `dispatch_item_op`'s own
excluded operations use. `execute_item_op_as`'s routing `matches!` gained
`Operation::UpdateTable { .. }` alongside the four PR 2a operations, so
every `UpdateTable` call reaches `dispatch_table_op` — including the ones
that fall through to `unsupported_by_generic_dispatch` inside it, never
`dispatch_item_op`'s own catch-all (whose message would have been
identically worded but a layer removed from the real reason).

**Tests converted** (`crates/animusd/src/sim_cluster_dynamo_update_table
.rs`, new sibling module of `sim_cluster_dynamo_table_ops.rs`): five of
`dynamo_throttling.rs`'s eleven tests —
`create_table_with_provisioned_throughput_throttles_without_any_admin_
call`, `update_table_to_pay_per_request_lifts_the_limit`, `update_table_
raising_units_admits_more`, `describe_table_reports_billing_mode_and_
throughput`, `update_table_throughput_on_a_follower_is_relayed_to_the_
leader`. Every assertion carried over unchanged in *kind* (an admission/
refusal outcome, a rendered `DescribeTable` shape, a converged per-table
throughput spec on every node) — none references a metric counter, so
none was blocked by `sim_cluster_throttle.rs`'s own documented gap
(`ThrottledWrites`/`ThrottledReads` never incrementing under `SimCluster`,
since every metric-recording site gates on `self.data.as_ref()` and this
fixture's `DataRole` — real since D2 PR 1 — never populates the specific
counters those two tests check). The `ProdEnv` original's own real-wall-
clock converged-or-timeout retry in `update_table_raising_units_admits_
more` (`ThrottleBucket::set_rate` refills at the OLD rate through the
moment of the change, so the first post-raise check still pays that
reassignment) becomes a bounded loop of further `SimCluster::dynamo`
calls — no `run_for`/sleep needed between attempts, since each call
already advances the cluster's own virtual clock by `OP_BUDGET` (12s).
The reconciler-hazard invariant (PR 2a item 5) is checked on the one
3-node scenario (`update_table_throughput_on_a_follower_is_relayed_to_
the_leader`) the identical way PR 2a's own follower-relay tests check it;
the four single-node scenarios don't need it (no second node for
`rebalance_placement` to move a replica onto). One `ANIMUS_SEED` replay
confirmed deterministic.

**No new `SimCluster` fixture bugs found this time** — both fixes PR 2a
needed (the member-liveness heartbeat gap, the tablet-id-allocator
collision) were sufficient; `UpdateTable`'s own commit-wait shape is
byte-identical to `CreateTable`'s, so nothing new was exercised structurally.

**One other `ProdEnv` test grepped and deliberately left alone**:
`crates/animusd/tests/auto_split_min_tablets.rs`'s `UpdateTable` call
raises a table's declared throughput to grow ADR 0067's derived minimum
tablet count — but the test's own subject is that background trigger's
real-thread behavior (a live per-tick auto-split loop forking a real CP-
data tablet group, `converged-or-timeout` polled against real wall time),
not `UpdateTable`'s own wire mechanics. `SimCluster` hand-hosts tablets
(ADR 0061 rung D1's own design choice — no real `animus_cp_data::host::
Reconciler`, no live auto-split loop) and has no analog for either half of
what this test actually proves, so it is out of `dispatch_table_op`'s
reach regardless of how far this rung widens the DDL core — a D4-shaped
gap (deterministic auto-split coverage), not a D3 one.

**Gates**: `cargo fmt --all --check`; `cargo clippy -p animusd
--all-targets --all-features -- -D warnings` (clean); `cargo test -p
animusd --lib` — 252 passed (247 before this PR's own change, +5 new,
zero regressions, ~68s wall, 3 ignored throughout); `cargo build -p
animusd --all-targets` (green); `cargo test -p animusd --test dynamo_
throttling --test schema_ddl_relay --test update_table_create_index
--test update_table_drop_index` — run in full **before** commit B (the
last two prove the full `update_table` dispatcher, GSI/LSI included, is
byte-identical) and the surviving, five-test-smaller `dynamo_throttling`
re-run green after; `Cargo.lock` unchanged.

#### 2026-09-07 amendment — D3 PR 3a landed: GSI/LSI `Query`/`Scan` dispatch through `SimCluster`, plus `CreateTable` with a declared GSI/LSI

PR 3a is "GSI/LSI `Query`/`Scan` dispatch through `SimCluster`, plus
`CreateTable` with a declared GSI/LSI" — the D2 PR 1 residual
(`dispatch_item_op`'s own doc named `run_index_query`/`run_gsi_query`/
`run_lsi_query`/`run_index_scan`/`run_gsi_scan`/`run_lsi_scan`/
`paginated_kind_examine`/`paginated_kind_examine_one` as "deferred to PR
2/3") and the D3 PR 2a residual (`dispatch_table_op`'s `CreateTable` arm
rejecting any declared index).

**The widening was mechanical, exactly as the design pass predicted**: all
eight functions' only `ProdEnv`-binding was their concrete `ctx: &ClientCtx`
parameter (`ClientCtx<E = ProdEnv, R = AnimusdRelayClient>`'s default type
parameters) — every callee (`ctx.cp_scan_kind`/`ctx.cp_scan_kind_table`,
`paginated_table_examine`, `table_known`, `mirror_catalog_schema`) was
already `<E: Env, R: RelayClient>` since rung C5/D2 PR 1. `run_index_query`/
`run_index_scan` themselves dispatch to their own GSI/LSI siblings by the
index's replicated `kind`, so widening the leaf pair alone would have left
the dispatcher above them stuck concrete — all eight were widened together
in one pass, `<E, R>` propagated to `ctx: &ClientCtx<E, R>` and nothing
else changed in any signature or body.

**`dispatch_item_op`'s `Query`/`Scan` arms** replace the `index.is_some()`
→ `unsupported_by_generic_dispatch` rejection with a branch mirroring
`run_query`/`run_scan`'s own dispatch exactly: `mirror_catalog_schema`
first, then `Some(index) => run_index_query(..)`/`run_index_scan(..)`,
falling through to the unchanged `run_base_query`/`run_base_scan` call when
no index is named. `run_operation`'s own `Query`/`Scan` arms are
**untouched**, still calling the full, concrete `run_query`/`run_scan` —
the D2 PR 1 lesson ("a narrowed generic split of a dispatcher must not
become the production dispatcher's ONLY path for cases the narrowing
dropped") deliberately repeated rather than re-derived a third time.
`ConsistentRead: true` against a GSI is still rejected the identical way
`run_index_query`/`run_index_scan` already reject it (a `Global` index
kind check inside those functions, unchanged) — `dispatch_item_op`'s own
arms add no new logic here, they only route to the function that already
had it.

**`dispatch_table_op`'s `CreateTable` arm** deletes only its
`!indexes.is_empty()` rejection, keeping the stream rejection unchanged.
`create_table` (already `<E, R>`-generic since PR 2a) proposes each
declared index's `CreateTableIndex` and waits for it to replicate exactly
as it always did for the `ProdEnv` production path — nothing about index
creation itself was `ProdEnv`-bound; the rejection was a pure scope guard
from PR 2a's own "base table only" brief, not a real capability gap.

**A genuine `SimCluster` capability boundary, found live and left
undisguised rather than worked around**: `create_table` proposes a
`CreateTableIndex` schema-catalog entry, but never a `CreateTablet` for the
index's own hidden `<base>$<index>` table — that hidden tablet is
materialized lazily, the first time `index_drain::change_consumer_loop`
(the GSI drain background loop) actually drains a row into it. `SimCluster`
never spawns that loop (ADR 0061 rung D1's own "hand-hosted, not
reconciler-hosted" design), so under this fixture **a GSI's hidden table
never gets a tablet at all**, at any depth — not "the rows are stale," but
"the table itself doesn't exist yet, forever." `run_gsi_query`/
`run_gsi_scan`'s own `!meta.has_table_tablet(&index_table)` gate (added
originally as the "drain hasn't run yet" empty-read fallback for
`ProdEnv`) is therefore **always** true here, and a GSI `Query`/`Scan`
always reads back `Count: 0` — pinned by a new positive regression,
`sim_cluster_dynamo_table_ops.rs::gsi_query_reads_empty_under_the_fixture_
until_the_drain_generalizes`, deliberately worded to flip the day a future
rung generalizes the drain (a widened `drain_tablet` plus a fixture
`drain_gsi` helper, the shape a follow-on PR would need — not attempted
here).

**A second-order consequence of the same gate, found converting
`cross_index_cursor_mismatch_is_rejected`**: the empty-page gate runs
*before* `validate_query_cursor_shape`, so a `Query` issued *against* the
GSI (`IndexName: by-cat`) short-circuits to an empty `200` before its
`ExclusiveStartKey` is ever inspected — regardless of the cursor's shape.
The `SimCluster` version of this test (`sim_cluster_dynamo_query_
pagination.rs`) therefore drops the original's "a base cursor replayed
against the GSI" sub-case; the other three directions (a GSI or LSI cursor
replayed against the base table; a GSI cursor replayed against the LSI)
all reach `run_base_query`/`run_lsi_query`, whose own `has_table_tablet`
gate is on the **base** table (always hosted here), so they convert
cleanly and are kept. This is a narrower gap than "GSI rows aren't
materialized" — even a row-free cursor-shape check is unreachable from
that one direction — and is documented in the test's own doc rather than
silently narrowed.

**Tests converted** (42 across nine new `sim_cluster_dynamo_*` sibling
modules in `crates/animusd/src/`, each replacing some or all of one
`tests/dynamo_*.rs` binary — see each module's own doc for the exact
mapping and which original tests stayed on `ProdEnv`):
`sim_cluster_dynamo_query_filter.rs` (5 of 6, from `dynamo_query_
filter.rs`), `sim_cluster_dynamo_query_pagination.rs` (5 of 6, from
`dynamo_query_pagination.rs`), `sim_cluster_dynamo_query_range.rs` (4 of 5,
from `dynamo_query_range.rs`), `sim_cluster_dynamo_scan_index_forward.rs`
(6 of 8, from `dynamo_scan_index_forward.rs`), `sim_cluster_dynamo_
consistent_read.rs` (1 of 1, `dynamo_consistent_read.rs` deleted whole),
`sim_cluster_dynamo_select.rs` (6 of 7, from `dynamo_select.rs`),
`sim_cluster_dynamo_consumed_capacity.rs` (7 of 7,
`dynamo_consumed_capacity.rs` deleted whole — capacity is computed from the
catalog's index definitions plus the written item, never a materialized
index row, so no GSI-drain boundary applies to any of these), `sim_cluster_
dynamo_item_collection_metrics.rs` (5 of 5, `dynamo_item_collection_
metrics.rs` deleted whole — LSI-scoped and priced synchronously at the
tablet leader, same reasoning), `sim_cluster_dynamo_indexes.rs` (2 of 3,
from `dynamo_indexes.rs` — `gsi_write_then_query` stays, per D2 PR 1's own
instruction that it is never converted, being the real-socket proof that
`run_operation`'s independent path works). Every converted test kept its
original assertions and issues at least one op from a non-leader-hosting
node where the original exercised a forwarded path.

**Gates**: `cargo fmt --all --check`; `cargo clippy -p animusd
--all-targets --all-features -- -D warnings` (clean); `cargo test -p
animusd --lib` — 252 before this PR's own change, 294 after (42 new, zero
regressions, ~110s wall, 3 ignored throughout; `gsi_drain_cursor_tests`
stays green, the D2 PR 1 regression net); `cargo build -p animusd
--all-targets` (green); `cargo test -p animusd --test dynamo_gsi_drain
--test dynamo_indexes --test update_table_create_index --test update_
table_drop_index --test index_backfill` (unchanged binaries, green) plus
every `tests/dynamo_*.rs` file this PR trims or deletes — run in full
**before** commit B against the widened `dynamo.rs` (37 tests across the
eight files, all green, confirming production behavior is byte-identical
after the widening) and the nine survivors (`dynamo_consistent_read.rs`/
`dynamo_consumed_capacity.rs`/`dynamo_item_collection_metrics.rs` deleted,
six other files trimmed) re-run green after, at their expected smaller test
counts; two `ANIMUS_SEED` replays (one converted LSI test, one converted
base test) confirmed deterministic; `Cargo.lock` unchanged.

**Deferred to PR 3b, unchanged from this rung's own brief**: generalizing
`index_drain::change_consumer_loop`/`drain_tablet` so `SimCluster` can
materialize a GSI's own hidden table — the boundary this PR's own new
regression pins. `TransactWriteItems`/`TransactGetItems` and PartiQL
remain unreached D2 residuals, untouched by this PR.

#### 2026-09-07 amendment — D3 PR 3b landed: GSI rows materialize under `SimCluster` on demand, closing PR 3a's own boundary

PR 3b closes the exact gap PR 3a's own amendment named: a GSI's hidden
`<base>$<index>` table never got a tablet under `SimCluster` because
nothing in that fixture ever ran `index_drain::change_consumer_loop`'s
GSI-drain arm. Two changes, both minimal in scope:

**Two signatures widened, no behavior change.** `index_drain::
drain_tablet` (now `pub(crate)`, so a sibling module can call it) and its
private helper `reconcile_partition` both go from a bare `ctx: &ClientCtx`/
`group: &CpGroup` (the crate's `ProdEnv`/`AnimusdRelayClient` default type
parameters, elided) to `<E: Env, R: RelayClient>(ctx: &ClientCtx<E, R>,
.., group: &CpGroup<E>, ..)`. Neither function's body needed any change —
every callee they use (`group.cursor_min_watermark`/`pending_changes`/
`local_get_kind`/`local_scan_bounded`/`scope_range`,
`ctx.provision_tablet`/`cp_kind_write_raw`/`cp_kind_write_raw_once`) was
already `<E: Env>`/`<E: Env, R: RelayClient>`-generic, the same "only the
concrete parameter type was `ProdEnv`-binding" shape PR 3a's own eight
functions had. Proven behavior-identical, not just believed to be: the
full pre-existing real-socket regression net for both functions
(`dynamo_gsi_drain.rs`, every `tests/dynamo_*.rs` file this PR goes on to
trim or delete, `update_table_create_index.rs`/`update_table_drop_index.rs`/
`index_backfill.rs`) was run in full against the widened code before any
`ProdEnv` file was touched, and passed unchanged (see Gates, below).
`change_consumer_loop` itself is untouched — only its two callees widened;
the loop's own five-arm structure, quiescence veto, and the other four
arms (seal/PITR-seal/backfill-seeder/hot-trim) are out of this PR's scope,
exactly as the brief said.

**`SimCluster::drain_gsi(&mut self, node: u64, table: &str)`** (new,
`sim_cluster.rs`) is the fixture-side half: a test-only stand-in for
`change_consumer_loop`'s GSI-drain arm, not a second implementation of it.
For every tablet `node`'s own `ClusterEdgeState::hosted_groups()` both
hosts *and* leads whose `Metadata` row names `table`, it recomputes `gsis`
the identical way the production loop does (`meta.table_indexes(table)`
filtered to `IndexKind::Global` with status `Creating`/`Active`) and calls
[`index_drain::drain_tablet`] once, then drives the simulator (via the
crate's existing `spawn_and_capture` idiom, `OP_BUDGET` = 12s of virtual
time) until every resulting `cp_kind_write_raw` call — the GSI row
writes/deletes and the trailing cursor write — has actually committed.
Two of the production loop's own guards are replicated by hand (the
leader check; the hidden-index-table name skip); two are **not**, because
they are structurally unreachable under this fixture rather than merely
untested: `is_quiesced()` always answers `false` (`SimCluster` never calls
`RaftKvNode::enable_quiescence`), and no tablet here is ever `Building`
(this fixture never splits a table). Both omissions are stated in the
method's own doc, not silently assumed — a future rung that gives
`SimCluster` real quiescence or splitting would need to add them back.

**A second, small, unplanned fixture fix was needed to make `drain_gsi`
usable at all**: `SimClusterHandle::leader_index_of` used to resolve a
tablet's leader by scanning only `SimCluster::create_table_with_
replication`'s own hand-hosted-table bookkeeping (`self.tablets`, populated
by exactly one call site) — every table `drain_gsi`'s own callers create is
created through the real DynamoDB wire instead
(`cluster.dynamo(0, "..CreateTable", ..)`, PR 2a/3a's own path), which
never populates that map at all, so `leader_index_of` always answered
`None` for one. Fixed by scanning every node id (`0..node_count`) instead
— strictly more general, and no less correct for a hand-hosted table
either, since `is_leader_local` already answers `false` for a node hosting
no replica of the tablet regardless of which set the caller iterates. See
`docs/engineering-lessons.md`'s matching entry.

**The boundary test flips, as PR 3a's own doc promised it would.**
`sim_cluster_dynamo_table_ops.rs::gsi_query_reads_empty_under_the_fixture_
until_the_drain_generalizes` is renamed `gsi_query_materializes_rows_
after_a_drain` and inverted: `CreateTable` with a declared GSI, `PutItem`,
confirm the hidden index table has **no** tablet and the query reads
`Count: 0`, then `drain_gsi`, confirm the hidden table now **has** a
tablet, then confirm the same query returns the row. `cross_index_cursor_
mismatch_is_rejected`'s dropped fourth sub-case ("a base cursor replayed
against the GSI") is restored in `sim_cluster_dynamo_query_pagination.rs`
now that a real tablet lets the cursor-shape check run before the
empty-page gate would have masked it.

**Every GSI-*data* test PR 3a's own sibling modules had to leave on
`ProdEnv` converts**: `sim_cluster_dynamo_query_filter.rs`
(`filter_applies_to_a_gsi_query`), `sim_cluster_dynamo_query_pagination.rs`
(`gsi_query_paginates_with_the_scan_cursor_shape`, plus the restored
cursor sub-case above), `sim_cluster_dynamo_query_range.rs`
(`gsi_range_queries_over_mixed_digit_count_n_sort_keys`), `sim_cluster_
dynamo_scan_index_forward.rs` (`descending_applies_to_a_gsi_query`,
`gsi_scan_index_forward_orders_n_sort_keys_numerically`), `sim_cluster_
dynamo_select.rs` (`count_select_applies_to_a_gsi_query`) — six tests
across five existing sibling files, each now fully converted (their own
`tests/dynamo_*.rs` source deleted). Three more conversions needed new
homes: `dynamo_documents.rs`'s all three tests (`document_set_types_
projection_and_return_values`, `multiple_gsis_composite_gsi_and_lsi`,
`n_partition_key_routes_and_reads_correctly` — only the middle one
actually touches a GSI) move to a new `sim_cluster_dynamo_documents.rs`;
`dynamo_update_add_delete.rs`'s last remaining test
(`an_add_that_changes_an_indexed_attribute_reindexes`) moves into the
existing `sim_cluster_dynamo_update_add_delete.rs`, draining twice (once
for the pre-update baseline, once after the `ADD`-driven reindex) rather
than polling; `dynamo_schema.rs`'s `create_table_index_replicates_to_
second_node` moves to a new `sim_cluster_dynamo_schema.rs` (that file's
other two tests, the restart proof and `extended_surface`, stay — the
former needs real WAL durability `SimCluster`'s `MemoryEngine` tier can't
give, the latter drives `TransactWriteItems` and other operations
`dispatch_item_op` doesn't reach yet). Finally, per this rung's own
instruction, `dynamo_indexes.rs::gsi_write_then_query` — D2 PR 1's
real-socket proof that `run_operation`'s own path works independently of
`dispatch_item_op` — is **not** deleted; `sim_cluster_dynamo_indexes.rs`
gains `gsi_write_then_query_sim`, a sim twin proving the identical write/
query/delete/reject sequence through the generic core instead.

**Files fully converted and deleted** (`crates/animusd/tests/`):
`dynamo_query_filter.rs`, `dynamo_query_pagination.rs`, `dynamo_query_
range.rs`, `dynamo_scan_index_forward.rs`, `dynamo_select.rs`, `dynamo_
documents.rs`, `dynamo_update_add_delete.rs` — seven files. `dynamo_
schema.rs` is trimmed (one of three tests removed); `dynamo_indexes.rs` is
untouched.

**Gates**: `cargo fmt --all --check`; `cargo clippy -p animusd
--all-targets --all-features -- -D warnings` (clean); `cargo test -p
animusd --lib` — 294 before this PR, 306 after (12 new: table_ops's
boundary test flip is a rename with no count change, +1 pagination, +1
query_filter, +1 query_range, +2 scan_index_forward, +1 select, +1
update_add_delete, +3 documents, +1 schema, +1 indexes; zero regressions,
~115s wall, 3 ignored throughout; `gsi_drain_cursor_tests` stays green);
`cargo build -p animusd --all-targets` (green); `cargo test -p animusd
--test dynamo_gsi_drain --test dynamo_indexes --test update_table_
create_index --test update_table_drop_index --test index_backfill` plus
every one of the eight `tests/dynamo_*.rs` files this PR trims or
deletes — all thirteen run in full **before** commit B against the widened
`index_drain.rs`, green (confirming production behavior is byte-identical
after the widening), and the survivors (`dynamo_schema.rs`'s remaining
four tests, the five unchanged binaries) re-run green after; `ANIMUS_SEED`
replay of two converted GSI tests (`sim_cluster_dynamo_table_ops::
gsi_query_materializes_rows_after_a_drain`, `sim_cluster_dynamo_update_
add_delete::an_add_that_changes_an_indexed_attribute_reindexes`) at six
seeds each, all deterministic; `Cargo.lock` unchanged.

**Still deferred, unchanged from PR 3a's own residual list**:
`TransactWriteItems`/`TransactGetItems` and PartiQL remain unreached D2
residuals. `SimCluster`'s own reconciler-hazard gap (a rebalanced-away
replica's `RaftKvNode` is never torn down, PR 2a's own finding) is
likewise untouched — every test in this PR stays at `node_count <= 3`.
This closes the D3 rung's own GSI-drain boundary in full; the remaining
`tests/dynamo_*.rs` binaries left on `ProdEnv` are there for genuine
real-thread-liveness or not-yet-generic-operation reasons, not the drain
gap.

#### 2026-09-07 amendment — D4 PR 1 landed: `SimCluster` hosted by the real `Reconciler`, closing issue #715

D4 PR 1 closes the gap PR 2a's own amendment named and every subsequent
D3 PR left untouched: `SimCluster` no longer hand-hosts tablets, or hosts
a wire-provisioned one through a minimal add-only watcher — every node
runs a real `animus_cp_data::host::Reconciler`, the exact production
mechanism, so a replica a rebalance drops is actually torn down.

**What replaced what, all in `sim_cluster.rs`.** `spawn_policy_tablet_
host_loop` (D3 PR 2a's own stand-in — hosted a policy-carrying tablet's
newly-named replica, never reacted to one dropped) is deleted outright.
Two new functions, `build_reconciler`/`spawn_reconciler_loop`, replace
it: `build_reconciler` constructs a fresh `Reconciler<SimEnv,
MemoryEngine>` for one node — an `on_host` closure registering a fresh
hosting into that node's `ClusterEdgeState` (mirroring `BoundNode::
start_with`'s own production `on_host` closure exactly) and an
`on_teardown` closure unregistering one — and `spawn_reconciler_loop`
drives it, racing `ctx.control.metadata_watch().changed(last_seen)`
against a fixed `RECONCILER_FALLBACK` (50ms) sleep, coalescing to the
freshest observed index, then calling `tick` once — the identical
event-driven-with-fallback shape `animusd::tablet_host_reconciler_loop`
uses in production, minus that function's `fork_wake()` race arm and
`last_applied() == 0` pre-recovery guard (this fixture never splits a
tablet, and `SimCluster::new` already settles the control group's first
election before any caller can reach the loop — see `spawn_reconciler_
loop`'s own doc for why both are structurally unreachable here rather
than merely untested). `SimCluster::new` builds one such reconciler per
node, each with its own fresh `MemoryTabletEngines` registry (a new
`SimCluster::engines: Vec<MemoryTabletEngines>` field, index == node id).

**`create_table_with_replication` no longer constructs a `RaftKvNode`
directly.** It now proposes `CreateTableSchema`/`CreateTablet` exactly as
before, plus a new `MetaCommand::SetTabletPolicy` — the signal every
node's real reconciler hosts a tablet off of, mirroring `ClientCtx::
provision_tablet`'s own shape but recording the caller's own `replication`
argument as the policy's target RF rather than the wire path's fixed
`MAX_REPLICATION_FACTOR`, which preserves this method's pre-existing
"exactly N replicas, on nodes `0..N`" contract for a caller wanting a
different factor than a real `CreateTable` would pick. The method then
waits (converged-or-timeout, the same shape it already used) for every
node's own reconciler to discover and host the tablet, instead of hosting
it synchronously by hand. Both this fixture's own hand-hosted tables and
a wire-issued `CreateTable`'s tablet are now discovered and hosted purely
off `Tablet.replicas.contains(&base_id)` plus policy-presence — the SAME
reconciler loop, the SAME code path, for both.

**`restart` purges every stale edge registration and rebuilds a fresh
reconciler, reusing the node's own engine registry.** The old restart
rebuilt a `RaftKvNode` by hand only for tablets `SimCluster`'s own
hand-hosted-table bookkeeping (`TabletInfo`) knew about — which never
covered a wire-created table at all. The new restart instead iterates
`ctx.edge.hosted_groups()` (every tablet id this node's edge has ANY
handle for, regardless of origin — exhaustive, since `Simulator::stop`
just dropped every one of those driver tasks) and unregisters each, then
builds a fresh `Reconciler` via `build_reconciler`, passing `self.
engines[node]` — the SAME `MemoryTabletEngines` handle this node was
built or last restarted with, not a fresh one. **This is a deliberate
behavior change**: the pre-D4 restart always built a brand-new
`MemoryEngine::new()` for each rehosted tablet (a true wipe-and-rejoin);
reusing the registry instead mirrors `crates/animus-cp-data/tests/
reconciler_corpus.rs::Cluster::crash_restart`'s own "a durable engine
(`LsmEngine` in production) survives a process crash" modeling, so a
restarted node's own tablet data is no longer wiped — recovery still
proceeds via ordinary peer catch-up/chunked `InstallSnapshot` either way,
a caught-up engine simply needs less of it. Every existing crash/restart
scenario (`sim_cluster.rs::tests::
crash_leader_write_through_survivor_then_restart_converges` and its
seed sibling) stayed green through this change with no test edit needed —
both restart models converge to the identical final state, which is all
those scenarios assert.

**Production signature count: zero.** The read-only pass that scoped this
PR flagged `ClusterEdgeState::unregister_raftkv` as a possible small
method to add "if no unregister exists next to `register_raftkv`" — it
already existed (added by an earlier rung for exactly this fixture's own
restart path), so no production code in `animusd`, `animus-cp-data`, or
any other crate needed to change at all. This PR is a pure `animusd`-
internal test-fixture change.

**No existing scenario's behavior changed.** A rebalance only ever fires
when a table's own tablet-hosting counts fall outside ADR 0029's max−min
≤ 1 balanced band, and every pre-existing `SimCluster`/corpus cell's own
`nodes`/`replication`/`tables` shape stays inside it: a single table's
initial placement (R replicas get 1, N−R get 0, so max−min = 1 exactly)
or `two_tables`' identical-replica-set pair (both tables on all 3 of 3
nodes, uniform). So the real reconciler's own `reconcile_loop`/`rebalance_
placement` never had anything to move for any pre-existing scenario, and
`SimClusterHandle::replicas_of`'s own static creation-time snapshot
(still populated exactly as before, still read by `sim_cluster_corpus.rs`/
`sim_cluster_dynamo_corpus.rs`) never went stale. Confirmed empirically,
not just argued: `cargo test -p animusd --lib` — 306 passed / 118.4s wall
before this rung, 307 passed / 113.4s wall after (net +1: this rung's own
+2 new tests, −1 renamed away, zero regressions in between — wall time
within ordinary run-to-run noise of the baseline, see the fallback-tuning
paragraph below for why it isn't higher); `cargo test -p animus-cp-data
--test reconciler_corpus --test inplace_split_reconciler` — unchanged,
green (this rung touches no `animus-cp-data` source at all).

**One tuning pass was needed to get there.** `RECONCILER_FALLBACK`
(`sim_cluster.rs`, the interval `spawn_reconciler_loop` falls back to when
`metadata_watch()` doesn't wake it) started at 50ms — matching
`SimCluster::poll_until`'s own convergence-check step, on the reasoning
that a reconciler should react at least that often. That reasoning turned
out not to matter for correctness (every real hosting decision is driven
by `metadata_watch()`'s own near-instant wake on an actual commit,
regardless of the fallback's length) but mattered a great deal for cost: a
corpus scenario's own `SETTLE`/fault-window/`DRAIN` sequence spans several
seconds of virtual time, and every node pays for a full reconciler tick
(`gather_facts` + `plan`) every 50ms of it whether or not anything
changed. Measured directly: `ANIMUS_SIMCLUSTER_SEEDS=3 cargo test -p
animusd --lib sim_cluster_corpus::sim_cluster_corpus_is_consistent --
--nocapture` ran ~3s/scenario at 50ms (vs. ~1.75s/scenario pre-D4-PR-1,
extrapolated from the corpus's own documented ~14s/8-cells-at-depth-1
baseline) — at `ANIMUS_SIMCLUSTER_SEEDS=10` (80 scenarios) that would be
several minutes, real but not a hang, confirmed by letting one run to
~3.5 minutes of steady CPU/memory growth before killing it to investigate
rather than waiting it out blind. Widened to 200ms (still 2.5x more
responsive than production's own 500ms `RECONCILE_FALLBACK_INTERVAL`) cut
that to ~2.1s/scenario and brought the full `cargo test -p animusd --lib`
wall time down from 133.5s to the 113.4s reported above — i.e. within
noise of not having changed at all. The general lesson: a fixture's own
"make it react fast" polling constant, copied from an unrelated caller's
own convergence-check cadence rather than derived from what the polled
thing actually needs to react to, is worth measuring before shipping —
especially once it's driven inside every long `run_for` window a corpus
multiplies by scenario count and seed depth.

**The one scenario that DOES rebalance, by design, gets a name change and
a flip.** `sim_cluster_dynamo_table_ops.rs::reconciler_hazard_fires_
deterministically_when_node_count_exceeds_replication` (D3 PR 2a's own
characterization: a 4-node cluster, RF 3, three wire-created tables — the
first two get rebalanced onto the idle fourth node, the third, created
once already balanced, doesn't move) is renamed `every_node_hosts_
exactly_its_replica_set_after_rebalance`: it still drives the identical
rebalance (still asserted, so the check below is non-vacuous — a
scenario where nothing ever moved would prove nothing about teardown),
but now additionally asserts every node's own `hosted_tablets()`
(`ClusterEdgeState::hosted_groups()`'s tablet-id set — a new
`SimClusterHandle`/`SimCluster::hosted_tablets` accessor) converges,
within a 10s budget, to EXACTLY the tablets whose current `Metadata`
replica set names that node — no zombie group (a stale handle for a
dropped replica) and no missing host. Run at the original pinned seed
(`0xE4AC_0000`) plus a new sibling test looping ten more
(`_over_seeds`, seeds `0xE4AC_1000..0xE4AC_1009`) — all eleven green.
`sim_cluster_dynamo_table_ops.rs::
create_table_issued_on_a_control_follower_relays_and_converges` was
separately bumped from 3 to 4 nodes (still RF 3, still balanced — one
table, one idle node) as this rung's own proof that the `node_count <= 3`
restriction PR 2a's finding forced on every OTHER real-wire `CreateTable`
scenario no longer applies either.

**A general "no zombie groups" invariant was added to `sim_cluster_
corpus.rs`'s end-of-scenario checks** (`check_no_zombie_groups`, a new
`ScenarioResult::no_zombie_groups: Result<(), String>` field, checked
unconditionally in `assert_scenario_ok`/`scenario_failed` alongside
`cycles`/`durability`/`convergence`/`delete_probe` — the identical "safety
property, fault or not" discipline `animus-control`'s own corpus doc
states for its schema-catalog-exclusivity check) — non-vacuous only
against a future cell that introduces a genuine rebalance (none does
today, per the paragraph above), but cheap to keep on unconditionally so
a future cell addition gets the coverage automatically rather than by
remembering to add it.

**What remains open for D4 (PRs 2-5), unchanged from before this PR**:
auto-split's own byte trigger needs `auto_split_loop`'s `tokio::time`
conversion before it can run under `SimEnv` at all; GC reclaim is already
a `HostAction::Reclaim` the reconciler this PR wires in can execute —
what's missing is a `drop_table` driver reachable from this fixture, not
reconciler machinery; join/growth needs `SimCluster` to gain an add-node
capability (today's `nodes` count is fixed at construction); the backup
janitor needs `client_ctx_host.rs`'s impls widened the same way this PR's
own `dispatch_item_op`/`dispatch_table_op` predecessors widened the item/
DDL paths. None of these four needed anything from this PR beyond the
real reconciler now being present to build on.

**Gates**: `cargo fmt --all --check`; `cargo clippy -p animusd
--all-targets --all-features -- -D warnings` (clean, zero new warnings);
`cargo build -p animusd --all-targets` (green); `cargo test -p animusd
--lib` (306 before → 307 after, see above); `ANIMUS_SIMCLUSTER_SEEDS=10
cargo test -p animusd --lib sim_cluster_corpus` and `ANIMUS_DYNAMO_
WIRE_SEEDS=10 cargo test -p animusd --lib sim_cluster_dynamo_corpus`
(both green at depth); `cargo test -p animus-cp-data --test reconciler_
corpus --test inplace_split_reconciler` (unchanged, green — proof this
PR's own animusd-side wiring exercises `Reconciler`/`MetadataView`
exactly as that crate's own corpus already does, with no drift);
`Cargo.lock` unchanged.

#### 2026-09-07 amendment — D3 closed: CI re-baseline and the residual `tests/*.rs` inventory (rung D3 PR 4)

D3 PR 4 is the closing PR named by its own row above: no source change,
CI re-baseline plus documentation. It re-measures D3 end to end (base
2c5e6c7a pre-D3, compared against 630782b7 — D3 PR 3b's own commit, the
last D3 PR before D4 PR 1 started touching `sim_cluster.rs` for an
unrelated reason), moves the deterministic sim tier out of the real-thread
CI tier it had been riding inside since PR 1, and inventories what is left
in `tests/*.rs` by why it's still there.

**Measured before/after.**

| Metric | Pre-D3 (`2c5e6c7a`) | Post-D3 (`630782b7`) | Δ |
|---|---|---|---|
| `crates/animusd/tests/*.rs` files | 120 | 100 | −20 |
| `crates/animusd/tests/*.rs` test fns | 521 | 418 | −103 |
| `sim_cluster*.rs` modules (`src/`) | 5 | 29 | +24 |
| `sim_cluster*.rs` test fns | 35 | 140 | +105 |
| `cargo test -p animusd --lib` | 240 | 306 (307 after D4 PR 1) | +66 (+67) |
| Net crate coverage (`--lib` + `tests/`) | 556 | 558 | +2 |

**The reframed success criterion (D3 PR 1's own correction) is verified,
not just repeated.** PR 1 corrected the roadmap's stale "drop the
`prod-liveness` retry" criterion to "shrink the real-thread tier's own
test count / wall time / flake surface" — checked directly against
`.github/workflows/ci.yml` before that PR started, confirming the retry
was already gone. The measured table above is that criterion's own
verdict: the real-thread `tests/*.rs` tier shrank by test count (521 →
418, −20%; 120 → 100 files, also −20%) while the deterministic tier it fed
grew 4x by module count (5 → 29) and 4x by test count (35 → 140) — exactly
the shape "convert real-thread ProdEnv coverage to deterministic SimCluster
coverage" predicts, not an accident of which tests happened to move.

**CI shard wall time**: `prod-liveness-animusd`'s slowest of its four
shards went from 598s (run 34103555943, pre-D3) to 564s (run 34123273726,
at D3 PR 3a — the closest available run to D3's midpoint) — a real but
modest drop, because each shard rebuilds `-p animusd` from a cold cache
(`cache-targets: false` in every job in this workflow) and that compile
alone sits under a floor of roughly five minutes regardless of how many
tests run afterward; per-shard spread was 231–303s across the four
partitions. **The partition count stays at 4, stated explicitly with the
numbers**: going to 3 or 2 partitions concentrates more of the (shrinking)
execution time and the (fixed) compile floor onto each remaining shard —
it raises the max shard wall time, it does not lower it. This is the same
"measure the fixed cost before touching a matrix" lesson this PR's own
`docs/engineering-lessons.md` entry generalizes (see below).

**The sim tier moves into `gates`.** Since PR 1, `cargo test -p animusd
--lib` ran only inside the `prod-liveness-animusd` shards
(`--lib --tests`), meaning every one of the +66 deterministic
`sim_cluster*` tests D3 added executed exclusively inside the real-thread
tier — a sim regression there would report as a `prod-liveness-animusd`
failure, indistinguishable at a glance from the real-thread flake surface
that tier exists to isolate, and would run at `--test-threads=1` (nextest's
`ci` profile) for no reason a deterministic test needs. This PR moves it:
`gates` gains its own `cargo test -p animusd --lib --locked
-- --test-threads=2` step (the `--test-threads=2` matching that job's
existing `--exclude animusd` step, for the identical 2-vCPU-runner
reason), and the four `prod-liveness-animusd` shards drop to
`--tests` only. `gates`'s own test step
(`cargo test --workspace --exclude animusd --locked
-- --test-threads=2`) never ran this tier before — this PR is what closes
that gap. Checked directly: nothing else in the workflow names `--lib` for
animusd (the hammer-pair job runs one named `--test` binary; the scattered
job never touches animusd at all), and nextest's `count:${{
matrix.partition }}/4` recomputes its balance from whatever targets are
passed, so dropping `--lib` from the shards' own command line needs no
further change there.

**`--lib` is not perfectly pure, and this PR says so rather than
overclaiming.** A handful of `#[cfg(test)] mod`s inside `--lib` predate D3
entirely and are genuine single-node `ProdEnv` bring-ups —
`confirm_futility_tests`/`forward_transport_failure_tests`/
`halted_shutdown_tests`/`issue_412_tests`/`issue_298_conflict_tests` in
`lib.rs`, `stream_write_path_tests` in `dynamo.rs`,
`gsi_drain_cursor_tests`/`stream_sealer_tests` in `index_drain.rs`,
`orphan_reap_tests` in `segment_janitor.rs`, `system_table_tests` in
`admin.rs`. `cargo test --lib` builds and runs one target with no way to
split it further, so these move into `gates` alongside the sim tests
rather than staying stranded in a shard that otherwise has nothing left to
shard for `--lib`. Judged acceptable rather than deferred: each is a
small, single-node bring-up (a `TcpListener`, a one-node `run_node`, no
multi-node election timing) — a materially lighter shape than the
multi-node cluster tests `tests/*.rs` holds, which is what `gates`
originally excluded `animusd` over (issues #280/#286). If this step turns
out to be a source of flakes, the fix named in `ci.yml`'s own comment is a
nextest `-E` filter narrowing `--lib`'s real-thread residue back out, not
a retry.

**`gates` job wall time**: 474s → 499s (+25s) *before this PR's own
`--lib` step existed* — that number reflects only clippy/build growth from
D3's added source (more code to typecheck and compile in a job that never
ran animusd's tests before), not this PR's own new test step, which this
worktree has no cargo to measure; the next CI run on this PR is the first
real number for it.

**Residual `tests/*.rs` inventory (100 files / 418 tests), by class.** A
read-only pass at 630782b7 classified every remaining file:

| Class | Files/Tests | Why it's still `ProdEnv` | Owning rung |
|---|---|---|---|
| A: real-thread liveness/timing | 10/13 | genuine OS-thread timing (election, group commit, lock races) — `SimEnv` proves logic and ordering, not liveness (root `CLAUDE.md`'s Testing section) | stays `ProdEnv` permanently |
| B: real-disk durability/restart | 9/24 | real fsync/crash-recovery proof on real disk | stays `ProdEnv` permanently |
| C: real crypto/DNS/TLS/OTLP/sockets | 9/32 | TLS handshake, DNS resolution, SigV4 crypto, OTLP export, raw frame/socket boundaries | stays `ProdEnv` permanently |
| D: waiting on a `SimCluster` capability | 65/317 | see breakdown below | mixed, see breakdown |
| E: frozen behind an open flake issue | 7/32 | #298, #418, #592, #601, #610, #619/#622, #627 — must not be touched incidentally while those issues are open | tracked by their own issues, out of C-04's scope |

Class D's own breakdown, each figure `files/tests`, with the rung that
owns closing it: admin/console/dashboard HTTP 10/66, PartiQL 2/37,
join/growth/decommission 9/34, Transact 6/32, index DDL beyond plain
`CreateTable` 9/30, backup/PITR/export/import 6/29, Streams 3/28,
control/data role split 5/21, reconciler-driven split/rebalance/GC 7/13,
TTL 1/9, node assembly/raw `ClientRequest` 2/8, throttle metric counters
1/6, auto-split loops 2/2, `--config` bring-up 2/2. **D4** already supplies
the mechanism (a real per-node `Reconciler`, landed by D4 PR 1) that
reconciler-driven split/rebalance/GC, auto-split loops,
join/growth/decommission, and backup/PITR/export/import (the backup
janitor's own async loop) all need next — D4 PRs 2-5's own scope, per that
rung's table entry. **D2's own named residuals** own Transact and PartiQL
directly — `dispatch_item_op`/`dispatch_table_op` were deliberately scoped
around them from PR 1 onward. **Unowned as of this close**: admin/console/
dashboard HTTP, Streams, TTL, the control/data role split, `--config`
bring-up, index DDL beyond plain `CreateTable` (an extension of D3's own
`dispatch_table_op`, not claimed by any planned rung), node
assembly/raw `ClientRequest`, and the throttle metric counters (this
fixture's `DataRole` never populates the two specific counters those tests
check, per `sim_cluster_throttle.rs`'s own doc — a fixture gap, not a
missing dispatch arm). None of these eight groups has a rung against it
today; the next C-04-shaped rung that wants one should start here rather
than re-deriving the classification.

**Gates**: `cargo fmt --all --check`; `python3 -c 'import yaml,sys;
yaml.safe_load(open(sys.argv[1]))' .github/workflows/ci.yml` (this
worktree carries no cargo at all — see this PR's own commit message for
why — so the actual `gates`/`prod-liveness-animusd` runs are unverified
here; the next CI run on this PR's branch is the first real signal).

### Phase E — the untested crates

| Rung | Work |
|---|---|
| E1 | `animus-operator`: a fake-kube-client harness and tests for `controller.rs` — `reconcile`, `apply_children`, `control_nodes_changed`, and the ADR 0032-driven `drain_and_remove_node` scale-down sequencing, which is precisely the stateful, ordering-sensitive logic this codebase otherwise insists gets a fault-injected test |
| E2 | `animus-cli`: argument/dispatch coverage for its 741 currently-untested lines |

#### 2026-09-04 amendment — E1 landed: a `ClusterApi`/`AdminOps` seam, not `kube`'s own mock service

E1 is done. `controller.rs`'s two live-cluster boundaries — the `kube::Api`
calls (`ConfigMap`/`Service`/`NetworkPolicy`/`StatefulSet` apply/get,
`AnimusCluster` status patch) and the admin-port HTTP calls
(`AdminClient`'s drain/status/remove) — are now two small `#[async_trait]`
traits, `cluster_api::ClusterApi` and `admin_client::AdminOps`, each with
exactly the handful of operations `controller.rs` actually performs.
`RealClusterApi`/`AdminClient` are the production implementors (unchanged
behavior — `RealClusterApi` issues the identical `kube::Api` calls
`controller.rs` used to make inline); `fakes::FakeClusterApi`/
`FakeAdminClient` (`#[cfg(test)]` only) are small in-memory record-and-serve
stores. `Context`, `reconcile`, `apply_children`, `control_nodes_changed`,
`drain_and_remove_node`, and `error_policy` are all generic over `C:
ClusterApi, A: AdminOps` now, monomorphized at `run()`'s call site to the
real implementors and at each test's call site to the fakes — the same
`E: Env`-style generic-over-a-trait shape the rest of the workspace uses,
just with two small leaf traits instead of one big seam, since this crate
has no `Env` and never will (its own `CLAUDE.md`'s "No `Env` seam here"
gotcha).

**Trade-off actually taken: a hand-written trait, not `kube`'s own
`tower_test`-backed mock `Client`.** The brief for this rung offered both;
a hand-written trait was chosen for three reasons found while scoping, not
assumed going in:

1. `controller.rs` has **two** live-cluster boundaries, not one — the
   `kube::Api` calls and a hand-rolled `hyper` client to a pod's admin port
   (`admin_client.rs`, deliberately not built on `kube::Client` — see that
   module's own doc for why). A `tower_test` mock `Client` would only ever
   cover the first; the admin-port drain sequence still needs *some* seam,
   so the "avoid a trait" saving is partial at best. Having decided E1
   needs a trait for the drain sequence regardless, giving `ClusterApi` the
   same shape rather than a `kube`-specific mock keeps both boundaries
   uniform and both fakes equally cheap to read.
2. `kube`'s mock service intercepts at the HTTP-request level — a test
   would have to match on real Kubernetes REST paths/verbs/content-types
   (`PATCH .../configmaps/{name}?fieldManager=...` with
   `application/apply-patch+yaml`, `PATCH .../status` as a JSON merge
   patch, `GET` with 404-vs-empty-body `Option` semantics) and hand-encode
   canned responses as wire JSON. A `ClusterApi` trait call is already
   typed at the exact granularity `controller.rs` reasons about ("apply
   this `ConfigMap`", "get this `StatefulSet` or `None`"), so the fake
   never needs to reconstruct Kubernetes' own wire conventions to be
   correct — there is strictly less protocol-shaped test-fixture code to
   get subtly wrong.
3. Recording "the fake's recorded applies by kind+name" (what the brief
   asks test (1) to assert on) falls out of the trait design for free —
   `FakeClusterApi::applies()` is a `Vec<(AppliedKind, String)>` built by
   the fake's own `apply_*` methods — where a wire-level mock would need a
   separate request-parsing step to recover the same information from raw
   HTTP bodies.

**What this proves.** `reconcile`'s full branch structure — the immutable
`controlNodes`-change refusal, the below-`controlNodes` scale-down refusal,
the highest-ordinal-first drain-then-remove sequence with its
stop-on-first-failure behavior, `AnimusClusterStatus.phase` computation
from `ready_replicas` vs `desired_replicas`, and `control_nodes_changed`'s
`ConfigMap`-JSON-round-trip inference — is now exercised by seed-free,
real-socket-free `#[tokio::test]`s, including the one shape a live cluster
makes awkward to test at all: a drain that never completes, which
`crate::controller::tests::drain_and_remove_node_is_bounded_when_drain_never_completes`
proves terminates in bounded polls (120, not indefinitely) using
`#[tokio::test(start_paused = true)]`'s auto-advancing virtual clock rather
than ten minutes of real wall-clock wait. It also pins, as an explicit
regression test rather than an implicit assumption, that `apply_children`
unconditionally re-applies all five children on every reconcile — there is
no diff-against-previous-state anywhere in this controller, so "a reconcile
of an unchanged cluster" is an idempotent re-apply, never a no-op.

**What this does not prove**, unchanged from the gap `src/controller.rs`'s
own module doc already named before this rung: real `kube::Api` wire
behavior (resourceVersion conflicts, admission, watch-driven requeue,
server-side-apply field-ownership semantics against a real API server), and
real-thread/real-network liveness of the `Controller::run` watch loop
itself. `RealClusterApi`'s methods are asserted by inspection to be the
same `kube::Api` calls `controller.rs` made directly before this rung (the
refactor commit is behavior-preserving, not tested against a live server
by this harness) — that gap is still `scripts/e2e-kind.sh`'s to close, and
still does, unchanged by this rung. E1's harness and the e2e smoke are
complementary, not overlapping: the harness proves the reconcile *logic*
deterministically and cheaply; the e2e smoke proves the *real* Kubernetes
interaction once, expensively, and only where the sandbox allows it to run
at all.

#### 2026-09-07 amendment — E2 closed: `admin_request` tests for the last seven mutating arms

E2 is done. U-08(i) (2026-09-04) and U-08(ii) (2026-09-06) covered the flat
GET arms and the dynamo-proxy wrappers, but left seven pre-existing
one-shot mutating arms — `drain`/`drain-status`/`remove`/`reconfigure`/
`flush`/`compact`/`stream-grow` — with no `admin_request` unit tests of
their own, so this ADR's "741 currently-untested lines" framing wasn't
fully retired. This rung closes exactly that gap: all seven arms already
built their `(method, path, body)` through `admin_request` (`crates/
animus-cli/src/main.rs`), the same pure, socket-free function every other
arm's tests already exercise, so no dispatch refactor was needed — only
tests, same shape as every existing `admin_request` test (happy path +
the argument-error paths the parser already has).

One test pins existing, deliberately-unvalidated behavior rather than
new behavior: `reconfigure`'s voter-list parsing (`split(',')`) does not
reject a trailing comma — it produces a voter named `""` — and
`reconfigure_a_trailing_comma_in_the_voter_list_produces_an_empty_voter`
documents that as the parser's actual contract (voter-identifier
validation is the server's job, same as every other admin route here),
not as new client-side validation added by this rung.

**Gates**: `cargo fmt --all --check`, `cargo clippy -p animus-cli
--all-targets --all-features -- -D warnings` (clean), `cargo test -p
animus-cli` (78 passed, up from 61). No production code changed —
`admin_request`'s dispatch is untouched, only its `#[cfg(test)]` module
grew.

With this, both Phase E rungs (E1, E2) are closed.

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

#### 2026-09-07 amendment — D4 PR 3 landed: deterministic `SimCluster` coverage for dropped-table GC, and a real reclaim gap found (not fixed)

D4 PR 3 is a driver-plus-assertions PR over the reconciler D4 PR 1 already
wired in — no code in `animus-cp-data`/`host.rs` changed. New module:
`crates/animusd/src/sim_cluster_dynamo_drop_table.rs`.

**Driver: the real wire, `DeleteTable`.** `dynamo::dispatch_table_op`'s
`DeleteTable` arm already calls `delete_table` → `ClientCtx::drop_table`
(`schema.rs`), which was already `<E: Env, R: RelayClient>`-generic since
D3 PR 2a — unlike several earlier D3/D4 rungs, this PR needed **zero** new
generic surface. Every scenario issues a real `DynamoDB_20120810.
DeleteTable` request via `SimCluster::dynamo`.

**A new observable, `SimCluster::storage(node, tablet) -> MemoryEngine`**
(mirroring `animus-cp-data/tests/reconciler_corpus.rs::Cluster::storage`'s
own "reads back empty" convention exactly — `MemoryTabletEngines::engine`
get-or-creates, so a reclaimed tablet's engine is a fresh, empty one) is
the actual physical-reclaim proof this PR adds beyond what `sim_cluster_
dynamo_table_ops.rs::delete_table_removes_it_and_a_repeat_delete_is_not_
found` already covered (metadata absence only). Three observables checked
together, converged-or-timeout: `Metadata::has_table_tablet` false on
every node, `SimCluster::hosted_tablets` no longer names the tablet on any
node, and `storage(node, tablet).entries()` empty on every node that ever
held a replica — plus a fourth, one-shot check that a fresh `CreateTable`
with the same name mints a NEW tablet id (ids never reused) and serves.

**Five scenarios, four of which converge cleanly**: (1) drop after writes,
base table, 3 nodes, 6 seeds total (1 pinned + 5 looped); (2) drop with a
declared GSI (materialized via `SimCluster::drain_gsi`, D3 PR 3b) —
`ClientCtx::drop_table`'s ADR 0041 §5 cascade reclaims the hidden
`<base>$<index>` table's own tablet too, proven with the identical three
observables against the hidden table; (3) `DeleteTable` issued
**immediately** after `CreateTable` returns, no intervening `run_for` at
all — the Host-vs-Reclaim race, asserting `reconciler_corpus.rs::
assert_idempotent`'s own discipline (converged *state*, never action
counts: a replica whose reconciler hasn't ticked even once before the
tablet vanishes from `Metadata` simply never hosts it, which reaches the
identical reclaimed end state as hosting-then-tearing-down); (5) a 4-node
cluster (`sim_cluster_dynamo_table_ops.rs::every_node_hosts_exactly_its_
replica_set_after_rebalance`'s own fixture shape) where the dropped
table's tablet was rebalanced onto a replica set different from the one
`CreateTable` first picked — reclaim correctly keys off `Metadata`'s
**current** replica set (`[n1,n2,n3]`), never the stale creation-time
snapshot (`[n0,n1,n2]`), and node 0 (already vacated by the ordinary ADR
0029 removed-replica GC before the drop was even issued) needs nothing
reclaimed from it.

**Scenario 4 — "a node crashed during the drop, restarted, converges" —
does NOT converge, and this is a real, previously-uncharacterized reclaim
gap, not a fixture bug.** `host::Reconciler::gather_facts` derives every
fact **exclusively** from the tablets currently named in `Metadata`
(`view.tablets.iter()`, both for the already-hosted branch and the
join-candidate branch) plus this reconciler's own in-process `LocalState`.
`DropTableTablets` removes a table's tablet rows from `Metadata`
**synchronously** at apply (ADR 0024) — so a node whose whole process is
down across the drop-and-`Metadata`-converges window comes back with (a) a
brand-new, empty `LocalState` (nothing persists it across a real restart —
see `crates/animusd/CLAUDE.md`'s drop-table-GC entry: "there is no more
durable `cp-hosted` marker... a restart just re-discovers every tablet to
host from replicated `Metadata`") and (b) a `Metadata` that already,
synchronously, never names the dropped tablet at all by the time this
node's reconciler first ticks — so `gather_facts` produces **no fact
whatsoever** for that tablet id, `plan()` never places it in `next.
hosted`, and `HostAction::Reclaim` (which only ever fires for a tablet
this reconciler's own `LocalState` currently claims) can never target it.
The tablet's own private engine — genuinely populated with real data
written before the crash — is a permanent, silent leak. Not a
`SimCluster`-fixture artifact: `gather_facts`'s scoping is unconditional,
and the real `LsmEngine` backend's `LsmTabletFactory::probe`/`destroy`
(`lib.rs`) is only ever called for tablet ids `gather_facts` already
decided to ask about — the identical mechanism, real disk included.
`docs/adr/0024-drop-table-data-gc.md`'s own text (lines 94-99) describes
the *pre-`host::Reconciler`* per-tablet-marker design's guarantee here — a
durable per-node marker forced a re-host attempt first, so a node that
missed a drop would re-host from its own marker, THEN discover (and
reclaim) the drop once its control replica caught up. That marker is gone
(ADR 0050); nothing replaced its restart-time "ask about what I used to
host, not just what `Metadata` currently says" role in the reconciler
rewrite.

Confirmed empirically, not just by static analysis: first written as a
POSITIVE convergence assertion (crash a non-leader replica hosting the
table, issue `DeleteTable` from a live node, restart the crashed node once
the drop has committed), it reliably failed at every one of 6 seeds tried
(`0xE4AF_0004`, `0xE4AF_4000..=0xE4AF_4004`) — never once converging within
a 15s virtual-time budget, while the metadata/hosted-set observables
(purged/re-derived independent of `gather_facts` entirely) converged fine.
Kept as ONE `#[ignore]`d regression,
`scenario_4_a_node_crashed_during_the_drop_and_restarted_leaks_its_engine`
— a green test asserting convergence would misleadingly read as an
accepted contract, and root `CLAUDE.md`'s green-is-an-invariant rule is
exactly why a genuine gap can't be asserted around; fixing `host::
Reconciler` itself is out of this PR's own "driver plus assertions, not
new mechanism" scope (a probe-and-reclaim-orphans mechanism would need a
new `EngineFactory::list()`-shaped capability across the trait and both
production implementors — real new mechanism, not this PR's job). Filed
here for a maintainer to pick up as its own follow-up PR.

**Production signature count: zero** (the `SimCluster::storage` accessor
is the only new surface, entirely test-fixture-internal — `pub(crate)` in
`sim_cluster.rs`, no `animus-cp-data`/`animus-control` change).

**`crates/animusd/tests/drop_table_gc.rs` and `drop_table_index_cascade.rs`
stay whole, unconverted** — every test in both files interleaves real-disk
`tablet_wal_present` (raw `LsmEngine` WAL-file-on-disk) assertions with
metadata/hosting-convergence ones in a single test body (including a real
process restart across the drop in `drop_table_gc.rs`'s own first test —
exactly the `LsmEngine`-durability shape `SimCluster`'s `MemoryEngine` tier
cannot stand in for), so per this PR's own "convert only the metadata/
hosting half, leave a mixed test whole" instruction, commit B removes
nothing from either file.

**Gates**: `cargo fmt --all --check` (clean); `cargo clippy -p animusd
--all-targets --all-features -- -D warnings` (clean, zero new warnings);
`cargo build -p animusd --all-targets` (green); `cargo test -p animusd
--lib` — 307 before → 315 after (+8 new passing, 0 removed, 0
regressions; ignored 3 → 4), ~121s wall (within noise of the 307-test
baseline); `cargo test -p animusd --test drop_table_gc --test
drop_table_index_cascade` (3 passed, unchanged, both before and after —
nothing was trimmed); `cargo test -p animus-cp-data --test
reconciler_corpus` (4 passed, unchanged, untouched by this PR);
`ANIMUS_SEED` replay confirmed deterministic for scenario 1
(`drop_after_writes_reclaims_base_table`) and scenario 5
(`drop_reclaims_off_the_rebalanced_replica_set`); `Cargo.lock` unchanged.

#### 2026-09-07 amendment — D4 PR 3's own finding closed: `EngineFactory::local_tablets`, the reconciler's second fact source (issue #722)

The gap the amendment immediately above reported — scenario 4's crashed-
during-the-drop-and-restarted engine leak, filed for a follow-up PR rather
than fixed there (out of that PR's own "driver plus assertions, not new
mechanism" scope) — is now fixed, in `animus-cp-data::host` this time, not
`animusd`.

**The fix.** `host::EngineFactory` gained one new method,
`local_tablets(&self) -> BTreeSet<TabletId>` (default-empty, so a
third-party implementor of this now-widened trait still compiles
unmodified): the tablet ids this node currently has DURABLE local engine
state for, independent of replicated `Metadata`. `MemoryTabletEngines`
answers from its own in-memory registry keys; `animusd`'s production
`LsmTabletFactory` answers by listing its node's data directory once (the
identical `env.list()` call `probe`/`destroy` already make) and parsing
each filename's own `db-t{tablet}-` prefix via a new, unit-tested
`parse_tablet_id_from_lsm_filename` (the inverse of `tablet_lsm_prefix`).
`host::Reconciler::tick` consults it exactly **once** — this reconciler's
very first tick after construction, never a later one, since a local
engine appearing after that first tick can only be this same reconciler's
own `Host`/`MaterializeSplitChild` action, already tracked in
`LocalState` — so the fix adds no per-tick directory-listing cost in
steady state, only a one-time cost right after a restart. `plan` gained a
matching `local_tablets: &BTreeSet<TabletId>` parameter and a new phase,
ahead of the pre-existing reclaim phase: any id present in `local_tablets`
but absent from a `known` set (every current `view.tablets` key, plus
every split child named on any tablet's own still-live `inplace_split`
intent, since a pre-cutover child is materialized before it has a map
entry of its own) is folded into `LocalState::hosted`, so the pre-existing,
**unmodified** reclaim phase picks it up exactly like any other
hosted-but-now-absent tablet — no new `HostAction` variant, no new
teardown path. See `crates/animus-cp-data/CLAUDE.md`'s host-module entry
for the full mechanism and the safety argument (an engine only ever exists
locally for a tablet id this exact node has itself, at some prior tick,
observed as real, so a locally-present id absent from `known` is never "a
tablet that hasn't appeared in `Metadata` yet," only ever a genuine
leftover) and `docs/adr/0024-drop-table-data-gc.md`'s own 2026-09-07
amendment for the incident account and the restart-time guarantee this
restores.

**Scenario 4 is now a positive, un-ignored assertion**
(`sim_cluster_dynamo_drop_table.rs`, renamed `scenario_4_a_node_crashed_
during_the_drop_and_restarted_reclaims_its_engine`), replayed at the
original six investigation seeds plus ten more. Delivering the positive
assertion found and fixed two real bugs in this module's own test
harness, neither in the fix itself — see `crates/animusd/CLAUDE.md`'s own
appendix entry on this closure for the full account of both (a victim
selection that could crash the control-plane leader by coincidence, and
`assert_reclaimed`'s convergence check being unsound when split across two
differently-timed passes for a just-restarted node specifically). A
matching real-disk regression, `a_node_stopped_before_the_drop_and_
restarted_after_reclaims_its_leftover_engine`, landed in `crates/animusd/
tests/drop_table_gc.rs` — still hand-written `ProdEnv`, per that PR's own
"a mixed metadata+real-disk test body has no `SimCluster` analog" finding,
unchanged by this fix.

**Gates**: `cargo fmt --all --check` (clean); `cargo clippy -p animus-cp-
data -p animusd --all-targets --all-features -- -D warnings` (clean, zero
new warnings); `cargo test -p animus-cp-data` (379 passed, 0 failed, 0
regressions — includes the crate's own unit tests, `reconciler_corpus`,
`inplace_split_reconciler`, `sharedwal_fault_corpus`, and every other
integration binary in the crate); `cargo test -p animusd --lib` (321
passed, 3 ignored, 0 failed — up from 315/4 before this fix;
`sim_cluster_corpus`/`sim_cluster_dynamo_corpus` both stay green
unmodified); `cargo build -p animusd --all-targets` (green); `cargo test
-p animusd --test drop_table_gc --test drop_table_index_cascade` (4
passed — the new real-disk regression plus the three pre-existing ones —
run 3x locally with no flake); `ANIMUS_RECONCILER_SEEDS=25 cargo test -p
animus-cp-data --test reconciler_corpus` (green); `ANIMUS_INPLACE_SPLIT_
SEEDS=25 cargo test -p animus-cp-data --test inplace_split_reconciler`
(green — the in-place split path is provably unaffected: the `known`-set
safety argument this fix rests on is exactly what makes a pre-cutover
split child's own eagerly-materialized engine exempt from the new
reclaim path). `Cargo.lock` unchanged.

## D4 PR 2 (2026-09-07): auto-split BYTE trigger widened to the `Env` seam, deterministic `SimCluster` coverage

Closes the "auto-split's own `tokio::time` conversion" residual D4 PR 1's
own amendment named. `auto_split_loop` (`crates/animusd/src/lib.rs`) —
concrete `ClientCtx`/`tokio::time::Instant`, i.e. `ProdEnv`-only, since its
own introduction (ADR 0034) — is now `async fn auto_split_loop<E: Env, R:
RelayClient>(ctx: ClientCtx<E, R>, thresholds: AutoSplitThresholds)`: one
production signature widened, plus one already-generic-elsewhere type
reference (`CpGroup` → `CpGroup<E>` inside the min-tablets arm's own local
`best` binding) that needed the same treatment to compile. Both per-tablet
bookkeeping maps (`last_triggered`, `last_counted`) are now `BTreeMap
<TabletId, animus_env::Nanos>`, read via `ctx.env.now().duration_since(..)`
instead of `tokio::time::Instant::elapsed()` — the mechanical conversion
is a pure seam substitution with the trigger arithmetic itself
byte-identical, confirmed by the unchanged real-socket `ProdEnv` tests
(`cp_plane.rs`'s survivors, `inplace_split_e2e.rs`, `f11_split_alignment.
rs`, `auto_split_min_tablets.rs`, `auto_split_ops_rate.rs`) staying green
before and after with zero behavior change. See ADR 0034's own matching
2026-09-07 amendment for the full conversion account.

**A second, narrower widening was needed for the fork's own cutover to be
reachable under `SimEnv` at all**: `index_drain::{inplace_split_driver_
tick, gsi_caught_up}` (previously `ProdEnv`-only, since the whole
`change_consumer_loop` they belong to has never been generic) were widened
the identical way — their own callees (`seal_now`/`pitr_seal_now`/
`drain_tablet`/`ClientCtx::propose_schema`) were already `<E, R>`-generic
since rung C5 step 3b. `SimCluster` still does not spawn `change_consumer_
loop` as a background task (it drives Streams/PITR/GSI-drain machinery
this fixture has no general need for); instead, a new `SimCluster::
drive_inplace_split_cutover(node)` manually drives one pass of
`inplace_split_driver_tick`, mirroring the pre-existing `SimCluster::
drain_gsi`'s own manual-drive shape for `drain_tablet`.

**The knob**: `SimCluster::set_auto_split_thresholds(thresholds:
AutoSplitThresholds)` spawns `auto_split_loop` on every node right now
(mirroring D4 PR 1's own `heartbeat_loop` spawn) and stores the
configuration in a new `SimCluster::auto_split: Option<AutoSplitThresholds>`
field so `SimCluster::restart` respawns it identically on a restarted
node. Defaulted `None` (off) — every scenario in every other `sim_cluster_
*` module never calls it, so this rung is a pure addition with zero effect
on existing coverage.

**Deterministic coverage**: `crates/animusd/src/sim_cluster_auto_split.rs`
— five scenarios, each replayed at 5 seeds (10 tests total): (a) a byte
threshold crossing forks exactly once, both children `Active`, every
pre-split key still readable through the DynamoDB wire; (b) staying below
the threshold over a long (past `AUTO_SPLIT_INTERVAL` + `AUTO_SPLIT_
COOLDOWN`) window never splits; (c) after one fork, modest writes below
threshold don't cause a second one, and a genuine burst that pushes a
child back over the SAME threshold triggers a further, independent fork —
proving the widened `Nanos`-keyed maps handle a tablet id minted mid-run
(a freshly-forked child), not just one present at loop start; (d) a
non-leader's own `ctx.edge.cp_leader(tablet)` gate structurally answers
`None`, and a leadership move mid-window (the original leader crashed)
still yields exactly one fork, driven by the newly elected leader's own
loop instance; (e) a node crashed before the fork ever starts, restarted
only after the fork fully converged elsewhere, itself converges too — no
zombie groups, its own leftover PARENT engine reclaimed via the issue #722
fix (`host::Reconciler`'s `EngineFactory::local_tablets` second fact
source, D4 PR 3). `cargo test -p animusd --lib`: 321 → 331 (+10 tests, 0
regressions, 3 ignored throughout).

**Two real-behavior gotchas the scenarios' own build found, both fixed in
the harness, neither in production code** — worth recording since they
generalize to any future `SimCluster` scenario that mixes writes with a
background trigger loop:

- **A burst of writes issued after `set_auto_split_thresholds` can
  genuinely land mid-fork.** `SimCluster::dynamo`/`put`/etc. all advance
  virtual time internally (`spawn_and_capture`'s own `run_for`), so once
  the auto-split loop is armed, a later write in the same test can find
  its target tablet `Splitting` (frozen for cutover, ADR 0050's whole-range
  seal discipline) and get refused with the house `"; retry"` transient
  error. A plain wait-and-retry loop is not enough on its own, since this
  fixture never spawns the cutover driver as a background loop (the
  previous paragraph) — the retry helper must also drive `SimCluster::
  drive_inplace_split_cutover` on every node on each attempt, or the
  freeze never clears and the retry spins to its own bound and fails.
- **A crashed-but-muted node's own stale "I am still leader" belief
  breaks a leader-index scan that doesn't exclude it.** `SimCluster::
  leader_index_of`/`SimClusterHandle::leader_index_of` scan every node id
  unconditionally; a `SimCluster::crash`ed former leader's own `RaftKvNode`
  never receives a higher-term message telling it to step down (it is
  muted, not stopped), so it keeps reporting itself leader forever. A test
  scenario that crashes a leader and then polls for a NEW one to be
  elected must scan only the live node ids directly (`is_leader_local` per
  id), never the cluster-wide accessor, or the poll spins until its own
  timeout finding the same (crashed) leader on every pass.

**ProdEnv test disposition** (`crates/animusd/tests/cp_plane.rs`): two
tests removed, replaced by the new sim scenarios — `tablet_auto_splits_
when_it_grows` (by scenario a) and `already_split_tablet_splits_again_
once_it_regrows` (by scenario c). Four tests/files stay, each for a stated
reason: `tablet_auto_splits_on_bytes_with_skewed_value_sizes` (this file's
own sibling) keeps its specific byte-weighted-median quantitative-balance
claim — a loose 15% floor derived from correlating written keys against
their real post-split token ranges — which the new `SimCluster` scenarios
don't reproduce (doing so would need correlating a DynamoDB item's `pk` to
its hashed token range, out of this rung's own scope);
`cp_tablet_splits_and_both_halves_serve` tests the MANUAL raw-`ClientRequest`
split path, a different subject from the byte trigger; `f11_split_
alignment.rs`'s one test and `inplace_split_e2e.rs`'s `inplace_split_
stream_shard_walks_parent_to_children_without_loss_or_duplication` are both
Streams-specific, a capability `SimCluster` still documents as `ProdEnv`-
only (no real `SegmentStoreHandle::Cluster`); `inplace_split_e2e.rs`'s
`inplace_split_survives_a_paced_continuous_writer_across_fork_and_cutover`
asserts real-thread-paced (5ms) continuous-writer timing through admin
HTTP kickoff — squarely the "keep anything asserting real-thread timing…
or admin/console HTTP" carve-out. `auto_split_min_tablets.rs`/
`auto_split_ops_rate.rs` are untouched (other triggers, out of this rung's
scope by the task's own instruction). Remaining D4 residuals: PR 4
(join/growth) and PR 5 (the backup janitor's own `client_ctx_host.rs`
widening).

**Gates**: `cargo fmt --all --check` (clean); `cargo clippy -p animusd
--all-targets --all-features -- -D warnings` (clean); `cargo build -p
animusd --all-targets` (clean); `cargo test -p animusd --lib` (321 → 331
passed, 0 failed, 3 ignored throughout); `cargo test -p animusd --test
cp_plane --test inplace_split_e2e --test f11_split_alignment --test
auto_split_min_tablets --test auto_split_ops_rate` both BEFORE (11 passed,
proving the widened loop behaves identically under `ProdEnv`) and AFTER
the `cp_plane.rs` test removal (9 passed — the two removed tests gone, the
other 9 unaffected); `cargo test -p animus-cp-data --test inplace_split_
reconciler` (untouched, sanity, 3 passed); `ANIMUS_SEED` replay of
scenarios (a) and (e) at their pinned seeds (both green). `Cargo.lock`
unchanged.

## D4 PR 5 (2026-09-07): backup janitor widened to the `Env` seam, deterministic `SimCluster` coverage

Closes D4's last open widening residual — D4 PR 1/2/3's own amendments all
named "the backup janitor needs `client_ctx_host.rs`'s impls widened" as
outstanding; PR 4 (join/growth) remains open, unaffected by this PR.

**Widening, zero new mechanism.** `animus_node::backup_janitor::
backup_janitor_loop<E, H>` was already `E: Env`-generic since rung C2 —
the only thing stopping it from being spawnable under `SimEnv` was that
`animusd::client_ctx_host.rs`'s four `ClientCtx` implementations of the
host-capability traits it needs (`ControlLeaderHost<E>`/`BackupObjectStore`/
`BackupJanitorProgressHost`, plus `TtlScanHost` — the fourth impl this
rung also widened per the original task scope, though not itself needed by
this loop) were all pinned to the concrete `ClientCtx` alias (`E = ProdEnv,
R = AnimusdRelayClient`), and `animusd::backup_janitor`'s own thin wrapper
took a concrete `ClientCtx` too. Both widened to `impl<E: Env, R:
RelayClient> .. for ClientCtx<E, R>`/`fn backup_janitor_loop<E: Env, R:
RelayClient>(ctx: ClientCtx<E, R>)` — a pure signature change: every
field/method each impl delegates to (`self.edge.leader_handle()` —
`ClusterEdgeState<E>::control` already widened by D3 PR 2a — `self.
backup_store`, `self.backup_janitor_progress`, `edge.hosted_groups()`,
`dynamo::kind_write_item_at_leader::<E, R>`, already generic since rung
C5) was already `E`/`R`-agnostic or already generic. `animus_node::host`'s
own trait definitions needed **no** change at all — confirming the design
pass's own expectation. `TtlReaperProgressHost` (a fifth impl in the same
file, sharing `BackupJanitorProgressHost`'s exact shape) was deliberately
**not** widened — nothing in this rung's scope needs it generic, and it
coexists as a separate, non-overlapping impl on the bare `ClientCtx`
default-type-parameter alias. Production's two real spawn sites
(`spawn_common_tail`) needed zero changes, inferring `E`/`R` from the
concrete `ClientCtx` they pass exactly as before.

**Store choice: one shared `SimSegmentStore`, wrapped in
`BackupStoreHandle::S3` on every node — not a per-node placeholder.**
`SimCluster` (`crates/animusd/src/sim_cluster.rs`) builds the store once
in `SimCluster::new` and every node's own `ClientCtx::backup_store` wraps
`Arc::new(backup_store.clone())` around it (`SimSegmentStore::clone` is
cheap — its state lives behind an `Arc<Mutex<..>>`). This mirrors real
production semantics deliberately: `BackupStoreHandle::S3` already holds
`Arc<dyn animus_env::SegmentStore>` specifically so a test can substitute
a fake transport (the identical seam `animusd::lib.rs`'s own
`s3_store_handle_tests` uses over `animus_s3::fake::FakeS3`), and a real
`s3://` bucket has no per-node locality at all — every node in a real
deployment shares the identical bucket. Sharing one store is also what
makes the leader-gating scenario meaningful: a per-node-local directory
(the shape every other `sim_cluster_*` module's own untouched `Fs`
placeholder still uses for fields nothing reads) would make "did a
follower's janitor touch the store" trivially true by construction, since
it would have its own private copy to *not* touch. `SimCluster::restart`
leaves `ctx.backup_store` untouched, so a restarted node's respawned
janitor loop still shares the identical store. `backup_janitor_loop` is
spawned unconditionally on every node in both `SimCluster::new` and
`SimCluster::restart` — mirroring `heartbeat_loop`'s own always-on D4 PR 1
spawn, not `auto_split_loop`'s opt-in `set_auto_split_thresholds` shape,
since this loop's own leader gate already makes a non-leader's tick a
cheap idle no-op.

**New `SimCluster` accessors**: `backup_store() -> SimSegmentStore` (a
cheap clone for direct assertions/seeding); `seed_backup_object(id,
bytes)` (a real `SegmentStore::put` via `spawn_and_capture`);
`backup_janitor_progress(node) -> JanitorProgress` (the `GET
/admin/backup-store` read, a plain lock/clone/drop); `propose_meta
(command: MetaCommand) -> ProposeResult` (a general-purpose sibling of
`set_table_throughput`'s own hand-rolled `self.controls[leader]
.propose(..)`, used to drive the backup catalog's own commands the
identical way `animus-control/tests/backup_catalog.rs`'s `propose_
accepted` helper does against a bare `RaftNode`); `transfer_control_
leadership_to(target)` (a real `RaftCore::transfer_leadership` handoff,
retried-bounded since a single arm attempt only succeeds if `target`'s own
log has already caught up to the leader's current commit index at that
precise instant — the identical one-shot-arm caveat `animusd/CLAUDE.md`'s
issue #405 entry documents).

**Five scenarios**, `crates/animusd/src/sim_cluster_backup_janitor.rs` (12
tests including `_over_seeds` siblings at 5 seeds each): (a) a completed
(`Available`) backup marked deleted is reclaimed — manifest and
data-chunk objects gone, catalog row gone, the leader's own
`JanitorProgress` ending `Idle` having seen the backup and reclaimed both
objects; (b) a `Failed` backup (no `MarkBackupDeleted` involved) is
reclaimed the identical way; (c1) a follower's own `JanitorProgress`
never leaves `Idle` while the leader alone reclaims; (c2) a real
leadership-transfer handoff issued immediately after `MarkBackupDeleted`
commits still converges to exactly one reclaim with no error recorded on
any node's own progress (idempotent whichever leader's own tick actually
does the work); (d) the control-plane leader crashes right after
`MarkBackupDeleted` commits and is restarted only once the survivors have
already reclaimed the backup on their own — the restarted node's own view
converges too, no stale error; (e) an `Available` backup (never marked,
never failed) is never touched over a long window, every node's own
`backups_seen` staying 0.

**One real gotcha found and fixed, in the test harness, not production**:
scenario (d)'s first draft called `propose_meta(MarkBackupDeleted)` then
immediately `crash(victim)` with zero intervening virtual time —
`RaftNode::propose` only appends to the leader's own local log and
returns `Accepted` the instant that append happens, never "committed to a
majority" (root `CLAUDE.md`'s durable-before-visible entry). With no time
advanced, the entry had not replicated to either follower before the
crash muted the leader's outbound sends — the survivors' own `Metadata`
never saw the backup marked at all, so their own janitor had nothing to
reclaim, and the poll spun to its own 20s budget every run. Fixed with a
short `run_for` (well under the janitor's own 200ms tick, so the crash
still lands before the about-to-crash leader's own tick could finish the
whole reclaim itself, keeping the scenario a genuine proof the survivors
do the work) between the propose and the crash. See `docs/engineering-
lessons.md`'s matching entry for the general lesson: any test that
proposes something and then immediately faults the node it proposed on
must let at least one round of replication happen first.

**No janitor bug found.** All five scenarios (and their `_over_seeds`
siblings) held at every seed tried. `crates/animusd/tests/dynamo_
backup.rs`'s own `create_backup_round_trip_survives_table_drop_and_
janitor_reclaims` stays entirely on `ProdEnv` — its janitor-convergence
assertion (the test's very last one) is fused into one long test that
also proves DynamoDB wire shapes (`CreateBackup`/`DescribeBackup`/
`ListBackups`/`DeleteBackup`'s JSON responses, the frozen `BackupSizeBytes`
across a table drop, the immediate-`DELETED`-then-`BackupNotFoundException`
contract) `SimCluster` cannot reach at all — it drives no DynamoDB
backup/restore wire operations, a residual named since rung D2. Nothing
was removed from that file; D4 PR 5 adds a third, middle tier
(deterministic, multi-node, fault-injecting) alongside the existing
primitive-level (`animus_node::backup_janitor::tests`) and wire-level
(`dynamo_backup.rs`) coverage, reaching properties neither of those two
does: leader gating, a real leadership handoff, and crashed-leader/restart
recovery.

`cargo test -p animusd --lib`: 331 → 343 (+12, 0 regressions, 3 ignored
throughout).

**Gates**: `cargo fmt --all --check` (clean); `cargo clippy -p animus-node
-p animusd --all-targets --all-features -- -D warnings` (clean); `cargo
test -p animus-node` (137 passed, proving the trait definitions are
untouched); `cargo test -p animus-control --test backup_catalog` (3
passed, the state machine this loop drives, untouched); `cargo test -p
animusd --lib` (331 passed before, 343 passed after); `cargo build -p
animusd --all-targets` (clean); `cargo test -p animusd --test dynamo_
backup --test dynamo_restore --test dynamo_pitr` (before and after, both
green); `ANIMUS_SEED` replay of scenarios (a) and (c1). `Cargo.lock`
unchanged.

## D4 PR 4 (2026-09-07): online growth/decommission (ADR 0030/0032) widened to a real `SimCluster` fixture surface, deterministic coverage, closing D4 and C-04

Closes D4's last open item — every prior PR's own "what remains" note named
join/growth (PRs 1/2/3 said so directly; PR 5's own closing line named it
as the one thing left). Unlike PR 1/2/3/5, which each widened one existing
production loop to the `Env` seam, this PR adds genuinely **new** fixture
mechanism: `SimCluster::new` fixes the whole node set up front (this file's
own "the whole node set is known at construction" note, `sim_cluster.rs`'s
module doc), and production's own growth/join constructors
(`animusd::run_node_join`/`BoundDataNode::start_data_with_growth`) are
real-socket, `ProdEnv`-only entry points that cannot run under `SimEnv` at
all — there was no existing generic loop to widen.

**The fixture surface, five new signatures** (`crates/animusd/src/
sim_cluster.rs`): `SimCluster::grow(role: &str) -> u64`, `SimCluster::
drain(node: u64)`, `SimCluster::remove(node: u64)`, plus two private
helpers `grow` builds on — `SimClusterHandle::push_ctx` (grows the shared
`ctxs` vec by one, mirroring `set_ctx`'s existing replace-in-place sibling)
and a new free function, `spawn_remote_mirror_sync_loop`. `role` must be
`"data"` today — a `"combined"` growth node (a new control-plane voter,
joining the *live* Raft quorum via `change_membership`) needs `self.
controls` itself to grow, a materially different mechanism than a
data-only node's `ControlHandle::Remote` mirror, and was deliberately
scoped out and named as a follow-up rather than attempted.

**The one genuinely new mechanism: `ControlHandle::Remote`'s real
mirror-sync logic, exercised under `SimEnv` for the first time.** Every
prior `SimCluster` node was a genuine control-group voter (`ControlHandle::
Local`) — this rung's growth node is the fixture's first `Remote`-handle
node. `animusd`'s own production `remote_metadata_watch_loop`/`remote_
metadata_sync_loop` are structurally unreachable here: `ClientCtx`'s bare
name defaults to `E = ProdEnv`/`R = AnimusdRelayClient`, so a `ClientCtx<
SimEnv, SimRelayClient<SimEnv>>` doesn't even type-check as their
parameter, and the production retry backoff is a bare `tokio::time::
sleep` — the exact `Env`-seam violation this whole fixture exists to route
around. `spawn_remote_mirror_sync_loop` is therefore a **new, parallel
`SimEnv`-native implementation of the identical long-poll wire protocol**
(`ClientRequest::WatchMetadata`/`Status`, `RemoteControlClient::observe`/
`observe_delta`) against the exact same `animus_node::control_handle::
RemoteControlClient<R>` type production's own `ControlHandle::Remote`
wraps — not a generalization of the production function, which stays
untouched — so what's actually under test is the real mirror
observe/delta/leader-hint logic, only the executor and sleep primitive
differ. A naming gotcha worth recording for anyone extending this fixture
further: this crate's own `use super::*` brings in a `RemoteControlClient`
type alias bound to `R = AnimusdRelayClient` (this crate's own
`control_handle.rs`), which is useless here and would silently resolve to
the wrong (unusable) type if referenced bare inside `sim_cluster.rs` —
`grow` imports the *generic* `animus_node::control_handle::
RemoteControlClient` under a distinct name (`GenericRemoteControlClient`)
specifically to avoid this, the identical "a bare alias resolves to its
own default, never the enclosing generic scope" family of gotcha rung C5
step 3a's own doc already records for a different type (`ControlHandle`)
in a different file.

**`drain`/`remove` reuse the REAL production primitives, not a fixture
bypass** — `ClientCtx::admin_drain`/`admin_remove_member`, already `<E,
R>`-generic since rung C5 step 3a, called directly off the control
leader's own `ClientCtx` exactly as production's admin HTTP handler does.
This is a deliberate asymmetry from `grow`'s own self-registration (a
`RegisterNode`+`UpsertMember{Active}` control-plane bypass, the identical
idiom `SimCluster::seed_members` already uses for the initial node set,
not the real relayed `admin_add_member` promotion dance) — see each
method's own doc for why: `grow` needed no new capability from
`admin_add_member` (its own `Down`→failure-detector-promotion timing would
have added nothing this fixture's own converged-or-timeout poll doesn't
already prove more directly), whereas `drain`/`remove` genuinely exercise
this ADR's own decommission sequence end to end only if they call the real
thing.

**Five scenarios, `crates/animusd/src/sim_cluster_growth.rs`**, replayed at
5 seeds each (`ANIMUS_SEED=<seed> cargo test -p animusd --lib <test name>`
replays any one; primary seeds `0x6706_0001`(a) / `0x6706_0002`(b) /
`0x6706_0003`(c) / `0x6706_0004`(d) / `0x6706_0005`(e)): (a) grow a 3-node
cluster to 4 (data-only) — the new node self-registers `Active` on every
node's own view, including its own `Remote` mirror, and serves a
genuinely forwarded write+read (it hosts no replica of the table at all);
(b) growth, then three `CreateTable`s (not one — see `provision_soak_
tables_and_wait_for_replica`'s own doc for why a single tablet's own
1,1,1,0 load is already within `rebalance_step`'s max−min ≤ 1 convergence
bound and would never move; a real first-draft mistake this rung's own
task caught and fixed, recorded in `docs/engineering-lessons.md`) — the
ordinary balance-driven rebalance (already running unconditionally)
converges to the new node holding a replica of at least one, and `assert_
no_zombie_groups` proves the replica it displaced was actually torn down;
(c) grow, wait for a rebalance-placed replica, then `drain`+`remove` —
every replica re-homes onto the three original survivors at the original
RF, no zombie group anywhere, no node still naming the removed id, and a
later `grow` mints a strictly higher index, never the removed one; (d) the
control-plane leader crashes between `grow`'s own two `MetaCommand`
proposes, reproduced by hand via `SimCluster::propose_meta` (re-resolving
the CURRENT leader on every call) since `grow` has no internal fault-
injection hook and one isn't worth adding for a single scenario — virtual
time is advanced between the first propose and the crash (the
propose-then-crash lesson, `docs/engineering-lessons.md`), and the
follow-up propose finds whichever new leader the two survivors just
elected and still commits; (e) the new node is partitioned from **every**
control voter (not merely the leader — `RemoteControlClient`'s own `seeds`
list is the whole pre-growth voter set, and its long-poll falls through
every seed in turn on a failed hop, so partitioning only the leader would
leave two other reachable seeds and prove nothing about the fallback path)
while a schema change commits, the partitioned mirror provably does not
see it, and it catches up once healed.

**No product bug found — the `ControlHandle::Remote`-under-`SimEnv` path,
new surface and the likeliest place for one, held at every seed tried.**
Every scenario passed on its first full clean run with no fixture fix
needed beyond what this PR's own design already anticipated (the
propose-then-crash timing in scenario (d), the multi-table rebalance setup
in scenario (b) — both caught during construction, not as a regression
against already-landed code).

**ProdEnv conversion: none.** Per this rung's own scope, only
control-plane-membership/mirror-sync/drain-sequencing assertions that
scenarios (a)-(e) reproduce were in scope for conversion, and none of
`cluster_growth.rs`/`seed_join.rs`/`seed_join_allocated.rs`/`data_join.rs`/
`decommission.rs`/`data_only.rs`'s own tests are *purely* that shape — each
mixes in a real-socket concern this fixture cannot reach: `cluster_growth.
rs`'s three tests cover a real growth-then-rebalance across genuine
`animusd` processes (dashboard health rendering, a real `--seed ADDR`
join), `seed_join.rs`/`seed_join_allocated.rs` prove the real `--seed`
CLI/`JoinInfo` discovery and allocate-node-id wire round trip,
`data_join.rs` proves a real data-only process joining over a real socket,
`decommission.rs` proves the real admin-HTTP decommission flow plus
dashboard health, and `data_only.rs` proves genuine control-only/data-only
process role assembly. `tablet_rf_self_heals.rs` and `learner_reconfigure.
rs` also stay whole — the former's subject is `provision_tablet`'s RF
self-heal after a real growth (a `ProvisionResult`/placement mechanism this
fixture's `create_table_with_replication` sidesteps entirely by minting
the full RF up front), and the latter is learner (non-voting) membership-
class fault injection, a different membership dimension than plain
active-member growth/decommission — neither is decommission/join
sequencing in the sense this rung's scenarios prove. All eight binaries
stay exactly as they were, verified unchanged by the real-socket sanity
run below.

**This closes D4, and with it C-04** (`docs/roadmap.md`'s matching
addendum has the full closing account) — the four D4 rungs (auto-split
byte trigger, dropped-table GC, the backup janitor, and this PR's join/
growth/decommission) all now have deterministic `SimCluster` coverage,
and issues #715/#722 (found and fixed along the way) are both closed. What
remains unowned by any planned rung, per the D3-closing residual inventory
(above): Transact/PartiQL (D2's own named residuals), Streams, TTL, admin/
console/dashboard HTTP, the control/data role split, and `--config`
bring-up.

`cargo test -p animusd --lib`: 343 passed before, 353 passed after (+10, 0
regressions, 3 ignored throughout both, full run 301.15s).

**Gates**: `cargo fmt --all --check` (clean, after one formatting fix);
`cargo clippy -p animusd --all-targets --all-features -- -D warnings`
(clean; `animus-node` untouched by this PR, so its own clippy gate did not
apply); `cargo test -p animusd --lib` (343 passed before, 353 passed
after, 0 regressions); `cargo build -p animusd --all-targets` (clean);
real-socket sanity on the eight named, unchanged binaries — `cargo test -p
animusd --test cluster_growth --test seed_join --test seed_join_allocated
--test data_join --test decommission --test data_only --test tablet_rf_
self_heals --test learner_reconfigure` (20 tests, all green, both before
and after this PR's doc-only tail); `ANIMUS_SIMCLUSTER_SEEDS=10 cargo test
-p animusd --lib sim_cluster_corpus` (3 passed, 1 ignored — the opt-in
shrink-replay entry point, 205.88s); `ANIMUS_SEED` replay of scenarios (a)
and (c) at their pinned seeds (both green). `Cargo.lock` unchanged.

## 2026-09-07 amendment — Rung F (post-C-04): Transact/PartiQL `SimCluster` dispatch (C-06), PR 1 (this docs-only opener)

D4 PR 4 closed D4, and with it C-04, leaving eight unowned residual groups
(that PR's own closing text and `docs/roadmap.md`'s matching addendum).
Two of the eight — Transact and PartiQL — are not new discoveries: they
were named as out-of-scope from the very start of Phase D. D2 PR 1's own
amendment scoped `dispatch_item_op` around exactly six item operations
plus a base-table `Query`/`Scan`, closing with "GSI/LSI, transact, and
PartiQL remain out of scope, named as D2's own residuals for whichever
rung generalizes those operations next." The D3-closing residual inventory
(this file's own "D3 closed" amendment, above) put numbers on the two: 6
files/32 tests still `ProdEnv`-only for Transact, 2 files/37 tests for
PartiQL — the largest and third-largest of Class D's thirteen groups after
admin/console/dashboard HTTP. This rung — informally "F," since it
continues Phase D's payoff after D4 rather than opening a new phase —
claims both, tracked in `docs/roadmap.md` as **C-06**.

**Why this is the natural continuation.** D3/D4 proved a template that
generalizes cleanly: a production function whose only `ProdEnv`-binding is
a concrete `&ClientCtx` parameter widens to `<E: Env, R: RelayClient>` with
zero behavior change (D3 PR 2a/3a/3b, D4 PR 2/5 all did exactly this); a
new, strictly additive `_as`/generic sibling function is what `SimCluster`
actually calls, never a widening of the production dispatcher itself (the
D2 PR 1 lesson, `docs/engineering-lessons.md`'s "A narrowed generic split
of a dispatcher must not become the production dispatcher's ONLY path"
entry). A read-only grep against `crates/animusd/src/dynamo.rs` at this
commit confirms Transact and PartiQL fit the identical shape, with one
real wrinkle each:

- **Transact.** `run_transact` (~4902), `run_transact_get` (~5674),
  `quiescent_multi_get` (~5753), `transact_write_idempotency_preflight`
  (~5345), `idempotency_claim_put` (~5412), `read_idempotency_record`
  (~5457), `record_transact_write_outcome` (~5508), and
  `idempotency_record_item` (~5437) are all still concrete `&ClientCtx` —
  the same shape every D3 rung widened. `ensure_txn_idempotency_table`
  (~5548) is the wrinkle: it carries six real wall-clock sites — four
  `tokio::time::Instant::now()` calls (building the two commit deadlines,
  then checking each against `Instant::now()` a second time) and two
  `tokio::time::sleep(SCHEMA_POLL_INTERVAL)` calls, one pair per commit
  loop (the table's own `CreateTableSchema` propose-and-poll, then its
  `SetTableTtl` propose-and-poll) — that would simply hang forever under
  `SimEnv`, whose virtual clock only advances when the seam itself is
  asked to sleep. This is exactly the `update_table_throughput`/
  `create_table` precedent (D3 PR 2a/2b) that already converted the
  identical `tokio::time::Instant::now() + TIMEOUT` / `tokio::time::sleep`
  pattern to `ctx.env.now().saturating_add(..)` / `ctx.env.sleep(..)`, so
  this is mechanical, not a new design.
- **PartiQL.** `execute_statement` (~6697) and `execute_transaction`
  (~6933) do not themselves touch `ClientCtx` concreteness as their
  primary blocker — `partiql::parse_statement` and the rest of
  `crates/animus-dynamo/src/partiql.rs`'s lowering functions are already
  pure and `Env`-free (ADR 0071). The actual blocker is that
  `execute_statement`'s `INSERT`/`UPDATE`/`DELETE` arms recurse into the
  concrete production `run_operation` (ADR 0071 PR 3's own design: "run
  through the exact same `run_operation` dispatcher a client-built request
  of that shape already uses"), and `run_batch_execute_statement`/
  `execute_one_batch_statement` (~7124/~7158) recurse into
  `execute_statement` itself. Widening `run_operation` is out of the
  question — it is the production dispatcher every real DynamoDB request
  goes through, and the D2 lesson above is exactly the failure mode a
  widening-in-place would risk. PartiQL's generic path therefore needs
  **parallel generic siblings**, in the same `_as` shape
  `execute_item_op_as` (~1716) already established for item ops, that
  call `dispatch_item_op`/the new generic Transact functions this rung's
  own PR 2/3 build — never a generalization of `run_operation`,
  `execute_statement`, or `execute_transaction` themselves.

**Decision.** Build `SimCluster`-reachable, deterministic coverage for
`TransactWriteItems`/`TransactGetItems` and for
`ExecuteStatement`/`BatchExecuteStatement`/`ExecuteTransaction`, following
the identical widen-then-add-a-generic-entry-point template D3/D4 already
validated four times over.

**Non-goals.** Every production dispatch path stays byte-identical:
`run_operation`, `execute_statement`, `execute_transaction`,
`run_batch_execute_statement`, and `execute_one_batch_statement` are none
of them touched in *shape* — only widened in *type parameter* where PR 2
says so, with every existing real-socket regression run against the
widened code before anything is trimmed, per the D3 PR 3b precedent. The
new PartiQL entry points are strictly additive and parallel to the
existing ones, never a replacement for them, per the D2 lesson this
amendment already restates above.

**The PR series**, with a per-PR signature budget so each stays reviewable
on its own:

- **PR 1 (this amendment).** Docs only: this ADR amendment, the C-06
  roadmap entry, and this file's own D-train table row.
- **PR 2 — Transact groundwork.** Widen nine functions to `<E: Env, R:
  RelayClient>`: `run_transact`, `run_transact_get`,
  `quiescent_multi_get`, `transact_write_idempotency_preflight`,
  `ensure_txn_idempotency_table`, `idempotency_claim_put`,
  `read_idempotency_record`, `record_transact_write_outcome`, and
  `idempotency_record_item`. Convert `ensure_txn_idempotency_table`'s six
  wall-clock sites to `ctx.env.now().saturating_add(..)`/`ctx.env.sleep(..)`.
  Zero new mechanism — a pure signature-and-timer conversion, the D3
  PR 2a/2b shape exactly.
- **PR 3 — Transact reachable from `SimCluster`.** `execute_item_op_as`'s
  `matches!` (~1742) gains `Operation::TransactWriteItems { .. } |
  Operation::TransactGetItems { .. }`, routing both to the now-generic PR 2
  functions — `run_operation`'s own arms untouched. A new
  `sim_cluster_dynamo_transact.rs` sibling module, ~6-8 scenarios: a
  commit that includes a `ConditionCheck` action; a condition failure
  surfacing the right per-action `CancellationReasons`; a
  `ClientRequestToken` idempotent retry; `TransactGetItems`'s snapshot
  behavior against a concurrent writer; a transaction issued from a node
  hosting no replica of any touched tablet (proving the forward path); and
  the idempotency-table bootstrap race between two callers that are both
  first to need it (see Risks, below).
- **PR 4 — Transact in the wire corpus.** `sim_cluster_dynamo_corpus.rs`
  gains a list-append `TransactWriteItems` op feeding the same
  `Recorder`/`History`/`check_cycles`/`check_durability`/`check_convergence`
  model the corpus's other ops already feed, and a `TransactGetItems` probe
  following `GetItem`'s own `ConsistentRead: true`/`false` split (only
  `true` feeds `check_cycles`, per this corpus's existing read-consistency
  modeling decision). `ANIMUS_DYNAMO_WIRE_SEEDS=25` run once, matching the
  bar every D2/D3 PR that touched this corpus already cleared.
- **PR 5 — PartiQL siblings.** New parallel generic entry points —
  `execute_statement_as`/`execute_transaction_as`/
  `run_batch_execute_statement_as` — built over the pure
  `crates/animus-dynamo/src/partiql.rs` lowering functions,
  `dispatch_item_op`/PR 2's generic Transact functions, and the existing
  pure reshapers (`reshape_write_response_to_execute_statement` ~7048,
  `reshape_query_scan_response_to_execute_statement` ~7071, `first_item`
  ~7104) unchanged. ~4 new signatures; `execute_statement`,
  `execute_transaction`, `run_batch_execute_statement`, and
  `execute_one_batch_statement` themselves stay untouched.
- **PR 6 — PartiQL sim tests.** ~8-10 scenarios in a new
  `sim_cluster_dynamo_partiql.rs`: `SELECT` with an exact-key WHERE and
  with a range WHERE; `INSERT` including `ON CONFLICT DO NOTHING` and the
  plain `DuplicateItemException` path; `UPDATE`/`DELETE` with `RETURNING`;
  `BatchExecuteStatement` with one failing statement among several (no
  cross-statement atomicity, mirroring `BatchWriteItem`); `ExecuteTransaction`
  both all-`SELECT` and all-write. Plus an optional corpus equivalence cell
  if PR 4's own corpus generalizes cheaply to a PartiQL-issued write.
- **PR 7 — docs close-out.** Update this ADR's D-train row, the
  `docs/roadmap.md` C-06 entry (deleted per that file's own maintenance
  rule once landed), and `crates/animusd/CLAUDE.md`'s residual inventory.

**Risks, named up front rather than discovered mid-series:**

- **The idempotency-table bootstrap race has never run under a
  fault-injecting simulator.** `ensure_txn_idempotency_table` is a
  propose-and-poll-to-commit dance any number of concurrent first callers
  can race into simultaneously (nothing serializes callers before the
  schema check); production has run this path for a long time without an
  incident report, but "no incident report" is not the same bar as "proven
  under `SimEnv` fault injection" — budget for a real finding here, the
  same way D3 PR 2a/3a each found one (a heartbeat gap, a tablet-id
  collision) the moment its own generalized surface got its first
  deterministic exercise.
- **`quiescent_multi_get`'s per-key non-blocking snapshot semantics** (ADR
  0018 §2) have to hold under a concurrent writer once genuinely
  fault-injectable — PR 3's own `TransactGetItems`-vs-concurrent-writer
  scenario is there specifically to give this a first deterministic proof,
  not to rubber-stamp an assumption.
- **`execute_one_batch_statement` must call the generic sibling, not the
  concrete `execute_statement`, once PR 5 exists** — the identical
  narrowed-split trap D2 PR 1 hit: if the widened PartiQL path is wired in
  as a caller of the *old* concrete function by mistake (or left calling
  it out of caution), `BatchExecuteStatement`'s `INSERT`/`UPDATE`/`DELETE`
  arm silently stays `ProdEnv`-only forever, with nothing failing loudly to
  say so.

**Gates, per PR:** `cargo fmt --all --check`; `cargo clippy -p animusd
--all-targets --all-features -- -D warnings`; `cargo build -p animusd
--all-targets`; `cargo test -p animusd --lib`; real-socket sanity,
run-only, on the `tests/*.rs` binaries this rung's own conversions are
adjacent to. **`cp_txn.rs` and `dynamo_txn_idempotency.rs` are open-flake
files (issue #298) and must never be edited by this series** — run them
for sanity, do not touch their content, and do not fold their own
conversion into this rung even if a PR's diff would make it tempting;
that stays #298's own scope.

**Website: no change needed.** `website/`'s transaction and PartiQL claims
(`compatibility.html` et al.) describe wire-level behavior — what
`TransactWriteItems`/`TransactGetItems`/`ExecuteStatement`/
`BatchExecuteStatement`/`ExecuteTransaction` do against a real cluster —
and stay true throughout this series, since every production dispatch
path is byte-identical per this amendment's own non-goals. This rung adds
a second, `SimEnv`-level deterministic proof underneath an already-true
site claim; it does not change what the site claims.

**As-built: C-06 PR 3 (2026-09-07), Transact reachable from `SimCluster`.**
`dynamo.rs::dispatch_item_op` (ADR 0061 rung D2 PR 1's generic item/query
core) gained two match arms — `Operation::TransactWriteItems`/
`Operation::TransactGetItems`, calling [`run_transact`]/[`run_transact_get`]
(PR 2's own widened functions) the identical call shape `run_operation`'s
own arms already use. `run_operation`, `execute_statement`,
`execute_transaction`, and `run_batch_execute_statement` are byte-identical
— zero changes, matching this rung's own non-goals and the D2 lesson (a
narrowed generic split must never become the production dispatcher's only
path). Two small new `SimCluster` primitives, `txn_prepare_only` (stage one
write of a raw 2PC transaction via `ClientCtx::txn_prepare` directly,
deliberately never deciding — this fixture's own way of expressing "the
coordinator crashed right after prepare," mirroring `cp_txn.rs`'s own
`prepare_via_any_node` idiom) and `raw_get` (a routed read of an arbitrary
physical key), back scenario (g) below; every other scenario drives the
real DynamoDB wire JSON through `SimClusterHandle::dynamo`.

New module: `crates/animusd/src/sim_cluster_dynamo_transact.rs`, 7
scenarios, `_over_seeds` at 5 seeds each (14 tests total):

- (a) a commit of two `Put`s plus a passing `ConditionCheck` across two
  different tables, readable afterward with `ConsistentRead: true` from a
  different node — **green**, replayed with `ANIMUS_SEED` at seed
  `3228499975`(decimal)/`0xC06F_0001`(default).
- (b) a failing `ConditionCheck` cancels the whole transaction —
  `TransactionCanceledException` with per-action `CancellationReasons`
  (`["None","None","ConditionalCheckFailed"]`) and no partial write, even
  though both `Put`s precede the failing check in list order — **green**.
- (c) `ClientRequestToken` idempotency: a same-token retry after commit is
  cached (an `ADD` counter proves no re-run); a different payload under the
  same token is rejected `IdempotentParameterMismatchException` — **green**.
- (d) `TransactGetItems` never observes a torn pair under a concurrent
  writer (a background writer keeps two keys summing to zero; two
  concurrent readers assert every observed pair sums to zero) — driven as
  genuinely racing tasks before one shared `Simulator::run_for`, mirroring
  `SimCluster::dynamo_concurrent`'s own shape — **green**.
- (e) a transaction issued on a node hosting no replica of either table's
  tablet is forwarded and commits (a 7-node cluster with two RF-3 tables
  always leaves an idle node, found via `SimCluster::hosted_tablets`) —
  **green**.
- (f) the internal idempotency-table bootstrap race this amendment's own
  Risks section named up front: two token-bearing transactions from two
  different nodes racing `ensure_txn_idempotency_table`'s
  `CreateTableSchema` propose in the same tick
  (`SimCluster::dynamo_concurrent`) — **no product bug found**: exactly one
  proposal wins (`Metadata`'s own schema-catalog first-committer-wins
  exclusivity) and both transactions commit regardless of which won —
  **green**.
- (g) the coordinator stages (prepares) both participants of a cross-table
  transaction and never decides — a real `SimCluster::crash` of the
  coordinator with virtual time advanced between the last prepare and the
  crash, then a `SimCluster::restart`. **A real finding, `#[ignore]`d as a
  characterization test** — see below.

**Finding (g): a structural deadlock in `animus_node::sim_relay::
SimRelayClient`, not a `dynamo.rs`/coordinator/idempotency bug.**
`SimRelayClient::serve_loop` (a different crate, `animus-node`) is one task
per node processing inbound relay requests strictly sequentially, awaiting
each request's own handler *inline* before looping back to receive the
next message. Recovering an in-doubt transaction from a **forwarded** read
(`cp_get_local_resolving_inner`'s `FastRead::Foreign` arm →
`confirm_or_push` → `ClientCtx::txn_status`/`txn_recover`/`txn_verify`) can
need the *serving* node's own handler to issue a further, nested outbound
`relay()` call to a *third* node (whichever leads the anchor's own tablet)
before it can answer the first request — but that nested call's own reply
can only ever be delivered by the same `serve_loop` task that is currently
blocked awaiting the handler, a genuine self-deadlock resolved only by the
nested call's own timeout. Diagnosed directly, not inferred: every poll
attempt returns the identical `SimRelayClient::relay`-native timeout text,
unchanging across the full budget and every seed tried, while a
same-cluster-state plain (non-transactional) forwarded `GetItem` on a
different key succeeds immediately — confirming the failure is specific to
the nested-relay path, not general post-crash routing. This is the first
scenario in this codebase's `SimCluster` fixtures whose own forwarded
handler needs a second hop (every earlier scenario's forwarded op answers
locally once it reaches the right leader), and it is only reachable via a
crash forcing re-election away from whichever node originally led every
touched tablet plus a **forwarded** (not locally-served) read — exactly
scenario (g)'s own shape. Production's real `AnimusdRelayClient` has no
analogous bottleneck (each inbound TCP connection is its own
`tokio::spawn`ed task), so this is a `SimRelayClient`-only, fixture-only
limitation — never reachable in a real cluster. Fixing `SimRelayClient`
(spawning each inbound request's handler onto its own task instead of
awaiting it inline, production's own shape) is a change to `animus-node`,
a shared testing primitive every `SimCluster`-based module in this crate
depends on, and is out of this PR's own scope; both
`coordinator_never_finished_past_prepare_recovers_atomically` and its
`_over_seeds` sibling are kept as `#[ignore]`d characterization tests
(reproducing identically at every seed tried) rather than reworked or
dropped. **Issue to be filed** against
`animus_node::sim_relay::SimRelayClient`.

**Converted (ProdEnv → sim): none.** Every test in `dynamo_txn.rs` builds
on `create_table_pre_split` (a genuinely **split** table, proving
cross-tablet behavior specifically via a real split boundary) — this
fixture's `SimCluster` never splits a hand/wire-created table at all (no
scenario here mints more than one tablet per table), so none of that
file's tests reproduce exactly; scenario (a)/(b)/(d) above prove the
identical wire-level properties (atomic multi-table commit including a
`ConditionCheck`, per-action cancellation, `TransactGetItems` snapshot
consistency) over two independently-created tables instead, which is
complementary coverage, not a byte-for-byte duplicate. `dynamo_txn_
cancellation.rs`'s own tests each cover a narrower or different specific
mechanic scenario (b) does not reach — `ReturnValuesOnConditionCheckFailure`
echo, a *write* action's own `ConditionExpression` failing (not a
`ConditionCheck` action), an all-`ConditionCheck` transaction, the
successful-commit "no `CancellationReasons` field" negative assertion,
`TransactionConflict`, and the real split-plus-forwarding-hop case — none
is subsumed. `dynamo_execute_transaction.rs` stays untouched (PR 5/6
territory, per this amendment's own plan). `txn_recovery_participant_
spans.rs` stays: it drives raw internal `TxnPrepare`/`TxnDecide` wire
requests against a real 3-process cluster and asserts on real wall-clock
`RECOVERY_GRACE` timing — a `SimEnv`-vs-real-wall-clock distinction this
rung's own scenario (g) inherits identically (its own recovery timing is
virtual, not real), so this file's real-thread-timing angle is not
reproduced either. `cp_txn.rs` and `dynamo_txn_idempotency.rs` were run
for sanity only, never edited, per this series' own standing rule.

**Gates, as run**: `cargo fmt --all --check` (clean after one auto-fix);
`cargo clippy -p animusd --all-targets --all-features -- -D warnings`
(clean); `cargo build -p animusd --all-targets` (clean); `cargo test -p
animusd --lib` (353 passed / 3 ignored before this PR → 365 passed / 5
ignored after — +12 passed, +2 ignored, 0 regressions); real-socket
sanity on `cp_txn.rs`/`dynamo_txn.rs`/`dynamo_txn_cancellation.rs`/
`dynamo_txn_idempotency.rs`/`dynamo_execute_transaction.rs`/`txn_
recovery_participant_spans.rs` (37 tests, all green, both before and
after — unchanged since nothing in these six files was edited);
`ANIMUS_SEED` replay of scenario (a) (green at seed `1`, `3228499975`,
and the default) and scenario (g) (reproduces the finding identically at
seed `3228499975` and the default). `Cargo.lock` unchanged.

## 2026-09-07 amendment — #731 closed: `SimRelayClient::serve_loop` dispatches each inbound request onto its own task

The finding this file's "C-06 PR 3" amendment recorded above —
`animus_node::sim_relay::SimRelayClient::serve_loop`'s single-task, inline
dispatch deadlocking on a nested outbound `relay()` call from inside a
forwarded request's own handler (issue #731) — is fixed.

**The fix**: `serve_loop` no longer `.await`s an inbound
`RelayWire::Request`'s installed handler inline before looping back to
`recv_stream` for the next message. It now dispatches each `Request` onto
its own `env.spawn_task`ed task, mirroring `AnimusdRelayClient`'s own
production shape (one `tokio::spawn`ed task per inbound TCP connection —
the exact precedent this module's own doc already cited as the reason
production has no analogous bottleneck). This loop stays the sole reader
of `RELAY_STREAM` (single-consumer, ADR 0026 — that invariant did not need
relaxing, only the *handling* of what it reads became concurrent, never
the *receiving*), and also still delivers the reply to any of this node's
own outbound `relay()` calls (the module doc's "one stream, two roles"
section, unaffected). A `RelayWire::Reply` is still stashed inline,
synchronously, in the loop itself — only `RelayWire::Request` dispatch
moved onto a spawned task, since a reply-stash is cheap and non-blocking
and gains nothing from spawning.

**No new correlation mechanism was needed.** `RelayWire`'s `req_id` +
the pre-existing `Pending`-slot `BTreeMap` already handle an arbitrary
number of concurrent in-flight requests/replies — the *client* side of
exactly this concurrency was already proven by the pre-existing
`concurrent_outstanding_requests_resolve_to_the_right_callers` unit test.
The deadlock was purely a server-side sequencing bug: the correlation
story was already sound for concurrent use, it just couldn't be reached
because the receive loop itself was the bottleneck.

**Still fully deterministic and seed-reproducible.** Dispatch runs on
`env.spawn_task` — the seeded `Simulator`'s own single-threaded
cooperative executor, never a raw `tokio::spawn` (this crate has no
`tokio` dependency at all) — so introducing concurrency here did not
introduce nondeterminism; it only gave the already-seeded scheduler more
tasks to interleave among, per `animus-sim/CLAUDE.md`'s own account of how
that scheduler orders ready tasks and timeline events.

**Verification, and a second, deeper finding the fix itself uncovered.**
`cargo test -p animus-node`: 137 passed (unchanged — the fix touches no
public contract, only `serve_loop`'s own internal dispatch shape). An
ordinary forwarded read (no nested relay involved) was re-confirmed
unaffected. `ANIMUS_SIMCLUSTER_SEEDS=10 cargo test -p animusd --lib
sim_cluster_corpus` and `ANIMUS_DYNAMO_WIRE_SEEDS=10 cargo test -p animusd
--lib sim_cluster_dynamo_corpus` both stayed green — the relay is on every
forwarded op's path in both corpora, so this is the ordering/determinism
regression net for the concurrency change itself, confirming nothing
regressed.

`coordinator_never_finished_past_prepare_recovers_atomically` and its
`_over_seeds` sibling (`crates/animusd/src/sim_cluster_dynamo_transact.rs`,
scenario (g)) were un-ignored and re-run to directly confirm the deadlock
itself is gone: at both the original pinned seed and the first
`_over_seeds` seed, the poll loop no longer returns `SimRelayClient::
relay`'s timeout text at all — proof the nested relay hop this scenario
needs now succeeds. **But the scenario still does not converge**, for a
second, distinct, pre-existing reason this fix's own removal of the
deadlock made reachable for the first time: `ClientCtx::txn_recover`
(`crates/animusd/src/txn_coordinator.rs`) computes its own grace-check
`now_ms` as an *elapsed near-zero duration* (`self.env.now().
duration_since(self.env.now())`), not an absolute timestamp, whenever
`cp_route` resolves to anything other than `Local` — the ordinary case for
this scenario's own on-demand recovery push, which runs on the
*participant*'s tablet leader, not necessarily the *anchor*'s. A near-zero
`now_ms` makes the grace check's `now_ms < view.created_ts.wall_ms +
RECOVERY_GRACE` comparison true forever, so recovery declines
(`Pending`) on every call, permanently. This is pre-existing — introduced,
and knowingly left unfixed as out of scope, by rung C5 step 3b's `tokio::
time::Instant::now().elapsed()` → `Env` conversion above (see that rung's
own bullet: "reproducing the identical near-zero result rather than
'fixing' what reads like a pre-existing latent bug — an incidental bug
gets its own PR") — unrelated to and unmodified by this rung's relay fix.
It was unreachable before this rung only because issue #731's own deadlock
intercepted every recovery attempt before `txn_recover` was ever actually
called. Both tests are kept `#[ignore]`d, with the full diagnosis in their
own doc comment (`coordinator_never_finished_past_prepare_recovers_
atomically`'s own doc has the complete two-stage account); **issue #731
itself is closed** (the relay deadlock, this rung's own scope), and **a
new issue is to be filed** against `ClientCtx::txn_recover`'s non-local
grace-check branch, a different subsystem entirely.

`cargo test -p animusd --lib`: 365 passed / 5 ignored before this fix →
365 passed / 5 ignored after (0 regressions — the two tests stayed
`#[ignore]`d, now for the second, distinct reason above instead of the
first).

See `crates/animus-node/CLAUDE.md`'s matching "`sim_relay`'s dispatch
model" entry, `crates/animusd/CLAUDE.md`'s updated scenario-(g) account,
and `docs/engineering-lessons.md`'s matching entries (the general lesson
on a test double that serializes what production runs concurrently hiding
deadlocks only nested calls reveal, and the follow-up on how fixing one
such deadlock can unmask an independent bug underneath it) for the full
record. `docs/roadmap.md`'s C-06 entry is updated to note issue #731's own
closure and the new finding; PRs 4-7 of that series (the wire corpus,
PartiQL siblings, PartiQL sim tests, docs close-out) remain open and
unrelated to either.

## 2026-09-07 amendment — #737 closed: `ClientCtx::txn_recover`'s non-local grace check now reads an absolute timestamp

The second, distinct finding the #731 fix above uncovered — `ClientCtx::
txn_recover`'s non-local grace-check branch (`crates/animusd/src/
txn_coordinator.rs`) computing `now_ms` as the elapsed gap between two
back-to-back `self.env.now()` reads (near-zero, forever) instead of an
absolute timestamp — is fixed.

**The fix**: both `txn_recover` call sites (the orphan-record branch and
the ordinary decided-record branch) now share one free function,
`recovery_grace_now_ms<E: Env>(env: &E, route: &CpRoute<E>) -> u64`, which
reads `leader.env().now().0 / 1_000_000` on `CpRoute::Local` (unchanged —
this branch was always correct) and `env.now().0 / 1_000_000` on every
other route — the pusher's own absolute virtual/monotonic time, converted
to milliseconds by the identical integer division `animus_cp_data::hlc::
Hlc::mint` uses when it produces `wall_ms` in the first place (`Hlc::
now_ms`, `hlc.rs`). This is the load-bearing correction: the bug was never
about *which* clock (both the buggy and the fixed code read `env.now()`
under `SimEnv`/`ProdEnv` alike) — it was about *what quantity* the read
produced. `Hlc::mint`'s `wall_ms` is an absolute reading of `env.now()` at
mint time; the grace check compares a *later* absolute reading against it
plus `RECOVERY_GRACE`. The pre-fix non-local branch instead computed
`t.duration_since(self.env.now())` where `t` was itself minted one
statement earlier — the elapsed gap between two adjacent reads, not a
comparable absolute value — which is always near-zero and so always
`< wall_ms + RECOVERY_GRACE`, declining recovery on every single call,
forever, once the record was more than an instant old. See `crates/
animusd/src/txn_coordinator.rs`'s own doc on `recovery_grace_now_ms` for
the full account, and `docs/engineering-lessons.md`'s matching entry for
the general lesson this generalizes to (a mechanical `Instant::now().
elapsed()` → `Env` conversion must preserve WHAT is measured, not just the
API — the very risk rung C5 step 3b's own doc already flagged and
deliberately deferred, see that rung's bullet above).

**Sharing one helper is what makes the two call sites unable to diverge
again** — before this fix, both sites independently duplicated the
identical buggy `duration_since` computation (copy-pasted, per the rung
C5 step 3b conversion notes), so a future partial fix to only one site
would have been a real, silent risk; now there is exactly one place this
comparison's "now" can be computed.

**Verification**: a new unit test module, `txn_coordinator::
recovery_grace_tests`, drives `recovery_grace_now_ms` directly off a bare
`SimEnv` (no `ClientCtx`/`CpGroup` fixture needed, since the function reads
nothing but `env`/`route`) — asserting the grace check still holds
immediately after minting (`CpRoute::None`, the non-local shape) and
clears once the simulator's own virtual clock has advanced past
`RECOVERY_GRACE`, plus that the `Local` and non-local arms agree when
driven off clocks minted from the same node. `coordinator_never_finished_
past_prepare_recovers_atomically` and its `_over_seeds` sibling (`crates/
animusd/src/sim_cluster_dynamo_transact.rs`, scenario (g)) are un-ignored
and renamed to `coordinator_crash_after_prepare_recovers_atomically_to_
commit`(`_over_seeds`) — both now converge: a strong read of the
participant key from a different, live node triggers `confirm_or_push`/
`txn_recover` on demand once the record has sat `Pending` past
`RECOVERY_GRACE`, and both the participant and anchor keys converge
together, atomically, with the restarted coordinator's own view agreeing
too. Confirmed at the original pinned seed `0xC06F_0007` (= `3228499975`)
and the `_over_seeds` five-seed loop, each run twice for determinism.
`cargo test -p animusd --lib`: 365 passed / 5 ignored before this fix →
367 passed / 3 ignored after (the two tests move from ignored to passing;
the three remaining `#[ignore]`d tests are unrelated flake-issue
characterizations, unmodified by this fix).

**Audit of every other `env.now()`/`duration_since`/`wall_now` read in
`txn_coordinator.rs`/`write_path.rs`/`read_path.rs`/`schema.rs`/
`forwarding.rs`** (rung C5 step 3b's conversion targets) found no second
instance of the same mistake: every other site is either a deadline/
retry-loop pattern (`let deadline = self.env.now().saturating_add(TIMEOUT);
... self.env.now() >= deadline`, or `deadline.duration_since(self.env.
now())` to compute remaining budget) — comparing two `env.now()`-derived
absolute values against each other, which is sound — or a throttle-bucket
`now` parameter (`ThrottleTracker::check_read`/`check_write`/`charge_read`,
ADR 0065) that is purely self-consistent (the bucket's own internally
stored `last: Nanos` is always compared against a fresh `env.now()` read,
never against an unrelated absolute value like an HLC `wall_ms`). None of
these compare an `env.now()` reading against a *stored*, previously-minted
absolute timestamp the way the grace check does, so none carried the same
risk. See `crates/animusd/CLAUDE.md`'s matching account for the per-file
breakdown.

Docs updated in the same change: `docs/adr/0018-cross-tablet-transactions.
md` (a dated amendment on `RECOVERY_GRACE`'s own clock requirement),
`docs/roadmap.md`'s C-06 entry (issue #737's own closure, alongside #731's),
`crates/animusd/CLAUDE.md` (the scenario-(g) account rewritten to drop the
`#[ignore]` caveat), and `docs/engineering-lessons.md` (the general lesson).
`Cargo.lock` unchanged.

## 2026-09-08 amendment — Rung F, PR 4 landed: Transact ops in the `SimCluster` DynamoDB-wire corpus, and three fixture bugs found (all classified (b), all fixed)

**What's covered.** `sim_cluster_dynamo_corpus.rs`'s randomized op mix
(the actual end-to-end `Recorder`/`History`/`check_cycles` corpus from D2
PR 2, above) gains `TransactWriteItems` and `TransactGetItems`, riding the
identical 8-cell fault matrix (`baseline`/`leader_crash`/`follower_crash`/
`stop_restart`/`leader_partition`/`split_brain`/`forward_heavy`/
`two_tables`) every other op in that corpus already does — closing the
first of D2 PR 1's two named residuals for the wire *corpus* specifically
(C-06 PR 3 had already closed it for the plain `SimCluster` smoke tier;
this PR is the fault-injecting corpus's own turn). `TransactWriteItems`
draws its two write keys from the same client's own already-owned key set
(preserving the single-writer-per-key discipline across both write
mechanisms), issuing two `SET items = list_append(..)` `Update` actions
plus an always-passing `ConditionCheck` against a per-table `__guard__`
marker, sometimes carrying a fresh `ClientRequestToken`; both appends
enter the shared history as ONE atomic `invoke`/`ok` entry, so atomicity
is checked by construction — `check_cycles` cannot observe one half
landing without the other. `TransactGetItems` reads two keys from the
full modeled keyspace and always feeds the shared history (real DynamoDB
gives it no `ConsistentRead` parameter at all, unlike plain `GetItem`), so
isolation with respect to every other op in the file (plain writes,
`Query`/`Scan`) falls out of the same `wr`/`rw` graph for free. A
dedicated, deterministic probe (`run_transact_probe`, run from every node
post-heal/drain like the existing delete/batch-write probes) proves the
two shapes the randomized workload cannot safely produce without breaking
the corpus's own modeling invariants: a genuinely failing
`ConditionCheck` (all-or-nothing atomicity) and `ClientRequestToken`
idempotency/mismatch (a cached-outcome retry, and a same-token
different-payload rejection).

**What's deliberately not covered (residuals, named explicitly, not
silently skipped)**: a `Delete` action inside a random transact write
(would tombstone one of the same two owned keys the list-append model
tracks, breaking its prefix invariant — the identical reason plain
`DeleteItem`/`BatchWriteItem` already stay out of `check_cycles`) and a
genuinely *reused* `ClientRequestToken` (a non-repeatable workload shape
no per-round random draw can safely produce without either undercounting
`ok_writes` on a cache hit or manufacturing a value-reuse hazard this
corpus's own global-uniqueness discipline forbids) — both proven instead,
deterministically, by `run_transact_probe`. GSI/LSI `Query`/`Scan` and
PartiQL remain out of scope, unchanged from D2 PR 1/2 — PRs 5-7's own
territory.

**Three real `SimCluster`/`ClientCtx` fixture bugs found, all classified
(b) per the task's own found-bug protocol** — a fixture/model staleness
gap, never a defect in the transact protocol itself; each was
root-caused, not left as an unexplained flake, and each is now fixed:

- **Finding A**: a tokened `TransactWriteItems` auto-provisions the
  internal `__animus_txn_idempotency` table, which shifts every node's
  total hosted-tablet count enough to trigger a real
  `rebalance_placement` move of this corpus's own modeled table on the
  `dynamowire_forward_heavy` cell (4 nodes, RF 2) — a move
  `SimClusterHandle::replicas_of`'s creation-time-frozen snapshot never
  reflects, so `run_scenario`'s durability check read an empty,
  no-longer-a-replica engine and reported every acknowledged write
  against it as lost. Fixed by a new `live_replicas` helper (a live
  `SimCluster::hosted_tablets` query), wired into every durability-check
  call site that used to read the stale snapshot, plus the analogous fix
  to `Nemesis::FollowerCrash`'s own victim selection (`sim_cluster_dynamo_
  corpus.rs::live_replicas`'s own doc has the full account).
- **Finding B**: `SimCluster` never spawns `animusd::txn_resolver_loop`,
  and the plain-`GetItem` local-read path (`cp_get_local_resolving_
  inner`) never calls `confirm_or_push` for a LOCAL (same-tablet) `Pending`
  intent — only a foreign one does. A `TransactWriteItems` whose anchor
  and sole participant land on the SAME tablet ("self-transaction"),
  abandoned mid-flight by a fault (leader crash/kill/partition landing on
  that one tablet), therefore had no path to resolution in this fixture
  at all — `RaftKvNode::local_get`'s own documented "a `Pending`-covered
  key reads as absent" contract permanently masked the true committed
  value from this file's own raw-`local_get`-based durability oracle,
  reported as a lost write. Fixed by issuing one covering
  `TransactGetItems` after drain (`force_resolve_all_keys`) —
  `TransactGetItems`'s own read primitive (`cp_get_local_snapshot`)
  resolves a local intent exactly like a foreign one via the shared
  `Pending | Foreign` arm, so a covering read pushes any outstanding
  transaction toward its real decision as a side effect, safe to invoke
  any number of times on an already-decided key. Confirmed at seed
  `13022590114329469744` (`dynamowire_leader_crash_s22`).
- **Finding C**: `SimCluster::restart` rebuilds a restarted node's own
  `ctx.control` handle but, before this fix, never told that node's
  `ClusterEdgeState` about it — `ClientCtx::propose_schema`'s
  local-propose fast path reads `ctx.edge`'s own `control` registry, a
  *different* field with a different lifecycle than `ctx.control`
  (`crates/animusd/CLAUDE.md`'s `ControlHandle` entry). A restarted
  node's edge kept pointing at the OLD, `Simulator::stop`ped (dead)
  `RaftNode` for the rest of the scenario, so any NEW schema proposal
  issued through that node's fast path (`ensure_txn_idempotency_table`'s
  `CreateTableSchema`, needed by any tokened `TransactWriteItems`) spun
  until `SCHEMA_COMMIT_TIMEOUT` even with a real, healthy, reachable
  leader elsewhere — reproduced deterministically at seed
  `6659558302105598543` (`dynamowire_stop_restart_s02`), and shown to be
  general control-plane-restart infrastructure rather than anything
  transact-specific (a plain, non-transactional `CreateTable` issued from
  the restarted node reproduced the identical failure). **A first attempt
  at this fix — calling the pre-existing `ClusterEdgeState::
  register_control` from `restart` — left the identical scenario failing**:
  `register_control`'s own contract is "called once per node's whole
  lifetime" (true in production, where a restart always builds a
  brand-new `ClusterEdgeState`), so calling it a second time on the SAME,
  reused `Arc<ClusterEdgeState>` (this fixture's own `restart` design,
  ADR 0061 rung D4 PR 1) appended a SECOND handle rather than replacing
  the first — `leader_handle()`'s `find` could then return the stale,
  frozen-at-its-last-belief handle ahead of the fresh one, silently
  reproducing the exact bug the append was meant to fix. The real fix is
  a new method, `ClusterEdgeState::replace_control` (`crates/animusd/src/
  lib.rs`, `#[cfg(test)]`-only — no production restart path reuses a
  `ClusterEdgeState` this way), which clears the registry before pushing,
  so a restarted node's edge holds exactly one control handle at all
  times, the same invariant every other node in the cluster maintains for
  its whole lifetime.

**A resource-scale finding filed, not fixed: peak memory grows with
depth on this rung's own 15 GiB development sandbox.** The default depth
(8 scenarios) and `ANIMUS_DYNAMO_WIRE_SEEDS=4` (32 scenarios, 460s wall)
both complete cleanly with modest memory. At `=12` (96 scenarios) process
RSS was observed climbing through ~6.3 GiB at the ~10-minute mark and on
to a ~13.8 GiB plateau by ~30-37 minutes; at `=25` (200 scenarios) the
same ~13.8 GiB plateau was reached and the run was OOM-killed by the
kernel on this same 4 vCPU / 15 GiB / no-swap sandbox before it could
finish. `MALLOC_ARENA_MAX=1` did not visibly change the trajectory. The
plateau's rough independence from total scenario count once depth is
large enough is consistent with ordinary glibc allocator high-water-mark
behavior (freed memory not returned to the OS) rather than confirmed
proof of a true per-scenario leak — but per this task's own explicit
instruction, this was measured and reported, not root-caused further, in
this PR. `sim_cluster_dynamo_corpus.rs`'s own module doc carries the same
account for anyone extending this corpus next. **This does not contradict
the D2 PR 2 entry's own established "held green at `=25` in ~10m2s wall"
figure** — that figure predates this PR's own Transact addition and was
presumably measured on a materially better-resourced CI runner; this PR's
own gates (default depth, `=4`) are what actually ran to completion on
this sandbox and are reported as such, rather than claiming a `=25` run
this sandbox could not sustain.

**Gates**: `cargo test -p animusd --lib sim_cluster_dynamo_corpus --
--test-threads=2` at the default depth (3 passed, 1 ignored, ~114s, twice
— once before and once after the Finding C fix, the second green);
`ANIMUS_DYNAMO_WIRE_SEEDS=4` (3 passed, 1 ignored, 32 scenarios, 460s wall,
no memory issue); `cargo test -p animusd --lib -- --test-threads=2` (370
passed, 0 failed, 3 ignored — matches the established baseline exactly);
`cargo fmt --all --check` (clean); `cargo clippy -p animusd --all-targets
--all-features -- -D warnings` (clean); `cargo build -p animusd
--all-targets` (clean). `ANIMUS_DYNAMO_WIRE_SEEDS=25` (200 scenarios) was
attempted twice and OOM-killed both times on this sandbox — see the
resource-scale finding above; not a gate failure attributable to this PR's
own correctness. `git diff --name-only` against the branch point excludes
`Cargo.lock`.

**Docs**: this amendment; `docs/roadmap.md`'s C-06 entry (PR 4 landed);
`crates/animusd/CLAUDE.md`'s corpus description (the new Transact op mix
and the three findings); `sim_cluster_dynamo_corpus.rs`'s own new
"Corpus-fixture findings" and resource-scale module-doc sections carry the
full per-finding account, cross-referenced from all three.

## 2026-09-08 amendment — Rung F, PR 5 landed: the PartiQL handlers themselves reachable from `SimCluster`

Closes the second of D2 PR 1's two named residuals — `ExecuteStatement`/
`BatchExecuteStatement`/`ExecuteTransaction` (PartiQL, ADR 0071) — the
generic-dispatch analogue of what PR 3 did for `TransactWriteItems`/
`TransactGetItems`. This rung's own PR-series amendment above named the
real wrinkle up front: `execute_statement`'s `INSERT`/`UPDATE`/`DELETE`
arms recurse into the concrete, production-only `run_operation`
(`Box::pin(run_operation(ctx, principal, op)).await`, needed because
`run_operation` itself calls back into `execute_statement` for its own
`ExecuteStatement` arm — a pre-existing cycle, left untouched), ruling out
widening `execute_statement` in place, per this rung's own repeated D2 PR 1
lesson.

**What's now reachable.** Four new, strictly additive,
`<E: Env, R: RelayClient>`-generic siblings in `dynamo.rs` (a new "SimEnv-
capable PartiQL siblings" section header carries the full design):
`execute_statement_as` (copies `execute_statement`'s body verbatim; its
`SELECT` branch needed only `run_query`/`run_scan` themselves widened —
every one of their own callees was already generic or `Metadata`-only — and
its `INSERT`/`UPDATE`/`DELETE` branches call a new `dispatch_lowered_
write_as` helper, the identical `reject_internal_table`/`authz::
authorize_op` prelude `run_operation` runs ahead of its own dispatch to
`dispatch_item_op` for these three operations, then `dispatch_item_op`
directly); `execute_transaction_as` (a pure signature widening — every
callee was already generic or `Metadata`-only, so unlike
`execute_statement_as` this needed no dispatch change at all);
`run_batch_execute_statement_as`/`execute_one_batch_statement_as` (the
identical pair, with the one load-bearing substitution this rung's own
Risks section named up front: the `INSERT`/`UPDATE`/`DELETE` arm calls
`execute_statement_as`, never the concrete `execute_statement`).
`dynamo::dispatch_item_op` gained three more match arms —
`Operation::ExecuteStatement`/`Operation::BatchExecuteStatement`/
`Operation::ExecuteTransaction` — calling these new siblings the identical
call shape `run_operation`'s own arms already use.

**The byte-identical guarantee.** `run_operation`, `execute_statement`,
`execute_transaction`, `run_batch_execute_statement`, and
`execute_one_batch_statement` are every one of them untouched in shape —
zero lines changed in any of their own bodies (`run_query`/`run_scan`
gained a type parameter, called identically by both the old concrete
callers and the new generic ones). The full 37-test real-socket suite
(`dynamo_partiql.rs`, `dynamo_execute_transaction.rs`) ran unmodified
against the widened code and stayed green, confirming it — the same
"convert nothing, run the existing suite, watch it pass" proof PR 3 used
for Transact.

**A second mutual-recursion cycle, distinct from `execute_statement`'s own,
found by the compiler (`E0733`) rather than anticipated in this rung's own
PR-series amendment above.** That amendment's own PR 5 description
predicted `execute_statement_as` would need no `Box::pin` ("`dispatch_
item_op` never calls back into `execute_statement_as`, so ... this needs
no `Box::pin`") — true in isolation, but wrong once combined with this
same PR's own `dispatch_item_op` change: `dispatch_item_op`'s new
`ExecuteStatement` arm calls `execute_statement_as`, whose `INSERT`/
`UPDATE`/`DELETE` arms call `dispatch_lowered_write_as`, which calls back
into `dispatch_item_op` — a genuine cycle one level up from the one the
first draft reasoned about. `cargo build` refused to compile with
`E0733: recursion in an async fn requires boxing`, naming the exact three
functions in the cycle. Closed by boxing `dispatch_lowered_write_as`'s own
call (`Box::pin(dispatch_item_op(ctx, principal, meta, op)).await`) — one
`Box::pin`, not three at `execute_statement_as`'s own call sites, since the
cycle has exactly one edge that needs breaking regardless of how many
paths lead into it. See `docs/engineering-lessons.md`'s matching entry for
the general lesson this generalizes to: whenever a new generic sibling is
added specifically so a dispatcher can route to it, check whether that
sibling (or anything it calls) can call back into the SAME dispatcher —
the recursion is invisible from either function's own local reasoning and
only checked by the compiler once both halves of the cycle exist in the
same build.

**New module: `crates/animusd/src/sim_cluster_dynamo_partiql.rs`**, 5
scenarios (`_over_seeds` at 5 seeds each, 10 tests total), each issued from
a non-leader node of a 3-node RF3 `SimCluster`: (a) `INSERT` then `SELECT`
sees it; (b) `UPDATE ... RETURNING ALL NEW *` then `DELETE ... RETURNING
ALL OLD *`; (c) a mixed `BatchExecuteStatement` (`INSERT`/`SELECT`/`UPDATE
... RETURNING`/`DELETE ... RETURNING`, the `DELETE` targeting the SAME
batch's own just-inserted key to prove in-batch sequential ordering) runs
every statement in request order with no cross-statement atomicity; (d) an
all-`INSERT` `ExecuteTransaction` commits atomically across two tables; (e)
a duplicate `INSERT` inside a transaction cancels the WHOLE transaction
with correct per-action `CancellationReasons`. See
`crates/animusd/CLAUDE.md`'s matching new appendix for the full scenario
account, including a small fixture gotcha found while writing it
(`SimCluster::tablet_of` only tracks tablets its own in-process
`create_table` minted, not one created over the real wire — a wire-created
table's tablet is looked up via `Metadata::tablets_for_table` instead,
mirroring `sim_cluster_dynamo_transact.rs`'s own identical lookup).

**No product bug found.** Deeper PartiQL fault-injection coverage (GSI/LSI
`SELECT`, a corpus equivalence cell) remains PR 6's own scope, unchanged
from this rung's own PR-series amendment above — this PR is reachability
smoke only, following PR 3's own precedent for Transact.

**Gates**: `cargo test -p animusd --lib sim_cluster_dynamo_partiql --
--test-threads=2` (10 passed); `cargo test -p animusd --lib --
--test-threads=2` (380 passed, 0 failed, 3 ignored — 370 baseline + this
PR's 10 new tests); `cargo test -p animusd --test dynamo_partiql --test
dynamo_execute_transaction` (37 passed, 0 failed); `cargo fmt --all
--check` (clean after one auto-fix); `cargo clippy -p animusd
--all-targets --all-features -- -D warnings` (clean); `cargo build -p
animusd --all-targets` (clean). `git diff --name-only` against the branch
point excludes `Cargo.lock`.

**Docs**: this amendment; `docs/roadmap.md`'s C-06 entry (PR 5 landed);
`crates/animusd/CLAUDE.md`'s new appendix (the full scenario account and
the `tablet_of` fixture gotcha); `docs/engineering-lessons.md`'s new entry
on the second mutual-recursion cycle.

## 2026-09-08 amendment — correcting the PR 4 "resource-scale finding": the OOM was a real per-test `Simulator`/`SimEnv` leak, not glibc allocator retention

PR 4's own amendment above (and `sim_cluster_dynamo_corpus.rs`'s matching
module doc) filed the `ANIMUS_DYNAMO_WIRE_SEEDS=12`/`=25` OOM as
"consistent with ordinary glibc allocator high-water-mark behavior... not
confirmed proof of a true per-scenario leak," explicitly not root-caused
further at the time. It has now been root-caused, in a dedicated
follow-up session, and the "glibc retention" explanation was **wrong** —
this was a genuine, provable per-test reference-cycle leak in
`animus-sim`'s own executor, unrelated to allocator behavior, that
happened to affect every `sim_cluster_*`-driven test in this crate (not
only the wire corpus), scaling with how many tests a single process runs
before exiting.

**Root cause**: `animus_sim::Simulator` holds one `Arc<Shared>`, and every
`SimEnv`/`Simulator` handle clones it. `Shared`'s own `SimState.tasks`
map — where every `Spawner::spawn`ed task's future lives — is stored
*inside* that same `Shared`. A perpetual task (any `loop { ..
env.sleep(..).await .. }` with no terminating condition — a Raft
heartbeat loop, a reconciler tick loop, `auto_split_loop`, the backup
janitor, every one of which `SimCluster::new` spawns per node) never
resolves, so its future is never removed from `tasks` — and that future
almost always captures a `SimEnv` (or a whole `Simulator` clone, per this
crate's own documented "`Simulator` is `Clone`" precedent), a strong
`Arc<Shared>` pointing right back at the state holding it. A genuine
reference cycle, entirely internal to the executor: dropping every
*external* `Simulator`/`SimEnv` handle a test held (the whole
`SimCluster` value going out of scope at the end of each `#[test]` fn, the
normal case) never frees anything once a single perpetual task has ever
been spawned on it, which is every `sim_cluster_*` scenario without
exception. One whole simulated cluster's worth of memory — every node's
metadata, every hosted CP group, the full accumulated trace — leaked per
test, for the remaining lifetime of the test **process**, which is
exactly the "no drop between tests, ~10 MB/s once `sim_cluster_*` starts"
shape observed both in PR 4's own investigation and, independently, in
the dedicated follow-up session that root-caused it.

**Proved with a `Weak`, not inferred from RSS** (this crate's own standing
rule: RSS assertions are flaky, and "memory didn't shrink" doesn't by
itself distinguish a true leak from allocator retention) —
`crates/animus-sim/tests/executor_leak.rs`: take a `Simulator::downgrade()`
`Weak` handle while a scenario with a spawned perpetual task is still
fully alive, drop every external handle, and the `Weak` still upgrades.
A sibling test proves the fix: call the new `Simulator::shutdown()`
(drains `SimState.tasks`/`task_owner` — the only two fields able to hold
a strong `Arc<Shared>` back-reference) before dropping the external
handles, and the same `Weak` no longer upgrades.

**Fix**: `animus-sim` gained `Simulator::shutdown()` (an explicit,
idempotent, callable-from-any-clone drain — not a `Drop` impl on
`Simulator` itself, which is deliberately `Clone` and so has no single
"last owner" moment to hook) and `Simulator::downgrade() -> WeakSimulator`
(the proof primitive above). `animusd`'s `SimCluster` (not `Clone`, and
the one type every `sim_cluster_*` scenario already owns for its whole
duration) gained `impl Drop for SimCluster { fn drop(&mut self) {
self.sim.shutdown(); } }` — zero changes to any of the ~30
`sim_cluster_*` sibling modules, since every one of them already lets its
`SimCluster` value drop naturally, success or panic-unwind alike, at the
end of each test.

**Measured effect** (foreground `/proc/<pid>/status` `VmRSS` sampling of
the animusd test binary itself, never a background process): `cargo test
-p animusd --lib sim_cluster_dynamo_partiql -- --test-threads=1` — peak
322 MB across the module's 10 tests, no per-test growth (the pre-fix
trajectory, measured on the 64-test PR 6 version of this same module,
climbed to ~3.7 GB at ~58 MB per test); `cargo test -p animusd --lib --
--test-threads=2` — the whole 383-test suite completes in 643s (a
two-thread run of this same suite had previously reached 13.9 GB and
page-fault-thrashed for 87 minutes, and a one-thread run was killed at
13.9 GB after 342 tests, both without finishing);
`ANIMUS_DYNAMO_WIRE_SEEDS=4 cargo test -p animusd --lib
sim_cluster_dynamo_corpus -- --test-threads=1` — 3 passed in 454s, where
the pre-fix trajectory (this amendment's own subject) was climbing toward
the same ~13.8 GiB plateau this correction addresses.
`ANIMUS_DYNAMO_WIRE_SEEDS=25` was not re-attempted in the follow-up
session (out of that session's own explicit scope, `=4` being sufficient
proof) but is now expected to hold flat too, since the fix is structural
(the task queue itself, not depth-dependent) rather than a

**`sim_cluster_dynamo_corpus.rs`'s own module doc, and PR 4's amendment
above, both need their "glibc allocator high-water-mark" framing read as
superseded by this correction** — left in place (this ADR is append-only)
rather than edited in place, per this repo's own convention for a finding
that turns out to be wrong: correct it forward, don't rewrite history.

See `docs/engineering-lessons.md`'s matching entry (filed the same day)
for the general lesson — an executor whose own task queue lives inside
the state a task's environment handle points back to is a reference cycle
the moment any task can run forever, which is the ordinary shape for a
distributed system's background loops; a `Weak`-based proof, not an RSS
number, is what actually distinguishes that from ordinary allocator
retention — and `crates/animus-sim/CLAUDE.md`'s "What's non-obvious"
section for the mechanism itself.

## 2026-09-08 amendment — the `Simulator` fix above was real but incomplete: a SECOND, independent reference cycle (`SimRelayClient`) was closed, found only after fixing a broken measurement

The amendment immediately above fixed a genuine cycle and reported it as
having closed the `sim_cluster_*` tier's leak — its own "Measured effect"
numbers (a 322 MB peak for `sim_cluster_dynamo_partiql`, a completing
383-test full-suite run) were real cargo output, but **the process
sampler that produced them was reading the wrong process**: its `pgrep -f
'target/debug/deps/animusd-'` pattern also matches the *invoking shell*
(the `cargo test -p animusd ...` command line itself, which contains that
same substring as plain text) as readily as the actual test binary, and
which one it happened to catch on a given poll was unstable — sometimes
the shell (a few MB, explaining figures like "~6 MB flat" from an earlier
draft of that measurement pass), sometimes an early, not-yet-representative
moment of the real binary (322 MB, closer to plausible but still not a
genuine peak-of-the-whole-run figure). **A real `animusd` test binary is
never a few MB resident** — that mismatch alone should have been (and, on
a later rerun, was) the tell.

A follow-up session, sampling correctly (anchored to the binary's own
absolute path — `pgrep -f '^/path/to/target/debug/deps/animusd-'`, which
a shell's own multi-word invocation can never match — with the first
sample sanity-checked to land in the hundreds of MB before trusting
anything after it), found the exact same monotonic RSS growth this whole
ADR section exists to fix, at the same rate, **completely unaffected** by
the `Simulator::shutdown()` fix above: 2.4 GB at 189s into
`sim_cluster_auto_split`, 10.8 GB at 656s, the run killed at 12.6 GB after
304 tests. The `Simulator`/`SimEnv` task-queue cycle fix was not wrong —
it is real, `tests/executor_leak.rs` still proves it — it was simply not
the *only* cycle keeping this tier's memory alive, and the broken sampler
never gave anyone a true reading to notice that against.

**The second cycle, root-caused once measurement was fixed**: `animus-
node`'s `SimRelayClient::serve(handler)` installs `handler` into `self.
handler: Arc<Mutex<Option<Arc<Handler>>>>`. Every real caller's handler
closure (`forwarding::handle_relayed_request`, closed over a cloned
`ClientCtx<E, R>`) captures a `ClientCtx` that itself owns a clone of the
*same* `SimRelayClient` (its own `relay: R` field) — and since
`SimRelayClient` is `Clone`-over-`Arc`, that captured `ctx.relay` shares
the identical `handler` `Arc` the closure is installed *into*. A closure
sitting inside an `Arc`'s own `Mutex`, itself holding a strong reference
back to that same `Arc`, is a self-contained cycle needing no help from
`animus-sim`'s task queue at all — entirely independent of the first
cycle, which is exactly why fixing only that one left this tier's
measured trajectory unchanged. `SimCluster::new` installs a `.serve()`
handler on every node unconditionally, so — like the first cycle — this
one fired on literally every `sim_cluster_*` scenario, from the very
first test that touches the fixture. A second, smaller instance of the
identical shape compounds it further: `SimCluster::restart` installs a
*fresh* handler on a *fresh* relay without ever clearing the one it
replaces, so every restart mid-scenario leaked one more whole
node-generation on its own, independent of the fixture's eventual `Drop`.

**Proved with the identical discipline, extended to a second `Arc`**: a
new `SimRelayClient::downgrade_handler() -> WeakHandlerSlot` (mirroring
`Simulator::downgrade`'s own shape) lets a test take a `Weak` onto the
handler slot before drop and assert it no longer upgrades after.
`crates/animusd/src/sim_cluster.rs::dropping_the_cluster_frees_every_
nodes_relay_and_edge_state` proves both the restart-time case (checked
immediately after a mid-scenario `SimCluster::restart`, not deferred to
the cluster's own eventual drop) and the final-drop case (every node's
current relay handler slot, plus two more `Weak`-checked `Arc`s —
`ClusterEdgeState`'s own `control`/`raftkv` registries — this cycle
transitively kept alive), confirmed red-before/green-after by temporarily
reverting the fix and rerunning.

**Fix**: `SimRelayClient::shutdown()` (new, `animus-node`) clears the
handler slot, dropping the closure and breaking the cycle. Called from
`SimCluster::restart` (on the OLD relay, before installing the fresh
one) and from `impl Drop for SimCluster` (on every node's CURRENT relay,
alongside the existing `Simulator::shutdown()` call — the two calls
address two unrelated cycles and both remain necessary).

**Measured, this time with the corrected anchored sampler**: `cargo test
-p animusd --lib sim_cluster_dynamo_ -- --test-threads=1` (the 140
dynamo-tier tests, ~150 modules matched) — RSS fluctuates per-test
(roughly 100–650 MB, driven by which scenario is running, never a
monotonic climb) and stays well under the 2 GB bound throughout, `140
passed; 0 failed; 1 ignored`, ~469s. The FULL `cargo test -p animusd
--lib -- --test-threads=2` suite — peak ~960 MB across the entire run
(under the 3 GB bound, and roughly 3x below it), `384 passed; 0 failed; 3
ignored` (the one additional test versus the prior amendment's 383 is
this fix's own new regression), a complete, un-truncated run — not one
that merely finished before something else killed it, the shape every
earlier "success" in this ADR section turned out to be measuring.

**This is not the first time this investigation's own measurement tooling
was the actual defect, not the subject under test** — see the "Correction"
framing of this amendment's own title, and treat any future "the leak is
fixed" claim in this area as provisional until the sampler itself has been
checked against a known-good baseline (a real test binary's own
documented minimum resident footprint), not just against whether the
number it reports happens to look plausible.

**Docs**: this amendment; `docs/engineering-lessons.md`'s matching
"Correction, same day" entry (the full sampler-bug account, alongside the
general lesson); `crates/animus-sim/CLAUDE.md`'s own correction note on
its `Simulator::shutdown` entry (pointing here — that crate's own fix is
unchanged and still correct, it just wasn't the whole story); and
`crates/animus-node/CLAUDE.md`'s new `SimRelayClient::shutdown`/
`downgrade_handler` section for the mechanism itself. `git diff
--name-only` against the branch point excludes `Cargo.lock`.

## 2026-09-08 amendment — Rung F, PR 6 landed: deeper deterministic PartiQL coverage, one `SimCluster` sibling per named real-socket LOGIC test

PR 5 (above) proved the generic PartiQL dispatch path — `execute_statement_
as`/`execute_transaction_as`/`run_batch_execute_statement_as`/
`execute_one_batch_statement_as` — exists and is reachable at all, with
five reachability-smoke scenarios. This PR is pure test authorship on top
of that dispatch, unchanged: **27 more `SimCluster` scenarios**, each named
identically to, and mirroring the exact wire request/assertion shape of,
one specific real-socket LOGIC test in `crates/animusd/tests/dynamo_
partiql.rs`/`dynamo_execute_transaction.rs` — statement semantics, error
mapping, pagination shape, index routing, and transaction cancellation
reasons. Every scenario carries a `_over_seeds` sibling at 5 seeds (27 × 2
= 54 tests), added to PR 5's own 10, for **64 tests total** in
`crates/animusd/src/sim_cluster_dynamo_partiql.rs`. No `dynamo.rs` change
of any kind — this PR touches no production dispatch code, only the new
test module and these docs.

**Scenario-by-scenario account** (grouped by the real-socket source file
each mirrors; the module's own top-of-file `//!` doc carries the identical
listing, kept in sync with this one by construction since both were
authored from the same coordinator-provided candidate list):

*From `tests/dynamo_partiql.rs` (20 scenarios)* — `SELECT` semantics:
`select_partition_equality_matches_query`, `select_begins_with_sort_key_
matches_query`, `select_sort_comparator_and_between_match_query_numeric_
ordering` (numeric `BETWEEN`/comparator ordering, not lexicographic),
`select_non_key_where_matches_scan_with_filter` (falls back to a filtered
`Scan` when the `WHERE` isn't a key condition), `select_projection_
narrows_returned_attributes`, `select_from_table_dot_index_queries_the_gsi`
(the `"table"."index"` PartiQL syntax routes to the GSI, exercising
`SimCluster::drain_gsi` since this fixture has no background drain loop),
`gsi_projected_attribute_updated_via_partiql_is_visible_through_index_
query` (an `UPDATE` through PartiQL keeps a GSI's projected attribute
current), `order_by_desc_matches_scan_index_forward_false` (`ORDER BY ...
DESC` lowers to `ScanIndexForward: false`), `pagination_next_token_
matches_query_last_evaluated_key_walk` (a PartiQL page token round-trips
through the same `LastEvaluatedKey` walk `Query` itself uses), `next_
token_rejected_when_replayed_against_a_different_statement` (a token
minted for one statement is rejected against another); `INSERT`/`UPDATE`/
`DELETE` semantics and error mapping: `insert_on_conflict_do_nothing_
swallows_duplicate`, `duplicate_insert_gives_duplicate_item_exception_and_
leaves_item_unchanged` (the two `ON CONFLICT` outcomes for the identical
duplicate-key case), `update_set_on_existing_item_with_returning_all_new`,
`update_of_missing_item_fails`, `update_with_non_key_where_term_as_
condition_met_and_unmet` (a non-key `WHERE` term lowers to a conditional
update, both the pass and fail side), `update_where_missing_partition_
key_is_a_validation_exception`, `delete_where_missing_sort_key_is_a_
validation_exception`, `unknown_table_is_resource_not_found`, `malformed_
statement_is_a_validation_exception` (a parse failure, not a semantic
one), `literal_value_in_where_is_rejected` (PartiQL literal comparisons
outside a key condition are out of scope and rejected, not silently
ignored).

*From `tests/dynamo_execute_transaction.rs` (7 scenarios)* —
`execute_transaction_write_commits_atomically_across_two_tables` (a fresh
sibling matching the real test's own name/shape; kept alongside PR 5's
similarly-shaped but differently-named `execute_transaction_commits_
across_two_tables` per this PR's instruction not to delete or rename PR
5's own scenarios), `execute_transaction_write_cancels_whole_on_duplicate_
insert`, `execute_transaction_rejects_zero_and_too_many_statements`,
`execute_transaction_mixed_select_and_insert_is_validation_exception`
(mixing a `SELECT` with a write statement in the same transaction's
statement list is rejected up front), `execute_transaction_client_request_
token_replay_is_cached` (idempotent replay under the identical
`ClientRequestToken` returns the cached result rather than re-executing),
`execute_transaction_all_select_returns_items_and_misses_in_order` (the
other legal shape — every statement a `SELECT` — is a genuine read
transaction, returning both hits and misses in list order rather than
rejecting the request), `execute_transaction_over_a_follower_connected_
node` (issued from a non-leader node by this module's own standing
construction already, so no special-cased fixture was needed beyond what
every other scenario here already does).

**Deliberately not converted, real-socket-only, and why**: `throttled_
table_throttles_a_partiql_insert` — throttle-window timing, named
out-of-scope in this rung's own PR-series amendment above; `SimCluster`'s
own `ThrottleBucket` corpus already lives in `sim_cluster_throttle.rs` with
no PartiQL-specific angle worth duplicating. `insert_then_select_sees_it`
and `delete_with_returning_all_old` (`dynamo_partiql.rs`) — already
subsumed by PR 5's own scenarios (a) and (b) respectively (the latter's
`DELETE ... RETURNING ALL OLD *` half is the identical assertion shape).
`delete_of_missing_key_is_a_silent_success`, `delete_returning_all_new_is_
rejected`, and every `batch_execute_statement_*` test beyond PR 5's own
scenario (c) (`one_failure_does_not_block_the_others`, `select_rejects_a_
sort_key_range`, `zero_and_over_cap_are_top_level_validation_exceptions`,
`through_a_follower_connected_node`) — genuine, distinct LOGIC tests with
no `SimCluster` blocker, simply not named in this PR's own coordinator-
provided candidate list; left for a future pass rather than converted
here. All of the above stay `ProdEnv`-only in `crates/animusd/tests/
dynamo_partiql.rs`/`dynamo_execute_transaction.rs`, both of which this PR
leaves byte-identical to `main` (confirmed by `git diff` against the
branch point printing nothing for either file).

**No product bug found.** Every converted scenario passed at its pinned
seed and every `_over_seeds` seed on first clean run — the generic
dispatch path PR 5 wired behaves identically to the concrete, real-socket-
only path for all 27 scenarios this PR adds, exactly as PR 5's own 5
scenarios already established for the smaller reachability slice.

**This PR's own gate was blocked on, and only became runnable once, the
two-cycle `sim_cluster_*` leak fix (the two amendments immediately above:
`Simulator`/`SimEnv`'s task-queue cycle, then the independent
`SimRelayClient` handler-slot cycle) landed.** A first checkpoint pass
(`cargo test -p animusd --lib sim_cluster_dynamo_partiql -- --test-
threads=1`: 64 passed, 423s) proved the module correct in isolation, but
the full `cargo test -p animusd --lib` run — needed to confirm this PR's
54 new tests don't regress anything else and to get a real peak-RSS number
across the whole suite — was not runnable at all before both leak fixes:
this module's own 54 extra `sim_cluster_*` tests were enough additional
per-test leakage to push the full-suite run's RSS past the sandbox's
ceiling before completion, the identical failure shape the leak-fix
amendments above document independently. With both fixes in place:

**Gates** (`ANIMUS_SEED=3228520449` on `insert_then_select_sees_it`, run
twice, gave identical output beforehand, confirming this module's own
determinism holds under replay): `cargo fmt --all --check` (clean); `cargo
clippy -p animusd --all-targets --all-features -- -D warnings` (clean);
`cargo build -p animusd --all-targets` (clean); `cargo test -p animusd
--lib -- --test-threads=2` (438 passed, 0 failed, 3 ignored, 811.16s; peak
resident memory ~968 MB across the run, sampled from the test binary's own
`/proc/<pid>/status` `VmRSS` — anchored `pgrep -f '^<abs-path>/target/
debug/deps/animusd-'`, per the sampler-bug lesson the leak-fix amendments
above record); `cargo test -p animusd --test dynamo_partiql --test dynamo_
execute_transaction` (37 passed, 0 failed — 30 + 7, 11.38s + 2.36s — the
real-socket regression proving `execute_statement`/`execute_transaction`/
`run_batch_execute_statement`/`execute_one_batch_statement` stayed
byte-identical throughout this PR, as they did through PR 5). `Cargo.lock`
unchanged.

**Docs**: this amendment; `docs/roadmap.md`'s C-06 entry (PR 6 landed, PR
7 — docs close-out — the sole remaining item); `crates/animusd/CLAUDE.md`'s
new matching appendix (the full scenario account, mirroring PR 5's own
appendix shape).

## 2026-09-08 amendment — Rung F closed: C-06 PR 7, the docs-only close-out

PR 7 is what its own row above and the PR-series amendment promised: no
source, test, or `Cargo` change — this ADR's own D-train row and this
amendment, `docs/roadmap.md`'s C-06 entry, and `crates/animusd/CLAUDE.md`'s
residual-inventory paragraph. Rung F (C-06) is now **closed**: both of D2
PR 1's named residuals — Transact and PartiQL — are reachable through
`dispatch_item_op`, exactly as this rung's own opening amendment (above)
set out to do.

**What the generic dispatch cores now cover.** `dynamo::dispatch_item_op`
(the D2 PR 1 generic item/query core, widened by this rung) now routes
seven kinds of operation to `SimCluster`-reachable, `<E: Env, R:
RelayClient>`-generic handlers: the original six item ops plus base-table
`Query`/`Scan` (D2), `TransactWriteItems`/`TransactGetItems` (PR 2/3, over
`run_transact`/`run_transact_get` widened and `ensure_txn_idempotency_
table`'s six wall-clock sites converted to `ctx.env`), and `ExecuteStatement`/
`BatchExecuteStatement`/`ExecuteTransaction` (PR 5, over four new parallel
generic siblings — `execute_statement_as`/`execute_transaction_as`/
`run_batch_execute_statement_as`/`execute_one_batch_statement_as` — since
PartiQL's own production functions recurse into the concrete `run_
operation` and could not be widened in place, per this rung's own Non-
goals). `run_operation`, `execute_statement`, `execute_transaction`,
`run_batch_execute_statement`, and `execute_one_batch_statement` are every
one of them byte-identical to their pre-rung shape — confirmed the whole
way by running the unmodified real-socket suites against the widened code
at every PR, never merely asserted.

**Two `SimCluster`/`ClientCtx` fixture bugs this series' own fault
injection found, both fixed as their own PRs rather than folded into this
rung's diff**: issue #731 (`SimRelayClient::serve_loop` awaited each
inbound forwarded request inline instead of dispatching it onto its own
task, deadlocking the moment a forwarded handler needed to make its own
nested outbound relay call — found by PR 3's coordinator-crash-recovery
scenario, fixed by #738) and issue #737 (`ClientCtx::txn_recover`'s
non-local grace-check read a near-zero elapsed-duration clock instead of
an absolute `env.now()`, permanently blocking recovery of a foreign
in-doubt transaction — a pre-existing bug #738's own fix uncovered, fixed
by #740 via a shared `recovery_grace_now_ms` helper on both call sites).
Both are general control-plane/transaction-recovery infrastructure, not
Transact-dispatch-specific, and both are closed.

**Two independent reference-cycle leaks, root-caused and fixed by PR
#753**, found while gating PR 6 (the largest `sim_cluster_*`-tier addition
of the series, 54 new tests, enough to push a full `cargo test -p animusd
--lib` run's resident memory past this sandbox's ceiling before
completion): `animus-sim`'s `Simulator`/`SimEnv` held every spawned task's
future inside the same `Arc<Shared>` a perpetual task's own captured
`SimEnv` points back to, so any driver loop that never terminates (every
`SimCluster` node runs several) kept that whole `Arc` alive for the rest of
the test process; and, independently, `animus-node`'s `SimRelayClient`
installed its serve handler inside an `Arc<Mutex<Option<Arc<Handler>>>>`
that the handler's own captured `ClientCtx` held a clone of the *same*
`SimRelayClient` back into — a self-contained cycle needing no help from
the first. Both were proved with a `Weak` handle (`Simulator::downgrade`/
`SimRelayClient::downgrade_handler`), not inferred from RSS alone (this
crate's own standing rule — see `crates/animus-sim/CLAUDE.md`), and both
are fixed: `Simulator::shutdown()` plus `SimRelayClient::shutdown()`,
called together from `impl Drop for SimCluster` and from `SimCluster::
restart`. **This corrects, rather than retracts, the "resource-scale
finding filed, not fixed" this file's own 2026-09-08 "Rung F, PR 4"
amendment recorded** — that finding's original "glibc allocator
high-water-mark" framing was itself wrong, per the two 2026-09-08
correction amendments above; left in place rather than edited, per this
ADR's own append-only convention. With both cycles closed: `cargo test -p
animusd --lib -- --test-threads=2` runs the full, un-truncated suite —
438 passed, 0 failed, 3 ignored, 811.16s, peak resident memory ~968 MB (PR
#753's own fix-branch measurement, before PR 6's own 54 tests, was 384
passed with peak ~960 MB — both figures from the same anchored-sampler
methodology, see PR #753's body and the two amendments immediately above
this one for the full account).

**`cargo test -p animusd --lib` test count across the series**: 353 passed
/ 3 ignored immediately before PR 3 (the count PR 3's own gates section
measured its own delta against) → 365 / 5 after PR 3 (+12, +2 ignored) →
367 / 3 after the #737 fix (the two ignored tests un-ignored) → 370 / 3 at
PR 4's own gate (unchanged from its own established baseline — PR 4 adds
op variety to existing corpus scenarios, not new `#[test]` functions; the
367→370 drift between PRs is ordinary mainline movement from unrelated
work landing on `main` in between, not anything this series touched) → 380
/ 3 after PR 5 (+10) → 438 / 3 after PR 6 (+54 from PR 6's own new
scenarios, plus a handful from the `animus-sim`/`animus-node` reference-
cycle regressions PR #753 added along the way — see the two 2026-09-08
leak-fix amendments above for their own exact deltas). Every `SimCluster`-
coverage step in the series itself was strictly additive with zero
regressions.

**Decision, stated explicitly so a later reader does not "clean up" these
files: `crates/animusd/tests/dynamo_partiql.rs` and `tests/dynamo_execute_
transaction.rs` are KEPT IN FULL, unconverted and untrimmed.** Every one of
their 37 tests still runs, unchanged, as the `ProdEnv` real-socket proof
that `run_operation`'s own production PartiQL dispatch — `execute_
statement`, `execute_transaction`, `run_batch_execute_statement`,
`execute_one_batch_statement`, never widened or replaced, only paralleled
by this rung's own new `_as` siblings — actually does what the generic
`SimCluster` siblings this rung built exercise. This is the D2 PR 1 lesson
this file's own "Rung F" opening amendment restated up front: a narrowed
generic split must never become the production dispatcher's only path, and
the way that invariant stays checked *going forward*, not just true at the
moment this series landed, is by keeping the real-socket suite as a
standing regression that both paths are run against on every future
change — not by trusting a one-time "ran it once, it matched" confirmation
and then deleting the proof. This mirrors D3 PR 3b's own identical
decision to keep `dynamo_indexes.rs::gsi_write_then_query` whole rather
than convert or delete it once its own sim twin existed (this file's D3
rung table entry, above). The same reasoning is why PR 3/4's Transact real-
socket sanity files (`cp_txn.rs`, `dynamo_txn.rs`, `dynamo_txn_
cancellation.rs`, `dynamo_txn_idempotency.rs`, `txn_recovery_participant_
spans.rs`) were run for sanity at every PR but never edited or trimmed —
`cp_txn.rs`/`dynamo_txn_idempotency.rs` additionally being open-flake files
(#298) this series was instructed not to touch regardless.

**Residual inventory after C-06**, updating the D3-closing residual table
(this file's own "D3 closed" amendment) and `crates/animusd/CLAUDE.md`'s
matching paragraph now that Transact and PartiQL are no longer on it:
admin/console/dashboard HTTP (10 files/66 tests), **Streams (3/28,
named next in line — the largest remaining unowned group after admin/
console/dashboard HTTP)**, TTL (1/9), the control/data role split (5/21),
`--config` bring-up (2/2), index DDL beyond plain `CreateTable` (9/30),
node assembly/raw `ClientRequest` (2/8), and the throttle-metric counters
(1/6). None of these eight groups has a rung against it today; the next
C-04/C-06-shaped rung that wants one should start here.

**Website: no change needed, verified again at this close.**
`website/compatibility.html`'s `ExecuteStatement`/`BatchExecuteStatement`/
`ExecuteTransaction`/`TransactWriteItems`/`TransactGetItems` rows describe
wire-level behavior against a real cluster, unaffected by this rung's own
non-goals; `website/index.html`'s simulator paragraph and `website/
articles/determinism.html` describe the deterministic-simulation story in
general terms that were already true before this rung and remain true
after it — neither names Transact or PartiQL as a gap this rung needed to
close.

**Gates**: none — documentation only, no `cargo` command run, `git status
--short` empty apart from the four docs files this PR touches.

**Docs**: this amendment (closing Rung F); `docs/roadmap.md`'s C-06 entry
closed; `crates/animusd/CLAUDE.md`'s residual-inventory paragraph updated
and a "C-06 closed" pointer added after the PR 6 appendix; `docs/
engineering-lessons.md`'s "keep the ProdEnv copy as the equivalence
regression" entry, if not already recorded.

## 2026-09-08 amendment — Rung G (post-C-06): Streams `SimCluster` dispatch (C-07), PR 1 (this docs-only opener)

Rung F closed C-06 (Transact, PartiQL), and its own closing residual
inventory named the next candidate explicitly: "**Streams (3/28, named
next in line — the largest remaining unowned group after admin/console/
dashboard HTTP)**." `docs/roadmap.md`'s matching C-06 close-out paragraph
says the same thing in one word — "**Streams (next in line)**." This rung
— informally "G," continuing the same post-C-04 payoff sequence Rung F
opened — claims it, tracked in `docs/roadmap.md` as **C-07**.

**Gap.** A read-only pass over this tree at this commit finds four
real-socket `tests/*.rs` files touching the DynamoDB Streams surface:
`dynamo_streams.rs` (15 tests), `stream_janitor.rs` (11 tests),
`console_stream.rs` (4 tests), and `stream_backfill_seed_filter.rs` (2
tests) — 32 tests total. The D3-closing residual inventory's own "Streams
(3/28)" figure is **three** of these four, not all four:
`dynamo_streams.rs` (15) + `stream_janitor.rs` (11) +
`stream_backfill_seed_filter.rs` (2) = 28, matching exactly.
`console_stream.rs`'s 4 tests are already filed under the separate
"admin/console/dashboard HTTP (10/66)" class in that same inventory — it
is the Streams tab's console HTTP surface (`docs/streams-notes.md`'s
"Console Streams tab" section), not a Streams-*dispatch* file, even though
it drives the same `ListStreams`/`DescribeStream`/`GetShardIterator`/
`GetRecords` wire ops the other three touch. This rung's own plan still
has to account for it (see Scenario disposition, below) but its residual
count belongs to the console/dashboard group, not this one — a distinction
worth stating plainly since it is easy to miscount otherwise. **A fifth
file, `tests/streams_e2e.rs` (12 tests), is explicitly excluded from this
rung's scope**: it is frozen behind an open flake issue (#298, joined by
#745 for this file specifically) and filed under the D3-closing
inventory's class (E) — "frozen behind an open flake issue" — never class
(D)'s "waiting on a `SimCluster` capability" group this rung addresses.
This series does not touch it, edit it, or attempt to convert any of its
scenarios; it stays real-socket `ProdEnv` for as long as #298/#745 stay
open, owned by whichever agent/issue closes those, not by this rung.

**Blockers, file/function precise** — from a read-only investigation of
the current tree, mirroring the shape of this file's own "Rung F" opener:

- **(a) `dynamo_streams.rs` is `ClientCtx`-concrete end to end.**
  `execute_as`, `run_operation`, `list_streams`, `describe_stream`,
  `get_shard_iterator`, `get_records`, `get_records_sealed`, and
  `get_records_open` all take `&ClientCtx` (`E = ProdEnv`, `R =
  AnimusdRelayClient`). Every callee these eight functions call is
  **already** generic — this is a fact from the tree, not a plan
  expectation: `authz::authorize`/`authz::authorize_unscoped`,
  `ctx.segment_store` (a non-`E` enum), `ClientCtx::
  read_stream_hot_records` (`schema.rs`), and `index_drain::hot_read<E>`
  are all `<E, R>`-agnostic or already generic today. The open-tail
  `StreamHotRead` forwarding path (`docs/streams-notes.md`'s "DynamoDB
  Streams wire edge" section) is likewise already generic. The plan
  expects this to be a pure signature-widening pass over the eight
  functions, zero new mechanism — the identical D3/D4/Rung-F template.
- **(b) `dynamo.rs::execute_routed_as` forks on the `X-Amz-Target` prefix
  before decoding into `Operation` at all.** A `DynamoDBStreams_20120810.*`
  target routes to `dynamo_streams::execute_as`; everything else goes to
  `execute_as` (the item API). `execute_item_op_as`/`dispatch_item_op` —
  the generic core `SimCluster` actually drives — sit **downstream** of
  `execute_as`'s own `wire::decode_request`/`run_operation` call, which
  means Streams operations can **never** reach them: the fork happens one
  layer above where item-op genericity starts. The plan expects a
  parallel entry point mirroring `execute_routed_as`'s own shape:
  `dynamo_streams::execute_streams_op_as<E, R>` (with `execute_as` staying
  byte-identical, calling it — the D2 PR 1 lesson applied to a second
  dispatcher) plus `SimClusterHandle::dynamo_streams(node, target, body)
  -> (u16, String)`, going through `animus_dynamo::streams_wire::
  decode_request` and an unauthenticated `Principal`, mirroring the
  existing `SimClusterHandle::dynamo` (`sim_cluster.rs:794`/`:2084`)
  byte-for-byte in shape.
- **(c) `dispatch_table_op` rejects a stream on `CreateTable`
  (`stream_view_type.is_some()`) and `UpdateTable`
  (`stream.is_some()`).** `dynamo.rs::enable_stream<E, R>` is **already
  generic** — a fact from the tree, not a plan expectation. Only
  `disable_stream(ctx: &ClientCtx, ..)` is concrete. The plan expects:
  widen `disable_stream`; add a stream-only `UpdateTable` sub-arm to
  `dispatch_table_op`, the same shape as the existing
  `update_table_throughput` arm (D3 PR 2b's own precedent), calling
  `enable_stream`/`disable_stream`.
- **(d) No shard seals run under `SimCluster` today.**
  `change_consumer_loop` — the sole caller of `seal_tick` — is never
  spawned by `SimCluster::new`/`restart`. `seal_tick(ctx: &ClientCtx, ..)`
  is concrete, but `seal_now<E, R>` (proposes `SealStreamShard`, writes
  `ctx.segment_store`) is **already generic** — a fact from the tree. The
  plan expects a new `SimCluster::drive_stream_seal(node)` calling
  `seal_now` per led streamed tablet directly, mirroring `SimCluster::
  drain_gsi`'s own on-demand-invocation shape (D3 PR 3b) rather than
  spawning a real perpetual loop — `seal_tick`/`change_consumer_loop`
  themselves are **not** widened by this plan, and the quiesced/`Building`
  guards those functions carry are not replicated by the on-demand call;
  this is a documented fixture gap, not a claim of equivalence.
- **(e) `ClientCtx::segment_store` is a placeholder
  `SegmentStoreHandle::Fs` under `SimCluster` today.**
  `SegmentStoreHandle::S3(Arc<dyn SegmentStore>)` already exists — the
  plan expects the identical trick D4 PR 5 already validated for the
  backup store: `SimCluster::new` builds one shared
  `animus_sim::SimSegmentStore`, and every node's `ClientCtx::
  segment_store` wraps `SegmentStoreHandle::S3` around a cheap clone of
  it (`SimSegmentStore::clone` is `Arc<Mutex<..>>`-backed); `SimCluster::
  restart` leaves the shared store untouched, matching the backup-store
  precedent's own restart semantics.
- **(f) `segment_janitor_tick(ctx: &ClientCtx, leader: &RaftNode<ProdEnv>,
  retention)` names `RaftNode<ProdEnv>` directly** — the subject of
  `tests/stream_janitor.rs`. The plan expects widening the tick and
  `segment_janitor_loop` to `<E, R>` (`RaftNode<E>`), the identical move
  D3 PR 2a made for `ClusterEdgeState::control`.
- **(g) `StreamSealKnobs` lives on `DataRole`, which is `Default` and
  `Env`-free.** `SimCluster` has had a real `DataRole` since D2 PR 1 —
  this is a fact from the tree: there is nothing to plumb here, the knobs
  already default correctly the moment a `DataRole` exists.

**Decision.** Build `SimCluster`-reachable, deterministic coverage for the
DynamoDB Streams read API (`ListStreams`/`DescribeStream`/
`GetShardIterator`/`GetRecords`), stream enable/disable via
`CreateTable`/`UpdateTable`, on-demand shard sealing, and the segment
janitor's two-phase retention sweep — following the identical
widen-then-add-a-generic-entry-point template D3/D4/Rung F already
validated five times over (D3 PR 2a/2b/3a, D4 PR 2/5, Rung F PR 2/5).

**Non-goals.** `execute_routed_as`, `execute_as`, `run_operation`,
`dynamo_streams::execute_as`, and every function this plan widens keep
their exact production shape — only the type parameter changes where a PR
says so, per the D2 PR 1 lesson this file already carries
(`docs/engineering-lessons.md`'s "A narrowed generic split of a dispatcher
must not become the production dispatcher's ONLY path" entry) and Rung
F's own restatement of it. `seal_tick`/`change_consumer_loop` are not
widened or spawned as a real loop under `SimCluster` — blocker (d)'s own
on-demand `drive_stream_seal` substitute is a deliberate, documented,
narrower fixture capability, not an equivalence claim. `console_stream.rs`
is out of this rung's own scope (its residual accounting sits with
admin/console/dashboard HTTP), and `streams_e2e.rs` is untouched
throughout, per the Gap section above.

**The PR series** (six PRs, stacked on the C-06 close-out), with a
per-PR gate line so each stays independently verifiable:

- **PR 1 (this amendment).** Docs only: this ADR amendment, the C-07
  roadmap entry, this file's own rung-table row, and
  `crates/animusd/CLAUDE.md`'s residual-inventory annotation. **Gates:**
  none — documentation only, no `cargo` command run.
- **PR 2 — Groundwork.** Widen `disable_stream`; add the stream-only
  `UpdateTable` sub-arm to `dispatch_table_op` (blocker (c)); wire
  `SegmentStoreHandle::S3` over a shared `SimSegmentStore` into
  `SimCluster::new`/`restart` (blocker (e)); add `SimCluster::
  drive_stream_seal(node)` (blocker (d)). **Gate:** `cargo test -p animusd
  --lib`; `cargo clippy -p animusd --all-targets --all-features -- -D
  warnings`; `cargo fmt --all --check`.
- **PR 3 — Streams read API reachable.** Widen the eight
  `dynamo_streams.rs` functions named in blocker (a); add
  `dynamo_streams::execute_streams_op_as<E, R>` and
  `SimClusterHandle::dynamo_streams` (blocker (b)); a new
  `sim_cluster_dynamo_streams.rs` module, roughly 8 scenarios covering the
  read path end to end: enable → open-tail `GetRecords` → `drive_stream_
  seal` → sealed `GetRecords` → an iterator surviving a seal mid-poll →
  cross-node forwarded reads → the disable grace window. **Gate:** `cargo
  test -p animusd --lib`; clippy/fmt as above; `tests/dynamo_streams.rs`
  and `tests/streams_e2e.rs` run unmodified and stay green.
- **PR 4 — `dynamo_streams.rs` siblings.** Convert the 12 sim-convertible
  tests named in Scenario disposition, below, into
  `sim_cluster_dynamo_streams*.rs` siblings; trim the source file, keeping
  the two tests that stay `ProdEnv`-only. **Gate:** same as PR 3, plus a
  before/after `--test dynamo_streams` test count.
- **PR 5 — Janitor under `SimCluster`.** Widen `segment_janitor_tick`/
  `segment_janitor_loop` to `<E, R>` (blocker (f)); spawn it
  unconditionally in `SimCluster::new`/`restart` over the shared
  `SimSegmentStore`, the D4 PR 5 backup-janitor shape; cover the new
  per-node loop in `SimCluster`'s own `Drop`/`restart` shutdown path, per
  issue #753 (see Risks, below); a new `sim_cluster_stream_janitor.rs`
  module, roughly 9 scenarios drawn from the Scenario disposition
  section's `stream_janitor.rs` breakdown. **Gate:** `cargo test -p
  animus-cp-data`; `cargo test -p animus-node`; `cargo test -p animusd
  --lib`; a before/after `--test stream_janitor` test count.
- **PR 6 — Docs close-out.** This ADR's rung-table row updated to
  "closed," a "Rung G closed" amendment, `docs/roadmap.md`'s C-07 status,
  and `crates/animusd/CLAUDE.md`'s residual inventory updated so Streams'
  own count drops to the tests that stay `ProdEnv` (two from
  `dynamo_streams.rs`, one from `stream_janitor.rs`, `console_stream.rs`'s
  four unaffected in the other group, `stream_backfill_seed_filter.rs`'s
  two unaffected). **Gates:** none — documentation only.

**Scenario disposition per file**, from the same read-only pass:

- **`tests/dynamo_streams.rs` (15).** 12 convert once blockers (a)-(e)
  land: DDL propagation/describe; the four `GetRecords`/iterator/
  pagination/cross-node tests; the two Transact-on-a-streamed-table tests
  (reachable because C-06 already made Transact dispatch generic);
  `bare_stream_hot_read_is_refused`; the disable-grace-window test; the
  pre-enable-marker test (`docs/streams-notes.md`'s "Consumer-hidden
  records" section); the sort-key-shape test. **Stays `ProdEnv` (2):**
  `set_table_stream_enable_propagates_and_survives_restart` (a genuine
  real-WAL restart, class B — real-disk durability `SimEnv` cannot prove);
  `disable_survives_concurrent_periodic_seal_on_local_route` (races the
  real periodic `change_consumer_loop`, which this plan deliberately does
  not spawn under `SimCluster` per blocker (d)'s own non-goal).
- **`tests/stream_janitor.rs` (11).** Most reachable after PR 5: two-phase
  expiry, the no-empty-success-gap-across-expiry property, the disable-
  grace lifecycle, the drop-table cascade, replica repair onto a fresh
  target (via `SimSegmentStore`'s fault-injection knobs — see Risks,
  below), the janitor's own metrics; a leader-kill-mid-sweep scenario via
  `SimCluster::crash`/`restart`, the D4 PR 5 shape. **Stays `ProdEnv`
  (1):** `segment_janitor_reclaims_objects_from_a_genuinely_control_only_
  leader` — a real control/data role-split residual, already counted in
  the separate "control/data role split (5/21)" class this rung does not
  claim.
- **`tests/console_stream.rs` (4).** Stays `ProdEnv` in full — a console
  HTTP surface, filed under the separate admin/console/dashboard HTTP
  residual, per the Gap section above.
- **`tests/stream_backfill_seed_filter.rs` (2).** Stays `ProdEnv` in
  full — its own blocker is `backfill_seed_tick`, filed under "index DDL
  beyond plain `CreateTable`," a separate, still-unowned residual group
  this rung does not claim; noted here only because the file lives in the
  Streams test-count neighborhood, per `docs/streams-notes.md`'s
  "Consumer-hidden records" cross-reference.
- **`tests/streams_e2e.rs`.** UNTOUCHED throughout the series — frozen
  behind #298/#745, per the Gap section above.

**Risks/gotchas, named up front rather than discovered mid-series:**

- **The D2 PR 1 generic-sibling lesson applies twice over in this rung**,
  not once: a narrowed generic split of `execute_routed_as` (blocker (b))
  must never become production's only path to Streams, and the same is
  true of the `dispatch_table_op` `UpdateTable` sub-arm (blocker (c)). Run
  every existing real-socket Streams suite against the widened code before
  trimming anything, per D3 PR 3b's own precedent and Rung F's own
  restatement.
- **Issue #753's Drop-coverage requirement applies to both new per-node
  loops this rung adds** — `drive_stream_seal`'s own call path and,
  especially, PR 5's `segment_janitor_loop` spawn. #753 fixed two
  independent reference cycles (`animus_sim::Simulator`'s task queue,
  `animus_node::SimRelayClient`'s handler slot) that a perpetual per-node
  loop can reintroduce if a new loop captures a `SimEnv`/relay handle and
  nothing calls `Simulator::shutdown()`/`SimRelayClient::shutdown()` on
  it; `impl Drop for SimCluster` and `SimCluster::restart` already call
  both, so the requirement here is narrower than it sounds — confirm the
  new janitor loop's own captured state is covered by those two existing
  calls rather than adding a third cycle they don't reach, and prove it
  with the same `Weak`-handle discipline (`Simulator::downgrade`/
  `SimRelayClient::downgrade_handler`) if anything looks new.
- **Memory claims only via the anchored sampler.** A `pgrep -f` pattern
  matched against a bare substring like `target/debug/deps/animusd-` can
  match the driving shell's own command line, not the test binary — the
  #753 postmortem's own "Correction, same day" lesson
  (`docs/engineering-lessons.md`). Anchor to the binary's absolute path
  (`pgrep -f '^/home/user/animus-db/target/debug/deps/animusd-'`) and
  sanity-check the first sample lands in the hundreds of MB before
  trusting anything the sampler reports.
- **Propose-then-crash scenarios need a `run_for` in between**, not a bare
  `crash()` immediately after a propose call — the coordinator/committer
  needs virtual time to actually advance and settle before the crash is
  meaningful, the same shape C-06 PR 3's own scenario (g) used.
- **Accessor staleness**: a `SimCluster` accessor that snapshots "who
  hosts this tablet" once and reuses it goes stale the moment a
  concurrent proposal shifts tablet placement — C-06 PR 4's own finding
  (A) (`SimClusterHandle::replicas_of` going stale mid-scenario). Any new
  Streams accessor this rung adds must re-derive from live
  `hosted_tablets`/`Metadata` state on every call, never memoize a
  snapshot from setup time.
- **`SimSegmentStore`'s fault-injection knobs stay off by default.** Only
  the janitor's replica-repair scenario (`stream_janitor.rs`'s
  repair-to-fresh-target test, per Scenario disposition above) turns them
  on deliberately; every other scenario in this rung runs against the
  fault-free default, matching this fixture's standing convention.

**Gates, per PR:** listed inline in the PR series above; no gate for PR 1
or PR 6 (docs only).

**Depends:** C-04 (closed), C-06 (closed) — the D3/D4 generic dispatch
cores (`dispatch_item_op`/`dispatch_table_op`) and the D2 PR 1
`SimCluster` harness this rung builds directly on, plus C-06's own
Transact widening, which is what makes two of `dynamo_streams.rs`'s tests
(the Transact-on-a-streamed-table pair) convertible in PR 4 at all.

**Website: no change needed.** `website/`'s Streams claims describe
wire-level behavior against a real cluster and stay true throughout this
series, since every production dispatch path is byte-identical per this
amendment's own non-goals — the same reasoning Rung F's own opener and
close-out amendments already applied to Transact/PartiQL.

## 2026-09-08 amendment — Rung G, PR 2 landed (groundwork)

PR 2's own scope — blockers (c), (d), (e) plus a first end-to-end smoke —
landed. `dynamo.rs::disable_stream` widened to `<E: Env, R: RelayClient>`
(its two `tokio::time` sites converted to `ctx.env.now()`/`ctx.env.
sleep(..)`, the identical `enable_stream`/`update_table_throughput`
precedent). `dispatch_table_op` gained a stream-only `UpdateTable` sub-arm
calling `enable_stream`/`disable_stream`, and `CreateTable`'s own arm
stopped rejecting a declared stream — tracing `create_table`'s own stream
branch found it already called the (already-generic) `enable_stream`, so
the rung opener's own open question ("check, and if that needs more than
the rejection removal, leave it rejected") resolved to "pure removal, no
new mechanism." `update_table`/`run_operation`/`execute_routed_as`/
`execute_as` stay byte-identical, per this amendment's own non-goals.

**A real gap the blockers above missed, found by the new smoke test, not
by inspection**: blocker (d)'s own text called `index_drain::seal_now`
"already generic — a fact from the tree," true of its *signature* but not
of its *body* — its commit-wait poll still read the wall clock via bare
`tokio::time::Instant::now()`/`tokio::time::sleep`, which panics under
`SimEnv` ("there is no reactor running") the instant a caller actually
reaches that loop. Nothing under `SimEnv` had ever called `seal_now`
before this PR, so a read-only pass had no way to see it. Fixed with the
identical `ctx.env.now()`/`ctx.env.sleep(..)` conversion `disable_stream`
above got. `pitr_seal_now` — `seal_now`'s structural twin — carries the
identical unfixed bug; it is unreachable from anything this PR wires up,
but a future rung driving PITR sealing under `SimCluster` will hit the
identical panic. See `docs/engineering-lessons.md`'s matching entry for
the general lesson: a function's generic type parameters prove nothing
about whether its body actually avoids the real clock/timer.

`SimCluster::new` now builds a second shared `SimSegmentStore` (independent
from the backup store) and wraps every node's `ClientCtx::segment_store`
in `SegmentStoreHandle::S3` around a clone of it; `SimCluster::restart`
leaves it untouched; `SimCluster::segment_store()` is the new accessor.
`SimCluster::drive_stream_seal(node)` calls the now-`SimEnv`-safe
`seal_now` per led, streamed tablet, looped to exhaustion with the same
bounded dueling-seal retry `inplace_split_driver_tick`'s own final seal
uses — see `crates/animusd/CLAUDE.md`'s matching appendix for the full
account of what it does and does not replicate (no `is_quiesced()`/
`Building`-child guard, both structurally unreachable here). No new
per-node loop was added, so issue #753's Drop-coverage requirement did not
apply — confirmed, not assumed: `segment_store` is plain `Arc<Mutex<..>>`
data with no captured `Env`/task handle, the identical reasoning
`backup_store` (D4 PR 5) already established with no `Weak`-handle
extension needed either.

One new sibling module, `sim_cluster_dynamo_streams.rs` (one scenario,
`_over_seeds` at 5 seeds): create → enable → write from a non-leader →
seal → assert a `stream_shards` row and a stored segment object → disable
from a different node → assert every node's catalog agrees. **No product
bug found beyond the `seal_now` clock conversion.**

**Gates**: `cargo fmt --all --check`, `cargo clippy -p animusd
--all-targets --all-features -- -D warnings`, `cargo build -p animusd
--all-targets` all clean; `cargo test -p animusd --lib -- --test-
threads=2`: 440 passed (438 + 2 new), 0 failed, 3 ignored, 831.76s, peak
RSS ~958 MB (anchored sampler); `cargo test -p animusd --test
dynamo_streams`: 15 passed, 0 failed — the real-socket equivalence
regression for `update_table`/`disable_stream`'s concrete path.
`Cargo.lock` unchanged.

## 2026-09-08 amendment — Rung G, PR 3 landed (the Streams read API)

PR 3's own scope — blockers (a)/(b) plus the read-path scenario suite —
landed. `dynamo_streams.rs`'s eight named functions (`execute_as`,
`run_operation`, `list_streams`, `describe_stream`, `get_shard_iterator`,
`get_records`, `get_records_sealed`, `get_records_open`) all widened from
`&ClientCtx` to `<E: Env, R: RelayClient>` — every one was already calling
only generic callees (`ctx.effective_metadata()`, `authz::authorize`/
`authorize_unscoped`, `ClientCtx::read_stream_hot_records`, and
`ctx.segment_store` — a plain non-`E`-typed enum), so this was a pure
move, not a rewrite, following the D2 PR 1/D3 PR 2a-2b/rung G PR 2
precedent once more. `execute_as` (the production entry `dynamo.rs::
execute_routed_as` still calls) is now a thin wrapper over a new generic
core, `dynamo_streams::execute_streams_op_as<E, R>`, monomorphized at `E =
ProdEnv, R = AnimusdRelayClient` by its own concrete signature —
production behavior unchanged, confirmed by the unmodified `tests/
dynamo_streams.rs` suite staying green (15 passed). Unlike
`dispatch_item_op`, nothing in this module turned out to be genuinely
`ProdEnv`-bound, so `execute_streams_op_as` covers all four Streams
operations with no `unsupported_by_generic_dispatch` gap.
`SimClusterHandle::dynamo_streams`/`SimCluster::dynamo_streams` mirror
their item-API `dynamo` siblings exactly, calling `execute_streams_op_as`
directly (an unrestricted `Principal`, mirroring `SimClusterHandle::
dynamo`'s own precedent of calling `execute_item_op_as` directly rather
than through the target-prefix fork).

`sim_cluster_dynamo_streams.rs` grew from 1 scenario (PR 2) to 9: 8 new
scenarios, each with a pinned seed and a `_over_seeds` sibling at 5 seeds,
every one issued from a non-leader node of a 3-node RF3 `SimCluster`
wherever the operation has a leader to forward to — `ListStreams`/
`DescribeStream` after enable; `GetRecords` over the open tail before any
seal (proving `get_records_open` → `read_stream_hot_records` →
`hot_read` forwarding, with an explicit assertion that no `stream_shards`
catalog row exists yet); `drive_stream_seal` then `GetRecords` over the
resulting sealed shard (proving `get_records_sealed` reads
`SegmentStoreHandle::S3`, `stored_ids()` asserted non-empty afterward); an
iterator minted before a seal continuing correctly across it (the
sealed-vs-open handoff, ADR 0042 §2); `Limit`-paginated exactly-once
traversal of a 5-record sealed shard; `LATEST`/`AT_SEQUENCE_NUMBER`/
`AFTER_SEQUENCE_NUMBER` (LATEST on a genuinely open shard, AT/AFTER on
that same content once sealed); the identical iterator token replayed
through every node of the cluster answering byte-identical results
(sealed-shard "any node serves it," ADR 0043 §A3); and F12-b's disable
grace window (`ListStreams` still lists the `DISABLED` label,
`DescribeStream` shows no open shard, its sealed reads keep working, a
never-existed label is `ResourceNotFoundException`).

**A real, non-obvious fixture gotcha found building four of the eight
scenarios (not a product bug)**: `DescribeStream` always appends a
tablet's still-open successor epoch behind a just-sealed one while the
stream stays `enabled` — a single `drive_stream_seal` call over a small
backlog therefore always leaves `Shards.len() == 2` (the sealed epoch plus
its fresh, empty open successor), never 1. Every affected scenario's
assertion now counts `sealed_count + 1` and always indexes the sealed
entry from the front (`shards[0]`, the array sorts ascending by epoch).
See `docs/engineering-lessons.md`'s matching entry for the general lesson
this generalizes to (any future Streams scenario that seals and then
inspects `DescribeStream` needs the identical `+1`).

**The ninth item this rung's own PR-3 line item named — a bare
`ClientRequest::StreamHotRead` refusal — was deliberately skipped, per
the "skip and say so if not [reachable]" instruction its own scope
carried.** `animus_node::sim_relay::SimRelayClient`'s inbound dispatch
(`forwarding::handle_relayed_request`) is not the mechanism the real
regression (`tests/dynamo_streams.rs::bare_stream_hot_read_is_refused`)
proves — that test exercises `handle_request`'s `Surface::Intra` port
guard and `cp_serve_forwarded`'s own bare-refusal match arm, neither of
which exists under this fixture's relay path (no `handle_request`, no
ports, no `Surface` classification under `SimEnv`).
`handle_relayed_request`'s own match is a three-arm allowlist
(`Status`/`Forwarded`/`ProposeSchema`) with a blanket `_ => "not
relayable under sim"` catch-all — a bare `StreamHotRead` sent through it
would be refused, but for a reason unrelated to the real mechanism, and
`sim_cluster.rs` exposes no raw-relay-send primitive to any sibling
module today besides. See `sim_cluster_dynamo_streams.rs`'s own module
doc for the full reasoning. The real mechanism stays covered by the
unmodified real-socket regression.

**Gates**: `cargo fmt --all --check`, `cargo clippy -p animusd
--all-targets --all-features -- -D warnings`, `cargo build -p animusd
--all-targets` all clean; `cargo test -p animusd --lib sim_cluster_
dynamo_streams`: 18 passed, 0 failed; `cargo test -p animusd --lib --
--test-threads=2`: 456 passed (438 + 18 new), 0 failed, 3 ignored,
862.19s, resident memory sampled every 10s via the anchored `pgrep -f
'^<abs-path>/target/debug/deps/animusd-'` pattern — first ~116 MB, peak
~844 MB, last ~181 MB; `cargo test -p animusd --test dynamo_streams`: 15
passed, 0 failed — the real-socket equivalence regression for
`execute_as`/`run_operation`'s concrete path. `Cargo.lock` unchanged.

## 2026-09-08 amendment — Rung G, PR 4 landed (`tests/dynamo_streams.rs` siblings)

PR 4's own scope — give every sim-convertible test in `tests/dynamo_
streams.rs` a deterministic sibling, then trim that file to the genuine
`ProdEnv` residual — landed. No `dynamo.rs`/`dynamo_streams.rs`/
`sim_cluster.rs` change was needed: PR 2's dispatch groundwork
(`dispatch_table_op`'s stream sub-arm, `SimCluster::drive_stream_seal`)
and PR 3's read API (`execute_streams_op_as`) already supplied every
primitive this PR needed; PR 4 is pure test authorship in
`sim_cluster_dynamo_streams.rs` (crate `animusd`, `src/`) plus the trim of
`tests/dynamo_streams.rs` itself.

**Disposition, 12 converted / 3 kept `ProdEnv`** (matching the plan
exactly, adjusted for PR 3's own finding that PR 2's list already had): of
the twelve, four are proven **in kind**, not duplicated, by scenarios PR 3
already built — `open_shard_iterator_survives_a_seal_and_keeps_working`
(scenario (d)), `limit_pagination_drains_a_sealed_shard_exactly_once`
(scenario (e)), `get_records_on_a_sealed_shard_works_from_every_node`
(scenario (g)), and `disabled_stream_grace_window_lists_and_serves_
sealed_reads_with_no_open_shard` (scenario (h)) — each cross-checked
assertion-by-assertion against its real-socket original before relying on
the equivalence. The remaining eight each got a genuinely new scenario:
`update_table_stream_enable_and_disable_through_every_node`, `describe_
table_returns_stream_spec_and_arn_reenable_mints_new_label`,
`transact_write_items_on_a_streamed_table_delivers_correct_events`,
`transact_write_items_abort_leaves_no_stream_event`, `get_records_walks_
the_shard_chain_and_drains_the_open_tail`, `get_records_on_an_open_shard_
forwards_correctly_from_every_node` (PR 3 had no every-node OPEN-shard
read — only every-node SEALED, scenario (g)), `pre_enable_marker_records_
never_surface_on_the_stream`, and `stream_keys_carry_n_sort_key_values_
across_mixed_magnitudes_and_signs`. Kept `ProdEnv`, exactly as the plan's
own disposition named, each with a one-line reason on the test itself and
in `tests/dynamo_streams.rs`'s own updated doc comment: `set_table_stream_
enable_propagates_and_survives_restart` (real WAL/restart durability),
`disable_survives_concurrent_periodic_seal_on_local_route` (races the real
periodic change-consumer loop's own timer, which `SimCluster` never
spawns), and `bare_stream_hot_read_is_refused` (the production port guard,
which PR 3 already found the simulated relay path cannot meaningfully
reproduce).

**The `tiny_seal_knobs`/`age_seal_knobs`/`never_seals_knobs` → `drive_
stream_seal` mapping** (documented once, in both `sim_cluster_dynamo_
streams.rs`'s own module doc and `crates/animusd/CLAUDE.md`'s matching
appendix, per this PR's own instruction): the real-socket knobs steer the
*periodic* seal arm, which `SimCluster` never spawns (PR 2's own finding).
A converted scenario instead calls `SimCluster::drive_stream_seal(leader)`
explicitly — trigger-free, sealing whatever is currently pending into one
epoch — once per desired shard for a `tiny_seal_knobs`-shaped chain, not
at all for a `never_seals_knobs`-shaped "stay open," and once over the
whole pending backlog for an `age_seal_knobs`-shaped "sweep it all
together" (already proven by PR 3's own scenario (e), which is exactly why
`limit_pagination_drains_a_sealed_shard_exactly_once` needed no new
scenario of its own).

**Two Transact scenarios reuse the C-06 PR 3 dispatch unmodified** —
`Operation::TransactWriteItems` already routes through `dispatch_item_op`
(generic since C-06 PR 2/3), so nothing in `dynamo.rs` needed touching;
both scenarios prove the change record materializes on the OPEN-tail read
path with no seal at all, since a streamed table's transactional write
resolves before `cp_txn` acks (ADR 0046 A1).

**No product bug found.** Every converted scenario passed at its pinned
seed and every `_over_seeds` seed on the first clean run.

**Gates, in the required order**: `cargo test -p animusd --test dynamo_
streams` on the untrimmed file — 15 passed (proving this branch's baseline
before removing anything, the D3 discipline); `cargo test -p animusd
--lib sim_cluster_dynamo_streams -- --test-threads=2` — 34 passed (18 from
PR 2/3 + 16 new); trim, then `cargo test -p animusd --test dynamo_streams`
again — 3 passed; `cargo fmt --all --check` (one auto-fix, then clean);
`cargo clippy -p animusd --all-targets --all-features -- -D warnings`
(clean); `cargo build -p animusd --all-targets` (clean); `cargo test -p
animusd --lib -- --test-threads=2` — 472 passed, 0 failed, 3 ignored,
901.75s (456 baseline + 16 new), resident memory sampled every 10s via the
anchored `pgrep -f '^<abs-path>/target/debug/deps/animusd-'` pattern —
first ~129 MB, peak ~902 MB, last ~180 MB, consistent with PR 3's own
figures. `Cargo.lock` unchanged.

## 2026-09-08 amendment — Rung G, PR 5 landed (`stream_janitor.rs` siblings)

PR 5's own scope — the segment janitor's own async loop (`segment_
janitor::segment_janitor_loop`, ADR 0043 §A9) widened to `<E: Env, R:
RelayClient>`, spawned unconditionally on every `SimCluster` node, and 9
of the 11 `tests/stream_janitor.rs` real-socket scenarios given
deterministic siblings — landed. `segment_janitor.rs`'s widening
(`segment_janitor_loop`/`segment_janitor_tick`/`update_segment_janitor_
progress`) is a pure signature change, mirroring `backup_janitor_loop`'s
own D4 PR 5 precedent; `reap_orphans` needed no change at all. `sim_
cluster.rs` gained a retention constructor knob (`SimCluster::new_with_
segment_janitor_retention`, default 3600s), the always-on spawn (mirroring
`backup_janitor_loop`'s own shape, not `auto_split_loop`'s opt-in one),
`segment_janitor_progress(node)`, and a new `grow_stream(node, table)`
driver primitive (wrapping `ClientCtx::grow_stream`, since this fixture
has no admin HTTP surface for `POST /admin/stream/grow`).

**Disposition, 9 converted / 2 kept `ProdEnv`**: `two_phase_expiry_
removes_the_row_and_every_replicas_object`, `expiry_survives_a_control_
leader_kill_mid_sweep`, `reader_never_sees_an_empty_success_gap_across_
expiry`, `disable_grace_lifecycle_end_to_end_with_reenable_coexistence`,
`drop_table_cascade_converges_via_the_janitor`, `mid_grace_drop_removes_
both_coexisting_labels`, `metrics_reflect_a_completed_retention_cycle`,
`retired_parents_shards_are_not_reaped_early`, and `retired_parents_
final_shard_expires_by_retention` all converted, each with a `_over_seeds`
sibling at 5 seeds — 18 tests total in the new `sim_cluster_stream_
janitor.rs` module. Kept `ProdEnv`, each with a one-line reason on the
test itself and in `tests/stream_janitor.rs`'s own trimmed doc comment:
`repair_re_replicates_to_a_fresh_target_after_a_replica_node_dies` (a
genuinely dead replica is a fault this fixture's shared `S3`-backed
segment store cannot express — that store has no per-node replica concept
at all, `row.replicas` is always empty for it) and `segment_janitor_
reclaims_objects_from_a_genuinely_control_only_leader` (needs a real
control-only/data-only process split; `SimCluster` has no notion of node
role — every node is the same shape).

**The `OP_BUDGET`-per-op-call timing gotcha (already documented for
earlier rungs) bit twice in this PR, once in the design and once in the
test authoring — both fixed, neither is a `segment_janitor.rs`/`sim_
cluster.rs` bug**:

- `metrics_reflect_a_completed_retention_cycle`/`two_phase_expiry_
  removes_the_row_and_every_replicas_object` both need to capture a
  sealed row's identity **immediately** after its own seal — a further op
  call (each burning the full 12s `OP_BUDGET`) can silently let a small
  `retention` mark-and-delete the row before the scenario ever reads it
  back.
- `two_phase_expiry_removes_the_row_and_every_replicas_object` additionally
  asserts the segment OBJECT still exists **immediately** after `drive_
  stream_seal` returns — with the scenario's own original `retention =
  2s`, `drive_stream_seal`'s own `spawn_and_capture` burning the *full*
  12s `OP_BUDGET` past the seal instant gave the janitor's 200ms tick
  ample room to mark-and-delete the object within that same call, before
  the assertion ran (`the segment object must exist right after its own
  seal: []`, reproduced deterministically at every seed). Fixed by
  raising this one scenario's own retention to 20s — comfortably clearing
  the 12s margin, still converging well inside the scenario's own 60s
  final poll. A scenario-local fix; no production code changed.
- `expiry_survives_a_control_leader_kill_mid_sweep` uses the general
  strategy this timing gotcha requires for catching a row genuinely
  mid-sweep: a retention deliberately larger than the setup phase's own
  computable cumulative `OP_BUDGET` cost, then a manual, small-step
  `SimCluster::run_for` poll loop (never another op call) to observe the
  marked-but-not-removed state precisely.

**No `segment_janitor.rs`/`sim_cluster.rs` bug found** — every fix in
this PR was scenario-local test-authoring, following the identical
timing discipline every earlier `sim_cluster_*` module already
documents.

**Gates, in the required order**: `cargo test -p animusd --test stream_
janitor` on the untrimmed file — 11 passed (proving production behavior
unchanged before removing anything, the D3 discipline); `cargo test -p
animusd --lib sim_cluster_stream_janitor -- --test-threads=2` — 18 passed
(after the one scenario-local retention fix above); trim, then `cargo
test -p animusd --test stream_janitor` again — 2 passed; `cargo fmt --all
--check` (clean after `cargo fmt --all`, which reformatted only the two
files this PR's own earlier turns had already touched — `sim_cluster.rs`
and `sim_cluster_stream_janitor.rs` — no file outside this PR's own
change set); `cargo clippy -p animusd --all-targets --all-features -- -D
warnings` (clean); `cargo build -p animusd --all-targets` (clean); `cargo
test -p animusd --lib sim_cluster -- --test-threads=2` — 315 passed, 0
failed, 2 ignored, 919.49s, resident memory sampled every 10s via the
anchored `pgrep -f '^<abs-path>/target/debug/deps/animusd-'` pattern —
first ~139 MB, peak ~880 MB, last ~166 MB, consistent with every prior
rung's own no-leak trajectory; `cargo test -p animusd --test dynamo_
streams --test stream_janitor` — 5 passed, 0 failed. `Cargo.lock`
unchanged.

## 2026-09-08 amendment — Rung G closed (C-07 complete)

PR 6 is what its own row above and the PR-series amendment promised: no
source, test, or `Cargo` change — this ADR's own D-train row and this
amendment, `docs/roadmap.md`'s C-07 entry, and `crates/animusd/CLAUDE.md`'s
residual-inventory paragraph. Rung G (C-07) is now **closed**: the DynamoDB
Streams read API, stream enable/disable, on-demand shard sealing, and the
segment janitor's two-phase retention sweep are all `SimCluster`-reachable,
exactly as this rung's own opening amendment (above) set out to do.

**What the rung set out to do.** Rung F's own close-out named Streams the
largest remaining unowned residual group after admin/console/dashboard
HTTP — three files, 28 tests (`dynamo_streams.rs` 15, `stream_janitor.rs`
11, `stream_backfill_seed_filter.rs` 2), plus `console_stream.rs`'s own 4
tests filed under the separate console/dashboard group. The opener's own
Gap section found `dynamo.rs::execute_routed_as` forks on the
`X-Amz-Target` prefix *before* decoding into an `Operation` — one layer
above where `dispatch_item_op`'s own genericity starts — so Streams could
never reach the D2/D3/D4/Rung-F generic dispatch cores through that path
alone; the plan was a parallel `dynamo_streams::execute_streams_op_as<E,
R>`/`SimClusterHandle::dynamo_streams` pair mirroring `SimClusterHandle::
dynamo`, plus the widen-then-add-a-generic-entry-point template D3/D4/
Rung F had already validated five times over, applied to
`dynamo_streams.rs`'s eight functions, `dispatch_table_op`'s stream-only
`UpdateTable` sub-arm, on-demand sealing, and the segment janitor's own
tick/loop.

**PR-by-PR, what landed.** PR 2 (groundwork): `disable_stream` widened;
`dispatch_table_op` gained a stream-only `UpdateTable` sub-arm (`create_
table`'s own stream branch turned out to already call the already-generic
`enable_stream`, so removing the rejection was pure deletion, no new
mechanism); `SimCluster::new` wired a second shared `animus_sim::
SimSegmentStore` into every node's `ClientCtx::segment_store` as
`SegmentStoreHandle::S3` (restart leaves it untouched, the D4 PR 5
backup-store precedent); `SimCluster::drive_stream_seal(node)` added,
calling `seal_now` per led streamed tablet to exhaustion. PR 3 (the
Streams read API): the eight `dynamo_streams.rs` functions
(`execute_as`, `run_operation`, `list_streams`, `describe_stream`,
`get_shard_iterator`, `get_records`, `get_records_sealed`, `get_records_
open`) widened to `<E, R>` — a pure move, every callee already generic;
`execute_as` is now a thin wrapper over the new `execute_streams_op_as<E,
R>`; `SimClusterHandle`/`SimCluster::dynamo_streams` added; 8 new
scenarios in `sim_cluster_dynamo_streams.rs` (16 tests with their
`_over_seeds` siblings) covering enable → open-tail reads → seal → sealed
reads → an iterator surviving a mid-poll seal → pagination → all three
`ShardIteratorType`s → every node answering identically → the disable
grace window. PR 4 (`dynamo_streams.rs` siblings): the file's 12
sim-convertible tests given deterministic siblings (four proven in kind by
scenarios PR 3 already built, eight genuinely new), the file trimmed to
its 3 `ProdEnv` residuals. PR 5 (`stream_janitor.rs` siblings):
`segment_janitor_loop`/`segment_janitor_tick`/`update_segment_janitor_
progress` widened to `<E, R>` and spawned unconditionally on every
`SimCluster` node (the D4 PR 5 backup-janitor shape, covered by the
existing `Drop`/`restart` shutdown path); a retention constructor knob, a
`segment_janitor_progress(node)` accessor, and a `grow_stream(node,
table)` driver added; 9 of the file's 11 scenarios converted (18 tests
with `_over_seeds` siblings) in a new `sim_cluster_stream_janitor.rs`, the
file trimmed to its 2 `ProdEnv` residuals.

**The final `ProdEnv` residue, per test, with its reason** — the complete
account, gathering what each PR's own amendment above already stated
individually:

- `tests/dynamo_streams.rs` (2 of 15): `set_table_stream_enable_
  propagates_and_survives_restart` (a genuine real-WAL restart —
  real-disk durability `SimEnv` cannot prove) and `disable_survives_
  concurrent_periodic_seal_on_local_route` (races the real periodic
  `change_consumer_loop`, which this rung deliberately never spawns under
  `SimCluster` — its own non-goal from the opener).
- `tests/dynamo_streams.rs` (1 more, PR 3's own finding, not named by the
  opener): `bare_stream_hot_read_is_refused` — exercises `handle_request`'s
  `Surface::Intra` port guard and `cp_serve_forwarded`'s bare-refusal
  match arm, neither of which exists under this fixture's relay path (no
  `handle_request`, no ports, no `Surface` classification under `SimEnv`).
- `tests/stream_janitor.rs` (2 of 11): `repair_re_replicates_to_a_fresh_
  target_after_a_replica_node_dies` (the shared `S3`-backed
  `SimSegmentStore` has no per-node replica concept at all — `row.
  replicas` is always empty for it, a fault this fixture cannot express)
  and `segment_janitor_reclaims_objects_from_a_genuinely_control_only_
  leader` (needs a real control-only/data-only process split; `SimCluster`
  has no notion of node role — every node is the same shape, already
  counted in the separate "control/data role split" residual group this
  rung never claimed).
- `tests/stream_backfill_seed_filter.rs` (2, untouched throughout): its
  own blocker is `backfill_seed_tick`, filed under "index DDL beyond
  plain `CreateTable`" — a separate, still-unowned residual group this
  rung never claimed, noted only because the file lives in the Streams
  test-count neighborhood.
- `tests/console_stream.rs` (4, untouched throughout): the Streams tab's
  console HTTP surface, filed under the separate admin/console/dashboard
  HTTP residual from the start, per the opener's own Gap section.
- `tests/streams_e2e.rs` (12, untouched throughout): frozen behind #298/
  #745, class (E) — never this rung's to claim.

Net: 26 tests converted to deterministic `SimCluster` siblings across two
new modules (`sim_cluster_dynamo_streams.rs`, `sim_cluster_stream_
janitor.rs`), 21 tests stay `ProdEnv` for documented reasons spread across
four files, none of them silently dropped.

**Two in-scope findings, both recorded in `docs/engineering-lessons.md`,
neither a product bug beyond the first:**

- **PR 2 — `index_drain::seal_now`'s generic signature did not mean its
  body avoided the real clock.** `seal_now<E, R>` had carried an `<E:
  Env, R: RelayClient>` signature for years, but its own commit-wait poll
  called bare `tokio::time::Instant::now()`/`tokio::time::sleep`
  internally — invisible to a read-only investigation pass, since nothing
  under `SimEnv` had ever actually called it before this PR (`change_
  consumer_loop`'s real periodic seal arm is the only production caller,
  and `SimCluster` never spawns that loop). `SimCluster::drive_stream_
  seal`'s first smoke test hit the panic ("there is no reactor running")
  immediately. Fixed with the standard `ctx.env.now()`/`ctx.env.
  sleep(..)` conversion. `pitr_seal_now` — `seal_now`'s structural
  twin — carries the identical unfixed bug, confirmed by direct
  inspection and deliberately left unfixed as outside this PR's scope; a
  future rung driving PITR sealing under `SimCluster` will need the
  identical conversion before its own first `SimEnv`-driven caller can
  reach it.
- **PR 5 — a `SimCluster` op call's fixed `OP_BUDGET` advance can retire a
  short-retention row before a scenario's own "still exists right after
  this call" assertion runs.** `SimCluster::spawn_and_capture` always
  advances virtual time by the full 12s `OP_BUDGET` regardless of how
  quickly the call's own future resolves; with every production
  background loop (including the segment janitor) spawned unconditionally
  on every node, a retention window shorter than that budget cannot be
  trusted to still be "not yet due" by the time control returns to the
  test. Two scenarios hit this deterministically at every seed before the
  fix (raising the affected scenario's own retention comfortably past
  12s); a third (`expiry_survives_a_control_leader_kill_mid_sweep`) uses
  the general strategy this requires for catching a row genuinely
  mid-sweep — a retention larger than the setup phase's own cumulative
  `OP_BUDGET` cost, then a manual small-step `run_for` poll loop rather
  than another op call. Scenario-local fixes only; no production code
  changed.

**`cargo test -p animusd --lib` test count across the series**: 456
passed / 3 ignored after PR 3 (438 C-06 baseline + 18) → 472 / 3 after PR
4 (+16, `dynamo_streams.rs` op-variety scenarios, not new `#[test]`
functions beyond the 12 conversions plus the 4 already covered in kind) →
the `sim_cluster`-tier-only run at PR 5's own gate: 315 passed / 2
ignored, 919.49s, peak resident memory ~880 MB via the anchored sampler,
consistent with every prior rung's own no-leak trajectory (issue #753's
fix holds — no new per-node loop this rung added needed its own `Weak`-
handle treatment: `drive_stream_seal` captures no `Env`/task handle at
all, and the janitor's own spawn is already covered by `SimCluster`'s
existing `Drop`/`restart` shutdown path).

**What remains unowned after C-07**, updating the C-06-closing residual
inventory (`crates/animusd/CLAUDE.md`'s Tests section) now that Streams is
no longer on it: admin/console/dashboard HTTP, TTL, the control/data role
split, `--config` bring-up, index DDL beyond plain `CreateTable`, node
assembly/raw `ClientRequest`, and the throttle-metric counters. Six groups
remain, none with a rung against it today — the next C-04/C-06/C-07-shaped
rung that wants one should start here, admin/console/dashboard HTTP still
being the largest.

**Website: no change needed, verified again at this close.** Neither
`website/compatibility.html`'s Streams-related rows nor `website/
index.html`'s "Streams and time to live" line nor `website/articles/
determinism.html`'s general deterministic-simulation story name Streams
`SimCluster` testing specifically as a gap this rung needed to close —
all describe wire-level behavior against a real cluster or the
deterministic-simulation story in general terms, both already true before
this rung and unaffected by it, since every production dispatch path this
rung touched stays byte-identical per its own opener's non-goals.

**Gates**: none — documentation only, no `cargo` command run, `git status
--short` shows only the docs files this PR touches.

**Docs**: this amendment (closing Rung G); `docs/roadmap.md`'s C-07 entry
closed; `crates/animusd/CLAUDE.md`'s residual-inventory paragraph updated
and a "C-07 closed" pointer added after the PR 5 appendix; `docs/
engineering-lessons.md` already carries both of this rung's own findings
(the "Rung G, C-07 PR 2"/"Rung G, C-07 PR 5" entries) — no new entry
needed at this close.

## 2026-09-08 amendment — Rung H (post-C-07): admin/console/dashboard HTTP `SimCluster` dispatch (C-08), PR 1 (this docs-only opener)

Rung G closed C-07 (Streams), and its own closing residual inventory named
the next candidate explicitly: admin/console/dashboard HTTP, the group
every rung back through the D3-closing inventory has flagged as "the
largest remaining unowned group" without claiming it. This rung —
informally "H," continuing the same post-C-04 payoff sequence F/G opened —
claims it, tracked in `docs/roadmap.md` as **C-08**.

**Gap.** A read-only pass over this tree at this commit finds ten
real-socket `tests/*.rs` files touching the admin/console/dashboard HTTP
surface, counts taken directly with `grep -c '#\[tokio::test' <file>`:
`admin_endpoint.rs` (23), `dashboard_endpoint.rs` (16), `console_
endpoint.rs` (3), `console_create_table.rs` (4), `console_items.rs` (4),
`console_stream.rs` (4), `console_table_config.rs` (9), `console_tables.rs`
(1), `metrics_endpoint.rs` (1), `system_table.rs` (2) — 67 tests total,
matching the D3-closing residual inventory's own "admin/console/dashboard
HTTP (10/66)" figure to within one test (a pre-existing minor staleness in
that count, not something this PR's own grep-verified numbers should
propagate — see the Docs section below). `console_stream.rs`'s four tests
are already counted here, not under Streams — C-07's own opener stated
this explicitly and this rung's own count keeps it that way. A search for
any other file touching this surface turned up one more candidate,
`control_membership_admin.rs` (12 tests): a genuine bring-up over real
TCP/time proving control-plane membership-change convergence and a
runtime-added voter surviving a leadership transfer — filed under the
separate "control/data role split" residual (its own scenarios explicitly
build a `bring_up_split` deployment), not this group, and this rung does
not claim it.

**Ground truth** — from a read-only investigation of the current tree, not
a plan expectation:

- `animus-node`'s `admin.rs::dispatch`/`AdminHost` (30 methods) and
  `console.rs::route`/`ConsoleBackend` (14 methods) are already pure,
  generic route tables — untouched by this rung, exactly like `animus-
  node`'s `dispatch`/`route` for every prior rung in this sequence.
- `crates/animusd/src/admin.rs:415`'s `impl AdminHost for ClientCtx` is
  bare (`ClientCtx` defaults `E = ProdEnv`, `R = AnimusdRelayClient`) —
  concrete — delegating to roughly 30 file-local handler functions.
  **Finding: mostly pure signature widening.** `ClientCtx<E, R>`'s own
  fields (`edge: ClusterEdgeState<E>`, `control: ControlHandle<E, R>`,
  `control_storage: Option<SharedEngine<E>>`, `segment_store`/`backup_
  store`) are already generic, and `lib.rs:9198`'s `impl<E: Env, R:
  RelayClient> ClientCtx<E, R>` block (spanning to line 10739) already
  covers `metrics_text`/`metrics_json`/`stream_change_rates`/`metrics_
  history`/`admin_drain`/`admin_add_member`/`admin_remove_member`/`admin_
  add_control_member`/`admin_remove_control_member`/`admin_transfer_
  control_leadership` — confirmed directly by function location, not
  inferred — plus `CpGroup<E>` (`lib.rs:563`) and `SharedEngine<E>`
  (`lib.rs:10745`, itself `E: Env = ProdEnv`, not hardcoded) and its
  `lsm_sstables`/`wal_stats` methods. This has been true since ADR 0061
  rung C5 — `crates/animus-node/CLAUDE.md`'s "hardcoded to `ProdEnv`" note
  (its C4d-era bullet, lines ~478/~500) describes an earlier rung's state
  and is corrected by this PR (see the `animus-node/CLAUDE.md` change
  below).
- `lib.rs:3040`'s `impl console::ConsoleBackend for ClientCtx` is likewise
  bare — concrete. All 14 methods build wire JSON and call `dynamo::
  execute_routed(self, target, body)` — the same wrapper `admin.rs::
  action_data_dynamo` calls. `dynamo.rs:537-581`'s `execute_routed`/
  `execute_routed_as`/`execute_as` fork `X-Amz-Target`'s prefix (the real
  edge's own fork, confirmed at `dynamo.rs:549-559`: a `DynamoDBStreams_
  20120810.*` target goes to `dynamo_streams::execute_as`, everything else
  to `execute_as`) but all three stay concrete, taking `&ClientCtx`.
  Underneath, `dispatch_item_op`/`execute_item_op_as<E, R>` (`dynamo.rs:
  1057`/`:1805`, generic since rung D2/D3) and `dynamo_streams::execute_
  streams_op_as<E, R>` (`dynamo_streams.rs:101`, generic since C-07 PR 3)
  are **already generic** — no generic sibling of the fork itself exists,
  since these two concrete callers don't statically know item-vs-Streams
  the way `SimClusterHandle::dynamo`/`dynamo_streams` do.
- **Blockers**, file/line precise:
  - **(a)** `admin.rs::action_put_credential`/`action_rotate_credential`/
    `action_revoke_credential` (`admin.rs:3067`/`:3145`/`:3200`) each carry
    their own `tokio::time::Instant::now()` deadline plus `tokio::time::
    sleep(crate::dynamo::SCHEMA_POLL_INTERVAL)` commit-wait loop
    (`admin.rs:3092-3128`/`:3164-3186`/`:3206-3224`) — the identical shape
    `update_time_to_live`'s own conversion already fixed elsewhere in this
    crate. Convert each to `ctx.env.now()`/`ctx.env.sleep(..)`.
  - **(b)** `admin.rs::action_data_seed` (`admin.rs:2565`) carries one
    `tokio::time::sleep(SEED_RETRY_BACKOFF)` retry at `admin.rs:2692`, same
    fix. (The `use tokio::time::sleep` at `admin.rs:3340` is inside the
    file's own `#[cfg(test)] mod system_table_tests` — a real bring-up
    fixture, not a blocker this rung touches.)
  - **(c)** No generic `execute_routed_as` sibling exists yet — confirmed
    by grep, zero hits for `execute_routed_as_generic` anywhere in the
    crate. Add `dynamo::execute_routed_as_generic<E: Env, R: RelayClient>
    (ctx, principal, target, body) -> (u16, String)`, forking exactly like
    the concrete `execute_routed_as` to `execute_item_op_as`/`dynamo_
    streams::execute_streams_op_as`.
  - **(d)** `dynamo.rs:1725-1774`'s `dispatch_table_op`'s `UpdateTable` arm
    has no sub-arm for an index change — confirmed directly:
    `if index_update.is_some() { return Err(unsupported_by_generic_
    dispatch("UpdateTable with an index change")); }` — only the
    stream-only and throughput-only shapes have one. `add_gsi`/`drop_gsi`
    would hit `unsupported_by_generic_dispatch` today. **Out of scope**:
    filed under the already-named "index DDL beyond plain `CreateTable`"
    residual, per the same call Rung G made for `stream_backfill_seed_
    filter.rs`. No other capability gap found: `create_table`, item CRUD,
    `Scan`/`Query`, TTL, and the Streams read API are all already generic;
    `TableDetail.pitr`/`backups` read `Metadata` fine but the `BeginBackup`/
    PITR data behind them is a separate unowned residual, so those
    assertions stay `ProdEnv`.

**Decision.** Build `SimCluster`-reachable, deterministic coverage for the
admin dispatch table's pure observers and mutating actions and the console
backend's table/item/stream/config surface, following the identical
widen-then-add-a-generic-entry-point template D3/D4/F/G already validated
six times over (D3 PR 2a/2b/3a, D4 PR 2/5, F PR 2/5, G PR 2/3).

**New `SimCluster` primitives (PR 2):**

- `SimCluster::admin(node, method, path, query, body) -> (u16, String)` —
  calls `animus_node::admin::dispatch(&self.ctx(node), ..)` directly,
  mirroring `SimCluster::dynamo`'s own shape (no HTTP framing —
  `dispatch` never sees headers, only method/path/query/body).
- `SimCluster::console(node, method, path, query, body) -> (u16,
  &'static str, String)` — builds a minimal `http::HttpRequest` and calls
  `animus_node::console::route(&req, &tables_fn, &self.ctx(node), ..)`.
  `SimClusterHandle::admin`/`console` mirror `dynamo`/`dynamo_streams`.
  **No new spawned loop is added by either primitive** — both are
  synchronous calls into an existing route table, not a perpetual task —
  so issue #753's Drop-coverage requirement (reference-cycle risk from a
  new per-node loop capturing a `SimEnv`/relay handle) is simply not
  engaged here, unlike Rung G's `drive_stream_seal`/segment-janitor spawn.

**Non-goals.** `dispatch`, `route`, `execute_routed`, `execute_routed_as`,
and `execute_as` keep their exact production shape — only the type
parameter changes where a PR says so, per the D2 PR 1 lesson this file
already carries (`docs/engineering-lessons.md`'s "A narrowed generic split
of a dispatcher must not become the production dispatcher's ONLY path"
entry) and F's/G's own restatements of it. `execute_routed_as_generic`
never becomes production's only path to the console backend — production
keeps calling the concrete `execute_routed`. Every untrimmed real-socket
suite in this group runs against the widened code before anything is
converted or deleted, per D3 PR 3b's own precedent.

**The PR series** (8 PRs, stacked on the C-07 close-out), with a per-PR
gate line so each stays independently verifiable:

- **PR 1 (this amendment).** Docs only: this ADR amendment, the C-08
  roadmap entry, this file's own rung-table row, `crates/animusd/CLAUDE.md`'s
  residual-inventory annotation, and a staleness correction in
  `crates/animus-node/CLAUDE.md`. **Gates:** none — documentation only, no
  `cargo` command run.
- **PR 2 — Groundwork.** Widen `impl AdminHost for ClientCtx` and its ~30
  handler functions, and `impl ConsoleBackend for ClientCtx`, to `<E, R>`;
  fix blockers (a)/(b) (convert the four `tokio::time` sites to `ctx.env`);
  add blocker (c)'s `execute_routed_as_generic`; add `SimCluster::admin`/
  `console` plus the `SimClusterHandle` mirrors. **Gate:** `cargo test -p
  animusd --lib`; `cargo clippy -p animusd --all-targets --all-features --
  -D warnings`; `cargo fmt --all --check`; `--test admin_endpoint --test
  console_endpoint --test dashboard_endpoint` run unmodified and stay
  green.
- **PR 3 — Console reachable + first siblings.** A new `sim_cluster_
  console.rs`: tables list, table detail, item CRUD, `Scan`/`Query` (base
  table plus a named index via `SimCluster::drain_gsi`), `create_table`,
  stream enable/disable and read — not `add_gsi`/`drop_gsi` (blocker (d)).
  Convert `console_tables.rs` (1), `console_create_table.rs` (4), `console_
  items.rs` (4), and the JSON-routing portion of `console_endpoint.rs` (up
  to 3). **Gate:** `cargo test -p animusd --lib`; before/after `--test
  console_tables --test console_create_table --test console_items` counts.
- **PR 4 — `console_stream.rs` + `console_table_config.rs` siblings.**
  Convert `console_stream.rs` (4, the file G's own close-out filed here)
  and 6 of `console_table_config.rs`'s 9 (table-detail, stream-toggle,
  TTL set/clear, delete-table, and the PITR/backups tests if no real
  `BeginBackup` data turns out to be needed). **Stays `ProdEnv` (3 of 9):**
  add/drop-GSI, declared-attribute-type, and reject-unknown-type — blocker
  (d). **Gate:** same shape, before/after `--test console_stream --test
  console_table_config`.
- **PR 5 — Admin dispatch, pure observers.** A new `sim_cluster_admin.rs`:
  every mutation-free `GET` route (config/peers/status/raft/raftkv/txns/
  storage-*/system-table/backups/restores/metrics[/history]/member-drain-
  status/health/live/control-members/credentials/backup-store/ttl/gc/
  segment-store). Convert the matching read-only `admin_*` tests
  (config-auth, raftkv-default/scoped-key-count, credentials-view, the
  backup-store/ttl/gc/segment-store reports, `admin_live_is_200_...` —
  verify `SimCluster`'s control quorum can be starved un-electable too),
  `system_table.rs` (2), `metrics_endpoint.rs` (1). **Gate:** `cargo test
  -p animusd --lib`; before/after test counts.
- **PR 6 — Admin mutating actions + remaining siblings.** Wire the
  remaining `action_*` handlers (data-dynamo, data-seed, drop-table,
  throttle-defaults, credential CRUD, control-transfer — reusing
  `transfer_control_leadership_to` — drain, member add/remove, control-
  member add/remove, split, flush, compact, reconfigure, stream-grow) into
  `sim_cluster_admin.rs`. Convert the remaining 11 `admin_*` tests
  (data-write-dynamo, table-management-create-and-drop, backups-view,
  seed-writes, split-in-place-inherits-replicas, system-table-split-
  lineage, credentials-put-rotate-revoke, credentials-relay, control-
  transfer ×2, storage-compact-action). **Stays `ProdEnv`:** `seed_load_
  does_not_storm_cp_elections` (real-thread election timing, class A);
  `admin_interface_surfaces_state_and_actions` trimmed to its JSON-content
  half if separable, else kept whole. **Gate:** `cargo test -p animusd
  --lib`; before/after `--test admin_endpoint --test metrics_endpoint
  --test system_table`.
- **PR 7 — `dashboard_endpoint.rs` + `console_endpoint.rs` close-out.**
  Convert the card/panel/action tests (render markers plus a live round
  trip against `SimCluster::admin`) — up to 14 of 16. **Stays `ProdEnv`:**
  `dashboard_serves_spa_with_cors_and_peers` (real CORS/keep-alive
  headers) and the three control-only/data-only role-split tests. **Gate:**
  `cargo test -p animusd --lib`; before/after `--test dashboard_endpoint
  --test console_endpoint`.
- **PR 8 — Docs close-out.** This ADR's rung-table row updated to "closed,"
  a "Rung H closed" amendment, `docs/roadmap.md`'s C-08 status, and
  `crates/animusd/CLAUDE.md`'s residual inventory updated so this group's
  own count drops to what stays `ProdEnv` (roughly 3 GSI/`UpdateTable`
  tests filed under index DDL, roughly 4 real-HTTP/role-split, roughly 2
  backup/PITR, 1-2 real-thread-timing tests). **Gates:** none —
  documentation only.

**Expected `ProdEnv` residue, with reasons, per test class:**

- **Real CORS/keep-alive headers** — `dashboard_serves_spa_with_cors_and_
  peers` and part of `console_endpoint.rs`: `SimCluster::admin`/`console`
  never construct HTTP framing, so a test asserting on real response
  headers has nothing to run against.
- **Control-only/data-only role split** — the three `dashboard_endpoint.rs`
  tests proving a genuinely control-only or data-only process bring-up:
  `SimCluster` has no per-node role concept, only a single combined
  assembly per node.
- **GSI/LSI `UpdateTable`** — the 3-of-9 `console_table_config.rs` tests
  (add/drop-GSI, declared-attribute-type, reject-unknown-type), filed
  under the separate "index DDL beyond plain `CreateTable`" residual per
  blocker (d) — not claimed by this rung, same disposition Rung G gave
  `stream_backfill_seed_filter.rs`.
- **Backup/PITR data** — `TableDetail.pitr`/`backups` read `Metadata`
  fine, but the real `BeginBackup`/PITR data behind a populated response
  is a separate unowned residual; the console tests asserting on that data
  stay `ProdEnv` unless PR 4 finds it unnecessary.
- **Real-thread election timing** — `seed_load_does_not_storm_cp_
  elections`, class A: genuine OS-thread election timing `SimEnv` cannot
  prove.

**Off-limits, never touched or read by this series**: `streams_e2e.rs`,
`dynamo_index_scan.rs`, `index_backfill.rs`, `batch_write.rs`, `dynamo_
index_writes.rs`, `split_placing_two_replica_diff_e2e.rs`, `cp_cross_
process.rs`, `cp_txn.rs`, `dynamo_txn_idempotency.rs`, `shared_wal_
liveness.rs`.

**Risks/gotchas, named up front rather than discovered mid-series:**

- **The D2 PR 1 generic-sibling lesson applies again**: a narrowed
  generic split of `execute_routed_as` (blocker (c)) must never become
  production's only path to the console backend. Run every existing
  real-socket admin/console/dashboard suite against the widened code
  before trimming anything, per D3 PR 3b's/G's own precedent.
- **No new Drop-coverage obligation.** Unlike Rung G's `drive_stream_
  seal`/segment-janitor spawn, neither `SimCluster::admin` nor `::console`
  starts a task or captures a handle beyond the call's own stack frame —
  issue #753's requirement (reference-cycle risk from a perpetual per-node
  loop) does not apply to this rung's own new primitives. If any later PR
  in this series introduces a genuinely new spawned loop, that PR's own
  gate must revisit this call.
- **Memory claims only via the anchored sampler**, per the #753 postmortem:
  `pgrep -f '^/home/user/animus-db/target/debug/deps/animusd-'`, never an
  unanchored substring match.
- **Accessor staleness**: any new admin/console accessor this rung adds
  must re-derive from live `hosted_tablets`/`Metadata` state on every
  call, never memoize a snapshot from setup time — C-06 PR 4's own finding
  (A).

**Depends:** C-04 (closed), C-06 (closed), C-07 (closed) — the D3/D4
generic dispatch cores (`dispatch_item_op`/`dispatch_table_op`) and the D2
PR 1 `SimCluster` harness this rung builds directly on, plus rung C5's own
widening of `ClientCtx`'s field types (`ClusterEdgeState<E>`/
`ControlHandle<E, R>`/`SharedEngine<E>`), which is what makes this rung
"mostly signature widening" rather than new mechanism.

**Website: no change needed.** `website/`'s admin/console/dashboard claims
describe wire-level behavior against a real cluster and stay true
throughout this series, since every production dispatch path is
byte-identical per this amendment's own non-goals — the same reasoning
F's and G's own openers and close-outs already applied.

**Docs:** this amendment (opening Rung H); `docs/roadmap.md`'s new C-08
entry and wave-9 row, and the C-07 entry's own "what remains unowned"
sentence re-pointed at this rung; `crates/animusd/CLAUDE.md`'s residual
inventory annotated with a "C-08 (open)" note beside the Streams/C-07
paragraph; `crates/animus-node/CLAUDE.md`'s stale "hardcoded to `ProdEnv`"
note (lines ~478/~500, a C4d-era description this investigation found
superseded by rung C5) corrected in place with a dated note, since this
PR is what surfaced the staleness. **Gates:** none — documentation only,
no `cargo` command run, `git diff --stat` shows only the docs files this
PR touches.

## 2026-09-08 amendment — Rung H, PR 2 landed (groundwork), corrected same day in review

PR 2's own scope — widen `impl AdminHost for ClientCtx` (43 handler
functions, not merely "~30" as the opener estimated — the exact grep count
once every function taking `ctx: &ClientCtx` was enumerated) and `impl
ConsoleBackend for ClientCtx` to `<E: Env, R: RelayClient>`, fix blockers
(a)/(b), add blocker (c)'s `execute_routed_as_generic`, add `SimCluster::
admin`/`console` plus the `SimClusterHandle` mirrors — landed, with two
material findings beyond the opener's own scope, and a same-day review
correction to how the second finding's own fix reaches the generic
dispatch (below).

**The widening itself was exactly as mechanical as the opener predicted,
for every handler function.** Every one of the 43 `admin.rs` handler
functions (`config_view` through `action_revoke_credential`) needed only a
signature change (`fn f(ctx: &ClientCtx, ..)` → `fn f<E: Env, R:
RelayClient>(ctx: &ClientCtx<E, R>, ..)`) — no body touched `ProdEnv`/
`AnimusdRelayClient` directly, confirming rung C5's own widening of
`ClientCtx`'s field types covers this surface completely. Blockers (a)/(b)
— the three credential handlers' and `action_data_seed`'s `tokio::time::
Instant::now()`/`tokio::time::sleep` commit-wait sites — converted to
`ctx.env.now()`/`ctx.env.sleep(..)`, the identical pattern `disable_
stream`/`update_table_throughput` already established in prior rungs.
`execute_routed_as_generic<E: Env, R: RelayClient>` (`dynamo.rs`) forks on
the `X-Amz-Target` prefix exactly like the concrete `execute_routed_as`,
routing to `execute_item_op_as`/`dynamo_streams::execute_streams_op_as`.
`SimCluster::admin`/`console` (plus the `SimClusterHandle` async mirrors)
call `animus_node::admin::dispatch`/`animus_node::console::route` directly
— no HTTP framing to build, mirroring `SimClusterHandle::dynamo`'s own
shape; `console` builds a minimal `http::HttpRequest` and passes `""` for
the HTML/CSS/JS shell content, since no scenario this PR adds exercises a
static-asset path. Two pinned-seed smoke tests
(`admin_status_is_reachable_from_sim_cluster`, `console_tables_lists_a_
created_table`) prove both primitives reach live replicated state end to
end; the bigger scenario files are PRs 3-7's own scope.

**Finding A (real, closed in-scope): `execute_routed_as_generic`'s own
coverage gap is a genuine production regression once `admin.rs::action_
data_dynamo` and `impl ConsoleBackend for ClientCtx<E, R>` are forced onto
it — not merely a SimCluster-only limitation.** Running this rung's own
gate 2 (the untrimmed `admin_endpoint.rs`/`console_create_table.rs`/
`console_table_config.rs`/`dashboard_endpoint.rs` suites, per the D2 PR 1
lesson's own prescribed check) against the widened-but-swapped code turned
up **nine** real failures — `UpdateTimeToLive`/`CreateBackup`/
`DeleteBackup` and `UpdateTable`-with-an-index-change are all reachable
through `/admin/data/dynamo` and the console's own mutating endpoints, and
none of the three is in `dispatch_item_op`'s current coverage. See
`docs/engineering-lessons.md`'s matching 2026-09-08 entry for the full
diagnosis and the general lesson (a general-purpose admin/console proxy's
coverage gap is visible to every real caller the instant the swap lands,
unlike the primary wire edge's own narrower, unchanged surface). Six of
the nine closed by genuinely widening the underlying operations —
`update_time_to_live`/`create_backup`/`delete_backup` (`dynamo.rs`) had no
`tokio::spawn` blocking them, only the same `tokio::time` → `ctx.env`
conversion this whole series already does mechanically, plus three new
match arms in `dispatch_item_op` (`Operation::UpdateTimeToLive`/
`CreateBackup`/`DeleteBackup`) — `run_operation`'s own arms for all three
are untouched, and every existing real-socket regression for them stayed
green. This half of the PR was never revisited by the review correction
below — it was correct as landed.

**Finding B, and the review correction to its fix: the remaining two
failures (`console_table_config.rs`'s `add_gsi_records_a_declared_
attribute_type`/`add_and_drop_gsi_round_trip`) are squarely blocker (d)**
— `UpdateTable` with an index change has no `dispatch_table_op` sub-arm,
and closing that gap for real needs the GSI backfill machinery
generalized too, genuinely out of this rung's scope (as the opener already
named it). **The PR's own first cut fixed this by widening `impl AdminHost
for ClientCtx`/`impl ConsoleBackend for ClientCtx` in place — a mistake,
caught and corrected in review the same day.** Widening a *blanket impl on
`ClientCtx` itself* is not the same move as widening a free function the
way every earlier generic-dispatch rung (D2/D3/D4/F/G) did: `ClientCtx`
(the bare, default-type-parameter alias) is production's own concrete
type, so replacing its one `impl AdminHost`/`impl ConsoleBackend` block
in place doesn't add a parallel path — it silently narrows *every*
production caller of that trait to whatever the generic dispatch core
covers, the instant `action_data_dynamo`/`add_gsi`/`drop_gsi`'s shared
body is forced to call `execute_routed_as_generic` to compile generically.
A first fix attempt for the resulting gap doubled down on the same
mistake one layer up: a concrete interception in `console.rs::serve`/
`handle_conn`, special-casing the two affected routes ahead of `route`'s
own dispatch. That compiled and passed every test — and was still wrong:
it touched a file this rung's own non-goals list as off-limits (only
`route` itself was named, but `serve`/`handle_conn` sit directly upstream
of it, in the same production console path), duplicated `animus_node::
console`'s own route-parsing/JSON-helper logic in a way that would
silently drift the moment that crate's route shapes changed, and threaded
a concrete `ClientCtx` into signatures (`serve`/`handle_conn`) that had
never needed one before this rung.

**The corrected fix**: the concrete `impl AdminHost for ClientCtx`/`impl
console::ConsoleBackend for ClientCtx` stay exactly as they were before
this rung — production, observably byte-identical, every dispatch call
site the concrete `execute_routed`/`execute_routed_as`, never
`execute_routed_as_generic` — and a **second type**,
`GenericAdminHost<E, R>(pub ClientCtx<E, R>)`/`GenericConsoleBackend<E,
R>(pub ClientCtx<E, R>)` (`admin.rs`/`lib.rs`), a one-field newtype
carrying its own separate `impl<E: Env, R: RelayClient> Trait for
Generic*<E, R>` that reaches the generic dispatch core instead. Coherence
allows this — the two impls target genuinely different types, even though
one always wraps the other — where it forbids a second `impl Trait for
ClientCtx<..>` outright. `SimCluster::admin`/`console` (`sim_cluster.rs`)
wrap `self.ctx(node)` in the newtype before calling `animus_node::admin::
dispatch`/`console::route`; production's own `spawn_common_tail` is
completely unaware the newtype exists, still building a bare `Arc<
ClientCtx>` as `Arc<dyn ConsoleBackend>` and handing a bare `&ClientCtx`
to `animus_node::admin::dispatch<H: AdminHost>` exactly as before this
rung. `console.rs` itself reverted to byte-identical with this rung's own
starting point (`git diff` against it is empty) — no interception, no new
route parsing, no `ClientCtx` in either `serve`'s or `handle_conn`'s
signature. Both `Generic*` impls share every byte of request-building/
response-parsing logic with their concrete siblings (factored into small,
`<E, R>`-generic or plain free functions both call, `self` vs. `&self.0`)
— only the one dispatch-call line differs — so the two paths cannot
quietly drift apart; `add_gsi`/`drop_gsi` on `GenericConsoleBackend` still
return blocker (d)'s own `unsupported_by_generic_dispatch` error (via the
unmodified `execute_routed_as_generic` → `dispatch_table_op` fallthrough)
rather than any bespoke handling — the honest, documented residual,
reached identically to how the generic dispatch already reports every
other not-yet-covered operation. `dispatch_table_op`/`execute_item_op_as`/
`execute_routed_as_generic` (`dynamo.rs`) and the generic `action_data_
dynamo` (`admin.rs`) — reachable only through the two `Generic*` types,
themselves constructed only by `SimCluster::admin`/`console` — regained
the `#[cfg_attr(not(test), allow(dead_code))]` treatment they carried
before this rung ever gave them a real (if mistaken) production caller,
mirroring `execute_statement_as`'s own precedent for the identical shape.
See `docs/engineering-lessons.md`'s matching (rewritten) 2026-09-08 entry
for the full incident and the general lesson (a blanket generic `impl
Trait for ClientCtx<E, R>` is not the free-function-widening shape every
earlier rung used safely; reach for a newtype the moment the type being
widened is production's own default-instantiated concrete type).

**`admin.rs::action_data_dynamo` needed the identical newtype split, not a
narrower fix** — Finding A's own widening (`update_time_to_live`/
`create_backup`/`delete_backup`) closed every *coverage* gap `/admin/
data/dynamo`'s own tests exercise, but the concrete `impl AdminHost for
ClientCtx`'s `action_data_dynamo` method still needed to keep calling the
full, unmodified `execute_routed` (via a new concrete-only sibling
function, `action_data_dynamo_concrete`) rather than `execute_routed_as_
generic` — the identical reasoning as `ConsoleBackend`'s own `add_gsi`/
`drop_gsi`, since `/admin/data/dynamo` can be asked to run **any**
DynamoDB operation, not just the six Finding A widened.

**Non-goals held throughout, verified via `git diff`, not merely
assumed**: `animus_node::admin::dispatch`, `animus_node::console::route`,
`execute_routed`, `execute_routed_as`, and `execute_as` are byte-identical
— zero changed lines inside any of the three `dynamo.rs` function bodies
(only `execute_routed`'s own doc comment changed, describing its actual
remaining callers), and `crates/animus-node/` has no changes in this PR's
diff at all — confirmed both before and after the review correction.
`crates/animusd/src/console.rs` is byte-identical to this rung's own
starting point (`git diff` against it, post-correction, is empty).
`dynamo.rs::dispatch` (the real DynamoDB wire listener) is untouched and
still calls `execute_routed_as` directly — `execute_routed_as_generic`
never becomes its path.

**The flake claim in the first cut's own gate report was unverified and
is corrected here.** A transient single-test failure
(`admin_backups_view_reflects_the_catalog`, "did not become serveable in
time: relay to peer node failed") was observed once, on an intermediate
run immediately following a `cargo build`, and the first cut's own report
attributed it to "this crate's own documented sandbox-contention flake
class for this exact file — see `crates/animusd/CLAUDE.md`'s issue #585
entries" — a citation nobody had actually checked. `grep -n 585 crates/
animusd/tests/admin_endpoint.rs` finds nothing; that attribution should
not have been made without verifying it. The failing run's own output was
not preserved (no saved log, nothing in shell history from that session)
and cannot be reproduced from this vantage point — recorded here as an
unattributed, unreproduced single transient failure, not a known flake.
Post-correction, the same test was re-run 3 times in direct succession
(twice, once immediately after the corrected code's own untrimmed-suite
run and once as a dedicated 3x check) and passed 6/6 — see the Gates line
below for the exact commands. If this test fails again in a future run,
its full assertion output and seed should be captured and filed as a new
issue rather than attributed to a prior, unverified citation.

**Gates**, in the required order, all foreground, run against the
corrected (newtype) code: `cargo build -p animusd --all-targets` (clean,
no warnings — including the six `dead_code` warnings the first cut's own
build had not checked for, closed by restoring the `#[cfg_attr(not(test),
allow(dead_code))]` attributes named above); `cargo test -p animusd --test
admin_endpoint --test console_endpoint --test dashboard_endpoint --test
console_tables --test console_create_table --test console_items --test
console_stream --test console_table_config --test metrics_endpoint --test
system_table` (67 passed, 0 failed, run twice post-correction, both
clean); `admin_backups_view_reflects_the_catalog` alone, 3 consecutive
runs, 3/3 passed (`cargo test -p animusd --test admin_endpoint
admin_backups_view_reflects_the_catalog -- --exact`, run twice more as
part of the two full-suite runs above — 6/6 total); `cargo test -p animusd
--lib sim_cluster -- --test-threads=2` (317 passed, 0 failed, 2 ignored,
run twice post-correction — 900.80s and 904.10s; anchored-sampler RSS
first ~62-111 MB, peak ~826-862 MB, last ~128-132 MB across the two runs
— consistent with every prior rung's own no-leak trajectory); `cargo fmt
--all --check` (clean); `cargo clippy -p animusd --all-targets
--all-features -- -D warnings` (clean). `Cargo.lock` unchanged throughout.
`git diff --stat` against the pre-PR-2 baseline: 5 files changed
(`admin.rs`, `console.rs` — net zero, reverted to the identical content —
`dynamo.rs`, `lib.rs`, `sim_cluster.rs`).

## 2026-09-08 amendment — Rung H, PR 3 landed (console reachable + first siblings)

Closes PR 3's own scope: every sim-convertible test in `tests/console_
tables.rs` (1), `tests/console_create_table.rs` (4), and `tests/console_
items.rs` (4) — 9 real-socket tests total — now has a deterministic
sibling in a new `crates/animusd/src/sim_cluster_console.rs`, and all
three source files are deleted whole (each ended with zero tests once its
sole/all tests converted, per this crate's own "a file left empty is
deleted" discipline — neither carried a `[[test]]` Cargo.toml entry to
remove, `tests/*.rs` binaries being implicit). `tests/console_endpoint.rs`
keeps its own three tests unconverted, each now carrying a one-line
`ProdEnv` reason, plus a tenth new scenario in the same sim module proving
its JSON-routing/error-mapping tail. **No `console.rs`/`dynamo.rs`/`sim_
cluster.rs` change was needed** — PR 2 already built every primitive this
PR reuses (`SimCluster::console` through `GenericConsoleBackend`,
`SimCluster::dynamo` for wire fixture setup, `SimCluster::drain_gsi` for
the one GSI-reading scenario) — this PR is pure test authorship, mirroring
C-07 PR 4's own "no dispatch change needed" precedent for `dynamo_
streams.rs`.

**Test-by-test disposition (9 converted, 3 kept `ProdEnv`, all `console_
endpoint.rs`)**:

| Real-socket test (file) | Disposition |
|---|---|
| `tables_endpoint_projects_the_schema_catalog_correctly` (`console_tables.rs`) | Converted → `run_tables_endpoint_projects_the_schema_catalog_correctly` |
| `create_minimal_table_appears_in_tables_list` (`console_create_table.rs`) | Converted → `run_create_minimal_table_appears_in_tables_list` |
| `create_full_table_declares_everything_exactly` (`console_create_table.rs`) | Converted → `run_create_full_table_declares_everything_exactly` |
| `create_table_rejects_a_duplicate_name` (`console_create_table.rs`) | Converted → `run_create_table_rejects_a_duplicate_name` |
| `create_table_rejects_an_lsi_with_no_sort_key` (`console_create_table.rs`) | Converted → `run_create_table_rejects_an_lsi_with_no_sort_key` |
| `scan_paginates_and_visits_every_item_exactly_once` (`console_items.rs`) | Converted → `run_scan_paginates_and_visits_every_item_exactly_once` |
| `query_by_partition_key_and_sort_condition` (`console_items.rs`) | Converted → `run_query_by_partition_key_and_sort_condition` |
| `put_get_delete_item_round_trip` (`console_items.rs`) | Converted → `run_put_get_delete_item_round_trip` |
| `scan_and_query_a_gsi_by_name` (`console_items.rs`) | Converted → `run_scan_and_query_a_gsi_by_name` (via `SimCluster::drain_gsi`, standing in for the real test's own converged-or-timeout poll waiting out a periodic drain this fixture never spawns) |
| `console_serves_shell_assets_and_deep_links_on_combined_node` (`console_endpoint.rs`) | **KEPT** whole — mostly real HTTP framing (status line, `Content-Type` headers, static-asset bytes, deep-link routing) `SimCluster::console` cannot reproduce (no framing at all); its own JSON-routing tail (empty tables list, unknown-path 404) is covered by new scenario `console_error_mapping_and_json_routing_assertions`, not by trimming this test |
| `console_serves_shell_on_data_only_node` (`console_endpoint.rs`) | **KEPT** — a genuine control-only/data-only process split; `SimCluster` has no node-role concept |
| `console_addr_panics_on_control_only_node` (`console_endpoint.rs`) | **KEPT** — the identical role-split reason |

A tenth scenario, `console_error_mapping_and_json_routing_assertions`, has
no real-socket original of its own — it is new coverage this PR's brief
asked for by name: a freshly-booted node's tables list is a valid, empty
JSON array and an unrecognized console path 404s (the two assertions from
`console_serves_shell_assets_and_deep_links_on_combined_node`'s own tail
that are genuinely about JSON routing, not HTTP framing), extended with
the console's error-mapping contract — a missing table's detail 404s with
a real error body, and a malformed JSON body on a mutating endpoint is a
400 — neither a 500 either way.

**Every scenario issues a control-plane mutation (wire `CreateTable`, the
console's own `POST /console/api/tables`) from a control follower node
wherever one is picked at all** (`control_leader_and_follower`, mirroring
`sim_cluster_dynamo_table_ops.rs::create_table_issued_on_a_control_
follower_relays_and_converges`'s own idiom), **and a tablet-scoped read/
write from a tablet non-leader** (`non_leader_of_table`, the `sim_cluster_
dynamo_streams.rs` idiom) — except the tables-LIST endpoint itself, which
is a pure local read off `effective_metadata()` with no leader/forwarding
concept at all, so those calls use a fixed node. Every read that verifies
a write asks for `ConsistentRead: true` (ADR 0055).

**No product bug found.** Every scenario passed at its pinned seed and
every `_over_seeds` seed (5 per scenario) on the first clean run, with the
wire/console JSON bodies copied directly from each real-socket original.

**Gates, in the required order**: `cargo test -p animusd --test console_
tables --test console_create_table --test console_items --test console_
endpoint` on the untrimmed files (12 passed, 41.4s, including the initial
build); `cargo test -p animusd --lib sim_cluster_console --
--test-threads=2` (20 passed, 0 failed, 36.78s); trim (delete the three
files, add reason comments to `console_endpoint.rs`), then `cargo test -p
animusd --test console_endpoint` (3 passed, 1.8s — no `--test console_
tables`/`console_create_table`/`console_items` invocation any more, since
those binaries no longer exist); `cargo test -p animusd --lib sim_cluster
-- --test-threads=2` (337 passed, 0 failed, 2 ignored, 938.11s — 317
baseline + this PR's 20 new tests; anchored-sampler RSS first ~62 MB, peak
~848 MB, last ~136 MB, consistent with every prior rung's own no-leak
trajectory); `cargo fmt --all --check` (one pass needed — the new file's
own long `assert_eq!`/`format!` call sites needed rustfmt's own
reflow, applied via `cargo fmt --all`, then clean); `cargo clippy -p
animusd --all-targets --all-features -- -D warnings` (clean). `Cargo.lock`
unchanged. `git diff --stat` against the PR 2 baseline: `lib.rs` (+15, the
new module's doc-comment registration), `tests/console_create_table.rs`/
`console_items.rs`/`console_tables.rs` deleted whole (-414/-510/-289),
`tests/console_endpoint.rs` (+33/-3, the module doc + three per-test
reason comments), plus the new `sim_cluster_console.rs` itself (1,123
lines).

## 2026-09-08 amendment — Rung H, PR 4 landed (`console_stream.rs` + `console_table_config.rs` siblings)

Closes PR 4's own scope: `tests/console_stream.rs` (3 of 4 tests) and
`tests/console_table_config.rs` (5 of 9 tests) — 8 real-socket tests total
— now each have a deterministic sibling in two new modules,
`crates/animusd/src/sim_cluster_console_stream.rs` and `crates/animusd/
src/sim_cluster_console_table_config.rs` (named `sim_cluster_console_*` so
PR 3's own `cargo test -p animusd --lib sim_cluster_console` gate
substring continues to reach them, with no gate-command change needed).
**No `console.rs`/`dynamo.rs`/`sim_cluster.rs` change was needed** — every
primitive both modules call (`SimCluster::console` through
`GenericConsoleBackend`, `SimCluster::dynamo` for wire fixture setup,
`SimCluster::drive_stream_seal` for the one bounded-pagination scenario)
was already generic before this PR — pure test authorship, mirroring PR
3's own "no dispatch change needed" precedent. The shared helpers PR 3
built in `sim_cluster_console.rs` (`env_seed`/`json`/`assert_no_cluster_
shape`/`create_table_via_wire`/`put_item_via_wire`/`get_item_via_wire`/
`tablet_of_table`/`leader_of_table`/`non_leader_of_table`/`control_
leader_and_follower`) were widened from module-private to `pub(crate)`
and reused by both new modules rather than duplicated, per this rung's
own "put shared helpers where PR 3 put them" discipline.

**Test-by-test disposition (8 converted, 5 kept `ProdEnv`)**:

| Real-socket test (file) | Disposition |
|---|---|
| `table_with_no_stream_reports_the_honest_disabled_answer` (`console_stream.rs`) | Converted → `run_table_with_no_stream_reports_the_honest_disabled_answer` |
| `stream_enabled_lists_shards_and_records_reflect_real_writes` (`console_stream.rs`) | Converted → `run_stream_enabled_lists_shards_and_records_reflect_real_writes` (a single `GetRecords` call, not a converged-or-timeout poll — every write is already committed before this open-tail read runs, mirroring `sim_cluster_dynamo_streams.rs::get_records_over_the_open_tail_before_any_seal`'s own shape) |
| `walking_a_shard_with_next_shard_iterator_visits_every_record_exactly_once` (`console_stream.rs`) | Converted → `run_walking_a_shard_with_next_shard_iterator_visits_every_record_exactly_once` — **deviates from a literal conversion**: seals the tablet first via `SimCluster::drive_stream_seal` rather than walking the still-open tail, since an open tail returns every already-committed record in one page under this fixture (no periodic seal ever narrows it), so a genuine multi-page `Limit`-bounded walk needs a sealed shard's own bounded record range — the identical reasoning `sim_cluster_dynamo_streams.rs::next_shard_iterator_pagination_with_small_limit_visits_each_record_once` already established |
| `ttl_deletion_carries_the_service_user_identity_through_the_console` (`console_stream.rs`) | **KEPT** `ProdEnv` — no primitive drives `animusd::ttl_reaper::ttl_reaper_loop` under `SimEnv`: `SimCluster::new`/`restart` never spawn it (unlike `heartbeat_loop`/the reconciler/the backup janitor/the segment janitor, all always-on since D4/rung G), so nothing in this fixture would ever reap the expired item this test depends on. The TTL reaper is its own unowned residual group; this rung's own brief was explicit that building a reaper driver is out of scope for this PR |
| `table_detail_projects_full_configuration` (`console_table_config.rs`) | Converted → `run_table_detail_projects_full_configuration` |
| `add_and_drop_gsi_round_trip` (`console_table_config.rs`) | **KEPT** `ProdEnv` — blocker (d): `UpdateTable` with an index change has no `dispatch_table_op` sub-arm (`add_gsi`/`drop_gsi` both route through it and would hit `unsupported_by_generic_dispatch`); the index-DDL residual this whole rung's opener named out of scope |
| `add_gsi_records_a_declared_attribute_type` (`console_table_config.rs`) | **KEPT** `ProdEnv` — identical blocker (d) |
| `add_gsi_rejects_an_unknown_attribute_type` (`console_table_config.rs`) | **KEPT** `ProdEnv` — identical blocker (d) |
| `stream_toggle_round_trips` (`console_table_config.rs`) | Converted → `run_stream_toggle_round_trips` |
| `ttl_set_and_clear_round_trips` (`console_table_config.rs`) | Converted → `run_ttl_set_and_clear_round_trips` |
| `delete_table_works` (`console_table_config.rs`) | Converted → `run_delete_table_works` |
| `table_detail_with_no_pitr_or_backups_is_null_and_empty` (`console_table_config.rs`) | Converted → `run_table_detail_with_no_pitr_or_backups_is_null_and_empty` |
| `table_detail_shows_pitr_status_and_backups` (`console_table_config.rs`) | **KEPT** `ProdEnv` — **deviates from the opener's own "6 convert" estimate**: the opener's brief allowed converting this one only if the PITR data it reads is producible under `SimCluster` via a generic `UpdateContinuousBackups` path; checked against the code and there isn't one — `Operation::UpdateContinuousBackups` is absent from both `dispatch_item_op`'s and `dispatch_table_op`'s `match` arms (`dynamo.rs`), so it falls to `unsupported_by_generic_dispatch`, unlike `CreateBackup`/`DeleteBackup`/`UpdateTimeToLive`, which PR 2 did widen. Backup/PITR data stays a separate residual, per the opener's own fallback reason |

**Issuing discipline**, mirroring PR 3's own: every table is created over
the real DynamoDB wire from node 0 (the wire path already does its own
internal routing regardless of which node issues it — PR 3's own
convention for its item-CRUD/Streams-adjacent scenarios); every write and
every Stream-tab read from a **non-leader** of the table's own tablet
(`console_stream.rs` siblings); every Config-tab mutation (`set_stream`/
`set_ttl`/`delete_table`, all `propose_schema`-shaped control-plane
mutations) and the `table_detail` read that follows it from the same
**control follower** (`console_table_config.rs` siblings) — `table_
detail` is itself a pure local `effective_metadata()` read with no leader
concept, so reusing the mutation's own follower keeps each scenario to
one node rather than introducing a second, arbitrary one with nothing to
prove by being different.

**No product bug found.** Every scenario passed at its pinned seed and
every `_over_seeds` seed (5 per scenario) on the first clean run.

**Gates, in the required order**: `cargo test -p animusd --test console_
stream --test console_table_config` on the untrimmed files (13 passed,
34.2s, including the initial build); `cargo test -p animusd --lib sim_
cluster_console -- --test-threads=2` (36 passed, 0 failed, 63.5s — 20 PR
3 baseline + this PR's 16 new tests, reached by the unchanged gate command
since both new modules are named `sim_cluster_console_*`); trim (remove
the 8 converted tests from both files, add reason comments to every kept
test), then `cargo test -p animusd --test console_stream --test console_
table_config` (5 passed, 8.7s — 1 kept in `console_stream.rs`, 4 kept in
`console_table_config.rs`); `cargo test -p animusd --lib sim_cluster --
--test-threads=2` (353 passed, 0 failed, 2 ignored, 962.08s — 337 PR 3
baseline + this PR's 16 new tests; anchored-sampler RSS across the
sampled window: first ~776 MB, peak ~776 MB, last ~132 MB — sampling
began several minutes into the run, so the true from-launch first sample
was not captured, but the observed trajectory is consistent with every
prior rung's own no-leak pattern and stayed well under the 6 GB/30 min
budget throughout); `cargo fmt --all --check` (one pass needed — the two
new files' own long `assert_eq!` call sites needed rustfmt's own reflow,
applied via `cargo fmt --all`, then clean); `cargo clippy -p animusd
--all-targets --all-features -- -D warnings` (clean). `Cargo.lock`
unchanged. `git diff --stat` against the PR 3 baseline: `lib.rs` (+24, the
two new modules' doc-comment registrations), `sim_cluster_console.rs`
(+39/-16, ten helpers widened to `pub(crate)` plus a doc note), `tests/
console_stream.rs` (-253 net, three tests removed plus a module-doc note
and one reason comment), `tests/console_table_config.rs` (-297 net, five
tests removed plus a module-doc note and four reason comments), plus the
two new sibling modules themselves (398 lines each).

## 2026-09-08 amendment — Rung H, PR 5 landed (admin dispatch pure observers)

Closes PR 5's own scope: the admin HTTP-JSON interface's **observer**
routes (every mutation-free `GET`) plus a JSON-route analog of the
metrics suite. Converts 5 of `tests/admin_endpoint.rs`'s 23 tests
(`admin_credentials_view_never_serves_a_secret`, `admin_raftkv_key_
count_is_scoped_per_tablet_after_split`, `admin_backups_view_reflects_
the_catalog`, `admin_backup_store_reports_reclaim_progress_and_leader_
state`, `admin_gc_reports_segment_janitor_progress_and_leader_state`)
plus the "tables" half of `admin_ttl_reports_reaper_progress_and_ttl_
tables` (a new scenario; that test itself stays whole) into a new
`crates/animusd/src/sim_cluster_admin.rs` (7 scenarios × pinned-seed +
5-seed `_over_seeds` = 14 tests). **`tests/system_table.rs`'s two tests
and `tests/metrics_endpoint.rs`'s one test stay `ProdEnv` whole** — see
below. **No `admin.rs`/`console.rs`/`dynamo.rs` dispatch change was
needed** — PR 2 already built `SimCluster::admin` (through
`GenericAdminHost`) and every generic handler this module's scenarios
reach.

**Three genuinely new `sim_cluster.rs` fixture changes were needed**,
surfaced by this rung's own required gate, not part of the original
design pass:

1. `SimCluster::put_raw`/`SimClusterHandle::put_raw` (new, test-only) —
   a literal-key write, unlike `SimCluster::put`'s `item_key`-encoded
   (DynamoDB composite-key, hash-token-prefixed) keys, which made
   choosing a literal, human-readable `POST /admin/tablet/split` split
   boundary impractical from a test the way the real-socket original's
   raw `ClientRequest::Put` keys made trivial.

2. **A real, previously-latent `SimCluster` fixture bug, found and
   fixed.** `SimCluster::new`/`SimCluster::grow` built every control
   `RaftNode<SimEnv>` via the plain `RaftNode::start(env, ids, engine)`
   constructor, which defaults its own metrics sink to `env.metrics()`.
   `SimEnv` does not override `Env::metrics`'s trait-default method,
   which unconditionally returns `MetricsHandle::noop()` — **one
   process-wide `static` shared sink** (already documented,
   `docs/engineering-lessons.md`'s "Observe a multi-group-per-node
   metric under `SimEnv`..." entry, ADR 0044 phase 2 C-02 PR 1). Every
   node's control `RaftNode`, on every `SimCluster` instance in the same
   test binary process, therefore shared the identical mutable
   `is_leader` gauge: the first control raft anywhere to become leader
   (during `SimCluster::new`'s own bootstrap) stamped `is_leader: 1`
   onto `GET /admin/metrics` for every node, permanently, regardless of
   that node's real role. A summed *counter* under the same sharing is
   merely inflated (harmless unless a test asserts an exact value, and
   none did) — only the gauge was corrupted outright, which is why this
   went unnoticed through five rungs' worth of `SimCluster` corpora:
   none of them read `/admin/metrics`'s `is_leader` field specifically
   (`ctx.control.is_leader()` — a direct `RaftCore` role check, used
   everywhere else this fixture reports leadership — is untouched by any
   of this). Caught only because this PR's own metrics scenario is the
   first `SimCluster` test ever to read it: every node in a fresh,
   otherwise-healthy 3-node cluster reported `is_leader: 1`, at every
   seed, reproducibly. **Fixed**: both node-construction sites now build
   one private `MetricsHandle::recording()` per node and call
   `RaftNode::start_with_metrics` (the API `start_with_metrics` was
   already built for, just never reached from this construction site);
   `DataRole::raftkv_metrics` reuses the SAME per-node handle, matching
   production's own "a combined node's control Raft and CP group record
   into the same sink" contract. See `docs/engineering-lessons.md`'s
   matching new entry for the general lesson.

3. `admin.rs::system_table` reads `ctx.control_storage` (the per-node
   system-keyspace mirror engine ADR 0038's `DRIVER_APPLIED` apply task
   durably writes) — `SimCluster`'s own node construction always sets it
   `None`, so `GET /admin/system-table` unconditionally answers
   `{"available": false}` under this fixture regardless of what's
   seeded. **Not fixed** — building that apply-task mirror is a new
   background-loop driver, explicitly out of this PR's scope; both
   `tests/system_table.rs` tests stay `ProdEnv` whole, with a reason
   comment.

**Two scenario-authoring bugs found and fixed along the way, neither a
product bug**: a first draft of the backup-store/gc scenarios issued a
WIRE `CreateBackup` and expected it to reach `AVAILABLE` — it never can
under this fixture, since `dynamo.rs::create_backup`'s own commit-wait
depends on the real per-tablet `backup_capture` driver actually
completing each tablet's capture, and no `SimCluster` primitive spawns
it (unlike the always-on heartbeat/reconciler/backup-janitor/segment-
janitor loops). Redesigned around direct `MetaCommand` proposals
(`BeginBackup`/`RecordBackupTabletComplete`/`CompleteBackup`/
`MarkBackupDeleted`) plus `SimCluster::seed_backup_object`, mirroring
`sim_cluster_backup_janitor.rs`'s own `complete_a_backup`/`seed_backup_
object` idiom exactly, since that module hit and solved the identical
gap first. Separately, the GC scenario's first draft called
`SimCluster::drive_stream_seal` on the CONTROL-plane leader — that
method only acts on tablets its own argument node leads at the
DATA-plane level, a generally different node, so the call silently
no-op'd and the test's own "a sealed row exists before the drop"
premise assertion failed; fixed to resolve the table's own data-plane
leader (`leader_of_table`) instead.

**Test-by-test disposition**:

| Real-socket test (file) | Disposition |
|---|---|
| `admin_credentials_view_never_serves_a_secret` (`admin_endpoint.rs`) | Converted → `sim_cluster_admin.rs::credentials_view_never_serves_a_secret` |
| `admin_raftkv_key_count_is_scoped_per_tablet_after_split` (`admin_endpoint.rs`) | Converted → `sim_cluster_admin.rs::raftkv_key_count_is_scoped_per_tablet_after_split` (via the fixture's own split + `SimCluster::put_raw`) |
| `admin_backups_view_reflects_the_catalog` (`admin_endpoint.rs`) | Converted → `sim_cluster_admin.rs::backups_view_reflects_the_catalog` (direct `MetaCommand`s via `SimCluster::propose_meta`, mirroring the original's own harness-level idiom) |
| `admin_backup_store_reports_reclaim_progress_and_leader_state` (`admin_endpoint.rs`) | Converted → `sim_cluster_admin.rs::backup_store_reports_reclaim_progress_and_leader_state` — redesigned around direct `MetaCommand`s + `seed_backup_object` (finding above); asserts `leader`/`janitor`/`objects.count` only, never `store` (`AdminInfo.backup_store` is always `None` under this fixture) |
| `admin_gc_reports_segment_janitor_progress_and_leader_state` (`admin_endpoint.rs`) | Converted → `sim_cluster_admin.rs::gc_reports_segment_janitor_progress_and_leader_state` — fixed to seal on the table's own data-plane leader (finding above) |
| `admin_ttl_reports_reaper_progress_and_ttl_tables` (`admin_endpoint.rs`) | **KEPT** `ProdEnv` whole — no primitive drives `ttl_reaper_loop` under `SimEnv`; its "tables" half alone gets a new sibling, `sim_cluster_admin.rs::ttl_tables_lists_a_ttl_enabled_table` |
| `admin_config_reports_auth_state_and_never_serves_the_secret` (`admin_endpoint.rs`) | **KEPT** — no `SimCluster` constructor knob configures `dynamo_auth` |
| `admin_raftkv_default_does_not_materialize_the_dataset` (`admin_endpoint.rs`) | **KEPT** — `SimCluster`'s engine is `MemoryEngine`, not `LsmEngine`: no SSTable/block-read counter exists under `SimEnv` |
| `admin_segment_store_reports_shard_placement_and_local_objects` (`admin_endpoint.rs`) | **KEPT** — `SimCluster`'s shared segment store is `S3`-kind, never `Cluster`-kind; no `ClusterSegmentStore` per-node replica-placement primitive exists under `SimEnv` |
| `admin_segment_store_reports_null_shards_for_the_fs_kind` (`admin_endpoint.rs`) | **KEPT** — identical store-kind gap; genuinely `fs`-kind-specific |
| `admin_live_is_200_while_a_genuinely_leaderless_admin_health_is_503` (`admin_endpoint.rs`) | **KEPT** — `SimCluster::new` unconditionally settles the control group's first election before returning; no constructor for a node that never completes bootstrap |
| `system_table_lists_every_seeded_entity_kind`, `system_table_pagination_is_gapless_and_duplicate_free` (`system_table.rs`) | **KEPT `ProdEnv` whole** — `ctx.control_storage` is always `None` under `SimCluster` (finding 3 above) |
| `metrics_endpoint_surfaces_control_plane_counters` (`metrics_endpoint.rs`) | **KEPT `ProdEnv` whole** — its real subject is the raw-text `/metrics` listener on the dynamo port, not the JSON `AdminHost` route table; a JSON-route analog exists instead: `sim_cluster_admin.rs::admin_metrics_surfaces_control_plane_counters` (the scenario that found finding 2 above) |

12 tests untouched (PR 6's own mutating-action territory, not even a
reason comment, per this rung's own scope): `admin_interface_surfaces_
state_and_actions`, `admin_data_write_dynamo`, `admin_table_management_
create_and_drop`, `seed_load_does_not_storm_cp_elections`, `admin_seed_
writes_synthetic_keys`, `admin_split_in_place_children_inherit_the_
parents_own_replicas`, `admin_system_table_split_lineage_after_a_real_
split`, `admin_credentials_put_rotate_revoke_round_trip`, `admin_
credentials_put_on_a_follower_is_relayed_to_the_leader`, `admin_control_
transfer_moves_leadership_to_the_named_node`, `admin_control_transfer_
on_a_follower_is_refused`, `admin_storage_compact_action`.

**Before/after counts**: `admin_endpoint.rs`: 23 → 18 (5 removed; one
now-dead bring-up helper, `bring_up_with_fs_backup_store`, whose one
caller was the removed backup-store test, deleted alongside it).
`system_table.rs`: 2 → 2 (kept whole). `metrics_endpoint.rs`: 1 → 1
(kept whole). `sim_cluster_admin.rs`: 14 new tests (7 scenarios).

**No product bug found in the widened dispatch code itself** (`admin.rs`/
`dynamo.rs`) — see above for the one real, previously-latent `SimCluster`
fixture bug found and fixed, and the two scenario-authoring issues found
and fixed. Every scenario passed at its pinned seed and every
`_over_seeds` seed once these were fixed.

**Gates, in the required order**: `cargo test -p animusd --test admin_
endpoint --test system_table --test metrics_endpoint` on the untrimmed
files (26 passed, 15.5s); `cargo test -p animusd --lib sim_cluster_
admin -- --test-threads=2` (14 passed, 0 failed — after fixing the two
scenario-authoring bugs and the fixture bug above); trim, then `cargo
test -p animusd --test admin_endpoint --test system_table --test
metrics_endpoint` again (21 passed — 18 + 2 + 1, clean build with no
dead-code warnings after deleting `bring_up_with_fs_backup_store`);
`cargo test -p animusd --lib sim_cluster -- --test-threads=2` (367
passed, 0 failed, 2 ignored, 1005.43s — 353 baseline + this PR's 14 new
tests; anchored-sampler RSS: first ~62 MB, peak ~891 MB, last ~167 MB,
consistent with every prior rung's own no-leak trajectory, well under
the 6 GB/30 min budget); `cargo fmt --all --check` (one pass needed —
`sim_cluster_admin.rs`'s own long call sites, applied via `cargo fmt
--all`, then clean); `cargo clippy -p animusd --all-targets
--all-features -- -D warnings` (clean). `Cargo.lock` unchanged. `git
diff --stat` against the PR 4 baseline: `lib.rs` (+27, the new module's
doc-comment registration), `sim_cluster.rs` (+107/-6, `put_raw` plus the
`MetricsHandle` fix), `admin_endpoint.rs` (-769 net, five tests removed
plus six reason comments plus one dead helper deleted), `metrics_
endpoint.rs`/`system_table.rs` (+14 each, a reason-comment doc block
each), `CLAUDE.md` (+16, two stale-reference corrections),
`engineering-lessons.md` (+58, the new lesson entry), plus the new
`sim_cluster_admin.rs` itself (937 lines).

See `docs/roadmap.md`'s C-08 entry for the full record; `docs/
engineering-lessons.md`'s new entry for the general lesson on shared
`MetricsHandle::noop()` sinks corrupting a gauge while leaving a counter
merely inflated.

## 2026-09-08 amendment — Rung H, PR 6 landed (admin dispatch mutating actions)

Closes PR 6's own scope, the sibling of PR 5 (pure observers): the admin
HTTP-JSON interface's **mutating** actions — `POST /admin/data/dynamo`,
`/admin/data/drop-table`, `/admin/data/seed`, `/admin/tablet/split`,
`/admin/credentials`(`/rotate`/`/revoke`), and `/admin/control/transfer`.
Converts 8 of `admin_endpoint.rs`'s remaining 12 tests (`admin_data_write_
dynamo`, `admin_table_management_create_and_drop`, `admin_seed_writes_
synthetic_keys`, `admin_split_in_place_children_inherit_the_parents_own_
replicas`, `admin_credentials_put_rotate_revoke_round_trip`, `admin_
credentials_put_on_a_follower_is_relayed_to_the_leader`, `admin_control_
transfer_moves_leadership_to_the_named_node`, `admin_control_transfer_on_
a_follower_is_refused`) into a new `crates/animusd/src/sim_cluster_
admin_actions.rs` (8 scenarios × pinned-seed + 5-seed `_over_seeds` = 16
tests). **No `admin.rs`/`dynamo.rs` dispatch change was needed** — every
route this PR drives was already reachable through `GenericAdminHost`
(PR 2's own newtype), the identical seam PR 5 already established.

**One real, previously-latent seam bug found and fixed, this rung's
second (the first was PR 5's shared `MetricsHandle::noop()` finding)**:
`ClientCtx::admin_transfer_control_leadership` (`lib.rs`) already had a
fully generic `<E: Env, R: RelayClient>` signature (rung C5) — its own
commit-wait loop, though, still read `tokio::time::Instant::now()`/called
`tokio::time::sleep(..)` directly instead of `self.env.now()`/
`self.env.sleep(..)`. A generic *signature* proves nothing about whether a
function's *body* actually avoids the real clock/timer — the identical
lesson rung G, C-07 PR 2's `index_drain::seal_now` finding already
recorded, and rung F/G's own `recovery_grace_now_ms`/`txn_recover`
findings before that; this is that lesson's **third** recurrence in this
crate, recorded as such (not a new entry) in `docs/engineering-lessons.md`.
`SimEnv` has no real Tokio reactor, so `tokio::time::sleep` panics ("there
is no reactor running") the instant this loop's first poll iteration is
reached whenever the initial arm attempt doesn't resolve on its very first
pass — found immediately by this PR's own control-transfer scenario, its
first real exercise of the route. Fixed with the same
`self.env.now().saturating_add(..)`/`self.env.now() >= deadline`/
`self.env.sleep(..)` conversion every prior rung's own `tokio::time`
finding used; the method's documented behavioral contract (issue #688's
fixed contract, the target-must-be-the-observed-leader semantics) is
unchanged.

**Test-by-test disposition**:

| Real-socket test (`admin_endpoint.rs`) | Disposition |
|---|---|
| `admin_data_write_dynamo` | Converted → `sim_cluster_admin_actions.rs::data_write_dynamo` — issued from a node hosting no replica of the table's own tablet |
| `admin_table_management_create_and_drop` | Converted → `sim_cluster_admin_actions.rs::table_management_create_and_drop` — issued from a control follower |
| `admin_seed_writes_synthetic_keys` | Converted → `sim_cluster_admin_actions.rs::seed_writes_synthetic_keys` — the full bulk-seed contract (404 on a nonexistent table, written-count/raw-scan/DynamoDB-readback for a simple and a composite table, the percent-encoded key round trip through `/admin/storage/key`; `percent_encode` moved with its last caller) |
| `admin_split_in_place_children_inherit_the_parents_own_replicas` | Converted → `sim_cluster_admin_actions.rs::split_in_place_children_inherit_the_parents_own_replicas` — a 4-node/RF-3 `SimCluster` (`create_table_with_replication`'s own deterministic `0..replication` replica set reproduces the real fixture's "n3 stays idle" premise exactly); `bring_up_with_streams_quiesce` moved with its last caller |
| `admin_credentials_put_rotate_revoke_round_trip` | Converted → `sim_cluster_admin_actions.rs::credentials_put_rotate_revoke_round_trip` |
| `admin_credentials_put_on_a_follower_is_relayed_to_the_leader` | Converted → `sim_cluster_admin_actions.rs::credentials_put_on_a_follower_is_relayed_to_the_leader` |
| `admin_control_transfer_moves_leadership_to_the_named_node` | Converted → `sim_cluster_admin_actions.rs::control_transfer_moves_leadership_to_the_named_node` — the whole-call retry discipline against every retryable 409, simplified from the real-socket original's own issue #671/#688 account since this fixture has no independent third-voter election-timer jitter to race; the scenario that found the seam bug above |
| `admin_control_transfer_on_a_follower_is_refused` | Converted → `sim_cluster_admin_actions.rs::control_transfer_on_a_follower_is_refused` |
| `admin_interface_surfaces_state_and_actions` | **KEPT** — its one remaining action, `/admin/storage/flush`, has the identical `MemoryEngine`-has-no-LSM-concept gap PR 5's own `admin_raftkv_default_does_not_materialize_the_dataset` KEPT reason already names; kept whole as this crate's sole remaining real-socket admin observer sweep |
| `seed_load_does_not_storm_cp_elections` | **KEPT** — real-thread election-timing liveness; `SimEnv`'s virtual clock cannot trip a wall-clock election timeout |
| `admin_system_table_split_lineage_after_a_real_split` | **KEPT** — `ctx.control_storage` is always `None` under `SimCluster` (the identical gap `tests/system_table.rs`'s own two tests are KEPT for, PR 5) |
| `admin_storage_compact_action` | **KEPT** — `CpGroup::compact_now()` is `None` for the `MemoryEngine` backend (the identical gap this test's own sibling flush action has) |

**Before/after counts**: `admin_endpoint.rs`: 18 → 10 (8 removed, plus two
now-dead helpers deleted — `bring_up_with_streams_quiesce`, `percent_
encode`, both losing their last caller). `sim_cluster_admin_actions.rs`:
16 new tests (8 scenarios).

**No product bug found beyond the `admin_transfer_control_leadership`
clock-conversion finding above.** Every scenario passed at its pinned seed
and every `_over_seeds` seed on the first full run once that fix landed —
no scenario-authoring bug needed a redesign this PR (unlike PR 5's two).

**Gates, in the required order**: `cargo build -p animusd --all-targets`
(clean; checkpoint push before the gates below, per this rung's own
container-loss-recovery instruction); `cargo test -p animusd --test
admin_endpoint` on the untrimmed file (18 passed, 10.64s); `cargo test -p
animusd --lib sim_cluster_admin -- --test-threads=2` (30 passed, 0
failed — 14 from PR 5 + 16 from this PR, both reached by the unchanged
`sim_cluster_admin` substring filter since this module is named `sim_
cluster_admin_actions`); trim, then `cargo test -p animusd --test admin_
endpoint` again (10 passed, 10.33s, clean build with no dead-code
warnings after deleting the two now-dead helpers); `cargo test -p animusd
--lib sim_cluster -- --test-threads=2` (383 passed, 0 failed, 2 ignored,
753.07s — 367 baseline + this PR's 16 new tests; anchored-sampler RSS,
sampled every 10s from the test binary's own `/proc/<pid>/status`
`VmRSS`: first ~66 MB, peak ~894 MB, last ~171 MB, well under the 6 GB/30
min budget and consistent with every prior rung's own no-leak
trajectory); `cargo test -p animusd --test dashboard_endpoint
dashboard_u05_control_member_actions` (1 passed, 0.37s — the one other
real caller of `/admin/control/transfer` found by grepping `tests/` for
"transfer"; no dedicated `admin_control_transfer.rs` file exists); `cargo
fmt --all --check` (one pass needed — `sim_cluster_admin_actions.rs`'s
own long call sites, applied via `cargo fmt --all`, then clean); `cargo
clippy -p animusd --all-targets --all-features -- -D warnings` (one fix
needed — a `nonminimal_bool` lint on the drop-table convergence
predicate, `!x.is_some_and(|w| !w.is_null())` rewritten to `x.is_none_or
(Value::is_null)` per clippy's own suggestion; clean after). `Cargo.lock`
unchanged throughout. `git diff --stat` against the PR 5 baseline:
`lib.rs` (+21/-3, the new module's doc-comment registration plus the
three-line `admin_transfer_control_leadership` clock fix), `admin_
endpoint.rs` (net reduction, eight tests removed plus four reason
comments plus two dead helpers deleted), plus the new `sim_cluster_
admin_actions.rs` itself.

See `docs/roadmap.md`'s C-08 entry for the updated status and
`docs/engineering-lessons.md`'s matching dated note (the third recorded
recurrence of "a generic signature does not imply a seam-clean body") for
the general lesson.

## 2026-09-08 amendment — Rung H, PR 7 landed (`dashboard_endpoint.rs` siblings + `console_endpoint.rs` close-out)

Closes PR 7's own scope: 12 of `tests/dashboard_endpoint.rs`'s 16 tests
(`docs/roadmap.md`'s U-01/U-02/U-04/U-05/U-07 dashboard follow-ups) now
have a deterministic sibling in a new `crates/animusd/src/sim_cluster_
dashboard.rs` (12 scenarios, 23 tests with `_over_seeds` — 11 scenarios at
pinned-seed + 5-seed, one, `u05_tablet_actions`, a pure marker check
against the served `TABLETS_JS` constant that touches no `SimCluster` at
all, so it carries no seed); `dashboard_endpoint.rs` is trimmed to its
remaining 4. `tests/console_endpoint.rs` needed no change at all — its own
3 tests already reached their final `ProdEnv` disposition in PR 3, and
this PR's own read confirmed nothing in it became sim-convertible in the
interim. **Landed ahead of PR 6 in the series' own numeric order**: PR 6
(admin mutating actions) and this PR are independent surface — PR 7
depends only on PR 2's groundwork (`GenericAdminHost`/`SimCluster::
admin`), not on anything PR 6 touches — so there was no reason to block
this PR on PR 6 landing first. **No `admin.rs`/`console.rs`/`dynamo.rs`/
`sim_cluster.rs` change was needed** — every generic handler this PR's
scenarios reach was already built by PR 2 and widened by PR 2 (`update_
time_to_live`/`create_backup`/`delete_backup`), PR 2a/2b (`dispatch_
table_op`'s `CreateTable`/`UpdateTable`-throughput arms), and rung G/C-07
PR 2 (`CreateTable`'s own GSI/LSI/stream acceptance) — confirmed by direct
inspection of `dispatch_table_op`'s `CreateTable` arm rather than assumed.

**The render-marker half of every converted test reads the served assets'
own compile-time constants directly** — `crate::dashboard::{HTML, CORE_JS,
OVERVIEW_JS, TABLETS_JS, TXNS_JS, STORAGE_JS, BACKUPS_JS, BROWSER_JS,
NODE_JS}` — rather than fetching them over a socket. `animus_node::admin`'s
own module doc says the dashboard's static assets (the shell HTML, every
per-view `.js`) are served from `animusd`'s own `handle_conn` **before**
its `AdminHost` dispatch table is ever reached; `SimCluster::admin` calls
straight into `animus_node::admin::dispatch` (through `GenericAdminHost`),
never `handle_conn`, so it cannot fetch `/admin/ui/*` at all. Since every
one of these assets is a plain `include_str!` compile-time constant with
no request-time computation whatsoever, reading the constant directly is
not a narrower proof than fetching it over a socket would have been — the
exact same bytes, minus a serving mechanism this rung has no reason to
reproduce. This is judged a new, generalizable lesson (grepped first, not
in `docs/engineering-lessons.md` already) — see that file's own new entry.
Each converted test's own **live JSON round trip** (`/admin/txns`,
`/admin/backups`, `/admin/restores`, `/admin/status`, `/admin/control/
members`, the four U-07 observability routes, and two "route exists"
probes) goes through `SimCluster::admin` instead, issued from a **control
follower** for a cluster-wide route (`super::sim_cluster_console::
control_leader_and_follower`) or a table's own tablet **non-leader**
(`non_leader_of_table`) once a table exists — reusing PR 3's shared
helpers exactly as `sim_cluster_admin.rs`/`sim_cluster_console_stream.rs`/
`sim_cluster_console_table_config.rs` already do, rather than duplicating
them.

**Test-by-test disposition**:

| Real-socket test (`dashboard_endpoint.rs`) | Disposition |
|---|---|
| `dashboard_u01_render_only_fixes` | Converted → `sim_cluster_dashboard.rs::u01_render_only_fixes` |
| `dashboard_u02_backups_tab` | Converted → `u02_backups_tab` |
| `dashboard_u07_backup_store_card` | Converted → `u07_backup_store_card` |
| `dashboard_u07_ttl_reaper_card` | Converted → `u07_ttl_reaper_card` |
| `dashboard_u07_gc_card` | Converted → `u07_gc_card` |
| `dashboard_u07_segment_store_card` | Converted → `u07_segment_store_card` |
| `dashboard_u04_ttl_row` | Converted → `u04_ttl_row` (via the `/admin/data/dynamo` proxy, mirroring the original's own posting shape) |
| `dashboard_u04_create_table_form` | Converted → `u04_create_table_form` |
| `dashboard_u05_control_members_panel` | Converted → `u05_control_members_panel`, issued from a control follower |
| `dashboard_u05_tablet_actions` | Converted → `u05_tablet_actions` — a pure marker check, touches no `SimCluster`, carries no seed |
| `dashboard_u05_node_actions` | Converted → `u05_node_actions` |
| `dashboard_u05_control_member_actions` | Converted → `u05_control_member_actions` |
| `dashboard_serves_spa_with_cors_and_peers` | **KEPT** `ProdEnv` — real HTTP framing (status line, `Content-Type`/CORS headers, `OPTIONS` preflight) `SimCluster::admin` cannot reproduce |
| `dashboard_role_gating_split_deployment` | **KEPT** — a genuine control-only/data-only process split; `SimCluster` has no node-role concept |
| `control_node_streams_read_path_is_ground_truth` | **KEPT** — the identical role-split reason |
| `dashboard_u05_lineage_panel` | **KEPT** — `GET /admin/system-table` reads `ctx.control_storage`, always `None` under `SimCluster` (the identical gap PR 5's own `system_table.rs` disposition documents) |

**Before/after counts**: `dashboard_endpoint.rs`: 16 → 4 (12 converted).
`console_endpoint.rs`: 3 → 3 (unchanged). `sim_cluster_dashboard.rs`: 23
new tests (12 scenarios).

**The full `ProdEnv` residue of this rung so far (PRs 3–7), collected
here so PR 8's close-out can copy it rather than re-deriving it**:

- **From PR 3** — `console_endpoint.rs` (3, kept whole):
  `console_serves_shell_assets_and_deep_links_on_combined_node` (real HTTP
  framing/CORS/static-asset bytes plus a JSON-routing tail now also
  covered by a sim sibling), `console_serves_shell_on_data_only_node` and
  `console_addr_panics_on_control_only_node` (a genuine control-only/
  data-only process split, twice).
- **From PR 4** — `console_stream.rs` (1 of 4 kept):
  `ttl_deletion_carries_the_service_user_identity_through_the_console` (no
  primitive drives `ttl_reaper_loop` under `SimEnv`). `console_table_
  config.rs` (4 of 9 kept): `add_and_drop_gsi_round_trip`/`add_gsi_
  records_a_declared_attribute_type`/`add_gsi_rejects_an_unknown_
  attribute_type` (blocker (d): `UpdateTable` with an index change has no
  `dispatch_table_op` sub-arm) and `table_detail_shows_pitr_status_and_
  backups` (`UpdateContinuousBackups` has no generic dispatch arm).
- **From PR 5** — `admin_endpoint.rs` (6 of 23 kept, with reason comments;
  12 more untouched, PR 6's own territory — see below):
  `admin_ttl_reports_reaper_progress_and_ttl_tables` (kept whole; its
  "tables" half alone got a new sim sibling), `admin_config_reports_
  auth_state_and_never_serves_the_secret` (no `SimCluster` constructor
  knob for `dynamo_auth`), `admin_raftkv_default_does_not_materialize_
  the_dataset` (this fixture's engine is `MemoryEngine`, no SSTable
  block-read counter exists), `admin_segment_store_reports_shard_
  placement_and_local_objects`/`admin_segment_store_reports_null_shards_
  for_the_fs_kind` (this fixture's shared segment store is always
  `S3`-kind, never `cluster`/`fs`), `admin_live_is_200_while_a_
  genuinely_leaderless_admin_health_is_503` (`SimCluster::new` always
  settles the control group's first election before returning; no
  constructor for a node that never completes bootstrap). Plus `tests/
  system_table.rs` (2, kept whole: `ctx.control_storage` always `None`
  under `SimCluster`) and `tests/metrics_endpoint.rs` (1, kept whole: its
  real subject is the raw-text `/metrics` listener on the dynamo port,
  unreachable through the JSON `AdminHost` route table — a JSON analog
  exists instead, `sim_cluster_admin.rs::admin_metrics_surfaces_control_
  plane_counters`).
- **From PR 6** — `admin_endpoint.rs` (4 of the 12 PR 5 left untouched,
  now kept with reason comments; the other 8 converted into `sim_cluster_
  admin_actions.rs`, `admin_endpoint.rs` going 18 → 10): `admin_interface_
  surfaces_state_and_actions` (its one remaining action, `/admin/storage/
  flush`, has the identical `MemoryEngine`-has-no-LSM-concept gap PR 5's
  own `admin_raftkv_default_does_not_materialize_the_dataset` names —
  kept whole as this crate's sole remaining real-socket admin observer
  sweep), `seed_load_does_not_storm_cp_elections` (real-thread election-
  timing liveness), `admin_system_table_split_lineage_after_a_real_split`
  (`ctx.control_storage` always `None`, the identical gap `system_
  table.rs`'s own disposition documents), `admin_storage_compact_action`
  (`CpGroup::compact_now()` is `None` for the `MemoryEngine` backend).
  PR 6 also found and fixed a second real, previously-latent seam bug:
  `ClientCtx::admin_transfer_control_leadership`'s commit-wait loop still
  read the real clock directly despite an already-generic `<E, R>`
  signature — the third recorded recurrence of this exact lesson in this
  crate.
- **From PR 7 (this PR)** — `dashboard_endpoint.rs` (4, kept whole, see
  the disposition table above): `dashboard_serves_spa_with_cors_and_peers`,
  `dashboard_role_gating_split_deployment`, `control_node_streams_read_
  path_is_ground_truth` (real HTTP framing, twice a genuine role split),
  `dashboard_u05_lineage_panel` (`ctx.control_storage` always `None`).

**This PR was written in two phases.** Phase A (design/implementation)
was a re-implementation, not the original PR 7 pass — an earlier,
fully-written version was lost to a container rebuild before it could be
pushed. It was rebuilt source-only against a checkout of PR 5's own
landed commit (`35280f23`) in a dedicated worktree, with no cargo
invocation available in that phase (a sibling session owned the shared
build tree); the design was checked by direct inspection of the current
source at every step that phase's own account claimed —
`dispatch_table_op`'s `CreateTable`/`UpdateTable` arms, every `AdminHost`
observer handler's `<E, R>`-generic signature, `SimCluster::admin`/
`dynamo`/`put_raw`'s exact signatures, and `sim_cluster_console.rs`'s
shared-helper signatures were all read from the live tree, not assumed
from an earlier session's memory. Phase B then rebased this PR onto PR
6's own landed commit (`02b66fd4`) — four textual doc conflicts (`crates/
animusd/CLAUDE.md`, `lib.rs`, this file, `docs/roadmap.md`), each
resolved by keeping both PRs' additions in sequence — and ran every gate
below for real, in the main tree. **No product/scenario-authoring bug was
found**: every one of the 23 `sim_cluster_dashboard.rs` tests passed at
its pinned seed and every `_over_seeds` seed on the very first run, with
no fixture change and no scenario rewrite needed — the design Phase A
committed without being able to compile it held up unchanged.

**Gates, in the required order, all foreground**: `cargo build -p
animusd --all-targets` (clean, 1m55s); `cargo test -p animusd --test
dashboard_endpoint` on the untrimmed file, checked out from PR 6's own
commit (16 passed, 0 failed, 1.98s); `cargo test -p animusd --lib
sim_cluster_dashboard -- --test-threads=2` (23 passed, 0 failed, 7.62s);
trim (already applied in this commit) and re-check; `cargo test -p
animusd --test dashboard_endpoint` again, trimmed (4 passed, 0 failed,
1.44s); `cargo test -p animusd --lib sim_cluster -- --test-threads=2`
(**406 passed, 0 failed, 2 ignored, 752.46s** — 383 PR 6 baseline + this
PR's 23 new tests, exactly as predicted; anchored-sampler RSS, sampled
every 10s from the test binary's own `/proc/<pid>/status` `VmRSS`: first
~99 MB, peak ~816 MB, last ~133 MB, well under the 6 GB/30 min budget and
consistent with every prior rung's own no-leak trajectory); `cargo fmt
--all --check` (one pass needed — `sim_cluster_dashboard.rs`'s own long
call sites plus a stray trailing blank line in the trimmed `dashboard_
endpoint.rs`, applied via `cargo fmt --all`, then clean; re-ran the
`sim_cluster_dashboard` suite after to confirm the reformat changed
nothing behaviorally — still 23 passed); `cargo clippy -p animusd
--all-targets --all-features -- -D warnings` (clean, no fix needed).
`Cargo.lock` unchanged (confirmed via `git diff` — no dependency
touched).

See `docs/roadmap.md`'s C-08 entry for the full record; `docs/
engineering-lessons.md`'s new entry on why a real-socket test whose
assertion is against a byte-identical compile-time-constant asset does
not need its sim fixture to reproduce the serving mechanism at all.

## 2026-09-08 amendment — Rung H closed (C-08 complete)

PR 8 is what its own row above and the PR-series amendment promised: no
source, test, or `Cargo` change — this ADR's own D-train row and this
amendment, `docs/roadmap.md`'s C-08 entry, and `crates/animusd/CLAUDE.md`'s
residual-inventory paragraph and new appendix. Rung H (C-08) is now
**closed**: the admin dispatch table's pure observers and mutating
actions, the console backend's table/item/stream/config surface, and the
dashboard's card/panel/action surface are all `SimCluster`-reachable,
exactly as this rung's own opening amendment (above) set out to do.

**What the rung set out to do.** Rung G's own close-out named admin/
console/dashboard HTTP the largest remaining unowned residual group,
every rung back through the D3-closing inventory having flagged it
without claiming it — ten real-socket `tests/*.rs` files, 67 tests
(`admin_endpoint.rs` 23, `dashboard_endpoint.rs` 16, `console_
endpoint.rs` 3, `console_create_table.rs` 4, `console_items.rs` 4,
`console_stream.rs` 4, `console_table_config.rs` 9, `console_tables.rs`
1, `metrics_endpoint.rs` 1, `system_table.rs` 2). The opener's own Gap
section found both `impl AdminHost for ClientCtx` and `impl ConsoleBackend
for ClientCtx` mostly pure signature widening already, thanks to rung
C5's own generic `ClientCtx<E, R>` field types — the plan was the
identical widen-then-add-a-generic-entry-point template D3/D4/C-06/C-07
had already validated six times over, plus a new `execute_routed_as_
generic` entry point and `SimCluster::admin`/`console` primitives.

**PR 2's own course-correction, and why it matters beyond this rung.** The
opener's template — widen a *free function*, add a parallel generic
sibling, leave production's own call site untouched — does not transfer
unmodified to a *trait impl on `ClientCtx` itself*. `ClientCtx` (bare,
default-type-parameter) is production's own concrete type; PR 2's first
cut widened `impl AdminHost for ClientCtx`/`impl ConsoleBackend for
ClientCtx` **in place** to `<E: Env, R: RelayClient>` — which does not
add a parallel path the way widening a free function does, it silently
replaces the *only* implementation those traits have, for every caller
including production's own, the instant a shared method body is forced to
call the narrower `execute_routed_as_generic` to compile generically. The
untrimmed real-socket suites caught this immediately: nine real failures
(`UpdateTimeToLive`/`CreateBackup`/`DeleteBackup`, both genuinely
widenable and fixed correctly, plus two GSI-`UpdateTable` tests that are
squarely blocker (d), out of scope). A first attempted fix for the GSI
two compounded the mistake one layer up — a concrete interception inside
`console.rs::serve`/`handle_conn`, a file this rung's own non-goals
already named off-limits. **Corrected the same day**: the two impls
reverted to concrete, byte-identical to before the rung; a new one-field
newtype pair, `GenericAdminHost<E, R>(pub ClientCtx<E, R>)`/
`GenericConsoleBackend<E, R>(pub ClientCtx<E, R>)`, carries a *separate*
`impl<E, R> Trait for Generic*<E, R>` reaching the generic dispatch
instead — coherence allows this because the two impls target genuinely
different types, even though one always wraps the other. Every PR from 3
onward built on this newtype pair without incident. The general lesson —
a blanket generic `impl Trait for ClientCtx<E, R>` is not the
free-function-widening shape every earlier rung used safely; reach for a
newtype the moment the type being widened is production's own
default-instantiated concrete type — is recorded in `docs/
engineering-lessons.md` (two 2026-09-08 entries, the second rewritten in
review) and is this rung's own most consequential finding: every prior
D2–C-07 rung widened *functions*, and this is the first time the template
met a *trait impl on the production type itself*.

**PR-by-PR, what landed.** PR 2 (groundwork, corrected same day): both
impls widened to their generic newtype siblings per the correction above;
blockers (a)/(b) (four `tokio::time` commit-wait sites in the credential
handlers and `action_data_seed`) converted to `ctx.env`; blocker (c)
(`execute_routed_as_generic`) added; `SimCluster::admin`/`console` added,
mirroring `SimCluster::dynamo`'s shape (no HTTP framing — `dispatch`/
`route` never see one). PR 3 (console reachable + first siblings): every
sim-convertible test in `console_tables.rs` (1), `console_create_
table.rs` (4), `console_items.rs` (4) — 9 tests — converted into a new
`sim_cluster_console.rs` (10 scenarios, 20 tests with `_over_seeds`, the
tenth a new JSON-routing/error-mapping scenario with no real-socket
original), all three files deleted whole; `console_endpoint.rs`'s own 3
tests stay `ProdEnv`. PR 4 (`console_stream.rs`/`console_table_config.rs`
siblings): 3 of 4 and 5 of 9 respectively — 8 tests — converted into two
new modules reusing PR 3's shared helpers (widened to `pub(crate)`); the
TTL-identity test, three GSI-DDL tests, and one PITR-present test stay
`ProdEnv`. PR 5 (admin dispatch pure observers): 5 of `admin_endpoint.rs`'s
23 tests plus the "tables" half of a sixth converted into a new
`sim_cluster_admin.rs` (7 scenarios, 14 tests); found and fixed the
shared-`MetricsHandle::noop()` gauge-corruption bug (below); `system_
table.rs`/`metrics_endpoint.rs` stay `ProdEnv` whole. PR 6 (admin dispatch
mutating actions): 8 of `admin_endpoint.rs`'s remaining 12 tests converted
into a new `sim_cluster_admin_actions.rs` (8 scenarios, 16 tests); found
and fixed the `admin_transfer_control_leadership` real-clock body bug
(below), the third recorded recurrence of that lesson. PR 7 (`dashboard_
endpoint.rs` siblings + `console_endpoint.rs` close-out, landed ahead of
PR 6 in the series' own numeric order since it depended only on PR 2's
groundwork): 12 of `dashboard_endpoint.rs`'s 16 tests converted into a new
`sim_cluster_dashboard.rs` (12 scenarios, 23 tests, one — the pure-marker
`u05_tablet_actions` — carrying no seed); `console_endpoint.rs` needed no
change. **PR 7 (landed as #777) was written in two phases.** Phase A
(design/implementation) was a re-implementation, not the original PR 7
pass — an earlier, fully-written version was lost to a container rebuild
before it could be pushed, rebuilt source-only against PR 5's landed
commit (`35280f23`) in a dedicated worktree with no `cargo` available (a
sibling session owned the shared build tree); its own gates were
explicitly recorded as **pending (Phase B)** in that phase's own landing
amendment. **Phase B then rebased #777 onto PR 6's own landed commit
(`02b66fd4`)** — four textual doc conflicts, each resolved by keeping
both PRs' additions in sequence — **and ran every gate for real, in the
main tree: no product or scenario-authoring bug was found**, every one of
the 23 `sim_cluster_dashboard.rs` tests passed at its pinned seed and
every `_over_seeds` seed on the very first run, with no fixture change
and no scenario rewrite needed — the design Phase A committed without
being able to compile it held up unchanged. See the "Test-count
trajectory" paragraph below for the confirmed numbers. No `admin.rs`/
`console.rs`/`dynamo.rs`/`sim_cluster.rs` change was needed by PR 3, 4, or
7 — every generic handler their scenarios reach was already built by PR 2
and widened by PR 2/2a/2b/3a (`dispatch_table_op`'s `CreateTable`/
`UpdateTable` arms) and rung G/C-07 PR 2 (`CreateTable`'s own GSI/LSI/
stream acceptance).

**The final `ProdEnv` residue, per test, with its reason** — the complete
account, gathering what PRs 3–7's own amendments already stated
individually, made final here:

| File | Kept test | Reason |
|---|---|---|
| `console_endpoint.rs` | `console_serves_shell_assets_and_deep_links_on_combined_node` | Real HTTP framing (status line, `Content-Type`, static-asset bytes, deep-link routing) — `SimCluster::console` builds no framing at all; its own JSON-routing tail is covered by the new `console_error_mapping_and_json_routing_assertions` sim sibling instead of trimming this test |
| `console_endpoint.rs` | `console_serves_shell_on_data_only_node` | Genuine control-only/data-only process split — `SimCluster` has no node-role concept |
| `console_endpoint.rs` | `console_addr_panics_on_control_only_node` | Identical role-split reason |
| `console_stream.rs` | `ttl_deletion_carries_the_service_user_identity_through_the_console` | No primitive drives `ttl_reaper_loop` under `SimEnv` — `SimCluster::new`/`restart` never spawn it, unlike the heartbeat/reconciler/backup-janitor/segment-janitor loops |
| `console_table_config.rs` | `add_and_drop_gsi_round_trip` | Blocker (d): `UpdateTable` with an index change has no `dispatch_table_op` sub-arm — filed under "index DDL beyond plain `CreateTable`" |
| `console_table_config.rs` | `add_gsi_records_a_declared_attribute_type` | Identical blocker (d) |
| `console_table_config.rs` | `add_gsi_rejects_an_unknown_attribute_type` | Identical blocker (d) |
| `console_table_config.rs` | `table_detail_shows_pitr_status_and_backups` | `Operation::UpdateContinuousBackups` has no arm in `dispatch_item_op` or `dispatch_table_op` — the real `BeginBackup`/PITR data behind a populated response is a separate, still-unowned residual (distinct from blocker (d)) |
| `admin_endpoint.rs` | `admin_ttl_reports_reaper_progress_and_ttl_tables` | No primitive drives `ttl_reaper_loop` under `SimEnv`; its "tables" half alone has a sim sibling (`sim_cluster_admin.rs::ttl_tables_lists_a_ttl_enabled_table`) |
| `admin_endpoint.rs` | `admin_config_reports_auth_state_and_never_serves_the_secret` | No `SimCluster` constructor knob configures `dynamo_auth` |
| `admin_endpoint.rs` | `admin_raftkv_default_does_not_materialize_the_dataset` | `SimCluster`'s engine is `MemoryEngine`, not `LsmEngine` — no SSTable/block-read counter exists under `SimEnv` |
| `admin_endpoint.rs` | `admin_segment_store_reports_shard_placement_and_local_objects` | `SimCluster`'s shared segment store is always `S3`-kind, never `Cluster`-kind — no `ClusterSegmentStore` per-node replica-placement primitive exists under `SimEnv` |
| `admin_endpoint.rs` | `admin_segment_store_reports_null_shards_for_the_fs_kind` | Identical store-kind gap, genuinely `fs`-kind-specific |
| `admin_endpoint.rs` | `admin_live_is_200_while_a_genuinely_leaderless_admin_health_is_503` | `SimCluster::new` unconditionally settles the control group's first election before returning — no constructor for a node that never completes bootstrap |
| `admin_endpoint.rs` | `admin_interface_surfaces_state_and_actions` | Its one remaining action, `/admin/storage/flush`, has the identical `MemoryEngine`-has-no-LSM-concept gap as `admin_raftkv_default_does_not_materialize_the_dataset` |
| `admin_endpoint.rs` | `seed_load_does_not_storm_cp_elections` | Real-thread election-timing liveness — `SimEnv`'s virtual clock cannot trip a wall-clock election timeout (class A, permanent) |
| `admin_endpoint.rs` | `admin_system_table_split_lineage_after_a_real_split` | `ctx.control_storage` always `None` under `SimCluster` — identical gap to `system_table.rs`'s own two tests |
| `admin_endpoint.rs` | `admin_storage_compact_action` | `CpGroup::compact_now()` is `None` for the `MemoryEngine` backend — identical gap to `admin_interface_surfaces_state_and_actions`'s flush action |
| `system_table.rs` | `system_table_lists_every_seeded_entity_kind` | `ctx.control_storage` (ADR 0038's `DRIVER_APPLIED` apply-task mirror engine) is always `None` under `SimCluster` — no background-loop driver for it exists under `SimEnv` |
| `system_table.rs` | `system_table_pagination_is_gapless_and_duplicate_free` | Identical `control_storage` gap |
| `metrics_endpoint.rs` | `metrics_endpoint_surfaces_control_plane_counters` | Its real subject is the raw-text `/metrics` listener on the dynamo port, not the JSON `AdminHost` route table — unreachable through `SimCluster::admin`; a JSON-route analog exists instead (`sim_cluster_admin.rs::admin_metrics_surfaces_control_plane_counters`) |
| `dashboard_endpoint.rs` | `dashboard_serves_spa_with_cors_and_peers` | Real HTTP framing (status line, `Content-Type`/CORS headers, `OPTIONS` preflight) — `SimCluster::admin` returns a bare `(status, body)` pair, no framing at all (class C, permanent) |
| `dashboard_endpoint.rs` | `dashboard_role_gating_split_deployment` | Genuine control-only/data-only process split — `SimCluster` has no node-role concept |
| `dashboard_endpoint.rs` | `control_node_streams_read_path_is_ground_truth` | Identical role-split reason |
| `dashboard_endpoint.rs` | `dashboard_u05_lineage_panel` | `GET /admin/system-table` reads `ctx.control_storage`, always `None` under `SimCluster` — identical gap to `system_table.rs`'s own disposition |

25 tests total, none silently dropped; 42 converted to 89 deterministic
`SimCluster` tests (with `_over_seeds`) across six new modules.

**In-scope findings, all recorded in `docs/engineering-lessons.md`
already (grepped again at this close, nothing new to add):**

- **The shared `MetricsHandle::noop()` gauge-corruption bug (PR 5).**
  `SimCluster::new`/`SimCluster::grow` built every control `RaftNode` via
  the plain `RaftNode::start` constructor, which defaults its metrics
  sink to `env.metrics()` — `SimEnv` doesn't override `Env::metrics`'s
  trait-default, so this resolved to one process-wide `static
  MetricsHandle::noop()`. Harmless for a summed counter (merely
  inflated), but every node's control `RaftNode` on every `SimCluster` in
  the same test process shared one mutable `is_leader` gauge outright —
  invisible through five rungs' worth of corpora because none of them
  read that specific field before PR 5's own metrics scenario did. Fixed
  via `RaftNode::start_with_metrics` with a private per-node sink, shared
  with that node's `DataRole::raftkv_metrics`.
- **The `admin_transfer_control_leadership` seam bug (PR 6), third
  recorded recurrence.** An already-`<E, R>`-generic signature (since
  rung C5) proved nothing about its body: the commit-wait loop still read
  `tokio::time::Instant::now()`/called `tokio::time::sleep` directly,
  panicking under `SimEnv` ("no reactor running") the instant its own
  first `SimEnv`-driven caller reached it. Fixed with the standard
  `self.env.now()`/`self.env.sleep(..)` conversion — the identical fix
  rung G's `seal_now` finding and rung F/G's `recovery_grace_now_ms`/
  `txn_recover` findings before it already used.
- **The PITR/backups gap: `UpdateContinuousBackups` has no generic
  dispatch arm.** PR 2's widening covered `UpdateTimeToLive`/
  `CreateBackup`/`DeleteBackup` (all reachable through `/admin/data/
  dynamo` and the console's mutating endpoints, closing a real production
  regression the swap would otherwise have introduced), but
  `UpdateContinuousBackups` was never added to `dispatch_item_op` or
  `dispatch_table_op` — confirmed by direct inspection at PR 4, not
  assumed. `console_table_config.rs::table_detail_shows_pitr_status_and_
  backups` stays `ProdEnv` for this reason, distinct from blocker (d).
- **Blocker (d) — GSI/LSI `UpdateTable` stays the index-DDL residual.**
  `dispatch_table_op`'s `UpdateTable` arm has no sub-arm for an index
  change; `add_gsi`/`drop_gsi` on `GenericConsoleBackend` return
  `unsupported_by_generic_dispatch` naturally, via the unmodified
  fallthrough — the honest, already-documented residual, never given
  bespoke handling. Filed under "index DDL beyond plain `CreateTable`",
  the same call rung G made for `stream_backfill_seed_filter.rs`.
- **A real-socket test asserting against a byte-identical compile-time
  constant needs no serving mechanism reproduced (PR 7).** The
  dashboard's static assets (`HTML`/`CORE_JS`/`OVERVIEW_JS`/etc.) are
  plain `include_str!` constants served from `handle_conn` before
  `AdminHost::dispatch` is ever reached — `SimCluster::admin` cannot fetch
  them over a socket, but reading the constant directly is the exact same
  bytes, not a narrower proof.

**Lessons recorded across this rung** (`docs/engineering-lessons.md`, all
2026-09-08, none needing a new entry at this close):

1. "A generic dispatch's own coverage gaps are a real production
   regression the moment a NON-primary but still-production-reachable
   proxy is switched to it" (PR 2, Finding A).
2. "A blanket generic `impl Trait for ClientCtx<E, R>` silently narrows
   production's own dispatch to whatever the generic sibling covers" (PR
   2, corrected same day in review).
3. "A shared `MetricsHandle::noop()` is silently harmless for a counter
   but corrupts a gauge outright" (PR 5).
4. The generic-signature-does-not-imply-seam-clean-body lesson (originally
   rung G, C-07 PR 2's `seal_now` finding) gained its third recorded
   recurrence in place, `admin_transfer_control_leadership` (PR 6) — not a
   new entry, per that lesson's own "record recurrences in place" practice.
5. "A pushed branch with no open PR costs nothing and survives a
   container loss — push implementation commits before the long gates,
   not after" — the procedural fix this rung's own container-rebuild
   incidents motivated.
6. "A real-socket test whose assertion is against a byte-identical
   compile-time-constant asset does not need its sim fixture to reproduce
   the serving mechanism at all" (PR 7).

**Test-count trajectory** (`cargo test -p animusd --lib sim_cluster --
--test-threads=2`, whole tier): 317 (PR 2 baseline) → 337 (PR 3, +20) →
353 (PR 4, +16) → 367 (PR 5, +14) → 383 (PR 6, +16) → **406 passed, 0
failed, 2 ignored** (#777, +23, exactly as predicted; 752.46s;
anchored-sampler RSS first ~99 MB, peak ~816 MB, last ~133 MB, well under
the 6 GB/30 min budget and consistent with every prior rung's own no-leak
trajectory). This is a real, confirmed gate run: #777's own Phase B
rebased onto PR 6's landed commit and ran every gate for real, in the
main tree — `cargo build -p animusd --all-targets` (clean, 1m55s);
`dashboard_endpoint` untrimmed (16 passed) → `sim_cluster_dashboard` (23
passed) → `dashboard_endpoint` trimmed (4 passed); the whole-tier run
above; `cargo fmt --all --check` (one pass needed, then clean); `cargo
clippy -p animusd --all-targets --all-features -- -D warnings` (clean).
`Cargo.lock` unchanged. No product or scenario-authoring bug was found —
see `crates/animusd/CLAUDE.md`'s matching PR 7 appendix and this file's
"Rung H, PR 7 landed" amendment for the full gate account.

**What remains unowned after C-08**, updating the C-07-closing residual
inventory (`crates/animusd/CLAUDE.md`'s Tests section) now that admin/
console/dashboard HTTP is no longer on it: TTL, the control/data role
split, `--config` bring-up, index DDL beyond plain `CreateTable`, node
assembly/raw `ClientRequest`, and the throttle-metric counters. Six
groups remain, none with a rung against it today. Naming exactly which of
this rung's own 25 kept `ProdEnv` tests each residual group would unlock,
where one is named at all (some of this rung's own kept tests are blocked
by a narrower, not-yet-formally-tracked `SimCluster` fixture gap instead —
listed separately below, not forced into one of the six):

- **TTL** (a `ttl_reaper_loop` driver under `SimEnv`) would unlock 2:
  `console_stream.rs::ttl_deletion_carries_the_service_user_identity_
  through_the_console` and `admin_endpoint.rs::admin_ttl_reports_reaper_
  progress_and_ttl_tables` — the reaper-progress and TTL-identity tests.
- **Control/data role split** (a genuine per-node role concept under
  `SimCluster`) would unlock 4: `console_endpoint.rs::console_serves_
  shell_on_data_only_node`, `console_endpoint.rs::console_addr_panics_on_
  control_only_node`, `dashboard_endpoint.rs::dashboard_role_gating_split_
  deployment`, `dashboard_endpoint.rs::control_node_streams_read_path_is_
  ground_truth` — the console/dashboard control-only/data-only tests.
  (`console_serves_shell_assets_and_deep_links_on_combined_node` and
  `dashboard_serves_spa_with_cors_and_peers` are real-HTTP-framing tests,
  class (C) and permanent regardless — a role-split primitive would not
  unlock them.)
- **Index DDL beyond plain `CreateTable`** (the GSI backfill machinery
  generalized, blocker (d)) would unlock 3: `console_table_config.rs`'s
  `add_and_drop_gsi_round_trip`/`add_gsi_records_a_declared_attribute_
  type`/`add_gsi_rejects_an_unknown_attribute_type`. (The same file's
  `table_detail_shows_pitr_status_and_backups` needs a separate
  `UpdateContinuousBackups` dispatch arm instead — the PITR/backups
  finding above, not this residual.)
- **`--config` bring-up, node assembly/raw `ClientRequest`, and the
  throttle-metric counters** unlock none of this rung's own kept tests —
  no test in the final residue table above is blocked by any of these
  three; they remain exactly as unowned, and exactly as sized, as rung G
  left them.

**Capability gaps this rung's own kept tests surfaced that are not one of
the six tracked groups above** — narrower `SimCluster` fixture-
construction gaps, each blocking a small, specific set of tests rather
than a whole residual class:

- **The ADR 0038 `DRIVER_APPLIED` control-storage mirror** (`ctx.
  control_storage`, always `None` under `SimCluster` — no per-node apply-
  task mirror engine driven under `SimEnv`) blocks 4: both of `system_
  table.rs`'s tests, `admin_endpoint.rs::admin_system_table_split_
  lineage_after_a_real_split`, and `dashboard_endpoint.rs::dashboard_u05_
  lineage_panel`.
- **`MemoryEngine` has no LSM/flush/compact/SSTable concept** (this
  fixture's engine is always `MemoryEngine`, never `LsmEngine`) blocks 3:
  `admin_endpoint.rs::admin_raftkv_default_does_not_materialize_the_
  dataset`, `admin_endpoint.rs::admin_interface_surfaces_state_and_
  actions` (its `/admin/storage/flush` action), and `admin_endpoint.
  rs::admin_storage_compact_action` — the storage action tests.
- **A `ClusterSegmentStore`/`fs`-kind segment-store primitive** (this
  fixture's shared segment store is always `S3`-kind) blocks 2:
  `admin_segment_store_reports_shard_placement_and_local_objects`/
  `admin_segment_store_reports_null_shards_for_the_fs_kind`.
- **A `dynamo_auth` constructor knob** blocks 1: `admin_config_reports_
  auth_state_and_never_serves_the_secret`.
- **A never-completes-bootstrap node constructor** blocks 1: `admin_live_
  is_200_while_a_genuinely_leaderless_admin_health_is_503`.
- **Real HTTP framing/CORS** (class C, permanent — no `SimCluster`
  primitive builds framing at all, by design) and **real-thread election
  timing** (class A, permanent) together account for the remaining 5
  kept tests in the table above and will never convert, regardless of any
  future rung.

None of these five narrower gaps is sized or claimed by any planned rung
today; a future rung wanting one of them should scope it directly against
the specific tests named above rather than assuming it falls inside one
of the six tracked groups.

**Recommendation for the next rung ("C-09"), advisory only — not a
decision.** Two candidates stand out. **TTL** is small (one background-
loop driver, `ttl_reaper_loop`, under `SimEnv` — the same "spawn it
unconditionally on every `SimCluster` node, cover it under the existing
`Drop`/`restart` shutdown path" shape the D4 PR 5 backup-janitor and rung
G PR 5 segment-janitor drivers already validated twice) and unlocks tests
spread across two already-closed rungs at once (C-07's own `console_
stream.rs` TTL-identity test plus this rung's `admin_ttl_reports_reaper_
progress_and_ttl_tables`) — a rung that pays off work already done, not
just new ground. **Index DDL beyond plain `CreateTable`** is the largest
remaining group (9/30 tests in the D3-closing class-D breakdown) and
would need the GSI backfill machinery itself generalized, not merely a
dispatch arm — genuinely more mechanism than any rung since D3 PR 3b's
own GSI-drain work — and three of its own files
(`dynamo_index_scan.rs`/`index_backfill.rs`/`dynamo_index_writes.rs`, per
this rung's own "off-limits" list) are frozen behind the open flake
issues (#298, #418, #592, #601, #610) class (E) names, which would need
resolving or working around before a rung there could even establish its
own untrimmed-suite baseline safely. On balance this recommends **TTL**
first: smaller, self-contained, immediately unlocks kept tests from two
rungs already closed, and does not depend on unblocking someone else's
open flake investigation first — but this is the maintainer's call, not
one this docs-only PR makes.

**Website: no change needed, verified again at this close.** A targeted
grep of `website/*.html` and `website/articles/*.html` for admin/console/
dashboard/simulation/test-coverage claims found only high-level,
wire-level, or general deterministic-simulation-story language (e.g.
`website/architecture.html`'s/`how-it-works.html`'s simulator
descriptions, `website/licence.html`'s "admin API or its consoles" AGPL
scoping paragraph) — none names admin/console/dashboard `SimCluster`
testing specifically as a gap this rung needed to close, and every
production dispatch path this rung touched stays byte-identical per its
own opener's non-goals, the same reasoning every prior rung's opener and
close-out already applied.

**Gates**: none — documentation only, no `cargo` command run, `git diff
--stat` shows only the docs files this PR touches.

**Docs**: this amendment (closing Rung H); `docs/roadmap.md`'s C-08 entry
closed, its wave-9 sequencing row marked closed, and the C-07 entry's own
"what remains unowned" sentence re-pointed; `crates/animusd/CLAUDE.md`'s
residual-inventory paragraph updated and a "C-08 closed" appendix added
after the PR 7 appendix; `crates/animus-node/CLAUDE.md`'s dated PR 1
correction re-pointed at this closed rung; `docs/engineering-lessons.md`
already carries every finding this rung's own PRs made — no new entry
needed at this close.

## 2026-09-08 amendment — Rung I (post-C-08): TTL reaper under `SimCluster` (C-09), PR 1 (this docs-only opener)

**The maintainer approved the C-08 close-out's recommendation on
2026-09-08.** Rung H's own close-out named six groups left unowned after
C-08 (TTL, the control/data role split, `--config` bring-up, index DDL
beyond plain `CreateTable`, node assembly/raw `ClientRequest`,
throttle-metric counters) and recommended TTL first, "but this is the
maintainer's call, not one this docs-only PR makes." This amendment is
that recommendation turned into a concrete, grep-verified plan — tracked
in `docs/roadmap.md` as **C-09** — and on 2026-09-08 the maintainer
approved it, sequencing index DDL beyond plain `CreateTable` as the rung
after this one (the C-10 candidate, not yet planned). PR 1 (docs only) is
built; PRs 2-5 below follow.

**Goal.** Make the DynamoDB-style TTL reaper (ADR 0051) — currently
provable only over `ProdEnv`'s real wall clock via `tests/dynamo_ttl.rs`'s
9 real-socket tests, plus one residue test apiece in `admin_endpoint.rs`
and `console_stream.rs` — deterministically fault-injectable and
seed-replayable under `SimCluster`/`SimEnv`, closing the smallest of the
six residual groups Rung H left behind.

**Ground truth**, from a read-only grep of the current tree, not a plan
expectation:

- `crates/animus-node/src/ttl_reaper.rs:188`'s `pub async fn
  ttl_reaper_loop<E, H>(env: E, host: H, interval: Duration) where E: Env,
  H: TtlScanHost + TtlReaperProgressHost` is **already fully generic**
  (rung C2) — reads `TtlSpec` via `host.ttl_metadata()`, compares against
  `env.wall_now()` (ADR 0051 §1), deletes through
  `host.ttl_delete_if_attribute_equals` → `dynamo::
  kind_write_item_at_leader` (ADR 0049's kind-write path, so index/stream/
  change-log maintenance is inherited, never reimplemented), and publishes
  a `TtlReaperProgress` per phase transition. `crates/animus-node/src/
  host.rs:121`'s `TtlScanHost` and `:185`'s `TtlReaperProgressHost` are the
  two traits it is generic over.
- `crates/animusd/src/ttl_reaper.rs` is a **concrete** (`ClientCtx`
  default type params) thin delegation, its entire body: `pub(crate) async
  fn ttl_reaper_loop(ctx: crate::ClientCtx, interval: Duration) { let env =
  ctx.env.clone(); animus_node::ttl_reaper::ttl_reaper_loop(env, ctx,
  interval).await; }`. This is the cheapest widening shape in the whole
  series (the D2/D3/D4/G free-function precedent), not the riskier
  trait-impl-on-`ClientCtx` shape C-08 PR 2's own same-day correction
  warns against (see that PR's `GenericAdminHost`/`GenericConsoleBackend`
  newtype fix).
- `crates/animusd/src/client_ctx_host.rs:106`'s `impl<E: Env, R:
  RelayClient> TtlScanHost for ClientCtx<E, R>` is **already generic**
  (D4 PR 5's own widening pass). `client_ctx_host.rs:98`'s `impl
  TtlReaperProgressHost for ClientCtx` (bare defaults — `ClientCtx<E:
  Env = ProdEnv, R: RelayClient = AnimusdRelayClient>`, `lib.rs:9710`) is
  the **one remaining concrete impl** in this whole surface — a two-line
  mutex-update body, safe to widen the same low-risk way
  `BackupJanitorProgressHost` already was, immediately above it in the
  same file.
- `crates/animus-sim/src/lib.rs:1418`'s `Clock::wall_now` — `UnixMillis(
  SIM_WALL_EPOCH_MS.saturating_add(self.now().0 / 1_000_000))`
  (`SIM_WALL_EPOCH_MS = 1_577_836_800_000`, line 1875) — is a **pure
  function of virtual time**: a `PutItem` at `wall_now_secs + delta`, then
  `run_for`/`drive_*` advancing virtual time past `delta`, deterministically
  expires an item with no real-clock dependency at all. This is the same
  "wall-clock seam: none" finding every TTL-touching test in this codebase
  already relies on.
- Production's own default sweep cadence is `DEFAULT_TTL_SWEEP_INTERVAL =
  Duration::from_secs(60)` (`ttl_reaper.rs:90`), unchanged by this rung —
  `crates/animusd/src/lib.rs`'s ~10 production spawn/construction sites all
  keep passing it verbatim to the (widened) wrapper. The segment janitor
  (C-07 PR 5, `crates/animusd/src/segment_janitor.rs:169`) and the backup
  janitor (D4 PR 5, `crates/animus-node/src/backup_janitor.rs:54`) both
  already spawn unconditionally in `SimCluster::new`/`::restart` at a
  hardcoded `Duration::from_millis(200)` sim interval, not gated behind an
  opt-in the way `auto_split_loop` is — the direct precedent for this
  rung's own always-on TTL spawn.
- The TTL reaper self-gates purely per-tablet: `client_ctx_host.rs`'s
  `TtlScanHost::led_tablets` filters `self.edge.hosted_groups()` on
  `group.is_leader()` — the identical primitive `segment_janitor`/
  `drain_gsi` already use, with **no control-plane-leader dependency** —
  the same low-risk "leader gating" shape every prior always-on loop in
  this series already has.
- `SimCluster`'s `Drop` (issue #753) already breaks every node's relay
  self-cycle and drains `self.sim` (every perpetual task, TTL's new one
  included) unconditionally — `sim_cluster.rs:1214`'s `impl Drop for
  SimCluster` needs **no new code** for this rung's spawn, the same "no
  new Drop-coverage obligation" finding Rung H's own opener already made
  for its own new (non-loop) primitives, restated here because this rung's
  primitive genuinely *is* a new perpetual loop, unlike Rung H's.

**Tests unlocked (11 total)**, verified by reading each test body and its
surrounding doc comment, not assumed from the file's test count:

| Test | File | Reaper-driven? |
|---|---|---|
| `update_time_to_live_enable_and_disable_round_trip` | `dynamo_ttl.rs` | No — pure DDL (`UpdateTimeToLive`/`DescribeTimeToLive`, already reachable via `dispatch_item_op`, C-08 PR 2) |
| `disable_with_a_mismatched_attribute_name_is_rejected` | `dynamo_ttl.rs` | No — pure DDL |
| `expired_item_is_still_readable_immediately` | `dynamo_ttl.rs` | No — needs `wall_now` past expiry only, not the loop ticking (proves AWS-faithful "expired but not yet reaped" visibility) |
| `expired_item_is_eventually_reaped` | `dynamo_ttl.rs` | Yes |
| `future_ttl_item_is_never_deleted` | `dynamo_ttl.rs` | Yes |
| `wrong_type_ttl_attribute_is_never_deleted` | `dynamo_ttl.rs` | Yes |
| `absurdly_past_ttl_is_never_deleted_the_five_year_safety_window` | `dynamo_ttl.rs` | Yes |
| `refreshed_ttl_survives_the_reaper` | `dynamo_ttl.rs` | Yes |
| `ttl_deletion_is_visible_in_the_stream_with_a_service_user_identity` | `dynamo_ttl.rs` | Yes |
| `admin_ttl_reports_reaper_progress_and_ttl_tables` | `admin_endpoint.rs` | Yes for its reaper-progress half; the "tables" half (`UpdateTimeToLive` + `GET /admin/ttl` convergence) is **already** covered by `sim_cluster_admin.rs::ttl_tables_lists_a_ttl_enabled_table` (C-08 PR 5) — this rung only needs to add the reaper-progress half |
| `ttl_deletion_carries_the_service_user_identity_through_the_console` | `console_stream.rs` | Yes |

2 pure-DDL + 1 wall-now-only + 6 reaper-driven (`dynamo_ttl.rs`) + 2
reaper-driven residue tests (`admin_endpoint.rs`, `console_stream.rs`) =
11. Both residue tests' own doc comments already say, verbatim, why they
stay `ProdEnv` today: "no primitive drives `animusd::ttl_reaper::
ttl_reaper_loop` under `SimEnv`" — exactly the gap this rung closes.
**No dashboard residue**: `dashboard_u07_ttl_reaper_card` already
converted in C-08 PR 7 (route/card rendering only, no reaper behavior
asserted). **Expected residue after this rung: none** — the first rung in
this whole sequence (F through I) with a clean sweep of its own named
group.

**The PR series** (5 PRs, stacked on the C-08 close-out — the smallest
rung yet, no new capability trait/newtype/store concept, since the reaper
loop is already generic):

- **PR 1 (this amendment).** Docs only: this ADR amendment, the C-09
  roadmap entry (and wave-10 sequencing row), this file's own rung-table
  row, and `crates/animusd/CLAUDE.md`'s residual-inventory pointer.
  **Gates:** none — documentation only, no `cargo` command run.
- **PR 2 — Groundwork.** Widen `impl TtlReaperProgressHost for ClientCtx`
  to `<E: Env, R: RelayClient>` (mirrors `BackupJanitorProgressHost`
  immediately above it). Widen `crates/animusd/src/ttl_reaper.rs::
  ttl_reaper_loop` to `<E: Env, R: RelayClient>` — every production spawn
  site keeps passing a bare `ClientCtx` + `DEFAULT_TTL_SWEEP_INTERVAL`
  unchanged, so `#[deny(clippy::disallowed_methods)]` on `lib.rs`'s five
  client-path modules stays satisfied and every call site stays
  byte-identical. In `sim_cluster.rs`: add `SIM_TTL_SWEEP_INTERVAL =
  Duration::from_millis(200)`, spawn `ttl_reaper::ttl_reaper_loop`
  unconditionally per node in both `SimCluster::new` and `::restart`,
  beside the segment-janitor/backup-janitor spawns (identical shape, same
  comment pattern); add `SimCluster::drive_ttl_sweep(node)` mirroring
  `drive_stream_seal` — a convenience for precisely-timed sweep assertions,
  not a strict necessity given `OP_BUDGET` (12s) is 60× the 200ms
  interval. **Gate:** untrimmed `cargo test -p animusd --test dynamo_ttl`
  (9 passed) first, to confirm nothing about the new always-on spawn
  perturbs the existing real-socket suite; then `cargo test -p animusd
  --lib`; `cargo clippy -p animusd --all-targets --all-features -- -D
  warnings`; `cargo fmt --all --check`.
- **PR 3 — `sim_cluster_ttl.rs`.** Converts `dynamo_ttl.rs`'s 9 tests into
  ~9 scenarios (`_over_seeds` ×5 ≈ 18 tests): wire `CreateTable`/
  `UpdateTimeToLive`, writes from a non-leader, virtual time advanced past
  expiry, reap/no-reap asserted with `ConsistentRead: true`; the
  stream-identity scenario reuses C-07 PR 3's `get_records` primitives.
  Delete `dynamo_ttl.rs` whole once its untrimmed run and the new sim
  suite both stay green — nothing in it needs `ProdEnv`. **Gate:** the sim
  suite green before the trim; a clean `cargo test -p animusd --lib`
  after.
- **PR 4 — Admin/console residue.** Add the reaper-progress half to a
  `sim_cluster_admin.rs` scenario asserting both the tables-list and the
  reaper-progress reports together (or extend the existing
  `ttl_tables_lists_a_ttl_enabled_table` scenario, whichever reads
  cleaner once written); delete `admin_endpoint.rs::admin_ttl_reports_
  reaper_progress_and_ttl_tables`. Add a scenario to `sim_cluster_
  console_stream.rs` via `GenericConsoleBackend`; delete `console_
  stream.rs::ttl_deletion_carries_the_service_user_identity_through_the_
  console`. **Gate:** untrimmed `admin_endpoint`/`console_stream` before,
  trimmed after, `cargo test -p animusd --lib` in between.
- **PR 5 — Docs close-out.** This rung marked "closed," a "Rung I closed"
  amendment with the PR-by-PR account, before/after `sim_cluster` test
  counts, the residue table (expected **none**, the first clean sweep in
  the F-through-I sequence), the gate transcript, and `docs/roadmap.md`/
  `crates/animusd/CLAUDE.md` updated to match — the same shape every prior
  rung's own PR 8/6/5/7 close-out already used. **Gates:** none —
  documentation only.

**Gates, in order, for each test-touching PR** (PR 2-4): untrimmed
file(s) green → `cargo test -p animusd --lib sim_cluster_ttl --
--test-threads=2` green → trim → re-run the untrimmed file(s) (until
deleted) → `cargo test -p animusd --lib sim_cluster -- --test-threads=2`
(whole tier, ~382 tests expected going in, growing by this rung's own
conversions; RSS via the anchored sampler — `pgrep -f
'^/home/user/animus-db/target/debug/deps/animusd-'`, per the #753
postmortem, never an unanchored substring match — watched for the same
no-leak shape every prior rung confirmed) → `cargo fmt --all --check` →
`cargo clippy -p animusd --all-targets --all-features -- -D warnings`.

**Binding rules restated.** Production paths stay byte-identical
throughout: a free function plus a trivial trait impl widened on
`ClientCtx<E, R>`'s own generic defaults — no `Generic*` newtype needed
here, unlike C-08 PR 2's `GenericAdminHost`/`GenericConsoleBackend`,
because neither `ttl_reaper_loop` nor `TtlReaperProgressHost` is ever
called through a `dyn`/trait-object production dispatch table the way
`AdminHost`/`ConsoleBackend` are — both stay concrete free functions/impls
callable exactly as they are today, so widening their type parameters in
place cannot silently narrow any production caller (the D2 PR 1 lesson
does not engage here). No `HashMap`/`tokio::time` may appear in the
now-generic loop body (it already has none — `ttl_reaper_loop` is
`animus-node` code, subject to the workspace-wide
`disallowed-methods`/`disallowed-types` lints, not `animusd`'s
package-level carve-out). `OP_BUDGET` (12s) vs the new 200ms sim interval:
a plain op call's fixed budget clears 60 sweep ticks, so any assertion
that depends on catching an *intermediate* sweep state (mid-cursor,
one-tick-before-reap) must use `drive_ttl_sweep`/a short `run_for`, never
an ordinary op call — the same `OP_BUDGET`-vs-short-interval caution
Rung G's own close-out recorded for its short-retention segment-janitor
scenario. Off-limits files, confirmed out of scope by this investigation
and untouched by this series: `streams_e2e.rs`, `dynamo_index_scan.rs`,
`index_backfill.rs`, `batch_write.rs`, `dynamo_index_writes.rs`,
`split_placing_two_replica_diff_e2e.rs`, `cp_cross_process.rs`,
`cp_txn.rs`, `dynamo_txn_idempotency.rs`, `shared_wal_liveness.rs`. Only
`dynamo_ttl.rs`, `admin_endpoint.rs`, `console_stream.rs`, `ttl_reaper.rs`,
`client_ctx_host.rs`, `sim_cluster.rs`, and the new `sim_cluster_ttl.rs`
are touched across PRs 2-4.

**Risks.** Wall-clock seam: none — `wall_now()` is a pure function of
virtual time; the only real risk is a scenario author reasoning in real
seconds instead of the sim's own virtual clock, the same caution every
prior TTL-touching test in this codebase already carries. Leader gating:
low — data-tablet leadership via `hosted_groups()`, the same primitive
`segment_janitor`/`drain_gsi` already use, with no control-plane
dependency to fake. Real LSM state: none — `scan_base_capped` is the same
generic engine scan every already-converted path already proves; nothing
TTL-specific touches storage beyond it. New-loop Drop coverage: covered by
#753's existing contract, confirmed above — the one genuine "is this
covered" question this rung's own primitive raises, and it resolves
without new code.

**Alternative considered: index DDL beyond plain `CreateTable`** (the
other blocker Rung H's own close-out named, "(d)"). It would unlock 9/30
of the class-D residual (3 `console_table_config.rs` tests — add/drop-GSI,
declared-attribute-type, reject-unknown-type — plus `dynamo_index_
scan.rs`/`index_backfill.rs`/`dynamo_index_writes.rs`, the last three
frozen behind the open flake issues #298/#418/#592/#601/#610/#619/#622/
#627). It needs the GSI backfill machinery itself generalized — genuinely
more new mechanism than any rung since D3 PR 3b's own GSI-drain work — and
is gated on someone else's open flake investigation before even an
untrimmed baseline could be established safely. TTL pays off two
already-closed rungs (its own tests plus the `console_stream.rs`/
`admin_endpoint.rs` residue those rungs left behind) for near-zero new
risk, with no capability gap and no open external blocker; index DDL is
the larger rung to take on once those flake issues resolve. This is why
this amendment recommends TTL first, per Rung H's own advisory note above
— but, again, the choice is the maintainer's, not this PR's.

**Website:** no change needed — this rung touches no wire-observable
behavior; every production dispatch path stays byte-identical per this
amendment's own binding rules, the same reasoning every prior rung's
opener already applied.

**Docs:** this amendment (opening Rung I, open); `docs/roadmap.md`'s
new C-09 entry and its wave-10 sequencing row; `crates/animusd/
CLAUDE.md`'s residual-inventory `TTL (1/9)` mention annotated with a
pointer to this open rung. **Gates:** none — documentation only, no
`cargo` command run, `git diff --stat` shows only the three docs files
this PR touches.

## 2026-09-08 amendment — Rung I, PR 2 landed (groundwork)

PR 2's own scope — the two concrete surfaces this rung's opener named,
plus `SimCluster`'s always-on spawn and a first smoke pair — landed, with
no product bug found.

`client_ctx_host.rs`'s `impl TtlReaperProgressHost for ClientCtx` widened
from the bare concrete alias to `impl<E: Env, R: RelayClient>
TtlReaperProgressHost for ClientCtx<E, R>`, mirroring
`BackupJanitorProgressHost` immediately above it in the same file
(`TtlScanHost` was already generic there, D4 PR 5) — a pure signature
widening, since `self.ttl_reaper_progress` is already `E`/`R`-agnostic (a
plain `Arc<Mutex<..>>`). `animusd::ttl_reaper::ttl_reaper_loop`'s thin
wrapper widened the same way, to `<E: Env, R: RelayClient>`. Every
production spawn site (`lib.rs`'s five `tokio::spawn(ttl_reaper::
ttl_reaper_loop(ctx.clone(), ..))` calls) passes a bare `ctx.clone()` — a
`ClientCtx` with no explicit type arguments — so all five keep inferring
`E = ProdEnv, R = AnimusdRelayClient` from `ClientCtx`'s own
definition-site default with zero call-site changes: confirmed, not
assumed, by `tests/dynamo_ttl.rs`'s untrimmed 9-test suite staying green
both before and after this widening (5.22s and 5.28s respectively).

`animus_node::ttl_reaper::ttl_sweep_one_tablet` widened from private to
`pub` — a pure visibility change, no behavior change (`TtlReaperPhase`/
`TtlReaperProgress`/`DEFAULT_TTL_SWEEP_INTERVAL` were already `pub`) — so
`SimCluster::drive_ttl_sweep` can drive the exact per-tablet sweep
function the always-on loop already calls every tick, rather than
reimplementing the scan/expire/delete control flow a second time.
`animus_node::host::{TtlScanHost, TtlReaperProgressHost}` themselves
needed no change.

**No `tokio::time`-body gap found this time** — unlike C-07 PR 2's
`index_drain::seal_now` and C-08 PR 6's `ClientCtx::
admin_transfer_control_leadership`, both of which had an already-generic
*signature* concealing a body still reading the real clock, `ttl_reaper_
loop`/`ttl_sweep_one_tablet` already read `env.wall_now()`/`env.sleep()`/
`env.now()` exclusively (ADR 0051's TTL feature is the one place
`wall_now()` exists for). Confirmed by actually running the loop under
`SimEnv` for the first time (this PR's own scenario (a)), not just by
reading the source — the lesson those two prior findings established
(a function's generic type parameters prove nothing about its body) held
again as a *negative* result here, worth stating so a future rung doesn't
re-derive the same caution from scratch.

`sim_cluster.rs`: `SIM_TTL_SWEEP_INTERVAL` (200ms) joins the janitors' own
interval constants; `ttl_reaper_loop` is spawned unconditionally on every
node in both `SimCluster::new` and `::restart`, mirroring the backup/
segment janitors' own always-on spawns exactly — this loop's own
per-tablet leader gate (`TtlScanHost::led_tablets`) already makes a node
leading nothing this tick a cheap idle sleep, so there is no reason to
gate the spawn behind an opt-in the way `auto_split_loop` is. Two new
accessors: `drive_ttl_sweep(node)` runs one full sweep to exhaustion over
`node`'s own led tablets through `ttl_sweep_one_tablet`, with its own
driver-local cursor entirely separate from the always-on loop's own — for
a scenario that must assert an intermediate, pre-cadence reaper state
without waiting out the 200ms cadence (not a strict necessity given
`OP_BUDGET`, 12s, is 60x the sim interval, but useful for a single
deterministic sweep with no leftover cadence noise); `ttl_reaper_progress
(node)` mirrors `backup_janitor_progress`/`segment_janitor_progress`
exactly (a plain lock/clone/drop, never held across an `.await`).

New module: `sim_cluster_ttl.rs`, two pinned-seed smoke tests — (a) a
`PutItem` with a TTL attribute a few virtual seconds in the past is
reaped by the always-on loop within one `run_for` past
`SIM_TTL_SWEEP_INTERVAL`, read back absent with `ConsistentRead: true`
(ADR 0055), with the reaping node's own `TtlReaperProgress.deleted_total`
confirmed `>= 1`; (b) an item with a future expiry survives a
`drive_ttl_sweep` call on its own tablet leader. Both issue `PutItem`/
`GetItem` from a **non-leader** of the table's tablet, mirroring every
`sim_cluster_dynamo_*` sibling's own forwarding-path convention.
`CreateTable`/`UpdateTimeToLive` are issued from node 0 (a schema-catalog
mutation, no tablet leader to route around). PR 3 extends this module
with the remainder of `tests/dynamo_ttl.rs`'s own 9 scenarios.

**Gates, in the required order**: `cargo test -p animusd --test
dynamo_ttl` on the untouched file (9 passed, 5.22s), then again after
this PR's changes (9 passed, 5.28s); `cargo build -p animusd
--all-targets` (clean, no warnings — the checkpoint push happened here,
before the longer gates below); `cargo test -p animusd --lib
sim_cluster_ttl -- --test-threads=2` (2 passed, 1.01s); `cargo test -p
animusd --lib sim_cluster -- --test-threads=2` (408 passed, 0 failed, 2
ignored, 1078.10s — 406 baseline + this PR's 2 new tests; RSS via the
anchored sampler, `pgrep -f '^/home/user/animus-db/target/debug/deps/
animusd-'`, every 10s: first ~70 MB, peak ~861 MB, last ~119 MB,
consistent with every prior rung's own no-leak trajectory — the always-on
reaper adds a per-scenario cost but no growth trend); `cargo test -p
animus-node ttl` (1 passed via substring match, plus a full run of
`--test ttl_reaper_sim` confirming both of that file's tests still pass —
its own test names don't happen to contain the literal substring "ttl",
so the substring filter alone undercounts it); `cargo fmt --all --check`
(one reformat needed — three long call/format-string lines in the new
`sim_cluster_ttl.rs` plus one method signature in `sim_cluster.rs`,
applied via `cargo fmt --all`, then clean); `cargo clippy -p animusd
--all-targets --all-features -- -D warnings` (clean). `Cargo.lock`
unchanged. Production spawn sites (`lib.rs`) and `animus-node`'s own
module map (`host.rs`, `wire.rs`, everything but `ttl_reaper.rs`'s one
visibility change) are untouched.

See `crates/animusd/CLAUDE.md`'s matching "C-09 PR 2" appendix and
`docs/roadmap.md`'s C-09 entry for the crate-level pointer and status.

## 2026-09-08 amendment — Rung I, PR 3 (`sim_cluster_ttl.rs` extended)

PR 3's own scope — converting the remaining 8 of `tests/dynamo_ttl.rs`'s 9
tests — is 5/8 done: `sim_cluster_ttl.rs` gained 5 new scenarios (e)-(i)
(the three always-on-loop "never deleted" negatives — future expiry,
wrong-type attribute, the five-year safety window; the refreshed-TTL-
survives outcome; and the stream `userIdentity` scenario, reusing C-07
PR 3's wire shapes as module-private local copies since the originals
aren't exported across modules), plus the `_over_seeds` sibling PR 2's own
two scenarios had not yet gained. **Two more scenarios were attempted,
written, then reverted** — see the next paragraph. See
`crates/animusd/CLAUDE.md`'s matching "C-09 PR 3" appendix for the full
9-test mapping table.

**Three tests do not convert, and this amendment corrects PR 1's own "no
residue expected" plan text.** `expired_item_is_still_readable_
immediately` stays in a trimmed `tests/dynamo_ttl.rs` for the reason PR 1's
own plan text failed to anticipate: every `SimCluster` wire call's
`spawn_and_capture` drives `self.sim.run_for(OP_BUDGET)` (12s)
unconditionally, and `animus_sim::Simulator::run_until` always drains every
scheduled event up to that deadline before returning — it does not stop
early once the awaited future itself resolves. With the always-on TTL
reaper ticking every `SIM_TTL_SWEEP_INTERVAL` (200ms), a single wire call
already spans 60 sweep opportunities, so by the time a `PutItem` writing an
already-expired attribute returns, the reaper has already had dozens of
chances to reap it — there is no way to issue a subsequent `GetItem` and
reliably observe the pre-reap state. This is the mirror image of the
caution PR 1's plan text stated ("any assertion that depends on catching an
intermediate sweep state... must use `drive_ttl_sweep`/a short `run_for`,
never an ordinary op call"): that caution protects an assertion that needs
to *force* an intermediate state, not one that needs to *suppress* one, and
no "hold the reaper back between two wire calls" primitive exists
(`drive_ttl_sweep` only ever adds sweeps, on top of whatever the always-on
loop already ran). The refreshed-TTL scenario (h) looked similarly fragile
at first read but isn't: the original real-socket test's own doc already
scopes its claim to the *observable outcome* (item present, correct
refreshed value), not the tighter scan-vs-propose timing (explicitly
disclaimed as covered elsewhere, by `animus-cp-data`'s own OCC seatbelt
tests) — and DynamoDB's own upsert-on-missing `UpdateItem` semantics mean
that outcome holds whether the reaper's conditional delete was skipped or
the item was deleted then recreated, so it converts cleanly despite the
same `OP_BUDGET` granularity.

`update_time_to_live_enable_and_disable_round_trip` and `disable_with_a_
mismatched_attribute_name_is_rejected` (the original suite's own (c) and
(d)) were written as `sim_cluster_ttl.rs` scenarios but reverted once
actually run: both depend on `DescribeTimeToLive`, and `dynamo::
dispatch_item_op` — the generic (`SimEnv`-capable) dispatch path
`SimClusterHandle::dynamo` calls through — has no arm for it. Every
`DescribeTimeToLive` call under `SimCluster` fails with a `500`
("this operation is not yet supported by the generic (SimEnv-capable)
dispatch path"), even though its write-side sibling, `UpdateTimeToLive`,
was widened onto this exact path in ADR 0061 rung H (C-08 PR 2) — the two
operations are asymmetric in what's already wired. Closing this needs a
`crates/animusd/src/dynamo.rs` change: widening `describe_time_to_live` to
`<E: Env, R: RelayClient>` (it already takes `_ctx: &ClientCtx` unused, so
this is a pure signature change) and adding an `Operation::
DescribeTimeToLive` arm to `dispatch_item_op`, mirroring `UpdateTimeToLive`
's own precedent exactly — both scenarios (and their `describe_ttl_via_wire`
helper) were reverted out of `sim_cluster_ttl.rs` rather than landed against
a 500, and both original tests were kept in `tests/dynamo_ttl.rs` with this
reasoning stated inline. A future PR that widens `dynamo.rs` this way can
convert both in a few lines.

**One deviation from this rung's own template, recorded rather than
silently applied**: no `ANIMUS_TTL_SEEDS` env var was added, and
`corpus-deep.yml` was left untouched. PR 3's own task brief asked for both,
modeled on the two dedicated multi-cell corpus files
(`sim_cluster_corpus.rs`/`sim_cluster_dynamo_corpus.rs`, the only two
`sim_cluster*` files `corpus-deep.yml` actually wires up). Grepping every
other converted-suite `sim_cluster_*.rs` module in the crate
(`sim_cluster_console_stream.rs`, `sim_cluster_backup_janitor.rs`,
`sim_cluster_dynamo_streams.rs`, and the rest) shows their own `_over_seeds`
siblings all use a fixed `for i in 0..5 { .. }` loop with no env var at
all — an env-var depth knob is a corpus-file-only convention, not a
per-scenario-module one. `sim_cluster_ttl.rs` follows the verified
majority (fixed 5-seed loop) instead of inventing an unused knob, per this
codebase's own "grep the code before implementing a documented gap" habit
(root `CLAUDE.md`'s Engineering practices section) — the same habit this
correction itself is an instance of.

**Gates**: run and green. `cargo fmt --all --check`, `cargo clippy -p
animusd --all-targets --all-features -- -D warnings`, and `cargo build -p
animusd --tests` all clean; `cargo test -p animusd --lib sim_cluster_ttl --
--test-threads=2` (14 passed, 0 failed); the trimmed `cargo test -p animusd
--test dynamo_ttl` (3 passed, 0 failed — down from 9, up from the originally
planned 1, per the two reverted scenarios above); `cargo test -p animusd
--lib sim_cluster -- --test-threads=2` (**420 passed, 0 failed, 2 ignored**,
1113.93s test time — PR 2's own 408-test baseline plus this PR's net 12 new
`#[test]` functions: 16 written (7 new scenarios × 2 each, pinned +
`_over_seeds`, plus the 2 `_over_seeds` added to PR 2's existing (a)/(b))
minus the 4 reverted with scenarios (c)/(d); peak resident memory 962204 kB
(~940 MB) per `/usr/bin/time -v`'s "Maximum resident set size", in line with
every prior rung's own no-leak trajectory). `Cargo.lock` unchanged.

See `crates/animusd/CLAUDE.md`'s matching "C-09 PR 3" appendix and
`docs/roadmap.md`'s C-09 entry for the crate-level pointer and status.

## 2026-09-09 amendment — Rung I, PR 4 (admin/console residue)

PR 4's own scope — this rung's plan's own "reaper-progress half" and
"console residue" items — landed, no product bug found, no `sim_cluster.rs`/
`admin.rs`/`console.rs`/`dynamo.rs` change needed (the same "pure test
authorship" shape PR 3 already established).

**`sim_cluster_admin.rs`** gains a new scenario (8),
`admin_ttl_reports_reaper_progress_and_ttl_tables` — the exact name of the
real-socket original it replaces, per this rung's own plan text ("delete
`admin_endpoint.rs::admin_ttl_reports_reaper_progress_and_ttl_tables`"),
rather than folding its reaper-progress half into scenario (6)'s
`ttl_tables_lists_a_ttl_enabled_table` (the plan's own "whichever reads
cleaner" alternative) — keeping them separate means scenario (6) stays the
narrower "tables projection only" regression it already was, and scenario
(8) is the literal one-to-one conversion, easier to audit against its
real-socket original line for line. Same shape as that original: `Create
Table` + `UpdateTimeToLive` over the wire, a converged poll on every
node's own `GET /admin/ttl` for the tables-list half plus a captured
`leader_tablets` per node (at least one node leads none of the table's
single tablet on a 3-node, RF-3 cluster — the same assertion the original
made), an already-expired item written through the `POST /admin/data/
dynamo` admin proxy from a **non-leader** of the table's tablet (this
rung's own forwarding-path convention), then a converged-or-timeout poll
across every node until SOME node's own `GET /admin/ttl` reports `reaper.
deleted_total >= 1` — riding the always-on loop exactly the way the
real-socket original's `sleep`-based poll rode out its own fast interval.
**No `SimCluster::drive_ttl_sweep` needed**: unlike `sim_cluster_ttl.rs`'s
own scenario (b), nothing here depends on an intermediate, pre-cadence
state — only "eventually reaped" — so the plan's own caution about
`OP_BUDGET` vs. the 200ms sweep interval doesn't bite the way it did for
`dynamo_ttl.rs`'s kept residual (PR 3's own finding). Scenario (6)'s own
doc comment is corrected in place: it used to say `deleted_total` stays 0
because "the reaper never actually runs under `SimEnv`" — no longer true
since PR 2 — it now says so because that scenario simply never writes an
expired item, pointing to scenario (8) as the one that actually exercises
a reap.

**`sim_cluster_console_stream.rs`** gains a new scenario (4),
`ttl_deletion_carries_the_service_user_identity_through_the_console` — the
exact name of `tests/console_stream.rs`'s sole remaining test, which is
deleted whole (it was the file's only test — nothing left to trim). Same
shape as the original: a streamed table with `NEW_AND_OLD_IMAGES`,
`UpdateTimeToLive`, an already-expired `PutItem` from a non-leader, a
bounded converged-or-timeout poll (a module-private `poll_until_reaped`,
an independent copy of `sim_cluster_ttl.rs`'s own private helper of the
same name and shape — not importable across modules) riding the always-on
loop, then the console's own `stream/shards` → `stream/iterator` →
`stream/records` walk (through [`SimCluster::console`], never the raw
`DynamoDBStreams_20120810.*` wire `sim_cluster_ttl.rs`'s own scenario (i)
uses) until the REMOVE record for the TTL-deleted key appears, asserting
its `userIdentity` is `{"PrincipalId": "dynamodb.amazonaws.com", "Type":
"Service"}` (ADR 0051 §7) — the console-side half of the identical
regression `sim_cluster_ttl.rs`'s scenario (i) already proves over the raw
wire (PR 3).

**Residual inventory, updated**: `tests/admin_endpoint.rs` now carries 9
tests (down from 10 — the TTL reaper-progress test converted, the other 9
untouched); `tests/console_stream.rs` is **deleted** (its sole remaining
test converted, nothing left to keep `ProdEnv` for) — TTL's own residual
group (ADR 0061 rung H's "TTL (1/9)" line) now has **zero** tests left in
`tests/*.rs` anywhere: `dynamo_ttl.rs` is already deleted (PR 3, one
residual test moved into it from nowhere — see that PR's own note), and
both of this PR's own targets are now gone or trimmed to nothing. This
rung reaches ADR 0061's own "expected residue after this rung: none"
prediction (the opener's own Tests-unlocked table) — the first clean
sweep in the whole F-through-I sequence, confirmed rather than assumed.

**Gates**: not run in this session — this PR's own working constraints
route compilation and test execution through the main tree, not this
worktree (`/home/user/wt-ttl4`). Remain to confirm there, in order: the
untrimmed `cargo test -p animusd --test admin_endpoint`/`--test
console_stream` (10 and 1 passed respectively) before this PR's edits;
after: `cargo test -p animusd --lib sim_cluster_admin`/`sim_cluster_
console_stream -- --test-threads=2`; the trimmed `cargo test -p animusd
--test admin_endpoint` (expected: 9 passed) with `console_stream.rs`
deleted (no binary to run at all); `cargo test -p animusd --lib
sim_cluster -- --test-threads=2` (expected 428 — PR 3's own 424 plus this
PR's 4 new `#[test]` functions: 2 new scenarios × 2 each, pinned +
`_over_seeds`); `cargo fmt --all --check`; `cargo clippy -p animusd
--all-targets --all-features -- -D warnings`. `Cargo.lock` untouched by
this PR.

**Known residual outside this PR's own file allowlist**: `crates/animusd/
src/lib.rs:18917`'s own doc comment on `mod sim_cluster_console_stream;`
still says the TTL-identity test "stays `ProdEnv`" — now stale, since PR 4
converts it — left uncorrected here because `lib.rs` is outside this PR's
constrained edit scope (`sim_cluster_admin.rs`/`sim_cluster_console_
stream.rs`/`admin_endpoint.rs`/`console_stream.rs`/docs only); a follow-up
touching `lib.rs` (this rung's own PR 5 close-out, or any later change
that already needs to touch that file) should fix it in passing.

See `crates/animusd/CLAUDE.md`'s matching "C-09 PR 4" appendix and
`docs/roadmap.md`'s C-09 entry for the crate-level pointer and status.

## 2026-09-08 amendment — Rung I, PR 5 (`DescribeTimeToLive` dispatch gap closed)

Closes the narrow `dynamo.rs` gap PR 3 named and reverted two scenarios
over: `dynamo::dispatch_item_op` had an arm for `UpdateTimeToLive` (since
ADR 0061 rung H, C-08 PR 2) but none for `DescribeTimeToLive`, so every
`DescribeTimeToLive` call issued under `SimCluster` answered `500`
(`unsupported_by_generic_dispatch`) regardless of how well-formed the
request was.

**The fix, exactly the shape PR 3's own doc already named**:
`describe_time_to_live` — a pure catalog read, `_ctx: &ClientCtx` already
unused before this change — widened from the bare concrete alias to
`<E: Env, R: RelayClient>(_ctx: &ClientCtx<E, R>, meta: &Metadata, table:
&str)`, mirroring `update_time_to_live`'s own C-08 PR 2 precedent (no
`tokio::time`/`ProdEnv`-only body to convert, unlike a widened async
handler — this one isn't even `async`). `dispatch_item_op` gained one new
arm, immediately after `UpdateTimeToLive`'s own:

```rust
Operation::DescribeTimeToLive { table } => describe_time_to_live(ctx, meta, &table),
```

`run_operation`'s own `DescribeTimeToLive` arm (`describe_time_to_live(ctx,
meta, &table)`, unchanged in shape) keeps calling this exact function,
monomorphized at `E = ProdEnv, R = AnimusdRelayClient` from `ClientCtx`'s
own definition-site default with **zero call-site changes** — the D2 PR 1
lesson (`docs/engineering-lessons.md`) applied once more: a narrowed
generic dispatcher gains a sibling arm, production dispatch is untouched.

**`sim_cluster_ttl.rs`**: scenarios (c) `update_time_to_live_enable_and_
disable_round_trip` and (d) `disable_with_a_mismatched_attribute_name_is_
rejected`, plus their `_over_seeds` siblings and the `describe_ttl_via_wire`
helper, restored from PR 3's own reverted first draft (seeds `0xC091_0003`/
`0xC091_0004` pinned, `0xC091_3000`/`0xC091_4000` bases for the `_over_
seeds` loops — PR 3 had already reserved this seed range for (c)/(d) before
reverting them, so no renumbering was needed). `tests/dynamo_ttl.rs` drops
to its one true residual, `expired_item_is_still_readable_immediately` —
the `OP_BUDGET`-granularity gap PR 3's own amendment above already
established has no fix available in this fixture at all, unlike the
`DescribeTimeToLive` gap this PR closes. The `json` helper in
`dynamo_ttl.rs`, used only by the two removed tests, is deleted with them.

**Scope note — this diverges from PR 1's own originally-planned PR
breakdown** (this amendment's "The PR series" list above named PR 4 as
admin/console residue and PR 5 as a docs-only close-out with no code).
This PR is a code change instead, sequenced ahead of that plan because
closing the `DescribeTimeToLive` gap is a smaller, more clearly-scoped
follow-up to PR 3's own named finding than either of those two — the
close-out amendment, once written, should renumber or fold in whichever
of the original PR 4/PR 5 scope still remains open at that time rather
than assume this PR satisfies it.

**Gates**: not run by this PR's own agent, which worked under a
hard constraint against invoking `cargo` at all (worktree
`/home/user/wt-ttl5`, compiled and gated separately by the maintainer,
who also rebased this PR onto PR 4's own landed tip rather than PR 3's —
so the baseline this PR's own new tests add to is PR 4's 424, not PR 3's
420). The expected shape, unchanged from every prior rung's own template:
`cargo test -p animusd --test dynamo_ttl` (1 passed, down from 3); `cargo
test -p animusd --lib sim_cluster_ttl -- --test-threads=2` (18 passed, up
from 14 — PR 3's 14 plus this PR's 4 new `#[test]` functions: scenarios
(c)/(d) pinned + `_over_seeds` each, untouched by PR 4); `cargo test -p
animusd --lib sim_cluster -- --test-threads=2` (428 passed, 0 failed, 2
ignored expected — PR 4's 424 plus these same 4); `cargo fmt --all
--check` and `cargo clippy -p animusd --all-targets --all-features -- -D
warnings` clean. `Cargo.lock` unchanged (no dependency touched). See
`crates/animusd/CLAUDE.md`'s matching "C-09 PR 5" appendix for the
crate-level pointer, and this PR's own commit for the actual gate
transcript the maintainer's session records.

## 2026-09-09 amendment — Rung I closed (C-09 complete)

PR 6 is what its own row above and the PR-series amendment promised: no
source, test, or `Cargo` change — this ADR's own D-train row and this
amendment, `docs/roadmap.md`'s C-09 entry, and `crates/animusd/CLAUDE.md`'s
consolidated TTL section are the entire PR. Rung I (C-09) is now **closed**:
the TTL reaper (ADR 0051) is deterministically fault-injectable and
seed-replayable under `SimCluster`/`SimEnv`, exactly as this rung's own
opening amendment set out to do — the smallest rung in the whole
F-through-I sequence, no new capability trait/newtype/store concept, since
the reaper loop was already generic going in.

**What the rung set out to do.** Rung H's own close-out named `TTL (1/9)`
the smallest of six residual groups left unowned after C-08 and
recommended it first: `tests/dynamo_ttl.rs`'s 9 real-socket tests plus one
residue test apiece in `admin_endpoint.rs` and `console_stream.rs`, all
provable only over `ProdEnv`'s real wall clock because no primitive drove
`animusd::ttl_reaper::ttl_reaper_loop` under `SimEnv`. The plan (this
amendment's own PR 1 opener, above) was the same widen-then-add-a-
generic-entry-point template every D3-through-C-08 rung had already
validated, applied to the smallest remaining surface: `animus_node::
ttl_reaper::ttl_reaper_loop` was already `<E: Env, H: TtlScanHost +
TtlReaperProgressHost>`-generic (rung C2), leaving only `impl
TtlReaperProgressHost for ClientCtx` and `animusd::ttl_reaper`'s thin
wrapper to widen, plus an always-on `SimCluster` spawn mirroring the
backup/segment janitors' own precedent.

**PR-by-PR, what landed.** PR 1 (docs-only opener, #780): the plan, the
grep-verified ground truth, the 11-test unlock table. PR 2 (groundwork,
#782): `TtlReaperProgressHost`/`ttl_reaper.rs`'s wrapper widened to `<E:
Env, R: RelayClient>` with zero production call-site changes;
`ttl_sweep_one_tablet` made `pub` in `animus-node`; `SimCluster` gained an
unconditional per-node reaper spawn at `SIM_TTL_SWEEP_INTERVAL` (200ms) in
both `::new` and `::restart`, plus `drive_ttl_sweep`; a new
`sim_cluster_ttl.rs` with two pinned-seed smokes. `cargo test -p animusd
--lib sim_cluster` 406 → 408. PR 3 (#785): `sim_cluster_ttl.rs` extended
with 5 more scenarios, converting 5 of the remaining 8 `dynamo_ttl.rs`
tests; found and named, rather than converted, the `OP_BUDGET`-vs-
always-on-reaper gap (below) and the `DescribeTimeToLive` dispatch gap
(below) — 420 passed. PR 4 (#786): the two named residue tests converted —
`sim_cluster_admin.rs`'s `admin_ttl_reports_reaper_progress_and_ttl_tables`
and `sim_cluster_console_stream.rs`'s `ttl_deletion_carries_the_service_
user_identity_through_the_console`, `console_stream.rs` deleted whole
(its sole remaining test) — 424 passed. PR 5 (#787): closed the
`DescribeTimeToLive` gap PR 3 named — `dynamo::describe_time_to_live`
widened to `<E: Env, R: RelayClient>`, a new `Operation::
DescribeTimeToLive` arm on `dispatch_item_op` — and restored PR 3's own
reverted scenarios (c)/(d); `tests/dynamo_ttl.rs` trimmed to its one true
residual — 428 passed. PR 6 (this PR): the close-out.

**The final `ProdEnv` residue, in full:**

| File | Kept test | Reason |
|---|---|---|
| `dynamo_ttl.rs` | `expired_item_is_still_readable_immediately` | Every `SimCluster` wire call's `spawn_and_capture` drives `self.sim.run_for(OP_BUDGET)` (12s) unconditionally, and `run_until` always drains every scheduled event up to that deadline before returning — with the always-on reaper ticking every 200ms, a single wire call already spans 60 sweep opportunities, so a `PutItem` writing an already-expired attribute has, by the time it returns, already handed the reaper dozens of chances to reap it. There is no "hold the reaper back between two wire calls" primitive — `drive_ttl_sweep` only ever adds sweeps on top of whatever the always-on loop already ran — so this test's own claim (an expired-but-unreaped item stays readable, ADR 0051's AWS-faithful visibility) cannot be reproduced through this fixture at all |

One test, one file — down from 9 tests across `dynamo_ttl.rs` plus one
apiece in `admin_endpoint.rs` (now 9 tests, none TTL-related) and
`console_stream.rs` (now deleted). This is the first rung in the whole
F-through-I sequence with only a single genuinely-permanent residual test
left, not a whole class.

**Mechanism lessons.**

1. **A fixed per-op virtual-clock budget can force an outcome a real
   fixture's slower cadence never would, in either direction.** Every
   prior `OP_BUDGET`-vs-short-interval caution in this codebase (Rung G's
   own close-out, this rung's own PR 1 opener) worried about the budget
   being too *short* to catch an intermediate state — the fix was always
   "drive it manually" (`drive_ttl_sweep`, `drive_stream_seal`). This
   rung found the mirror case: `expired_item_is_still_readable_
   immediately` needs the budget to be *short enough that the reaper
   hasn't run yet*, and once an always-on loop is ticking every 200ms
   against a 12s op budget, no amount of manual driving can *suppress*
   the 60 sweep opportunities that already happened inside the call that
   wrote the expired item. A "hold the reaper back" primitive would need
   to exist before this test could ever convert — nothing in this rung's
   own toolkit (`drive_ttl_sweep` only adds sweeps) provides one. The
   general form: an always-on background loop sharing a fixture's own
   virtual clock changes what's observable, not just what's driven by a
   test, the moment its cadence divides evenly into the fixture's own
   fixed per-call budget.
2. **A sibling operation missing from a generic dispatch path surfaces
   only when a conversion first actually calls it.** `UpdateTimeToLive`
   had been reachable through `dynamo::dispatch_item_op` since C-08 PR 2;
   `DescribeTimeToLive` had not — an asymmetry invisible by inspection
   (both looked equally "already generic" from their signatures, per the
   D2 PR 1 lesson) until PR 3 actually issued a `DescribeTimeToLive` call
   through `SimClusterHandle::dynamo` and got a `500`. This is the same
   family as the generic-signature-does-not-imply-seam-clean-body lesson
   (`seal_now`, `admin_transfer_control_leadership`) one layer up: a
   *dispatch table's* completeness is no more provable by reading
   signatures than a function body's clock-seam cleanliness is — both
   need an actual call through the generic path to confirm, not just a
   grep. Recorded in `docs/engineering-lessons.md` at PR 3/PR 5 (no new
   entry needed at this close).

**Test-count trajectory** (`cargo test -p animusd --lib sim_cluster --
--test-threads=2`, whole tier): 406 (C-08 baseline) → 408 (PR 2, +2) → 420
(PR 3, +12) → 424 (PR 4, +4) → **428 passed, 0 failed, 2 ignored** (PR 5,
+4) — 22 tests added across the rung, every gate reported green at each
step per that PR's own amendment above.

**What remains unowned after C-09**, updating the C-08-closing residual
inventory now that TTL is no longer on it: the control/data role split,
`--config` bring-up, index DDL beyond plain `CreateTable`, node
assembly/raw `ClientRequest`, and the throttle-metric counters. Five
groups remain, none with a rung against it today except index DDL, which
the maintainer sequenced as C-10 on 2026-09-08 (next, plan drafted — see
`docs/roadmap.md`'s C-10 entry).

**Website: no change needed, verified again at this close.** A targeted
grep of `website/*.html` for TTL/"time to live" found only claims already
true of production behavior — `UpdateTimeToLive`/`DescribeTimeToLive`
supported, an expired item stays readable until the reaper deletes it
(`docs.html`), the admin `ttl`/`ttl-reaper` CLI commands (`docs.html`,
matching `animus-cli`'s own `admin_request` arms) — none names `SimCluster`
testing specifically, and this rung changed no production dispatch path
or wire-observable behavior, the same reasoning every prior rung's opener
and close-out already applied.

**Gates**: none — documentation only, no `cargo` command run, `git diff
--stat` shows only the docs files (plus one doc comment in `lib.rs`) this
PR touches.

**Docs**: this amendment (closing Rung I); `docs/roadmap.md`'s C-09 entry
closed, its wave-10 sequencing row marked closed, C-10 noted as next
(plan drafted); `crates/animusd/CLAUDE.md`'s four PR 2-5 appendices folded
into one consolidated "TTL reaper under SimCluster" section with the
residual inventory corrected; `crates/animusd/src/lib.rs`'s
`sim_cluster_console_stream` doc comment corrected (it no longer says the
TTL-identity test stays `ProdEnv`); `docs/engineering-lessons.md` already
carries every finding this rung's own PRs made — no new entry needed at
this close.

## 2026-09-09 amendment — Rung J (post-C-09): index DDL beyond plain `CreateTable` `SimCluster` dispatch (C-10), PR 1 (this docs-only opener)

**Two rungs in a row have named this group and deferred it.** Rung H's
own close-out ("Rung H closed") listed blocker (d) — `dispatch_table_op`'s
`UpdateTable` arm rejecting any index change — among the six groups left
unowned after C-08, and its advisory recommendation for "C-09" explicitly
weighed this group against TTL and picked TTL first: smaller, no open
external blocker, and it paid off work two already-closed rungs had left
behind. Rung I's own opener repeated that comparison and its own
close-out named this group again as the sole entry with a rung against
it, "next, plan drafted." On 2026-09-08 the maintainer sequenced it as
**C-10**, the next rung in the F-through-J payoff sequence. This
amendment is that plan, grep-verified against the current tree — tracked
in `docs/roadmap.md` as **C-10**. PR 1 (docs only) is built; PRs 2-6
below follow, PR 7 closes the rung.

**Why this group, restated from both prior close-outs.** Of the six
groups Rung H's close-out left unowned (TTL, control/data role split,
`--config` bring-up, index DDL beyond plain `CreateTable`, node
assembly/raw `ClientRequest`, throttle-metric counters), TTL closed as
C-09, leaving five. Of those five, index DDL beyond plain `CreateTable`
is the only one with a rung against it: it is the largest by test count
(the D3-closing class-D breakdown's own snapshot said 9 files/30 tests;
a direct `grep -c '#\[tokio::test'` recount today across the nine files
this amendment names below — `update_table_create_index.rs` 4,
`update_table_drop_index.rs` 4, `dynamo_gsi_drain.rs` 1,
`backfill_seeder.rs` 5, `stream_backfill_seed_filter.rs` 2, 3 of
`console_table_config.rs`'s 4, `dynamo_index_scan.rs` 5,
`index_backfill.rs` 3, `dynamo_index_writes.rs` 6 — sums to 33, the same
kind of minor staleness Rung H's own opener already flagged for its own
D3-era count; still by far the largest of the five, versus single-digit
counts for the rest) and, unlike the control/data role split or
`--config` bring-up, needs no new deployment-shape fixture — only the
same widen-then-add-a-generic-entry-point template every rung since D3
has already validated, applied to `dispatch_table_op`'s one remaining
gap. Rung I's own opener flagged that three of this group's files
(`dynamo_index_scan.rs`, `index_backfill.rs`, `dynamo_index_writes.rs`)
were frozen behind open flake issues and recommended waiting on those;
this amendment does not touch any of the three (see "Frozen files,
untouched" below) — the rest of the group has no such blocker and does
not need those issues resolved first.

**Ground truth**, from a read-only grep of the current tree, not a plan
expectation — every claim below was checked against source, not
carried over from the plan draft that proposed it:

- `crates/animusd/src/dynamo.rs:1726`'s `dispatch_table_op`'s
  `UpdateTable` arm (`:1770-1815`) rejects any call carrying an index
  change before it even looks at `stream`/`throughput_update`:
  `if index_update.is_some() { return Err(unsupported_by_generic_dispatch(
  "UpdateTable with an index change")); }` (`:1777-1780`). `key_types` is
  accepted but ignored (`:1774`'s own `key_types: _`) — it only matters
  for `IndexUpdate::Create`, which this arm never reaches today.
- `create_index`/`drop_index`/`set_index_status`/`drop_table_index`
  (`dynamo.rs:4509`/`:4627`/`:4680`/`:4716`) are concrete: each takes
  `ctx: &ClientCtx` (bare `E = ProdEnv`, `R = AnimusdRelayClient`
  defaults) and each of `set_index_status`/`drop_table_index`/
  `create_index` runs its own `tokio::time::Instant::now()` deadline plus
  `tokio::time::sleep(SCHEMA_POLL_INTERVAL)` commit-wait loop — the exact
  shape `update_time_to_live`'s and `update_table_throughput`'s own
  conversions already fixed elsewhere in this same file. **Every
  `ClientCtx` method these four functions call —
  `propose_schema`/`drop_table_tablets`/`clear_backfill_cursor_for_table`
  — is already generic**: `schema.rs:24`'s
  `impl<E: Env, R: RelayClient> ClientCtx<E, R>` block covers all three.
  This makes the four functions' own widening pure signature-plus-clock-
  call surgery, no new capability — `update_table_throughput`'s own doc
  comment (`dynamo.rs`, immediately above its definition) already states
  the template verbatim: "its three `tokio::time::Instant::now()`/
  `tokio::time::sleep` sites became
  `ctx.env.now().saturating_add(..)`/`ctx.env.sleep(..)`."
  `update_table` itself (`dynamo.rs:4360`, the `ProdEnv`-only wire entry
  point) stays untouched and monomorphized, exactly as
  `update_table_throughput`'s doc says `dispatch_table_op`'s own arm is
  "what reaches this generically" — this rung follows the identical
  pattern one layer down, for the index-change shape.
- `index_drain::drain_tablet<E: Env, R: RelayClient>`
  (`index_drain.rs:1049`), `reconcile_partition<E: Env, R: RelayClient>`
  (`:1250`), and `gsi_caught_up<E: Env>` (`:1176`) are **already generic**
  — D3 PR 3b's own GSI-drain widening, unchanged since. Nothing about the
  drain mechanism itself needs touching by this rung.
- `crates/animus-node/src/index_backfill.rs:51`'s `pub async fn
  index_backfill_loop<E, H>(env: E, host: H, interval_ms: u64) where
  E: Env, H: ControlLeaderHost<E>` — the backfill-**completion
  aggregator** (ADR 0045 §4: watches `Metadata::index_backfill` and flips
  a table's index `Creating` → `Active` once every live tablet has
  reported) — is already fully generic (rung C2), and
  `client_ctx_host.rs:47`'s `impl<E: Env, R: RelayClient>
  ControlLeaderHost<E> for ClientCtx<E, R>` is **already generic too** —
  no host-trait groundwork is needed at all for this loop, an even
  smaller lift than C-09's TTL wrapper (which also needed
  `TtlReaperProgressHost` widened). The **only** concrete surface here is
  `crates/animusd/src/index_backfill.rs`'s entire 18-line body: `pub(crate)
  async fn index_backfill_loop(ctx: crate::ClientCtx) { let env =
  ctx.env.clone(); animus_node::index_backfill::index_backfill_loop(env,
  ctx, animus_node::index_backfill::INDEX_BACKFILL_LOOP_INTERVAL_MS).await;
  }` — the identical thin-delegation shape C-09's `ttl_reaper.rs` wrapper
  had, confirmed by direct comparison of both files.
- `index_drain::backfill_seed_tick` (`:1412`, concrete `&ClientCtx`/
  `&CpGroup`), `advance_backfill_cursor` (`:1539`, concrete `&CpGroup`),
  and `seed_change_log_record` (`:1651`, concrete `&CpGroup`, one
  `tokio::time::Instant::now()` deadline) are the **backfill seeder** —
  a distinct loop from the completion aggregator above, populating
  `Metadata::index_backfill` by scanning a led tablet's own base rows.
  All three are called only from `backfill_seed_tick`'s own caller inside
  `index_drain::change_consumer_loop` (`:648`), which `sim_cluster.rs:2987`'s
  own doc comment states explicitly: "This fixture never spawns
  `index_drain::change_consumer_loop` itself" (the identical finding
  D4 PR 2's `inplace_split_driver_tick`/`recompute_any_table_throughput_all`
  primitives already work around by hand-driving one piece of that loop
  at a time, never the whole thing). **No existing `SimCluster` primitive
  drives `backfill_seed_tick`** — confirmed by grep, zero hits for the
  name anywhere in `sim_cluster.rs`.
- `SimCluster::drain_gsi(&mut self, node: u64, table: &str)`
  (`sim_cluster.rs:2802`) already exists (D3 PR 3b) and calls
  `index_drain::drain_tablet` directly for every led tablet of `table`
  with at least one `Creating`/`Active` GSI — it materializes a GSI's rows
  from **existing** data (the drain side), not the seeder that populates
  `index_backfill` in the first place. This rung needs its own,
  differently-shaped sibling for the seed side (PR 2's
  `drive_backfill_seed`, below) — `drain_gsi` itself needs no change.
- `GenericConsoleBackend<E, R>`'s `add_gsi`/`drop_gsi`
  (`lib.rs:4107`/`:4123`, inside `lib.rs:3832`'s `impl<E: Env, R:
  RelayClient> console::ConsoleBackend for GenericConsoleBackend<E, R>`)
  already build the `UpdateTable` payload and call
  `crate::dynamo::execute_routed_as_generic` — the C-08 PR 2/3 generic
  fork already routes a console-driven add/drop-GSI call through
  `dispatch_table_op` exactly like a direct wire call would. **Zero
  `lib.rs`/console change is needed once `dispatch_table_op`'s own
  `UpdateTable` index sub-arm exists** — the console path is already
  wired to fall through to it.
- `MetaCommand::CreateTableIndex`/`DropTableIndex`/`SetIndexStatus`/
  `MarkIndexBackfilled` are already on `is_relayable_command`'s allowlist
  (`animus-node/src/wire.rs:764-781`) — a follower-hosted or control-only
  node relays every one of these commands to the control leader already;
  nothing here is a missing-allowlist bug of the kind the root
  `CLAUDE.md`'s own "grep every gating match site" rule warns about.
- **Frozen files, confirmed untouched by this series**:
  `dynamo_index_scan.rs` (5 tests currently, `grep -c '#\[tokio::test'`),
  `index_backfill.rs` (3 tests), and `dynamo_index_writes.rs` (6 tests)
  are each frozen behind an open flake issue, confirmed by issue number
  and title via `gh api repos/animus-db/animus-db/issues/<n>` at the time
  of this writing: **#418** ("Flaky under serialized full-suite load:
  dynamo_index_scan::gsi_scan_paginates_and_drains_all_rows", open),
  **#592** ("prod-liveness flake: index_backfill panics with `connect:
  Connection refused` on a freshly bound config address", open — also
  the subject of `docs/engineering-lessons.md`'s own dated entry), and
  **#610** ("prod-liveness-hammer-pair flake: dynamo_index_writes' first
  CreateTable after bootstrap exhausts SCHEMA_COMMIT_TIMEOUT", open).
  `#601` (also in the class-E list) is `batch_write.rs`'s own flake, a
  different file, not one of this group's three. This rung's own
  off-limits list (below) names all three explicitly; none is read,
  edited, or converted by any PR in this series while its issue stays
  open, matching the standing instruction every prior rung's own
  off-limits list has already applied to `streams_e2e.rs` (#298).
- `console_table_config.rs` (4 tests total, `grep -c` confirmed) has
  exactly 3 tests in this group — `add_and_drop_gsi_round_trip`,
  `add_gsi_records_a_declared_attribute_type`,
  `add_gsi_rejects_an_unknown_attribute_type` — plus one,
  `table_detail_shows_pitr_status_and_backups`, that is **not** part of
  this group: it asserts `TableDetail.pitr`/`backups`, which read
  `Metadata` fine today but whose underlying `BeginBackup`/PITR data is a
  separate unowned residual (per Rung H's own blocker-(d) note), so it
  stays `ProdEnv` and out of scope for this series regardless of what
  else converts in this file.

**Tests unlocked, by file** (counts taken directly with `grep -c
'#\[tokio::test' <file>`, not assumed):

| File | Tests | Disposition this series |
|---|---|---|
| `update_table_create_index.rs` | 4 | Converts (PR 3) |
| `update_table_drop_index.rs` | 4 | Converts (PR 3) |
| `dynamo_gsi_drain.rs` | 1 | Converts (PR 3) |
| `backfill_seeder.rs` | 5 | Converts (PR 4) — `split_during_backfill` may stay `ProdEnv` if it does not converge cleanly under `SimCluster` (see PR 4's own note below) |
| `stream_backfill_seed_filter.rs` | 2 | Converts (PR 5) |
| `console_table_config.rs` | 4 (3 in scope) | 3 GSI tests convert (PR 6); `table_detail_shows_pitr_status_and_backups` stays `ProdEnv`, filed under the separate PITR residual, out of scope |
| `dynamo_index_scan.rs` | 5 | **Frozen, untouched** — #418 |
| `index_backfill.rs` | 3 | **Frozen, untouched** — #592 |
| `dynamo_index_writes.rs` | 6 | **Frozen, untouched** — #610 |

16 tests convert (PRs 3-6); 14 stay `ProdEnv` (3 frozen files' 14 tests
plus 1 PITR test in `console_table_config.rs`). `schema_ddl_relay.rs` (7
tests, hand-driven `MetaCommand` proposals against a control-leader-only
aggregator, per that file's own doc comment mirroring
`index_backfill.rs`'s and `stream_janitor.rs`'s identical shape) converts
**0 of 7** — it is a real-thread bring-up proving the aggregator loop's
behavior directly via `ClientRequest::ProposeSchema`, not through the
DynamoDB wire, and this series does not touch it; any conversion there is
its own separate, unclaimed piece of work.

**The PR series** (7 PRs, stacked on the C-09 close-out):

- **PR 1 (this amendment).** Docs only: this ADR amendment, the C-10
  roadmap entry (and wave-11 sequencing row updated from "next, plan
  drafted" to open), this file's own rung-table row J, and
  `crates/animusd/CLAUDE.md`'s residual-inventory pointer. **Gates:**
  none — documentation only, no `cargo` command run.
- **PR 2 — Groundwork.** Widen `create_index`/`drop_index`/
  `set_index_status`/`drop_table_index` to `<E: Env, R: RelayClient>`
  (their `tokio::time::Instant::now()`/`tokio::time::sleep` sites become
  `ctx.env.now()`/`ctx.env.sleep(..)`, mirroring
  `update_table_throughput`'s own already-landed conversion exactly); add
  the `UpdateTable` index-change sub-arm to `dispatch_table_op`, mirroring
  `update_table`'s own `(None, Some(update), None)` match arm
  (`IndexUpdate::Create` → `create_index`, `IndexUpdate::Delete` →
  `drop_index`). Widen `crates/animusd/src/index_backfill.rs`'s wrapper to
  `<E: Env, R: RelayClient>` — no host-trait change needed
  (`ControlLeaderHost` is already generic). Widen `backfill_seed_tick`/
  `advance_backfill_cursor`/`seed_change_log_record` to the `Env` seam
  (`&CpGroup` → `&CpGroup<E>`, the one `tokio::time::Instant` site
  becomes `ctx.env.now()`). In `sim_cluster.rs`: spawn
  `index_backfill::index_backfill_loop` unconditionally per node in both
  `SimCluster::new` and `::restart`, beside the TTL/segment/backup janitor
  spawns (identical shape, `SIM_TTL_SWEEP_INTERVAL`'s own 200ms precedent);
  add `SimCluster::drive_backfill_seed(node, table)` mirroring
  `drain_gsi`'s own shape (hand-calls `backfill_seed_tick` for every led
  tablet of `table` with a `Creating` GSI, looped to exhaustion the way
  `drive_stream_seal` loops `seal_now`). **Gate:** untrimmed
  `cargo test -p animusd --test update_table_create_index --test
  update_table_drop_index --test dynamo_gsi_drain --test backfill_seeder
  --test stream_backfill_seed_filter --test schema_ddl_relay` first, to
  confirm the new always-on spawn and widened functions perturb nothing
  about the existing real-socket suites (`schema_ddl_relay.rs` stays
  untouched by design but must stay green as a control); then
  `cargo test -p animusd --lib`; `cargo clippy -p animusd --all-targets
  --all-features -- -D warnings`; `cargo fmt --all --check`.
- **PR 3 — `sim_cluster_dynamo_update_table_index.rs`.** Converts
  `update_table_create_index.rs` (4), `update_table_drop_index.rs` (4),
  and `dynamo_gsi_drain.rs` (1) — 9 tests total, delete all three files
  whole once the untrimmed baseline and the new sim module both stay
  green. **Gate:** untrimmed baseline first → sim module green → trim →
  re-run (nothing left to re-run once deleted) → whole
  `cargo test -p animusd --lib sim_cluster -- --test-threads=2` tier,
  anchored RSS sampler (`pgrep -f '^/home/user/animus-db/target/debug/
  deps/animusd-'`, never an unanchored substring match, per the #753
  postmortem) → `cargo fmt --all --check` → `cargo clippy -p animusd
  --all-targets --all-features -- -D warnings`.
- **PR 4 — `sim_cluster_backfill_seeder.rs`.** Converts `backfill_seeder.rs`'s
  5 tests using `drive_backfill_seed`; the split-during-backfill scenario
  is licensed to stay `ProdEnv` if it does not converge cleanly under
  `SimCluster` (a real split interacting with an in-flight seeder tick is
  exactly the kind of cross-cutting race this fixture's hand-driven
  primitives are not guaranteed to reproduce faithfully — same caution
  D4 PR 2's own split-driver primitives carry). **Gate:** same shape as
  PR 3 — untrimmed baseline → sim green → trim (or document the kept
  test's own reason if it stays) → whole `sim_cluster` tier + RSS sampler
  → fmt → clippy.
- **PR 5 — `sim_cluster_stream_backfill_seed_filter.rs`.** Converts
  `stream_backfill_seed_filter.rs`'s 2 tests, using `drive_backfill_seed`
  plus C-07's own `drive_stream_seal` primitive together (the file's own
  name says what it tests: the backfill seeder's filter interaction with
  an active stream). **Gate:** same shape as PR 3/4.
- **PR 6 — Console residue.** Add the 3 GSI-related
  `console_table_config.rs` scenarios (add/drop round trip, declared
  attribute type, rejected unknown type) to a new
  `sim_cluster_console_table_config.rs` (or extend an existing sibling
  module, whichever reads cleaner once written), driven through
  `GenericConsoleBackend`; trim `console_table_config.rs` to its one
  remaining test, `table_detail_shows_pitr_status_and_backups` (stays
  `ProdEnv`, out of scope per the Ground truth section above — never
  delete this file). **Gate:** untrimmed `console_table_config.rs` (4
  tests) before → sim scenarios green → trim to 1 → whole `sim_cluster`
  tier + RSS sampler → fmt → clippy.
- **PR 7 — Docs close-out.** This rung marked "closed": a "Rung J closed"
  amendment with the PR-by-PR account, before/after `sim_cluster` test
  counts, the final residue table (the 3 frozen files' 14 tests plus the
  1 PITR test — 15 total, matching this opener's own prediction unless a
  PR finds a genuine new blocker, in which case the close-out amendment
  says so explicitly rather than silently matching the prediction), the
  gate transcript, and `docs/roadmap.md`/`crates/animusd/CLAUDE.md`
  updated to match — the same shape every prior rung's own final PR
  already used. **Gates:** none — documentation only.

**Gates, in order, for each test-touching PR** (PR 2-6): untrimmed
file(s) green → the new/extended sim module green → trim → re-run the
untrimmed file(s) (until deleted, or reduced to a documented residual) →
`cargo test -p animusd --lib sim_cluster -- --test-threads=2` (whole
tier, 428 tests expected going in per C-09's own close-out, growing by
each PR's own conversions; RSS via the anchored sampler — `pgrep -f
'^/home/user/animus-db/target/debug/deps/animusd-'`, per the #753
postmortem, never an unanchored substring match — watched for the same
no-leak shape every prior rung confirmed) → `cargo fmt --all --check` →
`cargo clippy -p animusd --all-targets --all-features -- -D warnings`.
**Untrimmed-suite-first discipline restated**: every PR that trims or
deletes a real-socket file runs that file's own untrimmed baseline green
*before* writing a single line of the sim sibling, exactly as every gate
list above orders it — a sim module written first and only checked
against the real file's behavior after the fact risks silently reproducing
a bug the real file never had, the same caution the D2 PR 1 lesson
(a generic signature does not prove a seam-clean body) generalizes to
whole test files.

**Binding rules.** Production dispatch stays byte-identical throughout:
`create_index`/`drop_index`/`set_index_status`/`drop_table_index`/the
`index_backfill` wrapper/the three `index_drain` seeder functions widen
their own type parameters on `ClientCtx<E, R>`'s/`CpGroup<E>`'s already-
generic defaults — no `Generic*` newtype is needed here, unlike C-08 PR
2's `GenericAdminHost`/`GenericConsoleBackend`, because none of these
seven functions is ever called through a `dyn`/trait-object production
dispatch table; each stays a concrete free function/method callable
exactly as it is today, so widening in place cannot silently narrow a
production caller. **Generic siblings, never a blanket `impl Trait for
ClientCtx`** — the same C-08 PR 2 same-day-corrected lesson: if a rung-2
implementation finds itself reaching for a trait impl on bare `ClientCtx`
that a production dispatch table also uses, stop and use the newtype
pattern instead of narrowing production. No `HashMap`/`tokio::time` may
appear in any now-generic function body — `index_drain.rs`/`dynamo.rs`
are `animusd` code, workspace-wide `disallowed-methods`/
`disallowed-types` lints apply except where `animusd`'s package-level
carve-out and the five `#[deny(clippy::disallowed_methods)]`-marked
client-path modules (`lib.rs`'s `schema`/`read_path`/`write_path`/
`txn_coordinator`/`forwarding`) say otherwise — `dynamo.rs` and
`index_drain.rs` are not among those five denied modules, so review by
hand remains the enforcement there, same as every prior rung.
`OP_BUDGET` (12s) versus `BACKFILL_SEED_BATCH` (the seeder's own
per-tick partition-discovery cap, `index_drain.rs`'s own constant): a
backfill can need more partitions seeded than one tick covers, so a
scenario asserting full backfill completion must call
`drive_backfill_seed` **multiple times** (looped to exhaustion, mirroring
`drain_gsi`/`drive_stream_seal`'s own shape), never assume one call
seeds everything — the same `OP_BUDGET`-vs-short-interval caution every
prior rung's own gates section has carried, applied here to a bounded
per-tick batch rather than a wall-clock interval. The always-on
`index_backfill_loop` spawn needs **no new `Drop` coverage**: `#753`'s
existing contract (`sim_cluster.rs:1214`'s `impl Drop for SimCluster`)
already breaks every node's relay self-cycle and drains `self.sim`
(every perpetual task, this rung's new one included) unconditionally —
the identical "no new Drop obligation" finding Rung I's own opener made
for its own new perpetual loop. **Frozen files stay untouched**:
`dynamo_index_scan.rs`, `index_backfill.rs`, `dynamo_index_writes.rs`
(behind #418/#592/#610 respectively), plus this series' own inherited
off-limits list from every prior rung (`streams_e2e.rs` #298,
`batch_write.rs` #601, `split_placing_two_replica_diff_e2e.rs`,
`cp_cross_process.rs`, `cp_txn.rs`, `dynamo_txn_idempotency.rs`,
`shared_wal_liveness.rs`) — none of these seven is read for more than
this investigation, edited, or converted by any PR in this series.

**Risks.** GSI backfill machinery generalization: low, contrary to Rung
I's own opener's initial worry — the drain half was already generalized
by D3 PR 3b, and both remaining loops (completion aggregator, seeder)
turn out to need only signature/clock-call widening, no new mechanism,
once actually inspected (the `ControlLeaderHost` finding above is the
concrete reason this rung is smaller than Rung I's opener feared).
`OP_BUDGET`-vs-`BACKFILL_SEED_BATCH` mismatch: medium — a scenario author
who assumes one `drive_backfill_seed` call finishes an arbitrarily large
backfill will get a flaky-looking "index never went Active" failure that
is actually a scenario-design bug, not a product bug; the binding rule
above is the mitigation, and PR 4's backfill-seeder scenarios are the
first real test of it. Split-during-backfill convergence: medium,
explicitly licensed to stay `ProdEnv` in PR 4 if it does not converge —
this is the one test in the whole series pre-authorized to *not*
convert, so a genuine convergence gap there is not a series-blocking
finding, just a documented residual. Frozen-file leakage: low — the
three files are named explicitly in every PR's gate list and the binding
rules' off-limits list; the discipline every prior rung has already
applied to `streams_e2e.rs` generalizes directly.

**Website:** no change needed — this rung touches no wire-observable
behavior; every production dispatch path stays byte-identical per this
amendment's own binding rules, the same reasoning every prior rung's
opener already applied. Verified by a targeted grep of `website/*.html`
for GSI/index/`UpdateTable` claims: all describe already-true production
behavior (GSI add/drop on a populated table, backfill converging to
`Active`), none names `SimCluster` testing specifically.

**Docs:** this amendment (opening Rung J, open); `docs/roadmap.md`'s new
C-10 entry and its wave-11 sequencing row updated from "next, plan
drafted" to open with this PR's plan summary; `crates/animusd/
CLAUDE.md`'s residual-inventory index-DDL mention annotated with a
pointer to this open rung. **Gates:** none — documentation only, no
`cargo` command run, `git diff --stat` shows only the three docs files
this PR touches.

## 2026-09-09 amendment — Rung J, PR 2 landed (groundwork)

Brief as-built note; the opener's own plan (this docs-only PR, landed
alongside on `-109`) carries the full plan and grep-verified ground truth.

Widened `dynamo.rs`'s `create_index`/`drop_index`/`set_index_status`/
`drop_table_index` to `<E: Env, R: RelayClient>` (`tokio::time::
Instant::now()`/`sleep` → `ctx.env.now()`/`ctx.env.sleep(..)`, the
established rung-wide precedent), added a mirroring index-change sub-arm
to `dispatch_table_op`'s `UpdateTable` match (`(None, Some(update), None)`
→ `create_index`/`drop_index`, same shape as its stream/throughput
siblings), widened `index_backfill.rs`'s thin wrapper the same way, and
widened `index_drain.rs`'s `backfill_seed_tick`/`advance_backfill_cursor`/
`seed_change_log_record` (the last two take `group: &CpGroup<E>` with no
`&self` — converted via `group.env()`, the documented pattern `seal_now`
already established for this exact shape).

**A `tokio::time` body found in an already-generic-signature function,
the same recurring pattern this rung's own predecessors (`seal_now`,
`admin_transfer_control_leadership`, `recovery_grace_now_ms`) already
recorded**: `clear_backfill_cursor<E: Env>` (used by `drop_index`'s own
cascade via `ClientCtx::clear_backfill_cursor_for_table`) already carried
a generic signature but its commit-wait loop still called bare
`tokio::time::Instant::now()`/`tokio::time::sleep` — found immediately by
the first sim smoke run (`SimEnv` has no real Tokio reactor, so the call
panics "there is no reactor running" the instant it's reached), fixed
with the identical `group.env()` conversion. See `docs/engineering-
lessons.md`'s matching entry (this is that lesson's fourth recurrence in
this crate).

`SimCluster::new`/`::restart` spawn `index_backfill::index_backfill_loop`
unconditionally on every node (mirroring the backup/segment/TTL janitors'
own always-on spawns — its own leader gate already makes a non-leader's
tick a cheap no-op, so no opt-in gate is warranted); a new
`SimCluster::drive_backfill_seed(node, table)` mirrors `drain_gsi`'s exact
shape, driving one `backfill_seed_tick` per `Creating` GSI over every
tablet `node` leads, documented as needing several calls for a table with
more than `BACKFILL_SEED_BATCH` partitions.

Two new `sim_cluster_index_ddl.rs` scenarios (`_over_seeds` at 5 seeds
each): `UpdateTable` adding a GSI to a populated table returns it
`CREATING`/`Backfilling: true`, rejects a `Query` against it while
backfilling, and converges to `ACTIVE` (via `drive_backfill_seed` +
`drain_gsi`, polled through further `DescribeTable` calls — never a
single call assumed to finish) with the expected rows queryable; deleting
that GSI leaves it absent from `DescribeTable` and rejects a `Query`
against the now-gone name. **No `update_table`/`run_operation`/
`execute_routed` change** — `git diff` on `dynamo.rs` shows only
signatures, `tokio::time` → `ctx.env` conversions, and the new sub-arm;
the untrimmed real-socket suites (`update_table_create_index.rs`/
`update_table_drop_index.rs`/`dynamo_gsi_drain.rs`/`backfill_seeder.rs`/
`stream_backfill_seed_filter.rs`/`console_table_config.rs`, 20 tests) stay
byte-for-byte identical before and after.

`cargo test -p animusd --lib sim_cluster -- --test-threads=2`: 428 → 432
passed (+4, 0 regressions), peak RSS ~958 MB (`/usr/bin/time -v`).

## 2026-09-09 amendment — Rung J, PR 3 landed (`sim_cluster_dynamo_update_table_index.rs`)

Converts `tests/update_table_create_index.rs` (4), `tests/update_table_
drop_index.rs` (4), and `tests/dynamo_gsi_drain.rs` (1) — 9 tests total —
into `crates/animusd/src/sim_cluster_dynamo_update_table_index.rs`
(registered beside `sim_cluster_index_ddl` in `lib.rs`), 18 `#[test]`
functions (a pinned seed plus a 5-seed `_over_seeds` sibling per
scenario), and deletes all three real-socket files whole. No `dynamo.rs`/
`index_drain.rs`/`sim_cluster.rs` change — every primitive this PR drives
(`SimCluster::dynamo`/`drive_backfill_seed`/`drain_gsi`/`propose_meta`/
`handle`/`crash`/`restart`/`run_for`/`storage`/`hosted_tablets`) already
existed after PR 2's groundwork; this PR is driver-plus-assertions only.
Every scenario asserts the identical observable behaviour the original
asserted, through `SimCluster::dynamo`: client-side validation rejections
(duplicate/reserved/`$`-containing index names, a nonexistent table), the
`MAX_GSI_PER_TABLE` cap, a non-leader-issued `UpdateTable` converging on
every node's own `Metadata`, a populated-table drop's catalog and
physical reclaim, a drop racing an in-flight backfill, a create-drop-
recreate cycle proving the backfill cursor is genuinely cleared rather
than stale-resumed, and the GSI drain itself (materialize, move on
overwrite, prune on delete).

**Three substitutions, all documented inline in the module's own
top-of-file doc** (this rung's own binding rules license a documented
deviation rather than requiring literal reproduction where the fixture's
own shape makes one structurally impossible):

- `drop_of_an_active_index_on_a_populated_table_reclaims_everything`'s
  original checked a real WAL file's absence on disk
  (`tablet_wal_present`) — `SimCluster` hosts every tablet on
  `MemoryEngine`, so there is no file. The substitute checks `Metadata`/
  `hosted_tablets` absence **and** the hidden table's own tablet id
  reading back an empty engine, together in one converged-or-timeout
  poll — mirroring `sim_cluster_dynamo_drop_table.rs::assert_reclaimed`'s
  own discipline (metadata/hosted-set and engine-emptiness checked
  TOGETHER, never split across two passes), generalized to an index's
  own hidden table. Strictly stronger than the original's own check, not
  weaker.
- `in_flight_backfill_is_cancelled_by_a_concurrent_drop`'s original raced
  a background poll against a live `UpdateTable Delete` over real OS
  threads (`tokio::join!`). `SimCluster::dynamo` always runs a request to
  completion in one synchronous call, so there is no window from a
  test's own code to interleave a second action mid-flight. This
  scenario issues the drop immediately after exactly one partial
  `drive_backfill_seed` tick (300 rows, past the seeder's own per-tick
  discovery cap, so the index is provably still `Creating`) — a
  deterministic, single-seed-reproducible analogue that still proves the
  same property (an in-progress, not-yet-finished backfill is genuinely
  cancelled, not raced to completion), without literally reproducing the
  original's own thread interleaving.
- `a_crash_and_retry_mid_cascade_still_converges`'s original used a real
  process (`fire_and_forget_dynamo` + a 15ms sleep + `shutdown_graceful`)
  on a single-node cluster. This scenario spawns the `Delete` by hand on
  the target node's own `SimEnv` (`SimCluster::handle().env(node)`),
  drives the simulator a few milliseconds (letting the cascade make a
  little partial progress, mirroring the original's own brief sleep),
  then interrupts with `SimCluster::restart` — a true process stop that
  drops the still in-flight task, the fixture's own established idiom
  for "crash during X" (`sim_cluster_dynamo_drop_table.rs::run_scenario_
  4_a_node_crashed_during_the_drop_and_restarted_reclaims_its_engine`
  crashes BEFORE issuing the racing op rather than mid-flight, for the
  identical reason: there is no sub-call granularity to interrupt at).
  **Runs on a 3-node cluster, not the original's 1** — `SimCluster::
  restart` rebuilds the restarted node's own control-plane log from
  scratch (`RaftNode::start(.., MemoryEngine::new())`) and relies on
  ordinary peer catch-up to repopulate it; a 1-node cluster has no peer
  to catch up from, so it would lose all replicated `Metadata` (including
  the table itself) on restart — a genuine fixture-shape difference from
  the original's real single process, which recovered from its own
  on-disk WAL. See `docs/engineering-lessons.md`'s matching entry.

**Residual index-DDL count, updated from the opener's own nine-file/
33-test snapshot**: three files gone (9 tests, this PR); six remain —
`backfill_seeder.rs` (5, PR 4), `stream_backfill_seed_filter.rs` (2, PR
5), `console_table_config.rs` (4, 3 in scope for PR 6, 1 stays `ProdEnv`
under the separate PITR residual), and the three frozen files this
series does not touch (`dynamo_index_scan.rs` #418, `index_backfill.rs`
#592, `dynamo_index_writes.rs` #610, unchanged).

**Gates**: built in a worktree without `cargo` access (delegated to the
maintainer's own gate run per this session's own constraints) — every
API this module calls (`SimCluster::dynamo`/`scan`/`metadata`/`hosted_
tablets`/`storage`/`handle`/`propose_meta`/`control_leader_index`/`node_
count`/`leader_index_of`/`drive_backfill_seed`/`drain_gsi`/`crash`/
`restart`/`run_for`/`seed`, `SimClusterHandle::env`/`dynamo`,
`Metadata::table_indexes`/`has_table_tablet`/`tablets_for_table`/
`index_backfill`) was individually cross-checked against its declared
signature in `sim_cluster.rs`/`meta.rs` rather than compiled. The ADR's
own PR 3 gate list (untrimmed baseline green → sim module green → trim →
whole `sim_cluster` tier + RSS sampler → fmt → clippy) is the
maintainer's own next step, not yet recorded here.

See `crates/animusd/CLAUDE.md`'s matching appendix and `docs/roadmap.md`'s
C-10 entry for the full record.

## 2026-09-09 amendment — Rung J, PR 4 landed (the backfill seeder)

Converted 4 of the 5 scenarios in `tests/backfill_seeder.rs` (ADR 0045 §2)
into a new `crates/animusd/src/sim_cluster_backfill_seeder.rs`, driven
entirely through PR 2's own primitives (`SimCluster::drive_backfill_seed`/
`drain_gsi`, the always-on `index_backfill::index_backfill_loop`) — no
`dynamo.rs`/`index_drain.rs`/`sim_cluster.rs` change, pure test authorship.
`backfill_seeder_materializes_every_pre_existing_row_then_flips_active`,
`live_writes_during_backfill_converge_to_the_correct_final_gsi` (issued
sequenced, not raced — see below), `two_indexes_creating_simultaneously_
converge_independently`, and `a_crash_and_restart_mid_backfill_still_
converges` (a `SimCluster::crash`/`restart` of the tablet's own leader
mid-sweep, 300 rows > `BACKFILL_SEED_BATCH` so one seed round provably
cannot finish, resuming from the durable `KIND_CURSOR` row committed
before the crash) each gained an `_over_seeds`-paired sibling (5 seeds).

**`split_during_backfill_converges_with_correct_final_gsi` exercised the
opener's own explicit license (this amendment's plan already named it,
above) and stayed on `ProdEnv`, unmodified, alone in the trimmed
`tests/backfill_seeder.rs`.** `SimCluster` spawns no `index_drain::
change_consumer_loop` at all, so proving this scenario would mean
hand-interleaving three separately-timed on-demand primitives
(`drive_backfill_seed`/`drain_gsi`/`drive_inplace_split_cutover`) every
round with no way, in the session that wrote this PR, to verify offline
that the always-on completion aggregator can't race the cutover propose
to `Active` (the aggregator watches "every tablet *currently* in the
table's live map has reported," which is satisfied by the still-un-cut-
over parent alone, independent of whether cutover has actually committed)
or that the post-cutover Fork-A per-child resweep converges within any
round budget that was never run.

**A stated deviation**: the converted `live_writes_...` scenario's five
race writes are issued *sequenced*, immediately after the index commits
`Creating` and before any `drive_backfill_seed` round runs at all, rather
than raced via a second concurrent task — this fixture drives everything
from one thread with no automatic background sweep to race against. What
it proves is the property the original test's own doc names as load-
bearing (the final materialized GSI matches the final base-table state
regardless of write/seed interleaving), not the literal concurrency.

**Originally written with no `cargo` access** (the authoring session's own
worktree was isolated from a build environment) — every API called was
confirmed by reading its current signature/doc in the tree rather than by
compiling. **A subsequent real gate run found and fixed one genuine
fixture gap**: `SimCluster::restart` respawned every other always-on
background loop (`heartbeat_loop`/`backup_janitor_loop`/
`segment_janitor_loop`/`ttl_reaper_loop`/`auto_split_loop` when opted in)
but not `index_backfill::index_backfill_loop`, spawned unconditionally by
`SimCluster::new` since PR 2 — fixed by adding the identical respawn,
mirroring the `ttl_reaper_loop` respawn immediately above it. All 8 tests
in this module passed on the first full run after that fix
(`cargo test -p animusd --lib sim_cluster_backfill_seeder`); the trimmed
`tests/backfill_seeder.rs` residual test passed unchanged; the whole
`sim_cluster` tier passed 458/458 (2 ignored). `cargo fmt --all --check`
and `cargo clippy -p animusd --all-targets --all-features -- -D warnings`
both clean. See `crates/animusd/CLAUDE.md`'s matching C-10 PR 4 appendix,
`docs/engineering-lessons.md`'s matching entry (updated in place, not
duplicated), and `docs/roadmap.md`'s C-10 entry for the full account.

## 2026-09-09 amendment — Rung J, PR 5 (`sim_cluster_stream_backfill_seed_filter.rs`)

Converts `tests/stream_backfill_seed_filter.rs`'s two tests
(`backfill_seed_markers_never_surface_as_phantom_stream_events`,
`backfill_seed_markers_never_surface_from_sealed_shards_either`) 1:1 into
`crates/animusd/src/sim_cluster_stream_backfill_seed_filter.rs`, registered
in `lib.rs` beside `sim_cluster_index_ddl`; the original file is deleted
whole. No `dynamo.rs`/`index_drain.rs`/`sim_cluster.rs` change was needed —
PR 2's own `SimCluster::drive_backfill_seed` and C-07 PR 2's
`SimCluster::drive_stream_seal` already supply everything both scenarios
drive.

**Mapping.** Both scenarios keep the original's own structure: a streamed
table gets five pre-existing partitions, a GSI is added over the real
`UpdateTable` wire path (`dispatch_table_op`'s index sub-arm), four more
writes (two new partitions, a modify, a delete) are issued while the
backfill is in flight, and the backfill is driven to `ACTIVE` via the same
`converge_gsi_active` loop `sim_cluster_index_ddl.rs` established (a bounded
`drive_backfill_seed` + `drain_gsi` + `DescribeTable`-poll loop — never one
call assumed to finish). Scenario (a) never seals — the tablet's one shard
stays open for the whole run, so `drain_open_shard`'s stable-poll loop reads
straight off the hot tail. Scenario (b) calls `drive_stream_seal` once,
after the backfill converges, sweeping the whole pending backlog — seed
markers included, deliberately (`docs/streams-notes.md`: "hiding is a
serve-time decision") — into a sealed segment; `walk_lineage` then separates
sealed vs. still-open shards exactly as the original's own helper did. Both
assert the identical two properties the original asserted: zero delivered
records ever have the phantom shape (empty `Keys`, no images), and exactly
the 9 real writes (5 pre-existing + 2 new + 1 modify + 1 delete) are
delivered, each exactly once.

**What changed from the original, and why.** The real-socket test's own
`no_seal_knobs`/`tiny_seal_knobs` pair — tuned to *never* fire vs. fire on
any pending byte — has no sim analogue and needs none: this fixture never
spawns the periodic seal arm those knobs tune at all (C-07 PR 2's own
finding), so "never seals" is simply "never call `drive_stream_seal`" and
"seals aggressively" is one on-demand call. The original's sealed-path
scenario also carried a 60s converged-or-timeout polling loop, because it
raced a genuinely concurrent real-time writer against the periodic sealer;
under `SimCluster` every write is already committed, deterministically,
before the single `drive_stream_seal` call and the single lineage walk that
follows it, so no such loop is needed — `drive_stream_seal` itself already
loops `seal_now` to exhaustion of whatever is pending at the moment it's
called (documented on the method itself). The open-path scenario's own
30s/10-stable-poll convergence loop is kept in spirit (a bounded
stable-poll loop, `drain_open_shard`) purely as a defensive shape — nothing
in this fixture produces new records after the writes above complete, but
matching the original's own "don't assume one poll sees everything"
discipline costs nothing and generalizes if a future rung ever adds a
background writer this fixture doesn't have yet.

**Both tests convert cleanly — no residual.** Neither scenario needed a
capability this fixture lacks; the file is deleted whole, matching this
rung's own PR 3/4/6 precedent for a fully-convertible file.

`cargo test -p animusd --lib sim_cluster -- --test-threads=2`: 458 → 462
expected (+4, 0 regressions — PR 4 already landed the backfill-seeder
conversion ahead of this one, so the baseline going into this PR is 458,
not the 432 the module was drafted against) — gated by the maintainer
alongside this PR's own untrimmed-baseline-first discipline
(`stream_backfill_seed_filter.rs` green before conversion, the new sim
module green after, then the whole `sim_cluster` tier, fmt, clippy).

See `docs/roadmap.md`'s C-10 entry for the running per-PR record.
## 2026-09-09 amendment — Rung J, PR 6 landed (console GSI-DDL residue)

Brief as-built note, per the opener's own PR 6 plan above.

Converted the 3 GSI-related `console_table_config.rs` scenarios into
`sim_cluster_console_table_config.rs`'s (6)/(7)/(8), same names:
`add_gsi_rejects_an_unknown_attribute_type`, `add_gsi_records_a_declared_
attribute_type`, `add_and_drop_gsi_round_trip`, each with a pinned-seed
test plus its `_over_seeds` sibling (6 new `#[test]` fns). All three
reach `GenericConsoleBackend::add_gsi`/`drop_gsi` — already generic since
C-08 PR 2, previously dead-ending in `unsupported_by_generic_dispatch` —
which now falls through to PR 2's own index-change sub-arm and the real
`create_index`/`drop_index`. **No `lib.rs`/`console.rs`/`dynamo.rs`/
`sim_cluster.rs` change was needed at all**: `console_add_gsi_payload`'s
client-side type validation, `console_add_gsi_result`'s catalog re-read,
and `dispatch_table_op`'s sub-arm were all already in place from C-08 PR 2
and this rung's own PR 2 — this PR is pure test authorship on the
`sim_cluster_console_table_config.rs`/`console_table_config.rs` pair.

The first scenario (`add_gsi_rejects_an_unknown_attribute_type`) is pure
client-side validation — `console_add_gsi_payload`'s own attribute-type
check fails and returns before an `UpdateTable` is ever built, so it never
reaches `dispatch_table_op` and needs no backfill machinery. The second
(`add_gsi_records_a_declared_attribute_type`) adds a GSI to an *empty*
table and never asserts a `status`/`Backfilling` field, so it also needs
no convergence loop. Only the third (`add_and_drop_gsi_round_trip`)
populates the table first (so the added GSI genuinely starts `CREATING`)
and round-trips it to `ACTIVE` before dropping it — gained its own small
`converge_gsi_active_via_console` helper, `sim_cluster_index_ddl.rs::
converge_gsi_active`'s exact shape (drive `SimCluster::drive_backfill_
seed`/`drain_gsi` on the table's tablet leader, `sim_cluster_console.rs::
leader_of_table`, in a bounded loop of further calls) duplicated rather
than shared per this crate's per-file-fixture convention, but polling the
console's own `GET /console/api/tables/{name}` rather than `DescribeTable`
since the console surface is what this module tests.

`tests/console_table_config.rs` trimmed from 4 tests to the 1 the opener's
Ground-truth section always expected to stay: `table_detail_shows_pitr_
status_and_backups` (`UpdateContinuousBackups` has no generic-dispatch arm
in either `dispatch_item_op` or `dispatch_table_op`, and the test also
depends on the real `pitr_snapshot_loop`'s wall-clock-timed capture driver
— issue #593's own race, which the retained test guards against with its
own poll). No helper function was removable: `dynamo`/`console`/`json`/
`assert_no_cluster_shape` are all still used by the retained test.

**No product bug found.** Every response shape this PR's assertions rely
on (`{"gsi": ...}`/`{"ok": true}` wrapping, `GsiDetail`'s bare `name`/
`hash_attribute`/`sort_attribute`/`status`/`projection` fields, the
`gsis`/`ttl`/`stream` bare-array/bare-object shape on `TableDetail`) was
cross-checked against `animus-node/src/console.rs`'s `table_api_response`/
`wrap_json`/`ok_json`/`GsiDetail` before being written into the new
scenarios, since this PR (per its own worktree constraints) could not run
`cargo` itself — see `docs/engineering-lessons.md`'s matching entry on
verifying `SimCluster` fixture code this way when a session cannot
compile.

**Gates, run in the main tree after rebase onto PR 5's landed 576d99ee**:
`cargo build -p animusd --tests`, `cargo fmt --all --check`, and `cargo
clippy -p animusd --all-targets --all-features -- -D warnings` all clean
with no fixes needed; `cargo test -p animusd --lib
sim_cluster_console_table_config -- --test-threads=2` (16 tests: the 10
pre-existing plus these 6 new) and `cargo test -p animusd --test
console_table_config` (the trimmed 1-test residual) both green; the whole
`cargo test -p animusd --lib sim_cluster -- --test-threads=2` tier passed
468/468 (2 ignored, 0 failed — the expected 462 + 6), peak RSS ~970 MB
(`/usr/bin/time -v`, `Maximum resident set size` 992764 KB), consistent
with every prior rung's own no-leak trajectory. See `docs/roadmap.md`'s
C-10 entry and `crates/animusd/CLAUDE.md`'s own "index DDL beyond plain
`CreateTable`" appendix for the mapping table and residual-inventory
update.

## 2026-09-09 amendment — Rung J closed (C-10 complete)

PR 7 is what its own row above and the PR-series amendment promised: no
source, test, or `Cargo` change — this ADR's own J-row and this amendment,
`docs/roadmap.md`'s C-10 entry, and `crates/animusd/CLAUDE.md`'s
consolidated index-DDL section are the entire PR. Rung J (C-10) is now
**closed**: `UpdateTable`'s GSI add/drop half is deterministically
fault-injectable and seed-replayable under `SimCluster`/`SimEnv`, the
largest of the five groups Rung H's own close-out left unowned and, per
Rung I's own close-out, the only one of those five with a rung against it.

**What the rung set out to do.** `dispatch_table_op`'s `UpdateTable` arm
rejected any call carrying an index change outright
(`unsupported_by_generic_dispatch`), so adding or dropping a GSI on an
already-populated table (ADR 0045) was provable only over `ProdEnv`'s real
wire — nine files, 33 tests by this rung's own grep-verified recount. The
plan (this amendment's own PR 1 opener, above) was the same
widen-then-add-a-generic-entry-point template every rung since D3 had
already validated, applied one layer down from `update_table_throughput`'s
own already-landed precedent: `create_index`/`drop_index`/
`set_index_status`/`drop_table_index` and the `index_drain` backfill-seeder
trio needed only signature/clock-call widening (every `ClientCtx` method
they call was already generic), the GSI-drain side was already generic
since D3 PR 3b, and the backfill completion aggregator's host trait was
already generic too — leaving only an 18-line `animusd` wrapper concrete,
this rung's own smallest-lift finding.

**PR-by-PR, what landed.** PR 1 (docs-only opener, #789): the plan, the
grep-verified ground truth, the nine-file/33-test unlock table. PR 2
(groundwork, #790): the four `dynamo.rs` functions and the three
`index_drain` seeder functions widened to `<E: Env, R: RelayClient>`/
`<E: Env>`; a fourth `UpdateTable` sub-arm on `dispatch_table_op`
(`IndexUpdate::Create`/`Delete` → `create_index`/`drop_index`); the
`index_backfill` wrapper widened; `SimCluster::new`/`::restart` spawn
`index_backfill::index_backfill_loop` unconditionally per node; a new
`SimCluster::drive_backfill_seed(node, table)` mirroring `drain_gsi`'s own
shape; a fourth `tokio::time`-body-in-an-already-generic-signature finding
(`clear_backfill_cursor`, the fourth recurrence of that lesson in this
crate) fixed in the same PR. `cargo test -p animusd --lib sim_cluster`
428 → 432. PR 3 (#791): `sim_cluster_dynamo_update_table_index.rs`
converting `update_table_create_index.rs` (4), `update_table_drop_index.rs`
(4), and `dynamo_gsi_drain.rs` (1) — 9 tests, all three files deleted whole,
18 `#[test]` fns with two documented sequenced-analogue substitutions and
one stronger physical-reclaim substitution — 450. PR 4 (#792): `sim_cluster_
backfill_seeder.rs` converting 4 of `backfill_seeder.rs`'s 5 scenarios (8
tests); `split_during_backfill_converges_with_correct_final_gsi` exercised
its own explicit license and stayed `ProdEnv`, alone in the trimmed file —
`SimCluster` spawns no `change_consumer_loop`, so proving it would mean
hand-interleaving three separately-timed on-demand primitives with no way,
offline, to verify convergence; a real gate run found and fixed a genuine
fixture gap (`SimCluster::restart` respawned every other always-on loop but
not `index_backfill_loop`) — 458. PR 5 (#793): `sim_cluster_stream_backfill_
seed_filter.rs` converting `stream_backfill_seed_filter.rs`'s 2 tests, file
deleted whole, no residual — 462. PR 6 (#794): the 3 GSI-related
`console_table_config.rs` scenarios into `sim_cluster_console_table_
config.rs` (6 tests with `_over_seeds`), file trimmed to its 1 out-of-scope
PITR test (`table_detail_shows_pitr_status_and_backups`, a separate,
pre-excluded residual per the opener's own Ground truth section, never
counted against this rung's 33-test scope) — 468. PR 7 (this PR): the
close-out.

**The final `ProdEnv` residue, in full** (from the opener's own nine-file/
33-test scope — `schema_ddl_relay.rs`'s 7 tests are a separate, deliberately
unclaimed piece of work, never part of this scope, per the opener's own
Ground truth section):

| File | Kept tests | Reason |
|---|---|---|
| `dynamo_index_scan.rs` | 5 (all) | Frozen behind open flake issue #418, untouched by this series per its own off-limits list |
| `index_backfill.rs` | 3 (all) | Frozen behind open flake issue #592, untouched by this series per its own off-limits list |
| `dynamo_index_writes.rs` | 6 (all) | Frozen behind open flake issue #610, untouched by this series per its own off-limits list |
| `backfill_seeder.rs` | `split_during_backfill_converges_with_correct_final_gsi` | `SimCluster` spawns no `index_drain::change_consumer_loop`; proving this scenario would mean hand-interleaving three separately-timed on-demand primitives (`drive_backfill_seed`/`drain_gsi`/`drive_inplace_split_cutover`) every round with no way, offline, to verify the always-on completion aggregator can't race the cutover propose or that the post-cutover per-child resweep converges within an unrun round budget — explicitly licensed by this rung's own opener |
| `console_table_config.rs` | `table_detail_shows_pitr_status_and_backups` | Asserts `TableDetail.pitr`/`backups`; `UpdateContinuousBackups` has no generic-dispatch arm and the test needs the real wall-clock-timed `pitr_snapshot_loop` — a separate, pre-existing PITR residual never in this rung's own scope |

16 tests total (14 frozen + 1 licensed backfill residual + 1 pre-excluded
PITR residual) — **one more than the opener's own PR 7 plan predicted**
(above, "15 total, matching this opener's own prediction unless a PR finds
a genuine new blocker"). The deviation is `backfill_seeder.rs`'s licensed
residual: the opener's own per-file table already flagged this outcome as
possible ("`split_during_backfill` may stay `ProdEnv` if it does not
converge cleanly"), and PR 4 found that it did not, so this is a stated,
pre-licensed finding, not a silently-matched prediction nor a new blocker.
18 tests converted across PRs 3-6 (9 + 4 + 2 + 3), the remainder of the
opener's own 33-test scope once the 14 frozen tests are set aside.

**Mechanism lessons.**

1. **A generic signature does not prove a seam-clean body, fourth
   recurrence.** `index_drain::clear_backfill_cursor<E: Env>` already
   carried a generic signature but its commit-wait loop still called bare
   `tokio::time::Instant::now()`/`tokio::time::sleep`, found immediately by
   the first sim smoke run (`SimEnv` has no real Tokio reactor, so the call
   panics on first reach) — the same family as `seal_now`,
   `admin_transfer_control_leadership`, and `recovery_grace_now_ms`
   (three) before it. The general form, restated once more: a function's
   own type parameters say nothing about its body until it is actually run
   under `SimEnv`, not just read.
2. **A fixture `restart` that respawns only *some* of a process's always-on
   loops silently weakens every crash scenario that depends on the missing
   one.** `SimCluster::restart` respawned `heartbeat_loop`/
   `backup_janitor_loop`/`segment_janitor_loop`/`ttl_reaper_loop`/
   (conditionally) `auto_split_loop` but not `index_backfill_loop`, spawned
   unconditionally by `SimCluster::new` since this rung's own PR 2 — found
   only because PR 4's own crash-and-restart-mid-backfill scenario actually
   exercised the gap, not by inspection. Every future rung that adds a new
   always-on `SimCluster::new` spawn must add the matching `::restart`
   respawn in the same PR, or every "crash mid-X" scenario for that loop
   will silently pass for the wrong reason (the loop never restarts, so
   there's nothing left to race against).
3. **A `SimCluster` operation runs to completion in one synchronous call, so
   a real-thread race becomes a sequenced pair of actions — proving the
   same property, not the same mechanism.** Three separate PR 3/4
   substitutions (an in-flight-backfill-vs-drop race, a crash-mid-cascade,
   sequenced-not-raced live writes during backfill) all converted a
   `tokio::join!`/real-OS-thread race into "issue action A after exactly one
   partial primitive call, before action B" — the general shape every prior
   rung's own sequenced-analogue substitutions already used, restated here
   because this rung needed it three times in two PRs, the highest density
   yet.
4. **Recompute every baseline count after a rebase over a sibling PR's own
   landed work**, restated from this rung's own PR 5 finding: PR 5's module
   was drafted against a 432-test baseline but landed after PR 4's own
   458-test baseline, so its "+4" claim only holds relative to the number
   actually in the tree at merge time, never the number the module was
   originally drafted against.

**Test-count trajectory** (`cargo test -p animusd --lib sim_cluster --
--test-threads=2`, whole tier): 428 (C-09 baseline) → 432 (PR 2, +4) → 450
(PR 3, +18) → 458 (PR 4, +8) → 462 (PR 5, +4) → **468 passed, 0 failed, 2
ignored** (PR 6, +6) — 40 tests added across the rung, every PR's own gate
run reporting green with no regressions and a consistent no-leak RSS
trajectory (~958 MB → ~970 MB peak across PRs 2 and 6, the two PRs with a
recorded `/usr/bin/time -v` figure).

**What remains unowned after C-10**, updating the C-09-closing residual
inventory now that index DDL is no longer on it: the throttle-metric
counters, the control/data role split, `--config` bring-up, and node
assembly/raw `ClientRequest` — four groups, none with a rung against it
today. Of these, the throttle-metric counters are the smallest (1 file/6
tests per the D3-closing class-D breakdown, unchanged since), need no new
deployment-shape fixture, and are the recommended next rung (advisory only
— the maintainer sequences); the control/data role split needs a per-node
role concept this fixture does not have yet, a larger lift reserved for a
later rung; `--config` bring-up and node assembly/raw `ClientRequest` are,
on current evidence, likely permanent process-boundary residue in the same
family as this crate's real-thread-liveness/real-disk/real-crypto classes
(A/B/C) rather than a `SimCluster`-dispatch gap — a call for the maintainer
to assess and close (or take on) rather than a decided outcome of this
close-out.

**Website: no change needed, verified again at this close.** A targeted
grep of `website/*.html` for "secondary index"/GSI/`UpdateTable`/"global
secondary" found only claims already true of production behavior:
`compatibility.html` and `docs.html` both describe `UpdateTable` adding or
dropping a GSI on a populated table with online backfill as supported
today; `index.html` and `how-it-works.html` list "secondary indexes, added
and dropped online with backfill" as already working; `docs.html`'s own
consistency-model table states GSIs are "eventually consistent — maintained
asynchronously from the change log," matching the backfill/drain mechanism
this rung exercised. None of these claims names `SimCluster` testing
specifically, and this rung changed no production dispatch path or
wire-observable behavior — the same reasoning every prior rung's opener and
close-out already applied. No stale claim found.

**Gates**: none — documentation only, no `cargo` command run, `git diff
--stat` shows only the docs files this PR touches.

**Docs**: this amendment (closing Rung J); `docs/roadmap.md`'s C-10 entry
closed, its wave-11 sequencing row marked closed, C-11 noted as next (plan
drafted, awaiting the maintainer's sequencing); `crates/animusd/CLAUDE.md`'s
five PR 2-6 appendices folded into one consolidated "index DDL beyond plain
`CreateTable` under SimCluster" section with the residual inventory
corrected and the "PR 7 pending"/"in progress" wording removed;
`docs/engineering-lessons.md` already carries every finding this rung's own
PRs made — no new entry needed at this close.

## 2026-09-09 amendment — Rung K (post-C-10): throttle-metric counters under `SimCluster` (C-11), PR 1 (this docs-only opener)

**Why this group.** Rung J's own close-out ("Rung J closed", above) named
four groups left unowned after C-10 — the throttle-metric counters, the
control/data role split, `--config` bring-up, and node assembly/raw
`ClientRequest` — and recommended the throttle-metric counters first:
smallest (1 file/6 tests, unchanged since the D3-closing class-D
breakdown), and, unlike the control/data role split, needing no new
`SimCluster` deployment-shape fixture. That recommendation is advisory
only; **the maintainer has not sequenced C-11 explicitly** the way C-09
and C-10 were sequenced in so many words by their own opener amendments.
This PR proceeds under that recommendation as a stated assumption — see
"Two open decisions" below — rather than a confirmed sequencing decision,
so it is flagged rather than silently treated as settled.

**The group's documented blocker turned out stale.** Every prior mention
of this residual — Rung J's own close-out, `sim_cluster_dynamo_update_
table.rs`'s own module doc, and `crates/animusd/CLAUDE.md`'s "Test
coverage" section — repeats the same claim: `ThrottledWrites`/
`ThrottledReads` never increment under `SimCluster` because its nodes
carry no real metrics sink. A direct grep of the current tree shows this
is no longer true, and — per `sim_cluster.rs:1442-1459`'s own comment,
landed by rung D2 PR 1 — was **never** true of the claim's own stated
reason: `SimCluster::new` has built every node with a real `DataRole` (a
real per-node `MetricsHandle`, not a no-op stub) since D2 PR 1, well
before either stale doc was written. The blocker these two docs describe
does not exist today; this PR corrects both (see "The two doc-comment
fixes" below) as part of the opener, per this rung's own binding rule
that a docs-only PR that finds a stale claim fixes it in the same PR
rather than deferring it.

**Ground truth**, from a read-only grep of the current tree, not the plan
draft that proposed it (`/tmp/.../scratchpad/c11_throttle_metrics_plan.md`
— every line-number claim in that draft was independently re-checked
below; three were off by the small margin later insertions in the same
files always produce, noted where it matters):

- `crates/animusd/tests/dynamo_throttling.rs` still holds all 6 tests this
  group scopes: `batch_write_item_sheds_throttled_rows_into_unprocessed_
  items` (`:445`), `batch_get_item_sheds_throttled_keys_into_unprocessed_
  keys` (`:492`), `transact_write_items_cancels_with_throttling_error`
  (`:538`), `a_forwarded_write_is_throttled_on_the_leader` (`:605`),
  `admin_metrics_reports_nonzero_throttled_counters` (`:652`), and
  `cluster_wide_throttle_default_is_overridden_by_a_tables_own_throughput`
  (`:740`) — confirmed by `grep -n '^async fn' tests/dynamo_throttling.rs`,
  exact line numbers, not estimates.
- `dynamo::kind_write_item_at_leader<E: Env, R: RelayClient>`
  (`dynamo.rs:9032`, not `:9000` as the plan draft estimated) is already
  generic and increments `Metric::ThrottledWrites` via
  `ctx.data().raftkv_metrics.incr(..)` at `dynamo.rs:9069` (not `:9037` —
  the draft's own line undercounted by the doc-comment lines above the
  function). Two more leader-side write paths do the identical thing:
  `write_path.rs:327` (the ADR 0049 fast marker arm) and
  `txn_coordinator.rs:134` (`Transact`'s own per-action check) — neither
  named by the plan draft, both confirmed generic and both already
  reachable through `SimCluster`'s existing wire dispatch.
- The read-path `ThrottledReads` increment lives in
  `read_path.rs:99`, inside `ClientCtx::throttle_precharge_read` — **not**
  in `dynamo.rs` as the plan draft's phrasing implied. It reads `if let
  Some(data) = self.data.as_ref() { data.raftkv_metrics.incr(Metric::
  ThrottledReads); }` — a defensive `Option` check (this function is
  reachable from a control-only node's routing path in production, which
  has no `DataRole`), not evidence the counter is unreachable under
  `SimCluster` — `SimCluster`'s own nodes always carry `Some(DataRole
  { .. })`, so the guard is satisfied every time here.
- `dynamo::run_transact<E: Env, R: RelayClient>` is at `dynamo.rs:5171`
  (not `:5139`), confirmed generic; its `TransactWriteItems` dispatch arm
  is `dynamo.rs:850`. `dispatch_item_op<E: Env, R: RelayClient>`
  (`dynamo.rs:1069`) already routes `BatchGetItem` (`:1247`) and, via the
  shared `Operation::BatchGetItem { .. } | .. | Operation::
  BatchWriteItem { .. }` match arm (`:763-765`), `BatchWriteItem` too.
- `lib.rs`'s `DataRole` (`:9656`) carries `raftkv_metrics: MetricsHandle`;
  `ClientCtx::data` field is `Option<DataRole>` (`:9724`); `ClientCtx::
  data(&self) -> &DataRole` (`:9985`) panics with `"ClientCtx::data called
  on a control-only node (ADR 0035 PR3)"` when `None` — the panic message
  itself is the confirmation that a data-role node, `SimCluster`'s every
  node included, always returns `Some`. `ClientCtx::metrics_json`
  (`:10155`) and `::throttle_snapshot` (`:10205`) both exist and are
  already used by `/admin/metrics`.
- `admin::metrics_view<E: Env, R: RelayClient>` (`admin.rs:1982`) is
  generic; `animus-node/src/admin.rs:52`'s `("GET", "/admin/metrics") =>
  (200, host.metrics_view().await)` is the dispatch site the generic path
  already reaches.
- `sim_cluster.rs` builds every node with a real `DataRole` twice — the
  main multi-node constructor (`data: Some(DataRole { raftkv_metrics:
  node_metrics[i].clone(), .. })`, `:1460`, with `node_metrics[i]` the
  same per-node sink `controls[i]` was built with, matching production's
  "one node, one metrics sink" contract) and a second single-node
  constructor (`:3557`) — plus a `ThrottleTracker::new()` at `:1504` (and
  `:3587`). `SimCluster::set_throttle_defaults` (`:2121`, not `:2073` as
  the draft estimated) and `::set_throttle_defaults_all` (`:2134`, not
  `:2086`) already exist. `SimCluster::admin` (`:933`) already drives
  `GET /admin/metrics` end to end — `sim_cluster_admin.rs:886`'s
  `run_admin_metrics_surfaces_control_plane_counters` already calls it,
  proving the whole `metrics_view` round trip works under `SimEnv` for
  the **control**-plane counters that function also renders; only the
  **throttle** counters specifically are unproven there today.
- Wire-level sim siblings already exist for the exact operation family
  this rung needs: `sim_cluster_dynamo_batch_get.rs`,
  `sim_cluster_kind_batch_outcome.rs`, `sim_cluster_dynamo_transact.rs` —
  each is the template PR 2 follows for `BatchWriteItem`/`BatchGetItem`/
  `TransactWriteItems` dispatch through `SimCluster::dynamo`, minus the
  throttle-burst setup this rung adds.
- **The two stale module docs, confirmed stale by direct comparison with
  the code above**: `sim_cluster_throttle.rs:10-12` currently reads
  "`SimCluster`'s every node runs with `data: None` ... `ThrottledWrites`/
  `ThrottledReads` therefore never increment here (the metric-recording
  sites all gate on `self.data.as_ref()`)" — false on both counts: no
  node in `sim_cluster.rs` is built with `data: None` (`grep -n 'data:
  None' sim_cluster.rs` returns zero hits; both constructors use `data:
  Some(DataRole { .. })`), and the "gate on `self.data.as_ref()`" claim
  describes only the read-path site (`read_path.rs:99`) — the two
  write-path sites (`dynamo.rs:9069`, `write_path.rs:327`,
  `txn_coordinator.rs:134`) call `ctx.data()` unconditionally (the
  panicking accessor), never the `Option`-gated form. `sim_cluster_
  dynamo_update_table.rs:27-29` makes the identical claim, citing the
  first file's doc as its source — both are corrected by this PR.
  `sim_cluster.rs:1427`'s own `AdminInfo` comment ("No `DataRole` on any
  node in this fixture (`data: None` below)") carries the same stale
  claim about a *different* field's history — noted here for completeness
  since it is the same family of drift, but `sim_cluster.rs` is not in
  this rung's allowed-edits list, so it is left for whichever future PR
  next touches that file, not fixed here.

**Test inventory** (`tests/dynamo_throttling.rs`, 6 tests, all counts
confirmed by `grep -n '^async fn' tests/dynamo_throttling.rs`):

| Test | Line | Disposition |
|---|---|---|
| `batch_write_item_sheds_throttled_rows_into_unprocessed_items` | 445 | Converts (PR 2) — wire-shape (`UnprocessedItems`) |
| `batch_get_item_sheds_throttled_keys_into_unprocessed_keys` | 492 | Converts (PR 2) — wire-shape (`UnprocessedKeys`) |
| `transact_write_items_cancels_with_throttling_error` | 538 | Converts (PR 2) — wire-shape (`ThrottlingError` cancellation reason) |
| `a_forwarded_write_is_throttled_on_the_leader` | 605 | Converts (PR 2) — wire-shape, forwarded-hop throttle check |
| `admin_metrics_reports_nonzero_throttled_counters` | 652 | Converts (PR 3) — the counter assertion itself, into `sim_cluster_admin.rs` |
| `cluster_wide_throttle_default_is_overridden_by_a_tables_own_throughput` | 740 | Stays `ProdEnv` — targets `run_node_with_cluster_settings` (the `--throttle-*`/`cluster_settings` config-file/CLI bring-up path), a real process-boundary surface no `SimCluster` fixture reaches; expected permanent residual |

**The PR series** (4 PRs, stacked on the C-10 close-out):

- **PR 1 (this amendment).** Docs only: this ADR amendment, the rung-table
  row K above, `docs/roadmap.md`'s C-11 entry moved from candidate to
  open with this plan summary, `crates/animusd/CLAUDE.md`'s residual-
  inventory pointer, and the two stale module-doc corrections in
  `sim_cluster_throttle.rs`/`sim_cluster_dynamo_update_table.rs` (doc
  comments only, no code). **Gates:** none — documentation only, no
  `cargo` command run.
- **PR 2 — `sim_cluster_dynamo_throttle.rs`.** New sibling module
  converting the four wire-shape tests (`batch_write_item_sheds_..`,
  `batch_get_item_sheds_..`, `transact_write_items_cancels_..`,
  `a_forwarded_write_is_throttled_on_the_leader`), each calling
  `SimCluster::set_throttle_defaults_all` before driving the burst that
  trips the limit — the same setup shape `sim_cluster_throttle.rs`'s own
  existing scenarios already use, applied to the wire-level operations
  instead of the direct `put`/`get` bypass. `tests/dynamo_throttling.rs`
  stays **untrimmed** after this PR — trimming waits for PR 3 so the
  file's own before/after gate always brackets exactly one PR's worth of
  test movement, the same discipline every prior rung's gate list used.
  **Gate:** untrimmed `cargo test -p animusd --test dynamo_throttling`
  first (baseline, 6 tests green) → the new sim module green → untrimmed
  `dynamo_throttling.rs` re-run (still 6, unchanged — nothing trimmed yet)
  → `cargo test -p animusd --lib sim_cluster -- --test-threads=2` (whole
  tier, anchored RSS sampler — `pgrep -f '^/home/user/animus-db/target/
  debug/deps/animusd-'`, never an unanchored substring match, per the
  #753 postmortem) → `cargo fmt --all --check` → `cargo clippy -p animusd
  --all-targets --all-features -- -D warnings`.
- **PR 3 — Admin-metrics residue.** Add
  `admin_metrics_reports_nonzero_throttled_counters`'s scenario to
  `sim_cluster_admin.rs` (asserting the same nonzero `ThrottledWrites`/
  `ThrottledReads` fields `run_admin_metrics_surfaces_control_plane_
  counters` already reads `/admin/metrics` for, per the Ground truth
  section's own finding that only the throttle counters specifically were
  unproven there); trim `tests/dynamo_throttling.rs` to its one residual,
  `cluster_wide_throttle_default_is_overridden_by_a_tables_own_
  throughput`. **Gate:** untrimmed `dynamo_throttling.rs` (still 6, the
  PR 2 baseline) → new `sim_cluster_admin.rs` scenario green → trim to 1
  → re-run `dynamo_throttling.rs` (1 test) → whole `sim_cluster` tier +
  RSS sampler → fmt → clippy.
- **PR 4 — Docs close-out.** This rung marked "closed": a "Rung K closed"
  amendment with the PR-by-PR account, before/after `sim_cluster` test
  counts, the final one-test residue, the gate transcript, and
  `docs/roadmap.md`/`crates/animusd/CLAUDE.md` updated to match. **Gates:**
  none — documentation only.

**Binding rules.** **Zero production signature changes — the rung's own
distinguishing property.** Unlike every prior rung, nothing in this
group's production code needs widening: `kind_write_item_at_leader`,
`run_transact`, `dispatch_item_op`, the read-path precharge site, and
`metrics_view` are all already `<E: Env, R: RelayClient>` (or `<E: Env>`)
generic today, confirmed above — this rung is pure test-fixture work (two
new sim modules, one trimmed real-socket file), with no `dynamo.rs`/
`lib.rs`/`admin.rs` production code touched by PRs 2-3 at all. No
`HashMap`/`tokio::time` may appear in either new sim test module — both
are ordinary `#[cfg(test)]` code exercising already-generic production
paths, so this is a review-by-hand discipline (workspace-wide
`disallowed-methods`/`disallowed-types` lints apply to test code the same
as production code; `animusd`'s package-level carve-out does not need
invoking here since nothing in this rung's own new code calls a
disallowed method). **Frozen/off-limits files untouched**: `streams_e2e.rs`
(#298), `dynamo_index_scan.rs` (#418), `index_backfill.rs` (#592),
`batch_write.rs` (#601), `dynamo_index_writes.rs` (#610),
`operator`/`website` doc-drift (#615), `split_placing_two_replica_diff_
e2e.rs` (#619, #622, #670), `dynamo_streams.rs`'s periodic-seal flake
(#626), `cp_cross_process.rs` (#627), `client_cancellation_tests` (#638),
the admin DNS-name issue (#662), and the Raft `voted_for` durability issue
(#667) — none of these fourteen open issues names a file this rung reads,
edits, or converts (`dynamo_throttling.rs`, `sim_cluster_throttle.rs`,
`sim_cluster_dynamo_update_table.rs`, `sim_cluster_admin.rs`, and the new
`sim_cluster_dynamo_throttle.rs`), confirmed by direct title comparison
above; this series does not touch any of them.

**Risks.** The stale-doc finding itself is the rung's main risk: PR 2/3's
own scenario authors must verify the counter actually increments (a
direct `/admin/metrics` read or a `throttle_snapshot` check showing a
nonzero value) before writing an assertion against it, rather than
trusting either stale doc's prior "never increments" claim — the exact
mistake this opener itself avoided only by grepping the constructor
before writing this amendment. Low risk otherwise: no new capability, no
new fixture primitive beyond the setup shape `sim_cluster_throttle.rs`
already validates, and the group's own size (1 file, 6 tests, one of them
already licensed to stay `ProdEnv`) is the smallest of any rung since D3.

**Two open decisions for the maintainer**, restated from Rung J's own
close-out and not resolved by this opener: (a) **whether C-11 is next at
all** — this PR proceeds on Rung J's own close-out recommendation as a
stated assumption, not an explicit sequencing instruction of the kind
Rung I and Rung J each received in so many words; (b) whether to
**assess-and-close** `--config` bring-up and node assembly/raw
`ClientRequest` as permanent `ProdEnv` process-boundary residue (Rung J's
own close-out already flagged both as "likely permanent ... rather than a
`SimCluster`-dispatch gap, a call for the maintainer to assess and close
... rather than a decided outcome"), versus opening a rung for either; and
whether the **control/data role split** — the one remaining group needing
a genuinely new `SimCluster` deployment-shape fixture — becomes a later
rung once this one closes. None of these three is decided by this PR;
they are carried forward unchanged.

**Website:** no change needed — this rung touches no wire-observable
behavior; every production dispatch path stays byte-identical (see
"Binding rules" above — zero production signatures change). Verified by a
targeted grep of `website/*.html` for "throttl"/"provisioned throughput"/
`ThrottledWrites`: all claims describe already-true production throttling
behavior (per-table provisioned throughput, `ProvisionedThroughputExceeded`
on a burst), none names `SimCluster`/`/admin/metrics` testing specifically.

**Docs:** this amendment (opening Rung K, open); `docs/roadmap.md`'s
C-11 entry moved from candidate to open with this plan summary and the
same two-open-decisions note; `crates/animusd/CLAUDE.md`'s residual-
inventory pointer from the throttling residual to this amendment; the two
stale module-doc corrections (`sim_cluster_throttle.rs`,
`sim_cluster_dynamo_update_table.rs`).
