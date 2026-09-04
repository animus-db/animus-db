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
