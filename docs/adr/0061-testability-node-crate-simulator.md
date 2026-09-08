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
| F | Post-C-04: Transact/PartiQL `SimCluster` dispatch (C-06) — the two named D2 residuals (Transact, PartiQL), never claimed by any D3/D4 rung. **PR 1 (this amendment) landed 2026-09-07**; PRs 2-7 open — see the matching 2026-09-07 "Rung F" amendment below and `docs/roadmap.md`'s C-06 entry |

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
