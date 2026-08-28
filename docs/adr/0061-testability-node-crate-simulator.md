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
| B5 | `clippy.toml` `disallowed-methods` per Decision 4, with justified allows |
| B6 | Refresh ADR 0003's "known fidelity limits" — it undersells what shipped (clock skew, disk faults) and still lists gaps B2/B3 close |

B1 lands first so that everything after it (including every Phase D corpus)
is written against the shared harness rather than adding a twelfth copy.

### Phase C — the `animus-node` carve-out

The long pole. Ordered so each rung compiles and ships green, leaf-first,
brain-last.

| Rung | Work |
|---|---|
| C1 | Create `animus-node` with **no** `tokio::net`/`ProdEnv` in its manifest. Move the wire types (`ClientRequest`/`ClientResponse`/`Surface`/`is_relayable_command`), `topology.rs`, and A6's `decide` module. Boundary established and compiler-enforced from the first commit |
| C2 | Genericize and move the leaf background loops: `ttl_reaper`, `backup_janitor`, `pitr_janitor`, `segment_janitor`, `backup_completion`, `index_backfill`. Each is small, self-contained, and paced by `tokio::time` today — `env.sleep()`/`env.now()`/`env.spawn_task()` instead |
| C3 | `ControlHandle<E>`; move relay/forwarding (`relay_request`, `lib.rs:17617-17650`) off raw `TcpStream` onto the multiplexed `Network` (ADR 0026). This is what lets a multi-node cluster talk inside `SimEnv` |
| C4 | The HTTP edges per Decision 2: parsing and dispatch become pure bytes-in/bytes-out in `animus-node`; `animusd` keeps the accept loops |
| C5 | The brain: split `impl ClientCtx` into `read_path`, `write_path`, `txn_coordinator`, `forwarding`, `schema`; genericize over `E: Env`. The heaviest rung, but A6 has already lifted the pure predicates out of it |
| C6 | Node assembly: `Node<E>`/`BoundNode<E>`/`BoundControlNode<E>`/`BoundDataNode<E>` move; `animusd` shrinks to binary, config, listeners, lifecycle, and the one `ProdEnv` construction. The `start_cluster*`/`run_node*` harness functions move to test support where they belong |

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
