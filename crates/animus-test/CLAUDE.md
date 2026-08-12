# CLAUDE.md — animus-test

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

Elle/Jepsen-style history recording and consistency checking. A library other
crates' tests use to record operation histories and assert correctness
properties; it also hosts cross-crate fault sweeps.

## Entry points

`src/` is the small reusable library; the rich content lives under `tests/`.

- `src/history.rs` — `Recorder` (`invoke`/`ok`/`fail`/`info`), `History`, `Mop`
  (list-append `Append`/`Read`), `Outcome`.
- `src/check.rs` — `check_cycles` (serializability), `check_durability`,
  `check_convergence`; each returns a `CheckReport` carrying the run seed.
- `src/export.rs` — `to_json`, `to_edn` (Jepsen/Elle).
- `tests/support/mod.rs` — the shared Accord harness: the declarative
  `Scenario`/`NemesisAction` model, `run_scenario`, the frozen `corpus()`
  generator, and the depth/breadth expansions (`seed_expand`,
  `corpus_extended`). See the corpus sections below.

Env knobs at a glance (details in the sections below):

| Knob | Default | Effect |
|------|---------|--------|
| `ANIMUS_SEED` | unset | replay one failed sim run from its printed seed (repo-wide convention) |
| `ANIMUS_CORPUS_SEEDS=K` | 1 | K seed variants per Accord-corpus cell (depth) |
| `ANIMUS_CORPUS_FULL=1` | off | extended Accord-corpus dimensions (breadth) |
| `ANIMUS_RAFTKV_SEEDS=K` | 1 | K seed variants per raftkv-corpus cell |
| `ANIMUS_RAFTKV_LSM=1` | off | run the whole raftkv corpus over `LsmEngine<SimEnv>` |
| `ANIMUS_TXN_SEEDS=K` | 1 | K seed variants per multi-tablet transaction-corpus cell (ADR 0018) |

## What's non-obvious

- **Indeterminate outcomes (e.g. a timeout) MUST be recorded `info`, never
  `fail`.** `fail` asserts the op definitely did not happen; misclassifying
  makes a checker draw false conclusions. The harnesses follow this: a transaction
  whose commit could not be confirmed (a crash/partition) is `info`.
- `check_cycles` is the core Elle idea: recover each key's append order from
  observed reads, build wr/ww/**rw** edges, run Tarjan SCC. The `rw`
  anti-dependency rule (a read precedes the appenders of values it did *not*
  observe) is what catches write skew — keep it if you refactor.
- AP checks assume `R + W > N`: durability = every `ok` append is in a final
  quorum read; convergence = two final quorum reads agree (not raw per-replica
  state — there's no read-repair yet).
- `CheckReport.seed` exists so a flagged anomaly is replayable; surface it in
  assertion messages.

## Tests

`cargo test -p animus-test` — `cycle_checker.rs` (hand-built histories) + the
Accord serializability corpus + the CP-plane corpus below. (v1 is CP-only,
ADR 0019: the AP data-plane test files — `ap_data_plane.rs`, `fault_sweep.rs`, the
assembled-stack `end_to_end.rs` — were removed with `animus-data`, as was the
`Frontier` topology.)

### Elle-against-Accord + the frozen scenario corpus (ADR 0014)

The Accord-targeted suite exercises `check_cycles` under contention:

- `negative_control.rs` — the **teeth proof**: hand-built non-serializable
  histories (write skew G2, circular read dep G1c, a 3-txn cycle) the checker
  *must* reject, plus serializable ones it must accept. Run/read this before
  trusting any green corpus run.
- `support/mod.rs` — the shared harness: assembles a **pure-Accord** replica set
  (`AccordNode::start` — local execution + versioned-snapshot reads, the
  serialization authority), drives **concurrent conflicting** multi-key read/write
  transactions over a small shared key space as **genuine black-box list-append**
  (real list values stored and observed — see below), records an Elle list-append
  `History`, and runs all three checkers. Also defines the declarative `Scenario` /
  `NemesisAction` model, the `run_scenario` runner, and the frozen `corpus()`
  generator. (The former `Frontier` topology + the data-plane scaffolding were
  removed in v1.)
- `elle_accord.rs` — Accord under contention: a no-fault contended run + seed
  sweep + a determinism check, with teeth-guards asserting the run genuinely
  contended.
- `corpus.rs` — the parametric runner over the **frozen, named, seeded** corpus
  (119 base scenarios: fault type × timing × workload shape × cluster shape, plus
  baselines and compound lossy/overlapping scenarios), a coverage guard, a
  non-vacuity guard, a determinism check, and the seed-expansion / extended-tier
  structural guards. The headline `corpus_is_consistent` asserts **serializability**
  (`check_cycles`) over the env-scaled `corpus()`; the structural guards run the
  env-independent `corpus_base()`. Coverage scales by two env knobs (depth/breadth —
  see below).

### Elle-against-Raft: the leaderful (CP) plane corpus (ADR 0017)

- `raftkv_linearizable.rs` — the **CP counterpart** of the Accord corpus, for the
  `animus-cp-data` leaderful data plane. Crucially it is **not** built on
  `support/mod.rs`: that harness drives **multi-key transactions** via
  `AccordNode`, but the Raft KV plane is **single-tablet, non-transactional KV**
  (`put`/`delete`/`linearizable_get`, one key per op), so the transactional
  workload can't run over it. The file is self-contained — it reuses only the
  *checkers* (`check_cycles`/`check_durability`/`check_convergence`) and the
  `Recorder`/`History` model — and drives a **single-key list-append** workload
  over one Raft group (clients route each op to the current leader, tolerating
  crashes/partitions → `info`).
- **Serializability is sound *and* asserted here**: a
  single Raft group *is* the serialization authority, so a forked/stale read (the
  failure a deposed leader would cause) shows up as a `check_cycles` cycle. There
  is no eventually-consistent read path to manufacture torn-read false positives,
  so all three checks run on this one layer. Convergence + durability are still
  *eventual* (a lagging follower catches up via log/snapshot), so they use the same
  **converged-or-timeout** poll as the Accord runner.
- Frozen, name-seeded scenario set (29 cells): baselines + leader-kill /
  follower-kill / partition-leader / lossy × early/mid/late × 3- and 5-replica,
  **plus the deepened tier** mirroring the Accord fault matrix — `stop_restart`
  (a true process restart: `sim.stop` + a fresh `RaftKvNode::start` on the same
  id, recovering from the durable WAL — the CP recovery path), `split_brain`
  (full-mesh partition, no majority anywhere), `leader_minority` (5-replica:
  leader isolated *with* a minority — the stale-read window), and compound
  `lossy`+`stop_restart`. Deepened cells carry a non-zero `Scenario::window`
  (the runner holds the fault open before healing, so the group rides out a real
  outage); the original cells keep `window == 0` and their runs are
  **byte-identical** to the pre-deepening corpus (verified against captured
  histories when the tier landed). Depth knob **`ANIMUS_RAFTKV_SEEDS`** (default
  1 = byte-identical frozen set; held green at depth 20 / 580 scenarios). A
  structural `raftkv_corpus_covers_the_fault_matrix` guard keeps the matrix
  honest. The teeth-proof is the shared `negative_control.rs` (same
  `check_cycles`).
- **Engine tiers:** the corpus runs on `MemoryEngine` (always-on) and on
  **`LsmEngine<SimEnv>`** — the durable path (real WAL/SSTable recovery through
  the deterministic disk seam) that production actually runs; no corpus drove it
  under faults before. A 4-scenario representative LSM subset (baseline, a kill,
  the WAL-recovering `stop_restart`, the compound) runs by default;
  **`ANIMUS_RAFTKV_LSM=1`** runs the *whole* corpus over the LSM engine
  (composable with `ANIMUS_RAFTKV_SEEDS`; held green at ×10 / 290 scenarios).
  A `StopRestart` on this tier re-opens the engine via `LsmEngine::open_with`
  on the same per-node prefix — engine recovery *plus* Raft-WAL re-apply
  (idempotent).

### Elle-against-cross-tablet-transactions: the multi-tablet corpus (ADR 0018 §4, PR6)

- `txn_serializable.rs` — the multi-tablet counterpart to the single-Raft-group
  `raftkv_linearizable.rs` corpus, proving ADR 0018's 2PC transaction protocol
  (`animus-cp-data`'s `txn_stage_anchor`/`txn_stage_participant`/
  `txn_commit_at_least`/`txn_resolve`/recovery primitives) serializable under
  fault injection, at depth. **Self-contained, like `raftkv_linearizable.rs`**
  — an in-test coordinator (`run_txn`) reimplements `animusd::
  ClientCtx::cp_txn`'s protocol directly over raw `RaftKvNode` handles
  (mirroring `animus-cp-data/tests/txn_multi.rs`'s harness style), and a
  `push`/`recovery_resolve` pair (adapted from `animus-cp-data/tests/
  txn_recovery.rs`'s helpers of the same name, made async-native) plus a
  `resolver_loop` mirror `animusd::ClientCtx::txn_recover`/
  `txn_resolver_loop`. **This proves the protocol, not the wire layer** —
  `animusd/tests/cp_txn.rs`'s real multi-process `ProdEnv` cluster is the
  separate acceptance test for the actual wire coordinator; the two are
  complementary, not overlapping.
- **Topology**: 3 independent tablet Raft groups (`t0`/`t1`/`t2`, 3 replicas
  each), so a transaction spans 2–3 *independent leaders, independent `Hlc`
  clocks, independent commit pipelines* — unlike the single-Raft-group
  raftkv corpus (where a cycle can only mean a forked/stale read),
  `check_cycles` finding zero cycles here is a real, non-vacuous
  cross-tablet serializability claim.
- **Keyspace and single-writer-per-key, including read-modify-write.** Nine
  keys (3 groups × 3 clients), single-writer-per-key **throughout** — the
  rmw shape included. An earlier draft gave the rmw shape its own
  multi-writer "shared" keys and a live corpus run immediately found a
  spurious `check_cycles` cycle: the storage layer's plain `TxnStage` merge
  is an unconditional overwrite (like `Put`) with no CAS-style conflict
  check, so two transactions racing to stage the *same* key silently
  clobber each other — exactly the hazard single-writer-per-key exists to
  rule out for the other two shapes, and it turns out to bind on rmw too.
  The shipped design instead has rmw append to 2 of the client's *own*
  owned keys, conditioned on a precondition read of a *different* client's
  key (in the one group this transaction doesn't write to) — never two
  transactions writing the same key, but still genuine G2/write-skew teeth
  (the commit depends on a read of something a concurrently-running
  transaction writes).
- **Three shapes**: write-only multi-key (never a begin-time read — same
  discipline as the raftkv/Accord corpora, with one addition: a
  provably-rolled-back append must never leak into a later write's encoded
  prefix, so the client's list cache is only advanced on `Committed`/
  `Indeterminate`, never a confirmed `Aborted`); read-only multi-key via
  `quiescent_multi_read` (below) — plus a separate single-key point-read
  shape exercising the foreign-intent read-path push
  (`linearizable_get_served_fast` → cross-tablet `TxnStatus` →
  `resolve_intent_given_status`, lifted per PR5 §4); and read-modify-write
  (above).
- **The read-only shape's snapshot mechanism went through three redesigns
  before it stopped producing false-positive torn reads** — each one found
  by a *different* corpus scenario, none of them a real protocol bug.
  `quiescent_multi_read` (the current design) reads every key **at
  latest**, **concurrently** (`futures::future::join_all`, never a
  sequential per-key loop), after proactively force-resolving each one
  (`force_durably_resolve_key`), and accepts the result only once two
  consecutive concurrent rounds agree byte-for-byte — if nothing changed
  between two independent rounds, no transaction was in flight touching
  any involved key during that whole window, so the read is genuinely
  consistent, not just probably fine. The two abandoned designs, in order:
  (1) a single coordinator-minted `read_at` snapshot ts — undermined by
  `RaftKvNode::mint_pushed`'s write-conflict floor, which can stamp a write
  *above* whatever ceiling an **earlier** future-padded read already
  pushed that group's ceiling to, and since `Hlc::mint` is monotonic that
  becomes a **permanent** floor, so no margin (fixed or dynamically
  sampled) closes it — the group's clock only ever ratchets further ahead
  of wall-clock, never back (found by `participant_leader_kill_early`,
  then again by plain `baseline`, no fault injection at all); (2)
  force-resolve once, then read every key sequentially — a slow key's own
  resolve/read can itself take real sim time, so a transaction touching an
  *earlier*, already-read key can still land before a *later* key in the
  same list is read (found by `baseline_read_heavy`). See
  `quiescent_multi_read`'s own doc in `txn_serializable.rs` for the full
  account, including the exact seeds and observed symptoms for all three
  rounds.
- **Stage attempts push, never overwrite, and the coordinator must verify
  (ADR 0018 §2/PR6, task #16)** — a *different*, genuine durability bug
  this corpus found at depth (`ANIMUS_TXN_SEEDS=10`,
  `coordinator_abandon_prepare_s01`, seed 16358087571531249382, no fault
  injection needed): `KvCommand::TxnStage`'s apply now rejects
  (whole-or-nothing) a target key already holding a *different*
  transaction's unresolved intent, closing a corrupted-MVCC-version-chain
  hole an abandoned-then-overwritten-then-aborted transaction sequence
  could otherwise produce (full account in ADR 0018's PR5 amendment §1b).
  Since a stage call returning `Some(..)` only ever meant "this entry
  applied," never "my content landed," the corpus's coordinator
  (`stage_anchor_pushing`/`stage_participant_pushing`, mirroring
  `animusd::ClientCtx::txn_prepare_pushing`) now verifies every staged key
  via `RaftKvNode::txn_verify_staged` after each attempt, retrying
  (`STAGE_PUSH_ATTEMPTS`, backed off) before reporting the whole
  transaction `Aborted` — without this, a blocked stage would look
  identical to a genuine one, and the corpus's own coordinator would have
  reintroduced the exact atomicity violation this fix exists to close (a
  transaction "committing" without one of its own writes ever having
  happened).
- **Recovery push + resolver loop, and the fault matrix.** `push` and the
  per-scenario `resolver_loop` are the corpus's proof that a
  coordinator-abandoned transaction still converges: `Workload::
  abandon_prepare_pct`/`abandon_commit_pct` model a coordinator that stops
  mid-2PC (after a successful prepare, or after a confirmed-but-unresolved
  commit) — recorded `info`, never `fail` (house rule). ~25 frozen cells:
  3 baselines (default/rmw-heavy/read-heavy mix), 2 coordinator-abandon
  cells, participant/anchor leader-kill × 3 timings each, partition-during-
  prepare × 3 timings, lossy links, clock skew within/beyond
  `HLC_MAX_OFFSET` (beyond is a **liveness**-only knob — some reads may time
  out, `check_cycles` must stay green throughout, per the ADR's Decision
  section), and 6 compound cells crossing abandonment/faults/workload mix.
  Depth knob **`ANIMUS_TXN_SEEDS`** (default 1 = frozen; `seed_expand`'s
  usual variant-0-keeps-the-canonical-seed convention).
- **A single-decider assumption in the recovery protocol itself, found by
  this corpus under real fault injection**: see the ADR 0018 PR6 amendment
  for the full account — the coordinator's own decide attempt and the
  resolver's independent recovery push can both legitimately conclude
  "commit" for the same transaction with *different* minted timestamps if
  the coordinator's own round trip is still genuinely in flight past
  `RECOVERY_GRACE`, and the apply path's "two different commit timestamps
  is impossible by construction" assert does not tolerate that.

### Scaling coverage: the two env knobs + the topology split (ADR 0014)

- **Depth — `ANIMUS_CORPUS_SEEDS=K` (default 1).** `support::seed_expand` emits
  `K` seed variants of every structural cell. Variant 0 keeps the cell's
  **canonical (frozen) name+seed**; variants `1..K` get a `_sNN` suffix + a fresh
  name-derived seed. `K=1` is the identity, so the always-on default is
  byte-identical to the committed corpus — a frozen regression seed never moves.
  This is the dominant bug-finding lever: one structural cell × many interleavings.
- **Breadth — `ANIMUS_CORPUS_FULL=1` (default off).** `support::corpus_extended`
  adds new dimension *values* (`SlowLinks` fault; 7-node + asymmetric 3+5/5+3
  shapes; very-early/wind-down timings; write-only/big-txn/low-contention
  workloads; triple-fault + partition→heal→repartition schedules). All
  `ext_`-prefixed, so they never collide with or perturb a base name/seed.
- **Tiering.** Default `cargo test` → `K=1`, no FULL → the frozen 119. Deep tier
  (`ANIMUS_CORPUS_SEEDS=40 ANIMUS_CORPUS_FULL=1`) runs **nightly** in CI
  (`.github/workflows/corpus-deep.yml`), not per-push.
- **Serializability is the corpus's safety property, scaled to depth.**
  `check_cycles` is sound on **pure Accord** (`AccordNode::start`: local execution +
  versioned-snapshot reads `get_at(execute_at)`, fault-robust), which is the only
  layer the corpus now runs (v1 is CP-only). Serializability is a *safety* property,
  so `corpus_is_consistent` asserts it over the full env-scaled `corpus()` and it
  scales to the deep tier (it held 7,560/7,560 historically). (Pre-v1 the corpus also
  ran a `Frontier` topology — Accord wired to the AP data plane — checked only for
  **convergence + durability** because its quorum read is eventually consistent and
  would give `check_cycles` torn-read false positives; that topology + check went
  with `animus-data`, ADR 0019.)

- **Genuine black-box list-append over Accord (ADR 0014, closed limitation).**
  With **arbitrary write values** (ADR 0011) each key stores a *real list value*:
  a write is a list-append (append this txn's globally-unique element to the key's
  list, write the whole new list back as the stored value via
  `AccordNode::submit_writes`), and a read observes the **actual stored list**
  (decoded from `AccordNode::read_value_result`). The recovered order comes from
  observed *values* (Elle's `recover`), **not** from `applied_order` — so
  `check_cycles` is a real black-box serializability check (a single
  globally-agreed-but-non-serializable order now shows as a cycle, not just
  cross-replica divergence). The earlier "recover the list from `applied_order`"
  modelling is **obsolete** — do not reintroduce it.
- **`list_state` reads stored values, not the order.** Final state is read
  straight from each key's actually-stored value on two *distinct* replicas
  (`store_value` → `decode_list`), keeping convergence a real cross-replica
  agreement check and durability ("every ok append is in the final list")
  meaningful. **Do not** run `check_durability` against a single raw per-replica
  read with no read-repair expectation; read the converged stored list per
  replica.
- **Single-writer-per-key is load-bearing here (not just an optimisation).** Each
  key is written by exactly one client (`owner(key) = key % clients`); a write
  only appends to keys it owns. Two clients appending to one key lose updates by
  the *data model* (per-key LWW), which would show as a false-positive durability
  failure / divergent read — not a consistency bug. Cross-transaction conflict
  (the wr/rw/ww edges) still comes from multi-key transactions and from reads
  observing keys *other* clients write. **A client builds each append on its own
  authoritative in-memory list**, *not* a begin-time read: the apply marks a txn
  `Applied` before the driver's spawned task has merged the write into the engine,
  so a begin-time read can see a stale base and lose the client's own earlier
  appends (this bit during development — the seed sweep caught it as a divergent
  read).
- **The corpus is a committed generator, not a live-random test.** Each
  scenario's seed is a stable hash of its name, so the suite runs the same set
  every time and a failure names one scenario + seed. Regenerating/growing it is
  an explicit edit to `support::corpus`; a bug-catching scenario stays forever.
- **Indeterminate (`info`) vs the observed universe.** A write whose client timed
  out is recorded `info`, but if it actually executed its element is in the key's
  stored list (hence observed by later reads and present in the final list).
  That's sound: reads and the final state both read stored values, so they stay
  prefix-consistent under single-writer-per-key; `info` values simply have no `ok`
  appender, so they form no dependency edges. Never record an indeterminate op
  `ok`.
- **Workload modeling gotcha (Elle).** Every appended element must be **globally
  unique**, not just per-key-per-round. Reusing a value across rounds/phases makes
  `recover` collapse distinct transactions onto one `(key, value)` appender,
  manufacturing **spurious cycles** the checker correctly flags. The harness draws
  values from one monotonic `fresh_value()` source for exactly this reason. If
  `check_cycles` trips on a single-writer-per-key workload, suspect value reuse
  before suspecting the system.
