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
- `src/corpus.rs` (ADR 0061 rung B1) — the shared corpus-seeding scaffolding
  every fault-injection corpus in the repo builds on: `name_seed` (the
  house FNV-1a name→seed hash), `odd_name_seed` (a second, `| 1` hash
  flavor four corpora use — kept as its own function, never unified with
  `name_seed`, because unifying them would silently move those corpora's
  committed regression seeds; see this module's own doc for which corpora
  use which), `seeds_from_env` (the `ANIMUS_*_SEEDS` depth-knob parse), and
  two expansion shapes: `seed_expand<T: SeedVariant>` for corpora that
  build a `Vec<Scenario>` up front (implement `SeedVariant`'s
  `scenario_name`/`reseeded` on your own `Scenario` type — the harness owns
  the expansion loop, never forcing one struct shape), and `for_each_seed`
  for corpora that drive one named scenario directly via a closure. Used
  by `animus-test`'s own `tests/*_corpus.rs` files below and, as a dev-
  dependency, by `animus-cp-data`'s and `animus-control`'s corpus test
  files — a dev-dependency cycle Cargo permits, since neither crate's
  library needs the other to build.
- `src/shrink.rs` (ADR 0061 rung B4) — the generic failure-minimization
  engine every corpus can wire in: `minimize` (greedy delta-debugging over a
  caller-supplied `candidates`/`still_fails` pair), `ShrinkBudget`/
  `ShrinkReport`, `shrink_enabled`/`budget_from_env` (the `ANIMUS_SHRINK`/
  `ANIMUS_SHRINK_MAX_CHECKS` env-knob parse), and `describe`/`replay_json`
  for printing a human report plus a copy-pasteable machine handle. See
  "Failure minimization" below.

Env knobs at a glance (details in the sections below):

| Knob | Default | Effect |
|------|---------|--------|
| `ANIMUS_SEED` | unset | replay one failed sim run from its printed seed (repo-wide convention) |
| `ANIMUS_RAFTKV_SEEDS=K` | 1 | K seed variants per raftkv-corpus cell |
| `ANIMUS_RAFTKV_LSM=1` | off | run the whole raftkv corpus over `LsmEngine<SimEnv>` |
| `ANIMUS_TXN_SEEDS=K` | 1 | K seed variants per multi-tablet transaction-corpus cell (ADR 0018) |
| `ANIMUS_STREAM_SEEDS=K` | 1 | K seed variants per DynamoDB Streams lineage-walk cell (ADR 0042/0043) |
| `ANIMUS_BACKFILL_SEEDS=K` | 1 | K seed variants per secondary-index backfill fault-injection cell (ADR 0045) |
| `ANIMUS_BACKUP_SEEDS=K` | 1 | K seed variants per on-demand backup capture + restore fault-injection cell (ADR 0059 Train 1/2) |
| `ANIMUS_PITR_SEEDS=K` | 1 | K seed variants per PITR sealing fault-injection cell (ADR 0059 Train 3) |
| `ANIMUS_SHRINK=1` | off | minimize a failed corpus scenario's parameters to a small reproducing case (ADR 0061 rung B4) |
| `ANIMUS_SHRINK_MAX_CHECKS=N` | 500 | override the minimizer's check-count budget |
| `ANIMUS_SHRINK_REPLAY=<json>` | unset | a corpus's own replay entry point (e.g. `raftkv_shrink_replay`) re-runs this minimized case and asserts it still fails |

## What's non-obvious

- **Indeterminate outcomes (e.g. a timeout) MUST be recorded `info`, never
  `fail`.** `fail` asserts the op definitely did not happen; misclassifying
  makes a checker draw false conclusions. Every corpus in this crate follows
  this: an op whose outcome could not be confirmed (a crash/partition/timeout)
  is `info` — even though, if it actually executed, its effect is still
  present in the observed universe (a later read can see it; it can still
  appear in a final converged state). That's sound: an `info` op simply forms
  no dependency edge of its own, so a checker never draws a false conclusion
  from it either way. Never record an indeterminate op `ok`.
- `check_cycles` is the core Elle idea: recover each key's append order from
  observed reads, build wr/ww/**rw** edges, run Tarjan SCC. The `rw`
  anti-dependency rule (a read precedes the appenders of values it did *not*
  observe) is what catches write skew — keep it if you refactor.
- **Every appended element must be globally unique, not just per-key-per-round
  (Elle workload-modeling rule).** Reusing a value across rounds/phases/keys
  makes Elle's `recover` collapse distinct transactions onto one `(key,
  value)` appender, manufacturing **spurious cycles** the checker correctly
  flags even though nothing was actually wrong. Every list-append workload in
  this crate draws its values from one monotonic fresh-value source for
  exactly this reason. If `check_cycles` trips, suspect value reuse in the
  workload before suspecting the system under test.
- **Durability/convergence checks need a converged read, not a raw
  single-shot one.** `check_durability` = every `ok` op is present in the
  converged final state; `check_convergence` = two independent post-
  convergence reads agree — neither is sound against raw per-replica state
  with no read-repair expectation. How convergence is *reached* is each
  corpus's own concern (a quorum read, a caught-up Raft follower, a
  cross-tablet snapshot round) and belongs in the runner as a
  **converged-or-timeout poll, never a fixed-deadline one-shot read** — see
  each corpus section below for its own mechanism, including the multi-tablet
  corpus's three-redesign account of just how easy this is to get wrong.
- **Single-writer-per-key is a workload-design tool, not just an
  optimization**, when a corpus wants clean wr/rw/ww edges without the
  key's own data-model semantics (LWW) manufacturing false-positive
  divergence. Two writers racing one key under plain LWW lose updates *by
  design*, which reads as a checker failure for the wrong reason. Where a
  corpus needs a write to depend on another client's key anyway (to get real
  G2/write-skew teeth), route it through a **read** of that key, never a
  second writer on it — see the multi-tablet corpus's read-modify-write shape
  below for a worked example.
- **A corpus is a committed generator, not a live-random test.** Every
  scenario/cell's seed is a stable hash of its own name, so a suite run is
  the same set every time and a failure names one scenario + seed,
  replayable via `ANIMUS_SEED`. Regenerating/growing a corpus is an explicit
  edit to its own generator function; a bug-catching scenario stays forever
  once added.
- `CheckReport.seed` exists so a flagged anomaly is replayable; surface it in
  assertion messages.

### Failure minimization (`src/shrink.rs`, ADR 0061 rung B4)

- **You cannot shrink a seed.** A `SimEnv` run is a pure function of an
  *opaque* seed (ADR 0003) — no seed is "smaller" than another, so searching
  over seeds directly minimizes nothing. `shrink::minimize` instead
  delta-debugs a failing scenario's own **parameters** (whatever its
  `Scenario` type exposes — fault schedule, round/client/keyspace counts,
  outage windows), holding the seed fixed and re-running the whole
  simulation at each candidate. This is strategy (a) from the module's own
  doc, chosen over fault-*schedule* minimization (suppressing one specific
  injected fault decision, e.g. one dropped message out of an ambient
  `NetConfig` probability, rather than one whole scheduled `Scenario` fault
  entry) because the latter needs a recorded-schedule replay mode that
  doesn't exist yet — see `crates/animus-sim/CLAUDE.md`'s own note on this.
- **Greedy, not full ddmin.** Given the current case, try every "one step
  smaller" candidate (in a fixed order) and keep the first that still
  reproduces the failure, restarting from it; stop at a local fixpoint (no
  candidate reduces further) or when `ShrinkBudget::max_checks` runs out.
  This repo's `Scenario` types have small parameter spaces (a handful of
  scalar knobs, single-digit-to-low-tens fault lists), so the simpler
  algorithm converges in well under the default 500-check budget without
  needing ddmin's binary-search subset removal.
- **Deterministic by construction.** The candidate generator is a plain
  function of the current case (fixed order, no RNG), the predicate reruns
  the same seed every time, and the budget is a check *count* — never
  wall-clock time, so a slower machine can't silently produce a
  less-minimized result. Same failing input ⇒ same minimized output, always.
- **Opt-in, zero cost by default.** `shrink_enabled()` gates on
  `ANIMUS_SHRINK=1`; a corpus calls it only *after* already observing a
  failure (never on a green run's hot path), so an unset `ANIMUS_SHRINK`
  (the default) never calls into this module — normal corpus runs are
  unaffected: same seeds, same runtime, byte-for-byte.
- **Wiring a corpus in** (see `raftkv_linearizable.rs` for the worked
  example): (1) derive `Serialize`/`Deserialize` on the `Scenario` type
  (`std::time::Duration` fields serialize fine via serde's own support); (2)
  write a `candidates(&Scenario) -> Vec<Scenario>` covering the fields worth
  reducing — skip identity fields (`name`) and the seed itself, and skip any
  field whose value changes what's *being tested* rather than merely its
  size (e.g. `raftkv_linearizable.rs` never reduces `replicas`, since some
  nemeses are only meaningful at a specific group size); (3) at the point a
  scenario is observed to fail, call `shrink::minimize` with that generator
  and `shrink::budget_from_env()`, then `shrink::describe` (human report) and
  `shrink::replay_json` (machine handle) to print both — see
  `shrink_and_report` in `raftkv_linearizable.rs`; (4) add one `#[ignore]`d
  replay test that reads the printed JSON from `ANIMUS_SHRINK_REPLAY`,
  deserializes it, reruns it, and asserts the failure still reproduces (see
  `raftkv_shrink_replay`) — this is the "developer can replay the minimal
  case directly" half of the deliverable, proven to round-trip in
  `raftkv_shrink_reduces_a_real_regression_to_its_minimal_repro`.
- **What it cannot do yet**: isolate one specific dropped/corrupted/
  duplicated *message* out of an ambient `NetConfig`/`DiskConfig` probability
  — that granularity needs fault-schedule minimization (strategy (b)),
  deferred; see `crates/animus-sim/CLAUDE.md`. It also never touches fields a
  corpus's own `candidates` function doesn't mention (by design — the corpus
  author decides what's safe to reduce without changing which failure is
  being modeled).

## Tests

`cargo test -p animus-test` — `cycle_checker.rs` (hand-built histories) + the
CP-plane corpora below. (v1 is CP-only, ADR 0019: the AP data-plane test
files — `ap_data_plane.rs`, `fault_sweep.rs`, the assembled-stack
`end_to_end.rs` — were removed with `animus-data`, as was the `Frontier`
topology; the Accord transaction-consensus testbed (`animus-consensus`) and
its Elle corpus (`elle_accord.rs`, `corpus.rs`, `tests/support/`) were removed
once ADR 0019's AP deferral became permanent — AP's only remaining stated role
was Accord's transaction story, and with the CQL wire adapter also dropped
(ADR 0053) no shipping wire can even select `ReplicationMode::Ap`. If
transaction consensus over an AP data plane is ever revived, both are
retrievable from git history.)

- `negative_control.rs` — the **teeth proof**, shared by every corpus below
  that runs `check_cycles`: hand-built non-serializable histories (write skew
  G2, circular read dep G1c, a 3-txn cycle) the checker *must* reject, plus
  serializable ones it must accept. Run/read this before trusting any green
  corpus run.

### Elle-against-Raft: the leaderful (CP) plane corpus (ADR 0017)

- `raftkv_linearizable.rs` — proves `animus-cp-data`'s leaderful data plane
  serializable under contention. **Self-contained** — it reuses only the
  *checkers* (`check_cycles`/`check_durability`/`check_convergence`) and the
  `Recorder`/`History` model from this crate's `src/`, no shared scenario/
  nemesis harness — and drives a **single-key list-append** workload
  (`put`/`delete`/`linearizable_get`, one key per op — the Raft KV plane is
  single-tablet and non-transactional, so a multi-key transactional workload
  doesn't apply here) over one Raft group (clients route each op to the
  current leader, tolerating crashes/partitions → `info`).
- **Serializability is sound *and* asserted here**: a
  single Raft group *is* the serialization authority, so a forked/stale read (the
  failure a deposed leader would cause) shows up as a `check_cycles` cycle. There
  is no eventually-consistent read path to manufacture torn-read false positives,
  so all three checks run on this one layer. Convergence + durability are still
  *eventual* (a lagging follower catches up via log/snapshot), so they use the same
  **converged-or-timeout** poll every corpus in this crate does (see "What's
  non-obvious" above).
- Frozen, name-seeded scenario set (29 cells): baselines + leader-kill /
  follower-kill / partition-leader / lossy × early/mid/late × 3- and 5-replica,
  **plus a deepened tier** — `stop_restart`
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
- **`ANIMUS_SHRINK` wiring (ADR 0061 rung B4)** — this is the worked example
  the "Failure minimization" section above points to. `Scenario`/`Nemesis`
  derive `Serialize`/`Deserialize`; `scenario_candidates` reduces the fault
  list (drop one at a time), `window`, `rounds`, `keyspace`, and `clients`
  (never `name`/`seed`/`replicas` — see its own doc for why); when
  `raftkv_corpus_is_linearizable` observes a scenario fail and
  `ANIMUS_SHRINK=1` is set, `shrink_and_report` minimizes it and prints a
  report plus a JSON replay handle *before* the existing assertion still
  panics (minimization is a diagnostic only — it never softens the
  failure). `raftkv_shrink_replay` (`#[ignore]`d) is the paste-back replay
  entry point: set `ANIMUS_SHRINK_REPLAY` to the printed JSON and run it
  alone to re-confirm the minimized case fails. Proven two ways:
  `raftkv_shrink_reduces_a_real_regression_to_its_minimal_repro` builds a
  scenario with 4 real faults where a **genuine, real-simulator-derived**
  failure (`read_pct = 100` makes `client_loop` structurally never write, so
  `ok_writes == 0` regardless of every fault/size knob) reduces to zero
  faults and every scalar at its floor, checked and round-tripped through
  JSON; separately, forcing a real `cycles.ok = false` for one named cell and
  running the corpus with `ANIMUS_SHRINK=1` end-to-end (verified by hand
  during development, not committed — forcing a real failure into a
  currently-green corpus isn't a scenario that belongs in the tree) shrank
  `leader_kill_early_3`'s single fault away and every scalar to its floor,
  and the printed replay handle reproduced identically through
  `raftkv_shrink_replay`.

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
  discipline as the raftkv corpus, with one addition: a
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

### Elle-adjacent, but not Elle: the DynamoDB Streams lineage-walk corpus (ADR 0042/0043, round-3 PR8)

- `stream_lineage_corpus.rs` — a **self-contained** corpus (like
  `raftkv_linearizable.rs`/`txn_serializable.rs`) reimplementing the ADR
  0042/0043 **sealer** (`seal_now`, mirroring `animusd::index_drain::
  seal_now`'s exact sequence) and a **model consumer**
  (`collect_tablet_records`/`verify_lineage`, mirroring `DescribeStream`/
  `GetShardIterator(TRIM_HORIZON)`/`GetRecords`'s exact decision tree)
  directly over `animus-cp-data`'s `RaftKvNode`/`segment` module and a bare
  `animus-control::Metadata` (mutated with plain `.apply()` calls — no live
  control Raft, the same hand-scripted-catalog technique
  `animus-cp-data/tests/reconciler_corpus.rs` uses) and `animus-sim`'s
  `SimSegmentStore`. **Not built on the Elle `History`/`check_cycles`
  machinery** — the property under test (exactly-once delivery, per-item
  order, chain continuity across split lineage, segment-content fidelity)
  is a shard-chain reconstruction claim, not a serializability claim, and a
  bespoke write-journal-vs-delivered-stream diff (`verify_lineage`) states
  it more directly than coercing it into a list-append history would. The
  consumer is driven **once, to convergence, after each scenario's write/
  seal/fault schedule finishes**, not as a live interleaved poll — a
  documented delta from the real wire API's own live-poll shape; the
  `ProdEnv` e2e (`animusd/tests/streams_e2e.rs`) and the existing
  `animusd/tests/dynamo_streams.rs` cover "what does an in-flight poll see
  mid-stream" instead.
- **Frozen named cells** (`quiet_table_rollover`, `hot_table_size_seals`,
  `kill_sealing_leader`, `store_outage_then_heal`, `disable_grace_drain`;
  the five zero-copy-split cells — `split_mid_stream`, the #216/#220
  regressions, `combined_chaos` — died in ADR 0050 Train B rung 2 with the
  in-place lineage they modeled, see the file's tombstone; their
  copy-based successors landed in rung 6: `copy_split_children_born_empty`
  (sealed history + backlog → final seal → cutover-frozen `split_lineage`
  → children with EMPTY change logs sealing their own epoch 0,
  exactly-once across the walk) and
  `copy_split_endgame_survives_seal_faults` (the final seal crashing
  between `put` and catalog commit, then a store outage, then healing —
  the identical epoch lands on retry; the fidelity boundary — the animusd
  driver itself is `split_build.rs`/`streams_e2e.rs`'s subject — is
  stated in the cells' own section doc) plus a
  dedicated `durability_invariant_holds_at_every_kill_point` scenario
  (ADR 0042 §9, D9): a scripted seal lifecycle with a modeled crash between
  the segment `put` and the catalog commit, asserting every acked write
  stays recoverable (from hot Raft state or a committed segment) at every
  kill point — this corpus never implements retention (that's `animusd::
  segment_janitor`'s own `ProdEnv` suite, `stream_janitor.rs`), so nothing
  here is ever expected to answer `TrimmedDataAccess`. Depth knob
  `ANIMUS_STREAM_SEEDS` (default 1 = the frozen cells; held green at
  `=40`, matching `corpus-deep.yml`'s nightly tier).
- **A real bug this corpus found while being built** (not in the streams
  subsystem — in the corpus's own test harness): `RaftKvNode::start_scoped`
  pins every group to `PRIMARY_STREAM`, so two tablet groups sharing the
  same 3 node ids (any split scenario) cross-talk their Raft traffic
  unless started with `start_hosted(.., stream = tablet_id.0)` instead —
  see `docs/engineering-lessons.md`'s Testing section for the full
  livelock symptom and fix.

### Elle-adjacent, but not Elle: the secondary-index backfill fault corpus (ADR 0045, PR4)

- `backfill_fault_corpus.rs` — the identical layering fix `stream_lineage_
  corpus.rs` set for the backfill seeder (`animusd::index_drain::
  backfill_seed_tick`) and completion aggregator (`animusd::
  index_backfill::index_backfill_tick`), both `animusd`-only and thus with
  no `SimEnv` of their own: a self-contained reimplementation of both
  functions' exact algorithms directly over `RaftKvNode` and a bare
  `Metadata` (`.apply()` calls, no live control Raft). **Deliberately
  narrower than a full GSI-materialization proof** — it never reimplements
  `reconcile_partition`'s cross-table row diffing (out of scope by design;
  see the file's own doc for why that's sound) — instead proving exactly
  the seeder's own claim: every partition that ever held a row gets at
  least one dirty marker, checked directly by diffing `KIND_BASE` against
  `KIND_CHANGE` partition sets, plus the aggregator's convergence decision.
  The full-stack, exact-GSI-content counterpart is the deterministic
  `ProdEnv` test `animusd/tests/backfill_seeder.rs::
  split_during_backfill_converges_with_correct_final_gsi`.
- **Frozen named cells**: `single_tablet_backfill_converges`,
  `live_writes_race_the_sweep`, `leader_kill_mid_sweep`,
  `two_indexes_creating_independently`, `drop_table_mid_backfill` (the two
  zero-copy-split cells — `concurrent_split_during_backfill`,
  `split_after_tablet_already_reported_done` — died in ADR 0050 Train B
  rung 2, see the file's tombstone; split-during-backfill returns on the
  copy-based mechanism in the cutover rungs). Depth knob `ANIMUS_BACKFILL_SEEDS` (default
  1 = the frozen cells; held green at `=40` in well under a second,
  matching `corpus-deep.yml`'s nightly tier).
- **A real, previously-undetected bug this corpus found on its very first
  run, at every seed** (not just under fault injection — a structural
  defect, reproducible without any injected fault at all): the backfill
  cursor's own advance write (`ctx.cp_kind_write_raw`, whose fence is
  always the tablet's *current live* range) silently rejected every
  cursor-persist attempt for a split child whose `range.start` is not
  itself a bare `TOKEN_BYTES`-wide token — true of essentially every real
  split, since a split key is chosen from real row content, never the
  hash ring. `cursor::cursor_key` used to truncate `range.start` to its own
  8-byte token, which then sorted *below* the child's own (longer)
  `range.start` the instant the byte right after the token was non-zero
  (true for any real `escape(pk)`) — this same truncation was later closed
  outright (issue #355; `cursor_key` now embeds `range.start` verbatim), but
  at the time of this corpus the fix below (an unfenced write) was the only
  one available. Data coverage itself was never at
  risk (the change-log seed writes are keyed by real base keys, which
  correctly satisfy the fence) — only the cursor's own persistence was,
  so a split child's sweep silently restarted from scratch every tick
  instead of resuming. Harmless for the pre-existing `"gsi"` cursor tag
  (whose caller already tolerates a perpetually-absent cursor as "just
  reconcile everything, always correct") but a genuine **liveness** bug
  for backfill: a child with more than `BACKFILL_SEED_BATCH` (256)
  partitions on its own side could never advance past that one batch's
  worth and never reached its own end. Fixed in `animusd::index_drain`
  by giving the cursor's own advance write a dedicated helper
  (`advance_backfill_cursor`) that fences with `KeyRange::whole()`
  instead of the tablet's live range — a cursor row's identity is already
  fully captured by its own token (disjoint from base data by row kind)
  and needs no range-fencing, the same reasoning `seal.rs`/`ceiling.rs`'s
  engine-global markers already rely on. See
  `docs/engineering-lessons.md` for the general lesson and
  `advance_backfill_cursor`'s own doc for the full account.

### Elle-adjacent, but not Elle: the on-demand backup capture fault corpus (ADR 0059 Train 1 PR③)

- `backup_fault_corpus.rs` — the identical layering fix `backfill_fault_
  corpus.rs`/`stream_lineage_corpus.rs` set for the backup capture driver
  (`animusd::backup_capture::backup_capture_tick`) and completion
  aggregator (`animusd::backup_completion::backup_completion_tick`), both
  `animusd`-only: a self-contained reimplementation of both functions'
  exact algorithms directly over `RaftKvNode`, a bare `Metadata`, and
  `animus-sim`'s `SimSegmentStore` (ADR 0043 §A7's existing fault-injection
  store, reused verbatim per the ADR's own testing-plan instruction — never
  built anew). **The §6 split-re-planning DECISION is real production
  code, never reimplemented**: `Metadata::backup_capture_target`/
  `live_split_descendants`/`backup_ready_to_complete`/`backup_manifest_
  tablet_progress` (`animus-control`) are called directly — only the
  driver's own scan/chunk/cursor mechanics and the aggregator's own
  manifest-assembly/stuck-timeout mechanics are mirrored.
- **Verification is direct decode-and-diff, not restore** (Train 2's own
  concern, not yet built): `assert_backup_matches_model` fetches the
  completed backup's manifest object and every chunk object a reporting
  tablet's own id names, decodes them, and diffs the result against an
  independently-tracked model of the source table's committed state at
  each tablet's own capture-pin moment — asserting no key is ever decoded
  twice across two different reporting tablets (the §6 double-count
  hazard `Metadata::backup_manifest_tablet_progress`'s own doc names) and
  that no decoded value ever matches one only ever staged inside a
  pending, never-resolved transaction intent (ADR 0059 §5's "committed
  values only, never a raw envelope" rule, checked directly rather than
  merely by construction).
- **Frozen named cells**: `single_tablet_backup_converges_under_concurrent_
  writes` (a genuine staged-and-unresolved intent alongside pre-/post-pin
  writes), `leader_kill_mid_capture` (`sim.crash` + failover to a
  different replica, proving cursor durability), `capture_driver_node_
  crash_restart` (a **true process restart** — `sim.stop` + a fresh
  `RaftKvNode::start_hosted` on the same id and the same durable
  `MemoryEngine`, mirroring `raftkv_linearizable.rs`'s own `StopRestart`
  nemesis — as opposed to the previous cell's live-but-muted crash),
  `split_races_capture_and_replans_onto_descendants` (the named §6
  scenario: a split cuts over mid-capture, the parent's own unfinished
  progress is simply abandoned, and each child restarts its own share from
  scratch — `SplitPolicy::RestartFromScratch`), and `store_faults_ack_
  lost_puts_still_converge` (`SimSegmentStore`'s existing ack-lost-put
  fault, at probability 0.5). A sixth cell, `a_wedged_capture_fails_after_
  the_stuck_timeout`, proves the aggregator's own stuck-`Creating` mark
  phase directly against the sim's virtual `Clock` (env-time, deterministic
  — this corpus's own reimplementation is not bound by `animusd`'s real
  `tokio::time::Instant`, which this crate can't reach anyway) — both that
  it fires once genuinely stuck and that it does **not** fire early.
  **Train 2 (ADR 0059 §7) adds five restore cells**, the identical
  self-contained-reimplementation technique now driving a completed backup
  through `RestoreTableFromBackup`'s own mechanics
  (`animusd::backup_restore`, mirrored — never imported — the same layering
  reason as the capture half): `restore_round_trip_matches_model_at_
  capture_cut_version` (including a staged-never-resolved intent, proving
  restore only ever sees resolved values), `restore_driver_crash_restart_
  resumes`/`restore_leader_kill_mid_seed_converges` (a true process restart,
  and a live crash/failover, both mid-seed — each manufactures a genuine
  partial-progress precondition via a direct partial seed, since a
  whole-manifest-sweep-per-tick call has no natural interruption point of
  its own within one synchronous test invocation), `restore_store_faults_
  still_converge` (the store genuinely unavailable partway through, healing
  later — `SegmentFaultConfig`'s ack-lost thresholds are `put`/`delete`-only,
  so a read fault uses `SimSegmentStore::set_unavailable_until` instead),
  and `restore_after_source_drop`. GSI-rebuild convergence is deliberately
  **not** reimplemented here a third time — it's `backfill_fault_corpus.rs`'s
  own already-proven machinery, applied to an ordinary `Active` tablet
  indistinguishable from any other (ADR 0059 §8's own point); the real
  production stack's GSI-after-restore convergence is
  `animusd/tests/dynamo_restore.rs`'s job. Depth knob `ANIMUS_BACKUP_SEEDS`
  (default 1 = the frozen cells; held green at `=100` for the restore cells,
  `=200` for the whole file, both in well under a second).
- **Three `Env`-level fault-primitive cells (ADR 0061 Decision 3) add
  genuinely new fault kinds this corpus hadn't exercised**, each a close
  copy of an existing cell with one added fault call, never modifying the
  originals: `wal_fsync_lie_leader_kill_mid_capture`
  (`DiskConfig::set_fsync_lie_prob(0.3)` globally before the crash — proves
  the capture cursor's durability claim survives a lied-to `sync` on the
  crashing leader), `chaotic_network_capture_converges` (a compound
  `NetConfig` — 5% drop, 10% duplicate — active for the group's whole
  lifetime, proving ordinary Raft/2PC retry carries the capture driver and
  completion aggregator through a lossy/duplicating network), and
  `capture_driver_wal_torn_on_restart` (a global `DiskConfig{torn_tail_
  on_crash: true, corrupt_on_crash: true, ..}` set before group startup,
  combined with `capture_driver_node_crash_restart`'s fresh-process-restart
  idiom — see `docs/engineering-lessons.md`'s entry on why that combination
  needs `crash` → `restart` → `stop`, not `stop` alone, to actually exercise
  the tear ahead of the fresh restart). `NetConfig::set_corrupt_prob` is
  deliberately excluded from `chaotic_network_capture_converges`: as of
  this corpus's own branch, `animus-cp-data::codec`'s
  `Vec::with_capacity(n as usize)` call sites read an untrusted wire length
  prefix with no upper-bound check, so corrupting a message risks an
  allocator abort rather than a clean application-level error — safe to add
  once that fix lands. `DiskConfig::set_enospc_prob`/`set_error_prob` stay
  out of this whole file for a different, permanent reason: their error
  branches in `persist_wal` are `assert!(halted.load(..), ..)`, and this
  corpus's scenarios never call the per-node `shutdown()` that sets
  `halted`, so firing either on a live node hard-panics the test process.

### Elle-adjacent, but not Elle: the PITR sealing + restore corpus (ADR 0059 §9/§10, Train 3)

- `pitr_fault_corpus.rs` — the identical layering fix `stream_lineage_
  corpus.rs`/`backup_fault_corpus.rs` set for the fifth consumer arm
  (`animusd::index_drain::pitr_tick`/`pitr_seal_now`), `animusd`-only: a
  self-contained reimplementation of `pitr_seal_now`'s exact algorithm
  directly over `RaftKvNode`, a bare `Metadata`, and `animus-sim`'s
  `SimSegmentStore` (standing in for the backup store — both are the
  identical `SegmentStore` trait, ADR 0059 §1). Verification is
  decode-and-diff against an independently-tracked write journal
  (`verify_pitr_lineage`), the PITR twin of `stream_lineage_corpus.rs`'s
  `verify_lineage` — with one deliberate scoping difference: exactly-once
  delivery is checked **within each tablet's own chain**, not globally
  across a multi-tablet `lineage` array, since packed-HLC uniqueness is a
  per-group guarantee only (no node-id bits, ADR 0018 §2) — two independent
  sibling tablets can legitimately mint the identical packed value absent
  production's real `SeedBatch` witnessing, which this corpus doesn't model
  (sealing, not the split-build workflow, is what's under test here).
- **Periodic base snapshots and the retention janitor's own loop plumbing
  are deliberately NOT re-simulated end-to-end** — `animusd::pitr_janitor::
  pitr_snapshot_loop` reuses Train 1's `BeginBackup`/capture-driver/
  completion-aggregator machinery completely unmodified (already proven by
  `backup_fault_corpus.rs`), so re-testing that path here would just be a
  second, weaker copy of that corpus. What genuinely is new PITR logic —
  the retention janitor's own **keep-anchor predicate** (never mark a
  table's newest base-snapshot-at-or-before-the-floor for reclaim, since
  every still-retained segment sealed after it needs it as a replay base)
  — is reproduced verbatim from `animusd::pitr_janitor`'s own unit-tested
  pure function and proven here under **randomized** interleavings of
  base-snapshot/segment seal times, not just the janitor's own hand-picked
  cases.
- **Frozen named cells**: `quiet_table_pitr_rollover` (baseline seal +
  content match), `idle_group_never_proposes_a_pitr_seal` (the quiescence
  contract's structural half: nothing pending ⇒ no store `put`, no
  propose), `kill_sealing_leader_pitr_converges` (a crash between the
  store `put` and the catalog commit, then a leader failover, then the
  idempotent retry re-seals the full backlog), `disable_then_reenable_
  resets_generation_and_continues_epoch_chain` (a fresh generation, but the
  SAME tablet's own epoch chain continues, never resets), `split_children_
  seal_independently_and_inherit_generation` (a control-metadata-only
  `BeginSplit`/`CutoverSplit` cutover; each child seals its own epoch 0
  independently, inheriting PITR from the table spec with zero
  special-casing since `table_pitr` is table- not tablet-scoped; the union
  of parent-plus-children content covers the full journal with no
  double-counting), `drop_table_then_segments_and_generation_floor_survive`
  (the catalog's deliberate outlives-the-table override of the streams
  retention-zero rule), and `retention_keep_anchor_never_orphans_a_needed_
  replay_base` (the randomized keep-anchor property, above). Depth knob
  `ANIMUS_PITR_SEEDS` (default 1 = the frozen cells; held green at `=300`
  in ~1s release / well under a minute debug).
- **A real modeling bug this corpus found on its own split scenario's
  first run** (not a production bug — a test-harness one): the scenario
  originally reused the PARENT's own `engines()` map for both split
  children, giving parent and children the identical physical
  `MemoryEngine` per node — sibling tablets share nothing in production
  (ADR 0050 rung 1/2's whole point), so a child's own `pending_changes()`
  scan silently saw the parent's pre-split records too, corrupting the
  seal content. Fixed by giving each child its own fresh `engines()` map,
  mirroring `stream_lineage_corpus.rs::scenario_copy_split_children_born_
  empty`'s own (already-correct) three-separate-maps precedent, which this
  file's first draft failed to copy faithfully. See `docs/engineering-
  lessons.md` for the general lesson.
- **Train 3 PR② adds a `RestoreTableToPointInTime` tier**: five more named
  cells, verified against the **real** `Metadata::pitr_replay_segments`
  (called directly, never reimplemented) applied to segments this
  corpus's own `pitr_seal_now` reimplementation sealed, decoded and
  reduced last-writer-wins-by-HLC, and diffed against an independently-
  tracked model — `assert_replay_matches_model`, the restore-side sibling
  of `verify_pitr_lineage` above. `restore_to_random_second_matches_the_
  model_with_a_leader_kill` (the flagship property: mixed writes across
  several rounds, a leader kill mid-stream, checked against a model
  snapshotted at *every* successful seal, not just the last one — proving
  replay is correct at any point in the timeline, not merely at the end)
  and `restore_to_random_second_matches_the_model_across_a_split` (the
  same property carried across a cutover, parent and both children sealing
  independently) join `pitr_restore_window_scopes_to_the_latest_
  generation_under_random_cycles` (the generation-gap validation property
  under randomized disable/re-enable cycling, not just one hand-picked
  gap), `deleted_table_pitr_restore_matches_the_model` (a table dropped,
  not split, still replays correctly — this is the regression shape for
  the `live_split_descendants` bug below), and
  `use_latest_restorable_time_matches_the_full_model`. Held green at
  `ANIMUS_PITR_SEEDS=300` (~8s) alongside the PR① cells above (same file,
  same knob).
- **A real production bug this restore tier's own first run found, not
  review**: the first `Metadata::pitr_replay_segments` was built on
  `live_split_descendants` (ADR 0059 §6's on-demand-capture re-planning
  accessor), which returns empty for a tablet retired by an ordinary
  `DropTableTablets` (no `split_lineage` entry — that map only ever gets a
  row from a *split*) — so a deleted-table PITR restore silently replayed
  nothing. Rewritten as a direct forward DFS over `split_lineage` that
  includes every visited tablet's own segments regardless of current
  liveness; see the ADR's Train 3 PR② as-built amendment for the full
  account.
- **Three harness bugs this tier's own build found in itself, not in
  production** (fixed before the cells were trusted at depth): (1) a
  leader-kill scenario calling `pitr_seal_now` on the newly-elected leader
  immediately after the kill, with no intervening confirmed write, could
  read `pending_changes()` before that leader's own apply cursor caught up
  to the crashed leader's last committed entry — leadership and
  apply-catchup are not the same event; fixed by moving the kill to
  *before* that round's own write burst, whose internal confirm-by-
  applied-index forces the catchup as a side effect. (2) the split
  scenario picked write keys from the *whole* keyspace for both children,
  so a write occasionally targeted a key outside its own group's declared
  range and silently no-op'd at apply time (the routing-bug tripwire this
  file's `animusd/CLAUDE.md` entry names) — fixed with a `write_burst_
  ranged` helper taking an explicit per-group key range. (3) the
  deleted-table scenario proposed `DropTableTablets` on a tablet never
  registered via `MetaCommand::CreateTablet`, so the drop was silently a
  `NoOp` and the test proved nothing — fixed by registering the tablet
  first. All three are hand-scripted-`Metadata`-corpus pitfalls, not bugs
  in `pitr_replay_segments`/`pitr_seal_now` themselves; see
  `docs/engineering-lessons.md` for the general lessons.
