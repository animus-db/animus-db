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

Env knobs at a glance (details in the sections below):

| Knob | Default | Effect |
|------|---------|--------|
| `ANIMUS_SEED` | unset | replay one failed sim run from its printed seed (repo-wide convention) |
| `ANIMUS_RAFTKV_SEEDS=K` | 1 | K seed variants per raftkv-corpus cell |
| `ANIMUS_RAFTKV_LSM=1` | off | run the whole raftkv corpus over `LsmEngine<SimEnv>` |
| `ANIMUS_TXN_SEEDS=K` | 1 | K seed variants per multi-tablet transaction-corpus cell (ADR 0018) |
| `ANIMUS_STREAM_SEEDS=K` | 1 | K seed variants per DynamoDB Streams lineage-walk cell (ADR 0042/0043) |
| `ANIMUS_BACKFILL_SEEDS=K` | 1 | K seed variants per secondary-index backfill fault-injection cell (ADR 0045) |

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
  hash ring. `cursor::cursor_key` truncates `range.start` to its own
  8-byte token, which then sorts *below* the child's own (longer)
  `range.start` the instant the byte right after the token is non-zero
  (true for any real `escape(pk)`). Data coverage itself was never at
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
