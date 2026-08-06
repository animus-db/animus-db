# CLAUDE.md — animus-cp-data

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The **leaderful, strongly-consistent (CP) data plane** (ADR 0016, ADR 0017): each
tablet is its own Raft group with a single leader serving **linearizable**
single-tablet reads/writes, durable on a real `StorageEngine`. It is the CP
counterpart to the leaderless AP `animus-data` plane, built **additively** — AP is
untouched, no dual-mode seam yet. The control plane (`animus-control`) remains the
metadata authority (tablet map, placement, failure detection) in both modes.

## How it reuses the control-plane Raft

It instantiates `animus-control`'s generic, sync, I/O-free `RaftCore<C, S>`
(ADR 0009) with `C = KvCommand` and a **`DRIVER_APPLIED` state machine**
(`KvState`, a unit placeholder): the core agrees the order of commands but does
**not** apply them in-core (a `StorageEngine` apply is async I/O the sync core
can't do). Instead the core buffers committed-and-durable commands as effects;
this crate's **async driver** drains them (`RaftCore::drain_apply`) and applies to
the engine — the `AccordCore` sync-core/async-driver split.

## Entry points

- `KvCommand` (`Put`/`Batch`/`Delete`/`Cas`/`Split`/`NoOp`), `KvState` (the `DRIVER_APPLIED` SM).
- **Batch put** — `KvCommand::Batch(Vec<(k, v)>)` + `RaftKvNode::put_batch` commit
  **N keys as one Raft log entry** (one propose → one commit round → one apply), the
  bulk-write throughput primitive. Applied as one arm in `apply_and_compact`: every
  key merges at the entry's shared Raft `index` (the MVCC version — keys are
  distinct, so per-key LWW is well-defined; `engine_applied` advances once past the
  whole batch at end of the loop iteration). Re-applies idempotently on recovery
  exactly as a single `Put`. Uses the normal per-key `merge` path, so it composes
  with a coalesced-fsync `merge_batch` optimization. `tests/batch.rs`.
- `RaftKvNode<E, S>` — a running tablet-group node: `start(env, all_nodes,
  storage)`, `put`/`put_batch`/`delete` (proposed via Raft, honored on the leader), `is_leader`,
  `linearizable_get` (ReadIndex), `local_get` (a replica's raw engine read — *not*
  linearizable; a test/observability aid).
- **Linearizable CAS** — `cas(key, expected, value) -> ProposeResult` proposes a
  `KvCommand::Cas` (set iff the committed value `== expected`; `expected: None` ==
  "only if absent"); `cas_result(index) -> Option<bool>` reads the outcome recorded
  at the `Accepted { index }` once that entry applies; `compare_and_swap(key,
  expected, value) -> Option<bool>` is the all-in-one (propose on the leader, wait
  for the entry to apply, return the recorded outcome — `None` if not leader / times
  out). All public-additively; existing signatures unchanged.
- **Admin/debug accessors** (ADR 0020, consumed by `animusd`): read-only
  `role`/`term`/`commit_index`/`last_applied`/`durable_index`/`snapshot_index`/
  `log_len` (thin locks over `RaftCore`), and `storage()` (a `&S` borrow so the
  assembly layer can surface the engine's SSTable/WAL debug views without engine
  state leaking into the consensus core). **`engine_applied_index()`** is the
  confirm-by-index primitive (audit A4): the engine-merge watermark linearizable
  reads gate on, so a proposer confirms a specific `Accepted { index }` applied
  (`engine_applied_index() >= index` while still leader in the proposal's term)
  instead of polling value equality, which false-negatives under a concurrent
  same-key overwrite. `linearizable_scan` pushes a bounded range into
  `storage.scan(start, end)` (key-ordered by contract — no re-sort, no
  whole-tablet materialization); only the unbounded-above case still reads
  `entries()` (the trait's `scan` has no open upper bound).
- `KvWire` — the data-plane wire enum wrapping `RaftMsg` plus the ReadIndex
  read-barrier probes (`ReadProbe`/`ReadProbeAck`). The probes are driver-only, so
  ReadIndex lives entirely in this crate and the shared `RaftCore`/`RaftMsg` are
  untouched.

## What's non-obvious

- **The driver is split into two tasks: a consensus loop + an apply task.** This is
  the **driver-liveness fix** (ADR 0017): engine apply and compaction are slow
  (~180–300ms for a batch of LSM merges + a compaction rewrite on a real disk) and
  used to run *inline* on the same loop that services Raft messages, so under write
  load the driver blocked past the 150ms election timeout → followers campaigned → a
  **leader-election storm** (term climbed continuously) that truncated in-flight
  writes and collapsed throughput to ~15/s. Now:
  - **Consensus loop** (`drive`): recover from WAL, spawn the apply task, then loop
    `persist_wal` (drain WAL records → append + `fsync` → `mark_durable_through`,
    under `wal_lock`) → `select(recv, timer)` → step the core → `persist_wal` again
    (durability before shipping) → send. It does **no** engine apply, so it always
    heartbeats/acks within the election timeout. Heartbeat ticks re-replicate.
  - **Apply task** (`apply_loop` → `apply_and_compact`): install received snapshots,
    `drain_apply` → `merge`/`merge_tombstone` into the engine in commit order, and
    compact — all off the consensus loop. Backs off (`APPLY_IDLE_POLL`) only when
    idle; under load it stays in lockstep behind commit.
  - **Two invariants the split introduces:** (1) the core's `last_applied` (a buffer
    cursor advanced by the consensus loop) now *leads* the engine, so linearizable
    reads gate on a separate **`engine_applied`** atomic the apply task advances
    after each merge — never `last_applied` (else a read could observe past the
    engine). (2) The WAL file is written by both tasks (append vs. compaction
    rewrite), serialized by an async `wal_lock`; compaction snapshots only up to
    `engine_applied` via `RaftCore::snapshot_upto` (not `last_applied`, which the
    engine hasn't merged yet) and **discards the consensus loop's pending records**
    in the same locked block (`replay` is push-based → re-appending would duplicate).
    Compaction is skipped while `halted` (it's a WAL-bounding optimization; a rewrite
    racing teardown can fail the `replace`). `is_stopped()` requires *both* tasks
    stopped (`stopped` && `apply_stopped`) before the GC deletes artifacts.
- **Wake-on-propose cuts single-write latency.** `put`/`delete`/`cas`/
  `propose_split`/`change_membership` route through `propose_and_wake`: after the
  core appends the entry, the proposer raises a `ProposeSignal` (`AtomicBool` +
  `futures::task::AtomicWaker`) that the consensus loop races as a third arm of its
  `select(recv, timer)`. On that wake the loop calls `RaftCore::replicate_now`
  (broadcast `AppendEntries` immediately, resetting the heartbeat deadline) instead
  of leaving the entry parked until the next ~50ms heartbeat tick. **`AtomicWaker` is
  deliberately executor-agnostic** — it works under both `SimEnv`'s `ArcWake`
  executor (the wake runs synchronously on the single thread, marking the driver task
  ready for the next run-loop poll — fully deterministic, no wall clock) and tokio's
  multi-threaded `ProdEnv` (it resolves the register/wake race); **no tokio-only
  primitive** is used, so determinism holds. The `ProposePending` future *registers
  the waker before checking the flag* (the AtomicWaker discipline against a lost
  wakeup) and consumes the flag (`swap(false)`) on resolve, so it never busy-spins.
  A `NotLeader` propose appends nothing, so it doesn't wake. Latency-verified over
  `ProdEnv` in `animusd/tests/cp_plane.rs::single_write_latency_is_low` (median
  ~52ms → ~11ms).
- **The Raft log index is the MVCC version.** Apply uses `index` as the engine
  `version`, so per-key LWW reproduces the agreed Raft total order, and re-applying
  on recovery is idempotent.
- **CAS is decided at *apply* time, not propose time — that is what makes it
  linearizable + contention-correct.** The `RaftCore` agrees only the command
  *order*; `Cas` rides through it as opaque data (sync core untouched, like every
  other effect). `flush_and_apply` evaluates it in commit order: it `storage.get`s
  the key's *current committed* value (every earlier entry in the same batch has
  already merged, so this is the post-predecessor state) and compares to `expected`;
  equal → `merge` at `index` (same write path as `Put`), else no-op. Because every
  replica applies the same order against the same committed state with no clock/RNG,
  every replica makes the **identical** accept/reject decision. Two CAS racing from
  the same `expected` therefore have **exactly one winner** — whichever Raft ordered
  first: its swap moves the committed value, so the second's compare then fails.
- **CAS outcome plumbing: keyed by the Raft log index.** `propose` returns
  `Accepted { index }`; apply records `CasResults.outcomes[index] = swapped` (a
  `BTreeMap<u64,bool>` in driver state, mirroring `animus-consensus`'s `ReadResults`
  stash). The proposer waits until the entry applies (`last_applied >= index`, or
  just polls `cas_result`) and reads it. No wall clock in the wait — `compare_and_swap`
  uses only `env.now()`/`env.sleep()`, so it stays a pure function of the seed.
- **Durable-before-visible holds** (ADR 0009): effects are only drained for fsynced
  entries, and the engine write follows the WAL `fsync`.
- Distinct WAL file (`raftkv.wal`) from the control plane's `raft.wal`, so a node
  can host both planes. The name is exported (`animus_cp_data::WAL`) so the
  drop-table GC (ADR 0024) can delete a stopped group's WAL.
- **`shutdown()` is a graceful driver halt, not a kill** (ADR 0024): it latches a
  flag the driver observes at the top of its loop — i.e. *between* full
  persist+apply passes and within one wake (message or pending timer), so the WAL
  and engine are never left mid-write. Poll `is_stopped()` before touching the
  group's files. A halted node's accessors still answer from the **frozen** core
  (a halted leader keeps reporting `is_leader() == true`), so never route to a
  handle after unregistering it; a halted node must not be reused — restarting
  the tablet means a fresh `start` (the sim tests: `tests/shutdown.rs`).

## Stage status (ADR 0017)

- **B.1 (done)** — single-group driver + write path; `tests/single_tablet.rs`
  (writes replicate + apply on every replica; survive a leader kill + rejoin
  catch-up; trace reproducibility).
- **B.2 (done)** — linearizable **ReadIndex** reads (`linearizable_get`): the
  barrier first waits for the leader to have **committed an entry of its own
  term** (Raft §6.4's mandatory half — `commit_index >=
  RaftCore::first_term_index()`, the election no-op; a fresh leader's log holds
  every acked entry but its `commit_index` may not cover one the old leader
  committed, and the term-only probe can't see that), then a read-barrier quorum
  probe (`KvWire::ReadProbe`/`Ack`, driver-only) confirms the leader still leads
  its term, then it serves locally once applied (gated on `engine_applied`). No
  log entry, no wall clock; the whole barrier shares one `READ_TIMEOUT`.
  `tests/read_index.rs` (reads reflect committed writes + RYW; a
  deposed/partitioned leader returns `None`, never a stale value) and
  `tests/read_index_fresh_leader.rs` (drives *into* the fresh-leader window at
  1ms sim granularity — uncommitted acked entry on the heir + a ~40-entries-
  behind third replica so the no-op commit needs ~40 backtrack round-trips while
  the probe needs one; the read must wait and serve the acked value, never the
  stale one).
- **CAS (done)** — linearizable **compare-and-swap** (`cas`/`cas_result`/
  `compare_and_swap`, `KvCommand::Cas`). Decided at apply time in commit order
  against the committed engine state (deterministic; sync `RaftCore` untouched —
  CAS is opaque command data); outcome stashed in driver `CasResults` keyed by the
  log index. `tests/cas.rs` (concurrent same-`expected` race → exactly one winner,
  agreed on every replica; CAS-if-absent; a successful CAS survives `stop`+restart
  via WAL replay re-apply; seed sweep + trace reproducibility).
- **A.2 (done)** — compaction + streaming `InstallSnapshot`, with **lazy
  on-demand engine images** (audit P1/P5). The driver compacts once
  `COMPACT_THRESHOLD` entries apply: the core truncates the log prefix and the
  WAL is rewritten to its bounded image — **no whole-tablet scan/serialize on
  the threshold path**, and no image retained in the core at rest. Only when a
  replication attempt actually needs to ship a snapshot does the core raise
  `RaftCore::take_snapshot_needed`; the apply task then scans the engine
  (`engine_image`), re-bases (`snapshot_upto(engine_applied)` *before*
  `set_snapshot_blob`, so base and image agree), and the next heartbeat ships
  the chunks. The receiving follower writes the bytes into its engine
  (`drain_pending_install` → `merge`) and replays the log tail on top; it does
  *not* retain the bytes — a later re-ship regenerates from its engine (the
  second-hop invariant, and it now also covers a **recovered** leader, which
  used to ship 0 bytes until its next compaction). Wire + image ride the
  crate's compact **binary codec** (`codec.rs`, audit P2 — length-prefixed
  framing like the storage manifest codec; serde_json's decimal-array `Vec<u8>`
  rendering cost ~3–4x; decode failures are loud: magic/version-checked `Err`s
  logged before the message is dropped; the Raft WAL keeps the shared
  control-plane serde_json `PersistedState` format).
  `tests/snapshot_catchup.rs` (crash a follower, write past the
  threshold so the leader compacts, restart → it catches up via snapshot).
- **C (done)** — single-server Raft **membership change** (`change_membership`):
  config lives in the log (`RaftCore`, branched so the control plane is unchanged);
  a node uses the latest log config for quorum/election, the config rides snapshots
  + `InstallSnapshot`, a removed node stops campaigning, and changes are restricted
  to a single-server delta + one-in-flight + no leader self-removal.
  `tests/membership.rs` (remove a follower, add + catch up a node, reconfigure off
  a crashed node, reject multi-server/self-removal, reproducibility). The
  **automatic trigger is now wired** (SimEnv): `reconfigure_step` takes one
  single-server step toward a desired voter set (remove an extra non-leader voter
  before adding a missing one), and `spawn_reconfigure_loop` drives it from an
  **epoch-driven pull** — each group leader polls the control plane's replicated
  `Metadata.tablets[t].replicas` and reconfigures itself (no new control→data
  command; mirrors the control plane's `reconcile_loop` — decision in
  `reconfigure_step`, timing in the loop). `tests/reconfigure_trigger.rs` proves
  the end-to-end cascade (crash → detector `Down` → reconciler `CasTabletReplicas`
  → group leader swaps the dead node for a same-zone spare, which catches up and
  the group keeps serving). The `ProdEnv`/`animusd` production assembly (hosting
  groups + leader-reporting for routing) remains.
  - **Test gotcha (membership):** pre-start a to-be-added node knowing only the
    *current* voters, NOT itself — a node started inside its own initial config is
    a voter that can campaign, win, and inject itself into the group before the
    real add (`RaftCore::start_election` gates on `is_voter`). A `RaftKvNode::start`
    whose `all_nodes` excludes its own id is a quiet non-voter until the leader adds
    it. (Caught by the `reconfigure_trigger` seed sweep — a single seed hid it.)
- **D (done)** — **tablet split** (`propose_split`): the split point is agreed via
  a committed `KvCommand::Split { at }`, so every replica splits at the same point
  in the command order; on apply each replica **tombstones the handed-off range**
  `[at, ∞)` (it now serves only `[lo, at)`), and that range is seeded into a new
  independent group (`range_snapshot` → `start_seeded`). `tests/split.rs` (the
  original keeps the lower range + drops the upper on every replica; the new group
  serves the upper range; both operate independently; reproducibility).
  **In-band new-group creation is now wired** (the deferred `Env`-seam extension):
  the new `animus_env::Coresident` sub-trait (`sibling(id) -> Self`, impl'd for
  `SimEnv`) lets a replica mint a co-resident inbox at runtime, and the driver
  gained an optional **split hook** (`start_with_split_hook` +
  `in_band_split_hook`). On apply of `Split`, `flush_and_apply` captures the
  handed-off `[at, ∞)` range and invokes the hook; the in-band hook mints
  `sibling(my_new_id)` and `start_seeded`s the new-tablet replica there (collected
  into a caller sink for observation). Wire one hook per original replica → on
  apply the new group forms with no external handoff. `tests/split_in_band.rs`.
  Decided seam (per maintainer): SimEnv first; `Coresident` is a *separate* trait
  bound only on the split path, so `ProdEnv`/other envs and the external-handoff
  `split.rs` (hook = `None`) are untouched. **The hook fires on every apply**, so a
  `Split` re-applied after a crash recovery would mint the sibling twice — but
  **recovery-idempotency is now handled at the `animusd` assembly layer** (#2/#4):
  the ProdEnv hook gates on a per-node `minted` set that is pre-populated at start
  from a durable `cp-hosted` marker, so a re-applied `Split` finds the tablet already
  hosted and does not re-mint (and the tablet is instead re-hosted from its on-disk
  engine). **Deep splits (D3):** a split-created group can be split again — `animusd`
  starts *every* group with a hook (`start_seeded_with_split_hook` /
  `start_with_split_hook`) and derives member ids **flatly** from the node's base id
  (`base + tablet * STRIDE`, matching the reconfigure loop at any depth), so
  auto-sharding keeps working as a shard grows. **Remaining limitation:** the new
  group's ids are derived in `animusd` rather than allocated by the control plane's
  `SplitTablet`, and `Metadata.tablets[new].replicas` records the parent's base ids,
  not the derived member ids (the data plane translates per tablet) — fine for
  realistic clusters. **`propose_split`'s `ProposeResult::Accepted` is not
  confirmation** — like every proposal here, it only means the entry was appended
  to the leader's local log; a caller must poll
  [`current_split_bound`](RaftKvNode::current_split_bound) before trusting it, the
  same way `engine_applied_index` is polled to confirm a write. `animusd`'s
  `propose_split_data`/`cp_split_here` learned this the hard way: trusting
  `Accepted` let an accepted-but-never-committed `Split` (truncated by leader churn)
  report false success, permanently stranding the tablet its metadata layer had
  already created. **The confirmation must compare the *exact* key, not just "has
  this group split"**: if two callers race with *different* keys on the same
  tablet (a real scenario under `animusd`'s `--cluster N` shared-edge redundant
  triggering — see its `CLAUDE.md`), the loser's bare "did *a* split happen" check
  would pass even though its own key never applied.
- **A group can be split more than once over its life** — `KvCommand::Split`'s
  apply-time check is a CAS against the group's *current* boundary
  (`current_split_bound`), not the one-shot "has it split at all" latch this
  started as. `Split { at }` is accepted iff `at` is strictly less than the
  current boundary (`None` — never split — always accepts); each accepted split
  narrows the boundary further, so the sequence only ever moves toward smaller
  keys. This still rejects the race the original one-shot guard was built for —
  two proposers racing to split the *same* still-equally-bounded group at the
  same moment, whichever commits second finds `at` no longer strictly less than
  the boundary the first just set — while allowing a tablet that regrows past a
  threshold to shard again, any number of times, instead of being permanently
  frozen after its first split (the original design's actual limitation, not a
  deliberate choice — lifting it needed `animusd`'s `auto_split_loop` to stop
  treating "already split once" as permanent exclusion too; see its `CLAUDE.md`).
  `tests/cp_deep_split.rs` (a split-created tablet can be split again, via
  `animusd`), `animusd/tests/cp_plane.rs::already_split_tablet_splits_again_once_it_regrows`.
  - **The boundary is `current_split_bound`, deliberately just the current
    value — not a per-split history.** A history would let a caller confirm
    "did my exact key ever apply" unambiguously forever, but that's O(n) state
    that grows for the life of a heavily-resplit lineage, and costs O(n²) total
    to maintain (each split re-persists the whole history). Instead
    `current_split_bound` only answers "is `K` still a legal *new* split point"
    reliably; "did key `K` apply" is answered *reliably only while `K` is still
    the current value* — once a *later* split narrows past `K`, this can no
    longer distinguish "`K` applied, then something else narrowed further" from
    "`K` never applied, something else did instead." That ambiguity is
    intentional: `animusd` never trusts it alone to *delete* anything (see its
    `CLAUDE.md`'s `drop_orphan_tablet` note) — a caller confirming success
    treats "still equal" as certain success and anything else as "stop retrying
    this key," and a *second*, independent check (local hosting) is what
    decides whether the ambiguous case is actually safe to clean up. Bounded
    O(1) state forever is worth a caller-side ambiguity that has a cheap,
    independent safety net.
  - **Durability: stored *inside the engine*, not in Raft-log/WAL-snapshot
    state.** `RaftCore`'s `DRIVER_APPLIED` contract keeps `core.metadata`
    (`KvState`) a permanent unit placeholder for this state machine — real state
    lives only in the engine, never in-core (see the driver-liveness /
    `DRIVER_APPLIED` snapshot notes above). So `current_split_bound` rides as a
    normal entry under a reserved key (`SPLIT_BOUND_KEY`), written via the
    *same* `merge_batch` call as a `Split`'s tombstones (one fsync, not two).
    This is what makes it survive WAL compaction and process restart correctly:
    a bare in-memory flag (the original `applied_split_key`) is *not*
    recoverable once a `Split` entry's Raft index falls below
    `snapshot_index` — compaction discards it from the log for good, and
    nothing else re-derives that the group had already split (a real, latent
    bug in the original one-shot design, never triggered because nothing
    combined enough writes-after-a-split with a restart to hit it). Recovered
    at driver startup (`drive`) via a direct `storage.get(SPLIT_BOUND_KEY)` —
    never from WAL replay — and re-synced the same way after installing a
    received snapshot (`apply_and_compact`'s `pending_install` branch), since
    `RaftCore::handle_install_snapshot` clears this follower's log on
    completion. **Excluded from every application-facing read** (`keys_from`
    — also the split handoff, so a child never inherits the parent's boundary
    — and both branches of `linearizable_scan`); *not* excluded from
    `entries_with_tombstones` (`engine_image`'s source), since the snapshot
    image is exactly what should carry it to a lagging follower or a restart.
    `tests/cp_rehost.rs::split_tablet_survives_cluster_restart` exercises the
    restart path end to end (via `animusd`).

## Tests

`cargo test -p animus-cp-data` — `tests/single_tablet.rs` (SimEnv; drive with
`run_for`, never `run()` — the driver has perpetual heartbeat/election timers).
