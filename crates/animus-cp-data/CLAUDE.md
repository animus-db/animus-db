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

- `KvCommand` (`Put`/`Delete`/`Cas`/`Split`/`NoOp`), `KvState` (the `DRIVER_APPLIED` SM).
- `RaftKvNode<E, S>` — a running tablet-group node: `start(env, all_nodes,
  storage)`, `put`/`delete` (proposed via Raft, honored on the leader), `is_leader`,
  `linearizable_get` (ReadIndex), `local_get` (a replica's raw engine read — *not*
  linearizable; a test/observability aid).
- **Linearizable CAS** — `cas(key, expected, value) -> ProposeResult` proposes a
  `KvCommand::Cas` (set iff the committed value `== expected`; `expected: None` ==
  "only if absent"); `cas_result(index) -> Option<bool>` reads the outcome recorded
  at the `Accepted { index }` once that entry applies; `compare_and_swap(key,
  expected, value) -> Option<bool>` is the all-in-one (propose on the leader, wait
  for the entry to apply, return the recorded outcome — `None` if not leader / times
  out). All public-additively; existing signatures unchanged.
- `KvWire` — the data-plane wire enum wrapping `RaftMsg` plus the ReadIndex
  read-barrier probes (`ReadProbe`/`ReadProbeAck`). The probes are driver-only, so
  ReadIndex lives entirely in this crate and the shared `RaftCore`/`RaftMsg` are
  untouched.

## What's non-obvious

- **The driver mirrors the control-plane `RaftNode` driver, minus reconcile +
  failure-detection (control-plane only), plus engine apply.** Each loop:
  `flush_and_apply` (drain WAL records → append + `fsync` → `mark_durable_through`
  → `drain_apply` → `merge`/`merge_tombstone` into the engine, in commit order),
  then `select(recv, timer)`, step the core, `flush_and_apply` again (durability
  before shipping), then send. Heartbeat ticks re-replicate (no separate retry).
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
  can host both planes.

## Stage status (ADR 0017)

- **B.1 (done)** — single-group driver + write path; `tests/single_tablet.rs`
  (writes replicate + apply on every replica; survive a leader kill + rejoin
  catch-up; trace reproducibility).
- **B.2 (done)** — linearizable **ReadIndex** reads (`linearizable_get`): a
  read-barrier quorum probe (`KvWire::ReadProbe`/`Ack`, driver-only) confirms the
  leader still leads its term, then it serves locally once applied. No log entry,
  no wall clock. `tests/read_index.rs` (reads reflect committed writes + RYW; a
  deposed/partitioned leader returns `None`, never a stale value).
- **CAS (done)** — linearizable **compare-and-swap** (`cas`/`cas_result`/
  `compare_and_swap`, `KvCommand::Cas`). Decided at apply time in commit order
  against the committed engine state (deterministic; sync `RaftCore` untouched —
  CAS is opaque command data); outcome stashed in driver `CasResults` keyed by the
  log index. `tests/cas.rs` (concurrent same-`expected` race → exactly one winner,
  agreed on every replica; CAS-if-absent; a successful CAS survives `stop`+restart
  via WAL replay re-apply; seed sweep + trace reproducibility).
- **A.2 (done)** — compaction + streaming `InstallSnapshot`. The driver compacts
  once `COMPACT_THRESHOLD` entries apply: it snapshots the **engine image**
  (`set_snapshot_blob`), the core truncates the log prefix, and the WAL is
  rewritten to its bounded image. A lagging follower (behind the compacted prefix)
  is caught up by the chunked `InstallSnapshot` carrying the engine bytes, which
  the driver writes into its engine (`drain_pending_install` → `merge`), then
  replays the log tail on top. The `RaftCore` snapshot path branches on
  `DRIVER_APPLIED` (engine blob vs. in-core `metadata`), so the control plane is
  unchanged. `tests/snapshot_catchup.rs` (crash a follower, write past the
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
  `split.rs` (hook = `None`) are untouched. **Limitations (production-hardening,
  deferred to the `animusd` assembly):** the hook fires on every apply, so a
  `Split` re-applied after a crash recovery would mint the sibling twice
  (recovery-idempotency); and the new group's ids are wired per-replica here rather
  than allocated by the control plane's `SplitTablet`.

## Tests

`cargo test -p animus-cp-data` — `tests/single_tablet.rs` (SimEnv; drive with
`run_for`, never `run()` — the driver has perpetual heartbeat/election timers).
