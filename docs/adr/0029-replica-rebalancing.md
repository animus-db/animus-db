# ADR 0029 — Automatic tablet-replica rebalancing

- **Status:** Accepted — implemented in `animus-placement`, `animus-control`,
  `animus-cp-data`, `animusd`. Amends ADR 0005's placement reconciler and
  ADR 0017's per-tablet Raft membership-change primitive.
- **Date:** 2026-08-07

## Context

ADR 0005's placement reconciler is deliberately **violation-only**: given a
tablet's current replica set, `animus_placement::replan` seeds `must_keep`
from every still-eligible survivor and only fills the remaining slots up to
the replication factor. This is a good default — it repairs drift with
minimal churn — but it has a consequence operators hit immediately: growing a
cluster (e.g. 3 nodes to 5) moves **no existing data**. Every tablet already
has a full, healthy, policy-compliant replica set on the original nodes, so
`replan` never touches it; the new nodes register as `Active` members and then
sit idle forever, hosting only tablets created *after* the grow. `replan`'s
own test (`replan_is_a_noop_when_the_set_still_satisfies_the_policy`)
documents this as intentional. ADR 0017 and ADR 0002 both flag "the
rebalancer" as unimplemented future work.

Filling this gap turned out to need more than a planner. The per-tablet Raft
membership primitive (`RaftCore::change_membership`, ADR 0017 §C) was built
for **failure repair**: `RaftKvNode::reconfigure_step` removed an extra voter
before adding a missing one (restoring quorum margin ahead of a fresh,
not-yet-caught-up replica), and `change_membership` unconditionally rejects
removing the current leader ("transfer leadership first — out of scope
there"). Both are wrong, or at least incomplete, for a *healthy* move:

- Removing a live, fully-participating voter before its replacement has
  caught up needlessly drops quorum margin during the move — an availability
  regression relative to just leaving the extra replica in place a little
  longer.
- A rebalance plan will, in general, sometimes need to move the replica that
  happens to be hosting the group's current leader. `change_membership`'s
  self-removal rejection would make that move permanently stuck — and
  auditing the existing `drain` admin action showed this was already a live,
  if rare, gap: draining a node that leads one of its tablets' groups already
  parked that group at a stale, oversized config forever.

## Decision

We will add three independent, separately-landable pieces.

### 1. Safe membership mechanics for a healthy move (`animus-control`, `animus-cp-data`)

- **`RaftMsg::TimeoutNow`** and **`RaftCore::transfer_leadership`**: the
  current leader arms a transfer to a voter reasonably close to caught up
  (`peer_match(target) >= commit_index()`, no config change in flight; a
  fresh arm also records a deadline one election timeout out). **Hardened by
  a follow-up fix** (see the root `CLAUDE.md` engineering-practices entry):
  the arm gate was originally `peer_match(target) == last_log_index()`, which
  under sustained writes on a write-hot tablet is essentially never true at
  the instant the reconfigure loop samples it — `propose` is fire-and-forget,
  so `last_log_index` moves the instant a write is accepted, before any
  replication round trip, while the target's `peer_match` still reflects the
  *previous* entry. The arm silently failed forever and the discarded `bool`
  meant nothing surfaced it. Standard Raft §3.10 semantics close this: once
  armed, **`propose`/`change_membership` freeze** (`NotLeader`, hinting the
  transfer target) so the log stops growing and replication can close the
  remaining gap; `broadcast_append` sends `TimeoutNow` only once the target
  actually **reaches `last_log_index()`** (re-sent every heartbeat after that
  until this node steps down — resilient to one dropped message); and a
  transfer whose target never steps down by the deadline **aborts**,
  resuming proposals rather than stranding the group frozen. A caller may
  re-arm the same already-armed target every tick (idempotent) without
  resetting the deadline — only a fresh arm (first time, or a different
  target) starts a new one, so a caller that retries every tick can't starve
  the abort check. The receiver, on a matching-term `TimeoutNow`, calls
  `start_election` **directly** — bypassing pre-vote, since pre-vote's
  live-leader-lease protection exists to stop a partitioned node from
  disrupting a healthy leader, which does not apply when the healthy leader
  itself requested the handoff.
- **Departing-peer notification**: `broadcast_append` keeps replicating a
  removing config entry to the peer it just dropped from `peers` — via a
  leader-local `departing: BTreeMap<NodeId, u64>` (peer → removal index),
  cleared once that peer's `match_index` reaches the removal index — instead
  of leaving a removed node to infer its own removal only from a rejected
  pre-vote. This gives the release-GC below (§3) a durable, replay-independent
  "am I still a voter" signal.
- **`RaftKvNode::reconfigure_step(desired, down)`** becomes down-aware and
  priority-ordered: (1) remove an extra `Down` voter first (unchanged failure
  repair — nothing to wait for, it isn't acking anyway); (2) add a missing
  voter (a transient oversized-but-not-undersized config, strictly safer than
  dropping first); (3) remove an extra **healthy** voter only once every
  member of `desired` has caught up to `commit_index` (the new safety gate);
  (4) if the only remaining delta is removing the leader's own replica,
  transfer leadership to the lowest-id caught-up member of `desired` instead
  (selecting `peer_match(n) >= commit_index()`, now consistent with the arm
  gate above — the two were originally mismatched, see the follow-up fix
  note). **Hardened by the same follow-up fix**: step 1's search for a `Down`
  extra originally reused the generic "lowest-id extra" helper and only then
  filtered it on down-ness, so a `Down` extra sorting *after* a healthy one
  was invisible — the ungated removal never fired, and the step fell through
  to step 3's catch-up-gated healthy removal, which could then block the
  *entire* step behind an unrelated `desired` survivor's catch-up state. Step
  1 now searches directly for an extra that *is* down
  (`current.difference(desired).find(|n| down.contains(n))`, independent of
  id order) before ever considering a healthy one. `reconfigure_step` also now
  traces (via `tracing`) both a successful arm and an arming failure at step
  4, so a stalled transfer is no longer silent.
  — the new leader's next tick performs the removal as an ordinary, non-self
  step.

### 2. A pure rebalance planner + a paced trigger (`animus-placement`, `animus-control`)

- **`animus_placement::rebalance_step`**: given every policied tablet's
  current replicas and the live candidate pool, compute per-node replica
  counts (seeded 0 for a brand-new node, so it counts as a rebalance target)
  and propose **at most one** balance-improving move per call — from the
  most-loaded eligible source to the least-loaded eligible destination,
  subject to residency and spread still holding on the post-move set. One
  move per call bounds churn to one CAS per evaluation and, applied
  repeatedly, converges to max−min ≤ 1 without oscillating (each accepted
  move strictly reduces the sum of squared per-node counts).
- **`Metadata::rebalance`** wraps the chosen move in the existing
  `CasTabletReplicas{tablet, expected_epoch, replicas}` command — no new
  `MetaCommand` variant, so no relay-allowlist change. The **same**
  `reconcile_loop` that already drives repair drives this: each tick runs
  repair first, and only when repair proposed nothing does it evaluate
  `rebalance()` on a slower cadence (every `REBALANCE_EVERY_N_TICKS`). That
  cadence is pure churn control, not a safety invariant — correctness is
  carried entirely by the epoch-CAS (a stale move is rejected) and by §1's
  data-plane catch-up gate.
- A **split child now inherits its source tablet's placement policy** —
  previously `SplitTablet`'s apply copied range/replicas but not the policy,
  leaving every split child invisible to both repair and rebalance.

### 3. Removed-replica GC (`animusd`)

Moving a healthy replica off a node was, before this change, a one-way trip
with no cleanup: the per-node `cp_gc_loop` only ever reclaimed a tablet
**absent** from `Metadata.tablets` (the drop-table case, ADR 0024); a node
dropped from a still-*existing* tablet's replica set kept hosting an idle
group with un-erased scoped data forever (already true for a manual `drain`
or a failure-repair swap, not just for rebalance). `cp_gc_loop` gains a
**second phase**, the release dual of reclaim, gated on:

1. This node's own **local, durable Raft config** for the tablet already
   excludes it (`CpGroup::config()`) — the replay-independent anchor §1's
   departing-peer mechanism guarantees, as opposed to replicated `Metadata`,
   which a restarting control replica replays through transient historical
   states.
2. An **epoch-stability dampener**: the release condition must hold for
   `RELEASE_CONFIRM_TICKS` consecutive ticks at an unchanged tablet epoch,
   so a replay transient can't trigger a release and a re-add (which bumps
   the epoch via its own CAS) cancels one in progress.

Once confirmed, release runs the identical `cp_gc_tablet` teardown (stop the
group, erase its `StorageScope` range, delete its WAL) that reclaim already
used.

## Consequences

- A cluster grown N→M nodes now spreads its existing tablets across all
  `Active` members automatically, converging to a per-node replica-count
  imbalance of at most 1, with no operator action beyond registering the new
  nodes (which already happens automatically via `bootstrap`).
- The manual `POST /admin/raftkv/reconfigure` action and `drain` both
  inherit the safer "add before remove, gated on catch-up" ordering and the
  leadership-transfer fallback for free (passing an empty down-set), fixing
  the pre-existing "draining a group leader parks it forever" gap as a
  side effect.
- **Accepted residual gap**: a node that crashes *before* it ever receives
  its removal config entry recovers a Raft log whose config still lists
  itself, so the release GC's local-config gate never passes and that node
  leaks the idle group forever. This is the same shape every removal path
  already had before this change (nothing released such a tablet on any
  node), so it is not a regression — just not fully closed. A future fix
  would need a leader to re-derive "who still needs to learn of a past
  removal" from retained log configs after a leadership change, not just
  the current leader's own in-flight `departing` set.
- `MergeTablets` requires two tablets' replica sets to already coincide;
  rebalancing can diverge adjacent siblings' sets and block a later merge
  until they re-converge. Left unaddressed — merge is deferred/operator-driven.
- No new wire-visible `MetaCommand`; the new `RaftMsg::TimeoutNow` variant is
  additive on the shared Raft wire (and on `animus-cp-data`'s binary codec,
  version-bumped) exactly as `PreVote` was.
