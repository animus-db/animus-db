# ADR 0058 — Learner replicas and in-place tablet split

- **Status:** Accepted — implemented (Trains 1–2, rungs 1–4 layers 1–2, all
  as-built notes below; accepted by the maintainer merging the
  implementation stack on 2026-08-25: interim GSI-drain acceleration,
  learner class + reconciler adoption, SSTable clone, in-place split core,
  the `--split-mode` driver layer, the rung-4 write-blip fixes, and the
  rung-4 layer-2 default flip). **`InPlace` is now `SplitMode`'s default
  everywhere** — `--split-mode copy` still selects the original ADR 0050
  workflow explicitly. **Rung 4's remaining layer** (delete the ADR 0050
  build driver, the freeze-as-outage path, and the cutover vetoes now that
  the copy path is no longer the default) is this ADR's only unimplemented
  piece. **(2026-09-01 amendment) Rung 4's remaining layer is now in
  progress, accepted by the maintainer as a G5 gate pass (2026-09-01) for a
  stacked deletion series (`docs/engineering-lessons.md` has the delivery
  shape): Layer A (delete every copy-split-pinned test, `tests/
  split_build.rs` included) shipped first; Layer B1 (this rung's own
  scope — delete the entire `animusd`-side copy workflow —
  `SplitBuild`/`split_driver_tick`/`ship`/`ship_all`/`tail_pass`/
  `seed_row_bytes`/`packed_hlc`/`prefix_upper`/`max_change_hlc`/
  `SEED_KINDS`/`SEED_CHUNK_BYTES`/`SPLIT_MAX_TAIL_PASSES` in
  `index_drain.rs`, `ClientCtx::seed_rows_local`/`seed_child_rows` in
  `write_path.rs`, the `SeedRows` wire RPC, and `animusd::config::
  SplitMode`/`ClientCtx.split_mode`/`--split-mode {copy,inplace}` itself —
  plus the now-production-dead `split_child_placement`/fork F5 mint-at-
  placement logic in `lib.rs`) is done. `MetaCommand::BeginSplit`'s own
  definition/apply/mirror/relayable-command classification in
  `animus-control` is deliberately untouched by B1 (production-dead,
  compiles) — its deletion is Layer B2's job. Layer C (the remaining root
  `CLAUDE.md`/this ADR prose sweep) follows once B2 lands.** **(2026-08-31
  amendment) Train 2 Stage 1/2 — the fused split+move
  half of fork F5, where `BeginSplitInPlace` recruits both children's
  *final* homes as learners on the parent before it can ever fork — is
  superseded by [ADR 0062](0062-fork-first-split-directed-placing.md)**:
  `HostAction::AddSplitLearner`, `host::plan`'s phase-1.5 learner-recruitment
  loop, and the `bootstrap_voters` learner-union capture described in Train
  2 below are **deleted**; a split now forks directly onto the parent's own
  current replicas, with a child's real final placement decided separately,
  once, at `CutoverSplit`'s own apply (`Metadata::split_placing`) and driven
  there afterward by ordinary rebalance-style convergence. Kept below
  verbatim as the historical record of Stage 1/2's original design; **Train
  1** (the learner membership class, `reconfigure_step`'s learner-phased add
  sequencing) and **Train 2's remaining stages** (the single-entry
  `SplitTablet` atomic mint, local SSTable-clone-and-trim materialization,
  the G4 crash-recovery contract, the deterministic-first-leader campaign
  fix, the eager-materialization wake, and G1's pre-cutover drain-gate
  placement) are **not** superseded — ADR 0062 depends on and reuses all of
  them unmodified. See ADR 0062's own "Relationship to ADR 0050/0058's fork
  F5" section for the full account of what changed and why.
- **Date:** 2026-08-24 (accepted 2026-08-25)
- **Proposes superseding:** [ADR 0050](0050-per-tablet-storage-copy-based-splits.md)'s
  **Decision 2** (the `BeginSplit`/build/freeze/`CutoverSplit`/retire
  workflow) **only**. ADR 0050's **Decision 1** (per-tablet private engines)
  and **fork F9** (`split_lineage`, written at one immutable moment) are kept
  as load-bearing prerequisites this design builds directly on top of — see
  "What this keeps from ADR 0050" below.
- **Amends:** [ADR 0017](0017-per-tablet-raft-data-plane.md) §4 (the
  original in-place-split design this proposal revives, in a different
  concrete shape — see "Relationship to the original §4 design"); [ADR
  0028](0028-shared-storage-single-command-split.md) (narrows the scope of
  its recon finding — see "The layout-scoping insight"); [ADR
  0029](0029-replica-rebalancing.md) and [ADR 0030](0030-online-cluster-growth.md)
  (a replica move gains a learner catch-up phase; the growth mirror is a
  possible special case of the same primitive); [ADR
  0031](0031-tablet-host-reconciler.md) (the reconciler's replica-move and
  split arms both change shape); [ADR 0044](0044-split-only-tablets.md)
  (split-only stands unmodified; this changes only *how* a split happens);
  [ADR 0046](0046-tablet-log-model.md) (principle 3 — "no consumer offset
  ever crosses a split" — is preserved, not weakened, by the copy-kinds
  filter at materialization).
- **Depends on:** [ADR 0009](0009-in-house-raft-over-env.md) (the `RaftCore`
  this proposal adds a membership class to), [ADR 0018](0018-cross-tablet-transactions.md)
  §2 (the log-position ordering-fence principle this design generalizes),
  [ADR 0050](0050-per-tablet-storage-copy-based-splits.md) Decision 1
  (per-tablet private engines — the addressing change that reopens the
  in-place fork), [ADR 0049](0049-universal-kind-write-path.md) (the
  copy-kinds classification this design reuses unchanged).
- **Decision record:** accepted by Guillaume on 2026-08-25 by merging the
  implementation stack; the G1/G4 fork resolutions recorded in the as-built
  notes below were made during implementation and stand as decided.

## Context

### Why revisit a design that shipped eight days ago

ADR 0050 landed a working copy-based split (Train B, rungs 1–8, "Accepted —
implemented") and it is a real improvement over ADR 0028's zero-copy split —
it deleted an entire class of two-live-groups-sharing-physical-rows bugs
(#216, #220, the `hot_read` residual, the cross-group LWW hazard). But
ADR 0050's own as-built amendments already document three production
weaknesses in the copy-based *workflow* itself, not in the decision to move
off shared storage:

1. **The convergence predicate needed a liveness bound.** The rung-8 bench
   (a *continuous* writer, not a burst) found that "the latest tail pass
   shipped zero new records" is structurally unsatisfiable on a
   continuously-written parent — the hot tablet that most needs splitting is
   exactly the worst case. The fix, `SPLIT_MAX_TAIL_PASSES` (25 post-bulk
   chasing passes, ≈5s), is a bound on the *symptom*, not a design that
   converges by construction. See the engineering-lessons entry "A catch-up
   convergence predicate needs a liveness bound, and the load that breaks it
   is *sustained*, not bursty."
2. **The cutover has a second, far slower gate.** The GSI-drain veto
   (the parent's `"gsi"` cursor must reach the freeze) is not covered by
   `SPLIT_MAX_TAIL_PASSES` at all — it is a *cutover* gate, not a *tail*
   gate, and unlike the tail it guards a correctness property, so it cannot
   simply be bounded (see Alternative 1). Issue #288 found that an unpaced
   continuous write flood against an indexed table can make the drain
   livelock against the split it is supposed to unblock: the flood
   generates change-log backlog faster than the drain clears it, so cutover
   never fires within any budget the test author chose. The fix that
   shipped was pacing the *test's* write flood, which proves the mechanism
   can converge under polite load — it does nothing to make the veto's own
   convergence fast under impolite load.
3. **The freeze window is a measured, size-scaling write outage.** Rung 8
   measured **458ms** of retry-masked write blip at 2,000 rows on a 3-node
   cluster — inside fork F8's sub-second contract, but not zero, and the
   rung-8 fix that got it there (filtering the final image by a pre-bulk
   version floor) was itself needed *because* the unfiltered final image
   re-shipped the whole table inside the freeze window, with the blip
   scaling with table size rather than recent write rate. The blip's
   duration is fundamentally a function of *how much residue is left when
   the freeze fires* — which the tail-pass bound (item 1) exists precisely
   to cap, coupling the two.
4. **The change-log tail is structurally blind to signal-less writes.** ADR
   0049 gives every *client* mutation a change record, but transaction
   decisions (`TxnCommit`/`TxnAbort`) and resolves rewrite base rows with no
   change record of their own — they predate that contract and were never
   folded into it, because they are apply-side rewrites, not client
   mutations. An O(delta) tail therefore cannot see them by construction, so
   ADR 0050 rung 5 added a **second full engine scan** — the final image —
   specifically to backstop this class, at the cost of doubling (with the
   pre-pass, tripling) the number of full scans a split pays. See the
   engineering-lessons entry "Writes with no change record are invisible to
   every change-log-derived copy/tail — inventory them before trusting
   O(delta)."

None of these four are implementation bugs in the sense a follow-up PR
fixes cheaply. They are consequences of the workflow's own shape: a
bespoke replication protocol (`SeedBatch` bulk pass + change-log tail pass)
with a bespoke convergence predicate, built because — at the time ADR 0050
was written — the alternative was believed to be unavailable. This ADR
argues that belief no longer holds, for a specific, narrow reason, and
proposes the design that follows once it doesn't.

### The layout-scoping insight

ADR 0050's own Context section states the reason it rejected an in-place
(zero-copy, in-band-Raft-formed) split and reached instead for a bespoke
copy protocol:

> Tablet identity does not appear in physical keys. A key is
> `escape(table) || kind || token || escape(pk) || …`; a tablet's identity
> lives *only* in its live-narrowable `range` (`StorageScope`, ADR 0028).
> Two children of one table co-hosted on one node's shared engine would
> therefore write **byte-identical physical keys** — "copy the parent's
> data into both children" has nowhere to put the copies. Zero-copy was not
> merely convenient under this layout; it was the only split the layout
> permits.

That finding is correct, and it forced Decision 1 (per-tablet private
engines) — but it is a finding about the **shared-engine layout ADR 0028
established**, not a general fact about splits. Decision 1 changed the
layout it was reasoned against: under per-tablet private engines, physical
keys are `kind || logical_key` with **no table or tablet bytes at all**,
and a tablet's identity lives entirely in **which engine (which file
namespace) a key is written into** (ADR 0050 §Decision 1, F2b). Two
children of one table, co-hosted on the same node, therefore do **not**
write byte-identical physical keys — they write into two structurally
distinct engines. The exact argument that ruled out in-place split under
ADR 0028's layout does not apply under ADR 0050's own layout. The recon
finding foreclosed in-place split *while the shared-engine seam existed*;
Decision 1 removed that seam eight days before this ADR, but the workflow
built on top of it (Decision 2) was never re-examined against the new
layout it was itself built on.

This also clarifies what actually killed the *historical* in-place design.
ADR 0017 §4's original in-place split (Stage D) was **two-phase and
non-atomic**: the control plane committed `SplitTablet` metadata
separately from the data-plane group agreeing its own `Split` command and
minting a sibling via `Coresident::sibling`. ADR 0028's Context is a
litany of the bugs that non-atomicity caused (orphaned metadata-only
tablets, retry-storm amplification, an epoch-CAS mint race, a
`Coresident`-pool liveness cliff) — every one of them traces to the gap
*between* the two phases, not to the idea of splitting a live group's data
in place. A **single-entry atomic mint** — the shape this ADR proposes,
and the shape CockroachDB's own range split uses — has no such gap by
construction: there is no second step to fail independently, because
there is no second step.

### The two things a freeze conflates

ADR 0050's `Freeze` (Stage 3) does two jobs at once, and separating them is
the second premise of this design:

- **An ordering fence.** Something must mark a specific point in one
  group's history as the last state either side of a split gets to build
  from. This is genuinely irreducible — it is the same principle ADR 0018
  §2's range seal closes the "wide fence, un-ticked leader" case with, the
  same principle a stream shard's end-of-log close relies on, and the same
  principle fork F9's `split_lineage` depends on being written at "one
  immutable moment." Some entry, at some log position, has to be the line.
- **A write outage.** ADR 0050's `Freeze` additionally makes the parent
  **reject every subsequent user write** until cutover, because the parent
  and its children are different Raft groups with a residue-transfer window
  between them — the tail pass, the final image, and the GSI-drain veto all
  have to finish *before* the children can safely activate. That outage is
  not irreducible; it is a consequence of **fork F5's choice** (children
  placed at their final homes, a fused split+move) landing on top of the
  ADR 0028 legacy of "the fence lives in the parent's log, activation lives
  in the control log, and residue has to physically cross from one to the
  other in between."

Separating these two — keeping the fence, eliminating the residue-transfer
gap that makes the fence also have to be a write outage — is exactly what
an in-place, single-entry atomic split does, for the same reason CockroachDB's
own range split (a metadata-only left/right divide of one already-replicated
range, no data movement at all) has no comparable freeze window.

## What this keeps from ADR 0050

Both of the following survive this proposal unmodified, and Train 2 below
is written assuming them:

- **Decision 1 — per-tablet private engines.** This design's entire premise
  depends on it (see "The layout-scoping insight" above). A revert to
  shared-engine storage is Alternative 2 below, and is rejected.
- **Fork F9 — `split_lineage` written at one immutable moment.** The
  in-place split's single `SplitTablet` entry (Train 2) *is* that moment,
  written by the same discipline ADR 0050 established: at the point the
  parent's shard chain becomes complete and immutable, so lineage
  derivation stays race-free by construction. `split_parents` stays
  deleted.
- **The copy-kinds classification** (`KIND_BASE`/`KIND_LSI`/`KIND_FOOTPRINT`
  retained; `KIND_CHANGE`/`KIND_CURSOR` dropped) — unchanged, reused
  verbatim at a different mechanism (see Train 2).
- **The split-only lifecycle** (ADR 0044) and the byte-based auto-split
  trigger (ADR 0034) — this proposal changes *how* a triggered split
  executes, not *when* one triggers.

## Decision — two independently-shippable trains

### Train 1: learner replicas in the shared `RaftCore`

A new **non-voting membership class**, added to `animus-control`'s
`RaftCore<C, S>` (shared by both the control plane and `animus-cp-data`,
ADR 0009): a **learner** receives `AppendEntries` and `InstallSnapshot`
exactly like a voter, but is excluded from the voter set for every quorum
computation — commit-index advancement, election majorities — and never
pre-votes or campaigns. Promotion from learner to voter reuses the
**existing single-server `change_membership` step** unchanged (ADR 0017
Stage C): a learner becomes a voter via the same committed configuration
entry an add-voter always was, just gated on a catch-up criterion first.

This train is justified **independently of Train 2** and worth shipping on
its own. Today a new tablet replica is added straight as a **voter** and
catches up afterward (`animus-cp-data`'s single-server `change_membership`
path, `lib.rs` around the reconfigure/rebalance call sites) — which means
for however long that replica takes to catch up via `InstallSnapshot` +
log replay, it counts toward quorum while contributing nothing to it in
practice: a 3-node group temporarily has an effective quorum of "2 of 2
caught-up voters, but must still count the lagging third," which is the
textbook availability hazard learners exist to close (Raft's own
membership-change discussion, and the reason CockroachDB/etcd/TiKV all
have a learner/non-voter class). Closing it does not require Train 2 at
all.

**Mechanism:**

- `RaftCore` gains a per-member `role: Voter | Learner` alongside the
  existing config. Quorum math (`commit_index` advancement, election
  majority) is computed over voters only; a learner's `match_index` is
  still tracked (needed for the promotion criterion and for `AppendEntries`
  flow control) but never counted toward a majority.
- A learner never transitions to candidate — `start_election` gates on
  `is_voter`, exactly as it already gates against a not-yet-added node
  (the existing "pre-start a to-be-added node knowing only the current
  voters" test gotcha documented in `animus-cp-data/CLAUDE.md`; a learner
  is now a *durable*, not merely transient, instance of that same state).
- **Promotion criterion**: a learner is eligible for promotion once its
  `match_index` is within a small, configurable threshold of the leader's
  `last_index` — the same catch-up-complete signal `InstallSnapshot`
  completion plus subsequent `AppendEntries` acks already provide, not new
  machinery.
- The host reconciler's (`animus-cp-data::host`) replica-move flow changes
  from "add voter directly, let it catch up as a voter" to **add learner →
  poll `match_index` → promote to voter → remove the old replica** —
  a `HostAction::Reconfigure` sequencing change, not a new action.
- Applies to **both planes**: the control plane's own membership changes
  (ADR 0037) and the data plane's per-tablet reconfiguration (ADR 0029)
  both gain the same catch-up-safe replica move, since both instantiate
  the same `RaftCore`. ADR 0030's "permanently non-voting growth mirror"
  (a control-plane member that mirrors `Metadata` but never votes) is
  noted here as a **plausible special case** of a permanently-unpromoted
  learner — this ADR does not commit to that re-expression, only flags it
  as worth checking once learners exist, since collapsing two mechanisms
  that do almost the same thing is exactly the kind of simplification ADR
  0046 principle 5 warns is lost by *not* looking for it.

**Testing plan (Train 1):** a fault-injected sim corpus (new
`ANIMUS_LEARNER_SEEDS` knob, following the existing corpus-depth
convention) covering: learner catch-up under partition (the learner falls
behind, reconnects, still promotes correctly); a leader change while a
learner is mid-catch-up (the new leader must inherit the learner's
`match_index` bookkeeping, or safely restart it); a snapshot race (a
learner receiving `InstallSnapshot` while the leader concurrently commits
further entries); and the core safety property, asserted structurally
rather than by absence of failure — **a learner never appears in any
majority computation, and its liveness or death never flips a commit or
election outcome**, checked the same way the existing membership-change
corpus checks `change_membership` safety (`animus-control`'s core-level
test plus the `animus-cp-data`-level integration one, per the "Stage C"
audit note lesson that a primitive should be exercised at both layers).

**As-built (2026-08-25) — the reconciler adoption rung landed.** The
`RaftCore` primitive (`add_learner`/`promote_learner`/`remove_learner`/
`learner_caught_up`, PR #383) shipped with no reconciler-layer consumer —
`RaftKvNode::reconfigure_step` still added a missing replica straight to the
voter set. This rung closes that gap: `reconfigure_step` now sequences a
replica **add** as add-learner → (poll `learner_caught_up` against a fixed
`RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD` of 4 log entries) → promote →
remove-the-old-replica, one single-server step per call exactly as before
(ADR 0031 discipline unchanged — no new `HostAction`, no new workflow; only
what `reconfigure_step` proposes on each call changed). A remove-only delta
and a brand-new group's initial bootstrap are both untouched, per this
train's own scope note above. Concretely, `reconfigure_step`'s priority
order gained two steps ahead of the pre-existing add/remove ladder:

- **A learner no longer present in `desired` is dropped immediately**,
  regardless of catch-up progress or liveness — the fix for "a learner
  mid-catch-up that dies or is decommissioned must not wedge every later
  step" (placement retargeting `desired` away from a dead newcomer is what
  actually resolves the stuck state; the reconciler's only job is to not
  block on a target that's no longer wanted).
- **A caught-up learner still in `desired` is promoted** before any new
  add is considered, so an in-flight move finishes before a new one starts.

**Scope correction against the mechanism list above**: this rung is the
**CP data plane only** (`animus-cp-data::RaftKvNode::reconfigure_step`,
driven by `host::Reconciler`). The control plane's own runtime membership
change (ADR 0037, `admin_add_control_member`/`RaftNode::change_membership`)
has no automatic per-tick reconfigure loop to adopt this into — it is an
operator-invoked single command, not a reconciler decision — so it still
adds a control voter directly, unchanged by this rung. The bullet above
("applies to both planes... since both instantiate the same `RaftCore`")
was describing the *primitive*'s availability, which is genuinely
plane-agnostic (Train 1 shipped `add_learner`/`promote_learner`/
`remove_learner` on the shared `RaftCore`, usable by either plane); the
*reconciler-adoption sequencing* this note documents is data-plane-only, and
giving the control plane's own admin surface the same learner-phased
sequencing (if ever wanted) is unstarted follow-up, not part of this rung.

No `Metadata`/tablet-map representation change was needed: `Tablet::replicas`
stays the *target* voter set placement wants, exactly as before — the
learner bookkeeping already lives entirely in each tablet's own `RaftCore`
state (replicated via the group's own log to every replica, voter and
learner alike, since PR #383), which is exactly the state
`reconfigure_step` already had local access to. Placement decides *where*;
the reconciler decides *how to get there*, unchanged.

Structural regression closed: adding a replica used to grow the voter set
immediately (a `desired.len()+1`-voter transient), so losing one ORIGINAL
voter while the newcomer was still uncaught-up could leave the group short
of the *enlarged* majority even though the *original* quorum was intact.
With the learner phase the voter set never grows until the newcomer has
already proven it can keep up, so the original quorum's own majority is
never diluted by a replica that hasn't earned a vote yet — proven directly
in `animus-cp-data/tests/learner_reconfigure.rs`'s
`old_quorum_survives_an_old_voter_loss_while_the_new_replica_is_still_a_learner`.
Fault-injected reconciler-driven coverage (partition during catch-up, a
leader change mid-move, a learner's node crashing and being replaced by a
retargeted `desired`) lives in `animus-cp-data/tests/
reconciler_corpus.rs`'s `learner_move_survives_partition_during_catchup`/
`learner_move_survives_leader_change_mid_move`/
`learner_crash_is_replaced_by_a_new_target` scenarios; a real multi-process
`ProdEnv` exercise (observing the spare pass through an admin-visible
`learners` set before ever appearing in `voters`, and confirming writes
never stop landing during the move) lives in `animusd/tests/
learner_reconfigure.rs`. `admin::CpRaftView` gained a `learners` field
(`/admin/raftkv`) purely for this observability — read-only, drives nothing.

### Train 2: in-place split, replacing ADR 0050's build/freeze/cutover workflow

**Stage 1 — `BeginSplit` unchanged in shape, changed in effect.** The
control plane still consults placement for the children's final homes
(fork F5's fused split+move is preserved: the copy — now inside Raft
replication rather than a bespoke driver — is still the only data
movement). What changes: instead of minting two `Building` tablet ids at
those homes and starting two independent Raft groups, `BeginSplit`'s apply
**adds the union of the children's chosen homes as learners to the
parent's own group**. No new tablet id is minted yet — the parent group
itself is what will fork into two, at Stage 2.

**Stage 2 — ordinary Raft catch-up, no bespoke protocol.** The new
learners catch up via the **existing** `InstallSnapshot` + log replay —
Train 1's mechanism, unmodified. There is no `SeedBatch`, no scan-and-ship
bulk pass, no change-log tail pass, no convergence heuristic to bound with
something like `SPLIT_MAX_TAIL_PASSES`. Lag is `match_index` arithmetic
against the leader's `last_index`, backed by Raft's own flow control — the
same primitive that already answers "has this replica caught up" for
every other membership change in the system, not a purpose-built
convergence predicate that a continuous writer can starve.

**Stage 3 — the single-entry atomic fork.** When every added learner is
caught up (Train 1's promotion criterion), the parent's leader proposes
one new command, **`SplitTablet { split_key, children: [(id, replicas); 2] }`**,
into the **parent's own log** — not a control-plane command. This one
entry is simultaneously:

- **The ordering fence.** Every replica (voter and learner alike) that
  applies it treats it as the immutable line: no subsequent entry in the
  parent's log describes state either child inherits.
- **The data-plane activation.** At apply, on **every** replica
  (including the learners just caught up — this is why Stage 2 catches
  them up first), two new tablet ids are minted and two new engines are
  materialized **locally**, each cloned from the parent's own engine as of
  this exact log position:
  - Cloning is at the **SSTable level** (a per-tablet engine, ADR 0050
    Decision 1, makes file-granularity copy possible — named there as
    future storage headroom, and promoted here to a load-bearing
    prerequisite rung; `MemoryEngine` gets an equivalent cheap clone for
    `SimEnv`).
  - The clone is **filtered by the identical copy-kinds rule ADR 0050
    already established**: `KIND_BASE`/`KIND_LSI`/`KIND_FOOTPRINT` are
    retained (split by the same byte-weighted median split key, F11's
    token-alignment rule unchanged); `KIND_CHANGE`/`KIND_CURSOR` are
    **dropped** — children are still born with **empty change logs**,
    preserving the #220 defense ("no consumer offset ever crosses a
    split," ADR 0046 principle 3) exactly as before. Nothing about this
    invariant weakens; only the mechanism producing the filtered copy
    changes, from a scan-and-ship driver to a filtered local clone.
  - Both child groups **bootstrap in place, at the agreed log position**,
    with Raft configs **derived deterministically from the parent's own
    config at that entry** — every replica computes the same two child
    configs from the same input (the entry's own `children` field plus
    the parent's own voter/learner set at that position), so no
    coordination beyond ordinary Raft agreement on the entry itself is
    needed for every replica to reach the identical two new groups.
- **Inheritance of signal-less writes, for free.** Because Raft log
  replication carries **every applied entry by construction**, an
  in-flight transaction's intent, its eventual decision, and any resolve —
  the exact class ADR 0050 rung 5's final-image re-scan exists to
  backstop — are simply **whatever the cloned engine's `KIND_BASE` rows
  already say** at the fork point. There is no separate signal to miss,
  because nothing about this mechanism depends on a signal at all: it
  copies committed *state*, not a derived stream of *changes*. Fork F7's
  "copy intents; resolves chase" outcome falls out with **zero additional
  machinery** — no final image, no pre-bulk version floor, no second or
  third full engine scan.

**Stage 4 — `CutoverSplit`, now a pure recording step with no vetoes.** The
control plane's `CutoverSplit` still runs (children → `Active`, parent
removed from `tablets`, `split_lineage[child] = (parent, ...)` written for
both — fork F9 unchanged, written at the same "one immutable moment," now
literally the `SplitTablet` apply position itself rather than a
subsequently-observed one). What it **no longer gates on**: no freeze, no
GSI-drain veto, no backfill-seeder veto, no tail-convergence bound. The
children are already fully formed, already durable, already carrying every
row the parent had at the fork — there is nothing left for a veto to wait
for. The parent's residual, not-yet-drained consumer backlog (GSI/backfill
cursor rows that hadn't caught up to the fork point) is handled
**post-cutover**, draining from the retained, now-immutable parent engine
on its former replicas until the drain catches up, at which point the
parent engine is reclaimed — see "Open forks," below; this ADR
deliberately does not commit to the exact drain-then-reclaim protocol.

**Stage 5 — membership epilogue.** Each child group still needs trimming
to its own placement-chosen final replica set (the fork was seeded with
the *union* of both children's homes as learners on the *parent*, so each
child is over-replicated relative to its own final RF immediately after
the fork) — an ordinary sequence of existing single-server
`change_membership` steps (promote the learners that belong to this
child, remove the ones that don't), no new primitive.

**Stale routing.** A client routed to the retired parent by stale
`Metadata` hits a node that — because the fork happened locally, on every
replica the parent had — **already hosts both children**. It gets the
same retryable refusal shape a client hits today on any stale route, but
re-resolution is now purely **local**: the node already has the answer,
it just needs the client to re-resolve via `Metadata`. Fork F8's blip
contract contracts from "residue transfer + a control-plane commit +
`metadata_watch` propagation" to roughly **one routing refresh** — no
network hop, no consensus round, no drain to wait for.

## What this deletes (when Train 2 lands)

Mirroring ADR 0050's own "what this deletes" discipline, and using the
same house rule (mechanism-gone lessons move to the archive):

- The split driver in `animusd::index_drain` — `SeedBatch` bulk+tail,
  `SPLIT_MAX_TAIL_PASSES`, `tail_hlc`, the pre-bulk version floor, the
  bulk/tail/final-image three-scan sequence in its entirety.
- `KvCommand::Freeze` **as a write-refusing outage**. The underlying
  seal-discipline mechanism (`seal.rs`'s durable engine marker,
  re-latching on group start, ADR 0018 §2's log-position-is-authoritative
  principle) may survive in a narrower form — as the apply-side rejection
  of any entry ordered *after* `SplitTablet` in the parent's own log
  (there should never be one, since the parent's group stops proposing
  once it forks, but the same "reject a later-ordered entry regardless of
  its own timestamp" backstop ADR 0050 built is cheap insurance). It no
  longer needs to reject writes for a multi-second window while a
  separate workflow drains.
- The final-image re-scan and its named O(delta) follow-up (moot — Train 2
  never had an O(delta) tail to restore in the first place).
- The GSI-drain and backfill-seeder **cutover vetoes**, specifically —
  their drain mechanics survive, relocated to run **post-cutover** against
  the retained parent engine (Stage 4).
- Fork F8's blip contract (~458ms measured, scaling with residue) is
  superseded by a stronger, near-zero-outage one (Stage 3–5, "Stale
  routing").

## Alternatives considered and rejected

**1. Status quo, plus an accelerated GSI-drain endgame.** The narrowest
possible fix for weakness 2 above. Note the shape carefully: a
`SPLIT_MAX_TAIL_PASSES`-style *bound* on the cutover veto — the first
instinct, by analogy with the tail-pass fix — would be **unsafe**: the veto
is a correctness gate, not a liveness heuristic, because cutover retires
the parent and `Release`/`Reclaim` teardown deletes its engine files with
no drain-before-halt, so force-firing cutover past an un-drained `"gsi"`
cursor silently loses index updates. What *is* safe is acceleration: once
the parent is frozen it is static (its backlog only shrinks), so the
frozen endgame can drive the drain to exhaustion in a bounded loop within
the tick instead of one batch per 200ms tick, with the veto's own
pass/fail condition untouched. This is the interim fix and should ship
**separately, and first, regardless of this ADR's fate** — it closes a
real slow-convergence class (issue #288) with a small, well-understood
change, and this ADR's timeline should not gate it. It leaves the rest of
the copy-based workflow's structure untouched: the bespoke `SeedBatch`
protocol, the three-full-scan endgame, and the write-outage-shaped freeze
all remain exactly as they are.

**2. Metadata-only split + learner rebalance (the CockroachDB shape,
without Decision 1).** Instant and load-immune — a `SplitTablet` narrows
the parent's range and mints a sibling over the *same* physical rows,
predicate-only, then Train 1's learners rebalance the new sibling's
replicas onto its placement-chosen final homes at leisure. Rejected
because it **requires tablets to physically share substrate**, which
directly reverses Decision 1 and resurrects the entire defense stack ADR
0050's Context names as *the single largest source of production bug
families this codebase has had*: `StorageScope`'s live `Arc<Mutex<KeyRange>>`
narrowing, the `hot_read` scope-transition latch, `stream_split_basis`/
`in_declared_range`/`SealStreamShard`'s range-CAS, the #216/#220 class, and
shared-engine GC via `merge_tombstone` rather than plain deletion. It also
does not actually relieve a hot node's load until the post-split
rebalance completes anyway — the instant-split property is real but the
load-shedding property it is usually chosen *for* is deferred, not gained.
Rejected on the same grounds ADR 0050 rejected ADR 0028 in the first
place, applied to the same seam.

**3. Dual-write/redirect during a shortened freeze window.** Keep the
copy-based workflow's overall shape but mask the blip by having the parent
forward writes to the children during a shortened window instead of
refusing them outright. Rejected: it does not fix the underlying
convergence or O(table)-scan problems (weaknesses 1 and 4 above), it is
absent from ADR 0050's own fork record (a genuinely new mechanism, not a
tuning of an existing one), and it introduces a new ordering hazard at the
parent/child boundary — a write landing on the parent mid-forward races
the very cutover it is trying to make safe. Rejected here on
complexity-for-partial-benefit: it would ship real new machinery to buy
back only part of what Train 2 buys back with less.

**4. Image-seeded formation.** Already rejected by ADR 0050's own fork F3
("no new Raft machinery") — it needs a new bootstrap-snapshot mode *and*
the ordinary propose path for deltas anyway, so it is strictly more
machinery than either the copy-based workflow it lost to originally, or
Train 2's single-entry mint.

## Consequences / risks (honest)

- **Group-mint-at-apply is the hardest part of this design, not a detail.**
  Bootstrapping two live Raft groups from *inside* another group's state
  machine, deterministically, and **crash-safe per replica** — a crash
  between "materialized the two child engines" and "durably recorded the
  child group configs" must be idempotent on WAL replay, or a restarted
  replica either double-mints or loses a child silently. This is
  precisely the "D" stage machinery ADR 0017 §4 originally specified and
  ADR 0050's fork F3 was **written explicitly to avoid** ("no new Raft
  machinery anywhere"). The single-entry atomic shape closes ADR 0028's
  two-phase races (there is no second step to race against), but it does
  not make group-minting easy — it is still the single most intricate
  piece of consensus code this ADR proposes, and it needs its own deep
  sim corpus (a new depth knob, following the existing
  `ANIMUS_SPLIT_SEEDS`-style convention) before it is trusted anywhere
  near the confidence level ADR 0050's Train B corpus reached.
- **Learner snapshots cost more bytes when homes are disjoint.** A learner
  added at a child's *final* home (fork F5, preserved) receives the
  **whole parent range** via `InstallSnapshot`, not the range-filtered
  half a copy-based `SeedBatch` bulk pass would ship — roughly 2× the
  network bytes of today's design when the two children's homes don't
  overlap. Two mitigation forks (see G2 below): range-filtered snapshot
  install (ship only the learner's eventual half-range — still open), or
  pre-trim at the SSTable-clone step instead (**closed 2026-08-31** —
  `LsmEngine::clone_to_filtered`'s whole-file assignment; a straddling
  table still ships whole either way, so this bounds rather than
  eliminates the cost).
- **The fence-to-routing gap persists, but shrinks.** Routing still lives
  in control-plane `Metadata`, so a client can still be routed to a
  retired parent for a window bounded by `metadata_watch` propagation —
  this ADR does not eliminate that gap, only removes the *residue-transfer*
  component that used to dominate it (Stage 3–5, "Stale routing"). The new
  expected blip shape needs its own measurement once implemented, the same
  way ADR 0050 rung 8 measured its own.
- **Split cost model changes, not disappears.** A mis-timed split remains
  non-free (ADR 0050's own accepted consequence stands) — the marginal
  cost moves from "network copy proportional to data plus tail-chase
  passes" to "local SSTable clone plus membership churn (learner
  catch-up, promotion, old-replica removal) proportional to data." Bulk-
  load split storms are still an IO event; presplit-at-`CreateTable` and
  per-node concurrent-build throttling remain relevant follow-ups
  regardless of which workflow ships.
- **Two membership-change classes now coexist during a split**: the
  parent's own learner-add/catch-up/fork (Stages 1–3) and each child's
  post-fork trim (Stage 5). Getting the interaction between an in-flight
  split and an *unrelated* concurrent rebalance of the same tablet correct
  is new surface area Train 1's corpus needs to cover explicitly, not
  just as an afterthought of Train 2's own corpus.

## Sequencing (rungs)

1. **Learners in `RaftCore` + their own sim corpus** (Train 1). Ships
   alone; immediately improves membership-change availability for every
   existing replica move on both planes, independent of whether Train 2 is
   ever accepted.
2. **SSTable-level clone in `animus-storage`** (plus a `MemoryEngine`
   equivalent for `SimEnv`) — the prerequisite ADR 0050 named as future
   storage headroom, promoted here to a hard dependency of rung 3.

   **As-built (2026-08-25), landed independently of rung 3**:
   `LsmEngine::clone_to(target_prefix)` flushes the memtable (so the clone
   is SSTables-only with an empty WAL), hard-links every live SSTable file
   into a fresh prefix on the same `Env`, and writes the target a new
   manifest naming exactly those tables — full-engine only, no kind
   filtering or key-range trimming (that stays rung 3's business, including
   the G2 pre-trimming fork this ADR left open). The `Env` `Disk` trait
   gained one new primitive, `link(src, dst)` — a hard link, overwriting an
   existing `dst` so a crash-retried clone is idempotent — implemented by
   `ProdEnv` (`std::fs::hard_link` + the usual directory fsync) and modelled
   deterministically by `SimEnv` (a snapshot copy into an independent map
   slot, indistinguishable from a real hard link for this trait's
   sanctioned use, and wired into the existing disk fault-injection model).
   The commit point is the target's manifest `Disk::replace`: before it
   succeeds the target prefix has no manifest and opens empty, so a crash
   mid-clone is safe to retry and never exposes a torn clone — this is the
   contract rung 3's G4 (crash-recovery idempotency for group-mint-at-apply)
   builds on. `MemoryEngine::clone_to` is the `SimEnv`-corpus equivalent
   (deep-copies version history — there are no files to link).

   **Amended (2026-08-26, flush-retry-starvation fix)**: the
   issue #298 fix below (a single `flush()` call could silently no-op while
   `applies_in_flight > 0`, losing an acked row) originally closed that hole
   by retrying `flush()` in a bounded loop until the memtable read empty —
   which has no liveness guarantee against a *persistent* concurrent
   writer (one that refills the memtable faster than any bounded number of
   flushes drains it), and was caught doing exactly that in CI on an
   unrelated PR (`clone_to_under_live_load_never_drops_an_acked_write`
   erroring with "memtable still non-empty after 1000 flush retries" on a
   busier runner than the one the same code passed on days earlier). The
   fix replaces "retry until empty" with a **snapshot**: one best-effort
   `flush()` attempt (kept only to preserve the pure-SSTable-only clone
   shape in the common, quiescent case), then one atomic read of
   `(manifest.tables, memtable-contents)` under the write path's own lock —
   sufficient by construction, since `log_and_apply` only returns to its
   caller after applying under that identical lock, so anything already
   acked is provably captured with no retry needed. Any rows the snapshot
   still finds in the memtable are written out as a new SSTable inside the
   **clone's own** namespace (never the source's), bounded to that one
   point-in-time snapshot regardless of how long or how fast the writer
   keeps going afterward. See `docs/engineering-lessons.md`'s 2026-08-26
   entry for the general lesson and
   `crates/animus-storage/tests/lsm_clone_concurrent.rs`'s
   `clone_to_completes_under_a_writer_that_never_pauses` for the liveness
   regression.

   **Amended (2026-08-31, range-aware clone — G2 closed)**: the "full clone
   then trim" shape rung 3 landed with (immediately above, and see G2 in
   the residuals table) is superseded by `LsmEngine::clone_to_filtered
   (target_prefix, keep)`, a range/kind-aware sibling of `clone_to` doing
   **whole-file assignment** at the clone step itself: a source SSTable
   whose own `[min_key, max_key]` — already recorded in `SsTableMeta` at
   flush/compaction time, so this needed no manifest format change and no
   extra disk read — falls entirely outside every `keep` range is never
   linked into the target's namespace at all; only a table straddling a
   `keep` boundary is still linked whole, exactly as `trim_split_child`'s
   own post-clone `delete_range` already expected and correctly handles
   (trimming a table that was never linked is a harmless no-op). The
   memtable's own leftover-row snapshot is filtered the identical way, key
   by key. `clone_to` itself is now a thin wrapper passing one `keep` range
   covering the whole keyspace, so it shares this method's exact crash-
   safety/commit-point contract unchanged (see rung 2's own doc above).
   `EngineFactory::clone_engine` gained a `keep: &[(Vec<u8>, Option<Vec<u8>>)]`
   parameter (`MemoryEngine`'s implementor ignores it — no per-file dead
   space exists to save for an in-memory engine, and `trim_split_child`
   still runs immediately after and makes the result correct either way);
   `Reconciler::materialize_split_child` computes the child's own keep-set
   once, as its declared range sliced through `KIND_BASE`/`KIND_LSI`/
   `KIND_FOOTPRINT` (nothing of `KIND_CHANGE`/`KIND_CURSOR`, which a child
   is always born empty of), and passes it down. This closes three things
   at once: the cold/quiesced child's dead-space debt (a sibling-half table
   is never linked in the first place, so there is nothing left for that
   child's own compaction to eventually reclaim), the per-engine
   `approx_bytes`/file-size double-count across a split's two children
   (each wholly-sibling table no longer counts toward both), and — see G2's
   own residuals-table entry — the ~2× learner-snapshot bytes a disjoint-
   home learner's `InstallSnapshot` used to ship, now bounded by however
   much of the parent's own table set genuinely straddles the split
   boundary rather than the whole parent range. Proven at the file level
   (not just the row level, which the pre-existing `MemoryEngine`-backed
   corpus already covered) by a dedicated single-node `LsmEngine<SimEnv>`
   regression, `tests/inplace_split_dead_space.rs`
   (`split_child_engine_excludes_the_wholly_sibling_sstable`): a parent
   seeded with two explicitly flushed, range-disjoint SSTables forks, and
   each child's own materialized engine is asserted — by SSTable sequence
   number, not merely by row content — to have linked its own table and
   excluded its sibling's. `crates/animus-storage/tests/
   lsm_clone_filtered.rs` proves the underlying primitive directly
   (whole-file exclusion, a straddling table kept whole, crash-mid-clone
   safety); `lsm_clone_filtered_concurrent.rs` (real multi-thread
   `ProdEnv`) proves the leftover-memtable filtering path specifically,
   the one case `SimEnv`'s non-yielding disk model cannot reach (mirroring
   `lsm_clone_concurrent.rs`'s own rationale for `clone_to`, immediately
   above).

3. **The `SplitTablet` entry, group mint, and materialization**, behind
   the existing split trigger (ADR 0034) — built and proven alongside the
   still-shipping copy-based workflow, not in place of it, until it is
   trusted.
4. **Delete the ADR 0050 build driver, the freeze-as-outage path, and the
   cutover vetoes**, in the same bottom-up, red→green-per-cell discipline
   ADR 0050's own Train B used. **As-built: split into layers** — layer 1
   (eager child materialization, closing the write-blip tail) and layer 2
   (flip `SplitMode`'s default to `InPlace`, below) landed first, with the
   copy path kept fully selectable; the deletion itself is this rung's
   remaining, still-unstarted layer.

**Independent of this sequencing**, the interim GSI-drain endgame
acceleration (Alternative 1 — an acceleration, deliberately *not* a bound;
see that alternative for why a bound would be unsafe) should ship
**before rung 1**, as its own small, separate change — it fixes a
slow-convergence class on the record today (issue #288) and should not
wait on any part of this proposal.

**As-built (2026-08-25) — rung 3 landed, behind a flag, alongside the
still-shipping copy-based workflow (rung 4 unstarted).** `MetaCommand::
BeginSplitInPlace`/`CutoverSplit`'s in-place branch (`animus-control`),
`KvCommand::SplitTablet` and its apply-time mint (`animus-cp-data`), and
the host reconciler's own learner-add/fork-watch/materialize/trim sequence
(`animus-cp-data::host`) are all in place and sim-tested; this closes forks
G1 and G4 (below) with the resolutions decided here. Landed in three
stages, bottom-up: the data-plane mint machinery + its own fault
regressions (`tests/split_tablet.rs`); the host-reconciler wiring, its two
newly-discovered correctness gaps (a node recruited only via a child's own
`replicas` must still host the parent as a quiet non-voter, and must be
exempted from the ordinary release check), and its own corpus
(`tests/inplace_split_reconciler.rs`, held green through
`ANIMUS_INPLACE_SPLIT_SEEDS=200`); then the control-plane commands
themselves plus their unit tests.

**One concrete deviation from the Stage 3 prose above**: "Raft configs
derived deterministically from the parent's own config at that entry"
resolves to a SPECIFIC formula, stated here rather than left implicit —
both children's initial bootstrap voter set is the parent's own full
`RaftCore::config() ∪ RaftCore::learners()`, read once inside the
`SplitTablet` apply arm itself (deterministic across replicas by Raft log
order: every earlier config-change entry has necessarily already applied
by the time this one does). This is deliberately the SUPERSET of either
child's own final placement, not each child's own `children[i].replicas`
alone — it is what makes Stage 5's "each child is over-replicated relative
to its own final RF" literally true by construction, with the ordinary
`reconfigure_step` trim (unmodified) doing the rest once `CutoverSplit`
records each child's real, final `replicas` into `Metadata`.

**A second deviation, load-bearing and easy to miss**: the ADR's Stage 1
prose describes the parent's reconciler adding "the union of the
children's chosen homes" as learners, but says nothing about how a home
that is genuinely new (never a parent voter) comes to run a `RaftKvNode`
for the PARENT tablet at all before it can be added as a learner to it.
`host::plan`'s phase 1 gained a second host-candidate test for exactly
this: a node named in either child's own `replicas` (of a parent's
in-place split intent) hosts the parent as a quiet non-voter even though
it is never (and never becomes) a member of the parent's own `replicas` —
without this, the leader's `add_learner(home)` targets a node with no
local Raft instance running to receive the resulting `AppendEntries` at
all. The identical recruited-node set is also exempted from the ordinary
release check, which would otherwise fire on it immediately (it is never
in the parent's own `replicas`, and `config_excludes_me` is trivially true
for a still-a-learner recruit — a learner is never in `RaftCore::config()`
by construction).

**Residue — explicitly NOT part of this rung**, left for the layer above
(an `animusd`-level driver, mirroring `index_drain.rs::split_driver_tick`'s
shape for the copy-based workflow): the `--split-mode {copy,inplace}`
operator flag itself, `ClientCtx::trigger_split`'s in-place branch (mint
+ propose `BeginSplitInPlace`), the per-tick driver that watches a
forked-locally parent, runs the (unmodified) GSI-drain/backfill vetoes
against it, and proposes `CutoverSplit`, and the real multi-node `ProdEnv`
end-to-end regression (a paced continuous writer riding the fork). None of
animus-control/animus-cp-data's own mechanism depends on this layer
existing — every property above is proven by constructing the in-place
path directly (`MetadataView`/`Tablet::inplace_split` built by hand,
mirroring how `tests/reconciler_corpus.rs` already stands in for a live
control plane), the same posture this ADR's own Train 1 rung took for its
reconciler-adoption sub-rung.

**As-built (2026-08-25) — the `animusd`-level driver landed, closing the
rung-3 residue above.** `SplitMode` (`animusd::config`), a `--split-mode
{copy,inplace}` CLI flag (`--config`/`--cluster N` only — the same scope
`--quiesce-after` has, with the identical documented gap for
`--cluster-control`/`--cluster-data` and the standalone `control`/`data`/
`join` subcommands), `ClientCtx::trigger_split`'s branch on
`ClientCtx::split_mode` (proposing `BeginSplitInPlace` in place of
`BeginSplit`, everything else about that call — the idempotent
already-`Splitting` handling, the confirm loop, child-id allocation, F11
alignment — shared verbatim), `index_drain.rs::inplace_split_driver_tick`
(the cutover driver, wired into `change_consumer_loop` alongside the
copy-based `split_driver_tick`, selected per-tablet by
`Tablet::inplace_split.is_some()` — never by this node's own configured
`split_mode`, which only governs a *future* `trigger_split` call), and a
`ProdEnv` end-to-end regression (`animusd/tests/inplace_split_e2e.rs`) —
a real 3-node `--split-mode inplace` cluster, a paced continuous writer
riding kickoff through cutover, and a streams-enabled variant walking a
`GetRecords` iterator from the parent's own shard across the fork to both
children.

**A genuine correctness gap this rung found and closed, not anticipated by
this ADR's own residue paragraph or Stage 3's prose**: proposing
`CutoverSplit` as soon as `pending_split()` answers `Some` (this residue's
own literal reading) races the CP-data host reconciler's OWN,
independent, per-node discovery of that same fact. Stages 1–3 (learner
add, catch-up, the fork) commit nothing on the control plane, so
`tablet_host_reconciler_loop`'s `metadata_watch` wake fires once, at
`BeginSplitInPlace`'s own commit, and not again until `CutoverSplit`'s —
leaving the reconciler's periodic fallback (`RECONCILE_FALLBACK_INTERVAL`,
500ms) as the ONLY thing that can make it discover a completed fork and run
`HostAction::MaterializeSplitChild`. The `animusd`-level driver's own tick
(`INDEX_DRAIN_INTERVAL`, 200ms) can — and, in a real multi-process
`ProdEnv` cluster with no GSI/stream veto to wait on, routinely does —
observe the fork and get `CutoverSplit` committed before any given
replica's reconciler has ticked even once since the fork happened. Once
`CutoverSplit` removes the parent's row (and with it `Tablet::
inplace_split`, the only signal `plan`'s phase 1.5 branch keys on), that
replica's NEXT reconciler tick sees an ordinary freshly-`Active` tablet
with no memory of the fork, and hosts it via the wrong (non-split) path:
an empty engine and a `plan_join_host`-derived config with no relationship
to `bootstrap_voters` — permanent, silent data loss and (since every
replica computes this independently) a group whose replicas can disagree
on their own membership badly enough to never elect a leader. Found by
this rung's own `ProdEnv` e2e test (SimEnv's near-zero simulated latency
and `tests/inplace_split_reconciler.rs`'s own harness — which drives the
reconciler directly, never racing it against a second, independent
wall-clock-paced loop — never exercised this interaction). Closed with two
changes, both entirely inside `animusd` (no change to `animus-control`/
`animus-cp-data`, no new replicated command):

- `tablet_host_reconciler_loop` shortens its own fallback from 500ms to
  `INPLACE_SPLIT_RECONCILE_INTERVAL` (50ms) for as long as *any* tablet
  cluster-wide currently carries an in-place split intent — every fork
  participant observes the same `BeginSplitInPlace` commit and flips into
  this fast cadence together, well before Stage 3's fork ever applies, so
  by the time it does every replica is already polling at 50ms.
- `inplace_split_driver_tick` additionally requires, before it may propose
  `CutoverSplit`: (a) at least `INPLACE_SPLIT_MATERIALIZE_SETTLE_MS`
  (250ms — a small, justified multiple of the now-50ms reconciler cadence,
  not the withdrawn 500ms one) has elapsed since the fork applied
  (`PendingSplit::ts`, the identical `env.now()`-derived clock
  `cutover_wall_ms` already uses — no driver-local state, so this is fully
  re-derivable after a crash/re-lead), and (b) this replica's own
  `ClientCtx.edge.hosted_groups()` already contains both children — a
  direct local confirmation, never proposing ahead of its own state.

Both bounds are deliberately small relative to the withdrawn naive 500ms–
1000ms options tried while diagnosing this — a large bound would have
reintroduced exactly the write-availability outage Stage 3–5's own design
point (near-zero, "roughly one routing refresh") exists to avoid; the
shipped fix keeps the reconciler's own worst-case tightly bounded instead
of just waiting out a slow one. Measured on the e2e test's own 3-node
cluster: the paced writer needed **zero** retries across every observed
run — the fork/cutover transition was not visible to it at all.

**Measurement note (2026-08-25) — the rung-8 bench, pointed at Train 2, on
the same host as the copy-based number it's compared against.** Per this
ADR's own Testing plan ("the bench that proved [the convergence-predicate
starvation] problem for ADR 0050's design is exactly the bench that should
be pointed at this one before it is trusted"), a new sibling bench
(`animusd/tests/inplace_split_bench.rs::
bench_inplace_split_serve_latency_and_cutover_blip`) mirrors `split_build.
rs`'s own committed bench byte-for-byte in workload shape — 2,000 rows,
256-byte values, 3 nodes, the identical split key and per-tick get/put
sampling cadence — against a `--split-mode inplace` cluster instead. Both
benches were rerun 3x each on the same host, back to back, rather than
trusting the copy-based path's historical 458ms/2,000-row figure (measured
on a different host at ADR 0050 rung 8's own time) — this host runs
noticeably slower in absolute terms (its own idle linearizable-read floor
sits around 108ms median, evidently this environment's own ReadIndex/
heartbeat-round cost, not sandbox contention — it was identical, to the
millisecond, across all 6 runs of both benches), so only same-host,
same-run numbers are compared:

| | copy-based (run1 / run2 / run3) | in-place (run1 / run2 / run3) | copy-based median | in-place median |
|---|---|---|---|---|
| total split wall clock | 8.060s / 8.062s / 7.978s | 4.519s / 4.326s / 4.579s | **8.060s** | **4.519s** |
| write blip (max PUT) | 303.6ms / 251.9ms / 299.9ms | 726.3ms / 509.7ms / 733.7ms | **299.9ms** | **726.3ms** |
| put retries needed | (not instrumented) | 0 / 0 / 0 | — | **0** |

Two honest, not-spun findings, one in each direction:

- **Total split wall clock is markedly better**, as the ADR's core
  structural argument predicts: ~4.5s median vs. ~8.1s median (roughly
  1.8x faster) for the identical 2,000-row table, since Train 2 has no
  bulk-copy/tail-chase phase to run at all — the fork is a single atomic
  apply, and the wall clock measured here is essentially the time for the
  two added learners to catch up over ordinary Raft replication.
- **The write blip is NOT clearly better — it is measurably worse, and
  this is reported plainly rather than spun.** The in-place path's own
  worst observed PUT latency (726ms/510ms/734ms, median 726ms) is
  **roughly 2.4x the copy-based path's own** (304ms/252ms/300ms, median
  300ms), despite **zero** retries being needed on any of the three
  in-place runs (the new bench's retry-counting `put_in_counting`
  confirms this directly — every put that eventually landed slow did so
  on its FIRST attempt, unlike the copy-based path's blip, which is a
  fast-refused-then-retried shape). That means the elevated in-place
  number is not the documented `FROZEN_REFUSAL`-and-retry pattern at
  all — it is a single request that was simply slow end to end, most
  likely `cp_route`'s `RouteDecision::Wait` branch parking the write
  while a freshly-forked child group runs its own first election (never
  forwarding to a non-leader during election, by design) — a real,
  structurally different cost than the copy-based path's refuse-and-
  retry blip, and one this ADR's own "near-zero, roughly one routing
  refresh" framing did not anticipate. This was **not** investigated
  further here (out of scope for a bench-and-report pass — the
  investigation and any fix, if warranted, is separate work), but it is
  exactly the kind of number rung 4 (flipping the default) should not
  proceed past unexamined: **the in-place path trades a slower, more
  numerous small-retry pattern for a rarer but larger single stall**, and
  which shape a real client's own retry/backoff policy handles better is
  not something this bench alone answers.

**Soak (2026-08-25), same session, run after the bench above (never
concurrently, so neither perturbs the other's timing).** All green, no
findings:

- `ANIMUS_INPLACE_SPLIT_SEEDS=500` (`animus-cp-data --test
  inplace_split_reconciler`) — 3/3 tests passed, 18.2s wall clock.
- `ANIMUS_LEARNER_SEEDS=200` (`animus-control --test learner_corpus`) —
  4/4 tests passed, 39.7s wall clock.
- `ANIMUS_RECONCILER_SEEDS=300` (`animus-cp-data --test
  reconciler_corpus`) — 4/4 tests passed, 33.5s wall clock.
- `animusd --test inplace_split_e2e` (both `ProdEnv` tests — the paced-
  writer fork/cutover test and the streams-shard-lineage test), run **20
  times** as independent process invocations (not `cargo test`'s own
  repeat, which would share one binary but not one process — this
  matches the "real 3-node cluster brought up and torn down fresh" shape
  the ADR's own housed harness bug (the `tablet_host_reconciler_loop`
  race documented above) was actually found by): **20/20 runs green**, no
  flake — both tests passed on every run, and the writer needed zero
  retries on every run. Per the repo's own rule, a single failure here
  would have been reported as a real bug, not retried away; none
  occurred.

None of the three `SimEnv` corpora surfaced a failure at these depths, and
CARGO_PROFILE_DEV_DEBUG=0 was used throughout the soak (matching the
existing nightly `corpus-deep.yml` convention of a plain, unmodified `cargo
test` invocation — no release-profile build was found in that convention to
match, so none was introduced here).

**Measurement note addendum (2026-08-25) — rung 4's fix for the write-blip
regression, benched on the same host as the numbers above.** Diagnosis: a
freshly-forked child group has no leader until *some* replica's cold,
randomized election timeout eventually fires, and `cp_route`'s election-wait
branch parks a write meanwhile. Fix: the replica that was the PARENT's own
current Raft leader **at the moment it materializes a child** now campaigns
for that child's leadership immediately (`RaftKvNode::
start_hosted_campaigning`, `RaftCore::campaign_now` — a thin wrapper that
runs exactly the pre-vote round an ordinary timeout would run, just
triggered now instead of waited for), instead of every replica waiting out
the timeout. `inplace_split_bench.rs` was rerun 3x, and — per the same
same-host-only-comparison discipline as the table above —
`split_build.rs`'s copy-based bench was rerun once fresh (not reused from
the original table, in case host conditions had drifted since):

| | in-place, pre-fix (rung 3, run1/run2/run3) | in-place, post-fix (run1/run2/run3) | copy-based, fresh same-host reference |
|---|---|---|---|
| write blip (max PUT) | 726.3ms / 509.7ms / 733.7ms | 300.7ms / 513.2ms / 508.0ms | 252.0ms |
| write blip median | **726.3ms** | **508.0ms** | **252.0ms** |
| put retries needed | 0 / 0 / 0 | 0 / 0 / 0 | (not instrumented) |

Two honest findings, reported plainly:

- **The fix is a real, substantial improvement** — median write blip drops
  from 726ms to 508ms (≈30% down), with the best individual run (300.7ms)
  landing right at the copy-based path's own median. Every run remains a
  single slow request with **zero** retries, confirming the mechanism is
  doing what it was built to do: the parked write's own wait is shorter,
  not converted into a different kind of failure.
- **The median does NOT reliably reach the copy path's ~300ms bar, and this
  is reported as a genuine residual rather than forced with unrelated
  tuning.** The immediate campaign only supplies ONE vote (the ex-parent-
  leader's own) instantly; a 3-node child still needs a SECOND voter to
  grant a pre-vote/vote before it can elect. That second voter is a
  *different* replica's own `materialize_split_child` call completing and
  starting its own `RaftKvNode` for the child — gated by *that* replica's
  own tablet-host reconciler tick, not by anything this rung changed. Rung
  3 already fast-forwards that cadence to `INPLACE_SPLIT_RECONCILE_INTERVAL`
  (50ms) for the duration of an active split, but "50ms fast-poll cadence
  plus this host's own ~100ms per-round-trip floor" (the idle GET median
  measured here and in the rung-3 table is consistently ~108ms, the same
  environment characteristic, not new contention) is enough to explain a
  few-hundred-ms tail depending on how the fork's own commit instant lands
  relative to that second replica's next tick — not a cold multi-hundred-ms
  *randomized* timeout, but not zero either. This is squarely **rung 3's
  reconciler-cadence design**, not rung 4's own mechanism, so tightening it
  further is out of scope here (rung 4 is "who campaigns and when," not "how
  fast do the other replicas materialize") — left as a candidate for a
  future rung if the gap matters in practice, not force-fit into this one.

**Measurement note addendum (2026-08-25) — rung 4 layer 1: eager child
materialization at the fork, closing the residual the addendum immediately
above named.** Diagnosis (confirmed exactly as predicted): a freshly-forked
child's SECOND voter — the one that must grant the campaigning replica's
pre-vote before it can win a quorum — only ran its own
`materialize_split_child` on that replica's *next scheduled* tablet-host-
reconciler tick, which (even at rung 3's fast-polled
`INPLACE_SPLIT_RECONCILE_INTERVAL`, 50ms) could land anywhere up to 50ms
after the fork itself, on top of this host's own ~100ms per-round-trip
floor. Fix: **every replica now triggers its own materialization the
instant it applies `SplitTablet` locally**, instead of waiting for its next
scheduled tick — a new executor-agnostic **fork-observed signal**
(`ForkSignal`, the same `AtomicBool`+`AtomicWaker` shape as this crate's
existing `ProposeSignal`/`ApplySignal`/`WakeSignal`), raised by the
**async apply task** (`apply_and_compact`'s `KvCommand::SplitTablet` arm,
right after the durable split marker commits — never by the sync,
I/O-free `RaftCore`, per ADR 0003/0038 discipline) and consumed by a new
`host::Reconciler::fork_wake()` fan-in future that `animusd::
tablet_host_reconciler_loop`'s own `tokio::select!` races as a third arm
alongside `metadata_watch`/the periodic fallback. This is deliberately a
**wake, not a duplicated mechanism** — the trigger moved; `materialize_
split_child`'s clone/trim/host logic, its per-child G4 commit points, and
its idempotent re-derivation from the durable split marker are all
byte-for-byte unchanged, following the same discipline PR #394's own
"move the trigger, not the mechanism" lesson already named
(`docs/engineering-lessons.md`). The eager wake is deliberately **not
durable** — a crash between the apply task raising it and any tick
consuming it simply loses the signal (see `docs/engineering-lessons.md`'s
new entry) — recovery is unaffected because it was never on the critical
path: the reconciler's ordinary periodic tick still re-derives the fork
from the durable marker (`pending_split()`) exactly as it always did.

`inplace_split_bench.rs` was rerun 3x, and `split_build.rs`'s copy-based
bench was rerun once fresh, same-host, same-session, immediately
afterward (never concurrently), following the same discipline as every
prior measurement in this ADR:

| | in-place, rung 4 (campaign only, prior session) | in-place, + layer 1 (run1/run2/run3) | copy-based, fresh same-host reference |
|---|---|---|---|
| write blip (max PUT) | 300.7 / 513.2 / 508.0 ms | 775.0 / 355.7 / 258.7 ms | 447.9 ms |
| write blip median | **508.0ms** | **355.7ms** | **447.9ms** |
| put retries needed | 0 / 0 / 0 | 0 / 0 / 0 | (not instrumented) |
| fork/build wall clock | ~4.5s (prior session) | 4.55 / 4.17 / 4.15s | 8.04s |
| idle GET floor (this host) | ~108ms | 108ms (identical across all 3 runs) | 108ms |

Findings, reported plainly, in both directions:

- **The median write blip drops again**, from 508.0ms to 355.7ms (≈30%
  further down, and ≈51% down from rung 3's original 726.3ms) — and, for
  the first time, lands **at or below** a same-session copy-based
  reference run (447.9ms), meeting this rung's own acceptance target. The
  best individual run (258.7ms) sits comfortably inside the copy path's
  own historically-observed 250–300ms band; every run remains a single
  slow request with **zero** retries, so the mechanism is doing what it
  was built to do — shortening the parked write's own wait, not changing
  its failure shape.
- **Variance is still real and reported honestly, not smoothed over.**
  One of the three runs (775.0ms) is the single *worst* write-blip number
  measured anywhere in this ADR's whole measurement history, including
  every pre-fix number — and the fresh copy-based reference run (447.9ms)
  itself reads noticeably higher than this ADR's own earlier copy-path
  numbers (252–304ms, rung 3/4's tables above). The one measurement that
  stayed **exactly** stable across every run, in both benches, on both
  sides of this addendum (108ms, to the millisecond) is the idle
  linearizable-read floor — the same host-characteristic anchor rung 3/4
  already used to argue this is measurement variance, not a regression:
  whatever is producing the wider spread is landing in the "how long does
  this one parked write wait" component specifically (still bounded by
  the same small constants — the 50ms fast-poll cadence and this host's
  own ~100ms round-trip floor — this rung's own fix reasons about), not a
  change in the underlying mechanism's correctness or a new source of
  cost. Median-of-3 remains the right statistic to compare here for
  exactly the reason rung 3/4 already gave: a single run either side could
  read as "regressed" or "fixed" depending on which one you drew.
- **The eager wake's own contribution is structurally distinct from
  rung 4's campaign fix**, and this addendum's corpus cells (below) prove
  it directly rather than only inferring it from the bench: `ANIMUS_
  INPLACE_SPLIT_SEEDS`'s new `eager_wake_and_reconciler_tick_race_
  benignly` cell proves a hosted tablet's `fork_wake()` resolves with
  **zero** reconciler ticks having run, and that a second, immediately
  following tick (standing in for the ordinary periodic fallback
  rediscovering the identical state) is a byte-for-byte no-op; `crash_
  after_apply_loses_the_eager_wake_but_reconciler_fallback_recovers`
  proves the signal's own non-durability is harmless — a crash that
  strands the wake unconsumed still recovers, purely off the periodic
  tick reading the durable marker, with no special-cased path.

**As-built (2026-08-25) — rung 4 layer 2: `SplitMode`'s default flips from
`Copy` to `InPlace`.** The measurement record above — the layer-1 addendum's
355.7ms in-place median vs. its own same-session 447.9ms copy-based
reference (≈1.8× faster), the 500-seed `ANIMUS_INPLACE_SPLIT_SEEDS` +
20-run repeated-`inplace_split_e2e` soak, and the still-open, honestly
reported write-blip-tail residual (this addendum's own "Variance is still
real" finding, squarely rung-3's reconciler-cadence design, not a
correctness gap) — was judged sufficient to trust the in-place path as the
default, with the copy path staying fully selectable behind `--split-mode
copy` while its own deletion (this ADR's remaining rung 4 layer) is
sequenced as separate follow-up work rather than folded into the flip
itself. `animusd::config::SplitMode`'s `#[default]` variant moved from
`Copy` to `InPlace`; every call site that reads `SplitMode::default()`
rather than a caller-supplied value picked up the new default for free,
including the `--cluster-control`/`--cluster-data` dev-cluster shape and
the standalone `control`/`data`/`join` subcommands (rung C2's report had
flagged these as pinned to `Copy` with no flag plumbed through them — they
are now pinned to `InPlace` the same way, still with no `--split-mode` flag
of their own). `--split-mode {copy,inplace}` itself is unchanged — it still
threads through `--config`/`--node` and `--cluster N` only, and still
accepts `copy` explicitly for as long as the workflow exists. Every test
that specifically exercises the ADR 0050 copy workflow's own mechanics
(its `Splitting`/`Building` intermediate metadata shape, its
build/freeze/tail driver, its own bench) was audited and pinned to
`SplitMode::Copy` explicitly rather than silently riding the new default;
tests that exercise a split generically (auto-split, transactions racing a
split, streams/GSI convergence across a split) were left on the default and
now cover the in-place path instead — see
`crates/animusd/CLAUDE.md`'s `SplitMode` entry and
`docs/engineering-lessons.md` for the audit's own record of which tests
moved to which mode and why.

**A finding from that audit worth flagging explicitly, not just logging**:
`tests/streams_e2e.rs::multi_split_soak_streamed_gsi_table_under_mixed_load`
(rung 8's own named acceptance soak) went from "issue #298 occasionally
sighted" under copy to reproducing #298 on every run once its fixed
120-write/300s budget ran under in-place's much faster per-split
convergence — the flip didn't change #298's trigger condition, it let far
more splits complete inside the same fixed test budget, and #298 is a
per-split-boundary race. Pinned back to `Copy` for now (see G5 below and
`docs/engineering-lessons.md`'s matching entry for the general lesson).
This is a real, load-bearing dependency for **this ADR's own remaining
rung 4 layer**: deleting the copy workflow removes the option to pin away
from #298 here, so that deletion cannot ship until #298 is either fixed or
this soak's own budget is deliberately re-tuned to reproduce it at a
tolerable rate under in-place — whichever the maintainer decides, it needs
deciding before that layer, not discovered mid-deletion.

## Open forks (deliberately not decided by this ADR)

| # | Fork | Status |
|---|------|--------|
| G1 | Parent-engine drain-then-reclaim protocol post-cutover (Stage 4) | **Decided (2026-08-25), reversed from this ADR's own Stage 4 prose**: the drain gates stay PRE-cutover. The existing GSI-drain veto (now accelerated, Alternative 1/issue #288) and backfill-seeder veto run against the now-static, forked (hence write-frozen — Stage 3's own apply-time seal) parent exactly as in the copy-based endgame, gating the `CutoverSplit` propose the same way they gate it there today. Rationale: the parent's engine is retained, intact, on every fork participant until cutover reclaims it (nothing analogous to the copy-based `Freeze`-then-tear-down timing pressure exists — the fork itself is instantaneous and non-blocking), so there is no reason to invent a NEW retained-engine-after-retirement drain protocol when the existing pre-cutover one already has everything it needs merely by running against the (already fully-formed) children's own consumer state instead of the parent's. This is a caller-side (driver) concern in BOTH workflows — `MetaCommand::CutoverSplit`'s own apply never gated on drain state even in the copy-based branch — so it is entirely residue for the driver layer above (see the "Residue" paragraph above), not a change to the command shipped in this rung. |
| G2 | Range-filtered `InstallSnapshot` for a learner at a disjoint final home, vs. accepting the ~2× bytes and pre-trimming at the clone step | **The pre-trim half closed (2026-08-31)**: `LsmEngine::clone_to_filtered`'s whole-file assignment pre-trims at the clone step itself (see rung 2's own as-built amendment above) — a source SSTable wholly outside the child's own keep-set is never linked in, so the clone this rung's `trim_split_child` post-processes is already close to the child's own final size for any table set where the split boundary happens to land on table boundaries; a table straddling the boundary is still linked whole and trimmed after, same as before. This bounds — but does not eliminate — the disjoint-home learner's own `InstallSnapshot` bytes, since a snapshot ships the group's CURRENT engine state (now genuinely close to range-filtered rather than a full duplicate of the parent). **Still open**: a dedicated range-filtered `InstallSnapshot` wire format remains unshipped — any bytes a straddling table still carries beyond the child's own range ship over the wire exactly as they did before, and no snapshot-transfer code was touched by this closure. |
| G3 | Whether ADR 0030's permanently-non-voting growth mirror is re-expressed as a learner that is never promoted, once learners exist | Open — noted, not committed to |
| G4 | Exact crash-recovery idempotency contract for group-mint-at-apply (Stage 3) | **Decided (2026-08-25)**: two independent commit points, each checked and skipped on its own before a retry redoes it. (1) **The engine commit** — `EngineFactory::clone_engine`'s target manifest replace (ADR 0058 rung 2's own "absent or complete, never torn" contract) — checked via `EngineFactory::probe(child)` BEFORE ever cloning; if the engine already exists, an earlier attempt already committed it (possibly crashing before the group started), so re-cloning is skipped entirely (a second clone against an already-trimmed, or already-written-to, target would be a correctness bug, not merely wasted work) and the existing engine is simply re-opened. (2) **The group commit** — successfully calling `RaftKvNode::start_hosted` and registering the handle in the reconciler's own `hosted` map — reuses the IDENTICAL optimistic-claim-then-execute discipline the ordinary `Host` action already has (`plan` claims the child into `LocalState::hosted` before materialization ever runs; a failure calls `LocalState::release_unconfirmed_host` so the next tick retries). No separate "child group state" artifact needs writing at all: a brand-new Raft group bootstraps fresh from its own `all_nodes` config every time regardless (empty log, first election), so the group's own identity IS simply "does a live `RaftKvNode` exist for this id" — the same fact every other tablet's lifecycle already tracks. A crash between the two commit points re-runs exactly the group commit on retry; a crash before either re-runs both, deterministically, from the same `(parent, child, range, bootstrap_voters)` inputs every replica computes identically from the fork entry itself. Proven directly in `tests/inplace_split_reconciler.rs`'s `crash_between_fork_and_materialization` scenario (a node crashes after its own Raft log replicates the fork but before its reconciler ever ticks past it, and must independently recover both children plus the pre-fork write on restart). |
| G5 | Issue #298 (exactly-once duplication/deficit at a split boundary) reproduces far more readily under in-place's higher splits-per-unit-time than the copy-based workflow ever exercised it, and blocks rung 4's remaining copy-deletion layer, which removes the option to pin the affected soak to `Copy` while investigating | **Partially resolved (this branch's #298 fix PR).** Three mechanisms found and fixed, all confirmed **pre-existing** (none specific to in-place split's own fork/materialize path — in-place's density of splits-per-soak-run just gave each far more chances to fire per fixed test budget): (1) the frozen-endgame GSI-drain acceleration loop could self-deadlock across two co-hosted, cross-referencing tablets (`is_retryable_elsewhere` break-early + a fail-fast `cp_kind_write_raw_once` write primitive); (2) the ordinary per-tick seal arm (`seal_tick`) was missing the same `!splitting` guard `trim_janitor` already had, letting it race a `Splitting` tablet's own dedicated endgame seal loop and, combined with `seal_now` reading `effective_metadata()` instead of `metadata_fresh()` for a decision that becomes immutable segment bytes, produce two adjacent, non-colliding epochs with silently overlapping coverage — the confirmed mechanism behind the literal duplicate-record symptom, fixed by adding the guard and switching to `metadata_fresh()`; (3) `txn_resolver_loop`'s own recovery sweep discarded the `created_ts` hint `txn_recover`'s orphan-abort fallback needs, so a transaction record made unreachable by a split had no path to ever resolve — fixed by passing the hint through. **Not fully resolved**: a fourth, distinct failure shape — an intermittent base-row miss under a linearizable (`ConsistentRead: true`) read — remains open. One classified repro pins it as a **leader-side gap**: the row was present on both non-leader replicas of the owning tablet but absent specifically on that tablet's own current leader, which is what a `ConsistentRead` read always resolves to regardless of what followers hold — ruling out routing/caching as the explanation. Subsequently root-caused and FIXED in this same PR: `LsmEngine::clone_to` trusted `flush()`'s `Ok(())`, but `flush()` silently no-ops while a concurrent apply is in flight, so a materialization clone could hard-link the SSTable list while acked rows were still memtable-only — permanently excluding them from the child (Train-2-introduced; red/green multi-thread ProdEnv regression `lsm_clone_concurrent.rs`; zero recurrences in 15 post-fix soak runs). **`unresolved_decided`'s own lookup-failure gap, subsequently fixed (residuals round)**: `txn_resolver_loop`'s decided-but-unresolved sweep had no fallback at all when its own `txn_record_view` lookup failed (unlike the `pending_txns` path fixed above) — a persistent failure (e.g. the record's own tablet retiring mid-recovery) would retry forever with no bound and no signal. Fixed with a driver-local `BTreeMap<TxnId, first_seen>` grace tracker (pruned every tick against whatever `unresolved_decided()` actually reports, mirroring `change_consumer_loop`'s `first_hot_seen`/`marker_bytes_seen`): past `RECOVERY_GRACE` with no successful lookup, log + meter once (new `CpTxnUnresolvedDecidedStuck` metric) and keep retrying quietly rather than resolving anything speculatively — this can only ever make the loop stop *claiming* background progress it isn't making, never mis-resolve, since correctness for the stuck transaction's participants is independently covered by the on-demand foreign-intent read-path push (ADR 0018 §2/PR5 §3). **Named gap**: no dedicated deterministic regression was added for the seal-boundary mechanism (2) — `animusd` has no `animus-sim` dependency, so it isn't `SimEnv`-reachable, and a precise `ProdEnv` unit repro of the exact interleaving would need new test-hook plumbing this pass didn't build; the soak itself (`tiny_seal_knobs()`, cascading splits) is what caught it and is what validates the fix (10 consecutive clean in-place runs). Rung 4's remaining copy-deletion layer stays blocked on the one still-open residual: a rarer (~1 in 15 soak runs) stream-delivery-count failure beyond the fixed mechanisms — the soak stays pinned to `Copy` until that residual is resolved. **A FIFTH shape (2026-08-26, issue #298 residuals), investigated, NOT confirmed/fixed — soak stays pinned to `Copy`.** With every one of the four mechanisms above already on `main`, a 21-run un-pinned `InPlace` soak still failed 4/21 (~19%): two runs showed a NEW duplicate-delivery signature — one member of a transactional write pair delivered TWICE inside a SINGLE already-sealed shard (two distinct sequence ids, same tablet+epoch — a genuine double-append into the hot change log before any seal, not a seal-boundary race), with the OTHER member of the same pair showing mechanism (2)'s already-fixed cross-epoch pattern in the same run — implying one shared underlying event (the transaction's resolve running more than once) manifests differently per participant depending on whether a seal happens to land between the two applies on that participant's own tablet. Two other runs showed a related but distinct shape: `ClientCtx::txn_recover`'s in-doubt-recovery sweep independently decided **Abort** (`all_staged=false`) for a transaction whose write the test's own client believed had already succeeded, permanently losing an acked item. Diagnostic instrumentation (propose/resolve/recovery tracing, not shipped — see the engineering-lessons entry) captured the abort-loses-an-acked-write shape live but did not catch a live trace of the duplicate-materialize shape's own causal chain. Two real, confirmed-by-reading structural gaps were found and are strong candidates, neither proven as *the* root cause of either captured shape: (a) `KvCommand::TxnStage`'s apply-time `blocked_by` check (`animus-cp-data/src/lib.rs`) only rejects overwriting a *different* transaction's unresolved intent — it never checks whether the current value is already `Envelope::Committed`, so a stale/duplicate `TxnStage` propose landing after its own transaction already fully resolved can silently resurrect the key into `Intent` and let a later resolve re-materialize its change record a second time, at a fresh HLC; (b) `ClientCtx::txn_recover`'s `all_staged` computation (`animusd/src/lib.rs`) treats a `txn_verify` `Err` (a transient "no leader reachable" routing hiccup — expected and common while a participant's tablet is mid-fork/cutover) identically to a definitive "never staged," which can trigger an incorrect Abort racing the transaction's own still-live coordinator decision. Per this repo's "no speculative fix" rule, neither gap was patched this round — see `docs/engineering-lessons.md`'s matching entry for the full writeup, soak tally, and instrumentation description. The soak stays pinned to `Copy`; rung 4's remaining copy-deletion layer is blocked on this residual exactly as it was on the fourth shape before that one was root-caused. **Both candidate gaps from the fifth shape CONFIRMED and FIXED (2026-08-26, follow-up round).** (a) confirmed live (`delivered=146/144`, one transactional pair member duplicated under a single sealed shard) — fixed via `TxnTracker::recently_resolved`, a bounded per-group memo checked by `(key, txn_id)` identity, rejecting a same-txn restage over an already-resolved key; tracing the capture's own `txn_id`s showed the LIVE trigger is actually a deeper, distinct, still-open mechanism (a client-level retry racing its own already-committed first attempt after a coordinator confirmation-loss message was marked retryable identically to a provably-safe refusal) that this fix does not close, named but not fixed. (b) confirmed live (`all_staged=false`/`Aborted` immediately preceding an "acked write lost" panic) — fixed by making `txn_recover` decline (never decide) on any `Err`/inconclusive query, at BOTH the `all_staged` loop and a sibling conflation found in the same pass (`RaftKvNode::txn_record_view`'s own plain-`Option` "not served vs. absent" conflation, widened to the `stale_get_served` "served" discipline). **The soak is still NOT un-pinned.** A mandated 30-run contention-free proof soak found the two fixed mechanisms did not recur, but surfaced a THIRD residual: a **genuine, non-inconclusive** `all_staged=false` (a real `Ok(false)`, not a folded `Err`) computed for a transactional pair, coincident with an "acked write lost" panic in one capture and with no ill effect in another — plus an unexplained structural detail (`intent_spans` held entries for both members of what should be a plain two-item transaction, implying a third, unidentified anchor key) neither confirmed nor root-caused before this round's time ran out. Combined with the already-open lineage-delivery-timeout residual, the soak fails a clean 30-run bar and stays pinned to `Copy`; rung 4's remaining copy-deletion layer stays blocked. See `docs/engineering-lessons.md`'s matching amendments for the full account of both the two fixes and the newly-surfaced residual. **2026-08-26 follow-up, unrelated to the above**: the `clone_to` fix's own bounded flush-retry loop turned out to be a starvation flake by construction against a persistent concurrent writer (CI caught it failing on PR #404, a change that doesn't touch this crate) — fixed by replacing the retry with a point-in-time memtable snapshot; see this ADR's rung 2 "Amended" note and `docs/engineering-lessons.md`'s 2026-08-26 entry. **2026-08-27 follow-up (issue #298 "deep shape A" residual)**: the client-level-retry double-materialize mechanism named in the amendments above (ADR 0018's own 2026-08-27 amendment) is closed — `ClientRequestToken` idempotency's outcome bookkeeping no longer records a false `CANCELLED` for an ambiguous `cp_txn` outcome, and the soak's own writer now uses a token, closing the client-level race this mechanism needed to fire. 24/25 clean across two contention-free batches, with neither this mechanism nor the lineage-delivery-timeout residual recurring — but a NEW, different residual surfaced (one genuine `TransactionConflict` cancellation, hypothesized as the fix's own allowlisted retry racing a prior attempt's still-unresolved intent faster than recovery clears it, not confirmed live). Soak stays pinned to `Copy`; see `docs/engineering-lessons.md`'s matching entry for the full account. **2026-08-29 follow-up (ADR 0018's matching 2026-08-29 amendment)**: `KvCommand::TxnResolve` gained the `ResolveOutcome` (`Resolved`/`Fenced`/`OutcomeMismatch`) apply-time outcome channel ADR 0018 §3/§6 had identified as the clearest path to closing this whole class, plus a coordinator-side bounded-retry-with-fresh-routing fix (`ClientCtx::txn_resolve_participant_retrying`) and an independent, previously-silent bug (`TxnTracker::unresolved_decided` used to clear unconditionally on ANY resolve apply, including a fence-miss, silently disabling the passive resolver's own retry for exactly the transaction that needed it). **This does not itself un-pin the soak** — no fresh 30-run un-pinned soak was run this round, and per this file's own "any failure keeps the pin" mandate only that soak can move the pin; see ADR 0018's amendment for the full account of what was (and wasn't) proven. |

## Relationship to the original ADR 0017 §4 design

ADR 0017 §4's in-place split (Stage D) and this proposal share the same
top-level shape — split a live group's data in place, bootstrap a new
group from inside the apply path — and differ in exactly the ways that
matter for correctness:

- ADR 0017 §4 minted the sibling via a **separate `Env`-seam capability**
  (`Coresident::sibling`, a fresh `NodeId`/env/directory/WAL per new
  tablet) invoked from a **split hook** outside the ordinary apply/commit
  discipline. This proposal mints **inside** the committed apply path of a
  single log entry, with no new `Env` capability and no capacity cliff
  (`Coresident`'s `CP_SIBLING_POOL` cap is exactly the kind of hazard a
  fixed-size pre-bound pool creates, and this design has no pool to
  exhaust).
- ADR 0017 §4 was **two-phase across two Raft logs** — a control-plane
  `SplitTablet` (metadata) and a separately-agreed data-plane `Split`
  (mechanics) — which is what produced ADR 0028's entire bug litany. This
  proposal's fence-and-activation is **one entry, one log** (the parent's
  own); the control plane's `CutoverSplit` (Stage 4) is a **pure
  recording step after the fact**, not a second phase the split's
  correctness depends on.
- ADR 0017 §4 predates per-tablet private engines entirely (it split a
  per-tablet engine that already existed under the *original*, pre-ADR
  0028 layout). This proposal depends on ADR 0050 Decision 1's private
  engines specifically for the SSTable-clone materialization step — it is
  not simply "ADR 0017 §4, resurrected," but a design that could only be
  written once Decision 1 had already changed the addressing scheme
  underneath it.

## Testing plan

- **Train 1's own corpus** (learner catch-up/promotion under fault
  injection) — see "Testing plan (Train 1)" above.
- **Group-mint-at-apply fault corpus** (Train 2, `SimEnv`, its own depth
  knob): leader crash/re-lead at every stage (mid-learner-catch-up, the
  instant after `SplitTablet` commits but before every replica has
  applied it, post-fork pre-`CutoverSplit`, post-cutover pre-parent-
  reclaim); double-apply idempotency on WAL replay after a crash between
  materialization and config durability (fork G4); a learner crashing and
  restarting mid-catch-up, mid-fork; control-plane failover mid-
  `CutoverSplit`; a concurrent unrelated rebalance racing the split's own
  learner-add.
- **Streams exactly-once across the new lineage**: the existing
  `stream_lineage_corpus` (ADR 0050 rung 6) needs no basis-inheritance
  changes — fork F9's `split_lineage` write is unchanged in *when* and
  *what* it records, only in the mechanism that produces the moment it
  records at.
- **Transactions**: the same intents-copied/resolves-chase property ADR
  0050's F7 corpus already proves, re-targeted at the new fork mechanism —
  should require **less** new test surface than ADR 0050's own equivalent,
  since there is no freeze-window race to model (Stage 3's fork is a
  single atomic apply, not a multi-second window a resolve can land
  inside).
- **Reconciler corpus**: the learner-add/catch-up/fork/child-trim sequence
  replaces the `Building`-host/cutover-flip scenarios ADR 0050 rung 3
  built; per-tablet engine open/clone/close/delete under crash restart
  needs its own new cells (the clone step is new; open/close/delete are
  unchanged from ADR 0050 Decision 1).
- **Bench, following the rung-8 lesson directly**: a **continuous writer**
  against the tablet being split, not a burst — this ADR's central claim
  is that Train 2 has no convergence predicate for a continuous writer to
  starve, and the bench that proved the opposite for ADR 0050's design is
  exactly the bench that should be pointed at this one before it is
  trusted. Measure: fork-to-first-child-Active wall clock, write blip
  duration and shape (bytes learners must catch up vs. bytes actually
  moved), and the SSTable-clone step's own cost in isolation.

This ADR builds on ADR 0050 (whose Decision 1 and fork F9 it keeps, and
whose Decision 2 it proposes replacing), ADR 0009 (the `RaftCore` Train 1
extends), ADR 0018 §2 (the log-position ordering-fence principle Train 2
generalizes from a range-scoped seal to a whole-group fork), and ADR 0017
§4 (the original in-place design this proposal is not a resurrection of,
but a structurally corrected descendant of — see "Relationship to the
original ADR 0017 §4 design").
