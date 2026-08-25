# ADR 0058 — Learner replicas and in-place tablet split

- **Status:** Proposed — a design for maintainer review, not scheduled.
- **Date:** 2026-08-24
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
- **Decision record:** none yet — this ADR exists to be reviewed, forked,
  and either accepted, amended, or rejected by Guillaume, the same way ADR
  0050 itself went through fork review before Decision 1/2 were settled.

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
  overlap. Two mitigation forks, both left open (see below): range-filtered
  snapshot install (ship only the learner's eventual half-range), or
  accept the cost and pre-trim at the SSTable-clone step instead.
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
3. **The `SplitTablet` entry, group mint, and materialization**, behind
   the existing split trigger (ADR 0034) — built and proven alongside the
   still-shipping copy-based workflow, not in place of it, until it is
   trusted.
4. **Delete the ADR 0050 build driver, the freeze-as-outage path, and the
   cutover vetoes**, in the same bottom-up, red→green-per-cell discipline
   ADR 0050's own Train B used.

**Independent of this sequencing**, the interim GSI-drain endgame
acceleration (Alternative 1 — an acceleration, deliberately *not* a bound;
see that alternative for why a bound would be unsafe) should ship
**before rung 1**, as its own small, separate change — it fixes a
slow-convergence class on the record today (issue #288) and should not
wait on any part of this proposal.

## Open forks (deliberately not decided by this ADR)

| # | Fork | Status |
|---|------|--------|
| G1 | Parent-engine drain-then-reclaim protocol post-cutover (Stage 4) | Open — needs its own design pass before rung 3/4 |
| G2 | Range-filtered `InstallSnapshot` for a learner at a disjoint final home, vs. accepting the ~2× bytes and pre-trimming at the clone step | Open — a mitigation choice, not blocking rung 1–3 |
| G3 | Whether ADR 0030's permanently-non-voting growth mirror is re-expressed as a learner that is never promoted, once learners exist | Open — noted, not committed to |
| G4 | Exact crash-recovery idempotency contract for group-mint-at-apply (Stage 3) | Open — the load-bearing detail of rung 3, deliberately left to that rung's own design, not settled here |

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
