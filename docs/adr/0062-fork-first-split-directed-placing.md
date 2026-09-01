# ADR 0062 — Fork-first in-place split with directed placing

- **Status:** Accepted — implemented (rungs 1–7, all as-built notes below;
  the Sequencing rungs' own numbering starts at the `Metadata::
  split_placing` groundwork, landed as `a26de6c`). Rung 7 (e2e + bench
  re-validation) closed this ADR's own "not predicted or asserted here"
  deferral in its Testing plan. **Issue #513 investigated and closed as
  not reproducible**: directed Placing's own convergence machinery
  (`reconfigure_step`, ADR 0058 Train 1, unmodified here) was suspected of
  only reliably converging a one-replica-difference target, with a
  two-of-three target oscillating indefinitely (filed during rung 6). A
  dedicated re-investigation could not reproduce that oscillation — see
  the amendment below — so directed Placing's own reach was never actually
  narrower than "whatever `select_replicas` computes." **2026-09-01
  amendment**: the cluster>RF bench rung 7 named as out-of-scope is now
  run — it confirms this ADR's central time-to-relief claim decisively
  under sustained write load, but also surfaces a new, open, unconfirmed
  finding (the post-cutover directed-Placing completion loop failing to
  reach `done` within a 240s budget in 2 of 3 runs) that is flagged for
  follow-up, not fixed here — see that amendment for the full account.
- **Date:** 2026-08-31
- **Supersedes:** [ADR 0058](0058-learner-replicas-in-place-split.md) Train
  2 Stage 1's fork **F5** half only — the fused split+move, where
  `BeginSplitInPlace` consults placement for both children's *final* homes
  and the parent's group recruits that whole union as learners before it
  can fork. Everything else of ADR 0058 Train 2 is kept unmodified and
  depended on directly: the single-entry `SplitTablet` atomic mint (Stage
  3), the local SSTable-clone-and-trim materialization on every fork
  participant, the G4 crash-recovery idempotency contract, the rung-4
  deterministic-first-leader campaign fix, the rung-4-layer-1 eager
  materialization wake, and G1's decided pre-cutover drain-gate placement.
  ADR 0058 Train 1 (the learner membership class in `RaftCore`, and
  `reconfigure_step`'s learner-phased add sequencing) is not superseded at
  all — this design's own convergence mechanism (§2 below) is a direct
  *consumer* of it, unchanged.
- **Amends:** [ADR 0029](0029-replica-rebalancing.md) (a split child's
  replica placement is no longer decided once, at mint, the way
  `select_replicas_balanced`/fork F5 decided it for the copy-based
  workflow and the superseded half of ADR 0058 — it is decided at cutover
  and then driven the same way an ordinary rebalance move is); [ADR
  0031](0031-tablet-host-reconciler.md) (the control-plane leader's own
  `reconcile_loop` gains a third phase, alongside repair and rebalance);
  [ADR 0050](0050-per-tablet-storage-copy-based-splits.md) fork F5 (a split
  child's *final* home is no longer chosen at the same moment as the fork
  itself — see "Relationship to ADR 0050/0058's fork F5" below).
- **Depends on:** [ADR 0058](0058-learner-replicas-in-place-split.md)
  Train 1 (the learner membership class `reconfigure_step` already uses
  for every other replica move, reused here unmodified) and Train 2's
  single-entry mint/materialization mechanics (kept, see above); [ADR
  0005](0005-placement-residency.md)/[ADR
  0029](0029-replica-rebalancing.md) (`animus-placement`'s
  `select_replicas`, `reconcile_placement`'s already-established
  frozen-for-non-`Active`-tablets discipline); [ADR
  0059](0059-backup-restore.md) §3/§4 (the `BeginBackup`
  derive-from-already-agreed-`Metadata`-at-apply-time precedent this
  design reuses for placement, and the `MarkIndexBackfilled`/
  `RecordBackupTabletComplete` per-tablet-leader-reports-completion idiom
  this design's own completion signal is modeled on).
- **Decision record:** accepted in design by Guillaume on 2026-08-31. Four
  named forks were resolved as part of accepting this ADR, referenced by
  letter throughout: **(A)** split-complete is a derived diagnostic, never
  a serving gate; **(B)** an unsatisfiable-at-cutover placement is written
  as a durable, visible, keep-retrying obligation, mirroring
  `reconcile_placement`'s own stance rather than inventing a bound; **(C)**
  placement assignment happens once, at `CutoverSplit`'s own apply, as a
  pure function of already-agreed `Metadata` — the same discipline
  `BeginBackup` established; **(D)** dropping the learner union from
  `bootstrap_voters` is an accepted, narrow, self-healing residual, not a
  correctness gap.

## Context

### What ADR 0058 Train 2 actually ships today, and the one design choice this ADR revisits

ADR 0058 Train 2's in-place split is, structurally, exactly right: a
single Raft entry (`KvCommand::SplitTablet`) on the parent's own log mints
both children directly, materialized locally on every fork participant by
cloning the parent's own already-open engine — no bespoke replication
protocol, no freeze-shaped write outage, no residue-transfer window. That
mechanism (Stage 3, its crash contract, its deterministic-first-leader
fix, its eager-wake fix) is this ADR's own load-bearing dependency and is
**not** touched here.

What *is* touched is Stage 1/2, the phase that runs *before* the fork:

> `BeginSplitInPlace`'s apply adds the union of the children's chosen
> homes as learners to the parent's own group. [...] The new learners
> catch up via the existing `InstallSnapshot` + log replay.

This is ADR 0050's fork F5, carried over into the in-place design mostly
by inertia: F5 was decided for the **copy-based** workflow, where fusing
"pick the children's final homes" with "the split itself" was free — the
copy driver had to ship bytes to *some* destination anyway, so shipping
them straight to the final one cost nothing extra. Under the in-place
design the fusion is not free, and the code confirms it in three separate
places:

1. **`crates/animus-cp-data/src/host.rs`'s phase 1.5** (~ln. 574–660, the
   "recruited via a child's replicas" branch) has to teach `plan`'s
   host-candidate test a **second** membership predicate — a node named
   only in a child's own `replicas`, never the parent's — purely so that
   node has *somewhere to run a `RaftKvNode` for the parent* before it can
   ever be `add_learner`'d to it. Without this, `AddSplitLearner`'s
   `home` targets a node with no local Raft instance running to receive
   `AppendEntries` at all (the module's own doc: "there is nothing there
   to receive the resulting `AppendEntries` at all"). Phase 3's release
   check needs a matching exemption for the identical recruited set. This
   is real, permanent surface area purely to let a not-yet-a-replica node
   host a tablet it will never actually own.
2. **`crates/animus-cp-data/src/lib.rs`'s `bootstrap_voters` capture**
   (~ln. 6604, inside `KvCommand::SplitTablet`'s apply arm) reads
   `core_guard.config()` **extended with `core_guard.learners()`** — the
   parent's voters *and* whatever cross-cluster learners Stage 1 recruited
   — specifically so both children start over-replicated relative to
   either one's own final placement, with Stage 5's ordinary
   `reconfigure_step` trimming the rest afterward. The whole reason this
   union has to be a superset in the first place is that F5's homes can be
   *disjoint* from the parent's own current replicas.
3. **ADR 0058's own risk section names the direct cost**: "a learner added
   at a child's *final* home [...] receives the **whole parent range** via
   `InstallSnapshot`, not the range-filtered half a copy-based `SeedBatch`
   bulk pass would ship — roughly 2× the network bytes [...] when the two
   children's homes don't overlap," left as an explicitly open fork (G2 —
   its clone-step half is since closed, 2026-08-31; see the "Companion
   decisions" bullet below).

None of this is a bug — every one of these is a deliberate, documented
consequence of choosing F5 for the in-place shape. But once the fork
mechanism itself is fast and cheap (Stage 3's whole point), fusing
placement into it stops paying for itself and starts *costing* structural
complexity: a whole second host-candidacy branch in the reconciler's
hottest per-tick path, a bootstrap-voter set that has to be a strict
superset of what either child actually wants, and a documented 2× learner
InstallSnapshot bill with no closed mitigation. This ADR's premise is
narrow: **decouple the fork from the placement decision**, exactly the way
Stage 3/4 already decoupled the fork's *fence* from the fork's *activation*
(the parent's own `SplitTablet` apply is the fence; `CutoverSplit`, a
"pure recording step" per ADR 0058's own Stage 4 doc, is a separate,
later, independently-timed event).

### The insight this unlocks: placement is already a converged, driven process

`crates/animus-cp-data/src/host.rs:1090` — `Reconciler::execute`'s
`HostAction::Reconfigure` arm — calls `RaftKvNode::reconfigure_step`
directly, every tick, for every tablet this node leads, driving live Raft
membership toward whatever `desired` set `Metadata.tablets[t].replicas`
currently names. This is precisely the same machine ADR 0029's
`rebalance_step`/`Metadata::rebalance` already use to spread a grown
cluster's tablets: `crates/animus-control/src/meta.rs`'s
`rebalance_placement` (~ln. 1893) computes a single balance-improving
`CasTabletReplicas`, the control-plane leader proposes it, and
`reconfigure_step` (Train 1's learner-phased add sequencing, unmodified)
converges the live group to match — with no availability dip, because the
whole point of the learner phase is that a voter is never removed until
its replacement has caught up.

A split child that is born on its parent's own current homes and needs to
move to a *different* final set is not a new problem. It is the **exact
same problem** `rebalance_placement` already solves, for the exact same
reason (the cluster's shape changed since a tablet's replicas were last
chosen), converged by the exact same primitive. There is no reason a
split's own placement decision needs its own bespoke fused-with-the-fork
mechanism at all — it needs only (a) a decision of *where the child should
end up*, computed once, and (b) reuse of the machinery that already drives
any tablet's live replicas toward any recorded target. This ADR is that
reuse, made explicit.

## Decision

### 1. Fork first, always local

`BeginSplitInPlace`'s two children get the **parent's own current replica
set**, verbatim, identical for both children — never a placement-computed
final home. Concretely:

- **`ClientCtx::trigger_split`'s `InPlace` arm** (`crates/animusd/src/
  schema.rs:551`) stops calling `split_child_placement` (the F5
  `select_replicas_balanced` mint) for the in-place path. Both entries of
  `children: [(TabletId, Vec<NodeId>); 2]` carry `meta.tablets[tablet]
  .replicas.clone()` — the parent's own current set, read from the same
  already-fetched `meta` the confirm loop already holds. `MetaCommand::
  BeginSplitInPlace`'s own wire shape (`crates/animus-control/src/
  meta.rs:1138`) is **byte-for-byte unchanged** — this is a proposer-side
  computation change only, not a command-shape change. (The copy-based
  `BeginSplit` arm, and its own `split_child_placement` call, are
  untouched — see §4.)
- **`animus_tablet::SplitChild.replicas`'s doc comment changes meaning**,
  not shape: from "the child's placement-chosen final replica set (fork
  F5 [...])" to "the child's replica set at fork time — the parent's own
  current replicas, identical for both children, never placement-chosen."
  `InPlaceSplitIntent`/`SplitChild`'s Rust shape in `crates/animus-tablet/
  src/lib.rs` (~ln. 257–289) needs no field change.
- **Stage 1 and Stage 2 are deleted**: `HostAction::AddSplitLearner`,
  `host::plan`'s phase-1.5 learner-recruitment loop over `union_homes`,
  and the "recruited via a child's replicas" second host-candidate branch
  in phase 1 (`crates/animus-cp-data/src/host.rs`, the code discussed in
  Context point 1) all go away. A tablet's `inplace_split` intent, once
  recorded, has **nothing left to converge before it can fork** — every
  replica that already hosts the parent already has everything the fork
  needs. `host::plan`'s phase 1.5 collapses to: "an `inplace_split` intent
  exists and this replica is not yet forked here ⇒ propose
  `ProposeSplitFork` immediately, leader-only" (no learner-readiness gate
  at all, since there is no learner to wait on).
- **`bootstrap_voters`' capture drops the learner union** (`crates/
  animus-cp-data/src/lib.rs` ~ln. 6604): `let bootstrap_voters =
  core_guard.config();` — voters only, no `.extend(core_guard.learners())`.
  Both children bootstrap directly with the parent's own **current voter
  set**, no over-replication, no Stage-5 trim step needed at all (that
  step is also deleted — there is nothing left to trim down from).

**Fork D (accepted, narrow residual):** dropping the learner union means
an **unrelated, in-flight, ordinary rebalance's** learner on the parent —
`reconfigure_step`'s own Train-1 add-learner step, mid-catch-up for a
completely unrelated placement move, at the exact instant `SplitTablet`
applies — is no longer inherited by either child. Before this change that
learner rode along in `bootstrap_voters` as a bonus, half-caught-up
member; after this change it is simply absent from both children, and
whichever child a fresh reconcile decides that node still belongs on has
to re-run its own add-learner → catch-up → promote sequence from scratch,
via the ordinary Placing convergence path (§2) rather than inheriting
partial progress. This is judged acceptable: it costs at most one extra
`reconfigure_step` hop (bounded, self-healing, no availability loss — the
old voter that move was replacing is untouched either way), it only
matters in the narrow window where a rebalance and a split race on the
*same* tablet, and it trades away a genuinely obscure inheritance for the
much larger simplification of never fusing an arbitrary external node set
into the fork at all.

### 2. Directed Placing phase

Placement is decided **once, at `CutoverSplit`'s own apply**, as a pure
function of already-agreed `Metadata` — **fork C**, the same discipline
ADR 0059's `BeginBackup` already established for deriving its manifest
stub from agreed state rather than anything the proposer captured
(`crates/animus-control/CLAUDE.md`'s own words: "`BeginBackup` derives its
whole manifest stub [...] from already-agreed `Metadata` at apply time,
never from anything the proposer captured — the same determinism argument
`BeginSplit`'s child ranges and `CutoverSplit`'s child recomputation
already rest on"). `MetaCommand::CutoverSplit`'s in-place apply branch
(`crates/animus-control/src/meta.rs` ~ln. 1146–1180) already computes each
child's row and inherits the parent's policy at exactly this moment (per
that command's own doc: "the in-place workflow's only chance to, since
there was no tablet-map row to attach it to earlier"). This ADR adds one
more step to that same apply arm, per child:

```rust
// inside CutoverSplit's in-place branch, once per child, after the
// child's row (replicas = parent's inherited replicas) and its inherited
// policy are already decided:
let candidates = active_candidates(&self.members);   // same private helper
                                                       // reconcile_placement/
                                                       // rebalance_placement
                                                       // already use
match select_replicas(&candidates, &policy) {
    Ok(wanted) if wanted == child_replicas => { /* already satisfying: no entry */ }
    Ok(wanted) => {
        self.split_placing.insert(child_id, SplitPlacing {
            target: Some(wanted),
            done: false,
        });
    }
    Err(_) /* UNSATISFIABLE (too few Active candidates/domains) */ => {
        self.split_placing.insert(child_id, SplitPlacing {
            target: None,
            done: false,
        });
    }
}
```

**New replicated collection**: `Metadata::split_placing:
BTreeMap<TabletId, SplitPlacing>`, keyed by child tablet id.

```rust
pub struct SplitPlacing {
    /// What `CutoverSplit`'s own apply computed as this child's
    /// policy-satisfying target, at the moment it computed it. `None`
    /// means placement was UNSATISFIABLE at cutover — fork B (below).
    /// **Diagnostic only** — see fork B's own rationale for why the
    /// reconcile loop never trusts or updates this field after the
    /// initial write.
    pub target: Option<Vec<NodeId>>,
    /// Set once this child's live replicas converge to a fresh,
    /// currently-satisfying target. Never a serving gate (fork A).
    pub done: bool,
}
```

**Fork B (the unsatisfiable-at-cutover representation, decided):** when
`select_replicas` errs at cutover — too few `Active` candidates, or too
few distinct failure domains for a strict spread — the entry is still
**written**, with `target: None`, rather than skipped. This is
deliberate: `reconcile_placement`'s own repair phase already has an
identical shape ("keep trying every tick, forever, until candidates
recover") and never treats "currently unsatisfiable" as a reason to stay
silent about a tablet that needs attention — it just keeps re-evaluating.
`split_placing` mirrors that stance rather than inventing a liveness bound
(the wrong shape here, per ADR 0058's own Alternative 1 finding that a
*correctness*-adjacent convergence gate must never be force-bounded — see
§4/§5). The entry's presence, independent of `target`, is what makes an
unsatisfiable-at-cutover child a **visible obligation** — surfaced on
`/admin/status`/the Console the same way any other un-converged placement
row is, not a silent gap nothing will ever revisit.

**A deliberate design choice worth stating explicitly, since the sketch
above left it open:** the reconcile loop's third phase (below) **never
trusts or rewrites `SplitPlacing.target` after `CutoverSplit`'s initial
write**. It always **recomputes** `select_replicas` fresh, every tick,
exactly the way `reconcile_placement`/`rebalance_placement` already
recompute their own decisions fresh every tick rather than persisting and
trusting a prior one. Two reasons: (1) `target` is a snapshot of
membership as of the fork instant, and membership can change on every
later tick — trusting a stale value risks proposing a `CasTabletReplicas`
against candidates that are no longer `Active`, exactly the staleness bug
class `reconcile_placement`'s own statelessness already avoids by
construction; (2) persisting a *newly*-computed target (once a previously
`None` entry becomes satisfiable) would need a **second** new
`MetaCommand` purely to write it back, which this design deliberately
avoids — see the reuse finding immediately below. `SplitPlacing.target`
therefore stays exactly what `CutoverSplit` wrote, forever, as a
diagnostic record of "what cutover itself decided (or couldn't)" — an
operator's own answer to "why hasn't this child moved yet," never the
mechanism's own source of truth.

**The reconcile loop's third phase — the reuse finding, named
explicitly.** `crates/animus-control/src/node.rs`'s `reconcile_loop`
(~ln. 1460) already runs repair (`Metadata::reconcile`, unconditional
every tick) then, only if repair proposed nothing this tick and on its own
`REBALANCE_EVERY_N_TICKS` cadence, one balance-improving rebalance move
(`Metadata::rebalance`). This ADR adds a **third phase**, run
unconditionally every tick (own cadence, independent of repair/rebalance's
gating — a split-triggered relief obligation should not wait behind a
`REBALANCE_EVERY_N_TICKS` throttle meant for slow, cluster-wide balance
churn):

```rust
for (&child, entry) in &view.split_placing {
    if entry.done { continue; }
    let Some(t) = view.tablets.get(&child) else { continue }; // dropped table
    let Some(policy) = view.policies.get(&child) else { continue };
    let candidates = active_candidates(&view.members);
    if let Ok(wanted) = select_replicas(&candidates, policy)
        && wanted != t.replicas
    {
        propose(MetaCommand::CasTabletReplicas {
            tablet: child,
            expected_epoch: t.epoch,
            replicas: wanted,
        });
    }
    // Err(_) (still unsatisfiable): propose nothing, re-attempt next tick —
    // fork B's own keep-retrying stance, restated at the convergence site.
}
```

**This is the ADR's central mechanical claim, and its own name for it:
`reconfigure_step` + its production caller (`host.rs:1090`) already
converge live Raft membership to whatever `Metadata.tablets[t].replicas`
says — Placing needs no new movement primitive, only a new *source* of
`CasTabletReplicas` proposals feeding the exact same convergence loop
every other tablet's placement already rides.** No new `HostAction`, no
new `reconfigure_step` behavior, no new relay-allowlist entry (`CasTabletReplicas`
is not on `is_relayable_command`'s allowlist at all —
`crates/animus-node/src/wire.rs:950` — it is proposed **directly** by the
control-plane leader, exactly as `reconcile_placement`/`rebalance_placement`
already do).

**One ordering decision this phase needs, and its rationale**:
`rebalance_placement`'s own tablet-eligibility filter (`crates/
animus-control/src/meta.rs:1904`, the same `t.state != TabletState::Active`
skip it already applies) gains one more exclusion — a tablet carrying an
**un-done** `split_placing` entry is skipped by ordinary rebalance too.
Without this, a freshly-cutover child (already `Active`, already
policy-satisfying on its inherited-from-parent replicas, hence invisible
to `reconcile_placement`'s violation-driven repair) is exactly the kind of
imbalance `rebalance_placement`'s own balance-driven pass would eventually
notice on its own, slow `REBALANCE_EVERY_N_TICKS` cadence — competing with
the new, faster, directed Placing phase for the *same* tablet's epoch in
the *same* tick is wasted churn (a losing `CasTabletReplicas` simply
rejects on the epoch-CAS and both proposers retry — harmless by
construction, per the codebase's existing epoch-CAS discipline — but
avoidable). Once `done` flips (§3), the tablet rejoins the ordinary
`rebalance_placement` population like any other tablet — Placing owns a
child's relief exclusively only until it finishes, never longer.

**Repair-on-home-death falls out of the existing `replan` loop
unmodified.** If a replica named in a child's current `replicas` (whether
still the parent-inherited set, or already moved partway toward a Placing
target) goes `Down` before Placing converges, `Metadata::reconcile`'s
ordinary repair phase — which runs first, every tick, unconditionally —
already replaces it via `replan`'s survivor-preserving logic, exactly as
it would for any other tablet. Nothing about Placing needs to special-case
this; it is a genuine instance of the reuse finding above, not a gap.

### 3. Completion

**New command**: `MetaCommand::MarkSplitPlacingDone { tablet: TabletId,
expected_epoch: Epoch }` — epoch-CAS'd against the **child's** own current
epoch (so a stale confirm racing a *later* churn event on the same tablet
is rejected rather than marking done against state that has since moved
again), idempotent on an already-`done` entry (a re-propose from the
proposer's own retry is a harmless no-op — the `MarkIndexBackfilled`/
`RecordBackupTabletComplete` idiom exactly). Apply sets
`split_placing[tablet].done = true`; the row itself is never deleted (it
stays as a permanent, cheap, bounded-size record of "this child's
post-split placement finished," pruned only by `DropTableTablets`'s
existing cascade — see below).

**Where it is proposed, and a correction to the brief's own anchor.** The
brief for this ADR named "colocated with `spawn_reconfigure_loop`'s call
site" as the natural home. That call site does not exist in production:
`RaftKvNode::spawn_reconfigure_loop` (`crates/animus-cp-data/src/
lib.rs:3863`) has **zero callers anywhere in `animusd`** — the real,
production convergence driver is `host::Reconciler::execute`'s
`HostAction::Reconfigure` arm (`crates/animus-cp-data/src/host.rs:1090`),
called directly from the per-node tablet-host reconciler loop, never
through `spawn_reconfigure_loop`. `reconfigure_step`'s own "converged"
signal (`current == desired && learners.is_empty()`, the point at which it
returns `None`) is therefore only directly observable from *inside*
`animus-cp-data`'s own `RaftKvNode`, not from any call site the brief's
anchor would have reached — and proposing a control-plane `MetaCommand`
needs a `ClientCtx`/`ControlHandle`, which `animus-cp-data` has no
dependency on at all (mirroring `animus-placement`'s own "no reverse dep"
discipline: the data plane does not reach up into the control plane's
proposing surface).

**This ADR instead places the completion check where every other
"tablet-leader reports local convergence to the control-plane leader"
signal already lives: a new per-tablet, leader-gated `animusd`-level
background loop**, mirroring `backup_capture_loop`'s/`index_backfill`'s
own shape exactly (a loop that runs on every combined/data-only node,
self-gates per tablet on `group.is_leader()`, and proposes a relayable
completion command once it observes local convergence). Each tick, for
every tablet this node leads with an un-done `Metadata.split_placing`
entry:

```rust
if group.config() == BTreeSet::from_iter(t.replicas.iter().cloned())
    && group.learners().is_empty()
{
    propose(MetaCommand::MarkSplitPlacingDone {
        tablet: child,
        expected_epoch: t.epoch,
    });
}
```

— the identical convergence predicate `reconfigure_step`'s own early
return already checks, re-derived here from the two public accessors
Train 1 already exposes (`config()`/`learners()`), never a new one.
`MarkSplitPlacingDone` goes on `is_relayable_command`'s allowlist
(`crates/animus-node/src/wire.rs`, alongside `MarkIndexBackfilled`/
`SealStreamShard` — the same "a tablet leader's own completion report,
from wherever that leader actually runs" reasoning those two carry in
their own allowlist comments) — a tablet's leader is frequently not the
control-plane leader, so this must relay.

**Split-complete (admin/status, derived, NEVER a serving gate — fork A).**
A tablet reports "this in-place split is fully complete" as a **derived**
fact for observability only: `split_lineage` present for both children
(the existing fork F9 record, unchanged) **AND** (no `split_placing`
entries exist for either child, **OR** every existing entry has
`done: true`). Children serve `Active`, unconditionally, from the moment
`CutoverSplit` commits — exactly as ADR 0058 already established ("The
children are already fully formed, already durable [...] there is
nothing left for a veto to wait for"). Placing never gates serving,
routing, or anything else client-visible; it only gates when an operator
sees a green "split complete" pill versus an amber "still placing" one.

### 4. Explicitly out of scope / unaffected

- **`SplitMode::Copy` and `split_child_placement`** (the copy-based
  workflow's own fork F5) are entirely untouched. This ADR only changes
  the **in-place** path's placement timing; the copy-based workflow keeps
  minting `Building` children at placement-chosen final homes exactly as
  ADR 0050 designed, for as long as `--split-mode copy` remains
  selectable.
- **The #298-pinned soak** (`tests/streams_e2e.rs::
  multi_split_soak_streamed_gsi_table_under_mixed_load`, ADR 0058's own
  Open Forks table entry G5) is unaffected — this ADR never touches the
  copy path, and is not gated on that residual resolving.
- **`byte_weighted_median` and F11 token alignment** (ADR 0042 §14) are
  unaffected — this ADR changes *where a child ends up*, never *where the
  parent's range is cut*.
- **The 50ms-cadence (`INPLACE_SPLIT_RECONCILE_INTERVAL`) and 250ms-settle
  (`INPLACE_SPLIT_MATERIALIZE_SETTLE_MS`) cutover guards** (ADR 0058's own
  rung-3 as-built fix) are unaffected and remain load-bearing exactly as
  shipped. That fix closes a **materialize-vs-cutover race**: the
  in-place cutover driver (`inplace_split_driver_tick`) must not propose
  `CutoverSplit` before *every* fork participant's own reconciler has had
  a chance to materialize both children locally, or a slow replica hosts
  the freshly-`Active` child via the wrong (non-split) path with
  permanent, silent data loss. This ADR's own reorder is **orthogonal**:
  it changes what happens *after* `CutoverSplit` already ran (which
  replica set a child converges *toward*), never anything about *whether*
  or *when* `CutoverSplit` is safe to propose in the first place — the
  materialize-vs-cutover race is entirely a Stage 3/4 property this design
  inherits unmodified.
- **Companion decisions referenced, not decided, here** (separate
  ADR-adjacent follow-up work): a range-aware clone at the SSTable-clone
  step (ADR 0058's own open fork G2 — filtering an `InstallSnapshot`/clone
  by the eventual target range rather than shipping the whole parent range
  regardless of destination — this ADR narrows G2's *impact*, since a
  fork-first child now clones onto the parent's own existing homes, where
  every replica already has the whole range anyway, but does not close
  G2's own copy-based-analog case); removing the key-count auto-split
  trigger (unrelated surface, referenced only because it shares the
  `--auto-split`-family CLI flags this ADR does not touch).

  **The clone half is since closed (2026-08-31, `LsmEngine::
  clone_to_filtered`, ADR 0058's own rung-2 amendment)**: the fork's own
  clone step (`EngineFactory::clone_engine`) now does whole-file
  assignment — a source SSTable wholly outside the target's own keep-set
  is never linked in at all, rather than shipped whole and trimmed after.
  This closes the *clone-step* half of G2 unconditionally (independent of
  whether a child's eventual home is disjoint from the parent's — it helps
  the fork-first case described here too, since even a same-home clone
  benefits from not linking a wholly-sibling table it will only delete a
  moment later via `trim_split_child`). **What stays open**: a genuinely
  range-filtered `InstallSnapshot` **wire format** — a straddling table is
  still linked (and shipped) whole, and Placing's own post-cutover learner
  catch-up (the "child-range-only, not whole-parent-range" property two
  bullets below) was already true before this closure and is unaffected by
  it either way.

### Rationale — why fork-first / late-binding, and what is given up

**The structural argument.** Total data moved across a split's whole
lifetime is unchanged from F5's own accounting — every byte still has to
land on a child's eventual final home exactly once, whether that
happens *before* the fork (F5) or *after* it (this design). What changes
is *when* the placement decision is made and *what has to wait on it*:

- **The fork instant is decoupled from cluster size or topology.** F5's
  fork can only proceed once every one of its (possibly disjoint, possibly
  off-node) recruited homes has caught up via a whole-parent-range
  `InstallSnapshot` — Stage 1/2's own catch-up gate. Fork-first has no
  such gate at all: every replica the fork touches is already a member of
  the parent's own live group, so there is nothing to wait for between
  `BeginSplitInPlace`'s commit and the fork itself beyond ordinary Raft
  agreement on the `SplitTablet` entry — the fork proceeds at the same
  speed regardless of how large or spread the cluster is.
- **Learner snapshots, when they do happen, are child-range-only, not
  whole-parent-range.** Every learner add that Placing's own convergence
  loop drives (via `reconfigure_step`'s Train-1 sequencing, unmodified)
  catches a replica up to **one child's own already-narrowed range** — the
  same shape any ordinary post-split rebalance move already has — never
  the whole, pre-split parent range Stage 1/2's fused design forced (ADR
  0058's own named 2× cost).
- **The placement decision is made fresh at cutover, never aged through a
  catch-up window.** F5 picks a child's final home at `BeginSplitInPlace`
  propose time — before any learner has caught up, sometimes minutes
  before the fork actually happens on a loaded cluster — so cluster state
  (membership, load) can drift stale between the decision and its
  execution. This design decides at `CutoverSplit`'s own apply, off
  whatever `Metadata` state is agreed at that exact commit — as fresh as a
  placement decision can be, by construction (fork C).
- **No node outside the parent's own current group can stall the fork.**
  F5's Stage 1/2 makes the fork's own liveness depend on however many
  external, possibly-unrelated nodes placement happened to recruit — one
  slow or partitioned recruit blocks the whole fork. Fork-first's fork
  depends on nothing outside the parent's own already-live voter set, the
  same set that already has to be healthy for the parent to serve at all.
- **The completion envelope preserves F5's bounded-relief guarantee.** F5
  never leaves a split's job half-done — a child's relief (a hot tablet's
  own halving of load) is real the instant it activates, at its already-
  final home. This design's own completion signal (§3, "split complete
  only when placed") is the direct analog: a split is not reported done
  until its children are both forked *and* placed, so the *reported*
  outcome still carries the same "fully relieved, not just fully forked"
  guarantee F5 gave — only the *window* in which that guarantee is being
  driven-to shifts from before activation (F5) to after it (this design).
- **Directed targets are immune to `rebalance_step`'s own domain-guard
  blocking.** `rebalance_step` (ADR 0029) can legally make zero progress
  forever under a strict `SpreadPolicy` that blocks every improving move
  (documented, accepted, in `animus-placement`'s own property-test suite —
  "a strict or best-effort spread constraint can legally block every
  improving move on every eligible tablet"). Placing's own convergence
  never goes through `rebalance_step` at all — it computes a target
  directly via `select_replicas` (the same primitive `BeginBackup`/fresh
  placement already use) and drives straight to it via `CasTabletReplicas`,
  so a split child's own relief is never at the mercy of the generic
  balancer's own domain-guard stalemate.

**What is given up, honestly.** There is a genuine window — bounded by the
reconcile loop's own tick cadence plus whatever catch-up time a move
needs — between `CutoverSplit` and Placing-converged, during which **both
children still sit on the parent's own original nodes**. Under F5, by
contrast, a child is *born* on its final home; there is no such window at
all (its cost is paid earlier, during the fork's own catch-up gate,
instead). This is accepted because relief is still **driven to
completion** exactly as before — just after activation rather than before
it — and because the window this design opens is bounded by the same
convergence machinery (`reconfigure_step` + Train 1's learner phase) that
already bounds every other tablet's placement convergence in this
codebase; it is not a new, unbounded, or unmonitored kind of staleness.

## Alternatives considered and rejected

**1. Keep F5, only shrink Stage 1/2's own learner-add window (e.g. cap how
many homes may be recruited, or parallelize the catch-up more
aggressively).** Rejected: this treats the symptom (learner-catch-up
latency) without removing the actual structural hazard — any external node
still has to be recruited and caught up before the fork can proceed at
all, so a single slow or partitioned recruit still stalls the whole split
regardless of how the catch-up itself is tuned. It also does nothing about
the permanent `host::plan` phase-1 host-candidacy branch or the
`bootstrap_voters` superset — both survive unchanged. This is a tuning
knob on a design this ADR argues should not exist in this shape at all.

**2. Persist and trust a live-updated `SplitPlacing.target`, via a second
new command, instead of always recomputing fresh in the reconcile
loop.** Considered and rejected as part of designing §2's own
representation (fork B's companion decision): a persisted, caller-trusted
target needs its own write path (a second new `MetaCommand`, purely to
transition `target` from `None` to `Some` once satisfiable, or to update
it as membership shifts) with no benefit `select_replicas`'s own
recomputation doesn't already provide for free — `reconcile_placement`/
`rebalance_placement` already prove that recomputing a placement decision
fresh, every tick, off already-replicated state is cheap, pure, and
race-free by construction. Persisting a second, potentially-stale source
of truth purely to avoid a cheap recompute is strictly worse.

**3. Gate a child's serving on Placing being `done` (require the target
placement before a child ever answers a client read/write).** Rejected
outright (fork A): ADR 0058's whole reason for existing is that a fork's
children are "already fully formed, already durable" the instant the
`SplitTablet` entry applies — inventing a NEW serving gate that depends on
a completely separate, slower, best-effort background process (cluster
placement convergence, which can legitimately take longer than a fork
itself under a strained or degraded cluster) would reintroduce exactly the
kind of availability regression Train 2's whole design was built to avoid,
for a property (relief has fully landed on the *ideal* nodes) that has
nothing to do with correctness.

**4. Have the control-plane leader itself drive the fork's placement
decision proactively — a background sweep over every `Splitting` tablet,
rather than computing it once inline inside `CutoverSplit`'s own apply.**
Rejected: this duplicates exactly the "compute at apply, from agreed
state" discipline `BeginBackup` already established as the right shape
for this class of decision (fork C) — a background sweep would need its
own separate proposing mechanism (a new command just to *record* the
computed target before Placing's convergence phase could act on it),
reintroducing Alternative 2's rejected second write path by a different
route.

## Consequences / risks (honest)

- **A window of relief-not-yet-realized is now structurally normal**,
  visible via `split_placing`'s un-`done` entries and the derived
  split-complete diagnostic (§3). An operator watching a busy cluster
  split will, for the first time, routinely see a child reported "forked,
  not yet placed" — a new, legitimate, non-error state the dashboard/admin
  surface needs a pill for (mirroring the existing neutral "quiesced"/
  "forming" pills, per ADR 0021 §7's "cluster health means is the data at
  risk, not is anything in transition" rule — a not-yet-placed child is
  never a health/data-risk signal on its own).
- **Placing's own convergence competes for reconcile-loop cycles with
  ordinary repair and rebalance**, on every tick, for as long as any
  `split_placing` entry stays un-`done`. Bounded by construction (at most
  one `CasTabletReplicas` per un-done entry per tick, mirroring
  `rebalance_placement`'s own one-move-per-tick discipline), but a cluster
  mid-split-storm (many tablets splitting concurrently, e.g. from a bulk
  load) now has more concurrent Placing obligations than it previously had
  concurrent Stage-1-learner-catch-up obligations, and this shift in *when*
  the work happens (after cutover, in the control-plane leader's own
  hot reconcile path, rather than before it, distributed across each
  parent's own leader) has not been measured under load — a genuine open
  question for the bench work in Sequencing below.
- **The rebalance-exclusion ordering rule (§2) is new coupling** between
  `rebalance_placement` and `split_placing` — a future change to either
  function's own eligibility filter has to remember the other exists. This
  is the same class of coupling `reconcile_placement`/`rebalance_placement`
  already share (both independently skip non-`Active` tablets today), so
  it is not a new *kind* of hazard, just one more instance of it to keep
  in sync.
- **The learner-union residual (fork D)** is deliberately accepted as
  self-healing rather than closed — see §1's own paragraph. Should a
  future audit find this residual firing often enough in practice to
  matter (an unusually high rate of rebalance-races-split on the same
  tablet), the fix is narrow (special-case inheriting an in-flight
  learner at the `SplitTablet` apply, if it happens to already be a member
  of one child's own eventual Placing target) — not attempted here,
  per this ADR's own "decide only what's needed" scope.
- **`Metadata::split_placing` is one more replicated collection every
  future `Metadata`-touching change (`apply_key_write`'s exhaustive
  match, `mirror.rs`'s own no-wildcard-arm apply-derivation, a new
  `syskv::EntityKind::SplitPlacing`) has to keep in sync** — the same
  "grep every gating match site" discipline every prior `Metadata`
  collection addition in this repo already pays (ADR 0059's own
  `Metadata::backups`/`backup_tablet_progress` addition is the most
  recent precedent). `DropTableTablets`'s existing cascade needs the
  identical prune `MarkIndexBackfilled`'s own doc describes for
  `index_backfill` — a `split_placing` row for a tablet no longer in the
  live tablet map (the table was dropped) is an orphan the drop cascade
  must sweep, mirroring that precedent exactly rather than inventing a
  new cleanup shape.

## Sequencing (rungs)

Design-only as of this ADR; no rung has landed. A plausible bottom-up
order, following this repo's own red→green-per-cell convention:

1. **`Metadata::split_placing` + `MetaCommand::MarkSplitPlacingDone`** in
   `animus-control`: the new collection, its mirror/syskv plumbing, the
   command's apply arm (epoch-CAS, idempotent-on-already-`done`), and
   `CutoverSplit`'s in-place branch gaining the `select_replicas` call —
   proven at the pure `Metadata::apply` level first (unit tests mirroring
   `cutover_split_in_place_*`'s own shape), independent of any driver
   change.
2. **Delete Stage 1/2**: `HostAction::AddSplitLearner`, `host::plan`'s
   phase-1.5 learner loop and phase-1's recruited-host branch, and the
   `bootstrap_voters` learner-union drop, in `animus-cp-data` — proven
   against the existing `inplace_split_reconciler.rs` corpus first
   (`ANIMUS_INPLACE_SPLIT_SEEDS`), which needs its own scenario audit (see
   Testing plan below).
3. **`trigger_split`'s `InPlace` arm** stops calling `split_child_placement`
   and instead passes the parent's own current replicas for both children
   — `animusd`.
4. **The reconcile loop's third phase** (`animus-control::node::
   reconcile_loop`) plus `rebalance_placement`'s new exclusion — proven
   against a new placement-focused corpus (below) before wiring into the
   live driver loop.
5. **The completion loop** (`animusd`, the new per-tablet leader-gated
   background task, `is_relayable_command`'s new allowlist entry) —
   proven end-to-end against a real multi-node `ProdEnv` cluster.
6. **Delete `SplitChild`'s F5 doc language**, `HostAction::
   AddSplitLearner`'s doc, and every reference to Stage 1/2 in ADR 0058
   itself and `animus-cp-data/CLAUDE.md`'s "In-place split" section — a
   documentation-only closing rung, mirroring how ADR 0050's own "what
   this deletes" discipline is applied whenever a prior design's mechanism
   is actually removed.

## Testing plan

**Corpus disposition** (`ANIMUS_INPLACE_SPLIT_SEEDS`,
`tests/inplace_split_reconciler.rs`, `animus-cp-data`):

- `leader_crash_mid_catch_up` (a Stage-2-specific scenario, proving a
  leader crash while a recruited learner is still catching up) **dies** —
  there is no Stage 2 left to crash mid-catch-up.
- The happy-path scenario is **rewritten**: no learner-catch-up phase to
  assert on; instead asserts the fork happens immediately once
  `BeginSplitInPlace` commits (no gating condition at all beyond ordinary
  Raft agreement on the fork entry), and that both children bootstrap on
  exactly the parent's own current voter set (no over-replication, no
  Stage-5 trim needed).
- G4's crash-idempotency cells, the campaign/eager-wake cells (rung 4/
  rung-4-layer-1), and the concurrent-unrelated-rebalance-races-the-split
  cell are **unchanged** — none of them depend on Stage 1/2's own
  mechanics, only on Stage 3's mint/materialize path, which this ADR does
  not touch.

**Five new Placing fault cells**, a new corpus section (either a new depth
knob, e.g. `ANIMUS_SPLIT_PLACING_SEEDS`, or folded into the existing
`ANIMUS_INPLACE_SPLIT_SEEDS` corpus if scoping finds the fixture reuse
outweighs a dedicated knob — a call for the implementing rung, not decided
here):

1. **Home-dies-mid-move**: a `split_placing` target names a node that
   goes `Down` after the reconcile loop has already proposed the first
   `CasTabletReplicas` toward it but before convergence — proves the
   ordinary `reconcile_placement` repair phase (unmodified) picks up the
   replacement and Placing's own retry loop still eventually converges and
   marks `done`.
2. **Control-failover-mid-placing**: a control-plane leadership change
   while a child has an un-done `split_placing` entry — proves the new
   leader's own `reconcile_loop` picks the third phase back up from
   durable `Metadata` state with no lost obligation (mirroring the
   existing `learner_move_survives_leader_change_mid_move`-style
   assertion shape).
3. **Crash-between-cutover-and-assignment**: a crash of the proposing
   control-plane node between `CutoverSplit`'s commit and its own apply
   fully landing `split_placing` — proves the entry is either fully
   present (post-crash apply completes) or the whole `CutoverSplit`
   command itself never committed (nothing partially written), per the
   usual "durable-before-visible" discipline this codebase enforces
   everywhere else.
4. **Unsatisfiable-at-cutover-retries**: `select_replicas` errs at
   `CutoverSplit` time (too few `Active` candidates), then candidates
   recover on a later tick — proves a `target: None` entry is re-attempted
   and eventually converges once satisfiable, with no special-cased
   recovery path (fork B's own "keep retrying" stance, proven live).
5. **Spread-policy-target-converges**: a child inherits a policy with a
   `SpreadPolicy` set — proves `select_replicas`'s own strict/best-effort
   spread handling (already proven correct in isolation by
   `animus-placement`'s own property suite) produces a target Placing's
   convergence loop actually reaches, distinguishing this path from
   `rebalance_step`'s own documented (and here irrelevant) domain-guard
   stall.

**End-to-end**:

- `tests/inplace_split_e2e.rs` (`animusd`) reruns against the new default
  behavior — the paced-continuous-writer assertion (every acked write
  survives, retry count) should be unaffected, since this ADR changes
  nothing about the fork's own write-availability contract; what changes
  is a **new** assertion this rerun needs to add: after cutover, poll for
  both children's `split_placing` entries (if any) to reach `done`, and
  assert the final converged replica sets are exactly what a fresh
  `select_replicas` computation over the test cluster's own membership/
  policy would produce.
- A **new** multi-node e2e, `placing_relocates_data` (or similar): a
  cluster wide enough that the parent's own homes are provably *not*
  where a fresh split's children *should* end up (e.g. an unbalanced
  cluster with headroom elsewhere), proving the whole fork→cutover→Placing
  pipeline actually relocates a child's data off the parent's original
  nodes and onto its computed target, not just that the bookkeeping
  converges.
- **Bench re-validation**: `inplace_split_bench.rs`'s existing
  fork-to-first-child-Active / write-blip measurement should be **faster
  to first-child-Active** under this design (no Stage 1/2 catch-up gate
  at all) — re-run and publish fresh numbers, following this repo's own
  same-host-same-session comparison discipline, as a future as-built
  amendment to this ADR once implemented; **not** predicted or asserted
  here.

## Relationship to ADR 0050/0058's fork F5

F5 (ADR 0050) was decided once, for the copy-based workflow, on the
premise that a split's data movement and its placement decision are one
and the same event — true when the movement mechanism is a bespoke copy
driver that has to ship bytes to *somewhere* regardless. ADR 0058 Train 2
inherited F5 verbatim into the in-place design without re-examining that
premise, even though Stage 3's own mechanism (a local SSTable clone,
free of any network cost, onto whichever replicas already host the
parent) makes "movement" and "placement" genuinely separable for the
first time: a fork-first child costs nothing extra to bootstrap on the
parent's own current nodes, and *then* moving it — if it needs to move at
all — is ordinary, already-existing, already-proven replica-rebalancing
machinery. This ADR is the re-examination F5's move into the in-place
design never got: the layout argument that justified fusing movement with
placement for a *copy*-based split does not hold for a *fork*-based one,
the same shape of finding ADR 0058 itself made about ADR 0028's
shared-engine layout no longer constraining ADR 0050's private-engine one.

## Amendment (2026-08-31): rung 7 — e2e + bench re-validation, as-built

**Rungs implemented.** All seven of this ADR's Sequencing rungs landed:

| Commit | What |
|---|---|
| `6c43372` | This ADR, accepted in design |
| `a26de6c` | `animus-control`: `Metadata::split_placing` + `MarkSplitPlacingDone` (Sequencing rung 1) |
| `001c6b4` | `animus-control`: `reconcile_loop` split-placing phase + `rebalance_placement` exclusion (rung 4) |
| `2d2f8ba` | `animusd`/`animus-cp-data`: fork-first — children inherit parent replicas (rungs 2–3) |
| `de919df` | `animus-cp-data`: delete Stage 1/2 learner-recruitment machinery, fork-first corpus (rung 2, "delete Stage 1/2") |
| `2a2e89c` | `animusd`: split-placing completion loop + status surface (rung 5) |
| `645fbb5` | `animusd`: harden split-placing completion e2e (rung 5 follow-up — the settle-window fix, below) |

(Rung 6, the documentation-only closing sweep of ADR 0058's own F5/Stage
1-2 language, is folded into this amendment and the `crates/animusd/
CLAUDE.md`/`crates/animus-cp-data/CLAUDE.md` updates that shipped
alongside the commits above, rather than a separate commit.)

**The settle-window product bug found and fixed landing rung 5** (the
completion loop, `animusd::split_placing_completion`): a first, literal
implementation of this ADR's own §3 pseudocode marked a child's Placing
`done` the instant `group.config() == t.replicas`, which is **trivially
true on the very first post-cutover tick** (a fork-first child is born
already sitting on that value) — before the control-plane's own
`reconcile_loop` (§2's third phase) had a single chance to move it toward
its real, differing `split_placing[child].target`. The fix requires the
converged observation to hold continuously for a settle window
(`SPLIT_PLACING_DONE_SETTLE`) before trusting it, mirroring the identical
class of fix ADR 0058's own in-place cutover driver already used
(`INPLACE_SPLIT_MATERIALIZE_SETTLE_MS`) against an analogous race. Full
incident writeup, including the follow-up hardening of the test itself
(a one-shot assert on an eventually-converging value, fixed in `645fbb5`):
`docs/engineering-lessons.md`'s "A 'just compare live state to the target'
convergence check races the very proposer that sets the target" entry
(filed under this ADR's rung 6, and covering both the product race and its
test-hardening follow-up in one place).

**Issue #513, investigated and closed as not reproducible (2026-08-31
amendment).** Validating the fix above surfaced what looked like a second,
**pre-existing**, unrelated defect in `reconfigure_step` itself (ADR 0058
Train 1, unmodified by this ADR): driving live Raft membership toward a
target that replaces **two** of three replicas at once appeared to
oscillate indefinitely (reach the transient over-replicated 5-voter
intermediate state, then partially revert, repeating) rather than
converge — confirmed unrelated to this ADR's own completion loop
(reproduced with zero `MarkSplitPlacingDone` proposes fired). Filed as
[issue #513](https://github.com/animus-db/animus-db/issues/513).

A dedicated re-investigation could not reproduce the oscillation, across:
five `SimEnv` harness shapes of increasing fidelity (direct
`reconfigure_step` polling; `spawn_reconfigure_loop` with a shared target;
`spawn_reconfigure_loop` with each group member independently polling its
own control-plane replica; the real `host::Reconciler` driven uniformly;
all swept across dozens of seeds — `crates/animus-cp-data/tests/
reconfigure_multi_replica_diff.rs`, 60 seeds); and 30+ consecutive runs of
a real multi-threaded `ProdEnv` end-to-end test that reproduces the
ORIGINAL rung-6 recipe exactly — grow a 3-node cluster by two
lower-sorting-id nodes so a fresh `select_replicas` prefers both over two
of the parent's three, then drive a real in-place split's directed-Placing
target through it (`crates/animusd/tests/
split_placing_two_replica_diff_e2e.rs`), including several runs where the
tablet's own leader genuinely transfers mid-sequence via
`reconfigure_step`'s own step-6 self-removal case (one of the suspects
named when #513 was filed). Every run passes through the genuinely
over-replicated 5-voter intermediate the issue names, then shrinks
monotonically to the target with **no reversion observed**.

The most likely explanation, based on this investigation's own repeated
experience building repro harnesses before catching it: a convergence
check that includes an ALREADY-REMOVED replica's own `config()`/`voters`
snapshot. A replica excluded from a group's voter set stops receiving
`AppendEntries` the instant it's excluded, so its own locally-cached
config freezes at whatever it last observed — comparing that frozen value
against a live, still-converging replica's value can look exactly like a
"revert" to an observer that doesn't realize the two readings came from
different points in the sequence, or from a replica no longer part of the
group at all. See `docs/engineering-lessons.md`'s entry for the full
writeup. **This means directed Placing's own reach was never actually
narrower than "whatever `select_replicas` computes"** — no further change
is needed for a two-(or-more)-replica-difference target (plausible on a
freshly-grown or rebalancing cluster) to relocate a child correctly.
`tests/split_placing_completion.rs` keeps its one-replica shape (simpler,
and still a fully sufficient proof of this rung's own completion-loop
requirement); the two-replica shape has its own dedicated e2e instead of
folding a second, unrelated concern into that file's assertions.

Closing this out as "investigated, not reproduced" rather than "fixed" is
deliberate: no code in `reconfigure_step` or its callers changed, because
none of this investigation's evidence pointed at a concrete defect to
change. If the oscillation is ever genuinely observed again (or was real
under conditions this investigation didn't hit), the two regression tests
above are the first things that should catch it, and are a name and a
reproduction recipe for whoever picks this back up.

**E2e re-validation.** `cargo test -p animusd --test inplace_split_e2e`
(both tests — the paced-continuous-writer fork/cutover test and the
streams-shard-lineage test) run 3 times as independent invocations: **3/3
green**, no flake, matching ADR 0058's own rung-8/rung-4 soak precedent
for this file. `cargo test -p animusd --test split_placing_completion`
(the rung-5-hardened binary) reran once: **2/2 tests green**
(`placing_relocates_a_child_off_the_parents_original_nodes_and_the_
completion_loop_marks_it_done`, `mark_split_placing_done_tolerates_a_
stale_or_duplicate_relayed_propose`).

**Bench, both configurations, same host, same session.** Per this ADR's
own Testing plan ("re-run and publish fresh numbers... as a future
as-built amendment"), `animusd/tests/inplace_split_bench.rs`'s existing
bench (byte-for-byte the same workload `split_build.rs`'s copy-based bench
and ADR 0058's own rung-8 in-place bench use: 2,000 rows, 256-byte values,
3 nodes, RF 3) was run 3× against this branch's HEAD (`645fbb5`,
fork-first) and 3× against a worktree checked out at `6d2777d` — the
commit immediately preceding this ADR (`6c43372`), i.e. ADR 0058's own
Stage 1/2 F5-learner-recruitment in-place design — on this same host, back
to back, in this same session:

| | fork-first (this ADR), run1/run2/run3 | pre-ADR-0062 baseline (`6d2777d`), run1/run2/run3 | fork-first median | baseline median |
|---|---|---|---|---|
| fork-to-children-Active wall clock | 1.347s / 1.568s / 1.152s | 1.149s / 1.202s / 1.108s | **1.347s** | **1.149s** |
| write blip (max PUT) | 616.1ms / 840.7ms / 476.6ms | 418.8ms / 473.5ms / 371.8ms | **616.1ms** | **418.8ms** |
| put retries needed | 0 / 0 / 0 | 0 / 0 / 0 | **0** | **0** |

**This bench does not show the improvement this ADR's own structural
argument predicts, and that is reported honestly rather than reconciled
away — with a specific, checkable reason why.** The bench this ADR's
Testing plan names is deliberately identical in shape to ADR 0058's own
rung-8 bench: **3 nodes, RF 3**. At that scale, `select_replicas`/
`select_replicas_balanced` has no node outside the parent's own current
3-member replica set to ever recruit — both children's placement-chosen
"final home" and the parent's own current replicas are, of structural
necessity, the identical set. That is exactly the precondition under
which this ADR's own central claim (§ Rationale: "the fork instant is
decoupled from cluster size or topology... F5's fork can only proceed
once every one of its (possibly disjoint, possibly off-node) recruited
homes has caught up") has **nothing to demonstrate** — the pre-ADR-0062
baseline's own Stage 1/2 learner-recruitment-and-catch-up window is
recruiting learners that are already existing, already-caught-up voters,
so it pays none of the cross-cluster `InstallSnapshot` cost the ADR's
Context section documents as F5's actual cost driver. Reproducing that
win would need a bench cluster wider than RF (the same shape `tests/
split_placing_completion.rs`'s own e2e already uses to prove Placing
relocates data at all) — out of scope for this rung, which re-runs the
ADR's own named bench rather than authoring a new one.

Within that acknowledged limitation, the two honest findings from the
numbers actually measured:

- **Fork-to-Active wall clock is marginally higher under fork-first**
  (1.347s vs. 1.149s median, ~17% up) — consistent with a fork-first
  child briefly running the ordinary `reconfigure_step` convergence path
  its own directed-Placing phase feeds (a same-set `CasTabletReplicas`/
  no-op check every `reconcile_loop` tick for the duration any
  `split_placing` entry stays un-`done`) that the pre-ADR-0062 baseline's
  design never runs at all, on top of near-identical fork/bootstrap costs
  at N=3. Not confirmed by profiling — flagged as a plausible explanation,
  not investigated further (out of scope for a bench-and-report rung).
- **Write blip is higher under fork-first too** (616ms vs. 419ms median,
  ~47% up), with **zero retries in every run of both configurations** —
  the same "single slow request, not a refuse-and-retry pattern" shape
  ADR 0058's own rung-3→rung-4 write-blip investigation already
  characterized for this bench, so the elevated number here is plausibly
  the same `cp_route` election-wait/first-materialization cost that
  investigation chased, not a new fork-first-specific mechanism — again
  not chased further here.

**Caveats, stated plainly**: this is a **shared, contended host**
(observed load average ≈2.0 across 4 vCPUs while these runs executed) —
none of the individual numbers above should be read as a precise,
reproducible constant, only as same-session, same-host, directionally
comparable figures per this repo's own bench-comparison discipline. N=3
per configuration is the minimum this rung's instructions called for, not
a statistically powered sample — the run-to-run spread within each
configuration (fork-first's write blip alone spans 476.6ms–840.7ms) is
comparable in magnitude to the between-configuration delta the table
reports. Combined with the structural RF=3 ceiling above, this bench run
neither confirms nor refutes the ADR's core wall-clock claim; it
positively rules out a regression in write availability (zero retries,
both configurations, every run) and leaves the fork-to-Active/write-blip
comparison as a genuinely open question a wider-than-RF bench would need
to answer.

## Amendment (2026-09-01): cluster>RF bench — the wider-than-RF question, answered

The rung-7 amendment above named the gap plainly and left it open: the
named bench is structurally RF=3-at-3-nodes, so it cannot show the
decoupled-movement-from-placement claim doing any work, and "reproducing
that win would need a bench cluster wider than RF... out of scope for this
rung." This amendment is that bench, run fresh on this same host in one
session, both against current `HEAD` (fork-first, `39339c3`, which now
also includes the range-aware `clone_to_filtered` fork commit `c1874fc`
that landed after rung 7) and against a second worktree at `6d2777d`
(the pre-ADR-0062 F5-fused baseline, same commit rung 7 used). Two
separate measurements: a fresh 3-node re-run of rung 7's own bench (does
the range-aware clone change anything at RF=3), and the new cluster>RF
bench (`tests/cluster_gt_rf_split_bench.rs` on the fork-first tree,
`tests/cluster_gt_rf_split_bench_f5.rs` — uncommitted, throwaway — on the
baseline worktree).

**Caveats up front, unchanged from rung 7 and worth repeating**: this is a
shared, contended 4-vCPU host (background load observed throughout these
runs); N=3 per configuration is the floor this task's own instructions
called for, not a statistically powered sample; every number below is
same-host/same-session only, never comparable to a figure quoted from a
different run.

### 3-node re-run: does the range-aware clone change anything?

Byte-for-byte the same bench and workload as rung 7 (`inplace_split_bench.rs`,
2,000 rows, 256-byte values, 3 nodes, RF 3), 3 runs per configuration,
interleaved:

| | fork-first (`HEAD`), run1/run2/run3 | baseline (`6d2777d`), run1/run2/run3 | fork-first median | baseline median |
|---|---|---|---|---|
| fork-to-children-Active wall clock | 1.302s / 1.041s / 1.537s | 1.315s / 1.334s / 0.994s | **1.302s** | **1.315s** |
| write blip (max PUT) | 526.6ms / 315.4ms / 782.7ms | 582.3ms / 631.7ms / 267.3ms | **526.6ms** | **582.3ms** |
| put retries needed | 0 / 0 / 0 | 0 / 0 / 0 | **0** | **0** |

Rung 7's own numbers (context, not a baseline — a different session,
quoted only for direction, not magnitude) showed fork-first materially
**slower** at this shape: 1.347s/616ms median vs. baseline's 1.149s/419ms
(~17% and ~47% up respectively). Today's fresh numbers show the opposite
sign at the median — fork-first fractionally **faster** (1.302s vs.
1.315s) and with a **lower** median write blip (526.6ms vs. 582.3ms) —
though the run-to-run spread within each configuration (fork-first's own
cutover spans 1.041s–1.537s; its blip spans 315ms–783ms) is comparable in
magnitude to the between-configuration delta either session reports, so
neither session's median difference is a reliable signal on its own. The
honest reading: **the RF=3 gap rung 7 reported has closed** — today's two
medians sit within each other's own noise band, where rung 7's did not —
consistent with, though not proof of, the range-aware
`clone_to_filtered` commit (`c1874fc`, landed after rung 7, closing ADR
0058's own "full clone then trim" G2 deferral) removing a real per-fork
cost fork-first was paying at rung 7 that the baseline's copy-free
in-place design never had to pay. This bench cannot isolate that commit's
effect from ordinary session-to-session noise — it was not re-run with and
without `c1874fc` in isolation — so this is reported as a plausible
explanation, not a confirmed one, in the same spirit as rung 7's own
"not confirmed by profiling" notes.

### cluster>RF bench: the wider-than-RF measurement

`tests/cluster_gt_rf_split_bench.rs` (fork-first tree, committed) and its
close copy `tests/cluster_gt_rf_split_bench_f5.rs` (baseline worktree,
uncommitted throwaway scaffolding) grow a 3-node RF=3 cluster by one node
(`m0`, sorting lexically below every original node's id) immediately
before kickoff — the identical recipe `tests/split_placing_completion.rs`
uses to force a *real* placement move (`select_replicas` now prefers `m0`
over one of the parent's own three) rather than a vacuous already-placed
fork. A paced continuous writer (retry-counting `put`, same shape as the
3-node bench) runs throughout. Three clocks on the fork-first tree: (a)
kickoff → children Active (cutover/relief), (b) kickoff → directed-Placing
fully converged (every child's `split_placing` entry `done`), (c) max
write blip across the whole a→b window. The baseline has no separate (b) —
under F5 the recruited learner is caught up and promoted to voter *before*
the fork ever proposes, so cutover already IS full placement — so it
measures only (a′) kickoff → cutover and (c′) max write blip.

| | fork-first, run1/run2/run3 | fork-first median | F5 baseline, run1/run2/run3 | F5 baseline median |
|---|---|---|---|---|
| (a)/(a′) kickoff → relief | 1.656s / 1.441s / 1.477s | **1.477s** | DNC / DNC / DNC (>300s every run) | **did not converge** |
| (b) kickoff → fully placed | 4.456s / DNC(>240s) / DNC(>240s) | **converged 1/3 runs** | n/a (a′ already is full placement, when it happens at all) | n/a |
| (c)/(c′) max write blip | 1.422s / 1.200s / 1.336s | **1.336s** | 169.6ms / 300.9ms / 339.2ms | 300.9ms (not a real "blip" — see below) |
| put retries needed | 0 / 0 / 0 | **0** | 0 / 0 / 0 | **0** |

**The dual comparison, stated plainly, per this task's own instructions —
do not collapse it:**

- **Fork-first vs. baseline (a) vs. (a′), time to relief**: fork-first
  wins decisively and reliably — **1.3–1.7s in every run**, regardless of
  the sustained write load hitting the same tablet the whole time.
  The F5 baseline **did not complete its split within a 5-minute budget in
  any of the 3 runs** — not "slower," but observed to make **zero commit
  progress** on the recruited learner's catch-up for the entire
  measurement window in all three (see below). This is the sharpest,
  most direct confirmation this ADR's bench program has produced of the
  design's own central claim: decoupling the fork from the placement
  decision means a splitting tablet's own write load cannot block the
  split from relieving it, structurally, where the fused F5 design's own
  "the fork can only proceed once every recruited home has caught up" gate
  is exactly the mechanism a sustained write stream can starve.
- **Fork-first (a) vs. (b), time to relief vs. time to fully placed**:
  these are genuinely different numbers, and conflating them is exactly
  what this task's instructions warned against. Relief is fast and
  reliable (above). Full convergence to the directed-Placing target is
  **not** reliably fast under this same load: it completed in 4.456s in
  run 1, and did **not** complete within a 240s budget in runs 2 and 3.
  Inspecting `/admin/status` at the end of both non-converged runs shows a
  specific, reproducible shape: one child (`done: true`) had already
  drifted off its own recorded target via a later, independent, legitimate
  rebalance move (expected — ADR 0062 §2's own "once `done`, the tablet
  rejoins ordinary rebalance's eligible population" rule; not a defect);
  the **other** child sat with its live `replicas` already **exactly
  matching** its own `split_placing` target — the completion loop's own
  convergence predicate should therefore be satisfied — yet `done` stayed
  `false` for the balance of a 240-second window. That is a genuinely
  unexpected, reproducible (2 of 3 runs) shape this session did not
  root-cause (out of scope for a bench-and-report task, per this task's
  own instructions) — flagged here as a real, open finding rather than
  smoothed over: **fork-first's own advertised "relief," not "fully
  placed"**, is the number this bench actually delivers reliably at this
  scale under load; "fully placed" carries a real, currently-uncharacterized
  tail-latency (or possibly non-termination) risk that idle-cluster testing
  (`split_placing_completion.rs`'s own e2e, and rung 7's RF=3 bench) never
  exercised. This is worth a follow-up issue on its own, independent of
  this ADR's original scope.
- **F5 baseline's own `(c′)` numbers are not a real "write blip"** — they
  are the ordinary observed max PUT latency (169.6–339.2ms) sampled
  *during* a window where the split never actually cut over, so the
  parent tablet never stopped serving in the first place (it stays
  `Active`/`Splitting` — genuinely serving, not frozen — for as long as
  Stage 1/2's catch-up gate is unsatisfied). Zero retries in every baseline
  run confirms this: the client never saw a disruption, because the
  disruption-causing event (cutover) never happened. This is a materially
  **worse** outcome than any bounded blip, not a better one — a split that
  silently never completes is a starvation/liveness failure, not a fast,
  invisible one.

**What the baseline's own diagnostics show, without further root-causing
(flagged, not chased, per this task's scope)**: `/admin/raftkv` polled
every 15s through all 3 baseline runs shows the parent tablet's own
`commit_index` **frozen** for the entire multi-minute window (e.g. run 1:
`commit_index` pinned at 2394 from the 15s sample through the 300s
timeout, while `log_len` climbed from ~3,700 to ~25,000 and `learners`
stayed `["m0"]` throughout) even though client `PutOk` acks kept arriving
throughout with zero retries — a genuinely puzzling combination (a frozen
committed-index alongside acking writes) this session does not have an
explanation for and did not chase further; it may be an artifact of the
admin/metrics sampling path rather than the real consensus state.
Regardless of the exact mechanism, the **externally observable** fact
across all 3 runs is unambiguous: the split-with-a-real-recruited-learner
never completed within 5 minutes under a continuous write load to the
splitting tablet, on this host, in this session, every time it was tried.

**Verdict — did it improve?**

- **At 3-node/RF=3**: no longer a regression (rung 7's own honest finding);
  today's numbers put fork-first and the baseline within each other's
  noise band, plausibly attributable to the range-aware clone commit that
  landed between rung 7 and this amendment, though not confirmed in
  isolation.
- **At cluster>RF, time-to-relief**: **yes, decisively** — fork-first
  relieves a splitting tablet in 1.3–1.7s regardless of sustained write
  load; the F5 baseline failed to relieve it at all within 5 minutes, in
  every run, under the identical load. This is the concrete quantification
  this ADR's own design argument predicted and rung 7 could not produce.
- **At cluster>RF, time-to-fully-placed**: **not shown to have improved,
  and not comparable to the baseline at all** — the baseline never reaches
  "fully placed" in this scenario either (it never even reaches "split
  complete"), and fork-first's own post-cutover convergence loop failed to
  reach `done` within a 240s budget in 2 of 3 runs despite (in one of
  those two) the tablet's live replicas already matching its target. This
  is reported as a genuine open question — this ADR's decoupling of
  movement from placement demonstrably fixes availability under load, but
  this session's own numbers do not show the *placement* half converging
  reliably under the same load, and that gap deserves its own
  investigation rather than being folded into a categorical "the ADR's
  claim is confirmed."

## Amendment (2026-09-01): §3's completion loop never fires under load — issue #528, root cause and fix

**Symptom.** Under sustained write load with real host/CPU contention (not
reproducible under light `SimEnv` conditions), a split child's
`split_placing` entry could stay un-`done` indefinitely — the §3
completion loop (`animusd::split_placing_completion`) never observed the
convergence predicate it needs (`group.config() == t.replicas`, no
learners, held for `SPLIT_PLACING_DONE_SETTLE`) because that predicate
never actually held for long enough to settle.

**Root cause: §2's third reconcile-loop phase (`Metadata::
split_placing_reconcile`) recomputed `select_replicas` **fresh, off
current membership, every single tick** — the design this ADR's original
§2 text explicitly called for ("never trust or persist `SplitPlacing::
target`... always recomputing `select_replicas` fresh"). Under sustained
load, `animus-control`'s ordinary failure detector (ADR 0012) flaps
members `Active`↔`Down` at a sub-second timescale (confirmed: dozens of
`UpsertMember{Down}` proposals per node within a 240s window, momentarily
all four candidates `Down` at once) — each flip changes which members
`active_candidates` offers the placement engine, so `select_replicas`
picks a **different** target essentially every tick (confirmed: the
computed target flapped between two 3-of-4 candidate sets four times in
~130 reconcile ticks). Each retarget bumps the child's epoch via a fresh
`CasTabletReplicas` and restarts `animus-cp-data`'s `reconfigure_step`
learner-phased sequencing (add-learner → catch-up → promote → remove)
from scratch against a **new** node — the target moved faster than the
mover could ever complete one cycle, a livelock one layer below both §2's
own (otherwise-correct) per-tick logic and §3's own (otherwise-correct)
completion predicate. A secondary, compounding finding: the ordinary
repair phase (`Metadata::reconcile`) was **not** excluded for an un-done
`split_placing` tablet, so it could independently propose a competing
`CasTabletReplicas` for the same tablet in the same tick (harmless in
isolation — the loser's CAS just rejects — but additional avoidable
churn on top of the primary livelock).

**Fix, implemented in `animus-control`** (issue #528, PR TBD by the
orchestrator):

1. **`SplitPlacing::target` is now authoritative, not a write-once
   diagnostic.** The third reconcile phase drives toward the STORED target
   **verbatim** while every one of its members is currently `Active` —
   it no longer recomputes `select_replicas` on a healthy target at all,
   which is what makes the target itself stop flapping.
2. **A transiently-`Down` target member pauses the drive, never
   retargets.** If any stored-target member is not currently `Active`,
   the phase proposes nothing for that tablet this tick — never a
   `CasTabletReplicas` toward a target with a dead member, and never an
   immediate retarget either.
3. **A genuinely-dead target member re-targets only past a dwell.**
   `animus-control::node`'s `reconcile_loop` now tracks, per
   `(tablet, target-member)`, how long that member has been
   **continuously** non-`Active` (`env.now()`-keyed, `BTreeMap`, never
   wall clock, never `HashMap` — the workspace's determinism rules apply
   here exactly as everywhere else). Only once that duration exceeds
   [`SPLIT_PLACING_RETARGET_DWELL`] (5s — ten times the failure detector's
   own `DETECT_TIMEOUT`, comfortably past the sub-second flap noise this
   incident's own investigation measured, and past the pre-existing
   `SPLIT_PLACING_DONE_SETTLE` precedent of 1.5s for "how long an
   observation must hold before it's trusted") does the leader recompute —
   via [`replan`], not `select_replicas`, so a still-`Active` survivor of
   the old target is kept and only the genuinely-gone member is replaced,
   minimizing churn exactly the way ordinary repair already does for any
   other tablet — and propose the result as a new, **replicated**
   command, `MetaCommand::RetargetSplitPlacing { tablet, expected_epoch,
   target }` (epoch-CAS'd against the child's own current epoch, the
   `MarkSplitPlacingDone` discipline), rather than reaching for
   `CasTabletReplicas` directly. Replicating the retarget itself is what
   makes the new value stable for every subsequent tick and every replica
   (including a newly elected leader), instead of re-deciding it locally
   every time. `target: None` (unsatisfiable at cutover) keeps its
   original "no stored value to protect, so keep retrying every tick"
   stance — a successful recomputation there now *establishes* the first
   stored target via the same command, rather than leaving the
   diagnostic stuck at `None` forever once the live replicas converge.
4. **Repair (`Metadata::reconcile`) now excludes any un-done
   `split_placing` tablet**, the identical exclusion `rebalance_placement`
   already had — the dwell-gated placing phase is the sole mover for that
   tablet until `done`, closing the secondary compounding race named
   above.

`MetaCommand::RetargetSplitPlacing` is **not** on `is_relayable_command`'s
allowlist (`animus-node::wire`) — like `CasTabletReplicas`, it is proposed
directly by the control-plane leader off its own live `RaftNode` handle,
never relayed from a follower.

**What did NOT change**: §3's completion loop itself
(`animusd::split_placing_completion`), its settle tracker, its leader
gate, and `MarkSplitPlacingDone`'s own epoch-CAS — all correct as
designed; the predicate they wait on simply never used to hold long enough
to observe. `tests/split_placing_completion.rs` (`animusd`) stays green
unmodified and is now a stronger proof than it was before this fix: it
demonstrates the completion loop actually firing under the corrected §2
mechanism, not merely that its own isolated logic is sound.

**Regressions**: `crates/animus-control/src/meta.rs`'s unit tests
(`split_placing_reconcile_does_not_retarget_on_a_flap`,
`split_placing_reconcile_pauses_while_a_target_member_is_down_and_not_
ready`, `split_placing_reconcile_retargets_once_ready_keeping_live_
survivors`, the `retarget_split_placing_*` apply-arm suite, and
`reconcile_skips_an_undone_split_placing_tablet_even_with_a_down_
replica`) and `crates/animus-control/tests/placement_split_placing.rs`'s
two new node-level, real-`reconcile_loop`-driven tests
(`split_placing_phase_retargets_a_member_down_past_the_dwell`,
`split_placing_phase_flapping_under_the_dwell_does_not_retarget`) —
the latter proving the dwell gate live over `SimEnv`, not merely asserted
from the pure decision function in isolation.

See `docs/engineering-lessons.md`'s matching entry for the generalizable
lesson this incident teaches about auditing a convergence loop's *inputs*,
not just its own internal logic.
