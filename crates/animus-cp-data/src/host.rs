//! Pure planning half of the per-node **tablet-host reconciler** (ADR 0031).
//!
//! `animusd` currently scatters this node's "which tablets do I host, and what
//! should I do about each one" decision across four independent `ProdEnv` loops
//! (`cp_join_host_loop`/`cp_join_host`, `cp_gc_loop`/`cp_gc_release_phase`,
//! `cp_reconfigure_loop`) — each re-deriving its own slice of the same
//! replicated `Metadata` view and its own per-node bookkeeping (`minted`,
//! `pending_release`). This module unifies the *decision* into one pure,
//! synchronous function, [`plan`], over one metadata snapshot plus a small set
//! of caller-gathered facts — mirroring the sync-core/async-driver split used
//! throughout this crate and the control plane: **the decision is pure and
//! testable here; the timing, locking, and actual I/O stay in `animusd`** (a
//! sibling PR wires this in — see ADR 0031).
//!
//! This module is a straight semantic port of `animusd::topology`'s
//! `plan_join_host`/`tablets_to_reclaim`/`tablets_to_release` plus the
//! additional decisions that used to live only inline in `lib.rs`'s loop
//! bodies (scope narrowing, reconfigure, and the release-GC epoch-stability
//! dampener) — see the root `CLAUDE.md` engineering-practices entries on the
//! release-GC sibling-corruption bug and the stale-scope bug this design
//! exists to make structurally impossible by construction (there is exactly
//! one place, [`plan`], that decides "erase bounded by what range" instead of
//! four independent call sites that can each get it slightly wrong).
//!
//! No `Env`/time/RNG/I-O of any kind — [`plan`] takes an owned snapshot
//! ([`MetadataView`]) and a small bundle of already-gathered facts
//! ([`TabletFacts`]) and returns a plain `Vec<`[`HostAction`]`>` plus the
//! successor [`LocalState`]. `BTreeMap`/`BTreeSet` only (root `CLAUDE.md`'s
//! determinism rule applies to every crate, not just the sim-tested ones, and
//! this module has no excuse either way — it's pure logic).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

#[cfg(test)]
use animus_env::nid;
use animus_env::{Env, NodeId};
use animus_storage::StorageEngine;
use animus_tablet::{Epoch, KeyRange, Tablet, TabletId};

use crate::{RaftKvNode, StorageScope, wal_file};

/// How many consecutive [`plan`] calls the release condition (this node
/// dropped from a still-existing tablet's replica set, **and** its own durable
/// Raft config already excludes it — see [`TabletFacts::config_excludes_me`])
/// must hold *at an unchanged tablet epoch* before [`HostAction::Release`] is
/// actually planned (ADR 0029). Mirrors `animusd::RELEASE_CONFIRM_TICKS`
/// exactly (kept in lockstep by hand — this module has no dependency on
/// `animusd` to share the constant the other way). A small dampener so a
/// restarting control replica's replay transients (which pass the tablet map
/// through historical states) can't trigger a spurious release, and a
/// metadata re-add (which bumps the tablet's epoch via the placement CAS)
/// cancels a release part-way confirmed.
pub const RELEASE_CONFIRM_TICKS: u8 = 3;

/// An owned, minimal projection of replicated `Metadata` — *not* the whole
/// `animus_control::Metadata` (this crate stays decoupled from the control
/// plane's full state shape; only what a host-reconcile decision needs).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetadataView {
    /// Every tablet in the cluster's tablet map, keyed by id.
    pub tablets: BTreeMap<TabletId, Tablet>,
    /// Base node ids the control plane's failure detector currently considers
    /// `Down` — the priority input [`HostAction::Reconfigure`] carries so the
    /// executing `reconfigure_step` can tell a failure repair from a healthy
    /// rebalance move (ADR 0029).
    pub down: BTreeSet<NodeId>,
    /// Tablet ids merged away by a `MetaCommand::MergeTablets` commit (ADR
    /// 0033) — mirrors `animus_control::Metadata::merged_tablets` verbatim.
    /// A tablet in [`LocalState::hosted`] but absent from `tablets` is
    /// **reclaimed** (erased — its whole table was dropped) unless it also
    /// appears here, in which case it is **absorbed** (torn down, data left
    /// untouched — a sibling now owns its range on the same shared engine).
    /// See [`HostAction::Absorb`]'s doc for why this can't be inferred from
    /// `tablets` alone.
    pub merged: BTreeSet<TabletId>,
    /// Split provenance (ADR 0018 §2 amendment, PR2): every split child's id
    /// mapped to its source tablet's id — mirrors
    /// `animus_control::Metadata::split_parents` verbatim. Gates
    /// [`HostAction::Host`] for a fresh split child: it must not stand up
    /// until this node's own engine contains the source's range-seal marker
    /// covering the child's range (`TabletFacts::parent_seal_observed`) — see
    /// `seal.rs`'s module doc for why a structural version-space separation
    /// (the retired `version_floor`) isn't enough on its own.
    pub split_parent: BTreeMap<TabletId, TabletId>,
    /// Merge provenance (ADR 0018 §2 amendment, PR2): every absorbed
    /// tablet's id mapped to the surviving (`left`) tablet's id — mirrors
    /// `animus_control::Metadata::absorbed_by` verbatim. Lets a survivor's
    /// `WidenScope` gating find every tablet id it has ever absorbed, to
    /// check each one's seal marker before widening past the range it
    /// handed off.
    pub absorbed_by: BTreeMap<TabletId, TabletId>,
}

/// Per-tablet facts the caller gathers from live, impure state (a registered
/// group handle, an async engine read, this node's own Raft accessors) before
/// calling [`plan`]. A tablet with no entry in the facts map is treated as
/// "not currently hosted" (every field's default: `false`/`None`) — the same
/// outcome as an explicit all-`false`/`None` entry, so a caller may omit facts
/// entirely for a tablet it has no local handle for at all.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TabletFacts {
    /// This node currently has a registered local replica/group handle for
    /// this tablet. Independent of [`LocalState::hosted`] (the per-node claim
    /// set `plan` itself maintains): a tablet can be claimed (about to be
    /// hosted) slightly ahead of its async stand-up actually registering a
    /// handle, or a handle can still be registered while its teardown is
    /// in-flight. Every other fact below is meaningless (and must be left at
    /// its default) when this is `false`.
    pub hosted: bool,
    /// This node's local replica is the tablet's current Raft leader — drives
    /// [`HostAction::Reconfigure`]. Only consulted when `hosted` is `true`.
    pub is_leader: bool,
    /// This node's own **durable Raft log** voter config for this tablet's
    /// group already excludes this node — the release-gate anchor (ADR
    /// 0029): the replay-independent signal a removed node reliably adopts,
    /// unlike replicated `Metadata.tablets`, which a restarting control
    /// replica replays through historical states. Must be `false` whenever
    /// `hosted` is `false` ("stand-up in flight" reads the same as "still a
    /// voter" — never treated as excluded).
    pub config_excludes_me: bool,
    /// This group's own current live `StorageScope` range, if hosted — `None`
    /// when `hosted` is `false`. Compared against the tablet's current
    /// metadata range to decide [`HostAction::NarrowScope`]; **never** used as
    /// the bound for [`HostAction::Release`]'s erase (that always uses the
    /// tablet's current metadata range — see the doc on
    /// [`HostAction::Release`]).
    pub scope_range: Option<KeyRange>,
    /// Whether this tablet's scoped range already holds data in the shared
    /// engine (an async presence check, `StorageScope::has_data`) — gathered
    /// by the caller only for a *candidate* tablet (a fresh
    /// [`plan_join_host`] match not yet in [`LocalState::hosted`]); upgrades
    /// a restart of a tablet this node already held data for to full-voter
    /// re-formation (WAL recovery alone does not restore voter status from a
    /// non-voter start). Ignored once a tablet is already in
    /// [`LocalState::hosted`] — narrow-only from then on.
    pub has_data: bool,
    /// **ADR 0018 §2 amendment (PR2)**: for a not-yet-hosted candidate whose
    /// [`MetadataView::split_parent`] names a source tablet, whether this
    /// node's own engine already contains that source's range-seal marker
    /// covering the candidate's own range (an async prefix scan gathered by
    /// the caller, mirroring `has_data`'s shape). Irrelevant (left at its
    /// default `false`) for a candidate with no parent — a bootstrapped
    /// fresh table's first tablet hosts immediately regardless.
    pub parent_seal_observed: bool,
    /// **ADR 0018 §2 amendment (PR2)**: for an *already-hosted* tablet whose
    /// current metadata range is wider than this node's own live scope (a
    /// pending [`HostAction::WidenScope`]), whether this node's own engine
    /// already contains a seal marker — from any tablet id this survivor has
    /// ever absorbed ([`MetadataView::absorbed_by`]) — covering the
    /// newly-widened portion. Irrelevant (left at its default `false`) when
    /// no widen is pending.
    pub widen_seal_observed: bool,
    /// **ADR 0018 §2 amendment, fix (the split_cluster.rs livelock)**: every
    /// range this *already-hosted* tablet still owes a committed range-seal
    /// for, but whose local engine does not yet contain — gathered fresh
    /// every tick, never cached. Two sources, both re-derived from scratch
    /// each call (see `gather_facts`): (1) a **split handoff** — for each
    /// child of this tablet named in [`MetadataView::split_parent`], the
    /// child's own current metadata range, if this node's engine lacks a
    /// seal marker covering it; (2) an **absorb handoff** — this tablet's
    /// own current live scope, if this tablet itself is named in
    /// [`MetadataView::merged`] and this node's engine lacks a seal marker
    /// covering it. [`plan`] turns each entry into a
    /// [`HostAction::ProposeSeal`], executed only if this node is currently
    /// the tablet's leader.
    ///
    /// **Why this must be a persistent, re-derived-every-tick condition and
    /// not a one-shot side effect of the tick that first narrows/tears
    /// down**: the tick that first notices "my scope needs narrowing" (or
    /// "my tablet was absorbed") happens independently on every replica, on
    /// its own timer, and is not synchronized with *which* replica happens
    /// to hold leadership at that exact instant. A one-shot design — narrow
    /// (or tear down) unconditionally, and *only if I also happen to be
    /// leader right now* also propose the seal — has no second chance if
    /// leadership isn't held by anyone at that precise tick (e.g. mid
    /// leadership transfer): the local mutation that would have re-derived
    /// the trigger already happened, so the seal is simply never proposed by
    /// anyone. Re-deriving this fact from durable state every tick instead
    /// means whichever replica *eventually* holds leadership gets its
    /// chance, however leadership shuffles relative to the local scope
    /// change or teardown. See `docs/engineering-lessons.md` for the full
    /// story (this is what produced the deterministic `split_cluster.rs`
    /// failures under a genuine multi-process split deployment).
    pub pending_seals: Vec<KeyRange>,
}

/// This node's persistent-for-the-life-of-the-process bookkeeping that
/// [`plan`] threads through calls — the pure-state mirror of `animusd`'s
/// `minted` claim set and `pending_release` epoch-stability dampener. Not
/// meant to be durable: a restarted node starts from [`LocalState::default`]
/// and re-discovers every tablet it should host from replicated `Metadata`
/// fresh (exactly as `minted`/`pending_release` do today — see
/// `animusd::CpHostCtx`'s doc).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalState {
    /// The per-node claim set of tablets this node has decided to host (or is
    /// hosting) — mirrors `minted`. A tablet enters when [`plan`] emits a
    /// [`HostAction::Host`] for it. **`plan` never removes a tablet from this
    /// set on its own** when emitting [`HostAction::Reclaim`] /
    /// [`HostAction::Release`] — those teardowns are async and can fail/time
    /// out (mirroring `cp_gc_tablet`'s conditional `minted.remove`, which only
    /// fires once shutdown + erase + WAL removal actually succeed). The
    /// caller removes the tablet from its own `LocalState` (see
    /// [`LocalState::confirm_torn_down`]) only once it has confirmed the
    /// corresponding teardown fully completed; until then, the next `plan`
    /// call keeps re-planning the same `Reclaim`/`Release` action, exactly
    /// like the real loop retrying on a later tick.
    pub hosted: BTreeSet<TabletId>,
    /// The release-GC epoch-stability dampener (ADR 0029): `tablet -> (epoch
    /// observed, consecutive confirming ticks)`. Mirrors `pending_release`.
    pub pending_release: BTreeMap<TabletId, (Epoch, u8)>,
}

impl LocalState {
    /// Record that `tablet`'s teardown (the action side of a planned
    /// [`HostAction::Reclaim`] or [`HostAction::Release`]) has fully
    /// completed — removes it from [`hosted`](Self::hosted) and drops any
    /// leftover [`pending_release`](Self::pending_release) entry. Call this
    /// only once the caller's own teardown (unregister the handle, stop the
    /// driver, erase the scope, delete the WAL — `animusd::cp_gc_tablet`'s
    /// shape) has actually succeeded; a timed-out teardown must **not** call
    /// this, so the next [`plan`] call re-plans the same action.
    pub fn confirm_torn_down(&mut self, tablet: TabletId) {
        self.hosted.remove(&tablet);
        self.pending_release.remove(&tablet);
    }
}

/// This node's plan for join-hosting a tablet whose replica set currently
/// includes it (the pure decision behind [`plan_join_host`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinHostPlan {
    /// `true` — a fresh tablet forming for the first time (whole-keyspace or a
    /// split child — both start from data already present in the shared
    /// engine, if any): start with the **full** voting config so a replica can
    /// campaign with no live leader. `false` — this node is *joining* an
    /// existing, already-led group (the reconciler placed it as a spare):
    /// start as a quiet **non-voter** until the leader adds it.
    pub initial_formation: bool,
}

/// This node's plan for join-hosting `tablet` given its base id and the
/// tablet's current replica set / epoch — a direct port of
/// `animusd::topology::plan_join_host`. `None` means "not this node's concern
/// right now" (its base id is not in `replicas` at all).
///
/// Since a single-command (control-plane-only) split moves no data — a split
/// child's range is confined by its own `StorageScope` against the *same*
/// already-populated shared engine, not seeded from a handoff — a fresh split
/// child is formed exactly like a fresh whole-keyspace tablet: both get
/// `initial_formation: true`.
///
/// This does **not** perform the per-node dedup claim (stateful — [`plan`]
/// layers that on top via [`LocalState::hosted`]), nor the
/// `StorageScope::has_data` restart-upgrade (an async engine read, impure by
/// construction — the caller gathers it as [`TabletFacts::has_data`]).
#[must_use]
pub fn plan_join_host(base_id: NodeId, replicas: &[NodeId], epoch: Epoch) -> Option<JoinHostPlan> {
    if !replicas.contains(&base_id) {
        return None;
    }
    Some(JoinHostPlan {
        initial_formation: epoch <= Epoch::INITIAL,
    })
}

/// Which of this node's `hosted` tablets have been dropped from the
/// replicated tablet map and should be reclaimed (a direct port of
/// `animusd::topology::tablets_to_reclaim`, ADR 0024). A tablet is reclaimed
/// iff it is in `hosted` but absent from `tablets`. The caller is responsible
/// for the `last_applied == 0` recovery guard (skip entirely before
/// replicated `Metadata` has recovered) — that gate reads a live `RaftNode`
/// the pure function has no business taking, so it stays in the caller.
#[must_use]
pub fn tablets_to_reclaim(
    hosted: &BTreeSet<TabletId>,
    tablets: &BTreeMap<TabletId, Tablet>,
) -> Vec<TabletId> {
    hosted
        .iter()
        .copied()
        .filter(|t| !tablets.contains_key(t))
        .collect()
}

/// Which of this node's `hosted` tablets have had **this node** dropped from
/// their replica set while the tablet itself **still exists** — a direct port
/// of `animusd::topology::tablets_to_release` (ADR 0029). The dual of
/// [`tablets_to_reclaim`]: reclaim fires on the tablet being **absent**;
/// release fires on the tablet being **present** but no longer placing a
/// replica on `base_id`. The two predicates are **mutually exclusive** on the
/// same input.
#[must_use]
pub fn tablets_to_release(
    hosted: &BTreeSet<TabletId>,
    tablets: &BTreeMap<TabletId, Tablet>,
    base_id: NodeId,
) -> Vec<TabletId> {
    hosted
        .iter()
        .copied()
        .filter(|t| {
            tablets
                .get(t)
                .is_some_and(|tab| !tab.replicas.contains(&base_id))
        })
        .collect()
}

/// Whether `inner` is fully contained within `outer` (`inner ⊆ outer`) —
/// the narrow-only precondition [`HostAction::NarrowScope`] must satisfy
/// (never a widen), and — with the operands swapped — the widen-only
/// precondition of [`HostAction::WidenScope`]. Delegates to
/// [`KeyRange::contains_range`] (the shared primitive `animusd`'s read-path
/// scope pre-check uses too, ADR 0033).
fn is_subrange(inner: &KeyRange, outer: &KeyRange) -> bool {
    outer.contains_range(inner)
}

/// The range a merge's `WidenScope` is about to absorb, given the group's
/// scope `old` (just before widening) and `new` (the tablet's current, wider
/// metadata range).
///
/// A merge's `WidenScope` always widens **from the right only**:
/// `MetaCommand::MergeTablets` (`animus-control`) only ever extends the
/// survivor's `end` (`l.range.end = new_end`), so `new.start == old.start`
/// and `new.end` is strictly greater than `old.end`. The absorbed range is
/// therefore exactly `[old.end, new.end)`.
///
/// `None` if `old.end` is absent (a scope already unbounded above has
/// nothing further right to absorb — not reachable in practice) or if
/// `new.start != old.start` (defensive).
///
/// (A split's dual — the range a `NarrowScope` just handed off — used to
/// live here too as `handed_off_range`, computed from the source's own
/// before/after scope. It's gone: `gather_facts`'s `pending_seals` now
/// derives a split child's handed-off range directly from the child's own
/// metadata range (`view.tablets`), which is — by construction, a split
/// child's range is always exactly what its source seals for it — the
/// identical value, without needing the source's pre-narrow scope at all.
/// See `TabletFacts::pending_seals`'s doc.)
fn absorbed_range(old: &KeyRange, new: &KeyRange) -> Option<KeyRange> {
    if new.start != old.start {
        return None;
    }
    let old_end = old.end.clone()?;
    Some(KeyRange {
        start: old_end,
        end: new.end.clone(),
    })
}

/// Whether `tablet`'s group has ever proposed a range-seal marker covering
/// `needed` (ADR 0018 §2 amendment) — the async engine scan behind both
/// [`TabletFacts::parent_seal_observed`] and
/// [`TabletFacts::widen_seal_observed`]. Bounded to `tablet`'s own
/// seal-marker namespace (`seal::scan_bound`), never a whole-engine scan.
async fn seal_covers<S: StorageEngine>(storage: &S, tablet: u64, needed: &KeyRange) -> bool {
    let (start, end) = crate::seal::scan_bound(tablet);
    let rows = storage.scan(&start, &end).await.unwrap_or_default();
    rows.iter().any(|(_, vv)| {
        crate::seal::decode_seal_value(&vv.value)
            .is_some_and(|(range, _)| range.contains_range(needed))
    })
}

/// One reconciling action [`plan`] can emit for a single tablet. A caller
/// (`animusd`, PR4) executes these against its own live `ProdEnv` state;
/// `plan` itself performs no I/O.
///
/// Emitted in a fixed overall order — every [`ProposeSeal`](Self::ProposeSeal)
/// action, then every [`NarrowScope`](Self::NarrowScope)/
/// [`WidenScope`](Self::WidenScope) action, then every [`Host`](Self::Host),
/// then every [`Reconfigure`](Self::Reconfigure), then every
/// [`Release`](Self::Release)/[`Reclaim`](Self::Reclaim)/[`Absorb`](Self::Absorb)
/// — mirroring the existing loops' relative priority (give a pending seal a
/// chance to land before anything that gates on it; adjust a still-hosted
/// tablet's scope before deciding anything else about it; stand up a
/// newly-placed tablet before reconfiguring anyone; reconcile membership
/// before tearing anything down). Within each group, tablets are emitted in
/// `TabletId` order (a `BTreeMap` iteration is deterministic on every node).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostAction {
    /// (Re-)propose this already-hosted tablet's own range-seal for `range`,
    /// if this node is currently its leader — ADR 0018 §2 amendment fix.
    /// Derived from [`TabletFacts::pending_seals`], a **persistent condition
    /// re-checked every tick** (not a one-shot side effect bundled into
    /// [`NarrowScope`](Self::NarrowScope)'s or
    /// [`Absorb`](Self::Absorb)'s own tick, as it used to be) — see that
    /// field's doc for why a one-shot design can silently never propose the
    /// seal at all. A no-op execution-side if this node isn't currently the
    /// tablet's leader (harmless — whichever replica eventually *is* leader
    /// will see the same still-pending condition next tick and propose it
    /// then). Re-proposing an already-covered range is impossible by
    /// construction (`pending_seals` stops naming it once this node's own
    /// engine observes a covering seal marker).
    ProposeSeal {
        /// The (already-hosted) tablet whose own group should propose the
        /// seal — the range handed off (a split child's range) or the
        /// tablet's own current scope (an absorb handoff).
        tablet: TabletId,
        /// The range to seal.
        range: KeyRange,
    },
    /// Narrow this already-hosted tablet's live `StorageScope` range to match
    /// its current metadata range (a split narrowed the *source* tablet's
    /// range in `Metadata`, but the already-running group's own scope object
    /// is otherwise never touched again). Always a proper narrowing —
    /// [`plan`] never emits this for a range that would widen the scope.
    NarrowScope {
        /// The tablet whose local scope should narrow.
        tablet: TabletId,
        /// The new (narrower-or-equal) range to narrow to — the tablet's
        /// current metadata range.
        range: KeyRange,
    },
    /// Widen this already-hosted tablet's live `StorageScope` range to match
    /// its current (now-wider) metadata range — the dual of
    /// [`NarrowScope`](Self::NarrowScope) (ADR 0033 tablet merge): this
    /// tablet was the surviving (`left`) side of a `MetaCommand::MergeTablets`
    /// commit, so its replicated range now also covers what used to be the
    /// merged-away sibling's range, already physically present on the same
    /// node-shared engine under the same table prefix. Always a proper
    /// widening — [`plan`] never emits this for a range that would narrow the
    /// scope (that's [`NarrowScope`](Self::NarrowScope)'s job).
    WidenScope {
        /// The tablet whose local scope should widen.
        tablet: TabletId,
        /// The new (wider-or-equal) range to widen to — the tablet's current
        /// metadata range.
        range: KeyRange,
    },
    /// Stand up this node's member of `tablet`'s group for the first time —
    /// a fresh whole-keyspace tablet, a split child, or a reconciler-placed
    /// spare all reach this the same way.
    Host {
        /// The tablet to host.
        tablet: TabletId,
        /// The table this tablet is scoped to (empty string for the legacy
        /// whole-keyspace tablet, mirroring `t.table.unwrap_or_default()`).
        table: String,
        /// The range to scope the new group's `StorageScope` to.
        range: KeyRange,
        /// `true` — start with the full voter config (this node forms fresh,
        /// or is re-forming after a restart with data already on disk).
        /// `false` — start as a quiet non-voter joining an already-led group.
        initial_formation: bool,
    },
    /// Take one single-server `reconfigure_step` toward `desired`, given the
    /// currently-`Down` node set — planned every tick this node leads
    /// `tablet`'s group, converged or not (a steady group's `reconfigure_step`
    /// is itself a no-op, so this carries no churn on its own).
    Reconfigure {
        /// The tablet whose group this node leads.
        tablet: TabletId,
        /// The tablet's desired replica set per replicated `Metadata`.
        desired: BTreeSet<NodeId>,
        /// Base node ids the failure detector currently considers `Down`.
        down: BTreeSet<NodeId>,
    },
    /// Stop and tear down this node's now-idle group for a tablet whose
    /// replica set moved **off** this node while the tablet **still exists**
    /// (a drain, a failure-repair swap, or a rebalance move) — the release
    /// dual of [`Reclaim`](Self::Reclaim). Only planned once the release
    /// condition has held for [`RELEASE_CONFIRM_TICKS`] consecutive calls at
    /// an unchanged tablet epoch.
    ///
    /// **`erase_bound` is always the tablet's CURRENT metadata range — never
    /// a `TabletFacts::scope_range` fact.** A just-split source tablet's
    /// already-hosted scope can still be stale-wide if this node was dropped
    /// from its replica set before a `NarrowScope` action for it was ever
    /// planned and executed; erasing an unbounded/stale-wide scope on a
    /// shared engine would tombstone a co-hosted sibling's live keys (the
    /// documented release-GC sibling-corruption bug this design exists to
    /// make structurally impossible). The caller must re-narrow to
    /// `erase_bound` immediately before erasing, exactly as
    /// `animusd::cp_gc_tablet` does.
    Release {
        /// The tablet to release.
        tablet: TabletId,
        /// The range to bound the erase to — the tablet's current
        /// `Metadata`-replicated range.
        erase_bound: KeyRange,
    },
    /// Reclaim this node's local artifacts of a tablet whose whole table was
    /// dropped (absent from the tablet map entirely, ADR 0024) — the reclaim
    /// dual of [`Release`](Self::Release). No current range exists to narrow
    /// to (every same-prefix sibling still resident is dying in the same
    /// pass too), so the caller erases the group's full existing scope.
    Reclaim {
        /// The tablet to reclaim.
        tablet: TabletId,
    },
    /// Tear down this node's now-idle group for a tablet that **vanished
    /// from the tablet map because it was merged into a sibling** (ADR 0033
    /// `MetaCommand::MergeTablets`), rather than because its whole table was
    /// dropped ([`Reclaim`](Self::Reclaim)) — distinguished via
    /// [`MetadataView::merged`]. **Never erases any data**: the merge
    /// survivor (the tablet's former `left` neighbor) now owns this range on
    /// the very same node-shared engine, so this only stops the group's Raft
    /// driver and removes its own WAL file — the physical keys stay exactly
    /// where they are, now served through the survivor's widened
    /// [`WidenScope`](Self::WidenScope). Distinguishing this from `Reclaim`
    /// is not optional: erasing here would tombstone live data the merge
    /// survivor is about to (or already does) serve. Inferring "was this a
    /// merge" from `tablets` alone (e.g. "does some other tablet's range now
    /// cover mine") is unsound — two different tables' still-unsplit tablets
    /// can have byte-identical default ranges, so a range-only check could
    /// misattribute an unrelated table's tablet as the merge survivor and
    /// silently skip a real drop's erase instead.
    Absorb {
        /// The tablet to absorb (tear down without erasing).
        tablet: TabletId,
    },
}

/// The single pure decision behind every per-node tablet-host reconcile tick
/// (ADR 0031): given one snapshot of replicated state (`view`), this node's
/// base `raftkv` id, the caller-gathered per-tablet facts, and this node's own
/// prior [`LocalState`], decide every action to take and the successor
/// `LocalState` to carry into the next call.
///
/// Pure and synchronous — no `Env`, no clock, no RNG, no I/O of any kind. The
/// caller (`animusd`, PR4) gathers `facts` from its own live registry/engine,
/// calls `plan` on a fixed tick, executes the returned actions, and threads
/// the returned `LocalState` into the next call (removing a tablet from
/// [`LocalState::hosted`] only once its own async teardown for a planned
/// `Reclaim`/`Release` has actually completed — see
/// [`LocalState::confirm_torn_down`]).
#[must_use]
pub fn plan(
    view: &MetadataView,
    facts: &BTreeMap<TabletId, TabletFacts>,
    state: &LocalState,
    base_id: NodeId,
) -> (Vec<HostAction>, LocalState) {
    let mut next = state.clone();
    let mut actions = Vec::new();

    // ADR 0033: defer every `WidenScope` while this node still hosts a
    // merged-away tablet (present in `state.hosted` ∩ `view.merged`) — i.e.
    // while an `Absorb` teardown has not yet confirmed. The absorb's teardown
    // is what *drains* the absorbed group's committed writes into this node's
    // shared engine (see `Reconciler::teardown`'s Absorb arm); widening the
    // survivor's scope before that drain completes would let the survivor's
    // leader serve reads for the absorbed range from an engine that may not
    // yet hold all of its acked data. Coarse (any pending absorb defers every
    // widen, not just the one absorbing into this survivor — the planner has
    // no reliable absorbed→survivor association once the absorbed tablet is
    // gone from `view.tablets`) but sound, deterministic, and merges are rare
    // operator actions: the deferral costs one reconcile tick.
    let absorbing = state.hosted.iter().any(|t| view.merged.contains(t));

    // --- Phase 0: (re-)propose any range-seal this node's own hosted groups
    // still owe (ADR 0018 §2 amendment fix). `TabletFacts::pending_seals` is
    // re-derived from scratch every tick — a persistent condition, not a
    // one-shot side effect bundled into this same tick's `NarrowScope`/
    // `Absorb` handling — so whichever replica eventually holds leadership
    // for the tablet gets a chance to propose it, however leadership
    // shuffles relative to the local scope change or teardown. See
    // `TabletFacts::pending_seals`'s doc for the full "why a one-shot design
    // can silently never propose the seal at all" argument.
    for (&tablet, f) in facts {
        for range in &f.pending_seals {
            actions.push(HostAction::ProposeSeal {
                tablet,
                range: range.clone(),
            });
        }
    }

    // --- Phase 1: narrow an already-hosted tablet's scope, or host a
    // newly-placed one. `to_host` batches the Host actions so every
    // NarrowScope precedes every Host in the returned order, even though
    // both are decided from the same tablet-map walk.
    let mut to_host = Vec::new();
    for (&tablet, t) in &view.tablets {
        let Some(join_plan) = plan_join_host(base_id.clone(), &t.replicas, t.epoch) else {
            continue;
        };
        if next.hosted.contains(&tablet) {
            if let Some(f) = facts.get(&tablet)
                && f.hosted
                && let Some(current) = &f.scope_range
                && t.range != *current
            {
                if is_subrange(&t.range, current) {
                    actions.push(HostAction::NarrowScope {
                        tablet,
                        range: t.range.clone(),
                    });
                } else if !absorbing
                    && is_subrange(current, &t.range)
                    && facts.get(&tablet).is_some_and(|f| f.widen_seal_observed)
                {
                    // ADR 0033: this tablet was the surviving
                    // (`left`) side of a merge — its metadata
                    // range grew to cover the absorbed sibling's
                    // range, already present on the shared engine.
                    // Only once no absorb is pending locally (see
                    // `absorbing` above): drain before widen. ADR
                    // 0018 §2 amendment: ALSO only once this node's
                    // own engine already contains the absorbed
                    // tablet's range-seal marker covering the
                    // widened portion (`widen_seal_observed`) — the
                    // structural replacement for the retired
                    // `version_floor` bump. Deferred (not planned)
                    // until the seal lands; `plan` re-checks it every
                    // tick, so this is a liveness deferral, never a
                    // dropped action.
                    actions.push(HostAction::WidenScope {
                        tablet,
                        range: t.range.clone(),
                    });
                }
                // Neither a subset nor a superset of the current
                // scope: an incomparable range mismatch that
                // should never happen in practice — deliberately
                // no-op rather than guess a direction.
            }
        } else {
            to_host.push((tablet, t, join_plan));
        }
    }
    for (tablet, t, join_plan) in to_host {
        let has_data = facts.get(&tablet).is_some_and(|f| f.has_data);
        // ADR 0018 §2 amendment: a split child (named in `view.split_parent`)
        // must not host until this node's own engine contains its parent's
        // range-seal marker covering its own range
        // (`TabletFacts::parent_seal_observed`) — the structural replacement
        // for the retired `version_floor` seeding. A tablet with no parent
        // entry (a bootstrapped fresh table, or a merge survivor — merges
        // never mint a new tablet id) hosts immediately, exactly as before.
        let seal_ready = match view.split_parent.get(&tablet) {
            Some(_parent) => facts.get(&tablet).is_some_and(|f| f.parent_seal_observed),
            None => true,
        };
        if !seal_ready {
            continue;
        }
        actions.push(HostAction::Host {
            tablet,
            table: t.table.clone().unwrap_or_default(),
            range: t.range.clone(),
            initial_formation: join_plan.initial_formation || has_data,
        });
        next.hosted.insert(tablet);
    }

    // --- Phase 2: reconfigure every tablet this node currently leads.
    for (&tablet, t) in &view.tablets {
        let is_leader = facts.get(&tablet).is_some_and(|f| f.hosted && f.is_leader);
        if is_leader {
            actions.push(HostAction::Reconfigure {
                tablet,
                desired: t.replicas.iter().cloned().collect(),
                down: view.down.clone(),
            });
        }
    }

    // --- Phase 3: reclaim (absent) / release (present, moved off) — mutually
    // exclusive on the same `mine` input.
    let mine: BTreeSet<TabletId> = next.hosted.clone();

    for tablet in tablets_to_reclaim_set(&mine, &view.tablets) {
        // ADR 0033: a tablet absent from the map because it was merged into
        // a sibling (recorded in `view.merged`) is absorbed — torn down with
        // no erase, since a sibling now owns its range on the same shared
        // engine — never reclaimed (which would erase it).
        if view.merged.contains(&tablet) {
            actions.push(HostAction::Absorb { tablet });
        } else {
            actions.push(HostAction::Reclaim { tablet });
        }
        next.pending_release.remove(&tablet);
    }

    let release_candidates = tablets_to_release_set(&mine, &view.tablets, base_id);
    // Drop confirm state for anything no longer a candidate (condition
    // flipped — re-added to the replica set, or reclaimed): its counter must
    // restart from scratch if it becomes a candidate again later.
    next.pending_release
        .retain(|t, _| release_candidates.contains(t));

    for tablet in release_candidates {
        // Safety anchor: only trust the metadata-based signal once this
        // node's own durable Raft log independently confirms it's no longer a
        // voter. "Not hosted at all" reads the same as "still a voter" —
        // never treated as excluded (stand-up in flight, or teardown already
        // completed elsewhere).
        let excluded = facts
            .get(&tablet)
            .is_some_and(|f| f.hosted && f.config_excludes_me);
        if !excluded {
            next.pending_release.remove(&tablet);
            continue;
        }

        // `tablets_to_release` only returns present tablets.
        let Some(t) = view.tablets.get(&tablet) else {
            continue;
        };
        let epoch = t.epoch;
        let confirmed = match next.pending_release.get_mut(&tablet) {
            // Same epoch, still confirming: advance the tick count.
            Some((seen, ticks)) if *seen == epoch => {
                *ticks = ticks.saturating_add(1);
                *ticks >= RELEASE_CONFIRM_TICKS
            }
            // First observation, or the epoch changed (a re-add's CAS bumped
            // it): (re)start the counter at this epoch.
            _ => {
                next.pending_release.insert(tablet, (epoch, 1));
                RELEASE_CONFIRM_TICKS <= 1
            }
        };
        if confirmed {
            next.pending_release.remove(&tablet);
            actions.push(HostAction::Release {
                tablet,
                erase_bound: t.range.clone(),
            });
        }
    }

    (actions, next)
}

/// [`tablets_to_reclaim`] over a `BTreeSet` input (phase 3 of [`plan`] already
/// has one on hand — avoids a `Vec` round trip).
fn tablets_to_reclaim_set(
    hosted: &BTreeSet<TabletId>,
    tablets: &BTreeMap<TabletId, Tablet>,
) -> BTreeSet<TabletId> {
    hosted
        .iter()
        .copied()
        .filter(|t| !tablets.contains_key(t))
        .collect()
}

/// [`tablets_to_release`] over a `BTreeSet` input, see
/// [`tablets_to_reclaim_set`].
fn tablets_to_release_set(
    hosted: &BTreeSet<TabletId>,
    tablets: &BTreeMap<TabletId, Tablet>,
    base_id: NodeId,
) -> BTreeSet<TabletId> {
    hosted
        .iter()
        .copied()
        .filter(|t| {
            tablets
                .get(t)
                .is_some_and(|tab| !tab.replicas.contains(&base_id))
        })
        .collect()
}

// === The execute half (ADR 0031 PR4) ========================================

/// How long [`Reconciler::tick`] waits for a group's driver to actually stop
/// after a [`HostAction::Release`]/[`HostAction::Reclaim`] calls
/// [`RaftKvNode::shutdown`], before giving up for this tick — mirrors
/// `animusd`'s old `CP_GC_STOP_TIMEOUT`. On timeout the handle is
/// re-registered via `on_host` and the teardown is **not** confirmed: `plan`
/// simply re-emits the identical action on the next tick (see
/// [`LocalState::confirm_torn_down`]'s doc), so nothing is ever erased while
/// the driver might still be writing.
pub const RECLAIM_STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// How often [`Reconciler::tick`] polls [`RaftKvNode::is_stopped`] while
/// waiting out [`RECLAIM_STOP_TIMEOUT`].
const RECLAIM_STOP_POLL: Duration = Duration::from_millis(50);

/// How long an [`HostAction::Absorb`] teardown waits for the absorbed group's
/// **local drain** — its own commit index covering its full local log, and its
/// engine-applied watermark covering that commit — before the fallback path
/// (ADR 0033). Generous enough to span a leader loss + re-election inside the
/// dissolving group (which is what re-advances a follower's commit over its
/// tail if the old leader's own teardown won the race). See
/// [`Reconciler::teardown`]'s Absorb arm for the exact contract and the
/// documented residual on timeout.
pub const ABSORB_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// How often the Absorb drain re-checks its condition while waiting out
/// [`ABSORB_DRAIN_TIMEOUT`].
const ABSORB_DRAIN_POLL: Duration = Duration::from_millis(20);

/// The execute half of the per-node tablet-host reconciler (ADR 0031 PR4):
/// owns every [`RaftKvNode`] this node hosts and drives it through [`plan`] on
/// each [`tick`](Self::tick) — the sync-core/async-driver split this crate
/// already uses elsewhere (`plan` decides, `Reconciler` does the I/O), applied
/// one level up to the whole tablet lifecycle instead of one Raft group.
/// Generic over `E`/`S` like the rest of this crate: no tokio-only primitive,
/// no wall clock beyond `env.now()`/`env.sleep()`, no `HashMap`/`HashSet` — ADR
/// 0003's determinism rules apply here exactly as everywhere else, even though
/// this is real per-node lifecycle logic rather than a `SimEnv`-only helper
/// (it is `SimEnv`-testable for exactly that reason, see this module's tests).
///
/// `Reconciler` is the **single writer** of "does this node host tablet T" —
/// `animusd` mirrors every hosting change into its own routing registry
/// (`ClusterEdgeState`) purely as a read-only reaction, via the
/// `on_host`/`on_teardown` hooks passed to [`new`](Self::new).
pub struct Reconciler<E: Env, S: StorageEngine> {
    env: E,
    storage: S,
    base_id: NodeId,
    /// Every tablet this node currently hosts a live `RaftKvNode` for — the
    /// authoritative hosting state (kept in lockstep with
    /// [`LocalState::hosted`], but holding the live handle, not just the id).
    hosted: BTreeMap<TabletId, RaftKvNode<E, S>>,
    state: LocalState,
    /// `table name -> StorageScope` prefix (`animusd`'s `escape(table)`) —
    /// supplied by the caller so this crate never duplicates the wire-edge
    /// key-escaping convention (see `StorageScope`'s own doc).
    prefix_for: PrefixFn,
    /// Mirror a fresh (or re-registered-after-a-timed-out-teardown) hosting
    /// into the caller's own routing registry. Called once per successful
    /// [`HostAction::Host`], and again if a `Release`/`Reclaim` teardown times
    /// out waiting for the driver to stop (the handle must stay reachable for
    /// routing while a later tick retries the teardown).
    on_host: OnHostFn<E, S>,
    /// Unregister a tablet from the caller's routing registry — called
    /// **before** shutting the group's driver down, mirroring
    /// `animusd::cp_gc_tablet`'s unregister-then-shutdown order (routing must
    /// stop seeing a group before its driver starts winding down).
    on_teardown: OnTeardownFn,
}

/// `table name -> StorageScope` prefix hook — see [`Reconciler`]'s
/// `prefix_for` field doc.
type PrefixFn = Box<dyn Fn(&str) -> Vec<u8> + Send + Sync>;
/// Fresh/re-registered-hosting mirror hook — see [`Reconciler`]'s `on_host`
/// field doc.
type OnHostFn<E, S> = Box<dyn Fn(TabletId, &RaftKvNode<E, S>) + Send + Sync>;
/// Teardown-unregister mirror hook — see [`Reconciler`]'s `on_teardown` field
/// doc.
type OnTeardownFn = Box<dyn Fn(TabletId) + Send + Sync>;

impl<E: Env, S: StorageEngine + 'static> Reconciler<E, S> {
    /// A fresh reconciler for one node. `env`/`storage` are this node's
    /// `raftkv` env and shared storage engine — every tablet's `RaftKvNode`
    /// this reconciler ever hosts runs on `env.clone()` (stream-addressed by
    /// the tablet id, ADR 0026 Stage B) and shares `storage.clone()` (ADR
    /// 0028); `base_id` is this node's identity in a tablet's replica set.
    /// `prefix_for` maps a table name to its `StorageScope` prefix (the
    /// caller's own escaping convention — this crate never invents one);
    /// `on_host`/`on_teardown` mirror hosting changes into the caller's own
    /// routing registry, letting `Reconciler` stay the single writer of
    /// hosting state while the caller's registry becomes a read-only mirror.
    pub fn new(
        env: E,
        storage: S,
        base_id: NodeId,
        prefix_for: impl Fn(&str) -> Vec<u8> + Send + Sync + 'static,
        on_host: impl Fn(TabletId, &RaftKvNode<E, S>) + Send + Sync + 'static,
        on_teardown: impl Fn(TabletId) + Send + Sync + 'static,
    ) -> Self {
        Self {
            env,
            storage,
            base_id,
            hosted: BTreeMap::new(),
            state: LocalState::default(),
            prefix_for: Box::new(prefix_for),
            on_host: Box::new(on_host),
            on_teardown: Box::new(on_teardown),
        }
    }

    /// This node's current [`LocalState`] — read-only, for a caller (or a
    /// test) that wants to observe convergence without reaching into the
    /// private `hosted` map.
    pub fn local_state(&self) -> &LocalState {
        &self.state
    }

    /// The live `RaftKvNode` this reconciler hosts for `tablet`, if any.
    pub fn hosted_node(&self, tablet: TabletId) -> Option<&RaftKvNode<E, S>> {
        self.hosted.get(&tablet)
    }

    /// One reconcile tick (ADR 0031): snapshot the impure facts this node's
    /// own hosted groups + engine can answer, call [`plan`] exactly once, then
    /// execute the returned actions **in the fixed order `plan` emits them**
    /// (`NarrowScope` → `Host` → `Reconfigure` → `Release`/`Reclaim`).
    ///
    /// The caller is responsible for the `last_applied() == 0` pre-recovery
    /// guard (a live control-plane `RaftNode` read this crate has no business
    /// taking, per [`plan`]'s own doc) — skip calling `tick` at all before
    /// replicated `Metadata` has recovered.
    pub async fn tick(&mut self, view: &MetadataView) {
        let facts = self.gather_facts(view).await;
        let (actions, next) = plan(view, &facts, &self.state, self.base_id.clone());
        self.state = next;

        for action in actions {
            match action {
                HostAction::ProposeSeal { tablet, range } => {
                    // ADR 0018 §2 amendment, fix: the seal-proposal side
                    // effect used to be bundled one-shot into this same
                    // tick's `NarrowScope`/`Absorb` handling below — now it's
                    // its own persistent, re-derived-every-tick action (see
                    // `TabletFacts::pending_seals`'s doc). A no-op if this
                    // node isn't currently the tablet's leader; harmless,
                    // since `plan` will keep re-emitting this action every
                    // tick the seal remains unobserved, so whichever replica
                    // eventually holds leadership gets its chance.
                    if let Some(node) = self.hosted.get(&tablet)
                        && node.is_leader()
                    {
                        node.propose_seal(range);
                    }
                }
                HostAction::NarrowScope { tablet, range } => {
                    if let Some(node) = self.hosted.get(&tablet) {
                        node.narrow_scope(range);
                    }
                }
                HostAction::WidenScope { tablet, range } => {
                    if let Some(node) = self.hosted.get(&tablet) {
                        node.widen_scope(range);
                    }
                }
                HostAction::Host {
                    tablet,
                    table,
                    range,
                    initial_formation,
                } => {
                    self.host(view, tablet, &table, range, initial_formation)
                        .await;
                }
                HostAction::Reconfigure {
                    tablet,
                    desired,
                    down,
                } => {
                    if let Some(node) = self.hosted.get(&tablet) {
                        node.reconfigure_step(&desired, &down);
                    }
                }
                HostAction::Release {
                    tablet,
                    erase_bound,
                } => {
                    self.teardown(tablet, TeardownKind::Release(erase_bound))
                        .await;
                }
                HostAction::Reclaim { tablet } => {
                    self.teardown(tablet, TeardownKind::Reclaim).await;
                }
                HostAction::Absorb { tablet } => {
                    self.teardown(tablet, TeardownKind::Absorb).await;
                }
            }
        }
    }

    /// Gather the [`TabletFacts`] [`plan`] needs: every currently-hosted
    /// tablet's live state (`is_leader`/`config_excludes_me`/`scope_range`),
    /// plus a `has_data` presence check for every not-yet-hosted candidate
    /// [`plan_join_host`] would place on this node — the one input `plan`
    /// can't gather itself (an async engine read).
    async fn gather_facts(&self, view: &MetadataView) -> BTreeMap<TabletId, TabletFacts> {
        let mut facts = BTreeMap::new();
        for (&tablet, node) in &self.hosted {
            let scope_range = node.scope_range();
            // ADR 0018 §2 amendment: only run the async seal-marker scan when
            // a widen is actually pending (current metadata range is wider
            // than this node's own live scope) — the steady-state common
            // case skips it entirely, same discipline as `has_data` below.
            let widen_seal_observed = match view.tablets.get(&tablet) {
                Some(t) if t.range != scope_range && is_subrange(&scope_range, &t.range) => {
                    match absorbed_range(&scope_range, &t.range) {
                        Some(needed) => {
                            let mut observed = false;
                            for (&absorbed, survivor) in &view.absorbed_by {
                                if *survivor == tablet
                                    && seal_covers(&self.storage, absorbed.0, &needed).await
                                {
                                    observed = true;
                                    break;
                                }
                            }
                            observed
                        }
                        None => false,
                    }
                }
                _ => false,
            };
            // ADR 0018 §2 amendment, fix (the split_cluster.rs livelock): a
            // persistent, re-derived-every-tick condition — never a one-shot
            // side effect of the tick that first narrows/tears down. Two
            // sources: (1) a split handoff — every child of *this* tablet
            // named in `view.split_parent`, whose range this node's engine
            // doesn't yet have a covering seal marker for; (2) an absorb
            // handoff — this tablet's own current scope, if this tablet
            // itself was absorbed (`view.merged`) and this node's engine
            // doesn't yet have a covering seal marker for it either. See
            // `TabletFacts::pending_seals`'s doc for the full argument.
            let mut pending_seals = Vec::new();
            for (&child, parent) in &view.split_parent {
                if *parent != tablet {
                    continue;
                }
                if let Some(child_t) = view.tablets.get(&child)
                    && !seal_covers(&self.storage, tablet.0, &child_t.range).await
                {
                    pending_seals.push(child_t.range.clone());
                }
            }
            if view.merged.contains(&tablet)
                && !seal_covers(&self.storage, tablet.0, &scope_range).await
            {
                pending_seals.push(scope_range.clone());
            }
            facts.insert(
                tablet,
                TabletFacts {
                    hosted: true,
                    is_leader: node.is_leader(),
                    config_excludes_me: !node.config().contains(&self.base_id),
                    scope_range: Some(scope_range),
                    has_data: false,
                    parent_seal_observed: false,
                    widen_seal_observed,
                    pending_seals,
                },
            );
        }
        for (&tablet, t) in &view.tablets {
            if self.state.hosted.contains(&tablet) {
                continue;
            }
            if plan_join_host(self.base_id.clone(), &t.replicas, t.epoch).is_none() {
                continue;
            }
            let scope = StorageScope::new(
                (self.prefix_for)(t.table.as_deref().unwrap_or_default()),
                t.range.clone(),
            );
            let has_data = scope.has_data(&self.storage).await;
            // ADR 0018 §2 amendment: a split child must observe its parent's
            // seal before hosting — see `MetadataView::split_parent`'s doc.
            let parent_seal_observed = match view.split_parent.get(&tablet) {
                Some(parent) => seal_covers(&self.storage, parent.0, &t.range).await,
                None => false,
            };
            facts.insert(
                tablet,
                TabletFacts {
                    has_data,
                    parent_seal_observed,
                    ..Default::default()
                },
            );
        }
        facts
    }

    /// Execute a [`HostAction::Host`]: stand up this node's member of
    /// `tablet`'s group, choosing the full voter config (`initial_formation`
    /// — a fresh tablet, or a restart with data already on disk) vs. a quiet
    /// non-voter joining an existing, already-led group (the others) —
    /// exactly `animusd::cp_join_host`'s decision. Synchronous within one
    /// tick (`start_hosted` only spawns the driver task and returns), so
    /// there is no in-flight "claimed but not yet registered" window to dedup
    /// against — unlike the old `minted`-claim-set loop, `self.hosted` is
    /// authoritative the instant this returns.
    async fn host(
        &mut self,
        view: &MetadataView,
        tablet: TabletId,
        table: &str,
        range: KeyRange,
        initial_formation: bool,
    ) {
        let Some(t) = view.tablets.get(&tablet) else {
            return;
        };
        let scope = StorageScope::new((self.prefix_for)(table), range);
        let full: Vec<NodeId> = t.replicas.clone();
        let others: Vec<NodeId> = full
            .iter()
            .filter(|id| **id != self.base_id)
            .cloned()
            .collect();
        let config = if initial_formation { full } else { others };
        // ADR 0018 §2 amendment: cross-group MVCC ordering no longer needs a
        // version-floor seed here — `RaftKvNode::start_hosted` already
        // witnesses this group's HLC off the shared engine's own
        // `latest_version()` at construction, and `plan`'s `parent_seal_
        // observed` gate (above) already proved this engine holds the
        // source's seal marker before this call ever happens.
        let node = RaftKvNode::start_hosted(
            self.env.clone(),
            config,
            self.storage.clone(),
            scope,
            tablet.0,
        );
        (self.on_host)(tablet, &node);
        self.hosted.insert(tablet, node);
    }

    /// Execute a [`HostAction::Release`]/[`HostAction::Reclaim`]/
    /// [`HostAction::Absorb`] — `animusd::cp_gc_tablet`'s exact teardown
    /// shape: unregister from the caller's routing registry first, shut the
    /// driver down and wait for it to actually stop (never touch data under a
    /// live driver), then handle data per `kind` (see [`TeardownKind`]) and
    /// delete the tablet's WAL file, and only then confirm the teardown to
    /// [`LocalState`] and drop the local handle. A timeout waiting for the
    /// driver to stop re-registers the handle (so routing keeps working) and
    /// leaves `state`/`hosted` untouched — `plan` re-emits the identical
    /// action next tick.
    ///
    /// **An `Absorb` teardown first DRAINS the group while its driver is still
    /// live** (ADR 0033): waits, bounded by [`ABSORB_DRAIN_TIMEOUT`], for this
    /// replica's own commit index to cover its full local log and for the
    /// engine-applied watermark to cover that commit — because unlike
    /// `Release`/`Reclaim` (whose teardowns erase the data anyway), an
    /// absorbed tablet's data is about to be **served** through the merge
    /// survivor's widened scope from this very engine. The apply task exits on
    /// `shutdown()` *without* draining committed-but-unapplied entries, and
    /// this teardown then deletes the group's Raft WAL — the only local copy
    /// of those entries — so skipping the drain silently and permanently loses
    /// acked writes on this node (the observed ADR 0033 regression: a write
    /// acked by the absorbed group's leader right before the merge, not yet
    /// engine-applied on the follower that hosts the survivor's leader, read
    /// back as a definitive "absent").
    ///
    /// **Also requires (ADR 0018 §2 amendment, fix) that this replica's own
    /// engine already contains a COMMITTED range-seal marker covering this
    /// tablet's own scope — not merely "nothing pending locally".** The seal
    /// itself is proposed elsewhere now (`plan`'s `HostAction::ProposeSeal`,
    /// re-derived every tick — see `TabletFacts::pending_seals`'s doc), but
    /// this teardown is what must never let a replica race ahead of it: the
    /// original "nothing pending locally" check alone is satisfied trivially
    /// by a quiescent replica that hasn't even received the seal proposal
    /// yet, which is exactly the bug a real multi-process deployment exposed
    /// deterministically (`split_cluster.rs` — see `docs/engineering-
    /// lessons.md`) — a fast follower could tear itself down (deleting its
    /// WAL, the only local copy) before the leader had even proposed the
    /// seal, dropping the group below quorum and permanently stranding it as
    /// accepted-but-never-committed.
    ///
    /// **This gate is self-supporting, not a deadlock risk**: requiring every
    /// absorbed replica to observe the seal locally before tearing down means
    /// every replica stays alive — and hence the quorum needed to commit the
    /// seal in the first place stays intact — for exactly as long as it takes
    /// the seal to actually commit. Nothing here can starve a healthy group:
    /// the same majority that would have accepted any other write accepts
    /// this one. If the group has genuinely lost quorum for an unrelated
    /// reason (a real double failure), this correctly stalls loudly rather
    /// than tearing down early — correctness over liveness, the doctrine this
    /// crate uses everywhere else a durability/visibility gate is at stake.
    ///
    /// On a drain timeout: the **original** stuck-apply escape hatch (proceed
    /// anyway with a loud warning) fires **only** when the seal is already
    /// locally observed and it's purely the engine-merge watermark lagging a
    /// local commit — never when the seal itself hasn't committed yet. A
    /// stuck seal always retries next tick instead (logged loudly on every
    /// retry past the timeout, so a genuinely quorum-dead absorbed group is
    /// visible to operators, not silently waved through).
    async fn teardown(&mut self, tablet: TabletId, kind: TeardownKind) {
        let Some(node) = self.hosted.remove(&tablet) else {
            return;
        };
        if matches!(kind, TeardownKind::Absorb) {
            // The seal proposal itself is no longer made here — see
            // `plan`'s `HostAction::ProposeSeal` and this fn's own doc above.
            let seal_range = node.scope_range();
            let deadline = self.env.now().saturating_add(ABSORB_DRAIN_TIMEOUT);
            loop {
                let commit = node.commit_index();
                let log_end = node.snapshot_index() + node.log_len() as u64;
                let raft_drained = commit >= log_end && node.engine_applied_index() >= commit;
                let seal_observed = seal_covers(&self.storage, tablet.0, &seal_range).await;
                if raft_drained && seal_observed {
                    break;
                }
                if self.env.now() >= deadline {
                    if seal_observed && node.engine_applied_index() >= node.commit_index() {
                        // The ORIGINAL stuck-apply residual this escape
                        // hatch exists for: the seal is already locally
                        // applied, only the engine merge itself lags the
                        // local commit — see doc above.
                        tracing::warn!(
                            tablet = tablet.0,
                            "reconciler: absorb drain timed out with an uncommitted local \
                             log tail; proceeding (entries committed elsewhere are retained \
                             by the replicas that drained)"
                        );
                        break;
                    }
                    // A stuck seal COMMIT never takes the escape hatch above
                    // — see this fn's doc for why that would reopen the
                    // exact race this gate exists to close.
                    tracing::warn!(
                        tablet = tablet.0,
                        seal_observed,
                        "reconciler: absorb drain/seal did not converge in time; retrying \
                         next tick (a stuck seal commit stalls this teardown indefinitely \
                         by design — correctness over liveness)"
                    );
                    self.hosted.insert(tablet, node);
                    return;
                }
                self.env.sleep(ABSORB_DRAIN_POLL).await;
            }
        }
        (self.on_teardown)(tablet);
        node.shutdown();
        let deadline = self.env.now().saturating_add(RECLAIM_STOP_TIMEOUT);
        while !node.is_stopped() {
            if self.env.now() >= deadline {
                tracing::warn!(
                    tablet = tablet.0,
                    "reconciler: group driver did not stop in time"
                );
                (self.on_host)(tablet, &node);
                self.hosted.insert(tablet, node);
                return;
            }
            self.env.sleep(RECLAIM_STOP_POLL).await;
        }

        match kind {
            TeardownKind::Release(erase_bound) => {
                // Bound the erase to the tablet's current replicated range —
                // see `HostAction::Release`'s doc for why the group's own
                // `StorageScope` cannot be trusted for this instead.
                node.narrow_scope(erase_bound);
                node.erase_scope().await;
            }
            TeardownKind::Reclaim => {
                node.erase_scope().await;
            }
            TeardownKind::Absorb => {
                // ADR 0033: never touch data — a merge survivor now owns
                // this range on the same shared engine. Only the driver and
                // its own WAL file go away.
            }
        }
        if let Err(e) = self.env.remove(&wal_file(tablet.0)).await {
            tracing::warn!(
                ?e,
                tablet = tablet.0,
                "reconciler: removing the tablet's WAL"
            );
        }

        self.state.confirm_torn_down(tablet);
    }
}

/// How [`Reconciler::teardown`] should treat a group's data once its driver
/// has stopped — the three ways a hosted tablet's lifecycle can end.
enum TeardownKind {
    /// [`HostAction::Release`]: narrow to the given bound, then erase —
    /// moved off this node while the tablet still exists elsewhere.
    Release(KeyRange),
    /// [`HostAction::Reclaim`]: erase the group's full existing scope — the
    /// tablet's whole table was dropped.
    Reclaim,
    /// [`HostAction::Absorb`] (ADR 0033): never erase — a merge survivor now
    /// owns this range on the same shared engine.
    Absorb,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> NodeId {
        nid(300)
    }

    fn tablet(id: u64, start: &[u8], end: Option<&[u8]>, replicas: Vec<NodeId>) -> Tablet {
        Tablet::new(
            TabletId(id),
            KeyRange::new(start.to_vec(), end.map(<[u8]>::to_vec)),
            replicas,
        )
    }

    fn tablet_for_table(
        id: u64,
        table: &str,
        start: &[u8],
        end: Option<&[u8]>,
        replicas: Vec<NodeId>,
    ) -> Tablet {
        Tablet::new_for_table(
            TabletId(id),
            table,
            KeyRange::new(start.to_vec(), end.map(<[u8]>::to_vec)),
            replicas,
        )
    }

    fn view(tablets: impl IntoIterator<Item = (u64, Tablet)>) -> MetadataView {
        MetadataView {
            tablets: tablets
                .into_iter()
                .map(|(id, t)| (TabletId(id), t))
                .collect(),
            ..Default::default()
        }
    }

    /// Like [`view`], but also marks `merged` tablet ids as merged-away (ADR
    /// 0033) — for tests exercising `HostAction::Absorb` instead of `Reclaim`.
    fn view_with_merged(
        tablets: impl IntoIterator<Item = (u64, Tablet)>,
        merged: impl IntoIterator<Item = u64>,
    ) -> MetadataView {
        MetadataView {
            merged: merged.into_iter().map(TabletId).collect(),
            ..view(tablets)
        }
    }

    /// Like [`view`], but also records `split_parent` provenance (ADR 0018 §2
    /// amendment) — for tests exercising the split-child seal gate.
    fn view_with_split_parent(
        tablets: impl IntoIterator<Item = (u64, Tablet)>,
        split_parent: impl IntoIterator<Item = (u64, u64)>,
    ) -> MetadataView {
        MetadataView {
            split_parent: split_parent
                .into_iter()
                .map(|(child, parent)| (TabletId(child), TabletId(parent)))
                .collect(),
            ..view(tablets)
        }
    }

    // === Parity ports of animusd::topology's tests =========================

    #[test]
    fn join_host_skips_a_non_replica() {
        assert_eq!(
            plan_join_host(base(), &[nid(301), nid(302)], Epoch::INITIAL),
            None
        );
    }

    #[test]
    fn join_host_forms_a_fresh_tablet_whole_or_split_child_the_same_way() {
        assert_eq!(
            plan_join_host(base(), &[base()], Epoch::INITIAL),
            Some(JoinHostPlan {
                initial_formation: true
            })
        );
    }

    #[test]
    fn join_host_joins_an_existing_group_as_non_voter() {
        assert_eq!(
            plan_join_host(base(), &[base()], Epoch::INITIAL.next()),
            Some(JoinHostPlan {
                initial_formation: false
            })
        );
    }

    #[test]
    fn reclaims_a_hosted_tablet_absent_from_the_map() {
        let tablets: BTreeMap<TabletId, Tablet> =
            [(TabletId(1), tablet(1, b"", None, vec![base()]))]
                .into_iter()
                .collect();
        let hosted: BTreeSet<TabletId> = [TabletId(1), TabletId(2)].into_iter().collect();
        assert_eq!(tablets_to_reclaim(&hosted, &tablets), vec![TabletId(2)]);
    }

    #[test]
    fn does_not_reclaim_a_still_present_tablet() {
        let tablets: BTreeMap<TabletId, Tablet> = [
            (TabletId(1), tablet(1, b"", None, vec![base()])),
            (TabletId(2), tablet(2, b"", None, vec![base()])),
        ]
        .into_iter()
        .collect();
        let hosted: BTreeSet<TabletId> = [TabletId(1), TabletId(2)].into_iter().collect();
        assert_eq!(
            tablets_to_reclaim(&hosted, &tablets),
            Vec::<TabletId>::new()
        );
    }

    #[test]
    fn reclaim_over_empty_hosted_set_is_empty() {
        let tablets: BTreeMap<TabletId, Tablet> = BTreeMap::new();
        assert_eq!(
            tablets_to_reclaim(&BTreeSet::new(), &tablets),
            Vec::<TabletId>::new()
        );
    }

    #[test]
    fn release_over_empty_hosted_set_is_empty() {
        let tablets: BTreeMap<TabletId, Tablet> =
            [(TabletId(1), tablet(1, b"", None, vec![base()]))]
                .into_iter()
                .collect();
        assert_eq!(
            tablets_to_release(&BTreeSet::new(), &tablets, base()),
            Vec::<TabletId>::new()
        );
    }

    #[test]
    fn does_not_release_a_tablet_this_node_is_still_a_replica_of() {
        let tablets: BTreeMap<TabletId, Tablet> = [(
            TabletId(1),
            tablet(1, b"", None, vec![base(), nid(301), nid(302)]),
        )]
        .into_iter()
        .collect();
        let hosted: BTreeSet<TabletId> = [TabletId(1)].into_iter().collect();
        assert_eq!(
            tablets_to_release(&hosted, &tablets, base()),
            Vec::<TabletId>::new()
        );
    }

    #[test]
    fn releases_a_hosted_present_tablet_this_node_is_no_longer_a_replica_of() {
        let tablets: BTreeMap<TabletId, Tablet> = [(
            TabletId(1),
            tablet(1, b"", None, vec![nid(301), nid(302), nid(303)]),
        )]
        .into_iter()
        .collect();
        let hosted: BTreeSet<TabletId> = [TabletId(1)].into_iter().collect();
        assert_eq!(
            tablets_to_release(&hosted, &tablets, base()),
            vec![TabletId(1)]
        );
    }

    #[test]
    fn does_not_release_a_hosted_tablet_that_is_absent() {
        let tablets: BTreeMap<TabletId, Tablet> = BTreeMap::new();
        let hosted: BTreeSet<TabletId> = [TabletId(1)].into_iter().collect();
        assert_eq!(
            tablets_to_release(&hosted, &tablets, base()),
            Vec::<TabletId>::new()
        );
    }

    #[test]
    fn reclaim_and_release_are_mutually_exclusive_port() {
        let tablets: BTreeMap<TabletId, Tablet> = [
            (TabletId(1), tablet(1, b"", None, vec![base(), nid(301)])),
            (TabletId(2), tablet(2, b"", None, vec![nid(301), nid(302)])),
        ]
        .into_iter()
        .collect();
        let hosted: BTreeSet<TabletId> = [TabletId(1), TabletId(2), TabletId(3)]
            .into_iter()
            .collect();

        let reclaim = tablets_to_reclaim(&hosted, &tablets);
        let release = tablets_to_release(&hosted, &tablets, base());

        assert_eq!(reclaim, vec![TabletId(3)]);
        assert_eq!(release, vec![TabletId(2)]);
        assert!(reclaim.iter().all(|t| !release.contains(t)));
    }

    // === plan(): reclaim/release mutual exclusion on arbitrary input =======

    #[test]
    fn plan_reclaim_and_release_are_mutually_exclusive_on_any_input() {
        let v = view([
            (1, tablet(1, b"", None, vec![base(), nid(301)])), // still a replica -> neither
            (2, tablet(2, b"", None, vec![nid(301), nid(302)])), // present, moved off -> release
                                                               // tablet 3 absent entirely -> reclaim
        ]);
        let state = LocalState {
            hosted: [TabletId(1), TabletId(2), TabletId(3)]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let facts: BTreeMap<TabletId, TabletFacts> = [(
            TabletId(2),
            TabletFacts {
                hosted: true,
                config_excludes_me: true,
                ..Default::default()
            },
        )]
        .into_iter()
        .collect();

        // Drive to RELEASE_CONFIRM_TICKS to force the release action too.
        let mut last = (Vec::new(), state);
        for _ in 0..RELEASE_CONFIRM_TICKS {
            last = plan(&v, &facts, &last.1, base());
        }
        let (actions, _next) = last;

        let reclaimed: Vec<TabletId> = actions
            .iter()
            .filter_map(|a| match a {
                HostAction::Reclaim { tablet } => Some(*tablet),
                _ => None,
            })
            .collect();
        let released: Vec<TabletId> = actions
            .iter()
            .filter_map(|a| match a {
                HostAction::Release { tablet, .. } => Some(*tablet),
                _ => None,
            })
            .collect();
        assert_eq!(reclaimed, vec![TabletId(3)]);
        assert_eq!(released, vec![TabletId(2)]);
        assert!(reclaimed.iter().all(|t| !released.contains(t)));
    }

    // === plan(): idempotence on a fully converged state =====================

    #[test]
    fn plan_on_a_converged_state_emits_no_actions_and_state_is_unchanged() {
        let v = view([(
            1,
            tablet_for_table(1, "t", b"", None, vec![base(), nid(301), nid(302)]),
        )]);
        let mut state = LocalState::default();
        state.hosted.insert(TabletId(1));
        let facts: BTreeMap<TabletId, TabletFacts> = [(
            TabletId(1),
            TabletFacts {
                hosted: true,
                is_leader: false, // exclude Reconfigure noise: see its own test below.
                config_excludes_me: false,
                scope_range: Some(KeyRange::new(b"".to_vec(), None)),
                has_data: false,
                parent_seal_observed: false,
                widen_seal_observed: false,
                pending_seals: Vec::new(),
            },
        )]
        .into_iter()
        .collect();

        let (actions, next) = plan(&v, &facts, &state, base());
        assert_eq!(actions, Vec::new());
        assert_eq!(next, state);
    }

    // === plan(): NarrowScope semantics =======================================

    #[test]
    fn plan_narrows_an_already_hosted_tablets_scope_when_metadata_range_shrank() {
        // Metadata narrowed to [a, m); the group's own scope is still the
        // pre-split-wide [a, z).
        let v = view([(1, tablet_for_table(1, "t", b"a", Some(b"m"), vec![base()]))]);
        let mut state = LocalState::default();
        state.hosted.insert(TabletId(1));
        let facts: BTreeMap<TabletId, TabletFacts> = [(
            TabletId(1),
            TabletFacts {
                hosted: true,
                scope_range: Some(KeyRange::new(b"a".to_vec(), Some(b"z".to_vec()))),
                ..Default::default()
            },
        )]
        .into_iter()
        .collect();

        let (actions, _next) = plan(&v, &facts, &state, base());
        assert_eq!(
            actions,
            vec![HostAction::NarrowScope {
                tablet: TabletId(1),
                range: KeyRange::new(b"a".to_vec(), Some(b"m".to_vec())),
            }]
        );
    }

    #[test]
    fn plan_widens_scope_when_metadata_range_grew_via_merge() {
        // ADR 0033: metadata range is WIDER than the group's current live
        // scope — this tablet was the surviving (`left`) side of a merge.
        // ADR 0018 §2 amendment: also requires `widen_seal_observed` (this
        // node's engine already contains the absorbed sibling's seal marker
        // covering the widened portion) — see the dedicated deferral test
        // below for the `false` case.
        let v = view([(1, tablet_for_table(1, "t", b"a", Some(b"z"), vec![base()]))]);
        let mut state = LocalState::default();
        state.hosted.insert(TabletId(1));
        let facts: BTreeMap<TabletId, TabletFacts> = [(
            TabletId(1),
            TabletFacts {
                hosted: true,
                scope_range: Some(KeyRange::new(b"a".to_vec(), Some(b"m".to_vec()))),
                widen_seal_observed: true,
                ..Default::default()
            },
        )]
        .into_iter()
        .collect();

        let (actions, _next) = plan(&v, &facts, &state, base());
        assert_eq!(
            actions,
            vec![HostAction::WidenScope {
                tablet: TabletId(1),
                range: KeyRange::new(b"a".to_vec(), Some(b"z".to_vec())),
            }]
        );
    }

    #[test]
    fn widen_is_deferred_until_the_absorbed_tablets_seal_is_observed() {
        // ADR 0018 §2 amendment: even with no absorb pending (unlike the
        // drain-gate test below), `plan` must not emit `WidenScope` until
        // `TabletFacts::widen_seal_observed` is true — the structural
        // replacement for the retired `version_floor` bump.
        let v = view([(1, tablet_for_table(1, "t", b"a", Some(b"z"), vec![base()]))]);
        let mut state = LocalState::default();
        state.hosted.insert(TabletId(1));
        let facts: BTreeMap<TabletId, TabletFacts> = [(
            TabletId(1),
            TabletFacts {
                hosted: true,
                scope_range: Some(KeyRange::new(b"a".to_vec(), Some(b"m".to_vec()))),
                widen_seal_observed: false,
                ..Default::default()
            },
        )]
        .into_iter()
        .collect();

        let (actions, _next) = plan(&v, &facts, &state, base());
        assert_eq!(
            actions,
            Vec::new(),
            "widen must be deferred until the absorbed seal is observed locally: {actions:?}"
        );
    }

    #[test]
    fn widen_is_deferred_while_the_absorbed_sibling_is_still_hosted() {
        // ADR 0033 drain-before-widen: this node still hosts the merged-away
        // tablet 2 (its Absorb teardown has not yet confirmed), so the
        // survivor's widen must be deferred — the absorb's local drain is what
        // guarantees the absorbed range's acked data is actually in this
        // node's engine before the survivor starts serving it.
        let v = view_with_merged(
            [(1, tablet_for_table(1, "t", b"a", Some(b"z"), vec![base()]))],
            [2],
        );
        let mut state = LocalState::default();
        state.hosted.insert(TabletId(1));
        state.hosted.insert(TabletId(2)); // absorb pending
        // ADR 0018 §2 amendment: `widen_seal_observed: true` here so this
        // test isolates the `absorbing` drain-gate specifically (not the
        // separate seal gate, covered by its own dedicated test above) —
        // the drain-gate must defer the widen even when the seal has
        // already landed.
        let facts: BTreeMap<TabletId, TabletFacts> = [(
            TabletId(1),
            TabletFacts {
                hosted: true,
                scope_range: Some(KeyRange::new(b"a".to_vec(), Some(b"m".to_vec()))),
                widen_seal_observed: true,
                ..Default::default()
            },
        )]
        .into_iter()
        .collect();

        let (actions, next) = plan(&v, &facts, &state, base());
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, HostAction::WidenScope { .. })),
            "widen must be deferred while the absorb is pending: {actions:?}"
        );
        assert!(
            actions.contains(&HostAction::Absorb {
                tablet: TabletId(2)
            }),
            "the pending absorb itself is still planned: {actions:?}"
        );

        // Once the absorb confirms (tablet 2 leaves `hosted`), the very next
        // plan call emits the widen.
        let mut confirmed = next;
        confirmed.confirm_torn_down(TabletId(2));
        let (actions2, _next2) = plan(&v, &facts, &confirmed, base());
        assert_eq!(
            actions2,
            vec![HostAction::WidenScope {
                tablet: TabletId(1),
                range: KeyRange::new(b"a".to_vec(), Some(b"z".to_vec())),
            }]
        );
    }

    #[test]
    fn plan_does_not_touch_scope_for_an_incomparable_range_mismatch() {
        // Neither a subset nor a superset of the current live scope — should
        // never happen in practice, but the planner must not guess a
        // direction (defensive: no NarrowScope, no WidenScope).
        let v = view([(1, tablet_for_table(1, "t", b"a", Some(b"k"), vec![base()]))]);
        let mut state = LocalState::default();
        state.hosted.insert(TabletId(1));
        let facts: BTreeMap<TabletId, TabletFacts> = [(
            TabletId(1),
            TabletFacts {
                hosted: true,
                scope_range: Some(KeyRange::new(b"b".to_vec(), Some(b"m".to_vec()))),
                ..Default::default()
            },
        )]
        .into_iter()
        .collect();

        let (actions, _next) = plan(&v, &facts, &state, base());
        assert_eq!(actions, Vec::new());
    }

    #[test]
    fn plan_does_not_narrow_when_ranges_already_match() {
        let v = view([(1, tablet_for_table(1, "t", b"a", Some(b"m"), vec![base()]))]);
        let mut state = LocalState::default();
        state.hosted.insert(TabletId(1));
        let facts: BTreeMap<TabletId, TabletFacts> = [(
            TabletId(1),
            TabletFacts {
                hosted: true,
                scope_range: Some(KeyRange::new(b"a".to_vec(), Some(b"m".to_vec()))),
                ..Default::default()
            },
        )]
        .into_iter()
        .collect();

        let (actions, _next) = plan(&v, &facts, &state, base());
        assert_eq!(actions, Vec::new());
    }

    // === plan(): Host semantics ==============================================

    #[test]
    fn plan_hosts_a_fresh_replica_tablet_at_most_once() {
        let v = view([(1, tablet_for_table(1, "t", b"", None, vec![base()]))]);
        let state = LocalState::default();

        let (actions, next) = plan(&v, &BTreeMap::new(), &state, base());
        assert_eq!(
            actions,
            vec![HostAction::Host {
                tablet: TabletId(1),
                table: "t".to_string(),
                range: KeyRange::whole(),
                initial_formation: true,
            }]
        );
        assert!(next.hosted.contains(&TabletId(1)));

        // A second call with the tablet now in `hosted` (and no facts,
        // meaning not-yet-actually-registered) must not re-plan a Host.
        let (actions2, _next2) = plan(&v, &BTreeMap::new(), &next, base());
        assert_eq!(actions2, Vec::new());
    }

    #[test]
    fn plan_defers_hosting_a_split_child_until_its_parents_seal_is_observed() {
        // ADR 0018 §2 amendment: a candidate named in `split_parent` must not
        // host until `TabletFacts::parent_seal_observed` is true — the
        // structural replacement for the retired `version_floor` seeding.
        let v = view_with_split_parent(
            [(2, tablet_for_table(2, "t", b"m", None, vec![base()]))],
            [(2, 1)],
        );
        let state = LocalState::default();

        // No fact at all (equivalent to `parent_seal_observed: false`): the
        // child must not host yet.
        let (actions, next) = plan(&v, &BTreeMap::new(), &state, base());
        assert_eq!(
            actions,
            Vec::new(),
            "a split child must not host before its parent's seal is observed: {actions:?}"
        );
        assert!(
            !next.hosted.contains(&TabletId(2)),
            "a deferred Host must not mark the tablet hosted"
        );

        // Once the fact flips true, the very next `plan` call hosts it.
        let facts: BTreeMap<TabletId, TabletFacts> = [(
            TabletId(2),
            TabletFacts {
                parent_seal_observed: true,
                ..Default::default()
            },
        )]
        .into_iter()
        .collect();
        let (actions2, next2) = plan(&v, &facts, &next, base());
        assert_eq!(
            actions2,
            vec![HostAction::Host {
                tablet: TabletId(2),
                table: "t".to_string(),
                range: KeyRange::new(b"m".to_vec(), None),
                initial_formation: true,
            }]
        );
        assert!(next2.hosted.contains(&TabletId(2)));
    }

    #[test]
    fn plan_joins_an_existing_group_as_non_voter() {
        let mut t = tablet_for_table(1, "t", b"", None, vec![base()]);
        t.epoch = Epoch::INITIAL.next();
        let v = view([(1, t)]);
        let state = LocalState::default();
        let (actions, _next) = plan(&v, &BTreeMap::new(), &state, base());
        assert_eq!(
            actions,
            vec![HostAction::Host {
                tablet: TabletId(1),
                table: "t".to_string(),
                range: KeyRange::whole(),
                initial_formation: false,
            }]
        );
    }

    #[test]
    fn plan_upgrades_a_restart_to_initial_formation_via_has_data() {
        // Bumped epoch (looks like "join as spare"), but this node already
        // has data on disk for the tablet — the restart case.
        let mut t = tablet_for_table(1, "t", b"", None, vec![base()]);
        t.epoch = Epoch::INITIAL.next();
        let v = view([(1, t)]);
        let state = LocalState::default();
        let facts: BTreeMap<TabletId, TabletFacts> = [(
            TabletId(1),
            TabletFacts {
                has_data: true,
                ..Default::default()
            },
        )]
        .into_iter()
        .collect();

        let (actions, _next) = plan(&v, &facts, &state, base());
        assert_eq!(
            actions,
            vec![HostAction::Host {
                tablet: TabletId(1),
                table: "t".to_string(),
                range: KeyRange::whole(),
                initial_formation: true,
            }]
        );
    }

    #[test]
    fn plan_does_not_host_a_non_replica_tablet() {
        let v = view([(1, tablet(1, b"", None, vec![nid(301), nid(302)]))]);
        let state = LocalState::default();
        let (actions, next) = plan(&v, &BTreeMap::new(), &state, base());
        assert_eq!(actions, Vec::new());
        assert!(next.hosted.is_empty());
    }

    // === plan(): Reconfigure semantics =======================================

    #[test]
    fn plan_reconfigures_only_tablets_this_node_leads_carrying_the_down_set() {
        let mut v = view([
            (1, tablet(1, b"", None, vec![base(), nid(301)])),
            (2, tablet(2, b"", None, vec![base(), nid(302)])),
        ]);
        v.down.insert(nid(302));
        let state = LocalState {
            hosted: [TabletId(1), TabletId(2)].into_iter().collect(),
            ..Default::default()
        };
        let facts: BTreeMap<TabletId, TabletFacts> = [
            (
                TabletId(1),
                TabletFacts {
                    hosted: true,
                    is_leader: true,
                    ..Default::default()
                },
            ),
            (
                TabletId(2),
                TabletFacts {
                    hosted: true,
                    is_leader: false,
                    ..Default::default()
                },
            ),
        ]
        .into_iter()
        .collect();

        let (actions, _next) = plan(&v, &facts, &state, base());
        assert_eq!(
            actions,
            vec![HostAction::Reconfigure {
                tablet: TabletId(1),
                desired: [base(), nid(301)].into_iter().collect(),
                down: [302].into_iter().map(nid).collect(),
            }]
        );
    }

    #[test]
    fn plan_does_not_reconfigure_when_not_hosted_even_if_is_leader_is_set() {
        // Defensive: a facts entry claiming leadership without `hosted` must
        // never drive a Reconfigure (an impossible-but-guarded input shape).
        let v = view([(1, tablet(1, b"", None, vec![base()]))]);
        let state = LocalState::default();
        let facts: BTreeMap<TabletId, TabletFacts> = [(
            TabletId(1),
            TabletFacts {
                hosted: false,
                is_leader: true,
                ..Default::default()
            },
        )]
        .into_iter()
        .collect();
        let (actions, _next) = plan(&v, &facts, &state, base());
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, HostAction::Reconfigure { .. }))
        );
    }

    // === plan(): Release dampener semantics ==================================

    fn released_tablet_setup() -> (MetadataView, LocalState, BTreeMap<TabletId, TabletFacts>) {
        let v = view([(1, tablet(1, b"", None, vec![nid(301), nid(302)]))]); // base() moved off
        let mut state = LocalState::default();
        state.hosted.insert(TabletId(1));
        let facts: BTreeMap<TabletId, TabletFacts> = [(
            TabletId(1),
            TabletFacts {
                hosted: true,
                config_excludes_me: true,
                ..Default::default()
            },
        )]
        .into_iter()
        .collect();
        (v, state, facts)
    }

    #[test]
    fn release_requires_exactly_release_confirm_ticks_consecutive_qualifying_plans() {
        let (v, state, facts) = released_tablet_setup();

        let mut cur = state;
        for tick in 1..RELEASE_CONFIRM_TICKS {
            let (actions, next) = plan(&v, &facts, &cur, base());
            assert_eq!(
                actions,
                Vec::new(),
                "tick {tick} of {RELEASE_CONFIRM_TICKS} must not release yet"
            );
            // Still hosted (release action never fired) and still tracked.
            assert!(next.hosted.contains(&TabletId(1)));
            assert_eq!(
                next.pending_release.get(&TabletId(1)).map(|(_, t)| *t),
                Some(tick)
            );
            cur = next;
        }

        let (actions, next) = plan(&v, &facts, &cur, base());
        assert_eq!(
            actions,
            vec![HostAction::Release {
                tablet: TabletId(1),
                erase_bound: KeyRange::whole(),
            }]
        );
        // The dampener entry is cleared once confirmed (mirrors the real
        // loop's `pending.remove` right before teardown); `hosted` is left
        // untouched until the caller confirms the teardown succeeded.
        assert!(!next.pending_release.contains_key(&TabletId(1)));
        assert!(next.hosted.contains(&TabletId(1)));
    }

    #[test]
    fn an_epoch_bump_mid_count_resets_the_release_dampener() {
        let (v, state, facts) = released_tablet_setup();

        let (_actions, next1) = plan(&v, &facts, &state, base());
        assert_eq!(
            next1.pending_release.get(&TabletId(1)).map(|(_, t)| *t),
            Some(1)
        );

        // Epoch bumps (e.g. a re-add elsewhere then dropped again) between
        // ticks — same replica set, different epoch.
        let mut bumped = v.clone();
        let t = bumped.tablets.get_mut(&TabletId(1)).expect("tablet");
        t.epoch = t.epoch.next();

        let (_actions, next2) = plan(&bumped, &facts, &next1, base());
        assert_eq!(
            next2.pending_release.get(&TabletId(1)).map(|(_, t)| *t),
            Some(1),
            "an epoch change must restart the confirm count, not advance it"
        );
    }

    #[test]
    fn a_re_add_cancels_a_pending_release() {
        let (v, state, facts) = released_tablet_setup();

        let (_actions, next1) = plan(&v, &facts, &state, base());
        assert!(next1.pending_release.contains_key(&TabletId(1)));

        // The tablet's replica set gains base() back (a re-add).
        let mut readded = v;
        readded
            .tablets
            .get_mut(&TabletId(1))
            .expect("tablet")
            .replicas = vec![base(), nid(301), nid(302)];

        let (actions, next2) = plan(&readded, &facts, &next1, base());
        assert!(
            !next2.pending_release.contains_key(&TabletId(1)),
            "a re-add must cancel the pending release"
        );
        assert!(
            actions.is_empty()
                || !actions
                    .iter()
                    .any(|a| matches!(a, HostAction::Release { .. }))
        );
    }

    #[test]
    fn release_erase_bound_is_always_the_current_metadata_range_never_the_stale_scope_fact() {
        // The group's own live scope fact is stale-wide ([a, z)); the
        // tablet's current metadata range has since narrowed to [a, m) by a
        // split. The release must bound its erase to the CURRENT metadata
        // range, never the stale scope fact — this is the regression the
        // sibling-corruption bug (root CLAUDE.md) needs provable in a unit
        // test.
        let v = view([(1, tablet(1, b"a", Some(b"m"), vec![nid(301), nid(302)]))]); // base() moved off
        let mut state = LocalState::default();
        state.hosted.insert(TabletId(1));
        let facts: BTreeMap<TabletId, TabletFacts> = [(
            TabletId(1),
            TabletFacts {
                hosted: true,
                config_excludes_me: true,
                scope_range: Some(KeyRange::new(b"a".to_vec(), Some(b"z".to_vec()))),
                ..Default::default()
            },
        )]
        .into_iter()
        .collect();

        let mut cur = state;
        let mut last_actions = Vec::new();
        for _ in 0..RELEASE_CONFIRM_TICKS {
            let (actions, next) = plan(&v, &facts, &cur, base());
            last_actions = actions;
            cur = next;
        }

        assert_eq!(
            last_actions,
            vec![HostAction::Release {
                tablet: TabletId(1),
                erase_bound: KeyRange::new(b"a".to_vec(), Some(b"m".to_vec())),
            }]
        );
    }

    #[test]
    fn release_condition_resets_when_not_yet_excluded_by_own_durable_config() {
        // Present, moved off in Metadata — but this node's own durable Raft
        // config has not caught up yet (still lists itself), or there is no
        // local handle at all. Must never advance the confirm counter.
        let v = view([(1, tablet(1, b"", None, vec![nid(301), nid(302)]))]);
        let mut state = LocalState::default();
        state.hosted.insert(TabletId(1));

        // No facts entry at all: `hosted` fact is false -> "not excluded".
        let (actions, next) = plan(&v, &BTreeMap::new(), &state, base());
        assert_eq!(actions, Vec::new());
        assert!(!next.pending_release.contains_key(&TabletId(1)));

        // A handle exists, but its own config still lists this node.
        let facts: BTreeMap<TabletId, TabletFacts> = [(
            TabletId(1),
            TabletFacts {
                hosted: true,
                config_excludes_me: false,
                ..Default::default()
            },
        )]
        .into_iter()
        .collect();
        let (actions, next) = plan(&v, &facts, &state, base());
        assert_eq!(actions, Vec::new());
        assert!(!next.pending_release.contains_key(&TabletId(1)));
    }

    // === LocalState::confirm_torn_down =======================================

    #[test]
    fn confirm_torn_down_removes_hosted_and_pending_release_entries() {
        let mut state = LocalState::default();
        state.hosted.insert(TabletId(1));
        state
            .pending_release
            .insert(TabletId(1), (Epoch::INITIAL, 2));

        state.confirm_torn_down(TabletId(1));

        assert!(!state.hosted.contains(&TabletId(1)));
        assert!(!state.pending_release.contains_key(&TabletId(1)));
    }

    #[test]
    fn a_pending_reclaim_is_replanned_until_confirmed_torn_down() {
        // The tablet is dropped from the map entirely (whole table gone).
        let v = view([]);
        let mut state = LocalState::default();
        state.hosted.insert(TabletId(1));

        let (actions1, next1) = plan(&v, &BTreeMap::new(), &state, base());
        assert_eq!(
            actions1,
            vec![HostAction::Reclaim {
                tablet: TabletId(1)
            }]
        );
        // Not removed automatically — the caller's teardown may still fail.
        assert!(next1.hosted.contains(&TabletId(1)));

        // Retried identically on a second call before confirmation.
        let (actions2, next2) = plan(&v, &BTreeMap::new(), &next1, base());
        assert_eq!(
            actions2,
            vec![HostAction::Reclaim {
                tablet: TabletId(1)
            }]
        );

        // Once the caller confirms the teardown, it stops being replanned.
        let mut confirmed = next2;
        confirmed.confirm_torn_down(TabletId(1));
        let (actions3, _next3) = plan(&v, &BTreeMap::new(), &confirmed, base());
        assert_eq!(actions3, Vec::new());
    }

    // === plan(): Absorb vs Reclaim (ADR 0033 tablet merge) ==================

    #[test]
    fn a_merged_away_tablet_is_absorbed_not_reclaimed() {
        // Tablet 2 vanished from the map (merged into 1), recorded in `merged`.
        let v = view_with_merged([], [2]);
        let mut state = LocalState::default();
        state.hosted.insert(TabletId(2));

        let (actions, next) = plan(&v, &BTreeMap::new(), &state, base());
        assert_eq!(
            actions,
            vec![HostAction::Absorb {
                tablet: TabletId(2)
            }]
        );
        // Not removed automatically — mirrors Reclaim/Release: the caller's
        // teardown may still fail, so `plan` re-emits until confirmed.
        assert!(next.hosted.contains(&TabletId(2)));

        let (actions2, _next2) = plan(&v, &BTreeMap::new(), &next, base());
        assert_eq!(
            actions2,
            vec![HostAction::Absorb {
                tablet: TabletId(2)
            }],
            "an unconfirmed absorb must be replanned identically"
        );

        let mut confirmed = next;
        confirmed.confirm_torn_down(TabletId(2));
        let (actions3, _next3) = plan(&v, &BTreeMap::new(), &confirmed, base());
        assert_eq!(actions3, Vec::new());
    }

    #[test]
    fn a_dropped_tablet_absent_from_merged_is_reclaimed_not_absorbed() {
        // Same "vanished from the map" shape as the absorb case above, but
        // NOT recorded in `merged` — a genuine table drop.
        let v = view([]);
        let mut state = LocalState::default();
        state.hosted.insert(TabletId(2));

        let (actions, _next) = plan(&v, &BTreeMap::new(), &state, base());
        assert_eq!(
            actions,
            vec![HostAction::Reclaim {
                tablet: TabletId(2)
            }]
        );
    }

    #[test]
    fn absorb_and_reclaim_partition_vanished_tablets_by_the_merged_set() {
        // Three hosted tablets, all vanished from the map: one merged-away,
        // one genuinely dropped, one still present (untouched).
        let v = view_with_merged([(3, tablet(3, b"", None, vec![base()]))], [1]);
        let state = LocalState {
            hosted: [TabletId(1), TabletId(2), TabletId(3)]
                .into_iter()
                .collect(),
            ..Default::default()
        };

        let (actions, _next) = plan(&v, &BTreeMap::new(), &state, base());
        let absorbed: Vec<TabletId> = actions
            .iter()
            .filter_map(|a| match a {
                HostAction::Absorb { tablet } => Some(*tablet),
                _ => None,
            })
            .collect();
        let reclaimed: Vec<TabletId> = actions
            .iter()
            .filter_map(|a| match a {
                HostAction::Reclaim { tablet } => Some(*tablet),
                _ => None,
            })
            .collect();
        assert_eq!(absorbed, vec![TabletId(1)]);
        assert_eq!(reclaimed, vec![TabletId(2)]);
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, HostAction::Release { .. } | HostAction::Host { .. }))
        );
    }
}
