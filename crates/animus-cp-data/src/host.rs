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

use animus_env::NodeId;
use animus_tablet::{Epoch, KeyRange, Tablet, TabletId};

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
/// (never a widen).
fn is_subrange(inner: &KeyRange, outer: &KeyRange) -> bool {
    if inner.start < outer.start {
        return false;
    }
    match (&inner.end, &outer.end) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(inner_end), Some(outer_end)) => inner_end <= outer_end,
    }
}

/// One reconciling action [`plan`] can emit for a single tablet. A caller
/// (`animusd`, PR4) executes these against its own live `ProdEnv` state;
/// `plan` itself performs no I/O.
///
/// Emitted in a fixed overall order — every [`NarrowScope`](Self::NarrowScope)
/// action, then every [`Host`](Self::Host), then every
/// [`Reconfigure`](Self::Reconfigure), then every
/// [`Release`](Self::Release)/[`Reclaim`](Self::Reclaim) — mirroring the
/// existing loops' relative priority (narrow a still-hosted tablet's scope
/// before deciding anything else about it; stand up a newly-placed tablet
/// before reconfiguring anyone; reconcile membership before tearing anything
/// down). Within each group, tablets are emitted in `TabletId` order (a
/// `BTreeMap` iteration is deterministic on every node).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostAction {
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

    // --- Phase 1: narrow an already-hosted tablet's scope, or host a
    // newly-placed one. `to_host` batches the Host actions so every
    // NarrowScope precedes every Host in the returned order, even though
    // both are decided from the same tablet-map walk.
    let mut to_host = Vec::new();
    for (&tablet, t) in &view.tablets {
        let Some(join_plan) = plan_join_host(base_id, &t.replicas, t.epoch) else {
            continue;
        };
        if next.hosted.contains(&tablet) {
            if let Some(f) = facts.get(&tablet) {
                if f.hosted {
                    if let Some(current) = &f.scope_range {
                        if t.range != *current && is_subrange(&t.range, current) {
                            actions.push(HostAction::NarrowScope {
                                tablet,
                                range: t.range.clone(),
                            });
                        }
                    }
                }
            }
        } else {
            to_host.push((tablet, t, join_plan));
        }
    }
    for (tablet, t, join_plan) in to_host {
        let has_data = facts.get(&tablet).is_some_and(|f| f.has_data);
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
                desired: t.replicas.iter().copied().collect(),
                down: view.down.clone(),
            });
        }
    }

    // --- Phase 3: reclaim (absent) / release (present, moved off) — mutually
    // exclusive on the same `mine` input.
    let mine: BTreeSet<TabletId> = next.hosted.clone();

    for tablet in tablets_to_reclaim_set(&mine, &view.tablets) {
        actions.push(HostAction::Reclaim { tablet });
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

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: NodeId = 300;

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
            down: BTreeSet::new(),
        }
    }

    // === Parity ports of animusd::topology's tests =========================

    #[test]
    fn join_host_skips_a_non_replica() {
        assert_eq!(plan_join_host(BASE, &[301, 302], Epoch::INITIAL), None);
    }

    #[test]
    fn join_host_forms_a_fresh_tablet_whole_or_split_child_the_same_way() {
        assert_eq!(
            plan_join_host(BASE, &[BASE], Epoch::INITIAL),
            Some(JoinHostPlan {
                initial_formation: true
            })
        );
    }

    #[test]
    fn join_host_joins_an_existing_group_as_non_voter() {
        assert_eq!(
            plan_join_host(BASE, &[BASE], Epoch::INITIAL.next()),
            Some(JoinHostPlan {
                initial_formation: false
            })
        );
    }

    #[test]
    fn reclaims_a_hosted_tablet_absent_from_the_map() {
        let tablets: BTreeMap<TabletId, Tablet> = [(TabletId(1), tablet(1, b"", None, vec![BASE]))]
            .into_iter()
            .collect();
        let hosted: BTreeSet<TabletId> = [TabletId(1), TabletId(2)].into_iter().collect();
        assert_eq!(tablets_to_reclaim(&hosted, &tablets), vec![TabletId(2)]);
    }

    #[test]
    fn does_not_reclaim_a_still_present_tablet() {
        let tablets: BTreeMap<TabletId, Tablet> = [
            (TabletId(1), tablet(1, b"", None, vec![BASE])),
            (TabletId(2), tablet(2, b"", None, vec![BASE])),
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
        let tablets: BTreeMap<TabletId, Tablet> = [(TabletId(1), tablet(1, b"", None, vec![BASE]))]
            .into_iter()
            .collect();
        assert_eq!(
            tablets_to_release(&BTreeSet::new(), &tablets, BASE),
            Vec::<TabletId>::new()
        );
    }

    #[test]
    fn does_not_release_a_tablet_this_node_is_still_a_replica_of() {
        let tablets: BTreeMap<TabletId, Tablet> =
            [(TabletId(1), tablet(1, b"", None, vec![BASE, 301, 302]))]
                .into_iter()
                .collect();
        let hosted: BTreeSet<TabletId> = [TabletId(1)].into_iter().collect();
        assert_eq!(
            tablets_to_release(&hosted, &tablets, BASE),
            Vec::<TabletId>::new()
        );
    }

    #[test]
    fn releases_a_hosted_present_tablet_this_node_is_no_longer_a_replica_of() {
        let tablets: BTreeMap<TabletId, Tablet> =
            [(TabletId(1), tablet(1, b"", None, vec![301, 302, 303]))]
                .into_iter()
                .collect();
        let hosted: BTreeSet<TabletId> = [TabletId(1)].into_iter().collect();
        assert_eq!(
            tablets_to_release(&hosted, &tablets, BASE),
            vec![TabletId(1)]
        );
    }

    #[test]
    fn does_not_release_a_hosted_tablet_that_is_absent() {
        let tablets: BTreeMap<TabletId, Tablet> = BTreeMap::new();
        let hosted: BTreeSet<TabletId> = [TabletId(1)].into_iter().collect();
        assert_eq!(
            tablets_to_release(&hosted, &tablets, BASE),
            Vec::<TabletId>::new()
        );
    }

    #[test]
    fn reclaim_and_release_are_mutually_exclusive_port() {
        let tablets: BTreeMap<TabletId, Tablet> = [
            (TabletId(1), tablet(1, b"", None, vec![BASE, 301])),
            (TabletId(2), tablet(2, b"", None, vec![301, 302])),
        ]
        .into_iter()
        .collect();
        let hosted: BTreeSet<TabletId> = [TabletId(1), TabletId(2), TabletId(3)]
            .into_iter()
            .collect();

        let reclaim = tablets_to_reclaim(&hosted, &tablets);
        let release = tablets_to_release(&hosted, &tablets, BASE);

        assert_eq!(reclaim, vec![TabletId(3)]);
        assert_eq!(release, vec![TabletId(2)]);
        assert!(reclaim.iter().all(|t| !release.contains(t)));
    }

    // === plan(): reclaim/release mutual exclusion on arbitrary input =======

    #[test]
    fn plan_reclaim_and_release_are_mutually_exclusive_on_any_input() {
        let v = view([
            (1, tablet(1, b"", None, vec![BASE, 301])), // still a replica -> neither
            (2, tablet(2, b"", None, vec![301, 302])),  // present, moved off -> release
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
            last = plan(&v, &facts, &last.1, BASE);
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
        let v = view([(1, tablet_for_table(1, "t", b"", None, vec![BASE, 301, 302]))]);
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
            },
        )]
        .into_iter()
        .collect();

        let (actions, next) = plan(&v, &facts, &state, BASE);
        assert_eq!(actions, Vec::new());
        assert_eq!(next, state);
    }

    // === plan(): NarrowScope semantics =======================================

    #[test]
    fn plan_narrows_an_already_hosted_tablets_scope_when_metadata_range_shrank() {
        // Metadata narrowed to [a, m); the group's own scope is still the
        // pre-split-wide [a, z).
        let v = view([(1, tablet_for_table(1, "t", b"a", Some(b"m"), vec![BASE]))]);
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

        let (actions, _next) = plan(&v, &facts, &state, BASE);
        assert_eq!(
            actions,
            vec![HostAction::NarrowScope {
                tablet: TabletId(1),
                range: KeyRange::new(b"a".to_vec(), Some(b"m".to_vec())),
            }]
        );
    }

    #[test]
    fn plan_never_emits_a_widening_narrow_scope() {
        // Metadata range is WIDER than the group's current live scope (should
        // never happen in practice, but the planner must never widen).
        let v = view([(1, tablet_for_table(1, "t", b"a", Some(b"z"), vec![BASE]))]);
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

        let (actions, _next) = plan(&v, &facts, &state, BASE);
        assert_eq!(actions, Vec::new());
    }

    #[test]
    fn plan_does_not_narrow_when_ranges_already_match() {
        let v = view([(1, tablet_for_table(1, "t", b"a", Some(b"m"), vec![BASE]))]);
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

        let (actions, _next) = plan(&v, &facts, &state, BASE);
        assert_eq!(actions, Vec::new());
    }

    // === plan(): Host semantics ==============================================

    #[test]
    fn plan_hosts_a_fresh_replica_tablet_at_most_once() {
        let v = view([(1, tablet_for_table(1, "t", b"", None, vec![BASE]))]);
        let state = LocalState::default();

        let (actions, next) = plan(&v, &BTreeMap::new(), &state, BASE);
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
        let (actions2, _next2) = plan(&v, &BTreeMap::new(), &next, BASE);
        assert_eq!(actions2, Vec::new());
    }

    #[test]
    fn plan_joins_an_existing_group_as_non_voter() {
        let mut t = tablet_for_table(1, "t", b"", None, vec![BASE]);
        t.epoch = Epoch::INITIAL.next();
        let v = view([(1, t)]);
        let state = LocalState::default();
        let (actions, _next) = plan(&v, &BTreeMap::new(), &state, BASE);
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
        let mut t = tablet_for_table(1, "t", b"", None, vec![BASE]);
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

        let (actions, _next) = plan(&v, &facts, &state, BASE);
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
        let v = view([(1, tablet(1, b"", None, vec![301, 302]))]);
        let state = LocalState::default();
        let (actions, next) = plan(&v, &BTreeMap::new(), &state, BASE);
        assert_eq!(actions, Vec::new());
        assert!(next.hosted.is_empty());
    }

    // === plan(): Reconfigure semantics =======================================

    #[test]
    fn plan_reconfigures_only_tablets_this_node_leads_carrying_the_down_set() {
        let mut v = view([
            (1, tablet(1, b"", None, vec![BASE, 301])),
            (2, tablet(2, b"", None, vec![BASE, 302])),
        ]);
        v.down.insert(302);
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

        let (actions, _next) = plan(&v, &facts, &state, BASE);
        assert_eq!(
            actions,
            vec![HostAction::Reconfigure {
                tablet: TabletId(1),
                desired: [BASE, 301].into_iter().collect(),
                down: [302].into_iter().collect(),
            }]
        );
    }

    #[test]
    fn plan_does_not_reconfigure_when_not_hosted_even_if_is_leader_is_set() {
        // Defensive: a facts entry claiming leadership without `hosted` must
        // never drive a Reconfigure (an impossible-but-guarded input shape).
        let v = view([(1, tablet(1, b"", None, vec![BASE]))]);
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
        let (actions, _next) = plan(&v, &facts, &state, BASE);
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, HostAction::Reconfigure { .. }))
        );
    }

    // === plan(): Release dampener semantics ==================================

    fn released_tablet_setup() -> (MetadataView, LocalState, BTreeMap<TabletId, TabletFacts>) {
        let v = view([(1, tablet(1, b"", None, vec![301, 302]))]); // BASE moved off
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
            let (actions, next) = plan(&v, &facts, &cur, BASE);
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

        let (actions, next) = plan(&v, &facts, &cur, BASE);
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

        let (_actions, next1) = plan(&v, &facts, &state, BASE);
        assert_eq!(
            next1.pending_release.get(&TabletId(1)).map(|(_, t)| *t),
            Some(1)
        );

        // Epoch bumps (e.g. a re-add elsewhere then dropped again) between
        // ticks — same replica set, different epoch.
        let mut bumped = v.clone();
        let t = bumped.tablets.get_mut(&TabletId(1)).expect("tablet");
        t.epoch = t.epoch.next();

        let (_actions, next2) = plan(&bumped, &facts, &next1, BASE);
        assert_eq!(
            next2.pending_release.get(&TabletId(1)).map(|(_, t)| *t),
            Some(1),
            "an epoch change must restart the confirm count, not advance it"
        );
    }

    #[test]
    fn a_re_add_cancels_a_pending_release() {
        let (v, state, facts) = released_tablet_setup();

        let (_actions, next1) = plan(&v, &facts, &state, BASE);
        assert!(next1.pending_release.contains_key(&TabletId(1)));

        // The tablet's replica set gains BASE back (a re-add).
        let mut readded = v;
        readded
            .tablets
            .get_mut(&TabletId(1))
            .expect("tablet")
            .replicas = vec![BASE, 301, 302];

        let (actions, next2) = plan(&readded, &facts, &next1, BASE);
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
        let v = view([(1, tablet(1, b"a", Some(b"m"), vec![301, 302]))]); // BASE moved off
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
            let (actions, next) = plan(&v, &facts, &cur, BASE);
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
        let v = view([(1, tablet(1, b"", None, vec![301, 302]))]);
        let mut state = LocalState::default();
        state.hosted.insert(TabletId(1));

        // No facts entry at all: `hosted` fact is false -> "not excluded".
        let (actions, next) = plan(&v, &BTreeMap::new(), &state, BASE);
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
        let (actions, next) = plan(&v, &facts, &state, BASE);
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

        let (actions1, next1) = plan(&v, &BTreeMap::new(), &state, BASE);
        assert_eq!(
            actions1,
            vec![HostAction::Reclaim {
                tablet: TabletId(1)
            }]
        );
        // Not removed automatically — the caller's teardown may still fail.
        assert!(next1.hosted.contains(&TabletId(1)));

        // Retried identically on a second call before confirmation.
        let (actions2, next2) = plan(&v, &BTreeMap::new(), &next1, BASE);
        assert_eq!(
            actions2,
            vec![HostAction::Reclaim {
                tablet: TabletId(1)
            }]
        );

        // Once the caller confirms the teardown, it stops being replanned.
        let mut confirmed = next2;
        confirmed.confirm_torn_down(TabletId(1));
        let (actions3, _next3) = plan(&v, &BTreeMap::new(), &confirmed, BASE);
        assert_eq!(actions3, Vec::new());
    }
}
