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
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(test)]
use animus_env::nid;
use animus_env::{Env, NodeId};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{Epoch, KeyRange, Tablet, TabletId};

use crate::{RaftKvNode, StorageScope, wal_file};

/// The per-tablet engine seam (ADR 0050, Train B rung 1): every hosted
/// data-plane tablet gets its **own private `StorageEngine`**, opened by the
/// reconciler when it hosts the tablet and whose files are deleted outright
/// when the tablet is released/reclaimed. The reconciler owns engine
/// *lifecycle*; this trait owns engine *identity* — how a tablet id maps to
/// durable files (`animusd` derives a per-tablet filename prefix on the `Env`
/// `Disk` seam, the same naming-is-identity mechanism `raftkv.wal.<tablet>`
/// already uses; sim/test callers use [`MemoryTabletEngines`]' in-memory
/// registry).
///
/// `open` must be idempotent (re-opening recovers the engine's own durable
/// state); `probe` answers "does durable state for this tablet exist on this
/// node" *without* necessarily opening (the restart-upgrade signal
/// [`TabletFacts::has_data`] starts from); `destroy` deletes the engine's
/// files — the caller guarantees the engine is closed (its group driver
/// stopped) first.
#[async_trait::async_trait]
pub trait EngineFactory<S: StorageEngine>: Send + Sync {
    /// Open (or re-open) `tablet`'s own engine, recovering its durable state.
    async fn open(&self, tablet: TabletId) -> Result<S, String>;
    /// Does durable engine state for `tablet` exist on this node?
    async fn probe(&self, tablet: TabletId) -> bool;
    /// Delete every durable file of `tablet`'s engine. The engine must be
    /// closed. Idempotent — destroying an engine that never existed is a
    /// no-op.
    async fn destroy(&self, tablet: TabletId);
}

/// The [`MemoryEngine`] implementation of [`EngineFactory`]: an in-memory
/// registry keyed by tablet id. Production caller: `animusd`'s
/// `StorageBackend::Memory` (ephemeral runs); every sim/reconciler test uses
/// it too. Cloning shares the registry — a test models "a durable engine
/// surviving a process crash" by keeping one clone of this factory alive
/// across the restart (the same modeling `tests/reconciler_corpus.rs` used
/// to do with one shared `MemoryEngine`).
#[derive(Clone, Default)]
pub struct MemoryTabletEngines {
    engines: Arc<Mutex<BTreeMap<u64, MemoryEngine>>>,
}

impl MemoryTabletEngines {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Get-or-create `tablet`'s engine — the harness/test accessor for
    /// seeding data before a host and asserting on it after (clones share
    /// state, so this is the same engine the reconciler hosts with).
    #[must_use]
    pub fn engine(&self, tablet: TabletId) -> MemoryEngine {
        self.engines
            .lock()
            .expect("engine registry poisoned")
            .entry(tablet.0)
            .or_default()
            .clone()
    }
}

#[async_trait::async_trait]
impl EngineFactory<MemoryEngine> for MemoryTabletEngines {
    async fn open(&self, tablet: TabletId) -> Result<MemoryEngine, String> {
        Ok(self.engine(tablet))
    }

    async fn probe(&self, tablet: TabletId) -> bool {
        self.engines
            .lock()
            .expect("engine registry poisoned")
            .contains_key(&tablet.0)
    }

    async fn destroy(&self, tablet: TabletId) {
        self.engines
            .lock()
            .expect("engine registry poisoned")
            .remove(&tablet.0);
    }
}

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
    /// This group's own declared range, if hosted — `None` when `hosted` is
    /// `false`. Immutable for the group's lifetime (ADR 0050 rung 2);
    /// retained as a fact for diagnostics/idempotence checks only.
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
    /// like the real loop retrying on a later tick. The same discipline runs
    /// in reverse for a just-emitted `Host`: `plan` inserts the tablet here
    /// optimistically, before the executor has actually stood up a live
    /// handle, so a `host()` that skips the action (an `EngineFactory::open`
    /// failure, or the tablet vanishing from `Metadata` before execution)
    /// must call [`LocalState::release_unconfirmed_host`] to undo that insert
    /// — otherwise the claim is permanent and `plan` never re-emits `Host`
    /// for a tablet this node in fact never hosted.
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

    /// Record that a planned [`HostAction::Host`] for `tablet` did **not**
    /// actually establish a live handle (the executor's `host()` skipped it —
    /// today, `EngineFactory::open` failing, or the tablet having vanished
    /// from `Metadata` between `plan` and execution). [`plan`] itself already
    /// added `tablet` to [`hosted`](Self::hosted) the instant it decided to
    /// emit the action (it has no way to know execution will fail); this
    /// undoes exactly that insert so the phase-1 gate
    /// (`!next.hosted.contains(&tablet)`) stops treating the tablet as
    /// already hosted and the next `plan` call re-emits `Host` for it. Mirrors
    /// [`confirm_torn_down`](Self::confirm_torn_down)'s "the executor
    /// confirms completion" discipline, applied to the other end of the
    /// lifecycle: a claim in [`hosted`](Self::hosted) isn't real until a live
    /// handle actually backs it, exactly as a `Reclaim`/`Release` claim isn't
    /// cleared until its teardown actually completes.
    pub fn release_unconfirmed_host(&mut self, tablet: TabletId) {
        self.hosted.remove(&tablet);
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

/// One reconciling action [`plan`] can emit for a single tablet. A caller
/// (`animusd`, PR4) executes these against its own live `ProdEnv` state;
/// `plan` itself performs no I/O.
///
/// Emitted in a fixed overall order — every [`Host`](Self::Host), then every
/// [`Reconfigure`](Self::Reconfigure), then every
/// [`Release`](Self::Release)/[`Reclaim`](Self::Reclaim)
/// — mirroring the existing loops' relative priority (stand up a
/// newly-placed tablet before reconfiguring anyone; reconcile membership
/// before tearing anything down). Within each group, tablets are emitted in
/// `TabletId` order (a `BTreeMap` iteration is deterministic on every node).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostAction {
    /// Stand up this node's member of `tablet`'s group for the first time —
    /// a fresh whole-keyspace tablet, a split child, or a reconciler-placed
    /// spare all reach this the same way.
    Host {
        /// The tablet to host.
        tablet: TabletId,
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
    /// Teardown of a tablet moved off this node (ADR 0050: a private
    /// engine's teardown deletes the tablet's own files whole — the
    /// stale-wide-scope erase hazard this variant used to document died
    /// with the shared engine).
    Release {
        /// The tablet to release.
        tablet: TabletId,
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

    // --- Phase 1: host a newly-placed tablet (a tablet's declared range is
    // immutable, ADR 0050 rung 2 — there is no scope to adjust on an
    // already-hosted one).
    let mut to_host = Vec::new();
    for (&tablet, t) in &view.tablets {
        let Some(join_plan) = plan_join_host(base_id.clone(), &t.replicas, t.epoch) else {
            continue;
        };
        if !next.hosted.contains(&tablet) {
            to_host.push((tablet, t, join_plan));
        }
    }
    for (tablet, t, join_plan) in to_host {
        let has_data = facts.get(&tablet).is_some_and(|f| f.has_data);
        actions.push(HostAction::Host {
            tablet,
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
            actions.push(HostAction::Release { tablet });
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
    /// The per-tablet engine seam (ADR 0050 rung 1): how this node maps a
    /// tablet id to its own private engine — see [`EngineFactory`].
    factory: Box<dyn EngineFactory<S>>,
    /// Every open per-tablet engine handle, kept in lockstep with
    /// [`hosted`](Self::hosted) (plus, transiently within one tick, a probed
    /// join candidate's — pruned back to the hosted set at each tick's end).
    engines: BTreeMap<TabletId, S>,
    base_id: NodeId,
    /// Every tablet this node currently hosts a live `RaftKvNode` for — the
    /// authoritative hosting state (kept in lockstep with
    /// [`LocalState::hosted`], but holding the live handle, not just the id).
    hosted: BTreeMap<TabletId, RaftKvNode<E, S>>,
    state: LocalState,
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
    /// ADR 0044 phase-1 PR4 production wiring: opt every group this
    /// reconciler hosts *from now on* into quiescence with this idle
    /// threshold — see [`enable_quiescence`](Self::enable_quiescence).
    /// `None` (the default) is exactly today's behavior: every existing
    /// caller of [`new`](Self::new) is unaffected.
    quiesce_after: Option<Duration>,
}

/// Fresh/re-registered-hosting mirror hook — see [`Reconciler`]'s `on_host`
/// field doc.
type OnHostFn<E, S> = Box<dyn Fn(TabletId, &RaftKvNode<E, S>) + Send + Sync>;
/// Teardown-unregister mirror hook — see [`Reconciler`]'s `on_teardown` field
/// doc.
type OnTeardownFn = Box<dyn Fn(TabletId) + Send + Sync>;

impl<E: Env, S: StorageEngine + 'static> Reconciler<E, S> {
    /// A fresh reconciler for one node. `env` is this node's `raftkv` env —
    /// every tablet's `RaftKvNode` this reconciler ever hosts runs on
    /// `env.clone()` (stream-addressed by the tablet id, ADR 0026 Stage B) —
    /// and `factory` is the per-tablet engine seam (ADR 0050 rung 1): each
    /// hosted tablet gets its **own private engine**, opened via
    /// `factory.open(tablet)` at host time and destroyed (files deleted) at
    /// release/reclaim. `base_id` is this node's identity in a tablet's
    /// replica set. (F2b: physical keys carry no table prefix, so the old
    /// `prefix_for` table-escaping seam is gone.) `on_host`/`on_teardown`
    /// mirror hosting changes into the caller's own routing registry, letting
    /// `Reconciler` stay the single writer of hosting state while the
    /// caller's registry becomes a read-only mirror.
    pub fn new(
        env: E,
        factory: impl EngineFactory<S> + 'static,
        base_id: NodeId,
        on_host: impl Fn(TabletId, &RaftKvNode<E, S>) + Send + Sync + 'static,
        on_teardown: impl Fn(TabletId) + Send + Sync + 'static,
    ) -> Self {
        Self {
            env,
            factory: Box::new(factory),
            engines: BTreeMap::new(),
            base_id,
            hosted: BTreeMap::new(),
            state: LocalState::default(),
            on_host: Box::new(on_host),
            on_teardown: Box::new(on_teardown),
            quiesce_after: None,
        }
    }

    /// Get-or-open `tablet`'s own engine, caching the handle. `None` (with a
    /// warn) if the factory fails to open it — the caller skips the action;
    /// `plan` re-emits it next tick.
    async fn ensure_engine(&mut self, tablet: TabletId) -> Option<S> {
        if let Some(engine) = self.engines.get(&tablet) {
            return Some(engine.clone());
        }
        match self.factory.open(tablet).await {
            Ok(engine) => {
                self.engines.insert(tablet, engine.clone());
                Some(engine)
            }
            Err(e) => {
                tracing::warn!(tablet = tablet.0, %e, "reconciler: opening tablet engine");
                None
            }
        }
    }

    /// This node's current [`LocalState`] — read-only, for a caller (or a
    /// test) that wants to observe convergence without reaching into the
    /// private `hosted` map.
    pub fn local_state(&self) -> &LocalState {
        &self.state
    }

    /// Opt every group this reconciler hosts **from now on** into quiescence
    /// (ADR 0044 phase-1 PR4 production wiring — data-plane groups only,
    /// fork G; the control plane's own `RaftNode` never calls the
    /// equivalent). Call once, right after construction and before the first
    /// [`tick`](Self::tick) — a tablet already in [`hosted`](Self::hosted_node)
    /// at the time this is called is unaffected (there is no production
    /// caller that hosts before opting in, so this is a non-issue in
    /// practice; tests that need it can enable quiescence per-node directly
    /// via [`RaftKvNode::enable_quiescence`] instead).
    pub fn enable_quiescence(&mut self, after: Duration) {
        self.quiesce_after = Some(after);
    }

    /// The live `RaftKvNode` this reconciler hosts for `tablet`, if any.
    pub fn hosted_node(&self, tablet: TabletId) -> Option<&RaftKvNode<E, S>> {
        self.hosted.get(&tablet)
    }

    /// One reconcile tick (ADR 0031): snapshot the impure facts this node's
    /// own hosted groups + engine can answer, call [`plan`] exactly once, then
    /// execute the returned actions **in the fixed order `plan` emits them**
    /// (`Host` → `Reconfigure` → `Release`/`Reclaim`).
    ///
    /// The caller is responsible for the `last_applied() == 0` pre-recovery
    /// guard (a live control-plane `RaftNode` read this crate has no business
    /// taking, per [`plan`]'s own doc) — skip calling `tick` at all before
    /// replicated `Metadata` has recovered.
    pub async fn tick(&mut self, view: &MetadataView) {
        // ADR 0044 phase-1 PR4, fork H: proactively wake any hosted group
        // whose replica set intersects the failure detector's `down` set —
        // the TiKV-hibernate-regions lesson (a quiesced leader that dies
        // while dormant would otherwise stay cold until some client happens
        // to touch its tablet, a worse availability story than today).
        // `RaftKvNode::wake()` is a cheap, idempotent notify, safe to call
        // unconditionally on every hosted group, quiesced or not.
        for (&tablet, node) in &self.hosted {
            if let Some(t) = view.tablets.get(&tablet)
                && t.replicas.iter().any(|r| view.down.contains(r))
            {
                node.wake();
            }
        }

        let facts = self.gather_facts(view).await;
        let (actions, next) = plan(view, &facts, &self.state, self.base_id.clone());
        self.state = next;

        for action in actions {
            match action {
                HostAction::Host {
                    tablet,
                    range,
                    initial_formation,
                } => {
                    self.host(view, tablet, range, initial_formation).await;
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
                HostAction::Release { tablet } => {
                    self.teardown(tablet).await;
                }
                HostAction::Reclaim { tablet } => {
                    self.teardown(tablet).await;
                }
            }
        }

        // ADR 0050 rung 1: prune engine handles back to the hosted set — a
        // join candidate probed by `gather_facts` whose `Host` never fired
        // this tick (a gate deferred it) must not keep an open handle
        // parked here; the next tick that actually hosts it just re-opens.
        let hosted: BTreeSet<TabletId> = self.hosted.keys().copied().collect();
        self.engines.retain(|t, _| hosted.contains(t));
    }

    /// Gather the [`TabletFacts`] [`plan`] needs: every currently-hosted
    /// tablet's live state (`is_leader`/`config_excludes_me`/`scope_range`),
    /// plus a `has_data` presence check for every not-yet-hosted candidate
    /// [`plan_join_host`] would place on this node — the one input `plan`
    /// can't gather itself (an async engine read).
    async fn gather_facts(&mut self, view: &MetadataView) -> BTreeMap<TabletId, TabletFacts> {
        let mut facts = BTreeMap::new();
        for (&tablet, node) in &self.hosted {
            let scope_range = node.scope_range();
            facts.insert(
                tablet,
                TabletFacts {
                    hosted: true,
                    is_leader: node.is_leader(),
                    config_excludes_me: !node.config().contains(&self.base_id),
                    scope_range: Some(scope_range),
                    has_data: false,
                },
            );
        }
        let candidates: Vec<(TabletId, Tablet)> = view
            .tablets
            .iter()
            .filter(|(tablet, t)| {
                !self.state.hosted.contains(tablet)
                    && plan_join_host(self.base_id.clone(), &t.replicas, t.epoch).is_some()
            })
            .map(|(&tablet, t)| (tablet, t.clone()))
            .collect();
        for (tablet, t) in candidates {
            // ADR 0050 rung 1: the restart-upgrade signal is now two-step —
            // does this tablet's own engine exist on this node at all
            // (`probe`, cheap, no open), and if so, does it hold base rows
            // (the pre-existing `has_data` check, run against the tablet's
            // own private engine).
            let has_data = if self.factory.probe(tablet).await {
                match self.ensure_engine(tablet).await {
                    Some(engine) => {
                        let scope = StorageScope::new(t.range.clone());
                        // ADR 0041 §3: ask the **base**-kind scope. Base rows
                        // are the right signal for the reforming-vs-fresh-join
                        // question this answers: the other kinds only ever
                        // exist alongside base rows.
                        scope.with_kind(crate::KIND_BASE).has_data(&engine).await
                    }
                    None => false,
                }
            } else {
                false
            };
            facts.insert(
                tablet,
                TabletFacts {
                    has_data,
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
    ///
    /// Either early-return below leaves this tablet **not** in `self.hosted`
    /// (no live handle was ever created), so both call
    /// [`LocalState::release_unconfirmed_host`] to undo `plan`'s optimistic
    /// claim insert — otherwise the phase-1 gate
    /// (`!next.hosted.contains(&tablet)`) would treat the tablet as already
    /// hosted forever and `plan` would never re-emit `Host` for it.
    async fn host(
        &mut self,
        view: &MetadataView,
        tablet: TabletId,
        range: KeyRange,
        initial_formation: bool,
    ) {
        let Some(t) = view.tablets.get(&tablet) else {
            self.state.release_unconfirmed_host(tablet);
            return;
        };
        // ADR 0050 rung 1: open (or re-open) this tablet's own private
        // engine. A factory failure skips the host and releases the claim
        // (above); `plan` re-emits it next tick.
        let Some(engine) = self.ensure_engine(tablet).await else {
            self.state.release_unconfirmed_host(tablet);
            return;
        };
        let scope = StorageScope::new(range);
        let full: Vec<NodeId> = t.replicas.clone();
        let others: Vec<NodeId> = full
            .iter()
            .filter(|id| **id != self.base_id)
            .cloned()
            .collect();
        let config = if initial_formation { full } else { others };
        // ADR 0018 §2 amendment: cross-group MVCC ordering no longer needs a
        // version-floor seed here — `RaftKvNode::start_hosted` already
        // witnesses this group's HLC off its engine's own `latest_version()`
        // at construction (the tablet's private engine since ADR 0050 rung 1
        // — its own data is the only history a fresh group must out-version).
        let node = RaftKvNode::start_hosted(self.env.clone(), config, engine, scope, tablet.0);
        // ADR 0044 phase-1 PR4 production wiring: opt every freshly-hosted
        // data-plane group into quiescence if this reconciler has been
        // configured to (see `enable_quiescence`'s doc).
        if let Some(after) = self.quiesce_after {
            node.enable_quiescence(after);
        }
        (self.on_host)(tablet, &node);
        self.hosted.insert(tablet, node);
    }

    /// Execute a [`HostAction::Release`]/[`HostAction::Reclaim`]: unregister
    /// from the caller's routing registry first, shut the driver down and
    /// wait for it to actually stop (never touch data under a live driver),
    /// then **delete the tablet's own engine files** (ADR 0050 rung 1 — both
    /// actions reduce to the identical deletion since the engine is private,
    /// so whole-engine deletion is the erase either way — there is no
    /// behavioral fork left to take a `kind` parameter for) and its WAL file,
    /// and only then confirm the teardown to [`LocalState`] and drop the
    /// local handle. A timeout waiting for the driver to stop re-registers
    /// the handle (so routing keeps working) and leaves `state`/`hosted`
    /// untouched — `plan` re-emits the identical action next tick.
    ///
    /// `self.hosted.remove(&tablet)` returning `None` means a **zombie
    /// claim**: [`LocalState::hosted`] (or a caller's stale `plan` input)
    /// names a tablet with no live handle here, and nothing else ever
    /// populates `self.hosted` — so there is no driver to shut down and
    /// nothing to wait on. Best-effort cleanup still runs (a previous
    /// `host()` attempt may have left partial engine/WAL files on disk before
    /// failing to establish a handle) and the claim is confirmed torn down
    /// immediately, so `plan` stops re-emitting a teardown action that could
    /// otherwise never make progress.
    async fn teardown(&mut self, tablet: TabletId) {
        let Some(node) = self.hosted.remove(&tablet) else {
            tracing::warn!(
                tablet = tablet.0,
                "reconciler: tearing down a tablet with no live handle (zombie claim)"
            );
            (self.on_teardown)(tablet);
            self.erase_tablet_files(tablet).await;
            self.state.confirm_torn_down(tablet);
            return;
        };
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

        // ADR 0050 rung 1: a tablet's engine is private, so teardown reduces
        // to deleting its files whole — instant, real space reclaim, no
        // `merge_tombstone` sweep (the shared-engine sibling-sparing bound
        // this used to need died with the shared engine, Train B rung 7).
        drop(node);
        self.erase_tablet_files(tablet).await;
        self.state.confirm_torn_down(tablet);
    }

    /// Delete `tablet`'s own engine files (ADR 0050 rung 1 — the engine is
    /// private, so whole-engine deletion is the erase) and its WAL file.
    /// Shared by both [`teardown`](Self::teardown) paths: the normal
    /// stopped-driver path and the zombie-claim path, which has no driver to
    /// wait on but may still need to clean up files a failed `host()` left
    /// behind. Both `factory.destroy`/`env.remove` are documented
    /// idempotent/tolerant of nothing existing, so this is safe to call even
    /// when no files were ever written.
    async fn erase_tablet_files(&mut self, tablet: TabletId) {
        self.engines.remove(&tablet);
        self.factory.destroy(tablet).await;
        if let Err(e) = self.env.remove(&wal_file(tablet.0)).await {
            tracing::warn!(
                ?e,
                tablet = tablet.0,
                "reconciler: removing the tablet's WAL"
            );
        }
    }
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
            },
        )]
        .into_iter()
        .collect();

        let (actions, next) = plan(&v, &facts, &state, base());
        assert_eq!(actions, Vec::new());
        assert_eq!(next, state);
    }

    // DELETED (ADR 0050 Train B rung 7): the NarrowScope/ProposeSeal planner
    // semantics tests died with the variants (ranges are immutable; the
    // copy-based split hosts children fresh and retires parents whole).

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

    // === LocalState::release_unconfirmed_host ===============================

    #[test]
    fn release_unconfirmed_host_drops_the_claim_so_plan_re_emits_host() {
        // Mirrors `plan`'s own phase-1 insert: a fresh tablet is claimed
        // optimistically, before any executor has actually stood it up.
        let v = view([(1, tablet(1, b"", None, vec![base()]))]);
        let (actions, next) = plan(&v, &BTreeMap::new(), &LocalState::default(), base());
        assert!(matches!(actions[0], HostAction::Host { tablet, .. } if tablet == TabletId(1)));
        assert!(next.hosted.contains(&TabletId(1)));

        // The executor's `host()` failed to establish a live handle (an
        // `EngineFactory::open` I/O error) and releases the claim.
        let mut released = next;
        released.release_unconfirmed_host(TabletId(1));
        assert!(!released.hosted.contains(&TabletId(1)));

        // The next `plan` call must genuinely re-emit `Host` — the whole
        // point of releasing the claim — not silently swallow it forever
        // (the bug this method exists to close).
        let (actions2, _next2) = plan(&v, &BTreeMap::new(), &released, base());
        assert_eq!(
            actions2,
            vec![HostAction::Host {
                tablet: TabletId(1),
                range: KeyRange::whole(),
                initial_formation: true,
            }],
            "plan must re-emit Host once the failed claim is released"
        );
    }

    // === Reconciler::teardown: the zombie-claim backstop =====================
    //
    // `LocalState::hosted` naming a tablet with no live handle in
    // `Reconciler::hosted` is, by construction, otherwise unreachable through
    // the public `tick()` API today (`host()` itself now releases an
    // unconfirmed claim on failure via `release_unconfirmed_host` above) — so
    // this exercises the defensive backstop directly via crate-private field
    // access, the same way `lib.rs`'s `pr5_orphan_and_resurrection_tests`
    // builds a scenario the public API cannot express.

    #[test]
    fn teardown_clears_a_zombie_claim_with_no_live_handle() {
        let sim = animus_sim::Simulator::new(0x2A11_0C0D_u64);
        let env = sim.env(base());
        let mut reconciler: Reconciler<animus_sim::SimEnv, MemoryEngine> = Reconciler::new(
            env,
            MemoryTabletEngines::new(),
            base(),
            |_t, _n| {},
            |_t| {},
        );
        // Simulate the only way this shape can arise: a claim recorded with
        // no corresponding entry in `self.hosted` (the live-handle map).
        reconciler.state.hosted.insert(TabletId(1));
        reconciler
            .state
            .pending_release
            .insert(TabletId(1), (Epoch::INITIAL, 2));

        futures::executor::block_on(reconciler.teardown(TabletId(1)));

        assert!(
            !reconciler.local_state().hosted.contains(&TabletId(1)),
            "a zombie claim must be confirmed torn down, not re-planned forever"
        );
        assert!(
            !reconciler
                .local_state()
                .pending_release
                .contains_key(&TabletId(1)),
            "confirm_torn_down must also clear any leftover pending_release entry"
        );
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

    // === plan(): Reclaim on drop =============================================

    #[test]
    fn a_dropped_tablet_is_reclaimed() {
        // The tablet vanished from the map entirely (its whole table was
        // dropped) — this is the only way a hosted-but-absent tablet is
        // reconciled now that tablets are split-only (there is no merge to
        // distinguish it from).
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
}
