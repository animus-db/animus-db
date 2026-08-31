//! ADR 0062 §3 — the in-place split **directed-Placing completion loop**: a
//! per-tablet, leader-side, event-independent background loop that observes
//! a child tablet's own live Raft convergence and reports it to the
//! control-plane catalog.
//!
//! ## Why this loop, and where it lives
//!
//! `Metadata::split_placing_reconcile` (`animus-control`, wired into
//! `node.rs`'s `reconcile_loop`, ADR 0062 §2) recomputes each un-`done`
//! child's policy-satisfying target fresh every tick and proposes a
//! `CasTabletReplicas` whenever it differs from the tablet's current
//! `replicas` — that command is what actually moves a child's live Raft
//! membership, via `animus-cp-data::host::Reconciler`'s `HostAction::
//! Reconfigure` arm (`RaftKvNode::reconfigure_step`, ADR 0058 Train 1's
//! learner-phased sequencing, unmodified). Nothing at the pure `Metadata`
//! level can ever observe *when that convergence actually lands* — a live
//! Raft config is a fact of each replica's own `RaftCore`, not of
//! replicated `Metadata`. This loop is that observer, mirroring
//! `backup_capture_loop`'s/`index_backfill_loop`'s own "a tablet leader's
//! own background loop reports local convergence to the control-plane
//! leader" idiom (ADR 0059 §4's own precedent, and the ADR 0062 §3
//! correction to the original brief's anchor — `RaftKvNode::
//! spawn_reconfigure_loop` has zero production callers; the real
//! convergence driver is `host::Reconciler`, which has no `ClientCtx`/
//! `ControlHandle` of its own to propose through, mirroring
//! `animus-placement`'s "no reverse dep" discipline).
//!
//! ## The predicate
//!
//! Each tick, for every tablet this node currently **leads** with an
//! un-`done` `Metadata::split_placing` entry:
//!
//! ```text
//! group.config() == BTreeSet::from_iter(t.replicas) && group.learners().is_empty()
//! ```
//!
//! — the *exact* convergence predicate `RaftKvNode::reconfigure_step`'s own
//! early return already checks (`current == desired && learners.is_empty()`),
//! re-derived here from the two public accessors ADR 0058 Train 1 already
//! exposes (`config()`/`learners()` — `CpGroup::config()` already existed,
//! for ADR 0029's own release-GC safety anchor; `CpGroup::learners()` is
//! new with this rung, its sibling in every other respect) rather than a
//! new one. Compared against `t.replicas` (`Metadata`'s own CURRENT desired
//! replicas, kept fresh by the reconcile loop's third phase), never
//! `entry.target` directly — `target` is a frozen diagnostic snapshot of
//! what `CutoverSplit` computed at cutover (ADR 0062 §2's own "never trusts
//! or rewrites `target`" rule) and stays `None` forever for an
//! unsatisfiable-at-cutover entry even once a *later* tick's fresh
//! recomputation moves the tablet's real replicas — `t.replicas` is the
//! only value that stays live for both shapes of entry.
//!
//! ## The settle window (a real race this rung found, closed, not merely noted)
//!
//! Immediately after `CutoverSplit` commits, a child's live Raft group
//! already sits on exactly its (fork-inherited) `t.replicas` — the two are
//! trivially equal until the control-plane leader's OWN reconcile loop
//! (`RECONCILE_INTERVAL`, `animus-control::node`, 500ms) has had a tick to
//! bump `t.replicas` toward a genuinely different target. A first cut of
//! this loop compared `group.config()` to `t.replicas` with no further
//! guard, on the reasoning that a premature `done` is merely a diagnostic
//! inaccuracy (fork A: never a serving gate) that self-heals via ordinary
//! `rebalance_placement` once the tablet rejoins its eligible population.
//! **That reasoning was tested and found wrong in practice, not just in
//! theory**: `rebalance_placement`'s own `rebalance_step` moves ONE replica
//! per tick, gated behind `REBALANCE_EVERY_N_TICKS` (~4s) and only when
//! repair proposed nothing — an order of magnitude slower than directed
//! Placing, AND it does not necessarily converge to the SAME target
//! `select_replicas` would have picked (it optimizes cluster-wide balance,
//! not "the lowest-id candidates"), so a premature `done` measurably
//! degrades both the speed and the specific outcome of relief a split was
//! meant to provide — confirmed by a real end-to-end run
//! (`tests/split_placing_completion.rs`) where the race fired on
//! essentially every attempt (this loop's own tick cadence is well under
//! `RECONCILE_INTERVAL`) and left both children on a different, ordinary-
//! rebalance-driven placement instead of the directed one.
//!
//! The fix: a tablet is only reported done once its own convergence has
//! been observed **stable for [`SPLIT_PLACING_DONE_SETTLE`]**, a small
//! multiple of `RECONCILE_INTERVAL` — the same "wait out a slower sibling
//! loop's own worst-case reaction window before trusting an observation"
//! shape `index_drain.rs`'s in-place cutover driver already uses
//! (`INPLACE_SPLIT_MATERIALIZE_SETTLE_MS`, closing an analogous race
//! against the tablet-host reconciler's own fallback cadence — see
//! `animusd/CLAUDE.md`'s "Gotcha this rung found" entry on that one for the
//! precedent). Tracked per tablet in a driver-local `BTreeMap<TabletId,
//! Nanos>` (`first_seen_converged`, this loop's own memory — never
//! replicated, never durable, safe to lose on a leader change: a fresh
//! leader simply restarts its own settle timer from zero, which only ever
//! makes `done` land later, never wrong) — see [`split_placing_completion_tick`]'s
//! own doc for the exact state machine.
//!
//! **A separate, pre-existing finding surfaced while proving this loop
//! against a real cluster, named here rather than left implicit:** a target
//! that replaces TWO of a tablet's three replicas at once (a genuinely
//! plausible directed-Placing outcome — a parent grown far from its own
//! current homes) was observed, over `ProdEnv`, to make the live Raft
//! group's own membership **oscillate** — briefly reaching the full
//! 5-candidate over-replicated intermediate `reconfigure_step`'s add-before-
//! remove sequencing produces, then reverting toward the original 3-replica
//! set, repeatedly, never settling within a 60s budget. A target that
//! replaces only ONE of three replicas (a smaller, one-add-one-remove
//! sequence, no 5-member intermediate) converged cleanly and quickly every
//! time. This loop's own logic is not the cause — the oscillation was
//! observed with zero `MarkSplitPlacingDone` proposes ever having fired yet
//! — so this is a `host::Reconciler`/`reconfigure_step` (`animus-cp-data`,
//! ADR 0058 Train 1) concern, pre-existing and unmodified by this rung, out
//! of this rung's scope to fix. `tests/split_placing_completion.rs`
//! deliberately exercises only the one-replica-difference shape for exactly
//! this reason; see `docs/engineering-lessons.md` for the fuller account.
//!
//! ## Idempotence, relay, quiescence
//!
//! `MetaCommand::MarkSplitPlacingDone` is epoch-CAS'd against the CHILD's
//! own current epoch and idempotent on an already-`done` entry (rung 2's
//! own apply arm) — a duplicate or stale propose (a leader change mid-tick,
//! two ticks racing before either confirms) CAS-fails or no-ops harmlessly
//! by construction, so this loop's own settle-tracking state need not be
//! exactly right, only a reasonable heuristic — see
//! `tests/split_placing_completion.rs`'s own stale/duplicate-propose
//! regression. Proposed via [`crate::ClientCtx::propose_schema`]
//! (`MarkSplitPlacingDone` is on the `is_relayable_command` allowlist,
//! `animus-node::wire`) since a tablet's own leader need not be — and on a
//! split deployment, may not even be control-connected to — the
//! control-plane leader. Every read here (`ctx.effective_metadata()`,
//! `group.config()`/`group.learners()`) is a cheap, already-cached local
//! read with no propose/wake of its own — mirroring `ttl_reaper.rs`'s "read
//! for free, wake only to write" stance: this loop only ever touches the
//! network/Raft log on the one tick it actually finds something to report.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_env::{Clock, Nanos};
use animus_tablet::TabletId;

use crate::{ClientCtx, MetaCommand};

/// This loop's tick cadence — matches `backup_capture.rs`'s own
/// `BACKUP_CAPTURE_INTERVAL`: cheap per-tick work, and a fast tick keeps
/// this crate's own converged-or-timeout tests from becoming the slow part
/// of the suite.
pub(crate) const SPLIT_PLACING_COMPLETION_INTERVAL: Duration = Duration::from_millis(200);

/// How long a tablet's own convergence (`group.config() == t.replicas`, no
/// dangling learners) must hold, continuously observed by this loop's own
/// ticks, before it is trusted enough to report done — see the module doc's
/// "The settle window" section. Three times `animus-control::node`'s own
/// (private) `RECONCILE_INTERVAL` (500ms): comfortably past the
/// control-plane leader's own worst-case one-tick delay to react to a fresh
/// `split_placing` entry, plus margin for the resulting `CasTabletReplicas`
/// to commit and for this node's own metadata mirror to observe it.
pub(crate) const SPLIT_PLACING_DONE_SETTLE: Duration = Duration::from_millis(1_500);

/// Every led tablet's own completion check, once per
/// [`SPLIT_PLACING_COMPLETION_INTERVAL`] tick.
pub(crate) async fn split_placing_completion_loop(ctx: ClientCtx) {
    let mut first_seen_converged: BTreeMap<TabletId, Nanos> = BTreeMap::new();
    loop {
        tokio::time::sleep(SPLIT_PLACING_COMPLETION_INTERVAL).await;
        split_placing_completion_tick(&ctx, &mut first_seen_converged).await;
    }
}

/// One tick's worth of work — factored out of the sleep loop so it is
/// directly callable (mirrors `backup_capture_tick`'s own split from
/// `backup_capture_loop`).
///
/// For every tablet this node currently leads with an un-`done`
/// `Metadata::split_placing` entry: if its live Raft group is NOT currently
/// converged to `Metadata`'s own current `replicas` (or the entry/tablet
/// has disappeared, e.g. a drop-table race), its settle timer — if any — is
/// cleared, so a later re-convergence starts a fresh window rather than
/// crediting time observed before an intervening move. If it IS converged:
/// a first observation records `ctx.env.now()`; once the SAME converged
/// state has been observed continuously for at least
/// [`SPLIT_PLACING_DONE_SETTLE`], `MetaCommand::MarkSplitPlacingDone` is
/// proposed and the timer entry is removed either way (a rejected/lost
/// propose simply restarts the settle window on the next tick — harmless,
/// since the underlying command is itself idempotent).
pub(crate) async fn split_placing_completion_tick(
    ctx: &ClientCtx,
    first_seen_converged: &mut BTreeMap<TabletId, Nanos>,
) {
    let meta = ctx.effective_metadata();
    if meta.split_placing.is_empty() {
        first_seen_converged.clear();
        return;
    }
    for (tablet, group) in ctx.edge.hosted_groups() {
        if !group.is_leader() {
            continue;
        }
        let Some(entry) = meta.split_placing.get(&tablet) else {
            first_seen_converged.remove(&tablet);
            continue; // no obligation for this tablet
        };
        if entry.done {
            first_seen_converged.remove(&tablet);
            continue;
        }
        let Some(t) = meta.tablets.get(&tablet) else {
            first_seen_converged.remove(&tablet);
            continue; // table dropped underneath the entry — nothing left to report
        };
        let desired: BTreeSet<_> = t.replicas.iter().cloned().collect();
        if group.config() != desired || !group.learners().is_empty() {
            first_seen_converged.remove(&tablet);
            continue;
        }
        let now = ctx.env.now();
        let since = *first_seen_converged.entry(tablet).or_insert(now);
        if now.duration_since(since) < SPLIT_PLACING_DONE_SETTLE {
            continue;
        }
        first_seen_converged.remove(&tablet);
        let _ = ctx
            .propose_schema(&MetaCommand::MarkSplitPlacingDone {
                tablet,
                expected_epoch: t.epoch,
            })
            .await;
    }
}
