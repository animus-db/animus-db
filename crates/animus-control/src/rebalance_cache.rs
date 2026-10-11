//! The control leader's **rebalance-plan cache** (issue #1192, ADR 0029's
//! 2026-10-11 amendment).
//!
//! `reconcile_loop` used to rebuild the per-node load state over every tablet
//! on each rebalance evaluation to make ONE move, so converging a grown
//! cluster cost O(tablets x moves). [`RebalanceCache`] keeps the plan
//! [`PlacementView::rebalance_batch`] computed in one pass and serves it one
//! move per evaluation (the same data-movement rate and the same one CAS per
//! evaluation as before), rebuilding only when the cluster state diverges from
//! what the plan predicted.
//!
//! **Soundness contract**: [`RebalanceCache::next`] returns exactly what
//! `PlacementView::rebalance` would return on the same view. It serves from the
//! cache only when it can PROVE the view is the state the plan expects, using
//! an O(1) fingerprint:
//!
//! * `rev` -- [`Metadata::placement_rev`](crate::meta::Metadata::placement_rev),
//!   bumped by every applied command, so ANY change to members (membership,
//!   liveness, labels), tablets, policies or `split_placing` moves it;
//! * the plan's own prediction -- the single permitted rev step is `+1` right
//!   after we proposed the head move, and only if the head tablet's row now
//!   shows exactly the predicted epoch and replicas (our CAS applied and
//!   nothing else did);
//! * the driver inputs the pure decision reads besides the view -- the
//!   `recently_done` set (compared by value) and the Raft term (a leadership
//!   change drops the plan);
//! * the four map sizes, as a cheap belt-and-braces check against a direct
//!   edit of the public maps that bypassed `apply`.
//!
//! The head is only popped once its application is OBSERVED, never on
//! proposing it: a proposal that is still in flight (or was lost) leaves the
//! view unchanged, and the cache re-proposes the same head, which is also what
//! the uncached decision would do.

use std::collections::{BTreeSet, VecDeque};

use animus_tablet::TabletId;

use crate::meta::{MetaCommand, PlacementView};

/// First plan size. A plan is invalidated by any unrelated change, so a small
/// first plan bounds the work wasted on a churny cluster.
const BASE_PLAN_MOVES: usize = 64;
/// Ceiling of the exponential growth of the plan size when a plan is fully
/// consumed without the cluster having diverged.
const MAX_PLAN_MOVES: usize = 8192;

/// Counters showing the amortization (exported as the
/// `control_rebalance_evals` / `control_rebalance_plan_rebuilds` metrics).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RebalanceCacheStats {
    /// Calls to [`RebalanceCache::next`].
    pub evals: u64,
    /// Evaluations that rebuilt the plan (an O(tablets) pass).
    pub rebuilds: u64,
    /// Evaluations that dropped a cached plan because the view diverged from
    /// the prediction (a subset of the cases that then rebuild).
    pub invalidations: u64,
    /// Evaluations that advanced the cursor after observing the previous
    /// head applied.
    pub advances: u64,
}

#[derive(Debug)]
struct Plan {
    /// Remaining moves; the front is the one to propose next.
    moves: VecDeque<MetaCommand>,
    /// `moves` was not cut short by the size cap, so an empty `moves` means
    /// "balanced" rather than "plan used up".
    exhaustive: bool,
    /// The view rev the front of `moves` is valid at.
    rev: u64,
    term: u64,
    recently_done: BTreeSet<TabletId>,
    shape: [usize; 4],
    /// The front move was returned to the caller (so a `rev + 1` view may be
    /// its application).
    head_proposed: bool,
}

/// See the module docs.
#[derive(Debug)]
pub struct RebalanceCache {
    plan: Option<Plan>,
    cap: usize,
    stats: RebalanceCacheStats,
}

impl Default for RebalanceCache {
    fn default() -> Self {
        Self::new()
    }
}

fn shape_of(view: &PlacementView) -> [usize; 4] {
    [
        view.members.len(),
        view.tablets.len(),
        view.policies.len(),
        view.split_placing.len(),
    ]
}

/// Whether `head` (a `CasTabletReplicas`) is visibly applied in `view`.
fn head_applied(view: &PlacementView, head: &MetaCommand) -> bool {
    let MetaCommand::CasTabletReplicas {
        tablet,
        expected_epoch,
        replicas,
    } = head
    else {
        return false;
    };
    view.tablets
        .get(tablet)
        .is_some_and(|t| t.epoch == expected_epoch.next() && t.replicas == *replicas)
}

impl RebalanceCache {
    /// An empty cache.
    #[must_use]
    pub fn new() -> Self {
        RebalanceCache {
            plan: None,
            cap: BASE_PLAN_MOVES,
            stats: RebalanceCacheStats::default(),
        }
    }

    /// Drop the plan (leadership lost, a proposal refused). The next
    /// evaluation rebuilds.
    pub fn invalidate(&mut self) {
        self.plan = None;
        self.cap = BASE_PLAN_MOVES;
    }

    /// The counters so far.
    #[must_use]
    pub fn stats(&self) -> RebalanceCacheStats {
        self.stats
    }

    /// The remaining planned moves (test and diagnostics).
    #[must_use]
    pub fn planned_len(&self) -> usize {
        self.plan.as_ref().map_or(0, |p| p.moves.len())
    }

    /// The move to propose now: always equal to
    /// `view.rebalance(recently_done, ..)`, from the cache when the view is
    /// provably what the plan expects, from a fresh plan otherwise.
    pub fn next(
        &mut self,
        view: &PlacementView,
        term: u64,
        recently_done: &BTreeSet<TabletId>,
    ) -> Option<MetaCommand> {
        self.stats.evals += 1;
        let shape = shape_of(view);
        if let Some(plan) = &mut self.plan {
            let inputs_same =
                plan.term == term && plan.shape == shape && plan.recently_done == *recently_done;
            let valid = inputs_same
                && if view.rev == plan.rev {
                    true
                } else if view.rev == plan.rev.wrapping_add(1)
                    && plan.head_proposed
                    && plan.moves.front().is_some_and(|h| head_applied(view, h))
                {
                    plan.moves.pop_front();
                    plan.rev = view.rev;
                    plan.head_proposed = false;
                    self.stats.advances += 1;
                    true
                } else {
                    false
                };
            if valid {
                if let Some(head) = plan.moves.front().cloned() {
                    plan.head_proposed = true;
                    return Some(head);
                }
                if plan.exhaustive {
                    return None; // balanced, and nothing has changed since
                }
                // The capped plan is used up with the cluster on course: plan
                // further, in a bigger chunk.
                self.cap = (self.cap * 2).min(MAX_PLAN_MOVES);
            } else {
                self.stats.invalidations += 1;
                self.cap = BASE_PLAN_MOVES;
            }
            self.plan = None;
        }

        self.stats.rebuilds += 1;
        let moves: VecDeque<MetaCommand> = view.rebalance_batch(recently_done, self.cap).into();
        let head = moves.front().cloned();
        self.plan = Some(Plan {
            exhaustive: moves.len() < self.cap,
            moves,
            rev: view.rev,
            term,
            recently_done: recently_done.clone(),
            shape,
            head_proposed: head.is_some(),
        });
        head
    }
}
