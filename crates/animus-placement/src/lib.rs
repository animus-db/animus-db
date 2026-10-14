//! Topology-aware placement and data residency for AnimusDB (ADR 0005).
//!
//! The control plane owns the tablet map but should not place replicas blindly:
//! operators need to pin *where* data lives — for latency, and for legal
//! **residency** (e.g. EU data must stay on EU nodes). This crate is the
//! deterministic policy engine that decides a tablet's replica set from cluster
//! membership and a [`PlacementPolicy`].
//!
//! It is a **pure library**: no clock, no I/O, no randomness — given the same
//! candidates and policy it returns the same replica set, on every Raft replica
//! and on replay. The control plane (or operator tooling) feeds it candidates
//! built from the replicated membership and turns its output into a
//! `CasTabletReplicas`. Keeping it dependency-light (only [`NodeId`]) also keeps
//! it out of any dependency cycle with `animus-control`.
//!
//! Two entry points:
//! - [`select_replicas`] — choose a replica set for a fresh tablet.
//! - [`replan`] — recompute a replica set after a membership change, **keeping
//!   the surviving replicas** so only the failed/ineligible ones move (minimal
//!   data churn).
//!
//! Both enforce the same policy. As ADR 0005 stresses, residency is only as
//! strong as its weakest path: enforcing it on hinted handoff, read-repair,
//! anti-entropy, and backup is still future work — this crate covers placement.

use std::collections::{BTreeMap, BTreeSet};

use animus_env::NodeId;
use serde::{Deserialize, Serialize};

/// A node that may host replicas, with its topology labels (e.g.
/// `region=eu-west`, `zone=eu-west-1a`). The caller builds these from the
/// control plane's membership, typically including only nodes healthy enough to
/// place on (e.g. `Active`); this crate applies the *policy* (residency +
/// spread), not liveness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    /// The node's id.
    pub node: NodeId,
    /// The node's topology labels.
    pub labels: BTreeMap<String, String>,
    /// The node's declared capacity (ADR 0079 section 1). Defaults to
    /// [`Capacity::DEFAULT`] (weight 100, no ceiling), under which every
    /// function in this crate behaves exactly as it did before capacity existed.
    pub capacity: Capacity,
}

impl Candidate {
    /// Build a candidate from a node id and its labels, with the default
    /// capacity.
    pub fn new(node: NodeId, labels: BTreeMap<String, String>) -> Self {
        Self {
            node,
            labels,
            capacity: Capacity::DEFAULT,
        }
    }

    /// The same candidate with a declared `capacity`.
    #[must_use]
    pub fn with_capacity(mut self, capacity: Capacity) -> Self {
        self.capacity = capacity;
        self
    }
}

/// The weight a node that declares nothing has (ADR 0079 section 1).
pub const DEFAULT_WEIGHT: u32 = 100;

/// A node's declared capacity (ADR 0079 section 1).
///
/// `weight` is relative capacity (only ratios matter); a node's *fill* is
/// `replicas / weight`, compared as an exact rational (cross-multiplied
/// integers, never floating point, so every Raft replica computes the same
/// answer). `max_replicas` is an absolute ceiling on hosted replicas: a node
/// whose replica count is at or above it is never chosen as a destination.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capacity {
    /// Relative capacity. `0` is treated as `1` (a weightless node would
    /// otherwise have an infinite fill).
    pub weight: u32,
    /// Absolute ceiling on replicas (voters and learners); `None` is unbounded.
    pub max_replicas: Option<u32>,
}

impl Capacity {
    /// Weight [`DEFAULT_WEIGHT`], no ceiling.
    pub const DEFAULT: Capacity = Capacity {
        weight: DEFAULT_WEIGHT,
        max_replicas: None,
    };

    /// The weight actually used in comparisons (`weight`, at least 1).
    pub fn effective_weight(&self) -> u64 {
        u64::from(self.weight.max(1))
    }

    /// Whether a node holding `replicas` replicas is at (or above) its
    /// ceiling, so no further replica may be placed on it.
    pub fn is_full(&self, replicas: usize) -> bool {
        self.max_replicas.is_some_and(|m| replicas >= m as usize)
    }

    /// Whether a node holding `replicas` replicas is strictly above its
    /// ceiling (the cap was lowered): a rebalance source only.
    pub fn is_over(&self, replicas: usize) -> bool {
        self.max_replicas.is_some_and(|m| replicas > m as usize)
    }
}

impl Default for Capacity {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Compare `ca / wa` with `cb / wb` as exact rationals (cross-multiplication
/// in `u128`; no overflow for any `usize` count and `u32` weight).
fn fill_cmp(ca: usize, wa: u64, cb: usize, wb: u64) -> std::cmp::Ordering {
    (ca as u128 * u128::from(wb)).cmp(&(cb as u128 * u128::from(wa)))
}

/// Drop from `eligible` every node at its `max_replicas` under `load` (a node
/// absent from `load` counts `0`), **except** a node already in `current`:
/// capacity never shrinks a set (ADR 0079 section 1, rule 3).
fn drop_full(
    eligible: Vec<(NodeId, Domain)>,
    candidates: &[Candidate],
    load: &BTreeMap<NodeId, usize>,
    current: &[NodeId],
) -> Vec<(NodeId, Domain)> {
    if candidates.iter().all(|c| c.capacity.max_replicas.is_none()) {
        return eligible;
    }
    eligible
        .into_iter()
        .filter(|(n, _)| {
            current.contains(n)
                || candidate_for(candidates, n)
                    .is_none_or(|c| !c.capacity.is_full(load.get(n).copied().unwrap_or(0)))
        })
        .collect()
}

/// How replicas must spread across a topology dimension (a failure domain).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpreadPolicy {
    /// The label key whose distinct values are the failure domains, e.g.
    /// `"zone"`. A candidate lacking this label cannot satisfy the spread and is
    /// excluded from placement under this policy.
    pub domain: String,
    /// If true, every replica must occupy a *distinct* domain — fewer available
    /// domains than the replication factor is an error. If false, spreading is
    /// best-effort: domains are filled as evenly as possible, doubling up only
    /// once every domain holds a replica.
    pub strict: bool,
}

/// A named placement policy (a "placement group"): how many replicas a tablet
/// has, which nodes may host them (residency), and how they spread across
/// failure domains.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementPolicy {
    /// Policy name, for diagnostics and operator reference.
    pub name: String,
    /// Desired number of replicas.
    pub replication_factor: usize,
    /// Residency: a node is eligible only if, for every `(key, value)` here, its
    /// label `key` equals `value`. Empty ⇒ no residency restriction.
    pub required_labels: BTreeMap<String, String>,
    /// Residency **IN-set** (ADR 0075, G-01 stage G-c): a node is eligible only
    /// if, for every `(key, values)` here, its label `key` is *one of*
    /// `values`. Unlike [`required_labels`](Self::required_labels) (key ==
    /// one value) this can name "exactly these regions" — together with a
    /// strict [`SpreadPolicy`] over the same key it pins a tablet to one
    /// replica in each of a fixed set of regions
    /// ([`PlacementPolicy::mrsc`]). Empty ⇒ no restriction. **Replicated
    /// (class G, `Gate::GlobalTables`)**: `skip_serializing_if` empty, so a
    /// cluster with no pinned policy writes byte-identical policies.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub allowed_values: BTreeMap<String, BTreeSet<String>>,
    /// Optional failure-domain spread. `None` ⇒ replicas may share any domain.
    pub spread: Option<SpreadPolicy>,
}

impl PlacementPolicy {
    /// A policy with just a replication factor — no residency, no spread.
    pub fn simple(name: impl Into<String>, replication_factor: usize) -> Self {
        Self {
            name: name.into(),
            replication_factor,
            required_labels: BTreeMap::new(),
            allowed_values: BTreeMap::new(),
            spread: None,
        }
    }

    /// Builder: restrict placement to nodes whose label `key` is one of
    /// `values` (see [`allowed_values`](Self::allowed_values)).
    #[must_use]
    pub fn allow_values<I, V>(mut self, key: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<String>,
    {
        self.allowed_values
            .insert(key.into(), values.into_iter().map(Into::into).collect());
        self
    }

    /// The **MRSC** policy (ADR 0075 section 3.1): RF 3, restricted to nodes
    /// in exactly `regions` ([`REGION_LABEL`] IN-set), strictly one replica per
    /// region. With exactly three regions this is a perfect pin: the only
    /// admissible replica sets have one node in each named region.
    pub fn mrsc<I, V>(name: impl Into<String>, regions: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<String>,
    {
        Self::simple(name, 3)
            .allow_values(REGION_LABEL, regions)
            .spread_across(REGION_LABEL, true)
    }

    /// Whether this policy **pins** placement: an IN-set residency restriction
    /// combined with a strict spread. Pinned policies are repaired with
    /// [`replan_pinned`], which also fixes a survivor set that violates the
    /// strict spread (plain [`replan`] seeds survivors without re-validating
    /// it).
    #[must_use]
    pub fn is_pinned(&self) -> bool {
        !self.allowed_values.is_empty() && self.spread.as_ref().is_some_and(|s| s.strict)
    }

    /// Builder: restrict placement to nodes whose label `key` equals `value`.
    #[must_use]
    pub fn require_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.required_labels.insert(key.into(), value.into());
        self
    }

    /// Builder: spread replicas across distinct values of label `domain`.
    #[must_use]
    pub fn spread_across(mut self, domain: impl Into<String>, strict: bool) -> Self {
        self.spread = Some(SpreadPolicy {
            domain: domain.into(),
            strict,
        });
        self
    }

    /// Whether `candidate` passes this policy's residency constraint.
    #[must_use]
    pub fn admits(&self, candidate: &Candidate) -> bool {
        self.required_labels
            .iter()
            .all(|(k, v)| candidate.labels.get(k).is_some_and(|cv| cv == v))
            && self
                .allowed_values
                .iter()
                .all(|(k, vs)| candidate.labels.get(k).is_some_and(|cv| vs.contains(cv)))
    }
}

/// The well-known Kubernetes label key naming a node's region (G-01 stage G-a).
pub const REGION_LABEL: &str = "topology.kubernetes.io/region";

/// The well-known Kubernetes label key naming a node's zone, and the default
/// failure-domain key [`zone_spread_policy`] spreads over (G-01 stage G-a).
pub const ZONE_LABEL: &str = "topology.kubernetes.io/zone";

/// The default table placement policy: `replication_factor` replicas,
/// **best-effort spread** over [`ZONE_LABEL`] iff the live membership (`members`'
/// label maps) can actually honour it, otherwise [`PlacementPolicy::simple`].
///
/// "Can honour" means **every** member carries a non-empty zone value (a
/// spread policy excludes a node lacking the domain label from placement
/// entirely — see [`SpreadPolicy::domain`] — so installing one on a
/// partially-labelled cluster would silently shrink the candidate pool) and
/// there are at least `replication_factor` distinct zone values. The spread is
/// best-effort (`strict: false`), so after a whole zone is lost the repair pass
/// may double up in a surviving zone rather than refuse to heal.
///
/// Evaluated once, against the membership at table-creation time: a cluster
/// that gains zones later does not retro-upgrade existing tables' policies.
pub fn zone_spread_policy<'a>(
    name: impl Into<String>,
    replication_factor: usize,
    members: impl IntoIterator<Item = &'a BTreeMap<String, String>>,
) -> PlacementPolicy {
    let mut zones: BTreeSet<&str> = BTreeSet::new();
    let mut all_labelled = true;
    let mut any = false;
    for labels in members {
        any = true;
        match labels.get(ZONE_LABEL).map(String::as_str) {
            Some(z) if !z.is_empty() => {
                zones.insert(z);
            }
            _ => all_labelled = false,
        }
    }
    let policy = PlacementPolicy::simple(name, replication_factor);
    if any && all_labelled && zones.len() >= replication_factor && replication_factor > 1 {
        policy.spread_across(ZONE_LABEL, false)
    } else {
        policy
    }
}

/// Why a satisfying replica set could not be chosen.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlacementError {
    /// Fewer eligible candidates than the replication factor.
    #[error("not enough eligible candidates: need {needed}, have {eligible}")]
    InsufficientCandidates { needed: usize, eligible: usize },
    /// A strict spread needs at least `needed` distinct domains; only
    /// `available` have eligible candidates.
    #[error("not enough failure domains for strict spread: need {needed}, have {available}")]
    InsufficientDomains { needed: usize, available: usize },
}

/// Result alias for placement operations.
pub type Result<T> = std::result::Result<T, PlacementError>;

/// Choose a replica set for a fresh tablet under `policy`.
///
/// The returned ids are sorted and deduplicated.
///
/// # Errors
/// [`PlacementError`] when there are too few eligible candidates or, for a
/// strict spread, too few distinct domains.
pub fn select_replicas(candidates: &[Candidate], policy: &PlacementPolicy) -> Result<Vec<NodeId>> {
    choose(
        &eligible_domains(candidates, policy),
        &BTreeSet::new(),
        policy,
    )
}

/// Choose a replica set for a fresh tablet under `policy`, **balance-aware**
/// (ADR 0050 fork F5: a copy-based split's children are minted at their final
/// homes, so the mint itself must weigh current load the way
/// [`rebalance_step`] would only have fixed up afterwards). Identical hard
/// constraints to [`select_replicas`] (RF + residency + spread); within them,
/// prefers the least-loaded eligible nodes — `load` is the caller's per-node
/// replica count (a node absent from `load` counts as `0`, so a fresh member
/// is a genuine minimum, matching [`rebalance_step`]'s own seeding rule).
///
/// **Capacity-aware (ADR 0079):** nodes are ordered by the exact fill they would
/// have after one more replica, `(load + 1) / weight` (so a weight-200 node
/// takes twice the replicas of a weight-100 one), and a node already at its
/// `max_replicas` is not eligible (all-full yields `InsufficientCandidates`:
/// the caller refuses the mint). Equal weights reproduce the old order exactly.
///
/// Deterministic: ties break by node id, and [`choose`]'s own
/// least-loaded-domain-first greedy is unchanged — this only re-orders the
/// candidate list it feeds from node-sorted to `(load, node)`-sorted.
///
/// # Errors
/// As [`select_replicas`].
pub fn select_replicas_balanced(
    candidates: &[Candidate],
    policy: &PlacementPolicy,
    load: &BTreeMap<NodeId, usize>,
) -> Result<Vec<NodeId>> {
    let eligible = drop_full(eligible_domains(candidates, policy), candidates, load, &[]);
    let mut eligible: Vec<(NodeId, Domain, u64, usize)> = eligible
        .into_iter()
        .map(|(n, d)| {
            let w = candidate_for(candidates, &n)
                .map_or(u64::from(DEFAULT_WEIGHT), |c| c.capacity.effective_weight());
            let l = load.get(&n).copied().unwrap_or(0);
            (n, d, w, l)
        })
        .collect();
    // Order by the fill each node would have after taking one more replica
    // (`(load + 1) / weight`, exact), then node id. With equal weights this is
    // the old `(load, node)` order.
    eligible.sort_by(|(n, _, wn, ln), (m, _, wm, lm)| {
        fill_cmp(ln + 1, *wn, lm + 1, *wm).then_with(|| n.cmp(m))
    });
    let eligible: Vec<(NodeId, Domain)> = eligible.into_iter().map(|(n, d, _, _)| (n, d)).collect();
    choose(&eligible, &BTreeSet::new(), policy)
}

/// Recompute a replica set after a membership change, **preserving the current
/// replicas that are still eligible** so only failed/ineligible ones are
/// replaced (minimal data movement).
///
/// `current` is the tablet's present replica set; `candidates` is the fresh
/// pool (typically the now-healthy members). The result is sorted; if every
/// current replica is still eligible and the set already satisfies the policy,
/// it is returned unchanged (the caller can detect a no-op by comparing to a
/// sorted `current`).
///
/// # Errors
/// As [`select_replicas`].
pub fn replan(
    current: &[NodeId],
    candidates: &[Candidate],
    policy: &PlacementPolicy,
) -> Result<Vec<NodeId>> {
    replan_with_load(current, candidates, policy, &BTreeMap::new())
}

/// [`replan`] with the caller's per-node replica counts (`load`; absent = 0) so
/// a candidate's `max_replicas` is honoured (ADR 0079 section 1, rule 3): a
/// node at its ceiling is excluded from the pool, **except** that a node
/// already holding a replica of this tablet keeps it (capacity never shrinks a
/// set). Replacement choice is still by node id, exactly as [`replan`]: weights
/// shape the mint and the rebalance, not violation-driven repair.
///
/// # Errors
/// As [`replan`].
pub fn replan_with_load(
    current: &[NodeId],
    candidates: &[Candidate],
    policy: &PlacementPolicy,
    load: &BTreeMap<NodeId, usize>,
) -> Result<Vec<NodeId>> {
    let eligible = drop_full(
        eligible_domains(candidates, policy),
        candidates,
        load,
        current,
    );
    let eligible_ids: BTreeSet<NodeId> = eligible.iter().map(|(n, _)| n.clone()).collect();
    // Keep the survivors: current replicas that remain eligible.
    let keep: BTreeSet<NodeId> = current
        .iter()
        .filter(|n| eligible_ids.contains(*n))
        .cloned()
        .collect();
    choose(&eligible, &keep, policy)
}

/// [`replan`]'s **best-effort** sibling (issue #957): recompute a replica set
/// after a membership change exactly like `replan`, except when there are
/// fewer eligible candidates than the policy's `replication_factor` **and**
/// that pool is still bigger than the tablet's current eligible survivors —
/// i.e. real, if partial, progress is possible. In that one case this grows
/// the replica set to `eligible.len()` (every currently-eligible candidate)
/// instead of refusing outright, so a tablet in a cluster that is
/// chronically smaller than its own policy's target RF (e.g. RF 3 recorded
/// against a 2-node cluster — [`PlacementPolicy`] deliberately always
/// records the *target* RF, never the observed candidate count, so a wider
/// cluster can later grow the tablet the rest of the way) still gains every
/// replica it legitimately can, rather than being refused and left exactly
/// as under-replicated as before, forever, until the cluster happens to
/// reach the full RF in one step.
///
/// **Never used to shrink an already-at-capacity set.** When the eligible
/// pool is no larger than the current eligible survivors (`eligible.len()
/// <= keep.len()`) — the tablet already holds as many replicas as the
/// cluster can currently support, whether or not that satisfies the full
/// policy — this returns [`replan`]'s own plain, unmodified answer (`Err`
/// when the survivors alone can't reach `replication_factor`), which is
/// what preserves a stale, currently-ineligible replica rather than
/// dropping it the moment there's nothing better available to replace it
/// with. Growing "as far as possible" and never shrinking below what a
/// caller already achieved are two independent guarantees; this function
/// only ever relaxes the *growth* side of [`choose`]'s all-or-nothing rule.
///
/// A strict [`SpreadPolicy`] is still enforced against the SAME reduced
/// target (never the full `replication_factor`) in the best-effort branch —
/// best-effort growth never trades away a residency/spread guarantee for
/// the replicas it does place; [`PlacementError::InsufficientDomains`] can
/// still fire there exactly as it would for an ordinary `replan` call at
/// that smaller RF.
///
/// The control plane's own repair pass (`Metadata::reconcile` /
/// `PlacementView::reconcile`, `animus-control`) is this function's one
/// production caller — see that crate's `reconcile_placement` for the
/// wiring. [`select_replicas`]/[`select_replicas_balanced`] (fresh-tablet
/// placement) and the directed-Placing convergence phase's own `replan`/
/// `select_replicas` calls (ADR 0062 §2, ADR/`split_placing_reconcile`) are
/// deliberately untouched — a fresh mint or a directed-Placing target has
/// its own established "still write `target: None`/pause and retry" story
/// for "not enough candidates yet" (fork B), which this function's
/// broader, unconditional growth would silently change.
///
/// # Errors
/// As [`replan`] — only when even the best-effort target can't be reached
/// (a strict spread with too few available domains at that reduced target,
/// or, in the never-shrink branch, whatever `replan` itself would return).
#[must_use = "the caller must propose the returned replica set for it to take effect"]
pub fn replan_repair(
    current: &[NodeId],
    candidates: &[Candidate],
    policy: &PlacementPolicy,
) -> Result<Vec<NodeId>> {
    replan_repair_with_load(current, candidates, policy, &BTreeMap::new())
}

/// [`replan_repair`] with the caller's per-node replica counts, honouring
/// `max_replicas` strictly (ADR 0079 section 1 and Maintainer decision Q3): a
/// node at its ceiling is never chosen, even to restore the replication factor.
/// Repair grows as far as the caps allow and the tablet stays under-replicated
/// otherwise (the caller reports it). A node already holding a replica keeps it.
///
/// # Errors
/// As [`replan_repair`].
#[must_use = "the caller must propose the returned replica set for it to take effect"]
pub fn replan_repair_with_load(
    current: &[NodeId],
    candidates: &[Candidate],
    policy: &PlacementPolicy,
    load: &BTreeMap<NodeId, usize>,
) -> Result<Vec<NodeId>> {
    let eligible = drop_full(
        eligible_domains(candidates, policy),
        candidates,
        load,
        current,
    );
    let eligible_ids: BTreeSet<NodeId> = eligible.iter().map(|(n, _)| n.clone()).collect();
    let keep: BTreeSet<NodeId> = current
        .iter()
        .filter(|n| eligible_ids.contains(*n))
        .cloned()
        .collect();
    if eligible.len() > keep.len() && eligible.len() < policy.replication_factor {
        let capped = PlacementPolicy {
            replication_factor: eligible.len(),
            ..policy.clone()
        };
        choose(&eligible, &keep, &capped)
    } else {
        choose(&eligible, &keep, policy)
    }
}

/// [`replan_repair`]'s sibling for a **pinned** policy
/// ([`PlacementPolicy::is_pinned`]; ADR 0075, G-01 stage G-c): recompute a
/// replica set keeping the eligible survivors, but first **drop survivors that
/// duplicate a strict spread domain** (keeping the lowest node id per domain)
/// so a skewed starting set (two replicas in one region) converges to one per
/// region. Plain [`replan`]/[`replan_repair`] deliberately seed survivors
/// without re-validating the strict spread (documented, tested behaviour for
/// unpinned policies, which this leaves untouched); a pinned policy cannot
/// afford that, because there is no other path that would ever fix the set.
///
/// There is **no best-effort growth**: a pinned policy that cannot be
/// satisfied (a pinned region has no eligible node) is an error
/// ([`PlacementError::InsufficientDomains`]), and the caller proposes
/// nothing. The replica waits for its region rather than moving to another
/// (the MRSC contract is one replica per region). Repair *within* a region
/// (a node replaced by another node of the same region) works.
///
/// For a policy with no strict spread this equals [`replan`].
///
/// # Errors
/// As [`replan`].
pub fn replan_pinned(
    current: &[NodeId],
    candidates: &[Candidate],
    policy: &PlacementPolicy,
) -> Result<Vec<NodeId>> {
    replan_pinned_with_load(current, candidates, policy, &BTreeMap::new())
}

/// [`replan_pinned`] with per-node replica counts: capacity is applied inside
/// the pinned candidate set (ADR 0079), so a Region whose nodes are all at
/// `max_replicas` yields [`PlacementError::InsufficientDomains`] and the
/// replica waits.
///
/// # Errors
/// As [`replan_pinned`].
pub fn replan_pinned_with_load(
    current: &[NodeId],
    candidates: &[Candidate],
    policy: &PlacementPolicy,
    load: &BTreeMap<NodeId, usize>,
) -> Result<Vec<NodeId>> {
    let eligible = drop_full(
        eligible_domains(candidates, policy),
        candidates,
        load,
        current,
    );
    let domain_of: BTreeMap<&NodeId, &Domain> = eligible.iter().map(|(n, d)| (n, d)).collect();
    let strict = policy.spread.as_ref().is_some_and(|s| s.strict);
    let mut keep: BTreeSet<NodeId> = BTreeSet::new();
    let mut taken_domains: BTreeSet<&Domain> = BTreeSet::new();
    // BTreeSet iteration is ascending, so the lowest id per domain wins.
    let survivors: BTreeSet<&NodeId> = current.iter().collect();
    for n in survivors {
        let Some(d) = domain_of.get(n) else { continue };
        if strict && !taken_domains.insert(*d) {
            continue; // a second survivor in a strict domain: dropped
        }
        keep.insert(n.clone());
    }
    choose(&eligible, &keep, policy)
}

/// One step of **load rebalancing** across the candidate nodes (ADR 0029): move a
/// single replica of a single tablet from a most-loaded node to a least-loaded
/// one, iff doing so strictly improves balance and keeps every policy satisfied.
/// Returns `Some((tablet, new_sorted_replica_set))` for the one move to make, or
/// `None` when the cluster is already balanced (max−min replica count ≤ 1 across
/// candidates) or no policy-legal move exists.
///
/// Unlike [`replan`] — which is **violation-driven** (it only moves a replica off
/// a failed/ineligible node) — this is **balance-driven**: it moves *healthy*
/// replicas onto under-loaded nodes so that a cluster grown from N to M members
/// eventually spreads its existing tablets onto the new members. It is the
/// counterpart the control plane's reconciler calls once repair has nothing to do.
///
/// Algorithm:
/// - Seed a per-node replica count at `0` for **every** candidate (so a freshly
///   added, empty node participates as a genuine minimum), then `+1` for each
///   replica of each tablet whose *current* replica set already satisfies its
///   policy. A tablet whose set violates its policy is skipped entirely — bringing
///   it into compliance is [`replan`]'s / the reconciler's job, not rebalance's, so
///   this function is robust to being handed a mixed input.
/// - Consider candidate **sources** in `(count desc, node id asc)` order and
///   **destinations** in `(count asc, node id asc)` order, only for a `(src, dst)`
///   pair with `count[src] − count[dst] ≥ 2` (the per-pair form of "max − min ≥ 2":
///   moving one replica src→dst strictly reduces the sum-of-squares of the counts,
///   which is what guarantees termination — repeated application converges to
///   max − min ≤ 1 and never oscillates).
/// - For that pair, scan the eligible tablets in `K` order for the first with a
///   replica on `src` where `dst` is not already a replica, `dst` is admitted by
///   the policy, and the **post-move** set (src's replica swapped for dst) still
///   satisfies the policy (see [`set_satisfies`]) without worsening best-effort
///   spread. Return that move; at most **one** move per call (a deliberate
///   one-CAS-per-evaluation churn bound).
///
/// **Capacity-aware (ADR 0079):** "load" is `fill = replicas / weight` (exact
/// rational, [`Capacity`]); sources are ordered fill-descending and destinations
/// fill-ascending (node id breaks ties), and a pair is considered only when the
/// move strictly reduces the pair's larger fill, `(dst + 1) / w_dst < src /
/// w_src` (equal weights: `src >= dst + 2`, i.e. exactly the rule above). A
/// node at its `max_replicas` is never a destination; a node strictly above it
/// (the ceiling was lowered) is a source for any destination with room. The
/// terminal state (no spread, no binding ceiling, every pair movable) is within
/// `1 / w_dst` of proportional for every pair.
///
/// Fully deterministic (only `BTreeMap`/`BTreeSet` + stable sorts, no clock/RNG),
/// so it returns the identical move on every replica and under input permutation.
#[must_use]
pub fn rebalance_step<K: Ord + Copy>(
    tablets: &[(K, &[NodeId], &PlacementPolicy)],
    candidates: &[Candidate],
) -> Option<(K, Vec<NodeId>)> {
    RebalanceState::new(tablets, candidates).next_move()
}

/// The moves repeated calls of [`rebalance_step`] would make, computed in one
/// pass (issue #1192): `rebalance_step`, then apply its move to the input,
/// then `rebalance_step` again, up to `max_moves` times or until it returns
/// `None`. The result is **move for move identical** to that loop (an
/// equivalence test pins it), but the per-node load counts and the
/// per-node tablet lists are built once and updated incrementally instead of
/// being recomputed over every tablet per move, so converging a grown cluster
/// costs O(tablets + moves × search) instead of O(moves × tablets).
///
/// A tablet may appear more than once (moved again after an earlier move);
/// each entry carries that tablet's replica set **after** the move. The
/// caller applies the moves in order (and so must expect the tablet's epoch
/// to have advanced once per earlier move on the same tablet).
#[must_use]
pub fn rebalance_plan<K: Ord + Copy>(
    tablets: &[(K, &[NodeId], &PlacementPolicy)],
    candidates: &[Candidate],
    max_moves: usize,
) -> Vec<(K, Vec<NodeId>)> {
    let mut state = RebalanceState::new(tablets, candidates);
    let mut plan = Vec::new();
    while plan.len() < max_moves {
        let Some(mv) = state.next_move() else { break };
        plan.push(mv);
    }
    plan
}

/// The incremental working set behind [`rebalance_step`] / [`rebalance_plan`]:
/// per-node replica counts and, per node, the (K-ordered) positions of the
/// policy-satisfying tablets that have a replica there. The method
/// `next_move` picks exactly the move [`rebalance_step`]
/// documents and then applies it to this state.
struct RebalanceState<'a, K> {
    candidates: &'a [Candidate],
    /// Eligible tablets (current set satisfies the policy), sorted by `K`.
    eligible: Vec<(K, Vec<NodeId>, &'a PlacementPolicy)>,
    counts: BTreeMap<NodeId, usize>,
    caps: BTreeMap<NodeId, Capacity>,
    /// node -> indices into `eligible` of the tablets with a replica on it.
    on_node: BTreeMap<NodeId, BTreeSet<usize>>,
}

impl<'a, K: Ord + Copy> RebalanceState<'a, K> {
    fn new(tablets: &[(K, &[NodeId], &'a PlacementPolicy)], candidates: &'a [Candidate]) -> Self {
        // Per-node replica counts, seeded 0 for every candidate so an empty new
        // node is a genuine minimum (a destination), not simply absent.
        let mut counts: BTreeMap<NodeId, usize> =
            candidates.iter().map(|c| (c.node.clone(), 0)).collect();

        // Only tablets whose *current* set already satisfies their policy count
        // toward load or are eligible to move; a violating set is the repair
        // reconciler's job.
        let mut eligible: Vec<(K, Vec<NodeId>, &PlacementPolicy)> = tablets
            .iter()
            .filter(|(_, replicas, policy)| set_satisfies(replicas, candidates, policy))
            .map(|(k, replicas, policy)| (*k, replicas.to_vec(), *policy))
            .collect();
        eligible.sort_by_key(|(k, _, _)| *k);

        let mut on_node: BTreeMap<NodeId, BTreeSet<usize>> = BTreeMap::new();
        for (i, (_, replicas, _)) in eligible.iter().enumerate() {
            for r in replicas {
                if let Some(c) = counts.get_mut(r) {
                    *c += 1;
                }
                on_node.entry(r.clone()).or_default().insert(i);
            }
        }
        RebalanceState {
            candidates,
            eligible,
            counts,
            caps: capacity_map(candidates),
            on_node,
        }
    }

    /// Find the next move (see [`rebalance_step`]'s algorithm) and apply it to
    /// this state.
    fn next_move(&mut self) -> Option<(K, Vec<NodeId>)> {
        let candidates = self.candidates;
        // Sources most-loaded first; destinations least-loaded first (id-asc ties).
        let mut sources: Vec<(NodeId, usize)> =
            self.counts.iter().map(|(n, &c)| (n.clone(), c)).collect();
        let mut dests = sources.clone();
        sort_by_fill(&mut sources, &self.caps, true);
        sort_by_fill(&mut dests, &self.caps, false);

        for (src, src_count) in &sources {
            let Some(on_src) = self.on_node.get(src) else {
                continue;
            };
            for (dst, dst_count) in &dests {
                // Only a pair whose imbalance a single move strictly reduces.
                if !pair_movable(src, *src_count, dst, *dst_count, &self.caps) {
                    continue;
                }
                let Some(dst_cand) = candidate_for(candidates, dst) else {
                    continue;
                };
                // Tablets with a replica on `src`, in `K` order.
                for &i in on_src {
                    let (k, replicas, policy) = &self.eligible[i];
                    if replicas.contains(dst) || !policy.admits(dst_cand) {
                        continue;
                    }
                    let mut post: Vec<NodeId> =
                        replicas.iter().filter(|&n| n != src).cloned().collect();
                    post.push(dst.clone());
                    post.sort_unstable();
                    if !set_satisfies(&post, candidates, policy) {
                        continue;
                    }
                    // Best-effort spread: never make the worst domain worse.
                    if let Some(sp) = &policy.spread
                        && !sp.strict
                        && max_per_domain(&post, candidates, sp)
                            > max_per_domain(replicas, candidates, sp)
                    {
                        continue;
                    }
                    let k = *k;
                    let (src, dst) = (src.clone(), dst.clone());
                    self.apply(i, &src, &dst, post.clone());
                    return Some((k, post));
                }
            }
        }
        None
    }

    /// Account for moving tablet `i`'s replica `src` -> `dst` (new set `post`).
    fn apply(&mut self, i: usize, src: &NodeId, dst: &NodeId, post: Vec<NodeId>) {
        self.eligible[i].1 = post;
        if let Some(c) = self.counts.get_mut(src) {
            *c -= 1;
        }
        if let Some(c) = self.counts.get_mut(dst) {
            *c += 1;
        }
        if let Some(set) = self.on_node.get_mut(src) {
            set.remove(&i);
        }
        self.on_node.entry(dst.clone()).or_default().insert(i);
    }
}

/// The pre-#1192 `rebalance_step` body, kept verbatim as the reference the
/// equivalence tests compare [`rebalance_step`] / [`rebalance_plan`] against.
/// Not part of the API.
#[doc(hidden)]
#[must_use]
pub fn rebalance_step_reference<K: Ord + Copy>(
    tablets: &[(K, &[NodeId], &PlacementPolicy)],
    candidates: &[Candidate],
) -> Option<(K, Vec<NodeId>)> {
    // Per-node replica counts, seeded 0 for every candidate so an empty new node
    // is a genuine minimum (a destination), not simply absent.
    let mut counts: BTreeMap<NodeId, usize> =
        candidates.iter().map(|c| (c.node.clone(), 0)).collect();

    // Only tablets whose *current* set already satisfies their policy count toward
    // load or are eligible to move; a violating set is the repair reconciler's job.
    let mut eligible: Vec<(K, &[NodeId], &PlacementPolicy)> = tablets
        .iter()
        .filter(|(_, replicas, policy)| set_satisfies(replicas, candidates, policy))
        .map(|(k, replicas, policy)| (*k, *replicas, *policy))
        .collect();
    eligible.sort_by_key(|(k, _, _)| *k);

    for (_, replicas, _) in &eligible {
        for r in *replicas {
            if let Some(c) = counts.get_mut(r) {
                *c += 1;
            }
        }
    }

    // Sources most-loaded first; destinations least-loaded first (id-asc ties).
    let mut sources: Vec<(NodeId, usize)> = counts.iter().map(|(n, &c)| (n.clone(), c)).collect();
    let mut dests = sources.clone();
    let caps = capacity_map(candidates);
    sort_by_fill(&mut sources, &caps, true);
    sort_by_fill(&mut dests, &caps, false);

    for (src, src_count) in &sources {
        for (dst, dst_count) in &dests {
            // Only a pair whose imbalance a single move strictly reduces.
            if !pair_movable(src, *src_count, dst, *dst_count, &caps) {
                continue;
            }
            for (k, replicas, policy) in &eligible {
                if !replicas.contains(src) || replicas.contains(dst) {
                    continue;
                }
                let Some(dst_cand) = candidate_for(candidates, dst) else {
                    continue;
                };
                if !policy.admits(dst_cand) {
                    continue;
                }
                let mut post: Vec<NodeId> =
                    replicas.iter().filter(|&n| n != src).cloned().collect();
                post.push(dst.clone());
                post.sort_unstable();
                if !set_satisfies(&post, candidates, policy) {
                    continue;
                }
                // Best-effort spread: never make the worst domain worse.
                if let Some(sp) = &policy.spread
                    && !sp.strict
                    && max_per_domain(&post, candidates, sp)
                        > max_per_domain(replicas, candidates, sp)
                {
                    continue;
                }
                return Some((*k, post));
            }
        }
    }
    None
}

/// Each candidate's capacity by node id (the first entry wins on a duplicate id,
/// like [`candidate_for`]).
fn capacity_map(candidates: &[Candidate]) -> BTreeMap<NodeId, Capacity> {
    let mut m = BTreeMap::new();
    for c in candidates {
        m.entry(c.node.clone()).or_insert(c.capacity);
    }
    m
}

/// Sort `(node, count)` pairs by fill: highest first (`desc`) or lowest first,
/// node id ascending on ties. Equal weights reduce to `(count, node)`.
fn sort_by_fill(v: &mut [(NodeId, usize)], caps: &BTreeMap<NodeId, Capacity>, desc: bool) {
    let w = |n: &NodeId| {
        caps.get(n)
            .map_or(u64::from(DEFAULT_WEIGHT), |c| c.effective_weight())
    };
    v.sort_by(|a, b| {
        let o = fill_cmp(a.1, w(&a.0), b.1, w(&b.0));
        (if desc { o.reverse() } else { o }).then_with(|| a.0.cmp(&b.0))
    });
}

/// Whether moving one replica `src` -> `dst` is worth considering (ADR 0079
/// rule 4/5): `dst` is not at its ceiling, and either the move strictly
/// reduces the pair's larger fill (`(dst+1)/w_dst < src/w_src`; with equal
/// weights exactly `src >= dst + 2`) or `src` is over its ceiling (a drain).
/// Both kinds strictly decrease `(total overage, descending fill vector)`
/// lexicographically, so repeated application terminates.
fn pair_movable(
    src: &NodeId,
    src_count: usize,
    dst: &NodeId,
    dst_count: usize,
    caps: &BTreeMap<NodeId, Capacity>,
) -> bool {
    if src == dst {
        return false;
    }
    let cap = |n: &NodeId| caps.get(n).copied().unwrap_or(Capacity::DEFAULT);
    let (sc, dc) = (cap(src), cap(dst));
    if dc.is_full(dst_count) {
        return false;
    }
    sc.is_over(src_count)
        || fill_cmp(
            dst_count + 1,
            dc.effective_weight(),
            src_count,
            sc.effective_weight(),
        )
        .is_lt()
}

/// The candidate for `node`, if it is in the pool.
fn candidate_for<'a>(candidates: &'a [Candidate], node: &NodeId) -> Option<&'a Candidate> {
    candidates.iter().find(|c| &c.node == node)
}

/// Whether `replicas` satisfies `policy`'s **hard** constraints under the current
/// candidate pool: exactly `replication_factor` replicas, every one a candidate
/// admitted by residency, and — for a **strict** spread — each in a distinct
/// failure domain (with the domain label present). Best-effort spread imposes no
/// hard constraint here (doubling up is allowed); its "never worsen" rule is
/// applied per-move in [`rebalance_step`], which needs the pre-move set to compare
/// against. Used both to decide which tablets *count* toward load and to validate
/// a candidate post-move set.
///
/// Note this is **not** the same as `replan(replicas) == replicas`: `replan` seeds
/// its survivors without re-validating spread, so it would wrongly accept a
/// spread-violating survivor set (see `replan_keeps_survivors_and_replaces_only_the_lost`).
fn set_satisfies(replicas: &[NodeId], candidates: &[Candidate], policy: &PlacementPolicy) -> bool {
    if replicas.len() != policy.replication_factor {
        return false;
    }
    for r in replicas {
        match candidate_for(candidates, r) {
            Some(c) if policy.admits(c) => {}
            _ => return false,
        }
    }
    if let Some(sp) = &policy.spread
        && sp.strict
    {
        let mut seen: BTreeSet<&String> = BTreeSet::new();
        for r in replicas {
            let Some(c) = candidate_for(candidates, r) else {
                return false;
            };
            let Some(domain) = c.labels.get(&sp.domain) else {
                return false;
            };
            if !seen.insert(domain) {
                return false; // two replicas in one strict domain
            }
        }
    }
    true
}

/// The greatest number of `replicas` sharing any one failure domain (via each
/// replica's candidate's `sp.domain` label). Used to enforce that a best-effort
/// spread move never increases the worst domain's replica count.
fn max_per_domain(replicas: &[NodeId], candidates: &[Candidate], sp: &SpreadPolicy) -> usize {
    let mut counts: BTreeMap<Option<&String>, usize> = BTreeMap::new();
    for r in replicas {
        let domain = candidate_for(candidates, r).and_then(|c| c.labels.get(&sp.domain));
        *counts.entry(domain).or_default() += 1;
    }
    counts.values().copied().max().unwrap_or(0)
}

/// The eligible candidates with their spread-domain value, sorted by node id.
/// Under residency, ineligible nodes are dropped; under a spread policy, a node
/// lacking the spread label is also dropped (it cannot be placed in a domain).
fn eligible_domains(candidates: &[Candidate], policy: &PlacementPolicy) -> Vec<(NodeId, Domain)> {
    let mut out: Vec<(NodeId, Domain)> = candidates
        .iter()
        .filter(|c| policy.admits(c))
        .filter_map(|c| match &policy.spread {
            Some(sp) => c
                .labels
                .get(&sp.domain)
                .map(|d| (c.node.clone(), Some(d.clone()))),
            None => Some((c.node.clone(), None)),
        })
        .collect();
    out.sort_by(|(n, _), (m, _)| n.cmp(m));
    out.dedup_by(|(n, _), (m, _)| n == m);
    out
}

/// A failure domain value, or `None` when the policy has no spread (one bucket).
type Domain = Option<String>;

/// Core selection: fill up to `replication_factor` replicas from `eligible`,
/// seeding `must_keep` first, then greedily picking from the **least-loaded**
/// domain (ties broken by domain value, then node id) so replicas spread as
/// evenly as the policy allows. Deterministic for fixed inputs.
fn choose(
    eligible: &[(NodeId, Domain)],
    must_keep: &BTreeSet<NodeId>,
    policy: &PlacementPolicy,
) -> Result<Vec<NodeId>> {
    let rf = policy.replication_factor;

    // Bucket eligible nodes by domain; vectors stay node-sorted (input is).
    let mut domains: BTreeMap<Domain, Vec<NodeId>> = BTreeMap::new();
    for (node, domain) in eligible {
        domains
            .entry(domain.clone())
            .or_default()
            .push(node.clone());
    }
    let domain_of: BTreeMap<NodeId, Domain> = eligible
        .iter()
        .map(|(n, d)| (n.clone(), d.clone()))
        .collect();

    if let Some(sp) = &policy.spread
        && sp.strict
        && domains.len() < rf
    {
        return Err(PlacementError::InsufficientDomains {
            needed: rf,
            available: domains.len(),
        });
    }

    let mut count: BTreeMap<Domain, usize> = domains.keys().map(|k| (k.clone(), 0)).collect();
    let mut taken: BTreeSet<NodeId> = BTreeSet::new();
    let mut chosen: Vec<NodeId> = Vec::new();

    // Seed the survivors first (BTreeSet iterates sorted ⇒ deterministic).
    for node in must_keep {
        if chosen.len() >= rf {
            break;
        }
        if let Some(domain) = domain_of.get(node) {
            chosen.push(node.clone());
            taken.insert(node.clone());
            *count.get_mut(domain).expect("domain counted") += 1;
        }
    }

    // Greedily fill from the least-loaded domain that still has a free node.
    while chosen.len() < rf {
        let Some(domain) = least_loaded_domain(&domains, &count, &taken) else {
            break; // no candidates left
        };
        let node = domains[&domain]
            .iter()
            .find(|n| !taken.contains(*n))
            .expect("domain had a free node")
            .clone();
        chosen.push(node.clone());
        taken.insert(node);
        *count.get_mut(&domain).expect("domain counted") += 1;
    }

    if chosen.len() < rf {
        return Err(PlacementError::InsufficientCandidates {
            needed: rf,
            eligible: eligible.len(),
        });
    }

    chosen.sort_unstable();
    Ok(chosen)
}

/// The domain with a free node and the smallest `(replica count, domain)` —
/// iterating in `BTreeMap` key order makes the tie-break the domain value.
fn least_loaded_domain(
    domains: &BTreeMap<Domain, Vec<NodeId>>,
    count: &BTreeMap<Domain, usize>,
    taken: &BTreeSet<NodeId>,
) -> Option<Domain> {
    let mut best: Option<&Domain> = None;
    for (domain, nodes) in domains {
        let has_free = nodes.iter().any(|n| !taken.contains(n));
        if has_free && best.is_none_or(|b| count[domain] < count[b]) {
            best = Some(domain);
        }
    }
    best.cloned()
}
