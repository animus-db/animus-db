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
}

impl Candidate {
    /// Build a candidate from a node id and its labels.
    pub fn new(node: NodeId, labels: BTreeMap<String, String>) -> Self {
        Self { node, labels }
    }
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
            spread: None,
        }
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
    let eligible = eligible_domains(candidates, policy);
    let eligible_ids: BTreeSet<NodeId> = eligible.iter().map(|(n, _)| *n).collect();
    // Keep the survivors: current replicas that remain eligible.
    let keep: BTreeSet<NodeId> = current
        .iter()
        .copied()
        .filter(|n| eligible_ids.contains(n))
        .collect();
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
/// Fully deterministic (only `BTreeMap`/`BTreeSet` + stable sorts, no clock/RNG),
/// so it returns the identical move on every replica and under input permutation.
#[must_use]
pub fn rebalance_step<K: Ord + Copy>(
    tablets: &[(K, &[NodeId], &PlacementPolicy)],
    candidates: &[Candidate],
) -> Option<(K, Vec<NodeId>)> {
    // Per-node replica counts, seeded 0 for every candidate so an empty new node
    // is a genuine minimum (a destination), not simply absent.
    let mut counts: BTreeMap<NodeId, usize> = candidates.iter().map(|c| (c.node, 0)).collect();

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
    let mut sources: Vec<(NodeId, usize)> = counts.iter().map(|(&n, &c)| (n, c)).collect();
    let mut dests = sources.clone();
    sources.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    dests.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

    for &(src, src_count) in &sources {
        for &(dst, dst_count) in &dests {
            // Only a pair whose imbalance a single move strictly reduces.
            if src == dst || src_count < dst_count + 2 {
                continue;
            }
            for (k, replicas, policy) in &eligible {
                if !replicas.contains(&src) || replicas.contains(&dst) {
                    continue;
                }
                let Some(dst_cand) = candidate_for(candidates, dst) else {
                    continue;
                };
                if !policy.admits(dst_cand) {
                    continue;
                }
                let mut post: Vec<NodeId> =
                    replicas.iter().copied().filter(|&n| n != src).collect();
                post.push(dst);
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

/// The candidate for `node`, if it is in the pool.
fn candidate_for(candidates: &[Candidate], node: NodeId) -> Option<&Candidate> {
    candidates.iter().find(|c| c.node == node)
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
        match candidate_for(candidates, *r) {
            Some(c) if policy.admits(c) => {}
            _ => return false,
        }
    }
    if let Some(sp) = &policy.spread
        && sp.strict
    {
        let mut seen: BTreeSet<&String> = BTreeSet::new();
        for r in replicas {
            let Some(c) = candidate_for(candidates, *r) else {
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
        let domain = candidate_for(candidates, *r).and_then(|c| c.labels.get(&sp.domain));
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
            Some(sp) => c.labels.get(&sp.domain).map(|d| (c.node, Some(d.clone()))),
            None => Some((c.node, None)),
        })
        .collect();
    out.sort_by_key(|(n, _)| *n);
    out.dedup_by_key(|(n, _)| *n);
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
        domains.entry(domain.clone()).or_default().push(*node);
    }
    let domain_of: BTreeMap<NodeId, Domain> =
        eligible.iter().map(|(n, d)| (*n, d.clone())).collect();

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
            chosen.push(*node);
            taken.insert(*node);
            *count.get_mut(domain).expect("domain counted") += 1;
        }
    }

    // Greedily fill from the least-loaded domain that still has a free node.
    while chosen.len() < rf {
        let Some(domain) = least_loaded_domain(&domains, &count, &taken) else {
            break; // no candidates left
        };
        let node = *domains[&domain]
            .iter()
            .find(|n| !taken.contains(n))
            .expect("domain had a free node");
        chosen.push(node);
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
