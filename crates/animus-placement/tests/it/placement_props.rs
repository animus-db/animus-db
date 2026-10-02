//! Randomized-topology property tests for `rebalance_step` and `replan`
//! (ADR 0061 rung A1). The existing `tests/placement.rs`/`tests/rebalance.rs`
//! are fixed-scenario example tests; these generate random node sets, RF
//! values, residency labels, and failure domains to back the general claims
//! the root `CLAUDE.md` and this crate's own doc comments make.
//!
//! **A caveat these tests exist to pin down, discovered by reading
//! `rebalance_step`'s implementation rather than guessing:** the "repeated
//! application converges to max−min ≤ 1" claim is only unconditionally true
//! for a policy with **no** failure-domain spread. With a `SpreadPolicy`
//! (strict or best-effort) configured, a move that would improve raw node
//! balance can be legally blocked by the domain guard on *every* eligible
//! tablet — `rebalance_step`'s own doc already says as much ("`None` ...
//! or no policy-legal move exists"). So the balance property below is
//! asserted only when `policy.spread.is_none()`; for a spread policy we
//! instead assert what the function actually promises unconditionally:
//! at most one move per call, termination in a provably bounded number of
//! steps, and that no move ever worsens spread/residency compliance.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use animus_env::{NodeId, nid};
use animus_placement::{
    Candidate, PlacementPolicy, SpreadPolicy, rebalance_step, replan, select_replicas,
    select_replicas_balanced,
};
use proptest::prelude::*;

const REGIONS: &[&str] = &["eu", "us"];
const ZONES: &[&str] = &["a", "b", "c", "d"];

/// A pool of `min..=max` candidates with distinct ids and random `region`/
/// `zone` labels drawn from a small vocabulary (small enough that residency
/// and spread constraints are exercised, not vacuous).
fn candidate_pool_strategy(min: usize, max: usize) -> impl Strategy<Value = Vec<Candidate>> {
    proptest::collection::btree_set(1u64..1000, min..=max).prop_flat_map(|ids| {
        let ids: Vec<u64> = ids.into_iter().collect();
        let n = ids.len();
        (
            proptest::collection::vec(0..REGIONS.len(), n),
            proptest::collection::vec(0..ZONES.len(), n),
        )
            .prop_map(move |(regions, zones)| {
                ids.iter()
                    .zip(regions)
                    .zip(zones)
                    .map(|((&id, r), z)| {
                        let mut labels = BTreeMap::new();
                        labels.insert("region".to_string(), REGIONS[r].to_string());
                        labels.insert("zone".to_string(), ZONES[z].to_string());
                        Candidate::new(nid(id), labels)
                    })
                    .collect()
            })
    })
}

/// A policy "skeleton" (residency + spread shape, `replication_factor` set to
/// a placeholder `1`) — RF is chosen afterward once the caller knows how many
/// nodes the skeleton actually admits in a given pool.
///
/// The residency requirement, when generated, is always a `region` value
/// that's actually present in `pool` — a fixed `"eu"` requirement would make
/// a sizeable fraction of generated pools residency-infeasible (no eligible
/// node at all), which starves the property loop via `prop_assume!` discards
/// instead of testing anything. Deriving the requirement from the pool keeps
/// every generated case meaningful.
fn policy_skeleton_strategy(pool: &[Candidate]) -> impl Strategy<Value = PlacementPolicy> + use<> {
    let region_values: Vec<String> = pool.iter().map(|c| c.labels["region"].clone()).collect();
    (
        proptest::option::of(proptest::sample::select(region_values)),
        0..3u8,
    )
        .prop_map(|(residency, spread_kind)| {
            let mut p = PlacementPolicy::simple("p", 1);
            if let Some(region) = residency {
                p = p.require_label("region", region);
            }
            p = match spread_kind {
                1 => p.spread_across("zone", false),
                2 => p.spread_across("zone", true),
                _ => p,
            };
            p
        })
}

/// The greatest number of `replicas` sharing one failure domain, per `sp` —
/// a test-local mirror of the crate's private `max_per_domain`, used only to
/// check the "never worsens best-effort spread" property from outside.
fn max_per_domain(replicas: &[NodeId], pool: &[Candidate], sp: &SpreadPolicy) -> usize {
    let mut counts: BTreeMap<Option<&String>, usize> = BTreeMap::new();
    for r in replicas {
        let domain = pool
            .iter()
            .find(|c| &c.node == r)
            .and_then(|c| c.labels.get(&sp.domain));
        *counts.entry(domain).or_default() += 1;
    }
    counts.values().copied().max().unwrap_or(0)
}

/// Per-node replica counts across `pool`, seeded `0` for every candidate —
/// mirrors `rebalance_step`'s own seeding rule so a test-computed bound
/// matches what the function actually iterates over.
fn counts_of(pool: &[Candidate], tablets: &[(u32, Vec<NodeId>)]) -> BTreeMap<NodeId, usize> {
    let mut counts: BTreeMap<NodeId, usize> = pool.iter().map(|c| (c.node.clone(), 0)).collect();
    for (_, replicas) in tablets {
        for r in replicas {
            if let Some(c) = counts.get_mut(r) {
                *c += 1;
            }
        }
    }
    counts
}

fn sum_of_squares(counts: &BTreeMap<NodeId, usize>) -> u64 {
    counts.values().map(|&v| (v as u64) * (v as u64)).sum()
}

/// A generated topology: the full candidate pool, the policy under test, and
/// the tablets initially placed over (a subset of) it.
type Topology = (Vec<Candidate>, PlacementPolicy, Vec<(u32, Vec<NodeId>)>);

/// A random `(full_pool, initial_pool, policy, tablets)` topology: `policy`'s
/// RF and spread are chosen to be feasible over `full_pool` (verified via
/// `select_replicas`, not guessed); `initial_pool` is a random, usually-
/// proper, subset of `full_pool` that every tablet is placed over — so
/// `full_pool`'s extra nodes are, by construction, initially idle, giving
/// `rebalance_step` genuine imbalance to fix (the "cluster grew" scenario
/// the function's own doc motivates it with).
fn topology_strategy(num_tablets_range: std::ops::Range<usize>) -> impl Strategy<Value = Topology> {
    // Stage 1: pick the pool + residency/spread shape, then a feasible RF
    // for that shape over the pool (checked via actual admits/domain
    // counting, not guessed) — `(full_pool, base_pool, policy)`.
    candidate_pool_strategy(2, 9)
        .prop_flat_map(|full_pool| {
            let skeleton_strategy = policy_skeleton_strategy(&full_pool);
            (Just(full_pool), skeleton_strategy)
        })
        .prop_flat_map(|(full_pool, skeleton)| {
            let eligible: Vec<NodeId> = full_pool
                .iter()
                .filter(|c| skeleton.admits(c))
                .map(|c| c.node.clone())
                .collect();
            let domains: BTreeSet<Option<&String>> = full_pool
                .iter()
                .filter(|c| skeleton.admits(c))
                .map(|c| match &skeleton.spread {
                    Some(sp) => c.labels.get(&sp.domain),
                    None => None,
                })
                .collect();
            let max_rf = if skeleton.spread.as_ref().is_some_and(|sp| sp.strict) {
                eligible.len().min(domains.len())
            } else {
                eligible.len()
            };
            if max_rf == 0 {
                // No feasible RF for this shape over this pool — a `rf: 0`
                // policy always fails `select_replicas`/`_balanced`, so
                // stage 3 below naturally builds zero tablets for it
                // instead of ever calling into placement with a bogus RF.
                return Just((full_pool.clone(), full_pool.clone(), skeleton.clone())).boxed();
            }
            let full_pool2 = full_pool.clone();
            (1..=max_rf)
                .prop_map(move |rf| {
                    let mut policy = skeleton.clone();
                    policy.replication_factor = rf;
                    (full_pool2.clone(), full_pool2.clone(), policy)
                })
                .boxed()
        })
        // Stage 2: carve `initial_pool` as a random (usually proper) subset
        // of the pool that every tablet is placed over, so `full_pool`'s
        // extra nodes start idle. The mask is keep-biased (independent
        // node drops otherwise blow past infeasibility fast: e.g. a
        // strict-spread RF that needs both of a pool's two eligible nodes
        // survives only a 25% independent 50/50 mask); and whenever the
        // masked subset itself turns out infeasible for `policy` — despite
        // `policy` being feasible over the *full* pool — this falls back to
        // the full pool rather than leaving `initial_pool` unusable, so
        // "no tablets get built" stays reserved for the genuine `num_tablets
        // == 0` case the caller's range can generate, not a starved mask.
        .prop_flat_map(|(full_pool, base_pool, policy)| {
            let n = base_pool.len();
            (
                proptest::collection::vec(proptest::bool::weighted(0.75), n),
                Just((full_pool, base_pool, policy)),
            )
                .prop_map(move |(mask, (full_pool, base_pool, policy))| {
                    let mut initial_pool: Vec<Candidate> = base_pool
                        .iter()
                        .zip(mask.iter())
                        .filter(|&(_, &keep)| keep)
                        .map(|(c, _)| c.clone())
                        .collect();
                    if initial_pool.is_empty() || select_replicas(&initial_pool, &policy).is_err() {
                        initial_pool = base_pool.clone();
                    }
                    (full_pool, initial_pool, policy)
                })
        })
        // Stage 3: pick the tablet count and materialize the tablets.
        .prop_flat_map(move |(full_pool, initial_pool, policy)| {
            num_tablets_range
                .clone()
                .prop_map(move |n| (full_pool.clone(), initial_pool.clone(), policy.clone(), n))
        })
        .prop_map(|(full_pool, initial_pool, policy, num_tablets)| {
            // Build each tablet's initial (compliant, over `initial_pool` only)
            // replica set via the balance-aware constructor, so tablets spread
            // themselves across whatever `initial_pool` offers before
            // `rebalance_step` ever sees `full_pool`'s extra nodes.
            let mut load: BTreeMap<NodeId, usize> = BTreeMap::new();
            let mut tablets: Vec<(u32, Vec<NodeId>)> = Vec::new();
            for i in 0..num_tablets {
                if let Ok(set) = select_replicas_balanced(&initial_pool, &policy, &load) {
                    for n in &set {
                        *load.entry(n.clone()).or_default() += 1;
                    }
                    tablets.push((i as u32, set));
                }
            }
            (full_pool, policy, tablets)
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// `rebalance_step` moves **at most one** replica of **one** tablet per
    /// call: whenever it returns `Some((k, new_set))`, `new_set` differs
    /// from the tablet's old set by exactly one node removed and one added
    /// (so `new_set.len() == old_set.len()` too). Checked on every step of
    /// a full convergence run, not just the first call.
    #[test]
    fn rebalance_step_moves_at_most_one_replica(
        (full_pool, policy, mut tablets) in topology_strategy(0..6)
    ) {
        prop_assume!(!tablets.is_empty());
        for _ in 0..(full_pool.len() * tablets.len() + 30) {
            let refs: Vec<(u32, &[NodeId], &PlacementPolicy)> =
                tablets.iter().map(|(k, r)| (*k, r.as_slice(), &policy)).collect();
            let Some((k, new_set)) = rebalance_step(&refs, &full_pool) else {
                break;
            };
            let slot = tablets.iter_mut().find(|(kk, _)| *kk == k).expect("moved tablet exists");
            let old_set = slot.1.clone();
            prop_assert_eq!(new_set.len(), old_set.len(), "move changed replica count");
            let removed = old_set.iter().filter(|n| !new_set.contains(n)).count();
            let added = new_set.iter().filter(|n| !old_set.contains(n)).count();
            prop_assert_eq!(removed, 1, "more than one replica removed in a single move");
            prop_assert_eq!(added, 1, "more than one replica added in a single move");
            slot.1 = new_set;
        }
    }

    /// Repeated application of `rebalance_step`:
    /// - terminates within a bound derived from the strictly-decreasing
    ///   sum-of-squares argument in the function's own doc comment (every
    ///   move reduces it by at least 2, so steps ≤ initial_ssq / 2);
    /// - never worsens failure-domain spread compliance on any single move
    ///   (strict: the post-move set stays domain-distinct; best-effort: the
    ///   worst domain's count never increases) — this is the "monotonic
    ///   non-worsening" property;
    /// - when the policy has **no** spread constraint, converges to
    ///   max−min ≤ 1 across the *eligible* (residency-admitted) candidates.
    ///   With a spread constraint this final balance is **not** asserted:
    ///   see the module doc for why a spread policy can legitimately leave
    ///   the cluster unbalanced (a real, structural limit of the function,
    ///   not weakened-to-vacuity test slack).
    #[test]
    fn rebalance_step_converges_and_never_worsens_spread(
        (full_pool, policy, mut tablets) in topology_strategy(1..6)
    ) {
        prop_assume!(!tablets.is_empty());
        let initial_counts = counts_of(&full_pool, &tablets);
        let bound = sum_of_squares(&initial_counts) / 2 + 5;

        let mut steps: u64 = 0;
        loop {
            let refs: Vec<(u32, &[NodeId], &PlacementPolicy)> =
                tablets.iter().map(|(k, r)| (*k, r.as_slice(), &policy)).collect();
            let Some((k, new_set)) = rebalance_step(&refs, &full_pool) else {
                break;
            };
            let slot = tablets.iter_mut().find(|(kk, _)| *kk == k).expect("moved tablet exists");
            let old_set = slot.1.clone();

            prop_assert_eq!(new_set.len(), policy.replication_factor);
            for n in &new_set {
                let cand = full_pool.iter().find(|c| &c.node == n).expect("chosen node is a candidate");
                prop_assert!(policy.admits(cand), "move placed a residency-ineligible node");
            }
            if let Some(sp) = &policy.spread {
                if sp.strict {
                    let mut seen: BTreeSet<&String> = BTreeSet::new();
                    for n in &new_set {
                        let cand = full_pool.iter().find(|c| &c.node == n).unwrap();
                        let domain = cand.labels.get(&sp.domain).expect("eligible node has the domain label");
                        prop_assert!(seen.insert(domain), "move broke strict spread: {:?}", new_set);
                    }
                } else {
                    let before = max_per_domain(&old_set, &full_pool, sp);
                    let after = max_per_domain(&new_set, &full_pool, sp);
                    prop_assert!(after <= before, "move worsened best-effort spread: {} -> {}", before, after);
                }
            }

            slot.1 = new_set;
            steps += 1;
            prop_assert!(
                steps <= bound,
                "did not converge within the sum-of-squares bound ({bound} steps)"
            );
        }

        if policy.spread.is_none() {
            let final_counts = counts_of(&full_pool, &tablets);
            let eligible_vals: Vec<usize> = full_pool
                .iter()
                .filter(|c| policy.admits(c))
                .map(|c| final_counts[&c.node])
                .collect();
            if let (Some(min), Some(max)) = (eligible_vals.iter().min(), eligible_vals.iter().max()) {
                prop_assert!(
                    max - min <= 1,
                    "not balanced among eligible candidates after convergence: {:?}",
                    final_counts
                );
            }
        }
    }

    /// `replan` under random single-round failures: some of a compliant
    /// tablet's current replicas are dropped from the candidate pool (the
    /// crate's own convention for "down"/decommissioning — liveness is the
    /// caller's job, ADR 0005). Whenever `replan` returns `Ok`, the result:
    /// - never contains a dropped (down) node — `replan` can only choose
    ///   from the `candidates` it's given, so a caller that omits down nodes
    ///   gets a result that provably never lands on one;
    /// - has exactly `replication_factor` members, all residency-admitted;
    /// - keeps every replica that is both still in the pool and was already
    ///   a replica (minimal churn — only the dropped ones are replaced).
    ///
    /// Strict-spread domain-uniqueness of the result is checked only when
    /// the pre-failure `current` set was itself domain-distinct: `replan`
    /// documents (see `set_satisfies`'s doc in `lib.rs`) that it seeds
    /// survivors **without** re-validating spread, so it is not a general
    /// promise — this test's precondition names exactly the case where the
    /// crate's own comment says the guarantee holds.
    #[test]
    fn replan_invariants_under_random_failures(
        (full_pool, policy, tablets) in topology_strategy(1..2)
    ) {
        prop_assume!(!tablets.is_empty());
        let (_, current) = &tablets[0];
        prop_assume!(!current.is_empty());

        // Drop each current replica from the pool independently at random —
        // simulating a node failing/decommissioning.
        let drop_mask = current.len();
        for mask_bits in 0u32..(1u32 << drop_mask.min(4)) {
            let dropped: BTreeSet<NodeId> = current
                .iter()
                .enumerate()
                .filter(|(i, _)| mask_bits & (1 << i) != 0)
                .map(|(_, n)| n.clone())
                .collect();
            let surviving_pool: Vec<Candidate> = full_pool
                .iter()
                .filter(|c| !dropped.contains(&c.node))
                .cloned()
                .collect();

            let strict_spread_ok_precondition = match &policy.spread {
                Some(sp) if sp.strict => {
                    let mut seen: BTreeSet<&String> = BTreeSet::new();
                    current.iter().all(|n| {
                        full_pool
                            .iter()
                            .find(|c| &c.node == n)
                            .and_then(|c| c.labels.get(&sp.domain))
                            .is_some_and(|d| seen.insert(d))
                    })
                }
                _ => true,
            };

            let result = replan(current, &surviving_pool, &policy);
            match result {
                Ok(new_set) => {
                    prop_assert_eq!(new_set.len(), policy.replication_factor);
                    for n in &new_set {
                        prop_assert!(!dropped.contains(n), "replan placed a dropped/down node");
                        let cand = surviving_pool.iter().find(|c| &c.node == n)
                            .expect("result node is in the surviving pool");
                        prop_assert!(policy.admits(cand), "replan placed a residency-ineligible node");
                    }
                    // Churn minimization: every current replica that
                    // survived (not dropped) is kept.
                    for n in current {
                        if !dropped.contains(n) {
                            prop_assert!(
                                new_set.contains(n),
                                "replan needlessly moved a surviving replica {n}"
                            );
                        }
                    }
                    if strict_spread_ok_precondition
                        && let Some(sp) = &policy.spread
                        && sp.strict
                    {
                        let mut seen: BTreeSet<&String> = BTreeSet::new();
                        for n in &new_set {
                            let cand = surviving_pool.iter().find(|c| &c.node == n).unwrap();
                            let domain = cand.labels.get(&sp.domain).expect("admitted node has the domain label");
                            prop_assert!(seen.insert(domain), "replan result broke strict spread: {:?}", new_set);
                        }
                    }
                }
                Err(_) => {
                    // A legitimate infeasibility (too few survivors, or too
                    // few surviving domains for strict spread) — nothing
                    // more to check; `select_replicas`'s own tests already
                    // cover the exact error-variant contract.
                }
            }
        }
    }
}
