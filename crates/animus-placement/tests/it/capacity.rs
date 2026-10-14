//! Capacity-aware placement (ADR 0079 section 1, implementation-plan step 2):
//! weighted `select_replicas_balanced`, `max_replicas`-aware `replan*_with_load`
//! and weighted `rebalance_*`. Two families:
//!
//! - the **equal-weights guard**: with every node at the default capacity (or
//!   any uniform weight, no ceiling) each function returns what it returned
//!   before capacity existed. `OldRebalance` below is an independent,
//!   count-based copy of the pre-capacity algorithm, so this is not the crate
//!   comparing itself with itself;
//! - the **Tier 0 properties**: weighted convergence to within `1/min_weight` of
//!   proportional, ceilings respected (also by repair and rebalance), repair
//!   never shrinks a set, a full pinned Region fails cleanly.

use std::collections::{BTreeMap, BTreeSet};

use animus_env::{NodeId, nid};
use animus_placement::{
    Candidate, Capacity, PlacementError, PlacementPolicy, REGION_LABEL, rebalance_plan,
    rebalance_step, rebalance_step_reference, replan, replan_pinned, replan_pinned_with_load,
    replan_repair, replan_repair_with_load, replan_with_load, select_replicas,
    select_replicas_balanced,
};
use proptest::prelude::*;

use crate::placement_props::{Topology, candidate_pool_strategy, topology_strategy};

type Tablets = Vec<(u32, Vec<NodeId>)>;

fn with_caps(pool: &[Candidate], caps: &[Capacity]) -> Vec<Candidate> {
    pool.iter()
        .enumerate()
        .map(|(i, c)| c.clone().with_capacity(caps[i % caps.len()]))
        .collect()
}

fn uniform(pool: &[Candidate], weight: u32) -> Vec<Candidate> {
    with_caps(
        pool,
        &[Capacity {
            weight,
            max_replicas: None,
        }],
    )
}

fn refs<'a>(
    t: &'a Tablets,
    p: &'a PlacementPolicy,
) -> Vec<(u32, &'a [NodeId], &'a PlacementPolicy)> {
    t.iter().map(|(k, r)| (*k, r.as_slice(), p)).collect()
}

fn counts(pool: &[Candidate], t: &Tablets) -> BTreeMap<NodeId, usize> {
    let mut c: BTreeMap<NodeId, usize> = pool.iter().map(|c| (c.node.clone(), 0)).collect();
    for (_, rs) in t {
        for r in rs {
            if let Some(x) = c.get_mut(r) {
                *x += 1;
            }
        }
    }
    c
}

fn apply(t: &mut Tablets, k: u32, set: Vec<NodeId>) {
    t.iter_mut().find(|(kk, _)| *kk == k).unwrap().1 = set;
}

// ---------------------------------------------------------------------------
// Independent copy of the pre-capacity (count-based) rebalance algorithm.
// ---------------------------------------------------------------------------

fn old_satisfies(replicas: &[NodeId], pool: &[Candidate], policy: &PlacementPolicy) -> bool {
    if replicas.len() != policy.replication_factor {
        return false;
    }
    let find = |n: &NodeId| pool.iter().find(|c| &c.node == n);
    for r in replicas {
        match find(r) {
            Some(c) if policy.admits(c) => {}
            _ => return false,
        }
    }
    if let Some(sp) = &policy.spread
        && sp.strict
    {
        let mut seen = BTreeSet::new();
        for r in replicas {
            let Some(d) = find(r).and_then(|c| c.labels.get(&sp.domain)) else {
                return false;
            };
            if !seen.insert(d.clone()) {
                return false;
            }
        }
    }
    true
}

fn old_max_per_domain(replicas: &[NodeId], pool: &[Candidate], domain: &str) -> usize {
    let mut m: BTreeMap<Option<String>, usize> = BTreeMap::new();
    for r in replicas {
        let d = pool
            .iter()
            .find(|c| &c.node == r)
            .and_then(|c| c.labels.get(domain).cloned());
        *m.entry(d).or_default() += 1;
    }
    m.values().copied().max().unwrap_or(0)
}

fn old_rebalance_step(
    tablets: &Tablets,
    policy: &PlacementPolicy,
    pool: &[Candidate],
) -> Option<(u32, Vec<NodeId>)> {
    let mut cnt: BTreeMap<NodeId, usize> = pool.iter().map(|c| (c.node.clone(), 0)).collect();
    let mut elig: Vec<&(u32, Vec<NodeId>)> = tablets
        .iter()
        .filter(|(_, r)| old_satisfies(r, pool, policy))
        .collect();
    elig.sort_by_key(|(k, _)| *k);
    for (_, rs) in &elig {
        for r in rs {
            if let Some(c) = cnt.get_mut(r) {
                *c += 1;
            }
        }
    }
    let mut sources: Vec<(NodeId, usize)> = cnt.iter().map(|(n, &c)| (n.clone(), c)).collect();
    let mut dests = sources.clone();
    sources.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    dests.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    for (src, sc) in &sources {
        for (dst, dc) in &dests {
            if src == dst || *sc < dc + 2 {
                continue;
            }
            for (k, rs) in &elig {
                if !rs.contains(src) || rs.contains(dst) {
                    continue;
                }
                let dcand = pool.iter().find(|c| &c.node == dst).unwrap();
                if !policy.admits(dcand) {
                    continue;
                }
                let mut post: Vec<NodeId> = rs.iter().filter(|n| *n != src).cloned().collect();
                post.push(dst.clone());
                post.sort_unstable();
                if !old_satisfies(&post, pool, policy) {
                    continue;
                }
                if let Some(sp) = &policy.spread
                    && !sp.strict
                    && old_max_per_domain(&post, pool, &sp.domain)
                        > old_max_per_domain(rs, pool, &sp.domain)
                {
                    continue;
                }
                return Some((*k, post));
            }
        }
    }
    None
}

fn drain_plan(
    mut tablets: Tablets,
    policy: &PlacementPolicy,
    pool: &[Candidate],
    limit: usize,
) -> Vec<(u32, Vec<NodeId>)> {
    let mut out = Vec::new();
    for _ in 0..limit {
        let step = rebalance_step(&refs(&tablets, policy), pool);
        let Some((k, set)) = step else { break };
        apply(&mut tablets, k, set.clone());
        out.push((k, set));
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Guard: default capacity (and any uniform weight with no ceiling) makes
    /// `rebalance_step` / `rebalance_plan` / `rebalance_step_reference` produce,
    /// move for move, what the pre-capacity count-based algorithm produces.
    #[test]
    fn equal_weights_rebalance_equals_pre_capacity_algorithm(
        (pool, policy, tablets) in topology_strategy(0..30),
        w in 1u32..1000,
    ) {
        let cap = pool.len() * tablets.len() * 3 + 30;
        for p in [pool.clone(), uniform(&pool, w)] {
            let mut cur = tablets.clone();
            let mut expect = Vec::new();
            for _ in 0..cap {
                let Some((k, set)) = old_rebalance_step(&cur, &policy, &p) else { break };
                apply(&mut cur, k, set.clone());
                expect.push((k, set));
            }
            prop_assert_eq!(&drain_plan(tablets.clone(), &policy, &p, cap), &expect);
            prop_assert_eq!(&rebalance_plan(&refs(&tablets, &policy), &p, cap), &expect);
            let r = refs(&tablets, &policy);
            prop_assert_eq!(rebalance_step(&r, &p), rebalance_step_reference(&r, &p));
        }
    }

    /// Guard: balanced mint, replan, replan_repair and replan_pinned at default
    /// (or uniform) capacity are unchanged, for any `load` map. The mint's old
    /// order was `(load, node)`; without a spread constraint that is
    /// directly checkable as "the RF lowest `(load, node)` admitted nodes".
    #[test]
    fn equal_weights_mint_and_replan_unchanged(
        pool in candidate_pool_strategy(2, 9),
        rf in 1usize..4,
        loads in proptest::collection::vec(0usize..20, 9),
        w in 1u32..1000,
        drop_mask in proptest::collection::vec(any::<bool>(), 9),
    ) {
        let policy = PlacementPolicy::simple("p", rf);
        let load: BTreeMap<NodeId, usize> = pool
            .iter()
            .zip(&loads)
            .map(|(c, l)| (c.node.clone(), *l))
            .collect();
        let mut order: Vec<&NodeId> = pool.iter().map(|c| &c.node).collect();
        order.sort_by_key(|n| (load[*n], (*n).clone()));
        let mut oracle: Vec<NodeId> = order.into_iter().take(rf).cloned().collect();
        oracle.sort_unstable();
        for p in [pool.clone(), uniform(&pool, w)] {
            let got = select_replicas_balanced(&p, &policy, &load);
            if pool.len() < rf {
                prop_assert!(got.is_err());
            } else {
                prop_assert_eq!(got.unwrap(), oracle.clone());
            }
            // Spread policies: uniform weight must not move the answer.
            let spread = PlacementPolicy::simple("s", rf).spread_across("zone", false);
            prop_assert_eq!(
                select_replicas_balanced(&p, &spread, &load),
                select_replicas_balanced(&pool, &spread, &load)
            );

            // replan family: load is irrelevant without ceilings.
            let current: Vec<NodeId> = select_replicas(&pool, &policy).unwrap_or_default();
            let shrunk: Vec<Candidate> = p
                .iter()
                .zip(&drop_mask)
                .filter(|(_, d)| !**d)
                .map(|(c, _)| c.clone())
                .collect();
            prop_assert_eq!(
                replan_with_load(&current, &shrunk, &policy, &load),
                replan(&current, &shrunk, &policy)
            );
            prop_assert_eq!(
                replan_repair_with_load(&current, &shrunk, &policy, &load),
                replan_repair(&current, &shrunk, &policy)
            );
            prop_assert_eq!(
                replan_pinned_with_load(&current, &shrunk, &policy, &load),
                replan_pinned(&current, &shrunk, &policy)
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Weighted properties.
// ---------------------------------------------------------------------------

fn weights_strategy(n: usize) -> impl Strategy<Value = Vec<u32>> {
    proptest::collection::vec(prop_oneof![Just(50u32), Just(100), Just(200), 1u32..400], n)
}

/// `cs/ws - cd/wd <= 1/wd`, exactly (cross-multiplied).
fn within_one_replica(cs: usize, ws: u32, cd: usize, wd: u32) -> bool {
    (cs as u128) * u128::from(wd) <= (cd as u128) * u128::from(ws) + u128::from(ws)
}

/// Descending fill vector, as `(count, weight)`, ordered by exact fill.
fn fill_vector(pool: &[Candidate], t: &Tablets) -> Vec<(usize, u64)> {
    let c = counts(pool, t);
    let mut v: Vec<(usize, u64)> = pool
        .iter()
        .map(|p| (c[&p.node], p.capacity.effective_weight()))
        .collect();
    v.sort_by(|a, b| {
        (u128::from(b.0 as u64) * u128::from(a.1)).cmp(&(u128::from(a.0 as u64) * u128::from(b.1)))
    });
    v
}

fn fill_lex_less(a: &[(usize, u64)], b: &[(usize, u64)]) -> bool {
    for (x, y) in a.iter().zip(b) {
        let l = (x.0 as u128) * u128::from(y.1);
        let r = (y.0 as u128) * u128::from(x.1);
        if l != r {
            return l < r;
        }
    }
    false
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    /// Weighted convergence (RF 1, no spread: every pair always has a movable
    /// tablet): rebalance terminates and every pair ends within one replica's
    /// worth of weight of proportional, hence within `1/min_weight`.
    #[test]
    fn weighted_rebalance_converges_to_proportional(
        pool in candidate_pool_strategy(2, 8),
        ws in weights_strategy(8),
        placed in proptest::collection::vec(0usize..8, 1..60),
    ) {
        let caps: Vec<Capacity> = ws.iter().map(|&weight| Capacity { weight, max_replicas: None }).collect();
        let pool = with_caps(&pool, &caps);
        let policy = PlacementPolicy::simple("p", 1);
        // Everything starts on the first (arbitrary) nodes.
        let mut tablets: Tablets = placed
            .iter()
            .enumerate()
            .map(|(i, p)| (i as u32, vec![pool[p % pool.len().min(2)].node.clone()]))
            .collect();
        let mut prev = fill_vector(&pool, &tablets);
        let bound = tablets.len() * pool.len() * 4 + 16;
        let mut steps = 0;
        while let Some((k, set)) = rebalance_step(&refs(&tablets, &policy), &pool) {
            apply(&mut tablets, k, set);
            let cur = fill_vector(&pool, &tablets);
            prop_assert!(fill_lex_less(&cur, &prev), "fill vector must strictly decrease");
            prev = cur;
            steps += 1;
            prop_assert!(steps <= bound, "did not terminate");
        }
        let c = counts(&pool, &tablets);
        let min_w = pool.iter().map(|p| p.capacity.weight).min().unwrap();
        for s in &pool {
            for d in &pool {
                prop_assert!(
                    within_one_replica(c[&s.node], s.capacity.weight, c[&d.node], d.capacity.weight),
                    "pair not within one replica: {:?}", c
                );
                // The ADR's stated bound: |fill_s - fill_d| <= 1/min_weight.
                let diff = (c[&s.node] as u128) * u128::from(d.capacity.weight);
                let other = (c[&d.node] as u128) * u128::from(s.capacity.weight);
                prop_assert!(
                    diff.saturating_sub(other) * u128::from(min_w)
                        <= u128::from(s.capacity.weight) * u128::from(d.capacity.weight)
                );
            }
        }
    }

    /// Any policy (spread, residency, RF > 1), weights, no ceilings:
    /// monotone non-worsening (descending fill vector never increases) and
    /// termination, and `rebalance_plan` equals the looped step.
    #[test]
    fn weighted_rebalance_terminates_and_plan_equals_steps(
        (pool, policy, tablets) in topology_strategy(0..30),
        ws in weights_strategy(9),
    ) {
        let caps: Vec<Capacity> = ws.iter().map(|&weight| Capacity { weight, max_replicas: None }).collect();
        let pool = with_caps(&pool, &caps);
        let bound = pool.len() * tablets.len() * 4 + 30;
        let looped = drain_plan(tablets.clone(), &policy, &pool, bound);
        prop_assert!(looped.len() < bound, "did not terminate");
        let plan = rebalance_plan(&refs(&tablets, &policy), &pool, bound);
        prop_assert_eq!(&plan, &looped);
        let mut cur = tablets.clone();
        let mut prev = fill_vector(&pool, &cur);
        for (k, set) in &looped {
            apply(&mut cur, *k, set.clone());
            let now = fill_vector(&pool, &cur);
            prop_assert!(fill_lex_less(&now, &prev));
            prev = now;
        }
        let r = refs(&tablets, &policy);
        prop_assert_eq!(rebalance_step(&r, &pool), rebalance_step_reference(&r, &pool));
    }

    /// Ceilings: a mint never places on a full node; rebalance never moves onto
    /// a node at its ceiling and never raises a node above it; total overage
    /// never increases; it terminates (also after the ceilings are lowered, in
    /// which case an over-ceiling node only ever gives replicas away).
    #[test]
    fn ceilings_respected_by_mint_and_rebalance(
        pool in candidate_pool_strategy(3, 8),
        ws in weights_strategy(8),
        maxes in proptest::collection::vec(proptest::option::of(0u32..12), 8),
        lowered in proptest::collection::vec(proptest::option::of(0u32..6), 8),
        rf in 1usize..3,
        ntab in 1usize..30,
    ) {
        let caps: Vec<Capacity> = ws
            .iter()
            .zip(&maxes)
            .map(|(&weight, &max_replicas)| Capacity { weight, max_replicas })
            .collect();
        let pool = with_caps(&pool, &caps);
        let policy = PlacementPolicy::simple("p", rf);
        let mut load: BTreeMap<NodeId, usize> = BTreeMap::new();
        let mut tablets: Tablets = Vec::new();
        for i in 0..ntab {
            match select_replicas_balanced(&pool, &policy, &load) {
                Ok(set) => {
                    for n in &set {
                        let c = pool.iter().find(|c| &c.node == n).unwrap();
                        prop_assert!(!c.capacity.is_full(load.get(n).copied().unwrap_or(0)));
                        *load.entry(n.clone()).or_default() += 1;
                    }
                    tablets.push((i as u32, set));
                }
                Err(e) => {
                    let full = pool.iter().filter(|c| c.capacity.is_full(load.get(&c.node).copied().unwrap_or(0))).count();
                    let ok = matches!(e, PlacementError::InsufficientCandidates { .. });
                    prop_assert!(ok);
                    prop_assert!(pool.len() - full < rf);
                    break;
                }
            }
        }
        // Now lower some ceilings (a node may end above its cap), then rebalance.
        let lowered_caps: Vec<Capacity> = caps
            .iter()
            .zip(&lowered)
            .map(|(c, l)| Capacity { weight: c.weight, max_replicas: l.or(c.max_replicas) })
            .collect();
        let pool = with_caps(&pool, &lowered_caps);
        let overage = |t: &Tablets| -> usize {
            let c = counts(&pool, t);
            pool.iter().map(|p| p.capacity.max_replicas.map_or(0, |m| c[&p.node].saturating_sub(m as usize))).sum()
        };
        let bound = tablets.len() * pool.len() * 4 + 30;
        let mut cur = tablets.clone();
        let mut steps = 0;
        while let Some((k, set)) = rebalance_step(&refs(&cur, &policy), &pool) {
            let before = counts(&pool, &cur);
            let ov = overage(&cur);
            let old = cur.iter().find(|(kk, _)| *kk == k).unwrap().1.clone();
            let dst: Vec<&NodeId> = set.iter().filter(|n| !old.contains(n)).collect();
            prop_assert_eq!(dst.len(), 1);
            let dc = pool.iter().find(|c| &c.node == dst[0]).unwrap();
            prop_assert!(!dc.capacity.is_full(before[dst[0]]), "moved onto a full node");
            apply(&mut cur, k, set);
            prop_assert!(overage(&cur) <= ov);
            steps += 1;
            prop_assert!(steps <= bound, "did not terminate");
        }
    }

    /// Repair: with ceilings, `replan_repair_with_load` never shrinks a set,
    /// never adds a node at its ceiling, and a node holding a replica keeps it
    /// even when it is at (or above) its ceiling.
    #[test]
    fn repair_never_shrinks_and_respects_ceilings(
        pool in candidate_pool_strategy(2, 9),
        maxes in proptest::collection::vec(proptest::option::of(0u32..5), 9),
        loads in proptest::collection::vec(0usize..7, 9),
        rf in 1usize..5,
        cur_mask in proptest::collection::vec(any::<bool>(), 9),
        gone_mask in proptest::collection::vec(proptest::bool::weighted(0.25), 9),
    ) {
        let caps: Vec<Capacity> = maxes.iter().map(|&m| Capacity { weight: 100, max_replicas: m }).collect();
        let full_pool = with_caps(&pool, &caps);
        let load: BTreeMap<NodeId, usize> = full_pool.iter().zip(&loads).map(|(c, l)| (c.node.clone(), *l)).collect();
        let current: Vec<NodeId> = full_pool.iter().zip(&cur_mask).filter(|(_, m)| **m).map(|(c, _)| c.node.clone()).take(rf).collect();
        let live: Vec<Candidate> = full_pool.iter().zip(&gone_mask).filter(|(_, g)| !**g).map(|(c, _)| c.clone()).collect();
        let policy = PlacementPolicy::simple("p", rf);
        let live_ids: BTreeSet<&NodeId> = live.iter().map(|c| &c.node).collect();
        for set in [
            replan_repair_with_load(&current, &live, &policy, &load),
            replan_with_load(&current, &live, &policy, &load),
        ].into_iter().flatten() {
            {
                prop_assert!(set.len() <= rf);
                for n in &current {
                    if live_ids.contains(n) {
                        prop_assert!(set.contains(n), "capacity shrank a set");
                    }
                }
                for n in &set {
                    if !current.contains(n) {
                        let c = live.iter().find(|c| &c.node == n).unwrap();
                        prop_assert!(!c.capacity.is_full(load[n]), "repair filled a full node");
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Examples.
// ---------------------------------------------------------------------------

fn plain(id: u64, w: u32, max: Option<u32>) -> Candidate {
    Candidate::new(nid(id), BTreeMap::new()).with_capacity(Capacity {
        weight: w,
        max_replicas: max,
    })
}

#[test]
fn default_capacity_is_weight_100_no_max() {
    let c = Candidate::new(nid(1), BTreeMap::new());
    assert_eq!(
        c.capacity,
        Capacity {
            weight: 100,
            max_replicas: None
        }
    );
    assert_eq!(Capacity::default(), c.capacity);
    // Weight 0 is clamped, never a division by zero.
    assert_eq!(
        Capacity {
            weight: 0,
            max_replicas: None
        }
        .effective_weight(),
        1
    );
}

#[test]
fn weighted_mint_is_proportional() {
    let pool = vec![
        plain(1, 100, None),
        plain(2, 100, None),
        plain(3, 200, None),
    ];
    let policy = PlacementPolicy::simple("p", 1);
    let mut load: BTreeMap<NodeId, usize> = BTreeMap::new();
    for _ in 0..40 {
        let s = select_replicas_balanced(&pool, &policy, &load).unwrap();
        *load.entry(s[0].clone()).or_default() += 1;
    }
    assert_eq!(load[&nid(1)], 10);
    assert_eq!(load[&nid(2)], 10);
    assert_eq!(load[&nid(3)], 20);
}

#[test]
fn mint_refuses_when_every_node_is_full() {
    let pool = vec![plain(1, 100, Some(2)), plain(2, 100, Some(2))];
    let load: BTreeMap<NodeId, usize> = [(nid(1), 2), (nid(2), 2)].into();
    let policy = PlacementPolicy::simple("p", 1);
    assert!(matches!(
        select_replicas_balanced(&pool, &policy, &load),
        Err(PlacementError::InsufficientCandidates { .. })
    ));
    let load: BTreeMap<NodeId, usize> = [(nid(1), 2), (nid(2), 1)].into();
    assert_eq!(
        select_replicas_balanced(&pool, &policy, &load).unwrap(),
        vec![nid(2)]
    );
}

#[test]
fn repair_waits_rather_than_overfill_and_grows_as_far_as_caps_allow() {
    let policy = PlacementPolicy::simple("p", 3);
    let load: BTreeMap<NodeId, usize> = [(nid(2), 5), (nid(3), 5)].into();
    // Survivor 1, candidates 2 and 3 are full: repair cannot reach RF 3.
    let pool = vec![
        plain(1, 100, None),
        plain(2, 100, Some(5)),
        plain(3, 100, Some(5)),
    ];
    assert!(replan_repair_with_load(&[nid(1)], &pool, &policy, &load).is_err());
    // Without the caps (or load) the same call repairs to all three.
    assert_eq!(
        replan_repair(&[nid(1)], &pool, &policy).unwrap(),
        vec![nid(1), nid(2), nid(3)]
    );
    // One free node: partial growth to what the caps allow.
    let pool = vec![
        plain(1, 100, None),
        plain(2, 100, None),
        plain(3, 100, Some(5)),
    ];
    assert_eq!(
        replan_repair_with_load(&[nid(1)], &pool, &policy, &load).unwrap(),
        vec![nid(1), nid(2)]
    );
    // A survivor at its own ceiling keeps its replica.
    let pool = vec![
        plain(1, 100, Some(5)),
        plain(2, 100, None),
        plain(3, 100, None),
    ];
    let load: BTreeMap<NodeId, usize> = [(nid(1), 9)].into();
    assert_eq!(
        replan_with_load(&[nid(1)], &pool, &policy, &load).unwrap(),
        vec![nid(1), nid(2), nid(3)]
    );
}

#[test]
fn rebalance_drains_an_over_cap_node_and_never_fills_a_full_one() {
    let policy = PlacementPolicy::simple("p", 1);
    // Node 1 holds 6 but its cap was lowered to 3; node 2 is full at 2;
    // node 3 is empty and has room.
    let pool = vec![
        plain(1, 100, Some(3)),
        plain(2, 100, Some(2)),
        plain(3, 100, None),
    ];
    let mut t: Tablets = (0..6u32).map(|i| (i, vec![nid(1)])).collect();
    t.push((6, vec![nid(2)]));
    t.push((7, vec![nid(2)]));
    let mut moved_to = BTreeSet::new();
    while let Some((k, set)) = rebalance_step(&refs(&t, &policy), &pool) {
        moved_to.insert(set[0].clone());
        apply(&mut t, k, set);
    }
    assert_eq!(
        moved_to,
        [nid(3)].into(),
        "only the node with room receives"
    );
    let c = counts(&pool, &t);
    assert!(c[&nid(1)] <= 3, "over-cap node drained to its cap: {c:?}");
    assert_eq!(c[&nid(2)], 2);
}

#[test]
fn full_pinned_region_fails_cleanly() {
    let node = |id: u64, region: &str, max: Option<u32>| {
        let labels: BTreeMap<String, String> =
            [(REGION_LABEL.to_string(), region.to_string())].into();
        Candidate::new(nid(id), labels).with_capacity(Capacity {
            weight: 100,
            max_replicas: max,
        })
    };
    let policy = PlacementPolicy::mrsc("m", ["a", "b", "c"]);
    let pool = vec![
        node(1, "a", None),
        node(2, "b", Some(1)),
        node(3, "b", Some(1)),
        node(4, "c", None),
    ];
    let current = [nid(1), nid(2), nid(4)];
    let mut load: BTreeMap<NodeId, usize> = BTreeMap::new();
    // Region b has room: node 3 replaces a lost node 2... (node 2 gone)
    let live: Vec<Candidate> = pool.iter().filter(|c| c.node != nid(2)).cloned().collect();
    assert_eq!(
        replan_pinned_with_load(&current, &live, &policy, &load).unwrap(),
        vec![nid(1), nid(3), nid(4)]
    );
    // Now node 3 is full too: the replica waits (an error, nothing proposed).
    load.insert(nid(3), 1);
    assert!(matches!(
        replan_pinned_with_load(&current, &live, &policy, &load),
        Err(PlacementError::InsufficientDomains { .. })
    ));
    // The surviving replica in a full Region is never dropped.
    load.insert(nid(2), 7);
    assert_eq!(
        replan_pinned_with_load(&current, &pool, &policy, &load).unwrap(),
        vec![nid(1), nid(2), nid(4)]
    );
}

#[test]
fn fill_comparison_is_exact_for_huge_counts() {
    // (load + 1) / weight with counts near 2^63 would round-tie in f64.
    let a = (1usize << 62) + 1;
    let pool = vec![plain(1, 3, None), plain(2, 3, None)];
    let load: BTreeMap<NodeId, usize> = [(nid(1), a), (nid(2), a - 1)].into();
    let policy = PlacementPolicy::simple("p", 1);
    assert_eq!(
        select_replicas_balanced(&pool, &policy, &load).unwrap(),
        vec![nid(2)]
    );
}

#[test]
fn rebalance_does_not_swap_on_equal_fill_and_stops_at_the_weighted_optimum() {
    let policy = PlacementPolicy::simple("p", 1);
    let pool = vec![plain(1, 100, None), plain(2, 200, None)];
    // 2 on the weight-100 node, 3 on the weight-200 node: moving one would only
    // swap equal fills (0.02 vs 0.02), and would oscillate forever.
    let mut t: Tablets = (0..2u32).map(|i| (i, vec![nid(1)])).collect();
    t.extend((2..5u32).map(|i| (i, vec![nid(2)])));
    assert_eq!(rebalance_step(&refs(&t, &policy), &pool), None);
    // 5 on the light node drains to the 1:2 proportion (2 and 3 is within one
    // replica of 5/3 and 10/3).
    let t: Tablets = (0..5u32).map(|i| (i, vec![nid(1)])).collect();
    let moves = drain_plan(t.clone(), &policy, &pool, 100);
    let mut cur = t;
    for (k, s) in moves {
        apply(&mut cur, k, s);
    }
    let c = counts(&pool, &cur);
    assert_eq!((c[&nid(1)], c[&nid(2)]), (2, 3));
}

#[allow(dead_code)]
fn _topology_type_is_used(_: &Topology) {}
