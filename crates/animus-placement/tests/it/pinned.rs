//! Pinned (MRSC) placement: `allowed_values`, `PlacementPolicy::mrsc` and
//! `replan_pinned` (ADR 0075, G-01 stage G-c, M1). Example tests plus
//! property tests, **including convergence from a violating start** — a
//! pure planner can be correct on a fresh pick yet unable to repair a skewed
//! survivor set (plain `replan` seeds survivors without re-validating the
//! strict spread), so every pinned property is also checked from a start
//! that has two replicas in one region.

use std::collections::{BTreeMap, BTreeSet};

use animus_env::{NodeId, nid};
use animus_placement::{
    Candidate, PlacementError, PlacementPolicy, REGION_LABEL, rebalance_step, replan,
    replan_pinned, select_replicas,
};
use proptest::prelude::*;

fn node(id: u64, region: &str) -> Candidate {
    let labels: BTreeMap<String, String> = [(REGION_LABEL.to_string(), region.to_string())].into();
    Candidate::new(nid(id), labels)
}

fn region_of(n: &NodeId, pool: &[Candidate]) -> String {
    pool.iter().find(|c| &c.node == n).unwrap().labels[REGION_LABEL].clone()
}

fn regions_of(set: &[NodeId], pool: &[Candidate]) -> Vec<String> {
    let mut r: Vec<String> = set.iter().map(|n| region_of(n, pool)).collect();
    r.sort();
    r
}

fn mrsc() -> PlacementPolicy {
    PlacementPolicy::mrsc("mrsc", ["a", "b", "c"])
}

fn pool() -> Vec<Candidate> {
    vec![
        node(1, "a"),
        node(2, "a"),
        node(3, "b"),
        node(4, "b"),
        node(5, "c"),
        node(6, "c"),
        node(7, "d"), // outside the pinned set
        node(8, "d"),
        node(9, "e"),
    ]
}

#[test]
fn mrsc_policy_shape() {
    let p = mrsc();
    assert_eq!(p.replication_factor, 3);
    assert!(p.is_pinned());
    assert!(!PlacementPolicy::simple("s", 3).is_pinned());
    assert!(
        !PlacementPolicy::simple("s", 3)
            .allow_values(REGION_LABEL, ["a"])
            .is_pinned(),
        "an IN-set without a strict spread is not a pin"
    );
    assert!(p.admits(&node(1, "a")));
    assert!(!p.admits(&node(7, "d")));
    assert!(!p.admits(&Candidate::new(nid(1), BTreeMap::new())));
}

#[test]
fn fresh_pick_is_one_per_region_and_ignores_extra_regions() {
    let chosen = select_replicas(&pool(), &mrsc()).unwrap();
    assert_eq!(regions_of(&chosen, &pool()), ["a", "b", "c"]);
}

#[test]
fn absent_region_is_insufficient_domains() {
    let p: Vec<Candidate> = pool()
        .into_iter()
        .filter(|c| c.node != nid(5) && c.node != nid(6))
        .collect();
    let current = [nid(1), nid(3), nid(5)];
    assert_eq!(
        replan_pinned(&current, &p, &mrsc()),
        Err(PlacementError::InsufficientDomains {
            needed: 3,
            available: 2
        })
    );
}

#[test]
fn in_region_replacement_keeps_the_other_survivors() {
    // Node 5 (region c) is gone; node 6 is region c's other node.
    let p: Vec<Candidate> = pool().into_iter().filter(|c| c.node != nid(5)).collect();
    let got = replan_pinned(&[nid(1), nid(3), nid(5)], &p, &mrsc()).unwrap();
    assert_eq!(got, vec![nid(1), nid(3), nid(6)]);
}

#[test]
fn already_compliant_set_is_unchanged() {
    let got = replan_pinned(&[nid(2), nid(4), nid(6)], &pool(), &mrsc()).unwrap();
    assert_eq!(got, vec![nid(2), nid(4), nid(6)]);
}

#[test]
fn skewed_survivors_converge_where_plain_replan_does_not() {
    // Two replicas in region a (1 and 2), one in b, none in c.
    let current = [nid(1), nid(2), nid(3)];
    let plain = replan(&current, &pool(), &mrsc()).unwrap();
    assert_eq!(
        regions_of(&plain, &pool()),
        ["a", "a", "b"],
        "documented: plain replan keeps a spread-violating survivor set"
    );
    let pinned = replan_pinned(&current, &pool(), &mrsc()).unwrap();
    assert_eq!(regions_of(&pinned, &pool()), ["a", "b", "c"]);
    assert!(pinned.contains(&nid(1)), "lowest id per region is kept");
    assert!(pinned.contains(&nid(3)));
    assert!(!pinned.contains(&nid(2)));
    // And it is a fixpoint.
    assert_eq!(replan_pinned(&pinned, &pool(), &mrsc()).unwrap(), pinned);
}

#[test]
fn survivors_outside_the_allowed_set_are_replaced() {
    let got = replan_pinned(&[nid(1), nid(3), nid(7)], &pool(), &mrsc()).unwrap();
    assert_eq!(regions_of(&got, &pool()), ["a", "b", "c"]);
}

#[test]
fn unpinned_policy_without_strict_spread_matches_plain_replan() {
    let p = PlacementPolicy::simple("s", 3);
    let current = [nid(1), nid(2), nid(7)];
    assert_eq!(
        replan_pinned(&current, &pool(), &p).unwrap(),
        replan(&current, &pool(), &p).unwrap()
    );
}

#[test]
fn allowed_values_serde_is_skipped_at_default() {
    let simple = serde_json::to_string(&PlacementPolicy::simple("s", 3)).unwrap();
    assert!(!simple.contains("allowed_values"), "{simple}");
    let back: PlacementPolicy = serde_json::from_str(&simple).unwrap();
    assert!(back.allowed_values.is_empty());
    let pinned = serde_json::to_string(&mrsc()).unwrap();
    assert!(pinned.contains("allowed_values"), "{pinned}");
    assert_eq!(
        serde_json::from_str::<PlacementPolicy>(&pinned).unwrap(),
        mrsc()
    );
}

/// A random pool over regions a..e with 0..=3 nodes each, ids distinct.
fn pool_strategy() -> impl Strategy<Value = Vec<Candidate>> {
    proptest::collection::vec(0usize..=3, 5).prop_map(|counts| {
        let mut out = Vec::new();
        let mut id = 1u64;
        for (r, n) in ["a", "b", "c", "d", "e"].iter().zip(counts) {
            for _ in 0..n {
                out.push(node(id, r));
                id += 1;
            }
        }
        out
    })
}

proptest! {
    /// Balance-driven rebalancing of pinned tablets (from compliant starts,
    /// `rebalance_step` skips violators) keeps every tablet one-per-region
    /// inside the allowed set and terminates.
    #[test]
    fn rebalance_keeps_pinned_tablets_pinned_and_terminates(pool in pool_strategy()) {
        let policy = mrsc();
        let Ok(base) = select_replicas(&pool, &policy) else { return Ok(()); };
        let mut tablets: Vec<(u32, Vec<NodeId>)> = (0..4u32).map(|k| (k, base.clone())).collect();
        let bound = 4 * pool.len() * pool.len() + 8;
        let mut steps = 0usize;
        loop {
            let view: Vec<(u32, &[NodeId], &PlacementPolicy)> =
                tablets.iter().map(|(k, r)| (*k, r.as_slice(), &policy)).collect();
            let Some((k, post)) = rebalance_step(&view, &pool) else { break };
            prop_assert_eq!(regions_of(&post, &pool), vec!["a", "b", "c"]);
            tablets.iter_mut().find(|(t, _)| *t == k).unwrap().1 = post;
            steps += 1;
            prop_assert!(steps <= bound, "rebalance did not terminate");
        }
    }

    /// From any start (a violating one included): the result never places
    /// outside the allowed set, is exactly one per region, is deterministic
    /// under input permutation, and is a fixpoint (so repeated repair
    /// terminates); an `Err` only when a pinned region has no node.
    #[test]
    fn replan_pinned_converges_from_any_start(
        pool in pool_strategy(),
        picks in proptest::collection::vec(0usize..16, 0..5),
    ) {
        let policy = mrsc();
        let current: Vec<NodeId> = picks
            .iter()
            .filter_map(|i| pool.get(*i % pool.len().max(1)).map(|c| c.node.clone()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let regions_present: BTreeSet<String> = pool
            .iter()
            .map(|c| c.labels[REGION_LABEL].clone())
            .filter(|r| ["a", "b", "c"].contains(&r.as_str()))
            .collect();
        match replan_pinned(&current, &pool, &policy) {
            Err(e) => {
                prop_assert!(regions_present.len() < 3, "unexpected {e:?}");
            }
            Ok(set) => {
                prop_assert_eq!(regions_present.len(), 3);
                prop_assert_eq!(regions_of(&set, &pool), vec!["a", "b", "c"]);
                // Survivors that are the lowest-id of their region are kept.
                for r in ["a", "b", "c"] {
                    let kept: Vec<&NodeId> = current
                        .iter()
                        .filter(|n| region_of(n, &pool) == r)
                        .collect();
                    if let Some(lowest) = kept.iter().min() {
                        prop_assert!(set.contains(lowest), "churned survivor {lowest}");
                    }
                }
                // Fixpoint.
                prop_assert_eq!(replan_pinned(&set, &pool, &policy).unwrap(), set.clone());
                // Permutation-stable.
                let mut rev = pool.clone();
                rev.reverse();
                let mut cur_rev = current.clone();
                cur_rev.reverse();
                prop_assert_eq!(replan_pinned(&cur_rev, &rev, &policy).unwrap(), set);
            }
        }
    }

    /// A fresh pick (`select_replicas`) never leaves the allowed set either.
    #[test]
    fn fresh_pick_never_leaves_the_allowed_set(pool in pool_strategy()) {
        if let Ok(set) = select_replicas(&pool, &mrsc()) {
            prop_assert_eq!(regions_of(&set, &pool), vec!["a", "b", "c"]);
        }
    }
}
