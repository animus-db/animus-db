//! Unit tests for the load-rebalancing planner `rebalance_step` (ADR 0029): a
//! single, policy-legal, balance-improving replica move per call. All pure — no
//! simulator needed. Mirrors `tests/placement.rs`'s helper/style conventions.

use std::collections::BTreeMap;

use animus_env::{NodeId, nid};
use animus_placement::{Candidate, PlacementPolicy, rebalance_step};

/// A plain candidate with no topology labels.
fn plain(id: u64) -> Candidate {
    Candidate::new(nid(id), BTreeMap::new())
}

/// A candidate in `region` / `zone`.
fn node(id: u64, region: &str, zone: &str) -> Candidate {
    let labels: BTreeMap<String, String> = [("region", region), ("zone", zone)]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    Candidate::new(nid(id), labels)
}

/// Build the `(K, &[NodeId], &PlacementPolicy)` slice `rebalance_step` wants from
/// owned `(K, Vec<NodeId>)` tablets, all sharing one policy.
fn tablets<'a, K: Copy>(
    sets: &'a [(K, Vec<NodeId>)],
    policy: &'a PlacementPolicy,
) -> Vec<(K, &'a [NodeId], &'a PlacementPolicy)> {
    sets.iter()
        .map(|(k, v)| (*k, v.as_slice(), policy))
        .collect()
}

/// Per-node replica counts across `candidates`, seeded 0.
fn counts(sets: &[(u32, Vec<NodeId>)], candidates: &[Candidate]) -> BTreeMap<NodeId, usize> {
    let mut c: BTreeMap<NodeId, usize> = candidates.iter().map(|x| (x.node, 0)).collect();
    for (_, replicas) in sets {
        for r in replicas {
            if let Some(n) = c.get_mut(r) {
                *n += 1;
            }
        }
    }
    c
}

/// The zone label of `node` in the pool.
fn zone_of(pool: &[Candidate], node: u64) -> String {
    pool.iter().find(|c| c.node == nid(node)).unwrap().labels["zone"].clone()
}

#[test]
fn noop_when_balanced() {
    let policy = PlacementPolicy::simple("rf3", 3);
    // Five nodes, each holding exactly three replicas ⇒ max−min = 0.
    let sets: Vec<(u32, Vec<NodeId>)> = vec![
        (0, vec![nid(10), nid(11), nid(12)]),
        (1, vec![nid(11), nid(12), nid(13)]),
        (2, vec![nid(12), nid(13), nid(14)]),
        (3, vec![nid(13), nid(14), nid(10)]),
        (4, vec![nid(14), nid(10), nid(11)]),
    ];
    let cands: Vec<Candidate> = (10..=14).map(plain).collect();
    assert_eq!(rebalance_step(&tablets(&sets, &policy), &cands), None);
}

#[test]
fn moves_one_replica_from_most_to_least_loaded() {
    let policy = PlacementPolicy::simple("rf3", 3);
    // Grew 3 → 5, but every tablet still lives on {10,11,12}; 13 and 14 are empty.
    let sets: Vec<(u32, Vec<NodeId>)> = vec![
        (0, vec![nid(10), nid(11), nid(12)]),
        (1, vec![nid(10), nid(11), nid(12)]),
        (2, vec![nid(10), nid(11), nid(12)]),
    ];
    let cands: Vec<Candidate> = (10..=14).map(plain).collect();
    // Most-loaded source is 10 (count 3, lowest id); least-loaded dest is 13; the
    // first tablet in K order with a replica on 10 is tablet 0.
    let (k, new) = rebalance_step(&tablets(&sets, &policy), &cands).expect("a move");
    assert_eq!(k, 0);
    assert_eq!(new, vec![nid(11), nid(12), nid(13)]);
}

#[test]
fn respects_residency() {
    let policy = PlacementPolicy::simple("eu", 3).require_label("region", "eu");
    // 13 is the least-loaded node but sits outside residency; 14 is eu.
    let cands = vec![
        node(10, "eu", "a"),
        node(11, "eu", "a"),
        node(12, "eu", "a"),
        node(13, "us", "a"), // excluded by residency
        node(14, "eu", "a"),
    ];
    let sets: Vec<(u32, Vec<NodeId>)> = vec![
        (0, vec![nid(10), nid(11), nid(12)]),
        (1, vec![nid(10), nid(11), nid(12)]),
        (2, vec![nid(10), nid(11), nid(12)]),
    ];
    let (_, new) = rebalance_step(&tablets(&sets, &policy), &cands).expect("a move");
    assert!(
        !new.contains(&nid(13)),
        "residency-excluded node was placed: {new:?}"
    );
    assert!(
        new.contains(&nid(14)),
        "eligible new node not used: {new:?}"
    );
}

#[test]
fn strict_spread_blocks_a_domain_doubling_move() {
    // Strict zone spread. The tablet is one-per-zone (a,b,c). One new node, 13,
    // in zone b. The naive most-loaded→least-loaded move (10 in zone a → 13 in
    // zone b) would double zone b ([11(b),12(c),13(b)]) and is rejected; the
    // planner instead moves the zone-b replica 11 → 13, keeping spread.
    let policy = PlacementPolicy::simple("eu", 3)
        .require_label("region", "eu")
        .spread_across("zone", true);
    let cands = vec![
        node(10, "eu", "a"),
        node(11, "eu", "b"),
        node(12, "eu", "c"),
        node(13, "eu", "b"), // new, zone b
    ];
    let sets: Vec<(u32, Vec<NodeId>)> = vec![
        (0, vec![nid(10), nid(11), nid(12)]),
        (1, vec![nid(10), nid(11), nid(12)]),
        (2, vec![nid(10), nid(11), nid(12)]),
    ];
    let (_, new) = rebalance_step(&tablets(&sets, &policy), &cands).expect("a move");
    // Whatever move was chosen, strict spread holds: three distinct zones.
    let mut zones: Vec<String> = new.iter().map(|n| zone_of(&cands, (*n).as_u64())).collect();
    zones.sort();
    zones.dedup();
    assert_eq!(zones.len(), 3, "strict spread broken: {new:?}");
    assert!(new.contains(&nid(13)), "new node not used: {new:?}");
    // Specifically: the zone-b replica moved, not the zone-a one.
    assert_eq!(new, vec![nid(10), nid(12), nid(13)]);
}

#[test]
fn best_effort_spread_never_worsens() {
    // Best-effort zone spread. Tablet is one-per-zone (max-per-domain 1). New node
    // 13 in zone b. Moving zone-a replica 10 → 13(b) would make zone b hold two
    // replicas (max 2 > 1) and is rejected; moving zone-b replica 11 → 13(b)
    // keeps max-per-domain at 1 and is chosen.
    let policy = PlacementPolicy::simple("eu", 3)
        .require_label("region", "eu")
        .spread_across("zone", false);
    let cands = vec![
        node(10, "eu", "a"),
        node(11, "eu", "b"),
        node(12, "eu", "c"),
        node(13, "eu", "b"), // new, zone b
    ];
    let sets: Vec<(u32, Vec<NodeId>)> = vec![
        (0, vec![nid(10), nid(11), nid(12)]),
        (1, vec![nid(10), nid(11), nid(12)]),
        (2, vec![nid(10), nid(11), nid(12)]),
    ];
    let (_, new) = rebalance_step(&tablets(&sets, &policy), &cands).expect("a move");
    assert_eq!(new, vec![nid(10), nid(12), nid(13)]);
    // Max-per-domain did not increase from the pre-move set's 1.
    let mut per_zone: BTreeMap<String, usize> = BTreeMap::new();
    for n in &new {
        *per_zone.entry(zone_of(&cands, (*n).as_u64())).or_default() += 1;
    }
    assert_eq!(
        *per_zone.values().max().unwrap(),
        1,
        "spread worsened: {new:?}"
    );
}

#[test]
fn at_most_one_move_and_repeated_application_converges() {
    let policy = PlacementPolicy::simple("rf3", 3);
    let cands: Vec<Candidate> = (10..=14).map(plain).collect();
    // Heavily imbalanced: everything on {10,11,12}.
    let mut sets: Vec<(u32, Vec<NodeId>)> = vec![
        (0, vec![nid(10), nid(11), nid(12)]),
        (1, vec![nid(10), nid(11), nid(12)]),
        (2, vec![nid(10), nid(11), nid(12)]),
        (3, vec![nid(10), nid(11), nid(12)]),
        (4, vec![nid(10), nid(11), nid(12)]),
    ];

    let mut steps = 0;
    // Repeatedly apply, recomputing from the result each time; each call moves at
    // most one replica, so a finite cluster must converge in bounded steps.
    while let Some((k, new)) = rebalance_step(&tablets(&sets, &policy), &cands) {
        let slot = sets
            .iter_mut()
            .find(|(kk, _)| *kk == k)
            .expect("tablet exists");
        // One move per step: exactly one replica changed.
        let before = &slot.1;
        let diff = before.iter().filter(|n| !new.contains(n)).count();
        assert_eq!(diff, 1, "more than one replica moved in a step");
        slot.1 = new;
        steps += 1;
        assert!(steps < 1000, "did not converge");
    }

    // Converged: max − min ≤ 1 across all five nodes.
    let c = counts(&sets, &cands);
    let (min, max) = (c.values().min().unwrap(), c.values().max().unwrap());
    assert!(max - min <= 1, "not balanced: {c:?}");
}

#[test]
fn deterministic_under_input_permutation() {
    let policy = PlacementPolicy::simple("rf3", 3);
    let sets: Vec<(u32, Vec<NodeId>)> = vec![
        (0, vec![nid(10), nid(11), nid(12)]),
        (1, vec![nid(10), nid(11), nid(12)]),
        (2, vec![nid(10), nid(11), nid(12)]),
    ];
    let cands: Vec<Candidate> = (10..=14).map(plain).collect();
    let first = rebalance_step(&tablets(&sets, &policy), &cands).expect("a move");

    // Same logical input, tablets and candidates in reverse order.
    let mut rev_sets = sets.clone();
    rev_sets.reverse();
    let mut rev_cands = cands.clone();
    rev_cands.reverse();
    let second = rebalance_step(&tablets(&rev_sets, &policy), &rev_cands).expect("a move");

    assert_eq!(first, second, "move depended on input ordering");
}

#[test]
fn skips_unsatisfiable_policies() {
    let policy = PlacementPolicy::simple("rf3", 3);
    let cands: Vec<Candidate> = (10..=14).map(plain).collect();
    // Tablet 0's set violates its own policy (a replica, 99, isn't a candidate):
    // reconcile's job, not rebalance's — it must be ignored, not moved, and must
    // not crash. Tablet 1 is a valid, balanced-enough set with nothing to move.
    let sets: Vec<(u32, Vec<NodeId>)> = vec![
        (0, vec![nid(10), nid(11), nid(99)]),
        (1, vec![nid(12), nid(13), nid(14)]),
    ];
    assert_eq!(rebalance_step(&tablets(&sets, &policy), &cands), None);
}
