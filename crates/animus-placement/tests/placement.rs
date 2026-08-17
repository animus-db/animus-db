//! Unit tests for the placement policy engine: residency filtering, failure-
//! domain spread (strict and best-effort), error cases, determinism, and
//! churn-minimizing re-planning. All pure — no simulator needed.

use std::collections::BTreeMap;

use animus_env::{NodeId, nid};
use animus_placement::{Candidate, PlacementError, PlacementPolicy, replan, select_replicas};

/// A candidate in `region` / `zone`.
fn node(id: u64, region: &str, zone: &str) -> Candidate {
    let labels: BTreeMap<String, String> = [("region", region), ("zone", zone)]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    Candidate::new(nid(id), labels)
}

/// The set of `zone` labels the chosen nodes occupy, given the full candidate
/// pool.
fn zones_of(chosen: &[NodeId], pool: &[Candidate]) -> Vec<String> {
    let mut z: Vec<String> = chosen
        .iter()
        .map(|n| pool.iter().find(|c| c.node == *n).unwrap().labels["zone"].clone())
        .collect();
    z.sort();
    z
}

fn eu_pool() -> Vec<Candidate> {
    vec![
        node(10, "eu", "a"),
        node(11, "eu", "a"),
        node(12, "eu", "b"),
        node(13, "eu", "c"),
        node(20, "us", "a"), // outside residency
        node(21, "us", "b"),
    ]
}

#[test]
fn residency_restricts_to_matching_region() {
    let pool = eu_pool();
    let policy = PlacementPolicy::simple("eu", 3).require_label("region", "eu");
    let chosen = select_replicas(&pool, &policy).unwrap();
    assert_eq!(chosen.len(), 3);
    let eu_ids: Vec<NodeId> = (10..20).map(nid).collect();
    assert!(
        chosen.iter().all(|n| eu_ids.contains(n)),
        "a non-eu node was placed: {chosen:?}"
    );
}

#[test]
fn spreads_replicas_across_distinct_domains() {
    let pool = eu_pool();
    let policy = PlacementPolicy::simple("eu", 3)
        .require_label("region", "eu")
        .spread_across("zone", true);
    let chosen = select_replicas(&pool, &policy).unwrap();
    // One replica per zone a/b/c — never both eu-zone-a nodes (10 and 11).
    assert_eq!(zones_of(&chosen, &pool), vec!["a", "b", "c"]);
    assert!(!(chosen.contains(&nid(10)) && chosen.contains(&nid(11))));
}

#[test]
fn strict_spread_errors_when_too_few_domains() {
    // Only two zones exist, but RF=3 with strict spread needs three.
    let pool = vec![
        node(10, "eu", "a"),
        node(11, "eu", "a"),
        node(12, "eu", "b"),
    ];
    let policy = PlacementPolicy::simple("eu", 3)
        .require_label("region", "eu")
        .spread_across("zone", true);
    assert_eq!(
        select_replicas(&pool, &policy),
        Err(PlacementError::InsufficientDomains {
            needed: 3,
            available: 2
        })
    );
}

#[test]
fn best_effort_spread_doubles_up_only_after_each_domain_is_used() {
    // Two zones, RF=3, non-strict: a/a/b or a/b/b — every zone used, one doubled.
    let pool = vec![
        node(10, "eu", "a"),
        node(11, "eu", "a"),
        node(12, "eu", "b"),
        node(13, "eu", "b"),
    ];
    let policy = PlacementPolicy::simple("eu", 3)
        .require_label("region", "eu")
        .spread_across("zone", false);
    let chosen = select_replicas(&pool, &policy).unwrap();
    assert_eq!(chosen.len(), 3);
    let zones = zones_of(&chosen, &pool);
    assert!(zones.contains(&"a".to_string()) && zones.contains(&"b".to_string()));
}

#[test]
fn insufficient_candidates_errors() {
    let pool = vec![node(10, "eu", "a"), node(20, "us", "a")];
    let policy = PlacementPolicy::simple("eu", 3).require_label("region", "eu");
    assert_eq!(
        select_replicas(&pool, &policy),
        Err(PlacementError::InsufficientCandidates {
            needed: 3,
            eligible: 1
        })
    );
}

#[test]
fn selection_is_deterministic() {
    let pool = eu_pool();
    let policy = PlacementPolicy::simple("eu", 3)
        .require_label("region", "eu")
        .spread_across("zone", true);
    let a = select_replicas(&pool, &policy).unwrap();
    let b = select_replicas(&pool, &policy).unwrap();
    assert_eq!(a, b);
    assert!(
        a.windows(2).all(|w| w[0] < w[1]),
        "result not sorted: {a:?}"
    );
}

#[test]
fn replan_keeps_survivors_and_replaces_only_the_lost() {
    let pool = eu_pool();
    let policy = PlacementPolicy::simple("eu", 3)
        .require_label("region", "eu")
        .spread_across("zone", true);
    let current = select_replicas(&pool, &policy).unwrap(); // [10, 12, 13] (a, b, c)

    // Node 10 (zone a) is lost; the rest of the pool still has 11 in zone a.
    let survivors: Vec<Candidate> = pool.iter().filter(|c| c.node != nid(10)).cloned().collect();
    let new = replan(&current, &survivors, &policy).unwrap();

    assert_eq!(new.len(), 3);
    assert!(!new.contains(&nid(10)), "lost node not replaced: {new:?}");
    // The two survivors are kept; only the lost replica moved.
    for kept in current.iter().filter(|n| **n != nid(10)) {
        assert!(new.contains(kept), "survivor {kept} was needlessly moved");
    }
    // Spread is preserved: still one replica per zone.
    assert_eq!(zones_of(&new, &pool), vec!["a", "b", "c"]);
    assert!(
        new.contains(&nid(11)),
        "replacement should be the other zone-a node"
    );
}

#[test]
fn replan_is_a_noop_when_the_set_still_satisfies_the_policy() {
    let pool = eu_pool();
    let policy = PlacementPolicy::simple("eu", 3)
        .require_label("region", "eu")
        .spread_across("zone", true);
    let current = select_replicas(&pool, &policy).unwrap();
    // Nothing changed in the pool ⇒ the same set comes back unchanged.
    let again = replan(&current, &pool, &policy).unwrap();
    assert_eq!(again, current);
}

/// ADR 0050 fork F5 (`select_replicas_balanced`): fresh placement prefers
/// the least-loaded eligible nodes under the same hard constraints as
/// `select_replicas`, ties broken by node id, deterministic; a node absent
/// from the load map counts as zero (a fresh member is a genuine minimum).
#[test]
fn balanced_selection_prefers_least_loaded_within_policy() {
    use animus_placement::select_replicas_balanced;

    let pool = eu_pool();
    let policy = PlacementPolicy::simple("p", 2).require_label("region", "eu");
    // Heavy load on 10 and 12; 11 and 13 idle (13 absent from the map).
    let load: BTreeMap<NodeId, usize> = [(nid(10), 5), (nid(12), 4), (nid(11), 0)]
        .into_iter()
        .collect();
    let chosen = select_replicas_balanced(&pool, &policy, &load).unwrap();
    assert_eq!(chosen, vec![nid(11), nid(13)], "the two idle eu nodes");

    // Spread still binds: strict zone spread forces distinct zones even if
    // the two least-loaded nodes share one.
    let policy = PlacementPolicy::simple("p", 2)
        .require_label("region", "eu")
        .spread_across("zone", true);
    let load: BTreeMap<NodeId, usize> = [(nid(12), 5), (nid(13), 5)].into_iter().collect();
    // Least-loaded are 10 and 11 — both zone "a"; strict spread must pull
    // in a loaded node from another zone instead of doubling up.
    let chosen = select_replicas_balanced(&pool, &policy, &load).unwrap();
    let z = zones_of(&chosen, &pool);
    assert_eq!(z.len(), 2);
    assert_ne!(z[0], z[1], "strict spread beats load preference");
    assert!(
        chosen.contains(&nid(10)),
        "least-loaded zone-a node still first"
    );

    // Deterministic under repetition and equal load: ties break by node id.
    let even: BTreeMap<NodeId, usize> = BTreeMap::new();
    let eu3 = PlacementPolicy::simple("p", 3).require_label("region", "eu");
    let a = select_replicas_balanced(&pool, &eu3, &even).unwrap();
    let b = select_replicas_balanced(&pool, &eu3, &even).unwrap();
    assert_eq!(a, b);

    // RF too high for the eligible pool still errors like `select_replicas`.
    let eu5 = PlacementPolicy::simple("p", 5).require_label("region", "eu");
    assert!(matches!(
        select_replicas_balanced(&pool, &eu5, &even),
        Err(PlacementError::InsufficientCandidates { .. })
    ));
}
