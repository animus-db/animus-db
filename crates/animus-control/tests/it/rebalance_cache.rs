//! The leader's rebalance-plan cache (`animus_control::rebalance_cache`, issue
//! #1192, ADR 0029's 2026-10-11 amendment).
//!
//! Correctness requirement under test: **whatever the cluster does between
//! ticks, the move the cache proposes equals what a fresh `rebalance_step`
//! (`Metadata::rebalance`) chooses on the current `Metadata`.** The property
//! test drives a leader-shaped sequence of ticks (the cache sees a
//! `PlacementView` clone, exactly as `reconcile_loop` does) interleaved with
//! random external `Metadata` changes -- node add/remove, liveness flips,
//! new tablets, splits (and their directed-Placing completion), foreign
//! `CasTabletReplicas`, a `recently_done` grace window opening and closing, a
//! leadership change -- and with the leader's own proposals sometimes lost or
//! landing a tick late. Every run is a pure function of its seed
//! (`ANIMUS_REBALANCE_CACHE_SEEDS=K` deepens it, `ANIMUS_SEED=<seed>` replays
//! one).

use std::collections::{BTreeMap, BTreeSet};

use animus_control::meta::{ApplyOutcome, MetaCommand, Metadata, NodeStatus};
use animus_control::rebalance_cache::RebalanceCache;
use animus_control::schema::{ColumnType, TableSchema};
use animus_env::{NodeId, nid};
use animus_placement::PlacementPolicy;
use animus_tablet::{KeyRange, TabletId};

/// Deterministic splitmix64 -- the test is a pure function of its seed.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return vec![s];
    }
    let k: u64 = std::env::var("ANIMUS_REBALANCE_CACHE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    (0..k * 24).map(|i| 0xC0FFEE + i).collect()
}

fn upsert(meta: &mut Metadata, id: u64, status: NodeStatus) {
    assert_eq!(
        meta.apply(&MetaCommand::UpsertMember {
            node: nid(id),
            labels: BTreeMap::new(),
            status,
        }),
        ApplyOutcome::Applied
    );
}

fn add_tablet(meta: &mut Metadata, id: u64, replicas: Vec<NodeId>) -> bool {
    let tablet = TabletId(id);
    let created = meta.apply(&MetaCommand::CreateTablet {
        tablet,
        table: None,
        range: KeyRange::whole(),
        replicas,
    });
    if created != ApplyOutcome::Applied {
        return false;
    }
    assert_eq!(
        meta.apply(&MetaCommand::SetTabletPolicy {
            tablet,
            policy: Some(PlacementPolicy::simple("rf3", 3)),
        }),
        ApplyOutcome::Applied
    );
    meta.next_tablet_id = meta.next_tablet_id.max(id + 1);
    true
}

fn rf3(i: usize, nodes: usize) -> Vec<NodeId> {
    let mut v: Vec<NodeId> = (0..3).map(|j| nid(((i + j) % nodes) as u64)).collect();
    v.sort_unstable();
    v
}

fn base(tablets: usize, nodes: usize) -> Metadata {
    let mut meta = Metadata::default();
    for n in 0..nodes {
        upsert(&mut meta, n as u64, NodeStatus::Active);
    }
    for i in 0..tablets {
        assert!(add_tablet(&mut meta, i as u64 + 1, rf3(i, nodes)));
    }
    meta
}

/// One random external change. Returns `true` if `meta` changed.
fn external(rng: &mut Rng, meta: &mut Metadata, next_node: &mut u64, splits: &mut u64) -> bool {
    match rng.below(9) {
        // node add
        0 => {
            upsert(meta, *next_node, NodeStatus::Active);
            *next_node += 1;
            true
        }
        // node remove
        1 => {
            let ids: Vec<NodeId> = meta.members.keys().cloned().collect();
            let node = ids[rng.below(ids.len())].clone();
            meta.apply(&MetaCommand::RemoveMember { node }) == ApplyOutcome::Applied
        }
        // liveness flip (Active <-> Down / Leaving)
        2 | 3 => {
            let ids: Vec<NodeId> = meta.members.keys().cloned().collect();
            let node = ids[rng.below(ids.len())].clone();
            let status = match (meta.members[&node].status, rng.below(3)) {
                (NodeStatus::Active, 0) => NodeStatus::Leaving,
                (NodeStatus::Active, _) => NodeStatus::Down,
                _ => NodeStatus::Active,
            };
            meta.apply(&MetaCommand::UpsertMember {
                node: node.clone(),
                labels: BTreeMap::new(),
                status,
            }) == ApplyOutcome::Applied
        }
        // new tablet
        4 => {
            let id = meta.next_free_tablet_id().0;
            let ids: Vec<NodeId> = meta.members.keys().cloned().collect();
            if ids.len() < 3 {
                return false;
            }
            let start = rng.below(ids.len());
            let mut replicas: Vec<NodeId> = (0..3)
                .map(|j| ids[(start + j) % ids.len()].clone())
                .collect();
            replicas.sort_unstable();
            add_tablet(meta, id, replicas)
        }
        // split, then maybe finish its directed Placing
        5 => {
            let ids: Vec<TabletId> = meta.tablets.keys().copied().collect();
            let parent = ids[rng.below(ids.len())];
            let t = meta.tablets[&parent].clone();
            let a = meta.next_free_tablet_id();
            let b = TabletId(a.0 + 1);
            let key = 0x8000_0000_0000_0000u64.to_be_bytes().to_vec();
            let begun = meta.apply(&MetaCommand::BeginSplitInPlace {
                parent,
                expected_epoch: t.epoch,
                split_key: key,
                children: [(a, t.replicas.clone()), (b, t.replicas.clone())],
            });
            if begun != ApplyOutcome::Applied {
                return false;
            }
            let cut = meta.apply(&MetaCommand::CutoverSplit {
                parent,
                expected_epoch: t.epoch.next(),
                cutover_wall_ms: 1,
            });
            if cut == ApplyOutcome::Applied {
                *splits += 1;
            }
            true
        }
        // finish a pending directed Placing
        6 => {
            let pending: Vec<TabletId> = meta
                .split_placing
                .iter()
                .filter(|(_, e)| !e.done)
                .map(|(t, _)| *t)
                .collect();
            if pending.is_empty() {
                return false;
            }
            let tablet = pending[rng.below(pending.len())];
            let expected_epoch = meta.tablets[&tablet].epoch;
            meta.apply(&MetaCommand::MarkSplitPlacingDone {
                tablet,
                expected_epoch,
            }) == ApplyOutcome::Applied
        }
        // a foreign replica-set change (another proposer's CAS)
        7 => {
            let ids: Vec<TabletId> = meta.tablets.keys().copied().collect();
            let tablet = ids[rng.below(ids.len())];
            let t = &meta.tablets[&tablet];
            let members: Vec<NodeId> = meta.members.keys().cloned().collect();
            if members.len() < 3 {
                return false;
            }
            let start = rng.below(members.len());
            let mut replicas: Vec<NodeId> = (0..3)
                .map(|j| members[(start + j) % members.len()].clone())
                .collect();
            replicas.sort_unstable();
            meta.apply(&MetaCommand::CasTabletReplicas {
                tablet,
                expected_epoch: t.epoch,
                replicas,
            }) == ApplyOutcome::Applied
        }
        // an unrelated command (its apply still bumps the revision)
        _ => {
            meta.apply(&MetaCommand::CreateTableSchema {
                table: format!("noise{}", rng.next() % 4),
                schema: TableSchema::composite("pk", ColumnType::String, "sk", ColumnType::String),
            }) == ApplyOutcome::Applied
        }
    }
}

#[test]
fn cached_move_equals_fresh_rebalance_step_under_external_changes() {
    let mut total_ticks = 0u64;
    let mut total_rebuilds = 0u64;
    let mut total_splits = 0u64;
    let mut total_advances = 0u64;
    for seed in seeds() {
        let mut rng = Rng(seed);
        let nodes = 3 + rng.below(3);
        let mut meta = base(6 + rng.below(30), nodes);
        let mut next_node = nodes as u64;
        let mut cache = RebalanceCache::new();
        let mut term = 1u64;
        let mut done: BTreeSet<TabletId> = BTreeSet::new();
        let none_down = BTreeSet::new();
        // Proposals accepted but not yet applied (they land a tick late).
        let mut in_flight: Vec<MetaCommand> = Vec::new();
        // Start from an imbalanced cluster so there is a plan to cache.
        for _ in 0..3 {
            upsert(&mut meta, next_node, NodeStatus::Active);
            next_node += 1;
        }
        for tick in 0..400 {
            // Land (or lose) earlier proposals.
            if !in_flight.is_empty() && rng.below(2) == 0 {
                for cmd in in_flight.drain(..) {
                    meta.apply(&cmd);
                }
            }
            // External churn on a fraction of ticks.
            if rng.below(5) == 0 {
                external(&mut rng, &mut meta, &mut next_node, &mut total_splits);
            }
            if rng.below(12) == 0 {
                // The post-`done` grace window opens/closes.
                done = if done.is_empty() {
                    meta.tablets
                        .keys()
                        .copied()
                        .filter(|_| rng.below(4) == 0)
                        .collect()
                } else {
                    BTreeSet::new()
                };
            }
            if rng.below(40) == 0 {
                term += 1; // leadership changed hands and came back
            }
            let view = meta.placement_view();
            let got = cache.next(&view, term, &done);
            let want = meta.rebalance(&done, &none_down);
            assert_eq!(
                got,
                want,
                "seed {seed} tick {tick}: cached move != fresh rebalance_step \
                 (in flight: {})",
                in_flight.len()
            );
            total_ticks += 1;
            if let Some(cmd) = got {
                match rng.below(8) {
                    0 => cache.invalidate(),      // proposal refused (e.g. NotLeader)
                    1 => {}                       // accepted, lost before commit
                    2 | 3 => in_flight.push(cmd), // commits a tick late
                    _ => {
                        meta.apply(&cmd);
                    }
                }
            }
        }
        let st = cache.stats();
        total_rebuilds += st.rebuilds;
        total_advances += st.advances;
    }
    println!(
        "rebalance_cache property: ticks={total_ticks} rebuilds={total_rebuilds} \
         advances={total_advances} splits={total_splits}"
    );
    assert!(total_splits > 0, "the schedule never completed a split");
    assert!(total_advances > 0, "the cache never served a cached move");
    assert!(
        total_rebuilds < total_ticks,
        "no amortization at all: {total_rebuilds} rebuilds / {total_ticks} ticks"
    );
}

/// Quiet convergence: no external changes, every proposal applies before the
/// next tick. The move sequence equals the one-step reference loop and the
/// plan is rebuilt O(log moves) times, not once per move.
#[test]
fn quiet_convergence_rebuilds_rarely_and_matches_reference() {
    let none_done = BTreeSet::new();
    let none_down = BTreeSet::new();
    let mut meta = base(150, 3);
    for n in 3..9 {
        upsert(&mut meta, n, NodeStatus::Active);
    }
    let mut reference = meta.clone();
    let mut expected = Vec::new();
    while let Some(cmd) = reference.rebalance(&none_done, &none_down) {
        assert_eq!(reference.apply(&cmd), ApplyOutcome::Applied);
        expected.push(cmd);
    }
    assert!(expected.len() > 100, "scenario too small to be meaningful");

    let mut cache = RebalanceCache::new();
    let mut got = Vec::new();
    loop {
        let view = meta.placement_view();
        let Some(cmd) = cache.next(&view, 1, &none_done) else {
            break;
        };
        assert_eq!(meta.apply(&cmd), ApplyOutcome::Applied);
        got.push(cmd);
    }
    assert_eq!(got, expected, "cached sequence != repeated single steps");
    let st = cache.stats();
    println!(
        "quiet convergence: moves={} evals={} rebuilds={} advances={}",
        got.len(),
        st.evals,
        st.rebuilds,
        st.advances
    );
    assert!(
        st.rebuilds <= 5 && st.rebuilds * 20 <= got.len() as u64,
        "plan rebuilt {} times for {} moves",
        st.rebuilds,
        got.len()
    );
    // Balanced and unchanged: further evaluations are O(1) and rebuild nothing.
    let before = cache.stats().rebuilds;
    for _ in 0..10 {
        assert!(cache.next(&meta.placement_view(), 1, &none_done).is_none());
    }
    assert_eq!(cache.stats().rebuilds, before);
}

/// A proposal still in flight leaves the view unchanged: the cache offers the
/// same head again (what the uncached decision does) and does not run ahead.
#[test]
fn unapplied_head_is_reproposed_not_skipped() {
    let none_done = BTreeSet::new();
    let mut meta = base(30, 3);
    for n in 3..6 {
        upsert(&mut meta, n, NodeStatus::Active);
    }
    let mut cache = RebalanceCache::new();
    let first = cache.next(&meta.placement_view(), 1, &none_done).unwrap();
    for _ in 0..3 {
        assert_eq!(
            cache.next(&meta.placement_view(), 1, &none_done),
            Some(first.clone())
        );
    }
    assert_eq!(cache.stats().rebuilds, 1);
    meta.apply(&first);
    let second = cache.next(&meta.placement_view(), 1, &none_done).unwrap();
    assert_ne!(first, second);
    assert_eq!(
        second,
        meta.rebalance(&none_done, &BTreeSet::new()).unwrap()
    );
    assert_eq!(cache.stats().rebuilds, 1, "served from the cached plan");
}
