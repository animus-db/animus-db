//! Tablet split/merge through the control plane: the operations replicate in
//! order, partition/recombine the keyspace correctly, and bump epochs (so the
//! data plane fences coordinators holding a pre-split view).

use std::time::Duration;

use animus_control::{MetaCommand, Metadata, RaftNode};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{Epoch, KeyRange, TabletId};

const NODES: [u64; 3] = [0, 1, 2];

fn leader(nodes: &[RaftNode<SimEnv>]) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?}");
    ls[0]
}

fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| RaftNode::start(sim.env(id), NODES.to_vec(), MemoryEngine::new()))
        .collect();
    (sim, nodes)
}

/// Whether the tablet map's ranges partition the keyspace contiguously from the
/// empty key with no gaps or overlaps.
fn partitions_keyspace(meta: &Metadata) -> bool {
    let mut tablets: Vec<_> = meta.tablets.values().collect();
    tablets.sort_by(|a, b| a.range.start.cmp(&b.range.start));
    let mut expected_start: Vec<u8> = Vec::new();
    for (i, t) in tablets.iter().enumerate() {
        if t.range.start != expected_start {
            return false;
        }
        match &t.range.end {
            Some(end) => expected_start = end.clone(),
            None => return i == tablets.len() - 1, // unbounded must be the last
        }
    }
    false // the final tablet should be unbounded-above
}

#[test]
fn split_then_merge_round_trips_through_raft() {
    let seed = 0x59_17;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes);

    nodes[l].propose(MetaCommand::CreateTablet {
        tablet: TabletId(1),
        table: None,
        range: KeyRange::whole(),
        replicas: NODES.to_vec(),
    });
    nodes[l].propose(MetaCommand::SplitTablet {
        tablet: TabletId(1),
        expected_epoch: Epoch::INITIAL,
        split_key: b"m".to_vec(),
        new_id: TabletId(2),
    });
    sim.run_for(Duration::from_secs(2));

    // Every node agrees, the keyspace stays partitioned, and the split bumped
    // the original tablet's epoch while the new one starts fresh.
    let meta = nodes[l].metadata();
    for n in &nodes {
        assert_eq!(n.metadata(), meta, "metadata diverged across nodes");
    }
    assert_eq!(meta.tablets.len(), 2);
    assert_eq!(
        meta.tablets[&TabletId(1)].range.end.as_deref(),
        Some(b"m".as_slice())
    );
    assert_eq!(
        meta.tablets[&TabletId(1)].epoch,
        Epoch(2),
        "split bumps source epoch"
    );
    assert_eq!(meta.tablets[&TabletId(2)].range.start, b"m");
    assert_eq!(meta.tablets[&TabletId(2)].range.end, None);
    assert_eq!(meta.tablets[&TabletId(2)].epoch, Epoch::INITIAL);
    assert!(
        partitions_keyspace(&meta),
        "keyspace not cleanly partitioned after split"
    );

    // Merge them back; the keyspace is whole again under tablet 1.
    nodes[l].propose(MetaCommand::MergeTablets {
        left: TabletId(1),
        expected_left_epoch: Epoch(2),
        right: TabletId(2),
        expected_right_epoch: Epoch::INITIAL,
    });
    sim.run_for(Duration::from_secs(2));

    let meta = nodes[l].metadata();
    assert_eq!(
        meta.tablets.len(),
        1,
        "merge should remove the right tablet"
    );
    assert_eq!(meta.tablets[&TabletId(1)].range, KeyRange::whole());
    assert_eq!(
        meta.tablets[&TabletId(1)].epoch,
        Epoch(3),
        "merge bumps epoch again"
    );
    assert!(
        meta.merged_tablets.contains(&TabletId(2)),
        "merge must record the merged-away tablet (ADR 0033)"
    );
    for n in &nodes {
        assert_eq!(n.metadata(), meta);
    }
}

/// Two proposers racing to merge tablets computed from an equally-stale view —
/// one merging `left`+`right` and another concurrently CAS-ing `left`'s
/// replica set (a rebalance move) — must not let the merge apply against a
/// replica set the merge proposer never actually observed. With the
/// double-epoch CAS, whichever commits first wins and the other is cleanly
/// rejected.
#[test]
fn merge_rejects_a_stale_epoch_racing_a_concurrent_replica_change() {
    let seed = 0x59_19;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes);

    nodes[l].propose(MetaCommand::CreateTablet {
        tablet: TabletId(1),
        table: None,
        range: KeyRange::new(b"".to_vec(), Some(b"m".to_vec())),
        replicas: NODES.to_vec(),
    });
    nodes[l].propose(MetaCommand::CreateTablet {
        tablet: TabletId(2),
        table: None,
        range: KeyRange::new(b"m".to_vec(), None),
        replicas: NODES.to_vec(),
    });
    sim.run_for(Duration::from_secs(2));

    // A concurrent replica-set change on `left` at its current epoch races the
    // merge (both proposed back-to-back on the same leader's log).
    nodes[l].propose(MetaCommand::CasTabletReplicas {
        tablet: TabletId(1),
        expected_epoch: Epoch::INITIAL,
        replicas: vec![NODES[0], NODES[1]],
    });
    nodes[l].propose(MetaCommand::MergeTablets {
        left: TabletId(1),
        expected_left_epoch: Epoch::INITIAL,
        right: TabletId(2),
        expected_right_epoch: Epoch::INITIAL,
    });
    sim.run_for(Duration::from_secs(2));

    let meta = nodes[l].metadata();
    for n in &nodes {
        assert_eq!(n.metadata(), meta, "metadata diverged across nodes");
    }
    // The replica CAS landed first (proposed first, same epoch); the merge's
    // `expected_left_epoch` is now stale, so it must have been rejected.
    assert_eq!(meta.tablets.len(), 2, "the stale merge must not apply");
    assert_eq!(
        meta.tablets[&TabletId(1)].replicas,
        vec![NODES[0], NODES[1]]
    );
    assert_eq!(meta.tablets[&TabletId(1)].epoch, Epoch(2));
    assert!(!meta.merged_tablets.contains(&TabletId(2)));
}

/// Two proposers racing to split the same tablet at the same epoch — each
/// computing a different median from an equally-stale view of the pre-split
/// range, exactly what two independent `auto_split_loop` instances (or an
/// auto-split racing a manual admin trigger) can do — must not both commit.
/// Before the `expected_epoch` CAS, both would apply (each split key is
/// strictly inside the tablet's *original* range), minting two child tablet
/// ids when the tablet's own per-group CP-data Raft can only ever host one
/// real split, ever — leaving the loser permanently orphaned (observed live
/// under sustained `--auto-split` bulk-seed load). With the CAS, only the
/// first proposal to land in the log applies; the second is cleanly rejected
/// once the epoch has moved, so no orphan tablet id is ever minted.
#[test]
fn racing_splits_at_the_same_epoch_only_one_applies() {
    let seed = 0x59_18;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes);

    nodes[l].propose(MetaCommand::CreateTablet {
        tablet: TabletId(1),
        table: None,
        range: KeyRange::whole(),
        replicas: NODES.to_vec(),
    });
    sim.run_for(Duration::from_secs(2));

    nodes[l].propose(MetaCommand::SplitTablet {
        tablet: TabletId(1),
        expected_epoch: Epoch::INITIAL,
        split_key: b"m".to_vec(),
        new_id: TabletId(2),
    });
    nodes[l].propose(MetaCommand::SplitTablet {
        tablet: TabletId(1),
        expected_epoch: Epoch::INITIAL,
        split_key: b"q".to_vec(),
        new_id: TabletId(3),
    });
    sim.run_for(Duration::from_secs(2));

    let meta = nodes[l].metadata();
    for n in &nodes {
        assert_eq!(n.metadata(), meta, "metadata diverged across nodes");
    }
    assert_eq!(
        meta.tablets.len(),
        2,
        "the losing split must not create an orphan tablet"
    );
    assert_eq!(
        meta.tablets[&TabletId(1)].epoch,
        Epoch(2),
        "the source tablet split exactly once"
    );
    assert!(
        meta.tablets.contains_key(&TabletId(2)) ^ meta.tablets.contains_key(&TabletId(3)),
        "exactly one of the two racing splits should have won"
    );
    assert!(
        partitions_keyspace(&meta),
        "keyspace not cleanly partitioned after the race"
    );
}

#[test]
fn invalid_split_and_merge_are_rejected_deterministically() {
    let mut meta = Metadata::default();
    meta.apply(&MetaCommand::CreateTablet {
        tablet: TabletId(1),
        table: None,
        range: KeyRange::new(b"a".to_vec(), Some(b"c".to_vec())),
        replicas: vec![0, 1],
    });
    meta.apply(&MetaCommand::CreateTablet {
        tablet: TabletId(9),
        table: None,
        range: KeyRange::new(b"x".to_vec(), Some(b"z".to_vec())),
        replicas: vec![0, 1],
    });

    use animus_control::ApplyOutcome::Rejected;
    // Split key outside the range.
    assert!(matches!(
        meta.apply(&MetaCommand::SplitTablet {
            tablet: TabletId(1),
            expected_epoch: Epoch::INITIAL,
            split_key: b"q".to_vec(),
            new_id: TabletId(2),
        }),
        Rejected(_)
    ));
    // Non-adjacent tablets cannot merge ([a,c) and [x,z) have a gap).
    assert!(matches!(
        meta.apply(&MetaCommand::MergeTablets {
            left: TabletId(1),
            expected_left_epoch: Epoch::INITIAL,
            right: TabletId(9),
            expected_right_epoch: Epoch::INITIAL,
        }),
        Rejected(_)
    ));
    // State unchanged by the rejected commands.
    assert_eq!(meta.tablets.len(), 2);
    assert_eq!(meta.tablets[&TabletId(1)].epoch, Epoch::INITIAL);
}

/// A merge across two different tables' tablets is rejected even when their
/// ranges happen to abut and their replica sets happen to coincide — the
/// physical keyspace of each side lives under a different table's
/// `StorageScope` prefix on the shared engine (ADR 0026/0028), so merging
/// them would silently conflate two unrelated tables' data.
#[test]
fn merge_rejects_tablets_from_different_tables() {
    let mut meta = Metadata::default();
    meta.apply(&MetaCommand::CreateTablet {
        tablet: TabletId(1),
        table: Some("users".to_owned()),
        range: KeyRange::new(b"".to_vec(), Some(b"m".to_vec())),
        replicas: vec![0, 1],
    });
    meta.apply(&MetaCommand::CreateTablet {
        tablet: TabletId(2),
        table: Some("orders".to_owned()),
        range: KeyRange::new(b"m".to_vec(), None),
        replicas: vec![0, 1],
    });

    use animus_control::ApplyOutcome::Rejected;
    assert!(matches!(
        meta.apply(&MetaCommand::MergeTablets {
            left: TabletId(1),
            expected_left_epoch: Epoch::INITIAL,
            right: TabletId(2),
            expected_right_epoch: Epoch::INITIAL,
        }),
        Rejected(_)
    ));
    assert_eq!(meta.tablets.len(), 2, "cross-table merge must not apply");
}

/// A split child inherits the source tablet's placement policy (ADR 0029):
/// without it the new sibling would have no policy and be invisible to both the
/// repair reconciler and the load rebalancer, so it would never be re-placed or
/// balanced onto new members.
#[test]
fn split_child_inherits_the_source_policy() {
    use animus_control::ApplyOutcome::Applied;
    use animus_placement::PlacementPolicy;

    let mut meta = Metadata::default();
    let policy = PlacementPolicy::simple("cp-rf3", 3).require_label("region", "eu");
    assert_eq!(
        meta.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("users".to_owned()),
            range: KeyRange::whole(),
            replicas: vec![10, 11, 12],
        }),
        Applied
    );
    assert_eq!(
        meta.apply(&MetaCommand::SetTabletPolicy {
            tablet: TabletId(1),
            policy: Some(policy.clone()),
        }),
        Applied
    );

    // Split the tablet; the new sibling id must carry the same policy.
    assert_eq!(
        meta.apply(&MetaCommand::SplitTablet {
            tablet: TabletId(1),
            expected_epoch: Epoch::INITIAL,
            split_key: b"m".to_vec(),
            new_id: TabletId(2),
        }),
        Applied
    );
    assert_eq!(meta.policies.get(&TabletId(1)), Some(&policy));
    assert_eq!(
        meta.policies.get(&TabletId(2)),
        Some(&policy),
        "split child did not inherit the source's placement policy"
    );

    // A split of a policy-less tablet leaves the child policy-less (no panic).
    assert_eq!(
        meta.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(3),
            table: None,
            range: KeyRange::whole(),
            replicas: vec![10, 11, 12],
        }),
        Applied
    );
    assert_eq!(
        meta.apply(&MetaCommand::SplitTablet {
            tablet: TabletId(3),
            expected_epoch: Epoch::INITIAL,
            split_key: b"m".to_vec(),
            new_id: TabletId(4),
        }),
        Applied
    );
    assert!(!meta.policies.contains_key(&TabletId(4)));
}
