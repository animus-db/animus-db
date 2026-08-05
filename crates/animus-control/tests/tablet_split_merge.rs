//! Tablet split/merge through the control plane: the operations replicate in
//! order, partition/recombine the keyspace correctly, and bump epochs (so the
//! data plane fences coordinators holding a pre-split view).

use std::time::Duration;

use animus_control::{MetaCommand, Metadata, RaftNode};
use animus_sim::{SimEnv, Simulator};
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
        .map(|&id| RaftNode::start(sim.env(id), NODES.to_vec()))
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
        right: TabletId(2),
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
    for n in &nodes {
        assert_eq!(n.metadata(), meta);
    }
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
            split_key: b"q".to_vec(),
            new_id: TabletId(2),
        }),
        Rejected(_)
    ));
    // Non-adjacent tablets cannot merge ([a,c) and [x,z) have a gap).
    assert!(matches!(
        meta.apply(&MetaCommand::MergeTablets {
            left: TabletId(1),
            right: TabletId(9)
        }),
        Rejected(_)
    ));
    // State unchanged by the rejected commands.
    assert_eq!(meta.tablets.len(), 2);
    assert_eq!(meta.tablets[&TabletId(1)].epoch, Epoch::INITIAL);
}
