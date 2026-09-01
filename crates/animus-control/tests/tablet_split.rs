//! Tablet split through the control plane: the operation replicates in
//! order, partitions the keyspace correctly, and bumps epochs (so the
//! data plane fences coordinators holding a pre-split view).

use std::time::Duration;

use animus_control::{MetaCommand, Metadata, RaftNode};
use animus_env::nid;
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
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
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
fn split_round_trips_through_raft() {
    let seed = 0x59_17;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes);

    nodes[l].propose(MetaCommand::CreateTablet {
        tablet: TabletId(1),
        table: None,
        range: KeyRange::whole(),
        replicas: NODES.iter().copied().map(nid).collect(),
    });
    let replicas: Vec<_> = NODES.iter().copied().map(nid).collect();
    nodes[l].propose(MetaCommand::BeginSplitInPlace {
        parent: TabletId(1),
        expected_epoch: Epoch::INITIAL,
        split_key: b"m".to_vec(),
        children: [(TabletId(2), replicas.clone()), (TabletId(3), replicas)],
    });
    sim.run_for(Duration::from_secs(2));
    let bumped = nodes[l].metadata().tablets[&TabletId(1)].epoch;
    nodes[l].propose(MetaCommand::CutoverSplit {
        parent: TabletId(1),
        expected_epoch: bumped,
        cutover_wall_ms: 1_000,
    });
    sim.run_for(Duration::from_secs(2));

    // Every node agrees, the parent is retired, the children partition the
    // keyspace, and both carry frozen lineage naming the parent (ADR 0050).
    let meta = nodes[l].metadata();
    for n in &nodes {
        assert_eq!(n.metadata(), meta, "metadata diverged across nodes");
    }
    assert_eq!(meta.tablets.len(), 2);
    assert!(!meta.tablets.contains_key(&TabletId(1)), "parent retired");
    assert_eq!(
        meta.tablets[&TabletId(2)].range.end.as_deref(),
        Some(b"m".as_slice())
    );
    assert_eq!(meta.tablets[&TabletId(3)].range.start, b"m");
    assert_eq!(meta.tablets[&TabletId(3)].range.end, None);
    assert_eq!(meta.split_lineage[&TabletId(2)].parent, TabletId(1));
    assert_eq!(meta.split_lineage[&TabletId(3)].parent, TabletId(1));
    assert!(
        partitions_keyspace(&meta),
        "keyspace not cleanly partitioned after split"
    );
}

/// Two proposers racing to split the same tablet at the same epoch — each
/// computing a different median from an equally-stale view of the pre-split
/// range, exactly what two independent `auto_split_loop` instances (or an
/// auto-split racing a manual admin trigger) can do — must not both commit.
/// Before the `expected_epoch` CAS, both would apply (each split key is
/// strictly inside the tablet's *original* range), each recording a
/// DIFFERING in-place intent on the SAME parent when a tablet can only ever
/// carry one live split intent at a time — leaving the loser's own intent
/// silently overwritten, never reachable again (observed live under
/// sustained `--auto-split` bulk-seed load, the original copy-based-split
/// shape of this hazard). With the CAS, only the first proposal to land in
/// the log applies; the second is cleanly rejected once the epoch has
/// moved, so only one intent (and, eventually, one real child pair) is ever
/// recorded.
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
        replicas: NODES.iter().copied().map(nid).collect(),
    });
    sim.run_for(Duration::from_secs(2));

    let replicas: Vec<_> = NODES.iter().copied().map(nid).collect();
    nodes[l].propose(MetaCommand::BeginSplitInPlace {
        parent: TabletId(1),
        expected_epoch: Epoch::INITIAL,
        split_key: b"m".to_vec(),
        children: [
            (TabletId(2), replicas.clone()),
            (TabletId(3), replicas.clone()),
        ],
    });
    nodes[l].propose(MetaCommand::BeginSplitInPlace {
        parent: TabletId(1),
        expected_epoch: Epoch::INITIAL,
        split_key: b"q".to_vec(),
        children: [(TabletId(4), replicas.clone()), (TabletId(5), replicas)],
    });
    sim.run_for(Duration::from_secs(2));

    let meta = nodes[l].metadata();
    for n in &nodes {
        assert_eq!(n.metadata(), meta, "metadata diverged across nodes");
    }
    assert_eq!(
        meta.tablets.len(),
        1,
        "the losing begin-split-in-place must not mint anything at all — in-place split \
         mints no tablet-map rows until cutover, which this test never reaches"
    );
    let intent = meta.tablets[&TabletId(1)]
        .inplace_split
        .as_ref()
        .expect("the winning proposal's intent must be recorded");
    let won_first = intent.children[0].id == TabletId(2);
    let won_second = intent.children[0].id == TabletId(4);
    assert!(
        won_first ^ won_second,
        "exactly one of the two racing begin-split-in-place proposals should have won: {intent:?}"
    );
    assert_eq!(
        meta.tablets[&TabletId(1)].range.end,
        None,
        "a Splitting parent's own range is untouched (the winning intent carries the split key)"
    );
}

#[test]
fn invalid_split_is_rejected_deterministically() {
    let mut meta = Metadata::default();
    meta.apply(&MetaCommand::CreateTablet {
        tablet: TabletId(1),
        table: None,
        range: KeyRange::new(b"a".to_vec(), Some(b"c".to_vec())),
        replicas: vec![nid(0), nid(1)],
    });

    use animus_control::ApplyOutcome::Rejected;
    // Split key outside the range.
    assert!(matches!(
        meta.apply(&MetaCommand::BeginSplitInPlace {
            parent: TabletId(1),
            expected_epoch: Epoch::INITIAL,
            split_key: b"q".to_vec(),
            children: [(TabletId(2), vec![nid(0)]), (TabletId(3), vec![nid(0)])],
        }),
        Rejected(_)
    ));
    // State unchanged by the rejected command.
    assert_eq!(meta.tablets.len(), 1);
    assert_eq!(meta.tablets[&TabletId(1)].epoch, Epoch::INITIAL);
}

/// A split child inherits the source tablet's placement policy (ADR 0029):
/// without it the new sibling would have no policy and be invisible to both the
/// repair reconciler and the load rebalancer, so it would never be re-placed or
/// balanced onto new members.
///
/// **Deliberately still `BeginSplit`** (copy-split deletion stack, layer 1):
/// its whole subject — a child's `Metadata::policies` row existing
/// immediately at BEGIN time, copied straight onto the freshly-minted
/// `Building` row — is copy-workflow-specific mechanics with no in-place
/// analogue at this point in that command's own lifecycle (`BeginSplitInPlace`
/// copies no policy at all — there is no tablet row yet to attach one to;
/// see that command's own doc). The equivalent in-place behavior (a policy
/// inherited onto both children, but only at `CutoverSplit` — the in-place
/// workflow's one and only chance to) is already covered by
/// `meta::tests::cutover_split_in_place_activates_both_children_from_the_
/// intent`. Left for the later layer that deletes the copy-based workflow
/// to remove.
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
            replicas: vec![nid(10), nid(11), nid(12)],
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

    // Begin the split; BOTH children must carry the same policy.
    let homes = vec![nid(10), nid(11), nid(12)];
    assert_eq!(
        meta.apply(&MetaCommand::BeginSplit {
            parent: TabletId(1),
            expected_epoch: Epoch::INITIAL,
            split_key: b"m".to_vec(),
            children: [(TabletId(2), homes.clone()), (TabletId(5), homes.clone())],
        }),
        Applied
    );
    assert_eq!(meta.policies.get(&TabletId(1)), Some(&policy));
    assert_eq!(
        meta.policies.get(&TabletId(2)),
        Some(&policy),
        "split child did not inherit the source's placement policy"
    );
    assert_eq!(meta.policies.get(&TabletId(5)), Some(&policy));

    // A split of a policy-less tablet leaves the child policy-less (no panic).
    assert_eq!(
        meta.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(3),
            table: None,
            range: KeyRange::whole(),
            replicas: vec![nid(10), nid(11), nid(12)],
        }),
        Applied
    );
    let next = meta.next_free_tablet_id();
    assert_eq!(
        meta.apply(&MetaCommand::BeginSplit {
            parent: TabletId(3),
            expected_epoch: Epoch::INITIAL,
            split_key: b"m".to_vec(),
            children: [
                (next, vec![nid(10), nid(11), nid(12)]),
                (TabletId(next.0 + 1), vec![nid(10), nid(11), nid(12)]),
            ],
        }),
        Applied
    );
    assert!(!meta.policies.contains_key(&next));
}
