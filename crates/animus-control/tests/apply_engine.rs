//! ADR 0038 PR3's differential oracle: the apply task's published `Metadata`
//! cache always agrees with an independent scan-and-rebuild of the same
//! node's system-keyspace engine (`mirror::rebuild_metadata_from_engine`) —
//! the property that makes the cutover safe (the engine, not any in-core
//! field, is now the durable source of truth). The successor of PR2's
//! shadow-mode `mirror_engine.rs`: there is no more "shadow" side to diff
//! against a separate "real" side — `cache` *is* the real side, and this
//! file asserts it never diverges from its own engine, including across a
//! genuine crash + restart (proving the restart-recovery contract: rebuild
//! from the engine's `_applied_index` watermark, replay only the log tail).

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::{MetaCommand, NodeStatus, RaftNode, mirror};
use animus_env::nid;
use animus_placement::PlacementPolicy;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{Epoch, KeyRange, TabletId};

const NODES: [u64; 3] = [0, 1, 2];

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(node),
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

fn unique_leader(nodes: &[RaftNode<SimEnv>], seed: u64) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    ls[0]
}

/// Assert every node's published cache agrees with its own engine's rebuild
/// — the core invariant this whole PR is built on.
async fn assert_cache_matches_engine(
    nodes: &[RaftNode<SimEnv>],
    engines: &[MemoryEngine],
    seed: u64,
    label: &str,
) {
    for (i, (node, engine)) in nodes.iter().zip(engines).enumerate() {
        let rebuilt = mirror::rebuild_metadata_from_engine(engine)
            .await
            .expect("engine scan");
        assert_eq!(
            rebuilt,
            node.metadata(),
            "seed={seed} node {i} ({label}): cache diverged from its own engine's rebuild"
        );
    }
}

/// A seed-swept mixed scenario — membership, tablet create/split/merge,
/// schema DDL, keyspace, node-id-allocation — asserting the cache/engine
/// agreement holds after every step, then a crash-and-restart of one node,
/// asserting it holds again once the restarted node reconverges.
#[test]
fn cache_matches_engine_through_a_mixed_scenario_and_a_restart() {
    for seed in [0x5CE0_0001u64, 0x5CE0_0002, 0x5CE0_0003] {
        run_scenario(seed);
    }
}

fn run_scenario(seed: u64) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async move {
        let mut sim = Simulator::new(seed);
        let engines: Vec<MemoryEngine> = NODES.iter().map(|_| MemoryEngine::new()).collect();
        let mut nodes: Vec<RaftNode<SimEnv>> = NODES
            .iter()
            .map(|&id| {
                RaftNode::start(
                    sim.env(nid(id)),
                    NODES.iter().copied().map(nid).collect(),
                    engines[id as usize].clone(),
                )
            })
            .collect();
        sim.run_for(Duration::from_secs(2));
        let leader = unique_leader(&nodes, seed);

        // Membership + a table's tablet lifecycle + schema + keyspace + a
        // cluster-allocated node id, one after another.
        nodes[leader].propose(upsert(10));
        nodes[leader].propose(upsert(11));
        nodes[leader].propose(MetaCommand::CreateTableSchema {
            table: "orders".to_string(),
            schema: animus_control::TableSchema::simple("id", animus_control::ColumnType::String),
        });
        nodes[leader].propose(MetaCommand::CreateKeyspace {
            keyspace: "ks1".to_string(),
        });
        nodes[leader].propose(MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("orders".to_string()),
            range: KeyRange::whole(),
            replicas: vec![nid(10), nid(11)],
        });
        nodes[leader].propose(MetaCommand::SetTabletPolicy {
            tablet: TabletId(1),
            policy: Some(PlacementPolicy::simple("p", 2)),
        });
        nodes[leader].propose(MetaCommand::AllocateNodeId {
            nonce: format!("join-{seed}"),
            labels: BTreeMap::new(),
        });
        sim.run_for(Duration::from_secs(2));
        assert_cache_matches_engine(&nodes, &engines, seed, "after initial commands").await;

        // Split the tablet, then drop the table (removes every tablet + policy).
        let split_key = vec![128u8];
        nodes[leader].propose(MetaCommand::SplitTablet {
            tablet: TabletId(1),
            expected_epoch: Epoch::INITIAL,
            split_key,
            new_id: TabletId(2),
        });
        sim.run_for(Duration::from_secs(1));
        assert_cache_matches_engine(&nodes, &engines, seed, "after split").await;

        nodes[leader].propose(MetaCommand::DropTableTablets {
            table: "orders".to_string(),
        });
        sim.run_for(Duration::from_secs(1));
        assert_cache_matches_engine(&nodes, &engines, seed, "after drop-table").await;

        // Crash-and-restart a follower on the same disk *and* the same
        // (durable) engine — recovery rebuilds the cache from the engine's
        // watermark and replays only the surviving log tail.
        let follower = (0..3).find(|&i| i != leader).unwrap();
        sim.stop(nid(follower as u64));
        sim.run_for(Duration::from_millis(200));
        nodes[follower] = RaftNode::start(
            sim.env(nid(follower as u64)),
            NODES.iter().copied().map(nid).collect(),
            engines[follower].clone(),
        );
        sim.run_for(Duration::from_secs(3));

        assert_cache_matches_engine(&nodes, &engines, seed, "after restart").await;
        // And every node still agrees with every other node.
        let leader = unique_leader(&nodes, seed);
        let reference = nodes[leader].metadata();
        for (i, n) in nodes.iter().enumerate() {
            assert_eq!(
                n.metadata(),
                reference,
                "seed={seed} node {i}: diverged from the leader after restart"
            );
        }
    });
}
