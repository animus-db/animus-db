//! Differential-oracle + crash-recovery coverage for the ADR 0038 PR2 shadow
//! mirror (`animus_control::mirror`): a control group driven under `SimEnv`
//! with `RaftNode::start_with_mirror` attached on one node, asserting that
//! `mirror::rebuild_metadata_from_engine` — a `Metadata` scanned purely out of
//! the mirror's `StorageEngine` — is byte-identical (`PartialEq`) to that
//! node's own real, unchanged in-core `Metadata` at every checkpoint,
//! including across a simulated crash + restart.
//!
//! This is the shadow-mode mirror's whole point: it must be provably
//! redundant with the in-core state it shadows (zero behavior change) before
//! anything is ever allowed to *read* it instead (PR3's cutover).

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::mirror::rebuild_metadata_from_engine;
use animus_control::{ColumnType, IndexDef, IndexKind, IndexProjection};
use animus_control::{
    MetaCommand, Metadata, NodeStatus, PlacementPolicy, RaftNode, ReplicationMode, TableSchema,
};
use animus_env::Env as _;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, TabletId};

const NODES: [u64; 3] = [0, 1, 2];
const TABLE: &str = "orders";

fn leader(nodes: &[RaftNode<SimEnv>]) -> usize {
    (0..nodes.len())
        .find(|&i| nodes[i].is_leader())
        .expect("a leader elected")
}

fn upsert(node: u64, status: NodeStatus) -> MetaCommand {
    MetaCommand::UpsertMember {
        node,
        labels: BTreeMap::new(),
        status,
    }
}

/// Propose `command` on whichever node is currently leader and let the
/// cluster (and the mirror's own 50ms poll) settle before returning.
fn propose_and_settle(nodes: &[RaftNode<SimEnv>], sim: &mut Simulator, command: MetaCommand) {
    let l = leader(nodes);
    nodes[l].propose(command);
    sim.run_for(Duration::from_millis(300));
}

/// Drive a representative mix of every `MetaCommand` family through a real
/// 3-node control group with the mirror attached on node 0: membership,
/// tablet create/split/merge (epoch-CAS), schema DDL (create/index/mode/
/// drop), keyspace lifecycle, the legacy CP-addr registration (and its
/// merge-triggered prune), node-id allocation, and member removal. Asserts
/// the mirror agrees with node 0's own in-core `Metadata` after every step,
/// not just at the end — a divergence introduced by one command family
/// should fail at the step that caused it.
fn run_scenario(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let metrics0 = sim.env(0).metrics();
    let nodes: Vec<RaftNode<SimEnv>> = vec![
        RaftNode::start_with_mirror(sim.env(0), NODES.to_vec(), metrics0, engine.clone()),
        RaftNode::start(sim.env(1), NODES.to_vec()),
        RaftNode::start(sim.env(2), NODES.to_vec()),
    ];
    sim.run_for(Duration::from_secs(2));
    assert_eq!(leader(&nodes), leader(&nodes), "a stable leader exists");

    let assert_agrees = |nodes: &[RaftNode<SimEnv>], step: &str| {
        let rebuilt =
            futures::executor::block_on(rebuild_metadata_from_engine(&engine)).expect("rebuild");
        assert_eq!(
            rebuilt,
            nodes[0].metadata(),
            "mirror diverged from node 0's in-core Metadata after {step} (seed={seed:#x})"
        );
    };

    // Membership.
    propose_and_settle(&nodes, &mut sim, upsert(10, NodeStatus::Active));
    propose_and_settle(&nodes, &mut sim, upsert(20, NodeStatus::Active));
    assert_agrees(&nodes, "membership upserts");

    // Schema DDL.
    propose_and_settle(
        &nodes,
        &mut sim,
        MetaCommand::CreateTableSchema {
            table: TABLE.to_string(),
            schema: TableSchema::simple("id", ColumnType::String),
        },
    );
    propose_and_settle(
        &nodes,
        &mut sim,
        MetaCommand::CreateTableIndex {
            table: TABLE.to_string(),
            index: IndexDef {
                name: "by_status".to_string(),
                kind: IndexKind::Global,
                hash_attribute: "id".to_string(),
                sort_attribute: None,
                projection: IndexProjection::All,
            },
        },
    );
    propose_and_settle(
        &nodes,
        &mut sim,
        MetaCommand::SetTableMode {
            table: TABLE.to_string(),
            mode: ReplicationMode::Cp,
        },
    );
    assert_agrees(&nodes, "schema DDL");

    // Tablet lifecycle: create, policy, split (epoch-CAS), legacy CP-addr
    // registration, then merge back (exercising the merge-triggered prune of
    // that same legacy registration).
    propose_and_settle(
        &nodes,
        &mut sim,
        MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some(TABLE.to_string()),
            range: KeyRange::whole(),
            replicas: vec![10, 20],
        },
    );
    propose_and_settle(
        &nodes,
        &mut sim,
        MetaCommand::SetTabletPolicy {
            tablet: TabletId(1),
            policy: Some(PlacementPolicy::simple("p", 2)),
        },
    );
    propose_and_settle(
        &nodes,
        &mut sim,
        MetaCommand::RegisterCpAddr {
            id: 500,
            addr: "127.0.0.1:1".to_string(),
            tablet: Some(TabletId(1)),
        },
    );
    assert_agrees(&nodes, "tablet create + policy + legacy cp-addr");

    let split_epoch = nodes[0].metadata().tablets[&TabletId(1)].epoch;
    propose_and_settle(
        &nodes,
        &mut sim,
        MetaCommand::SplitTablet {
            tablet: TabletId(1),
            expected_epoch: split_epoch,
            split_key: vec![128],
            new_id: TabletId(2),
        },
    );
    assert_agrees(&nodes, "split");

    let left_epoch = nodes[0].metadata().tablets[&TabletId(1)].epoch;
    let right_epoch = nodes[0].metadata().tablets[&TabletId(2)].epoch;
    propose_and_settle(
        &nodes,
        &mut sim,
        MetaCommand::MergeTablets {
            left: TabletId(1),
            expected_left_epoch: left_epoch,
            right: TabletId(2),
            expected_right_epoch: right_epoch,
        },
    );
    assert_agrees(&nodes, "merge (+ legacy cp-addr prune)");

    // Keyspace lifecycle.
    propose_and_settle(
        &nodes,
        &mut sim,
        MetaCommand::CreateKeyspace {
            keyspace: "ks1".to_string(),
        },
    );
    assert_agrees(&nodes, "create keyspace");

    // Node-id allocation (ADR 0036/0038's `AllocateNodeId`).
    propose_and_settle(
        &nodes,
        &mut sim,
        MetaCommand::AllocateNodeId {
            nonce: "join-1".to_string(),
            labels: BTreeMap::new(),
        },
    );
    assert_agrees(&nodes, "allocate node id");

    // Drop-table GC + schema/keyspace teardown + member removal.
    propose_and_settle(
        &nodes,
        &mut sim,
        MetaCommand::DropTableTablets {
            table: TABLE.to_string(),
        },
    );
    propose_and_settle(
        &nodes,
        &mut sim,
        MetaCommand::DropTableSchema {
            table: TABLE.to_string(),
        },
    );
    propose_and_settle(
        &nodes,
        &mut sim,
        MetaCommand::DropKeyspace {
            keyspace: "ks1".to_string(),
        },
    );
    propose_and_settle(&nodes, &mut sim, upsert(20, NodeStatus::Leaving));
    propose_and_settle(&nodes, &mut sim, MetaCommand::RemoveMember { node: 20 });
    assert_agrees(&nodes, "drop-table GC + member removal");

    // Final settle + check, mirroring how a real driver's mirror loop would
    // lag slightly behind bursty proposing.
    sim.run_for(Duration::from_secs(1));
    assert_agrees(&nodes, "final settle");
}

#[test]
fn differential_oracle_matches_in_core_metadata_across_seeds() {
    // A small seed corpus (ADR 0038 PR2 plan: "seed-swept over a small
    // corpus incl. splits/merges/schema DDL/member changes/allocations").
    for seed in [0x1u64, 0x2A, 0xC0FFEE, 777, 99_999] {
        run_scenario(seed);
    }
}

/// Crash-recovery: kill the mirror-attached node mid-cluster-life (its
/// volatile `RaftCore` state and `mirror_capture` flag are wiped; its WAL and
/// the mirror's own engine both survive — the engine because the test holds
/// the same `MemoryEngine` handle across the restart, exactly like a real
/// `LsmEngine::open` recovering durable on-disk state), then restart it on
/// the same node id. Once it rejoins and its mirror rebuilds its shadow cache
/// from the engine (`mirror_loop`'s startup step) and replays the post-crash
/// tail, the engine's content is again byte-identical to the (now caught-up)
/// in-core `Metadata`, and the `_applied_index` watermark has advanced past
/// its pre-crash value.
#[test]
fn mirror_survives_a_crash_and_resumes_from_the_persisted_watermark() {
    let seed = 0xABCD_u64;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let metrics0 = sim.env(0).metrics();
    let mut nodes: Vec<RaftNode<SimEnv>> = vec![
        RaftNode::start_with_mirror(sim.env(0), NODES.to_vec(), metrics0, engine.clone()),
        RaftNode::start(sim.env(1), NODES.to_vec()),
        RaftNode::start(sim.env(2), NODES.to_vec()),
    ];
    sim.run_for(Duration::from_secs(2));

    for id in 0..5 {
        propose_and_settle(&nodes, &mut sim, upsert(id, NodeStatus::Active));
    }
    sim.run_for(Duration::from_secs(1));

    let watermark_before = read_watermark(&engine);
    assert!(
        watermark_before > 0,
        "watermark should have advanced pre-crash"
    );
    let rebuilt_before =
        futures::executor::block_on(rebuild_metadata_from_engine(&engine)).expect("rebuild");
    assert_eq!(
        rebuilt_before,
        nodes[0].metadata(),
        "mirror agrees pre-crash"
    );

    // Crash node 0.
    sim.stop(0);

    // The surviving majority keeps committing while node 0 is down.
    for id in 5..9 {
        propose_and_settle(&nodes, &mut sim, upsert(id, NodeStatus::Active));
    }
    sim.run_for(Duration::from_secs(1));

    // Restart node 0 on the same id/disk, re-attaching the *same* (durable)
    // mirror engine handle.
    let metrics0b = sim.env(0).metrics();
    nodes[0] = RaftNode::start_with_mirror(sim.env(0), NODES.to_vec(), metrics0b, engine.clone());
    sim.run_for(Duration::from_secs(3));

    let reference: Metadata = nodes[0].metadata();
    assert_eq!(
        reference.members.len(),
        9,
        "all writes present after rejoin"
    );
    let rebuilt_after =
        futures::executor::block_on(rebuild_metadata_from_engine(&engine)).expect("rebuild");
    assert_eq!(
        rebuilt_after, reference,
        "post-restart mirror diverged from in-core Metadata (seed={seed:#x})"
    );

    let watermark_after = read_watermark(&engine);
    assert!(
        watermark_after > watermark_before,
        "watermark should advance past the crash (before={watermark_before}, after={watermark_after})"
    );
}

fn read_watermark(engine: &MemoryEngine) -> u64 {
    use animus_control::syskv::applied_index_key;
    use animus_storage::StorageEngine;

    futures::executor::block_on(engine.get(&applied_index_key()))
        .expect("engine read")
        .map(|v| {
            let bytes: [u8; 8] = v.value.try_into().expect("watermark is 8 bytes");
            u64::from_be_bytes(bytes)
        })
        .unwrap_or(0)
}
