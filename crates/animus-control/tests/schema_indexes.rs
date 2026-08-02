//! Replicated **secondary-index definitions** (GSI/LSI), end to end through real
//! Raft (ADR 0013).
//!
//! DynamoDB GSI/LSI *definitions* used to live in the wire edge's process-local
//! registry, so index existence was neither agreed cluster-wide nor durable. This
//! test exercises the control-plane substrate that fixes it: an index definition
//! is added to a table's replicated [`TableSchema`] via
//! [`MetaCommand::CreateTableIndex`] and removed via [`MetaCommand::DropTableIndex`],
//! so it is Raft-replicated, durable, and consistent on every node. Under
//! `SimEnv`, a 3-node control group:
//!
//! 1. creates a table and then a GSI on it; both replicate so a **second node**
//!    sees the index definition;
//! 2. rejects an index on a non-existent table and a malformed (LSI-without-sort)
//!    index, deterministically on the state machine;
//! 3. **restarts a node** (its volatile state dies, its WAL survives) and asserts
//!    the index definition is recovered from the replicated catalog, not local
//!    memory;
//! 4. drops the index on the leader and sees it disappear cluster-wide.
//!
//! The whole run is a pure function of its seed.

use std::time::Duration;

use animus_control::ColumnType;
use animus_control::raft::ProposeResult;
use animus_control::{IndexDef, IndexKind, IndexProjection, MetaCommand, RaftNode, TableSchema};
use animus_sim::{SimEnv, Simulator};

const NODES: [u64; 3] = [0, 1, 2];

fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| RaftNode::start(sim.env(id), NODES.to_vec()))
        .collect();
    (sim, nodes)
}

fn unique_leader(nodes: &[RaftNode<SimEnv>], live: &[usize], seed: u64) -> usize {
    let leaders: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        leaders.len(),
        1,
        "expected exactly one leader among {live:?}, found {leaders:?} (seed={seed})"
    );
    leaders[0]
}

/// A simple (hash-only) base table.
fn users_schema() -> TableSchema {
    TableSchema::simple("id", ColumnType::String)
}

/// A global secondary index keyed by `email`.
fn email_index() -> IndexDef {
    IndexDef {
        name: "by-email".into(),
        kind: IndexKind::Global,
        hash_attribute: "email".into(),
        sort_attribute: None,
        projection: IndexProjection::All,
    }
}

#[test]
fn index_definition_replicates_survives_restart_and_drops() {
    run(0x1DE7_0011);
}

fn run(seed: u64) {
    let (mut sim, mut nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, &[0, 1, 2], seed);

    // 1. Create the base table, then add a GSI to it. Both replicate.
    assert!(matches!(
        nodes[leader].propose(MetaCommand::CreateTableSchema {
            table: "users".into(),
            schema: users_schema(),
        }),
        ProposeResult::Accepted { .. }
    ));
    assert!(matches!(
        nodes[leader].propose(MetaCommand::CreateTableIndex {
            table: "users".into(),
            index: email_index(),
        }),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(1));

    // The index definition is committed and replicated to every node — a SECOND
    // node sees it, agreed cluster-wide.
    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        let idxs = m.table_indexes("users");
        assert_eq!(
            idxs,
            &[email_index()],
            "node {i}: index definition missing/wrong (seed={seed})"
        );
        assert_eq!(
            m.table_schema("users").map(|s| s.indexes.as_slice()),
            Some([email_index()].as_slice()),
            "node {i}: index not on the table schema (seed={seed})"
        );
    }

    // 2. An index on a non-existent table is rejected; a malformed index (LSI
    //    with no sort attribute) is rejected on the state machine — neither
    //    changes state.
    nodes[leader].propose(MetaCommand::CreateTableIndex {
        table: "ghost".into(),
        index: email_index(),
    });
    nodes[leader].propose(MetaCommand::CreateTableIndex {
        table: "users".into(),
        index: IndexDef {
            name: "bad-lsi".into(),
            kind: IndexKind::Local,
            hash_attribute: "id".into(),
            sort_attribute: None, // an LSI must have a sort attribute
            projection: IndexProjection::All,
        },
    });
    sim.run_for(Duration::from_secs(1));
    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        assert!(
            !m.has_table_schema("ghost"),
            "node {i}: index created a phantom table (seed={seed})"
        );
        assert_eq!(
            m.table_indexes("users"),
            &[email_index()],
            "node {i}: a rejected index leaked into state (seed={seed})"
        );
    }

    // 3. Restart a follower: its volatile state dies, its WAL survives. The index
    //    definition must come back from the replicated catalog, not local memory.
    let follower = (0..3).find(|&i| i != leader).unwrap();
    sim.stop(follower as u64);
    sim.run_for(Duration::from_secs(1));
    nodes[follower] = RaftNode::start(sim.env(follower as u64), NODES.to_vec());
    sim.run_for(Duration::from_secs(3));
    assert_eq!(
        nodes[follower].metadata().table_indexes("users"),
        &[email_index()],
        "restarted node lost the index definition (seed={seed})"
    );

    // 4. Drop the index on the (possibly new) leader; the drop replicates.
    let leader = unique_leader(&nodes, &[0, 1, 2], seed);
    assert!(matches!(
        nodes[leader].propose(MetaCommand::DropTableIndex {
            table: "users".into(),
            index: "by-email".into(),
        }),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(1));
    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        assert!(
            m.table_indexes("users").is_empty(),
            "node {i}: dropped index still present (seed={seed})"
        );
        // The base table schema itself survives the index drop.
        assert!(
            m.has_table_schema("users"),
            "node {i}: index drop removed the table schema (seed={seed})"
        );
    }
}

#[test]
fn index_catalog_is_reproducible_from_seed() {
    fn trace(seed: u64) -> Vec<String> {
        let (mut sim, nodes) = cluster(seed);
        sim.run_for(Duration::from_secs(2));
        let leader = unique_leader(&nodes, &[0, 1, 2], seed);
        nodes[leader].propose(MetaCommand::CreateTableSchema {
            table: "users".into(),
            schema: users_schema(),
        });
        nodes[leader].propose(MetaCommand::CreateTableIndex {
            table: "users".into(),
            index: email_index(),
        });
        sim.run_for(Duration::from_secs(1));
        sim.crash(leader as u64);
        sim.run_for(Duration::from_secs(3));
        sim.trace_lines()
    }
    assert_eq!(trace(0x1DE7_5EED), trace(0x1DE7_5EED));
}
