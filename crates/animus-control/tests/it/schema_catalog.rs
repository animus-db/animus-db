//! Replicated table-schema catalog, end to end through real Raft (ADR 0013).
//!
//! Both wire adapters previously kept table schemas in per-process, in-memory
//! catalogs, so a `CreateTable` / `CREATE TABLE` neither survived a restart nor
//! replicated. This test exercises the control-plane substrate that fixes it:
//! schemas live in [`Metadata`] and are mutated by replicated
//! [`MetaCommand`]s. Under `SimEnv`, a 3-node control group:
//!
//! 1. proposes two table schemas (one simple, one composite) on the leader and
//!    sees them committed and replicated to every follower;
//! 2. rejects a duplicate `CreateTableSchema` and a malformed one
//!    (deterministic, on the state machine);
//! 3. **kills the leader** and asserts the schemas survive on the survivors,
//!    which agree on identical catalogs (replicated + durable through the WAL);
//! 4. drops a schema and sees the drop replicate.
//!
//! The whole run is a pure function of its seed.

use std::time::Duration;

use animus_control::raft::ProposeResult;
use animus_control::{ColumnDef, ColumnType, MetaCommand, RaftNode, TableSchema};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];

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

/// A DynamoDB-style simple (hash-only) table.
fn users_schema() -> TableSchema {
    TableSchema::simple("id", ColumnType::Uuid)
}

/// A table with a partition key, one clustering key, and extra columns —
/// the general (wider-than-DynamoDB) shape the catalog still supports.
fn events_schema() -> TableSchema {
    TableSchema::with_columns(
        "device",
        vec!["ts".into()],
        vec![
            ColumnDef::new("device", ColumnType::String),
            ColumnDef::new("ts", ColumnType::BigInt),
            ColumnDef::new("payload", ColumnType::Binary),
        ],
    )
}

#[test]
fn schema_replicates_survives_leader_kill_and_reproduces() {
    run(0x5C4E_3A11);
}

fn run(seed: u64) {
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, &[0, 1, 2], seed);

    // 1. Propose two schemas; both are accepted.
    for cmd in [
        MetaCommand::CreateTableSchema {
            table: "users".into(),
            schema: users_schema(),
        },
        MetaCommand::CreateTableSchema {
            table: "events".into(),
            schema: events_schema(),
        },
    ] {
        assert!(
            matches!(nodes[leader].propose(cmd), ProposeResult::Accepted { .. }),
            "schema proposal rejected (seed={seed})"
        );
    }
    sim.run_for(Duration::from_secs(1));

    // It is committed and replicated to every node, byte-identically.
    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        assert_eq!(
            m.table_schema("users"),
            Some(&users_schema()),
            "node {i}: users schema missing/wrong (seed={seed})"
        );
        assert_eq!(
            m.table_schema("events"),
            Some(&events_schema()),
            "node {i}: events schema missing/wrong (seed={seed})"
        );
        assert!(m.has_table_schema("users"));
        assert_eq!(
            m.table_schemas().count(),
            2,
            "node {i}: unexpected catalog size (seed={seed})"
        );
    }

    // 2. A duplicate create is rejected by the state machine (accepted into the
    //    log, but applies as Rejected — so it does not overwrite). A malformed
    //    schema is likewise rejected.
    let before = nodes[leader].metadata().table_schema("users").cloned();
    nodes[leader].propose(MetaCommand::CreateTableSchema {
        table: "users".into(),
        // A different schema, to prove no silent overwrite.
        schema: TableSchema::simple("other", ColumnType::Int),
    });
    nodes[leader].propose(MetaCommand::CreateTableSchema {
        table: "broken".into(),
        // Partition key names a column that does not exist: malformed.
        schema: TableSchema::with_columns(
            "nope",
            Vec::new(),
            vec![ColumnDef::new("a", ColumnType::Int)],
        ),
    });
    sim.run_for(Duration::from_secs(1));
    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        assert_eq!(
            m.table_schema("users").cloned(),
            before,
            "node {i}: duplicate create overwrote the schema (seed={seed})"
        );
        assert!(
            !m.has_table_schema("broken"),
            "node {i}: malformed schema was recorded (seed={seed})"
        );
    }

    // 3. Kill the leader; survivors re-elect and must still hold the schemas.
    sim.crash(nid(leader as u64));
    sim.run_for(Duration::from_secs(3));
    let survivors: Vec<usize> = (0..3).filter(|&i| i != leader).collect();
    let new_leader = unique_leader(&nodes, &survivors, seed);
    assert!(survivors.contains(&new_leader));

    let a = nodes[survivors[0]].metadata();
    let b = nodes[survivors[1]].metadata();
    assert_eq!(
        a.schemas, b.schemas,
        "survivor catalogs diverged after leader kill (seed={seed})"
    );
    assert_eq!(
        a.table_schema("users"),
        Some(&users_schema()),
        "users schema lost across leader kill (seed={seed})"
    );
    assert_eq!(
        a.table_schema("events"),
        Some(&events_schema()),
        "events schema lost across leader kill (seed={seed})"
    );

    // 4. Drop a schema on the new leader; the drop replicates.
    assert!(matches!(
        nodes[new_leader].propose(MetaCommand::DropTableSchema {
            table: "users".into(),
        }),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(1));
    for &i in &survivors {
        let m = nodes[i].metadata();
        assert!(
            !m.has_table_schema("users"),
            "node {i}: dropped schema still present (seed={seed})"
        );
        assert!(
            m.has_table_schema("events"),
            "node {i}: drop removed the wrong schema (seed={seed})"
        );
    }
}

#[test]
fn schema_catalog_is_reproducible_from_seed() {
    fn trace(seed: u64) -> Vec<String> {
        let (mut sim, nodes) = cluster(seed);
        sim.run_for(Duration::from_secs(2));
        let leader = unique_leader(&nodes, &[0, 1, 2], seed);
        nodes[leader].propose(MetaCommand::CreateTableSchema {
            table: "users".into(),
            schema: users_schema(),
        });
        nodes[leader].propose(MetaCommand::CreateTableSchema {
            table: "events".into(),
            schema: events_schema(),
        });
        sim.run_for(Duration::from_secs(1));
        sim.crash(nid(leader as u64));
        sim.run_for(Duration::from_secs(3));
        sim.trace_lines()
    }
    assert_eq!(trace(0x5C4E_5EED), trace(0x5C4E_5EED));
}
