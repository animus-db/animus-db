//! The replicated backup catalog, end to end through real Raft (ADR 0059
//! §3). Mirrors `schema_catalog.rs`'s and `restart.rs`'s own conventions for
//! this crate's catalog-shaped `MetaCommand` families:
//!
//! 1. propose `BeginBackup` on the leader of a 3-node control group and see
//!    it committed and replicated to every follower, with a manifest stub
//!    derived from the table's own current schema/tablet list;
//! 2. drive a pinned tablet's completion report through to `CompleteBackup`;
//! 3. **kill the leader** and assert the catalog survives on the survivors;
//! 4. drop the source table on the new leader and assert the backup row (and
//!    its progress record) survive — ADR 0024/ADR 0059 §3's explicit
//!    carve-out;
//! 5. `DeleteBackup` on the new leader and see the removal replicate;
//! 6. a real node restart (WAL + system-keyspace engine recovery, ADR 0038)
//!    recovers the catalog exactly like every other `Metadata` collection.
//!
//! Every run is a pure function of its seed.

use std::time::Duration;

use animus_control::raft::ProposeResult;
use animus_control::{BackupStatus, ColumnType, MetaCommand, RaftNode, TableSchema};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, TabletId};

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

fn propose_accepted(node: &RaftNode<SimEnv>, command: MetaCommand, what: &str, seed: u64) {
    assert!(
        matches!(node.propose(command), ProposeResult::Accepted { .. }),
        "{what} rejected (seed={seed})"
    );
}

#[test]
fn backup_catalog_replicates_survives_leader_kill_and_table_drop() {
    run(0xB4C4_0001);
}

fn run(seed: u64) {
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, &[0, 1, 2], seed);

    // Provision the table this backup will pin.
    propose_accepted(
        &nodes[leader],
        MetaCommand::CreateTableSchema {
            table: "orders".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        },
        "CreateTableSchema",
        seed,
    );
    propose_accepted(
        &nodes[leader],
        MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("orders".into()),
            range: KeyRange::whole(),
            replicas: vec![nid(0), nid(1), nid(2)],
        },
        "CreateTablet",
        seed,
    );
    sim.run_for(Duration::from_secs(1));

    // 1. `BeginBackup` — the manifest stub is derived from already-agreed
    //    state, so every node computes the identical row.
    propose_accepted(
        &nodes[leader],
        MetaCommand::BeginBackup {
            backup_id: "backup-1".into(),
            table: "orders".into(),
            created_wall_ms: 1_000,
        },
        "BeginBackup",
        seed,
    );
    sim.run_for(Duration::from_secs(1));
    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        let row = m
            .backup("backup-1")
            .unwrap_or_else(|| panic!("node {i}: backup row missing (seed={seed})"));
        assert_eq!(row.table, "orders");
        assert_eq!(row.status, BackupStatus::Creating);
        assert_eq!(row.manifest.pinned_tablets.len(), 1);
        assert_eq!(row.manifest.pinned_tablets[0].tablet, TabletId(1));
    }

    // 2. The pinned tablet reports completion; the aggregator (here, the
    //    test itself, standing in for the real leader-only loop) completes
    //    the backup.
    propose_accepted(
        &nodes[leader],
        MetaCommand::RecordBackupTabletComplete {
            backup_id: "backup-1".into(),
            tablet: TabletId(1),
            cut_version: 42,
            bytes: 4_096,
        },
        "RecordBackupTabletComplete",
        seed,
    );
    propose_accepted(
        &nodes[leader],
        MetaCommand::CompleteBackup {
            backup_id: "backup-1".into(),
        },
        "CompleteBackup",
        seed,
    );
    sim.run_for(Duration::from_secs(1));
    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        assert_eq!(
            m.backup("backup-1").unwrap().status,
            BackupStatus::Available,
            "node {i}: backup did not complete (seed={seed})"
        );
        assert_eq!(m.backup_total_bytes("backup-1"), 4_096);
    }

    // 3. Kill the leader; survivors re-elect and must still hold the catalog.
    sim.crash(nid(leader as u64));
    sim.run_for(Duration::from_secs(3));
    let survivors: Vec<usize> = (0..3).filter(|&i| i != leader).collect();
    let new_leader = unique_leader(&nodes, &survivors, seed);
    assert!(survivors.contains(&new_leader));

    let a = nodes[survivors[0]].metadata();
    let b = nodes[survivors[1]].metadata();
    assert_eq!(
        a.backups, b.backups,
        "survivor backup catalogs diverged after leader kill (seed={seed})"
    );
    assert_eq!(
        a.backup_tablet_progress, b.backup_tablet_progress,
        "survivor backup progress diverged after leader kill (seed={seed})"
    );
    assert_eq!(
        a.backup("backup-1").unwrap().status,
        BackupStatus::Available
    );

    // 4. Drop the source table on the new leader — ADR 0024/ADR 0059 §3's
    //    explicit carve-out: the backup row (and its progress record) must
    //    survive, untouched.
    propose_accepted(
        &nodes[new_leader],
        MetaCommand::DropTableTablets {
            table: "orders".into(),
        },
        "DropTableTablets",
        seed,
    );
    propose_accepted(
        &nodes[new_leader],
        MetaCommand::DropTableSchema {
            table: "orders".into(),
        },
        "DropTableSchema",
        seed,
    );
    sim.run_for(Duration::from_secs(1));
    for &i in &survivors {
        let m = nodes[i].metadata();
        assert!(!m.has_table_schema("orders"), "node {i}: table not dropped");
        assert!(
            !m.has_table_tablet("orders"),
            "node {i}: tablet not dropped"
        );
        let row = m.backup("backup-1").unwrap_or_else(|| {
            panic!("node {i}: backup row was removed by the table drop (seed={seed})")
        });
        assert_eq!(row.status, BackupStatus::Available);
        assert_eq!(row.table, "orders");
        assert_eq!(
            m.backup_tablet_progress
                .get(&("backup-1".to_string(), TabletId(1))),
            Some(&animus_control::BackupTabletProgress {
                cut_version: 42,
                bytes: 4_096,
            }),
            "node {i}: progress record was removed by the table drop (seed={seed})"
        );
    }

    // 5. `DeleteBackup` on the new leader — the removal replicates.
    propose_accepted(
        &nodes[new_leader],
        MetaCommand::DeleteBackup {
            backup_id: "backup-1".into(),
        },
        "DeleteBackup",
        seed,
    );
    sim.run_for(Duration::from_secs(1));
    for &i in &survivors {
        let m = nodes[i].metadata();
        assert!(
            m.backup("backup-1").is_none(),
            "node {i}: backup row survived DeleteBackup (seed={seed})"
        );
        assert!(
            m.backup_tablet_progress_for("backup-1").next().is_none(),
            "node {i}: progress record survived DeleteBackup (seed={seed})"
        );
    }
}

#[test]
fn backup_catalog_is_reproducible_from_seed() {
    fn trace(seed: u64) -> Vec<String> {
        let (mut sim, nodes) = cluster(seed);
        sim.run_for(Duration::from_secs(2));
        let leader = unique_leader(&nodes, &[0, 1, 2], seed);
        nodes[leader].propose(MetaCommand::CreateTableSchema {
            table: "orders".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        });
        nodes[leader].propose(MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("orders".into()),
            range: KeyRange::whole(),
            replicas: vec![nid(0), nid(1), nid(2)],
        });
        sim.run_for(Duration::from_secs(1));
        nodes[leader].propose(MetaCommand::BeginBackup {
            backup_id: "backup-1".into(),
            table: "orders".into(),
            created_wall_ms: 1_000,
        });
        sim.run_for(Duration::from_secs(1));
        sim.crash(nid(leader as u64));
        sim.run_for(Duration::from_secs(3));
        sim.trace_lines()
    }
    assert_eq!(trace(0xB4C4_5EED), trace(0xB4C4_5EED));
}

/// WAL/snapshot recovery (ADR 0038): a node whose process stops (tasks and
/// volatile state gone, its durable system-keyspace engine kept) and
/// restarts on the same disk recovers the backup catalog exactly like every
/// other `Metadata` collection — mirroring `restart.rs`'s own pattern.
#[test]
fn backup_catalog_survives_node_restart() {
    let seed = 0xB4C4_5715;
    let mut sim = Simulator::new(seed);
    // `MemoryEngine` clones share state (a real node's on-disk engine
    // surviving a process restart) — re-cloning the *same* handle at restart
    // is what exercises genuine durable recovery, not a fresh empty engine.
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
    let leader = unique_leader(&nodes, &[0, 1, 2], seed);

    propose_accepted(
        &nodes[leader],
        MetaCommand::CreateTableSchema {
            table: "orders".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        },
        "CreateTableSchema",
        seed,
    );
    propose_accepted(
        &nodes[leader],
        MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("orders".into()),
            range: KeyRange::whole(),
            replicas: vec![nid(0), nid(1), nid(2)],
        },
        "CreateTablet",
        seed,
    );
    propose_accepted(
        &nodes[leader],
        MetaCommand::BeginBackup {
            backup_id: "backup-1".into(),
            table: "orders".into(),
            created_wall_ms: 1_000,
        },
        "BeginBackup",
        seed,
    );
    sim.run_for(Duration::from_secs(2));
    let follower = (0..3).find(|&i| i != leader).unwrap();
    assert!(
        nodes[follower].metadata().backup("backup-1").is_some(),
        "follower has the pre-stop backup row"
    );

    // Stop the follower's process; the surviving majority keeps committing
    // while it is down.
    sim.stop(nid(follower as u64));
    propose_accepted(
        &nodes[leader],
        MetaCommand::RecordBackupTabletComplete {
            backup_id: "backup-1".into(),
            tablet: TabletId(1),
            cut_version: 42,
            bytes: 4_096,
        },
        "RecordBackupTabletComplete",
        seed,
    );
    propose_accepted(
        &nodes[leader],
        MetaCommand::CompleteBackup {
            backup_id: "backup-1".into(),
        },
        "CompleteBackup",
        seed,
    );
    sim.run_for(Duration::from_secs(2));

    // Restart the stopped node on the same disk — it recovers from the WAL
    // and the durable system-keyspace engine, exactly like a real restart.
    nodes[follower] = RaftNode::start(
        sim.env(nid(follower as u64)),
        NODES.iter().copied().map(nid).collect(),
        engines[follower].clone(),
    );
    sim.run_for(Duration::from_secs(3));

    let reference = nodes[leader].metadata();
    assert_eq!(
        reference.backup("backup-1").unwrap().status,
        BackupStatus::Available
    );
    for (i, n) in nodes.iter().enumerate() {
        let m = n.metadata();
        assert_eq!(
            m.backups, reference.backups,
            "node {i} backup catalog diverged after restart (seed={seed})"
        );
        assert_eq!(
            m.backup_tablet_progress, reference.backup_tablet_progress,
            "node {i} backup progress diverged after restart (seed={seed})"
        );
    }
}
