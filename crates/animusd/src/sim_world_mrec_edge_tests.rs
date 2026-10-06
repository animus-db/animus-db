//! G-d M4c: replica-lifecycle edge cases over the two-cluster `SimWorld`
//! (the M4b `S` harness): TTL propagation, Delete while Creating, saga driver
//! failover, a restoring table, the CreationFailed retry, and the gate-closed
//! case that proves the `MrecReplication` check specifically (the cluster
//! is finalized to version 2, so the G-c gate is open and only the MREC gate
//! is closed). Each test runs at >= 20 seeds (`saga_seeds`).

use animus_control::MetaCommand;
use animus_control::schema::{ColumnType, TableSchema};
use animus_tablet::TabletId;

use super::sim_world_mrec_saga_tests::{S, create_body, delete_body, saga_seeds};
use super::sim_world_mrec_tests::{A, B, TABLE};

fn ttl_body(enabled: bool) -> String {
    format!(
        r#"{{"TableName":"{TABLE}","TimeToLiveSpecification":{{"Enabled":{enabled},"AttributeName":"ttl"}}}}"#
    )
}

fn ttl_enabled(s: &mut S, c: usize) -> bool {
    let (st, r) = s.call(
        c,
        "DescribeTimeToLive",
        &format!(r#"{{"TableName":"{TABLE}"}}"#),
    );
    st == 200 && r.contains("\"ENABLED\"")
}

fn lowest_tablet(s: &mut S, c: usize) -> TabletId {
    let n = s.up(c);
    s.w.clusters[c]
        .metadata(n)
        .tablets_for_table(TABLE)
        .map(|(id, _)| *id)
        .min()
        .expect("a tablet")
}

#[test]
fn ttl_changes_propagate_to_every_replica_both_ways() {
    for seed in saga_seeds() {
        let mut s = S::new(seed, true);
        s.make_active(3);
        // A few passes so each driver records its baseline first.
        for _ in 0..3 {
            s.step_both();
        }
        s.ok(A, "UpdateTimeToLive", &ttl_body(true));
        s.until("TTL enabled on B", |s| ttl_enabled(s, B));
        s.ok(A, "UpdateTimeToLive", &ttl_body(false));
        s.until("TTL disabled on B", |s| !ttl_enabled(s, B));
        // And from the other side.
        s.ok(B, "UpdateTimeToLive", &ttl_body(true));
        s.until("TTL enabled on A", |s| ttl_enabled(s, A));
        s.ok(B, "UpdateTimeToLive", &ttl_body(false));
        s.until("TTL disabled on A", |s| !ttl_enabled(s, A));
    }
}

#[test]
fn delete_while_creating_settles_standalone_and_a_new_create_works() {
    for seed in saga_seeds() {
        let mut s = S::new(seed, true);
        for i in 0..4 {
            s.put(A, &format!("pre{i}"), "old");
        }
        s.ok(A, "UpdateTable", &create_body("b"));
        // A seed-dependent number of passes: the Delete lands before, during
        // or after the copy.
        for _ in 0..(seed % 3) {
            s.step_both();
        }
        s.ok(A, "UpdateTable", &delete_body("b"));
        s.until("A standalone again", |s| s.spec_replicas(A).len() <= 1);
        // The peer, if it adopted the table, is standalone too (never stuck).
        s.until("B holds no replica list", |s| s.replicas(B).len() <= 1);
        // A fresh Create works afterwards and converges.
        s.ok(A, "UpdateTable", &create_body("b"));
        s.until("both ACTIVE after the re-create", |s| s.both_active());
        s.put(A, "after", "x");
        s.until("post re-create write on B", |s| {
            s.read(B, "after").as_deref() == Some("x")
        });
    }
}

#[test]
fn the_saga_survives_a_driver_failover_mid_create() {
    for seed in saga_seeds() {
        let mut s = S::new(seed, true);
        for i in 0..24 {
            s.put(A, &format!("pre{i:02}"), "old");
        }
        s.ok(A, "UpdateTable", &create_body("b"));
        for _ in 0..(seed % 2) {
            s.step(A);
        }
        // Kill the leader of the lowest tablet: it is the saga's driver.
        let tablet = lowest_tablet(&mut s, A);
        let victim = s.w.clusters[A].leader_index_of(tablet).expect("a leader");
        s.crash(A, victim);
        s.until("both ACTIVE despite the failover", |s| s.both_active());
        for i in 0..24 {
            assert_eq!(
                s.read(B, &format!("pre{i:02}")).as_deref(),
                Some("old"),
                "seed={seed}: row {i} missing on B after the failover"
            );
        }
    }
}

#[test]
fn a_restoring_table_cannot_become_a_global_table() {
    for seed in saga_seeds() {
        let mut s = S::new(seed, true);
        let c = &mut s.w.clusters[A];
        let ok = |r: animus_control::ProposeResult| {
            matches!(r, animus_control::ProposeResult::Accepted { .. })
        };
        assert!(ok(c.propose_meta(MetaCommand::CreateTableSchema {
            table: "rst".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        })));
        c.run_for(std::time::Duration::from_millis(300));
        let tablet = c.metadata(0).next_free_tablet_id();
        assert!(ok(c.propose_meta(MetaCommand::BeginRestore {
            restore_id: "r1".into(),
            backup_id: "b1".into(),
            source_table: "src".into(),
            target_table: "rst".into(),
            tablet,
            replicas: vec![animus_env::nid(0)],
            gsi_defs: Vec::new(),
            pitr: None,
        })));
        c.run_for(std::time::Duration::from_millis(300));
        let body = r#"{"TableName":"rst","ReplicaUpdates":[{"Create":{"RegionName":"b"}}]}"#;
        let (st, r) = s.call(A, "UpdateTable", body);
        assert_eq!(st, 400, "seed={seed}: {r}");
        assert!(r.contains("not ACTIVE"), "seed={seed}: {r}");
        assert!(
            s.w.clusters[A].metadata(0).table_global("rst").is_none(),
            "seed={seed}: the restoring table was converted"
        );
    }
}

#[test]
fn a_new_create_after_creation_failed_works() {
    for seed in saga_seeds() {
        let mut s = S::new(seed, true);
        for i in 0..3 {
            s.put(A, &format!("pre{i}"), "old");
        }
        // B already holds `tbl` with the same keys but an extra index (a shape
        // the saga refuses for good). A different key schema would also do, but
        // a delete + re-create with another key schema trips a pre-existing
        // registry staleness bug (see the M4c notes), so keep the keys equal.
        let bad = format!(
            r#"{{"TableName":"{TABLE}","AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}},{{"AttributeName":"sk","AttributeType":"S"}},{{"AttributeName":"g","AttributeType":"S"}}],"KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}},{{"AttributeName":"sk","KeyType":"RANGE"}}],"GlobalSecondaryIndexes":[{{"IndexName":"gidx","KeySchema":[{{"AttributeName":"g","KeyType":"HASH"}}],"Projection":{{"ProjectionType":"ALL"}}}}],"BillingMode":"PAY_PER_REQUEST"}}"#
        );
        s.ok(B, "CreateTable", &bad);
        s.ok(A, "UpdateTable", &create_body("b"));
        s.until("CREATION_FAILED", |s| {
            s.replicas(A).get("b").map(String::as_str) == Some("CREATION_FAILED")
        });
        // Clear the obstacle, remove the failed replica, create again.
        s.ok(B, "DeleteTable", &format!(r#"{{"TableName":"{TABLE}"}}"#));
        s.ok(A, "UpdateTable", &delete_body("b"));
        s.until("the failed replica is gone", |s| {
            s.spec_replicas(A).len() <= 1
        });
        s.ok(A, "UpdateTable", &create_body("b"));
        s.until("both ACTIVE on the retry", |s| s.both_active());
        s.until("the rows readable on B", |s| {
            s.read(B, "pre0").as_deref() == Some("old")
        });
        for i in 0..3 {
            assert_eq!(
                s.read(B, &format!("pre{i}")).as_deref(),
                Some("old"),
                "seed={seed}"
            );
        }
    }
}

/// Finalize every node to cluster version 2 only: the G-c gate is open, the
/// `MrecReplication` gate is not.
fn finalize_to_v2(s: &mut S) {
    use animus_control::version::VersionRange;
    for c in [A, B] {
        let seed = s.seed;
        let cl = &mut s.w.clusters[c];
        let n = cl.node_count() as u64;
        cl.set_all_node_versions(Some(VersionRange::new(1, 3)));
        super::sim_world_mrec_tests::poll(cl, "the era", seed, |cl| {
            (0..n).all(|i| {
                cl.features(i).era_active() && cl.metadata(i).node_versions.len() == n as usize
            })
        });
        let idx = cl.control_leader_index();
        let leader = cl.control_node_id(idx);
        let (st, v) = cl.admin(
            leader,
            "POST",
            "/admin/cluster-version/finalize",
            "",
            br#"{"to":2,"expected":1}"#,
        );
        assert_eq!(st, 200, "seed={seed}: {v}");
        super::sim_world_mrec_tests::poll(cl, "version 2", seed, |cl| {
            (0..n).all(|i| cl.features(i).cluster_version() == 2)
        });
        assert!(
            !(0..n).any(|i| cl
                .features(i)
                .is_open(animus_control::version::Gate::MrecReplication)),
            "seed={seed}: the MREC gate must still be closed at version 2"
        );
    }
}

#[test]
fn only_the_mrec_gate_closed_keeps_the_old_rejection() {
    for seed in saga_seeds().into_iter().take(20) {
        let mut s = S::new(seed, false);
        finalize_to_v2(&mut s);
        let body = create_body("b");
        let (st, r) = s.call(A, "UpdateTable", &body);
        assert_eq!(st, 400, "seed={seed}: {r}");
        assert!(r.contains("is not supported yet"), "seed={seed}: {r}");
        // The G-c (MRSC) path stays open at version 2: the rejection above
        // is the MREC gate's alone, and nothing is created or shipped.
        assert!(
            s.w.clusters[A].metadata(0).table_global(TABLE).is_none(),
            "seed={seed}"
        );
        for _ in 0..3 {
            s.step_both();
        }
        assert!(
            s.w.bridge_log().is_empty(),
            "seed={seed}: traffic crossed the WAN below the gate"
        );
    }
}
