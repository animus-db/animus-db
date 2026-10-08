//! `SimCluster`-driven end-to-end test of the **every-replica-full** contract
//! over the DynamoDB wire (issue #1228, `docs/chaos.md` findings F-1 / F-3,
//! ADR 0074 section 2): with every node's disk full, a write is refused
//! *promptly* with a named `StorageFull` 503 (never a timeout), and reads of
//! already-written data -- `ConsistentRead: true` and the eventual default --
//! keep being served on every node, because the established tablet leader keeps
//! leading and a full follower keeps acking (frozen at its durable index).
//!
//! The real-process counterpart is `chaos_disk_full`
//! (`crates/animusd/tests/chaos.rs`); this one is seed-reproducible
//! (`ANIMUS_SEED=<seed>`) and asserts strictly where the chaos run can only
//! sample.

use std::time::Duration;

use animus_sim::DiskConfig;

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

const PUT: &str = "DynamoDB_20120810.PutItem";
const GET: &str = "DynamoDB_20120810.GetItem";

fn put_body(v: &str) -> String {
    format!(
        r#"{{"TableName":"full","Item":{{"pk":{{"S":"p1"}},"sk":{{"S":"a"}},"v":{{"S":"{v}"}}}}}}"#
    )
}

fn get_body(consistent: bool) -> String {
    format!(
        r#"{{"TableName":"full","Key":{{"pk":{{"S":"p1"}},"sk":{{"S":"a"}}}},"ConsistentRead":{consistent}}}"#
    )
}

#[test]
fn every_replica_full_refuses_writes_with_503_and_still_serves_reads() {
    let seed = env_seed(0xF011_A11D);
    let mut cluster = SimCluster::new(seed, 3, 3);
    cluster.create_table("full");
    let (status, body) = cluster.dynamo(0, PUT, put_body("before").as_bytes());
    assert_eq!(status, 200, "seed={seed}: healthy put failed: {body}");
    cluster.run_for(Duration::from_millis(500));

    // Every disk fills.
    let mut cfg = DiskConfig::default();
    cfg.set_enospc_prob(1.0);
    cluster.set_disk_config(cfg);

    // The first write after the fill is what discovers it (its persist fails);
    // it may not be acked. Offer it on every node so every replica discovers.
    for node in 0..3u64 {
        let _ = cluster.dynamo(node, PUT, put_body("poke").as_bytes());
    }
    cluster.run_for(Duration::from_millis(500));

    // From here on, in a window long enough to span the 5s read-barrier timeout
    // several times over, every write is a prompt named 503 and every read is
    // served -- on every node, strong and eventual.
    let leader_before = cluster
        .tablet_of("full")
        .and_then(|t| cluster.leader_index_of(t));
    for round in 0..6 {
        for node in 0..3u64 {
            let (status, body) = cluster.dynamo(node, PUT, put_body("refused").as_bytes());
            assert_eq!(
                status, 503,
                "seed={seed} round={round} node={node}: a write with every disk full must be a 503, got {status}: {body}"
            );
            assert!(
                body.contains("StorageFull"),
                "seed={seed} round={round} node={node}: the 503 must name StorageFull: {body}"
            );
            for consistent in [true, false] {
                let (status, body) = cluster.dynamo(node, GET, get_body(consistent).as_bytes());
                assert_eq!(
                    status, 200,
                    "seed={seed} round={round} node={node} consistent={consistent}: read not served: {body}"
                );
                assert!(
                    body.contains("\"before\""),
                    "seed={seed} round={round} node={node} consistent={consistent}: wrong value: {body}"
                );
            }
        }
        cluster.run_for(Duration::from_millis(700));
    }
    let leader_after = cluster
        .tablet_of("full")
        .and_then(|t| cluster.leader_index_of(t));
    assert_eq!(
        leader_before, leader_after,
        "seed={seed}: the tablet leader moved while every replica was full"
    );

    // Space returns: writes resume with no restart.
    cluster.set_disk_config(DiskConfig::default());
    let mut ok = false;
    for _ in 0..60 {
        let (status, _) = cluster.dynamo(1, PUT, put_body("after").as_bytes());
        if status == 200 {
            ok = true;
            break;
        }
        cluster.run_for(Duration::from_millis(500));
    }
    assert!(
        ok,
        "seed={seed}: writes did not resume after space returned"
    );
}
