//! `sim_cluster_dynamo_global_table` — G-01 stage G-c, M3: the DynamoDB wire
//! surface of MRSC global tables over [`SimCluster`] (ADR 0075 section 5).
//!
//! Three combined nodes, one per Region (`topology.kubernetes.io/region` =
//! `r-a`/`r-b`/`r-c`), RF 3. Per seed, over the real wire edge:
//!
//! 1. **gate closed** (cluster version 1): `ReplicaUpdates` is refused with the
//!    pre-G-c text byte for byte, and the table stays regional;
//! 2. **legacy ops**: every 2017.11.29 operation is rejected by name, gate or no;
//! 3. after `finalize` to version 2: the named request rejections (EVENTUAL /
//!    absent consistency, wrong Region count, an unknown Region, per-replica
//!    overrides, a non-empty table, TTL, the table's own Region named);
//! 4. a good conversion (`STRONG`, two Creates) returns a `TableDescription`
//!    with `Replicas`/`MultiRegionConsistency`/`GlobalTableVersion`, the
//!    tablet's replica set spans the three Regions, `DescribeTable` agrees and
//!    a plain regional table's `DescribeTable` carries none of those fields;
//! 5. the **restrictions** after conversion: no second conversion / add / delete
//!    replica, no TTL, no transaction APIs; plain item writes and
//!    `ConsistentRead` reads keep working;
//! 6. the witness shape (`Create` + `GlobalTableWitnessUpdates`).
//!
//! Depth: `ANIMUS_SEED=<s>` replays one seed. Every message carries the seed.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::version::VersionRange;
use animus_placement::REGION_LABEL;
use serde_json::Value;

use super::sim_cluster::SimCluster;

const REGIONS: [&str; 3] = ["r-a", "r-b", "r-c"];

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        return vec![s];
    }
    (0..3).map(|i| 0x6C00_0000 + i).collect()
}

fn poll(
    cluster: &mut SimCluster,
    budget: Duration,
    seed: u64,
    what: &str,
    mut cond: impl FnMut(&mut SimCluster) -> bool,
) {
    let mut waited = Duration::ZERO;
    while !cond(cluster) {
        assert!(waited < budget, "seed={seed}: {what} never converged");
        cluster.run_for(Duration::from_millis(100));
        waited += Duration::from_millis(100);
    }
}

fn call(cluster: &mut SimCluster, node: u64, op: &str, body: &str) -> (u16, Value) {
    let (status, resp) = cluster.dynamo(node, &format!("DynamoDB_20120810.{op}"), body.as_bytes());
    let v = serde_json::from_str(&resp).unwrap_or(Value::String(resp));
    (status, v)
}

fn create_table(cluster: &mut SimCluster, seed: u64, table: &str) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    let (s, v) = call(cluster, 0, "CreateTable", &body);
    assert_eq!(s, 200, "seed={seed}: CreateTable {table}: {v}");
}

fn expect_validation(
    cluster: &mut SimCluster,
    seed: u64,
    node: u64,
    op: &str,
    body: &str,
    needle: &str,
) {
    let (s, v) = call(cluster, node, op, body);
    assert_eq!(s, 400, "seed={seed}: {op} {body}: {v}");
    let msg = v["message"]
        .as_str()
        .or(v["Message"].as_str())
        .unwrap_or("");
    assert!(
        v["__type"]
            .as_str()
            .unwrap_or("")
            .contains("ValidationException")
            && msg.contains(needle),
        "seed={seed}: {op} {body}: expected a ValidationException containing `{needle}`, got {v}"
    );
}

fn update(table: &str, rest: &str) -> String {
    format!(r#"{{"TableName":"{table}",{rest}}}"#)
}

const TWO_CREATES: &str = r#""MultiRegionConsistency":"STRONG","ReplicaUpdates":[{"Create":{"RegionName":"r-b"}},{"Create":{"RegionName":"r-c"}}]"#;

fn run_seed(seed: u64) {
    let labels: Vec<BTreeMap<String, String>> = REGIONS
        .iter()
        .map(|r| BTreeMap::from([(REGION_LABEL.to_owned(), (*r).to_owned())]))
        .collect();
    let mut cluster = SimCluster::new_with_node_labels(seed, 3, labels);
    let _ = cluster.control_leader_index();
    for t in ["glob1", "glob2", "plain"] {
        create_table(&mut cluster, seed, t);
    }

    // 2. Legacy operations: rejected by name regardless of the gate.
    for op in animus_dynamo::global::LEGACY_GLOBAL_TABLE_OPERATIONS {
        expect_validation(&mut cluster, seed, 0, op, "{}", "legacy");
    }

    // 1. Gate closed: the pre-G-c text, byte for byte; the table stays regional.
    let (s, v) = call(
        &mut cluster,
        0,
        "UpdateTable",
        &update("glob1", TWO_CREATES),
    );
    assert_eq!(s, 400, "seed={seed}: {v}");
    assert_eq!(
        v["message"].as_str().or(v["Message"].as_str()),
        Some("UpdateTable: ReplicaUpdates is not supported"),
        "seed={seed}: {v}"
    );
    assert!(cluster.metadata(0).table_global("glob1").is_none());

    // Open the gate: every node is a [1, 2] binary, then finalize on the leader.
    cluster.set_all_node_versions(Some(VersionRange::new(1, 2)));
    poll(
        &mut cluster,
        Duration::from_secs(60),
        seed,
        "the era",
        |c| (0..3u64).all(|n| c.features(n).era_active() && c.metadata(n).node_versions.len() == 3),
    );
    let leader = {
        let idx = cluster.control_leader_index();
        cluster.control_node_id(idx)
    };
    let (s, b) = cluster.admin(
        leader,
        "POST",
        "/admin/cluster-version/finalize",
        "",
        br#"{"to":2,"expected":1}"#,
    );
    assert_eq!(s, 200, "seed={seed}: finalize: {b}");
    poll(
        &mut cluster,
        Duration::from_secs(30),
        seed,
        "version 2",
        |c| (0..3u64).all(|n| c.features(n).cluster_version() == 2),
    );

    // 3. Named request rejections.
    expect_validation(
        &mut cluster,
        seed,
        0,
        "UpdateTable",
        &update(
            "glob1",
            r#""ReplicaUpdates":[{"Create":{"RegionName":"r-b"}},{"Create":{"RegionName":"r-c"}}]"#,
        ),
        "eventual consistency",
    );
    expect_validation(
        &mut cluster,
        seed,
        0,
        "UpdateTable",
        &update(
            "glob1",
            r#""MultiRegionConsistency":"EVENTUAL","ReplicaUpdates":[{"Create":{"RegionName":"r-b"}},{"Create":{"RegionName":"r-c"}}]"#,
        ),
        "eventual consistency",
    );
    expect_validation(
        &mut cluster,
        seed,
        0,
        "UpdateTable",
        &update(
            "glob1",
            r#""MultiRegionConsistency":"STRONG","ReplicaUpdates":[{"Create":{"RegionName":"r-b"}}]"#,
        ),
        "exactly 3 Regions",
    );
    expect_validation(
        &mut cluster,
        seed,
        0,
        "UpdateTable",
        &update(
            "glob1",
            r#""MultiRegionConsistency":"STRONG","ReplicaUpdates":[{"Create":{"RegionName":"r-b"}},{"Create":{"RegionName":"r-z"}}]"#,
        ),
        "not a Region of this cluster",
    );
    expect_validation(
        &mut cluster,
        seed,
        0,
        "UpdateTable",
        &update(
            "glob1",
            r#""MultiRegionConsistency":"STRONG","ReplicaUpdates":[{"Create":{"RegionName":"r-a"}},{"Create":{"RegionName":"r-c"}}]"#,
        ),
        "must not be named",
    );
    expect_validation(
        &mut cluster,
        seed,
        0,
        "UpdateTable",
        &update(
            "glob1",
            r#""MultiRegionConsistency":"STRONG","ReplicaUpdates":[{"Create":{"RegionName":"r-b","KMSMasterKeyId":"k"}},{"Create":{"RegionName":"r-c"}}]"#,
        ),
        "KMSMasterKeyId",
    );
    // A non-empty table cannot be converted (AWS fidelity, plan D6).
    let (s, v) = call(
        &mut cluster,
        0,
        "PutItem",
        r#"{"TableName":"glob2","Item":{"pk":{"S":"x"}}}"#,
    );
    assert_eq!(s, 200, "seed={seed}: {v}");
    expect_validation(
        &mut cluster,
        seed,
        0,
        "UpdateTable",
        &update("glob2", TWO_CREATES),
        "must be empty",
    );
    // A table with TTL cannot be converted.
    let (s, v) = call(
        &mut cluster,
        0,
        "UpdateTimeToLive",
        r#"{"TableName":"plain","TimeToLiveSpecification":{"AttributeName":"ttl","Enabled":true}}"#,
    );
    assert_eq!(s, 200, "seed={seed}: {v}");
    expect_validation(
        &mut cluster,
        seed,
        0,
        "UpdateTable",
        &update("plain", TWO_CREATES),
        "TTL",
    );
    // Nothing above changed anything.
    for t in ["glob1", "glob2", "plain"] {
        assert!(
            cluster.metadata(0).table_global(t).is_none(),
            "seed={seed}: {t}"
        );
    }

    // 4. A good conversion (served by a non-leader node: its Region is r-b).
    let (s, v) = call(
        &mut cluster,
        1,
        "UpdateTable",
        &update(
            "glob1",
            r#""MultiRegionConsistency":"STRONG","ReplicaUpdates":[{"Create":{"RegionName":"r-a"}},{"Create":{"RegionName":"r-c"}}]"#,
        ),
    );
    assert_eq!(s, 200, "seed={seed}: convert: {v}");
    let td = &v["TableDescription"];
    assert_eq!(td["MultiRegionConsistency"], "STRONG", "seed={seed}: {v}");
    assert_eq!(td["GlobalTableVersion"], "2019.11.21", "seed={seed}: {v}");
    let spec = cluster
        .metadata(1)
        .table_global("glob1")
        .cloned()
        .expect("global");
    assert_eq!(
        spec.preferred_leader_region, "r-b",
        "seed={seed}: D3 home Region"
    );
    poll(
        &mut cluster,
        Duration::from_secs(60),
        seed,
        "replicas ACTIVE in 3 Regions",
        |c| {
            let (s, v) = call(c, 2, "DescribeTable", r#"{"TableName":"glob1"}"#);
            s == 200
                && v["Table"]["Replicas"].as_array().is_some_and(|r| {
                    r.len() == 3 && r.iter().all(|x| x["ReplicaStatus"] == "ACTIVE")
                })
        },
    );
    let meta = cluster.metadata(2);
    for (_, tablet) in meta.tablets_for_table("glob1") {
        assert_eq!(tablet.replicas.len(), 3, "seed={seed}: {tablet:?}");
    }
    let (_, plain) = call(&mut cluster, 0, "DescribeTable", r#"{"TableName":"plain"}"#);
    for key in [
        "GlobalTableVersion",
        "Replicas",
        "MultiRegionConsistency",
        "GlobalTableWitnesses",
    ] {
        assert!(
            plain["Table"].get(key).is_none(),
            "seed={seed}: {key} on a regional table"
        );
    }

    // 5. Restrictions after conversion.
    expect_validation(
        &mut cluster,
        seed,
        0,
        "UpdateTable",
        &update("glob1", TWO_CREATES),
        "already a global table",
    );
    expect_validation(
        &mut cluster,
        seed,
        0,
        "UpdateTable",
        &update(
            "glob1",
            r#""ReplicaUpdates":[{"Delete":{"RegionName":"r-c"}}]"#,
        ),
        "Delete is not supported",
    );
    expect_validation(
        &mut cluster,
        seed,
        0,
        "UpdateTimeToLive",
        r#"{"TableName":"glob1","TimeToLiveSpecification":{"AttributeName":"ttl","Enabled":true}}"#,
        "TTL is not supported",
    );
    expect_validation(
        &mut cluster,
        seed,
        0,
        "TransactWriteItems",
        r#"{"TransactItems":[{"Put":{"TableName":"glob1","Item":{"pk":{"S":"t"}}}}]}"#,
        "transactions are not supported",
    );
    expect_validation(
        &mut cluster,
        seed,
        0,
        "TransactGetItems",
        r#"{"TransactItems":[{"Get":{"TableName":"glob1","Key":{"pk":{"S":"t"}}}}]}"#,
        "transactions are not supported",
    );
    // Plain item operations keep working, from any Region's node.
    let mut put_ok = false;
    for _ in 0..30 {
        let (s, _) = call(
            &mut cluster,
            2,
            "PutItem",
            r#"{"TableName":"glob1","Item":{"pk":{"S":"k"},"v":{"S":"1"}}}"#,
        );
        if s == 200 {
            put_ok = true;
            break;
        }
        cluster.run_for(Duration::from_secs(1));
    }
    assert!(
        put_ok,
        "seed={seed}: PutItem on the global table never acked"
    );
    let (s, v) = call(
        &mut cluster,
        0,
        "GetItem",
        r#"{"TableName":"glob1","Key":{"pk":{"S":"k"}},"ConsistentRead":true}"#,
    );
    assert_eq!(s, 200, "seed={seed}: {v}");
    assert_eq!(v["Item"]["v"]["S"], "1", "seed={seed}: {v}");
}

fn run_witness_seed(seed: u64) {
    let labels: Vec<BTreeMap<String, String>> = REGIONS
        .iter()
        .map(|r| BTreeMap::from([(REGION_LABEL.to_owned(), (*r).to_owned())]))
        .collect();
    let mut cluster = SimCluster::new_with_node_labels(seed, 3, labels);
    let _ = cluster.control_leader_index();
    cluster.set_all_node_versions(Some(VersionRange::new(1, 2)));
    poll(
        &mut cluster,
        Duration::from_secs(60),
        seed,
        "the era",
        |c| (0..3u64).all(|n| c.features(n).era_active() && c.metadata(n).node_versions.len() == 3),
    );
    let leader = {
        let idx = cluster.control_leader_index();
        cluster.control_node_id(idx)
    };
    let (s, b) = cluster.admin(
        leader,
        "POST",
        "/admin/cluster-version/finalize",
        "",
        br#"{"to":2,"expected":1}"#,
    );
    assert_eq!(s, 200, "seed={seed}: finalize: {b}");
    poll(
        &mut cluster,
        Duration::from_secs(30),
        seed,
        "version 2",
        |c| (0..3u64).all(|n| c.features(n).cluster_version() == 2),
    );
    create_table(&mut cluster, seed, "wit1");
    // A witness without a replica Create, or two witnesses, is refused.
    expect_validation(
        &mut cluster,
        seed,
        0,
        "UpdateTable",
        &update(
            "wit1",
            r#""MultiRegionConsistency":"STRONG","GlobalTableWitnessUpdates":[{"Create":{"RegionName":"r-c"}}]"#,
        ),
        "requires ReplicaUpdates",
    );
    let (s, v) = call(
        &mut cluster,
        0,
        "UpdateTable",
        &update(
            "wit1",
            r#""MultiRegionConsistency":"STRONG","ReplicaUpdates":[{"Create":{"RegionName":"r-b"}}],"GlobalTableWitnessUpdates":[{"Create":{"RegionName":"r-c"}}]"#,
        ),
    );
    assert_eq!(s, 200, "seed={seed}: witness convert: {v}");
    let td = &v["TableDescription"];
    assert_eq!(
        td["GlobalTableWitnesses"][0]["RegionName"], "r-c",
        "seed={seed}: {v}"
    );
    let replicas: Vec<&str> = td["Replicas"]
        .as_array()
        .expect("Replicas")
        .iter()
        .filter_map(|r| r["RegionName"].as_str())
        .collect();
    assert_eq!(
        replicas,
        ["r-a", "r-b"],
        "seed={seed}: the witness is not a replica: {v}"
    );
}

#[test]
fn mrsc_wire_surface_end_to_end() {
    for seed in seeds() {
        run_seed(seed);
    }
}

#[test]
fn mrsc_witness_shape() {
    for seed in seeds() {
        run_witness_seed(seed);
    }
}
