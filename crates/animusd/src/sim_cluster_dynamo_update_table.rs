//! `SimCluster`-driven end-to-end tests for `UpdateTable`'s **throughput**
//! change over the DynamoDB wire (ADR 0061 rung D3 PR 2b, C-04 D3) — the
//! second half of `dynamo::dispatch_table_op`'s DDL surface, driven through
//! its new `UpdateTable` arm (throughput-only; a stream/index change stays
//! `unsupported_by_generic_dispatch`, PR 2a's own scope). Sibling of
//! `sim_cluster_dynamo_table_ops.rs` (PR 2a's own module, `CreateTable`/
//! `DeleteTable`/`ListTables`/`DescribeTable`), for the identical reason:
//! needs `SimCluster`'s own `pub(crate)` surface, no further visibility
//! widened.
//!
//! Replaces five of `crates/animusd/tests/dynamo_throttling.rs`'s eleven
//! tests: `create_table_with_provisioned_throughput_throttles_without_any_
//! admin_call`, `update_table_to_pay_per_request_lifts_the_limit`,
//! `update_table_raising_units_admits_more`,
//! `describe_table_reports_billing_mode_and_throughput`, and
//! `update_table_throughput_on_a_follower_is_relayed_to_the_leader` — the
//! real DynamoDB-wire `CreateTable`/`UpdateTable`/`DescribeTable`
//! throughput-config-surface tests, all reachable through `dispatch_table_
//! op`'s new arm. The other six (`BatchWriteItem`/`BatchGetItem` shedding,
//! `TransactWriteItems` cancellation, a forwarded-write throttle check, the
//! `/admin/metrics` counter regression, and the cluster-wide
//! `cluster_settings` config-surface test) stay on `ProdEnv` — none is
//! reachable through `dispatch_item_op`/`dispatch_table_op` today (no
//! generic `BatchWriteItem`/`TransactWriteItems`/admin-metrics path), and
//! `set_throttle_defaults_everywhere`/`admin_metrics_reports_nonzero_
//! throttled_counters` specifically need the real `/admin/metrics`
//! `ThrottledWrites`/`ThrottledReads` counters — `SimCluster`'s nodes
//! carry a real `DataRole` since D2 PR 1, with a real per-node metrics
//! sink, so these counters **do** increment under this fixture
//! (`sim_cluster_throttle.rs`'s own module doc now confirms this); they
//! were simply never this rung's own concern. ADR 0061 rung K (post-C-10)
//! is what adds the counter-asserting coverage; see that rung's opener
//! amendment for the full account. **These five tests therefore assert
//! admission behavior (a burst throttles/doesn't/recovers) and
//! `DescribeTable`'s own rendered shape — never a metric counter.**
//!
//! Seed replay (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <test name>`.

use std::time::Duration;

use animus_env::NodeId;

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// The substring `dynamo::THROTTLE_WRITE_REFUSAL`/`THROTTLE_READ_REFUSAL`
/// share — mirrors `sim_cluster_throttle.rs`'s own `REFUSAL_MARKER`.
const REFUSAL_MARKER: &str = "provisioned throughput exceeded";

/// A large-enough value that a single `PutItem` costs many write capacity
/// units against a tiny declared budget — the same "large per-request cost,
/// not a vanishingly small rate" idiom `sim_cluster_throttle.rs`'s own
/// write-side test uses (a 100 KiB value against 1 WCU/s exhausts a
/// 300-unit burst in ~3 writes, clearing the ~12 WCU refill `SimCluster::
/// dynamo`'s own per-call `OP_BUDGET` (12s of virtual time) grants between
/// requests — see that file's module doc for why this per-call refill has
/// to be cleared by a wide margin for a drain loop to net-throttle at all).
fn big_value() -> String {
    "x".repeat(100 * 1024)
}

fn put_body(table: &str, id: &str, value: &str) -> String {
    format!(r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"v":{{"S":"{value}"}}}}}}"#)
}

fn error_type(body: &str) -> String {
    let json: serde_json::Value =
        serde_json::from_str(body).unwrap_or_else(|e| panic!("body is not JSON ({e}): {body}"));
    json["__type"]
        .as_str()
        .unwrap_or_else(|| panic!("no __type in: {body}"))
        .to_string()
}

/// `table`'s own tablet's current, sorted replica set on `node`'s own view
/// of `Metadata` — the reconciler-hazard invariant's own read primitive,
/// mirroring `sim_cluster_dynamo_table_ops.rs`'s identical helper.
fn replica_ids(cluster: &SimCluster, node: u64, table: &str) -> Vec<NodeId> {
    let meta = cluster.metadata(node);
    let (_, t) = meta
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("table {table} has no tablet (seed={})", cluster.seed()));
    let mut ids = t.replicas.clone();
    ids.sort();
    ids
}

/// ADR 0061 rung D3 PR 2a item 5's reconciler-hazard invariant, applied here
/// too: `table`'s tablet must still carry the exact replica set `expected`
/// (captured right after its `CreateTable` returned 200) — see
/// `sim_cluster_dynamo_table_ops.rs`'s own `assert_replicas_unperturbed` doc
/// for the full mechanism this checks. Kept to `node_count <= 3`
/// (`MAX_REPLICATION_FACTOR`) in every test below, per that module's own
/// documented mitigation for the fixture gap this invariant guards against.
fn assert_replicas_unperturbed(cluster: &SimCluster, node: u64, table: &str, expected: &[NodeId]) {
    let got = replica_ids(cluster, node, table);
    assert_eq!(
        got,
        expected,
        "RECONCILER HAZARD (ADR 0061 rung D3 PR 2a item 5): table `{table}`'s tablet \
         replicas moved from {expected:?} (what provisioning picked) to {got:?} on node \
         {node}'s own view (seed={})",
        cluster.seed()
    );
}

/// A `CreateTable` with `BillingMode: "PROVISIONED"` and an explicit
/// `ProvisionedThroughput` — mirrors `dynamo_throttling.rs`'s own
/// `create_table_with_throughput` helper, over `SimCluster::dynamo` instead
/// of a real socket.
fn create_table_with_throughput(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    read_units: u64,
    write_units: u64,
) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
            "BillingMode":"PROVISIONED",
            "ProvisionedThroughput":{{"ReadCapacityUnits":{read_units},"WriteCapacityUnits":{write_units}}}}}"#
    );
    let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes());
    assert_eq!(
        status,
        200,
        "CreateTable({table}) with ProvisionedThroughput failed: {resp} (seed={})",
        cluster.seed()
    );
}

/// A plain, unthrottled `CreateTable` (`PAY_PER_REQUEST`) — mirrors
/// `sim_cluster_dynamo_table_ops.rs`'s own private `create_table` helper
/// (not reusable across modules — each module's own doc explains the
/// in-crate-`#[cfg(test)]`-per-file privacy shape).
fn create_table(cluster: &mut SimCluster, node: u64, table: &str) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}]}}"#
    );
    let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes());
    assert_eq!(
        status,
        200,
        "CreateTable({table}) failed: {resp} (seed={})",
        cluster.seed()
    );
}

/// `CreateTable` with `BillingMode: "PROVISIONED"` and a tiny
/// `ProvisionedThroughput` throttles a write burst with **no** admin call at
/// all — the per-table spec alone is enough. Mirrors `dynamo_throttling
/// .rs`'s own test of the same name.
#[test]
fn create_table_with_provisioned_throughput_throttles_without_any_admin_call() {
    let seed = env_seed(0x5448_5032_0001);
    let mut cluster = SimCluster::new(seed, 1, 1);
    // 1 WCU/s declared directly on the table — no admin call anywhere in
    // this test.
    create_table_with_throughput(&mut cluster, 0, "thr_ct_provisioned", 5, 1);

    let value = big_value();
    let mut refused = None;
    for i in 0..20 {
        let (status, body) = cluster.dynamo(
            0,
            "DynamoDB_20120810.PutItem",
            put_body("thr_ct_provisioned", &format!("k{i}"), &value).as_bytes(),
        );
        if status == 400 {
            refused = Some(body);
            break;
        }
        assert_eq!(
            status, 200,
            "unexpected PutItem failure: {body} (seed={seed})"
        );
    }
    let body = refused.unwrap_or_else(|| {
        panic!(
            "expected CreateTable's own declared ProvisionedThroughput to throttle the burst \
             (seed={seed})"
        )
    });
    assert_eq!(
        error_type(&body),
        "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException",
        "unexpected error body: {body} (seed={seed})"
    );
    assert!(
        body.contains(REFUSAL_MARKER),
        "expected the refusal message itself, got: {body} (seed={seed})"
    );
}

/// `UpdateTable` with `BillingMode: "PAY_PER_REQUEST"` lifts a previously
/// throttling per-table limit — the table goes back to unthrottled. Mirrors
/// `dynamo_throttling.rs`'s own test of the same name.
#[test]
fn update_table_to_pay_per_request_lifts_the_limit() {
    let seed = env_seed(0x5448_5032_0002);
    let mut cluster = SimCluster::new(seed, 1, 1);
    create_table_with_throughput(&mut cluster, 0, "thr_ct_lift", 5, 1);
    let value = big_value();

    // Drain the tiny burst first.
    let mut refused = false;
    for i in 0..20 {
        let (status, body) = cluster.dynamo(
            0,
            "DynamoDB_20120810.PutItem",
            put_body("thr_ct_lift", &format!("k{i}"), &value).as_bytes(),
        );
        if status == 400 {
            refused = true;
            break;
        }
        assert_eq!(
            status, 200,
            "unexpected PutItem failure: {body} (seed={seed})"
        );
    }
    assert!(
        refused,
        "expected the tiny declared budget to throttle first (seed={seed})"
    );

    // Lift it: switch back to PAY_PER_REQUEST.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.UpdateTable",
        br#"{"TableName":"thr_ct_lift","BillingMode":"PAY_PER_REQUEST"}"#,
    );
    assert_eq!(
        status, 200,
        "UpdateTable to PAY_PER_REQUEST failed: {body} (seed={seed})"
    );
    assert!(
        body.contains("\"BillingMode\":\"PAY_PER_REQUEST\""),
        "{body} (seed={seed})"
    );

    // Every further write must now succeed — the table is unthrottled
    // again, byte-for-byte the same as a table that was never provisioned.
    for i in 0..10 {
        let (status, body) = cluster.dynamo(
            0,
            "DynamoDB_20120810.PutItem",
            put_body("thr_ct_lift", &format!("after{i}"), &value).as_bytes(),
        );
        assert_eq!(
            status, 200,
            "put {i} unexpectedly refused after reverting to PAY_PER_REQUEST: {body} \
             (seed={seed})"
        );
    }
}

/// `UpdateTable` raising a table's own `ProvisionedThroughput` admits more
/// than the old, tighter budget would have. Mirrors `dynamo_throttling.rs`'s
/// own test of the same name; the real-`ProdEnv` original's own
/// converged-or-timeout retry (over real wall-clock sleeps, since
/// `ThrottleBucket::set_rate` refills at the OLD rate through the moment of
/// the change) becomes a bounded loop of further `SimCluster::dynamo` calls
/// here — each one already advances the cluster's own virtual clock by
/// `OP_BUDGET` (12s), so no explicit `run_for`/sleep is needed between
/// attempts.
#[test]
fn update_table_raising_units_admits_more() {
    let seed = env_seed(0x5448_5032_0003);
    let mut cluster = SimCluster::new(seed, 1, 1);
    create_table_with_throughput(&mut cluster, 0, "thr_ct_raise", 5, 1);
    let value = big_value();

    let mut refused = false;
    for i in 0..20 {
        let (status, body) = cluster.dynamo(
            0,
            "DynamoDB_20120810.PutItem",
            put_body("thr_ct_raise", &format!("k{i}"), &value).as_bytes(),
        );
        if status == 400 {
            refused = true;
            break;
        }
        assert_eq!(
            status, 200,
            "unexpected PutItem failure: {body} (seed={seed})"
        );
    }
    assert!(
        refused,
        "expected the tiny declared budget to throttle first (seed={seed})"
    );

    // Raise the write budget by many orders of magnitude.
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.UpdateTable",
        br#"{"TableName":"thr_ct_raise","BillingMode":"PROVISIONED",
            "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":1000000}}"#,
    );
    assert_eq!(
        status, 200,
        "UpdateTable raising units failed: {body} (seed={seed})"
    );
    assert!(
        body.contains("\"WriteCapacityUnits\":1000000"),
        "{body} (seed={seed})"
    );

    let mut admitted = false;
    for _ in 0..20 {
        let (status, body) = cluster.dynamo(
            0,
            "DynamoDB_20120810.PutItem",
            put_body("thr_ct_raise", "after-raise", &value).as_bytes(),
        );
        if status == 200 {
            admitted = true;
            break;
        }
        assert_eq!(
            status, 400,
            "unexpected PutItem failure after raising units: {body} (seed={seed})"
        );
    }
    assert!(
        admitted,
        "expected a write to eventually be admitted once the table's own raised write units \
         actually refill the bucket (seed={seed})"
    );
}

/// `DescribeTable` reports `BillingModeSummary`/`ProvisionedThroughput` for
/// both a `PROVISIONED` table (real declared units) and a `PAY_PER_REQUEST`
/// one (0/0 units, matching real DynamoDB's own reporting for that mode).
/// Mirrors `dynamo_throttling.rs`'s own test of the same name.
#[test]
fn describe_table_reports_billing_mode_and_throughput() {
    let seed = env_seed(0x5448_5032_0004);
    let mut cluster = SimCluster::new(seed, 1, 1);
    create_table_with_throughput(&mut cluster, 0, "thr_describe_prov", 7, 3);
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.DescribeTable",
        br#"{"TableName":"thr_describe_prov"}"#,
    );
    assert_eq!(status, 200, "DescribeTable failed: {body} (seed={seed})");
    assert!(
        body.contains("\"BillingMode\":\"PROVISIONED\""),
        "{body} (seed={seed})"
    );
    assert!(
        body.contains("\"ReadCapacityUnits\":7"),
        "{body} (seed={seed})"
    );
    assert!(
        body.contains("\"WriteCapacityUnits\":3"),
        "{body} (seed={seed})"
    );

    create_table(&mut cluster, 0, "thr_describe_ppr");
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.DescribeTable",
        br#"{"TableName":"thr_describe_ppr"}"#,
    );
    assert_eq!(status, 200, "DescribeTable failed: {body} (seed={seed})");
    assert!(
        body.contains("\"BillingMode\":\"PAY_PER_REQUEST\""),
        "{body} (seed={seed})"
    );
    assert!(
        body.contains("\"ReadCapacityUnits\":0"),
        "{body} (seed={seed})"
    );
    assert!(
        body.contains("\"WriteCapacityUnits\":0"),
        "{body} (seed={seed})"
    );
}

/// The bimodal-per-process-flake regression class root `CLAUDE.md`/
/// `docs/engineering-lessons.md` warn about: `UpdateTable`'s
/// `ProvisionedThroughput` change (`MetaCommand::SetTableThroughput`) issued
/// against a node that is **not** the control-plane leader must still
/// commit — it must be on `is_relayable_command`'s allowlist. This is the
/// direct proof `dispatch_table_op`'s new `UpdateTable` arm reaches
/// `update_table_throughput`'s own relay branch, mirroring
/// `sim_cluster_dynamo_table_ops.rs`'s own follower-relay tests (issued
/// against a node `control_leader_index` does not currently name, so the
/// relay branch is the only branch that can possibly succeed there).
/// Mirrors `dynamo_throttling.rs`'s own test of the same name.
#[test]
fn update_table_throughput_on_a_follower_is_relayed_to_the_leader() {
    let seed = env_seed(0x5448_5032_0005);
    let mut cluster = SimCluster::new(seed, 3, 3);
    let leader = cluster.control_leader_index() as u64;
    let follower = (0..cluster.node_count() as u64)
        .find(|&n| n != leader)
        .expect("a 3-node cluster has a non-leader node");

    create_table(&mut cluster, leader, "thr_relay");
    let provisioned = replica_ids(&cluster, leader, "thr_relay");

    let (status, body) = cluster.dynamo(
        follower,
        "DynamoDB_20120810.UpdateTable",
        br#"{"TableName":"thr_relay","BillingMode":"PROVISIONED",
            "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}}"#,
    );
    assert_eq!(
        status, 200,
        "follower-issued UpdateTable(ProvisionedThroughput) failed: {body} \
         (seed={seed}, leader={leader}, follower={follower})"
    );
    assert!(
        body.contains("\"BillingMode\":\"PROVISIONED\""),
        "{body} (seed={seed})"
    );

    // Replicated to every node's own catalog.
    let expected = animus_control::ProvisionedThroughput {
        read_units: 5,
        write_units: 5,
    };
    for node in 0..cluster.node_count() as u64 {
        assert_eq!(
            cluster.metadata(node).table_throughput("thr_relay"),
            Some(&expected),
            "node {node} does not see the follower-relayed throughput spec (seed={seed})"
        );
    }

    // Item 5's reconciler-hazard invariant, after the throughput change.
    cluster.run_for(Duration::from_secs(3));
    assert_replicas_unperturbed(&cluster, leader, "thr_relay", &provisioned);
}
