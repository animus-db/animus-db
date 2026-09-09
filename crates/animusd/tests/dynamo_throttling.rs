//! ADR 0065 (per-table throttling, W-08 steps 3 and 4) — the real-`ProdEnv`,
//! real-TCP wire-shape regression for the one throttling scenario with no
//! `SimCluster` analog: the cluster-wide `cluster_settings`/CLI config
//! surface (`animusd::run_node_with_cluster_settings`'s own
//! `throttle_read_units`/`throttle_write_units` params) versus a per-table
//! `ProvisionedThroughput` override.
//!
//! **Everything else this file used to cover has moved to `SimCluster`.**
//! `put_item_is_throttled_once_the_write_budget_is_exhausted`/
//! `get_item_is_throttled_once_the_read_budget_is_exhausted` moved first
//! (ADR 0061 rung D3, redundancy-audit follow-up) to `sim_cluster_
//! throttle.rs::write_admits_a_burst_then_refuses_then_recovers_after_a_
//! full_refill`/`read_admits_a_burst_then_refuses_then_recovers_after_a_
//! full_refill`. `create_table_with_provisioned_throughput_throttles_
//! without_any_admin_call`/`update_table_to_pay_per_request_lifts_the_
//! limit`/`update_table_raising_units_admits_more`/`describe_table_
//! reports_billing_mode_and_throughput`/`update_table_throughput_on_a_
//! follower_is_relayed_to_the_leader` moved next (ADR 0061 rung D3 PR 2b)
//! to `sim_cluster_dynamo_update_table.rs`. Then, ADR 0061 rung K (C-11):
//! `batch_write_item_sheds_throttled_rows_into_unprocessed_items`/
//! `batch_get_item_sheds_throttled_keys_into_unprocessed_keys`/
//! `transact_write_items_cancels_with_throttling_error`/`a_forwarded_
//! write_is_throttled_on_the_leader` (PR 2) moved to `sim_cluster_dynamo_
//! throttle.rs`, and `admin_metrics_reports_nonzero_throttled_counters`
//! (PR 3, this change) moved to `sim_cluster_admin.rs`'s own scenario (9)
//! — see that module's own doc for the mapping (a single-node `SimCluster`,
//! `SimCluster::set_throttle_defaults_all`, bounded drain loops, `GET
//! /admin/metrics` on the table's own tablet leader).
//!
//! **`cluster_wide_throttle_default_is_overridden_by_a_tables_own_
//! throughput` is the one residual and stays real-`ProdEnv`, deliberately**:
//! unlike every other test this file used to hold, its own subject is not a
//! throttle-bucket *check* reachable through the generic dispatch core —
//! it's `animusd::run_node_with_cluster_settings`'s own **config-parse**
//! path, a node-bring-up/process-boundary shape (`ClusterSettings` →
//! per-node `ClientCtx::throttle_defaults` at construction time) that has
//! no `SimCluster` analog: `SimCluster::new` builds every node in-process
//! from its own fixed constructor, never from a `ClusterConfig` a real
//! `animusd` binary would parse. Every item used here is deliberately
//! large (tens of KB) so a handful of real HTTP round trips exhausts a
//! 300-unit burst — the ADR's fixed `300 × rate` burst window means a
//! *small* configured rate still yields a moderate token count, and the
//! cheapest way to drain it quickly in a real-time test is a large
//! per-request cost, not a vanishingly small rate. Real TCP/time, so a
//! bounded loop rather than a fixed op count where real network jitter
//! could matter.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

mod support;

// ---------------------------------------------------------------------------
// Shared bring-up + protocol helpers (mirrors dynamo_txn_cancellation.rs /
// admin_endpoint.rs).
// ---------------------------------------------------------------------------

/// Brings up a cluster with every node started with a real cluster-wide
/// throttle default (ADR 0065 §5(a), W-08 step 4) — the config-surface path
/// itself (`animusd::run_node_with_cluster_settings`'s own
/// `throttle_read_units`/`throttle_write_units` params), this file's own
/// remaining test's subject. Also stamps `ClusterConfig::cluster_settings`
/// with the same values for documentation, though
/// `run_node_with_cluster_settings` itself takes them as explicit arguments
/// rather than re-reading the config.
async fn bring_up_with_throttle_defaults(
    n: usize,
    dir: &std::path::Path,
    throttle_read_units: Option<u64>,
    throttle_write_units: Option<u64>,
) -> (Vec<animusd::Node>, animusd::ClusterConfig) {
    for attempt in 0..16 {
        let addrs = support::free_addrs(n * 6);
        let nodes_cfg: Vec<animusd::RoleAddrs> = (0..n)
            .map(|i| animusd::RoleAddrs {
                id: animusd::config::node_id(i),
                role: animusd::config::NodeRole::Both,
                internal: addrs[6 * i],
                client: addrs[6 * i + 1],
                dynamo: addrs[6 * i + 2],
                admin: addrs[6 * i + 3],
                intra: addrs[6 * i + 4],
                console: addrs[6 * i + 5],
                advertise_host: None,
                tls: None,
                encryption_key_path: None,
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: Some(animusd::config::ClusterSettings {
                throttle_read_units,
                throttle_write_units,
                // ADR 0067 (W-08b): this file's own test deliberately
                // creates tables with a huge `ProvisionedThroughput` (to
                // drive the *throttle* bucket's arithmetic, not to test the
                // auto-split trigger) — `run_node`'s own default per-tablet
                // capacity ceilings (3000 RCU / 1000 WCU) would otherwise
                // derive a huge minimum tablet count for that table and
                // repeatedly split it mid-test; `Some(0)`/`Some(0)`
                // disables that trigger entirely (see
                // `TabletCapacityCeilings`'s own doc).
                tablet_max_read_units: Some(0),
                tablet_max_write_units: Some(0),
                ..Default::default()
            }),
        };
        let mut nodes = Vec::new();
        let mut failed = false;
        for i in 0..n {
            match animusd::run_node_with_cluster_settings(
                &config,
                i,
                dir.join(format!("node-{attempt}-{i}")),
                animusd::StorageBackend::default(),
                animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER,
                animusd::StreamSealKnobs::default(),
                animusd::SegmentStoreConfig::default(),
                animusd::DEFAULT_STREAM_RETENTION,
                Duration::ZERO, // quiescence: irrelevant here, disabled
                false,          // heartbeat batching: irrelevant here, off
                None,
                None,
                None,
                animusd::BackupStoreConfig::default(),
                throttle_read_units,
                throttle_write_units,
                Some(0),
                Some(0),
                None,
                false,
            )
            .await
            {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            return (nodes, config);
        }
        for node in &nodes {
            node.shutdown_graceful().await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up cluster after retries (ports kept getting stolen)");
}

async fn await_bootstrap(nodes: &[animusd::Node]) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(animusd::Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cluster did not bootstrap in 20s");
}

/// One DynamoDB request over a fresh HTTP/1.1 connection → `(status, body)`.
async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to dynamo");
    let request = format!(
        "POST / HTTP/1.1\r\n\
         Host: animus\r\n\
         X-Amz-Target: {target}\r\n\
         Content-Type: application/x-amz-json-1.0\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len(),
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .expect("read full response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    (status, payload.to_string())
}

async fn create_table(dynamo_addr: SocketAddr, table: &str) {
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable({table}) failed: {body}");
}

/// `CreateTable` with `BillingMode: "PROVISIONED"` and an explicit
/// `ProvisionedThroughput` (ADR 0065 §5(b), W-08 step 4) — the per-table
/// config-surface sibling of [`create_table`] above, which declares no
/// throughput at all (`PAY_PER_REQUEST`).
async fn create_table_with_throughput(
    dynamo_addr: SocketAddr,
    table: &str,
    read_units: u64,
    write_units: u64,
) {
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "BillingMode":"PROVISIONED",
                "ProvisionedThroughput":{{"ReadCapacityUnits":{read_units},"WriteCapacityUnits":{write_units}}}}}"#
        ),
    )
    .await;
    assert_eq!(
        status, 200,
        "CreateTable({table}) with ProvisionedThroughput failed: {body}"
    );
}

/// A large (~256 KiB), JSON-safe attribute value — big enough that a single
/// `PutItem`/`GetItem` costs many capacity units, so a small handful of real
/// HTTP round trips exhausts a 300-unit burst window (see this file's own
/// module doc for why this is the fast route to a real refusal, not a
/// vanishingly small configured rate).
fn big_value() -> String {
    "x".repeat(256 * 1024)
}

fn put_body(table: &str, id: &str, value: &str) -> String {
    format!(r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"v":{{"S":"{value}"}}}}}}"#)
}

// ---------------------------------------------------------------------------
// The real config surface (W-08 step 4). `CreateTable`/`UpdateTable`'s own
// `BillingMode`/`ProvisionedThroughput` change, `DescribeTable`'s reporting
// of them, and the follower-relay regression for `MetaCommand::
// SetTableThroughput` all moved to `SimCluster` (ADR 0061 rung D3 PR 2b,
// `crates/animusd/src/sim_cluster_dynamo_update_table.rs`) — see this file's
// own module doc. The one test left here is the one with no sim analog: the
// cluster-wide `--throttle-{read,write}-units`/`cluster_settings` **config
// surface** itself (`animusd::run_node_with_cluster_settings`'s own
// `throttle_read_units`/`throttle_write_units` params) versus a per-table
// override.
// ---------------------------------------------------------------------------

/// ADR 0065 §5(b): a cluster started with a cluster-wide default (the config
/// surface, not `POST /admin/throttle/defaults`) throttles a table with no
/// per-table setting of its own, while a table with its own **higher**
/// setting is not throttled — the per-table spec overrides the cluster
/// default entirely rather than merging with it.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_wide_throttle_default_is_overridden_by_a_tables_own_throughput() {
    let dir = support::panic_safe_tempdir();
    // A tiny cluster-wide default write budget, set only via the config
    // surface (`run_node_with_cluster_settings`) — no admin call.
    let (nodes, config) = bring_up_with_throttle_defaults(1, dir.path(), None, Some(1)).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    // This table declares no throughput of its own, so it inherits the tiny
    // cluster-wide default and throttles quickly.
    create_table(addr, "thr_cfg_default").await;
    let value = big_value();
    let mut refused = false;
    for i in 0..20 {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &put_body("thr_cfg_default", &format!("k{i}"), &value),
        )
        .await;
        if status == 400 {
            refused = true;
            break;
        }
        assert_eq!(status, 200, "unexpected PutItem failure: {body}");
    }
    assert!(
        refused,
        "expected the table with no per-table setting to inherit the tiny cluster default \
         and eventually throttle"
    );

    // This table declares its OWN, much larger throughput — it must ignore
    // the cluster-wide default entirely and stay unthrottled.
    create_table_with_throughput(addr, "thr_cfg_override", 1_000_000, 1_000_000).await;
    for i in 0..10 {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &put_body("thr_cfg_override", &format!("k{i}"), &value),
        )
        .await;
        assert_eq!(
            status, 200,
            "put {i} unexpectedly refused despite the table's own generous override: {body}"
        );
    }

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}
