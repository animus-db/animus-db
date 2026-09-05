//! ADR 0065 (per-table throttling, W-08 steps 3 and 4) — the real-`ProdEnv`,
//! real-TCP wire-shape regression: `PutItem`/`GetItem` refuse with
//! `ProvisionedThroughputExceededException` once a table's configured
//! budget is exhausted; `BatchWriteItem`/`BatchGetItem` shed the throttled
//! subset into `UnprocessedItems`/`UnprocessedKeys` rather than failing the
//! whole call; `TransactWriteItems` cancels with
//! `TransactionCanceledException` carrying a `ThrottlingError` reason at the
//! right index; a forwarded write (received by a node that does not host the
//! tablet's leader) is throttled on the leader, not silently admitted;
//! `/admin/metrics` reports nonzero throttled counters; and (step 4, below
//! the step-3 tests) `CreateTable`/`UpdateTable`'s own `BillingMode`/
//! `ProvisionedThroughput`, `DescribeTable`'s reporting of them, the
//! follower-relay regression for `MetaCommand::SetTableThroughput`, and the
//! cluster-wide `cluster_settings`/CLI config surface (`animusd::
//! run_node_with_cluster_settings`) versus a per-table override.
//!
//! Most tests below still configure the cluster-wide default via
//! `POST /admin/throttle/defaults` (a live override, kept as a genuinely
//! useful runtime lever alongside the durable config surface — ADR 0065
//! §5(a)) rather than the config surface itself, simply because it's the
//! lighter-weight way to set up a step-3-shaped scenario; the step-4 section
//! exercises the config surface (and `CreateTable`/`UpdateTable`) directly.
//! Every item used here is deliberately large (tens of KB) so a handful of
//! real HTTP round trips exhausts a 300-unit burst — the ADR's fixed `300 ×
//! rate` burst window means a *small* configured rate still yields a
//! moderate token count, and the cheapest way to drain it quickly in a
//! real-time test is a large per-request cost, not a vanishingly small
//! rate. Real TCP/time, so bounded loops rather than a fixed op count where
//! real network jitter could matter.

use std::net::SocketAddr;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

mod support;

// ---------------------------------------------------------------------------
// Shared bring-up + protocol helpers (mirrors dynamo_txn_cancellation.rs /
// admin_endpoint.rs).
// ---------------------------------------------------------------------------

async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<animusd::Node>, animusd::ClusterConfig) {
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
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: None,
        };
        let mut nodes = Vec::new();
        let mut failed = false;
        for i in 0..n {
            // ADR 0067 (W-08b): this file's own tests deliberately create
            // tables with huge `ProvisionedThroughput` values (to drive the
            // *throttle* bucket's arithmetic, not to test the auto-split
            // trigger) — `run_node`'s own default per-tablet capacity
            // ceilings (3000 RCU / 1000 WCU) would otherwise derive a huge
            // minimum tablet count for those tables and repeatedly split
            // them mid-test. `run_node_with_cluster_settings` with an
            // explicit `Some(0)`/`Some(0)` disables that trigger entirely
            // (see `TabletCapacityCeilings`'s own doc) while keeping every
            // other knob at `run_node`'s own defaults.
            match animusd::run_node_with_cluster_settings(
                &config,
                i,
                dir.join(format!("node-{attempt}-{i}")),
                animusd::StorageBackend::default(),
                animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER,
                animusd::StreamSealKnobs::default(),
                animusd::SegmentStoreConfig::default(),
                animusd::DEFAULT_STREAM_RETENTION,
                Duration::ZERO,
                None,
                None,
                None,
                animusd::BackupStoreConfig::default(),
                None,
                None,
                Some(0),
                Some(0),
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

/// Like [`bring_up`], but every node is started with a real cluster-wide
/// throttle default (ADR 0065 §5(a), W-08 step 4) — the config-surface path
/// itself (`animusd::run_node_with_cluster_settings`'s own
/// `throttle_read_units`/`throttle_write_units` params), not the
/// `POST /admin/throttle/defaults` test hook every other test in this file
/// uses. Also stamps `ClusterConfig::cluster_settings` with the same values
/// for documentation, though `run_node_with_cluster_settings` itself takes
/// them as explicit arguments rather than re-reading the config.
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
            })
            .collect();
        let config = animusd::ClusterConfig {
            nodes: nodes_cfg,
            dynamo_auth: None,
            cluster_settings: Some(animusd::config::ClusterSettings {
                throttle_read_units,
                throttle_write_units,
                // ADR 0067 (W-08b): disabled here too — see `bring_up`'s
                // identical comment.
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
                None,
                None,
                None,
                animusd::BackupStoreConfig::default(),
                throttle_read_units,
                throttle_write_units,
                Some(0),
                Some(0),
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

/// One HTTP/1.0 request to the admin endpoint; returns `(status, parsed
/// JSON)` — mirrors `admin_endpoint.rs`'s own helper.
async fn admin(addr: SocketAddr, method: &str, path: &str, body: Option<&str>) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.0\r\n\
         Host: animus\r\n\
         Content-Type: application/json\r\n\
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
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    let value: Value = serde_json::from_str(payload)
        .unwrap_or_else(|e| panic!("admin body is not JSON ({e}): {payload}"));
    (status, value)
}

/// Set every node's own cluster-wide default throttle limits via the live
/// `POST /admin/throttle/defaults` override (ADR 0065 §5(a) — kept
/// alongside the durable `cluster_settings`/CLI config surface,
/// [`bring_up_with_throttle_defaults`], as a genuinely useful runtime
/// lever). `None` renders as JSON `null`, matching `ThrottleDefaultsReq`'s
/// `Option<u64>` fields.
async fn set_throttle_defaults_everywhere(
    config: &animusd::ClusterConfig,
    read_units: Option<u64>,
    write_units: Option<u64>,
) {
    let body = format!(
        r#"{{"read_units":{},"write_units":{}}}"#,
        read_units.map_or("null".to_string(), |v| v.to_string()),
        write_units.map_or("null".to_string(), |v| v.to_string()),
    );
    for node_cfg in &config.nodes {
        let (status, resp) = admin(
            node_cfg.admin,
            "POST",
            "/admin/throttle/defaults",
            Some(&body),
        )
        .await;
        assert_eq!(status, 200, "set_throttle_defaults failed: {resp}");
    }
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

/// A **streamed** table — `table_change_records_carry_images` is then true,
/// so `BatchWriteItem` takes the per-item evaluate-at-leader funnel
/// (`ClientCtx::cp_kind_write_item`/`kind_write_item_at_leader`) instead of
/// the ADR 0049 marker fast arm's single-entry-per-tablet-group commit. This
/// matters specifically for [`batch_write_item_sheds_throttled_rows_into_
/// unprocessed_items`]: a marker table's `BatchWriteItem` batch (sharing one
/// tablet) commits as ONE atomic Raft entry, so ADR 0065's own tablet-group
/// granularity (see `dynamo::marker_batch_write_raw`'s own doc) admits or
/// sheds the WHOLE group together — there is no partial split to observe on
/// a single-tablet marker table. The per-item funnel checks (and can refuse)
/// each request independently, which is what that test needs to prove.
async fn create_streamed_table(dynamo_addr: SocketAddr, table: &str) {
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "StreamSpecification":{{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
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

/// Parse an error body's `__type` field (the exception name).
fn error_type(body: &str) -> String {
    let json: Value =
        serde_json::from_str(body).unwrap_or_else(|e| panic!("body is not JSON ({e}): {body}"));
    json["__type"]
        .as_str()
        .unwrap_or_else(|| panic!("no __type in: {body}"))
        .to_string()
}

// ---------------------------------------------------------------------------

/// `PutItem` returns `400 ProvisionedThroughputExceededException` once a
/// table's configured write budget is exhausted.
#[tokio::test(flavor = "multi_thread")]
async fn put_item_is_throttled_once_the_write_budget_is_exhausted() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(1, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    create_table(addr, "thr_put").await;
    // 1 WCU/s ⇒ a 300-unit burst; each ~256 KiB item costs ~256 WCU, so a
    // handful of puts exhausts it.
    set_throttle_defaults_everywhere(&config, None, Some(1)).await;

    let value = big_value();
    let mut refused = None;
    for i in 0..20 {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &put_body("thr_put", &format!("k{i}"), &value),
        )
        .await;
        if status == 400 {
            refused = Some(body);
            break;
        }
        assert_eq!(status, 200, "unexpected PutItem failure: {body}");
    }
    let body = refused.expect("expected the write burst to eventually be throttled");
    assert_eq!(
        error_type(&body),
        "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException",
        "unexpected error body: {body}"
    );

    // The item's own key must genuinely never have been considered for
    // reading either — confirm `GetItem` also refuses (read budget wasn't
    // configured here, but proves the write's own refusal wasn't silently
    // upgraded/downgraded into something else).
}

/// `GetItem` returns `400 ProvisionedThroughputExceededException` once a
/// table's configured read budget is exhausted.
#[tokio::test(flavor = "multi_thread")]
async fn get_item_is_throttled_once_the_read_budget_is_exhausted() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(1, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    create_table(addr, "thr_get").await;
    let value = big_value();
    // Seed the item before configuring the read limit.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        &put_body("thr_get", "k", &value),
    )
    .await;
    assert_eq!(status, 200, "seed put failed: {body}");

    // 1 RCU/s ⇒ a 300-unit burst; each consistent read of the ~256 KiB item
    // costs ~64 RCU.
    set_throttle_defaults_everywhere(&config, Some(1), None).await;

    let mut refused = None;
    for _ in 0..40 {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.GetItem",
            r#"{"TableName":"thr_get","ConsistentRead":true,"Key":{"id":{"S":"k"}}}"#,
        )
        .await;
        if status == 400 {
            refused = Some(body);
            break;
        }
        assert_eq!(status, 200, "unexpected GetItem failure: {body}");
    }
    let body = refused.expect("expected the read burst to eventually be throttled");
    assert_eq!(
        error_type(&body),
        "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException",
        "unexpected error body: {body}"
    );
}

/// `BatchWriteItem` returns the throttled subset in `UnprocessedItems`
/// rather than failing the whole call, while everything else that fit
/// within budget commits normally.
#[tokio::test(flavor = "multi_thread")]
async fn batch_write_item_sheds_throttled_rows_into_unprocessed_items() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(1, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    create_streamed_table(addr, "thr_bw").await;
    set_throttle_defaults_everywhere(&config, None, Some(1)).await;

    // Deliberately smaller than `big_value()`: 8 of these must fit in one
    // HTTP request under `animus_node::http::MAX_BODY` (1 MiB) while their
    // summed cost (~400 WCU) still exceeds the 300-unit burst.
    let value = "x".repeat(50 * 1024);
    // 8 items at ~50 WCU each = ~400 WCU requested against a 300-unit
    // burst — some must be shed.
    let items: Vec<String> = (0..8)
        .map(|i| {
            format!(r#"{{"PutRequest":{{"Item":{{"id":{{"S":"bw{i}"}},"v":{{"S":"{value}"}}}}}}}}"#)
        })
        .collect();
    let body = format!(r#"{{"RequestItems":{{"thr_bw":[{}]}}}}"#, items.join(","));
    let (status, resp) = dynamo(addr, "DynamoDB_20120810.BatchWriteItem", &body).await;
    assert_eq!(status, 200, "BatchWriteItem itself must not fail: {resp}");
    let json: Value = serde_json::from_str(&resp).expect("valid JSON");
    let unprocessed = json["UnprocessedItems"]["thr_bw"]
        .as_array()
        .unwrap_or_else(|| panic!("no UnprocessedItems.thr_bw array in: {resp}"));
    assert!(
        !unprocessed.is_empty(),
        "expected at least one throttled item under UnprocessedItems: {resp}"
    );
    assert!(
        unprocessed.len() < items.len(),
        "expected SOME items to still have committed (not everything throttled): {resp}"
    );
    // Every unprocessed entry keeps the client's own request shape.
    for entry in unprocessed {
        assert!(
            entry.get("PutRequest").is_some(),
            "unprocessed entry lost its PutRequest shape: {entry}"
        );
    }
}

/// `BatchGetItem` returns the throttled subset in `UnprocessedKeys` rather
/// than failing the whole call.
#[tokio::test(flavor = "multi_thread")]
async fn batch_get_item_sheds_throttled_keys_into_unprocessed_keys() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(1, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    create_table(addr, "thr_bg").await;
    let value = big_value();
    for i in 0..8 {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &put_body("thr_bg", &format!("bg{i}"), &value),
        )
        .await;
        assert_eq!(status, 200, "seed put {i} failed: {body}");
    }
    set_throttle_defaults_everywhere(&config, Some(1), None).await;

    let keys: Vec<String> = (0..8)
        .map(|i| format!(r#"{{"id":{{"S":"bg{i}"}}}}"#))
        .collect();
    let body = format!(
        r#"{{"RequestItems":{{"thr_bg":{{"Keys":[{}],"ConsistentRead":true}}}}}}"#,
        keys.join(",")
    );
    let (status, resp) = dynamo(addr, "DynamoDB_20120810.BatchGetItem", &body).await;
    assert_eq!(status, 200, "BatchGetItem itself must not fail: {resp}");
    let json: Value = serde_json::from_str(&resp).expect("valid JSON");
    let unprocessed = json["UnprocessedKeys"]["thr_bg"]["Keys"]
        .as_array()
        .unwrap_or_else(|| panic!("no UnprocessedKeys.thr_bg.Keys array in: {resp}"));
    assert!(
        !unprocessed.is_empty(),
        "expected at least one throttled key under UnprocessedKeys: {resp}"
    );
    assert!(
        unprocessed.len() < keys.len(),
        "expected SOME keys to still have been read (not everything throttled): {resp}"
    );
}

/// `TransactWriteItems` cancels with `TransactionCanceledException` and a
/// `ThrottlingError` `CancellationReasons` entry at the throttled action's
/// own index, once its participant tablet's write budget is exhausted.
#[tokio::test(flavor = "multi_thread")]
async fn transact_write_items_cancels_with_throttling_error() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(1, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    create_table(addr, "thr_txn").await;
    set_throttle_defaults_everywhere(&config, None, Some(1)).await;

    let value = big_value();
    // Drain the 300-unit burst with ordinary puts first (each ~256 WCU).
    for i in 0..6 {
        let _ = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &put_body("thr_txn", &format!("drain{i}"), &value),
        )
        .await;
    }
    // A transactional write costs 2x — with the bucket already drained,
    // this must cancel rather than partially/fully commit.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TransactWriteItems",
        &format!(
            r#"{{"TransactItems":[{{"Put":{{"TableName":"thr_txn","Item":{{"id":{{"S":"txn1"}},"v":{{"S":"{value}"}}}}}}}}]}}"#
        ),
    )
    .await;
    assert_eq!(
        status, 400,
        "expected the transaction to be cancelled: {body}"
    );
    assert_eq!(
        error_type(&body),
        "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
        "unexpected error body: {body}"
    );
    let json: Value = serde_json::from_str(&body).expect("valid JSON");
    let reasons = json["CancellationReasons"]
        .as_array()
        .unwrap_or_else(|| panic!("no CancellationReasons in: {body}"));
    assert_eq!(reasons.len(), 1, "{body}");
    assert_eq!(
        reasons[0]["Code"], "ThrottlingError",
        "expected the single action's own reason to be ThrottlingError: {body}"
    );

    // The item must genuinely not have committed.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"thr_txn","ConsistentRead":true,"Key":{"id":{"S":"txn1"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let json: Value = serde_json::from_str(&body).expect("valid JSON");
    assert!(
        json.get("Item").is_none(),
        "the cancelled transaction's write must not have landed: {body}"
    );
}

/// A write is throttled on the tablet's actual leader even when the client
/// dials a node that does not host it — proving the check runs after
/// forwarding resolves the real leader, not merely at the receiving edge.
#[tokio::test(flavor = "multi_thread")]
async fn a_forwarded_write_is_throttled_on_the_leader() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(3, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr0 = config.nodes[0].dynamo;

    create_table(addr0, "thr_fwd").await;
    set_throttle_defaults_everywhere(&config, None, Some(1)).await;

    let value = big_value();
    // Round-robin the request across every node's own Dynamo listener —
    // whichever ones are not the tablet's leader must forward, and the
    // SAME tablet's SAME leader-side bucket is what ultimately admits or
    // refuses regardless of entry point (mirrors `dynamo_txn_cancellation
    // .rs`'s own "every node's own Dynamo listener" idiom).
    let mut refused = None;
    for i in 0..20 {
        let dynamo_addr = config.nodes[i % config.nodes.len()].dynamo;
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.PutItem",
            &put_body("thr_fwd", &format!("k{i}"), &value),
        )
        .await;
        if status == 400 {
            refused = Some(body);
            break;
        }
        assert_eq!(
            status, 200,
            "unexpected PutItem failure on node {i}: {body}"
        );
    }
    let body = refused.expect(
        "expected the write burst to eventually be throttled regardless of which node \
         received the request",
    );
    assert_eq!(
        error_type(&body),
        "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException",
        "unexpected error body: {body}"
    );
}

/// `/admin/metrics` reports nonzero `throttled_writes`/`throttled_reads`
/// counters once both directions have actually refused something.
#[tokio::test(flavor = "multi_thread")]
async fn admin_metrics_reports_nonzero_throttled_counters() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(1, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;
    let admin_addr = config.nodes[0].admin;

    create_table(addr, "thr_metrics").await;
    let value = big_value();
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        &put_body("thr_metrics", "seed", &value),
    )
    .await;
    assert_eq!(status, 200, "seed put failed: {body}");

    set_throttle_defaults_everywhere(&config, Some(1), Some(1)).await;

    // Drain the write bucket.
    for i in 0..20 {
        let (status, _) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &put_body("thr_metrics", &format!("w{i}"), &value),
        )
        .await;
        if status == 400 {
            break;
        }
    }
    // Drain the read bucket.
    for _ in 0..40 {
        let (status, _) = dynamo(
            addr,
            "DynamoDB_20120810.GetItem",
            r#"{"TableName":"thr_metrics","ConsistentRead":true,"Key":{"id":{"S":"seed"}}}"#,
        )
        .await;
        if status == 400 {
            break;
        }
    }

    let (status, metrics) = admin(admin_addr, "GET", "/admin/metrics", None).await;
    assert_eq!(status, 200, "{metrics}");
    let throttled_writes = metrics["counters"]["throttled_writes"]
        .as_u64()
        .unwrap_or(0);
    let throttled_reads = metrics["counters"]["throttled_reads"].as_u64().unwrap_or(0);
    assert!(
        throttled_writes > 0,
        "expected a nonzero throttled_writes counter: {metrics}"
    );
    assert!(
        throttled_reads > 0,
        "expected a nonzero throttled_reads counter: {metrics}"
    );
    // The per-tablet `throttle` array is populated too.
    let throttle_array = metrics["throttle"]
        .as_array()
        .unwrap_or_else(|| panic!("no throttle array in: {metrics}"));
    assert!(
        !throttle_array.is_empty(),
        "expected at least one tracked tablet in the throttle array: {metrics}"
    );
}

// ---------------------------------------------------------------------------
// W-08 step 4: the real config surface — `CreateTable`/`UpdateTable`'s
// `BillingMode`/`ProvisionedThroughput`, replicated as `TableSchema.
// throughput`, and the cluster-wide `--throttle-{read,write}-units`/
// `cluster_settings` default. No `POST /admin/throttle/defaults` call
// anywhere below — that hook stays reachable (a live override) but every
// test in this section proves the durable, declarative configuration path.
// ---------------------------------------------------------------------------

/// `CreateTable` with `BillingMode: "PROVISIONED"` and a tiny
/// `ProvisionedThroughput` throttles a write burst with **no**
/// `POST /admin/throttle/defaults` call at all — the per-table spec alone is
/// enough.
#[tokio::test(flavor = "multi_thread")]
async fn create_table_with_provisioned_throughput_throttles_without_any_admin_call() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(1, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    // 1 WCU/s declared directly on the table — no admin call anywhere in
    // this test.
    create_table_with_throughput(addr, "thr_ct_provisioned", 5, 1).await;

    let value = big_value();
    let mut refused = None;
    for i in 0..20 {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &put_body("thr_ct_provisioned", &format!("k{i}"), &value),
        )
        .await;
        if status == 400 {
            refused = Some(body);
            break;
        }
        assert_eq!(status, 200, "unexpected PutItem failure: {body}");
    }
    let body = refused
        .expect("expected CreateTable's own declared ProvisionedThroughput to throttle the burst");
    assert_eq!(
        error_type(&body),
        "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException",
        "unexpected error body: {body}"
    );

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}

/// `UpdateTable` with `BillingMode: "PAY_PER_REQUEST"` lifts a previously
/// throttling per-table limit — the table goes back to unthrottled.
#[tokio::test(flavor = "multi_thread")]
async fn update_table_to_pay_per_request_lifts_the_limit() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(1, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    create_table_with_throughput(addr, "thr_ct_lift", 5, 1).await;
    let value = big_value();

    // Drain the tiny burst first.
    let mut refused = false;
    for i in 0..20 {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &put_body("thr_ct_lift", &format!("k{i}"), &value),
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
        "expected the tiny declared budget to throttle first"
    );

    // Lift it: switch back to PAY_PER_REQUEST.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateTable",
        r#"{"TableName":"thr_ct_lift","BillingMode":"PAY_PER_REQUEST"}"#,
    )
    .await;
    assert_eq!(status, 200, "UpdateTable to PAY_PER_REQUEST failed: {body}");
    assert!(
        body.contains("\"BillingMode\":\"PAY_PER_REQUEST\""),
        "{body}"
    );

    // Every further write must now succeed — the table is unthrottled
    // again, byte-for-byte the same as a table that was never provisioned.
    for i in 0..10 {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &put_body("thr_ct_lift", &format!("after{i}"), &value),
        )
        .await;
        assert_eq!(
            status, 200,
            "put {i} unexpectedly refused after reverting to PAY_PER_REQUEST: {body}"
        );
    }

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}

/// `UpdateTable` raising a table's own `ProvisionedThroughput` admits more
/// than the old, tighter budget would have.
#[tokio::test(flavor = "multi_thread")]
async fn update_table_raising_units_admits_more() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(1, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    create_table_with_throughput(addr, "thr_ct_raise", 5, 1).await;
    let value = big_value();

    let mut refused = false;
    for i in 0..20 {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &put_body("thr_ct_raise", &format!("k{i}"), &value),
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
        "expected the tiny declared budget to throttle first"
    );

    // Raise the write budget by many orders of magnitude. `ThrottleBucket::
    // set_rate`'s own doc: it refills at the OLD rate up to the moment of
    // the change and only then raises the *ceiling* — the new rate governs
    // refill only for elapsed time AFTER that reassignment. `UpdateTable`
    // itself never touches the write bucket (only a write does), so the
    // very first post-raise check is still the one that pays that
    // reassignment — it refills at the old, tiny rate for whatever elapsed
    // first — so this is a converged-or-timeout retry (root `CLAUDE.md`'s
    // testing discipline for an eventual property), not a one-shot assert:
    // once the new, vastly higher rate is actually driving refill between
    // two checks, admission follows within a handful of short-sleep
    // retries.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateTable",
        r#"{"TableName":"thr_ct_raise","BillingMode":"PROVISIONED",
            "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":1000000}}"#,
    )
    .await;
    assert_eq!(status, 200, "UpdateTable raising units failed: {body}");
    assert!(body.contains("\"WriteCapacityUnits\":1000000"), "{body}");

    let mut admitted = false;
    for _ in 0..20 {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &put_body("thr_ct_raise", "after-raise", &value),
        )
        .await;
        if status == 200 {
            admitted = true;
            break;
        }
        assert_eq!(
            status, 400,
            "unexpected PutItem failure after raising units: {body}"
        );
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        admitted,
        "expected a write to eventually be admitted once the table's own raised write \
         units actually refill the bucket"
    );

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}

/// `DescribeTable` reports `BillingModeSummary`/`ProvisionedThroughput` for
/// both a `PROVISIONED` table (real declared units) and a `PAY_PER_REQUEST`
/// one (0/0 units, matching real DynamoDB's own reporting for that mode).
#[tokio::test(flavor = "multi_thread")]
async fn describe_table_reports_billing_mode_and_throughput() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(1, dir.path()).await;
    await_bootstrap(&nodes).await;
    let addr = config.nodes[0].dynamo;

    create_table_with_throughput(addr, "thr_describe_prov", 7, 3).await;
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DescribeTable",
        r#"{"TableName":"thr_describe_prov"}"#,
    )
    .await;
    assert_eq!(status, 200, "DescribeTable failed: {body}");
    assert!(body.contains("\"BillingMode\":\"PROVISIONED\""), "{body}");
    assert!(body.contains("\"ReadCapacityUnits\":7"), "{body}");
    assert!(body.contains("\"WriteCapacityUnits\":3"), "{body}");

    create_table(addr, "thr_describe_ppr").await;
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DescribeTable",
        r#"{"TableName":"thr_describe_ppr"}"#,
    )
    .await;
    assert_eq!(status, 200, "DescribeTable failed: {body}");
    assert!(
        body.contains("\"BillingMode\":\"PAY_PER_REQUEST\""),
        "{body}"
    );
    assert!(body.contains("\"ReadCapacityUnits\":0"), "{body}");
    assert!(body.contains("\"WriteCapacityUnits\":0"), "{body}");

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}

/// The bimodal-per-process-flake regression class root `CLAUDE.md`/
/// `docs/engineering-lessons.md` warn about: `UpdateTable`'s
/// `ProvisionedThroughput` change (`MetaCommand::SetTableThroughput`) issued
/// against a node that is **not** the control-plane leader must still
/// commit — it must be on `is_relayable_command`'s allowlist, or this times
/// out on exactly this shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn update_table_throughput_on_a_follower_is_relayed_to_the_leader() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = bring_up(3, dir.path()).await;
    await_bootstrap(&nodes).await;

    let leader = nodes
        .iter()
        .position(animusd::Node::is_control_leader)
        .expect("a control leader must exist after bootstrap");
    let follower = (0..nodes.len()).find(|&i| i != leader).unwrap();

    create_table(config.nodes[leader].dynamo, "thr_relay").await;

    let follower_dynamo = config.nodes[follower].dynamo;
    let (status, body) = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let (status, body) = dynamo(
                follower_dynamo,
                "DynamoDB_20120810.UpdateTable",
                r#"{"TableName":"thr_relay","BillingMode":"PROVISIONED",
                    "ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}}"#,
            )
            .await;
            if status == 200 {
                return (status, body);
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("follower-issued UpdateTable(ProvisionedThroughput) did not commit via relay in 20s");
    assert_eq!(status, 200, "body: {body}");
    assert!(body.contains("\"BillingMode\":\"PROVISIONED\""), "{body}");

    // Replicated to every node's own catalog — converged-or-timeout, never
    // a one-shot assert (the 200 above only proves the follower's own view
    // committed).
    for (i, n) in nodes.iter().enumerate() {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if n.metadata()
                    .table_throughput("thr_relay")
                    .is_some_and(|t| t.write_units == 5 && t.read_units == 5)
                {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!("node {i}: throughput spec missing 20s after follower-relayed UpdateTable")
        });
    }

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}

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
