//! Real-socket, real-`ProdEnv` regression for issue #1035: a throttle
//! refusal minted at a remote tablet leader (`dynamo.rs::
//! kind_write_item_at_leader`'s own precharge check,
//! `WireError::provisioned_throughput_exceeded`) must survive the forwarded
//! `KindWriteItem` hop with its own `ProvisionedThroughputExceededException`
//! code — not degrade to a bare 500 `InternalServerError` — when the client
//! is connected to a non-leader node. `dynamo.rs::decode_relayed_error`'s
//! own allowlist used to be missing exactly this one code (it was already
//! present in the batch hop's sibling allowlist,
//! `wire_error_from_batch_rejected`), so a leader-connected client saw the
//! correct 400 while a non-leader-connected client saw a 500 for the
//! identical refusal.
//!
//! `sim_cluster_dynamo_throttle.rs`'s own `SimCluster` sibling
//! (`a_conditioned_forwarded_write_is_throttled_on_the_leader`) covers the
//! identical claim deterministically; this file is the real-socket
//! companion the issue itself asks for — a genuine multi-process-shaped
//! forwarding hop over real TCP/`ProdEnv`, not the in-process `SimCluster`
//! harness.
//!
//! Mirrors `dynamo_throttling.rs`'s own bring-up/big-item idiom (see that
//! file's own module doc for why a large item, not a tiny configured rate,
//! is the fast, real-time route to a genuine refusal) and
//! `kind_write_batch.rs`'s non-leader-node forwarding shape;
//! `cluster_growth.rs`'s `/admin/raftkv` `groups` reading is how this file
//! finds the tablet's actual leader.
//!
//! **The one fact this file exists to exploit**: a plain, unconditioned
//! `PutItem` on a plain (no GSI/LSI/stream) table takes
//! `dynamo.rs::fast_marker_write` instead of `kind_write_item_at_leader` —
//! its own forwarded error channel (`map_throttleable_error`, a bare
//! string) never touches `decode_relayed_error`'s allowlist at all, so it
//! can never exercise this bug. Every pre-existing throttle-forwarding test
//! (`sim_cluster_dynamo_throttle.rs::a_forwarded_write_is_throttled_on_
//! the_leader`, this crate's own former `dynamo_throttling.rs` scenarios)
//! used exactly that unconditioned shape. Only a `PutItem`/`UpdateItem`/
//! `DeleteItem` carrying a `ConditionExpression` (or `ReturnValues`, or an
//! indexed table) is evaluated at `kind_write_item_at_leader`, the path
//! `decode_relayed_error` guards — so the assertion write below always
//! carries a `ConditionExpression`.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

mod support;

// ---------------------------------------------------------------------------
// Shared bring-up + protocol helpers (mirrors dynamo_throttling.rs /
// kind_write_batch.rs / create_table_ready.rs — this crate's own
// per-file-fixture convention favors duplicating these small helpers over
// reaching into a sibling test binary, which cargo cannot share code with
// anyway).
// ---------------------------------------------------------------------------

/// Brings up a cluster with every node started with a real cluster-wide
/// write-throttle default (ADR 0065 §5(a), W-08 step 4) — byte-identical to
/// `dynamo_throttling.rs::bring_up_with_throttle_defaults`, just under this
/// file's own name. `tablet_max_{read,write}_units: Some(0)` disables the
/// ADR 0067 throughput-derived auto-split trigger, and no `auto_split_bytes`
/// is set, so the table's single tablet never splits mid-test — this file's
/// whole premise (finding "the" leader of "the" tablet) needs exactly one
/// tablet for the table's lifetime.
async fn bring_up_with_throttle_defaults(
    n: usize,
    dir: &std::path::Path,
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
                throttle_read_units: None,
                throttle_write_units,
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
                None,
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

async fn await_cluster_bootstrap(nodes: &[animusd::Node]) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let leader = nodes.iter().any(animusd::Node::is_control_leader);
            let everyone_registered = nodes.iter().all(|n| !n.metadata().members.is_empty());
            if leader && everyone_registered {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cluster did not bootstrap within 30s");
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

/// One HTTP/1.0 GET to the admin endpoint; returns `(status, parsed JSON)`
/// — mirrors `create_table_ready.rs`'s identical helper.
async fn admin_get(addr: SocketAddr, path: &str) -> (u16, serde_json::Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let request = format!("GET {path} HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n");
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
    let value: serde_json::Value = serde_json::from_str(payload).expect("admin body is JSON");
    (status, value)
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

/// A large (~256 KiB), JSON-safe attribute value — mirrors
/// `dynamo_throttling.rs::big_value`: big enough that a single `PutItem`
/// costs many capacity units, so a small handful of real HTTP round trips
/// exhausts the ADR 0065 `300 x rate` burst window regardless of the tiny
/// configured refill rate.
fn big_value() -> String {
    "x".repeat(256 * 1024)
}

fn put_body(table: &str, id: &str, value: &str) -> String {
    format!(r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"v":{{"S":"{value}"}}}}}}"#)
}

/// A `ConditionExpression`-carrying `PutItem` — the one shape that takes
/// `kind_write_item_at_leader`/`decode_relayed_error`'s own forwarded hop
/// instead of `fast_marker_write`'s unaffected one. See this file's own
/// module doc for why this is load-bearing.
fn put_body_conditioned(table: &str, id: &str, value: &str) -> String {
    format!(
        r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"v":{{"S":"{value}"}}}},
            "ConditionExpression":"attribute_not_exists(never_set)"}}"#
    )
}

/// Parse an error body's `__type` field.
fn error_type(body: &str) -> String {
    let json: serde_json::Value =
        serde_json::from_str(body).unwrap_or_else(|e| panic!("body is not JSON ({e}): {body}"));
    json["__type"]
        .as_str()
        .unwrap_or_else(|| panic!("no __type in: {body}"))
        .to_string()
}

/// Finds the index, in `nodes`, of the node that reports itself the leader
/// of `tablet` in its own node-local `/admin/raftkv` view
/// (`cluster_growth.rs::raftkv_groups`'s identical `(tablet, node,
/// is_leader)` shape). `CreateTable`'s own 200 already implies the tablet's
/// group is elected and serving (`create_table_ready.rs`'s own subject), so
/// this converges almost immediately — a short bounded poll rather than a
/// one-shot check purely to absorb ordinary scheduling jitter across three
/// real processes/sockets, not because the property itself is expected to
/// be pending.
async fn find_leader_index(nodes: &[animusd::Node], tablet: u64) -> usize {
    for _ in 0..100 {
        for (i, node) in nodes.iter().enumerate() {
            let (status, body) = admin_get(node.admin_addr(), "/admin/raftkv").await;
            if status != 200 {
                continue;
            }
            let Some(groups) = body["groups"].as_array() else {
                continue;
            };
            let is_leader = groups.iter().any(|g| {
                g["tablet"].as_u64() == Some(tablet) && g["is_leader"].as_bool() == Some(true)
            });
            if is_leader {
                return i;
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("no node ever reported itself leader of tablet {tablet} within 5s");
}

// ---------------------------------------------------------------------------
// Issue #1035: a conditioned write throttled at a remote leader must come
// back with its own code through a non-leader-connected node, exactly like
// it already does through the leader-connected node.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_conditioned_write_throttled_at_a_remote_leader_reports_400_not_500() {
    let dir = support::panic_safe_tempdir();
    // A tiny cluster-wide write budget, so a couple of big-item puts
    // exhausts the burst quickly and deterministically (see `big_value`'s
    // own doc).
    let (nodes, config) = bring_up_with_throttle_defaults(3, dir.path(), Some(1)).await;
    await_cluster_bootstrap(&nodes).await;

    create_table(config.nodes[0].dynamo, "relay_thr").await;

    let tablet = nodes[0]
        .metadata()
        .tablets_for_table("relay_thr")
        .next()
        .map(|(id, _)| id.0)
        .expect("relay_thr has a tablet the instant CreateTable acks");
    let leader = find_leader_index(&nodes, tablet).await;
    // Replication is 3 == node count (`MAX_REPLICATION_FACTOR`), so every
    // node hosts this tablet — any node other than the leader is a genuine
    // forwarding entry point.
    let non_leader = (0..3).find(|&i| i != leader).expect("3 nodes, 1 leader");

    let value = big_value();
    // Drain the 300-unit burst with plain, unconditioned puts directly on
    // the leader — cheapest way to exhaust the bucket; the shape under
    // test is the CONDITIONED assertions below, not this drain. Four
    // ~256 KiB puts cost roughly 4x the whole burst window, so this
    // cannot leave the bucket un-exhausted regardless of the tiny (1
    // unit/s) refill rate or real-time jitter across the drain calls.
    for i in 0..4 {
        let _ = dynamo(
            config.nodes[leader].dynamo,
            "DynamoDB_20120810.PutItem",
            &put_body("relay_thr", &format!("drain{i}"), &value),
        )
        .await;
    }

    // The regression: a conditioned write via the NON-leader-connected node
    // must be throttled with the correct code, not degrade to a 500.
    let (status, body) = dynamo(
        config.nodes[non_leader].dynamo,
        "DynamoDB_20120810.PutItem",
        &put_body_conditioned("relay_thr", "cond_non_leader", &value),
    )
    .await;
    assert_eq!(
        status, 400,
        "expected the conditioned write via the non-leader node ({non_leader}) to be throttled, \
         not degrade to a 500: {body}"
    );
    assert_eq!(
        error_type(&body),
        "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException",
        "unexpected error body from the non-leader node: {body}"
    );

    // The control: the identical conditioned write against the leader's own
    // node never crosses the forwarded hop at all, so it must already show
    // the correct code both before and after this fix.
    let (status, body) = dynamo(
        config.nodes[leader].dynamo,
        "DynamoDB_20120810.PutItem",
        &put_body_conditioned("relay_thr", "cond_leader", &value),
    )
    .await;
    assert_eq!(
        status, 400,
        "expected the conditioned write via the leader node ({leader}) to be throttled: {body}"
    );
    assert_eq!(
        error_type(&body),
        "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException",
        "unexpected error body from the leader node: {body}"
    );

    for node in nodes {
        node.shutdown_graceful().await;
    }
}
