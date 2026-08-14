//! DynamoDB Streams catalog + write-gate end-to-end tests (ADR 0042 §2/§4/
//! §9/§1) — `SetTableStream`'s replication, `UpdateTable`/`DescribeTable`'s
//! wire surface, `TransactWriteItems`'s extended rejection, and the trim
//! janitor's `"copier"`-tag expectation. Real time/sockets (the `ProdEnv`
//! edge), so every eventual property is a converged-or-timeout poll, never a
//! fixed sleep.
//!
//! The write-path itself (a streamed-unindexed table committing exactly a
//! base row and a change record, view-type storage invariance, trim staying
//! blocked) is covered in-crate (`animusd::dynamo::stream_write_path_tests`)
//! — those assertions need `CpGroup`'s private kind-scan accessors this
//! external `tests/` crate cannot reach; see that module's own doc.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{Node, StorageBackend, bind_cluster, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// One DynamoDB JSON request over a fresh HTTP/1.1 connection → `(status,
/// body)`. Mirrors every other `tests/dynamo_*.rs` file's identical helper.
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

async fn await_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("cluster did not bootstrap within 20s");
}

async fn await_node_bootstrap(node: &Node) {
    let ready = async {
        loop {
            if node.is_control_leader() && !node.metadata().members.is_empty() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("node did not bootstrap within 20s");
}

/// Poll until every node in `nodes` sees `table`'s stream enabled with
/// `label` — the schema-replication regression (ADR 0042 §4/§9).
async fn await_stream_label_everywhere(nodes: &[Node], table: &str, label: &str) {
    let converged = async {
        loop {
            if nodes.iter().all(|n| {
                n.metadata()
                    .table_stream(table)
                    .is_some_and(|s| s.label == label)
            }) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), converged)
        .await
        .unwrap_or_else(|_| panic!("stream label `{label}` never converged on every node"));
}

/// Extract a `"LatestStreamLabel":"..."` (or `"StreamViewType":"..."`) field's
/// value out of a raw JSON response body — a tiny substring parse, matching
/// this codebase's existing `tests/*.rs` convention of not pulling in a JSON
/// crate for response assertions.
fn field(body: &str, name: &str) -> String {
    let needle = format!("\"{name}\":\"");
    let start = body
        .find(&needle)
        .unwrap_or_else(|| panic!("field `{name}` not found in: {body}"))
        + needle.len();
    let end = body[start..].find('"').expect("closing quote") + start;
    body[start..end].to_owned()
}

/// `SetTableStream` enable (via `CreateTable`) replicates to every node's
/// mirrored schema and survives a control-plane restart (ADR 0038's durable
/// mirror) — the schema-replication regression, mirroring
/// `dynamo_schema.rs::create_table_survives_node_restart`'s shape but for
/// the stream configuration rather than the key schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_table_stream_enable_propagates_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let node_dir = dir.path().join("node-0");
    let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
    let dynamo_addr = config.nodes[0].dynamo;
    await_node_bootstrap(&node).await;

    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"orders",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    assert!(body.contains("\"StreamEnabled\":true"), "{body}");
    let label = field(&body, "LatestStreamLabel");

    await_stream_label_everywhere(std::slice::from_ref(&node), "orders", &label).await;

    node.shutdown_graceful().await;
    drop(node);
    sleep(Duration::from_millis(200)).await;

    let node = support::restart_same_addrs(&config, 0, &node_dir, StorageBackend::default()).await;
    await_node_bootstrap(&node).await;
    await_stream_label_everywhere(std::slice::from_ref(&node), "orders", &label).await;
}

/// `UpdateTable`'s enable and disable, issued through **every** node of a
/// 3-node cluster in turn — the relay-allowlist regression
/// (`is_relayable_command` must carry `SetTableStream`, mirroring every
/// other schema-catalog command's identical follower-connected test).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_table_stream_enable_and_disable_through_every_node() {
    let dir = tempfile::TempDir::new().unwrap();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    let (status, body) = dynamo(
        nodes[0].dynamo_addr(),
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    let mut last_label = String::new();
    for (i, node) in nodes.iter().enumerate() {
        // Enable through node `i`.
        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.UpdateTable",
            r#"{"TableName":"t","StreamSpecification":
                {"StreamEnabled":true,"StreamViewType":"KEYS_ONLY"}}"#,
        )
        .await;
        assert_eq!(status, 200, "enable via node {i} failed: {body}");
        let label = field(&body, "LatestStreamLabel");
        assert_ne!(
            label, last_label,
            "node {i}: re-enable must mint a fresh label (ADR 0042 §9)"
        );
        last_label = label.clone();
        await_stream_label_everywhere(&nodes, "t", &label).await;

        // Disable through node `i` too.
        let (status, body) = dynamo(
            node.dynamo_addr(),
            "DynamoDB_20120810.UpdateTable",
            r#"{"TableName":"t","StreamSpecification":{"StreamEnabled":false}}"#,
        )
        .await;
        assert_eq!(status, 200, "disable via node {i} failed: {body}");
        assert!(!body.contains("StreamSpecification"), "{body}");
        let disabled = async {
            loop {
                if nodes
                    .iter()
                    .all(|n| n.metadata().table_stream("t").is_none())
                {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        };
        timeout(Duration::from_secs(20), disabled)
            .await
            .unwrap_or_else(|_| panic!("disable via node {i} never converged"));
    }
}

/// `DescribeTable` returns the stream's spec + ARN once enabled; a
/// disable-then-re-enable mints a genuinely different label (ADR 0042 §4/§9).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_table_returns_stream_spec_and_arn_reenable_mints_new_label() {
    let dir = tempfile::tempdir().unwrap();
    let node_dir = dir.path().join("node-0");
    let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
    let addr = config.nodes[0].dynamo;
    await_node_bootstrap(&node).await;

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_IMAGE"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let first_label = field(&body, "LatestStreamLabel");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DescribeTable",
        r#"{"TableName":"t"}"#,
    )
    .await;
    assert_eq!(status, 200, "DescribeTable failed: {body}");
    assert!(body.contains("\"Table\""), "{body}");
    assert!(body.contains("\"StreamEnabled\":true"), "{body}");
    assert!(body.contains("\"StreamViewType\":\"NEW_IMAGE\""), "{body}");
    let arn = field(&body, "LatestStreamArn");
    assert_eq!(
        arn,
        format!("arn:aws:dynamodb:animus:0:table/t/stream/{first_label}")
    );
    assert_eq!(field(&body, "LatestStreamLabel"), first_label);

    // Disable, then re-enable: a fresh, distinct label (a genuinely new,
    // empty stream — ADR 0042 §9).
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateTable",
        r#"{"TableName":"t","StreamSpecification":{"StreamEnabled":false}}"#,
    )
    .await;
    assert_eq!(status, 200);
    let disabled = async {
        loop {
            if node.metadata().table_stream("t").is_none() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), disabled)
        .await
        .expect("disable never converged");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateTable",
        r#"{"TableName":"t","StreamSpecification":
            {"StreamEnabled":true,"StreamViewType":"OLD_IMAGE"}}"#,
    )
    .await;
    assert_eq!(status, 200, "re-enable failed: {body}");
    let second_label = field(&body, "LatestStreamLabel");
    assert_ne!(
        first_label, second_label,
        "re-enable must mint a fresh label, never reuse the old one"
    );

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DescribeTable",
        r#"{"TableName":"t"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(field(&body, "LatestStreamLabel"), second_label);
    assert!(body.contains("\"StreamViewType\":\"OLD_IMAGE\""), "{body}");
}

/// `TransactWriteItems` is rejected on a streamed table (ADR 0042's
/// extension of the ADR 0041 indexed-table rejection) but still works
/// unmodified on a plain table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transact_write_items_rejected_on_streamed_table_but_not_plain() {
    let dir = tempfile::tempdir().unwrap();
    let node_dir = dir.path().join("node-0");
    let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
    let addr = config.nodes[0].dynamo;
    await_node_bootstrap(&node).await;

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"streamed",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"KEYS_ONLY"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable(streamed) failed: {body}");
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"plain",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable(plain) failed: {body}");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[{"Put":{"TableName":"streamed",
            "Item":{"id":{"S":"a"}}}}]}"#,
    )
    .await;
    assert_eq!(status, 400, "expected rejection: {body}");
    assert!(body.contains("ValidationException"), "{body}");
    assert!(body.contains("streamed table"), "{body}");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[{"Put":{"TableName":"plain",
            "Item":{"id":{"S":"a"}}}}]}"#,
    )
    .await;
    assert_eq!(
        status, 200,
        "plain-table transaction should succeed: {body}"
    );
}
