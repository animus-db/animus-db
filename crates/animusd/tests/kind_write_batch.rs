//! End-to-end tests for issue #996 layer 2: `Operation::BatchWriteItem`'s
//! images-carrying arm proposing ONE `KvCommand::KindEvalBatch` Raft entry
//! per tablet, instead of one `KvCommand::KindEval` entry per item.
//!
//! Real TCP/HTTP wire, real `ProdEnv`, generous timeouts — the same house
//! style `batch_write.rs`/`dynamo_streams.rs` already use. Scenario (a)'s
//! "one accepted proposal" assertion follows `batch_write.rs`'s own
//! documented discipline (issue #601/#974): scrape `cp_proposals_accepted`/
//! `cp_housekeeping_proposals_accepted` off `GET /metrics` and assert on the
//! **counter delta**, never wall-clock time, with the trim janitor's own
//! housekeeping proposals subtracted out so a same-tick trim landing inside
//! the measurement window can't produce a false failure.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{Node, StorageBackend};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// One DynamoDB request over a fresh HTTP/1.1 connection → `(status, body)`.
/// Mirrors `batch_write.rs`'s identical helper — duplicated per this crate's
/// own per-file-fixture convention rather than shared across compilation
/// units.
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

/// `GET /metrics` over a fresh HTTP/1.1 connection to `addr` — same shape as
/// `batch_write.rs`'s identical helper.
async fn metrics(addr: SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("connect to metrics");
    let request = "GET /metrics HTTP/1.1\r\n\
         Host: animus\r\n\
         Connection: close\r\n\
         \r\n";
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
    let (_, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    payload.to_string()
}

fn metric_value(body: &str, name: &str) -> i64 {
    body.lines()
        .find_map(|line| {
            let (n, v) = line.split_once(' ')?;
            if n == name {
                v.trim().parse().ok()
            } else {
                None
            }
        })
        .unwrap_or(0)
}

/// The multi-node sibling of [`await_bootstrap`] — mirrors `cluster_growth.
/// rs`'s identical helper: in a multi-node cluster only ONE node ever
/// becomes control leader, so waiting on every node's OWN leadership (as
/// [`await_bootstrap`] does for the single-node case) hangs forever for
/// every follower.
async fn await_cluster_bootstrap(nodes: &[Node]) {
    let ready = async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|node| !node.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(30), ready)
        .await
        .expect("cluster did not bootstrap within 30s");
}

async fn stop(node: Node) {
    node.shutdown_graceful().await;
    drop(node);
    sleep(Duration::from_millis(200)).await;
}

fn json(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("not JSON: {e}: {body}"))
}

/// Scenario (a): a same-tablet, multi-item `BatchWriteItem` on a
/// Stream+GSI+LSI table costs exactly ONE accepted Raft proposal for the
/// tablet group (never one per item), and every item lands a stream record
/// in submission order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_tablet_batch_costs_one_proposal_and_orders_stream_records() {
    let dir = support::panic_safe_tempdir();
    let node_dir = dir.path().join("node-0");
    let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
    let dynamo_addr = config.nodes[0].dynamo;
    support::await_bootstrap(std::slice::from_ref(&node)).await;

    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"sgl",
            "AttributeDefinitions":[
                {"AttributeName":"pk","AttributeType":"S"},
                {"AttributeName":"sk","AttributeType":"N"},
                {"AttributeName":"cat","AttributeType":"S"},
                {"AttributeName":"alt","AttributeType":"N"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-cat",
                 "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}]}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-alt",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"alt","KeyType":"RANGE"}]}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let label = json(&body)["TableDescription"]["StreamSpecification"].clone();
    assert!(label.is_object(), "stream spec present: {body}");

    const N: usize = 5;
    let puts: Vec<String> = (0..N)
        .map(|i| {
            format!(
                r#"{{"PutRequest":{{"Item":{{"pk":{{"S":"p{i}"}},"sk":{{"N":"{i}"}},"cat":{{"S":"c{i}"}},"alt":{{"N":"{i}"}}}}}}}}"#
            )
        })
        .collect();
    let batch_body = format!(r#"{{"RequestItems":{{"sgl":[{}]}}}}"#, puts.join(","));

    const PROPOSALS: &str = "cp_proposals_accepted";
    const HOUSEKEEPING: &str = "cp_housekeeping_proposals_accepted";
    let before = metrics(dynamo_addr).await;
    let (before_p, before_h) = (
        metric_value(&before, PROPOSALS),
        metric_value(&before, HOUSEKEEPING),
    );

    let (status, body) = dynamo(dynamo_addr, "DynamoDB_20120810.BatchWriteItem", &batch_body).await;
    assert_eq!(status, 200, "BatchWriteItem failed: {body}");
    assert_eq!(body, r#"{"UnprocessedItems":{}}"#, "got: {body}");

    let after = metrics(dynamo_addr).await;
    let client_proposals = (metric_value(&after, PROPOSALS) - before_p)
        - (metric_value(&after, HOUSEKEEPING) - before_h);
    assert_eq!(
        client_proposals, 1,
        "a {N}-item BatchWriteItem against one (unsplit) tablet must cost exactly one client \
         Raft proposal, not one per item"
    );

    // Every item reads back.
    for i in 0..N {
        let (s, b) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.GetItem",
            &format!(
                r#"{{"ConsistentRead":true,"TableName":"sgl","Key":{{"pk":{{"S":"p{i}"}},"sk":{{"N":"{i}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(s, 200, "GetItem p{i} failed: {b}");
        assert!(b.contains(&format!(r#""cat":{{"S":"c{i}"}}"#)), "p{i}: {b}");
    }

    // Every item left a stream record, in submission order.
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDBStreams_20120810.ListStreams",
        r#"{"TableName":"sgl"}"#,
    )
    .await;
    assert_eq!(status, 200, "ListStreams failed: {body}");
    let streams = json(&body)["Streams"].clone();
    let stream_arn = streams[0]["StreamArn"]
        .as_str()
        .unwrap_or_else(|| panic!("no stream ARN: {body}"))
        .to_owned();

    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDBStreams_20120810.DescribeStream",
        &format!(r#"{{"StreamArn":"{stream_arn}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "DescribeStream failed: {body}");
    let shard_id = json(&body)["StreamDescription"]["Shards"][0]["ShardId"]
        .as_str()
        .unwrap_or_else(|| panic!("no shard: {body}"))
        .to_owned();

    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDBStreams_20120810.GetShardIterator",
        &format!(
            r#"{{"StreamArn":"{stream_arn}","ShardId":"{shard_id}","ShardIteratorType":"TRIM_HORIZON"}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "GetShardIterator failed: {body}");
    let iterator = json(&body)["ShardIterator"]
        .as_str()
        .unwrap_or_else(|| panic!("no iterator: {body}"))
        .to_owned();

    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDBStreams_20120810.GetRecords",
        &format!(r#"{{"ShardIterator":"{iterator}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "GetRecords failed: {body}");
    let records = json(&body)["Records"].clone();
    let records = records.as_array().unwrap_or_else(|| panic!("{body}"));
    assert_eq!(records.len(), N, "expected {N} stream records: {body}");
    for (i, record) in records.iter().enumerate() {
        assert_eq!(
            record["dynamodb"]["Keys"]["pk"]["S"],
            format!("p{i}"),
            "stream record {i} out of submission order: {body}"
        );
    }

    stop(node).await;
}

/// Scenario (b): a cross-tablet chunk where one tablet group is refused —
/// here, one PROVISIONED table whose single-tablet throughput is exhausted
/// by an over-cap item, forcing a genuine
/// `ProvisionedThroughputExceededException` — leaves the OTHER table's
/// items applied and reports only the refused ones as unprocessed.
///
/// A real intra-table split (to get two tablets of the SAME table) needs a
/// deterministic split key or an auto-split threshold this fixture doesn't
/// configure; two distinct images-carrying tables in one `BatchWriteItem`
/// call exercise the identical per-tablet grouping/shedding mechanism
/// (`kind_write_batch_at_leader`'s `Vec<Result<..>>` per tablet group), just
/// with the two groups belonging to different tables rather than the same
/// one split in two — the grouping code (`BTreeMap<Option<TabletId>, ..>`)
/// does not distinguish the two cases.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_throttled_tablet_group_sheds_while_the_other_table_applies() {
    let dir = support::panic_safe_tempdir();
    let node_dir = dir.path().join("node-0");
    let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
    let dynamo_addr = config.nodes[0].dynamo;
    support::await_bootstrap(std::slice::from_ref(&node)).await;

    // Table A: PROVISIONED at the minimum (1 WCU) with a single tablet —
    // capacity is a full 300s burst (`ThrottleBucket::new`), i.e. 300 write
    // capacity units, comfortably below one ~350 KB item's own cost
    // (`capacity::write_units`, ceil(bytes/1KB)) so the very first write
    // against it is refused with no pre-drain needed.
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"thA",
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"KEYS_ONLY"},
            "BillingMode":"PROVISIONED",
            "ProvisionedThroughput":{"ReadCapacityUnits":1,"WriteCapacityUnits":1}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable thA failed: {body}");

    // Table B: unindexed by throughput (PAY_PER_REQUEST), images-carrying
    // via a stream so its writes go through the same evaluate-at-leader
    // batched funnel as table A's.
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"thB",
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"KEYS_ONLY"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable thB failed: {body}");

    // Two over-cap items for table A (~350 KB each, cost ~342 WCU > the 300
    // WCU capacity every fresh bucket starts at) and two ordinary items for
    // table B, all in ONE `BatchWriteItem` call.
    let big = "x".repeat(350_000);
    let batch_body = format!(
        r#"{{"RequestItems":{{
            "thA":[
                {{"PutRequest":{{"Item":{{"pk":{{"S":"a0"}},"blob":{{"S":"{big}"}}}}}}}},
                {{"PutRequest":{{"Item":{{"pk":{{"S":"a1"}},"blob":{{"S":"{big}"}}}}}}}}
            ],
            "thB":[
                {{"PutRequest":{{"Item":{{"pk":{{"S":"b0"}},"v":{{"N":"0"}}}}}}}},
                {{"PutRequest":{{"Item":{{"pk":{{"S":"b1"}},"v":{{"N":"1"}}}}}}}}
            ]}}}}"#
    );
    let (status, body) = dynamo(dynamo_addr, "DynamoDB_20120810.BatchWriteItem", &batch_body).await;
    assert_eq!(status, 200, "BatchWriteItem failed: {body}");

    let v = json(&body);
    let unprocessed = &v["UnprocessedItems"];
    assert!(
        unprocessed.get("thB").is_none(),
        "thB's items must not be shed: {body}"
    );
    let th_a_unprocessed = unprocessed["thA"]
        .as_array()
        .unwrap_or_else(|| panic!("thA must have shed items: {body}"));
    assert_eq!(
        th_a_unprocessed.len(),
        2,
        "both of thA's over-cap items must be shed: {body}"
    );

    // thB's items landed.
    for pk in ["b0", "b1"] {
        let (s, b) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.GetItem",
            &format!(
                r#"{{"ConsistentRead":true,"TableName":"thB","Key":{{"pk":{{"S":"{pk}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(s, 200, "GetItem thB/{pk} failed: {b}");
        assert!(b.contains("\"pk\""), "thB/{pk}: {b}");
    }

    // thA's items never landed.
    for pk in ["a0", "a1"] {
        let (s, b) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.GetItem",
            &format!(
                r#"{{"ConsistentRead":true,"TableName":"thA","Key":{{"pk":{{"S":"{pk}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(s, 200, "GetItem thA/{pk} failed: {b}");
        assert_eq!(b, "{}", "thA/{pk} must not have landed: {b}");
    }

    stop(node).await;
}

/// Scenario (c): a `BatchWriteItem` against an images-carrying table,
/// issued from EVERY node of a 2-node cluster in turn — since only one node
/// can lead the table's sole tablet, at least one of the two calls lands on
/// a non-leader-connected node, driving the forwarded `ClientRequest::
/// KindWriteBatch` path (`cp_serve_forwarded`'s new arm). A missing
/// `cp_serve_forwarded` arm for `KindWriteBatch` would fail exactly this
/// call with "must be sent wrapped in `Forwarded`".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_write_from_a_non_leader_connected_node_is_forwarded() {
    let dir = support::panic_safe_tempdir();
    let (nodes, config) = support::bring_up_deadline(2, dir.path(), Duration::from_secs(30)).await;
    await_cluster_bootstrap(&nodes).await;

    let seed_addr = config.nodes[0].dynamo;
    let (status, body) = dynamo(
        seed_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"fwd",
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"KEYS_ONLY"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    for (i, cfg) in config.nodes.iter().enumerate() {
        let batch_body = format!(
            r#"{{"RequestItems":{{"fwd":[
                {{"PutRequest":{{"Item":{{"pk":{{"S":"n{i}a"}},"v":{{"N":"{i}"}}}}}}}},
                {{"PutRequest":{{"Item":{{"pk":{{"S":"n{i}b"}},"v":{{"N":"{i}"}}}}}}}}
            ]}}}}"#
        );
        let (status, body) =
            dynamo(cfg.dynamo, "DynamoDB_20120810.BatchWriteItem", &batch_body).await;
        assert_eq!(status, 200, "BatchWriteItem via node {i} failed: {body}");
        assert_eq!(body, r#"{"UnprocessedItems":{}}"#, "node {i}: {body}");

        for suffix in ["a", "b"] {
            let (s, b) = dynamo(
                seed_addr,
                "DynamoDB_20120810.GetItem",
                &format!(
                    r#"{{"ConsistentRead":true,"TableName":"fwd","Key":{{"pk":{{"S":"n{i}{suffix}"}}}}}}"#
                ),
            )
            .await;
            assert_eq!(s, 200, "GetItem n{i}{suffix} failed: {b}");
            assert!(
                b.contains(&format!(r#""v":{{"N":"{i}"}}"#)),
                "n{i}{suffix}: {b}"
            );
        }
    }

    for node in nodes {
        stop(node).await;
    }
}

/// Scenario (d): a same-key duplicate inside one `BatchWriteItem` call on an
/// images (streamed) table ends with the LAST write visible via `GetItem` —
/// the preserved last-write-wins-by-submission-order behavior layer 1's own
/// in-apply overlay/write-collapse exists for (see `KvCommand::
/// KindEvalBatch`'s own doc, `animus-cp-data`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_same_key_duplicate_in_one_batch_leaves_the_last_write_visible() {
    let dir = support::panic_safe_tempdir();
    let node_dir = dir.path().join("node-0");
    let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
    let dynamo_addr = config.nodes[0].dynamo;
    support::await_bootstrap(std::slice::from_ref(&node)).await;

    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"dupimg",
            "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_IMAGE"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.BatchWriteItem",
        r#"{"RequestItems":{"dupimg":[
            {"PutRequest":{"Item":{"pk":{"S":"x"},"v":{"S":"first"}}}},
            {"PutRequest":{"Item":{"pk":{"S":"x"},"v":{"S":"second"}}}}
        ]}}"#,
    )
    .await;
    assert_eq!(status, 200, "BatchWriteItem failed: {body}");

    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.GetItem",
        r#"{"ConsistentRead":true,"TableName":"dupimg","Key":{"pk":{"S":"x"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "GetItem failed: {body}");
    assert!(
        body.contains(r#""v":{"S":"second"}"#),
        "the LAST write in submission order must be visible: {body}"
    );
    assert!(
        !body.contains(r#""v":{"S":"first"}"#),
        "the first (overwritten) write must not be visible: {body}"
    );

    stop(node).await;
}
