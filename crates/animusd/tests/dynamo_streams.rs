//! DynamoDB Streams catalog + write-gate end-to-end tests (ADR 0042 §2/§4/
//! §9/§1) — `SetTableStream`'s replication, `UpdateTable`/`DescribeTable`'s
//! wire surface, `TransactWriteItems` on a streamed table (ADR 0046 A1/U3,
//! `TxnStage` kind-writes stack — supersedes the old wholesale rejection this
//! file used to test), and the trim janitor's `"copier"`-tag expectation.
//! Real time/sockets (the `ProdEnv` edge), so every eventual property is a
//! converged-or-timeout poll, never a fixed sleep.
//!
//! The write-path itself (a streamed-unindexed table committing exactly a
//! base row and a change record, view-type storage invariance, trim staying
//! blocked) is covered in-crate (`animusd::dynamo::stream_write_path_tests`)
//! — those assertions need `CpGroup`'s private kind-scan accessors this
//! external `tests/` crate cannot reach; see that module's own doc.

use std::net::SocketAddr;
use std::time::Duration;

use animus_control::Metadata;
use animus_tablet::TabletId;
use animusd::{
    Node, SegmentStoreConfig, StorageBackend, StreamSealKnobs, bind_cluster, start_cluster,
    start_cluster_with_streams,
};
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
async fn transact_write_items_on_a_streamed_table_delivers_correct_events() {
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
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable(streamed) failed: {body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/streamed/stream/{label}");

    // A plain (unstreamed) table's own transaction still works unaffected.
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
        r#"{"TransactItems":[{"Put":{"TableName":"plain",
            "Item":{"id":{"S":"a"}}}}]}"#,
    )
    .await;
    assert_eq!(
        status, 200,
        "plain-table transaction should succeed: {body}"
    );

    // The transaction under test: two Puts on the streamed table, one
    // participant each (ADR 0046 A1/U3, `TxnStage` kind-writes stack) —
    // each must produce exactly one change record, correctly imaged.
    //
    // NOTE on `ApproximateCreationDateTime` (ADR 0046 B1's "informational
    // commit_ts" clause): this suite does NOT assert its value reports the
    // transaction's true commit instant — that sub-piece is a documented,
    // deliberate scope cut (see this PR's own notes / the ADR 0018
    // amendment): the change-log record's bytes are frozen at STAGE time
    // (`eval_kind_txn_write`), strictly before the anchor's commit_ts even
    // exists, and `animus-cp-data`'s `materialize_derived` must keep
    // treating those bytes as opaque (ADR 0043) to stay the one shared
    // helper `KindBatch` also uses — so there is no correctness-preserving
    // place left to patch the true commit instant in after the fact. B1's
    // load-bearing half (the KEY position comes from the resolve's own
    // monotonic ts, never the commit_ts) is unaffected and is what this
    // test actually exercises.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[
            {"Put":{"TableName":"streamed","Item":{"id":{"S":"x1"},"v":{"N":"1"}}}},
            {"Put":{"TableName":"streamed","Item":{"id":{"S":"x2"},"v":{"N":"2"}}}}]}"#,
    )
    .await;
    assert_eq!(status, 200, "streamed-table transaction failed: {body}");

    let (status, body) = dynamo(
        addr,
        "DynamoDBStreams_20120810.DescribeStream",
        &format!(r#"{{"StreamArn":"{stream_arn}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "DescribeStream failed: {body}");
    let v = json(&body);
    let shards = v["StreamDescription"]["Shards"].as_array().unwrap();
    let shard_id = shards.last().expect("at least one shard")["ShardId"]
        .as_str()
        .unwrap()
        .to_owned();

    let it = get_shard_iterator(addr, &stream_arn, &shard_id, "TRIM_HORIZON", None).await;
    let (records, _next) = get_records(addr, &it, None).await;
    assert_eq!(
        records.len(),
        2,
        "expected exactly one change record per transactional write, got: {records:?}"
    );
    let mut seen_ids: Vec<String> = Vec::new();
    for record in &records {
        assert_eq!(record["eventName"], "INSERT", "{record:?}");
        let new_image = &record["dynamodb"]["NewImage"];
        let id = new_image["id"]["S"].as_str().unwrap().to_owned();
        assert!(
            id == "x1" || id == "x2",
            "unexpected id in transactional stream record: {record:?}"
        );
        seen_ids.push(id);
        // Present and numeric — see this test's own doc for why its exact
        // value (whether it reports the true commit instant) is out of
        // scope here.
        record["dynamodb"]["ApproximateCreationDateTime"]
            .as_f64()
            .unwrap_or_else(|| {
                panic!("ApproximateCreationDateTime missing/non-numeric: {record:?}")
            });
    }
    seen_ids.sort();
    assert_eq!(seen_ids, vec!["x1".to_string(), "x2".to_string()]);
}

/// **Abort case**: a `TransactWriteItems` that fails a `ConditionCheck`
/// leaves no stream event — ADR 0046 A1's "abort discards the kind-writes
/// payload entirely" (which includes the change-log record) at the wire
/// level, proven through the real `GetRecords` read path this time rather
/// than a direct kind-scope read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transact_write_items_abort_leaves_no_stream_event() {
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
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/streamed/stream/{label}");

    // The ConditionCheck targets a key that does not exist — the whole
    // transaction, including the Put on the streamed table, must abort.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.TransactWriteItems",
        r#"{"TransactItems":[
            {"ConditionCheck":{"TableName":"streamed","Key":{"id":{"S":"missing"}},
                               "ConditionExpression":"attribute_exists(id)"}},
            {"Put":{"TableName":"streamed","Item":{"id":{"S":"x1"}}}}]}"#,
    )
    .await;
    assert_eq!(status, 400, "expected TransactionCanceledException: {body}");
    assert!(body.contains("TransactionCanceledException"), "{body}");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"streamed","Key":{"id":{"S":"x1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body, "{}", "x1 must not have committed: {body}");

    // A genuine, immediately-following write proves the stream is still
    // alive and correctly ordered — its own event must be the FIRST one
    // this shard ever serves, with no phantom event from the aborted
    // transaction ahead of it.
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"streamed","Item":{"id":{"S":"x2"}}}"#,
    )
    .await;
    assert_eq!(status, 200);

    let (status, body) = dynamo(
        addr,
        "DynamoDBStreams_20120810.DescribeStream",
        &format!(r#"{{"StreamArn":"{stream_arn}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "DescribeStream failed: {body}");
    let v = json(&body);
    let shards = v["StreamDescription"]["Shards"].as_array().unwrap();
    let shard_id = shards.last().expect("at least one shard")["ShardId"]
        .as_str()
        .unwrap()
        .to_owned();

    let it = get_shard_iterator(addr, &stream_arn, &shard_id, "TRIM_HORIZON", None).await;
    let (records, _next) = get_records(addr, &it, None).await;
    assert_eq!(
        records.len(),
        1,
        "the aborted transaction must not surface any event; only x2's genuine write \
         should appear: {records:?}"
    );
    assert_eq!(records[0]["dynamodb"]["NewImage"]["id"]["S"], "x2");
}

// ---------------------------------------------------------------------------
// PR6: the DynamoDB Streams read API (ADR 0042 §3/§5/§6/§7/§9/§10/§11) —
// ListStreams/DescribeStream/GetShardIterator/GetRecords end to end. Real
// `ProdEnv` time/sockets throughout: sealing is driven by the genuine
// `change_consumer_loop` tick against tiny knobs (never the production
// defaults, and never the disable path — F12-b's own final seal gets its
// own dedicated test below), so every seal is awaited via a
// converged-or-timeout poll, matching every other eventual property in this
// file.
// ---------------------------------------------------------------------------

/// Seals almost immediately on any pending byte (`seal_bytes: 1`) — one
/// write becomes one shard on the `change_consumer_loop`'s very next tick.
/// The knob a test wants when it needs a **chain of small, individually
/// sealed shards**.
fn tiny_seal_knobs() -> StreamSealKnobs {
    StreamSealKnobs {
        seal_bytes: 1,
        seal_age: Duration::from_secs(3600),
    }
}

/// Never seals on size and ages out only after an hour — the knob a test
/// wants when it needs a **stable open shard** for the whole test's
/// duration, with no race against the periodic seal arm.
fn never_seals_knobs() -> StreamSealKnobs {
    StreamSealKnobs {
        seal_bytes: 100_000_000,
        seal_age: Duration::from_secs(3600),
    }
}

/// Never seals on size but ages out fast (`seal_age: 300ms`) — the knob a
/// test wants when it needs **several records landing in one shard**: write
/// them all in a quick burst (well under the age threshold), then let the
/// age trigger sweep the whole backlog in one seal once it elapses.
fn age_seal_knobs() -> StreamSealKnobs {
    StreamSealKnobs {
        seal_bytes: 100_000_000,
        seal_age: Duration::from_millis(300),
    }
}

async fn start_streamed_cluster(
    n: usize,
    dir: &std::path::Path,
    knobs: StreamSealKnobs,
) -> Vec<Node> {
    let bound = bind_cluster(n, "127.0.0.1".parse().unwrap(), dir)
        .await
        .unwrap();
    start_cluster_with_streams(
        bound,
        StorageBackend::default(),
        None,
        None,
        Duration::from_secs(600),
        knobs,
        SegmentStoreConfig::default(),
        animusd::DEFAULT_STREAM_RETENTION,
    )
    .await
    .unwrap()
}

fn tablet_for(meta: &Metadata, table: &str) -> TabletId {
    meta.tablets_for_table(table)
        .next()
        .map(|(&t, _)| t)
        .unwrap_or_else(|| panic!("table `{table}` has no tablet yet"))
}

fn chain_len(meta: &Metadata, tablet: TabletId) -> usize {
    meta.stream_shards
        .range((tablet, 0)..=(tablet, u64::MAX))
        .count()
}

/// Poll until `table`'s tablet has sealed at least `at_least` shards **on
/// every node** (the `change_consumer_loop`'s seal arm having ticked enough
/// times, and every node's own control-plane replica having caught up to
/// that commit) — the house converged-or-timeout idiom, never a fixed
/// sleep. Checking every node, not just `nodes[0]`, is load-bearing for a
/// multi-node "every node" test: a per-node `Metadata` replica can lag its
/// own control Raft's commit by a few ms, and `GetRecords`/
/// `GetShardIterator` resolve against *that* node's own snapshot
/// (`ClientCtx::effective_metadata`) — querying a node whose view hasn't
/// caught up yet would transiently (and correctly, per the stream's own
/// eventually consistent contract) resolve a just-sealed shard as still
/// "open," which this test's single-shot assertions are not meant to race.
async fn await_chain_len(nodes: &[Node], table: &str, at_least: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if nodes.iter().all(|n| {
            let meta = n.metadata();
            meta.has_table_schema(table) && chain_len(&meta, tablet_for(&meta, table)) >= at_least
        }) {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("chain length {at_least} for `{table}` never reached on every node in 20s");
        }
        sleep(Duration::from_millis(20)).await;
    }
}

fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
}

async fn get_shard_iterator(
    addr: SocketAddr,
    stream_arn: &str,
    shard_id: &str,
    iterator_type: &str,
    sequence_number: Option<&str>,
) -> String {
    let seq = sequence_number
        .map(|s| format!(r#","SequenceNumber":"{s}""#))
        .unwrap_or_default();
    let body = format!(
        r#"{{"StreamArn":"{stream_arn}","ShardId":"{shard_id}","ShardIteratorType":"{iterator_type}"{seq}}}"#
    );
    let (status, resp) = dynamo(addr, "DynamoDBStreams_20120810.GetShardIterator", &body).await;
    assert_eq!(status, 200, "GetShardIterator failed: {resp}");
    json(&resp)["ShardIterator"]
        .as_str()
        .unwrap_or_else(|| panic!("no ShardIterator in: {resp}"))
        .to_owned()
}

async fn get_records(
    addr: SocketAddr,
    iterator: &str,
    limit: Option<usize>,
) -> (Vec<serde_json::Value>, Option<String>) {
    let lim = limit
        .map(|l| format!(r#","Limit":{l}"#))
        .unwrap_or_default();
    let body = format!(r#"{{"ShardIterator":"{iterator}"{lim}}}"#);
    let (status, resp) = dynamo(addr, "DynamoDBStreams_20120810.GetRecords", &body).await;
    assert_eq!(status, 200, "GetRecords failed: {resp}");
    let v = json(&resp);
    let records = v["Records"].as_array().cloned().unwrap_or_default();
    let next = v["NextShardIterator"].as_str().map(str::to_owned);
    (records, next)
}

/// The full read-path walk: `ListStreams`/`DescribeStream` show a chain of
/// two closed shards followed by one open one (ADR 0042 §2/§3, ADR 0043
/// §A4's parent-before-child lineage), each closed shard drains to a null
/// `NextShardIterator`, and the open shard never nulls — an empty poll
/// returns the identical iterator (F4/§7).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_records_walks_the_shard_chain_and_drains_the_open_tail() {
    let dir = tempfile::TempDir::new().unwrap();
    let nodes = start_streamed_cluster(1, dir.path(), tiny_seal_knobs()).await;
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/t/stream/{label}");

    // Shard 0: one INSERT, sealed on its own (seal_bytes=1).
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p1"},"v":{"N":"1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    await_chain_len(&nodes, "t", 1).await;

    // Shard 1: one MODIFY, sealed on its own.
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p1"},"v":{"N":"2"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    await_chain_len(&nodes, "t", 2).await;

    // The open tail: one REMOVE, deliberately left unsealed.
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.DeleteItem",
        r#"{"TableName":"t","Key":{"id":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);

    let (status, body) = dynamo(addr, "DynamoDBStreams_20120810.ListStreams", "{}").await;
    assert_eq!(status, 200, "ListStreams failed: {body}");
    let v = json(&body);
    assert!(
        v["Streams"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["TableName"] == "t" && s["StreamLabel"] == label),
        "{body}"
    );

    let (status, body) = dynamo(
        addr,
        "DynamoDBStreams_20120810.DescribeStream",
        &format!(r#"{{"StreamArn":"{stream_arn}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "DescribeStream failed: {body}");
    let v = json(&body);
    assert_eq!(v["StreamDescription"]["StreamStatus"], "ENABLED", "{body}");
    let shards = v["StreamDescription"]["Shards"].as_array().unwrap();
    assert_eq!(shards.len(), 3, "expected 2 closed + 1 open: {body}");
    assert!(
        shards[0]["SequenceNumberRange"]["EndingSequenceNumber"].is_string(),
        "{body}"
    );
    assert!(
        shards[1]["SequenceNumberRange"]["EndingSequenceNumber"].is_string(),
        "{body}"
    );
    assert!(
        shards[2]["SequenceNumberRange"]["EndingSequenceNumber"].is_null(),
        "the tail shard must still be open: {body}"
    );
    assert_eq!(
        shards[1]["ParentShardId"], shards[0]["ShardId"],
        "shard 1 must name shard 0 as its parent"
    );
    assert_eq!(
        shards[2]["ParentShardId"], shards[1]["ShardId"],
        "the open shard must name the last sealed shard as its parent"
    );
    let shard0 = shards[0]["ShardId"].as_str().unwrap().to_owned();
    let shard1 = shards[1]["ShardId"].as_str().unwrap().to_owned();
    let shard2 = shards[2]["ShardId"].as_str().unwrap().to_owned();

    // Walk shard 0: one INSERT, then null (exhausted, ADR 0042 §2).
    let it0 = get_shard_iterator(addr, &stream_arn, &shard0, "TRIM_HORIZON", None).await;
    let (records, next) = get_records(addr, &it0, None).await;
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["eventName"], "INSERT");
    assert!(next.is_none(), "shard 0 must exhaust to a null iterator");

    // Walk shard 1: one MODIFY, then null.
    let it1 = get_shard_iterator(addr, &stream_arn, &shard1, "TRIM_HORIZON", None).await;
    let (records, next) = get_records(addr, &it1, None).await;
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["eventName"], "MODIFY");
    assert!(next.is_none(), "shard 1 must exhaust to a null iterator");

    // Walk the open shard: one REMOVE, never null; an empty poll after it
    // must return the SAME iterator (F4/§7's "not there yet, poll again").
    let it2 = get_shard_iterator(addr, &stream_arn, &shard2, "TRIM_HORIZON", None).await;
    let (records, next) = get_records(addr, &it2, None).await;
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["eventName"], "REMOVE");
    let it2b = next.expect("an open shard's iterator must never null");
    let (records2, next2) = get_records(addr, &it2b, None).await;
    assert!(records2.is_empty(), "{records2:?}");
    assert_eq!(
        next2.as_deref(),
        Some(it2b.as_str()),
        "an empty poll on an open shard must return the SAME iterator"
    );
}

/// **The iterator-survives-seal property (F4, ADR 0042 §2's "sealing never
/// invalidates an open-shard iterator")**: mint an iterator against a shard
/// while it is still open, let it seal underneath (the periodic seal arm,
/// no re-mint), and prove the SAME token — still naming the same shard id —
/// now resolves through the sealed path and still returns the correct data.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_shard_iterator_survives_a_seal_and_keeps_working() {
    let dir = tempfile::TempDir::new().unwrap();
    let nodes = start_streamed_cluster(1, dir.path(), tiny_seal_knobs()).await;
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/t/stream/{label}");

    // Mint an iterator against the tablet's still-empty open shard
    // (epoch 0 — nothing has sealed yet).
    let (status, body) = dynamo(
        addr,
        "DynamoDBStreams_20120810.DescribeStream",
        &format!(r#"{{"StreamArn":"{stream_arn}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let v = json(&body);
    let shards = v["StreamDescription"]["Shards"].as_array().unwrap();
    assert_eq!(
        shards.len(),
        1,
        "expected exactly the open epoch-0 shard: {body}"
    );
    let shard0 = shards[0]["ShardId"].as_str().unwrap().to_owned();
    let token = get_shard_iterator(addr, &stream_arn, &shard0, "TRIM_HORIZON", None).await;

    let (records, next) = get_records(addr, &token, None).await;
    assert!(records.is_empty(), "{records:?}");
    let token = next.expect("open shard, must not null");

    // Write, then let the periodic seal arm seal this exact epoch (never a
    // re-mint — the whole point of this test).
    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    await_chain_len(&nodes, "t", 1).await;

    let (records, next) = get_records(addr, &token, None).await;
    assert_eq!(
        records.len(),
        1,
        "the pre-seal token must still return the record now sealed under it: {records:?}"
    );
    assert_eq!(records[0]["eventName"], "INSERT");
    assert!(
        next.is_none(),
        "now sealed and fully drained, so this must null"
    );
}

/// `Limit` pagination drains a multi-record sealed shard **exactly once**
/// (no gaps, no duplicates) — several records land in one shard via the
/// age trigger (a size-bytes-huge/age-tiny knob burst, never the disable
/// path), then a small `Limit` walks the whole thing page by page.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limit_pagination_drains_a_sealed_shard_exactly_once() {
    let dir = tempfile::TempDir::new().unwrap();
    let nodes = start_streamed_cluster(1, dir.path(), age_seal_knobs()).await;
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"KEYS_ONLY"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/t/stream/{label}");

    let ids = ["p1", "p2", "p3", "p4", "p5"];
    for id in ids {
        let (status, _) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &format!(r#"{{"TableName":"t","Item":{{"id":{{"S":"{id}"}}}}}}"#),
        )
        .await;
        assert_eq!(status, 200);
    }
    // The age trigger sweeps the whole backlog together once it elapses —
    // exactly one shard, all 5 records.
    await_chain_len(&nodes, "t", 1).await;

    let (status, body) = dynamo(
        addr,
        "DynamoDBStreams_20120810.DescribeStream",
        &format!(r#"{{"StreamArn":"{stream_arn}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let v = json(&body);
    let shards = v["StreamDescription"]["Shards"].as_array().unwrap();
    let shard0 = shards[0]["ShardId"].as_str().unwrap().to_owned();
    assert!(
        shards[0]["SequenceNumberRange"]["EndingSequenceNumber"].is_string(),
        "{body}"
    );

    let mut token = get_shard_iterator(addr, &stream_arn, &shard0, "TRIM_HORIZON", None).await;
    let mut seen: Vec<String> = Vec::new();
    loop {
        let (records, next) = get_records(addr, &token, Some(2)).await;
        for r in &records {
            let id = r["dynamodb"]["Keys"]["id"]["S"]
                .as_str()
                .unwrap_or_else(|| panic!("no id in {r:?}"))
                .to_owned();
            seen.push(id);
        }
        match next {
            Some(n) => token = n,
            None => break,
        }
        if seen.len() > ids.len() {
            panic!("paginated past the expected record count: {seen:?}");
        }
    }
    seen.sort();
    assert_eq!(
        seen,
        ids.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        "must see every record exactly once, no gaps, no duplicates"
    );
}

/// A sealed shard is served by **any** node — no forwarding needed, since
/// the default `ClusterSegmentStore` replicates to every node of a 3-node
/// cluster (`K = min(RF, candidates) = 3`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_records_on_a_sealed_shard_works_from_every_node() {
    let dir = tempfile::TempDir::new().unwrap();
    let nodes = start_streamed_cluster(3, dir.path(), tiny_seal_knobs()).await;
    await_bootstrap(&nodes).await;
    let addr0 = nodes[0].dynamo_addr();

    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"KEYS_ONLY"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/t/stream/{label}");

    let (status, _) = dynamo(
        addr0,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);
    await_chain_len(&nodes, "t", 1).await;

    let (status, body) = dynamo(
        addr0,
        "DynamoDBStreams_20120810.DescribeStream",
        &format!(r#"{{"StreamArn":"{stream_arn}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let v = json(&body);
    let shards = v["StreamDescription"]["Shards"].as_array().unwrap();
    let shard0 = shards[0]["ShardId"].as_str().unwrap().to_owned();
    assert!(
        shards[0]["SequenceNumberRange"]["EndingSequenceNumber"].is_string(),
        "{body}"
    );

    for (i, node) in nodes.iter().enumerate() {
        let it = get_shard_iterator(
            node.dynamo_addr(),
            &stream_arn,
            &shard0,
            "TRIM_HORIZON",
            None,
        )
        .await;
        let (records, next) = get_records(node.dynamo_addr(), &it, None).await;
        assert_eq!(records.len(), 1, "node {i}: {records:?}");
        assert_eq!(records[0]["eventName"], "INSERT", "node {i}");
        assert!(next.is_none(), "node {i}: shard must exhaust");
    }
}

/// An open shard's `GetRecords` is served by whichever node leads the
/// tablet, **forwarded** from any other node it's issued through — the
/// `ClientRequest::StreamHotRead` allowlist regression (mirroring
/// `kind_scan.rs`'s house pattern for this class of bimodal flake).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_records_on_an_open_shard_forwards_correctly_from_every_node() {
    let dir = tempfile::TempDir::new().unwrap();
    let nodes = start_streamed_cluster(3, dir.path(), never_seals_knobs()).await;
    await_bootstrap(&nodes).await;
    let addr0 = nodes[0].dynamo_addr();

    let (status, body) = dynamo(
        addr0,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"KEYS_ONLY"}}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/t/stream/{label}");
    // Every node's own control-plane replica must see the enabled stream
    // before this test starts issuing `GetShardIterator`/`GetRecords`
    // through each of them in turn — the same convergence discipline
    // `await_chain_len` enforces for a sealed shard.
    await_stream_label_everywhere(&nodes, "t", &label).await;

    let (status, _) = dynamo(
        addr0,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);

    let (status, body) = dynamo(
        addr0,
        "DynamoDBStreams_20120810.DescribeStream",
        &format!(r#"{{"StreamArn":"{stream_arn}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let v = json(&body);
    let shards = v["StreamDescription"]["Shards"].as_array().unwrap();
    assert_eq!(shards.len(), 1, "must still be the one open shard: {body}");
    let shard0 = shards[0]["ShardId"].as_str().unwrap().to_owned();
    assert!(
        shards[0]["SequenceNumberRange"]["EndingSequenceNumber"].is_null(),
        "{body}"
    );

    for (i, node) in nodes.iter().enumerate() {
        let it = get_shard_iterator(
            node.dynamo_addr(),
            &stream_arn,
            &shard0,
            "TRIM_HORIZON",
            None,
        )
        .await;
        let (records, next) = get_records(node.dynamo_addr(), &it, None).await;
        assert_eq!(records.len(), 1, "node {i}: {records:?}");
        assert_eq!(records[0]["eventName"], "INSERT", "node {i}");
        assert!(next.is_some(), "node {i}: an open shard must never null");
    }
}

/// A bare (non-`Forwarded`) `ClientRequest::StreamHotRead` over the plain
/// client protocol is refused — mirroring `KindWrite`/`KindScan`/
/// `ForceSeal`'s identical contract (the house "internal RPC must reject a
/// bare delivery" rule).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bare_stream_hot_read_is_refused() {
    let dir = tempfile::TempDir::new().unwrap();
    let bound = bind_cluster(1, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    let mut stream = TcpStream::connect(nodes[0].client_addr())
        .await
        .expect("connect to client port");
    let request = animusd::ClientRequest::StreamHotRead {
        tablet: 1,
        from_position: 0,
        limit: 10,
    };
    animusd::write_frame(&mut stream, &request)
        .await
        .expect("write frame");
    let response: animusd::ClientResponse = animusd::read_frame(&mut stream)
        .await
        .expect("read frame")
        .expect("connection stayed open for a reply");
    match response {
        animusd::ClientResponse::Error(msg) => {
            assert!(
                msg.contains("must be sent wrapped"),
                "expected the internal-RPC refusal message, got: {msg}"
            );
        }
        other => panic!("expected a bare-request refusal, got: {other:?}"),
    }
}

/// F12-b's disable grace window (ADR 0042 §11): `ListStreams` still names
/// the `DISABLED` label, `DescribeStream` reports it with no open shard,
/// and its already-sealed reads keep working — all via the ordinary
/// catalog-row-based label resolution (§4), no dedicated disable janitor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_stream_grace_window_lists_and_serves_sealed_reads_with_no_open_shard() {
    let dir = tempfile::TempDir::new().unwrap();
    let nodes = start_streamed_cluster(1, dir.path(), tiny_seal_knobs()).await;
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"t",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "StreamSpecification":{"StreamEnabled":true,
                "StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let label = field(&body, "LatestStreamLabel");
    let stream_arn = format!("arn:aws:dynamodb:animus:0:table/t/stream/{label}");

    let (status, _) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"t","Item":{"id":{"S":"p1"}}}"#,
    )
    .await;
    assert_eq!(status, 200);

    // Disable: F12-b's own final seal moves every record to the sealed
    // tier before the write gate closes (ADR 0043 §A3), no matter whether
    // the periodic seal arm had already gotten to it.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateTable",
        r#"{"TableName":"t","StreamSpecification":{"StreamEnabled":false}}"#,
    )
    .await;
    assert_eq!(status, 200, "disable failed: {body}");
    let disabled = async {
        loop {
            if nodes[0].metadata().table_stream("t").is_none() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), disabled)
        .await
        .expect("disable never converged");

    let (status, body) = dynamo(addr, "DynamoDBStreams_20120810.ListStreams", "{}").await;
    assert_eq!(status, 200, "{body}");
    let v = json(&body);
    assert!(
        v["Streams"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["StreamLabel"] == label),
        "a disabled-but-unreaped stream must still be listed: {body}"
    );

    let (status, body) = dynamo(
        addr,
        "DynamoDBStreams_20120810.DescribeStream",
        &format!(r#"{{"StreamArn":"{stream_arn}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let v = json(&body);
    assert_eq!(v["StreamDescription"]["StreamStatus"], "DISABLED", "{body}");
    let shards = v["StreamDescription"]["Shards"].as_array().unwrap();
    assert!(!shards.is_empty(), "{body}");
    for s in shards {
        assert!(
            s["SequenceNumberRange"]["EndingSequenceNumber"].is_string(),
            "a DISABLED stream must have no open shard: {body}"
        );
    }

    let shard0 = shards[0]["ShardId"].as_str().unwrap().to_owned();
    let it = get_shard_iterator(addr, &stream_arn, &shard0, "TRIM_HORIZON", None).await;
    let (records, _next) = get_records(addr, &it, None).await;
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["eventName"], "INSERT");

    // A label that never existed at all (the ResourceNotFound branch this
    // PR covers; genuine post-expiry ResourceNotFound is PR7's own test).
    let bogus_arn = "arn:aws:dynamodb:animus:0:table/t/stream/never-existed";
    let (status, body) = dynamo(
        addr,
        "DynamoDBStreams_20120810.DescribeStream",
        &format!(r#"{{"StreamArn":"{bogus_arn}"}}"#),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("ResourceNotFoundException"), "{body}");
}
