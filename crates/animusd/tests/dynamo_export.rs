//! End-to-end tests for the S3 export wire surface (ADR 0068, S-05):
//! `ExportTableToPointInTime`/`DescribeExport`/`ListExports` over the real
//! DynamoDB JSON/HTTP wire, with the customer-bucket store swapped for the
//! S-04 in-process fake (`animus_s3::fake::FakeS3`) via
//! [`animusd::Node::set_export_store_factory`] — no real sockets to S3, a
//! real socket for the DynamoDB wire itself (`ProdEnv`). Every eventual
//! property is a converged-or-timeout poll, never a fixed sleep (this
//! codebase's own testing discipline).

use std::io::Read;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use animus_control::Metadata;
use animus_env::SegmentStore;
use animus_s3::client::{HttpRequest, HttpResponse, S3Config, Transport, TransportError};
use animus_s3::fake::FakeS3;
use animus_s3::sigv4::Credentials;
use animus_tablet::{TabletId, partition_token};
use animusd::{ClientRequest, ClientResponse, ExportStoreFactory, Node, read_frame, write_frame};
use async_trait::async_trait;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

const FAKE_BUCKET: &str = "export-test-bucket";
const FAKE_ENDPOINT: &str = "http://fake.export.example:9000";
const FAKE_REGION: &str = "us-east-1";
const FAKE_ACCESS_KEY: &str = "AKIDEXPORTTEST";
const FAKE_SECRET: &str = "export-test-secret";

/// A `Transport` over a *shared* [`FakeS3`] — `FakeS3` itself holds its
/// state behind plain `Mutex`es (not `Arc`-internally-shared), so every
/// node's own [`ExportStoreFactory`] closure needs to share ONE instance
/// (wrapped here) to observe each other's writes, mirroring
/// `animusd::s3_store_handle_tests`' own fake construction but shared
/// across more than one `S3SegmentStore`.
#[derive(Clone)]
struct SharedFakeS3(Arc<FakeS3>);

#[async_trait]
impl Transport for SharedFakeS3 {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        self.0.send(request).await
    }
}

/// Build an [`ExportStoreFactory`] over a shared [`FakeS3`] — the
/// production default this test replaces via
/// [`animusd::Node::set_export_store_factory`] on every node in the
/// cluster, so the export job succeeds regardless of which node's wire
/// edge happens to receive the `ExportTableToPointInTime` call.
fn fake_export_store_factory(fake: Arc<FakeS3>) -> ExportStoreFactory {
    Arc::new(move |bucket: &str, prefix: Option<&str>| {
        let transport = SharedFakeS3(fake.clone());
        let config = S3Config {
            endpoint: FAKE_ENDPOINT.to_string(),
            bucket: bucket.to_string(),
            region: FAKE_REGION.to_string(),
            credentials: Credentials::new(FAKE_ACCESS_KEY, FAKE_SECRET),
        };
        let store: Arc<dyn animus_env::SegmentStore> = Arc::new(animus_env::S3SegmentStore::new(
            transport,
            config,
            prefix.map(str::to_owned),
        ));
        Ok(store)
    })
}

/// Bring up an `n`-node cluster, one process per node (mirrors
/// `admin_endpoint.rs`'s own `bring_up`), then install the shared-fake
/// export store factory on every node.
async fn bring_up(n: usize, dir: &std::path::Path) -> (Vec<Node>, Arc<FakeS3>) {
    let fake = Arc::new(FakeS3::new(FAKE_BUCKET).with_credential(FAKE_ACCESS_KEY, FAKE_SECRET));
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
            match animusd::run_node(&config, i, dir.join(format!("node-{attempt}-{i}"))).await {
                Ok(node) => nodes.push(node),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            for node in &nodes {
                node.set_export_store_factory(fake_export_store_factory(fake.clone()));
            }
            return (nodes, fake);
        }
        for node in &nodes {
            node.shutdown_graceful().await;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("could not bring up cluster after retries (ports kept getting stolen)");
}

async fn await_bootstrap(nodes: &[Node]) {
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
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

/// One DynamoDB request over a fresh HTTP/1.1 connection → `(status,
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

fn json(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
}

/// A string field of an `ExportTableToPointInTime`/`DescribeExport`
/// response's own `ExportDescription` object.
fn field(body: &str, name: &str) -> String {
    json(body)["ExportDescription"]
        .get(name)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("missing string field `ExportDescription.{name}` in {body}"))
        .to_string()
}

fn tablets_for(meta: &Metadata, table: &str) -> Vec<TabletId> {
    meta.tablets_for_table(table).map(|(&t, _)| t).collect()
}

/// A plain-client-protocol `SplitTablet` call — an **arbitrary binary**
/// `split_key` (murmur hash bytes are not, in general, valid UTF-8, so the
/// admin HTTP surface's JSON-string `split_key` field can't carry one) —
/// mirrors `streams_e2e.rs`'s identical helper.
async fn plain_split(client_addr: SocketAddr, tablet: TabletId, split_key: Vec<u8>) {
    let mut stream = TcpStream::connect(client_addr)
        .await
        .expect("connect to client port");
    write_frame(
        &mut stream,
        &ClientRequest::SplitTablet {
            tablet: tablet.0,
            split_key,
        },
    )
    .await
    .expect("send SplitTablet");
    let resp: ClientResponse = read_frame(&mut stream)
        .await
        .expect("read reply")
        .expect("a reply");
    assert!(
        matches!(resp, ClientResponse::PutOk),
        "plain-protocol split of tablet {} failed: {resp:?}",
        tablet.0
    );
}

/// `CreateTable`, then `PutItem` `n` items (`{"id": {"S": "o{i:05}"}, "body":
/// {"S": "..."}}`) round-robin across the cluster, then force a real,
/// data-driven bootstrap split at the median of the written items' own
/// partition tokens (mirrors `streams_e2e.rs`'s
/// `admin_stream_grow_doubles_a_multi_tablet_table_with_exactly_once_delivery`)
/// — so the exported table genuinely has more than one tablet. Returns the
/// ids written, in write order.
async fn create_table_write_items_and_split(nodes: &[Node], table: &str, n: usize) -> Vec<String> {
    let (status, body) = dynamo(
        nodes[0].dynamo_addr(),
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let id = format!("o{i:05}");
        let issuer = &nodes[i % nodes.len()];
        let (status, body) = dynamo(
            issuer.dynamo_addr(),
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},
                    "body":{{"S":"filler-{i}"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem({id}) failed: {body}");
        ids.push(id);
    }

    let bootstrap_tablet = tablets_for(&nodes[0].metadata(), table)
        .into_iter()
        .next()
        .expect("bootstrap tablet exists");
    let mut tokens: Vec<[u8; 8]> = ids
        .iter()
        .map(|id| partition_token(id.as_bytes()))
        .collect();
    tokens.sort_unstable();
    let median_token = tokens[tokens.len() / 2].to_vec();
    plain_split(nodes[0].client_addr(), bootstrap_tablet, median_token).await;

    timeout(Duration::from_secs(15), async {
        loop {
            if tablets_for(&nodes[0].metadata(), table).len() >= 2 {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("table did not split into at least two tablets");

    ids
}

/// Poll `DescribeExport` until it reports a terminal `ExportStatus`
/// (`COMPLETED`/`FAILED`) or the deadline elapses — converged-or-timeout,
/// never a fixed sleep.
async fn await_export_terminal(addr: SocketAddr, export_arn: &str) -> Value {
    timeout(Duration::from_secs(30), async {
        loop {
            let (status, body) = dynamo(
                addr,
                "DynamoDB_20120810.DescribeExport",
                &format!(r#"{{"ExportArn":"{export_arn}"}}"#),
            )
            .await;
            assert_eq!(status, 200, "DescribeExport failed: {body}");
            let desc = json(&body)["ExportDescription"].clone();
            match desc["ExportStatus"].as_str() {
                Some("COMPLETED") | Some("FAILED") => return desc,
                _ => sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .expect("export did not reach a terminal state in 30s")
}

/// Fetch one object directly out of the shared fake bucket, via the
/// identical `SegmentStore` seam the export job itself uses (no prefix:
/// every key this test reads back is already the object's full path) —
/// used only to verify what the job actually wrote.
async fn get_object(fake: &Arc<FakeS3>, key: &str) -> Option<Vec<u8>> {
    let transport = SharedFakeS3(fake.clone());
    let config = S3Config {
        endpoint: FAKE_ENDPOINT.to_string(),
        bucket: FAKE_BUCKET.to_string(),
        region: FAKE_REGION.to_string(),
        credentials: Credentials::new(FAKE_ACCESS_KEY, FAKE_SECRET),
    };
    let store = animus_env::S3SegmentStore::new(transport, config, None);
    store.get(key).await.expect("get object")
}

/// gunzip `bytes` into a UTF-8 string.
fn gunzip(bytes: &[u8]) -> String {
    let mut decoder = flate2::read::GzDecoder::new(bytes);
    let mut out = String::new();
    decoder
        .read_to_string(&mut out)
        .expect("data file is valid gzip");
    out
}

/// The full happy path: create a table, write items across (after a forced
/// split) two tablets, export it (issued against a **follower-connected**
/// node — ADR 0068's `BeginExport`/`CompleteExport`/`FailExport` relay
/// allowlist regression), poll to `COMPLETED`, then read the fake bucket
/// directly: `manifest-summary.json`'s shape, `manifest-files.json`'s
/// lines, every data file gunzips to `{"Item": ...}` lines that round-trip
/// through `animus_dynamo::wire::decode_item` back to the original items,
/// and the total item count matches what was written.
#[tokio::test(flavor = "multi_thread")]
async fn export_full_flow_completes_and_round_trips_every_item() {
    timeout(Duration::from_secs(90), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, fake) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        let ids = create_table_write_items_and_split(&nodes, "orders", 30).await;
        assert_eq!(
            tablets_for(&nodes[0].metadata(), "orders").len(),
            2,
            "the export must genuinely span more than one tablet"
        );

        // Issue the export against a **follower-connected** node (whichever
        // one is not the control leader) — `MetaCommand::BeginExport` must
        // relay to the control-plane leader.
        let follower = nodes
            .iter()
            .find(|n| !n.is_control_leader())
            .expect("at least one follower exists");
        let table_arn = "arn:aws:dynamodb:animus:0:table/orders";
        let (status, body) = dynamo(
            follower.dynamo_addr(),
            "DynamoDB_20120810.ExportTableToPointInTime",
            &format!(r#"{{"TableArn":"{table_arn}","S3Bucket":"{FAKE_BUCKET}"}}"#),
        )
        .await;
        assert_eq!(status, 200, "ExportTableToPointInTime failed: {body}");
        let export_arn = field(&body, "ExportArn").to_string();
        assert!(export_arn.contains("/export/"), "{export_arn}");
        assert_eq!(field(&body, "ExportStatus"), "IN_PROGRESS");

        let desc = await_export_terminal(follower.dynamo_addr(), &export_arn).await;
        assert_eq!(
            desc["ExportStatus"].as_str(),
            Some("COMPLETED"),
            "export did not complete: {desc}"
        );
        assert_eq!(desc["ItemCount"].as_u64(), Some(ids.len() as u64));
        let manifest_key = desc["ExportManifest"]
            .as_str()
            .expect("ExportManifest present once COMPLETED")
            .to_string();

        // Read the manifest-summary.json object directly out of the fake
        // bucket.
        let summary_bytes = get_object(&fake, &manifest_key)
            .await
            .expect("manifest-summary.json present");
        let summary: Value =
            serde_json::from_slice(&summary_bytes).expect("manifest-summary.json is valid JSON");
        assert_eq!(summary["itemCount"].as_u64(), Some(ids.len() as u64));
        assert_eq!(summary["s3Bucket"].as_str(), Some(FAKE_BUCKET));
        let files_key = summary["manifestFilesS3Key"]
            .as_str()
            .expect("manifestFilesS3Key present")
            .to_string();

        // `manifest-files.json`: one JSON line per data file.
        let files_bytes = get_object(&fake, &files_key)
            .await
            .expect("manifest-files.json present");
        let files_text = String::from_utf8(files_bytes).expect("utf8");
        let mut total_from_files = 0u64;
        let mut data_keys = Vec::new();
        for line in files_text.lines() {
            let entry: Value = serde_json::from_str(line).expect("manifest-files.json line");
            assert!(entry["dataFileS3Key"].is_string());
            assert!(entry["md5Checksum"].is_string());
            assert!(entry["etag"].is_string());
            total_from_files += entry["itemCount"].as_u64().expect("itemCount");
            data_keys.push(entry["dataFileS3Key"].as_str().unwrap().to_string());
        }
        assert_eq!(total_from_files, ids.len() as u64);
        assert!(!data_keys.is_empty(), "at least one data file was written");

        // Every data file gunzips to one `{"Item": {...}}` object per line,
        // and every item round-trips through the real DynamoDB-JSON decoder
        // back to one of the ids this test wrote.
        let mut seen_ids: Vec<String> = Vec::new();
        for key in &data_keys {
            let gz = get_object(&fake, key).await.expect("data file present");
            let text = gunzip(&gz);
            for line in text.lines() {
                let obj: Value = serde_json::from_str(line).expect("data file line is JSON");
                let item_json = obj["Item"]
                    .as_object()
                    .expect("each line carries an `Item` object")
                    .clone();
                let item = animus_dynamo::wire::decode_item(&item_json)
                    .expect("item decodes via the real DynamoDB-JSON decoder");
                let id = item
                    .get("id")
                    .and_then(|v| match v {
                        animus_dynamo::AttributeValue::S(s) => Some(s.clone()),
                        _ => None,
                    })
                    .expect("item has a string `id`");
                seen_ids.push(id);
            }
        }
        seen_ids.sort();
        let mut expected_ids = ids.clone();
        expected_ids.sort();
        assert_eq!(
            seen_ids, expected_ids,
            "every written item was exported exactly once"
        );

        // `_started` was written before any data file.
        let started_key = format!(
            "AWSDynamoDB/{}/_started",
            export_arn.rsplit('/').next().unwrap()
        );
        assert!(
            get_object(&fake, &started_key).await.is_some(),
            "the `_started` marker must exist"
        );

        // `ListExports` (no filter) lists this export.
        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.ListExports",
            "{}",
        )
        .await;
        assert_eq!(status, 200, "ListExports failed: {body}");
        let summaries = json(&body)["ExportSummaries"]
            .as_array()
            .expect("ExportSummaries array")
            .clone();
        assert!(
            summaries
                .iter()
                .any(|s| s["ExportArn"].as_str() == Some(export_arn.as_str())
                    && s["ExportStatus"].as_str() == Some("COMPLETED")),
            "ListExports must include the completed export: {summaries:?}"
        );

        // `ListExports` filtered by `TableArn` also finds it, and filtering
        // by an unrelated table ARN does not.
        let (_, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.ListExports",
            &format!(r#"{{"TableArn":"{table_arn}"}}"#),
        )
        .await;
        let filtered = json(&body)["ExportSummaries"].as_array().unwrap().clone();
        assert!(
            filtered
                .iter()
                .any(|s| s["ExportArn"].as_str() == Some(export_arn.as_str()))
        );
        let (_, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.ListExports",
            r#"{"TableArn":"arn:aws:dynamodb:animus:0:table/nope"}"#,
        )
        .await;
        let excluded = json(&body)["ExportSummaries"].as_array().unwrap().clone();
        assert!(
            !excluded
                .iter()
                .any(|s| s["ExportArn"].as_str() == Some(export_arn.as_str()))
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("export_full_flow_completes_and_round_trips_every_item timed out");
}

/// A repeated `ExportTableToPointInTime` call with the same `ClientToken`
/// and table resolves back to the SAME export (ADR 0068 §5's idempotency
/// contract) rather than minting a second one.
#[tokio::test(flavor = "multi_thread")]
async fn export_client_token_is_idempotent() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _fake) = bring_up(1, dir.path()).await;
        await_bootstrap(&nodes).await;

        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"widgets",
                "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        let table_arn = "arn:aws:dynamodb:animus:0:table/widgets";
        let request_body = format!(
            r#"{{"TableArn":"{table_arn}","S3Bucket":"{FAKE_BUCKET}","ClientToken":"tok-1"}}"#
        );
        let (status, body1) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.ExportTableToPointInTime",
            &request_body,
        )
        .await;
        assert_eq!(status, 200, "first export failed: {body1}");
        let arn1 = field(&body1, "ExportArn").to_string();

        let (status, body2) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.ExportTableToPointInTime",
            &request_body,
        )
        .await;
        assert_eq!(status, 200, "second (idempotent) export failed: {body2}");
        let arn2 = field(&body2, "ExportArn").to_string();
        assert_eq!(
            arn1, arn2,
            "a repeated ClientToken must resolve to the same export"
        );

        // A different token for the same table mints a genuinely new export.
        let other_body = format!(
            r#"{{"TableArn":"{table_arn}","S3Bucket":"{FAKE_BUCKET}","ClientToken":"tok-2"}}"#
        );
        let (status, body3) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.ExportTableToPointInTime",
            &other_body,
        )
        .await;
        assert_eq!(status, 200, "third export failed: {body3}");
        let arn3 = field(&body3, "ExportArn").to_string();
        assert_ne!(
            arn1, arn3,
            "a different ClientToken must mint a distinct export"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("export_client_token_is_idempotent timed out");
}

/// `ExportTableToPointInTime` against an unknown table is
/// `TableNotFoundException`; `DescribeExport` against an unknown ARN is
/// `ExportNotFoundException`.
#[tokio::test(flavor = "multi_thread")]
async fn export_errors_on_unknown_table_and_unknown_export() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _fake) = bring_up(1, dir.path()).await;
        await_bootstrap(&nodes).await;

        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.ExportTableToPointInTime",
            &format!(
                r#"{{"TableArn":"arn:aws:dynamodb:animus:0:table/ghost","S3Bucket":"{FAKE_BUCKET}"}}"#
            ),
        )
        .await;
        assert_eq!(status, 400, "unknown table must be rejected: {body}");
        assert_eq!(
            json(&body)["__type"].as_str().map(|t| t.ends_with("TableNotFoundException")),
            Some(true),
            "{body}"
        );

        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.DescribeExport",
            r#"{"ExportArn":"arn:aws:dynamodb:animus:0:table/ghost/export/nope"}"#,
        )
        .await;
        assert_eq!(status, 400, "unknown export must be rejected: {body}");
        assert_eq!(
            json(&body)["__type"].as_str().map(|t| t.ends_with("ExportNotFoundException")),
            Some(true),
            "{body}"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("export_errors_on_unknown_table_and_unknown_export timed out");
}

/// `ExportFormat: "ION"`/`ExportType: "INCREMENTAL_EXPORT"` are both
/// documented, unimplemented — `ValidationException` at decode time, before
/// anything is ever proposed.
#[tokio::test(flavor = "multi_thread")]
async fn export_rejects_unsupported_format_and_type() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _fake) = bring_up(1, dir.path()).await;
        await_bootstrap(&nodes).await;

        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"orders",
                "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        let table_arn = "arn:aws:dynamodb:animus:0:table/orders";
        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.ExportTableToPointInTime",
            &format!(
                r#"{{"TableArn":"{table_arn}","S3Bucket":"{FAKE_BUCKET}","ExportFormat":"ION"}}"#
            ),
        )
        .await;
        assert_eq!(status, 400, "ION must be rejected: {body}");
        assert_eq!(
            json(&body)["__type"]
                .as_str()
                .map(|t| t.ends_with("ValidationException")),
            Some(true),
            "{body}"
        );

        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.ExportTableToPointInTime",
            &format!(
                r#"{{"TableArn":"{table_arn}","S3Bucket":"{FAKE_BUCKET}",
                    "ExportType":"INCREMENTAL_EXPORT"}}"#
            ),
        )
        .await;
        assert_eq!(status, 400, "INCREMENTAL_EXPORT must be rejected: {body}");
        assert_eq!(
            json(&body)["__type"]
                .as_str()
                .map(|t| t.ends_with("ValidationException")),
            Some(true),
            "{body}"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("export_rejects_unsupported_format_and_type timed out");
}

/// An `ExportTime` on a table with no PITR history at all is
/// `InvalidExportTimeException` (ADR 0068's `validate_export_time`).
#[tokio::test(flavor = "multi_thread")]
async fn export_time_without_pitr_history_is_invalid() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _fake) = bring_up(1, dir.path()).await;
        await_bootstrap(&nodes).await;

        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"orders",
                "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        let table_arn = "arn:aws:dynamodb:animus:0:table/orders";
        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.ExportTableToPointInTime",
            &format!(
                r#"{{"TableArn":"{table_arn}","S3Bucket":"{FAKE_BUCKET}","ExportTime":1700000000}}"#
            ),
        )
        .await;
        assert_eq!(
            status, 400,
            "ExportTime with no PITR history must be rejected: {body}"
        );
        assert_eq!(
            json(&body)["__type"]
                .as_str()
                .map(|t| t.ends_with("InvalidExportTimeException")),
            Some(true),
            "{body}"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("export_time_without_pitr_history_is_invalid timed out");
}
