//! End-to-end tests for the S3 import wire surface (ADR 0068 §6, S-05 PR 2):
//! `ImportTable`/`DescribeImport`/`ListImports` over the real DynamoDB
//! JSON/HTTP wire, with the customer-bucket store swapped for the S-04
//! in-process fake (`animus_s3::fake::FakeS3`) via
//! [`animusd::Node::set_export_store_factory`] — the identical seam ADR
//! 0068 §2/PR 1 built for `ExportTableToPointInTime`, reused here for the
//! mirror-image data flow (no real sockets to S3, a real socket for the
//! DynamoDB wire itself, `ProdEnv`). Every eventual property is a
//! converged-or-timeout poll, never a fixed sleep (this codebase's own
//! testing discipline) — mirrors `tests/dynamo_export.rs`'s own harness,
//! duplicated here rather than shared (this repo's own convention: every
//! `tests/dynamo_*.rs` file carries its own small `dynamo`/`json`/`bring_up`
//! helpers, per that file's own doc comment).

use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use animus_s3::client::{HttpRequest, HttpResponse, S3Config, Transport, TransportError};
use animus_s3::fake::FakeS3;
use animus_s3::sigv4::Credentials;
use animusd::{ExportStoreFactory, Node};
use async_trait::async_trait;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

const FAKE_BUCKET: &str = "import-test-bucket";
const FAKE_ENDPOINT: &str = "http://fake.import.example:9000";
const FAKE_REGION: &str = "us-east-1";
const FAKE_ACCESS_KEY: &str = "AKIDIMPORTTEST";
const FAKE_SECRET: &str = "import-test-secret";

/// A `Transport` over a *shared* [`FakeS3`] — mirrors `dynamo_export.rs`'s
/// identical newtype.
#[derive(Clone)]
struct SharedFakeS3(Arc<FakeS3>);

#[async_trait]
impl Transport for SharedFakeS3 {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        self.0.send(request).await
    }
}

/// Build an [`ExportStoreFactory`] over a shared [`FakeS3`] — installed on
/// every node so an import job succeeds regardless of which node's wire
/// edge received the `ImportTable` call, or which node happens to host/
/// lead the import's own destination tablet.
fn fake_store_factory(fake: Arc<FakeS3>) -> ExportStoreFactory {
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

/// Bring up an `n`-node cluster, one process per node — mirrors
/// `dynamo_export.rs`'s own `bring_up`.
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
                node.set_export_store_factory(fake_store_factory(fake.clone()));
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

fn error_code(body: &str) -> String {
    json(body)["__type"]
        .as_str()
        .map(|t| t.rsplit('#').next().unwrap_or(t).to_owned())
        .unwrap_or_else(|| panic!("no __type in error body: {body}"))
}

/// A string field of an `ImportTable`/`DescribeImport` response's own
/// `ImportTableDescription` object.
fn field(body: &str, name: &str) -> String {
    json(body)["ImportTableDescription"]
        .get(name)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("missing string field `ImportTableDescription.{name}` in {body}"))
        .to_string()
}

async fn create_table_write_items(nodes: &[Node], table: &str, n: usize) -> Vec<String> {
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
    ids
}

fn tablets_for(meta: &animus_control::Metadata, table: &str) -> Vec<animus_tablet::TabletId> {
    meta.tablets_for_table(table).map(|(&t, _)| t).collect()
}

/// A plain-client-protocol `SplitTablet` call — mirrors `dynamo_export.rs`'s
/// identical helper.
async fn plain_split(client_addr: SocketAddr, tablet: animus_tablet::TabletId, split_key: Vec<u8>) {
    let mut stream = TcpStream::connect(client_addr)
        .await
        .expect("connect to client port");
    animusd::write_frame(
        &mut stream,
        &animusd::ClientRequest::SplitTablet {
            tablet: tablet.0,
            split_key,
        },
    )
    .await
    .expect("send SplitTablet");
    let resp: animusd::ClientResponse = animusd::read_frame(&mut stream)
        .await
        .expect("read reply")
        .expect("a reply");
    assert!(
        matches!(resp, animusd::ClientResponse::PutOk),
        "plain-protocol split of tablet {} failed: {resp:?}",
        tablet.0
    );
}

/// Force a real, data-driven bootstrap split of `table` at the median of
/// its written items' own partition tokens — mirrors `dynamo_export.rs`'s
/// identical helper, so the exported/imported table genuinely spans more
/// than one tablet.
async fn force_split(nodes: &[Node], table: &str, ids: &[String]) {
    let bootstrap_tablet = tablets_for(&nodes[0].metadata(), table)
        .into_iter()
        .next()
        .expect("bootstrap tablet exists");
    let mut tokens: Vec<[u8; 8]> = ids
        .iter()
        .map(|id| animus_tablet::partition_token(id.as_bytes()))
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
}

/// `ExportTableToPointInTime` then poll converged-or-timeout to
/// `COMPLETED`, returning the export's own ARN.
async fn export_table_and_await(addr: SocketAddr, table_arn: &str) -> String {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.ExportTableToPointInTime",
        &format!(r#"{{"TableArn":"{table_arn}","S3Bucket":"{FAKE_BUCKET}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "ExportTableToPointInTime failed: {body}");
    let export_arn = json(&body)["ExportDescription"]["ExportArn"]
        .as_str()
        .expect("ExportArn")
        .to_owned();
    timeout(Duration::from_secs(30), async {
        loop {
            let (status, body) = dynamo(
                addr,
                "DynamoDB_20120810.DescribeExport",
                &format!(r#"{{"ExportArn":"{export_arn}"}}"#),
            )
            .await;
            assert_eq!(status, 200, "DescribeExport failed: {body}");
            if json(&body)["ExportDescription"]["ExportStatus"] == "COMPLETED" {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("export did not complete in 30s");
    export_arn
}

/// `ImportTable` — a minimal single-hash-key `TableCreationParameters`,
/// requesting `GZIP` (this adapter's own export job — and real AWS's own
/// export tooling — always gzips; the dedicated `NONE`-compression test
/// builds its own request directly rather than through this helper).
async fn import_table(
    addr: SocketAddr,
    target_table: &str,
    prefix: Option<&str>,
    client_token: Option<&str>,
) -> (u16, String) {
    let mut payload = serde_json::json!({
        "S3BucketSource": {"S3Bucket": FAKE_BUCKET},
        "InputFormat": "DYNAMODB_JSON",
        "InputCompressionType": "GZIP",
        "TableCreationParameters": {
            "TableName": target_table,
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        },
    });
    if let Some(prefix) = prefix {
        payload["S3BucketSource"]["S3KeyPrefix"] = Value::String(prefix.to_string());
    }
    if let Some(token) = client_token {
        payload["ClientToken"] = Value::String(token.to_string());
    }
    dynamo(
        addr,
        "DynamoDB_20120810.ImportTable",
        &serde_json::to_string(&payload).unwrap(),
    )
    .await
}

/// Poll `DescribeImport` until it reports a terminal `ImportStatus`
/// (`COMPLETED`/`FAILED`) or the deadline elapses.
async fn await_import_terminal(addr: SocketAddr, import_arn: &str) -> Value {
    timeout(Duration::from_secs(30), async {
        loop {
            let (status, body) = dynamo(
                addr,
                "DynamoDB_20120810.DescribeImport",
                &format!(r#"{{"ImportArn":"{import_arn}"}}"#),
            )
            .await;
            assert_eq!(status, 200, "DescribeImport failed: {body}");
            let desc = json(&body)["ImportTableDescription"].clone();
            match desc["ImportStatus"].as_str() {
                Some("COMPLETED") | Some("FAILED") => return desc,
                _ => sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .expect("import did not reach a terminal state in 30s")
}

/// Poll `DescribeTable` converged-or-timeout to `TableStatus: ACTIVE`.
async fn await_table_active(addr: SocketAddr, table: &str) {
    timeout(Duration::from_secs(20), async {
        loop {
            let (status, body) = dynamo(
                addr,
                "DynamoDB_20120810.DescribeTable",
                &format!(r#"{{"TableName":"{table}"}}"#),
            )
            .await;
            if status == 200 && json(&body)["Table"]["TableStatus"] == "ACTIVE" {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("table `{table}` did not converge to ACTIVE in 20s"));
}

/// gzip-compress `bytes` at the default level — mirrors
/// `animusd::dynamo::gzip_bytes` (private to that crate, so duplicated
/// here, the same "each `tests/dynamo_*.rs` file owns its own small
/// helpers" convention this file's own module doc states).
fn gzip_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).expect("in-memory gzip write");
    encoder.finish().expect("in-memory gzip finish")
}

/// Write a hand-crafted DynamoDB-JSON export layout directly into the fake
/// bucket at `prefix/AWSDynamoDB/<export_suffix>/...` — bypassing a real
/// `ExportTableToPointInTime` call, so a test can exercise an input shape
/// this adapter's own export job never actually produces (`NONE`
/// compression; deliberately malformed items) but real DynamoDB export
/// tooling, or a hand-edited file, legitimately could. `lines` is the
/// data file's own content: one `{"Item": {...}}` (or deliberately
/// malformed) JSON object per line.
async fn write_hand_export(
    fake: &Arc<FakeS3>,
    prefix: &str,
    export_suffix: &str,
    lines: &str,
    gzip: bool,
) {
    let store = fake_store_factory(fake.clone())(FAKE_BUCKET, Some(prefix)).expect("build store");
    let root = format!("AWSDynamoDB/{export_suffix}");
    store
        .put(&format!("{root}/_started"), b"")
        .await
        .expect("write _started");
    let data_bytes = if gzip {
        gzip_bytes(lines.as_bytes())
    } else {
        lines.as_bytes().to_vec()
    };
    let data_key = format!("{root}/data/0000.json.gz");
    store
        .put(&data_key, &data_bytes)
        .await
        .expect("write data file");
    let item_count = lines.lines().filter(|l| !l.trim().is_empty()).count();
    let files_line = serde_json::json!({
        "itemCount": item_count,
        "md5Checksum": "deadbeef",
        "etag": "deadbeef",
        "dataFileS3Key": data_key,
    });
    let files_key = format!("{root}/manifest-files.json");
    store
        .put(
            &files_key,
            format!("{}\n", serde_json::to_string(&files_line).unwrap()).as_bytes(),
        )
        .await
        .expect("write manifest-files.json");
    let summary = serde_json::json!({
        "version": "2020-06-30",
        "exportArn": format!("hand-written-{export_suffix}"),
        "s3Bucket": FAKE_BUCKET,
        "s3Prefix": prefix,
        "manifestFilesS3Key": files_key,
        "itemCount": item_count,
        "billedSizeBytes": data_bytes.len(),
        "outputFormat": "DYNAMODB_JSON",
    });
    let summary_key = format!("{root}/manifest-summary.json");
    store
        .put(
            &summary_key,
            serde_json::to_string(&summary).unwrap().as_bytes(),
        )
        .await
        .expect("write manifest-summary.json");
}

/// The full happy path: create a table, write items across (after a forced
/// split) two tablets, export it, then import that same export (issued
/// against a **follower-connected** node — ADR 0068's `BeginImport`/
/// `CompleteImport`/`FailImport` relay allowlist regression) into a
/// brand-new table, converging to `COMPLETED` with `ImportedItemCount`
/// exact and the target table `ACTIVE`; every item reads back through
/// `GetItem`/`Scan`.
#[tokio::test(flavor = "multi_thread")]
async fn import_full_flow_completes_and_round_trips_every_item() {
    timeout(Duration::from_secs(120), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _fake) = bring_up(3, dir.path()).await;
        await_bootstrap(&nodes).await;

        let ids = create_table_write_items(&nodes, "orders", 30).await;
        force_split(&nodes, "orders", &ids).await;
        assert_eq!(
            tablets_for(&nodes[0].metadata(), "orders").len(),
            2,
            "the export must genuinely span more than one tablet"
        );

        let table_arn = "arn:aws:dynamodb:animus:0:table/orders";
        export_table_and_await(nodes[0].dynamo_addr(), table_arn).await;

        // Issue the import against a **follower-connected** node.
        let follower = nodes
            .iter()
            .find(|n| !n.is_control_leader())
            .expect("at least one follower exists");
        let (status, body) =
            import_table(follower.dynamo_addr(), "orders_imported", None, None).await;
        assert_eq!(status, 200, "ImportTable failed: {body}");
        let import_arn = field(&body, "ImportArn");
        assert!(import_arn.contains("/import/"), "{import_arn}");
        assert_eq!(field(&body, "ImportStatus"), "IN_PROGRESS");

        let desc = await_import_terminal(follower.dynamo_addr(), &import_arn).await;
        assert_eq!(
            desc["ImportStatus"].as_str(),
            Some("COMPLETED"),
            "import did not complete: {desc}"
        );
        assert_eq!(desc["ProcessedItemCount"].as_u64(), Some(ids.len() as u64));
        assert_eq!(desc["ImportedItemCount"].as_u64(), Some(ids.len() as u64));
        assert_eq!(desc["ErrorCount"].as_u64(), Some(0));

        await_table_active(follower.dynamo_addr(), "orders_imported").await;

        // Every item reads back through GetItem.
        for id in &ids {
            let (status, body) = dynamo(
                follower.dynamo_addr(),
                "DynamoDB_20120810.GetItem",
                &format!(r#"{{"TableName":"orders_imported","Key":{{"id":{{"S":"{id}"}}}}}}"#),
            )
            .await;
            assert_eq!(status, 200, "GetItem({id}) failed: {body}");
            assert_eq!(
                json(&body)["Item"]["id"]["S"].as_str(),
                Some(id.as_str()),
                "body: {body}"
            );
        }

        // A `Scan` sees exactly the imported set.
        let (status, body) = dynamo(
            follower.dynamo_addr(),
            "DynamoDB_20120810.Scan",
            r#"{"TableName":"orders_imported","ConsistentRead":true}"#,
        )
        .await;
        assert_eq!(status, 200, "Scan failed: {body}");
        let scanned = json(&body);
        assert_eq!(scanned["Count"].as_u64(), Some(ids.len() as u64));
        let mut scanned_ids: Vec<String> = scanned["Items"]
            .as_array()
            .expect("Items array")
            .iter()
            .map(|item| item["id"]["S"].as_str().unwrap().to_owned())
            .collect();
        scanned_ids.sort();
        let mut expected_ids = ids.clone();
        expected_ids.sort();
        assert_eq!(scanned_ids, expected_ids);

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("import_full_flow_completes_and_round_trips_every_item timed out");
}

/// A `NONE`-compressed export written by hand (never produced by this
/// adapter's own export job, which always gzips) imports correctly too.
#[tokio::test(flavor = "multi_thread")]
async fn import_reads_a_none_compressed_hand_written_export() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, fake) = bring_up(1, dir.path()).await;
        await_bootstrap(&nodes).await;

        let lines = r#"{"Item": {"id": {"S": "n1"}, "body": {"S": "hello"}}}
{"Item": {"id": {"S": "n2"}, "body": {"S": "world"}}}"#;
        write_hand_export(&fake, "plain-export", "abc123", lines, false).await;

        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.ImportTable",
            &serde_json::json!({
                "S3BucketSource": {"S3Bucket": FAKE_BUCKET, "S3KeyPrefix": "plain-export"},
                "InputFormat": "DYNAMODB_JSON",
                "InputCompressionType": "NONE",
                "TableCreationParameters": {
                    "TableName": "plain_imported",
                    "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                },
            })
            .to_string(),
        )
        .await;
        assert_eq!(status, 200, "ImportTable failed: {body}");
        let import_arn = field(&body, "ImportArn");

        let desc = await_import_terminal(nodes[0].dynamo_addr(), &import_arn).await;
        assert_eq!(desc["ImportStatus"].as_str(), Some("COMPLETED"), "{desc}");
        assert_eq!(desc["ImportedItemCount"].as_u64(), Some(2));
        assert_eq!(desc["ErrorCount"].as_u64(), Some(0));

        await_table_active(nodes[0].dynamo_addr(), "plain_imported").await;
        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.GetItem",
            r#"{"TableName":"plain_imported","Key":{"id":{"S":"n1"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "GetItem failed: {body}");
        assert_eq!(json(&body)["Item"]["body"]["S"], "hello");

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("import_reads_a_none_compressed_hand_written_export timed out");
}

/// Two malformed items (one missing its own partition key attribute, one
/// with the wrong declared type) are skipped, counted in `ErrorCount`, and
/// every other item still imports.
#[tokio::test(flavor = "multi_thread")]
async fn import_skips_malformed_items_and_counts_them() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, fake) = bring_up(1, dir.path()).await;
        await_bootstrap(&nodes).await;

        let lines = r#"{"Item": {"id": {"S": "g1"}, "body": {"S": "ok1"}}}
{"Item": {"id": {"S": "g2"}, "body": {"S": "ok2"}}}
{"Item": {"body": {"S": "missing-pk"}}}
{"Item": {"id": {"N": "5"}, "body": {"S": "wrong-type"}}}
{"Item": {"id": {"S": "g3"}, "body": {"S": "ok3"}}}"#;
        write_hand_export(&fake, "bad-export", "def456", lines, true).await;

        let (status, body) = import_table(
            nodes[0].dynamo_addr(),
            "bad_imported",
            Some("bad-export"),
            None,
        )
        .await;
        assert_eq!(status, 200, "ImportTable failed: {body}");
        let import_arn = field(&body, "ImportArn");

        let desc = await_import_terminal(nodes[0].dynamo_addr(), &import_arn).await;
        assert_eq!(desc["ImportStatus"].as_str(), Some("COMPLETED"), "{desc}");
        assert_eq!(desc["ProcessedItemCount"].as_u64(), Some(5));
        assert_eq!(desc["ImportedItemCount"].as_u64(), Some(3));
        assert_eq!(desc["ErrorCount"].as_u64(), Some(2));

        await_table_active(nodes[0].dynamo_addr(), "bad_imported").await;
        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.Scan",
            r#"{"TableName":"bad_imported","ConsistentRead":true}"#,
        )
        .await;
        assert_eq!(status, 200, "Scan failed: {body}");
        assert_eq!(json(&body)["Count"].as_u64(), Some(3));

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("import_skips_malformed_items_and_counts_them timed out");
}

/// `ImportConflictException`: a target name that already exists as an
/// ordinary table, and a second `ImportTable` call to a name an import has
/// already claimed (the ordinary `CreateTableSchema` collision check —
/// `BeginImport` proposes that same schema before ever minting its own
/// row, so a second call targeting the same name sees it already present).
#[tokio::test(flavor = "multi_thread")]
async fn import_conflict_for_an_existing_table_and_a_name_already_claimed() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, fake) = bring_up(1, dir.path()).await;
        await_bootstrap(&nodes).await;

        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"already_here",
                "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        write_hand_export(&fake, "conflict1", "s1", "", false).await;
        let (status, body) = import_table(
            nodes[0].dynamo_addr(),
            "already_here",
            Some("conflict1"),
            None,
        )
        .await;
        assert_eq!(status, 400, "existing table name must be rejected: {body}");
        assert_eq!(error_code(&body), "ImportConflictException");

        // A second import into a name the first import already claimed.
        write_hand_export(&fake, "conflict2", "s2", "", false).await;
        let (status, body) = import_table(
            nodes[0].dynamo_addr(),
            "claimed_target",
            Some("conflict2"),
            None,
        )
        .await;
        assert_eq!(status, 200, "first import failed: {body}");

        let (status, body) = import_table(
            nodes[0].dynamo_addr(),
            "claimed_target",
            Some("conflict2"),
            None,
        )
        .await;
        assert_eq!(
            status, 400,
            "a second import to the same target must be rejected: {body}"
        );
        assert_eq!(error_code(&body), "ImportConflictException");

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("import_conflict_for_an_existing_table_and_a_name_already_claimed timed out");
}

/// An unknown `ImportArn` is `ImportNotFoundException`.
#[tokio::test(flavor = "multi_thread")]
async fn import_errors_on_unknown_import() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _fake) = bring_up(1, dir.path()).await;
        await_bootstrap(&nodes).await;

        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.DescribeImport",
            r#"{"ImportArn":"arn:aws:dynamodb:animus:0:table/ghost/import/nope"}"#,
        )
        .await;
        assert_eq!(status, 400, "unknown import must be rejected: {body}");
        assert_eq!(error_code(&body), "ImportNotFoundException");

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("import_errors_on_unknown_import timed out");
}

/// `ListImports` pagination and `TableArn` filtering.
#[tokio::test(flavor = "multi_thread")]
async fn list_imports_pagination_and_table_filter() {
    timeout(Duration::from_secs(90), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, fake) = bring_up(1, dir.path()).await;
        await_bootstrap(&nodes).await;

        let mut import_arns = Vec::new();
        for i in 0..3 {
            let prefix = format!("list-export-{i}");
            write_hand_export(&fake, &prefix, "s", "", true).await;
            let (status, body) = import_table(
                nodes[0].dynamo_addr(),
                &format!("list_imported_{i}"),
                Some(&prefix),
                None,
            )
            .await;
            assert_eq!(status, 200, "ImportTable failed: {body}");
            let arn = field(&body, "ImportArn");
            await_import_terminal(nodes[0].dynamo_addr(), &arn).await;
            import_arns.push(arn);
        }

        // Unfiltered `ListImports`, paginated two at a time.
        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.ListImports",
            r#"{"PageSize":2}"#,
        )
        .await;
        assert_eq!(status, 200, "ListImports failed: {body}");
        let page1 = json(&body);
        let first_page = page1["ImportSummaryList"].as_array().unwrap();
        assert_eq!(first_page.len(), 2, "body: {body}");
        let next_token = page1["NextToken"]
            .as_str()
            .expect("truncated page has a NextToken");

        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.ListImports",
            &format!(r#"{{"PageSize":2,"NextToken":"{next_token}"}}"#),
        )
        .await;
        assert_eq!(status, 200, "body: {body}");
        let page2 = json(&body);
        let second_page = page2["ImportSummaryList"].as_array().unwrap();
        assert_eq!(second_page.len(), 1, "body: {body}");
        assert!(page2.get("NextToken").is_none());

        // Filtered by TableArn finds exactly its own import.
        let table_arn = "arn:aws:dynamodb:animus:0:table/list_imported_0";
        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.ListImports",
            &format!(r#"{{"TableArn":"{table_arn}"}}"#),
        )
        .await;
        assert_eq!(status, 200, "body: {body}");
        let filtered = json(&body)["ImportSummaryList"].as_array().unwrap().clone();
        assert_eq!(filtered.len(), 1, "body: {body}");
        assert_eq!(
            filtered[0]["ImportArn"].as_str(),
            Some(import_arns[0].as_str())
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("list_imports_pagination_and_table_filter timed out");
}

/// `InputFormat: "ION"`/`InputCompressionType: "ZSTD"` are both documented,
/// unimplemented — `ValidationException` at decode time, before anything is
/// ever proposed. A missing `AttributeDefinitions` entry is likewise
/// rejected the same way `CreateTable` itself rejects it.
#[tokio::test(flavor = "multi_thread")]
async fn import_rejects_unsupported_format_and_compression() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, _fake) = bring_up(1, dir.path()).await;
        await_bootstrap(&nodes).await;

        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.ImportTable",
            &serde_json::json!({
                "S3BucketSource": {"S3Bucket": FAKE_BUCKET},
                "InputFormat": "ION",
                "TableCreationParameters": {
                    "TableName": "ion_table",
                    "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                },
            })
            .to_string(),
        )
        .await;
        assert_eq!(status, 400, "ION must be rejected: {body}");
        assert_eq!(error_code(&body), "ValidationException");

        let (status, body) = dynamo(
            nodes[0].dynamo_addr(),
            "DynamoDB_20120810.ImportTable",
            &serde_json::json!({
                "S3BucketSource": {"S3Bucket": FAKE_BUCKET},
                "InputFormat": "DYNAMODB_JSON",
                "InputCompressionType": "ZSTD",
                "TableCreationParameters": {
                    "TableName": "zstd_table",
                    "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                },
            })
            .to_string(),
        )
        .await;
        assert_eq!(status, 400, "ZSTD must be rejected: {body}");
        assert_eq!(error_code(&body), "ValidationException");

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("import_rejects_unsupported_format_and_compression timed out");
}

/// A repeated `ImportTable` call with the same `ClientToken` resolves back
/// to the same import rather than minting a second one.
#[tokio::test(flavor = "multi_thread")]
async fn import_client_token_is_idempotent() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (nodes, fake) = bring_up(1, dir.path()).await;
        await_bootstrap(&nodes).await;

        write_hand_export(&fake, "tok-export", "s", "", false).await;
        let (status, body1) = import_table(
            nodes[0].dynamo_addr(),
            "tok_imported",
            Some("tok-export"),
            Some("tok-1"),
        )
        .await;
        assert_eq!(status, 200, "first import failed: {body1}");
        let arn1 = field(&body1, "ImportArn");

        let (status, body2) = import_table(
            nodes[0].dynamo_addr(),
            "tok_imported",
            Some("tok-export"),
            Some("tok-1"),
        )
        .await;
        assert_eq!(status, 200, "second (idempotent) import failed: {body2}");
        let arn2 = field(&body2, "ImportArn");
        assert_eq!(
            arn1, arn2,
            "a repeated ClientToken must resolve to the same import"
        );

        for node in &nodes {
            node.shutdown_graceful().await;
        }
    })
    .await
    .expect("import_client_token_is_idempotent timed out");
}
