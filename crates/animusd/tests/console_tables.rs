//! End-to-end tests for the AnimusDB Data Console's tables-list JSON
//! endpoint (`GET /console/api/tables`, ADR 0052 PR2): the projection from
//! the replicated schema catalog is correct for a table with a sort key, one
//! without, one carrying GSIs and LSIs, one with a stream, and one with TTL
//! enabled; a GSI's hidden `<base>$<index>` materialization table (ADR 0041)
//! is excluded; and — the property most worth a regression test — the
//! response carries no node/tablet/replica-shaped field anywhere in it.
//!
//! Tables are created through the real DynamoDB JSON/HTTP wire (the same
//! path an application would use), and read back through the **console**
//! port, never the admin port — this is a console-only surface.
//!
//! Real time + sockets, so it brings the cluster up with the documented
//! port-TOCTOU bounded retry (`support::start_single_node`, itself a
//! fresh-config-per-attempt retry) rather than a fixed-port config.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

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

/// One plain HTTP GET against the **console** listener → `(status,
/// body)`. Mirrors `console_endpoint.rs::raw`, trimmed to what this file
/// needs.
async fn console_get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to console");
    let request = format!("GET {path} HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush");
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await.expect("read response");
    let text = String::from_utf8(bytes).expect("utf8 response");
    let (head, body) = text.split_once("\r\n\r\n").expect("response has a body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line");
    (status, body.to_string())
}

fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
}

/// Fetch the console's tables list and index it by table name for
/// convenient per-table lookups in the assertions below.
async fn fetch_tables(
    console_addr: SocketAddr,
) -> std::collections::BTreeMap<String, serde_json::Value> {
    let (status, body) = console_get(console_addr, "/console/api/tables").await;
    assert_eq!(status, 200, "tables endpoint failed: {body}");
    let value = json(&body);
    value["tables"]
        .as_array()
        .expect("tables is an array")
        .iter()
        .map(|t| (t["name"].as_str().unwrap().to_string(), t.clone()))
        .collect()
}

/// A single node carrying five tables, each exercising one dimension of the
/// projection: a composite key (with a typed sort key), a hash-only key, a
/// GSI+LSI combination, a stream, and TTL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tables_endpoint_projects_the_schema_catalog_correctly() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let dynamo_addr = node.dynamo_addr();
        let console_addr = node.console_addr();

        // ---- a table WITH a sort key (typed N) --------------------------
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"with_sort_key",
                "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"},
                                         {"AttributeName":"ts","AttributeType":"N"}],
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"},
                             {"AttributeName":"ts","KeyType":"RANGE"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable(with_sort_key) failed: {body}");

        // ---- a table WITHOUT a sort key ----------------------------------
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"without_sort_key",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable(without_sort_key) failed: {body}");

        // ---- a table with a GSI AND two LSIs ------------------------------
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"with_indexes",
                "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},
                                         {"AttributeName":"sk","AttributeType":"S"},
                                         {"AttributeName":"cat","AttributeType":"S"},
                                         {"AttributeName":"score","AttributeType":"N"},
                                         {"AttributeName":"rank","AttributeType":"N"}],
                "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                             {"AttributeName":"sk","KeyType":"RANGE"}],
                "GlobalSecondaryIndexes":[
                    {"IndexName":"by-cat",
                     "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}],
                     "Projection":{"ProjectionType":"ALL"}}],
                "LocalSecondaryIndexes":[
                    {"IndexName":"by-score",
                     "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                                  {"AttributeName":"score","KeyType":"RANGE"}]},
                    {"IndexName":"by-rank",
                     "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                                  {"AttributeName":"rank","KeyType":"RANGE"}]}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable(with_indexes) failed: {body}");

        // ---- a table with a stream ----------------------------------------
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"with_stream",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_IMAGE"}}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable(with_stream) failed: {body}");

        // ---- a table with TTL enabled --------------------------------------
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"with_ttl",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable(with_ttl) failed: {body}");
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.UpdateTimeToLive",
            r#"{"TableName":"with_ttl",
                "TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}}"#,
        )
        .await;
        assert_eq!(status, 200, "UpdateTimeToLive(with_ttl) failed: {body}");

        // ---- fetch the console's projection --------------------------------
        let tables = fetch_tables(console_addr).await;
        assert_eq!(
            tables.len(),
            5,
            "exactly the five created tables, nothing hidden or extra: {tables:?}"
        );

        // -- with_sort_key --
        let t = &tables["with_sort_key"];
        assert_eq!(t["partition_key"]["name"], "id");
        assert_eq!(t["partition_key"]["attribute_type"], "S");
        assert_eq!(t["sort_key"]["name"], "ts");
        assert_eq!(t["sort_key"]["attribute_type"], "N");
        assert_eq!(t["gsi_count"], 0, "zero GSIs still renders as 0");
        assert_eq!(
            t["lsi_count"], 0,
            "a sort key is present, so zero LSIs renders as 0, not null"
        );
        assert_eq!(t["stream"]["enabled"], false);
        assert!(t["stream"]["view_type"].is_null());
        assert_eq!(t["ttl"]["enabled"], false);
        assert!(t["ttl"]["attribute_name"].is_null());

        // -- without_sort_key --
        let t = &tables["without_sort_key"];
        assert_eq!(t["partition_key"]["name"], "id");
        assert!(
            t["sort_key"].is_null(),
            "no sort key reads as null, not an empty object"
        );
        assert!(
            t["lsi_count"].is_null(),
            "no sort key means LSIs are structurally impossible: null, not 0"
        );
        assert_eq!(t["gsi_count"], 0);

        // -- with_indexes --
        let t = &tables["with_indexes"];
        assert_eq!(t["gsi_count"], 1, "one GSI (by-cat)");
        assert_eq!(t["lsi_count"], 2, "two LSIs (by-score, by-rank)");
        assert_eq!(t["sort_key"]["name"], "sk");

        // -- with_stream --
        let t = &tables["with_stream"];
        assert_eq!(t["stream"]["enabled"], true);
        assert_eq!(t["stream"]["view_type"], "NEW_IMAGE");
        assert_eq!(t["ttl"]["enabled"], false);

        // -- with_ttl --
        let t = &tables["with_ttl"];
        assert_eq!(t["ttl"]["enabled"], true);
        assert_eq!(t["ttl"]["attribute_name"], "expiresAt");
        assert_eq!(t["stream"]["enabled"], false);

        // ---- the hidden GSI materialization table never appears -----------
        assert!(
            !tables.contains_key("with_indexes$by-cat"),
            "the hidden index table must never appear as a table of its own: {tables:?}"
        );
        for name in tables.keys() {
            assert!(
                !name.contains('$'),
                "no table name in the response names a hidden index table: {name}"
            );
        }

        // ---- no node/tablet/replica-shaped field anywhere in the response -
        let (_, raw_body) = console_get(console_addr, "/console/api/tables").await;
        let lower = raw_body.to_ascii_lowercase();
        for forbidden in [
            "\"node",
            "\"tablet",
            "\"replica",
            "\"raft",
            "\"leader",
            "\"quorum",
            "\"placement",
            "\"health",
            "\"epoch",
        ] {
            assert!(
                !lower.contains(forbidden),
                "found cluster-shaped key `{forbidden}` in the console's tables response: {raw_body}"
            );
        }

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}
