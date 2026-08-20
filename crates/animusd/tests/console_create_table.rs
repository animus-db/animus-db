//! End-to-end tests for the AnimusDB Data Console's create-table form (ADR
//! 0052, the stack's final PR): `POST /console/api/tables` — table name,
//! partition key, an optional sort key, LSIs (declarable **only** here),
//! GSIs (with a projection), a stream, and TTL, all in one call — through
//! the **console** port, never the admin port, exactly like every other
//! `console_*.rs` file in this crate. The property most worth a regression
//! test (again): no node/tablet/replica-shaped field anywhere in any
//! response.
//!
//! Unlike `console_table_config.rs`/`console_items.rs`, tables here are
//! created **through the console port itself** (that is what this PR ships)
//! rather than the raw DynamoDB wire — the DynamoDB wire is used only to
//! confirm the console's `CreateTable` actually reaches the real data plane
//! (a `PutItem`/`GetItem` round trip against a console-created table).
//!
//! Real time + sockets, so it brings the cluster up with the documented
//! port-TOCTOU bounded retry (`support::start_single_node`).

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

mod support;

/// One DynamoDB JSON request over a fresh HTTP/1.1 connection → `(status,
/// body)`. Mirrors every other `tests/dynamo_*.rs`/`console_*.rs` file's
/// identical helper.
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

/// One request against the **console** listener with an arbitrary method
/// and (optional) JSON body → `(status, body)`. Identical to
/// `console_table_config.rs`/`console_items.rs`'s own helper.
async fn console(addr: SocketAddr, method: &str, path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to console");
    let request = format!(
        "{method} {path} HTTP/1.1\r\n\
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

fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
}

/// No node/tablet/replica/raft/leader/quorum/placement/health/epoch-shaped
/// key anywhere in `body` — the same forbidden-substring list every other
/// `console_*.rs` file checks every response against.
fn assert_no_cluster_shape(body: &str) {
    let lower = body.to_ascii_lowercase();
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
            "found cluster-shaped key `{forbidden}` in the console's response: {body}"
        );
    }
}

/// A minimal create (partition key only, the common case) succeeds and the
/// new table shows up in the tables-list endpoint with the declared key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_minimal_table_appears_in_tables_list() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let console_addr = node.console_addr();
        let dynamo_addr = node.dynamo_addr();

        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables",
            r#"{"table_name":"simple","partition_key":{"name":"id","attribute_type":"S"}}"#,
        )
        .await;
        assert_eq!(status, 201, "create_table failed: {body}");
        let resp = json(&body);
        assert_eq!(resp["table"]["name"], "simple");
        assert_eq!(resp["table"]["partition_key"]["name"], "id");
        assert_eq!(resp["table"]["partition_key"]["attribute_type"], "S");
        assert!(resp["table"]["sort_key"].is_null());
        assert!(resp["table"]["gsis"].as_array().unwrap().is_empty());
        assert!(resp["table"]["lsis"].as_array().unwrap().is_empty());
        assert_eq!(resp["table"]["stream"]["enabled"], false);
        assert_eq!(resp["table"]["ttl"]["enabled"], false);
        assert_no_cluster_shape(&body);

        let (status, body) = console(console_addr, "GET", "/console/api/tables", "").await;
        assert_eq!(status, 200);
        let tables = json(&body)["tables"].as_array().unwrap().clone();
        let simple = tables
            .iter()
            .find(|t| t["name"] == "simple")
            .unwrap_or_else(|| panic!("`simple` missing from the tables list: {body}"));
        assert_eq!(simple["partition_key"]["name"], "id");
        assert!(simple["sort_key"].is_null());
        assert!(
            simple["lsi_count"].is_null(),
            "a hash-only table structurally has no LSI count: {body}"
        );
        assert_no_cluster_shape(&body);

        // The table the console created is a real, working table — prove it
        // over the real DynamoDB wire, not just the catalog projection.
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.PutItem",
            r#"{"TableName":"simple","Item":{"id":{"S":"x1"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "PutItem on a console-created table: {body}");
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.GetItem",
            r#"{"TableName":"simple","Key":{"id":{"S":"x1"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "GetItem on a console-created table: {body}");
        assert_eq!(json(&body)["Item"]["id"]["S"], "x1");

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// A full declaration — sort key, an LSI, a GSI (with an `INCLUDE`
/// projection), a stream, and TTL — survives **exactly as declared**,
/// verified through a fresh `GET /console/api/tables/{name}`, not just the
/// create call's own echoed response. This is the test that would have
/// caught issue #319 on the *create* path: tracing `CreateTable`'s decoder
/// (`animus_dynamo::wire::decode_key_schema`/`decode_attribute_types`, the
/// `schema` bridge's `to_control`/`index_to_control`) found that an index's
/// own key attribute gets **no** recorded type even when the index is
/// declared at `CreateTable` time — `to_control` only ever builds a
/// `ColumnDef` for the base table's own partition/sort key, and
/// `index_to_control` never receives `key_types` at all. So both the GSI's
/// hash/sort attributes and the LSI's own alternate sort attribute (all
/// three chosen here to be distinct from the base table's own key names,
/// so nothing resolves a type by pure name coincidence) must read back with
/// `attribute_type: null` — the console honestly reporting an absence, not
/// a fabricated `"S"`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_full_table_declares_everything_exactly() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let console_addr = node.console_addr();

        let req = r#"{
            "table_name": "orders",
            "partition_key": {"name": "order_id", "attribute_type": "S"},
            "sort_key": {"name": "created_at", "attribute_type": "N"},
            "lsis": [{"index_name": "by-score", "sort_attribute": "score"}],
            "gsis": [{
                "index_name": "by-region",
                "hash_attribute": "region",
                "sort_attribute": "priority",
                "projection_type": "INCLUDE",
                "projection_non_key_attributes": ["total"]
            }],
            "stream_enabled": true,
            "stream_view_type": "NEW_AND_OLD_IMAGES",
            "ttl_enabled": true,
            "ttl_attribute_name": "expiresAt"
        }"#;
        let (status, body) = console(console_addr, "POST", "/console/api/tables", req).await;
        assert_eq!(status, 201, "create_table failed: {body}");
        assert_no_cluster_shape(&body);

        // Re-fetch fresh rather than trusting only the create call's own
        // echoed response — the property this test exists to pin is what
        // the catalog actually recorded, not what `create_table` computed
        // in memory before returning.
        let (status, body) = console(console_addr, "GET", "/console/api/tables/orders", "").await;
        assert_eq!(status, 200, "table detail failed: {body}");
        let d = json(&body);

        assert_eq!(d["name"], "orders");
        assert_eq!(d["partition_key"]["name"], "order_id");
        assert_eq!(d["partition_key"]["attribute_type"], "S");
        assert_eq!(d["sort_key"]["name"], "created_at");
        assert_eq!(d["sort_key"]["attribute_type"], "N");

        let lsis = d["lsis"].as_array().unwrap();
        assert_eq!(lsis.len(), 1);
        assert_eq!(lsis[0]["name"], "by-score");
        assert_eq!(lsis[0]["sort_attribute"]["name"], "score");
        assert!(
            lsis[0]["sort_attribute"]["attribute_type"].is_null(),
            "an LSI's own alternate sort attribute has no recorded type, even \
             declared at CreateTable time: {body}"
        );
        assert!(
            lsis[0].get("status").is_none(),
            "an LSI row carries no lifecycle status field: {body}"
        );

        let gsis = d["gsis"].as_array().unwrap();
        assert_eq!(gsis.len(), 1);
        assert_eq!(gsis[0]["name"], "by-region");
        assert_eq!(gsis[0]["hash_attribute"]["name"], "region");
        assert!(
            gsis[0]["hash_attribute"]["attribute_type"].is_null(),
            "a GSI's hash attribute has no recorded type, even declared at \
             CreateTable time: {body}"
        );
        assert_eq!(gsis[0]["sort_attribute"]["name"], "priority");
        assert!(gsis[0]["sort_attribute"]["attribute_type"].is_null());
        assert_eq!(
            gsis[0]["status"], "ACTIVE",
            "a CreateTable-declared index is Active immediately (ADR 0041 §5, \
             `schema::index_to_control`) — never CREATING, unlike a later \
             UpdateTable-added GSI on a populated table"
        );
        assert_eq!(gsis[0]["projection"]["projection_type"], "INCLUDE");
        assert_eq!(
            gsis[0]["projection"]["non_key_attributes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["total"]
        );

        assert_eq!(d["stream"]["enabled"], true);
        assert_eq!(d["stream"]["view_type"], "NEW_AND_OLD_IMAGES");
        assert_eq!(d["ttl"]["enabled"], true);
        assert_eq!(d["ttl"]["attribute_name"], "expiresAt");
        assert_no_cluster_shape(&body);

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// Creating a table whose name is already taken returns a client error
/// (DynamoDB's own `ResourceInUseException`, mapped to a `4xx`), never a
/// `500` — and the original table is left exactly as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_table_rejects_a_duplicate_name() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let console_addr = node.console_addr();

        let req = r#"{"table_name":"dup","partition_key":{"name":"id","attribute_type":"S"}}"#;
        let (status, body) = console(console_addr, "POST", "/console/api/tables", req).await;
        assert_eq!(status, 201, "first create failed: {body}");

        // A second create under the same name, with a *different* shape
        // (a sort key this time) — proves the rejection, not merely an
        // idempotent no-op that happened to look like success.
        let dup_req = r#"{
            "table_name": "dup",
            "partition_key": {"name": "id", "attribute_type": "S"},
            "sort_key": {"name": "sk", "attribute_type": "S"}
        }"#;
        let (status, body) = console(console_addr, "POST", "/console/api/tables", dup_req).await;
        assert!(
            (400..500).contains(&status),
            "duplicate create must be a client error, not a {status}: {body}"
        );
        assert!(!json(&body)["error"].as_str().unwrap_or_default().is_empty());
        assert_no_cluster_shape(&body);

        // The original table is untouched — still no sort key.
        let (status, body) = console(console_addr, "GET", "/console/api/tables/dup", "").await;
        assert_eq!(status, 200, "original table gone: {body}");
        assert!(json(&body)["sort_key"].is_null());

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// An LSI with no sort key — either because the LSI's own `sort_attribute`
/// is blank, or because the table itself declares no sort key at all while
/// still trying to declare one — is rejected with a sensible client error,
/// never a `500`, and never left half-created.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_table_rejects_an_lsi_with_no_sort_key() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let console_addr = node.console_addr();

        // The LSI's own `sort_attribute` is blank.
        let req = r#"{
            "table_name": "no_lsi_sort",
            "partition_key": {"name": "id", "attribute_type": "S"},
            "sort_key": {"name": "sk", "attribute_type": "S"},
            "lsis": [{"index_name": "broken", "sort_attribute": ""}]
        }"#;
        let (status, body) = console(console_addr, "POST", "/console/api/tables", req).await;
        assert!(
            (400..500).contains(&status),
            "an LSI with a blank sort attribute must be a client error, not a {status}: {body}"
        );
        assert!(!json(&body)["error"].as_str().unwrap_or_default().is_empty());
        assert_no_cluster_shape(&body);
        let (status, _) = console(console_addr, "GET", "/console/api/tables/no_lsi_sort", "").await;
        assert_eq!(
            status, 404,
            "rejected create must not leave a half-made table"
        );

        // The table itself has no sort key at all, but still declares an LSI.
        let req = r#"{
            "table_name": "no_table_sort",
            "partition_key": {"name": "id", "attribute_type": "S"},
            "lsis": [{"index_name": "broken", "sort_attribute": "score"}]
        }"#;
        let (status, body) = console(console_addr, "POST", "/console/api/tables", req).await;
        assert!(
            (400..500).contains(&status),
            "an LSI on a sort-key-less table must be a client error, not a {status}: {body}"
        );
        assert_no_cluster_shape(&body);
        let (status, _) =
            console(console_addr, "GET", "/console/api/tables/no_table_sort", "").await;
        assert_eq!(
            status, 404,
            "rejected create must not leave a half-made table"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}
