//! End-to-end tests for animusd console's (ADR 0052's "AnimusDB Data
//! Console") table page Items tab
//! (ADR 0052 PR4): `Scan` (paginated), `Query` by partition key, and the
//! `GetItem`/`PutItem`/`DeleteItem` round trip — all through the
//! **console** port, never the admin port, modeled on
//! `tests/console_table_config.rs`. The property most worth a regression
//! test (again): no node/tablet/replica-shaped field anywhere in any
//! response.
//!
//! Tables are created and populated through the real DynamoDB JSON/HTTP
//! wire so the fixtures match what an application would actually declare;
//! only the Items tab's own reads/writes go through the console port.
//!
//! Real time + sockets, so it brings the cluster up with the documented
//! port-TOCTOU bounded retry (`support::start_single_node`).

use std::collections::BTreeSet;
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
/// `console_table_config.rs`'s own helper.
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
/// key anywhere in `body` — the same forbidden-substring list
/// `console_table_config.rs` checks every Config tab response against,
/// reused here for every Items tab response.
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

/// `POST /console/api/tables/{name}/items/scan` returns the items a real
/// `Scan` would, and paginates with `ExclusiveStartKey`/`LastEvaluatedKey`
/// exactly as DynamoDB's own contract requires: walking every page (via
/// `last_evaluated_key`, never a fake offset) visits every row exactly
/// once, not just "page 2 is non-empty".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_paginates_and_visits_every_item_exactly_once() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let dynamo_addr = node.dynamo_addr();
        let console_addr = node.console_addr();

        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"widgets",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        let mut expected_ids = BTreeSet::new();
        for i in 0..7 {
            let id = format!("w{i}");
            let (status, body) = dynamo(
                dynamo_addr,
                "DynamoDB_20120810.PutItem",
                &format!(
                    r#"{{"TableName":"widgets","Item":{{"id":{{"S":"{id}"}},"n":{{"N":"{i}"}}}}}}"#
                ),
            )
            .await;
            assert_eq!(status, 200, "PutItem({id}) failed: {body}");
            expected_ids.insert(id);
        }

        // A single un-paginated scan first, sanity-checking the plain shape.
        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/widgets/items/scan",
            "{}",
        )
        .await;
        assert_eq!(status, 200, "scan failed: {body}");
        assert_no_cluster_shape(&body);
        let v = json(&body);
        assert_eq!(v["items"].as_array().unwrap().len(), 7);
        assert!(v["last_evaluated_key"].is_null());

        // Now walk it with Limit=2 per page, following LastEvaluatedKey —
        // real DynamoDB-shaped paging, never an offset.
        let mut seen = BTreeSet::new();
        let mut cursor: Option<serde_json::Value> = None;
        let mut pages = 0;
        loop {
            let req = match &cursor {
                Some(key) => {
                    serde_json::json!({"limit": 2, "exclusive_start_key": key}).to_string()
                }
                None => serde_json::json!({"limit": 2}).to_string(),
            };
            let (status, body) = console(
                console_addr,
                "POST",
                "/console/api/tables/widgets/items/scan",
                &req,
            )
            .await;
            assert_eq!(status, 200, "paginated scan failed: {body}");
            assert_no_cluster_shape(&body);
            let page = json(&body);
            pages += 1;
            for item in page["items"].as_array().unwrap() {
                let id = item["id"]["S"].as_str().unwrap().to_string();
                assert!(
                    seen.insert(id.clone()),
                    "item {id} was returned by more than one page"
                );
            }
            assert!(
                pages < 20,
                "pagination did not converge within 20 pages: seen so far {seen:?}"
            );
            if page["last_evaluated_key"].is_null() {
                break;
            }
            cursor = Some(page["last_evaluated_key"].clone());
        }
        assert!(
            pages > 1,
            "the walk should have taken more than one page at Limit=2 over 7 items"
        );
        assert_eq!(
            seen, expected_ids,
            "every item must be visited exactly once across the whole walk"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// `POST /console/api/tables/{name}/items/query` narrows to one partition
/// (and, when the table has one, a sort-key condition).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_by_partition_key_and_sort_condition() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let dynamo_addr = node.dynamo_addr();
        let console_addr = node.console_addr();

        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"orders",
                "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},
                                         {"AttributeName":"sk","AttributeType":"N"}],
                "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                             {"AttributeName":"sk","KeyType":"RANGE"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        for (pk, sk) in [("cust-1", 1), ("cust-1", 2), ("cust-1", 3), ("cust-2", 1)] {
            let (status, body) = dynamo(
                dynamo_addr,
                "DynamoDB_20120810.PutItem",
                &format!(
                    r#"{{"TableName":"orders","Item":{{"pk":{{"S":"{pk}"}},"sk":{{"N":"{sk}"}}}}}}"#
                ),
            )
            .await;
            assert_eq!(status, 200, "PutItem failed: {body}");
        }

        // Plain partition-key query: all three cust-1 rows, none of cust-2's.
        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/orders/items/query",
            r#"{"partition_value":{"S":"cust-1"}}"#,
        )
        .await;
        assert_eq!(status, 200, "query failed: {body}");
        assert_no_cluster_shape(&body);
        let v = json(&body);
        let items = v["items"].as_array().unwrap();
        assert_eq!(items.len(), 3, "query result: {body}");
        for item in items {
            assert_eq!(item["pk"]["S"], "cust-1");
        }

        // Narrowed further with a sort-key condition (Between).
        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/orders/items/query",
            r#"{"partition_value":{"S":"cust-1"},
                "sort_condition":{"kind":"between","lo":{"N":"2"},"hi":{"N":"3"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "narrowed query failed: {body}");
        let v = json(&body);
        let items = v["items"].as_array().unwrap();
        assert_eq!(items.len(), 2, "narrowed query result: {body}");
        let mut sks: Vec<i64> = items
            .iter()
            .map(|i| i["sk"]["N"].as_str().unwrap().parse().unwrap())
            .collect();
        sks.sort_unstable();
        assert_eq!(sks, vec![2, 3]);

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// The `GetItem`/`PutItem`/`DeleteItem` round trip through the console
/// port: create, read back, overwrite, read the new value, delete, confirm
/// gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_get_delete_item_round_trip() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let dynamo_addr = node.dynamo_addr();
        let console_addr = node.console_addr();

        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"sessions",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        // ---- GetItem on a never-written key: 200 + a null item ----------
        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/sessions/items/get",
            r#"{"key":{"id":{"S":"s1"}}}"#,
        )
        .await;
        assert_eq!(
            status, 200,
            "get of a missing item must still be 200: {body}"
        );
        assert_no_cluster_shape(&body);
        assert!(json(&body)["item"].is_null());

        // ---- PutItem --------------------------------------------------
        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/sessions/items/put",
            r#"{"item":{"id":{"S":"s1"},"active":{"BOOL":true},"tag":{"S":"first"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "put failed: {body}");
        assert_no_cluster_shape(&body);
        assert_eq!(json(&body)["ok"], true);

        // ---- GetItem: read the exact wire shape back -------------------
        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/sessions/items/get",
            r#"{"key":{"id":{"S":"s1"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "get failed: {body}");
        let v = json(&body);
        assert_eq!(v["item"]["id"]["S"], "s1");
        assert_eq!(v["item"]["active"]["BOOL"], true);
        assert_eq!(v["item"]["tag"]["S"], "first");

        // ---- PutItem again (whole-item replace) -------------------------
        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/sessions/items/put",
            r#"{"item":{"id":{"S":"s1"},"active":{"BOOL":false}}}"#,
        )
        .await;
        assert_eq!(status, 200, "overwrite put failed: {body}");

        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/sessions/items/get",
            r#"{"key":{"id":{"S":"s1"}}}"#,
        )
        .await;
        assert_eq!(status, 200);
        let v = json(&body);
        assert_eq!(v["item"]["active"]["BOOL"], false);
        assert!(
            v["item"].get("tag").is_none(),
            "PutItem wholesale-replaces the item — the old `tag` attribute must be gone: {body}"
        );

        // ---- DeleteItem --------------------------------------------------
        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/sessions/items/delete",
            r#"{"key":{"id":{"S":"s1"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "delete failed: {body}");
        assert_no_cluster_shape(&body);
        assert_eq!(json(&body)["ok"], true);

        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/sessions/items/get",
            r#"{"key":{"id":{"S":"s1"}}}"#,
        )
        .await;
        assert_eq!(status, 200);
        assert!(
            json(&body)["item"].is_null(),
            "item must be gone after delete: {body}"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// Querying/scanning a GSI by name (`index_name`) works the same way as the
/// base table — the "natural extension" ADR 0052's PR4 amendment mentions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_and_query_a_gsi_by_name() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let dynamo_addr = node.dynamo_addr();
        let console_addr = node.console_addr();

        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"orders",
                "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"},
                                         {"AttributeName":"status","AttributeType":"S"}],
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
                "GlobalSecondaryIndexes":[
                    {"IndexName":"by-status",
                     "KeySchema":[{"AttributeName":"status","KeyType":"HASH"}],
                     "Projection":{"ProjectionType":"ALL"}}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        for (id, status_val) in [("o1", "open"), ("o2", "open"), ("o3", "closed")] {
            let (status, body) = dynamo(
                dynamo_addr,
                "DynamoDB_20120810.PutItem",
                &format!(
                    r#"{{"TableName":"orders","Item":{{"id":{{"S":"{id}"}},"status":{{"S":"{status_val}"}}}}}}"#
                ),
            )
            .await;
            assert_eq!(status, 200, "PutItem({id}) failed: {body}");
        }

        // GSI Scan: every row through the index.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let (status, body) = console(
                console_addr,
                "POST",
                "/console/api/tables/orders/items/scan",
                r#"{"index_name":"by-status"}"#,
            )
            .await;
            assert_eq!(status, 200, "gsi scan failed: {body}");
            assert_no_cluster_shape(&body);
            if json(&body)["items"].as_array().unwrap().len() == 3 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "gsi scan never saw all 3 rows: {body}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // GSI Query: narrow to status=open.
        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/orders/items/query",
            r#"{"index_name":"by-status","partition_value":{"S":"open"}}"#,
        )
        .await;
        assert_eq!(status, 200, "gsi query failed: {body}");
        assert_no_cluster_shape(&body);
        let items = json(&body)["items"].as_array().unwrap().clone();
        assert_eq!(items.len(), 2, "gsi query result: {body}");
        for item in &items {
            assert_eq!(item["status"]["S"], "open");
        }

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}
