//! End-to-end tests for animusd console's (ADR 0052's "AnimusDB Data
//! Console") table page Config tab
//! endpoints (ADR 0052 PR3): `GET /console/api/tables/{name}` (full
//! configuration), adding/dropping a GSI, toggling the stream, setting/
//! clearing TTL, and deleting a table — all through the **console** port,
//! never the admin port, exactly like `tests/console_tables.rs` proves for
//! the tables-list endpoint. The property most worth a regression test
//! (again): no node/tablet/replica-shaped field anywhere in any response.
//!
//! Tables are created and read back through the real DynamoDB JSON/HTTP
//! wire so the fixtures match what an application would actually declare;
//! only the Config tab's own mutations go through the console port.
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
/// body)`. Mirrors every other `tests/dynamo_*.rs`/`console_tables.rs`
/// file's identical helper.
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
/// and (optional) JSON body → `(status, body)`. The console-port sibling of
/// `dynamo` above; `console_tables.rs::console_get` is this helper's
/// GET-only, body-less special case.
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
/// `console_tables.rs` checks the tables-list response against, reused here
/// for every Config tab response.
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

/// `GET /console/api/tables/{name}` projects a table's full configuration
/// correctly across the dimensions that vary: with/without a sort key, a
/// GSI (with its lifecycle status), an LSI (which must NOT carry a
/// `status`/hash-attribute field — it isn't a GSI), a stream, and TTL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn table_detail_projects_full_configuration() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let dynamo_addr = node.dynamo_addr();
        let console_addr = node.console_addr();

        // ---- a hash-only table, no sort key --------------------------
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"simple",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable(simple) failed: {body}");

        let (status, body) = console(console_addr, "GET", "/console/api/tables/simple", "").await;
        assert_eq!(status, 200, "table detail failed: {body}");
        let d = json(&body);
        assert_eq!(d["name"], "simple");
        assert_eq!(d["partition_key"]["name"], "id");
        assert!(d["sort_key"].is_null());
        assert!(d["gsis"].as_array().unwrap().is_empty());
        assert!(d["lsis"].as_array().unwrap().is_empty());
        assert_eq!(d["stream"]["enabled"], false);
        assert_eq!(d["ttl"]["enabled"], false);
        assert_no_cluster_shape(&body);

        // ---- a full-featured table: sort key + GSI + LSI + stream ----
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"full",
                "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},
                                         {"AttributeName":"sk","AttributeType":"S"},
                                         {"AttributeName":"cat","AttributeType":"S"},
                                         {"AttributeName":"score","AttributeType":"N"}],
                "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                             {"AttributeName":"sk","KeyType":"RANGE"}],
                "GlobalSecondaryIndexes":[
                    {"IndexName":"by-cat",
                     "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}],
                     "Projection":{"ProjectionType":"ALL"}}],
                "LocalSecondaryIndexes":[
                    {"IndexName":"by-score",
                     "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                                  {"AttributeName":"score","KeyType":"RANGE"}]}],
                "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable(full) failed: {body}");
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.UpdateTimeToLive",
            r#"{"TableName":"full",
                "TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}}"#,
        )
        .await;
        assert_eq!(status, 200, "UpdateTimeToLive(full) failed: {body}");

        let (status, body) = console(console_addr, "GET", "/console/api/tables/full", "").await;
        assert_eq!(status, 200, "table detail failed: {body}");
        let d = json(&body);
        assert_eq!(d["sort_key"]["name"], "sk");
        let gsis = d["gsis"].as_array().unwrap();
        assert_eq!(gsis.len(), 1);
        assert_eq!(gsis[0]["name"], "by-cat");
        assert_eq!(gsis[0]["hash_attribute"]["name"], "cat");
        assert!(gsis[0]["sort_attribute"].is_null());
        assert_eq!(
            gsis[0]["status"], "ACTIVE",
            "a table created non-empty gets its GSIs Active immediately (ADR 0041 §5)"
        );
        let lsis = d["lsis"].as_array().unwrap();
        assert_eq!(lsis.len(), 1);
        assert_eq!(lsis[0]["name"], "by-score");
        assert_eq!(lsis[0]["sort_attribute"]["name"], "score");
        assert!(
            lsis[0].get("status").is_none(),
            "an LSI row carries no lifecycle status field: {body}"
        );
        assert_eq!(d["stream"]["enabled"], true);
        assert_eq!(d["stream"]["view_type"], "NEW_AND_OLD_IMAGES");
        assert_eq!(d["ttl"]["enabled"], true);
        assert_eq!(d["ttl"]["attribute_name"], "expiresAt");
        assert_no_cluster_shape(&body);

        // ---- an unknown table 404s -------------------------------------
        let (status, body) =
            console(console_addr, "GET", "/console/api/tables/nope", "").await;
        assert_eq!(status, 404, "unknown table: {body}");
        assert_eq!(json(&body)["error"], "no such table");

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// Adding a GSI through the console shows it `CREATING` (a populated table's
/// added index backfills, ADR 0045 §2) and it later converges to `ACTIVE`;
/// dropping it removes it from the detail response.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_and_drop_gsi_round_trip() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let dynamo_addr = node.dynamo_addr();
        let console_addr = node.console_addr();

        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"orders",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");
        // Populate the table so the added GSI actually needs a backfill —
        // an empty table's index would go straight to Active, which would
        // not distinguish this test from the create-time GSI case above.
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.PutItem",
            r#"{"TableName":"orders","Item":{"id":{"S":"o1"},"status":{"S":"open"}}}"#,
        )
        .await;
        assert_eq!(status, 200, "PutItem failed: {body}");

        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/orders/gsi",
            r#"{"index_name":"by-status","hash_attribute":"status"}"#,
        )
        .await;
        assert_eq!(status, 200, "add_gsi failed: {body}");
        let resp = json(&body);
        assert_eq!(resp["gsi"]["name"], "by-status");
        assert_eq!(resp["gsi"]["hash_attribute"]["name"], "status");
        // No declared type: this request gave no `hash_attribute_type`
        // (issue #319's fields are optional), so `add_gsi` sent no
        // `AttributeDefinitions` entry and the console reports `null`
        // rather than an invented `"S"` — see
        // `add_gsi_records_a_declared_attribute_type` below for the
        // positive case, where a type genuinely round-trips.
        assert!(
            resp["gsi"]["hash_attribute"]["attribute_type"].is_null(),
            "an added GSI's key attribute must not claim a type: {body}"
        );
        assert!(resp["gsi"]["sort_attribute"].is_null());
        assert_eq!(
            resp["gsi"]["status"], "CREATING",
            "a populated table's added GSI starts backfilling: {body}"
        );
        assert_no_cluster_shape(&body);

        let (status, body) = console(console_addr, "GET", "/console/api/tables/orders", "").await;
        assert_eq!(status, 200);
        let gsis = json(&body)["gsis"].as_array().unwrap().clone();
        assert_eq!(gsis.len(), 1);
        assert_eq!(gsis[0]["name"], "by-status");

        // ---- converges to Active ---------------------------------------
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let (status, body) =
                console(console_addr, "GET", "/console/api/tables/orders", "").await;
            assert_eq!(status, 200);
            let d = json(&body);
            if d["gsis"][0]["status"] == "ACTIVE" {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "GSI never reached Active: {body}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // ---- drop it -----------------------------------------------------
        let (status, body) = console(
            console_addr,
            "DELETE",
            "/console/api/tables/orders/gsi/by-status",
            "",
        )
        .await;
        assert_eq!(status, 200, "drop_gsi failed: {body}");
        assert_eq!(json(&body)["ok"], true);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let (status, body) =
                console(console_addr, "GET", "/console/api/tables/orders", "").await;
            assert_eq!(status, 200);
            if json(&body)["gsis"].as_array().unwrap().is_empty() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "GSI never disappeared after drop: {body}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// Issue #319: an Add-GSI call that *does* supply `hash_attribute_type`/
/// `sort_attribute_type` gets a real declared type recorded — the console
/// response echoes it back immediately, and it still reads that way off a
/// fresh `GET /console/api/tables/{name}` afterward (a real replicated-
/// catalog round trip, not just an echo of the request). Companion to
/// `add_and_drop_gsi_round_trip`'s own no-type case just above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_gsi_records_a_declared_attribute_type() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let dynamo_addr = node.dynamo_addr();
        let console_addr = node.console_addr();

        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"readings",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/readings/gsi",
            r#"{"index_name":"by-score","hash_attribute":"score",
                "hash_attribute_type":"N","sort_attribute":"rank",
                "sort_attribute_type":"B"}"#,
        )
        .await;
        assert_eq!(status, 200, "add_gsi failed: {body}");
        let resp = json(&body);
        assert_eq!(resp["gsi"]["hash_attribute"]["name"], "score");
        assert_eq!(resp["gsi"]["hash_attribute"]["attribute_type"], "N");
        assert_eq!(resp["gsi"]["sort_attribute"]["name"], "rank");
        assert_eq!(resp["gsi"]["sort_attribute"]["attribute_type"], "B");
        assert_no_cluster_shape(&body);

        // Re-read the table detail fresh — the type is durably in the
        // replicated catalog, not merely echoed off the request.
        let (status, body) = console(console_addr, "GET", "/console/api/tables/readings", "").await;
        assert_eq!(status, 200);
        let d = json(&body);
        assert_eq!(d["gsis"][0]["hash_attribute"]["attribute_type"], "N");
        assert_eq!(d["gsis"][0]["sort_attribute"]["attribute_type"], "B");

        // And `DescribeTable`'s own `AttributeDefinitions` — the original
        // issue #319 complaint — covers both, for real, not `"S"`.
        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.DescribeTable",
            r#"{"TableName":"readings"}"#,
        )
        .await;
        assert_eq!(status, 200, "DescribeTable failed: {body}");
        assert!(
            body.contains(r#"{"AttributeName":"score","AttributeType":"N"}"#),
            "score's declared N type missing from AttributeDefinitions: {body}"
        );
        assert!(
            body.contains(r#"{"AttributeName":"rank","AttributeType":"B"}"#),
            "rank's declared B type missing from AttributeDefinitions: {body}"
        );

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// A malformed `hash_attribute_type`/`sort_attribute_type` (anything but
/// `S`/`N`/`B`, case-insensitively) is a client error, matching real
/// DynamoDB's own rejection of an unknown `AttributeType` — never silently
/// dropped or defaulted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_gsi_rejects_an_unknown_attribute_type() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let dynamo_addr = node.dynamo_addr();
        let console_addr = node.console_addr();

        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"orders",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/orders/gsi",
            r#"{"index_name":"by-status","hash_attribute":"status","hash_attribute_type":"X"}"#,
        )
        .await;
        assert_eq!(status, 400, "expected a client error: {body}");

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// The stream toggle round-trips: enable with a view type, then disable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_toggle_round_trips() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let dynamo_addr = node.dynamo_addr();
        let console_addr = node.console_addr();

        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"events",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        // ---- enable --------------------------------------------------
        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/events/stream",
            r#"{"enabled":true,"view_type":"NEW_IMAGE"}"#,
        )
        .await;
        assert_eq!(status, 200, "enable stream failed: {body}");
        let resp = json(&body);
        assert_eq!(resp["stream"]["enabled"], true);
        assert_eq!(resp["stream"]["view_type"], "NEW_IMAGE");
        assert_no_cluster_shape(&body);

        let (status, body) = console(console_addr, "GET", "/console/api/tables/events", "").await;
        assert_eq!(status, 200);
        let d = json(&body);
        assert_eq!(d["stream"]["enabled"], true);
        assert_eq!(d["stream"]["view_type"], "NEW_IMAGE");

        // ---- disable ---------------------------------------------------
        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/events/stream",
            r#"{"enabled":false}"#,
        )
        .await;
        assert_eq!(status, 200, "disable stream failed: {body}");
        let resp = json(&body);
        assert_eq!(resp["stream"]["enabled"], false);
        assert!(resp["stream"]["view_type"].is_null());

        let (status, body) = console(console_addr, "GET", "/console/api/tables/events", "").await;
        assert_eq!(status, 200);
        assert_eq!(json(&body)["stream"]["enabled"], false);

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// TTL set/clear round-trips (ADR 0051).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ttl_set_and_clear_round_trips() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
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

        // ---- set ---------------------------------------------------------
        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/sessions/ttl",
            r#"{"enabled":true,"attribute_name":"expiresAt"}"#,
        )
        .await;
        assert_eq!(status, 200, "set ttl failed: {body}");
        let resp = json(&body);
        assert_eq!(resp["ttl"]["enabled"], true);
        assert_eq!(resp["ttl"]["attribute_name"], "expiresAt");
        assert_no_cluster_shape(&body);

        let (status, body) = console(console_addr, "GET", "/console/api/tables/sessions", "").await;
        assert_eq!(status, 200);
        let d = json(&body);
        assert_eq!(d["ttl"]["enabled"], true);
        assert_eq!(d["ttl"]["attribute_name"], "expiresAt");

        // ---- clear (disable) ----------------------------------------------
        let (status, body) = console(
            console_addr,
            "POST",
            "/console/api/tables/sessions/ttl",
            r#"{"enabled":false,"attribute_name":"expiresAt"}"#,
        )
        .await;
        assert_eq!(status, 200, "clear ttl failed: {body}");
        let resp = json(&body);
        assert_eq!(resp["ttl"]["enabled"], false);

        let (status, body) = console(console_addr, "GET", "/console/api/tables/sessions", "").await;
        assert_eq!(status, 200);
        assert_eq!(json(&body)["ttl"]["enabled"], false);

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}

/// Deleting a table through the console removes it from the tables list and
/// its own detail endpoint 404s afterward.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_table_works() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (node, _config) =
            support::start_single_node(dir.path(), animusd::StorageBackend::Memory).await;
        let dynamo_addr = node.dynamo_addr();
        let console_addr = node.console_addr();

        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateTable",
            r#"{"TableName":"scratch",
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        let (status, body) = console(console_addr, "GET", "/console/api/tables/scratch", "").await;
        assert_eq!(status, 200, "table exists before delete: {body}");

        let (status, body) =
            console(console_addr, "DELETE", "/console/api/tables/scratch", "").await;
        assert_eq!(status, 200, "delete_table failed: {body}");
        assert_eq!(json(&body)["ok"], true);
        assert_no_cluster_shape(&body);

        let (status, body) = console(console_addr, "GET", "/console/api/tables/scratch", "").await;
        assert_eq!(status, 404, "table gone after delete: {body}");

        let (status, body) = console(console_addr, "GET", "/console/api/tables", "").await;
        assert_eq!(status, 200);
        let names: Vec<String> = json(&body)["tables"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect();
        assert!(
            !names.contains(&"scratch".to_string()),
            "dropped table absent from the tables list: {names:?}"
        );

        // Deleting an already-gone table 404s rather than pretending success.
        let (status, body) =
            console(console_addr, "DELETE", "/console/api/tables/scratch", "").await;
        assert_eq!(status, 404, "double-delete: {body}");

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}
