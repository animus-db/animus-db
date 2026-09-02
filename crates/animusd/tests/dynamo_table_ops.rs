//! End-to-end tests for the DynamoDB `DeleteTable`/`ListTables` operations,
//! over real HTTP/TCP, mirroring `dynamo_wire.rs`/`dynamo_consistent_read.rs`'s
//! bring-up and request-helper idioms.
//!
//! - `list_tables_sorts_paginates_and_excludes_gsi_hidden_tables` proves
//!   `ListTables`' name-ordering, `Limit`/`ExclusiveStartTableName`
//!   round-trip, and (ADR 0041 §1) that a materialized GSI's hidden table
//!   (`<base>$<index>`) never appears in the output.
//! - `delete_table_removes_it_and_a_repeat_delete_is_not_found` proves
//!   `DeleteTable`'s `TableDescription`/`DELETING` response, that the table
//!   converges to gone from `ListTables` (a converged-or-timeout poll — the
//!   drop propagates through the replicated catalog, per
//!   `docs/engineering-lessons.md`'s "no fixed-deadline one-shot assert on an
//!   eventual property"), and that deleting it again is a
//!   `ResourceNotFoundException`.
//! - `delete_table_through_a_follower_connected_node_is_relayed_to_the_leader`
//!   proves `DeleteTable` works when the HTTP request lands on a control-plane
//!   follower (`ClientCtx::drop_table`'s `MetaCommand::DropTableSchema`/
//!   `DropTableTablets` are both on `is_relayable_command`'s allowlist —
//!   already exercised by `schema_ddl_relay.rs` for other schema commands;
//!   this is the `DeleteTable`-specific regression the root `CLAUDE.md`'s
//!   "grep every gating match site" lesson calls for).

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{Node, bind_cluster, start_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

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

/// One DynamoDB JSON request over the real HTTP wire.
async fn dynamo(addr: SocketAddr, target: &str, body: &str) -> (u16, String) {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    let req = format!(
        "POST / HTTP/1.1\r\nHost: x\r\nX-Amz-Target: {target}\r\n\
         Connection: close\r\n\
         Content-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).await.expect("write");
    s.flush().await.expect("flush");
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).await.expect("read");
    let text = String::from_utf8(raw).expect("utf8");
    let (head, payload) = text.split_once("\r\n\r\n").expect("has body");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .expect("status line");
    (status, payload.to_string())
}

async fn create_table(addr: SocketAddr, name: &str) {
    let body = format!(
        r#"{{"TableName":"{name}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}]}}"#
    );
    let (status, resp) = dynamo(addr, "DynamoDB_20120810.CreateTable", &body).await;
    assert_eq!(status, 200, "CreateTable `{name}` failed: {resp}");
}

/// Extract `TableNames` from a `ListTables` response body as a `Vec<String>`,
/// via `serde_json` (cheaper than the string-`contains` idiom other tests use,
/// and needed here since order/membership must be checked precisely).
fn table_names(body: &str) -> Vec<String> {
    let v: serde_json::Value = serde_json::from_str(body).expect("valid JSON");
    v["TableNames"]
        .as_array()
        .expect("TableNames array")
        .iter()
        .map(|n| n.as_str().expect("table name string").to_owned())
        .collect()
}

fn last_evaluated_table_name(body: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(body).expect("valid JSON");
    v["LastEvaluatedTableName"].as_str().map(str::to_owned)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_tables_sorts_paginates_and_excludes_gsi_hidden_tables() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(1, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    // Created out of lexicographic order, on purpose.
    create_table(addr, "zebra").await;
    create_table(addr, "apple").await;

    // A table with a GSI: its hidden materialization table (`with_gsi$by-x`)
    // must never surface in `ListTables`.
    let (status, resp) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"with_gsi",
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-x",
                 "KeySchema":[{"AttributeName":"x","KeyType":"HASH"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable with_gsi failed: {resp}");

    // Full listing: sorted ascending, exactly the three real tables — the
    // GSI's hidden table (`with_gsi$by-x`) is absent.
    let (status, body) = dynamo(addr, "DynamoDB_20120810.ListTables", "{}").await;
    assert_eq!(status, 200, "ListTables failed: {body}");
    let names = table_names(&body);
    assert_eq!(
        names,
        vec![
            "apple".to_owned(),
            "with_gsi".to_owned(),
            "zebra".to_owned()
        ],
        "got: {body}"
    );
    assert_eq!(
        last_evaluated_table_name(&body),
        None,
        "an untruncated listing must not carry a cursor: {body}"
    );

    // `Limit`/`ExclusiveStartTableName` round trip: page 1 of 2.
    let (status, body) = dynamo(addr, "DynamoDB_20120810.ListTables", r#"{"Limit":2}"#).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        table_names(&body),
        vec!["apple".to_owned(), "with_gsi".to_owned()]
    );
    let cursor = last_evaluated_table_name(&body)
        .expect("a truncated page must carry LastEvaluatedTableName");
    assert_eq!(cursor, "with_gsi");

    // Page 2, starting strictly after the cursor.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.ListTables",
        &format!(r#"{{"Limit":2,"ExclusiveStartTableName":"{cursor}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(table_names(&body), vec!["zebra".to_owned()]);
    assert_eq!(
        last_evaluated_table_name(&body),
        None,
        "the final page must not carry a cursor: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_table_removes_it_and_a_repeat_delete_is_not_found() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(1, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    create_table(addr, "keepme").await;
    create_table(addr, "dropme").await;

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DeleteTable",
        r#"{"TableName":"dropme"}"#,
    )
    .await;
    assert_eq!(status, 200, "DeleteTable failed: {body}");
    assert!(
        body.contains("\"TableDescription\""),
        "expected a TableDescription wrapper, got: {body}"
    );
    assert!(
        body.contains("\"TableStatus\":\"DELETING\""),
        "expected TableStatus DELETING, got: {body}"
    );
    assert!(body.contains("\"TableName\":\"dropme\""), "got: {body}");

    // The drop propagates through the replicated catalog — a
    // converged-or-timeout poll, never a fixed-deadline one-shot assert
    // (docs/engineering-lessons.md).
    timeout(Duration::from_secs(20), async {
        loop {
            let (status, body) = dynamo(addr, "DynamoDB_20120810.ListTables", "{}").await;
            assert_eq!(status, 200, "{body}");
            let names = table_names(&body);
            if !names.contains(&"dropme".to_owned()) && names.contains(&"keepme".to_owned()) {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("dropped table did not disappear from ListTables within 20s");

    // `DescribeTable` against the now-gone table is also
    // `ResourceNotFoundException`.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DescribeTable",
        r#"{"TableName":"dropme"}"#,
    )
    .await;
    assert_ne!(status, 200);
    assert!(body.contains("ResourceNotFoundException"), "got: {body}");

    // A second `DeleteTable` of the same (now-absent) table is a
    // `ResourceNotFoundException`, not a false success — `ClientCtx::
    // drop_table` itself is idempotent, so this is exactly what the
    // explicit existence check in `dynamo.rs::delete_table` exists for.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DeleteTable",
        r#"{"TableName":"dropme"}"#,
    )
    .await;
    assert_ne!(status, 200, "expected an error, got 200: {body}");
    assert!(body.contains("ResourceNotFoundException"), "got: {body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_table_through_a_follower_connected_node_is_relayed_to_the_leader() {
    let dir = support::panic_safe_tempdir();
    let bound = bind_cluster(3, "127.0.0.1".parse().unwrap(), dir.path())
        .await
        .unwrap();
    let nodes = start_cluster(bound).await.unwrap();
    await_bootstrap(&nodes).await;

    let leader = nodes.iter().position(Node::is_control_leader).unwrap();
    let follower = (0..nodes.len()).find(|&i| i != leader).unwrap();

    create_table(nodes[leader].dynamo_addr(), "relay_target").await;

    // Wait for the create to replicate to every node before the
    // follower-issued delete (else the follower might not see it exists yet
    // and this test would flake on the wrong precondition).
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes
                .iter()
                .all(|n| n.metadata().has_table_schema("relay_target"))
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("CreateTable did not replicate to every node within 20s");

    // Issue `DeleteTable` against the FOLLOWER's own dynamo edge — it must
    // relay `MetaCommand::DropTableSchema`/`DropTableTablets` to the leader
    // rather than timing out (both are on `is_relayable_command`'s
    // allowlist).
    let (status, body) = dynamo(
        nodes[follower].dynamo_addr(),
        "DynamoDB_20120810.DeleteTable",
        r#"{"TableName":"relay_target"}"#,
    )
    .await;
    assert_eq!(status, 200, "follower-issued DeleteTable failed: {body}");

    // Converges to gone on every node, including the leader.
    timeout(Duration::from_secs(20), async {
        loop {
            if nodes
                .iter()
                .all(|n| !n.metadata().has_table_schema("relay_target"))
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("follower-relayed DeleteTable did not converge within 20s");
}
