//! End-to-end tests for the on-demand backup wire surface (ADR 0059, Train 1
//! PR④) over the real DynamoDB JSON/HTTP wire: `CreateBackup`/
//! `DescribeBackup`/`ListBackups`/`DeleteBackup`, the AWS-faithful error
//! shapes, and the backup janitor's own two-phase reclaim (`animusd::
//! backup_janitor`) converging a `DeleteBackup`-marked row all the way to
//! physical removal. Real time/sockets (the `ProdEnv` edge) — every eventual
//! property is a converged-or-timeout poll, never a fixed sleep (this
//! codebase's own testing discipline).
//!
//! `RestoreTableFromBackup` is out of scope (ADR 0059 Train 2) — nothing
//! here exercises restore.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{Node, StorageBackend};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// One DynamoDB request over a fresh HTTP/1.1 connection → `(status, body)`.
/// Mirrors every other `tests/dynamo_*.rs` file's identical helper.
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

async fn await_bootstrap(node: &Node) {
    timeout(Duration::from_secs(10), async {
        loop {
            if node.is_control_leader() && !node.metadata().members.is_empty() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("node did not bootstrap in 10s");
}

fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
}

fn create_table_body(table: &str) -> String {
    format!(
        r#"{{"TableName":"{table}","KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}]}}"#
    )
}

/// A `BatchWriteItem` body writing items `[start, end)` to `table`, each with
/// pk `id` = `item{i}` — mirrors `batch_write.rs`'s identical helper, used
/// here purely to populate a table quickly enough to observe a backup still
/// `CREATING` mid-capture.
fn batch_put_body_range(table: &str, start: usize, end: usize) -> String {
    let puts: Vec<String> = (start..end)
        .map(|i| format!(r#"{{"PutRequest":{{"Item":{{"id":{{"S":"item{i}"}}}}}}}}"#))
        .collect();
    format!(r#"{{"RequestItems":{{"{table}":[{}]}}}}"#, puts.join(","))
}

async fn create_backup(
    addr: SocketAddr,
    table: &str,
    backup_name: &str,
) -> (u16, serde_json::Value) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateBackup",
        &format!(r#"{{"TableName":"{table}","BackupName":"{backup_name}"}}"#),
    )
    .await;
    (status, json(&body))
}

async fn describe_backup(addr: SocketAddr, backup_arn: &str) -> (u16, String) {
    dynamo(
        addr,
        "DynamoDB_20120810.DescribeBackup",
        &format!(r#"{{"BackupArn":"{backup_arn}"}}"#),
    )
    .await
}

async fn delete_backup(addr: SocketAddr, backup_arn: &str) -> (u16, String) {
    dynamo(
        addr,
        "DynamoDB_20120810.DeleteBackup",
        &format!(r#"{{"BackupArn":"{backup_arn}"}}"#),
    )
    .await
}

fn error_code(body: &str) -> String {
    json(body)["__type"]
        .as_str()
        .map(|t| t.rsplit('#').next().unwrap_or(t).to_owned())
        .unwrap_or_else(|| panic!("no __type in error body: {body}"))
}

/// The full wire round trip: `CreateBackup` → poll to `AVAILABLE` →
/// `DescribeBackup`/`ListBackups` → drop the source table → `DescribeBackup`
/// still works → `DeleteBackup` → the janitor reclaims objects and removes
/// the row, poll converged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_backup_round_trip_survives_table_drop_and_janitor_reclaims() {
    let dir = TempDir::new().unwrap();
    let (node, config) = support::start_single_node(dir.path(), StorageBackend::default()).await;
    let addr = config.nodes[0].dynamo;
    await_bootstrap(&node).await;

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        &create_table_body("orders"),
    )
    .await;
    assert_eq!(status, 200, "body: {body}");

    for i in 0..3 {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &format!(r#"{{"TableName":"orders","Item":{{"id":{{"S":"o{i}"}}}}}}"#),
        )
        .await;
        assert_eq!(status, 200, "body: {body}");
    }

    let (status, created) = create_backup(addr, "orders", "nightly-1").await;
    assert_eq!(status, 200, "body: {created}");
    let details = &created["BackupDetails"];
    assert_eq!(details["BackupName"], "nightly-1");
    assert_eq!(details["BackupStatus"], "CREATING");
    assert_eq!(details["BackupType"], "USER");
    let backup_arn = details["BackupArn"].as_str().expect("BackupArn").to_owned();
    assert!(
        backup_arn.contains(":table/orders/backup/"),
        "arn: {backup_arn}"
    );

    // Poll converged-or-timeout to AVAILABLE.
    let describe_available = timeout(Duration::from_secs(20), async {
        loop {
            let (status, body) = describe_backup(addr, &backup_arn).await;
            assert_eq!(status, 200, "body: {body}");
            let v = json(&body);
            if v["BackupDescription"]["BackupDetails"]["BackupStatus"] == "AVAILABLE" {
                return v;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("backup did not become AVAILABLE in 20s");

    let desc = &describe_available["BackupDescription"];
    assert_eq!(desc["BackupDetails"]["BackupName"], "nightly-1");
    assert_eq!(desc["SourceTableDetails"]["TableName"], "orders");
    assert_eq!(
        desc["SourceTableDetails"]["KeySchema"][0]["AttributeName"],
        "id"
    );
    let size_bytes = desc["BackupDetails"]["BackupSizeBytes"]
        .as_u64()
        .expect("BackupSizeBytes is a number");
    assert!(size_bytes > 0, "expected a non-zero captured size: {desc}");

    // `ListBackups`: found, unfiltered and filtered by the right table; not
    // found when filtered by a different table.
    let (status, body) = dynamo(addr, "DynamoDB_20120810.ListBackups", "{}").await;
    assert_eq!(status, 200, "body: {body}");
    let summaries = json(&body)["BackupSummaries"].clone();
    assert!(
        summaries
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["BackupArn"] == backup_arn),
        "backup missing from unfiltered ListBackups: {summaries}"
    );

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.ListBackups",
        r#"{"TableName":"orders"}"#,
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    assert!(
        json(&body)["BackupSummaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["BackupArn"] == backup_arn),
    );

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.ListBackups",
        r#"{"TableName":"nonexistent"}"#,
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    assert!(
        json(&body)["BackupSummaries"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    // Drop the source table — `DescribeBackup`/`ListBackups` must still work,
    // reading purely from the manifest's own captured snapshot (ADR 0059
    // §2/§3).
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DeleteTable",
        r#"{"TableName":"orders"}"#,
    )
    .await;
    assert_eq!(status, 200, "body: {body}");

    let (status, body) = describe_backup(addr, &backup_arn).await;
    assert_eq!(
        status, 200,
        "DescribeBackup must still work after the source table is dropped: {body}"
    );
    let v = json(&body);
    let desc = &v["BackupDescription"];
    assert_eq!(desc["SourceTableDetails"]["TableName"], "orders");
    assert_eq!(
        desc["BackupDetails"]["BackupSizeBytes"].as_u64().unwrap(),
        size_bytes,
        "the frozen size must not silently collapse to zero post-drop"
    );

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.ListBackups",
        r#"{"TableName":"orders"}"#,
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    assert!(
        json(&body)["BackupSummaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["BackupArn"] == backup_arn),
        "ListBackups(TableName) must still find it post-drop"
    );

    // `DeleteBackup` — immediately reports `DELETED`.
    let (status, body) = delete_backup(addr, &backup_arn).await;
    assert_eq!(status, 200, "body: {body}");
    let v = json(&body);
    assert_eq!(
        v["BackupDescription"]["BackupDetails"]["BackupStatus"],
        "DELETED"
    );

    // From here it is immediately gone at the wire (AWS-faithful — a
    // deleted backup's ARN is invalid right away, even though this
    // adapter's own physical reclaim is asynchronous).
    let (status, body) = describe_backup(addr, &backup_arn).await;
    assert_eq!(status, 400, "body: {body}");
    assert_eq!(error_code(&body), "BackupNotFoundException");

    // The janitor reclaims the objects and removes the row — poll converged
    // (never a fixed sleep): the row disappears from the replicated catalog
    // entirely, not merely `Expired`.
    timeout(Duration::from_secs(20), async {
        loop {
            if node.metadata().backup(&backup_arn).is_none() {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("backup janitor did not finalize the row within 20s");

    node.shutdown_graceful().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_backup_rejects_an_unknown_table() {
    let dir = TempDir::new().unwrap();
    let (node, config) = support::start_single_node(dir.path(), StorageBackend::default()).await;
    let addr = config.nodes[0].dynamo;
    await_bootstrap(&node).await;

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateBackup",
        r#"{"TableName":"ghost","BackupName":"nightly-1"}"#,
    )
    .await;
    assert_eq!(status, 400, "body: {body}");
    assert_eq!(error_code(&body), "TableNotFoundException");

    node.shutdown_graceful().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn describe_and_delete_backup_reject_an_unknown_arn() {
    let dir = TempDir::new().unwrap();
    let (node, config) = support::start_single_node(dir.path(), StorageBackend::default()).await;
    let addr = config.nodes[0].dynamo;
    await_bootstrap(&node).await;

    let ghost_arn = "arn:aws:dynamodb:animus:0:table/orders/backup/ghost";
    let (status, body) = describe_backup(addr, ghost_arn).await;
    assert_eq!(status, 400, "body: {body}");
    assert_eq!(error_code(&body), "BackupNotFoundException");

    let (status, body) = delete_backup(addr, ghost_arn).await;
    assert_eq!(status, 400, "body: {body}");
    assert_eq!(error_code(&body), "BackupNotFoundException");

    node.shutdown_graceful().await;
}

/// `DeleteBackup` against a still-`CREATING` backup is
/// `BackupInUseException` (AWS-faithful). A populated table (well over the
/// capture driver's own `CHUNK_ROWS`, ADR 0059 §4/§5's row-count-capped
/// chunking) is what makes the `CREATING` window reliably observable —
/// `DeleteBackup` is called immediately, back-to-back with `CreateBackup`'s
/// own response, giving the capture driver no realistic chance to have
/// reached `AVAILABLE` yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_backup_rejects_a_still_creating_backup() {
    let dir = TempDir::new().unwrap();
    let (node, config) = support::start_single_node(dir.path(), StorageBackend::default()).await;
    let addr = config.nodes[0].dynamo;
    await_bootstrap(&node).await;

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        &create_table_body("orders"),
    )
    .await;
    assert_eq!(status, 200, "body: {body}");

    const N: usize = 300;
    for chunk_start in (0..N).step_by(25) {
        let chunk_end = (chunk_start + 25).min(N);
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.BatchWriteItem",
            &batch_put_body_range("orders", chunk_start, chunk_end),
        )
        .await;
        assert_eq!(status, 200, "body: {body}");
    }

    let (status, created) = create_backup(addr, "orders", "nightly-1").await;
    assert_eq!(status, 200, "body: {created}");
    let backup_arn = created["BackupDetails"]["BackupArn"]
        .as_str()
        .expect("BackupArn")
        .to_owned();

    let (status, body) = delete_backup(addr, &backup_arn).await;
    assert_eq!(status, 400, "body: {body}");
    assert_eq!(error_code(&body), "BackupInUseException");

    node.shutdown_graceful().await;
}

/// `ListBackups` pagination (`Limit`/`ExclusiveStartBackupArn`) — three
/// backups of the same table, paged one at a time in ascending-ARN order
/// (`Metadata::backups`' own `BTreeMap` order).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_backups_paginates_with_limit_and_cursor() {
    let dir = TempDir::new().unwrap();
    let (node, config) = support::start_single_node(dir.path(), StorageBackend::default()).await;
    let addr = config.nodes[0].dynamo;
    await_bootstrap(&node).await;

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        &create_table_body("orders"),
    )
    .await;
    assert_eq!(status, 200, "body: {body}");

    let mut arns = Vec::new();
    for i in 0..3 {
        let (status, created) = create_backup(addr, "orders", &format!("backup-{i}")).await;
        assert_eq!(status, 200, "body: {created}");
        arns.push(
            created["BackupDetails"]["BackupArn"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    arns.sort();

    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let body_json = match &cursor {
            None => r#"{"TableName":"orders","Limit":1}"#.to_owned(),
            Some(c) => {
                format!(r#"{{"TableName":"orders","Limit":1,"ExclusiveStartBackupArn":"{c}"}}"#)
            }
        };
        let (status, body) = dynamo(addr, "DynamoDB_20120810.ListBackups", &body_json).await;
        assert_eq!(status, 200, "body: {body}");
        let v = json(&body);
        let page = v["BackupSummaries"].as_array().unwrap();
        assert_eq!(
            page.len(),
            1,
            "each page must have exactly Limit=1 item: {v}"
        );
        seen.push(page[0]["BackupArn"].as_str().unwrap().to_owned());
        match v.get("LastEvaluatedBackupArn").and_then(|c| c.as_str()) {
            Some(next) => cursor = Some(next.to_owned()),
            None => break,
        }
    }
    assert_eq!(
        seen, arns,
        "pagination must walk every backup exactly once, in ARN order"
    );

    node.shutdown_graceful().await;
}
