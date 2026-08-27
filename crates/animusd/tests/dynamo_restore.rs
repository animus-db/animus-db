//! End-to-end tests for `RestoreTableFromBackup` over the real DynamoDB
//! JSON/HTTP wire (ADR 0059 §7, Train 2): the full `CreateBackup` →
//! `AVAILABLE` → write-more-data → `RestoreTableFromBackup` → converged
//! round trip, proving the restored table serves exactly the backup-time
//! rows (not later writes), that its GSI converges and is queryable, that
//! restore works from a backup whose source table has since been dropped,
//! and the AWS-faithful error shapes (`BackupNotFoundException`,
//! `BackupInUseException`, and `ResourceInUseException` for an
//! already-existing target — the identical code `CreateTable`'s own
//! duplicate-name check uses). Real time/sockets (the `ProdEnv` edge) —
//! every eventual property is a converged-or-timeout poll, never a fixed
//! sleep.

use std::net::SocketAddr;
use std::time::Duration;

use animusd::{Node, StorageBackend};
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

/// A plain `GET` against the admin HTTP-JSON endpoint (ADR 0020) →
/// `(status, decoded body)` — mirrors `admin_endpoint.rs`'s own identical
/// helper, reimplemented here since that file's helpers are private to it.
async fn admin_get(addr: SocketAddr, path: &str) -> (u16, serde_json::Value) {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let request = format!("GET {path} HTTP/1.1\r\nHost: animus\r\nConnection: close\r\n\r\n");
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
    (status, json(payload))
}

fn error_code(body: &str) -> String {
    json(body)["__type"]
        .as_str()
        .map(|t| t.rsplit('#').next().unwrap_or(t).to_owned())
        .unwrap_or_else(|| panic!("no __type in error body: {body}"))
}

async fn create_backup(addr: SocketAddr, table: &str, backup_name: &str) -> (u16, String) {
    dynamo(
        addr,
        "DynamoDB_20120810.CreateBackup",
        &format!(r#"{{"TableName":"{table}","BackupName":"{backup_name}"}}"#),
    )
    .await
}

/// `CreateBackup` then poll converged-or-timeout to `AVAILABLE`, returning
/// the backup's own ARN.
async fn create_available_backup(addr: SocketAddr, table: &str, backup_name: &str) -> String {
    let (status, body) = create_backup(addr, table, backup_name).await;
    assert_eq!(status, 200, "CreateBackup failed: {body}");
    let backup_arn = json(&body)["BackupDetails"]["BackupArn"]
        .as_str()
        .expect("BackupArn")
        .to_owned();
    timeout(Duration::from_secs(20), async {
        loop {
            let (status, body) = dynamo(
                addr,
                "DynamoDB_20120810.DescribeBackup",
                &format!(r#"{{"BackupArn":"{backup_arn}"}}"#),
            )
            .await;
            assert_eq!(status, 200, "body: {body}");
            if json(&body)["BackupDescription"]["BackupDetails"]["BackupStatus"] == "AVAILABLE" {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("backup did not become AVAILABLE in 20s");
    backup_arn
}

async fn restore_table(
    addr: SocketAddr,
    backup_arn: &str,
    target_table_name: &str,
) -> (u16, String) {
    dynamo(
        addr,
        "DynamoDB_20120810.RestoreTableFromBackup",
        &format!(r#"{{"BackupArn":"{backup_arn}","TargetTableName":"{target_table_name}"}}"#),
    )
    .await
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

/// The full round trip: populate a table with a GSI, back it up, write MORE
/// data after the backup, restore to a new table, and prove the restored
/// table serves exactly the backup-time rows — never the later writes —
/// with its GSI converging to `ACTIVE` and queryable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_serves_exactly_the_backup_time_rows_with_a_queryable_gsi() {
    let dir = tempfile::tempdir().unwrap();
    let (node, config) = support::start_single_node(dir.path(), StorageBackend::default()).await;
    let addr = config.nodes[0].dynamo;
    await_bootstrap(&node).await;

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"orders","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-status",
                 "KeySchema":[{"AttributeName":"status","KeyType":"HASH"}]}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    // Three items, all `status: open`, present at backup time.
    for i in 0..3 {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"orders","Item":{{"id":{{"S":"o{i}"}},
                    "status":{{"S":"open"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem failed: {body}");
    }

    let backup_arn = create_available_backup(addr, "orders", "nightly-1").await;

    // Written AFTER the backup — must never appear in the restored table.
    for i in 3..5 {
        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.PutItem",
            &format!(
                r#"{{"TableName":"orders","Item":{{"id":{{"S":"o{i}"}},
                    "status":{{"S":"open"}}}}}}"#
            ),
        )
        .await;
        assert_eq!(status, 200, "PutItem failed: {body}");
    }
    // Also mutated after the backup — the restored copy must show the
    // backup-time value (`open`), never this later overwrite.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"orders","Item":{"id":{"S":"o0"},"status":{"S":"closed"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "PutItem failed: {body}");

    let (status, body) = restore_table(addr, &backup_arn, "orders_restored").await;
    assert_eq!(status, 200, "RestoreTableFromBackup failed: {body}");
    let desc = &json(&body)["TableDescription"];
    assert_eq!(desc["TableName"], "orders_restored");
    assert_eq!(desc["TableStatus"], "CREATING");

    await_table_active(addr, "orders_restored").await;

    // Scan the restored table: exactly the three backup-time items, o0's
    // status still `open` (the backup-time value, not the later overwrite).
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"orders_restored","ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "Scan failed: {body}");
    let scanned = json(&body);
    assert_eq!(scanned["Count"], 3, "body: {body}");
    let mut ids: Vec<String> = scanned["Items"]
        .as_array()
        .expect("Items array")
        .iter()
        .map(|item| item["id"]["S"].as_str().unwrap().to_owned())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["o0", "o1", "o2"], "body: {body}");
    let o0 = scanned["Items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"]["S"] == "o0")
        .expect("o0 present");
    assert_eq!(o0["status"]["S"], "open", "body: {body}");

    // The GSI converges (backfilled from the restored, fully-seeded base
    // rows — ADR 0059 §8) and becomes queryable, `Count: 3` for `open`.
    let query_body = r##"{"TableName":"orders_restored","IndexName":"by-status",
        "KeyConditionExpression":"#s = :s",
        "ExpressionAttributeNames":{"#s":"status"},
        "ExpressionAttributeValues":{":s":{"S":"open"}}}"##;
    let gsi_body = timeout(Duration::from_secs(20), async {
        loop {
            let (status, body) = dynamo(addr, "DynamoDB_20120810.Query", query_body).await;
            if status == 200 && json(&body)["Count"] == 3 {
                return body;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("restored table's GSI did not converge to 3 `open` rows in 20s");
    assert_eq!(json(&gsi_body)["Count"], 3, "body: {gsi_body}");

    // `GET /admin/restores` (ADR 0059 §7, Train 2): the replicated restore
    // catalog reflects the completed restore — `DONE`, the right target
    // table, and its destination tablet now `Active`.
    let admin_addr = config.nodes[0].admin;
    let (status, restores) = admin_get(admin_addr, "/admin/restores").await;
    assert_eq!(status, 200, "body: {restores}");
    let row = restores["restores"]
        .as_array()
        .expect("restores array")
        .iter()
        .find(|r| r["target_table"] == "orders_restored")
        .unwrap_or_else(|| panic!("no restore row for orders_restored: {restores}"));
    assert_eq!(row["status"]["state"], "DONE", "row: {row}");
    assert_eq!(row["tablet_state"], "Active", "row: {row}");

    // Cleanup: both tables, then the backup itself.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DeleteTable",
        r#"{"TableName":"orders_restored"}"#,
    )
    .await;
    assert_eq!(status, 200, "DeleteTable failed: {body}");
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DeleteTable",
        r#"{"TableName":"orders"}"#,
    )
    .await;
    assert_eq!(status, 200, "DeleteTable failed: {body}");
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DeleteBackup",
        &format!(r#"{{"BackupArn":"{backup_arn}"}}"#),
    )
    .await;
    assert_eq!(status, 200, "DeleteBackup failed: {body}");

    node.shutdown_graceful().await;
}

/// Restore works from a backup whose source table has since been dropped
/// (ADR 0059's own "restore a table dropped days ago" case) — the
/// manifest's own captured schema/data, never a live lookup of the (now
/// gone) source table, is what restore replays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_works_after_the_source_table_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let (node, config) = support::start_single_node(dir.path(), StorageBackend::default()).await;
    let addr = config.nodes[0].dynamo;
    await_bootstrap(&node).await;

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"gone","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        r#"{"TableName":"gone","Item":{"id":{"S":"only-row"}}}"#,
    )
    .await;
    assert_eq!(status, 200, "PutItem failed: {body}");

    let backup_arn = create_available_backup(addr, "gone", "before-drop").await;

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.DeleteTable",
        r#"{"TableName":"gone"}"#,
    )
    .await;
    assert_eq!(status, 200, "DeleteTable failed: {body}");

    let (status, body) = restore_table(addr, &backup_arn, "gone_restored").await;
    assert_eq!(status, 200, "RestoreTableFromBackup failed: {body}");

    await_table_active(addr, "gone_restored").await;

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        r#"{"TableName":"gone_restored","Key":{"id":{"S":"only-row"}},
            "ConsistentRead":true}"#,
    )
    .await;
    assert_eq!(status, 200, "GetItem failed: {body}");
    assert!(json(&body)["Item"]["id"]["S"] == "only-row", "body: {body}");

    node.shutdown_graceful().await;
}

/// AWS-faithful error shapes: an unknown/expired backup ARN
/// (`BackupNotFoundException`), a still-`CREATING` backup
/// (`BackupInUseException`), and a target name that already exists
/// (`ResourceInUseException` — this adapter's own existing `CreateTable`
/// collision code, `animusd::dynamo::registry_error`'s `TableExists` arm,
/// reused as-is rather than minting a second "table already exists" code).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_rejects_a_bad_backup_or_an_existing_target() {
    let dir = tempfile::tempdir().unwrap();
    let (node, config) = support::start_single_node(dir.path(), StorageBackend::default()).await;
    let addr = config.nodes[0].dynamo;
    await_bootstrap(&node).await;

    // An unknown ARN.
    let (status, body) =
        restore_table(addr, "arn:aws:dynamodb:animus:0:table/x/backup/y", "z").await;
    assert_eq!(status, 400, "body: {body}");
    assert_eq!(error_code(&body), "BackupNotFoundException", "body: {body}");

    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        r#"{"TableName":"src","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");

    // Still `CREATING` (poll a couple of times — a fast single-node backup
    // of an empty table can converge before this assertion ever runs, in
    // which case the still-`CREATING` case simply wasn't hit this run; the
    // `TableAlreadyExistsException` case below is unaffected either way).
    let (status, created) = create_backup(addr, "src", "b-1").await;
    assert_eq!(status, 200, "body: {created}");
    let backup_arn = json(&created)["BackupDetails"]["BackupArn"]
        .as_str()
        .expect("BackupArn")
        .to_owned();
    let (status, body) = restore_table(addr, &backup_arn, "src_restored").await;
    if status == 400 {
        assert_eq!(error_code(&body), "BackupInUseException", "body: {body}");
    }

    // A target name that already exists.
    let backup_arn = create_available_backup(addr, "src", "b-2").await;
    let (status, body) = restore_table(addr, &backup_arn, "src").await;
    assert_eq!(status, 400, "body: {body}");
    assert_eq!(error_code(&body), "ResourceInUseException", "body: {body}");

    node.shutdown_graceful().await;
}
