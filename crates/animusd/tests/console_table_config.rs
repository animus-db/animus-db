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
//!
//! **ADR 0061 rung H, C-08 PR 4**: five of this file's original nine tests
//! converted to deterministic `SimCluster` siblings in
//! `crates/animusd/src/sim_cluster_console_table_config.rs`
//! (`table_detail_projects_full_configuration`, `stream_toggle_round_
//! trips`, `ttl_set_and_clear_round_trips`, `delete_table_works`,
//! `table_detail_with_no_pitr_or_backups_is_null_and_empty`); **C-10 PR 6**
//! (ADR 0061 rung J) converted three more — `add_and_drop_gsi_round_trip`,
//! `add_gsi_records_a_declared_attribute_type`, `add_gsi_rejects_an_
//! unknown_attribute_type` — once PR 2 closed blocker (d)
//! (`dispatch_table_op`'s missing index-change sub-arm). The one test left
//! below, `table_detail_shows_pitr_status_and_backups`, stays `ProdEnv`: its
//! own reason comment explains why (`UpdateContinuousBackups` has no
//! generic-dispatch arm, and this test also needs the real `pitr_snapshot_
//! loop`'s wall-clock-timed capture driver, which `SimCluster` does not
//! run).

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

/// U-03's round trip: enable continuous backups (PITR) and create an
/// on-demand backup, then confirm the table detail page reports both — the
/// exact fields `DescribeContinuousBackups`/`ListBackups` themselves would
/// report, sourced from the same catalog reads (`animusd::dynamo::
/// pitr_description`/`backup_wire_status`), never re-derived — and nothing
/// cluster-shaped alongside them.
///
/// **KEPT `ProdEnv` — the sole test left in this file.** This rung's brief
/// allowed converting this test only if the PITR data it reads is
/// producible under `SimCluster` via a generic `UpdateContinuousBackups`
/// path — checked against the code and there isn't one:
/// `Operation::UpdateContinuousBackups` is absent from both `dispatch_
/// item_op`'s and `dispatch_table_op`'s `match` arms (`dynamo.rs`), so it
/// falls to `unsupported_by_generic_dispatch`, unlike `CreateBackup`/
/// `DeleteBackup`/`UpdateTimeToLive` (C-08 PR 2) and the GSI-DDL
/// `UpdateTable` sub-arm (C-10 PR 2) which did widen. This test also needs
/// the real `pitr_snapshot_loop`'s own wall-clock-timed capture driver
/// (issue #593's own race, guarded against by the poll below), which
/// `SimCluster` does not run. Backup/PITR data stays a separate residual,
/// per the brief's own fallback reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn table_detail_shows_pitr_status_and_backups() {
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
                "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],
                "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateTable failed: {body}");

        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.UpdateContinuousBackups",
            r#"{"TableName":"orders",
                "PointInTimeRecoverySpecification":{"PointInTimeRecoveryEnabled":true}}"#,
        )
        .await;
        assert_eq!(
            status, 200,
            "UpdateContinuousBackups(enable) failed: {body}"
        );

        let (status, body) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.CreateBackup",
            r#"{"TableName":"orders","BackupName":"snap-1"}"#,
        )
        .await;
        assert_eq!(status, 200, "CreateBackup failed: {body}");
        let backup_arn = json(&body)["BackupDetails"]["BackupArn"]
            .as_str()
            .expect("BackupArn")
            .to_string();

        // Issue #593 regression: enabling PITR makes `pitr_snapshot_loop`
        // propose its own internally-triggered base snapshot
        // (`BeginBackup { pitr_base: true, .. }`) on its own 200ms tick
        // cadence, racing this test's own `CreateBackup`. Before the fix,
        // the loop minted that row via `BeginBackup` and only THEN tagged
        // it via a separate `MarkBackupPitrBase` commit — a real committed
        // window in which the internal row was an ordinary, untagged
        // `Creating` backup, indistinguishable from the user's own
        // `CreateBackup` and double-counted by this console projection
        // (which mirrors `ListBackups`' default `USER`-only filter,
        // `Metadata::pitr_base_backups`). The fix folds the tag into
        // `BeginBackup` itself, so no committed state should ever show 2
        // backups here. Poll repeatedly through several of the loop's own
        // tick intervals and fail on the FIRST sighting of a second
        // (untagged) row — not a converged-or-timeout tolerance of a
        // transient miscount, which would only prove the race eventually
        // resolves, not that it never happens.
        let poll_window = tokio::time::Instant::now() + Duration::from_secs(5);
        let (d, body) = loop {
            let (status, body) =
                console(console_addr, "GET", "/console/api/tables/orders", "").await;
            assert_eq!(status, 200, "table detail failed: {body}");
            let d = json(&body);
            let count = d["backups"].as_array().map_or(0, Vec::len);
            assert!(
                count <= 1,
                "a PITR base snapshot was observably untagged: saw {count} \
                 backups (expected at most the user's own one): {body}"
            );
            if count == 1 {
                break (d, body);
            }
            assert!(
                tokio::time::Instant::now() < poll_window,
                "backups never reached the user's own one: {body}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        // Keep polling for the rest of the window: the user's own backup is
        // now visible, but `pitr_snapshot_loop` may still fire its own
        // internally-triggered snapshot within this test's lifetime — it
        // must never appear as a second row either.
        while tokio::time::Instant::now() < poll_window {
            let (status, body) =
                console(console_addr, "GET", "/console/api/tables/orders", "").await;
            assert_eq!(status, 200, "table detail failed: {body}");
            let count = json(&body)["backups"].as_array().map_or(0, Vec::len);
            assert!(
                count <= 1,
                "a PITR base snapshot was observably untagged: saw {count} \
                 backups (expected at most the user's own one): {body}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let pitr = &d["pitr"];
        assert!(!pitr.is_null(), "PITR enabled: {body}");
        assert!(
            pitr["earliest_restorable_ms"].as_u64().unwrap() > 0,
            "earliest_restorable_ms: {body}"
        );
        assert!(
            pitr["latest_restorable_ms"].as_u64().unwrap() > 0,
            "latest_restorable_ms: {body}"
        );

        let backups = d["backups"].as_array().unwrap();
        assert_eq!(backups.len(), 1, "exactly one backup: {body}");
        assert_eq!(backups[0]["backup_id"], backup_arn);
        let status_label = backups[0]["status"].as_str().unwrap();
        assert!(
            status_label == "CREATING" || status_label == "AVAILABLE",
            "unexpected status {status_label}: {body}"
        );
        assert!(
            backups[0]["created_wall_ms"].as_u64().unwrap() > 0,
            "created_wall_ms: {body}"
        );
        assert_no_cluster_shape(&body);

        node.shutdown_graceful().await;
    })
    .await
    .expect("test timed out");
}
