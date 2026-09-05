//! End-to-end tests for `RestoreTableToPointInTime` (ADR 0059 §10, Train 3
//! PR②) over the real DynamoDB JSON/HTTP wire: enable PITR → timed writes →
//! restore to a mid-point second → exactly the rows as of that second →
//! GSIs converge; `UseLatestRestorableTime`; deleted-table PITR restore; and
//! the AWS-faithful error shapes (`TableNotFoundException`,
//! `PointInTimeRecoveryUnavailableException`, `InvalidRestoreTimeException`).
//! Real time/sockets (the `ProdEnv` edge) — every eventual property is a
//! converged-or-timeout poll, never a fixed sleep (this codebase's own
//! testing discipline); the target restore second is chosen from real
//! wall-clock reads with generous spacing around it, since PITR segment
//! inclusion is only ever precise to one seal interval (see
//! `animus_control::Metadata::pitr_replay_segments`'s own doc).
//!
//! `UpdateContinuousBackups`/`DescribeContinuousBackups`'s own lifecycle
//! coverage lives in `tests/dynamo_pitr.rs` — nothing here duplicates it.
//! `RestoreTableFromBackup`'s own coverage lives in `tests/dynamo_restore.rs`.

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use animusd::config::NodeRole;
use animusd::{
    ClusterConfig, Node, RoleAddrs, SegmentStoreConfig, StorageBackend, StreamSealKnobs,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// Tiny seal-size trigger, mirroring `dynamo_pitr.rs`'s own `TEST_SEAL_KNOBS`
/// — a PITR segment seals within a test's own budget rather than waiting
/// out a production-scale age trigger.
const TEST_SEAL_KNOBS: StreamSealKnobs = StreamSealKnobs {
    seal_bytes: 200,
    seal_age: Duration::from_secs(3600),
};

/// Bring up a single node with [`TEST_SEAL_KNOBS`] and a tiny PITR periodic
/// base-snapshot cadence (never the real 6-hour production default), retrying
/// the port-TOCTOU race exactly like `support::start_single_node` does.
async fn start_single_node_fast_pitr(dir: &Path) -> (Node, ClusterConfig) {
    let mut last_err = None;
    for attempt in 0..10 {
        let addrs = support::free_addrs(6);
        let config = ClusterConfig {
            nodes: vec![RoleAddrs {
                id: animusd::config::node_id(0),
                role: NodeRole::Both,
                internal: addrs[0],
                client: addrs[1],
                dynamo: addrs[2],
                admin: addrs[3],
                intra: addrs[4],
                console: addrs[5],
                advertise_host: None,
                tls: None,
            }],
            dynamo_auth: None,
            cluster_settings: None,
        };
        match animusd::run_node_with_streams_and_pitr_snapshot_cadence(
            &config,
            0,
            dir,
            StorageBackend::default(),
            Duration::from_secs(600),
            TEST_SEAL_KNOBS,
            SegmentStoreConfig::default(),
            animusd::DEFAULT_STREAM_RETENTION,
            Duration::from_millis(200),
        )
        .await
        {
            Ok(node) => return (node, config),
            Err(e) => {
                last_err = Some(e);
                sleep(Duration::from_millis(50 * (attempt + 1))).await;
            }
        }
    }
    panic!("single node (fast PITR) failed to start after 10 attempts: {last_err:?}");
}

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

fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
}

fn error_code(body: &str) -> String {
    json(body)["__type"]
        .as_str()
        .map(|t| t.rsplit('#').next().unwrap_or(t).to_owned())
        .unwrap_or_else(|| panic!("no __type in error body: {body}"))
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

async fn create_table(addr: SocketAddr, table: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable failed: {body}");
}

async fn enable_pitr(addr: SocketAddr, table: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateContinuousBackups",
        &format!(
            r#"{{"TableName":"{table}","PointInTimeRecoverySpecification":{{"PointInTimeRecoveryEnabled":true}}}}"#
        ),
    )
    .await;
    assert_eq!(
        status, 200,
        "UpdateContinuousBackups(enable) failed: {body}"
    );
}

/// A padded `PutItem` — large enough on its own to cross
/// [`TEST_SEAL_KNOBS`]'s tiny `seal_bytes` threshold, so a lone write
/// triggers a PITR seal promptly rather than waiting on several small
/// writes to accumulate (or the 3600s age trigger, never meant to fire in
/// this file's own tests).
async fn put_item_padded(addr: SocketAddr, table: &str, id: &str, val: &str, pad_len: usize) {
    let pad = "x".repeat(pad_len);
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        &format!(
            r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"val":{{"S":"{val}"}},"pad":{{"S":"{pad}"}}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "PutItem({id}) failed: {body}");
}

async fn get_item(addr: SocketAddr, table: &str, id: &str) -> Option<String> {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.GetItem",
        &format!(
            r#"{{"TableName":"{table}","Key":{{"id":{{"S":"{id}"}}}},"ConsistentRead":true}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "GetItem({id}) failed: {body}");
    let v = json(&body);
    v.get("Item")
        .and_then(|item| item.get("val"))
        .and_then(|val| val.get("S"))
        .and_then(|s| s.as_str())
        .map(str::to_owned)
}

async fn restore_to_point_in_time(
    addr: SocketAddr,
    source_table: &str,
    target_table: &str,
    restore_date_time_secs: Option<u64>,
    use_latest: bool,
) -> (u16, String) {
    let time_field = match (restore_date_time_secs, use_latest) {
        (_, true) => r#""UseLatestRestorableTime":true"#.to_owned(),
        (Some(secs), false) => format!(r#""RestoreDateTime":{secs}"#),
        (None, false) => String::new(),
    };
    let body = if time_field.is_empty() {
        format!(r#"{{"SourceTableName":"{source_table}","TargetTableName":"{target_table}"}}"#)
    } else {
        format!(
            r#"{{"SourceTableName":"{source_table}","TargetTableName":"{target_table}",{time_field}}}"#
        )
    };
    dynamo(addr, "DynamoDB_20120810.RestoreTableToPointInTime", &body).await
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

/// Wait for `table` to have sealed one MORE PITR segment than
/// `since_count` already covers, returning `(new_count, seal_secs)` —
/// `seal_secs` is that new segment's own real `seal_wall_ms` (ADR 0059
/// §10's own bug-fixed wall-clock stamp, floored to whole seconds).
/// Reading the segment's OWN stamp directly (rather than an independently
/// read wall clock plus a hoped-for ordering) is what makes a
/// restore-to-exactly-`seal_secs` request deterministically include every
/// write that triggered this seal and none that arrive later — sidestepping
/// this codebase's own documented segment-granularity precision limit
/// (`animus_control::Metadata::pitr_replay_segments`'s own doc) rather than
/// racing it.
async fn await_next_pitr_seal(node: &Node, table: &str, since_count: usize) -> (usize, u64) {
    timeout(Duration::from_secs(20), async {
        loop {
            let meta = node.metadata();
            let mut rows: Vec<(u64, u64)> = meta
                .pitr_segments
                .iter()
                .filter(|(_, row)| row.table == table)
                .map(|((_, epoch), row)| (*epoch, row.seal_wall_ms))
                .collect();
            let count = rows.len();
            if count > since_count {
                rows.sort_by_key(|(epoch, _)| *epoch);
                let (_, seal_wall_ms) = *rows.last().expect("count > since_count");
                return (count, seal_wall_ms / 1000);
            }
            drop(meta);
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no new PITR segment sealed for `{table}` in 20s"))
}

async fn await_pitr_base_snapshot(node: &Node, table: &str) {
    timeout(Duration::from_secs(20), async {
        loop {
            // `MarkBackupPitrBase` tags a base's row while it is still
            // `Creating` (ADR 0059 Train 3 PR① amendment: the tag lands
            // "immediately after its own `BeginBackup` is observed to
            // land") — `RestoreTableToPointInTime`'s own base-selection
            // only ever considers an `Available` row, so this must wait
            // for that, not merely for the tag to exist.
            if node
                .metadata()
                .pitr_base_backups_for_table(table)
                .any(|(_, row)| matches!(row.status, animus_control::BackupStatus::Available))
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("no PITR base snapshot became AVAILABLE in 20s");
}

/// **The full round trip**: enable PITR on a table, write item `a=v1`, let
/// it seal (recording that seal's own real wall-clock second as the
/// restore's mid-point target), let a PITR base snapshot land, then write
/// `a=v2` and a brand-new item `b=v1` and let THAT seal too, then
/// `RestoreTableToPointInTime` to the recorded mid-point second — the
/// restored table must show `a=v1` and no `b` at all (the writes after the
/// target second never happened, from the restored table's point of view).
/// `UseLatestRestorableTime` is proven in the same test, into a second
/// target table, showing `a=v2` and `b=v1` (everything written).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_to_a_mid_point_second_shows_exactly_the_rows_as_of_then() {
    timeout(Duration::from_secs(90), async {
        let dir = support::panic_safe_tempdir();
        let (node, config) = start_single_node_fast_pitr(dir.path()).await;
        let addr = config.nodes[0].dynamo;
        let table = "orders";
        create_table(addr, table).await;
        enable_pitr(addr, table).await;

        // Let the FIRST PITR base snapshot land (`ListBackups(SYSTEM)` —
        // the internally-triggered `BeginBackup` `pitr_snapshot_loop`
        // proposes, ADR 0059 §9's own "reusing Train 1 unchanged") BEFORE
        // any of the writes below: `RestoreTableToPointInTime` picks the
        // newest base at or before its own target second, so the base
        // must exist, and be no younger than, whatever second the restore
        // below targets — ordering it first sidesteps a race against the
        // snapshot loop's own multi-tick completion latency entirely.
        await_pitr_base_snapshot(&node, table).await;

        put_item_padded(addr, table, "a", "v1", 250).await;
        let (sealed, target_secs) = await_next_pitr_seal(&node, table, 0).await;

        // A real second boundary between the two write bursts — restore's
        // own segment-inclusion cutoff is only ever precise to the whole
        // second (`Metadata::pitr_replay_segments`'s own doc), so the two
        // bursts' own seals must land in genuinely different seconds for
        // `target_secs` to distinguish them at all.
        sleep(Duration::from_secs(2)).await;

        put_item_padded(addr, table, "a", "v2", 250).await;
        put_item_padded(addr, table, "b", "v1", 250).await;
        await_next_pitr_seal(&node, table, sealed).await;

        // Restore to the recorded mid-point second.
        let (status, body) =
            restore_to_point_in_time(addr, table, "orders_mid", Some(target_secs), false).await;
        assert_eq!(status, 200, "RestoreTableToPointInTime failed: {body}");
        let desc = &json(&body)["TableDescription"];
        assert_eq!(desc["TableName"], "orders_mid");
        assert_eq!(desc["TableStatus"], "CREATING");
        await_table_active(addr, "orders_mid").await;

        assert_eq!(
            get_item(addr, "orders_mid", "a").await,
            Some("v1".to_owned()),
            "item `a` must show its mid-point value, not the later overwrite"
        );
        assert_eq!(
            get_item(addr, "orders_mid", "b").await,
            None,
            "item `b` did not exist yet as of the mid-point second"
        );

        // Restore to `UseLatestRestorableTime`: both later writes present.
        let (status, body) =
            restore_to_point_in_time(addr, table, "orders_latest", None, true).await;
        assert_eq!(status, 200, "RestoreTableToPointInTime failed: {body}");
        await_table_active(addr, "orders_latest").await;
        assert_eq!(
            get_item(addr, "orders_latest", "a").await,
            Some("v2".to_owned())
        );
        assert_eq!(
            get_item(addr, "orders_latest", "b").await,
            Some("v1".to_owned())
        );
    })
    .await
    .expect("did not converge in time");
}

/// **Deleted-table PITR restore**: the catalog + segments survive
/// `DeleteTable` (ADR 0059 §9/§10's own "a dropped table's PITR history
/// survives" carve-out) — a restore to a second recorded before the drop
/// still works, from a brand-new schema sourced from the base snapshot's
/// own manifest (never live `Metadata`, which no longer has one).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleted_table_pitr_restore_within_the_window_still_works() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (node, config) = start_single_node_fast_pitr(dir.path()).await;
        let addr = config.nodes[0].dynamo;
        let table = "orders";
        create_table(addr, table).await;
        enable_pitr(addr, table).await;

        // Order matters — see the mid-point test's identical comment.
        await_pitr_base_snapshot(&node, table).await;
        put_item_padded(addr, table, "a", "v1", 250).await;
        let (_, target_secs) = await_next_pitr_seal(&node, table, 0).await;

        let (status, body) = dynamo(
            addr,
            "DynamoDB_20120810.DeleteTable",
            &format!(r#"{{"TableName":"{table}"}}"#),
        )
        .await;
        assert_eq!(status, 200, "DeleteTable failed: {body}");

        let (status, body) =
            restore_to_point_in_time(addr, table, "orders_after_drop", Some(target_secs), false)
                .await;
        assert_eq!(status, 200, "RestoreTableToPointInTime failed: {body}");
        await_table_active(addr, "orders_after_drop").await;
        assert_eq!(
            get_item(addr, "orders_after_drop", "a").await,
            Some("v1".to_owned())
        );
    })
    .await
    .expect("did not converge in time");
}

/// The AWS-faithful error shapes: an unknown source table
/// (`TableNotFoundException`), a real table that never enabled PITR
/// (`PointInTimeRecoveryUnavailableException`), and a `RestoreDateTime`
/// outside the currently-restorable window (`InvalidRestoreTimeException`)
/// — both a time far in the future and one before the generation's own
/// enable moment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_to_point_in_time_error_shapes() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (_node, config) = start_single_node_fast_pitr(dir.path()).await;
        let addr = config.nodes[0].dynamo;

        // Unknown source: no table, no PITR history at all.
        let (status, body) =
            restore_to_point_in_time(addr, "no-such-table", "t1", Some(now_secs()), false).await;
        assert_eq!(status, 400, "expected a client error: {body}");
        assert_eq!(error_code(&body), "TableNotFoundException");

        // A real table that has never enabled PITR.
        create_table(addr, "plain").await;
        let (status, body) =
            restore_to_point_in_time(addr, "plain", "t2", Some(now_secs()), false).await;
        assert_eq!(status, 400, "expected a client error: {body}");
        assert_eq!(error_code(&body), "PointInTimeRecoveryUnavailableException");

        // PITR enabled, but the requested second is far in the future.
        create_table(addr, "orders").await;
        enable_pitr(addr, "orders").await;
        let far_future = now_secs() + 100_000;
        let (status, body) =
            restore_to_point_in_time(addr, "orders", "t3", Some(far_future), false).await;
        assert_eq!(status, 400, "expected a client error: {body}");
        assert_eq!(error_code(&body), "InvalidRestoreTimeException");

        // Before this generation's own enable moment.
        let (status, body) = restore_to_point_in_time(addr, "orders", "t4", Some(0), false).await;
        assert_eq!(status, 400, "expected a client error: {body}");
        assert_eq!(error_code(&body), "InvalidRestoreTimeException");
    })
    .await
    .expect("did not converge in time");
}
