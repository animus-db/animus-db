//! End-to-end tests for point-in-time recovery (PITR, ADR 0059 §9, Train 3)
//! over the real DynamoDB JSON/HTTP wire: `UpdateContinuousBackups`/
//! `DescribeContinuousBackups`, the "enable starts the window at now, a
//! disable+re-enable resets it" contract. Real time/sockets (the `ProdEnv`
//! edge) — every eventual property is a converged-or-timeout poll, never a
//! fixed sleep (this codebase's own testing discipline).
//!
//! The `UpdateContinuousBackups` follower-relay regression
//! (`is_relayable_command` must allow `MetaCommand::UpdateContinuousBackups`)
//! lives in `tests/schema_ddl_relay.rs`, mirroring that file's own DDL-relay
//! suite — not duplicated here.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use animusd::config::NodeRole;
use animusd::{
    ClusterConfig, Node, RoleAddrs, SegmentStoreConfig, StorageBackend, StreamSealKnobs,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// Tiny seal-size trigger so a PITR segment seals within a test's own
/// budget, mirroring `streams_e2e.rs`'s `tiny_seal_knobs` idea — never wait
/// out a production-scale trigger.
const TEST_SEAL_KNOBS: StreamSealKnobs = StreamSealKnobs {
    seal_bytes: 200,
    seal_age: Duration::from_secs(3600),
};

/// Bring up a single node with [`TEST_SEAL_KNOBS`], retrying the port-TOCTOU
/// race exactly like `support::start_single_node` does.
async fn start_single_node_fast_seal(dir: &Path) -> (Node, ClusterConfig) {
    start_single_node_with_knobs(dir, TEST_SEAL_KNOBS).await
}

/// [`start_single_node_fast_seal`] generalized to a caller-chosen
/// [`StreamSealKnobs`] — the knob a test wants when it needs to tune how
/// eagerly the periodic size-triggered seal arm (`INDEX_DRAIN_INTERVAL`,
/// 200ms) fires, e.g. issue #572's dueling-seal race regression.
async fn start_single_node_with_knobs(dir: &Path, knobs: StreamSealKnobs) -> (Node, ClusterConfig) {
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
            }],
            dynamo_auth: None,
            cluster_settings: None,
        };
        match animusd::run_node_with_streams(
            &config,
            0,
            dir,
            StorageBackend::default(),
            Duration::from_secs(600),
            knobs,
            SegmentStoreConfig::default(),
            animusd::DEFAULT_STREAM_RETENTION,
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
    panic!("single node (fast seal) failed to start after 10 attempts: {last_err:?}");
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

async fn create_base_table(addr: SocketAddr, table: &str) {
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

async fn update_continuous_backups(addr: SocketAddr, table: &str, enabled: bool) -> (u16, String) {
    dynamo(
        addr,
        "DynamoDB_20120810.UpdateContinuousBackups",
        &format!(
            r#"{{"TableName":"{table}","PointInTimeRecoverySpecification":{{"PointInTimeRecoveryEnabled":{enabled}}}}}"#
        ),
    )
    .await
}

async fn describe_continuous_backups(addr: SocketAddr, table: &str) -> (u16, String) {
    dynamo(
        addr,
        "DynamoDB_20120810.DescribeContinuousBackups",
        &format!(r#"{{"TableName":"{table}"}}"#),
    )
    .await
}

async fn put_item_padded(addr: SocketAddr, table: &str, id: &str, pad_len: usize) {
    let pad = "x".repeat(pad_len);
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        &format!(
            r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"val":{{"S":"{pad}"}}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "PutItem({id}) failed: {body}");
}

async fn await_true<F: Fn() -> bool>(secs: u64, what: &str, cond: F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while !cond() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        sleep(Duration::from_millis(50)).await;
    }
}

/// `UpdateContinuousBackups` against an unknown table is `TableNotFoundException`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_continuous_backups_rejects_unknown_table() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (node, _config) = start_single_node_fast_seal(dir.path()).await;
        let (status, body) =
            update_continuous_backups(node.dynamo_addr(), "no-such-table", true).await;
        assert_eq!(status, 400, "expected a client error: {body}");
        assert_eq!(error_code(&body), "TableNotFoundException");
    })
    .await
    .expect("did not converge in time");
}

/// `DescribeContinuousBackups` before ever enabling PITR reports `DISABLED`
/// with no restorable window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn describe_continuous_backups_disabled_by_default() {
    timeout(Duration::from_secs(30), async {
        let dir = support::panic_safe_tempdir();
        let (node, _config) = start_single_node_fast_seal(dir.path()).await;
        let table = "orders";
        create_base_table(node.dynamo_addr(), table).await;

        let (status, body) = describe_continuous_backups(node.dynamo_addr(), table).await;
        assert_eq!(status, 200, "DescribeContinuousBackups failed: {body}");
        let v = json(&body);
        let pitr = &v["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"];
        assert_eq!(
            pitr["PointInTimeRecoveryStatus"].as_str().unwrap(),
            "DISABLED"
        );
        assert!(pitr.get("EarliestRestorableDateTime").is_none());
        assert!(pitr.get("LatestRestorableDateTime").is_none());
    })
    .await
    .expect("did not converge in time");
}

/// **The full lifecycle**: enable PITR → write → `DescribeContinuousBackups`
/// shows a sane window (`ENABLED`, `Earliest <= Latest <= now`) → disable
/// (reports `DISABLED` again, no window) → re-enable resets the window (a
/// fresh `EarliestRestorableDateTime` at-or-after the re-enable moment,
/// never inheriting the first window's earlier start).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enable_write_describe_disable_reenable_resets_the_window() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let (node, _config) = start_single_node_fast_seal(dir.path()).await;
        let table = "orders";
        create_base_table(node.dynamo_addr(), table).await;

        let now_ms_before_enable = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let (status, body) = update_continuous_backups(node.dynamo_addr(), table, true).await;
        assert_eq!(
            status, 200,
            "UpdateContinuousBackups(enable) failed: {body}"
        );
        let v = json(&body);
        let pitr = &v["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"];
        assert_eq!(
            pitr["PointInTimeRecoveryStatus"].as_str().unwrap(),
            "ENABLED"
        );
        let first_earliest = pitr["EarliestRestorableDateTime"].as_f64().unwrap();

        for i in 0..10u32 {
            put_item_padded(node.dynamo_addr(), table, &format!("o{i}"), 50).await;
        }

        // A sane window: `DescribeContinuousBackups` reports `ENABLED` with
        // `Earliest <= Latest <= now` (never claiming "now" itself — ADR
        // 0059 §9's own "honestly trail now by seal lag" rule — but never
        // ahead of it either).
        await_true(
            20,
            "a PITR segment seals so Latest advances past Earliest",
            || {
                let meta = node.metadata();
                meta.pitr_segments.values().any(|row| row.table == table)
            },
        )
        .await;
        let (status, body) = describe_continuous_backups(node.dynamo_addr(), table).await;
        assert_eq!(status, 200, "DescribeContinuousBackups failed: {body}");
        let v = json(&body);
        let pitr = &v["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"];
        assert_eq!(
            pitr["PointInTimeRecoveryStatus"].as_str().unwrap(),
            "ENABLED"
        );
        let earliest = pitr["EarliestRestorableDateTime"].as_f64().unwrap();
        let latest = pitr["LatestRestorableDateTime"].as_f64().unwrap();
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        assert!(
            earliest <= latest,
            "Earliest ({earliest}) must not exceed Latest ({latest})"
        );
        assert!(
            latest <= now_secs + 5.0,
            "Latest ({latest}) must never claim the future"
        );
        assert!(
            (earliest * 1000.0) >= (now_ms_before_enable as f64) - 1000.0,
            "Earliest ({earliest}) must not predate this generation's own enable moment"
        );

        // Disable: reports DISABLED, no window.
        let (status, body) = update_continuous_backups(node.dynamo_addr(), table, false).await;
        assert_eq!(
            status, 200,
            "UpdateContinuousBackups(disable) failed: {body}"
        );
        let v = json(&body);
        let pitr = &v["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"];
        assert_eq!(
            pitr["PointInTimeRecoveryStatus"].as_str().unwrap(),
            "DISABLED"
        );
        assert!(pitr.get("EarliestRestorableDateTime").is_none());

        // Re-enable: a fresh window, never claiming continuity with the
        // disabled interval — its own EarliestRestorableDateTime is at or
        // after this very moment, strictly not tied to `first_earliest`.
        let now_ms_before_reenable = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let (status, body) = update_continuous_backups(node.dynamo_addr(), table, true).await;
        assert_eq!(
            status, 200,
            "UpdateContinuousBackups(re-enable) failed: {body}"
        );
        let v = json(&body);
        let pitr = &v["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"];
        assert_eq!(
            pitr["PointInTimeRecoveryStatus"].as_str().unwrap(),
            "ENABLED"
        );
        let second_earliest = pitr["EarliestRestorableDateTime"].as_f64().unwrap();
        assert!(
            (second_earliest * 1000.0) >= (now_ms_before_reenable as f64) - 1000.0,
            "the re-enabled window's own Earliest ({second_earliest}) must start at the \
             re-enable moment, not inherit the disabled generation's earlier one \
             ({first_earliest})"
        );
        assert!(
            second_earliest >= first_earliest,
            "a fresh window never starts earlier than the one it replaced"
        );
        assert_eq!(
            node.metadata().table_pitr(table).map(|s| s.generation),
            Some(2),
            "re-enable mints a fresh generation, never reusing the first"
        );
    })
    .await
    .expect("did not converge in time");
}

/// Issue #572 regression: `UpdateContinuousBackups(disable)`'s own final
/// seal (`ClientCtx::force_pitr_seal_tablet`) must retry a dueling-seal race
/// against the periodic size-triggered arm (`pitr_tick`, `INDEX_DRAIN_
/// INTERVAL` = 200ms) on the `CpRoute::Local` route exactly as it already
/// does on `CpRoute::Forward` — not surface the transient "lost to a
/// concurrent seal ...; retry" error as a hard 500.
///
/// A single-node cluster is deliberate: with one node, the tablet leader is
/// always *this* node, so `resolve_cp_route` can only ever return
/// `CpRoute::Local` — every disable call in this test exercises exactly the
/// route the bug lives on, with no ambiguity about which branch ran.
///
/// The race itself needs both arms computing the identical `(tablet,
/// next_epoch)` slot from overlapping stale views: a small `seal_bytes`
/// keeps the periodic arm sealing on nearly every 200ms tick, and several
/// concurrent `PutItem` writers keep running *through* each disable call
/// (not just before it) so a periodic seal can commit mid-flight while the
/// disable's own force-seal is still computing/proposing its own. One
/// attempt reproduces only sporadically (the original report: 1 in 15) —
/// so this loops many enable/write/disable cycles, each a fresh chance at
/// the same race, and asserts every single disable succeeds. On unmodified
/// `main` this fails intermittently with the exact reported signature
/// (500, `SealPitrSegment(..) lost to a concurrent seal ...; retry`); after
/// the fix every cycle converges to 200.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn disable_survives_concurrent_periodic_seal_on_local_route() {
    timeout(Duration::from_secs(90), async {
        let dir = support::panic_safe_tempdir();
        let racy_knobs = StreamSealKnobs {
            seal_bytes: 48,
            seal_age: Duration::from_secs(3600),
        };
        let (node, _config) = start_single_node_with_knobs(dir.path(), racy_knobs).await;
        let table = "orders";
        create_base_table(node.dynamo_addr(), table).await;

        const CYCLES: u32 = 40;
        const WRITERS: u32 = 8;
        for cycle in 0..CYCLES {
            let (status, body) = update_continuous_backups(node.dynamo_addr(), table, true).await;
            assert_eq!(status, 200, "enable (cycle {cycle}) failed: {body}");

            // Several writers hammer PutItem concurrently, kept running
            // through the disable call below (stopped only afterward) so the
            // periodic arm's own seal attempt can race the disable's.
            let stop = Arc::new(AtomicBool::new(false));
            let mut writers = Vec::with_capacity(WRITERS as usize);
            for w in 0..WRITERS {
                let addr = node.dynamo_addr();
                let table = table.to_string();
                let stop = Arc::clone(&stop);
                writers.push(tokio::spawn(async move {
                    let mut i = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        put_item_padded(addr, &table, &format!("c{cycle}-w{w}-{i}"), 24).await;
                        i += 1;
                    }
                }));
            }
            // Give the periodic arm at least one full tick's worth of
            // pending bytes to seal before racing the disable call.
            sleep(Duration::from_millis(120)).await;

            let (status, body) = update_continuous_backups(node.dynamo_addr(), table, false).await;

            stop.store(true, Ordering::Relaxed);
            for w in writers {
                let _ = w.await;
            }

            assert_eq!(
                status, 200,
                "UpdateContinuousBackups(disable) failed on cycle {cycle} — issue #572's \
                 dueling-seal race on the CpRoute::Local route: {body}"
            );
        }

        // One more clean (non-racing) round proves the fix doesn't
        // silently drop coverage across the stress loop above: every write
        // in this fresh generation is fully accounted for by that
        // generation's own sealed segments before the final disable.
        let (status, body) = update_continuous_backups(node.dynamo_addr(), table, true).await;
        assert_eq!(status, 200, "final re-enable failed: {body}");
        let generation = node
            .metadata()
            .table_pitr(table)
            .expect("PITR just enabled")
            .generation;
        const FINAL_WRITES: u64 = 12;
        for i in 0..FINAL_WRITES {
            put_item_padded(node.dynamo_addr(), table, &format!("final-{i}"), 40).await;
        }
        await_true(
            20,
            "this generation's PITR segments cover every final write",
            || {
                let meta = node.metadata();
                let sealed: u64 = meta
                    .pitr_segments
                    .values()
                    .filter(|r| r.table == table && r.generation == generation)
                    .map(|r| r.count)
                    .sum();
                sealed >= FINAL_WRITES
            },
        )
        .await;

        let (status, body) = update_continuous_backups(node.dynamo_addr(), table, false).await;
        assert_eq!(status, 200, "final disable failed: {body}");
    })
    .await
    .expect("did not converge in time");
}
