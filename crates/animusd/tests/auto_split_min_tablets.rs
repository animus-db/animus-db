//! Throughput-derived minimum tablet count (ADR 0067, W-08b) —
//! **`ProdEnv` end-to-end**: a table declaring `ProvisionedThroughput`
//! forks its widest led tablet, at most once per tick, until its `Active`
//! tablet count reaches the minimum `min_tablets_for` derives under
//! small, test-sized per-tablet capacity ceilings; raising the throughput
//! via `UpdateTable` raises the derived minimum and the loop mints more;
//! a table with no `ProvisionedThroughput` (`PAY_PER_REQUEST`, the
//! default) is never touched by this trigger regardless of how long the
//! cluster runs. Mirrors `auto_split_ops_rate.rs`'s own shape/fixtures.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animus_control::Metadata;
use animus_tablet::TabletId;
use animusd::{BackupStoreConfig, Node, SegmentStoreConfig, StorageBackend, StreamSealKnobs};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

mod support;

/// [`animusd::bind_cluster`] + [`animusd::start_cluster_with_growth_and_
/// quiesce_after`] with none of the three opt-in byte/change-rate/ops-rate
/// triggers configured, but small, test-sized per-tablet capacity
/// ceilings (ADR 0067) — the fourth trigger is on by default, so this is
/// the "everything else off" fixture for exercising it in isolation.
async fn start_cluster_with_tablet_ceilings(
    n: usize,
    dir: &Path,
    max_read_units: u64,
    max_write_units: u64,
) -> Vec<Node> {
    let bound = animusd::bind_cluster(n, "127.0.0.1".parse().unwrap(), dir)
        .await
        .unwrap();
    animusd::start_cluster_with_growth_and_quiesce_after(
        bound,
        StorageBackend::default(),
        None,
        Duration::from_secs(600),
        StreamSealKnobs::default(),
        SegmentStoreConfig::default(),
        animusd::DEFAULT_STREAM_RETENTION,
        None,
        None,
        Duration::from_secs(600), // quiescence: irrelevant here, effectively off
        None,
        BackupStoreConfig::default(),
        None,
        None,
        Some(max_read_units),
        Some(max_write_units),
    )
    .await
    .unwrap()
}

async fn await_bootstrap(nodes: &[Node]) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if nodes.iter().any(Node::is_control_leader)
                && nodes.iter().all(|n| !n.metadata().members.is_empty())
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cluster did not bootstrap within 20s");
}

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

fn tablets_for(meta: &Metadata, table: &str) -> Vec<TabletId> {
    meta.tablets_for_table(table).map(|(&t, _)| t).collect()
}

async fn await_true(secs: u64, msg: &str, mut check: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if check() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("{msg} (timed out after {secs}s)");
        }
        sleep(Duration::from_millis(200)).await;
    }
}

async fn create_provisioned_table(
    addr: SocketAddr,
    table: &str,
    read_units: u64,
    write_units: u64,
) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{table}",
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "BillingMode":"PROVISIONED",
                "ProvisionedThroughput":{{"ReadCapacityUnits":{read_units},"WriteCapacityUnits":{write_units}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable({table}) failed: {body}");
}

async fn create_plain_table(addr: SocketAddr, table: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{table}","AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable({table}) failed: {body}");
}

/// A provisioned table forks up to its throughput-derived minimum tablet
/// count with **no writes at all** (ADR 0067's own empty-tablet handling —
/// the synthetic token-midpoint split key), then forks further once
/// `UpdateTable` raises its throughput; a `PAY_PER_REQUEST` table created
/// alongside it is never touched by this trigger over the same window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_split_min_tablets_forks_a_provisioned_table_up_to_its_derived_minimum() {
    let dir = support::panic_safe_tempdir();
    // Small test-sized ceilings (ADR 0067's own suggested shape) so the
    // test needs no huge numbers: 200 RCU / 200 WCU under a 100/100
    // ceiling derives `ceil(200/100 + 200/100) = 4`.
    let nodes = start_cluster_with_tablet_ceilings(1, dir.path(), 100, 100).await;
    await_bootstrap(&nodes).await;
    let addr = nodes[0].dynamo_addr();

    create_provisioned_table(addr, "mt_provisioned", 200, 200).await;
    create_plain_table(addr, "mt_unprovisioned").await;

    await_true(
        60,
        "provisioned table never reached its derived minimum of 4 tablets",
        || tablets_for(&nodes[0].metadata(), "mt_provisioned").len() >= 4,
    )
    .await;

    // Raise throughput: 400/400 under the same 100/100 ceiling derives
    // `ceil(400/100 + 400/100) = 8`.
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.UpdateTable",
        r#"{"TableName":"mt_provisioned","BillingMode":"PROVISIONED",
            "ProvisionedThroughput":{"ReadCapacityUnits":400,"WriteCapacityUnits":400}}"#,
    )
    .await;
    assert_eq!(status, 200, "UpdateTable raising units failed: {body}");

    await_true(
        60,
        "provisioned table never reached its raised derived minimum of 8 tablets",
        || tablets_for(&nodes[0].metadata(), "mt_provisioned").len() >= 8,
    )
    .await;

    // The unprovisioned (`PAY_PER_REQUEST`) table must never have been
    // touched by this trigger, over the whole window above.
    assert_eq!(
        tablets_for(&nodes[0].metadata(), "mt_unprovisioned").len(),
        1,
        "a table with no ProvisionedThroughput must never be split by the \
         throughput-derived-minimum-tablet-count trigger"
    );

    for n in &nodes {
        n.shutdown_graceful().await;
    }
}
