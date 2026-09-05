//! `--auto-split-ops-rate` **`ProdEnv` end-to-end** (W-09, ADR 0034's
//! deferred bullet, closed): a **plain, unstreamed** table under a fast
//! burst of small `PutItem`s splits once its own led tablet's smoothed
//! write-request rate ([`RequestRateTracker`]) sustains above the
//! configured threshold, while a second plain table receiving only a slow
//! trickle of writes stays at one tablet — the general-table dual of
//! `streams_e2e.rs`'s `auto_split_change_rate_splits_a_high_churn_streamed_
//! table_never_a_plain_one` (that test's own "hot vs. cold" shape, but
//! proving the *ops-rate* trigger instead of the *change-rate* one, and
//! deliberately over **unstreamed** tables — `auto_split_change_rate` is
//! streamed-tables-only by design, `auto_split_ops_rate` is not).
//!
//! Small fixtures are duplicated from `streams_e2e.rs` rather than shared
//! (this crate's own stated convention — see that file's own header).

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use animus_control::Metadata;
use animus_tablet::TabletId;
use animusd::{Node, SegmentStoreConfig, StorageBackend, StreamSealKnobs, bind_cluster};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

mod support;

/// [`bind_cluster`] + [`animusd::start_cluster_with_growth`] with only the
/// opt-in **ops-rate** auto-split trigger enabled (`auto_split_bytes` and
/// `auto_split_change_rate` both `None`) — this test's own fixture,
/// mirroring `streams_e2e.rs::start_streamed_cluster_with_change_rate`'s
/// shape exactly, one knob substituted for the other.
async fn start_cluster_with_ops_rate(n: usize, dir: &Path, ops_rate_per_sec: u64) -> Vec<Node> {
    let bound = bind_cluster(n, "127.0.0.1".parse().unwrap(), dir)
        .await
        .unwrap();
    animusd::start_cluster_with_growth(
        bound,
        StorageBackend::default(),
        None,
        Duration::from_secs(600),
        StreamSealKnobs::default(),
        SegmentStoreConfig::default(),
        animusd::DEFAULT_STREAM_RETENTION,
        None,
        Some(ops_rate_per_sec),
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
        sleep(Duration::from_millis(50)).await;
    }
}

async fn create_plain_table(addr: SocketAddr, name: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.CreateTable",
        &format!(
            r#"{{"TableName":"{name}","AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "CreateTable({name}) failed: {body}");
}

async fn put_item(addr: SocketAddr, table: &str, id: &str, filler: &str) {
    let (status, body) = dynamo(
        addr,
        "DynamoDB_20120810.PutItem",
        &format!(
            r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"body":{{"S":"{filler}"}}}}}}"#
        ),
    )
    .await;
    assert_eq!(status, 200, "PutItem({table}, {id}) failed: {body}");
}

/// A plain, **unstreamed** table under a fast burst of small `PutItem`s
/// splits once its own tablet's smoothed write-rate sustains above the
/// configured `--auto-split-ops-rate` threshold; a second plain table
/// receiving only a slow trickle of writes over a comparable window never
/// splits. Neither table declares a stream, and no byte/change-rate
/// threshold is configured — the ops-rate trigger alone must account for
/// both outcomes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_split_ops_rate_splits_a_hot_plain_table_never_a_cold_one() {
    let dir = support::panic_safe_tempdir();
    // Deliberately aggressive (1 op/sec) — mirrors `streams_e2e.rs`'s own
    // "pick a threshold the burst clears by orders of magnitude, and the
    // trickle stays comfortably under" convention. The burst below issues
    // writes back-to-back with no pacing at all (routinely tens to
    // hundreds of ops/sec on a real host); the trickle below paces one
    // write every 5s (0.2 ops/sec) — both sides have a wide margin
    // regardless of this host's own per-write latency.
    let nodes = start_cluster_with_ops_rate(1, dir.path(), 1).await;
    await_bootstrap(&nodes).await;

    create_plain_table(nodes[0].dynamo_addr(), "hot_ops").await;
    create_plain_table(nodes[0].dynamo_addr(), "cold_ops").await;

    // The hot table: 80 items, back-to-back, no pacing — comfortably above
    // 1 op/sec on any real host, and enough distinct keys for a real
    // interior split point (`SplitTablet` requires `start < at < end`).
    let filler = "x".repeat(200);
    for i in 0..80u32 {
        put_item(
            nodes[0].dynamo_addr(),
            "hot_ops",
            &format!("h{i:04}"),
            &filler,
        )
        .await;
    }

    await_true(
        20,
        "hot_ops never auto-split on its own write-request rate",
        || tablets_for(&nodes[0].metadata(), "hot_ops").len() >= 2,
    )
    .await;

    // The cold table: a slow trickle (one write every 5s, 0.2 ops/sec — well
    // under the 1 op/sec threshold) for a window comparable to the hot
    // table's own convergence window above — it must never gain a second
    // tablet. A converged-or-timeout loop that fails the instant a split is
    // observed, never a fixed sleep followed by one assertion.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(16);
    let mut i = 0u32;
    loop {
        put_item(
            nodes[0].dynamo_addr(),
            "cold_ops",
            &format!("c{i:04}"),
            &filler,
        )
        .await;
        i += 1;
        let cold_tablets = tablets_for(&nodes[0].metadata(), "cold_ops").len();
        assert_eq!(
            cold_tablets, 1,
            "a table receiving only a slow trickle must never be split by \
             --auto-split-ops-rate"
        );
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        sleep(Duration::from_secs(5)).await;
    }
}
