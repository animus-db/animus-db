//! End-to-end test of DynamoDB `BatchWriteItem` over the real JSON/HTTP wire,
//! backed by the CP batch-put primitive (`KvCommand::Batch` — one Raft entry per
//! tablet, ADR 0017 bulk-write batching).
//!
//! Covers: (1) a `BatchWriteItem` round-trip — put a batch of items in one request,
//! `GetItem` each back — that **survives a process restart** (the batch was
//! Raft-committed + WAL-fsynced before the ack, so the on-disk LSM recovers it);
//! and (2) a throughput contrast showing a batched write of N items beats N
//! individual `PutItem`s (one consensus round for the whole batch vs one per key).
//!
//! Like the other `animusd` tests this uses real TCP/time and polls with generous
//! timeouts (the `ProdEnv` edge is non-deterministic by design).

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use animusd::{Node, StorageBackend};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// One DynamoDB request over a fresh HTTP/1.1 connection → `(status, body)`.
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
    let ready = async {
        loop {
            if node.is_control_leader() && !node.metadata().members.is_empty() {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(20), ready)
        .await
        .expect("node did not bootstrap in 20s");
}

async fn stop(node: Node) {
    node.shutdown_graceful().await;
    drop(node);
    sleep(Duration::from_millis(200)).await;
}

/// A `BatchWriteItem` body writing `n` items to `table` with pk `pk` = `bN`.
fn batch_put_body(table: &str, n: usize) -> String {
    let puts: Vec<String> = (0..n)
        .map(|i| {
            format!(r#"{{"PutRequest":{{"Item":{{"pk":{{"S":"b{i}"}},"v":{{"N":"{i}"}}}}}}}}"#)
        })
        .collect();
    format!(r#"{{"RequestItems":{{"{table}":[{}]}}}}"#, puts.join(","))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_write_round_trip_survives_restart() {
    let dir = TempDir::new().unwrap();
    let node_dir = dir.path().join("node-0");

    let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
    let dynamo_addr = config.nodes[0].dynamo;
    await_bootstrap(&node).await;

    const N: usize = 20;
    // One BatchWriteItem request commits all N items as a single Raft entry.
    let (status, body) = dynamo(
        dynamo_addr,
        "DynamoDB_20120810.BatchWriteItem",
        &batch_put_body("bt", N),
    )
    .await;
    assert_eq!(status, 200, "BatchWriteItem failed: {body}");
    assert_eq!(body, r#"{"UnprocessedItems":{}}"#, "got: {body}");

    // Every item reads back (the durable-before-ack batch is committed + applied).
    for i in 0..N {
        let (s, b) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.GetItem",
            &format!(r#"{{"TableName":"bt","Key":{{"pk":{{"S":"b{i}"}}}}}}"#),
        )
        .await;
        assert_eq!(s, 200, "GetItem b{i} failed: {b}");
        assert!(b.contains(&format!(r#""v":{{"N":"{i}"}}"#)), "b{i}: {b}");
    }

    // Restart on the same dir + addresses: the on-disk LSM + Raft WAL recover the
    // whole batch (it was fsynced before the ack).
    stop(node).await;
    let node = support::restart_same_addrs(&config, 0, &node_dir, StorageBackend::default()).await;
    await_bootstrap(&node).await;

    for i in 0..N {
        // Poll: after restart the CP group must re-elect + recover before serving.
        let mut found = None;
        for _ in 0..100 {
            let (s, b) = dynamo(
                dynamo_addr,
                "DynamoDB_20120810.GetItem",
                &format!(r#"{{"TableName":"bt","Key":{{"pk":{{"S":"b{i}"}}}}}}"#),
            )
            .await;
            if s == 200 && b.contains(&format!(r#""v":{{"N":"{i}"}}"#)) {
                found = Some(b);
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
        assert!(found.is_some(), "b{i} did not survive the restart");
    }

    stop(node).await;
}

/// A batched write of N items (one `BatchWriteItem` → one Raft entry) is faster
/// than N individual `PutItem`s (one consensus round each). This is the whole
/// point of the batch primitive; the margin is large (round count differs by N),
/// so the assertion is robust despite real-time noise.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batched_write_beats_per_key() {
    timeout(Duration::from_secs(60), async {
        let dir = TempDir::new().unwrap();
        let node_dir = dir.path().join("node-0");
        let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
        let dynamo_addr = config.nodes[0].dynamo;
        await_bootstrap(&node).await;

        const N: usize = 200;

        // Per-key: N individual PutItems, serially (each its own consensus round).
        let per_key_start = Instant::now();
        for i in 0..N {
            let (s, b) = dynamo(
                dynamo_addr,
                "DynamoDB_20120810.PutItem",
                &format!(
                    r#"{{"TableName":"pk","Item":{{"pk":{{"S":"k{i}"}},"v":{{"N":"{i}"}}}}}}"#
                ),
            )
            .await;
            assert_eq!(s, 200, "per-key PutItem k{i}: {b}");
        }
        let per_key = per_key_start.elapsed();

        // Batched: the same N items in one BatchWriteItem (one Raft entry).
        let batched_start = Instant::now();
        let (s, b) = dynamo(
            dynamo_addr,
            "DynamoDB_20120810.BatchWriteItem",
            &batch_put_body("bk", N),
        )
        .await;
        assert_eq!(s, 200, "batched BatchWriteItem: {b}");
        let batched = batched_start.elapsed();

        eprintln!(
            "batched {N} items in {batched:?} vs per-key {per_key:?} \
             (speedup {:.1}x)",
            per_key.as_secs_f64() / batched.as_secs_f64().max(1e-9)
        );
        assert!(
            batched < per_key,
            "batched write ({batched:?}) should beat {N} per-key writes ({per_key:?})"
        );

        // Sanity: all batched items are present.
        for i in 0..N {
            let (s, gb) = dynamo(
                dynamo_addr,
                "DynamoDB_20120810.GetItem",
                &format!(r#"{{"TableName":"bk","Key":{{"pk":{{"S":"b{i}"}}}}}}"#),
            )
            .await;
            assert_eq!(s, 200, "batched GetItem b{i}: {gb}");
            assert!(gb.contains(&format!(r#""v":{{"N":"{i}"}}"#)), "b{i}: {gb}");
        }

        stop(node).await;
    })
    .await
    .expect("throughput test timed out");
}
