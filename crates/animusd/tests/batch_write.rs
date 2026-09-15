//! End-to-end test of DynamoDB `BatchWriteItem` over the real JSON/HTTP wire,
//! backed by the CP batch-put primitive (`KvCommand::Batch` — one Raft entry per
//! tablet, ADR 0017 bulk-write batching).
//!
//! Covers: (1) a `BatchWriteItem` round-trip — put a batch of items in one request,
//! `GetItem` each back — that **survives a process restart** (the batch was
//! Raft-committed + WAL-fsynced before the ack, so the on-disk LSM recovers it);
//! and (2) the mechanism the batch primitive exists for — a batched write of N
//! items proposes far fewer Raft entries than N individual `PutItem`s (one
//! consensus round per `BATCH_WRITE_MAX_ITEMS`-sized chunk of the batch — AWS's
//! own 25-item-per-call cap — vs one round per key).
//!
//! Like the other `animusd` tests this uses real TCP/time and polls with generous
//! timeouts (the `ProdEnv` edge is non-deterministic by design).
//!
//! **(2) asserts a deterministic observable, never wall-clock (issue #601).**
//! It used to compare two `Instant::now()` spans (`batched < per_key`) — on a
//! loaded/shared CI runner a scheduling stall, a compaction, or the group's
//! first-write warm-up landing in either phase can invert or flatten that
//! ratio with no regression in the mechanism at all (observed both 0.9x and
//! 0.7x in CI while the same tree measured 2.3x-3.7x locally). The property
//! the test actually wants — one Raft entry per chunk instead of one per
//! item — is exactly what the `cp_proposals_accepted` counter
//! (`animus-env`'s metrics seam, ADR 0015; incremented once per accepted
//! `put`/`put_batch`/`put_kind_batch` propose, never per item inside one,
//! see `animus-cp-data::record_propose`) counts directly and exactly, so the
//! test scrapes it off the real `GET /metrics` endpoint before/after each
//! phase and asserts the *counts*, not the clock. Timing is kept as an
//! `eprintln!` diagnostic only.

use std::net::SocketAddr;
use std::time::Duration;

use animus_dynamo::wire::BATCH_WRITE_MAX_ITEMS;
use animusd::{Node, StorageBackend};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// `GET /metrics` over a fresh HTTP/1.1 connection to `addr` (the node's
/// dynamo-port text endpoint, ADR 0015) — same shape as
/// `metrics_endpoint.rs`'s identical helper. Returns the response body.
async fn metrics(addr: SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("connect to metrics");
    let request = "GET /metrics HTTP/1.1\r\n\
         Host: animus\r\n\
         Connection: close\r\n\
         \r\n";
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
    let (_, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    payload.to_string()
}

/// Parse a `name value` line export into a counter value (0 if absent — a
/// counter that has never incremented is not printed by some sinks).
fn metric_value(body: &str, name: &str) -> i64 {
    body.lines()
        .find_map(|line| {
            let (n, v) = line.split_once(' ')?;
            if n == name { v.trim().parse().ok() } else { None }
        })
        .unwrap_or(0)
}

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
    batch_put_body_range(table, 0, n)
}

/// A `BatchWriteItem` body writing items `[start, end)` to `table`, each with
/// pk `pk` = `b{i}` — the chunking primitive [`batched_write_beats_per_key`]
/// uses to stay under [`BATCH_WRITE_MAX_ITEMS`] per call.
fn batch_put_body_range(table: &str, start: usize, end: usize) -> String {
    let puts: Vec<String> = (start..end)
        .map(|i| {
            format!(r#"{{"PutRequest":{{"Item":{{"pk":{{"S":"b{i}"}},"v":{{"N":"{i}"}}}}}}}}"#)
        })
        .collect();
    format!(r#"{{"RequestItems":{{"{table}":[{}]}}}}"#, puts.join(","))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_write_round_trip_survives_restart() {
    let dir = support::panic_safe_tempdir();
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

/// A batched write of N items (one `BatchWriteItem` per [`BATCH_WRITE_MAX_ITEMS`]
/// chunk → one Raft entry each) proposes far fewer Raft entries than N
/// individual `PutItem`s (one consensus round each). This is the whole point
/// of the batch primitive, and it is asserted on the `cp_proposals_accepted`
/// counter delta scraped off `GET /metrics` around each phase — never on
/// wall-clock elapsed time (issue #601: a wall-clock ratio measures the CI
/// runner's own scheduling noise, not this mechanism; see this file's module
/// doc). `N` is chunked into `BATCH_WRITE_MAX_ITEMS`-sized `BatchWriteItem`
/// calls — real DynamoDB's own 25-item-per-call cap, which a real client SDK
/// chunks around the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batched_write_beats_per_key() {
    timeout(Duration::from_secs(60), async {
        let dir = support::panic_safe_tempdir();
        let node_dir = dir.path().join("node-0");
        let (node, config) = support::start_single_node(&node_dir, StorageBackend::default()).await;
        let dynamo_addr = config.nodes[0].dynamo;
        await_bootstrap(&node).await;

        const N: usize = 200;
        const PROPOSALS: &str = "cp_proposals_accepted";

        // Per-key: N individual PutItems, serially (each its own consensus round,
        // so each its own accepted propose).
        let before_per_key = metric_value(&metrics(dynamo_addr).await, PROPOSALS);
        let per_key_wall = std::time::Instant::now();
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
        let per_key_wall = per_key_wall.elapsed();
        let after_per_key = metric_value(&metrics(dynamo_addr).await, PROPOSALS);
        let per_key_proposals = after_per_key - before_per_key;

        // Batched: the same N items, chunked to BATCH_WRITE_MAX_ITEMS per
        // BatchWriteItem call — one accepted propose per chunk, not per item.
        let expected_chunks = N.div_ceil(BATCH_WRITE_MAX_ITEMS) as i64;
        let before_batched = metric_value(&metrics(dynamo_addr).await, PROPOSALS);
        let batched_wall = std::time::Instant::now();
        for chunk_start in (0..N).step_by(BATCH_WRITE_MAX_ITEMS) {
            let chunk_end = (chunk_start + BATCH_WRITE_MAX_ITEMS).min(N);
            let (s, b) = dynamo(
                dynamo_addr,
                "DynamoDB_20120810.BatchWriteItem",
                &batch_put_body_range("bk", chunk_start, chunk_end),
            )
            .await;
            assert_eq!(
                s, 200,
                "batched BatchWriteItem[{chunk_start}..{chunk_end}]: {b}"
            );
        }
        let batched_wall = batched_wall.elapsed();
        let after_batched = metric_value(&metrics(dynamo_addr).await, PROPOSALS);
        let batched_proposals = after_batched - before_batched;

        // Diagnostic only — real-time noise on a shared runner is expected and
        // is exactly why nothing below asserts on it (issue #601).
        eprintln!(
            "batched {N} items in {batched_wall:?} ({batched_proposals} proposals) vs \
             per-key {per_key_wall:?} ({per_key_proposals} proposals)"
        );

        // The mechanism: one propose per key for per-key writes, and at most
        // one propose per BATCH_WRITE_MAX_ITEMS-sized chunk for the batch —
        // deterministic and seed-independent, no wall clock involved.
        assert_eq!(
            per_key_proposals, N as i64,
            "N per-key PutItems should accept exactly N proposals (got {per_key_proposals})"
        );
        assert!(
            batched_proposals <= expected_chunks,
            "batched write of {N} items in chunks of {BATCH_WRITE_MAX_ITEMS} should accept \
             at most {expected_chunks} proposals (got {batched_proposals})"
        );
        assert!(
            batched_proposals < per_key_proposals,
            "batched write ({batched_proposals} proposals) should need fewer Raft proposals \
             than {N} per-key writes ({per_key_proposals} proposals)"
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
