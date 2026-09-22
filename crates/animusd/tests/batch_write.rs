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
//!
//! **`cp_proposals_accepted` is not exclusively a client-write counter —
//! issue #974, closing the investigation issue #911/#967/#971 left open.**
//! It counts every accepted propose on the group's Raft log, whoever issued
//! it. On a fully quiescent single-write cluster the only other proposer is
//! `animusd::index_drain::trim_janitor` (ADR 0049 §1): its hot-trim arm runs
//! unconditionally on every led tablet each `INDEX_DRAIN_INTERVAL` tick,
//! even for a plain (no GSI/stream/PITR) table, since a marker record is
//! never itself consumer-visible and so is always immediately safe to
//! delete — a fault-free, single-node run of this very test reproduced 9
//! accepted proposals for 8 `BatchWriteItem` chunks in the FIRST unloaded
//! run tried, and `eprintln!`-level tracing of every accepted propose (kind,
//! tablet, index, term, call site) showed the 9th to be exactly one such
//! trim: a `KindBatch` tombstone-delete of the per-key phase's own 200
//! now-consumed change-log markers, landing — by ordinary scheduling luck,
//! not a bug in the write path — inside the batched phase's own before/
//! after `/metrics` window rather than the per-key phase's. **No confirm
//! loop re-proposed anything**: the trace carries zero `"; retry"` results
//! and zero superseded/no-op confirms anywhere in the run. So the #911
//! hazard closed by #971 really is closed, and the margin this file used to
//! carry for it was never covering a duplicate client propose — it was
//! covering an entirely different, legitimate proposer sharing the same
//! counter. Fixed at the root: `trim_janitor` now also increments
//! `Metric::CpHousekeepingProposalsAccepted` (see that metric's own doc) at
//! its one call site, so this test (and anyone else who needs "proposals a
//! client write actually caused") can subtract that delta from
//! `cp_proposals_accepted`'s own and assert **exact** equality with no
//! margin, regardless of which side of the phase boundary a trim tick lands
//! on.
//!
//! **Issue #1037: #974's fix was correct about WHICH propose but wrong
//! about WHEN it gets marked, and that gap reopened the exact-equality
//! assertion under load.** The failure signature moved from "9 raw / 1
//! housekeeping" (the mechanism #974 pinned) to **9 raw / 0 housekeeping**
//! — the trim janitor's own propose again went unattributed, but this time
//! `cp_housekeeping_proposals_accepted` never incremented for it at all
//! within the test's own measurement window, rather than incrementing on
//! the wrong side of a phase boundary. Reproduced first as a rare CPU-
//! contention flake (one local failure, one on a loaded GitHub Actions
//! runner, both on branches whose only diff to this file was unrelated),
//! then — once a third gate run (PR #1038 shard 2/4 and PR #1040, same
//! day) turned up the identical 9/0 signature on diffs that don't touch the
//! write path — recognized as **near-deterministic on a loaded CI runner**,
//! not a rare race at all: the trim janitor's own `INDEX_DRAIN_INTERVAL`
//! (200ms) tick reliably lands inside one of this test's two ~0.3-1.5s
//! phases on a real multi-second run, so the only question was ever
//! whether the counter that attributes it lags behind the one that counts
//! it.
//!
//! It did. #974's fix incremented `CpHousekeepingProposalsAccepted` in
//! `trim_janitor` itself (`animusd::index_drain`), **after** its own
//! `cp_kind_write_raw` call returned — i.e. after that write's full
//! propose-commit-apply-confirm cycle. But `cp_proposals_accepted` is
//! incremented much earlier, synchronously, the instant the entry is
//! accepted onto the leader's own local Raft log (`animus-cp-data::
//! record_propose`, inside `put_kind_batch`) — before any of the real
//! (`ProdEnv`) async time the confirm loop then spends waiting for the
//! entry to actually commit+apply. Between those two moments sat a real,
//! unbounded window — wider under CPU contention, since a loaded scheduler
//! makes the confirm loop's own awaits take longer in wall-clock terms —
//! during which the raw counter had already counted the trim's propose but
//! the housekeeping counter had not yet. A `GET /metrics` scrape landing in
//! that window (exactly what this test's own before/after calls do) read
//! the trim back as an unattributed client write: 9 raw, 0 housekeeping,
//! `batched_proposals` computed as 9 instead of 8.
//!
//! Fixed at the root, one layer down from where #974 put it:
//! `RaftKvNode::metrics_handle` exposes the group's own `MetricsHandle`
//! (the same sink `record_propose` writes into), `CpGroup::metrics`
//! mirrors it, and `ClientCtx::cp_kind_raw_local` takes a `housekeeping`
//! argument that — when set — marks `CpHousekeepingProposalsAccepted` in
//! the **same synchronous step** as the propose's own acceptance, before
//! the confirm loop below it ever awaits anything. `trim_janitor` reaches
//! this through a new `ClientCtx::cp_kind_write_raw_housekeeping` (in place
//! of the plain `cp_kind_write_raw` it used to call, with its own two
//! now-redundant post-confirm `.incr(Metric::
//! CpHousekeepingProposalsAccepted)` calls removed). The forwarded
//! `ClientRequest::KindWrite` RPC carries the same flag (`#[serde(default)]
//! housekeeping: bool`) so the one theoretical cross-node case — leadership
//! moving between `trim_janitor`'s own `is_leader()` gate and its propose —
//! stays correctly attributed too, though `trim_janitor` only ever calls
//! this on a tablet it already leads locally, so that hop is untested here
//! and covered instead by `write_path::
//! cp_kind_raw_local_housekeeping_attribution_tests` (a seeded `SimEnv`
//! regression that catches the write's own confirm loop at its first park
//! — accepted but not yet committed — and asserts both counters already
//! agree at that exact instant, which fails against the pre-fix,
//! post-confirm-only increment).
//!
//! **PR #1036 (issue #1035, relayed-throttle error-code decoding, merged
//! the same day) was investigated and ruled out**: its diff is entirely
//! `decode_relayed_error`/`wire_error_from_batch_rejected`'s shared
//! allowlist table, on the `KindWriteItem`/`KindWriteBatch` forwarded-hop
//! *error* path, with no propose or retry logic touched — and this test's
//! single-node cluster never forwards a write at all, so that hop is not
//! reachable from here regardless.

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
            if n == name {
                v.trim().parse().ok()
            } else {
                None
            }
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
    support::await_bootstrap(std::slice::from_ref(&node)).await;

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
    support::await_bootstrap(std::slice::from_ref(&node)).await;

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
        support::await_bootstrap(std::slice::from_ref(&node)).await;

        const N: usize = 200;
        const PROPOSALS: &str = "cp_proposals_accepted";
        // Issue #974: the trim janitor (`index_drain::trim_janitor`) shares
        // `cp_proposals_accepted` with every client write on the same
        // group — see this file's module doc. Subtracting this counter's
        // own delta recovers "proposals a client write actually caused"
        // over any window, regardless of which side of a phase boundary a
        // trim tick's own propose happens to land on.
        const HOUSEKEEPING: &str = "cp_housekeeping_proposals_accepted";

        // Per-key: N individual PutItems, serially (each its own consensus round,
        // so each its own accepted propose).
        let before = metrics(dynamo_addr).await;
        let (before_per_key, before_per_key_housekeeping) = (
            metric_value(&before, PROPOSALS),
            metric_value(&before, HOUSEKEEPING),
        );
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
        let after = metrics(dynamo_addr).await;
        let per_key_proposals = metric_value(&after, PROPOSALS) - before_per_key;
        let per_key_housekeeping = metric_value(&after, HOUSEKEEPING) - before_per_key_housekeeping;

        // Batched: the same N items, chunked to BATCH_WRITE_MAX_ITEMS per
        // BatchWriteItem call — one accepted propose per chunk, not per item.
        let expected_chunks = N.div_ceil(BATCH_WRITE_MAX_ITEMS) as i64;
        let before = metrics(dynamo_addr).await;
        let (before_batched, before_batched_housekeeping) = (
            metric_value(&before, PROPOSALS),
            metric_value(&before, HOUSEKEEPING),
        );
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
        let after = metrics(dynamo_addr).await;
        let batched_proposals_raw = metric_value(&after, PROPOSALS) - before_batched;
        let batched_housekeeping = metric_value(&after, HOUSEKEEPING) - before_batched_housekeeping;
        let batched_proposals = batched_proposals_raw - batched_housekeeping;

        // Diagnostic only — real-time noise on a shared runner is expected and
        // is exactly why nothing below asserts on it (issue #601).
        eprintln!(
            "batched {N} items in {batched_wall:?} ({batched_proposals_raw} raw / \
             {batched_housekeeping} housekeeping / {batched_proposals} client proposals) vs \
             per-key {per_key_wall:?} ({per_key_proposals} proposals, \
             {per_key_housekeeping} housekeeping)"
        );

        // The mechanism: one propose per key for per-key writes, and exactly
        // one propose per BATCH_WRITE_MAX_ITEMS-sized chunk for the batch,
        // once the trim janitor's own housekeeping proposals (issue #974) are
        // subtracted out — deterministic and seed-independent, no wall clock,
        // and no margin: every accepted propose is now attributed to either a
        // client write or the janitor, never left unexplained.
        assert_eq!(
            per_key_proposals - per_key_housekeeping,
            N as i64,
            "N per-key PutItems should accept exactly N client proposals (got \
             {per_key_proposals} raw, {per_key_housekeeping} housekeeping)"
        );
        assert_eq!(
            batched_proposals, expected_chunks,
            "batched write of {N} items in chunks of {BATCH_WRITE_MAX_ITEMS} should accept \
             exactly {expected_chunks} client proposals (got {batched_proposals_raw} raw, \
             {batched_housekeeping} housekeeping)"
        );
        assert!(
            batched_proposals * 4 < per_key_proposals,
            "batched write ({batched_proposals} proposals) should need far fewer Raft proposals \
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
