//! Real-socket `ProdEnv` tests for the per-peer outbox (ADR 0080 section 3,
//! Tier 3 (iii)): a stalled peer cannot grow memory or tasks without bound, it
//! does not delay another peer's frames, and a `Bulk` producer waits for credit
//! instead of losing data. Its own `tests/` target (real sockets and threads,
//! never merged into a SimEnv binary — see CLAUDE.md). Every wait is a
//! converged-or-timeout poll.
#![cfg(feature = "prod")]
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a real-socket ProdEnv liveness test: real wall-clock timeouts guard it"
)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use animus_env::{Env, Metric, MsgClass, Network, NodeId, ProdEnv, nid};

fn unique_tmp_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("animus-outbox-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

async fn bind(id: u64) -> (ProdEnv, SocketAddr) {
    let loop0: SocketAddr = "127.0.0.1:0".parse().unwrap();
    ProdEnv::bind(nid(id), loop0, unique_tmp_dir())
        .await
        .expect("bind")
}

fn peers(of: &[(NodeId, String)]) -> std::collections::BTreeMap<NodeId, String> {
    of.iter().cloned().collect()
}

const T: Duration = Duration::from_secs(30);
const STREAM: u64 = 7;

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + T;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A peer that never answers: its listener is bound (so the kernel completes
/// the TCP handshake) but nothing ever accepts, so the preamble exchange hangs.
fn stalled_peer() -> (std::net::TcpListener, String) {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind stalled listener");
    let addr = l.local_addr().expect("addr").to_string();
    (l, addr)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_peer_keeps_the_outbox_and_the_task_count_bounded_and_does_not_slow_another_peer()
{
    let (a, _) = bind(0).await;
    let (b, b_addr) = bind(1).await;
    let (_stalled, stalled_addr) = stalled_peer();
    a.set_peers(peers(&[
        (nid(1), b_addr.to_string()),
        (nid(2), stalled_addr),
    ]));

    let got = std::sync::Arc::new(AtomicU64::new(0));
    let counter = std::sync::Arc::clone(&got);
    let rx = b.clone();
    tokio::spawn(async move {
        loop {
            let _ = rx.recv_stream(STREAM).await;
            counter.fetch_add(1, Ordering::Relaxed);
        }
    });

    // Flood the stalled peer far past every bound (40 MiB offered) while a
    // steady heartbeat stream goes to the healthy one.
    let payload = vec![0u8; 8 * 1024];
    let mut sent_to_b = 0u64;
    let started = Instant::now();
    let mut max_bytes = 0usize;
    let mut max_tasks = 0u64;
    for round in 0..5_000u32 {
        a.send_stream_classed(nid(2), STREAM, MsgClass::Normal, None, payload.clone())
            .await;
        if round % 50 == 0 {
            a.send_stream_classed(nid(1), STREAM, MsgClass::Heartbeat, Some(1), vec![1])
                .await;
            sent_to_b += 1;
            let (_, _, bytes) = a.outbox_stats();
            max_bytes = max_bytes.max(bytes);
            max_tasks = max_tasks.max(a.metrics().get(Metric::SpawnedTaskHandlesTracked));
        }
        tokio::task::yield_now().await;
    }
    assert!(
        started.elapsed() < T,
        "a stalled peer must never block the producer"
    );

    // Memory: the stalled peer's queue is capped at the replicate bound (8 MiB)
    // plus the bulk bound (4 MiB), whatever was offered.
    assert!(
        max_bytes <= 13 << 20,
        "outbox bytes reached {max_bytes}, the bound is replicate 8 MiB + bulk 4 MiB"
    );
    // Tasks: two writers per peer, not one per frame.
    assert!(
        max_tasks <= 40,
        "tracked task handles reached {max_tasks}: the stalled peer must not spawn per frame"
    );
    let (peers_n, _, _) = a.outbox_stats();
    assert_eq!(peers_n, 2);
    // The drop-oldest policy fired and was counted, never silent.
    let dropped = a.metrics().get(Metric::TransportOutboxDroppedReplicate);
    assert!(dropped > 0, "the flood must have evicted frames");

    // The healthy peer got its heartbeats (supersede may have coalesced some).
    eventually("the healthy peer to receive heartbeats", || {
        got.load(Ordering::Relaxed) >= 1
    })
    .await;
    assert!(got.load(Ordering::Relaxed) <= sent_to_b);
    a.shutdown();
    b.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bulk_producer_waits_for_credit_and_loses_nothing() {
    let (a, _) = bind(0).await;
    let (b, b_addr) = bind(1).await;
    a.set_peers(peers(&[(nid(1), b_addr.to_string())]));
    // 24 MiB of bulk against a 4 MiB credit bound.
    let chunk = vec![9u8; 256 * 1024];
    let total = 96usize;
    let recv = tokio::spawn({
        let b = b.clone();
        async move {
            let mut n = 0usize;
            while n < total {
                let env = tokio::time::timeout(T, b.recv_stream(STREAM))
                    .await
                    .expect("a bulk frame");
                assert_eq!(env.payload.len(), 256 * 1024);
                n += 1;
            }
            n
        }
    });
    let mut max_bytes = 0usize;
    for _ in 0..total {
        a.send_stream_classed(nid(1), STREAM, MsgClass::Bulk, None, chunk.clone())
            .await;
        let (_, _, bytes) = a.outbox_stats();
        max_bytes = max_bytes.max(bytes);
    }
    assert!(
        max_bytes <= 4 * 1024 * 1024 + 256 * 1024,
        "queued bulk bytes reached {max_bytes}, the credit bound is 4 MiB"
    );
    assert_eq!(recv.await.expect("receiver"), total, "nothing was lost");
    a.shutdown();
    b.shutdown();
}
