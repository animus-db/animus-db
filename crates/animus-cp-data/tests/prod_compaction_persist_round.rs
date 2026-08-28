//! Real-thread `ProdEnv` liveness check for issue #279's decoupled WAL
//! persistence: writes must keep confirming while the **apply task's compaction
//! rewrite** competes with the consensus loop for the same WAL.
//!
//! **What changed and why this needs real threads.** Since #279 the loop no
//! longer blocks on its own `fsync`; it buffers the messages that make a
//! durability claim (vote grants, append accepts) against a persist round and
//! returns to `select`. That is only sound if the WAL's two drainers agree on
//! the round accounting — and the second drainer is the apply task, on another
//! OS thread. `SimEnv`'s single-threaded scheduler cannot interleave the two at
//! all, so this crate's `SimEnv` regression (`slow_disk_no_livelock.rs`) proves
//! the livelock fix and nothing about this. Both reverted fix attempts were
//! `SimEnv`-green and end-to-end red for exactly that reason.
//!
//! **What this test does prove:** a real 3-node group, driven with enough
//! writes to force many compactions (`COMPACT_THRESHOLD` is 64 applies), keeps
//! confirming every write inside a bounded budget — confirmed by reading the
//! value back, never by `Accepted { index }`, which only ever means "appended
//! locally". A gross regression in the buffering/release path (acks released
//! late, or a round-completion wake lost) shows up here as writes that stop
//! confirming while the group is otherwise healthy. It also asserts, via the
//! metric rather than by assumption, that compaction really did fire.
//!
//! **What it does NOT prove, deliberately stated.** The specific defect that
//! sank attempt #2 — compaction draining `core.pending` in the microsecond
//! window between a step releasing the core lock and the loop next looking at
//! it, leaving a buffered ack waiting on a round with no drainer — is *not*
//! reachable by wall-clock load: with that bug deliberately reintroduced, this
//! test (and a two-node variant where the single follower's ack is required for
//! quorum) stayed green run after run. That class is closed structurally
//! instead — `persist_round::drain_for_round` is the only sanctioned drain, so
//! numbering cannot be skipped, and `PersistProgress::fully_durable` releases
//! the buffer whenever nothing is pending and no round is in flight regardless
//! of round numbers. See that module's "Two layers" section. This test is the
//! real-thread liveness coverage for the new concurrency, not a fault injection
//! for that window.

// ADR 0003 / ADR 0061 Decision 4 (rung B5): a real-thread ProdEnv liveness
// test (see the module doc above) — the race under test is a microsecond
// scheduling window SimEnv's cooperative single-thread scheduler cannot
// produce, so real time/threads are the point here, not a determinism hole.
#![allow(
    clippy::disallowed_methods,
    reason = "real-thread ProdEnv liveness test (a scheduling race SimEnv cannot produce, see module doc); ADR 0061 Decision 4"
)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{Env, Metric, MetricsHandle, NodeId, ProdEnv, nid};
use animus_storage::MemoryEngine;
use tokio::time::{Instant, sleep};

type KvNode = RaftKvNode<ProdEnv, MemoryEngine>;

/// Comfortably more than `COMPACT_THRESHOLD` (64), so every replica rewrites
/// its WAL several times *while* the loop keeps taking writes.
const WRITES: usize = 400;
/// Per-write converged-or-timeout budget. Healthy writes confirm in
/// milliseconds; the stranding this guards against ran to whole seconds, so
/// this is deliberately far above healthy and far below the failure.
const WRITE_BUDGET: Duration = Duration::from_secs(5);
/// A latency ceiling on any single write, well above healthy (milliseconds) and
/// well below the whole-run budget: catches a release path that degrades
/// gradually rather than failing outright.
const WORST_CONFIRM: Duration = Duration::from_millis(1500);
/// Whole-run ceiling, so a group that degrades gradually fails the test rather
/// than running until the harness kills it.
const RUN_BUDGET: Duration = Duration::from_secs(120);
const POLL: Duration = Duration::from_millis(5);

fn unique_tmp_dir() -> std::path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "animus-cp-persist-round-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// Start a real 3-node group over `ProdEnv` with recording metric handles, and
/// return it once a leader is elected.
async fn start_group() -> (Vec<KvNode>, Vec<MetricsHandle>) {
    let group: Vec<NodeId> = vec![nid(0), nid(1), nid(2)];
    let loop0 = || "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let mut envs = Vec::new();
    for i in 0..3 {
        let dir = unique_tmp_dir();
        let (env, _addr) = ProdEnv::bind(nid(i as u64), loop0(), &dir)
            .await
            .expect("bind");
        envs.push(env);
    }
    let book: BTreeMap<NodeId, String> = envs
        .iter()
        .map(|e| (e.node_id(), e.local_addr().to_string()))
        .collect();
    for e in &envs {
        e.set_peers(book.clone());
    }

    let handles: Vec<MetricsHandle> = (0..3).map(|_| MetricsHandle::recording()).collect();
    let nodes: Vec<KvNode> = envs
        .into_iter()
        .zip(handles.iter())
        .map(|(env, m)| {
            RaftKvNode::start_with_metrics(env, group.clone(), MemoryEngine::new(), m.clone())
        })
        .collect();

    for _ in 0..200 {
        if nodes.iter().any(RaftKvNode::is_leader) {
            return (nodes, handles);
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader elected within 10s");
}

/// Re-resolved on every attempt: an election under load can depose any
/// previously-resolved leader (the harness lesson `prod_concurrent_ts_
/// monotonic.rs`'s module doc records).
async fn current_leader(nodes: &[KvNode], deadline: Instant) -> Option<KvNode> {
    loop {
        if let Some(n) = nodes.iter().find(|n| n.is_leader()) {
            return Some(n.clone());
        }
        if Instant::now() >= deadline {
            return None;
        }
        sleep(POLL).await;
    }
}

/// Put and confirm by reading the value back. Returns how long it took, or
/// `None` if it never confirmed inside `WRITE_BUDGET`.
async fn put_then_confirm(nodes: &[KvNode], key: &[u8], value: &[u8]) -> Option<Duration> {
    let start = Instant::now();
    let deadline = start + WRITE_BUDGET;
    loop {
        let leader = current_leader(nodes, deadline).await?;
        if matches!(
            leader.put(key.to_vec(), value.to_vec()),
            ProposeResult::Accepted { .. }
        ) {
            // Confirm the write actually committed and applied. Re-putting the
            // same key/value is idempotent, so a stale read just retries.
            loop {
                if let Some(l) = current_leader(nodes, deadline).await
                    && l.linearizable_get(key).await.as_deref() == Some(value)
                {
                    return Some(start.elapsed());
                }
                if Instant::now() >= deadline {
                    return None;
                }
                sleep(POLL).await;
            }
        }
        if Instant::now() >= deadline {
            return None;
        }
        sleep(POLL).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_keep_confirming_while_compaction_drains_the_wal() {
    let run_start = Instant::now();
    let (nodes, handles) = start_group().await;

    let mut worst = Duration::ZERO;
    let mut worst_at = 0usize;
    for i in 0..WRITES {
        let key = format!("k{i:04}").into_bytes();
        let value = vec![b'v'; 256];
        let took = put_then_confirm(&nodes, &key, &value)
            .await
            .unwrap_or_else(|| {
                panic!(
                    "write {i} never confirmed within {WRITE_BUDGET:?} — a gated ack \
                 stranded behind a compaction-drained persist round would look \
                 exactly like this (worst confirm so far: {worst:?} at write {worst_at})"
                )
            });
        if took > worst {
            worst = took;
            worst_at = i;
        }
        assert!(
            run_start.elapsed() < RUN_BUDGET,
            "the run exceeded {RUN_BUDGET:?} at write {i} (worst single confirm {worst:?})"
        );
    }

    // The premise of the test, asserted rather than assumed: compaction really
    // did drain the WAL out from under the consensus loop, repeatedly. Without
    // this a future change to `COMPACT_THRESHOLD` (or to the compaction
    // trigger) could silently turn this into a plain write-throughput test.
    let compactions: u64 = handles
        .iter()
        .map(|h| h.get(Metric::CpSnapshotTriggers))
        .sum();
    assert!(
        compactions >= 3,
        "expected the apply task's compaction rewrite to fire repeatedly during \
         the write stream, saw {compactions} across the group — this test's \
         premise (compaction competing with the consensus loop for the WAL) no \
         longer holds"
    );

    assert!(
        worst < WORST_CONFIRM,
        "worst confirm was {worst:?} at write {worst_at} (limit {WORST_CONFIRM:?})"
    );
    for node in &nodes {
        node.shutdown();
    }
}
