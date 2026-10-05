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
use std::io::Write as _;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
/// A probe `write + fsync` slower than this marks the disk as stalled for the
/// interval it took (a healthy `fsync` here is well under a millisecond to a
/// few milliseconds, even under moderate contention).
const DISK_STALL: Duration = Duration::from_millis(100);
const PROBE_EVERY: Duration = Duration::from_millis(10);

type Window = (std::time::Instant, std::time::Instant);

/// Independent disk-health probe (issue #1222). Every write this test times
/// commits through quorum `fsync`s on a *shared* filesystem, so a confirm
/// latency is `release-path cost + however long the disk stalled`. The bound
/// exists to catch the former (an ack released late, a lost wake) and cannot
/// be met by any implementation when the latter is whole seconds -- CI's
/// `prod-liveness-scattered` job runs straight after a heavy `rm -rf` on the
/// same runner disk. This thread does nothing but `write + fsync` one byte in
/// the same temp directory tree every 10ms on its own OS thread (independent of
/// the tokio runtime and of the group under test) and records every interval in
/// which one such `fsync` took >= [`DISK_STALL`]; a timed write whose window
/// overlaps one is attributed to the disk, not to the release path.
struct DiskProbe {
    stalls: Arc<Mutex<Vec<Window>>>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl DiskProbe {
    fn start() -> Self {
        let dir = unique_tmp_dir();
        let stalls = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (st, sp) = (stalls.clone(), stop.clone());
        let thread = std::thread::spawn(move || {
            let mut f = std::fs::File::create(dir.join("probe")).expect("probe file");
            while !sp.load(Ordering::Relaxed) {
                let t0 = std::time::Instant::now();
                let _ = f.write_all(b"x");
                let _ = f.sync_all();
                let t1 = std::time::Instant::now();
                if t1 - t0 >= DISK_STALL {
                    st.lock().expect("probe lock").push((t0, t1));
                }
                std::thread::sleep(PROBE_EVERY);
            }
        });
        Self {
            stalls,
            stop,
            thread: Some(thread),
        }
    }

    /// Stop the probe and return every recorded stall interval.
    fn finish(mut self) -> Vec<Window> {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
        self.stalls.lock().expect("probe lock").clone()
    }
}

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

/// Per-write phase timings, so a tripped latency bound is attributable to an
/// election, a stalled commit (fsync / quorum) or a read barrier instead of
/// just "the loop was slow" (issue #1222). Cheap: a few `Instant` reads.
#[derive(Clone, Debug, Default)]
struct Phases {
    /// Start until the first leader was resolved.
    resolve: Duration,
    /// Time spent in `put` proposals (including retries after a rejection).
    put_attempts: u32,
    /// First `put` accepted (appended locally on the leader), from start.
    accepted: Duration,
    /// `linearizable_get` calls until the value read back, and the time the
    /// calls spent inside the read barrier in total.
    reads: u32,
    read_barrier: Duration,
    /// Accepted until the read-back returned the value (commit + apply +
    /// barrier), i.e. the cost that a slow quorum fsync shows up in.
    commit_to_readback: Duration,
    /// Leader node index / term when the put was accepted and when the value
    /// was read back; a differing pair means an election ran inside the write.
    leader_at_put: (usize, u64),
    leader_at_read: (usize, u64),
    total: Duration,
}

fn leader_ix(nodes: &[KvNode]) -> (usize, u64) {
    nodes
        .iter()
        .position(KvNode::is_leader)
        .map_or((usize::MAX, 0), |i| (i, nodes[i].term()))
}

/// Put and confirm by reading the value back. Returns the phase timings, or
/// `None` if it never confirmed inside `WRITE_BUDGET`.
async fn put_then_confirm(nodes: &[KvNode], key: &[u8], value: &[u8]) -> Option<Phases> {
    let start = Instant::now();
    let deadline = start + WRITE_BUDGET;
    let mut ph = Phases::default();
    loop {
        let leader = current_leader(nodes, deadline).await?;
        if ph.put_attempts == 0 {
            ph.resolve = start.elapsed();
        }
        ph.put_attempts += 1;
        if matches!(
            leader.put(key.to_vec(), value.to_vec()),
            ProposeResult::Accepted { .. }
        ) {
            let accepted_at = Instant::now();
            ph.accepted = start.elapsed();
            ph.leader_at_put = leader_ix(nodes);
            // Confirm the write actually committed and applied. Re-putting the
            // same key/value is idempotent, so a stale read just retries.
            loop {
                if let Some(l) = current_leader(nodes, deadline).await {
                    let t = Instant::now();
                    let got = l.linearizable_get(key).await;
                    ph.reads += 1;
                    ph.read_barrier += t.elapsed();
                    if got.as_deref() == Some(value) {
                        ph.commit_to_readback = accepted_at.elapsed();
                        ph.leader_at_read = leader_ix(nodes);
                        ph.total = start.elapsed();
                        return Some(ph);
                    }
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
    let probe = DiskProbe::start();
    let (nodes, handles) = start_group().await;

    // Every write's (index, window, phases), judged after the run against the
    // probe's recorded disk stalls.
    let mut samples: Vec<(usize, Window, Phases)> = Vec::new();
    let mut worst = Duration::ZERO;
    let mut worst_at = 0usize;
    let mut worst_ph = Phases::default();
    let mut term_changes = 0u32;
    let mut last_term = leader_ix(&nodes).1;
    for i in 0..WRITES {
        let key = format!("k{i:04}").into_bytes();
        let value = vec![b'v'; 256];
        let w0 = std::time::Instant::now();
        let ph = put_then_confirm(&nodes, &key, &value)
            .await
            .unwrap_or_else(|| {
                panic!(
                    "write {i} never confirmed within {WRITE_BUDGET:?} — a gated ack \
                 stranded behind a compaction-drained persist round would look \
                 exactly like this (worst confirm so far: {worst:?} at write {worst_at})"
                )
            });
        let took = ph.total;
        samples.push((i, (w0, std::time::Instant::now()), ph.clone()));
        if ph.leader_at_read.1 != last_term {
            term_changes += 1;
            eprintln!("write {i}: term {last_term} -> {:?}", ph.leader_at_read);
            last_term = ph.leader_at_read.1;
        }
        if took > Duration::from_millis(250) {
            eprintln!("slow write {i}: {ph:?}");
        }
        if took > worst {
            worst = took;
            worst_at = i;
            worst_ph = ph;
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

    // Attribute each write: one whose window overlaps a probe `fsync` stall
    // is charged to the disk and judged only against `WRITE_BUDGET` (it must
    // still confirm); every other write must beat `WORST_CONFIRM`. A lost
    // wake or a late ack release strands a write with a *healthy* disk, so it
    // lands in the second class and still fails the bound.
    let stalls = probe.finish();
    let mut disk_attributed = 0usize;
    let mut worst_clean = Duration::ZERO;
    let mut worst_clean_at = 0usize;
    let mut worst_clean_ph = Phases::default();
    for (i, (w0, w1), ph) in &samples {
        let overlap = stalls
            .iter()
            .filter(|(a, b)| a < w1 && w0 < b)
            .map(|(a, b)| *b - *a)
            .max();
        if let Some(stall) = overlap {
            disk_attributed += 1;
            eprintln!(
                "write {i}: confirm {:?} overlapped a {stall:?} probe fsync stall -> \
                 attributed to the disk",
                ph.total
            );
        } else if ph.total > worst_clean {
            worst_clean = ph.total;
            worst_clean_at = *i;
            worst_clean_ph = ph.clone();
        }
    }

    // Printed (visible with `--nocapture`) so a CI log of a near-miss shows how
    // close the run came to the limit.
    eprintln!(
        "worst confirm overall: {worst:?} at write {worst_at}; worst on a healthy \
         disk: {worst_clean:?} at write {worst_clean_at} (limit {WORST_CONFIRM:?}); \
         {disk_attributed}/{WRITES} writes overlapped a disk stall ({} probe stalls); \
         term changes during writes: {term_changes}; phases of the worst write: {worst_ph:?}",
        stalls.len()
    );
    assert!(
        worst_clean < WORST_CONFIRM,
        "worst confirm on a healthy disk was {worst_clean:?} at write {worst_clean_at} \
         (limit {WORST_CONFIRM:?}); term changes during writes: {term_changes}; phases: \
         {worst_clean_ph:?}; overall worst {worst:?} at write {worst_at} (phases \
         {worst_ph:?}); {disk_attributed} writes attributed to {} probe disk stalls",
        stalls.len()
    );
    for node in &nodes {
        node.shutdown();
    }
}
