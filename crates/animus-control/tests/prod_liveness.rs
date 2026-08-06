//! Real-time `ProdEnv` **driver-liveness** smoke test for the control plane
//! (deferred fix #5 — the counterpart of the CP-data fix, ADR 0017): on real
//! sockets/time/threads, a freshly-joined follower must catch a **large, compacted
//! `Metadata`** cluster up *quickly*, and leadership must not run away while it does.
//!
//! The specific hazard this fix removes — [`RaftCore::snapshot_chunk_for`]
//! re-serializing the whole `Metadata` per 1KB chunk (O(state) per `InstallSnapshot`
//! message) — is exercised with a huge, deterministic margin by the `RaftCore`-level
//! `install_snapshot.rs::large_snapshot_ships_in_o_chunk_time_not_o_state` (a live
//! cluster catch-up races leadership/AppendEntries and does not reliably traverse a
//! long chunk-stream, so that timing teeth lives at the core level). This test is the
//! real-thread *integration* guard: it confirms the assembled control plane stays
//! live (no deadlock, no election runaway, catch-up completes promptly) over a
//! multi-MB metadata on the multi-threaded `ProdEnv` — the class `SimEnv`'s virtual
//! time cannot observe.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use animus_control::{MetaCommand, NodeStatus, RaftNode};
use animus_env::{Env, NodeId, ProdEnv};
use tokio::time::{sleep, timeout};

fn unique_tmp_dir() -> std::path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("animus-ctrl-live-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// An `UpsertMember` carrying a labels map with `n_keys` entries, so a handful of
/// these build a many-thousand-entry (multi-MB) `Metadata`.
fn fat_member(node: u64, n_keys: usize) -> MetaCommand {
    let mut labels = BTreeMap::new();
    for k in 0..n_keys {
        labels.insert(format!("k{node}_{k}"), format!("v{k}"));
    }
    MetaCommand::UpsertMember {
        node,
        labels,
        status: NodeStatus::Active,
    }
}

/// A freshly-joined follower catches a large, compacted control cluster up quickly,
/// and leadership does not run away while it does.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn large_metadata_catch_up_stays_live() {
    // Coarse runaway-storm guard only: a true storm climbs the term continuously; a
    // handful of bumps from a bounded compaction stall + `ProdEnv` scheduling jitter
    // (3 nodes × several loops on a busy CI box) is not a storm. The primary signal
    // is *prompt catch-up* below.
    const MAX_TERM_DELTA: u64 = 25;

    timeout(Duration::from_secs(90), async {
        let group: Vec<NodeId> = vec![0, 1, 2];
        let dirs: Vec<_> = (0..3).map(|_| unique_tmp_dir()).collect();
        let loop0 = || "127.0.0.1:0".parse::<SocketAddr>().unwrap();

        // Bind all three envs up front (so every address is known), but only *start*
        // the Raft driver on nodes 0 and 1 — node 2 stays dark so it falls behind.
        let mut envs = Vec::new();
        for (i, dir) in dirs.iter().enumerate() {
            let (env, _addr) = ProdEnv::bind(i as u64, loop0(), dir).await.expect("bind");
            envs.push(env);
        }
        let book: BTreeMap<NodeId, SocketAddr> =
            envs.iter().map(|e| (e.node_id(), e.local_addr())).collect();
        for e in &envs {
            e.set_peers(book.clone());
        }

        // Start the two-node majority (2/3 of the 3-voter group commits without 2).
        let node0 = RaftNode::start(envs[0].clone(), group.clone());
        let node1 = RaftNode::start(envs[1].clone(), group.clone());

        async fn leader_of<'a>(nodes: &'a [&'a RaftNode<ProdEnv>]) -> Option<usize> {
            for _ in 0..200 {
                for (i, n) in nodes.iter().enumerate() {
                    if n.is_leader() {
                        return Some(i);
                    }
                }
                sleep(Duration::from_millis(50)).await;
            }
            None
        }
        let running = [&node0, &node1];
        let leader_idx = leader_of(&running).await.expect("no leader elected");
        let leader = running[leader_idx];

        // Grow the replicated Metadata to ~1.1MB (130 members * 500 label entries).
        // Sized so a single compaction serialize (~50ms) stays under the 150ms
        // election timeout (stable setup — hazard #1, the bounded compaction stall).
        for i in 0..130u64 {
            leader.propose(fat_member(100 + i, 500));
        }
        // A burst of tiny filler entries (re-upserting one dummy member) pushes the
        // *log* far past the snapshot threshold on **every** replica, so all of them
        // compact their prefix away — a freshly-joined node then cannot catch up from
        // a not-yet-compacted peer's log. (Compaction is local, so without the filler
        // whichever node leads may still hold the whole log and ship it cheaply.)
        for _ in 0..300 {
            leader.propose(MetaCommand::UpsertMember {
                node: 999,
                labels: BTreeMap::new(),
                status: NodeStatus::Active,
            });
        }

        // Wait until both running replicas have compacted well past the fat members.
        let mut compacted = false;
        for _ in 0..600 {
            leader.flush().await;
            if node0.snapshot_index() >= 200 && node1.snapshot_index() >= 200 {
                compacted = true;
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        assert!(
            compacted,
            "replicas did not compact past the fat members (node0 snap={}, node1 snap={})",
            node0.snapshot_index(),
            node1.snapshot_index()
        );

        // Re-resolve the current leader (leadership may have moved during setup).
        let leader_idx = leader_of(&running).await.expect("no leader after setup");
        let leader = running[leader_idx];
        let target = leader.snapshot_index();
        let term_before = leader.term();

        // Start node 2 (dark until now): it must catch up to the large, compacted
        // state. Primary signal: it does so promptly (12s budget; the fix serves the
        // ~1100-chunk snapshot in well under a second — the timed core test proves the
        // per-chunk cost directly).
        let node2 = RaftNode::start(envs[2].clone(), group.clone());
        let started = std::time::Instant::now();
        let mut caught_up = false;
        for _ in 0..240 {
            leader.flush().await;
            if node2.snapshot_index() >= target || node2.last_applied() >= target {
                caught_up = true;
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        let secs = started.elapsed().as_secs_f64();
        let delta = leader.term().saturating_sub(term_before);
        eprintln!(
            "catch-up: metadata≈{} members, target={target}, caught_up={caught_up} in {secs:.2}s, \
             control term Δ{delta}",
            leader.metadata().members.len(),
        );
        assert!(
            caught_up,
            "node 2 did not catch up to the compacted state ({secs:.1}s): node2 snap={} \
             applied={}, target={target}",
            node2.snapshot_index(),
            node2.last_applied(),
        );
        assert!(
            delta <= MAX_TERM_DELTA,
            "control leadership ran away while a follower caught up: term Δ{delta} > \
             {MAX_TERM_DELTA}"
        );

        for e in &envs {
            e.shutdown();
        }
        for dir in &dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    })
    .await
    .expect("control driver-liveness smoke test timed out");
}
