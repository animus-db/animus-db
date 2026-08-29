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

// ADR 0003 / ADR 0061 Decision 4 (rung B5): a real-thread ProdEnv driver-
// liveness smoke test (see the module doc above) — the whole point is
// observing real time/threads, which SimEnv's virtual clock structurally
// cannot do.
#![allow(
    clippy::disallowed_methods,
    reason = "real-thread ProdEnv driver-liveness smoke test (the class SimEnv's virtual clock cannot observe, see module doc); ADR 0061 Decision 4"
)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use animus_control::{MetaCommand, NodeStatus, ProposeResult, RaftNode};
use animus_env::{Env, NodeId, ProdEnv, nid};
use animus_storage::{LsmEngine, MemoryEngine};
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
///
/// `Joining`, not `Active`: these ids are pure bulk payload (no envs, no
/// heartbeats — the test only cares about `Metadata`'s serialized *size*), and
/// `Joining`/`Leaving` are the two statuses the failure detector deliberately
/// never judges (ADR 0012). Registering 130 fake nodes `Active` (ADR 0030
/// phantom-member hardening: an `Active` member the detector never hears a
/// heartbeat from is now demoted after one `DETECT_TIMEOUT`) turned every one of
/// them into a "phantom" at once, and `detect_loop` proposing ~130 `Down`
/// transitions in a single tick flooded the leader's WAL right as node 2 was
/// trying to catch up — a real regression this test caught, not a flake.
fn fat_member(node: u64, n_keys: usize) -> MetaCommand {
    let mut labels = BTreeMap::new();
    for k in 0..n_keys {
        labels.insert(format!("k{node}_{k}"), format!("v{k}"));
    }
    MetaCommand::UpsertMember {
        node: nid(node),
        labels,
        status: NodeStatus::Joining,
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
        let group: Vec<NodeId> = vec![nid(0), nid(1), nid(2)];
        let dirs: Vec<_> = (0..3).map(|_| unique_tmp_dir()).collect();
        let loop0 = || "127.0.0.1:0".parse::<SocketAddr>().unwrap();

        // Bind all three envs up front (so every address is known), but only *start*
        // the Raft driver on nodes 0 and 1 — node 2 stays dark so it falls behind.
        let mut envs = Vec::new();
        for (i, dir) in dirs.iter().enumerate() {
            let (env, _addr) = ProdEnv::bind(nid(i as u64), loop0(), dir)
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

        // Start the two-node majority (2/3 of the 3-voter group commits without 2).
        let node0 = RaftNode::start(envs[0].clone(), group.clone(), MemoryEngine::new());
        let node1 = RaftNode::start(envs[1].clone(), group.clone(), MemoryEngine::new());

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
                node: nid(999),
                labels: BTreeMap::new(),
                // `Joining`, not `Active` — see `fat_member`'s doc.
                status: NodeStatus::Joining,
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
        let node2 = RaftNode::start(envs[2].clone(), group.clone(), MemoryEngine::new());
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
            e.shutdown_and_wait().await;
        }
        for dir in &dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    })
    .await
    .expect("control driver-liveness smoke test timed out");
}

/// How many `MetaCommand`s the sustained-churn test below proposes.
const CHURN_COMMANDS: u64 = 300;

/// Spacing between churn proposals — a steady drip, not a burst, so the
/// apply task's real engine I/O is exercised continuously across several
/// seconds of wall-clock time rather than all at once.
const CHURN_INTERVAL: Duration = Duration::from_millis(10);

/// **ADR 0038 PR3's actual storm-risk surface**: sustained `MetaCommand`
/// churn through the leader of a real 3-node control group backed by a
/// **genuine on-disk `LsmEngine`** — not `MemoryEngine`, whose I/O is
/// synchronous/trivial and so cannot exercise the hazard at all. Since the
/// cutover, every committed-and-durable command is applied by a separate
/// async apply task that does real engine I/O (`merge_batch`, periodic
/// WAL/SSTable compaction) — exactly the shape that caused the
/// `animus-cp-data` election-storm bug class when apply/compaction ran
/// inline on the consensus loop (root `CLAUDE.md`'s engineering-lessons
/// entry). This test is the real-thread integration guard that a slow batch
/// of real disk merges never blocks the *consensus* loop's own
/// heartbeat/`AppendEntries` servicing long enough to trip the election
/// timeout — the property `SimEnv`'s virtual clock cannot observe and that
/// this file's other `MemoryEngine`-backed test does not exercise.
///
/// Asserts, mirroring `control_membership_prod.rs`'s
/// `grow_three_to_five_under_real_time_stays_live` technique: (a) a bounded
/// term delta across the whole churn window (a coarse runaway-storm guard —
/// a handful of bumps from real scheduling jitter is not a storm) *and* a
/// bounded count of leadership transitions actually observed while churning
/// (the tighter "stayed stable throughout," not just "didn't run away"
/// signal); and (b) every node's published cache converges on all churned
/// commands within a bounded deadline.
///
/// Because (b) demands **every** churned command commit, the churn drip must
/// be a *well-behaved Raft client*, not a fire-and-forget one (issue #269,
/// the lessons-log retry discipline): a `NotLeader` return is retried
/// against the re-resolved leader, and a proposal `Accepted` by a leader
/// deposed before replicating it (appended-but-superseded — invisible at
/// propose time) is caught and re-proposed by the confirm-and-repair pass
/// in the convergence loop below. A leadership transition mid-churn is
/// within this test's tolerances (`MAX_TRANSITIONS`), so losing the
/// handful of commands proposed across one must not fail convergence.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn sustained_metadata_churn_over_a_real_engine_stays_live() {
    const MAX_TERM_DELTA: u64 = 20;
    const MAX_TRANSITIONS: u32 = 3;

    timeout(Duration::from_secs(120), async {
        let group: Vec<NodeId> = vec![nid(0), nid(1), nid(2)];
        let dirs: Vec<_> = (0..3).map(|_| unique_tmp_dir()).collect();
        let loop0 = || "127.0.0.1:0".parse::<SocketAddr>().unwrap();

        let mut envs = Vec::new();
        for (i, dir) in dirs.iter().enumerate() {
            let (env, _addr) = ProdEnv::bind(nid(i as u64), loop0(), dir)
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

        // A REAL on-disk system-keyspace engine per node (distinct file
        // prefix from `raft.wal`, same directory — mirrors `animusd`'s own
        // `SYSKV_LSM_PREFIX` convention) — the apply task's `merge_batch`
        // and periodic compaction genuinely touch disk here, unlike every
        // other `RaftNode`-based test in this file/suite.
        let engine0 = LsmEngine::open(envs[0].clone(), "ctrl-syskv")
            .await
            .expect("open node 0's real on-disk system-keyspace engine");
        let engine1 = LsmEngine::open(envs[1].clone(), "ctrl-syskv")
            .await
            .expect("open node 1's real on-disk system-keyspace engine");
        let engine2 = LsmEngine::open(envs[2].clone(), "ctrl-syskv")
            .await
            .expect("open node 2's real on-disk system-keyspace engine");

        let node0 = RaftNode::start(envs[0].clone(), group.clone(), engine0);
        let node1 = RaftNode::start(envs[1].clone(), group.clone(), engine1);
        let node2 = RaftNode::start(envs[2].clone(), group.clone(), engine2);
        let nodes = [&node0, &node1, &node2];

        async fn leader_of(nodes: &[&RaftNode<ProdEnv>; 3]) -> Option<usize> {
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

        // --- Initial settle: elect a leader before measuring anything.
        let mut leader_idx = leader_of(&nodes).await.expect("no leader elected");
        let term_start = nodes[leader_idx].term();

        // --- Sustained churn: CHURN_COMMANDS at a steady drip through the
        // leader (re-resolving it if leadership moves mid-churn), tracking
        // every leadership transition actually observed along the way.
        let churn_started = std::time::Instant::now();
        let mut transitions = 0u32;
        let mut last_leader_term = term_start;
        for i in 0..CHURN_COMMANDS {
            let cmd = MetaCommand::UpsertMember {
                node: nid(1_000 + i),
                labels: BTreeMap::new(),
                // `Joining`, not `Active` — see `fat_member`'s doc above:
                // the failure detector never judges this status, so this
                // pure churn workload can't trip an unrelated `Down` storm.
                status: NodeStatus::Joining,
            };
            // Fold the whole "find leader → propose" sequence into one
            // bounded retry poll (lessons log): leadership can move *between*
            // the `is_leader()` check and the propose, and an armed transfer
            // freezes proposals on a still-leader — both surface as
            // `NotLeader`, a routine race to retry through, not a failure.
            let mut accepted = false;
            for _attempt in 0..100 {
                if !nodes[leader_idx].is_leader() {
                    leader_idx = leader_of(&nodes)
                        .await
                        .unwrap_or_else(|| panic!("no leader mid-churn at command {i}"));
                }
                let term_now = nodes[leader_idx].term();
                if term_now != last_leader_term {
                    transitions += 1;
                    last_leader_term = term_now;
                }
                match nodes[leader_idx].propose(cmd.clone()) {
                    ProposeResult::Accepted { .. } => {
                        accepted = true;
                        break;
                    }
                    ProposeResult::NotLeader { .. } => sleep(Duration::from_millis(50)).await,
                }
            }
            assert!(
                accepted,
                "churn command {i} was never accepted by any leader"
            );
            sleep(CHURN_INTERVAL).await;
        }
        let churn_secs = churn_started.elapsed().as_secs_f64();
        let term_after_churn = nodes.iter().map(|n| n.term()).max().unwrap();
        let delta = term_after_churn.saturating_sub(term_start);

        // --- Convergence + repair: every node's apply-task-published cache
        // reflects all CHURN_COMMANDS churned members within a bounded
        // deadline — real engine I/O (fsync/compaction) is slower than
        // `MemoryEngine`, so this budget is generous (20s) to stay non-flaky
        // on a busy box.
        //
        // `Accepted` means "appended on the then-leader," never "committed"
        // (lessons log): a proposal appended just before a leadership
        // transition can be superseded and vanish with no signal at propose
        // time (issue #269 — all three nodes flat at 266/300 for the full
        // budget after exactly one mid-churn transition). The well-behaved
        // client's answer is confirm-and-repair: watch the *current
        // leader's* applied cache and re-propose whatever it still lacks.
        // `UpsertMember` is idempotent, so re-proposing a committed-but-not-
        // yet-applied member is harmless; the ~500ms cadence gives a merely
        // slow apply task time to publish before being re-proposed at.
        let convergence_started = std::time::Instant::now();
        let mut converged = false;
        let mut repairs = 0u64;
        for round in 0..400u32 {
            if nodes
                .iter()
                .all(|n| n.metadata().members.len() as u64 >= CHURN_COMMANDS)
            {
                converged = true;
                break;
            }
            if round % 10 == 9
                && let Some(li) = nodes.iter().position(|n| n.is_leader())
            {
                let present = nodes[li].metadata().members;
                for i in 0..CHURN_COMMANDS {
                    if !present.contains_key(&nid(1_000 + i))
                        && matches!(
                            nodes[li].propose(MetaCommand::UpsertMember {
                                node: nid(1_000 + i),
                                labels: BTreeMap::new(),
                                status: NodeStatus::Joining,
                            }),
                            ProposeResult::Accepted { .. }
                        )
                    {
                        repairs += 1;
                    }
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
        let convergence_secs = convergence_started.elapsed().as_secs_f64();
        let member_counts: Vec<usize> = nodes.iter().map(|n| n.metadata().members.len()).collect();

        eprintln!(
            "sustained real-engine churn: {CHURN_COMMANDS} commands over {churn_secs:.2}s, \
             control term Δ{delta} (start={term_start}, after={term_after_churn}), \
             {transitions} leadership transition(s) observed during churn, \
             converged={converged} in {convergence_secs:.2}s with {repairs} repair \
             re-proposal(s) (member counts={member_counts:?})"
        );

        assert!(
            converged,
            "nodes did not converge on {CHURN_COMMANDS} churned members within budget: \
             member counts = {member_counts:?}"
        );
        assert!(
            delta <= MAX_TERM_DELTA,
            "control leadership ran away during sustained real-engine churn: term Δ{delta} > \
             {MAX_TERM_DELTA}"
        );
        assert!(
            transitions <= MAX_TRANSITIONS,
            "leadership did not stay stable during sustained real-engine churn: {transitions} \
             transitions observed (> {MAX_TRANSITIONS}), term Δ{delta}"
        );

        for e in &envs {
            e.shutdown_and_wait().await;
        }
        for dir in &dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    })
    .await
    .expect("sustained real-engine metadata churn liveness test timed out");
}
