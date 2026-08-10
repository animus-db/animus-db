//! Real-time `ProdEnv` liveness guard for control-plane membership change (ADR
//! 0037 PR5, §9's "election/liveness regression check"): `SimEnv`'s virtual
//! clock proves the *logic* of `change_membership` (`control_membership.rs`),
//! but not that growing a real, multi-threaded control group under real
//! scheduling doesn't itself trigger an election storm — the class of risk
//! `prod_liveness.rs` already guards for plain catch-up. This test grows a
//! real 3-node control group to 5 (two sequential single-server
//! `change_membership` calls, exactly as the operator runbook / `animus admin
//! control-grow` sequences them) over real sockets/time/threads and asserts
//! both that each new voter actually catches up and that leadership stays
//! bounded (no runaway term growth) and settles to one stable leader
//! afterward.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use animus_control::{MetaCommand, NodeStatus, RaftNode};
use animus_env::{Env, NodeId, ProdEnv};
use animus_storage::MemoryEngine;
use tokio::time::{Instant, sleep, timeout};

fn unique_tmp_dir() -> std::path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("animus-ctrl-grow-live-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node,
        labels: BTreeMap::new(),
        status: NodeStatus::Joining,
    }
}

async fn wait_for_leader(nodes: &[&RaftNode<ProdEnv>]) -> usize {
    for _ in 0..400 {
        for (i, n) in nodes.iter().enumerate() {
            if n.is_leader() {
                return i;
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader elected within budget");
}

/// Grows a real 3-node control group to 5 (two sequential single-server
/// `change_membership` calls — the single-server constraint means "3 -> 5" is
/// never one call, mirroring `animus admin control-grow`'s own client-side
/// loop), asserting each new voter catches up promptly and that leadership
/// stays bounded throughout and settles to exactly one stable leader
/// afterward (no election storm from real-thread scheduling under a runtime
/// membership change).
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn grow_three_to_five_under_real_time_stays_live() {
    // A true election storm climbs the term continuously and never settles; a
    // handful of bumps from real scheduling jitter around each config change
    // (5 nodes x several loops on a busy CI box) is not a storm. The primary
    // signal is prompt catch-up + a stable leader once things settle, below.
    const MAX_TERM_DELTA: u64 = 30;

    timeout(Duration::from_secs(90), async {
        let group: Vec<NodeId> = vec![0, 1, 2];
        let dirs: Vec<_> = (0..5).map(|_| unique_tmp_dir()).collect();
        let loop0 = || "127.0.0.1:0".parse::<SocketAddr>().unwrap();

        // Bind all five envs up front (every address known from the start —
        // mirrors a real deployment where the new voters' addresses are
        // discoverable before they're added), but only start the Raft driver
        // on the first three.
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

        let node0 = RaftNode::start(envs[0].clone(), group.clone(), MemoryEngine::new());
        let node1 = RaftNode::start(envs[1].clone(), group.clone(), MemoryEngine::new());
        let node2 = RaftNode::start(envs[2].clone(), group.clone(), MemoryEngine::new());
        let original = [&node0, &node1, &node2];

        let leader_idx = wait_for_leader(&original).await;
        let leader = original[leader_idx];
        let term_start = leader.term();

        // Some pre-existing state every new voter must catch up on.
        for i in 0..20u64 {
            leader.propose(upsert(100 + i));
        }
        leader.flush().await;

        // Node 3 starts as a quiet non-voter (its own config already
        // includes itself and the current three voters; the current three
        // don't yet know about it) — the same shape as a real freshly-started
        // `animusd control` growth process.
        let node3 = RaftNode::start(envs[3].clone(), vec![0, 1, 2, 3], MemoryEngine::new());
        let leader_idx = wait_for_leader(&original).await;
        let leader = original[leader_idx];
        assert!(
            matches!(
                leader.change_membership([0u64, 1, 2, 3].into_iter().collect()),
                animus_control::ProposeResult::Accepted { .. }
            ),
            "growing 3 -> 4 should be accepted as a single-server delta"
        );

        let target = leader.last_log_index();
        let mut caught_up = false;
        for _ in 0..200 {
            leader.flush().await;
            if node3.last_applied() >= target && node3.config().len() == 4 {
                caught_up = true;
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        assert!(caught_up, "node 3 did not catch up growing 3 -> 4");

        // Grow again, 4 -> 5, against whichever node is leader now (may have
        // moved during the first growth step's scheduling jitter).
        let quartet = [&node0, &node1, &node2, &node3];
        let node4 = RaftNode::start(envs[4].clone(), vec![0, 1, 2, 3, 4], MemoryEngine::new());
        let leader_idx = wait_for_leader(&quartet).await;
        let leader = quartet[leader_idx];
        assert!(
            matches!(
                leader.change_membership([0u64, 1, 2, 3, 4].into_iter().collect()),
                animus_control::ProposeResult::Accepted { .. }
            ),
            "growing 4 -> 5 should be accepted as a single-server delta"
        );

        let target = leader.last_log_index();
        let mut caught_up = false;
        for _ in 0..200 {
            leader.flush().await;
            if node4.last_applied() >= target && node4.config().len() == 5 {
                caught_up = true;
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        assert!(caught_up, "node 4 did not catch up growing 4 -> 5");

        let quintet = [&node0, &node1, &node2, &node3, &node4];
        let term_after_growth = quintet.iter().map(|n| n.term()).max().unwrap();
        let delta = term_after_growth.saturating_sub(term_start);
        assert!(
            delta <= MAX_TERM_DELTA,
            "control leadership ran away while growing 3 -> 5: term delta {delta} > \
             {MAX_TERM_DELTA} (start={term_start}, after={term_after_growth})"
        );

        // Settle: after growth completes, leadership must become — and stay
        // — stable (exactly one leader, no further transitions) over a
        // continued observation window. This is the "no election storm"
        // signal that actually matters: a bounded delta above merely rules
        // out a runaway climb, this rules out an ongoing low-grade churn.
        wait_for_leader(&quintet).await;
        let settle_deadline = Instant::now() + Duration::from_secs(5);
        let mut last_leader: Option<(NodeId, u64)> = None;
        let mut transitions = 0u32;
        while Instant::now() < settle_deadline {
            let leaders: Vec<(NodeId, u64)> = quintet
                .iter()
                .filter(|n| n.is_leader())
                .map(|n| (n.env().node_id(), n.term()))
                .collect();
            assert!(
                leaders.len() <= 1,
                "more than one node believes itself leader at once: {leaders:?}"
            );
            if let Some(&current) = leaders.first()
                && last_leader != Some(current)
            {
                if last_leader.is_some() {
                    transitions += 1;
                }
                last_leader = Some(current);
            }
            sleep(Duration::from_millis(100)).await;
        }
        assert!(
            transitions <= 2,
            "leadership did not settle after growth completed: {transitions} transitions \
             observed over the 5s settle window (last leader {last_leader:?})"
        );
        assert!(
            last_leader.is_some(),
            "no stable leader observed during the settle window"
        );

        for e in &envs {
            e.shutdown();
        }
        for dir in &dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    })
    .await
    .expect("control-plane 3 -> 5 growth liveness smoke test timed out");
}
