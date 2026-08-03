//! Stage A.2 (ADR 0017): a lagging follower catches up via a **streaming
//! `InstallSnapshot`** carrying the leader's **engine image**. After the leader
//! compacts (snapshots the engine + truncates the Raft log prefix), a replica
//! that missed the writes can no longer be caught up by `AppendEntries` (the log
//! is gone), so the leader ships the engine image; the follower writes it into
//! its own engine and then replays the log tail on top.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn group(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| RaftKvNode::start(sim.env(id), NODES.to_vec(), MemoryEngine::new()))
        .collect();
    (sim, nodes)
}

fn leader(nodes: &[KvNode], live: &[usize], seed: u64) -> usize {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(i, n)| live.contains(i) && n.is_leader())
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one leader among {live:?}, got {ls:?} (seed={seed})"
    );
    ls[0]
}

#[test]
fn lagging_follower_catches_up_via_snapshot() {
    let seed = 0x5A0;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect
    let l = leader(&nodes, &[0, 1, 2], seed);
    let lagging = (0..3).find(|&i| i != l).expect("a follower exists");

    // Crash the lagging follower (so it stays at its old term — no rejoin churn).
    // The surviving two are still a majority.
    sim.crash(lagging as u64);

    // Write well past the compaction threshold (64) so the leader snapshots and
    // truncates the log prefix the crashed follower would have needed.
    const N: u64 = 150;
    for i in 0..N {
        match nodes[l].put(
            format!("k{i:03}").into_bytes(),
            format!("v{i}").into_bytes(),
        ) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected put {i}: {other:?} (seed={seed})"),
        }
    }
    sim.run_for(Duration::from_secs(3)); // replicate + apply + compact on {l, third}

    // Restart the lagging follower. Its log is far behind the leader's compacted
    // base, so the leader must catch it up with an InstallSnapshot (engine image),
    // then replay the post-snapshot log tail on top.
    sim.restart(lagging as u64);
    sim.run_for(Duration::from_secs(6));

    // The recovered follower's engine converged to every write (sample the range).
    for i in [0u64, 1, 64, 100, N - 1] {
        let key = format!("k{i:03}").into_bytes();
        assert_eq!(
            block_on(nodes[lagging].local_get(&key)),
            Some(format!("v{i}").into_bytes()),
            "follower {lagging} missing k{i:03} after snapshot catch-up (seed={seed})"
        );
    }
}
