//! Issue #279: a slow `fsync` must not livelock a tablet group.
//!
//! The bug this pins down: the consensus loop used to call `persist_wal`
//! (drain → `append` → `fsync`) **inline**, before it could return to its
//! `select`. While that I/O blocked, a leader sent no heartbeats and a follower
//! neither processed inbound ones nor re-armed its election deadline — so on a
//! disk whose `fsync` outlasts the 150 ms `election_base`, followers campaigned,
//! every leadership change's no-op commit made more persist work on every
//! replica, and the group never settled. On GitHub's shared runners that showed
//! up as `backfill_seeder::split_during_backfill_converges_with_correct_final_
//! gsi` polling `"CP group leader moved; retry"` for its whole 180 s budget.
//!
//! Reproducing that needed a disk model with **latency**, which `SimEnv` gained
//! in `DiskConfig::set_sync_delay`. With it the failure is deterministic and
//! seed-reproducible instead of a runner-speed lottery.
//!
//! Two assertions, deliberately together — either one alone can pass by
//! coincidence:
//!
//! 1. **No runaway term churn.** A healthy slow-but-stable group elects once or
//!    twice; a livelocked one climbs without bound.
//! 2. **A read barrier still converges.** That is the property the production
//!    symptom actually violated, and it needs a real, reachable leader whose
//!    commit index is advancing — not merely a low term.
//!
//! The group is deliberately elected on a **fast** disk before the delay is
//! injected: a disk slower than the election timeout makes even a correct first
//! election inherently hard, which is an operational limit, not this bug.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{EnvExt, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];

/// Well past `RaftCore`'s 150 ms `election_base`, so a driver that blocks on the
/// I/O provably misses its own deadline.
const SYNC_DELAY: Duration = Duration::from_millis(400);
/// A slow-but-healthy group elects once, maybe twice under jitter. Runaway term
/// churn is the livelock's signature, and it climbs far past this.
const MAX_HEALTHY_TERM: u64 = 6;
const WRITES: usize = 10;
/// Spaced so the group has to keep persisting throughout the window rather than
/// absorbing one burst and then idling.
const WRITE_SPACING: Duration = Duration::from_secs(1);

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn seed() -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7)
}

fn group(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

fn leader(nodes: &[KvNode]) -> Option<usize> {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(_, n)| n.is_leader())
        .map(|(i, _)| i)
        .collect();
    // Two nodes claiming leadership at once is a stale-term artifact of an
    // in-flight election, not a usable leader.
    (ls.len() == 1).then(|| ls[0])
}

fn slow_disk(sim: &Simulator) {
    let mut cfg = DiskConfig::default();
    cfg.set_sync_delay(SYNC_DELAY);
    sim.set_disk_config(cfg.clone());
    for &id in &NODES {
        sim.set_disk_config_for(nid(id), cfg.clone());
    }
}

#[test]
fn a_group_on_a_disk_slower_than_the_election_timeout_stays_led_and_readable() {
    let seed = seed();
    let (mut sim, nodes) = group(seed);

    // Elect on a fast disk — see the module doc for why.
    sim.run_for(Duration::from_secs(5));
    let l =
        leader(&nodes).unwrap_or_else(|| panic!("no leader elected on a fast disk (seed={seed})"));
    let elected_term = nodes[l].term();

    slow_disk(&sim);

    // Sustained writes against whoever currently leads. Some are expected to be
    // refused mid-election even in a healthy run, so this asserts a floor, not
    // every attempt — a run where almost nothing is accepted is itself the
    // "mostly leaderless" failure.
    let mut accepted = 0usize;
    let first_key = b"k0".to_vec();
    for i in 0..WRITES {
        if let Some(l) = leader(&nodes)
            && matches!(
                nodes[l].put(format!("k{i}").into_bytes(), vec![i as u8; 64]),
                ProposeResult::Accepted { .. }
            )
        {
            accepted += 1;
        }
        sim.run_for(WRITE_SPACING);
    }
    assert!(
        accepted * 2 >= WRITES,
        "only {accepted}/{WRITES} writes were accepted — the group spent the \
         window without a usable leader (seed={seed})"
    );

    // Let the backlog drain and any election settle.
    sim.run_for(Duration::from_secs(15));

    let term = nodes.iter().map(|n| n.term()).max().expect("nodes");
    assert!(
        term <= MAX_HEALTHY_TERM,
        "term churned to {term} (elected at {elected_term}) on a slow disk — \
         the consensus loop is missing its own election deadline while \
         persisting (seed={seed})"
    );

    // The property the production symptom actually violated: a read barrier
    // still confirms a live leader and serves committed data.
    let l = leader(&nodes)
        .unwrap_or_else(|| panic!("no stable leader after the write window (seed={seed})"));
    let slot: Arc<Mutex<Option<Option<Vec<u8>>>>> = Arc::new(Mutex::new(None));
    let n = nodes[l].clone();
    let s = Arc::clone(&slot);
    let k = first_key.clone();
    nodes[l].env().clone().spawn_task(async move {
        let v = n.linearizable_get(&k).await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(Duration::from_secs(10));
    let read = slot
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(|| panic!("the read barrier never converged (seed={seed})"));
    assert_eq!(
        read,
        Some(vec![0u8; 64]),
        "the leader could not serve a committed key on a slow disk (seed={seed})"
    );
}
