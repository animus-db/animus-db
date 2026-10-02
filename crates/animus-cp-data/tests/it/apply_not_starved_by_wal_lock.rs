//! Liveness: the apply/compaction task must not be starved of `wal_lock` by a
//! back-to-back persist loop (the cp-data twin of
//! `animus-control/tests/apply_not_starved_by_wal_lock.rs`).
//!
//! `drive` starts a new `persist_wal` round whenever `has_unflushed_wal()`,
//! and `persist_wal` and `apply_and_compact`'s compaction rewrite both take
//! the per-group `wal_lock` (on the `SharedWal` path too: `persist_wal` locks
//! it before branching on `shared`). Under continuous proposals on a slow disk
//! an unfair mutex lets the persist loop re-lock ahead of the apply task, so
//! `engine_applied_index` freezes while core `last_applied` keeps tracking
//! commit. This test therefore asserts on `engine_applied_index`, never on
//! `last_applied`, sampled *during* the load and then converged-or-timeout.
//!
//! Runs for both the per-group-WAL and the `SharedWal` path, over several
//! seeds (`ANIMUS_SEED` replays one).

use std::sync::Arc;
use std::time::Duration;

use animus_control::{ProposeResult, SharedWal};
use animus_cp_data::{KvCommand, KvState, RaftKvNode, SHARED_WAL, StorageScope};
use animus_env::nid;
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;
type Wal = SharedWal<KvCommand, KvState>;

const NODES: [u64; 3] = [0, 1, 2];
const SYNC_DELAY: Duration = Duration::from_millis(15);
const TICK: Duration = Duration::from_millis(20);
const LOAD_TICKS: usize = 800;
const SAMPLE_EVERY: usize = 25;
const MAX_ENGINE_LAG: u64 = 1_500;
const CONVERGE_BUDGET_SECS: u64 = 120;

fn start(sim: &Simulator, id: u64, engine: &MemoryEngine, shared: bool) -> KvNode {
    let env = sim.env(nid(id));
    let wal: Option<Arc<Wal>> =
        shared.then(|| block_on(Wal::open(&env, SHARED_WAL)).expect("open"));
    RaftKvNode::start_hosted_with_batcher_and_shared_wal(
        env,
        NODES.iter().copied().map(nid).collect(),
        engine.clone(),
        StorageScope::whole(),
        1,
        None,
        wal,
    )
}

fn leader(nodes: &[KvNode]) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    (ls.len() == 1).then(|| ls[0])
}

fn run(seed: u64, shared: bool) {
    let path = if shared {
        "shared-wal"
    } else {
        "per-group-wal"
    };
    let mut sim = Simulator::new(seed);
    let engines: Vec<MemoryEngine> = NODES.iter().map(|_| MemoryEngine::new()).collect();
    let mut nodes: Vec<KvNode> = NODES
        .iter()
        .map(|&id| start(&sim, id, &engines[id as usize], shared))
        .collect();
    sim.run_for(Duration::from_secs(5));
    assert!(leader(&nodes).is_some(), "no leader ({path}, seed={seed})");

    let mut cfg = DiskConfig::default();
    cfg.set_sync_delay(SYNC_DELAY);
    sim.set_disk_config(cfg.clone());
    for &id in &NODES {
        sim.set_disk_config_for(nid(id), cfg.clone());
    }

    let mut accepted: Vec<usize> = Vec::new();
    let mut crashed: Option<usize> = None;
    let mut worst_lag = 0u64;
    let mut last: [Option<u64>; 3] = [None; 3];
    for t in 0..LOAD_TICKS {
        if let Some(l) = leader(&nodes)
            && matches!(
                nodes[l].put(format!("k{t}").into_bytes(), vec![7u8; 64]),
                ProposeResult::Accepted { .. }
            )
        {
            accepted.push(t);
        }
        if t == LOAD_TICKS / 4
            && let Some(l) = leader(&nodes)
        {
            sim.stop(nid(l as u64));
            crashed = Some(l);
        }
        if t == LOAD_TICKS / 2
            && let Some(l) = crashed.take()
        {
            nodes[l] = start(&sim, l as u64, &engines[l], shared);
            last[l] = None;
        }
        sim.run_for(TICK);

        if t % SAMPLE_EVERY == SAMPLE_EVERY - 1 && t >= SAMPLE_EVERY * 2 {
            for (i, n) in nodes.iter().enumerate() {
                if crashed == Some(i) {
                    last[i] = None;
                    continue;
                }
                let ea = n.engine_applied_index();
                let c = n.commit_index();
                let lag = c.saturating_sub(ea);
                worst_lag = worst_lag.max(lag);
                assert!(
                    lag <= MAX_ENGINE_LAG,
                    "node {i}: engine_applied_index {ea} lags commit {c} by {lag} at tick {t}: \
                     apply task starved ({path}, seed={seed})"
                );
                if let Some(prev) = last[i] {
                    assert!(
                        ea > prev || ea >= c,
                        "node {i}: engine_applied_index frozen at {ea} for {SAMPLE_EVERY} ticks \
                         behind commit {c} under continuous writes at tick {t}: apply task \
                         starved of wal_lock ({path}, seed={seed})"
                    );
                }
                last[i] = Some(ea);
            }
        }
    }
    assert!(
        accepted.len() > 500,
        "only {} proposals accepted ({path}, seed={seed})",
        accepted.len()
    );

    let mut settled = false;
    for _ in 0..CONVERGE_BUDGET_SECS {
        sim.run_for(Duration::from_secs(1));
        if let Some(l) = leader(&nodes) {
            let c = nodes[l].commit_index();
            if nodes.iter().all(|n| n.engine_applied_index() >= c) {
                settled = true;
                break;
            }
        }
    }
    assert!(
        settled,
        "apply frontier did not reach commit within {CONVERGE_BUDGET_SECS}s (worst lag \
         {worst_lag}, {path}, seed={seed})"
    );
}

fn seeds() -> Vec<u64> {
    match std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(s) => vec![s],
        None => (1..=6).collect(),
    }
}

#[test]
fn apply_frontier_tracks_commit_per_group_wal() {
    for s in seeds() {
        run(s, false);
    }
}

#[test]
fn apply_frontier_tracks_commit_shared_wal() {
    for s in seeds() {
        run(s, true);
    }
}
