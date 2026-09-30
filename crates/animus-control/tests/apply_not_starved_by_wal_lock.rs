//! ADR 0038 liveness: the apply/compaction task must not be starved of
//! `wal_lock` by a back-to-back persist loop.
//!
//! `drive` starts a new `persist_wal` round the moment the previous one
//! lands, and under continuous proposals `has_unflushed_wal()` never goes
//! false. With an unfair async mutex the consensus loop re-locked `wal_lock`
//! ahead of the apply task's compaction section (woken waiter loses to a
//! barging new locker), so once the log passed `SNAPSHOT_THRESHOLD` the
//! apply task parked forever inside `meta_apply_and_compact`: no further
//! `merge_batch`, `engine_applied_index` frozen, the `metadata()` cache
//! stale, and `pending_apply` growing without bound. Core `last_applied`
//! kept tracking commit and hid the stall, so this test asserts on the ADR
//! 0038 frontier (`engine_applied_index`) and on `metadata()`, never on
//! `last_applied`.
//!
//! Every node runs a nonzero `SyncDelay`; proposals are continuous; the
//! leader is crashed and restarted mid-run. The assertion is sampled
//! *during* the load (after the load stops the stall would drain by itself).

use std::time::Duration;

use animus_control::raft::ProposeResult;
use animus_control::{ColumnType, MetaCommand, RaftNode, TableSchema};
use animus_env::nid;
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];
const SYNC_DELAY: Duration = Duration::from_millis(15);
/// One proposal per tick: far faster than a 15 ms fsync can drain, so a
/// persist round is always pending.
const TICK: Duration = Duration::from_millis(20);
const LOAD_TICKS: usize = 800;
/// Well over `SNAPSHOT_THRESHOLD` (64) several times, so compaction fires
/// repeatedly under load.
const MAX_ENGINE_LAG: u64 = 1_500;
/// Ticks between frontier samples (500 ms of virtual time).
const SAMPLE_EVERY: usize = 25;
const CONVERGE_BUDGET: Duration = Duration::from_secs(120);

fn seed() -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0xA11_D0C)
}

fn schema(i: usize) -> MetaCommand {
    MetaCommand::CreateTableSchema {
        table: format!("t{i}"),
        schema: TableSchema::simple("id", ColumnType::Uuid),
    }
}

fn leader(nodes: &[RaftNode<SimEnv>]) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    (ls.len() == 1).then(|| ls[0])
}

#[test]
fn apply_frontier_tracks_commit_under_continuous_proposals_on_a_slow_disk() {
    let seed = seed();
    let mut sim = Simulator::new(seed);
    let engines: Vec<MemoryEngine> = NODES.iter().map(|_| MemoryEngine::new()).collect();
    let peers = || NODES.iter().copied().map(nid).collect::<Vec<_>>();
    let mut nodes: Vec<RaftNode<SimEnv>> = NODES
        .iter()
        .map(|&id| RaftNode::start(sim.env(nid(id)), peers(), engines[id as usize].clone()))
        .collect();

    sim.run_for(Duration::from_secs(5));
    assert!(leader(&nodes).is_some(), "no leader (seed={seed})");

    let mut cfg = DiskConfig::default();
    cfg.set_sync_delay(SYNC_DELAY);
    sim.set_disk_config(cfg.clone());
    for &id in &NODES {
        sim.set_disk_config_for(nid(id), cfg.clone());
    }

    let mut accepted: Vec<usize> = Vec::new();
    let mut crashed: Option<(usize, usize)> = None;
    let mut worst_lag = 0u64;
    let mut last_frontier: [Option<u64>; 3] = [None; 3];
    for t in 0..LOAD_TICKS {
        if let Some(l) = leader(&nodes)
            && matches!(nodes[l].propose(schema(t)), ProposeResult::Accepted { .. })
        {
            accepted.push(t);
        }
        // Crash the current leader a quarter in, restart it at the half.
        if t == LOAD_TICKS / 4
            && let Some(l) = leader(&nodes)
        {
            sim.stop(nid(l as u64));
            crashed = Some((l, t));
        }
        if t == LOAD_TICKS / 2
            && let Some((l, _)) = crashed.take()
        {
            nodes[l] = RaftNode::start(sim.env(nid(l as u64)), peers(), engines[l].clone());
            last_frontier[l] = None;
        }
        sim.run_for(TICK);

        // Sample the ADR 0038 frontier during the load on every live node:
        // it must keep advancing (a frozen `engine_applied_index` is the
        // stall) and stay within a bound of commit.
        if t % SAMPLE_EVERY == SAMPLE_EVERY - 1 && t >= SAMPLE_EVERY * 2 {
            for (i, n) in nodes.iter().enumerate() {
                if crashed.is_some_and(|(c, _)| c == i) {
                    last_frontier[i] = None;
                    continue;
                }
                let ea = n.engine_applied_index();
                let lag = n.commit_index().saturating_sub(ea);
                worst_lag = worst_lag.max(lag);
                assert!(
                    lag <= MAX_ENGINE_LAG,
                    "node {i}: engine_applied_index {ea} lags commit {} by {lag} \
                     (> {MAX_ENGINE_LAG}) at tick {t}: the apply task is starved \
                     (core last_applied={}) (seed={seed})",
                    n.commit_index(),
                    n.last_applied(),
                );
                if let Some(prev) = last_frontier[i] {
                    // Only a frontier that is *behind commit and not moving*
                    // is the stall; a leaderless window (the crash) freezes
                    // commit and frontier together, which is fine.
                    assert!(
                        ea > prev || ea >= n.commit_index(),
                        "node {i}: engine_applied_index frozen at {ea} for {SAMPLE_EVERY} \
                         ticks behind commit under continuous proposals at tick {t} (commit={}, core \
                         last_applied={}): the apply task is starved of wal_lock \
                         (seed={seed})",
                        n.commit_index(),
                        n.last_applied(),
                    );
                }
                last_frontier[i] = Some(ea);
            }
        }
    }
    assert!(
        accepted.len() > 500,
        "only {} proposals accepted (seed={seed})",
        accepted.len()
    );

    // Converged-or-timeout: every node's frontier reaches the leader's commit
    // and the leader's metadata reflects every accepted schema.
    let deadline = CONVERGE_BUDGET.as_secs();
    let mut settled = false;
    for _ in 0..deadline {
        sim.run_for(Duration::from_secs(1));
        if let Some(l) = leader(&nodes) {
            let c = nodes[l].commit_index();
            let md = nodes[l].metadata();
            // `Accepted` is "appended locally", not committed: proposals the
            // crashed leader took but never replicated are legitimately
            // lost, so only the proposals made after the restart are
            // required to be visible.
            let visible = accepted
                .iter()
                .filter(|&&i| i > LOAD_TICKS / 2)
                .all(|i| md.schemas.contains(&format!("t{i}")));
            if visible && nodes.iter().all(|n| n.engine_applied_index() >= c) {
                settled = true;
                break;
            }
        }
    }
    assert!(
        settled,
        "apply frontier / metadata did not converge within {deadline}s (worst lag {worst_lag}, \
         seed={seed})"
    );
}
