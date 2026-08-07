//! Regression for the leadership-transfer follow-up fix to ADR 0029 (see the
//! root `CLAUDE.md` engineering-practices entry on the two-layer transfer gate
//! mismatch): under **sustained writes**, `RaftKvNode::reconfigure_step` must
//! actually be able to relocate a group's leader, not stall forever.
//!
//! Defect A (the stall): `reconfigure_step`'s step 4 picked a transfer target
//! with `peer_match(n) >= commit_index()`, but `RaftCore::transfer_leadership`
//! only armed at `peer_match(target) == last_log_index()` — and the returned
//! `bool` was discarded, so the mismatch surfaced nowhere. `propose` (the
//! `RaftKvNode::put` a client uses) is synchronous and fire-and-forget: it
//! appends to the leader's local log and returns immediately, *before* any
//! replication round trip, so `last_log_index` moves the instant a write is
//! proposed while every peer's `peer_match` still reflects the *previous*
//! entry. A reconfigure tick that samples state right after such a propose —
//! exactly what a write-hot tablet produces continuously — therefore always
//! saw the target one entry short of `last_log_index`, so the old `==` arm
//! gate rejected it *every single tick, forever*.
//!
//! The fix aligns the thresholds (`transfer_leadership` now arms at `>=
//! commit_index`, matching the selector) and freezes `propose`/
//! `change_membership` while a transfer is armed (Raft §3.10) — that freeze is
//! what actually lets a target that is merely "caught up to commit" close the
//! remaining gap to `last_log_index`, since new writes stop landing once
//! armed.
//!
//! `reconfigure_step_arms_and_converges_even_while_proposing_every_tick`
//! below is **the test that fails against the pre-fix source** — confirmed by
//! stashing the `animus-control`/`animus-cp-data` source changes (keeping
//! this test file) and re-running it in isolation: it panics
//! ("...never converged...") every time pre-fix, and passes post-fix. Both
//! tests are deterministic and bounded (drive with `run_for`, never `run()`).

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{Clock, EnvExt, NodeId};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

const IDS: [u64; 3] = [0, 1, 2];
/// Realistic production reconfigure-poll cadence (matches
/// `animusd::CP_RECONFIGURE_INTERVAL`'s order of magnitude).
const RECONFIGURE_INTERVAL: Duration = Duration::from_millis(150);
/// Sustained-write cadence, much faster than the reconfigure poll — the log is
/// essentially always growing at every sampling instant, the exact condition
/// defect A needs to reproduce (a lone/occasional write would let the target
/// coincidentally catch all the way up between ticks and mask the bug).
const WRITE_INTERVAL: Duration = Duration::from_millis(5);

fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().collect()
}

/// The current leader among `nodes`, if exactly one reports it.
fn leader_among(nodes: &[KvNode]) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    if ls.len() == 1 { Some(ls[0]) } else { None }
}

/// End-to-end confirmation, over the **production** `spawn_reconfigure_loop`
/// timer and a genuinely asynchronous background write task, that the whole
/// mechanism converges under continuous load — not just the tightly
/// interleaved unit-style reproduction below. (Note: because `SimEnv` message
/// delivery is effectively zero-latency, an async write task's propose+ack
/// round trip can complete before the next scheduled event even against the
/// *pre-fix* source, so this particular test does not reliably fail pre-fix —
/// `reconfigure_step_arms_and_converges_even_while_proposing_every_tick`
/// below is the one that does, by removing that scheduling slack.)
#[test]
fn reconfigure_step_relocates_a_write_hot_leader_under_sustained_writes() {
    let seed = 0x1EAD_7EA5;
    let mut sim = Simulator::new(seed);
    let nodes: Vec<KvNode> = IDS
        .iter()
        .map(|&id| RaftKvNode::start(sim.env(id), IDS.to_vec(), MemoryEngine::new()))
        .collect();
    sim.run_for(Duration::from_secs(2));
    let l0 = leader_among(&nodes).expect("an initial leader");

    // A move that must relocate leadership itself: drop the current leader
    // from the group entirely (the shape a drain, or a rebalance move that
    // happens to land on the current leader, produces — see ADR 0029 §1).
    let desired: BTreeSet<NodeId> = IDS.iter().copied().filter(|&n| n != l0 as u64).collect();
    assert_eq!(desired.len(), 2);

    // Every node runs the production automatic-reconfigure loop toward the
    // fixed `desired` (no control-plane seam needed to reproduce this).
    for node in &nodes {
        let d = desired.clone();
        node.spawn_reconfigure_loop(RECONFIGURE_INTERVAL, move || Some(d.clone()), BTreeSet::new);
    }

    // Sustained write task: every WRITE_INTERVAL, find whoever currently
    // reports leadership and propose a write through them. This keeps
    // `last_log_index` perpetually growing, which is what starves the old,
    // mismatched arm gate.
    let write_nodes = nodes.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_writer = Arc::clone(&stop);
    let writer_env = sim.env(IDS[0]);
    writer_env.clone().spawn_task(async move {
        let mut i: u64 = 0;
        while !stop_writer.load(Ordering::Relaxed) {
            if let Some(l) = leader_among(&write_nodes) {
                let _ = write_nodes[l].put(format!("k{i}").into_bytes(), b"v".to_vec());
                i += 1;
            }
            writer_env.sleep(WRITE_INTERVAL).await;
        }
    });

    // Bounded poll for convergence: a generous budget (20s of sim time) well
    // beyond what a healthy transfer should ever need (a handful of
    // reconfigure ticks once armed) — the "converged-or-timeout" pattern for
    // an eventual property (root CLAUDE.md), not a single fixed-drain
    // snapshot.
    let mut converged = false;
    for _ in 0..40 {
        sim.run_for(Duration::from_millis(500));
        if nodes.iter().all(|n| n.config() == desired) {
            converged = true;
            break;
        }
    }
    stop.store(true, Ordering::Relaxed);

    assert!(
        converged,
        "group never converged to {desired:?} under sustained writes (configs: {:?})",
        nodes.iter().map(|n| n.config()).collect::<Vec<_>>()
    );

    // The old leader is no longer a voter; the relocated group still serves
    // both old and new writes on the new configuration.
    assert!(
        !nodes[l0].config().contains(&(l0 as u64)),
        "the relocated leader must have been dropped from the config"
    );
    let l1 = leader_among(&nodes).expect("a new leader after the transfer");
    assert!(
        desired.contains(&(l1 as u64)),
        "the new leader must be a member of the desired set"
    );
    assert!(matches!(
        nodes[l1].put(b"post".to_vec(), b"v".to_vec()),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));
    for &id in &desired {
        assert_eq!(
            futures::executor::block_on(nodes[id as usize].local_get(b"post")),
            Some(b"v".to_vec()),
            "node {id} missing the post-transfer write"
        );
    }
}

/// Unit-level complement of the sim test above, isolating the exact
/// mechanism: a group whose only remaining reconfigure delta is removing the
/// leader converges *without* ever needing a lucky quiet instant, because the
/// freeze (not luck) is what lets the target catch up.
#[test]
fn reconfigure_step_arms_and_converges_even_while_proposing_every_tick() {
    let seed = 0x0C0F_FEE1;
    let mut sim = Simulator::new(seed);
    let nodes: Vec<KvNode> = IDS
        .iter()
        .map(|&id| RaftKvNode::start(sim.env(id), IDS.to_vec(), MemoryEngine::new()))
        .collect();
    sim.run_for(Duration::from_secs(2));
    let l0 = leader_among(&nodes).expect("an initial leader");
    let desired = set(&IDS
        .iter()
        .copied()
        .filter(|&n| n != l0 as u64)
        .collect::<Vec<_>>());

    // Drive reconfigure_step by hand on every node every tick (only the
    // current leader's call ever does anything — a follower's `is_leader()`
    // gate makes the rest free no-ops, same as production), proposing a write
    // immediately beforehand each time on whoever currently leads — the
    // tightest possible interleaving of "the log just grew" and "try to arm
    // the transfer". Once leadership actually moves, this also drives the
    // *new* leader's own subsequent removal of the old one.
    for _ in 0..100 {
        if let Some(l) = leader_among(&nodes) {
            let _ = nodes[l].put(b"k".to_vec(), b"v".to_vec());
        }
        for n in &nodes {
            n.reconfigure_step(&desired, &BTreeSet::new());
        }
        sim.run_for(Duration::from_millis(20));
        if nodes.iter().all(|n| n.config() == desired) {
            break;
        }
    }
    assert!(
        nodes.iter().all(|n| n.config() == desired),
        "hand-driven reconfigure_step under continuous proposals never converged (configs: {:?})",
        nodes.iter().map(|n| n.config()).collect::<Vec<_>>()
    );
}
