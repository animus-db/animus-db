//! Issue #1116: a WAL-compaction rewrite must not stall persist rounds.
//!
//! `prod_compaction_persist_round` failed intermittently on CI with a worst
//! confirm of 2.1s. The mechanism: the apply task's compaction ran the whole
//! `env.replace` (temp-file write + `fsync` + rename + directory `fsync`)
//! **while holding `wal_lock`**, the same FIFO lock the consensus loop's
//! `persist_wal` takes for every round. One slow `fsync` inside the rewrite
//! therefore froze the group's persist/ack path for its whole duration.
//!
//! `SimEnv` models that slow half of a `replace` with
//! `DiskConfig::set_replace_data_delay` (the temp-file content write + fsync;
//! the swap itself costs only `sync_delay`). With a multi-second replace
//! delay on every node, a write must still confirm in well under the delay:
//! the rewrite's slow phase has to run outside `wal_lock`.
//!
//! Deterministic and seed-reproducible (`ANIMUS_SEED`): a pure `SimEnv`
//! virtual-time measurement, no wall clock.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{EnvExt, Metric, MetricsHandle, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];
/// How long the rewrite's temp-file write + fsync takes on every node.
const REPLACE_DELAY: Duration = Duration::from_secs(3);
/// Far below `REPLACE_DELAY`: a write that waits out a rewrite's slow phase
/// cannot meet this; a healthy fast-disk confirm is milliseconds.
const CONFIRM_LIMIT: Duration = Duration::from_millis(600);
/// Comfortably more than `COMPACT_THRESHOLD` (64) applies per replica.
const WRITES: usize = 200;
const STEP: Duration = Duration::from_millis(10);

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn seed() -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(11)
}

fn leader(nodes: &[KvNode]) -> Option<usize> {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(_, n)| n.is_leader())
        .map(|(i, _)| i)
        .collect();
    (ls.len() == 1).then(|| ls[0])
}

#[test]
fn writes_confirm_while_a_wal_rewrite_is_stalled_in_its_fsync() {
    let seed = seed();
    let mut sim = Simulator::new(seed);
    let handles: Vec<MetricsHandle> = NODES.iter().map(|_| MetricsHandle::recording()).collect();
    let nodes: Vec<KvNode> = NODES
        .iter()
        .zip(&handles)
        .map(|(&id, m)| {
            RaftKvNode::start_with_metrics(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
                m.clone(),
            )
        })
        .collect();

    // Elect on a fast disk, then make every replace's fsync slow.
    sim.run_for(Duration::from_secs(5));
    assert!(leader(&nodes).is_some(), "no leader (seed={seed})");
    let mut cfg = DiskConfig::default();
    cfg.set_replace_data_delay(REPLACE_DELAY);
    sim.set_disk_config(cfg.clone());
    for &id in &NODES {
        sim.set_disk_config_for(nid(id), cfg.clone());
    }

    let mut worst = Duration::ZERO;
    let mut worst_at = 0usize;
    for i in 0..WRITES {
        let key = format!("k{i:04}").into_bytes();
        let value = vec![b'v'; 64];
        // Put via the current leader (retry across elections).
        let mut waited = Duration::ZERO;
        loop {
            if let Some(l) = leader(&nodes)
                && matches!(
                    nodes[l].put(key.clone(), value.clone()),
                    ProposeResult::Accepted { .. }
                )
            {
                break;
            }
            sim.run_for(STEP);
            waited += STEP;
            assert!(
                waited < Duration::from_secs(30),
                "no leader to take write {i} (seed={seed})"
            );
        }
        // Confirm by reading it back through the leader's read barrier.
        let slot: Arc<Mutex<Option<bool>>> = Arc::new(Mutex::new(None));
        let mut in_flight = false;
        loop {
            if !in_flight && let Some(l) = leader(&nodes) {
                let n = nodes[l].clone();
                let (s, k, v) = (Arc::clone(&slot), key.clone(), value.clone());
                nodes[l].env().clone().spawn_task(async move {
                    let got = n.linearizable_get(&k).await;
                    *s.lock().unwrap() = Some(got.as_deref() == Some(v.as_slice()));
                });
                in_flight = true;
            }
            sim.run_for(STEP);
            waited += STEP;
            match slot.lock().unwrap().take() {
                Some(true) => break,
                Some(false) => in_flight = false,
                None => {}
            }
            assert!(
                waited < Duration::from_secs(30),
                "write {i} never confirmed (seed={seed})"
            );
        }
        if waited > worst {
            worst = waited;
            worst_at = i;
        }
    }

    let compactions: u64 = handles
        .iter()
        .map(|h| h.get(Metric::CpSnapshotTriggers))
        .sum();
    assert!(
        compactions >= 3,
        "premise: compaction must fire repeatedly, saw {compactions} (seed={seed})"
    );
    assert!(
        worst < CONFIRM_LIMIT,
        "worst confirm {worst:?} at write {worst_at} (limit {CONFIRM_LIMIT:?}, \
         replace delay {REPLACE_DELAY:?}) — the WAL rewrite's slow fsync is stalling \
         persist rounds (seed={seed})"
    );
}
