//! Issue #1116: crash safety of the staged (outside-`wal_lock`) WAL rewrite.
//!
//! The compaction rewrite now stages its replacement WAL (`stage_replace`),
//! catches it up with the rounds persisted meanwhile (`stage_extend`) and swaps
//! it in (`commit_staged`) — all but the swap outside `wal_lock`. This corpus
//! kills the **whole cluster at once** at a seed-chosen offset into a stalled
//! rewrite (so every offset lands in a different phase: before the staged
//! write is durable, between the catch-up appends, mid-swap, after it), then
//! restarts every node from nothing but its disk. Whole-cluster loss matters:
//! with a surviving peer holding the data, a WAL that lost an acked record
//! would be silently repaired by replication; here every replica's own WAL +
//! engine must independently still hold every write the cluster acked.
//!
//! "Acked" is observed, not assumed: a key counts once the then-leader's
//! engine returned it (committed + applied, i.e. a durable quorum). After the
//! restart every such key must read back through a linearizable read. The
//! variants also arm the torn/corrupted-tail crash model (the same WAL fault
//! pairing `ANIMUS_RAFTKV_WAL_FAULTS` uses) on top of the slow-rewrite disk.
//!
//! Depth: `ANIMUS_WAL_REWRITE_CRASH_SEEDS=K` (default 1) multiplies the
//! offset sweep by K distinct seeds.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{Metric, MetricsHandle, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::LsmEngine;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
type LsmNode = RaftKvNode<SimEnv, LsmEngine<SimEnv>>;

/// Slow enough that the staged write spans many writer steps.
const REPLACE_DELAY: Duration = Duration::from_millis(400);
const STEP: Duration = Duration::from_millis(10);
/// Offsets (ms past the first observed rewrite start) swept per seed:
/// 0 is "just after the image was captured", 400+ is "swap territory".
const OFFSETS_MS: [u64; 12] = [0, 20, 60, 120, 200, 300, 380, 400, 410, 420, 450, 600];

fn depth() -> u64 {
    std::env::var("ANIMUS_WAL_REWRITE_CRASH_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}

fn leader(nodes: &[LsmNode]) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    (ls.len() == 1).then(|| ls[0])
}

fn start_node(sim: &Simulator, id: u64, prefix: &str, metrics: MetricsHandle) -> LsmNode {
    let env = sim.env(nid(id));
    let engine = block_on(LsmEngine::open(env.clone(), prefix.to_owned()))
        .expect("open LsmEngine on the sim disk");
    LsmNode::start_with_metrics(
        env,
        NODES.iter().copied().map(nid).collect(),
        engine,
        metrics,
    )
}

fn run_cell(seed: u64, offset: Duration, wal_faults: bool) {
    let label = format!("seed={seed} offset={offset:?} wal_faults={wal_faults}");
    let mut sim = Simulator::new(seed);
    let prefixes: Vec<String> = NODES.iter().map(|i| format!("db-rwcrash-n{i}-")).collect();
    let handles: Vec<MetricsHandle> = NODES.iter().map(|_| MetricsHandle::recording()).collect();
    let nodes: Vec<LsmNode> = NODES
        .iter()
        .enumerate()
        .map(|(i, &id)| start_node(&sim, id, &prefixes[i], handles[i].clone()))
        .collect();

    sim.run_for(Duration::from_secs(3));
    assert!(leader(&nodes).is_some(), "no leader ({label})");

    let mut slow = DiskConfig::default();
    slow.set_replace_data_delay(REPLACE_DELAY);
    // Non-zero fsync latency so rounds land *between* the staged write, the
    // catch-up appends and the swap instead of all collapsing to one instant.
    slow.set_sync_delay(Duration::from_millis(8));
    sim.set_disk_config(slow.clone());
    for &id in &NODES {
        sim.set_disk_config_for(nid(id), slow.clone());
    }

    // key -> expected value, for every put the leader accepted; `acked` is the
    // subset the then-leader's engine has been seen to hold.
    let mut proposed: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut acked: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut i = 0u32;
    let triggers = |hs: &[MetricsHandle]| -> u64 {
        hs.iter().map(|h| h.get(Metric::CpSnapshotTriggers)).sum()
    };
    let mut step = |sim: &mut Simulator, nodes: &[LsmNode], i: &mut u32| {
        if let Some(l) = leader(nodes) {
            let key = format!("k{i:05}").into_bytes();
            let val = format!("v{i:05}-{seed}").into_bytes();
            if matches!(
                nodes[l].put(key.clone(), val.clone()),
                ProposeResult::Accepted { .. }
            ) {
                proposed.insert(key, val);
                *i += 1;
            }
        }
        sim.run_for(STEP);
        if let Some(l) = leader(nodes) {
            for (k, v) in &proposed {
                if !acked.contains_key(k) && block_on(nodes[l].local_get(k)).as_deref() == Some(v) {
                    acked.insert(k.clone(), v.clone());
                }
            }
        }
    };

    // Write until the first compaction rewrite begins (its `CpSnapshotTriggers`
    // increment happens when the base advances, i.e. as the image is captured).
    let mut guard = 0;
    while triggers(&handles) == 0 {
        step(&mut sim, &nodes, &mut i);
        guard += 1;
        assert!(guard < 5_000, "compaction never started ({label})");
    }
    // Keep the writer going through the chosen offset into the stalled rewrite.
    let mut waited = Duration::ZERO;
    while waited < offset {
        step(&mut sim, &nodes, &mut i);
        waited += STEP;
    }
    assert!(
        !acked.is_empty(),
        "nothing acked before the crash ({label})"
    );

    // Power-cut the whole cluster at once, optionally tearing/corrupting each
    // WAL's un-synced tail, then restart every node from its disk alone.
    let mut crash_cfg = slow;
    if wal_faults {
        crash_cfg.torn_tail_on_crash = true;
        crash_cfg.corrupt_on_crash = true;
    }
    for &id in &NODES {
        sim.set_disk_config_for(nid(id), crash_cfg.clone());
        sim.crash(nid(id));
    }
    for &id in &NODES {
        sim.stop(nid(id));
    }
    drop(nodes);
    // Healthy disks for the recovery (the fault under test is the crash).
    for &id in &NODES {
        sim.set_disk_config_for(nid(id), DiskConfig::default());
        sim.restart(nid(id));
    }
    let nodes: Vec<LsmNode> = NODES
        .iter()
        .enumerate()
        .map(|(n, &id)| start_node(&sim, id, &prefixes[n], MetricsHandle::recording()))
        .collect();

    sim.run_for(Duration::from_secs(8));
    let l = leader(&nodes).unwrap_or_else(|| panic!("no leader after restart ({label})"));
    for (k, v) in &acked {
        let got = read_linearizable(&mut sim, &nodes[l], k);
        assert_eq!(
            got.as_deref(),
            Some(v.as_slice()),
            "acked key {} lost after a whole-cluster crash {offset:?} into a WAL rewrite ({label})",
            String::from_utf8_lossy(k)
        );
    }
}

fn read_linearizable(sim: &mut Simulator, node: &LsmNode, key: &[u8]) -> Option<Vec<u8>> {
    use animus_env::EnvExt;
    use std::sync::{Arc, Mutex};
    let slot: Arc<Mutex<Option<Option<Vec<u8>>>>> = Arc::new(Mutex::new(None));
    let (n, s, k) = (node.clone(), Arc::clone(&slot), key.to_vec());
    node.env().clone().spawn_task(async move {
        let v = n.linearizable_get(&k).await;
        *s.lock().unwrap() = Some(v);
    });
    for _ in 0..500 {
        sim.run_for(Duration::from_millis(10));
        if let Some(v) = slot.lock().unwrap().take() {
            return v;
        }
    }
    panic!(
        "linearizable read of {} never returned",
        String::from_utf8_lossy(key)
    );
}

#[test]
fn a_whole_cluster_crash_at_any_point_of_a_staged_wal_rewrite_loses_no_acked_write() {
    let base: u64 = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x1116);
    for k in 0..depth() {
        for (n, &ms) in OFFSETS_MS.iter().enumerate() {
            let seed = base + k * 1000 + n as u64;
            run_cell(seed, Duration::from_millis(ms), false);
        }
    }
}

#[test]
fn the_same_crash_sweep_with_torn_and_corrupted_wal_tails() {
    let base: u64 = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x1116_0001);
    for k in 0..depth() {
        for (n, &ms) in OFFSETS_MS.iter().enumerate() {
            let seed = base + k * 1000 + n as u64;
            run_cell(seed, Duration::from_millis(ms), true);
        }
    }
}
