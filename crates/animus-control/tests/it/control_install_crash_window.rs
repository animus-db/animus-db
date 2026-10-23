//! The control-plane twin of issue #1368 (found by a real-process soak: a
//! restarted node halted with "recovered log is not contiguous with its
//! snapshot base 0: the entry at position 63 carries index 65").
//!
//! A control-plane follower that catches up by `InstallSnapshot` moves its log
//! base (here 64, `SNAPSHOT_THRESHOLD`) in memory but, unlike the data plane,
//! deliberately does not write a `Snapshot` record: its WAL must never run
//! ahead of its engine. The base reached the WAL only with the apply task's
//! rewrite, which was gated on `behind >= SNAPSHOT_THRESHOLD`; meanwhile the
//! consensus loop appended the leader's next entries (index 65..) after the
//! OLD tail (1..63). Replayed positionally that is a gap.
//!
//! Real `RaftNode`s under the `Simulator`: a follower is isolated while the
//! leader commits and compacts past it, healed, and stopped (a crash) at a
//! seeded number of milliseconds after its install, while the leader keeps
//! proposing. The WAL it left must replay contiguously, and a restarted node
//! must not halt and must converge. Replay with
//! `ANIMUS_SEED=<seed> cargo test -p animus-control --test it control_install_crash_window::`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::persist::{PersistedState, WalRecord};
use animus_control::recover::CONTROL_WAL_FILE;
use animus_control::{MetaCommand, NodeStatus, RaftNode};
use animus_env::{Disk, EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];

fn seeds() -> Vec<u64> {
    if let Ok(s) = std::env::var("ANIMUS_SEED") {
        return vec![s.parse().expect("ANIMUS_SEED")];
    }
    (0..4).map(|i| 0x1368_c000 + i * 0x9E37).collect()
}

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(node),
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

fn start(sim: &Simulator, id: u64, engine: &MemoryEngine) -> RaftNode<SimEnv> {
    RaftNode::start(
        sim.env(nid(id)),
        NODES.iter().copied().map(nid).collect(),
        engine.clone(),
    )
}

fn read_wal(sim: &mut Simulator, id: u64) -> Vec<u8> {
    let env = sim.env(nid(id));
    let slot = Arc::new(Mutex::new(None));
    let slot2 = Arc::clone(&slot);
    let env2 = env.clone();
    env.spawn_task(async move {
        *slot2.lock().unwrap() = Some(env2.read(CONTROL_WAL_FILE).await.unwrap_or_default());
    });
    sim.run_for(Duration::from_millis(50));
    slot.lock().unwrap().take().expect("wal read finished")
}

fn run_one(seed: u64, crash_after_install_ms: u64) {
    let mut sim = Simulator::new(seed);
    let engines: Vec<MemoryEngine> = NODES.iter().map(|_| MemoryEngine::new()).collect();
    let mut nodes: Vec<RaftNode<SimEnv>> = NODES
        .iter()
        .map(|&i| start(&sim, i, &engines[i as usize]))
        .collect();
    sim.run_for(Duration::from_secs(2));
    let ls: Vec<usize> = (0..3).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "one leader expected (seed={seed})");
    let leader = ls[0];
    let f = (0..3).find(|&i| i != leader).unwrap();
    let f_id = nid(f as u64);

    for p in NODES {
        if nid(p) != f_id {
            sim.partition_pair(f_id.clone(), nid(p));
        }
    }
    let mut next = 100u64;
    for _ in 0..150 {
        nodes[leader].propose(upsert(next));
        next += 1;
    }
    sim.run_for(Duration::from_secs(4));
    assert!(
        nodes[leader].snapshot_index() >= 60 && nodes[f].snapshot_index() == 0,
        "setup: leader must have compacted past the isolated follower (seed={seed})"
    );
    for p in NODES {
        if nid(p) != f_id {
            sim.heal(f_id.clone(), nid(p));
        }
    }

    // Until the follower has installed the snapshot, then keep the leader
    // proposing so appends past the new base reach the follower's WAL.
    let mut waited = 0;
    while nodes[f].snapshot_index() == 0 {
        sim.run_for(Duration::from_millis(1));
        waited += 1;
        assert!(waited < 20_000, "follower never installed (seed={seed})");
    }
    for _ in 0..crash_after_install_ms {
        for _ in 0..3 {
            nodes[leader].propose(upsert(next));
            next += 1;
        }
        sim.run_for(Duration::from_millis(1));
    }
    let base = nodes[f].snapshot_index();
    sim.stop(f_id.clone());

    let bytes = read_wal(&mut sim, f as u64);
    let records: Vec<WalRecord> = PersistedState::decode(&bytes).expect("wal decodes");
    let state = PersistedState::replay(records);
    if let Err(e) = state.check_log_contiguous() {
        panic!(
            "crash {crash_after_install_ms} ms after the install at base {base}: WAL replays \
             non-contiguously: {e} (seed={seed})"
        );
    }

    // The restarted node must recover (not halt) and converge.
    nodes[f] = start(&sim, f as u64, &engines[f]);
    let mut converged = false;
    for _ in 0..1200 {
        sim.run_for(Duration::from_millis(25));
        assert!(!nodes[f].is_halted(), "restarted node halted (seed={seed})");
        if nodes[f].metadata() == nodes[leader].metadata() {
            converged = true;
            break;
        }
    }
    assert!(
        converged,
        "restarted follower never converged (seed={seed})"
    );
}

#[test]
fn a_crash_after_a_control_plane_install_replays_a_contiguous_wal() {
    for seed in seeds() {
        for ms in [0u64, 1, 2, 5, 20, 60] {
            run_one(seed, ms);
        }
    }
}
