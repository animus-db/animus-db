//! Issue #899 / PR #903 under real faults: a follower that is stopped and
//! restarted partway through a multi-chunk `InstallSnapshot` loses its volatile
//! reassembly buffer and honestly reports `next_offset: 0` on its next ack. The
//! leader's monotonic `max` guard on the per-peer `SnapshotSend::acked_offset` used to
//! swallow that reset, so it kept sending chunks at the stale high offset and
//! the follower never converged.
//!
//! `install_snapshot.rs::leader_resumes_from_offset_zero_after_a_restarted_
//! follower_resets` pins the message-level rule with hand-built messages. This
//! file drives real `RaftNode`s under the `Simulator`: a lagging follower is
//! sent a snapshot that spans many (deliberately small) chunks over a slow
//! link, and is stopped + restarted (fresh node, same durable engine/disk) at
//! seeded points mid-transfer, up to three times. Oracles: the follower was
//! genuinely mid-transfer at each stop (no snapshot installed yet, chunk
//! messages observed on the wire), the leader re-ships a full transfer after the
//! last restart, and the follower converges (converged-or-timeout poll) to
//! byte-identical metadata. Every run is a pure function of its seed; replay
//! with `ANIMUS_SEED=<seed> cargo test -p animus-control --test it
//! snapshot_follower_restart_midtransfer::`.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::{MetaCommand, NodeStatus, RaftNode};
use animus_env::{NodeId, nid};
use animus_sim::{NetConfig, SimEnv, Simulator, TraceEvent};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];
/// Small chunk so a modest state spans many chunks.
const CHUNK_BYTES: usize = 512;
/// Only `InstallSnapshot` chunks are this large on the leader->follower link
/// once the follower is partitioned away from `AppendEntries` traffic.
const BIG_MSG: usize = 400;
const MEMBERS: u64 = 150;

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn seeds() -> Vec<u64> {
    if let Ok(s) = std::env::var("ANIMUS_SEED") {
        return vec![s.parse().expect("ANIMUS_SEED")];
    }
    (0..6).map(|i| 0x899_0000 + i * 0x9E37).collect()
}

fn upsert(node: u64) -> MetaCommand {
    let mut labels = BTreeMap::new();
    // Fat labels: the snapshot image must span many CHUNK_BYTES chunks.
    labels.insert("pad".to_string(), "x".repeat(200));
    labels.insert("tag".to_string(), node.to_string());
    MetaCommand::UpsertMember {
        node: nid(node),
        labels,
        status: NodeStatus::Active,
    }
}

fn start(sim: &Simulator, id: u64, engine: &MemoryEngine) -> RaftNode<SimEnv> {
    let n = RaftNode::start(
        sim.env(nid(id)),
        NODES.iter().copied().map(nid).collect(),
        engine.clone(),
    );
    n.set_snapshot_chunk_bytes(CHUNK_BYTES);
    n
}

/// Chunk-sized messages the leader has delivered to `to` so far.
fn big_deliveries(sim: &Simulator, from: &NodeId, to: &NodeId) -> usize {
    sim.trace()
        .iter()
        .filter(|e| {
            matches!(e, TraceEvent::Deliver { from: f, to: t, len, .. }
                if f == from && t == to && *len >= BIG_MSG)
        })
        .count()
}

fn run_one(seed: u64) {
    let mut sim = Simulator::new(seed);
    let engines: Vec<MemoryEngine> = NODES.iter().map(|_| MemoryEngine::new()).collect();
    let mut nodes: Vec<RaftNode<SimEnv>> = NODES
        .iter()
        .map(|&i| start(&sim, i, &engines[i as usize]))
        .collect();
    sim.run_for(Duration::from_secs(2));
    let ls: Vec<usize> = (0..3).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    let leader = ls[0];
    let f = (0..3).find(|&i| i != leader).unwrap();
    let (leader_id, f_id) = (nid(leader as u64), nid(f as u64));
    for n in &nodes {
        n.set_snapshot_chunk_bytes(CHUNK_BYTES);
    }

    // Isolate the follower while the leader commits and compacts past it.
    for p in NODES {
        if nid(p) != f_id {
            sim.partition_pair(f_id.clone(), nid(p));
        }
    }
    for i in 0..MEMBERS {
        nodes[leader].propose(upsert(100 + i));
    }
    sim.run_for(Duration::from_secs(4));
    assert!(
        nodes[leader].snapshot_index() >= 60 && nodes[f].snapshot_index() == 0,
        "setup: leader must have compacted past the isolated follower (seed={seed})"
    );

    // Slow leader<->follower link so the transfer spans many virtual ms and
    // the seeded stop lands mid-flight.
    let mut slow = NetConfig::default();
    slow.base_delay = Duration::from_millis(8);
    slow.max_jitter = Duration::from_millis(4);
    sim.set_link_net_config(leader_id.clone(), f_id.clone(), slow.clone());
    sim.set_link_net_config(f_id.clone(), leader_id.clone(), slow);
    for p in NODES {
        if nid(p) != f_id {
            sim.heal(f_id.clone(), nid(p));
        }
    }

    let restarts = 1 + (splitmix64(seed) % 3) as usize;
    let mut chunks_before_last_stop = 0usize;
    for r in 0..restarts {
        // Seeded point: stop after this many MORE chunk messages reached the
        // follower than at the previous restart (at least 2 so there is real
        // progress to lose, small enough to stay mid-transfer).
        let base = big_deliveries(&sim, &leader_id, &f_id);
        let k = 2 + (splitmix64(seed ^ ((r as u64 + 1) * 0x51)) % 12) as usize;
        let mut waited = 0;
        while big_deliveries(&sim, &leader_id, &f_id) < base + k {
            sim.run_for(Duration::from_millis(5));
            waited += 1;
            assert!(
                waited < 2000,
                "leader never shipped {k} chunks to the follower (restart {r}, seed={seed})"
            );
        }
        assert_eq!(
            nodes[f].snapshot_index(),
            0,
            "follower finished the snapshot before restart {r}: not mid-transfer (seed={seed})"
        );
        chunks_before_last_stop = big_deliveries(&sim, &leader_id, &f_id);
        sim.stop(f_id.clone());
        sim.run_for(Duration::from_millis(
            30 * (1 + (splitmix64(seed ^ r as u64) % 5)),
        ));
        nodes[f] = start(&sim, f as u64, &engines[f]);
    }

    // Converged-or-timeout: the restarted follower must resume from offset 0
    // and fully catch up.
    let mut converged = false;
    for _ in 0..1200 {
        sim.run_for(Duration::from_millis(25));
        if nodes[f].snapshot_index() > 0 && nodes[f].metadata() == nodes[leader].metadata() {
            converged = true;
            break;
        }
    }
    assert!(
        converged,
        "follower never converged after {restarts} mid-transfer restart(s): \
         follower snapshot_index={} last_applied={} members={}, leader snapshot_index={} \
         members={} (seed={seed}; issue #899 max-guard stall)",
        nodes[f].snapshot_index(),
        nodes[f].last_applied(),
        nodes[f].metadata().members.len(),
        nodes[leader].snapshot_index(),
        nodes[leader].metadata().members.len(),
    );
    assert!(
        nodes[f].metadata().members.len() >= MEMBERS as usize,
        "converged on the wrong state (seed={seed})"
    );
    // Resumed from offset 0: a complete re-ship follows the last restart, so
    // strictly more chunks crossed the wire than had before the stop.
    let after = big_deliveries(&sim, &leader_id, &f_id);
    assert!(
        after > chunks_before_last_stop,
        "no chunks shipped after the last restart ({after} <= {chunks_before_last_stop}) (seed={seed})"
    );
}

#[test]
fn follower_restarted_mid_chunked_snapshot_converges_across_seeds() {
    for seed in seeds() {
        run_one(seed);
    }
}
