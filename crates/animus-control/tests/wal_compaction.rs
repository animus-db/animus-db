//! WAL compaction + log truncation, engine-backed (ADR 0038 PR3): the apply
//! task's `meta_apply_and_compact` snapshots the Raft log once the
//! system-keyspace engine has durably merged enough past the current base,
//! truncating the covered log prefix so the WAL stays bounded by the live
//! tail — and a crash at any point recovers exactly (the engine's own
//! `_applied_index` watermark, not a whole-`Metadata` WAL blob, is the source
//! of truth `docs/adr/0038-control-metadata-system-keyspace.md` documents).
//!
//! The precise, deterministic unit-level proof that the apply task replays
//! **only the log tail beyond the engine's watermark** — never re-deriving
//! writes for a command the engine already durably reflects — lives in
//! `src/node.rs`'s own `#[cfg(test)]` module (it drives the private
//! `meta_apply_and_compact` directly); this file is the end-to-end,
//! `RaftNode`-level integration counterpart.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::{MetaCommand, NodeStatus, PersistedState, RaftNode, WalRecord, mirror};
use animus_env::{Disk, EnvExt};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node,
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

/// Read a node's on-disk WAL bytes via its env.
fn read_wal(sim: &mut Simulator, env: &SimEnv) -> Vec<u8> {
    use std::sync::{Arc, Mutex};
    let out = Arc::new(Mutex::new(Vec::new()));
    let (env, slot) = (env.clone(), Arc::clone(&out));
    env.clone().spawn_task(async move {
        *slot.lock().unwrap() = env.read("raft.wal").await.unwrap();
    });
    sim.run_for(Duration::from_millis(1));
    out.lock().unwrap().clone()
}

#[tokio::test]
async fn driver_truncates_the_wal_and_the_engine_stays_the_source_of_truth() {
    let seed = 0x_C0FFEE;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let node = RaftNode::start(sim.env(0), vec![0], engine.clone()); // single-node group
    sim.run_for(Duration::from_secs(1)); // elect

    for i in 0..80 {
        node.propose(upsert(i));
    }
    sim.run_for(Duration::from_secs(3)); // apply + flush + threshold snapshot

    let bytes = read_wal(&mut sim, node.env());
    let records: Vec<WalRecord> = PersistedState::decode(&bytes);

    // A threshold snapshot ran: the WAL carries a snapshot record covering a
    // truncated prefix, and is bounded far below one-record-per-operation.
    let snapshot_at = records.iter().find_map(|r| match r {
        WalRecord::Snapshot { last_index, .. } => Some(*last_index),
        _ => None,
    });
    assert!(
        snapshot_at.is_some_and(|li| li >= 60),
        "expected a snapshot covering a large prefix, got {snapshot_at:?}"
    );
    assert!(
        records.len() < 80,
        "WAL not bounded by truncation: {} records",
        records.len()
    );

    // The engine — not the WAL's (trivial, `DRIVER_APPLIED`) snapshot record —
    // is the durable source of truth: rebuilding straight from it agrees with
    // the live node's published cache.
    let rebuilt = mirror::rebuild_metadata_from_engine(&engine)
        .await
        .expect("engine scan");
    assert_eq!(
        rebuilt,
        node.metadata(),
        "engine rebuild diverged from the live cache"
    );
    assert_eq!(rebuilt.members.len(), 80);
}

/// A crash at some point during sustained compaction-triggering load recovers
/// to the same final state a same-seed uninterrupted run reaches — the engine
/// (not the WAL's own now-trivial `DRIVER_APPLIED` snapshot blob) is what
/// makes this possible: recovery rebuilds the cache from the engine's
/// `_applied_index` watermark and replays only the surviving log tail on top
/// (proven precisely, at the unit level, in `src/node.rs`'s own tests). This
/// end-to-end version doesn't pin the crash to the exact engine-scan instant
/// (no fault-injection hook exists at that granularity for `MemoryEngine`),
/// but it does land squarely inside the compaction-triggering window (well
/// past `SNAPSHOT_THRESHOLD`), so a crash here routinely interrupts an
/// in-flight or just-triggered compaction/image-build pass.
#[test]
fn crash_during_sustained_compaction_recovers_to_the_uninterrupted_reference_state() {
    let seed = 0x5EED_CA5E;

    // Reference: an uninterrupted run at the same seed.
    let reference = {
        let mut sim = Simulator::new(seed);
        let node = RaftNode::start(sim.env(0), vec![0], MemoryEngine::new());
        sim.run_for(Duration::from_secs(1));
        for i in 0..200u64 {
            node.propose(upsert(i));
        }
        sim.run_for(Duration::from_secs(4));
        node.metadata()
    };

    // Crashed-and-recovered: kill the node mid-way through the same load,
    // deep inside the window where compaction is actively triggering, then
    // restart on the same disk *and* the same (durable) engine.
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let node = RaftNode::start(sim.env(0), vec![0], engine.clone());
    sim.run_for(Duration::from_secs(1));
    for i in 0..200u64 {
        node.propose(upsert(i));
    }
    // Advance just long enough for compaction/snapshot-image activity to be
    // underway, then crash with no graceful flush.
    sim.run_for(Duration::from_millis(750));
    sim.stop(0);

    let node = RaftNode::start(sim.env(0), vec![0], engine);
    sim.run_for(Duration::from_secs(4));

    assert_eq!(
        node.metadata(),
        reference,
        "seed={seed}: crash-during-compaction recovery diverged from the uninterrupted reference"
    );
}
