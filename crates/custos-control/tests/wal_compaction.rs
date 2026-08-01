//! WAL compaction: the on-disk log is bounded to the live state (latest
//! checkpoint + hard state + current log) instead of growing with every apply,
//! and a node still recovers exactly from the compacted WAL.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use custos_control::{MetaCommand, NodeStatus, PersistedState, RaftCore, RaftNode, WalRecord};
use custos_env::{Disk, EnvExt, Nanos};
use custos_sim::{SimEnv, Simulator};

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node,
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

fn snapshot_count(records: &[WalRecord]) -> usize {
    records
        .iter()
        .filter(|r| matches!(r, WalRecord::Snapshot { .. }))
        .count()
}

#[test]
fn wal_image_replays_to_the_same_state_with_one_checkpoint() {
    // Drive a single-node group through many applies, collecting every WAL
    // record it emits (the uncompacted history).
    let mut core = RaftCore::new(0, &[0], Nanos(0), 7);
    let mut uncompacted = core.drain_persist();
    core.tick(Nanos(1_000_000_000), 7); // election timeout -> sole leader
    uncompacted.extend(core.drain_persist());
    for i in 0..50 {
        core.propose(upsert(i));
        uncompacted.extend(core.drain_persist());
    }

    let image = core.wal_image();

    // The uncompacted history accrues a fresh checkpoint per apply; the image
    // keeps exactly one — that is the growth compaction removes.
    assert!(
        snapshot_count(&uncompacted) >= 50,
        "expected churn: {}",
        snapshot_count(&uncompacted)
    );
    assert_eq!(snapshot_count(&image), 1, "image keeps a single checkpoint");
    assert!(
        image.len() < uncompacted.len(),
        "image should be smaller than the history"
    );

    // Both replay to the same durable state.
    let from_history = PersistedState::replay(uncompacted);
    let from_image = PersistedState::replay(image);
    assert_eq!(from_history.term, from_image.term);
    assert_eq!(from_history.log, from_image.log);
    assert_eq!(from_history.snapshot, from_image.snapshot);
}

/// Read a node's on-disk WAL bytes via its env.
fn read_wal(sim: &mut Simulator, env: &SimEnv) -> Vec<u8> {
    let out = Arc::new(Mutex::new(Vec::new()));
    let (env, slot) = (env.clone(), Arc::clone(&out));
    env.clone().spawn_task(async move {
        *slot.lock().unwrap() = env.read("raft.wal").await.unwrap();
    });
    sim.run_for(Duration::from_millis(1));
    out.lock().unwrap().clone()
}

#[test]
fn driver_compacts_the_wal_and_still_recovers() {
    let seed = 0x_C0FFEE;
    let mut sim = Simulator::new(seed);
    let node = RaftNode::start(sim.env(0), vec![0]); // single-node group
    sim.run_for(Duration::from_secs(1)); // elect

    for i in 0..80 {
        node.propose(upsert(i));
    }
    sim.run_for(Duration::from_secs(3)); // apply + flush + compact

    let bytes = read_wal(&mut sim, node.env());
    let records = PersistedState::decode(&bytes);

    // Compaction ran: the WAL holds far fewer checkpoints than there were
    // applies (an uncompacted WAL would carry one per apply).
    let applies = node.commit_index();
    assert!(
        applies >= 80,
        "precondition: most proposals committed (got {applies})"
    );
    assert!(
        snapshot_count(&records) < applies as usize,
        "WAL not compacted: {} checkpoints for {applies} applies",
        snapshot_count(&records)
    );
    assert!(
        snapshot_count(&records) >= 1,
        "a compacted WAL still has its checkpoint"
    );

    // The compacted WAL recovers the node's exact metadata.
    let state = PersistedState::replay(records);
    let recovered = RaftCore::recovered(0, &[0], state, Nanos(0), 7);
    assert_eq!(
        recovered.metadata(),
        node.metadata(),
        "compacted WAL did not recover the state"
    );
}
