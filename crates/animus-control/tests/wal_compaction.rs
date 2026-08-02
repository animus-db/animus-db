//! WAL compaction + log truncation: taking a snapshot discards the log prefix
//! it covers, so the WAL is bounded by the live tail (one snapshot + the entries
//! after it), and a node still recovers exactly.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::{MetaCommand, NodeStatus, PersistedState, RaftCore, RaftNode, WalRecord};
use animus_env::{Disk, EnvExt, Nanos};
use animus_sim::{SimEnv, Simulator};

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
fn snapshot_truncates_the_log_prefix_and_recovers() {
    // Single-node leader applies everything it commits, so after a snapshot the
    // whole log prefix is covered and discarded.
    let mut core = RaftCore::new(0, &[0], Nanos(0), 7);
    core.tick(Nanos(1_000_000_000), 7); // election timeout -> sole leader
    for i in 0..50 {
        core.propose(upsert(i));
    }
    let before = core.log_len();
    assert!(
        before >= 50,
        "log holds the proposed entries (got {before})"
    );

    core.snapshot(); // advance the snapshot base to last_applied, drop the prefix
    assert!(
        core.log_len() < before,
        "log prefix truncated: {before} -> {}",
        core.log_len()
    );
    assert!(
        core.snapshot_index() >= 50,
        "snapshot base advanced past the entries"
    );

    // The WAL image is bounded by the (now-small) tail plus the single snapshot.
    let image = core.wal_image();
    assert_eq!(snapshot_count(&image), 1);
    assert!(
        image.len() <= core.log_len() + 2,
        "image bounded by tail + snapshot"
    );

    // Recovering from that image restores the applied state directly from the
    // snapshot (no log prefix to replay).
    let state = PersistedState::replay(image);
    let recovered = RaftCore::recovered(0, &[0], state, Nanos(0), 7);
    assert_eq!(
        recovered.metadata(),
        core.metadata(),
        "snapshot recovered the state"
    );
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
fn driver_truncates_the_wal_and_still_recovers() {
    let seed = 0x_C0FFEE;
    let mut sim = Simulator::new(seed);
    let node = RaftNode::start(sim.env(0), vec![0]); // single-node group
    sim.run_for(Duration::from_secs(1)); // elect

    for i in 0..80 {
        node.propose(upsert(i));
    }
    sim.run_for(Duration::from_secs(3)); // apply + flush + threshold snapshot

    let bytes = read_wal(&mut sim, node.env());
    let records = PersistedState::decode(&bytes);

    // A threshold snapshot ran: the WAL carries a snapshot covering a truncated
    // prefix, and is bounded far below one-record-per-operation.
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

    // Recovering and driving the node re-applies the (small) tail, reaching the
    // node's exact state — committed commands applied exactly once.
    let state = PersistedState::replay(records);
    let mut recovered = RaftCore::recovered(0, &[0], state, Nanos(0), 7);
    recovered.tick(Nanos(10_000_000_000), 7);
    recovered.propose(MetaCommand::NoOp);
    assert_eq!(
        recovered.metadata(),
        node.metadata(),
        "recovered state diverged"
    );
}
