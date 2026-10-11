//! Issue #1235 regression: on `SimCluster`'s `Memory` backend a restarted
//! control node must keep its system-keyspace mirror engine (a real restart
//! reopens the same disk), so a control log compacted past
//! `SNAPSHOT_THRESHOLD` still restarts into the leader's FULL `Metadata`,
//! never a partial one at a matching applied index. (The control node refuses
//! to start on the deliberate-loss shape instead; see the `animus-control`
//! `restart_retained_syskv_engine` tests, issue #1194.)

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::{MetaCommand, NodeStatus};
use animus_env::nid;

use super::sim_cluster::SimCluster;
use crate::config::NodeRole;

fn run(seed: u64) {
    let mut cluster = SimCluster::new_with_roles(seed, &[NodeRole::Both; 3], 3);
    cluster.run_for(Duration::from_secs(3));
    let leader = cluster.control_leader_index() as u64;
    let victim = (0..3u64).find(|&n| n != leader).unwrap();

    // Several SNAPSHOT_THRESHOLD (64) crossings.
    const MEMBERS: u64 = 300;
    for i in 0..MEMBERS {
        cluster.propose_meta(MetaCommand::UpsertMember {
            node: nid(1000 + i),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        });
        if i % 20 == 19 {
            cluster.run_for(Duration::from_millis(300));
        }
    }
    cluster.run_for(Duration::from_secs(3));
    let want = cluster.metadata(leader).members.len();
    assert!(want >= MEMBERS as usize, "seed={seed}: leader has {want}");

    cluster.restart(victim);
    cluster.run_for(Duration::from_secs(8));

    let got = cluster.metadata(victim).members.len();
    let lead_now = cluster.control_leader_index() as u64;
    let want = cluster.metadata(lead_now).members.len();
    assert_eq!(
        got, want,
        "seed={seed}: restarted node {victim} serves partial Metadata ({got} of {want} members)"
    );
}

#[test]
fn memory_backend_control_restart_after_compaction_keeps_full_metadata() {
    for seed in [8140559029270420170, 0x1235, 0x1236] {
        run(seed);
    }
}
