//! ADR 0079 step 6: the repair-settled report (`MetaCommand::RepairSettled`,
//! `Gate::NodeCapacity`) under `SimCluster` (seed-reproducible; replay one
//! seed with `ANIMUS_SEED=<seed>`).
//!
//! A tablet is moved off node 2 onto the data-only node 3 by a
//! `CasTabletReplicas` (the shape a repair has). Covered:
//! - **gate closed: nothing is reported** even once the group has converged
//!   (an older voter cannot decode the variant), on every node;
//! - **gate open: the tablet's leader reports** once its group is the new
//!   voter set with no learner, and `Tablet::settled_epoch` lands on every
//!   replica of `Metadata` (the control leader's pacer reads it there);
//! - a report proposed from a **control follower** is relayed to the leader
//!   and applies (the `is_relayable_command` arm; without it the report is
//!   rejected, a bimodal per-process flake the compiler cannot catch);
//! - a **stale-epoch** report is rejected and a duplicate is a no-op.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::MetaCommand;
use animus_control::version::VersionRange;
use animus_env::nid;
use animus_tablet::Epoch;

use super::sim_cluster::SimCluster;
use crate::config::NodeRole;
use crate::repair_settled::repair_settled_tick;

const ROLES: [NodeRole; 4] = [
    NodeRole::Both,
    NodeRole::Both,
    NodeRole::Both,
    NodeRole::Data,
];

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return vec![s];
    }
    vec![0x79A0_0000]
}

fn poll(
    cluster: &mut SimCluster,
    seed: u64,
    what: &str,
    mut cond: impl FnMut(&mut SimCluster) -> bool,
) {
    let mut elapsed = Duration::ZERO;
    while !cond(cluster) {
        assert!(
            elapsed < Duration::from_secs(90),
            "seed={seed}: {what} never converged"
        );
        cluster.run_for(Duration::from_millis(100));
        elapsed += Duration::from_millis(100);
    }
}

/// One tick of every node's report loop.
fn tick_all(cluster: &mut SimCluster) {
    for n in 0..ROLES.len() as u64 {
        let _ = cluster.run_with_ctx(n, |ctx| async move {
            let mut proposed = BTreeMap::new();
            repair_settled_tick(&ctx, &mut proposed).await;
        });
    }
}

fn run(seed: u64) {
    let mut cluster = SimCluster::new_with_roles(seed, &ROLES, 3);
    let _ = cluster.control_leader_index();
    cluster.set_all_node_versions(Some(VersionRange::new(1, 5)));
    poll(&mut cluster, seed, "the era", |c| {
        (0..ROLES.len() as u64).all(|n| c.features(n).era_active())
            && (0..ROLES.len() as u64).all(|n| c.metadata(n).node_versions.len() == ROLES.len())
    });
    let tablet = cluster.create_table_on("t", vec![0, 1, 2]);
    // The repair-shaped placement change: node 2's replica moves to node 3.
    let epoch0 = cluster.metadata(0).tablets[&tablet].epoch;
    let r = cluster.propose_meta(MetaCommand::CasTabletReplicas {
        tablet,
        expected_epoch: epoch0,
        replicas: vec![nid(0), nid(1), nid(3)],
    });
    assert!(
        matches!(r, animus_control::raft::ProposeResult::Accepted { .. }),
        "seed={seed}"
    );
    let epoch = epoch0.next();
    poll(
        &mut cluster,
        seed,
        "the group to converge on the new voter set",
        |c| c.metadata(0).tablets[&tablet].epoch == epoch && c.hosted_tablets(3).contains(&tablet),
    );
    // Gate closed (era active, cluster version 1): the loop reports nothing.
    for _ in 0..40 {
        tick_all(&mut cluster);
        cluster.run_for(Duration::from_millis(250));
    }
    for n in 0..ROLES.len() as u64 {
        assert_eq!(
            cluster.metadata(n).tablets[&tablet].settled_epoch,
            None,
            "seed={seed}: node {n} saw a report through a closed gate"
        );
    }
    // Open the gate (finalize 1 -> 5, one step at a time).
    for target in 2..=5u32 {
        let leader = {
            let i = cluster.control_leader_index();
            cluster.control_node_id(i)
        };
        let body = format!(r#"{{"to":{target},"expected":{}}}"#, target - 1);
        let (status, resp) = cluster.admin(
            leader,
            "POST",
            "/admin/cluster-version/finalize",
            "",
            body.as_bytes(),
        );
        assert_eq!(status, 200, "seed={seed}: finalize to {target}: {resp}");
        poll(&mut cluster, seed, "the finalize to be observed", |c| {
            c.metadata(0).cluster_version() == target
        });
    }
    poll(
        &mut cluster,
        seed,
        "the tablet leader's report to land",
        |c| {
            tick_all(c);
            (0..ROLES.len() as u64)
                .all(|n| c.metadata(n).tablets[&tablet].settled_epoch == Some(epoch))
        },
    );
    // Relay regression: a report from a control follower reaches the leader
    // (a duplicate of the applied one is a no-op, a stale one is rejected).
    let leader = {
        let i = cluster.control_leader_index();
        cluster.control_node_id(i)
    };
    let follower = (0..3u64).find(|n| *n != leader).expect("a follower");
    let ok = cluster.run_with_ctx(follower, move |ctx| async move {
        ctx.propose_schema(&MetaCommand::RepairSettled { tablet, epoch })
            .await
    });
    assert!(
        ok.is_some(),
        "seed={seed}: the follower-connected report did not resolve"
    );
    let stale = cluster.run_with_ctx(follower, move |ctx| async move {
        ctx.propose_schema(&MetaCommand::RepairSettled {
            tablet,
            epoch: Epoch(epoch.0 - 1),
        })
        .await
    });
    assert!(stale.is_some(), "seed={seed}");
    cluster.run_for(Duration::from_secs(2));
    for n in 0..ROLES.len() as u64 {
        assert_eq!(
            cluster.metadata(n).tablets[&tablet].settled_epoch,
            Some(epoch),
            "seed={seed}"
        );
    }
}

#[test]
fn the_tablet_leader_reports_a_converged_repair_only_once_the_gate_is_open() {
    for seed in seeds() {
        run(seed);
    }
}
