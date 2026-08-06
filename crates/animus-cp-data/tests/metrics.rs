//! CP-plane observability counters move under a known workload (ADR 0015).
//!
//! `SimEnv::metrics()` is the no-op default, so this test threads a *recording*
//! [`MetricsHandle`] into each tablet-group node via [`RaftKvNode::start_with_metrics`]
//! (mirroring `animus-control/tests/metrics.rs`) and reads the counters back — no
//! change to `animus-sim` is required to observe them. Linearizable reads are async
//! (a read-barrier probe round), so they are driven as spawned tasks + `run_for`,
//! same as `read_index.rs`.
//!
//! It drives a handful of puts + a linearizable read through a 3-node group and
//! asserts the real outcome is what moved each counter (the repo rule: count the
//! outcome, never the attempt) — not just "it compiles":
//! - every accepted `put` bumps `cp_proposals_accepted` by exactly one;
//! - a `put` sent to a follower is rejected and bumps *that follower's*
//!   `cp_proposals_rejected_not_leader`, not the leader's;
//! - the log entries those puts commit bump `cp_commits`;
//! - the apply task draining and applying those commands bumps `cp_applies`, via at
//!   least one batched `merge_batch` run (`cp_apply_batch_runs` +
//!   `cp_apply_batch_size_sum`, so the average batch size is derivable);
//! - a `linearizable_get` on the leader bumps `cp_read_barriers_served`;
//! - a single-server `change_membership` bumps `cp_reconfigure_accepted`.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{EnvExt, Metric, MetricsHandle};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// Stand up a 3-node group, each node recording into its own handle (index-aligned
/// with the returned nodes), so a per-node counter (e.g. a follower's rejection) is
/// attributable to the right replica.
fn group(seed: u64) -> (Simulator, Vec<KvNode>, Vec<MetricsHandle>) {
    let sim = Simulator::new(seed);
    let handles: Vec<MetricsHandle> = NODES.iter().map(|_| MetricsHandle::recording()).collect();
    let nodes: Vec<KvNode> = NODES
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            RaftKvNode::start_with_metrics(
                sim.env(id),
                NODES.to_vec(),
                MemoryEngine::new(),
                handles[i].clone(),
            )
        })
        .collect();
    (sim, nodes, handles)
}

fn leader_index(nodes: &[KvNode], seed: u64) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    ls[0]
}

/// Run a linearizable read on `node` to completion (spawned as a task, since it
/// awaits a quorum probe round), driving the sim up to `budget`. Mirrors
/// `read_index.rs::lin_read` — a linearizable read cannot be driven by a bare
/// `block_on`, since its internal `env.sleep` polling only resolves while the
/// `Simulator` is advancing virtual time via `run_for`.
fn lin_read(sim: &mut Simulator, node: &KvNode, key: &[u8], budget: Duration) -> Option<Vec<u8>> {
    let slot: Arc<Mutex<Option<Option<Vec<u8>>>>> = Arc::new(Mutex::new(None));
    let n = node.clone();
    let s = Arc::clone(&slot);
    let k = key.to_vec();
    node.env().clone().spawn_task(async move {
        let v = n.linearizable_get(&k).await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock()
        .unwrap()
        .clone()
        .expect("linearizable read did not complete")
}

#[test]
fn cp_counters_move_under_a_known_workload() {
    run(0xC9_DA7A);
}

fn run(seed: u64) {
    let (mut sim, nodes, handles) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect

    let leader = leader_index(&nodes, seed);
    let follower = (0..nodes.len())
        .find(|&i| i != leader)
        .expect("a follower exists");

    // --- Proposals: accepted on the leader, rejected-not-leader on a follower. ---
    let before_accepted = handles[leader].get(Metric::CpProposalsAccepted);
    const PUTS: usize = 6;
    for i in 0..PUTS {
        let key = format!("k{i}").into_bytes();
        let value = format!("v{i}").into_bytes();
        match nodes[leader].put(key, value) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected a put: {other:?} (seed={seed})"),
        }
    }
    assert_eq!(
        handles[leader].get(Metric::CpProposalsAccepted) - before_accepted,
        PUTS as u64,
        "leader's accepted-proposal counter should move by exactly one per accepted put (seed={seed})"
    );

    match nodes[follower].put(b"nope".to_vec(), b"nope".to_vec()) {
        ProposeResult::NotLeader { .. } => {}
        other => panic!("follower unexpectedly accepted a put: {other:?} (seed={seed})"),
    }
    assert_eq!(
        handles[follower].get(Metric::CpProposalsRejectedNotLeader),
        1,
        "the follower's own not-leader-rejection counter should move (seed={seed})"
    );
    assert_eq!(
        handles[follower].get(Metric::CpProposalsAccepted),
        0,
        "a follower that never had a propose accepted should read zero accepted (seed={seed})"
    );

    sim.run_for(Duration::from_secs(2)); // replicate, commit, apply

    // --- Commits + applies: the PUTS committed/applied entries moved both. ---
    let commits = handles[leader].get(Metric::CpCommits);
    assert!(
        commits >= PUTS as u64,
        "cp_commits should have advanced by at least the {PUTS} puts (seed={seed}): {commits}"
    );
    let applies = handles[leader].get(Metric::CpApplies);
    assert!(
        applies >= PUTS as u64,
        "cp_applies should count at least the {PUTS} applied puts (seed={seed}): {applies}"
    );

    // --- Batching: at least one merge_batch run, sized so the average is sane. ---
    let batch_runs = handles[leader].get(Metric::CpApplyBatchRuns);
    let batch_size_sum = handles[leader].get(Metric::CpApplyBatchSizeSum);
    assert!(
        batch_runs >= 1,
        "at least one apply batch should have flushed (seed={seed})"
    );
    assert!(
        batch_size_sum >= PUTS as u64,
        "the batch-size sum should cover at least the {PUTS} puts (seed={seed}): {batch_size_sum}"
    );
    assert!(
        batch_size_sum >= batch_runs,
        "average batch size (sum/runs) must be >= 1 (seed={seed})"
    );

    // Every replica actually applied every put (sanity: the counters above are not
    // just moving without correct effect).
    for (i, n) in nodes.iter().enumerate() {
        for k in 0..PUTS {
            let key = format!("k{k}").into_bytes();
            let value = format!("v{k}").into_bytes();
            assert_eq!(
                block_on(n.local_get(&key)),
                Some(value),
                "node {i} missing k{k} (seed={seed})"
            );
        }
    }

    // --- Read barrier: a linearizable read on the leader is *served*. ---
    let served_before = handles[leader].get(Metric::CpReadBarriersServed);
    let got = lin_read(&mut sim, &nodes[leader], b"k0", Duration::from_secs(2));
    assert_eq!(got, Some(b"v0".to_vec()), "seed={seed}");
    assert_eq!(
        handles[leader].get(Metric::CpReadBarriersServed) - served_before,
        1,
        "a successful linearizable read should bump served-by-one (seed={seed})"
    );
    assert_eq!(
        handles[leader].get(Metric::CpReadBarriersTimedOut),
        0,
        "no read barrier should have failed in this run (seed={seed})"
    );

    // --- Reconfigure: a single-server membership change is accepted. ---
    let mut voters: BTreeSet<u64> = NODES.iter().copied().collect();
    voters.remove(&(follower as u64));
    match nodes[leader].change_membership(voters) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected the reconfigure: {other:?} (seed={seed})"),
    }
    assert_eq!(
        handles[leader].get(Metric::CpReconfigureAccepted),
        1,
        "the accepted single-server change should bump the leader's counter (seed={seed})"
    );
    assert_eq!(
        handles[leader].get(Metric::CpReconfigureRejected),
        0,
        "seed={seed}"
    );

    // A snapshot taken twice back-to-back (no new activity in between) is
    // byte-identical — the seam is a pure read (ADR 0015).
    let snap_a = handles[leader].snapshot();
    let snap_b = handles[leader].snapshot();
    assert_eq!(snap_a, snap_b, "seed={seed}");
}
