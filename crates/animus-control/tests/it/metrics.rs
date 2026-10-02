//! Control-plane observability counters move under known events (ADR 0015).
//!
//! The metrics seam ([`animus_env::metrics`]) is deterministic-safe: recording is
//! a relaxed atomic add, every timestamp the loops observe comes from the `Env`
//! clock, and a snapshot is a pure read into a `BTreeMap`. `SimEnv::metrics()` is
//! the no-op default, so this test threads a *recording* [`MetricsHandle`] into
//! each control node via [`RaftNode::start_with_metrics`] and reads the counters
//! back — no change to `animus-sim` is required to observe them.
//!
//! It asserts two classes of event drive their counters, and that the whole run
//! is a pure function of its seed:
//!
//! 1. **Elections.** Standing up a fresh cluster forces exactly one node to win
//!    an election: the winner's `elections_started` and `elections_won` counters
//!    are non-zero and its leadership gauge reads 1, while a follower's gauge
//!    reads 0. Steady-state replication keeps `append_entries_sent` climbing.
//! 2. **Failure-detector transitions (ADR 0012).** Crashing a heartbeating member
//!    drives an `Active`->`Down` verdict on the leader (its
//!    `failure_detector_down` counter increments); restarting it drives the
//!    `Down`->`Active` recovery (`failure_detector_up` increments).

use std::time::Duration;

use animus_control::node::heartbeat_loop;
use animus_control::{MetaCommand, NodeStatus, ProposeResult, RaftNode};
use animus_env::{EnvExt, Metric, MetricsHandle, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const CONTROL: [u64; 3] = [0, 1, 2];
const MEMBER: u64 = 10;

/// Stand up a 3-node control cluster, each node recording into its own handle,
/// plus one data member that heartbeats the control group. Returns the sim, the
/// nodes, and the per-node recording handles (index-aligned with `nodes`).
fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>, Vec<MetricsHandle>) {
    let sim = Simulator::new(seed);
    let handles: Vec<MetricsHandle> = CONTROL.iter().map(|_| MetricsHandle::recording()).collect();
    let nodes: Vec<RaftNode<SimEnv>> = CONTROL
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            RaftNode::start_with_metrics(
                sim.env(nid(id)),
                CONTROL.iter().copied().map(nid).collect(),
                handles[i].clone(),
                MemoryEngine::new(),
            )
        })
        .collect();
    // One data member heartbeats the whole control group on a timer, so the
    // leader's failure detector has someone to track.
    let env = sim.env(nid(MEMBER));
    env.spawn_task(heartbeat_loop(
        env.clone(),
        CONTROL.iter().copied().map(nid).collect(),
    ));
    (sim, nodes, handles)
}

fn leader_index(nodes: &[RaftNode<SimEnv>]) -> usize {
    let leaders: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(
        leaders.len(),
        1,
        "expected exactly one leader, got {leaders:?}"
    );
    leaders[0]
}

#[test]
fn election_and_failure_detector_counters_move() {
    run(0x0B5E_4ABE);
}

fn run(seed: u64) {
    let (mut sim, nodes, handles) = cluster(seed);

    // --- Elections: a fresh cluster forces exactly one election win. ---
    sim.run_for(Duration::from_secs(2));
    let leader = leader_index(&nodes);
    let lead_snap = handles[leader].snapshot();

    assert!(
        lead_snap.counters[&Metric::ElectionsStarted] >= 1,
        "leader should have started >=1 election (seed={seed}): {lead_snap:?}"
    );
    assert!(
        lead_snap.counters[&Metric::ElectionsWon] >= 1,
        "leader should have won >=1 election (seed={seed}): {lead_snap:?}"
    );
    assert_eq!(
        lead_snap.is_leader, 1,
        "leader's leadership gauge should read 1 (seed={seed})"
    );
    // A heartbeating, replicating leader keeps sending AppendEntries.
    assert!(
        lead_snap.counters[&Metric::AppendEntriesSent] >= 1,
        "leader should have sent >=1 AppendEntries (seed={seed}): {lead_snap:?}"
    );
    // A follower never won and reads 0 on the gauge.
    let follower = (0..nodes.len()).find(|&i| i != leader).unwrap();
    assert_eq!(
        handles[follower].snapshot().is_leader,
        0,
        "a follower's leadership gauge should read 0 (seed={seed})"
    );

    // Register the member Active so a later silence is an Active->Down edge.
    nodes[leader].propose(MetaCommand::UpsertMember {
        node: nid(MEMBER),
        labels: std::collections::BTreeMap::new(),
        status: NodeStatus::Active,
    });
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        nodes[leader].metadata().members[&nid(MEMBER)].status,
        NodeStatus::Active,
        "member should be Active before the crash (seed={seed})"
    );

    // --- Failure-detector down edge: crash the member; its heartbeats stop. ---
    let down_before = handles[leader].get(Metric::FailureDetectorDown);
    sim.crash(nid(MEMBER));
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        nodes[leader].metadata().members[&nid(MEMBER)].status,
        NodeStatus::Down,
        "leader should have committed Down for the silent member (seed={seed})"
    );
    let down_after = handles[leader].get(Metric::FailureDetectorDown);
    assert!(
        down_after > down_before,
        "failure_detector_down should increment on the Active->Down edge \
         (before={down_before}, after={down_after}, seed={seed})"
    );

    // --- Failure-detector up edge: restart the member; heartbeats resume. ---
    let up_before = handles[leader].get(Metric::FailureDetectorUp);
    sim.restart(nid(MEMBER));
    sim.run_for(Duration::from_secs(2));
    assert_eq!(
        nodes[leader].metadata().members[&nid(MEMBER)].status,
        NodeStatus::Active,
        "leader should have committed the recovery (seed={seed})"
    );
    let up_after = handles[leader].get(Metric::FailureDetectorUp);
    assert!(
        up_after > up_before,
        "failure_detector_up should increment on the Down->Active edge \
         (before={up_before}, after={up_after}, seed={seed})"
    );
}

/// Issue #313: an aborted leadership transfer used to be cleared with no log,
/// metric, or trace anywhere in the path — a caller (or an operator) could
/// not tell "transfer in progress" from "transfer was silently dropped
/// moments ago". Arm a transfer, then crash the target *before it can ever
/// receive/act on `TimeoutNow`* (mirroring "the target crashed after
/// arming"), and drive past the un-randomized `election_base` deadline
/// (150ms default) `RaftCore::tick` aborts on. The abort must now be
/// observable via `Metric::ControlTransferAborted`, and `transfer_target()`
/// must reflect both the arm and the clear (`/admin/raft`'s own window onto
/// this, ADR 0037/0009).
#[test]
fn aborted_leadership_transfer_is_observable() {
    aborted_leadership_transfer_run(0xABCD_1234);
}

/// The abort (and its metric) are a pure function of the seed, same
/// discipline as `metrics_are_reproducible_from_seed` below.
#[test]
fn aborted_leadership_transfer_is_reproducible_from_seed() {
    fn trace(seed: u64) -> u64 {
        aborted_leadership_transfer_run(seed)
    }
    assert_eq!(trace(0x0313_5EED), trace(0x0313_5EED));
}

/// Drives the abort scenario described above once and returns the leader's
/// final `ControlTransferAborted` count, so both tests above can share one
/// implementation (the seed-reproducibility test needs the return value; the
/// primary test only needs its own assertions along the way).
fn aborted_leadership_transfer_run(seed: u64) -> u64 {
    let (mut sim, nodes, handles) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = leader_index(&nodes);
    let target = (0..nodes.len())
        .find(|&i| i != leader)
        .expect("a 3-node cluster has a non-leader");
    let target_id = nid(CONTROL[target]);

    let aborted_before = handles[leader].get(Metric::ControlTransferAborted);
    assert!(
        nodes[leader].transfer_leadership(target_id.clone()),
        "target should already be caught up enough to arm (seed={seed})"
    );
    assert_eq!(
        nodes[leader].transfer_target(),
        Some(target_id.clone()),
        "an armed transfer should be visible via transfer_target() (seed={seed})"
    );

    // Kill the target immediately — no sim time elapses between arming and
    // this crash, so it can never receive (let alone act on) `TimeoutNow`.
    sim.crash(target_id);
    // Well past one un-randomized election_base (150ms default): long enough
    // for `RaftCore::tick`'s deadline check to fire on every plausible tick
    // cadence, short enough to stay well inside this test's own budget.
    sim.run_for(Duration::from_millis(500));

    assert!(
        nodes[leader].is_leader(),
        "an aborted transfer must not itself demote the leader (seed={seed})"
    );
    assert_eq!(
        nodes[leader].transfer_target(),
        None,
        "the aborted transfer must have cleared (seed={seed})"
    );
    let aborted_after = handles[leader].get(Metric::ControlTransferAborted);
    assert!(
        aborted_after > aborted_before,
        "an aborted transfer must be observable via Metric::ControlTransferAborted \
         (before={aborted_before}, after={aborted_after}, seed={seed})"
    );

    // The freeze must lift once the abort clears — an ordinary propose
    // succeeds again with no further intervention.
    assert!(
        matches!(
            nodes[leader].propose(MetaCommand::NoOp),
            ProposeResult::Accepted { .. }
        ),
        "proposing must resume once the aborted transfer clears (seed={seed})"
    );

    aborted_after
}

/// The recorded counters are a pure function of the seed: the same seed yields a
/// byte-identical text export of the leader's snapshot.
#[test]
fn metrics_are_reproducible_from_seed() {
    fn trace(seed: u64) -> String {
        let (mut sim, nodes, handles) = cluster(seed);
        sim.run_for(Duration::from_secs(2));
        let leader = leader_index(&nodes);
        nodes[leader].propose(MetaCommand::UpsertMember {
            node: nid(MEMBER),
            labels: std::collections::BTreeMap::new(),
            status: NodeStatus::Active,
        });
        sim.run_for(Duration::from_secs(2));
        sim.crash(nid(MEMBER));
        sim.run_for(Duration::from_secs(2));
        sim.restart(nid(MEMBER));
        sim.run_for(Duration::from_secs(2));
        handles[leader].snapshot().to_text()
    }
    assert_eq!(trace(0x5EED_0B5E), trace(0x5EED_0B5E));
}
