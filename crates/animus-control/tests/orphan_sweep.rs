//! Fault-injecting acceptance tests for the ADR 0040 PR6 orphan-member sweep
//! (`animus_control::node::orphan_sweep_loop`, private — exercised only
//! through the public `RaftNode` surface, mirroring `failure_detection.rs`'s
//! style for its own ADR-0012 sibling mechanism): a 3-node control group
//! under `SimEnv`, seed-reproducible.
//!
//! Covers the plan's full scenario list (ADR 0040 delivery plan §10):
//! crash-mid-join swept after the grace period; the losing racer of two
//! concurrent omitted-id `control-add`s swept while the winner (now a live
//! control voter) is protected; a slow-but-legit joiner that activates before
//! the grace period elapses is never swept; a member that was genuinely
//! `Active` once and later went `Down` is never swept (the `has_activated`
//! guard); a leader failover mid-countdown still converges (just later, on
//! the new leader's own timer); the sweep disabled (`Duration::ZERO`) keeps
//! an orphan forever; the control-role claim-without-member shape (a
//! `node_addrs` entry with no `members` row at all) is swept too; and its
//! dual, a `members`-row-only claim with no `node_addrs` entry at all
//! (`admin_add_member`'s bare growth registration), is swept as well.
//!
//! The safety argument for a sweep proposal racing a genuine late activation
//! — "no `Active` member is ever removed by the sweep" — is a **structural**
//! property of `MetaCommand::RemoveMember`'s existing apply-time guard (it
//! re-checks the member's status fresh at apply time, rejecting
//! `Active`/`Joining` outright), not a real-time race this suite tries to
//! force through timer coincidence. It is proven directly and exhaustively
//! as a pure state-machine property in
//! `animus_control::meta::tests::remove_member_never_removes_a_member_that_
//! activated_first_regardless_of_proposal_order` and the "no resurrection via
//! the normal detector path" half in
//! `animus_control::node::tests::liveness_transitions_never_proposes_for_an_
//! absent_member` (both in-crate unit tests, since they drive
//! crate-private `Metadata`/`liveness_transitions` internals directly rather
//! than approximate the interleaving through `SimEnv` timing).

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::meta::NodeAddrs;
use animus_control::node::heartbeat_loop;
use animus_control::raft::ProposeResult;
use animus_control::{DeltaRing, MetaCommand, NodeStatus, RaftNode};
use animus_env::{Clock, EnvExt, MetricsHandle, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const CONTROL: [u64; 3] = [0, 1, 2];

/// A short-relative-to-production sweep grace period: comfortably larger
/// than `ORPHAN_SWEEP_CHECK_INTERVAL` (5s, private to `node.rs`) so every
/// test sees several check ticks, while staying fast under `SimEnv`'s
/// virtual clock (which does not cost real wall time to advance).
const SWEEP_AFTER: Duration = Duration::from_secs(30);

/// Stand up a 3-node control cluster with the orphan sweep enabled at
/// `sweep_after` (pass `Duration::ZERO` to disable it outright).
fn cluster(seed: u64, sweep_after: Duration) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = CONTROL
        .iter()
        .map(|&id| {
            RaftNode::start_with_orphan_sweep_after(
                sim.env(nid(id)),
                CONTROL.iter().copied().map(nid).collect(),
                MetricsHandle::recording(),
                MemoryEngine::new(),
                DeltaRing::default(),
                sweep_after,
            )
        })
        .collect();
    (sim, nodes)
}

/// Index of the unique leader among every control node, asserting there is
/// exactly one.
fn unique_leader(nodes: &[RaftNode<SimEnv>], seed: u64) -> usize {
    let leaders: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(
        leaders.len(),
        1,
        "expected exactly one leader, found {leaders:?} (seed={seed})"
    );
    leaders[0]
}

fn addrs(role: &str, suffix: u16) -> NodeAddrs {
    NodeAddrs {
        internal: format!("127.0.0.1:{}", 9300 + suffix),
        client: format!("127.0.0.1:{}", 9000 + suffix),
        admin: format!("127.0.0.1:{}", 9500 + suffix),
        intra: format!("127.0.0.1:{}", 9600 + suffix),
        role: role.to_string(),
    }
}

fn register(node: NodeId, role: &str, suffix: u16) -> MetaCommand {
    MetaCommand::RegisterNode {
        node,
        addrs: addrs(role, suffix),
        labels: BTreeMap::new(),
    }
}

/// A crash-mid-join orphan (a data-capable registration whose node never
/// heartbeats — a real join that never got past `RegisterNode`) is swept
/// once `SWEEP_AFTER` elapses.
#[test]
fn crash_mid_join_orphan_swept_after_ttl() {
    let seed = 0x5EED_0001;
    let (mut sim, nodes) = cluster(seed, SWEEP_AFTER);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, seed);

    let orphan = nid(900);
    assert!(matches!(
        nodes[leader].propose(register(orphan.clone(), "combined", 0)),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(1));
    // Claimed, but never activated: present and Down everywhere.
    for node in &nodes {
        let m = node.metadata();
        assert!(
            m.node_addrs.contains_key(&orphan),
            "claim missing pre-sweep (seed={seed})"
        );
        assert_eq!(m.members[&orphan].status, NodeStatus::Down, "seed={seed}");
        assert!(!m.members[&orphan].has_activated, "seed={seed}");
    }

    // No heartbeat ever arrives. Run well past the grace period.
    sim.run_for(SWEEP_AFTER + Duration::from_secs(15));

    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        assert!(
            !m.node_addrs.contains_key(&orphan) && !m.members.contains_key(&orphan),
            "node {i}: crash-mid-join orphan not swept (seed={seed})"
        );
    }
}

/// The losing racer of two concurrent omitted-id `control-add`s (both
/// register a control-role, claim-without-member id; only one ever actually
/// becomes a live control voter — the other's `change_membership` lost the
/// race, exactly as `animusd::admin_add_control_member`'s own concurrent
/// test documents) is swept, while the winner's claim is permanently
/// protected by the control-voter exclusion.
#[test]
fn losing_racer_of_concurrent_omitted_id_control_adds_swept() {
    let seed = 0x5EED_0002;
    let (mut sim, nodes) = cluster(seed, SWEEP_AFTER);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, seed);

    let winner = nid(901);
    let loser = nid(902);
    for (id, suffix) in [(&winner, 1u16), (&loser, 2u16)] {
        assert!(matches!(
            nodes[leader].propose(register(id.clone(), "control", suffix)),
            ProposeResult::Accepted { .. }
        ));
    }
    sim.run_for(Duration::from_secs(1));
    // Claim-without-member shape: no `members` row for either, by design
    // (a control-role registration never claims one).
    for node in &nodes {
        let m = node.metadata();
        for id in [&winner, &loser] {
            assert!(m.node_addrs.contains_key(id), "seed={seed}");
            assert!(!m.members.contains_key(id), "seed={seed}");
        }
    }

    // The winner's own `change_membership` succeeds — it is now a live
    // control voter. The loser's never lands (simulating its own
    // `change_membership` call losing the single-in-flight-change race and
    // never being retried by anything, exactly the abandoned-claim shape
    // `animusd::CLAUDE.md`'s concurrent-add test documents).
    let mut voters = nodes[leader].config();
    voters.insert(winner.clone());
    assert!(matches!(
        nodes[leader].change_membership(voters),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));
    assert!(
        nodes[leader].config().contains(&winner),
        "winner should be a live control voter (seed={seed})"
    );

    sim.run_for(SWEEP_AFTER + Duration::from_secs(15));

    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        assert!(
            m.node_addrs.contains_key(&winner),
            "node {i}: a live control voter's claim was swept! (seed={seed})"
        );
        assert!(
            !m.node_addrs.contains_key(&loser),
            "node {i}: losing racer's abandoned claim not swept (seed={seed})"
        );
    }
}

/// The claim-without-member shape on its own (no promotion race involved):
/// a control-role registration that never becomes a live control voter is
/// swept — proving `RemoveMember`'s extension cleans up the orphaned
/// `node_addrs` entry `RegisterNode` can leave with no `members` row at all.
#[test]
fn control_role_claim_without_member_orphan_swept() {
    let seed = 0x5EED_0003;
    let (mut sim, nodes) = cluster(seed, SWEEP_AFTER);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, seed);

    let orphan = nid(903);
    assert!(matches!(
        nodes[leader].propose(register(orphan.clone(), "control", 3)),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(1));
    assert!(!nodes[leader].metadata().members.contains_key(&orphan));

    sim.run_for(SWEEP_AFTER + Duration::from_secs(15));

    for (i, node) in nodes.iter().enumerate() {
        assert!(
            !node.metadata().node_addrs.contains_key(&orphan),
            "node {i}: control-role claim-without-member orphan not swept (seed={seed})"
        );
    }
}

/// The dual shape: a **`members`-row-only** claim with no `node_addrs` entry
/// at all — exactly `admin_add_member`'s bare `UpsertMember{Down}` growth
/// registration (ADR 0030 online growth, `POST /admin/member/add`), proposed
/// ahead of the node's own later self-registration. If that node never
/// actually boots, this is a declared-but-never-booted phantom, structurally
/// the same "never showed up" orphan class as a crash-mid-join — swept too,
/// proving `Metadata::orphan_sweep_candidates`'s union covers this shape, not
/// just the `node_addrs`-keyed ones.
#[test]
fn admin_added_growth_member_with_no_address_is_swept() {
    let seed = 0x5EED_0008;
    let (mut sim, nodes) = cluster(seed, SWEEP_AFTER);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, seed);

    let orphan = nid(908);
    assert!(matches!(
        nodes[leader].propose(MetaCommand::UpsertMember {
            node: orphan.clone(),
            labels: BTreeMap::new(),
            status: NodeStatus::Down,
        }),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(1));
    for node in &nodes {
        let m = node.metadata();
        assert!(m.members.contains_key(&orphan), "seed={seed}");
        assert!(
            !m.node_addrs.contains_key(&orphan),
            "test premise: seed={seed}"
        );
        assert!(!m.members[&orphan].has_activated, "seed={seed}");
    }

    // The node this id names never actually starts (no heartbeat, no
    // self-registration ever arrives).
    sim.run_for(SWEEP_AFTER + Duration::from_secs(15));

    for (i, node) in nodes.iter().enumerate() {
        assert!(
            !node.metadata().members.contains_key(&orphan),
            "node {i}: admin-added never-booted growth member not swept (seed={seed})"
        );
    }
}

/// A slow-but-legit joiner — its own real heartbeats start only partway
/// through the grace period, well before it elapses — activates and is
/// never swept.
#[test]
fn slow_but_legit_joiner_activates_before_ttl_not_swept() {
    let seed = 0x5EED_0004;
    let (mut sim, nodes) = cluster(seed, SWEEP_AFTER);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, seed);

    let joiner = nid(904);
    assert!(matches!(
        nodes[leader].propose(register(joiner.clone(), "combined", 4)),
        ProposeResult::Accepted { .. }
    ));

    // Heartbeats start only after a real delay — well short of `SWEEP_AFTER`,
    // proving this isn't just "activates instantly at t=0".
    let env = sim.env(joiner.clone());
    let control: Vec<NodeId> = CONTROL.iter().copied().map(nid).collect();
    let spawn_env = env.clone();
    spawn_env.spawn_task(async move {
        env.sleep(Duration::from_secs(10)).await;
        heartbeat_loop(env.clone(), control).await;
    });

    sim.run_for(SWEEP_AFTER + Duration::from_secs(15));

    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        assert!(
            m.members.contains_key(&joiner),
            "node {i}: legit joiner was swept (seed={seed})"
        );
        assert_eq!(
            m.members[&joiner].status,
            NodeStatus::Active,
            "node {i}: legit joiner never activated (seed={seed})"
        );
        assert!(m.members[&joiner].has_activated, "seed={seed}");
    }
}

/// A member that was genuinely `Active` once and later crashed (`Down`) is
/// never swept — the `has_activated` guard distinguishes "was alive,
/// currently down" (repair/decommission territory) from "never showed up"
/// (sweepable).
#[test]
fn member_active_once_then_down_not_swept() {
    let seed = 0x5EED_0005;
    let (mut sim, nodes) = cluster(seed, SWEEP_AFTER);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, seed);

    let member = nid(905);
    assert!(matches!(
        nodes[leader].propose(register(member.clone(), "combined", 5)),
        ProposeResult::Accepted { .. }
    ));
    let env = sim.env(member.clone());
    let control: Vec<NodeId> = CONTROL.iter().copied().map(nid).collect();
    env.spawn_task(heartbeat_loop(env.clone(), control));

    // Let it genuinely activate first.
    sim.run_for(Duration::from_secs(3));
    assert_eq!(
        nodes[leader].metadata().members[&member].status,
        NodeStatus::Active,
        "seed={seed}"
    );

    // Now it crashes for good — heartbeats stop, the detector eventually
    // marks it Down.
    sim.crash(member.clone());
    sim.run_for(SWEEP_AFTER + Duration::from_secs(15));

    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        assert!(
            m.members.contains_key(&member),
            "node {i}: a once-active member was swept while merely Down (seed={seed})"
        );
        assert_eq!(m.members[&member].status, NodeStatus::Down, "seed={seed}");
        assert!(m.members[&member].has_activated, "seed={seed}");
    }
}

/// A leader failover mid-countdown resets the volatile timer (acceptable —
/// convergent, just delayed): the sweep still eventually converges once the
/// new leader's own countdown elapses.
#[test]
fn leader_failover_mid_countdown_still_converges() {
    let seed = 0x5EED_0006;
    let (mut sim, nodes) = cluster(seed, SWEEP_AFTER);
    sim.run_for(Duration::from_secs(2));
    let first_leader = unique_leader(&nodes, seed);

    let orphan = nid(906);
    assert!(matches!(
        nodes[first_leader].propose(register(orphan.clone(), "combined", 6)),
        ProposeResult::Accepted { .. }
    ));

    // Let the first leader's countdown run partway, then kill it — forcing
    // an election and resetting every volatile sweep timer.
    sim.run_for(SWEEP_AFTER / 2);
    sim.crash(nid(CONTROL[first_leader]));
    sim.run_for(Duration::from_secs(2));
    let live: Vec<usize> = (0..CONTROL.len()).filter(|&i| i != first_leader).collect();
    let new_leader = live
        .iter()
        .copied()
        .find(|&i| nodes[i].is_leader())
        .expect("a new leader elected");
    assert_ne!(new_leader, first_leader, "seed={seed}");

    // The orphan survives the failover itself (its countdown merely reset,
    // not cancelled) — not yet swept right after the election.
    assert!(
        nodes[new_leader]
            .metadata()
            .node_addrs
            .contains_key(&orphan),
        "orphan swept prematurely across a mere leadership change (seed={seed})"
    );

    // The new leader's own countdown now has to run its full course.
    sim.run_for(SWEEP_AFTER + Duration::from_secs(15));

    let m = nodes[new_leader].metadata();
    assert!(
        !m.node_addrs.contains_key(&orphan) && !m.members.contains_key(&orphan),
        "orphan never swept after failover converged (seed={seed})"
    );
}

/// `orphan_sweep_after == Duration::ZERO` disables the sweep outright: an
/// otherwise-eligible orphan is kept indefinitely (no loop is even spawned).
#[test]
fn sweep_disabled_keeps_entry_indefinitely() {
    let seed = 0x5EED_0007;
    let (mut sim, nodes) = cluster(seed, Duration::ZERO);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, seed);

    let orphan = nid(907);
    assert!(matches!(
        nodes[leader].propose(register(orphan.clone(), "combined", 7)),
        ProposeResult::Accepted { .. }
    ));

    // Run for several multiples of what would otherwise be a generous grace
    // period — nothing sweeps it.
    sim.run_for(SWEEP_AFTER * 4);

    for (i, node) in nodes.iter().enumerate() {
        let m = node.metadata();
        assert!(
            m.node_addrs.contains_key(&orphan) && m.members.contains_key(&orphan),
            "node {i}: orphan swept despite the sweep being disabled (seed={seed})"
        );
    }
}
