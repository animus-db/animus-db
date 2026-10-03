//! ADR 0073 Phase 2 (P2-A) **residual risk #1 at the node-assembly level**:
//! "P itself trusts heartbeats' `ext`; P2-A must test that every role's
//! traffic carries it." The era-start precondition P is evaluated by the
//! control leader off its passive observation table (one entry per inbound
//! control envelope sender, `animus_control::version_observe`); if some node
//! role never reached the leader's table, P could never hold (a liveness
//! bug) or, worse, a role could be mis-keyed. This test builds a `SimCluster`
//! with every role the fixture can express and asserts the leader's table
//! eventually holds every node, of every role, with its advertised range.
//!
//! **Roles covered** (all via `SimCluster`, no production code touched):
//! - control-only voters (`NodeRole::Control`, indices 0-2): no heartbeat
//!   loop, seen only through Raft traffic (AppendEntries responses, votes);
//! - a combined node (`NodeRole::Both`, index 3): heartbeats AND Raft;
//! - data-only nodes (`NodeRole::Data`, indices 4-5): heartbeats only
//!   (`ControlHandle::Remote`, no local control Raft);
//! - a control voter grown at runtime (`SimCluster::grow_control`, a learner
//!   that is promoted): Raft traffic;
//! - `--seed` joiners (`SimCluster::join_via_seed_with_role`), one data-only
//!   and one combined: self-minted ids (`n{idx}` does NOT apply), heartbeats.
//!
//! **Not covered**: a control *learner* in its non-voting phase through
//! `SimCluster` (the fixture exposes only the finished `grow_control`; the
//! learner phase is covered at the `RaftNode` level by `animus-control`'s
//! `version_observe_corpus::every_role_reaches_the_leader_observation_table`),
//! and any real-socket/`ProdEnv` path: `SimCluster` heartbeats through
//! `animus_control::node::heartbeat_loop` over the `Env` network, while
//! production's `heartbeat_loop_live` dials over TCP/TLS and its `ProdEnv`
//! only advertises an `ext` once P2-C wires `ProdEnv::set_own_ext` (so this
//! proves the observation keying and the role mix, not the P2-C wiring).
//!
//! Also checked: the table is keyed by the sender, so before any node
//! advertises (Phase 1, empty `ext`) every peer is present as `range: None`
//! (parsed, not missing), and after a leadership change the new leader (which
//! only ever received what was addressed to it) fills its own table within a
//! few heartbeat intervals.
//!
//! Replay one seed with `ANIMUS_SEED=<seed>`.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::version::VersionRange;
use animus_env::handshake::encode_ext;
use animus_env::{Env, NodeId, nid};

use super::sim_cluster::SimCluster;
use crate::config::NodeRole;

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return vec![s];
    }
    (0..4).map(|i| 0x7A2A_0000 + i).collect()
}

fn b2_ext() -> Vec<u8> {
    encode_ext(Some((1, 1)), Some("b2"))
}

/// Converged-or-timeout poll (this crate's per-module duplicate of the
/// shared idiom).
fn poll_until(
    cluster: &mut SimCluster,
    budget: Duration,
    seed: u64,
    what: &str,
    mut cond: impl FnMut(&mut SimCluster) -> bool,
) {
    const STEP: Duration = Duration::from_millis(100);
    let mut elapsed = Duration::ZERO;
    loop {
        if cond(cluster) {
            return;
        }
        assert!(
            elapsed < budget,
            "seed={seed}: {what} never converged within {budget:?}"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

/// The leader's table: every node in `all` but the leader itself, with
/// `want` as its range (`None` = parsed as a Phase 1 peer).
fn leader_table_matches(
    cluster: &mut SimCluster,
    all: &BTreeSet<NodeId>,
    want: Option<VersionRange>,
) -> bool {
    let idx = cluster.control_leader_index();
    let leader = cluster.control_node_id(idx);
    let obs = cluster.control_version_observations(leader);
    all.iter().filter(|id| **id != nid(leader)).all(|id| {
        obs.get(id).is_some_and(|o| {
            o.range == want && (want.is_none() || o.build.as_deref() == Some("b2"))
        })
    })
}

fn run(seed: u64) {
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Both,
        NodeRole::Data,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 2);
    let sim = cluster.simulator();
    let mut all: BTreeSet<NodeId> = (0..roles.len() as u64).map(nid).collect();
    let _ = cluster.control_leader_index();

    // Phase 1 first: every peer is observed, parsed as range `None`.
    poll_until(
        &mut cluster,
        Duration::from_secs(20),
        seed,
        "leader observes every initial node as a Phase 1 peer",
        |c| leader_table_matches(c, &all, None),
    );

    // Grow the cluster with a runtime control voter and two seed joiners.
    let grown = cluster.grow_control();
    all.insert(nid(grown));
    let joined_data = cluster.join_via_seed_with_role(0, NodeRole::Data);
    let joined_both = cluster.join_via_seed(0);
    for idx in [joined_data, joined_both] {
        // Self-minted ids: ask the node's own env, never `nid(idx)`.
        all.insert(cluster.handle().env(idx).node_id());
    }

    // Everyone upgrades to B2 (an `ext` change, no restart).
    for id in &all {
        sim.set_network_ext_for(id.clone(), b2_ext());
    }
    let want = Some(VersionRange::new(1, 1));
    poll_until(
        &mut cluster,
        Duration::from_secs(30),
        seed,
        "leader observes every node of every role as B2",
        |c| leader_table_matches(c, &all, want),
    );

    // Leadership moves to a control-only node, then to the combined node:
    // the new leader fills its own table from traffic addressed to it.
    for target in [1u64, 3] {
        cluster.transfer_control_leadership_to(target);
        poll_until(
            &mut cluster,
            Duration::from_secs(30),
            seed,
            &format!("new leader (node {target}) observes every node as B2"),
            |c| {
                let idx = c.control_leader_index();
                c.control_node_id(idx) == target && leader_table_matches(c, &all, want)
            },
        );
    }
}

#[test]
fn every_node_role_reaches_the_control_leaders_observation_table_over_seeds() {
    for seed in seeds() {
        run(seed);
    }
}
