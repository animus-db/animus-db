//! Issue #667 follow-up: three boot-time cluster-check shapes the checkpoint
//! left unproven by a dedicated regression, each red against the pre-
//! `ever_heard_from_prober` code and green with the fix in place:
//!
//! 1. A genuine **one-node genesis** (no peers at all) must resolve
//!    instantly and never refuse itself — proven at the `RaftNode`/`SimEnv`
//!    driver level, the same layer a real single-node `animusd --cluster 1`
//!    boots through.
//! 2. **Growing 1 -> 2 -> 3** via `change_membership` (the same primitive
//!    `POST /admin/control/member/add` drives, ADR 0037) must let each new
//!    voter join without ever being falsely refused, even though each
//!    grown node's own peer(s) already have real committed history the
//!    instant it starts.
//! 3. The **exact race** that made (2) a real hazard, isolated at the bare
//!    `RaftCore` level so it is deterministic and needs no timing luck: a
//!    solo leader's `change_membership` adopts the new config *immediately*
//!    (Raft single-server changes, before the entry itself commits), so a
//!    freshly-grown node's very first `ClusterProbe` can be answered by a
//!    peer whose own `config` ALREADY names it and who already has real
//!    history — `config.contains(asker)` alone can never tell that apart
//!    from a genuinely wiped, previously-established voter. Before the
//!    `ever_heard_from_prober` signal (`RaftMsg::ClusterProbeResp`'s own
//!    doc), a single-peer joiner's boot-time check resolved this
//!    combination as an immediate, permanent, false refusal (`pending`
//!    starts and ends as exactly one peer, so there is no "wait for more
//!    evidence" window at all).

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::{ProposeResult, RaftCore, RaftMsg, RaftNode};
use animus_env::{Nanos, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

fn set(ids: &[u64]) -> BTreeSet<NodeId> {
    ids.iter().copied().map(nid).collect()
}

/// Mirrors `wiped_voter_rejoin.rs`'s own `health_ok` — the e2e's actual
/// `/admin/health` liveness gate (3 election timeouts of grace).
fn health_ok(nodes: &[RaftNode<SimEnv>], live: &[usize]) -> bool {
    live.iter().all(|&i| {
        let grace = nodes[i].election_timeout() * 3;
        nodes[i].leader_within(grace).is_some()
    })
}

fn assert_none_refused(nodes: &[RaftNode<SimEnv>], live: &[usize], seed: u64, when: &str) {
    for &i in live {
        assert!(
            !nodes[i].refused_as_voter(),
            "seed={seed}: node {i} was falsely refused as a voter {when} -- the exact \
             issue #667 regression this test exists to catch"
        );
    }
}

/// (1) A genuine one-node genesis: no peers at all, so `begin_cluster_check`
/// must resolve immediately (the `peers.is_empty()` short-circuit) with no
/// pending check, no refusal, and a real, working single-voter group.
#[test]
fn single_node_genesis_never_refused_and_serves() {
    let seed = 0x1670_0001;
    let mut sim = Simulator::new(seed);
    let node = RaftNode::start(
        sim.env(nid(0)),
        set(&[0]).into_iter().collect(),
        MemoryEngine::new(),
    );
    let nodes = vec![node];

    sim.run_for(Duration::from_secs(2));

    assert!(
        !nodes[0].cluster_check_pending(),
        "seed={seed}: a lone voter with zero peers must never have a pending cluster check"
    );
    assert_none_refused(&nodes, &[0], seed, "during a one-node genesis");
    assert!(
        nodes[0].is_leader(),
        "seed={seed}: a lone voter must self-elect"
    );
    assert!(
        health_ok(&nodes, &[0]),
        "seed={seed}: a lone voter must be its own healthy leader"
    );

    sim.run_for(Duration::from_secs(1));
    assert!(
        !nodes[0].refused_as_voter(),
        "seed={seed}: still not refused a second later"
    );
}

/// (2) Growing a genuine one-node genesis to three voters, one at a time
/// (`change_membership`'s own single-server-change contract), via the
/// identical primitive `POST /admin/control/member/add` drives. Each
/// grown node starts fresh (empty WAL) with its peer(s) already an
/// established voter with real history -- exactly the shape a real
/// `animusd join`/admin-add sequence produces.
#[test]
fn grow_from_one_to_three_never_falsely_refuses() {
    let seed = 0x1670_0002;
    let mut sim = Simulator::new(seed);

    // Start alone.
    let node0 = RaftNode::start(
        sim.env(nid(0)),
        set(&[0]).into_iter().collect(),
        MemoryEngine::new(),
    );
    let mut nodes = vec![node0];
    sim.run_for(Duration::from_secs(2));
    assert!(
        nodes[0].is_leader(),
        "seed={seed}: the sole founder must self-elect"
    );
    assert_none_refused(&nodes, &[0], seed, "before any growth");

    // Grow to 2.
    assert!(
        matches!(
            nodes[0].change_membership(set(&[0, 1])),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}: 1 -> 2 growth must be accepted"
    );
    let node1 = RaftNode::start(
        sim.env(nid(1)),
        set(&[0, 1]).into_iter().collect(),
        MemoryEngine::new(),
    );
    nodes.push(node1);
    sim.run_for(Duration::from_secs(3));
    assert_eq!(
        nodes[1].config(),
        set(&[0, 1]),
        "seed={seed}: node 1 must adopt the grown config"
    );
    assert_none_refused(&nodes, &[0, 1], seed, "after growing to 2");
    assert!(
        health_ok(&nodes, &[0, 1]),
        "seed={seed}: the 2-voter group must be healthy after growth"
    );

    // Grow to 3.
    let leader01 = if nodes[0].is_leader() { 0 } else { 1 };
    assert!(
        matches!(
            nodes[leader01].change_membership(set(&[0, 1, 2])),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}: 2 -> 3 growth must be accepted"
    );
    let node2 = RaftNode::start(
        sim.env(nid(2)),
        set(&[0, 1, 2]).into_iter().collect(),
        MemoryEngine::new(),
    );
    nodes.push(node2);
    sim.run_for(Duration::from_secs(3));
    assert_eq!(
        nodes[2].config(),
        set(&[0, 1, 2]),
        "seed={seed}: node 2 must adopt the grown config"
    );
    assert_none_refused(&nodes, &[0, 1, 2], seed, "after growing to 3");
    assert!(
        health_ok(&nodes, &[0, 1, 2]),
        "seed={seed}: the 3-voter group must be healthy after growth"
    );
}

/// (3) The exact race isolated at the bare `RaftCore` level, deterministic
/// and seed-free: a solo leader's `change_membership` adopts `{0, 1}`
/// *immediately* (before the entry commits), so node 1's very first
/// `ClusterProbe` to node 0 is answered with a `config` that ALREADY names
/// node 1 and a `term`/`committed_index` that already prove real history --
/// the observationally-identical-to-a-wiped-voter-restart combination this
/// whole mechanism exists to disambiguate. With only one peer, the old
/// (pre-`ever_heard_from_prober`) design had no "wait for more evidence"
/// window at all: this single reply was immediately and permanently
/// decisive, falsely refusing node 1 forever. `ever_heard_from_prober`
/// (node 0 has never received any real protocol message from node 1, since
/// node 1 has not campaigned or voted yet) resolves it safely instead.
#[test]
fn grow_from_one_race_where_config_already_names_the_joiner_resolves_safely() {
    let mut core0: RaftCore = RaftCore::new(nid(0), &[nid(0)], Nanos(0), 7);
    // A lone voter wins its own election on the very first tick at a
    // far-future timestamp.
    let _ = core0.tick(Nanos(10_000_000_000), 7);
    assert!(core0.is_leader(), "a lone voter must self-elect");

    // Grow the *active* config to {0, 1} -- adopted immediately, per Raft's
    // single-server-change discipline, regardless of whether this entry
    // has itself committed yet.
    assert!(
        matches!(
            core0.change_membership(set(&[0, 1])),
            ProposeResult::Accepted { .. }
        ),
        "1 -> 2 growth must be accepted by the solo leader"
    );
    assert!(
        core0.config().contains(&nid(1)),
        "the leader's own active config must already name node 1"
    );

    // Node 1 starts fresh, with node 0 as its only configured peer, and
    // begins its own boot-time check.
    let mut core1: RaftCore = RaftCore::new(nid(1), &[nid(0), nid(1)], Nanos(0), 11);
    let probe_out = core1.begin_cluster_check(Nanos(0), 11);
    assert!(
        core1.cluster_check_pending(),
        "node 1 must start a pending check (it has one peer)"
    );
    let (to, msg) = probe_out
        .into_iter()
        .next()
        .expect("begin_cluster_check must broadcast a probe to its one peer");
    assert_eq!(to, nid(0));
    assert!(matches!(msg, RaftMsg::ClusterProbe));

    // Node 0 answers honestly -- real history, and a config that already
    // names the asker, but node 0 has never itself received any real
    // protocol message from node 1 (it hasn't campaigned or voted yet).
    let resp_out = core0.handle(nid(1), msg, Nanos(1_000_000), 7);
    let (back_to, resp) = resp_out
        .into_iter()
        .next()
        .expect("node 0 must answer the probe");
    assert_eq!(back_to, nid(1));
    let RaftMsg::ClusterProbeResp {
        term,
        committed_index,
        config,
        ever_heard_from_prober,
    } = resp.clone()
    else {
        panic!("expected a ClusterProbeResp, got {resp:?}");
    };
    assert!(
        term > 0 || committed_index > 0,
        "node 0 must report real history"
    );
    assert!(
        config.contains(&nid(1)),
        "node 0's own config must already name node 1 -- the race this test isolates"
    );
    assert!(
        !ever_heard_from_prober,
        "node 0 must honestly report it has never heard from node 1 before -- node 1 has \
         not campaigned or voted yet"
    );

    // Node 1 processes the reply: this must resolve the check safely,
    // immediately, with NO refusal -- the single-peer case that used to be
    // immediately and permanently decisive the other way.
    let _ = core1.handle(nid(0), resp, Nanos(1_000_000), 11);
    assert!(
        !core1.cluster_check_pending(),
        "a single decisive reply must resolve the check immediately"
    );
    assert!(
        !core1.refused_as_voter(),
        "node 1 must NOT be refused: this is a genuine ADR 0060 growth race, not a \
         wiped-voter restart -- the exact issue #667 regression this test exists to catch"
    );
}
