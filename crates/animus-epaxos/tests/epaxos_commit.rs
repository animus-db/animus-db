//! ADR 0025 acceptance tests for the minimal EPaxos slice.
//!
//! Under `SimEnv`, a 3-node replica set agrees on and commits commands via the
//! leaderless PreAccept → (fast path) Commit protocol. The core properties this
//! slice proves:
//!
//! - an uncontended command commits on every replica on the **fast path**;
//! - two **conflicting** commands both commit, every replica agrees on each
//!   command's `(seq, deps)`, and there is a **dependency edge** between them (the
//!   EPaxos quorum-intersection invariant that pins their execution order);
//! - **disjoint** commands are independent (neither depends on the other);
//! - the whole run is byte-reproducible from its seed.
//!
//! Execution order (the SCC executor) and recovery are deferred (see the crate
//! docs), so these tests assert *agreement on attributes*, not executed state.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_epaxos::{EPaxosNode, Key};
use animus_sim::{SimEnv, Simulator};

const NODES: [u64; 3] = [0, 1, 2];

fn cluster(seed: u64) -> (Simulator, Vec<EPaxosNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| EPaxosNode::start(sim.env(id), NODES.to_vec()))
        .collect();
    (sim, nodes)
}

fn keys(ks: &[Key]) -> BTreeSet<Key> {
    ks.iter().copied().collect()
}

/// A single command submitted to one leader commits on every replica, via the
/// fast path, with identical attributes and no dependencies.
#[test]
fn single_command_commits_on_all_replicas() {
    let seed = 0xE9A5_0001;
    let (mut sim, nodes) = cluster(seed);

    let cmd = nodes[0].submit(keys(&[42]));
    sim.run_for(Duration::from_secs(1));

    let decisions = nodes[0].decisions();
    assert_eq!(decisions.len(), 1, "one decision expected (seed={seed})");
    assert_eq!(decisions[0].instance, cmd, "decision is for our command");
    assert!(
        decisions[0].fast_path,
        "uncontended command should take the fast path (seed={seed})"
    );

    let attrs: Vec<_> = nodes.iter().map(|n| n.committed_attrs(cmd)).collect();
    assert!(
        attrs.iter().all(Option::is_some),
        "command not committed on every replica: {attrs:?} (seed={seed})"
    );
    assert!(
        attrs.iter().all(|a| *a == attrs[0]),
        "committed attributes diverged across replicas: {attrs:?} (seed={seed})"
    );
    assert!(
        nodes[0].committed_deps(cmd).unwrap().is_empty(),
        "an uncontended command has no dependencies (seed={seed})"
    );
}

/// Two commands over an overlapping key both commit; every replica agrees on each
/// command's attributes and there is a dependency edge between them.
#[test]
fn conflicting_commands_commit_with_consistent_attrs() {
    let seed = 0xE9A5_0002;
    let (mut sim, nodes) = cluster(seed);

    let a = nodes[0].submit(keys(&[7]));
    let b = nodes[1].submit(keys(&[7]));
    sim.run_for(Duration::from_secs(2));

    assert_consistent_and_dependent(&nodes, a, b, seed);
}

/// Same, submitted in the opposite leader order and with a different seed, to
/// exercise a different interleaving.
#[test]
fn conflicting_commands_consistent_reverse() {
    let seed = 0xE9A5_0003;
    let (mut sim, nodes) = cluster(seed);

    let b = nodes[2].submit(keys(&[7, 9]));
    let a = nodes[0].submit(keys(&[9]));
    sim.run_for(Duration::from_secs(2));

    assert_consistent_and_dependent(&nodes, a, b, seed);
}

/// Conflicting commands agree across a range of seeds (different network jitter ⇒
/// different message interleavings).
#[test]
fn conflicting_consistent_across_seeds() {
    for seed in 0xE9A5_1000..0xE9A5_1040 {
        let (mut sim, nodes) = cluster(seed);
        let a = nodes[0].submit(keys(&[5]));
        let b = nodes[1].submit(keys(&[5]));
        sim.run_for(Duration::from_secs(3));
        assert_consistent_and_dependent(&nodes, a, b, seed);
    }
}

/// Disjoint-key commands both commit; neither depends on the other.
#[test]
fn disjoint_commands_are_independent() {
    let seed = 0xE9A5_0004;
    let (mut sim, nodes) = cluster(seed);

    let a = nodes[0].submit(keys(&[1]));
    let b = nodes[1].submit(keys(&[2]));
    sim.run_for(Duration::from_secs(2));

    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.committed_attrs(a).is_some() && n.committed_attrs(b).is_some(),
            "node {i}: both disjoint commands must commit (seed={seed})"
        );
        assert!(
            !n.committed_deps(a).unwrap_or_default().contains(&b)
                && !n.committed_deps(b).unwrap_or_default().contains(&a),
            "node {i}: disjoint commands must not depend on each other (seed={seed})"
        );
    }
}

/// Replaying the same seed produces a byte-identical trace.
#[test]
fn run_is_reproducible_from_seed() {
    let seed = 0xE9A5_0005;
    let trace = |seed| {
        let (mut sim, nodes) = cluster(seed);
        nodes[0].submit(keys(&[3]));
        nodes[1].submit(keys(&[3]));
        sim.run_for(Duration::from_secs(2));
        sim.trace_lines()
    };
    assert_eq!(trace(seed), trace(seed), "trace not reproducible");
}

/// Assert both commands committed on every replica, that every replica agrees on
/// each command's `(seq, deps)`, and that the two conflicting commands have a
/// dependency edge between them (so their execution order is pinned).
fn assert_consistent_and_dependent(
    nodes: &[EPaxosNode<SimEnv>],
    a: animus_epaxos::InstanceId,
    b: animus_epaxos::InstanceId,
    seed: u64,
) {
    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.committed_attrs(a).is_some() && n.committed_attrs(b).is_some(),
            "node {i} missing a commit (seed={seed})"
        );
    }
    // Agreement: every replica records identical attributes for each command.
    for (name, id) in [("a", a), ("b", b)] {
        let all: Vec<_> = nodes.iter().map(|n| n.committed_attrs(id)).collect();
        assert!(
            all.iter().all(|x| *x == all[0]),
            "{name} attributes diverged across replicas: {all:?} (seed={seed})"
        );
    }
    // Conflict reflected: at least one direction of dependency exists (EPaxos
    // guarantees this via quorum intersection — the intersecting replica saw both).
    let deps_a = nodes[0].committed_deps(a).unwrap_or_default();
    let deps_b = nodes[0].committed_deps(b).unwrap_or_default();
    assert!(
        deps_a.contains(&b) || deps_b.contains(&a),
        "conflicting commands must have a dependency edge (seed={seed}): \
         a.deps={deps_a:?} b.deps={deps_b:?}"
    );
}
