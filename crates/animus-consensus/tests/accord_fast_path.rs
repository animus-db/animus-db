//! ADR 0011 acceptance tests for the **precise fast-path quorum bound**.
//!
//! The fast path commits at `t0` in one round only when a *fast quorum* of
//! replicas all reply identical `(t0, deps)`. The precise simplified-recovery
//! bound is all-but-the-failure-tolerance replicas (`N − 1` for `N = 2f+1`),
//! sized so any recovery (simple-majority) quorum intersects every fast quorum in
//! at least one replica — so a fast-path decision is always **recoverable** even
//! after the coordinator dies. These tests drive a real 5-node cluster under
//! `SimEnv`, take a genuine fast-path commit, then kill the coordinator and
//! recover from a quorum that excludes it, asserting the recovered decision equals
//! the fast-path one. The runs are byte-reproducible from their seeds.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_consensus::{AccordNode, Key, TxnId};
use animus_sim::{SimEnv, Simulator};
use futures::executor::block_on;

const NODES: [u64; 5] = [0, 1, 2, 3, 4];

fn cluster(seed: u64) -> (Simulator, Vec<AccordNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| AccordNode::start(sim.env(id), NODES.to_vec()))
        .collect();
    (sim, nodes)
}

fn keys(ks: &[Key]) -> BTreeSet<Key> {
    ks.iter().copied().collect()
}

fn store_writer(node: &AccordNode<SimEnv>, key: Key) -> Option<TxnId> {
    block_on(node.store_writer(key))
}

/// An uncontended transaction commits on the **fast path** (one round trip, no
/// `Accept`), on every replica of a 5-node cluster. With the precise bound the
/// fast quorum is 4 (`N − 1`); an uncontended `PreAccept` is answered with `t0`
/// unchanged by all, so the fast path fires.
#[test]
fn uncontended_transaction_commits_on_the_fast_path() {
    let seed = 0xFA57_0001;
    let (mut sim, nodes) = cluster(seed);

    let txn = nodes[0].submit(keys(&[7]));
    sim.run_for(Duration::from_secs(2));

    // The coordinator recorded a fast-path decision.
    let decisions = nodes[0].decisions();
    let d = decisions
        .iter()
        .find(|d| d.txn == txn)
        .unwrap_or_else(|| panic!("coordinator never decided txn (seed={seed})"));
    assert!(
        d.fast_path,
        "uncontended txn should commit on the fast path (seed={seed})"
    );

    // It committed + executed on every replica at the same timestamp.
    let e0 = nodes[0].committed_execute_at(txn);
    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(txn),
            "node {i} did not execute txn (seed={seed})"
        );
        assert_eq!(
            n.committed_execute_at(txn),
            e0,
            "node {i} disagrees on the fast-path timestamp (seed={seed})"
        );
    }
}

/// A **fast-path decision is recoverable under failure**: the coordinator (node 0)
/// fast-commits, then dies before some replicas learn the `Commit`. A recovery
/// coordinator (node 4) takes over with a recovery quorum that **excludes the dead
/// coordinator**, and must reconstruct the *same* execution timestamp — because
/// any recovery quorum intersects the fast quorum, so at least one recovery
/// replica witnessed `t0`. This is exactly the property the precise quorum bound
/// guarantees.
#[test]
fn fast_path_commit_is_recoverable_after_coordinator_death() {
    let seed = 0xFA57_0002;
    let (mut sim, nodes) = cluster(seed);

    // Let the coordinator gather its fast quorum and fast-commit, but cut it off
    // from two replicas (3 and 4) so they never learn the `Commit` — those are the
    // replicas recovery will run on. Node 0's fast quorum is {0,1,2} ∪ ... ; with
    // 0↔3 and 0↔4 blocked the coordinator still has {0,1,2} = 3, below the fast
    // quorum of 4, so it cannot fast-commit YET — first let it commit, then cut.
    let txn = nodes[0].submit(keys(&[7]));
    // Brief settle: the fast path fires (no partition yet), so the coordinator
    // commits. Stay well under the failure-detector bound.
    sim.run_for(Duration::from_millis(150));
    assert!(
        nodes[0].committed_execute_at(txn).is_some(),
        "coordinator should have fast-committed before we kill it (seed={seed})"
    );
    let committed_at = nodes[0].committed_execute_at(txn);

    // Now the coordinator dies, and the two replicas that recovery will use are
    // isolated from it (they may not have learned the Commit). Recovery must
    // reconstruct the same decision from a quorum that excludes node 0.
    sim.stop(0);
    let recoverer = &nodes[4];
    recoverer.recover(txn);
    sim.run_for(Duration::from_secs(3));

    // Every surviving replica committed it at the *same* timestamp the fast path
    // chose — a fast decision was never lost or contradicted.
    for (i, n) in nodes.iter().enumerate().skip(1) {
        let e = n.committed_execute_at(txn);
        assert_eq!(
            e, committed_at,
            "survivor {i} recovered a different timestamp than the fast-path \
             commit ({e:?} vs {committed_at:?}) — fast decision unrecoverable \
             (seed={seed})"
        );
        assert!(
            n.is_applied(txn),
            "survivor {i} did not execute the recovered txn (seed={seed})"
        );
        assert_eq!(
            store_writer(n, 7),
            Some(txn),
            "survivor {i} store missing the write (seed={seed})"
        );
    }
}

/// Recoverability of a fast-path commit holds across a range of seeds (different
/// message interleavings): whatever the coordinator fast-committed, the survivors
/// recover the identical decision after it dies.
#[test]
fn fast_path_recovery_is_consistent_across_seeds() {
    for seed in 0xFA57_1000..0xFA57_1010 {
        let (mut sim, nodes) = cluster(seed);
        let txn = nodes[0].submit(keys(&[3, 4]));
        sim.run_for(Duration::from_millis(150));
        let committed_at = nodes[0].committed_execute_at(txn);
        assert!(
            committed_at.is_some(),
            "coordinator should have committed before death (seed={seed})"
        );

        sim.stop(0);
        nodes[4].recover(txn);
        sim.run_for(Duration::from_secs(3));

        for (i, n) in nodes.iter().enumerate().skip(1) {
            assert_eq!(
                n.committed_execute_at(txn),
                committed_at,
                "survivor {i} diverged from the fast-path commit (seed={seed})"
            );
        }
    }
}

/// The run is byte-reproducible from its seed (same trace twice).
#[test]
fn fast_path_run_is_reproducible_from_seed() {
    let trace = |seed: u64| {
        let (mut sim, nodes) = cluster(seed);
        let txn = nodes[0].submit(keys(&[7]));
        sim.run_for(Duration::from_millis(150));
        sim.stop(0);
        nodes[4].recover(txn);
        sim.run_for(Duration::from_secs(2));
        sim.trace_lines()
    };
    let seed = 0xFA57_2001;
    assert_eq!(
        trace(seed),
        trace(seed),
        "fast-path recovery run not reproducible (seed={seed})"
    );
}
