//! ADR 0011 acceptance tests for **precise recovery ballots + duelling
//! recovery coordinators** (the second recovery slice).
//!
//! The first recovery slice (`accord_recover.rs`) proved a *single* recovery
//! coordinator drives a stranded transaction to a consistent commit. This file
//! proves the harder property: when **two** recoverers (or a recoverer racing
//! the original coordinator's late `Commit`) contend for the same transaction,
//! recovery ballots make them **converge deterministically** to one decision on
//! every replica — and a decision, once committed anywhere, is **never
//! reverted**. Each run is byte-reproducible from its seed.
//!
//! The mechanism under test (`AccordCore`): a replica promises the highest
//! recovery ballot it has seen for a txn and **rejects** a `Recover`/`Accept`
//! below it (reporting the higher ballot it promised); a superseded recoverer
//! retries strictly above it; `RecoverOk` aggregation re-proposes the
//! highest-ballot accepted value (or, absent one, max-ts/union-deps) so two
//! recoverers can only agree.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_consensus::{AccordNode, Key, TxnId};
use animus_sim::{NetConfig, SimEnv, Simulator};
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

/// Assert every replica that has committed `txn` agrees on `(execute_at, deps)`,
/// and that at least one replica has committed it. Returns the agreed
/// execution timestamp.
fn assert_committed_consistently(
    nodes: &[AccordNode<SimEnv>],
    txn: TxnId,
    seed: u64,
) -> animus_consensus::Timestamp {
    let mut agreed: Option<animus_consensus::Timestamp> = None;
    let mut agreed_deps: Option<BTreeSet<TxnId>> = None;
    let mut committed_count = 0;
    for (i, n) in nodes.iter().enumerate() {
        if let Some(e) = n.committed_execute_at(txn) {
            committed_count += 1;
            match agreed {
                None => {
                    agreed = Some(e);
                    agreed_deps = n.committed_deps(txn);
                }
                Some(prev) => assert_eq!(
                    prev, e,
                    "replica {i} committed txn at a different execute_at (seed={seed})"
                ),
            }
            assert_eq!(
                n.committed_deps(txn),
                agreed_deps,
                "replica {i} committed txn with different deps (seed={seed})"
            );
        }
    }
    assert!(
        committed_count > 0,
        "no replica committed the recovered txn (seed={seed})"
    );
    agreed.expect("committed_count > 0")
}

/// **Two recoverers, concurrently, converge to one decision.** The original
/// coordinator (node 0) broadcasts `PreAccept` to the whole replica set, which
/// the survivors witness (along with the txn's keys), then dies before it can
/// gather replies and commit — the realistic failover window. Then **two**
/// survivors (nodes 2 and 4) both start recovery of the same transaction at the
/// same virtual time. Their ballots differ (tiebroken by node id), so the
/// higher-ballot recoverer wins the duel and the lower one stands down; the
/// transaction commits at one `(execute_at, deps)` on every survivor.
#[test]
fn two_concurrent_recoverers_converge() {
    for seed in 0xBA11_0000..0xBA11_0010 {
        let (mut sim, nodes) = cluster(seed);

        let txn = nodes[0].submit(keys(&[7, 8]));
        // Let the PreAccept reach the survivors (so any recovery quorum learns
        // the keys), then isolate the coordinator. Depending on the seed's network
        // timing the coordinator may or may not have committed by now — either way
        // the property under test is that two concurrent recoverers converge to a
        // single decision (adopting an existing commit, or agreeing a fresh one).
        sim.run_for(Duration::from_millis(30));
        for peer in [1, 2, 3, 4] {
            sim.partition_pair(0, peer);
        }
        sim.run_for(Duration::from_millis(200));

        // Two survivors race to recover the same transaction.
        nodes[2].recover(txn);
        nodes[4].recover(txn);
        sim.run_for(Duration::from_secs(3));

        let agreed = assert_committed_consistently(&nodes, txn, seed);
        // The survivors that can reach a quorum all execute it, converged store.
        for &k in &[7u64, 8u64] {
            let mut writers = Vec::new();
            for n in &nodes {
                if n.is_applied(txn) {
                    writers.push(store_writer(n, k));
                }
            }
            assert!(
                writers.iter().all(|w| *w == Some(txn)),
                "an applied replica missed the recovered write on key {k} (seed={seed}); \
                 agreed execute_at={agreed:?}"
            );
        }
    }
}

/// **Coordinator failover under partition, then heal.** The original
/// coordinator is fully partitioned away (never commits). A survivor recovers
/// the transaction to a commit on the majority side; then the partition heals
/// and the original coordinator rejoins — it must **not** be able to commit a
/// *contradicting* decision (its stale `Ballot::ZERO` `Accept` is fenced by the
/// recoverer's promise), and the recovered decision stands on every replica.
#[test]
fn failover_under_partition_then_heal() {
    let seed = 0xBA11_0100;
    let (mut sim, nodes) = cluster(seed);

    let txn = nodes[0].submit(keys(&[5]));
    // PreAccept reaches the survivors, then the coordinator is isolated.
    sim.run_for(Duration::from_millis(30));
    for peer in [1, 2, 3, 4] {
        sim.partition_pair(0, peer);
    }
    sim.run_for(Duration::from_millis(200));

    // A survivor recovers it on the majority side {1,2,3,4}.
    nodes[1].recover(txn);
    sim.run_for(Duration::from_secs(2));

    let agreed = assert_committed_consistently(&nodes, txn, seed);
    // The recovered write landed on the applied survivors.
    for n in &nodes {
        if n.is_applied(txn) {
            assert_eq!(
                store_writer(n, 5),
                Some(txn),
                "an applied survivor missed the recovered write (seed={seed})"
            );
        }
    }
    // Snapshot the survivors' decision before the heal.
    let before: Vec<Option<animus_consensus::Timestamp>> =
        nodes.iter().map(|n| n.committed_execute_at(txn)).collect();

    // Heal: the original coordinator rejoins and its retry tick re-drives its
    // stale PreAccept/Accept at Ballot::ZERO. Those are fenced by the survivors'
    // recovery promise, so they cannot overturn the committed decision.
    for peer in [1, 2, 3, 4] {
        sim.heal(0, peer);
    }
    sim.run_for(Duration::from_secs(3));

    // No survivor's decision changed; the late coordinator did not revert it.
    for (i, n) in nodes.iter().enumerate() {
        if let Some(b) = before[i] {
            assert_eq!(
                n.committed_execute_at(txn),
                Some(b),
                "replica {i} reverted a committed decision after heal (seed={seed})"
            );
        }
    }
    // And the original coordinator, once healed, converges to the same decision.
    assert_eq!(
        assert_committed_consistently(&nodes, txn, seed),
        agreed,
        "post-heal agreement drifted from the recovered decision (seed={seed})"
    );
}

/// **Recover racing the original coordinator's `Commit`.** The original
/// coordinator commits normally (it is *not* dead), but a survivor spuriously
/// starts recovery at almost the same time. Recovery must discover the existing
/// commit (or re-propose a value that equals it) and **never** invent a
/// contradicting decision. The committed decision is identical everywhere.
#[test]
fn recover_racing_original_commit() {
    for seed in 0xBA11_0200..0xBA11_0210 {
        let (mut sim, nodes) = cluster(seed);

        let txn = nodes[0].submit(keys(&[3, 4]));
        // Let the commit get partway out, then race a recovery against it.
        sim.run_for(Duration::from_millis(40));
        let early = nodes[0].committed_execute_at(txn);
        nodes[3].recover(txn);
        sim.run_for(Duration::from_secs(2));

        let agreed = assert_committed_consistently(&nodes, txn, seed);
        // If the original coordinator had already committed before recovery
        // started, recovery must have adopted exactly that decision.
        if let Some(e) = early {
            assert_eq!(
                e, agreed,
                "recovery overturned an already-committed decision (seed={seed})"
            );
        }
        // Every replica that committed agrees, and the writes converged.
        for &k in &[3u64, 4u64] {
            let mut writers = Vec::new();
            for n in &nodes {
                if n.is_applied(txn) {
                    writers.push(store_writer(n, k));
                }
            }
            assert!(
                writers.iter().all(|w| *w == Some(txn)),
                "applied store diverged on key {k} (seed={seed})"
            );
        }
    }
}

/// **Message loss during recovery.** Under a lossy network a single recoverer's
/// `Recover`/`Accept`/`Commit` messages can drop; the driver's retry tick
/// re-drives them, and the transaction still commits consistently on a quorum.
/// Combined with a stalled original coordinator, this is failover under loss.
#[test]
fn recovery_survives_message_loss() {
    for seed in 0xBA11_0300..0xBA11_0310 {
        let (mut sim, nodes) = cluster(seed);

        let txn = nodes[0].submit(keys(&[11]));
        // PreAccept reaches the survivors, then the coordinator dies.
        sim.run_for(Duration::from_millis(30));
        for peer in [1, 2, 3, 4] {
            sim.partition_pair(0, peer);
        }
        // Now drop a meaningful fraction of the *recovery* traffic.
        let mut cfg = NetConfig::default();
        cfg.set_drop_prob(0.25);
        sim.set_net_config(cfg);
        sim.run_for(Duration::from_millis(200));

        // A survivor recovers under loss; retries re-drive dropped messages.
        nodes[2].recover(txn);
        sim.run_for(Duration::from_secs(8));

        let _ = assert_committed_consistently(&nodes, txn, seed);
        // Every replica that committed and applied agrees on the writer.
        let mut writers = Vec::new();
        for n in &nodes {
            if n.is_applied(txn) {
                writers.push(store_writer(n, 11));
            }
        }
        assert!(
            !writers.is_empty() && writers.iter().all(|w| *w == Some(txn)),
            "lossy recovery did not converge the store (seed={seed})"
        );
    }
}

/// A stale recoverer is **superseded** by a higher one and the loser does not
/// strand the transaction. Node 2 recovers first (lower id → lower ballot among
/// same-round recoverers); node 4 recovers an instant later. Both promised
/// ballots are visible to the cluster; the cluster converges to a single commit
/// regardless of which recoverer's ballot wins, and the phase reaches at least
/// `Committed` on a quorum.
#[test]
fn superseded_recoverer_does_not_strand() {
    let seed = 0xBA11_0400;
    let (mut sim, nodes) = cluster(seed);
    let txn = nodes[0].submit(keys(&[21]));
    sim.run_for(Duration::from_millis(30));
    for peer in [1, 2, 3, 4] {
        sim.partition_pair(0, peer);
    }
    sim.run_for(Duration::from_millis(200));

    // Node 2 starts, then node 4 starts (a duelling, slightly-later recoverer).
    nodes[2].recover(txn);
    sim.run_for(Duration::from_millis(5));
    nodes[4].recover(txn);
    sim.run_for(Duration::from_secs(4));

    let _ = assert_committed_consistently(&nodes, txn, seed);
    // A quorum reached at least Committed (none stuck mid-duel).
    let committed = nodes
        .iter()
        .filter(|n| n.committed_execute_at(txn).is_some())
        .count();
    assert!(
        committed >= 3,
        "fewer than a quorum committed after the duel (seed={seed}); committed={committed}"
    );
}

/// The duelling-recovery run is byte-reproducible from its seed.
#[test]
fn duelling_recovery_is_reproducible_from_seed() {
    let seed = 0xBA11_0500;
    let trace = |seed| {
        let (mut sim, nodes) = cluster(seed);
        let txn = nodes[0].submit(keys(&[7]));
        sim.run_for(Duration::from_millis(30));
        for peer in [1, 2, 3, 4] {
            sim.partition_pair(0, peer);
        }
        sim.run_for(Duration::from_millis(200));
        nodes[2].recover(txn);
        nodes[4].recover(txn);
        sim.run_for(Duration::from_secs(3));
        sim.trace_lines()
    };
    assert_eq!(
        trace(seed),
        trace(seed),
        "duelling-recovery trace not reproducible (seed={seed})"
    );
}
