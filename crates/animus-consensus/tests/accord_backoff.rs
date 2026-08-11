//! ADR 0011 acceptance tests for **adaptive retry backoff**.
//!
//! The driver's retry tick re-sends the un-acknowledged messages of an in-flight
//! round (so a dropped fire-and-forget `send` does not strand a transaction), but
//! the interval is now **exponential backoff** rather than fixed: it starts at a
//! base interval and doubles (capped) while a round stays stuck, and resets to
//! the base the moment the round makes progress or completes. So a transaction
//! that genuinely cannot reach a quorum is retried ever less often — far fewer
//! redundant sends than a fixed-interval re-send — while a transient drop is
//! still recovered promptly and the transaction still converges.
//!
//! The whole run is byte-reproducible from its seed.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_consensus::{AccordNode, Key};
use animus_env::nid;
use animus_sim::{NetConfig, SimEnv, Simulator};

const NODES: [u64; 3] = [0, 1, 2];

fn keys(ks: &[Key]) -> BTreeSet<Key> {
    ks.iter().copied().collect()
}

fn cluster(seed: u64) -> (Simulator, Vec<AccordNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| AccordNode::start(sim.env(nid(id)), NODES.iter().copied().map(nid).collect()))
        .collect();
    (sim, nodes)
}

/// Count the protocol `SEND`s originating from node `from` in the trace.
fn sends_from(sim: &Simulator, from: u64) -> usize {
    let needle = format!("SEND {}->", nid(from));
    sim.trace_lines()
        .iter()
        .filter(|l| l.contains(&needle))
        .count()
}

/// A coordinator fully partitioned from its peers cannot gather a quorum, so its
/// transaction stays stuck and the retry tick keeps re-sending. With **backoff**
/// the number of re-sends over a long window is far **sub-linear** in the window
/// length — dramatically fewer than a fixed base-interval re-send would emit.
#[test]
fn backoff_cuts_redundant_sends_while_stuck() {
    let seed = 0xBAC0_0001;
    let (mut sim, nodes) = cluster(seed);

    // Isolate the coordinator (node 0) from both peers: its PreAccept can never
    // be answered, so the round never completes and only the retry tick re-sends.
    sim.partition_pair(nid(0), nid(1));
    sim.partition_pair(nid(0), nid(2));

    nodes[0].submit(keys(&[1]));

    // Run a long window. Base interval is 200ms; a *fixed* re-send would fire
    // ~ window/200ms times, each shipping 2 messages (to the two peers).
    let window = Duration::from_secs(30);
    sim.run_for(window);

    let sends = sends_from(&sim, 0);

    // A fixed 200ms re-send over 30s would emit ~150 ticks * 2 peers = ~300
    // sends. Backoff (200ms doubling to a 1.6s cap) emits far fewer — assert a
    // generous ceiling well below the fixed-interval count, so the test asserts
    // the *backoff*, not an exact schedule.
    assert!(
        sends < 80,
        "expected backoff to keep sends well below the fixed-interval count, \
         got {sends} sends from the stuck coordinator over 30s (seed={seed})"
    );
    // It is still actively retrying (not wedged at zero after the first burst).
    assert!(
        sends >= 4,
        "the retry tick should still fire several times under backoff, \
         got only {sends} (seed={seed})"
    );
}

/// Backoff does not cost liveness: once the partition heals, the stuck
/// transaction still commits and executes on every replica — the interval reset
/// on progress means recovery is prompt, not throttled by the backed-off delay.
#[test]
fn backoff_still_converges_after_a_heal() {
    let seed = 0xBAC0_0002;
    let (mut sim, nodes) = cluster(seed);

    sim.partition_pair(nid(0), nid(1));
    sim.partition_pair(nid(0), nid(2));

    let txn = nodes[0].submit(keys(&[42]));
    // Let the coordinator back off well into its capped interval while isolated.
    sim.run_for(Duration::from_secs(10));
    for n in &nodes {
        assert!(
            !n.is_applied(txn.clone()),
            "must be stuck while partitioned"
        );
    }

    // Heal and let the next retry carry it home.
    sim.heal(nid(0), nid(1));
    sim.heal(nid(0), nid(2));
    sim.run_for(Duration::from_secs(10));

    for (i, n) in nodes.iter().enumerate() {
        assert!(
            n.is_applied(txn.clone()),
            "node {i} never executed after heal — backoff must not strand it (seed={seed})"
        );
        assert_eq!(
            n.committed_execute_at(txn.clone()),
            nodes[0].committed_execute_at(txn.clone()),
            "node {i} committed at a different timestamp (seed={seed})"
        );
    }
}

/// Under ordinary (lossy but unpartitioned) operation, backoff still converges:
/// progress resets the interval, so a transient drop is recovered at the base
/// interval and the transaction commits everywhere across a seed sweep.
#[test]
fn backoff_converges_under_loss_across_seeds() {
    for seed in 0xBAC0_1000..0xBAC0_1010 {
        let sim = Simulator::new(seed);
        let mut cfg = NetConfig::default();
        cfg.set_drop_prob(0.3);
        sim.set_net_config(cfg);
        let nodes: Vec<AccordNode<SimEnv>> = NODES
            .iter()
            .map(|&id| {
                AccordNode::start(sim.env(nid(id)), NODES.iter().copied().map(nid).collect())
            })
            .collect();
        let mut sim = sim;

        let txn = nodes[0].submit(keys(&[5]));
        sim.run_for(Duration::from_secs(40));
        for (i, n) in nodes.iter().enumerate() {
            assert!(
                n.is_applied(txn.clone()),
                "node {i} never executed under loss with backoff (seed={seed})"
            );
        }
    }
}

/// The backoff run is byte-reproducible from its seed (the backoff state is a
/// plain local; the timer is a deterministic `Env` timer).
#[test]
fn backoff_run_is_reproducible_from_seed() {
    let seed = 0xBAC0_0003;
    let trace = |seed| {
        let (mut sim, nodes) = cluster(seed);
        sim.partition_pair(nid(0), nid(1));
        sim.partition_pair(nid(0), nid(2));
        nodes[0].submit(keys(&[1]));
        sim.run_for(Duration::from_secs(15));
        sim.trace_lines()
    };
    assert_eq!(trace(seed), trace(seed), "backoff trace not reproducible");
}
