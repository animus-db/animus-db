//! `SimEnv::metrics()` is a recording sink scoped to the (Simulator, node):
//! the trait default is a process-wide no-op sink, which would couple every
//! simulator in a test binary (so before/after counter deltas read under plain
//! `cargo test` saw other tests' increments).

use animus_env::{Env, Metric, nid};
use animus_sim::Simulator;

#[test]
fn metrics_are_scoped_per_simulator_and_per_node() {
    let sim_a = Simulator::new(0xA1);
    let sim_b = Simulator::new(0xB2);

    let a1 = sim_a.env(nid(1));
    let a2 = sim_a.env(nid(2));
    let b1 = sim_b.env(nid(1));

    for _ in 0..3 {
        a1.metrics().incr(Metric::CpEngineRebuilt);
    }
    for _ in 0..5 {
        b1.metrics().incr(Metric::CpEngineRebuilt);
    }

    assert_eq!(a1.metrics().get(Metric::CpEngineRebuilt), 3, "sim A node 1");
    assert_eq!(b1.metrics().get(Metric::CpEngineRebuilt), 5, "sim B node 1");
    assert_eq!(
        a2.metrics().get(Metric::CpEngineRebuilt),
        0,
        "sim A node 2 must not see node 1's increments"
    );
}

#[test]
fn handles_for_the_same_node_share_one_sink() {
    let sim = Simulator::new(0xC3);
    let first = sim.env(nid(1));
    let second = sim.env(nid(1));
    let cloned = first.clone();

    first.metrics().incr(Metric::CpEngineRebuilt);
    second.metrics().incr_by(Metric::CpEngineRebuilt, 2);

    assert_eq!(cloned.metrics().get(Metric::CpEngineRebuilt), 3);
    assert_eq!(second.metrics().get(Metric::CpEngineRebuilt), 3);
    assert!(first.metrics().is_same_sink(&second.metrics()));
}
