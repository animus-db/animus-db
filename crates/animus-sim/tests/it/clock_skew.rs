//! Per-node clock skew (`Simulator::set_clock_skew_for`, ADR 0018 §2 sim
//! support): opt-in, default-zero, read-side-only skew of `Clock::now()`.
//!
//! An HLC (ADR 0018) has to stay correct even when different nodes' physical
//! clocks disagree; this is the sim knob that lets a test model that
//! disagreement while keeping every existing test byte-identical (default
//! skew is zero everywhere).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Clock, Env, EnvExt, Network, nid};
use animus_sim::Simulator;

/// A node's skewed `now()` differs from the simulator's global clock by
/// exactly its configured skew; an unskewed node still tracks the global
/// clock exactly.
#[test]
fn skew_offsets_now_reads_per_node() {
    let mut sim = Simulator::new(1);
    let a = sim.env(nid(0));
    let b = sim.env(nid(1));
    let c = sim.env(nid(2));

    sim.set_clock_skew_for(nid(0), 50_000_000); // +50ms
    sim.set_clock_skew_for(nid(1), -10_000_000); // -10ms

    // Advance real (unskewed) time well past the negative skew so the b's
    // clamp-at-zero floor (tested separately below) doesn't interfere here.
    sim.run_for(Duration::from_millis(100));

    let global = sim.now().0;
    assert_eq!(
        a.now().0,
        global + 50_000_000,
        "positive skew must offset now()"
    );
    assert_eq!(
        b.now().0,
        global - 10_000_000,
        "negative skew must offset now()"
    );
    assert_eq!(
        c.now().0,
        global,
        "an unskewed node must track the global clock exactly"
    );
}

/// A large negative skew must clamp at 0 near time zero rather than
/// underflowing (wrapping) the underlying `u64`.
#[test]
fn negative_skew_clamps_at_zero_near_time_zero() {
    let sim = Simulator::new(2);
    let a = sim.env(nid(0));
    sim.set_clock_skew_for(nid(0), -1);
    assert_eq!(
        sim.now().0,
        0,
        "sanity: sim starts at time zero before any run_for/run_until"
    );
    assert_eq!(
        a.now().0,
        0,
        "negative skew must clamp at 0, not underflow, when the global clock is at/near 0"
    );

    // A skew far larger in magnitude than any elapsed time must still clamp,
    // not wrap around to a huge u64.
    let sim2 = Simulator::new(3);
    let b = sim2.env(nid(0));
    sim2.set_clock_skew_for(nid(0), i64::MIN);
    assert_eq!(b.now().0, 0);
}

const N: u64 = 3;
const MAX_HOPS: u8 = 6;

/// Build a ring scenario (mirrors `tests/determinism.rs`'s), additionally
/// recording each hop's `(node, now())` reading so a run's observed skewed
/// clock sequence can be compared across repeats.
fn build_ring(sim: &Simulator, readings: Arc<Mutex<Vec<(String, u64)>>>) {
    for id in 0..N {
        let env = sim.env(nid(id));
        let readings = Arc::clone(&readings);
        env.clone().spawn_task(async move {
            loop {
                let msg = env.recv().await;
                let hop = msg.payload[0];
                readings
                    .lock()
                    .expect("readings mutex poisoned")
                    .push((env.node_id().to_string(), env.now().0));
                if hop >= MAX_HOPS {
                    continue; // park: token has finished its journey
                }
                env.sleep(Duration::from_micros(100)).await;
                let cur: u64 = env
                    .node_id()
                    .as_str()
                    .trim_start_matches('n')
                    .parse()
                    .expect("ring node ids are nid(n)-formatted");
                let next = nid((cur + 1) % N);
                env.send(next, vec![hop + 1]).await;
            }
        });
    }
    let starter = sim.env(nid(0));
    starter.clone().spawn_task(async move {
        starter.sleep(Duration::from_millis(1)).await;
        starter.send(nid(1), vec![0]).await;
    });
}

/// Run the skewed ring scenario for `seed`, returning the trace and the
/// recorded `(node, now())` sequence.
fn run_skewed_ring(seed: u64) -> (Vec<String>, Vec<(String, u64)>) {
    let mut sim = Simulator::new(seed);
    sim.set_clock_skew_for(nid(0), 30_000_000);
    sim.set_clock_skew_for(nid(2), -7_000_000);
    let readings = Arc::new(Mutex::new(Vec::new()));
    build_ring(&sim, Arc::clone(&readings));
    assert!(
        sim.run_until_quiescent(100_000),
        "ring did not reach quiescence (seed={})",
        sim.seed()
    );
    let recorded = readings.lock().expect("readings mutex poisoned").clone();
    (sim.trace_lines(), recorded)
}

/// Same seed + same clock-skew script must reproduce byte-identical traces
/// and an identical observed `now()` sequence — clock skew is set
/// explicitly, draws no RNG, and adds no timeline event, so it must not
/// perturb the determinism guarantee (ADR 0003).
#[test]
fn same_seed_and_skew_script_is_deterministic() {
    let (trace_1, readings_1) = run_skewed_ring(42);
    let (trace_2, readings_2) = run_skewed_ring(42);
    assert_eq!(
        trace_1, trace_2,
        "trace must be byte-identical for the same seed+skew script"
    );
    assert_eq!(
        readings_1, readings_2,
        "the observed now() sequence must be identical for the same seed+skew script"
    );
    // And a different seed's skewed run should very likely diverge in trace
    // (sanity that the harness is actually exercising randomness elsewhere,
    // not asserting a tautology).
    let (trace_3, _) = run_skewed_ring(43);
    assert_ne!(
        trace_1, trace_3,
        "a different seed should not coincidentally match"
    );
}

/// `Simulator::set_clock_drift_for`: a node's clock diverges progressively
/// with elapsed virtual time, at exactly the configured ppm rate, layered on
/// top of any static skew — and an undrifted node still tracks the global
/// clock exactly.
#[test]
fn drift_widens_the_observed_skew_over_elapsed_time() {
    let mut sim = Simulator::new(5);
    let fast = sim.env(nid(0)); // +100 ppm: runs fast
    let slow = sim.env(nid(1)); // -100 ppm: runs slow
    let plain = sim.env(nid(2)); // no drift, no skew

    sim.set_clock_drift_for(nid(0), 100);
    sim.set_clock_drift_for(nid(1), -100);

    // At the drift's own start instant, elapsed = 0, so no divergence yet.
    assert_eq!(fast.now().0, sim.now().0, "no elapsed time yet: no drift");
    assert_eq!(slow.now().0, sim.now().0, "no elapsed time yet: no drift");

    // Advance one second of virtual time (1_000_000_000 ns). At 100 ppm that
    // is exactly 100_000 ns of accumulated drift.
    sim.run_for(Duration::from_secs(1));
    let global = sim.now().0;
    assert_eq!(
        fast.now().0,
        global + 100_000,
        "positive drift must add 100ppm of elapsed time"
    );
    assert_eq!(
        slow.now().0,
        global - 100_000,
        "negative drift must subtract 100ppm of elapsed time"
    );
    assert_eq!(
        plain.now().0,
        global,
        "an undrifted, unskewed node must track the global clock exactly"
    );

    // Advancing a second more doubles the accumulated drift.
    sim.run_for(Duration::from_secs(1));
    let global2 = sim.now().0;
    assert_eq!(fast.now().0, global2 + 200_000);
    assert_eq!(slow.now().0, global2 - 200_000);
}

/// Drift composes with a static skew (drift is layered on top, not a
/// replacement) — and is deterministic and reproducible from the seed, same
/// as static skew.
#[test]
fn drift_layers_on_top_of_static_skew_and_is_deterministic() {
    fn run(seed: u64) -> (u64, Vec<String>) {
        let mut sim = Simulator::new(seed);
        let a = sim.env(nid(0));
        sim.set_clock_skew_for(nid(0), 1_000_000); // +1ms static
        sim.set_clock_drift_for(nid(0), 50); // +50ppm drift on top
        sim.run_for(Duration::from_secs(2)); // 2s * 50ppm = 100_000ns drift
        (a.now().0, sim.trace_lines())
    }

    let seed = 0xD817_7001;
    let (now_a, trace_a) = run(seed);
    let (now_b, trace_b) = run(seed);
    assert_eq!(now_a, now_b, "drift+skew composition must be reproducible");
    assert_eq!(trace_a, trace_b, "trace must be byte-identical");

    let mut sim = Simulator::new(seed);
    sim.run_for(Duration::from_secs(2));
    let global = sim.now().0;
    assert_eq!(
        now_a,
        global + 1_000_000 + 100_000,
        "drift must add on top of the static skew, not replace it"
    );
}

/// With no `set_clock_drift_for` call, `now()` is byte-identical to a
/// simulator with no drift model at all — the same default-off contract
/// static skew already has.
#[test]
fn drift_defaults_to_no_divergence() {
    let mut sim = Simulator::new(6);
    let a = sim.env(nid(0));
    sim.run_for(Duration::from_secs(10));
    assert_eq!(
        a.now().0,
        sim.now().0,
        "an undrifted node must never diverge from the global clock"
    );
}
