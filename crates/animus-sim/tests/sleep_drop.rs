//! Regression coverage for issue #837: a `Sleep` future dropped before its
//! deadline (the losing branch of a `select`, which production code does
//! everywhere) must not leave a phantom timer on the shared timeline. See
//! `crates/animus-sim/CLAUDE.md`'s timer section for the full contract this
//! proves, including why `stop`/`shutdown` needed their own fix alongside
//! `Sleep`'s new `Drop` impl (deferring the drop of a removed task's future
//! until after the state lock is released, so a live `Sleep`'s own
//! lock-reentrant cleanup can't deadlock against them).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Clock, EnvExt, nid};
use animus_sim::Simulator;

/// A task that races a long sleep against an already-ready future (the
/// `select` shape production code uses everywhere — e.g.
/// `animusd/src/write_path.rs`) must leave no trace on the timeline once the
/// race is decided: the losing `Sleep` is dropped before its deadline, and
/// before the fix nothing ever removed its scheduled `Event::Timer` from
/// `st.timeline` (no `Drop` impl existed at all) — so the whole task
/// resolves in the very first synchronous drain, with **zero** timeline
/// events ever fired.
#[test]
fn dropped_sleep_leaves_no_phantom_timer_in_the_timeline() {
    let seed = seed_from_env(0x5701_0837);
    let mut sim = Simulator::new(seed);

    let done = Arc::new(Mutex::new(false));
    {
        let env = sim.env(nid(0));
        let out = Arc::clone(&done);
        env.clone().spawn_task(async move {
            // A deadline far in virtual-time future — if this ever actually
            // fires (rather than being dropped as the race's loser), the
            // assertions below would trivially fail (the task setting
            // `done` only happens after the race resolves either way, but a
            // firing sleep would burn a real timeline step + RNG-free timer
            // fire, which `timer_fires` below would catch).
            let sleep_fut = env.sleep(Duration::from_secs(3600));
            let ready_fut = futures::future::ready(());
            let _ = futures::future::select(sleep_fut, ready_fut).await;
            *out.lock().unwrap() = true;
        });
    }

    let before = sim.stats();
    let quiescent = sim.run_until_quiescent(0);
    let after = sim.stats();

    assert!(
        *done.lock().unwrap(),
        "the task must have run to completion in the initial synchronous \
         drain (seed={seed})"
    );
    assert!(
        quiescent,
        "the run must be quiescent with zero timeline steps fired — a \
         phantom timer for the dropped sleep would force at least one \
         (seed={seed})"
    );
    assert_eq!(
        after.timer_fires, before.timer_fires,
        "no timeline event (timer or delivery) should have fired at all — \
         the dropped sleep's own timer must have been removed at drop time, \
         not left to fire at its stale deadline (seed={seed})"
    );

    let trace = sim.trace_lines();
    assert!(
        !trace.iter().any(|l| l.contains("TIMER")),
        "no TIMER trace line should exist for a sleep that was dropped \
         before its deadline (seed={seed}); trace:\n{}",
        trace.join("\n")
    );
}

/// `Simulator::stop` on a node whose only task is parked mid-`sleep` must
/// not deadlock: `stop` removes the task (and, with it, the live `Sleep`
/// embedded in its suspended future), and that `Sleep`'s own `Drop` impl
/// re-locks the simulator's shared state to clean up its timer. `stop` must
/// not still be holding that same lock when the drop runs.
#[test]
fn stopping_a_node_parked_on_sleep_does_not_deadlock() {
    let seed = seed_from_env(0x5701_0838);
    let mut sim = Simulator::new(seed);

    {
        let env = sim.env(nid(0));
        env.clone().spawn_task(async move {
            env.sleep(Duration::from_secs(3600)).await;
        });
    }
    // Let the task park on its sleep (registering a live timer) before
    // stopping it.
    sim.run_for(Duration::from_millis(1));

    // If `stop` regressed to dropping the removed task's future while still
    // holding the state lock, this call would hang forever — the test
    // finishing at all (within the harness's normal timeout) is the proof.
    sim.stop(nid(0));

    let trace = sim.trace_lines();
    assert!(
        !trace.iter().any(|l| l.contains("TIMER")),
        "the parked sleep's timer must never fire once its node is \
         stopped (seed={seed}); trace:\n{}",
        trace.join("\n")
    );
}

/// `Simulator::shutdown` on a simulation with a perpetual `loop { sleep(..)
/// .await; .. }` task (the shape every real background driver takes) must
/// not deadlock for the same reason as `stop` above.
#[test]
fn shutdown_with_a_live_sleep_does_not_deadlock() {
    let seed = seed_from_env(0x5701_0839);
    let mut sim = Simulator::new(seed);

    {
        let env = sim.env(nid(0));
        env.clone().spawn_task(async move {
            loop {
                env.sleep(Duration::from_secs(3600)).await;
            }
        });
    }
    sim.run_for(Duration::from_millis(1));

    // Same proof-by-completion as the `stop` test above: reaching this line
    // at all (rather than hanging forever) is what the test proves.
    sim.shutdown();
    let _ = seed;
}

fn seed_from_env(default: u64) -> u64 {
    match std::env::var("ANIMUS_SEED") {
        Ok(s) => s.parse().unwrap_or(default),
        Err(_) => default,
    }
}
