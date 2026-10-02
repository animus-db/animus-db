//! Regression for the `Simulator`/`SimEnv` reference-cycle leak: a spawned
//! task's own captured `Env`/`Simulator` handle holds a strong `Arc` right
//! back into the executor's own task queue, so a *perpetual* task (one that
//! never resolves on its own — a Raft heartbeat loop, a reconciler tick
//! loop, any `loop { env.sleep(..).await; .. }` shape) keeps the whole
//! simulated world alive even after every external `Simulator`/`SimEnv`
//! handle a caller held has been dropped. `Simulator::shutdown` breaks the
//! cycle by draining the task queue directly, rather than relying on
//! reference counting ever reaching zero on its own (it structurally
//! cannot, absent an explicit drain — see `Simulator::shutdown`'s own doc).
//!
//! This is proven with a `Weak` handle taken before every strong handle is
//! dropped (`Simulator::downgrade`/`WeakSimulator::is_alive`) — the only way
//! to observe "is this simulated world still reachable" from outside once
//! nothing else is left to ask.

use std::time::Duration;

use animus_env::{Clock, EnvExt, nid};
use animus_sim::Simulator;

/// Spawn one never-resolving task (the shape every real perpetual driver
/// loop in this workspace takes) and return a `Weak` handle taken while the
/// simulator is still fully alive.
fn spawn_perpetual_task_and_downgrade(seed: u64) -> (Simulator, animus_sim::WeakSimulator) {
    let sim = Simulator::new(seed);
    let node = nid(0);
    let env = sim.env(node);

    // A perpetual task: it never returns `Poll::Ready`, exactly like a real
    // `animus_control::node::heartbeat_loop`/`tablet_host_reconciler_loop`/
    // `auto_split_loop` shape — every one of which is a `loop { .. env.sleep
    // (..).await .. }` with no terminating condition.
    let loop_env = env.clone();
    env.spawn_task(async move {
        loop {
            loop_env.sleep(Duration::from_millis(1)).await;
        }
    });

    let mut sim = sim;
    sim.run_for(Duration::from_millis(10));

    let weak = sim.downgrade();
    assert!(
        weak.is_alive(),
        "sanity: the simulator must still be alive while `sim`/`env` are held"
    );
    // Drop the extra outer `env` handle now — only `sim` itself (returned
    // below) remains as an *external* strong reference from here on.
    drop(env);
    (sim, weak)
}

/// Proves the bug's mechanism directly: with no explicit `shutdown()`,
/// dropping the caller's own last external `Simulator` handle does **not**
/// free the simulated world, because the perpetual task's own captured
/// `SimEnv` (parked inside the simulator's own task queue, which lives
/// inside the very state being reference-counted) still holds a strong
/// `Arc` back to it.
#[test]
fn a_perpetual_tasks_own_captured_env_keeps_the_world_alive_after_every_external_handle_drops() {
    let (sim, weak) = spawn_perpetual_task_and_downgrade(0xE1EA_5E17);

    drop(sim);

    assert!(
        weak.is_alive(),
        "known mechanism: a perpetual task's own captured `SimEnv` still owns \
         a strong `Arc` back into the executor's task queue (which lives \
         inside the very `Shared` being reference-counted), so nothing frees \
         until something explicitly drains that queue — see \
         `Simulator::shutdown`'s own doc"
    );
}

/// Proves the fix: `Simulator::shutdown()` drains the task queue, dropping
/// the perpetual task's own captured `SimEnv` (and with it, its strong
/// `Arc`) — so once every *external* handle is also dropped, the simulated
/// world is genuinely freed.
#[test]
fn simulator_shutdown_breaks_the_cycle_and_frees_the_world() {
    let (sim, weak) = spawn_perpetual_task_and_downgrade(0xE1EA_5E17);

    sim.shutdown();
    drop(sim);

    assert!(
        !weak.is_alive(),
        "`Simulator::shutdown()` should have dropped every spawned task's \
         future, breaking the cycle, so the simulated world is freed once \
         every external `Simulator`/`SimEnv` handle is also gone"
    );
}

/// `shutdown()` is idempotent and safe to call on any one of several
/// `Simulator` handles sharing the same world (the crate's own "`Simulator`
/// is `Clone`" precedent, e.g. a driver task carrying its own clone to call
/// fault-injection methods from inside a scenario script) — calling it
/// twice, or from a clone rather than the original, must not panic and must
/// still free the world once every handle is dropped.
#[test]
fn shutdown_is_idempotent_and_works_from_any_clone() {
    let (sim, weak) = spawn_perpetual_task_and_downgrade(0xC10E_5EED);
    let sim_clone = sim.clone();

    sim_clone.shutdown();
    sim.shutdown(); // second call, and from a different handle: must be a no-op.

    drop(sim_clone);
    drop(sim);

    assert!(
        !weak.is_alive(),
        "shutdown() must be safe to call more than once, and from any clone \
         of a shared `Simulator`, while still freeing the world"
    );
}

/// A finished (non-perpetual) task never causes this leak in the first
/// place — `shutdown()` is for the perpetual-task case specifically, and
/// must not be required for a scenario that only ever spawns tasks that
/// complete on their own.
#[test]
fn a_task_that_completes_on_its_own_does_not_leak_even_without_shutdown() {
    let sim = Simulator::new(0xF1E5_0001);
    let node = nid(0);
    let env = sim.env(node);

    let done_env = env.clone();
    env.spawn_task(async move {
        done_env.sleep(Duration::from_millis(1)).await;
        // Returns normally — this task is removed from the task queue once
        // it resolves, per `Simulator::poll_task`.
    });

    let mut sim = sim;
    sim.run_for(Duration::from_millis(5));
    let weak = sim.downgrade();

    drop(env);
    drop(sim);

    assert!(
        !weak.is_alive(),
        "a task that resolves on its own should already have dropped its \
         own captured `SimEnv` when it completed, well before either handle \
         here was ever dropped"
    );
}
