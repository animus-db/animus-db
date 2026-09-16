//! Regression coverage for issue #836: `Simulator::stop` must discard any
//! `Deliver` event already scheduled on the timeline for the stopped node —
//! a real process exit drops its open TCP connections, so a message sent
//! before `stop` but not yet delivered must never land in a later
//! incarnation's inbox on restart. See `crates/animus-sim/CLAUDE.md`'s
//! `stop` section for the full contract this proves.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{EnvExt, Network, nid};
use animus_sim::{NetConfig, Simulator};

/// A message sent before `stop` — still in flight on the timeline — must
/// never be observed by a fresh incarnation started on the same node id
/// after the stop. Before the fix, `stop` never touched `st.timeline`, so
/// `fire_event` found the target neither crashed nor partitioned once its
/// scheduled `deliver_at` arrived and pushed the envelope straight into the
/// new incarnation's inbox — exactly the connection-survives-a-process-exit
/// bug this test pins.
#[test]
fn stop_drops_a_message_already_in_flight_to_it() {
    let seed = seed_from_env(0x5701_0836);
    let mut sim = Simulator::new(seed);

    // A large, jitter-free delay so the message is provably still in flight
    // (not yet delivered) at the moment `stop` is called.
    let mut net = NetConfig::default();
    net.base_delay = Duration::from_millis(100);
    net.max_jitter = Duration::ZERO;
    sim.set_net_config(net);

    // Pre-stop incarnation of node 0: parked on recv, never gets to process
    // anything (it is stopped before the message arrives).
    let pre_stop_seen = Arc::new(Mutex::new(Vec::<u8>::new()));
    {
        let env = sim.env(nid(0));
        let out = Arc::clone(&pre_stop_seen);
        env.clone().spawn_task(async move {
            loop {
                let msg = env.recv().await;
                out.lock().unwrap().push(msg.payload[0]);
            }
        });
    }

    // Send the message from node 1; with a 100ms delay it cannot possibly
    // have been delivered by the time we advance only 10ms below.
    {
        let sender = sim.env(nid(1));
        sender.clone().spawn_task(async move {
            sender.send(nid(0), vec![42]).await;
        });
    }
    sim.run_for(Duration::from_millis(10));
    assert!(
        pre_stop_seen.lock().unwrap().is_empty(),
        "message must not have been delivered yet at t=10ms (seed={seed})"
    );

    // Stop node 0 while the message is still in flight (it was scheduled
    // for t=100ms, we're at t=10ms) — this must remove it from the timeline,
    // not merely tear down the task/inbox that would have received it.
    sim.stop(nid(0));

    // A fresh incarnation of node 0, started after the stop.
    let post_restart_seen = Arc::new(Mutex::new(Vec::<u8>::new()));
    {
        let env = sim.env(nid(0));
        let out = Arc::clone(&post_restart_seen);
        env.clone().spawn_task(async move {
            loop {
                let msg = env.recv().await;
                out.lock().unwrap().push(msg.payload[0]);
            }
        });
    }

    // Run well past the original message's t=100ms deliver_at.
    sim.run_for(Duration::from_millis(500));

    assert!(
        post_restart_seen.lock().unwrap().is_empty(),
        "a message sent before stop() must never surface in a later \
         incarnation's inbox — it should have been discarded at stop time, \
         not delivered after restart (seed={seed})"
    );
    assert!(
        pre_stop_seen.lock().unwrap().is_empty(),
        "the pre-stop task was torn down by stop() and must not have \
         observed anything either (seed={seed})"
    );

    let trace = sim.trace_lines();
    assert!(
        trace
            .iter()
            .any(|l| l.contains("DROP") && l.contains("(stopped)")),
        "expected a traced drop with reason \"stopped\" for the in-flight \
         message discarded at stop() time; trace:\n{}",
        trace.join("\n")
    );
    assert!(
        !trace
            .iter()
            .any(|l| l.contains("DELIVER") && l.contains("->n0")),
        "the discarded message must never appear as a DELIVER in the trace \
         (seed={seed}); trace:\n{}",
        trace.join("\n")
    );
}

/// A message sent by a *new* incarnation, after `stop`, must still be
/// delivered normally — `stop`'s timeline cleanup is a one-time snapshot at
/// the moment it's called, not a standing mute of the node id (unlike
/// `crashed`), so it must not interfere with traffic the fresh incarnation
/// sends or receives.
#[test]
fn stop_does_not_mute_a_fresh_incarnation() {
    let seed = seed_from_env(0x5701_0837);
    let mut sim = Simulator::new(seed);

    // Stop a node that never had any tasks or in-flight messages at all —
    // the degenerate case, just to establish stop() alone doesn't leave any
    // node-id-keyed mute behind.
    sim.stop(nid(0));

    let seen = Arc::new(Mutex::new(Vec::<u8>::new()));
    {
        let env = sim.env(nid(0));
        let out = Arc::clone(&seen);
        env.clone().spawn_task(async move {
            let msg = env.recv().await;
            out.lock().unwrap().push(msg.payload[0]);
        });
    }
    {
        let sender = sim.env(nid(1));
        sender.clone().spawn_task(async move {
            sender.send(nid(0), vec![7]).await;
        });
    }
    assert!(
        sim.run_until_quiescent(10_000),
        "post-stop traffic to a fresh incarnation should settle (seed={seed})"
    );
    assert_eq!(
        &*seen.lock().unwrap(),
        &[7],
        "a message sent to the node id after stop() (with no stale in-flight \
         delivery involved) must be delivered normally (seed={seed})"
    );
}

fn seed_from_env(default: u64) -> u64 {
    match std::env::var("ANIMUS_SEED") {
        Ok(s) => s.parse().unwrap_or(default),
        Err(_) => default,
    }
}
