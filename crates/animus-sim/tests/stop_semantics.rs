//! Regression coverage for issue #836: `Simulator::stop` must discard any
//! `Deliver` event already scheduled on the timeline for the stopped node —
//! a real process exit drops its open TCP connections, so a message sent
//! before `stop` but not yet delivered must never land in a later
//! incarnation's inbox on restart. See `crates/animus-sim/CLAUDE.md`'s
//! `stop` section for the full contract this proves.
//!
//! Also covers issue #841: `crash`/`stop`/`wipe_disk` node-prefix-range
//! (not full-)scan `disks`/`inboxes`/`recv_wakers` for the target node —
//! the `other_nodes_are_untouched_by` tests below prove that rewrite didn't
//! just get cheaper, it stayed correct: a fault op on one node must never
//! read or mutate another node's disk, inbox, or running task.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Disk, EnvExt, Network, NodeId, nid};
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

// --- Issue #841: a fault op on one node must not touch another node's
// disk, inbox, or task ownership. `nid(10)` == "n10" is always the fault
// target below: lexicographically it sorts *between* `nid(1)` == "n1" and
// `nid(2)` == "n2" (NodeId's own Ord), which is exactly the boundary a
// naive off-by-one range bound would get wrong. `nid(1)`/`nid(2)` are the
// bystanders each test asserts stayed untouched.

const FILE: &str = "wal";

/// Write durable bytes to `node`'s `FILE`, driving the write to completion.
fn write_disk(sim: &mut Simulator, node: NodeId, bytes: &'static [u8]) {
    let env = sim.env(node);
    env.clone().spawn_task(async move {
        env.append(FILE, bytes).await.unwrap();
        env.sync(FILE).await.unwrap();
    });
    assert!(sim.run_until_quiescent(10_000), "disk write should settle");
}

/// Read `node`'s `FILE` back, driving the read to completion.
fn read_disk(sim: &mut Simulator, node: NodeId) -> Vec<u8> {
    let out = Arc::new(Mutex::new(Vec::new()));
    let env = sim.env(node);
    let o = Arc::clone(&out);
    env.clone().spawn_task(async move {
        *o.lock().unwrap() = env.read(FILE).await.unwrap();
    });
    assert!(sim.run_until_quiescent(10_000), "disk read should settle");
    out.lock().unwrap().clone()
}

/// Spawn a perpetual echo loop on `node`: every message it receives is sent
/// straight back to whoever sent it. Used to prove `node`'s task (and its
/// `recv` registration) is still alive and polled after a fault op on some
/// *other* node.
fn spawn_echo(sim: &Simulator, node: NodeId) {
    let env = sim.env(node);
    env.clone().spawn_task(async move {
        loop {
            let msg = env.recv().await;
            env.send(msg.from, msg.payload).await;
        }
    });
}

/// Probe `target`'s echo loop from `prober` with a one-byte round trip and
/// assert the exact byte comes back.
fn assert_echo_round_trips(sim: &mut Simulator, prober: NodeId, target: NodeId, tag: u8) {
    let seen = Arc::new(Mutex::new(Vec::<u8>::new()));
    let env = sim.env(prober);
    let out = Arc::clone(&seen);
    env.clone().spawn_task(async move {
        env.send(target, vec![tag]).await;
        let reply = env.recv().await;
        out.lock().unwrap().push(reply.payload[0]);
    });
    assert!(
        sim.run_until_quiescent(10_000),
        "echo round trip (tag={tag}) should settle"
    );
    assert_eq!(
        &*seen.lock().unwrap(),
        &[tag],
        "target's echo task must still be alive and responsive (tag={tag})"
    );
}

/// Shared scaffold for the three `other_nodes_are_untouched_by_*` tests
/// below: set up bystanders `nid(1)`/`nid(2)` (durable disk content, a live
/// echo task on `nid(1)`, one message pre-queued — unconsumed — in
/// `nid(2)`'s inbox) and matching durable disk content on the fault target
/// `nid(10)`, run `fault` on `nid(10)`, then assert every bystander's disk,
/// inbox and task are exactly as they were.
fn assert_fault_op_leaves_other_nodes_untouched(seed: u64, fault: impl FnOnce(&mut Simulator)) {
    let mut sim = Simulator::new(seed);
    let a = nid(1); // bystander: has a live task through the fault
    let b = nid(10); // fault target: sorts between `a` and `c`
    let c = nid(2); // bystander: has an unconsumed pre-queued inbox message
    let prober = nid(99);

    write_disk(&mut sim, a.clone(), b"node-a-durable-bytes");
    write_disk(&mut sim, b.clone(), b"node-b-durable-bytes");
    write_disk(&mut sim, c.clone(), b"node-c-durable-bytes");

    spawn_echo(&sim, a.clone());
    assert_echo_round_trips(&mut sim, prober.clone(), a.clone(), 1);

    // Queue one message in `c`'s inbox with no task consuming it yet, so it
    // sits in `st.inboxes`/is the only entry for `c` while the fault op on
    // `b` range-scans past it.
    {
        let env = sim.env(prober.clone());
        let target = c.clone();
        env.clone().spawn_task(async move {
            env.send(target, vec![0xC1]).await;
        });
        assert!(
            sim.run_until_quiescent(10_000),
            "pre-queued send to c should settle"
        );
    }

    fault(&mut sim);

    // `a`'s disk, still exactly what was written.
    assert_eq!(
        read_disk(&mut sim, a.clone()),
        b"node-a-durable-bytes",
        "bystander a's disk must be untouched by a fault op on b (seed={seed})"
    );
    // `c`'s disk, still exactly what was written.
    assert_eq!(
        read_disk(&mut sim, c.clone()),
        b"node-c-durable-bytes",
        "bystander c's disk must be untouched by a fault op on b (seed={seed})"
    );

    // `a`'s task (and task_owner/recv_wakers entries) is still alive.
    assert_echo_round_trips(&mut sim, prober.clone(), a.clone(), 2);

    // `c`'s pre-queued inbox message survived exactly, in order: spawn its
    // echo loop only now and check the very first thing it gets back is
    // the byte queued *before* the fault op on b.
    spawn_echo(&sim, c.clone());
    let first = Arc::new(Mutex::new(Vec::<u8>::new()));
    {
        let env = sim.env(prober.clone());
        let out = Arc::clone(&first);
        env.clone().spawn_task(async move {
            let reply = env.recv().await;
            out.lock().unwrap().push(reply.payload[0]);
        });
        assert!(
            sim.run_until_quiescent(10_000),
            "c's pre-queued message should echo back (seed={seed})"
        );
    }
    assert_eq!(
        &*first.lock().unwrap(),
        &[0xC1],
        "c's inbox entry queued before the fault on b must survive it \
         unchanged (seed={seed})"
    );

    // And a fresh round trip to `c` still works too.
    assert_echo_round_trips(&mut sim, prober, c, 3);
}

#[test]
fn other_nodes_are_untouched_by_crash() {
    let seed = seed_from_env(0x5701_0841);
    assert_fault_op_leaves_other_nodes_untouched(seed, |sim| sim.crash(nid(10)));
}

#[test]
fn other_nodes_are_untouched_by_stop() {
    let seed = seed_from_env(0x5701_0842);
    assert_fault_op_leaves_other_nodes_untouched(seed, |sim| sim.stop(nid(10)));
}

#[test]
fn other_nodes_are_untouched_by_wipe_disk() {
    let seed = seed_from_env(0x5701_0843);
    assert_fault_op_leaves_other_nodes_untouched(seed, |sim| sim.wipe_disk(nid(10)));
}
