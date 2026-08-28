//! Process pause (`Simulator::pause`): alive but frozen for a bounded span of
//! virtual time, then resumes on its own with full state intact — distinct
//! from both `crash` (drops volatile state) and `stop` (removes tasks).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Clock, EnvExt, Network, nid};
use animus_sim::Simulator;

/// A sleep timer due while its owning node is paused does not fire until the
/// node resumes — it is deferred, not cancelled or lost.
#[test]
fn pause_defers_a_timer_until_resume() {
    let seed = 0xBA05_E001u32 as u64;
    let mut sim = Simulator::new(seed);
    let pause_dur = Duration::from_millis(300);
    let node = nid(0);

    sim.pause(node.clone(), pause_dur);

    let observed = Arc::new(Mutex::new(None));
    {
        let env = sim.env(node.clone());
        let out = Arc::clone(&observed);
        env.clone().spawn_task(async move {
            // Scheduled to fire at t=50ms, well inside the 300ms pause.
            env.sleep(Duration::from_millis(50)).await;
            *out.lock().unwrap() = Some(env.now().0);
        });
    }
    sim.run_for(Duration::from_millis(400));

    let fired_at = observed
        .lock()
        .unwrap()
        .expect("timer must eventually fire");
    assert!(
        fired_at >= pause_dur.as_nanos() as u64,
        "seed={seed}: a timer due mid-pause must not fire before the pause ends \
         (fired at {fired_at}ns, pause ends at {}ns)",
        pause_dur.as_nanos()
    );
}

/// A message sent to a paused node while it is paused queues — it is
/// delivered only once the node resumes, never dropped.
#[test]
fn pause_queues_inbound_messages_until_resume() {
    let seed = 0xBA05_E002u32 as u64;
    let mut sim = Simulator::new(seed);
    let pause_dur = Duration::from_millis(300);
    let receiver = nid(1);

    sim.pause(receiver.clone(), pause_dur);

    let observed = Arc::new(Mutex::new(None));
    {
        let env = sim.env(receiver.clone());
        let out = Arc::clone(&observed);
        env.clone().spawn_task(async move {
            let msg = env.recv().await;
            *out.lock().unwrap() = Some((env.now().0, msg.payload[0]));
        });
    }
    {
        let sender = sim.env(nid(0));
        sender.clone().spawn_task(async move {
            // Delivered quickly under the default NetConfig — well within
            // the pause window if pause were not respected.
            sender.send(receiver.clone(), vec![42]).await;
        });
    }
    sim.run_for(Duration::from_millis(400));

    let (received_at, payload) = observed
        .lock()
        .unwrap()
        .expect("the queued message must eventually be delivered, not dropped");
    assert_eq!(payload, 42, "seed={seed}: payload must survive the pause");
    assert!(
        received_at >= pause_dur.as_nanos() as u64,
        "seed={seed}: a message addressed to a paused node must not be visible \
         before the pause ends (received at {received_at}ns, pause ends at {}ns)",
        pause_dur.as_nanos()
    );
}

/// A send made by an already-paused node (the ready-queued-at-pause-time edge
/// case: the task runs synchronously and calls `send` immediately, with
/// nothing to block it) does not leave before the node resumes.
#[test]
fn pause_holds_back_an_outbound_send_until_resume() {
    let seed = 0xBA05_E003u32 as u64;
    let mut sim = Simulator::new(seed);
    let pause_dur = Duration::from_millis(500);
    let sender_node = nid(0);

    sim.pause(sender_node.clone(), pause_dur);
    {
        let env = sim.env(sender_node.clone());
        env.clone().spawn_task(async move {
            // No await before this: the task runs in the very first drain,
            // synchronously, regardless of the node being paused.
            env.send(nid(1), b"hi".to_vec()).await;
        });
    }
    sim.run_for(Duration::from_millis(600));

    let trace = sim.trace_lines();
    let deliver_line = trace
        .iter()
        .find(|l| l.contains("DELIVER"))
        .unwrap_or_else(|| panic!("seed={seed}: message must eventually be delivered: {trace:?}"));
    let t: u64 = deliver_line
        .split_whitespace()
        .next()
        .and_then(|s| s.strip_prefix("t="))
        .and_then(|s| s.parse().ok())
        .expect("trace line must start with t=<nanos>");
    assert!(
        t >= pause_dur.as_nanos() as u64,
        "seed={seed}: a paused sender's message must not leave before it resumes \
         (delivered at t={t}, pause ends at {}) — line: {deliver_line}",
        pause_dur.as_nanos()
    );
}

/// `pause` is deterministic and traced: the same seed + pause script
/// reproduces a byte-identical trace, and the pause itself is visible in it.
#[test]
fn pause_is_deterministic_and_traced() {
    fn run(seed: u64) -> Vec<String> {
        let mut sim = Simulator::new(seed);
        let node = nid(0);
        sim.pause(node.clone(), Duration::from_millis(100));
        let env = sim.env(node);
        env.clone().spawn_task(async move {
            env.sleep(Duration::from_millis(10)).await;
        });
        sim.run_for(Duration::from_millis(200));
        sim.trace_lines()
    }

    let seed = 0xBA05_E004u32 as u64;
    let a = run(seed);
    let b = run(seed);
    assert_eq!(a, b, "seed={seed}: pause script must be reproducible");
    assert!(
        a.iter()
            .any(|l| l.contains("PAUSE") && l.contains("node=n0")),
        "seed={seed}: pause must be traced: {a:?}"
    );
}

/// A node that is never paused behaves exactly as before: `pause` on other
/// nodes must not perturb its own timer/delivery schedule.
#[test]
fn unpaused_nodes_are_unaffected_by_another_nodes_pause() {
    let seed = 0xBA05_E005u32 as u64;
    let mut sim = Simulator::new(seed);
    sim.pause(nid(0), Duration::from_millis(200));

    let observed = Arc::new(Mutex::new(None));
    {
        let env = sim.env(nid(1));
        let out = Arc::clone(&observed);
        env.clone().spawn_task(async move {
            env.sleep(Duration::from_millis(10)).await;
            *out.lock().unwrap() = Some(env.now().0);
        });
    }
    sim.run_for(Duration::from_millis(50));

    let fired_at = observed
        .lock()
        .unwrap()
        .expect("an unpaused node's timer must fire on schedule");
    assert!(
        fired_at < Duration::from_millis(200).as_nanos() as u64,
        "seed={seed}: node 1's timer must not be delayed by node 0's pause \
         (fired at {fired_at}ns)"
    );
}
