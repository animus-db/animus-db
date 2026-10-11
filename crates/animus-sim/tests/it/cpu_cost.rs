//! Virtual-CPU starvation (`Simulator::set_cpu_cost`, ADR 0078 "step zero"):
//! a node's events are serialized and each costs `per_event` of virtual time,
//! so an offered load above capacity makes its timers late and its inbox back
//! up. Default-off, deterministic, traced.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Clock, EnvExt, Network, nid};
use animus_sim::Simulator;

const MS: u64 = 1_000_000;

/// A node with no cost configured is untouched: the same script produces a
/// byte-identical trace whether or not *another* node has a cost.
#[test]
fn unset_cost_is_a_no_op_for_every_other_node() {
    fn run(seed: u64, starve_other: bool) -> Vec<String> {
        let mut sim = Simulator::new(seed);
        if starve_other {
            sim.set_cpu_cost(nid(9), Duration::from_millis(50));
        }
        for n in [0u64, 1] {
            let env = sim.env(nid(n));
            env.clone().spawn_task(async move {
                for i in 0..20u8 {
                    env.sleep(Duration::from_millis(7)).await;
                    env.send(nid(1 - n), vec![i]).await;
                }
            });
        }
        sim.run_for(Duration::from_secs(1));
        let mut lines = sim.trace_lines();
        lines.retain(|l| !l.contains("CPUCOST"));
        lines
    }
    let seed = 0xC0DE_0001;
    assert_eq!(run(seed, false), run(seed, true));
}

/// Timers fire serialized: N timers all due at once on a costed node fire at
/// `t, t + cost, t + 2*cost, ...` and the CPU stays busy a further `cost`
/// after the last one.
#[test]
fn simultaneous_timers_are_serialized_by_the_cost() {
    let seed = 0xC0DE_0002;
    let mut sim = Simulator::new(seed);
    let node = nid(0);
    sim.set_cpu_cost(node.clone(), Duration::from_millis(10));
    let fired = Arc::new(Mutex::new(Vec::new()));
    for _ in 0..5 {
        let env = sim.env(node.clone());
        let out = Arc::clone(&fired);
        env.clone().spawn_task(async move {
            env.sleep(Duration::from_millis(100)).await;
            out.lock().unwrap().push(env.now().0);
        });
    }
    sim.run_for(Duration::from_secs(1));
    let got = fired.lock().unwrap().clone();
    let want: Vec<u64> = (0..5).map(|i| 100 * MS + i * 10 * MS).collect();
    assert_eq!(got, want, "seed={seed}: events must run one per cost slot");
}

/// A timer due after the CPU is idle again is not delayed (cost is a
/// capacity, not a fixed latency).
#[test]
fn an_idle_node_is_not_delayed() {
    let seed = 0xC0DE_0003;
    let mut sim = Simulator::new(seed);
    let node = nid(0);
    sim.set_cpu_cost(node.clone(), Duration::from_millis(10));
    let fired = Arc::new(Mutex::new(Vec::new()));
    let env = sim.env(node);
    let out = Arc::clone(&fired);
    env.clone().spawn_task(async move {
        for _ in 0..3 {
            env.sleep(Duration::from_millis(100)).await;
            out.lock().unwrap().push(env.now().0);
        }
    });
    sim.run_for(Duration::from_secs(1));
    let got = fired.lock().unwrap().clone();
    // Each sleep re-arms at the instant the previous fired; the CPU is free
    // again 10ms later, long before the next 100ms deadline.
    assert_eq!(got, vec![100 * MS, 200 * MS, 300 * MS], "seed={seed}");
}

/// Deliveries to a costed node queue behind its busy CPU: a burst of
/// messages is observed spread at `cost` intervals, none dropped, in order.
#[test]
fn deliveries_are_serialized_and_none_are_lost() {
    let seed = 0xC0DE_0004;
    let mut sim = Simulator::new(seed);
    let mut cfg = animus_sim::NetConfig::default();
    cfg.base_delay = Duration::from_millis(1);
    cfg.max_jitter = Duration::ZERO;
    sim.set_net_config(cfg);
    let rx = nid(1);
    sim.set_cpu_cost(rx.clone(), Duration::from_millis(10));
    let got = Arc::new(Mutex::new(Vec::new()));
    {
        let env = sim.env(rx);
        let out = Arc::clone(&got);
        env.clone().spawn_task(async move {
            loop {
                let m = env.recv().await;
                out.lock().unwrap().push((env.now().0, m.payload[0]));
            }
        });
    }
    {
        let env = sim.env(nid(0));
        env.clone().spawn_task(async move {
            for i in 0..6u8 {
                env.send(nid(1), vec![i]).await;
            }
        });
    }
    sim.run_for(Duration::from_secs(1));
    let got = got.lock().unwrap().clone();
    assert_eq!(got.len(), 6, "seed={seed}: no message may be lost: {got:?}");
    let payloads: Vec<u8> = got.iter().map(|(_, p)| *p).collect();
    assert_eq!(payloads, vec![0, 1, 2, 3, 4, 5], "seed={seed}: FIFO");
    for w in got.windows(2) {
        assert!(
            w[1].0 - w[0].0 >= 10 * MS,
            "seed={seed}: deliveries closer than the cost: {got:?}"
        );
    }
}

/// The node falls further behind the longer it is overloaded, and recovers
/// once the cost is lowered: `set_cpu_cost` mid-run drains the backlog.
#[test]
fn overload_builds_a_backlog_and_lowering_the_cost_drains_it() {
    let seed = 0xC0DE_0005;
    let mut sim = Simulator::new(seed);
    let mut cfg = animus_sim::NetConfig::default();
    cfg.base_delay = Duration::from_millis(1);
    cfg.max_jitter = Duration::ZERO;
    sim.set_net_config(cfg);
    let rx = nid(1);
    // 100 msgs/s offered, capacity 50/s: lateness grows ~10ms per message.
    sim.set_cpu_cost(rx.clone(), Duration::from_millis(20));
    let lateness = Arc::new(Mutex::new(Vec::<(u64, u64)>::new()));
    {
        let env = sim.env(rx);
        let out = Arc::clone(&lateness);
        env.clone().spawn_task(async move {
            loop {
                let m = env.recv().await;
                let sent = u64::from_be_bytes(m.payload[..8].try_into().unwrap());
                out.lock().unwrap().push((sent, env.now().0 - sent));
            }
        });
    }
    {
        let env = sim.env(nid(0));
        env.clone().spawn_task(async move {
            loop {
                env.sleep(Duration::from_millis(10)).await;
                env.send(nid(1), env.now().0.to_be_bytes().to_vec()).await;
            }
        });
    }
    sim.run_for(Duration::from_secs(2));
    let late_at_2s = lateness.lock().unwrap().last().unwrap().1;
    assert!(
        late_at_2s > 500 * MS,
        "seed={seed}: overload must build a backlog, lateness={late_at_2s}ns"
    );
    sim.set_cpu_cost(nid(1), Duration::from_millis(1));
    sim.run_for(Duration::from_secs(4));
    let late_end = lateness.lock().unwrap().last().unwrap().1;
    assert!(
        late_end < 20 * MS,
        "seed={seed}: relief must drain the backlog, lateness={late_end}ns"
    );
}

/// Deterministic and traced: the same seed + script reproduces a
/// byte-identical trace and the cost is visible in it.
#[test]
fn cpu_cost_is_deterministic_and_traced() {
    fn run(seed: u64) -> Vec<String> {
        let mut sim = Simulator::new(seed);
        sim.set_cpu_cost(nid(0), Duration::from_millis(3));
        for n in [0u64, 1] {
            let env = sim.env(nid(n));
            env.clone().spawn_task(async move {
                for i in 0..30u8 {
                    env.sleep(Duration::from_millis(5)).await;
                    env.send(nid(1 - n), vec![i]).await;
                }
            });
        }
        sim.run_for(Duration::from_secs(1));
        sim.trace_lines()
    }
    let seed = 0xC0DE_0006;
    let a = run(seed);
    assert_eq!(a, run(seed), "seed={seed}: must be reproducible");
    assert!(
        a.iter()
            .any(|l| l.contains("CPUCOST") && l.contains("node=n0")),
        "seed={seed}: the cost must be traced"
    );
}

/// A message dropped for a crashed node costs it nothing.
#[test]
fn dropped_deliveries_are_free() {
    let seed = 0xC0DE_0007;
    let mut sim = Simulator::new(seed);
    sim.set_cpu_cost(nid(1), Duration::from_millis(500));
    sim.crash(nid(1));
    let env = sim.env(nid(0));
    env.clone().spawn_task(async move {
        for i in 0..5u8 {
            env.send(nid(1), vec![i]).await;
        }
    });
    sim.run_for(Duration::from_millis(100));
    sim.restart(nid(1));
    let got = Arc::new(Mutex::new(None));
    let env = sim.env(nid(1));
    let out = Arc::clone(&got);
    env.clone().spawn_task(async move {
        // First post-restart timer must not queue behind phantom charges.
        env.sleep(Duration::from_millis(10)).await;
        *out.lock().unwrap() = Some(env.now().0);
    });
    let start = sim.now().0;
    sim.run_for(Duration::from_millis(100));
    let at = got.lock().unwrap().expect("timer fired");
    assert!(
        at - start <= 11 * MS,
        "seed={seed}: delayed by dropped events"
    );
}

/// An expired timer runs before the messages queued behind a busy CPU (the
/// ordering that lets a starved follower's election timer beat the heartbeat
/// already waiting for it); timers FIFO, messages FIFO.
#[test]
fn an_expired_timer_runs_before_queued_messages() {
    let seed = 0xC0DE_0008;
    let mut sim = Simulator::new(seed);
    let mut cfg = animus_sim::NetConfig::default();
    cfg.base_delay = Duration::from_millis(1);
    cfg.max_jitter = Duration::ZERO;
    sim.set_net_config(cfg);
    let node = nid(1);
    sim.set_cpu_cost(node.clone(), Duration::from_millis(10));
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    {
        let env = sim.env(node.clone());
        let out = Arc::clone(&log);
        env.clone().spawn_task(async move {
            loop {
                let m = env.recv().await;
                out.lock().unwrap().push(format!("msg{}", m.payload[0]));
            }
        });
    }
    {
        let env = sim.env(node.clone());
        let out = Arc::clone(&log);
        env.clone().spawn_task(async move {
            env.sleep(Duration::from_millis(5)).await;
            out.lock().unwrap().push("timer".to_string());
        });
    }
    {
        let env = sim.env(nid(0));
        env.clone().spawn_task(async move {
            for i in 0..4u8 {
                env.send(nid(1), vec![i]).await;
            }
        });
    }
    sim.run_for(Duration::from_secs(1));
    // msg0 takes the idle CPU at 1ms; the timer expires at 5ms while msgs 1-3
    // wait; the timer jumps the queue.
    assert_eq!(
        log.lock().unwrap().clone(),
        vec!["msg0", "timer", "msg1", "msg2", "msg3"],
        "seed={seed}"
    );
}

/// A timer cancelled (its `Sleep` dropped) while it waits for the CPU is
/// free: it neither fires nor consumes a slot.
#[test]
fn a_timer_cancelled_while_queued_costs_nothing() {
    let seed = 0xC0DE_0009;
    let mut sim = Simulator::new(seed);
    let node = nid(0);
    sim.set_cpu_cost(node.clone(), Duration::from_millis(100));
    let fired = Arc::new(Mutex::new(Vec::new()));
    // Task A keeps the CPU busy: its timer fires at 10ms, busy until 110ms.
    {
        let env = sim.env(node.clone());
        let out = Arc::clone(&fired);
        env.clone().spawn_task(async move {
            env.sleep(Duration::from_millis(10)).await;
            out.lock().unwrap().push(("a", env.now().0));
        });
    }
    // Task B's 50ms timer expires while the CPU is busy, so it queues; the
    // 60ms sleep-or-nothing race cancels it from a separate wake.
    {
        let env = sim.env(node.clone());
        let out = Arc::clone(&fired);
        env.clone().spawn_task(async move {
            let t = env.sleep(Duration::from_millis(50));
            let u = env.sleep(Duration::from_millis(60));
            futures::pin_mut!(t, u);
            futures::future::select(t, u).await;
            out.lock().unwrap().push(("b", env.now().0));
        });
    }
    sim.run_for(Duration::from_secs(1));
    let got = fired.lock().unwrap().clone();
    assert_eq!(got.len(), 2, "seed={seed}: {got:?}");
    assert_eq!(got[0], ("a", 10 * MS));
    // B's winning timer (50ms) was queued behind A until 110ms; the losing
    // 60ms sleep was dropped and must not have cost a slot or fired.
    assert_eq!(got[1].1, 110 * MS, "seed={seed}: {got:?}");
}
