//! Per-node/per-link `NetConfig` overrides (B2) and the new network fault
//! primitives — message duplication, wire-payload corruption, and
//! heavy-tailed delay (B3) — all opt-in, seed-driven, and default-off.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Clock, EnvExt, Network, nid};
use animus_sim::{NetConfig, Simulator};

/// A link override beats a per-node override, which beats the global config
/// — `NetConfig`'s documented most-specific-wins resolution order.
#[test]
fn link_override_beats_node_override_beats_global() {
    let seed = 0xD1_0001;
    let mut sim = Simulator::new(seed);

    // Global: never drops. Per-node override for node 0 (the sender):
    // always drops. Link override for the specific (0 -> 2) pair: never
    // drops, overriding the per-node override for just that destination.
    let mut always_drop = NetConfig::default();
    always_drop.max_jitter = Duration::ZERO;
    always_drop.set_drop_prob(1.0);
    sim.set_net_config_for(nid(0), always_drop);

    let mut never_drop = NetConfig::default();
    never_drop.max_jitter = Duration::ZERO;
    never_drop.set_drop_prob(0.0);
    sim.set_link_net_config(nid(0), nid(2), never_drop);

    let received = Arc::new(Mutex::new((false, false)));
    {
        let sink1 = sim.env(nid(1));
        let out = Arc::clone(&received);
        sink1.clone().spawn_task(async move {
            let _ = sink1.recv().await;
            out.lock().unwrap().0 = true;
        });
    }
    {
        let sink2 = sim.env(nid(2));
        let out = Arc::clone(&received);
        sink2.clone().spawn_task(async move {
            let _ = sink2.recv().await;
            out.lock().unwrap().1 = true;
        });
    }
    {
        let sender = sim.env(nid(0));
        sender.clone().spawn_task(async move {
            sender.send(nid(1), vec![1]).await; // node-level override: dropped
            sender.send(nid(2), vec![2]).await; // link override: delivered
        });
    }
    sim.run_for(Duration::from_millis(50));

    let (got1, got2) = *received.lock().unwrap();
    assert!(
        !got1,
        "seed={seed}: node 0's per-node override (always drop) must apply to node 1"
    );
    assert!(
        got2,
        "seed={seed}: the (0,2) link override must beat the per-node override"
    );
}

/// A per-node override scopes to the *sender*: node 0's outbound sends are
/// governed by its own override; node 1's outbound sends (to the same
/// destination) still see the global config.
#[test]
fn per_node_net_config_is_keyed_on_the_sender() {
    let seed = 0xD1_0002;
    let mut sim = Simulator::new(seed);

    let mut always_drop = NetConfig::default();
    always_drop.max_jitter = Duration::ZERO;
    always_drop.set_drop_prob(1.0);
    sim.set_net_config_for(nid(0), always_drop);
    // Global stays at its default (jitter only, no drop).

    let out = Arc::new(Mutex::new((false, false)));
    {
        let sink = sim.env(nid(2));
        let flags = Arc::clone(&out);
        sink.clone().spawn_task(async move {
            for _ in 0..1 {
                let _ = sink.recv().await;
                flags.lock().unwrap().0 = true;
            }
        });
    }
    {
        let a = sim.env(nid(0));
        a.clone().spawn_task(async move {
            a.send(nid(2), vec![9]).await; // must be dropped
        });
    }
    sim.run_for(Duration::from_millis(50));

    let sink2 = sim.env(nid(3));
    let out2 = Arc::clone(&out);
    sink2.clone().spawn_task(async move {
        let _ = sink2.recv().await;
        out2.lock().unwrap().1 = true;
    });
    let b = sim.env(nid(1));
    b.clone().spawn_task(async move {
        b.send(nid(3), vec![9]).await; // node 1 is unaffected by node 0's override
    });
    sim.run_for(Duration::from_millis(50));

    let (from_a, from_b) = *out.lock().unwrap();
    assert!(
        !from_a,
        "seed={seed}: node 0's override must drop its own sends"
    );
    assert!(
        from_b,
        "seed={seed}: node 1's sends must be unaffected by node 0's per-node override"
    );
}

/// Message duplication (`NetConfig::set_duplicate_prob`): at probability 1.0
/// every message is delivered twice, with two independent (usually
/// differing) delays. Off (the default) delivers exactly once. Both the
/// schedule and the resulting delivered sequence are reproducible from the
/// seed.
#[test]
fn duplication_delivers_twice_with_independent_delays_reproducibly() {
    fn run(seed: u64, dup_prob: f64) -> (Vec<u8>, Vec<String>) {
        let mut sim = Simulator::new(seed);
        let mut cfg = NetConfig::default();
        cfg.set_duplicate_prob(dup_prob);
        sim.set_net_config(cfg);

        let seen = Arc::new(Mutex::new(Vec::new()));
        {
            let sink = sim.env(nid(1));
            let out = Arc::clone(&seen);
            sink.clone().spawn_task(async move {
                loop {
                    let msg = sink.recv().await;
                    out.lock().unwrap().push(msg.payload[0]);
                }
            });
        }
        {
            let sender = sim.env(nid(0));
            sender.clone().spawn_task(async move {
                for i in 0..5u8 {
                    sender.send(nid(1), vec![i]).await;
                }
            });
        }
        sim.run_for(Duration::from_millis(200));
        (seen.lock().unwrap().clone(), sim.trace_lines())
    }

    let seed = 0xD1_0003;
    let (always, trace_always_a) = run(seed, 1.0);
    let (always_b, trace_always_b) = run(seed, 1.0);
    assert_eq!(
        always, always_b,
        "seed={seed}: duplicated delivery sequence must be reproducible"
    );
    assert_eq!(trace_always_a, trace_always_b, "seed={seed}");
    assert_eq!(
        always.len(),
        10,
        "seed={seed}: at duplicate_prob=1.0, 5 sends must yield 10 deliveries, got {always:?}"
    );
    let mut sorted = always.clone();
    sorted.sort_unstable();
    assert_eq!(
        sorted,
        vec![0, 0, 1, 1, 2, 2, 3, 3, 4, 4],
        "seed={seed}: each payload must arrive exactly twice"
    );
    assert!(
        trace_always_a.iter().any(|l| l.contains("DUPLICATE")),
        "seed={seed}: duplication must be traced"
    );

    let (never, _) = run(seed, 0.0);
    assert_eq!(
        never.len(),
        5,
        "seed={seed}: duplicate_prob=0.0 (default) must deliver each message exactly once"
    );
}

/// Wire-payload corruption (`NetConfig::set_corrupt_prob`): at probability
/// 1.0 the delivered payload differs from what was sent by exactly one
/// bit-flipped byte, reproducibly from the seed; at probability 0.0 (the
/// default) the payload always arrives unmodified.
#[test]
fn corruption_flips_one_byte_reproducibly() {
    fn run(seed: u64, corrupt_prob: f64) -> (Vec<u8>, Vec<String>) {
        let mut sim = Simulator::new(seed);
        let mut cfg = NetConfig::default();
        cfg.max_jitter = Duration::ZERO;
        cfg.set_corrupt_prob(corrupt_prob);
        sim.set_net_config(cfg);

        let received = Arc::new(Mutex::new(Vec::new()));
        {
            let sink = sim.env(nid(1));
            let out = Arc::clone(&received);
            sink.clone().spawn_task(async move {
                let msg = sink.recv().await;
                *out.lock().unwrap() = msg.payload;
            });
        }
        {
            let sender = sim.env(nid(0));
            sender.clone().spawn_task(async move {
                sender.send(nid(1), vec![0xAAu8; 16]).await;
            });
        }
        sim.run_for(Duration::from_millis(50));
        (received.lock().unwrap().clone(), sim.trace_lines())
    }

    let seed = 0xD1_0004;
    let original = vec![0xAAu8; 16];

    let (corrupted_a, trace_a) = run(seed, 1.0);
    let (corrupted_b, trace_b) = run(seed, 1.0);
    assert_eq!(
        corrupted_a, corrupted_b,
        "seed={seed}: corruption must be reproducible"
    );
    assert_eq!(trace_a, trace_b, "seed={seed}");
    assert_eq!(corrupted_a.len(), original.len(), "seed={seed}");
    let diffs: Vec<usize> = (0..original.len())
        .filter(|&i| original[i] != corrupted_a[i])
        .collect();
    assert_eq!(
        diffs.len(),
        1,
        "seed={seed}: corrupt_prob=1.0 must flip exactly one byte, got diffs at {diffs:?}"
    );
    assert_eq!(
        corrupted_a[diffs[0]],
        original[diffs[0]] ^ 0xFF,
        "seed={seed}: the flipped byte must be bit-inverted"
    );
    assert!(
        trace_a.iter().any(|l| l.contains("NETCORRUPT")),
        "seed={seed}: corruption must be traced"
    );

    let (untouched, _) = run(seed, 0.0);
    assert_eq!(
        untouched, original,
        "seed={seed}: corrupt_prob=0.0 (default) must never modify the payload"
    );
}

/// Heavy-tailed delay (`NetConfig::set_heavy_tail_prob` +
/// `heavy_tail_max_jitter`): with the ordinary jitter ceiling at zero and the
/// heavy tail always selected, delivery delay can reach far past what the
/// ordinary ceiling would ever allow — an occasional very slow message
/// modelled without raising the common-case delay — reproducibly from the
/// seed.
#[test]
fn heavy_tail_jitter_can_exceed_the_ordinary_ceiling() {
    fn run(seed: u64) -> (Vec<u64>, Vec<String>) {
        let mut sim = Simulator::new(seed);
        let mut cfg = NetConfig::default();
        cfg.base_delay = Duration::ZERO;
        cfg.max_jitter = Duration::ZERO; // ordinary jitter is always 0
        cfg.heavy_tail_max_jitter = Duration::from_millis(500);
        cfg.set_heavy_tail_prob(1.0); // always use the heavy tail
        sim.set_net_config(cfg);

        let deliveries = Arc::new(Mutex::new(Vec::new()));
        {
            let sink = sim.env(nid(1));
            let out = Arc::clone(&deliveries);
            sink.clone().spawn_task(async move {
                loop {
                    let msg = sink.recv().await;
                    let t = sink.now().0;
                    out.lock().unwrap().push((t, msg.payload[0]));
                }
            });
        }
        {
            let sender = sim.env(nid(0));
            sender.clone().spawn_task(async move {
                for i in 0..20u8 {
                    sender.send(nid(1), vec![i]).await;
                }
            });
        }
        sim.run_for(Duration::from_millis(600));
        let times = deliveries.lock().unwrap().iter().map(|&(t, _)| t).collect();
        (times, sim.trace_lines())
    }

    let seed = 0xD1_0005;
    let (times_a, trace_a) = run(seed);
    let (times_b, trace_b) = run(seed);
    assert_eq!(
        times_a, times_b,
        "seed={seed}: heavy-tail delay schedule must be reproducible"
    );
    assert_eq!(trace_a, trace_b, "seed={seed}");
    assert_eq!(times_a.len(), 20, "seed={seed}");
    assert!(
        times_a.iter().any(|&t| t > 100_000_000),
        "seed={seed}: with the heavy tail always selected and a 500ms ceiling, \
         at least one of 20 messages should land well past 100ms, got {times_a:?}"
    );
}

/// With no per-node/per-link override configured and every new `NetConfig`
/// threshold at its default (0), behavior is identical to a `NetConfig`
/// predating these knobs: exactly the same draw sequence (a drop roll, then
/// a jitter draw) fires, so setting an explicitly-default config (globally,
/// per-node, and per-link) changes nothing observable.
#[test]
fn default_net_config_extensions_are_byte_identical() {
    fn run(seed: u64, configure_defaults: bool) -> Vec<String> {
        let mut sim = Simulator::new(seed);
        if configure_defaults {
            sim.set_net_config(NetConfig::default());
            sim.set_net_config_for(nid(0), NetConfig::default());
            sim.set_link_net_config(nid(0), nid(1), NetConfig::default());
        }
        let out = Arc::new(Mutex::new(Vec::new()));
        {
            let sink = sim.env(nid(1));
            let seen = Arc::clone(&out);
            sink.clone().spawn_task(async move {
                for _ in 0..5 {
                    let msg = sink.recv().await;
                    seen.lock().unwrap().push(msg.payload[0]);
                }
            });
        }
        {
            let sender = sim.env(nid(0));
            sender.clone().spawn_task(async move {
                for i in 0..5u8 {
                    sender.send(nid(1), vec![i]).await;
                }
            });
        }
        assert!(sim.run_until_quiescent(100_000), "seed={seed}: must settle");
        sim.trace_lines()
    }

    let seed = 0xD1_0006;
    let bare = run(seed, false);
    let explicit = run(seed, true);
    assert_eq!(
        bare, explicit,
        "seed={seed}: explicitly-default global/per-node/per-link NetConfig \
         must not perturb the run"
    );
    assert!(
        !bare
            .iter()
            .any(|l| l.contains("DUPLICATE") || l.contains("NETCORRUPT")),
        "seed={seed}: no new fault should ever fire with every threshold at 0"
    );
}
