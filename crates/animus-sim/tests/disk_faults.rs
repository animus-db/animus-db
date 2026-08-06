//! Disk fault injection (opt-in, seed-driven, default-off).
//!
//! The two guarantees under test:
//!
//! 1. **Default-off is byte-identical**: with no [`DiskConfig`] (or an
//!    explicitly default one) the disk draws no RNG and emits no trace event,
//!    so a run's history is byte-for-byte what it was before the fault model
//!    existed — every pre-existing test is unaffected.
//! 2. **Faults are a pure function of the seed**: with a given config, the
//!    schedule of injected errors, the torn-tail tear points, and the
//!    corrupted byte are identical across re-runs of the same seed.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Disk, EnvExt, Network};
use animus_sim::{DiskConfig, Simulator};

/// A workload that interleaves disk ops with network sends. The sends draw RNG
/// (delivery jitter), so if the disk model drew *any* extra RNG the delivery
/// schedule — and therefore the trace — would shift: trace equality proves the
/// disk ops drew nothing.
fn run_disk_and_net_workload(seed: u64, cfg: Option<DiskConfig>) -> (Vec<String>, Vec<u8>) {
    let mut sim = Simulator::new(seed);
    if let Some(cfg) = cfg {
        sim.set_disk_config(cfg.clone());
        sim.set_disk_config_for(0, cfg);
    }
    let out = Arc::new(Mutex::new(Vec::new()));
    {
        let env = sim.env(0);
        let sink = sim.env(1);
        sink.clone().spawn_task(async move {
            loop {
                let _ = sink.recv().await;
            }
        });
        let out = Arc::clone(&out);
        env.clone().spawn_task(async move {
            for i in 0..5u8 {
                env.append("wal", &[i; 8]).await.unwrap();
                env.send(1, vec![i]).await;
                env.sync("wal").await.unwrap();
                env.send(1, vec![i, i]).await;
            }
            env.append("wal", b"unsynced-tail").await.unwrap();
            *out.lock().unwrap() = env.read("wal").await.unwrap();
        });
        sim.run_for(Duration::from_millis(50));
    }
    // Crash + read back what survived.
    sim.crash(0);
    sim.restart(0);
    let after = Arc::new(Mutex::new(Vec::new()));
    {
        let env = sim.env(0);
        let out = Arc::clone(&after);
        env.clone().spawn_task(async move {
            *out.lock().unwrap() = env.read("wal").await.unwrap();
        });
        sim.run_for(Duration::from_millis(1));
    }
    let bytes = after.lock().unwrap().clone();
    (sim.trace_lines(), bytes)
}

/// Default-off: a run with no disk config, one with an explicitly-default
/// global config, and one with an explicitly-default per-node override are all
/// byte-identical — and the crash keeps the pre-fault-model semantics (the
/// whole un-synced buffer dropped atomically).
#[test]
fn default_disk_config_is_byte_identical_and_atomic_on_crash() {
    let seed = 0xD15C_0001;
    let (trace_none, bytes_none) = run_disk_and_net_workload(seed, None);
    let (trace_default, bytes_default) =
        run_disk_and_net_workload(seed, Some(DiskConfig::default()));

    assert_eq!(
        trace_none, trace_default,
        "a default DiskConfig must not perturb the run (seed={seed})"
    );
    assert_eq!(bytes_none, bytes_default, "seed={seed}");
    // Old crash semantics: 5 synced 8-byte records survive, the un-synced tail
    // is dropped whole.
    assert_eq!(
        bytes_none.len(),
        40,
        "default crash must drop the whole un-synced buffer (seed={seed})"
    );
    assert!(
        !trace_none.iter().any(|l| l.contains("DISK")),
        "no disk fault trace events with the default config (seed={seed})"
    );
}

/// With a non-zero error rate, the schedule of injected failures (which ops
/// failed, and the trace) is byte-identical across re-runs of the same seed —
/// and both outcomes actually occur.
#[test]
fn injected_error_schedule_is_reproducible_from_the_seed() {
    fn run(seed: u64) -> (Vec<bool>, Vec<String>) {
        let sim = Simulator::new(seed);
        let mut cfg = DiskConfig::default();
        cfg.set_error_prob(0.4);
        sim.set_disk_config(cfg);
        let results = Arc::new(Mutex::new(Vec::new()));
        let mut sim = sim;
        {
            let env = sim.env(0);
            let out = Arc::clone(&results);
            env.clone().spawn_task(async move {
                for i in 0..30u8 {
                    let ok = env.append("f", &[i]).await.is_ok();
                    let synced = env.sync("f").await.is_ok();
                    out.lock().unwrap().push(ok);
                    out.lock().unwrap().push(synced);
                }
            });
            sim.run();
        }
        let r = results.lock().unwrap().clone();
        (r, sim.trace_lines())
    }

    let seed = 0xD15C_0002;
    let (a, trace_a) = run(seed);
    let (b, trace_b) = run(seed);
    assert_eq!(a, b, "fault schedule diverged across re-runs (seed={seed})");
    assert_eq!(trace_a, trace_b, "trace diverged (seed={seed})");
    assert!(
        a.iter().any(|&ok| ok) && a.iter().any(|&ok| !ok),
        "at p=0.4 over 60 ops both outcomes should occur (seed={seed})"
    );
    assert!(
        trace_a.iter().any(|l| l.contains("DISKFAULT")),
        "injected errors must be traced (seed={seed})"
    );
}

/// A per-node override scopes the fault model: node 0's disk always fails,
/// node 1's (on the global default) never does.
#[test]
fn per_node_disk_config_overrides_the_global() {
    let seed = 0xD15C_0003;
    let mut sim = Simulator::new(seed);
    let mut cfg = DiskConfig::default();
    cfg.set_error_prob(1.0);
    sim.set_disk_config_for(0, cfg);

    let out = Arc::new(Mutex::new((None, None)));
    {
        let e0 = sim.env(0);
        let e1 = sim.env(1);
        let out = Arc::clone(&out);
        e0.clone().spawn_task(async move {
            let r0 = e0.append("f", b"x").await.is_ok();
            let r1 = e1.append("f", b"x").await.is_ok();
            *out.lock().unwrap() = (Some(r0), Some(r1));
        });
        sim.run();
    }
    let (r0, r1) = *out.lock().unwrap();
    assert_eq!(r0, Some(false), "node 0 must fail (p=1.0) (seed={seed})");
    assert_eq!(r1, Some(true), "node 1 must be unaffected (seed={seed})");
}

/// Torn tail: a crash keeps a strict, seed-chosen prefix of the un-synced
/// buffer; the durable prefix is exact; re-running the same seed reproduces
/// the identical surviving bytes.
#[test]
fn torn_tail_crash_keeps_a_reproducible_strict_prefix() {
    fn run(seed: u64) -> Vec<u8> {
        let mut sim = Simulator::new(seed);
        let mut cfg = DiskConfig::default();
        cfg.torn_tail_on_crash = true;
        sim.set_disk_config(cfg);
        {
            let env = sim.env(0);
            env.clone().spawn_task(async move {
                env.append("wal", b"DURABLE!").await.unwrap();
                env.sync("wal").await.unwrap();
                env.append("wal", b"unsynced-record-tail").await.unwrap();
            });
            sim.run();
        }
        sim.crash(0);
        sim.restart(0);
        let out = Arc::new(Mutex::new(Vec::new()));
        {
            let env = sim.env(0);
            let o = Arc::clone(&out);
            env.clone().spawn_task(async move {
                *o.lock().unwrap() = env.read("wal").await.unwrap();
            });
            sim.run();
        }
        let bytes = out.lock().unwrap().clone();
        bytes
    }

    let seed = 0xD15C_0004;
    let a = run(seed);
    let b = run(seed);
    assert_eq!(
        a, b,
        "torn tail not reproducible from the seed (seed={seed})"
    );

    let full = b"DURABLE!unsynced-record-tail";
    assert!(
        a.starts_with(b"DURABLE!"),
        "durable prefix must be exact (seed={seed}, got {a:?})"
    );
    assert!(
        a.len() < full.len(),
        "a tear must lose at least one byte (seed={seed})"
    );
    assert!(
        full.starts_with(&a),
        "survivor must be a prefix of the written bytes (seed={seed}, got {a:?})"
    );
}

/// The torn tail survives a *restart* (it is durable — those bytes hit the
/// platter), and across seeds the tear point varies while the durable prefix
/// never does.
#[test]
fn torn_tail_varies_by_seed_but_durable_prefix_never_torn() {
    let mut lens = std::collections::BTreeSet::new();
    for seed in [1u64, 2, 3, 4, 5, 6, 7, 8] {
        let mut sim = Simulator::new(seed);
        let mut cfg = DiskConfig::default();
        cfg.torn_tail_on_crash = true;
        sim.set_disk_config(cfg);
        {
            let env = sim.env(0);
            env.clone().spawn_task(async move {
                env.append("wal", b"KEEP").await.unwrap();
                env.sync("wal").await.unwrap();
                env.append("wal", b"0123456789abcdef0123456789abcdef")
                    .await
                    .unwrap();
            });
            sim.run();
        }
        sim.crash(0);
        sim.restart(0);
        let out = Arc::new(Mutex::new(Vec::new()));
        {
            let env = sim.env(0);
            let o = Arc::clone(&out);
            env.clone().spawn_task(async move {
                *o.lock().unwrap() = env.read("wal").await.unwrap();
            });
            sim.run();
        }
        let bytes = out.lock().unwrap().clone();
        assert!(
            bytes.starts_with(b"KEEP"),
            "seed={seed}: durable prefix torn"
        );
        assert!(bytes.len() < 4 + 32, "seed={seed}: nothing was torn");
        lens.insert(bytes.len());
    }
    assert!(
        lens.len() > 1,
        "8 seeds all tore at the same point — tear is not seed-driven: {lens:?}"
    );
}

/// `corrupt_on_crash`: when the tear retains bytes, exactly one byte of the
/// retained region differs from what was written; the durable prefix is
/// untouched; the outcome is reproducible from the seed.
#[test]
fn corrupt_on_crash_flips_one_byte_in_the_retained_region() {
    fn run(seed: u64) -> Vec<u8> {
        let mut sim = Simulator::new(seed);
        let mut cfg = DiskConfig::default();
        cfg.torn_tail_on_crash = true;
        cfg.corrupt_on_crash = true;
        sim.set_disk_config(cfg);
        {
            let env = sim.env(0);
            env.clone().spawn_task(async move {
                env.append("wal", b"KEEP").await.unwrap();
                env.sync("wal").await.unwrap();
                env.append("wal", b"0123456789abcdef0123456789abcdef")
                    .await
                    .unwrap();
            });
            sim.run();
        }
        sim.crash(0);
        sim.restart(0);
        let out = Arc::new(Mutex::new(Vec::new()));
        {
            let env = sim.env(0);
            let o = Arc::clone(&out);
            env.clone().spawn_task(async move {
                *o.lock().unwrap() = env.read("wal").await.unwrap();
            });
            sim.run();
        }
        let bytes = out.lock().unwrap().clone();
        bytes
    }

    let written: &[u8] = b"KEEP0123456789abcdef0123456789abcdef";
    let mut saw_corruption = false;
    for seed in [11u64, 12, 13, 14, 15, 16] {
        let a = run(seed);
        let b = run(seed);
        assert_eq!(a, b, "corruption not reproducible (seed={seed})");
        assert!(
            a.starts_with(b"KEEP"),
            "seed={seed}: durable prefix corrupted"
        );
        assert!(a.len() < written.len(), "seed={seed}: nothing torn");
        let kept = a.len() - 4;
        let diffs: Vec<usize> = (0..a.len()).filter(|&i| a[i] != written[i]).collect();
        if kept == 0 {
            assert!(
                diffs.is_empty(),
                "seed={seed}: corrupted with nothing retained"
            );
        } else {
            assert!(
                diffs.len() <= 1,
                "seed={seed}: more than one byte corrupted: {diffs:?}"
            );
            if diffs.len() == 1 {
                assert!(diffs[0] >= 4, "seed={seed}: corruption outside torn region");
                saw_corruption = true;
            }
        }
    }
    assert!(saw_corruption, "no seed produced a retained+corrupted byte");
}

/// `corrupt_durable` flips exactly the addressed durable byte (no RNG), and
/// reports a miss for an out-of-range offset or unknown file.
#[test]
fn corrupt_durable_flips_the_exact_byte() {
    let seed = 0xD15C_0005;
    let mut sim = Simulator::new(seed);
    {
        let env = sim.env(0);
        env.clone().spawn_task(async move {
            env.append("sst", b"abcdef").await.unwrap();
            env.sync("sst").await.unwrap();
        });
        sim.run();
    }
    assert!(sim.corrupt_durable(0, "sst", 2), "seed={seed}");
    assert!(
        !sim.corrupt_durable(0, "sst", 99),
        "offset past EOF must miss"
    );
    assert!(!sim.corrupt_durable(0, "nope", 0), "unknown file must miss");

    let out = Arc::new(Mutex::new(Vec::new()));
    {
        let env = sim.env(0);
        let o = Arc::clone(&out);
        env.clone().spawn_task(async move {
            *o.lock().unwrap() = env.read("sst").await.unwrap();
        });
        sim.run();
    }
    let bytes = out.lock().unwrap().clone();
    assert_eq!(bytes.len(), 6, "seed={seed}");
    assert_eq!(&bytes[..2], b"ab", "seed={seed}");
    assert_eq!(
        bytes[2],
        b'c' ^ 0xFF,
        "seed={seed}: byte 2 must be bit-flipped"
    );
    assert_eq!(&bytes[3..], b"def", "seed={seed}");
    assert!(
        sim.trace_lines().iter().any(|l| l.contains("DISKCORRUPT")),
        "corruption must be traced (seed={seed})"
    );
}
