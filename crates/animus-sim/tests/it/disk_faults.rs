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

use animus_env::{Clock, Disk, EnvExt, Network, nid};
use animus_sim::{DiskConfig, Simulator};

/// A workload that interleaves disk ops with network sends. The sends draw RNG
/// (delivery jitter), so if the disk model drew *any* extra RNG the delivery
/// schedule — and therefore the trace — would shift: trace equality proves the
/// disk ops drew nothing.
fn run_disk_and_net_workload(seed: u64, cfg: Option<DiskConfig>) -> (Vec<String>, Vec<u8>) {
    let mut sim = Simulator::new(seed);
    if let Some(cfg) = cfg {
        sim.set_disk_config(cfg.clone());
        sim.set_disk_config_for(nid(0), cfg);
    }
    let out = Arc::new(Mutex::new(Vec::new()));
    {
        let env = sim.env(nid(0));
        let sink = sim.env(nid(1));
        sink.clone().spawn_task(async move {
            loop {
                let _ = sink.recv().await;
            }
        });
        let out = Arc::clone(&out);
        env.clone().spawn_task(async move {
            for i in 0..5u8 {
                env.append("wal", &[i; 8]).await.unwrap();
                env.send(nid(1), vec![i]).await;
                env.sync("wal").await.unwrap();
                env.send(nid(1), vec![i, i]).await;
            }
            env.append("wal", b"unsynced-tail").await.unwrap();
            *out.lock().unwrap() = env.read("wal").await.unwrap();
        });
        sim.run_for(Duration::from_millis(50));
    }
    // Crash + read back what survived.
    sim.crash(nid(0));
    sim.restart(nid(0));
    let after = Arc::new(Mutex::new(Vec::new()));
    {
        let env = sim.env(nid(0));
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
            let env = sim.env(nid(0));
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
    sim.set_disk_config_for(nid(0), cfg);

    let out = Arc::new(Mutex::new((None, None)));
    {
        let e0 = sim.env(nid(0));
        let e1 = sim.env(nid(1));
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
            let env = sim.env(nid(0));
            env.clone().spawn_task(async move {
                env.append("wal", b"DURABLE!").await.unwrap();
                env.sync("wal").await.unwrap();
                env.append("wal", b"unsynced-record-tail").await.unwrap();
            });
            sim.run();
        }
        sim.crash(nid(0));
        sim.restart(nid(0));
        let out = Arc::new(Mutex::new(Vec::new()));
        {
            let env = sim.env(nid(0));
            let o = Arc::clone(&out);
            env.clone().spawn_task(async move {
                *o.lock().unwrap() = env.read("wal").await.unwrap();
            });
            sim.run();
        }

        out.lock().unwrap().clone()
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
            let env = sim.env(nid(0));
            env.clone().spawn_task(async move {
                env.append("wal", b"KEEP").await.unwrap();
                env.sync("wal").await.unwrap();
                env.append("wal", b"0123456789abcdef0123456789abcdef")
                    .await
                    .unwrap();
            });
            sim.run();
        }
        sim.crash(nid(0));
        sim.restart(nid(0));
        let out = Arc::new(Mutex::new(Vec::new()));
        {
            let env = sim.env(nid(0));
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
            let env = sim.env(nid(0));
            env.clone().spawn_task(async move {
                env.append("wal", b"KEEP").await.unwrap();
                env.sync("wal").await.unwrap();
                env.append("wal", b"0123456789abcdef0123456789abcdef")
                    .await
                    .unwrap();
            });
            sim.run();
        }
        sim.crash(nid(0));
        sim.restart(nid(0));
        let out = Arc::new(Mutex::new(Vec::new()));
        {
            let env = sim.env(nid(0));
            let o = Arc::clone(&out);
            env.clone().spawn_task(async move {
                *o.lock().unwrap() = env.read("wal").await.unwrap();
            });
            sim.run();
        }

        out.lock().unwrap().clone()
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

/// `DiskConfig::set_sync_delay` (issue #279): a configured delay actually
/// delays `sync`'s (and `append`'s) completion in virtual time, and a run
/// with no delay configured takes none.
#[test]
fn sync_delay_actually_delays_a_sync_in_sim_time() {
    let seed = 0xD15C_0006;
    let mut sim = Simulator::new(seed);
    let mut cfg = DiskConfig::default();
    cfg.set_sync_delay(Duration::from_millis(400));
    sim.set_disk_config(cfg);

    let elapsed = Arc::new(Mutex::new(None));
    {
        let env = sim.env(nid(0));
        let out = Arc::clone(&elapsed);
        env.clone().spawn_task(async move {
            env.append("wal", b"hello").await.unwrap();
            let before = env.now();
            env.sync("wal").await.unwrap();
            let after = env.now();
            *out.lock().unwrap() = Some(after.0.saturating_sub(before.0));
        });
        // `append` and `sync` are each delayed 400ms, so the task needs up to
        // ~800ms of virtual time to reach the `sync` measurement.
        sim.run_for(Duration::from_millis(1000));
    }
    let nanos = elapsed.lock().unwrap().expect("task must have run");
    assert!(
        nanos >= Duration::from_millis(400).as_nanos() as u64,
        "seed={seed}: sync must not resolve before its configured delay elapses (got {nanos}ns)"
    );
}

/// With no `sync_delay` configured, `sync` resolves without advancing virtual
/// time at all (matches every pre-existing test's assumption).
#[test]
fn no_sync_delay_configured_takes_no_virtual_time() {
    let seed = 0xD15C_0007;
    let mut sim = Simulator::new(seed);
    sim.set_disk_config(DiskConfig::default());

    let elapsed = Arc::new(Mutex::new(None));
    {
        let env = sim.env(nid(0));
        let out = Arc::clone(&elapsed);
        env.clone().spawn_task(async move {
            env.append("wal", b"hello").await.unwrap();
            let before = env.now();
            env.sync("wal").await.unwrap();
            let after = env.now();
            *out.lock().unwrap() = Some(after.0.saturating_sub(before.0));
        });
        sim.run();
    }
    assert_eq!(
        elapsed.lock().unwrap().expect("task must have run"),
        0,
        "seed={seed}: an unconfigured sync_delay must not advance virtual time"
    );
}

/// `corrupt_durable` flips exactly the addressed durable byte (no RNG), and
/// reports a miss for an out-of-range offset or unknown file.
#[test]
fn corrupt_durable_flips_the_exact_byte() {
    let seed = 0xD15C_0005;
    let mut sim = Simulator::new(seed);
    {
        let env = sim.env(nid(0));
        env.clone().spawn_task(async move {
            env.append("sst", b"abcdef").await.unwrap();
            env.sync("sst").await.unwrap();
        });
        sim.run();
    }
    assert!(sim.corrupt_durable(nid(0), "sst", 2), "seed={seed}");
    assert!(
        !sim.corrupt_durable(nid(0), "sst", 99),
        "offset past EOF must miss"
    );
    assert!(
        !sim.corrupt_durable(nid(0), "nope", 0),
        "unknown file must miss"
    );

    let out = Arc::new(Mutex::new(Vec::new()));
    {
        let env = sim.env(nid(0));
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

/// `Disk::link` (ADR 0058 rung 2) models a hard link: `dst` reads back
/// `src`'s bytes, `remove`ing `src` afterward leaves `dst` unaffected (the
/// two are independent map slots once linked, exactly like two directory
/// entries sharing one inode until the last one is unlinked), and relinking
/// over an already-present `dst` succeeds — overwriting it — rather than
/// erroring, which is what makes a crash-retried clone idempotent.
#[test]
fn link_models_hard_link_semantics() {
    let seed = 0xD15C_0006;
    let mut sim = Simulator::new(seed);
    let env = sim.env(nid(0));
    env.clone().spawn_task(async move {
        env.append("src", b"hello").await.unwrap();
        env.sync("src").await.unwrap();
        env.link("src", "dst").await.expect("link");
        assert_eq!(env.read("dst").await.unwrap(), b"hello");

        // Overwrite-on-relink: linking again over an existing `dst` succeeds.
        env.link("src", "dst")
            .await
            .expect("relink over existing dst");
        assert_eq!(env.read("dst").await.unwrap(), b"hello");

        // Removing `src` leaves `dst`'s own bytes intact.
        env.remove("src").await.unwrap();
        assert_eq!(
            env.read("dst").await.unwrap(),
            b"hello",
            "dst must survive removal of src"
        );

        // Linking a nonexistent source is a clean NotFound.
        let err = env
            .link("does-not-exist", "also-dst")
            .await
            .expect_err("missing source must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    });
    sim.run();
}

/// `link` participates in the same opt-in, seed-reproducible error-injection
/// model every other disk op does: a configured error rate makes it fail
/// (with no state change) on a schedule that is a pure function of the seed.
#[test]
fn link_participates_in_the_injected_error_schedule() {
    // `src` is written with the fault disabled first, then the fault config
    // is installed on the *same* simulator instance (a fresh `Simulator`
    // would change the RNG stream the assertions below depend on) before the
    // `link` calls under test.
    fn run_with_preseeded_src(seed: u64) -> (Vec<bool>, bool) {
        let mut sim = Simulator::new(seed);
        {
            let env = sim.env(nid(0));
            env.clone().spawn_task(async move {
                env.append("src", b"hello").await.unwrap();
                env.sync("src").await.unwrap();
            });
            sim.run();
        }
        let mut cfg = DiskConfig::default();
        cfg.set_error_prob(0.5);
        sim.set_disk_config(cfg);
        let env = sim.env(nid(0));
        let results = Arc::new(Mutex::new(Vec::new()));
        let dst_has_data = Arc::new(Mutex::new(false));
        {
            let env = env.clone();
            let results = Arc::clone(&results);
            let dst_has_data = Arc::clone(&dst_has_data);
            env.clone().spawn_task(async move {
                for i in 0..20u8 {
                    let dst = format!("dst{i}");
                    let ok = env.link("src", &dst).await.is_ok();
                    results.lock().unwrap().push(ok);
                    if ok {
                        // `read` is itself an injectable op under this same
                        // fault config — only check the bytes when the read
                        // happens to succeed; a successful `link` followed by
                        // a failed `read` says nothing about `link`'s own
                        // correctness.
                        if let Ok(bytes) = env.read(&dst).await {
                            assert_eq!(bytes, b"hello", "a successful link must carry real bytes");
                            *dst_has_data.lock().unwrap() = true;
                        }
                    }
                }
            });
        }
        sim.run();
        let results = results.lock().unwrap().clone();
        (results, *dst_has_data.lock().unwrap())
    }

    let seed = 0xD15C_0007;
    let (a, a_had_data) = run_with_preseeded_src(seed);
    let (b, b_had_data) = run_with_preseeded_src(seed);
    assert_eq!(
        a, b,
        "seed={seed}: link's error schedule must reproduce exactly"
    );
    assert_eq!(a_had_data, b_had_data);
    assert!(
        a.iter().any(|&ok| ok),
        "seed={seed}: expected at least one link to succeed over 20 tries at p=0.5"
    );
    assert!(
        a.iter().any(|&ok| !ok),
        "seed={seed}: expected at least one link to fail over 20 tries at p=0.5"
    );
    assert!(
        a_had_data,
        "seed={seed}: a successful link must expose the real bytes"
    );
}

/// ENOSPC (`DiskConfig::set_enospc_prob`) is distinguishable from a generic
/// injected error by `ErrorKind`, and the two compose on one shared roll: a
/// config with only `error_prob` set never produces `StorageFull`, one with
/// only `enospc_prob` set never produces `Other`, and one with both set
/// produces both kinds over enough tries, reproducibly from the seed.
#[test]
fn enospc_is_distinguishable_from_a_generic_disk_error() {
    fn run(seed: u64, cfg: DiskConfig) -> Vec<std::io::ErrorKind> {
        let mut sim = Simulator::new(seed);
        sim.set_disk_config(cfg);
        let out = Arc::new(Mutex::new(Vec::new()));
        {
            let env = sim.env(nid(0));
            let out = Arc::clone(&out);
            env.clone().spawn_task(async move {
                for i in 0..40u8 {
                    if let Err(e) = env.append("f", &[i]).await {
                        out.lock().unwrap().push(e.kind());
                    }
                }
            });
            sim.run();
        }
        out.lock().unwrap().clone()
    }

    let seed = 0xD15C_0008;

    let mut generic_only = DiskConfig::default();
    generic_only.set_error_prob(0.5);
    let kinds = run(seed, generic_only);
    assert!(
        !kinds.is_empty(),
        "seed={seed}: expected some failures at p=0.5"
    );
    assert!(
        kinds.iter().all(|&k| k == std::io::ErrorKind::Other),
        "seed={seed}: error_prob alone must never produce StorageFull, got {kinds:?}"
    );

    let mut enospc_only = DiskConfig::default();
    enospc_only.set_enospc_prob(0.5);
    let kinds = run(seed, enospc_only);
    assert!(
        !kinds.is_empty(),
        "seed={seed}: expected some failures at p=0.5"
    );
    assert!(
        kinds.iter().all(|&k| k == std::io::ErrorKind::StorageFull),
        "seed={seed}: enospc_prob alone must never produce Other, got {kinds:?}"
    );

    let mut both = DiskConfig::default();
    both.set_enospc_prob(0.3);
    both.set_error_prob(0.3);
    let a = run(seed, both.clone());
    let b = run(seed, both);
    assert_eq!(
        a, b,
        "seed={seed}: the enospc-vs-generic bucket choice must be reproducible"
    );
    assert!(
        a.contains(&std::io::ErrorKind::StorageFull),
        "seed={seed}: expected at least one ENOSPC over 40 tries"
    );
    assert!(
        a.contains(&std::io::ErrorKind::Other),
        "seed={seed}: expected at least one generic error over 40 tries"
    );
}

/// With `enospc_prob` at its default (0), `error_prob`'s own draw/comparison
/// is byte-identical to before this knob existed: the fault schedule for a
/// generic-error-only config is unaffected by ENOSPC's addition to the code
/// path.
#[test]
fn enospc_default_off_leaves_the_generic_error_schedule_unchanged() {
    fn run(seed: u64) -> (Vec<bool>, Vec<String>) {
        let mut sim = Simulator::new(seed);
        let mut cfg = DiskConfig::default();
        cfg.set_error_prob(0.4);
        sim.set_disk_config(cfg);
        let results = Arc::new(Mutex::new(Vec::new()));
        {
            let env = sim.env(nid(0));
            let out = Arc::clone(&results);
            env.clone().spawn_task(async move {
                for i in 0..30u8 {
                    let ok = env.append("f", &[i]).await.is_ok();
                    out.lock().unwrap().push(ok);
                }
            });
            sim.run();
        }
        let r = results.lock().unwrap().clone();
        (r, sim.trace_lines())
    }

    // Same seed as `injected_error_schedule_is_reproducible_from_the_seed`
    // above, which pins the exact same config's outcome sequence — this test
    // pins that ENOSPC's addition to `inject_disk_fault` did not perturb it.
    let seed = 0xD15C_0002;
    let (results, trace) = run(seed);
    assert!(
        results.iter().any(|&ok| ok) && results.iter().any(|&ok| !ok),
        "seed={seed}: at p=0.4 over 30 ops both outcomes should occur"
    );
    assert!(
        trace
            .iter()
            .any(|l| l.contains("DISKFAULT") && l.contains("kind=error")),
        "seed={seed}: a generic-only config must tag its faults kind=error, never kind=enospc"
    );
    assert!(
        !trace.iter().any(|l| l.contains("kind=enospc")),
        "seed={seed}: enospc_prob defaults to 0 — no ENOSPC fault should ever fire"
    );
}

/// fsync-acked-but-lost (`DiskConfig::set_fsync_lie_prob`): with the fault
/// always firing, `sync` still returns `Ok`, but a subsequent crash loses
/// the bytes anyway — exactly like an un-synced tail, even though the caller
/// was told the fsync succeeded. With the fault off (default), the same
/// script persists normally across a crash.
#[test]
fn fsync_lie_acks_but_a_crash_still_loses_the_bytes() {
    fn run(seed: u64, lie_prob: f64) -> (bool, Vec<u8>) {
        let mut sim = Simulator::new(seed);
        // First write+sync genuinely persists (fault still off) so the test
        // can tell "the fault lost a specific later write" apart from "no
        // write was ever really durable at all".
        {
            let env = sim.env(nid(0));
            env.clone().spawn_task(async move {
                env.append("wal", b"KEPT").await.unwrap();
                env.sync("wal").await.unwrap();
            });
            sim.run();
        }
        let mut cfg = DiskConfig::default();
        cfg.set_fsync_lie_prob(lie_prob);
        sim.set_disk_config(cfg);
        let sync_ok = Arc::new(Mutex::new(false));
        {
            let env = sim.env(nid(0));
            let out = Arc::clone(&sync_ok);
            env.clone().spawn_task(async move {
                env.append("wal", b"maybe-lost").await.unwrap();
                let ok = env.sync("wal").await.is_ok();
                *out.lock().unwrap() = ok;
            });
            sim.run();
        }
        sim.crash(nid(0));
        sim.restart(nid(0));
        let after = Arc::new(Mutex::new(Vec::new()));
        {
            let env = sim.env(nid(0));
            let o = Arc::clone(&after);
            env.clone().spawn_task(async move {
                *o.lock().unwrap() = env.read("wal").await.unwrap();
            });
            sim.run();
        }
        (*sync_ok.lock().unwrap(), after.lock().unwrap().clone())
    }

    let seed = 0xD15C_0009;

    // Always lies: `sync` acks (`Ok`) but the second write never actually
    // became durable, so the crash loses it — only the first, genuinely
    // synced write survives.
    let (ok_a, bytes_a) = run(seed, 1.0);
    let (ok_b, bytes_b) = run(seed, 1.0);
    assert!(ok_a, "seed={seed}: a lied-to sync must still return Ok");
    assert_eq!(
        ok_a, ok_b,
        "seed={seed}: the lie itself must be reproducible"
    );
    assert_eq!(
        bytes_a, bytes_b,
        "seed={seed}: surviving bytes must be reproducible"
    );
    assert_eq!(
        bytes_a, b"KEPT",
        "seed={seed}: the lied-about write must be lost on crash despite the Ok ack, \
         got {bytes_a:?}"
    );

    // Default off: the same script persists both writes across the crash.
    let (ok_default, bytes_default) = run(seed, 0.0);
    assert!(ok_default, "seed={seed}");
    assert_eq!(
        bytes_default, b"KEPTmaybe-lost",
        "seed={seed}: with the fault off, a genuinely synced write must survive a crash"
    );
}
