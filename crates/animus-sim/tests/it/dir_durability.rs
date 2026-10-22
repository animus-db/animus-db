//! Directory-entry (namespace) durability of the `SimEnv` disk (PR #27).
//!
//! The `ProdEnv` contract these pin: a file's name is durable only after the
//! first `sync` of it (which fsyncs the parent directory chain), or after a
//! completed `replace`/`commit_staged`/`link`; a `remove` is NOT made durable
//! by anything but a later directory fsync. A crash therefore (1) drops a
//! never-synced created file entirely, (2) may bring an unsynced-removed file
//! back with its last durable bytes (when
//! `DiskConfig::set_resurrect_unsynced_removes_on_crash_prob` is armed), and
//! (3) never exposes a half-done `replace`.
//!
//! Replay any seeded test with `ANIMUS_SEED=<seed> cargo test -p animus-sim
//! --test it dir_durability::<name>`.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Disk, EnvExt, nid};
use animus_sim::{DiskConfig, Simulator};

fn seed_or(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Run `f` on node 0 to completion inside the simulator and return its output.
fn run<T, F, Fut>(sim: &mut Simulator, f: F) -> T
where
    T: Send + 'static,
    F: FnOnce(animus_sim::SimEnv) -> Fut,
    Fut: Future<Output = T> + Send + 'static,
{
    let out = Arc::new(Mutex::new(None));
    let env = sim.env(nid(0));
    let slot = Arc::clone(&out);
    let fut = f(env.clone());
    env.spawn_task(async move {
        *slot.lock().unwrap() = Some(fut.await);
    });
    sim.run_for(Duration::from_millis(50));
    out.lock().unwrap().take().expect("task did not finish")
}

fn crash_restart(sim: &Simulator) {
    sim.crash(nid(0));
    sim.restart(nid(0));
}

fn armed(p: f64) -> DiskConfig {
    let mut c = DiskConfig::default();
    c.set_resurrect_unsynced_removes_on_crash_prob(p);
    c
}

#[test]
fn never_synced_created_file_vanishes_on_crash() {
    let mut sim = Simulator::new(seed_or(0xD1A0_0001));
    run(&mut sim, |e| async move {
        e.append("never", b"abc").await.unwrap();
        e.append("synced", b"xyz").await.unwrap();
        e.sync("synced").await.unwrap();
        e.append("synced", b"tail").await.unwrap(); // un-synced tail only
    });
    crash_restart(&sim);
    let (names, synced, never) = run(&mut sim, |e| async move {
        (
            e.list().await.unwrap(),
            e.read("synced").await.unwrap(),
            e.read("never").await.unwrap(),
        )
    });
    assert_eq!(names, vec!["synced".to_string()], "list after crash");
    assert_eq!(synced, b"xyz");
    assert!(never.is_empty());
}

#[test]
fn vanish_also_applies_under_torn_tails() {
    let mut cfg = DiskConfig::default();
    cfg.torn_tail_on_crash = true;
    let mut sim = Simulator::new(seed_or(0xD1A0_0002));
    sim.set_disk_config(cfg);
    run(&mut sim, |e| async move {
        e.append("never", &[7u8; 64]).await.unwrap();
        e.append("kept", b"a").await.unwrap();
        e.sync("kept").await.unwrap();
        e.append("kept", &[9u8; 64]).await.unwrap();
    });
    crash_restart(&sim);
    let (names, kept) = run(&mut sim, |e| async move {
        (e.list().await.unwrap(), e.read("kept").await.unwrap())
    });
    assert_eq!(names, vec!["kept".to_string()]);
    assert!(!kept.is_empty() && kept.len() < 65, "strict prefix of tail");
}

#[test]
fn replace_is_durable_and_atomic_across_crash() {
    let mut sim = Simulator::new(seed_or(0xD1A0_0003));
    run(&mut sim, |e| async move {
        // A brand-new name made only by `replace` survives with no sync.
        e.replace("manifest", b"v1").await.unwrap();
        e.replace("manifest", b"v2-longer").await.unwrap();
        // replace over a file with an un-synced tail drops the tail.
        e.append("wal", b"old").await.unwrap();
        e.sync("wal").await.unwrap();
        e.append("wal", b"buffered").await.unwrap();
        e.replace("wal", b"new").await.unwrap();
    });
    crash_restart(&sim);
    let (m, w) = run(&mut sim, |e| async move {
        (
            e.read("manifest").await.unwrap(),
            e.read("wal").await.unwrap(),
        )
    });
    assert_eq!(m, b"v2-longer");
    assert_eq!(w, b"new");
}

#[test]
fn replace_with_a_slow_data_phase_crashes_to_the_whole_old_or_whole_new() {
    for seed in 0..8u64 {
        let seed = seed_or(0xD1A0_0004 + seed);
        let mut cfg = DiskConfig::default();
        cfg.set_replace_data_delay(Duration::from_millis(20));
        let mut sim = Simulator::new(seed);
        sim.set_disk_config(cfg);
        run(&mut sim, |e| async move {
            e.replace("m", b"old").await.unwrap()
        });
        let e2 = sim.env(nid(0));
        e2.spawn_task({
            let e = e2.clone();
            async move {
                e.replace("m", b"new!").await.unwrap();
            }
        });
        // Crash at a seed-chosen instant, before or after the 20ms swap.
        sim.run_for(Duration::from_millis(5 + (seed % 4) * 10));
        // Kill the process too: the in-flight replace must not complete later.
        sim.crash(nid(0));
        sim.stop(nid(0));
        sim.restart(nid(0));
        let got = run(&mut sim, |e| async move { e.read("m").await.unwrap() });
        assert!(
            got == b"old" || got == b"new!",
            "half-state {got:?} (seed={seed})"
        );
    }
}

#[test]
fn staged_name_is_an_orphan_until_commit_then_durable() {
    let mut sim = Simulator::new(seed_or(0xD1A0_0005));
    run(&mut sim, |e| async move {
        e.replace("f", b"old").await.unwrap();
        e.stage_replace("f", b"staged").await.unwrap();
    });
    crash_restart(&sim);
    let (names, f) = run(&mut sim, |e| async move {
        let n = e.list().await.unwrap();
        let f = e.read("f").await.unwrap();
        e.stage_replace("f", b"staged2").await.unwrap();
        e.commit_staged("f").await.unwrap();
        (n, f)
    });
    assert_eq!(names, vec!["f".to_string()], "unsynced .tmp name vanishes");
    assert_eq!(f, b"old");
    crash_restart(&sim);
    let (names, f) = run(&mut sim, |e| async move {
        (e.list().await.unwrap(), e.read("f").await.unwrap())
    });
    assert_eq!(names, vec!["f".to_string()]);
    assert_eq!(f, b"staged2");
}

#[test]
fn link_destination_is_durable_without_a_sync() {
    let mut sim = Simulator::new(seed_or(0xD1A0_0006));
    run(&mut sim, |e| async move {
        e.append("src", b"sst").await.unwrap();
        e.sync("src").await.unwrap();
        e.link("src", "dst").await.unwrap();
    });
    crash_restart(&sim);
    let d = run(&mut sim, |e| async move { e.read("dst").await.unwrap() });
    assert_eq!(d, b"sst");
}

#[test]
fn unsynced_remove_stays_removed_by_default() {
    let mut sim = Simulator::new(seed_or(0xD1A0_0007));
    run(&mut sim, |e| async move {
        e.append("sst", b"data").await.unwrap();
        e.sync("sst").await.unwrap();
        e.remove("sst").await.unwrap();
    });
    crash_restart(&sim);
    let names = run(&mut sim, |e| async move { e.list().await.unwrap() });
    assert!(
        names.is_empty(),
        "default model: a remove sticks ({names:?})"
    );
}

#[test]
fn armed_unsynced_remove_resurrects_with_its_durable_bytes() {
    let mut sim = Simulator::new(seed_or(0xD1A0_0008));
    sim.set_disk_config(armed(1.0));
    run(&mut sim, |e| async move {
        e.append("sst", b"data").await.unwrap();
        e.sync("sst").await.unwrap();
        e.append("sst", b"-unsynced").await.unwrap();
        e.remove("sst").await.unwrap();
        // A never-name-durable file removed is just gone (nothing to restore).
        e.append("tmp", b"x").await.unwrap();
        e.remove("tmp").await.unwrap();
    });
    crash_restart(&sim);
    let (names, sst) = run(&mut sim, |e| async move {
        (e.list().await.unwrap(), e.read("sst").await.unwrap())
    });
    assert_eq!(names, vec!["sst".to_string()]);
    assert_eq!(sst, b"data", "orphan returns with durable bytes only");
    assert!(
        sim.trace_lines()
            .iter()
            .any(|l| l.contains("DISKRESURRECT")),
        "resurrection is traced"
    );
}

#[test]
fn a_later_directory_fsync_makes_the_remove_permanent() {
    for via in ["sync", "replace", "link", "commit"] {
        let mut sim = Simulator::new(seed_or(0xD1A0_0009));
        sim.set_disk_config(armed(1.0));
        let via_s = via.to_string();
        run(&mut sim, move |e| async move {
            e.append("old", b"data").await.unwrap();
            e.sync("old").await.unwrap();
            e.remove("old").await.unwrap();
            match via_s.as_str() {
                "sync" => {
                    e.append("other", b"1").await.unwrap();
                    e.sync("other").await.unwrap(); // first sync: dir fsync
                }
                "replace" => e.replace("other", b"1").await.unwrap(),
                "link" => {
                    e.replace("s", b"1").await.unwrap();
                    e.link("s", "other").await.unwrap();
                }
                _ => {
                    e.stage_replace("other", b"1").await.unwrap();
                    e.commit_staged("other").await.unwrap();
                }
            }
        });
        crash_restart(&sim);
        let names = run(&mut sim, |e| async move { e.list().await.unwrap() });
        assert!(!names.contains(&"old".to_string()), "via {via}: {names:?}");
    }
}

#[test]
fn only_the_first_sync_of_a_file_fsyncs_the_directory() {
    // ProdEnv's `dir_synced` memo: a second sync of an already-synced name
    // does NOT re-fsync the directory, so it cannot make a later remove
    // permanent. This is the contract the model must not be stronger than.
    let mut sim = Simulator::new(seed_or(0xD1A0_000A));
    sim.set_disk_config(armed(1.0));
    run(&mut sim, |e| async move {
        e.append("a", b"1").await.unwrap();
        e.sync("a").await.unwrap();
        e.append("b", b"2").await.unwrap();
        e.sync("b").await.unwrap();
        e.remove("b").await.unwrap();
        e.append("a", b"3").await.unwrap();
        e.sync("a").await.unwrap(); // memoized: no dir fsync
    });
    crash_restart(&sim);
    let (names, b) = run(&mut sim, |e| async move {
        (e.list().await.unwrap(), e.read("b").await.unwrap())
    });
    assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
    assert_eq!(b, b"2");
}

#[test]
fn subdirectory_removes_need_a_sync_in_their_chain() {
    let mut sim = Simulator::new(seed_or(0xD1A0_000B));
    sim.set_disk_config(armed(1.0));
    run(&mut sim, |e| async move {
        e.append("db/x", b"1").await.unwrap();
        e.sync("db/x").await.unwrap();
        e.append("other/y", b"1").await.unwrap();
        e.sync("other/y").await.unwrap();
        e.remove("db/x").await.unwrap();
        // A dir fsync in an unrelated subdirectory does not cover `db`.
        e.append("other/z", b"1").await.unwrap();
        e.sync("other/z").await.unwrap();
    });
    crash_restart(&sim);
    let names = run(&mut sim, |e| async move { e.list().await.unwrap() });
    assert!(names.contains(&"db/x".to_string()), "{names:?}");
}

#[test]
fn partial_resurrection_is_seed_driven_and_reproducible() {
    fn run_once(seed: u64) -> Vec<String> {
        let mut sim = Simulator::new(seed);
        sim.set_disk_config(armed(0.5));
        run(&mut sim, |e| async move {
            for i in 0..16u8 {
                let n = format!("f{i:02}");
                e.append(&n, &[i]).await.unwrap();
                e.sync(&n).await.unwrap();
            }
            // Sync of fresh files above memoised; remove all, no new dir fsync.
            for i in 0..16u8 {
                e.remove(&format!("f{i:02}")).await.unwrap();
            }
        });
        crash_restart(&sim);
        run(&mut sim, |e| async move { e.list().await.unwrap() })
    }
    let seed = seed_or(0xD1A0_000C);
    let a = run_once(seed);
    assert_eq!(a, run_once(seed), "same seed, same outcome (seed={seed})");
    assert!(
        !a.is_empty() && a.len() < 16,
        "p=0.5 over 16 files should resurrect some but not all: {a:?} (seed={seed})"
    );
}
