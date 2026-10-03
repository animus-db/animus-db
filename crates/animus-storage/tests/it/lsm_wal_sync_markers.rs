//! Issue #1142 regression: the LSM WAL's torn-tail proof across **coalesced**
//! group-commit batches (WAL v2 sync markers).
//!
//! A group-commit batch is many writers' frames in one append + one fsync, so a
//! crash can leave several un-synced frames, and `animus-sim`'s crash model
//! (`torn_tail_on_crash` + `corrupt_on_crash`) keeps a random strict prefix of
//! that region and flips one byte *anywhere inside the retained region* —
//! never in previously synced bytes (`Simulator::crash`). So "a valid frame
//! follows the bad one, so it is not a torn tail" (the v1 rule) wrongly
//! refused a correct writer's file in 76 of 300 seeds. v2 sync markers fix it;
//! these tests pin it:
//!
//! * the coalesced tear reopens (and keeps every acked write), 300 seeds, with
//!   1..=7 concurrent writers and 0..=3 prior synced rounds (so markers exist);
//! * the marker's *own* sync failing, and a sync failure followed by more
//!   acked rounds, are covered by the same sweep;
//! * damage to an already-synced frame before a marker is still refused
//!   (`synced_frame_before_a_marker_corrupted_is_refused`);
//! * a recovered v1 segment keeps working without markers.

use animus_env::{EnvExt, nid};
use animus_sim::{DiskConfig, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use futures::executor::block_on;

const PREFIX: &str = "db/";

fn opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 1 << 20,
        compaction_trigger: 100,
        target_table_bytes: 1 << 20,
        level_fanout: 8,
        wal_segment_bytes: 1 << 20,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: false,
        background_maintenance: false,
    }
}

fn faults(sync_fail: bool) -> DiskConfig {
    let mut cfg = DiskConfig::default();
    cfg.torn_tail_on_crash = true;
    cfg.corrupt_on_crash = true;
    if sync_fail {
        cfg.set_sync_error_prob(1.0);
    }
    cfg
}

/// One run: `rounds` acked sequential puts (so sync markers exist), then —
/// with tear + corrupt armed and every fsync failing — `writers` concurrent
/// puts coalesce into one never-acked batch, then (`recover_rounds`) the disk
/// heals, more rounds are acked, and a second failing batch follows. Crash,
/// strict reopen. Returns the reopen error text, or the acked keys that were
/// lost.
pub(crate) fn run_coalesced_tear(
    seed: u64,
    rounds: u64,
    writers: u64,
    recover_rounds: u64,
) -> Result<Vec<String>, String> {
    let mut sim = Simulator::new(seed);
    let mut acked: Vec<String> = Vec::new();
    {
        let e = block_on(LsmEngine::open_with(sim.env(nid(0)), PREFIX, opts())).expect("open");
        let mut ver = 0u64;
        block_on(async {
            for i in 0..rounds {
                ver += 1;
                let k = format!("a{i}");
                e.put(k.as_bytes(), b"v", ver).await.unwrap();
                acked.push(k);
            }
        });
        for phase in 0..2u64 {
            sim.set_disk_config(faults(true));
            for i in 0..writers {
                ver += 1;
                let e = e.clone();
                let v = ver;
                sim.env(nid(0)).spawn_task(async move {
                    // never acked: the group fsync fails
                    let _ = e.put(format!("u{phase}-{i}").as_bytes(), b"v", v).await;
                });
            }
            sim.run();
            if phase == 0 && recover_rounds > 0 {
                // The disk heals: more acked rounds ride after the failed one.
                sim.set_disk_config(DiskConfig::default());
                block_on(async {
                    for i in 0..recover_rounds {
                        ver += 1;
                        let k = format!("b{i}");
                        e.put(k.as_bytes(), b"v", ver).await.unwrap();
                        acked.push(k);
                    }
                });
            } else if phase == 0 {
                break;
            }
        }
    }
    sim.crash(nid(0));
    sim.set_disk_config(faults(false));
    match block_on(LsmEngine::open_with(sim.env(nid(0)), PREFIX, opts())) {
        Err(e) => Err(e.to_string()),
        Ok(e) => Ok(acked
            .into_iter()
            .filter(|k| block_on(e.get(k.as_bytes())).unwrap().is_none())
            .collect()),
    }
}

fn depth() -> u64 {
    std::env::var("ANIMUS_LSM_MARKER_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300)
}

#[test]
fn coalesced_unsynced_batch_torn_and_flipped_still_reopens() {
    let n = depth();
    let (mut refused, mut lost) = (0, 0);
    let mut first = None;
    for seed in 0..n {
        let r = run_coalesced_tear(seed, seed % 4, 1 + seed % 7, (seed / 7) % 3);
        match r {
            Err(e) => {
                refused += 1;
                first.get_or_insert((seed, e));
            }
            Ok(l) if !l.is_empty() => {
                lost += 1;
                first.get_or_insert((seed, format!("acked lost: {l:?}")));
            }
            Ok(_) => {}
        }
    }
    assert!(
        refused == 0 && lost == 0,
        "wrongful refusals {refused}/{n}, acked-lost seeds {lost}, first={first:?}"
    );
}

/// The counterpart: a **synced** frame that sits before a marker, damaged at
/// rest (`corrupt_durable`, no crash), is real corruption and must still be a
/// loud refusal, not a silently shortened log.
/// One seed of the refusal check (also a `lsm_disk_faults` corpus cell).
pub(crate) fn synced_frame_corruption_is_refused(seed: u64) {
    let sim = Simulator::new(seed);
    {
        let e = block_on(LsmEngine::open_with(sim.env(nid(0)), PREFIX, opts())).expect("open");
        block_on(async {
            // 4 sequential synced rounds: markers ride rounds 2..=4, so
            // rounds 1..=3 start before the last marker.
            for i in 0..4u64 {
                e.put(format!("a{i}").as_bytes(), b"value-value", i + 1)
                    .await
                    .unwrap();
            }
        });
    }
    sim.crash(nid(0));
    // Flip one durable byte inside the first frame, the first marker or the
    // second frame (frames here are 38 bytes, a marker 17, after a 5-byte
    // header): all start before the last marker.
    let off = 5 + (seed % 60);
    assert!(sim.corrupt_durable(nid(0), "db/wal-000000", off));
    let r = block_on(LsmEngine::open_with(sim.env(nid(0)), PREFIX, opts()));
    assert!(
        r.is_err(),
        "seed={seed} off={off}: a flipped synced byte before a marker was silently accepted"
    );
}

#[test]
fn synced_frame_before_a_marker_corrupted_is_refused() {
    for seed in 0..depth().min(60) {
        synced_frame_corruption_is_refused(seed);
    }
}
