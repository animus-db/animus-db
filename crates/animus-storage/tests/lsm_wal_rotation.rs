//! WAL **segment rotation** (ADR 0008): the WAL is written as numbered segments
//! `<prefix>wal-NNNNNN`; the group commit rolls to a fresh segment past a byte
//! threshold, a flush `remove`s the segments it fully covers (bounding WAL size
//! without a whole-file rewrite), and recovery replays all live segments in order
//! to rebuild the memtable — equivalent to the old single-file replay.
//!
//! These run under the deterministic `SimEnv` disk model (durable/synced bytes vs.
//! buffered/un-synced; a crash drops the buffer), so every property is reproducible
//! from the seed in the assertion messages.

use animus_env::Disk;
use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use futures::executor::block_on;

const PREFIX: &str = "db/";

/// A tiny WAL segment budget so a handful of writes spans several segments, with a
/// flush threshold low enough that flushes happen and GC covered segments — but not
/// so low that every single write flushes (we want multiple segments live between
/// flushes).
fn opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 512,
        compaction_trigger: 100,
        target_table_bytes: 1 << 20,
        level_fanout: 8,
        wal_segment_bytes: 64,
    }
}

fn open(sim: &Simulator) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with(sim.env(0), PREFIX, opts())).expect("open")
}

/// Many writes roll the WAL across several segments; the active segment number
/// climbs as bytes accumulate. (Sanity that rotation actually happens before the
/// GC / recovery properties below mean anything.)
#[test]
fn writes_span_multiple_wal_segments() {
    let seed = 0x5E6;
    let sim = Simulator::new(seed);
    // No-flush variant: a huge flush threshold so the only thing that changes the
    // WAL is rotation, and we can watch the live set grow.
    let no_flush = LsmOptions {
        flush_threshold_bytes: 1 << 20,
        ..opts()
    };
    let e = block_on(LsmEngine::open_with(sim.env(0), PREFIX, no_flush)).expect("open");
    block_on(async {
        for i in 0u64..40 {
            let k = format!("key-{i:04}");
            e.merge(k.as_bytes(), b"some-value-bytes", i + 1)
                .await
                .unwrap();
        }
    });
    assert!(
        e.wal_segment_count() >= 3,
        "seed={seed}: expected the WAL to roll into several segments, got {} ({:?})",
        e.wal_segment_count(),
        e.wal_segments(),
    );
    // All data is readable across the live segments (it is all still in the memtable
    // here; the recovery test below proves it survives a reopen).
    block_on(async {
        for i in 0u64..40 {
            let k = format!("key-{i:04}");
            assert_eq!(
                e.get(k.as_bytes()).await.unwrap().unwrap().value,
                b"some-value-bytes",
                "seed={seed}",
            );
        }
    });
}

/// After a flush, the WAL segments the flush fully covered are **removed** from
/// disk and from the live set, while the active (partially-covered) segment
/// remains — so total WAL size is bounded instead of one growing file.
#[test]
fn flush_removes_covered_segments() {
    let seed = 0xC0FFEE5E6;
    let sim = Simulator::new(seed);
    let e = open(&sim);
    block_on(async {
        // Enough writes to fill several segments and force at least one flush.
        for i in 0u64..80 {
            let k = format!("key-{i:04}");
            e.merge(k.as_bytes(), b"v-with-some-bytes", i + 1)
                .await
                .unwrap();
        }
    });
    assert!(
        e.flush_count() >= 1,
        "seed={seed}: expected at least one flush, got {}",
        e.flush_count(),
    );

    // The segments the manifest now considers live; every earlier (covered) segment
    // file must be gone from disk.
    let live = e.wal_segments();
    let lowest_live = *live.iter().min().expect("at least the active segment");
    let env = sim.env(0);
    block_on(async {
        // Probe the segment files below the lowest live one: all removed.
        for seg in 0..lowest_live {
            let file = format!("{PREFIX}wal-{seg:06}");
            assert_eq!(
                env.size(&file).await.unwrap(),
                0,
                "seed={seed}: covered segment {seg} should have been removed",
            );
        }
        // The live segments still exist on disk.
        for &seg in &live {
            let file = format!("{PREFIX}wal-{seg:06}");
            assert!(
                env.size(&file).await.unwrap() > 0
                    // The freshly-rotated active segment may legitimately be empty if
                    // no write has landed in it yet; only assert presence isn't an
                    // error for sealed (non-max) ones.
                    || seg == *live.last().unwrap(),
                "seed={seed}: live segment {seg} unexpectedly empty/absent",
            );
        }
    });
    assert!(
        lowest_live > 0,
        "seed={seed}: a flush should have GC'd at least segment 0 (live={live:?})",
    );
}

/// Recovery replays **all** live segments in order: after writes that span several
/// segments and at least one flush, a crash + reopen restores every acked write
/// (the ones still in live WAL segments **and** the ones already flushed to an
/// SSTable), with the monotonic floor restored.
#[test]
fn recovery_replays_all_live_segments() {
    let seed = 0xBEEF5E6;
    let sim = Simulator::new(seed);
    let count = 120u64;
    {
        let e = open(&sim);
        block_on(async {
            for i in 0..count {
                let k = format!("key-{i:04}");
                // `put` keeps the global monotonic floor so we can assert it on
                // recovery; versions are strictly increasing.
                e.put(k.as_bytes(), format!("val-{i}").as_bytes(), i + 1)
                    .await
                    .unwrap();
            }
        });
        // Some data is in SSTables (flushed, covered segments GC'd) and some is in
        // the live WAL segments not yet flushed.
        assert!(
            e.flush_count() >= 1,
            "seed={seed}: expected a flush so recovery spans SSTables + WAL",
        );
        assert!(
            e.wal_segment_count() >= 1,
            "seed={seed}: expected at least one live WAL segment",
        );
    }
    // Power loss: drop un-synced bytes (none past the last synced WAL append, since
    // every write syncs before returning) and all volatile state.
    sim.crash(0);

    let e = open(&sim);
    block_on(async {
        for i in 0..count {
            let k = format!("key-{i:04}");
            assert_eq!(
                e.get(k.as_bytes()).await.unwrap().unwrap().value,
                format!("val-{i}").as_bytes(),
                "seed={seed}: key {k} lost across crash (multi-segment recovery)",
            );
        }
        assert_eq!(
            e.latest_version(),
            count,
            "seed={seed}: monotonic floor not restored across multi-segment recovery",
        );
    });
}

/// A crash *mid-rotation* — writes filling a fresh segment that has been created
/// but whose containing flush/manifest swap hasn't happened — recovers correctly:
/// every acked write survives, including those in the newest (not-yet-manifest)
/// segment. This is the WAL analogue of the mid-flush crash: recovery picks up live
/// segment files present on disk beyond the manifest's recorded set.
#[test]
fn crash_mid_rotation_recovers_all_acked_writes() {
    let seed = 0xF00D5E6;
    let sim = Simulator::new(seed);
    let count = 100u64;
    {
        let e = open(&sim);
        block_on(async {
            for i in 0..count {
                let k = format!("k{i:04}");
                e.put(k.as_bytes(), format!("v{i}").as_bytes(), i + 1)
                    .await
                    .unwrap();
            }
        });
        // The newest writes live in WAL segments created since the last flush — not
        // yet folded into an SSTable, possibly beyond what the manifest recorded.
        assert!(
            e.wal_segment_count() >= 2 || e.flush_count() >= 1,
            "seed={seed}: expected rotation and/or a flush to have happened",
        );
    }
    sim.crash(0);

    let e = open(&sim);
    block_on(async {
        for i in 0..count {
            let k = format!("k{i:04}");
            assert_eq!(
                e.get(k.as_bytes()).await.unwrap().unwrap().value,
                format!("v{i}").as_bytes(),
                "seed={seed}: acked write {k} lost across mid-rotation crash",
            );
        }
    });

    // Reopen again (idempotent recovery): replaying the same live segments a second
    // time must not change the observable state.
    sim.crash(0);
    let e2 = open(&sim);
    block_on(async {
        assert_eq!(
            e2.get(b"k0000").await.unwrap().unwrap().value,
            b"v0",
            "seed={seed}: re-recovery diverged",
        );
        assert_eq!(
            e2.get(&format!("k{:04}", count - 1).into_bytes())
                .await
                .unwrap()
                .unwrap()
                .value,
            format!("v{}", count - 1).as_bytes(),
            "seed={seed}: re-recovery lost the tail",
        );
    });
}

/// A flush followed by more writes that roll new segments, then a flush again:
/// recovery still reconstructs everything, proving segment numbers keep climbing
/// and GC + fresh-segment allocation interleave correctly across flushes.
#[test]
fn interleaved_flush_and_rotation_recover() {
    let seed = 0xABCDEF;
    let sim = Simulator::new(seed);
    {
        let e = open(&sim);
        block_on(async {
            for i in 0u64..200 {
                let k = format!("key-{i:05}");
                e.put(k.as_bytes(), format!("payload-{i}").as_bytes(), i + 1)
                    .await
                    .unwrap();
            }
        });
        assert!(
            e.flush_count() >= 2,
            "seed={seed}: expected several flushes, got {}",
            e.flush_count(),
        );
    }
    sim.crash(0);
    let e = open(&sim);
    block_on(async {
        for i in 0u64..200 {
            let k = format!("key-{i:05}");
            assert_eq!(
                e.get(k.as_bytes()).await.unwrap().unwrap().value,
                format!("payload-{i}").as_bytes(),
                "seed={seed}: key {k} lost across interleaved flush+rotation",
            );
        }
    });
}
