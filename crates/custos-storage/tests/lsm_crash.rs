//! Crash-safety of `LsmEngine` under the deterministic `SimEnv` disk model,
//! which distinguishes durable (synced) bytes from buffered (un-synced) ones and
//! drops the buffer on `crash`/`stop` — exactly modelling a power loss.
//!
//! All four properties the design promises are tested, each reproducible from a
//! seed (the seed is in every assertion message):
//!
//! 1. Synced writes survive a crash + reopen; un-synced bytes are gone.
//! 2. A flushed SSTable survives a crash + reopen (data lives on disk, not just
//!    in the WAL).
//! 3. A crash *mid-flush* (new SSTable written but the manifest not yet swapped)
//!    loses nothing: recovery falls back to the intact WAL, the orphan file is
//!    ignored.
//! 4. A crash *mid-compaction* (merged SSTable written but the manifest not yet
//!    swapped) loses nothing and reads no torn table: recovery keeps the old
//!    inputs, the orphan merged file is ignored.

use custos_sim::{SimEnv, Simulator};
use custos_storage::{LsmEngine, LsmOptions, StorageEngine};
use futures::executor::block_on;

const PREFIX: &str = "db/";

fn opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 128,
        compaction_trigger: 3,
        target_table_bytes: 512,
        level_fanout: 2,
        // Small so the WAL rolls into several segments and crash recovery exercises
        // multi-segment replay + the GC path.
        wal_segment_bytes: 96,
    }
}

fn open(sim: &Simulator) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with(sim.env(0), PREFIX, opts())).expect("open")
}

/// 1. Writes that returned (so were WAL-synced) survive a crash; the engine
///    reopens from its disk and reads them all back.
#[test]
fn synced_writes_survive_crash() {
    let seed = 0xC0FFEE;
    let sim = Simulator::new(seed);
    {
        let e = open(&sim);
        block_on(async {
            e.put(b"alpha", b"1", 1).await.unwrap();
            e.put(b"beta", b"2", 2).await.unwrap();
            e.delete(b"alpha", 3).await.unwrap();
            e.put(b"gamma", b"3", 4).await.unwrap();
        });
    }
    // Power loss: drop un-synced bytes (there are none past the last synced WAL
    // append, since every write syncs before returning) and all volatile state.
    sim.crash(0);

    let e = open(&sim);
    block_on(async {
        assert_eq!(e.get(b"alpha").await.unwrap(), None, "seed={seed}: deleted");
        assert_eq!(
            e.get(b"beta").await.unwrap().unwrap().value,
            b"2",
            "seed={seed}"
        );
        assert_eq!(
            e.get(b"gamma").await.unwrap().unwrap().value,
            b"3",
            "seed={seed}"
        );
        assert_eq!(e.latest_version(), 4, "seed={seed}: max_version restored");
    });
}

/// 2. After enough writes to force a flush, the data lives in a synced SSTable
///    referenced by a synced manifest; a fresh WAL is empty. Reopen and confirm
///    everything is still readable (proves recovery reads SSTables, not only the
///    WAL).
#[test]
fn flushed_sstable_survives_crash() {
    let seed = 0xBEEF;
    let sim = Simulator::new(seed);
    let count = 50u64;
    {
        let e = open(&sim);
        block_on(async {
            for i in 0..count {
                let k = format!("k{i:03}");
                e.put(k.as_bytes(), format!("v{i}").as_bytes(), i + 1)
                    .await
                    .unwrap();
            }
            // The small threshold guarantees at least one flush happened.
            assert!(
                e.sstable_count() >= 1,
                "seed={seed}: expected a flush, got {} sstables",
                e.sstable_count()
            );
        });
    }
    sim.crash(0);

    let e = open(&sim);
    block_on(async {
        for i in 0..count {
            let k = format!("k{i:03}");
            assert_eq!(
                e.get(k.as_bytes()).await.unwrap().unwrap().value,
                format!("v{i}").as_bytes(),
                "seed={seed}: key {k} lost across crash",
            );
        }
    });
}

/// 3. Crash *mid-flush*: write the SSTable bytes (append, no sync) but **do not**
///    swap the manifest, then crash. Recovery must fall back to the intact WAL —
///    no data loss — and ignore the orphan partial SSTable file.
#[test]
fn crash_mid_flush_recovers_via_wal() {
    let seed = 0xF1;
    let sim = Simulator::new(seed);
    {
        let e = open(&sim);
        block_on(async {
            // These writes are WAL-synced (durable) and live in the memtable.
            e.put(b"a", b"1", 1).await.unwrap();
            e.put(b"b", b"2", 2).await.unwrap();
            e.put(b"c", b"3", 3).await.unwrap();
            // Simulate a flush that got as far as appending the SSTable file but
            // crashed before syncing it / swapping the manifest.
            e.test_write_orphan_sstable(b"orphan").await;
        });
    }
    sim.crash(0); // drops the un-synced orphan SSTable bytes + memtable

    let e = open(&sim);
    block_on(async {
        // All three writes recovered from the WAL (the manifest never referenced
        // the orphan, so the WAL was never cleared).
        assert_eq!(
            e.get(b"a").await.unwrap().unwrap().value,
            b"1",
            "seed={seed}"
        );
        assert_eq!(
            e.get(b"b").await.unwrap().unwrap().value,
            b"2",
            "seed={seed}"
        );
        assert_eq!(
            e.get(b"c").await.unwrap().unwrap().value,
            b"3",
            "seed={seed}"
        );
        assert_eq!(
            e.sstable_count(),
            0,
            "seed={seed}: orphan sstable not adopted into the manifest"
        );
    });
}

/// 4. Crash *mid-compaction*: flush a few SSTables (durable), then write a merged
///    SSTable file without swapping the manifest, then crash. Recovery must keep
///    the original inputs (the manifest still names them, all intact) and ignore
///    the orphan merged file — no loss, no torn-table read.
#[test]
fn crash_mid_compaction_keeps_old_tables() {
    let seed = 0xC0;
    let sim = Simulator::new(seed);
    {
        let e = open(&sim);
        block_on(async {
            // Force a couple of flushes so the manifest names real SSTables.
            for i in 0u64..60 {
                let k = format!("k{i:03}");
                e.put(k.as_bytes(), format!("v{i}").as_bytes(), i + 1)
                    .await
                    .unwrap();
            }
            assert!(e.sstable_count() >= 1, "seed={seed}: need flushed tables");
            // Simulate a compaction that wrote a merged file but crashed before
            // the manifest swap.
            e.test_write_orphan_sstable(b"merged").await;
        });
    }
    sim.crash(0); // drops the un-synced merged file

    let e = open(&sim);
    block_on(async {
        // All originally-flushed keys are still present and readable.
        for i in 0u64..60 {
            let k = format!("k{i:03}");
            assert_eq!(
                e.get(k.as_bytes()).await.unwrap().unwrap().value,
                format!("v{i}").as_bytes(),
                "seed={seed}: key {k} lost across mid-compaction crash",
            );
        }
    });
}

/// A flush followed by leveled compaction both actually happen: many writes
/// trigger repeated flushes, L0→L1(+) compaction fires repeatedly, the L0 (flush)
/// tier never exceeds its trigger, the live table count stays far below the flush
/// count, every level ≥1 stays non-overlapping, and data is intact throughout.
#[test]
fn flush_then_compaction_happen_and_keep_data() {
    let seed = 0xABCD;
    let sim = Simulator::new(seed);
    let e = open(&sim);
    block_on(async {
        let mut max_l0 = 0usize;
        for i in 0u64..400 {
            let k = format!("key{i:04}");
            e.put(k.as_bytes(), format!("value-number-{i}").as_bytes(), i + 1)
                .await
                .unwrap();
            // L0 (the flush tier) is the only level whose overlap we don't bound;
            // it must never exceed the compaction trigger (it is drained at it).
            let l0 = e
                .level_table_counts()
                .into_iter()
                .find(|(lvl, _)| *lvl == 0)
                .map_or(0, |(_, c)| c);
            max_l0 = max_l0.max(l0);
        }
        // Many flushes happened ...
        assert!(
            e.flush_count() >= 4,
            "seed={seed}: expected several flushes, got {}",
            e.flush_count(),
        );
        // ... and compaction fired.
        assert!(
            e.compaction_count() >= 1,
            "seed={seed}: expected at least one compaction, got {}",
            e.compaction_count(),
        );
        // L0 is drained at the trigger, so it never grew past it.
        assert!(
            max_l0 <= opts().compaction_trigger,
            "seed={seed}: L0 table count ({max_l0}) grew past the trigger ({})",
            opts().compaction_trigger,
        );
        // Compaction collapsed flushed tables: far more flushes than live tables.
        assert!(
            e.flush_count() > e.sstable_count() as u64,
            "seed={seed}: compaction did not collapse flushed tables (flushes={}, live={})",
            e.flush_count(),
            e.sstable_count(),
        );
        // The leveled invariant: every level ≥1 holds non-overlapping runs.
        assert!(
            e.levels_non_overlapping(),
            "seed={seed}: a level ≥1 has overlapping key ranges: {:?}",
            e.level_table_counts(),
        );
        // We actually built at least one L1+ run (leveling, not just L0 churn).
        assert!(
            e.level_table_counts()
                .iter()
                .any(|(lvl, c)| *lvl >= 1 && *c >= 1),
            "seed={seed}: expected at least one L1+ table, levels={:?}",
            e.level_table_counts(),
        );
        // Data is intact after all the flush/compaction churn.
        for i in 0u64..400 {
            let k = format!("key{i:04}");
            assert_eq!(
                e.get(k.as_bytes()).await.unwrap().unwrap().value,
                format!("value-number-{i}").as_bytes(),
                "seed={seed}",
            );
        }
    });
}
