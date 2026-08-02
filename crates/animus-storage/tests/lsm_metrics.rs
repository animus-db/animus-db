//! Storage-engine observability (ADR 0015): the on-disk `LsmEngine` records
//! deterministic counters at the real LSM sites — memtable flushes, leveled
//! compactions (+ tables/bytes merged, tombstones reclaimed), SSTable block reads,
//! per-table Bloom hits/misses, and WAL segment rotations — all through the
//! `Env` metrics seam, all observe-only (they change no engine behavior).
//!
//! Under `SimEnv` `env.metrics()` is the no-op handle, so a test that wants to
//! *read* the counters threads a recording [`MetricsHandle`] in via
//! [`LsmEngine::open_with_metrics`] (the additive `*_with_metrics` pattern). Every
//! assertion is reproducible from a seed, and the recorded snapshot is asserted
//! byte-identical across two runs of the same seed (the determinism guarantee).

use animus_env::{Metric, MetricsHandle};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use futures::executor::block_on;

const PREFIX: &str = "db/";

/// Small thresholds so a modest workload rolls the memtable several times,
/// triggers an L0->L1 compaction, and rotates the WAL across several segments.
fn opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 256,
        compaction_trigger: 3,
        target_table_bytes: 1024,
        level_fanout: 2,
        wal_segment_bytes: 128,
        // Large grace: this run asserts flush/compaction/read counters, not GC.
        tombstone_grace_versions: 1 << 20,
    }
}

fn open(sim: &Simulator, metrics: MetricsHandle) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with_metrics(
        sim.env(0),
        PREFIX,
        opts(),
        metrics,
    ))
    .expect("open")
}

/// Drive a write workload heavy enough to flush + compact + rotate the WAL, then
/// do point reads (a present key and a proven-absent one), recording into
/// `metrics`. Returns nothing — the caller asserts on the handle.
fn run_workload(sim: &Simulator, metrics: &MetricsHandle) {
    let e = open(sim, metrics.clone());
    block_on(async {
        for i in 0u64..400 {
            let k = format!("key-{i:05}");
            e.put(k.as_bytes(), format!("value-number-{i}").as_bytes(), i + 1)
                .await
                .unwrap();
        }
        // Point reads that hit on-disk tables (block reads + Bloom hits), plus a
        // read for a key that was never written but falls inside a table's key
        // range (a Bloom miss that reads no block).
        let _ = e.get(b"key-00100").await.unwrap();
        let _ = e.get(b"key-00399").await.unwrap();
        // "key-0010X" sorts inside the [key-00000, key-00399] range but was never
        // written, so the per-table Bloom should rule it out (a miss).
        assert_eq!(
            e.get(b"key-0010X").await.unwrap(),
            None,
            "absent key reads None"
        );
    });
}

/// Every storage counter moves under a workload that forces a flush, a leveled
/// compaction, WAL rotation, and on-disk point reads.
#[test]
fn storage_counters_move_under_flush_and_compaction() {
    let seed = 0x0570_7A6E;
    let sim = Simulator::new(seed);
    let metrics = MetricsHandle::recording();
    run_workload(&sim, &metrics);

    let snap = metrics.snapshot();
    let get = |m: Metric| snap.counters.get(&m).copied().unwrap_or(0);

    assert!(
        get(Metric::StorageFlushes) >= 2,
        "seed={seed}: expected several flushes, got {}",
        get(Metric::StorageFlushes)
    );
    assert!(
        get(Metric::StorageCompactions) >= 1,
        "seed={seed}: expected at least one compaction, got {}",
        get(Metric::StorageCompactions)
    );
    assert!(
        get(Metric::StorageCompactionTablesMerged) >= 2,
        "seed={seed}: a compaction merges >=2 input tables, got {}",
        get(Metric::StorageCompactionTablesMerged)
    );
    assert!(
        get(Metric::StorageCompactionBytesMerged) > 0,
        "seed={seed}: a compaction folds input bytes",
    );
    assert!(
        get(Metric::StorageSstableBlockReads) >= 1,
        "seed={seed}: point reads against on-disk tables fetch blocks, got {}",
        get(Metric::StorageSstableBlockReads)
    );
    assert!(
        get(Metric::StorageBloomMisses) >= 1,
        "seed={seed}: the proven-absent in-range key is a Bloom miss, got {}",
        get(Metric::StorageBloomMisses)
    );
    assert!(
        get(Metric::StorageWalSegmentRotations) >= 1,
        "seed={seed}: the WAL rolled to a fresh segment, got {}",
        get(Metric::StorageWalSegmentRotations)
    );
}

/// The recorded metric snapshot is a pure function of the seed: two independent
/// runs of the same workload over the same seed yield a byte-identical text
/// export (the determinism guarantee, ADR 0003 + ADR 0015).
#[test]
fn storage_metrics_snapshot_is_byte_identical_across_runs() {
    let seed = 0xDEC1_5117;

    let m1 = MetricsHandle::recording();
    run_workload(&Simulator::new(seed), &m1);

    let m2 = MetricsHandle::recording();
    run_workload(&Simulator::new(seed), &m2);

    assert_eq!(
        m1.snapshot().to_text(),
        m2.snapshot().to_text(),
        "seed={seed}: storage metric snapshot must be byte-identical across runs"
    );
}

/// A Bloom-rejected point miss reads **zero** blocks while still being recorded as
/// a Bloom miss — proving the counter sits at the gate, not after a wasted read.
#[test]
fn bloom_miss_records_without_reading_a_block() {
    let seed = 0xB100_0155u64;
    let sim = Simulator::new(seed);
    let metrics = MetricsHandle::recording();
    let e = open(&sim, metrics.clone());
    block_on(async {
        // Enough writes to force at least one flush, so a real on-disk table with a
        // Bloom exists. Keys are dense in [k-00000, k-00299].
        for i in 0u64..300 {
            let k = format!("k-{i:05}");
            e.put(k.as_bytes(), b"v", i + 1).await.unwrap();
        }
        assert!(e.flush_count() >= 1, "seed={seed}: a table was flushed");
        e.reset_block_reads();
        let blocks_before = e.block_read_count();
        let misses_before = metrics
            .snapshot()
            .counters
            .get(&Metric::StorageBloomMisses)
            .copied()
            .unwrap_or(0);

        // A key inside the range but never written: the Bloom rules it out.
        assert_eq!(e.get(b"k-0010X").await.unwrap(), None, "seed={seed}");

        let blocks_after = e.block_read_count();
        let misses_after = metrics
            .snapshot()
            .counters
            .get(&Metric::StorageBloomMisses)
            .copied()
            .unwrap_or(0);
        assert!(
            misses_after > misses_before,
            "seed={seed}: the proven-absent in-range key must record a Bloom miss"
        );
        assert_eq!(
            blocks_after, blocks_before,
            "seed={seed}: a Bloom-rejected miss must read no block"
        );
    });
}

/// Tombstone GC counter: an aged tombstone (and the versions it shadows) reclaimed
/// during compaction bumps `storage_tombstones_reclaimed`.
#[test]
fn tombstone_gc_is_recorded() {
    let seed = 0x7085_704E;
    let sim = Simulator::new(seed);
    let metrics = MetricsHandle::recording();
    let grace = 64;
    let e = block_on(LsmEngine::open_with_metrics(
        sim.env(0),
        PREFIX,
        LsmOptions {
            flush_threshold_bytes: 256,
            compaction_trigger: 2,
            target_table_bytes: 1024,
            level_fanout: 2,
            wal_segment_bytes: 128,
            tombstone_grace_versions: grace,
        },
        metrics.clone(),
    ))
    .expect("open");
    block_on(async {
        // Write a value then delete it, then advance the version floor well past
        // the grace window with many further writes so the tombstone ages below
        // the GC floor and a compaction reclaims it.
        e.put(b"victim", b"hello", 1).await.unwrap();
        e.delete(b"victim", 2).await.unwrap();
        for i in 0u64..400 {
            let k = format!("filler-{i:05}");
            e.put(k.as_bytes(), format!("v{i}").as_bytes(), 100 + i)
                .await
                .unwrap();
        }
        // The victim now reads absent (tombstone reclaimed or still present, both
        // read absent) and the GC counter must have moved (the shadowed value +
        // the aged tombstone were physically dropped during a compaction).
        assert_eq!(e.get(b"victim").await.unwrap(), None, "seed={seed}");
    });
    let reclaimed = metrics
        .snapshot()
        .counters
        .get(&Metric::StorageTombstonesReclaimed)
        .copied()
        .unwrap_or(0);
    assert!(
        reclaimed >= 1,
        "seed={seed}: an aged tombstone (and its shadowed value) must be counted as reclaimed, got {reclaimed}"
    );
}
