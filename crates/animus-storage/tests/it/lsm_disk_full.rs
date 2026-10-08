//! `LsmEngine` under **ENOSPC** (R-01 (d) residual, issue #1218; ADR 0074 §2
//! amendment). `SimEnv`'s `DiskConfig::set_enospc_prob` injects
//! `ErrorKind::StorageFull` on disk ops; the engine must classify it
//! ([`StorageError::StorageFull`]) and recover without losing, duplicating or
//! reordering data:
//!
//! - a WAL commit that hits ENOSPC applies **nothing** (the caller may retry
//!   the identical call) and a short write it left behind is cut back before
//!   later acked writes ride the segment;
//! - a flush that hits ENOSPC leaves the memtable + WAL intact and its partial
//!   SSTable removed, and a later flush succeeds;
//! - a compaction that hits ENOSPC leaves its inputs authoritative and no
//!   orphan outputs on disk;
//! - inline post-write maintenance ENOSPC is **deferred**, never surfaced as a
//!   failure of the (already durable) write.
//!
//! Every scenario is a pure function of its seed (`ANIMUS_SEED` replays); the
//! depth knob is `ANIMUS_LSM_DISK_FAULT_SEEDS` (shared with `lsm_disk_faults`;
//! default 1 canonical seed per cell).

use animus_env::{Disk, nid};
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, MergeOp, StorageEngine, StorageError};
use animus_test::corpus;
use futures::executor::block_on;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

const PREFIX: &str = "db/";

/// Corpus-wide counters proving each flaky window really injected the failure
/// it targets (a single seed may legitimately dodge a 25-35% window).
static FLUSH_FAILURES: AtomicU64 = AtomicU64::new(0);
static COMPACTION_FAILURES: AtomicU64 = AtomicU64::new(0);
static DEFERRALS: AtomicU64 = AtomicU64::new(0);

fn opts(flush: usize, trigger: usize) -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: flush,
        compaction_trigger: trigger,
        target_table_bytes: 256,
        level_fanout: 2,
        wal_segment_bytes: 1 << 20,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: false,
        background_maintenance: false,
    }
}

/// Never auto-flushes or compacts: the test drives both explicitly.
fn manual_opts() -> LsmOptions {
    opts(1 << 20, 1 << 20)
}

fn open(sim: &Simulator, o: LsmOptions) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with(sim.env(nid(0)), PREFIX, o)).expect("open")
}

fn full(p: f64) -> DiskConfig {
    let mut cfg = DiskConfig::default();
    cfg.set_enospc_prob(p);
    cfg
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i:03}").into_bytes()
}

fn val(i: u64) -> Vec<u8> {
    format!("value-{i}-{}", "x".repeat(24)).into_bytes()
}

fn op(i: u64) -> MergeOp {
    MergeOp {
        key: key(i),
        value: Some(val(i)),
        version: i + 1,
    }
}

/// The `sst-*` files physically on the node's disk.
fn sst_files(sim: &Simulator) -> Vec<String> {
    let env = sim.env(nid(0));
    block_on(env.list())
        .expect("list")
        .into_iter()
        .filter(|n| n.starts_with(&format!("{PREFIX}sst-")))
        .collect()
}

fn assert_all(e: &LsmEngine<SimEnv>, want: &BTreeMap<u64, ()>, seed: u64, what: &str) {
    block_on(async {
        for &i in want.keys() {
            let got = e.get(&key(i)).await.expect("get");
            assert_eq!(
                got.map(|v| v.value),
                Some(val(i)),
                "seed={seed}: {what}: key {i} lost or wrong"
            );
        }
    });
}

/// A WAL commit that hits ENOSPC returns `StorageFull`, applies nothing, and
/// the identical retry succeeds once space returns; the data survives a crash.
fn scenario_wal_commit_enospc_applies_nothing_and_retries(seed: u64) {
    let sim = Simulator::new(seed);
    let e = open(&sim, manual_opts());
    block_on(e.merge_batch(vec![op(0)])).expect("baseline write");
    sim.set_disk_config(full(1.0));
    let err = block_on(e.merge_batch(vec![op(1), op(2)])).expect_err("must fail on a full disk");
    assert!(
        matches!(err, StorageError::StorageFull(_)),
        "seed={seed}: ENOSPC must classify as StorageFull, got {err:?}"
    );
    sim.set_disk_config(DiskConfig::default());
    for i in [1, 2] {
        assert_eq!(
            block_on(e.get(&key(i))).unwrap().map(|v| v.value),
            None,
            "seed={seed}: a failed commit must apply nothing (key {i} visible)"
        );
    }
    block_on(e.merge_batch(vec![op(1), op(2)])).expect("retry after space returns");
    sim.crash(nid(0));
    let e = open(&sim, manual_opts());
    let want: BTreeMap<u64, ()> = (0..3).map(|i| (i, ())).collect();
    assert_all(&e, &want, seed, "after crash+reopen");
}

/// ProdEnv can leave a short write behind a failed append. Emulate it (a torn
/// frame appended + synced onto the active segment between the failed commit
/// and the retry) and require that the next acked writes survive a restart:
/// the engine must cut the tail back before riding the segment again.
fn scenario_wal_short_write_tail_is_repaired(seed: u64) {
    let sim = Simulator::new(seed);
    let e = open(&sim, manual_opts());
    block_on(e.merge_batch(vec![op(0)])).expect("baseline");
    sim.set_disk_config(full(1.0));
    block_on(e.merge_batch(vec![op(1)])).expect_err("full");
    sim.set_disk_config(DiskConfig::default());
    // The short write a real ENOSPC left behind: part of a frame header.
    let env = sim.env(nid(0));
    block_on(async {
        let seg = format!(
            "{PREFIX}wal-{:06}",
            e.wal_segments().last().copied().unwrap()
        );
        env.append(&seg, &[0, 0, 1, 0, 0xde, 0xad, 0xbe])
            .await
            .expect("append torn tail");
        env.sync(&seg).await.expect("sync torn tail");
    });
    for i in 1..6 {
        block_on(e.merge_batch(vec![op(i)])).expect("writes after the repair");
    }
    sim.crash(nid(0));
    let e = open(&sim, manual_opts());
    let want: BTreeMap<u64, ()> = (0..6).map(|i| (i, ())).collect();
    assert_all(&e, &want, seed, "acked writes behind a torn ENOSPC tail");
}

/// A failed flush leaves the memtable + WAL intact and no orphan SSTable; the
/// retry onto free space succeeds and a crash recovers the same state.
fn scenario_flush_enospc_is_clean_and_retries(seed: u64) {
    let sim = Simulator::new(seed);
    let e = open(&sim, manual_opts());
    let mut want = BTreeMap::new();
    for i in 0..20 {
        block_on(e.merge_batch(vec![op(i)])).expect("write");
        want.insert(i, ());
    }
    let (mem_before, tables_before) = (e.memtable_len(), e.sstable_count());
    sim.set_disk_config(full(1.0));
    let err = block_on(e.flush_now()).expect_err("flush must fail on a full disk");
    assert!(err.is_storage_full(), "seed={seed}: got {err:?}");
    sim.set_disk_config(DiskConfig::default());
    assert_eq!(
        e.memtable_len(),
        mem_before,
        "seed={seed}: memtable changed"
    );
    assert_eq!(
        e.sstable_count(),
        tables_before,
        "seed={seed}: table leaked"
    );
    assert!(
        sst_files(&sim).is_empty(),
        "seed={seed}: orphan sstable left"
    );
    assert_all(&e, &want, seed, "after failed flush");
    block_on(e.flush_now()).expect("flush retries onto free space");
    assert_eq!(e.sstable_count(), 1, "seed={seed}");
    assert_eq!(e.memtable_len(), 0, "seed={seed}");
    assert_all(&e, &want, seed, "after retried flush");
    sim.crash(nid(0));
    let e = open(&sim, manual_opts());
    assert_all(&e, &want, seed, "after crash+reopen");
}

/// A flush that fails *part-way* (some blocks written) removes its partial
/// output; repeated attempts under a flaky full disk never leak a file and
/// never lose a write.
fn scenario_flush_partial_output_is_removed(seed: u64) {
    let sim = Simulator::new(seed);
    let e = open(&sim, manual_opts());
    let mut want = BTreeMap::new();
    for i in 0..60 {
        block_on(e.merge_batch(vec![op(i)])).expect("write");
        want.insert(i, ());
    }
    sim.set_disk_config(full(0.35));
    let mut failures = 0u64;
    for _ in 0..40 {
        match block_on(e.flush_now()) {
            Ok(()) => break,
            Err(err) => {
                assert!(err.is_storage_full(), "seed={seed}: {err:?}");
                failures += 1;
                assert_eq!(
                    sst_files(&sim).len(),
                    e.sstable_count(),
                    "seed={seed}: failed flush left an orphan output"
                );
            }
        }
    }
    sim.set_disk_config(DiskConfig::default());
    block_on(e.flush_now()).expect("flush once space returns");
    FLUSH_FAILURES.fetch_add(failures, Ordering::Relaxed);
    assert_eq!(sst_files(&sim).len(), e.sstable_count(), "seed={seed}");
    assert_all(&e, &want, seed, "after flaky flush");
    sim.crash(nid(0));
    let e = open(&sim, manual_opts());
    assert_all(&e, &want, seed, "after crash+reopen");
}

/// A compaction that fails (ENOSPC mid-output) keeps its inputs authoritative
/// and removes every partial output; the retry completes with the merged view
/// unchanged.
fn scenario_compaction_enospc_keeps_inputs_and_cleans_outputs(seed: u64) {
    let sim = Simulator::new(seed);
    let e = open(&sim, manual_opts());
    let mut want = BTreeMap::new();
    // Several overlapping L0 tables (manual flushes only).
    for round in 0..4u64 {
        for i in 0..24 {
            let k = i * 2 + (round % 2);
            block_on(e.merge_batch(vec![MergeOp {
                key: key(k),
                value: Some(val(k)),
                version: round * 100 + i + 1,
            }]))
            .expect("write");
            want.insert(k, ());
        }
        block_on(e.flush_now()).expect("flush");
    }
    drop(e);
    // Reopen with a low compaction trigger: the four L0 tables are now due.
    let e = open(&sim, opts(1 << 20, 3));
    let tables_before = e.sstable_count();
    assert!(tables_before >= 4, "seed={seed}: need several tables");
    sim.set_disk_config(full(0.2));
    let mut failed = false;
    for _ in 0..40 {
        match block_on(e.compact_now()) {
            Ok(()) => break,
            Err(err) => {
                failed = true;
                assert!(err.is_storage_full(), "seed={seed}: {err:?}");
                assert_eq!(
                    sst_files(&sim).len(),
                    e.sstable_count(),
                    "seed={seed}: failed compaction left an orphan output (or lost an input)"
                );
            }
        }
    }
    sim.set_disk_config(DiskConfig::default());
    COMPACTION_FAILURES.fetch_add(u64::from(failed), Ordering::Relaxed);
    block_on(e.compact_now()).expect("compaction onto free space");
    assert!(e.compaction_count() > 0, "seed={seed}: nothing compacted");
    assert_eq!(sst_files(&sim).len(), e.sstable_count(), "seed={seed}");
    assert_all(&e, &want, seed, "after compaction retries");
    sim.crash(nid(0));
    let e = open(&sim, opts(1 << 20, 3));
    assert_all(&e, &want, seed, "after crash+reopen");
}

/// Inline maintenance ENOSPC after a durable write is deferred: a write that
/// returns `Ok` is present, a write that returns `Err` is `StorageFull` and
/// absent, no panic, and everything acked survives a crash once space returns.
fn scenario_inline_maintenance_enospc_is_deferred(seed: u64) {
    let sim = Simulator::new(seed);
    let e = open(&sim, opts(256, 3));
    sim.set_disk_config(full(0.25));
    let mut acked = BTreeMap::new();
    for i in 0..150u64 {
        match block_on(e.merge_batch(vec![op(i)])) {
            Ok(()) => {
                acked.insert(i, ());
                // Present immediately even if post-write maintenance failed.
                if let Ok(Some(v)) = block_on(e.get(&key(i))) {
                    assert_eq!(v.value, val(i), "seed={seed}");
                }
            }
            Err(err) => assert!(
                err.is_storage_full(),
                "seed={seed}: only StorageFull may surface under ENOSPC, got {err:?}"
            ),
        }
    }
    assert!(!acked.is_empty(), "seed={seed}: some writes must succeed");
    DEFERRALS.fetch_add(e.maintenance_deferral_count(), Ordering::Relaxed);
    sim.set_disk_config(DiskConfig::default());
    // Space is back: the next write re-attempts the deferred maintenance.
    block_on(e.merge_batch(vec![op(1000)])).expect("write after space returns");
    acked.insert(1000, ());
    sim.crash(nid(0));
    let e = open(&sim, opts(256, 3));
    assert_all(
        &e,
        &acked,
        seed,
        "acked writes across a deferred-maintenance run",
    );
}

#[test]
fn lsm_disk_full_corpus() {
    let depth = corpus::seeds_from_env("ANIMUS_LSM_DISK_FAULT_SEEDS");
    type Cell = (&'static str, fn(u64));
    let cells: &[Cell] = &[
        (
            "wal_commit_enospc_applies_nothing",
            scenario_wal_commit_enospc_applies_nothing_and_retries,
        ),
        (
            "wal_short_write_tail_repaired",
            scenario_wal_short_write_tail_is_repaired,
        ),
        (
            "flush_enospc_clean_and_retries",
            scenario_flush_enospc_is_clean_and_retries,
        ),
        (
            "flush_partial_output_removed",
            scenario_flush_partial_output_is_removed,
        ),
        (
            "compaction_enospc_keeps_inputs",
            scenario_compaction_enospc_keeps_inputs_and_cleans_outputs,
        ),
        (
            "inline_maintenance_deferred",
            scenario_inline_maintenance_enospc_is_deferred,
        ),
    ];
    for (name, f) in cells {
        corpus::for_each_seed(name, depth, f);
    }
    assert!(
        FLUSH_FAILURES.load(Ordering::Relaxed) > 0,
        "no flush ever hit ENOSPC"
    );
    assert!(
        COMPACTION_FAILURES.load(Ordering::Relaxed) > 0,
        "no compaction ever hit ENOSPC"
    );
    assert!(
        DEFERRALS.load(Ordering::Relaxed) > 0,
        "no inline maintenance ENOSPC was ever deferred"
    );
}
