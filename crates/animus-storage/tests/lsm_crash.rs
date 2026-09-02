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
//! 5. A **lying** flush `sync` (`DiskConfig::set_fsync_lie_prob`), revealed by a
//!    later process exit (`Simulator::stop`, mirroring `raftkv_linearizable.rs`'s
//!    `StopRestart`): properties 1-4 above assume an *honest* disk, and here the
//!    manifest swap and the WAL-segment GC that follow a flush both commit the
//!    instant the lying `sync` returns `Ok`, with nothing in `LsmEngine` able to
//!    tell a lie from a genuine sync ahead of time. This layer's job is to fail
//!    **loudly** on reopen (an ordinary `Err`, detected and reported) rather
//!    than silently serving an engine short a table's data — the property this
//!    scenario pins. Recovering the *replica* is a layer up: issue #554
//!    proposes the host reconciler treat an unopenable engine as lost and
//!    rebuild it fresh from the group, not yet built — see `open`'s own doc and
//!    `docs/engineering-lessons.md`.
//!
//! **Corpus shape** (ADR 0061 rung B1, house corpus doctrine): follows the
//! standard `animus_test::corpus` scaffolding every fault-injection corpus in
//! this repo uses (see `raftkv_linearizable.rs`/`reconciler_corpus.rs` for the
//! canonical worked examples) — a frozen, name-seeded scenario list (one cell
//! per property above, each a `fn(seed: u64)`), depth knob
//! **`ANIMUS_LSM_CRASH_SEEDS`** (default 1 = the 6 frozen cells, byte-identical
//! to this file's own pre-corpus form — none of these scenarios' outcomes
//! depend on the sim seed at all, since none of them spawn concurrent tasks or
//! touch `DiskConfig`'s randomness, but the seed is threaded through anyway to
//! match house convention and so a future scenario that DOES need seed
//! diversity fits the same shape). See `lsm_disk_faults.rs` for the sibling
//! corpus over the *fault-injection* dimension (torn tails, corruption,
//! injected I/O errors) — kept as a separate knob, since the two files probe
//! genuinely different fault classes (plain crash/recovery correctness here,
//! vs. `DiskConfig`-driven fault injection there).

use animus_env::nid;
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use animus_test::corpus::{self, SeedVariant};
use futures::executor::block_on;
use std::collections::BTreeSet;

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
        // Default-ish large grace: these tests assert durability/recovery, not GC.
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: false,
        background_maintenance: false,
    }
}

fn open(sim: &Simulator) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with(sim.env(nid(0)), PREFIX, opts())).expect("open")
}

/// 1. Writes that returned (so were WAL-synced) survive a crash; the engine
///    reopens from its disk and reads them all back.
fn scenario_synced_writes_survive_crash(seed: u64) {
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
    sim.crash(nid(0));

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
fn scenario_flushed_sstable_survives_crash(seed: u64) {
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
    sim.crash(nid(0));

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
fn scenario_crash_mid_flush_recovers_via_wal(seed: u64) {
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
    sim.crash(nid(0)); // drops the un-synced orphan SSTable bytes + memtable

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
fn scenario_crash_mid_compaction_keeps_old_tables(seed: u64) {
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
    sim.crash(nid(0)); // drops the un-synced merged file

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

/// 5. A flush whose SSTable-file `sync` **lies** (returns `Ok` without actually
///    promoting the file's buffered bytes to durable — `DiskConfig::
///    set_fsync_lie_prob`, ADR 0061 Decision 3), revealed by a following process
///    exit (`Simulator::stop`, the same primitive `raftkv_linearizable.rs`'s
///    `StopRestart` nemesis uses): the manifest is *already* durably swapped to
///    reference the now-empty table and the WAL segments the flush judged "now
///    covered" are *already* removed, both committed the instant the lying
///    `sync` returned `Ok` — nothing in `LsmEngine` can tell a lie from an
///    honest sync ahead of the crash that reveals it. Root cause of issue
///    #554's nightly `corpus-deep` failure
///    (`raftkv_lsm_full_corpus_is_linearizable`, scenario
///    `fsync_lie_stop_restart_early_3_s03`). What this scenario pins is that
///    `open` still fails **loudly** on reopen — an ordinary `Err`, never a
///    panic escaping this crate or a silent, incomplete-but-successful open.
///    That's this layer's whole job here: recovering the *replica* belongs
///    one layer up, in `animus-cp-data`'s host reconciler — issue #554
///    proposes treating an unopenable engine as lost and rebuilding it fresh
///    from the group (Raft catch-up), the same recovery the corpus's
///    `MemoryEngine` tier already gets on every restart; not yet built. A
///    future change that makes `open` swallow this and continue anyway
///    (serving missing/stale data) would be a correctness regression, not a
///    fix — this test exists to catch that.
fn scenario_fsync_lie_flush_survives_as_a_clean_open_error(seed: u64) {
    let sim = Simulator::new(seed);
    {
        let e = open(&sim);
        block_on(async {
            // Ordinary, honestly-synced writes — durable in the WAL (and
            // readable) before the lie is ever armed.
            e.put(b"a", b"1", 1).await.unwrap();
            e.put(b"b", b"2", 2).await.unwrap();
        });
        // Arm the lie for exactly the flush that follows: its SSTable file's
        // `sync` returns `Ok` but leaves the bytes buffered, not durable.
        let mut lying = DiskConfig::default();
        lying.set_fsync_lie_prob(1.0);
        sim.set_disk_config(lying);
        block_on(e.flush_now()).unwrap();
        assert_eq!(
            e.sstable_count(),
            1,
            "seed={seed}: expected exactly one (lyingly-synced) flushed table"
        );
        // Disarm before the crash, mirroring `raftkv_linearizable.rs`'s own
        // `HealAll` reset — keeps this scenario isolated to the one lying sync.
        sim.set_disk_config(DiskConfig::default());
    }
    // Process exit reveals the lie: the SSTable file's whole un-synced buffer
    // (its only copy of the flushed data) is dropped.
    sim.stop(nid(0));

    let result = block_on(LsmEngine::open_with(sim.env(nid(0)), PREFIX, opts()));
    let err = match result {
        Ok(_) => panic!(
            "seed={seed}: expected a clean open error after the lying flush was \
             revealed by a crash, engine opened successfully instead"
        ),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("corrupt sstable index"),
        "seed={seed}: expected a clean 'corrupt sstable index' open error, got: {msg}"
    );
}

/// A flush followed by leveled compaction both actually happen: many writes
/// trigger repeated flushes, L0→L1(+) compaction fires repeatedly, the L0 (flush)
/// tier never exceeds its trigger, the live table count stays far below the flush
/// count, every level ≥1 stays non-overlapping, and data is intact throughout.
fn scenario_flush_then_compaction_happen_and_keep_data(seed: u64) {
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

// ---------------------------------------------------------------------------
// The frozen corpus: a committed, deterministic generator (ADR 0061 rung B1 —
// mirrors `raftkv_linearizable.rs`/`reconciler_corpus.rs`'s own shape exactly).
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Scenario {
    name: String,
    seed: u64,
    run: fn(u64),
}

impl SeedVariant for Scenario {
    fn scenario_name(&self) -> &str {
        &self.name
    }
    fn reseeded(&self, name: String, seed: u64) -> Self {
        Scenario {
            name,
            seed,
            run: self.run,
        }
    }
}

/// Depth knob (`ANIMUS_LSM_CRASH_SEEDS`, default 1) — mirrors
/// `ANIMUS_RAFTKV_SEEDS`/`ANIMUS_RECONCILER_SEEDS`.
fn seeds_per_cell() -> usize {
    corpus::seeds_from_env("ANIMUS_LSM_CRASH_SEEDS")
}

macro_rules! scenario {
    ($name:expr, $f:ident) => {
        Scenario {
            name: $name.to_string(),
            seed: corpus::name_seed($name),
            run: $f,
        }
    };
}

fn scenario_cells() -> Vec<Scenario> {
    vec![
        scenario!(
            "synced_writes_survive_crash",
            scenario_synced_writes_survive_crash
        ),
        scenario!(
            "flushed_sstable_survives_crash",
            scenario_flushed_sstable_survives_crash
        ),
        scenario!(
            "crash_mid_flush_recovers_via_wal",
            scenario_crash_mid_flush_recovers_via_wal
        ),
        scenario!(
            "crash_mid_compaction_keeps_old_tables",
            scenario_crash_mid_compaction_keeps_old_tables
        ),
        scenario!(
            "fsync_lie_flush_survives_as_a_clean_open_error",
            scenario_fsync_lie_flush_survives_as_a_clean_open_error
        ),
        scenario!(
            "flush_then_compaction_happen_and_keep_data",
            scenario_flush_then_compaction_happen_and_keep_data
        ),
    ]
}

fn corpus() -> Vec<Scenario> {
    corpus::seed_expand(scenario_cells(), seeds_per_cell())
}

// ---------------------------------------------------------------------------
// The tests.
// ---------------------------------------------------------------------------

#[test]
fn lsm_crash_corpus_runs_every_scenario() {
    for s in corpus() {
        (s.run)(s.seed);
    }
}

/// Coverage/structural guard: names + seeds are unique, the frozen cells keep
/// their canonical name-derived seeds, and the corpus has not silently shrunk.
#[test]
fn lsm_crash_corpus_names_and_seeds_are_unique_and_frozen() {
    let cells = scenario_cells();
    assert!(
        cells.len() >= 6,
        "corpus shrank unexpectedly to {} cells",
        cells.len()
    );

    let names: BTreeSet<&str> = cells.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names.len(), cells.len(), "corpus names must be unique");
    let seeds: BTreeSet<u64> = cells.iter().map(|s| s.seed).collect();
    assert_eq!(seeds.len(), cells.len(), "corpus seeds must be unique");

    for cell in &cells {
        assert_eq!(
            cell.seed,
            corpus::name_seed(&cell.name),
            "frozen seed moved for {}",
            cell.name
        );
    }
}

/// Seed-depth lever (`ANIMUS_LSM_CRASH_SEEDS`): expanding by `k` yields exactly
/// `k×` scenarios, all uniquely named/seeded, and **variant 0 preserves the
/// canonical (frozen) name+seed** — growing depth never moves a regression
/// seed. Structural only (mirrors the sibling corpora's guard).
#[test]
fn lsm_crash_corpus_seed_expansion_is_additive_and_unique() {
    let base = scenario_cells();
    let k = 3;
    let expanded = corpus::seed_expand(scenario_cells(), k);
    assert_eq!(expanded.len(), base.len() * k);

    let names: BTreeSet<&str> = expanded.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names.len(), expanded.len(), "expanded names must be unique");
    let seeds: BTreeSet<u64> = expanded.iter().map(|s| s.seed).collect();
    assert_eq!(seeds.len(), expanded.len(), "expanded seeds must be unique");

    for b in &base {
        let kept = expanded
            .iter()
            .find(|s| s.name == b.name)
            .unwrap_or_else(|| panic!("base scenario {} missing after expansion", b.name));
        assert_eq!(kept.seed, b.seed, "seed moved for {}", b.name);
    }
    assert_eq!(corpus::seed_expand(scenario_cells(), 1).len(), base.len());
}

/// A single deterministic replay of one scenario twice must behave identically
/// (ADR 0003).
#[test]
fn lsm_crash_scenario_is_reproducible_from_its_seed() {
    let seed = corpus::name_seed("synced_writes_survive_crash");
    scenario_synced_writes_survive_crash(seed);
    scenario_synced_writes_survive_crash(seed);
}
