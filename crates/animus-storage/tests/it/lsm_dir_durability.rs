//! `LsmEngine` under the `SimEnv` **directory-entry durability** model
//! (PR #27's contract, `docs/sim-coverage.md`): a file created by `append` is
//! gone after a crash unless a `sync` (first sync of the name) or a completed
//! `replace`/`link` made its name durable, and a `remove` that no later
//! directory fsync covered may *un-happen* (`DiskConfig::
//! set_resurrect_unsynced_removes_on_crash_prob`) - the file returns with its
//! last durable bytes.
//!
//! The churn workload repeats open -> put/delete/flush/compact (SSTable
//! creation, manifest swap via `replace`, obsolete-file `remove`) -> crash ->
//! reopen, with the adversary armed, and the oracle is "every acked write /
//! delete reads back exactly, `open` never errors". Every cell is a pure
//! function of its seed (`ANIMUS_SEED=<seed>` replays one; the seed is in every
//! message). Depth knob: **`ANIMUS_LSM_DIR_DURABILITY_SEEDS`** (default 1).
//!
//! The negative controls arm `DiskConfig::set_sync_skips_dir_fsync` (the
//! pre-PR-27 `ProdEnv`: `sync` does not make a new name durable); the same
//! oracle MUST fail, otherwise the cells prove nothing.

use animus_env::nid;
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use animus_test::corpus::{self, SeedVariant};
use futures::executor::block_on;
use std::collections::{BTreeMap, BTreeSet};

const PREFIX: &str = "db/";
const ROUNDS: usize = 8;
const KEYS: u64 = 40;

fn opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 128,
        compaction_trigger: 3,
        target_table_bytes: 512,
        level_fanout: 2,
        wal_segment_bytes: 96,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: false,
        background_maintenance: false,
        ..LsmOptions::default()
    }
}

/// splitmix64 - the workload's own deterministic stream (never `thread_rng`).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[derive(Clone, Copy)]
struct Knobs {
    resurrect: f64,
    torn: bool,
    /// Negative control: the pre-PR-27 `ProdEnv` (`sync` skips the dir fsync).
    legacy_sync: bool,
}

#[derive(Default, Debug)]
struct Stats {
    flushes: u64,
    compactions: u64,
    resurrections: usize,
}

type Model = BTreeMap<u64, Option<String>>;

fn check(e: &LsmEngine<SimEnv>, model: &Model, seed: u64, when: &str) -> Result<(), String> {
    block_on(async {
        for k in 0..KEYS {
            let want = model.get(&k).cloned().flatten();
            let got = e
                .get(format!("k{k:03}").as_bytes())
                .await
                .map_err(|err| format!("seed={seed} {when}: get k{k:03} errored: {err}"))?
                .map(|v| String::from_utf8_lossy(&v.value).into_owned());
            if got != want {
                return Err(format!(
                    "seed={seed} {when}: k{k:03} want {want:?} got {got:?} (acked write lost or stale value resurrected)"
                ));
            }
        }
        Ok(())
    })
}

/// The oracle. `Err` = a violation (an acked write lost, a stale value, or a
/// recovery error); the positive cells require `Ok`, the controls `Err`.
fn churn(seed: u64, knobs: Knobs, stats: &mut Stats) -> Result<(), String> {
    let sim = Simulator::new(seed);
    let mut cfg = DiskConfig::default();
    cfg.set_resurrect_unsynced_removes_on_crash_prob(knobs.resurrect);
    cfg.torn_tail_on_crash = knobs.torn;
    cfg.set_sync_skips_dir_fsync(knobs.legacy_sync);
    sim.set_disk_config(cfg);

    let mut rng = Rng(seed);
    let mut model: Model = BTreeMap::new();
    let mut version = 0u64;
    for round in 0..=ROUNDS {
        let e = block_on(LsmEngine::open_with(sim.env(nid(0)), PREFIX, opts()))
            .map_err(|err| format!("seed={seed} round {round}: open failed: {err}"))?;
        check(&e, &model, seed, &format!("round {round} after reopen"))?;
        if round == ROUNDS {
            break;
        }
        let n = 20 + rng.below(40);
        for _ in 0..n {
            version += 1;
            let k = rng.below(KEYS);
            let key = format!("k{k:03}");
            if rng.below(5) == 0 {
                block_on(e.delete(key.as_bytes(), version))
                    .map_err(|err| format!("seed={seed}: delete: {err}"))?;
                model.insert(k, None);
            } else {
                let v = format!("v{version}-{}", "x".repeat(rng.below(24) as usize));
                block_on(e.put(key.as_bytes(), v.as_bytes(), version))
                    .map_err(|err| format!("seed={seed}: put: {err}"))?;
                model.insert(k, Some(v));
            }
            if rng.below(16) == 0 {
                block_on(e.flush_now()).map_err(|err| format!("seed={seed}: flush: {err}"))?;
            }
            if rng.below(24) == 0 {
                block_on(e.compact_now()).map_err(|err| format!("seed={seed}: compact: {err}"))?;
            }
        }
        stats.flushes += e.flush_count();
        stats.compactions += e.compaction_count();
        drop(e);
        sim.crash(nid(0));
        sim.restart(nid(0));
    }
    stats.resurrections += sim
        .trace_lines()
        .iter()
        .filter(|l| l.contains("DISKRESURRECT"))
        .count();
    Ok(())
}

const ALL: Knobs = Knobs {
    resurrect: 1.0,
    torn: false,
    legacy_sync: false,
};

fn cell_all_orphans_return(seed: u64) {
    churn(seed, ALL, &mut Stats::default()).unwrap_or_else(|e| panic!("{e}"));
}
fn cell_some_orphans_return(seed: u64) {
    let k = Knobs {
        resurrect: 0.5,
        ..ALL
    };
    churn(seed, k, &mut Stats::default()).unwrap_or_else(|e| panic!("{e}"));
}
fn cell_orphans_and_torn_tails(seed: u64) {
    let k = Knobs {
        resurrect: 0.5,
        torn: true,
        ..ALL
    };
    churn(seed, k, &mut Stats::default()).unwrap_or_else(|e| panic!("{e}"));
}
fn cell_vanish_only(seed: u64) {
    let k = Knobs {
        resurrect: 0.0,
        ..ALL
    };
    churn(seed, k, &mut Stats::default()).unwrap_or_else(|e| panic!("{e}"));
}

/// Negative control: with `sync` not making new names durable, the very same
/// oracle must report a lost write or a failed recovery.
fn control_sync_without_dir_fsync_loses_acked_writes(seed: u64) {
    let k = Knobs {
        legacy_sync: true,
        ..ALL
    };
    let r = churn(seed, k, &mut Stats::default());
    assert!(
        r.is_err(),
        "seed={seed}: control did not bite - the oracle passed although `sync` \
         no longer made new files' names durable"
    );
}

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

macro_rules! scenario {
    ($f:ident) => {
        Scenario {
            name: stringify!($f).to_string(),
            seed: corpus::name_seed(stringify!($f)),
            run: $f,
        }
    };
}

fn scenario_cells() -> Vec<Scenario> {
    vec![
        scenario!(cell_all_orphans_return),
        scenario!(cell_some_orphans_return),
        scenario!(cell_orphans_and_torn_tails),
        scenario!(cell_vanish_only),
        scenario!(control_sync_without_dir_fsync_loses_acked_writes),
    ]
}

fn corpus() -> Vec<Scenario> {
    corpus::seed_expand(
        scenario_cells(),
        corpus::seeds_from_env("ANIMUS_LSM_DIR_DURABILITY_SEEDS"),
    )
}

#[test]
fn lsm_dir_durability_corpus_runs_every_scenario() {
    for s in corpus() {
        (s.run)(s.seed);
    }
}

#[test]
fn lsm_dir_durability_corpus_names_and_seeds_are_unique_and_frozen() {
    let cells = scenario_cells();
    let names: BTreeSet<&str> = cells.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names.len(), cells.len());
    let seeds: BTreeSet<u64> = cells.iter().map(|s| s.seed).collect();
    assert_eq!(seeds.len(), cells.len());
    for c in &cells {
        assert_eq!(c.seed, corpus::name_seed(&c.name));
    }
}

/// The cells are not vacuous: the workload really flushes, compacts (so it
/// swaps manifests and removes obsolete files) and the adversary really
/// brings removed files back.
#[test]
fn lsm_dir_durability_workload_exercises_the_namespace_operations() {
    let mut stats = Stats::default();
    let seed = corpus::name_seed("cell_all_orphans_return");
    churn(seed, ALL, &mut stats).unwrap_or_else(|e| panic!("{e}"));
    assert!(stats.flushes >= 8, "{stats:?} (seed={seed})");
    assert!(stats.compactions >= 2, "{stats:?} (seed={seed})");
    assert!(
        stats.resurrections >= 1,
        "no removed file was ever resurrected: {stats:?} (seed={seed})"
    );
}

#[test]
fn lsm_dir_durability_cell_is_reproducible_from_its_seed() {
    let seed = corpus::name_seed("cell_some_orphans_return");
    let k = Knobs {
        resurrect: 0.5,
        ..ALL
    };
    let (mut a, mut b) = (Stats::default(), Stats::default());
    churn(seed, k, &mut a).unwrap();
    churn(seed, k, &mut b).unwrap();
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
}
