//! Issue #1389: the flush threshold is sized against a per-node memtable budget
//! (`FlushBudget`), not fixed at the test-sized 64 KiB.
//!
//! Workload: `LsmEngine<SimEnv>` with the production sizes except the flush
//! threshold, hashed 8-byte keys, steady 1 KiB puts of incompressible values,
//! `ANIMUS_ADAPTIVE_FLUSH_MB` (default 16) ingested. Oracles per seed:
//!
//! 1. **Benefit**: compaction input bytes per ingested byte with the adaptive
//!    threshold (a lone hot engine gets the 4 MiB cap) is materially lower than
//!    with the fixed 64 KiB one (we require at most 60 %; measured ratio is
//!    printed with `ANIMUS_ADAPTIVE_FLUSH_PRINT=1`).
//! 2. **Negative control**: with thousands of other engines holding memtable
//!    bytes the share falls to the floor (64 KiB) and the engine is back at the
//!    fixed-threshold write amplification (within 10 %), proving the gain comes
//!    from the larger flush and not from some other change.
//! 3. **Memory**: the memtable never exceeds the engine's stall cap
//!    (4 x threshold) plus one write, and the final state equals a model.
//! 4. **Runtime change**: raising the active count mid-run shrinks the next
//!    flushes to the floor.
//!
//! Knobs: `ANIMUS_ADAPTIVE_FLUSH_SEEDS=K` (default 1), `ANIMUS_SEED=<seed>`.

use std::collections::BTreeMap;

use animus_env::{Metric, MetricsHandle, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{FlushBudget, LsmEngine, LsmOptions, StorageEngine};
use futures::executor::block_on;

const PREFIX: &str = "db/";
const VALUE: usize = 1024;
const FLOOR: usize = 64 << 10;
const CAP: usize = 4 << 20;
const NODE_BUDGET: usize = 256 << 20;

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
    {
        return vec![s];
    }
    let k: u64 = std::env::var("ANIMUS_ADAPTIVE_FLUSH_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    (0..k.max(1)).map(|i| 0x0001_389F + i).collect()
}

fn ingest_mb() -> u64 {
    std::env::var("ANIMUS_ADAPTIVE_FLUSH_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16)
}

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

struct Outcome {
    /// Compaction input bytes per ingested byte.
    wa: f64,
    flushes: u64,
    mem_hwm: usize,
}

/// Ingest `ingest_mb` of hashed puts. `budget` is the node budget to join, if
/// any; `bump_at` (fraction of the run) raises the node's active count to
/// `bump_active` mid-run.
fn run(seed: u64, budget: Option<&FlushBudget>) -> Outcome {
    let sim = Simulator::new(seed);
    let metrics = MetricsHandle::recording();
    let opts = LsmOptions {
        tombstone_grace_versions: 1 << 40,
        trust_monotonic_versions: true,
        ..LsmOptions::default()
    };
    let engine: LsmEngine<SimEnv> = block_on(LsmEngine::open_with_metrics(
        sim.env(nid(0)),
        PREFIX,
        opts,
        metrics.clone(),
    ))
    .expect("open");
    if let Some(b) = budget {
        engine.set_flush_budget(b);
    }
    let limit = ingest_mb() << 20;
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let (mut ingested, mut ver, mut rng, mut mem_hwm) = (0u64, 0u64, splitmix(seed), 0usize);
    block_on(async {
        while ingested < limit {
            rng = splitmix(rng);
            ver += 1;
            let key = splitmix(ver ^ 0xA5A5).to_be_bytes().to_vec();
            let mut value = Vec::with_capacity(VALUE);
            let mut x = rng ^ ver;
            while value.len() < VALUE {
                x = splitmix(x);
                value.extend_from_slice(&x.to_be_bytes());
            }
            engine.put(&key, &value, ver).await.expect("put");
            ingested += (key.len() + value.len()) as u64;
            model.insert(key, value);
            mem_hwm = mem_hwm.max(engine.memtable_bytes());
        }
        for (k, v) in &model {
            let got = engine.get(k).await.expect("get").map(|vv| vv.value);
            assert_eq!(got.as_ref(), Some(v), "seed {seed:#x}: key {k:?}");
        }
    });
    let merged = metrics
        .snapshot()
        .counters
        .get(&Metric::StorageCompactionBytesMerged)
        .copied()
        .unwrap_or(0);
    Outcome {
        wa: merged as f64 / ingested as f64,
        flushes: engine.flush_count(),
        mem_hwm,
    }
}

#[test]
fn adaptive_flush_cuts_compaction_bytes_per_ingested_byte() {
    for seed in seeds() {
        let fixed = run(seed, None);

        let lone = FlushBudget::new(NODE_BUDGET, FLOOR, CAP);
        let adaptive = run(seed, Some(&lone));
        assert_eq!(lone.active(), 0, "the dropped engine must leave the budget");

        // Negative control: 4096 other engines hold memtable bytes, so the
        // share is the floor.
        let crowded = FlushBudget::new(NODE_BUDGET, FLOOR, CAP);
        let others: Vec<_> = (0..4096)
            .map(|_| {
                let s = crowded.slot();
                s.set_dirty(true);
                s
            })
            .collect();
        assert_eq!(crowded.threshold(), FLOOR);
        let at_floor = run(seed, Some(&crowded));
        drop(others);

        if std::env::var("ANIMUS_ADAPTIVE_FLUSH_PRINT").is_ok() {
            eprintln!(
                "seed {seed:#x} fixed64K: WA {:.2} flushes {} | adaptive: WA {:.2} flushes {} \
                 mem_hwm {} | crowded(floor): WA {:.2} flushes {}  (adaptive/fixed = {:.2})",
                fixed.wa,
                fixed.flushes,
                adaptive.wa,
                adaptive.flushes,
                adaptive.mem_hwm,
                at_floor.wa,
                at_floor.flushes,
                adaptive.wa / fixed.wa
            );
        }
        assert!(
            adaptive.wa <= 0.6 * fixed.wa,
            "seed {seed:#x}: adaptive WA {:.2} is not materially below fixed 64 KiB WA {:.2}",
            adaptive.wa,
            fixed.wa
        );
        assert!(adaptive.flushes < fixed.flushes / 8, "seed {seed:#x}");
        // Negative control: at the floor the gain is gone.
        assert!(
            (at_floor.wa - fixed.wa).abs() <= 0.1 * fixed.wa,
            "seed {seed:#x}: floor-sized adaptive WA {:.2} differs from fixed {:.2}",
            at_floor.wa,
            fixed.wa
        );
        // Memory: inline maintenance flushes at the threshold, so the memtable
        // stays within the cap (4 x threshold) with room to spare.
        assert!(adaptive.mem_hwm <= CAP + 4 * VALUE, "seed {seed:#x}");
        assert!(at_floor.mem_hwm <= FLOOR + 4 * VALUE, "seed {seed:#x}");
    }
}

/// The share follows tablets coming and going while the engine runs: after the
/// node fills with other active engines the engine flushes at the floor, and
/// when they leave it goes back to the cap.
#[test]
fn threshold_follows_the_node_at_runtime() {
    let seed = seeds()[0];
    let sim = Simulator::new(seed);
    let budget = FlushBudget::new(64 << 20, FLOOR, CAP);
    let engine: LsmEngine<SimEnv> = block_on(LsmEngine::open_with(
        sim.env(nid(0)),
        PREFIX,
        LsmOptions {
            trust_monotonic_versions: true,
            ..LsmOptions::default()
        },
    ))
    .expect("open");
    engine.set_flush_budget(&budget);
    let value = vec![7u8; VALUE];
    let mut ver = 0u64;
    let mut put_until_flush = |n_flushes: u64| {
        block_on(async {
            let start = engine.flush_count();
            let mut peak = 0usize;
            while engine.flush_count() < start + n_flushes {
                ver += 1;
                engine
                    .put(&splitmix(ver).to_be_bytes(), &value, ver)
                    .await
                    .expect("put");
                peak = peak.max(engine.memtable_bytes());
            }
            peak
        })
    };
    // Alone: flushes at the cap.
    let alone = put_until_flush(1);
    assert!(alone > CAP - 4 * VALUE, "alone peak {alone}");
    // 1023 other active engines: 64 MiB / 1024 = 64 KiB.
    let others: Vec<_> = (0..1023)
        .map(|_| {
            let s = budget.slot();
            s.set_dirty(true);
            s
        })
        .collect();
    let crowded = put_until_flush(2);
    assert!(crowded <= 2 * FLOOR, "crowded peak {crowded}");
    // They leave (quiesce / release): back to the cap.
    drop(others);
    let alone_again = put_until_flush(2);
    assert!(alone_again > CAP - 4 * VALUE, "peak {alone_again}");
}
