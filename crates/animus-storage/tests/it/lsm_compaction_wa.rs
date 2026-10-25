//! Issue #1389: compaction write amplification must not grow with tablet size.
//!
//! The old policy compacted L0 into L1 every `compaction_trigger` L0 tables, and
//! with hashed keys that rewrites **all** of L1 each time, so bytes compacted per
//! ingested byte grew like `L1_bytes / flush_size` (a real 3-node cluster lost
//! 60-70 % throughput in 25 minutes). The byte-based L0 trigger plus L0->L0
//! merges bound it by a constant (about the fanout per level, times the number
//! of levels in play).
//!
//! The workload is `LsmEngine<SimEnv>` with the production sizes (64 KiB flush,
//! trigger 4, 2 MiB tables, fanout 4), hashed 8-byte keys and steady puts of 1 KiB
//! values (plus ~10 % overwrites and ~5 % deletes, checked against a model at the
//! end), ingested up to `ANIMUS_COMPACTION_WA_MB` (default 64). Per power-of-two
//! checkpoint it reads `storage_compaction_bytes_merged` (input bytes) and the
//! L0 table count.
//!
//! Oracles: (1) the windowed ratio (compaction input bytes per ingested byte
//! since the previous checkpoint) stays below `MAX_WINDOW_WA` at every
//! checkpoint from 2 MB up, and the last window is within 25 % of the one before
//! (it has plateaued: only the number of levels, not the size of L1, adds to it);
//! (2) the L0 table count never exceeds `2 x compaction_trigger` at a
//! checkpoint (the ADR 0081 stall keys on it); (3) the final state equals a
//! `BTreeMap` model and L1+ stays non-overlapping.
//!
//! Measured (seed 0x1389, windows at 2/4/8/16/32/64 MB): the pre-#1389 policy
//! gives 6.6, 12.8, 27.0, 18.9, 22.5, 21.5; the byte-based trigger gives 3.5,
//! 5.3, 6.4, 7.0, 9.5, 9.5. `MAX_WINDOW_WA` sits between them, so this test FAILS
//! on the old policy (run it against a checkout without the `lsm.rs` change).
//!
//! Knobs: `ANIMUS_COMPACTION_WA_SEEDS=K` (default 1), `ANIMUS_COMPACTION_WA_MB=N`,
//! `ANIMUS_COMPACTION_WA_PRINT=1` (print the table), `ANIMUS_SEED=<seed>`.

use std::collections::BTreeMap;

use animus_env::{Metric, MetricsHandle, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use futures::executor::block_on;

const PREFIX: &str = "db/";
const VALUE: usize = 1024;
/// Post-fix windows peak at about 9.5; the pre-fix policy reaches 27 by 8 MB.
const MAX_WINDOW_WA: f64 = 14.0;

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
    {
        return vec![s];
    }
    let k: u64 = std::env::var("ANIMUS_COMPACTION_WA_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    (0..k.max(1)).map(|i| 0x1389 + i).collect()
}

fn max_mb() -> u64 {
    std::env::var("ANIMUS_COMPACTION_WA_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64)
}

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Production sizes, inline maintenance (the policy is the same; no driver
/// needed), tombstones retained for the whole run.
fn opts() -> LsmOptions {
    LsmOptions {
        tombstone_grace_versions: 1 << 40,
        trust_monotonic_versions: true,
        ..LsmOptions::default()
    }
}

struct Row {
    ingested_mb: f64,
    window_wa: f64,
    l0: usize,
    tables: usize,
}

fn run(seed: u64) -> Vec<Row> {
    let sim = Simulator::new(seed);
    let metrics = MetricsHandle::recording();
    let engine: LsmEngine<SimEnv> = block_on(LsmEngine::open_with_metrics(
        sim.env(nid(0)),
        PREFIX,
        opts(),
        metrics.clone(),
    ))
    .expect("open");
    let merged = || {
        metrics
            .snapshot()
            .counters
            .get(&Metric::StorageCompactionBytesMerged)
            .copied()
            .unwrap_or(0)
    };

    let limit = max_mb() << 20;
    let mut model: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
    let mut inserted: Vec<u64> = Vec::new();
    let mut ingested = 0u64;
    let mut ver = 0u64;
    let mut next_ckpt = 1u64 << 20;
    let (mut prev_ingested, mut prev_merged) = (0u64, 0u64);
    let mut rows = Vec::new();
    let mut rng = splitmix(seed);

    block_on(async {
        while ingested < limit {
            rng = splitmix(rng);
            ver += 1;
            let roll = rng % 100;
            let (id, delete) = if roll < 10 && !inserted.is_empty() {
                // Overwrite an existing key.
                (inserted[(splitmix(rng) as usize) % inserted.len()], false)
            } else if roll < 15 && !inserted.is_empty() {
                (inserted[(splitmix(rng) as usize) % inserted.len()], true)
            } else {
                let id = ver;
                inserted.push(id);
                (id, false)
            };
            // Hashed key: uniformly spread over the whole key space.
            let key = splitmix(id ^ 0xA5A5).to_be_bytes().to_vec();
            if delete {
                engine.delete(&key, ver).await.expect("delete");
                model.insert(key, None);
            } else {
                // Incompressible: SSTable blocks are lz4-compressed.
                let mut value = Vec::with_capacity(VALUE);
                let mut x = rng ^ ver;
                while value.len() < VALUE {
                    x = splitmix(x);
                    value.extend_from_slice(&x.to_be_bytes());
                }
                engine.put(&key, &value, ver).await.expect("put");
                ingested += (key.len() + value.len()) as u64;
                model.insert(key, Some(value));
            }
            if ingested >= next_ckpt {
                let m = merged();
                let window_wa = (m - prev_merged) as f64 / (ingested - prev_ingested) as f64;
                let levels = engine.level_table_counts();
                rows.push(Row {
                    ingested_mb: ingested as f64 / f64::from(1u32 << 20),
                    window_wa,
                    l0: engine.l0_table_count(),
                    tables: levels.iter().map(|(_, c)| c).sum(),
                });
                prev_ingested = ingested;
                prev_merged = m;
                next_ckpt *= 2;
            }
        }
        // Oracle 3: the engine equals the model.
        for (k, v) in &model {
            let got = engine.get(k).await.expect("get").map(|vv| vv.value);
            assert_eq!(got.as_ref(), v.as_ref(), "seed {seed:#x}: key {k:?}");
        }
    });
    assert!(engine.levels_non_overlapping(), "seed {seed:#x}");
    rows
}

#[test]
fn compaction_write_amplification_is_bounded_as_the_engine_grows() {
    for seed in seeds() {
        let rows = run(seed);
        if std::env::var("ANIMUS_COMPACTION_WA_PRINT").is_ok() {
            for r in &rows {
                eprintln!(
                    "seed {seed:#x} ingested {:>7.1} MB  window WA {:>6.2}  L0 {:>3}  tables {:>4}",
                    r.ingested_mb, r.window_wa, r.l0, r.tables
                );
            }
        }
        let trigger = LsmOptions::default().compaction_trigger;
        for r in &rows {
            assert!(
                r.l0 <= 2 * trigger,
                "seed {seed:#x}: L0 holds {} tables at {:.1} MB (> 2 x trigger)",
                r.l0,
                r.ingested_mb
            );
            if r.ingested_mb >= 2.0 {
                assert!(
                    r.window_wa <= MAX_WINDOW_WA,
                    "seed {seed:#x}: window WA {:.2} at {:.1} MB exceeds {MAX_WINDOW_WA} \
                     (compaction write amplification grows with size, issue #1389)",
                    r.window_wa,
                    r.ingested_mb
                );
            }
        }
        // Plateau: the last window is within 25 % of the one before.
        if rows.len() >= 4 {
            let prev = rows[rows.len() - 2].window_wa;
            let last = rows[rows.len() - 1].window_wa;
            assert!(
                last <= 1.25 * prev + 1.0,
                "seed {seed:#x}: WA still growing, {prev:.2} then {last:.2}"
            );
        }
    }
}

/// Tiny tables and a tiny tombstone grace so L0->L0 merges, L0->L1 compactions
/// and tombstone GC all fire constantly; key/value sizes vary so the size-tiered
/// group excludes some L0 tables. A tombstone reclaimed by a merge that left an
/// older value behind in a sibling L0 table would resurrect it: every read is
/// checked against a model after every few operations.
#[test]
fn l0_merge_with_tombstone_gc_matches_a_model() {
    for seed in seeds() {
        let sim = Simulator::new(seed);
        let engine: LsmEngine<SimEnv> = block_on(LsmEngine::open_with(
            sim.env(nid(0)),
            PREFIX,
            LsmOptions {
                flush_threshold_bytes: 400,
                compaction_trigger: 3,
                target_table_bytes: 1500,
                level_fanout: 3,
                wal_segment_bytes: 2048,
                tombstone_grace_versions: 8,
                trust_monotonic_versions: true,
                ..LsmOptions::default()
            },
        ))
        .expect("open");
        let mut model: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        let mut rng = splitmix(seed);
        block_on(async {
            for ver in 1u64..4000 {
                rng = splitmix(rng);
                let key = (rng % 300).to_be_bytes()[4..].to_vec();
                if rng % 100 < 40 {
                    engine.delete(&key, ver).await.expect("delete");
                    model.insert(key, None);
                } else {
                    let len = 20 + (splitmix(rng) % 200) as usize;
                    let value = vec![(ver % 251) as u8; len];
                    engine.put(&key, &value, ver).await.expect("put");
                    model.insert(key, Some(value));
                }
                if ver % 97 == 0 {
                    for (k, v) in &model {
                        let got = engine.get(k).await.expect("get").map(|vv| vv.value);
                        assert_eq!(
                            got.as_ref(),
                            v.as_ref(),
                            "seed {seed:#x} ver {ver} key {k:?}"
                        );
                    }
                }
            }
        });
        assert!(engine.compaction_count() > 20, "seed {seed:#x}");
        assert!(engine.levels_non_overlapping(), "seed {seed:#x}");
    }
}

fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len + 8);
    let mut x = seed;
    while out.len() < len {
        x = splitmix(x);
        out.extend_from_slice(&x.to_be_bytes());
    }
    out.truncate(len);
    out
}

/// Directed: an L0->L0 merge of the small tables (a tombstone for `a` and one
/// more key) leaves a larger L0 table behind that still holds `a`'s old value,
/// and no L1 table's range covers `a` (while L0's combined range does overlap L1). Reclaiming the tombstone would resurrect the
/// value, so it must be kept (the tables left in L0 count as "deeper").
#[test]
fn l0_merge_keeps_a_tombstone_that_shadows_a_sibling_l0_table() {
    let sim = Simulator::new(0x1389);
    let engine: LsmEngine<SimEnv> = block_on(LsmEngine::open_with(
        sim.env(nid(0)),
        PREFIX,
        LsmOptions {
            flush_threshold_bytes: 1 << 30,
            compaction_trigger: 3,
            target_table_bytes: 1 << 20,
            level_fanout: 3,
            tombstone_grace_versions: 0,
            trust_monotonic_versions: true,
            ..LsmOptions::default()
        },
    ))
    .expect("open");
    block_on(async {
        // L1: ~60 KB of `zz` keys, none overlapping `a`.
        let mut ver = 0u64;
        for t in 0..3u64 {
            for i in 0..20u64 {
                ver += 1;
                let key = format!("zz{t}{i:02}");
                engine
                    .put(key.as_bytes(), &noise(ver, 1024), ver)
                    .await
                    .expect("put");
            }
            engine.flush_now().await.expect("flush");
        }
        engine.compact_now().await.expect("compact");
        assert_eq!(engine.level_table_counts(), vec![(1, 1)]);
        // L0: A (old value of `a`, 3 KB), then B (tombstone), then C.
        engine.put(b"a", &noise(7, 3000), 100).await.expect("put");
        // A key inside L1's range, so L0's combined range overlaps L1 and the
        // byte floor applies; the tombstoned key `a` itself lies outside L1.
        engine.put(b"zz1000", b"y", 101).await.expect("put");
        engine.flush_now().await.expect("flush");
        engine.delete(b"a", 200).await.expect("delete");
        engine.flush_now().await.expect("flush");
        engine.put(b"b", b"x", 201).await.expect("put");
        engine.flush_now().await.expect("flush");
        // The third flush reaches the trigger and runs the due compaction
        // itself: one L0->L0 merge of B and C; A stays, L1 untouched.
        engine.compact_now().await.expect("compact");
        assert_eq!(engine.level_table_counts(), vec![(0, 2), (1, 1)]);
        assert_eq!(engine.get(b"a").await.expect("get"), None);
        assert_eq!(
            engine.get(b"b").await.expect("get").map(|v| v.value),
            Some(b"x".to_vec())
        );
    });
}
