//! The LSM `MANIFEST` is **not** an append-only log: every flush and compaction
//! publishes a complete image of the live version with `Disk::replace`
//! (write-temp, fsync, rename, fsync-dir), so its size is a function of the
//! *live* table set, never of history (issue #1389: the 149 KB observed after a
//! soak was live Bloom filters, not accumulated records). Two properties are
//! pinned here:
//!
//! 1. over many flush/compaction cycles over a fixed small key set the manifest size plateaus, and a
//!    reopen reads it back;
//! 2. the manifest costs a small constant per live table **regardless of how
//!    many keys a table holds**: the Bloom filters live in each SSTable (format
//!    2, manifest v3), not in the manifest. With the filters inline (manifest
//!    v1/v2) a 256-key table costs about 300 B of filter alone, and this fails.

use std::collections::BTreeMap;

use animus_env::{Disk, nid};
use animus_sim::Simulator;
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use futures::executor::block_on;

const PREFIX: &str = "db/";

#[test]
fn manifest_size_plateaus_over_a_fixed_key_set() {
    let seed = 1389u64;
    let sim = Simulator::new(seed);
    let env = sim.env(nid(0));
    let opts = LsmOptions {
        flush_threshold_bytes: 256,
        compaction_trigger: 3,
        target_table_bytes: 1024,
        level_fanout: 2,
        wal_segment_bytes: 512,
        background_maintenance: false,
        ..LsmOptions::default()
    };
    let e = block_on(LsmEngine::open_with(env.clone(), PREFIX, opts)).unwrap();
    let mut version = 0u64;
    let mut sizes = Vec::new();
    for cycle in 0..400u64 {
        for k in 0..8u64 {
            version += 1;
            let v = format!("v{cycle}-{k}");
            block_on(e.put(format!("k{k:03}").as_bytes(), v.as_bytes(), version)).unwrap();
        }
        block_on(e.flush_now()).unwrap();
        if cycle % 3 == 0 {
            block_on(e.compact_now()).unwrap();
        }
        sizes.push(block_on(env.size(&format!("{PREFIX}MANIFEST"))).unwrap());
    }
    let early = *sizes[20..100].iter().max().unwrap();
    let late = *sizes[300..].iter().max().unwrap();
    eprintln!(
        "manifest bytes: first {} early-max {early} late-max {late} last {}",
        sizes[0], sizes[399]
    );
    assert!(
        late <= early * 2 + 1024,
        "seed={seed}: manifest grew with history: early max {early} B, late max {late} B"
    );
    assert!(e.compaction_count() > 50 && e.flush_count() >= 400);
    drop(e);
    sim.crash(nid(0));
    sim.restart(nid(0));
    let e = block_on(LsmEngine::open_with(sim.env(nid(0)), PREFIX, opts)).unwrap();
    for k in 0..8u64 {
        let got = block_on(e.get(format!("k{k:03}").as_bytes()))
            .unwrap()
            .unwrap();
        assert_eq!(got.value, format!("v399-{k}").into_bytes(), "seed={seed}");
    }
}

/// Distinct keys written per flush: each live table carries a real filter.
const KEYS_PER_CYCLE: u64 = 256;
/// The key universe; cycle `c` rewrites the slice starting at `c * 256 mod 1024`.
const UNIVERSE: u64 = 1024;
/// The budget for manifest bytes per live table (bounds and counters only).
const MAX_BYTES_PER_TABLE: u64 = 256;

#[test]
fn manifest_bytes_per_live_table_are_constant_whatever_the_key_count() {
    let seed = 1389u64;
    let sim = Simulator::new(seed);
    let env = sim.env(nid(0));
    let opts = LsmOptions {
        flush_threshold_bytes: 1 << 20,
        compaction_trigger: 3,
        target_table_bytes: 16 * 1024,
        level_fanout: 2,
        wal_segment_bytes: 64 * 1024,
        background_maintenance: false,
        ..LsmOptions::default()
    };
    let e = block_on(LsmEngine::open_with(env.clone(), PREFIX, opts)).unwrap();
    let mut version = 0u64;
    let mut sizes = Vec::new();
    let mut worst_per_table = 0u64;
    let mut expected: BTreeMap<u64, String> = BTreeMap::new();
    for cycle in 0..400u64 {
        for j in 0..KEYS_PER_CYCLE {
            version += 1;
            let k = (cycle * KEYS_PER_CYCLE + j) % UNIVERSE;
            let v = format!("v{cycle}-{k:04}");
            block_on(e.put(format!("k{k:04}").as_bytes(), v.as_bytes(), version)).unwrap();
            expected.insert(k, v);
        }
        block_on(e.flush_now()).unwrap();
        if cycle % 3 == 0 {
            block_on(e.compact_now()).unwrap();
        }
        let size = block_on(env.size(&format!("{PREFIX}MANIFEST"))).unwrap();
        sizes.push(size);
        let tables = e.sstable_views().len().max(1) as u64;
        worst_per_table = worst_per_table.max(size / tables);
    }
    eprintln!(
        "manifest bytes: first {} last {}; live tables at end {}; worst bytes per live table \
         {worst_per_table}",
        sizes[0],
        sizes[399],
        e.sstable_views().len()
    );
    assert!(
        worst_per_table <= MAX_BYTES_PER_TABLE,
        "seed={seed}: manifest costs {worst_per_table} B per live table (limit \
         {MAX_BYTES_PER_TABLE}): the Bloom filters are inline instead of in the SSTables"
    );
    assert!(e.compaction_count() > 50 && e.flush_count() >= 400);
    drop(e);
    sim.crash(nid(0));
    sim.restart(nid(0));
    let e = block_on(LsmEngine::open_with(sim.env(nid(0)), PREFIX, opts)).unwrap();
    for (k, v) in &expected {
        let got = block_on(e.get(format!("k{k:04}").as_bytes()))
            .unwrap()
            .unwrap();
        assert_eq!(got.value, v.clone().into_bytes(), "seed={seed} key {k}");
    }
}
