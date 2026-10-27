//! The LSM `MANIFEST` is **not** an append-only log: every flush and compaction
//! publishes a complete image of the live version with `Disk::replace`
//! (write-temp, fsync, rename, fsync-dir), so its size is a function of the
//! *live* table set (key bounds + Bloom bits per table), never of history
//! (issue #1389: the 149 KB observed after a soak is live Bloom filters, not
//! accumulated records). This pins that property: over many flush/compaction
//! cycles over a fixed key set the manifest size must plateau, and a reopen
//! must read it back.

use animus_env::{Disk, nid};
use animus_sim::Simulator;
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use futures::executor::block_on;

const PREFIX: &str = "db/";

#[test]
fn manifest_size_is_bounded_by_the_live_version_not_history() {
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
