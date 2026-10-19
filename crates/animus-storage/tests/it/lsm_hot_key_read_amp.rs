//! Read amplification of an apply-style loop on hot keys (issue #1349).
//!
//! A follower's Raft apply task reads committed state before most entries (a
//! `get` of the row, a tombstone-aware newest-version probe of each staged key
//! and its resolved marker). On a hot key every flush leaves the key's
//! retained versions in several consecutive 4 KiB blocks, and since #1315 a
//! point read must be correct across those blocks. Reading *all* of them per
//! probe made a debug-build apply task slower than the commit rate.
//!
//! The probes now stop at the newest block that holds the key (versions run
//! oldest to newest through a table's blocks) and the replay-ahead probe is
//! Bloom-gated. This test pins the cost: block reads per apply stay below a
//! small bound, with a negative control showing the old range-scan probe
//! exceeds it on the very same engine state.

use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, MergeOp, StorageEngine};
use futures::executor::block_on;

const KEYS: usize = 20;
const ENTRIES: usize = 1200;
/// ~5 versions per 4 KiB block.
const VALUE_LEN: usize = 700;

fn opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 1 << 30,
        compaction_trigger: 1 << 20,
        target_table_bytes: 1 << 20,
        level_fanout: 4,
        wal_segment_bytes: 1 << 20,
        tombstone_grace_versions: 1 << 40,
        trust_monotonic_versions: true,
        background_maintenance: false,
        ..LsmOptions::default()
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("k{:06}", i % KEYS).into_bytes()
}

/// Run the apply loop; `probe` is the replay-ahead probe under test. Returns
/// block reads per entry.
fn run(
    seed: u64,
    probe: impl for<'a> Fn(
        &'a LsmEngine<SimEnv>,
        &'a [u8],
    ) -> futures::future::BoxFuture<'a, Option<u64>>,
) -> f64 {
    let sim = Simulator::new(seed);
    let e = block_on(LsmEngine::open_with(sim.env(nid(0)), "db/", opts())).expect("open");
    let value = vec![b'v'; VALUE_LEN];
    // Build history: a few tables, every key with many versions in each.
    for i in 0..ENTRIES {
        block_on(e.merge_batch(vec![MergeOp::put(key(i), value.clone(), (i + 1) as u64)]))
            .expect("seed");
        if i % 300 == 299 {
            block_on(e.flush_now()).expect("flush");
        }
    }
    e.reset_block_reads();
    let measured = 400;
    block_on(async {
        for i in ENTRIES..ENTRIES + measured {
            let k = key(i);
            let _ = e.get(&k).await.expect("get");
            let v = probe(&e, &k).await;
            assert!(v.is_some(), "seed={seed}: hot key has a version");
            e.merge_batch(vec![MergeOp::put(k, value.clone(), (i + 1) as u64)])
                .await
                .expect("apply");
            // Flush often, so the next read of a key finds its newest version
            // in a table, not the memtable.
            if i % 50 == 49 {
                e.flush_now().await.expect("flush");
            }
        }
    });
    e.block_read_count() as f64 / measured as f64
}

#[test]
fn replay_ahead_probe_on_hot_keys_reads_few_blocks_per_apply() {
    let seed = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7);
    let point = run(seed, |e, k| {
        Box::pin(async move {
            StorageEngine::latest_version_incl_tombstone(e, k)
                .await
                .expect("probe")
        })
    });
    let scan = run(seed, |e, k| {
        Box::pin(async move {
            let mut end = k.to_vec();
            end.push(0);
            e.scan_with_tombstones(k, &end)
                .await
                .expect("scan")
                .into_iter()
                .next()
                .map(|(_, _, v)| v)
        })
    });
    eprintln!("block reads / apply: point probe {point:.2}, range-scan probe {scan:.2}");
    // One `get` plus one probe: a handful of blocks at most (the memtable
    // answers the keys written since the last flush, no block at all).
    assert!(
        point <= 3.0,
        "seed={seed}: {point:.2} block reads per apply with the point probe (bound 3.0)"
    );
    assert!(
        scan > point * 2.0,
        "seed={seed}: negative control: the range-scan probe ({scan:.2}) should cost well over the point probe ({point:.2})"
    );
}
