//! `LsmOptions::trust_monotonic_versions` (opt-in, default `false`): a fast path
//! for `merge`/`merge_batch` that skips the cross-SSTable `latest_version_of`
//! point read normally used to decide the per-key LWW winner. Under the CP
//! plane's monotonic Raft-log-index versions, that read is structurally always
//! a winner — every version handed to `merge*` is already known to be newer
//! than anything the engine holds for that key — so it is pure overhead.
//!
//! Two things must hold: (1) behavior is unchanged (same values readable
//! afterward, including within-batch dedup for a repeated key); (2) the fast
//! path actually skips the read — proven by asserting **zero SSTable block
//! reads** happen for a `merge`/`merge_batch` call whose key already lives in a
//! flushed, on-disk table (so without the fast path, deciding the LWW winner
//! would have to fetch a block).

use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, MergeOp, StorageEngine};
use futures::executor::block_on;

const PREFIX: &str = "db/";

fn opts(trust_monotonic_versions: bool) -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 1 << 20,
        compaction_trigger: 100,
        target_table_bytes: 1 << 20,
        level_fanout: 8,
        wal_segment_bytes: 1 << 20,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions,
        background_maintenance: false,
    }
}

fn open(sim: &Simulator, trust_monotonic_versions: bool) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with(
        sim.env(nid(0)),
        PREFIX,
        opts(trust_monotonic_versions),
    ))
    .expect("open")
}

/// With the fast path **off** (default), `merge` on a key that lives in a
/// flushed SSTable must read at least one block to decide the LWW winner — the
/// baseline this test's "on" half is compared against.
#[test]
fn default_merge_reads_the_sstable_for_the_lww_check() {
    let sim = Simulator::new(1);
    let e = open(&sim, false);
    block_on(async {
        e.put(b"k", b"v1", 1).await.unwrap();
        e.flush_now().await.unwrap();
        assert_eq!(e.sstable_count(), 1, "expected one flushed table");

        e.reset_block_reads();
        assert!(
            e.merge(b"k", b"v2", 2).await.unwrap(),
            "a strictly newer version must win"
        );
        assert!(
            e.block_read_count() > 0,
            "expected the default (non-fast) path to read the on-disk table \
             for the LWW check"
        );
        assert_eq!(e.get(b"k").await.unwrap().unwrap().value, b"v2");
    });
}

/// With the fast path **on**, `merge`/`merge_tombstone` on a key that lives in a
/// flushed SSTable read **zero** blocks — the cross-SSTable LWW check is
/// skipped entirely — while still applying correctly.
#[test]
fn trusted_merge_skips_the_sstable_read_and_still_applies() {
    let sim = Simulator::new(2);
    let e = open(&sim, true);
    block_on(async {
        e.put(b"k", b"v1", 1).await.unwrap();
        e.flush_now().await.unwrap();
        assert_eq!(e.sstable_count(), 1, "expected one flushed table");

        e.reset_block_reads();
        assert!(
            e.merge(b"k", b"v2", 2).await.unwrap(),
            "trust_monotonic_versions must still report a win"
        );
        assert_eq!(
            e.block_read_count(),
            0,
            "trust_monotonic_versions must skip the cross-SSTable LWW read"
        );
        assert_eq!(e.get(b"k").await.unwrap().unwrap().value, b"v2");

        e.reset_block_reads();
        assert!(
            e.merge_tombstone(b"k", 3).await.unwrap(),
            "trust_monotonic_versions must still report a win for a tombstone"
        );
        assert_eq!(
            e.block_read_count(),
            0,
            "trust_monotonic_versions must skip the read for merge_tombstone too"
        );
        assert_eq!(e.get(b"k").await.unwrap(), None);
    });
}

/// `merge_batch` under the fast path also skips the cross-SSTable read per op,
/// but still dedupes multiple ops for the *same* key within one batch
/// (in-memory, no read needed) — the highest version wins, matching the
/// non-fast-path semantics exactly.
#[test]
fn trusted_merge_batch_skips_reads_but_still_dedupes_within_batch() {
    let sim = Simulator::new(3);
    let e = open(&sim, true);
    block_on(async {
        e.put(b"a", b"a0", 1).await.unwrap();
        e.put(b"b", b"b0", 2).await.unwrap();
        e.flush_now().await.unwrap();
        assert_eq!(e.sstable_count(), 1, "expected one flushed table");

        e.reset_block_reads();
        e.merge_batch(vec![
            // Two ops for the same key "a": the higher version must win, decided
            // purely from the in-batch comparison (no engine-state read).
            MergeOp {
                key: b"a".to_vec(),
                value: Some(b"a1".to_vec()),
                version: 2,
            },
            MergeOp {
                key: b"a".to_vec(),
                value: Some(b"a2".to_vec()),
                version: 3,
            },
            MergeOp {
                key: b"b".to_vec(),
                value: None, // tombstone
                version: 3,
            },
            MergeOp {
                key: b"c".to_vec(), // brand-new key, not in any SSTable
                value: Some(b"c0".to_vec()),
                version: 1,
            },
        ])
        .await
        .unwrap();
        assert_eq!(
            e.block_read_count(),
            0,
            "merge_batch under trust_monotonic_versions must issue no SSTable reads"
        );

        assert_eq!(
            e.get(b"a").await.unwrap().unwrap().value,
            b"a2",
            "the higher in-batch version must win"
        );
        assert_eq!(e.get(b"b").await.unwrap(), None, "b was tombstoned");
        assert_eq!(e.get(b"c").await.unwrap().unwrap().value, b"c0");
    });
}
