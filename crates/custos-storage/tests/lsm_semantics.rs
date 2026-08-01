//! `LsmEngine` must be **observationally identical** to `MemoryEngine`: the same
//! MVCC semantics for put/get/get_at/merge/tombstone/scan/entries/snapshot. We
//! mirror the focused `storage_basic.rs` units against the on-disk engine, and
//! add a differential property test that drives the *same* op stream through both
//! engines and asserts equal observations after a flush has pushed data to disk.
//!
//! The engine does all I/O through the `Env` disk seam; under `SimEnv` those ops
//! resolve synchronously (no timers/recv), so we drive each future with
//! `block_on` directly against `sim.env(node)` — deterministic and seed-pure.

use custos_sim::Simulator;
use custos_storage::{LsmEngine, LsmOptions, Snapshot, StorageEngine, StorageError, WriteBatch};
use futures::executor::block_on;
use proptest::prelude::*;

/// Open a fresh LSM engine on a fresh simulated disk, with a small flush
/// threshold so tests exercise the on-disk path, not just the memtable.
fn open(seed: u64) -> LsmEngine<custos_sim::SimEnv> {
    let sim = Simulator::new(seed);
    let opts = LsmOptions {
        flush_threshold_bytes: 64,
        compaction_trigger: 3,
    };
    block_on(LsmEngine::open_with(sim.env(0), "db/", opts)).expect("open")
}

#[test]
fn historical_reads_see_old_versions() {
    let e = open(1);
    block_on(async {
        e.put(b"k", b"v1", 10).await.unwrap();
        e.put(b"k", b"v2", 20).await.unwrap();

        assert_eq!(e.get_at(b"k", 9).await.unwrap(), None);
        assert_eq!(e.get_at(b"k", 10).await.unwrap().unwrap().value, b"v1");
        assert_eq!(e.get_at(b"k", 15).await.unwrap().unwrap().value, b"v1");
        assert_eq!(e.get_at(b"k", 20).await.unwrap().unwrap().value, b"v2");
        assert_eq!(e.get(b"k").await.unwrap().unwrap().value, b"v2");
    });
}

#[test]
fn delete_is_a_tombstone_not_history_loss() {
    let e = open(2);
    block_on(async {
        e.put(b"k", b"v", 1).await.unwrap();
        e.delete(b"k", 2).await.unwrap();
        assert_eq!(e.get(b"k").await.unwrap(), None);
        assert_eq!(e.get_at(b"k", 1).await.unwrap().unwrap().value, b"v");
    });
}

#[test]
fn range_delete_tombstones_the_range_only() {
    let e = open(3);
    block_on(async {
        for (i, k) in [b"a", b"b", b"c", b"d"].iter().enumerate() {
            e.put(*k, b"x", i as u64 + 1).await.unwrap();
        }
        e.delete_range(b"b", b"d", 100).await.unwrap();
        let live: Vec<_> = e
            .scan(b"a", b"z")
            .await
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(live, vec![b"a".to_vec(), b"d".to_vec()]);
    });
}

#[test]
fn write_batch_applies_at_one_version() {
    let e = open(4);
    block_on(async {
        e.put(b"old", b"1", 1).await.unwrap();
        let batch = WriteBatch::new(5)
            .put(b"x", b"10")
            .put(b"y", b"20")
            .delete(b"old");
        e.write_batch(batch).await.unwrap();
        assert_eq!(e.get(b"x").await.unwrap().unwrap().version, 5);
        assert_eq!(e.get(b"y").await.unwrap().unwrap().version, 5);
        assert_eq!(e.get(b"old").await.unwrap(), None);
        assert_eq!(e.latest_version(), 5);
    });
}

#[test]
fn non_monotonic_version_is_rejected() {
    let e = open(5);
    block_on(async {
        e.put(b"k", b"v", 10).await.unwrap();
        let err = e.put(b"k", b"v2", 10).await.unwrap_err();
        assert!(matches!(
            err,
            StorageError::NonMonotonicVersion {
                got: 10,
                latest: 10
            }
        ));
        assert_eq!(e.get(b"k").await.unwrap().unwrap().value, b"v");
    });
}

#[test]
fn merge_is_per_key_lww_ignoring_the_global_floor() {
    let e = open(6);
    block_on(async {
        e.put(b"other", b"x", 100).await.unwrap();
        assert!(e.merge(b"k", b"v1", 5).await.unwrap(), "fresh key applies");
        assert_eq!(e.get(b"k").await.unwrap().unwrap().value, b"v1");
        assert!(e.merge(b"k", b"v2", 7).await.unwrap());
        assert_eq!(e.get(b"k").await.unwrap().unwrap().value, b"v2");
        assert!(!e.merge(b"k", b"v2-dup", 7).await.unwrap(), "equal no-op");
        assert!(!e.merge(b"k", b"v0", 3).await.unwrap(), "older no-op");
        assert_eq!(e.get(b"k").await.unwrap().unwrap().value, b"v2");
    });
}

#[test]
fn merge_tombstone_lww_and_entries_with_tombstones_retains_deletes() {
    let e = open(7);
    block_on(async {
        e.put(b"other", b"x", 100).await.unwrap();
        assert!(e.merge_tombstone(b"k", 5).await.unwrap(), "fresh applies");
        assert_eq!(e.get(b"k").await.unwrap(), None);
        assert!(e.merge(b"k", b"v2", 7).await.unwrap());
        assert_eq!(e.get(b"k").await.unwrap().unwrap().value, b"v2");
        assert!(e.merge_tombstone(b"k", 9).await.unwrap());
        assert!(!e.merge_tombstone(b"k", 9).await.unwrap(), "equal no-op");
        assert!(!e.merge(b"k", b"v0", 6).await.unwrap(), "older no-op");
        assert_eq!(e.get(b"k").await.unwrap(), None);

        let live: Vec<_> = e
            .entries()
            .await
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(live, vec![b"other".to_vec()]);
        let with_ts = e.entries_with_tombstones().await.unwrap();
        assert_eq!(
            with_ts,
            vec![
                (b"k".to_vec(), None, 9),
                (b"other".to_vec(), Some(b"x".to_vec()), 100),
            ]
        );
    });
}

#[test]
fn snapshot_scan_is_isolated() {
    let e = open(8);
    block_on(async {
        e.put(b"a", b"1", 1).await.unwrap();
        e.put(b"b", b"2", 2).await.unwrap();
        let snap = e.snapshot();
        e.put(b"c", b"3", 3).await.unwrap();
        e.delete(b"a", 4).await.unwrap();

        let snap_keys: Vec<_> = snap
            .scan(b"", b"z")
            .await
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(snap_keys, vec![b"a".to_vec(), b"b".to_vec()]);
        let live_keys: Vec<_> = e
            .scan(b"", b"z")
            .await
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(live_keys, vec![b"b".to_vec(), b"c".to_vec()]);
    });
}

/// Many puts spanning several flushes are all readable (across the memtable +
/// multiple SSTables + a compaction).
#[test]
fn many_keys_survive_flushes_and_compaction() {
    let e = open(9);
    block_on(async {
        for i in 0u64..200 {
            let k = format!("key-{i:04}");
            e.put(k.as_bytes(), format!("val-{i}").as_bytes(), i + 1)
                .await
                .unwrap();
        }
        for i in 0u64..200 {
            let k = format!("key-{i:04}");
            assert_eq!(
                e.get(k.as_bytes()).await.unwrap().unwrap().value,
                format!("val-{i}").as_bytes(),
            );
        }
        // A full scan returns all 200 in key order.
        let all = e.scan(b"", b"key-9999").await.unwrap();
        assert_eq!(all.len(), 200);
        for pair in all.windows(2) {
            assert!(pair[0].0 < pair[1].0, "scan not strictly ascending");
        }
    });
}

proptest! {
    /// Differential test: drive the same op stream through `LsmEngine` (flushing
    /// to disk) and an in-memory `MemoryEngine`, then assert identical reads. Any
    /// divergence in MVCC merge/tombstone/scan semantics fails here.
    #[test]
    fn lsm_matches_memory_engine(
        ops in proptest::collection::vec(
            (0u8..6, proptest::collection::vec(0u8..3, 0..3), proptest::collection::vec(0u8..3, 0..2)),
            1..40,
        ),
        seed in any::<u64>(),
    ) {
        use custos_storage::MemoryEngine;
        let lsm = open(seed);
        let mem = MemoryEngine::new();
        block_on(async {
            let mut version = 0u64;
            for (kind, key, val) in &ops {
                version += 1;
                match kind {
                    0..=2 => {
                        // put
                        lsm.put(key, val, version).await.unwrap();
                        mem.put(key, val, version).await.unwrap();
                    }
                    3 => {
                        // delete
                        lsm.delete(key, version).await.unwrap();
                        mem.delete(key, version).await.unwrap();
                    }
                    4 => {
                        // merge at an explicit (possibly below-floor) version
                        let mv = if version > 2 { version - 2 } else { version };
                        let a = lsm.merge(key, val, mv).await.unwrap();
                        let b = mem.merge(key, val, mv).await.unwrap();
                        prop_assert_eq!(a, b, "merge applied-ness diverged");
                    }
                    _ => {
                        // merge_tombstone at an explicit version
                        let mv = if version > 2 { version - 2 } else { version };
                        let a = lsm.merge_tombstone(key, mv).await.unwrap();
                        let b = mem.merge_tombstone(key, mv).await.unwrap();
                        prop_assert_eq!(a, b, "merge_tombstone applied-ness diverged");
                    }
                }
            }
            // Compare full live digest and tombstone digest.
            prop_assert_eq!(lsm.entries().await.unwrap(), mem.entries().await.unwrap());
            prop_assert_eq!(
                lsm.entries_with_tombstones().await.unwrap(),
                mem.entries_with_tombstones().await.unwrap()
            );
            // Compare point reads for every key in [0..3]^len up to length 2.
            for a in 0u8..3 {
                let k1 = vec![a];
                prop_assert_eq!(
                    lsm.get(&k1).await.unwrap(),
                    mem.get(&k1).await.unwrap(),
                    "get diverged"
                );
                for b in 0u8..3 {
                    let k2 = vec![a, b];
                    prop_assert_eq!(
                        lsm.get(&k2).await.unwrap(),
                        mem.get(&k2).await.unwrap(),
                        "get diverged"
                    );
                }
            }
            // Compare a full scan.
            prop_assert_eq!(
                lsm.scan(b"", &[0xff]).await.unwrap(),
                mem.scan(b"", &[0xff]).await.unwrap(),
                "scan diverged"
            );
            Ok(())
        })?;
    }
}
