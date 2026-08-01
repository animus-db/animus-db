//! Focused unit tests for storage semantics the property tests don't pin down:
//! historical reads, tombstones, range delete, batch atomicity, and the
//! monotonic-version contract.

use custos_storage::{MemoryEngine, Snapshot, StorageEngine, StorageError, WriteBatch};
use futures::executor::block_on;

#[test]
fn historical_reads_see_old_versions() {
    block_on(async {
        let e = MemoryEngine::new();
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
    block_on(async {
        let e = MemoryEngine::new();
        e.put(b"k", b"v", 1).await.unwrap();
        e.delete(b"k", 2).await.unwrap();

        assert_eq!(
            e.get(b"k").await.unwrap(),
            None,
            "latest read is tombstoned"
        );
        assert_eq!(
            e.get_at(b"k", 1).await.unwrap().unwrap().value,
            b"v",
            "pre-delete read intact"
        );
    });
}

#[test]
fn range_delete_tombstones_the_range_only() {
    block_on(async {
        let e = MemoryEngine::new();
        for (i, k) in [b"a", b"b", b"c", b"d"].iter().enumerate() {
            e.put(*k, b"x", i as u64 + 1).await.unwrap();
        }
        e.delete_range(b"b", b"d", 100).await.unwrap(); // [b, d): removes b and c

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
    block_on(async {
        let e = MemoryEngine::new();
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
    block_on(async {
        let e = MemoryEngine::new();
        e.put(b"k", b"v", 10).await.unwrap();
        let err = e.put(b"k", b"v2", 10).await.unwrap_err();
        assert!(matches!(
            err,
            StorageError::NonMonotonicVersion {
                got: 10,
                latest: 10
            }
        ));
        // The rejected write left no trace.
        assert_eq!(e.get(b"k").await.unwrap().unwrap().value, b"v");
    });
}

#[test]
fn merge_is_per_key_lww_ignoring_the_global_floor() {
    block_on(async {
        let e = MemoryEngine::new();
        // Bump the engine-wide floor high on an unrelated key.
        e.put(b"other", b"x", 100).await.unwrap();

        // A merge below the global floor still applies, because the key is fresh.
        assert!(e.merge(b"k", b"v1", 5).await.unwrap(), "fresh key applies");
        assert_eq!(e.get(b"k").await.unwrap().unwrap().value, b"v1");

        // A strictly-newer version for the same key wins.
        assert!(e.merge(b"k", b"v2", 7).await.unwrap());
        assert_eq!(e.get(b"k").await.unwrap().unwrap().value, b"v2");

        // Equal or older versions are no-ops (idempotent / commutative).
        assert!(
            !e.merge(b"k", b"v2-dup", 7).await.unwrap(),
            "equal is a no-op"
        );
        assert!(!e.merge(b"k", b"v0", 3).await.unwrap(), "older is a no-op");
        assert_eq!(e.get(b"k").await.unwrap().unwrap().value, b"v2");
    });
}

#[test]
fn entries_returns_every_live_latest_in_key_order() {
    block_on(async {
        let e = MemoryEngine::new();
        e.put(b"a", b"1", 1).await.unwrap();
        e.put(b"b", b"2", 2).await.unwrap();
        e.put(b"a", b"1b", 3).await.unwrap(); // newer wins
        e.put(b"c", b"3", 4).await.unwrap();
        e.delete(b"c", 5).await.unwrap(); // tombstoned -> excluded

        let entries: Vec<_> = e
            .entries()
            .await
            .unwrap()
            .into_iter()
            .map(|(k, vv)| (k, vv.value))
            .collect();
        assert_eq!(
            entries,
            vec![
                (b"a".to_vec(), b"1b".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
            ]
        );
    });
}

#[test]
fn merge_tombstone_is_per_key_lww_and_entries_with_tombstones_retains_deletes() {
    block_on(async {
        let e = MemoryEngine::new();
        e.put(b"other", b"x", 100).await.unwrap(); // bump the global floor on another key

        // A fresh tombstone below the global floor applies (per-key LWW).
        assert!(
            e.merge_tombstone(b"k", 5).await.unwrap(),
            "fresh key applies"
        );
        assert_eq!(
            e.get(b"k").await.unwrap(),
            None,
            "tombstoned key reads absent"
        );

        // A value strictly newer than the tombstone resurrects the key.
        assert!(e.merge(b"k", b"v2", 7).await.unwrap());
        assert_eq!(e.get(b"k").await.unwrap().unwrap().value, b"v2");

        // A tombstone strictly newer than that wins again; equal/older are no-ops.
        assert!(e.merge_tombstone(b"k", 9).await.unwrap());
        assert!(
            !e.merge_tombstone(b"k", 9).await.unwrap(),
            "equal is a no-op"
        );
        assert!(
            !e.merge(b"k", b"v0", 6).await.unwrap(),
            "older value is a no-op"
        );
        assert_eq!(e.get(b"k").await.unwrap(), None);

        // `entries` hides the deleted key; `entries_with_tombstones` retains it as a
        // `None` at the tombstone's version, so anti-entropy can propagate the delete.
        let live: Vec<_> = e
            .entries()
            .await
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(live, vec![b"other".to_vec()], "k is hidden from `entries`");
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
    block_on(async {
        let e = MemoryEngine::new();
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
