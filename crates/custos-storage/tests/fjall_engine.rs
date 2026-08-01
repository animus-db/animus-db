//! Semantics + persistence tests for the `fjall` backend. Run with
//! `cargo test -p custos-storage --features fjall`.
#![cfg(feature = "fjall")]

use custos_storage::{FjallEngine, Snapshot, StorageEngine, StorageError, WriteBatch};
use futures::executor::block_on;

fn engine() -> (tempfile::TempDir, FjallEngine) {
    let dir = tempfile::tempdir().unwrap();
    let engine = FjallEngine::open(dir.path()).unwrap();
    (dir, engine)
}

#[test]
fn mvcc_reads_match_the_memory_engine_semantics() {
    block_on(async {
        let (_dir, e) = engine();
        e.put(b"k", b"v1", 10).await.unwrap();
        e.put(b"k", b"v2", 20).await.unwrap();

        assert_eq!(e.get_at(b"k", 9).await.unwrap(), None);
        assert_eq!(e.get_at(b"k", 10).await.unwrap().unwrap().value, b"v1");
        assert_eq!(e.get_at(b"k", 15).await.unwrap().unwrap().value, b"v1");
        assert_eq!(e.get_at(b"k", 20).await.unwrap().unwrap().value, b"v2");
        assert_eq!(e.get(b"k").await.unwrap().unwrap().value, b"v2");

        e.delete(b"k", 30).await.unwrap();
        assert_eq!(e.get(b"k").await.unwrap(), None);
        assert_eq!(
            e.get_at(b"k", 20).await.unwrap().unwrap().value,
            b"v2",
            "history intact behind tombstone"
        );
    });
}

#[test]
fn scan_is_ordered_and_range_delete_works() {
    block_on(async {
        let (_dir, e) = engine();
        for (i, k) in [b"a", b"b", b"c", b"d"].iter().enumerate() {
            e.put(*k, b"x", i as u64 + 1).await.unwrap();
        }
        let keys: Vec<_> = e
            .scan(b"a", b"z")
            .await
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(
            keys,
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
        );

        e.delete_range(b"b", b"d", 100).await.unwrap(); // removes b, c
        let keys: Vec<_> = e
            .scan(b"a", b"z")
            .await
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"d".to_vec()]);
    });
}

#[test]
fn keys_with_zero_bytes_do_not_collide() {
    block_on(async {
        // Exercises the prefix-free escape: "a" must not be confused with "a\0b".
        let (_dir, e) = engine();
        e.put(b"a", b"1", 1).await.unwrap();
        e.put(b"a\x00b", b"2", 2).await.unwrap();
        e.put(b"ab", b"3", 3).await.unwrap();
        assert_eq!(e.get(b"a").await.unwrap().unwrap().value, b"1");
        assert_eq!(e.get(b"a\x00b").await.unwrap().unwrap().value, b"2");
        assert_eq!(e.get(b"ab").await.unwrap().unwrap().value, b"3");

        let keys: Vec<_> = e
            .scan(b"", b"\xff")
            .await
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(
            keys,
            vec![b"a".to_vec(), b"a\x00b".to_vec(), b"ab".to_vec()]
        );
    });
}

#[test]
fn snapshot_is_isolated_and_versions_are_monotonic() {
    block_on(async {
        let (_dir, e) = engine();
        e.put(b"a", b"1", 1).await.unwrap();
        e.put(b"b", b"2", 2).await.unwrap();
        let snap = e.snapshot();
        assert_eq!(snap.version(), 2);

        e.put(b"a", b"99", 3).await.unwrap();
        e.delete(b"b", 4).await.unwrap();
        assert_eq!(
            snap.get(b"a").await.unwrap().value,
            b"1",
            "snapshot isolated from later writes"
        );
        assert_eq!(snap.get(b"b").await.unwrap().value, b"2");

        let err = e.put(b"a", b"x", 2).await.unwrap_err();
        assert!(matches!(err, StorageError::NonMonotonicVersion { .. }));
    });
}

#[test]
fn write_batch_is_atomic_at_one_version() {
    block_on(async {
        let (_dir, e) = engine();
        e.put(b"old", b"1", 1).await.unwrap();
        e.write_batch(
            WriteBatch::new(5)
                .put(b"x", b"10")
                .put(b"y", b"20")
                .delete(b"old"),
        )
        .await
        .unwrap();
        assert_eq!(e.get(b"x").await.unwrap().unwrap().version, 5);
        assert_eq!(e.get(b"y").await.unwrap().unwrap().value, b"20");
        assert_eq!(e.get(b"old").await.unwrap(), None);
        assert_eq!(e.latest_version(), 5);
    });
}

#[test]
fn merge_is_per_key_lww_and_entries_lists_live_latest() {
    block_on(async {
        let (_dir, e) = engine();
        e.put(b"other", b"x", 100).await.unwrap(); // raise the global floor

        assert!(
            e.merge(b"k", b"v1", 5).await.unwrap(),
            "fresh key below floor applies"
        );
        assert!(e.merge(b"k", b"v2", 7).await.unwrap(), "newer wins");
        assert!(
            !e.merge(b"k", b"v2-dup", 7).await.unwrap(),
            "equal is a no-op"
        );
        assert!(!e.merge(b"k", b"v0", 3).await.unwrap(), "older is a no-op");
        assert_eq!(e.get(b"k").await.unwrap().unwrap().value, b"v2");

        e.delete(b"other", 101).await.unwrap(); // tombstoned -> excluded from entries
        let entries: Vec<_> = e
            .entries()
            .await
            .unwrap()
            .into_iter()
            .map(|(k, vv)| (k, vv.value))
            .collect();
        assert_eq!(entries, vec![(b"k".to_vec(), b"v2".to_vec())]);
    });
}

#[test]
fn merge_tombstone_is_per_key_lww_and_entries_with_tombstones_retains_deletes() {
    block_on(async {
        let (_dir, e) = engine();
        e.put(b"other", b"x", 100).await.unwrap(); // raise the global floor

        // A fresh tombstone below the floor applies (per-key LWW).
        assert!(
            e.merge_tombstone(b"k", 5).await.unwrap(),
            "fresh key applies"
        );
        assert_eq!(e.get(b"k").await.unwrap(), None);

        // A newer value resurrects, a newer tombstone deletes again; equal/older no-op.
        assert!(e.merge(b"k", b"v2", 7).await.unwrap());
        assert_eq!(e.get(b"k").await.unwrap().unwrap().value, b"v2");
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

        // `entries` hides the delete; `entries_with_tombstones` retains it as `None`.
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
fn data_and_version_floor_survive_reopen() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        {
            let e = FjallEngine::open(dir.path()).unwrap();
            e.put(b"durable", b"value", 7).await.unwrap();
            e.put(b"durable", b"value2", 12).await.unwrap();
        } // engine dropped — the keyspace is closed

        let e = FjallEngine::open(dir.path()).unwrap();
        assert_eq!(
            e.get(b"durable").await.unwrap().unwrap().value,
            b"value2",
            "data lost across reopen"
        );
        assert_eq!(e.latest_version(), 12, "monotonic floor not recovered");
        // A write below the recovered floor is still rejected after reopen.
        assert!(e.put(b"durable", b"v", 10).await.is_err());
        e.put(b"durable", b"v3", 13).await.unwrap();
        assert_eq!(e.get(b"durable").await.unwrap().unwrap().value, b"v3");
    });
}
