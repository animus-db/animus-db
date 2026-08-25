//! Focused unit tests for storage semantics the property tests don't pin down:
//! historical reads, tombstones, range delete, batch atomicity, and the
//! monotonic-version contract.

use animus_storage::{
    MemoryEngine, Snapshot, StorageEngine, StorageError, VersionedValue, WriteBatch,
};
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

/// `scan_at` (ADR 0018 §2/PR2b): the range counterpart of `get_at`. Covers the
/// case a naive "scan current, then get_at anything stale" approach would
/// miss — a key deleted *after* the target version must still show up at its
/// pre-deletion value, even though it is invisible to a `scan` of the
/// engine's current (post-deletion) state.
#[test]
fn scan_at_sees_the_range_as_of_an_older_version_including_since_deleted_keys() {
    block_on(async {
        let e = MemoryEngine::new();
        e.put(b"a", b"a1", 1).await.unwrap();
        e.put(b"b", b"b1", 2).await.unwrap();
        e.put(b"a", b"a2", 5).await.unwrap();
        e.delete(b"b", 6).await.unwrap(); // "b" is gone as of "now"

        // As of version 2: both keys are live.
        assert_eq!(
            e.scan_at(b"a", b"c", 2).await.unwrap(),
            vec![
                (
                    b"a".to_vec(),
                    VersionedValue {
                        version: 1,
                        value: b"a1".to_vec()
                    }
                ),
                (
                    b"b".to_vec(),
                    VersionedValue {
                        version: 2,
                        value: b"b1".to_vec()
                    }
                ),
            ],
        );
        // As of version 5: "a" moved to a2; "b" is still live (not yet deleted).
        assert_eq!(
            e.scan_at(b"a", b"c", 5).await.unwrap(),
            vec![
                (
                    b"a".to_vec(),
                    VersionedValue {
                        version: 5,
                        value: b"a2".to_vec()
                    }
                ),
                (
                    b"b".to_vec(),
                    VersionedValue {
                        version: 2,
                        value: b"b1".to_vec()
                    }
                ),
            ],
        );
        // As of "now" (post-delete): "b" is gone even though `scan` (its
        // current-latest sibling) would never surface it as "was here".
        assert_eq!(
            e.scan_at(b"a", b"c", u64::MAX).await.unwrap(),
            vec![(
                b"a".to_vec(),
                VersionedValue {
                    version: 5,
                    value: b"a2".to_vec()
                }
            )],
        );
        // Matches plain `scan` (latest) exactly.
        assert_eq!(
            e.scan_at(b"a", b"c", u64::MAX).await.unwrap(),
            e.scan(b"a", b"c").await.unwrap(),
        );

        // Before anything existed: empty.
        assert_eq!(e.scan_at(b"a", b"c", 0).await.unwrap(), Vec::new());

        // Inverted range is rejected, like `scan`.
        assert!(e.scan_at(b"c", b"a", 5).await.is_err());
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

        // `scan_with_tombstones` is the range-scoped sibling (ADR 0050): same
        // shape, `[start, end)`-bounded, tombstones retained.
        let ranged = e.scan_with_tombstones(b"k", b"l").await.unwrap();
        assert_eq!(ranged, vec![(b"k".to_vec(), None, 9)]);
        let all = e.scan_with_tombstones(b"", b"z").await.unwrap();
        assert_eq!(
            all, with_ts,
            "whole-range scan matches entries_with_tombstones"
        );
        let empty = e.scan_with_tombstones(b"l", b"o").await.unwrap();
        assert!(empty.is_empty(), "range excluding both keys is empty");
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

/// `MemoryEngine::clone_to` (ADR 0058 rung 2's `SimEnv`-corpus equivalent of
/// `LsmEngine::clone_to`): the clone matches the source's full record set
/// (values, an overwrite winner, and a tombstone) at clone time, and the two
/// engines are fully independent afterward — a write to either is never
/// visible through the other.
#[test]
fn clone_to_matches_source_and_isolates_subsequent_writes() {
    block_on(async {
        let e = MemoryEngine::new();
        e.put(b"a", b"1", 1).await.unwrap();
        e.put(b"b", b"2", 2).await.unwrap();
        e.put(b"a", b"1-overwritten", 3).await.unwrap();
        e.delete(b"b", 4).await.unwrap();

        let clone = e.clone_to();

        let mut src_view = e.entries_with_tombstones().await.unwrap();
        src_view.sort();
        let mut clone_view = clone.entries_with_tombstones().await.unwrap();
        clone_view.sort();
        assert_eq!(
            src_view, clone_view,
            "clone must match the source's values, overwrite winner, and tombstone"
        );

        e.put(b"only-src", b"x", 5).await.unwrap();
        clone.put(b"only-clone", b"x", 5).await.unwrap();
        assert_eq!(
            e.get(b"only-clone").await.unwrap(),
            None,
            "the source must not see a write made to the clone"
        );
        assert_eq!(
            clone.get(b"only-src").await.unwrap(),
            None,
            "the clone must not see a write made to the source"
        );
    });
}
