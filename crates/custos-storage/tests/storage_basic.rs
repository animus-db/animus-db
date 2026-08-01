//! Focused unit tests for storage semantics the property tests don't pin down:
//! historical reads, tombstones, range delete, batch atomicity, and the
//! monotonic-version contract.

use custos_storage::{MemoryEngine, Snapshot, StorageEngine, StorageError, WriteBatch};

#[test]
fn historical_reads_see_old_versions() {
    let e = MemoryEngine::new();
    e.put(b"k", b"v1", 10).unwrap();
    e.put(b"k", b"v2", 20).unwrap();

    assert_eq!(e.get_at(b"k", 9).unwrap(), None);
    assert_eq!(e.get_at(b"k", 10).unwrap().unwrap().value, b"v1");
    assert_eq!(e.get_at(b"k", 15).unwrap().unwrap().value, b"v1");
    assert_eq!(e.get_at(b"k", 20).unwrap().unwrap().value, b"v2");
    assert_eq!(e.get(b"k").unwrap().unwrap().value, b"v2");
}

#[test]
fn delete_is_a_tombstone_not_history_loss() {
    let e = MemoryEngine::new();
    e.put(b"k", b"v", 1).unwrap();
    e.delete(b"k", 2).unwrap();

    assert_eq!(e.get(b"k").unwrap(), None, "latest read is tombstoned");
    assert_eq!(
        e.get_at(b"k", 1).unwrap().unwrap().value,
        b"v",
        "pre-delete read intact"
    );
}

#[test]
fn range_delete_tombstones_the_range_only() {
    let e = MemoryEngine::new();
    for (i, k) in [b"a", b"b", b"c", b"d"].iter().enumerate() {
        e.put(*k, b"x", i as u64 + 1).unwrap();
    }
    e.delete_range(b"b", b"d", 100).unwrap(); // [b, d): removes b and c

    let live: Vec<_> = e
        .scan(b"a", b"z")
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(live, vec![b"a".to_vec(), b"d".to_vec()]);
}

#[test]
fn write_batch_applies_at_one_version() {
    let e = MemoryEngine::new();
    e.put(b"old", b"1", 1).unwrap();
    let batch = WriteBatch::new(5)
        .put(b"x", b"10")
        .put(b"y", b"20")
        .delete(b"old");
    e.write_batch(batch).unwrap();

    assert_eq!(e.get(b"x").unwrap().unwrap().version, 5);
    assert_eq!(e.get(b"y").unwrap().unwrap().version, 5);
    assert_eq!(e.get(b"old").unwrap(), None);
    assert_eq!(e.latest_version(), 5);
}

#[test]
fn non_monotonic_version_is_rejected() {
    let e = MemoryEngine::new();
    e.put(b"k", b"v", 10).unwrap();
    let err = e.put(b"k", b"v2", 10).unwrap_err();
    assert!(matches!(
        err,
        StorageError::NonMonotonicVersion {
            got: 10,
            latest: 10
        }
    ));
    // The rejected write left no trace.
    assert_eq!(e.get(b"k").unwrap().unwrap().value, b"v");
}

#[test]
fn snapshot_scan_is_isolated() {
    let e = MemoryEngine::new();
    e.put(b"a", b"1", 1).unwrap();
    e.put(b"b", b"2", 2).unwrap();
    let snap = e.snapshot();

    e.put(b"c", b"3", 3).unwrap();
    e.delete(b"a", 4).unwrap();

    let snap_keys: Vec<_> = snap.scan(b"", b"z").into_iter().map(|(k, _)| k).collect();
    assert_eq!(snap_keys, vec![b"a".to_vec(), b"b".to_vec()]);

    let live_keys: Vec<_> = e
        .scan(b"", b"z")
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(live_keys, vec![b"b".to_vec(), b"c".to_vec()]);
}
