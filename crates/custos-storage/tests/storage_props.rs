//! M2 acceptance: property tests for the in-memory storage engine.
//!
//! Three properties, checked against a simple `BTreeMap` reference model:
//! key/value round-trips, range-scan ordering, and snapshot isolation between a
//! snapshot read and concurrent (higher-version) writes.

use std::collections::BTreeMap;

use custos_storage::{MemoryEngine, Snapshot, StorageEngine};
use futures::executor::block_on;
use proptest::prelude::*;

/// Small byte strings keep the search space dense so keys actually collide and
/// ranges actually overlap.
fn small_bytes() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(0u8..4, 0..4)
}

proptest! {
    /// Writing a sequence of puts at monotonically increasing versions leaves
    /// each key holding the last value written to it.
    #[test]
    fn put_get_roundtrip(ops in proptest::collection::vec((small_bytes(), small_bytes()), 1..60)) {
        let engine = MemoryEngine::new();
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        for (i, (k, v)) in ops.iter().enumerate() {
            let version = i as u64 + 1;
            block_on(engine.put(k, v, version)).unwrap();
            model.insert(k.clone(), v.clone());
        }

        for (k, v) in &model {
            let got = block_on(engine.get(k)).unwrap();
            prop_assert_eq!(got.map(|vv| vv.value), Some(v.clone()));
        }
        prop_assert_eq!(engine.latest_version(), ops.len() as u64);
    }

    /// A scan over `[start, end)` returns exactly the live keys in that range,
    /// in ascending key order, matching the reference model.
    #[test]
    fn scan_is_ordered_and_bounded(
        entries in proptest::collection::btree_map(small_bytes(), small_bytes(), 0..40),
        a in small_bytes(),
        b in small_bytes(),
    ) {
        let engine = MemoryEngine::new();
        for (i, (k, v)) in entries.iter().enumerate() {
            block_on(engine.put(k, v, i as u64 + 1)).unwrap();
        }
        let (start, end) = if a <= b { (a, b) } else { (b, a) };

        let got = block_on(engine.scan(&start, &end)).unwrap();

        // Ordered ascending by key.
        for pair in got.windows(2) {
            prop_assert!(pair[0].0 < pair[1].0, "scan keys not strictly ascending");
        }
        // Exactly the model's keys in [start, end).
        let expected: Vec<(Vec<u8>, Vec<u8>)> = entries
            .range(start.clone()..end.clone())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let got_kv: Vec<(Vec<u8>, Vec<u8>)> =
            got.into_iter().map(|(k, vv)| (k, vv.value)).collect();
        prop_assert_eq!(got_kv, expected);
    }

    /// A snapshot taken at version `v` is isolated: later writes (at versions
    /// `> v`) never change what the snapshot reads.
    #[test]
    fn snapshot_isolated_from_later_writes(
        initial in proptest::collection::btree_map(small_bytes(), small_bytes(), 0..30),
        updates in proptest::collection::vec((small_bytes(), small_bytes(), any::<bool>()), 0..40),
    ) {
        let engine = MemoryEngine::new();
        let mut version = 0u64;
        let mut reference: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        for (k, v) in &initial {
            version += 1;
            block_on(engine.put(k, v, version)).unwrap();
            reference.insert(k.clone(), v.clone());
        }

        let snap = engine.snapshot();
        prop_assert_eq!(snap.version(), version);

        // Concurrent writes at strictly higher versions, plus a key set we can
        // check is invisible to the snapshot.
        let mut touched_keys: Vec<Vec<u8>> = Vec::new();
        for (k, v, is_delete) in &updates {
            version += 1;
            if *is_delete {
                block_on(engine.delete(k, version)).unwrap();
            } else {
                block_on(engine.put(k, v, version)).unwrap();
            }
            touched_keys.push(k.clone());
        }

        // The snapshot still reflects exactly the reference (pre-snapshot) state.
        for (k, v) in &reference {
            prop_assert_eq!(
                block_on(snap.get(k)).map(|vv| vv.value),
                Some(v.clone()),
                "snapshot read changed after later writes"
            );
        }
        // Keys that only exist post-snapshot are invisible to the snapshot.
        for k in &touched_keys {
            if !reference.contains_key(k) {
                prop_assert_eq!(block_on(snap.get(k)), None, "snapshot saw a post-snapshot key");
            }
        }
    }
}
