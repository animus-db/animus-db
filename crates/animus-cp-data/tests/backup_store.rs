//! Backup object naming + codec (`animus_cp_data::backup`, ADR 0059 §2/§4)
//! exercised through a real [`SegmentStore`](animus_env::SegmentStore) —
//! `animus-sim`'s `SimSegmentStore`, the identical corpus store the stream
//! sealer's own fault-injection suite reuses (ADR 0059 §1's explicit
//! "reuses the `SegmentStore` trait unchanged" decision). Plumbing-only
//! coverage (ADR 0059 Train 1 PR②): no capture driver exists yet, so this
//! drives the codec + naming helpers directly against the store rather than
//! through any higher-level backup machinery.

use animus_control::{
    BackupManifest, BackupPinnedTablet, BackupTabletProgress, ColumnType, TableSchema,
};
use animus_cp_data::backup::{
    BackupManifestObject, BackupManifestTabletEntry, backup_data_object_id,
    backup_manifest_object_id, decode_data_chunk, decode_manifest_object, encode_data_chunk,
    encode_manifest_object,
};
use animus_cp_data::{KIND_BASE, KIND_FOOTPRINT, SeedRow};
use animus_env::{EnvExt, SegmentStore, nid};
use animus_sim::{SimSegmentStore, Simulator};
use animus_tablet::{KeyRange, TabletId};

const MAX_STEPS: usize = 10_000;

// `backup_id` is not itself part of the manifest object (it rides the
// object's own id, `backup_manifest_object_id`, and the catalog row's key —
// ADR 0059 §3's "keyed by backup id, never embedded as data" convention);
// the parameter here exists only so a caller can build one manifest per
// test scenario without every one being byte-identical.
fn sample_manifest_object(created_wall_ms: u64) -> BackupManifestObject {
    BackupManifestObject {
        manifest: BackupManifest {
            schema: TableSchema::simple("pk", ColumnType::String),
            pinned_tablets: vec![
                BackupPinnedTablet {
                    tablet: TabletId(1),
                    range: KeyRange::whole(),
                },
                BackupPinnedTablet {
                    tablet: TabletId(2),
                    range: KeyRange::whole(),
                },
            ],
            created_wall_ms,
        },
        tablet_progress: vec![
            BackupManifestTabletEntry {
                tablet: TabletId(1),
                progress: BackupTabletProgress {
                    cut_version: 10,
                    bytes: 100,
                },
            },
            BackupManifestTabletEntry {
                tablet: TabletId(2),
                progress: BackupTabletProgress {
                    cut_version: 11,
                    bytes: 200,
                },
            },
        ],
    }
}

fn sample_rows(tablet: u64, n: u64) -> Vec<SeedRow> {
    (0..n)
        .map(|i| {
            let key = format!("t{tablet}-k{i}").into_bytes();
            if i % 4 == 0 {
                (KIND_FOOTPRINT, key, None, tablet * 1000 + i)
            } else {
                (
                    KIND_BASE,
                    key,
                    Some(format!("v{i}").into_bytes()),
                    tablet * 1000 + i,
                )
            }
        })
        .collect()
}

/// The manifest object is the backup's own durability commit point (ADR
/// 0059 §4): encode it, `put` it at its own object id, `get` it back from a
/// fresh store handle (a different clone, as a different task/process would
/// hold), decode, and confirm it round-trips byte-for-byte.
#[test]
fn manifest_object_round_trips_through_a_real_segment_store() {
    let mut sim = Simulator::new(101);
    let store = SimSegmentStore::new(sim.env(nid(0)));
    let store_for_task = store.clone();
    sim.env(nid(0)).spawn_task(async move {
        let obj = sample_manifest_object(1_723_000_000_000);
        let id = backup_manifest_object_id("bkp-1");
        let bytes = encode_manifest_object(&obj);
        store_for_task.put(&id, &bytes).await.expect("put manifest");

        // A fresh clone, as a later restore driver's own handle would be.
        let reader = store_for_task.clone();
        let fetched = reader
            .get(&id)
            .await
            .expect("get")
            .expect("manifest object must be present");
        let decoded = decode_manifest_object(&fetched).expect("decode");
        assert_eq!(decoded, obj);
    });
    assert!(sim.run_until_quiescent(MAX_STEPS), "must settle");
}

/// A tablet's chunked data objects (ADR 0059 §2/§4): several chunks, each
/// its own object at `backup/{backup_id}/{tablet}/{chunk}`, all round-trip
/// independently and are discoverable by a debug/sweep `list` over the
/// backup's own prefix.
#[test]
fn chunked_data_objects_round_trip_and_list_under_the_backup_prefix() {
    let mut sim = Simulator::new(102);
    let store = SimSegmentStore::new(sim.env(nid(0)));
    let store_for_task = store.clone();
    sim.env(nid(0)).spawn_task(async move {
        let backup_id = "bkp-2";
        let tablet = 7u64;
        let chunks: Vec<Vec<SeedRow>> = vec![sample_rows(tablet, 3), sample_rows(tablet, 5)];

        for (chunk_idx, rows) in chunks.iter().enumerate() {
            let id = backup_data_object_id(backup_id, tablet, chunk_idx as u64);
            let bytes = encode_data_chunk(rows);
            store_for_task.put(&id, &bytes).await.expect("put chunk");
        }

        for (chunk_idx, rows) in chunks.iter().enumerate() {
            let id = backup_data_object_id(backup_id, tablet, chunk_idx as u64);
            let fetched = store_for_task
                .get(&id)
                .await
                .expect("get")
                .unwrap_or_else(|| panic!("chunk {chunk_idx} must be present"));
            let decoded = decode_data_chunk(&fetched).expect("decode");
            assert_eq!(&decoded, rows);
        }

        let listed = store_for_task
            .list(&format!("backup/{backup_id}/"))
            .await
            .expect("list");
        assert_eq!(listed.len(), chunks.len());
    });
    assert!(sim.run_until_quiescent(MAX_STEPS), "must settle");
}

/// Two backups of the same table never collide, and a data object's tablet
/// axis is independent of another tablet's within the same backup — the
/// object-id scheme's own disjointness, proven here against a real store
/// rather than just as a pure string comparison (`backup.rs`'s own unit
/// tests already cover the latter).
#[test]
fn distinct_backups_and_tablets_never_collide_in_a_shared_store() {
    let mut sim = Simulator::new(103);
    let store = SimSegmentStore::new(sim.env(nid(0)));
    let store_for_task = store.clone();
    sim.env(nid(0)).spawn_task(async move {
        let a_rows = sample_rows(1, 2);
        let b_rows = sample_rows(1, 2); // same tablet id, different backup
        let a_id = backup_data_object_id("bkp-a", 1, 0);
        let b_id = backup_data_object_id("bkp-b", 1, 0);
        assert_ne!(a_id, b_id);

        store_for_task
            .put(&a_id, &encode_data_chunk(&a_rows))
            .await
            .expect("put a");
        store_for_task
            .put(&b_id, &encode_data_chunk(&b_rows))
            .await
            .expect("put b");

        let a_back = decode_data_chunk(
            &store_for_task
                .get(&a_id)
                .await
                .expect("get a")
                .expect("a present"),
        )
        .expect("decode a");
        let b_back = decode_data_chunk(
            &store_for_task
                .get(&b_id)
                .await
                .expect("get b")
                .expect("b present"),
        )
        .expect("decode b");
        assert_eq!(a_back, a_rows);
        assert_eq!(b_back, b_rows);
    });
    assert!(sim.run_until_quiescent(MAX_STEPS), "must settle");
}

/// The write-once discipline (`animus_env::SegmentStore::put`'s own
/// contract, reused unchanged for backups per ADR 0059 §1): re-`put`ting a
/// backup object's id with byte-identical content is a safe no-op (a
/// same-attempt retry after a lost ack); re-`put`ting it with genuinely
/// different content is a hard error, and the stored bytes are left
/// untouched.
#[test]
fn backup_objects_are_write_once_like_every_segment_store_object() {
    let mut sim = Simulator::new(104);
    let store = SimSegmentStore::new(sim.env(nid(0)));
    let store_for_task = store.clone();
    sim.env(nid(0)).spawn_task(async move {
        let manifest_id = backup_manifest_object_id("bkp-3");
        let first = encode_manifest_object(&sample_manifest_object(1_723_000_000_001));

        store_for_task
            .put(&manifest_id, &first)
            .await
            .expect("first put");

        // Identical bytes: a safe no-op (the retry-after-lost-ack case).
        store_for_task
            .put(&manifest_id, &first)
            .await
            .expect("identical-content re-put must succeed");
        assert_eq!(
            store_for_task.get(&manifest_id).await.expect("get"),
            Some(first.clone())
        );

        // Genuinely different content at the same id: rejected, and the
        // originally-stored bytes are unchanged.
        let mut different_backup = sample_manifest_object(1_723_000_000_001);
        different_backup.manifest.created_wall_ms += 1;
        let second = encode_manifest_object(&different_backup);
        assert_ne!(first, second, "the two encodings must actually differ");
        let err = store_for_task
            .put(&manifest_id, &second)
            .await
            .expect_err("a write-once violation must be rejected");
        drop(err);
        assert_eq!(
            store_for_task.get(&manifest_id).await.expect("get"),
            Some(first),
            "a rejected write-once violation must not change the stored bytes"
        );

        // A genuinely different id (a different chunk of the SAME backup)
        // is an unrelated first write, unaffected by the rejection above.
        let chunk_id = backup_data_object_id("bkp-3", 1, 0);
        store_for_task
            .put(&chunk_id, &encode_data_chunk(&sample_rows(1, 1)))
            .await
            .expect("a different object id is a plain first write");
    });
    assert!(sim.run_until_quiescent(MAX_STEPS), "must settle");
}
