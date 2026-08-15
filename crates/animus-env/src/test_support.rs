//! Cross-crate test support for [`SegmentStore`] implementations (ADR 0043
//! §A7): one contract-assertion function exercised against every
//! implementation, so the trait's behavioral guarantees are pinned once
//! instead of hand-copied per impl.
//!
//! Always compiled (not `#[cfg(test)]`, which only gates *this* crate's own
//! test binaries) and `#[doc(hidden)]`, mirroring `animus-storage`'s
//! `LsmEngine` introspection helpers: plain public functions meant for test
//! code in other crates, not part of the crate's primary API surface.
//! `animus-sim`'s `SimSegmentStore` tests and this crate's own
//! `FsSegmentStore` tests both call [`assert_segment_store_contract`].

use crate::SegmentStore;

/// Assert the [`SegmentStore`] trait contract holds for `store`: put/get
/// round-trip; idempotent overwrite (same id + same bytes, and same id +
/// different bytes, are both `Ok` — last-write-wins); a never-written id
/// reads `None`; delete is idempotent (including deleting an absent id); a
/// deleted id reads `None`, not an error; a put after a delete resurrects
/// the id (deletion is not a poison); `list` filters by prefix and excludes
/// deleted ids.
///
/// Every id this function writes is scoped under the `"contract-test/"`
/// prefix and cleaned up (deleted) before returning, so it composes with a
/// caller that already has other data in the same store — `store` need not
/// start out empty, only free of ids under that one prefix.
///
/// Does **not** exercise fault injection, replication, or path-traversal
/// rejection — those are implementation-specific (see `SimSegmentStore`'s
/// own fault-injection tests and `FsSegmentStore`'s own path-guard test).
#[doc(hidden)]
pub async fn assert_segment_store_contract<S: SegmentStore>(store: &S) {
    let id_a = "contract-test/a";
    let id_b = "contract-test/b";
    let id_nested = "contract-test/nested/c";
    let id_missing = "contract-test/never-written";

    // put/get round-trip.
    store.put(id_a, b"hello").await.expect("put a");
    assert_eq!(
        store.get(id_a).await.expect("get a"),
        Some(b"hello".to_vec()),
        "get must return exactly what was put"
    );

    // Write-once, identical-bytes case: a safe no-op.
    store
        .put(id_a, b"hello")
        .await
        .expect("put a again, identical bytes must be a safe no-op");
    assert_eq!(
        store.get(id_a).await.expect("get a"),
        Some(b"hello".to_vec())
    );

    // Write-once, differing-bytes case: a hard error, and the stored
    // content must be untouched by the rejected attempt.
    store
        .put(id_a, b"goodbye")
        .await
        .expect_err("differing bytes at an existing id must be rejected (write-once)");
    assert_eq!(
        store.get(id_a).await.expect("get a"),
        Some(b"hello".to_vec()),
        "a rejected write-once violation must not change the stored bytes"
    );

    // A never-written id reads None, not an error.
    assert_eq!(
        store.get(id_missing).await.expect("get missing"),
        None,
        "a never-written id must read None"
    );

    // A multi-segment (nested) id — the shape production ids actually take.
    store.put(id_nested, b"nested").await.expect("put nested");
    assert_eq!(
        store.get(id_nested).await.expect("get nested"),
        Some(b"nested".to_vec())
    );

    // A second id, so `list` has more than one entry to filter over.
    store.put(id_b, b"b-bytes").await.expect("put b");

    let listed = store.list("contract-test/").await.expect("list");
    assert!(
        listed.contains(&id_a.to_string()),
        "list must include a: {listed:?}"
    );
    assert!(
        listed.contains(&id_b.to_string()),
        "list must include b: {listed:?}"
    );
    assert!(
        listed.contains(&id_nested.to_string()),
        "list must include the nested id: {listed:?}"
    );

    // Delete: idempotent, and a subsequent get is a defined None, not an
    // error.
    store.delete(id_a).await.expect("delete a");
    store.delete(id_a).await.expect("delete a again is a no-op");
    assert_eq!(
        store.get(id_a).await.expect("get deleted a"),
        None,
        "get after delete must read None, never error"
    );

    // Deleting something that never existed is also Ok.
    store
        .delete(id_missing)
        .await
        .expect("delete of a never-written id is Ok");

    // `list` excludes the deleted id but still includes the survivors.
    let listed_after_delete = store
        .list("contract-test/")
        .await
        .expect("list after delete");
    assert!(
        !listed_after_delete.contains(&id_a.to_string()),
        "list must exclude a deleted id: {listed_after_delete:?}"
    );
    assert!(listed_after_delete.contains(&id_b.to_string()));
    assert!(listed_after_delete.contains(&id_nested.to_string()));

    // A narrower prefix filters down to exactly the matching id.
    let listed_b_only = store.list("contract-test/b").await.expect("list prefix b");
    assert_eq!(listed_b_only, vec![id_b.to_string()]);

    // Deletion is not a poison: a fresh put at the same id afterward
    // resurrects it with the new bytes.
    store
        .put(id_a, b"resurrected")
        .await
        .expect("put a after delete (resurrect)");
    assert_eq!(
        store.get(id_a).await.expect("get resurrected a"),
        Some(b"resurrected".to_vec()),
        "put after delete must resurrect the id with the new bytes"
    );

    // Cleanup, so a caller running this against a shared/long-lived store
    // isn't left with residue.
    store.delete(id_a).await.expect("cleanup delete a");
    store.delete(id_b).await.expect("cleanup delete b");
    store
        .delete(id_nested)
        .await
        .expect("cleanup delete nested");
}
