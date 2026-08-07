//! `StorageScope::has_data` (PR5 of the single-command-split redesign): the
//! presence check `animusd` uses on a shared engine to distinguish a node
//! **reforming** a group it already hosted before a restart from one
//! **joining fresh** as a reconciler-placed spare, now that there is no
//! per-tablet dedicated engine file to ask instead.

use animus_cp_data::StorageScope;
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::KeyRange;
use futures::executor::block_on;

/// A fresh scope over an empty engine has no data — the "joining fresh"
/// case — for both a bounded and an open-ended range.
#[test]
fn empty_engine_has_no_data_bounded_and_unbounded() {
    let engine = MemoryEngine::new();
    let bounded = StorageScope::new(
        b"T:".to_vec(),
        KeyRange::new(Vec::new(), Some(b"m".to_vec())),
    );
    let unbounded = StorageScope::new(b"T:".to_vec(), KeyRange::whole());
    assert!(!block_on(bounded.has_data(&engine)));
    assert!(!block_on(unbounded.has_data(&engine)));
}

/// Once a key inside the scope's range is written, `has_data` flips true —
/// the "reforming after a restart" case — for both a bounded and an
/// open-ended range.
#[test]
fn engine_with_a_key_in_range_has_data_bounded_and_unbounded() {
    let engine = MemoryEngine::new();
    let bounded = StorageScope::new(
        b"T:".to_vec(),
        KeyRange::new(Vec::new(), Some(b"m".to_vec())),
    );
    block_on(async {
        engine
            .merge(b"T:a", b"v", 1)
            .await
            .expect("seed write succeeds");
    });
    assert!(block_on(bounded.has_data(&engine)));

    let unbounded = StorageScope::new(b"U:".to_vec(), KeyRange::whole());
    block_on(async {
        engine
            .merge(b"U:z", b"v", 2)
            .await
            .expect("seed write succeeds");
    });
    assert!(block_on(unbounded.has_data(&engine)));
}

/// A scope must never report data present because a **sibling** tenant
/// (different prefix, or same prefix but a disjoint range) happens to hold
/// some — the whole point of scoping a shared engine.
#[test]
fn has_data_never_sees_a_sibling_scopes_data() {
    let engine = MemoryEngine::new();
    // Different table prefix entirely.
    block_on(async {
        engine
            .merge(b"OTHER:x", b"v", 1)
            .await
            .expect("seed write succeeds");
    });
    let mine = StorageScope::new(b"MINE:".to_vec(), KeyRange::whole());
    assert!(!block_on(mine.has_data(&engine)));

    // Same table prefix, but a disjoint tablet range (a post-split sibling).
    block_on(async {
        engine
            .merge(b"MINE:z", b"v", 2) // >= "m", outside the "lo" range below
            .await
            .expect("seed write succeeds");
    });
    let lo_half = StorageScope::new(
        b"MINE:".to_vec(),
        KeyRange::new(Vec::new(), Some(b"m".to_vec())),
    );
    assert!(
        !block_on(lo_half.has_data(&engine)),
        "a sibling tablet's data under the same table prefix must not count as \
         this tablet's own"
    );
}
