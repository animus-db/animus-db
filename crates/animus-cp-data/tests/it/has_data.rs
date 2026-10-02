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
    let bounded = StorageScope::new(KeyRange::new(Vec::new(), Some(b"m".to_vec())));
    let unbounded = StorageScope::new(KeyRange::whole());
    assert!(!block_on(bounded.has_data(&engine)));
    assert!(!block_on(unbounded.has_data(&engine)));
}

/// Once a key inside the scope's range is written, `has_data` flips true —
/// the "reforming after a restart" case — for both a bounded and an
/// open-ended range.
#[test]
fn engine_with_a_key_in_range_has_data_bounded_and_unbounded() {
    let engine = MemoryEngine::new();
    let bounded = StorageScope::new(KeyRange::new(Vec::new(), Some(b"m".to_vec())));
    block_on(async {
        engine
            .merge(b"T:a", b"v", 1)
            .await
            .expect("seed write succeeds");
    });
    assert!(block_on(bounded.has_data(&engine)));

    let unbounded = StorageScope::new(KeyRange::whole());
    block_on(async {
        engine
            .merge(b"U:z", b"v", 2)
            .await
            .expect("seed write succeeds");
    });
    assert!(block_on(unbounded.has_data(&engine)));
}

/// A kind scope must never report data present because a **sibling kind**
/// or an engine-global reserved-namespace marker happens to hold rows in
/// this tablet's own private engine (F2b: the engine is the tablet — the
/// pre-pivot sibling-*tenant* exclusions died with the shared engine; kind
/// and marker exclusion are what remain load-bearing).
#[test]
fn has_data_never_counts_a_sibling_kinds_rows_or_a_marker() {
    let engine = MemoryEngine::new();
    let base = StorageScope::new(KeyRange::whole()).with_kind(animus_cp_data::KIND_BASE);

    // A cursor-kind row (0x04 lead) — bookkeeping, not base data.
    block_on(async {
        engine
            .merge(b"\x04some-cursor-row", b"v", 1)
            .await
            .expect("seed write succeeds");
    });
    // An engine-global reserved-namespace marker (0x5F lead).
    block_on(async {
        engine
            .merge(b"_animus-style-marker", b"v", 2)
            .await
            .expect("seed write succeeds");
    });
    assert!(
        !block_on(base.has_data(&engine)),
        "sibling-kind rows and reserved-namespace markers must not count as \
         this tablet's base data"
    );

    // A real base-kind row flips it.
    block_on(async {
        engine
            .merge(b"\x00k", b"v", 3)
            .await
            .expect("seed write succeeds");
    });
    assert!(block_on(base.has_data(&engine)));
}
