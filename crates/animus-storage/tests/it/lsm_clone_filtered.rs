//! `LsmEngine::clone_to_filtered` (ADR 0058 fork closed) — the range/kind-aware
//! sibling of `clone_to`'s plain full-engine clone (covered by
//! `lsm_clone.rs`/`lsm_clone_prodenv.rs`/`lsm_clone_concurrent.rs`, all
//! unaffected by this file: `clone_to` is now a thin wrapper over this
//! method with one whole-keyspace `keep` range, so those suites double as
//! this method's own "keep = everything" regression).
//!
//! Covers:
//!
//! 1. **Whole-file assignment**: a table whose own `[min_key, max_key]`
//!    falls entirely outside every `keep` range is never linked into the
//!    target at all (proven by `sstable_count()`/`sstable_views()` on the
//!    clone, not just by absent rows — the point of this design is that the
//!    file itself never becomes a directory entry in the target's
//!    namespace).
//! 2. A table straddling a `keep` boundary is still linked **whole** — rows
//!    outside `keep` ride along, exactly as a caller's own post-clone
//!    `delete_range` trim step already expects.
//! 3. The target opens correctly and serves exactly the keep-set rows.
//! 4. Crash injection mid-clone: the same fault/retry contract `clone_to`
//!    itself carries (`lsm_clone.rs`'s own test), now exercised through the
//!    filtered entry point with a `keep` set that actually excludes some
//!    tables — proving the filtering step itself introduces no new
//!    torn-clone window.
//!
//! The memtable snapshot is filtered the identical way, key by key — proven
//! two other ways, not in *this* file: `crate::lsm::clone_filter_tests`
//! (in `src/lsm.rs`) unit-tests the extracted `key_in_keep` predicate
//! directly, and `lsm_clone_filtered_concurrent.rs` (real multi-thread
//! `ProdEnv`) proves the end-to-end leftover-memtable path — a genuinely
//! still-memtable-resident row at snapshot time only arises from a
//! concurrent writer racing this method's own internal `flush()`, a window
//! the deterministic, single-threaded `SimEnv` this file uses cannot
//! reproduce (see that file's own module doc, mirroring
//! `lsm_clone_concurrent.rs`'s identical rationale for plain `clone_to`).

use animus_env::nid;
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use futures::executor::block_on;

const SRC: &str = "src/";
const DST: &str = "dst/";

/// No auto-compaction and a large flush threshold — every table below is
/// produced by an explicit `flush_now()`, so the source's own table set
/// (and its key ranges) are exactly what the test built, nothing more.
fn no_compact_opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 1 << 20,
        compaction_trigger: 100,
        target_table_bytes: 1 << 20,
        level_fanout: 8,
        wal_segment_bytes: 1 << 20,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: false,
        background_maintenance: false,
    }
}

fn open(sim: &Simulator, prefix: &str, opts: LsmOptions) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with(sim.env(nid(0)), prefix, opts)).expect("open")
}

async fn all_records(e: &LsmEngine<SimEnv>) -> Vec<(Vec<u8>, Option<Vec<u8>>, u64)> {
    let mut v = e.entries_with_tombstones().await.unwrap();
    v.sort();
    v
}

/// Builds a source with three flushed, range-disjoint SSTables — `a*`,
/// `b*`, `c*` — each ten keys, so whole-file assignment has real,
/// independently-linkable files to skip.
async fn seed_three_disjoint_tables(e: &LsmEngine<SimEnv>) {
    let mut v = 0u64;
    for prefix in ["a", "b", "c"] {
        for i in 0..10u64 {
            v += 1;
            e.put(
                format!("{prefix}{i:04}").as_bytes(),
                format!("v{prefix}{i}").as_bytes(),
                v,
            )
            .await
            .unwrap();
        }
        e.flush_now().await.unwrap();
    }
}

#[test]
fn wholly_outside_tables_are_never_linked() {
    let seed = 0xF11E_0001;
    let sim = Simulator::new(seed);
    let src = open(&sim, SRC, no_compact_opts());
    block_on(seed_three_disjoint_tables(&src));
    assert_eq!(
        src.sstable_count(),
        3,
        "seed={seed}: sanity — three flushed, disjoint tables"
    );

    // Keep only the `a*` range: `b*`/`c*` are each wholly outside it.
    let keep = [(b"a".to_vec(), Some(b"b".to_vec()))];
    let clone = block_on(src.clone_to_filtered(DST, &keep)).expect("clone_to_filtered");

    assert_eq!(
        clone.sstable_count(),
        1,
        "seed={seed}: the `b*`/`c*` tables are wholly outside `keep` and must \
         never be linked into the target at all"
    );
    let clone_view = block_on(all_records(&clone));
    assert_eq!(
        clone_view.len(),
        10,
        "seed={seed}: only the ten `a*` rows survive"
    );
    assert!(
        clone_view.iter().all(|(k, _, _)| k.starts_with(b"a")),
        "seed={seed}: no `b*`/`c*` row leaked into the filtered clone: {clone_view:?}"
    );

    // Independent reopen confirms this is real durable state, not a view
    // riding the live handle.
    let reopened = open(&sim, DST, no_compact_opts());
    assert_eq!(
        block_on(all_records(&reopened)),
        clone_view,
        "seed={seed}: the filtered clone's durable state matches on reopen"
    );
}

#[test]
fn a_boundary_straddling_table_is_linked_whole() {
    let seed = 0xF11E_0002;
    let sim = Simulator::new(seed);
    let src = open(&sim, SRC, no_compact_opts());
    block_on(seed_three_disjoint_tables(&src));

    // `keep` = [a0005, b0000): straddles the `a*` table's own range
    // ([a0000, a0009]) — a genuine boundary case, not a clean split — while
    // cleanly excluding `b*` ([b0000, b0009], starts exactly at `keep`'s own
    // exclusive end) and `c*` entirely.
    let keep = [(b"a0005".to_vec(), Some(b"b0000".to_vec()))];
    let clone = block_on(src.clone_to_filtered(DST, &keep)).expect("clone_to_filtered");

    assert_eq!(
        clone.sstable_count(),
        1,
        "seed={seed}: only the straddling `a*` table is linked; `b*`/`c*` are \
         wholly outside `keep` and excluded"
    );
    let clone_view = block_on(all_records(&clone));
    assert_eq!(
        clone_view.len(),
        10,
        "seed={seed}: the straddling table is linked WHOLE — all ten `a*` \
         rows are present, including a0000..a0004, which fall BEFORE keep's \
         own start (a0005): {clone_view:?}"
    );
    assert!(
        clone_view.iter().all(|(k, _, _)| k.starts_with(b"a")),
        "seed={seed}: still no `b*`/`c*` row present: {clone_view:?}"
    );
}

#[test]
fn crash_mid_filtered_clone_leaves_no_visible_clone_and_retry_succeeds() {
    let seed = 0xF11E_0004;
    let sim = Simulator::new(seed);
    let src = open(&sim, SRC, no_compact_opts());
    block_on(seed_three_disjoint_tables(&src));

    let keep = [(b"a".to_vec(), Some(b"b".to_vec()))];
    let expected = {
        // What a clean filtered clone must equal, computed once up front
        // (never itself under fault injection) so the loop below has a
        // stable oracle.
        let probe_prefix = "probe/";
        let probe = block_on(src.clone_to_filtered(probe_prefix, &keep)).expect("probe clone");
        block_on(all_records(&probe))
    };
    assert_eq!(
        expected.len(),
        10,
        "seed={seed}: sanity — the probe clone kept exactly the `a*` rows"
    );

    let mut fault = DiskConfig::default();
    fault.set_error_prob(0.35);

    let mut failures = 0u32;
    let mut succeeded = false;
    for attempt in 0..500u32 {
        sim.set_disk_config(fault.clone());
        let result = block_on(src.clone_to_filtered(DST, &keep));
        sim.set_disk_config(DiskConfig::default());

        match result {
            Ok(_clone) => {
                succeeded = true;
                break;
            }
            Err(_) => {
                failures += 1;
                let dst_engine = open(&sim, DST, no_compact_opts());
                let dst_view = block_on(all_records(&dst_engine));
                assert!(
                    dst_view.is_empty() || dst_view == expected,
                    "seed={seed} attempt={attempt}: a failed clone_to_filtered left \
                     a partial/torn clone at the target: {dst_view:?}"
                );
                assert_eq!(
                    src.sstable_count(),
                    3,
                    "seed={seed} attempt={attempt}: a failed clone_to_filtered must \
                     not disturb the source engine's own table set"
                );
            }
        }
    }
    assert!(
        failures > 0,
        "seed={seed}: expected the configured fault rate to trip at least once"
    );
    assert!(
        succeeded,
        "seed={seed}: retrying clone_to_filtered after transient faults must \
         eventually succeed ({failures} failures observed first)"
    );

    let clone = block_on(src.clone_to_filtered(DST, &keep)).expect("clean retry after faults");
    assert_eq!(
        block_on(all_records(&clone)),
        expected,
        "seed={seed}: the eventually-successful filtered clone matches the oracle"
    );
    assert_eq!(
        clone.sstable_count(),
        1,
        "seed={seed}: the eventually-successful clone still excludes the \
         wholly-outside `b*`/`c*` tables"
    );
}
