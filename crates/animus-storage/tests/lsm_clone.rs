//! `LsmEngine::clone_to` (ADR 0058 rung 2): the SSTable-granularity engine
//! clone primitive — cloning an engine's current durable state into a new,
//! independent engine on the same `Env`.
//!
//! Covers:
//!
//! 1. **Equivalence + isolation**: a clone's full scan (values, overwrites,
//!    deletes) matches the source at clone time, an independent reopen of
//!    the clone's prefix sees the same durable state, and writes to either
//!    engine after cloning are never visible through the other.
//! 2. **Crash injection** (`SimEnv`'s opt-in `DiskConfig` fault model): a
//!    fault landing mid-`clone_to` leaves the source completely unaffected,
//!    and the target is always either **fully absent** or **fully valid**
//!    — never a torn, partially-linked clone. (The one case an `Err` does
//!    *not* imply "nothing was committed" is a fault in `clone_to`'s own
//!    final `open` of the just-written target — the manifest commit can
//!    have already durably succeeded by then; see the method's own doc
//!    comment. The test accounts for this rather than treating every `Err`
//!    as proof of an empty target.) A retry after the fault clears
//!    succeeds. A *successful* `clone_to` call *is* this design's "crash
//!    after rename" case — by the time it returns, the manifest commit
//!    already happened, so a fresh reopen of the clone's prefix in test 1
//!    above stands in for that scenario rather than repeating it as a
//!    separate test.
//! 3. **Source compaction racing a live clone's files**: cloning, then
//!    forcing the *source* to compact away the very tables the clone links,
//!    must not disturb the clone — hard links are exactly what makes this
//!    safe (the clone's own directory entries keep the bytes alive).
//!
//! A real-filesystem `ProdEnv` regression (real hard links, not merely the
//! `SimEnv` model) lives in `lsm_clone_prodenv.rs`.

use animus_env::nid;
use animus_sim::{DiskConfig, SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use futures::executor::block_on;

const SRC: &str = "src/";
const DST: &str = "dst/";

/// Small thresholds so a realistic workload produces several SSTables across
/// a couple of levels (flushes *and* compactions), exercising more than the
/// single-table case.
fn churn_opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 200,
        compaction_trigger: 3,
        target_table_bytes: 1024,
        level_fanout: 2,
        wal_segment_bytes: 4096,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: false,
        background_maintenance: false,
    }
}

/// No auto-compaction, for the fault-injection test: keeps the exact number
/// of `clone_to`-visited files (and so the failure probability per attempt)
/// small and predictable.
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

/// Puts, same-key overwrites (newer version), and deletes, in that order —
/// so the clone must carry over live values, the *winning* version of an
/// overwritten key, and tombstones, not just an arbitrary snapshot. `put`
/// enforces a single **engine-wide** monotonic version floor (not a
/// per-key one), so every op below shares one strictly-increasing counter.
async fn seed_workload(e: &LsmEngine<SimEnv>) {
    let mut v = 0u64;
    for i in 0..60u64 {
        v += 1;
        e.put(
            format!("k{i:04}").as_bytes(),
            format!("v{i}-a").as_bytes(),
            v,
        )
        .await
        .unwrap();
    }
    for i in (0..60u64).step_by(3) {
        v += 1;
        e.put(
            format!("k{i:04}").as_bytes(),
            format!("v{i}-b").as_bytes(),
            v,
        )
        .await
        .unwrap();
    }
    for i in (0..60u64).step_by(5) {
        v += 1;
        e.delete(format!("k{i:04}").as_bytes(), v).await.unwrap();
    }
}

/// Every `(key, value-or-tombstone, version)` record, sorted — the strictest
/// available equality check (it distinguishes a clone that merely matches
/// the *live* view from one that also carries tombstones and full history
/// correctly).
async fn all_records(e: &LsmEngine<SimEnv>) -> Vec<(Vec<u8>, Option<Vec<u8>>, u64)> {
    let mut v = e.entries_with_tombstones().await.unwrap();
    v.sort();
    v
}

#[test]
fn clone_preserves_state_and_isolates_subsequent_writes() {
    let seed = 0x5105_7AB1;
    let sim = Simulator::new(seed);
    let src = open(&sim, SRC, churn_opts());
    block_on(seed_workload(&src));

    let clone = block_on(src.clone_to(DST)).expect("clone_to");

    let src_view = block_on(all_records(&src));
    let clone_view = block_on(all_records(&clone));
    assert!(
        !src_view.is_empty(),
        "seed={seed}: sanity — the workload must have written data"
    );
    assert_eq!(
        src_view, clone_view,
        "seed={seed}: clone must match the source's full record set (values, \
         overwrite winners, and tombstones) at clone time"
    );

    // A completely independent reopen of the clone's prefix sees the same
    // durable state — it's a real, durable engine, not a view riding the
    // live handle `clone_to` happened to return.
    let reopened = open(&sim, DST, churn_opts());
    assert_eq!(
        block_on(all_records(&reopened)),
        clone_view,
        "seed={seed}: an independent reopen of the clone's prefix must see \
         the same durable state (this also stands in for the 'crash after \
         rename' case: by the time clone_to returned, the manifest commit \
         had already happened)"
    );

    // Isolation: a write to either engine after cloning must never appear on
    // the other.
    block_on(async {
        src.put(b"only-src", b"1", u64::MAX - 4).await.unwrap();
        clone.put(b"only-clone", b"1", u64::MAX - 4).await.unwrap();
    });
    assert!(
        block_on(src.get(b"only-clone")).unwrap().is_none(),
        "seed={seed}: the source must not see a write made to the clone"
    );
    assert!(
        block_on(clone.get(b"only-src")).unwrap().is_none(),
        "seed={seed}: the clone must not see a write made to the source"
    );
}

#[test]
fn source_compaction_after_clone_does_not_disturb_the_clone() {
    let seed = 0xC027_1100;
    let sim = Simulator::new(seed);
    // A low compaction trigger so real merge work (removing >1 input file)
    // happens on the source after cloning.
    let opts = LsmOptions {
        compaction_trigger: 2,
        ..churn_opts()
    };
    let src = open(&sim, SRC, opts);
    block_on(seed_workload(&src));

    let clone = block_on(src.clone_to(DST)).expect("clone_to");
    let expected = block_on(all_records(&src));
    assert_eq!(
        block_on(all_records(&clone)),
        expected,
        "seed={seed}: clone matches the source right after cloning"
    );
    let compactions_before = src.compaction_count();

    // More writes, then force the SOURCE (only) to compact to quiescence —
    // this physically merges/removes the very SSTable files the clone's own
    // directory entries link.
    block_on(async {
        for i in 60..90u64 {
            src.put(
                format!("k{i:04}").as_bytes(),
                format!("v{i}").as_bytes(),
                1000 + i,
            )
            .await
            .unwrap();
        }
    });
    block_on(src.compact_now()).expect("compact_now");
    assert!(
        src.compaction_count() > compactions_before,
        "seed={seed}: sanity — expected at least one compaction to actually run \
         on the source after cloning"
    );

    // The clone — reopened fresh, standing in for a completely separate
    // process — is unaffected: its own linked files are untouched even
    // though the source removed its copies of the same underlying bytes.
    let reopened_clone = open(&sim, DST, churn_opts());
    assert_eq!(
        block_on(all_records(&reopened_clone)),
        expected,
        "seed={seed}: the clone must survive the source compacting away the \
         tables it originally linked"
    );
    // And the source itself, of course, still reads correctly post-compaction.
    block_on(async {
        for i in 60..90u64 {
            assert_eq!(
                src.get(format!("k{i:04}").as_bytes())
                    .await
                    .unwrap()
                    .unwrap()
                    .value,
                format!("v{i}").as_bytes(),
                "seed={seed}: source must still read correctly after compacting"
            );
        }
    });
}

/// A fault landing mid-`clone_to` — on a link or on the final manifest
/// commit — must leave the source completely unaffected and no data visible
/// at the target; retrying after the fault clears must succeed. Uses a tiny,
/// fixed two-SSTable source (via `flush_now`, no auto-compaction) so the
/// per-attempt failure probability under a moderate error rate stays
/// predictable rather than vanishing as the op count grows.
#[test]
fn crash_mid_clone_leaves_no_visible_clone_and_retry_succeeds() {
    let seed = 0xC105_7C2A;
    let sim = Simulator::new(seed);
    let src = open(&sim, SRC, no_compact_opts());
    block_on(async {
        src.put(b"a", b"1", 1).await.unwrap();
        src.flush_now().await.unwrap();
        src.put(b"b", b"2", 2).await.unwrap();
        src.flush_now().await.unwrap();
    });
    assert_eq!(
        src.sstable_count(),
        2,
        "seed={seed}: sanity — two L0 tables"
    );
    let src_view = block_on(all_records(&src));

    let mut fault = DiskConfig::default();
    fault.set_error_prob(0.35);

    let mut failures = 0u32;
    let mut succeeded = false;
    for attempt in 0..500u32 {
        sim.set_disk_config(fault.clone());
        let result = block_on(src.clone_to(DST));
        // Clear the fault before doing any verification I/O of our own —
        // `read`/`replace` are themselves injectable ops, and the checks
        // below must not spuriously fail because of the very fault under
        // test.
        sim.set_disk_config(DiskConfig::default());

        match result {
            Ok(_clone) => {
                succeeded = true;
                break;
            }
            Err(_) => {
                failures += 1;
                // The target is always either fully absent (nothing durable
                // yet — any stray already-linked SSTable file left behind is
                // an unreferenced orphan with no manifest naming it, exactly
                // like a crashed flush's orphan table) or fully valid (the
                // manifest commit had already succeeded and the fault landed
                // in this call's own trailing `open` of the target instead —
                // see `clone_to`'s doc comment on that asymmetry). Never a
                // partial, torn clone.
                let dst_engine = open(&sim, DST, no_compact_opts());
                let dst_view = block_on(all_records(&dst_engine));
                assert!(
                    dst_view.is_empty() || dst_view == src_view,
                    "seed={seed} attempt={attempt}: a failed clone_to left a \
                     partial/torn clone at the target: {dst_view:?}"
                );
                // The source is untouched by a failed clone.
                assert_eq!(
                    block_on(all_records(&src)),
                    src_view,
                    "seed={seed} attempt={attempt}: a failed clone_to must not \
                     disturb the source engine"
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
        "seed={seed}: retrying clone_to after transient faults must eventually \
         succeed ({failures} failures observed first)"
    );

    // A clean retry (fault fully cleared) is a fully valid, matching clone.
    let clone = block_on(src.clone_to(DST)).expect("clean retry after faults cleared");
    assert_eq!(
        block_on(all_records(&clone)),
        src_view,
        "seed={seed}: the eventually-successful clone matches the source"
    );
}
