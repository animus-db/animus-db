//! `LsmEngine::merged_at`'s key-range gate (issue #835): every reader whose
//! own `[min_key, max_key]` cannot overlap the query range is skipped before
//! any block read — the same cheap in-memory gate `may_contain_observed`
//! (point reads), `ranges_overlap` (compaction), and `sstable_overlaps`
//! (`approx_bytes_in_range`) already apply. Two properties:
//!
//! 1. **Correctness**: a scan/scan_at/entries_with_tombstones over a range
//!    that straddles several tables — entirely below, entirely above,
//!    overlapping, straddling `start` exactly, and one whose `max_key`
//!    equals `start` — returns identical results to a single-table baseline
//!    and to `MemoryEngine`.
//! 2. **Cost**: a scan whose range sorts entirely above every on-disk
//!    table's own key range reads **zero** blocks (before the fix in this
//!    issue, it read one block per below-range table — `SsTableReader::
//!    scan_at`'s `block_for_key(start)` resolves to that table's *last*
//!    block for a table wholly below `start`).

use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, MemoryEngine, StorageEngine};
use futures::executor::block_on;

/// Small flush threshold, and a compaction trigger high enough that the
/// individually-flushed tables below are never merged away — each `put`
/// group below lands in its own SSTable with a known, disjoint key range.
fn opts() -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 32,
        compaction_trigger: 1000,
        target_table_bytes: 1 << 20,
        level_fanout: 4,
        wal_segment_bytes: 1 << 16,
        tombstone_grace_versions: 1 << 20,
        trust_monotonic_versions: false,
        background_maintenance: false,
    }
}

fn open(sim: &Simulator, prefix: &str) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with(sim.env(nid(0)), prefix, opts())).expect("open")
}

/// Same threshold but with a huge flush trigger so nothing auto-flushes:
/// every write stays in the memtable, i.e. logically "one table" (a single
/// in-memory source) — the correctness baseline that never exercises the
/// per-reader range gate at all.
fn open_single_table(sim: &Simulator, prefix: &str) -> LsmEngine<SimEnv> {
    let mut o = opts();
    o.flush_threshold_bytes = 1 << 20;
    block_on(LsmEngine::open_with(sim.env(nid(0)), prefix, o)).expect("open")
}

/// Writes six known, disjoint-range groups (flushing each into its own
/// SSTable on `gated`/`single` — `single` never actually flushes, per
/// [`open_single_table`], but the writes land at the same keys/versions
/// either way), then an update and a delete, exercising every case the
/// issue calls out:
///   - `a1,a2` / `b1,b2`: entirely **below** `start` ("m0")
///   - `l5,m0`: **`max_key == start`** exactly
///   - `l0,m2`: **straddles** `start` (`min < start < max`)
///   - `n0,n5`: fully **inside** `[start, end)`
///   - `z1,z2`: entirely **above** `end` ("p0")
///
/// Then `n0` is overwritten (tests `scan_at` at an old version vs. latest)
/// and `m0` is deleted (tests `entries_with_tombstones`/`scan_with_tombstones`
/// retain the tombstone in-range).
async fn run_script(e: &LsmEngine<SimEnv>, flush_each_group: bool) {
    let groups: &[&[(&[u8], &[u8])]] = &[
        &[(b"a1", b"va1"), (b"a2", b"va2")],
        &[(b"b1", b"vb1"), (b"b2", b"vb2")],
        &[(b"l5", b"vl5"), (b"m0", b"vm0")],
        &[(b"l0", b"vl0"), (b"m2", b"vm2")],
        &[(b"n0", b"vn0-orig"), (b"n5", b"vn5")],
        &[(b"z1", b"vz1"), (b"z2", b"vz2")],
    ];
    let mut version = 1u64;
    for group in groups {
        for (k, v) in *group {
            e.put(k, v, version).await.unwrap();
            version += 1;
        }
        if flush_each_group {
            e.flush_now().await.unwrap();
        }
    }
    // version is now 13. Overwrite n0 at v20, delete m0 at v21.
    e.put(b"n0", b"vn0-updated", 20).await.unwrap();
    e.delete(b"m0", 21).await.unwrap();
}

/// The full result triple this test compares across engines: latest `scan`,
/// `scan_at` at a version before the update/delete, and
/// `entries_with_tombstones` restricted to the query range (so the whole-
/// keyspace and range-scoped tombstone paths are both covered — see
/// `scan_with_tombstones`'s own doc on reusing `merged_at`).
async fn results<E: StorageEngine>(
    e: &E,
    start: &[u8],
    end: &[u8],
) -> (
    Vec<(Vec<u8>, Vec<u8>)>,
    Vec<(Vec<u8>, Vec<u8>)>,
    Vec<(Vec<u8>, Option<Vec<u8>>)>,
) {
    let scan = e
        .scan(start, end)
        .await
        .unwrap()
        .into_iter()
        .map(|(k, vv)| (k, vv.value))
        .collect();
    let scan_at_15 = e
        .scan_at(start, end, 15)
        .await
        .unwrap()
        .into_iter()
        .map(|(k, vv)| (k, vv.value))
        .collect();
    let tombstones = e
        .scan_with_tombstones(start, end)
        .await
        .unwrap()
        .into_iter()
        .map(|(k, slot, _v)| (k, slot))
        .collect();
    (scan, scan_at_15, tombstones)
}

/// The gated multi-table `LsmEngine`, a single-table `LsmEngine` baseline,
/// and `MemoryEngine` must all agree on `scan`, `scan_at` (old version), and
/// `scan_with_tombstones` over `[m0, p0)` — a range that straddles every
/// case the issue calls out.
#[test]
fn range_gate_matches_baselines_across_table_layouts() {
    let seed = 0x8351_u64;
    let sim = Simulator::new(seed);
    let start = b"m0".as_slice();
    let end = b"p0".as_slice();

    let gated = open(&sim, "gated/");
    block_on(run_script(&gated, true));
    assert!(
        gated.sstable_views().len() >= 6,
        "seed={seed}: expected each write group to have flushed into its own table, got {}",
        gated.sstable_views().len()
    );

    let single = open_single_table(&sim, "single/");
    block_on(run_script(&single, false));
    assert_eq!(
        single.sstable_views().len(),
        0,
        "seed={seed}: the single-table baseline must never flush"
    );

    let mem = MemoryEngine::new();
    block_on(run_script_mem(&mem));

    let (gated_scan, gated_at, gated_tomb) = block_on(results(&gated, start, end));
    let (single_scan, single_at, single_tomb) = block_on(results(&single, start, end));
    let (mem_scan, mem_at, mem_tomb) = block_on(results(&mem, start, end));

    // Expected, worked out by hand from the script above: m2 (from the
    // straddling table), and n0/n5 (updated n0) are the live keys in range;
    // m0 is a tombstone (excluded from `scan`, present in
    // `scan_with_tombstones`); l5/l0/a*/b*/z* are all out of range.
    assert_eq!(
        gated_scan,
        vec![
            (b"m2".to_vec(), b"vm2".to_vec()),
            (b"n0".to_vec(), b"vn0-updated".to_vec()),
            (b"n5".to_vec(), b"vn5".to_vec()),
        ],
        "seed={seed}"
    );
    assert_eq!(
        gated_at,
        vec![
            (b"m0".to_vec(), b"vm0".to_vec()),
            (b"m2".to_vec(), b"vm2".to_vec()),
            (b"n0".to_vec(), b"vn0-orig".to_vec()),
            (b"n5".to_vec(), b"vn5".to_vec()),
        ],
        "seed={seed}: as-of-v15, before n0's update and m0's delete"
    );
    assert_eq!(
        gated_tomb,
        vec![
            (b"m0".to_vec(), None),
            (b"m2".to_vec(), Some(b"vm2".to_vec())),
            (b"n0".to_vec(), Some(b"vn0-updated".to_vec())),
            (b"n5".to_vec(), Some(b"vn5".to_vec())),
        ],
        "seed={seed}: m0's tombstone survives in-range"
    );

    assert_eq!(gated_scan, single_scan, "seed={seed}: scan vs single-table");
    assert_eq!(gated_at, single_at, "seed={seed}: scan_at vs single-table");
    assert_eq!(
        gated_tomb, single_tomb,
        "seed={seed}: scan_with_tombstones vs single-table"
    );
    assert_eq!(gated_scan, mem_scan, "seed={seed}: scan vs MemoryEngine");
    assert_eq!(gated_at, mem_at, "seed={seed}: scan_at vs MemoryEngine");
    assert_eq!(
        gated_tomb, mem_tomb,
        "seed={seed}: scan_with_tombstones vs MemoryEngine"
    );
}

/// `MemoryEngine` has no `flush_now`/SSTables at all, so this mirrors
/// `run_script`'s writes without the `LsmEngine`-only flush step.
async fn run_script_mem(e: &MemoryEngine) {
    let groups: &[&[(&[u8], &[u8])]] = &[
        &[(b"a1", b"va1"), (b"a2", b"va2")],
        &[(b"b1", b"vb1"), (b"b2", b"vb2")],
        &[(b"l5", b"vl5"), (b"m0", b"vm0")],
        &[(b"l0", b"vl0"), (b"m2", b"vm2")],
        &[(b"n0", b"vn0-orig"), (b"n5", b"vn5")],
        &[(b"z1", b"vz1"), (b"z2", b"vz2")],
    ];
    let mut version = 1u64;
    for group in groups {
        for (k, v) in *group {
            e.put(k, v, version).await.unwrap();
            version += 1;
        }
    }
    e.put(b"n0", b"vn0-updated", 20).await.unwrap();
    e.delete(b"m0", 21).await.unwrap();
}

/// A scan whose range sorts entirely **above** every on-disk table (each
/// flushed group's `max_key < start`) must read **zero** blocks — before
/// this issue's fix, `merged_at` visited every reader unconditionally, and
/// `SsTableReader::scan_at` resolves a below-range table's `block_for_key
/// (start)` to that table's *last* block, fetching (and then discarding)
/// one block per table.
#[test]
fn scan_above_every_table_reads_zero_blocks() {
    let seed = 0x8352_u64;
    let sim = Simulator::new(seed);
    let e = open(&sim, "above/");

    // Five disjoint below-range tables, one per flush.
    let mut version = 1u64;
    block_on(async {
        for group in 0..5u64 {
            let k = format!("g{group:02}-a");
            e.put(k.as_bytes(), b"v", version).await.unwrap();
            version += 1;
            let k2 = format!("g{group:02}-b");
            e.put(k2.as_bytes(), b"v", version).await.unwrap();
            version += 1;
            e.flush_now().await.unwrap();
        }
    });
    assert_eq!(
        e.sstable_views().len(),
        5,
        "seed={seed}: five independent flushed tables"
    );
    let max_written = e
        .sstable_views()
        .iter()
        .filter_map(|t| t.max_key.clone())
        .max()
        .expect("at least one table");
    // A start strictly above every table's own max_key.
    let mut start = max_written.clone();
    start.push(0xFF);

    e.reset_block_reads();
    let rows = block_on(e.scan(&start, b"\xff\xff\xff\xff")).unwrap();
    assert!(
        rows.is_empty(),
        "seed={seed}: nothing lives above every table"
    );
    assert_eq!(
        e.block_read_count(),
        0,
        "seed={seed}: a scan sorting above every table must skip all of them \
         without a block read (issue #835)"
    );
}
