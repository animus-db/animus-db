//! Range-scoped byte estimator (ADR 0034: byte-based auto-split trigger).
//!
//! `StorageEngine::approx_bytes_in_range` is exact by default (any backend can
//! rely on it); `LsmEngine` overrides it with a cheap, non-materializing
//! estimate built from its own SSTable/memtable metadata. These tests pin the
//! `LsmEngine` override's behavior directly: known SSTables at known key
//! ranges must produce a sane estimate for a query range, and the estimate's
//! bias must be **over**-estimating, never under-estimating (a real database
//! flushes/compacts as it goes, so an "estimate too low" failure mode would
//! silently under-count a tablet and delay a split it actually needs).

use animus_sim::{SimEnv, Simulator};
use animus_storage::{LsmEngine, LsmOptions, StorageEngine};
use futures::executor::block_on;

const PREFIX: &str = "db/";

fn opts() -> LsmOptions {
    LsmOptions {
        // Small flush threshold so a handful of `put`s each land in their own
        // flushed SSTable (known, disjoint key ranges) rather than all sitting
        // in one memtable.
        flush_threshold_bytes: 48,
        compaction_trigger: 1000, // don't compact away the individual tables
        target_table_bytes: 1 << 20,
        level_fanout: 4,
        wal_segment_bytes: 1 << 16,
        tombstone_grace_versions: 1000,
        trust_monotonic_versions: false,
        background_maintenance: false,
    }
}

fn open(sim: &Simulator) -> LsmEngine<SimEnv> {
    block_on(LsmEngine::open_with(sim.env(0), PREFIX, opts())).expect("open")
}

/// A generous flush threshold so several small `put`s stay in the memtable
/// together until an explicit `flush_now`, landing in **one** SSTable
/// spanning all their keys (rather than each auto-flushing on its own, which
/// the tiny threshold in [`opts`] deliberately triggers for the other test).
fn open_wide_memtable(sim: &Simulator) -> LsmEngine<SimEnv> {
    let mut o = opts();
    o.flush_threshold_bytes = 1 << 20;
    block_on(LsmEngine::open_with(sim.env(0), PREFIX, o)).expect("open")
}

/// The exact byte total (`key.len() + value.len()`) of every `(k, v)` pair
/// with `start <= k < end` (or `k >= start` when `end` is `None`) — the
/// ground truth the estimate is checked against.
fn exact_bytes_in(pairs: &[(&[u8], &[u8])], start: &[u8], end: Option<&[u8]>) -> u64 {
    pairs
        .iter()
        .filter(|(k, _)| *k >= start && end.is_none_or(|e| *k < e))
        .map(|(k, v)| (k.len() + v.len()) as u64)
        .sum()
}

/// A query range that exactly covers one flushed SSTable's key range (each
/// key below flushes on its own, given the tiny `flush_threshold_bytes`)
/// returns an estimate that (a) is never less than the real bytes in range
/// (over-estimate bias, never under) and (b) is a sane, bounded-above
/// estimate — not wildly inflated — since the query range exactly matches
/// the table's own `[min_key, max_key]` (no sibling overlap to inflate it).
#[test]
fn known_sstables_known_range_gives_a_sane_estimate() {
    let seed = 0xB47E_u64;
    let sim = Simulator::new(seed);
    let e = open(&sim);
    let pairs: Vec<(&[u8], &[u8])> = vec![
        (
            b"a",
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
        (
            b"m",
            b"mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm",
        ),
        (
            b"z",
            b"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
        ),
    ];
    block_on(async {
        for (i, (k, v)) in pairs.iter().enumerate() {
            e.put(k, v, (i + 1) as u64).await.unwrap();
            // Force each key to flush into its own SSTable before the next put,
            // so the manifest holds 3 known, disjoint-range tables.
            e.flush_now().await.unwrap();
        }
        assert_eq!(
            e.sstable_views().len(),
            3,
            "each key should have its own flushed table"
        );

        // Query exactly the middle table's own range: [b"m", b"n").
        let start = b"m".to_vec();
        let end = b"n".to_vec();
        let exact = exact_bytes_in(&pairs, &start, Some(&end));
        let estimate = e.approx_bytes_in_range(&start, Some(&end)).await.unwrap();
        assert!(
            estimate >= exact,
            "estimate ({estimate}) must never under-count the real bytes in range ({exact}) — seed {seed}"
        );
        // A tight upper bound: the estimate must not include every table's
        // bytes (i.e. it must be scoped, not a whole-engine sum).
        let whole_engine: u64 = e.sstable_views().iter().map(|t| t.file_size).sum();
        assert!(
            estimate < whole_engine,
            "a range covering only one of three disjoint tables must not report the whole engine's bytes (estimate {estimate}, whole engine {whole_engine}) — seed {seed}"
        );
    });
}

/// An **unbounded-above** query range (`end: None`) includes every table at
/// or after `start` — the estimate must still never under-count.
#[test]
fn unbounded_above_range_includes_everything_from_start() {
    let seed = 0x0A11_u64;
    let sim = Simulator::new(seed);
    let e = open(&sim);
    let pairs: Vec<(&[u8], &[u8])> = vec![
        (b"a", b"short"),
        (b"m", b"a much longer value that is definitely bigger"),
        (b"z", b"final"),
    ];
    block_on(async {
        for (i, (k, v)) in pairs.iter().enumerate() {
            e.put(k, v, (i + 1) as u64).await.unwrap();
            e.flush_now().await.unwrap();
        }
        let start = b"m".to_vec();
        let exact = exact_bytes_in(&pairs, &start, None);
        let estimate = e.approx_bytes_in_range(&start, None).await.unwrap();
        assert!(
            estimate >= exact,
            "unbounded-above estimate ({estimate}) must never under-count ({exact}) — seed {seed}"
        );
    });
}

/// A range that overlaps a table only **partially** still counts that
/// table's **whole** `file_size` — the documented over-estimate bias, proven
/// directly: shrinking the query range to barely clip a table's edge must
/// not shrink the estimate below that table's full size.
#[test]
fn partial_overlap_counts_the_whole_table_over_estimate_bias() {
    let seed = 0x0FF5E7_u64;
    let sim = Simulator::new(seed);
    let e = open_wide_memtable(&sim);
    block_on(async {
        // One table spanning keys "b".."f" (multiple puts before a single flush,
        // so they land in one SSTable together).
        let entries: [(&[u8], &[u8]); 2] = [
            (b"b", b"vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"),
            (b"f", b"wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww"),
        ];
        for (i, (k, v)) in entries.iter().enumerate() {
            e.put(k, v, (i + 1) as u64).await.unwrap();
        }
        e.flush_now().await.unwrap();
        assert_eq!(e.sstable_views().len(), 1);
        let file_size = e.sstable_views()[0].file_size;

        // Query range [c, d) falls strictly *inside* [b, f] — partial overlap,
        // no full containment either way.
        let estimate = e.approx_bytes_in_range(b"c", Some(b"d")).await.unwrap();
        assert_eq!(
            estimate, file_size,
            "a partially-overlapping table must count its whole file_size (over-estimate bias) — seed {seed}"
        );

        // A range that doesn't overlap the table at all contributes nothing.
        let miss = e.approx_bytes_in_range(b"x", Some(b"y")).await.unwrap();
        assert_eq!(
            miss, 0,
            "a non-overlapping range must not count the table — seed {seed}"
        );
    });
}

/// The `MemoryEngine`/default-trait-impl path is **exact**, not an estimate —
/// this is what every non-`LsmEngine` backend gets for free.
#[test]
fn default_trait_impl_is_exact_on_memory_engine() {
    use animus_storage::MemoryEngine;
    let e = MemoryEngine::new();
    block_on(async {
        e.put(b"a", b"1", 1).await.unwrap();
        e.put(b"m", b"22", 2).await.unwrap();
        e.put(b"z", b"333", 3).await.unwrap();
        let estimate = e.approx_bytes_in_range(b"m", None).await.unwrap();
        // Exactly "m"+"22" (1+2=3) + "z"+"333" (1+3=4) = 7.
        assert_eq!(estimate, 7);
    });
}
