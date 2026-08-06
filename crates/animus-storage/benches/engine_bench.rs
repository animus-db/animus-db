//! Hand-rolled throughput / latency benchmark for the storage engines over
//! **`ProdEnv`** (real wall clock + real `tokio::fs` I/O), comparing the on-disk
//! [`LsmEngine`] against the in-memory [`MemoryEngine`].
//!
//! This is a plain binary (`harness = false`), not a `#[bench]`/criterion target,
//! so it pulls in **no extra dependencies** — timing is `std::time::Instant` and
//! statistics are computed by hand. It is intentionally a rough, reproducible
//! macro-benchmark (single-threaded driver, fixed workload) for tracking the
//! relative cost of the LSM's WAL/flush/compaction against the memtable-only
//! baseline, not a microbenchmark with statistical rigor.
//!
//! ## Running
//!
//! ```sh
//! cargo bench -p animus-storage                 # default sizes
//! ANIMUS_BENCH_KEYS=200000 cargo bench -p animus-storage   # bigger run
//! ```
//!
//! `cargo bench` builds in release. Override the workload with env vars:
//! `ANIMUS_BENCH_KEYS` (default 3_000 — the on-disk engine fsyncs the WAL on
//! every put, so this is deliberately modest to keep a default run quick; raise
//! it to see flush/compaction kick in), `ANIMUS_BENCH_VALUE_BYTES` (default 64),
//! `ANIMUS_BENCH_GETS` (default 50_000), `ANIMUS_BENCH_SCAN` (default 1_000).
//!
//! [`LsmEngine`]: animus_storage::LsmEngine
//! [`MemoryEngine`]: animus_storage::MemoryEngine

use std::time::{Duration, Instant};

use animus_env::ProdEnv;
use animus_storage::{LsmEngine, LsmOptions, MemoryEngine, MergeOp, StorageEngine};

/// Workload parameters, read from the environment with defaults.
struct Config {
    keys: u64,
    value_bytes: usize,
    gets: u64,
    scan_keys: u64,
}

impl Config {
    fn from_env() -> Self {
        let var = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        Self {
            keys: var("ANIMUS_BENCH_KEYS", 3_000),
            value_bytes: var("ANIMUS_BENCH_VALUE_BYTES", 64) as usize,
            gets: var("ANIMUS_BENCH_GETS", 50_000),
            scan_keys: var("ANIMUS_BENCH_SCAN", 1_000),
        }
    }
}

/// A simple deterministic key for index `i`: zero-padded so keys sort in numeric
/// order (and a point read can find a specific one).
fn key_for(i: u64) -> Vec<u8> {
    format!("key-{i:012}").into_bytes()
}

/// A deterministic value of `n` bytes derived from `i` (cheap, no allocation
/// churn beyond the vec itself).
fn value_for(i: u64, n: usize) -> Vec<u8> {
    let mut v = vec![0u8; n];
    let seed = i.to_le_bytes();
    for (j, b) in v.iter_mut().enumerate() {
        *b = seed[j % 8].wrapping_add(j as u8);
    }
    v
}

/// A small linear-congruential PRNG so the `get` workload probes keys in a
/// pseudo-random order (defeating any sequential-access advantage) without an
/// `rand` dependency. Deterministic for a fixed seed.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.0 >> 11
    }
}

/// Latency stats over a sorted slice of nanosecond samples.
struct Stats {
    count: u64,
    total: Duration,
    p50: Duration,
    p99: Duration,
    max: Duration,
}

impl Stats {
    fn from_sorted(samples: &[Duration]) -> Self {
        let count = samples.len() as u64;
        let total: Duration = samples.iter().sum();
        let pct = |p: f64| {
            if samples.is_empty() {
                Duration::ZERO
            } else {
                let idx = ((p * samples.len() as f64) as usize).min(samples.len() - 1);
                samples[idx]
            }
        };
        Self {
            count,
            total,
            p50: pct(0.50),
            p99: pct(0.99),
            max: samples.last().copied().unwrap_or(Duration::ZERO),
        }
    }

    fn throughput_per_sec(&self) -> f64 {
        if self.total.is_zero() {
            0.0
        } else {
            self.count as f64 / self.total.as_secs_f64()
        }
    }
}

/// Print a labelled result line.
fn report(label: &str, stats: &Stats) {
    println!(
        "  {label:<28} {:>10.0} ops/s   p50 {:>8.1}us  p99 {:>8.1}us  max {:>8.1}us",
        stats.throughput_per_sec(),
        stats.p50.as_nanos() as f64 / 1000.0,
        stats.p99.as_nanos() as f64 / 1000.0,
        stats.max.as_nanos() as f64 / 1000.0,
    );
}

/// Run the put / get / scan workload against `engine`, returning the three
/// stat blocks. `engine` starts empty.
async fn run_workload<E: StorageEngine>(engine: &E, cfg: &Config) -> (Stats, Stats, Stats) {
    // ---- puts ----
    let mut put_samples = Vec::with_capacity(cfg.keys as usize);
    for i in 0..cfg.keys {
        let k = key_for(i);
        let v = value_for(i, cfg.value_bytes);
        let t = Instant::now();
        engine.put(&k, &v, i + 1).await.expect("put");
        put_samples.push(t.elapsed());
    }
    put_samples.sort_unstable();

    // ---- gets (pseudo-random key order) ----
    let mut get_samples = Vec::with_capacity(cfg.gets as usize);
    let mut rng = Lcg(0x9e37_79b9_7f4a_7c15);
    for _ in 0..cfg.gets {
        let i = rng.next() % cfg.keys;
        let k = key_for(i);
        let t = Instant::now();
        let got = engine.get(&k).await.expect("get");
        get_samples.push(t.elapsed());
        debug_assert!(got.is_some());
    }
    get_samples.sort_unstable();

    // ---- scans of a contiguous window ----
    let mut scan_samples = Vec::new();
    let window = cfg.scan_keys.min(cfg.keys);
    let iters = (cfg.keys / window.max(1)).clamp(1, 200);
    for s in 0..iters {
        let start = key_for(s * window);
        let end = key_for(s * window + window);
        let t = Instant::now();
        let rows = engine.scan(&start, &end).await.expect("scan");
        scan_samples.push(t.elapsed());
        debug_assert!(!rows.is_empty());
    }
    scan_samples.sort_unstable();

    (
        Stats::from_sorted(&put_samples),
        Stats::from_sorted(&get_samples),
        Stats::from_sorted(&scan_samples),
    )
}

/// Aggregate write throughput with `writers` tasks issuing writes **concurrently**
/// against one shared engine. This is the workload WAL group commit targets: many
/// in-flight writes coalescing their `fsync`s. Returns (writes/s, elapsed).
///
/// Uses `merge` (per-key last-writer-wins), the data-plane's actual write
/// primitive — unlike `put` it does not enforce the engine-wide monotonic floor,
/// so concurrent writers on disjoint keys never collide on the version contract.
/// Each writer owns a disjoint key partition, so every merge applies.
async fn concurrent_put_throughput<E>(
    engine: &E,
    total: u64,
    value_bytes: usize,
    writers: u64,
) -> (f64, Duration)
where
    E: StorageEngine + 'static,
{
    let per = total / writers;
    let start = Instant::now();
    let mut handles = Vec::with_capacity(writers as usize);
    for w in 0..writers {
        let engine = engine.clone();
        handles.push(tokio::spawn(async move {
            for j in 0..per {
                // Disjoint key per (writer, j); a strictly increasing per-key
                // version (always 1 here, since each key is written once).
                let idx = j * writers + w;
                let k = key_for(idx);
                let v = value_for(idx, value_bytes);
                engine
                    .merge(&k, &v, idx + 1)
                    .await
                    .expect("concurrent merge");
            }
        }));
    }
    for h in handles {
        h.await.expect("writer task");
    }
    let elapsed = start.elapsed();
    let done = per * writers;
    let tput = if elapsed.is_zero() {
        0.0
    } else {
        done as f64 / elapsed.as_secs_f64()
    };
    (tput, elapsed)
}

/// The leaderful-Raft **apply-path** write pattern: a single task applies a run of
/// committed commands sequentially. Two variants, on fresh engines:
///
/// - `per_op`: one `merge` per command (one WAL `fsync` each — the old behavior);
/// - `batched`: coalesce each run of `batch` commands into one `merge_batch` (one
///   `fsync` per run — the fix).
///
/// Reports puts/s and the batch-fsync count so the coalescing is visible.
async fn apply_path_throughput(
    dir_tag: &str,
    addr: std::net::SocketAddr,
    opts: LsmOptions,
    total: u64,
    value_bytes: usize,
    batch: u64,
    batched: bool,
) -> (f64, Duration, u64) {
    let dir = std::env::temp_dir().join(format!(
        "animus-bench-apply-{}-{dir_tag}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let (env, _b) = ProdEnv::bind(0, addr, &dir).await.expect("bind ProdEnv");
    let lsm = LsmEngine::open_with(env, "db-", opts).await.expect("open");
    let start = Instant::now();
    if batched {
        let mut i = 0u64;
        while i < total {
            let n = batch.min(total - i);
            let ops: Vec<MergeOp> = (i..i + n)
                .map(|j| MergeOp::put(key_for(j), value_for(j, value_bytes), j + 1))
                .collect();
            lsm.merge_batch(ops).await.expect("merge_batch");
            i += n;
        }
    } else {
        for j in 0..total {
            lsm.merge(&key_for(j), &value_for(j, value_bytes), j + 1)
                .await
                .expect("merge");
        }
    }
    let elapsed = start.elapsed();
    let tput = if elapsed.is_zero() {
        0.0
    } else {
        total as f64 / elapsed.as_secs_f64()
    };
    let syncs = lsm.wal_batch_sync_count();
    let _ = std::fs::remove_dir_all(&dir);
    (tput, elapsed, syncs)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cfg = Config::from_env();
    println!(
        "storage engine benchmark (ProdEnv): keys={}, value_bytes={}, gets={}, scan_window={}",
        cfg.keys, cfg.value_bytes, cfg.gets, cfg.scan_keys
    );

    // ---- MemoryEngine baseline ----
    println!("\nMemoryEngine (in-memory baseline):");
    let mem = MemoryEngine::new();
    let (put, get, scan) = run_workload(&mem, &cfg).await;
    report("put", &put);
    report("get", &get);
    report("scan", &scan);

    // ---- LsmEngine over ProdEnv (real disk I/O) ----
    println!("\nLsmEngine (on-disk, ProdEnv):");
    let dir = std::env::temp_dir().join(format!("animus-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addr = "127.0.0.1:0".parse().expect("addr");
    let (env, _bound) = ProdEnv::bind(0, addr, &dir).await.expect("bind ProdEnv");
    // Modest knobs so the default workload actually exercises flush + leveled
    // compaction (raise these for a production-sized memtable).
    let opts = LsmOptions {
        flush_threshold_bytes: 48 * 1024,
        compaction_trigger: 4,
        target_table_bytes: 128 * 1024,
        level_fanout: 8,
        // Roll the WAL near the flush threshold so a flush typically GCs a segment.
        wal_segment_bytes: 48 * 1024,
        tombstone_grace_versions: 1 << 20,
    };
    let lsm = LsmEngine::open_with(env, "db-", opts)
        .await
        .expect("open lsm");
    let compact_start = Instant::now();
    let (put, get, scan) = run_workload(&lsm, &cfg).await;
    let total = compact_start.elapsed();
    report("put", &put);
    report("get", &get);
    report("scan", &scan);
    println!(
        "  {:<28} flushes={}  compactions={}  live_sstables={}  total_wall={:.2}s",
        "lsm internals",
        lsm.flush_count(),
        lsm.compaction_count(),
        lsm.sstable_count(),
        total.as_secs_f64(),
    );

    // ---- Concurrent-write throughput (WAL group commit) ----
    // The sequential `put` loop above coalesces nothing (one in-flight write at a
    // time). Group commit pays off when writes are concurrent: this measures
    // aggregate put throughput as the writer count grows, on a fresh engine each
    // time. The per-batch fsync count is reported so the coalescing is visible.
    let writers_set: Vec<u64> = std::env::var("ANIMUS_BENCH_WRITERS")
        .ok()
        .map(|s| s.split(',').filter_map(|p| p.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![1, 8, 32, 128]);
    let conc_keys = cfg.keys;
    println!("\nLsmEngine concurrent put throughput (group commit):");
    for &writers in &writers_set {
        let cdir = std::env::temp_dir().join(format!(
            "animus-bench-conc-{}-{writers}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&cdir);
        let (cenv, _cb) = ProdEnv::bind(0, addr, &cdir).await.expect("bind ProdEnv");
        let clsm = LsmEngine::open_with(cenv, "db-", opts).await.expect("open");
        let (tput, elapsed) =
            concurrent_put_throughput(&clsm, conc_keys, cfg.value_bytes, writers).await;
        println!(
            "  writers={writers:<4} {tput:>10.0} puts/s   batch_fsyncs={:<6} ({:.2}s for {conc_keys} puts)",
            clsm.wal_batch_sync_count(),
            elapsed.as_secs_f64(),
        );
        let _ = std::fs::remove_dir_all(&cdir);
    }

    // ---- Sequential apply-path throughput (merge vs merge_batch) ----
    // The CP-data Raft apply loop applies a run of committed commands from ONE
    // task. Per-op `merge` pays a full fsync each; `merge_batch` coalesces a run
    // into a single fsync. This is the write-stall fix's target.
    let batch = std::env::var("ANIMUS_BENCH_APPLY_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30u64);
    let apply_total = cfg.keys;
    println!("\nLsmEngine sequential apply-path (batch={batch}):");
    let (t0, e0, s0) = apply_path_throughput(
        "perop",
        addr,
        opts,
        apply_total,
        cfg.value_bytes,
        batch,
        false,
    )
    .await;
    println!(
        "  per-op merge      {t0:>10.0} puts/s   batch_fsyncs={s0:<6} ({:.2}s for {apply_total})",
        e0.as_secs_f64()
    );
    let (t1, e1, s1) = apply_path_throughput(
        "batched",
        addr,
        opts,
        apply_total,
        cfg.value_bytes,
        batch,
        true,
    )
    .await;
    println!(
        "  merge_batch       {t1:>10.0} puts/s   batch_fsyncs={s1:<6} ({:.2}s for {apply_total})",
        e1.as_secs_f64()
    );
    println!(
        "  speedup           {:.1}x",
        if t0 > 0.0 { t1 / t0 } else { 0.0 }
    );

    // Best-effort cleanup of the temp data dir.
    let _ = std::fs::remove_dir_all(&dir);
}
