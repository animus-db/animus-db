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
//! cargo bench -p custos-storage                 # default sizes
//! CUSTOS_BENCH_KEYS=200000 cargo bench -p custos-storage   # bigger run
//! ```
//!
//! `cargo bench` builds in release. Override the workload with env vars:
//! `CUSTOS_BENCH_KEYS` (default 3_000 — the on-disk engine fsyncs the WAL on
//! every put, so this is deliberately modest to keep a default run quick; raise
//! it to see flush/compaction kick in), `CUSTOS_BENCH_VALUE_BYTES` (default 64),
//! `CUSTOS_BENCH_GETS` (default 50_000), `CUSTOS_BENCH_SCAN` (default 1_000).
//!
//! [`LsmEngine`]: custos_storage::LsmEngine
//! [`MemoryEngine`]: custos_storage::MemoryEngine

use std::time::{Duration, Instant};

use custos_env::ProdEnv;
use custos_storage::{LsmEngine, LsmOptions, MemoryEngine, StorageEngine};

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
            keys: var("CUSTOS_BENCH_KEYS", 3_000),
            value_bytes: var("CUSTOS_BENCH_VALUE_BYTES", 64) as usize,
            gets: var("CUSTOS_BENCH_GETS", 50_000),
            scan_keys: var("CUSTOS_BENCH_SCAN", 1_000),
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
    let dir = std::env::temp_dir().join(format!("custos-bench-{}", std::process::id()));
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

    // Best-effort cleanup of the temp data dir.
    let _ = std::fs::remove_dir_all(&dir);
}
