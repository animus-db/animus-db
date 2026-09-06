//! `ProdEnv` wall-clock benchmark gating whether `SharedWal` (built,
//! unwired — `animus_control::shared_wal`, ADR 0028) is worth wiring into
//! `animus-cp-data`'s persist path (`docs/roadmap.md` C-05 PR 1; the design
//! note this bench's own numbers feed is
//! `docs/design/shared-wal-fsync-benchmark.md`).
//!
//! ## The question
//!
//! Today every hosted tablet group's own consensus loop persists to its
//! **own** WAL file (`crate::lib.rs`'s `persist_wal`, one `env.append` +
//! `env.sync` per group per round — see that function's doc). A prior
//! `SimEnv`-only measurement (throwaway harness, 2026-09-02, recorded in
//! `docs/roadmap.md`'s C-05 entry) found a burst of one write to each of K
//! active groups on one node costs K `Disk::sync` calls, one per group's own
//! file, with no cross-group coalescing. `SharedWal` (`animus-control::
//! shared_wal`) exists to fix exactly this: route every hosted group's WAL
//! I/O through one shared coordinator, so K groups' concurrent bursts
//! coalesce into a single physical `append`+`sync`.
//!
//! `SimEnv`'s virtual clock proves the **fsync-count** shape (this bench
//! reproduces that count too, instrumented directly rather than assumed —
//! see `CountedEnv` below) but says nothing about **wall-clock cost**: on
//! some media, K concurrent fsyncs to K different files may already be
//! cheap (parallel, batched by the device/filesystem), in which case
//! `SharedWal`'s coalescing buys little latency even though it cuts the
//! syscall count. This bench measures wall-clock **round latency**
//! (percentiles, not just a mean — `website/performance.html`'s "tail
//! latency, not averages" commitment applies to internal benches too) over
//! real disk I/O (`ProdEnv`, real `tokio::fs`/`fsync`), so the gating
//! decision in the design note rests on a real number, not an assumption.
//!
//! ## The three shapes measured, per `K` in `ANIMUS_BENCH_GROUPS`
//!
//! A "round" is a burst of one write to each of `K` **active groups** on one
//! node — the exact shape the roadmap names, and the shape a split, a
//! failover, or an ordinary write burst across several co-hosted tablets
//! produces today.
//!
//! - **(a) per-group files, concurrent** — `K` distinct files, `K` tasks
//!   spawned concurrently, each doing one `append` + `sync` on its own file.
//!   This is the faithful reproduction of today's real behavior: each
//!   hosted `RaftKvNode`'s consensus loop is an independent `tokio` task
//!   with its own `wal_lock`, so `K` groups' own `persist_wal` calls run
//!   concurrently with respect to each other (nothing serializes them
//!   against one another — only each group's own apply-task compaction
//!   rewrite shares that group's `wal_lock`). This is the number
//!   `SharedWal` wiring would replace.
//! - **(a′) per-group files, sequential** — the identical `K` files, but one
//!   `append`+`sync` after another with no concurrency at all: a cheap
//!   worst-case reference point ("measure both if cheap" per the roadmap),
//!   not a claim about production behavior.
//! - **(b) `SharedWal`, one file, K groups** — `K` tasks concurrently call
//!   `animus_control::shared_wal::SharedWal::append` against ONE shared
//!   file, using the real, already-built-but-unwired API directly (no
//!   reimplementation). `SharedWal`'s own queue+leader-flush algorithm
//!   (`shared_wal.rs`'s module doc) coalesces whatever is queued when the
//!   flushing "leader" task runs into a single `append`+`sync` — this is
//!   the number wiring `SharedWal` in would actually buy.
//!
//! Plus one **fixed control**, independent of the `K` sweep:
//!
//! - **(c) single-group 32-write burst** — the per-group group-commit
//!   baseline the roadmap names: 32 concurrent writers against **one**
//!   file, coalesced the same way `persist_round.rs`'s own
//!   `drain_for_round` already coalesces concurrent proposals against a
//!   SINGLE group's WAL in production today (queue everything pending under
//!   one lock, one leader flushes the batch as one `append`+`sync`).
//!   `SharedWal` is reused here too, deliberately: it implements the
//!   identical queue+leader-flush shape `persist_round.rs` does (an async
//!   mutex-guarded queue, one flushing task per round — see both modules'
//!   own docs), and standing up a full `RaftKvNode` purely to reproduce
//!   that same coalescing would test nothing this reuse doesn't already
//!   prove. The point of this control is to show that **within-group**
//!   batching is not the gap — `persist_round.rs` already gets a K=32
//!   burst on ONE group down to ~1 fsync today, with no `SharedWal`
//!   involved in production at all. What (a) vs (b) above measures is the
//!   **cross-group** gap this control does NOT cover.
//!
//! Every scenario is instrumented with a real fsync counter
//! (`CountedEnv`, below) rather than assumed from the code shape — a
//! concurrent burst racing `SharedWal`'s internal queue can in principle
//! still split into more than one round if not every submitter has
//! enqueued before the current "leader" starts flushing (see
//! `shared_wal.rs`'s own `overlapping_appends_are_coalesced_into_one_
//! physical_write` test for the analogous `SimEnv` proof), so the reported
//! fsyncs-per-round for (b)/(c) is a real, honest measurement, not a
//! restatement of the API's contract.
//!
//! ## Media
//!
//! `fsync`'s real cost is entirely a function of the underlying storage —
//! a `tmpfs`/`overlay` mount can make it nearly free, while a real block
//! device pays a real cost per call. This bench reads `/proc/mounts` for
//! whatever directory it writes into and prints the matching filesystem
//! type and device, so a reader of its output knows which case they're
//! looking at without re-deriving it. **Do not compare a number from this
//! bench against a number from a different host/session/media** — the
//! same rule `docs/engineering-lessons.md`'s "a historical bench figure
//! from a different host is not a baseline" entry states for every other
//! bench in this repo.
//!
//! ## Running
//!
//! ```sh
//! cargo bench -p animus-cp-data --bench wal_fsync_bench          # default
//! ANIMUS_BENCH_GROUPS=1,4,16,64 cargo bench -p animus-cp-data --bench wal_fsync_bench
//! ANIMUS_BENCH_JSON=/tmp/wal_fsync.json cargo bench -p animus-cp-data --bench wal_fsync_bench
//! ```
//!
//! Env knobs: `ANIMUS_BENCH_GROUPS` (default `1,8,32,128` — the `K` values
//! swept for scenarios (a)/(a′)/(b)), `ANIMUS_BENCH_ROUNDS` (default `20` —
//! bursts measured per scenario; keep the default run comfortably under a
//! minute on real disk), `ANIMUS_BENCH_VALUE_BYTES` (default `96` — payload
//! size per write, a rough single Raft log entry), and `ANIMUS_BENCH_JSON`
//! (unset — a file path to also write results as JSON, mirroring
//! `animus-storage`'s `engine_bench`/`animusd`'s `cluster_bench`).
//!
//! This bench is manual/local, like its two siblings above — real disk I/O
//! and real elapsed wall clock make it unsuitable for a shared CI runner's
//! noise floor. Run it locally when the gating question needs a fresh
//! number for a maintainer's own hardware; it needs no other setup than a
//! writable scratch directory.

// ADR 0003 / ADR 0061 Decision 4 (rung B5): a ProdEnv wall-clock macro-bench
// (see the module doc above) — timing real elapsed time against a real disk
// is this file's entire job, not system logic under the Env seam. Mirrors
// `animus-storage/benches/engine_bench.rs`'s identical top-of-file allow.
#![allow(
    clippy::disallowed_methods,
    reason = "ProdEnv wall-clock macro-benchmark over a real disk (see module doc); ADR 0061 Decision 4"
)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use animus_control::SharedWal;
#[cfg(test)]
use animus_env::nid;
use animus_env::{
    BoxFuture, Clock, Disk, Env, Envelope, MetricsHandle, Nanos, Network, NodeId, ProdEnv, Rng,
    Spawner, UnixMillis,
};

// ---------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------

struct Config {
    groups: Vec<u64>,
    rounds: u64,
    value_bytes: usize,
    json_path: Option<PathBuf>,
}

impl Config {
    fn from_env() -> Self {
        let groups = std::env::var("ANIMUS_BENCH_GROUPS")
            .ok()
            .map(|s| {
                s.split(',')
                    .filter_map(|p| p.trim().parse::<u64>().ok())
                    .collect::<Vec<u64>>()
            })
            .filter(|v: &Vec<u64>| !v.is_empty())
            .unwrap_or_else(|| vec![1, 8, 32, 128]);
        let var_u64 = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        Self {
            groups,
            rounds: var_u64("ANIMUS_BENCH_ROUNDS", 20),
            value_bytes: var_u64("ANIMUS_BENCH_VALUE_BYTES", 96) as usize,
            json_path: std::env::var("ANIMUS_BENCH_JSON").ok().map(PathBuf::from),
        }
    }
}

/// A deterministic `n`-byte payload for round `r`, group `g` — content
/// doesn't matter here (unlike `PersistedState`'s real WAL records, this
/// bench never replays what it writes), only that it's a realistic,
/// non-trivially-compressible size.
fn payload_for(r: u64, g: u64, n: usize) -> Vec<u8> {
    let mut v = vec![0u8; n];
    let seed = (r.wrapping_mul(0x9E37_79B9).wrapping_add(g)).to_le_bytes();
    for (i, b) in v.iter_mut().enumerate() {
        *b = seed[i % 8].wrapping_add(i as u8);
    }
    v
}

// ---------------------------------------------------------------------
// CountedEnv — a thin `Env` wrapper that counts real `Disk::sync` calls
// ---------------------------------------------------------------------

/// Wraps a `ProdEnv` and counts every real `Disk::sync` call that goes
/// through it, so this bench can report **measured** fsyncs-per-round
/// rather than a count merely assumed from the code shape (see the module
/// doc's note on why this matters for the `SharedWal` scenarios — a
/// concurrent burst can in principle still split into more than one
/// physical round). Every other `Env` method is a plain pass-through to
/// the wrapped `ProdEnv` — this is not a fake, it does real I/O.
#[derive(Clone)]
struct CountedEnv {
    inner: ProdEnv,
    syncs: Arc<AtomicU64>,
}

impl CountedEnv {
    fn new(inner: ProdEnv) -> Self {
        Self {
            inner,
            syncs: Arc::new(AtomicU64::new(0)),
        }
    }

    fn sync_count(&self) -> u64 {
        self.syncs.load(Ordering::SeqCst)
    }

    fn reset(&self) {
        self.syncs.store(0, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl Clock for CountedEnv {
    fn now(&self) -> Nanos {
        self.inner.now()
    }
    fn wall_now(&self) -> UnixMillis {
        self.inner.wall_now()
    }
    async fn sleep(&self, dur: Duration) {
        self.inner.sleep(dur).await
    }
}

impl Rng for CountedEnv {
    fn next_u64(&self) -> u64 {
        self.inner.next_u64()
    }
    fn fill_bytes(&self, dst: &mut [u8]) {
        self.inner.fill_bytes(dst)
    }
}

#[async_trait::async_trait]
impl Network for CountedEnv {
    async fn send_stream(&self, to: NodeId, stream: u64, payload: Vec<u8>) {
        self.inner.send_stream(to, stream, payload).await
    }
    async fn recv_stream(&self, stream: u64) -> Envelope {
        self.inner.recv_stream(stream).await
    }
}

#[async_trait::async_trait]
impl Disk for CountedEnv {
    async fn append(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.append(file, bytes).await
    }
    async fn sync(&self, file: &str) -> std::io::Result<()> {
        self.syncs.fetch_add(1, Ordering::SeqCst);
        self.inner.sync(file).await
    }
    async fn read(&self, file: &str) -> std::io::Result<Vec<u8>> {
        self.inner.read(file).await
    }
    async fn read_at(&self, file: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        self.inner.read_at(file, offset, len).await
    }
    async fn size(&self, file: &str) -> std::io::Result<u64> {
        self.inner.size(file).await
    }
    async fn remove(&self, file: &str) -> std::io::Result<()> {
        self.inner.remove(file).await
    }
    async fn replace(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.replace(file, bytes).await
    }
    async fn list(&self) -> std::io::Result<Vec<String>> {
        self.inner.list().await
    }
    async fn link(&self, src: &str, dst: &str) -> std::io::Result<()> {
        self.inner.link(src, dst).await
    }
}

impl Spawner for CountedEnv {
    fn spawn(&self, fut: BoxFuture<'static, ()>) {
        self.inner.spawn(fut)
    }
}

impl Env for CountedEnv {
    fn node_id(&self) -> NodeId {
        self.inner.node_id()
    }
    fn metrics(&self) -> MetricsHandle {
        self.inner.metrics()
    }
}

// ---------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------

/// Round-latency percentiles, computed by hand — the same shape
/// `animus-storage`'s `engine_bench`/`animusd`'s `cluster_bench` use.
struct Stats {
    count: u64,
    mean: Duration,
    p50: Duration,
    p99: Duration,
    max: Duration,
    mean_fsyncs: f64,
}

impl Stats {
    fn from_samples(mut latencies: Vec<Duration>, fsyncs: &[u64]) -> Self {
        latencies.sort_unstable();
        let count = latencies.len() as u64;
        let total: Duration = latencies.iter().sum();
        let mean = if count > 0 {
            total / count as u32
        } else {
            Duration::ZERO
        };
        let pct = |p: f64| {
            if latencies.is_empty() {
                Duration::ZERO
            } else {
                let idx = ((p * latencies.len() as f64) as usize).min(latencies.len() - 1);
                latencies[idx]
            }
        };
        let mean_fsyncs = if fsyncs.is_empty() {
            0.0
        } else {
            fsyncs.iter().sum::<u64>() as f64 / fsyncs.len() as f64
        };
        Self {
            count,
            mean,
            p50: pct(0.50),
            p99: pct(0.99),
            max: latencies.last().copied().unwrap_or(Duration::ZERO),
            mean_fsyncs,
        }
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "count": self.count,
            "mean_us": self.mean.as_secs_f64() * 1_000_000.0,
            "p50_us": self.p50.as_secs_f64() * 1_000_000.0,
            "p99_us": self.p99.as_secs_f64() * 1_000_000.0,
            "max_us": self.max.as_secs_f64() * 1_000_000.0,
            "mean_fsyncs_per_round": self.mean_fsyncs,
        })
    }
}

fn report(label: &str, k: Option<u64>, stats: &Stats) {
    let k_col = k
        .map(|k| format!("K={k:<4}"))
        .unwrap_or_else(|| "     ".into());
    println!(
        "  {label:<30} {k_col} p50 {:>8.1}us  p99 {:>8.1}us  max {:>9.1}us  fsyncs/round {:>6.2}  n={}",
        stats.p50.as_secs_f64() * 1_000_000.0,
        stats.p99.as_secs_f64() * 1_000_000.0,
        stats.max.as_secs_f64() * 1_000_000.0,
        stats.mean_fsyncs,
        stats.count,
    );
}

// ---------------------------------------------------------------------
// Scenario (a)/(a'): per-group files
// ---------------------------------------------------------------------

/// One round of scenario (a)/(a'): a burst of one write to each of `k`
/// distinct files (one per "active group"), either concurrently (spawned
/// tasks) or sequentially (a plain loop) — see the module doc for why
/// concurrent is the faithful reproduction of today's real behavior.
async fn per_group_round(
    env: &CountedEnv,
    dir_tag: &str,
    round: u64,
    k: u64,
    value_bytes: usize,
    concurrent: bool,
) -> (Duration, u64) {
    env.reset();
    let start = Instant::now();
    if concurrent {
        let mut handles = Vec::with_capacity(k as usize);
        for g in 0..k {
            let env = env.clone();
            let file = format!("{dir_tag}-group-{g}.wal");
            let bytes = payload_for(round, g, value_bytes);
            handles.push(tokio::spawn(async move {
                env.append(&file, &bytes).await.expect("append");
                env.sync(&file).await.expect("sync");
            }));
        }
        for h in handles {
            h.await.expect("join per-group task");
        }
    } else {
        for g in 0..k {
            let file = format!("{dir_tag}-group-{g}.wal");
            let bytes = payload_for(round, g, value_bytes);
            env.append(&file, &bytes).await.expect("append");
            env.sync(&file).await.expect("sync");
        }
    }
    (start.elapsed(), env.sync_count())
}

async fn run_per_group(
    env: &CountedEnv,
    dir_tag: &str,
    k: u64,
    rounds: u64,
    value_bytes: usize,
    concurrent: bool,
) -> Stats {
    let mut latencies = Vec::with_capacity(rounds as usize);
    let mut fsyncs = Vec::with_capacity(rounds as usize);
    for r in 0..rounds {
        let (elapsed, synced) = per_group_round(env, dir_tag, r, k, value_bytes, concurrent).await;
        latencies.push(elapsed);
        fsyncs.push(synced);
    }
    Stats::from_samples(latencies, &fsyncs)
}

// ---------------------------------------------------------------------
// Scenario (b)/(c): SharedWal
// ---------------------------------------------------------------------

/// One round: `writers` concurrent `SharedWal::append` calls against ONE
/// shared file. Used both for (b) (`writers = k`, the cross-group
/// coalescing case) and (c) (`writers = 32` fixed, the single-group
/// group-commit control — see the module doc).
async fn shared_wal_round(
    env: &CountedEnv,
    wal: &Arc<SharedWal>,
    file: &str,
    round: u64,
    writers: u64,
    value_bytes: usize,
) -> (Duration, u64) {
    env.reset();
    let start = Instant::now();
    let mut handles = Vec::with_capacity(writers as usize);
    for w in 0..writers {
        let env = env.clone();
        let wal = Arc::clone(wal);
        let file = file.to_string();
        let bytes = payload_for(round, w, value_bytes);
        handles.push(tokio::spawn(async move {
            wal.append(&env, &file, bytes).await.expect("shared append");
        }));
    }
    for h in handles {
        h.await.expect("join shared-wal task");
    }
    (start.elapsed(), env.sync_count())
}

async fn run_shared_wal(
    env: &CountedEnv,
    file: &str,
    writers: u64,
    rounds: u64,
    value_bytes: usize,
) -> Stats {
    let wal = Arc::new(SharedWal::new());
    let mut latencies = Vec::with_capacity(rounds as usize);
    let mut fsyncs = Vec::with_capacity(rounds as usize);
    for r in 0..rounds {
        let (elapsed, synced) = shared_wal_round(env, &wal, file, r, writers, value_bytes).await;
        latencies.push(elapsed);
        fsyncs.push(synced);
    }
    Stats::from_samples(latencies, &fsyncs)
}

// ---------------------------------------------------------------------
// Media detection
// ---------------------------------------------------------------------

/// Reads `/proc/mounts` and finds the mount entry whose path is the
/// longest prefix of `dir` (the standard "which mount owns this path"
/// resolution) — reports its filesystem type and source device, so a
/// reader of this bench's output knows whether `fsync` here is hitting a
/// real block device or a memory-backed mount (`tmpfs`/`overlay`) that can
/// make it nearly free. Best-effort: returns a placeholder string rather
/// than failing if `/proc/mounts` is unreadable (e.g. a non-Linux host).
fn describe_media(dir: &Path) -> String {
    let canon = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    let Ok(mounts) = std::fs::read_to_string("/proc/mounts") else {
        return "unknown (could not read /proc/mounts)".to_string();
    };
    let mut best: Option<(usize, String, String, String)> = None;
    for line in mounts.lines() {
        let mut fields = line.split_whitespace();
        let Some(device) = fields.next() else {
            continue;
        };
        let Some(mount_point) = fields.next() else {
            continue;
        };
        let Some(fstype) = fields.next() else {
            continue;
        };
        if canon.starts_with(mount_point) {
            let len = mount_point.len();
            if best.as_ref().is_none_or(|(best_len, ..)| len > *best_len) {
                best = Some((
                    len,
                    mount_point.to_string(),
                    fstype.to_string(),
                    device.to_string(),
                ));
            }
        }
    }
    match best {
        Some((_, mount_point, fstype, device)) => {
            let media_note = if matches!(fstype.as_str(), "tmpfs" | "overlay" | "overlayfs") {
                " (memory-backed — fsync here is likely near-free; treat any numbers below as inconclusive for the gating decision, see the design note)"
            } else {
                " (a real block-device-backed filesystem — fsync cost here is meaningful)"
            };
            format!("{} on {mount_point} (device {device}){media_note}", fstype)
        }
        None => "unknown (no matching /proc/mounts entry)".to_string(),
    }
}

// ---------------------------------------------------------------------
// JSON output
// ---------------------------------------------------------------------

fn write_json(path: &Path, cfg: &Config, media: &str, rows: &[(String, Option<u64>, Stats)]) {
    let doc = serde_json::json!({
        "params": {
            "groups": cfg.groups,
            "rounds": cfg.rounds,
            "value_bytes": cfg.value_bytes,
        },
        "media": media,
        "scenarios": rows.iter().map(|(label, k, s)| {
            let mut v = s.to_json();
            v["label"] = serde_json::json!(label);
            v["k"] = match k {
                Some(k) => serde_json::json!(k),
                None => serde_json::Value::Null,
            };
            v
        }).collect::<Vec<_>>(),
        "note": "comparable only to another run on the same host/session/media",
    });
    std::fs::write(
        path,
        serde_json::to_string_pretty(&doc).expect("serialize results"),
    )
    .unwrap_or_else(|e| panic!("failed to write {}: {e}", path.display()));
}

// ---------------------------------------------------------------------
// main
// ---------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cfg = Config::from_env();
    let dir = std::env::temp_dir().join(format!("animus-wal-fsync-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create bench dir");

    let media = describe_media(&dir);
    println!(
        "WAL fsync benchmark (ProdEnv, gating SharedWal wiring, ADR 0028/C-05 PR 1):\n\
         groups={:?}, rounds={}, value_bytes={}\n\
         bench dir: {}\n\
         media: {media}\n",
        cfg.groups,
        cfg.rounds,
        cfg.value_bytes,
        dir.display(),
    );

    let addr = "127.0.0.1:0".parse().expect("addr");
    let (prod_env, _bound) = ProdEnv::bind(nid(0), addr, &dir)
        .await
        .expect("bind ProdEnv");
    let env = CountedEnv::new(prod_env);

    let mut rows: Vec<(String, Option<u64>, Stats)> = Vec::new();

    for &k in &cfg.groups {
        println!("-- K={k} active groups --");

        let concurrent = run_per_group(
            &env,
            &format!("pg-conc-{k}"),
            k,
            cfg.rounds,
            cfg.value_bytes,
            true,
        )
        .await;
        report("(a) per-group files, concurrent", Some(k), &concurrent);
        rows.push(("per_group_concurrent".to_string(), Some(k), concurrent));

        let sequential = run_per_group(
            &env,
            &format!("pg-seq-{k}"),
            k,
            cfg.rounds,
            cfg.value_bytes,
            false,
        )
        .await;
        report("(a') per-group files, sequential", Some(k), &sequential);
        rows.push(("per_group_sequential".to_string(), Some(k), sequential));

        let shared_file = format!("shared-{k}.wal");
        let shared = run_shared_wal(&env, &shared_file, k, cfg.rounds, cfg.value_bytes).await;
        report("(b) SharedWal, one file, K groups", Some(k), &shared);
        rows.push(("shared_wal_k_groups".to_string(), Some(k), shared));

        println!();
    }

    println!("-- single-group group-commit control (not swept by K) --");
    let control = run_shared_wal(
        &env,
        "control-single-group.wal",
        32,
        cfg.rounds,
        cfg.value_bytes,
    )
    .await;
    report("(c) single-group 32-write burst", None, &control);
    rows.push(("single_group_burst32".to_string(), None, control));

    if let Some(path) = &cfg.json_path {
        write_json(path, &cfg, &media, &rows);
        println!("\nwrote JSON results to {}", path.display());
    }

    let _ = std::fs::remove_dir_all(&dir);
}
