//! C-17 Tier 2 (no-load-generator part): per-hosted-group in-memory cost of a
//! CP-data tablet group (`RaftCore`/`RaftKvNode` bookkeeping + its drive task)
//! at G groups on one `ProdEnv` node, measured **awake-but-idle** (heartbeating)
//! and then **quiesced** (ADR 0048). Decides C-03's quiesced threshold: the
//! item reopens if, at 1,000 groups/node, quiesced overhead exceeds 64 KB RSS
//! per group or burns any nonzero steady CPU.
//!
//! `#[ignore]`d: a measurement, never a gate; assertions are correctness and
//! liveness only (every group hosted, every group quiesced, a woken group
//! commits). Run:
//!
//! ```text
//! ANIMUS_DENSITY_GROUPS=100,500,1000,5000,10000 ANIMUS_DENSITY_RF=1,3 \
//!   cargo test --release -p animus-cp-data --test group_density_cost \
//!   --features prod-heavy -- --ignored --nocapture
//! ```
//!
//! Knobs: `ANIMUS_DENSITY_GROUPS` (default `100,1000`), `ANIMUS_DENSITY_RF`
//! (default `1,3`), `ANIMUS_DENSITY_WINDOW_SECS` (steady-CPU window, default
//! 10). RF=3 hosts three replicas per group over three `ProdEnv`s in this one
//! process (real loopback sockets), so heartbeats, the per-node heartbeat
//! batcher and the quorum path count; per-group figures divide by `RF * G`
//! replicas. Output lines are `C17T2 ...` (machine-readable).
//!
//! Active-at-a-fixed-write-rate, hot-tablet p99 and multi-node scaling need
//! B-01's load generator and are NOT measured here.

#![allow(
    clippy::disallowed_methods,
    reason = "real-thread ProdEnv measurement (wall-clock windows, /proc reads); ADR 0061 Decision 4"
)]

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use animus_control::ProposeResult;
use animus_cp_data::heartbeat_batch::{DEFAULT_HEARTBEAT_BATCH_INTERVAL, HeartbeatBatcher};
use animus_cp_data::{RaftKvNode, StorageScope};
use animus_env::{Env, NodeId, ProdEnv, nid};
use animus_storage::MemoryEngine;
use tokio::time::{sleep, timeout};

type KvNode = RaftKvNode<ProdEnv, MemoryEngine>;

const QUIESCE_AFTER: Duration = Duration::from_secs(2);

fn getconf(name: &str) -> u64 {
    let out = std::process::Command::new("getconf")
        .arg(name)
        .output()
        .expect("run getconf");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .expect("getconf number")
}

fn rss_bytes(page: u64) -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").expect("statm");
    statm
        .split_whitespace()
        .nth(1)
        .expect("resident field")
        .parse::<u64>()
        .expect("number")
        * page
}

/// utime + stime of this process, in clock ticks.
fn cpu_ticks() -> u64 {
    let stat = std::fs::read_to_string("/proc/self/stat").expect("stat");
    // Fields after the `(comm)` close paren; utime/stime are overall fields 14/15.
    let rest = &stat[stat.rfind(')').expect("comm close") + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    f[11].parse::<u64>().expect("utime") + f[12].parse::<u64>().expect("stime")
}

fn open_fds() -> u64 {
    std::fs::read_dir("/proc/self/fd").expect("fd dir").count() as u64
}

fn alive_tasks() -> u64 {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks() as u64
}

fn env_list(name: &str, default: &str) -> Vec<usize> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .split(',')
        .map(|s| s.trim().parse().expect("number list"))
        .collect()
}

struct Snapshot {
    rss: u64,
    tasks: u64,
    fds: u64,
}

fn snap(page: u64) -> Snapshot {
    Snapshot {
        rss: rss_bytes(page),
        tasks: alive_tasks(),
        fds: open_fds(),
    }
}

#[allow(clippy::too_many_arguments)]
fn report(
    mode: &str,
    rf: usize,
    g: usize,
    base: &Snapshot,
    now: &Snapshot,
    page: u64,
    cpu_base: f64,
    cpu_ms_per_s: f64,
    wake_us: Option<u64>,
    build_ms: u128,
) {
    let reps = (rf * g) as f64;
    let net = (cpu_ms_per_s - cpu_base).max(0.0);
    let rss_delta = now.rss.saturating_sub(base.rss);
    let tasks = now.tasks.saturating_sub(base.tasks);
    let fds = now.fds.saturating_sub(base.fds);
    println!(
        "C17T2 groups={g} rf={rf} mode={mode} rss_total_bytes={rss_delta} \
         rss_per_group_bytes={:.0} rss_per_replica_bytes={:.0} cpu_ms_per_s={cpu_ms_per_s:.1} \
         cpu_net_ms_per_s={net:.1} cpu_net_ms_per_s_per_group={:.4} tasks={tasks} tasks_per_group={:.2} fds={fds} \
         fds_per_group={:.3} wake_latency_us={} host_ms={build_ms} page={page}",
        rss_delta as f64 / g as f64,
        rss_delta as f64 / reps,
        net / g as f64,
        tasks as f64 / g as f64,
        fds as f64 / g as f64,
        wake_us.map_or("na".to_string(), |v| v.to_string()),
    );
}

async fn measure_cpu(window: Duration, tick_hz: u64) -> f64 {
    let t0 = cpu_ticks();
    let w0 = Instant::now();
    sleep(window).await;
    let ticks = cpu_ticks() - t0;
    let secs = w0.elapsed().as_secs_f64();
    (ticks as f64 * 1000.0 / tick_hz as f64) / secs
}

async fn one_density_run(g: usize, rf: usize, window: Duration, page: u64, hz: u64) {
    let dirs: Vec<_> = (0..rf)
        .map(|i| {
            let d = std::env::temp_dir().join(format!(
                "animus-density-{}-{rf}-{g}-{i}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&d);
            std::fs::create_dir_all(&d).expect("dir");
            d
        })
        .collect();
    let all: Vec<NodeId> = (0..rf as u64).map(nid).collect();

    let mut envs = Vec::new();
    for (i, d) in dirs.iter().enumerate() {
        let (env, _) = ProdEnv::bind(nid(i as u64), "127.0.0.1:0".parse().unwrap(), d)
            .await
            .expect("bind");
        envs.push(env);
    }
    let book: BTreeMap<NodeId, String> = envs
        .iter()
        .map(|e| (e.node_id(), e.local_addr().to_string()))
        .collect();
    for e in &envs {
        e.set_peers(book.clone());
    }
    let batchers: Vec<_> = envs
        .iter()
        .map(|e| HeartbeatBatcher::new(e.clone(), DEFAULT_HEARTBEAT_BATCH_INTERVAL, e.metrics()))
        .collect();

    // Baseline AFTER envs + batchers exist, BEFORE any group is hosted.
    sleep(Duration::from_millis(300)).await;
    let base = snap(page);
    // Steady CPU of the bare envs + batchers (no groups): subtracted as `cpu_net`.
    let cpu_base = measure_cpu(window, hz).await;
    println!("C17T2 groups=0 rf={rf} mode=baseline cpu_ms_per_s={cpu_base:.1}");

    // groups[g][replica]; replica 0 campaigns immediately (deterministic leader).
    let t0 = Instant::now();
    let mut groups: Vec<Vec<KvNode>> = Vec::with_capacity(g);
    for s in 0..g as u64 {
        let mut reps = Vec::with_capacity(rf);
        // Followers first, the campaigning replica 0 last, so its first vote
        // request finds every peer's stream already subscribed.
        for (i, e) in envs.iter().enumerate().rev() {
            let b = Some(batchers[i].clone());
            reps.push(if i == 0 {
                RaftKvNode::start_hosted_campaigning_with_batcher(
                    e.clone(),
                    all.clone(),
                    MemoryEngine::new(),
                    StorageScope::whole(),
                    s,
                    b,
                )
            } else {
                RaftKvNode::start_hosted_split_follower_with_batcher(
                    e.clone(),
                    all.clone(),
                    MemoryEngine::new(),
                    StorageScope::whole(),
                    s,
                    b,
                )
            });
        }
        reps.reverse();
        groups.push(reps);
    }
    // Every group hosted AND led.
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let led = groups
            .iter()
            .filter(|r| r.iter().any(KvNode::is_leader))
            .count();
        if led == g {
            break;
        }
        assert!(Instant::now() < deadline, "only {led}/{g} groups elected");
        sleep(Duration::from_millis(100)).await;
    }
    let host_ms = t0.elapsed().as_millis();
    // Settle: let initial election/no-op replication traffic die down.
    sleep(Duration::from_secs(3)).await;

    // --- awake-idle (never enabled quiescence: timers + heartbeats run) ---
    let cpu = measure_cpu(window, hz).await;
    let awake = snap(page);
    report(
        "awake", rf, g, &base, &awake, page, cpu_base, cpu, None, host_ms,
    );

    // --- quiesced ---
    for reps in &groups {
        for n in reps {
            n.enable_quiescence(QUIESCE_AFTER);
        }
    }
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let q = groups
            .iter()
            .flat_map(|r| r.iter())
            .filter(|n| n.is_quiesced())
            .count();
        if q == g * rf {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "only {q}/{} replicas quiesced",
            g * rf
        );
        sleep(Duration::from_millis(200)).await;
    }
    sleep(Duration::from_secs(2)).await;
    let cpu = measure_cpu(window, hz).await;
    let quiesced = snap(page);

    // Idle wake latency: one direct propose to a few quiesced groups.
    let mut samples = Vec::new();
    for k in 0..5usize.min(g) {
        let leader = groups[(k * 7919) % g]
            .iter()
            .find(|n| n.is_leader())
            .expect("group has a leader");
        assert!(leader.is_quiesced(), "group must be quiesced before wake");
        let t = Instant::now();
        let idx = match leader.put(b"wake".to_vec(), b"1".to_vec()) {
            ProposeResult::Accepted { index, .. } => index,
            other => panic!("woken leader refused write: {other:?}"),
        };
        timeout(Duration::from_secs(30), async {
            while leader.commit_index() < idx {
                sleep(Duration::from_micros(100)).await;
            }
        })
        .await
        .expect("woken group never committed the write");
        samples.push(t.elapsed().as_micros() as u64);
    }
    samples.sort_unstable();
    let wake = samples.get(samples.len() / 2).copied();
    report(
        "quiesced", rf, g, &base, &quiesced, page, cpu_base, cpu, wake, host_ms,
    );
    println!("C17T2 groups={g} rf={rf} wake_samples_us={samples:?} (median reported above)");

    drop(groups);
    for e in &envs {
        e.shutdown_and_wait().await;
    }
    drop(batchers);
    for d in &dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, not a gate — run with --ignored --nocapture (see module doc)"]
async fn group_density_cost_measurement() {
    let gs = env_list("ANIMUS_DENSITY_GROUPS", "100,1000");
    let rfs = env_list("ANIMUS_DENSITY_RF", "1,3");
    let window = Duration::from_secs(
        std::env::var("ANIMUS_DENSITY_WINDOW_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10),
    );
    let page = getconf("PAGESIZE");
    let hz = getconf("CLK_TCK");
    // Child mode: exactly one (rf, g) cell in this fresh process.
    if let Ok(cell) = std::env::var("ANIMUS_DENSITY_CELL") {
        let (rf, g) = cell.split_once(':').expect("rf:g");
        let (rf, g) = (rf.parse().expect("rf"), g.parse().expect("g"));
        timeout(
            Duration::from_secs(1800),
            one_density_run(g, rf, window, page, hz),
        )
        .await
        .unwrap_or_else(|_| panic!("density run g={g} rf={rf} timed out"));
        return;
    }
    // Parent mode: one child process per cell, so the allocator's retained
    // pages from a previous cell can never deflate the next cell's RSS delta.
    let exe = std::env::current_exe().expect("current_exe");
    for &rf in &rfs {
        for &g in &gs {
            let exe = exe.clone();
            let status = tokio::task::spawn_blocking(move || {
                std::process::Command::new(exe)
                    .args([
                        "--ignored",
                        "--nocapture",
                        "--exact",
                        "group_density_cost_measurement",
                    ])
                    .env("ANIMUS_DENSITY_CELL", format!("{rf}:{g}"))
                    .status()
                    .expect("spawn child")
            })
            .await
            .expect("join");
            assert!(status.success(), "density cell rf={rf} g={g} failed");
        }
    }
}
