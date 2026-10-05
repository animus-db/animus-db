//! Real-cluster soak (R-01 sub-track a): real `animusd` processes on loopback
//! under a continuous recorded DynamoDB-wire workload for hours or days, the
//! `animus-test` oracles run over the history **per epoch**, and per-node
//! resource series (RSS, fds, threads, disk, WAL/SSTable files, queue gauges)
//! checked for monotone growth with `animus_test::soak`. See `docs/soak.md`.
//!
//! Opt-in: `ANIMUS_SOAK_DURATION=15m cargo test -p animusd --features soak
//! --test soak -- --nocapture` (the `soak` feature keeps this out of the
//! per-push gates). Knobs: `ANIMUS_SOAK_DURATION` (`90`, `15m`, `2h`, `7d`;
//! default `10m`), `ANIMUS_SOAK_SEED`, `ANIMUS_SOAK_NODES` (>=3),
//! `ANIMUS_SOAK_TABLETS`, `ANIMUS_SOAK_EPOCH_SECS` (default 300),
//! `ANIMUS_SOAK_SAMPLE_SECS` (default 15), `ANIMUS_SOAK_WARMUP` (duration
//! syntax; default a quarter of the run, at most 6h), `ANIMUS_SOAK_PACE_MS`
//! (`base,spread`, default `20,60`), `ANIMUS_SOAK_RESTART_EVERY` (kill -9 and
//! restart one node, rotating, every N epochs; default 0 = never),
//! `ANIMUS_SOAK_DIR`, `ANIMUS_SOAK_OUT`.
//!
//! # Why memory is bounded for a multi-day run
//!
//! The history is cut into **epochs**. Each epoch uses a fresh key range
//! (`key_base`), runs the workload for `ANIMUS_SOAK_EPOCH_SECS`, stops it, does
//! the final reads and runs `check_cycles`/`check_durability`/
//! `check_convergence` over *that epoch's* history, then drops the history.
//! Every key is touched by exactly one epoch's clients, so per-epoch
//! verification loses nothing: the oracles' properties are per-key. Cold data
//! is still re-verified: the previous epoch's keys (and epoch 0's) are re-read
//! at each epoch end and must be byte-identical to their recorded final state.
//! Keys two epochs old are deleted (epoch 0 stays as the canary), so the live data set (and with it disk,
//! SSTables and compaction work) is bounded by design and any growth the trend
//! detector sees is a leak, not the workload.

#[allow(
    dead_code,
    reason = "shared with chaos.rs; the soak uses only part of it"
)]
mod chaos_support;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use animus_test::soak::{TrendConfig, Verdict, evaluate, parse_duration};
use chaos_support::client::{dynamo_call, http_get};
use chaos_support::cluster::ChaosCluster;
use chaos_support::rng::name_seed;
use chaos_support::workload::{self, CLIENTS, KEYS, Shared, pk};
use serde_json::{Value, json};
use tokio::time::Instant;

fn env_str(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

fn env_u64(name: &str) -> Option<u64> {
    env_str(name).and_then(|v| v.trim().parse().ok())
}

fn env_dur(name: &str) -> Option<Duration> {
    env_str(name)
        .map(|v| parse_duration(&v).unwrap_or_else(|| panic!("{name}={v:?} is not a duration")))
}

/// Gauges (level metrics) from `/admin/metrics` worth trending. Names are the
/// `Metric::name()` strings in `animus-env`'s `metrics.rs`.
const GAUGES: [&str; 4] = [
    "demux_queued_frames",
    "demux_queued_bytes",
    "spawned_task_handles_tracked",
    "stream_hot_bytes",
];

/// One sampled series' identity and tolerance: `(name, rel_tol, abs_tol)`.
const SERIES: [(&str, f64, f64); 9] = [
    ("rss_bytes", 0.25, 48.0 * 1048576.0),
    ("fds", 0.25, 32.0),
    ("threads", 0.25, 16.0),
    ("data_bytes", 0.5, 96.0 * 1048576.0),
    ("wal_bytes", 0.5, 96.0 * 1048576.0),
    ("sst_files", 0.5, 24.0),
    ("demux_queued_frames", 0.5, 256.0),
    ("demux_queued_bytes", 0.5, 4.0 * 1048576.0),
    ("spawned_task_handles_tracked", 0.5, 128.0),
];

fn proc_kb(pid: u32, field: &str) -> Option<u64> {
    let s = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    s.lines().find_map(|l| l.strip_prefix(field)).and_then(|r| {
        r.trim_start_matches(':')
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
}

/// `(total bytes, WAL-file bytes, SSTable file count)` under a data dir. The
/// LSM names its files `.../wal-NNNNNN` and `.../sst-NNNNNN`; the control
/// plane / shared WAL files carry `wal` in their names too.
fn walk(dir: &Path, acc: &mut (u64, u64, u64)) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let Ok(md) = e.metadata() else { continue };
        if md.is_dir() {
            walk(&e.path(), acc);
        } else {
            let name = e.file_name().to_string_lossy().into_owned();
            acc.0 += md.len();
            if name.contains("wal") {
                acc.1 += md.len();
            }
            if name.contains("sst-") {
                acc.2 += 1;
            }
        }
    }
}

/// All samples: `series[node][name] -> Vec<(secs, value)>`.
type Series = Vec<BTreeMap<&'static str, Vec<(f64, f64)>>>;

async fn sample(cluster: &ChaosCluster, t: f64, series: &mut Series, csv: &mut std::fs::File) {
    for (i, node_series) in series.iter_mut().enumerate() {
        let Some(pid) = cluster.pid(i) else { continue };
        let mut row: Vec<(&'static str, f64)> = Vec::new();
        if let Some(kb) = proc_kb(pid, "VmRSS") {
            row.push(("rss_bytes", kb as f64 * 1024.0));
        }
        if let Some(th) = proc_kb(pid, "Threads") {
            row.push(("threads", th as f64));
        }
        if let Ok(rd) = std::fs::read_dir(format!("/proc/{pid}/fd")) {
            row.push(("fds", rd.count() as f64));
        }
        let mut acc = (0, 0, 0);
        walk(&cluster.data_dir(i), &mut acc);
        row.push(("data_bytes", acc.0 as f64));
        row.push(("wal_bytes", acc.1 as f64));
        row.push(("sst_files", acc.2 as f64));
        if let Ok((200, body)) = http_get(
            cluster.admin_addr(i),
            "/admin/metrics",
            Duration::from_secs(5),
        )
        .await
            && let Ok(v) = serde_json::from_str::<Value>(&body)
        {
            for g in GAUGES {
                if let Some(x) = v["counters"][g].as_u64() {
                    row.push((g, x as f64));
                }
            }
        }
        for (name, v) in row {
            let _ = writeln!(csv, "{t:.1},{i},{name},{v}");
            let name = SERIES
                .iter()
                .map(|s| s.0)
                .chain(GAUGES)
                .find(|s| *s == name)
                .expect("known series");
            node_series.entry(name).or_default().push((t, v));
        }
    }
}

/// Bytes appended to `path` since `*off`, as lines containing `panicked at`.
fn new_panics(path: &Path, off: &mut u64) -> Vec<String> {
    let Ok(mut f) = std::fs::File::open(path) else {
        return Vec::new();
    };
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len < *off || f.seek(SeekFrom::Start(*off)).is_err() {
        *off = 0;
        let _ = f.seek(SeekFrom::Start(0));
    }
    let mut buf = Vec::new();
    let _ = f.read_to_end(&mut buf);
    *off += buf.len() as u64;
    String::from_utf8_lossy(&buf)
        .lines()
        .filter(|l| l.contains("panicked at"))
        .map(str::to_string)
        .collect()
}

async fn delete_keys(nodes: &[SocketAddr], base: u64) -> usize {
    let mut failed = 0;
    for k in 0..KEYS {
        let body = json!({"TableName": workload::TABLE, "Key": pk(base + k)}).to_string();
        let mut ok = false;
        for attempt in 0..6 {
            let node = nodes[(k as usize + attempt) % nodes.len()];
            if matches!(
                dynamo_call(node, "DeleteItem", &body, Duration::from_secs(10)).await,
                Ok((200, _))
            ) {
                ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        failed += usize::from(!ok);
    }
    failed
}

async fn read_epoch(
    node: SocketAddr,
    base: u64,
    budget: Duration,
    violations: &mut Vec<String>,
) -> BTreeMap<u64, Vec<u64>> {
    let mut m = BTreeMap::new();
    for k in 0..KEYS {
        match workload::final_read(node, base + k, budget).await {
            Ok(l) => {
                m.insert(base + k, l);
            }
            Err(e) => violations.push(format!("[final-read] {e}")),
        }
    }
    m
}

async fn soak() -> Vec<String> {
    let duration = env_dur("ANIMUS_SOAK_DURATION").unwrap_or(Duration::from_secs(600));
    let seed = env_u64("ANIMUS_SOAK_SEED").unwrap_or_else(|| name_seed("soak"));
    let n = env_u64("ANIMUS_SOAK_NODES").unwrap_or(3) as usize;
    let tablets = env_u64("ANIMUS_SOAK_TABLETS").unwrap_or(4);
    let epoch_len = Duration::from_secs(env_u64("ANIMUS_SOAK_EPOCH_SECS").unwrap_or(300).max(30));
    let sample_every = Duration::from_secs(env_u64("ANIMUS_SOAK_SAMPLE_SECS").unwrap_or(15).max(1));
    let warmup = env_dur("ANIMUS_SOAK_WARMUP")
        .unwrap_or_else(|| (duration / 4).min(Duration::from_secs(6 * 3600)));
    let restart_every = env_u64("ANIMUS_SOAK_RESTART_EVERY").unwrap_or(0);
    let pace = env_str("ANIMUS_SOAK_PACE_MS")
        .and_then(|v| {
            let (a, b) = v.split_once(',')?;
            Some((a.trim().parse().ok()?, b.trim().parse().ok()?))
        })
        .unwrap_or((20, 60));
    let recovery = Duration::from_secs(60);
    assert!(n >= 3, "ANIMUS_SOAK_NODES must be at least 3");

    let base = env_str("ANIMUS_SOAK_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    std::fs::create_dir_all(&base).expect("soak base dir");
    let scratch = tempfile::Builder::new()
        .prefix("animus-soak-")
        .tempdir_in(&base)
        .expect("soak scratch dir");
    let out_dir = env_str("ANIMUS_SOAK_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| base.join("animus-soak-out"))
        .join(format!("soak-{seed}"));
    std::fs::create_dir_all(&out_dir).expect("soak out dir");
    let mut csv = std::fs::File::create(out_dir.join("samples.csv")).expect("samples.csv");
    let _ = writeln!(csv, "secs,node,series,value");
    eprintln!(
        "soak: seed={seed} duration={duration:?} nodes={n} tablets={tablets} epoch={epoch_len:?} \
         sample={sample_every:?} warmup={warmup:?} restart_every={restart_every} pace={pace:?} out={}",
        out_dir.display()
    );

    let mut cluster = ChaosCluster::prepare(n, scratch.path(), seed).await;
    cluster.start_all();
    let nodes: Vec<SocketAddr> = (0..n).map(|i| cluster.dynamo_addr(i)).collect();
    let mut violations: Vec<String> = Vec::new();
    let mut events: Vec<String> = Vec::new();

    workload::create_table(&nodes, tablets, Duration::from_secs(120))
        .await
        .expect("bring-up: CreateTable");
    if tablets > 1 {
        let t0 = Instant::now();
        while cluster.tablet_count(workload::TABLE).await < tablets as usize
            && t0.elapsed() < Duration::from_secs(90)
        {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    for (i, a) in nodes.iter().enumerate() {
        workload::probe_available(*a, 1000 + i as u64, Duration::from_secs(60))
            .await
            .expect("bring-up: every node serves");
    }

    let mut series: Series = (0..n).map(|_| BTreeMap::new()).collect();
    let mut log_off = vec![0u64; n];
    let t_start = Instant::now();
    let secs = |t: Instant| t.duration_since(t_start).as_secs_f64();
    sample(&cluster, 0.0, &mut series, &mut csv).await;

    let (mut ok_writes, mut ok_reads, mut info_ops) = (0u64, 0u64, 0u64);
    let mut epoch = 0u64;
    let mut prev: Option<(u64, BTreeMap<u64, Vec<u64>>)> = None;
    let mut first: Option<(u64, BTreeMap<u64, Vec<u64>>)> = None;
    let mut cleanup_failed = 0usize;
    while t_start.elapsed() + Duration::from_secs(10) < duration && violations.is_empty() {
        let remaining = duration - t_start.elapsed();
        let this_len = epoch_len.min(remaining);
        let key_base = epoch * KEYS;
        let eseed = seed.wrapping_add(epoch);
        let shared = Arc::new(Shared::with_base(eseed, key_base, pace));
        let clients: Vec<_> = (1..=CLIENTS)
            .map(|proc| {
                let sh = Arc::clone(&shared);
                let nodes = nodes.clone();
                tokio::spawn(async move { workload::client_loop(&sh, proc, nodes).await })
            })
            .collect();
        let e0 = Instant::now();
        while e0.elapsed() < this_len {
            tokio::time::sleep(sample_every.min(this_len - e0.elapsed())).await;
            sample(&cluster, secs(Instant::now()), &mut series, &mut csv).await;
            for (i, status) in cluster.unexpected_exits() {
                violations.push(format!("[node-exit] n{i} exited on its own: {status}"));
            }
            if !violations.is_empty() {
                break;
            }
        }
        shared.stop.store(true, Ordering::Relaxed);
        for c in clients {
            let _ = c.await;
        }

        // ---- per-epoch verification --------------------------------
        let fin_a = read_epoch(nodes[0], key_base, recovery, &mut violations).await;
        let fin_b = read_epoch(nodes[1], key_base, recovery, &mut violations).await;
        let (history, verdict) = workload::run_oracles(&shared, &fin_a, &fin_b);
        violations.extend(
            verdict
                .violations
                .iter()
                .map(|v| format!("epoch {epoch}: {v}")),
        );
        let st = &shared.stats;
        let (w, r) = (
            st.ok_writes.load(Ordering::Relaxed),
            st.ok_reads.load(Ordering::Relaxed),
        );
        let inf = st.info_writes.load(Ordering::Relaxed) + st.info_reads.load(Ordering::Relaxed);
        ok_writes += w;
        ok_reads += r;
        info_ops += inf;
        eprintln!(
            "soak: epoch {epoch} t={:.0}s ok_writes={w} ok_reads={r} info={inf} fail_writes={} history_entries={} violations={}",
            secs(Instant::now()),
            st.fail_writes.load(Ordering::Relaxed),
            history.entries.len(),
            violations.len()
        );
        if w < 50 {
            violations.push(format!(
                "epoch {epoch}: [non-vacuity] only {w} acknowledged writes"
            ));
        }
        // Cold data: epoch N-1 and epoch 0 must still read back unchanged.
        for (e, want) in prev.iter().chain(first.iter().filter(|_| epoch > 1)) {
            let got = read_epoch(
                nodes[(epoch as usize) % n],
                e * KEYS,
                recovery,
                &mut violations,
            )
            .await;
            if &got != want {
                let k = got
                    .iter()
                    .find(|(k, v)| want.get(k) != Some(v))
                    .map(|(k, _)| *k);
                violations.push(format!("epoch {epoch}: [cold-data] epoch {e} changed since it was verified (first differing key {k:?})"));
            }
        }
        for (i, off) in log_off.iter_mut().enumerate() {
            for p in new_panics(&cluster.log_path(i), off) {
                violations.push(format!("[node-panic] n{i}: {p}"));
            }
        }
        if !violations.is_empty() {
            let _ = std::fs::write(
                out_dir.join("history.json"),
                animus_test::export::to_json(&history),
            );
            let _ = std::fs::write(
                out_dir.join("op-trace.txt"),
                shared.trace.lock().expect("trace").join("\n"),
            );
            break;
        }
        // Bound the live data set: keys two epochs old are deleted, except
        // epoch 0's, which stay as the long-lived cold-data canary.
        if epoch >= 3 {
            cleanup_failed += delete_keys(&nodes, (epoch - 2) * KEYS).await;
        }
        if first.is_none() {
            first = Some((epoch, fin_a.clone()));
        }
        prev = Some((epoch, fin_a));
        if restart_every > 0 && (epoch + 1).is_multiple_of(restart_every) {
            let victim = ((epoch + 1) / restart_every) as usize % n;
            cluster.kill9(victim);
            cluster.start(victim);
            events.push(format!("epoch {epoch}: kill -9 + restart n{victim}"));
            log_off[victim] = std::fs::metadata(cluster.log_path(victim)).map_or(0, |m| m.len());
            match workload::probe_available(nodes[victim], 5000 + epoch, recovery).await {
                Ok(d) => eprintln!("soak: n{victim} restarted, serving after {d:?}"),
                Err(e) => violations.push(format!("[availability] {e}")),
            }
        }
        epoch += 1;
    }
    sample(&cluster, secs(Instant::now()), &mut series, &mut csv).await;
    for (i, status) in cluster.unexpected_exits() {
        violations.push(format!("[node-exit] n{i} exited on its own: {status}"));
    }

    // ---- resource trends ----------------------------------------------
    let warm = warmup.as_secs_f64();
    let mut summary = String::new();
    for (i, node_series) in series.iter().enumerate() {
        for (name, rel, abs) in SERIES {
            let Some(s) = node_series.get(name) else {
                continue;
            };
            let cfg = TrendConfig::new(warm, rel, abs);
            let r = evaluate(s, &cfg);
            let last = s.last().map_or(f64::NAN, |p| p.1);
            let line = format!(
                "n{i} {name:<30} samples={:<5} last={last:<14.0} max={:<14.0} medians={:?} => {:?}",
                s.len(),
                r.max,
                r.window_medians
                    .iter()
                    .map(|m| m.round() as i64)
                    .collect::<Vec<_>>(),
                r.verdict
            );
            eprintln!("soak: trend {line}");
            let _ = writeln!(summary, "{line}");
            if let Verdict::Growing(why) = r.verdict {
                violations.push(format!("[resource-trend] n{i} {name}: {why}"));
            }
        }
    }
    eprintln!(
        "soak: done epochs={} ok_writes={ok_writes} ok_reads={ok_reads} info_ops={info_ops} cleanup_failed={cleanup_failed} violations={}",
        epoch + 1,
        violations.len()
    );
    let _ = std::fs::write(out_dir.join("trend-summary.txt"), &summary);
    let _ = std::fs::write(out_dir.join("events.txt"), events.join("\n"));
    if !violations.is_empty() {
        let _ = std::fs::write(out_dir.join("violations.txt"), violations.join("\n"));
        for i in 0..n {
            let _ = std::fs::copy(cluster.log_path(i), out_dir.join(format!("n{i}.log")));
        }
        for v in violations.iter().take(20) {
            eprintln!("soak: VIOLATION {v}");
        }
        eprintln!("soak: FAILED; artifacts under {}", out_dir.display());
    }
    drop(cluster);
    violations
}

#[test]
fn soak_run() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    let violations = rt.block_on(soak());
    assert!(
        violations.is_empty(),
        "soak found {} violation(s); first: {}",
        violations.len(),
        violations.first().map_or("", String::as_str)
    );
}
