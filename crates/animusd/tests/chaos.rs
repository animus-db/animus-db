//! Real-cluster chaos (R-01 sub-track b): real `animusd` processes on
//! loopback, a continuous recorded DynamoDB-wire workload, real faults, and
//! the `animus-test` oracles over the recorded history. See `docs/chaos.md`.
//!
//! Opt-in: `cargo test -p animusd --features chaos --test chaos -- --test-threads=1`
//! (the `chaos` feature keeps this multi-minute run out of the per-push
//! gates). Knobs: `ANIMUS_CHAOS_SEED`, `ANIMUS_CHAOS_SECS`,
//! `ANIMUS_CHAOS_NODES`, `ANIMUS_CHAOS_TABLETS`, `ANIMUS_CHAOS_RECOVERY_SECS`,
//! `ANIMUS_CHAOS_DIR`, `ANIMUS_CHAOS_OUT`. The disk-full scenario
//! (`chaos_disk_full`, issue #1221) mounts real size-limited tmpfs
//! filesystems, so it needs `CAP_SYS_ADMIN` (root, or passwordless `sudo`)
//! and **skips with a message** where mounting is not permitted
//! (`ANIMUS_CHAOS_REQUIRE_MOUNT=1` turns that skip into a failure, for CI);
//! its own knobs are `ANIMUS_CHAOS_DISK_MB` (per-node mount size, default 64)
//! and `ANIMUS_CHAOS_DISK_TXN=0` (drop the 2PC ops, which are on by default
//! since F-2 in `docs/chaos.md` was fixed).
//!
//! The fault *schedule* is a pure function of the seed; the processes are
//! real and not deterministic (ADR 0003 determinism is `SimEnv`-only). The
//! sim corpora remain the correctness proof; this checks the `ProdEnv` seams
//! they cannot (real sockets, real fsync, real scheduling, real SIGKILL).

mod chaos_support;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chaos_support::client::{dynamo_call, http_get};
use chaos_support::cluster::ChaosCluster;
use chaos_support::diskfull::{self, Tmpfs};
use chaos_support::nemesis::{Scenario, plan, run_plan};
use chaos_support::rng::name_seed;
use chaos_support::workload::{self, CLIENTS, KEYS, Shared};

/// Scenarios bind fixed-shape loopback port ranges and share one process
/// tree budget: never run two at once, even without `--test-threads=1`.
static SERIAL: Mutex<()> = Mutex::new(());

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.trim().parse().ok())
}

struct Outcome {
    violations: Vec<String>,
}

async fn run_scenario(scn: Scenario) -> Outcome {
    let seed = env_u64("ANIMUS_CHAOS_SEED").unwrap_or_else(|| name_seed(scn.name()));
    let window =
        Duration::from_secs(env_u64("ANIMUS_CHAOS_SECS").unwrap_or_else(|| scn.default_secs()));
    let n = env_u64("ANIMUS_CHAOS_NODES").unwrap_or(3) as usize;
    let tablets = env_u64("ANIMUS_CHAOS_TABLETS").unwrap_or(4);
    let recovery = Duration::from_secs(env_u64("ANIMUS_CHAOS_RECOVERY_SECS").unwrap_or(60));
    assert!(n >= 3, "ANIMUS_CHAOS_NODES must be at least 3");

    let base = std::env::var("ANIMUS_CHAOS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    std::fs::create_dir_all(&base).expect("chaos base dir");
    let scratch = tempfile::Builder::new()
        .prefix("animus-chaos-")
        .tempdir_in(&base)
        .expect("chaos scratch dir");
    let out_dir = std::env::var("ANIMUS_CHAOS_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| base.join("animus-chaos-out"))
        .join(format!("{}-{seed}", scn.name()));

    eprintln!(
        "chaos[{}]: seed={seed} window={window:?} nodes={n} tablets={tablets} \
         (replay the fault schedule with ANIMUS_CHAOS_SEED={seed}; process timing is not deterministic)",
        scn.name()
    );
    let steps = plan(scn, seed, window, n);
    for s in &steps {
        eprintln!(
            "chaos[{}]: planned t={:>4}s {:?}",
            scn.name(),
            s.at.as_secs(),
            s.fault
        );
    }

    let mut cluster = ChaosCluster::prepare(n, scratch.path(), seed).await;
    cluster.start_all();
    let nodes: Vec<SocketAddr> = (0..n).map(|i| cluster.dynamo_addr(i)).collect();

    let mut events: Vec<String> = Vec::new();
    let mut violations: Vec<String> = Vec::new();

    // ---- bring-up ----------------------------------------------------
    workload::create_table(&nodes, tablets, Duration::from_secs(120))
        .await
        .expect("bring-up: CreateTable");
    if tablets > 1 {
        let want = tablets as usize;
        let t0 = tokio::time::Instant::now();
        while cluster.tablet_count(workload::TABLE).await < want
            && t0.elapsed() < Duration::from_secs(90)
        {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        eprintln!(
            "chaos[{}]: table has {} tablet(s) after {:?} (wanted {want})",
            scn.name(),
            cluster.tablet_count(workload::TABLE).await,
            t0.elapsed()
        );
    }
    // Every node must serve before the faults start.
    for (i, a) in nodes.iter().enumerate() {
        workload::probe_available(*a, 1000 + i as u64, Duration::from_secs(60))
            .await
            .expect("bring-up: every node serves");
    }

    // ---- workload + faults ------------------------------------------
    let shared = Arc::new(Shared::new(seed));
    let clients: Vec<_> = (1..=CLIENTS)
        .map(|proc| {
            let sh = Arc::clone(&shared);
            let nodes = nodes.clone();
            tokio::spawn(async move { workload::client_loop(&sh, proc, nodes).await })
        })
        .collect();

    let start = tokio::time::Instant::now();
    run_plan(&mut cluster, &steps, start, seed, &mut events).await;
    tokio::time::sleep_until(start + window).await;
    shared.stop.store(true, Ordering::Relaxed);
    for c in clients {
        let _ = c.await;
    }
    for (i, status) in cluster.unexpected_exits() {
        violations.push(format!("[node-exit] n{i} exited on its own: {status}"));
    }

    // ---- heal, converge ---------------------------------------------
    cluster.faults.heal();
    for i in 0..n {
        cluster.resume(i);
        if !cluster.is_running(i) {
            cluster.start(i);
        }
    }
    events.push("healed everything; probing availability".into());
    let mut recovered = Vec::new();
    for (i, a) in nodes.iter().enumerate() {
        match workload::probe_available(*a, i as u64, recovery).await {
            Ok(d) => recovered.push((i, d)),
            Err(e) => violations.push(format!("[availability] {e}")),
        }
    }
    eprintln!(
        "chaos[{}]: post-heal time-to-serve per node: {recovered:?}",
        scn.name()
    );

    let mut fin_a: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    let mut fin_b: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for key in 0..KEYS {
        match workload::final_read(nodes[0], key, recovery).await {
            Ok(l) => {
                fin_a.insert(key, l);
            }
            Err(e) => violations.push(format!("[final-read] {e}")),
        }
        match workload::final_read(nodes[1], key, recovery).await {
            Ok(l) => {
                fin_b.insert(key, l);
            }
            Err(e) => violations.push(format!("[final-read] {e}")),
        }
    }

    // ---- replica convergence (diagnostic) ----------------------------
    // Every node's own eventual read must reach the final state: a node that
    // never does holds a permanently diverged replica (as opposed to one
    // stale read that later caught up).
    let convergence_budget = Duration::from_secs(20);
    let divergences = workload::replica_convergence(&nodes, &fin_a, convergence_budget).await;
    violations.extend(divergences.iter().map(|d| d.violation(convergence_budget)));

    // ---- node counters (diagnostic) ------------------------------------
    // Which recovery paths this run actually took (a snapshot install, a
    // merge that silently took no effect, a failure-detector flap), so a red
    // run names the mechanism it exercised instead of leaving it to a guess.
    let mut counter_lines: Vec<String> = Vec::new();
    for i in 0..n {
        let c = cluster
            .counters(
                i,
                &[
                    "snapshot",
                    "no_effect",
                    "failure_detector",
                    "txn_",
                    "elections_won",
                    "needs_snapshot",
                ],
            )
            .await;
        counter_lines.push(format!("n{i}: {}", c.join(" ")));
    }
    for l in &counter_lines {
        eprintln!("chaos[{}]: counters {l}", scn.name());
    }

    // ---- oracles -----------------------------------------------------
    let (history, verdict) = workload::run_oracles(&shared, &fin_a, &fin_b);
    let oracle_summary: Vec<String> = verdict.summary_lines(&divergences);
    // Harness-level violations (availability, final reads, node exits, and
    // below non-vacuity and node panics): the summary lists them beside the
    // oracle groups; the replica-convergence ones are already in there.
    let mut harness_only: Vec<String> = violations
        .iter()
        .filter(|v| !v.starts_with("[replica-convergence]"))
        .cloned()
        .collect();
    violations.extend(verdict.violations);

    let st = &shared.stats;
    let ok_writes = st.ok_writes.load(Ordering::Relaxed);
    eprintln!(
        "chaos[{}]: ok_writes={ok_writes} (txn {}) info_writes={} fail_writes={} ok_reads={} (txn {}) \
         info_reads={} eventual_reads={} history_entries={}",
        scn.name(),
        st.ok_txn_writes.load(Ordering::Relaxed),
        st.info_writes.load(Ordering::Relaxed),
        st.fail_writes.load(Ordering::Relaxed),
        st.ok_reads.load(Ordering::Relaxed),
        st.ok_txn_reads.load(Ordering::Relaxed),
        st.info_reads.load(Ordering::Relaxed),
        st.eventual_reads.load(Ordering::Relaxed),
        history.entries.len(),
    );
    if ok_writes < 50 {
        let v =
            format!("[non-vacuity] only {ok_writes} acknowledged writes: the workload barely ran");
        harness_only.push(v.clone());
        violations.push(v);
    }
    for i in 0..n {
        if let Ok(log) = std::fs::read_to_string(cluster.log_path(i)) {
            for line in log.lines().filter(|l| l.contains("panicked at")) {
                let v = format!("[node-panic] n{i}: {line}");
                harness_only.push(v.clone());
                violations.push(v);
            }
        }
    }

    // ---- artifacts on failure ---------------------------------------
    if !violations.is_empty() {
        let _ = std::fs::create_dir_all(&out_dir);
        let _ = std::fs::write(
            out_dir.join("history.json"),
            animus_test::export::to_json(&history),
        );
        let _ = std::fs::write(out_dir.join("events.txt"), events.join("\n"));
        let _ = std::fs::write(
            out_dir.join("op-trace.txt"),
            shared.trace.lock().expect("trace").join("\n"),
        );
        let _ = std::fs::write(out_dir.join("violations.txt"), violations.join("\n"));
        let _ = std::fs::write(out_dir.join("counters.txt"), counter_lines.join("\n"));
        // The compact summary: what the CI annotation shows first. One line
        // per violation group (never the value lists, which live in
        // violations.txt), the replica-convergence verdict, and every node's
        // counters, all inside the annotation's ~3,500-character cap.
        let mut kinds: BTreeMap<String, usize> = BTreeMap::new();
        for v in &violations {
            let kind = v.split(']').next().unwrap_or("?").trim_start_matches('[');
            *kinds.entry(kind.to_owned()).or_default() += 1;
        }
        let header = format!(
            "chaos[{}] seed={seed}: {} violation(s): {}",
            scn.name(),
            violations.len(),
            kinds
                .iter()
                .map(|(k, n)| format!("{k} x{n}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        // The replica-convergence verdict and the counters are always kept;
        // the violation groups fill whatever is left of the cap (the CI
        // annotation shows at most ~3,500 characters), so a run with many
        // distinct groups degrades to "first N groups + a count", never to a
        // summary that drops the decisive lines off the end.
        const SUMMARY_CAP: usize = 3300;
        let (mut keep, mut groups): (Vec<String>, Vec<String>) = (Vec::new(), Vec::new());
        for l in oracle_summary.into_iter().chain(
            harness_only
                .iter()
                .map(|v| v.chars().take(240).collect::<String>()),
        ) {
            if l.starts_with("[replica-convergence]") {
                keep.push(l);
            } else {
                groups.push(l);
            }
        }
        keep.extend(counter_lines.iter().map(|l| format!("counters {l}")));
        let mut budget = SUMMARY_CAP
            .saturating_sub(header.len() + keep.iter().map(|l| l.len() + 1).sum::<usize>() + 80);
        let mut shown = Vec::new();
        for (i, l) in groups.iter().enumerate() {
            if l.len() + 1 > budget {
                shown.push(format!(
                    "... {} more violation group(s); see violations.txt",
                    groups.len() - i
                ));
                break;
            }
            budget -= l.len() + 1;
            shown.push(l.clone());
        }
        let mut summary = vec![header];
        summary.extend(shown);
        summary.extend(keep);
        let _ = std::fs::write(out_dir.join("summary.txt"), summary.join("\n"));
        for l in &summary {
            eprintln!("chaos[{}]: SUMMARY {l}", scn.name());
        }
        for i in 0..n {
            let _ = std::fs::copy(cluster.log_path(i), out_dir.join(format!("n{i}.log")));
        }
        eprintln!(
            "chaos[{}]: FAILED seed={seed}; history, events and node logs saved under {}",
            scn.name(),
            out_dir.display()
        );
        for v in violations.iter().take(20) {
            eprintln!("chaos[{}]: VIOLATION {v}", scn.name());
        }
    }
    drop(cluster);
    Outcome { violations }
}

fn run(scn: Scenario) {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    let outcome = rt.block_on(run_scenario(scn));
    assert!(
        outcome.violations.is_empty(),
        "chaos scenario `{}` found {} violation(s); first: {}",
        scn.name(),
        outcome.violations.len(),
        outcome.violations.first().map_or("", String::as_str),
    );
}

#[test]
fn chaos_smoke() {
    run(Scenario::Smoke);
}

#[test]
fn chaos_kill() {
    run(Scenario::Kill);
}

#[test]
fn chaos_partition() {
    run(Scenario::Partition);
}

#[test]
fn chaos_pause() {
    run(Scenario::Pause);
}

#[test]
fn chaos_delay() {
    run(Scenario::Delay);
}

#[test]
fn chaos_mixed() {
    run(Scenario::Mixed);
}

// ---- disk full (issue #1221) ----------------------------------------------

fn storage_full(health: &serde_json::Value) -> bool {
    health["storage_full"].as_bool() == Some(true)
}

async fn admin_json(
    cluster: &ChaosCluster,
    i: usize,
    path: &str,
) -> Option<(u16, serde_json::Value)> {
    let (s, b) = http_get(cluster.admin_addr(i), path, Duration::from_secs(3))
        .await
        .ok()?;
    Some((s, serde_json::from_str(&b).ok()?))
}

async fn counter(cluster: &ChaosCluster, i: usize, name: &str) -> u64 {
    admin_json(cluster, i, "/admin/metrics")
        .await
        .and_then(|(_, v)| v["counters"][name].as_u64())
        .unwrap_or(0)
}

fn put_body(key: &str) -> String {
    serde_json::json!({
        "TableName": workload::TABLE,
        "Key": {"pk": {"S": key}, "sk": {"S": "s"}},
        "UpdateExpression": "SET v = :v",
        "ExpressionAttributeValues": {":v": {"N": "1"}},
    })
    .to_string()
}

/// One write of `key` through `node`: `(status, body)`, or the transport error.
async fn put(node: SocketAddr, key: &str) -> Result<(u16, String), String> {
    dynamo_call(node, "UpdateItem", &put_body(key), Duration::from_secs(10))
        .await
        .map_err(|e| e.to_string())
}

fn is_storage_full_refusal(r: &Result<(u16, String), String>) -> bool {
    matches!(r, Ok((503, b)) if b.contains("StorageFull"))
}

/// Retry a write of `key` through `node` until it is acknowledged.
async fn put_until_ok(node: SocketAddr, key: &str, budget: Duration) -> Result<Duration, String> {
    let t0 = tokio::time::Instant::now();
    let mut last = String::new();
    while t0.elapsed() < budget {
        match put(node, key).await {
            Ok((200, _)) => return Ok(t0.elapsed()),
            Ok((s, b)) => last = format!("{s} {b}"),
            Err(e) => last = e,
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!(
        "write of {key} via {node} not acknowledged in {budget:?}: {last}"
    ))
}

/// A read of `key` through `node` (eventually consistent unless `consistent`):
/// `(status, body)`, or the transport error.
async fn read_item(
    node: SocketAddr,
    key: &str,
    consistent: bool,
    budget: Duration,
) -> Result<(u16, String), String> {
    let body = serde_json::json!({
        "ConsistentRead": consistent,
        "TableName": workload::TABLE,
        "Key": {"pk": {"S": key}, "sk": {"S": "s"}},
    })
    .to_string();
    dynamo_call(node, "GetItem", &body, budget)
        .await
        .map_err(|e| e.to_string())
}

/// An eventually-consistent read of `key` through `node`: the HTTP status.
async fn eventual_read_status(node: SocketAddr, key: &str) -> Result<u16, String> {
    read_item(node, key, false, Duration::from_secs(10))
        .await
        .map(|(s, _)| s)
}

/// Poll `cond` every 250 ms until it holds or `budget` runs out.
async fn poll_until<F, Fut>(budget: Duration, mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let t0 = tokio::time::Instant::now();
    while t0.elapsed() < budget {
        if cond().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    false
}

async fn node_storage_full(cluster: &ChaosCluster, i: usize) -> Option<bool> {
    admin_json(cluster, i, "/admin/health")
        .await
        .map(|(_, v)| storage_full(&v))
}

/// Disk full on real filesystems (issue #1221, ADR 0074 D-7, B-1).
///
/// Each node's data dir lives on its own small tmpfs. Under a continuous
/// recorded workload: (1) fill ONE node's filesystem, assert it reports
/// `storage_full` while writes keep being acknowledged through the other
/// replicas (leader step-down, #1219) and reads are still served, then free
/// it and assert it recovers; (2) fill EVERY node's filesystem, assert named
/// 503 `StorageFull` refusals while reads are still served; (3) free
/// everything and assert writes resume on every node with no restart. The
/// oracles then run over the whole history.
async fn run_disk_full() -> Option<Outcome> {
    let name = "disk_full";
    let seed = env_u64("ANIMUS_CHAOS_SEED").unwrap_or_else(|| name_seed(name));
    let n = 3usize;
    let disk_mb = env_u64("ANIMUS_CHAOS_DISK_MB").unwrap_or(64);
    let base = std::env::var("ANIMUS_CHAOS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    std::fs::create_dir_all(&base).expect("chaos base dir");
    let scratch = tempfile::Builder::new()
        .prefix("animus-chaos-")
        .tempdir_in(&base)
        .expect("chaos scratch dir");
    let out_dir = std::env::var("ANIMUS_CHAOS_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| base.join("animus-chaos-out"))
        .join(format!("{name}-{seed}"));

    if let Err(why) = diskfull::mount_supported(scratch.path()) {
        let msg = format!(
            "chaos[{name}]: SKIPPED: this environment cannot mount a tmpfs ({why}); \
             needs CAP_SYS_ADMIN (root or passwordless sudo)"
        );
        assert!(
            std::env::var("ANIMUS_CHAOS_REQUIRE_MOUNT").map_or(true, |v| v.trim() != "1"),
            "{msg} and ANIMUS_CHAOS_REQUIRE_MOUNT=1"
        );
        eprintln!("{msg}");
        return None;
    }
    eprintln!("chaos[{name}]: seed={seed} nodes={n} per-node tmpfs={disk_mb}MiB");

    // One tmpfs per node; the data dir is a subdirectory so the ballast and
    // the node's own files share the one size budget.
    let mut mounts = Vec::new();
    let mut cluster = ChaosCluster::prepare(n, scratch.path(), seed).await;
    for i in 0..n {
        let m =
            Tmpfs::mount(&scratch.path().join(format!("mnt{i}")), disk_mb).expect("mount tmpfs");
        let data = m.path().join("data");
        std::fs::create_dir_all(&data).expect("data dir");
        cluster.set_data_dir(i, data);
        mounts.push(m);
    }
    cluster.start_all();
    let nodes: Vec<SocketAddr> = (0..n).map(|i| cluster.dynamo_addr(i)).collect();
    let mut events: Vec<String> = Vec::new();
    let mut violations: Vec<String> = Vec::new();

    workload::create_table(&nodes, 2, Duration::from_secs(120))
        .await
        .expect("bring-up: CreateTable");
    for (i, a) in nodes.iter().enumerate() {
        workload::probe_available(*a, 1000 + i as u64, Duration::from_secs(60))
            .await
            .expect("bring-up: every node serves");
    }
    // A key written while healthy, to read back while the disks are full.
    put_until_ok(nodes[0], "df-seed", Duration::from_secs(30))
        .await
        .expect("seed write");
    let pids: Vec<Option<u32>> = (0..n).map(|i| cluster.pid(i)).collect();

    // The multi-key transaction ops are ON by default (finding F-2 in
    // docs/chaos.md, a split cutting a txn record off its anchor's item, is
    // fixed); `ANIMUS_CHAOS_DISK_TXN=0` drops them.
    let mut sh0 = Shared::new(seed);
    sh0.txn_ops = std::env::var("ANIMUS_CHAOS_DISK_TXN").map_or(true, |v| v.trim() != "0");
    let shared = Arc::new(sh0);
    let clients: Vec<_> = (1..=CLIENTS)
        .map(|proc| {
            let sh = Arc::clone(&shared);
            let nodes = nodes.clone();
            tokio::spawn(async move { workload::client_loop(&sh, proc, nodes).await })
        })
        .collect();
    let t0 = tokio::time::Instant::now();
    let note = |events: &mut Vec<String>, what: String| {
        eprintln!("chaos[{name}]: t={:>3}s {what}", t0.elapsed().as_secs());
        events.push(format!("t={}s {what}", t0.elapsed().as_secs()));
    };
    tokio::time::sleep(Duration::from_secs(8)).await;

    // ---- phase 1: one node's disk full -------------------------------------
    note(&mut events, "phase 1: filling node 0's filesystem".into());
    let ballast = diskfull::fill(mounts[0].path()).expect("fill node 0");
    note(&mut events, format!("node 0 ballast {ballast} bytes"));
    let seen = poll_until(Duration::from_secs(60), || async {
        node_storage_full(&cluster, 0).await == Some(true)
    })
    .await;
    if !seen {
        violations.push(
            "[disk-full] node 0 never reported storage_full on /admin/health with its disk full"
                .into(),
        );
    }
    // The healthy replicas keep accepting writes (a full leader steps down).
    for (k, node) in [1usize, 2].iter().enumerate() {
        for j in 0..6 {
            let key = format!("df-one-{k}-{j}");
            if let Err(e) = put_until_ok(nodes[*node], &key, Duration::from_secs(40)).await {
                violations.push(format!("[disk-full/one-node] {e}"));
            }
        }
    }
    match eventual_read_status(nodes[0], "df-seed").await {
        Ok(200) => {}
        other => violations.push(format!(
            "[disk-full/one-node] read via the full node 0: {other:?}"
        )),
    }
    note(&mut events, "phase 1: freeing node 0".into());
    diskfull::free(mounts[0].path());
    let recovered = poll_until(Duration::from_secs(60), || async {
        node_storage_full(&cluster, 0).await == Some(false)
    })
    .await;
    if !recovered {
        violations.push(
            "[disk-full/one-node] node 0 still reports storage_full 60s after space was freed"
                .into(),
        );
    }
    if let Err(e) = put_until_ok(nodes[0], "df-one-recovered", Duration::from_secs(40)).await {
        violations.push(format!("[disk-full/one-node] {e}"));
    }

    // ---- phase 2: every node's disk full -----------------------------------
    note(&mut events, "phase 2: filling every filesystem".into());
    for m in &mounts {
        diskfull::fill(m.path()).expect("fill");
    }
    let mut refused = [0u32; 3];
    let mut other_answers: Vec<String> = Vec::new();
    let t_full = tokio::time::Instant::now();
    let mut round = 0u32;
    while t_full.elapsed() < Duration::from_secs(40) && refused.contains(&0) {
        for (i, node) in nodes.iter().enumerate() {
            let r = put(*node, &format!("df-full-{round}")).await;
            if is_storage_full_refusal(&r) {
                refused[i] += 1;
            } else if other_answers.len() < 6 {
                other_answers.push(format!("n{i}: {r:?}"));
            }
        }
        // Space a node frees for itself (a deleted WAL file) goes back in.
        if round % 8 == 7 {
            for m in &mounts {
                let _ = diskfull::fill(m.path());
            }
        }
        round += 1;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    note(
        &mut events,
        format!(
            "phase 2: StorageFull refusals per node {refused:?}; other answers {other_answers:?}"
        ),
    );
    // With every disk full the established tablet leaders keep leading (a full
    // follower keeps acking, frozen at its durable index, and a leader only
    // steps down to a successor that can win), so a write is refused with the
    // named 503 `StorageFull` rather than timing out (issue #1228; findings F-1
    // and F-3 in docs/chaos.md). Every node must have seen one: a node that is
    // not the leader forwards to it, and the leader refuses.
    for (i, r) in refused.iter().enumerate() {
        if *r == 0 {
            violations.push(format!(
                "[disk-full/all-nodes] node {i} never answered a write with a 503 StorageFull \
                 while every disk was full (other answers {other_answers:?})"
            ));
        }
    }
    if t_full.elapsed() > Duration::from_secs(30) {
        violations.push(format!(
            "[disk-full/all-nodes] the refusals took {}s: a write timed out instead of being \
             refused promptly",
            t_full.elapsed().as_secs()
        ));
    }
    let mut overload = 0;
    for i in 0..n {
        overload += counter(&cluster, i, "overload_storage_full").await;
    }
    if overload == 0 {
        violations.push("[disk-full/all-nodes] overload_storage_full never incremented".into());
    }
    // Reads of already-applied state while every disk is full continue, on every
    // node, eventual and linearizable: the leader keeps leading, a full follower
    // keeps acking (so ReadIndex confirms), and a full leader serves at its
    // committed floor instead of a ceiling it cannot commit (issue #1228).
    let mut read_ok = [0u32; 3];
    let mut read_tries = [0u32; 3];
    let mut strong_ok = [0u32; 3];
    for _ in 0..4 {
        for (i, node) in nodes.iter().enumerate() {
            read_tries[i] += 1;
            let r = read_item(*node, "df-seed", false, Duration::from_secs(3)).await;
            if matches!(&r, Ok((200, body)) if body.contains("\"v\"")) {
                read_ok[i] += 1;
            }
            let r = read_item(*node, "df-seed", true, Duration::from_secs(8)).await;
            if matches!(&r, Ok((200, body)) if body.contains("\"v\"")) {
                strong_ok[i] += 1;
            }
        }
    }
    note(
        &mut events,
        format!(
            "phase 2: reads served while every disk is full: eventual {read_ok:?}, consistent {strong_ok:?} of {read_tries:?}"
        ),
    );
    for i in 0..n {
        if read_ok[i] != read_tries[i] || strong_ok[i] != read_tries[i] {
            violations.push(format!(
                "[disk-full/all-nodes] node {i} served {}/{} eventual and {}/{} consistent reads of a \
                 written key while every disk was full",
                read_ok[i], read_tries[i], strong_ok[i], read_tries[i]
            ));
        }
    }

    // `ANIMUS_CHAOS_KEEP=1`: dump each node's per-group Raft view while every
    // disk is still full (who leads which tablet, terms, roles), the evidence
    // a "reads not served" or "no leader" failure needs.
    if std::env::var("ANIMUS_CHAOS_KEEP").is_ok_and(|v| v.trim() == "1") {
        let _ = std::fs::create_dir_all(&out_dir);
        for i in 0..n {
            if let Some((_, v)) = admin_json(&cluster, i, "/admin/raftkv").await {
                let _ = std::fs::write(out_dir.join(format!("raftkv-n{i}.json")), v.to_string());
            }
            if let Some((_, v)) = admin_json(&cluster, i, "/admin/metrics").await {
                let _ = std::fs::write(
                    out_dir.join(format!("metrics-n{i}.json")),
                    v["counters"].to_string(),
                );
            }
        }
    }

    // ---- phase 3: free everything, writes resume with no restart ----------
    note(&mut events, "phase 3: freeing every filesystem".into());
    for m in &mounts {
        diskfull::free(m.path());
    }
    let cleared = poll_until(Duration::from_secs(90), || async {
        for i in 0..n {
            if node_storage_full(&cluster, i).await != Some(false) {
                return false;
            }
        }
        true
    })
    .await;
    if !cleared {
        violations.push("[disk-full/recovery] storage_full did not clear on every node within 90s of freeing space".into());
    }
    for (i, a) in nodes.iter().enumerate() {
        if let Err(e) =
            put_until_ok(*a, &format!("df-recovered-{i}"), Duration::from_secs(60)).await
        {
            violations.push(format!("[disk-full/recovery] {e}"));
        }
    }
    let after: Vec<Option<u32>> = (0..n).map(|i| cluster.pid(i)).collect();
    if after != pids {
        violations.push(format!(
            "[disk-full/recovery] a node was restarted: pids {pids:?} -> {after:?}"
        ));
    }
    note(&mut events, "phase 3: writes resumed on every node".into());
    tokio::time::sleep(Duration::from_secs(5)).await;

    shared.stop.store(true, Ordering::Relaxed);
    for c in clients {
        let _ = c.await;
    }
    for (i, status) in cluster.unexpected_exits() {
        violations.push(format!("[node-exit] n{i} exited on its own: {status}"));
    }

    // ---- final reads + oracles ---------------------------------------------
    let recovery = Duration::from_secs(60);
    let mut fin_a: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    let mut fin_b: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for key in 0..KEYS {
        match workload::final_read(nodes[0], key, recovery).await {
            Ok(l) => {
                fin_a.insert(key, l);
            }
            Err(e) => violations.push(format!("[final-read] {e}")),
        }
        match workload::final_read(nodes[1], key, recovery).await {
            Ok(l) => {
                fin_b.insert(key, l);
            }
            Err(e) => violations.push(format!("[final-read] {e}")),
        }
    }
    let (history, verdict) = workload::run_oracles(&shared, &fin_a, &fin_b);
    violations.extend(verdict.violations);
    let ok_writes = shared.stats.ok_writes.load(Ordering::Relaxed);
    eprintln!(
        "chaos[{name}]: ok_writes={ok_writes} info_writes={} fail_writes={} ok_reads={} history_entries={}",
        shared.stats.info_writes.load(Ordering::Relaxed),
        shared.stats.fail_writes.load(Ordering::Relaxed),
        shared.stats.ok_reads.load(Ordering::Relaxed),
        history.entries.len(),
    );
    if ok_writes < 50 {
        violations.push(format!(
            "[non-vacuity] only {ok_writes} acknowledged writes: the workload barely ran"
        ));
    }
    for i in 0..n {
        if let Ok(log) = std::fs::read_to_string(cluster.log_path(i)) {
            for line in log.lines().filter(|l| l.contains("panicked at")) {
                violations.push(format!("[node-panic] n{i}: {line}"));
            }
        }
    }
    let keep_artifacts = std::env::var("ANIMUS_CHAOS_KEEP").is_ok_and(|v| v.trim() == "1");
    if !violations.is_empty() || keep_artifacts {
        let _ = std::fs::create_dir_all(&out_dir);
        let _ = std::fs::write(
            out_dir.join("history.json"),
            animus_test::export::to_json(&history),
        );
        let _ = std::fs::write(out_dir.join("events.txt"), events.join("\n"));
        let _ = std::fs::write(out_dir.join("violations.txt"), violations.join("\n"));
        for i in 0..n {
            let _ = std::fs::copy(cluster.log_path(i), out_dir.join(format!("n{i}.log")));
        }
        eprintln!(
            "chaos[{name}]: FAILED seed={seed}; artifacts under {}",
            out_dir.display()
        );
        for v in violations.iter().take(20) {
            eprintln!("chaos[{name}]: VIOLATION {v}");
        }
    }
    // Stop the nodes before the mounts go away.
    drop(cluster);
    drop(mounts);
    Some(Outcome { violations })
}

#[test]
fn chaos_disk_full() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    let Some(outcome) = rt.block_on(run_disk_full()) else {
        return;
    };
    assert!(
        outcome.violations.is_empty(),
        "chaos scenario `disk_full` found {} violation(s); first: {}",
        outcome.violations.len(),
        outcome.violations.first().map_or("", String::as_str),
    );
}
