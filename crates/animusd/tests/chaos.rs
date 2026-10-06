//! Real-cluster chaos (R-01 sub-track b): real `animusd` processes on
//! loopback, a continuous recorded DynamoDB-wire workload, real faults, and
//! the `animus-test` oracles over the recorded history. See `docs/chaos.md`.
//!
//! Opt-in: `cargo test -p animusd --features chaos --test chaos -- --test-threads=1`
//! (the `chaos` feature keeps this multi-minute run out of the per-push
//! gates). Knobs: `ANIMUS_CHAOS_SEED`, `ANIMUS_CHAOS_SECS`,
//! `ANIMUS_CHAOS_NODES`, `ANIMUS_CHAOS_TABLETS`, `ANIMUS_CHAOS_RECOVERY_SECS`,
//! `ANIMUS_CHAOS_DIR`, `ANIMUS_CHAOS_OUT`.
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

use chaos_support::cluster::ChaosCluster;
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
    violations.extend(workload::replica_convergence(&nodes, &fin_a, Duration::from_secs(20)).await);

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
