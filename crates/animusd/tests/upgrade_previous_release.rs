//! ADR 0073 Phase 3 (P3-E, D10 `ProdEnv` tier): a **real rolling upgrade from
//! the previous release's real bytes**.
//!
//! Phase 2's `BinaryProfile` corpus models gate discipline but never runs an
//! old binary. This test does: it starts a multi-node cluster of the
//! reference build **R-1** (`ANIMUS_UPGRADE_FROM_BIN`, built from the commit
//! or tag pinned in `scripts/upgrade-from.txt` by
//! `scripts/build-upgrade-from.sh`), runs the recorded DynamoDB-wire workload
//! of the chaos harness (`chaos_support`, the `animus-test` list-append model),
//! then rolls the cluster node by node onto the **current** build exactly as
//! `docs/runbook/upgrade.md` says, driving the roll with the real `animus
//! cluster roll plan/wait` CLI, and finally finalizes. It asserts:
//!
//! * **no acked write is lost** and the history is cycle-free
//!   (`check_durability`/`check_cycles`/`check_convergence` over the recorded
//!   history plus the converged final state; an op that may have been sent and
//!   got no clean answer is `info`, never `fail`);
//! * **availability within client retries**: the longest stretch with no
//!   acknowledged write across the whole roll stays under [`STALL_BOUND`];
//! * the roll converges: every `roll wait` succeeds, the version era becomes
//!   active, `can_finalize` becomes true, `roll.phase` is `ready_to_finalize`,
//!   `animus cluster finalize` succeeds and every node observes the new
//!   cluster version;
//! * a whole-cluster restart on the current build afterwards serves the same
//!   data (D10's "then restart-all on R").
//!
//! It also **measures D4** (ADR 0073 Phase 3, maintainer decision 2: measure
//! before building a maintenance mark): per roll step, how many tablets had
//! their replica set changed and how many snapshots were installed/shipped
//! (replica rebuild traffic), read from `/admin/status` and `/admin/metrics`.
//! It is reported (stderr, plus `<ANIMUS_UPGRADE_FROM_REPORT_DIR>/<variant>.json`),
//! never asserted: a repair is correct, only potentially wasteful. A 3-node
//! RF-3 cluster has no spare candidate (nothing can move); the 4-node variant
//! has one, which is where the churn can show.
//!
//! Variants (each a separate test, run one at a time):
//!
//! * `clean_3_nodes`: SIGTERM, leadership transfer first (the runbook as written);
//! * `spare_node_repair_churn_4_nodes`: the same on 4 nodes (D4 measurement);
//! * `kill_the_control_leader_3_nodes`: the control leader is SIGKILLed instead
//!   of transferred+stopped (an unclean stop at the roll's point of no return);
//! * `torn_wal_tail_3_nodes`: every node is SIGKILLed and a partial record is
//!   appended to its control and shared WAL before it restarts on the new
//!   binary (a power cut mid-append), so the new binary's recovery reads
//!   the old binary's torn files.
//!
//! **Known findings** (against the pinned R-1 `ac57d56a`, 2026-10-05; all are
//! reproduced, intermittently, by `ANIMUS_UPGRADE_FROM_TXN=1`, which is why the
//! workload runs without multi-key transactions by default):
//!
//! 0. A legacy (v1, tag 1) intent written by an R-1 node and still unresolved
//!    when the new binary resolves it carries no `prior`, so an abort falls
//!    back to the old lookback and can tombstone an acknowledged committed
//!    value: acked writes lost after a roll (seen as `[durability]` in the
//!    SIGKILL variant and `[restart-all]` data loss in the clean one).
//! 1. `ac57d56a` itself loses acknowledged writes under transactions plus
//!    replica repair (an aborted intent tombstones the committed value it
//!    shadows once a snapshot shipped only the latest record), fixed on `main`
//!    by `efcaa6cb` (ADR 0018 section 2, 2026-10-04). It reproduces with
//!    `ANIMUS_UPGRADE_FROM_CONTROL=same-binary` (no binary change at all).
//! 2. That fix introduced `txn-envelope` v2 (intent tag 2), which the current
//!    build writes **ungated**: an R-1 replica that receives an engine image
//!    from an upgraded node (a repair snapshot) panics in its apply task with
//!    `txn: unknown envelope tag 2 (corrupt engine value)`
//!    (`animus-cp-data/src/txn.rs`, R-1's line 835), after which reads of its
//!    groups stall or diverge. A rolling upgrade across that format needs a gate.
//!
//! **A missing reference binary FAILS this test** (it is feature-gated, so a
//! job that selects it cannot silently match nothing, and the gate itself
//! panics rather than skipping). Locally: `scripts/build-upgrade-from.sh`
//! prints the path to export as `ANIMUS_UPGRADE_FROM_BIN`.
//!
//! `ProdEnv`: real processes, real time. Every wait is converged-or-timeout
//! and the whole test is timeout-guarded; a failure is a real bug, never a
//! wider timeout.

#![cfg(feature = "upgrade-from")]

#[allow(
    dead_code,
    reason = "shared with chaos.rs and soak.rs; this test uses only part of it"
)]
mod chaos_support;
mod support;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chaos_support::client::http_get;
use chaos_support::workload::{self, CLIENTS, KEYS, Shared};
use serde_json::{Value, json};

/// The build under test (this checkout).
const CURRENT_BIN: &str = env!("CARGO_BIN_EXE_animusd");

/// No acknowledged write for longer than this, at any point of the roll, is an
/// availability failure (a leader election, a restart and a catch-up are all
/// well inside it; the workload's clients do not retry, so a stall here means
/// every node refused or timed out for this long).
const STALL_BOUND: Duration = Duration::from_secs(30);
/// One roll step's `animus cluster roll wait` budget (the CLI default).
const WAIT_TIMEOUT: &str = "5m";
/// How long a SIGTERMed node may take to exit (a pod's grace period is 90 s).
const STOP_DEADLINE: Duration = Duration::from_secs(60);
/// The whole test.
const TEST_DEADLINE: Duration = Duration::from_secs(1200);
/// Tablets of the workload table (provisioned so the ADR 0067 loop splits it).
const TABLETS: u64 = 4;

/// One test at a time: they share the process tree budget and port space.
static SERIAL: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Variant {
    Clean,
    KillLeader,
    TornTail,
}

impl Variant {
    fn name(self, nodes: usize) -> String {
        match self {
            Variant::Clean if nodes == 3 => "clean_3_nodes".into(),
            Variant::Clean => format!("spare_node_repair_churn_{nodes}_nodes"),
            Variant::KillLeader => format!("kill_the_control_leader_{nodes}_nodes"),
            Variant::TornTail => format!("torn_wal_tail_{nodes}_nodes"),
        }
    }
}

// ---- the reference environment ---------------------------------------------

struct Refs {
    prev_bin: PathBuf,
    prev_ref: String,
    cli: PathBuf,
}

/// Resolve the reference binary and the CLI, or **panic**: a missing R-1 must
/// never turn this test into a silent pass.
fn refs() -> Refs {
    let prev_bin = std::env::var("ANIMUS_UPGRADE_FROM_BIN").unwrap_or_else(|_| {
        panic!(
            "ANIMUS_UPGRADE_FROM_BIN is not set. This test needs the previous release's real \
             `animusd` binary (ADR 0073 Phase 3, D10) and refuses to skip: run \
             `scripts/build-upgrade-from.sh` and export the path it prints."
        )
    });
    let prev_bin = PathBuf::from(prev_bin);
    assert!(
        prev_bin.is_file(),
        "ANIMUS_UPGRADE_FROM_BIN={} is not a file",
        prev_bin.display()
    );
    let cli = std::env::var("ANIMUS_CLI_BIN").map_or_else(
        |_| {
            Path::new(CURRENT_BIN)
                .parent()
                .expect("animusd binary has a parent dir")
                .join("animus")
        },
        PathBuf::from,
    );
    assert!(
        cli.is_file(),
        "the `animus` CLI binary is missing at {} (build it with `cargo build -p animus-cli`, or \
         point ANIMUS_CLI_BIN at it)",
        cli.display()
    );
    let prev_ref = std::env::var("ANIMUS_UPGRADE_FROM_REF").unwrap_or_else(|_| "(unset)".into());
    Refs {
        prev_bin,
        prev_ref,
        cli,
    }
}

// ---- a multi-process cluster -----------------------------------------------

struct NodeAddrs {
    id: String,
    dynamo: SocketAddr,
    admin: SocketAddr,
}

struct RollCluster {
    root: PathBuf,
    cfg: PathBuf,
    nodes: Vec<NodeAddrs>,
    children: Vec<Option<Child>>,
}

impl RollCluster {
    /// A config generated by the **reference** binary (so R-1 certainly parses
    /// it) with every port replaced by a reserved free one.
    fn prepare(n: usize, root: &Path, prev_bin: &Path) -> Self {
        let out = Command::new(prev_bin)
            .args(["gen-config", "--nodes", &n.to_string()])
            .output()
            .expect("run the reference animusd gen-config");
        assert!(out.status.success(), "reference gen-config failed");
        let mut cfg: Value = serde_json::from_slice(&out.stdout).expect("gen-config json");
        let ports = support::free_addrs(6 * n);
        let mut nodes = Vec::new();
        for (i, entry) in cfg["nodes"]
            .as_array_mut()
            .expect("nodes")
            .iter_mut()
            .enumerate()
        {
            for (k, key) in ["internal", "client", "dynamo", "admin", "intra", "console"]
                .iter()
                .enumerate()
            {
                entry[*key] = Value::String(ports[6 * i + k].to_string());
            }
            nodes.push(NodeAddrs {
                id: entry["id"].as_str().expect("id").to_string(),
                dynamo: ports[6 * i + 2],
                admin: ports[6 * i + 3],
            });
        }
        let path = root.join("cluster.json");
        std::fs::write(&path, serde_json::to_vec_pretty(&cfg).expect("cfg")).expect("write cfg");
        std::fs::create_dir_all(root.join("logs")).expect("logs dir");
        Self {
            root: root.to_path_buf(),
            cfg: path,
            children: (0..n).map(|_| None).collect(),
            nodes,
        }
    }

    fn n(&self) -> usize {
        self.nodes.len()
    }

    fn log_path(&self, i: usize) -> PathBuf {
        self.root.join("logs").join(format!("n{i}.log"))
    }

    fn data_dir(&self, i: usize) -> PathBuf {
        self.root.join(format!("data{i}"))
    }

    fn index_of(&self, id: &str) -> usize {
        self.nodes
            .iter()
            .position(|n| n.id == id)
            .unwrap_or_else(|| panic!("unknown node id {id}"))
    }

    /// Start (or restart on the same data dir) node `i` with `bin`.
    fn start(&mut self, i: usize, bin: &Path) {
        assert!(self.children[i].is_none(), "node {i} already running");
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log_path(i))
            .expect("open node log");
        let err = log.try_clone().expect("clone log fd");
        let child = Command::new(bin)
            .arg("--config")
            .arg(&self.cfg)
            .arg("--node")
            .arg(i.to_string())
            .arg("--dir")
            .arg(self.data_dir(i))
            .env("RUST_BACKTRACE", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(err))
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {}: {e}", bin.display()));
        self.children[i] = Some(child);
    }

    /// SIGTERM and wait for a clean exit. Returns how long the stop took.
    fn stop_graceful(&mut self, i: usize) -> Duration {
        let t0 = Instant::now();
        let mut child = self.children[i].take().expect("node is running");
        let _ = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status();
        loop {
            match child.try_wait().expect("try_wait") {
                Some(status) => {
                    assert!(
                        status.success() || t0.elapsed() < STOP_DEADLINE,
                        "node {i} exited with {status} after SIGTERM"
                    );
                    return t0.elapsed();
                }
                None if t0.elapsed() > STOP_DEADLINE => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!(
                        "node {i} did not exit within {STOP_DEADLINE:?} of SIGTERM (graceful \
                         shutdown hang)\n{}",
                        self.log_tail(i)
                    );
                }
                None => std::thread::sleep(Duration::from_millis(100)),
            }
        }
    }

    /// `kill -9` and reap.
    fn kill9(&mut self, i: usize) {
        if let Some(mut c) = self.children[i].take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// Append a partial record (a tear mid-append) to every control and shared
    /// WAL file of node `i`. Returns the files torn.
    fn tear_wal_tails(&self, i: usize) -> Vec<PathBuf> {
        fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
            let Ok(rd) = std::fs::read_dir(dir) else {
                return;
            };
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, out);
                } else if matches!(
                    p.file_name().and_then(|n| n.to_str()),
                    Some("raft.wal" | "raftkv.wal.shared")
                ) {
                    out.push(p);
                }
            }
        }
        let mut files = Vec::new();
        walk(&self.data_dir(i), &mut files);
        for f in &files {
            use std::io::Write as _;
            let mut h = std::fs::OpenOptions::new()
                .append(true)
                .open(f)
                .expect("open wal for tearing");
            h.write_all(&[0xFF; 13]).expect("tear wal tail");
        }
        files
    }

    fn log_tail(&self, i: usize) -> String {
        let text = std::fs::read_to_string(self.log_path(i)).unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        let from = lines.len().saturating_sub(40);
        format!("---- node {i} log tail ----\n{}", lines[from..].join("\n"))
    }

    async fn admin_json(&self, i: usize, path: &str) -> Option<Value> {
        match http_get(self.nodes[i].admin, path, Duration::from_secs(5)).await {
            Ok((200, body)) => serde_json::from_str(&body).ok(),
            _ => None,
        }
    }

    /// The control leader's index, per any live node.
    async fn control_leader(&self) -> Option<usize> {
        for i in 0..self.n() {
            if self.children[i].is_none() {
                continue;
            }
            if let Some(v) = self.admin_json(i, "/admin/raft").await
                && let Some(l) = v["leader"].as_str()
                && self.nodes.iter().any(|n| n.id == l)
            {
                return Some(self.index_of(l));
            }
        }
        None
    }
}

impl Drop for RollCluster {
    fn drop(&mut self) {
        let panicking = std::thread::panicking();
        for i in 0..self.children.len() {
            if panicking {
                eprintln!("{}", self.log_tail(i));
            }
            if let Some(c) = self.children[i].as_mut() {
                let _ = c.kill();
                let _ = c.wait();
            }
        }
    }
}

// ---- the CLI ----------------------------------------------------------------

/// Run `animus <args>`; `(success, stdout, stderr)`.
async fn cli(bin: &Path, args: &[&str]) -> (bool, String, String) {
    let bin = bin.to_path_buf();
    let args: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
    tokio::task::spawn_blocking(move || {
        let out = Command::new(&bin).args(&args).output().expect("run animus");
        (
            out.status.success(),
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    })
    .await
    .expect("cli task")
}

// ---- D4 measurement -----------------------------------------------------------

const COUNTERS: [&str; 6] = [
    "cp_snapshot_installs",
    "cp_snapshot_ships",
    "cp_reconfigure_accepted",
    "cp_engine_rebuilt",
    "cp_engine_needs_snapshot",
    "control_snapshot_installs",
];

type Tablets = BTreeMap<u64, (u64, Vec<String>)>;

struct Snap {
    /// Per live node.
    counters: BTreeMap<usize, BTreeMap<String, u64>>,
    tablets: Tablets,
}

async fn snapshot(c: &RollCluster) -> Snap {
    let mut counters = BTreeMap::new();
    let mut tablets = Tablets::new();
    for i in 0..c.n() {
        if c.children[i].is_none() {
            continue;
        }
        if let Some(m) = c.admin_json(i, "/admin/metrics").await {
            let map = COUNTERS
                .iter()
                .map(|k| ((*k).to_string(), m["counters"][*k].as_u64().unwrap_or(0)))
                .collect();
            counters.insert(i, map);
        }
        if tablets.is_empty()
            && let Some(s) = c.admin_json(i, "/admin/status").await
            && let Some(map) = s["tablets"].as_object()
        {
            for (id, t) in map {
                let mut reps: Vec<String> = t["replicas"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|r| r.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                reps.sort();
                if let Ok(id) = id.parse() {
                    tablets.insert(id, (t["epoch"].as_u64().unwrap_or(0), reps));
                }
            }
        }
    }
    Snap { counters, tablets }
}

/// One step's repair churn. The restarted node's counters reset with its
/// process, so its contribution is its absolute value after the restart.
fn churn(before: &Snap, after: &Snap, restarted: usize) -> Value {
    let mut sums = serde_json::Map::new();
    for k in COUNTERS {
        let mut total = 0u64;
        for (node, a) in &after.counters {
            let a = a[k];
            total += if *node == restarted {
                a
            } else {
                a.saturating_sub(before.counters.get(node).map_or(0, |b| b[k]))
            };
        }
        sums.insert(k.to_string(), json!(total));
    }
    let mut replica_set_changes = 0u64;
    let mut epoch_delta = 0u64;
    for (id, (epoch, reps)) in &after.tablets {
        if let Some((e0, r0)) = before.tablets.get(id) {
            if r0 != reps {
                replica_set_changes += 1;
            }
            epoch_delta += epoch.saturating_sub(*e0);
        }
    }
    json!({
        "tablets": after.tablets.len(),
        "tablets_replica_set_changed": replica_set_changes,
        "tablet_epoch_delta": epoch_delta,
        "counters": Value::Object(sums),
    })
}

// ---- the roll ------------------------------------------------------------------

async fn poll_until<T, F, Fut>(what: &str, budget: Duration, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    let t0 = Instant::now();
    let mut last;
    loop {
        match f().await {
            Ok(v) => return v,
            Err(e) => last = e,
        }
        assert!(
            t0.elapsed() < budget,
            "{what}: not converged within {budget:?}; last: {last}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn run(variant: Variant, nodes: usize) {
    let refs = refs();
    let name = variant.name(nodes);
    let dir = support::panic_safe_tempdir();
    let seed = chaos_support::rng::name_seed(&name);
    eprintln!(
        "upgrade[{name}]: R-1 = {} ({}), current = {CURRENT_BIN}, cli = {}",
        refs.prev_bin.display(),
        refs.prev_ref,
        refs.cli.display()
    );
    let result = tokio::time::timeout(
        TEST_DEADLINE,
        roll_scenario(variant, nodes, &name, seed, &refs, dir.path()),
    )
    .await;
    assert!(
        result.is_ok(),
        "upgrade[{name}]: exceeded {TEST_DEADLINE:?}"
    );
}

#[allow(
    clippy::too_many_lines,
    reason = "one linear scenario, read top to bottom"
)]
async fn roll_scenario(
    variant: Variant,
    n: usize,
    name: &str,
    seed: u64,
    refs: &Refs,
    root: &Path,
) {
    let current = PathBuf::from(CURRENT_BIN);
    // `ANIMUS_UPGRADE_FROM_RESTART_GAP_SECS` overrides; the spare-node
    // measurement variant defaults to a gap past `REPAIR_DWELL` (5 s).
    let gap = Duration::from_secs(
        std::env::var("ANIMUS_UPGRADE_FROM_RESTART_GAP_SECS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(if variant == Variant::Clean && n > 3 {
                12
            } else {
                0
            }),
    );
    // Diagnostic controls (triage aid, not run in CI): the same roll with no
    // binary change at all, to tell a mixed-version defect from a restart or
    // repair defect. `same-binary`: every restart is on R-1; `current-only`:
    // the cluster starts on the current build. Both skip the CLI/era/finalize.
    let control = std::env::var("ANIMUS_UPGRADE_FROM_CONTROL").ok();
    let (start_bin, roll_bin) = match control.as_deref() {
        None => (refs.prev_bin.clone(), current.clone()),
        Some("same-binary") => (refs.prev_bin.clone(), refs.prev_bin.clone()),
        Some("current-only") => (current.clone(), current.clone()),
        Some(other) => panic!("ANIMUS_UPGRADE_FROM_CONTROL={other}: want same-binary|current-only"),
    };
    let mut c = RollCluster::prepare(n, root, &refs.prev_bin);
    for i in 0..n {
        c.start(i, &start_bin);
    }
    let dynamo: Vec<SocketAddr> = c.nodes.iter().map(|x| x.dynamo).collect();

    // ---- bring-up on R-1 ----------------------------------------------------
    poll_until("R-1 cluster healthy", Duration::from_secs(120), || async {
        for i in 0..n {
            match c.admin_json(i, "/admin/health").await {
                Some(h) if h["ok"] == true => {}
                other => return Err(format!("node {i} health: {other:?}")),
            }
        }
        Ok(())
    })
    .await;
    workload::create_table(&dynamo, TABLETS, Duration::from_secs(120))
        .await
        .expect("bring-up: CreateTable");
    poll_until(
        "workload table splits into its tablets",
        Duration::from_secs(120),
        || async {
            let have = tablet_count(&c).await;
            if have >= TABLETS as usize {
                Ok(())
            } else {
                Err(format!("{have} of {TABLETS} tablets"))
            }
        },
    )
    .await;
    for (i, a) in dynamo.iter().enumerate() {
        workload::probe_available(*a, 1000 + i as u64, Duration::from_secs(60))
            .await
            .expect("bring-up: every R-1 node serves");
    }

    // ---- workload ---------------------------------------------------------------
    // Multi-key transactions (2PC intents) are OFF by default: against the
    // pinned `ac57d56a` an in-flight intent across the roll hits the known
    // findings in the module doc (an acked write lost, or an R-1 replica
    // panic), which would make this job red for reasons that are not the roll's
    // mechanics. `ANIMUS_UPGRADE_FROM_TXN=1` turns them on (CI runs one such
    // variant as an informational step); flip this default once the findings are
    // fixed or R-1 moves to a release that has the fix.
    let txns = std::env::var("ANIMUS_UPGRADE_FROM_TXN").is_ok_and(|v| v.trim() == "1");
    if !txns {
        eprintln!(
            "upgrade[{name}]: multi-key transactions are OFF (see the module doc, \"Known \
             findings\"); ANIMUS_UPGRADE_FROM_TXN=1 turns them on"
        );
    }
    let mut workload_state = Shared::with_base(seed, 0, (10, 30));
    workload_state.txn_ops = txns;
    let shared = Arc::new(workload_state);
    let clients: Vec<_> = (1..=CLIENTS)
        .map(|proc| {
            let sh = Arc::clone(&shared);
            let nodes = dynamo.clone();
            tokio::spawn(async move { workload::client_loop(&sh, proc, nodes).await })
        })
        .collect();
    // The longest stretch with no acknowledged write.
    let stall = {
        let sh = Arc::clone(&shared);
        tokio::spawn(async move {
            let (mut last_ok, mut last_at) = (0u64, Instant::now());
            let mut worst = Duration::ZERO;
            while !sh.stop.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(250)).await;
                let ok = sh.stats.ok_writes.load(Ordering::Relaxed);
                if ok > last_ok {
                    worst = worst.max(last_at.elapsed());
                    last_ok = ok;
                    last_at = Instant::now();
                }
            }
            worst.max(last_at.elapsed())
        })
    };
    // Let the workload settle (and any initial repair finish) before step 1.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let baseline = snapshot(&c).await;

    // ---- the roll, as the runbook says --------------------------------------------
    let mut steps: Vec<Value> = Vec::new();
    // The plan is taken ONCE, over the Phase-1 cluster, and followed in order.
    // It cannot be re-asked mid-roll: until the version era starts (after the
    // last member is on the new binary) no node has a recorded range, so a
    // second `roll plan` over a half-rolled R-1 -> R cluster still lists every
    // node as "old" (see the report: `plan` is not re-entrant for the first
    // Phase 1 -> Phase 2 roll). `roll wait` is judged from the restarted node's
    // own range and does work step by step.
    let admin0 = c.nodes[0].admin.to_string();
    let order: Vec<Value> = if control.is_some() {
        // No plan to take: followers in index order, node 0 last.
        (1..n)
            .chain(std::iter::once(0))
            .map(|i| json!({"action": "restart", "node": c.nodes[i].id}))
            .collect()
    } else {
        let plan: Value = poll_until("roll plan accepted", Duration::from_secs(120), || async {
            let (_, out, err) =
                cli(&refs.cli, &["cluster", "roll", "plan", &admin0, "--json"]).await;
            let p: Value = serde_json::from_str(out.trim())
                .map_err(|e| format!("plan output is not JSON ({e}): {out} {err}"))?;
            if p["ok"] != true {
                return Err(format!("plan refused: {}", p["refused"]));
            }
            Ok(p)
        })
        .await;
        let order = plan["steps"].as_array().expect("plan steps").clone();
        assert!(
            !order.is_empty(),
            "the plan over R-1 has nothing to roll: {plan}"
        );
        order
    };
    eprintln!("upgrade[{name}]: PLAN {}", Value::Array(order.clone()));
    let mut transfer_from: Option<String> = None;
    for first in &order {
        if first["action"] == "transfer_control_leadership" {
            let from = first["from"].as_str().expect("from").to_string();
            let to = first["to"].as_str().expect("transfer target").to_string();
            transfer_from = Some(from.clone());
            // The leader may already have moved on its own; transfer only if not.
            let still_leader = c.control_leader().await.map(|l| c.nodes[l].id == from);
            if variant != Variant::KillLeader && still_leader == Some(true) {
                let from_admin = c.nodes[c.index_of(&from)].admin.to_string();
                let (ok, out, err) =
                    cli(&refs.cli, &["admin", "control-transfer", &from_admin, &to]).await;
                assert!(ok, "control-transfer {from} -> {to} failed: {out} {err}");
                poll_until(
                    "a control leader other than the old one",
                    Duration::from_secs(60),
                    || async {
                        match c.control_leader().await {
                            Some(l) if c.nodes[l].id != from => Ok(()),
                            other => Err(format!("leader {other:?}")),
                        }
                    },
                )
                .await;
            }
            continue;
        }
        assert_eq!(first["action"], "restart", "unexpected plan step: {first}");
        let id = first["node"].as_str().expect("node").to_string();
        let i = c.index_of(&id);
        let was_leader = transfer_from.as_deref() == Some(id.as_str());

        let before = snapshot(&c).await;
        let t0 = Instant::now();
        let how = match variant {
            Variant::Clean => {
                c.stop_graceful(i);
                "SIGTERM"
            }
            Variant::KillLeader if was_leader => {
                c.kill9(i);
                "SIGKILL (control leader, no transfer)"
            }
            Variant::KillLeader => {
                c.stop_graceful(i);
                "SIGTERM"
            }
            Variant::TornTail => {
                c.kill9(i);
                let torn = c.tear_wal_tails(i);
                assert!(!torn.is_empty(), "no WAL file found to tear for node {i}");
                "SIGKILL + torn WAL tail"
            }
        };
        // A slow restart (a pod's termination grace + image pull + boot) keeps
        // the node down past placement repair's dwell; this is the gap that
        // lets D4's rebuild traffic show.
        tokio::time::sleep(gap).await;
        let down = t0.elapsed();
        c.start(i, &roll_bin);
        let node_admin = c.nodes[i].admin.to_string();
        if control.is_some() {
            poll_until(
                "restarted node healthy",
                Duration::from_secs(120),
                || async {
                    match c.admin_json(i, "/admin/health").await {
                        Some(h) if h["ok"] == true => Ok(()),
                        other => Err(format!("health: {other:?}")),
                    }
                },
            )
            .await;
            tokio::time::sleep(Duration::from_secs(6)).await;
        } else {
            let (ok, out, err) = cli(
                &refs.cli,
                &[
                    "cluster",
                    "roll",
                    "wait",
                    &node_admin,
                    "--node",
                    &id,
                    "--timeout",
                    WAIT_TIMEOUT,
                ],
            )
            .await;
            assert!(ok, "roll wait {id} failed: {out} {err}\n{}", c.log_tail(i));
        }
        let healthy_after = t0.elapsed();
        let after = snapshot(&c).await;
        let mut row = churn(&before, &after, i);
        row["node"] = json!(id);
        row["stop"] = json!(how);
        row["control_leader"] = json!(was_leader);
        row["stopped_ms"] = json!(down.as_millis() as u64);
        row["restart_gap_ms"] = json!(gap.as_millis() as u64);
        row["restart_to_healthy_ms"] = json!(healthy_after.saturating_sub(down).as_millis() as u64);
        eprintln!("upgrade[{name}]: step {id}: {row}");
        steps.push(row);
        // Let repair settle so its traffic is attributed to this step, and
        // give the workload a window on the new mix before the next one.
        tokio::time::sleep(Duration::from_secs(8)).await;
    }

    let target: u64 = if control.is_some() {
        0
    } else {
        // ---- era, finalize ---------------------------------------------------------------
        poll_until(
            "era active and ready to finalize on every node",
            Duration::from_secs(180),
            || async {
                for i in 0..n {
                    let Some(v) = c.admin_json(i, "/admin/cluster-version").await else {
                        return Err(format!("node {i}: no cluster-version"));
                    };
                    if v["era_active"] != true || v["can_finalize"] != true {
                        return Err(format!("node {i}: {v}"));
                    }
                    if v["roll"]["phase"] != "ready_to_finalize" {
                        return Err(format!("node {i}: roll phase {}", v["roll"]["phase"]));
                    }
                }
                Ok(())
            },
        )
        .await;
        let leader = c
            .control_leader()
            .await
            .expect("a control leader before finalize");
        let target_v = c
            .admin_json(leader, "/admin/cluster-version")
            .await
            .expect("cluster-version")["safe_target"]
            .as_u64()
            .expect("safe_target");
        let leader_admin = c.nodes[leader].admin.to_string();
        let (ok, out, err) = cli(&refs.cli, &["cluster", "finalize", &leader_admin, "--yes"]).await;
        assert!(ok, "finalize failed: {out} {err}");
        poll_until(
            "every node observes the finalized version",
            Duration::from_secs(60),
            || async {
                for i in 0..n {
                    let v = c.admin_json(i, "/admin/cluster-version").await;
                    if v.as_ref().map(|v| &v["active"]) != Some(&json!(target_v)) {
                        return Err(format!("node {i}: {v:?}"));
                    }
                }
                Ok(())
            },
        )
        .await;
        target_v
    };
    // The workload runs on across the finalize.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let end = snapshot(&c).await;

    // ---- stop the workload, converge, check ------------------------------------------
    shared.stop.store(true, Ordering::Relaxed);
    for cl in clients {
        let _ = cl.await;
    }
    let worst_stall = stall.await.expect("stall tracker");
    let recovery = Duration::from_secs(60);
    for (i, a) in dynamo.iter().enumerate() {
        workload::probe_available(*a, i as u64, recovery)
            .await
            .unwrap_or_else(|e| panic!("after the roll: {e}"));
    }
    let mut violations: Vec<String> = Vec::new();
    let (fin_a, fin_b) = final_state(&dynamo, recovery, &mut violations).await;
    // A failed final read leaves its key absent, which the oracles would
    // misreport as lost acks: when a read failed, that failure is the finding.
    let (history, verdict) = workload::run_oracles(&shared, &fin_a, &fin_b);
    if violations.is_empty() {
        violations.extend(verdict.violations);
    }
    let ok_writes = shared.stats.ok_writes.load(Ordering::Relaxed);
    if ok_writes < 200 {
        violations.push(format!(
            "[non-vacuity] only {ok_writes} acknowledged writes"
        ));
    }
    if worst_stall > STALL_BOUND {
        violations.push(format!(
            "[availability] no write was acknowledged for {worst_stall:?} (bound {STALL_BOUND:?})"
        ));
    }

    // ---- restart everything on the current build ----------------------------------------
    for i in 0..n {
        c.stop_graceful(i);
    }
    for i in 0..n {
        c.start(i, &current);
    }
    poll_until(
        "whole-cluster restart on the current build",
        Duration::from_secs(120),
        || async {
            for i in 0..n {
                match c.admin_json(i, "/admin/health").await {
                    Some(h) if h["ok"] == true => {}
                    other => return Err(format!("node {i} health: {other:?}")),
                }
                if target != 0 {
                    let v = c.admin_json(i, "/admin/cluster-version").await;
                    if v.as_ref().map(|v| &v["active"]) != Some(&json!(target)) {
                        return Err(format!("node {i} cluster-version: {v:?}"));
                    }
                }
            }
            Ok(())
        },
    )
    .await;
    for (i, a) in dynamo.iter().enumerate() {
        workload::probe_available(*a, 2000 + i as u64, recovery)
            .await
            .unwrap_or_else(|e| panic!("after the whole-cluster restart: {e}"));
    }
    let mut again = Vec::new();
    let (fin_c, _) = final_state(&dynamo, recovery, &mut again).await;
    violations.extend(again);
    // The state can only have grown: a write the client gave up on (`info`,
    // timed out mid-election) may legitimately commit after the workload
    // stopped, so the check is "everything seen before is still there, in
    // order, and anything new is a write this workload really issued".
    let issued: std::collections::BTreeSet<u64> = history
        .entries
        .iter()
        .filter(|e| e.outcome == animus_test::Outcome::Invoke)
        .flat_map(|e| e.mops.iter())
        .filter_map(|m| match m {
            animus_test::history::Mop::Append { value, .. } => Some(*value),
            animus_test::history::Mop::Read { .. } => None,
        })
        .collect();
    let mut late = 0usize;
    for (k, before) in &fin_a {
        let Some(after) = fin_c.get(k).map(Vec::as_slice) else {
            continue; // its read failed; already reported
        };
        if !after.starts_with(before) {
            violations.push(format!(
                "[restart-all] key {k}: the data after the whole-cluster restart lost or reordered \
                 what was there before it (before {before:?}, after {after:?})"
            ));
            continue;
        }
        for v in &after[before.len()..] {
            late += 1;
            if !issued.contains(v) {
                violations.push(format!(
                    "[restart-all] key {k}: value {v} appeared after the restart and was never written"
                ));
            }
        }
    }
    if late > 0 {
        eprintln!(
            "upgrade[{name}]: {late} previously unacknowledged write(s) committed after the workload stopped"
        );
    }
    for i in 0..n {
        if let Ok(log) = std::fs::read_to_string(c.log_path(i)) {
            for line in log.lines().filter(|l| l.contains("panicked at")) {
                violations.push(format!("[node-panic] n{i}: {line}"));
            }
        }
    }

    // ---- report -------------------------------------------------------------------------
    // Counters reset with each restarted process, so the roll's total is the
    // sum of the per-step figures; the replica-set change is end vs baseline.
    let mut total = serde_json::Map::new();
    for k in COUNTERS {
        let sum: u64 = steps.iter().filter_map(|s| s["counters"][k].as_u64()).sum();
        total.insert(k.to_string(), json!(sum));
    }
    total.insert(
        "tablets_replica_set_changed".into(),
        churn(&baseline, &end, usize::MAX)["tablets_replica_set_changed"].clone(),
    );
    let report = json!({
        "variant": name,
        "reference": refs.prev_ref,
        "nodes": n,
        "ok_writes": ok_writes,
        "info_writes": shared.stats.info_writes.load(Ordering::Relaxed),
        "fail_writes": shared.stats.fail_writes.load(Ordering::Relaxed),
        "ok_reads": shared.stats.ok_reads.load(Ordering::Relaxed),
        "longest_write_stall_ms": worst_stall.as_millis() as u64,
        "finalized_to": target,
        "steps": steps,
        "total_over_roll": Value::Object(total),
    });
    eprintln!("upgrade[{name}]: REPORT {report}");
    if let Ok(dir) = std::env::var("ANIMUS_UPGRADE_FROM_REPORT_DIR") {
        let dir = PathBuf::from(dir);
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::write(
            dir.join(format!("{name}.json")),
            serde_json::to_vec_pretty(&report).expect("report"),
        );
    }
    for v in violations.iter().take(20) {
        eprintln!("upgrade[{name}]: VIOLATION {v}");
    }
    if !violations.is_empty()
        && let Ok(dir) = std::env::var("ANIMUS_UPGRADE_FROM_REPORT_DIR")
    {
        // Everything needed to triage offline: the recorded history, the op
        // trace of non-200 answers, and every node's log.
        let out = PathBuf::from(dir).join(format!("{name}-failure-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&out);
        let _ = std::fs::write(
            out.join("history.json"),
            animus_test::export::to_json(&history),
        );
        let _ = std::fs::write(
            out.join("op-trace.txt"),
            shared.trace.lock().expect("trace").join("\n"),
        );
        let _ = std::fs::write(out.join("violations.txt"), violations.join("\n"));
        for i in 0..n {
            let _ = std::fs::copy(c.log_path(i), out.join(format!("n{i}.log")));
            // The live admin state of every node, for a stalled-group triage.
            for path in ["raft", "raftkv", "txns", "status", "metrics"] {
                let body = c
                    .admin_json(i, &format!("/admin/{path}"))
                    .await
                    .map_or_else(|| "null".into(), |v| v.to_string());
                let _ = std::fs::write(out.join(format!("n{i}.{path}.json")), body);
            }
        }
    }
    assert!(
        violations.is_empty(),
        "upgrade[{name}]: {} violation(s); first: {}",
        violations.len(),
        violations.first().map_or("", String::as_str)
    );
}

/// The converged state read through two different nodes.
async fn final_state(
    nodes: &[SocketAddr],
    budget: Duration,
    violations: &mut Vec<String>,
) -> (BTreeMap<u64, Vec<u64>>, BTreeMap<u64, Vec<u64>>) {
    let mut a = BTreeMap::new();
    let mut b = BTreeMap::new();
    for key in 0..KEYS {
        match workload::final_read(nodes[0], key, budget).await {
            Ok(l) => {
                a.insert(key, l);
            }
            Err(e) => violations.push(format!("[final-read] {e}")),
        }
        match workload::final_read(nodes[1], key, budget).await {
            Ok(l) => {
                b.insert(key, l);
            }
            Err(e) => violations.push(format!("[final-read] {e}")),
        }
    }
    (a, b)
}

/// This table's tablets in the replicated map, per the first live node.
async fn tablet_count(c: &RollCluster) -> usize {
    for i in 0..c.n() {
        if let Some(s) = c.admin_json(i, "/admin/status").await
            && let Some(map) = s["tablets"].as_object()
        {
            return map
                .values()
                .filter(|t| t["table"] == workload::TABLE)
                .count();
        }
    }
    0
}

fn go(variant: Variant, nodes: usize) {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(run(variant, nodes));
}

#[test]
fn clean_3_nodes() {
    go(Variant::Clean, 3);
}

#[test]
fn spare_node_repair_churn_4_nodes() {
    go(Variant::Clean, 4);
}

#[test]
fn kill_the_control_leader_3_nodes() {
    go(Variant::KillLeader, 3);
}

#[test]
fn torn_wal_tail_3_nodes() {
    go(Variant::TornTail, 3);
}
