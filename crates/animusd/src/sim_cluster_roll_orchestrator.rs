//! `sim_cluster_roll_orchestrator` — ADR 0073 Phase 3, workstream P3-C: the
//! **roll driver** (`animus_roll`, the pure state machine the CLI's `cluster
//! roll` and the operator's partition driver share) driven over `SimCluster`
//! (roles `[Both, Both, Both, Data]`, RF 3) under the linearizable DynamoDB-wire
//! workload, with fault injection.
//!
//! The harness is "the platform": it executes the driver's [`Action`]s
//! (`Restart` = stop the process, start the new binary a seeded 0.3-2 s later;
//! `TransferControlLeadership` = the real `POST /admin/control/transfer`;
//! `Finalize` = the real `POST /admin/cluster-version/finalize`), and each tick
//! builds an [`Observation`] from the same synchronous bodies `GET
//! /admin/cluster-version` and `GET /admin/roll-health` serve
//! (`SimCluster::{cluster_version_view, roll_health_view}`) through
//! `animus_roll::json`. The machine keeps no state, so every tick is a
//! "restarted driver"; the `driver_restarts` cell also drops the caller-side
//! timers at random ticks.
//!
//! # Oracle (independent of the driver's own inputs)
//!
//! At every decision the harness re-derives the truth from the cluster:
//!
//! - **`Restart`**: every other node is up and its roll-health is `ok`, every
//!   member is `Active`, no other node is below the gate (restarting, or on the
//!   new binary but unhealthy / not yet reporting the new range), the target is
//!   not the control leader (it must have been transferred first), and a
//!   control-bearing node is not rolled while a data-only node is still old;
//! - **`Finalize`**: `can_finalize` and no blockers on the leader's own view,
//!   every node on the new binary;
//! - **order** (clean cells): the target is `roll.remaining[0]` of the live
//!   `cluster-version` view (the driver and `animusd` agree on the D1 order);
//! - the mixed corpus's instant checks (era safety, no capped rejection) and
//!   its end checks (`check_cycles`, `check_durability`, `check_convergence`,
//!   no wedged control replica, acks before/during/after the roll).
//!
//! # Cells (`ANIMUS_UPGRADE_SEEDS` x variants, `ANIMUS_UPGRADE_CELL=<substring>`,
//! `ANIMUS_SEED=<seed>` replay; shares the knob with the mixed-version corpus)
//!
//! - `release_roll_manual_finalize`: `Release(1) -> Release(2)`, manual
//!   finalize: the driver only ever offers it, the "human" finalizes;
//! - `release_roll_auto_finalize_soak`: opt-in auto-finalize after a soak, with
//!   a transient partition mid-roll;
//! - `release_roll_leader_kill`: the control leader is killed mid-roll;
//! - `release_roll_crash_after_restart`: a just-rolled node crashes again;
//! - `release_roll_member_down_blocks`: a not-yet-rolled node dies mid-roll and
//!   stays down: the driver blocks (nothing restarted, nothing finalized) until
//!   it comes back on the new binary, then completes;
//! - `release_roll_member_down_at_finalize`: every node rolled and healthy, then a
//!   member dies just before the finalize: nothing finalizes until it is back;
//! - `release_roll_bad_node_stalls`: a rolled node cannot catch up (isolated):
//!   the driver waits, then surfaces `NodeStalled`, and touches no other node;
//! - `release_roll_driver_restarts`: the caller-side timers are dropped at
//!   random ticks;
//! - `phase1_to_b2_roll` / `phase1_to_b2_leader_kill`: no era (D8 case 1): the
//!   gate is the platform fact plus roll-health (previous-release nodes have no
//!   such endpoint and answer `Unavailable`), then `AwaitEra` until the era
//!   starts by itself;
//! - `negative_control_skip_gate` / `negative_control_finalize_with_blocker`:
//!   the harness doctors the driver's input (health forced `ok`, blockers
//!   hidden) while a node is really down; the oracle MUST flag the unsafe
//!   action by name.
//!
//! Each cell runs on its own OS thread under a wall-clock watchdog.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::mpsc;
use std::time::Duration;

use super::sim_cluster::SimClusterHandle;
use animus_control::sim_versions::BinaryProfile;
use animus_control::version::VersionRange;
use animus_env::{Clock, Env, EnvExt, Rng};
use animus_roll::{
    Action, Block, Config, FinalizeMode, Health, Observation, Platform, decide,
    json::{self, Inputs},
};
use animus_sim::SimEnv;
use animus_test::corpus::{self, SeedVariant};
use animus_test::history::Key;
use animus_test::history::{Mop, Process};
use animus_test::{check_convergence, check_cycles, check_durability};
use futures::future::{Either, select};
use serde_json::json;
use std::sync::Arc;

use super::sim_cluster::SimCluster;
use super::sim_cluster_mixed_version_corpus::{
    CONTROL, ROLES, Watch, era_fully_recorded, wedged_control,
};
use super::sim_cluster_upgrade_corpus::{
    CELL_WATCHDOG, CLIENTS, KEYSPACE, NODES, POLL, READ_PCT, Shared, TBL, combine, converge,
    decode_items_attr, final_state, get_body, live_replicas, ok_appends, pk_sk,
    run_until_clients_done, tablet_of,
};

const ROUNDS: u64 = 150;
const ROUNDS_PHASE2: u64 = 6;
/// One driver tick of virtual time.
const TICK: Duration = Duration::from_millis(200);
/// A virtual-time bound on one roll (a hang bound, never a verdict on timing).
const ROLL_BUDGET: Duration = Duration::from_secs(400);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// `Release(1) -> Release(2)` with the era on: observable version ranges,
    /// Finalize at the end.
    Release,
    /// Phase 1 -> B2: no era until the last node is B2.
    Phase1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Clean,
    PartitionMidRoll,
    LeaderKill,
    CrashAfterRestart,
    MemberDown,
    DownAtFinalize,
    BadNode,
    DriverRestarts,
    NegSkipGate,
    NegFinalizeBlocker,
}

#[derive(Clone, Debug)]
struct Cell {
    name: String,
    seed: u64,
    mode: Mode,
    kind: Kind,
    auto: bool,
}

impl SeedVariant for Cell {
    fn scenario_name(&self) -> &str {
        &self.name
    }
    fn reseeded(&self, name: String, seed: u64) -> Self {
        Cell {
            name,
            seed,
            ..self.clone()
        }
    }
}

fn cell(name: &str, mode: Mode, kind: Kind, auto: bool) -> Cell {
    Cell {
        seed: corpus::name_seed(&format!("sim_cluster_roll_orchestrator/{name}")),
        name: name.to_string(),
        mode,
        kind,
        auto,
    }
}

fn cells() -> Vec<Cell> {
    use Kind::*;
    use Mode::*;
    vec![
        cell("release_roll_manual_finalize", Release, Clean, false),
        cell(
            "release_roll_auto_finalize_soak",
            Release,
            PartitionMidRoll,
            true,
        ),
        cell("release_roll_leader_kill", Release, LeaderKill, true),
        cell(
            "release_roll_crash_after_restart",
            Release,
            CrashAfterRestart,
            false,
        ),
        cell(
            "release_roll_member_down_blocks",
            Release,
            MemberDown,
            false,
        ),
        cell(
            "release_roll_member_down_at_finalize",
            Release,
            DownAtFinalize,
            true,
        ),
        cell("release_roll_bad_node_stalls", Release, BadNode, true),
        cell(
            "release_roll_driver_restarts",
            Release,
            DriverRestarts,
            true,
        ),
        cell("phase1_to_b2_roll", Phase1, Clean, false),
        cell("phase1_to_b2_leader_kill", Phase1, LeaderKill, false),
        cell("negative_control_skip_gate", Release, NegSkipGate, false),
        cell(
            "negative_control_finalize_with_blocker",
            Release,
            NegFinalizeBlocker,
            true,
        ),
    ]
}

fn corpus_cells() -> Vec<Cell> {
    let filter = std::env::var("ANIMUS_UPGRADE_CELL").ok();
    let expanded = if let Some(seed) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        cells()
            .into_iter()
            .map(|c| {
                let name = c.name.clone();
                c.reseeded(name, seed)
            })
            .collect()
    } else {
        corpus::seed_expand(cells(), corpus::seeds_from_env("ANIMUS_UPGRADE_SEEDS"))
    };
    expanded
        .into_iter()
        .filter(|c| filter.as_ref().is_none_or(|f| c.name.contains(f.as_str())))
        .collect()
}

#[derive(Clone, Debug)]
struct Verdict {
    cell: String,
    seed: u64,
    violations: Vec<String>,
    /// `(restarts, transfers, blocked ticks, stalled ticks, finalizes)`.
    stats: (usize, usize, usize, usize, usize),
    trace: Vec<String>,
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|m| (*m).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string())
}

fn run_cell(c: &Cell) -> Verdict {
    let (tx, rx) = mpsc::channel();
    let cc = c.clone();
    std::thread::Builder::new()
        .name(format!("sim_cluster_roll_orchestrator/{}", c.name))
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            eprintln!(
                "sim_cluster_roll_orchestrator cell={} seed={}",
                cc.name, cc.seed
            );
            let v = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&cc))) {
                Ok(v) => v,
                Err(p) => Verdict {
                    cell: cc.name.clone(),
                    seed: cc.seed,
                    violations: vec![format!("panic: {}", panic_message(p))],
                    stats: (0, 0, 0, 0, 0),
                    trace: Vec::new(),
                },
            };
            let _ = tx.send(v);
        })
        .expect("spawn the cell thread");
    match rx.recv_timeout(CELL_WATCHDOG) {
        Ok(v) => v,
        Err(e) => panic!(
            "sim_cluster_roll_orchestrator cell={} HUNG or died (seed={}): no verdict within \
             {:?} ({e}) -- replay with ANIMUS_SEED={} ANIMUS_UPGRADE_CELL={}",
            c.name, c.seed, CELL_WATCHDOG, c.seed, c.name
        ),
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

// ---------------------------------------------------------------------------
// The workload: `sim_cluster_upgrade_corpus`'s client loop with a bounded op
// ---------------------------------------------------------------------------
//
// A request issued against a node the platform has stopped (`crash`, then a
// fresh process after `restart`) never completes in the simulator: the old
// process's timers die with it, so the call's own `CLIENT_TIMEOUT` never fires.
// A real client's socket would reset or time out. The shared loop has no
// client-side bound (and cannot gain one without perturbing the other corpora's
// schedules), so this copy caps each op on the CLIENT's own (never-crashed)
// env and records a capped op as indeterminate, exactly as a failed one.

const OP_CAP: Duration = Duration::from_secs(20);

async fn bounded_dynamo(
    env: &SimEnv,
    handle: &SimClusterHandle,
    node: u64,
    target: &str,
    body: &[u8],
) -> (u16, String) {
    let op = Box::pin(handle.dynamo(node, target, body));
    let cap = Box::pin(env.sleep(OP_CAP));
    match select(op, cap).await {
        Either::Left((r, _)) => r,
        Either::Right(_) => (599, "client-side op cap".to_string()),
    }
}

async fn write_op(
    env: &SimEnv,
    handle: &SimClusterHandle,
    shared: &Arc<Shared>,
    proc: Process,
    key: animus_test::history::Key,
    node: u64,
) {
    let value = shared.fresh_value();
    let (pk, sk) = pk_sk(key);
    let mops = vec![Mop::Append { key, value }];
    shared
        .rec
        .lock()
        .expect("recorder poisoned")
        .invoke(proc, env.now().0, mops.clone());
    let body = json!({
        "TableName": TBL,
        "Key": {"pk": {"S": pk}, "sk": {"S": sk}},
        "UpdateExpression": "SET items = list_append(if_not_exists(items, :empty), :v)",
        "ExpressionAttributeValues": {
            ":empty": {"L": []},
            ":v": {"L": [{"N": value.to_string()}]},
        },
    })
    .to_string();
    let (status, _) = bounded_dynamo(
        env,
        handle,
        node,
        "DynamoDB_20120810.UpdateItem",
        body.as_bytes(),
    )
    .await;
    let mut rec = shared.rec.lock().expect("recorder poisoned");
    if status == 200 {
        rec.ok(proc, env.now().0, mops);
    } else {
        // Indeterminate: the write may have applied and only the reply lost.
        rec.info(proc, env.now().0, mops);
    }
}

async fn read_op(
    env: &SimEnv,
    handle: &SimClusterHandle,
    shared: &Arc<Shared>,
    proc: Process,
    key: animus_test::history::Key,
    node: u64,
) {
    let read = |observed| vec![Mop::Read { key, observed }];
    shared
        .rec
        .lock()
        .expect("recorder poisoned")
        .invoke(proc, env.now().0, read(None));
    let (status, body) = bounded_dynamo(
        env,
        handle,
        node,
        "DynamoDB_20120810.GetItem",
        get_body(key).as_bytes(),
    )
    .await;
    let mut rec = shared.rec.lock().expect("recorder poisoned");
    if status != 200 {
        rec.info(proc, env.now().0, read(None));
        return;
    }
    let list = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v.get("Item").map(decode_items_attr))
        .unwrap_or_default();
    rec.ok(proc, env.now().0, read(Some(list)));
}

async fn client_loop(
    env: SimEnv,
    handle: SimClusterHandle,
    shared: Arc<Shared>,
    proc: Process,
    client: u64,
    rounds: u64,
) {
    let owned: Vec<animus_test::history::Key> =
        (0..KEYSPACE).filter(|k| k % CLIENTS == client).collect();
    for _ in 0..rounds {
        let node = env.gen_below(NODES);
        if env.gen_below(100) < READ_PCT {
            let key = env.gen_below(KEYSPACE);
            read_op(&env, &handle, &shared, proc, key, node).await;
        } else if !owned.is_empty() {
            let key = owned[env.gen_below(owned.len() as u64) as usize];
            write_op(&env, &handle, &shared, proc, key, node).await;
        }
        env.sleep(POLL).await;
    }
    *shared.done.lock().expect("done poisoned") += 1;
}

fn spawn_clients(cluster: &SimCluster, shared: &Arc<Shared>, phase: u64, rounds: u64) {
    *shared.done.lock().expect("done poisoned") = 0;
    let handle = cluster.handle();
    for c in 0..CLIENTS {
        let env = cluster.client_env(phase * 10 + c);
        let (handle, shared) = (handle.clone(), Arc::clone(shared));
        let proc: Process = phase * 100 + c;
        env.clone().spawn_task(async move {
            client_loop(env, handle, shared, proc, c, rounds).await;
        });
    }
}

/// The `setup` of the mixed-version corpus over `LsmEngine<SimEnv>`s. A roll
/// restarts every node, and the `Memory` backend's data groups' Raft state
/// comes back empty on restart (the control mirror is retained since #1235),
/// which is not what restarting a process does; `LsmEngine` is the faithful
/// durable shape for a whole-cluster roll.
fn lsm_setup(seed: u64) -> (SimCluster, Arc<Shared>) {
    let mut cluster = SimCluster::new_with_lsm_engines(
        seed,
        &ROLES,
        3,
        Some(Duration::from_secs(crate::DEFAULT_QUIESCE_AFTER_SECS)),
    );
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        super::sim_cluster_upgrade_corpus::create_table_body(TBL, false).as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    cluster.run_for(super::sim_cluster_upgrade_corpus::SETTLE);
    // A node never given a profile has no capped decode at all: install Phase 1
    // explicitly, as the mixed-version corpus does.
    for n in 0..NODES {
        cluster.set_binary_profile(n, BinaryProfile::Phase1);
    }
    let shared = Arc::new(Shared {
        rec: std::sync::Mutex::new(animus_test::Recorder::new(seed)),
        next_value: std::sync::Mutex::new(0),
        done: std::sync::Mutex::new(0),
    });
    (cluster, shared)
}

// ---------------------------------------------------------------------------
// The platform harness
// ---------------------------------------------------------------------------

struct Rig {
    cluster: SimCluster,
    w: Watch,
    mode: Mode,
    seed: u64,
    /// The cluster version the roll finalizes to.
    goal: u32,
    cfg: Config,
    plat: BTreeMap<u64, Platform>,
    /// Nodes whose process is not running (crashed / mid-restart).
    down: BTreeSet<u64>,
    /// `node -> (when to start it, on the new binary)`.
    pending_up: BTreeMap<u64, (Duration, bool)>,
    up_at: BTreeMap<u64, Duration>,
    ids: BTreeMap<String, u64>,
    flight_since: Option<Duration>,
    settled_since: Option<Duration>,
    draws: u64,
    // Counters (non-vacuity) and fault latches.
    restarts: Vec<u64>,
    transfers: usize,
    blocked: usize,
    blocked_while_down: usize,
    /// The node `MemberDown` killed.
    victim: Option<u64>,
    stalled: usize,
    finalizes: usize,
    human_finalizes: usize,
    fired: BTreeSet<&'static str>,
    window_until: Option<Duration>,
    parity: bool,
    /// Every distinct driver decision with its virtual time (printed on a
    /// failure: the replayable story of the roll).
    trace: Vec<String>,
}

impl Rig {
    fn new(c: &Cell, auto: bool, stall_after: Duration) -> (Self, Arc<Shared>) {
        let (mut cluster, shared) = lsm_setup(c.seed);
        let goal = match c.mode {
            Mode::Release => 2,
            Mode::Phase1 => 1,
        };
        let mut w = Watch::new(true);
        if c.mode == Mode::Release {
            // The era on at cluster version 1, every node a `[1,1]` binary.
            for n in 0..NODES {
                cluster.set_binary_profile(n, BinaryProfile::B2);
                cluster.set_node_version(n, Some(VersionRange::new(1, 1)));
            }
            if !converge(&mut cluster, |cl| {
                w.sample(cl);
                (0..NODES).all(|n| cl.features(n).era_active())
                    && (0..NODES).all(|n| cl.metadata(n).node_versions.len() == NODES as usize)
            }) {
                w.violations.push("the era never started".into());
            }
        }
        let ids = (0..NODES)
            .map(|n| (cluster.handle().env(n).node_id().to_string(), n))
            .collect();
        let cfg = Config {
            finalize: if auto {
                FinalizeMode::Auto {
                    soak: Duration::from_secs(2),
                }
            } else {
                FinalizeMode::Manual
            },
            stall_after,
        };
        (
            Self {
                cluster,
                w,
                mode: c.mode,
                seed: c.seed,
                goal,
                cfg,
                plat: BTreeMap::new(),
                down: BTreeSet::new(),
                pending_up: BTreeMap::new(),
                up_at: BTreeMap::new(),
                ids,
                flight_since: None,
                settled_since: None,
                draws: 0,
                restarts: Vec::new(),
                transfers: 0,
                blocked: 0,
                blocked_while_down: 0,
                victim: None,
                stalled: 0,
                finalizes: 0,
                human_finalizes: 0,
                fired: BTreeSet::new(),
                window_until: None,
                trace: Vec::new(),
                parity: c.kind == Kind::Clean && c.mode == Mode::Release,
            },
            shared,
        )
    }

    fn now(&self) -> Duration {
        Duration::from_nanos(self.cluster.handle().env(0).now().0)
    }

    fn draw(&mut self) -> u64 {
        self.draws += 1;
        splitmix64(self.seed ^ self.draws.wrapping_mul(0xA24B_AED4_963E_E407))
    }

    fn plat_of(&self, n: u64) -> Platform {
        self.plat.get(&n).copied().unwrap_or(Platform::Old)
    }

    fn id_of(&self, n: u64) -> String {
        self.cluster.handle().env(n).node_id().to_string()
    }

    fn step(&mut self, d: Duration) {
        self.w.run(&mut self.cluster, d);
    }

    fn leader(&self) -> Option<u64> {
        CONTROL
            .iter()
            .copied()
            .find(|n| !self.down.contains(n) && self.cluster.is_control_leader(*n))
    }

    /// A node the harness can ask: up, lowest index first.
    fn observer(&self) -> Option<u64> {
        (0..NODES).find(|n| !self.down.contains(n))
    }

    // ---- the platform ----------------------------------------------------

    /// Start `n`'s process on the new (`new`) or previous binary.
    fn bring_up(&mut self, n: u64, new: bool) {
        match (self.mode, new) {
            (Mode::Release, true) => self
                .cluster
                .set_binary_profile(n, BinaryProfile::Release(2)),
            (Mode::Release, false) => self.cluster.set_binary_profile(n, BinaryProfile::B2),
            (Mode::Phase1, true) => self.cluster.set_binary_profile(n, BinaryProfile::B2),
            (Mode::Phase1, false) => self.cluster.set_binary_profile(n, BinaryProfile::Phase1),
        }
        self.cluster.restart(n);
        if self.mode == Mode::Release {
            let hi = if new { 2 } else { 1 };
            self.cluster
                .set_node_version(n, Some(VersionRange::new(1, hi)));
        }
        self.down.remove(&n);
        if new {
            self.plat.insert(n, Platform::New);
            let now = self.now();
            self.up_at.insert(n, now);
        }
    }

    fn pump_platform(&mut self) {
        let now = self.now();
        let due: Vec<(u64, bool)> = self
            .pending_up
            .iter()
            .filter(|(_, (t, _))| *t <= now)
            .map(|(n, (_, new))| (*n, *new))
            .collect();
        for (n, new) in due {
            self.pending_up.remove(&n);
            self.bring_up(n, new);
        }
    }

    /// Crash `n` now and bring it back after `after`, on the binary it ran.
    fn crash_for(&mut self, n: u64, after: Duration) {
        let now = self.now();
        let new = self.plat_of(n) == Platform::New;
        self.cluster.crash(n);
        self.down.insert(n);
        self.pending_up.insert(n, (now + after, new));
        if new {
            self.plat.insert(n, Platform::Restarting);
        }
    }

    // ---- observation -----------------------------------------------------

    fn observe(&mut self) -> Option<Observation> {
        let observer = self.observer()?;
        let view = self.cluster.cluster_version_view(observer);
        let now = self.now();
        let mut inputs = Inputs {
            goal: self.goal,
            control_leader: self.leader().map(|n| self.id_of(n)),
            in_flight_for: self.flight_since.map(|t| now.saturating_sub(t)),
            settled_for: self.settled_since.map(|t| now.saturating_sub(t)),
            ..Default::default()
        };
        for n in 0..NODES {
            let id = self.id_of(n);
            inputs.platform.insert(id.clone(), self.plat_of(n));
            let h = if self.down.contains(&n) {
                Health::Unreachable
            } else if self.mode == Mode::Phase1 && self.plat_of(n) == Platform::Old {
                // A Phase 1 binary has no such endpoint.
                Health::Unavailable
            } else {
                json::parse_health(&self.cluster.roll_health_view(n))
            };
            inputs.health.insert(id, h);
        }
        match json::observation(&view, &inputs) {
            Ok(o) => Some(o),
            Err(e) => {
                self.w.violations.push(format!("observation: {e}"));
                None
            }
        }
    }

    // ---- the oracle ------------------------------------------------------

    /// What is really true about touching the cluster now with `target` the
    /// node about to be restarted (`None` for a finalize).
    fn truth_gate(&mut self, target: Option<u64>, what: &str) -> Vec<String> {
        let mut v = Vec::new();
        let Some(observer) = self.observer() else {
            return vec![format!("{what}: no node is up to ask")];
        };
        for n in 0..NODES {
            if Some(n) == target {
                continue;
            }
            if self.down.contains(&n) {
                v.push(format!("{what} while node {n} is down"));
                continue;
            }
            let h = self.cluster.roll_health_view(n);
            if h["ok"] != true {
                v.push(format!(
                    "{what} while roll-health on node {n} is not ok: {}",
                    h["reasons"]
                ));
            }
            match self.plat_of(n) {
                Platform::Restarting => v.push(format!(
                    "{what} while node {n} is restarting (below the gate)"
                )),
                Platform::New if self.mode == Mode::Release => {
                    let reported = self
                        .cluster
                        .metadata(observer)
                        .node_versions
                        .get(&animus_env::nid(n))
                        .is_some_and(|r| r.range.max >= self.goal);
                    if !reported {
                        v.push(format!(
                            "{what} while rolled node {n} has not reported the new range"
                        ));
                    }
                }
                _ => {}
            }
        }
        let view = self.cluster.cluster_version_view(observer);
        for node in view["nodes"].as_array().into_iter().flatten() {
            if node["status"] != "Active" {
                v.push(format!(
                    "{what} while member {} is {}",
                    node["node"], node["status"]
                ));
            }
        }
        v
    }

    fn check_restart(&mut self, node: u64) {
        let what = format!("restart of node {node}");
        let v = self.truth_gate(Some(node), &what);
        self.w.violations.extend(v);
        if CONTROL.contains(&node) && self.cluster.is_control_leader(node) {
            self.w.violations.push(format!(
                "{what}: it is the control leader (no transfer first)"
            ));
        }
        if CONTROL.contains(&node)
            && (0..NODES).any(|d| !CONTROL.contains(&d) && self.plat_of(d) == Platform::Old)
        {
            self.w.violations.push(format!(
                "{what}: a data-only node is still on the old binary"
            ));
        }
        if self.parity
            && let Some(o) = self.observer()
        {
            let want = self.cluster.cluster_version_view(o)["roll"]["remaining"][0].clone();
            if want != json!(self.id_of(node)) {
                self.w.violations.push(format!(
                    "{what}: roll.remaining[0] is {want}, the driver chose {}",
                    self.id_of(node)
                ));
            }
        }
    }

    fn check_finalize(&mut self) {
        let what = "finalize".to_string();
        let mut v = self.truth_gate(None, &what);
        let leader = self.leader().or_else(|| self.observer());
        if let Some(l) = leader {
            let view = self.cluster.cluster_version_view(l);
            if view["can_finalize"] != true {
                v.push(format!(
                    "{what} while can_finalize is false: {}",
                    view["blockers"]
                ));
            }
            if view["blockers"].as_array().is_some_and(|b| !b.is_empty()) {
                v.push(format!("{what} with blockers {}", view["blockers"]));
            }
        }
        if (0..NODES).any(|n| self.plat_of(n) != Platform::New) {
            v.push(format!("{what} before every node is on the new binary"));
        }
        self.w.violations.extend(v);
    }

    // ---- executing an action --------------------------------------------

    fn post_finalize(&mut self, to: u32, expected: u32) -> bool {
        let Some(l) = self.leader() else {
            return false;
        };
        let body = json!({ "to": to, "expected": expected }).to_string();
        let (status, resp) = self.cluster.admin(
            l,
            "POST",
            "/admin/cluster-version/finalize",
            "",
            body.as_bytes(),
        );
        let _ = resp;
        status == 200
    }

    /// Execute `a`; `false` ends the roll loop.
    fn apply(&mut self, a: &Action) -> bool {
        let now = self.now();
        if matches!(a, Action::Wait { .. } | Action::Blocked(_))
            && self.victim.is_some_and(|v| self.down.contains(&v))
        {
            self.blocked_while_down += 1;
        }
        let line = format!("{a:?}");
        if self.trace.last().is_none_or(|l| !l.ends_with(&line)) {
            self.trace
                .push(format!("t={now:?} down={:?} {line}", self.down));
        }
        let (mut flight, mut settled) = (self.flight_since, self.settled_since);
        match a {
            Action::Restart { node } => {
                let n = self.ids[node];
                self.check_restart(n);
                self.restarts.push(n);
                let down_for = Duration::from_millis(300 + self.draw() % 1700);
                self.cluster.crash(n);
                self.down.insert(n);
                self.plat.insert(n, Platform::Restarting);
                self.pending_up.insert(n, (now + down_for, true));
                flight = Some(now);
                settled = None;
            }
            Action::TransferControlLeadership { from, to } => {
                self.transfers += 1;
                let (f, t) = (self.ids[from], self.ids[to]);
                if !CONTROL.contains(&t) || self.down.contains(&t) {
                    self.w
                        .violations
                        .push(format!("transfer to node {t}, which cannot lead"));
                }
                let body = json!({ "to": to }).to_string();
                let _ =
                    self.cluster
                        .admin(f, "POST", "/admin/control/transfer", "", body.as_bytes());
                flight = None;
            }
            Action::Wait { .. } => {
                settled = None;
            }
            Action::Blocked(b) => {
                self.blocked += 1;
                if matches!(b, Block::NodeStalled { .. }) {
                    self.stalled += 1;
                }
                settled = None;
            }
            Action::AwaitEra | Action::Soak { .. } => {
                if matches!(a, Action::Soak { .. }) && settled.is_none() {
                    settled = Some(now);
                }
            }
            Action::ReadyToFinalize { to } => {
                if matches!(self.cfg.finalize, FinalizeMode::Auto { .. }) {
                    self.w
                        .violations
                        .push("ReadyToFinalize in auto mode".into());
                }
                // The human decides.
                self.human_finalizes += 1;
                self.check_finalize();
                let _ = self.post_finalize(*to, *to - 1);
            }
            Action::Finalize { to, expected } => {
                if matches!(self.cfg.finalize, FinalizeMode::Manual) {
                    self.w.violations.push("Finalize in manual mode".into());
                }
                self.finalizes += 1;
                self.check_finalize();
                let _ = self.post_finalize(*to, *expected);
                settled = None;
            }
            Action::Complete => {
                self.settled_since = None;
                return false;
            }
        }
        if !matches!(a, Action::Wait { .. } | Action::Blocked(_))
            && !matches!(a, Action::Restart { .. })
        {
            flight = None;
        }
        // Soak clock: runs from the first all-done tick.
        if matches!(
            a,
            Action::Soak { .. } | Action::ReadyToFinalize { .. } | Action::Finalize { .. }
        ) && settled.is_none()
        {
            settled = Some(now);
        }
        self.flight_since = flight;
        self.settled_since = settled;
        true
    }

    // ---- fault injection -------------------------------------------------

    fn inject(&mut self, kind: Kind, tick: u64) {
        let now = self.now();
        match kind {
            Kind::PartitionMidRoll => {
                if self.restarts.len() >= 2 && self.fired.insert("partition") {
                    self.cluster.partition(0, 3);
                    self.window_until = Some(now + Duration::from_millis(1500));
                }
                if self.window_until.is_some_and(|t| now >= t) {
                    self.cluster.heal_all();
                    self.window_until = None;
                }
            }
            Kind::LeaderKill => {
                if !self.restarts.is_empty()
                    && self.fired.insert("leader_kill")
                    && let Some(l) = self.leader()
                {
                    self.crash_for(l, Duration::from_millis(1500));
                    self.w.leaders |= 4;
                }
            }
            Kind::CrashAfterRestart => {
                if !self.fired.contains("crash_after")
                    && let Some((&n, &t)) = self.up_at.iter().find(|(n, _)| {
                        self.plat_of(**n) == Platform::New && !self.down.contains(*n)
                    })
                    && now >= t + Duration::from_millis(700)
                {
                    self.fired.insert("crash_after");
                    self.crash_for(n, Duration::from_millis(1500));
                }
            }
            Kind::MemberDown => {
                // Judge the state the NEXT observation will see, so the fault
                // lands when the driver would otherwise restart the next node
                // (a gate decision), not while a node is in flight (a wait).
                let cur = self.observe();
                let settled = cur.as_ref().is_some_and(|o| {
                    o.nodes.iter().any(|x| x.platform == Platform::New)
                        && o.nodes.iter().all(|x| match x.platform {
                            Platform::Old => true,
                            Platform::Restarting => false,
                            Platform::New => x.health == Health::Ok && x.reported_new,
                        })
                });
                if settled
                    && !self.fired.contains("member_down")
                    && let Some(v) = (0..NODES).rev().find(|n| {
                        self.plat_of(*n) == Platform::Old
                            && !self.down.contains(n)
                            && self.leader() != Some(*n)
                    })
                {
                    self.fired.insert("member_down");
                    self.victim = Some(v);
                    self.cluster.crash(v);
                    self.down.insert(v);
                    // The operator brings it back on the NEW binary (D1 step 5).
                    self.pending_up
                        .insert(v, (now + Duration::from_secs(25), true));
                }
            }
            Kind::DownAtFinalize => {
                // Every node rolled and healthy, the finalize about to be
                // decided: a member dies first. Nothing may finalize until it is
                // back (on the new binary).
                let cur = self.observe();
                let all_done = cur.as_ref().is_some_and(|o| {
                    o.nodes.iter().all(|x| {
                        x.platform == Platform::New && x.health == Health::Ok && x.reported_new
                    }) && o.active < o.goal
                });
                if all_done
                    && !self.fired.contains("down_at_finalize")
                    && let Some(v) = (0..NODES).rev().find(|n| self.leader() != Some(*n))
                {
                    self.fired.insert("down_at_finalize");
                    self.victim = Some(v);
                    self.cluster.crash(v);
                    self.down.insert(v);
                    self.plat.insert(v, Platform::New);
                    self.pending_up
                        .insert(v, (now + Duration::from_secs(12), true));
                }
            }
            Kind::BadNode => {
                if !self.fired.contains("bad_node")
                    && let Some((&n, _)) = self.up_at.iter().find(|(n, _)| {
                        self.plat_of(**n) == Platform::New && !self.down.contains(*n)
                    })
                {
                    self.fired.insert("bad_node");
                    for m in (0..NODES).filter(|m| *m != n) {
                        self.cluster.partition(n, m);
                    }
                    self.window_until = Some(now + Duration::from_secs(30));
                }
                if self.window_until.is_some_and(|t| now >= t) {
                    self.cluster.heal_all();
                    self.window_until = None;
                }
            }
            Kind::DriverRestarts => {
                if tick % 13 == 3 {
                    self.flight_since = None;
                    self.settled_since = None;
                }
            }
            Kind::Clean | Kind::NegSkipGate | Kind::NegFinalizeBlocker => {}
        }
    }

    // ---- the loop --------------------------------------------------------

    /// Drive the roll to `Complete` (or the budget).
    fn run_roll(&mut self, kind: Kind) -> bool {
        let start = self.now();
        let mut tick = 0u64;
        loop {
            if self.now().saturating_sub(start) > ROLL_BUDGET {
                return false;
            }
            tick += 1;
            self.pump_platform();
            // Faults land BETWEEN ticks (before the observation), so the
            // oracle judges each decision against the state it was made on.
            self.inject(kind, tick);
            if let Some(obs) = self.observe() {
                let a = decide(&self.cfg, &obs);
                // A restarted driver decides the same thing from the same input.
                if decide(&Config { ..self.cfg }, &obs.clone()) != a {
                    self.w
                        .violations
                        .push("decide is not a function of its observation".into());
                }
                let go_on = self.apply(&a);
                if !go_on {
                    return true;
                }
            }
            self.step(TICK);
        }
    }
}

// ---------------------------------------------------------------------------
// The cells
// ---------------------------------------------------------------------------

fn run(c: &Cell) -> Verdict {
    match c.kind {
        Kind::NegSkipGate => run_neg_skip_gate(c),
        Kind::NegFinalizeBlocker => run_neg_finalize(c),
        _ => run_roll(c),
    }
}

fn verdict_of(c: &Cell, rig: &Rig, violations: Vec<String>) -> Verdict {
    Verdict {
        cell: c.name.clone(),
        seed: c.seed,
        violations,
        stats: (
            rig.restarts.len(),
            rig.transfers,
            rig.blocked,
            rig.stalled,
            rig.finalizes + rig.human_finalizes,
        ),
        trace: rig.trace.clone(),
    }
}

fn run_roll(c: &Cell) -> Verdict {
    let stall_after = if c.kind == Kind::BadNode {
        Duration::from_secs(12)
    } else {
        Duration::from_secs(300)
    };
    let (mut rig, shared) = Rig::new(c, c.auto, stall_after);
    let seed = c.seed;
    spawn_clients(&rig.cluster, &shared, 0, ROUNDS);
    rig.step(Duration::from_secs(1));
    let acks_before = ok_appends(&shared.history());
    let tablet = tablet_of(&rig.cluster, 0, TBL).expect("table has a tablet");

    let finished = rig.run_roll(c.kind);
    if !finished {
        rig.w
            .violations
            .push("the roll did not complete within its virtual-time budget".into());
    }
    rig.cluster.heal_all();
    let acks_during = ok_appends(&shared.history());

    // ---- outcome ----------------------------------------------------------
    if (0..NODES).any(|n| rig.plat_of(n) != Platform::New) {
        rig.w
            .violations
            .push("a node never reached the new binary".into());
    }
    let started = converge(&mut rig.cluster, |cl| {
        rig.w.sample(cl);
        era_fully_recorded(cl)
            && (rig.mode == Mode::Phase1
                || (0..NODES).all(|n| cl.metadata(n).cluster_version() == rig.goal))
    });
    if !started {
        rig.w.violations.push(
            "the cluster never reached the finalized state on every node (era/active version)"
                .into(),
        );
    }
    // Per-kind non-vacuity and driver-behaviour assertions.
    if rig.restarts.len() != NODES as usize
        && !matches!(c.kind, Kind::CrashAfterRestart | Kind::MemberDown)
    {
        rig.w.violations.push(format!(
            "expected exactly {NODES} driver restarts, saw {:?}",
            rig.restarts
        ));
    }
    let distinct: BTreeSet<u64> = rig.restarts.iter().copied().collect();
    if distinct.len() != rig.restarts.len() {
        rig.w.violations.push(format!(
            "a node was restarted twice by the driver: {:?}",
            rig.restarts
        ));
    }
    if rig.transfers == 0 && rig.leader_was_rolled_last_needed() {
        rig.w
            .violations
            .push("vacuous: the control leader was never handed off".into());
    }
    match c.kind {
        Kind::MemberDown | Kind::DownAtFinalize if rig.blocked_while_down == 0 => {
            rig.w.violations.push(
                "vacuous: the driver never held (wait/blocked) while the member was down".into(),
            );
        }
        Kind::BadNode if rig.stalled == 0 => {
            rig.w
                .violations
                .push("vacuous: the stalled node was never surfaced as NodeStalled".into());
        }
        _ => {}
    }
    match (c.auto, rig.finalizes, rig.human_finalizes, c.mode) {
        (_, _, _, Mode::Phase1) => {}
        (true, 0, _, _) => rig.w.violations.push("auto mode never finalized".into()),
        (false, _, 0, _) => rig
            .w
            .violations
            .push("manual mode never offered the finalize".into()),
        _ => {}
    }

    // The workload keeps going on the finished cluster.
    if !run_until_clients_done(&mut rig.cluster, &shared) {
        rig.w
            .violations
            .push("the workload did not finish within its budget".into());
    }
    rig.cluster
        .run_for(super::sim_cluster_upgrade_corpus::DRAIN);
    spawn_clients(&rig.cluster, &shared, 1, ROUNDS_PHASE2);
    if !run_until_clients_done(&mut rig.cluster, &shared) {
        rig.w
            .violations
            .push("post-roll workload did not finish within its budget".into());
    }
    rig.cluster
        .run_for(super::sim_cluster_upgrade_corpus::DRAIN);
    rig.w.sample(&rig.cluster);

    let wedged = {
        let mut last = wedged_control(&rig.cluster);
        let _ = converge(&mut rig.cluster, |cl| {
            last = wedged_control(cl);
            last.is_empty()
        });
        last
    };
    if !wedged.is_empty() {
        rig.w.violations.push(format!(
            "control replicas {wedged:?} wedged: applied < leader commit"
        ));
    }
    let history = shared.history();
    let cycles = check_cycles(&history);
    rig.w.violations.extend(
        cycles
            .violations
            .into_iter()
            .map(|v| format!("cycles: {v}")),
    );
    let handle = rig.cluster.handle();
    let states = |cl: &SimCluster| -> Vec<BTreeMap<Key, Vec<u64>>> {
        live_replicas(cl, tablet)
            .iter()
            .map(|&n| final_state(&handle, tablet, n))
            .collect()
    };
    let check = |cl: &SimCluster| {
        let st = states(cl);
        let durability = combine(seed, st.iter().map(|s| check_durability(&history, s)));
        let convergence = combine(
            seed,
            st.iter()
                .skip(1)
                .map(|s| check_convergence(seed, &st[0], s)),
        );
        (st.len(), durability, convergence)
    };
    let mut last = check(&rig.cluster);
    let _ = converge(&mut rig.cluster, |cl| {
        last = check(cl);
        last.0 > 0 && last.1.ok && last.2.ok
    });
    if last.0 == 0 {
        rig.w.violations.push("no live replica of the table".into());
    }
    rig.w.violations.extend(
        last.1
            .violations
            .into_iter()
            .map(|v| format!("durability: {v}")),
    );
    rig.w.violations.extend(
        last.2
            .violations
            .into_iter()
            .map(|v| format!("convergence: {v}")),
    );
    let total = ok_appends(&history);
    if acks_before == 0 {
        rig.w
            .violations
            .push("vacuous: no ack before the roll".into());
    }
    if acks_during <= acks_before {
        rig.w
            .violations
            .push("vacuous: no ack during the roll".into());
    }
    if total <= acks_during {
        rig.w
            .violations
            .push("vacuous: no ack after the roll".into());
    }
    let violations = std::mem::take(&mut rig.w.violations);
    verdict_of(c, &rig, violations)
}

impl Rig {
    /// Whether the roll necessarily had to hand the control leader off: every
    /// cell that completes rolls the leader last unless a fault moved
    /// leadership off the node before its turn, so "no transfer at all" is only
    /// excusable when a kill/crash happened.
    fn leader_was_rolled_last_needed(&self) -> bool {
        self.fired.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Negative controls: the harness doctors the driver's input
// ---------------------------------------------------------------------------

/// Pretend every node is healthy and `Active` (a buggy observer that skips the
/// D2 gate).
fn doctor_skip_gate(o: &mut Observation) {
    for n in &mut o.nodes {
        n.status = Some("Active".into());
        n.health = Health::Ok;
    }
}

fn run_neg_skip_gate(c: &Cell) -> Verdict {
    let (mut rig, _shared) = Rig::new(c, false, Duration::from_secs(300));
    rig.step(Duration::from_secs(1));
    // A really-dead member the doctored observation hides.
    let victim = 1;
    rig.cluster.crash(victim);
    rig.down.insert(victim);
    rig.step(Duration::from_secs(3));
    let mut o = rig.observe().expect("an observation");
    // Honest: blocked by the dead member.
    let honest = decide(&rig.cfg, &o);
    let mut v: Vec<String> = Vec::new();
    if !matches!(honest, Action::Blocked(_)) {
        v.push(format!("the honest driver did not block: {honest:?}"));
    }
    doctor_skip_gate(&mut o);
    let a = decide(&rig.cfg, &o);
    let Action::Restart { node } = a else {
        v.push(format!("the doctored driver did not restart a node: {a:?}"));
        return verdict_of(c, &rig, v);
    };
    let n = rig.ids[&node];
    rig.check_restart(n);
    let flagged = std::mem::take(&mut rig.w.violations);
    if !flagged
        .iter()
        .any(|s| s.contains(&format!("restart of node {n} while node {victim} is down")))
    {
        v.push(format!(
            "the oracle did not flag the ungated restart by name: {flagged:?}"
        ));
    }
    verdict_of(c, &rig, v)
}

fn run_neg_finalize(c: &Cell) -> Verdict {
    let (mut rig, shared) = Rig::new(c, true, Duration::from_secs(300));
    spawn_clients(&rig.cluster, &shared, 0, ROUNDS);
    rig.step(Duration::from_secs(1));
    // Roll everything honestly, but stop at the first Soak/Finalize decision.
    let start = rig.now();
    let mut v: Vec<String> = Vec::new();
    loop {
        if rig.now().saturating_sub(start) > ROLL_BUDGET {
            v.push("the roll never reached the finalize decision".into());
            return verdict_of(c, &rig, v);
        }
        rig.pump_platform();
        if let Some(obs) = rig.observe() {
            let a = decide(&rig.cfg, &obs);
            if matches!(a, Action::Soak { .. } | Action::Finalize { .. }) {
                break;
            }
            if !rig.apply(&a) {
                v.push("the roll completed without a finalize decision".into());
                return verdict_of(c, &rig, v);
            }
        }
        rig.step(TICK);
    }
    // A member dies after the last node rolled; the observer hides it.
    let victim = 1;
    rig.cluster.crash(victim);
    rig.down.insert(victim);
    rig.step(Duration::from_secs(3));
    let mut o = rig.observe().expect("an observation");
    let honest = decide(&rig.cfg, &o);
    if matches!(
        honest,
        Action::Finalize { .. } | Action::ReadyToFinalize { .. }
    ) {
        v.push(format!("the honest driver offered a finalize: {honest:?}"));
    }
    doctor_skip_gate(&mut o);
    o.can_finalize = true;
    o.finalize_blockers.clear();
    o.settled_for = Some(Duration::from_secs(60));
    let a = decide(&rig.cfg, &o);
    if !matches!(a, Action::Finalize { .. }) {
        v.push(format!("the doctored driver did not finalize: {a:?}"));
        return verdict_of(c, &rig, v);
    }
    rig.check_finalize();
    let flagged = std::mem::take(&mut rig.w.violations);
    if !flagged
        .iter()
        .any(|s| s.starts_with("finalize while") && s.contains(&format!("node {victim}")))
    {
        v.push(format!(
            "the oracle did not flag the finalize by name: {flagged:?}"
        ));
    }
    verdict_of(c, &rig, v)
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

/// Run every expanded cell of one family (exact cell name) and assert it.
fn run_family(name: &str) {
    let mut failures: Vec<String> = Vec::new();
    let mut ran = 0usize;
    for c in corpus_cells().into_iter().filter(|c| {
        c.name == name
            || c.name
                .strip_prefix(name)
                .and_then(|r| r.strip_prefix("_s"))
                .is_some_and(|d| !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit()))
    }) {
        ran += 1;
        let v = run_cell(&c);
        eprintln!(
            "sim_cluster_roll_orchestrator cell={} seed={} restarts={} transfers={} blocked={} \
             stalled={} finalizes={} violations={}",
            v.cell,
            v.seed,
            v.stats.0,
            v.stats.1,
            v.stats.2,
            v.stats.3,
            v.stats.4,
            v.violations.len()
        );
        if !v.violations.is_empty() {
            failures.push(format!(
                "cell={} FAILED (seed={}): {:#?} -- replay with ANIMUS_SEED={} \
                 ANIMUS_UPGRADE_CELL={}",
                v.cell, v.seed, v.violations, v.seed, name
            ));
            for l in v.trace.iter().rev().take(40).rev() {
                eprintln!("  trace {}: {l}", v.cell);
            }
        }
    }
    // `ANIMUS_UPGRADE_CELL` may legitimately narrow a family away.
    assert!(
        ran > 0 || std::env::var("ANIMUS_UPGRADE_CELL").is_ok(),
        "no cell ran for family {name}"
    );
    assert!(
        failures.is_empty(),
        "sim_cluster_roll_orchestrator: {}",
        failures.join("\n")
    );
}

#[test]
fn sim_cluster_roll_orchestrator_manual_finalize() {
    run_family("release_roll_manual_finalize");
}

#[test]
fn sim_cluster_roll_orchestrator_auto_finalize_soak() {
    run_family("release_roll_auto_finalize_soak");
}

#[test]
fn sim_cluster_roll_orchestrator_leader_kill() {
    run_family("release_roll_leader_kill");
}

#[test]
fn sim_cluster_roll_orchestrator_crash_after_restart() {
    run_family("release_roll_crash_after_restart");
}

#[test]
fn sim_cluster_roll_orchestrator_member_down_blocks() {
    run_family("release_roll_member_down_blocks");
}

#[test]
fn sim_cluster_roll_orchestrator_member_down_at_finalize() {
    run_family("release_roll_member_down_at_finalize");
}

#[test]
fn sim_cluster_roll_orchestrator_bad_node_stalls() {
    run_family("release_roll_bad_node_stalls");
}

#[test]
fn sim_cluster_roll_orchestrator_driver_restarts() {
    run_family("release_roll_driver_restarts");
}

#[test]
fn sim_cluster_roll_orchestrator_phase1_to_b2() {
    run_family("phase1_to_b2_roll");
}

#[test]
fn sim_cluster_roll_orchestrator_phase1_to_b2_leader_kill() {
    run_family("phase1_to_b2_leader_kill");
}

#[test]
fn sim_cluster_roll_orchestrator_negative_control_skip_gate() {
    run_family("negative_control_skip_gate");
}

#[test]
fn sim_cluster_roll_orchestrator_negative_control_finalize_with_blocker() {
    run_family("negative_control_finalize_with_blocker");
}

#[test]
fn sim_cluster_roll_orchestrator_cell_names_and_seeds_are_unique() {
    let cs = corpus::seed_expand(cells(), 3);
    let names: BTreeSet<_> = cs.iter().map(|c| c.name.clone()).collect();
    let seeds: BTreeSet<_> = cs.iter().map(|c| c.seed).collect();
    assert_eq!(names.len(), cs.len(), "duplicate cell name");
    assert_eq!(seeds.len(), cs.len(), "duplicate cell seed");
}

#[test]
fn sim_cluster_roll_orchestrator_run_is_deterministic() {
    let c = cell(
        "release_roll_leader_kill",
        Mode::Release,
        Kind::LeaderKill,
        true,
    );
    let a = run_cell(&c);
    let b = run_cell(&c);
    assert_eq!(a.violations, b.violations, "same seed, different verdict");
    assert_eq!(a.stats, b.stats, "same seed, different driver trace");
}
