//! End-to-end correctness of the *assembled* system under fault injection.
//!
//! Unlike the per-crate tests, this drives both planes wired together at
//! meaningful scale over `SimEnv`:
//!
//! - **Control plane**: a 3-node in-house Raft group ([`RaftNode`]) that owns
//!   the tablet map. We elect a leader, register two tablets (splitting the
//!   keyspace), and read the committed map back to build the data-plane router —
//!   exactly the cached-`TabletView` decoupling of ADR 0001.
//! - **Data plane**: six replica nodes (three per tablet) served by
//!   [`serve_replica`], each with a background [`serve_anti_entropy`] loop so raw
//!   replica state converges even for keys nobody reads (ADR 0010).
//! - **Workload**: four concurrent client coordinators, each running a
//!   list-append read-modify-write workload over a *disjoint* set of keys that
//!   span both tablets. Single-writer-per-key keeps list-append well-defined
//!   under last-writer-wins storage (concurrent writers to one key would lose
//!   updates by the data model, not by a bug); the clients still run truly
//!   concurrently, interleaved by the cooperative executor, and route through
//!   the same control-derived map.
//! - **Faults**: injected *mid-run* — link loss, a transient partition that
//!   isolates one replica of each tablet, a control-plane **leader kill**, and a
//!   data-replica **crash**, then a heal.
//!
//! Then we record the concurrent history with the [`Recorder`] and run all three
//! checkers ([`check_cycles`] for serializability, [`check_durability`] for no
//! lost acknowledged writes, [`check_convergence`] for replica agreement). The
//! whole run is a pure function of its seed: replay with `ANIMUS_SEED=<seed>`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::{MetaCommand, RaftNode};
use animus_data::{DataClient, ReadResult, Router, serve_anti_entropy, serve_replica};
use animus_env::{Clock, EnvExt};
use animus_sim::{NetConfig, SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{Epoch, KeyRange, TabletId};
use animus_test::history::{Mop, Process};
use animus_test::{Recorder, check_convergence, check_cycles, check_durability};

// --- Topology. Distinct node ids per role: control 0..3, data replicas 10..16,
// client coordinators 20..24 (one inbox per node is single-consumer). ---
const CONTROL: [u64; 3] = [0, 1, 2];
/// Replicas of tablet 1 (lower half of the keyspace).
const TABLET1_REPLICAS: [u64; 3] = [10, 11, 12];
/// Replicas of tablet 2 (upper half of the keyspace).
const TABLET2_REPLICAS: [u64; 3] = [13, 14, 15];
const CLIENTS: [u64; 4] = [20, 21, 22, 23];
/// A separate observer coordinator used for the final quorum snapshots.
const OBSERVER: u64 = 30;

const TABLET1: TabletId = TabletId(1);
const TABLET2: TabletId = TabletId(2);
/// Keyspace split point: keys `< SPLIT` live in tablet 1, the rest in tablet 2.
/// Keys are encoded big-endian, so this is an ordering on the raw bytes.
const SPLIT: u64 = 0x8000_0000_0000_0000;

const TIMEOUT: Duration = Duration::from_secs(5);
const SETTLE: Duration = Duration::from_secs(2);
const AE_INTERVAL: Duration = Duration::from_millis(250);
/// Generous virtual-time bound for a workload phase: lossy links can force a
/// full client timeout before a quorum is declared unreachable.
const PHASE: Duration = Duration::from_secs(40);

const R: usize = 2;
const W: usize = 2;

type Lists = BTreeMap<u64, Vec<u64>>;

fn enc(list: &[u64]) -> Vec<u8> {
    serde_json::to_vec(list).expect("list encodes")
}
fn dec(bytes: &[u8]) -> Vec<u64> {
    serde_json::from_slice(bytes).unwrap_or_default()
}
fn key_bytes(key: u64) -> Vec<u8> {
    key.to_be_bytes().to_vec()
}

fn control_leader(nodes: &[RaftNode<SimEnv>], seed: u64) -> usize {
    let leaders: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(
        leaders.len(),
        1,
        "expected exactly one control leader, got {leaders:?} (seed={seed})"
    );
    leaders[0]
}

/// Shared recorder + per-key version oracle + globally-unique value source.
struct Shared {
    rec: Mutex<Recorder>,
    /// Strictly-increasing version assigned per key, so writes from any
    /// coordinator stay monotonic (the storage layer rejects non-monotonic
    /// versions). Single-writer-per-key means no contention on a given entry.
    versions: Mutex<BTreeMap<u64, u64>>,
    /// A monotonic source of **globally unique** appended values. The Elle model
    /// requires each appended element to be unique so the checker can recover a
    /// total append order per key — reusing a value across rounds/phases would
    /// manufacture spurious cycles.
    next_value: Mutex<u64>,
}

impl Shared {
    fn next_version(&self, key: u64) -> u64 {
        let mut v = self.versions.lock().unwrap();
        let e = v.entry(key).or_insert(0);
        *e += 1;
        *e
    }

    fn fresh_value(&self) -> u64 {
        let mut v = self.next_value.lock().unwrap();
        *v += 1;
        *v
    }
}

/// One list-append read-modify-write op, routed via the cached map. Records
/// `invoke` then exactly one terminal entry: `ok` on an acknowledged write,
/// `info` on any indeterminate outcome (a non-quorum read or write) — never
/// `fail`, which would assert the op provably did not happen.
async fn append(
    env: &SimEnv,
    proc: Process,
    shared: &Shared,
    router: &Router,
    key: u64,
    value: u64,
) {
    let client = DataClient::new(env.clone());
    let mop = vec![Mop::Append { key, value }];
    shared
        .rec
        .lock()
        .unwrap()
        .invoke(proc, env.now().0, mop.clone());

    let view = router.view_for(&key_bytes(key)).expect("key is routable");

    let current = match client.read(&view, &key_bytes(key), TIMEOUT).await {
        ReadResult::Value(Some(bytes)) => dec(&bytes),
        ReadResult::Value(None) => Vec::new(),
        ReadResult::Failed => {
            shared.rec.lock().unwrap().info(proc, env.now().0, mop);
            return;
        }
    };

    let mut list = current;
    list.push(value);
    let version = shared.next_version(key);
    let acked = client
        .write(&view, &key_bytes(key), &enc(&list), version, TIMEOUT)
        .await;
    if acked {
        shared.rec.lock().unwrap().ok(proc, env.now().0, mop);
    } else {
        shared.rec.lock().unwrap().info(proc, env.now().0, mop);
    }
}

/// A pure read op (no write), recorded as a single-mop transaction. The cycle
/// checker uses these reads to recover order and to build `rw` anti-dependency
/// edges, so interleaving reads among the appends gives the checker more teeth.
async fn observe(env: &SimEnv, proc: Process, shared: &Shared, router: &Router, key: u64) {
    let client = DataClient::new(env.clone());
    shared.rec.lock().unwrap().invoke(
        proc,
        env.now().0,
        vec![Mop::Read {
            key,
            observed: None,
        }],
    );
    let view = router.view_for(&key_bytes(key)).expect("key is routable");
    match client.read(&view, &key_bytes(key), TIMEOUT).await {
        ReadResult::Value(v) => {
            let observed = v.map(|b| dec(&b)).unwrap_or_default();
            shared.rec.lock().unwrap().ok(
                proc,
                env.now().0,
                vec![Mop::Read {
                    key,
                    observed: Some(observed),
                }],
            );
        }
        ReadResult::Failed => {
            shared.rec.lock().unwrap().info(
                proc,
                env.now().0,
                vec![Mop::Read {
                    key,
                    observed: None,
                }],
            );
        }
    }
}

/// A client's workload over its own keys: a mix of appends and interleaved
/// reads, parameterized so each client produces a distinct value stream.
async fn client_workload(
    env: SimEnv,
    proc: Process,
    shared: Arc<Shared>,
    router: Router,
    keys: Vec<u64>,
    rounds: u64,
) {
    for round in 0..rounds {
        for &key in &keys {
            // Each appended element is globally unique (Elle requirement).
            let value = shared.fresh_value();
            append(&env, proc, &shared, &router, key, value).await;
            // Interleave a read of one of the client's own keys.
            if round % 2 == 0 {
                let rk = keys[(round as usize) % keys.len()];
                observe(&env, proc, &shared, &router, rk).await;
            }
        }
    }
}

/// Final quorum read of every key into a list-per-key map, via the observer.
async fn snapshot(env: &SimEnv, router: &Router, keys: &[u64]) -> Lists {
    let client = DataClient::new(env.clone());
    let mut out = Lists::new();
    for &key in keys {
        let view = router.view_for(&key_bytes(key)).expect("key is routable");
        let list = match client.read(&view, &key_bytes(key), TIMEOUT).await {
            ReadResult::Value(Some(bytes)) => dec(&bytes),
            _ => Vec::new(),
        };
        out.insert(key, list);
    }
    out
}

fn lossy(p: f64) -> NetConfig {
    let mut cfg = NetConfig::default();
    cfg.set_drop_prob(p);
    cfg
}

/// All keys the workload touches, half below the split (tablet 1), half above.
fn all_keys() -> Vec<u64> {
    let mut keys = Vec::new();
    for client_idx in 0..CLIENTS.len() as u64 {
        // Two keys per client: one in each tablet, so every client spans both.
        keys.push(client_idx); // tablet 1 (small keys < SPLIT)
        keys.push(SPLIT + client_idx); // tablet 2
    }
    keys.sort_unstable();
    keys
}

/// The two keys owned by client index `i` (disjoint across clients).
fn keys_for(i: u64) -> Vec<u64> {
    vec![i, SPLIT + i]
}

/// Spawn the four concurrent client workloads for `rounds` rounds each.
fn spawn_clients(sim: &mut Simulator, shared: &Arc<Shared>, router: &Router, rounds: u64) {
    for (i, &client_id) in CLIENTS.iter().enumerate() {
        let env = sim.env(client_id);
        let shared = Arc::clone(shared);
        let router = router.clone();
        let keys = keys_for(i as u64);
        env.clone().spawn_task(async move {
            client_workload(env, client_id, shared, router, keys, rounds).await;
        });
    }
}

#[test]
fn assembled_system_stays_consistent_under_faults() {
    let seed = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0x0E2E_0E2E);
    let mut sim = Simulator::new(seed);

    // --- Bring up the control plane. ---
    let control: Vec<RaftNode<SimEnv>> = CONTROL
        .iter()
        .map(|&id| RaftNode::start(sim.env(id), CONTROL.to_vec()))
        .collect();

    // --- Bring up the data plane: six replicas, two tablets, each replica with
    // a background anti-entropy loop pushing its digest to its tablet peers. ---
    let bring_up = |replicas: &[u64], tablet: TabletId| {
        for &id in replicas {
            let handle = serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL);
            // Anti-entropy is send-only, so it shares the node's env with the
            // replica server without contending on the single-consumer inbox. It
            // reads the tablet's live epoch from the handle each round.
            serve_anti_entropy(sim.env(id), handle, tablet, replicas.to_vec(), AE_INTERVAL);
        }
    };
    bring_up(&TABLET1_REPLICAS, TABLET1);
    bring_up(&TABLET2_REPLICAS, TABLET2);

    // --- Elect a leader and register the two tablets (split keyspace). ---
    sim.run_for(SETTLE);
    let leader = control_leader(&control, seed);
    let (lo, hi) = KeyRange::whole()
        .split_at(&key_bytes(SPLIT))
        .expect("split point lies in the whole range");
    control[leader].propose(MetaCommand::CreateTablet {
        tablet: TABLET1,
        range: lo,
        replicas: TABLET1_REPLICAS.to_vec(),
    });
    control[leader].propose(MetaCommand::CreateTablet {
        tablet: TABLET2,
        range: hi,
        replicas: TABLET2_REPLICAS.to_vec(),
    });
    sim.run_for(SETTLE);

    // --- Build the data-plane router from the *committed* control metadata
    // (the cached-view decoupling of ADR 0001). ---
    let meta = control[leader].metadata();
    let tablets: Vec<_> = [TABLET1, TABLET2]
        .iter()
        .map(|t| meta.tablets[t].clone())
        .collect();
    assert_eq!(tablets[0].replicas, TABLET1_REPLICAS.to_vec());
    assert_eq!(tablets[1].replicas, TABLET2_REPLICAS.to_vec());
    let router = Router::new(tablets, R, W);

    let shared = Arc::new(Shared {
        rec: Mutex::new(Recorder::new(seed)),
        versions: Mutex::new(BTreeMap::new()),
        next_value: Mutex::new(0),
    });

    // --- Phase 1: warm-up under lossy links, four concurrent clients. ---
    sim.set_net_config(lossy(0.05));
    spawn_clients(&mut sim, &shared, &router, 3);
    sim.run_for(PHASE);

    // --- Phase 2: mid-run faults, clients still running concurrently. ---
    // Isolate one replica of each tablet (quorum still reachable from the other
    // two), kill the control-plane leader, and crash one tablet-1 replica.
    sim.partition_pair(TABLET1_REPLICAS[2], TABLET1_REPLICAS[0]);
    sim.partition_pair(TABLET1_REPLICAS[2], TABLET1_REPLICAS[1]);
    sim.partition_pair(TABLET2_REPLICAS[2], TABLET2_REPLICAS[0]);
    sim.partition_pair(TABLET2_REPLICAS[2], TABLET2_REPLICAS[1]);
    // Partition the isolated replicas from every client too, so the quorum must
    // come from the remaining pair.
    for &c in &CLIENTS {
        sim.partition_pair(TABLET1_REPLICAS[2], c);
        sim.partition_pair(TABLET2_REPLICAS[2], c);
    }
    sim.crash(leader as u64);
    sim.set_net_config(lossy(0.1));
    spawn_clients(&mut sim, &shared, &router, 3);
    sim.run_for(PHASE);

    // --- Phase 3: crash a *taking* replica of tablet 1, heal the partitions,
    // keep writing. Acknowledged writes from {10,11} must still survive because
    // the quorum from any two of {10,11,12} intersects, and anti-entropy fills
    // the gaps. ---
    sim.crash(TABLET1_REPLICAS[0]);
    sim.heal(TABLET1_REPLICAS[2], TABLET1_REPLICAS[1]);
    sim.heal(TABLET2_REPLICAS[2], TABLET2_REPLICAS[0]);
    sim.heal(TABLET2_REPLICAS[2], TABLET2_REPLICAS[1]);
    for &c in &CLIENTS {
        sim.heal(TABLET1_REPLICAS[2], c);
        sim.heal(TABLET2_REPLICAS[2], c);
    }
    spawn_clients(&mut sim, &shared, &router, 2);
    sim.run_for(PHASE);

    // --- Phase 4: fully heal the network, let anti-entropy converge, then take
    // two independent final quorum reads from the observer. ---
    sim.set_net_config(lossy(0.0));
    sim.heal(TABLET1_REPLICAS[2], TABLET1_REPLICAS[0]);
    // Let background anti-entropy run to convergence (it never quiesces, so use a
    // bounded run, not run()).
    sim.run_for(Duration::from_secs(20));

    let keys = all_keys();
    let finals: Arc<Mutex<Option<(Lists, Lists)>>> = Arc::new(Mutex::new(None));
    {
        let env = sim.env(OBSERVER);
        let router = router.clone();
        let keys = keys.clone();
        let out = Arc::clone(&finals);
        env.clone().spawn_task(async move {
            let a = snapshot(&env, &router, &keys).await;
            let b = snapshot(&env, &router, &keys).await;
            *out.lock().unwrap() = Some((a, b));
        });
    }
    sim.run_for(PHASE);

    // --- Check the recorded history with all three checkers. ---
    let history = shared.rec.lock().unwrap().history().clone();
    let (final_a, final_b) = finals
        .lock()
        .unwrap()
        .clone()
        .expect("final reads completed");

    let cycles = check_cycles(&history);
    assert!(
        cycles.ok,
        "serializability cycle in the assembled system (seed={seed}): {:?}",
        cycles.violations
    );

    let dur = check_durability(&history, &final_a);
    assert!(
        dur.ok,
        "lost acknowledged write in the assembled system (seed={seed}): {:?}",
        dur.violations
    );

    let conv = check_convergence(seed, &final_a, &final_b);
    assert!(
        conv.ok,
        "non-convergent final reads in the assembled system (seed={seed}): {:?}",
        conv.violations
    );

    // --- Guard against a vacuous pass: the run must have acknowledged a healthy
    // number of writes across both tablets, not degenerate to all-`info`. ---
    let ok_appends = history
        .ok_entries()
        .flat_map(|e| &e.mops)
        .filter(|m| matches!(m, Mop::Append { .. }))
        .count();
    assert!(
        ok_appends >= 20,
        "near-vacuous run: only {ok_appends} acknowledged appends (seed={seed})"
    );
    // Both tablets must have live data in the final state.
    let tablet1_filled = final_a.iter().any(|(k, v)| *k < SPLIT && !v.is_empty());
    let tablet2_filled = final_a.iter().any(|(k, v)| *k >= SPLIT && !v.is_empty());
    assert!(
        tablet1_filled && tablet2_filled,
        "expected acknowledged data in both tablets (seed={seed}): {final_a:?}"
    );
}

#[test]
fn run_is_deterministic_from_seed() {
    // Two runs at the same seed must record byte-identical histories — the
    // determinism contract (ADR 0003) for the whole assembled stack.
    fn run(seed: u64) -> Vec<String> {
        let mut sim = Simulator::new(seed);
        let _control: Vec<RaftNode<SimEnv>> = CONTROL
            .iter()
            .map(|&id| RaftNode::start(sim.env(id), CONTROL.to_vec()))
            .collect();
        for &id in TABLET1_REPLICAS.iter().chain(TABLET2_REPLICAS.iter()) {
            serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL);
        }
        sim.run_for(SETTLE);
        sim.trace_lines()
    }
    let a = run(0x000D_37E2);
    let b = run(0x000D_37E2);
    assert_eq!(a, b, "same seed must produce an identical trace");
}
