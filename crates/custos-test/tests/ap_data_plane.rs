//! M5 (AP path): run the M4 quorum data-plane workload through the recorder and
//! check it. List-append is layered over the last-write-wins KV store by
//! read-modify-write from a single sequential coordinator.
//!
//! - A correct run satisfies durability (every acknowledged append survives in a
//!   final quorum read) and convergence (two final quorum reads agree).
//! - A run with an unsafe quorum (W=1) under a partition + crash loses an
//!   acknowledged write; the durability checker flags it with the replay seed.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use custos_data::{DataClient, ReadResult, TabletView, serve_replica};
use custos_env::{Clock, EnvExt};
use custos_sim::{SimEnv, Simulator};
use custos_storage::MemoryEngine;
use custos_tablet::Epoch;
use custos_test::history::Mop;
use custos_test::{Recorder, check_convergence, check_durability};

const REPLICAS: [u64; 3] = [0, 1, 2];
const CLIENT: u64 = 10;
const TIMEOUT: Duration = Duration::from_secs(2);

fn enc(list: &[u64]) -> Vec<u8> {
    serde_json::to_vec(list).unwrap()
}
fn dec(bytes: &[u8]) -> Vec<u64> {
    serde_json::from_slice(bytes).unwrap_or_default()
}
fn key_bytes(key: u64) -> Vec<u8> {
    key.to_be_bytes().to_vec()
}

type Lists = BTreeMap<u64, Vec<u64>>;

/// Read-modify-write append, recording invoke then ok (success) or info
/// (indeterminate — never fail).
async fn append(
    env: &SimEnv,
    rec: &Mutex<Recorder>,
    next_version: &Mutex<u64>,
    view: &TabletView,
    key: u64,
    value: u64,
) {
    let client = DataClient::new(env.clone());
    rec.lock()
        .unwrap()
        .invoke(CLIENT, env.now().0, vec![Mop::Append { key, value }]);

    let current = match client.read(view, &key_bytes(key), TIMEOUT).await {
        ReadResult::Value(Some(bytes)) => dec(&bytes),
        ReadResult::Value(None) => Vec::new(),
        ReadResult::Failed => {
            rec.lock()
                .unwrap()
                .info(CLIENT, env.now().0, vec![Mop::Append { key, value }]);
            return;
        }
    };
    let mut list = current;
    list.push(value);
    let version = {
        let mut v = next_version.lock().unwrap();
        *v += 1;
        *v
    };
    let acked = client
        .write(view, &key_bytes(key), &enc(&list), version, TIMEOUT)
        .await;
    let mop = vec![Mop::Append { key, value }];
    if acked {
        rec.lock().unwrap().ok(CLIENT, env.now().0, mop);
    } else {
        rec.lock().unwrap().info(CLIENT, env.now().0, mop);
    }
}

/// Final quorum read of every key into a list-per-key map.
async fn snapshot(env: &SimEnv, view: &TabletView, keys: &[u64]) -> Lists {
    let client = DataClient::new(env.clone());
    let mut out = Lists::new();
    for &key in keys {
        let list = match client.read(view, &key_bytes(key), TIMEOUT).await {
            ReadResult::Value(Some(bytes)) => dec(&bytes),
            _ => Vec::new(),
        };
        out.insert(key, list);
    }
    out
}

fn view(epoch: Epoch, r: usize, w: usize) -> TabletView {
    TabletView {
        tablet: custos_tablet::TabletId(1),
        replicas: REPLICAS.to_vec(),
        epoch,
        r,
        w,
    }
}

/// Spawn `f` on the client node and run the data plane to quiescence.
fn phase(
    sim: &mut Simulator,
    f: impl FnOnce(SimEnv) -> futures::future::BoxFuture<'static, ()> + Send + 'static,
) {
    let env = sim.env(CLIENT);
    env.clone().spawn_task(f(env));
    sim.run();
}

#[test]
fn correct_run_satisfies_durability_and_convergence() {
    let seed = 0xA9_DA7A;
    let sim = Simulator::new(seed);
    let mut sim = sim;
    for &id in &REPLICAS {
        serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL);
    }

    let rec = Arc::new(Mutex::new(Recorder::new(seed)));
    let version = Arc::new(Mutex::new(0u64));
    let finals: Arc<Mutex<Option<(Lists, Lists)>>> = Arc::new(Mutex::new(None));
    let keys = [0u64, 1];

    let rec_t = Arc::clone(&rec);
    let ver_t = Arc::clone(&version);
    let fin_t = Arc::clone(&finals);
    phase(&mut sim, move |env| {
        Box::pin(async move {
            let v = view(Epoch::INITIAL, 2, 2);
            for (key, value) in [(0, 10), (0, 11), (1, 20), (0, 12), (1, 21)] {
                append(&env, &rec_t, &ver_t, &v, key, value).await;
            }
            let a = snapshot(&env, &v, &keys).await;
            let b = snapshot(&env, &v, &keys).await;
            *fin_t.lock().unwrap() = Some((a, b));
        })
    });

    let history = rec.lock().unwrap().history().clone();
    let (final_a, final_b) = finals.lock().unwrap().clone().expect("phase completed");

    let dur = check_durability(&history, &final_a);
    assert!(
        dur.ok,
        "durability violations on a correct run: {:?}",
        dur.violations
    );
    let conv = check_convergence(seed, &final_a, &final_b);
    assert!(
        conv.ok,
        "convergence violations on a correct run: {:?}",
        conv.violations
    );

    // Sanity: the appends actually landed.
    assert_eq!(final_a.get(&0), Some(&vec![10, 11, 12]));
    assert_eq!(final_a.get(&1), Some(&vec![20, 21]));
}

#[test]
fn unsafe_quorum_loses_a_write_and_is_flagged() {
    let seed = 0x105_7DB;
    let mut sim = Simulator::new(seed);
    for &id in &REPLICAS {
        serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL);
    }

    let rec = Arc::new(Mutex::new(Recorder::new(seed)));
    let version = Arc::new(Mutex::new(0u64));
    let keys = [0u64];

    // Phase 1: a healthy W=2 append establishes [10].
    let (r1, v1) = (Arc::clone(&rec), Arc::clone(&version));
    phase(&mut sim, move |env| {
        Box::pin(async move {
            append(&env, &r1, &v1, &view(Epoch::INITIAL, 2, 2), 0, 10).await;
        })
    });

    // Phase 2: partition the client from replicas 1 and 2, then append with the
    // unsafe W=1. The write is acknowledged by replica 0 alone.
    sim.partition_pair(CLIENT, 1);
    sim.partition_pair(CLIENT, 2);
    let (r2, v2) = (Arc::clone(&rec), Arc::clone(&version));
    phase(&mut sim, move |env| {
        Box::pin(async move {
            append(&env, &r2, &v2, &view(Epoch::INITIAL, 1, 1), 0, 99).await;
        })
    });

    // Phase 3: kill the only replica that took the write, heal, read a quorum.
    sim.crash(0);
    sim.heal(CLIENT, 1);
    sim.heal(CLIENT, 2);
    let finals: Arc<Mutex<Option<Lists>>> = Arc::new(Mutex::new(None));
    let fin_t = Arc::clone(&finals);
    phase(&mut sim, move |env| {
        Box::pin(async move {
            let v = view(Epoch::INITIAL, 2, 2);
            *fin_t.lock().unwrap() = Some(snapshot(&env, &v, &keys).await);
        })
    });

    let history = rec.lock().unwrap().history().clone();
    let final_state = finals.lock().unwrap().clone().expect("phase completed");

    // The W=1 append of 99 was acknowledged but is absent from the surviving
    // quorum — a lost acknowledged write.
    let report = check_durability(&history, &final_state);
    assert!(
        !report.ok,
        "durability checker missed the lost write (seed={seed})"
    );
    assert!(
        report.violations.iter().any(|v| v.contains("99")),
        "expected the lost value 99 to be reported: {:?}",
        report.violations
    );
    assert_eq!(report.seed, seed, "report should carry the replay seed");
}
