//! Hardening: a multi-seed fault sweep of the quorum data plane.
//!
//! For each seed, a single sequential coordinator runs a list-append workload
//! (read-modify-write at R=W=2 over 3 replicas) through lossy links, a transient
//! partition, and a node crash, recording the history. The invariant: with
//! `R + W > N`, every **acknowledged** write survives a final healed quorum read
//! (durability), and two final quorum reads agree (convergence). Writes that did
//! not reach a quorum are recorded `info` (indeterminate) and are not required
//! to survive.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use custos_data::{DataClient, ReadResult, TabletView, serve_replica};
use custos_env::{Clock, EnvExt};
use custos_sim::{NetConfig, SimEnv, Simulator};
use custos_storage::MemoryEngine;
use custos_tablet::Epoch;
use custos_test::history::Mop;
use custos_test::{Recorder, check_convergence, check_durability};

const REPLICAS: [u64; 3] = [0, 1, 2];
const CLIENT: u64 = 10;
const TIMEOUT: Duration = Duration::from_secs(5);
type Lists = BTreeMap<u64, Vec<u64>>;

fn enc(list: &[u64]) -> Vec<u8> {
    serde_json::to_vec(list).unwrap()
}
fn dec(bytes: &[u8]) -> Vec<u64> {
    serde_json::from_slice(bytes).unwrap_or_default()
}
fn kb(key: u64) -> Vec<u8> {
    key.to_be_bytes().to_vec()
}
fn view() -> TabletView {
    TabletView {
        replicas: REPLICAS.to_vec(),
        epoch: Epoch::INITIAL,
        r: 2,
        w: 2,
    }
}

async fn append(env: &SimEnv, rec: &Mutex<Recorder>, ver: &Mutex<u64>, key: u64, value: u64) {
    let client = DataClient::new(env.clone());
    rec.lock()
        .unwrap()
        .invoke(CLIENT, env.now().0, vec![Mop::Append { key, value }]);
    let current = match client.read(&view(), &kb(key), TIMEOUT).await {
        ReadResult::Value(Some(b)) => dec(&b),
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
    let v = {
        let mut g = ver.lock().unwrap();
        *g += 1;
        *g
    };
    let acked = client
        .write(&view(), &kb(key), &enc(&list), v, TIMEOUT)
        .await;
    let mop = vec![Mop::Append { key, value }];
    if acked {
        rec.lock().unwrap().ok(CLIENT, env.now().0, mop);
    } else {
        rec.lock().unwrap().info(CLIENT, env.now().0, mop);
    }
}

async fn snapshot(env: &SimEnv, keys: &[u64]) -> Lists {
    let client = DataClient::new(env.clone());
    let mut out = Lists::new();
    for &key in keys {
        let list = match client.read(&view(), &kb(key), TIMEOUT).await {
            ReadResult::Value(Some(b)) => dec(&b),
            _ => Vec::new(),
        };
        out.insert(key, list);
    }
    out
}

fn phase(
    sim: &mut Simulator,
    f: impl FnOnce(SimEnv) -> futures::future::BoxFuture<'static, ()> + Send + 'static,
) {
    let env = sim.env(CLIENT);
    env.clone().spawn_task(f(env));
    // Generous bound: lossy links can force a full client timeout before a
    // quorum is declared unreachable.
    sim.run_for(Duration::from_secs(30));
}

fn lossy(drop_prob: f64) -> NetConfig {
    let mut cfg = NetConfig::default();
    cfg.set_drop_prob(drop_prob);
    cfg
}

/// Count acknowledged appends in a recorded history.
fn ok_appends(rec: &Recorder) -> usize {
    rec.history()
        .ok_entries()
        .flat_map(|e| &e.mops)
        .filter(|m| matches!(m, Mop::Append { .. }))
        .count()
}

#[test]
fn durability_holds_under_faults_across_seeds() {
    let keys = [0u64, 1];
    let mut total_ok = 0usize;

    for seed in 0..24u64 {
        let mut sim = Simulator::new(seed);
        for &id in &REPLICAS {
            serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL);
        }
        sim.set_net_config(lossy(0.1));

        let rec = Arc::new(Mutex::new(Recorder::new(seed)));
        let ver = Arc::new(Mutex::new(0u64));

        // Phase 1: lossy links only.
        let (r, v) = (Arc::clone(&rec), Arc::clone(&ver));
        phase(&mut sim, move |env| {
            Box::pin(async move {
                for (k, val) in [(0, 1), (0, 2), (1, 3)] {
                    append(&env, &r, &v, k, val).await;
                }
            })
        });

        // Phase 2: replica 2 partitioned away from everyone; quorum from {0,1}.
        sim.partition_pair(2, 0);
        sim.partition_pair(2, 1);
        sim.partition_pair(2, CLIENT);
        let (r, v) = (Arc::clone(&rec), Arc::clone(&ver));
        phase(&mut sim, move |env| {
            Box::pin(async move {
                for (k, val) in [(0, 4), (1, 5)] {
                    append(&env, &r, &v, k, val).await;
                }
            })
        });

        // Phase 3: heal 2, crash 0; quorum from {1,2}.
        sim.heal(2, 0);
        sim.heal(2, 1);
        sim.heal(2, CLIENT);
        sim.crash(0);
        let (r, v) = (Arc::clone(&rec), Arc::clone(&ver));
        phase(&mut sim, move |env| {
            Box::pin(async move {
                for (k, val) in [(0, 6), (1, 7)] {
                    append(&env, &r, &v, k, val).await;
                }
            })
        });

        // Phase 4: heal the network, take two final quorum reads from {1,2}.
        sim.set_net_config(lossy(0.0));
        let finals: Arc<Mutex<Option<(Lists, Lists)>>> = Arc::new(Mutex::new(None));
        let fin = Arc::clone(&finals);
        phase(&mut sim, move |env| {
            Box::pin(async move {
                let a = snapshot(&env, &keys).await;
                let b = snapshot(&env, &keys).await;
                *fin.lock().unwrap() = Some((a, b));
            })
        });

        let history = rec.lock().unwrap().history().clone();
        let (final_a, final_b) = finals.lock().unwrap().clone().expect("final reads taken");

        let dur = check_durability(&history, &final_a);
        assert!(
            dur.ok,
            "seed {seed}: durability violated under faults: {:?}",
            dur.violations
        );
        let conv = check_convergence(seed, &final_a, &final_b);
        assert!(
            conv.ok,
            "seed {seed}: convergence violated: {:?}",
            conv.violations
        );

        total_ok += ok_appends(&rec.lock().unwrap());
    }

    // The sweep must actually exercise acknowledged writes (not vacuously pass
    // because every op was indeterminate).
    assert!(
        total_ok > 24,
        "sweep was near-vacuous: only {total_ok} acknowledged appends"
    );
}
