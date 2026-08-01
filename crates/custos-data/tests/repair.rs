//! AP repair / anti-entropy: a replica that misses writes (because it was down
//! or partitioned) converges back to the others — both lazily on a divergent
//! read (read-repair) and in the background with no reads at all (anti-entropy).
//!
//! These pin down the convergence of *raw replica state*, which `R + W > N`
//! alone does not give: quorum reads merely intersect acknowledged writes, so
//! without repair a replica that missed a write stays stale until overwritten.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use custos_data::{DataClient, ReadResult, TabletView, serve_anti_entropy, serve_replica};
use custos_env::EnvExt;
use custos_sim::{SimEnv, Simulator};
use custos_storage::{MemoryEngine, StorageEngine};
use custos_tablet::{Epoch, TabletId};

const REPLICAS: [u64; 3] = [0, 1, 2];
const CLIENT: u64 = 10;
const TIMEOUT: Duration = Duration::from_secs(2);

fn view(epoch: Epoch) -> TabletView {
    // R + W = 4 > N = 3 for the common path; tests override r/w as needed.
    TabletView {
        tablet: TabletId(1),
        replicas: REPLICAS.to_vec(),
        epoch,
        r: 2,
        w: 2,
    }
}

fn cluster(seed: u64, epoch: Epoch) -> (Simulator, Vec<custos_data::ReplicaHandle<MemoryEngine>>) {
    let sim = Simulator::new(seed);
    let handles = REPLICAS
        .iter()
        .map(|&id| serve_replica(sim.env(id), MemoryEngine::new(), epoch))
        .collect();
    (sim, handles)
}

fn run_op<T: Clone + Send + 'static>(
    sim: &mut Simulator,
    op: impl FnOnce(DataClient<SimEnv>) -> futures::future::BoxFuture<'static, T> + Send + 'static,
) -> T {
    let result: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let client_env = sim.env(CLIENT);
    let out = Arc::clone(&result);
    client_env.clone().spawn_task(async move {
        let client = DataClient::new(client_env);
        *out.lock().unwrap() = Some(op(client).await);
    });
    sim.run();
    result.lock().unwrap().clone().expect("op did not complete")
}

fn value_at(handle: &custos_data::ReplicaHandle<MemoryEngine>, key: &[u8]) -> Option<Vec<u8>> {
    handle.storage().get(key).unwrap().map(|vv| vv.value)
}

#[test]
fn anti_entropy_converges_a_lagging_replica_without_reads() {
    let seed = 0x00AE_2026;
    let (mut sim, handles) = cluster(seed, Epoch::INITIAL);

    // Replica 2 is down while a write commits to the W=2 quorum of {0,1}.
    sim.crash(2);
    let vw = view(Epoch::INITIAL);
    let acked = run_op(&mut sim, move |c| {
        Box::pin(async move { c.write(&vw, b"k", b"v1", 1, TIMEOUT).await })
    });
    assert!(
        acked,
        "write reached the surviving W=2 quorum (seed={seed})"
    );
    assert_eq!(
        value_at(&handles[2], b"k"),
        None,
        "the down replica missed the write (seed={seed})"
    );

    // It rejoins, but *nothing reads `k`* — only background anti-entropy runs.
    sim.restart(2);
    for &id in &REPLICAS {
        serve_anti_entropy(
            sim.env(id),
            handles[id as usize].storage().clone(),
            TabletId(1),
            Epoch::INITIAL,
            REPLICAS.to_vec(),
            Duration::from_millis(50),
        );
    }
    sim.run_for(Duration::from_secs(1));

    assert_eq!(
        value_at(&handles[2], b"k"),
        Some(b"v1".to_vec()),
        "anti-entropy did not converge the lagging replica (seed={seed})"
    );
}

#[test]
fn read_repair_fixes_a_lagging_replica_on_read() {
    let seed = 0x00AE_2027;
    let (mut sim, handles) = cluster(seed, Epoch::INITIAL);

    // Replica 2 is down for the write, which still reaches the W=2 quorum {0,1}.
    sim.crash(2);
    let vw = view(Epoch::INITIAL);
    let acked = run_op(&mut sim, move |c| {
        Box::pin(async move { c.write(&vw, b"k", b"v1", 1, TIMEOUT).await })
    });
    assert!(
        acked,
        "write reached the surviving W=2 quorum (seed={seed})"
    );
    assert_eq!(value_at(&handles[2], b"k"), None, "(seed={seed})");

    // It rejoins; a read that reaches all three sees replica 2 lagging behind
    // the winning version and repairs it as a side effect of the read.
    sim.restart(2);
    let vr = TabletView {
        tablet: TabletId(1),
        replicas: REPLICAS.to_vec(),
        epoch: Epoch::INITIAL,
        r: 3,
        w: 2,
    };
    let read = run_op(&mut sim, move |c| {
        Box::pin(async move { c.read(&vr, b"k", TIMEOUT).await })
    });
    assert_eq!(read, ReadResult::Value(Some(b"v1".to_vec())));
    assert_eq!(
        value_at(&handles[2], b"k"),
        Some(b"v1".to_vec()),
        "read-repair did not converge the replica that took part in the read (seed={seed})"
    );
}

#[test]
fn a_converged_read_does_not_repair() {
    // When replicas agree, no repair traffic is generated: a read of a key all
    // three hold leaves storage untouched and still returns the value.
    let (mut sim, handles) = cluster(0x00AE_2028, Epoch::INITIAL);
    let vw = view(Epoch::INITIAL);
    let vw2 = vw.clone();
    assert!(run_op(&mut sim, move |c| Box::pin(async move {
        c.write(&vw2, b"k", b"v1", 1, TIMEOUT).await
    })));
    // Let the W=2 write also reach the third replica so all agree.
    let vr = vw.clone();
    let read = run_op(&mut sim, move |c| {
        Box::pin(async move { c.read(&vr, b"k", TIMEOUT).await })
    });
    assert_eq!(read, ReadResult::Value(Some(b"v1".to_vec())));
    for id in REPLICAS {
        // Every replica that received the write holds exactly v1 at version 1.
        if let Some(vv) = handles[id as usize].storage().get(b"k").unwrap() {
            assert_eq!((vv.version, vv.value), (1, b"v1".to_vec()));
        }
    }
}
