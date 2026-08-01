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

/// The replica's latest *raw* record for `key`: `(Some(value)|None, version)`,
/// retaining tombstones (unlike [`value_at`], which hides deleted keys). `None`
/// for the value means a tombstone is the latest entry.
fn raw_at(
    handle: &custos_data::ReplicaHandle<MemoryEngine>,
    key: &[u8],
) -> Option<(Option<Vec<u8>>, u64)> {
    handle
        .storage()
        .entries_with_tombstones()
        .unwrap()
        .into_iter()
        .find(|(k, _, _)| k == key)
        .map(|(_, v, ver)| (v, ver))
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

// A replica isolated by a partition for the delete (rather than crashed) still
// runs its serve loop, so a healed partition lets background anti-entropy push
// the tombstone in. (A crash that drops the parked `recv` wakeup is a separate
// simulator concern; partitions exercise the same "missed the delete" gap.)
fn isolate(sim: &Simulator, node: u64) {
    for &id in REPLICAS.iter().chain(std::iter::once(&CLIENT)) {
        if id != node {
            sim.partition_pair(node, id);
        }
    }
}

fn rejoin(sim: &Simulator, node: u64) {
    for &id in REPLICAS.iter().chain(std::iter::once(&CLIENT)) {
        if id != node {
            sim.heal(node, id);
        }
    }
}

fn start_anti_entropy(
    sim: &Simulator,
    handles: &[custos_data::ReplicaHandle<MemoryEngine>],
    epoch: Epoch,
) {
    for &id in &REPLICAS {
        serve_anti_entropy(
            sim.env(id),
            handles[id as usize].storage().clone(),
            TabletId(1),
            epoch,
            REPLICAS.to_vec(),
            Duration::from_millis(50),
        );
    }
}

#[test]
fn anti_entropy_propagates_a_tombstone_to_a_lagging_replica_without_reads() {
    // A delete must converge the same way a write does: a replica that holds the
    // value but missed the delete is brought to the tombstone by background
    // anti-entropy alone, with *no read* to trigger read-repair (ADR 0010).
    let seed = 0x00AE_2029;
    let (mut sim, handles) = cluster(seed, Epoch::INITIAL);
    let vw = view(Epoch::INITIAL);

    // First land v1 on all three replicas so replica 2 genuinely holds the value
    // it will later have to *forget* — not merely "never saw the key".
    let vw1 = vw.clone();
    assert!(
        run_op(&mut sim, move |c| Box::pin(async move {
            c.write(&vw1, b"k", b"v1", 1, TIMEOUT).await
        })),
        "initial write reached a quorum (seed={seed})"
    );
    sim.run_for(Duration::from_millis(100)); // let W=2 reach replica 2 too
    assert_eq!(
        value_at(&handles[2], b"k"),
        Some(b"v1".to_vec()),
        "replica 2 holds v1 before it is isolated (seed={seed})"
    );

    // Replica 2 is partitioned away while a DELETE commits to the quorum {0,1}.
    isolate(&sim, 2);
    let vd = vw.clone();
    let deleted = run_op(&mut sim, move |c| {
        Box::pin(async move { c.delete(&vd, b"k", 2, TIMEOUT).await })
    });
    assert!(
        deleted,
        "delete reached the surviving W=2 quorum (seed={seed})"
    );
    assert_eq!(
        raw_at(&handles[2], b"k"),
        Some((Some(b"v1".to_vec()), 1)),
        "the isolated replica still holds v1 — it missed the delete (seed={seed})"
    );

    // Heal, but *nothing reads `k`* — only background anti-entropy runs.
    rejoin(&sim, 2);
    start_anti_entropy(&sim, &handles, Epoch::INITIAL);
    sim.run_for(Duration::from_secs(1));

    // The tombstone (version 2) has propagated: the key now reads as absent and
    // the raw record is a tombstone, not the stale value.
    assert_eq!(
        raw_at(&handles[2], b"k"),
        Some((None, 2)),
        "anti-entropy did not propagate the tombstone to the lagging replica (seed={seed})"
    );
    assert_eq!(
        value_at(&handles[2], b"k"),
        None,
        "the lagging replica still serves the deleted value (seed={seed})"
    );
}

#[test]
fn anti_entropy_does_not_resurrect_a_deleted_key() {
    // Anti-entropy carries tombstones, so the lagging replica pushing its stale
    // value back must not resurrect the key anywhere: the delete (v2) wins by
    // per-key LWW on every replica, including the one that held the value.
    let seed = 0x00AE_202A;
    let (mut sim, handles) = cluster(seed, Epoch::INITIAL);
    let vw = view(Epoch::INITIAL);

    let vw1 = vw.clone();
    assert!(run_op(&mut sim, move |c| Box::pin(async move {
        c.write(&vw1, b"k", b"v1", 1, TIMEOUT).await
    })));
    sim.run_for(Duration::from_millis(100));

    isolate(&sim, 2);
    let vd = vw.clone();
    assert!(run_op(&mut sim, move |c| Box::pin(async move {
        c.delete(&vd, b"k", 2, TIMEOUT).await
    })));
    rejoin(&sim, 2);

    // Anti-entropy on every replica, including replica 2 (which pushes its stale
    // v1 back). LWW (delete v2 > value v1) must win everywhere.
    start_anti_entropy(&sim, &handles, Epoch::INITIAL);
    sim.run_for(Duration::from_secs(1));

    for id in REPLICAS {
        assert_eq!(
            raw_at(&handles[id as usize], b"k"),
            Some((None, 2)),
            "replica {id} did not converge to the tombstone (seed={seed})"
        );
    }
}
