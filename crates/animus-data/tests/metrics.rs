//! Data-plane observability counters move under a known workload (ADR 0015).
//!
//! The metrics seam ([`animus_env::metrics`]) is deterministic-safe: recording is
//! a relaxed atomic add, no wall clock, no `HashMap`, no I/O. `SimEnv::metrics()`
//! is the no-op default, so this test threads a *recording* [`MetricsHandle`] into
//! the coordinator (`DataClient::with_metrics`) and into the background loops
//! (`serve_anti_entropy_with_metrics`, `serve_hint_replay_with_metrics`) and reads
//! the counters back — no change to `animus-sim` is required to observe them.
//!
//! It asserts the data-plane counters move by the expected amounts under a known
//! workload (quorum write/read, a sub-quorum failure, an induced read-repair, a
//! buffered+delivered hint, and background anti-entropy rounds), and that the
//! recorded snapshot is byte-identical for the same seed (determinism).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_data::{
    DataClient, HintStore, ReadResult, ReplicaHandle, TabletView, serve_anti_entropy_with_metrics,
    serve_hint_replay_with_metrics, serve_replica,
};
use animus_env::{EnvExt, Metric, MetricsHandle};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{Epoch, TabletId};

const REPLICAS: [u64; 3] = [0, 1, 2];
const CLIENT: u64 = 10;
const TIMEOUT: Duration = Duration::from_secs(2);

/// R + W = 4 > N = 3 ⇒ a read intersects every acknowledged write.
fn view(epoch: Epoch) -> TabletView {
    TabletView {
        tablet: TabletId(1),
        replicas: REPLICAS.to_vec(),
        epoch,
        r: 2,
        w: 2,
    }
}

fn cluster(seed: u64, epoch: Epoch) -> (Simulator, Vec<ReplicaHandle<MemoryEngine>>) {
    let sim = Simulator::new(seed);
    let handles = REPLICAS
        .iter()
        .map(|&id| serve_replica(sim.env(id), MemoryEngine::new(), epoch))
        .collect();
    (sim, handles)
}

/// Run one coordinator op to completion, recording into `metrics` (a recording
/// handle the caller keeps so it can read counters back, since `SimEnv` uses the
/// no-op default).
fn run_op<T: Clone + Send + 'static>(
    sim: &mut Simulator,
    metrics: MetricsHandle,
    op: impl FnOnce(DataClient<SimEnv>) -> futures::future::BoxFuture<'static, T> + Send + 'static,
) -> T {
    let result: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let client_env = sim.env(CLIENT);
    let out = Arc::clone(&result);
    client_env.clone().spawn_task(async move {
        let client = DataClient::new(client_env).with_metrics(metrics);
        *out.lock().unwrap() = Some(op(client).await);
    });
    sim.run();
    result.lock().unwrap().clone().expect("op did not complete")
}

#[test]
fn quorum_and_repair_counters_move() {
    let seed = 0x0DA7_A015;
    let (mut sim, _h) = cluster(seed, Epoch::INITIAL);
    let m = MetricsHandle::recording();

    // --- A successful quorum write, then a successful quorum read. ---
    let vw = view(Epoch::INITIAL);
    let acked = run_op(&mut sim, m.clone(), move |c| {
        Box::pin(async move { c.write(&vw, b"k", b"v1", 1, TIMEOUT).await })
    });
    assert!(acked, "write should reach the W=2 quorum (seed={seed})");

    let vr = view(Epoch::INITIAL);
    let read = run_op(&mut sim, m.clone(), move |c| {
        Box::pin(async move { c.read(&vr, b"k", TIMEOUT).await })
    });
    assert_eq!(read, ReadResult::Value(Some(b"v1".to_vec())));

    let snap = m.snapshot();
    assert_eq!(
        snap.counters[&Metric::DataQuorumWritesAttempted],
        1,
        "one quorum write attempted (seed={seed}): {snap:?}"
    );
    assert_eq!(
        snap.counters[&Metric::DataQuorumWritesSucceeded],
        1,
        "the write committed (seed={seed}): {snap:?}"
    );
    assert_eq!(
        snap.counters[&Metric::DataQuorumWritesFailed],
        0,
        "no write failed (seed={seed}): {snap:?}"
    );
    assert_eq!(
        snap.counters[&Metric::DataQuorumReadsAttempted],
        1,
        "one quorum read attempted (seed={seed}): {snap:?}"
    );
    assert_eq!(
        snap.counters[&Metric::DataQuorumReadsSucceeded],
        1,
        "the read reached R=2 (seed={seed}): {snap:?}"
    );
    assert_eq!(
        snap.counters[&Metric::DataQuorumReadsFailed],
        0,
        "no read failed (seed={seed}): {snap:?}"
    );
    // No divergence on a fresh write+read ⇒ no read-repair.
    assert_eq!(
        snap.counters[&Metric::DataReadRepairTriggered],
        0,
        "no divergence yet ⇒ no read-repair (seed={seed}): {snap:?}"
    );
}

#[test]
fn sub_quorum_failure_and_read_repair_counters_move() {
    let seed = 0x50B0_F415u64;
    let (mut sim, _h) = cluster(seed, Epoch::INITIAL);
    let m = MetricsHandle::recording();

    // --- Sub-quorum write failure: crash two of three replicas so W=2 is
    //     unreachable; the lone survivor cannot form a quorum. ---
    sim.crash(1);
    sim.crash(2);
    let vw = view(Epoch::INITIAL);
    let acked = run_op(&mut sim, m.clone(), move |c| {
        Box::pin(async move { c.write(&vw, b"k", b"v1", 1, TIMEOUT).await })
    });
    assert!(!acked, "write must fail below W=2 (seed={seed})");
    assert_eq!(
        m.snapshot().counters[&Metric::DataQuorumWritesFailed],
        1,
        "the sub-quorum write is counted failed (seed={seed})"
    );

    // Recover the replicas, then commit a write to {0,1} with replica 2 still
    // missing it (crash 2 again for the duration of the write).
    sim.restart(1);
    sim.restart(2);
    sim.crash(2);
    let vw2 = view(Epoch::INITIAL);
    let acked = run_op(&mut sim, m.clone(), move |c| {
        Box::pin(async move { c.write(&vw2, b"k", b"v2", 2, TIMEOUT).await })
    });
    assert!(acked, "write reaches surviving {{0,1}} (seed={seed})");
    sim.restart(2);

    // --- Read-repair: force the stale replica 2 to participate by demanding
    //     R = 3. It returns an older/absent version ⇒ divergence ⇒ read-repair. ---
    let repair_before = m.snapshot();
    let vr = TabletView {
        tablet: TabletId(1),
        replicas: REPLICAS.to_vec(),
        epoch: Epoch::INITIAL,
        r: 3,
        w: 2,
    };
    let read = run_op(&mut sim, m.clone(), move |c| {
        Box::pin(async move { c.read(&vr, b"k", TIMEOUT).await })
    });
    assert_eq!(read, ReadResult::Value(Some(b"v2".to_vec())));
    let after = m.snapshot();
    assert_eq!(
        after.counters[&Metric::DataReadRepairTriggered]
            - repair_before.counters[&Metric::DataReadRepairTriggered],
        1,
        "the divergent R=3 read triggers exactly one read-repair (seed={seed}): {after:?}"
    );
    assert_eq!(
        after.counters[&Metric::DataReadRepairKeysRepaired]
            - repair_before.counters[&Metric::DataReadRepairKeysRepaired],
        1,
        "read-repair pushed back exactly one key (seed={seed}): {after:?}"
    );
}

#[test]
fn hint_stored_and_delivered_counters_move() {
    let seed = 0x1417_0DEF;
    let (mut sim, _h) = cluster(seed, Epoch::INITIAL);
    let m = MetricsHandle::recording();
    let store = HintStore::new();

    // The send-only replay loop on the coordinator's env redelivers buffered
    // hints each round; record its deliveries into the same handle.
    serve_hint_replay_with_metrics(
        sim.env(CLIENT),
        store.clone(),
        None,
        Duration::from_millis(50),
        m.clone(),
    );

    // Replica 2 is down while a hinting coordinator commits a write to {0,1}.
    sim.crash(2);
    let vw = view(Epoch::INITIAL);
    let store_w = store.clone();
    let m_w = m.clone();
    let result: Arc<Mutex<Option<bool>>> = Arc::new(Mutex::new(None));
    let out = Arc::clone(&result);
    let client_env = sim.env(CLIENT);
    client_env.clone().spawn_task(async move {
        let client = DataClient::with_hints(client_env, store_w, None).with_metrics(m_w);
        *out.lock().unwrap() = Some(client.write(&vw, b"k", b"v1", 1, TIMEOUT).await);
    });
    sim.run_for(Duration::from_millis(10));
    assert_eq!(
        result.lock().unwrap().clone(),
        Some(true),
        "write commits to surviving {{0,1}} (seed={seed})"
    );
    assert_eq!(
        m.snapshot().counters[&Metric::DataHintsStored],
        1,
        "a hint is buffered for the unreached replica 2 (seed={seed})"
    );

    // Replica 2 returns; the replay loop redelivers the buffered hint.
    sim.restart(2);
    sim.run_for(Duration::from_millis(200));
    assert!(
        m.snapshot().counters[&Metric::DataHintsDelivered] >= 1,
        "the buffered hint is replayed to the returning replica (seed={seed}): {:?}",
        m.snapshot()
    );
}

#[test]
fn anti_entropy_round_counter_moves() {
    let seed = 0x00AE_2026;
    let (mut sim, handles) = cluster(seed, Epoch::INITIAL);
    let m = MetricsHandle::recording();

    // Seed replica 0 with data so its anti-entropy rounds emit a non-empty
    // digest (an empty digest is skipped and not counted).
    sim.crash(1);
    sim.crash(2);
    // A direct merge into replica 0's storage (no quorum needed for the digest).
    futures::executor::block_on(async {
        use animus_storage::StorageEngine;
        handles[0].storage().merge(b"k", b"v1", 1).await.unwrap();
    });
    sim.restart(1);
    sim.restart(2);

    // Run replica 0's anti-entropy loop on a tight interval, recording rounds.
    serve_anti_entropy_with_metrics(
        sim.env(0),
        handles[0].clone(),
        TabletId(1),
        REPLICAS.to_vec(),
        Duration::from_millis(100),
        m.clone(),
    );
    sim.run_for(Duration::from_millis(350));

    assert!(
        m.snapshot().counters[&Metric::DataAntiEntropyRounds] >= 2,
        "the loop fired multiple non-empty rounds (seed={seed}): {:?}",
        m.snapshot()
    );
}

/// The recorded data-plane counters are a pure function of the seed: the same
/// seed yields a byte-identical text export of the coordinator's snapshot.
#[test]
fn data_metrics_are_reproducible_from_seed() {
    fn trace(seed: u64) -> String {
        let (mut sim, _h) = cluster(seed, Epoch::INITIAL);
        let m = MetricsHandle::recording();
        let vw = view(Epoch::INITIAL);
        let _ = run_op(&mut sim, m.clone(), move |c| {
            Box::pin(async move { c.write(&vw, b"k", b"v1", 1, TIMEOUT).await })
        });
        let vr = view(Epoch::INITIAL);
        let _ = run_op(&mut sim, m.clone(), move |c| {
            Box::pin(async move { c.read(&vr, b"k", TIMEOUT).await })
        });
        m.snapshot().to_text()
    }
    assert_eq!(trace(0x5EED_DA7A), trace(0x5EED_DA7A));
}
