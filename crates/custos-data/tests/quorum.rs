//! M4 acceptance: the thin end-to-end data-plane slice under simulation.
//!
//! A 3-replica tablet stores a key (W quorum) and reads it back (R quorum, with
//! R + W > N), survives one node kill without losing the acknowledged write,
//! fences operations bearing a stale epoch, and is byte-reproducible from a seed.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use custos_data::{DataClient, ReadResult, TabletView, serve_replica};
use custos_env::EnvExt;
use custos_sim::{SimEnv, Simulator};
use custos_storage::MemoryEngine;
use custos_tablet::Epoch;

const REPLICAS: [u64; 3] = [0, 1, 2];
const CLIENT: u64 = 10;
const TIMEOUT: Duration = Duration::from_secs(2);

fn view(epoch: Epoch) -> TabletView {
    // R + W = 4 > N = 3 ⇒ a read intersects every acknowledged write.
    TabletView {
        replicas: REPLICAS.to_vec(),
        epoch,
        r: 2,
        w: 2,
    }
}

/// Stand up three replicas at `epoch`, returning the sim and their handles.
fn cluster(seed: u64, epoch: Epoch) -> (Simulator, Vec<custos_data::ReplicaHandle<MemoryEngine>>) {
    let sim = Simulator::new(seed);
    let handles = REPLICAS
        .iter()
        .map(|&id| serve_replica(sim.env(id), MemoryEngine::new(), epoch))
        .collect();
    (sim, handles)
}

/// Run a single client operation to completion, returning its result.
fn run_op<T: Clone + Send + 'static>(
    sim: &mut Simulator,
    op: impl FnOnce(DataClient<SimEnv>) -> futures::future::BoxFuture<'static, T> + Send + 'static,
) -> T {
    let result: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let client_env = sim.env(CLIENT);
    let out = Arc::clone(&result);
    client_env.clone().spawn_task(async move {
        let client = DataClient::new(client_env);
        let value = op(client).await;
        *out.lock().unwrap() = Some(value);
    });
    sim.run();
    result
        .lock()
        .unwrap()
        .clone()
        .expect("client op did not complete")
}

#[test]
fn write_then_read_through_quorum() {
    let (mut sim, _h) = cluster(0xDA7A, Epoch::INITIAL);

    let v = view(Epoch::INITIAL);
    let v2 = v.clone();
    let acked = run_op(&mut sim, move |c| {
        Box::pin(async move { c.write(&v2, b"k", b"v1", 1, TIMEOUT).await })
    });
    assert!(acked, "write should reach the W=2 quorum");

    let read = run_op(&mut sim, move |c| {
        Box::pin(async move { c.read(&v, b"k", TIMEOUT).await })
    });
    assert_eq!(read, ReadResult::Value(Some(b"v1".to_vec())));
}

#[test]
fn acknowledged_write_survives_one_node_kill() {
    let seed = 0x0533_70DB;
    let (mut sim, _h) = cluster(seed, Epoch::INITIAL);

    let vw = view(Epoch::INITIAL);
    let acked = run_op(&mut sim, move |c| {
        Box::pin(async move { c.write(&vw, b"k", b"v1", 1, TIMEOUT).await })
    });
    assert!(acked, "write should be acknowledged (seed={seed})");

    // Kill one replica. With W=2, at least two replicas hold the value, so at
    // least one survivor still has it; R=2 from the two survivors observes it.
    sim.crash(0);

    let vr = view(Epoch::INITIAL);
    let read = run_op(&mut sim, move |c| {
        Box::pin(async move { c.read(&vr, b"k", TIMEOUT).await })
    });
    assert_eq!(
        read,
        ReadResult::Value(Some(b"v1".to_vec())),
        "acknowledged write lost after a single node kill (seed={seed})"
    );
}

#[test]
fn stale_epoch_operations_are_fenced() {
    // Replicas know epoch 2 (a topology change already happened).
    let (mut sim, _h) = cluster(0xFE7C, Epoch(2));

    // A coordinator acting on the old epoch 1 is fenced ⇒ no quorum.
    let stale = view(Epoch(1));
    let stale2 = stale.clone();
    let acked = run_op(&mut sim, move |c| {
        Box::pin(async move { c.write(&stale2, b"k", b"v", 1, TIMEOUT).await })
    });
    assert!(!acked, "a stale-epoch write must be fenced");

    let stale_read = run_op(&mut sim, move |c| {
        Box::pin(async move { c.read(&stale, b"k", TIMEOUT).await })
    });
    assert_eq!(
        stale_read,
        ReadResult::Failed,
        "a stale-epoch read must be fenced"
    );

    // The current epoch 2 is honored.
    let fresh = view(Epoch(2));
    let fresh2 = fresh.clone();
    let acked = run_op(&mut sim, move |c| {
        Box::pin(async move { c.write(&fresh2, b"k", b"v", 1, TIMEOUT).await })
    });
    assert!(acked, "a current-epoch write must succeed");
    let read = run_op(&mut sim, move |c| {
        Box::pin(async move { c.read(&fresh, b"k", TIMEOUT).await })
    });
    assert_eq!(read, ReadResult::Value(Some(b"v".to_vec())));
}

#[test]
fn run_is_byte_reproducible_from_seed() {
    fn scenario(seed: u64) -> (Vec<String>, bool, ReadResult) {
        let (mut sim, _h) = cluster(seed, Epoch::INITIAL);
        let vw = view(Epoch::INITIAL);
        let acked = run_op(&mut sim, move |c| {
            Box::pin(async move { c.write(&vw, b"k", b"v1", 1, TIMEOUT).await })
        });
        let vr = view(Epoch::INITIAL);
        let read = run_op(&mut sim, move |c| {
            Box::pin(async move { c.read(&vr, b"k", TIMEOUT).await })
        });
        (sim.trace_lines(), acked, read)
    }

    let a = scenario(0x1234_5678);
    let b = scenario(0x1234_5678);
    assert_eq!(a.0, b.0, "data-plane run was not byte-reproducible");
    assert_eq!((a.1, a.2), (b.1, b.2));
    assert!(!a.0.is_empty());
}
