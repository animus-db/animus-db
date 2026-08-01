//! Multi-tablet routing: a coordinator routes each key to the tablet that owns
//! it (a different replica set per tablet), and epoch fencing is per tablet — a
//! topology change to one tablet does not fence operations on another.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use custos_data::{DataClient, ReadResult, Router, serve_replica};
use custos_env::EnvExt;
use custos_sim::{SimEnv, Simulator};
use custos_storage::MemoryEngine;
use custos_tablet::{Epoch, KeyRange, Tablet, TabletId};

const CLIENT: u64 = 10;
const TIMEOUT: Duration = Duration::from_secs(2);

/// Tablet 1 owns `[_, "m")` on replicas {3,4}; tablet 2 owns `["m", _)` on
/// replicas {4,5}. Node 4 serves both.
fn tablets() -> Vec<Tablet> {
    vec![
        Tablet::new(
            TabletId(1),
            KeyRange::new(Vec::new(), Some(b"m".to_vec())),
            vec![3, 4],
        ),
        Tablet::new(TabletId(2), KeyRange::new(b"m".to_vec(), None), vec![4, 5]),
    ]
}

fn cluster(seed: u64) -> (Simulator, Vec<custos_data::ReplicaHandle<MemoryEngine>>) {
    let sim = Simulator::new(seed);
    let handles = [3u64, 4, 5]
        .iter()
        .map(|&id| serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL))
        .collect();
    (sim, handles)
}

fn run_op<T: Clone + Send + 'static>(
    sim: &mut Simulator,
    op: impl FnOnce(DataClient<SimEnv>) -> futures::future::BoxFuture<'static, T> + Send + 'static,
) -> T {
    let result: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let env = sim.env(CLIENT);
    let out = Arc::clone(&result);
    env.clone().spawn_task(async move {
        let value = op(DataClient::new(env)).await;
        *out.lock().unwrap() = Some(value);
    });
    sim.run_for(Duration::from_secs(5));
    result.lock().unwrap().clone().expect("op completed")
}

#[test]
fn keys_route_to_their_owning_tablet() {
    let (mut sim, _h) = cluster(0x_7007);
    let router = Router::new(tablets(), 2, 2);

    // "apple" → tablet 1 ({3,4}); "zebra" → tablet 2 ({4,5}).
    assert_eq!(router.view_for(b"apple").unwrap().tablet, TabletId(1));
    assert_eq!(router.view_for(b"apple").unwrap().replicas, vec![3, 4]);
    assert_eq!(router.view_for(b"zebra").unwrap().tablet, TabletId(2));
    assert_eq!(router.view_for(b"zebra").unwrap().replicas, vec![4, 5]);

    for (key, val) in [
        (b"apple".to_vec(), b"red".to_vec()),
        (b"zebra".to_vec(), b"striped".to_vec()),
    ] {
        let r = router.clone();
        let acked = run_op(&mut sim, move |c| {
            let v = r.view_for(&key).unwrap();
            Box::pin(async move { c.write(&v, &key, &val, 1, TIMEOUT).await })
        });
        assert!(acked, "routed quorum write should be acknowledged");
    }

    // Reads route to the correct replica set and return the values.
    for (key, want) in [
        (b"apple".to_vec(), b"red".to_vec()),
        (b"zebra".to_vec(), b"striped".to_vec()),
    ] {
        let r = router.clone();
        let got = run_op(&mut sim, move |c| {
            let v = r.view_for(&key).unwrap();
            Box::pin(async move { c.read(&v, &key, TIMEOUT).await })
        });
        assert_eq!(got, ReadResult::Value(Some(want)));
    }
}

#[test]
fn epoch_fencing_is_per_tablet() {
    let (mut sim, handles) = cluster(0xFE_CE);
    let stale_router = Router::new(tablets(), 2, 2);

    // A topology change bumps tablet 1's epoch to 2 on its replicas {3,4}.
    // Tablet 2's epoch is untouched.
    for h in &handles {
        h.set_epoch(TabletId(1), Epoch(2));
    }

    // A write to a tablet-1 key with the stale (epoch-1) view is fenced...
    let r = stale_router.clone();
    let fenced = run_op(&mut sim, move |c| {
        let v = r.view_for(b"apple").unwrap();
        Box::pin(async move { c.write(&v, b"apple", b"x", 1, TIMEOUT).await })
    });
    assert!(!fenced, "stale-epoch write to tablet 1 must be fenced");

    // ...but a write to a tablet-2 key with the same router still succeeds,
    // because fencing is per tablet.
    let r = stale_router.clone();
    let acked = run_op(&mut sim, move |c| {
        let v = r.view_for(b"zebra").unwrap();
        Box::pin(async move { c.write(&v, b"zebra", b"y", 1, TIMEOUT).await })
    });
    assert!(
        acked,
        "tablet 2 ops must not be fenced by a tablet 1 topology change"
    );

    // Routing with the current epoch for tablet 1 succeeds again.
    let mut current = tablets();
    current[0].epoch = Epoch(2);
    let fresh_router = Router::new(current, 2, 2);
    let acked = run_op(&mut sim, move |c| {
        let v = fresh_router.view_for(b"apple").unwrap();
        Box::pin(async move { c.write(&v, b"apple", b"x", 2, TIMEOUT).await })
    });
    assert!(acked, "current-epoch write to tablet 1 must succeed");
}
