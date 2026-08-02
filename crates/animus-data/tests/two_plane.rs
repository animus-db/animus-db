//! M4 integration: the two planes together (ADR 0001).
//!
//! The control plane (a 3-node Raft group) owns the tablet map; the data plane
//! (3 replica nodes) serves quorum reads/writes. A coordinator routes using a
//! tablet view it read from cached control-plane metadata — and keeps serving
//! after the control-plane leader is killed, because routing does not depend on
//! the control plane being live (only topology changes do).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::{MetaCommand, RaftNode};
use animus_data::{DataClient, ReadResult, TabletView, serve_replica};
use animus_env::EnvExt;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, TabletId};

const CONTROL: [u64; 3] = [0, 1, 2];
const DATA: [u64; 3] = [3, 4, 5];
const CLIENT: u64 = 10;
const TIMEOUT: Duration = Duration::from_secs(2);
const SETTLE: Duration = Duration::from_secs(2);

fn control_leader(nodes: &[RaftNode<SimEnv>], seed: u64) -> usize {
    let leaders: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(
        leaders.len(),
        1,
        "expected one control leader, got {leaders:?} (seed={seed})"
    );
    leaders[0]
}

/// Run a client op for a bounded amount of virtual time and return its result.
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
    sim.run_for(TIMEOUT);
    result
        .lock()
        .unwrap()
        .clone()
        .expect("client op did not complete")
}

#[test]
fn data_plane_routes_via_control_map_and_survives_control_outage() {
    let seed = 0x7405_9E11;
    let sim_owner = Simulator::new(seed);
    let mut sim = sim_owner;

    // Control plane.
    let control: Vec<RaftNode<SimEnv>> = CONTROL
        .iter()
        .map(|&id| RaftNode::start(sim.env(id), CONTROL.to_vec()))
        .collect();
    // Data plane.
    let _replicas: Vec<_> = DATA
        .iter()
        .map(|&id| {
            serve_replica(
                sim.env(id),
                MemoryEngine::new(),
                animus_tablet::Epoch::INITIAL,
            )
        })
        .collect();

    // Elect a control leader, then register the tablet (replicas = data nodes).
    sim.run_for(SETTLE);
    let leader = control_leader(&control, seed);
    control[leader].propose(MetaCommand::CreateTablet {
        tablet: TabletId(1),
        range: KeyRange::whole(),
        replicas: DATA.to_vec(),
    });
    sim.run_for(SETTLE);

    // The coordinator reads the tablet map from cached control metadata.
    let tablet = control[leader].metadata().tablets[&TabletId(1)].clone();
    assert_eq!(tablet.replicas, DATA.to_vec());
    let view = TabletView::from_tablet(&tablet, 2, 2);

    // Quorum write + read routed via that view.
    let v = view.clone();
    assert!(
        run_op(&mut sim, move |c| Box::pin(async move {
            c.write(&v, b"key", b"hello", 1, TIMEOUT).await
        })),
        "quorum write should be acknowledged"
    );
    let v = view.clone();
    assert_eq!(
        run_op(&mut sim, move |c| Box::pin(async move {
            c.read(&v, b"key", TIMEOUT).await
        })),
        ReadResult::Value(Some(b"hello".to_vec()))
    );

    // Kill the control-plane leader. The data plane must keep serving from the
    // cached view (ADR 0001): only topology changes need the control plane.
    sim.crash(leader as u64);

    let v = view.clone();
    assert!(
        run_op(&mut sim, move |c| Box::pin(async move {
            c.write(&v, b"key2", b"world", 2, TIMEOUT).await
        })),
        "data plane should keep serving writes during a control-plane outage"
    );
    let v = view.clone();
    assert_eq!(
        run_op(&mut sim, move |c| Box::pin(async move {
            c.read(&v, b"key2", TIMEOUT).await
        })),
        ReadResult::Value(Some(b"world".to_vec())),
        "data plane should keep serving reads during a control-plane outage"
    );

    // And losing a data replica too still preserves acknowledged writes.
    sim.crash(DATA[2]);
    let v = view.clone();
    assert_eq!(
        run_op(&mut sim, move |c| Box::pin(async move {
            c.read(&v, b"key", TIMEOUT).await
        })),
        ReadResult::Value(Some(b"hello".to_vec())),
        "acknowledged write lost after a data replica kill"
    );
}
