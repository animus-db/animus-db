//! Residency on the repair paths (ADR 0005 + 0010).
//!
//! ADR 0005 stresses that data residency is only as strong as its weakest path:
//! placement can pin a tablet's replicas to (say) EU nodes, but if read-repair
//! or background anti-entropy then pushes that data to a reachable non-EU node,
//! residency has leaked. These tests pin that gap shut: repair traffic is bound
//! to the tablet's residency-eligible placement on **both** sides — the eligible
//! replicas only send digests/syncs to each other, and they reject any repair
//! message arriving from a node outside the placement, even though it is fully
//! reachable on the network.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use custos_data::{
    DataClient, DataMsg, ReplicaHandle, TabletView, serve_anti_entropy, serve_replica,
    serve_replica_with_residency,
};
use custos_env::{EnvExt, Network, NodeId};
use custos_placement::{Candidate, PlacementPolicy};
use custos_sim::{SimEnv, Simulator};
use custos_storage::{MemoryEngine, StorageEngine};
use custos_tablet::{Epoch, TabletId};

// Eligible (EU) replicas of the tablet; node 3 (US) is reachable but ineligible.
const EU_REPLICAS: [u64; 3] = [0, 1, 2];
const US_NODE: u64 = 3;
const CLIENT: u64 = 10;
const TABLET: TabletId = TabletId(1);
const TIMEOUT: Duration = Duration::from_secs(2);

fn candidate(node: NodeId, region: &str) -> Candidate {
    let mut labels = BTreeMap::new();
    labels.insert("region".to_string(), region.to_string());
    Candidate::new(node, labels)
}

/// The residency-eligible peer set for an EU-only policy, derived from the same
/// `PlacementPolicy::admits` the control plane uses for placement (ADR 0005).
fn eu_allowed() -> BTreeSet<NodeId> {
    let policy = PlacementPolicy::simple("eu", 3).require_label("region", "eu");
    let candidates = [
        candidate(0, "eu"),
        candidate(1, "eu"),
        candidate(2, "eu"),
        candidate(US_NODE, "us"),
    ];
    let allowed: BTreeSet<NodeId> = candidates
        .iter()
        .filter(|c| policy.admits(c))
        .map(|c| c.node)
        .collect();
    // Sanity: the policy admits exactly the EU nodes, not the US node.
    assert_eq!(allowed, EU_REPLICAS.iter().copied().collect());
    allowed
}

fn view(epoch: Epoch) -> TabletView {
    TabletView {
        tablet: TABLET,
        replicas: EU_REPLICAS.to_vec(),
        epoch,
        r: 2,
        w: 2,
    }
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

fn value_at(handle: &ReplicaHandle<MemoryEngine>, key: &[u8]) -> Option<Vec<u8>> {
    futures::executor::block_on(handle.storage().get(key))
        .unwrap()
        .map(|vv| vv.value)
}

#[test]
fn anti_entropy_never_leaks_data_to_a_residency_ineligible_peer() {
    let seed = 0x0D5_2026;
    let allowed = eu_allowed();
    let sim = Simulator::new(seed);

    // The three EU replicas enforce residency on repair; the US node is a plain
    // reachable replica that should never come to hold the EU data.
    let eu: Vec<ReplicaHandle<MemoryEngine>> = EU_REPLICAS
        .iter()
        .map(|&id| {
            serve_replica_with_residency(
                sim.env(id),
                MemoryEngine::new(),
                Epoch::INITIAL,
                allowed.clone(),
            )
        })
        .collect();
    let us = serve_replica(sim.env(US_NODE), MemoryEngine::new(), Epoch::INITIAL);

    let mut sim = sim;

    // Land a write on the EU quorum {0,1} while replica 2 is isolated, so it must
    // be repaired (exercising the repair path that could leak).
    sim.partition_pair(2, 0);
    sim.partition_pair(2, 1);
    sim.partition_pair(2, CLIENT);

    let vw = view(Epoch::INITIAL);
    let acked = run_op(&mut sim, move |c| {
        Box::pin(async move { c.write(&vw, b"eu-secret", b"v1", 1, TIMEOUT).await })
    });
    assert!(
        acked,
        "write reached the EU W=2 quorum {{0,1}} (seed={seed})"
    );

    // Heal everything. Now run anti-entropy on the EU replicas, restricted to the
    // EU placement — and ALSO (adversarially) on the US node pointed at the EU
    // replicas, so the only thing stopping a leak is the residency guard.
    sim.heal(2, 0);
    sim.heal(2, 1);
    sim.heal(2, CLIENT);

    let interval = Duration::from_millis(50);
    for &id in &EU_REPLICAS {
        serve_anti_entropy(
            sim.env(id),
            eu[id as usize].clone(),
            TABLET,
            EU_REPLICAS.to_vec(), // residency-restricted: only EU peers
            interval,
        );
    }
    // The US node tries to participate in anti-entropy against the EU replicas.
    serve_anti_entropy(
        sim.env(US_NODE),
        us.clone(),
        TABLET,
        EU_REPLICAS.to_vec(),
        interval,
    );

    sim.run_for(Duration::from_secs(1));

    // Replica 2 converged via in-region anti-entropy...
    assert_eq!(
        value_at(&eu[2], b"eu-secret"),
        Some(b"v1".to_vec()),
        "in-region anti-entropy did not converge the lagging EU replica (seed={seed})"
    );
    // ...but the US node never received the data, even though it was reachable
    // and actively soliciting repair from the EU replicas.
    assert_eq!(
        value_at(&us, b"eu-secret"),
        None,
        "residency leaked: a non-EU node received EU data via repair (seed={seed})"
    );
}

#[test]
fn an_eligible_replica_rejects_a_direct_sync_from_an_ineligible_peer() {
    // The receive-side guard in isolation: a residency replica drops a `Sync`
    // (the read-repair / pull-response shape) that arrives from a node outside
    // its placement, so even a node that forges repair traffic cannot inject
    // out-of-region data.
    let seed = 0x0D5_2027;
    let allowed = eu_allowed();
    let sim = Simulator::new(seed);

    let eu0 = serve_replica_with_residency(
        sim.env(0),
        MemoryEngine::new(),
        Epoch::INITIAL,
        allowed.clone(),
    );
    let mut sim = sim;

    // The US node pushes a Sync straight at EU replica 0.
    let us_env = sim.env(US_NODE);
    us_env.clone().spawn_task(async move {
        let msg = DataMsg::Sync {
            tablet: TABLET,
            epoch: Epoch::INITIAL,
            entries: vec![(b"smuggled".to_vec(), Some(b"x".to_vec()), 1)],
        };
        us_env.send(0, serde_json::to_vec(&msg).unwrap()).await;
    });
    sim.run_for(Duration::from_millis(200));

    assert_eq!(
        value_at(&eu0, b"smuggled"),
        None,
        "residency leaked: an EU replica accepted a Sync from a non-EU node (seed={seed})"
    );

    // Control: the same Sync from an in-region peer (node 1) IS accepted.
    let eu1_env = sim.env(1);
    eu1_env.clone().spawn_task(async move {
        let msg = DataMsg::Sync {
            tablet: TABLET,
            epoch: Epoch::INITIAL,
            entries: vec![(b"legit".to_vec(), Some(b"y".to_vec()), 1)],
        };
        eu1_env.send(0, serde_json::to_vec(&msg).unwrap()).await;
    });
    sim.run_for(Duration::from_millis(200));
    assert_eq!(
        value_at(&eu0, b"legit"),
        Some(b"y".to_vec()),
        "an in-region Sync was wrongly rejected (seed={seed})"
    );
}
