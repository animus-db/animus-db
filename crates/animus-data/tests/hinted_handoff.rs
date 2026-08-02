//! Hinted handoff (ADR 0010 + 0005): a write that could not reach a down replica
//! is buffered as a *hint* at the coordinator and replayed to the replica the
//! moment it returns — so it converges promptly without waiting for a read or a
//! background anti-entropy round. Residency-bounded: a hint is never recorded for
//! or replayed to a replica the tablet's placement forbids.
//!
//! These assert convergence directly via the replica's storage (no read needed),
//! and are byte-reproducible from the printed seed.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_data::{
    DataClient, HintStore, ReplicaHandle, TabletView, serve_hint_handoff, serve_hint_replay,
    serve_replica, serve_replica_with_residency,
};
use animus_env::{EnvExt, NodeId};
use animus_placement::{Candidate, PlacementPolicy};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{Epoch, TabletId};

const REPLICAS: [u64; 3] = [0, 1, 2];
const CLIENT: u64 = 10;
const HOLDER: u64 = 11;
const TABLET: TabletId = TabletId(1);
const TIMEOUT: Duration = Duration::from_secs(2);
const INTERVAL: Duration = Duration::from_millis(50);

fn view(epoch: Epoch) -> TabletView {
    TabletView {
        tablet: TABLET,
        replicas: REPLICAS.to_vec(),
        epoch,
        r: 2,
        w: 2,
    }
}

fn value_at(handle: &ReplicaHandle<MemoryEngine>, key: &[u8]) -> Option<Vec<u8>> {
    futures::executor::block_on(handle.storage().get(key))
        .unwrap()
        .map(|vv| vv.value)
}

/// The replica's latest *raw* record for `key`, retaining tombstones.
fn raw_at(handle: &ReplicaHandle<MemoryEngine>, key: &[u8]) -> Option<(Option<Vec<u8>>, u64)> {
    futures::executor::block_on(handle.storage().entries_with_tombstones())
        .unwrap()
        .into_iter()
        .find(|(k, _, _)| k == key)
        .map(|(_, v, ver)| (v, ver))
}

/// Drive one coordinator op to completion on the `CLIENT` node, recording hints
/// into `store` (residency `allowed`). The coordinator and the hint-handoff
/// holder run on **distinct** node ids, so they never contend on a single
/// inbox: the coordinator (`CLIENT`) issues the write/read; the holder (`HOLDER`)
/// owns the replay loop.
fn run_op<T: Clone + Send + 'static>(
    sim: &mut Simulator,
    store: HintStore,
    allowed: Option<BTreeSet<NodeId>>,
    op: impl FnOnce(DataClient<SimEnv>) -> futures::future::BoxFuture<'static, T> + Send + 'static,
) -> T {
    let result: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let client_env = sim.env(CLIENT);
    let out = Arc::clone(&result);
    client_env.clone().spawn_task(async move {
        let client = DataClient::with_hints(client_env, store, allowed);
        *out.lock().unwrap() = Some(op(client).await);
    });
    sim.run();
    result.lock().unwrap().clone().expect("op did not complete")
}

#[test]
fn a_hint_is_buffered_for_a_down_replica_and_replayed_on_recovery() {
    let seed = 0x0418_7000u64;
    let sim = Simulator::new(seed);
    let handles: Vec<ReplicaHandle<MemoryEngine>> = REPLICAS
        .iter()
        .map(|&id| serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL))
        .collect();
    let mut sim = sim;

    // Replica 2 is down while a write commits to the W=2 quorum {0,1}.
    sim.crash(2);
    let store = HintStore::new();
    let vw = view(Epoch::INITIAL);
    let acked = run_op(&mut sim, store.clone(), None, move |c| {
        Box::pin(async move { c.write(&vw, b"k", b"v1", 1, TIMEOUT).await })
    });
    assert!(
        acked,
        "write reached the surviving W=2 quorum (seed={seed:#x})"
    );
    assert_eq!(
        value_at(&handles[2], b"k"),
        None,
        "the down replica missed the write (seed={seed:#x})"
    );
    // A hint was buffered for the unreached replica (exactly one: replica 2).
    assert_eq!(
        store.len(),
        1,
        "a hint must be buffered for the down replica (seed={seed:#x})"
    );

    // Replica 2 recovers; *nothing reads `k`* and no anti-entropy runs — only the
    // hint-handoff replay loop (on its own HOLDER node) can converge it.
    sim.restart(2);
    serve_hint_handoff(sim.env(HOLDER), store.clone(), None, INTERVAL);
    sim.run_for(Duration::from_secs(1));

    assert_eq!(
        value_at(&handles[2], b"k"),
        Some(b"v1".to_vec()),
        "hinted handoff did not replay the missed write to the recovered replica (seed={seed:#x})"
    );
    assert!(
        store.is_empty(),
        "the hint must be cleared once delivered (seed={seed:#x})"
    );
}

#[test]
fn a_hinted_delete_replays_as_a_tombstone() {
    let seed = 0x0418_7001u64;
    let sim = Simulator::new(seed);
    let handles: Vec<ReplicaHandle<MemoryEngine>> = REPLICAS
        .iter()
        .map(|&id| serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL))
        .collect();
    let mut sim = sim;

    // Land v1 on all three replicas first, so replica 2 genuinely holds the value
    // it must later forget. Use W=3 so the write only commits once *every* replica
    // acks — then no hint is buffered (the point being contrasted with the delete).
    let store = HintStore::new();
    let vw = view(Epoch::INITIAL);
    let vw_all = TabletView { w: 3, ..vw.clone() };
    assert!(run_op(&mut sim, store.clone(), None, move |c| Box::pin(
        async move { c.write(&vw_all, b"k", b"v1", 1, TIMEOUT).await }
    )));
    assert_eq!(value_at(&handles[2], b"k"), Some(b"v1".to_vec()));
    assert!(store.is_empty(), "no hint when every replica acked");

    // Replica 2 down while a DELETE commits to {0,1}: a tombstone hint is buffered.
    sim.crash(2);
    let vd = vw.clone();
    assert!(run_op(&mut sim, store.clone(), None, move |c| Box::pin(
        async move { c.delete(&vd, b"k", 2, TIMEOUT).await }
    )));
    assert_eq!(
        store.len(),
        1,
        "a tombstone hint buffered for the down replica"
    );
    assert_eq!(
        raw_at(&handles[2], b"k"),
        Some((Some(b"v1".to_vec()), 1)),
        "the down replica still holds the value — it missed the delete (seed={seed:#x})"
    );

    // It recovers; the hint-handoff loop replays the tombstone.
    sim.restart(2);
    serve_hint_handoff(sim.env(HOLDER), store.clone(), None, INTERVAL);
    sim.run_for(Duration::from_secs(1));

    assert_eq!(
        raw_at(&handles[2], b"k"),
        Some((None, 2)),
        "hinted handoff did not replay the tombstone (seed={seed:#x})"
    );
    assert_eq!(value_at(&handles[2], b"k"), None);
}

#[test]
fn no_hint_is_buffered_or_replayed_for_a_residency_ineligible_replica() {
    // The residency bound (ADR 0005): if the placement forbids a replica, the
    // coordinator never buffers a hint for it, and the handoff loop never replays
    // to it — even though it is a (mis-)listed replica in the view and reachable.
    let seed = 0x0418_7002u64;
    let sim = Simulator::new(seed);

    // EU policy admits replicas 0,1; replica 2 is "US" — ineligible.
    let policy = PlacementPolicy::simple("eu", 2).require_label("region", "eu");
    let candidate = |node: NodeId, region: &str| {
        let mut labels = BTreeMap::new();
        labels.insert("region".to_string(), region.to_string());
        Candidate::new(node, labels)
    };
    let candidates = [candidate(0, "eu"), candidate(1, "eu"), candidate(2, "us")];
    let allowed: BTreeSet<NodeId> = candidates
        .iter()
        .filter(|c| policy.admits(c))
        .map(|c| c.node)
        .collect();
    assert_eq!(
        allowed,
        BTreeSet::from([0, 1]),
        "policy admits exactly the EU nodes"
    );

    // All three replicas enforce residency on their receive side too (defence in
    // depth): even if a hint reached the US replica it would be dropped.
    let handles: Vec<ReplicaHandle<MemoryEngine>> = REPLICAS
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
    let mut sim = sim;

    // The view (mistakenly / adversarially) lists all three replicas, but only the
    // EU quorum {0,1} is up for the write; replica 2 (US) is down.
    sim.crash(2);
    let store = HintStore::new();
    let vw = view(Epoch::INITIAL); // r=2, w=2 over {0,1,2}
    let allowed_for_op = Some(allowed.clone());
    let acked = run_op(&mut sim, store.clone(), allowed_for_op, move |c| {
        Box::pin(async move { c.write(&vw, b"eu-secret", b"v1", 1, TIMEOUT).await })
    });
    assert!(
        acked,
        "write reached the EU W=2 quorum {{0,1}} (seed={seed:#x})"
    );

    // No hint was buffered for the ineligible US replica, even though it was the
    // one that missed the write.
    assert!(
        store.is_empty(),
        "a hint was buffered for a residency-ineligible replica (seed={seed:#x})"
    );

    // Recover the US replica and run the handoff loop; with no hint to replay (and
    // the receive-side guard as backstop) it must never receive the EU data.
    sim.restart(2);
    serve_hint_handoff(
        sim.env(HOLDER),
        store.clone(),
        Some(allowed.clone()),
        INTERVAL,
    );
    sim.run_for(Duration::from_secs(1));

    assert_eq!(
        value_at(&handles[2], b"eu-secret"),
        None,
        "residency leaked: the ineligible replica received hinted data (seed={seed:#x})"
    );

    // Control: an eligible replica that had been down WOULD be hinted. Re-run with
    // replica 1 (eligible) down instead. The replicas' receive-side residency set
    // also admits the HOLDER node — the hint holder (a coordinator) is a trusted
    // in-region participant, exactly as the coordinator already is for read-repair.
    let sim2 = Simulator::new(seed.wrapping_add(1));
    let mut allowed_with_holder = allowed.clone();
    allowed_with_holder.insert(HOLDER);
    let handles2: Vec<ReplicaHandle<MemoryEngine>> = REPLICAS
        .iter()
        .map(|&id| {
            serve_replica_with_residency(
                sim2.env(id),
                MemoryEngine::new(),
                Epoch::INITIAL,
                allowed_with_holder.clone(),
            )
        })
        .collect();
    let mut sim2 = sim2;
    // Quorum over the eligible EU replicas {0,1}; replica 1 down, replica 0 acks.
    let eu_view = TabletView {
        tablet: TABLET,
        replicas: vec![0, 1],
        epoch: Epoch::INITIAL,
        r: 1,
        w: 1,
    };
    sim2.crash(1);
    let store2 = HintStore::new();
    let allowed2 = Some(allowed.clone());
    let acked2 = {
        let result: Arc<Mutex<Option<bool>>> = Arc::new(Mutex::new(None));
        let client_env = sim2.env(CLIENT);
        let out = Arc::clone(&result);
        let store_c = store2.clone();
        client_env.clone().spawn_task(async move {
            let client = DataClient::with_hints(client_env, store_c, allowed2);
            *out.lock().unwrap() = Some(client.write(&eu_view, b"k2", b"v2", 1, TIMEOUT).await);
        });
        sim2.run();
        result.lock().unwrap().take().expect("op did not complete")
    };
    assert!(
        acked2,
        "write reached the W=1 quorum {{0}} (seed={seed:#x})"
    );
    assert_eq!(
        store2.len(),
        1,
        "an eligible down replica MUST be hinted (control) (seed={seed:#x})"
    );
    sim2.restart(1);
    serve_hint_handoff(
        sim2.env(HOLDER),
        store2.clone(),
        Some(allowed.clone()),
        INTERVAL,
    );
    sim2.run_for(Duration::from_secs(1));
    assert_eq!(
        value_at(&handles2[1], b"k2"),
        Some(b"v2".to_vec()),
        "hinted handoff did not converge an eligible replica (seed={seed:#x})"
    );
}

#[test]
fn send_only_replay_converges_a_replica_that_recovers_a_later_round() {
    // The send-only variant `serve_hint_replay` (what `animusd` wires onto the
    // shared coord env) cannot probe, so it re-sends each round. A replica still
    // down when the loop starts must still converge once it returns a few rounds
    // later — the loop does not drain a hint until it is superseded.
    let seed = 0x0418_7003u64;
    let sim = Simulator::new(seed);
    let handles: Vec<ReplicaHandle<MemoryEngine>> = REPLICAS
        .iter()
        .map(|&id| serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL))
        .collect();
    let mut sim = sim;

    sim.crash(2);
    let store = HintStore::new();
    let vw = view(Epoch::INITIAL);
    let acked = run_op(&mut sim, store.clone(), None, move |c| {
        Box::pin(async move { c.write(&vw, b"k", b"v1", 1, TIMEOUT).await })
    });
    assert!(acked, "write reached the W=2 quorum (seed={seed:#x})");
    assert_eq!(store.len(), 1);

    // Start the send-only replay loop while replica 2 is STILL down: its first few
    // rounds send into the void (replica 2 is crashed/muted).
    serve_hint_replay(sim.env(HOLDER), store.clone(), None, INTERVAL);
    sim.run_for(Duration::from_millis(200));
    assert_eq!(
        value_at(&handles[2], b"k"),
        None,
        "replica 2 is still down — nothing converged yet (seed={seed:#x})"
    );

    // Replica 2 recovers; a later replay round delivers the buffered hint.
    sim.restart(2);
    sim.run_for(Duration::from_secs(1));
    assert_eq!(
        value_at(&handles[2], b"k"),
        Some(b"v1".to_vec()),
        "send-only replay did not converge the recovered replica (seed={seed:#x})"
    );
}
