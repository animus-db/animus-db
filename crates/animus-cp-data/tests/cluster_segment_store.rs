//! `ClusterSegmentStore` (ADR 0043 §A7b, decision F5): the K-replicated
//! **default** `SegmentStore` — a `SimEnv`, seed-reproducible, fault-
//! injecting corpus proving the durability contract this store exists for:
//! `put` only acks once every chosen replica has durably written the
//! object, `get_from` serves from any live recorded replica, and every
//! failure mode (a partitioned/crashed target, an ack-lost inner store,
//! a node dying mid-flight) fails cleanly within a bounded timeout rather
//! than hanging or silently losing data.
//!
//! **Harness.** Each test builds a small cluster of
//! `ClusterSegmentStore<SimEnv, SimSegmentStore>` handles — one per node,
//! each wrapping its own independent `SimSegmentStore` (the per-node local
//! building block) — via [`build_cluster`], `start`ing each store's serving
//! task immediately. A scenario that needs to interleave a fault injection
//! (crash/partition/heal) with an in-flight async call runs its whole body
//! as one spawned task carrying a cloned [`Simulator`] handle (mirroring
//! `tests/reconciler_corpus.rs`'s `run`/`poll_until` shape) so the fault can
//! land at a precise point in virtual time relative to the call, never a
//! bare `futures::executor::block_on` racing the simulator's own clock.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_cp_data::cluster_segment_store::{ClusterSegmentStore, StaticPlacementView};
use animus_env::test_support::assert_segment_store_contract;
use animus_env::{Clock, EnvExt, NodeId, SegmentStore, nid};
use animus_sim::{NetConfig, SegmentFaultConfig, SimEnv, SimSegmentStore, Simulator};
use futures::executor::block_on;

type ClusterStore = ClusterSegmentStore<SimEnv, SimSegmentStore>;
/// The outcome of a `get_from` call, stashed by a spawned scenario task for
/// the outer (non-async) test body to inspect once `run` returns.
type GetFromOutcome = std::io::Result<Option<Vec<u8>>>;

/// Bounds every scenario: generous relative to this module's own
/// `PUT_TIMEOUT`/`DELETE_TIMEOUT` (10s each) and `FETCH_ATTEMPT_TIMEOUT` × 3
/// attempts (9s), since a test can chain a couple of these calls.
const SCENARIO_BUDGET: Duration = Duration::from_secs(90);
const SCENARIO_STEP: Duration = Duration::from_millis(50);

/// A driver env id used only to host a scenario's own top-level script task
/// — never a cluster node id (900 is well clear of every test's own ids).
fn driver_id() -> NodeId {
    nid(900)
}

/// Build a cluster: one `ClusterSegmentStore` per id in `ids`, each backed by
/// its own fresh `SimSegmentStore`, sharing a [`StaticPlacementView`] whose
/// candidate set is every id in `ids` — and each store's serving task
/// already spawned (`start_with_k`), so a scenario can send it a request
/// immediately after this returns.
fn build_cluster(sim: &Simulator, ids: &[u64], k: usize) -> Vec<ClusterStore> {
    let node_ids: Vec<NodeId> = ids.iter().copied().map(nid).collect();
    ids.iter()
        .map(|&id| {
            let self_id = nid(id);
            let view = Arc::new(StaticPlacementView::new(self_id.clone(), node_ids.clone()));
            ClusterSegmentStore::start_with_k(
                sim.env(self_id.clone()),
                SimSegmentStore::new(sim.env(self_id)),
                view,
                k,
            )
        })
        .collect()
}

/// The cluster's store for node `id` (built by [`build_cluster`] from the
/// same `ids` slice).
fn store_for<'a>(stores: &'a [ClusterStore], ids: &[u64], id: u64) -> &'a ClusterStore {
    let idx = ids
        .iter()
        .position(|&x| x == id)
        .expect("node id in cluster");
    &stores[idx]
}

/// Drive `sim` in `step`-sized increments until `check` reports done or
/// `budget` is exhausted (a hang, not slowness, per the house doctrine on an
/// unbounded step — see the root `CLAUDE.md`).
fn poll_until(
    sim: &mut Simulator,
    budget: Duration,
    step: Duration,
    msg: &str,
    mut check: impl FnMut() -> bool,
) {
    let mut waited = Duration::ZERO;
    while waited < budget {
        sim.run_for(step);
        waited += step;
        if check() {
            return;
        }
    }
    panic!("{msg} (seed={})", sim.seed());
}

/// Run `body` (handed a cloned [`Simulator`], usable to script faults from
/// *inside* the async scenario) as a spawned task, driving the outer
/// `Simulator` to completion via [`poll_until`] — the one harness every test
/// below uses, mirroring `tests/reconciler_corpus.rs`'s `run`.
fn run<F, Fut>(seed: u64, body: F)
where
    F: FnOnce(Simulator) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut sim = Simulator::new(seed);
    let driver_env = sim.env(driver_id());
    let done = Arc::new(Mutex::new(false));
    let done2 = Arc::clone(&done);
    let sim_in_task = sim.clone();
    driver_env.spawn_task(async move {
        body(sim_in_task).await;
        *done2.lock().unwrap() = true;
    });
    poll_until(
        &mut sim,
        SCENARIO_BUDGET,
        SCENARIO_STEP,
        "scenario never completed",
        move || *done.lock().unwrap(),
    );
}

// ---------------------------------------------------------------------------
// Contract
// ---------------------------------------------------------------------------

/// The shared cross-implementation contract (`animus-env`'s own
/// `assert_segment_store_contract`, already pinned against `SimSegmentStore`
/// and `FsSegmentStore`) holds for `ClusterSegmentStore` too, exercised
/// through its trait surface (`put`/`get`/`delete`/`list`) over a 3-node
/// cluster at `K = 3` (so every id lands on this test's own node as one of
/// its replicas, letting the contract's own local-shaped assertions —
/// idempotent overwrite, `None` after delete, prefix-filtered `list` —
/// resolve without needing this store's best-effort remote fallback).
#[test]
fn satisfies_the_segment_store_contract() {
    run(1, |sim| async move {
        let ids = [0u64, 1, 2];
        let stores = build_cluster(&sim, &ids, 3);
        assert_segment_store_contract(&stores[0]).await;
    });
}

// ---------------------------------------------------------------------------
// put_replicated: happy path
// ---------------------------------------------------------------------------

/// All `K` chosen targets receive and durably store the object; the
/// returned replica set is exactly the `K` distinct nodes, sorted.
#[test]
fn put_replicated_happy_path_writes_to_every_chosen_replica() {
    let ids = [10u64, 11, 12];
    let result: Arc<Mutex<Option<Vec<NodeId>>>> = Arc::new(Mutex::new(None));
    let result2 = Arc::clone(&result);
    run(2, move |sim| async move {
        let stores = build_cluster(&sim, &ids, 3);
        let replicas = stores[0]
            .put_replicated("seg/happy-path", b"hello")
            .await
            .expect("put must succeed when every target is reachable");
        for (i, &id) in ids.iter().enumerate() {
            assert_eq!(
                stores[i].local().stored_ids(),
                vec!["seg/happy-path".to_string()],
                "node {id} must hold its own local copy of the object"
            );
        }
        *result2.lock().unwrap() = Some(replicas);
    });
    let mut expected: Vec<NodeId> = ids.iter().copied().map(nid).collect();
    expected.sort();
    assert_eq!(
        result.lock().unwrap().take().unwrap(),
        expected,
        "the returned replica set must be exactly the K distinct target nodes, sorted"
    );
}

// ---------------------------------------------------------------------------
// Partial-K: a target down before the put
// ---------------------------------------------------------------------------

/// A `put` fails cleanly (never hangs) while one of its `K` targets is
/// crashed, and never leaves a catalog-visible success — but retrying the
/// **same deterministic id** after the target heals succeeds, converging to
/// the identical bytes (idempotent overwrite covers whatever orphan copy the
/// failed attempt may have left on the healthy replicas).
#[test]
fn partial_k_target_down_fails_cleanly_then_retry_after_heal_succeeds_to_same_id() {
    run(3, |sim| async move {
        let ids = [20u64, 21, 22];
        let stores = build_cluster(&sim, &ids, 3);
        sim.crash(nid(22));

        let first = stores[0].put_replicated("seg/partial-k", b"x").await;
        assert!(
            first.is_err(),
            "a put must fail, not hang, while a chosen target is down: {first:?}"
        );

        sim.restart(nid(22));
        let second = stores[0]
            .put_replicated("seg/partial-k", b"x")
            .await
            .expect("retry after heal must succeed to the same id");
        assert_eq!(second.len(), 3, "the healed retry must reach every replica");
        for (i, &id) in ids.iter().enumerate() {
            assert_eq!(
                stores[i].local().stored_ids(),
                vec!["seg/partial-k".to_string()],
                "node {id} must hold exactly the retried object, no residue"
            );
        }
    });
}

// ---------------------------------------------------------------------------
// Ack-lost inner fault
// ---------------------------------------------------------------------------

/// An inner `SimSegmentStore` fault that writes the object but reports the
/// write as failed (`SegmentFaultConfig::set_put_ack_lost_prob`, the exact
/// ambiguity ADR 0043's seal step must tolerate) surfaces as a `put` error
/// at the `ClusterSegmentStore` level too — the object may exist on some
/// replicas despite the reported failure — and a retry (after clearing the
/// fault) converges every replica to the identical, final bytes.
#[test]
fn ack_lost_inner_fault_surfaces_as_an_error_then_retry_converges() {
    run(4, |sim| async move {
        let ids = [30u64, 31, 32];
        let stores = build_cluster(&sim, &ids, 3);
        let mut cfg = SegmentFaultConfig::default();
        cfg.set_put_ack_lost_prob(1.0);
        store_for(&stores, &ids, 32).local().set_fault_config(cfg);

        let first = stores[0].put_replicated("seg/ack-lost", b"y").await;
        assert!(
            first.is_err(),
            "an ack-lost replica must surface as a put failure: {first:?}"
        );
        // The ambiguity itself: the object landed on 32 despite the
        // reported failure (SimSegmentStore's own documented contract).
        assert_eq!(
            store_for(&stores, &ids, 32).local().stored_ids(),
            vec!["seg/ack-lost".to_string()],
            "the object must have actually landed even though the caller saw an error"
        );

        store_for(&stores, &ids, 32)
            .local()
            .set_fault_config(SegmentFaultConfig::default());
        let second = stores[0]
            .put_replicated("seg/ack-lost", b"y")
            .await
            .expect("retry with the fault cleared must converge");
        assert_eq!(second.len(), 3);
        for (i, &id) in ids.iter().enumerate() {
            assert_eq!(
                block_on(stores[i].local().get("seg/ack-lost")).expect("get"),
                Some(b"y".to_vec()),
                "node {id} must hold the identical final bytes after convergence"
            );
        }
    });
}

// ---------------------------------------------------------------------------
// Node death mid-put (killed between acks)
// ---------------------------------------------------------------------------

/// A target that dies **after** the `Store` message was sent but **before**
/// it is delivered (a nonzero network delay makes this deterministic: the
/// crash lands inside the delivery window) fails the put — the other two
/// targets may well have already durably stored the object, but the whole
/// call still reports failure, never a partial success — and a retry after
/// the node restarts succeeds.
#[test]
fn node_death_mid_put_fails_the_call_then_retry_after_restart_succeeds() {
    run(5, |sim| async move {
        let mut net_cfg = NetConfig::default();
        net_cfg.base_delay = Duration::from_millis(200);
        net_cfg.max_jitter = Duration::from_millis(0);
        sim.set_net_config(net_cfg);
        let ids = [40u64, 41, 42];
        let stores = build_cluster(&sim, &ids, 3);

        let sim_for_kill = sim.clone();
        let killer_env = sim.env(nid(999));
        killer_env.spawn_task(async move {
            // Shorter than the 200ms delivery delay above, so the `Store`
            // sent to node 42 is still in flight when this fires — the
            // simulator's own delivery-time crashed-check (not a send-time
            // one) is what makes this deterministic (see `fire_event` in
            // `animus-sim`).
            sim_for_kill
                .env(nid(999))
                .sleep(Duration::from_millis(50))
                .await;
            sim_for_kill.crash(nid(42));
        });

        let first = stores[0].put_replicated("seg/mid-put", b"z").await;
        assert!(
            first.is_err(),
            "a target dying in flight must fail the whole put, never a partial success: {first:?}"
        );

        sim.restart(nid(42));
        let second = stores[0]
            .put_replicated("seg/mid-put", b"z")
            .await
            .expect("retry after restart must succeed");
        assert_eq!(second.len(), 3);
    });
}

// ---------------------------------------------------------------------------
// get_from
// ---------------------------------------------------------------------------

/// `get_from` short-circuits to the **local** copy when this node is itself
/// one of the recorded replicas — proven by crashing the *other* recorded
/// replica outright: if the call had gone to the network first, it would
/// have to exhaust `FETCH_ATTEMPT_TIMEOUT` × `FETCH_ATTEMPTS` before ever
/// trying local, which this test's own budget (`SCENARIO_BUDGET`) does not
/// forbid, but the store's own success across a tight step count does not
/// exercise the deadline at all — a genuine local-first hit resolves within
/// the first couple of polling steps.
#[test]
fn get_from_short_circuits_to_local_when_self_is_a_replica() {
    let outcome: Arc<Mutex<Option<GetFromOutcome>>> = Arc::new(Mutex::new(None));
    let outcome2 = Arc::clone(&outcome);
    run(6, move |sim| async move {
        let ids = [50u64, 51];
        let stores = build_cluster(&sim, &ids, 2);
        stores[0]
            .put_replicated("seg/local-first", b"local")
            .await
            .expect("seed put");
        // The other recorded replica is now unreachable; a genuinely
        // local-first `get_from` never needs it.
        sim.crash(nid(51));

        let replicas: Vec<NodeId> = ids.iter().copied().map(nid).collect();
        let r = stores[0].get_from(&replicas, "seg/local-first").await;
        *outcome2.lock().unwrap() = Some(r);
    });
    assert_eq!(
        outcome
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .expect("get_from must succeed locally"),
        Some(b"local".to_vec())
    );
}

/// A reader that is **not** itself a recorded replica still gets the object
/// via `get_from`, served by whichever recorded replica is actually live —
/// proven by taking down one of the two recorded replicas and confirming the
/// call still succeeds via the survivor.
#[test]
fn get_from_serves_from_a_live_recorded_replica_while_another_is_down() {
    let outcome: Arc<Mutex<Option<GetFromOutcome>>> = Arc::new(Mutex::new(None));
    let outcome2 = Arc::clone(&outcome);
    run(7, move |sim| async move {
        let ids = [60u64, 61, 62];
        // K = 2 over 3 candidates: `select_replicas`'s deterministic,
        // least-loaded-domain-first (here: node-id-ascending, since there is
        // no spread policy) choice picks the two lowest ids, [60, 61] — node
        // 62 is never a replica for anything, making it the reader with no
        // local copy this test needs.
        let stores = build_cluster(&sim, &ids, 2);
        let replicas = stores[0]
            .put_replicated("seg/failover-read", b"survivor")
            .await
            .expect("seed put");
        assert_eq!(
            replicas,
            vec![nid(60), nid(61)],
            "sanity: expected replica set"
        );

        sim.crash(nid(60));
        let r = store_for(&stores, &ids, 62)
            .get_from(&replicas, "seg/failover-read")
            .await;
        *outcome2.lock().unwrap() = Some(r);
    });
    assert_eq!(
        outcome
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .expect("get_from must fail over to the live replica"),
        Some(b"survivor".to_vec())
    );
}

// ---------------------------------------------------------------------------
// delete_from
// ---------------------------------------------------------------------------

/// `delete_from` removes the id at every recorded replica and is idempotent:
/// deleting again (including an id already missing everywhere) is still
/// `Ok`.
#[test]
fn delete_from_is_idempotent_including_already_missing_replicas() {
    run(8, |sim| async move {
        let ids = [70u64, 71, 72];
        let stores = build_cluster(&sim, &ids, 3);
        let replicas = stores[0]
            .put_replicated("seg/delete-me", b"gone-soon")
            .await
            .expect("seed put");

        stores[0]
            .delete_from(&replicas, "seg/delete-me")
            .await
            .expect("first delete must succeed");
        for (i, &id) in ids.iter().enumerate() {
            assert!(
                stores[i].local().stored_ids().is_empty(),
                "node {id} must have no residue after delete_from"
            );
        }

        // Idempotent: deleting an already-gone id at every replica is Ok.
        stores[0]
            .delete_from(&replicas, "seg/delete-me")
            .await
            .expect("repeat delete of an already-missing id must be Ok");

        // Also idempotent for an id that was never written anywhere.
        stores[0]
            .delete_from(&replicas, "seg/never-written")
            .await
            .expect("delete of a never-written id must be Ok");
    });
}

// ---------------------------------------------------------------------------
// K = 1 degraded single-node mode
// ---------------------------------------------------------------------------

/// A single-node "cluster" (one candidate, the node itself) degrades `K`
/// from [`DEFAULT_K`](animus_cp_data::cluster_segment_store::DEFAULT_K) down
/// to `1` rather than refusing to serve — the shape a single-node dev
/// cluster needs.
#[test]
fn k_one_degraded_single_node_mode_still_serves_puts_and_gets() {
    run(9, |sim| async move {
        let ids = [80u64];
        let stores = build_cluster(&sim, &ids, animus_cp_data::cluster_segment_store::DEFAULT_K);
        let replicas = stores[0]
            .put_replicated("seg/solo", b"alone")
            .await
            .expect("a single-node cluster must still be able to put");
        assert_eq!(
            replicas,
            vec![nid(80)],
            "K must degrade to exactly 1 candidate"
        );

        assert_eq!(
            stores[0].get("seg/solo").await.expect("get"),
            Some(b"alone".to_vec())
        );
    });
}

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

/// Ten sequential `put_replicated` calls to distinct ids, over a 3-node
/// cluster where one node has a non-trivial (`0.4`) ack-lost probability —
/// the same seed must reproduce the identical Ok/Err sequence; a different
/// seed (almost certainly) diverges. Guards against a hidden nondeterministic
/// draw anywhere in the new fan-out/poll machinery (ADR 0003).
fn run_fault_scenario(seed: u64) -> Vec<bool> {
    let outcomes: Arc<Mutex<Vec<bool>>> = Arc::new(Mutex::new(Vec::new()));
    let outcomes2 = Arc::clone(&outcomes);
    run(seed, move |sim| async move {
        let ids = [90u64, 91, 92];
        let stores = build_cluster(&sim, &ids, 3);
        let mut cfg = SegmentFaultConfig::default();
        cfg.set_put_ack_lost_prob(0.4);
        store_for(&stores, &ids, 92).local().set_fault_config(cfg);

        for i in 0..10u32 {
            let id = format!("seg/determinism-{i}");
            let ok = stores[0].put_replicated(&id, b"v").await.is_ok();
            outcomes2.lock().unwrap().push(ok);
        }
    });
    Arc::try_unwrap(outcomes)
        .expect("no other Arc holders once the scenario completed")
        .into_inner()
        .expect("outcomes mutex poisoned")
}

#[test]
fn same_seed_reproduces_an_identical_outcome_sequence_under_faults() {
    let a = run_fault_scenario(42);
    let b = run_fault_scenario(42);
    assert_eq!(
        a, b,
        "identical seed must reproduce byte-identical outcomes"
    );
    assert!(
        a.contains(&true),
        "expected at least one successful put: {a:?}"
    );
    assert!(
        a.contains(&false),
        "expected at least one ack-lost failure: {a:?}"
    );

    let c = run_fault_scenario(43);
    assert_ne!(a, c, "a different seed must (almost certainly) diverge");
}
