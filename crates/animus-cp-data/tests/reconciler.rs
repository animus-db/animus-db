//! The execute half of the per-node tablet-host reconciler (ADR 0031 PR4):
//! `host::Reconciler` driven through a realistic host → narrow → release
//! sequence, asserting the same sibling-sparing erase-bound invariant
//! `narrow_scope.rs::narrow_then_erase_scope_spares_a_co_hosted_siblings_data`
//! proves at the bare `RaftKvNode` primitive level — but end to end through
//! the reconciler's own `plan`-then-execute `tick`, including a **real**
//! Raft membership change so `TabletFacts::config_excludes_me` (the
//! release-gate anchor, ADR 0029) comes from this node's own durable Raft
//! config, not an injected fact.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers). Per
//! the documented `SimEnv` gotcha, a `tick()` call whose planned action tears
//! a group down internally polls `env.sleep()` (waiting for the driver to
//! actually stop) — such a call must be spawned and driven via `run_for`,
//! never bare `block_on`'d, or it hangs forever with no panic.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_cp_data::host::{MetadataView, Reconciler};
use animus_cp_data::{RaftKvNode, StorageScope};
use animus_env::{Clock, EnvExt};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{KeyRange, Tablet, TabletId};
use futures::executor::block_on;

const BASE: u64 = 300;
const OTHER: u64 = 301;
const TABLE: &str = "t";
/// The boundary a "split" narrows the parent tablet to: keys `< BOUNDARY` stay
/// with tablet 1 (the parent), `>= BOUNDARY` belong to tablet 2 (the sibling
/// that ends up co-hosted on `BASE`'s one shared engine).
const BOUNDARY: &[u8] = b"m";

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn prefix_for(table: &str) -> Vec<u8> {
    table.as_bytes().to_vec()
}

fn physical(key: &[u8]) -> Vec<u8> {
    let mut out = prefix_for(TABLE);
    out.extend_from_slice(key);
    out
}

fn tablet(id: u64, start: &[u8], end: Option<&[u8]>, replicas: Vec<u64>) -> Tablet {
    Tablet::new_for_table(
        TabletId(id),
        TABLE,
        KeyRange::new(start.to_vec(), end.map(<[u8]>::to_vec)),
        replicas,
    )
}

fn view(tablets: impl IntoIterator<Item = Tablet>) -> MetadataView {
    MetadataView {
        tablets: tablets.into_iter().map(|t| (t.id, t)).collect(),
        down: Default::default(),
        merged: Default::default(),
    }
}

/// A bounded convergence poll — advance `sim` in `step` increments, calling
/// `check` after each, up to `budget` total — the "converged-or-timeout"
/// pattern this codebase uses for every eventual property instead of a fixed
/// sleep. Panics with `msg` if `check` never returns `true`.
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
    panic!("{msg}");
}

/// End-to-end: host a fresh whole-keyspace tablet, narrow it (as a split's
/// source tablet would), host a second, co-hosted "sibling" tablet on the
/// same node/engine (as a split's new child would), force a **real** Raft
/// membership change that excludes this node from the first tablet's group
/// (a realistic release condition, not an injected fact), then drive the
/// reconciler's own release-confirm dampener to completion and assert:
/// the released tablet's data is gone from this node's local engine, the
/// co-hosted sibling's data survives untouched, and the reconciler's
/// bookkeeping (`on_host`/`on_teardown` hooks, `LocalState`) converges
/// correctly.
#[test]
fn reconciler_hosts_narrows_releases_and_confirms_sparing_a_sibling() {
    let seed = 0x9EC0_0311;
    let mut sim = Simulator::new(seed);
    let base_env = sim.env(BASE);
    let other_env = sim.env(OTHER);

    let storage = MemoryEngine::new();
    let reconciler_storage = storage.clone();

    // `OTHER` is a second, independent real replica of tablet 1's group —
    // needed so this node (`BASE`) can be genuinely, durably excluded from
    // it later (a lone voter can never remove itself; `config_excludes_me`
    // needs a real second voter to take over and do the removal, exactly as
    // a live rebalance/repair move works in production).
    let other_storage = MemoryEngine::new();

    let hosted_log: Arc<Mutex<Vec<TabletId>>> = Arc::new(Mutex::new(Vec::new()));
    let teardown_log: Arc<Mutex<Vec<TabletId>>> = Arc::new(Mutex::new(Vec::new()));
    let final_hosted: Arc<Mutex<Option<Vec<TabletId>>>> = Arc::new(Mutex::new(None));
    let done = Arc::new(Mutex::new(false));

    let h_log = Arc::clone(&hosted_log);
    let t_log = Arc::clone(&teardown_log);
    let final_hosted2 = Arc::clone(&final_hosted);
    let done2 = Arc::clone(&done);
    let task_env = base_env.clone();
    let other_env_moved = other_env;

    base_env.clone().spawn_task(async move {
        let other_env = other_env_moved;
        let mut reconciler: Reconciler<SimEnv, MemoryEngine> = Reconciler::new(
            task_env.clone(),
            reconciler_storage,
            BASE,
            prefix_for,
            move |tablet, _node| h_log.lock().unwrap().push(tablet),
            move |tablet| t_log.lock().unwrap().push(tablet),
        );

        // Tablet 1: a fresh whole-keyspace tablet, replicated on {BASE, OTHER}
        // from the start (both real voters) so leadership can land on either.
        let v1 = view([tablet(1, b"", None, vec![BASE, OTHER])]);
        reconciler.tick(&v1).await;
        task_env.sleep(Duration::from_secs(2)).await; // elect

        // `OTHER`'s own replica of tablet 1 — constructed directly (not
        // through any reconciler; this test only needs a real second voter,
        // not a second node's full lifecycle).
        let other = KvNode::start_hosted(
            other_env,
            vec![BASE, OTHER],
            other_storage,
            StorageScope::new(prefix_for(TABLE), KeyRange::whole()),
            1,
        );
        task_env.sleep(Duration::from_secs(2)).await; // let both sides settle

        // Write into BOTH the soon-to-be-kept lower half ("a..", tablet 1's
        // own post-split range) and the soon-to-be-sibling upper half ("z..",
        // tablet 2's future range) — whichever replica currently leads.
        let base_h1 = reconciler.hosted_node(TabletId(1)).unwrap().clone();
        for i in 0..5u64 {
            let leader = if base_h1.is_leader() {
                &base_h1
            } else {
                &other
            };
            leader.put(
                format!("a{i:02}").into_bytes(),
                format!("lo{i}").into_bytes(),
            );
            leader.put(
                format!("z{i:02}").into_bytes(),
                format!("hi{i}").into_bytes(),
            );
        }
        task_env.sleep(Duration::from_secs(2)).await; // replicate + apply

        // Narrow tablet 1 to the lower half (the split's source-side effect).
        let v2 = view([tablet(1, b"", Some(BOUNDARY), vec![BASE, OTHER])]);
        reconciler.tick(&v2).await;

        // Host tablet 2 (the sibling / split child) on the SAME node/engine,
        // covering the upper half. `has_data` will find the "z.." keys
        // already present, so it forms with the full (single-voter) config.
        let v3 = view([
            tablet(1, b"", Some(BOUNDARY), vec![BASE, OTHER]),
            tablet(2, BOUNDARY, None, vec![BASE]),
        ]);
        reconciler.tick(&v3).await;
        task_env.sleep(Duration::from_secs(2)).await; // elect tablet 2

        // Sanity: the sibling sees its own data before anything is released.
        assert_eq!(
            reconciler
                .hosted_node(TabletId(2))
                .unwrap()
                .local_get(b"z00")
                .await,
            Some(b"hi0".to_vec()),
            "sanity: the sibling tablet must see its own data before release"
        );

        // Force a REAL membership change that excludes BASE from tablet 1's
        // group: if BASE currently leads, hand leadership to OTHER first
        // (`change_membership` forbids leader self-removal); then the leader
        // (now OTHER) removes BASE outright — an ordinary, non-self removal.
        let base_h1 = reconciler.hosted_node(TabletId(1)).unwrap().clone();
        if base_h1.is_leader() {
            let mut armed = false;
            for _ in 0..50 {
                if base_h1.transfer_leadership(OTHER) {
                    armed = true;
                    break;
                }
                task_env.sleep(Duration::from_millis(100)).await;
            }
            assert!(armed, "leadership transfer to OTHER never armed");
            for _ in 0..50 {
                task_env.sleep(Duration::from_millis(100)).await;
                if other.is_leader() {
                    break;
                }
            }
            assert!(
                other.is_leader(),
                "OTHER never took over tablet 1's leadership"
            );
        }
        let removed = other.change_membership([OTHER].into_iter().collect());
        assert!(
            matches!(removed, animus_control::ProposeResult::Accepted { .. }),
            "OTHER (leader) must accept removing BASE: {removed:?}"
        );
        // Let the removal commit and replicate down to BASE (a departing
        // peer keeps receiving the removal entry until it acks past it).
        for _ in 0..50 {
            task_env.sleep(Duration::from_millis(100)).await;
            if !base_h1.config().contains(&BASE) {
                break;
            }
        }
        assert!(
            !base_h1.config().contains(&BASE),
            "BASE's own durable Raft config never excluded it after removal"
        );

        // Now drive the reconciler's own release-confirm dampener: tablet 1
        // still exists in `Metadata`, but BASE is no longer in its replica
        // set — the tick view the reconciler must react to.
        let v4 = view([
            tablet(1, b"", Some(BOUNDARY), vec![OTHER]),
            tablet(2, BOUNDARY, None, vec![BASE]),
        ]);
        // A generous number of ticks: RELEASE_CONFIRM_TICKS consecutive
        // qualifying calls are required before Release actually fires.
        for _ in 0..10 {
            reconciler.tick(&v4).await;
            task_env.sleep(Duration::from_millis(50)).await;
        }

        *final_hosted2.lock().unwrap() =
            Some(reconciler.local_state().hosted.iter().copied().collect());
        *done2.lock().unwrap() = true;
    });

    poll_until(
        &mut sim,
        Duration::from_secs(60),
        Duration::from_secs(1),
        "the reconciler scenario task never completed",
        || *done.lock().unwrap(),
    );

    // --- Assertions -------------------------------------------------------

    // The reconciler's own bookkeeping converged: tablet 1 was torn down and
    // dropped from `hosted`; tablet 2 (the sibling) is still hosted.
    let hosted = final_hosted
        .lock()
        .unwrap()
        .clone()
        .expect("scenario task must have recorded final state");
    assert_eq!(
        hosted,
        vec![TabletId(2)],
        "tablet 1 must be released (dropped) and tablet 2 must remain hosted"
    );

    // The `on_host`/`on_teardown` hooks mirrored exactly what happened: both
    // tablets were hosted once, and only tablet 1 was ever torn down.
    assert_eq!(
        hosted_log.lock().unwrap().as_slice(),
        &[TabletId(1), TabletId(2)],
        "on_host must fire exactly once per tablet, in hosting order"
    );
    assert_eq!(
        teardown_log.lock().unwrap().as_slice(),
        &[TabletId(1)],
        "on_teardown must fire exactly once, for the released tablet only"
    );

    // The released tablet's own (narrowed) range is fully erased from this
    // node's local engine...
    for i in 0..5u64 {
        let key = physical(format!("a{i:02}").as_bytes());
        assert_eq!(
            block_on(storage.get(&key)).expect("engine read ok"),
            None,
            "released tablet 1's key a{i:02} must be erased from BASE's local engine"
        );
    }
    // ...but the co-hosted sibling's keys — on the SAME shared engine, SAME
    // table prefix — are completely untouched: this is the sibling-sparing
    // erase-bound invariant this whole design exists to make structural.
    for i in 0..5u64 {
        let key = physical(format!("z{i:02}").as_bytes());
        assert_eq!(
            block_on(storage.get(&key))
                .expect("engine read ok")
                .map(|vv| vv.value),
            Some(format!("hi{i}").into_bytes()),
            "co-hosted sibling tablet 2's key z{i:02} must survive tablet 1's release"
        );
    }
}
