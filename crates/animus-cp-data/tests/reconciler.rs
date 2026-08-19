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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_cp_data::host::{EngineFactory, MemoryTabletEngines, MetadataView, Reconciler};
use animus_cp_data::{RaftKvNode, StorageScope};
use animus_env::{Clock, EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{KeyRange, Tablet, TabletId};
use futures::executor::block_on;

const BASE: u64 = 300;
const OTHER: u64 = 301;
const TABLE: &str = "t";
type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// F2b (ADR 0050 rung 2): a group's physical key is its row-kind byte plus
/// the logical key — no table prefix, no tablet identity in the bytes.
fn physical(key: &[u8]) -> Vec<u8> {
    let mut out = vec![animus_cp_data::KIND_BASE];
    out.extend_from_slice(key);
    out
}

fn tablet(id: u64, start: &[u8], end: Option<&[u8]>, replicas: Vec<u64>) -> Tablet {
    Tablet::new_for_table(
        TabletId(id),
        TABLE,
        KeyRange::new(start.to_vec(), end.map(<[u8]>::to_vec)),
        replicas.into_iter().map(nid).collect(),
    )
}

fn view(tablets: impl IntoIterator<Item = Tablet>) -> MetadataView {
    MetadataView {
        tablets: tablets.into_iter().map(|t| (t.id, t)).collect(),
        ..Default::default()
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

/// End-to-end (ADR 0050 rung 1): host a fresh whole-keyspace tablet, host a
/// second, co-hosted tablet of the SAME table on the same node — each with
/// its OWN private engine — write the SAME logical keys through both groups
/// (physically impossible to hold independently on the old shared engine,
/// where both mapped to one physical key and last-writer-wins collapsed
/// them), force a **real** Raft membership change that excludes this node
/// from the first tablet's group, drive the reconciler's release-confirm
/// dampener to completion, and assert: the released tablet's engine is
/// destroyed whole (reads back fresh/empty), the co-hosted sibling's engine
/// — same table, same logical keys — survives untouched, and the
/// reconciler's bookkeeping (`on_host`/`on_teardown` hooks, `LocalState`)
/// converges correctly.
#[test]
fn reconciler_hosts_releases_and_spares_a_co_hosted_siblings_own_engine() {
    let seed = 0x9EC0_0311;
    let mut sim = Simulator::new(seed);
    let base_env = sim.env(nid(BASE));
    let other_env = sim.env(nid(OTHER));

    // ADR 0050 rung 1: BASE's per-tablet engine registry — the reconciler
    // opens one private engine per hosted tablet through it, and the test
    // asserts against specific tablets' engines below.
    let engines = MemoryTabletEngines::new();
    let reconciler_engines = engines.clone();

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
            reconciler_engines,
            nid(BASE),
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
        // not a second node's full lifecycle), on its own private engine.
        let other = KvNode::start_hosted(
            other_env,
            vec![nid(BASE), nid(OTHER)],
            other_storage,
            StorageScope::new(KeyRange::whole()),
            1,
        );
        task_env.sleep(Duration::from_secs(2)).await; // let both sides settle

        // Write through tablet 1 — both "a.." keys (t1's own) and "z.."
        // keys (which the co-hosted sibling will ALSO write, differently).
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
                format!("t1z{i}").into_bytes(),
            );
        }
        task_env.sleep(Duration::from_secs(2)).await; // replicate + apply

        // Host tablet 2 — SAME table, SAME node, overlapping range, its own
        // fresh private engine (empty: nothing is inherited).
        let v3 = view([
            tablet(1, b"", None, vec![BASE, OTHER]),
            tablet(2, b"", None, vec![BASE]),
        ]);
        reconciler.tick(&v3).await;
        task_env.sleep(Duration::from_secs(2)).await; // elect tablet 2

        let h2 = reconciler.hosted_node(TabletId(2)).unwrap().clone();
        assert_eq!(
            h2.local_get(b"z00").await,
            None,
            "a fresh sibling's private engine must start EMPTY - no zero-copy inheritance"
        );
        for i in 0..5u64 {
            h2.put(
                format!("z{i:02}").into_bytes(),
                format!("hi{i}").into_bytes(),
            );
        }
        task_env.sleep(Duration::from_secs(1)).await;

        // THE rung-1 property: the same logical key holds two different
        // values in the two co-hosted tablets, independently readable.
        assert_eq!(
            base_h1.local_get(b"z00").await,
            Some(b"t1z0".to_vec()),
            "tablet 1 must keep ITS OWN value for the shared logical key"
        );
        assert_eq!(
            h2.local_get(b"z00").await,
            Some(b"hi0".to_vec()),
            "tablet 2 must hold ITS OWN value for the same logical key"
        );

        // Force a REAL membership change that excludes BASE from tablet 1's
        // group: if BASE currently leads, hand leadership to OTHER first
        // (`change_membership` forbids leader self-removal); then the leader
        // (now OTHER) removes BASE outright — an ordinary, non-self removal.
        let base_h1 = reconciler.hosted_node(TabletId(1)).unwrap().clone();
        if base_h1.is_leader() {
            let mut armed = false;
            for _ in 0..50 {
                if base_h1.transfer_leadership(nid(OTHER)) {
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
        let removed = other.change_membership([OTHER].into_iter().map(nid).collect());
        assert!(
            matches!(removed, animus_control::ProposeResult::Accepted { .. }),
            "OTHER (leader) must accept removing BASE: {removed:?}"
        );
        // Let the removal commit and replicate down to BASE (a departing
        // peer keeps receiving the removal entry until it acks past it).
        for _ in 0..50 {
            task_env.sleep(Duration::from_millis(100)).await;
            if !base_h1.config().contains(&nid(BASE)) {
                break;
            }
        }
        assert!(
            !base_h1.config().contains(&nid(BASE)),
            "BASE's own durable Raft config never excluded it after removal"
        );

        // Now drive the reconciler's own release-confirm dampener: tablet 1
        // still exists in `Metadata`, but BASE is no longer in its replica
        // set — the tick view the reconciler must react to.
        let v4 = view([
            tablet(1, b"", None, vec![OTHER]),
            tablet(2, b"", None, vec![BASE]),
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

    // The released tablet's engine was destroyed whole — the registry hands
    // back a FRESH, empty engine for it now (ADR 0050 rung 1: release =
    // delete the tablet's own files, no bounded erase needed)...
    let t1_engine = engines.engine(TabletId(1));
    for i in 0..5u64 {
        for prefix in ["a", "z"] {
            let key = physical(format!("{prefix}{i:02}").as_bytes());
            assert_eq!(
                block_on(t1_engine.get(&key)).expect("engine read ok"),
                None,
                "released tablet 1's key {prefix}{i:02} must be gone with its engine"
            );
        }
    }
    // ...and the co-hosted sibling's OWN engine — same table, same logical
    // keys — is completely untouched: sibling-sparing is structural now.
    // The stored bytes carry the ADR 0018 §2/PR3 committed-value envelope
    // (a leading `0` tag byte, `animus_cp_data`'s apply path wraps every
    // committed value) — this reads the engine directly (not through
    // `local_get`, which unwraps it), so the expected bytes below do too.
    let t2_engine = engines.engine(TabletId(2));
    for i in 0..5u64 {
        let key = physical(format!("z{i:02}").as_bytes());
        let mut expected = vec![0u8];
        expected.extend_from_slice(format!("hi{i}").as_bytes());
        assert_eq!(
            block_on(t2_engine.get(&key))
                .expect("engine read ok")
                .map(|vv| vv.value),
            Some(expected),
            "co-hosted sibling tablet 2's key z{i:02} must survive tablet 1's release"
        );
    }
}

// ---------------------------------------------------------------------------
// Fault injection: `EngineFactory::open` failures.
// ---------------------------------------------------------------------------

/// An [`EngineFactory`] wrapper that fails `open` for a chosen tablet a fixed
/// number of times before delegating to a real [`MemoryTabletEngines`] —
/// standing in for a real `EngineFactory::open` I/O failure (`animusd`'s
/// `LsmTabletFactory::open` does real disk I/O and can genuinely fail), which
/// `MemoryTabletEngines` alone (always `Ok`) can never exercise. `probe`/
/// `destroy` always delegate straight through — only `open` is faulty.
#[derive(Clone, Default)]
struct FaultyTabletEngines {
    inner: MemoryTabletEngines,
    fail_remaining: Arc<Mutex<BTreeMap<u64, u32>>>,
}

impl FaultyTabletEngines {
    fn new() -> Self {
        Self::default()
    }

    /// `tablet`'s next `n` `open` calls fail before the (n+1)th succeeds.
    fn fail_open_n_times(&self, tablet: TabletId, n: u32) {
        self.fail_remaining
            .lock()
            .expect("fail_remaining poisoned")
            .insert(tablet.0, n);
    }
}

#[async_trait::async_trait]
impl EngineFactory<MemoryEngine> for FaultyTabletEngines {
    async fn open(&self, tablet: TabletId) -> Result<MemoryEngine, String> {
        // The guard must not survive into the `await` below — an
        // `EngineFactory` future has to stay `Send`, and a `MutexGuard`
        // isn't. A plain `drop(remaining)` is not enough: the generator
        // still captures the binding's storage across the suspend point,
        // so scope the whole lock instead.
        {
            let mut remaining = self.fail_remaining.lock().expect("fail_remaining poisoned");
            if let Some(n) = remaining.get_mut(&tablet.0)
                && *n > 0
            {
                *n -= 1;
                return Err(format!("injected open failure for tablet {}", tablet.0));
            }
        }
        self.inner.open(tablet).await
    }

    async fn probe(&self, tablet: TabletId) -> bool {
        self.inner.probe(tablet).await
    }

    async fn destroy(&self, tablet: TabletId) {
        self.inner.destroy(tablet).await;
    }
}

/// ADR 0031 PR4 fault-injection regression: a transient `EngineFactory::open`
/// failure must not strand a phantom claim in `LocalState::hosted` forever.
/// Before the fix, `plan`'s phase-1 gate (`!next.hosted.contains(&tablet)`)
/// would never re-emit `Host` for a tablet whose claim `host()` inserted
/// optimistically but never actually backed with a live handle — a silent,
/// permanent RF degradation with no operator signal. `host()` now calls
/// `LocalState::release_unconfirmed_host` on a factory failure, so `plan`
/// genuinely retries every tick, and the tablet recovers the moment the
/// factory starts succeeding again.
#[test]
fn reconciler_recovers_a_tablet_after_a_transient_engine_open_failure() {
    let seed = 0x0FA0_77EC;
    let mut sim = Simulator::new(seed);
    let base_env = sim.env(nid(BASE));

    let engines = FaultyTabletEngines::new();
    engines.fail_open_n_times(TabletId(1), 2);
    let reconciler_engines = engines.clone();

    let done = Arc::new(Mutex::new(false));
    let hosted_after_first_failure = Arc::new(Mutex::new(None));
    let hosted_after_second_failure = Arc::new(Mutex::new(None));
    let claim_after_second_failure = Arc::new(Mutex::new(None));
    let hosted_after_recovery = Arc::new(Mutex::new(None));
    let done2 = Arc::clone(&done);
    let hf1 = Arc::clone(&hosted_after_first_failure);
    let hf2 = Arc::clone(&hosted_after_second_failure);
    let cf2 = Arc::clone(&claim_after_second_failure);
    let hr = Arc::clone(&hosted_after_recovery);
    let task_env = base_env.clone();

    base_env.clone().spawn_task(async move {
        let mut reconciler: Reconciler<SimEnv, MemoryEngine> = Reconciler::new(
            task_env.clone(),
            reconciler_engines,
            nid(BASE),
            |_t, _n| {},
            |_t| {},
        );

        // A fresh, single-replica tablet placed on this node from tick one.
        let v = view([tablet(1, b"", None, vec![BASE])]);

        // Tick 1: the factory's first injected failure — Host must be
        // skipped, with no live handle established.
        reconciler.tick(&v).await;
        *hf1.lock().unwrap() = Some(reconciler.hosted_node(TabletId(1)).is_some());

        // Tick 2: the factory's second (last) injected failure — same
        // outcome, and critically the claim must NOT be stranded: `plan`
        // must still consider this tablet unhosted, or it would never be
        // retried again.
        reconciler.tick(&v).await;
        *hf2.lock().unwrap() = Some(reconciler.hosted_node(TabletId(1)).is_some());
        *cf2.lock().unwrap() = Some(reconciler.local_state().hosted.contains(&TabletId(1)));

        // Tick 3: the factory now succeeds — proving the claim really was
        // released (not just stuck), `plan` re-emits `Host` and it lands.
        reconciler.tick(&v).await;
        task_env.sleep(Duration::from_secs(2)).await; // elect (single voter)
        *hr.lock().unwrap() = Some(reconciler.hosted_node(TabletId(1)).is_some());

        *done2.lock().unwrap() = true;
    });

    poll_until(
        &mut sim,
        Duration::from_secs(30),
        Duration::from_secs(1),
        "the fault-recovery scenario task never completed",
        || *done.lock().unwrap(),
    );

    assert_eq!(
        *hosted_after_first_failure.lock().unwrap(),
        Some(false),
        "a factory failure must not host the tablet"
    );
    assert_eq!(
        *hosted_after_second_failure.lock().unwrap(),
        Some(false),
        "a second consecutive factory failure must still not host the tablet"
    );
    assert_eq!(
        *claim_after_second_failure.lock().unwrap(),
        Some(false),
        "the claim must be released after a failed Host, not stranded in LocalState::hosted"
    );
    assert_eq!(
        *hosted_after_recovery.lock().unwrap(),
        Some(true),
        "the reconciler must recover the tablet once the factory starts succeeding"
    );

    // The tablet's engine genuinely holds durable state now (the third,
    // successful open) — a real recovery, not just a live handle with no
    // backing store.
    assert!(block_on(engines.probe(TabletId(1))));
}
