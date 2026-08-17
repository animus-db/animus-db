//! ADR 0018 §2 amendment (PR2): cross-group MVCC ordering under the
//! **range-seal** design.
//!
//! This file used to prove the `version_floor` cross-group-LWW fix directly
//! (a structural version-space separation: a fresh/widened group's stamped
//! version could never numerically collide with a different group's, no
//! matter the timing). PR2 retires that fix — the engine's MVCC version is
//! now the packed HLC commit timestamp (`hlc::pack`), and cross-group
//! ordering instead comes from **witnessing** (`Hlc::witness`, at group
//! start off the shared engine's `latest_version()`, among other points —
//! see `hlc.rs`) plus, for the residual race witnessing alone cannot close
//! (an in-flight write from a source leader that hasn't yet observed a
//! split), the **range seal** (`KvCommand::Seal`, `seal.rs`).
//!
//! Kept the original split shape (its merge dual was deleted along with
//! tablet merge — split-only tablets) — updated to the new mechanism — plus
//! the scenario the seal specifically exists for: an in-flight write racing
//! the handoff with **zero** intervening sim time (mirroring
//! `reconciler_corpus.rs`'s zero-tick drain-regression technique), and the
//! "wide fence, un-ticked leader" case (a write proposed well after the seal
//! has landed must still be rejected). A final test proves the design does
//! not secretly depend on synchronized clocks: the successor's node can read
//! *behind* the source's the entire time and still win, because it witnesses
//! via the shared engine, never wall time.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers).

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::host::{MemoryTabletEngines, MetadataView, Reconciler};
use animus_cp_data::{RaftKvNode, StorageScope};
use animus_env::{Clock, EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, Tablet, TabletId};
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// Every raw-`RaftKvNode`-level test in this file (everything except the
/// clock-skew one, which deliberately uses two distinct node ids) hosts on
/// this one node id, distinguished only by stream (ADR 0026 Stage B) — the
/// exact shape a real node uses to host several tablets over one shared
/// engine.
const NODE: u64 = 0;
const TABLE: &str = "t";
/// The split boundary: keys `< BOUNDARY` are the kept (source) range,
/// `>= BOUNDARY` are the handed-off (successor) range.
const BOUNDARY: &[u8] = b"m";
/// A key in the handed-off (upper) range, so it exercises the exact
/// crossover this file is about.
const KEY: &[u8] = b"m0";

/// A single-voter group elects itself immediately, but still needs the
/// driver loop to actually run at least once.
const ELECT: Duration = Duration::from_secs(1);

fn scope(range: KeyRange) -> StorageScope {
    StorageScope::new(b"T:".to_vec(), range)
}

fn put(node: &KvNode, key: &[u8], value: &[u8]) -> ProposeResult {
    let result = node.put(key.to_vec(), value.to_vec());
    assert!(
        matches!(result, ProposeResult::Accepted { .. }),
        "single-voter leader rejected a put: {result:?}"
    );
    result
}

// ============================================================================
// (a) Split shape: source writes pre-split, narrows + seals the handed-off
// range, and only THEN does a successor host and write — its value must win.
// ============================================================================

#[test]
fn split_successor_wins_after_seal_lands() {
    let seed = 0xC0_5E17_u64;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();

    let source: KvNode = RaftKvNode::start_hosted(
        sim.env(nid(NODE)),
        vec![nid(NODE)],
        engine.clone(),
        scope(KeyRange::whole()),
        1,
    );
    sim.run_for(ELECT);

    put(&source, KEY, b"source-value");
    sim.run_for(ELECT);
    assert_eq!(
        block_on(source.local_get(KEY)),
        Some(b"source-value".to_vec()),
        "sanity: the source's own write must land before any split (seed={seed})"
    );

    // The split: the source narrows to the kept (lower) range, then seals
    // the handed-off (upper) range through its own Raft log — mirroring
    // `host::Reconciler`'s `NarrowScope` handling.
    source.narrow_scope(KeyRange::new(Vec::new(), Some(BOUNDARY.to_vec())));
    let handed_off = KeyRange::new(BOUNDARY.to_vec(), None);
    let sealed = source.propose_seal(handed_off);
    assert!(matches!(sealed, ProposeResult::Accepted { .. }));
    sim.run_for(ELECT); // let the seal apply (durably write its marker)

    // A fresh sibling, same node id + shared engine, its own stream, scoped
    // to the handed-off range — hosted only AFTER the seal has landed
    // (exactly what `host::Reconciler`'s `parent_seal_observed` gate
    // requires in production).
    let sibling: KvNode = RaftKvNode::start_hosted(
        sim.env(nid(NODE)),
        vec![nid(NODE)],
        engine.clone(),
        scope(KeyRange::new(BOUNDARY.to_vec(), None)),
        2,
    );
    sim.run_for(ELECT);

    put(&sibling, KEY, b"sibling-value");
    sim.run_for(ELECT);

    assert_eq!(
        block_on(sibling.local_get(KEY)),
        Some(b"sibling-value".to_vec()),
        "the sibling's overwrite must land (seed={seed})"
    );
    // Every replica of the physical key (there's only one engine here, but
    // reading through BOTH groups' own scopes proves neither has a stale
    // cached view) sees the successor's value.
    assert_eq!(
        block_on(source.local_get(KEY)),
        Some(b"sibling-value".to_vec()),
        "the source's own (now-irrelevant) read of the same physical key must \
         also see the successor's value (seed={seed})"
    );
}

// ============================================================================
// The residual "wide fence, un-ticked leader" case: a write proposed through
// the SAME group, for the same range, well after its own seal has landed,
// must be rejected outright — never override what the seal protects.
// ============================================================================

#[test]
fn a_write_proposed_after_its_own_range_is_sealed_is_rejected_on_every_replica() {
    let seed = 0xC0_5E1D_u64;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();

    let source: KvNode = RaftKvNode::start_hosted(
        sim.env(nid(NODE)),
        vec![nid(NODE)],
        engine.clone(),
        scope(KeyRange::whole()),
        1,
    );
    sim.run_for(ELECT);

    put(&source, KEY, b"initial");
    sim.run_for(ELECT);

    source.narrow_scope(KeyRange::new(Vec::new(), Some(BOUNDARY.to_vec())));
    let sealed = source.propose_seal(KeyRange::new(BOUNDARY.to_vec(), None));
    assert!(matches!(sealed, ProposeResult::Accepted { .. }));
    sim.run_for(ELECT); // let the seal apply

    // `RaftKvNode::put` stamps `fence = KeyRange::whole()` (unconstrained) —
    // it does not itself pre-check the group's own live scope (that pre-check
    // lives one layer up, in `animusd`'s callers). This models exactly the
    // "wide fence, un-ticked leader" residual: a caller that still thinks
    // this group owns `KEY` and proposes anyway, well after the seal.
    let late = source.put(KEY.to_vec(), b"stale-after-seal".to_vec());
    assert!(matches!(late, ProposeResult::Accepted { .. }));
    sim.run_for(ELECT);

    assert_eq!(
        block_on(source.local_get(KEY)),
        Some(b"initial".to_vec()),
        "a write ordered after the seal, for a key inside the sealed range, \
         must be rejected at apply — the pre-seal value must survive \
         untouched (seed={seed})"
    );
}

// ============================================================================
// (c) The in-flight split race — the reason seals exist. An entry proposed
// through the source group *before* the split is even known about must never
// let the source's stale value become the final one, whether it ends up
// applied-then-overridden (ts order) or rejected outright by the seal.
// ============================================================================

fn prefix_for(_table: &str) -> Vec<u8> {
    b"T:".to_vec()
}

fn tablet(id: u64, start: &[u8], end: Option<&[u8]>) -> Tablet {
    Tablet::new_for_table(
        TabletId(id),
        TABLE,
        KeyRange::new(start.to_vec(), end.map(<[u8]>::to_vec)),
        vec![nid(NODE)],
    )
}

fn view(tablets: impl IntoIterator<Item = Tablet>) -> MetadataView {
    MetadataView {
        tablets: tablets.into_iter().map(|t| (t.id, t)).collect(),
        ..Default::default()
    }
}

fn view_with_split_parent(
    tablets: impl IntoIterator<Item = Tablet>,
    split_parent: impl IntoIterator<Item = (u64, u64)>,
) -> MetadataView {
    MetadataView {
        split_parent: split_parent
            .into_iter()
            .map(|(child, parent)| (TabletId(child), TabletId(parent)))
            .collect(),
        ..view(tablets)
    }
}

/// Drives an async scenario body to completion under `Simulator::run_for`,
/// spawned as a task — mirrors `reconciler_corpus.rs`'s `run`/`poll_until`
/// harness (needed because `Reconciler::tick` can internally `env.sleep()`,
/// which only resolves while sim time is advancing; never bare `block_on`
/// a `tick()`, per the documented `SimEnv` gotcha).
fn run<F, Fut>(seed: u64, body: F)
where
    F: FnOnce(Simulator) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut sim = Simulator::new(seed);
    let driver_env = sim.env(nid(900)); // clear of NODE (0)
    let done = Arc::new(Mutex::new(false));
    let done2 = Arc::clone(&done);
    let sim_in_task = sim.clone();
    driver_env.spawn_task(async move {
        body(sim_in_task).await;
        *done2.lock().unwrap() = true;
    });
    let budget = Duration::from_secs(60);
    let step = Duration::from_secs(1);
    let mut waited = Duration::ZERO;
    while waited < budget {
        sim.run_for(step);
        waited += step;
        if *done.lock().unwrap() {
            return;
        }
    }
    panic!("in-flight-race scenario never completed (seed={seed})");
}

#[test]
#[ignore = "PARKED (ADR 0050 Train B): the zero-copy split's narrow/seal handoff race \
            this exercises is disabled during the storage pivot — under per-tablet \
            engines a split child no longer shares the source's physical keys at all; \
            deleted with its machinery in the deletion rung"]
fn in_flight_split_race_successor_always_wins() {
    let seed = 0xC0_5E1B_u64;
    run(seed, move |sim| async move {
        let env = sim.env(nid(NODE));
        let mut reconciler: Reconciler<SimEnv, MemoryEngine> = Reconciler::new(
            env.clone(),
            MemoryTabletEngines::new(),
            nid(NODE),
            prefix_for,
            |_, _| {},
            |_| {},
        );

        // Host the source as a fresh whole-keyspace tablet.
        let v1 = view([tablet(1, b"", None)]);
        reconciler.tick(&v1).await;
        env.sleep(ELECT).await;

        let source = reconciler.hosted_node(TabletId(1)).unwrap().clone();
        assert!(source.is_leader(), "the lone voter must self-elect");
        put(&source, KEY, b"initial");
        env.sleep(ELECT).await;

        // The race: propose a write to the about-to-be-handed-off range,
        // then — with ZERO additional sim time — tick the reconciler
        // through the split view (source narrows + seals; the child is
        // named in `split_parent`). The apply task has not yet had a chance
        // to run when this tick executes, so the write above is
        // committed-but-not-yet-applied.
        let late = source.put(KEY.to_vec(), b"source-late".to_vec());
        assert!(matches!(late, ProposeResult::Accepted { .. }));

        let v2 = view_with_split_parent(
            [tablet(1, b"", Some(BOUNDARY)), tablet(2, BOUNDARY, None)],
            [(2, 1)],
        );
        reconciler.tick(&v2).await;
        assert!(
            reconciler.hosted_node(TabletId(2)).is_none(),
            "the split child must NOT host before its parent's seal marker \
             is observable locally (seed={seed})"
        );

        // Let the apply task catch up — both the in-flight write and the
        // seal itself drain to the engine — then keep ticking until the
        // gate opens.
        let mut opened = false;
        for _ in 0..30 {
            env.sleep(Duration::from_millis(200)).await;
            reconciler.tick(&v2).await;
            if reconciler.hosted_node(TabletId(2)).is_some() {
                opened = true;
                break;
            }
        }
        assert!(
            opened,
            "the split child must eventually host once the seal lands (seed={seed})"
        );
        let successor = reconciler.hosted_node(TabletId(2)).unwrap().clone();
        env.sleep(ELECT).await;

        put(&successor, KEY, b"successor-value");
        env.sleep(ELECT).await;

        assert_eq!(
            successor.local_get(KEY).await,
            Some(b"successor-value".to_vec()),
        );
        assert_eq!(
            source.local_get(KEY).await,
            Some(b"successor-value".to_vec()),
            "every replica (the shared physical key) must see the successor's \
             value — the source's in-flight write must never be the final \
             value, whether it ended up applied-then-overridden or \
             seal-rejected outright (seed={seed})"
        );

        // The residual "wide fence, un-ticked leader" case: a write proposed
        // through the SOURCE group, well after the seal is known to have
        // landed, must be rejected outright.
        let stale = source.put(KEY.to_vec(), b"stale-after-seal".to_vec());
        assert!(matches!(stale, ProposeResult::Accepted { .. }));
        env.sleep(ELECT).await;
        assert_eq!(
            source.local_get(KEY).await,
            Some(b"successor-value".to_vec()),
            "a source write proposed after the seal is known to have landed \
             must be rejected — never override the successor's value \
             (seed={seed})"
        );
    });
}

// ============================================================================
// Clock-skew composition: the successor's node can read BEHIND the source's
// the entire time and still win — witnessing comes from the shared engine's
// own `latest_version()`, never wall-clock synchronization between nodes.
// ============================================================================

#[test]
fn successor_wins_despite_the_nodes_clocks_being_out_of_sync() {
    let seed = 0xC0_5E1E_u64;
    let mut sim = Simulator::new(seed);
    let node_a = nid(10);
    let node_b = nid(11);
    // A's clock reads 500ms AHEAD of the shared timeline; B is left
    // unskewed, i.e. behind A for the whole scenario.
    sim.set_clock_skew_for(node_a.clone(), 500_000_000);

    let engine = MemoryEngine::new();

    let source: KvNode = RaftKvNode::start_hosted(
        sim.env(node_a.clone()),
        vec![node_a.clone()],
        engine.clone(),
        scope(KeyRange::whole()),
        1,
    );
    sim.run_for(ELECT);
    put(&source, KEY, b"source-value");
    sim.run_for(ELECT);

    source.narrow_scope(KeyRange::new(Vec::new(), Some(BOUNDARY.to_vec())));
    let sealed = source.propose_seal(KeyRange::new(BOUNDARY.to_vec(), None));
    assert!(matches!(sealed, ProposeResult::Accepted { .. }));
    sim.run_for(ELECT);

    // The successor hosts on a DIFFERENT node whose clock reads BEHIND the
    // source's the whole time.
    let sibling: KvNode = RaftKvNode::start_hosted(
        sim.env(node_b.clone()),
        vec![node_b.clone()],
        engine.clone(),
        scope(KeyRange::new(BOUNDARY.to_vec(), None)),
        2,
    );
    sim.run_for(ELECT);
    put(&sibling, KEY, b"sibling-value");
    sim.run_for(ELECT);

    assert_eq!(
        block_on(sibling.local_get(KEY)),
        Some(b"sibling-value".to_vec()),
        "sanity: the sibling's own overwrite lands (seed={seed})"
    );
    assert_eq!(
        block_on(source.local_get(KEY)),
        Some(b"sibling-value".to_vec()),
        "the successor's write must win even though its own node's clock \
         reads behind the source's — witnessing off the shared engine's \
         latest_version() at group start, not wall-clock synchronization, \
         is what orders them (seed={seed})"
    );
}
