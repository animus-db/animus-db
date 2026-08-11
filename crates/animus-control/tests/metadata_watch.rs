//! `RaftNode::metadata_watch()` (ADR 0031 §trigger): an executor-agnostic
//! "applied index advanced" notification, the primitive the future per-node
//! `TabletHostReconciler` (PR4) will use to react to a `Metadata` change
//! instead of polling on a fixed timer.
//!
//! `MetadataWatch::changed` is driven as a spawned task (it parks on a
//! `futures::task::AtomicWaker`, which only resolves while `Simulator::run_for`
//! is advancing virtual time) — the documented "never `block_on` something that
//! polls under `SimEnv`" gotcha; see `animus-cp-data/tests/read_index.rs`'s
//! `lin_read` for the same pattern.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::{MetaCommand, NodeStatus, RaftNode};
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(node),
        labels: std::collections::BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

fn cluster(seed: u64) -> (Simulator, Vec<RaftNode<SimEnv>>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            RaftNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

fn unique_leader(nodes: &[RaftNode<SimEnv>], seed: u64) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    ls[0]
}

/// Spawn a task parked on `watch.changed(last_seen)`, driving `sim` for
/// `budget` of virtual time, and return whatever it resolved to (`None` if it
/// never did within the budget).
fn await_changed(
    sim: &mut Simulator,
    node: &RaftNode<SimEnv>,
    watch: &animus_control::MetadataWatch,
    last_seen: u64,
    budget: Duration,
) -> Option<u64> {
    let slot: Arc<Mutex<Option<u64>>> = Arc::new(Mutex::new(None));
    let w = watch.clone();
    let s = Arc::clone(&slot);
    node.env().clone().spawn_task(async move {
        let new_index = w.changed(last_seen).await;
        *s.lock().unwrap() = Some(new_index);
    });
    sim.run_for(budget);
    *slot.lock().unwrap()
}

/// The core contract: a watcher parked on `changed()` wakes once a proposal
/// actually commits and applies (becomes visible via `metadata()`), yielding
/// the new applied index.
#[test]
fn watcher_wakes_when_a_proposal_applies() {
    let seed = 0xADF0_0311;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, seed);

    let watch = nodes[leader].metadata_watch();
    let baseline = watch.latest();

    nodes[leader].propose(upsert(41));
    let woke = await_changed(
        &mut sim,
        &nodes[leader],
        &watch,
        baseline,
        Duration::from_secs(2),
    );

    let new_index = woke
        .unwrap_or_else(|| panic!("watcher never woke after a committed proposal (seed={seed})"));
    assert!(
        new_index > baseline,
        "the woken index ({new_index}) must exceed the pre-proposal baseline ({baseline})"
    );
    assert!(
        nodes[leader].metadata_watch().latest() >= new_index,
        "the watch's own latest() must have reached at least what the waiter observed"
    );
    assert!(
        nodes[leader].metadata().members.contains_key(&nid(41)),
        "the applied index the watch reported must actually be visible via metadata()"
    );
}

/// No committed change ⇒ no spurious wake. A stable leader's routine
/// heartbeat/tick traffic must not by itself advance the applied index (no
/// members were ever upserted, so the failure detector and reconciler have
/// nothing to propose), so a watcher parked at the post-election baseline
/// stays parked across further virtual time.
#[test]
fn watcher_does_not_wake_without_a_committed_change() {
    let seed = 0xADF0_0312;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, seed);

    let watch = nodes[leader].metadata_watch();
    let baseline = watch.latest();

    // No propose here — just let steady-state heartbeats/ticks run.
    let woke = await_changed(
        &mut sim,
        &nodes[leader],
        &watch,
        baseline,
        Duration::from_secs(2),
    );

    assert!(
        woke.is_none(),
        "watcher must stay parked with no committed change (seed={seed}), got {woke:?}"
    );
    assert_eq!(
        watch.latest(),
        baseline,
        "the applied index must not have moved either"
    );
}

/// Wake-before-park race: the proposal is committed and fully applied
/// *before* the watcher's future is ever created or polled. Because
/// `changed()` re-checks the watermark fresh on every poll (never a consumed
/// one-shot flag, unlike `animus-cp-data`'s `ProposeSignal`), the very first
/// poll must resolve immediately — there is no window where a wake that
/// raced ahead of registration gets lost.
#[test]
fn changed_resolves_immediately_when_the_advance_already_happened() {
    let seed = 0xADF0_0313;
    let (mut sim, nodes) = cluster(seed);
    sim.run_for(Duration::from_secs(2));
    let leader = unique_leader(&nodes, seed);

    // Commit and fully apply a proposal *before* touching `changed()` at all.
    nodes[leader].propose(upsert(42));
    sim.run_for(Duration::from_secs(2));
    assert!(
        nodes[leader].metadata().members.contains_key(&nid(42)),
        "setup: the proposal must already be applied before the watch is ever polled"
    );

    let watch = nodes[leader].metadata_watch();
    let already_advanced = watch.latest();
    assert!(already_advanced > 0, "setup: the watch must have advanced");

    // `changed(0)` — asking about a baseline from before the whole run — must
    // resolve on the very first poll, with only enough virtual time advanced
    // for the spawned task to actually be polled once (no new proposal needed).
    let woke = await_changed(
        &mut sim,
        &nodes[leader],
        &watch,
        0,
        Duration::from_millis(50),
    );
    assert_eq!(
        woke,
        Some(already_advanced),
        "a watcher created after the advance must still resolve on its first poll (seed={seed})"
    );
}
