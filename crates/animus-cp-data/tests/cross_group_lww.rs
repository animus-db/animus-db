//! Cross-group LWW version-floor hazard (flagged in a PR #90 review comment):
//! since every tablet a node hosts shares one physical `StorageEngine` (ADR
//! 0026/0028) and the Raft log index of a tablet's *own* group is the MVCC
//! version stamped on writes ("the Raft log index is the MVCC version"), a
//! **fresh** group's index restarts low/independent of any other group's.
//! A split's new sibling is a brand-new group serving keys the *source*
//! group already wrote at whatever (possibly much higher) index it had
//! reached; a merge survivor's group keeps running, but starts serving keys
//! the *absorbed* sibling's group already wrote under its own, unrelated
//! index sequence. Either way, a subsequent write through the new/widened
//! group can carry a version no higher than what's already stored, and
//! per-key LWW (`StorageEngine::merge`) silently drops it — surfacing as a
//! write-confirm timeout (the confirm loop polls for exact value equality),
//! never corruption, but the write never lands.
//!
//! **CONFIRMED real** — this file proves it directly at the `RaftKvNode`
//! level (no control-plane/`animusd` machinery needed to reproduce the
//! collision itself): `split_sibling_without_a_version_floor_...` and
//! `merge_survivor_without_a_version_floor_bump_...` show the un-seeded
//! shape silently losing an overwrite; the `..._accepts_the_overwrite`
//! siblings prove the fix (`animus_tablet::Tablet::version_floor` +
//! `RaftKvNode::start_hosted_with_floor`/`bump_version_floor`) closes it,
//! using the *exact* floor values `MetaCommand::SplitTablet`/`MergeTablets`
//! apply would compute (see `animus-control`'s `meta.rs`).
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers).

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{RaftKvNode, StorageScope};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::KeyRange;
use futures::executor::block_on;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// Every group in this file hosts on the same node id, distinguished only by
/// stream (ADR 0026 Stage B) — the exact shape a real node uses to host
/// several tablets over one shared engine.
const NODE: u64 = 0;
/// The split/merge boundary: keys `< BOUNDARY` are the "kept"/`left` range,
/// `>= BOUNDARY` are the "handed off"/`right` range.
const BOUNDARY: &[u8] = b"m";
/// A key in the handed-off/absorbed (upper) range, so it exercises the exact
/// crossover this file is about.
const KEY: &[u8] = b"m0";

fn scope(range: KeyRange) -> StorageScope {
    StorageScope::new(b"T:".to_vec(), range)
}

fn put(node: &KvNode, key: &[u8], value: &[u8]) {
    match node.put(key.to_vec(), value.to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("single-voter leader rejected a put: {other:?}"),
    }
}

/// A single-voter group (this file only needs one replica to reproduce a
/// purely local per-key-LWW collision) elects itself immediately, but still
/// needs the driver loop to actually run at least once.
const ELECT: Duration = Duration::from_secs(1);

/// Advance `node`'s own local Raft log index well past any small floor by
/// writing `n` filler keys (outside `KEY`'s own history), so a subsequent
/// write to `KEY` on this group lands at a comfortably high index.
fn bump_index(sim: &mut Simulator, node: &KvNode, n: u32) {
    for i in 0..n {
        put(node, format!("filler{i:04}").as_bytes(), b"x");
    }
    sim.run_for(ELECT);
}

// ============================================================================
// Split shape: a source group narrows away from KEY's range; a brand-new
// sibling group, sharing the same physical engine, is scoped to take it over.
// ============================================================================

/// Pre-fix / unseeded shape: the sibling starts via `start_hosted` (version
/// floor `0`, i.e. raw log index) exactly like the source did — reproducing
/// the hazard: the source's high index for `KEY` permanently outranks
/// anything the fresh sibling's own low index can produce.
#[test]
fn split_sibling_without_a_version_floor_silently_drops_an_overwrite() {
    let seed = 0xC0_5E17_u64;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();

    let source: KvNode = RaftKvNode::start_hosted(
        sim.env(NODE),
        vec![NODE],
        engine.clone(),
        scope(KeyRange::whole()),
        1,
    );
    sim.run_for(ELECT);

    // Push the source's own index up, then write KEY (in what will become
    // the sibling's post-split range) through the source while it still
    // owns the whole keyspace.
    bump_index(&mut sim, &source, 30);
    put(&source, KEY, b"source-value");
    sim.run_for(ELECT);
    assert_eq!(
        block_on(source.local_get(KEY)),
        Some(b"source-value".to_vec()),
        "sanity: the source's own write must land before any split (seed={seed})"
    );

    // Simulate the split: the source narrows to the kept (lower) range —
    // KEY now belongs exclusively to the sibling being minted below.
    source.narrow_scope(KeyRange::new(Vec::new(), Some(BOUNDARY.to_vec())));

    // A fresh sibling, same node id + shared engine, its own stream, scoped
    // to the handed-off (upper) range — started the UN-SEEDED way (no
    // version floor), reproducing the hazard.
    let sibling: KvNode = RaftKvNode::start_hosted(
        sim.env(NODE),
        vec![NODE],
        engine.clone(),
        scope(KeyRange::new(BOUNDARY.to_vec(), None)),
        2,
    );
    sim.run_for(ELECT);

    put(&sibling, KEY, b"sibling-value");
    sim.run_for(ELECT);

    // The hazard: the sibling's write is silently dropped by per-key LWW —
    // the physical engine still holds the source's stale value.
    assert_eq!(
        block_on(sibling.local_get(KEY)),
        Some(b"source-value".to_vec()),
        "expected the confirmed hazard: an un-seeded fresh sibling's overwrite \
         is silently dropped by per-key LWW (seed={seed})"
    );
}

/// The fix: the sibling is seeded via `start_hosted_with_floor` with exactly
/// the floor `MetaCommand::SplitTablet`'s apply computes
/// (`source.version_floor + 1` — the source here has never itself been
/// split/merged, so its own floor is `0` and the sibling's is `1`). The
/// overwrite now lands.
#[test]
fn split_sibling_seeded_with_a_version_floor_accepts_the_overwrite() {
    let seed = 0xC0_5E18_u64;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();

    let source: KvNode = RaftKvNode::start_hosted(
        sim.env(NODE),
        vec![NODE],
        engine.clone(),
        scope(KeyRange::whole()),
        1,
    );
    sim.run_for(ELECT);
    assert_eq!(source.version_floor(), 0, "an un-split tablet's floor is 0");

    bump_index(&mut sim, &source, 30);
    put(&source, KEY, b"source-value");
    sim.run_for(ELECT);

    source.narrow_scope(KeyRange::new(Vec::new(), Some(BOUNDARY.to_vec())));

    // Seeded with `source.version_floor() + 1` — exactly the
    // `MetaCommand::SplitTablet` apply-time formula.
    let sibling: KvNode = RaftKvNode::start_hosted_with_floor(
        sim.env(NODE),
        vec![NODE],
        engine.clone(),
        scope(KeyRange::new(BOUNDARY.to_vec(), None)),
        2,
        source.version_floor() + 1,
    );
    sim.run_for(ELECT);

    put(&sibling, KEY, b"sibling-value");
    sim.run_for(ELECT);

    assert_eq!(
        block_on(sibling.local_get(KEY)),
        Some(b"sibling-value".to_vec()),
        "the seeded sibling's overwrite must land — its floor strictly \
         exceeds anything the source could have stamped (seed={seed})"
    );
}

// ============================================================================
// Merge shape: a `right` group accumulates a high index for KEY, is torn
// down (Absorb), and the (already-running, low-index) `left` survivor widens
// its scope to serve KEY on the same shared engine.
// ============================================================================

/// Pre-fix / un-bumped shape: `left` widens its scope but its own version
/// floor is never bumped, reproducing the hazard from the merge side: an
/// already-running low-index survivor can't out-version a hotter absorbed
/// sibling's history for the same key.
#[test]
fn merge_survivor_without_a_version_floor_bump_silently_drops_an_overwrite() {
    let seed = 0xC0_5E19_u64;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();

    // `right`: owns the upper range, accumulates a high index, writes KEY.
    let right: KvNode = RaftKvNode::start_hosted(
        sim.env(NODE),
        vec![NODE],
        engine.clone(),
        scope(KeyRange::new(BOUNDARY.to_vec(), None)),
        2,
    );
    sim.run_for(ELECT);
    bump_index(&mut sim, &right, 30);
    put(&right, KEY, b"right-value");
    sim.run_for(ELECT);
    assert_eq!(
        block_on(right.local_get(KEY)),
        Some(b"right-value".to_vec())
    );

    // `left`: owns the lower range, has done far fewer writes (a low index).
    let left: KvNode = RaftKvNode::start_hosted(
        sim.env(NODE),
        vec![NODE],
        engine.clone(),
        scope(KeyRange::new(Vec::new(), Some(BOUNDARY.to_vec()))),
        1,
    );
    sim.run_for(ELECT);
    put(&left, b"a0", b"unrelated");
    sim.run_for(ELECT);

    // Simulate the merge's Absorb teardown: `right`'s group stops (its
    // physical data — including KEY at its high version — stays on the
    // shared engine, now to be served by `left`).
    right.shutdown();
    sim.run_for(ELECT);

    // `left` widens its scope to cover the whole (now-merged) range —
    // WITHOUT bumping its version floor, reproducing the hazard.
    left.widen_scope(KeyRange::whole());

    put(&left, KEY, b"left-value");
    sim.run_for(ELECT);

    assert_eq!(
        block_on(left.local_get(KEY)),
        Some(b"right-value".to_vec()),
        "expected the confirmed hazard: an un-bumped merge survivor's \
         overwrite is silently dropped by per-key LWW (seed={seed})"
    );
}

/// The fix: `left` also calls `bump_version_floor` alongside `widen_scope`,
/// using exactly the `MetaCommand::MergeTablets` apply-time formula
/// (`max(left.version_floor, right.version_floor) + 1` — both are `0` here,
/// neither tablet having been split/merged before, so `left`'s new floor is
/// `1`). The overwrite now lands.
#[test]
fn merge_survivor_with_a_version_floor_bump_accepts_the_overwrite() {
    let seed = 0xC0_5E1A_u64;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();

    let right: KvNode = RaftKvNode::start_hosted(
        sim.env(NODE),
        vec![NODE],
        engine.clone(),
        scope(KeyRange::new(BOUNDARY.to_vec(), None)),
        2,
    );
    sim.run_for(ELECT);
    bump_index(&mut sim, &right, 30);
    put(&right, KEY, b"right-value");
    sim.run_for(ELECT);
    let right_floor = right.version_floor();

    let left: KvNode = RaftKvNode::start_hosted(
        sim.env(NODE),
        vec![NODE],
        engine.clone(),
        scope(KeyRange::new(Vec::new(), Some(BOUNDARY.to_vec()))),
        1,
    );
    sim.run_for(ELECT);
    put(&left, b"a0", b"unrelated");
    sim.run_for(ELECT);
    let left_floor = left.version_floor();

    right.shutdown();
    sim.run_for(ELECT);

    left.widen_scope(KeyRange::whole());
    // Exactly the `MetaCommand::MergeTablets` apply-time formula.
    left.bump_version_floor(left_floor.max(right_floor) + 1);

    put(&left, KEY, b"left-value");
    sim.run_for(ELECT);

    assert_eq!(
        block_on(left.local_get(KEY)),
        Some(b"left-value".to_vec()),
        "the bumped survivor's overwrite must land — its new floor strictly \
         exceeds anything either side could have stamped (seed={seed})"
    );
}
