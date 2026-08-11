//! `RaftKvNode::narrow_scope` (PR5 of the single-command-split redesign):
//! a group's `StorageScope` range is live-narrowable, not fixed at
//! construction — the shape needed when this tablet is later the *source* of
//! a split: its own range shrinks while its physical data does not move, so
//! `engine_image` (unbounded, self-contained snapshot capture) must stop
//! shipping the already-handed-off portion once the group learns its range
//! has narrowed, even though that data is still physically present in every
//! live replica's own engine.
//!
//! `narrow_then_erase_scope_spares_a_co_hosted_siblings_data` below is the
//! primitive-level proof for the removed-replica release-GC fix
//! (`animusd::cp_gc_tablet`, ADR 0029): a node's `RaftKvNode` for a just-split
//! source tablet can be released (moved off, then GC'd) before its own
//! `cp_join_host_loop` narrow tick has re-narrowed its `StorageScope`, so the
//! scope can still be stale-wide when a teardown calls `erase_scope`. Since
//! ADR 0026/0028 put every tablet a node hosts on **one shared engine**, a
//! stale-wide erase tombstones the split's new sibling's live keys too. The
//! fix narrows to the tablet's *current replicated range* immediately before
//! erasing; this test proves that ordering spares a co-hosted sibling's data
//! without needing to race the real ~250ms narrow-tick timer.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers).

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{RaftKvNode, StorageScope};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::KeyRange;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
/// The boundary a "split" narrows to: keys `< BOUNDARY` are kept, `>=` are
/// treated as already handed off to an (unmodeled here) sibling tablet.
const BOUNDARY: &[u8] = b"m";

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn group(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start_scoped(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
                StorageScope::new(b"T:".to_vec(), KeyRange::whole()),
            )
        })
        .collect();
    (sim, nodes)
}

fn leader(nodes: &[KvNode], live: &[usize], seed: u64) -> usize {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(i, n)| live.contains(i) && n.is_leader())
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one leader among {live:?}, got {ls:?} (seed={seed})"
    );
    ls[0]
}

fn put(nodes: &[KvNode], l: usize, seed: u64, key: &[u8], value: &[u8]) {
    match nodes[l].put(key.to_vec(), value.to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("leader rejected a put: {other:?} (seed={seed})"),
    }
}

/// A replica that crashes before a scope narrows, then catches up via
/// `InstallSnapshot` after the leader has narrowed and compacted, must not
/// receive the already-handed-off portion — even though that data is still
/// physically present in every *other* live replica's own engine (this test
/// deliberately never inspects those, since `local_get` isn't range-gated by
/// design; only future snapshot *captures* are).
#[test]
fn narrowing_excludes_already_handed_off_data_from_future_snapshots() {
    let seed = 0x5C0E;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect
    let l = leader(&nodes, &[0, 1, 2], seed);
    let lagging = (0..3).find(|&i| i != l).expect("a follower exists");

    // Crash the lagging follower immediately — it misses everything below.
    sim.crash(nid(NODES[lagging]));

    // Write into BOTH the soon-to-be-kept ("lo") and soon-to-be-handed-off
    // ("hi") portions while the scope is still whole.
    for i in 0..10u64 {
        put(
            &nodes,
            l,
            seed,
            format!("a{i:02}").as_bytes(), // < "m": kept
            format!("lo{i}").as_bytes(),
        );
        put(
            &nodes,
            l,
            seed,
            format!("z{i:02}").as_bytes(), // >= "m": handed off
            format!("hi{i}").as_bytes(),
        );
    }
    sim.run_for(Duration::from_secs(1));

    // Simulate the split: every live replica narrows to the kept range.
    let kept_range = KeyRange::new(Vec::new(), Some(BOUNDARY.to_vec()));
    for &i in &[l, (0..3).find(|&i| i != l && i != lagging).unwrap()] {
        nodes[i].narrow_scope(kept_range.clone());
    }

    // Push well past the compaction threshold (64) with more "kept" writes so
    // the leader snapshots + truncates the log prefix the lagging follower
    // would otherwise need.
    for i in 10..160u64 {
        put(
            &nodes,
            l,
            seed,
            format!("a{i:03}").as_bytes(),
            format!("lo{i}").as_bytes(),
        );
    }
    sim.run_for(Duration::from_secs(3));

    // Bring the lagging replica back: its log is far behind the leader's
    // compacted (and now range-narrowed) base, so it must catch up via
    // InstallSnapshot.
    sim.restart(nid(NODES[lagging]));
    sim.run_for(Duration::from_secs(6));

    // It has every "kept" write...
    for i in [0u64, 1, 64, 100, 159] {
        let key = if i < 10 {
            format!("a{i:02}")
        } else {
            format!("a{i:03}")
        };
        assert_eq!(
            block_on(nodes[lagging].local_get(key.as_bytes())),
            Some(format!("lo{i}").into_bytes()),
            "lagging replica {lagging} missing kept key {key} after catch-up (seed={seed})"
        );
    }
    // ...but NONE of the handed-off ones: the snapshot it caught up from was
    // captured *after* narrowing, so it was never shipped this data at all.
    for i in 0..10u64 {
        let key = format!("z{i:02}");
        assert_eq!(
            block_on(nodes[lagging].local_get(key.as_bytes())),
            None,
            "lagging replica {lagging} received already-handed-off key {key} \
             via a snapshot captured after narrowing (seed={seed})"
        );
    }
}

/// The release-GC teardown ordering (`animusd::cp_gc_tablet`, ADR 0029): narrow
/// a group's `StorageScope` to the tablet's *current* range **before**
/// `erase_scope`, so a group whose scope is still stale-wide (a split narrowed
/// the tablet's replicated range, but this node's own `cp_join_host_loop`
/// narrow tick hasn't run yet) never tombstones a co-hosted sibling's live keys
/// that happen to share the same physical engine + prefix (ADR 0026/0028).
///
/// Deliberately does not model a real split or the narrow-tick race at all —
/// that would need real-time/`ProdEnv` (`cp_rebalance_gc.rs` covers the
/// end-to-end race). Here the bug is reproduced directly at the primitive
/// level: build every replica's scope WHOLE (as it is right after a split,
/// before any narrow tick), populate both the soon-to-be-kept lower half
/// (this tablet's own, post-split range) and a co-hosted sibling's upper half
/// (data on the very same engine/prefix a split's new child would occupy),
/// then narrow-then-erase exactly as the fixed `cp_gc_tablet` now does, and
/// assert the sibling's keys — value AND version — are completely untouched
/// while the tablet's own (narrowed) range is fully tombstoned.
#[test]
fn narrow_then_erase_scope_spares_a_co_hosted_siblings_data() {
    let seed = 0x5C10;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect
    let l = leader(&nodes, &[0, 1, 2], seed);

    // Populate BOTH the soon-to-be-kept lower half ("lo", < BOUNDARY — this
    // tablet's own range after a hypothetical split) and a co-hosted sibling's
    // upper half ("hi", >= BOUNDARY) — sharing one physical engine + prefix,
    // exactly as a split's parent + child do on the same node (ADR 0028).
    for i in 0..5u64 {
        put(
            &nodes,
            l,
            seed,
            format!("a{i:02}").as_bytes(),
            format!("lo{i}").as_bytes(),
        );
        put(
            &nodes,
            l,
            seed,
            format!("z{i:02}").as_bytes(),
            format!("hi{i}").as_bytes(),
        );
    }
    sim.run_for(Duration::from_secs(1));

    // Sanity: before narrowing, every replica's still-whole scope really does
    // see the sibling's keys too — proving the pre-fix ordering (erase without
    // narrowing first) would have tombstoned them.
    for node in &nodes {
        assert_eq!(
            block_on(node.local_get(b"z00")),
            Some(b"hi0".to_vec()),
            "sanity: the whole scope must see the sibling's keys pre-erase"
        );
    }
    // Capture the sibling's raw versioned records (bypassing scope filtering
    // entirely, straight off the engine) so the post-erase assertion below can
    // confirm the version is untouched too, not just the value.
    let physical = |key: &[u8]| {
        let mut out = b"T:".to_vec();
        out.extend_from_slice(key);
        out
    };
    let sibling_before: Vec<_> = (0..5u64)
        .map(|i| {
            let key = format!("z{i:02}");
            let vv = block_on(nodes[l].storage().get(&physical(key.as_bytes())))
                .expect("engine read ok")
                .expect("sibling key present pre-erase");
            (key, vv)
        })
        .collect();

    // The fix: narrow every replica to the tablet's CURRENT (post-split)
    // range, then erase — the exact order `cp_gc_tablet` now uses on the
    // release path (narrow via the tablet's replicated range, then
    // `erase_scope`).
    let kept_range = KeyRange::new(Vec::new(), Some(BOUNDARY.to_vec()));
    for node in &nodes {
        node.narrow_scope(kept_range.clone());
    }
    for node in &nodes {
        block_on(node.erase_scope());
    }

    // The tablet's own (now-narrowed) range is fully tombstoned on every
    // replica...
    for i in 0..5u64 {
        let key = format!("a{i:02}");
        for node in &nodes {
            assert_eq!(
                block_on(node.local_get(key.as_bytes())),
                None,
                "erase_scope must tombstone the tablet's own narrowed range \
                 (key {key})"
            );
        }
    }
    // ...but the co-hosted sibling's keys are completely untouched: same
    // value, same version, on every replica — the exact data a stale-wide
    // erase (the pre-fix ordering) would have corrupted.
    for (key, before) in &sibling_before {
        for node in &nodes {
            let after = block_on(node.storage().get(&physical(key.as_bytes())))
                .expect("engine read ok")
                .expect("sibling key must survive the erase");
            assert_eq!(
                after.value, before.value,
                "erase_scope corrupted a co-hosted sibling's value for {key}"
            );
            assert_eq!(
                after.version, before.version,
                "erase_scope bumped a co-hosted sibling's version for {key} \
                 (a version bump here would let a stale write shadow a fresh \
                 one under per-key LWW)"
            );
        }
    }
}
