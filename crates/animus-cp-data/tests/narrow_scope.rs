//! `RaftKvNode::narrow_scope` (PR5 of the single-command-split redesign):
//! a group's `StorageScope` range is live-narrowable, not fixed at
//! construction — the shape needed when this tablet is later the *source* of
//! a split: its own range shrinks while its physical data does not move, so
//! `engine_image` (unbounded, self-contained snapshot capture) must stop
//! shipping the already-handed-off portion once the group learns its range
//! has narrowed, even though that data is still physically present in every
//! live replica's own engine.
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
                sim.env(id),
                NODES.to_vec(),
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
    sim.crash(NODES[lagging]);

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
    sim.restart(NODES[lagging]);
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
