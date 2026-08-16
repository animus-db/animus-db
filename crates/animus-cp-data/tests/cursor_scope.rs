//! `KIND_CURSOR` (ADR 0042/0043 foundation): consumer cursor rows get their
//! own row-kind scope (ADR 0041 §3's mechanism, extended), and the ADR 0042
//! §7 min-over-rows rule's read-side primitives (`cursor_watermark`/
//! `cursor_rows`/`cursor_min_watermark`) behave correctly across the
//! tablet-lifecycle event that rule exists for: a source tablet's split
//! (`narrow_scope`). One test also exercises the raw `widen_scope`
//! `StorageScope` setter directly (no reconciler action calls it in
//! production today — tablets are split-only, ADR 0044) to prove the
//! min-over-rows primitive itself, not any specific lifecycle event, is
//! what's actually under test there.
//!
//! Mirrors `tests/kind_batch.rs`'s scope-isolation style, `tests/
//! snapshot_catchup.rs`'s InstallSnapshot-forcing technique, and `tests/
//! shared_engine.rs`'s two-groups-one-engine idiom for modeling co-hosted
//! sibling tablets of the same table (ADR 0026/0028).
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers).

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::cursor::{cursor_key, encode_watermark};
use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::{KIND_BASE, KIND_CURSOR, RaftKvNode, StorageScope};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::KeyRange;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
const GROUP_A: [u64; 3] = [0, 1, 2];
const GROUP_B: [u64; 3] = [10, 11, 12];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn ts(wall_ms: u64) -> HlcTimestamp {
    HlcTimestamp {
        wall_ms,
        logical: 0,
    }
}

fn group(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

fn scoped_group(
    sim: &Simulator,
    ids: &[u64; 3],
    engine: MemoryEngine,
    range: KeyRange,
) -> Vec<KvNode> {
    ids.iter()
        .map(|&id| {
            RaftKvNode::start_scoped(
                sim.env(nid(id)),
                ids.iter().copied().map(nid).collect(),
                engine.clone(),
                StorageScope::new(b"STREAMS:".to_vec(), range.clone()),
            )
        })
        .collect()
}

fn leader(nodes: &[KvNode], seed: u64) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(
        ls.len(),
        1,
        "expected exactly one leader, got {ls:?} (seed={seed})"
    );
    ls[0]
}

#[test]
fn kind_cursor_writes_land_in_their_own_scope_and_dont_alias_a_sibling_scope() {
    let seed = 0x0042_0001;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, seed);

    // A cursor row and a base row that happen to share the exact same
    // logical key bytes — the only way this test means anything (mirrors
    // `kind_batch.rs`'s identical trick for KIND_BASE vs KIND_FOOTPRINT).
    let shared_key = cursor_key(&[], "gsi");
    let watermark = ts(123_456);

    assert!(
        matches!(
            nodes[l].put_kind_batch(
                vec![
                    (
                        KIND_CURSOR,
                        shared_key.clone(),
                        Some(encode_watermark(watermark))
                    ),
                    (KIND_BASE, shared_key.clone(), Some(b"unrelated".to_vec())),
                ],
                Vec::new(),
            ),
            ProposeResult::Accepted { .. }
        ),
        "leader {l} rejected the kind batch (seed={seed})"
    );
    sim.run_for(Duration::from_secs(2));

    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(node.cursor_watermark("gsi")),
            Some(watermark),
            "node {i} cursor watermark (seed={seed})"
        );
        assert_eq!(
            block_on(node.local_get_kind(KIND_BASE, &shared_key)),
            Some(b"unrelated".to_vec()),
            "node {i} base row must not be shadowed by the cursor write (seed={seed})"
        );
        // The base scope must never see the cursor row's own bytes either.
        assert_ne!(
            block_on(node.local_get_kind(KIND_BASE, &shared_key)),
            Some(encode_watermark(watermark)),
            "node {i}: KIND_BASE must not alias the KIND_CURSOR value (seed={seed})"
        );
    }

    // Exactly one row, correctly tagged, via the whole-scope enumerator too.
    let rows = block_on(nodes[l].cursor_rows());
    assert_eq!(rows, vec![("gsi".to_string(), watermark)]);
}

#[test]
fn snapshot_catchup_carries_cursor_rows() {
    let seed = 0x0042_0002;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect
    let l = leader(&nodes, seed);
    let lagging = (0..3).find(|&i| i != l).expect("a follower exists");

    sim.crash(nid(lagging as u64));

    let watermark = ts(42);
    let cursor_row_key = cursor_key(&[], "copier");
    assert!(matches!(
        nodes[l].put_kind_batch(
            vec![(
                KIND_CURSOR,
                cursor_row_key.clone(),
                Some(encode_watermark(watermark))
            )],
            Vec::new(),
        ),
        ProposeResult::Accepted { .. }
    ));

    // Push well past the compaction threshold (64) with base writes so the
    // leader snapshots + truncates the log prefix the crashed follower would
    // otherwise need — the cursor row above must ride the resulting
    // `engine_image`/`InstallSnapshot`, not the (already-compacted) log tail.
    const N: u64 = 150;
    for i in 0..N {
        match nodes[l].put(
            format!("k{i:03}").into_bytes(),
            format!("v{i}").into_bytes(),
        ) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected put {i}: {other:?} (seed={seed})"),
        }
    }
    sim.run_for(Duration::from_secs(3));

    sim.restart(nid(lagging as u64));
    sim.run_for(Duration::from_secs(6));

    assert_eq!(
        block_on(nodes[lagging].cursor_watermark("copier")),
        Some(watermark),
        "follower {lagging} missing the cursor row after snapshot catch-up (seed={seed})"
    );
    // Sanity: the ordinary base data converged too (the pre-existing property
    // this test's structure is borrowed from).
    assert_eq!(
        block_on(nodes[lagging].local_get(b"k000")),
        Some(b"v0".to_vec())
    );
}

/// Split (ADR 0028's `narrow_scope`): the retained/left tablet's own
/// `range.start` never changes across a split (only its `range.end`
/// narrows), so a cursor row it wrote *before* the split — keyed from that
/// same unchanging `range.start` — stays visible after narrowing (ADR 0042
/// §7: "left child inherits the parent's row, same range.start"). A fresh
/// right sibling, born at the split boundary with no cursor row ever
/// written under its own (different) `range.start`, correctly finds none —
/// not because of any special-casing, simply because nothing was ever
/// written there.
#[test]
fn split_narrow_keeps_the_left_childs_cursor_row_visible_and_a_fresh_right_child_finds_none() {
    let seed = 0x0042_0003;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, seed);

    // Written while the tablet was still whole (range = [.., ..)), keyed off
    // its own range.start (empty — the ring's own beginning).
    let watermark = ts(7);
    assert!(matches!(
        nodes[l].put_kind_batch(
            vec![(
                KIND_CURSOR,
                cursor_key(&[], "gsi"),
                Some(encode_watermark(watermark))
            )],
            Vec::new(),
        ),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));

    // Simulate the split: every replica narrows to the retained (left)
    // range, its start unchanged.
    let left_range = KeyRange::new(Vec::new(), Some(b"m".to_vec()));
    for node in &nodes {
        node.narrow_scope(left_range.clone());
    }

    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(node.cursor_watermark("gsi")),
            Some(watermark),
            "left child {i} lost its own cursor row across a narrow (seed={seed})"
        );
    }

    // A separate group modeling the freshly-minted right sibling
    // (range.start = the split key "m"): nothing was ever written keyed off
    // its own start, so it must find no row at all, not a stale/borrowed one.
    let right_sim = Simulator::new(seed ^ 0x5EED);
    let right_range = KeyRange::new(b"m".to_vec(), None);
    let right_nodes = scoped_group(&right_sim, &GROUP_B, MemoryEngine::new(), right_range);
    let mut right_sim = right_sim;
    right_sim.run_for(Duration::from_secs(2));
    for node in &right_nodes {
        assert_eq!(
            block_on(node.cursor_watermark("gsi")),
            None,
            "a fresh right child must never inherit a row it never wrote (seed={seed})"
        );
    }
}

/// Exercises the raw `widen_scope` `StorageScope` setter directly (no
/// reconciler action calls it in production today — tablets are split-only,
/// ADR 0044) to prove the ADR 0042 §7 min-over-rows rule itself, independent
/// of any specific lifecycle event that might trigger a scope change: two
/// co-hosted tablets of the **same table** share one physical engine under
/// one `StorageScope` prefix (ADR 0026/0028) — each wrote its own cursor row
/// while scoped to its own, disjoint range, keyed off its own `range.start`.
/// Once one tablet's scope is widened (by whatever means) to cover the
/// other's range, both physically-present rows become visible in its one
/// `KIND_CURSOR` scope, and `cursor_min_watermark` must report the
/// **minimum** of the two — never just its own, higher watermark (the one
/// genuine loss hazard the rule exists to close: silently claiming records a
/// row it never itself wrote).
#[test]
fn widened_scope_exposes_a_co_hosted_row_and_min_over_rows_picks_the_lower_watermark() {
    let seed = 0x0042_0004;
    let sim = Simulator::new(seed);
    let engine = MemoryEngine::new();

    // The eventual widener: starts scoped to [.., "m").
    let left = scoped_group(
        &sim,
        &GROUP_A,
        engine.clone(),
        KeyRange::new(Vec::new(), Some(b"m".to_vec())),
    );
    // The other co-hosted tablet whose row will become visible once `left`
    // widens: [ "m", ..) — same engine, same prefix, a distinct Raft group
    // (own node ids), exactly as two co-hosted tablets of one table share a
    // node's engine.
    let right = scoped_group(&sim, &GROUP_B, engine, KeyRange::new(b"m".to_vec(), None));

    let mut sim = sim;
    sim.run_for(Duration::from_secs(2));
    let l_left = leader(&left, seed);
    let l_right = leader(&right, seed);

    let left_watermark = ts(500);
    let right_watermark = ts(100); // lower — the other tablet's own row
    assert!(matches!(
        left[l_left].put_kind_batch(
            vec![(
                KIND_CURSOR,
                cursor_key(&[], "copier"),
                Some(encode_watermark(left_watermark))
            )],
            Vec::new(),
        ),
        ProposeResult::Accepted { .. }
    ));
    assert!(matches!(
        right[l_right].put_kind_batch(
            vec![(
                KIND_CURSOR,
                cursor_key(b"m", "copier"),
                Some(encode_watermark(right_watermark))
            )],
            Vec::new(),
        ),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));

    // The raw widen: `left`'s scope grows to cover `right`'s range too — no
    // reconciler action drives this in production (tablets are split-only,
    // ADR 0044); this test exercises `widen_scope` directly to prove the
    // read-side primitive, not any specific lifecycle event that might one
    // day call it.
    for node in &left {
        node.widen_scope(KeyRange::whole());
    }

    let mut expected = vec![
        ("copier".to_string(), right_watermark),
        ("copier".to_string(), left_watermark),
    ];
    expected.sort();

    for (i, node) in left.iter().enumerate() {
        let mut rows = block_on(node.cursor_rows());
        rows.sort();
        assert_eq!(
            rows, expected,
            "widened node {i} must see both rows post-widen (seed={seed})"
        );
        assert_eq!(
            block_on(node.cursor_min_watermark("copier")),
            Some(right_watermark),
            "widened node {i}: min-over-rows must pick the lower (other tablet's) \
             watermark, never just its own higher one (seed={seed})"
        );
    }
}
