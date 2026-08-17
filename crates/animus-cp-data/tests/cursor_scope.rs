//! `KIND_CURSOR` (ADR 0042/0043 foundation): consumer cursor rows get their
//! own row-kind scope (ADR 0041 §3's mechanism, extended), and the read-side
//! primitives (`cursor_watermark`/`cursor_rows`/`cursor_min_watermark`)
//! behave correctly. (The pre-ADR-0050 split-narrow and raw-`widen_scope`
//! lifecycle tests died with the live-narrowable scope — a tablet's range is
//! immutable now, and the scenarios they drove are structurally
//! inexpressible; the min-over-rows read primitive itself is still covered
//! by the surviving tests here and by `animusd`'s drain suites.)
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers).

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::cursor::{cursor_key, encode_watermark};
use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::{KIND_BASE, KIND_CURSOR, RaftKvNode};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];

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
