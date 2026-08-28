//! `KvCommand::SeedBatch` (ADR 0050 Train B rung 4, fork F3): the split-build
//! driver's version-carrying row-transfer command. A chunk of the parent
//! tablet's raw engine rows — tombstones, MVCC versions, and value bytes
//! verbatim — proposed into a **child** group's own log and applied as
//! per-key-LWW merges at the **carried** versions, emitting nothing into the
//! child's change log.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()`. Corpus depth knob: `ANIMUS_SPLIT_SEEDS` (default 1).

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{KIND_BASE, KIND_LSI, RaftKvNode, StorageScope, hlc};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::KeyRange;
use animus_test::corpus;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// A parent group (whole range) and a child group (upper half, `m..`), each
/// on its own private engine (ADR 0050 rung 1), on distinct streams.
fn parent_and_child(seed: u64) -> (Simulator, Vec<KvNode>, Vec<KvNode>, Vec<MemoryEngine>) {
    let sim = Simulator::new(seed);
    let all: Vec<_> = NODES.iter().copied().map(nid).collect();
    let parent = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start_hosted(
                sim.env(nid(id)),
                all.clone(),
                MemoryEngine::new(),
                StorageScope::new(KeyRange::whole()),
                1,
            )
        })
        .collect();
    let child_engines: Vec<MemoryEngine> = NODES.iter().map(|_| MemoryEngine::new()).collect();
    let child = NODES
        .iter()
        .zip(&child_engines)
        .map(|(&id, engine)| {
            RaftKvNode::start_hosted(
                sim.env(nid(id)),
                all.clone(),
                engine.clone(),
                StorageScope::new(child_range()),
                2,
            )
        })
        .collect();
    (sim, parent, child, child_engines)
}

fn child_range() -> KeyRange {
    KeyRange::new(b"m".to_vec(), None)
}

fn leader(nodes: &[KvNode], seed: u64) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    ls[0]
}

fn settle(sim: &mut Simulator) {
    sim.run_for(Duration::from_secs(2));
}

/// Propose `rows` to the child leader and settle; panics on a non-accept.
fn seed(sim: &mut Simulator, child: &[KvNode], rows: Vec<animus_cp_data::SeedRow>, seed_n: u64) {
    let l = leader(child, seed_n);
    match child[l].propose_seed_batch(rows) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("seed propose refused: {other:?} (seed={seed_n})"),
    }
    settle(sim);
}

/// The teeth: rows across two kinds — a value, a tombstone, odd bytes — land
/// on the child **byte-identically at their carried versions**, the child's
/// change log stays empty, and re-proposing the same chunk is a no-op.
#[test]
fn seed_batch_installs_rows_byte_identically_and_emits_no_change_records() {
    let seed_n = 11;
    let (mut sim, _parent, child, engines) = parent_and_child(seed_n);
    settle(&mut sim);

    let rows = vec![
        (
            KIND_BASE,
            b"m-key".to_vec(),
            Some(vec![0u8, 1, 0xFF, 42]),
            1_000_777,
        ),
        (KIND_BASE, b"m-gone".to_vec(), None, 1_000_778), // a tombstone row
        (
            KIND_LSI,
            b"m-lsi".to_vec(),
            Some(b"lsi-bytes".to_vec()),
            1_000_779,
        ),
    ];
    seed(&mut sim, &child, rows.clone(), seed_n);

    let l = leader(&child, seed_n);
    let read_back = |engines: &[MemoryEngine], l: usize| {
        block_on(async {
            engines[l]
                .entries_with_tombstones()
                .await
                .expect("child engine scan")
        })
    };
    let before = read_back(&engines, l);
    // Physical layout post-F2b is [kind] || logical (ADR 0050 rung 2).
    let phys = |kind: u8, logical: &[u8]| -> Vec<u8> {
        let mut k = vec![kind];
        k.extend_from_slice(logical);
        k
    };
    assert_eq!(
        before,
        vec![
            (phys(KIND_BASE, b"m-gone"), None, 1_000_778),
            (
                phys(KIND_BASE, b"m-key"),
                Some(vec![0u8, 1, 0xFF, 42]),
                1_000_777
            ),
            (
                phys(KIND_LSI, b"m-lsi"),
                Some(b"lsi-bytes".to_vec()),
                1_000_779
            ),
        ],
        "seeded rows must land byte-identically at their carried versions (seed={seed_n})"
    );
    // No change-log emission: history transfer, not new mutation.
    let changes = block_on(child[l].pending_changes());
    assert!(
        changes.is_empty(),
        "a seed must emit nothing into the child's change log, got {changes:?}"
    );

    // Idempotence: the identical chunk re-proposed changes nothing.
    let rows_again = vec![
        (
            KIND_BASE,
            b"m-key".to_vec(),
            Some(vec![0u8, 1, 0xFF, 42]),
            1_000_777,
        ),
        (KIND_BASE, b"m-gone".to_vec(), None, 1_000_778),
        (
            KIND_LSI,
            b"m-lsi".to_vec(),
            Some(b"lsi-bytes".to_vec()),
            1_000_779,
        ),
    ];
    seed(&mut sim, &child, rows_again, seed_n);
    assert_eq!(
        read_back(&engines, l),
        before,
        "re-proposing the same chunk must be a byte-identical no-op"
    );
}

/// A row updated on the parent mid-build ships again at a higher carried
/// version and wins on the child; a stale re-send of the older row loses —
/// the per-key LWW discipline that makes tail passes safe in any order.
#[test]
fn mid_build_updates_win_by_carried_version_and_stale_resends_lose() {
    let seed_n = 12;
    let (mut sim, _parent, child, engines) = parent_and_child(seed_n);
    settle(&mut sim);

    seed(
        &mut sim,
        &child,
        vec![(KIND_BASE, b"m-k".to_vec(), Some(b"v1".to_vec()), 100)],
        seed_n,
    );
    seed(
        &mut sim,
        &child,
        vec![(KIND_BASE, b"m-k".to_vec(), Some(b"v2".to_vec()), 200)],
        seed_n,
    );
    // Stale duplicate of the older version: must lose.
    seed(
        &mut sim,
        &child,
        vec![(KIND_BASE, b"m-k".to_vec(), Some(b"v1".to_vec()), 100)],
        seed_n,
    );

    let l = leader(&child, seed_n);
    let rows = block_on(async { engines[l].entries_with_tombstones().await.unwrap() });
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1,
        Some(b"v2".to_vec()),
        "newest carried version wins"
    );
    assert_eq!(rows[0].2, 200);
}

// DELETED (ADR 0050 Train B rung 7): `seed_batch_outside_the_fence_is_a_
// whole_batch_noop` exercised SeedBatch's per-entry range fence, removed
// with the fence machinery (ranges are immutable; the driver filters rows
// to each child's range by construction, and the frozen-group seal is the
// surviving whole-batch gate — covered by `tests/freeze.rs`).

/// Depth knob (`ANIMUS_SPLIT_SEEDS`, default 1) — mirrors
/// `ANIMUS_RECONCILER_SEEDS`/`ANIMUS_RAFTKV_SEEDS`.
fn seeds_per_cell() -> u64 {
    corpus::seeds_from_env("ANIMUS_SPLIT_SEEDS") as u64
}

/// Corpus cell: the driver's whole recovery story is "re-run the pass" —
/// crash the child's leader mid-seed, let the survivors re-elect, re-propose
/// EVERY chunk (duplicates included), and the surviving replicas converge to
/// the exact final row set. Seed-scaled via `ANIMUS_SPLIT_SEEDS`.
#[test]
fn corpus_child_leader_crash_mid_seed_reseeds_to_convergence() {
    for round in 0..seeds_per_cell() {
        let seed_n = 4242 + round;
        let (mut sim, _parent, child, engines) = parent_and_child(seed_n);
        settle(&mut sim);

        let chunk1 = vec![(KIND_BASE, b"m-a".to_vec(), Some(b"v-a".to_vec()), 100)];
        let chunk2 = vec![(KIND_BASE, b"m-b".to_vec(), None, 101)];
        seed(&mut sim, &child, chunk1.clone(), seed_n);

        // Crash the child's leader mid-build; the survivors re-elect.
        let dead = leader(&child, seed_n);
        child[dead].shutdown();
        sim.run_for(Duration::from_secs(5));
        let survivors: Vec<usize> = (0..child.len())
            .filter(|&i| i != dead && child[i].is_leader())
            .collect();
        assert_eq!(survivors.len(), 1, "one new leader (seed={seed_n})");
        let l = survivors[0];

        // The re-led driver re-runs the pass from scratch: both chunks,
        // duplicate first chunk included.
        for chunk in [chunk1, chunk2] {
            match child[l].propose_seed_batch(chunk) {
                ProposeResult::Accepted { .. } => {}
                other => panic!("re-seed refused: {other:?} (seed={seed_n})"),
            }
            sim.run_for(Duration::from_secs(2));
        }
        let rows = block_on(async { engines[l].entries_with_tombstones().await.unwrap() });
        assert_eq!(
            rows,
            vec![
                (
                    {
                        let mut k = vec![KIND_BASE];
                        k.extend_from_slice(b"m-a");
                        k
                    },
                    Some(b"v-a".to_vec()),
                    100
                ),
                (
                    {
                        let mut k = vec![KIND_BASE];
                        k.extend_from_slice(b"m-b");
                        k
                    },
                    None,
                    101
                ),
            ],
            "converged after crash + full re-run (seed={seed_n})"
        );
    }
}

/// The witnessing teeth (the snapshot-install discipline applied to seeding):
/// after seeding a carried version minted well ahead of the child's own
/// clock, the child leader's own next write still lands **strictly above**
/// it — without the apply-arm witness this assert fails (the child would
/// mint below the seeded version and per-key LWW would drop its own write).
#[test]
fn a_child_leaders_own_writes_out_version_every_seeded_row() {
    let seed_n = 14;
    let (mut sim, _parent, child, engines) = parent_and_child(seed_n);
    settle(&mut sim);

    // A carried version ~10s ahead of the sim clock's own HLC domain.
    let ahead = hlc::pack(hlc::HlcTimestamp {
        wall_ms: 10_000,
        logical: 3,
    });
    seed(
        &mut sim,
        &child,
        vec![(KIND_BASE, b"m-k".to_vec(), Some(b"seeded".to_vec()), ahead)],
        seed_n,
    );

    let l = leader(&child, seed_n);
    match child[l].put(b"m-k".to_vec(), b"own-write".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("own write refused: {other:?}"),
    }
    settle(&mut sim);
    let rows = block_on(async { engines[l].entries_with_tombstones().await.unwrap() });
    let row = rows
        .iter()
        .find(|(k, _, _)| k.ends_with(b"m-k"))
        .expect("row");
    assert!(
        row.2 > ahead,
        "the child's own write must out-version the seeded row: {} <= {ahead}",
        row.2
    );
    assert_ne!(row.1, Some(b"seeded".to_vec()), "own write must win");
}
