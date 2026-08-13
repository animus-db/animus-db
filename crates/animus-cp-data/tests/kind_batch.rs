//! Multi-kind atomic batch (ADR 0041 §3/§4): `KvCommand::KindBatch` commits
//! writes spanning several of a tablet's row-kind scopes as **one** Raft log
//! entry.
//!
//! This is the primitive materialized secondary indexes rest on, so these tests
//! pin the three properties the design actually depends on:
//!
//! 1. Writes land in **their own kind's scope** and nowhere else — the physical
//!    separation that keeps a base `Scan` from traversing change-log bytes.
//! 2. A `None` value in the same entry **removes** a row, so an overwrite can
//!    add the new index row and drop the stale one atomically.
//! 3. The whole entry is **fence-gated as a unit**: one out-of-range key and
//!    *nothing* applies, because a half-applied index write is exactly what
//!    colocating the kinds exists to prevent.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`, never
//! `run()` (the driver has perpetual heartbeat/election timers).

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{KIND_BASE, KIND_CHANGE, KIND_FOOTPRINT, KIND_LSI, RaftKvNode, StorageScope};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{KeyRange, escape, partition_token};
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// A real ADR 0022-shaped logical key: `partition_token(pk) || escape(pk) ||
/// rk`. Every kind's key leads with the token, which is what puts them all in
/// one tablet — and what `txn_stage` asserts elsewhere in this crate.
fn logical(pk: &[u8], rk: &[u8]) -> Vec<u8> {
    let mut out = partition_token(pk).to_vec();
    out.extend_from_slice(&escape(pk));
    out.extend_from_slice(rk);
    out
}

fn group(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start_scoped(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
                StorageScope::new(escape(b"users"), KeyRange::whole()),
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

/// The raw stored bytes for `key` under `kind` on `node`, addressed through the
/// group's own scope mapping (never a hand-assembled prefix — see the
/// `physical_key` doc). Strips the ADR 0018 §2/PR3 committed-value envelope
/// (leading tag `0`) the apply path wraps every committed value in.
fn stored(node: &KvNode, kind: u8, key: &[u8]) -> Option<Vec<u8>> {
    let raw = block_on(node.storage().get(&node.physical_key(kind, key)))
        .expect("engine read ok")?
        .value;
    assert_eq!(
        raw.first().copied(),
        Some(0u8),
        "expected a committed-value envelope (tag 0), got {raw:?}"
    );
    Some(raw[1..].to_vec())
}

#[test]
fn one_entry_writes_every_kind_into_its_own_scope() {
    let seed = 0x0041_0001;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, seed);

    let base = logical(b"alice", b"");
    let lsi = logical(b"alice", b"\x01age30");
    let change = logical(b"alice", b"\x02hlc");
    let footprint = logical(b"alice", b"");

    assert!(
        matches!(
            nodes[l].put_kind_batch(
                vec![
                    (KIND_BASE, base.clone(), Some(b"item".to_vec())),
                    (KIND_LSI, lsi.clone(), Some(b"lsi-row".to_vec())),
                    (KIND_CHANGE, change.clone(), Some(b"change".to_vec())),
                    (KIND_FOOTPRINT, footprint.clone(), Some(b"fp".to_vec())),
                ],
                None
            ),
            ProposeResult::Accepted { .. }
        ),
        "leader {l} rejected the kind batch (seed={seed})"
    );
    sim.run_for(Duration::from_secs(2));

    // Every replica has every kind, each in its own scope.
    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            stored(node, KIND_BASE, &base).as_deref(),
            Some(&b"item"[..]),
            "node {i} base"
        );
        assert_eq!(
            stored(node, KIND_LSI, &lsi).as_deref(),
            Some(&b"lsi-row"[..]),
            "node {i} lsi"
        );
        assert_eq!(
            stored(node, KIND_CHANGE, &change).as_deref(),
            Some(&b"change"[..]),
            "node {i} change"
        );
        assert_eq!(
            stored(node, KIND_FOOTPRINT, &footprint).as_deref(),
            Some(&b"fp"[..]),
            "node {i} footprint"
        );
    }

    // The separation that matters: the base and footprint keys are byte-identical
    // logical keys, yet reading one kind never sees the other's value. If the
    // kinds shared a keyspace this would be a silent overwrite, not a pass.
    assert_eq!(
        base, footprint,
        "this test is only meaningful if the keys collide"
    );
    assert_ne!(
        stored(&nodes[l], KIND_BASE, &base),
        stored(&nodes[l], KIND_FOOTPRINT, &footprint),
        "identical logical keys in different kinds must not alias (seed={seed})"
    );

    // And a base read never observes another kind's row at all.
    assert_eq!(
        stored(&nodes[l], KIND_BASE, &lsi),
        None,
        "the LSI row must not be visible in the base scope (seed={seed})"
    );
}

#[test]
fn one_entry_adds_a_new_index_row_and_removes_the_stale_one() {
    let seed = 0x0041_0002;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, seed);

    let base = logical(b"alice", b"");
    let old_row = logical(b"alice", b"\x01age30");
    let new_row = logical(b"alice", b"\x01age31");

    assert!(matches!(
        nodes[l].put_kind_batch(
            vec![
                (KIND_BASE, base.clone(), Some(b"age=30".to_vec())),
                (KIND_LSI, old_row.clone(), Some(b"row".to_vec())),
            ],
            None
        ),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));
    assert!(stored(&nodes[l], KIND_LSI, &old_row).is_some());

    // The overwrite: new base value, new index row, stale index row tombstoned —
    // all in one entry, so no replica can ever observe the pair disagreeing.
    assert!(matches!(
        nodes[l].put_kind_batch(
            vec![
                (KIND_BASE, base.clone(), Some(b"age=31".to_vec())),
                (KIND_LSI, old_row.clone(), None),
                (KIND_LSI, new_row.clone(), Some(b"row".to_vec())),
            ],
            None
        ),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));

    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            stored(node, KIND_BASE, &base).as_deref(),
            Some(&b"age=31"[..]),
            "node {i} base not overwritten (seed={seed})"
        );
        assert_eq!(
            stored(node, KIND_LSI, &old_row),
            None,
            "node {i} still holds the stale index row (seed={seed})"
        );
        assert_eq!(
            stored(node, KIND_LSI, &new_row).as_deref(),
            Some(&b"row"[..]),
            "node {i} missing the new index row (seed={seed})"
        );
    }
}

#[test]
fn an_out_of_fence_key_blocks_the_whole_entry() {
    let seed = 0x0041_0003;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, seed);

    let inside = logical(b"alice", b"");
    let lsi = logical(b"alice", b"\x01age30");
    // A fence that genuinely contains the base key but **not** the LSI row.
    // Getting this wrong is how this test would pass for the wrong reason: an
    // empty range (`start == end`, the range being half-open) excludes both
    // keys, so "nothing applied" would prove nothing about atomicity. The
    // assertions below pin the setup itself.
    let mut fence_end = inside.clone();
    fence_end.push(0x01);
    let fence = KeyRange::new(inside.clone(), Some(fence_end));
    assert!(
        fence.contains(&inside),
        "setup: the base key must be INSIDE the fence, or this test is vacuous"
    );
    assert!(
        !fence.contains(&lsi),
        "setup: the LSI key must be OUTSIDE the fence, or this test is vacuous"
    );

    assert!(matches!(
        nodes[l].put_kind_batch_fenced(
            vec![
                (KIND_BASE, inside.clone(), Some(b"item".to_vec())),
                (KIND_LSI, lsi.clone(), Some(b"lsi-row".to_vec())),
            ],
            None,
            fence,
        ),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));

    // Neither wrote: the fence gates the entry, not the individual keys. Letting
    // the base row through alone would leave an item with a missing index row —
    // precisely the inconsistency atomicity is here to rule out.
    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            stored(node, KIND_BASE, &inside),
            None,
            "node {i} applied the in-fence half of a fenced-out entry (seed={seed})"
        );
        assert_eq!(
            stored(node, KIND_LSI, &lsi),
            None,
            "node {i} applied the out-of-fence half (seed={seed})"
        );
    }
}

#[test]
fn the_change_log_key_is_the_entrys_own_commit_timestamp() {
    // The proposer supplies only a key *prefix*; apply completes it with this
    // entry's commit `ts`. That is what makes the log readable in commit order
    // (ADR 0041 §4a) — an edge cannot know the ts, since it is minted inside
    // `propose_ordered`, so letting it guess would silently mis-order the log.
    let seed = 0x0041_0004;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, seed);

    let base = logical(b"alice", b"");
    let prefix = logical(b"alice", b"");

    for (i, v) in [b"v1".to_vec(), b"v2".to_vec()].into_iter().enumerate() {
        assert!(
            matches!(
                nodes[l].put_kind_batch(
                    vec![(KIND_BASE, base.clone(), Some(v.clone()))],
                    Some((prefix.clone(), format!("rec{i}").into_bytes())),
                ),
                ProposeResult::Accepted { .. }
            ),
            "write {i} rejected (seed={seed})"
        );
        sim.run_for(Duration::from_secs(1));
    }

    // Two writes ⇒ two distinct, non-colliding log records under one prefix
    // (a single collapsing marker would have left one), in commit order.
    let scope_start = nodes[l].physical_key(KIND_CHANGE, &prefix);
    let mut scope_end = scope_start.clone();
    *scope_end.last_mut().unwrap() += 1;
    let rows = block_on(nodes[l].storage().scan(&scope_start, &scope_end)).expect("scan ok");
    let values: Vec<Vec<u8>> = rows
        .iter()
        .map(|(_, vv)| {
            assert_eq!(vv.value.first().copied(), Some(0u8), "committed envelope");
            vv.value[1..].to_vec()
        })
        .collect();
    assert_eq!(
        values,
        vec![b"rec0".to_vec(), b"rec1".to_vec()],
        "the log must be non-collapsing and in commit order (seed={seed})"
    );

    // And each record's key really is the prefix plus that write's commit ts,
    // so the suffix is strictly increasing rather than an edge-chosen value.
    let suffixes: Vec<Vec<u8>> = rows
        .iter()
        .map(|(k, _)| k[scope_start.len()..].to_vec())
        .collect();
    assert_eq!(suffixes.len(), 2);
    assert!(
        suffixes[0] < suffixes[1],
        "commit timestamps must increase (seed={seed})"
    );
    assert!(
        suffixes.iter().all(|s| s.len() == 8),
        "each suffix is a packed 8-byte HLC (seed={seed})"
    );
}

#[test]
fn kind_scoped_reads_see_their_own_scope_and_no_other() {
    let seed = 0x0041_0005;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, seed);

    let base = logical(b"alice", b"");
    let row_a = logical(b"alice", b"\x01a");
    let row_b = logical(b"alice", b"\x01b");

    assert!(matches!(
        nodes[l].put_kind_batch(
            vec![
                (KIND_BASE, base.clone(), Some(b"item".to_vec())),
                (KIND_LSI, row_a.clone(), Some(b"ra".to_vec())),
                (KIND_LSI, row_b.clone(), Some(b"rb".to_vec())),
            ],
            None,
        ),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(Duration::from_secs(2));

    // A kind read sees its own scope's value...
    assert_eq!(
        block_on(nodes[l].local_get_kind(KIND_LSI, &row_a)),
        Some(b"ra".to_vec())
    );
    // ...and never another kind's, even for a key that exists there.
    assert_eq!(block_on(nodes[l].local_get_kind(KIND_LSI, &base)), None);
    assert_eq!(block_on(nodes[l].local_get_kind(KIND_CHANGE, &row_a)), None);
    // The base row stays invisible to the LSI scope and vice versa.
    assert_eq!(block_on(nodes[l].local_get(&row_a)), None);

    // A scan is ordered, confined to its scope, and honours its bounds.
    let mut end = logical(b"alice", b"\x01");
    *end.last_mut().unwrap() += 1;
    let rows = block_on(nodes[l].local_scan_kind(KIND_LSI, &logical(b"alice", b"\x01"), &end));
    assert_eq!(
        rows,
        vec![
            (row_a.clone(), b"ra".to_vec()),
            (row_b.clone(), b"rb".to_vec())
        ]
    );
    assert!(
        block_on(nodes[l].local_scan_kind(KIND_CHANGE, &logical(b"alice", b"\x01"), &end))
            .is_empty()
    );

    // An unknown kind is inert rather than aliasing onto a real scope.
    assert_eq!(block_on(nodes[l].local_get_kind(200, &base)), None);
    assert!(block_on(nodes[l].local_scan_kind(200, &base, &end)).is_empty());
}
