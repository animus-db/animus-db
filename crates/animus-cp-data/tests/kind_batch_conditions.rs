//! ADR 0046 "evaluate at leader" seatbelt (PR1): `KvCommand::KindBatch`'s own
//! `conditions` field — the byte-level OCC primitive modeled directly on
//! `KvCommand::TxnStage`'s own `conditions` field (see `tests/
//! txn_conditions.rs`, which this file mirrors scenario-for-scenario), minus
//! the `StageOutcome` introspection channel: a `KindBatch` condition failure
//! no-ops silently, indistinguishable from a fence/seal miss, so these tests
//! only ever observe the engine's resulting state, never a returned outcome.
//!
//! Harness style borrowed from `tests/kind_batch.rs` (the `group`/`leader`/
//! `logical`/`stored` helpers) rather than `txn_conditions.rs`'s single-key
//! harness, since the primitive under test is `put_kind_batch_fenced`, not
//! `txn_stage_anchor`.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers).

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{KIND_BASE, KIND_LSI, RaftKvNode, StorageScope};
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{KeyRange, escape, partition_token};
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
const ELECT: Duration = Duration::from_secs(2);
const SETTLE: Duration = Duration::from_secs(2);

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

/// A real ADR 0022-shaped logical key: `partition_token(pk) || escape(pk) ||
/// rk`. Matches `kind_batch.rs`'s identical helper.
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

/// The raw stored bytes for `key` under `kind` on `node` — mirrors
/// `kind_batch.rs`'s identical helper (strips the committed-value envelope).
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

/// A condition matching the base key's current committed value lets the
/// whole batch (base row + an LSI row in the same entry) apply — the
/// unconditional-success dual of `TxnStage`'s identical scenario.
#[test]
fn matching_condition_lets_the_batch_apply() {
    let seed = 0x0046_0001;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, seed);

    let base = logical(b"alice", b"");
    let lsi = logical(b"alice", b"\x01age30");
    assert!(matches!(
        nodes[l].put_kind_batch(
            vec![(KIND_BASE, base.clone(), Some(b"v0".to_vec()))],
            Vec::new()
        ),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);

    assert!(
        matches!(
            nodes[l].put_kind_batch_fenced(
                vec![
                    (KIND_BASE, base.clone(), Some(b"v1".to_vec())),
                    (KIND_LSI, lsi.clone(), Some(b"row".to_vec())),
                ],
                Vec::new(),
                vec![(base.clone(), Some(b"v0".to_vec()))],
                KeyRange::whole(),
            ),
            ProposeResult::Accepted { .. }
        ),
        "leader {l} rejected the kind batch (seed={seed})"
    );
    sim.run_for(SETTLE);

    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            stored(node, KIND_BASE, &base).as_deref(),
            Some(&b"v1"[..]),
            "node {i} base not updated (seed={seed})"
        );
        assert_eq!(
            stored(node, KIND_LSI, &lsi).as_deref(),
            Some(&b"row"[..]),
            "node {i} missing the LSI row (seed={seed})"
        );
    }
}

/// A "must be absent" condition on a genuinely-absent key lets the batch
/// apply — the dual of the present-value case above.
#[test]
fn must_be_absent_condition_passes_when_truly_absent() {
    let seed = 0x0046_0002;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, seed);

    let base = logical(b"bob", b"");
    assert!(
        matches!(
            nodes[l].put_kind_batch_fenced(
                vec![(KIND_BASE, base.clone(), Some(b"created".to_vec()))],
                Vec::new(),
                vec![(base.clone(), None)],
                KeyRange::whole(),
            ),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}"
    );
    sim.run_for(SETTLE);

    assert_eq!(
        stored(&nodes[l], KIND_BASE, &base).as_deref(),
        Some(&b"created"[..]),
        "seed={seed}"
    );
}

/// A "must be absent" condition on a key that already holds a committed
/// value no-ops the WHOLE batch — including a second, unconditioned write in
/// the same entry, proving the whole-or-nothing property `TxnStage`'s own
/// mirror scenario proves for staging.
#[test]
fn must_be_absent_condition_fails_when_present_no_ops_the_whole_batch() {
    let seed = 0x0046_0003;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, seed);

    let base = logical(b"carol", b"");
    let lsi = logical(b"carol", b"\x01age30");
    assert!(matches!(
        nodes[l].put_kind_batch(
            vec![(KIND_BASE, base.clone(), Some(b"existing".to_vec()))],
            Vec::new()
        ),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);

    assert!(
        matches!(
            nodes[l].put_kind_batch_fenced(
                vec![
                    (KIND_BASE, base.clone(), Some(b"overwrite".to_vec())),
                    (KIND_LSI, lsi.clone(), Some(b"row".to_vec())),
                ],
                Vec::new(),
                vec![(base.clone(), None)], // must be absent — but it isn't.
                KeyRange::whole(),
            ),
            ProposeResult::Accepted { .. }
        ),
        "the propose itself is still accepted — the no-op happens at apply (seed={seed})"
    );
    sim.run_for(SETTLE);

    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            stored(node, KIND_BASE, &base).as_deref(),
            Some(&b"existing"[..]),
            "node {i}: the conditioned key's prior value must survive untouched (seed={seed})"
        );
        assert_eq!(
            stored(node, KIND_LSI, &lsi),
            None,
            "node {i}: whole-or-nothing — the OTHER key in the same batch must never have \
             landed either, even though it carried no condition of its own (seed={seed})"
        );
    }
}

/// A condition that does not match the current committed value no-ops the
/// whole batch — the single-key counterpart of the multi-key test above.
#[test]
fn mismatched_value_condition_no_ops_and_leaves_the_key_untouched() {
    let seed = 0x0046_0004;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, seed);

    let base = logical(b"dave", b"");
    assert!(matches!(
        nodes[l].put_kind_batch(
            vec![(KIND_BASE, base.clone(), Some(b"v0".to_vec()))],
            Vec::new()
        ),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);

    assert!(matches!(
        nodes[l].put_kind_batch_fenced(
            vec![(KIND_BASE, base.clone(), Some(b"v1".to_vec()))],
            Vec::new(),
            vec![(base.clone(), Some(b"999-wrong".to_vec()))],
            KeyRange::whole(),
        ),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);

    assert_eq!(
        stored(&nodes[l], KIND_BASE, &base).as_deref(),
        Some(&b"v0"[..]),
        "a failed condition must never let the write land (seed={seed})"
    );
}

/// The condition is checked BEFORE the fence/seal gate (this PR's own
/// deliberate ordering difference from `TxnStage`'s `condition_failure`, see
/// `KvCommand::KindBatch`'s doc) — but the two gates still compose by simple
/// AND: a batch that fails its condition AND falls outside its fence is
/// exactly as rejected as one that only fails one of the two. This scenario
/// pins that composition by using a fence that genuinely admits the base key
/// so only the condition is actually doing the rejecting.
#[test]
fn condition_check_composes_with_the_fence_gate() {
    let seed = 0x0046_0005;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, seed);

    let base = logical(b"erin", b"");
    assert!(matches!(
        nodes[l].put_kind_batch(
            vec![(KIND_BASE, base.clone(), Some(b"v0".to_vec()))],
            Vec::new()
        ),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);

    // A fence that genuinely contains the base key — the condition alone
    // must be what blocks this entry.
    let mut fence_end = base.clone();
    fence_end.push(0xFF);
    let fence = KeyRange::new(base.clone(), Some(fence_end));
    assert!(
        fence.contains(&base),
        "setup: the base key must be INSIDE the fence, or this test is vacuous"
    );

    assert!(matches!(
        nodes[l].put_kind_batch_fenced(
            vec![(KIND_BASE, base.clone(), Some(b"v1".to_vec()))],
            Vec::new(),
            vec![(base.clone(), Some(b"wrong".to_vec()))],
            fence,
        ),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);

    assert_eq!(
        stored(&nodes[l], KIND_BASE, &base).as_deref(),
        Some(&b"v0"[..]),
        "an in-fence entry with a failing condition must still no-op (seed={seed})"
    );
}

/// Crash/restart WAL-replay idempotency (mirrors `txn_conditions.rs`'s
/// identical scenario): a condition-gated batch that committed before a
/// restart re-derives the identical committed value after the node recovers
/// via WAL replay.
#[test]
fn condition_gated_batch_survives_crash_restart_idempotently() {
    let seed = 0x0046_0006;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let id = nid(0);

    let node: KvNode = RaftKvNode::start_scoped(
        sim.env(id.clone()),
        vec![id.clone()],
        engine.clone(),
        StorageScope::new(escape(b"users"), KeyRange::whole()),
    );
    sim.run_for(ELECT);

    let base = logical(b"frank", b"");
    assert!(matches!(
        node.put_kind_batch(
            vec![(KIND_BASE, base.clone(), Some(b"v0".to_vec()))],
            Vec::new()
        ),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);

    assert!(matches!(
        node.put_kind_batch_fenced(
            vec![(KIND_BASE, base.clone(), Some(b"v1".to_vec()))],
            Vec::new(),
            vec![(base.clone(), Some(b"v0".to_vec()))],
            KeyRange::whole(),
        ),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);
    assert_eq!(stored(&node, KIND_BASE, &base).as_deref(), Some(&b"v1"[..]));

    // A genuine process restart (`stop`, not `crash`/`restart`) — the WAL
    // survives on the same engine; a fresh `RaftKvNode::start_scoped` replays
    // it from scratch, re-applying the conditioned `KindBatch` entries exactly
    // as they first applied.
    sim.stop(id.clone());
    let restarted: KvNode = RaftKvNode::start_scoped(
        sim.env(id.clone()),
        vec![id.clone()],
        engine.clone(),
        StorageScope::new(escape(b"users"), KeyRange::whole()),
    );
    sim.run_for(ELECT);

    assert_eq!(
        stored(&restarted, KIND_BASE, &base).as_deref(),
        Some(&b"v1"[..]),
        "WAL replay of a condition-gated batch must re-derive the identical committed value \
         (seed={seed})"
    );
}

/// The whole suite above is reproducible from its seed — a light determinism
/// sweep across a handful of fresh seeds re-running the matching-condition
/// scenario, mirroring every other file in this crate's
/// `run_is_deterministic_from_seed` convention.
#[test]
fn matching_condition_scenario_is_reproducible_across_seeds() {
    for seed in [
        0x0046_1001,
        0x0046_1002,
        0x0046_1003,
        0x0046_1004,
        0x0046_1005,
    ] {
        let (mut sim, nodes) = group(seed);
        sim.run_for(ELECT);
        let l = leader(&nodes, seed);

        let base = logical(b"grace", b"");
        assert!(matches!(
            nodes[l].put_kind_batch(
                vec![(KIND_BASE, base.clone(), Some(b"v0".to_vec()))],
                Vec::new()
            ),
            ProposeResult::Accepted { .. }
        ));
        sim.run_for(SETTLE);

        assert!(matches!(
            nodes[l].put_kind_batch_fenced(
                vec![(KIND_BASE, base.clone(), Some(b"v1".to_vec()))],
                Vec::new(),
                vec![(base.clone(), Some(b"v0".to_vec()))],
                KeyRange::whole(),
            ),
            ProposeResult::Accepted { .. }
        ));
        sim.run_for(SETTLE);
        assert_eq!(
            stored(&nodes[l], KIND_BASE, &base).as_deref(),
            Some(&b"v1"[..]),
            "seed={seed}"
        );
    }
}
