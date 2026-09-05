//! `KvCommand::KindEval` (ADR 0054 step 2, wired into `animusd` at step 3):
//! the self-contained evaluated write — apply reads the current committed
//! item, evaluates the condition and the operation, and derives the index
//! rows/change record, all in one place, in commit order. Every scenario
//! here drives [`RaftKvNode::propose_kind_eval`] directly, the same way
//! `tests/kind_batch.rs` drives `put_kind_batch` (its own former
//! `conditions` OCC seatbelt, `put_kind_batch_conditioned`, was deleted at
//! step 4b — `KindEval`'s apply-time read replaced the need for it).
//!
//! Harness style borrowed wholesale from `tests/kind_batch.rs` (the
//! `group`/`leader`/`logical`/`stored` helpers).
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers).

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{KIND_BASE, KIND_CHANGE, KIND_LSI, KindBatchOutcome, KindEvalOp, RaftKvNode};
use animus_env::nid;
use animus_item::{
    AttributeValue, Comparator, ConditionExpression, Item, LsiDef, PathSegment, Projection,
    TableSchema, UpdateAction, WriteSchema, decode_stored_item, derive_kind_writes,
    encode_stored_item,
};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::partition_token;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
const ELECT: Duration = Duration::from_secs(2);
const SETTLE: Duration = Duration::from_secs(2);

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

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

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_owned())
}

fn n(v: &str) -> AttributeValue {
    AttributeValue::N(v.to_owned())
}

fn item(pairs: &[(&str, AttributeValue)]) -> Item {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

/// The `KIND_BASE`/`KIND_LSI` logical key an item's own identity resolves
/// to — matches `RaftKvNode::propose_kind_eval`'s own private
/// `kind_eval_base_key`/the ADR 0022 layout exactly, so a test can address
/// the same physical row `derive_kind_writes` would.
fn base_key(pk: &AttributeValue, sk: Option<&AttributeValue>) -> Vec<u8> {
    let mut key = partition_token(&animus_item::storage_key(pk, None)).to_vec();
    key.extend_from_slice(&animus_item::storage_key(pk, sk));
    key
}

fn schema_with_lsi() -> WriteSchema {
    WriteSchema {
        key: TableSchema::simple("pk"),
        lsis: vec![LsiDef {
            name: "byAge".to_owned(),
            sort_attribute: "age".to_owned(),
            projection: Projection::All,
        }],
        change_records_carry_images: true,
    }
}

fn schema_no_index() -> WriteSchema {
    WriteSchema {
        key: TableSchema::simple("pk"),
        lsis: Vec::new(),
        change_records_carry_images: false,
    }
}

/// Propose a `KindEval` on `nodes[leader]`, panicking with `context` on a
/// refusal, and return the accepted `(index, term)`.
#[allow(clippy::too_many_arguments)] // test helper, mirrors the propose signature's own arity
fn propose_ok(
    nodes: &[KvNode],
    l: usize,
    schema: WriteSchema,
    pk: AttributeValue,
    sk: Option<AttributeValue>,
    op: KindEvalOp,
    condition: Option<ConditionExpression>,
    seed: u64,
    context: &str,
) -> (u64, u64) {
    match nodes[l].propose_kind_eval(schema, pk, sk, op, condition, false) {
        ProposeResult::Accepted { index, term } => (index, term),
        other => panic!("{context}: KindEval rejected: {other:?} (seed={seed})"),
    }
}

// ---------------------------------------------------------------------------
// (a) Differential: a KindEval's own apply-time derivation is byte-identical
//     to a KindBatch built by hand from the pure `derive_kind_writes` core.
// ---------------------------------------------------------------------------

#[test]
fn kind_eval_derives_byte_identical_rows_to_a_hand_built_kind_batch() {
    let seed = 0x0054_0001;
    let schema = schema_with_lsi();
    let pk = s("alice");
    let new = item(&[("pk", pk.clone()), ("age", n("30"))]);

    // Path 1: a fresh group evaluates a `KindEval` Put at apply.
    let (mut sim_a, nodes_a) = group(seed);
    sim_a.run_for(ELECT);
    let la = leader(&nodes_a, seed);
    propose_ok(
        &nodes_a,
        la,
        schema.clone(),
        pk.clone(),
        None,
        KindEvalOp::Put(new.clone()),
        None,
        seed,
        "path 1",
    );
    sim_a.run_for(SETTLE);

    // Path 2: an independent group applies a `KindBatch` built by hand from
    // the identical pure `derive_kind_writes` call `evaluate_kind_eval`
    // itself makes.
    let seed_b = seed.wrapping_add(1);
    let (mut sim_b, nodes_b) = group(seed_b);
    sim_b.run_for(ELECT);
    let lb = leader(&nodes_b, seed_b);
    let token = partition_token(&animus_item::storage_key(&pk, None));
    let derived = derive_kind_writes(
        &schema,
        &pk,
        None,
        &token,
        encode_stored_item(&new),
        None,
        Some(&new),
        false,
        KIND_BASE,
        KIND_LSI,
    );
    match nodes_b[lb].put_kind_batch(derived.writes.clone(), vec![derived.change_log.clone()]) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("path 2: KindBatch rejected: {other:?} (seed={seed_b})"),
    }
    sim_b.run_for(SETTLE);

    let key = base_key(&pk, None);
    let lsi_key = {
        let mut k = partition_token(&animus_item::storage_key(&pk, None)).to_vec();
        k.extend_from_slice(&animus_item::index::lsi_row_key(
            &pk,
            "byAge",
            &n("30"),
            None,
        ));
        k
    };
    let change_prefix = {
        let mut k = partition_token(&animus_item::storage_key(&pk, None)).to_vec();
        k.extend_from_slice(&animus_item::index::change_prefix(&pk));
        k
    };
    for i in 0..NODES.len() {
        let base_a = block_on(nodes_a[i].local_get_kind(KIND_BASE, &key));
        let base_b = block_on(nodes_b[i].local_get_kind(KIND_BASE, &key));
        assert_eq!(
            base_a, base_b,
            "node {i} base row must be byte-identical (seed={seed})"
        );
        assert_eq!(base_a.as_deref(), Some(encode_stored_item(&new).as_slice()));

        let lsi_a = block_on(nodes_a[i].local_get_kind(KIND_LSI, &lsi_key));
        let lsi_b = block_on(nodes_b[i].local_get_kind(KIND_LSI, &lsi_key));
        assert_eq!(
            lsi_a, lsi_b,
            "node {i} LSI row must be byte-identical (seed={seed})"
        );
        assert!(lsi_a.is_some(), "node {i} LSI row must exist");

        // The change record's own VALUE (never its key, whose HLC suffix
        // legitimately differs between the two independent groups).
        let change_a =
            block_on(nodes_a[i].local_scan_kind(KIND_CHANGE, &change_prefix, None, None));
        let change_b =
            block_on(nodes_b[i].local_scan_kind(KIND_CHANGE, &change_prefix, None, None));
        assert_eq!(
            change_a.len(),
            1,
            "node {i} exactly one change record (path 1)"
        );
        assert_eq!(
            change_b.len(),
            1,
            "node {i} exactly one change record (path 2)"
        );
        assert_eq!(
            change_a[0].1, change_b[0].1,
            "node {i} change record value must be byte-identical (seed={seed})"
        );
    }
}

// ---------------------------------------------------------------------------
// (b) A false condition no-ops the whole write — every replica, no row
//     changes.
// ---------------------------------------------------------------------------

#[test]
fn a_false_condition_leaves_every_row_untouched_on_every_replica() {
    let seed = 0x0054_0002;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, seed);
    let schema = schema_with_lsi();
    let pk = s("bob");

    let original = item(&[("pk", pk.clone()), ("age", n("30"))]);
    propose_ok(
        &nodes,
        l,
        schema.clone(),
        pk.clone(),
        None,
        KindEvalOp::Put(original.clone()),
        None,
        seed,
        "seed put",
    );
    sim.run_for(SETTLE);

    let key = base_key(&pk, None);
    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(node.local_get_kind(KIND_BASE, &key)).as_deref(),
            Some(encode_stored_item(&original).as_slice()),
            "node {i} seeded correctly"
        );
    }

    // A condition that cannot hold: `age = 999`.
    let false_condition = ConditionExpression::Compare("age".to_owned(), Comparator::Eq, n("999"));
    let (index, term) = propose_ok(
        &nodes,
        l,
        schema,
        pk.clone(),
        None,
        KindEvalOp::Update {
            key_item: item(&[("pk", pk.clone())]),
            actions: vec![UpdateAction::Set(
                vec![PathSegment::Field("age".to_owned())],
                animus_item::UpdateExpr::value(n("999")),
            )],
        },
        Some(false_condition),
        seed,
        "conditioned update",
    );
    sim.run_for(SETTLE);

    match nodes[l].kind_batch_outcome(index) {
        Some((t, KindBatchOutcome::ConditionFailed { key: k })) => {
            assert_eq!(
                t, term,
                "outcome term must match the accepted term (seed={seed})"
            );
            assert_eq!(k, key);
        }
        other => panic!("expected ConditionFailed, got {other:?} (seed={seed})"),
    }
    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(node.local_get_kind(KIND_BASE, &key)).as_deref(),
            Some(encode_stored_item(&original).as_slice()),
            "node {i} row must be unchanged by the rejected write (seed={seed})"
        );
    }
}

// ---------------------------------------------------------------------------
// (c) Two proposals racing the propose→apply window both apply — the ADR's
//     own motivating property, now provable with no `rmw_lock` involved.
// ---------------------------------------------------------------------------

#[test]
fn concurrent_add_proposals_against_one_key_both_apply_with_zero_refusals() {
    let seed = 0x0054_0003;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, seed);
    let schema = schema_no_index();
    let pk = s("counter");

    // Seed the counter at zero.
    propose_ok(
        &nodes,
        l,
        schema.clone(),
        pk.clone(),
        None,
        KindEvalOp::Put(item(&[("pk", pk.clone()), ("n", n("0"))])),
        None,
        seed,
        "seed",
    );
    sim.run_for(SETTLE);

    let add_one = |key_item: Item| KindEvalOp::Update {
        key_item,
        actions: vec![UpdateAction::Add(
            vec![PathSegment::Field("n".to_owned())],
            n("1"),
        )],
    };

    // Both proposed BEFORE either applies — the exact race ADR 0054 exists
    // to absorb: under the leader-evaluates-then-proposes design this used
    // to refuse one of the two (the "2 of 10 concurrent increments
    // refused" measurement the ADR cites); apply now evaluates each in
    // COMMIT order, so both see the truth at their own turn.
    let (i1, t1) = propose_ok(
        &nodes,
        l,
        schema.clone(),
        pk.clone(),
        None,
        add_one(item(&[("pk", pk.clone())])),
        None,
        seed,
        "add #1",
    );
    let (i2, t2) = propose_ok(
        &nodes,
        l,
        schema,
        pk.clone(),
        None,
        add_one(item(&[("pk", pk.clone())])),
        None,
        seed,
        "add #2",
    );
    assert_ne!(
        i1, i2,
        "two distinct proposals get two distinct log indices"
    );
    sim.run_for(SETTLE);

    for (label, index, term) in [("#1", i1, t1), ("#2", i2, t2)] {
        match nodes[l].kind_batch_outcome(index) {
            Some((t, KindBatchOutcome::Applied)) => {
                assert_eq!(
                    t, term,
                    "add {label}'s outcome term must match (seed={seed})"
                );
            }
            other => panic!(
                "add {label} was refused: {other:?} (seed={seed}) — ADR 0054 exists precisely to prevent this"
            ),
        }
    }

    let key = base_key(&pk, None);
    let final_item = block_on(nodes[l].local_get_kind(KIND_BASE, &key))
        .and_then(|b| decode_stored_item(&b).expect("decodes"))
        .unwrap_or_else(|| panic!("counter item missing (seed={seed})"));
    assert_eq!(
        final_item.get("n"),
        Some(&n("2")),
        "both increments landed exactly once each (seed={seed})"
    );
}

// ---------------------------------------------------------------------------
// (d) The leader-local result payload: the proposer sees it, a
//     non-registered replica never does, and it is gone after one read.
// ---------------------------------------------------------------------------

#[test]
fn the_leader_local_result_payload_is_scoped_to_the_proposer_and_consumed_once() {
    let seed = 0x0054_0004;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, seed);
    let other = (0..NODES.len())
        .find(|&i| i != l)
        .expect("a non-leader exists");
    let schema = schema_no_index();
    let pk = s("dana");
    let new = item(&[("pk", pk.clone()), ("v", n("1"))]);

    let (index, term) = propose_ok(
        &nodes,
        l,
        schema,
        pk.clone(),
        None,
        KindEvalOp::Put(new.clone()),
        None,
        seed,
        "put",
    );
    sim.run_for(SETTLE);

    // Every replica applied — sanity check via the replicated outcome.
    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            node.kind_batch_outcome(index).map(|(_, o)| o),
            Some(KindBatchOutcome::Applied),
            "node {i} must have applied the entry (seed={seed})"
        );
    }

    // The non-proposing replica never registered interest — it sees
    // nothing, on any of its own (index, term) reads.
    assert_eq!(
        nodes[other].take_kind_eval_result(index, term),
        None,
        "a node that never proposed this entry must never see its payload (seed={seed})"
    );

    // The proposer sees the real old/new images...
    let result = nodes[l]
        .take_kind_eval_result(index, term)
        .unwrap_or_else(|| panic!("the proposer must see its own payload (seed={seed})"));
    assert_eq!(result.old, None, "the item did not exist before this write");
    assert_eq!(result.new, Some(new));

    // ...and exactly once — a second read finds the slot already consumed.
    assert_eq!(
        nodes[l].take_kind_eval_result(index, term),
        None,
        "the slot must be dropped after being read once (seed={seed})"
    );
}

// ---------------------------------------------------------------------------
// (e) The sealed/frozen gate rejects a `KindEval` exactly as it rejects a
//     `KindBatch`.
// ---------------------------------------------------------------------------

#[test]
fn a_frozen_group_seals_a_kind_eval_exactly_like_a_kind_batch() {
    let seed = 0x0054_0005;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, seed);

    match nodes[l].propose_freeze() {
        ProposeResult::Accepted { .. } => {}
        other => panic!("freeze rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(SETTLE);
    for (i, node) in nodes.iter().enumerate() {
        assert!(node.is_frozen(), "node {i} must be frozen (seed={seed})");
    }

    let schema = schema_no_index();
    let pk = s("evan");
    let key = base_key(&pk, None);
    let (index, term) = propose_ok(
        &nodes,
        l,
        schema,
        pk.clone(),
        None,
        KindEvalOp::Put(item(&[("pk", pk.clone())])),
        None,
        seed,
        "put against a frozen group",
    );
    sim.run_for(SETTLE);

    match nodes[l].kind_batch_outcome(index) {
        Some((t, KindBatchOutcome::Sealed { key: k })) => {
            assert_eq!(t, term, "seal outcome term must match (seed={seed})");
            assert_eq!(k, key);
        }
        other => panic!("expected Sealed, got {other:?} (seed={seed})"),
    }
    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(node.local_get_kind(KIND_BASE, &key)),
            None,
            "node {i}: a sealed write must not have landed (seed={seed})"
        );
    }
}

// ---------------------------------------------------------------------------
// (f) Crash/restart mid-batch replays to the identical state.
// ---------------------------------------------------------------------------

#[test]
fn a_kind_eval_survives_crash_restart_idempotently() {
    let seed = 0x0054_0006;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let id = nid(0);

    let node: KvNode = RaftKvNode::start(sim.env(id.clone()), vec![id.clone()], engine.clone());
    sim.run_for(ELECT);

    let schema = schema_with_lsi();
    let pk = s("frank");
    let v0 = item(&[("pk", pk.clone()), ("age", n("30"))]);
    match node.propose_kind_eval(
        schema.clone(),
        pk.clone(),
        None,
        KindEvalOp::Put(v0.clone()),
        None,
        false,
    ) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("initial put rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(SETTLE);

    let v1 = item(&[("pk", pk.clone()), ("age", n("31"))]);
    match node.propose_kind_eval(
        schema,
        pk.clone(),
        None,
        KindEvalOp::Put(v1.clone()),
        None,
        false,
    ) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("second put rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(SETTLE);

    let key = base_key(&pk, None);
    assert_eq!(
        block_on(node.local_get_kind(KIND_BASE, &key)).as_deref(),
        Some(encode_stored_item(&v1).as_slice())
    );

    // A genuine process restart — the WAL survives on the same engine; a
    // fresh `RaftKvNode::start` replays it from scratch, re-applying both
    // `KindEval` entries exactly as they first applied (each one re-reads
    // whatever the previous replayed entry left, in the same commit order).
    sim.stop(id.clone());
    let restarted: KvNode = RaftKvNode::start(sim.env(id.clone()), vec![id.clone()], engine);
    sim.run_for(ELECT);

    assert_eq!(
        block_on(restarted.local_get_kind(KIND_BASE, &key)).as_deref(),
        Some(encode_stored_item(&v1).as_slice()),
        "WAL replay of a KindEval pair must re-derive the identical committed value (seed={seed})"
    );
    let lsi_key = {
        let mut k = partition_token(&animus_item::storage_key(&pk, None)).to_vec();
        k.extend_from_slice(&animus_item::index::lsi_row_key(
            &pk,
            "byAge",
            &n("31"),
            None,
        ));
        k
    };
    assert!(
        block_on(restarted.local_get_kind(KIND_LSI, &lsi_key)).is_some(),
        "the current LSI row must also survive replay (seed={seed})"
    );
    let stale_lsi_key = {
        let mut k = partition_token(&animus_item::storage_key(&pk, None)).to_vec();
        k.extend_from_slice(&animus_item::index::lsi_row_key(
            &pk,
            "byAge",
            &n("30"),
            None,
        ));
        k
    };
    assert!(
        block_on(restarted.local_get_kind(KIND_LSI, &stale_lsi_key)).is_none(),
        "the stale LSI row from the first value must have been removed, and stay removed \
         across replay (seed={seed})"
    );
}
