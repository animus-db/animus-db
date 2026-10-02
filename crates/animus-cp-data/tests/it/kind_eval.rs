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
use animus_cp_data::{
    KIND_BASE, KIND_CHANGE, KIND_LSI, KindBatchOutcome, KindEvalBatchResult, KindEvalEntry,
    KindEvalItemResult, KindEvalOp, RaftKvNode,
};
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

// ---------------------------------------------------------------------------
// `KvCommand::KindEvalBatch` (issue #996 layer 1): one Raft entry carrying N
// independent evaluate-at-apply item writes for the same tablet. Every
// scenario below drives `RaftKvNode::propose_kind_eval_batch` directly, the
// same way the scenarios above drive the singular `propose_kind_eval`.
// ---------------------------------------------------------------------------

/// Propose a `KindEvalBatch` on `nodes[l]`, panicking with `context` on a
/// refusal, and return the accepted `(index, term)`.
fn propose_batch_ok(
    nodes: &[KvNode],
    l: usize,
    entries: Vec<KindEvalEntry>,
    seed: u64,
    context: &str,
) -> (u64, u64) {
    match nodes[l].propose_kind_eval_batch(entries) {
        ProposeResult::Accepted { index, term } => (index, term),
        other => panic!("{context}: KindEvalBatch rejected: {other:?} (seed={seed})"),
    }
}

/// Recovers a change record's own `(packed_hlc, ordinal)` pair from its
/// key's trailing 12 bytes — mirrors `materialize_derived`'s own key
/// completion (`prefix || hlc::pack(ts) || ordinal`, big-endian).
fn record_seqno_suffix(key: &[u8]) -> (u64, u32) {
    let n = key.len() - 12;
    let hlc = u64::from_be_bytes(key[n..n + 8].try_into().expect("8B"));
    let ordinal = u32::from_be_bytes(key[n + 8..].try_into().expect("4B"));
    (hlc, ordinal)
}

fn entry(
    schema: WriteSchema,
    pk: AttributeValue,
    op: KindEvalOp,
    condition: Option<ConditionExpression>,
) -> KindEvalEntry {
    KindEvalEntry {
        schema,
        pk,
        sk: None,
        op,
        condition,
        ttl_expired: false,
    }
}

// ---------------------------------------------------------------------------
// (a) N distinct-key Put/Delete/Update entries in one batch: all applied,
//     correct per-item old/new, and distinct, order-preserving change-log
//     ordinals sharing the entry's own ts.
// ---------------------------------------------------------------------------

#[test]
fn kind_eval_batch_applies_distinct_key_entries_with_ordered_change_records() {
    let seed = 0x0996_0001;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, seed);
    let schema = schema_no_index();

    let alice = s("alice");
    let bob = s("bob");
    let carol = s("carol");
    let entries = vec![
        entry(
            schema.clone(),
            alice.clone(),
            KindEvalOp::Put(item(&[("pk", alice.clone()), ("v", n("1"))])),
            None,
        ),
        entry(schema.clone(), bob.clone(), KindEvalOp::Delete, None),
        entry(
            schema.clone(),
            carol.clone(),
            KindEvalOp::Update {
                key_item: item(&[("pk", carol.clone())]),
                actions: vec![UpdateAction::Set(
                    vec![PathSegment::Field("v".to_owned())],
                    animus_item::UpdateExpr::value(n("9")),
                )],
            },
            None,
        ),
    ];
    let (index, term) = propose_batch_ok(&nodes, l, entries, seed, "distinct-key batch");
    sim.run_for(SETTLE);

    match nodes[l].kind_batch_outcome(index) {
        Some((t, KindBatchOutcome::Applied)) => {
            assert_eq!(t, term, "outcome term must match (seed={seed})");
        }
        other => panic!("expected Applied, got {other:?} (seed={seed})"),
    }

    let result = nodes[l]
        .take_kind_eval_batch_result(index, term)
        .unwrap_or_else(|| panic!("the proposer must see its own batch payload (seed={seed})"));
    assert_eq!(result.items.len(), 3, "seed={seed}");
    match &result.items[0] {
        KindEvalItemResult::Applied { old, new } => {
            assert_eq!(*old, None, "alice never existed (seed={seed})");
            assert_eq!(
                new.as_ref(),
                Some(&item(&[("pk", alice.clone()), ("v", n("1"))]))
            );
        }
        other => panic!("item0 (alice) unexpected: {other:?} (seed={seed})"),
    }
    match &result.items[1] {
        KindEvalItemResult::Applied { old, new } => {
            assert_eq!(*old, None, "bob never existed (seed={seed})");
            assert_eq!(*new, None, "a delete's own image is None (seed={seed})");
        }
        other => panic!("item1 (bob) unexpected: {other:?} (seed={seed})"),
    }
    match &result.items[2] {
        KindEvalItemResult::Applied { old, new } => {
            assert_eq!(*old, None, "carol never existed (seed={seed})");
            assert_eq!(
                new.as_ref(),
                Some(&item(&[("pk", carol.clone()), ("v", n("9"))]))
            );
        }
        other => panic!("item2 (carol) unexpected: {other:?} (seed={seed})"),
    }

    for (i, node) in nodes.iter().enumerate() {
        let got = |pk: &AttributeValue| {
            block_on(node.local_get_kind(KIND_BASE, &base_key(pk, None)))
                .and_then(|b| decode_stored_item(&b).expect("decodes"))
        };
        assert_eq!(
            got(&alice),
            Some(item(&[("pk", alice.clone()), ("v", n("1"))])),
            "node {i} alice (seed={seed})"
        );
        assert_eq!(got(&bob), None, "node {i} bob deleted (seed={seed})");
        assert_eq!(
            got(&carol),
            Some(item(&[("pk", carol.clone()), ("v", n("9"))])),
            "node {i} carol (seed={seed})"
        );
    }

    // Change-log: exactly 3 records, one per item, sharing this entry's own
    // ts, at distinct, order-preserving ordinals 0..3.
    let records = block_on(nodes[l].pending_changes_key_order());
    assert_eq!(records.len(), 3, "seed={seed}: {records:?}");
    let mut seqnos: Vec<(u64, u32)> = records
        .iter()
        .map(|(k, _)| record_seqno_suffix(k))
        .collect();
    seqnos.sort_unstable();
    let hlc0 = seqnos[0].0;
    assert!(
        seqnos.iter().all(|(h, _)| *h == hlc0),
        "every item in one entry shares that entry's own ts (seed={seed}): {seqnos:?}"
    );
    let ordinals: Vec<u32> = seqnos.iter().map(|(_, o)| *o).collect();
    assert_eq!(
        ordinals,
        vec![0, 1, 2],
        "ordinals must be exactly 0..3, no gaps or repeats (seed={seed})"
    );
}

// ---------------------------------------------------------------------------
// (b) One entry's own condition fails; its siblings still apply, and the
//     result vector's order matches the input order.
// ---------------------------------------------------------------------------

#[test]
fn kind_eval_batch_a_failed_condition_does_not_abort_its_siblings() {
    let seed = 0x0996_0002;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, seed);
    let schema = schema_no_index();
    let alice = s("alice");
    let bob = s("bob");

    propose_ok(
        &nodes,
        l,
        schema.clone(),
        alice.clone(),
        None,
        KindEvalOp::Put(item(&[("pk", alice.clone()), ("age", n("30"))])),
        None,
        seed,
        "seed alice",
    );
    sim.run_for(SETTLE);

    let false_condition = ConditionExpression::Compare("age".to_owned(), Comparator::Eq, n("999"));
    let entries = vec![
        entry(
            schema.clone(),
            alice.clone(),
            KindEvalOp::Update {
                key_item: item(&[("pk", alice.clone())]),
                actions: vec![UpdateAction::Set(
                    vec![PathSegment::Field("age".to_owned())],
                    animus_item::UpdateExpr::value(n("999")),
                )],
            },
            Some(false_condition),
        ),
        entry(
            schema,
            bob.clone(),
            KindEvalOp::Put(item(&[("pk", bob.clone())])),
            None,
        ),
    ];
    let (index, term) = propose_batch_ok(&nodes, l, entries, seed, "conditioned batch");
    sim.run_for(SETTLE);

    match nodes[l].kind_batch_outcome(index) {
        Some((t, KindBatchOutcome::Applied)) => assert_eq!(t, term, "seed={seed}"),
        other => panic!(
            "expected the whole entry to record Applied — one item's condition failure must \
             never abort the entry: {other:?} (seed={seed})"
        ),
    }
    let result = nodes[l]
        .take_kind_eval_batch_result(index, term)
        .unwrap_or_else(|| panic!("the proposer must see its own batch payload (seed={seed})"));
    assert_eq!(result.items.len(), 2, "seed={seed}");
    assert_eq!(
        result.items[0],
        KindEvalItemResult::ConditionFailed,
        "item0 (alice) must fail its own condition (seed={seed})"
    );
    match &result.items[1] {
        KindEvalItemResult::Applied { old, new } => {
            assert_eq!(*old, None, "seed={seed}");
            assert_eq!(new.as_ref(), Some(&item(&[("pk", bob.clone())])));
        }
        other => panic!("item1 (bob) must still apply on its own merits: {other:?} (seed={seed})"),
    }

    for (i, node) in nodes.iter().enumerate() {
        let got = |pk: &AttributeValue| {
            block_on(node.local_get_kind(KIND_BASE, &base_key(pk, None)))
                .and_then(|b| decode_stored_item(&b).expect("decodes"))
        };
        assert_eq!(
            got(&alice),
            Some(item(&[("pk", alice.clone()), ("age", n("30"))])),
            "node {i}: alice's row must be unchanged by the rejected item (seed={seed})"
        );
        assert_eq!(
            got(&bob),
            Some(item(&[("pk", bob.clone())])),
            "node {i}: bob's own write must still have landed (seed={seed})"
        );
    }
}

// ---------------------------------------------------------------------------
// (c) Two entries sharing the SAME key: the second observes the first's
//     write (its own `old` equals the first's `new`), and GSI/change-log
//     derivation is consistent with the final state only.
// ---------------------------------------------------------------------------

#[test]
fn kind_eval_batch_a_same_key_duplicate_observes_the_earlier_items_write() {
    let seed = 0x0996_0003;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, seed);
    let schema = schema_with_lsi();
    let dana = s("dana");

    let entries = vec![
        entry(
            schema.clone(),
            dana.clone(),
            KindEvalOp::Put(item(&[("pk", dana.clone()), ("age", n("10"))])),
            None,
        ),
        entry(
            schema,
            dana.clone(),
            KindEvalOp::Update {
                key_item: item(&[("pk", dana.clone())]),
                actions: vec![UpdateAction::Set(
                    vec![PathSegment::Field("age".to_owned())],
                    animus_item::UpdateExpr::value(n("20")),
                )],
            },
            None,
        ),
    ];
    let (index, term) = propose_batch_ok(&nodes, l, entries, seed, "same-key duplicate");
    sim.run_for(SETTLE);

    let result = nodes[l]
        .take_kind_eval_batch_result(index, term)
        .unwrap_or_else(|| panic!("the proposer must see its own batch payload (seed={seed})"));
    assert_eq!(result.items.len(), 2, "seed={seed}");
    let item0_new = match &result.items[0] {
        KindEvalItemResult::Applied { old, new } => {
            assert_eq!(
                *old, None,
                "dana never existed before this entry (seed={seed})"
            );
            new.clone().expect("item0 put a real item")
        }
        other => panic!("item0 unexpected: {other:?} (seed={seed})"),
    };
    match &result.items[1] {
        KindEvalItemResult::Applied { old, new } => {
            assert_eq!(
                old.as_ref(),
                Some(&item0_new),
                "item1 must observe item0's own write within this same entry, not a stale or \
                 absent read (seed={seed})"
            );
            assert_eq!(
                new.as_ref(),
                Some(&item(&[("pk", dana.clone()), ("age", n("20"))]))
            );
        }
        other => panic!("item1 unexpected: {other:?} (seed={seed})"),
    }

    let key = base_key(&dana, None);
    let lsi_key = |age: &str| {
        let mut k = partition_token(&animus_item::storage_key(&dana, None)).to_vec();
        k.extend_from_slice(&animus_item::index::lsi_row_key(
            &dana,
            "byAge",
            &n(age),
            None,
        ));
        k
    };
    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(node.local_get_kind(KIND_BASE, &key)).as_deref(),
            Some(encode_stored_item(&item(&[("pk", dana.clone()), ("age", n("20"))])).as_slice()),
            "node {i}: final base row must reflect only the LAST write (seed={seed})"
        );
        assert!(
            block_on(node.local_get_kind(KIND_LSI, &lsi_key("20"))).is_some(),
            "node {i}: the current LSI row must exist (seed={seed})"
        );
        assert!(
            block_on(node.local_get_kind(KIND_LSI, &lsi_key("10"))).is_none(),
            "node {i}: the stale LSI row from the first (overwritten) write must not survive \
             (seed={seed})"
        );
    }

    // Two records — one per item, GSI/change-log derivation consistent with
    // the final state only, at distinct ordinals.
    let records = block_on(nodes[l].pending_changes_key_order());
    assert_eq!(records.len(), 2, "seed={seed}: {records:?}");
    let mut ordinals: Vec<u32> = records
        .iter()
        .map(|(k, _)| record_seqno_suffix(k).1)
        .collect();
    ordinals.sort_unstable();
    assert_eq!(ordinals, vec![0, 1], "seed={seed}");
}

// ---------------------------------------------------------------------------
// (d) A batch against a sealed/frozen tablet: whole-entry `Sealed`, nothing
//     materialized, `take_kind_eval_batch_result` returns `None`.
// ---------------------------------------------------------------------------

#[test]
fn kind_eval_batch_against_a_frozen_group_seals_the_whole_entry() {
    let seed = 0x0996_0004;
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
    let eve = s("eve");
    let frank = s("frank");
    let key_eve = base_key(&eve, None);
    let key_frank = base_key(&frank, None);
    let entries = vec![
        entry(
            schema.clone(),
            eve.clone(),
            KindEvalOp::Put(item(&[("pk", eve.clone())])),
            None,
        ),
        entry(
            schema,
            frank.clone(),
            KindEvalOp::Put(item(&[("pk", frank.clone())])),
            None,
        ),
    ];
    let (index, term) = propose_batch_ok(&nodes, l, entries, seed, "batch against a frozen group");
    sim.run_for(SETTLE);

    match nodes[l].kind_batch_outcome(index) {
        Some((t, KindBatchOutcome::Sealed { key: k })) => {
            assert_eq!(t, term, "seal outcome term must match (seed={seed})");
            assert_eq!(
                k, key_eve,
                "the first item's own key names the seal (seed={seed})"
            );
        }
        other => panic!("expected Sealed, got {other:?} (seed={seed})"),
    }
    assert_eq!(
        nodes[l].take_kind_eval_batch_result(index, term),
        None,
        "a sealed entry fills no per-item results (seed={seed})"
    );
    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(node.local_get_kind(KIND_BASE, &key_eve)),
            None,
            "node {i}: a sealed batch must not materialize any item (seed={seed})"
        );
        assert_eq!(
            block_on(node.local_get_kind(KIND_BASE, &key_frank)),
            None,
            "node {i}: neither item — sealed whole, never torn (seed={seed})"
        );
    }
}

// ---------------------------------------------------------------------------
// (e) The result is identified by term: taking with the wrong term returns
//     `None`, mirroring `the_leader_local_result_payload_is_scoped_to_the_
//     proposer_and_consumed_once`'s single-item identity discipline.
// ---------------------------------------------------------------------------

#[test]
fn kind_eval_batch_result_is_identified_by_term_not_index_alone() {
    let seed = 0x0996_0005;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let l = leader(&nodes, seed);
    let schema = schema_no_index();
    let gina = s("gina");

    let entries = vec![entry(
        schema,
        gina.clone(),
        KindEvalOp::Put(item(&[("pk", gina.clone())])),
        None,
    )];
    let (index, term) = propose_batch_ok(&nodes, l, entries, seed, "single-item batch");
    sim.run_for(SETTLE);

    assert_eq!(
        nodes[l].kind_batch_outcome(index).map(|(_, o)| o),
        Some(KindBatchOutcome::Applied),
        "sanity: the entry must have applied (seed={seed})"
    );
    assert_eq!(
        nodes[l].take_kind_eval_batch_result(index, term.wrapping_add(1)),
        None,
        "a term mismatch must never return this entry's payload — it may belong to a different, \
         reoccupying entry after a leadership change (seed={seed})"
    );
    let result: KindEvalBatchResult = nodes[l]
        .take_kind_eval_batch_result(index, term)
        .unwrap_or_else(|| {
            panic!("the correct (index, term) pair must still see the payload (seed={seed})")
        });
    assert_eq!(result.items.len(), 1, "seed={seed}");
}
