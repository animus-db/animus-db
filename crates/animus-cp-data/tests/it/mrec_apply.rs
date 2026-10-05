//! ADR 0075 G-d M2: MREC last-writer-wins apply over a real (SimEnv) Raft
//! group. The pure rule and its convergence oracle live in
//! `src/mrec_props.rs`; this suite proves the same rule through the actual
//! apply arms (`KindEval`, `KindEvalBatch`, `TxnStage`/`TxnResolve`): stamps
//! are byte-identical on every replica, a replicated record applies only when
//! its stamp is strictly greater, a `Superseded` write leaves no trace (no
//! row, no LSI row, no change record), a tombstone keeps its stamp (no
//! resurrection), and a table with no `mrec` context is byte-identical to
//! before.
//!
//! Deterministic and seed-reproducible (ADR 0003): `run_for`, never `run()`.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_control::version::ClusterFeatures;
use animus_cp_data::{
    HostedOptions, KIND_BASE, KIND_CHANGE, KIND_LSI, KindBatchOutcome, KindEvalEntry,
    KindEvalItemResult, KindEvalOp, KindEvalResult, PendingTxnWrite, RaftKvNode, StageOutcome,
    StorageScope, TxnOutcome, TxnWrite,
};
use animus_env::{EnvExt, PRIMARY_STREAM, nid};
use animus_item::{
    AttributeValue, ChangeRecord, Item, LsiDef, MrecVersion, MrecWriteStamp, Projection,
    TableSchema, WriteSchema, decode_stored_item_versioned, encode_stored_item,
    encode_stored_item_versioned, encode_tombstone_versioned,
};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, partition_token};
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
const ELECT: Duration = Duration::from_secs(2);
const SETTLE: Duration = Duration::from_secs(2);
const TABLE: &str = "global";

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

const LOCAL: u32 = 0x00a1;
const REMOTE: u32 = 0x00b2;

/// The propose site refuses a gated shape while `Gate::MrecReplication` is
/// closed (cluster version 3, ADR 0073 Phase 2), so every group here is
/// started with the gate open.
fn mrec_options() -> HostedOptions<SimEnv> {
    let features = ClusterFeatures::new();
    features.update(&animus_control::Metadata {
        cluster_version: 3,
        ..Default::default()
    });
    HostedOptions {
        features,
        ..HostedOptions::default()
    }
}

fn group(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start_hosted_with_options(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
                StorageScope::whole(),
                PRIMARY_STREAM,
                mrec_options(),
            )
        })
        .collect();
    (sim, nodes)
}

fn leader(nodes: &[KvNode], seed: u64) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
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

fn base_key(pk: &AttributeValue) -> Vec<u8> {
    let mut key = partition_token(&animus_item::storage_key(pk, None)).to_vec();
    key.extend_from_slice(&animus_item::storage_key(pk, None));
    key
}

fn change_prefix(pk: &AttributeValue) -> Vec<u8> {
    let mut k = partition_token(&animus_item::storage_key(pk, None)).to_vec();
    k.extend_from_slice(&animus_item::index::change_prefix(pk));
    k
}

fn v(wall_ms: u64, logical: u32, region_id: u32) -> MrecVersion {
    MrecVersion {
        wall_ms,
        logical,
        region_id,
    }
}

/// An MREC table's write context: local region `LOCAL`, the leader's clock at
/// `wall_ms`.
fn mrec_schema(wall_ms: u64) -> WriteSchema {
    WriteSchema {
        key: TableSchema::simple("pk"),
        lsis: vec![LsiDef {
            name: "byAge".to_owned(),
            sort_attribute: "age".to_owned(),
            projection: Projection::All,
        }],
        change_records_carry_images: true,
        mrec: Some(MrecWriteStamp {
            region_id: LOCAL,
            wall_ms,
        }),
    }
}

fn plain_schema() -> WriteSchema {
    WriteSchema {
        mrec: None,
        ..mrec_schema(0)
    }
}

fn eval(
    sim: &mut Simulator,
    nodes: &[KvNode],
    schema: WriteSchema,
    pk: &AttributeValue,
    op: KindEvalOp,
    seed: u64,
) -> (u64, KindEvalResult) {
    let l = leader(nodes, seed);
    let (index, term) = match nodes[l].propose_kind_eval(schema, pk.clone(), None, op, None, false)
    {
        ProposeResult::Accepted { index, term } => (index, term),
        other => panic!("KindEval rejected: {other:?} (seed={seed})"),
    };
    sim.run_for(SETTLE);
    let result = nodes[l]
        .take_kind_eval_result(index, term)
        .unwrap_or_else(|| panic!("no leader-local result (seed={seed})"));
    (index, result)
}

fn replicate(item: Option<Item>, ver: MrecVersion) -> KindEvalOp {
    KindEvalOp::Replicate { item, ver }
}

/// The base row, asserted byte-identical on every replica.
fn base_row(nodes: &[KvNode], pk: &AttributeValue, seed: u64) -> Option<Vec<u8>> {
    let key = base_key(pk);
    let first = block_on(nodes[0].local_get_kind(KIND_BASE, &key));
    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(node.local_get_kind(KIND_BASE, &key)),
            first,
            "replica {i} base row must be byte-identical (seed={seed})"
        );
    }
    first
}

fn changes(nodes: &[KvNode], pk: &AttributeValue) -> Vec<ChangeRecord> {
    let prefix = change_prefix(pk);
    let end = animus_item::index::range_end(&prefix);
    block_on(nodes[0].local_scan_kind(KIND_CHANGE, &prefix, Some(&end), None))
        .into_iter()
        .map(|(_, v)| ChangeRecord::decode(&v).expect("decodes"))
        .collect()
}

fn lsi_rows(nodes: &[KvNode], pk: &AttributeValue) -> usize {
    let mut prefix = partition_token(&animus_item::storage_key(pk, None)).to_vec();
    prefix.extend_from_slice(&animus_item::index::lsi_index_prefix(pk, "byAge"));
    let end = animus_item::index::range_end(&prefix);
    block_on(nodes[0].local_scan_kind(KIND_LSI, &prefix, Some(&end), None)).len()
}

#[test]
fn a_local_write_stamps_identical_bytes_on_every_replica_and_bumps_from_the_stored_stamp() {
    let seed = 0x0075_0201;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let pk = s("alice");
    let first = item(&[("pk", pk.clone()), ("age", n("30"))]);

    // No stored row: the stamp is the write's own clock, logical 0, local region.
    eval(
        &mut sim,
        &nodes,
        mrec_schema(1_000),
        &pk,
        KindEvalOp::Put(first.clone()),
        seed,
    );
    assert_eq!(
        base_row(&nodes, &pk, seed),
        Some(encode_stored_item_versioned(&first, v(1_000, 0, LOCAL)))
    );

    // Same millisecond: logical bumps. Clock behind: still strictly above.
    let second = item(&[("pk", pk.clone()), ("age", n("31"))]);
    eval(
        &mut sim,
        &nodes,
        mrec_schema(1_000),
        &pk,
        KindEvalOp::Put(second.clone()),
        seed,
    );
    assert_eq!(
        base_row(&nodes, &pk, seed),
        Some(encode_stored_item_versioned(&second, v(1_000, 1, LOCAL)))
    );
    eval(
        &mut sim,
        &nodes,
        mrec_schema(400),
        &pk,
        KindEvalOp::Delete,
        seed,
    );
    // A delete writes a *versioned tombstone* (never an unversioned one).
    assert_eq!(
        base_row(&nodes, &pk, seed),
        Some(encode_tombstone_versioned(v(1_000, 2, LOCAL)))
    );
    // The change records carry the images as for any local write.
    let recs = changes(&nodes, &pk);
    assert_eq!(recs.len(), 3, "seed={seed}");
    assert_eq!(recs[2].old_image.as_ref(), Some(&second));
    assert_eq!(recs[2].new_image, None);
}

#[test]
fn replicate_applies_only_a_strictly_greater_stamp_and_a_loser_leaves_no_trace() {
    let seed = 0x0075_0202;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let pk = s("bob");
    let a = item(&[("pk", pk.clone()), ("age", n("20"))]);
    let b = item(&[("pk", pk.clone()), ("age", n("21"))]);
    let c = item(&[("pk", pk.clone()), ("age", n("22"))]);

    // Newer than an absent row: applies, `ver` written verbatim, LSI row + change record.
    let (_, r) = eval(
        &mut sim,
        &nodes,
        mrec_schema(1),
        &pk,
        replicate(Some(a.clone()), v(500, 0, REMOTE)),
        seed,
    );
    assert!(!r.superseded);
    assert_eq!(
        base_row(&nodes, &pk, seed),
        Some(encode_stored_item_versioned(&a, v(500, 0, REMOTE)))
    );
    assert_eq!(lsi_rows(&nodes, &pk), 1);
    assert_eq!(changes(&nodes, &pk).len(), 1);

    // Equal stamp (an idempotent re-delivery) and older stamps: Superseded, no writes.
    for stale in [v(500, 0, REMOTE), v(499, 9, REMOTE), v(500, 0, 1)] {
        let (_, r) = eval(
            &mut sim,
            &nodes,
            mrec_schema(1),
            &pk,
            replicate(Some(b.clone()), stale),
            seed,
        );
        assert!(r.superseded, "{stale:?} must lose (seed={seed})");
        assert_eq!((r.old.as_ref(), r.new.as_ref()), (Some(&a), Some(&a)));
    }
    assert_eq!(
        base_row(&nodes, &pk, seed),
        Some(encode_stored_item_versioned(&a, v(500, 0, REMOTE)))
    );
    assert_eq!(
        changes(&nodes, &pk).len(),
        1,
        "a loser writes no change record"
    );
    assert_eq!(lsi_rows(&nodes, &pk), 1);

    // A higher stamp from a lower-id region on the same wall/logical ties break by region id.
    let (_, r) = eval(
        &mut sim,
        &nodes,
        mrec_schema(1),
        &pk,
        replicate(Some(c.clone()), v(500, 0, REMOTE + 1)),
        seed,
    );
    assert!(!r.superseded);
    assert_eq!(
        base_row(&nodes, &pk, seed),
        Some(encode_stored_item_versioned(&c, v(500, 0, REMOTE + 1)))
    );
    // The LSI row followed the item (22), the old one (21 -> a's 20) is gone.
    assert_eq!(lsi_rows(&nodes, &pk), 1);
    assert_eq!(changes(&nodes, &pk).len(), 2);
}

#[test]
fn a_replicated_tombstone_keeps_its_stamp_and_blocks_a_stale_put() {
    let seed = 0x0075_0203;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let pk = s("carol");
    let live = item(&[("pk", pk.clone()), ("age", n("40"))]);

    eval(
        &mut sim,
        &nodes,
        mrec_schema(1),
        &pk,
        replicate(Some(live.clone()), v(100, 0, REMOTE)),
        seed,
    );
    eval(
        &mut sim,
        &nodes,
        mrec_schema(1),
        &pk,
        replicate(None, v(200, 0, REMOTE)),
        seed,
    );
    assert_eq!(
        base_row(&nodes, &pk, seed),
        Some(encode_tombstone_versioned(v(200, 0, REMOTE)))
    );
    assert_eq!(lsi_rows(&nodes, &pk), 0, "the delete removed the LSI row");

    // A put stamped before the delete arrives late: no resurrection.
    let (_, r) = eval(
        &mut sim,
        &nodes,
        mrec_schema(1),
        &pk,
        replicate(Some(live), v(150, 0, 5)),
        seed,
    );
    assert!(r.superseded);
    let (item_now, ver_now) =
        decode_stored_item_versioned(&base_row(&nodes, &pk, seed).unwrap()).unwrap();
    assert_eq!((item_now, ver_now), (None, Some(v(200, 0, REMOTE))));
}

#[test]
fn a_local_write_after_observing_a_remote_one_beats_it_even_with_a_slow_clock() {
    let seed = 0x0075_0204;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let pk = s("dave");
    let remote = item(&[("pk", pk.clone()), ("age", n("1"))]);
    let local = item(&[("pk", pk.clone()), ("age", n("2"))]);

    eval(
        &mut sim,
        &nodes,
        mrec_schema(1),
        &pk,
        replicate(Some(remote), v(9_000, 4, REMOTE)),
        seed,
    );
    // The local clock is far behind the remote stamp (skew): causality still wins.
    eval(
        &mut sim,
        &nodes,
        mrec_schema(10),
        &pk,
        KindEvalOp::Put(local.clone()),
        seed,
    );
    assert_eq!(
        base_row(&nodes, &pk, seed),
        Some(encode_stored_item_versioned(&local, v(9_000, 5, LOCAL)))
    );
}

#[test]
fn an_unversioned_row_compares_as_zero_and_a_non_mrec_table_is_byte_identical_to_before() {
    let seed = 0x0075_0205;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let pk = s("erin");
    let legacy = item(&[("pk", pk.clone()), ("age", n("9"))]);

    // No `mrec` context: the row is the pre-MREC, unversioned encoding.
    eval(
        &mut sim,
        &nodes,
        plain_schema(),
        &pk,
        KindEvalOp::Put(legacy.clone()),
        seed,
    );
    assert_eq!(
        base_row(&nodes, &pk, seed),
        Some(encode_stored_item(&legacy))
    );
    // A replicate on a table with no `mrec` is a deterministic rejection: nothing written.
    let l = leader(&nodes, seed);
    let ProposeResult::Accepted { index, .. } = nodes[l].propose_kind_eval(
        plain_schema(),
        pk.clone(),
        None,
        replicate(None, v(5, 0, REMOTE)),
        None,
        false,
    ) else {
        panic!("propose (seed={seed})");
    };
    sim.run_for(SETTLE);
    assert!(
        matches!(
            nodes[l].kind_batch_outcome(index).map(|(_, o)| o),
            Some(KindBatchOutcome::Rejected { .. })
        ),
        "seed={seed}"
    );
    assert_eq!(
        base_row(&nodes, &pk, seed),
        Some(encode_stored_item(&legacy))
    );

    // The table is converted to MREC: the legacy row loses to any real stamp...
    let newer = item(&[("pk", pk.clone()), ("age", n("10"))]);
    let (_, r) = eval(
        &mut sim,
        &nodes,
        mrec_schema(1),
        &pk,
        replicate(Some(newer.clone()), v(1, 0, REMOTE)),
        seed,
    );
    assert!(!r.superseded);
    // ...and a local write over an unversioned row stamps from the clock.
    let pk2 = s("frank");
    eval(
        &mut sim,
        &nodes,
        plain_schema(),
        &pk2,
        KindEvalOp::Put(legacy.clone()),
        seed,
    );
    eval(
        &mut sim,
        &nodes,
        mrec_schema(77),
        &pk2,
        KindEvalOp::Put(newer.clone()),
        seed,
    );
    assert_eq!(
        base_row(&nodes, &pk2, seed),
        Some(encode_stored_item_versioned(&newer, v(77, 0, LOCAL)))
    );
}

#[test]
fn kind_eval_batch_mixes_applied_and_superseded_and_bumps_a_same_key_duplicate() {
    let seed = 0x0075_0206;
    let (mut sim, nodes) = group(seed);
    sim.run_for(ELECT);
    let (p1, p2, p3) = (s("g1"), s("g2"), s("g3"));
    let it = |pk: &AttributeValue, age: &str| item(&[("pk", pk.clone()), ("age", n(age))]);
    // p2 already holds a high stamp.
    eval(
        &mut sim,
        &nodes,
        mrec_schema(1),
        &p2,
        replicate(Some(it(&p2, "5")), v(900, 0, REMOTE)),
        seed,
    );

    let entry = |schema: WriteSchema, pk: &AttributeValue, op: KindEvalOp| KindEvalEntry {
        schema,
        pk: pk.clone(),
        sk: None,
        op,
        condition: None,
        ttl_expired: false,
    };
    let entries = vec![
        entry(
            mrec_schema(10),
            &p1,
            replicate(Some(it(&p1, "1")), v(10, 0, REMOTE)),
        ),
        entry(
            mrec_schema(10),
            &p2,
            replicate(Some(it(&p2, "6")), v(800, 0, REMOTE)),
        ),
        // Two local writes to the same key in one entry: the second bumps from the first.
        entry(mrec_schema(10), &p3, KindEvalOp::Put(it(&p3, "7"))),
        entry(mrec_schema(10), &p3, KindEvalOp::Put(it(&p3, "8"))),
    ];
    let l = leader(&nodes, seed);
    let ProposeResult::Accepted { index, term } = nodes[l].propose_kind_eval_batch(entries) else {
        panic!("batch rejected (seed={seed})");
    };
    sim.run_for(SETTLE);
    let result = nodes[l]
        .take_kind_eval_batch_result(index, term)
        .expect("leader-local batch result");
    assert!(matches!(
        result.items[0],
        KindEvalItemResult::Applied { .. }
    ));
    assert!(matches!(
        result.items[1],
        KindEvalItemResult::Superseded { .. }
    ));
    assert!(matches!(
        result.items[2],
        KindEvalItemResult::Applied { .. }
    ));
    assert!(matches!(
        result.items[3],
        KindEvalItemResult::Applied { .. }
    ));
    assert_eq!(
        base_row(&nodes, &p1, seed),
        Some(encode_stored_item_versioned(
            &it(&p1, "1"),
            v(10, 0, REMOTE)
        ))
    );
    assert_eq!(
        base_row(&nodes, &p2, seed),
        Some(encode_stored_item_versioned(
            &it(&p2, "5"),
            v(900, 0, REMOTE)
        ))
    );
    assert_eq!(
        changes(&nodes, &p2).len(),
        1,
        "the superseded item left no record"
    );
    assert_eq!(
        base_row(&nodes, &p3, seed),
        Some(encode_stored_item_versioned(&it(&p3, "8"), v(10, 1, LOCAL)))
    );
}

// ---------------------------------------------------------------------------
// Transactions are region-local: a pending write is stamped at stage (the
// intent shields the key from a replicated apply until resolve), and a
// replicate staged in a transaction is rejected.
// ---------------------------------------------------------------------------

fn single(seed: u64) -> (Simulator, KvNode) {
    let sim = Simulator::new(seed);
    let node: KvNode = RaftKvNode::start_hosted_with_options(
        sim.env(nid(0)),
        vec![nid(0)],
        MemoryEngine::new(),
        StorageScope::new(KeyRange::whole()),
        PRIMARY_STREAM,
        mrec_options(),
    );
    (sim, node)
}

fn drive<T: Send + 'static>(
    sim: &mut Simulator,
    env: &SimEnv,
    fut: impl std::future::Future<Output = T> + Send + 'static,
) -> Option<T> {
    let slot = std::sync::Arc::new(std::sync::Mutex::new(None));
    let s = std::sync::Arc::clone(&slot);
    env.clone().spawn_task(async move {
        let v = fut.await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(Duration::from_millis(300));
    slot.lock().unwrap().take()
}

fn stage(
    sim: &mut Simulator,
    node: &KvNode,
    pk: &AttributeValue,
    op: KindEvalOp,
    wall_ms: u64,
) -> (animus_cp_data::TxnId, Vec<u8>, StageOutcome) {
    let write = TxnWrite::pending_eval(
        base_key(pk),
        None,
        PendingTxnWrite {
            schema: mrec_schema(wall_ms),
            pk: pk.clone(),
            sk: None,
            op,
            condition: None,
            ttl_expired: false,
        },
    );
    let n = node.clone();
    drive(sim, node.env(), async move {
        n.txn_stage_anchor(TABLE, vec![write], Vec::new(), Vec::new())
            .await
    })
    .flatten()
    .expect("stage completes")
}

#[test]
fn a_transactional_write_is_stamped_at_stage_and_a_replicate_meets_the_intent_with_a_retry_cue() {
    let seed = 0x0075_0207;
    let (mut sim, node) = single(seed);
    sim.run_for(ELECT);
    let pk = s("hank");
    let key = base_key(&pk);
    let old = item(&[("pk", pk.clone()), ("age", n("1"))]);
    let staged = item(&[("pk", pk.clone()), ("age", n("2"))]);
    let late = item(&[("pk", pk.clone()), ("age", n("3"))]);

    // A replicated row from the remote region already sits at (50, 2, REMOTE).
    let ProposeResult::Accepted { .. } = node.propose_kind_eval(
        mrec_schema(1),
        pk.clone(),
        None,
        replicate(Some(old), v(50, 2, REMOTE)),
        None,
        false,
    ) else {
        panic!("seed={seed}");
    };
    sim.run_for(SETTLE);

    // Stage a local transactional put with a *slow* clock: stamped from the stored stamp.
    let (txn_id, record_key, outcome) =
        stage(&mut sim, &node, &pk, KindEvalOp::Put(staged.clone()), 10);
    assert_eq!(outcome, StageOutcome::Staged, "seed={seed}");

    // While the intent is unresolved a replicate gets the entry's retry cue
    // (ConditionFailed: it carries no condition of its own) and writes nothing.
    let ProposeResult::Accepted { index, .. } = node.propose_kind_eval(
        mrec_schema(1),
        pk.clone(),
        None,
        replicate(Some(late.clone()), v(10_000, 0, REMOTE)),
        None,
        false,
    ) else {
        panic!("seed={seed}");
    };
    sim.run_for(SETTLE);
    assert!(
        matches!(
            node.kind_batch_outcome(index).map(|(_, o)| o),
            Some(KindBatchOutcome::ConditionFailed { .. })
        ),
        "seed={seed}"
    );

    // Resolve: the committed row is the staged item with the stage-time stamp.
    let n2 = node.clone();
    let (tid, rk) = (txn_id.clone(), record_key.clone());
    let commit_ts = drive(&mut sim, node.env(), async move {
        n2.txn_commit_at_least(tid.clone(), rk, tid.ts).await
    })
    .flatten()
    .expect("commit");
    let n3 = node.clone();
    let k2 = key.clone();
    drive(&mut sim, node.env(), async move {
        n3.txn_resolve(
            txn_id,
            record_key,
            vec![k2],
            TxnOutcome::Committed { commit_ts },
        )
        .await
    })
    .flatten()
    .expect("resolve");
    assert_eq!(
        block_on(node.local_get(&key)),
        Some(encode_stored_item_versioned(&staged, v(50, 3, LOCAL))),
        "seed={seed}"
    );

    // The shipper's retry now applies (and wins on its stamp).
    let ProposeResult::Accepted { index, term } = node.propose_kind_eval(
        mrec_schema(1),
        pk.clone(),
        None,
        replicate(Some(late.clone()), v(10_000, 0, REMOTE)),
        None,
        false,
    ) else {
        panic!("seed={seed}");
    };
    sim.run_for(SETTLE);
    assert!(!node.take_kind_eval_result(index, term).unwrap().superseded);
    assert_eq!(
        block_on(node.local_get(&key)),
        Some(encode_stored_item_versioned(&late, v(10_000, 0, REMOTE)))
    );
}

#[test]
fn a_replicate_staged_in_a_transaction_is_a_validation_rejection() {
    let seed = 0x0075_0208;
    let (mut sim, node) = single(seed);
    sim.run_for(ELECT);
    let pk = s("ivy");
    let (_, _, outcome) = stage(
        &mut sim,
        &node,
        &pk,
        replicate(Some(item(&[("pk", pk.clone())])), v(5, 0, REMOTE)),
        10,
    );
    assert!(
        matches!(outcome, StageOutcome::Rejected { .. }),
        "got {outcome:?} (seed={seed})"
    );
    assert_eq!(block_on(node.local_get(&base_key(&pk))), None);
}
