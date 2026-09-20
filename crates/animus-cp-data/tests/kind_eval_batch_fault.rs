//! Fault-injection coverage for `KvCommand::KindEvalBatch` (issue #996,
//! layer 1): the batched sibling of `KvCommand::KindEval` (ADR 0054 step
//! 2/3), one Raft entry carrying `N` independent evaluate-at-apply item
//! writes for the same tablet.
//!
//! `crates/animus-cp-data/tests/kind_eval.rs` already proves the apply-time
//! evaluation/overlay/outcome-split logic against a quiet, single-leader
//! group; this file proves the SAME entry survives the distributed failure
//! modes the house convention requires ("every distributed behavior lands
//! with a fault-injecting simulation test that is reproducible from a
//! seed") — a leader-kill truncation (mirroring `tests/
//! kind_batch_outcome_identity.rs`'s own scenario, applied to the batched
//! grain's term-identity discipline), a follower kill mid-commit
//! (linearizability: every surviving replica converges to byte-identical
//! state), and a genuine process crash and restart (WAL replay of a
//! multi-item entry, mirroring `tests/kind_eval.rs`'s own single-item
//! `a_kind_eval_survives_crash_restart_idempotently`).
//!
//! **Not part of `raftkv_linearizable.rs`'s own `ANIMUS_RAFTKV_SEEDS`
//! corpus** — that corpus drives a generic single-key list-append workload
//! (`put`/`delete`/`linearizable_get` only, plus a poison-only `Cas`) with no
//! per-command-type cells to extend; it never proposes `KindEval`/
//! `KindBatch` at all (confirmed by grep). The house's own existing
//! precedent for fault-injecting a *specific* `KvCommand` variant's outcome/
//! term-identity discipline is `tests/kind_batch_outcome_identity.rs`'s
//! self-contained, fixed-seed scenario style, which this file follows
//! instead of forcing a mismatched fit into the list-append corpus.
//!
//! Deterministic and seed-reproducible (ADR 0003): drive with `run_for`,
//! never `run()` (the driver has perpetual heartbeat/election timers).

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::{
    KIND_BASE, KindBatchOutcome, KindEvalBatchResult, KindEvalEntry, KindEvalItemResult,
    KindEvalOp, RaftKvNode,
};
use animus_env::nid;
use animus_item::{AttributeValue, Item, TableSchema, WriteSchema, decode_stored_item};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::partition_token;
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];

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

fn leader(nodes: &[KvNode], live: &[usize], seed: u64) -> usize {
    let ls: Vec<usize> = live
        .iter()
        .copied()
        .filter(|&i| nodes[i].is_leader())
        .collect();
    assert_eq!(
        ls.len(),
        1,
        "expected one leader among {live:?}, got {ls:?} (seed={seed})"
    );
    ls[0]
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_owned())
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

fn schema_no_index() -> WriteSchema {
    WriteSchema {
        key: TableSchema::simple("pk"),
        lsis: Vec::new(),
        change_records_carry_images: false,
    }
}

fn entry(pk: AttributeValue) -> KindEvalEntry {
    KindEvalEntry {
        schema: schema_no_index(),
        pk: pk.clone(),
        sk: None,
        op: KindEvalOp::Put(item(&[("pk", pk)])),
        condition: None,
        ttl_expired: false,
    }
}

// ---------------------------------------------------------------------------
// (1) Leader-kill truncation: the batched sibling of `tests/
//     kind_batch_outcome_identity.rs`'s own regression — a `KindEvalBatch`
//     entry accepted (appended locally) but never committed, whose log
//     position is reoccupied by a DIFFERENT entry after a leadership
//     change, must never be falsely confirmed by an index-alone check.
// ---------------------------------------------------------------------------

#[test]
fn a_truncated_kind_eval_batch_entry_is_not_falsely_confirmed_by_a_reoccupying_entry() {
    let seed = 0x0996_1001;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect

    let old = leader(&nodes, &[0, 1, 2], seed);
    let survivors: Vec<usize> = (0..3).filter(|&i| i != old).collect();

    // Isolate the leader from both survivors — it keeps accepting proposals
    // locally (no leader-lease step-down in this core) but can never again
    // reach a majority to commit them.
    for &sv in &survivors {
        sim.partition_pair(nid(old as u64), nid(sv as u64));
    }

    // A filler entry to occupy a log slot, then the batch under test —
    // three items, never destined to commit on this node.
    let filler = nodes[old].propose_kind_eval_batch(vec![entry(s("filler"))]);
    assert!(
        matches!(filler, ProposeResult::Accepted { .. }),
        "isolated leader must still accept locally (seed={seed}): {filler:?}"
    );
    let mine = vec![entry(s("mine-a")), entry(s("mine-b")), entry(s("mine-c"))];
    let (accepted_index, accepted_term) = match nodes[old].propose_kind_eval_batch(mine) {
        ProposeResult::Accepted { index, term } => (index, term),
        other => panic!("isolated leader must still accept locally (seed={seed}): {other:?}"),
    };

    // The survivors elect a new leader — its election no-op occupies the
    // identical slot the isolated leader's own filler did.
    sim.run_for(Duration::from_secs(3));
    let new = leader(&nodes, &survivors, seed);
    assert_ne!(new, old, "the new leader must not be the isolated node");

    // The new leader's own distinct batch lands at the identical index the
    // isolated leader's own batch did — the collision this test forces.
    let theirs = vec![entry(s("theirs-a")), entry(s("theirs-b"))];
    let (theirs_index, theirs_term) = match nodes[new].propose_kind_eval_batch(theirs) {
        ProposeResult::Accepted { index, term } => (index, term),
        other => panic!("new leader rejected its own batch (seed={seed}): {other:?}"),
    };
    assert_eq!(
        theirs_index, accepted_index,
        "the collision this test needs didn't happen — indices differ (seed={seed})"
    );
    assert_ne!(
        theirs_term, accepted_term,
        "the collision needs distinct terms, or it doesn't exercise the bug (seed={seed})"
    );
    sim.run_for(Duration::from_secs(2)); // survivors commit + apply it

    // Heal: the old leader rejoins, discovers a higher term, steps down, and
    // log-matching truncates its uncommitted tail.
    for &sv in &survivors {
        sim.heal(nid(old as u64), nid(sv as u64));
    }
    sim.run_for(Duration::from_secs(3));

    for (i, node) in nodes.iter().enumerate() {
        let recorded = node.kind_batch_outcome(accepted_index);
        assert_eq!(
            recorded,
            Some((theirs_term, KindBatchOutcome::Applied)),
            "node {i}: the reoccupying entry's own outcome (seed={seed})"
        );

        // The term-gated confirm: the isolated leader's own batch is never
        // falsely confirmed at this index.
        assert_eq!(
            node.take_kind_eval_batch_result(accepted_index, accepted_term),
            None,
            "node {i}: a term mismatch must never surface the ORIGINAL proposer's own \
             per-item payload — this is the false-ack the (index, term) discipline \
             prevents (seed={seed})"
        );

        // None of the truncated batch's three items ever landed anywhere.
        for pk in ["mine-a", "mine-b", "mine-c"] {
            assert_eq!(
                block_on(node.local_get_kind(KIND_BASE, &base_key(&s(pk)))),
                None,
                "node {i}: the truncated batch's own item {pk} must never appear \
                 anywhere (seed={seed})"
            );
        }
        // The winning batch's own items did land.
        for pk in ["theirs-a", "theirs-b"] {
            assert!(
                block_on(node.local_get_kind(KIND_BASE, &base_key(&s(pk)))).is_some(),
                "node {i}: the committed batch's own item {pk} must have landed (seed={seed})"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// (2) Follower-kill mid-commit: a `KindEvalBatch` entry proposed while one
//     replica is briefly unreachable must still commit and, once every
//     replica is caught up, every one of them agrees byte-for-byte —
//     linearizability of the batched entry across a real fault.
// ---------------------------------------------------------------------------

#[test]
fn a_kind_eval_batch_survives_follower_kill_mid_commit_and_converges() {
    let seed = 0x0996_1002;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect
    let l = leader(&nodes, &[0, 1, 2], seed);
    let follower = (0..3).find(|&i| i != l).expect("a non-leader exists");

    // Isolate one follower right before proposing — it will need to catch
    // up on this entry entirely from replication/recovery after healing.
    let other = (0..3)
        .find(|&i| i != l && i != follower)
        .expect("third node");
    sim.partition_pair(nid(follower as u64), nid(l as u64));
    sim.partition_pair(nid(follower as u64), nid(other as u64));

    let pks = ["ann", "bea", "cleo", "dot"];
    let entries: Vec<KindEvalEntry> = pks.iter().map(|p| entry(s(p))).collect();
    let (index, term) = match nodes[l].propose_kind_eval_batch(entries) {
        ProposeResult::Accepted { index, term } => (index, term),
        other => panic!("leader rejected the batch: {other:?} (seed={seed})"),
    };
    // Committing needs only the leader + the other live follower — a
    // majority of 3 without the isolated one.
    sim.run_for(Duration::from_secs(2));
    match nodes[l].kind_batch_outcome(index) {
        Some((t, KindBatchOutcome::Applied)) => {
            assert_eq!(t, term, "seed={seed}");
        }
        other => panic!(
            "the batch must commit on a live majority even with one follower down: {other:?} \
             (seed={seed})"
        ),
    }

    // Heal and let the isolated follower catch up via ordinary replication.
    sim.heal(nid(follower as u64), nid(l as u64));
    sim.heal(nid(follower as u64), nid(other as u64));
    sim.run_for(Duration::from_secs(3));

    // Every replica — including the one that was down for the whole commit
    // — must now agree on every one of the batch's items, byte-for-byte.
    for pk in pks {
        let key = base_key(&s(pk));
        let expected = block_on(nodes[l].local_get_kind(KIND_BASE, &key))
            .and_then(|b| decode_stored_item(&b).expect("decodes"))
            .unwrap_or_else(|| panic!("leader must have item {pk} (seed={seed})"));
        for (i, node) in nodes.iter().enumerate() {
            let got = block_on(node.local_get_kind(KIND_BASE, &key))
                .and_then(|b| decode_stored_item(&b).expect("decodes"));
            assert_eq!(
                got,
                Some(expected.clone()),
                "node {i}: item {pk} must converge to the leader's own value, including \
                 the follower that was down for the whole commit (seed={seed})"
            );
        }
    }

    // The recovered follower's own per-item result channel was never
    // registered by it (it never proposed this entry) — confirms the
    // leader-local scoping holds under a real recovery path too.
    assert_eq!(
        nodes[follower].take_kind_eval_batch_result(index, term),
        None,
        "node {follower}: never proposed this entry, so it must never see its payload \
         (seed={seed})"
    );
    let result: KindEvalBatchResult = nodes[l]
        .take_kind_eval_batch_result(index, term)
        .unwrap_or_else(|| panic!("the proposer must still see its own payload (seed={seed})"));
    assert_eq!(result.items.len(), pks.len(), "seed={seed}");
    for (i, r) in result.items.iter().enumerate() {
        match r {
            KindEvalItemResult::Applied { old, new } => {
                assert_eq!(*old, None, "item {i} never existed before (seed={seed})");
                assert!(
                    new.is_some(),
                    "item {i} must carry its own new image (seed={seed})"
                );
            }
            other => panic!("item {i} unexpected: {other:?} (seed={seed})"),
        }
    }
}

// ---------------------------------------------------------------------------
// (3) A genuine process crash + restart replays a multi-item entry from the
//     WAL idempotently — mirroring `tests/kind_eval.rs`'s own single-item
//     `a_kind_eval_survives_crash_restart_idempotently`.
// ---------------------------------------------------------------------------

#[test]
fn a_kind_eval_batch_survives_crash_restart_idempotently() {
    let seed = 0x0996_1003;
    let mut sim = Simulator::new(seed);
    let engine = MemoryEngine::new();
    let id = nid(0);

    let node: KvNode = RaftKvNode::start(sim.env(id.clone()), vec![id.clone()], engine.clone());
    sim.run_for(Duration::from_secs(2)); // elect (single node, near-instant)

    let pks = ["greg", "hana", "ivan"];
    let entries: Vec<KindEvalEntry> = pks.iter().map(|p| entry(s(p))).collect();
    match node.propose_kind_eval_batch(entries) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("batch rejected: {other:?} (seed={seed})"),
    }
    sim.run_for(Duration::from_secs(2));

    for pk in pks {
        assert!(
            block_on(node.local_get_kind(KIND_BASE, &base_key(&s(pk)))).is_some(),
            "item {pk} must have applied before the crash (seed={seed})"
        );
    }

    // A genuine process restart — the WAL survives on the same engine; a
    // fresh `RaftKvNode::start` replays it from scratch, re-applying the
    // whole `KindEvalBatch` entry exactly as it first applied.
    sim.stop(id.clone());
    let restarted: KvNode = RaftKvNode::start(sim.env(id.clone()), vec![id.clone()], engine);
    sim.run_for(Duration::from_secs(2));

    for pk in pks {
        let key = base_key(&s(pk));
        assert_eq!(
            block_on(restarted.local_get_kind(KIND_BASE, &key))
                .and_then(|b| decode_stored_item(&b).expect("decodes")),
            Some(item(&[("pk", s(pk))])),
            "WAL replay of a KindEvalBatch entry must re-derive item {pk} identically \
             (seed={seed})"
        );
    }
}
