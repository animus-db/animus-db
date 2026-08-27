//! **`RaftKvNode::txn_record_view`'s "served" contract** (ADR 0018 §2, issue
//! #298 shape B fix): outer `None` means "not served" (this replica's own
//! read barrier failed — a deposed/partitioned leader, or an election in
//! progress), `Some(None)` means the answering leader's own barrier
//! **definitively confirmed no record exists** at this key, and
//! `Some(Some(view))` means found. Before this fix the method returned a
//! plain `Option<TxnRecordView>`, conflating "not served" and "genuinely no
//! record" into the same bare `None` — `animusd::ClientCtx::txn_recover`'s
//! orphan-record recovery path read that `None` as license to synthesize an
//! abort tombstone, which is only sound for a *definitive* absence, never
//! for "I couldn't tell right now" (exactly what a leadership change or a
//! concurrent in-place split produces routinely). See
//! `docs/engineering-lessons.md`'s issue #298 shape B entry for the full
//! incident (captured live via a `SplitMode::InPlace` soak: an acked write
//! permanently lost across a split cascade).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_cp_data::{RaftKvNode, StageOutcome, TxnDecisionStatus, TxnId, TxnWrite};
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

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

/// Drive `node.txn_record_view(key)` to completion (spawned, since it awaits
/// a read-barrier probe round).
fn view(
    sim: &mut Simulator,
    node: &KvNode,
    key: &[u8],
    budget: Duration,
) -> Option<Option<animus_cp_data::TxnRecordView>> {
    let slot: Arc<Mutex<Option<Option<Option<animus_cp_data::TxnRecordView>>>>> =
        Arc::new(Mutex::new(None));
    let n = node.clone();
    let s = Arc::clone(&slot);
    let k = key.to_vec();
    node.env().clone().spawn_task(async move {
        let v = n.txn_record_view(&k).await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock().unwrap().take().flatten()
}

fn key(token: u8, tail: &[u8]) -> Vec<u8> {
    let mut k = vec![token; animus_tablet::TOKEN_BYTES];
    k.extend_from_slice(tail);
    k
}

type AnchorStageResult = Option<(TxnId, Vec<u8>, StageOutcome)>;

/// `Some(None)`: a real leader's own confirmed read barrier, over a key that
/// genuinely has no transaction record.
#[test]
fn a_genuinely_absent_record_is_a_confirmed_none_not_a_barrier_failure() {
    let seed = 0xB58E_0001u64;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    let never_used = key(1, b":never-staged");
    let result = view(&mut sim, &nodes[l], &never_used, Duration::from_secs(1));
    assert_eq!(
        result,
        Some(None),
        "a live leader's own confirmed barrier over a genuinely unused key must report \
         Some(None) — definitively absent, never the outer None a caller could mistake for \
         'not served' (seed={seed})"
    );
}

/// `Some(Some(view))`: a real, freshly staged record is found.
#[test]
fn a_staged_record_is_found() {
    let seed = 0xB58E_0002u64;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));
    let l = leader(&nodes, &[0, 1, 2], seed);

    let anchor_key = key(1, b":order");
    let n = nodes[l].clone();
    let ak = anchor_key.clone();
    let slot: Arc<Mutex<Option<AnchorStageResult>>> = Arc::new(Mutex::new(None));
    let s = Arc::clone(&slot);
    nodes[l].env().clone().spawn_task(async move {
        let v = n
            .txn_stage_anchor(
                "orders",
                vec![TxnWrite::plain(ak, Some(b"placed".to_vec()))],
                Vec::new(),
                Vec::new(),
            )
            .await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(Duration::from_secs(1));
    let (_txn_id, record_key, outcome) = slot
        .lock()
        .unwrap()
        .take()
        .flatten()
        .unwrap_or_else(|| panic!("anchor stage should succeed (seed={seed})"));
    assert_eq!(outcome, StageOutcome::Staged, "seed={seed}");

    let result = view(&mut sim, &nodes[l], &record_key, Duration::from_secs(1));
    let found = result
        .unwrap_or_else(|| panic!("the leader's own barrier must be served (seed={seed})"))
        .unwrap_or_else(|| panic!("the just-staged record must be found (seed={seed})"));
    assert_eq!(found.status, TxnDecisionStatus::Pending, "seed={seed}");
}

/// Outer `None`: a deposed (partitioned) leader's own read barrier fails —
/// this must NEVER be reported the same way as `Some(None)` (definitively
/// absent), which is exactly the pre-fix conflation issue #298 shape B's
/// `txn_recover` orphan path relied on.
#[test]
fn a_deposed_leaders_barrier_failure_is_the_outer_none_never_confused_with_absence() {
    let seed = 0xB58E_0003u64;
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2));

    let old = leader(&nodes, &[0, 1, 2], seed);
    let survivors: Vec<usize> = (0..3).filter(|&i| i != old).collect();
    for &s in &survivors {
        sim.partition_pair(nid(old as u64), nid(s as u64));
    }
    // Let the survivors elect a new leader while the old one sits isolated,
    // still believing it leads its own (stale) term.
    sim.run_for(Duration::from_secs(3));

    let never_used = key(1, b":never-staged");
    let result = view(&mut sim, &nodes[old], &never_used, Duration::from_secs(7));
    assert_eq!(
        result, None,
        "a deposed leader's own read barrier must fail — reported as the OUTER None (not \
         served), never `Some(None)` (which would claim a confirmed, definitive absence this \
         isolated replica has no authority to confirm) (seed={seed})"
    );
}
