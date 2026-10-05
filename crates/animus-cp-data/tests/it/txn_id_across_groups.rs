//! R-01 F-2 (third mechanism): a `TxnId` is `(ts, node)`, and `ts` is minted
//! from the **tablet group's own** `Hlc`. `TxnId`'s doc named the node as the
//! tiebreak "if two different groups' leaders mint the identical pair" -- but
//! a node routinely leads several tablets, so two transactions anchored on
//! two tablets led by the same node could get the very same `TxnId`. The
//! chaos run that found it showed txn A (anchor on tablet 3) and txn B (anchor
//! on tablet 2) both `TxnId { ts: (9180, 18), node: n0 }`: A's participant
//! resolve then landed on B's freshly staged intent as "the same transaction"
//! and committed it before B had decided, losing one half of B.
//!
//! Two single-node groups on the same node (distinct streams = distinct
//! tablets) stage an anchor at the same virtual instant, so both `Hlc`s mint
//! the identical `(wall_ms, logical)`; their `TxnId`s must still differ.
//! Deterministic (ADR 0003): driven with `run_for`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_cp_data::{RaftKvNode, StorageScope, TxnId};
use animus_env::{EnvExt, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{escape, partition_token};

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn key(pk: &[u8]) -> Vec<u8> {
    let mut out = partition_token(pk).to_vec();
    out.extend_from_slice(&escape(pk));
    out.extend_from_slice(b"row");
    out
}

fn group(sim: &Simulator, stream: u64) -> KvNode {
    RaftKvNode::start_hosted(
        sim.env(nid(0)),
        vec![nid(0)],
        MemoryEngine::new(),
        StorageScope::whole(),
        stream,
    )
}

fn stage_all_at_once(seed: u64) {
    let mut sim = Simulator::new(seed);
    let a = group(&sim, 100);
    let b = group(&sim, 200);
    sim.run_for(Duration::from_secs(2));
    assert!(a.is_leader() && b.is_leader(), "seed={seed}: both elect");

    let ids: Arc<Mutex<Vec<TxnId>>> = Arc::new(Mutex::new(Vec::new()));
    for (n, pk) in [(a.clone(), &b"acct-a"[..]), (b.clone(), &b"acct-b"[..])] {
        let ids = Arc::clone(&ids);
        let k = key(pk);
        let env = n.env().clone();
        env.spawn_task(async move {
            let (txn_id, _record_key, _outcome) = n
                .txn_stage("t", vec![(k, Some(b"v".to_vec()))])
                .await
                .expect("stage completes");
            ids.lock().unwrap().push(txn_id);
        });
    }
    sim.run_for(Duration::from_secs(2));
    let ids = ids.lock().unwrap();
    assert_eq!(ids.len(), 2, "seed={seed}: both stages completed");
    assert_ne!(
        ids[0], ids[1],
        "seed={seed}: two transactions anchored on two groups led by the same node must \
         never share a TxnId (a resolve for one would act on the other's intent)"
    );
}

#[test]
fn two_groups_led_by_one_node_never_mint_the_same_txn_id() {
    stage_all_at_once(0x7A_0001);
}

#[test]
fn two_groups_led_by_one_node_never_mint_the_same_txn_id_over_seeds() {
    for i in 0..8 {
        stage_all_at_once(0x7A_1000 + i);
    }
}
