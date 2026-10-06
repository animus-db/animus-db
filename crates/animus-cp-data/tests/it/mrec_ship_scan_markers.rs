//! The MREC shipper's scan window (`RaftKvNode::local_scan_for_ship`) must skip
//! every txn-internal bookkeeping row, not just records: a committed
//! transaction leaves a `txn-resolved-marker` row (value leads `0xA1`) in the
//! engine, and decoding it as a value envelope panicked ("unknown envelope
//! tag 161"). Found by CI merging main into the G-d branch (every
//! `sim_world_mrec_corpus` cell failed at the default seed).

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_cp_data::{RaftKvNode, StorageScope};
use animus_env::{EnvExt, PRIMARY_STREAM, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{escape, partition_token};

const NODES: [u64; 3] = [0, 1, 2];
const ELECT: Duration = Duration::from_secs(2);
const SETTLE: Duration = Duration::from_secs(2);

type MemNode = RaftKvNode<SimEnv, MemoryEngine>;

fn key(pk: &[u8], rk: &[u8]) -> Vec<u8> {
    let mut out = partition_token(pk).to_vec();
    out.extend_from_slice(&escape(pk));
    out.extend_from_slice(rk);
    out
}

fn drive<T: Send + 'static>(
    sim: &mut Simulator,
    env: &SimEnv,
    budget: Duration,
    fut: impl Future<Output = T> + Send + 'static,
) -> Option<T> {
    let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let s = Arc::clone(&slot);
    env.clone().spawn_task(async move {
        let v = fut.await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock().unwrap().take()
}

#[test]
fn ship_scan_skips_resolved_markers() {
    for seed in [0x4D52_0000u64, 0x4D52_0001, 0x4D52_0002] {
        let mut sim = Simulator::new(seed);
        let nodes: Vec<MemNode> = NODES
            .iter()
            .map(|&id| {
                RaftKvNode::start_hosted(
                    sim.env(nid(id)),
                    NODES.iter().copied().map(nid).collect(),
                    MemoryEngine::new(),
                    StorageScope::whole(),
                    PRIMARY_STREAM,
                )
            })
            .collect();
        sim.run_for(ELECT);
        let l = nodes
            .iter()
            .position(MemNode::is_leader)
            .unwrap_or_else(|| panic!("no leader elected (seed={seed})"));
        let k = key(b"acct", b"balance");
        let n = nodes[l].clone();
        let kk = k.clone();
        let (txn_id, record_key, _) = drive(&mut sim, nodes[l].env(), SETTLE, async move {
            n.txn_stage("t", vec![(kk, Some(b"staged".to_vec()))]).await
        })
        .flatten()
        .unwrap_or_else(|| panic!("txn_stage did not complete (seed={seed})"));
        let n = nodes[l].clone();
        let kk = k.clone();
        assert!(
            drive(&mut sim, nodes[l].env(), SETTLE, async move {
                n.txn_decide(txn_id, record_key, vec![kk], true).await
            })
            .flatten()
            .is_some(),
            "commit must complete (seed={seed})"
        );
        sim.run_for(SETTLE);
        let n = nodes[l].clone();
        let (rows, resume) = drive(&mut sim, nodes[l].env(), SETTLE, async move {
            n.local_scan_for_ship(&[], 1000).await
        })
        .unwrap_or_else(|| panic!("scan did not complete (seed={seed})"));
        assert_eq!(resume, None, "seed={seed}");
        assert_eq!(rows, vec![(k, b"staged".to_vec())], "seed={seed}");
    }
}
