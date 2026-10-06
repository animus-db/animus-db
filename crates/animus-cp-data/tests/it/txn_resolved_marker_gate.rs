//! ADR 0073 class G: the `txn-resolved-marker` row (issue #1243) must not
//! cross to a node that cannot read it while its introducing gate
//! (`Gate::GlobalTables`) is closed. A previous-release replica's scan and
//! `has_data` filters know only record keys and would surface a marker row
//! (`token || 0x00 0x04 || key`) to clients, so `engine_image` omits markers
//! while the gate is closed and ships them once it is open. Apply writes
//! markers unconditionally, so the sender's own engine always holds one.
//!
//! A follower is partitioned past the log-retention cap, so it can only catch
//! up by `InstallSnapshot` (asserted via `CpSnapshotInstalls`); a transaction
//! is staged and committed while it is away.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::Metadata;
use animus_control::version::ClusterFeatures;
use animus_cp_data::{HostedOptions, KIND_BASE, RaftKvNode, StorageScope};
use animus_env::{Env, EnvExt, Metric, PRIMARY_STREAM, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{escape, partition_token};
use futures::executor::block_on;

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

/// The marker's physical-scope row key: `token || [0x00, 0x04] || key`.
fn marker_logical_key(k: &[u8]) -> Vec<u8> {
    let mut out = k[..8].to_vec();
    out.extend_from_slice(&[0x00, 0x04]);
    out.extend_from_slice(k);
    out
}

/// Returns `(leader holds marker, follower holds marker)`.
fn markers_after_snapshot(seed: u64, cluster_version: u32) -> (bool, bool) {
    let mut sim = Simulator::new(seed);
    let nodes: Vec<MemNode> = NODES
        .iter()
        .map(|&id| {
            let features = ClusterFeatures::new();
            features.update(&Metadata {
                cluster_version,
                ..Metadata::default()
            });
            RaftKvNode::start_hosted_with_options(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
                StorageScope::whole(),
                PRIMARY_STREAM,
                HostedOptions {
                    features,
                    ..HostedOptions::default()
                },
            )
        })
        .collect();
    sim.run_for(ELECT);
    let l = nodes
        .iter()
        .position(MemNode::is_leader)
        .unwrap_or_else(|| panic!("no leader elected (seed={seed})"));
    let lagging = (0..3).find(|&i| i != l).expect("a follower exists");
    sim.partition_pair(nid(NODES[l]), nid(NODES[lagging]));

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

    for round in 0..92u64 {
        for j in 0..50u64 {
            let _ = nodes[l].put(format!("k-{round}-{j}").into_bytes(), b"v".to_vec());
        }
        sim.run_for(Duration::from_millis(5));
    }
    sim.run_for(Duration::from_secs(3));
    sim.heal(nid(NODES[l]), nid(NODES[lagging]));
    sim.heal(nid(NODES[lagging]), nid(NODES[l]));
    let metrics = sim.env(nid(NODES[lagging])).metrics();
    let mut installed = false;
    for _ in 0..600 {
        sim.run_for(Duration::from_millis(50));
        if metrics.get(Metric::CpSnapshotInstalls) > 0
            && nodes[lagging].engine_applied_index() >= nodes[l].engine_applied_index()
        {
            installed = true;
            break;
        }
    }
    assert!(
        installed,
        "precondition: follower {lagging} must catch up via InstallSnapshot (seed={seed})"
    );
    let mk = marker_logical_key(&k);
    let has = |n: &MemNode| {
        block_on(n.storage().get(&n.physical_key(KIND_BASE, &mk)))
            .expect("engine read ok")
            .is_some()
    };
    // The committed value itself always crosses, gate or no gate.
    assert_eq!(
        block_on(nodes[lagging].local_get(&k)),
        Some(b"staged".to_vec()),
        "the resolved value must reach the follower (seed={seed})"
    );
    (has(&nodes[l]), has(&nodes[lagging]))
}

fn seeds(base: u64) -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
    {
        return vec![s];
    }
    let k = std::env::var("ANIMUS_UPGRADE_SEEDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1)
        .max(1);
    (0..k).map(|i| base + i).collect()
}

/// Gate closed (cluster version 1: a previous-release replica may be a
/// member): the image carries no marker; the sender's own engine still does.
#[test]
fn snapshot_omits_resolved_markers_while_the_gate_is_closed() {
    for seed in seeds(0x1243_0001) {
        assert_eq!(
            markers_after_snapshot(seed, 1),
            (true, false),
            "leader keeps its marker, an N-1 follower must never receive one (seed={seed})"
        );
    }
}

/// Gate open: the image carries the marker, so the follower's guard is
/// identical to the sender's.
#[test]
fn snapshot_ships_resolved_markers_once_the_gate_is_open() {
    for seed in seeds(0x1243_1001) {
        assert_eq!(
            markers_after_snapshot(seed, 2),
            (true, true),
            "a finalized cluster ships the marker (seed={seed})"
        );
    }
}
