//! ADR 0073 class G: the `txn-envelope` v2 intent (tag 2) must not cross to
//! a node that cannot read it (ADR 0073's 2026-10-05 `txn-envelope`
//! amendment; GitHub issue #1237).
//!
//! `efcaa6cb` made `TxnStage`'s apply write v2 intents (the committed value
//! the key held, carried inside the intent). Engine values cross nodes in an
//! `InstallSnapshot` image (`engine_image`), and a previous-release replica
//! panics on tag 2 (`txn: unknown envelope tag 2`). The sender therefore
//! down-converts every v2 intent in the image to v1 while
//! `Gate::GlobalTables` (cluster version 2, the release that introduces v2) is
//! closed, and ships v2 once it is open. Apply never branches on the gate: the
//! sender's own engine keeps v2 either way.
//!
//! Both tests partition one follower past the log-retention cap, so it can only
//! catch up by `InstallSnapshot` (asserted via `CpSnapshotInstalls`), stage a
//! transaction while it is away, and inspect the raw envelope tag the follower
//! holds after the image is installed.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::Metadata;
use animus_control::ProposeResult;
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

/// The envelope tags (`txn-envelope`, every version).
const TAG_INTENT_V1: u8 = 1;
const TAG_INTENT_V2: u8 = 2;

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

/// Run the snapshot-catch-up scenario with every node's feature handle at
/// `cluster_version`, and return the envelope tag the snapshot-caught-up
/// follower holds for the staged key.
fn follower_intent_tag_after_snapshot(
    seed: u64,
    cluster_version: u32,
    staged: Option<Vec<u8>>,
) -> u8 {
    let mut sim = Simulator::new(seed);
    let nodes: Vec<MemNode> = NODES
        .iter()
        .map(|&id| {
            let features = ClusterFeatures::new();
            let meta = Metadata {
                cluster_version,
                ..Metadata::default()
            };
            features.update(&meta);
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

    // The lagging follower never sees anything through its log.
    sim.partition_pair(nid(NODES[l]), nid(NODES[lagging]));

    let k = key(b"acct", b"balance");
    assert!(matches!(
        nodes[l].put(k.clone(), b"acked-committed".to_vec()),
        ProposeResult::Accepted { .. }
    ));
    sim.run_for(SETTLE);
    let n = nodes[l].clone();
    let kk = k.clone();
    let (txn_id, record_key, _) = drive(&mut sim, nodes[l].env(), SETTLE, async move {
        n.txn_stage("t", vec![(kk, staged)]).await
    })
    .flatten()
    .unwrap_or_else(|| panic!("txn_stage did not complete (seed={seed})"));

    // Well past the retention cap (4096 entries): only a snapshot can catch it up.
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

    // The leader's own engine is untouched by the gate: apply writes v2 always.
    let leader_raw = block_on(
        nodes[l]
            .storage()
            .get(&nodes[l].physical_key(KIND_BASE, &k)),
    )
    .expect("engine read ok")
    .expect("leader holds the staged intent");
    assert_eq!(
        leader_raw.value[0], TAG_INTENT_V2,
        "the sender's local engine keeps v2 whatever the gate says (seed={seed})"
    );

    let raw = block_on(
        nodes[lagging]
            .storage()
            .get(&nodes[lagging].physical_key(KIND_BASE, &k)),
    )
    .expect("engine read ok")
    .unwrap_or_else(|| panic!("follower {lagging} missing the staged intent (seed={seed})"));
    let tag = raw.value[0];

    // Whichever envelope the image carried, an abort must restore the
    // committed value on every replica, the snapshot-caught-up follower
    // included (a v1 image ships the prior one MVCC version below the
    // intent, where the v1 lookback reads it).
    let n = nodes[l].clone();
    let kk = k.clone();
    assert!(
        drive(&mut sim, nodes[l].env(), SETTLE, async move {
            n.txn_decide(txn_id, record_key, vec![kk], false).await
        })
        .flatten()
        .is_some(),
        "abort must complete (seed={seed})"
    );
    sim.run_for(Duration::from_secs(3));
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(
            block_on(n.local_get(&k)),
            Some(b"acked-committed".to_vec()),
            "node {i} (cluster version {cluster_version}): the abort must restore the committed \
             value (seed={seed})"
        );
    }
    tag
}

/// The corpus depth: `ANIMUS_UPGRADE_SEEDS` seeds per cell (the same knob as the
/// control-plane and cluster tiers of the mixed-version corpus); `ANIMUS_SEED`
/// replays one.
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

/// The staged-write shapes: a put and a delete over a committed value.
fn staged_shapes() -> [(&'static str, Option<Vec<u8>>); 2] {
    [("put", Some(b"staged".to_vec())), ("delete", None)]
}

/// The #1237 regression: with the gate closed (cluster version 1: a
/// previous-release replica may be a member) the shipped image carries a v1
/// intent, never tag 2, and the abort still restores the committed value.
/// Failed before the gate: the follower held tag 2.
#[test]
fn snapshot_ships_v1_intents_while_the_gate_is_closed() {
    for (shape, staged) in staged_shapes() {
        for seed in seeds(0x1237_0001) {
            assert_eq!(
                follower_intent_tag_after_snapshot(seed, 1, staged.clone()),
                TAG_INTENT_V1,
                "an N-1 replica cannot read tag 2: the image must carry the {shape} intent as \
                 v1 (seed={seed})"
            );
        }
    }
}

/// Once the cluster finalized to the version that introduces v2, the image
/// ships v2 unchanged (the prior value survives the snapshot).
#[test]
fn snapshot_ships_v2_intents_once_the_gate_is_open() {
    for (shape, staged) in staged_shapes() {
        for seed in seeds(0x1237_1001) {
            assert_eq!(
                follower_intent_tag_after_snapshot(seed, 2, staged.clone()),
                TAG_INTENT_V2,
                "a finalized cluster ships the {shape} intent as v2 as is (seed={seed})"
            );
        }
    }
}
