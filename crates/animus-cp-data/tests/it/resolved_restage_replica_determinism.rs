//! A duplicate/stale `TxnStage` that reaches a group's log AFTER its own
//! transaction's `TxnResolve` must be a no-op on **every** replica, whether
//! or not that replica restarted in between.
//!
//! The `TxnTracker::recently_resolved` seatbelt (issue #298 shape A) used to
//! be the only thing rejecting such a stage, and it is in-memory and not
//! rebuilt at restart: a replica that restarted between the resolve and the
//! stale stage re-staged the already-resolved transaction (resurrecting an
//! `Intent`), while its peers rejected it — a replica-divergent apply
//! outcome from the same log entry, which the apply-time read-modify-write
//! arms (`KindEval`, pending-eval stages) then propagate into permanent
//! value divergence (observed as a `ConsistentRead: false` read missing
//! acked appends in the chaos smoke).
//!
//! Two replica-divergence shapes are covered, each over several seeds
//! (`ANIMUS_RESTAGE_SEEDS=K`, default 2): a replica that **restarted**
//! after the resolve, and a replica that missed the resolve entirely and
//! caught up through an **`InstallSnapshot`** (it never applied the
//! resolve, so no in-process memory of it can exist there).
//!
//! Deterministic and seed-reproducible (ADR 0003).

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::Metadata;
use animus_control::version::ClusterFeatures;
use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::{HostedOptions, RaftKvNode, StorageScope, TxnId, TxnOutcome, TxnWrite};
use animus_env::{EnvExt, PRIMARY_STREAM, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{escape, partition_token};
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
const ELECT: Duration = Duration::from_secs(2);
const SETTLE: Duration = Duration::from_secs(2);
const BASE_SEED: u64 = 0x1DE_0A11;

fn seeds() -> Vec<u64> {
    let k = std::env::var("ANIMUS_RESTAGE_SEEDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(2)
        .max(1);
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        return vec![s];
    }
    (0..k).map(|i| BASE_SEED + i * 0x9E37).collect()
}

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

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
    let slot = Arc::new(Mutex::new(None));
    let s = slot.clone();
    env.spawn_task(async move {
        let v = fut.await;
        *s.lock().unwrap() = Some(v);
    });
    sim.run_for(budget);
    slot.lock().unwrap().take()
}

fn leader(nodes: &[KvNode]) -> usize {
    let ls: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(_, n)| n.is_leader())
        .map(|(i, _)| i)
        .collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?}");
    ls[0]
}

struct Fixture {
    sim: Simulator,
    engines: Vec<MemoryEngine>,
    nodes: Vec<KvNode>,
    k: Vec<u8>,
    record_key: Vec<u8>,
    txn_id: TxnId,
    writes: Vec<TxnWrite>,
    seed: u64,
}

fn voters() -> Vec<animus_env::NodeId> {
    NODES.iter().copied().map(nid).collect()
}

/// A node whose feature handle reads `cluster_version` (2 = the marker gate
/// open: `engine_image` ships markers as base rows; 1 = closed: markers ship
/// through the wire kind a previous-release replica ignores, issue #1251, so
/// a snapshot install is divergence-free at BOTH versions).
fn start_node(sim: &Simulator, id: u64, engine: MemoryEngine, cluster_version: u32) -> KvNode {
    let features = ClusterFeatures::new();
    features.update(&Metadata {
        cluster_version,
        ..Metadata::default()
    });
    RaftKvNode::start_hosted_with_options(
        sim.env(nid(id)),
        voters(),
        engine,
        StorageScope::whole(),
        PRIMARY_STREAM,
        HostedOptions {
            features,
            ..HostedOptions::default()
        },
    )
}

fn fixture(seed: u64, cluster_version: u32) -> Fixture {
    let mut sim = Simulator::new(seed);
    let engines: Vec<MemoryEngine> = NODES.iter().map(|_| MemoryEngine::new()).collect();
    let nodes: Vec<KvNode> = NODES
        .iter()
        .map(|&i| start_node(&sim, i, engines[i as usize].clone(), cluster_version))
        .collect();
    sim.run_for(ELECT);
    let k = key(b"acct-restage", b"balance");
    // A pure participant of a transaction anchored elsewhere: the record
    // key is not in this group's range, so no local record exists.
    let record_key = key(b"anchor-elsewhere", b"rec");
    let txn_id = TxnId {
        ts: HlcTimestamp {
            wall_ms: 1,
            logical: 0,
        },
        node: nid(9),
    };
    let writes = vec![TxnWrite::plain(k.clone(), Some(b"v1".to_vec()))];
    Fixture {
        sim,
        engines,
        nodes,
        k,
        record_key,
        txn_id,
        writes,
        seed,
    }
}

impl Fixture {
    fn stage(&mut self, l: usize) -> Option<(HlcTimestamp, animus_cp_data::StageOutcome)> {
        let n = self.nodes[l].clone();
        let (rk, w, id) = (
            self.record_key.clone(),
            self.writes.clone(),
            self.txn_id.clone(),
        );
        drive(&mut self.sim, self.nodes[l].env(), SETTLE, async move {
            n.txn_stage_participant(id, rk, "t".to_string(), w, Vec::new())
                .await
        })
        .flatten()
    }

    fn stage_and_resolve(&mut self, l: usize) {
        let seed = self.seed;
        let staged = self
            .stage(l)
            .unwrap_or_else(|| panic!("stage did not complete (seed={seed})"));
        let n = self.nodes[l].clone();
        let (rk, ks, id) = (
            self.record_key.clone(),
            vec![self.k.clone()],
            self.txn_id.clone(),
        );
        let commit_ts: HlcTimestamp = staged.0;
        drive(&mut self.sim, self.nodes[l].env(), SETTLE, async move {
            n.txn_resolve(id, rk, ks, TxnOutcome::Committed { commit_ts })
                .await
        })
        .flatten()
        .unwrap_or_else(|| panic!("resolve did not complete (seed={seed})"));
        self.sim.run_for(SETTLE);
    }

    /// A duplicate stage of the same, already-resolved transaction (a
    /// retried `TxnPrepare` whose first copy was accepted-unconfirmed),
    /// then assert every replica took the identical decision for it.
    fn dup_stage_then_assert_identical(&mut self, what: &str) {
        let l = leader(&self.nodes);
        let _ = self.stage(l);
        self.sim.run_for(SETTLE * 2);
        let seed = self.seed;
        // Raw rows incl. tombstones, anchor records and resolved markers: a
        // resurrected intent or a missing marker shows here even when a
        // plain `local_get` still agrees (issue #1251).
        let raw: Vec<super::txn_stage_replay_stability::RawRows> = self
            .engines
            .iter()
            .map(|e| {
                let mut rows = block_on(e.entries_with_tombstones()).unwrap();
                rows.retain(|(k, _, _)| !k.starts_with(b"__animus_system"));
                rows
            })
            .collect();
        for (i, rows) in raw.iter().enumerate().skip(1) {
            assert_eq!(
                rows, &raw[0],
                "replica {i} raw rows diverged from replica 0 after a stale re-stage ({what}) (seed={seed})"
            );
        }
        let heads: Vec<Option<Vec<u8>>> = self
            .nodes
            .iter()
            .map(|n| block_on(n.local_get(&self.k)))
            .collect();
        assert!(
            heads.windows(2).all(|p| p[0] == p[1]),
            "replicas diverged on a stale re-stage after resolve ({what}): {heads:?} (seed={seed})"
        );
        assert_eq!(
            heads[0],
            Some(b"v1".to_vec()),
            "resolved value must survive a stale re-stage ({what}) (seed={seed})"
        );
    }
}

#[test]
fn stale_restage_after_resolve_is_a_noop_on_a_restarted_replica() {
    for seed in seeds() {
        let mut f = fixture(seed, 2);
        let l = leader(&f.nodes);
        f.stage_and_resolve(l);
        for (i, n) in f.nodes.iter().enumerate() {
            assert_eq!(
                block_on(n.local_get(&f.k)),
                Some(b"v1".to_vec()),
                "node {i} resolved value (seed={seed})"
            );
        }
        // Restart one FOLLOWER: a fresh process, same durable engine.
        let fol = (0..3).find(|&i| i != l).unwrap();
        f.sim.stop(nid(fol as u64));
        f.nodes[fol] = start_node(&f.sim, fol as u64, f.engines[fol].clone(), 2);
        f.sim.run_for(ELECT);
        f.dup_stage_then_assert_identical(&format!("restarted={fol}"));
    }
}

/// A replica that was down for the stage AND the resolve, and catches up
/// through an `InstallSnapshot` (the log prefix holding them is compacted
/// away), has no in-process trace that the transaction ever resolved.
#[test]
fn stale_restage_after_resolve_is_a_noop_on_a_snapshot_installed_replica() {
    snapshot_installed_replica_case(2);
}

/// Issue #1251: the same at cluster version 1 (`Gate::GlobalTables` closed,
/// every unfinalized cluster). The snapshot sender must still carry the
/// resolved markers (through the wire kind a previous-release replica
/// drops), or the installed replica accepts the stale stage its peers reject.
#[test]
fn stale_restage_after_resolve_is_a_noop_on_a_snapshot_installed_replica_at_cluster_version_1() {
    snapshot_installed_replica_case(1);
}

fn snapshot_installed_replica_case(version: u32) {
    for seed in seeds() {
        let mut f = fixture(seed, version);
        let l = leader(&f.nodes);
        let lag = (0..3).find(|&i| i != l).unwrap();
        f.sim.crash(nid(lag as u64));
        f.stage_and_resolve(l);

        // Write past the follower-aware retention cap (4096 entries) so the leader's log
        // prefix (holding the stage + resolve) is truncated.
        for i in 0..4300u64 {
            match f.nodes[l].put(format!("pad{i:04}").into_bytes(), b"x".to_vec()) {
                animus_control::ProposeResult::Accepted { .. } => {}
                other => panic!("pad put {i} rejected: {other:?} (seed={seed})"),
            }
        }
        f.sim.run_for(Duration::from_secs(3));
        assert!(
            f.nodes[l].snapshot_index() > 0,
            "leader must have compacted (seed={seed})"
        );

        f.sim.restart(nid(lag as u64));
        f.sim.run_for(Duration::from_secs(5));
        assert_eq!(
            block_on(f.nodes[lag].local_get(&f.k)),
            Some(b"v1".to_vec()),
            "lagging replica must have caught up via snapshot (seed={seed})"
        );
        f.dup_stage_then_assert_identical(&format!("snapshot-installed={lag}"));
    }
}
