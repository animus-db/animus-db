//! Issue #1315: does a mixed previous-release / current cluster apply the SAME
//! log entry differently, and does that produce the `upgrade-previous-release`
//! CI symptoms (an acknowledged write lost, a transaction half applied)?
//!
//! The previous release R-1 (`f6709564`) rejects a stale or duplicate
//! `TxnStage` from `TxnTracker::recently_resolved`, process memory that a
//! restart or an `InstallSnapshot` empties. The current build rejects it from
//! the durable resolved marker (#1243/#1251). The `legacy-r1-model` feature
//! (`animus_cp_data::legacy_r1_model`) arms a node with R-1's apply, so these
//! cells replay one log through both semantics, with no R-1 binary.
//!
//! Cells (each over `ANIMUS_RESTAGE_SEEDS` seeds, default 2):
//! * controls: an all-current cluster and an all-R-1 cluster each stay
//!   replica-identical across a stale stage (so the mix, not either side
//!   alone, is what diverges);
//! * a current replica caught up by `InstallSnapshot` from an R-1 leader holds
//!   no markers (R-1 never wrote any) and accepts what its R-1 peers reject;
//! * an R-1 replica caught up by `InstallSnapshot` from a current leader has
//!   an empty map and accepts what its current peers reject;
//! * the consequence: the resurrected intent is later resolved again (a
//!   resolver retry or a read-path push), rewriting the key to the old
//!   transaction value on that replica only, so an acked later write is gone
//!   there, and a later transaction touching the key is blocked there.
//!
//! The divergence cells are CHARACTERIZATIONS of a hazard the current side
//! cannot close on its own (a current replica cannot tell "never resolved"
//! from "resolved by a binary that wrote no marker"): they assert the
//! divergence exists. If a mitigation lands they flip, on purpose.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::Metadata;
use animus_control::version::ClusterFeatures;
use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::{
    HostedOptions, RaftKvNode, StorageScope, TxnId, TxnOutcome, TxnWrite, legacy_r1_model,
};
use animus_env::{EnvExt, PRIMARY_STREAM, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{escape, partition_token};
use futures::executor::block_on;

const NODES: [u64; 3] = [0, 1, 2];
const ELECT: Duration = Duration::from_secs(2);
const SETTLE: Duration = Duration::from_secs(2);
const BASE_SEED: u64 = 0x1315_0A11;

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;
type Row = Option<(Vec<u8>, Option<Vec<u8>>, u64)>;

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        return vec![s];
    }
    let k = std::env::var("ANIMUS_RESTAGE_SEEDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(2)
        .max(1);
    (0..k).map(|i| BASE_SEED + i * 0x9E37).collect()
}

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

/// `legacy[i]` arms node `i` with the previous release's apply.
fn start_node(sim: &Simulator, id: u64, engine: MemoryEngine, legacy: bool) -> KvNode {
    legacy_r1_model::set(nid(id), legacy);
    let features = ClusterFeatures::new();
    // Cluster version 1: an unfinalized cluster, where every rolling upgrade
    // starts (the marker travels in snapshot images through the wire kind).
    features.update(&Metadata {
        cluster_version: 1,
        ..Metadata::default()
    });
    RaftKvNode::start_hosted_with_options(
        sim.env(nid(id)),
        NODES.iter().copied().map(nid).collect(),
        engine,
        StorageScope::whole(),
        PRIMARY_STREAM,
        HostedOptions {
            features,
            ..HostedOptions::default()
        },
    )
}

struct Rig {
    sim: Simulator,
    engines: Vec<MemoryEngine>,
    nodes: Vec<KvNode>,
    k: Vec<u8>,
    record_key: Vec<u8>,
    txn_id: TxnId,
    commit_ts: Option<HlcTimestamp>,
    /// A crashed node still answers `is_leader` from stale state.
    down: Option<usize>,
    seed: u64,
}

fn rig(seed: u64, legacy: [bool; 3]) -> Rig {
    let mut sim = Simulator::new(seed);
    let engines: Vec<MemoryEngine> = NODES.iter().map(|_| MemoryEngine::new()).collect();
    let nodes: Vec<KvNode> = NODES
        .iter()
        .map(|&i| start_node(&sim, i, engines[i as usize].clone(), legacy[i as usize]))
        .collect();
    sim.run_for(ELECT);
    Rig {
        sim,
        engines,
        nodes,
        k: key(b"acct-1315", b"balance"),
        record_key: key(b"anchor-elsewhere", b"rec"),
        txn_id: TxnId {
            ts: HlcTimestamp {
                wall_ms: 1,
                logical: 0,
            },
            node: nid(9),
        },
        commit_ts: None,
        down: None,
        seed,
    }
}

impl Rig {
    fn leader(&self) -> usize {
        let ls: Vec<usize> = (0..3)
            .filter(|&i| self.down != Some(i) && self.nodes[i].is_leader())
            .collect();
        assert_eq!(
            ls.len(),
            1,
            "expected one leader, got {ls:?} (seed={})",
            self.seed
        );
        ls[0]
    }

    fn writes(&self) -> Vec<TxnWrite> {
        vec![TxnWrite::plain(self.k.clone(), Some(b"v1".to_vec()))]
    }

    fn stage(&mut self, l: usize) -> Option<HlcTimestamp> {
        let n = self.nodes[l].clone();
        let (rk, w, id) = (self.record_key.clone(), self.writes(), self.txn_id.clone());
        drive(&mut self.sim, self.nodes[l].env(), SETTLE, async move {
            n.txn_stage_participant(id, rk, "t".to_string(), w, Vec::new())
                .await
        })
        .flatten()
        .map(|(ts, _)| ts)
    }

    fn resolve(&mut self, l: usize, commit_ts: HlcTimestamp) {
        let n = self.nodes[l].clone();
        let (rk, ks, id) = (
            self.record_key.clone(),
            vec![self.k.clone()],
            self.txn_id.clone(),
        );
        drive(&mut self.sim, self.nodes[l].env(), SETTLE, async move {
            n.txn_resolve(id, rk, ks, TxnOutcome::Committed { commit_ts })
                .await
        })
        .flatten()
        .unwrap_or_else(|| panic!("resolve did not complete (seed={})", self.seed));
        self.sim.run_for(SETTLE);
    }

    fn stage_and_resolve(&mut self, l: usize) {
        let ts = self
            .stage(l)
            .unwrap_or_else(|| panic!("stage did not complete (seed={})", self.seed));
        self.commit_ts = Some(ts);
        self.resolve(l, ts);
    }

    /// Write past the follower-aware retention cap so the leader compacts the
    /// log prefix holding the stage and the resolve.
    fn compact_leader(&mut self, l: usize) {
        for i in 0..4300u64 {
            match self.nodes[l].put(format!("pad{i:04}").into_bytes(), b"x".to_vec()) {
                animus_control::ProposeResult::Accepted { .. } => {}
                other => panic!("pad put {i} rejected: {other:?} (seed={})", self.seed),
            }
        }
        self.sim.run_for(Duration::from_secs(3));
        assert!(
            self.nodes[l].snapshot_index() > 0,
            "leader must have compacted (seed={})",
            self.seed
        );
    }

    /// A duplicate of the already-resolved stage (a retried `TxnPrepare`
    /// whose first copy was accepted-unconfirmed), on the current leader.
    fn dup_stage(&mut self) {
        let l = self.leader();
        let _ = self.stage(l);
        self.sim.run_for(SETTLE * 2);
    }

    /// The raw envelope row of the base key on node `i` (the shortest row key
    /// ending in the logical key: the marker row also ends in it but is
    /// longer, and an R-1 node has none).
    fn base_row(&self, i: usize) -> Row {
        let rows = block_on(self.engines[i].entries_with_tombstones()).unwrap();
        rows.into_iter()
            .filter(|(rk, _, _)| rk.ends_with(&self.k))
            .min_by_key(|(rk, _, _)| rk.len())
    }

    fn base_rows(&self) -> Vec<Row> {
        (0..3).map(|i| self.base_row(i)).collect()
    }

    fn heads(&self) -> Vec<Option<Vec<u8>>> {
        self.nodes
            .iter()
            .map(|n| block_on(n.local_get(&self.k)))
            .collect()
    }
}

fn assert_identical(r: &Rig, what: &str) {
    let rows = r.base_rows();
    assert!(
        rows.windows(2).all(|p| p[0] == p[1]),
        "{what}: base-key rows diverged after a stale re-stage: {rows:?} (seed={})",
        r.seed
    );
}

#[test]
fn control_all_current_is_identical_across_a_stale_stage() {
    for seed in seeds() {
        let mut r = rig(seed, [false; 3]);
        let l = r.leader();
        r.stage_and_resolve(l);
        r.dup_stage();
        assert_identical(&r, "all current");
        assert_eq!(r.heads()[0], Some(b"v1".to_vec()));
    }
}

#[test]
fn control_all_r1_without_a_restart_is_identical_across_a_stale_stage() {
    for seed in seeds() {
        let mut r = rig(seed, [true; 3]);
        let l = r.leader();
        r.stage_and_resolve(l);
        r.dup_stage();
        assert_identical(&r, "all R-1, same uptime");
        assert_eq!(r.heads()[0], Some(b"v1".to_vec()));
    }
}

/// Mixed cluster, the lagging replica is CURRENT and its leader is R-1: the
/// image it installs carries no marker (R-1 never wrote one), so it accepts
/// the stale stage its R-1 peers reject from memory.
#[test]
fn current_replica_installed_from_an_r1_leader_resurrects_a_stale_stage() {
    for seed in seeds() {
        let mut r = rig(seed, [true, true, false]);
        let lag = 2;
        r.sim.crash(nid(lag as u64));
        r.down = Some(lag);
        r.sim.run_for(ELECT);
        let l = r.leader();
        assert_ne!(l, lag, "seed={seed}");
        r.stage_and_resolve(l);
        r.compact_leader(l);
        r.sim.restart(nid(lag as u64));
        r.down = None;
        r.sim.run_for(Duration::from_secs(5));
        assert_eq!(
            r.heads()[lag],
            Some(b"v1".to_vec()),
            "current replica must have caught up via snapshot (seed={seed})"
        );
        assert_identical(&r, "after the snapshot install, before the stale stage");
        r.dup_stage();
        let rows = r.base_rows();
        assert_ne!(
            rows[lag], rows[l],
            "HAZARD EXPECTED: the current replica installed from an R-1 leader must accept \
             the stale stage its R-1 peers reject (seed={seed}): {rows:?}"
        );
    }
}

/// The converse: the lagging replica is R-1 and its leader is current. The
/// marker rides the image (wire kind, #1251) but R-1 never reads it.
#[test]
fn r1_replica_installed_from_a_current_leader_resurrects_a_stale_stage() {
    for seed in seeds() {
        let mut r = rig(seed, [false, false, true]);
        let lag = 2;
        r.sim.crash(nid(lag as u64));
        r.down = Some(lag);
        r.sim.run_for(ELECT);
        let l = r.leader();
        assert_ne!(l, lag, "seed={seed}");
        r.stage_and_resolve(l);
        r.compact_leader(l);
        r.sim.restart(nid(lag as u64));
        r.down = None;
        r.sim.run_for(Duration::from_secs(5));
        assert_eq!(r.heads()[lag], Some(b"v1".to_vec()), "seed={seed}");
        assert_identical(&r, "after the snapshot install, before the stale stage");
        r.dup_stage();
        let rows = r.base_rows();
        assert_ne!(
            rows[lag], rows[l],
            "HAZARD EXPECTED: an R-1 replica installed from a current leader has an empty \
             recently_resolved and must accept the stale stage (seed={seed}): {rows:?}"
        );
    }
}

/// The CI symptom: after the divergence, the resurrected intent is resolved
/// again (a resolver retry or a read-path push finds it and its decided
/// record), which rewrites the key to the old transaction value on THAT
/// replica only: an acknowledged later write is lost there. If that replica
/// leads, the loss is client-visible.
#[test]
fn a_resurrected_intent_later_overwrites_an_acked_write_on_one_replica() {
    for seed in seeds() {
        let mut r = rig(seed, [true, true, false]);
        let lag = 2;
        r.sim.crash(nid(lag as u64));
        r.down = Some(lag);
        r.sim.run_for(ELECT);
        let l = r.leader();
        r.stage_and_resolve(l);
        let commit_ts = r.commit_ts.unwrap();
        r.compact_leader(l);
        r.sim.restart(nid(lag as u64));
        r.down = None;
        r.sim.run_for(Duration::from_secs(5));
        // An acknowledged later write to the same key, THEN the stale stage
        // (a retried prepare delivered late).
        let l = r.leader();
        match r.nodes[l].put(r.k.clone(), b"v2-acked".to_vec()) {
            animus_control::ProposeResult::Accepted { .. } => {}
            other => panic!("later put rejected: {other:?} (seed={seed})"),
        }
        r.sim.run_for(SETTLE * 2);
        assert!(
            r.heads()
                .iter()
                .all(|h| h.as_deref() == Some(&b"v2-acked"[..])),
            "all replicas hold the acked write before the stale stage (seed={seed}): {:?}",
            r.heads()
        );
        r.dup_stage();
        // The decided transaction's resolve is retried (same outcome).
        r.resolve(r.leader(), commit_ts);
        let heads = r.heads();
        eprintln!("seed={seed} heads after the re-resolve: {heads:?}");
        assert_eq!(
            heads[l],
            Some(b"v2-acked".to_vec()),
            "the R-1 leader keeps the acked write (seed={seed})"
        );
        assert_ne!(
            heads[lag],
            Some(b"v2-acked".to_vec()),
            "HAZARD EXPECTED: the current replica that resurrected the intent loses the acked \
             write to the retried resolve (seed={seed}): {heads:?}"
        );
    }
}

/// The other CI symptom, a transaction half applied: the resurrected intent
/// blocks every later stage touching the key on that replica only
/// (whole-or-nothing, `blocked_by`), so an acknowledged later transaction is
/// applied on its peers and missing there; with the second key on another
/// tablet this is "one key of an acked transaction applied, the other not"
/// as soon as that replica leads or serves an eventual read.
#[test]
fn a_resurrected_intent_blocks_a_later_acked_transaction_on_one_replica() {
    for seed in seeds() {
        let mut r = rig(seed, [true, true, false]);
        let lag = 2;
        r.sim.crash(nid(lag as u64));
        r.down = Some(lag);
        r.sim.run_for(ELECT);
        let l = r.leader();
        r.stage_and_resolve(l);
        r.compact_leader(l);
        r.sim.restart(nid(lag as u64));
        r.down = None;
        r.sim.run_for(Duration::from_secs(5));
        r.dup_stage();
        // A later, different transaction T2 stages the same key and is acked.
        r.txn_id = TxnId {
            ts: HlcTimestamp {
                wall_ms: 2,
                logical: 0,
            },
            node: nid(9),
        };
        let l = r.leader();
        let staged = r.stage(l);
        assert!(staged.is_some(), "T2 stage acked (seed={seed})");
        r.sim.run_for(SETTLE * 2);
        let rows = r.base_rows();
        assert_eq!(rows[0], rows[1], "the two R-1 peers agree (seed={seed})");
        assert_ne!(
            rows[lag], rows[0],
            "HAZARD EXPECTED: T2's intent is blocked on the replica holding the resurrected \
             T1 intent only (seed={seed}): {rows:?}"
        );
    }
}

/// Issue #1355: the same hazard through a plain RESTART instead of a snapshot.
/// A node rolled from R-1 to the current build re-applies its recovered log
/// tail (from the log's compaction base, which log compaction keeps at tens of
/// entries) over an engine R-1 wrote. A stale stage inside that tail was
/// rejected live by R-1's in-memory guard; the resolve that guard remembered
/// was compacted away, so the replay finds no marker (R-1 wrote none) and no
/// later version on the key, and accepts the stage: an intent exists on the
/// restarted replica only, which later rewrites the key to the old transaction
/// value and so loses acked appends there (the eventual-read prefix violation
/// on key 20 in `upgrade-previous-release`, `clean_3_nodes`).
#[test]
fn a_replica_restarted_from_r1_resurrects_a_stale_stage_in_its_replayed_tail() {
    for seed in seeds() {
        let mut r = rig(seed, [true; 3]);
        let l = r.leader();
        r.stage_and_resolve(l);
        r.compact_leader(l);
        // The stale stage lands after the compaction base, so it is in every
        // replica's replay tail while its resolve is not.
        r.dup_stage();
        assert_identical(&r, "all R-1, stale stage rejected from memory");
        let fol = (0..3).find(|&i| i != l).unwrap();
        r.sim.stop(nid(fol as u64));
        r.nodes[fol] = start_node(&r.sim, fol as u64, r.engines[fol].clone(), false);
        r.sim.run_for(ELECT);
        let rows = r.base_rows();
        assert_ne!(
            rows[fol], rows[l],
            "HAZARD EXPECTED: the replica restarted on the current build replays the stale \
             stage its R-1 peers rejected (seed={seed}): {rows:?}"
        );
    }
}

/// Control for the cell above: the identical schedule on an all-current
/// cluster (markers written live, so the replayed tail agrees with the live
/// decision) stays replica-identical. The mix, not the restart, diverges.
#[test]
fn control_a_restarted_current_replica_replays_a_stale_stage_identically() {
    for seed in seeds() {
        let mut r = rig(seed, [false; 3]);
        let l = r.leader();
        r.stage_and_resolve(l);
        r.compact_leader(l);
        r.dup_stage();
        assert_identical(&r, "all current, stale stage rejected from the marker");
        let fol = (0..3).find(|&i| i != l).unwrap();
        r.sim.stop(nid(fol as u64));
        r.nodes[fol] = start_node(&r.sim, fol as u64, r.engines[fol].clone(), false);
        r.sim.run_for(ELECT);
        assert_identical(&r, "all current, after a restart replaying the tail");
    }
}
