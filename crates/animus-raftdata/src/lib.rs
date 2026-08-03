//! Leaderful per-tablet Raft **data plane** (ADR 0016 / ADR 0017): each tablet is
//! its own Raft group with a single leader serving linearizable single-tablet
//! reads and writes, durable on a real [`StorageEngine`].
//!
//! This is the CP counterpart to the leaderless AP `animus-data` plane, built
//! additively. It reuses the control plane's generic, sync, I/O-free
//! [`RaftCore`](animus_control::RaftCore) (ADR 0009) — instantiated here with a
//! key-value command and a `DRIVER_APPLIED` state machine, so committed commands
//! are applied by **this** async driver to the engine rather than in-core.
//!
//! Stage status (ADR 0017): **B.1 (writes) + B.2 (ReadIndex reads)**. The driver
//! recovers from its WAL, replicates `KvCommand`s through Raft, fsyncs the WAL
//! before acting (durable-before-visible), and applies committed-and-durable
//! commands to the engine in commit order (the Raft index is the MVCC version, so
//! per-key LWW reproduces the agreed total order). Linearizable reads use
//! **ReadIndex** ([`RaftKvNode::linearizable_get`]): a read-barrier quorum probe
//! confirms current leadership (no log entry, no wall clock) before the leader
//! serves from its local engine. Still ahead: streaming snapshots A.2;
//! reconfiguration C; tablet split D.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::raft::{Out, RaftCore, RaftMsg, StateMachine};
use animus_control::{PersistedState, ProposeResult};
use animus_env::{Env, EnvExt, NodeId};
use animus_storage::StorageEngine;
use futures::future::{Either, select};
use serde::{Deserialize, Serialize};

/// The data plane's Raft log command: a key-value mutation (or the election
/// no-op). Keys/values are opaque bytes; ordering + durability come from Raft.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvCommand {
    /// Set `key` to `value`.
    Put { key: Vec<u8>, value: Vec<u8> },
    /// Remove `key` (a tombstone in the engine).
    Delete { key: Vec<u8> },
    /// The leader's no-op-on-election (Raft); applies nothing.
    NoOp,
}

/// The data-plane state machine is `DRIVER_APPLIED`: the real applied state lives
/// in the [`StorageEngine`], written by the async driver, so the in-core image is
/// a unit placeholder and the core never applies it in-core (ADR 0017).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvState;

impl StateMachine<KvCommand> for KvState {
    const DRIVER_APPLIED: bool = true;
    fn apply(&mut self, _command: &KvCommand) {
        unreachable!("KvState is DRIVER_APPLIED; the driver applies to the engine");
    }
    fn noop() -> KvCommand {
        KvCommand::NoOp
    }
}

/// The per-tablet Raft core, specialized to the KV command + driver-applied state.
type KvCore = RaftCore<KvCommand, KvState>;

/// The data-plane wire: Raft consensus traffic plus the **ReadIndex** read-barrier
/// probes (ADR 0017). The probes are *not* consensus traffic — they never touch
/// `RaftCore`; the driver handles them — so ReadIndex lives entirely in this crate
/// and the shared `RaftCore`/`RaftMsg` are untouched.
#[derive(Clone, Debug, Serialize, Deserialize)]
enum KvWire {
    /// A control-plane-shaped Raft message for this group.
    Raft(RaftMsg<KvCommand>),
    /// Leader → peers: "are you still in term `term`?" (a ReadIndex barrier,
    /// tagged with the leader's read `epoch`).
    ReadProbe { term: u64, epoch: u64 },
    /// Peer → leader: "yes, I am still in term `term`" for read `epoch`. A quorum
    /// of these confirms the prober is still leader as of now (a newer leader would
    /// require a quorum to have moved to a higher term, which would *not* ack).
    ReadProbeAck { term: u64, epoch: u64 },
}

/// How long a [`linearizable_get`](RaftKvNode::linearizable_get) waits for a quorum
/// read-barrier confirmation before giving up.
const READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Poll granularity while a linearizable read waits for confirmation.
const READ_POLL: Duration = Duration::from_millis(20);

/// Leader-side ReadIndex state: per in-flight read `epoch`, the term it was issued
/// under and the set of peers (including self) that have confirmed leadership.
#[derive(Default)]
struct ReadState {
    next_epoch: u64,
    /// `epoch -> (term, acking nodes)`.
    pending: BTreeMap<u64, (u64, BTreeSet<NodeId>)>,
}

/// WAL file for a tablet group's Raft log (distinct from the control plane's
/// `raft.wal`, so a node can host both without collision).
const WAL: &str = "raftkv.wal";

/// A running data-plane Raft node for one tablet group. Cheap to clone; clones
/// share the one [`RaftCore`] + engine. The driver loop runs on `env`.
#[derive(Clone)]
pub struct RaftKvNode<E: Env, S: StorageEngine> {
    env: E,
    core: Arc<Mutex<KvCore>>,
    storage: S,
    all_nodes: Vec<NodeId>,
    reads: Arc<Mutex<ReadState>>,
}

impl<E: Env, S: StorageEngine + 'static> RaftKvNode<E, S> {
    /// Start a tablet group node over `env`, backed by `storage`. `all_nodes` is
    /// the group's full replica set (including this node). Spawns the driver loop.
    pub fn start(env: E, all_nodes: Vec<NodeId>, storage: S) -> Self {
        let core = Arc::new(Mutex::new(RaftCore::new(
            env.node_id(),
            &all_nodes,
            env.now(),
            env.next_u64(),
        )));
        let reads = Arc::new(Mutex::new(ReadState::default()));
        let node = Self {
            env: env.clone(),
            core: Arc::clone(&core),
            storage: storage.clone(),
            all_nodes: all_nodes.clone(),
            reads: Arc::clone(&reads),
        };
        env.spawn_task(drive(env.clone(), core, all_nodes, storage, reads));
        node
    }

    fn majority(&self) -> usize {
        self.all_nodes.len() / 2 + 1
    }

    /// Propose a write to this group. Honored only on the leader (otherwise
    /// returns the leader hint); the value is durable + applied once committed.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> ProposeResult {
        self.lock().propose(KvCommand::Put { key, value })
    }

    /// Propose a delete (tombstone) to this group.
    pub fn delete(&self, key: Vec<u8>) -> ProposeResult {
        self.lock().propose(KvCommand::Delete { key })
    }

    /// Read `key` from this replica's **local engine**. NOTE: this is a local read
    /// — it is *not* yet linearizable (that is ReadIndex, Stage B.2). It is used by
    /// tests to observe a replica's applied state and to confirm convergence.
    pub async fn local_get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.storage
            .get(key)
            .await
            .ok()
            .flatten()
            .map(|vv| vv.value)
    }

    /// A **linearizable** read of `key` via **ReadIndex** (ADR 0017): only the
    /// leader can serve it. Records `read_index = commit_index`, confirms it is
    /// still leader by a quorum of peers acking its current term (a read-barrier
    /// probe — no log entry, no wall clock), waits until its applied state reaches
    /// `read_index`, then reads the local engine. Returns `None` if this node is
    /// not (or stops being) the leader, or if confirmation times out — never a
    /// stale value: a deposed leader cannot collect a quorum ack (a newer leader
    /// requires a quorum at a higher term, which would reject the probe).
    pub async fn linearizable_get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let (term, read_index) = {
            let c = self.lock();
            if !c.is_leader() {
                return None;
            }
            (c.term(), c.commit_index())
        };
        // Register the read barrier (self trivially confirms its own term).
        let epoch = {
            let mut r = self.reads.lock().expect("read state poisoned");
            r.next_epoch += 1;
            let epoch = r.next_epoch;
            let mut acks = BTreeSet::new();
            acks.insert(self.env.node_id());
            r.pending.insert(epoch, (term, acks));
            epoch
        };
        // Probe peers immediately (periodic heartbeats would also carry it, but an
        // explicit probe confirms promptly).
        let probe =
            serde_json::to_vec(&KvWire::ReadProbe { term, epoch }).expect("probe serializes");
        for &p in &self.all_nodes {
            if p != self.env.node_id() {
                self.env.send(p, probe.clone()).await;
            }
        }

        let majority = self.majority();
        let deadline = self.env.now().0 + READ_TIMEOUT.as_nanos() as u64;
        let result = loop {
            // Still the leader for this term? A step-down/term change invalidates
            // the barrier — fail rather than risk a stale read.
            let still_leader = {
                let c = self.lock();
                c.is_leader() && c.term() == term
            };
            let confirmed = {
                let r = self.reads.lock().expect("read state poisoned");
                r.pending
                    .get(&epoch)
                    .is_some_and(|(_, acks)| acks.len() >= majority)
            };
            let applied = self.lock().last_applied() >= read_index;
            if !still_leader || self.env.now().0 >= deadline {
                break None;
            }
            if confirmed && applied {
                break self.local_get(key).await;
            }
            self.env.sleep(READ_POLL).await;
        };
        self.reads
            .lock()
            .expect("read state poisoned")
            .pending
            .remove(&epoch);
        result
    }

    /// Whether this node currently believes it is the group's leader.
    pub fn is_leader(&self) -> bool {
        self.lock().is_leader()
    }

    /// This node's `Env` handle.
    pub fn env(&self) -> &E {
        &self.env
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, KvCore> {
        self.core.lock().expect("raftkv core poisoned")
    }
}

/// Persist the core's pending WAL records (append + `fsync`), then advance the
/// durable watermark so committed entries become applicable, then **apply** the
/// now-durable commands to the engine in commit order. Durability precedes both
/// visibility and the engine write (ADR 0009 / ADR 0017).
async fn flush_and_apply<E: Env, S: StorageEngine>(
    env: &E,
    core: &Arc<Mutex<KvCore>>,
    storage: &S,
) {
    // Drain WAL records + the log high-water under one lock.
    let (records, through) = {
        let mut c = core.lock().expect("raftkv core poisoned");
        (c.drain_persist(), c.last_log_index())
    };
    if !records.is_empty() {
        for record in &records {
            env.append(WAL, &PersistedState::encode_record(record))
                .await
                .expect("raftkv wal append");
        }
        env.sync(WAL).await.expect("raftkv wal sync");
        core.lock()
            .expect("raftkv core poisoned")
            .mark_durable_through(through);
    }

    // Apply the now-durable committed commands to the engine, in commit order.
    // The Raft index is the MVCC version: per-key LWW then reproduces the agreed
    // total order, and re-applying on recovery is idempotent.
    let effects = core.lock().expect("raftkv core poisoned").drain_apply();
    for (index, command) in effects {
        match command {
            KvCommand::Put { key, value } => {
                storage
                    .merge(&key, &value, index)
                    .await
                    .expect("raftkv apply put");
            }
            KvCommand::Delete { key } => {
                storage
                    .merge_tombstone(&key, index)
                    .await
                    .expect("raftkv apply delete");
            }
            KvCommand::NoOp => {}
        }
    }
}

/// The per-node driver loop: recover from the WAL, then repeatedly persist+apply,
/// wait for the next message or timer, step the core, persist+apply again, and
/// ship outbound. Mirrors the control-plane `RaftNode` driver, minus the
/// reconcile/failure-detector loops (control-plane only), plus engine apply.
async fn drive<E: Env, S: StorageEngine>(
    env: E,
    core: Arc<Mutex<KvCore>>,
    all_nodes: Vec<NodeId>,
    storage: S,
    reads: Arc<Mutex<ReadState>>,
) {
    let bytes = env.read(WAL).await.unwrap_or_default();
    let state = PersistedState::replay(PersistedState::decode(&bytes));
    if !state.is_empty() {
        let recovered =
            RaftCore::recovered(env.node_id(), &all_nodes, state, env.now(), env.next_u64());
        *core.lock().expect("raftkv core poisoned") = recovered;
    }

    loop {
        flush_and_apply(&env, &core, &storage).await;

        let now = env.now();
        let deadline = core.lock().expect("raftkv core poisoned").next_deadline();
        let wait = Duration::from_nanos(deadline.0.saturating_sub(now.0));

        // Each step yields outbound `KvWire` messages (Raft traffic and/or a read
        // probe ack).
        let outs: Vec<(NodeId, KvWire)> = match select(env.recv(), env.sleep(wait)).await {
            Either::Left((envelope, _)) => {
                let entropy = env.next_u64();
                match serde_json::from_slice::<KvWire>(&envelope.payload) {
                    Ok(KvWire::Raft(msg)) => {
                        let raft_outs: Vec<Out<KvCommand>> = core
                            .lock()
                            .expect("raftkv core poisoned")
                            .handle(envelope.from, msg, env.now(), entropy);
                        raft_outs
                            .into_iter()
                            .map(|(to, m)| (to, KvWire::Raft(m)))
                            .collect()
                    }
                    // A ReadProbe is answered iff we are still in the prober's term
                    // (we have not moved on to help elect a newer leader). Not
                    // consensus traffic — the core never sees it.
                    Ok(KvWire::ReadProbe { term, epoch }) => {
                        let same_term = core.lock().expect("raftkv core poisoned").term() == term;
                        if same_term {
                            vec![(envelope.from, KvWire::ReadProbeAck { term, epoch })]
                        } else {
                            Vec::new()
                        }
                    }
                    Ok(KvWire::ReadProbeAck { term, epoch }) => {
                        let mut r = reads.lock().expect("read state poisoned");
                        if let Some((t, acks)) = r.pending.get_mut(&epoch) {
                            if *t == term {
                                acks.insert(envelope.from);
                            }
                        }
                        Vec::new()
                    }
                    Err(err) => {
                        tracing::warn!(?err, "undecodable raftkv message dropped");
                        Vec::new()
                    }
                }
            }
            Either::Right(((), _)) => {
                let entropy = env.next_u64();
                let raft_outs = core
                    .lock()
                    .expect("raftkv core poisoned")
                    .tick(env.now(), entropy);
                raft_outs
                    .into_iter()
                    .map(|(to, m)| (to, KvWire::Raft(m)))
                    .collect()
            }
        };

        // Durability before action: persist + apply before shipping responses.
        flush_and_apply(&env, &core, &storage).await;

        for (to, wire) in outs {
            let bytes = serde_json::to_vec(&wire).expect("raftkv message serializes");
            env.send(to, bytes).await;
        }
    }
}
