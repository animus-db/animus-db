//! The `Env`-driven Raft node: a thin driver that owns the environment and
//! ferries time and messages between the network and the synchronous
//! [`RaftCore`].

use std::sync::{Arc, Mutex};
use std::time::Duration;

use custos_env::{Env, EnvExt, NodeId};
use futures::future::{Either, select};

use crate::meta::{MetaCommand, Metadata};
use crate::persist::PersistedState;
use crate::raft::{ProposeResult, RaftCore, RaftMsg, Role};

/// File name of the per-node Raft write-ahead log on the `Env` disk.
const WAL: &str = "raft.wal";

/// Rewrite the WAL to its compact image once this many records have been
/// appended since the last compaction. Bounds the WAL to the live state
/// (latest checkpoint + hard state + current log) instead of growing with every
/// apply.
const WAL_COMPACT_THRESHOLD: usize = 64;

/// A running control-plane node. Cheap to clone; clones share one [`RaftCore`].
#[derive(Clone)]
pub struct RaftNode<E: Env> {
    env: E,
    core: Arc<Mutex<RaftCore>>,
}

impl<E: Env> RaftNode<E> {
    /// Start a node: build its [`RaftCore`] and spawn the driver loop on `env`.
    /// `all_nodes` is the full control-group membership (including this node).
    pub fn start(env: E, all_nodes: Vec<NodeId>) -> Self {
        let core = Arc::new(Mutex::new(RaftCore::new(
            env.node_id(),
            &all_nodes,
            env.now(),
            env.next_u64(),
        )));
        let node = Self {
            env: env.clone(),
            core: Arc::clone(&core),
        };
        env.spawn_task(drive(env.clone(), core, all_nodes));
        node
    }

    /// Propose a metadata command. See [`ProposeResult`].
    pub fn propose(&self, command: MetaCommand) -> ProposeResult {
        self.lock().propose(command)
    }

    /// This node's environment handle.
    pub fn env(&self) -> &E {
        &self.env
    }

    /// Whether this node currently believes it is leader.
    pub fn is_leader(&self) -> bool {
        self.lock().is_leader()
    }

    /// The node's current role.
    pub fn role(&self) -> Role {
        self.lock().role()
    }

    /// The current term.
    pub fn term(&self) -> u64 {
        self.lock().term()
    }

    /// Best-known leader id.
    pub fn leader(&self) -> Option<NodeId> {
        self.lock().leader()
    }

    /// A clone of the applied metadata state.
    pub fn metadata(&self) -> Metadata {
        self.lock().metadata()
    }

    /// The sequence of commands applied so far, in order.
    pub fn applied(&self) -> Vec<MetaCommand> {
        self.lock().applied()
    }

    /// Highest committed log index.
    pub fn commit_index(&self) -> u64 {
        self.lock().commit_index()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RaftCore> {
        self.core.lock().expect("raft core poisoned")
    }
}

/// The per-node driver loop: recover durable state, then repeatedly wait for the
/// next message or timer, hand it to the core, persist the resulting durable
/// changes, and ship whatever the core wants sent.
async fn drive<E: Env>(env: E, core: Arc<Mutex<RaftCore>>, all_nodes: Vec<NodeId>) {
    // Recover from the WAL before serving anything.
    let bytes = env.read(WAL).await.unwrap_or_default();
    let state = PersistedState::replay(PersistedState::decode(&bytes));
    if !state.is_empty() {
        let recovered =
            RaftCore::recovered(env.node_id(), &all_nodes, state, env.now(), env.next_u64());
        *core.lock().expect("raft core poisoned") = recovered;
    }

    let mut since_compaction = 0usize;
    loop {
        // Persist anything queued out-of-band (e.g. a client `propose`).
        flush_and_maybe_compact(&env, &core, &mut since_compaction).await;

        let now = env.now();
        let deadline = core.lock().expect("raft core poisoned").next_deadline();
        let wait = Duration::from_nanos(deadline.0.saturating_sub(now.0));

        let outs = match select(env.recv(), env.sleep(wait)).await {
            Either::Left((envelope, _)) => {
                let entropy = env.next_u64();
                match serde_json::from_slice::<RaftMsg>(&envelope.payload) {
                    Ok(msg) => core.lock().expect("raft core poisoned").handle(
                        envelope.from,
                        msg,
                        env.now(),
                        entropy,
                    ),
                    Err(err) => {
                        tracing::warn!(?err, "undecodable raft message dropped");
                        Vec::new()
                    }
                }
            }
            Either::Right(((), _)) => {
                let entropy = env.next_u64();
                core.lock()
                    .expect("raft core poisoned")
                    .tick(env.now(), entropy)
            }
        };

        // Durability before action: persist (and fsync) the core's state changes
        // before sending the responses that depend on them (a granted vote, an
        // acknowledged append).
        flush_and_maybe_compact(&env, &core, &mut since_compaction).await;

        for (to, msg) in outs {
            let bytes = serde_json::to_vec(&msg).expect("raft message serializes");
            env.send(to, bytes).await;
        }
    }
}

/// Flush pending records, counting them toward the next compaction; rewrite the
/// WAL to its compact image once the threshold is crossed.
async fn flush_and_maybe_compact<E: Env>(
    env: &E,
    core: &Arc<Mutex<RaftCore>>,
    since_compaction: &mut usize,
) {
    *since_compaction += flush_wal(env, core).await;
    if *since_compaction >= WAL_COMPACT_THRESHOLD {
        compact_wal(env, core).await;
        *since_compaction = 0;
    }
}

/// Append and `fsync` any pending durable-state records to the WAL. Returns how
/// many records were written.
async fn flush_wal<E: Env>(env: &E, core: &Arc<Mutex<RaftCore>>) -> usize {
    let records = core.lock().expect("raft core poisoned").drain_persist();
    if records.is_empty() {
        return 0;
    }
    for record in &records {
        env.append(WAL, &PersistedState::encode_record(record))
            .await
            .expect("wal append");
    }
    env.sync(WAL).await.expect("wal sync");
    records.len()
}

/// Atomically rewrite the WAL to the core's compact image (latest checkpoint +
/// hard state + current log). Safe because [`flush_wal`] has already persisted
/// everything the image is built from.
async fn compact_wal<E: Env>(env: &E, core: &Arc<Mutex<RaftCore>>) {
    let image = core.lock().expect("raft core poisoned").wal_image();
    let mut bytes = Vec::new();
    for record in &image {
        bytes.extend(PersistedState::encode_record(record));
    }
    env.replace(WAL, &bytes).await.expect("wal compaction");
}
