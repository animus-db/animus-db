//! The `Env`-driven Raft node: a thin driver that owns the environment and
//! ferries time and messages between the network and the synchronous
//! [`RaftCore`].

use std::sync::{Arc, Mutex};
use std::time::Duration;

use custos_env::{Env, EnvExt, NodeId};
use futures::future::{Either, select};

use crate::meta::{MetaCommand, Metadata};
use crate::raft::{ProposeResult, RaftCore, RaftMsg, Role};

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
        env.spawn_task(drive(env.clone(), core));
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

/// The per-node driver loop: wait for the next message or timer, hand it to the
/// core, and ship whatever the core wants sent.
async fn drive<E: Env>(env: E, core: Arc<Mutex<RaftCore>>) {
    loop {
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

        for (to, msg) in outs {
            let bytes = serde_json::to_vec(&msg).expect("raft message serializes");
            env.send(to, bytes).await;
        }
    }
}
