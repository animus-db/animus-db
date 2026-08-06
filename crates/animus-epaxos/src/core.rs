//! The synchronous, I/O-free EPaxos state machine.
//!
//! Mirrors `animus-consensus`'s `AccordCore` and the control plane's `RaftCore`:
//! all protocol logic lives here, the core returns outbound messages and durable
//! [`WalRecord`]s, and it never touches `Env`. The [`EPaxosNode`](crate::EPaxosNode)
//! driver does the I/O. Like `AccordCore` — and unlike `RaftCore` — it takes **no
//! clock and no randomness**: EPaxos orders by a dependency graph plus a sequence
//! number, so determinism rests purely on `BTreeMap`/`BTreeSet` iteration order.
//!
//! ## What this skeleton implements
//!
//! The steady-state agreement: a command leader proposes an instance
//! (`PreAccept`), replicas merge their own conflicting instances into the
//! attributes and reply, and the leader either commits directly (**fast path**,
//! when a fast quorum reports identical `(seq, deps)`) or runs an `Accept` round
//! adopting the max-`seq`/union-`deps` value (**slow path**) before `Commit`.
//! Every committed instance carries the same `(seq, deps)` on every replica, and
//! two conflicting commands always end up with a dependency edge between them
//! (the EPaxos quorum-intersection invariant).
//!
//! ## What is deliberately deferred (the "build onto" surface)
//!
//! - **Execution** — the Tarjan **SCC executor** that topologically sorts the
//!   committed dependency graph and orders within a cycle by `seq` (then instance
//!   id). This skeleton agrees the order but does not yet *run* commands against a
//!   `StorageEngine`.
//! - **Recovery** — the `Prepare`/`PrepareOk` sub-protocol that lets a replica
//!   take over a dead command leader's instance. This is EPaxos's hardest part
//!   (fast-path witness reasoning); until it lands, a dead leader strands its
//!   instance and the smaller fast quorum below is not yet fault-recoverable.
//! - **Message retry, failure detection, WAL snapshotting, arbitrary write
//!   values, read-only commands** — all present in `animus-consensus`; each slots
//!   in at this same sync-core boundary.

use std::collections::{BTreeMap, BTreeSet};

use animus_env::NodeId;

use crate::instance::InstanceId;
use crate::message::{EPaxosMsg, Out};
use crate::persist::{PersistedState, WalRecord};

/// A command key. A bare `u64` for now (the real system keys by partition/range),
/// matching `animus-consensus`. Conflict is intersecting key sets.
pub type Key = u64;

/// How far an instance has advanced at a replica. Ordered so a phase never
/// downgrades (`status.max(..)`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum Status {
    /// Never witnessed here.
    #[default]
    NotSeen,
    /// Witnessed via `PreAccept` (attributes may still change).
    PreAccepted,
    /// Attributes fixed by a slow-path `Accept`.
    Accepted,
    /// Final `(seq, deps)` agreed via `Commit`.
    Committed,
    /// Executed against the store (deferred — see the crate docs).
    Executed,
}

/// A decision this node reached **as command leader**: the final agreed
/// attributes for an instance it coordinated, and whether it took the fast path.
/// The observability handle the acceptance tests read (mirrors
/// `animus-consensus::Decision`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decision {
    /// The instance that was decided.
    pub instance: InstanceId,
    /// The agreed sequence number.
    pub seq: u64,
    /// The agreed dependency set.
    pub deps: BTreeSet<InstanceId>,
    /// Whether the decision was reached on the fast path (one round trip).
    pub fast_path: bool,
}

/// The replica-side view of one instance (what every node records for every
/// command, whether or not it leads it).
#[derive(Clone, Debug, Default)]
struct InstanceState {
    keys: BTreeSet<Key>,
    seq: u64,
    deps: BTreeSet<InstanceId>,
    status: Status,
}

/// The command-leader view of an instance this node is coordinating.
struct Coordinating {
    keys: BTreeSet<Key>,
    seq: u64,
    deps: BTreeSet<InstanceId>,
    phase: CoordPhase,
    /// PreAccept replies received (including this node's own seeded reply).
    preaccept_oks: BTreeMap<NodeId, (u64, BTreeSet<InstanceId>)>,
    /// Peers that have acked the slow-path `Accept` (including self).
    accept_oks: BTreeSet<NodeId>,
}

/// Where a coordinated instance is in the leader's own protocol.
enum CoordPhase {
    PreAccept,
    Accept,
    Done,
}

/// The EPaxos state machine for one node. Holds both the replica view (`instances`
/// — every command it has witnessed) and the coordinator view (`coordinating` —
/// commands it leads).
pub struct EPaxosCore {
    node: NodeId,
    peers: Vec<NodeId>,
    n: usize,
    /// This node's next instance slot.
    next_slot: u64,
    instances: BTreeMap<InstanceId, InstanceState>,
    coordinating: BTreeMap<InstanceId, Coordinating>,
    decisions: Vec<Decision>,
    pending: Vec<WalRecord>,
}

impl EPaxosCore {
    /// A fresh core for `node`, part of the `all_nodes` replica set (including
    /// itself).
    #[must_use]
    pub fn new(node: NodeId, all_nodes: &[NodeId]) -> EPaxosCore {
        let peers = all_nodes.iter().copied().filter(|&n| n != node).collect();
        EPaxosCore {
            node,
            peers,
            n: all_nodes.len(),
            next_slot: 0,
            instances: BTreeMap::new(),
            coordinating: BTreeMap::new(),
            decisions: Vec::new(),
            pending: Vec::new(),
        }
    }

    /// The EPaxos **fast-path quorum** `f + ⌊(f+1)/2⌋` (for `N = 2f+1`), floored at
    /// a majority so a single-node cluster still commits. This is the bound
    /// EPaxos is famous for — smaller than Fast Paxos's `⌈3N/4⌉` — but it is only
    /// *fault-recoverable* once the `Prepare` recovery sub-protocol lands (see the
    /// crate docs). Under the no-fault acceptance tests it changes only how many
    /// replies the leader waits for.
    fn fast_quorum(&self) -> usize {
        let f = (self.n - 1) / 2;
        (f + (f + 1) / 2).max(f + 1)
    }

    /// The classic majority quorum used by the slow-path `Accept`.
    fn slow_quorum(&self) -> usize {
        self.n / 2 + 1
    }

    /// Compute a command's attributes from the instances known here: `deps` is
    /// every conflicting instance (intersecting keys), and `seq` is one greater
    /// than the max `seq` among them. `exclude` skips the instance being computed
    /// (a command does not depend on itself).
    fn attrs_for(&self, keys: &BTreeSet<Key>, exclude: InstanceId) -> (u64, BTreeSet<InstanceId>) {
        let mut deps = BTreeSet::new();
        let mut max_seq = 0u64;
        for (id, st) in &self.instances {
            if *id == exclude || st.keys.is_disjoint(keys) {
                continue;
            }
            deps.insert(*id);
            max_seq = max_seq.max(st.seq);
        }
        (max_seq + 1, deps)
    }

    /// Submit a new command over `keys` for this node to coordinate. Mints an
    /// instance in this node's slot space, records it PreAccepted, and returns the
    /// instance id plus the outbound `PreAccept` burst (and, in a single-node
    /// cluster, the immediate `Commit`).
    pub fn submit(&mut self, keys: BTreeSet<Key>) -> (InstanceId, Vec<Out>) {
        let instance = InstanceId::new(self.node, self.next_slot);
        self.next_slot += 1;
        let (seq, deps) = self.attrs_for(&keys, instance);

        let st = self.instances.entry(instance).or_default();
        st.keys = keys.clone();
        st.seq = seq;
        st.deps = deps.clone();
        st.status = Status::PreAccepted;
        self.pending.push(WalRecord::PreAccepted {
            instance,
            keys: keys.clone(),
            seq,
            deps: deps.clone(),
        });

        let mut coord = Coordinating {
            keys: keys.clone(),
            seq,
            deps: deps.clone(),
            phase: CoordPhase::PreAccept,
            preaccept_oks: BTreeMap::new(),
            accept_oks: BTreeSet::new(),
        };
        // The leader is also a replica: seed its own PreAccept reply so it counts
        // toward the fast quorum (mirrors `AccordCore::submit`).
        coord.preaccept_oks.insert(self.node, (seq, deps.clone()));
        self.coordinating.insert(instance, coord);

        let mut outs: Vec<Out> = self
            .peers
            .iter()
            .map(|&p| {
                (
                    p,
                    EPaxosMsg::PreAccept {
                        instance,
                        keys: keys.clone(),
                        seq,
                        deps: deps.clone(),
                    },
                )
            })
            .collect();
        outs.extend(self.advance_coordinator(instance));
        (instance, outs)
    }

    /// Process an inbound message from `from`.
    pub fn handle(&mut self, from: NodeId, msg: EPaxosMsg) -> Vec<Out> {
        match msg {
            EPaxosMsg::PreAccept {
                instance,
                keys,
                seq,
                deps,
            } => self.replica_pre_accept(from, instance, keys, seq, deps),
            EPaxosMsg::PreAcceptOk {
                instance,
                seq,
                deps,
            } => {
                if let Some(coord) = self.coordinating.get_mut(&instance) {
                    if matches!(coord.phase, CoordPhase::PreAccept) {
                        coord.preaccept_oks.insert(from, (seq, deps));
                    }
                }
                self.advance_coordinator(instance)
            }
            EPaxosMsg::Accept {
                instance,
                keys,
                seq,
                deps,
            } => self.replica_accept(from, instance, keys, seq, deps),
            EPaxosMsg::AcceptOk { instance } => {
                if let Some(coord) = self.coordinating.get_mut(&instance) {
                    if matches!(coord.phase, CoordPhase::Accept) {
                        coord.accept_oks.insert(from);
                    }
                }
                self.advance_coordinator(instance)
            }
            EPaxosMsg::Commit {
                instance,
                keys,
                seq,
                deps,
            } => {
                self.replica_commit(instance, keys, seq, deps);
                Vec::new()
            }
        }
    }

    /// Replica handling of a `PreAccept`: merge the leader's proposal with this
    /// replica's own conflicting instances and reply with the merged attributes.
    fn replica_pre_accept(
        &mut self,
        from: NodeId,
        instance: InstanceId,
        keys: BTreeSet<Key>,
        seq: u64,
        deps: BTreeSet<InstanceId>,
    ) -> Vec<Out> {
        let (my_seq, my_deps) = self.attrs_for(&keys, instance);
        let merged_seq = seq.max(my_seq);
        let st = self.instances.entry(instance).or_default();
        if st.status < Status::Committed {
            st.keys.extend(keys.iter().copied());
            st.seq = st.seq.max(merged_seq);
            st.deps.extend(deps.iter().copied());
            st.deps.extend(my_deps);
            st.status = st.status.max(Status::PreAccepted);
        }
        let reply_seq = st.seq;
        let reply_deps = st.deps.clone();
        self.pending.push(WalRecord::PreAccepted {
            instance,
            keys,
            seq: reply_seq,
            deps: reply_deps.clone(),
        });
        vec![(
            from,
            EPaxosMsg::PreAcceptOk {
                instance,
                seq: reply_seq,
                deps: reply_deps,
            },
        )]
    }

    /// Replica handling of a slow-path `Accept`: adopt the coordinator's chosen
    /// `(seq, deps)` authoritatively and ack.
    fn replica_accept(
        &mut self,
        from: NodeId,
        instance: InstanceId,
        keys: BTreeSet<Key>,
        seq: u64,
        deps: BTreeSet<InstanceId>,
    ) -> Vec<Out> {
        let st = self.instances.entry(instance).or_default();
        if st.status < Status::Committed {
            st.keys.extend(keys.iter().copied());
            st.seq = seq;
            st.deps = deps.clone();
            st.status = st.status.max(Status::Accepted);
        }
        self.pending.push(WalRecord::Accepted {
            instance,
            keys,
            seq,
            deps,
        });
        vec![(from, EPaxosMsg::AcceptOk { instance })]
    }

    /// Replica handling of a `Commit`: record the final agreed attributes.
    fn replica_commit(
        &mut self,
        instance: InstanceId,
        keys: BTreeSet<Key>,
        seq: u64,
        deps: BTreeSet<InstanceId>,
    ) {
        let st = self.instances.entry(instance).or_default();
        st.keys.extend(keys.iter().copied());
        st.seq = seq;
        st.deps = deps.clone();
        st.status = Status::Committed;
        self.pending.push(WalRecord::Committed {
            instance,
            keys,
            seq,
            deps,
        });
    }

    /// Drive a coordinated instance forward based on the replies gathered so far:
    /// commit on the fast path, escalate to the slow path, or commit after the
    /// slow-path quorum acks. A no-op once decided.
    fn advance_coordinator(&mut self, instance: InstanceId) -> Vec<Out> {
        enum Next {
            Nothing,
            FastCommit {
                seq: u64,
                deps: BTreeSet<InstanceId>,
                keys: BTreeSet<Key>,
            },
            SlowAccept {
                seq: u64,
                deps: BTreeSet<InstanceId>,
                keys: BTreeSet<Key>,
            },
            SlowCommit {
                seq: u64,
                deps: BTreeSet<InstanceId>,
                keys: BTreeSet<Key>,
            },
        }

        let fast_quorum = self.fast_quorum();
        let slow_quorum = self.slow_quorum();
        let self_node = self.node;

        let next = {
            let Some(coord) = self.coordinating.get_mut(&instance) else {
                return Vec::new();
            };
            match coord.phase {
                CoordPhase::PreAccept if coord.preaccept_oks.len() >= fast_quorum => {
                    let all_agree = coord
                        .preaccept_oks
                        .values()
                        .all(|(s, d)| *s == coord.seq && *d == coord.deps);
                    if all_agree {
                        coord.phase = CoordPhase::Done;
                        Next::FastCommit {
                            seq: coord.seq,
                            deps: coord.deps.clone(),
                            keys: coord.keys.clone(),
                        }
                    } else {
                        // Slow path: adopt the max seq and union of deps across
                        // every reply (including this node's seeded one).
                        let mut seq = coord.seq;
                        let mut deps = coord.deps.clone();
                        for (s, d) in coord.preaccept_oks.values() {
                            seq = seq.max(*s);
                            deps.extend(d.iter().copied());
                        }
                        coord.seq = seq;
                        coord.deps = deps.clone();
                        coord.phase = CoordPhase::Accept;
                        coord.accept_oks.clear();
                        coord.accept_oks.insert(self_node);
                        Next::SlowAccept {
                            seq,
                            deps,
                            keys: coord.keys.clone(),
                        }
                    }
                }
                CoordPhase::Accept if coord.accept_oks.len() >= slow_quorum => {
                    coord.phase = CoordPhase::Done;
                    Next::SlowCommit {
                        seq: coord.seq,
                        deps: coord.deps.clone(),
                        keys: coord.keys.clone(),
                    }
                }
                _ => Next::Nothing,
            }
        };

        match next {
            Next::Nothing => Vec::new(),
            Next::FastCommit { seq, deps, keys } => {
                self.commit_as_leader(instance, keys, seq, deps, true)
            }
            Next::SlowCommit { seq, deps, keys } => {
                self.commit_as_leader(instance, keys, seq, deps, false)
            }
            Next::SlowAccept { seq, deps, keys } => {
                self.record_accepted(instance, keys.clone(), seq, deps.clone());
                self.peers
                    .iter()
                    .map(|&p| {
                        (
                            p,
                            EPaxosMsg::Accept {
                                instance,
                                keys: keys.clone(),
                                seq,
                                deps: deps.clone(),
                            },
                        )
                    })
                    .collect()
            }
        }
    }

    /// Record a leader-side commit: fix the replica state, log it, record the
    /// [`Decision`], and broadcast `Commit`.
    fn commit_as_leader(
        &mut self,
        instance: InstanceId,
        keys: BTreeSet<Key>,
        seq: u64,
        deps: BTreeSet<InstanceId>,
        fast_path: bool,
    ) -> Vec<Out> {
        let st = self.instances.entry(instance).or_default();
        st.keys.extend(keys.iter().copied());
        st.seq = seq;
        st.deps = deps.clone();
        st.status = Status::Committed;
        self.pending.push(WalRecord::Committed {
            instance,
            keys: keys.clone(),
            seq,
            deps: deps.clone(),
        });
        self.decisions.push(Decision {
            instance,
            seq,
            deps: deps.clone(),
            fast_path,
        });
        self.peers
            .iter()
            .map(|&p| {
                (
                    p,
                    EPaxosMsg::Commit {
                        instance,
                        keys: keys.clone(),
                        seq,
                        deps: deps.clone(),
                    },
                )
            })
            .collect()
    }

    /// Record a leader-side slow-path `Accept` in the local replica state + WAL.
    fn record_accepted(
        &mut self,
        instance: InstanceId,
        keys: BTreeSet<Key>,
        seq: u64,
        deps: BTreeSet<InstanceId>,
    ) {
        let st = self.instances.entry(instance).or_default();
        st.keys.extend(keys.iter().copied());
        st.seq = seq;
        st.deps = deps.clone();
        st.status = st.status.max(Status::Accepted);
        self.pending.push(WalRecord::Accepted {
            instance,
            keys,
            seq,
            deps,
        });
    }

    /// Rebuild the replica view from recovered durable state (the coordinator view
    /// is not recovered in this skeleton — see the crate docs).
    pub fn recovered(&mut self, state: PersistedState) {
        for (id, pi) in state.instances {
            if id.replica == self.node {
                self.next_slot = self.next_slot.max(id.slot + 1);
            }
            self.instances.insert(
                id,
                InstanceState {
                    keys: pi.keys,
                    seq: pi.seq,
                    deps: pi.deps,
                    status: pi.status,
                },
            );
        }
    }

    /// Hand the accumulated durable records to the driver (to fsync before acting).
    pub fn drain_persist(&mut self) -> Vec<WalRecord> {
        std::mem::take(&mut self.pending)
    }

    /// The decisions this node reached as command leader (observability).
    #[must_use]
    pub fn decisions(&self) -> &[Decision] {
        &self.decisions
    }

    /// The agreed `(seq, deps)` this replica recorded for `instance`, if it has
    /// reached the committed phase.
    #[must_use]
    pub fn committed_attrs(&self, instance: InstanceId) -> Option<(u64, BTreeSet<InstanceId>)> {
        let st = self.instances.get(&instance)?;
        (st.status >= Status::Committed).then(|| (st.seq, st.deps.clone()))
    }

    /// The phase this replica has reached for `instance` (`NotSeen` if unknown).
    #[must_use]
    pub fn status(&self, instance: InstanceId) -> Status {
        self.instances
            .get(&instance)
            .map(|st| st.status)
            .unwrap_or_default()
    }
}
