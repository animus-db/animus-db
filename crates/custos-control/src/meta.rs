//! The control-plane metadata state machine: cluster membership and the tablet
//! map, mutated by replicated [`MetaCommand`]s.
//!
//! Tablet placement mutations are **compare-and-swap** transactions keyed by the
//! tablet's epoch (ADR 0002): a `CasTabletReplicas` applies only if the tablet's
//! current epoch equals the expected one, and on success bumps the epoch. Apply
//! is a deterministic pure function of the command and current state, so every
//! Raft replica computes the identical accept/reject decision.

use std::collections::BTreeMap;

use custos_env::NodeId;
use custos_tablet::{Epoch, KeyRange, Tablet, TabletId};
use serde::{Deserialize, Serialize};

/// Lifecycle status of a cluster member.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Bootstrapping, not yet serving.
    Joining,
    /// Live and serving.
    Active,
    /// Draining ahead of removal.
    Leaving,
    /// Believed dead.
    Down,
}

/// A cluster member: its topology labels (ADR 0005) and current status.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    /// Topology labels, e.g. `region=eu-west`.
    pub labels: BTreeMap<String, String>,
    /// Current lifecycle status.
    pub status: NodeStatus,
}

/// The replicated control-plane state: membership and the (single-table) tablet
/// map.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metadata {
    /// Cluster membership keyed by node id.
    pub members: BTreeMap<NodeId, Member>,
    /// The tablet map keyed by tablet id.
    pub tablets: BTreeMap<TabletId, Tablet>,
}

/// A mutation of [`Metadata`], replicated through Raft and applied in log order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaCommand {
    /// A no-op, used by a freshly elected leader to commit prior-term entries.
    NoOp,
    /// Insert or update a member.
    UpsertMember {
        node: NodeId,
        labels: BTreeMap<String, String>,
        status: NodeStatus,
    },
    /// Create a tablet (starting at [`Epoch::INITIAL`]). No-op if it exists.
    CreateTablet {
        tablet: TabletId,
        range: KeyRange,
        replicas: Vec<NodeId>,
    },
    /// Compare-and-swap a tablet's replica set: applies only if the tablet's
    /// epoch equals `expected_epoch`, then bumps the epoch.
    CasTabletReplicas {
        tablet: TabletId,
        expected_epoch: Epoch,
        replicas: Vec<NodeId>,
    },
    /// Split `tablet` at `split_key` into `[start, split_key)` (the original,
    /// with a bumped epoch) and `[split_key, end)` (a new tablet `new_id`,
    /// inheriting the replica set at [`Epoch::INITIAL`]). The split key must lie
    /// strictly inside the tablet's range.
    SplitTablet {
        tablet: TabletId,
        split_key: Vec<u8>,
        new_id: TabletId,
    },
    /// Merge adjacent tablets `left` and `right` (where `left.end == right.start`
    /// and they share a replica set) into `left`, extended to cover both ranges
    /// with a bumped epoch; `right` is removed.
    MergeTablets { left: TabletId, right: TabletId },
}

/// The deterministic result of applying a [`MetaCommand`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The command changed state.
    Applied,
    /// The command was a no-op by design.
    NoOp,
    /// The command's precondition failed; state is unchanged.
    Rejected(&'static str),
}

impl Metadata {
    /// Apply a command, returning the (deterministic) outcome.
    pub fn apply(&mut self, command: &MetaCommand) -> ApplyOutcome {
        match command {
            MetaCommand::NoOp => ApplyOutcome::NoOp,
            MetaCommand::UpsertMember {
                node,
                labels,
                status,
            } => {
                self.members.insert(
                    *node,
                    Member {
                        labels: labels.clone(),
                        status: *status,
                    },
                );
                ApplyOutcome::Applied
            }
            MetaCommand::CreateTablet {
                tablet,
                range,
                replicas,
            } => {
                if self.tablets.contains_key(tablet) {
                    ApplyOutcome::Rejected("tablet already exists")
                } else {
                    self.tablets.insert(
                        *tablet,
                        Tablet::new(*tablet, range.clone(), replicas.clone()),
                    );
                    ApplyOutcome::Applied
                }
            }
            MetaCommand::CasTabletReplicas {
                tablet,
                expected_epoch,
                replicas,
            } => match self.tablets.get_mut(tablet) {
                None => ApplyOutcome::Rejected("no such tablet"),
                Some(t) if t.epoch != *expected_epoch => ApplyOutcome::Rejected("epoch mismatch"),
                Some(t) => {
                    let mut replicas = replicas.clone();
                    replicas.sort_unstable();
                    replicas.dedup();
                    t.replicas = replicas;
                    t.epoch = t.epoch.next();
                    ApplyOutcome::Applied
                }
            },
            MetaCommand::SplitTablet {
                tablet,
                split_key,
                new_id,
            } => {
                if self.tablets.contains_key(new_id) {
                    return ApplyOutcome::Rejected("new tablet id already exists");
                }
                let Some(source) = self.tablets.get(tablet) else {
                    return ApplyOutcome::Rejected("no such tablet");
                };
                let Some((left, right)) = source.range.split_at(split_key) else {
                    return ApplyOutcome::Rejected("split key not strictly inside range");
                };
                let new_tablet = Tablet::new(*new_id, right, source.replicas.clone());
                let source = self.tablets.get_mut(tablet).expect("tablet present");
                source.range = left;
                source.epoch = source.epoch.next();
                self.tablets.insert(*new_id, new_tablet);
                ApplyOutcome::Applied
            }
            MetaCommand::MergeTablets { left, right } => {
                let (Some(l), Some(r)) = (self.tablets.get(left), self.tablets.get(right)) else {
                    return ApplyOutcome::Rejected("no such tablet");
                };
                if !l.range.abuts(&r.range) {
                    return ApplyOutcome::Rejected("tablets are not adjacent");
                }
                if l.replicas != r.replicas {
                    return ApplyOutcome::Rejected("tablets have different replica sets");
                }
                let new_end = r.range.end.clone();
                let l = self.tablets.get_mut(left).expect("tablet present");
                l.range.end = new_end;
                l.epoch = l.epoch.next();
                self.tablets.remove(right);
                ApplyOutcome::Applied
            }
        }
    }
}
