//! Per-shard Accord consensus: **one consensus group per tablet** (ADR 0011,
//! the per-shard-consensus slice).
//!
//! The earlier sharding slice (`AccordNode::start_with_router`) ran **one global
//! Accord replica set** over the whole key space and sharded only the *execution
//! effect* per tablet. This module flips that axis: a tablet's replica set **is**
//! its own Accord group, so the keyspace partitions into independent consensus
//! groups and a transaction is routed to the group(s) that own its keys.
//!
//! ## What lives here
//!
//! - [`ShardRouter`]: maps an Accord [`Key`] to the [`Tablet`] (id + replica set)
//!   that owns it, derived from the **existing tablet map** (`animus-tablet`) — no
//!   new control-plane state. It groups a transaction's keys into one per-tablet
//!   slice.
//! - [`ShardedOwner`]: what a *physical* node runs. A node typically replicates
//!   several tablets, so it hosts **one [`AccordNode`] per local shard** (one per
//!   tablet whose replica set includes this node), each on its **own** `Env`
//!   node-id — a distinct inbox and a distinct WAL, because a node's inbox is
//!   single-consumer (ADR 0001 cross-cutting gotcha) and two groups must not share
//!   one.
//!
//! ## Single- vs cross-shard transactions
//!
//! - **Single-shard** (the common case): every key falls in one tablet. The
//!   transaction is submitted to **that group only**; every other group is
//!   untouched, so a fault on an unrelated shard cannot stall it. This is just
//!   [`AccordNode::submit`] on the owning group.
//! - **Cross-shard**: the key set spans more than one tablet. The transaction is
//!   split into one per-tablet **slice** and each slice is submitted to its own
//!   group as a sub-transaction. Each group orders its slice against every other
//!   transaction touching its keys — so two conflicting cross-shard transactions
//!   (which must share at least one key, hence one common tablet) are **serialized
//!   by that shared group**, and every key's data is owned by exactly one group,
//!   so there is no torn write across shards once all slices commit. The
//!   coordinator ([`CrossShardTxn`]) awaits **all** slices applying, giving the
//!   transaction all-or-nothing visibility. See ADR 0011 for the precise guarantee
//!   and the deferred unified-global-timestamp atomic-commit protocol.
//!
//! The sync `AccordCore` is **unchanged**: per-shard consensus is a *driver*-level
//! composition of existing per-group `AccordNode`s. Each group keeps the full
//! Accord machinery (fast/slow path, durability, recovery ballots, the
//! failure-detector tick), so a stranded slice on one shard is recovered by that
//! shard's own nominee without touching the others.

use std::collections::{BTreeMap, BTreeSet};

use animus_env::{Env, NodeId};
use animus_storage::{MemoryEngine, StorageEngine};
use animus_tablet::{Tablet, TabletId};

use crate::core::{Key, TxnId};
use crate::node::AccordNode;

/// Maps an Accord [`Key`] to the tablet (consensus group) that owns it.
///
/// Derived from the **existing tablet map** — the same `Vec<Tablet>` the control
/// plane's `Metadata` holds — so per-shard consensus adds **no** new control-plane
/// state (ADR 0011 / ADR 0001: the tablet map is the single source of placement
/// truth). The tablets
/// are expected to partition the keyspace into disjoint ranges (the control plane
/// maintains this via split/merge).
///
/// An Accord `Key` (a `u64`) is mapped to storage-key bytes the same way
/// [`AccordNode`] stores them — big-endian — so this router and the node's own
/// data-plane routing agree on which tablet a key belongs to.
#[derive(Clone, Debug)]
pub struct ShardRouter {
    tablets: Vec<Tablet>,
}

/// The storage-key bytes for an Accord [`Key`], matching the node's internal
/// encoding (big-endian, so numeric order == byte order).
fn storage_key(key: Key) -> Vec<u8> {
    key.to_be_bytes().to_vec()
}

impl ShardRouter {
    /// Build a shard router over a tablet map. The tablets should partition the
    /// keyspace into disjoint ranges.
    #[must_use]
    pub fn new(tablets: Vec<Tablet>) -> ShardRouter {
        ShardRouter { tablets }
    }

    /// The tablet owning `key`, or `None` if no tablet covers it.
    #[must_use]
    pub fn tablet_for(&self, key: Key) -> Option<&Tablet> {
        let sk = storage_key(key);
        self.tablets.iter().find(|t| t.range.contains(&sk))
    }

    /// The id of the tablet (consensus group) owning `key`, if any.
    #[must_use]
    pub fn group_for(&self, key: Key) -> Option<TabletId> {
        self.tablet_for(key).map(|t| t.id)
    }

    /// All tablets in the map (the full set of consensus groups).
    #[must_use]
    pub fn tablets(&self) -> &[Tablet] {
        &self.tablets
    }

    /// The tablet by id, if present.
    #[must_use]
    pub fn tablet(&self, id: TabletId) -> Option<&Tablet> {
        self.tablets.iter().find(|t| t.id == id)
    }

    /// Group a transaction's `keys` into one per-tablet slice: `tablet id → the
    /// subset of `keys` that tablet owns`. A key no tablet covers is dropped (it
    /// belongs to no group). A single-shard transaction yields exactly one entry;
    /// a cross-shard transaction yields several.
    #[must_use]
    pub fn slices(&self, keys: &BTreeSet<Key>) -> BTreeMap<TabletId, BTreeSet<Key>> {
        let mut out: BTreeMap<TabletId, BTreeSet<Key>> = BTreeMap::new();
        for &key in keys {
            if let Some(t) = self.tablet_for(key) {
                out.entry(t.id).or_default().insert(key);
            }
        }
        out
    }

    /// Group a transaction's `(key → value)` write map into one per-tablet slice.
    /// Mirrors [`ShardRouter::slices`] but carries the values, for value-bearing
    /// cross-shard writes.
    #[must_use]
    pub fn value_slices(
        &self,
        writes: &BTreeMap<Key, Vec<u8>>,
    ) -> BTreeMap<TabletId, BTreeMap<Key, Vec<u8>>> {
        let mut out: BTreeMap<TabletId, BTreeMap<Key, Vec<u8>>> = BTreeMap::new();
        for (&key, value) in writes {
            if let Some(t) = self.tablet_for(key) {
                out.entry(t.id).or_default().insert(key, value.clone());
            }
        }
        out
    }
}

/// What a **physical node** runs to participate in per-shard consensus: one
/// [`AccordNode`] per **local shard** — every tablet whose replica set includes
/// this node — keyed by [`TabletId`].
///
/// A node typically replicates several tablets, and each tablet's group is an
/// independent Accord replica set. Because a node's network inbox is
/// single-consumer (ADR 0001), each local group must run on a **distinct `Env`
/// node-id** (a distinct inbox and a distinct `accord.wal`). The caller supplies a
/// function mapping `(physical node, TabletId) → Env` so the test/production wiring
/// owns the id-allocation policy; this type only composes the groups.
///
/// The owner is the routing front-end: [`ShardedOwner::submit`] routes a
/// transaction to the owning group (single-shard) or splits it across groups
/// (cross-shard), and [`ShardedOwner::group`] exposes a local group for direct
/// inspection (tests assert per-group execution/order).
pub struct ShardedOwner<E: Env, S: StorageEngine = MemoryEngine> {
    /// This physical node's id (for documentation/inspection; the per-group
    /// `AccordNode`s each carry their own group `Env` id).
    node: NodeId,
    router: ShardRouter,
    /// The local consensus groups, one per tablet this node replicates.
    groups: BTreeMap<TabletId, AccordNode<E, S>>,
}

impl<E: Env, S: StorageEngine + 'static> Clone for ShardedOwner<E, S> {
    fn clone(&self) -> Self {
        ShardedOwner {
            node: self.node,
            router: self.router.clone(),
            groups: self.groups.clone(),
        }
    }
}

impl<E: Env> ShardedOwner<E, MemoryEngine> {
    /// Start the per-shard consensus groups this `node` participates in, each
    /// backed by a fresh in-memory [`MemoryEngine`].
    ///
    /// `group_env(tablet)` must return an `Env` whose node-id is **distinct** for
    /// each local group (and from every other role's id), since a node's inbox is
    /// single-consumer. `group_env` is called once per tablet this `node`
    /// replicates.
    pub fn start(
        node: NodeId,
        router: ShardRouter,
        mut group_env: impl FnMut(TabletId) -> E,
    ) -> ShardedOwner<E, MemoryEngine> {
        Self::start_with(node, router, |tablet, all_nodes| {
            AccordNode::start(group_env(tablet), all_nodes)
        })
    }
}

impl<E: Env, S: StorageEngine + 'static> ShardedOwner<E, S> {
    /// Start the per-shard consensus groups this `node` participates in, building
    /// each group's [`AccordNode`] with `make_group(tablet, replica_set)`.
    ///
    /// One group is started for **every tablet whose replica set includes
    /// `node`** — those are exactly this physical node's local shards. The
    /// `replica_set` passed to `make_group` is the tablet's replica node-ids (the
    /// Accord `all_nodes` for that group). The closure owns how each group's
    /// `AccordNode` is wired (plain, storage-backed, or frontier/data-plane), so
    /// this composer stays agnostic to the execution substrate.
    pub fn start_with(
        node: NodeId,
        router: ShardRouter,
        mut make_group: impl FnMut(TabletId, Vec<NodeId>) -> AccordNode<E, S>,
    ) -> ShardedOwner<E, S> {
        let mut groups = BTreeMap::new();
        for tablet in router.tablets() {
            if tablet.has_replica(node) {
                let group = make_group(tablet.id, tablet.replicas.clone());
                groups.insert(tablet.id, group);
            }
        }
        ShardedOwner {
            node,
            router,
            groups,
        }
    }

    /// This physical node's id.
    #[must_use]
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// The shard router this owner uses.
    #[must_use]
    pub fn router(&self) -> &ShardRouter {
        &self.router
    }

    /// The ids of the local shards (tablets) this node hosts a group for.
    #[must_use]
    pub fn local_groups(&self) -> Vec<TabletId> {
        self.groups.keys().copied().collect()
    }

    /// The local [`AccordNode`] for tablet `id`, if this node replicates it.
    #[must_use]
    pub fn group(&self, id: TabletId) -> Option<&AccordNode<E, S>> {
        self.groups.get(&id)
    }

    /// Submit a **write** transaction over `keys`, routed by tablet.
    ///
    /// - If every key falls in a single tablet this node replicates, the
    ///   transaction is submitted to **that group only** (single-shard, the common
    ///   case) and the returned [`ShardedTxn`] names that one group's local txn id.
    /// - If the keys span more than one tablet, the transaction is split into one
    ///   per-tablet slice and each slice this node replicates is submitted to its
    ///   own group (cross-shard); the returned handle names every per-group txn id.
    ///
    /// A returned handle lets a caller await/inspect all the per-group slices that
    /// make up the transaction (see [`ShardedTxn`]). Slices for tablets this node
    /// does *not* replicate are not submitted here — the coordinator must run on a
    /// node that replicates the relevant shards, or use [`ShardedOwner::submit_on`]
    /// against a cluster of owners (tests drive every owner).
    ///
    /// Returns an error if a key routes to no tablet, or to a tablet this node does
    /// not replicate (so the caller cannot silently lose a slice).
    pub fn submit(&self, keys: BTreeSet<Key>) -> Result<ShardedTxn, ShardError> {
        let slices = self.checked_slices(&keys)?;
        let mut parts = BTreeMap::new();
        for (tablet, slice) in slices {
            let group = self.group(tablet).expect("checked above");
            parts.insert(tablet, group.submit(slice));
        }
        Ok(ShardedTxn { parts })
    }

    /// Submit a **value-carrying write** transaction (arbitrary write values, ADR
    /// 0011), routed by tablet. Like [`ShardedOwner::submit`] but each key carries
    /// explicit bytes; the per-tablet slices carry only their own keys' values.
    pub fn submit_writes(&self, writes: BTreeMap<Key, Vec<u8>>) -> Result<ShardedTxn, ShardError> {
        let keys: BTreeSet<Key> = writes.keys().copied().collect();
        // Validate routing/replication for every key before submitting any slice.
        self.checked_slices(&keys)?;
        let slices = self.router.value_slices(&writes);
        let mut parts = BTreeMap::new();
        for (tablet, slice) in slices {
            let group = self.group(tablet).expect("validated above");
            parts.insert(tablet, group.submit_writes(slice));
        }
        Ok(ShardedTxn { parts })
    }

    /// Submit a **read-only** transaction over `keys`, routed by tablet. Each
    /// owning group orders and executes its slice as a read; the returned handle
    /// names the per-group read txn ids so the caller can collect each shard's
    /// observation.
    pub fn submit_read(&self, keys: BTreeSet<Key>) -> Result<ShardedTxn, ShardError> {
        let slices = self.checked_slices(&keys)?;
        let mut parts = BTreeMap::new();
        for (tablet, slice) in slices {
            let group = self.group(tablet).expect("checked above");
            parts.insert(tablet, group.submit_read(slice));
        }
        Ok(ShardedTxn { parts })
    }

    /// Whether every per-group slice of `txn` has applied on this node's local
    /// groups. For a single-shard transaction this is just the one group; for a
    /// cross-shard transaction it is **all** local slices — the all-or-nothing
    /// visibility point (a slice on a tablet this node does not replicate is not
    /// observed here).
    #[must_use]
    pub fn is_applied(&self, txn: &ShardedTxn) -> bool {
        txn.parts
            .iter()
            .all(|(tablet, id)| self.group(*tablet).is_some_and(|g| g.is_applied(*id)))
    }

    /// Resolve and validate the per-tablet slices of `keys`: every key must route
    /// to a tablet, and every involved tablet must be one this node replicates.
    fn checked_slices(
        &self,
        keys: &BTreeSet<Key>,
    ) -> Result<BTreeMap<TabletId, BTreeSet<Key>>, ShardError> {
        for &key in keys {
            match self.router.group_for(key) {
                None => return Err(ShardError::Unrouted(key)),
                Some(t) if !self.groups.contains_key(&t) => {
                    return Err(ShardError::NotLocal { key, tablet: t });
                }
                Some(_) => {}
            }
        }
        Ok(self.router.slices(keys))
    }
}

/// A handle to a transaction routed across one or more shards. Names the per-group
/// (per-tablet) sub-transaction ids so a caller can await/inspect each shard's
/// progress. A single-shard transaction has exactly one part; a cross-shard
/// transaction has one per involved tablet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardedTxn {
    /// The per-tablet sub-transaction id (each group mints its own `t0`).
    parts: BTreeMap<TabletId, TxnId>,
}

impl ShardedTxn {
    /// The per-group (tablet → local txn id) parts of this transaction.
    #[must_use]
    pub fn parts(&self) -> &BTreeMap<TabletId, TxnId> {
        &self.parts
    }

    /// The local sub-transaction id on tablet `id`, if this transaction touches it.
    #[must_use]
    pub fn part(&self, id: TabletId) -> Option<TxnId> {
        self.parts.get(&id).copied()
    }

    /// Whether this transaction spans more than one shard.
    #[must_use]
    pub fn is_cross_shard(&self) -> bool {
        self.parts.len() > 1
    }

    /// The tablets (groups) this transaction touches.
    #[must_use]
    pub fn tablets(&self) -> Vec<TabletId> {
        self.parts.keys().copied().collect()
    }
}

/// Why a sharded submit could not be routed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardError {
    /// A key falls in no tablet's range (the tablet map does not cover it).
    Unrouted(Key),
    /// A key routes to a tablet this node does not replicate, so this owner
    /// cannot coordinate that slice locally.
    NotLocal {
        /// The offending key.
        key: Key,
        /// The tablet it routes to.
        tablet: TabletId,
    },
}

impl std::fmt::Display for ShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardError::Unrouted(key) => write!(f, "key {key} routes to no tablet"),
            ShardError::NotLocal { key, tablet } => {
                write!(
                    f,
                    "key {key} routes to tablet {tablet:?} this node does not replicate"
                )
            }
        }
    }
}

impl std::error::Error for ShardError {}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_tablet::{Epoch, KeyRange, Tablet, TabletId};

    fn sk(key: Key) -> Vec<u8> {
        key.to_be_bytes().to_vec()
    }

    fn two_tablets() -> Vec<Tablet> {
        let split = sk(1000);
        vec![
            Tablet::new(
                TabletId(1),
                KeyRange::new(Vec::new(), Some(split.clone())),
                vec![3, 4],
            ),
            Tablet::new(TabletId(2), KeyRange::new(split, None), vec![4, 5]),
        ]
    }

    #[test]
    fn routes_keys_to_tablets() {
        let r = ShardRouter::new(two_tablets());
        assert_eq!(r.group_for(5), Some(TabletId(1)));
        assert_eq!(r.group_for(5000), Some(TabletId(2)));
        assert_eq!(r.group_for(999), Some(TabletId(1)));
        assert_eq!(r.group_for(1000), Some(TabletId(2)));
    }

    #[test]
    fn unrouted_key_when_no_tablet_covers() {
        // A map with a hole above 1000.
        let r = ShardRouter::new(vec![Tablet::new(
            TabletId(1),
            KeyRange::new(Vec::new(), Some(sk(1000))),
            vec![3, 4],
        )]);
        assert_eq!(r.group_for(5), Some(TabletId(1)));
        assert_eq!(r.group_for(5000), None);
    }

    #[test]
    fn slices_split_a_cross_shard_key_set() {
        let r = ShardRouter::new(two_tablets());
        let slices = r.slices(&[5, 6, 5000].into_iter().collect());
        assert_eq!(slices.len(), 2);
        assert_eq!(slices[&TabletId(1)], [5, 6].into_iter().collect());
        assert_eq!(slices[&TabletId(2)], [5000].into_iter().collect());
    }

    #[test]
    fn single_shard_key_set_yields_one_slice() {
        let r = ShardRouter::new(two_tablets());
        let slices = r.slices(&[5, 6, 7].into_iter().collect());
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[&TabletId(1)], [5, 6, 7].into_iter().collect());
    }

    #[test]
    fn value_slices_carry_values_per_tablet() {
        let r = ShardRouter::new(two_tablets());
        let writes: BTreeMap<Key, Vec<u8>> =
            [(5u64, vec![1]), (5000u64, vec![2])].into_iter().collect();
        let slices = r.value_slices(&writes);
        assert_eq!(slices[&TabletId(1)][&5], vec![1]);
        assert_eq!(slices[&TabletId(2)][&5000], vec![2]);
    }

    #[test]
    fn epoch_in_tablet_does_not_affect_routing() {
        // A defensive check that routing ignores epoch (placement version).
        let mut tablets = two_tablets();
        tablets[0].epoch = Epoch(7);
        let r = ShardRouter::new(tablets);
        assert_eq!(r.group_for(5), Some(TabletId(1)));
    }
}
