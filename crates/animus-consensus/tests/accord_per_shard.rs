//! ADR 0011 **per-shard consensus** acceptance tests.
//!
//! The earlier sharding slice (`accord_sharded.rs`) ran **one global Accord
//! replica set** and sharded only the execution *effect* per tablet. This file
//! exercises the new axis: **one Accord consensus group per tablet** — a tablet's
//! replica set *is* its own Accord group. A transaction routes to the group(s)
//! owning its keys; a single-shard transaction touches only its group; a
//! cross-shard transaction splits into one slice per involved group.
//!
//! ## Topology (every group is a full, independent Accord replica set)
//!
//! Two tablets split at the (big-endian `u64`) key `1000`:
//!
//! - **Group A** = tablet 1, range `[_, 1000)`, physical replicas `{0, 1, 2}`.
//! - **Group B** = tablet 2, range `[1000, _)`, physical replicas `{2, 3, 4}`.
//!
//! Physical node `2` replicates **both** tablets, so it can coordinate a
//! cross-shard transaction over the two groups locally.
//!
//! A node's inbox is single-consumer (ADR 0001), so each `(physical node, tablet)`
//! group runs on its **own** `Env` node-id: `group_env_id(p, t) = 1000*t + p`. So
//! node 2 runs two `AccordNode`s on ids `1002` (group A) and `2002` (group B) —
//! distinct inboxes and distinct WALs. A group's Accord `all_nodes` is the env-ids
//! of *all* physical replicas of that tablet.
//!
//! Every run is byte-reproducible from its seed.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_consensus::{Key, ShardRouter, ShardedOwner, ShardedTxn, TxnId};
use animus_sim::{SimEnv, Simulator};
use animus_tablet::{KeyRange, Tablet, TabletId};

/// The tablet-split boundary: keys `< SPLIT` → tablet 1 (group A), keys `>= SPLIT`
/// → tablet 2 (group B).
const SPLIT: Key = 1000;

/// Physical replica sets per tablet.
const GROUP_A: [u64; 3] = [0, 1, 2];
const GROUP_B: [u64; 3] = [2, 3, 4];

fn keys(ks: &[Key]) -> BTreeSet<Key> {
    ks.iter().copied().collect()
}

fn sk(key: Key) -> Vec<u8> {
    key.to_be_bytes().to_vec()
}

/// The two tablets partitioning the keyspace at [`SPLIT`].
fn tablets() -> Vec<Tablet> {
    let boundary = sk(SPLIT);
    vec![
        Tablet::new(
            TabletId(1),
            KeyRange::new(Vec::new(), Some(boundary.clone())),
            GROUP_A.to_vec(),
        ),
        Tablet::new(TabletId(2), KeyRange::new(boundary, None), GROUP_B.to_vec()),
    ]
}

/// The `Env` node-id for the consensus group of `tablet` running on physical node
/// `physical`. Distinct per `(physical, tablet)` so each local group has its own
/// single-consumer inbox + WAL.
fn group_env_id(physical: u64, tablet: TabletId) -> u64 {
    1000 * tablet.0 + physical
}

/// The Accord `all_nodes` for `tablet`'s group: the env-ids of every physical
/// replica of that tablet.
fn group_all_nodes(router: &ShardRouter, tablet: TabletId) -> Vec<u64> {
    router
        .tablet(tablet)
        .expect("tablet exists")
        .replicas
        .iter()
        .map(|&p| group_env_id(p, tablet))
        .collect()
}

/// Stand up the whole cluster: a [`ShardedOwner`] per physical node, each hosting
/// one `AccordNode` per local shard on its own env id. Returns the simulator, the
/// owners keyed by physical node id, and the shared router.
fn cluster(seed: u64) -> (Simulator, BTreeMap<u64, ShardedOwner<SimEnv>>, ShardRouter) {
    let sim = Simulator::new(seed);
    let router = ShardRouter::new(tablets());

    // The set of all physical nodes across both groups.
    let physical: BTreeSet<u64> = GROUP_A.iter().chain(GROUP_B.iter()).copied().collect();

    let mut owners = BTreeMap::new();
    for &p in &physical {
        let owner = ShardedOwner::start_with(p, router.clone(), |tablet, _physical_replicas| {
            // This group's Accord replica set is the env-ids of the tablet's
            // physical replicas; this node's group runs on its own env id.
            let all = group_all_nodes(&router, tablet);
            let env = sim.env(group_env_id(p, tablet));
            animus_consensus::AccordNode::start(env, all)
        });
        owners.insert(p, owner);
    }
    (sim, owners, router)
}

/// The agreed execution order of two sub-transactions on a group, restricted to
/// the two ids and as observed by one group `AccordNode`.
fn order_on(node: &animus_consensus::AccordNode<SimEnv>, a: TxnId, b: TxnId) -> Vec<TxnId> {
    node.applied_order()
        .into_iter()
        .filter(|t| *t == a || *t == b)
        .collect()
}

// ---------------------------------------------------------------------------
// Single-shard transactions (the common case): routed to the owning group only.
// ---------------------------------------------------------------------------

/// A single-shard transaction executes on **its owning group only** — every
/// replica of that group applies it, and the *other* group is wholly untouched
/// (no sub-transaction, nothing applied there).
#[test]
fn single_shard_txn_executes_on_owning_group_only() {
    let seed = 0x5121_0001;
    let (mut sim, owners, _router) = cluster(seed);

    // Keys 5 and 6 both live in tablet 1 (group A). Coordinate from node 0.
    let txn = owners[&0]
        .submit(keys(&[5, 6]))
        .expect("keys are local to node 0's group A");
    assert!(
        !txn.is_cross_shard(),
        "single-tablet txn must be single-shard"
    );
    assert_eq!(txn.tablets(), vec![TabletId(1)], "routed to tablet 1 only");
    let part = txn.part(TabletId(1)).unwrap();

    sim.run_for(Duration::from_secs(2));

    // Every physical replica of group A executed the sub-transaction.
    for &p in &GROUP_A {
        let g = owners[&p].group(TabletId(1)).expect("group A local");
        assert!(
            g.is_applied(part),
            "group A node {p} did not execute the single-shard txn (seed={seed})"
        );
    }

    // Group B is wholly untouched: the txn never reached it, nothing applied.
    for &p in &GROUP_B {
        let g = owners[&p].group(TabletId(2)).expect("group B local");
        assert!(
            g.applied_order().is_empty(),
            "group B node {p} executed something for an unrelated single-shard txn (seed={seed})"
        );
    }

    // The written key carries the sub-transaction id on every group-A replica.
    for &p in &GROUP_A {
        let g = owners[&p].group(TabletId(1)).unwrap().clone();
        let w = futures::executor::block_on(g.store_writer(5));
        assert_eq!(
            w,
            Some(part),
            "group A node {p} key 5 not written by the txn (seed={seed})"
        );
    }
}

/// Two single-shard transactions on **different** tablets are fully independent:
/// each executes on its own group and neither appears in the other group's order.
#[test]
fn single_shard_txns_on_distinct_tablets_are_independent() {
    let seed = 0x5121_0002;
    let (mut sim, owners, _router) = cluster(seed);

    // a → tablet 1 (coordinate from node 1, a replica of group A).
    // b → tablet 2 (coordinate from node 3, a replica of group B).
    let a = owners[&1].submit(keys(&[10])).expect("local to group A");
    let b = owners[&3].submit(keys(&[2000])).expect("local to group B");
    let a_part = a.part(TabletId(1)).unwrap();
    let b_part = b.part(TabletId(2)).unwrap();

    sim.run_for(Duration::from_secs(2));

    for &p in &GROUP_A {
        let g = owners[&p].group(TabletId(1)).unwrap();
        assert!(
            g.is_applied(a_part),
            "group A node {p} missing a (seed={seed})"
        );
        assert!(
            !g.applied_order().contains(&b_part),
            "group A node {p} saw b — leaked across shards (seed={seed})"
        );
    }
    for &p in &GROUP_B {
        let g = owners[&p].group(TabletId(2)).unwrap();
        assert!(
            g.is_applied(b_part),
            "group B node {p} missing b (seed={seed})"
        );
        assert!(
            !g.applied_order().contains(&a_part),
            "group B node {p} saw a — leaked across shards (seed={seed})"
        );
    }
}

/// Submitting a key the coordinator's node does not replicate is rejected, not
/// silently dropped: node 0 (group A only) cannot coordinate a tablet-2 key.
#[test]
fn submit_rejects_non_local_shard() {
    let seed = 0x5121_0003;
    let (_sim, owners, _router) = cluster(seed);

    let err = owners[&0].submit(keys(&[2000])).unwrap_err();
    assert!(
        matches!(
            err,
            animus_consensus::ShardError::NotLocal {
                tablet: TabletId(2),
                ..
            }
        ),
        "expected NotLocal for a tablet-2 key on a group-A-only node, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Cross-shard transactions: split into per-group slices, coordinated together.
// ---------------------------------------------------------------------------

/// A cross-shard transaction commits **atomically across both groups**: node 2
/// (a replica of both tablets) coordinates a transaction over a tablet-1 key and a
/// tablet-2 key; both slices commit, every replica of each group applies its
/// slice, and the two shards agree (each key carries its own slice's id).
#[test]
fn cross_shard_txn_commits_atomically_on_both_groups() {
    let seed = 0x5121_0004;
    let (mut sim, owners, _router) = cluster(seed);

    // 5 → group A, 5000 → group B. Node 2 replicates both, so it coordinates.
    let txn = owners[&2]
        .submit(keys(&[5, 5000]))
        .expect("node 2 replicates both tablets");
    assert!(txn.is_cross_shard(), "must span two shards");
    assert_eq!(txn.tablets(), vec![TabletId(1), TabletId(2)]);
    let a_part = txn.part(TabletId(1)).unwrap();
    let b_part = txn.part(TabletId(2)).unwrap();

    sim.run_for(Duration::from_secs(3));

    // The coordinator sees both slices applied — the all-or-nothing point.
    assert!(
        owners[&2].is_applied(&txn),
        "coordinator did not see both slices apply (seed={seed})"
    );

    // Every replica of group A applied the tablet-1 slice; every replica of group
    // B applied the tablet-2 slice.
    for &p in &GROUP_A {
        let g = owners[&p].group(TabletId(1)).unwrap();
        assert!(
            g.is_applied(a_part),
            "group A node {p} missing slice (seed={seed})"
        );
    }
    for &p in &GROUP_B {
        let g = owners[&p].group(TabletId(2)).unwrap();
        assert!(
            g.is_applied(b_part),
            "group B node {p} missing slice (seed={seed})"
        );
    }

    // Each shard's key carries its own slice id, on every replica of that shard.
    for &p in &GROUP_A {
        let g = owners[&p].group(TabletId(1)).unwrap().clone();
        assert_eq!(
            futures::executor::block_on(g.store_writer(5)),
            Some(a_part),
            "group A node {p} key 5 not the cross-shard slice (seed={seed})"
        );
    }
    for &p in &GROUP_B {
        let g = owners[&p].group(TabletId(2)).unwrap().clone();
        assert_eq!(
            futures::executor::block_on(g.store_writer(5000)),
            Some(b_part),
            "group B node {p} key 5000 not the cross-shard slice (seed={seed})"
        );
    }
}

/// Two conflicting cross-shard transactions (sharing a key in group A) **serialize
/// via the shared group**: group A orders the two shared-key slices consistently
/// on every group-A replica, and the shared key ends up carrying the
/// second-ordered transaction's slice. Each transaction's private (group-B) key
/// carries its own slice — no torn write set.
#[test]
fn conflicting_cross_shard_txns_serialize_via_shared_group() {
    let seed = 0x5121_0005;
    let (mut sim, owners, _router) = cluster(seed);

    // Both touch shared key 7 (group A); a also touches 7000, b also 8000 (group
    // B). Both coordinated from node 2 (replicates both tablets).
    let a = owners[&2]
        .submit(keys(&[7, 7000]))
        .expect("local to node 2");
    let b = owners[&2]
        .submit(keys(&[7, 8000]))
        .expect("local to node 2");
    let a_a = a.part(TabletId(1)).unwrap();
    let b_a = b.part(TabletId(1)).unwrap();
    let a_b = a.part(TabletId(2)).unwrap();
    let b_b = b.part(TabletId(2)).unwrap();

    sim.run_for(Duration::from_secs(5));

    // Group A serialized the two shared-key slices the same way on every replica.
    let reference = {
        let g = owners[&0].group(TabletId(1)).unwrap();
        order_on(g, a_a, b_a)
    };
    assert_eq!(
        reference.len(),
        2,
        "both shared-key slices must execute (seed={seed})"
    );
    for &p in &GROUP_A {
        let g = owners[&p].group(TabletId(1)).unwrap();
        assert_eq!(
            order_on(g, a_a, b_a),
            reference,
            "group A node {p} diverged on the shared-key order (seed={seed})"
        );
    }
    let winner_a = *reference.last().unwrap();

    // The shared key carries the second-ordered slice on every group-A replica.
    for &p in &GROUP_A {
        let g = owners[&p].group(TabletId(1)).unwrap().clone();
        assert_eq!(
            futures::executor::block_on(g.store_writer(7)),
            Some(winner_a),
            "group A node {p} shared key 7 not the second-ordered slice (seed={seed})"
        );
    }

    // Private keys: each carries its own transaction's slice, on every group-B
    // replica (no torn write set across shards).
    for &p in &GROUP_B {
        let g = owners[&p].group(TabletId(2)).unwrap().clone();
        assert_eq!(
            futures::executor::block_on(g.clone().store_writer(7000)),
            Some(a_b),
            "group B node {p} key 7000 torn from a's write set (seed={seed})"
        );
        assert_eq!(
            futures::executor::block_on(g.store_writer(8000)),
            Some(b_b),
            "group B node {p} key 8000 torn from b's write set (seed={seed})"
        );
    }
}

/// Arbitrary write values survive cross-shard routing: each shard's key is written
/// with the caller-supplied bytes for that key.
#[test]
fn cross_shard_values_route_per_shard() {
    let seed = 0x5121_0006;
    let (mut sim, owners, _router) = cluster(seed);

    let writes: BTreeMap<Key, Vec<u8>> = [(5u64, b"alpha".to_vec()), (5000u64, b"omega".to_vec())]
        .into_iter()
        .collect();
    let txn = owners[&2]
        .submit_writes(writes)
        .expect("node 2 replicates both");

    sim.run_for(Duration::from_secs(3));

    assert!(
        owners[&2].is_applied(&txn),
        "both value slices must apply (seed={seed})"
    );
    for &p in &GROUP_A {
        let g = owners[&p].group(TabletId(1)).unwrap().clone();
        assert_eq!(
            futures::executor::block_on(g.store_value(5)),
            Some(b"alpha".to_vec()),
            "group A node {p} key 5 value wrong (seed={seed})"
        );
    }
    for &p in &GROUP_B {
        let g = owners[&p].group(TabletId(2)).unwrap().clone();
        assert_eq!(
            futures::executor::block_on(g.store_value(5000)),
            Some(b"omega".to_vec()),
            "group B node {p} key 5000 value wrong (seed={seed})"
        );
    }
}

// ---------------------------------------------------------------------------
// Fault isolation: a fault on one shard must not stall an unrelated shard.
// ---------------------------------------------------------------------------

/// A fault confined to one shard does **not** stall an unrelated shard. We
/// partition group B's replicas from each other (so a group-B transaction cannot
/// gather a quorum) and show a group-A transaction still commits + executes on
/// every group-A replica within the window — the groups are independent consensus
/// instances.
#[test]
fn fault_on_one_shard_does_not_stall_another() {
    let seed = 0x5121_0007;
    let (mut sim, owners, _router) = cluster(seed);

    // Partition every group-B env from every *other* group-B env, so group B
    // cannot reach a quorum (its coordinator strands). Group A is on entirely
    // different env ids and fully connected, so it is unaffected.
    for &p in &GROUP_B {
        for &q in &GROUP_B {
            if p != q {
                sim.partition_pair(group_env_id(p, TabletId(2)), group_env_id(q, TabletId(2)));
            }
        }
    }

    // A group-B transaction (coordinated by node 3) cannot make progress…
    let b = owners[&3].submit(keys(&[3000])).expect("local to group B");
    let b_part = b.part(TabletId(2)).unwrap();
    // …while a group-A transaction (coordinated by node 0) must still commit.
    let a = owners[&0].submit(keys(&[11])).expect("local to group A");
    let a_part = a.part(TabletId(1)).unwrap();

    sim.run_for(Duration::from_secs(3));

    // Group A committed + executed on every replica despite group B being stuck.
    for &p in &GROUP_A {
        let g = owners[&p].group(TabletId(1)).unwrap();
        assert!(
            g.is_applied(a_part),
            "group A node {p} stalled by an unrelated group-B fault (seed={seed})"
        );
    }
    // Group B's transaction is indeed stranded (no replica applied it).
    for &p in &GROUP_B {
        let g = owners[&p].group(TabletId(2)).unwrap();
        assert!(
            !g.is_applied(b_part),
            "group B node {p} unexpectedly applied while partitioned (seed={seed})"
        );
    }
}

// ---------------------------------------------------------------------------
// Reproducibility.
// ---------------------------------------------------------------------------

/// The whole per-shard run (single- and cross-shard) is byte-reproducible from its
/// seed.
#[test]
fn per_shard_run_is_reproducible_from_seed() {
    let seed = 0x5121_0008;
    let trace = |seed| {
        let (mut sim, owners, _router) = cluster(seed);
        let _: ShardedTxn = owners[&0].submit(keys(&[5, 6])).unwrap();
        let _: ShardedTxn = owners[&2].submit(keys(&[7, 7000])).unwrap();
        let _: ShardedTxn = owners[&3].submit(keys(&[2000])).unwrap();
        sim.run_for(Duration::from_secs(4));
        sim.trace_lines()
    };
    assert_eq!(trace(seed), trace(seed), "per-shard trace not reproducible");
}
