//! Issue #1230: node self-registration survives a slow control-plane start.
//!
//! Production's self-registration task used to make ONE `register_node`
//! attempt (bounded by `SCHEMA_COMMIT_TIMEOUT`, 10 s) and discard its error,
//! so a node that started while no control leader was reachable stayed
//! unregistered (no `node_addrs` entry, empty labels) for the life of the
//! process. The fix is `ClientCtx::register_node_until_settled`; production's
//! `spawn_common_tail` task is a thin wrapper around it, so these tests drive
//! it over `SimCluster`'s real relay/raft, partitioning a non-leader node
//! from the whole control quorum for longer than the commit timeout.
//!
//! - (a) the old single attempt gives up for good (the negative control
//!   documenting the bug), the retrying loop converges once the partition
//!   heals — on every node's view, labels included;
//! - (b) the retry loop stops as soon as registration is observable, and
//!   never re-proposes after a `RemoveMember` (no resurrection).
//!
//! Seed-reproducible: `ANIMUS_SEED=<seed>` replays one seed.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{EnvExt, nid};

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::env_seed;
use super::{MetaCommand, NodeAddrs, RegisterOutcome};

type Slot = Arc<Mutex<Option<Result<RegisterOutcome, String>>>>;

const NODES: usize = 3;
/// Longer than `SCHEMA_COMMIT_TIMEOUT` (10 s) by a comfortable margin: at
/// least one whole attempt, and part of a second, runs inside the outage.
const OUTAGE: Duration = Duration::from_secs(25);
const NEW_NODE: u64 = 50;

fn addrs() -> NodeAddrs {
    let a = "127.0.0.1:9050".to_owned();
    NodeAddrs {
        internal: a.clone(),
        client: a.clone(),
        intra: a.clone(),
        admin: a,
        role: "data".to_owned(),
    }
}

fn labels() -> BTreeMap<String, String> {
    BTreeMap::from([(
        "topology.kubernetes.io/zone".to_owned(),
        "zone-x".to_owned(),
    )])
}

/// Start `register` on `node` and cut `node` off from every other node for
/// `OUTAGE`, then return the slot its result lands in. Leaves the partition
/// in place; the caller heals.
fn start_during_outage(cluster: &mut SimCluster, retrying: bool) -> (u64, Slot) {
    let leader = cluster.control_leader_index() as u64;
    let node = (0..NODES as u64)
        .find(|n| *n != leader)
        .expect("a follower");
    for other in 0..NODES as u64 {
        if other != node {
            cluster.partition(node, other);
        }
    }
    let handle = cluster.handle();
    let slot: Slot = Arc::new(Mutex::new(None));
    let out = slot.clone();
    handle.env(node).spawn_task(async move {
        let r = if retrying {
            Ok(handle
                .register_node_retrying(node, nid(NEW_NODE), addrs(), labels())
                .await)
        } else {
            handle
                .register_node_once(node, nid(NEW_NODE), addrs(), labels())
                .await
        };
        *out.lock().expect("slot") = Some(r);
    });
    cluster.run_for(OUTAGE);
    (node, slot)
}

fn registered_everywhere(cluster: &SimCluster) -> bool {
    let id = nid(NEW_NODE);
    (0..NODES as u64).all(|n| {
        let m = cluster.metadata(n);
        m.node_addrs.get(&id) == Some(&addrs())
            && m.members.get(&id).map(|mm| &mm.labels) == Some(&labels())
    })
}

fn nowhere(cluster: &SimCluster) -> bool {
    let id = nid(NEW_NODE);
    (0..NODES as u64).all(|n| {
        let m = cluster.metadata(n);
        !m.node_addrs.contains_key(&id) && !m.members.contains_key(&id)
    })
}

fn poll(cluster: &mut SimCluster, budget: Duration, what: &str, f: impl Fn(&SimCluster) -> bool) {
    let mut elapsed = Duration::ZERO;
    while !f(cluster) {
        assert!(
            elapsed < budget,
            "seed={}: {what} did not converge within {budget:?}",
            cluster.seed()
        );
        cluster.run_for(Duration::from_millis(100));
        elapsed += Duration::from_millis(100);
    }
}

/// (a, negative control) the pre-fix single attempt: it times out inside the
/// outage and nothing ever re-proposes, so the node stays unregistered even
/// long after the partition heals. This is the bug of issue #1230.
fn run_single_attempt_never_registers(seed: u64) {
    let mut cluster = SimCluster::new(seed, NODES, 3);
    let (_node, slot) = start_during_outage(&mut cluster, false);
    let first = slot.lock().expect("slot").take();
    assert!(
        matches!(first, Some(Err(_))),
        "seed={seed}: the single attempt must have timed out during the outage, got {first:?}"
    );
    cluster.heal_all();
    cluster.run_for(Duration::from_secs(60));
    assert!(
        nowhere(&cluster),
        "seed={seed}: a single failed attempt is never retried (the #1230 bug)"
    );
}

/// (a) the retry loop outlives the outage and converges everywhere.
fn run_retry_converges_after_outage(seed: u64) {
    let mut cluster = SimCluster::new(seed, NODES, 3);
    let (_node, slot) = start_during_outage(&mut cluster, true);
    assert!(
        slot.lock().expect("slot").is_none(),
        "seed={seed}: the loop must still be retrying through the outage"
    );
    assert!(
        nowhere(&cluster),
        "seed={seed}: nothing may have committed yet"
    );
    cluster.heal_all();
    poll(
        &mut cluster,
        Duration::from_secs(60),
        "every node's node_addrs + labels",
        registered_everywhere,
    );
    poll(
        &mut cluster,
        Duration::from_secs(30),
        "loop termination",
        |_| slot.lock().expect("slot").is_some(),
    );
    assert_eq!(
        slot.lock().expect("slot").take().expect("result").ok(),
        Some(RegisterOutcome::Registered),
        "seed={seed}"
    );
}

/// (b) a registration that becomes observable stops the loop, and a
/// subsequent `RemoveMember` is never undone by it.
fn run_removed_node_is_not_resurrected(seed: u64) {
    let mut cluster = SimCluster::new(seed, NODES, 3);
    let (node, slot) = start_during_outage(&mut cluster, true);
    // Meanwhile the registration lands through another path (a leader-side
    // claim, as `admin_add_member` would).
    let leader = cluster.control_leader_index() as u64;
    assert_ne!(leader, node);
    cluster.propose_meta(MetaCommand::RegisterNode {
        node: nid(NEW_NODE),
        addrs: addrs(),
        labels: labels(),
    });
    cluster.heal_all();
    poll(
        &mut cluster,
        Duration::from_secs(60),
        "the loop observing the registration and stopping",
        |_| slot.lock().expect("slot").is_some(),
    );
    assert_eq!(
        slot.lock().expect("slot").take().expect("result").ok(),
        Some(RegisterOutcome::Registered),
        "seed={seed}"
    );
    // Decommission it while the (now finished) loop's node is still up.
    cluster.remove(NEW_NODE);
    cluster.run_for(Duration::from_secs(60));
    assert!(
        (0..NODES as u64).all(|n| !cluster.metadata(n).members.contains_key(&nid(NEW_NODE))),
        "seed={seed}: a removed node must stay removed"
    );
}

fn over_seeds(f: fn(u64)) {
    let base = env_seed(0x1230_0000);
    let n = if std::env::var("ANIMUS_SEED").is_ok() {
        1
    } else {
        20
    };
    for i in 0..n {
        f(base + i);
    }
}

#[test]
fn single_attempt_never_registers_after_a_long_outage() {
    run_single_attempt_never_registers(env_seed(0x1230_0001));
}

#[test]
fn retry_converges_after_a_long_outage() {
    run_retry_converges_after_outage(env_seed(0x1230_0001));
}

#[test]
fn retry_converges_after_a_long_outage_over_seeds() {
    over_seeds(run_retry_converges_after_outage);
}

#[test]
fn single_attempt_never_registers_over_seeds() {
    over_seeds(run_single_attempt_never_registers);
}

#[test]
fn removed_node_is_not_resurrected() {
    run_removed_node_is_not_resurrected(env_seed(0x1230_0001));
}

#[test]
fn removed_node_is_not_resurrected_over_seeds() {
    over_seeds(run_removed_node_is_not_resurrected);
}
