//! `SimCluster`-driven conversion of `tests/split_cluster.rs` (8 tests) —
//! ADR 0061 rung L, C-12 PR 4b. Builds on `sim_cluster_control_data_
//! split.rs`'s own PR 2/3/4a mechanism (`NodeRole`-aware `SimCluster::
//! new_with_roles`, role-aware `restart`/`crash`) — kept as a **separate**
//! module rather than a section appended to that file, per this rung's own
//! ~1800-line-per-module guidance (the two doc tables plus 6 converted
//! scenarios would have pushed `sim_cluster_control_data_split.rs` past
//! that bound). Pure test authorship: **no `sim_cluster.rs` production-
//! shaped change was needed beyond one small accessor**
//! ([`SimCluster::control_leader_index_excluding`], added alongside this
//! module — a `control_leader_index` sibling that skips a crashed former
//! leader's own stale self-belief, needed only because no existing
//! `SimCluster` primitive let a scenario find the NEW control leader right
//! after crashing the old one). Every other primitive this module uses —
//! `new_with_roles`, `control_leader_index`, `crash`, `put`/`put_raw`/
//! `get`/`raw_get`, `admin`, `drive_inplace_split_cutover`, `metadata` —
//! already existed.
//!
//! `tests/split_cluster.rs` covers split-deployment scenarios beyond what
//! `data_only.rs`/`control_only.rs` already do: control-leader failover
//! under live data traffic, an in-place split over a split deployment,
//! failure-driven replica repair onto a spare, decommission via the
//! control leader, a full-cluster stop/restart, a simultaneous
//! control-leader + data-node failure, a decommission racing a split
//! crossover, and the `--cluster-control`/`--cluster-data` `--quiesce-
//! after` CLI-wiring proof (issue #676).
//!
//! ## Classification table (D3 discipline, per original test)
//!
//! | Original test | A/B | Sim sibling |
//! |---|---|---|
//! | `control_leader_failover_under_live_data_traffic` | A | [`run_control_leader_failover_under_live_data_traffic`] — full convert. No `tokio::spawn` traffic task (every write this fixture drives is already sequential): writes issued both before AND after the control-leader kill, a new control leader found via the new [`SimCluster::control_leader_index_excluding`] (a crashed-but-muted former leader never observes a higher-term vote to step down from — the identical stale-self-belief gotcha `sim_cluster_auto_split.rs`'s own module doc already documents for a CP-data tablet leader, generalized here to the control plane), every tracked write still readable, and a post-kill DDL issued from a DATA node that must relay to whichever control node now leads |
//! | `split_over_a_split_deployment` | A | [`run_split_over_a_split_deployment`] — full convert: an admin-triggered in-place split issued against a DATA-only node's admin port, driven to full two-`Active`-children convergence via [`SimCluster::drive_inplace_split_cutover`] (this fixture never spawns `index_drain::change_consumer_loop` as a background loop — the same manual-drive idiom `sim_cluster_auto_split.rs`'s own `poll_split_converged` already established), both halves independently writable/servable afterward and every pre-split key surviving the crossover. Uses `SimCluster::put_raw`/`raw_get` (literal byte keys), not `put`/`get`'s own `item_key`-hashed composite encoding — a split key is compared against a stored key's raw bytes with no decoding step |
//! | `data_node_failure_is_detected_and_repaired_onto_a_spare` | A | [`run_data_node_failure_is_detected_and_repaired_onto_a_spare`] — full convert: a real [`SimCluster::crash`] of a live replica, the control-plane leader's own failure detector marking it `Down` in its real (non-mirrored) `Metadata`, and its own placement `reconcile_loop`/`rebalance_step` repairing the tablet onto the spare — all genuine production convergence, not a fixture stand-in |
//! | `decommission_a_data_node_over_split_deployment_via_the_control_leader` | A | [`run_decommission_a_data_node_over_split_deployment_via_the_control_leader`] — full convert: the refusal/success pairing (`/admin/drain`+`/admin/member/remove` refused with a `409`+leader-hint via the DATA node's own admin port, succeeding via the control LEADER's), `/admin/member/drain-status` read cross-node-type via the data node's own mirror, and membership/address-book pruning — all driven via raw [`SimCluster::admin`] calls (not `sim_cluster_growth.rs`'s own `SimCluster::drain`/`SimCluster::remove` convenience wrappers, which always target the leader and so can't reproduce the refusal half) |
//! | `full_split_cluster_restart_recovers_metadata_and_data` | B | **KEPT** whole in the trimmed file — a genuine on-disk `StorageBackend::Lsm` full-outage restart (every control AND data process stopped, then rebound on the same dir/addresses): real fsync/on-disk WAL crash recovery `SimCluster` cannot stand in for (its own `SimCluster::restart` rebuilds a node's `RaftNode`/`RaftKvNode` in-process over `MemoryEngine`, reusing the same in-memory engine handle rather than replaying an on-disk WAL, ADR 0061 rung D4 PR 1) — this crate's own residual-inventory class (B) "real-disk durability/restart," permanent |
//! | `control_leader_and_data_node_failure_simultaneously_still_converges` | A | [`run_control_leader_and_data_node_failure_simultaneously_still_converges`] — full convert: both faults land at the same discrete-event instant ([`SimCluster::crash`] called twice back to back, with no intervening `run_for` — `SimCluster` is single-threaded and event-driven, so this genuinely IS the same-instant shape, not a weakened approximation of `tokio::join!`'s real-thread simultaneity), the surviving control pair electing a new leader (again via [`SimCluster::control_leader_index_excluding`]) while placement independently repairs the dead data replica onto the spare, no tracked write lost, and both a post-dual-failure DDL and a fresh write recovering |
//! | `decommission_racing_a_tablet_split_converges_with_no_data_loss` | A (weakened) | [`run_decommission_racing_a_tablet_split_converges_with_no_data_loss`] — converts the *shape* (a split kickoff and a drain kickoff fired back to back against the SAME tablet, neither call waiting for the other's own convergence before the second fires) but not `tokio::join!`'s literal single-instant simultaneity of two real HTTP round trips; the reconciler must still evacuate the draining node off BOTH the narrowed parent and the freshly-forked child, driven to convergence via the same [`SimCluster::drive_inplace_split_cutover`] manual-drive idiom `split_over_a_split_deployment` above uses. Every pre-split key plus the crossover-window writes are proven to survive |
//! | `cluster_control_data_threads_quiesce_after_to_admin_config` | B | **KEPT** whole in the trimmed file — the real `animusd::start_split_cluster_with_growth` process assembly (`--cluster-control`/`--cluster-data`'s own CLI-equivalent config wiring), proving `--quiesce-after`/`--heartbeat-batch`/`--shared-wal` reach every data-role node THAT WAY too, not just `--config`/`--node` and `--cluster N`: `SimCluster` never goes through that config-parse/process-boundary path at all (it builds every node in-process from its own fixed constructor — see `dynamo_throttling.rs`'s own residual entry, `crates/animusd/CLAUDE.md`, for the identical "config-parse path, not a dispatch-core check" reasoning) |
//!
//! Real-socket counts before/after PR 4b: 8 → 2 (both kept whole for the
//! reasons in the table above — `tests/split_cluster.rs` stays, trimmed,
//! never deleted). Every scenario issues from the intended role's own node
//! index and asks for `ConsistentRead: true` (or `SimCluster::raw_get`'s
//! own `consistent: true`) on any read that verifies a write, per ADR
//! 0055's standing testing discipline. `_over_seeds` siblings run 5 fixed
//! seeds each, mirroring `sim_cluster_control_data_split.rs`'s own
//! convention exactly. Seed replay (repo convention): `ANIMUS_SEED=<seed>
//! cargo test -p animusd --lib <test name>`.

use std::time::Duration;

use animus_control::{Metadata, NodeStatus};
use animus_env::{NodeId, nid};
use animus_tablet::{TabletId, TabletState};

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{create_table_via_wire, env_seed, json};
use crate::config::NodeRole;

/// A single hash-key (`pk`, string) `CreateTable`, issued from `node` —
/// mirrors `sim_cluster_control_data_split.rs`'s identically-named helper
/// (duplicated per this crate's own "small fixtures duplicated per test
/// module" convention).
fn create_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    create_table_via_wire(cluster, node, &body)
}

/// Converged-or-timeout poll on `cond(cluster)` — the shared shape every
/// `sim_cluster_*` module's own scenario-local convergence checks use
/// (duplicated, not reached into, per this crate's own convention —
/// `sim_cluster_control_data_split.rs` carries the identical copy).
fn poll_until(
    cluster: &mut SimCluster,
    budget: Duration,
    seed: u64,
    what: &str,
    mut cond: impl FnMut(&mut SimCluster) -> bool,
) {
    const STEP: Duration = Duration::from_millis(100);
    let mut elapsed = Duration::ZERO;
    loop {
        if cond(cluster) {
            return;
        }
        assert!(
            elapsed < budget,
            "seed={seed}: {what} never converged within {budget:?}"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

/// The (sole, pre-split) tablet id hosting `table`, from `node`'s own view —
/// mirrors `sim_cluster_auto_split.rs`'s identically-named helper (this
/// crate's own "small fixtures duplicated per test module" convention). A
/// wire-provisioned table (every table this module creates) never has an
/// entry in `SimCluster::create_table_with_replication`'s own hand-hosted
/// bookkeeping, so this reads live `Metadata` instead.
fn tablet_of(cluster: &SimCluster, node: u64, table: &str) -> TabletId {
    let meta = cluster.metadata(node);
    *meta
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("table {table} has no tablet on node {node}'s own view"))
        .0
}

/// Poll `run_for(STEP)` interleaved with [`SimCluster::drive_inplace_split_
/// cutover`] on every id in `nodes` (this fixture never spawns that driver
/// as a background loop — mirrors `sim_cluster_auto_split.rs`'s own
/// `poll_split_converged`) until `table` shows exactly TWO `Active` tablets
/// and zero `Splitting` ones on EVERY id in `nodes`, or `budget` is
/// exceeded.
fn poll_split_converged(
    cluster: &mut SimCluster,
    table: &str,
    nodes: &[u64],
    budget: Duration,
    seed: u64,
) {
    const STEP: Duration = Duration::from_millis(100);
    let mut elapsed = Duration::ZERO;
    loop {
        for &n in nodes {
            cluster.drive_inplace_split_cutover(n);
        }
        let converged = nodes.iter().all(|&n| {
            let meta = cluster.metadata(n);
            let mut active = 0;
            let mut splitting = 0;
            for (_, t) in meta.tablets_for_table(table) {
                match t.state {
                    TabletState::Active => active += 1,
                    TabletState::Splitting => splitting += 1,
                    _ => {}
                }
            }
            active == 2 && splitting == 0
        });
        if converged {
            return;
        }
        assert!(
            elapsed < budget,
            "seed={seed}: split of {table} did not converge to two Active children \
             within {budget:?}"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

/// `create_table`, tolerant of the DDL-after-recovery race: schema and
/// tablet DO commit even when `await_table_serveable`'s own bounded
/// serving-wait legitimately loses a race against the just-recovered
/// control plane/reconciler settling (no settle buffer between a
/// `control_leader_index_excluding`/dual-crash recovery and the next
/// DDL). The first attempt's own failure is tolerated only when it's
/// this specific race (a `500` naming the "did not become serveable"
/// wait, or an outright `ResourceInUseException` if some earlier caller
/// already won the schema commit); a retried `CreateTable` then either
/// succeeds outright or sees `ResourceInUseException` (the schema already
/// committed on the first attempt) — either way the caller can trust the
/// table exists once this returns, and its own subsequent
/// `has_table_schema` poll picks up the rest.
fn create_table_after_recovery(cluster: &mut SimCluster, node: u64, table: &str, seed: u64) {
    let (status, body) = create_table(cluster, node, table);
    if status == 200 {
        return;
    }
    assert!(
        body.contains("ResourceInUseException") || body.contains("did not become serveable"),
        "seed={seed}: CreateTable({table}) failed for an unexpected reason: {body}"
    );
    let (status2, body2) = create_table(cluster, node, table);
    assert!(
        status2 == 200 || body2.contains("ResourceInUseException"),
        "seed={seed}: retry CreateTable({table}) failed: {body2}"
    );
}

/// [`SimCluster::put`], retried on any error (a simultaneous
/// control+data fault can legitimately need more than one relay attempt
/// to route around — e.g. a stale forward hint chasing the
/// just-crashed node, or a group briefly leaderless) — bounded by
/// `budget`, advancing virtual time between attempts so the fault
/// actually has room to resolve.
#[allow(clippy::too_many_arguments)]
fn put_retry(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    pk: &str,
    sk: &str,
    value: &[u8],
    seed: u64,
    what: &str,
) {
    const STEP: Duration = Duration::from_millis(300);
    let budget = Duration::from_secs(30);
    let mut elapsed = Duration::ZERO;
    loop {
        match cluster.put(node, table, pk, sk, value) {
            Ok(()) => return,
            Err(e) => {
                assert!(
                    elapsed < budget,
                    "seed={seed}: {what} never succeeded within {budget:?}: {e}"
                );
                cluster.run_for(STEP);
                elapsed += STEP;
            }
        }
    }
}

/// [`SimCluster::put_raw`], retried while asserting the transient
/// "; retry" split-cutover-freeze substring (`ADR 0050`'s frozen-tablet
/// refusal — see `sim_cluster_auto_split.rs`'s own `put_item_retry`
/// precedent) and driving [`SimCluster::drive_inplace_split_cutover`] on
/// every id in `cutover_nodes` on each retry attempt, since this fixture
/// never runs the periodic cutover driver as a background loop.
#[allow(clippy::too_many_arguments)]
fn put_raw_retry(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    key: Vec<u8>,
    value: Vec<u8>,
    cutover_nodes: &[u64],
    seed: u64,
    what: &str,
) {
    const STEP: Duration = Duration::from_millis(100);
    let budget = Duration::from_secs(30);
    let mut elapsed = Duration::ZERO;
    loop {
        match cluster.put_raw(node, table, key.clone(), value.clone()) {
            Ok(()) => return,
            Err(e) => {
                assert!(
                    e.contains("; retry"),
                    "seed={seed}: {what} failed for a non-retryable reason: {e}"
                );
                assert!(
                    elapsed < budget,
                    "seed={seed}: {what} never succeeded within {budget:?}: {e}"
                );
                for &n in cutover_nodes {
                    cluster.drive_inplace_split_cutover(n);
                }
                cluster.run_for(STEP);
                elapsed += STEP;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// (1) control_leader_failover_under_live_data_traffic
// ---------------------------------------------------------------------------

fn run_control_leader_failover_under_live_data_traffic(seed: u64) {
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 2);
    let leader = cluster.control_leader_index() as u64;

    let (status, body) = create_table(&mut cluster, leader, "failover_t");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    // Writes before the kill, alternating the two data nodes.
    let mut written: Vec<(String, Vec<u8>)> = Vec::new();
    for i in 0..6u32 {
        let node = 3 + (i % 2) as u64;
        let key = format!("traffic-{i}");
        let value = format!("v{i}").into_bytes();
        cluster
            .put(node, "failover_t", &key, "sk", &value)
            .unwrap_or_else(|e| panic!("seed={seed}: pre-kill put #{i} failed: {e}"));
        written.push((key, value));
    }

    // Kill the CURRENT control LEADER specifically — the harder failover
    // case: the node every proposal was routing through.
    cluster.crash(leader);

    // Writes spanning the failover, still alternating data nodes — the data
    // plane's own CP-data Raft groups are independent of the control plane,
    // so an ongoing write need not itself be interrupted; what this proves
    // is that the write path keeps working while the control plane is
    // mid-election.
    for i in 6..12u32 {
        let node = 3 + (i % 2) as u64;
        let key = format!("traffic-{i}");
        let value = format!("v{i}").into_bytes();
        cluster
            .put(node, "failover_t", &key, "sk", &value)
            .unwrap_or_else(|e| panic!("seed={seed}: post-kill put #{i} failed: {e}"));
        written.push((key, value));
    }

    // The remaining pair elects a new leader.
    let new_leader = cluster.control_leader_index_excluding(leader) as u64;
    assert_ne!(new_leader, leader, "seed={seed}");

    // No write is lost.
    for (key, value) in &written {
        let got = cluster
            .get(3, "failover_t", key, "sk", true)
            .unwrap_or_else(|e| panic!("seed={seed}: get({key}) failed: {e}"));
        assert_eq!(got.as_ref(), Some(value), "seed={seed}: key {key}");
    }

    // A DDL issued only AFTER the kill, from a DATA node, still commits —
    // relayed to whichever control node now leads.
    create_table_after_recovery(&mut cluster, 3, "failover_ddl_t", seed);
    let survivors: Vec<u64> = (0..3u64).filter(|&n| n != leader).collect();
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        "the surviving control pair observing the post-failover schema",
        |c| {
            survivors
                .iter()
                .all(|&n| c.metadata(n).has_table_schema("failover_ddl_t"))
        },
    );
}

#[test]
fn control_leader_failover_under_live_data_traffic() {
    run_control_leader_failover_under_live_data_traffic(env_seed(0xC12B_0001));
}

#[test]
fn control_leader_failover_under_live_data_traffic_over_seeds() {
    for i in 0..5 {
        run_control_leader_failover_under_live_data_traffic(0xC12B_1000 + i);
    }
}

// ---------------------------------------------------------------------------
// (2) split_over_a_split_deployment
// ---------------------------------------------------------------------------

const SPLIT_KEYS: [(&str, &str); 5] = [
    ("a", "v-a"),
    ("g", "v-g"),
    ("m", "v-m"),
    ("s", "v-s"),
    ("z", "v-z"),
];

fn run_split_over_a_split_deployment(seed: u64) {
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 2);
    let leader = cluster.control_leader_index() as u64;

    let (status, body) = create_table(&mut cluster, leader, "split_merge_t");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    for (k, v) in SPLIT_KEYS {
        cluster
            .put_raw(
                3,
                "split_merge_t",
                k.as_bytes().to_vec(),
                v.as_bytes().to_vec(),
            )
            .unwrap_or_else(|e| panic!("seed={seed}: put_raw({k}) failed: {e}"));
    }

    let parent = tablet_of(&cluster, 3, "split_merge_t");

    // Trigger the split against a DATA-only node's admin port — a
    // control-plane admin action reached through the genuinely-`Remote`
    // fleet, not only against the control deployment.
    let split_body = format!(r#"{{"tablet":{},"split_key":"m"}}"#, parent.0);
    let (status, body) = cluster.admin(3, "POST", "/admin/tablet/split", "", split_body.as_bytes());
    assert_eq!(status, 200, "seed={seed}: split trigger: {body}");

    poll_split_converged(
        &mut cluster,
        "split_merge_t",
        &[3u64, 4],
        Duration::from_secs(30),
        seed,
    );

    // Both halves independently writable/servable post-split, and every
    // pre-split key survives the crossover.
    cluster
        .put_raw(3, "split_merge_t", b"b".to_vec(), b"v-b2".to_vec())
        .unwrap_or_else(|e| panic!("seed={seed}: put_raw(b) failed: {e}"));
    cluster
        .put_raw(4, "split_merge_t", b"y".to_vec(), b"v-y2".to_vec())
        .unwrap_or_else(|e| panic!("seed={seed}: put_raw(y) failed: {e}"));
    assert_eq!(
        cluster.raw_get(3, "split_merge_t", b"b".to_vec(), true),
        Ok(Some(b"v-b2".to_vec())),
        "seed={seed}"
    );
    assert_eq!(
        cluster.raw_get(4, "split_merge_t", b"y".to_vec(), true),
        Ok(Some(b"v-y2".to_vec())),
        "seed={seed}"
    );
    for (k, v) in SPLIT_KEYS {
        assert_eq!(
            cluster.raw_get(3, "split_merge_t", k.as_bytes().to_vec(), true),
            Ok(Some(v.as_bytes().to_vec())),
            "seed={seed}: key {k}"
        );
    }
}

#[test]
fn split_over_a_split_deployment() {
    run_split_over_a_split_deployment(env_seed(0xC12B_0002));
}

#[test]
fn split_over_a_split_deployment_over_seeds() {
    for i in 0..5 {
        run_split_over_a_split_deployment(0xC12B_2000 + i);
    }
}

// ---------------------------------------------------------------------------
// (3) data_node_failure_is_detected_and_repaired_onto_a_spare
// ---------------------------------------------------------------------------

fn run_data_node_failure_is_detected_and_repaired_onto_a_spare(seed: u64) {
    // RF = min(N,3): 4 data nodes leaves exactly one idle spare once the
    // table's tablet provisions onto the 3 lowest-id Active members.
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
        NodeRole::Data,
        NodeRole::Data,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 3);
    let leader = cluster.control_leader_index() as u64;

    let (status, body) = create_table(&mut cluster, leader, "repair_t");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    cluster
        .put(3, "repair_t", "k0", "sk", b"v0")
        .unwrap_or_else(|e| panic!("seed={seed}: initial put failed: {e}"));

    let data_ids: Vec<NodeId> = (3..7u64).map(nid).collect();
    let mut found = None;
    poll_until(
        &mut cluster,
        Duration::from_secs(20),
        seed,
        "the tablet provisioning with 3 replicas",
        |c| {
            let meta = c.metadata(leader);
            if let Some((&id, t)) = meta.tablets_for_table("repair_t").next()
                && t.replicas.len() == 3
            {
                found = Some((id, t.replicas.clone()));
                return true;
            }
            false
        },
    );
    let (tablet, replicas_before) = found.expect("captured by the poll above");

    let spare = data_ids
        .iter()
        .find(|id| !replicas_before.contains(id))
        .cloned()
        .unwrap_or_else(|| panic!("seed={seed}: a spare data node exists"));
    let victim_id = replicas_before[0].clone();
    let victim_idx = 3 + data_ids
        .iter()
        .position(|id| *id == victim_id)
        .unwrap_or_else(|| panic!("seed={seed}: victim id resolves to a known data index"))
        as u64;

    cluster.crash(victim_idx);

    // The detector marks it Down in the control leader's own (real,
    // non-mirrored) metadata.
    poll_until(
        &mut cluster,
        Duration::from_secs(30),
        seed,
        "the killed data node marked Down",
        |c| c.metadata(leader).members.get(&victim_id).map(|m| m.status) == Some(NodeStatus::Down),
    );

    // Placement repairs the tablet onto the spare.
    poll_until(
        &mut cluster,
        Duration::from_secs(60),
        seed,
        "the dead replica auto-replaced by the spare",
        |c| {
            c.metadata(leader)
                .tablets
                .get(&tablet)
                .is_some_and(|t| t.replicas.contains(&spare) && !t.replicas.contains(&victim_id))
        },
    );

    // Still readable through the survivors, and a fresh write commits.
    let survivor = (3..7u64).find(|&n| n != victim_idx).unwrap();
    poll_until(
        &mut cluster,
        Duration::from_secs(30),
        seed,
        "the pre-crash write still readable through a survivor",
        |c| {
            matches!(
                c.get(survivor, "repair_t", "k0", "sk", true),
                Ok(Some(ref v)) if v.as_slice() == b"v0"
            )
        },
    );
    cluster
        .put(survivor, "repair_t", "k1", "sk", b"v1")
        .unwrap_or_else(|e| panic!("seed={seed}: post-repair put failed: {e}"));
    poll_until(
        &mut cluster,
        Duration::from_secs(20),
        seed,
        "the post-repair write readable",
        |c| {
            matches!(
                c.get(survivor, "repair_t", "k1", "sk", true),
                Ok(Some(ref v)) if v.as_slice() == b"v1"
            )
        },
    );
}

#[test]
fn data_node_failure_is_detected_and_repaired_onto_a_spare() {
    run_data_node_failure_is_detected_and_repaired_onto_a_spare(env_seed(0xC12B_0003));
}

#[test]
fn data_node_failure_is_detected_and_repaired_onto_a_spare_over_seeds() {
    for i in 0..5 {
        run_data_node_failure_is_detected_and_repaired_onto_a_spare(0xC12B_3000 + i);
    }
}

// ---------------------------------------------------------------------------
// (4) decommission_a_data_node_over_split_deployment_via_the_control_leader
// ---------------------------------------------------------------------------

/// Several independent tables (mirroring the original test's own
/// `DECOMM_TABLES`): initial RF-based placement always picks the *lowest*
/// `min(N,3)` Active raftkv ids, so with one table the highest-id data node
/// would never gain a replica at all — several tables raise the
/// rebalancer's (ADR 0029) global imbalance enough that it actually moves
/// one onto it.
const DECOMM_TABLES: [&str; 3] = ["dsplit0", "dsplit1", "dsplit2"];

fn table_with_replica(meta: &Metadata, target: &NodeId) -> Option<String> {
    meta.tablets
        .values()
        .find(|t| t.replicas.contains(target))
        .and_then(|t| t.table.clone())
}

fn run_decommission_a_data_node_over_split_deployment_via_the_control_leader(seed: u64) {
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
        NodeRole::Data,
        NodeRole::Data,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 3);
    let leader = cluster.control_leader_index() as u64;

    for table in DECOMM_TABLES {
        let (status, body) = create_table(&mut cluster, leader, table);
        assert_eq!(
            status, 200,
            "seed={seed}: CreateTable({table}) failed: {body}"
        );
    }
    for table in DECOMM_TABLES {
        cluster
            .put(3, table, "k0", "sk", b"v0")
            .unwrap_or_else(|e| panic!("seed={seed}: initial put({table}) failed: {e}"));
    }

    // Target the highest-id data node — guaranteed to start as a pure
    // spare — and wait for the rebalancer to actually give it a real
    // replica before decommissioning it; this proves the flow drains a
    // node that genuinely hosts data, not an already-idle one.
    let target_id = nid(6);
    let mut found = None;
    poll_until(
        &mut cluster,
        Duration::from_secs(90),
        seed,
        "the target data node gaining a rebalanced replica",
        |c| {
            if let Some(t) = table_with_replica(&c.metadata(leader), &target_id) {
                found = Some(t);
                true
            } else {
                false
            }
        },
    );
    let hosted_table = found.expect("captured by the poll above");

    // Sanity: it genuinely serves before decommission starts.
    cluster
        .put(6, &hosted_table, "pre", "sk", b"ok")
        .unwrap_or_else(|e| panic!("seed={seed}: pre-decommission put failed: {e}"));
    poll_until(
        &mut cluster,
        Duration::from_secs(20),
        seed,
        "the pre-decommission write visible cluster-wide",
        |c| {
            matches!(
                c.get(3, &hosted_table, "pre", "sk", true),
                Ok(Some(ref v)) if v.as_slice() == b"ok"
            )
        },
    );

    let drain_body = format!(r#"{{"node":"{target_id}"}}"#);

    // A control-plane admin action against the DATA node's OWN admin port
    // must refuse with a leader-routing hint: a data-only node never
    // registers a local control handle at all (`ClusterEdgeState::
    // leader_handle` is unconditionally empty there).
    {
        let (status, body) = cluster.admin(6, "POST", "/admin/drain", "", drain_body.as_bytes());
        assert_eq!(
            status, 409,
            "seed={seed}: drain via the data node's own admin port should be refused: {body}"
        );
        assert!(
            body.to_ascii_lowercase().contains("leader"),
            "seed={seed}: expected a leader-routing hint, got: {body}"
        );
    }

    // The control LEADER's admin port succeeds.
    {
        let (status, body) =
            cluster.admin(leader, "POST", "/admin/drain", "", drain_body.as_bytes());
        assert_eq!(status, 200, "seed={seed}: drain failed: {body}");
    }

    // Drain-status is read-only and serves off ANY node's
    // `effective_metadata()` — poll it via the DATA node's own admin port
    // (its mirror), proving that cross-node-type read path too.
    poll_until(
        &mut cluster,
        Duration::from_secs(60),
        seed,
        "the target node finishing draining",
        |c| {
            let (status, body) = c.admin(
                6,
                "GET",
                "/admin/member/drain-status",
                &format!("node={target_id}"),
                &[],
            );
            if status != 200 {
                return false;
            }
            let v = json(&body);
            let remaining = v["tablets_remaining"].as_u64().unwrap_or(u64::MAX);
            let node_status = v["status"].as_str().unwrap_or("");
            remaining == 0 && node_status != "Active"
        },
    );

    // Remove: refused via the data node's own admin port, succeeds via the
    // control leader — same refusal/success pairing as drain above.
    {
        let (status, body) =
            cluster.admin(6, "POST", "/admin/member/remove", "", drain_body.as_bytes());
        assert_eq!(
            status, 409,
            "seed={seed}: remove via the data node's own admin port should be refused: {body}"
        );
        assert!(
            body.to_ascii_lowercase().contains("leader"),
            "seed={seed}: expected a leader-routing hint, got: {body}"
        );
    }
    {
        let (status, body) = cluster.admin(
            leader,
            "POST",
            "/admin/member/remove",
            "",
            drain_body.as_bytes(),
        );
        assert_eq!(status, 200, "seed={seed}: remove failed: {body}");
    }

    // Membership + address book pruned; cluster still serving.
    poll_until(
        &mut cluster,
        Duration::from_secs(30),
        seed,
        "the removed node disappearing from membership/the address book",
        |c| {
            let meta = c.metadata(leader);
            !meta.members.contains_key(&target_id) && !meta.node_addrs.contains_key(&target_id)
        },
    );

    let survivor = 3u64;
    cluster
        .put(survivor, &hosted_table, "post-remove", "sk", b"ok")
        .unwrap_or_else(|e| panic!("seed={seed}: post-remove put failed: {e}"));
    poll_until(
        &mut cluster,
        Duration::from_secs(30),
        seed,
        "the post-remove write visible cluster-wide",
        |c| {
            matches!(
                c.get(survivor, &hosted_table, "post-remove", "sk", true),
                Ok(Some(ref v)) if v.as_slice() == b"ok"
            )
        },
    );
}

#[test]
fn decommission_a_data_node_over_split_deployment_via_the_control_leader() {
    run_decommission_a_data_node_over_split_deployment_via_the_control_leader(env_seed(
        0xC12B_0004,
    ));
}

#[test]
fn decommission_a_data_node_over_split_deployment_via_the_control_leader_over_seeds() {
    for i in 0..5 {
        run_decommission_a_data_node_over_split_deployment_via_the_control_leader(0xC12B_4000 + i);
    }
}

// ---------------------------------------------------------------------------
// (5) control_leader_and_data_node_failure_simultaneously_still_converges
// ---------------------------------------------------------------------------

fn run_control_leader_and_data_node_failure_simultaneously_still_converges(seed: u64) {
    // RF = min(N,3): 4 data nodes leaves exactly one spare once the tablet
    // provisions onto the 3 lowest-id Active members, so the killed data
    // replica has somewhere to be auto-repaired onto.
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
        NodeRole::Data,
        NodeRole::Data,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 3);
    let leader = cluster.control_leader_index() as u64;

    let (status, body) = create_table(&mut cluster, leader, "dualfail_t");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    cluster
        .put(3, "dualfail_t", "k0", "sk", b"v0")
        .unwrap_or_else(|e| panic!("seed={seed}: initial put failed: {e}"));

    let data_ids: Vec<NodeId> = (3..7u64).map(nid).collect();
    let mut found = None;
    poll_until(
        &mut cluster,
        Duration::from_secs(20),
        seed,
        "the tablet provisioning with 3 replicas",
        |c| {
            let meta = c.metadata(leader);
            if let Some((&id, t)) = meta.tablets_for_table("dualfail_t").next()
                && t.replicas.len() == 3
            {
                found = Some((id, t.replicas.clone()));
                return true;
            }
            false
        },
    );
    let (tablet, replicas_before) = found.expect("captured by the poll above");

    let spare = data_ids
        .iter()
        .find(|id| !replicas_before.contains(id))
        .cloned()
        .unwrap_or_else(|| panic!("seed={seed}: a spare data node exists"));
    let killed_data_id = replicas_before[0].clone();
    let victim_idx = 3 + data_ids
        .iter()
        .position(|id| *id == killed_data_id)
        .unwrap_or_else(|| panic!("seed={seed}: victim id resolves to a known data index"))
        as u64;

    // A few writes before disrupting anything, spanning every live data
    // node (so the traffic isn't tied to whichever node is about to die).
    let mut written: Vec<(String, Vec<u8>)> = Vec::new();
    for i in 0..6u32 {
        let node = 3 + (i % 4) as u64;
        let key = format!("dual-{i}");
        let value = format!("v{i}").into_bytes();
        cluster
            .put(node, "dualfail_t", &key, "sk", &value)
            .unwrap_or_else(|e| panic!("seed={seed}: pre-failure put #{i} failed: {e}"));
        written.push((key, value));
    }

    // Kill the control LEADER and the live DATA replica AT THE SAME
    // discrete-event instant: two `SimCluster::crash` calls, no `run_for`
    // between them — `SimCluster` is single-threaded/event-driven, so
    // nothing distinguishes this from a genuine simultaneous failure.
    cluster.crash(leader);
    cluster.crash(victim_idx);

    // Writes spanning both failures, only across the surviving data nodes.
    let survivor_data: Vec<u64> = (3..7u64).filter(|&n| n != victim_idx).collect();
    for i in 6..12u32 {
        let node = survivor_data[(i as usize) % survivor_data.len()];
        let key = format!("dual-{i}");
        let value = format!("v{i}").into_bytes();
        put_retry(
            &mut cluster,
            node,
            "dualfail_t",
            &key,
            "sk",
            &value,
            seed,
            &format!("post-failure put #{i}"),
        );
        written.push((key, value));
    }

    // The remaining control pair elects a new leader...
    let new_leader = cluster.control_leader_index_excluding(leader) as u64;
    assert_ne!(new_leader, leader, "seed={seed}");

    // ...and placement independently repairs the tablet onto the spare,
    // closing the dead replica out of its set.
    poll_until(
        &mut cluster,
        Duration::from_secs(60),
        seed,
        "the dead replica auto-replaced by the spare after the dual failure",
        move |c| {
            c.metadata(new_leader)
                .tablets
                .get(&tablet)
                .is_some_and(|t| {
                    t.replicas.contains(&spare) && !t.replicas.contains(&killed_data_id)
                })
        },
    );

    // No tracked write was lost.
    for (key, value) in &written {
        poll_until(
            &mut cluster,
            Duration::from_secs(30),
            seed,
            &format!("key {key} readable through a survivor"),
            |c| {
                matches!(
                    c.get(survivor_data[0], "dualfail_t", key, "sk", true),
                    Ok(Some(ref v)) if v == value
                )
            },
        );
    }

    // A DDL issued only AFTER both kills still commits, relayed from a
    // surviving DATA node to whichever control node now leads.
    create_table_after_recovery(&mut cluster, survivor_data[0], "dualfail_ddl_t", seed);
    let control_survivors: Vec<u64> = (0..3u64).filter(|&n| n != leader).collect();
    poll_until(
        &mut cluster,
        Duration::from_secs(20),
        seed,
        "the surviving control pair observing the post-dual-failure schema",
        |c| {
            control_survivors
                .iter()
                .all(|&n| c.metadata(n).has_table_schema("dualfail_ddl_t"))
        },
    );

    // A brand-new write also works end to end, through the fully-converged
    // cluster.
    cluster
        .put(survivor_data[0], "dualfail_t", "k1", "sk", b"v1")
        .unwrap_or_else(|e| panic!("seed={seed}: post-convergence put failed: {e}"));
    poll_until(
        &mut cluster,
        Duration::from_secs(20),
        seed,
        "the post-convergence write visible",
        |c| {
            matches!(
                c.get(survivor_data[0], "dualfail_t", "k1", "sk", true),
                Ok(Some(ref v)) if v.as_slice() == b"v1"
            )
        },
    );
}

#[test]
fn control_leader_and_data_node_failure_simultaneously_still_converges() {
    run_control_leader_and_data_node_failure_simultaneously_still_converges(env_seed(0xC12B_0005));
}

#[test]
fn control_leader_and_data_node_failure_simultaneously_still_converges_over_seeds() {
    for i in 0..5 {
        run_control_leader_and_data_node_failure_simultaneously_still_converges(0xC12B_5000 + i);
    }
}

// ---------------------------------------------------------------------------
// (6) decommission_racing_a_tablet_split_converges_with_no_data_loss
// ---------------------------------------------------------------------------

const CROSSOVER_KEYS: [(&str, &str); 5] = [
    ("a", "v-a"),
    ("g", "v-g"),
    ("m", "v-m"),
    ("s", "v-s"),
    ("z", "v-z"),
];

fn run_decommission_racing_a_tablet_split_converges_with_no_data_loss(seed: u64) {
    // RF = min(N,3): 4 data nodes leaves exactly one spare so the
    // decommissioned replica has somewhere for BOTH the split parent and
    // its new child to be repaired onto.
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
        NodeRole::Data,
        NodeRole::Data,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 3);
    let leader = cluster.control_leader_index() as u64;

    let (status, body) = create_table(&mut cluster, leader, "cross_t");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    for (k, v) in CROSSOVER_KEYS {
        cluster
            .put_raw(3, "cross_t", k.as_bytes().to_vec(), v.as_bytes().to_vec())
            .unwrap_or_else(|e| panic!("seed={seed}: put_raw({k}) failed: {e}"));
    }

    let data_ids: Vec<NodeId> = (3..7u64).map(nid).collect();
    let mut found = None;
    poll_until(
        &mut cluster,
        Duration::from_secs(20),
        seed,
        "the tablet provisioning with 3 replicas",
        |c| {
            let meta = c.metadata(leader);
            if let Some((&id, t)) = meta.tablets_for_table("cross_t").next()
                && t.replicas.len() == 3
            {
                found = Some((id, t.replicas.clone()));
                return true;
            }
            false
        },
    );
    let (parent, replicas_before) = found.expect("captured by the poll above");

    let victim_id = replicas_before[0].clone();
    let victim_idx = 3 + data_ids
        .iter()
        .position(|id| *id == victim_id)
        .unwrap_or_else(|| panic!("seed={seed}: victim id resolves to a known data index"))
        as u64;

    // Fire the split kickoff and the drain kickoff back to back — neither
    // waits for the other's own convergence before the second fires (the
    // closest `SimCluster`'s own synchronous op-call shape gets to
    // `tokio::join!`'s real-thread simultaneity — see this module's own doc
    // table entry for this scenario).
    let split_body = format!(r#"{{"tablet":{},"split_key":"m"}}"#, parent.0);
    let (status, body) = cluster.admin(3, "POST", "/admin/tablet/split", "", split_body.as_bytes());
    assert_eq!(status, 200, "seed={seed}: split trigger: {body}");

    let drain_body = format!(r#"{{"node":"{victim_id}"}}"#);
    let (status, body) = cluster.admin(leader, "POST", "/admin/drain", "", drain_body.as_bytes());
    assert_eq!(status, 200, "seed={seed}: drain trigger: {body}");

    // Writes racing the crossover window itself — one key on each future
    // half of the range. The parent is frozen for cutover the instant the
    // fork happens (ADR 0050's "; retry" transient), so these retry while
    // driving the cutover manually, mirroring `sim_cluster_auto_split.rs`'s
    // own `put_item_retry` idiom.
    put_raw_retry(
        &mut cluster,
        3,
        "cross_t",
        b"b".to_vec(),
        b"v-b2".to_vec(),
        &[3, 4, 5, 6],
        seed,
        "put_raw(b)",
    );
    put_raw_retry(
        &mut cluster,
        3,
        "cross_t",
        b"y".to_vec(),
        b"v-y2".to_vec(),
        &[3, 4, 5, 6],
        seed,
        "put_raw(y)",
    );

    // Poll BOTH the split's own convergence and the drain's evacuation of
    // the victim off every tablet of `cross_t` — driving the cutover
    // manually on every live data node id (the draining node still serves
    // throughout; it isn't crashed, just being decommissioned).
    {
        const STEP: Duration = Duration::from_millis(100);
        let budget = Duration::from_secs(90);
        let mut elapsed = Duration::ZERO;
        loop {
            for &n in &[3u64, 4, 5, 6] {
                cluster.drive_inplace_split_cutover(n);
            }
            let meta = cluster.metadata(leader);
            let mut active = 0;
            let mut splitting = 0;
            let mut still_hosts_victim = false;
            for (_, t) in meta.tablets_for_table("cross_t") {
                match t.state {
                    TabletState::Active => active += 1,
                    TabletState::Splitting => splitting += 1,
                    _ => {}
                }
                if t.replicas.contains(&victim_id) {
                    still_hosts_victim = true;
                }
            }
            if active == 2 && splitting == 0 && !still_hosts_victim {
                break;
            }
            assert!(
                elapsed < budget,
                "seed={seed}: split+drain crossover did not converge within {budget:?}"
            );
            cluster.run_for(STEP);
            elapsed += STEP;
        }
    }

    // Drain-status confirms it before removal is attempted.
    poll_until(
        &mut cluster,
        Duration::from_secs(30),
        seed,
        "the drained node finishing draining",
        |c| {
            let (status, body) = c.admin(
                leader,
                "GET",
                "/admin/member/drain-status",
                &format!("node={victim_id}"),
                &[],
            );
            if status != 200 {
                return false;
            }
            let v = json(&body);
            let remaining = v["tablets_remaining"].as_u64().unwrap_or(u64::MAX);
            let node_status = v["status"].as_str().unwrap_or("");
            remaining == 0 && node_status != "Active"
        },
    );

    let (status, body) = cluster.admin(
        leader,
        "POST",
        "/admin/member/remove",
        "",
        drain_body.as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: remove failed: {body}");

    // No data lost: every pre-split key, plus both crossover writes, read
    // through the survivors after full convergence.
    let survivor = (3..7u64).find(|&n| n != victim_idx).unwrap();
    for (k, v) in CROSSOVER_KEYS {
        poll_until(
            &mut cluster,
            Duration::from_secs(30),
            seed,
            &format!("key {k} readable through a survivor"),
            |c| {
                matches!(
                    c.raw_get(survivor, "cross_t", k.as_bytes().to_vec(), true),
                    Ok(Some(ref got)) if got.as_slice() == v.as_bytes()
                )
            },
        );
    }
    poll_until(
        &mut cluster,
        Duration::from_secs(30),
        seed,
        "key b readable through a survivor",
        |c| {
            matches!(
                c.raw_get(survivor, "cross_t", b"b".to_vec(), true),
                Ok(Some(ref v)) if v.as_slice() == b"v-b2"
            )
        },
    );
    poll_until(
        &mut cluster,
        Duration::from_secs(30),
        seed,
        "key y readable through a survivor",
        |c| {
            matches!(
                c.raw_get(survivor, "cross_t", b"y".to_vec(), true),
                Ok(Some(ref v)) if v.as_slice() == b"v-y2"
            )
        },
    );

    // Metadata converged: the decommissioned node is gone from
    // membership/the address book.
    poll_until(
        &mut cluster,
        Duration::from_secs(30),
        seed,
        "the removed node disappearing from membership/the address book",
        |c| {
            let meta = c.metadata(leader);
            !meta.members.contains_key(&victim_id) && !meta.node_addrs.contains_key(&victim_id)
        },
    );

    // Fresh writes on both halves of the split still work post-convergence.
    cluster
        .put_raw(survivor, "cross_t", b"b2".to_vec(), b"v-b3".to_vec())
        .unwrap_or_else(|e| panic!("seed={seed}: post-convergence put_raw(b2) failed: {e}"));
    cluster
        .put_raw(survivor, "cross_t", b"y2".to_vec(), b"v-y3".to_vec())
        .unwrap_or_else(|e| panic!("seed={seed}: post-convergence put_raw(y2) failed: {e}"));
    poll_until(
        &mut cluster,
        Duration::from_secs(20),
        seed,
        "key b2 readable",
        |c| {
            matches!(
                c.raw_get(survivor, "cross_t", b"b2".to_vec(), true),
                Ok(Some(ref v)) if v.as_slice() == b"v-b3"
            )
        },
    );
    poll_until(
        &mut cluster,
        Duration::from_secs(20),
        seed,
        "key y2 readable",
        |c| {
            matches!(
                c.raw_get(survivor, "cross_t", b"y2".to_vec(), true),
                Ok(Some(ref v)) if v.as_slice() == b"v-y3"
            )
        },
    );
}

#[test]
fn decommission_racing_a_tablet_split_converges_with_no_data_loss() {
    run_decommission_racing_a_tablet_split_converges_with_no_data_loss(env_seed(0xC12B_0006));
}

#[test]
fn decommission_racing_a_tablet_split_converges_with_no_data_loss_over_seeds() {
    for i in 0..5 {
        run_decommission_racing_a_tablet_split_converges_with_no_data_loss(0xC12B_6000 + i);
    }
}
