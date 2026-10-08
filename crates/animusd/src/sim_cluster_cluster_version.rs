//! ADR 0073 Phase 2 (P2-C): node wiring and the admin surface, under
//! `SimCluster` (seed-reproducible; replay one seed with `ANIMUS_SEED=<seed>`).
//!
//! A per-node "binary" is `SimCluster::set_node_version` (own version profile,
//! the control `RaftNode`'s own range and the simulated handshake `ext`).
//!
//! Synthetic ranges `[1, 2]` stand in for a second release. The real
//! `MAX_SUPPORTED` is 2 since G-01 stage G-c shipped the first real gate
//! (`Gate::GlobalTables`), so a real binary's own range is `[1, 2]` too; the
//! synthetic ranges here keep these cells independent of the real constant.
//!
//! Covered here:
//! - the era starts and every role (control-only, combined, **data-only**,
//!   which has no local apply task and learns it through its mirror) feeds
//!   its own `ClusterFeatures`;
//! - **no `ReportNodeVersion` ever precedes the era** (negative control: the
//!   safety property of the whole PR, since a Phase 1 voter cannot decode the
//!   variant);
//! - the boot-time self-report, including **from a data-only node** and
//!   through a follower-connected node (the relayed-`ReportNodeVersion`
//!   regression: without the `is_relayable_command` arm the report is
//!   rejected "command not allowed over the relay path", a bimodal
//!   per-process flake the compiler cannot catch);
//! - `GET /admin/cluster-version` content on the leader, a follower and a
//!   data-only node;
//! - Finalize: success (every node, mirror included, observes the new
//!   version), refused by name with a Down member and with a member whose
//!   recorded range excludes the target, refused on a non-leader (including a
//!   data-only node), one-step / CAS validation;
//! - ADR 0073 Phase 3 (P3-A): the derived `roll` object of `GET /admin/cluster-version`
//!   across a node-by-node roll, and `GET /admin/roll-health`'s verdict
//!   agreeing with the dashboard's tablet ladder on the same state;
//! - the joiner's pure range check and the `JoinInfo` additive-field shape.

use std::time::Duration;

use animus_control::MetaCommand;
use animus_control::version::{Gate, GateSurface, VersionRange};
use animus_env::{Env, Metric};
use animus_node::{ClientRequest, ClientResponse};
use serde_json::Value;

use super::sim_cluster::SimCluster;
use crate::config::NodeRole;

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return vec![s];
    }
    (0..3).map(|i| 0x7C20_0000 + i).collect()
}

/// Converged-or-timeout poll (this crate's per-module duplicate of the shared
/// idiom).
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

const ROLES: [NodeRole; 4] = [
    NodeRole::Both,
    NodeRole::Both,
    NodeRole::Both,
    NodeRole::Data,
];
const DATA_NODE: u64 = 3;

fn new_cluster(seed: u64) -> SimCluster {
    let mut cluster = SimCluster::new_with_roles(seed, &ROLES, 2);
    let _ = cluster.control_leader_index();
    cluster
}

fn leader_node(cluster: &mut SimCluster) -> u64 {
    let idx = cluster.control_leader_index();
    cluster.control_node_id(idx)
}

fn a_follower(cluster: &mut SimCluster) -> u64 {
    let leader = leader_node(cluster);
    (0..3u64).find(|n| *n != leader).expect("a follower")
}

fn get_view(cluster: &mut SimCluster, node: u64) -> Value {
    let (status, body) = cluster.admin(node, "GET", "/admin/cluster-version", "", b"");
    assert_eq!(
        status, 200,
        "GET /admin/cluster-version on node {node}: {body}"
    );
    serde_json::from_str(&body).expect("view json")
}

fn post_finalize(cluster: &mut SimCluster, node: u64, body: &str) -> (u16, Value) {
    let (status, body) = cluster.admin(
        node,
        "POST",
        "/admin/cluster-version/finalize",
        "",
        body.as_bytes(),
    );
    let v = serde_json::from_str(&body).unwrap_or(Value::String(body));
    (status, v)
}

/// Start the era with every node a `[1, hi]` binary and wait until every node
/// (the data-only one through its mirror) sees it and every node has a
/// replicated record.
fn start_era(cluster: &mut SimCluster, seed: u64, hi: u32) {
    cluster.set_all_node_versions(Some(VersionRange::new(1, hi)));
    poll_until(
        cluster,
        Duration::from_secs(60),
        seed,
        "the era to start and every node to hold a record",
        |c| {
            (0..ROLES.len() as u64).all(|n| c.features(n).era_active())
                && (0..ROLES.len() as u64).all(|n| c.metadata(n).node_versions.len() == ROLES.len())
        },
    );
}

// ---------------------------------------------------------------------------

fn run_no_report_before_the_era(seed: u64) {
    let mut cluster = new_cluster(seed);
    // Default `SimCluster` node: a Phase 1 `RaftNode` (never evaluates P)
    // under a feeder that would self-report if it could. The era must never
    // start and nothing version-shaped may ever be replicated.
    cluster.run_for(Duration::from_secs(20));
    for node in 0..ROLES.len() as u64 {
        let meta = cluster.metadata(node);
        assert_eq!(
            meta.cluster_version, 0,
            "seed={seed}: node {node} era started"
        );
        assert!(
            meta.node_versions.is_empty(),
            "seed={seed}: node {node} holds a version record before the era"
        );
        assert!(
            !cluster.features(node).era_active(),
            "seed={seed}: node {node} reads the era as active"
        );
    }
}

#[test]
fn no_report_node_version_is_ever_proposed_before_the_era() {
    for seed in seeds() {
        run_no_report_before_the_era(seed);
    }
}

fn run_era_starts_and_every_role_is_fed(seed: u64) {
    let mut cluster = new_cluster(seed);
    start_era(&mut cluster, seed, 1);
    for node in 0..ROLES.len() as u64 {
        let f = cluster.features(node);
        assert!(f.era_active(), "seed={seed}: node {node}");
        assert_eq!(f.cluster_version(), 1, "seed={seed}: node {node}");
        assert!(
            cluster.version_halt(node).is_none(),
            "seed={seed}: node {node}"
        );
    }
}

#[test]
fn the_era_starts_and_every_role_feeds_its_cluster_features() {
    for seed in seeds() {
        run_era_starts_and_every_role_is_fed(seed);
    }
}

/// Switch the era upkeep (`era_on_proposals`) off on EVERY control node (after
/// any `set_node_version`, which would otherwise restore a node's range), so a
/// mid-test leadership change cannot hand the report to an upkeep-capable
/// leader and mask a missing relay arm.
fn disable_all_upkeep(cluster: &mut SimCluster) {
    for n in 0..3u64 {
        cluster.set_raft_own_range(n, None);
    }
}

fn run_report_from_data_only_node_lands(seed: u64) {
    let mut cluster = new_cluster(seed);
    start_era(&mut cluster, seed, 2);
    // The data-only node (no local `RaftNode`) re-reports a different range:
    // the report must travel the relayed `ProposeSchema` path (the
    // `ReportNodeVersion` relay arm), not a local propose.
    // The leader's own upkeep (`era_on_proposals`, which would also notice a
    // changed observed range) is switched off, so ONLY the node's own
    // self-report can land: a regression for the `ReportNodeVersion` relay
    // arm (the report is otherwise rejected "not allowed over the relay path").
    cluster.set_node_version(DATA_NODE, Some(VersionRange::new(1, 1)));
    disable_all_upkeep(&mut cluster);
    let id = cluster.handle().env(DATA_NODE).node_id();
    poll_until(
        &mut cluster,
        Duration::from_secs(60),
        seed,
        "the data-only node's re-report to land",
        |c| {
            let leader = leader_node(c);
            c.metadata(leader)
                .node_versions
                .get(&id)
                .is_some_and(|v| v.range == VersionRange::new(1, 1))
        },
    );
}

#[test]
fn a_data_only_nodes_self_report_is_relayed_and_lands() {
    for seed in seeds() {
        run_report_from_data_only_node_lands(seed);
    }
}

fn run_report_from_follower_lands(seed: u64) {
    let mut cluster = new_cluster(seed);
    start_era(&mut cluster, seed, 2);
    let follower = a_follower(&mut cluster);
    cluster.set_node_version(follower, Some(VersionRange::new(1, 1)));
    disable_all_upkeep(&mut cluster);
    let id = cluster.handle().env(follower).node_id();
    poll_until(
        &mut cluster,
        Duration::from_secs(60),
        seed,
        "a follower-connected node's re-report to land",
        |c| {
            let leader = leader_node(c);
            c.metadata(leader)
                .node_versions
                .get(&id)
                .is_some_and(|v| v.range == VersionRange::new(1, 1))
        },
    );
}

#[test]
fn a_follower_connected_nodes_self_report_is_relayed_and_lands() {
    for seed in seeds() {
        run_report_from_follower_lands(seed);
    }
}

fn run_view_content(seed: u64) {
    let mut cluster = new_cluster(seed);
    start_era(&mut cluster, seed, 2);
    let leader = leader_node(&mut cluster);
    let follower = a_follower(&mut cluster);
    for node in [leader, follower, DATA_NODE] {
        let v = get_view(&mut cluster, node);
        assert_eq!(v["era_active"], true, "seed={seed} node {node}: {v}");
        assert_eq!(v["active"], 1, "seed={seed} node {node}: {v}");
        assert_eq!(v["target"], 2, "seed={seed} node {node}: {v}");
        assert_eq!(v["safe_target"], 2, "seed={seed} node {node}: {v}");
        assert_eq!(v["can_finalize"], true, "seed={seed} node {node}: {v}");
        assert!(v["blockers"].as_array().unwrap().is_empty(), "{v}");
        let nodes = v["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), ROLES.len(), "seed={seed} node {node}: {v}");
        assert!(nodes.iter().all(|n| n["reported"] == true), "{v}");
        // The live observation table exists only on the control leader.
        let has_observed = nodes.iter().any(|n| !n["observed_range"].is_null());
        assert_eq!(has_observed, node == leader, "seed={seed} node {node}: {v}");
    }
}

#[test]
fn the_cluster_version_view_is_served_by_every_node_and_observed_only_on_the_leader() {
    for seed in seeds() {
        run_view_content(seed);
    }
}

fn run_finalize_success_and_refusals(seed: u64) {
    let mut cluster = new_cluster(seed);
    start_era(&mut cluster, seed, 2);
    let leader = leader_node(&mut cluster);
    let follower = a_follower(&mut cluster);

    // Not the leader: a follower and the data-only node are both refused with
    // the not-leader message, and nothing changes.
    for node in [follower, DATA_NODE] {
        let (status, v) = post_finalize(&mut cluster, node, "{}");
        assert_eq!(status, 409, "seed={seed} node {node}: {v}");
        assert!(
            v["error"]
                .as_str()
                .unwrap()
                .contains("not the control-plane leader"),
            "seed={seed} node {node}: {v}"
        );
    }
    assert_eq!(cluster.metadata(leader).cluster_version, 1);

    // One step at a time, and the CAS.
    let (status, v) = post_finalize(&mut cluster, leader, r#"{"to":3}"#);
    assert_eq!(status, 400, "seed={seed}: {v}");
    let (status, v) = post_finalize(&mut cluster, leader, r#"{"expected":7}"#);
    assert_eq!(status, 409, "seed={seed}: {v}");

    // Success.
    let (status, v) = post_finalize(&mut cluster, leader, r#"{"to":2,"expected":1}"#);
    assert_eq!(status, 200, "seed={seed}: {v}");
    assert_eq!(v["active"], 2);
    poll_until(
        &mut cluster,
        Duration::from_secs(30),
        seed,
        "every node (data-only mirror included) to observe cluster version 2",
        |c| (0..ROLES.len() as u64).all(|n| c.features(n).cluster_version() == 2),
    );
    let v = get_view(&mut cluster, DATA_NODE);
    assert_eq!(v["active"], 2, "{v}");
    // Our own max is 2: version 3 is out of reach.
    assert_eq!(v["can_finalize"], false, "{v}");
    let (status, v) = post_finalize(&mut cluster, leader, "{}");
    assert_eq!(status, 409, "seed={seed}: {v}");
}

#[test]
fn finalize_succeeds_on_the_leader_and_is_refused_elsewhere() {
    for seed in seeds() {
        run_finalize_success_and_refusals(seed);
    }
}

/// Finalize on a just-elected leader: the leader's apply-task cache may lag its
/// committed log, so the handler waits for apply to catch up (issue #406
/// pattern) before reading the version. A stale read would answer a false
/// "expected 1 but the active version is 1"-shaped refusal or re-propose the
/// already-applied step.
fn run_finalize_on_a_new_leader_sees_current_state(seed: u64) {
    let mut cluster = new_cluster(seed);
    start_era(&mut cluster, seed, 3);
    let first = leader_node(&mut cluster);
    let (status, v) = post_finalize(&mut cluster, first, r#"{"to":2,"expected":1}"#);
    assert_eq!(status, 200, "seed={seed}: {v}");
    cluster.crash(first);
    // Polls (bounded) for the survivors' real election; the crashed node
    // keeps believing it leads (muted, not stopped).
    let idx = cluster.control_leader_index_excluding(first);
    let second = cluster.control_node_id(idx);
    // Immediately, with no settle: the CAS on the NEW version must hold.
    let (status, v) = post_finalize(&mut cluster, second, r#"{"to":3,"expected":2}"#);
    assert_eq!(status, 200, "seed={seed}: {v}");
    assert_eq!(v["active"], 3, "seed={seed}: {v}");
}

#[test]
fn finalize_on_a_new_leader_sees_the_previous_finalize() {
    for seed in seeds() {
        run_finalize_on_a_new_leader_sees_current_state(seed);
    }
}

fn run_finalize_blocked_by_a_down_member(seed: u64) {
    let mut cluster = new_cluster(seed);
    start_era(&mut cluster, seed, 2);
    let leader = leader_node(&mut cluster);
    let victim = (0..3u64).find(|n| *n != leader).unwrap();
    let victim_id = cluster.handle().env(victim).node_id();
    cluster.crash(victim);
    poll_until(
        &mut cluster,
        Duration::from_secs(60),
        seed,
        "the failure detector to mark the crashed member Down",
        |c| {
            c.metadata(leader)
                .members
                .get(&victim_id)
                .is_some_and(|m| m.status == animus_control::meta::NodeStatus::Down)
        },
    );
    let v = get_view(&mut cluster, leader);
    assert_eq!(v["can_finalize"], false, "seed={seed}: {v}");
    let blockers = v["blockers"].as_array().unwrap();
    assert!(
        blockers.iter().any(|b| b["node"] == victim_id.to_string().as_str()
            && b["reason"] == "member is Down"),
        "seed={seed}: {v}"
    );
    // Strict: no recorded range overrides it.
    assert!(
        cluster
            .metadata(leader)
            .node_versions
            .contains_key(&victim_id)
    );
    let (status, v) = post_finalize(&mut cluster, leader, "{}");
    assert_eq!(status, 409, "seed={seed}: {v}");
    let msg = v["error"].as_str().unwrap();
    assert!(
        msg.contains(&victim_id.to_string()) && msg.contains("member is Down"),
        "seed={seed}: {v}"
    );
    assert_eq!(
        cluster.metadata(leader).cluster_version,
        1,
        "seed={seed}: a refused Finalize must leave the cluster unaffected"
    );
}

#[test]
fn finalize_is_refused_by_name_while_a_member_is_down() {
    for seed in seeds() {
        run_finalize_blocked_by_a_down_member(seed);
    }
}

fn run_finalize_blocked_by_a_range_excluding_the_target(seed: u64) {
    let mut cluster = new_cluster(seed);
    start_era(&mut cluster, seed, 2);
    let leader = leader_node(&mut cluster);
    // The data-only node's binary tops out at version 1: once its re-report
    // lands, it does not support the target.
    cluster.set_node_version(DATA_NODE, Some(VersionRange::new(1, 1)));
    let id = cluster.handle().env(DATA_NODE).node_id();
    poll_until(
        &mut cluster,
        Duration::from_secs(60),
        seed,
        "the data-only node's narrower range to be recorded",
        |c| {
            c.metadata(leader)
                .node_versions
                .get(&id)
                .is_some_and(|v| v.range == VersionRange::new(1, 1))
        },
    );
    let (status, v) = post_finalize(&mut cluster, leader, "{}");
    assert_eq!(status, 409, "seed={seed}: {v}");
    let msg = v["error"].as_str().unwrap();
    assert!(
        msg.contains(&id.to_string()) && msg.contains("excludes target 2"),
        "seed={seed}: {v}"
    );
    let view = get_view(&mut cluster, leader);
    assert_eq!(view["safe_target"], 1, "seed={seed}: {view}");
    assert_eq!(cluster.metadata(leader).cluster_version, 1);
}

#[test]
fn finalize_is_refused_by_name_while_a_member_does_not_report_the_target() {
    for seed in seeds() {
        run_finalize_blocked_by_a_range_excluding_the_target(seed);
    }
}

fn run_out_of_range_binary_latches_the_halt(seed: u64) {
    let mut cluster = new_cluster(seed);
    start_era(&mut cluster, seed, 1);
    // A binary that supports only [3, 4] is started against a cluster at 1:
    // the feeder latches the named halt (the process exits on it).
    cluster.set_node_version(DATA_NODE, Some(VersionRange::new(3, 4)));
    poll_until(
        &mut cluster,
        Duration::from_secs(30),
        seed,
        "the out-of-range data-only node to latch a halt",
        |c| c.version_halt(DATA_NODE).is_some(),
    );
    assert_eq!(
        cluster.version_halt(DATA_NODE).unwrap(),
        "cluster version 1 is below this binary's min 3 \
         (upgrade through a release whose range contains 1 first)",
        "seed={seed}"
    );
    // An in-range node never halts.
    assert!(cluster.version_halt(0).is_none());
}

#[test]
fn an_out_of_range_binary_latches_the_named_halt() {
    for seed in seeds() {
        run_out_of_range_binary_latches_the_halt(seed);
    }
}

#[test]
fn join_info_carries_the_cluster_version_only_when_the_era_is_on() {
    use animus_node::ClientResponse;
    let base = || ClientResponse::JoinInfo {
        control_ids: vec![],
        peers: Default::default(),
        client_route: Default::default(),
        intra_route: Default::default(),
        admin_addrs: vec![],
        cluster_version: 0,
    };
    // Pre-era bytes carry no new key at all: identical to Phase 1's.
    let pre = serde_json::to_string(&base()).unwrap();
    assert!(!pre.contains("cluster_version"), "{pre}");
    // A Phase 1 reply (no field) still decodes, as version 0 (reads as 1).
    let decoded: ClientResponse = serde_json::from_str(&pre).unwrap();
    assert!(matches!(
        decoded,
        ClientResponse::JoinInfo {
            cluster_version: 0,
            ..
        }
    ));
    let mut on = base();
    if let ClientResponse::JoinInfo {
        cluster_version, ..
    } = &mut on
    {
        *cluster_version = 2;
    }
    let era = serde_json::to_string(&on).unwrap();
    assert!(era.contains("\"cluster_version\":2"), "{era}");
}

fn run_admission_refuses_an_unversioned_control_voter(seed: u64) {
    let mut cluster = new_cluster(seed);
    start_era(&mut cluster, seed, 1);
    let leader = leader_node(&mut cluster);
    // A node nobody has ever heard a range from (a Phase 1 binary would look
    // exactly like this): refused by name, nothing registered.
    let (status, body) = cluster.admin(
        leader,
        "POST",
        "/admin/control/member/add",
        "",
        br#"{"node":"ghost","addr":"127.0.0.1:1"}"#,
    );
    assert_eq!(status, 409, "seed={seed}: {body}");
    assert!(
        body.contains("ghost") && body.contains("no known version range"),
        "seed={seed}: {body}"
    );
    let meta = cluster.metadata(leader);
    let ghost = animus_env::NodeId::propose("ghost").unwrap();
    assert!(
        !meta.node_addrs.contains_key(&ghost) && !meta.members.contains_key(&ghost),
        "seed={seed}: a refused admission must register nothing"
    );
}

#[test]
fn an_era_on_cluster_refuses_a_control_voter_with_no_known_range() {
    for seed in seeds() {
        run_admission_refuses_an_unversioned_control_voter(seed);
    }
}

// ---------------------------------------------------------------------------
// P2-B -> P2-C handoffs (close-out): relay receiver gate check, the control-fed
// handle in every hosted group, and the exported violation levels.

/// A relayed era-only command, as a follower-connected node would forward it.
fn report_for(cluster: &SimCluster, node: u64) -> ClientRequest {
    ClientRequest::ProposeSchema(MetaCommand::ReportNodeVersion {
        node: cluster.handle().env(node).node_id(),
        range: VersionRange::new(1, 1),
        build: "relay-gate-test".into(),
    })
}

fn run_relay_receiver_refuses_a_closed_gate(seed: u64) {
    let mut cluster = new_cluster(seed);
    let leader = leader_node(&mut cluster);
    let follower = a_follower(&mut cluster);
    // Pre-era: every gate above Base is closed on every node. A follower-
    // connected node relaying an era-only command to the leader, and the
    // leader relaying to a follower, are both refused by name by the
    // RECEIVER (the sender here is a sim relay, which has no gate of its own:
    // production's sender gate is `AnimusdRelayClient`, tested separately).
    for (from, to) in [(follower, leader), (leader, follower)] {
        let before = cluster.metric(to, Metric::ClusterGateRelayRefused);
        let resp = cluster
            .relay_request(from, to, report_for(&cluster, from))
            .unwrap_or_else(|| panic!("seed={seed}: relay {from}->{to} never resolved"));
        match resp {
            ClientResponse::Error(msg) => assert!(
                msg.contains("relayed command refused") && msg.contains("Era"),
                "seed={seed}: {from}->{to}: {msg}"
            ),
            other => panic!("seed={seed}: {from}->{to} was not refused: {other:?}"),
        }
        assert_eq!(
            cluster.metric(to, Metric::ClusterGateRelayRefused),
            before + 1,
            "seed={seed}: refusal not counted on node {to}"
        );
    }
    // Nothing was proposed: no record, no era, anywhere.
    cluster.run_for(Duration::from_secs(5));
    for node in 0..ROLES.len() as u64 {
        let meta = cluster.metadata(node);
        assert!(
            meta.node_versions.is_empty() && meta.cluster_version == 0,
            "seed={seed}: node {node} saw a version record from a refused relay"
        );
    }

    // Positive control: with the era on (every gate the command needs open),
    // the identical relay is accepted and lands.
    start_era(&mut cluster, seed, 2);
    disable_all_upkeep(&mut cluster);
    let resp = cluster
        .relay_request(follower, leader, report_for(&cluster, follower))
        .unwrap_or_else(|| panic!("seed={seed}: era-on relay never resolved"));
    assert!(
        matches!(resp, ClientResponse::PutOk),
        "seed={seed}: an era-on relay was refused: {resp:?}"
    );
}

#[test]
fn a_relayed_command_whose_gate_is_closed_is_refused_by_the_receiving_node() {
    for seed in seeds() {
        run_relay_receiver_refuses_a_closed_gate(seed);
    }
}

fn run_hosted_groups_share_the_nodes_handle(seed: u64) {
    // Replication over every node, the data-only one included, so every role
    // (a local `RaftNode`'s applied view, and the mirror-fed data-only node)
    // hosts at least one group.
    let mut cluster = SimCluster::new_with_roles(seed, &ROLES, ROLES.len());
    let _ = cluster.control_leader_index();
    cluster.create_table("gate_handles");
    for node in 0..ROLES.len() as u64 {
        assert!(
            !cluster.hosted_group_features(node).is_empty(),
            "seed={seed}: node {node} hosts no group"
        );
    }
    // The groups were started pre-era, on a floor view. A group on a private
    // handle would never learn the era; one on the node's control-fed handle
    // must.
    for node in 0..ROLES.len() as u64 {
        for f in cluster.hosted_group_features(node) {
            assert!(!f.era_active(), "seed={seed}: node {node} era before start");
        }
    }
    start_era(&mut cluster, seed, 2);
    poll_until(
        &mut cluster,
        Duration::from_secs(30),
        seed,
        "every hosted group's handle to see the era",
        |c| {
            (0..ROLES.len() as u64).all(|n| {
                c.hosted_group_features(n)
                    .iter()
                    .all(|f| f.era_active() && f.cluster_version() == 1)
            })
        },
    );
    // And a finalize reaches them too (the version, not only the era flag).
    let leader = leader_node(&mut cluster);
    let (status, v) = post_finalize(&mut cluster, leader, r#"{"to":2,"expected":1}"#);
    assert_eq!(status, 200, "seed={seed}: {v}");
    poll_until(
        &mut cluster,
        Duration::from_secs(30),
        seed,
        "every hosted group's handle to see cluster version 2",
        |c| {
            (0..ROLES.len() as u64).all(|n| {
                c.hosted_group_features(n)
                    .iter()
                    .all(|f| f.cluster_version() == 2)
            })
        },
    );
}

#[test]
fn every_hosted_group_runs_on_its_nodes_control_fed_feature_handle() {
    for seed in seeds() {
        run_hosted_groups_share_the_nodes_handle(seed);
    }
}

fn run_violation_levels_are_exported(seed: u64) {
    let cluster = new_cluster(seed);
    let f = cluster.features(1);
    let all = [
        (GateSurface::RaftMsg, Metric::ClusterGateViolationsRaftMsg),
        (
            GateSurface::MetaCommand,
            Metric::ClusterGateViolationsMetaCommand,
        ),
        (GateSurface::KvWire, Metric::ClusterGateViolationsKvWire),
        (
            GateSurface::KvCommand,
            Metric::ClusterGateViolationsKvCommand,
        ),
        (
            GateSurface::ClientRequest,
            Metric::ClusterGateViolationsClientRequest,
        ),
        (
            GateSurface::ClientResponse,
            Metric::ClusterGateViolationsClientResponse,
        ),
    ];
    for (_, m) in all {
        assert_eq!(cluster.metric(1, m), 0, "seed={seed}: {m:?} starts at 0");
    }
    // `check` counts BEFORE its `debug_assert!`, so the counter moves in both
    // build profiles; the panic of a debug build is caught here.
    for (i, (surface, _)) in all.iter().enumerate() {
        for _ in 0..=i {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                f.check(*surface, Gate::Era)
            }));
        }
    }
    for (i, (surface, metric)) in all.iter().enumerate() {
        assert_eq!(f.violations(*surface), i as u64 + 1);
        assert_eq!(
            cluster.metric(1, *metric),
            i as u64 + 1,
            "seed={seed}: {metric:?} is not the surface's counter"
        );
        // Per-node: another node's levels are its own.
        assert_eq!(
            cluster.metric(0, *metric),
            0,
            "seed={seed}: {metric:?} leaked"
        );
    }
}

#[test]
fn gate_violation_counters_are_exported_per_surface_as_metrics() {
    for seed in seeds() {
        run_violation_levels_are_exported(seed);
    }
}

// ---- ADR 0073 Phase 3, P3-A: roll status and roll health ----

fn get_roll_health(cluster: &mut SimCluster, node: u64) -> Value {
    let (status, body) = cluster.admin(node, "GET", "/admin/roll-health", "", b"");
    assert_eq!(status, 200, "GET /admin/roll-health on node {node}: {body}");
    serde_json::from_str(&body).expect("roll-health json")
}

fn run_roll_object_tracks_a_node_by_node_roll(seed: u64) {
    let mut cluster = new_cluster(seed);
    // The era is on with every node on the "old" binary: range [1,1].
    start_era(&mut cluster, seed, 1);
    let leader = leader_node(&mut cluster);
    let v = get_view(&mut cluster, leader);
    assert_eq!(v["roll"]["phase"], "not_started", "seed={seed}: {v}");
    assert_eq!(v["roll"]["on_new"], 0, "seed={seed}: {v}");
    assert_eq!(v["roll"]["total"], ROLES.len(), "seed={seed}: {v}");

    // Roll order: the data-only node first, the control leader last.
    let order: Vec<String> = v["roll"]["remaining"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap().to_string())
        .collect();
    let data_id = cluster.handle().env(DATA_NODE).node_id().to_string();
    let leader_id = cluster.handle().env(leader).node_id().to_string();
    assert_eq!(order.len(), ROLES.len(), "seed={seed}: {order:?}");
    assert_eq!(order.first(), Some(&data_id), "seed={seed}: {order:?}");
    assert_eq!(order.last(), Some(&leader_id), "seed={seed}: {order:?}");

    // Roll the nodes in that order: each re-reports a [1,2] range.
    let node_of = |c: &SimCluster, id: &str| -> u64 {
        (0..ROLES.len() as u64)
            .find(|n| c.handle().env(*n).node_id().to_string() == id)
            .expect("node by id")
    };
    for (i, id) in order.iter().enumerate() {
        let n = node_of(&cluster, id);
        cluster.set_node_version(n, Some(VersionRange::new(1, 2)));
        let want = i + 1;
        poll_until(
            &mut cluster,
            Duration::from_secs(60),
            seed,
            "the rolled node's new range to be recorded and visible",
            |c| get_view(c, leader)["roll"]["on_new"] == want,
        );
        let v = get_view(&mut cluster, leader);
        let phase = v["roll"]["phase"].as_str().unwrap().to_string();
        if want < ROLES.len() {
            assert_eq!(phase, "rolling", "seed={seed} after {want}: {v}");
            assert_eq!(
                v["roll"]["remaining"].as_array().unwrap().len(),
                ROLES.len() - want,
                "seed={seed}: {v}"
            );
        } else {
            assert_eq!(phase, "ready_to_finalize", "seed={seed}: {v}");
            assert_eq!(v["can_finalize"], true, "seed={seed}: {v}");
            assert!(v["roll"]["remaining"].as_array().unwrap().is_empty());
        }
    }
    // The same object is served by a follower and the data-only node.
    for node in [a_follower(&mut cluster), DATA_NODE] {
        let v = get_view(&mut cluster, node);
        assert_eq!(v["roll"]["phase"], "ready_to_finalize", "seed={seed}: {v}");
        assert!(v["roll"]["health"]["ok"].is_boolean(), "seed={seed}: {v}");
    }
}

#[test]
fn the_roll_object_tracks_a_node_by_node_roll() {
    for seed in seeds() {
        run_roll_object_tracks_a_node_by_node_roll(seed);
    }
}

/// The server verdict equals the dashboard ladder on the same state: with a
/// healthy cluster `ok` is true on every node (the data-only one included);
/// after a member dies the verdict names it, and the tablet counts equal what
/// the shared ladder (`roll_health::tablet_status`, itself proven equal to
/// `dashboard_core.js` by `roll_health`'s oracle test) computes from the
/// leader's own `Metadata`.
fn run_roll_health_matches_dashboard_ladder(seed: u64) {
    use crate::roll_health::{TabletStatus, tablet_status};
    let mut cluster = new_cluster(seed);
    start_era(&mut cluster, seed, 2);
    let tablet = cluster.create_table("rolled");
    let leader = leader_node(&mut cluster);

    // Healthy and converged: ok everywhere, with the data-only node's own
    // answer coming off its mirror.
    for node in 0..ROLES.len() as u64 {
        poll_until(
            &mut cluster,
            Duration::from_secs(60),
            seed,
            "roll-health to report ok on a healthy cluster",
            |c| get_roll_health(c, node)["ok"] == true,
        );
    }

    // Kill a non-leader member that hosts a replica of the table's tablet.
    let replicas = cluster.metadata(leader).tablets[&tablet].replicas.clone();
    let victim = (0..ROLES.len() as u64)
        .find(|n| *n != leader && replicas.contains(&cluster.handle().env(*n).node_id()))
        .expect("a non-leader replica");
    let victim_id = cluster.handle().env(victim).node_id();
    cluster.crash(victim);
    poll_until(
        &mut cluster,
        Duration::from_secs(60),
        seed,
        "the crashed member to be marked Down",
        |c| {
            c.metadata(leader)
                .members
                .get(&victim_id)
                .is_some_and(|m| m.status == animus_control::meta::NodeStatus::Down)
        },
    );
    // One snapshot, one verdict over it, no simulated time between (the
    // repair loop would otherwise move the replica away mid-assertion).
    let meta = cluster.metadata(leader);
    let h = cluster.roll_health_over(leader, &meta);
    assert_eq!(h["ok"], false, "seed={seed}: {h}");
    let reasons = h["reasons"].as_array().unwrap();
    assert!(
        reasons.iter().any(
            |r| r["kind"] == "member_not_active" && r["node"] == victim_id.to_string().as_str()
        ),
        "seed={seed}: {h}"
    );
    assert_eq!(h["members"]["not_active"][0]["status"], "Down", "{h}");

    // Ladder agreement, on the very same snapshot.
    let (mut ql, mut ur) = (0u64, 0u64);
    for t in meta.tablets.values() {
        match tablet_status(&t.replicas, &meta.members, true, t.replicas.len()) {
            TabletStatus::QuorumLost => ql += 1,
            TabletStatus::UnderReplicated => ur += 1,
            _ => {}
        }
    }
    assert!(
        ur + ql > 0,
        "seed={seed}: the victim hosted no tablet replica, the scenario proves nothing: {meta:?}"
    );
    assert_eq!(h["tablets"]["quorum_lost"], ql, "seed={seed}: {h}");
    assert_eq!(h["tablets"]["under_replicated"], ur, "seed={seed}: {h}");
    // The embedded summary in cluster-version is the same verdict.
    // (The member stays Down: nothing restarts it, and repair only moves
    // replicas.) The endpoint serves the same shape as the snapshot verdict.
    let v = get_view(&mut cluster, leader);
    assert_eq!(v["roll"]["health"]["ok"], false, "seed={seed}: {v}");
    assert_eq!(v["roll"]["down"][0], victim_id.to_string().as_str(), "{v}");
    assert_eq!(v["roll"]["phase"], "blocked", "seed={seed}: {v}");
    let h2 = get_roll_health(&mut cluster, leader);
    assert_eq!(h2["ok"], false, "seed={seed}: {h2}");
}

#[test]
fn roll_health_matches_dashboard_ladder() {
    for seed in seeds() {
        run_roll_health_matches_dashboard_ladder(seed);
    }
}
