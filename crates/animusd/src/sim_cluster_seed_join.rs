//! `SimCluster`-driven deterministic coverage for the real seed/join dial
//! (ADR 0061 rung M, C-13 PR 2/3) — `SimCluster::join_via_seed`/
//! `join_via_seed_with_role`, their `_via_relay` siblings, and
//! `forwarding::handle_relayed_request`'s `ClientRequest::JoinInfo` arm.
//! See `sim_cluster.rs`'s own doc on `SimCluster::join_via_seed_with_role`
//! for the full mechanism (what it drives for real, the two documented
//! forks it resolves, what it deliberately does not prove) and
//! `crates/animusd/CLAUDE.md`'s matching appendix for the cross-cutting
//! account. PR 2's own scope was the smallest end-to-end slice — self-
//! minted identity, combined mode, a single joiner — proving the dial
//! primitive itself; PR 3 adds the data-only role arm and the first real
//! conversion, `tests/data_join.rs`'s own scenario (see (c) below).
//! `seed_join.rs`/`seed_join_allocated.rs` stay later PRs' own scope.
//!
//! **What each scenario proves, and how**:
//!
//! (a) `joiner_discovers_claims_and_is_promoted_by_the_real_detector` — a
//!     3-node combined `SimCluster`, joined via a seed that is deliberately
//!     a control FOLLOWER, not the leader (`SimCluster::
//!     control_follower_index`) — exercising `MetaCommand::RegisterNode`'s
//!     `is_relayable_command` allowlist relay-to-leader path, not just the
//!     leader-local fast path. Asserts, in order: the joiner's own id was
//!     self-minted (present in the diff of `Metadata::members` before/
//!     after, and — the mint's own 22-char base64url shape — never the
//!     index-derived `nid()` a bypass or a `grow`-style fixture would use);
//!     the member reached `Active` on every node's own view; promotion took
//!     genuine, non-trivial virtual time (see "Why the timing proxy,
//!     specifically" below — this scenario's own proof that the real
//!     detector, not a bypass, decided it, since `join_via_seed` itself
//!     never proposes `UpsertMember` at all, by construction — grep its own
//!     body); and the joined node's own `client_route`/`intra_route` (built
//!     from the `JoinInfo` discovery reply, per `join_via_seed`'s own doc
//!     on the route-table fork) are genuinely usable — a `put`/`get`
//!     issued through the JOINED node's own `ClientCtx`, which hosts no
//!     replica of the table at all, must forward correctly to the real
//!     tablet leader.
//! (b) `rejoin_same_identity_is_a_noop` — after a normal join, re-runs the
//!     identical relayed `RegisterNode` CAS for the SAME `(id, addrs)` via
//!     `SimCluster::rejoin_same_identity` and asserts it is accepted as an
//!     idempotent no-op (never a collision) — the ADR 0032 same-identity-
//!     rejoin contract, proven directly against the new relay-based
//!     `register_node_over_wire_via_relay` sibling rather than a second
//!     full `join_via_seed` call (this rung's own combined-mode dial only
//!     ever self-mints, so a literal "the SAME process rejoins" scenario
//!     needs the explicit-`--id` claim branch `claim_join_identity_via_
//!     relay` does not build yet — see that function's own doc; C-13 PR 4
//!     is where `seed_join.rs`'s own `rejoin_same` scenario gets a full
//!     conversion). A second call with the SAME id but DIFFERENT `addrs`
//!     is asserted to be a genuine collision, proving the CAS actually
//!     discriminates rather than always answering "registered."
//! (c) `data_only_joiner_over_a_split_deployment_gets_a_rebalanced_replica`
//!     (C-13 PR 3) — the data-only dual of (a), and the sim sibling for the
//!     real `tests/data_join.rs`'s own scenario: a **split** deployment (3
//!     control-only + 2 data-only, `SimCluster::new_with_roles`, mirroring
//!     `support::bring_up_split(3, 2, ..)`), three independently-provisioned
//!     tables (wire-created, `create_table_via_wire` — never `SimCluster::
//!     create_table`'s own hand-hosted shortcut, which picks replicas
//!     `0..replication` and would put them on the CONTROL-only nodes in a
//!     mixed cluster) written through the two pre-existing data nodes, then
//!     a THIRD data-only node joined via [`SimCluster::join_via_seed_with_
//!     role`]`(seed, NodeRole::Data)` against a control-only seed (again
//!     deliberately a FOLLOWER, not the leader — see (a)'s own reasoning).
//!     Asserts, in order, mirroring `data_join.rs`'s own six numbered steps
//!     one for one: the joiner's id is self-minted (same shape checks as
//!     (a)); every node's view reaches `Active` **through the real
//!     detector** (the same non-instantaneous timing proxy as (a) — `join_
//!     via_seed_with_role`'s data arm proposes no `UpsertMember` either, by
//!     construction); the real placement reconciler — not a fixture stand-
//!     in — eventually lands a real tablet replica on it (`MAX_REPLICATION_
//!     FACTOR` is always the *recorded target* regardless of how many
//!     candidates were `Active` at `CreateTable` time, per `ClientCtx::
//!     provision_tablet`'s own doc in `schema.rs` — with only 2 data nodes
//!     initially and a target of 3, every one of the three tables is
//!     already under-replicated from creation, so `reconcile_placement`'s
//!     violation-repair path, not merely balance, is what lands the third
//!     replica the moment the joiner is `Active` — a stronger, more
//!     deterministic guarantee than (a)'s own doc for `data_join.rs`'s own
//!     "several tables" caveat, kept anyway to mirror the original one for
//!     one); and reads/writes round-trip **both directions** through the
//!     joined node's own `ClientCtx` for whichever table it actually ended
//!     up hosting (never an arbitrary one — the rebalancer only ever moves
//!     while it improves the *global* picture, `data_join.rs`'s own `table_
//!     with_replica` doc, restated here) against the pre-existing data
//!     nodes, proving it a genuine CP-data participant, not just a
//!     registered-but-inert member.
//!
//! **Why the timing proxy, specifically.** There is no "did this code path
//! call `propose` for `UpsertMember`" instrumentation hook anywhere in this
//! fixture (adding one purely to satisfy a test would be a fixture-only
//! capability with no production analogue, the same "no narrowed generic
//! core / no production-adjacent-only mechanism" discipline this crate's
//! own history states repeatedly) — so scenario (a) instead asserts the
//! OBSERVABLE CONSEQUENCE of the real detector deciding promotion: it
//! cannot happen before the control leader's own `detect_loop` has ticked
//! at least once after the joiner's first real heartbeat lands
//! (`animus_control::node::{DETECT_INTERVAL, HEARTBEAT_INTERVAL}`, both
//! 100ms) — a bypass propose (`SimCluster::grow`/`seed_members`'s own
//! shape) instead commits `UpsertMember{Active}` in the SAME call that
//! proposes `RegisterNode`, converging in whatever `poll_until`'s own
//! first check finds already true (near-zero virtual time). The assertion
//! itself only needs a loose, comfortably-clear-of-zero lower bound (well
//! under one full `DETECT_INTERVAL`) — it is not trying to pin the exact
//! detection latency, only to distinguish "some real heartbeat/detect
//! cycle had to run" from "committed instantly." Combined with `join_via_
//! seed`'s own doc stating, and this module's own reading of its body
//! confirming, that it never proposes `UpsertMember` anywhere, this is a
//! sound (if indirect) proof.
//!
//! **`OP_BUDGET`/`JOIN_DIAL_DRIVE_BUDGET`** (both `sim_cluster.rs`) bound
//! every op/dial call here, as everywhere else in this fixture — no
//! wall-clock wait anywhere in this module.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_control::{NodeAddrs, NodeStatus};
use animus_env::{NodeId, nid};
use animus_tablet::TabletId;

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{create_table_via_wire, tablet_of_table};
use crate::config::NodeRole;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

const PRIMARY_SEED_A: u64 = 0xC13E_0001;
const OVER_SEEDS_A: [u64; 5] = [
    0xC13E_1001,
    0xC13E_1002,
    0xC13E_1003,
    0xC13E_1004,
    0xC13E_1005,
];

const PRIMARY_SEED_B: u64 = 0xC13E_0002;
const OVER_SEEDS_B: [u64; 5] = [
    0xC13E_2001,
    0xC13E_2002,
    0xC13E_2003,
    0xC13E_2004,
    0xC13E_2005,
];

/// This cluster's current `Metadata::members` key set, as observed from
/// node 0 — every control-bearing node's own view is identical once
/// converged, and node 0 always exists.
fn member_ids(cluster: &SimCluster) -> BTreeSet<NodeId> {
    cluster.metadata(0).members.keys().cloned().collect()
}

/// The single id present in `after` but not `before` — the joiner's own
/// newly-registered member row. Panics (naming both sets) if the diff
/// isn't exactly one id, which would be a scenario bug, never a
/// legitimate outcome.
fn only_new_member(before: &BTreeSet<NodeId>, after: &BTreeSet<NodeId>) -> NodeId {
    let mut new_ids: Vec<&NodeId> = after.difference(before).collect();
    assert_eq!(
        new_ids.len(),
        1,
        "expected exactly one new member id (before={before:?}, after={after:?})"
    );
    new_ids.pop().expect("checked len == 1 above").clone()
}

/// [`SimCluster::join_via_seed`], plus the before/after `Metadata::members`
/// diff needed to recover the joiner's own self-minted id (there is no
/// direct "what id did the last join mint" accessor — the dial's own point
/// is that the caller does not get to choose it).
fn join_and_get_id(cluster: &mut SimCluster, seed_node: usize) -> (u64, NodeId) {
    let before = member_ids(cluster);
    let idx = cluster.join_via_seed(seed_node);
    let after = member_ids(cluster);
    (idx, only_new_member(&before, &after))
}

/// [`join_and_get_id`]'s role-parameterized sibling (C-13 PR 3) —
/// [`SimCluster::join_via_seed_with_role`] plus the identical before/after
/// `Metadata::members` diff.
fn join_and_get_id_with_role(
    cluster: &mut SimCluster,
    seed_node: usize,
    role: NodeRole,
) -> (u64, NodeId) {
    let before = member_ids(cluster);
    let idx = cluster.join_via_seed_with_role(seed_node, role);
    let after = member_ids(cluster);
    (idx, only_new_member(&before, &after))
}

/// The `NodeAddrs` a self-minted `join_via_seed` claim always builds for
/// `id` — the fixed "every address string is `id.to_string()`, role
/// `\"combined\"`" convention `claim_join_identity_via_relay` uses
/// (`sim_cluster.rs`). Reconstructed here rather than threaded out of
/// `join_via_seed`'s own return value, since the convention is a stable,
/// documented fact this module can rely on directly.
fn joined_addrs(id: &NodeId) -> NodeAddrs {
    let addr = id.to_string();
    NodeAddrs {
        internal: addr.clone(),
        client: addr.clone(),
        intra: addr.clone(),
        admin: addr,
        role: "combined".to_owned(),
    }
}

fn run_joiner_discovers_claims_and_is_promoted_by_the_real_detector(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    // Hosted by the pre-existing three nodes only (replication == node
    // count at construction) — the joiner's very own put/get below must
    // genuinely forward, never resolve locally.
    cluster.create_table("t1");

    let follower = cluster.control_follower_index();
    let before_time = cluster.sim_now();
    let (joined, joined_id) = join_and_get_id(&mut cluster, follower as usize);
    let after_time = cluster.sim_now();

    assert_eq!(
        joined, 3,
        "seed={seed}: the joiner should get the next sequential index"
    );

    // Self-minted, not pre-chosen: never the index-derived `nid()`, and
    // shaped like a real mint (22-char base64url — `NodeId::mint`'s own
    // 16-byte-packed encoding).
    assert_ne!(
        joined_id,
        nid(joined),
        "seed={seed}: joined id must be self-minted, not `nid({joined})` \
         (joined_id={joined_id})"
    );
    assert_eq!(
        joined_id.as_str().len(),
        22,
        "seed={seed}: a minted NodeId is a 22-char base64url string \
         (joined_id={joined_id})"
    );

    // Promoted through the REAL control-leader detector — never a bypass
    // propose (`join_via_seed` proposes no `UpsertMember` at all; see this
    // module's own doc on why the timing proxy below is the sound
    // observable consequence of that).
    let elapsed = after_time.duration_since(before_time);
    assert!(
        elapsed >= Duration::from_millis(80),
        "seed={seed}: promotion resolved in {elapsed:?} of virtual time — \
         too fast to be genuine heartbeat/detect_loop-driven promotion \
         (looks like a bypass propose)"
    );

    for n in 0..cluster.node_count() as u64 {
        let status = cluster
            .metadata(n)
            .members
            .get(&joined_id)
            .map(|m| m.status);
        assert_eq!(
            status,
            Some(NodeStatus::Active),
            "seed={seed}: node {n}'s own view of the joiner must show Active"
        );
    }

    // The joined node's own route tables came from discovery, not a patch
    // (`join_via_seed`'s own doc on the route-table fork) — proven
    // behaviorally: a put/get issued from the JOINER's own `ClientCtx`
    // (it hosts no replica of `t1` at all) must forward correctly to the
    // real tablet leader.
    cluster
        .put(joined, "t1", "pk-from-joiner", "sk", b"hello-from-joiner")
        .unwrap_or_else(|e| panic!("seed={seed}: put from the joined node ({joined}) failed: {e}"));
    let got = cluster
        .get(joined, "t1", "pk-from-joiner", "sk", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get from the joined node ({joined}) failed: {e}"));
    assert_eq!(
        got.as_deref(),
        Some(b"hello-from-joiner".as_slice()),
        "seed={seed}: put/get through the joined node's own ClientCtx must \
         round-trip (its route tables came straight from JoinInfo discovery)"
    );
}

#[test]
fn joiner_discovers_claims_and_is_promoted_by_the_real_detector() {
    run_joiner_discovers_claims_and_is_promoted_by_the_real_detector(env_seed(PRIMARY_SEED_A));
}

#[test]
fn joiner_discovers_claims_and_is_promoted_by_the_real_detector_over_seeds() {
    for &seed in &OVER_SEEDS_A {
        run_joiner_discovers_claims_and_is_promoted_by_the_real_detector(seed);
    }
}

fn run_rejoin_same_identity_is_a_noop(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let follower = cluster.control_follower_index();
    let (_joined, joined_id) = join_and_get_id(&mut cluster, follower as usize);
    let addrs = joined_addrs(&joined_id);

    // The ADR 0032 same-identity-rejoin contract: re-registering the
    // IDENTICAL (id, addrs) pair over the relay path — possibly against a
    // DIFFERENT seed than the original join used — is a CAS no-op, never a
    // collision.
    let follower_again = cluster.control_follower_index();
    let registered =
        cluster.rejoin_same_identity(follower_again as usize, joined_id.clone(), addrs.clone());
    assert!(
        registered,
        "seed={seed}: re-registering the SAME (id, addrs) must be accepted \
         as an idempotent CAS no-op, not a collision"
    );

    // The CAS actually discriminates: the SAME id with DIFFERENT addrs is
    // a genuine collision, never silently accepted.
    let mut different_addrs = addrs;
    different_addrs.client = format!("{}-different", different_addrs.client);
    let follower_third = cluster.control_follower_index();
    let collided =
        cluster.rejoin_same_identity(follower_third as usize, joined_id, different_addrs);
    assert!(
        !collided,
        "seed={seed}: re-registering the SAME id with DIFFERENT addrs must \
         be a genuine collision, never accepted"
    );
}

#[test]
fn rejoin_same_identity_is_a_noop() {
    run_rejoin_same_identity_is_a_noop(env_seed(PRIMARY_SEED_B));
}

#[test]
fn rejoin_same_identity_is_a_noop_over_seeds() {
    for &seed in &OVER_SEEDS_B {
        run_rejoin_same_identity_is_a_noop(seed);
    }
}

const PRIMARY_SEED_C: u64 = 0xC13E_0003;
const OVER_SEEDS_C: [u64; 5] = [
    0xC13E_3001,
    0xC13E_3002,
    0xC13E_3003,
    0xC13E_3004,
    0xC13E_3005,
];

/// The three independent tables [`run_data_only_joiner_over_a_split_
/// deployment_gets_a_rebalanced_replica`] seeds — mirrors `tests/
/// data_join.rs`'s own fixed `TABLES` constant (three, not one — kept to
/// mirror the original one for one even though this module's own doc notes
/// a single table already suffices here, since `MAX_REPLICATION_FACTOR` is
/// always the recorded *target*, not a point-in-time observation).
const DATA_JOIN_TABLES: [&str; 3] = ["datajoin0", "datajoin1", "datajoin2"];

/// One hash-key (`pk`, string) `CreateTable`, issued from `node` — mirrors
/// every other `sim_cluster_*` module's identically-named helper (this
/// crate's own "small fixtures duplicated per test module" convention;
/// `sim_cluster_control_data_split.rs`/`sim_cluster_split_cluster.rs` carry
/// the identical copy).
fn create_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    create_table_via_wire(cluster, node, &body)
}

/// Converged-or-timeout poll on `cond(cluster)` — the shared shape every
/// `sim_cluster_*` module's own scenario-local convergence check uses
/// (duplicated, not reached into, per this crate's own convention —
/// `sim_cluster_control_data_split.rs`/`sim_cluster_split_cluster.rs` carry
/// the identical copy).
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

/// (c) `data_only_joiner_over_a_split_deployment_gets_a_rebalanced_replica`
/// — see this module's own doc for the full six-step mapping onto `tests/
/// data_join.rs`'s own real-socket scenario.
fn run_data_only_joiner_over_a_split_deployment_gets_a_rebalanced_replica(seed: u64) {
    // 1. A split deployment: 3 control-only + 2 data-only, mirroring
    // `support::bring_up_split(3, 2, ..)`.
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 2);
    let leader = cluster.control_leader_index() as u64;

    // 2. Three independent tables, wire-created (never `SimCluster::
    // create_table`'s own hand-hosted shortcut, which would pick replicas
    // `0..replication` — the CONTROL-only nodes in this mixed cluster —
    // see this module's own doc), written through a pre-existing data node
    // (3) so the pre-join cluster genuinely holds data before the join.
    for table in DATA_JOIN_TABLES {
        let (status, body) = create_table(&mut cluster, leader, table);
        assert_eq!(
            status, 200,
            "seed={seed}: CreateTable({table}) failed: {body}"
        );
        poll_until(
            &mut cluster,
            Duration::from_secs(10),
            seed,
            &format!("a pre-existing data node hosting {table}"),
            |c| {
                let tablet = tablet_of_table(c, table);
                (3..5u64).any(|n| c.hosted_tablets(n).contains(&tablet))
            },
        );
        cluster
            .put(3, table, "k0", "sk", b"v0")
            .unwrap_or_else(|e| panic!("seed={seed}: seed put into {table} failed: {e}"));
    }

    // 3. Join a THIRD data-only node, via a control-only seed that is
    // deliberately a FOLLOWER, not the leader (scenario (a)'s own
    // reasoning) — no expanded config, no operator admin call, exactly the
    // real ADR 0030/0032 discovery+claim dial `data_join.rs` proves.
    let seed_node = cluster.control_follower_index();
    let before_time = cluster.sim_now();
    let (joined, joined_id) =
        join_and_get_id_with_role(&mut cluster, seed_node as usize, NodeRole::Data);
    let after_time = cluster.sim_now();

    assert_eq!(
        joined, 5,
        "seed={seed}: the joiner should get the next sequential index"
    );

    // 4. Self-minted, not pre-chosen — identical shape checks to (a).
    assert_ne!(
        joined_id,
        nid(joined),
        "seed={seed}: joined id must be self-minted, not `nid({joined})` \
         (joined_id={joined_id})"
    );
    assert_eq!(
        joined_id.as_str().len(),
        22,
        "seed={seed}: a minted NodeId is a 22-char base64url string \
         (joined_id={joined_id})"
    );

    // Promoted through the REAL control-leader detector — `join_via_seed_
    // with_role`'s data arm proposes no `UpsertMember` at all either (the
    // same observable-consequence proxy as scenario (a); see this module's
    // own doc on why the timing proxy is sound).
    let elapsed = after_time.duration_since(before_time);
    assert!(
        elapsed >= Duration::from_millis(80),
        "seed={seed}: promotion resolved in {elapsed:?} of virtual time — \
         too fast to be genuine heartbeat/detect_loop-driven promotion \
         (looks like a bypass propose)"
    );

    for n in 0..cluster.node_count() as u64 {
        let status = cluster
            .metadata(n)
            .members
            .get(&joined_id)
            .map(|m| m.status);
        assert_eq!(
            status,
            Some(NodeStatus::Active),
            "seed={seed}: node {n}'s own view of the joiner must show Active"
        );
    }

    // 5. The real placement reconciler eventually lands a real tablet
    // replica on the joiner — see this module's own doc for why every one
    // of the three tables is already under-replicated relative to its
    // recorded target the moment this node joins, not merely a balance
    // move.
    poll_until(
        &mut cluster,
        Duration::from_secs(20),
        seed,
        "joined data node gaining a tablet replica",
        |c| !c.hosted_tablets(joined).is_empty(),
    );
    let hosted_replica: BTreeSet<TabletId> = cluster.hosted_tablets(joined);
    let hosted_table: &str = DATA_JOIN_TABLES
        .iter()
        .copied()
        .find(|&table| hosted_replica.contains(&tablet_of_table(&cluster, table)))
        .unwrap_or_else(|| {
            panic!(
                "seed={seed}: joined node hosts {hosted_replica:?}, none of which is one of \
                 this scenario's own tables ({DATA_JOIN_TABLES:?})"
            )
        });

    // 6. Reads and writes round-trip BOTH directions through the joined
    // node's own `ClientCtx` for `hosted_table` — the one it's confirmed to
    // actually replicate — against a pre-existing data node, proving it a
    // genuine CP-data participant, not just a registered-but-inert member.
    cluster
        .put(
            joined,
            hosted_table,
            "pk-from-joiner",
            "sk",
            b"hello-from-joiner",
        )
        .unwrap_or_else(|e| panic!("seed={seed}: put from the joined node ({joined}) failed: {e}"));
    let got = cluster
        .get(3, hosted_table, "pk-from-joiner", "sk", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get from the pre-existing data node failed: {e}"));
    assert_eq!(
        got.as_deref(),
        Some(b"hello-from-joiner".as_slice()),
        "seed={seed}: a put issued from the joined node must be visible from a \
         pre-existing data node"
    );

    cluster
        .put(
            3,
            hosted_table,
            "pk-from-existing",
            "sk",
            b"hello-from-existing",
        )
        .unwrap_or_else(|e| panic!("seed={seed}: put from the pre-existing data node failed: {e}"));
    let got2 = cluster
        .get(joined, hosted_table, "pk-from-existing", "sk", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get from the joined node ({joined}) failed: {e}"));
    assert_eq!(
        got2.as_deref(),
        Some(b"hello-from-existing".as_slice()),
        "seed={seed}: a put issued from a pre-existing data node must be visible from the \
         joined node's own ClientCtx"
    );
}

#[test]
fn data_only_joiner_over_a_split_deployment_gets_a_rebalanced_replica() {
    run_data_only_joiner_over_a_split_deployment_gets_a_rebalanced_replica(env_seed(
        PRIMARY_SEED_C,
    ));
}

#[test]
fn data_only_joiner_over_a_split_deployment_gets_a_rebalanced_replica_over_seeds() {
    for &seed in &OVER_SEEDS_C {
        run_data_only_joiner_over_a_split_deployment_gets_a_rebalanced_replica(seed);
    }
}
