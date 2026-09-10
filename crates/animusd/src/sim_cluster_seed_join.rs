//! `SimCluster`-driven deterministic coverage for the real seed/join dial
//! (ADR 0061 rung M, C-13 PR 2) — `SimCluster::join_via_seed`, its
//! `_via_relay` siblings, and `forwarding::handle_relayed_request`'s new
//! `ClientRequest::JoinInfo` arm. See `sim_cluster.rs`'s own doc on
//! `SimCluster::join_via_seed` for the full mechanism (what it drives for
//! real, the two documented forks it resolves, what it deliberately does
//! not prove) and `crates/animusd/CLAUDE.md`'s matching appendix for the
//! cross-cutting account. This module is the smallest end-to-end slice
//! only — self-minted identity, combined mode, a single joiner — proving
//! the dial primitive itself; converting the real `data_join.rs`/
//! `seed_join.rs`/`seed_join_allocated.rs` scenarios onto it is later PRs'
//! own scope (C-13 PR 3+).
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

use super::sim_cluster::SimCluster;

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
