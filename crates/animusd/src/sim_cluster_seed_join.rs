//! `SimCluster`-driven deterministic coverage for the real seed/join dial
//! (ADR 0061 rung M, C-13 PR 2/3/4/5) — `SimCluster::join_via_seed`/
//! `join_via_seed_with_role`/`join_via_seed_concurrently`/`join_via_seed_
//! forcing_mint_collision`/`join_via_seed_with_explicit_id`, their
//! `_via_relay` siblings, and `forwarding::handle_relayed_request`'s
//! `ClientRequest::JoinInfo` arm. See `sim_cluster.rs`'s own doc on
//! `SimCluster::join_via_seed_concurrently`/`join_via_seed_with_explicit_id`
//! for the full mechanism (what each drives for real, the documented forks
//! it resolves, what it deliberately does not prove) and `crates/animusd/
//! CLAUDE.md`'s matching appendix for the cross-cutting account. PR 2's own
//! scope was the smallest end-to-end slice — self-minted identity, combined
//! mode, a single joiner — proving the dial primitive itself; PR 3 added
//! the data-only role arm and the first real conversion, `tests/
//! data_join.rs`'s own scenario (see (c) below); PR 4 converted `tests/
//! seed_join.rs` (see (d) below) and `tests/seed_join_allocated.rs`'s own
//! tests 1/3/5 (see this module's own doc note after (d) for the exact
//! mapping — tests 3/5 turn out to already be a strict subset of (c)/(a)
//! respectively, so PR 4 added no new scenario for either; test 1 is what
//! (a)'s own extension below, the trailing balance-driven-replica/peers
//! assertions, covers); **PR 5 converts `seed_join_allocated.rs`'s own test
//! 2 (concurrent minting, see (e) below) and adds a deterministic mint-
//! collision proof (f) the real-socket test could only ever hit by luck**.
//! Test 4 (ephemeral-identity restart) stays permanently out of scope — see
//! that file's own updated doc.
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
//!     full `join_via_seed` call (this rung's own combined-mode self-mint
//!     dial has no "the SAME process rejoins" shape of its own to call
//!     twice). A second call with the SAME id but DIFFERENT `addrs` is
//!     asserted to be a genuine collision, proving the CAS actually
//!     discriminates rather than always answering "registered." **C-13
//!     PR 4** builds the explicit-`--id` claim branch this scenario's own
//!     doc used to flag as missing (`SimCluster::
//!     join_via_seed_with_explicit_id`) and reuses it, plus a genuine
//!     `crash`+`restart` (this fixture's own closest analogue to "the
//!     process goes away and comes back on the same dir"), for a fuller
//!     rejoin proof against an ACTUAL explicit id — see (d) below.
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
//! (d) `explicit_id_joiner_gets_a_balanced_replica_and_survives_a_restart_
//!     rejoin` (C-13 PR 4) — the sim sibling for the real `tests/
//!     seed_join.rs`'s own single, eight-step scenario:
//!     [`SimCluster::join_via_seed_with_explicit_id`] (built this PR), fed
//!     `id = nid(cluster.node_count())` deliberately — an index-derived id,
//!     mirroring `seed_join.rs`'s own `animusd::config::node_id(join_
//!     index)` exactly (never a self-mint), and the ONE choice that keeps
//!     [`SimCluster::restart`]/[`crash`](SimCluster::crash) usable on the
//!     joined node afterward (see that method's own doc for why). Asserts,
//!     mapping onto `seed_join.rs`'s own eight numbered steps: the joiner
//!     lands at the id it was explicitly given (never minted); promoted
//!     through the real detector (the same timing proxy as (a)/(c)); a
//!     real, BALANCE-driven tablet replica (three tables, the identical
//!     reasoning as (a)'s own extension above) lands on it; bidirectional
//!     put/get through the joined node's own `ClientCtx` and a pre-existing
//!     node; every pre-existing node's own peer/route book names the
//!     joiner and vice versa (`SimCluster::client_route_ids`, the sim-
//!     native equivalent of `seed_join.rs`'s own `GET /admin/peers` check);
//!     a genuine collision (the SAME explicit id, DIFFERENT addrs, via
//!     [`SimCluster::rejoin_same_identity`]) is rejected and leaves the
//!     cluster's already-written data untouched; and a `crash`+`restart`
//!     of the joined node (this fixture's own closest analogue to
//!     `seed_join.rs`'s own literal "shut the process down, start a fresh
//!     one on the same dir/addrs" — see `SimCluster::restart`'s own doc:
//!     it reuses the SAME `MemoryTabletEngines` handle, so prior writes
//!     stay durable across it) is followed by re-registering the identical
//!     `(id, addrs)` once more, asserting the ADR 0032 rejoin CAS still
//!     accepts it as a no-op even post-restart.
//! (e) `two_concurrent_self_minted_joiners_get_distinct_ids_and_both_go_
//!     active` (C-13 PR 5) — the sim sibling for `tests/seed_join_
//!     allocated.rs`'s own `two_concurrent_allocated_joins_get_distinct_
//!     ids`: [`SimCluster::join_via_seed_concurrently`] with `count = 2`
//!     against the SAME seed, in ONE shared drive (see that method's own
//!     doc for why splitting spawn from drive is what makes this a genuine
//!     race, not two sequential dials). Asserts: exactly two new member
//!     ids appear (the before/after `Metadata::members` diff), both
//!     self-minted (22-char base64url, neither index-derived) and mutually
//!     DISTINCT — the direct proof `register_node_over_wire_via_relay`'s
//!     CAS discriminates two concurrent claims against the same control
//!     leader, closing the identical residual race ADR 0032 documents for
//!     the real allocated-join path; and both promoted to `Active` on
//!     every node's own view through the REAL detector (the same non-
//!     instantaneous timing proxy as (a)/(c)/(d) — `join_via_seed_
//!     concurrently` proposes no `UpsertMember` for either joiner).
//!     **Deliberately no tables and no forwarding proof** — matching the
//!     real-socket test's own scope exactly (it asserts only distinct/
//!     minted ids and promotion too); creating balance pressure here the
//!     way (a)'s own extension does for a SINGLE joiner would let the
//!     reconciler start moving tablets across BOTH new nodes' own
//!     promotion windows (a real, if incidental, discovery of this rung —
//!     see `docs/engineering-lessons.md`), racing a mechanism this
//!     scenario's own real subject has nothing to do with.
//! (f) `forced_mint_collision_retries_and_the_colliding_member_is_untouched`
//!     (C-13 PR 5) — a proof the real-socket test could only ever hit by
//!     astronomically unlucky chance: [`SimCluster::join_via_seed_forcing_
//!     mint_collision`] deliberately forces attempt 0 of the self-mint
//!     retry loop to collide with an EXISTING member's own id (`nid(0)`,
//!     joined under a DIFFERENT wire role so the forced candidate's addrs
//!     genuinely differ — see that method's own doc on why role must
//!     differ for this to be a real collision, not an idempotent no-op).
//!     Asserts: the join still succeeds (the loop actually retries into a
//!     real mint on attempt 1 and claims that instead); the node the join
//!     lands on carries a DIFFERENT id than `colliding_with` (never the
//!     forced, rejected candidate); the colliding member's own row
//!     (`Metadata::members[nid(0)]`) is completely untouched — same status,
//!     same addrs, as before the collision attempt; and the joiner is
//!     promoted to `Active` through the real detector like every other
//!     scenario here.
//!
//! **What (d) does NOT need to build, and why**: `tests/seed_join_
//! allocated.rs`'s own test 5 (`follower_connected_seed_completes_the_
//! allocate_node_id_round_trip`) turns out to be a strict SUBSET of (a)
//! — a self-minted combined join via a deliberately follower-only seed,
//! asserting only the minted-id shape and real-detector promotion, both
//! already asserted by (a) (which additionally proves the forwarding/
//! balance/peers properties test 5 never checks) — so PR 4 adds no new
//! scenario for it. Test 3 (`data_only_allocated_join_becomes_active_
//! and_gets_a_replica`) is likewise a strict subset of (c) — an identical
//! 3-control/2-data split deployment, self-minted data-only join, real
//! replica landing, bidirectional put/get — so PR 4 adds none for it
//! either. See `tests/seed_join_allocated.rs`'s own updated file doc for
//! the final per-test disposition table.
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
    // genuinely forward, never resolve locally. Two MORE tables are added
    // below (`t2`/`t3`, C-13 PR 4) purely so a later step can prove a
    // BALANCE-driven replica placement too (`tests/seed_join_allocated.rs`'s
    // own `no_node_join_becomes_active_and_gets_a_replica` shape, three
    // tables at RF 3 across exactly 3 pre-existing nodes) — a mechanism
    // distinct from scenario (c)'s own VIOLATION-driven placement (its
    // under-replicated-from-creation split deployment): with only `t1` at
    // RF 3 across 3 nodes, one tablet over 4 nodes already sits at
    // `max - min == 1`, `rebalance_step`'s own convergence threshold
    // (`crates/animus-placement/CLAUDE.md`), so nothing would ever move —
    // three tablets over 4 nodes (9 replica-slots, ideal ~2.25/node) is
    // what actually creates room for a move. `t1` stays the ONE table this
    // function's own forwarding proof below targets — checked immediately
    // after promotion, before the later balance-driven poll runs enough
    // additional virtual time for `t1`'s own tablet to possibly (though
    // not necessarily) end up moved too; that later movement, if any,
    // does not retroactively invalidate the point-in-time fact already
    // asserted below.
    cluster.create_table("t1");
    cluster.create_table("t2");
    cluster.create_table("t3");

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

    // C-13 PR 4: the two assertions `tests/seed_join_allocated.rs`'s own
    // `no_node_join_becomes_active_and_gets_a_replica` makes that this
    // scenario (pre-PR-4) did not — a real, BALANCE-driven tablet replica
    // eventually lands on the joiner (see this function's own doc on why
    // three tables, not one, are needed for that to happen at all), and
    // every pre-existing node's own peer/route book — the sim-native
    // equivalent of that real test's `GET /admin/peers` check (see
    // `SimCluster::client_route_ids`'s own doc for why route tables are the
    // right sim-native substitute) — now names the joiner, in BOTH
    // directions (the joiner's own table came from discovery, the
    // pre-existing nodes' via `join_via_seed`'s direct-patch shape).
    poll_until(
        &mut cluster,
        Duration::from_secs(20),
        seed,
        "joined combined node gaining a real (balance-driven) tablet replica",
        |c| !c.hosted_tablets(joined).is_empty(),
    );
    for n in 0..cluster.node_count() as u64 {
        assert!(
            cluster.client_route_ids(n).contains(&joined_id),
            "seed={seed}: node {n}'s own client_route must name the joiner \
             ({joined_id}) — the sim-native equivalent of a real node's own \
             GET /admin/peers listing it"
        );
    }
    let joiner_routes = cluster.client_route_ids(joined);
    for n in 0..3u64 {
        assert!(
            joiner_routes.contains(&nid(n)),
            "seed={seed}: the joiner's own client_route (derived straight from its \
             JoinInfo discovery reply) must name every pre-existing node — missing \
             node {n} (joiner routes={joiner_routes:?})"
        );
    }
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

const PRIMARY_SEED_D: u64 = 0xC13E_0004;
const OVER_SEEDS_D: [u64; 5] = [
    0xC13E_4001,
    0xC13E_4002,
    0xC13E_4003,
    0xC13E_4004,
    0xC13E_4005,
];

/// The three independent tables (d) seeds — mirrors `tests/seed_join.rs`'s
/// own fixed `TABLES` constant (three, kept for the identical balance-
/// driven-move reason this module's own doc on scenario (a)'s extension
/// spells out).
const SEED_JOIN_TABLES: [&str; 3] = ["seedjoin0", "seedjoin1", "seedjoin2"];

/// (d) `explicit_id_joiner_gets_a_balanced_replica_and_survives_a_restart_
/// rejoin` — see this module's own doc for the full eight-step mapping onto
/// `tests/seed_join.rs`'s own real-socket scenario.
fn run_explicit_id_joiner_gets_a_balanced_replica_and_survives_a_restart_rejoin(seed: u64) {
    // 1. A plain 3-node combined cluster; three independent tables (hand-
    // hosted — this cluster is uniformly combined, so `create_table`'s own
    // `0..replication` shortcut lands every replica on a real node, unlike
    // (c)'s own mixed-role cluster), each seeded with one write.
    let mut cluster = SimCluster::new(seed, 3, 3);
    for table in SEED_JOIN_TABLES {
        cluster.create_table(table);
        cluster
            .put(0, table, "k0", "sk", b"v0")
            .unwrap_or_else(|e| panic!("seed={seed}: seed put into {table} failed: {e}"));
    }

    // 2. Join a 4th node via a control FOLLOWER seed (scenario (a)'s own
    // reasoning), with an EXPLICIT, index-derived id — `nid(3)`, mirroring
    // `seed_join.rs`'s own `animusd::config::node_id(join_index)` — never a
    // self-mint (see `join_via_seed_with_explicit_id`'s own doc on why this
    // specific choice is what keeps `restart`/`crash` usable on this node
    // later in this same scenario).
    let seed_node = cluster.control_follower_index();
    let explicit_id = nid(cluster.node_count() as u64);
    let before_time = cluster.sim_now();
    let joined = cluster
        .join_via_seed_with_explicit_id(seed_node as usize, NodeRole::Both, explicit_id.clone())
        .unwrap_or_else(|e| panic!("seed={seed}: explicit-id join failed: {e}"));
    let after_time = cluster.sim_now();

    assert_eq!(
        joined, 3,
        "seed={seed}: the joiner should get the next sequential index"
    );

    // 3. Promoted through the REAL control-leader detector — the identical
    // timing proxy as (a)/(c) (`join_via_seed_with_explicit_id` proposes no
    // `UpsertMember` either, by construction — it shares (a)'s own
    // `finish_join` tail verbatim).
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
            .get(&explicit_id)
            .map(|m| m.status);
        assert_eq!(
            status,
            Some(NodeStatus::Active),
            "seed={seed}: node {n}'s own view of the joiner must show Active"
        );
    }

    // 4. A real, BALANCE-driven tablet replica lands on the joiner — the
    // identical reasoning as scenario (a)'s own extension above (three
    // tables at RF 3 across exactly 3 pre-existing nodes).
    poll_until(
        &mut cluster,
        Duration::from_secs(20),
        seed,
        "explicit-id joiner gaining a balance-driven tablet replica",
        |c| !c.hosted_tablets(joined).is_empty(),
    );
    let hosted_replica: BTreeSet<TabletId> = cluster.hosted_tablets(joined);
    let hosted_table: &str = SEED_JOIN_TABLES
        .iter()
        .copied()
        .find(|&table| hosted_replica.contains(&tablet_of_table(&cluster, table)))
        .unwrap_or_else(|| {
            panic!(
                "seed={seed}: joined node hosts {hosted_replica:?}, none of which is one of \
                 this scenario's own tables ({SEED_JOIN_TABLES:?})"
            )
        });

    // 5. Reads and writes round-trip both directions through the joined
    // node's own `ClientCtx` and a pre-existing node.
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
        .get(0, hosted_table, "pk-from-joiner", "sk", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get from node 0 failed: {e}"));
    assert_eq!(
        got.as_deref(),
        Some(b"hello-from-joiner".as_slice()),
        "seed={seed}: a put issued from the joined node must be visible from a pre-existing node"
    );

    cluster
        .put(
            0,
            hosted_table,
            "pk-from-existing",
            "sk",
            b"hello-from-existing",
        )
        .unwrap_or_else(|e| panic!("seed={seed}: put from node 0 failed: {e}"));
    let got2 = cluster
        .get(joined, hosted_table, "pk-from-existing", "sk", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get from the joined node ({joined}) failed: {e}"));
    assert_eq!(
        got2.as_deref(),
        Some(b"hello-from-existing".as_slice()),
        "seed={seed}: a put issued from a pre-existing node must be visible from the joined \
         node's own ClientCtx"
    );

    // 6. Every pre-existing node's own peer/route book names the joiner,
    // and vice versa — the sim-native equivalent of `seed_join.rs`'s own
    // `GET /admin/peers` check (see `SimCluster::client_route_ids`'s own
    // doc for why route tables are the right sim-native substitute).
    for n in 0..3u64 {
        assert!(
            cluster.client_route_ids(n).contains(&explicit_id),
            "seed={seed}: node {n}'s own client_route must name the joiner ({explicit_id})"
        );
    }
    let joiner_routes = cluster.client_route_ids(joined);
    for n in 0..3u64 {
        assert!(
            joiner_routes.contains(&nid(n)),
            "seed={seed}: the joiner's own client_route must name every pre-existing node — \
             missing node {n} (joiner routes={joiner_routes:?})"
        );
    }

    // 7. Collision: the SAME explicit id with DIFFERENT addrs is rejected
    // (never silently accepted), and the cluster's already-written data
    // stays untouched.
    let mut different_addrs = joined_addrs(&explicit_id);
    different_addrs.client = format!("{}-different", different_addrs.client);
    let collision_seed_node = cluster.control_follower_index();
    let collided = cluster.rejoin_same_identity(
        collision_seed_node as usize,
        explicit_id.clone(),
        different_addrs,
    );
    assert!(
        !collided,
        "seed={seed}: re-registering the SAME explicit id with DIFFERENT addrs must be a \
         genuine collision, never accepted"
    );
    let still_there = cluster
        .get(0, hosted_table, "pk-from-joiner", "sk", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get after the rejected collision failed: {e}"));
    assert_eq!(
        still_there.as_deref(),
        Some(b"hello-from-joiner".as_slice()),
        "seed={seed}: a rejected collision attempt must leave the cluster's own \
         already-written data untouched"
    );

    // 8. Rejoin: `crash`+`restart` the joined node — this fixture's own
    // closest analogue to `seed_join.rs`'s own literal "shut the process
    // down, start a fresh one on the same dir/addrs" (`SimCluster::
    // restart`'s own doc: it reuses the SAME `MemoryTabletEngines` handle,
    // so prior writes stay durable across it — never wiped the way a naive
    // rebuild might). Prior data survives, and re-registering the identical
    // `(id, addrs)` once more is still accepted as a CAS no-op, never a
    // collision, even post-restart.
    cluster.crash(joined);
    cluster.restart(joined);
    let after_restart = cluster
        .get(0, hosted_table, "pk-from-joiner", "sk", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get after crash+restart failed: {e}"));
    assert_eq!(
        after_restart.as_deref(),
        Some(b"hello-from-joiner".as_slice()),
        "seed={seed}: data written through the joined node must be durable across its own \
         crash+restart"
    );

    let rejoin_seed_node = cluster.control_follower_index();
    let rejoin_addrs = joined_addrs(&explicit_id);
    let rejoined =
        cluster.rejoin_same_identity(rejoin_seed_node as usize, explicit_id, rejoin_addrs);
    assert!(
        rejoined,
        "seed={seed}: re-registering the SAME (id, addrs) after a crash+restart must still be \
         accepted as an idempotent CAS no-op, not a collision"
    );
}

#[test]
fn explicit_id_joiner_gets_a_balanced_replica_and_survives_a_restart_rejoin() {
    run_explicit_id_joiner_gets_a_balanced_replica_and_survives_a_restart_rejoin(env_seed(
        PRIMARY_SEED_D,
    ));
}

#[test]
fn explicit_id_joiner_gets_a_balanced_replica_and_survives_a_restart_rejoin_over_seeds() {
    for &seed in &OVER_SEEDS_D {
        run_explicit_id_joiner_gets_a_balanced_replica_and_survives_a_restart_rejoin(seed);
    }
}

const PRIMARY_SEED_E: u64 = 0xC13E_0005;
const OVER_SEEDS_E: [u64; 5] = [
    0xC13E_5001,
    0xC13E_5002,
    0xC13E_5003,
    0xC13E_5004,
    0xC13E_5005,
];

/// (e) `two_concurrent_self_minted_joiners_get_distinct_ids_and_both_go_
/// active` — see this module's own doc for the full mapping onto `tests/
/// seed_join_allocated.rs`'s own `two_concurrent_allocated_joins_get_
/// distinct_ids`.
fn run_two_concurrent_self_minted_joiners_get_distinct_ids_and_both_go_active(seed: u64) {
    // Deliberately no tables at all — mirroring `tests/seed_join_
    // allocated.rs`'s own `two_concurrent_allocated_joins_get_distinct_
    // ids`, which asserts only distinct/minted ids and real-detector
    // promotion, never a forwarding round trip (unlike scenarios (a)/(c)/
    // (d) above). This is deliberate, not an omission: creating balance
    // pressure here (as (a)'s own extension does for a SINGLE joiner)
    // would let the placement reconciler start moving tablets across BOTH
    // new nodes' own promotion windows (strictly more elapsed virtual time
    // than any single-joiner scenario gives it), and a subsequent
    // forwarding proof would then be racing an active reconfigure this
    // scenario's own real subject (the concurrent-mint race) has nothing
    // to do with — see `docs/engineering-lessons.md`'s matching entry.
    let mut cluster = SimCluster::new(seed, 3, 3);

    let seed_node = cluster.control_follower_index();
    let before = member_ids(&cluster);
    let before_time = cluster.sim_now();
    // The genuine race: both dials are spawned before either resolves, then
    // driven by ONE shared `run_for` — see `join_via_seed_concurrently`'s
    // own doc for why this is what makes them actually race through
    // `register_node_over_wire_via_relay`'s CAS against the same leader,
    // never two sequential single-dial calls in a row.
    let joined = cluster.join_via_seed_concurrently(seed_node as usize, NodeRole::Both, 2);
    let after_time = cluster.sim_now();
    let after = member_ids(&cluster);

    assert_eq!(
        joined.len(),
        2,
        "seed={seed}: join_via_seed_concurrently(count=2) must return exactly two indices"
    );
    assert_eq!(
        joined,
        vec![3, 4],
        "seed={seed}: the two concurrent joiners should get the next two sequential indices, \
         in spawn order (joined={joined:?})"
    );

    // Exactly two new member ids, both self-minted (never index-derived)
    // and mutually DISTINCT — the direct proof the registration CAS
    // discriminates two concurrent claims against the same control leader,
    // closing the identical residual race ADR 0032 documents for the real
    // allocated-join path (this scenario's own real-socket counterpart,
    // `two_concurrent_allocated_joins_get_distinct_ids`, could only ever
    // prove this by running the race for real — this proves it
    // deterministically, from a pinned seed).
    let new_ids: Vec<NodeId> = after.difference(&before).cloned().collect();
    assert_eq!(
        new_ids.len(),
        2,
        "seed={seed}: expected exactly two new member ids (before={before:?}, after={after:?})"
    );
    assert_ne!(
        new_ids[0], new_ids[1],
        "seed={seed}: two concurrent join attempts must never be allocated the same id \
         (new_ids={new_ids:?})"
    );
    for id in &new_ids {
        assert_eq!(
            id.as_str().len(),
            22,
            "seed={seed}: a minted NodeId is a 22-char base64url string (id={id})"
        );
        assert!(
            joined.iter().all(|&idx| *id != nid(idx)),
            "seed={seed}: joined id must be self-minted, not index-derived (id={id}, \
             joined={joined:?})"
        );
    }

    // Promoted through the REAL control-leader detector for BOTH joiners —
    // the identical non-instantaneous timing proxy as every other scenario
    // in this module (`join_via_seed_concurrently` proposes no
    // `UpsertMember` for either one).
    let elapsed = after_time.duration_since(before_time);
    assert!(
        elapsed >= Duration::from_millis(80),
        "seed={seed}: promotion resolved in {elapsed:?} of virtual time — too fast to be \
         genuine heartbeat/detect_loop-driven promotion (looks like a bypass propose)"
    );
    for id in &new_ids {
        for n in 0..cluster.node_count() as u64 {
            let status = cluster.metadata(n).members.get(id).map(|m| m.status);
            assert_eq!(
                status,
                Some(NodeStatus::Active),
                "seed={seed}: node {n}'s own view of joiner {id} must show Active"
            );
        }
    }
}

#[test]
fn two_concurrent_self_minted_joiners_get_distinct_ids_and_both_go_active() {
    run_two_concurrent_self_minted_joiners_get_distinct_ids_and_both_go_active(env_seed(
        PRIMARY_SEED_E,
    ));
}

#[test]
fn two_concurrent_self_minted_joiners_get_distinct_ids_and_both_go_active_over_seeds() {
    for &seed in &OVER_SEEDS_E {
        run_two_concurrent_self_minted_joiners_get_distinct_ids_and_both_go_active(seed);
    }
}

const PRIMARY_SEED_F: u64 = 0xC13E_0006;
const OVER_SEEDS_F: [u64; 5] = [
    0xC13E_6001,
    0xC13E_6002,
    0xC13E_6003,
    0xC13E_6004,
    0xC13E_6005,
];

/// (f) `forced_mint_collision_retries_and_the_colliding_member_is_
/// untouched` — a deterministic proof of the self-mint retry-on-collision
/// loop the real-socket `two_concurrent_allocated_joins_get_distinct_ids`
/// could only ever exercise by astronomically unlucky chance. See this
/// module's own doc for the full account and
/// `SimCluster::join_via_seed_forcing_mint_collision`'s own doc for why the
/// joiner uses [`NodeRole::Data`] specifically (a different wire role than
/// the colliding node's own `"combined"` registration, so the forced
/// candidate's addrs genuinely differ — a same-role forced candidate would
/// build byte-identical addrs, which the CAS accepts as an idempotent
/// no-op re-registration rather than a genuine collision).
fn run_forced_mint_collision_retries_and_the_colliding_member_is_untouched(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let colliding_with = nid(0);
    let before = member_ids(&cluster);
    let before_addrs = cluster
        .metadata(0)
        .node_addrs
        .get(&colliding_with)
        .cloned()
        .unwrap_or_else(|| {
            panic!("seed={seed}: node 0 ({colliding_with}) must already be registered")
        });
    let before_status = cluster
        .metadata(0)
        .members
        .get(&colliding_with)
        .map(|m| m.status);

    let seed_node = cluster.control_follower_index();
    let joined = cluster.join_via_seed_forcing_mint_collision(
        seed_node as usize,
        NodeRole::Data,
        colliding_with.clone(),
    );
    let after = member_ids(&cluster);

    assert_eq!(
        joined, 3,
        "seed={seed}: the joiner should still get the next sequential index despite the \
         forced collision on attempt 0"
    );

    let joined_id = only_new_member(&before, &after);
    assert_ne!(
        joined_id, colliding_with,
        "seed={seed}: the joiner must land on a DIFFERENT id than the forced, rejected \
         candidate — the retry loop must have minted a fresh one on a later attempt \
         (joined_id={joined_id})"
    );
    assert_eq!(
        joined_id.as_str().len(),
        22,
        "seed={seed}: the retried mint is still a real 22-char base64url self-mint \
         (joined_id={joined_id})"
    );

    // The colliding member's own row is completely untouched by the
    // rejected attempt — same addrs, same status as before.
    let after_addrs = cluster.metadata(0).node_addrs.get(&colliding_with).cloned();
    assert_eq!(
        after_addrs,
        Some(before_addrs),
        "seed={seed}: the colliding member's own NodeAddrs must be untouched by a rejected \
         collision attempt"
    );
    let after_status = cluster
        .metadata(0)
        .members
        .get(&colliding_with)
        .map(|m| m.status);
    assert_eq!(
        after_status, before_status,
        "seed={seed}: the colliding member's own status must be untouched by a rejected \
         collision attempt"
    );

    // The joiner itself is promoted through the real detector, like every
    // other scenario in this module.
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
}

#[test]
fn forced_mint_collision_retries_and_the_colliding_member_is_untouched() {
    run_forced_mint_collision_retries_and_the_colliding_member_is_untouched(env_seed(
        PRIMARY_SEED_F,
    ));
}

#[test]
fn forced_mint_collision_retries_and_the_colliding_member_is_untouched_over_seeds() {
    for &seed in &OVER_SEEDS_F {
        run_forced_mint_collision_retries_and_the_colliding_member_is_untouched(seed);
    }
}
