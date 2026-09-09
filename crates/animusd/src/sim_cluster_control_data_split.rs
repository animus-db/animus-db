//! `SimCluster`-driven conversion of `tests/control_only.rs` (3 tests),
//! `tests/data_only.rs` (5 tests), and `tests/cluster_split.rs` (3 tests) —
//! ADR 0061 rung L, C-12 PR 4a. `sim_cluster_control_only.rs` (C-12 PR 2)
//! and `sim_cluster_data_only.rs` (C-12 PR 3) built the mechanism (per-node
//! `NodeRole` under `SimCluster`, role-aware `restart`/`crash`, a
//! `NodeRole::Data` node first-class at construction); this module is pure
//! test authorship on top of it — **no `sim_cluster.rs` production-shaped
//! change was needed beyond one small accessor** ([`SimCluster::
//! control_voters`], added alongside this module — a thin, sync wrapper
//! over `ClientCtx::control.config()`, needed only because no existing
//! `SimCluster` primitive surfaced a node's own live-voter belief; every
//! other primitive this module uses — `new_with_roles`, `role_of`,
//! `control_leader_index`, `crash`, `restart`, `put`/`get`, `dynamo`,
//! `admin` — already existed).
//!
//! ## Classification table (D3 discipline, per original test)
//!
//! | Original test | A/B | Sim sibling |
//! |---|---|---|
//! | `control_only.rs::control_only_cluster_elects_leader_and_serves_status` | mixed | [`run_bare_control_only_cluster_elects_and_serves_status`] covers the (A) half (leader election, quiescence with zero data members, `/admin/status`/`/admin/health`/`/admin/config`); the original **stays whole** in the trimmed file for its (B) half — `/admin/storage/control`/`/admin/system-table` both key on `ctx.control_storage`, which is `None` on *every* `SimCluster` node regardless of role (a pre-existing, already-documented capability gap — see `crates/animusd/CLAUDE.md`'s Rung H PR 5 appendix on `tests/system_table.rs`'s own disposition — not something this rung's role split could close) |
//! | `control_only.rs::schema_ddl_via_control_node_commits_and_relays` | A | [`run_schema_ddl_via_control_node_commits_and_relays`] — full convert |
//! | `control_only.rs::mixed_cluster_put_via_control_node_forwards_to_data_node` | A | [`run_mixed_cluster_put_via_control_node_forwards_to_data_node`] — full convert, substituting `NodeRole::Data` (ADR 0035 PR4's real `ControlHandle::Remote`) for the original's ADR 0030 growth-node mirror, which was only ever "the closest existing mechanism... until ADR 0035 PR4" (the original test's own doc, quoted) — `NodeRole::Data` is the thing PR4 actually shipped |
//! | `data_only.rs::split_cluster_serves_reads_and_writes_across_data_nodes` | mixed | [`run_split_cluster_serves_reads_and_writes_across_data_nodes`] covers the (A) half (cross-data-node routing, the fixed-control-node forward path, `/admin/health`/`/admin/config` on the data nodes); the original **stays whole** for its (B) half — the identical `ctx.control_storage`-always-`None` gap, both directions (`/admin/storage/control`+`/admin/system-table` on both control and data nodes) |
//! | `data_only.rs::schema_ddl_via_a_data_node_relays_and_commits` | A | [`run_schema_ddl_via_a_data_node_relays_and_commits`] — full convert |
//! | `data_only.rs::data_node_falls_over_to_a_remaining_control_seed` | A | [`run_data_node_falls_over_to_a_remaining_control_seed`] — full convert (`SimCluster::crash` of a non-leader control node) |
//! | `data_only.rs::data_node_restart_rejoins_and_serves_reads_again` | A | [`run_data_node_restart_rejoins_and_serves_reads_again`] — full convert (`SimCluster::restart` of a data-only node) |
//! | `data_only.rs::data_node_observes_live_control_voters_after_a_fresh_fetch` | A | [`run_data_node_observes_live_control_voters_after_a_fresh_fetch`] — full convert, via the new `SimCluster::control_voters` accessor |
//! | `cluster_split.rs::in_process_split_cluster_serves_writes_and_reports_roles` | A | [`run_in_process_split_cluster_serves_writes_and_reports_roles`] — full convert (the stale `addrs.raftkv`/`addrs.control` null-checks, both fields removed by ADR 0040 PR1's `internal` merge years before this rung, are dropped rather than reproduced — the real assertion, `role` differing by node, is kept) |
//! | `cluster_split.rs::fixed_control_node_write_read_is_deterministic` | A | [`run_fixed_control_node_write_read_is_deterministic`] — full convert, with one deliberate adaptation: `SimCluster::put` is `cp_kind_write_raw`, which does **not** auto-provision (unlike the real client protocol's `ClientRequest::Put`, which commits through `dynamo::marker_batch_write_raw` and does) — so this sibling creates the table over the wire first, then runs the identical 20-iteration fixed-control-node round trip |
//! | `cluster_split.rs::single_shot_first_write_through_control_node_succeeds` | A (weakened) | [`run_single_shot_first_write_through_control_node_succeeds`] — converts the *shape* (one `put` call, no client-side retry loop, through a zero-replica control-only node, must succeed) but **not** the literal race window: every `SimCluster` table-creation primitive (`create_table_via_wire`'s own `await_table_serveable` wait, `create_table_with_replication`'s own leader-election poll) already waits out group formation before returning, so by the time this sibling's own single `put` runs, the tablet already has an elected leader — there is no `SimCluster` primitive that provisions a tablet *without* waiting for it to serve. What this sibling still proves: a single, unretried forwarded write through a genuinely zero-replica node succeeds when nothing is racing formation — the `FORWARD_ELECTION_BACKOFF` wait-out-a-live-election mechanism itself has its own direct unit coverage in `animus-node::decide::tests` (root `CLAUDE.md`'s "zero-replica blind-forward" entry) |
//!
//! Every scenario issues from the intended role's own node index and asks
//! for `ConsistentRead: true`/the CP linearizable path (`consistent: true`
//! to [`SimCluster::get`]) on any read that verifies a write, per ADR 0055's
//! standing testing discipline. `_over_seeds` siblings run 5 fixed seeds
//! each, mirroring `sim_cluster_control_only.rs`/`sim_cluster_data_
//! only.rs`'s own convention exactly. Seed replay (repo convention):
//! `ANIMUS_SEED=<seed> cargo test -p animusd --lib <test name>`.

use std::time::Duration;

use super::sim_cluster::SimCluster;
use super::sim_cluster_console::{create_table_via_wire, env_seed, json};
use crate::config::NodeRole;

/// A single hash-key (`pk`, string) `CreateTable`, issued from `node` —
/// mirrors every other `sim_cluster_*` module's identically-named helper
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
/// `sim_cluster_control_only.rs`/`sim_cluster_data_only.rs` carry the
/// identical copy).
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

/// One `GET` against `path`, parsed as JSON — the JSON-dispatch analog of
/// every real-socket test's own `admin_get` helper (`tests/support` has no
/// equivalent this fixture can reach into, since `SimCluster::admin` speaks
/// straight through `animus_node::admin::dispatch`, never real HTTP
/// framing).
fn admin_json(cluster: &mut SimCluster, node: u64, path: &str) -> (u16, serde_json::Value) {
    let (status, body) = cluster.admin(node, "GET", path, "", &[]);
    (status, json(&body))
}

// ---------------------------------------------------------------------------
// (1) control_only.rs::control_only_cluster_elects_leader_and_serves_status
//     — the (A) half only; the original stays whole for its (B) half.
// ---------------------------------------------------------------------------

fn run_bare_control_only_cluster_elects_and_serves_status(seed: u64) {
    let mut cluster = SimCluster::new_with_roles(seed, &[NodeRole::Control; 3], 1);

    let leader = cluster.control_leader_index() as u64;
    assert_eq!(
        cluster.role_of(leader),
        NodeRole::Control,
        "seed={seed}: the elected leader must itself be one of the 3 control-only voters"
    );

    for n in 0..3u64 {
        let (status, status_json) = admin_json(&mut cluster, n, "/admin/status");
        assert_eq!(status, 200, "seed={seed}: /admin/status on node {n}");
        assert!(
            status_json["members"]
                .as_object()
                .is_some_and(|m| m.is_empty()),
            "seed={seed}: status should carry the (empty) members map: {status_json}"
        );

        let (status, health) = admin_json(&mut cluster, n, "/admin/health");
        assert_eq!(status, 200, "seed={seed}: /admin/health on node {n}");
        assert_eq!(
            health["hosts_cp"], false,
            "seed={seed}: a control-only node never hosts a CP group: {health}"
        );

        let (status, config_view) = admin_json(&mut cluster, n, "/admin/config");
        assert_eq!(status, 200, "seed={seed}: /admin/config on node {n}");
        assert!(
            !config_view["node_id"].is_null(),
            "seed={seed}: every node has one id (ADR 0040 PR1): {config_view}"
        );
        assert!(
            !config_view["addrs"]["internal"].is_null(),
            "seed={seed}: every role binds the one internal address: {config_view}"
        );
        assert_eq!(
            config_view["role"], "control",
            "seed={seed}: a control-only node's own config reports its role: {config_view}"
        );
    }

    // Quiescence: zero data members registered anywhere, so the placement
    // reconciler and failure detector have nothing to do — a bounded
    // window of repeated status polls must stay quiet (an empty members
    // map every time), never panic, mirroring the real test's own repeated-
    // poll-instead-of-one-fixed-sleep idiom (a crash surfaces as this
    // scenario's own panic, not merely a missed assertion).
    for _ in 0..5 {
        for n in 0..3u64 {
            let (status, status_json) = admin_json(&mut cluster, n, "/admin/status");
            assert_eq!(status, 200, "seed={seed}: /admin/status on node {n}");
            assert!(
                status_json["members"]
                    .as_object()
                    .is_some_and(|m| m.is_empty()),
                "seed={seed}: members map must stay empty: {status_json}"
            );
        }
        cluster.run_for(Duration::from_millis(100));
    }
}

#[test]
fn bare_control_only_cluster_elects_and_serves_status() {
    run_bare_control_only_cluster_elects_and_serves_status(env_seed(0xC12A_0001));
}

#[test]
fn bare_control_only_cluster_elects_and_serves_status_over_seeds() {
    for i in 0..5 {
        run_bare_control_only_cluster_elects_and_serves_status(0xC12A_1000 + i);
    }
}

// ---------------------------------------------------------------------------
// (2) control_only.rs::schema_ddl_via_control_node_commits_and_relays
// ---------------------------------------------------------------------------

fn run_schema_ddl_via_control_node_commits_and_relays(seed: u64) {
    // A bare 3-control-only cluster has no data-capable member at all, so
    // a wire `CreateTable` (which auto-provisions AND waits for the fresh
    // tablet to elect/serve, `await_table_serveable`) can never succeed —
    // there is nothing eligible to become a replica. One data-only node is
    // added purely so the DDL commit/relay proof below has somewhere to
    // land; the scenario's own subject (leader-local propose vs.
    // follower-relay, both from CONTROL-only nodes) is unaffected.
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 1);
    let leader = cluster.control_leader_index() as u64;

    // Issued against the LEADER first — the direct-propose path.
    let (status, body) = create_table(&mut cluster, leader, "control_ddl_t");
    assert_eq!(
        status, 200,
        "seed={seed}: leader-local CreateTable failed: {body}"
    );
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        "every control node observing the leader-issued schema",
        |c| (0..3u64).all(|n| c.metadata(n).has_table_schema("control_ddl_t")),
    );

    // Now against a FOLLOWER — must relay to the leader (`propose_schema`'s
    // own relay path), not time out.
    let follower = (0..3u64).find(|&n| n != leader).unwrap();
    let (status2, body2) = create_table(&mut cluster, follower, "control_ddl_t2");
    assert_eq!(
        status2, 200,
        "seed={seed}: follower-relayed CreateTable failed: {body2}"
    );
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        "every control node observing the follower-relayed schema",
        |c| (0..3u64).all(|n| c.metadata(n).has_table_schema("control_ddl_t2")),
    );
}

#[test]
fn schema_ddl_via_control_node_commits_and_relays() {
    run_schema_ddl_via_control_node_commits_and_relays(env_seed(0xC12A_0002));
}

#[test]
fn schema_ddl_via_control_node_commits_and_relays_over_seeds() {
    for i in 0..5 {
        run_schema_ddl_via_control_node_commits_and_relays(0xC12A_2000 + i);
    }
}

// ---------------------------------------------------------------------------
// (3) control_only.rs::mixed_cluster_put_via_control_node_forwards_to_data_node
// ---------------------------------------------------------------------------

fn run_mixed_cluster_put_via_control_node_forwards_to_data_node(seed: u64) {
    // 3 control-only + 1 data-only (the real ADR 0035 PR4 `ControlHandle::
    // Remote` shape — this rung's own substitution for the original's ADR
    // 0030 growth-node mirror, which its own doc named only "the closest
    // existing mechanism... until ADR 0035 PR4").
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 1);
    let leader = cluster.control_leader_index() as u64;

    let (status, body) = create_table(&mut cluster, leader, "mixed_t");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    // A `Put` sent to a CONTROL node: `resolve_cp_route` — this control
    // node hosts no local CP group at all — forwards it to the sole data
    // node.
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        "node 3 hosting table `mixed_t`'s tablet",
        |c| !c.hosted_tablets(3).is_empty(),
    );
    cluster
        .put(0, "mixed_t", "mixed-key", "sk", b"mixed-val")
        .unwrap_or_else(|e| panic!("seed={seed}: put via the control node failed: {e}"));

    let got = cluster
        .get(0, "mixed_t", "mixed-key", "sk", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get via the control node failed: {e}"));
    assert_eq!(got.as_deref(), Some(&b"mixed-val"[..]), "seed={seed}");
    let got_direct = cluster
        .get(3, "mixed_t", "mixed-key", "sk", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get directly on the data node failed: {e}"));
    assert_eq!(
        got_direct.as_deref(),
        Some(&b"mixed-val"[..]),
        "seed={seed}"
    );

    // A schema command issued against the DATA node relays to the control
    // leader (the data node's own control role can never accept a local
    // propose — it is `ControlHandle::Remote`).
    let (status2, body2) = create_table(&mut cluster, 3, "mixed_ddl_t");
    assert_eq!(
        status2, 200,
        "seed={seed}: data-node-issued CreateTable failed: {body2}"
    );
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        "every control node observing the data-node-relayed schema",
        |c| (0..3u64).all(|n| c.metadata(n).has_table_schema("mixed_ddl_t")),
    );
}

#[test]
fn mixed_cluster_put_via_control_node_forwards_to_data_node() {
    run_mixed_cluster_put_via_control_node_forwards_to_data_node(env_seed(0xC12A_0003));
}

#[test]
fn mixed_cluster_put_via_control_node_forwards_to_data_node_over_seeds() {
    for i in 0..5 {
        run_mixed_cluster_put_via_control_node_forwards_to_data_node(0xC12A_3000 + i);
    }
}

// ---------------------------------------------------------------------------
// (4) data_only.rs::split_cluster_serves_reads_and_writes_across_data_nodes
//     — the (A) half only; the original stays whole for its (B) half.
// ---------------------------------------------------------------------------

fn run_split_cluster_serves_reads_and_writes_across_data_nodes(seed: u64) {
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 2);
    let leader = cluster.control_leader_index() as u64;

    let (status, body) = create_table(&mut cluster, leader, "split_t");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    for n in 3..5u64 {
        poll_until(
            &mut cluster,
            Duration::from_secs(10),
            seed,
            &format!("data node {n} hosting table `split_t`'s tablet"),
            |c| !c.hosted_tablets(n).is_empty(),
        );
    }

    // A `Put` via one data node, a `Get` via the OTHER — no control node
    // involved in the data path at all.
    cluster
        .put(3, "split_t", "split-key", "sk", b"split-val")
        .unwrap_or_else(|e| panic!("seed={seed}: put via data node 3 failed: {e}"));
    let got = cluster
        .get(4, "split_t", "split-key", "sk", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get via data node 4 failed: {e}"));
    assert_eq!(got.as_deref(), Some(&b"split-val"[..]), "seed={seed}");

    // A `Put`/`Get` via a single FIXED control-only node's own index —
    // hosts zero local CP replicas, so this takes `resolve_cp_route`'s
    // no-local-replica forward branch (the hinted-retry forwarder, root
    // `CLAUDE.md`'s "zero-replica blind-forward" entry).
    let fixed_control = 0u64;
    cluster
        .put(
            fixed_control,
            "split_t",
            "via-control-key",
            "sk",
            b"via-control-val",
        )
        .unwrap_or_else(|e| panic!("seed={seed}: put via the fixed control node failed: {e}"));
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        "the fixed control node reading back its own write",
        |c| {
            matches!(
                c.get(fixed_control, "split_t", "via-control-key", "sk", true),
                Ok(Some(ref v)) if v.as_slice() == b"via-control-val"
            )
        },
    );

    // The data-only nodes' own `/admin/health`: `is_control_leader` is
    // hardcoded false (`ControlHandle::Remote::is_leader()`), and `hosts_cp`
    // converges to true (a real reconciler standing the replica up, not an
    // immediate fact).
    for n in 3..5u64 {
        let (status, health) = admin_json(&mut cluster, n, "/admin/health");
        assert_eq!(status, 200, "seed={seed}: /admin/health on node {n}");
        assert_eq!(
            health["is_control_leader"], false,
            "seed={seed}: a data-only node never leads the control plane: {health}"
        );
    }
    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        "both data nodes converging to hosting the tablet's CP group",
        |c| {
            (3..5u64).all(|n| {
                let (_, health) = admin_json(c, n, "/admin/health");
                health["hosts_cp"] == true
            })
        },
    );

    // Every node has one id and one internal address regardless of role
    // (ADR 0040 PR1) — a data-only node included.
    for n in 3..5u64 {
        let (status, cfg) = admin_json(&mut cluster, n, "/admin/config");
        assert_eq!(status, 200, "seed={seed}: /admin/config on node {n}");
        assert!(
            !cfg["node_id"].is_null(),
            "seed={seed}: a data-only node still has its own id: {cfg}"
        );
        assert!(
            !cfg["addrs"]["internal"].is_null(),
            "seed={seed}: a data-only node still has its own internal address: {cfg}"
        );
    }
}

#[test]
fn split_cluster_serves_reads_and_writes_across_data_nodes() {
    run_split_cluster_serves_reads_and_writes_across_data_nodes(env_seed(0xC12A_0004));
}

#[test]
fn split_cluster_serves_reads_and_writes_across_data_nodes_over_seeds() {
    for i in 0..5 {
        run_split_cluster_serves_reads_and_writes_across_data_nodes(0xC12A_4000 + i);
    }
}

// ---------------------------------------------------------------------------
// (5) data_only.rs::schema_ddl_via_a_data_node_relays_and_commits
// ---------------------------------------------------------------------------

fn run_schema_ddl_via_a_data_node_relays_and_commits(seed: u64) {
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 2);

    // A data-only node can never satisfy `propose_schema`'s local-leader
    // branch (`ControlHandle::Remote`) — this proves the relay path reaches
    // the real control leader from a node with zero control-plane state of
    // its own.
    let (status, body) = create_table(&mut cluster, 3, "data_ddl_t");
    assert_eq!(
        status, 200,
        "seed={seed}: data-node-issued CreateTable failed: {body}"
    );

    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        "every node observing the data-node-relayed schema",
        |c| (0..5u64).all(|n| c.metadata(n).has_table_schema("data_ddl_t")),
    );
}

#[test]
fn schema_ddl_via_a_data_node_relays_and_commits() {
    run_schema_ddl_via_a_data_node_relays_and_commits(env_seed(0xC12A_0005));
}

#[test]
fn schema_ddl_via_a_data_node_relays_and_commits_over_seeds() {
    for i in 0..5 {
        run_schema_ddl_via_a_data_node_relays_and_commits(0xC12A_5000 + i);
    }
}

// ---------------------------------------------------------------------------
// (6) data_only.rs::data_node_falls_over_to_a_remaining_control_seed
// ---------------------------------------------------------------------------

fn run_data_node_falls_over_to_a_remaining_control_seed(seed: u64) {
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 2);
    let leader = cluster.control_leader_index() as u64;

    // `SimCluster::put` is `cp_kind_write_raw`, which does NOT
    // auto-provision a table's first tablet (unlike production's
    // plain-protocol Put) — create the table over the wire first, from
    // the control leader, before anything below writes to it.
    let (status, _) = create_table(&mut cluster, leader, "split_t2");
    assert_eq!(status, 200, "seed={seed}: create_table split_t2 failed");

    // Crash a control node that is NOT the current leader (crashing the
    // leader just forces an ordinary re-election among the remaining two —
    // a different, already-covered scenario) — the data nodes' own
    // mirror-sync loop must fall over to a remaining seed rather than
    // getting stuck retrying a dead one forever.
    let victim = (0..3u64).find(|&n| n != leader).unwrap();
    cluster.crash(victim);

    // A *new* write, issued only after the control node is down, still has
    // to reach the (still up) leader through the mirror's seed-scan
    // fallback.
    cluster
        .put(3, "split_t2", "post-failure-key", "sk", b"post-failure-val")
        .unwrap_or_else(|e| {
            panic!("seed={seed}: put via a data node after a control node crashed failed: {e}")
        });
    let got = cluster
        .get(4, "split_t2", "post-failure-key", "sk", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get via the other data node failed: {e}"));
    assert_eq!(
        got.as_deref(),
        Some(&b"post-failure-val"[..]),
        "seed={seed}"
    );
}

#[test]
fn data_node_falls_over_to_a_remaining_control_seed() {
    run_data_node_falls_over_to_a_remaining_control_seed(env_seed(0xC12A_0006));
}

#[test]
fn data_node_falls_over_to_a_remaining_control_seed_over_seeds() {
    for i in 0..5 {
        run_data_node_falls_over_to_a_remaining_control_seed(0xC12A_6000 + i);
    }
}

// ---------------------------------------------------------------------------
// (7) data_only.rs::data_node_restart_rejoins_and_serves_reads_again
// ---------------------------------------------------------------------------

fn run_data_node_restart_rejoins_and_serves_reads_again(seed: u64) {
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 2);
    let leader = cluster.control_leader_index() as u64;

    let (status, body) = create_table(&mut cluster, leader, "split_t3");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    cluster
        .put(3, "split_t3", "restart-key", "sk", b"restart-val")
        .unwrap_or_else(|e| panic!("seed={seed}: initial put failed: {e}"));

    // Restart data node 3 — a true process restart (`Simulator::stop` then
    // a fresh rebuild), no leadership gate (a data-only node is never a
    // "leader" of anything) — poll for catch-up instead.
    cluster.restart(3);

    poll_until(
        &mut cluster,
        Duration::from_secs(10),
        seed,
        "the restarted data node re-hosting and serving the pre-restart write",
        |c| {
            matches!(
                c.get(3, "split_t3", "restart-key", "sk", true),
                Ok(Some(ref v)) if v.as_slice() == b"restart-val"
            )
        },
    );
}

#[test]
fn data_node_restart_rejoins_and_serves_reads_again() {
    run_data_node_restart_rejoins_and_serves_reads_again(env_seed(0xC12A_0007));
}

#[test]
fn data_node_restart_rejoins_and_serves_reads_again_over_seeds() {
    for i in 0..5 {
        run_data_node_restart_rejoins_and_serves_reads_again(0xC12A_7000 + i);
    }
}

// ---------------------------------------------------------------------------
// (8) data_only.rs::data_node_observes_live_control_voters_after_a_fresh_fetch
// ---------------------------------------------------------------------------

fn run_data_node_observes_live_control_voters_after_a_fresh_fetch(seed: u64) {
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 2);
    // Force at least one control-plane round trip so the mirror has
    // something to converge from.
    let _ = cluster.control_leader_index();

    let expected: std::collections::BTreeSet<animus_env::NodeId> =
        (0..3u64).map(animus_env::nid).collect();

    for n in 3..5u64 {
        poll_until(
            &mut cluster,
            Duration::from_secs(10),
            seed,
            &format!("data node {n} observing the live control-voter set"),
            |c| c.control_voters(n).as_ref() == Some(&expected),
        );
    }
}

#[test]
fn data_node_observes_live_control_voters_after_a_fresh_fetch() {
    run_data_node_observes_live_control_voters_after_a_fresh_fetch(env_seed(0xC12A_0008));
}

#[test]
fn data_node_observes_live_control_voters_after_a_fresh_fetch_over_seeds() {
    for i in 0..5 {
        run_data_node_observes_live_control_voters_after_a_fresh_fetch(0xC12A_8000 + i);
    }
}

// ---------------------------------------------------------------------------
// (9) cluster_split.rs::in_process_split_cluster_serves_writes_and_reports_roles
// ---------------------------------------------------------------------------

fn run_in_process_split_cluster_serves_writes_and_reports_roles(seed: u64) {
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 2);
    let leader = cluster.control_leader_index() as u64;

    let (status, body) = create_table(&mut cluster, leader, "kv");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    cluster
        .put(3, "kv", "hello", "sk", b"world")
        .unwrap_or_else(|e| panic!("seed={seed}: put via a data node failed: {e}"));
    for n in 0..5u64 {
        poll_until(
            &mut cluster,
            Duration::from_secs(10),
            seed,
            &format!("node {n} observing the write cluster-wide"),
            |c| {
                matches!(
                    c.get(n, "kv", "hello", "sk", true),
                    Ok(Some(ref v)) if v.as_slice() == b"world"
                )
            },
        );
    }

    // A write via one data node, read via the *other* data node.
    cluster
        .put(3, "kv", "cross", "sk", b"replica")
        .unwrap_or_else(|e| panic!("seed={seed}: put via data node 3 failed: {e}"));
    let got = cluster
        .get(4, "kv", "cross", "sk", true)
        .unwrap_or_else(|e| panic!("seed={seed}: get via data node 4 failed: {e}"));
    assert_eq!(got.as_deref(), Some(&b"replica"[..]), "seed={seed}");

    // A write through a single FIXED control-only node's own index — the
    // hinted-retry forwarder's own no-local-replica branch.
    let fixed_control = 0u64;
    cluster
        .put(fixed_control, "kv", "via-control", "sk", b"ok")
        .unwrap_or_else(|e| panic!("seed={seed}: put via the fixed control node failed: {e}"));
    for n in 0..5u64 {
        poll_until(
            &mut cluster,
            Duration::from_secs(10),
            seed,
            &format!("node {n} observing the control-node-issued write cluster-wide"),
            |c| {
                matches!(
                    c.get(n, "kv", "via-control", "sk", true),
                    Ok(Some(ref v)) if v.as_slice() == b"ok"
                )
            },
        );
    }

    // `/admin/config`'s `role` differs across a control vs. data node — the
    // ADR 0040 PR1 field merge means the original's own `addrs.raftkv`/
    // `addrs.control` null-checks have nothing left to check (both fields
    // were removed years before this rung), so only the real assertion —
    // `role` itself — is reproduced here.
    let (status, control_cfg) = admin_json(&mut cluster, 0, "/admin/config");
    assert_eq!(status, 200, "seed={seed}");
    assert_eq!(control_cfg["role"], "control", "seed={seed}: {control_cfg}");

    let (status, data_cfg) = admin_json(&mut cluster, 3, "/admin/config");
    assert_eq!(status, 200, "seed={seed}");
    assert_eq!(data_cfg["role"], "data", "seed={seed}: {data_cfg}");
}

#[test]
fn in_process_split_cluster_serves_writes_and_reports_roles() {
    run_in_process_split_cluster_serves_writes_and_reports_roles(env_seed(0xC12A_0009));
}

#[test]
fn in_process_split_cluster_serves_writes_and_reports_roles_over_seeds() {
    for i in 0..5 {
        run_in_process_split_cluster_serves_writes_and_reports_roles(0xC12A_9000 + i);
    }
}

// ---------------------------------------------------------------------------
// (10) cluster_split.rs::fixed_control_node_write_read_is_deterministic
// ---------------------------------------------------------------------------

fn run_fixed_control_node_write_read_is_deterministic(seed: u64) {
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 2);
    let leader = cluster.control_leader_index() as u64;

    // `SimCluster::put` (`cp_kind_write_raw`) does not auto-provision, unlike
    // the real client protocol's `ClientRequest::Put` this original test
    // relies on — create the table over the wire first (a deliberate,
    // documented adaptation; see this module's own doc table).
    let (status, body) = create_table(&mut cluster, leader, "fixed_kv");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let fixed_control = 0u64;
    for i in 0..20u32 {
        let key = format!("fixed-key-{i}");
        let value = format!("fixed-val-{i}").into_bytes();
        cluster
            .put(fixed_control, "fixed_kv", &key, "sk", &value)
            .unwrap_or_else(|e| {
                panic!("seed={seed}: put #{i} via the fixed control node failed: {e}")
            });
        let got = cluster
            .get(fixed_control, "fixed_kv", &key, "sk", true)
            .unwrap_or_else(|e| {
                panic!("seed={seed}: get #{i} via the fixed control node failed: {e}")
            });
        assert_eq!(got, Some(value), "seed={seed}: iteration {i}");
    }
}

#[test]
fn fixed_control_node_write_read_is_deterministic() {
    run_fixed_control_node_write_read_is_deterministic(env_seed(0xC12A_0010));
}

#[test]
fn fixed_control_node_write_read_is_deterministic_over_seeds() {
    for i in 0..5 {
        run_fixed_control_node_write_read_is_deterministic(0xC12A_A000 + i);
    }
}

// ---------------------------------------------------------------------------
// (11) cluster_split.rs::single_shot_first_write_through_control_node_succeeds
// ---------------------------------------------------------------------------

fn run_single_shot_first_write_through_control_node_succeeds(seed: u64) {
    let roles = [
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Control,
        NodeRole::Data,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 2);
    let leader = cluster.control_leader_index() as u64;

    // Every `SimCluster` table-creation primitive already waits out group
    // formation before returning (see this module's own doc table's own
    // entry for this scenario) — so this sibling proves the *shape* (one
    // unretried `put` through a zero-replica control node succeeds), not
    // the original's literal mid-election race.
    let (status, body) = create_table(&mut cluster, leader, "single_shot_kv");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");

    let fixed_control = 0u64;
    cluster
        .put(
            fixed_control,
            "single_shot_kv",
            "first-key",
            "sk",
            b"first-val",
        )
        .unwrap_or_else(|e| panic!("seed={seed}: single-shot put failed: {e}"));
    let got = cluster
        .get(fixed_control, "single_shot_kv", "first-key", "sk", true)
        .unwrap_or_else(|e| panic!("seed={seed}: single-shot get failed: {e}"));
    assert_eq!(got.as_deref(), Some(&b"first-val"[..]), "seed={seed}");
}

#[test]
fn single_shot_first_write_through_control_node_succeeds() {
    run_single_shot_first_write_through_control_node_succeeds(env_seed(0xC12A_0011));
}

#[test]
fn single_shot_first_write_through_control_node_succeeds_over_seeds() {
    for i in 0..5 {
        run_single_shot_first_write_through_control_node_succeeds(0xC12A_B000 + i);
    }
}
