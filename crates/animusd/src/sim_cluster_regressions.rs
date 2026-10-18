//! `SimCluster` seeded regression tests for past bugs whose fixes were
//! covered only by unit or `ProdEnv` (real-thread) tests (the sim-coverage
//! audit). One test (or a small seed loop) per gap; each doc comment names
//! the issue / PR it pins.
//!
//! Seed replay: `ANIMUS_SEED=<seed> cargo test -p animusd --lib
//! sim_cluster_regressions::<test name>`.

use std::time::Duration;

use animus_control::{ColumnType, MetaCommand, NodeAddrs, ProposeResult, TableSchema};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use animus_env::{EnvExt, nid};
use animus_tablet::TOKEN_BYTES;

use super::sim_cluster::SimCluster;
use crate::config::NodeRole;

/// Seeds for a loop-style test: the replay seed alone under `ANIMUS_SEED`,
/// otherwise `count` seeds derived from `base`.
fn seeds(base: u64, count: u64) -> Vec<u64> {
    match std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        Some(s) => vec![s],
        None => (0..count).map(|i| base + i).collect(),
    }
}

/// A single-hash-key (`pk`, string) wire `CreateTable` issued from `node`.
fn wire_create_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

// ---------------------------------------------------------------------------
// J: PR #149 — undersized-cluster RF growth.
// ---------------------------------------------------------------------------

/// Pins PR #149 (`provision_tablet` recorded the placement policy as the
/// creation-time replica count): a tablet wire-created on a 2-node cluster
/// starts with 2 replicas (RF target 3 exceeds the membership) and must grow
/// to 3 once a third node joins. Sim sibling of
/// `tests/tablet_rf_self_heals.rs`.
#[test]
fn undersized_tablet_grows_to_rf3_when_third_node_joins() {
    for seed in seeds(0x7E60_0149, 4) {
        let mut cluster = SimCluster::new(seed, 2, 2);
        let (status, body) = wire_create_table(&mut cluster, 0, "rf_heal");
        assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
        let tablet = *cluster
            .metadata(0)
            .tablets_for_table("rf_heal")
            .next()
            .unwrap_or_else(|| panic!("seed={seed}: no tablet for rf_heal"))
            .0;
        let initial = cluster.metadata(0).tablets[&tablet].replicas.len();
        assert_eq!(
            initial, 2,
            "seed={seed}: precondition: a 2-node cluster can only place 2 replicas"
        );

        let new_node = cluster.grow("data");
        let target = nid(new_node);
        const BUDGET: Duration = Duration::from_secs(30);
        const STEP: Duration = Duration::from_millis(100);
        let mut elapsed = Duration::ZERO;
        loop {
            let reps = cluster.metadata(0).tablets[&tablet].replicas.clone();
            if reps.len() == 3 && reps.contains(&target) {
                break;
            }
            assert!(
                elapsed < BUDGET,
                "seed={seed}: tablet {tablet:?} never grew to RF 3 after node {new_node} \
                 joined within {BUDGET:?}; replicas={reps:?}"
            );
            cluster.run_for(STEP);
            elapsed += STEP;
        }
    }
}

// ---------------------------------------------------------------------------
// I: PR #162 — 2PC half-staged recovery.
// ---------------------------------------------------------------------------

/// A raw plain-KV key of at least `TOKEN_BYTES` length (a tablet's routing
/// token prefix needs that much).
fn txn_key(prefix: &str, suffix: &str) -> Vec<u8> {
    let mut s = format!("{prefix}{suffix}");
    while s.len() < TOKEN_BYTES {
        s.push('_');
    }
    s.into_bytes()
}

/// Pins PR #162 (task #18): the coordinator staged the anchor, whose record
/// names a participant in `participant_spans`, then crashed BEFORE the
/// participant's own stage. In-doubt recovery must ABORT (the participant
/// never staged); the unfixed path saw only the anchor's own keys, called
/// the transaction fully staged and COMMITTED half of it. Sibling of
/// `sim_cluster_dynamo_transact::coordinator_crash_after_prepare_recovers_
/// atomically_to_commit` (the full-stage case) and of the `ProdEnv`
/// `tests/txn_recovery_participant_spans.rs`.
///
/// Oracle: across the whole recovery window neither key is ever readable
/// (no half commit, no late commit), and once the read has stopped being
/// blocked by the anchor's intent both stay absent and fresh writes to both
/// keys succeed and read back (no wedged intent).
#[test]
fn half_staged_coordinator_crash_recovers_to_abort() {
    for seed in seeds(0x7E60_0162, 3) {
        let mut cluster = SimCluster::new(seed, 3, 3);
        for t in ["hs_a", "hs_b"] {
            let (status, body) = wire_create_table(&mut cluster, 0, t);
            assert_eq!(status, 200, "seed={seed}: CreateTable({t}) failed: {body}");
        }
        let coordinator = 0u64;
        let anchor_key = txn_key("anchor", "-hs");
        let participant_key = txn_key("participant", "-hs");
        let mut span_end = participant_key.clone();
        span_end.push(0);
        let participant_spans = vec![(
            "hs_b".to_owned(),
            animus_tablet::KeyRange::new(participant_key.clone(), Some(span_end)),
        )];

        // Stage the anchor only; the participant's stage never happens.
        let (txn_id, record_key, record_table) = cluster.txn_prepare_only(
            coordinator,
            "hs_a",
            None,
            participant_spans,
            anchor_key.clone(),
            Some(b"anchor-must-not-commit".to_vec()),
        );
        // Time advances between the propose and the crash.
        cluster.run_for(Duration::from_millis(500));
        cluster.crash(coordinator);

        // `SimCluster` spawns no `txn_resolver_loop`; drive the same
        // recovery entry point it calls (`ClientCtx::txn_recover`) from a
        // survivor, once per attempt, until it decides.
        let reader = 1u64;
        let mut decided = None;
        let mut last = String::new();
        for _ in 0..12 {
            let (id, rk, rt) = (txn_id.clone(), record_key.clone(), record_table.clone());
            let status = cluster
                .run_with_ctx(reader, move |ctx| async move {
                    ctx.txn_recover(&rt, &rk, &id, None).await
                })
                .unwrap_or_else(|| panic!("seed={seed}: txn_recover did not complete"));
            let a = cluster.raw_get(reader, "hs_a", anchor_key.clone(), true);
            let p = cluster.raw_get(reader, "hs_b", participant_key.clone(), true);
            last = format!("status={status:?} anchor={a:?} participant={p:?}");
            assert!(
                !matches!(
                    status,
                    Ok(animus_cp_data::TxnDecisionStatus::Committed { .. })
                ) && !matches!(a, Ok(Some(_)))
                    && !matches!(p, Ok(Some(_))),
                "seed={seed}: recovery COMMITTED a half-staged transaction \
                 (atomicity violation): {last}"
            );
            if matches!(status, Ok(animus_cp_data::TxnDecisionStatus::Aborted)) {
                decided = Some(status);
                break;
            }
        }
        assert!(
            decided.is_some(),
            "seed={seed}: recovery never decided the half-staged transaction; last: {last}"
        );
        // The abort is final and leaves nothing wedged: both keys absent and
        // freshly writable, the anchor holding only the post-abort write.
        assert_eq!(
            cluster.raw_get(reader, "hs_a", anchor_key.clone(), true),
            Ok(None),
            "seed={seed}: aborted anchor key must read absent"
        );
        cluster
            .put_raw(reader, "hs_a", anchor_key.clone(), b"fresh-a".to_vec())
            .unwrap_or_else(|e| panic!("seed={seed}: anchor key left wedged: {e}"));
        cluster
            .put_raw(reader, "hs_b", participant_key.clone(), b"fresh-b".to_vec())
            .unwrap_or_else(|e| panic!("seed={seed}: participant key left wedged: {e}"));
        assert_eq!(
            cluster.raw_get(reader, "hs_a", anchor_key.clone(), true),
            Ok(Some(b"fresh-a".to_vec())),
            "seed={seed}: anchor key must hold only the post-abort write"
        );
    }
}

// ---------------------------------------------------------------------------
// D: issues #268 / #283, PR #42 — propose pacing under no-commit.
// ---------------------------------------------------------------------------

/// Upper bound on put-attributable control-log appends over a ~12s no-commit
/// window: one per `SCHEMA_PROPOSE_PATIENCE` (1s) for at most the 10s commit
/// budget is ~10-11; an unpaced 50ms poll loop appends ~200. Generous
/// headroom, still ten times below the regression.
const PACING_APPEND_BOUND: u64 = 20;

/// Three combined nodes plus one data-only node (whose control handle is a
/// remote mirror, so its proposals relay to the leader). The control leader
/// (returned) is left alone with its quorum gone: both followers are
/// crashed, so it can accept proposals but never commit them. A schema-only
/// table `pace_t` (no tablet yet) is committed first, while healthy.
fn quorumless_leader(seed: u64) -> (SimCluster, u64) {
    let roles = [
        NodeRole::Both,
        NodeRole::Both,
        NodeRole::Both,
        NodeRole::Data,
    ];
    let mut cluster = SimCluster::new_with_roles(seed, &roles, 3);
    let leader = cluster.control_leader_index() as u64;
    assert!(
        matches!(
            cluster.propose_meta(MetaCommand::CreateTableSchema {
                table: "pace_t".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ProposeResult::Accepted { .. }
        ),
        "seed={seed}: CreateTableSchema not accepted by the control leader"
    );
    cluster.run_for(Duration::from_secs(3));
    for n in 0..3u64 {
        if n != leader {
            cluster.crash(n);
        }
    }
    // Let the leader notice nothing can commit.
    cluster.run_for(Duration::from_secs(2));
    (cluster, leader)
}

/// Heal the quorum (restart both crashed followers) and let it re-elect.
fn heal_quorum(cluster: &mut SimCluster, leader: u64) {
    cluster.restart((leader + 1) % 3);
    cluster.restart((leader + 2) % 3);
    cluster.run_for(Duration::from_secs(5));
}

/// The control leader's log growth over a window with nothing in flight: its
/// own steady append rate (heartbeat-driven), subtracted from every measured
/// window so only the operation's own proposals remain.
fn idle_baseline(cluster: &mut SimCluster, leader: u64) -> u64 {
    let b0 = cluster.control_last_log_index(leader);
    cluster.run_for(Duration::from_secs(12));
    cluster.control_last_log_index(leader) - b0
}

/// Run `fut` on `node` over the usual window and return its output with the
/// leader's attributable control-log growth (growth minus `baseline`).
fn appended_during<T, Fut>(
    cluster: &mut SimCluster,
    leader: u64,
    node: u64,
    baseline: u64,
    seed: u64,
    f: impl FnOnce(super::sim_cluster::SimNodeCtx) -> Fut,
) -> (T, u64)
where
    T: Send + 'static,
    Fut: std::future::Future<Output = T> + Send + 'static,
{
    let before = cluster.control_last_log_index(leader);
    let out = cluster
        .run_with_ctx(node, f)
        .unwrap_or_else(|| panic!("seed={seed}: the operation on node {node} did not resolve"));
    let grew = cluster.control_last_log_index(leader) - before;
    (out, grew.saturating_sub(baseline))
}

fn assert_paced(what: &str, node: u64, extra: u64, seed: u64) {
    assert!(
        extra >= 1,
        "seed={seed}: vacuous run: {what} on node {node} appended nothing, so the leader never \
         accepted its proposal"
    );
    assert!(
        extra <= PACING_APPEND_BOUND,
        "seed={seed}: {what} on node {node} re-proposed too aggressively with no commit quorum: \
         {extra} attributable appends (paced ~10, unpaced 50ms loop ~200)"
    );
}

/// Pins issues #268/#283 (`provision_tablet` re-proposed `CreateTablet` every
/// 50ms poll tick under slow commit): with the control group unable to
/// commit, `provision_tablet` must append about one entry per
/// `SCHEMA_PROPOSE_PATIENCE`, not one per tick, whether it runs on the
/// control leader or on a data-only node relaying to it; after healing it
/// provisions. Sim sibling of `tests/provision_propose_pacing.rs`.
#[test]
fn provision_tablet_does_not_respam_proposals_without_a_commit_quorum() {
    for seed in seeds(0x7E60_0268, 3) {
        let (mut cluster, leader) = quorumless_leader(seed);
        let baseline = idle_baseline(&mut cluster, leader);
        for node in [leader, 3] {
            let (res, extra) = appended_during(
                &mut cluster,
                leader,
                node,
                baseline,
                seed,
                |ctx| async move { ctx.provision_tablet("pace_t").await },
            );
            assert!(
                res.is_err(),
                "seed={seed}: nothing can commit, provision_tablet on node {node} must time \
                 out: {res:?}"
            );
            assert_paced("provision_tablet", node, extra, seed);
        }

        heal_quorum(&mut cluster, leader);
        let healed = cluster
            .run_with_ctx(
                leader,
                |ctx| async move { ctx.provision_tablet("pace_t").await },
            )
            .unwrap_or_else(|| panic!("seed={seed}: healed provision_tablet did not resolve"));
        assert!(
            healed.is_ok(),
            "seed={seed}: provision_tablet must succeed once the quorum is back: {healed:?}"
        );
        assert!(
            cluster
                .metadata(leader)
                .tablets_for_table("pace_t")
                .next()
                .is_some(),
            "seed={seed}: no tablet exists for pace_t after the healed provision"
        );
    }
}

/// Pins PR #42 (`propose_and_await` resubmitted its `MetaCommand` every 50ms
/// poll, ~200 duplicates per wait): with no commit quorum it must append
/// about one entry per `SCHEMA_PROPOSE_PATIENCE` when relaying from a
/// non-leader (on the control leader itself it holds its single pending
/// entry and appends no duplicate at all), give up at its timeout, and
/// succeed after healing.
#[test]
fn propose_and_await_does_not_respam_proposals_without_a_commit_quorum() {
    // `committed` polls the node's cached view: a fresh read would itself
    // block for the whole window without a quorum, which would mask a
    // re-propose-per-tick bug behind its own latency.
    fn ttl_set(ctx: &super::sim_cluster::SimNodeCtx) -> bool {
        ctx.effective_metadata().table_ttl("pace_t").is_some()
    }
    fn command() -> MetaCommand {
        MetaCommand::SetTableTtl {
            table: "pace_t".to_owned(),
            spec: Some(animus_control::TtlSpec {
                attribute_name: "ttl".to_owned(),
            }),
        }
    }
    for seed in seeds(0x7E60_0042, 3) {
        let (mut cluster, leader) = quorumless_leader(seed);
        let baseline = idle_baseline(&mut cluster, leader);
        for node in [leader, 3] {
            let (res, extra) = appended_during(
                &mut cluster,
                leader,
                node,
                baseline,
                seed,
                |ctx| async move {
                    let c = ctx.clone();
                    ctx.propose_and_await(command(), Duration::from_secs(10), || {
                        let hit = ttl_set(&c);
                        async move { hit.then_some(()) }
                    })
                    .await
                },
            );
            assert!(
                res.is_err(),
                "seed={seed}: nothing can commit, propose_and_await on node {node} must give up"
            );
            // The leader-local path appends exactly one pending entry and
            // holds it; only the relaying node paces repeated proposals.
            if node == 3 {
                assert_paced("propose_and_await", node, extra, seed);
            } else {
                assert!(
                    extra <= PACING_APPEND_BOUND,
                    "seed={seed}: leader-local propose_and_await appended {extra} entries"
                );
            }
        }

        heal_quorum(&mut cluster, leader);
        let healed = cluster
            .run_with_ctx(leader, |ctx| async move {
                let c = ctx.clone();
                ctx.propose_and_await(command(), Duration::from_secs(10), || {
                    let hit = ttl_set(&c);
                    async move { hit.then_some(()) }
                })
                .await
            })
            .unwrap_or_else(|| panic!("seed={seed}: healed propose_and_await did not resolve"));
        assert!(
            healed.is_ok(),
            "seed={seed}: propose_and_await must commit once the quorum is back"
        );
    }
}

// ---------------------------------------------------------------------------
// E: issue #265, PRs #106 / #275 — CreateTable ack implies serveable.
// ---------------------------------------------------------------------------

/// Pins issue #265 / PRs #106, #275 (`CreateTable` acked 200 before the
/// table's first tablet group had formed and elected, so the very next write
/// raced the formation window or failed). Sim sibling of
/// `tests/create_table_ready.rs`; the `SimCluster` helpers (`create_table`,
/// and `dynamo`, which burns a full op budget of virtual time after the
/// response) hide that window, so this drives the wire request from a task
/// and samples the cluster in the same task step the response lands, with no
/// virtual time passing in between.
///
/// Oracle, at the instant of the 200: every tablet of the table has a
/// leader (`SimClusterHandle::leader_index_of`, exact, not polled); then a
/// write through every node (combined, control-only forwarding, data-only
/// forwarding) succeeds in one shot. `CreateTable` is issued from each node
/// kind in turn (one seed per issuing node).
#[test]
fn create_table_ack_implies_every_tablet_has_a_leader_and_serves_every_node() {
    let roles = [
        NodeRole::Both,
        NodeRole::Both,
        NodeRole::Both,
        NodeRole::Control,
        NodeRole::Data,
    ];
    let base = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok());
    let runs: Vec<(u64, u64)> = match base {
        Some(seed) => vec![(seed, seed % roles.len() as u64)],
        None => (0..roles.len() as u64)
            .map(|issuer| (0x7E60_0265 + issuer, issuer))
            .collect(),
    };
    for (seed, issuer) in runs {
        let mut cluster = SimCluster::new_with_roles(seed, &roles, 3);
        let handle = cluster.handle();
        let node_count = roles.len() as u64;
        let table = format!("ready_{issuer}");
        let body = format!(
            r#"{{"TableName":"{table}",
                "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
                "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
        );
        let observed = cluster
            .run_with_ctx(issuer, move |_ctx| async move {
                let (status, resp) = handle
                    .dynamo(issuer, "DynamoDB_20120810.CreateTable", body.as_bytes())
                    .await;
                // No await since the response: this is the ack instant.
                let tablets: Vec<_> = handle
                    .metadata(issuer)
                    .tablets_for_table(&table)
                    .map(|(id, _)| *id)
                    .collect();
                let leaderless: Vec<_> = tablets
                    .iter()
                    .filter(|t| handle.leader_index_of(**t).is_none())
                    .copied()
                    .collect();
                let mut writes = Vec::new();
                for node in 0..node_count {
                    // The raw routed write every node kind can issue (a
                    // control-only node forwards it; it has no dynamo edge).
                    let r = handle
                        .put(node, &table, &format!("k{node}"), "sk", b"v")
                        .await;
                    writes.push((node, r));
                }
                (status, resp, tablets, leaderless, writes)
            })
            .unwrap_or_else(|| {
                panic!("seed={seed}: CreateTable from node {issuer} did not finish")
            });
        let (status, resp, tablets, leaderless, writes) = observed;
        assert_eq!(
            status, 200,
            "seed={seed}: CreateTable from node {issuer} failed: {resp}"
        );
        assert!(
            !tablets.is_empty(),
            "seed={seed}: CreateTable acked but the issuing node's metadata has no tablet"
        );
        assert!(
            leaderless.is_empty(),
            "seed={seed}: CreateTable (from node {issuer}) was acked before tablet(s) \
             {leaderless:?} had an elected leader"
        );
        for (node, r) in writes {
            assert!(
                r.is_ok(),
                "seed={seed}: first write through node {node} right after the CreateTable \
                 ack (issuer {issuer}) failed: {r:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// M: issue #1274 — readiness is not gated on a control leader.
// ---------------------------------------------------------------------------

/// One admin `GET` on `node`, parsed as JSON.
fn admin_json(cluster: &mut SimCluster, node: u64, path: &str) -> (u16, serde_json::Value) {
    let (status, body) = cluster.admin(node, "GET", path, "", &[]);
    let json = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// Pins issue #1274 (control-plane quorum loss made `/admin/health` 503, and
/// since the operator's readiness probe was `/admin/health`, every client
/// endpoint was removed although the tablet groups kept serving): with the
/// control quorum gone, a node that has synced metadata keeps
/// `/admin/ready` at 200 (`control_leader_recent` a pure diagnostic) while
/// `/admin/health` goes 503; once the quorum returns both are 200 again.
/// Sim sibling of `admin_endpoint.rs::admin_ready_is_200_while_leaderless_
/// with_metadata`; the sim tier previously had only a happy-path smoke.
#[test]
fn admin_ready_stays_200_while_health_is_503_without_a_control_quorum() {
    for seed in seeds(0x7E60_1274, 3) {
        let mut cluster = SimCluster::new(seed, 3, 3);
        cluster.create_table("ready_t");
        // The survivor must be a follower: a lone former leader keeps
        // leading (no check-quorum step-down) so its health stays 200.
        let survivor = cluster.control_follower_index();

        let (h, hb) = admin_json(&mut cluster, survivor, "/admin/health");
        let (r, rb) = admin_json(&mut cluster, survivor, "/admin/ready");
        assert_eq!(
            (h, r),
            (200, 200),
            "seed={seed}: a healthy cluster must report both 200: health={hb} ready={rb}"
        );

        // Lose the control quorum: crash the other two voters.
        for n in 0..3u64 {
            if n != survivor {
                cluster.crash(n);
            }
        }

        // Converged-or-timeout: health goes 503 once the leader lease lapses.
        let mut health = None;
        for _ in 0..10 {
            let (status, body) = admin_json(&mut cluster, survivor, "/admin/health");
            if status == 503 {
                health = Some(body);
                break;
            }
        }
        let health = health.unwrap_or_else(|| {
            panic!("seed={seed}: /admin/health never went 503 after the control quorum was lost")
        });
        assert_eq!(
            health["control_leader_recent"], false,
            "seed={seed}: {health}"
        );

        let (status, ready) = admin_json(&mut cluster, survivor, "/admin/ready");
        assert_eq!(
            status, 200,
            "seed={seed}: readiness must not gate on a control leader: {ready}"
        );
        assert_eq!(ready["ok"], true, "seed={seed}: {ready}");
        assert_eq!(ready["metadata_synced"], true, "seed={seed}: {ready}");
        assert_eq!(
            ready["control_leader_recent"], false,
            "seed={seed}: {ready}"
        );
        assert_eq!(ready["consensus_task_panics"], 0, "seed={seed}: {ready}");

        // Heal: the quorum returns and health recovers.
        for n in 0..3u64 {
            if n != survivor {
                cluster.restart(n);
            }
        }
        let mut recovered = false;
        for _ in 0..10 {
            let (status, _) = admin_json(&mut cluster, survivor, "/admin/health");
            if status == 200 {
                recovered = true;
                break;
            }
        }
        assert!(
            recovered,
            "seed={seed}: /admin/health never recovered after the quorum returned"
        );
        let (status, ready) = admin_json(&mut cluster, survivor, "/admin/ready");
        assert_eq!(status, 200, "seed={seed}: {ready}");
    }
}

// ---------------------------------------------------------------------------
// K: PR #714 / issue #712 — admin add-control-member vs the target's own
// self-registration.
// ---------------------------------------------------------------------------

/// Pins PR #714 / issue #712 (`admin_add_control_member` read a control-plane
/// view that lagged the leader's own log, concluded the target was unclaimed,
/// proposed its own `RegisterNode` and collided with the target's
/// self-registration, surfacing a spurious 409, or worse committed a blank
/// address book). The target's real self-registration (full `client` /
/// `admin` / `intra` addresses) is appended to the leader's log, and the
/// operator's add-control-member call for the same id arrives before that
/// entry has committed and applied, so the leader's cache has no trace of the
/// id. The add must succeed (never a 409 collision), the target must become a
/// voter, and its address book must still hold the target's own registration
/// (not a blank re-derivation). The gap between the two events is swept
/// (0-8ms of virtual time) so the commit-and-apply boundary falls on both
/// sides of the admin call's barrier.
#[test]
fn admin_add_control_member_vs_inflight_self_registration_never_409s() {
    const TARGET: u64 = 70;
    const INTERNAL: &str = "127.0.0.1:1";
    let real = || NodeAddrs {
        internal: INTERNAL.to_owned(),
        client: "127.0.0.1:2".to_owned(),
        intra: "127.0.0.1:3".to_owned(),
        admin: "127.0.0.1:4".to_owned(),
        role: "control".to_owned(),
    };
    for base in seeds(0x7E60_0714, 3) {
        for gap_ms in [0u64, 1, 2, 4, 8] {
            let seed = base;
            let mut cluster = SimCluster::new_with_roles(seed, &[NodeRole::Both; 3], 1);
            let leader = cluster.control_leader_index() as u64;
            let accepted = cluster.propose_meta(MetaCommand::RegisterNode {
                node: nid(TARGET),
                addrs: real(),
                labels: Default::default(),
                capacity: None,
            });
            assert!(
                matches!(accepted, ProposeResult::Accepted { .. }),
                "seed={seed} gap={gap_ms}ms: self-registration not accepted: {accepted:?}"
            );
            if gap_ms > 0 {
                cluster.run_for(Duration::from_millis(gap_ms));
            }
            let body =
                serde_json::json!({"node": nid(TARGET).to_string(), "addr": INTERNAL}).to_string();
            let (status, resp) = cluster.admin(
                leader,
                "POST",
                "/admin/control/member/add",
                "",
                body.as_bytes(),
            );
            assert_eq!(
                status, 200,
                "seed={seed} gap={gap_ms}ms: add-control-member raced the target's own \
                 self-registration and failed (spurious collision?): {resp}"
            );
            // The target's own registration survives intact.
            cluster.run_for(Duration::from_secs(2));
            let meta = cluster.metadata(leader);
            assert_eq!(
                meta.node_addrs.get(&nid(TARGET)),
                Some(&real()),
                "seed={seed} gap={gap_ms}ms: the target's address book entry was overwritten \
                 or blanked"
            );
            assert!(
                cluster
                    .control_voters(leader)
                    .is_some_and(|v| v.contains(&nid(TARGET))),
                "seed={seed} gap={gap_ms}ms: the target never became a control voter"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// A: issues #911 / #967 / #971 / #1308, PRs #917 / #970 / #975 — confirm-loop
// false supersede.
// ---------------------------------------------------------------------------

type Slot = Arc<Mutex<Option<(u16, String)>>>;

/// Spawn one DynamoDB request on `node`'s own env without driving the
/// simulator, so a fault can be injected while it is in flight.
fn spawn_dynamo(cluster: &SimCluster, node: u64, target: &'static str, body: String) -> Slot {
    let handle = cluster.handle();
    let slot: Slot = Arc::new(Mutex::new(None));
    let out = slot.clone();
    handle.env(node).spawn_task(async move {
        let r = handle.dynamo(node, target, body.as_bytes()).await;
        *out.lock().expect("slot poisoned") = Some(r);
    });
    slot
}

/// Pins the confirm-loop false-supersede family: issues #911 / #967 / #971
/// (a write accepted by the tablet leader whose confirm loop gave up on
/// `!is_leader()` alone, so a term bump that does NOT lose the entry made
/// the proposer treat its accepted write as superseded and re-propose it),
/// PRs #917 / #970 / #975, and issue #1308 (the confirm loop sampling the
/// applied index twice and reporting its own just-applied entry as
/// superseded). Each retry duplicated the write: a second item version and
/// a second Streams change record.
///
/// Scenario: a streamed table; in each round a batch of `PutItem`s and one
/// `BatchWriteItem` (distinct fresh keys, each issued exactly once, never
/// retried by the test) are accepted by the tablet leader, then, `d`
/// milliseconds later, the leader is partitioned from both followers long
/// enough for them to elect a new leader (a term bump; the entries that had
/// already reached a follower survive and commit under it), and healed so the
/// old leader steps down while its confirm loops are still waiting. `d` is
/// swept so the cut lands before, during and after replication of the
/// accepted entries. A genuinely lost entry (cut before it replicated) is
/// legitimately retried by the product and still applies once.
///
/// Oracle: after convergence, every key has AT MOST ONE change record (a
/// duplicate apply would add a second one), and a write the client was told
/// succeeded has exactly one. The proposals-vs-writes counter is not usable
/// as the oracle here: a genuinely lost entry legitimately costs a second
/// proposal.
#[test]
fn leadership_flap_during_confirm_never_double_applies_an_accepted_write() {
    const PER_ROUND: usize = 4;
    const BATCH: usize = 3;
    for seed in seeds(0x7E60_0911, 3) {
        let mut cluster = SimCluster::new(seed, 3, 3);
        let table = "flap";
        let (status, body) = wire_create_table(&mut cluster, 0, table);
        assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
        let upd = format!(
            r#"{{"TableName":"{table}","StreamSpecification":
                {{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
        );
        let (status, body) = cluster.dynamo(0, "DynamoDB_20120810.UpdateTable", upd.as_bytes());
        assert_eq!(
            status, 200,
            "seed={seed}: enabling the stream failed: {body}"
        );
        let tablet = *cluster
            .metadata(0)
            .tablets_for_table(table)
            .next()
            .expect("tablet")
            .0;

        // key -> client was told it succeeded
        let mut acked: BTreeMap<String, bool> = BTreeMap::new();
        for (round, d_ms) in [1u64, 2, 3, 4, 6, 8, 12, 16, 24].into_iter().enumerate() {
            let mut waiting = cluster.leader_index_of(tablet);
            for _ in 0..40 {
                if waiting.is_some() {
                    break;
                }
                cluster.run_for(Duration::from_millis(250));
                waiting = cluster.leader_index_of(tablet);
            }
            let leader = waiting
                .unwrap_or_else(|| panic!("seed={seed} round={round}: tablet has no leader"));
            let mut slots: Vec<(Vec<String>, Slot)> = Vec::new();
            for i in 0..PER_ROUND {
                let key = format!("r{round}_p{i}");
                let body = format!(
                    r#"{{"TableName":"{table}","Item":{{"pk":{{"S":"{key}"}},"v":{{"S":"x"}}}}}}"#
                );
                slots.push((
                    vec![key],
                    spawn_dynamo(&cluster, leader, "DynamoDB_20120810.PutItem", body),
                ));
            }
            let batch_keys: Vec<String> = (0..BATCH).map(|i| format!("r{round}_b{i}")).collect();
            let reqs: Vec<String> = batch_keys
                .iter()
                .map(|k| {
                    format!(r#"{{"PutRequest":{{"Item":{{"pk":{{"S":"{k}"}},"v":{{"S":"x"}}}}}}}}"#)
                })
                .collect();
            let body = format!(r#"{{"RequestItems":{{"{table}":[{}]}}}}"#, reqs.join(","));
            slots.push((
                batch_keys,
                spawn_dynamo(&cluster, leader, "DynamoDB_20120810.BatchWriteItem", body),
            ));

            cluster.run_for(Duration::from_millis(d_ms));
            for n in 0..3u64 {
                if n != leader {
                    cluster.partition(leader, n);
                }
            }
            // Long enough for the followers to elect a new leader.
            cluster.run_for(Duration::from_millis(2500));
            cluster.heal_all();
            cluster.run_for(Duration::from_secs(8));

            for (keys, slot) in slots {
                let result = slot.lock().expect("slot poisoned").take();
                let ok = matches!(&result, Some((200, b)) if !b.contains("UnprocessedItems\":{\"")
                    || b.contains("UnprocessedItems\":{}"));
                for k in keys {
                    acked.insert(k, ok);
                }
            }
        }

        // Converge, then read the whole open tail from the current leader.
        cluster.run_for(Duration::from_secs(5));
        cluster.await_replicas_caught_up("flap workload");
        let label = cluster
            .metadata(0)
            .table_stream(table)
            .map(|s| s.label.clone())
            .expect("stream label");
        let arn = format!("arn:aws:dynamodb:animus:0:table/{table}/stream/{label}");
        let reader = cluster
            .leader_index_of(tablet)
            .unwrap_or_else(|| panic!("seed={seed}: no tablet leader after the workload"));
        let (status, desc) = cluster.dynamo_streams(
            reader,
            "DynamoDBStreams_20120810.DescribeStream",
            format!(r#"{{"StreamArn":"{arn}"}}"#).as_bytes(),
        );
        assert_eq!(status, 200, "seed={seed}: DescribeStream failed: {desc}");
        let desc: serde_json::Value = serde_json::from_str(&desc).expect("describe json");
        let shards = desc["StreamDescription"]["Shards"]
            .as_array()
            .expect("shards");
        let mut per_key: BTreeMap<String, usize> = BTreeMap::new();
        for shard in shards {
            let shard_id = shard["ShardId"].as_str().expect("shard id");
            let body = format!(
                r#"{{"StreamArn":"{arn}","ShardId":"{shard_id}","ShardIteratorType":"TRIM_HORIZON"}}"#
            );
            let (status, it) = cluster.dynamo_streams(
                reader,
                "DynamoDBStreams_20120810.GetShardIterator",
                body.as_bytes(),
            );
            assert_eq!(status, 200, "seed={seed}: GetShardIterator failed: {it}");
            let mut iter =
                serde_json::from_str::<serde_json::Value>(&it).expect("iter json")["ShardIterator"]
                    .as_str()
                    .expect("iterator")
                    .to_owned();
            for _ in 0..50 {
                let (status, resp) = cluster.dynamo_streams(
                    reader,
                    "DynamoDBStreams_20120810.GetRecords",
                    format!(r#"{{"ShardIterator":"{iter}","Limit":1000}}"#).as_bytes(),
                );
                assert_eq!(status, 200, "seed={seed}: GetRecords failed: {resp}");
                let v: serde_json::Value = serde_json::from_str(&resp).expect("records json");
                let records = v["Records"].as_array().cloned().unwrap_or_default();
                for r in &records {
                    if let Some(pk) = r["dynamodb"]["Keys"]["pk"]["S"].as_str() {
                        *per_key.entry(pk.to_owned()).or_default() += 1;
                    }
                }
                match v["NextShardIterator"].as_str() {
                    Some(next) if !records.is_empty() => iter = next.to_owned(),
                    _ => break,
                }
            }
        }

        let dupes: Vec<_> = per_key.iter().filter(|(_, n)| **n > 1).collect();
        assert!(
            dupes.is_empty(),
            "seed={seed}: an accepted write was applied more than once (change records per \
             key, keys with >1): {dupes:?}"
        );
        let lost: Vec<_> = acked
            .iter()
            .filter(|(k, ok)| **ok && !per_key.contains_key(*k))
            .map(|(k, _)| k)
            .collect();
        assert!(
            lost.is_empty(),
            "seed={seed}: writes the client was told succeeded have no change record: {lost:?}"
        );
    }
}
