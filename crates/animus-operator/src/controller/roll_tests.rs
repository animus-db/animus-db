//! `reconcile`-level tests of the rolling-upgrade driver (ADR 0073 Phase 3,
//! P3-D) over `crate::fakes::{FakeClusterApi, FakeAdminClient}`: the
//! operator-side rows of the ADR's test plan. The pure decision rules are
//! tested in `crate::roll`'s own unit tests and (the machine itself) in
//! `animus-roll`; these pin the wiring: what gets applied, which admin calls
//! are (not) made, and what lands on `status`.
//!
//! Like every fake-driven test in this crate they prove the operator's
//! decisions, **not** Kubernetes' partition semantics or a real pod restart:
//! the kind e2e is the only thing that does.

use std::collections::BTreeMap;
use std::sync::Arc;

use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetStatus};
use k8s_openapi::api::core::v1::{ConfigMap, Pod, PodCondition, PodSpec, PodStatus};
use serde_json::{Value, json};

use super::*;
use crate::crd::{
    AnimusClusterSpec, CONDITION_ROLL_COMPLETE, CONDITION_UPGRADE_BLOCKED,
    CONDITION_UPGRADE_CHANGES_HELD, CONDITION_UPGRADE_FINALIZE_PENDING,
    CONDITION_UPGRADE_IN_PROGRESS, FinalizePolicy, UpgradePhase, UpgradeSpec, UpgradeStatus,
};
use crate::desired::statefulset::build_with_partition;
use crate::desired::test_support::test_cluster;
use crate::fakes::{FakeAdminClient, FakeClusterApi};
use crate::roll::{REVISION_LABEL, WallClock};

const NAME: &str = "demo";
const NS: &str = "ns1";
const OLD: &str = "img:1";
const NEW: &str = "img:2";

fn cluster(nodes: i32, control: i32, image: &str) -> AnimusCluster {
    let mut c = test_cluster(NAME, NS, nodes, Some(control));
    c.spec.image = Some(image.to_string());
    c
}

fn pod(ordinal: i32, rev: &str, ready: bool) -> Pod {
    let mut p = Pod::default();
    p.metadata.name = Some(format!("{NAME}-{ordinal}"));
    p.metadata.labels = Some(BTreeMap::from([(
        REVISION_LABEL.to_string(),
        rev.to_string(),
    )]));
    p.spec = Some(PodSpec {
        node_name: Some("n".to_string()),
        ..Default::default()
    });
    p.status = Some(PodStatus {
        conditions: Some(vec![PodCondition {
            type_: "Ready".to_string(),
            status: if ready { "True" } else { "False" }.to_string(),
            ..Default::default()
        }]),
        ..Default::default()
    });
    p
}

/// A live `StatefulSet` as the cluster would store it after our last apply of
/// `c`'s template at `partition`, with the controller's own revisions.
fn live_sts(c: &AnimusCluster, partition: i32, update: &str, current: &str) -> StatefulSet {
    let mut s = build_with_partition(c, &c.spec, partition);
    s.status = Some(StatefulSetStatus {
        update_revision: Some(update.to_string()),
        current_revision: Some(current.to_string()),
        ready_replicas: Some(c.spec.nodes),
        ..Default::default()
    });
    s
}

fn cv_view(era: bool, active: u32, can_finalize: bool, ranges: &[u32], statuses: &[&str]) -> Value {
    let nodes: Vec<Value> = ranges
        .iter()
        .enumerate()
        .map(|(i, m)| {
            json!({
                "node": format!("{NAME}-{i}"),
                "role": if i < 3 { "combined" } else { "data" },
                "status": statuses.get(i).copied().unwrap_or("Active"),
                "range": {"min": 1, "max": m},
            })
        })
        .collect();
    json!({
        "era_active": era,
        "active": active,
        "own_range": {"min": 1, "max": 2},
        "nodes": nodes,
        "can_finalize": can_finalize,
        "target": active + 1,
        "blockers": [],
    })
}

struct Roll {
    ctx: Arc<Context<FakeClusterApi, FakeAdminClient>>,
    cluster: AnimusCluster,
}

impl Roll {
    fn new(cluster: AnimusCluster) -> Self {
        let api = FakeClusterApi::new();
        api.seed_node_labels("n", &[]);
        let ctx = Arc::new(Context {
            cluster_api: api,
            admin: FakeAdminClient::new(),
            clock: WallClock::fixed(10_000),
        });
        Self { ctx, cluster }
    }

    fn api(&self) -> &FakeClusterApi {
        &self.ctx.cluster_api
    }

    fn admin(&self) -> &FakeAdminClient {
        &self.ctx.admin
    }

    /// Seed the live `StatefulSet` (this roll's own template at `partition`)
    /// and the pods as `(revision, ready)` per ordinal.
    fn world(&self, partition: i32, pods: &[(&str, bool)]) {
        self.api()
            .seed_statefulset_full(live_sts(&self.cluster, partition, "r2", "r1"));
        self.api().set_pods(
            pods.iter()
                .enumerate()
                .map(|(i, (rev, ready))| pod(i as i32, rev, *ready))
                .collect(),
        );
    }

    /// Script the admin side: a healthy cluster-version view and roll-health
    /// everywhere, `leader` leading.
    fn healthy(&self, era: bool, active: u32, can_finalize: bool, ranges: &[u32], leader: i32) {
        let admin = self.admin();
        admin.clear_scripted_gets();
        admin.script_get(
            None,
            "/admin/cluster-version",
            Ok(cv_view(era, active, can_finalize, ranges, &[])),
        );
        admin.script_get(
            None,
            "/admin/roll-health",
            Ok(json!({"ok": true, "reasons": []})),
        );
        for i in 0..ranges.len() as i32 {
            admin.script_get(
                Some(i),
                "/admin/health",
                Ok(json!({"ok": true, "is_control_leader": i == leader})),
            );
        }
    }

    async fn tick(&mut self) -> Action {
        let a = reconcile(Arc::new(self.cluster.clone()), Arc::clone(&self.ctx))
            .await
            .expect("reconcile");
        // What the watch would hand the next reconcile: our own status.
        self.cluster.status = self.api().last_status();
        a
    }

    fn partition(&self) -> i32 {
        self.api()
            .statefulset(NAME)
            .unwrap()
            .spec
            .unwrap()
            .update_strategy
            .unwrap()
            .rolling_update
            .unwrap()
            .partition
            .unwrap()
    }

    fn applied_image(&self) -> Option<String> {
        crate::desired::statefulset::template_image(&self.api().statefulset(NAME).unwrap())
    }

    fn cond(&self, t: &str) -> Option<String> {
        self.cluster
            .status
            .as_ref()?
            .conditions
            .iter()
            .find(|c| c.type_ == t)
            .and_then(|c| c.message.clone())
    }

    fn upgrade(&self) -> UpgradeStatus {
        self.cluster
            .status
            .as_ref()
            .and_then(|s| s.upgrade.clone())
            .expect("status.upgrade")
    }

    /// Admin calls the *roll* made (observation or effects); the unrelated
    /// control-growth probe (`/admin/control/members`, `/admin/config`) that
    /// every reconcile with a prior ConfigMap makes is not counted.
    fn admin_calls(&self) -> usize {
        self.admin()
            .calls()
            .iter()
            .filter(|(_, u)| {
                [
                    "/admin/roll-health",
                    "/admin/cluster-version",
                    "/admin/health",
                    "/admin/status",
                    "/admin/control/transfer",
                ]
                .iter()
                .any(|p| u.ends_with(p))
            })
            .count()
    }

    fn posts(&self, path: &str) -> Vec<Value> {
        self.admin()
            .post_bodies()
            .into_iter()
            .filter(|(p, _)| p == path)
            .map(|(_, b)| b)
            .collect()
    }
}

// --- D8: the first apply of a changed template carries partition = replicas ---

#[tokio::test]
async fn a_changed_image_is_applied_with_partition_equal_to_replicas() {
    let old = cluster(3, 3, OLD);
    let mut roll = Roll::new(cluster(3, 3, NEW));
    // The stored StatefulSet was applied from the OLD image at partition 0.
    roll.api()
        .seed_statefulset_full(live_sts(&old, 0, "r1", "r1"));
    roll.api()
        .set_pods((0..3).map(|i| pod(i, "r1", true)).collect());

    let action = roll.tick().await;

    assert_eq!(roll.applied_image().as_deref(), Some(NEW));
    assert_eq!(
        roll.partition(),
        3,
        "template and partition land in ONE apply"
    );
    // exactly one StatefulSet apply: there is no second patch that could
    // briefly leave the new template at partition 0
    let sts_applies = roll
        .api()
        .applies()
        .iter()
        .filter(|(k, _)| *k == crate::fakes::AppliedKind::StatefulSet)
        .count();
    assert_eq!(sts_applies, 1);
    let up = roll.upgrade();
    assert_eq!(up.phase, UpgradePhase::InProgress);
    assert_eq!(up.from_image.as_deref(), Some(OLD));
    assert_eq!(up.to_image.as_deref(), Some(NEW));
    assert!(roll.cond(CONDITION_UPGRADE_IN_PROGRESS).is_some());
    assert_eq!(
        roll.admin_calls(),
        0,
        "starting needs no admin access (fail closed)"
    );
    assert_eq!(action, Action::requeue(REQUEUE_ROLL));
}

#[tokio::test]
async fn a_config_hash_change_is_gated_exactly_like_an_image_change() {
    // controlNodes growth changes only the config-hash annotation.
    let old = cluster(4, 3, OLD);
    let mut roll = Roll::new(cluster(4, 4, OLD));
    roll.api()
        .seed_statefulset_full(live_sts(&old, 0, "r1", "r1"));
    roll.api()
        .set_pods((0..4).map(|i| pod(i, "r1", true)).collect());
    roll.api()
        .seed_configmap(&desired::config_map_name(NAME), prior_configmap(&old.spec));
    roll.admin()
        .seed_control_voters((0..3).map(|i| format!("{NAME}-{i}")));

    roll.tick().await;

    assert_eq!(roll.partition(), 4, "a config-hash roll is gated too");
    assert_eq!(roll.upgrade().phase, UpgradePhase::InProgress);
    assert_eq!(roll.upgrade().from_image, roll.upgrade().to_image);
}

fn prior_configmap(spec: &AnimusClusterSpec) -> ConfigMap {
    let config = desired::cluster_config::build_cluster_config(NAME, NS, spec);
    let mut cm = ConfigMap::default();
    cm.metadata.name = Some(desired::config_map_name(NAME));
    cm.data = Some(BTreeMap::from([(
        desired::cluster_config::CONFIG_FILE_NAME.to_string(),
        desired::cluster_config::to_json(&config),
    )]));
    cm
}

#[tokio::test]
async fn steady_state_and_fresh_creation_apply_partition_zero_and_touch_no_admin_port() {
    // fresh: no StatefulSet at all
    let mut fresh = Roll::new(cluster(3, 3, OLD));
    fresh.tick().await;
    assert_eq!(fresh.partition(), 0);
    assert_eq!(fresh.admin_calls(), 0);
    assert!(fresh.cluster.status.as_ref().unwrap().upgrade.is_none());

    // steady: stored template == desired, nothing rolling
    let mut steady = Roll::new(cluster(3, 3, OLD));
    steady
        .api()
        .seed_statefulset_full(live_sts(&steady.cluster, 0, "r1", "r1"));
    steady.tick().await;
    assert_eq!(steady.partition(), 0);
    assert_eq!(steady.admin_calls(), 0);
}

#[tokio::test]
async fn a_statefulset_from_an_older_operator_is_adopted_not_rolled() {
    // No template-hash annotation and an empty template: nothing to compare,
    // so the first reconcile must not invent a roll.
    let mut roll = Roll::new(cluster(3, 3, OLD));
    roll.api().seed_statefulset(NAME, 3, 3);
    roll.tick().await;
    assert_eq!(roll.partition(), 0);
    assert!(roll.cluster.status.as_ref().unwrap().upgrade.is_none());
    assert_eq!(roll.admin_calls(), 0);
}

#[tokio::test]
async fn a_stale_statefulset_status_never_resets_the_partition() {
    // Right after our apply the controller has not observed the new
    // generation: revisions still look equal. Resetting to partition 0 here
    // would roll everything ungated.
    let mut roll = Roll::new(cluster(3, 3, NEW));
    let mut sts = live_sts(&roll.cluster, 3, "r1", "r1");
    sts.metadata.generation = Some(9);
    sts.status.as_mut().unwrap().observed_generation = Some(8);
    roll.api().seed_statefulset_full(sts);
    roll.api()
        .set_pods((0..3).map(|i| pod(i, "r1", true)).collect());
    roll.tick().await;
    assert_eq!(roll.partition(), 3);
    assert_eq!(roll.admin_calls(), 0);
}

// --- D7: the partition lowers one step per ok observation ---------------------

#[tokio::test]
async fn the_partition_lowers_one_step_per_ok_observation_down_to_zero() {
    let mut roll = Roll::new(cluster(3, 3, NEW));
    // started: nothing rolled yet; era on, active 1, node 0 leads
    roll.world(3, &[("r1", true), ("r1", true), ("r1", true)]);
    roll.healthy(true, 1, false, &[1, 1, 1], 0);
    roll.tick().await;
    assert_eq!(roll.partition(), 2, "gate open -> exactly one step");

    // pod 2 is being replaced: the gate holds
    roll.world(2, &[("r1", true), ("r1", true), ("r2", false)]);
    roll.tick().await;
    assert_eq!(roll.partition(), 2);
    assert!(
        roll.cond(CONDITION_UPGRADE_IN_PROGRESS)
            .unwrap()
            .contains("demo-2")
    );

    // pod 2 is Ready on r2 but has not reported the new range yet: still holds
    roll.world(2, &[("r1", true), ("r1", true), ("r2", true)]);
    roll.healthy(true, 1, false, &[1, 1, 1], 0);
    roll.tick().await;
    assert_eq!(
        roll.partition(),
        2,
        "a new pod that has not reported is not done"
    );

    // reported + healthy: next step
    roll.healthy(true, 1, false, &[1, 1, 2], 0);
    roll.tick().await;
    assert_eq!(roll.partition(), 1);
    assert_eq!(roll.upgrade().to_version, Some(2));
    assert_eq!(roll.upgrade().on_new, Some(1));

    roll.world(1, &[("r1", true), ("r2", true), ("r2", true)]);
    roll.healthy(true, 1, false, &[1, 2, 2], 0);
    roll.tick().await;
    // pod 1 done; ordinal 0 is the control leader: transfer first, partition holds
    assert_eq!(roll.partition(), 1);
}

#[tokio::test]
async fn the_gate_resumes_from_the_stored_partition_after_an_operator_restart() {
    // A brand new Context (no memory) over a roll that is mid-way: partition 1,
    // pods 1 and 2 on r2 and done. It must continue, not restart from the top.
    let mut roll = Roll::new(cluster(3, 3, NEW));
    roll.world(1, &[("r1", true), ("r2", true), ("r2", true)]);
    roll.healthy(true, 1, false, &[1, 2, 2], 2);
    roll.tick().await;
    assert_eq!(roll.partition(), 0, "resumed from live truth");
    assert!(roll.upgrade().on_new.unwrap() >= 2);
}

// --- D2/D9: blocked, fail closed -----------------------------------------------

#[tokio::test]
async fn every_d2_reason_holds_the_partition_and_is_named() {
    // roll-health not ok on one node
    let mut roll = Roll::new(cluster(3, 3, NEW));
    roll.world(3, &[("r1", true), ("r1", true), ("r1", true)]);
    roll.healthy(true, 1, false, &[1, 1, 1], 0);
    roll.admin().script_get(
        Some(1),
        "/admin/roll-health",
        Ok(json!({"ok": false, "reasons": [{"kind": "tablet_under_replicated", "tablet": 7}]})),
    );
    roll.tick().await;
    assert_eq!(roll.partition(), 3);
    assert_eq!(roll.upgrade().phase, UpgradePhase::Blocked);
    let msg = roll.cond(CONDITION_UPGRADE_BLOCKED).expect("blocked");
    assert!(msg.contains("tablet_under_replicated"), "{msg}");

    // a Down member
    let mut roll = Roll::new(cluster(3, 3, NEW));
    roll.world(3, &[("r1", true), ("r1", true), ("r1", true)]);
    roll.healthy(true, 1, false, &[1, 1, 1], 0);
    roll.admin().script_get(
        None,
        "/admin/cluster-version",
        Ok(cv_view(
            true,
            1,
            false,
            &[1, 1, 1],
            &["Active", "Down", "Active"],
        )),
    );
    roll.tick().await;
    assert_eq!(roll.partition(), 3);
    assert!(
        roll.cond(CONDITION_UPGRADE_BLOCKED)
            .unwrap()
            .contains("Down")
    );

    // a pod that is not Ready (and still old): unobservable, fail closed
    let mut roll = Roll::new(cluster(3, 3, NEW));
    roll.world(3, &[("r1", true), ("r1", false), ("r1", true)]);
    roll.healthy(true, 1, false, &[1, 1, 1], 0);
    roll.tick().await;
    assert_eq!(roll.partition(), 3);
    assert_eq!(roll.upgrade().phase, UpgradePhase::Blocked);
}

#[tokio::test]
async fn an_unreachable_admin_port_fails_closed_and_is_surfaced() {
    let mut roll = Roll::new(cluster(3, 3, NEW));
    roll.world(2, &[("r1", true), ("r1", true), ("r2", true)]);
    for path in [
        "/admin/cluster-version",
        "/admin/roll-health",
        "/admin/health",
    ] {
        roll.admin()
            .script_get(None, path, Err("connection refused".to_string()));
    }
    roll.tick().await;
    assert_eq!(
        roll.partition(),
        2,
        "nothing moves while the gate cannot be read"
    );
    assert_eq!(roll.upgrade().phase, UpgradePhase::Blocked);
    let msg = roll.cond(CONDITION_UPGRADE_BLOCKED).unwrap();
    assert!(msg.contains("failing closed"), "{msg}");
}

#[tokio::test]
async fn a_node_in_flight_past_the_stall_budget_is_blocked_not_acted_on() {
    let mut roll = Roll::new(cluster(3, 3, NEW));
    roll.world(2, &[("r1", true), ("r1", true), ("r2", false)]);
    roll.healthy(true, 1, false, &[1, 1, 1], 0);
    roll.tick().await;
    assert_eq!(roll.upgrade().in_flight_node.as_deref(), Some("demo-2"));
    assert_eq!(roll.upgrade().in_flight_since, Some(10_000));
    assert_eq!(roll.upgrade().phase, UpgradePhase::InProgress);
    roll.ctx.clock.set(10_000 + 899);
    roll.tick().await;
    assert_eq!(roll.upgrade().phase, UpgradePhase::InProgress);
    roll.ctx.clock.set(10_000 + 900);
    roll.tick().await;
    assert_eq!(roll.upgrade().phase, UpgradePhase::Blocked);
    assert!(
        roll.cond(CONDITION_UPGRADE_BLOCKED)
            .unwrap()
            .contains("demo-2")
    );
    assert_eq!(roll.partition(), 2);
}

#[tokio::test]
async fn the_first_roll_over_phase1_binaries_gates_on_status_and_awaits_the_era() {
    // No pod serves /admin/cluster-version (404) nor /admin/roll-health: the
    // observation comes from /admin/status and the era is inactive.
    let mut roll = Roll::new(cluster(3, 3, NEW));
    roll.world(3, &[("r1", true), ("r1", true), ("r1", true)]);
    let not_found = || Err("admin endpoint returned status 404: not found".to_string());
    let admin = roll.admin();
    admin.script_get(None, "/admin/cluster-version", not_found());
    admin.script_get(None, "/admin/roll-health", not_found());
    admin.script_get(
        None,
        "/admin/status",
        Ok(json!({"members": {
            "demo-0": {"status": "Active"},
            "demo-1": {"status": "Active"},
            "demo-2": {"status": "Active"},
        }})),
    );
    for i in 0..3 {
        admin.script_get(
            Some(i),
            "/admin/health",
            Ok(json!({"ok": true, "is_control_leader": i == 0})),
        );
    }
    roll.tick().await;
    assert_eq!(
        roll.partition(),
        2,
        "Phase 1 binaries roll under the platform gate"
    );

    // every pod on the new binary, era not started yet: no finalize, no
    // completion, just waiting for the era (never a finalize call)
    let mut roll = Roll::new(cluster(3, 3, NEW));
    roll.world(0, &[("r2", true), ("r2", true), ("r2", true)]);
    roll.healthy(false, 0, false, &[1, 1, 1], 0);
    roll.tick().await;
    assert_eq!(roll.upgrade().phase, UpgradePhase::InProgress);
    assert!(
        roll.cond(CONDITION_UPGRADE_IN_PROGRESS)
            .unwrap()
            .contains("era")
    );
    assert!(roll.posts("/admin/cluster-version/finalize").is_empty());
}

// --- control leader -----------------------------------------------------------

#[tokio::test]
async fn the_control_leader_gets_a_transfer_before_its_ordinal_is_admitted() {
    let mut roll = Roll::new(cluster(3, 3, NEW));
    // pod 2 is done; pod 1 is next and leads
    roll.world(2, &[("r1", true), ("r1", true), ("r2", true)]);
    roll.healthy(true, 1, false, &[1, 1, 2], 1);
    roll.tick().await;
    assert_eq!(
        roll.partition(),
        2,
        "the partition must not move before the transfer"
    );
    let transfers = roll.posts("/admin/control/transfer");
    assert_eq!(transfers.len(), 1, "{transfers:?}");
    let to = transfers[0]["to"].as_str().unwrap();
    assert_ne!(to, "demo-1");
    // it was sent to the leader's own admin port
    assert!(roll.admin().calls().iter().any(|(m, u)| m == "POST"
        && u.contains("demo-1.")
        && u.ends_with("/admin/control/transfer")));

    // leadership has moved: the very next reconcile lowers the partition
    roll.healthy(true, 1, false, &[1, 1, 2], 0);
    roll.tick().await;
    assert_eq!(roll.partition(), 1);
}

#[tokio::test]
async fn a_failed_transfer_holds_the_partition_and_is_surfaced() {
    let mut roll = Roll::new(cluster(3, 3, NEW));
    roll.world(2, &[("r1", true), ("r1", true), ("r2", true)]);
    roll.healthy(true, 1, false, &[1, 1, 2], 1);
    roll.admin().script_post_error(
        "/admin/control/transfer",
        "admin endpoint returned status 409: busy",
    );
    roll.tick().await;
    assert_eq!(roll.partition(), 2);
    assert_eq!(roll.upgrade().phase, UpgradePhase::Blocked);
    assert!(
        roll.cond(CONDITION_UPGRADE_BLOCKED)
            .unwrap()
            .contains("409")
    );
}

// --- finalize (D6) -------------------------------------------------------------

fn done_world(roll: &Roll, can_finalize: bool) {
    roll.world(0, &[("r2", true), ("r2", true), ("r2", true)]);
    roll.api().seed_statefulset_full({
        let mut s = live_sts(&roll.cluster, 0, "r2", "r2");
        s.status.as_mut().unwrap().current_revision = Some("r2".into());
        s
    });
    roll.healthy(true, 1, can_finalize, &[2, 2, 2], 0);
}

fn rolling_status() -> AnimusClusterStatus {
    AnimusClusterStatus {
        upgrade: Some(UpgradeStatus {
            phase: UpgradePhase::InProgress,
            from_version: Some(1),
            to_version: Some(2),
            on_new: Some(2),
            total: Some(3),
            active_cluster_version: Some(1),
            from_image: Some(OLD.into()),
            to_image: Some(NEW.into()),
            in_flight_node: None,
            in_flight_since: None,
            settled_since: None,
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn manual_finalize_only_offers_it_and_never_calls_the_finalize_endpoint() {
    let mut roll = Roll::new(cluster(3, 3, NEW));
    roll.cluster.status = Some(rolling_status());
    done_world(&roll, true);
    roll.tick().await;
    assert_eq!(roll.upgrade().phase, UpgradePhase::FinalizePending);
    let msg = roll.cond(CONDITION_UPGRADE_FINALIZE_PENDING).unwrap();
    assert!(msg.contains("animus cluster finalize"), "{msg}");
    assert!(roll.posts("/admin/cluster-version/finalize").is_empty());
}

#[tokio::test]
async fn auto_finalizes_only_with_can_finalize_and_only_after_the_soak() {
    let mut c = cluster(3, 3, NEW);
    c.spec.upgrade = Some(UpgradeSpec {
        finalize: Some(FinalizePolicy::Auto),
        soak_seconds: Some(60),
    });
    let mut roll = Roll::new(c);
    roll.cluster.status = Some(rolling_status());

    // can_finalize false: never, however long it soaks
    done_world(&roll, false);
    roll.tick().await;
    roll.ctx.clock.set(10_000 + 1_000);
    roll.tick().await;
    assert!(roll.posts("/admin/cluster-version/finalize").is_empty());
    assert_eq!(roll.upgrade().phase, UpgradePhase::Blocked);

    // can_finalize true: the soak clock starts, finalize waits for it
    done_world(&roll, true);
    roll.ctx.clock.set(20_000);
    roll.tick().await;
    assert!(roll.posts("/admin/cluster-version/finalize").is_empty());
    assert_eq!(roll.upgrade().settled_since, Some(20_000));
    roll.ctx.clock.set(20_059);
    roll.tick().await;
    assert!(roll.posts("/admin/cluster-version/finalize").is_empty());
    roll.ctx.clock.set(20_060);
    roll.tick().await;
    let posts = roll.posts("/admin/cluster-version/finalize");
    assert_eq!(posts, vec![json!({"to": 2, "expected": 1})]);
    assert_eq!(roll.upgrade().phase, UpgradePhase::Complete);
    assert!(roll.cond(CONDITION_ROLL_COMPLETE).unwrap().contains('2'));
    assert!(roll.cond(CONDITION_UPGRADE_FINALIZE_PENDING).is_none());
    // it went to the control leader (demo-0)
    assert!(roll.admin().calls().iter().any(|(m, u)| m == "POST"
        && u.contains("demo-0.")
        && u.ends_with("/admin/cluster-version/finalize")));
}

#[tokio::test]
async fn a_failed_finalize_is_retried_never_forced() {
    let mut c = cluster(3, 3, NEW);
    c.spec.upgrade = Some(UpgradeSpec {
        finalize: Some(FinalizePolicy::Auto),
        soak_seconds: None,
    });
    let mut roll = Roll::new(c);
    roll.cluster.status = Some(rolling_status());
    done_world(&roll, true);
    roll.admin().script_post_error(
        "/admin/cluster-version/finalize",
        "admin endpoint returned status 409: a member is Down",
    );
    roll.tick().await;
    assert_eq!(roll.upgrade().phase, UpgradePhase::FinalizePending);
    assert!(
        roll.cond(CONDITION_UPGRADE_FINALIZE_PENDING)
            .unwrap()
            .contains("never forced")
    );
    assert_eq!(roll.posts("/admin/cluster-version/finalize").len(), 1);
    // retried on the next reconcile
    roll.tick().await;
    assert_eq!(roll.posts("/admin/cluster-version/finalize").len(), 2);
}

#[tokio::test]
async fn a_config_only_roll_completes_without_a_finalize() {
    let mut roll = Roll::new(cluster(3, 3, NEW));
    roll.cluster.status = Some(rolling_status());
    roll.world(0, &[("r2", true), ("r2", true), ("r2", true)]);
    // nobody's own range exceeds `active`: goal == active
    let mut view = cv_view(true, 1, false, &[1, 1, 1], &[]);
    view["own_range"] = json!({"min": 1, "max": 1});
    roll.admin()
        .script_get(None, "/admin/cluster-version", Ok(view));
    roll.admin().script_get(
        None,
        "/admin/roll-health",
        Ok(json!({"ok": true, "reasons": []})),
    );
    roll.admin().script_get(
        Some(0),
        "/admin/health",
        Ok(json!({"is_control_leader": true})),
    );
    roll.tick().await;
    assert_eq!(roll.upgrade().phase, UpgradePhase::Complete);
    assert!(roll.posts("/admin/cluster-version/finalize").is_empty());
}

// --- D9: revert, D8: topology, maintainer decision 6: PDB ----------------------

#[tokio::test]
async fn a_revert_is_refused_once_a_pod_reported_the_new_range_and_free_before() {
    // a roll to NEW is in flight, pod 2 already runs it; the user sets OLD again
    let target = cluster(3, 3, NEW);
    let mut reverted = cluster(3, 3, OLD);
    reverted.status = Some(rolling_status());
    let mut roll = Roll::new(reverted);
    roll.api()
        .seed_statefulset_full(live_sts(&target, 2, "r2", "r1"));
    roll.api().set_pods(vec![
        pod(0, "r1", true),
        pod(1, "r1", true),
        pod(2, "r2", true),
    ]);
    roll.healthy(true, 1, false, &[1, 1, 2], 0);
    roll.tick().await;
    assert_eq!(
        roll.applied_image().as_deref(),
        Some(NEW),
        "the reconciler pins the roll's target image"
    );
    let held = roll.cond(CONDITION_UPGRADE_CHANGES_HELD).expect("held");
    assert!(held.contains("refusing to revert"), "{held}");

    // before any pod reported the new range (on_new 0) a revert is free: the
    // template simply goes back and a (trivial) roll is gated from the top
    let mut st = rolling_status();
    st.upgrade.as_mut().unwrap().on_new = Some(0);
    let mut free = cluster(3, 3, OLD);
    free.status = Some(st);
    let mut roll = Roll::new(free);
    roll.api()
        .seed_statefulset_full(live_sts(&target, 3, "r2", "r1"));
    roll.api()
        .set_pods((0..3).map(|i| pod(i, "r1", true)).collect());
    roll.tick().await;
    assert_eq!(roll.applied_image().as_deref(), Some(OLD));
    assert!(roll.cond(CONDITION_UPGRADE_CHANGES_HELD).is_none());
    assert_eq!(roll.partition(), 3);
}

#[tokio::test]
async fn topology_changes_are_held_until_the_roll_completes() {
    // roll in flight (partition 2); the user asks for 5 nodes and no scale-down
    // drain may run
    let running = cluster(3, 3, NEW);
    let mut wanted = cluster(5, 3, NEW);
    wanted.status = None;
    let mut roll = Roll::new(wanted);
    roll.api()
        .seed_statefulset_full(live_sts(&running, 2, "r2", "r1"));
    roll.api().set_pods(vec![
        pod(0, "r1", true),
        pod(1, "r1", true),
        pod(2, "r2", true),
    ]);
    roll.healthy(true, 1, false, &[1, 1, 2], 0);
    roll.tick().await;
    let sts = roll.api().statefulset(NAME).unwrap();
    assert_eq!(sts.spec.unwrap().replicas, Some(3), "scale held");
    let held = roll.cond(CONDITION_UPGRADE_CHANGES_HELD).expect("held");
    assert!(held.contains("spec.nodes"), "{held}");

    // and a scale-DOWN never drains mid-roll
    let mut shrink = Roll::new(cluster(2, 2, NEW));
    shrink
        .api()
        .seed_statefulset_full(live_sts(&running, 2, "r2", "r1"));
    shrink.api().set_pods(vec![
        pod(0, "r1", true),
        pod(1, "r1", true),
        pod(2, "r2", true),
    ]);
    shrink.healthy(true, 1, false, &[1, 1, 2], 0);
    shrink.tick().await;
    assert!(
        !shrink
            .admin()
            .calls()
            .iter()
            .any(|(_, u)| u.contains("/admin/drain")),
        "{:?}",
        shrink.admin().calls()
    );

    // controlNodes is held too (grow-only, but not mid-roll)
    let mut grow = Roll::new(cluster(4, 4, NEW));
    let running4 = cluster(4, 3, NEW);
    grow.api()
        .seed_statefulset_full(live_sts(&running4, 2, "r2", "r1"));
    grow.api().seed_configmap(
        &desired::config_map_name(NAME),
        prior_configmap(&running4.spec),
    );
    grow.admin()
        .seed_control_voters((0..3).map(|i| format!("{NAME}-{i}")));
    grow.api().set_pods(vec![
        pod(0, "r1", true),
        pod(1, "r1", true),
        pod(2, "r1", true),
        pod(3, "r2", true),
    ]);
    grow.healthy(true, 1, false, &[1, 1, 1, 2], 0);
    grow.tick().await;
    assert!(
        grow.cond(CONDITION_UPGRADE_CHANGES_HELD)
            .unwrap()
            .contains("spec.controlNodes")
    );
    // the regenerated config keeps the RUNNING controlNodes (3), not the wanted 4
    let cm = grow
        .api()
        .configmap(&desired::config_map_name(NAME))
        .unwrap();
    assert_eq!(
        cm.data.as_ref().unwrap()["cluster.json"],
        prior_configmap(&running4.spec).data.unwrap()["cluster.json"],
        "the config keeps the running controlNodes"
    );
}

#[tokio::test]
async fn pdb_zero_refuses_to_start_a_roll_and_touches_nothing() {
    // 3 nodes but ONE control node: PDB maxUnavailable 0
    let old = cluster(3, 1, OLD);
    let mut roll = Roll::new(cluster(3, 1, NEW));
    roll.api()
        .seed_statefulset_full(live_sts(&old, 0, "r1", "r1"));
    roll.api()
        .set_pods((0..3).map(|i| pod(i, "r1", true)).collect());
    roll.tick().await;
    assert_eq!(
        roll.partition(),
        3,
        "the template is staged but nothing rolls"
    );
    assert_eq!(roll.upgrade().phase, UpgradePhase::Blocked);
    let msg = roll.cond(CONDITION_UPGRADE_BLOCKED).unwrap();
    assert!(msg.contains("maxUnavailable 0"), "{msg}");

    // and it stays refused on every later reconcile, with no admin traffic
    roll.world(3, &[("r1", true), ("r1", true), ("r1", true)]);
    roll.healthy(true, 1, false, &[1, 1, 1], 0);
    let before = roll.admin_calls();
    roll.tick().await;
    assert_eq!(roll.partition(), 3);
    assert_eq!(roll.admin_calls(), before, "{:?}", roll.admin().calls());
    assert_eq!(roll.upgrade().phase, UpgradePhase::Blocked);
}

// --- fix forward -----------------------------------------------------------------

#[tokio::test]
async fn editing_the_image_again_mid_roll_regates_from_the_top() {
    // Roll to NEW is at partition 1 (pods 1, 2 on r2). A fixed image NEW2 is
    // applied: the partition goes back to replicas in the same apply, the
    // pre-roll image is remembered.
    let running = cluster(3, 3, NEW);
    let mut c = cluster(3, 3, "img:3");
    c.status = Some(rolling_status());
    let mut roll = Roll::new(c);
    roll.api()
        .seed_statefulset_full(live_sts(&running, 1, "r2", "r1"));
    roll.api().set_pods(vec![
        pod(0, "r1", true),
        pod(1, "r2", true),
        pod(2, "r2", true),
    ]);
    roll.tick().await;
    assert_eq!(roll.applied_image().as_deref(), Some("img:3"));
    assert_eq!(roll.partition(), 3);
    let up = roll.upgrade();
    assert_eq!(up.from_image.as_deref(), Some(OLD));
    assert_eq!(up.to_image.as_deref(), Some("img:3"));
}
