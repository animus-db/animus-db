//! The reconcile loop: a thin imperative shell over [`crate::desired`]'s
//! pure builders. All the interesting *shape* decisions live there and are
//! unit-tested without a cluster; this module's own job is just "build the
//! desired children, apply them, compute status, requeue" plus the two
//! stateful edges a pure function cannot express: the scale-down drain
//! sequence (talks to a real pod's admin port) and refusing an immutable
//! field change (reads the live object's own prior-applied state).
//!
//! **No finalizer in v1** — deletion relies entirely on Kubernetes garbage
//! collection following the `controller: true` owner references every
//! child carries (`crate::desired::owner_reference`). This is a deliberate
//! scope cut: nothing here needs pre-delete cleanup (an `AnimusCluster`
//! owns no external resource outside the Kubernetes API — no backup store,
//! no DNS record, nothing an orphaned finalizer could leak) and it keeps a
//! stuck-finalizer failure mode out of a v1 operator entirely.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use kube::{Api, Client, ResourceExt};
use serde_json::json;
use tracing::{error, info, warn};

use crate::admin_client::{AdminClient, AdminOps};
use crate::cluster_api::{ClusterApi, RealClusterApi};
use crate::crd::{
    AnimusCluster, AnimusClusterStatus, CONDITION_DRAIN_FAILED, CONDITION_IMMUTABLE_FIELD_CHANGED,
    CONDITION_SCALE_BELOW_CONTROL_NODES_REFUSED, CONDITION_TLS_SPEC_INVALID, ClusterCondition,
    ClusterPhase, ConditionStatus,
};
use crate::desired;

/// The field manager name every server-side-apply call uses
/// ([`crate::cluster_api::RealClusterApi`]'s own `PatchParams::apply`).
pub const FIELD_MANAGER: &str = "animus-operator";
/// Requeue interval after a clean reconcile.
const REQUEUE_OK: Duration = Duration::from_secs(30);
/// Requeue interval after a reconcile error (kube's `Controller` also
/// backs this off internally, but a fixed floor keeps a persistently
/// failing cluster from hot-looping the operator process).
const REQUEUE_ERR: Duration = Duration::from_secs(15);

/// Reconcile error type — every fallible step folds into this so the
/// `Controller`'s error hook can log it and set `Degraded` without a panic.
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("kube API error: {0}")]
    Kube(#[from] kube::Error),
    #[error("cluster object has no name")]
    MissingName,
    #[error("cluster object has no namespace")]
    MissingNamespace,
}

/// Shared context every reconcile call gets: the Kubernetes API seam and
/// the admin-port seam used for the scale-down drain sequence. Generic over
/// both so tests can substitute `crate::fakes::{FakeClusterApi,
/// FakeAdminClient}` for the real `kube`/HTTP implementors — see
/// `crate::cluster_api`/`crate::admin_client`'s own docs for why this is
/// the seam boundary.
pub struct Context<C: ClusterApi, A: AdminOps> {
    pub cluster_api: C,
    pub admin: A,
}

/// Apply every desired child for `cluster`, in a fixed order (`ConfigMap`
/// before `StatefulSet`, so a rolling pod never briefly reads a
/// `StatefulSet`-implied config that its `ConfigMap` doesn't have yet).
/// Every child is applied unconditionally on every call — there is no diff
/// against the previously-applied object, so a reconcile of an otherwise
/// unchanged cluster still re-applies all five children (an idempotent
/// re-apply, not a no-op; `crate::controller::tests` pins this).
async fn apply_children<C: ClusterApi>(
    cluster_api: &C,
    cluster: &AnimusCluster,
    ns: &str,
) -> Result<StatefulSet, ReconcileError> {
    let spec = &cluster.spec;

    let cm = desired::configmap::build(cluster, spec);
    cluster_api.apply_configmap(ns, &cm).await?;

    // ADR 0064 commit 3: a sixth child, applied only when `spec.tls.
    // certManager` is set (`build` returns `None` for the `secretName`
    // shape and for no TLS at all) — before the `StatefulSet` so the
    // `Secret` it names has a chance to exist by the time a pod starts.
    if let Some(cert) = desired::certificate::build(cluster, spec) {
        cluster_api.apply_certificate(ns, &cert).await?;
    }

    let internal_svc = desired::services::build_internal(cluster, spec);
    cluster_api.apply_service(ns, &internal_svc).await?;

    let client_svc = desired::services::build_client(cluster, spec);
    cluster_api.apply_service(ns, &client_svc).await?;

    let netpol = desired::networkpolicy::build(cluster, spec);
    cluster_api.apply_networkpolicy(ns, &netpol).await?;

    let sts = desired::statefulset::build(cluster, spec);
    let applied = cluster_api.apply_statefulset(ns, &sts).await?;

    Ok(applied)
}

/// Whether `spec.controlNodes` (resolved against its own default) differs
/// from the value the *previous* reconcile actually applied, as recorded on
/// `status.conditions`. With no admission webhook in v1, the controller is
/// the only thing that can catch this — so it is caught here, every
/// reconcile, by comparing against the live `StatefulSet`'s replica count
/// is not enough (that only tells us `nodes`, not `controlNodes`); instead
/// this compares against a dedicated status annotation-free signal: the
/// `ConfigMap`'s own already-applied config, which is cheap to read back
/// (server-side apply already wrote it) and is the actual source of truth
/// for which ordinals were minted `Both` vs `Data` last time.
async fn control_nodes_changed<C: ClusterApi>(
    cluster_api: &C,
    ns: &str,
    cluster: &AnimusCluster,
) -> Result<Option<i32>, ReconcileError> {
    let name = cluster.name_any();
    let cm_name = desired::config_map_name(&name);
    let existing = cluster_api.get_configmap(ns, &cm_name).await?;
    let Some(existing) = existing else {
        return Ok(None);
    };
    let Some(data) = &existing.data else {
        return Ok(None);
    };
    let Some(json) = data.get(desired::cluster_config::CONFIG_FILE_NAME) else {
        return Ok(None);
    };
    let Ok(parsed) = serde_json::from_str::<desired::cluster_config::ClusterConfig>(json) else {
        return Ok(None);
    };
    let previous_control_nodes = parsed
        .nodes
        .iter()
        .take_while(|n| matches!(n.role, desired::cluster_config::NodeRole::Both))
        .count() as i32;
    let desired_control_nodes = cluster.spec.control_nodes_or_default();
    if previous_control_nodes != desired_control_nodes && !parsed.nodes.is_empty() {
        Ok(Some(previous_control_nodes))
    } else {
        Ok(None)
    }
}

/// The admin base URL for pod ordinal `ordinal` of cluster `name` in
/// namespace `ns` — the headless internal `Service`'s own per-pod DNS name.
/// `tls`: whether the admin port speaks TLS (ADR 0064 commit 3, server-only
/// — `animusd` serves `admin` that way whenever `spec.tls` is set), which
/// selects the URL scheme; the caller must pass the matching CA bytes to
/// `AdminOps::get_json`/`post_json` in that case (see [`drain_and_remove_node`]).
fn admin_base_url(name: &str, ns: &str, ordinal: i32, admin_port: i32, tls: bool) -> String {
    let scheme = if tls { "https" } else { "http" };
    format!(
        "{scheme}://{}:{admin_port}",
        desired::pod_fqdn(name, ns, ordinal)
    )
}

/// Drain and remove one pod ordinal before it is scaled away, via the
/// sequence `crate::CLAUDE.md`/the delivery brief document: `POST
/// /admin/drain {node}`, poll `GET /admin/member/drain-status?node=` to
/// completion, then `POST /admin/member/remove {node}`. `tls_ca` (ADR 0064
/// commit 3): `Some(pem)` dials the admin port over TLS trusting `pem` as
/// the cluster CA; `None` plain TCP — see `reconcile`'s own call site for
/// where this is read out of `spec.tls`'s resolved `Secret`.
async fn drain_and_remove_node<A: AdminOps>(
    admin: &A,
    name: &str,
    ns: &str,
    ordinal: i32,
    admin_port: i32,
    tls_ca: Option<&[u8]>,
) -> Result<(), String> {
    let node_id = desired::cluster_config::node_id(name, ordinal);
    let base = admin_base_url(name, ns, ordinal, admin_port, tls_ca.is_some());

    admin
        .post_json(
            &format!("{base}/admin/drain"),
            &json!({ "node": node_id }),
            tls_ca,
        )
        .await
        .map_err(|e| format!("draining {node_id}: {e}"))?;

    const MAX_POLLS: u32 = 120;
    const POLL_INTERVAL: Duration = Duration::from_secs(5);
    for attempt in 0..MAX_POLLS {
        let status: serde_json::Value = admin
            .get_json(
                &format!("{base}/admin/member/drain-status?node={node_id}"),
                tls_ca,
            )
            .await
            .map_err(|e| format!("polling drain-status for {node_id}: {e}"))?;
        let tablets_remaining = status["tablets_remaining"].as_u64().unwrap_or(u64::MAX);
        let member_status = status["status"].as_str().unwrap_or("");
        if tablets_remaining == 0 && member_status != "Active" {
            break;
        }
        if attempt + 1 == MAX_POLLS {
            return Err(format!(
                "{node_id} did not finish draining after {MAX_POLLS} polls \
                 ({tablets_remaining} tablets remaining, status {member_status:?})"
            ));
        }
        // ADR 0003 / ADR 0061 Decision 4 (rung B5): this reconcile loop polls a
        // real Kubernetes pod's admin port over a real network, outside the
        // Env seam (kube-rs, no SimEnv counterpart) — a real wall-clock wait
        // is the correct tool here, not a determinism hole.
        #[allow(
            clippy::disallowed_methods,
            reason = "animus-operator polls a real pod's admin port outside the Env seam, not system logic (ADR 0003); see ADR 0061 Decision 4"
        )]
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    admin
        .post_json(
            &format!("{base}/admin/member/remove"),
            &json!({ "node": node_id }),
            tls_ca,
        )
        .await
        .map_err(|e| format!("removing {node_id}: {e}"))?;
    Ok(())
}

/// Set (replacing any existing entry of the same `type`) one condition on
/// `status`.
fn set_condition(status: &mut AnimusClusterStatus, type_: &str, message: String) {
    status.conditions.retain(|c| c.type_ != type_);
    status.conditions.push(ClusterCondition {
        type_: type_.to_string(),
        status: ConditionStatus::True,
        reason: Some(type_.to_string()),
        message: Some(message),
        last_transition_time: None,
    });
}

async fn reconcile<C: ClusterApi, A: AdminOps>(
    cluster: Arc<AnimusCluster>,
    ctx: Arc<Context<C, A>>,
) -> Result<Action, ReconcileError> {
    let name = cluster.name_any();
    let ns = cluster
        .namespace()
        .ok_or(ReconcileError::MissingNamespace)?;
    info!(cluster = %name, namespace = %ns, "reconciling AnimusCluster");

    let mut status = cluster.status.clone().unwrap_or_default();
    status.observed_generation = cluster.metadata.generation;

    // Validate `spec.tls` (ADR 0064 commit 3): no admission webhook in v1
    // to reject the write itself, so — same posture as `controlNodes`'
    // immutability check above — this is the one place that can catch a
    // spec setting both or neither of `secretName`/`certManager`. Set a
    // condition and reconcile the rest of the spec with TLS stripped
    // (every other field — image, resources, scale — still deserves to
    // converge) rather than getting stuck entirely on one bad field; the
    // next reconcile (30s later, or sooner on a spec edit) retries the
    // validation once the spec is fixed.
    if let Some(tls) = &cluster.spec.tls
        && let Err(e) = tls.validate()
    {
        warn!(cluster = %name, error = %e, "refusing invalid spec.tls");
        set_condition(&mut status, CONDITION_TLS_SPEC_INVALID, e);
        let mut pinned = (*cluster).clone();
        pinned.spec.tls = None;
        return finish_reconcile(&pinned, &ctx, &ns, status).await;
    }
    status
        .conditions
        .retain(|c| c.type_ != CONDITION_TLS_SPEC_INVALID);

    // Refuse an immutable `controlNodes` change: set a condition, keep
    // going (the rest of the spec — image, resources, scale — still
    // deserves to converge), but never regenerate the config with the new
    // value.
    if let Some(prior) = control_nodes_changed(&ctx.cluster_api, &ns, &cluster).await? {
        warn!(
            cluster = %name,
            prior_control_nodes = prior,
            requested_control_nodes = cluster.spec.control_nodes_or_default(),
            "refusing controlNodes change (immutable field)"
        );
        set_condition(
            &mut status,
            CONDITION_IMMUTABLE_FIELD_CHANGED,
            format!(
                "spec.controlNodes changed from {prior} to {} — ignored; \
                 controlNodes is immutable after creation",
                cluster.spec.control_nodes_or_default()
            ),
        );
        // Reconcile with the *prior* control-node count so the running
        // cluster's own role split never actually changes underneath it.
        let mut pinned = (*cluster).clone();
        pinned.spec.control_nodes = Some(prior);
        return finish_reconcile(&pinned, &ctx, &ns, status).await;
    }

    // Refuse scaling below `controlNodes` — every control-role pod must
    // stay present (the control-plane Raft group needs its full voter
    // set); a data-only pod may always be removed.
    let control_nodes = cluster.spec.control_nodes_or_default();
    if cluster.spec.nodes < control_nodes {
        warn!(
            cluster = %name,
            nodes = cluster.spec.nodes,
            control_nodes,
            "refusing scale below controlNodes"
        );
        set_condition(
            &mut status,
            CONDITION_SCALE_BELOW_CONTROL_NODES_REFUSED,
            format!(
                "spec.nodes ({}) is below spec.controlNodes ({control_nodes}) — ignored",
                cluster.spec.nodes
            ),
        );
        return finish_reconcile(&cluster, &ctx, &ns, status).await;
    }

    // Scale-down: drain+remove every pod ordinal being dropped, highest
    // first, before the `StatefulSet`'s own replica count goes down.
    if let Some(existing) = ctx.cluster_api.get_statefulset(&ns, &name).await? {
        let current_replicas = existing.spec.and_then(|s| s.replicas).unwrap_or(0);
        let target_replicas = cluster.spec.nodes;
        if target_replicas < current_replicas {
            let admin_port =
                cluster.spec.base_port_or_default() + desired::cluster_config::PORT_ADMIN;
            // ADR 0064 commit 3: the admin port speaks server-only TLS
            // whenever `spec.tls` is set (`animusd` serves it that way —
            // see `crd::TlsSpec`'s own doc); read the cluster CA out of
            // the resolved `Secret` once, up front, for every drain call
            // below. `None` here when `spec.tls` names a `Secret`
            // cert-manager hasn't finished issuing yet — the drain call
            // then fails against a TLS-only admin port (a real, surfaced
            // `DrainFailed` condition), retried on the next reconcile once
            // the `Secret` exists, rather than silently dialing plaintext
            // into a TLS listener.
            let tls_ca: Option<Vec<u8>> = match &cluster.spec.tls {
                Some(tls) => {
                    let secret_name = tls.secret_name_or_default(&name);
                    ctx.cluster_api
                        .get_secret(&ns, &secret_name)
                        .await?
                        .and_then(|s| s.data)
                        .and_then(|d| d.get("ca.crt").cloned())
                        .map(|b| b.0)
                }
                None => None,
            };
            for ordinal in (target_replicas..current_replicas).rev() {
                if let Err(e) = drain_and_remove_node(
                    &ctx.admin,
                    &name,
                    &ns,
                    ordinal,
                    admin_port,
                    tls_ca.as_deref(),
                )
                .await
                {
                    error!(cluster = %name, ordinal, error = %e, "scale-down drain failed");
                    set_condition(
                        &mut status,
                        CONDITION_DRAIN_FAILED,
                        format!("draining pod ordinal {ordinal} before scale-down: {e}"),
                    );
                    // Stop the drain sequence here — don't scale the
                    // StatefulSet down past a pod that never finished
                    // draining, and don't attempt a lower ordinal either
                    // (they must go highest-first).
                    return finish_reconcile(&cluster, &ctx, &ns, status).await;
                }
            }
            status
                .conditions
                .retain(|c| c.type_ != CONDITION_DRAIN_FAILED);
        }
    }

    finish_reconcile(&cluster, &ctx, &ns, status).await
}

async fn finish_reconcile<C: ClusterApi, A: AdminOps>(
    cluster: &AnimusCluster,
    ctx: &Context<C, A>,
    ns: &str,
    mut status: AnimusClusterStatus,
) -> Result<Action, ReconcileError> {
    let name = cluster.name_any();
    let applied_sts = apply_children(&ctx.cluster_api, cluster, ns).await?;

    let desired_replicas = cluster.spec.nodes;
    let ready = applied_sts
        .status
        .as_ref()
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0);
    status.ready_nodes = Some(ready);

    let has_blocking_condition = status.conditions.iter().any(|c| {
        c.type_ == CONDITION_DRAIN_FAILED
            || c.type_ == CONDITION_IMMUTABLE_FIELD_CHANGED
            || c.type_ == CONDITION_SCALE_BELOW_CONTROL_NODES_REFUSED
    });
    status.phase = Some(if has_blocking_condition && ready < desired_replicas {
        ClusterPhase::Degraded
    } else if ready >= desired_replicas {
        ClusterPhase::Ready
    } else if ready > 0 {
        ClusterPhase::Bootstrapping
    } else {
        ClusterPhase::Pending
    });

    ctx.cluster_api
        .patch_cluster_status(ns, &name, &status)
        .await?;

    Ok(Action::requeue(REQUEUE_OK))
}

fn error_policy<C: ClusterApi, A: AdminOps>(
    cluster: Arc<AnimusCluster>,
    err: &ReconcileError,
    _ctx: Arc<Context<C, A>>,
) -> Action {
    error!(
        cluster = %cluster.name_any(),
        error = %err,
        "reconcile failed"
    );
    Action::requeue(REQUEUE_ERR)
}

/// Run the controller loop against `client` forever (until the process is
/// asked to stop). Watches `AnimusCluster` plus its four owned child kinds
/// so an out-of-band edit to a child (e.g. `kubectl edit statefulset`)
/// triggers a reconcile that reverts the drift, not just a spec change on
/// the parent.
pub async fn run(client: Client) {
    let clusters = Api::<AnimusCluster>::all(client.clone());
    let ctx = Arc::new(Context {
        cluster_api: RealClusterApi::new(client.clone()),
        admin: AdminClient::new(),
    });

    Controller::new(clusters, watcher::Config::default())
        .owns(
            Api::<StatefulSet>::all(client.clone()),
            watcher::Config::default(),
        )
        .owns(
            Api::<ConfigMap>::all(client.clone()),
            watcher::Config::default(),
        )
        .owns(
            Api::<Service>::all(client.clone()),
            watcher::Config::default(),
        )
        .owns(
            Api::<NetworkPolicy>::all(client),
            watcher::Config::default(),
        )
        .run(
            reconcile::<RealClusterApi, AdminClient>,
            error_policy::<RealClusterApi, AdminClient>,
            ctx,
        )
        .for_each(|res| async move {
            match res {
                Ok((obj, action)) => {
                    tracing::debug!(?obj, ?action, "reconciled");
                }
                Err(e) => {
                    tracing::warn!(error = %e, "reconcile stream error");
                }
            }
        })
        .await;
}

/// ADR 0061 rung E1: `reconcile`/`control_nodes_changed`/
/// `drain_and_remove_node` exercised via `crate::fakes::{FakeClusterApi,
/// FakeAdminClient}` — no live API server, no real socket. See that
/// module's doc and `crates/animus-operator/CLAUDE.md`'s testing section
/// for what this harness does and does not prove.
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use k8s_openapi::api::core::v1::ConfigMap;

    use super::*;
    use crate::crd::{AnimusClusterSpec, CertManagerSpec, IssuerRef, TlsSpec};
    use crate::desired::test_support::test_cluster;
    use crate::fakes::{AppliedKind, FakeAdminClient, FakeClusterApi};

    fn make_ctx(
        cluster_api: FakeClusterApi,
        admin: FakeAdminClient,
    ) -> Arc<Context<FakeClusterApi, FakeAdminClient>> {
        Arc::new(Context { cluster_api, admin })
    }

    /// The exact URL `admin_base_url` + a path suffix builds — used to
    /// assert on `FakeAdminClient::calls()` without duplicating
    /// `drain_and_remove_node`'s own URL-building logic.
    fn admin_url(name: &str, ns: &str, ordinal: i32, admin_port: i32, path: &str) -> String {
        format!(
            "{}{path}",
            admin_base_url(name, ns, ordinal, admin_port, false)
        )
    }

    /// A `ConfigMap` shaped exactly like the one a previous reconcile would
    /// have applied for `spec`, for seeding `FakeClusterApi::seed_configmap`
    /// in the `control_nodes_changed`/immutable-field tests.
    fn prior_cluster_configmap(name: &str, ns: &str, spec: &AnimusClusterSpec) -> ConfigMap {
        let config = desired::cluster_config::build_cluster_config(name, ns, spec);
        let mut cm = ConfigMap::default();
        cm.metadata.name = Some(desired::config_map_name(name));
        cm.data = Some(BTreeMap::from([(
            desired::cluster_config::CONFIG_FILE_NAME.to_string(),
            desired::cluster_config::to_json(&config),
        )]));
        cm
    }

    // --- (1) a fresh cluster reconcile creates the expected children -----

    #[tokio::test]
    async fn reconcile_fresh_cluster_applies_all_five_children_in_order() {
        let cluster = Arc::new(test_cluster("demo", "ns1", 3, None));
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        let result = reconcile(Arc::clone(&cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        assert_eq!(
            ctx.cluster_api.applies(),
            vec![
                (AppliedKind::ConfigMap, desired::config_map_name("demo")),
                (AppliedKind::Service, desired::internal_service_name("demo")),
                (AppliedKind::Service, desired::client_service_name("demo")),
                (
                    AppliedKind::NetworkPolicy,
                    desired::network_policy_name("demo")
                ),
                (AppliedKind::StatefulSet, "demo".to_string()),
            ]
        );

        // No `StatefulSet` status was ever reported ready (nothing seeded),
        // so a fresh cluster's own first reconcile lands in `Pending`.
        let status = ctx.cluster_api.last_status().expect("status was patched");
        assert_eq!(status.phase, Some(ClusterPhase::Pending));
        assert_eq!(status.ready_nodes, Some(0));
    }

    // --- (2) a reconcile of an unchanged cluster: pin the actual behavior -

    #[tokio::test]
    async fn reconcile_of_unchanged_cluster_reapplies_every_child_again() {
        // `apply_children` never diffs against what's already applied —
        // every reconcile unconditionally re-applies all five children, an
        // idempotent re-apply rather than a no-op. This test pins that
        // choice so a future change to the behavior is a deliberate,
        // visible diff here, not a silent regression.
        let cluster = Arc::new(test_cluster("demo", "ns1", 3, None));
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        reconcile(Arc::clone(&cluster), Arc::clone(&ctx))
            .await
            .unwrap();
        let first = ctx.cluster_api.applies();
        assert_eq!(first.len(), 5);

        reconcile(Arc::clone(&cluster), Arc::clone(&ctx))
            .await
            .unwrap();
        let second = ctx.cluster_api.applies();
        assert_eq!(second.len(), 10);
        assert_eq!(&second[..5], &first[..]);
        assert_eq!(&second[5..], &first[..]);
    }

    // --- (3) control_nodes_changed detects a change vs no change ---------

    #[tokio::test]
    async fn control_nodes_changed_detects_a_real_change() {
        let fake = FakeClusterApi::new();
        let prior_spec = AnimusClusterSpec {
            nodes: 5,
            control_nodes: Some(3),
            ..Default::default()
        };
        fake.seed_configmap(
            &desired::config_map_name("demo"),
            prior_cluster_configmap("demo", "ns1", &prior_spec),
        );

        let cluster = test_cluster("demo", "ns1", 5, Some(5));
        let result = control_nodes_changed(&fake, "ns1", &cluster).await.unwrap();
        assert_eq!(result, Some(3));
    }

    #[tokio::test]
    async fn control_nodes_changed_is_none_when_unchanged() {
        let fake = FakeClusterApi::new();
        let prior_spec = AnimusClusterSpec {
            nodes: 5,
            control_nodes: Some(3),
            ..Default::default()
        };
        fake.seed_configmap(
            &desired::config_map_name("demo"),
            prior_cluster_configmap("demo", "ns1", &prior_spec),
        );

        let cluster = test_cluster("demo", "ns1", 5, Some(3));
        let result = control_nodes_changed(&fake, "ns1", &cluster).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn control_nodes_changed_is_none_when_no_prior_configmap() {
        // A fresh cluster (nothing applied yet): nothing to compare
        // against, so this must never look like an immutable-field change.
        let fake = FakeClusterApi::new();
        let cluster = test_cluster("demo", "ns1", 3, None);
        let result = control_nodes_changed(&fake, "ns1", &cluster).await.unwrap();
        assert_eq!(result, None);
    }

    // --- (4) drain_and_remove_node's sequence, including the bounded ------
    // --- never-completes failure path -------------------------------------

    #[tokio::test]
    async fn drain_and_remove_node_succeeds_when_drain_completes_immediately() {
        // No response queued: FakeAdminClient's default GET response is
        // "already fully drained", so the sequence completes in one poll.
        let admin = FakeAdminClient::new();
        let result = drain_and_remove_node(&admin, "demo", "ns1", 2, 14003, None).await;
        assert!(result.is_ok(), "{result:?}");

        assert_eq!(
            admin.calls(),
            vec![
                (
                    "POST".to_string(),
                    admin_url("demo", "ns1", 2, 14003, "/admin/drain")
                ),
                (
                    "GET".to_string(),
                    admin_url(
                        "demo",
                        "ns1",
                        2,
                        14003,
                        &format!("/admin/member/drain-status?node={}", node_id_for_test())
                    )
                ),
                (
                    "POST".to_string(),
                    admin_url("demo", "ns1", 2, 14003, "/admin/member/remove")
                ),
            ]
        );
    }

    /// `node_id("demo", 2)` — a tiny local mirror since `node_id` is
    /// private to `desired::cluster_config`.
    fn node_id_for_test() -> String {
        desired::cluster_config::node_id("demo", 2)
    }

    #[tokio::test(start_paused = true)]
    async fn drain_and_remove_node_is_bounded_when_drain_never_completes() {
        let admin = FakeAdminClient::new();
        // Exactly one queued response: it repeats forever (see
        // `FakeAdminClient`'s own doc), so the drain never satisfies
        // `drain_and_remove_node`'s completion check.
        admin.queue_drain_status(3, "Draining");

        let result = drain_and_remove_node(&admin, "demo", "ns1", 4, 14003, None).await;
        let err = result.expect_err("a drain that never completes must eventually give up");
        assert!(
            err.contains("did not finish draining after 120 polls"),
            "{err}"
        );

        let calls = admin.calls();
        // 1 drain POST + 120 status GETs, never reaching remove — bounded,
        // not a spin loop.
        assert_eq!(calls.len(), 1 + 120);
        assert!(calls.iter().all(|(_, url)| !url.contains("/member/remove")));
    }

    // --- reconcile-level scale-down sequencing, both the happy path and ---
    // --- the stop-on-first-failure path ------------------------------------

    #[tokio::test]
    async fn reconcile_scale_down_drains_removed_ordinals_highest_first() {
        let fake_cluster = FakeClusterApi::new();
        // A previous reconcile already scaled this cluster to 5 replicas.
        fake_cluster.seed_statefulset("demo", 5, 5);
        let ctx = make_ctx(fake_cluster, FakeAdminClient::new());

        // Target: 3 nodes — ordinals 3 and 4 must be drained+removed,
        // highest first, before anything else.
        let cluster = Arc::new(test_cluster("demo", "ns1", 3, None));
        let result = reconcile(Arc::clone(&cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let drain_posts: Vec<String> = ctx
            .admin
            .calls()
            .into_iter()
            .filter(|(m, u)| m == "POST" && u.ends_with("/admin/drain"))
            .map(|(_, u)| u)
            .collect();
        assert_eq!(
            drain_posts,
            vec![
                admin_url("demo", "ns1", 4, 14003, "/admin/drain"),
                admin_url("demo", "ns1", 3, 14003, "/admin/drain"),
            ]
        );
        let remove_posts: Vec<String> = ctx
            .admin
            .calls()
            .into_iter()
            .filter(|(m, u)| m == "POST" && u.ends_with("/admin/member/remove"))
            .map(|(_, u)| u)
            .collect();
        assert_eq!(
            remove_posts,
            vec![
                admin_url("demo", "ns1", 4, 14003, "/admin/member/remove"),
                admin_url("demo", "ns1", 3, 14003, "/admin/member/remove"),
            ]
        );

        // No blocking condition: the drain sequence succeeded.
        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            !status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_DRAIN_FAILED)
        );
    }

    #[tokio::test]
    async fn reconcile_scale_down_stops_on_first_drain_failure() {
        let fake_cluster = FakeClusterApi::new();
        fake_cluster.seed_statefulset("demo", 5, 5);
        let fake_admin = FakeAdminClient::new();
        fake_admin.fail_drain();
        let ctx = make_ctx(fake_cluster, fake_admin);

        let cluster = Arc::new(test_cluster("demo", "ns1", 3, None));
        let result = reconcile(Arc::clone(&cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_DRAIN_FAILED)
        );

        // Only the highest ordinal (4) was ever attempted — the sequence
        // must stop there, never touching ordinal 3, and never reaching
        // "remove" for anything.
        let calls = ctx.admin.calls();
        let drain_posts: Vec<&String> = calls
            .iter()
            .filter(|(m, u)| m == "POST" && u.ends_with("/admin/drain"))
            .map(|(_, u)| u)
            .collect();
        assert_eq!(
            drain_posts,
            vec![&admin_url("demo", "ns1", 4, 14003, "/admin/drain")]
        );
        assert!(!calls.iter().any(|(_, u)| u.contains("/member/remove")));
    }

    // --- bonus: the immutable-controlNodes-change path end to end ---------

    #[tokio::test]
    async fn reconcile_refuses_immutable_control_nodes_change() {
        let fake_cluster = FakeClusterApi::new();
        let prior_spec = AnimusClusterSpec {
            nodes: 5,
            control_nodes: Some(3),
            ..Default::default()
        };
        fake_cluster.seed_configmap(
            &desired::config_map_name("demo"),
            prior_cluster_configmap("demo", "ns1", &prior_spec),
        );
        let ctx = make_ctx(fake_cluster, FakeAdminClient::new());

        // The spec now asks for controlNodes: 5 — refused, since it
        // previously applied as 3.
        let cluster = Arc::new(test_cluster("demo", "ns1", 5, Some(5)));
        let result = reconcile(Arc::clone(&cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_IMMUTABLE_FIELD_CHANGED)
        );

        // The re-applied ConfigMap must still reflect the *prior*
        // controlNodes value (3 "both" roles), never the refused new one.
        let cm = ctx
            .cluster_api
            .configmap(&desired::config_map_name("demo"))
            .expect("ConfigMap re-applied");
        let json = cm
            .data
            .as_ref()
            .unwrap()
            .get(desired::cluster_config::CONFIG_FILE_NAME)
            .unwrap();
        let parsed: desired::cluster_config::ClusterConfig = serde_json::from_str(json).unwrap();
        let both_count = parsed
            .nodes
            .iter()
            .filter(|n| matches!(n.role, desired::cluster_config::NodeRole::Both))
            .count();
        assert_eq!(both_count, 3);
    }

    // --- (6) spec.tls (ADR 0064 commit 3) --------------------------------

    fn cert_manager_tls() -> TlsSpec {
        TlsSpec {
            secret_name: None,
            cert_manager: Some(CertManagerSpec {
                issuer_ref: IssuerRef {
                    name: "i".to_string(),
                    kind: "ClusterIssuer".to_string(),
                    group: None,
                },
                duration: None,
                renew_before: None,
            }),
        }
    }

    #[tokio::test]
    async fn reconcile_applies_a_certificate_as_a_sixth_child_when_cert_manager_configured() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.tls = Some(cert_manager_tls());
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        let result = reconcile(Arc::new(cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let applies = ctx.cluster_api.applies();
        assert_eq!(applies.len(), 6, "{applies:?}");
        assert_eq!(
            applies[1],
            (AppliedKind::Certificate, "demo-tls".to_string())
        );
    }

    #[tokio::test]
    async fn reconcile_applies_no_certificate_for_the_secret_name_shape() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.tls = Some(TlsSpec {
            secret_name: Some("preexisting".to_string()),
            cert_manager: None,
        });
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        reconcile(Arc::new(cluster), Arc::clone(&ctx))
            .await
            .unwrap();

        let applies = ctx.cluster_api.applies();
        assert_eq!(applies.len(), 5, "{applies:?}");
        assert!(!applies.iter().any(|(k, _)| *k == AppliedKind::Certificate));
    }

    #[tokio::test]
    async fn reconcile_rejects_tls_spec_with_both_shapes_set() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.tls = Some(TlsSpec {
            secret_name: Some("preexisting".to_string()),
            cert_manager: Some(match cert_manager_tls().cert_manager {
                Some(cm) => cm,
                None => unreachable!(),
            }),
        });
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        let result = reconcile(Arc::new(cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_TLS_SPEC_INVALID),
            "{:?}",
            status.conditions
        );
        // Reconciled as if TLS were unset: no Certificate applied.
        assert!(
            !ctx.cluster_api
                .applies()
                .iter()
                .any(|(k, _)| *k == AppliedKind::Certificate)
        );
    }

    #[tokio::test]
    async fn reconcile_rejects_tls_spec_with_neither_shape_set() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.tls = Some(TlsSpec::default());
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        let result = reconcile(Arc::new(cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_TLS_SPEC_INVALID)
        );
    }

    #[tokio::test]
    async fn drain_and_remove_node_over_tls_dials_https_and_forwards_the_ca() {
        let admin = FakeAdminClient::new();
        let ca = b"fake-ca-pem";
        let result = drain_and_remove_node(&admin, "demo", "ns1", 2, 14003, Some(ca)).await;
        assert!(result.is_ok(), "{result:?}");
        let calls = admin.calls();
        assert!(
            calls.iter().all(|(_, url)| url.starts_with("https://")),
            "{calls:?}"
        );
    }

    #[tokio::test]
    async fn reconcile_scale_down_over_tls_reads_the_ca_from_the_resolved_secret() {
        use k8s_openapi::ByteString;
        use k8s_openapi::api::core::v1::Secret;

        let fake_cluster = FakeClusterApi::new();
        fake_cluster.seed_statefulset("demo", 5, 5);
        fake_cluster.seed_secret(
            "my-tls",
            Secret {
                data: Some(BTreeMap::from([(
                    "ca.crt".to_string(),
                    ByteString(b"fake-ca-pem".to_vec()),
                )])),
                ..Default::default()
            },
        );
        let ctx = make_ctx(fake_cluster, FakeAdminClient::new());

        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.tls = Some(TlsSpec {
            secret_name: Some("my-tls".to_string()),
            cert_manager: None,
        });
        let result = reconcile(Arc::new(cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let drain_posts: Vec<String> = ctx
            .admin
            .calls()
            .into_iter()
            .filter(|(m, u)| m == "POST" && u.ends_with("/admin/drain"))
            .map(|(_, u)| u)
            .collect();
        assert_eq!(
            drain_posts,
            vec![
                admin_url("demo", "ns1", 4, 14003, "/admin/drain")
                    .replacen("http://", "https://", 1),
                admin_url("demo", "ns1", 3, 14003, "/admin/drain")
                    .replacen("http://", "https://", 1),
            ]
        );
    }
}
