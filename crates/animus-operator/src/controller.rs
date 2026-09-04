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
    CONDITION_SCALE_BELOW_CONTROL_NODES_REFUSED, ClusterCondition, ClusterPhase, ConditionStatus,
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
fn admin_base_url(name: &str, ns: &str, ordinal: i32, admin_port: i32) -> String {
    format!(
        "http://{}:{admin_port}",
        desired::pod_fqdn(name, ns, ordinal)
    )
}

/// Drain and remove one pod ordinal before it is scaled away, via the
/// sequence `crate::CLAUDE.md`/the delivery brief document: `POST
/// /admin/drain {node}`, poll `GET /admin/member/drain-status?node=` to
/// completion, then `POST /admin/member/remove {node}`.
async fn drain_and_remove_node<A: AdminOps>(
    admin: &A,
    name: &str,
    ns: &str,
    ordinal: i32,
    admin_port: i32,
) -> Result<(), String> {
    let node_id = desired::cluster_config::node_id(name, ordinal);
    let base = admin_base_url(name, ns, ordinal, admin_port);

    admin
        .post_json(&format!("{base}/admin/drain"), &json!({ "node": node_id }))
        .await
        .map_err(|e| format!("draining {node_id}: {e}"))?;

    const MAX_POLLS: u32 = 120;
    const POLL_INTERVAL: Duration = Duration::from_secs(5);
    for attempt in 0..MAX_POLLS {
        let status: serde_json::Value = admin
            .get_json(&format!("{base}/admin/member/drain-status?node={node_id}"))
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
            for ordinal in (target_replicas..current_replicas).rev() {
                if let Err(e) =
                    drain_and_remove_node(&ctx.admin, &name, &ns, ordinal, admin_port).await
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
