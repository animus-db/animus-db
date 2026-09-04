//! [`ClusterApi`]: the handful of Kubernetes API operations
//! [`crate::controller`]'s reconcile logic actually performs, factored out
//! so that logic can be driven by an in-memory fake in tests
//! (`crate::fakes::FakeClusterApi`, `#[cfg(test)]`) instead of a live API
//! server. [`RealClusterApi`] is the only production implementor, a thin
//! wrapper over `kube::Client` — see `crates/animus-operator/CLAUDE.md`'s
//! testing section and ADR 0061's Phase E amendment note for the seam's
//! trade-off (why a trait rather than `kube`'s own `tower_test` mock
//! service).
//!
//! Every method takes the already-built desired object (or, for a read,
//! just the name) and does exactly one API call — no retry, no diffing,
//! no logic. All of that stays in `crate::controller` and `crate::desired`,
//! which is what makes those two things testable independently of this
//! seam.

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use serde_json::json;

use crate::controller::{FIELD_MANAGER, ReconcileError};
use crate::crd::{AnimusCluster, AnimusClusterStatus};

fn apply_params() -> PatchParams {
    PatchParams::apply(FIELD_MANAGER).force()
}

/// The Kubernetes operations `crate::controller::reconcile` and its helpers
/// perform. One method per distinct API call site — see each call site in
/// `crate::controller` for why it needs exactly this shape.
#[async_trait::async_trait]
pub trait ClusterApi: Send + Sync {
    /// Server-side-apply `cm` (`PatchParams::apply(FIELD_MANAGER).force()`).
    async fn apply_configmap(&self, ns: &str, cm: &ConfigMap) -> Result<(), ReconcileError>;
    /// Server-side-apply `svc` — used for both the headless internal
    /// `Service` and the client-facing `dynamo` `Service`.
    async fn apply_service(&self, ns: &str, svc: &Service) -> Result<(), ReconcileError>;
    /// Server-side-apply `np`.
    async fn apply_networkpolicy(&self, ns: &str, np: &NetworkPolicy)
    -> Result<(), ReconcileError>;
    /// Server-side-apply `sts`, returning the object the API server stored
    /// (its `status.readyReplicas` is what `finish_reconcile` computes
    /// `AnimusClusterStatus.phase` from).
    async fn apply_statefulset(
        &self,
        ns: &str,
        sts: &StatefulSet,
    ) -> Result<StatefulSet, ReconcileError>;
    /// `GET` the `ConfigMap` named `name`, or `None` if it does not exist —
    /// used by `control_nodes_changed` to read back the previous reconcile's
    /// own applied `cluster.json`.
    async fn get_configmap(
        &self,
        ns: &str,
        name: &str,
    ) -> Result<Option<ConfigMap>, ReconcileError>;
    /// `GET` the `StatefulSet` named `name`, or `None` if it does not exist
    /// — used by `reconcile` to read the *current* replica count before
    /// deciding whether a scale-down drain sequence is needed.
    async fn get_statefulset(
        &self,
        ns: &str,
        name: &str,
    ) -> Result<Option<StatefulSet>, ReconcileError>;
    /// Merge-patch `status` onto the `AnimusCluster` named `name`'s status
    /// subresource.
    async fn patch_cluster_status(
        &self,
        ns: &str,
        name: &str,
        status: &AnimusClusterStatus,
    ) -> Result<(), ReconcileError>;
}

/// The production [`ClusterApi`]: every method is exactly the `kube::Api`
/// call `crate::controller` used to make directly before this seam existed.
pub struct RealClusterApi {
    client: Client,
}

impl RealClusterApi {
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait::async_trait]
impl ClusterApi for RealClusterApi {
    async fn apply_configmap(&self, ns: &str, cm: &ConfigMap) -> Result<(), ReconcileError> {
        Api::<ConfigMap>::namespaced(self.client.clone(), ns)
            .patch(
                cm.metadata.name.as_deref().unwrap(),
                &apply_params(),
                &Patch::Apply(cm),
            )
            .await?;
        Ok(())
    }

    async fn apply_service(&self, ns: &str, svc: &Service) -> Result<(), ReconcileError> {
        Api::<Service>::namespaced(self.client.clone(), ns)
            .patch(
                svc.metadata.name.as_deref().unwrap(),
                &apply_params(),
                &Patch::Apply(svc),
            )
            .await?;
        Ok(())
    }

    async fn apply_networkpolicy(
        &self,
        ns: &str,
        np: &NetworkPolicy,
    ) -> Result<(), ReconcileError> {
        Api::<NetworkPolicy>::namespaced(self.client.clone(), ns)
            .patch(
                np.metadata.name.as_deref().unwrap(),
                &apply_params(),
                &Patch::Apply(np),
            )
            .await?;
        Ok(())
    }

    async fn apply_statefulset(
        &self,
        ns: &str,
        sts: &StatefulSet,
    ) -> Result<StatefulSet, ReconcileError> {
        let applied = Api::<StatefulSet>::namespaced(self.client.clone(), ns)
            .patch(
                sts.metadata.name.as_deref().unwrap(),
                &apply_params(),
                &Patch::Apply(sts),
            )
            .await?;
        Ok(applied)
    }

    async fn get_configmap(
        &self,
        ns: &str,
        name: &str,
    ) -> Result<Option<ConfigMap>, ReconcileError> {
        Ok(Api::<ConfigMap>::namespaced(self.client.clone(), ns)
            .get_opt(name)
            .await?)
    }

    async fn get_statefulset(
        &self,
        ns: &str,
        name: &str,
    ) -> Result<Option<StatefulSet>, ReconcileError> {
        Ok(Api::<StatefulSet>::namespaced(self.client.clone(), ns)
            .get_opt(name)
            .await?)
    }

    async fn patch_cluster_status(
        &self,
        ns: &str,
        name: &str,
        status: &AnimusClusterStatus,
    ) -> Result<(), ReconcileError> {
        let cluster_api = Api::<AnimusCluster>::namespaced(self.client.clone(), ns);
        let patch = json!({ "status": status });
        // A merge patch, not server-side apply: `PatchParams::force` is only
        // valid with `Patch::Apply`, and the API client rejects the
        // combination before the request is even sent.
        cluster_api
            .patch_status(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await?;
        Ok(())
    }
}
