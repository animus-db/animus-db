//! Pure builder functions for an `AnimusCluster`'s desired child objects.
//!
//! Every function here is deterministic and side-effect-free: `(name, ns,
//! spec) -> a typed k8s-openapi object`, no cluster access. This is where
//! almost all of this crate's tests live — the controller (`crate::controller`)
//! is a thin imperative shell that calls these builders and applies the
//! result; the desired *shape* of a child is proven here, once, independent
//! of the k8s API.

pub mod certificate;
pub mod cluster_config;
pub mod configmap;
pub mod networkpolicy;
pub mod services;
pub mod statefulset;
#[cfg(test)]
pub mod test_support;

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

use crate::crd::AnimusCluster;

/// `app.kubernetes.io/name` value for every child of every `AnimusCluster`.
pub const APP_NAME: &str = "animusdb";
/// `app.kubernetes.io/managed-by` value for every child.
pub const MANAGED_BY: &str = "animus-operator";
/// The operator's own `app.kubernetes.io/name` label (distinct from
/// [`APP_NAME`] — this labels the *operator's* pods, not a cluster's),
/// referenced by the `NetworkPolicy` builder to allow the operator to reach
/// the admin port.
pub const OPERATOR_APP_NAME: &str = "animus-operator";

/// The stable label set every child of cluster `name` carries, and the
/// selector every child's `Service`/`StatefulSet`/`NetworkPolicy` matches
/// pods with.
#[must_use]
pub fn common_labels(name: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), APP_NAME.to_string());
    labels.insert("app.kubernetes.io/instance".to_string(), name.to_string());
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        MANAGED_BY.to_string(),
    );
    labels
}

/// Just the two labels that identify *this cluster's* pods — the selector
/// every child uses (a `Service`/`StatefulSet` selector must not include
/// `managed-by`, which is a descriptive label, not part of pod identity, so
/// it can't change independently without orphaning pods).
#[must_use]
pub fn selector_labels(name: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), APP_NAME.to_string());
    labels.insert("app.kubernetes.io/instance".to_string(), name.to_string());
    labels
}

/// The headless internal `Service`'s name for cluster `name` — also the
/// `StatefulSet`'s `serviceName`, and the DNS zone every pod's stable
/// hostname (`{name}-{ordinal}.{name}-internal.{ns}.svc.cluster.local`)
/// resolves through.
#[must_use]
pub fn internal_service_name(name: &str) -> String {
    format!("{name}-internal")
}

/// The client-facing dynamo `Service`'s name for cluster `name`.
#[must_use]
pub fn client_service_name(name: &str) -> String {
    format!("{name}-dynamo")
}

/// The cluster config `ConfigMap`'s name for cluster `name`.
#[must_use]
pub fn config_map_name(name: &str) -> String {
    format!("{name}-config")
}

/// The `NetworkPolicy`'s name for cluster `name`.
#[must_use]
pub fn network_policy_name(name: &str) -> String {
    format!("{name}-internal-only")
}

/// A single owner reference pointing at `cluster`, `controller: true` (so
/// Kubernetes GC deletes every child when the `AnimusCluster` is deleted —
/// this operator ships no finalizer in v1; deletion relies entirely on this
/// mechanism, see `crate::controller`'s own doc).
///
/// # Panics
/// Never in practice: `cluster.metadata.name`/`.uid` are always populated
/// on an object read back from the API server, which is the only place a
/// live `&AnimusCluster` this function is called with ever comes from.
#[must_use]
pub fn owner_reference(cluster: &AnimusCluster) -> OwnerReference {
    OwnerReference {
        api_version: "animusdb.io/v1alpha1".to_string(),
        kind: "AnimusCluster".to_string(),
        name: cluster
            .metadata
            .name
            .clone()
            .expect("AnimusCluster read from the API server always has a name"),
        uid: cluster
            .metadata
            .uid
            .clone()
            .expect("AnimusCluster read from the API server always has a uid"),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// The pod DNS hostname a headless `Service` gives ordinal `ordinal` of
/// cluster `name` in namespace `ns` — what `RoleAddrs::advertise_host`
/// carries for that node.
#[must_use]
pub fn pod_fqdn(name: &str, ns: &str, ordinal: i32) -> String {
    format!(
        "{name}-{ordinal}.{}.{ns}.svc.cluster.local",
        internal_service_name(name)
    )
}
