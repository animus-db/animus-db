//! The `AnimusCluster` custom resource (group `animusdb.io`, version
//! `v1alpha1`, namespaced).
//!
//! This module holds only the CRD's Rust shape (spec/status types) — no
//! logic. Deriving from a spec what the cluster's children should look like
//! lives in [`crate::desired`]; driving the actual reconcile loop lives in
//! [`crate::controller`].

use k8s_openapi::api::core::v1::ResourceRequirements;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Storage configuration for the cluster's data volume.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    /// PVC size (e.g. `"10Gi"`). Defaults to `"10Gi"` when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
    /// `StorageClassName` for the PVC. Omitted lets the cluster default
    /// apply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,
    /// When `true`, every pod uses an `emptyDir` instead of a
    /// `PersistentVolumeClaim`, and `animusd` is started with `--ephemeral`
    /// (the volatile in-memory storage engine) — data does not survive a
    /// pod restart. Defaults to `false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ephemeral: Option<bool>,
}

impl StorageSpec {
    pub const DEFAULT_SIZE: &'static str = "10Gi";

    #[must_use]
    pub fn size_or_default(&self) -> &str {
        self.size.as_deref().unwrap_or(Self::DEFAULT_SIZE)
    }

    #[must_use]
    pub fn is_ephemeral(&self) -> bool {
        self.ephemeral.unwrap_or(false)
    }
}

/// The client-facing `dynamo` port's `Service` configuration.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ClientServiceSpec {
    /// `Service.spec.type` — `ClusterIP` (default), `LoadBalancer`, or
    /// `NodePort`.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "type")]
    pub type_: Option<String>,
}

impl ClientServiceSpec {
    pub const DEFAULT_TYPE: &'static str = "ClusterIP";

    #[must_use]
    pub fn type_or_default(&self) -> &str {
        self.type_.as_deref().unwrap_or(Self::DEFAULT_TYPE)
    }
}

/// `AnimusCluster.spec` — the desired state of one AnimusDB cluster.
#[derive(CustomResource, Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "animusdb.io",
    version = "v1alpha1",
    kind = "AnimusCluster",
    plural = "animusclusters",
    shortname = "adbc",
    namespaced,
    status = "AnimusClusterStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct AnimusClusterSpec {
    /// The `animusd` container image. Defaults to
    /// [`DEFAULT_IMAGE`](Self::DEFAULT_IMAGE) when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// Total pod count (`StatefulSet.spec.replicas`). Must be at least 1.
    pub nodes: i32,
    /// How many of the leading pods (ordinals `0..controlNodes`) run the
    /// combined control+data role (`NodeRole::Both`); the rest run
    /// data-only (`NodeRole::Data`). Defaults to `min(3, nodes)`.
    /// **Immutable after creation** — a later change is rejected (a status
    /// condition is set; the field's original value keeps governing the
    /// cluster) since there is no admission webhook in v1 to reject the
    /// write itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_nodes: Option<i32>,
    /// Data volume configuration.
    #[serde(default)]
    pub storage: StorageSpec,
    /// Passthrough container resource requests/limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
    /// The first of the six-port stride every pod binds
    /// (`base_port + {internal:0, client:1, dynamo:2, admin:3, intra:4,
    /// console:5}`). Defaults to [`DEFAULT_BASE_PORT`](Self::DEFAULT_BASE_PORT).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_port: Option<i32>,
    /// The client-facing dynamo port's own `Service`.
    #[serde(default)]
    pub client_service: ClientServiceSpec,
    /// `--quiesce-after SECS` (combined-role pods only — see
    /// `crates/animus-operator/CLAUDE.md`'s CLI-flag-support table).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quiesce_after_secs: Option<u64>,
    /// `--split-mode {copy,inplace}` (combined-role pods only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split_mode: Option<String>,
    /// **Not currently wired into `entrypoint.sh`** — `animusd`'s
    /// `--config FILE --node I` invocation (what every pod in this
    /// deployment shape runs) does not accept `--auto-split-bytes` today;
    /// that flag exists only on the dev-only `--cluster N` in-process mode.
    /// Kept on the spec for forward compatibility and documented loudly
    /// here and in the crate guide rather than silently dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_split_bytes: Option<u64>,
    /// Name of a `Secret` (in the same namespace) holding the DynamoDB
    /// SigV4 credential store (`{"credentials": {"AKID...": "secret...",
    /// ...}}`, ADR 0057). Mounted read-only at `/etc/animus/dynamo-auth/`
    /// and passed as `--dynamo-auth /etc/animus/dynamo-auth/credentials.json`
    /// to every pod (both roles accept the flag).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dynamo_auth_secret_name: Option<String>,
}

impl AnimusClusterSpec {
    pub const DEFAULT_IMAGE: &'static str = "ghcr.io/animus-db/animusd:latest";
    pub const DEFAULT_BASE_PORT: i32 = 14000;

    #[must_use]
    pub fn image_or_default(&self) -> &str {
        self.image.as_deref().unwrap_or(Self::DEFAULT_IMAGE)
    }

    #[must_use]
    pub fn base_port_or_default(&self) -> i32 {
        self.base_port.unwrap_or(Self::DEFAULT_BASE_PORT)
    }

    /// `min(3, nodes)` when `controlNodes` is omitted.
    #[must_use]
    pub fn control_nodes_or_default(&self) -> i32 {
        self.control_nodes.unwrap_or_else(|| self.nodes.min(3))
    }
}

/// `AnimusCluster.status`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AnimusClusterStatus {
    /// The `.metadata.generation` this status was computed from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Ready pod count, mirrored from the `StatefulSet`'s own
    /// `status.readyReplicas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_nodes: Option<i32>,
    /// The cluster's coarse lifecycle phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<ClusterPhase>,
    /// Typed conditions (`status.conditions[]`), the usual Kubernetes shape.
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
}

/// The cluster's coarse lifecycle phase (`AnimusClusterStatus.phase`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ClusterPhase {
    Pending,
    Bootstrapping,
    Ready,
    Degraded,
    Deleting,
}

/// One `status.conditions[]` entry — the standard Kubernetes condition
/// shape (`type`/`status`/`reason`/`message`/`lastTransitionTime`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterCondition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: ConditionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
}

/// The three-valued Kubernetes condition status.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ConditionStatus {
    True,
    False,
    Unknown,
}

/// Condition type name used when a `spec.controlNodes` change is rejected
/// (no admission webhook in v1 — see [`AnimusClusterSpec::control_nodes`]'s
/// doc).
pub const CONDITION_IMMUTABLE_FIELD_CHANGED: &str = "ImmutableFieldChanged";
/// Condition type name used when a scale-down below `controlNodes` is
/// refused.
pub const CONDITION_SCALE_BELOW_CONTROL_NODES_REFUSED: &str = "ScaleBelowControlNodesRefused";
/// Condition type name used when a member-drain step of a scale-down fails.
pub const CONDITION_DRAIN_FAILED: &str = "DrainFailed";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_nodes_defaults_to_min_three_nodes() {
        let mut spec = AnimusClusterSpec {
            nodes: 5,
            ..Default::default()
        };
        assert_eq!(spec.control_nodes_or_default(), 3);
        spec.nodes = 2;
        assert_eq!(spec.control_nodes_or_default(), 2);
        spec.control_nodes = Some(1);
        assert_eq!(spec.control_nodes_or_default(), 1);
    }

    #[test]
    fn image_and_base_port_defaults() {
        let spec = AnimusClusterSpec {
            nodes: 3,
            ..Default::default()
        };
        assert_eq!(spec.image_or_default(), AnimusClusterSpec::DEFAULT_IMAGE);
        assert_eq!(spec.base_port_or_default(), 14000);
    }

    #[test]
    fn storage_defaults() {
        let storage = StorageSpec::default();
        assert_eq!(storage.size_or_default(), "10Gi");
        assert!(!storage.is_ephemeral());
    }
}
