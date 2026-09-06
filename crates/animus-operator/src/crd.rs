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

/// `spec.tls` — TLS material for the cluster (ADR 0064 commit 3). Exactly
/// one of `secretName`/`certManager` must be set; both or neither is
/// rejected by [`TlsSpec::validate`] (called from `crate::controller`,
/// since there is no admission webhook in v1 to reject the write itself —
/// same posture as `controlNodes`' immutability check).
///
/// Either shape resolves to the *same* `Secret` name
/// ([`TlsSpec::secret_name_or_default`]): a pre-existing `kubernetes.io/tls`
/// Secret the operator only reads (`secretName`), or one cert-manager
/// issues and keeps renewed (`certManager`, materialized by
/// `crate::desired::certificate::build`'s `Certificate.spec.secretName`).
/// Either way every pod mounts it read-only at `/etc/animus/tls`
/// (`crate::desired::statefulset::build`) and every generated node's
/// `RoleAddrs.tls` in `cluster.json` points at the three files inside it
/// (`crate::desired::cluster_config::build_cluster_config`) — one shared
/// cert/key across every pod, not a per-pod one: simpler to issue and mount
/// than a distinct cert per ordinal, and every pod's certificate already
/// needs the union of every ordinal's SANs for cross-node dialing to work,
/// so a per-pod split would buy no smaller a SAN list, only more objects to
/// manage.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TlsSpec {
    /// Name of a pre-existing `Secret` (same namespace, `kubernetes.io/tls`
    /// shape: `tls.crt`/`tls.key`/`ca.crt`) an operator user issued and
    /// placed by hand. Mutually exclusive with `certManager`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_name: Option<String>,
    /// Have cert-manager issue and renew the cluster's cert via a
    /// `Certificate` resource this operator creates and owns (`crate::
    /// desired::certificate::build`). Mutually exclusive with `secretName`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_manager: Option<CertManagerSpec>,
}

impl TlsSpec {
    /// `Ok(())` iff exactly one of `secretName`/`certManager` is set.
    pub fn validate(&self) -> Result<(), String> {
        match (&self.secret_name, &self.cert_manager) {
            (Some(_), Some(_)) => Err(
                "spec.tls: exactly one of secretName/certManager must be set, not both".to_string(),
            ),
            (None, None) => Err(
                "spec.tls: exactly one of secretName/certManager must be set, neither is"
                    .to_string(),
            ),
            _ => Ok(()),
        }
    }

    /// The `Secret` name every pod mounts at `/etc/animus/tls` — the
    /// explicit `secretName`, or, for `certManager`, the name the generated
    /// `Certificate` is told to write to (`{cluster_name}-tls`).
    ///
    /// # Panics
    /// Never in practice: only called after [`Self::validate`] has
    /// confirmed exactly one variant is set.
    #[must_use]
    pub fn secret_name_or_default(&self, cluster_name: &str) -> String {
        self.secret_name
            .clone()
            .unwrap_or_else(|| format!("{cluster_name}-tls"))
    }
}

/// `spec.tls.certManager` — issue via cert-manager against an existing
/// `Issuer`/`ClusterIssuer` (referenced, never created by this operator).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CertManagerSpec {
    pub issuer_ref: IssuerRef,
    /// `Certificate.spec.duration` (e.g. `"2160h"`), passed through
    /// verbatim. Omitted lets cert-manager/the issuer apply its own
    /// default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,
    /// `Certificate.spec.renewBefore` (e.g. `"360h"`), passed through
    /// verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renew_before: Option<String>,
}

/// `spec.tls.certManager.issuerRef` — mirrors cert-manager's own
/// `ObjectReference` shape (`name`/`kind`/`group`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IssuerRef {
    pub name: String,
    /// `"Issuer"` (namespace-scoped) or `"ClusterIssuer"`. Defaults to
    /// `"Issuer"`, matching cert-manager's own default when `kind` is
    /// omitted from a `Certificate`'s `issuerRef`.
    #[serde(default = "IssuerRef::default_kind")]
    pub kind: String,
    /// `Certificate.spec.issuerRef.group` — omitted lets cert-manager
    /// apply its own default (`cert-manager.io`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
}

impl IssuerRef {
    fn default_kind() -> String {
        "Issuer".to_string()
    }
}

/// `spec.s3` — S3-compatible object-store wiring for the cluster's backup
/// and/or stream-segment stores (S-04 PR 3, closing `docs/roadmap.md`'s
/// S-04 item; ADR 0059's 2026-09-06 amendment / "As-built: PR 2" note).
/// Mirrors [`TlsSpec`]'s own precedent — a CRD section that only
/// *references* a pre-existing `Secret` an operator user manages, never one
/// this operator creates or writes to.
///
/// Either or both of `backupStore`/`segmentStore` may independently be set
/// to an `s3://...` URI — the identical shape `animusd`'s own
/// `--backup-store`/`--segment-store` flags accept
/// (`crates/animusd/src/main.rs`'s `parse_s3_uri`, ADR 0059's As-built PR 2
/// note). This crate does not depend on `animusd` (see this crate's own
/// `CLAUDE.md`), so [`S3StoreSpec::validate`] only re-checks a minimal
/// syntactic subset of that parser via [`crate::s3_uri::parse`] — see that
/// module's own doc for exactly what is, and is not, re-verified here. At
/// least one of the two store fields must be set.
///
/// Reaches only **combined-role** pods (`animusd --config FILE --node I`) —
/// a **pre-existing** `animusd` gap, not introduced here: `animusd data
/// --config FILE --node I` accepts neither `--backup-store`,
/// `--segment-store`, `--s3-credentials`, nor `--allow-insecure-s3` today
/// (see `crates/animus-operator/CLAUDE.md`'s CLI-flag-support table and
/// `crates/animusd/src/main.rs::run_data_config`'s own "same documented gap
/// as `--backup-store`" comment).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct S3StoreSpec {
    /// `--backup-store` value for every combined-role pod.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_store: Option<String>,
    /// `--segment-store` value for every combined-role pod.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub segment_store: Option<String>,
    /// Name of a pre-existing `Secret` (same namespace) holding two keys,
    /// `access_key_id`/`secret_access_key` — this operator only ever
    /// *references* it (mounted read-only at `/etc/animus/s3` on every
    /// pod, `crate::desired::statefulset::build`), never creates or writes
    /// one. A combined-role pod's own generated `entrypoint.sh` reads both
    /// files at container-start time and writes a small `--s3-credentials`
    /// JSON file naming only the *path* to `secret_access_key`
    /// (`crate::desired::cluster_config::entrypoint_script`) — the secret
    /// value itself is never copied into the `ConfigMap` or `cluster.json`,
    /// only read out of the mounted `Secret` at runtime, inside the pod.
    pub credentials_secret_name: String,
    /// Whether a plain-`http://` `endpoint=` (`insecure_http=true` in
    /// either store URI's own query string) is allowed. Defaults to
    /// `false`; a URI setting `insecure_http=true` while this is `false` is
    /// rejected by [`Self::validate`] — the same two-flag deliberate-opt-in
    /// posture `animusd`'s own `--allow-insecure-s3` establishes
    /// (`parse_s3_uri`'s "refused unless `--allow-insecure-s3` is also
    /// given"), mirrored here since this field's only job is to become
    /// that flag on the generated `entrypoint.sh`.
    #[serde(default)]
    pub allow_insecure_http: bool,
    /// CIDRs the generated `NetworkPolicy`'s egress section allows toward
    /// the S3 endpoint's own port, in addition to the cluster's baseline
    /// intra-cluster + DNS egress (`crate::desired::networkpolicy`).
    /// Defaults to `["0.0.0.0/0"]`.
    ///
    /// **`NetworkPolicy` cannot express a hostname allowlist** — only IP
    /// blocks — so this operator has no way to resolve `endpoint=...`'s
    /// hostname into the right CIDR for you. **Narrow this list to your
    /// object store's actual address range in any environment where
    /// open-to-any-destination egress on that port is unacceptable** — see
    /// `deploy/operator/example.yaml`'s own commented `s3:` section.
    #[serde(default = "S3StoreSpec::default_egress_cidrs")]
    pub egress_cidrs: Vec<String>,
}

impl S3StoreSpec {
    #[must_use]
    pub fn default_egress_cidrs() -> Vec<String> {
        vec!["0.0.0.0/0".to_string()]
    }

    /// `Ok(())` iff: at least one of `backupStore`/`segmentStore` is set;
    /// `credentialsSecretName` is non-empty; every set store URI has the
    /// minimal shape [`crate::s3_uri::parse`] requires; and no set URI's
    /// `insecure_http=true` without `allowInsecureHttp` also being `true`.
    pub fn validate(&self) -> Result<(), String> {
        if self.backup_store.is_none() && self.segment_store.is_none() {
            return Err(
                "spec.s3: at least one of backupStore/segmentStore must be set".to_string(),
            );
        }
        if self.credentials_secret_name.trim().is_empty() {
            return Err("spec.s3.credentialsSecretName must not be empty".to_string());
        }
        for (field, value) in [
            ("backupStore", &self.backup_store),
            ("segmentStore", &self.segment_store),
        ] {
            if let Some(uri) = value {
                let info =
                    crate::s3_uri::parse(uri).map_err(|e| format!("spec.s3.{field}: {e}"))?;
                if info.insecure_http && !self.allow_insecure_http {
                    return Err(format!(
                        "spec.s3.{field} {uri:?}: insecure_http=true requires \
                         spec.s3.allowInsecureHttp=true"
                    ));
                }
            }
        }
        Ok(())
    }
}

impl Default for S3StoreSpec {
    fn default() -> Self {
        Self {
            backup_store: None,
            segment_store: None,
            credentials_secret_name: String::new(),
            allow_insecure_http: false,
            egress_cidrs: Self::default_egress_cidrs(),
        }
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
    /// The idle-before-quiescing grace period for a data-plane CP group
    /// (ADR 0044 phase-1 / ADR 0048). **S-06**: emitted into the generated
    /// `cluster.json`'s `cluster_settings.quiesce_after_secs` section
    /// (`desired::cluster_config::build_cluster_config`), not a CLI flag —
    /// this now applies to **every** pod, combined and data-role alike (an
    /// `animusd` data-only node had no route to quiescence at all before
    /// S-06 closed that gap; see `crates/animus-operator/CLAUDE.md`'s
    /// CLI-flag-support table for the full before/after picture).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quiesce_after_secs: Option<u64>,
    /// The scoped-bytes threshold that auto-splits a led tablet (ADR 0034).
    /// **S-06**: like `quiesce_after_secs` above, now emitted into the
    /// generated `cluster.json`'s `cluster_settings.auto_split_bytes`
    /// section rather than a CLI flag — `--auto-split-bytes` itself only
    /// ever existed on the dev-only `--cluster N` in-process mode, so this
    /// field went from **never wired into `entrypoint.sh` at all** to
    /// reaching every pod (combined and data-role) through the config file
    /// instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_split_bytes: Option<u64>,
    /// Name of a `Secret` (in the same namespace) holding the DynamoDB
    /// SigV4 credential store (`{"credentials": {"AKID...": "secret...",
    /// ...}}`, ADR 0057). Mounted read-only at `/etc/animus/dynamo-auth/`
    /// and passed as `--dynamo-auth /etc/animus/dynamo-auth/credentials.json`
    /// to every pod (both roles accept the flag).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dynamo_auth_secret_name: Option<String>,
    /// TLS material for the cluster (ADR 0064 commit 3). `None` (default)
    /// keeps every pod on plain TCP, byte-for-byte the pre-existing
    /// behavior. See [`TlsSpec`]'s own doc for the two mutually exclusive
    /// shapes and how the resolved `Secret` is mounted/wired.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsSpec>,
    /// S3-compatible object-store wiring for the cluster's backup and/or
    /// stream-segment stores (S-04 PR 3). `None` (default) leaves both
    /// stores at their pre-existing default (`cluster`/`ClusterSegmentStore`)
    /// or whatever a future non-S3 CRD surface sets (S-07b, not this PR).
    /// See [`S3StoreSpec`]'s own doc for the two mutually-independent store
    /// fields, the referenced credential `Secret`, and the egress CIDRs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3: Option<S3StoreSpec>,
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
/// Condition type name used when `spec.tls` sets both or neither of
/// `secretName`/`certManager` — see [`TlsSpec::validate`].
pub const CONDITION_TLS_SPEC_INVALID: &str = "TlsSpecInvalid";
/// Condition type name used when `spec.s3` fails [`S3StoreSpec::validate`]
/// (neither store set, an empty `credentialsSecretName`, a malformed store
/// URI, or `insecure_http=true` without `allowInsecureHttp`).
pub const CONDITION_S3_SPEC_INVALID: &str = "S3SpecInvalid";

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

    // --- TlsSpec (ADR 0064 commit 3) -------------------------------------

    #[test]
    fn tls_spec_rejects_both_shapes_set() {
        let tls = TlsSpec {
            secret_name: Some("s".to_string()),
            cert_manager: Some(CertManagerSpec {
                issuer_ref: IssuerRef {
                    name: "i".to_string(),
                    kind: "Issuer".to_string(),
                    group: None,
                },
                duration: None,
                renew_before: None,
            }),
        };
        assert!(tls.validate().is_err());
    }

    #[test]
    fn tls_spec_rejects_neither_shape_set() {
        assert!(TlsSpec::default().validate().is_err());
    }

    #[test]
    fn tls_spec_secret_name_shape_is_valid() {
        let tls = TlsSpec {
            secret_name: Some("my-tls".to_string()),
            cert_manager: None,
        };
        assert!(tls.validate().is_ok());
        assert_eq!(tls.secret_name_or_default("c"), "my-tls");
    }

    #[test]
    fn tls_spec_cert_manager_shape_is_valid_and_defaults_secret_name() {
        let tls = TlsSpec {
            secret_name: None,
            cert_manager: Some(CertManagerSpec {
                issuer_ref: IssuerRef {
                    name: "letsencrypt".to_string(),
                    kind: "ClusterIssuer".to_string(),
                    group: None,
                },
                duration: None,
                renew_before: None,
            }),
        };
        assert!(tls.validate().is_ok());
        assert_eq!(tls.secret_name_or_default("c"), "c-tls");
    }

    #[test]
    fn issuer_ref_kind_defaults_to_issuer() {
        let json = serde_json::json!({ "name": "my-issuer" });
        let issuer_ref: IssuerRef = serde_json::from_value(json).unwrap();
        assert_eq!(issuer_ref.kind, "Issuer");
    }

    // --- S3StoreSpec (S-04 PR 3) ------------------------------------------

    fn valid_s3_spec() -> S3StoreSpec {
        S3StoreSpec {
            backup_store: Some(
                "s3://my-bucket/backups?endpoint=https://s3.example.com".to_string(),
            ),
            segment_store: None,
            credentials_secret_name: "my-s3-creds".to_string(),
            allow_insecure_http: false,
            egress_cidrs: S3StoreSpec::default_egress_cidrs(),
        }
    }

    #[test]
    fn s3_spec_valid_when_backup_store_alone_is_set() {
        assert!(valid_s3_spec().validate().is_ok());
    }

    #[test]
    fn s3_spec_valid_when_segment_store_alone_is_set() {
        let mut s3 = valid_s3_spec();
        s3.backup_store = None;
        s3.segment_store = Some("s3://bucket?endpoint=https://s3.example.com".to_string());
        assert!(s3.validate().is_ok());
    }

    #[test]
    fn s3_spec_rejects_neither_store_set() {
        let mut s3 = valid_s3_spec();
        s3.backup_store = None;
        let err = s3.validate().unwrap_err();
        assert!(err.contains("backupStore/segmentStore"), "{err}");
    }

    #[test]
    fn s3_spec_rejects_empty_credentials_secret_name() {
        let mut s3 = valid_s3_spec();
        s3.credentials_secret_name = String::new();
        let err = s3.validate().unwrap_err();
        assert!(err.contains("credentialsSecretName"), "{err}");
    }

    #[test]
    fn s3_spec_rejects_blank_credentials_secret_name() {
        let mut s3 = valid_s3_spec();
        s3.credentials_secret_name = "   ".to_string();
        assert!(s3.validate().is_err());
    }

    #[test]
    fn s3_spec_rejects_malformed_store_uri() {
        let mut s3 = valid_s3_spec();
        s3.backup_store = Some("not-an-s3-uri".to_string());
        let err = s3.validate().unwrap_err();
        assert!(err.contains("backupStore"), "{err}");
    }

    #[test]
    fn s3_spec_rejects_insecure_http_without_allow_insecure_http() {
        let mut s3 = valid_s3_spec();
        s3.backup_store =
            Some("s3://bucket?endpoint=http://minio.ns.svc:9000&insecure_http=true".to_string());
        let err = s3.validate().unwrap_err();
        assert!(err.contains("allowInsecureHttp"), "{err}");
    }

    #[test]
    fn s3_spec_accepts_insecure_http_when_allowed() {
        let mut s3 = valid_s3_spec();
        s3.backup_store =
            Some("s3://bucket?endpoint=http://minio.ns.svc:9000&insecure_http=true".to_string());
        s3.allow_insecure_http = true;
        assert!(s3.validate().is_ok());
    }

    #[test]
    fn s3_spec_default_egress_cidrs_is_open_to_any_destination() {
        assert_eq!(S3StoreSpec::default_egress_cidrs(), vec!["0.0.0.0/0"]);
        assert_eq!(S3StoreSpec::default().egress_cidrs, vec!["0.0.0.0/0"]);
    }

    #[test]
    fn s3_spec_json_round_trip_defaults_allow_insecure_http_and_egress_cidrs() {
        let json = serde_json::json!({
            "backupStore": "s3://bucket?endpoint=https://s3.example.com",
            "credentialsSecretName": "creds"
        });
        let s3: S3StoreSpec = serde_json::from_value(json).unwrap();
        assert!(!s3.allow_insecure_http);
        assert_eq!(s3.egress_cidrs, vec!["0.0.0.0/0"]);
        assert!(s3.validate().is_ok());
    }
}
