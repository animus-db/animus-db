//! The `StatefulSet` builder: one pod per node ordinal, running
//! `entrypoint.sh` off the cluster `ConfigMap`, probed on the admin port's
//! `GET /admin/health`. The probes' scheme follows `spec.tls`: HTTP when
//! unset, HTTPS (unverified, as the kubelet itself does not check the
//! server certificate) when set — admin is server-only TLS (ADR 0064), so
//! a plaintext probe against a TLS-only listener fails the handshake on
//! the server side every probe period and the pod never goes Ready.
//!
//! **Config-hash restart annotation (S-07d groundwork, 2026-09-06)**: the
//! pod template carries a [`CONFIG_HASH_ANNOTATION`] whose value is a
//! content hash of [`restart_relevant_projection`] — **not** of the raw
//! generated `ConfigMap`. A mounted `ConfigMap` volume's content changes in
//! place on the kubelet's own sync period, but nothing makes the
//! already-running `animusd` process re-read it (it's a startup-time config
//! file, not a hot-reloaded one), so a `cluster.json` change (e.g. a role
//! flip from a `spec.controlNodes` increase, S-07d) had no way to actually
//! reach a running pod short of an operator manually deleting it. Baking a
//! hash into the pod template makes such a change a change to the
//! `StatefulSet`'s own `spec.template` — which the `StatefulSet` controller
//! treats exactly like an image bump: a rolling restart of every pod,
//! highest ordinal first, one at a time, honoring the existing readiness
//! probe before moving to the next.
//!
//! **The hash is deliberately a projection, not the whole `ConfigMap`
//! (2026-09-06 fix, closing the `e2e-kind-tls` regression this same commit
//! first introduced)**: hashing the entire generated `ConfigMap` `data` map
//! made a plain `spec.nodes` scale-up/down — which appends/removes a
//! `RoleAddrs` entry in `cluster.json`'s `nodes` array without touching any
//! *existing* entry — change the hash and roll every pod, even though a
//! running `animusd` never rereads that array at all: it learns about new
//! peers through replicated `Metadata` (ADR 0030 self-registration), never
//! from `cluster.json`. Concretely, `scripts/e2e-kind.sh`'s scale phase
//! (3 → 4 nodes) rolled `e2e-0`/`e2e-1`/`e2e-2` out from under the very
//! `port-forward` the script had open to them, killing the post-scale
//! `GetItem` check. The rule going forward: **hash exactly what a running
//! pod read once at boot and cannot pick up live — never the node list, its
//! length, or any per-node address/id/`advertise_host`.** See
//! [`restart_relevant_projection`]'s own doc for the exact field-by-field
//! in/out list, and `crates/animus-operator/CLAUDE.md`'s matching note for
//! the same rule stated for future maintainers who land here without
//! reading this module first.

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::{
    ConfigMapVolumeSource, Container, EmptyDirVolumeSource, EnvVar, HTTPGetAction,
    PersistentVolumeClaim, PersistentVolumeClaimSpec, PodSpec, PodTemplateSpec, Probe,
    ResourceRequirements, SecretVolumeSource, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use serde::Serialize;

use super::cluster_config::{
    self, CONFIG_MOUNT_DIR, DATA_DIR, DYNAMO_AUTH_MOUNT_DIR, ENTRYPOINT_FILE_NAME, S3_MOUNT_DIR,
    TLS_MOUNT_DIR,
};
use super::{
    common_labels, config_map_name, internal_service_name, owner_reference, selector_labels,
};
use crate::crd::{AnimusCluster, AnimusClusterSpec};

/// `readinessProbe`: `periodSeconds: 5`, `failureThreshold: 3` — fast to
/// pull a pod out of `Endpoints` (and therefore the client `Service`'s LB
/// rotation) once its own `/admin/health` starts reporting no known control
/// leader.
const READINESS_PERIOD_SECS: i32 = 5;
const READINESS_FAILURE_THRESHOLD: i32 = 3;
/// `livenessProbe`: generous thresholds (`initialDelaySeconds: 30`,
/// `periodSeconds: 10`, `failureThreshold: 6` — a full minute of failures)
/// so a pod recovering from a slow Raft snapshot install or a large
/// compaction is never killed out from under itself; a liveness restart is
/// meant only for a genuinely wedged process.
const LIVENESS_INITIAL_DELAY_SECS: i32 = 30;
const LIVENESS_PERIOD_SECS: i32 = 10;
const LIVENESS_FAILURE_THRESHOLD: i32 = 6;
/// A pod draining (control-plane relay + tablet handoff) or shutting down
/// gracefully (`SIGTERM`, ADR 0060 groundwork) needs real time — generous
/// on purpose, matching this deployment's own graceful-shutdown contract
/// rather than the Kubernetes 30s default.
const TERMINATION_GRACE_PERIOD_SECS: i64 = 90;

/// Default `RUST_LOG` every pod starts with (`animusd::otel::init_tracing`
/// falls back to this same level when the env var is absent, so this is
/// belt-and-suspenders — the point is making the value an explicit,
/// visible pod env var rather than an implicit fallback baked into a
/// library nobody reconciling a stuck cluster thinks to go read). A cluster
/// that never elects a control-plane leader (the failure mode this exists
/// for — see `animus_env::prod::spawn_accept`'s own doc for a concrete
/// instance) produces zero diagnostic signal at `animusd`'s default level
/// otherwise: `kubectl logs` on every pod shows only the one-line startup
/// banner forever, with no indication why. Overridable per cluster by
/// setting `RUST_LOG` through `spec.resources`'s container env is not
/// exposed by the CRD today (no live use case yet); bump this constant (or
/// add a CRD field) if one shows up.
const DEFAULT_RUST_LOG: &str = "info";

/// The pod-template annotation carrying the generated `ConfigMap`'s own
/// content hash — see this module's own doc for why this exists and what
/// it triggers.
pub const CONFIG_HASH_ANNOTATION: &str = "animusdb.io/config-hash";

/// The restart-relevant subset of the generated config (`cluster.json` +
/// `entrypoint.sh`) — everything a running `animusd` process read once at
/// boot and cannot pick up live, deliberately **excluding** `spec.nodes`
/// itself and every per-node `id`/address/`advertise_host`
/// `build_cluster_config` emits for `0..spec.nodes`: a running pod never
/// rereads that list (it learns about new/changed peers through replicated
/// `Metadata`, ADR 0030 self-registration), so those fields must never
/// affect [`CONFIG_HASH_ANNOTATION`] — see this module's own doc for the
/// e2e failure that hashing the raw `ConfigMap` caused.
///
/// **In:**
/// - `control_nodes`: the `if [ "$ord" -lt N ]` role-split threshold
///   (`AnimusClusterSpec::control_nodes_or_default`) baked once into
///   `entrypoint.sh`, shared by every pod regardless of ordinal. A
///   `spec.controlNodes` change flips which subcommand/flags an *existing*
///   ordinal's `entrypoint.sh` branch execs — exactly the case S-07d's
///   growth flow exists to restart pods for — so this, and the
///   `entrypoint_sh` text it's already embedded in, must roll every pod.
///   Listed as its own field only for readability in a diffed dump of this
///   struct; `entrypoint_sh` alone already changes whenever this does.
/// - `entrypoint_sh`: the full script text. Already independent of
///   `spec.nodes` on its own — [`cluster_config::entrypoint_script`] takes
///   only `spec`, never a node count — so including it costs nothing and
///   catches `ephemeral`/`dynamo_auth_secret_name` presence and every
///   resolved `s3`/`backupStore`/`segmentStore` flag in one field.
/// - `cluster_settings`: `cluster.json`'s own section
///   (`autoSplitBytes`/`quiesceAfterSecs` today, via
///   [`cluster_config::cluster_settings_or_none`], the exact same
///   "empty means absent" rule [`cluster_config::build_cluster_config`]
///   uses) — read once at boot by every pod regardless of role.
/// - `tls`: whether TLS is wired at all, plus the (fixed, mount-path-only)
///   section every node's `cluster.json` entry gets when it is
///   ([`cluster_config::tls_section`], identical across nodes by
///   construction). `spec.tls`'s *secret name* is deliberately not part of
///   this: it only changes which `Secret` a volume points at, which is
///   already a `spec.template` change (the volume/mount list itself, plus
///   the readiness/liveness probe scheme via [`admin_probe`]) that the
///   `StatefulSet` controller diffs on its own, with no help from this
///   annotation needed.
///
/// **Out:** `spec.nodes`, and every `RoleAddrs` field
/// [`cluster_config::build_cluster_config`] derives per-ordinal
/// (`id`/ports/`advertise_host`) — see this module's own doc.
#[derive(Serialize)]
struct RestartRelevantConfig {
    control_nodes: i32,
    entrypoint_sh: String,
    cluster_settings: Option<cluster_config::ClusterSettings>,
    tls: Option<cluster_config::TlsSection>,
}

fn restart_relevant_projection(spec: &AnimusClusterSpec) -> RestartRelevantConfig {
    RestartRelevantConfig {
        control_nodes: spec.control_nodes_or_default(),
        entrypoint_sh: cluster_config::entrypoint_script(spec),
        cluster_settings: cluster_config::cluster_settings_or_none(spec),
        tls: spec.tls.as_ref().map(|_| cluster_config::tls_section()),
    }
}

/// FNV-1a 64-bit: a fixed, dependency-free hash whose bit pattern is part
/// of this function's own definition, not of any library or standard-
/// library internal — unlike `std::collections::hash_map::DefaultHasher`,
/// which carries **no** stability guarantee across Rust releases (its docs
/// say so explicitly). [`CONFIG_HASH_ANNOTATION`]'s value is persisted on
/// disk as part of every cluster's live `StatefulSet`, so a hash that could
/// silently change bit pattern on a routine toolchain bump would roll every
/// deployed cluster's pods for no operator-visible reason — not acceptable
/// for a value whose entire job is to change **only** when the restart-
/// relevant config actually does. Not cryptographic — this is a
/// change-detection annotation, not a security boundary, so collision
/// resistance beyond "vanishingly unlikely by accident" is not a goal.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET_BASIS;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// The [`CONFIG_HASH_ANNOTATION`] value for `spec`: FNV-1a 64 (see
/// [`fnv1a_64`]) over [`restart_relevant_projection`]'s canonical JSON
/// encoding. JSON, not the struct's raw bytes, so a future field addition
/// changes the hash the same way any other content change does, without
/// needing a manual `Hash` impl kept in sync with [`Serialize`] by hand.
fn restart_relevant_config_hash(spec: &AnimusClusterSpec) -> String {
    let projection = restart_relevant_projection(spec);
    let json = serde_json::to_string(&projection).expect("RestartRelevantConfig always serializes");
    format!("{:016x}", fnv1a_64(json.as_bytes()))
}

const CONFIG_VOLUME: &str = "config";
const DATA_VOLUME: &str = "data";
const DYNAMO_AUTH_VOLUME: &str = "dynamo-auth";
const TLS_VOLUME: &str = "tls";
const S3_VOLUME: &str = "s3";

/// `tls_enabled` mirrors `spec.tls.is_some()`: admin is server-only TLS
/// (ADR 0064), so when it's on the probe's `GET /admin/health` must speak
/// HTTPS too, or the kubelet's plaintext request just fails the TLS
/// handshake on the server side every probe period. The kubelet's HTTPS
/// probe scheme does not verify the server certificate, so this needs no
/// CA plumbed into it — see ADR 0064 and the fix that added this.
fn admin_probe(admin_port: i32, tls_enabled: bool, extra: impl FnOnce(&mut Probe)) -> Probe {
    let mut probe = Probe {
        http_get: Some(HTTPGetAction {
            path: Some("/admin/health".to_string()),
            port: IntOrString::Int(admin_port),
            scheme: tls_enabled.then(|| "HTTPS".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };
    extra(&mut probe);
    probe
}

/// Build the `StatefulSet` for `cluster`.
#[must_use]
pub fn build(cluster: &AnimusCluster, spec: &AnimusClusterSpec) -> StatefulSet {
    let name = cluster
        .metadata
        .name
        .as_deref()
        .expect("AnimusCluster read from the API server always has a name");
    let ns = cluster
        .metadata
        .namespace
        .as_deref()
        .expect("AnimusCluster read from the API server always has a namespace");
    let admin_port = spec.base_port_or_default() + super::cluster_config::PORT_ADMIN;
    let labels = common_labels(name);
    let selector = selector_labels(name);
    let ephemeral = spec.storage.is_ephemeral();

    let mut volumes = vec![Volume {
        name: CONFIG_VOLUME.to_string(),
        config_map: Some(ConfigMapVolumeSource {
            name: config_map_name(name),
            ..Default::default()
        }),
        ..Default::default()
    }];
    let mut volume_mounts = vec![
        VolumeMount {
            name: CONFIG_VOLUME.to_string(),
            mount_path: CONFIG_MOUNT_DIR.to_string(),
            read_only: Some(true),
            ..Default::default()
        },
        VolumeMount {
            name: DATA_VOLUME.to_string(),
            mount_path: DATA_DIR.to_string(),
            ..Default::default()
        },
    ];

    if ephemeral {
        volumes.push(Volume {
            name: DATA_VOLUME.to_string(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Default::default()
        });
    }

    let mut volume_claim_templates = Vec::new();
    if !ephemeral {
        volume_claim_templates.push(PersistentVolumeClaim {
            metadata: ObjectMeta {
                name: Some(DATA_VOLUME.to_string()),
                labels: Some(labels.clone()),
                ..Default::default()
            },
            spec: Some(PersistentVolumeClaimSpec {
                access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                storage_class_name: spec.storage.storage_class_name.clone(),
                resources: Some(k8s_openapi::api::core::v1::VolumeResourceRequirements {
                    requests: Some(BTreeMap::from([(
                        "storage".to_string(),
                        Quantity(spec.storage.size_or_default().to_string()),
                    )])),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
    }

    if let Some(secret_name) = &spec.dynamo_auth_secret_name {
        volumes.push(Volume {
            name: DYNAMO_AUTH_VOLUME.to_string(),
            secret: Some(SecretVolumeSource {
                secret_name: Some(secret_name.clone()),
                ..Default::default()
            }),
            ..Default::default()
        });
        volume_mounts.push(VolumeMount {
            name: DYNAMO_AUTH_VOLUME.to_string(),
            mount_path: DYNAMO_AUTH_MOUNT_DIR.to_string(),
            read_only: Some(true),
            ..Default::default()
        });
    }

    // ADR 0064 commit 3: one shared `Secret` (a pre-existing
    // `kubernetes.io/tls` one, or cert-manager's own output) mounted
    // identically on every pod — see `TlsSpec`'s own doc for why one
    // shared cert, not a per-pod one.
    let tls_enabled = spec.tls.is_some();
    if let Some(tls) = &spec.tls {
        volumes.push(Volume {
            name: TLS_VOLUME.to_string(),
            secret: Some(SecretVolumeSource {
                secret_name: Some(tls.secret_name_or_default(name)),
                ..Default::default()
            }),
            ..Default::default()
        });
        volume_mounts.push(VolumeMount {
            name: TLS_VOLUME.to_string(),
            mount_path: TLS_MOUNT_DIR.to_string(),
            read_only: Some(true),
            ..Default::default()
        });
    }

    // S-04 PR 3: `spec.s3`'s credential `Secret` (never created or written
    // by this operator, only referenced — same idiom as `spec.tls` above),
    // mounted read-only on **every** pod even though only a combined-role
    // pod's own `entrypoint.sh` branch actually reads it (see
    // `cluster_config::entrypoint_script`'s own doc for the data-role
    // gap) — mounting it everywhere keeps this builder's volume/mount logic
    // identical to `dynamo-auth`/`tls`'s own "one shared Secret, every pod"
    // shape, rather than conditioning the mount itself on pod role (which
    // this builder has no ordinal to do per-pod anyway — see this crate's
    // own "no per-pod port striding" doc for why every pod's spec here is
    // otherwise identical).
    if let Some(s3) = &spec.s3 {
        volumes.push(Volume {
            name: S3_VOLUME.to_string(),
            secret: Some(SecretVolumeSource {
                secret_name: Some(s3.credentials_secret_name.clone()),
                ..Default::default()
            }),
            ..Default::default()
        });
        volume_mounts.push(VolumeMount {
            name: S3_VOLUME.to_string(),
            mount_path: S3_MOUNT_DIR.to_string(),
            read_only: Some(true),
            ..Default::default()
        });
    }

    let container = Container {
        name: "animusd".to_string(),
        image: Some(spec.image_or_default().to_string()),
        command: Some(vec![
            "/bin/sh".to_string(),
            format!("{CONFIG_MOUNT_DIR}/{ENTRYPOINT_FILE_NAME}"),
        ]),
        env: Some(vec![EnvVar {
            name: "RUST_LOG".to_string(),
            value: Some(DEFAULT_RUST_LOG.to_string()),
            ..Default::default()
        }]),
        volume_mounts: Some(volume_mounts),
        resources: spec
            .resources
            .clone()
            .or(Some(ResourceRequirements::default())),
        readiness_probe: Some(admin_probe(admin_port, tls_enabled, |p| {
            p.period_seconds = Some(READINESS_PERIOD_SECS);
            p.failure_threshold = Some(READINESS_FAILURE_THRESHOLD);
        })),
        liveness_probe: Some(admin_probe(admin_port, tls_enabled, |p| {
            p.initial_delay_seconds = Some(LIVENESS_INITIAL_DELAY_SECS);
            p.period_seconds = Some(LIVENESS_PERIOD_SECS);
            p.failure_threshold = Some(LIVENESS_FAILURE_THRESHOLD);
        })),
        ..Default::default()
    };

    StatefulSet {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(ns.to_string()),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            service_name: Some(internal_service_name(name)),
            replicas: Some(spec.nodes),
            pod_management_policy: Some("Parallel".to_string()),
            selector: LabelSelector {
                match_labels: Some(selector.clone()),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    annotations: Some(BTreeMap::from([(
                        CONFIG_HASH_ANNOTATION.to_string(),
                        restart_relevant_config_hash(spec),
                    )])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container],
                    volumes: Some(volumes),
                    termination_grace_period_seconds: Some(TERMINATION_GRACE_PERIOD_SECS),
                    ..Default::default()
                }),
            },
            volume_claim_templates: if volume_claim_templates.is_empty() {
                None
            } else {
                Some(volume_claim_templates)
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::desired::test_support::test_cluster;

    fn container(sts: &StatefulSet) -> Container {
        sts.spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0]
            .clone()
    }

    /// The [`CONFIG_HASH_ANNOTATION`] value `build` set on `cluster`'s
    /// generated `StatefulSet`.
    fn config_hash(cluster: &AnimusCluster) -> String {
        build(cluster, &cluster.spec)
            .spec
            .unwrap()
            .template
            .metadata
            .unwrap()
            .annotations
            .unwrap()[CONFIG_HASH_ANNOTATION]
            .clone()
    }

    #[test]
    fn replicas_and_service_name_and_pod_management_policy() {
        let cluster = test_cluster("c", "ns", 5, None);
        let sts = build(&cluster, &cluster.spec);
        let spec = sts.spec.unwrap();
        assert_eq!(spec.replicas, Some(5));
        assert_eq!(spec.service_name.as_deref(), Some("c-internal"));
        assert_eq!(spec.pod_management_policy.as_deref(), Some("Parallel"));
    }

    #[test]
    fn rust_log_defaults_to_info_so_a_stuck_cluster_leaves_a_trail() {
        let cluster = test_cluster("c", "ns", 3, None);
        let sts = build(&cluster, &cluster.spec);
        let c = container(&sts);
        let env = c.env.expect("container sets an env list");
        let rust_log = env
            .iter()
            .find(|e| e.name == "RUST_LOG")
            .expect("RUST_LOG is set");
        assert_eq!(rust_log.value.as_deref(), Some(DEFAULT_RUST_LOG));
    }

    #[test]
    fn probes_target_admin_health_on_admin_port() {
        let cluster = test_cluster("c", "ns", 3, None);
        let sts = build(&cluster, &cluster.spec);
        let c = container(&sts);
        let readiness = c.readiness_probe.unwrap();
        let liveness = c.liveness_probe.unwrap();
        for probe in [&readiness, &liveness] {
            let get = probe.http_get.as_ref().unwrap();
            assert_eq!(get.path.as_deref(), Some("/admin/health"));
            assert_eq!(get.port, IntOrString::Int(14003));
        }
        assert_eq!(readiness.period_seconds, Some(5));
        assert_eq!(readiness.failure_threshold, Some(3));
        assert_eq!(liveness.initial_delay_seconds, Some(30));
        assert_eq!(liveness.period_seconds, Some(10));
        assert_eq!(liveness.failure_threshold, Some(6));
    }

    #[test]
    fn probe_port_tracks_a_custom_base_port() {
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.base_port = Some(20000);
        let sts = build(&cluster, &cluster.spec);
        let c = container(&sts);
        assert_eq!(
            c.readiness_probe.unwrap().http_get.unwrap().port,
            IntOrString::Int(20003)
        );
    }

    #[test]
    fn durable_storage_gets_a_volume_claim_template_not_empty_dir() {
        let cluster = test_cluster("c", "ns", 3, None);
        let sts = build(&cluster, &cluster.spec);
        let spec = sts.spec.unwrap();
        let vcts = spec.volume_claim_templates.expect("vct present");
        assert_eq!(vcts.len(), 1);
        assert_eq!(vcts[0].metadata.name.as_deref(), Some("data"));
        let pvc_spec = vcts[0].spec.as_ref().unwrap();
        assert_eq!(
            pvc_spec
                .resources
                .as_ref()
                .unwrap()
                .requests
                .as_ref()
                .unwrap()["storage"],
            Quantity("10Gi".to_string())
        );
        let pod_spec = spec.template.spec.as_ref().unwrap();
        assert!(
            !pod_spec
                .volumes
                .as_ref()
                .unwrap()
                .iter()
                .any(|v| v.name == "data"),
            "durable storage must not also define a `data` emptyDir volume"
        );
    }

    #[test]
    fn ephemeral_storage_uses_empty_dir_and_no_volume_claim_template() {
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.storage.ephemeral = Some(true);
        let sts = build(&cluster, &cluster.spec);
        let spec = sts.spec.unwrap();
        assert!(spec.volume_claim_templates.is_none());
        let pod_spec = spec.template.spec.unwrap();
        let data_vol = pod_spec
            .volumes
            .unwrap()
            .into_iter()
            .find(|v| v.name == "data")
            .expect("emptyDir data volume present");
        assert!(data_vol.empty_dir.is_some());
    }

    #[test]
    fn custom_storage_class_name_passes_through() {
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.storage.storage_class_name = Some("fast-ssd".to_string());
        let sts = build(&cluster, &cluster.spec);
        let vcts = sts.spec.unwrap().volume_claim_templates.unwrap();
        assert_eq!(
            vcts[0].spec.as_ref().unwrap().storage_class_name.as_deref(),
            Some("fast-ssd")
        );
    }

    #[test]
    fn command_execs_entrypoint_via_sh() {
        let cluster = test_cluster("c", "ns", 3, None);
        let sts = build(&cluster, &cluster.spec);
        let c = container(&sts);
        assert_eq!(
            c.command,
            Some(vec![
                "/bin/sh".to_string(),
                "/etc/animus/entrypoint.sh".to_string()
            ])
        );
    }

    #[test]
    fn config_volume_mounted_read_only_at_etc_animus() {
        let cluster = test_cluster("c", "ns", 3, None);
        let sts = build(&cluster, &cluster.spec);
        let c = container(&sts);
        let mount = c
            .volume_mounts
            .unwrap()
            .into_iter()
            .find(|m| m.name == "config")
            .unwrap();
        assert_eq!(mount.mount_path, "/etc/animus");
        assert_eq!(mount.read_only, Some(true));
    }

    #[test]
    fn dynamo_auth_secret_mounted_when_named() {
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.dynamo_auth_secret_name = Some("my-secret".to_string());
        let sts = build(&cluster, &cluster.spec);
        let pod_spec = sts.spec.unwrap().template.spec.unwrap();
        let vol = pod_spec
            .volumes
            .unwrap()
            .into_iter()
            .find(|v| v.name == "dynamo-auth")
            .expect("dynamo-auth volume present");
        assert_eq!(
            vol.secret.unwrap().secret_name.as_deref(),
            Some("my-secret")
        );
        let mount = pod_spec.containers[0]
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|m| m.name == "dynamo-auth")
            .expect("dynamo-auth mount present");
        assert_eq!(mount.mount_path, "/etc/animus/dynamo-auth");
        assert_eq!(mount.read_only, Some(true));
    }

    #[test]
    fn no_dynamo_auth_volume_when_secret_unset() {
        let cluster = test_cluster("c", "ns", 3, None);
        let sts = build(&cluster, &cluster.spec);
        let pod_spec = sts.spec.unwrap().template.spec.unwrap();
        assert!(
            !pod_spec
                .volumes
                .unwrap()
                .iter()
                .any(|v| v.name == "dynamo-auth")
        );
    }

    #[test]
    fn tls_secret_mounted_when_tls_set_secret_name_shape() {
        use crate::crd::TlsSpec;
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.tls = Some(TlsSpec {
            secret_name: Some("preexisting-tls".to_string()),
            cert_manager: None,
        });
        let sts = build(&cluster, &cluster.spec);
        let pod_spec = sts.spec.unwrap().template.spec.unwrap();
        let vol = pod_spec
            .volumes
            .unwrap()
            .into_iter()
            .find(|v| v.name == "tls")
            .expect("tls volume present");
        assert_eq!(
            vol.secret.unwrap().secret_name.as_deref(),
            Some("preexisting-tls")
        );
        let mount = pod_spec.containers[0]
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|m| m.name == "tls")
            .expect("tls mount present");
        assert_eq!(mount.mount_path, "/etc/animus/tls");
        assert_eq!(mount.read_only, Some(true));
    }

    #[test]
    fn tls_secret_mounted_at_the_default_name_for_cert_manager_shape() {
        use crate::crd::{CertManagerSpec, IssuerRef, TlsSpec};
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.tls = Some(TlsSpec {
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
        });
        let sts = build(&cluster, &cluster.spec);
        let pod_spec = sts.spec.unwrap().template.spec.unwrap();
        let vol = pod_spec
            .volumes
            .unwrap()
            .into_iter()
            .find(|v| v.name == "tls")
            .expect("tls volume present");
        assert_eq!(vol.secret.unwrap().secret_name.as_deref(), Some("c-tls"));
    }

    #[test]
    fn no_tls_volume_when_tls_unset() {
        let cluster = test_cluster("c", "ns", 3, None);
        let sts = build(&cluster, &cluster.spec);
        let pod_spec = sts.spec.unwrap().template.spec.unwrap();
        assert!(!pod_spec.volumes.unwrap().iter().any(|v| v.name == "tls"));
    }

    // --- `s3` (S-04 PR 3) --------------------------------------------------

    fn test_s3_spec() -> crate::crd::S3StoreSpec {
        crate::crd::S3StoreSpec {
            backup_store: Some("s3://bucket?endpoint=https://s3.example.com".to_string()),
            segment_store: None,
            credentials_secret_name: "my-s3-creds".to_string(),
            allow_insecure_http: false,
            egress_cidrs: crate::crd::S3StoreSpec::default_egress_cidrs(),
        }
    }

    #[test]
    fn s3_credentials_secret_mounted_read_only_when_s3_set() {
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.s3 = Some(test_s3_spec());
        let sts = build(&cluster, &cluster.spec);
        let pod_spec = sts.spec.unwrap().template.spec.unwrap();
        let vol = pod_spec
            .volumes
            .unwrap()
            .into_iter()
            .find(|v| v.name == "s3")
            .expect("s3 volume present");
        assert_eq!(
            vol.secret.unwrap().secret_name.as_deref(),
            Some("my-s3-creds")
        );
        let mount = pod_spec.containers[0]
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|m| m.name == "s3")
            .expect("s3 mount present");
        assert_eq!(mount.mount_path, "/etc/animus/s3");
        assert_eq!(mount.read_only, Some(true));
    }

    #[test]
    fn no_s3_volume_when_s3_unset() {
        let cluster = test_cluster("c", "ns", 3, None);
        let sts = build(&cluster, &cluster.spec);
        let pod_spec = sts.spec.unwrap().template.spec.unwrap();
        assert!(!pod_spec.volumes.unwrap().iter().any(|v| v.name == "s3"));
    }

    #[test]
    fn probes_use_https_scheme_when_tls_set_secret_name_shape() {
        use crate::crd::TlsSpec;
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.tls = Some(TlsSpec {
            secret_name: Some("preexisting-tls".to_string()),
            cert_manager: None,
        });
        let sts = build(&cluster, &cluster.spec);
        let c = container(&sts);
        let readiness = c.readiness_probe.unwrap();
        let liveness = c.liveness_probe.unwrap();
        for probe in [&readiness, &liveness] {
            let get = probe.http_get.as_ref().unwrap();
            assert_eq!(
                get.scheme.as_deref(),
                Some("HTTPS"),
                "kubelet probe must speak TLS to a TLS-only admin listener"
            );
        }
    }

    #[test]
    fn probes_use_https_scheme_when_tls_set_cert_manager_shape() {
        use crate::crd::{CertManagerSpec, IssuerRef, TlsSpec};
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.tls = Some(TlsSpec {
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
        });
        let sts = build(&cluster, &cluster.spec);
        let c = container(&sts);
        let readiness = c.readiness_probe.unwrap();
        let liveness = c.liveness_probe.unwrap();
        for probe in [&readiness, &liveness] {
            let get = probe.http_get.as_ref().unwrap();
            assert_eq!(get.scheme.as_deref(), Some("HTTPS"));
        }
    }

    #[test]
    fn probes_have_no_scheme_override_when_tls_unset() {
        let cluster = test_cluster("c", "ns", 3, None);
        let sts = build(&cluster, &cluster.spec);
        let c = container(&sts);
        let readiness = c.readiness_probe.unwrap();
        let liveness = c.liveness_probe.unwrap();
        for probe in [&readiness, &liveness] {
            let get = probe.http_get.as_ref().unwrap();
            assert_eq!(get.scheme, None, "plain HTTP probe leaves scheme unset");
        }
    }

    #[test]
    fn termination_grace_period_is_generous() {
        let cluster = test_cluster("c", "ns", 3, None);
        let sts = build(&cluster, &cluster.spec);
        let pod_spec = sts.spec.unwrap().template.spec.unwrap();
        assert_eq!(pod_spec.termination_grace_period_seconds, Some(90));
    }

    #[test]
    fn owner_reference_present() {
        let cluster = test_cluster("c", "ns", 3, None);
        let sts = build(&cluster, &cluster.spec);
        let owners = sts.metadata.owner_references.unwrap();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].controller, Some(true));
    }

    // --- config-hash restart annotation (S-07d groundwork) ----------------

    #[test]
    fn pod_template_carries_a_config_hash_annotation() {
        let cluster = test_cluster("c", "ns", 3, None);
        let sts = build(&cluster, &cluster.spec);
        let annotations = sts
            .spec
            .unwrap()
            .template
            .metadata
            .unwrap()
            .annotations
            .expect("pod template carries annotations");
        assert!(
            annotations.contains_key(CONFIG_HASH_ANNOTATION),
            "{annotations:?}"
        );
    }

    #[test]
    fn config_hash_is_stable_for_an_unchanged_spec() {
        let cluster = test_cluster("c", "ns", 3, None);
        let a = build(&cluster, &cluster.spec)
            .spec
            .unwrap()
            .template
            .metadata
            .unwrap()
            .annotations
            .unwrap()[CONFIG_HASH_ANNOTATION]
            .clone();
        let b = build(&cluster, &cluster.spec)
            .spec
            .unwrap()
            .template
            .metadata
            .unwrap()
            .annotations
            .unwrap()[CONFIG_HASH_ANNOTATION]
            .clone();
        assert_eq!(a, b, "the hash must be a pure function of (cluster, spec)");
    }

    #[test]
    fn config_hash_changes_when_control_nodes_changes() {
        // The exact case S-07d cares about: a `spec.controlNodes` change
        // flips role assignment in the generated `cluster.json`/
        // `entrypoint.sh`, which must change this hash so the affected
        // pods actually restart.
        let mut cluster = test_cluster("c", "ns", 5, Some(3));
        let before = build(&cluster, &cluster.spec)
            .spec
            .unwrap()
            .template
            .metadata
            .unwrap()
            .annotations
            .unwrap()[CONFIG_HASH_ANNOTATION]
            .clone();
        cluster.spec.control_nodes = Some(5);
        let after = build(&cluster, &cluster.spec)
            .spec
            .unwrap()
            .template
            .metadata
            .unwrap()
            .annotations
            .unwrap()[CONFIG_HASH_ANNOTATION]
            .clone();
        assert_ne!(before, after);
    }

    #[test]
    fn config_hash_is_unchanged_by_a_nodes_only_scale_up() {
        // The exact regression this fix closes: `scripts/e2e-kind.sh`'s
        // scale phase (3 -> 4 nodes, `controlNodes` left at its default)
        // must not roll the pods already up — a running `animusd` never
        // rereads `cluster.json`'s node list; it learns of the new peer
        // through replicated `Metadata` (ADR 0030).
        let before = test_cluster("c", "ns", 3, None);
        let after = test_cluster("c", "ns", 4, None);
        assert_eq!(config_hash(&before), config_hash(&after));
    }

    #[test]
    fn config_hash_is_unchanged_by_a_nodes_only_scale_down() {
        let before = test_cluster("c", "ns", 4, None);
        let after = test_cluster("c", "ns", 3, None);
        assert_eq!(config_hash(&before), config_hash(&after));
    }

    #[test]
    fn config_hash_changes_when_control_nodes_moves_from_3_to_4() {
        let before = test_cluster("c", "ns", 5, Some(3));
        let after = test_cluster("c", "ns", 5, Some(4));
        assert_ne!(config_hash(&before), config_hash(&after));
    }

    #[test]
    fn config_hash_changes_when_spec_s3_backup_store_is_added() {
        let before = test_cluster("c", "ns", 3, None);
        let mut after = before.clone();
        after.spec.s3 = Some(test_s3_spec());
        assert_ne!(config_hash(&before), config_hash(&after));
    }

    #[test]
    fn config_hash_changes_when_top_level_backup_store_is_added() {
        let before = test_cluster("c", "ns", 3, None);
        let mut after = before.clone();
        after.spec.backup_store = Some("fs:/var/lib/animus/backups".to_string());
        assert_ne!(config_hash(&before), config_hash(&after));
    }

    #[test]
    fn config_hash_changes_when_tls_is_toggled() {
        use crate::crd::TlsSpec;
        let before = test_cluster("c", "ns", 3, None);
        let mut after = before.clone();
        after.spec.tls = Some(TlsSpec {
            secret_name: Some("my-tls".to_string()),
            cert_manager: None,
        });
        assert_ne!(config_hash(&before), config_hash(&after));
    }

    #[test]
    fn config_hash_pinned_for_a_fixed_fixture() {
        // Pins the hash *value*, not just its stability, so a change to the
        // hash function itself (algorithm, projection shape, or field
        // order) shows up as an explicit, reviewable diff here rather than
        // silently rolling every cluster's pods on the next operator
        // release. If this test needs to change, the change is deliberate
        // — update the literal and say so in the commit body.
        let cluster = test_cluster("c", "ns", 3, None);
        assert_eq!(config_hash(&cluster), "f5c65fc10dcc4e1c");
    }

    #[test]
    fn resources_pass_through_when_set() {
        use k8s_openapi::apimachinery::pkg::api::resource::Quantity as Q;
        let mut cluster = test_cluster("c", "ns", 3, None);
        let mut limits = BTreeMap::new();
        limits.insert("cpu".to_string(), Q("2".to_string()));
        cluster.spec.resources = Some(ResourceRequirements {
            limits: Some(limits),
            ..Default::default()
        });
        let sts = build(&cluster, &cluster.spec);
        let c = container(&sts);
        assert_eq!(
            c.resources.unwrap().limits.unwrap()["cpu"],
            Q("2".to_string())
        );
    }
}
