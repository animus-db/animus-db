//! The `StatefulSet` builder: one pod per node ordinal, running
//! `entrypoint.sh` off the cluster `ConfigMap`, probed on the admin port's
//! `GET /admin/health`. The probes' scheme follows `spec.tls`: HTTP when
//! unset, HTTPS (unverified, as the kubelet itself does not check the
//! server certificate) when set — admin is server-only TLS (ADR 0064), so
//! a plaintext probe against a TLS-only listener fails the handshake on
//! the server side every probe period and the pod never goes Ready.

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

use super::cluster_config::{
    CONFIG_MOUNT_DIR, DATA_DIR, DYNAMO_AUTH_MOUNT_DIR, ENTRYPOINT_FILE_NAME, TLS_MOUNT_DIR,
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

const CONFIG_VOLUME: &str = "config";
const DATA_VOLUME: &str = "data";
const DYNAMO_AUTH_VOLUME: &str = "dynamo-auth";
const TLS_VOLUME: &str = "tls";

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
