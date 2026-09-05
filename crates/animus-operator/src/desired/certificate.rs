//! The cert-manager `Certificate` builder (ADR 0064 commit 3) — only built
//! when `spec.tls.certManager` is set; the `secretName` shape (a
//! pre-existing `Secret` an operator user issued by hand) needs no object
//! of this crate's own, since the operator only *reads* that Secret.
//!
//! `cert-manager.io/v1` is not a `k8s-openapi` type (that crate only ships
//! the built-in Kubernetes API groups), so this builder produces a
//! `kube::core::DynamicObject` instead of a typed struct — the same
//! approach `k8s-openapi`-less CRD consumers use generally; see `kube`'s
//! own `dynamic_watcher`/`crd_derive` examples. The `Issuer`/`ClusterIssuer`
//! itself is only *referenced* (`spec.issuerRef`), never created — cert
//! issuance policy (ACME account, CA secret, …) is the cluster operator's
//! own concern, out of scope for this crate exactly as ADR 0064 Decision 6
//! records for the whole TLS milestone ("a managed/rotated CA... no
//! CA-issuance logic lives in this codebase").

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::core::{ApiResource, DynamicObject, GroupVersionKind};
use serde_json::json;

use super::{client_service_name, common_labels, internal_service_name, owner_reference, pod_fqdn};
use crate::crd::{AnimusCluster, AnimusClusterSpec};

/// `cert-manager.io/v1` `Certificate`'s [`ApiResource`] — used both to build
/// the [`DynamicObject`] below and, by `crate::cluster_api`, to address it
/// through `kube::Api::namespaced_with`.
#[must_use]
pub fn api_resource() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind {
        group: "cert-manager.io".to_string(),
        version: "v1".to_string(),
        kind: "Certificate".to_string(),
    })
}

/// The `Certificate`'s own name for cluster `name` — also the default
/// `Secret` name every shape resolves to when `certManager` is set (see
/// `TlsSpec::secret_name_or_default`).
#[must_use]
pub fn certificate_name(name: &str) -> String {
    format!("{name}-tls")
}

/// Every DNS name a peer might dial a pod of this cluster by, so the
/// issued certificate's SAN list covers every string `animusd`'s own peer
/// book / a client could present during a TLS handshake (ADR 0064 Decision
/// 7): each pod's own stable per-ordinal FQDN (what `RoleAddrs.
/// advertise_host` carries and the internal wire/intra relays dial), the
/// headless internal `Service`'s own zone name (short + FQDN — some
/// in-cluster callers resolve the bare `Service` name), and the
/// client-facing `dynamo` `Service` name (short + FQDN — what a DynamoDB
/// client inside the cluster, or a `kubectl port-forward` combined with
/// `--resolve`, dials).
#[must_use]
pub fn dns_names(name: &str, ns: &str, nodes: i32) -> Vec<String> {
    let mut names: Vec<String> = (0..nodes).map(|i| pod_fqdn(name, ns, i)).collect();
    let internal = internal_service_name(name);
    names.push(internal.clone());
    names.push(format!("{internal}.{ns}"));
    names.push(format!("{internal}.{ns}.svc.cluster.local"));
    let client = client_service_name(name);
    names.push(client.clone());
    names.push(format!("{client}.{ns}"));
    names.push(format!("{client}.{ns}.svc.cluster.local"));
    names
}

/// Build the `Certificate` for `cluster`, or `None` when `spec.tls` isn't
/// the `certManager` shape (nothing to create for a pre-existing
/// `secretName`, or when TLS is off entirely).
#[must_use]
pub fn build(cluster: &AnimusCluster, spec: &AnimusClusterSpec) -> Option<DynamicObject> {
    let cert_manager = spec.tls.as_ref()?.cert_manager.as_ref()?;
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

    let names = dns_names(name, ns, spec.nodes);
    let mut issuer_ref = json!({
        "name": cert_manager.issuer_ref.name,
        "kind": cert_manager.issuer_ref.kind,
    });
    if let Some(group) = &cert_manager.issuer_ref.group {
        issuer_ref["group"] = json!(group);
    }
    let mut cert_spec = json!({
        "secretName": certificate_name(name),
        "commonName": names[0],
        "dnsNames": names,
        "issuerRef": issuer_ref,
        "usages": ["server auth", "client auth"],
        "isCA": false,
    });
    if let Some(duration) = &cert_manager.duration {
        cert_spec["duration"] = json!(duration);
    }
    if let Some(renew_before) = &cert_manager.renew_before {
        cert_spec["renewBefore"] = json!(renew_before);
    }

    let resource = api_resource();
    let mut obj = DynamicObject::new(&certificate_name(name), &resource).data(json!({
        "spec": cert_spec,
    }));
    obj.metadata = ObjectMeta {
        name: Some(certificate_name(name)),
        namespace: Some(ns.to_string()),
        labels: Some(common_labels(name)),
        owner_references: Some(vec![owner_reference(cluster)]),
        ..Default::default()
    };
    Some(obj)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{CertManagerSpec, IssuerRef, TlsSpec};
    use crate::desired::test_support::test_cluster;

    fn cert_manager_cluster(kind: &str, group: Option<&str>) -> AnimusCluster {
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.tls = Some(TlsSpec {
            secret_name: None,
            cert_manager: Some(CertManagerSpec {
                issuer_ref: IssuerRef {
                    name: "my-issuer".to_string(),
                    kind: kind.to_string(),
                    group: group.map(str::to_string),
                },
                duration: None,
                renew_before: None,
            }),
        });
        cluster
    }

    #[test]
    fn no_tls_spec_builds_nothing() {
        let cluster = test_cluster("c", "ns", 3, None);
        assert!(build(&cluster, &cluster.spec).is_none());
    }

    #[test]
    fn secret_name_shape_builds_nothing() {
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.tls = Some(TlsSpec {
            secret_name: Some("preexisting".to_string()),
            cert_manager: None,
        });
        assert!(build(&cluster, &cluster.spec).is_none());
    }

    #[test]
    fn cert_manager_shape_builds_a_certificate_with_the_right_gvk_and_name() {
        let cluster = cert_manager_cluster("ClusterIssuer", None);
        let obj = build(&cluster, &cluster.spec).expect("certManager shape builds an object");
        let types = obj.types.expect("DynamicObject carries TypeMeta");
        assert_eq!(types.api_version, "cert-manager.io/v1");
        assert_eq!(types.kind, "Certificate");
        assert_eq!(obj.metadata.name.as_deref(), Some("c-tls"));
        assert_eq!(obj.metadata.namespace.as_deref(), Some("ns"));
        assert_eq!(obj.data["spec"]["secretName"], "c-tls");
    }

    #[test]
    fn dns_names_cover_every_pod_plus_both_services_short_and_fqdn() {
        let names = dns_names("c", "ns", 3);
        assert_eq!(
            names,
            vec![
                "c-0.c-internal.ns.svc.cluster.local",
                "c-1.c-internal.ns.svc.cluster.local",
                "c-2.c-internal.ns.svc.cluster.local",
                "c-internal",
                "c-internal.ns",
                "c-internal.ns.svc.cluster.local",
                "c-dynamo",
                "c-dynamo.ns",
                "c-dynamo.ns.svc.cluster.local",
            ]
        );
    }

    #[test]
    fn issuer_ref_and_usages_and_is_ca() {
        let cluster = cert_manager_cluster("ClusterIssuer", Some("cert-manager.io"));
        let obj = build(&cluster, &cluster.spec).unwrap();
        assert_eq!(
            obj.data["spec"]["issuerRef"],
            json!({ "name": "my-issuer", "kind": "ClusterIssuer", "group": "cert-manager.io" })
        );
        assert_eq!(
            obj.data["spec"]["usages"],
            json!(["server auth", "client auth"])
        );
        assert_eq!(obj.data["spec"]["isCA"], json!(false));
    }

    #[test]
    fn duration_and_renew_before_pass_through_when_set() {
        let mut cluster = cert_manager_cluster("Issuer", None);
        if let Some(tls) = cluster.spec.tls.as_mut()
            && let Some(cm) = tls.cert_manager.as_mut()
        {
            cm.duration = Some("2160h".to_string());
            cm.renew_before = Some("360h".to_string());
        }
        let obj = build(&cluster, &cluster.spec).unwrap();
        assert_eq!(obj.data["spec"]["duration"], "2160h");
        assert_eq!(obj.data["spec"]["renewBefore"], "360h");
    }

    #[test]
    fn duration_and_renew_before_absent_when_unset() {
        let cluster = cert_manager_cluster("Issuer", None);
        let obj = build(&cluster, &cluster.spec).unwrap();
        assert!(obj.data["spec"].get("duration").is_none());
        assert!(obj.data["spec"].get("renewBefore").is_none());
    }

    #[test]
    fn owner_reference_present() {
        let cluster = cert_manager_cluster("Issuer", None);
        let obj = build(&cluster, &cluster.spec).unwrap();
        let owners = obj.metadata.owner_references.unwrap();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].controller, Some(true));
    }

    #[test]
    fn common_name_is_the_first_pod_fqdn() {
        let cluster = cert_manager_cluster("Issuer", None);
        let obj = build(&cluster, &cluster.spec).unwrap();
        assert_eq!(
            obj.data["spec"]["commonName"],
            "c-0.c-internal.ns.svc.cluster.local"
        );
    }
}
