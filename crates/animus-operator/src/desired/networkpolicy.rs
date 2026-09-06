//! The `{name}-internal-only` `NetworkPolicy` builder: node-to-node traffic
//! stays inside the cluster's own pods, the admin port additionally accepts
//! the operator, the dynamo port is open to any source, and everything else
//! is denied by default (ADR 0047's Kubernetes deployment intent — see this
//! crate's `CLAUDE.md`).
//!
//! **Unaffected by `spec.tls` (ADR 0064 commit 3).** TLS is a mode each
//! port's own listener can be configured into, not a change to which pods
//! may reach which port at all — the port topology (and therefore this
//! builder's own ingress output) is identical whether or not `spec.tls` is
//! set.
//!
//! **Egress (S-04 PR 3).** Before this PR the generated policy set
//! `policyTypes: [Ingress]` only — Kubernetes then leaves egress
//! **completely unrestricted by omission** (a `NetworkPolicy` naming no
//! `Egress` in `policyTypes` never touches egress traffic at all,
//! regardless of what else it says). `docs/roadmap.md`'s S-04 item named
//! this exactly: "egress unrestricted by omission." Every cluster now also
//! gets an explicit `Egress` section with two baseline rules — intra-
//! cluster (this cluster's own pods, `internal`+`intra` ports only, the
//! two ports node-to-node Raft/RPC traffic actually uses) and DNS to
//! kube-dns (so in-cluster name resolution, including an S3 endpoint's own
//! hostname, keeps working under a default-deny egress policy) — plus a
//! third rule, only when `spec.s3` is set, opening the S3 endpoint's own
//! port(s) to `spec.s3.egressCidrs` (`["0.0.0.0/0"]` by default — see
//! `crd::S3StoreSpec::egress_cidrs`'s own doc for why `NetworkPolicy`
//! itself cannot express a hostname allowlist here). A cluster with no
//! `spec.s3` gets exactly the two baseline rules, nothing more.

use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::api::networking::v1::{
    IPBlock, NetworkPolicy, NetworkPolicyEgressRule, NetworkPolicyIngressRule, NetworkPolicyPeer,
    NetworkPolicyPort, NetworkPolicySpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use super::cluster_config::{PORT_ADMIN, PORT_INTERNAL, PORT_INTRA};
use super::{
    OPERATOR_APP_NAME, common_labels, network_policy_name, owner_reference, selector_labels,
};
use crate::crd::{AnimusCluster, AnimusClusterSpec};
use crate::s3_uri;

/// `kube-dns`'/CoreDNS's own well-known Service-selector label — the
/// standard way to target the DNS pods regardless of which of the two a
/// given cluster runs (both ship with this label for exactly this reason:
/// so a `NetworkPolicy` written against it works either way).
const DNS_POD_LABEL_KEY: &str = "k8s-app";
const DNS_POD_LABEL_VALUE: &str = "kube-dns";
/// The label every namespace carries automatically since Kubernetes 1.22
/// (`--feature-gates=NamespaceDefaultLabelName`, GA and unconditional since
/// then) — a `NetworkPolicy`'s `namespaceSelector` can only match labels,
/// never a namespace's name directly, so this is the standard way to target
/// `kube-system` by name.
const NAMESPACE_NAME_LABEL_KEY: &str = "kubernetes.io/metadata.name";
const KUBE_SYSTEM_NAMESPACE: &str = "kube-system";
const DNS_PORT: i32 = 53;

fn tcp_port(p: i32) -> NetworkPolicyPort {
    NetworkPolicyPort {
        protocol: Some("TCP".to_string()),
        port: Some(IntOrString::Int(p)),
        ..Default::default()
    }
}

fn udp_port(p: i32) -> NetworkPolicyPort {
    NetworkPolicyPort {
        protocol: Some("UDP".to_string()),
        port: Some(IntOrString::Int(p)),
        ..Default::default()
    }
}

fn pod_selector(labels: BTreeMap<String, String>) -> NetworkPolicyPeer {
    NetworkPolicyPeer {
        pod_selector: Some(LabelSelector {
            match_labels: Some(labels),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn ip_block_peer(cidr: &str) -> NetworkPolicyPeer {
    NetworkPolicyPeer {
        ip_block: Some(IPBlock {
            cidr: cidr.to_string(),
            except: None,
        }),
        ..Default::default()
    }
}

/// The peer matching `kube-system`'s own `kube-dns`/CoreDNS pods —
/// `namespaceSelector` + `podSelector` together (a `NetworkPolicyPeer` ANDs
/// them when both are set), so this only ever matches the DNS pods
/// specifically, not every pod in `kube-system`.
fn dns_peer() -> NetworkPolicyPeer {
    let mut ns_labels = BTreeMap::new();
    ns_labels.insert(
        NAMESPACE_NAME_LABEL_KEY.to_string(),
        KUBE_SYSTEM_NAMESPACE.to_string(),
    );
    let mut pod_labels = BTreeMap::new();
    pod_labels.insert(
        DNS_POD_LABEL_KEY.to_string(),
        DNS_POD_LABEL_VALUE.to_string(),
    );
    NetworkPolicyPeer {
        namespace_selector: Some(LabelSelector {
            match_labels: Some(ns_labels),
            ..Default::default()
        }),
        pod_selector: Some(LabelSelector {
            match_labels: Some(pod_labels),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The distinct port(s) `spec.s3`'s configured store URIs' `endpoint=`
/// values resolve to — deduplicated (`backupStore`/`segmentStore`
/// typically share one endpoint, but need not). A URI that somehow fails
/// [`s3_uri::parse`] here contributes no port rather than panicking: by the
/// time `build` is ever called with this `spec.s3` in production,
/// `crate::controller::reconcile` has already validated it
/// (`crd::S3StoreSpec::validate`) or stripped the field entirely — this
/// builder stays a total function of its input regardless.
fn s3_endpoint_ports(s3: &crate::crd::S3StoreSpec) -> BTreeSet<i32> {
    [s3.backup_store.as_deref(), s3.segment_store.as_deref()]
        .into_iter()
        .flatten()
        .filter_map(|uri| s3_uri::parse(uri).ok())
        .filter_map(|info| info.port)
        .collect()
}

/// Build the `NetworkPolicy` for `cluster`.
#[must_use]
pub fn build(cluster: &AnimusCluster, spec: &AnimusClusterSpec) -> NetworkPolicy {
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
    let admin_port = spec.base_port_or_default() + PORT_ADMIN;
    let dynamo_port = spec.base_port_or_default() + super::cluster_config::PORT_DYNAMO;
    let internal_port = spec.base_port_or_default() + PORT_INTERNAL;
    let intra_port = spec.base_port_or_default() + PORT_INTRA;
    let own_pods = selector_labels(name);

    let mut operator_labels = BTreeMap::new();
    operator_labels.insert(
        "app.kubernetes.io/name".to_string(),
        OPERATOR_APP_NAME.to_string(),
    );

    let ingress = vec![
        // Every port, from the cluster's own pods only (node-to-node
        // traffic: control/data Raft, intra RPC, and any in-cluster caller
        // of the client/admin/console ports that happens to also be one of
        // this cluster's own pods).
        NetworkPolicyIngressRule {
            from: Some(vec![pod_selector(own_pods.clone())]),
            ports: None,
        },
        // The admin port, from the operator's own pods (health/status
        // polling and the scale-down drain sequence — see
        // `crate::controller`).
        NetworkPolicyIngressRule {
            from: Some(vec![pod_selector(operator_labels)]),
            ports: Some(vec![tcp_port(admin_port)]),
        },
        // The dynamo (client-facing DynamoDB wire) port, open to any
        // source — the one port this deployment shape means to expose
        // outside the cluster (ADR 0047).
        NetworkPolicyIngressRule {
            from: None,
            ports: Some(vec![tcp_port(dynamo_port)]),
        },
    ];

    let mut egress = vec![
        // Intra-cluster: this cluster's own pods, on the two ports
        // node-to-node traffic actually uses (control-plane `internal` +
        // data-plane `intra` Raft/RPC) — not every port ingress rule 1
        // above allows inbound, since a pod never needs to *dial out* to
        // its peers' client/dynamo/admin/console ports.
        NetworkPolicyEgressRule {
            to: Some(vec![pod_selector(own_pods.clone())]),
            ports: Some(vec![tcp_port(internal_port), tcp_port(intra_port)]),
        },
        // DNS to kube-dns/CoreDNS — required for in-cluster name
        // resolution (including an S3 endpoint's own hostname, see below)
        // to keep working once egress is no longer wide open by omission.
        NetworkPolicyEgressRule {
            to: Some(vec![dns_peer()]),
            ports: Some(vec![udp_port(DNS_PORT), tcp_port(DNS_PORT)]),
        },
    ];

    // Only when `spec.s3` is set: egress to the configured store(s)'
    // endpoint port(s), scoped to `spec.s3.egressCidrs`
    // (`["0.0.0.0/0"]` by default — narrower CIDRs are the operator's own
    // responsibility, see `crd::S3StoreSpec::egress_cidrs`'s own doc for
    // why this builder can't derive them itself). Absent entirely when
    // `spec.s3` is `None` — no S3 egress rule at all in that case.
    if let Some(s3) = &spec.s3 {
        let ports = s3_endpoint_ports(s3);
        if !ports.is_empty() {
            egress.push(NetworkPolicyEgressRule {
                to: Some(s3.egress_cidrs.iter().map(|c| ip_block_peer(c)).collect()),
                ports: Some(ports.into_iter().map(tcp_port).collect()),
            });
        }
    }

    NetworkPolicy {
        metadata: ObjectMeta {
            name: Some(network_policy_name(name)),
            namespace: Some(ns.to_string()),
            labels: Some(common_labels(name)),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(NetworkPolicySpec {
            pod_selector: Some(LabelSelector {
                match_labels: Some(own_pods),
                ..Default::default()
            }),
            policy_types: Some(vec!["Ingress".to_string(), "Egress".to_string()]),
            ingress: Some(ingress),
            egress: Some(egress),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::desired::test_support::test_cluster;

    #[test]
    fn selector_matches_the_clusters_own_pods() {
        let cluster = test_cluster("c", "ns", 3, None);
        let np = build(&cluster, &cluster.spec);
        let spec = np.spec.unwrap();
        assert_eq!(
            spec.pod_selector.unwrap().match_labels,
            Some(selector_labels("c"))
        );
        assert_eq!(
            spec.policy_types,
            Some(vec!["Ingress".to_string(), "Egress".to_string()])
        );
    }

    #[test]
    fn three_ingress_rules_in_order_all_ports_admin_dynamo() {
        let cluster = test_cluster("c", "ns", 3, None);
        let np = build(&cluster, &cluster.spec);
        let rules = np.spec.unwrap().ingress.unwrap();
        assert_eq!(rules.len(), 3);

        // Rule 1: from own pods, every port (no `ports` restriction).
        assert!(rules[0].ports.is_none());
        let from0 = rules[0].from.as_ref().unwrap();
        assert_eq!(
            from0[0].pod_selector.as_ref().unwrap().match_labels,
            Some(selector_labels("c"))
        );

        // Rule 2: from the operator, admin port only.
        let from1 = rules[1].from.as_ref().unwrap();
        let operator_sel = from1[0]
            .pod_selector
            .as_ref()
            .unwrap()
            .match_labels
            .as_ref()
            .unwrap();
        assert_eq!(
            operator_sel
                .get("app.kubernetes.io/name")
                .map(String::as_str),
            Some("animus-operator")
        );
        assert_eq!(
            rules[1].ports.as_ref().unwrap()[0].port,
            Some(IntOrString::Int(14003))
        );

        // Rule 3: dynamo port, no `from` restriction (open to any source).
        assert!(rules[2].from.is_none());
        assert_eq!(
            rules[2].ports.as_ref().unwrap()[0].port,
            Some(IntOrString::Int(14002))
        );
    }

    #[test]
    fn ports_track_a_custom_base_port() {
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.base_port = Some(20000);
        let np = build(&cluster, &cluster.spec);
        let rules = np.spec.unwrap().ingress.unwrap();
        assert_eq!(
            rules[1].ports.as_ref().unwrap()[0].port,
            Some(IntOrString::Int(20003))
        );
        assert_eq!(
            rules[2].ports.as_ref().unwrap()[0].port,
            Some(IntOrString::Int(20002))
        );
    }

    // --- egress (S-04 PR 3) ------------------------------------------------

    fn test_s3_spec(backup: &str, egress_cidrs: Vec<&str>) -> crate::crd::S3StoreSpec {
        crate::crd::S3StoreSpec {
            backup_store: Some(backup.to_string()),
            segment_store: None,
            credentials_secret_name: "my-s3-creds".to_string(),
            allow_insecure_http: true,
            egress_cidrs: egress_cidrs.into_iter().map(str::to_string).collect(),
        }
    }

    #[test]
    fn baseline_egress_has_two_rules_intra_and_dns_when_s3_unset() {
        let cluster = test_cluster("c", "ns", 3, None);
        let np = build(&cluster, &cluster.spec);
        let egress = np.spec.unwrap().egress.unwrap();
        assert_eq!(egress.len(), 2, "expected exactly the two baseline rules");

        // Rule 1: this cluster's own pods, internal (14000) + intra (14004)
        // ports only.
        let to0 = egress[0].to.as_ref().unwrap();
        assert_eq!(
            to0[0].pod_selector.as_ref().unwrap().match_labels,
            Some(selector_labels("c"))
        );
        let ports0: Vec<_> = egress[0]
            .ports
            .as_ref()
            .unwrap()
            .iter()
            .map(|p| p.port.clone())
            .collect();
        assert_eq!(
            ports0,
            vec![Some(IntOrString::Int(14000)), Some(IntOrString::Int(14004))]
        );

        // Rule 2: DNS to kube-system's kube-dns, UDP+TCP 53.
        let to1 = egress[1].to.as_ref().unwrap();
        let ns_sel = to1[0].namespace_selector.as_ref().unwrap();
        assert_eq!(
            ns_sel
                .match_labels
                .as_ref()
                .unwrap()
                .get("kubernetes.io/metadata.name"),
            Some(&"kube-system".to_string())
        );
        let pod_sel = to1[0].pod_selector.as_ref().unwrap();
        assert_eq!(
            pod_sel.match_labels.as_ref().unwrap().get("k8s-app"),
            Some(&"kube-dns".to_string())
        );
        let protocols: Vec<_> = egress[1]
            .ports
            .as_ref()
            .unwrap()
            .iter()
            .map(|p| (p.protocol.clone(), p.port.clone()))
            .collect();
        assert_eq!(
            protocols,
            vec![
                (Some("UDP".to_string()), Some(IntOrString::Int(53))),
                (Some("TCP".to_string()), Some(IntOrString::Int(53))),
            ]
        );
    }

    #[test]
    fn s3_egress_rule_absent_when_spec_s3_is_none() {
        let cluster = test_cluster("c", "ns", 3, None);
        let np = build(&cluster, &cluster.spec);
        assert_eq!(np.spec.unwrap().egress.unwrap().len(), 2);
    }

    #[test]
    fn s3_egress_rule_present_with_default_cidr_when_spec_s3_is_set() {
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.s3 = Some(test_s3_spec(
            "s3://bucket?endpoint=https://s3.example.com",
            vec!["0.0.0.0/0"],
        ));
        let np = build(&cluster, &cluster.spec);
        let egress = np.spec.unwrap().egress.unwrap();
        assert_eq!(
            egress.len(),
            3,
            "expected the two baseline rules plus one S3 rule"
        );

        let s3_rule = &egress[2];
        let to = s3_rule.to.as_ref().unwrap();
        assert_eq!(to.len(), 1);
        assert_eq!(to[0].ip_block.as_ref().unwrap().cidr, "0.0.0.0/0");
        assert_eq!(
            s3_rule.ports.as_ref().unwrap()[0].port,
            Some(IntOrString::Int(443)),
            "https endpoint with no explicit port defaults to 443"
        );
    }

    #[test]
    fn s3_egress_rule_uses_the_endpoints_explicit_port() {
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.s3 = Some(test_s3_spec(
            "s3://bucket?endpoint=http://minio.ns.svc:9000&insecure_http=true",
            vec!["10.0.0.0/8"],
        ));
        let np = build(&cluster, &cluster.spec);
        let egress = np.spec.unwrap().egress.unwrap();
        let s3_rule = &egress[2];
        assert_eq!(
            s3_rule.ports.as_ref().unwrap()[0].port,
            Some(IntOrString::Int(9000))
        );
        assert_eq!(
            s3_rule.to.as_ref().unwrap()[0]
                .ip_block
                .as_ref()
                .unwrap()
                .cidr,
            "10.0.0.0/8"
        );
    }

    #[test]
    fn s3_egress_rule_carries_every_configured_cidr() {
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.s3 = Some(test_s3_spec(
            "s3://bucket?endpoint=https://s3.example.com",
            vec!["10.0.0.0/8", "192.168.1.0/24"],
        ));
        let np = build(&cluster, &cluster.spec);
        let egress = np.spec.unwrap().egress.unwrap();
        let cidrs: Vec<_> = egress[2]
            .to
            .as_ref()
            .unwrap()
            .iter()
            .map(|p| p.ip_block.as_ref().unwrap().cidr.clone())
            .collect();
        assert_eq!(cidrs, vec!["10.0.0.0/8", "192.168.1.0/24"]);
    }

    #[test]
    fn s3_egress_rule_dedupes_identical_backup_and_segment_ports() {
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.s3 = Some(crate::crd::S3StoreSpec {
            backup_store: Some("s3://bucket/backups?endpoint=https://s3.example.com".to_string()),
            segment_store: Some("s3://bucket/streams?endpoint=https://s3.example.com".to_string()),
            credentials_secret_name: "my-s3-creds".to_string(),
            allow_insecure_http: false,
            egress_cidrs: crate::crd::S3StoreSpec::default_egress_cidrs(),
        });
        let np = build(&cluster, &cluster.spec);
        let egress = np.spec.unwrap().egress.unwrap();
        assert_eq!(egress.len(), 3);
        assert_eq!(
            egress[2].ports.as_ref().unwrap().len(),
            1,
            "one shared port, not two identical rules"
        );
    }

    #[test]
    fn s3_egress_rule_lists_both_ports_when_stores_use_different_endpoints() {
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.s3 = Some(crate::crd::S3StoreSpec {
            backup_store: Some("s3://bucket/backups?endpoint=https://s3.example.com".to_string()),
            segment_store: Some(
                "s3://bucket/streams?endpoint=http://minio.ns.svc:9000&insecure_http=true"
                    .to_string(),
            ),
            credentials_secret_name: "my-s3-creds".to_string(),
            allow_insecure_http: true,
            egress_cidrs: crate::crd::S3StoreSpec::default_egress_cidrs(),
        });
        let np = build(&cluster, &cluster.spec);
        let egress = np.spec.unwrap().egress.unwrap();
        let ports: Vec<_> = egress[2]
            .ports
            .as_ref()
            .unwrap()
            .iter()
            .map(|p| p.port.clone())
            .collect();
        assert_eq!(
            ports,
            vec![Some(IntOrString::Int(443)), Some(IntOrString::Int(9000))]
        );
    }

    #[test]
    fn egress_intra_ports_track_a_custom_base_port() {
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.base_port = Some(20000);
        let np = build(&cluster, &cluster.spec);
        let egress = np.spec.unwrap().egress.unwrap();
        let ports: Vec<_> = egress[0]
            .ports
            .as_ref()
            .unwrap()
            .iter()
            .map(|p| p.port.clone())
            .collect();
        assert_eq!(
            ports,
            vec![Some(IntOrString::Int(20000)), Some(IntOrString::Int(20004))]
        );
    }
}
