//! The `{name}-internal-only` `NetworkPolicy` builder: node-to-node traffic
//! stays inside the cluster's own pods, the admin port additionally accepts
//! the operator, the dynamo port is open to any source, and everything else
//! is denied by default (ADR 0047's Kubernetes deployment intent — see this
//! crate's `CLAUDE.md`).
//!
//! **Unaffected by `spec.tls` (ADR 0064 commit 3).** TLS is a mode each
//! port's own listener can be configured into, not a change to which pods
//! may reach which port at all — the port topology (and therefore this
//! builder's own output) is identical whether or not `spec.tls` is set.

use k8s_openapi::api::networking::v1::{
    NetworkPolicy, NetworkPolicyIngressRule, NetworkPolicyPeer, NetworkPolicyPort,
    NetworkPolicySpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use super::cluster_config::PORT_ADMIN;
use super::{
    OPERATOR_APP_NAME, common_labels, network_policy_name, owner_reference, selector_labels,
};
use crate::crd::{AnimusCluster, AnimusClusterSpec};

fn tcp_port(p: i32) -> NetworkPolicyPort {
    NetworkPolicyPort {
        protocol: Some("TCP".to_string()),
        port: Some(IntOrString::Int(p)),
        ..Default::default()
    }
}

fn pod_selector(labels: std::collections::BTreeMap<String, String>) -> NetworkPolicyPeer {
    NetworkPolicyPeer {
        pod_selector: Some(LabelSelector {
            match_labels: Some(labels),
            ..Default::default()
        }),
        ..Default::default()
    }
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
    let own_pods = selector_labels(name);

    let mut operator_labels = std::collections::BTreeMap::new();
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
            policy_types: Some(vec!["Ingress".to_string()]),
            ingress: Some(ingress),
            ..Default::default()
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
        assert_eq!(spec.policy_types, Some(vec!["Ingress".to_string()]));
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
}
