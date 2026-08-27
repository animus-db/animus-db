//! The two `Service` builders: the headless internal `Service` (node-to-node
//! ports + admin/console) and the client-facing dynamo `Service`.

use k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use super::cluster_config::{PORT_ADMIN, PORT_CONSOLE, PORT_DYNAMO, PORT_INTERNAL, PORT_INTRA};
use super::{
    client_service_name, common_labels, internal_service_name, owner_reference, selector_labels,
};
use crate::crd::{AnimusCluster, AnimusClusterSpec};

fn port(name: &str, base_port: i32, offset: i32) -> ServicePort {
    let p = base_port + offset;
    ServicePort {
        name: Some(name.to_string()),
        port: p,
        target_port: Some(IntOrString::Int(p)),
        protocol: Some("TCP".to_string()),
        ..Default::default()
    }
}

/// The headless `{name}-internal` `Service`: `clusterIP: None`,
/// `publishNotReadyAddresses: true` (so a not-yet-ready peer is still
/// dialable during bootstrap/rolling restarts — node-to-node Raft/RPC
/// traffic must reach a starting peer, not wait on its own readiness gate),
/// carrying the `internal`/`intra`/`admin`/`console` ports. The `client`
/// and `dynamo` ports are deliberately absent here — `dynamo` gets its own
/// `Service` ([`build_client`]), and `client` (the raw length-prefixed TCP
/// client protocol) has no external-facing `Service` at all in this
/// operator's scope (an in-cluster caller can still reach it by pod DNS
/// name/port directly; ADR 0047's deployment intent is that only the
/// DynamoDB wire edge is meant to be reachable, which is what
/// [`build_client`]'s own `Service` type controls).
#[must_use]
pub fn build_internal(cluster: &AnimusCluster, spec: &AnimusClusterSpec) -> Service {
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
    let base_port = spec.base_port_or_default();

    Service {
        metadata: ObjectMeta {
            name: Some(internal_service_name(name)),
            namespace: Some(ns.to_string()),
            labels: Some(common_labels(name)),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            publish_not_ready_addresses: Some(true),
            selector: Some(selector_labels(name)),
            ports: Some(vec![
                port("internal", base_port, PORT_INTERNAL),
                port("intra", base_port, PORT_INTRA),
                port("admin", base_port, PORT_ADMIN),
                port("console", base_port, PORT_CONSOLE),
            ]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The client-facing `{name}-dynamo` `Service`: just the `dynamo` port,
/// `spec.clientService.type` (default `ClusterIP`).
#[must_use]
pub fn build_client(cluster: &AnimusCluster, spec: &AnimusClusterSpec) -> Service {
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
    let base_port = spec.base_port_or_default();

    Service {
        metadata: ObjectMeta {
            name: Some(client_service_name(name)),
            namespace: Some(ns.to_string()),
            labels: Some(common_labels(name)),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            type_: Some(spec.client_service.type_or_default().to_string()),
            selector: Some(selector_labels(name)),
            ports: Some(vec![port("dynamo", base_port, PORT_DYNAMO)]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::desired::test_support::test_cluster;

    fn port_names(svc: &Service) -> Vec<String> {
        svc.spec
            .as_ref()
            .unwrap()
            .ports
            .as_ref()
            .unwrap()
            .iter()
            .map(|p| p.name.clone().unwrap())
            .collect()
    }

    #[test]
    fn internal_service_is_headless_and_carries_four_ports() {
        let cluster = test_cluster("c", "ns", 3, None);
        let svc = build_internal(&cluster, &cluster.spec);
        assert_eq!(svc.metadata.name.as_deref(), Some("c-internal"));
        let spec = svc.spec.unwrap();
        assert_eq!(spec.cluster_ip.as_deref(), Some("None"));
        assert_eq!(spec.publish_not_ready_addresses, Some(true));
        let names: Vec<String> = spec
            .ports
            .as_ref()
            .unwrap()
            .iter()
            .map(|p| p.name.clone().unwrap())
            .collect();
        assert_eq!(names, vec!["internal", "intra", "admin", "console"]);
        assert!(!names.contains(&"client".to_string()));
        assert!(!names.contains(&"dynamo".to_string()));
    }

    #[test]
    fn internal_service_port_numbers_match_base_port_offsets() {
        let cluster = test_cluster("c", "ns", 3, None);
        let svc = build_internal(&cluster, &cluster.spec);
        let ports = svc.spec.unwrap().ports.unwrap();
        let by_name: std::collections::BTreeMap<_, _> = ports
            .iter()
            .map(|p| (p.name.clone().unwrap(), p.port))
            .collect();
        assert_eq!(by_name["internal"], 14000);
        assert_eq!(by_name["admin"], 14003);
        assert_eq!(by_name["intra"], 14004);
        assert_eq!(by_name["console"], 14005);
    }

    #[test]
    fn client_service_carries_only_dynamo_and_respects_type() {
        let mut cluster = test_cluster("c", "ns", 3, None);
        cluster.spec.client_service.type_ = Some("LoadBalancer".to_string());
        let svc = build_client(&cluster, &cluster.spec);
        assert_eq!(svc.metadata.name.as_deref(), Some("c-dynamo"));
        assert_eq!(port_names(&svc), vec!["dynamo"]);
        assert_eq!(svc.spec.unwrap().type_.as_deref(), Some("LoadBalancer"));
    }

    #[test]
    fn client_service_defaults_to_cluster_ip() {
        let cluster = test_cluster("c", "ns", 3, None);
        let svc = build_client(&cluster, &cluster.spec);
        assert_eq!(svc.spec.unwrap().type_.as_deref(), Some("ClusterIP"));
    }

    #[test]
    fn services_carry_selector_matching_pod_labels() {
        let cluster = test_cluster("c", "ns", 3, None);
        let internal = build_internal(&cluster, &cluster.spec);
        let client = build_client(&cluster, &cluster.spec);
        let expected = selector_labels("c");
        assert_eq!(internal.spec.unwrap().selector, Some(expected.clone()));
        assert_eq!(client.spec.unwrap().selector, Some(expected));
    }
}
