//! The `{name}-config` `ConfigMap` builder: `cluster.json` (the
//! `animusd` `ClusterConfig`) + `entrypoint.sh` (the per-pod dispatch
//! script) — see [`super::cluster_config`] for both bodies.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

use super::cluster_config::{self, CONFIG_FILE_NAME, ENTRYPOINT_FILE_NAME};
use super::{common_labels, config_map_name, owner_reference};
use crate::crd::{AnimusCluster, AnimusClusterSpec};

/// Build the cluster config `ConfigMap` for `cluster` (name/namespace read
/// off its metadata) from `spec`.
///
/// # Panics
/// See [`super::owner_reference`]'s doc — the same "always populated on a
/// live object" precondition applies here.
#[must_use]
pub fn build(cluster: &AnimusCluster, spec: &AnimusClusterSpec) -> ConfigMap {
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

    let config = cluster_config::build_cluster_config(name, ns, spec);
    let mut data = BTreeMap::new();
    data.insert(
        CONFIG_FILE_NAME.to_string(),
        cluster_config::to_json(&config),
    );
    data.insert(
        ENTRYPOINT_FILE_NAME.to_string(),
        cluster_config::entrypoint_script(spec),
    );

    ConfigMap {
        metadata: ObjectMeta {
            name: Some(config_map_name(name)),
            namespace: Some(ns.to_string()),
            labels: Some(common_labels(name)),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::desired::test_support::test_cluster;

    #[test]
    fn config_map_carries_both_keys_and_owner_reference() {
        let cluster = test_cluster("c", "ns", 3, None);
        let cm = build(&cluster, &cluster.spec);
        assert_eq!(cm.metadata.name.as_deref(), Some("c-config"));
        assert_eq!(cm.metadata.namespace.as_deref(), Some("ns"));
        let data = cm.data.expect("data present");
        assert!(data.contains_key(CONFIG_FILE_NAME));
        assert!(data.contains_key(ENTRYPOINT_FILE_NAME));
        let owners = cm.metadata.owner_references.expect("owner refs present");
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].kind, "AnimusCluster");
        assert_eq!(owners[0].name, "c");
        assert_eq!(owners[0].controller, Some(true));
    }

    #[test]
    fn config_map_json_key_parses_back_to_three_nodes() {
        let cluster = test_cluster("c", "ns", 3, None);
        let cm = build(&cluster, &cluster.spec);
        let data = cm.data.unwrap();
        let json = &data[CONFIG_FILE_NAME];
        let value: serde_json::Value = serde_json::from_str(json).unwrap();
        assert_eq!(value["nodes"].as_array().unwrap().len(), 3);
    }
}
