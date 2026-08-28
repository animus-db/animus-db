//! Shared test fixtures for the `desired` module's builder tests.

use crate::crd::{AnimusCluster, AnimusClusterSpec};

/// A minimal, fully-populated `AnimusCluster` (name/namespace/uid always
/// set, matching what a live object read back from the API server carries)
/// for builder unit tests. `control_nodes: None` lets the caller pass a
/// specific value or leave the `min(3, nodes)` default in effect.
#[must_use]
pub fn test_cluster(name: &str, ns: &str, nodes: i32, control_nodes: Option<i32>) -> AnimusCluster {
    let mut cluster = AnimusCluster::new(
        name,
        AnimusClusterSpec {
            nodes,
            control_nodes,
            ..Default::default()
        },
    );
    cluster.metadata.namespace = Some(ns.to_string());
    cluster.metadata.uid = Some(format!("uid-{name}"));
    cluster
}
