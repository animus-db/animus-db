//! Kubernetes operator for AnimusDB clusters: the `AnimusCluster` custom
//! resource, pure builders for its desired children, and the reconcile
//! loop. See `crates/animus-operator/CLAUDE.md` for the module map and the
//! pure-builders/imperative-shell split this crate follows.

pub mod admin_client;
pub mod cluster_api;
pub mod controller;
pub mod crd;
pub mod desired;
#[cfg(test)]
pub mod fakes;

pub use crd::{AnimusCluster, AnimusClusterSpec, AnimusClusterStatus};
