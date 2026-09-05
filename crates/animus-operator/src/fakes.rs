//! In-memory fakes for the [`crate::cluster_api::ClusterApi`] and
//! [`crate::admin_client::AdminOps`] seams, `#[cfg(test)]` only — this is
//! ADR 0061 rung E1's harness. See `crates/animus-operator/CLAUDE.md`'s
//! testing section and ADR 0061's own amendment note for what these do and
//! do not prove.
//!
//! Both fakes are deliberately minimal: they store exactly what
//! `controller.rs`'s tests need to assert on or seed, not a general-purpose
//! mock API server. In particular [`FakeClusterApi`] does not model
//! resourceVersion/conflict semantics, admission, or watch events — it is a
//! same-process record-and-serve store, not `kube`'s own wire protocol.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;

use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec, StatefulSetStatus};
use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use kube::core::DynamicObject;
use serde_json::Value;

use crate::admin_client::AdminOps;
use crate::cluster_api::ClusterApi;
use crate::controller::ReconcileError;
use crate::crd::AnimusClusterStatus;

/// The kind of a recorded [`FakeClusterApi`] apply call.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum AppliedKind {
    ConfigMap,
    Service,
    NetworkPolicy,
    StatefulSet,
    Certificate,
}

/// An in-memory [`ClusterApi`]: records every apply call (kind + name, in
/// call order) and serves `get_configmap`/`get_statefulset`/`get_secret`
/// from a small seedable store, so a test can both seed "what a previous
/// reconcile already applied" and assert on "what this reconcile just
/// applied".
#[derive(Default)]
pub struct FakeClusterApi {
    applies: Mutex<Vec<(AppliedKind, String)>>,
    configmaps: Mutex<BTreeMap<String, ConfigMap>>,
    statefulsets: Mutex<BTreeMap<String, StatefulSet>>,
    status_patches: Mutex<Vec<AnimusClusterStatus>>,
    secrets: Mutex<BTreeMap<String, Secret>>,
}

impl FakeClusterApi {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed a `ConfigMap` as if a previous reconcile had already applied
    /// it — used to drive `control_nodes_changed`.
    pub fn seed_configmap(&self, name: &str, cm: ConfigMap) {
        self.configmaps.lock().unwrap().insert(name.to_string(), cm);
    }

    /// Seed a `StatefulSet` with `spec.replicas` and
    /// `status.readyReplicas` set, as if a previous reconcile had already
    /// applied it and the API server had since reported readiness — used
    /// to drive the scale-down replica-count check and, once
    /// `apply_statefulset` preserves it below, `finish_reconcile`'s phase
    /// computation.
    pub fn seed_statefulset(&self, name: &str, replicas: i32, ready_replicas: i32) {
        let mut sts = StatefulSet {
            spec: Some(StatefulSetSpec {
                replicas: Some(replicas),
                ..Default::default()
            }),
            status: Some(StatefulSetStatus {
                ready_replicas: Some(ready_replicas),
                ..Default::default()
            }),
            ..Default::default()
        };
        sts.metadata.name = Some(name.to_string());
        self.statefulsets
            .lock()
            .unwrap()
            .insert(name.to_string(), sts);
    }

    /// Every apply call recorded so far, in call order.
    #[must_use]
    pub fn applies(&self) -> Vec<(AppliedKind, String)> {
        self.applies.lock().unwrap().clone()
    }

    /// Every `status` patch recorded so far, in call order.
    #[must_use]
    pub fn status_patches(&self) -> Vec<AnimusClusterStatus> {
        self.status_patches.lock().unwrap().clone()
    }

    /// The most recent `status` patch, if any.
    #[must_use]
    pub fn last_status(&self) -> Option<AnimusClusterStatus> {
        self.status_patches.lock().unwrap().last().cloned()
    }

    /// The `ConfigMap` currently stored under `name` (seeded, or the most
    /// recently applied one, whichever happened last).
    #[must_use]
    pub fn configmap(&self, name: &str) -> Option<ConfigMap> {
        self.configmaps.lock().unwrap().get(name).cloned()
    }

    /// Seed a `Secret` (e.g. `spec.tls`'s resolved cert Secret) — used to
    /// drive the admin client's TLS-CA lookup.
    pub fn seed_secret(&self, name: &str, secret: Secret) {
        self.secrets
            .lock()
            .unwrap()
            .insert(name.to_string(), secret);
    }
}

#[async_trait::async_trait]
impl ClusterApi for FakeClusterApi {
    async fn apply_configmap(&self, _ns: &str, cm: &ConfigMap) -> Result<(), ReconcileError> {
        let name = cm.metadata.name.clone().unwrap();
        self.applies
            .lock()
            .unwrap()
            .push((AppliedKind::ConfigMap, name.clone()));
        self.configmaps.lock().unwrap().insert(name, cm.clone());
        Ok(())
    }

    async fn apply_service(&self, _ns: &str, svc: &Service) -> Result<(), ReconcileError> {
        let name = svc.metadata.name.clone().unwrap();
        self.applies
            .lock()
            .unwrap()
            .push((AppliedKind::Service, name));
        Ok(())
    }

    async fn apply_networkpolicy(
        &self,
        _ns: &str,
        np: &NetworkPolicy,
    ) -> Result<(), ReconcileError> {
        let name = np.metadata.name.clone().unwrap();
        self.applies
            .lock()
            .unwrap()
            .push((AppliedKind::NetworkPolicy, name));
        Ok(())
    }

    async fn apply_statefulset(
        &self,
        _ns: &str,
        sts: &StatefulSet,
    ) -> Result<StatefulSet, ReconcileError> {
        let name = sts.metadata.name.clone().unwrap();
        self.applies
            .lock()
            .unwrap()
            .push((AppliedKind::StatefulSet, name.clone()));
        let mut stored = sts.clone();
        let mut statefulsets = self.statefulsets.lock().unwrap();
        // A real server-side-apply of a spec-only patch never clobbers the
        // status subresource — preserve whatever was seeded/previously
        // stored so `finish_reconcile`'s `applied_sts.status.ready_replicas`
        // read reflects "what the cluster currently reports", not "None,
        // because this reconcile only just applied the spec".
        if let Some(existing) = statefulsets.get(&name) {
            stored.status = existing.status.clone();
        }
        statefulsets.insert(name, stored.clone());
        Ok(stored)
    }

    async fn get_configmap(
        &self,
        _ns: &str,
        name: &str,
    ) -> Result<Option<ConfigMap>, ReconcileError> {
        Ok(self.configmaps.lock().unwrap().get(name).cloned())
    }

    async fn get_statefulset(
        &self,
        _ns: &str,
        name: &str,
    ) -> Result<Option<StatefulSet>, ReconcileError> {
        Ok(self.statefulsets.lock().unwrap().get(name).cloned())
    }

    async fn patch_cluster_status(
        &self,
        _ns: &str,
        _name: &str,
        status: &AnimusClusterStatus,
    ) -> Result<(), ReconcileError> {
        self.status_patches.lock().unwrap().push(status.clone());
        Ok(())
    }

    async fn apply_certificate(
        &self,
        _ns: &str,
        cert: &DynamicObject,
    ) -> Result<(), ReconcileError> {
        let name = cert.metadata.name.clone().unwrap();
        self.applies
            .lock()
            .unwrap()
            .push((AppliedKind::Certificate, name));
        Ok(())
    }

    async fn get_secret(&self, _ns: &str, name: &str) -> Result<Option<Secret>, ReconcileError> {
        Ok(self.secrets.lock().unwrap().get(name).cloned())
    }
}

/// An in-memory [`AdminOps`]: records every call (method + url, in call
/// order) and serves canned responses. `GET .../drain-status` responses
/// are queued per call **except** the last queued entry, which repeats
/// forever once reached — this lets a test express "drain finishes after N
/// polls" (queue N-1 busy responses then one done response, which then
/// repeats) or "drain never finishes" (queue exactly one busy response) in
/// the same mechanism. `POST .../drain` and `POST .../remove` default to
/// success; `fail_drain`/`fail_remove` make them error instead, for the
/// scale-down drain-failure path.
#[derive(Default)]
pub struct FakeAdminClient {
    calls: Mutex<Vec<(String, String)>>,
    drain_status_responses: Mutex<VecDeque<Value>>,
    fail_drain: Mutex<bool>,
    fail_remove: Mutex<bool>,
}

impl FakeAdminClient {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue one `GET .../drain-status` response. See the type doc for how
    /// the queue is consumed (last entry sticks).
    pub fn queue_drain_status(&self, tablets_remaining: u64, status: &str) {
        self.drain_status_responses
            .lock()
            .unwrap()
            .push_back(serde_json::json!({
                "tablets_remaining": tablets_remaining,
                "status": status,
            }));
    }

    /// Make every future `POST .../admin/drain` call fail.
    pub fn fail_drain(&self) {
        *self.fail_drain.lock().unwrap() = true;
    }

    /// Make every future `POST .../admin/member/remove` call fail.
    pub fn fail_remove(&self) {
        *self.fail_remove.lock().unwrap() = true;
    }

    /// Every call recorded so far, as `(method, url)`, in call order.
    #[must_use]
    pub fn calls(&self) -> Vec<(String, String)> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl AdminOps for FakeAdminClient {
    async fn post_json(
        &self,
        url: &str,
        _body: &Value,
        _ca_pem: Option<&[u8]>,
    ) -> Result<Value, String> {
        self.calls
            .lock()
            .unwrap()
            .push(("POST".to_string(), url.to_string()));
        if url.contains("/admin/member/remove") {
            if *self.fail_remove.lock().unwrap() {
                return Err("remove failed (fake)".to_string());
            }
        } else if *self.fail_drain.lock().unwrap() {
            return Err("drain failed (fake)".to_string());
        }
        Ok(serde_json::json!({}))
    }

    async fn get_json(&self, url: &str, _ca_pem: Option<&[u8]>) -> Result<Value, String> {
        self.calls
            .lock()
            .unwrap()
            .push(("GET".to_string(), url.to_string()));
        let mut queue = self.drain_status_responses.lock().unwrap();
        if queue.len() > 1 {
            Ok(queue.pop_front().unwrap())
        } else if let Some(last) = queue.front() {
            Ok(last.clone())
        } else {
            // No response queued at all: default to "already fully
            // drained", so a test that doesn't care about the drain
            // sequence's own pacing gets a fast, deterministic success.
            Ok(serde_json::json!({ "tablets_remaining": 0, "status": "Removed" }))
        }
    }
}
