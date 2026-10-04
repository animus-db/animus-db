//! Node wiring for ADR 0073 Phase 2 (workstream P2-C): everything `animusd`
//! does with the cluster-version machinery `animus-control` provides.
//!
//! - **Pure helpers**: [`BUILD`], [`own_ext`] (the handshake `ext` this binary
//!   advertises), [`cluster_version_view`] / [`finalize_blockers`] (the admin
//!   `GET /admin/cluster-version` body and the Finalize pre-check),
//!   [`check_join_range`] (the joiner's startup range check).
//! - **Per-node state** ([`VersionState`], living on `ClusterEdgeState`): the
//!   node's `ClusterFeatures` handle (the one handle every gated emitter
//!   consults), its own [`VersionProfile`] (range + build), and the
//!   [`VersionHalt`] cell the process exit hangs off. Never a static: `SimEnv`
//!   runs many nodes (different "binaries") in one process.
//! - [`version_wiring_loop`]: the one generic task, spawned for every role,
//!   that feeds `ClusterFeatures` from replicated `Metadata`, flips
//!   `require_peer_ext` on nodes with no local apply task (data-only), latches
//!   the out-of-range halt, and **self-reports** `ReportNodeVersion` once the
//!   era is on.
//!
//! **No upgrade ever needs a restart**: nothing here emits a byte a Phase 1
//! binary cannot decode before the era is on. The self-report is guarded by
//! `era_active()` read from replicated state (a lagging view only delays it);
//! the handshake `ext` is ignored by Phase 1 binaries.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::meta::{MetaCommand, Metadata, NodeStatus};
use animus_control::version::{ClusterFeatures, NodeVersion, VersionRange, own_range};
use animus_control::version_observe::{OBSERVATION_WINDOW, VersionObservation};
use animus_env::{Env, NodeId, handshake};
use animus_node::control_handle::ControlHandle;
use animus_node::host::RelayClient;
use serde_json::{Value, json};

use crate::{ClientCtx, SCHEMA_COMMIT_TIMEOUT};

/// The build string this binary reports (handshake `ext` TLV 2, the
/// replicated `NodeVersion::build`, `RaftNode::set_own_build`). ONE constant
/// for all three: `era_on_proposals` compares `NodeVersion { range, build }`
/// for equality, so two spellings would make the leader re-report forever.
pub(crate) const BUILD: &str = env!("CARGO_PKG_VERSION");

/// The handshake `ext` bytes a node with `range`/`build` advertises. `None`
/// range is a Phase 1 profile (empty `ext`).
pub(crate) fn ext_for(range: Option<VersionRange>, build: &str) -> Vec<u8> {
    match range {
        Some(r) => handshake::encode_ext(Some((r.min, r.max)), Some(build)),
        None => Vec::new(),
    }
}

/// This binary's handshake `ext`: its compiled-in range and build. A pure
/// function of constants, not process-global mutable state. Always non-empty
/// (range presence is what distinguishes a Phase 2 binary from Phase 1, even
/// at `[1, 1]`).
pub(crate) fn own_ext() -> Vec<u8> {
    ext_for(Some(own_range()), BUILD)
}

/// What a node's "binary" advertises: a version range (`None` = Phase 1
/// profile: empty `ext`, never evaluates the era) and a build string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VersionProfile {
    pub(crate) range: Option<VersionRange>,
    pub(crate) build: String,
}

impl VersionProfile {
    /// This binary: its compiled-in range and build.
    pub(crate) fn current() -> Self {
        Self {
            range: Some(own_range()),
            build: BUILD.to_string(),
        }
    }

    /// A Phase 1 binary (tests / `SimCluster` skew cells only).
    #[cfg(test)]
    pub(crate) fn phase1() -> Self {
        Self {
            range: None,
            build: BUILD.to_string(),
        }
    }
}

/// The once-set cell a halted node's process exit waits on (first reason
/// wins).
#[derive(Default)]
pub(crate) struct VersionHalt {
    reason: Mutex<Option<String>>,
    notify: tokio::sync::Notify,
}

impl VersionHalt {
    pub(crate) fn set(&self, reason: String) {
        let mut slot = self.reason.lock().expect("version halt poisoned");
        if slot.is_none() {
            *slot = Some(reason);
            drop(slot);
            self.notify.notify_waiters();
        }
    }

    pub(crate) fn get(&self) -> Option<String> {
        self.reason.lock().expect("version halt poisoned").clone()
    }

    pub(crate) async fn wait(&self) -> String {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Register before the check so a `set` between check and await
            // is not lost.
            notified.as_mut().enable();
            if let Some(r) = self.get() {
                return r;
            }
            notified.await;
        }
    }
}

/// Per-node version state: one handle set per node (cheap clones share it).
#[derive(Clone)]
pub(crate) struct VersionState {
    pub(crate) features: ClusterFeatures,
    pub(crate) halt: Arc<VersionHalt>,
    profile: Arc<Mutex<VersionProfile>>,
}

impl Default for VersionState {
    fn default() -> Self {
        Self {
            features: ClusterFeatures::new(),
            halt: Arc::new(VersionHalt::default()),
            profile: Arc::new(Mutex::new(VersionProfile::current())),
        }
    }
}

impl VersionState {
    pub(crate) fn profile(&self) -> VersionProfile {
        self.profile
            .lock()
            .expect("version profile poisoned")
            .clone()
    }

    pub(crate) fn set_profile(&self, p: VersionProfile) {
        *self.profile.lock().expect("version profile poisoned") = p;
    }
}

/// The joiner's range check against the cluster's raw `cluster_version`
/// (`0` = era off, reads as `1`). `Err` carries the refusal text.
pub(crate) fn check_join_range(raw_cluster_version: u32, own: &VersionRange) -> Result<(), String> {
    let cv = raw_cluster_version.max(1);
    match own.exclusion_message(cv) {
        Some(msg) => Err(format!("cannot join: {msg}")),
        None => Ok(()),
    }
}

/// The era-on admission check for `change_membership` (ADR 0073 section 2(c)):
/// refuse a control voter with no known version range (a Phase 1 binary
/// never advertises one) or whose range excludes the cluster version. The
/// range is the replicated record if there is one, else a *fresh* leader
/// observation (within [`OBSERVATION_WINDOW`] of `now`). A pre-era cluster is
/// never checked here (nothing to refuse against yet).
pub(crate) fn check_member_admission(
    meta: &Metadata,
    observed: &BTreeMap<NodeId, VersionObservation>,
    now: animus_env::Nanos,
    node: &NodeId,
) -> Result<(), String> {
    if !meta.versioning_active() {
        return Ok(());
    }
    let cv = meta.cluster_version();
    let range = meta.node_versions.get(node).map(|v| v.range).or_else(|| {
        observed
            .get(node)
            .filter(|o| now.duration_since(o.observed_at) <= OBSERVATION_WINDOW)
            .and_then(|o| o.range)
    });
    match range {
        None => Err(format!(
            "node {node} has no known version range (a Phase 1 binary, or not yet heard \
             from): the cluster is versioned, so it cannot be added; start it on a \
             current binary and retry"
        )),
        Some(r) if !r.contains(cv) => Err(format!(
            "node {node}'s version range [{},{}] excludes cluster version {cv}",
            r.min, r.max
        )),
        Some(_) => Ok(()),
    }
}

/// One node that blocks Finalize, with the named reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Blocker {
    pub(crate) node: NodeId,
    pub(crate) reason: String,
}

/// Every node that blocks `FinalizeClusterVersion { target }` right now.
///
/// Superset of what `Metadata::apply` enforces (no record; range excludes
/// `target`): ADR 0073 decision 6 also makes `Down`, `Leaving` and
/// never-activated `Joining` members block regardless of any recorded range.
/// Apply does NOT enforce the status half; this pre-check does, on the
/// leader's own view (racy, operator-level). Apply-level enforcement is a
/// known gap tracked in issue #1168.
pub(crate) fn finalize_blockers(meta: &Metadata, target: u32) -> Vec<Blocker> {
    let mut out = Vec::new();
    for node in meta.required_version_set() {
        let status_reason = meta.members.get(&node).and_then(|m| match m.status {
            NodeStatus::Down => Some("member is Down".to_string()),
            NodeStatus::Leaving => Some("member is Leaving".to_string()),
            NodeStatus::Joining if !m.has_activated => {
                Some("member is Joining (never activated)".to_string())
            }
            _ => None,
        });
        let reason = status_reason.or_else(|| match meta.node_versions.get(&node) {
            None => Some("not reported".to_string()),
            Some(v) if !v.range.contains(target) => Some(format!(
                "range [{},{}] excludes target {target}",
                v.range.min, v.range.max
            )),
            Some(_) => None,
        });
        if let Some(reason) = reason {
            out.push(Blocker { node, reason });
        }
    }
    out
}

/// The minimum recorded `range.max` over the required set, capped at `own.max`;
/// `None` when any required node has no record.
pub(crate) fn safe_target(meta: &Metadata, own: &VersionRange) -> Option<u32> {
    let mut min = own.max;
    for node in meta.required_version_set() {
        min = min.min(meta.node_versions.get(&node)?.range.max);
    }
    Some(min)
}

fn range_json(r: &VersionRange) -> Value {
    json!({ "min": r.min, "max": r.max })
}

/// The `GET /admin/cluster-version` body. `observed` is the control leader's
/// live observation table (`None` on a follower / data-only node, reported as
/// `null`).
pub(crate) fn cluster_version_view(
    meta: &Metadata,
    own: &VersionProfile,
    observed: Option<&BTreeMap<NodeId, VersionObservation>>,
) -> Value {
    let active = meta.cluster_version();
    let era_active = meta.versioning_active();
    let target = active + 1;
    let own_range_v = own.range.unwrap_or_else(own_range);
    let blockers = finalize_blockers(meta, target);
    let can_finalize = era_active && blockers.is_empty() && target <= own_range_v.max;
    let nodes: Vec<Value> = meta
        .required_version_set()
        .iter()
        .map(|id| {
            let rec: Option<&NodeVersion> = meta.node_versions.get(id);
            let role = meta
                .node_addrs
                .get(id)
                .map_or("data", |a| a.role.as_str())
                .to_string();
            let status = meta.members.get(id).map(|m| format!("{:?}", m.status));
            let obs = observed.map(|o| match o.get(id).and_then(|v| v.range) {
                Some(r) => range_json(&r),
                None => Value::Null,
            });
            json!({
                "node": id.to_string(),
                "role": role,
                "status": status,
                "range": rec.map(|v| range_json(&v.range)),
                "build": rec.map(|v| v.build.clone()),
                "reported": rec.is_some(),
                "observed_range": obs,
            })
        })
        .collect();
    json!({
        "era_active": era_active,
        "active": active,
        "own_range": range_json(&own_range_v),
        "own_build": own.build,
        "nodes": nodes,
        "safe_target": if era_active { safe_target(meta, &own_range_v) } else { None },
        "can_finalize": can_finalize,
        "target": target,
        "blockers": blockers
            .iter()
            .map(|b| json!({ "node": b.node.to_string(), "reason": b.reason }))
            .collect::<Vec<_>>(),
    })
}

/// How often the feeder re-evaluates with no metadata change (a fallback; the
/// metadata watch normally wakes it).
const FEED_FALLBACK_INTERVAL: Duration = Duration::from_millis(500);

/// Minimum spacing between self-report attempts (a rejected report must not
/// spin).
const SELF_REPORT_RETRY: Duration = Duration::from_secs(5);

/// The per-node version feeder, spawned for every role (see the module doc).
pub(crate) async fn version_wiring_loop<E: Env, R: RelayClient>(ctx: ClientCtx<E, R>) {
    let state = ctx.edge.version().clone();
    let me = ctx.env.node_id();
    let watch = ctx.control.metadata_watch();
    let mut last_seen = watch.latest();
    let mut flipped = false;
    let mut last_report_attempt: Option<animus_env::Nanos> = None;
    loop {
        // Skip until this node has some trustworthy view of `Metadata`
        // (otherwise a default `Metadata` would read as cv 1).
        let ready = ctx.control.last_applied() > 0
            || ctx.control.has_synced_metadata()
            || ctx
                .remote_metadata
                .lock()
                .expect("remote metadata poisoned")
                .is_some();
        if ready {
            let meta = ctx.effective_metadata();
            state.features.update(&meta);
            let profile = state.profile();

            // Era flip for nodes with no local apply task (data-only /
            // mirror-only). Idempotent and harmless where the apply task
            // already did it.
            if meta.versioning_active() && !flipped {
                ctx.env.set_require_peer_ext(true);
                flipped = true;
            }

            // Out-of-range halt: nodes with no RaftNode evaluate it here;
            // a local RaftNode latches its own, polled below.
            if meta.versioning_active()
                && let Some(range) = profile.range
                && let Some(msg) = range.exclusion_message(meta.cluster_version())
            {
                state.halt.set(msg);
            }
            if let ControlHandle::Local(raft) = &ctx.control
                && let Some(reason) = raft.halt_reason()
            {
                state.halt.set(reason);
            }

            // Boot-time self-report: only once the era is on (a Phase 1
            // peer could not decode the command before), and only if the
            // record differs from this binary's.
            if let Some(range) = profile.range
                && meta.versioning_active()
                && state.halt.get().is_none()
            {
                let want = NodeVersion {
                    range,
                    build: profile.build.clone(),
                };
                if meta.node_versions.get(&me) != Some(&want) {
                    let now = ctx.env.now();
                    let due = last_report_attempt
                        .is_none_or(|t| now.duration_since(t) >= SELF_REPORT_RETRY);
                    // Only report once registered (apply rejects unknown
                    // nodes); a node not yet in the required set retries.
                    if due && meta.required_version_set().contains(&me) {
                        last_report_attempt = Some(now);
                        let cmd = MetaCommand::ReportNodeVersion {
                            node: me.clone(),
                            range,
                            build: profile.build.clone(),
                        };
                        let c = ctx.clone();
                        let me2 = me.clone();
                        let want2 = want.clone();
                        let _ = ctx
                            .propose_and_await(cmd, SCHEMA_COMMIT_TIMEOUT, || {
                                let c = c.clone();
                                let me = me2.clone();
                                let want = want2.clone();
                                async move {
                                    (c.effective_metadata().node_versions.get(&me) == Some(&want))
                                        .then_some(())
                                }
                            })
                            .await;
                    }
                }
            }
        }
        tokio::select! {
            _ = watch.changed(last_seen) => {}
            _ = ctx.env.sleep(FEED_FALLBACK_INTERVAL) => {}
        }
        last_seen = watch.latest();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_control::meta::{Member, NodeAddrs};

    fn nid(s: &str) -> NodeId {
        NodeId::propose(s).unwrap()
    }

    fn member(status: NodeStatus, activated: bool) -> Member {
        Member {
            labels: Default::default(),
            status,
            has_activated: activated,
        }
    }

    fn rec(min: u32, max: u32) -> NodeVersion {
        NodeVersion {
            range: VersionRange::new(min, max),
            build: "t".into(),
        }
    }

    #[test]
    fn own_ext_is_nonempty_and_round_trips() {
        let ext = own_ext();
        assert!(!ext.is_empty(), "range presence is the B2 signal");
        let r = own_range();
        assert_eq!(
            handshake::parse_ext_range(&ext).unwrap(),
            Some((r.min, r.max))
        );
        assert_eq!(handshake::parse_ext_build(&ext).as_deref(), Some(BUILD));
        assert!(ext_for(None, BUILD).is_empty());
    }

    #[test]
    fn join_range_check_reads_zero_as_one() {
        let own = VersionRange::new(3, 4);
        assert!(check_join_range(0, &own).unwrap_err().contains("below"));
        assert!(check_join_range(3, &own).is_ok());
        assert!(check_join_range(9, &own).unwrap_err().contains("above"));
        assert!(check_join_range(0, &VersionRange::new(1, 1)).is_ok());
    }

    #[test]
    fn member_admission_is_era_gated_and_names_the_problem() {
        use animus_env::Nanos;
        let node = nid("x");
        let mut meta = Metadata::default();
        let mut obs = BTreeMap::new();
        // Pre-era: never checked.
        assert!(check_member_admission(&meta, &obs, Nanos(0), &node).is_ok());
        meta.cluster_version = 1;
        let e = check_member_admission(&meta, &obs, Nanos(0), &node).unwrap_err();
        assert!(e.contains("no known version range"), "{e}");
        // A fresh observation with a range admits; a stale one does not.
        obs.insert(
            node.clone(),
            VersionObservation {
                range: Some(VersionRange::new(1, 2)),
                build: None,
                observed_at: Nanos(0),
            },
        );
        assert!(check_member_admission(&meta, &obs, Nanos(1_000), &node).is_ok());
        let far = Nanos(OBSERVATION_WINDOW.as_nanos() as u64 * 10);
        assert!(check_member_admission(&meta, &obs, far, &node).is_err());
        // A Phase 1 observation (no range) does not admit.
        obs.get_mut(&node).unwrap().range = None;
        assert!(check_member_admission(&meta, &obs, Nanos(1_000), &node).is_err());
        // A recorded range wins and must contain the cluster version.
        meta.node_versions.insert(node.clone(), rec(3, 4));
        let e = check_member_admission(&meta, &obs, Nanos(1_000), &node).unwrap_err();
        assert!(e.contains("excludes cluster version 1"), "{e}");
        meta.node_versions.insert(node.clone(), rec(1, 4));
        assert!(check_member_admission(&meta, &obs, Nanos(1_000), &node).is_ok());
    }

    #[test]
    fn blockers_and_safe_target_matrix() {
        let mut meta = Metadata::default();
        for (n, st, act) in [
            ("a", NodeStatus::Active, true),
            ("b", NodeStatus::Down, true),
            ("c", NodeStatus::Leaving, true),
            ("d", NodeStatus::Joining, false),
            ("e", NodeStatus::Active, true),
            ("f", NodeStatus::Active, true),
        ] {
            meta.members.insert(nid(n), member(st, act));
        }
        meta.node_versions.insert(nid("a"), rec(1, 2));
        meta.node_versions.insert(nid("b"), rec(1, 2));
        meta.node_versions.insert(nid("c"), rec(1, 2));
        meta.node_versions.insert(nid("d"), rec(1, 2));
        meta.node_versions.insert(nid("e"), rec(1, 1));
        // f never reported.
        let b = finalize_blockers(&meta, 2);
        let got: BTreeMap<String, String> = b
            .iter()
            .map(|b| (b.node.to_string(), b.reason.clone()))
            .collect();
        assert_eq!(got.len(), 5, "{got:?}");
        assert_eq!(got["b"], "member is Down");
        assert_eq!(got["c"], "member is Leaving");
        assert_eq!(got["d"], "member is Joining (never activated)");
        assert_eq!(got["e"], "range [1,1] excludes target 2");
        assert_eq!(got["f"], "not reported");
        assert_eq!(safe_target(&meta, &VersionRange::new(1, 2)), None);

        let mut ok = Metadata::default();
        ok.members
            .insert(nid("a"), member(NodeStatus::Active, true));
        ok.node_addrs.insert(
            nid("v"),
            NodeAddrs {
                internal: String::new(),
                client: String::new(),
                intra: String::new(),
                admin: String::new(),
                role: "control".into(),
            },
        );
        ok.node_versions.insert(nid("a"), rec(1, 3));
        ok.node_versions.insert(nid("v"), rec(1, 2));
        ok.cluster_version = 1;
        assert!(finalize_blockers(&ok, 2).is_empty());
        assert_eq!(safe_target(&ok, &VersionRange::new(1, 5)), Some(2));
        assert_eq!(safe_target(&ok, &VersionRange::new(1, 1)), Some(1));
        let v = cluster_version_view(&ok, &VersionProfile::current(), None);
        assert_eq!(v["active"], 1);
        assert_eq!(v["era_active"], true);
        // own max is 1 (MAX_SUPPORTED), so target 2 is out of reach here.
        assert_eq!(v["can_finalize"], false);
        assert_eq!(v["nodes"].as_array().unwrap().len(), 2);
        assert!(v["nodes"][0]["observed_range"].is_null());
    }
}
