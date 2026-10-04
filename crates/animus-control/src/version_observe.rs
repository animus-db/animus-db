//! Leader-local passive version observation and era start (ADR 0073 Phase 2,
//! workstream P2-A, section 2).
//!
//! **Why this exists.** While any Phase 1 binary can be in the cluster, a
//! Phase 2 ("B2") node may emit nothing a Phase 1 node cannot decode, and
//! `ReportNodeVersion` is a new `MetaCommand` variant a Phase 1 voter cannot
//! decode. So B2 cannot learn versions *through* the replicated log before
//! every voter is B2. Instead every node advertises its supported range in the
//! handshake `ext` and each control node records, for every inbound control
//! envelope, `(range, build, observed_at)` keyed by the envelope's sender
//! ([`VersionObservations`]). Only the leader acts on it.
//!
//! **Precondition P** ([`era_start_proposals`]): the era is off; this node is
//! leader with an own range; and *every* node in
//! `Metadata::required_version_set()` ∪ control voters ∪ control learners has
//! an observation within the window `T` ([`OBSERVATION_WINDOW`]) carrying
//! `Some(range)` that contains the current cluster version. A node never
//! observed, or last observed as a Phase 1 binary (empty `ext`), blocks P.
//! When P holds the leader proposes `ReportNodeVersion` for every node in the
//! required set (the first applied one starts the sticky era).
//!
//! **Era on** ([`era_on_proposals`]): the leader keeps the records current by
//! re-reporting any required node whose fresh observation differs from its
//! recorded entry (or that has no record yet, e.g. a node registered after the
//! era started). Proposals are idempotent (an identical re-report applies as
//! `NoOp`), fire-and-forget (`ProposeResult::Accepted` means "appended
//! locally", never "committed"), and re-derived from the applied cache each
//! tick, so a lost proposal is retried; the loop rate-limits to one proposal
//! per node per `T` so a slow commit never causes a duplicate storm.
//!
//! Everything in this module except [`VersionObservations`]' `Instant`
//! arguments is pure: instants are `Env`-supplied (`env.now()`).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use animus_env::handshake::{parse_ext_build, parse_ext_range};
use animus_env::{Nanos, NodeId};

use crate::meta::{MetaCommand, Metadata};
use crate::node::HEARTBEAT_INTERVAL;
use crate::version::{ClusterVersion, NodeVersion, VersionRange};

/// `T`: how recently the leader must have observed a node for the node to
/// count as observed (`3 x HEARTBEAT_INTERVAL`). Also the minimum time a
/// freshly elected leader must have held leadership before it evaluates P, and
/// the per-node re-proposal rate limit.
pub const OBSERVATION_WINDOW: Duration =
    Duration::from_nanos(3 * HEARTBEAT_INTERVAL.as_nanos() as u64);

/// What the leader last saw of one peer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionObservation {
    /// The peer's advertised range; `None` for a Phase 1 binary (empty `ext`)
    /// or a malformed/range-less `ext`.
    pub range: Option<VersionRange>,
    /// The peer's advertised build string, if any (display only).
    pub build: Option<String>,
    /// When (`env.now()`) the most recent envelope from the peer arrived.
    pub observed_at: Nanos,
}

#[derive(Debug)]
struct Entry {
    ext: Arc<[u8]>,
    obs: VersionObservation,
}

/// The per-node observation table: sender id -> latest [`VersionObservation`].
#[derive(Debug, Default)]
pub struct VersionObservations {
    map: BTreeMap<NodeId, Entry>,
}

impl VersionObservations {
    /// Record an inbound envelope from `from` carrying handshake `ext` at
    /// `now`. The `ext` is only re-parsed when it changed since the previous
    /// envelope from the same sender.
    pub fn observe(&mut self, from: &NodeId, ext: &Arc<[u8]>, now: Nanos) {
        if let Some(e) = self.map.get_mut(from)
            && (Arc::ptr_eq(&e.ext, ext) || *e.ext == **ext)
        {
            e.obs.observed_at = now;
            return;
        }
        let obs = VersionObservation {
            range: parse_range(ext),
            build: parse_ext_build(ext),
            observed_at: now,
        };
        self.map.insert(
            from.clone(),
            Entry {
                ext: Arc::clone(ext),
                obs,
            },
        );
    }

    /// The latest observation of `node`, if any.
    #[must_use]
    pub fn get(&self, node: &NodeId) -> Option<&VersionObservation> {
        self.map.get(node).map(|e| &e.obs)
    }

    /// A snapshot of the whole table.
    #[must_use]
    pub fn snapshot(&self) -> BTreeMap<NodeId, VersionObservation> {
        self.map
            .iter()
            .map(|(k, e)| (k.clone(), e.obs.clone()))
            .collect()
    }
}

/// Parse a handshake `ext` into a valid [`VersionRange`]; empty, malformed,
/// range-less or invalid -> `None` (treated as a Phase 1 binary).
fn parse_range(ext: &[u8]) -> Option<VersionRange> {
    match parse_ext_range(ext) {
        Ok(Some((min, max))) => {
            let r = VersionRange::new(min, max);
            r.is_valid().then_some(r)
        }
        _ => None,
    }
}

/// This node's own advertised version profile.
///
/// **The default is the Phase 1 profile (`range: None`), not
/// `Some(own_range())`.** A `RaftNode` only proposes version commands once an
/// assembler opts in with [`RaftNode::set_own_version_range`] (P2-C/P2-D, at
/// the same time it gives the `Env` its handshake `ext`). With a `Some`
/// default, a lone voter whose required set is just itself satisfies
/// precondition P at once, starts the era, and the era-on refusal then
/// refuses every empty-`ext` peer: found live as 13 failing `animusd` lib
/// tests (single-node `ProdEnv` and `SimCluster` clusters) whose environments
/// advertise no `ext` yet. Until `ProdEnv` advertises one, a `Some` default
/// would also make a P2-A-only build start the era on a one-node production
/// cluster and refuse every later joiner.
#[derive(Clone, Debug)]
pub struct OwnVersion {
    /// `None` behaves as a Phase 1 binary: never starts the era, never
    /// proposes version commands.
    pub range: Option<VersionRange>,
    /// Build string recorded for this node.
    pub build: String,
    /// Test-only capped decode (ADR 0073 P2-D): installed by
    /// `RaftNode::set_binary_profile`; `None` (the default) is inert.
    #[cfg(any(test, feature = "sim-versions"))]
    pub sim_cap: Option<crate::sim_versions::SimCap>,
}

impl Default for OwnVersion {
    fn default() -> Self {
        Self {
            range: None,
            build: env!("CARGO_PKG_VERSION").to_string(),
            #[cfg(any(test, feature = "sim-versions"))]
            sim_cap: None,
        }
    }
}

/// The slice of `Metadata` the version loop needs, cheap to extract under the
/// cache lock (the loop ticks every 100 ms; cloning the whole blob is the
/// documented clone-churn hot spot).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VersionView {
    /// `Metadata::versioning_active()`.
    pub era_active: bool,
    /// `Metadata::cluster_version()`.
    pub cluster_version: ClusterVersion,
    /// `Metadata::required_version_set()`.
    pub required: BTreeSet<NodeId>,
    /// `Metadata::node_versions`.
    pub recorded: BTreeMap<NodeId, NodeVersion>,
}

impl VersionView {
    /// Extract the view from a `Metadata`.
    #[must_use]
    pub fn from_metadata(meta: &Metadata) -> Self {
        Self {
            era_active: meta.versioning_active(),
            cluster_version: meta.cluster_version(),
            required: meta.required_version_set(),
            recorded: meta.node_versions.clone(),
        }
    }
}

/// What the leader knows about one node: its own profile for itself, the
/// observation table for everyone else.
fn view_of(
    node: &NodeId,
    self_id: &NodeId,
    own: &OwnVersion,
    obs: &BTreeMap<NodeId, VersionObservation>,
    now: Nanos,
    window: Duration,
) -> Option<(Option<VersionRange>, Option<String>)> {
    if node == self_id {
        return Some((own.range, Some(own.build.clone())));
    }
    let o = obs.get(node)?;
    (now.duration_since(o.observed_at) <= window).then(|| (o.range, o.build.clone()))
}

fn report(node: &NodeId, range: VersionRange, build: Option<String>) -> MetaCommand {
    MetaCommand::ReportNodeVersion {
        node: node.clone(),
        range,
        build: build.unwrap_or_default(),
    }
}

/// Precondition P and, when it holds, the era-start proposals: one
/// `ReportNodeVersion` per node in `view.required`, in ascending node order.
/// Empty when P does not hold. See the module doc.
///
/// `control_nodes` is every control voter and learner (they are required for P
/// even if not (yet) registered in `Metadata`, but only registered nodes get a
/// report, since the apply rejects a report from an unregistered node).
#[must_use]
pub fn era_start_proposals(
    view: &VersionView,
    control_nodes: &BTreeSet<NodeId>,
    observations: &BTreeMap<NodeId, VersionObservation>,
    self_id: &NodeId,
    own: &OwnVersion,
    now: Nanos,
    window: Duration,
) -> Vec<MetaCommand> {
    if view.era_active {
        return Vec::new();
    }
    // The leader must itself be a B2 binary whose range holds the version.
    match own.range {
        Some(r) if r.contains(view.cluster_version) => {}
        _ => return Vec::new(),
    }
    let mut out = Vec::new();
    for node in view.required.union(control_nodes) {
        let Some((Some(range), build)) = view_of(node, self_id, own, observations, now, window)
        else {
            return Vec::new();
        };
        if !range.contains(view.cluster_version) {
            return Vec::new();
        }
        if view.required.contains(node) {
            out.push(report(node, range, build));
        }
    }
    out
}

/// Era-on upkeep: a `ReportNodeVersion` for every required node whose fresh
/// observation (within `window`) carries a range that contains the cluster
/// version and differs from (or has no) recorded entry. Empty while the era is
/// off or when this node has no own range (a Phase 1 profile proposes
/// nothing).
#[must_use]
pub fn era_on_proposals(
    view: &VersionView,
    observations: &BTreeMap<NodeId, VersionObservation>,
    self_id: &NodeId,
    own: &OwnVersion,
    now: Nanos,
    window: Duration,
) -> Vec<MetaCommand> {
    if !view.era_active || own.range.is_none() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for node in &view.required {
        let Some((Some(range), build)) = view_of(node, self_id, own, observations, now, window)
        else {
            continue;
        };
        if !range.contains(view.cluster_version) {
            continue;
        }
        let want = NodeVersion {
            range,
            build: build.clone().unwrap_or_default(),
        };
        if view.recorded.get(node) != Some(&want) {
            out.push(report(node, range, build));
        }
    }
    out
}

/// The node a version command is about (for the loop's per-node rate limit).
#[must_use]
pub fn command_node(cmd: &MetaCommand) -> Option<&NodeId> {
    match cmd {
        MetaCommand::ReportNodeVersion { node, .. } => Some(node),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_env::handshake::encode_ext;
    use animus_env::nid;

    fn ext(range: Option<(u32, u32)>, build: Option<&str>) -> Arc<[u8]> {
        Arc::from(encode_ext(range, build))
    }

    fn obs_of(range: Option<(u32, u32)>, build: &str, at: u64) -> VersionObservation {
        VersionObservation {
            range: range.map(|(a, b)| VersionRange::new(a, b)),
            build: Some(build.to_string()),
            observed_at: Nanos(at),
        }
    }

    fn view(required: &[u64], era: bool) -> VersionView {
        VersionView {
            era_active: era,
            cluster_version: 1,
            required: required.iter().copied().map(nid).collect(),
            recorded: BTreeMap::new(),
        }
    }

    #[test]
    fn observe_parses_empty_malformed_and_valid_ext() {
        let mut t = VersionObservations::default();
        t.observe(&nid(1), &ext(None, None), Nanos(1));
        assert_eq!(t.get(&nid(1)).unwrap().range, None);
        t.observe(&nid(2), &Arc::from(vec![1u8, 0, 99]), Nanos(2));
        assert_eq!(t.get(&nid(2)).unwrap().range, None, "malformed -> Phase 1");
        t.observe(&nid(3), &ext(Some((1, 2)), Some("b2")), Nanos(3));
        let o = t.get(&nid(3)).unwrap();
        assert_eq!(o.range, Some(VersionRange::new(1, 2)));
        assert_eq!(o.build.as_deref(), Some("b2"));
        // An unchanged ext only refreshes the time.
        t.observe(&nid(3), &ext(Some((1, 2)), Some("b2")), Nanos(9));
        assert_eq!(t.get(&nid(3)).unwrap().observed_at, Nanos(9));
        // A changed ext (a restart on another build) replaces the entry.
        t.observe(&nid(3), &ext(None, None), Nanos(10));
        assert_eq!(t.get(&nid(3)).unwrap().range, None);
    }

    fn b2_own() -> OwnVersion {
        OwnVersion {
            range: Some(crate::version::own_range()),
            ..OwnVersion::default()
        }
    }

    #[test]
    fn the_default_profile_is_phase_1_and_never_proposes() {
        assert_eq!(OwnVersion::default().range, None);
    }

    #[test]
    fn p_needs_every_required_and_control_node_fresh_and_b2() {
        let own = b2_own();
        let w = OBSERVATION_WINDOW;
        let now = Nanos(1_000_000_000);
        let fresh = obs_of(Some((1, 1)), "b2", now.0);
        let mut obs: BTreeMap<NodeId, VersionObservation> = BTreeMap::new();
        obs.insert(nid(2), fresh.clone());
        let ctl: BTreeSet<NodeId> = [nid(1), nid(2), nid(3)].into_iter().collect();
        let v = view(&[1, 2], false);
        let go = |obs: &BTreeMap<NodeId, VersionObservation>| {
            era_start_proposals(&v, &ctl, obs, &nid(1), &own, now, w)
        };
        // Control node 3 never observed: blocks.
        assert!(go(&obs).is_empty());
        obs.insert(nid(3), fresh.clone());
        let p = go(&obs);
        // Reports only for the required set (1 is self, 2 observed).
        assert_eq!(p.len(), 2);
        // Stale observation blocks.
        obs.insert(nid(3), obs_of(Some((1, 1)), "b2", 0));
        assert!(go(&obs).is_empty());
        // Phase 1 (None) blocks.
        obs.insert(nid(3), obs_of(None, "", now.0));
        assert!(go(&obs).is_empty());
    }

    #[test]
    fn p_needs_the_range_to_contain_the_cluster_version() {
        let own = b2_own();
        let now = Nanos(1_000_000_000);
        let mut obs = BTreeMap::new();
        obs.insert(nid(2), obs_of(Some((2, 3)), "future", now.0));
        let ctl: BTreeSet<NodeId> = BTreeSet::new();
        let v = view(&[1, 2], false);
        assert!(
            era_start_proposals(&v, &ctl, &obs, &nid(1), &own, now, OBSERVATION_WINDOW).is_empty()
        );
    }

    #[test]
    fn a_phase1_profile_leader_never_proposes() {
        let own = OwnVersion {
            range: None,
            build: String::new(),
            ..OwnVersion::default()
        };
        let now = Nanos(1_000_000_000);
        let mut obs = BTreeMap::new();
        obs.insert(nid(2), obs_of(Some((1, 1)), "b2", now.0));
        let ctl = BTreeSet::new();
        assert!(
            era_start_proposals(
                &view(&[1, 2], false),
                &ctl,
                &obs,
                &nid(1),
                &own,
                now,
                OBSERVATION_WINDOW
            )
            .is_empty()
        );
        assert!(
            era_on_proposals(
                &view(&[1, 2], true),
                &obs,
                &nid(1),
                &own,
                now,
                OBSERVATION_WINDOW
            )
            .is_empty()
        );
    }

    #[test]
    fn era_on_reports_only_changed_or_missing_records() {
        let own = b2_own();
        let now = Nanos(1_000_000_000);
        let mut obs = BTreeMap::new();
        obs.insert(nid(2), obs_of(Some((1, 2)), "b3", now.0));
        obs.insert(nid(3), obs_of(Some((1, 1)), "b2", now.0));
        obs.insert(nid(4), obs_of(None, "", now.0)); // Phase 1: never reported
        let mut v = view(&[1, 2, 3, 4], true);
        v.recorded.insert(
            nid(2),
            NodeVersion {
                range: VersionRange::new(1, 1),
                build: "b2".into(),
            },
        );
        v.recorded.insert(
            nid(3),
            NodeVersion {
                range: VersionRange::new(1, 1),
                build: "b2".into(),
            },
        );
        let p = era_on_proposals(&v, &obs, &nid(1), &own, now, OBSERVATION_WINDOW);
        let nodes: Vec<_> = p.iter().filter_map(command_node).cloned().collect();
        // 1 (self, no record) and 2 (changed); 3 is current; 4 is Phase 1.
        assert_eq!(nodes, vec![nid(1), nid(2)]);
    }
}
