//! Cluster version, per-binary version range, and feature gates (ADR 0073
//! Phase 2, workstream P2-A).
//!
//! **What this module is.** A binary supports a closed range of *cluster
//! versions* ([`own_range`]); the cluster as a whole runs at one
//! [`ClusterVersion`] recorded in [`Metadata`] (`cluster_version()`), raised one
//! step at a time by `MetaCommand::FinalizeClusterVersion` once every
//! registered node has reported a range containing the target. A [`Gate`]
//! names a cross-node surface (wire shape, replicated command, ...) that may
//! only be *emitted* once the cluster version reaches the gate's version.
//! Decoders always accept every version and `Metadata::apply` never consults
//! a gate (ADR 0073 decision 4): gates are consulted by *proposers and
//! emitters* only, so replicated state stays a pure function of the log.
//!
//! **Type choice.** [`ClusterVersion`] is a plain `u32` alias, not a newtype:
//! it is persisted as a bare `u32` in `Metadata` (an additive, skipped-at-
//! default field), and the ADR's wire TLV is a bare `u32` LE, so a newtype
//! would only add conversions at every boundary.
//!
//! **Floor semantics.** [`ClusterFeatures`] starts at the *floor*
//! ([`MIN_SUPPORTED`], era off) until it is first fed a `Metadata`, so a node
//! that has not yet read the replicated version never opens a gate it cannot
//! prove the cluster has reached.
//!
//! Pure: no `Env`, no I/O, no global state. Each node owns its own
//! [`ClusterFeatures`] handle.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use serde::{Deserialize, Serialize};

use crate::meta::Metadata;

/// A cluster version. `1` is "everything in Phase 1 and B2"; real gates start
/// at `2`. See the module doc for why this is an alias.
pub type ClusterVersion = u32;

/// The highest cluster version this binary can run at.
pub const MAX_SUPPORTED: ClusterVersion = 1;

/// The lowest cluster version this binary can still emit for: `max - 1`,
/// floored at `1` (ADR 0073 decision 7, N-1 and N skew only). Raised only by
/// an ADR amendment naming the stepping-stone release; durable readability
/// stays forever regardless.
pub const MIN_SUPPORTED: ClusterVersion = if MAX_SUPPORTED > 1 {
    MAX_SUPPORTED - 1
} else {
    1
};

/// A closed range `[min, max]` of cluster versions a binary supports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRange {
    /// Lowest cluster version supported.
    pub min: ClusterVersion,
    /// Highest cluster version supported.
    pub max: ClusterVersion,
}

impl VersionRange {
    /// A range `[min, max]` (not validated; see [`is_valid`](Self::is_valid)).
    #[must_use]
    pub const fn new(min: ClusterVersion, max: ClusterVersion) -> Self {
        Self { min, max }
    }

    /// Well-formed: `1 <= min <= max` (versions start at 1; `0` is the
    /// "absent" encoding of `Metadata::cluster_version`, never a real version).
    #[must_use]
    pub const fn is_valid(&self) -> bool {
        self.min >= 1 && self.min <= self.max
    }

    /// Whether `v` lies inside the range.
    #[must_use]
    pub const fn contains(&self, v: ClusterVersion) -> bool {
        self.min <= v && v <= self.max
    }

    /// Whether the two ranges share at least one version.
    #[must_use]
    pub const fn intersects(&self, other: &VersionRange) -> bool {
        self.min <= other.max && other.min <= self.max
    }

    /// The handshake `ext` TLV tag-1 *value*: `min:u32 LE ++ max:u32 LE`
    /// (ADR 0073 section 1). The TLV framing itself lives in `animus-env`.
    #[must_use]
    pub fn to_ext_value(&self) -> [u8; 8] {
        let mut out = [0u8; 8];
        out[..4].copy_from_slice(&self.min.to_le_bytes());
        out[4..].copy_from_slice(&self.max.to_le_bytes());
        out
    }

    /// Inverse of [`to_ext_value`](Self::to_ext_value). `None` unless the
    /// value is exactly 8 bytes and decodes to a [valid](Self::is_valid)
    /// range.
    #[must_use]
    pub fn from_ext_value(value: &[u8]) -> Option<Self> {
        let value: [u8; 8] = value.try_into().ok()?;
        let min = u32::from_le_bytes(value[..4].try_into().ok()?);
        let max = u32::from_le_bytes(value[4..].try_into().ok()?);
        let range = Self { min, max };
        range.is_valid().then_some(range)
    }

    /// The startup/install range check (ADR 0073 section 1): `None` when
    /// `cluster_version` lies inside this range, else the named refusal
    /// message a halted node reports.
    #[must_use]
    pub fn exclusion_message(&self, cluster_version: ClusterVersion) -> Option<String> {
        if cluster_version > self.max {
            Some(format!(
                "cluster version {cluster_version} is above this binary's max {} \
                 (downgrade is not supported)",
                self.max
            ))
        } else if cluster_version < self.min {
            Some(format!(
                "cluster version {cluster_version} is below this binary's min {} \
                 (upgrade through a release whose range contains {cluster_version} first)",
                self.min
            ))
        } else {
            None
        }
    }

    /// The range an empty handshake `ext` denotes: a Phase 1 binary, `[1, 1]`.
    #[must_use]
    pub const fn phase1() -> Self {
        Self { min: 1, max: 1 }
    }
}

/// This binary's own supported range, `[MIN_SUPPORTED, MAX_SUPPORTED]`.
#[must_use]
pub const fn own_range() -> VersionRange {
    VersionRange::new(MIN_SUPPORTED, MAX_SUPPORTED)
}

/// A node's replicated version record (`Metadata::node_versions`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeVersion {
    /// The cluster-version range the node's binary supports.
    pub range: VersionRange,
    /// Build string (display only; never interpreted).
    pub build: String,
}

/// A cross-node feature gate. Add a variant here, give it a row in
/// [`Gate::version`] (naming the ADR/PR that introduces it) and in
/// [`Gate::ALL`]; the exhaustive match makes forgetting either a build error.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Gate {
    /// The *era*: open iff the cluster has started versioning
    /// (`Metadata::versioning_active()`, a sticky marker: the first applied
    /// report starts it and nothing ends it), i.e. the era-only
    /// commands/entities may be emitted. Not tied to
    /// a cluster version (ADR 0073 section 2, P2-A).
    Era,
}

impl Gate {
    /// Every gate, in declaration order.
    pub const ALL: &'static [Gate] = &[Gate::Era];

    /// The cluster version at which this gate opens, or `None` for a gate
    /// opened by the era rather than by a version. Exhaustive: no wildcard.
    #[must_use]
    pub const fn version(self) -> Option<ClusterVersion> {
        match self {
            // ADR 0073 P2-A (this PR): era-gated, no version.
            Gate::Era => None,
        }
    }
}

#[derive(Debug)]
struct Inner {
    cluster_version: AtomicU32,
    era_active: AtomicBool,
}

/// A cheap-clone, per-node handle on the cluster's feature state. All clones
/// share one cell; there is no global. Fed from a `&Metadata` by
/// [`update`](Self::update) (the control apply task's job, wired by P2-B/C).
#[derive(Clone, Debug)]
pub struct ClusterFeatures {
    inner: Arc<Inner>,
}

impl Default for ClusterFeatures {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterFeatures {
    /// A handle at the floor: [`MIN_SUPPORTED`], era off. Every gate that
    /// needs a version above the floor, and the era gate, is closed.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                cluster_version: AtomicU32::new(MIN_SUPPORTED),
                era_active: AtomicBool::new(false),
            }),
        }
    }

    /// Refresh from the replicated `Metadata`.
    pub fn update(&self, meta: &Metadata) {
        self.inner
            .cluster_version
            .store(meta.cluster_version(), Ordering::Release);
        self.inner
            .era_active
            .store(meta.versioning_active(), Ordering::Release);
    }

    /// The cluster version last observed (the floor before any update).
    #[must_use]
    pub fn cluster_version(&self) -> ClusterVersion {
        self.inner.cluster_version.load(Ordering::Acquire)
    }

    /// Whether the era has started, as last observed.
    #[must_use]
    pub fn era_active(&self) -> bool {
        self.inner.era_active.load(Ordering::Acquire)
    }

    /// Whether `gate` is open: version-gated gates open once the observed
    /// cluster version reaches the gate's version; era-gated gates
    /// (`version() == None`) open iff the era is active.
    #[must_use]
    pub fn is_open(&self, gate: Gate) -> bool {
        match gate.version() {
            Some(v) => self.cluster_version() >= v,
            None => self.era_active(),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn exclusion_message_names_above_max_and_below_min() {
        let r = VersionRange::new(2, 3);
        assert_eq!(r.exclusion_message(2), None);
        assert_eq!(r.exclusion_message(3), None);
        assert_eq!(
            r.exclusion_message(4).as_deref(),
            Some("cluster version 4 is above this binary's max 3 (downgrade is not supported)")
        );
        assert_eq!(
            r.exclusion_message(1).as_deref(),
            Some(
                "cluster version 1 is below this binary's min 2 \
                     (upgrade through a release whose range contains 1 first)"
            )
        );
    }

    use animus_env::nid;

    use super::*;
    use crate::meta::{MetaCommand, NodeAddrs};

    #[test]
    fn floor_formula_is_max_minus_one_floored_at_one() {
        const { assert!(MIN_SUPPORTED >= 1) };
        const { assert!(MIN_SUPPORTED <= MAX_SUPPORTED) };
        assert_eq!(MIN_SUPPORTED, MAX_SUPPORTED.saturating_sub(1).max(1));
        assert_eq!(own_range(), VersionRange::new(MIN_SUPPORTED, MAX_SUPPORTED));
    }

    #[test]
    fn range_contains_and_intersects() {
        let r = VersionRange::new(2, 4);
        assert!(!r.contains(1) && r.contains(2) && r.contains(4) && !r.contains(5));
        assert!(r.intersects(&VersionRange::new(4, 9)));
        assert!(r.intersects(&VersionRange::new(1, 2)));
        assert!(!r.intersects(&VersionRange::new(5, 9)));
        assert!(!r.intersects(&VersionRange::new(1, 1)));
        assert!(VersionRange::new(1, 1).is_valid());
        assert!(!VersionRange::new(0, 1).is_valid());
        assert!(!VersionRange::new(3, 2).is_valid());
    }

    #[test]
    fn ext_value_round_trips_and_refuses_malformed() {
        let r = VersionRange::new(1, 3);
        let bytes = r.to_ext_value();
        assert_eq!(bytes, [1, 0, 0, 0, 3, 0, 0, 0]);
        assert_eq!(VersionRange::from_ext_value(&bytes), Some(r));
        assert_eq!(VersionRange::from_ext_value(&bytes[..7]), None);
        assert_eq!(VersionRange::from_ext_value(&[0; 9]), None);
        assert_eq!(VersionRange::from_ext_value(&[0; 8]), None, "min 0 invalid");
        assert_eq!(
            VersionRange::from_ext_value(&VersionRange::new(3, 2).to_ext_value()),
            None
        );
    }

    /// The gate table is exhaustive: the `match` below fails to compile when
    /// a `Gate` is added without a row here, `ALL` must list every gate once,
    /// and a version-gated gate never opens below the floor-plus-one.
    #[test]
    fn gate_table_is_exhaustive() {
        let mut seen = std::collections::BTreeSet::new();
        for &g in Gate::ALL {
            assert!(seen.insert(g), "duplicate {g:?} in Gate::ALL");
            let expected: Option<ClusterVersion> = match g {
                Gate::Era => None,
            };
            assert_eq!(g.version(), expected, "{g:?}");
            if let Some(v) = g.version() {
                assert!(v > MIN_SUPPORTED, "{g:?}: gating at the floor is no gate");
            }
        }
        assert!(seen.contains(&Gate::Era));
    }

    fn registered(meta: &mut Metadata, n: u64) {
        let node = nid(n);
        let addrs = NodeAddrs {
            internal: format!("i{n}"),
            client: format!("c{n}"),
            intra: format!("x{n}"),
            admin: format!("a{n}"),
            role: "control".into(),
        };
        meta.apply(&MetaCommand::RegisterNode {
            node,
            addrs,
            labels: Default::default(),
        });
    }

    #[test]
    fn cluster_features_floor_then_updates() {
        let f = ClusterFeatures::new();
        let g = f.clone();
        assert_eq!(f.cluster_version(), MIN_SUPPORTED);
        assert!(!f.era_active());
        for &gate in Gate::ALL {
            assert!(!f.is_open(gate), "{gate:?} must be closed at the floor");
        }

        let mut meta = Metadata::default();
        registered(&mut meta, 1);
        f.update(&meta);
        assert!(!g.is_open(Gate::Era), "registered but no report yet");

        meta.apply(&MetaCommand::ReportNodeVersion {
            node: nid(1),
            range: own_range(),
            build: "t".into(),
        });
        f.update(&meta);
        assert!(g.is_open(Gate::Era), "clones share one cell");
        assert_eq!(g.cluster_version(), meta.cluster_version());
    }
}
