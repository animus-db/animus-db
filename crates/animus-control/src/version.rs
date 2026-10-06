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
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::meta::Metadata;

/// A cluster version. `1` is "everything in Phase 1 and B2"; real gates start
/// at `2`. See the module doc for why this is an alias.
pub type ClusterVersion = u32;

/// The highest cluster version this binary can run at.
pub const MAX_SUPPORTED: ClusterVersion = 3;

/// The lowest cluster version this binary can run at.
///
/// ADR 0073 decision 7's N-1/N skew policy would make this `max - 1` (it was
/// `1` while `MAX_SUPPORTED <= 2`), but **it is held at `1` through
/// `MAX_SUPPORTED = 3`** (G-01 stage G-d M1, ADR 0073's 2026-10-05
/// amendment): the version era starts at cluster version `1` (the first
/// applied `ReportNodeVersion`) and `Metadata::apply` rejects a report whose
/// range excludes the *current* cluster version, so a binary with `min = 2`
/// could never report into a fresh cluster and the era could never start.
/// Raising the floor therefore needs the era-start rule redesigned first (an
/// ADR amendment naming the stepping-stone release, as before). Holding it
/// at `1` costs nothing in safety: the gates still open one finalize step at
/// a time (each requires every registered node's range to contain the
/// target), decoders accept every version forever, and a v1 -> v3 skip is
/// merely *unsupported and untested* (no mixed-version cell covers it), not
/// refused.
pub const MIN_SUPPORTED: ClusterVersion = 1;

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
/// [`Gate::version`] (naming the ADR/PR that introduces it), in
/// [`Gate::rank`] and in [`Gate::ALL`]; the exhaustive matches make
/// forgetting any a build error.
///
/// Variants are declared in **opening order** (`Base` first, then `Era`, then
/// version gates by ascending version), which is what the derived `Ord`
/// follows. Do not use `Ord` for "is this gate open"; use
/// [`ClusterFeatures::is_open`]. Use [`Gate::rank`] / [`Gate::join`] to find the
/// strictest of several gates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Gate {
    /// Everything Phase 1 and B2 emit before the era: always open (ADR 0073
    /// P2-B). The gate of every variant that exists today, so a
    /// `required_gate` table can name *something* for it. Version 1, which
    /// `cluster_version() >= 1` always satisfies.
    Base,
    /// The *era*: open iff the cluster has started versioning
    /// (`Metadata::versioning_active()`, a sticky marker: the first applied
    /// report starts it and nothing ends it), i.e. the era-only
    /// commands/entities may be emitted. Not tied to
    /// a cluster version (ADR 0073 section 2, P2-A).
    Era,
    /// **Global tables** (ADR 0075, G-01 stage G-c): the first real version
    /// gate, opening at cluster version 2. Guards `MetaCommand::
    /// ConvertTableToGlobal` and the replicated shapes it writes
    /// (`TableSchema.global`, `PlacementPolicy.allowed_values`) plus client
    /// acceptance of the multi-Region `UpdateTable` surface. One gate for the
    /// whole release surface (everything ships at one version: one finalize
    /// step, one set of mixed-version cells); `MrecReplication` (stage G-d)
    /// will be its own gate. It is also the release's gate for **`txn-envelope`
    /// v2** (ADR 0073's 2026-10-05 amendment, #1237): the tablet snapshot
    /// image ships v1 intents until it opens.
    GlobalTables,
    /// **MREC global tables** (ADR 0075, G-01 stage G-d): the second real
    /// version gate, opening at cluster version 3. Guards the eventual
    /// (multi-Region eventual-consistency) mode: `MultiRegionConsistency::
    /// Eventual`, the MREC replica-set fields of `GlobalTableSpec`,
    /// `MetaCommand::{ConvertTableToMrec, AddMrecReplica, RemoveMrecReplica,
    /// SetMrecReplicaStatus}`, and the data-plane shapes (`WriteSchema.mrec`,
    /// `KindEvalOp::Replicate`). It is its own gate, not `GlobalTables`,
    /// because a Release(2) voter (G-c) already knows `GlobalTables` yet
    /// cannot decode any of these.
    MrecReplication,
    /// A **synthetic** version gate `n` (test/sim builds only): opens at
    /// cluster version `n`, ranks `n`. It exists so the gate *ladder*
    /// (several version gates, each opening at its own finalize) can be
    /// exercised before a real release adds a second gate; no production
    /// `required_gate` row names it. Reached only through the
    /// `synthetic.gate` member label ([`SYNTHETIC_GATE_LABEL`]).
    #[cfg(any(test, feature = "sim-versions"))]
    Synthetic(ClusterVersion),
}

/// The `UpsertMember` label that marks a command as requiring
/// `Gate::Synthetic(n)` (value: the decimal `n`, `>= 2`); the only way a
/// synthetic gate is attached to a real command. Test/sim builds only.
#[cfg(any(test, feature = "sim-versions"))]
pub const SYNTHETIC_GATE_LABEL: &str = "synthetic.gate";

impl Gate {
    /// Every gate, in declaration order.
    pub const ALL: &'static [Gate] = &[
        Gate::Base,
        Gate::Era,
        Gate::GlobalTables,
        Gate::MrecReplication,
    ];

    /// The cluster version at which this gate opens, or `None` for a gate
    /// opened by the era rather than by a version. Exhaustive: no wildcard.
    #[must_use]
    pub const fn version(self) -> Option<ClusterVersion> {
        match self {
            // ADR 0073 P2-B: everything that exists at version 1.
            Gate::Base => Some(1),
            // ADR 0073 P2-A: era-gated, no version.
            Gate::Era => None,
            // ADR 0075 (G-01 stage G-c): the first real version gate.
            Gate::GlobalTables => Some(2),
            // ADR 0075 (G-01 stage G-d): the second real version gate.
            Gate::MrecReplication => Some(3),
            #[cfg(any(test, feature = "sim-versions"))]
            Gate::Synthetic(n) => Some(n),
        }
    }

    /// Strictness rank: a higher rank opens later. `Base` is 0; `Era` is 1
    /// (it ranks below every version gate above 1: such a gate is only
    /// reachable once a finalize raised the version, and `Finalize` itself is
    /// era-only); a version gate `v` ranks `v`. Exhaustive: no wildcard.
    #[must_use]
    pub const fn rank(self) -> u32 {
        match self {
            Gate::Base => 0,
            Gate::Era => 1,
            Gate::GlobalTables => 2,
            Gate::MrecReplication => 3,
            #[cfg(any(test, feature = "sim-versions"))]
            Gate::Synthetic(n) => {
                if n > 1 {
                    n
                } else {
                    1
                }
            }
        }
    }

    /// The stricter (later-opening) of two gates: what a composite value
    /// (a batch, a nested message) requires.
    #[must_use]
    pub const fn join(self, other: Gate) -> Gate {
        if other.rank() > self.rank() {
            other
        } else {
            self
        }
    }
}

/// A value whose emission is gated: the gate that must be open before this
/// value may be proposed or sent to another node (ADR 0073 section 3).
/// Implementations are an **exhaustive match with no `_` arm** so a new
/// variant does not compile until it names its gate.
pub trait GatedCommand {
    /// The gate that must be open to emit `self`.
    fn required_gate(&self) -> Gate;
}

/// A cross-node surface a gate check guards; one violation counter each
/// ([`ClusterFeatures::violations`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GateSurface {
    /// Control `RaftMsg` (JSON).
    RaftMsg,
    /// `MetaCommand` (control log entries, `ProposeSchema`).
    MetaCommand,
    /// `KvWire` (cp-data wire frames).
    KvWire,
    /// `KvCommand` (cp-data log entries).
    KvCommand,
    /// `ClientRequest` frames.
    ClientRequest,
    /// `ClientResponse` frames.
    ClientResponse,
}

impl GateSurface {
    /// Every surface, in counter-slot order.
    pub const ALL: &'static [GateSurface] = &[
        GateSurface::RaftMsg,
        GateSurface::MetaCommand,
        GateSurface::KvWire,
        GateSurface::KvCommand,
        GateSurface::ClientRequest,
        GateSurface::ClientResponse,
    ];

    const fn slot(self) -> usize {
        match self {
            GateSurface::RaftMsg => 0,
            GateSurface::MetaCommand => 1,
            GateSurface::KvWire => 2,
            GateSurface::KvCommand => 3,
            GateSurface::ClientRequest => 4,
            GateSurface::ClientResponse => 5,
        }
    }

    /// A stable lowercase name (metric label / log field).
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            GateSurface::RaftMsg => "raft_msg",
            GateSurface::MetaCommand => "meta_command",
            GateSurface::KvWire => "kv_wire",
            GateSurface::KvCommand => "kv_command",
            GateSurface::ClientRequest => "client_request",
            GateSurface::ClientResponse => "client_response",
        }
    }
}

#[derive(Debug)]
struct Inner {
    cluster_version: AtomicU32,
    era_active: AtomicBool,
    violations: [AtomicU64; GateSurface::ALL.len()],
}

/// A cheap-clone, per-node handle on the cluster's feature state. All clones
/// share one cell; there is no global. Fed from a `&Metadata` by
/// [`update`](Self::update) (the control apply task's job, wired by P2-B/C).
///
/// It also holds the per-surface **gate-violation counters** (ADR 0073 section
/// 3): [`check`](Self::check) bumps one when an emitter tries to emit
/// something whose gate is closed. `animus-env`'s `Metric` enum is outside
/// P2-B's scope, so the release-build "metric" is these counters; P2-C
/// exports them through `MetricsHandle::set`.
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
                violations: std::array::from_fn(|_| AtomicU64::new(0)),
            }),
        }
    }

    /// Refresh from the replicated `Metadata`.
    ///
    /// **Monotonic**: the version only rises and the era flag, once set,
    /// stays set. Gates only ever open (ADR 0073 section 3; a finalize cannot
    /// be undone and the era marker is sticky), and two feeders (the apply
    /// task and a proposer re-reading the applied cache) may race, so an
    /// older view must never close a gate a newer one opened.
    pub fn update(&self, meta: &Metadata) {
        self.inner
            .cluster_version
            .fetch_max(meta.cluster_version(), Ordering::AcqRel);
        if meta.versioning_active() {
            self.inner.era_active.store(true, Ordering::Release);
        }
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

    /// The emit-site check: `true` when `gate` is open. When closed, bumps
    /// the `surface` violation counter, logs an error, and `debug_assert!`s
    /// (a test or debug build fails loudly); in a release build it never
    /// panics, and the caller must **refuse** to emit on `false`.
    #[must_use]
    pub fn check(&self, surface: GateSurface, gate: Gate) -> bool {
        if self.is_open(gate) {
            return true;
        }
        self.inner.violations[surface.slot()].fetch_add(1, Ordering::Relaxed);
        tracing::error!(
            surface = surface.name(),
            ?gate,
            "gate violation: refusing to emit a value whose feature gate is closed"
        );
        debug_assert!(
            false,
            "gate violation on {}: {gate:?} is closed",
            surface.name()
        );
        false
    }

    /// Gate violations counted on `surface` since this handle was created.
    #[must_use]
    pub fn violations(&self, surface: GateSurface) -> u64 {
        self.inner.violations[surface.slot()].load(Ordering::Relaxed)
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
    fn floor_is_held_at_one_so_the_era_can_start() {
        const { assert!(MIN_SUPPORTED >= 1) };
        const { assert!(MIN_SUPPORTED <= MAX_SUPPORTED) };
        // The era starts at cluster version 1 and a report is rejected when
        // its range excludes the current version, so the floor must stay 1
        // until the era-start rule is redesigned (see `MIN_SUPPORTED`).
        assert_eq!(MIN_SUPPORTED, 1);
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
                Gate::Base => Some(1),
                Gate::Era => None,
                // ADR 0075 (G-01 G-c): the first real version gate.
                Gate::GlobalTables => Some(2),
                // ADR 0075 (G-01 G-d): the second.
                Gate::MrecReplication => Some(3),
                // Never in `ALL` (parametric, test/sim only): see the ladder test.
                Gate::Synthetic(_) => unreachable!("synthetic gates are not in Gate::ALL"),
            };
            assert_eq!(g.version(), expected, "{g:?}");
            if let Some(v) = g.version() {
                // `Base` is the one gate at version 1: always open.
                assert!(
                    g == Gate::Base || v > MIN_SUPPORTED,
                    "{g:?}: gating at the floor is no gate"
                );
            }
        }
        assert!(seen.contains(&Gate::Base) && seen.contains(&Gate::Era));
        assert!(seen.contains(&Gate::GlobalTables));
        assert_eq!(Gate::Era.join(Gate::GlobalTables), Gate::GlobalTables);
        assert_eq!(Gate::GlobalTables.join(Gate::Base), Gate::GlobalTables);
        assert!(seen.contains(&Gate::MrecReplication));
        assert_eq!(
            Gate::GlobalTables.join(Gate::MrecReplication),
            Gate::MrecReplication
        );
        assert_eq!(
            MAX_SUPPORTED, 3,
            "G-01 G-d is the release that takes MAX to 3"
        );
        assert_eq!(MIN_SUPPORTED, 1);
        // Declaration order is opening order: ranks are non-decreasing.
        let ranks: Vec<u32> = Gate::ALL.iter().map(|g| g.rank()).collect();
        assert!(ranks.windows(2).all(|w| w[0] < w[1]), "{ranks:?}");
        assert_eq!(Gate::Base.join(Gate::Era), Gate::Era);
        assert_eq!(Gate::Era.join(Gate::Base), Gate::Era);
        assert_eq!(Gate::Base.join(Gate::Base), Gate::Base);
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
        assert!(f.is_open(Gate::Base), "Base is always open");
        for &gate in Gate::ALL.iter().filter(|g| **g != Gate::Base) {
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

    #[test]
    fn check_counts_and_refuses_a_closed_gate_per_surface() {
        let f = ClusterFeatures::new();
        assert!(f.check(GateSurface::MetaCommand, Gate::Base));
        for &s in GateSurface::ALL {
            assert_eq!(f.violations(s), 0);
        }
        // A closed gate debug-asserts in a debug build; observe the counter
        // through the release path only when assertions are off.
        if !cfg!(debug_assertions) {
            assert!(!f.check(GateSurface::KvWire, Gate::Era));
            assert_eq!(f.violations(GateSurface::KvWire), 1);
            assert_eq!(f.violations(GateSurface::RaftMsg), 0);
        }
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "gate violation on raft_msg")]
    fn check_debug_asserts_on_a_closed_gate() {
        let f = ClusterFeatures::new();
        let _ = f.check(GateSurface::RaftMsg, Gate::Era);
    }

    #[test]
    fn surface_slots_are_distinct_and_cover_all() {
        let mut slots: Vec<usize> = GateSurface::ALL.iter().map(|s| s.slot()).collect();
        slots.sort_unstable();
        assert_eq!(slots, (0..GateSurface::ALL.len()).collect::<Vec<_>>());
    }

    /// The synthetic gate ladder (ADR 0073 P2-D): several version gates, each
    /// opening at its own finalize, observed through one `ClusterFeatures`.
    /// Only `Gate::Base` and `Gate::Era` exist in production, so the ladder
    /// is the only place `is_open` on a version gate above the era is
    /// exercised end to end.
    #[test]
    fn synthetic_gates_open_one_finalize_at_a_time() {
        let (g2, g3) = (Gate::Synthetic(2), Gate::Synthetic(3));
        assert_eq!((g2.version(), g3.version()), (Some(2), Some(3)));
        assert!(Gate::Era.rank() < g2.rank() && g2.rank() < g3.rank());
        assert_eq!(Gate::Era.join(g3), g3);
        assert_eq!(g3.join(g2), g3);
        assert_eq!(g2.join(Gate::Base), g2);

        let f = ClusterFeatures::new();
        let mut meta = Metadata::default();
        registered(&mut meta, 1);
        meta.apply(&MetaCommand::ReportNodeVersion {
            node: nid(1),
            range: VersionRange::new(1, 3),
            build: "r3".into(),
        });
        f.update(&meta);
        assert!(f.is_open(Gate::Era) && !f.is_open(g2) && !f.is_open(g3));

        for (expected, target) in [(1, 2), (2, 3)] {
            let out = meta.apply(&MetaCommand::FinalizeClusterVersion { expected, target });
            assert_eq!(out, crate::meta::ApplyOutcome::Applied, "finalize {target}");
            f.update(&meta);
            assert_eq!(f.is_open(g2), target >= 2, "g2 at {target}");
            assert_eq!(f.is_open(g3), target >= 3, "g3 at {target}");
        }
        // A later view never closes a gate an earlier one opened.
        f.update(&Metadata::default());
        assert!(f.is_open(g3));
    }

    /// `required_gate` classification of the synthetic marker: the label
    /// raises an `UpsertMember`, and a `RaftMsg` carrying one joins to it.
    #[test]
    fn synthetic_label_classifies_commands_and_carrying_messages() {
        use crate::meta::NodeStatus;
        let plain = MetaCommand::UpsertMember {
            node: nid(1),
            labels: Default::default(),
            status: NodeStatus::Active,
        };
        assert_eq!(plain.required_gate(), Gate::Base);
        for n in [2u32, 3] {
            let marked = MetaCommand::UpsertMember {
                node: nid(1),
                labels: [(SYNTHETIC_GATE_LABEL.to_owned(), n.to_string())].into(),
                status: NodeStatus::Active,
            };
            assert_eq!(marked.required_gate(), Gate::Synthetic(n));
        }
        let garbage = MetaCommand::UpsertMember {
            node: nid(1),
            labels: [(SYNTHETIC_GATE_LABEL.to_owned(), "x".to_owned())].into(),
            status: NodeStatus::Active,
        };
        assert_eq!(garbage.required_gate(), Gate::Base, "unparsable is no gate");
    }
}
