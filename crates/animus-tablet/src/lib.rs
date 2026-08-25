//! Tablet model: contiguous, sorted key ranges that are the unit of placement
//! and migration, each with a replica set and a monotonic epoch (see
//! `docs/adr/0002-tablets-unit-of-placement.md`).
//!
//! Ranges support [`KeyRange::split_at`] and [`KeyRange::abuts`] (the
//! primitives behind control-plane tablet split — tablets are split-only,
//! ADR 0044; `abuts` was originally merge's own adjacency check and has no
//! production caller now); the epoch is the fencing token used by the data
//! plane (ADR 0001).
//!
//! Each table is its own **hash ring** (ADR 0022/0023): a data-plane key is
//! `partition_token(pk) || escape(pk) || rk` — **no table prefix**; the table is
//! tablet metadata ([`Tablet::table`]) and an explicit routing argument, and
//! partitions are spread by the [`partition_token`] (a Murmur3 hash of the
//! partition key). A tablet is
//! **scoped to one table** (`Tablet::table`) and owns a contiguous `[start, end)`
//! sub-range of that table's keyspace — a token sub-range — so the whole
//! range/epoch/split machinery is reused unchanged while load spreads
//! evenly and one partition's rows stay contiguous + sort-ordered (the token is
//! over the partition key only, so all of a partition's keys share a prefix). A
//! table starts as one tablet and **splits on demand** as it grows.

use animus_env::NodeId;
#[cfg(test)]
use animus_env::nid;
use serde::{Deserialize, Serialize};

/// Width, in bytes, of a [`partition_token`] — a big-endian `u64`.
pub const TOKEN_BYTES: usize = 8;

/// The 64-bit **partition token** for a partition key: the top 64 bits of
/// MurmurHash3 (x64, 128-bit, seed 0) — the same hash Cassandra's
/// `Murmur3Partitioner` uses — returned big-endian so byte order equals numeric
/// order (a [`KeyRange`] byte comparison over the token prefix then *is* a token
/// comparison). It leads every data-plane key (`token || escape(pk) || rk`,
/// ADR 0022/0023 — no table prefix; the table is a routing argument), spreading
/// a table's partitions evenly across that table's ring.
///
/// **Every node and every restart must agree**: the same partition key always
/// routes to the same tablet, so this is a fixed, seedless algorithm with no
/// RNG or process/host state. Do not change the hash without a data migration —
/// the bytes are baked into stored keys.
#[must_use]
pub fn partition_token(partition_key: &[u8]) -> [u8; TOKEN_BYTES] {
    // Cassandra reduces the 128-bit hash to a token via its first 64-bit half.
    murmur3_x64_128(partition_key, 0).0.to_be_bytes()
}

/// MurmurHash3 128-bit, x64 variant (the algorithm behind Cassandra's
/// `Murmur3Partitioner`). Deterministic and allocation-free; returns both 64-bit
/// halves `(h1, h2)`. Reference: Austin Appleby's `MurmurHash3.cpp`.
fn murmur3_x64_128(data: &[u8], seed: u32) -> (u64, u64) {
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;

    let mut h1 = u64::from(seed);
    let mut h2 = u64::from(seed);

    let mut blocks = data.chunks_exact(16);
    for block in &mut blocks {
        let mut k1 = u64::from_le_bytes(block[0..8].try_into().unwrap());
        let mut k2 = u64::from_le_bytes(block[8..16].try_into().unwrap());

        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dc_e729);

        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;
        h2 = h2.rotate_left(31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x3849_5ab5);
    }

    let tail = blocks.remainder();
    let mut k1 = 0u64;
    let mut k2 = 0u64;
    // Tail bytes, little-endian: bytes 8.. feed k2, bytes 0..8 feed k1.
    if tail.len() > 8 {
        for (i, &b) in tail.iter().enumerate().skip(8) {
            k2 ^= u64::from(b) << (8 * (i - 8));
        }
        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;
    }
    if !tail.is_empty() {
        for (i, &b) in tail.iter().enumerate().take(8) {
            k1 ^= u64::from(b) << (8 * i);
        }
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= data.len() as u64;
    h2 ^= data.len() as u64;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    (h1, h2)
}

/// Murmur3's 64-bit finalization mix.
fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

/// The order-preserving, **prefix-free** escape of `bytes`: a byte stream whose
/// encoding never prefixes another's (each `0x00` is doubled to `0x00 0x01`, and
/// the whole is terminated by `0x00 0x00`). This must match the wire adapters'
/// `escape` byte-for-byte, because a table-scoped tablet's [`KeyRange`] is computed
/// from `escape(table)` here and must line up with the `escape(table)`-prefixed
/// data keys the adapters write (ADR 0023). It is duplicated here (rather than
/// shared from `animus-dynamo`) so the keyspace crate stays dependency-light and
/// the control plane can compute a table's block without an adapter dependency.
#[must_use]
pub fn escape(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 2);
    for &b in bytes {
        out.push(b);
        if b == 0x00 {
            out.push(0x01);
        }
    }
    out.extend_from_slice(&[0x00, 0x00]);
    out
}

/// Stable identifier for a tablet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TabletId(pub u64);

/// Monotonic version of a tablet's placement (range + replica set). Bumped on
/// every change; used by the data plane as a fencing token.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Epoch(pub u64);

impl Epoch {
    /// The epoch a freshly created tablet starts at.
    pub const INITIAL: Epoch = Epoch(1);

    /// The next epoch after this one.
    #[must_use]
    pub fn next(self) -> Epoch {
        Epoch(self.0 + 1)
    }
}

/// A half-open key range `[start, end)`. `end == None` means unbounded above, so
/// `KeyRange::whole()` covers the entire keyspace.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct KeyRange {
    /// Inclusive lower bound (empty vector = unbounded below).
    pub start: Vec<u8>,
    /// Exclusive upper bound, or `None` for unbounded above.
    pub end: Option<Vec<u8>>,
}

impl KeyRange {
    /// The range covering the entire keyspace.
    #[must_use]
    pub fn whole() -> Self {
        Self {
            start: Vec::new(),
            end: None,
        }
    }

    /// A half-open range `[start, end)`.
    #[must_use]
    pub fn new(start: impl Into<Vec<u8>>, end: Option<Vec<u8>>) -> Self {
        Self {
            start: start.into(),
            end,
        }
    }

    /// Whether `key` falls within this range.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && self.end.as_deref().is_none_or(|e| key < e)
    }

    /// Whether `other` is fully contained within this range (`other ⊆ self`).
    /// The range-level counterpart of [`contains`](Self::contains) — used by
    /// the tablet-host reconciler's narrow-only/widen-only checks (ADR 0031/
    /// 0033) and by read-path scope pre-checks (a scan whose requested bounds
    /// exceed a group's live scope must be retried, not silently truncated).
    #[must_use]
    pub fn contains_range(&self, other: &KeyRange) -> bool {
        if other.start < self.start {
            return false;
        }
        match (&other.end, &self.end) {
            (_, None) => true,
            (None, Some(_)) => false,
            (Some(inner_end), Some(outer_end)) => inner_end <= outer_end,
        }
    }

    /// Split into `[start, at)` and `[at, end)`. Returns `None` unless `at` lies
    /// strictly inside the range (`start < at < end`), so neither side is empty.
    #[must_use]
    pub fn split_at(&self, at: &[u8]) -> Option<(KeyRange, KeyRange)> {
        if at <= self.start.as_slice() {
            return None;
        }
        if let Some(end) = self.end.as_deref()
            && at >= end
        {
            return None;
        }
        let left = KeyRange {
            start: self.start.clone(),
            end: Some(at.to_vec()),
        };
        let right = KeyRange {
            start: at.to_vec(),
            end: self.end.clone(),
        };
        Some((left, right))
    }
}

/// A table name — the catalog identifier a tablet is scoped to. A bare string;
/// the control plane treats it as opaque (the wire adapters own namespacing). A
/// duplicate of `animus_control::TableName` kept here so the tablet model has no
/// dependency on the control crate (`animus-control` depends on `animus-tablet`,
/// not the reverse).
pub type TableName = String;

/// One child of an **in-place split** (ADR 0058 Train 2, rung 3): a tablet id
/// minted up front (before any data has moved) plus its placement-chosen
/// final replica set. Carried by [`InPlaceSplitIntent`] and by the data
/// plane's own `KvCommand::SplitTablet` — the SAME pair of `(id, replicas)`
/// rides both the control-plane intent and the data-plane fork entry, so
/// every replica derives the identical two child configs from identical
/// inputs (the design's own "same inputs on every replica" requirement).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitChild {
    /// The child's tablet id, minted from the same monotonic allocator a
    /// copy-based split's `Building` children use — reserved (never
    /// reused) the instant the intent is recorded, even though (unlike a
    /// copy-based `Building` child) no [`Tablet`] map entry exists for it
    /// until `MetaCommand::CutoverSplit` activates it.
    pub id: TabletId,
    /// The child's placement-chosen final replica set (fork F5, preserved
    /// from the copy-based design) — the **union** of both children's
    /// replica sets is what the parent's own group adds as learners
    /// (ADR 0058 Train 2 Stage 1).
    pub replicas: Vec<NodeId>,
}

/// A tablet's **in-place split intent** (ADR 0058 Train 2 rung 3, Stage 1):
/// recorded on the parent by `MetaCommand::BeginSplitInPlace`'s apply once
/// placement has chosen both children's final homes and their ids are
/// minted — the smallest representation that lets every node's reconciler
/// (`animus-cp-data::host`) discover "this hosted tablet is splitting
/// in-place, here are its two children's ids/homes" from replicated
/// `Metadata` alone, with **no** `Building` tablet-map entries (unlike the
/// copy-based workflow — no data has moved yet, so there is nothing to
/// place a routable-but-empty entry over). `split_key` is carried
/// verbatim, not re-derived, so every replica splits the parent's range
/// identically ([`KeyRange::split_at`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InPlaceSplitIntent {
    /// The key the parent's range splits at (left half first, matching the
    /// copy-based `BeginSplit` convention).
    pub split_key: Vec<u8>,
    /// Exactly two children, left half first.
    pub children: [SplitChild; 2],
}

/// A tablet: a **table-scoped** key range, its replica set, and its placement
/// epoch.
///
/// A tablet belongs to exactly one table (`table`): every key it owns is data of
/// that table, so a tablet never mixes two tables' rows. The `range` is then a
/// sub-range *within that table's* keyspace. The special `table: None` tablet is
/// the legacy/bootstrap **whole-keyspace** tablet that serves *any* table (it
/// predates table scoping); routing prefers a table-scoped tablet over it. A fresh
/// cluster bootstraps a table-scoped tablet per table, so `None` only appears in
/// snapshots written before scoping existed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tablet {
    /// The tablet's identity.
    pub id: TabletId,
    /// The table this tablet is scoped to (`None` = the legacy whole-keyspace
    /// tablet that serves every table). `#[serde(default)]` keeps pre-scoping
    /// snapshots loading as `None`.
    #[serde(default)]
    pub table: Option<TableName>,
    /// The key range this tablet owns (within `table`'s keyspace).
    pub range: KeyRange,
    /// The nodes replicating this tablet, sorted and deduplicated.
    pub replicas: Vec<NodeId>,
    /// The current placement epoch.
    pub epoch: Epoch,
    /// The tablet's lifecycle state (ADR 0050). `#[serde(default)]` keeps
    /// pre-lifecycle snapshots loading as `Active`.
    #[serde(default)]
    pub state: TabletState,
    /// This parent's **in-place split intent** (ADR 0058 Train 2 rung 3),
    /// if a `MetaCommand::BeginSplitInPlace` is mid-workflow on it —
    /// `None` for every ordinary tablet and for a copy-based `Splitting`
    /// parent (whose children instead get real `Building` map entries).
    /// `#[serde(default)]` keeps every pre-existing snapshot loading as
    /// `None`.
    #[serde(default)]
    pub inplace_split: Option<InPlaceSplitIntent>,
}

/// A tablet's lifecycle state (ADR 0050, copy-based splits).
///
/// - `Active` — the steady state: routable, rebalance-eligible, splittable.
/// - `Building` — a split child being seeded by the split driver: hosted (its
///   group runs, its engine fills) but **unroutable** and frozen for
///   placement until `CutoverSplit` activates it.
/// - `Splitting` — a split parent mid-workflow: still fully serving (reads
///   AND writes, until the B5 freeze) but frozen for placement and not
///   re-splittable; removed from the tablet map at cutover.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TabletState {
    /// Routable, rebalance-eligible, splittable — the steady state.
    #[default]
    Active,
    /// A split child under construction: hosted but unroutable.
    Building,
    /// A split parent mid-workflow: serving, but frozen for placement.
    Splitting,
}

impl Tablet {
    /// Create a **legacy whole-keyspace** tablet (`table: None`) at
    /// [`Epoch::INITIAL`] with a normalized replica set. Prefer
    /// [`Tablet::new_for_table`] for a table-scoped tablet.
    #[must_use]
    pub fn new(id: TabletId, range: KeyRange, replicas: Vec<NodeId>) -> Self {
        Self::with_table(id, None, range, replicas)
    }

    /// Create a **table-scoped** tablet at [`Epoch::INITIAL`] with a normalized
    /// replica set.
    #[must_use]
    pub fn new_for_table(
        id: TabletId,
        table: impl Into<TableName>,
        range: KeyRange,
        replicas: Vec<NodeId>,
    ) -> Self {
        Self::with_table(id, Some(table.into()), range, replicas)
    }

    /// Create a tablet with an explicit (optional) table scope.
    #[must_use]
    pub fn with_table(
        id: TabletId,
        table: Option<TableName>,
        range: KeyRange,
        replicas: Vec<NodeId>,
    ) -> Self {
        let mut replicas = replicas;
        replicas.sort_unstable();
        replicas.dedup();
        Self {
            id,
            table,
            range,
            replicas,
            epoch: Epoch::INITIAL,
            state: TabletState::default(),
            inplace_split: None,
        }
    }

    /// Whether client routing may serve keys from this tablet (ADR 0050): an
    /// `Active` tablet always; a `Splitting` parent still serves (reads AND
    /// writes) until the split workflow's freeze/cutover; a `Building` split
    /// child never does — its range **overlaps its parent's** (the parent's
    /// range is not narrowed at `BeginSplit`), so routing to it would serve
    /// a half-copied engine.
    #[must_use]
    pub fn is_routable(&self) -> bool {
        !matches!(self.state, TabletState::Building)
    }

    /// Whether this tablet can serve keys of `table`: a table-scoped tablet serves
    /// only its own table; the legacy `None` tablet serves any table.
    #[must_use]
    pub fn serves_table(&self, table: &str) -> bool {
        match &self.table {
            Some(t) => t == table,
            None => true,
        }
    }

    /// Whether `node` is a replica of this tablet.
    #[must_use]
    pub fn has_replica(&self, node: NodeId) -> bool {
        self.replicas.binary_search(&node).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whole_range_contains_everything() {
        let r = KeyRange::whole();
        assert!(r.contains(b""));
        assert!(r.contains(b"anything"));
    }

    #[test]
    fn half_open_bounds() {
        let r = KeyRange::new(b"b".to_vec(), Some(b"d".to_vec()));
        assert!(!r.contains(b"a"));
        assert!(r.contains(b"b"));
        assert!(r.contains(b"c"));
        assert!(!r.contains(b"d"));
    }

    #[test]
    fn contains_range_is_subset_containment() {
        let whole = KeyRange::whole();
        let bd = KeyRange::new(b"b".to_vec(), Some(b"d".to_vec()));
        let bc = KeyRange::new(b"b".to_vec(), Some(b"c".to_vec()));
        let cz = KeyRange::new(b"c".to_vec(), None);
        assert!(whole.contains_range(&bd));
        assert!(whole.contains_range(&whole));
        assert!(bd.contains_range(&bc));
        assert!(bd.contains_range(&bd));
        assert!(!bd.contains_range(&whole));
        assert!(
            !bd.contains_range(&cz),
            "unbounded end exceeds a bounded one"
        );
        assert!(!bc.contains_range(&bd), "longer end is not contained");
        assert!(cz.contains_range(&KeyRange::new(b"d".to_vec(), None)));
        assert!(!cz.contains_range(&KeyRange::new(b"a".to_vec(), Some(b"d".to_vec()))));
    }

    #[test]
    fn replicas_are_normalized() {
        let t = Tablet::new(
            TabletId(1),
            KeyRange::whole(),
            vec![nid(3), nid(1), nid(2), nid(1)],
        );
        assert_eq!(t.replicas, vec![nid(1), nid(2), nid(3)]);
        assert!(t.has_replica(nid(2)));
        assert!(!t.has_replica(nid(9)));
        assert_eq!(t.epoch, Epoch::INITIAL);
    }

    #[test]
    fn split_partitions_the_range() {
        let (left, right) = KeyRange::whole().split_at(b"m").unwrap();
        assert!(left.contains(b"a") && left.contains(b"l"));
        assert!(!left.contains(b"m"));
        assert!(right.contains(b"m") && right.contains(b"z"));
        // The two halves are adjacent and recombine to the whole keyspace.
    }

    #[test]
    fn split_rejects_out_of_range_or_boundary_keys() {
        let r = KeyRange::new(b"b".to_vec(), Some(b"d".to_vec()));
        assert!(
            r.split_at(b"b").is_none(),
            "split at start would make an empty left"
        );
        assert!(
            r.split_at(b"d").is_none(),
            "split at end would make an empty right"
        );
        assert!(r.split_at(b"a").is_none(), "split before start");
        assert!(r.split_at(b"e").is_none(), "split after end");
        assert!(r.split_at(b"c").is_some());
    }

    #[test]
    fn token_is_deterministic_and_fixed_width() {
        // Same input -> same token, every call (all nodes must agree).
        assert_eq!(partition_token(b"alice"), partition_token(b"alice"));
        assert_eq!(partition_token(b"alice").len(), TOKEN_BYTES);
        // Distinct partitions almost certainly land on distinct tokens.
        assert_ne!(partition_token(b"alice"), partition_token(b"bob"));
    }

    #[test]
    fn murmur3_empty_input_is_zero() {
        // Spec anchor: Murmur3 x64_128 of the empty input (seed 0) is (0, 0).
        assert_eq!(murmur3_x64_128(b"", 0), (0, 0));
        assert_eq!(partition_token(b""), [0u8; TOKEN_BYTES]);
    }

    #[test]
    fn tokens_spread_across_the_token_space() {
        // Murmur3 avalanches: 64 single-byte keys cover most octants of the
        // 64-bit token space (a low-entropy prefix hash would clump).
        let mut octants = std::collections::BTreeSet::new();
        for i in 0..64u8 {
            let token = u64::from_be_bytes(partition_token(&[i]));
            octants.insert(token >> 61); // top 3 bits = one of 8 octants.
        }
        assert!(octants.len() >= 6, "tokens should cover most octants");
    }

    #[test]
    fn table_scoped_tablet_serves_only_its_table() {
        let t = Tablet::new_for_table(TabletId(1), "users", KeyRange::whole(), vec![nid(1)]);
        assert_eq!(t.table.as_deref(), Some("users"));
        assert!(t.serves_table("users"));
        assert!(!t.serves_table("orders"));
        // The legacy whole-keyspace tablet serves any table.
        let legacy = Tablet::new(TabletId(2), KeyRange::whole(), vec![nid(1)]);
        assert_eq!(legacy.table, None);
        assert!(legacy.serves_table("users") && legacy.serves_table("orders"));
    }
}
