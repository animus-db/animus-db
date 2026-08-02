//! Tablet model: contiguous, sorted key ranges that are the unit of placement
//! and migration, each with a replica set and a monotonic epoch (see
//! `docs/adr/0002-tablets-unit-of-placement.md`).
//!
//! Ranges support [`KeyRange::split_at`] and [`KeyRange::abuts`] (the
//! primitives behind control-plane tablet split/merge); the epoch is the
//! fencing token used by the data plane (ADR 0001). Automatic split-point
//! selection and replica rebalancing on split/merge remain future work.

use animus_env::NodeId;
use serde::{Deserialize, Serialize};

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

    /// Split into `[start, at)` and `[at, end)`. Returns `None` unless `at` lies
    /// strictly inside the range (`start < at < end`), so neither side is empty.
    #[must_use]
    pub fn split_at(&self, at: &[u8]) -> Option<(KeyRange, KeyRange)> {
        if at <= self.start.as_slice() {
            return None;
        }
        if let Some(end) = self.end.as_deref() {
            if at >= end {
                return None;
            }
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

    /// Whether this range is immediately followed by `next` (`self.end ==
    /// next.start`), so the two can be merged into one contiguous range.
    #[must_use]
    pub fn abuts(&self, next: &KeyRange) -> bool {
        self.end.as_deref() == Some(next.start.as_slice())
    }
}

/// A tablet: a key range, its replica set, and its placement epoch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tablet {
    /// The tablet's identity.
    pub id: TabletId,
    /// The key range this tablet owns.
    pub range: KeyRange,
    /// The nodes replicating this tablet, sorted and deduplicated.
    pub replicas: Vec<NodeId>,
    /// The current placement epoch.
    pub epoch: Epoch,
}

impl Tablet {
    /// Create a tablet at [`Epoch::INITIAL`] with a normalized replica set.
    #[must_use]
    pub fn new(id: TabletId, range: KeyRange, replicas: Vec<NodeId>) -> Self {
        let mut replicas = replicas;
        replicas.sort_unstable();
        replicas.dedup();
        Self {
            id,
            range,
            replicas,
            epoch: Epoch::INITIAL,
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
    fn replicas_are_normalized() {
        let t = Tablet::new(TabletId(1), KeyRange::whole(), vec![3, 1, 2, 1]);
        assert_eq!(t.replicas, vec![1, 2, 3]);
        assert!(t.has_replica(2));
        assert!(!t.has_replica(9));
        assert_eq!(t.epoch, Epoch::INITIAL);
    }

    #[test]
    fn split_partitions_the_range() {
        let (left, right) = KeyRange::whole().split_at(b"m").unwrap();
        assert!(left.contains(b"a") && left.contains(b"l"));
        assert!(!left.contains(b"m"));
        assert!(right.contains(b"m") && right.contains(b"z"));
        // The two halves are adjacent and recombine to the whole keyspace.
        assert!(left.abuts(&right));
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
    fn abuts_only_when_contiguous() {
        let a = KeyRange::new(b"a".to_vec(), Some(b"m".to_vec()));
        let b = KeyRange::new(b"m".to_vec(), Some(b"z".to_vec()));
        let gap = KeyRange::new(b"n".to_vec(), Some(b"z".to_vec()));
        assert!(a.abuts(&b));
        assert!(!a.abuts(&gap));
        assert!(
            !KeyRange::whole().abuts(&b),
            "unbounded-above range abuts nothing"
        );
    }
}
