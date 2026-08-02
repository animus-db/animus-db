//! Segment digests for range-based anti-entropy (ADR 0010).
//!
//! Anti-entropy converges replicas by reconciling their data, but a *full-push*
//! of every entry each round is `O(data)` even when the replicas already agree.
//! A segment digest is the classic Merkle/range-digest optimization: each
//! replica buckets its keys into a fixed number of segments and summarizes each
//! with an order-independent content hash plus an entry count. Two replicas
//! compare digests, and only the segments whose `(hash, count)` disagree carry
//! any entry data over the wire — a converged pair transfers nothing.
//!
//! This module is **pure and deterministic**: the bucket of a key and the hash
//! of an entry are fixed functions of their bytes, so every replica computes the
//! same digest for the same data, on every node and on replay (ADR 0003). No
//! clock, no RNG, no `Env`.

use crate::{SegmentDigest, SyncEntry};

/// Number of segments the keyspace is partitioned into. A small fixed fan-out:
/// enough that a single divergent key isolates a small slice of the data, few
/// enough that the digest itself stays tiny. Not security-sensitive — only a
/// load-shedding bucket — so a cheap stable hash suffices.
pub const SEGMENTS: u32 = 64;

/// FNV-1a over `bytes` (stable, deterministic, dependency-free).
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// The segment a `key` falls in: a stable hash of the key bytes, bucketed.
#[must_use]
pub fn segment_of(key: &[u8]) -> u32 {
    (fnv1a(key) % u64::from(SEGMENTS)) as u32
}

/// The per-entry content hash folded (by XOR) into a segment's hash. Covers the
/// key, the version, and the value/tombstone, so any divergence in any of them
/// flips the segment hash. XOR-folding makes the segment hash order-independent.
fn entry_hash(key: &[u8], value: &Option<Vec<u8>>, version: u64) -> u64 {
    let mut buf = Vec::with_capacity(key.len() + 16);
    buf.extend_from_slice(&(key.len() as u64).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&version.to_le_bytes());
    match value {
        Some(v) => {
            buf.push(1);
            buf.extend_from_slice(v);
        }
        None => buf.push(0),
    }
    fnv1a(&buf)
}

/// Summarize a replica's `entries` (each `(key, Option<value>, version)`, as
/// from `StorageEngine::entries_with_tombstones`) into one [`SegmentDigest`] per
/// non-empty segment, sorted by segment index. Empty segments are omitted (an
/// absent segment is implicitly `count = 0`).
#[must_use]
pub fn digest(entries: &[SyncEntry]) -> Vec<SegmentDigest> {
    // BTreeMap keeps segments sorted ⇒ deterministic output order.
    let mut acc: std::collections::BTreeMap<u32, (u64, u32)> = std::collections::BTreeMap::new();
    for (key, value, version) in entries {
        let seg = segment_of(key);
        let e = acc.entry(seg).or_insert((0, 0));
        e.0 ^= entry_hash(key, value, *version);
        e.1 += 1;
    }
    acc.into_iter()
        .map(|(segment, (hash, count))| SegmentDigest {
            segment,
            hash,
            count,
        })
        .collect()
}

/// The segments where the receiver (`mine`) disagrees with a peer's digest
/// (`theirs`): any segment whose `(hash, count)` differs, or that one side has
/// and the other lacks. Sorted, deduplicated. These are exactly the segments
/// worth pulling — a converged pair yields an empty set.
#[must_use]
pub fn divergent(mine: &[SegmentDigest], theirs: &[SegmentDigest]) -> Vec<u32> {
    use std::collections::BTreeMap;
    let m: BTreeMap<u32, (u64, u32)> = mine
        .iter()
        .map(|s| (s.segment, (s.hash, s.count)))
        .collect();
    let t: BTreeMap<u32, (u64, u32)> = theirs
        .iter()
        .map(|s| (s.segment, (s.hash, s.count)))
        .collect();
    let mut out: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
    for (&seg, mv) in &m {
        if t.get(&seg) != Some(mv) {
            out.insert(seg);
        }
    }
    for &seg in t.keys() {
        if !m.contains_key(&seg) {
            out.insert(seg);
        }
    }
    out.into_iter().collect()
}

/// The subset of `entries` falling in any of `segments` (sorted set membership).
/// Used to answer a `SyncPull` with only the requested segments' data.
#[must_use]
pub fn entries_in_segments(entries: &[SyncEntry], segments: &[u32]) -> Vec<SyncEntry> {
    let wanted: std::collections::BTreeSet<u32> = segments.iter().copied().collect();
    entries
        .iter()
        .filter(|(k, _, _)| wanted.contains(&segment_of(k)))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(k: &[u8], v: Option<&[u8]>, ver: u64) -> (Vec<u8>, Option<Vec<u8>>, u64) {
        (k.to_vec(), v.map(<[u8]>::to_vec), ver)
    }

    #[test]
    fn identical_data_has_identical_digest_and_no_divergence() {
        let a = vec![e(b"a", Some(b"1"), 1), e(b"b", Some(b"2"), 2)];
        let b = vec![e(b"b", Some(b"2"), 2), e(b"a", Some(b"1"), 1)]; // different order
        let da = digest(&a);
        let db = digest(&b);
        assert_eq!(da, db, "digest must be order-independent");
        assert!(divergent(&da, &db).is_empty());
    }

    #[test]
    fn a_single_changed_value_diverges_only_its_segment() {
        let a = vec![e(b"a", Some(b"1"), 1), e(b"b", Some(b"2"), 2)];
        let mut b = a.clone();
        b[0] = e(b"a", Some(b"CHANGED"), 3);
        let diff = divergent(&digest(&a), &digest(&b));
        assert_eq!(diff, vec![segment_of(b"a")]);
        // The other key's segment is untouched (unless it collides, which these
        // two keys do not under SEGMENTS=64).
        assert_ne!(segment_of(b"a"), segment_of(b"b"));
    }

    #[test]
    fn a_tombstone_differs_from_the_value_it_replaces() {
        let val = vec![e(b"k", Some(b"v"), 1)];
        let tomb = vec![e(b"k", None, 2)];
        assert!(!divergent(&digest(&val), &digest(&tomb)).is_empty());
    }

    #[test]
    fn missing_segment_on_one_side_diverges() {
        let a = vec![e(b"a", Some(b"1"), 1)];
        let b: Vec<_> = vec![];
        let diff = divergent(&digest(&a), &digest(&b));
        assert_eq!(diff, vec![segment_of(b"a")]);
    }

    #[test]
    fn entries_in_segments_selects_only_requested() {
        let all = vec![e(b"a", Some(b"1"), 1), e(b"b", Some(b"2"), 2)];
        let sel = entries_in_segments(&all, &[segment_of(b"a")]);
        assert_eq!(sel, vec![e(b"a", Some(b"1"), 1)]);
    }
}
