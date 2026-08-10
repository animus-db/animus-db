//! **Bounded per-node in-memory ring of system-keyspace deltas** (ADR 0038
//! PR5, "Phase 2: incremental watch deltas"). The apply task
//! (`node.rs::meta_apply_and_compact`) pushes each drained command's derived
//! [`KeyWrite`]s here, keyed by the command's own Raft log index, so
//! `WatchMetadata` can answer with just the writes since a caller's
//! `last_seen` watermark instead of a full `Metadata` clone (`RaftNode::
//! watch_delta_since`).
//!
//! Bounded by **both** entry count and total byte size — oldest evicted
//! first — and deliberately **per-node and best-effort**: nothing here is
//! replicated or agreed on, and no correctness property depends on its
//! contents surviving. A caller whose `last_seen` has aged out of the
//! window (or predates a snapshot install / restart, which [`DeltaRing::
//! clear`] resets to empty) gets `None` back from [`DeltaRing::writes_since`],
//! telling it to fall back to a full fetch — mirroring the existing
//! log-tail-vs-`InstallSnapshot` fallback shape this plane already has for
//! catching up a lagging Raft follower.

use std::collections::VecDeque;

use crate::mirror::KeyWrite;

/// Default entry-count bound (see [`DeltaRing::default`]).
pub const DEFAULT_MAX_ENTRIES: usize = 1024;
/// Default total-byte bound (see [`DeltaRing::default`]).
pub const DEFAULT_MAX_BYTES: usize = 4 * 1024 * 1024;

/// One drained command's ring entry: the Raft log index it was applied at,
/// plus the (possibly empty — a `NoOp`/rejected command derives nothing)
/// `KeyWrite`s it produced.
#[derive(Clone, Debug, PartialEq, Eq)]
struct DeltaEntry {
    index: u64,
    writes: Vec<KeyWrite>,
}

/// A rough, stable accounting of one entry's footprint against the byte
/// bound: the index itself plus every write's key/value bytes. Not exact
/// wire-serialized size (that would need to run `serde_json` per push) —
/// just monotonic in what's actually retained, which is all a bound needs.
fn entry_size(entry: &DeltaEntry) -> usize {
    let writes: usize = entry
        .writes
        .iter()
        .map(|w| match w {
            KeyWrite::Put(k, v) => k.len() + v.len(),
            KeyWrite::Delete(k) => k.len(),
        })
        .sum();
    std::mem::size_of::<u64>() + writes
}

/// The bounded ring itself. See the module doc for the shape/contract.
pub struct DeltaRing {
    entries: VecDeque<DeltaEntry>,
    bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}

impl Default for DeltaRing {
    fn default() -> Self {
        Self::with_bounds(DEFAULT_MAX_ENTRIES, DEFAULT_MAX_BYTES)
    }
}

impl DeltaRing {
    /// A ring with explicit bounds — the "configurable" half of the design
    /// (defaults via [`DeltaRing::default`]/[`crate::RaftNode::start`]; a
    /// caller that wants tighter bounds, e.g. a test proving eviction
    /// behavior without pushing thousands of entries, uses this directly).
    #[must_use]
    pub fn with_bounds(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            bytes: 0,
            max_entries,
            max_bytes,
        }
    }

    /// Reset to empty. Called by the apply task whenever its `shadow`/`cache`
    /// was just rebuilt from a jump the ring itself didn't witness (a
    /// received `InstallSnapshot`, or this task's own startup/restart
    /// rebuild) — the ring's coverage window is meaningless across such a
    /// jump, so every watcher correctly falls back to a full fetch until the
    /// ring has accumulated fresh coverage again.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }

    /// Push one drained command's derived writes at `index`, evicting the
    /// oldest entries first if this exceeds either bound. Every push must be
    /// at a strictly higher index than the previous one (the apply task's
    /// own commit-order discipline) — debug-asserted, not a runtime-enforced
    /// invariant, since this type is a read-side cache no safety property
    /// depends on; a violation would only ever be an internal apply-task bug.
    pub fn push(&mut self, index: u64, writes: Vec<KeyWrite>) {
        debug_assert!(
            self.entries.back().is_none_or(|back| index > back.index),
            "delta ring entries must be pushed in strictly increasing index order"
        );
        let entry = DeltaEntry { index, writes };
        self.bytes += entry_size(&entry);
        self.entries.push_back(entry);
        // Never evict the entry just pushed, even if it alone exceeds
        // `max_bytes` (there is nothing smaller left to evict down to, and a
        // ring that discarded its own freshest entry would be self-defeating
        // — every push must leave at least one entry retained).
        while self.entries.len() > 1
            && (self.entries.len() > self.max_entries || self.bytes > self.max_bytes)
        {
            let Some(evicted) = self.entries.pop_front() else {
                break;
            };
            self.bytes -= entry_size(&evicted);
        }
    }

    /// The writes strictly after `last_seen` up to and including `upto`,
    /// flattened in commit order — or `None` if the ring doesn't (or no
    /// longer) contiguously cover that range: `upto` itself might be ahead
    /// of what the ring has accumulated so far (a narrow race against the
    /// apply task's own push, resolved by falling back), or `last_seen`
    /// might have already aged out of the retained window (a gap at the
    /// front — the eviction case), or the ring might simply be empty (fresh,
    /// or just [`clear`](Self::clear)ed).
    ///
    /// `last_seen >= upto` (nothing to report) is always `Some(vec![])` — the
    /// trivial case needs no ring coverage at all, so an idle long-poll's
    /// timeout-elapsed reply is always cheap, ring or no ring.
    #[must_use]
    pub fn writes_since(&self, last_seen: u64, upto: u64) -> Option<Vec<KeyWrite>> {
        if last_seen >= upto {
            return Some(Vec::new());
        }
        let front = self.entries.front()?;
        if last_seen + 1 < front.index {
            return None; // the ring has already evicted this range
        }
        let back = self.entries.back()?;
        if back.index < upto {
            return None; // the ring hasn't caught up to `upto` yet
        }
        let mut writes = Vec::new();
        for entry in self
            .entries
            .iter()
            .filter(|e| e.index > last_seen && e.index <= upto)
        {
            writes.extend(entry.writes.iter().cloned());
        }
        Some(writes)
    }

    /// The number of entries currently retained (test/introspection only —
    /// `pub(crate)` so `node.rs`'s own white-box apply-task tests can assert
    /// on it too).
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// The current total tracked byte footprint (test/introspection only).
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(key: &[u8], value: &[u8]) -> KeyWrite {
        KeyWrite::Put(key.to_vec(), value.to_vec())
    }

    #[test]
    fn empty_ring_answers_only_the_trivial_no_op_case() {
        let ring = DeltaRing::default();
        assert_eq!(ring.writes_since(5, 5), Some(Vec::new()));
        assert_eq!(ring.writes_since(0, 3), None);
    }

    #[test]
    fn contiguous_range_is_covered_and_flattened_in_order() {
        let mut ring = DeltaRing::default();
        ring.push(1, vec![put(b"a", b"1")]);
        ring.push(2, vec![put(b"b", b"2"), KeyWrite::Delete(b"a".to_vec())]);
        ring.push(3, vec![put(b"c", b"3")]);

        assert_eq!(
            ring.writes_since(0, 3),
            Some(vec![
                put(b"a", b"1"),
                put(b"b", b"2"),
                KeyWrite::Delete(b"a".to_vec()),
                put(b"c", b"3"),
            ])
        );
        // A partial range still flattens correctly.
        assert_eq!(
            ring.writes_since(1, 2),
            Some(vec![put(b"b", b"2"), KeyWrite::Delete(b"a".to_vec())])
        );
        // Nothing new since the ring's own head.
        assert_eq!(ring.writes_since(3, 3), Some(Vec::new()));
    }

    #[test]
    fn upto_ahead_of_the_ring_falls_back() {
        let mut ring = DeltaRing::default();
        ring.push(1, vec![put(b"a", b"1")]);
        // The ring hasn't caught up to index 5 yet (a narrow race against a
        // concurrent apply-task push) — fall back rather than under-report.
        assert_eq!(ring.writes_since(0, 5), None);
    }

    #[test]
    fn entry_count_eviction_produces_a_gap_fallback() {
        let mut ring = DeltaRing::with_bounds(3, usize::MAX);
        for i in 1..=5u64 {
            ring.push(i, vec![put(b"k", &i.to_be_bytes())]);
        }
        assert_eq!(ring.len(), 3, "only the newest 3 entries are retained");
        // Indices 1 and 2 were evicted — a caller stuck at last_seen=1 has
        // fallen outside the window.
        assert_eq!(ring.writes_since(1, 5), None);
        // But a caller at last_seen=2 (the last evicted index) is exactly at
        // the boundary the retained window still covers.
        assert_eq!(
            ring.writes_since(2, 5),
            Some(vec![
                put(b"k", &3u64.to_be_bytes()),
                put(b"k", &4u64.to_be_bytes()),
                put(b"k", &5u64.to_be_bytes()),
            ])
        );
    }

    #[test]
    fn byte_bound_eviction_from_one_huge_entry() {
        // A single entry whose own footprint exceeds the byte bound still
        // gets retained (there is nothing smaller to evict down to), but it
        // evicts every entry that came before it.
        let mut ring = DeltaRing::with_bounds(1024, 100);
        ring.push(1, vec![put(b"a", b"small")]);
        ring.push(2, vec![put(b"b", b"also-small")]);
        assert_eq!(ring.len(), 2);

        let huge_value = vec![0u8; 200];
        ring.push(3, vec![KeyWrite::Put(b"big".to_vec(), huge_value.clone())]);
        assert_eq!(
            ring.len(),
            1,
            "the huge entry must evict everything smaller that preceded it"
        );
        assert!(
            ring.bytes() > 100,
            "the retained entry alone exceeds the byte bound"
        );

        // Indices 1 and 2 are gone. A caller at last_seen=1 needs index 2's
        // content (which was evicted) — a real gap, falls back.
        assert_eq!(ring.writes_since(1, 3), None);
        // A caller at last_seen=2 only needs index 3 onward, and index 3 is
        // exactly the ring's (huge, retained) front entry — no gap, still
        // covered, even though index 2 itself is gone.
        assert_eq!(
            ring.writes_since(2, 3),
            Some(vec![KeyWrite::Put(b"big".to_vec(), huge_value.clone())]),
            "last_seen+1 landing exactly on the ring's front is contiguous, not a gap"
        );
        assert_eq!(
            ring.writes_since(3, 3),
            Some(Vec::new()),
            "nothing new past the ring's own head is always trivially covered"
        );
    }

    #[test]
    fn clear_resets_the_ring_to_empty() {
        let mut ring = DeltaRing::default();
        ring.push(1, vec![put(b"a", b"1")]);
        ring.push(2, vec![put(b"b", b"2")]);
        assert_eq!(
            ring.writes_since(0, 2),
            Some(vec![put(b"a", b"1"), put(b"b", b"2")])
        );

        ring.clear();
        assert_eq!(ring.len(), 0);
        assert_eq!(ring.bytes(), 0);
        // The exact same range that was covered a moment ago is now a gap —
        // this is the "after an InstallSnapshot / restart rebuild, the ring
        // resets to empty and mirrors hit the full-reply fallback" contract.
        assert_eq!(ring.writes_since(0, 2), None);
        // A fresh push after clearing starts a brand-new, independently
        // covered window.
        ring.push(3, vec![put(b"c", b"3")]);
        assert_eq!(ring.writes_since(2, 3), Some(vec![put(b"c", b"3")]));
    }
}
