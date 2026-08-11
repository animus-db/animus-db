//! The per-tablet **read-timestamp cache** (ADR 0018 §2/PR2b, the concrete
//! mechanism for PR1's Amendment §2 write-conflict push): the serializability
//! half of the CP data plane's MVCC design. A write must never commit at a
//! timestamp `≤` a timestamp at which the affected keys were already served
//! to a reader — otherwise a reader could observe a snapshot that a later
//! "earlier" write then silently invalidates, breaking serializability even
//! though each single-key version is still well-formed MVCC.
//!
//! This is **leader-local, in-memory, best-effort acceleration** — not the
//! safety mechanism itself. It closes the *common* case cheaply (push a
//! concurrent write above a read it raced with, with no extra round trip);
//! the actual cross-leader-change safety net is the **logged read ceiling**
//! (`RaftKvNode`'s `committed_ceiling` + `KvCommand::ReadCeiling`, in
//! `lib.rs`), which this cache's `max_overlapping` also folds in as a floor.
//! Losing this cache (a leader crash, a restart) is always **safe** — it can
//! only make future writes more conservative (pushed higher than strictly
//! necessary), never less.
//!
//! ## Data structure
//!
//! Two-generation rotating [`BTreeMap`] (this codebase bans `HashMap`/`HashSet`
//! in logic, ADR 0003 — an ordinary interval tree isn't in the dependency
//! tree, and isn't needed for a cache this small): `current` accumulates new
//! entries; once it exceeds [`ROTATION_BOUND`], it rotates into `previous`
//! (which is discarded, its highest timestamp folded into `low_water`) and a
//! fresh empty generation starts. `low_water` is therefore a coarse floor
//! that only ever rises — every past read this cache has ever recorded, once
//! evicted, still pushes writes above **something**, just less precisely
//! than the original per-span entry did. **Over-conservative eviction is
//! safe, never wrong**: a write pushed higher than the minimum necessary is
//! still a correct write, just a marginally later-timestamped one.
//!
//! Each entry is a span `(start, end)` — `end: None` is unbounded above,
//! exactly like [`KeyRange`](animus_tablet::KeyRange) — mapped to the highest
//! timestamp any read of that exact span recorded. A point read (`local_get`)
//! is recorded as the singleton span `[key, key ++ [0x00])`: appending a zero
//! byte is the immediate lexicographic successor of `key` (no byte string
//! sorts strictly between `key` and `key ++ [0x00]`), so this span contains
//! `key` and nothing else.

use std::collections::BTreeMap;

use crate::hlc::HlcTimestamp;

/// Once `current`'s entry count exceeds this, it rotates into `previous`
/// (discarding the old `previous` into `low_water`). Bounds the cache's
/// memory and the cost of [`TsCache::max_overlapping`]'s linear scan.
const ROTATION_BOUND: usize = 4096;

/// A recorded span: `(start, end)`, `end: None` unbounded above — the same
/// half-open-range shape as [`KeyRange`](animus_tablet::KeyRange), duplicated
/// here as a plain tuple so it can be a `BTreeMap` key (`KeyRange` itself
/// doesn't derive `Ord`, and pulling in the whole type just for that would
/// couple this cache to more of that type's API than it needs).
type Span = (Vec<u8>, Option<Vec<u8>>);

/// The immediate lexicographic successor of `key`: no byte string sorts
/// strictly between `key` and `key ++ [0x00]`. Used to record a point read as
/// a span containing exactly that one key.
pub(crate) fn point_span(key: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
    let mut end = key.to_vec();
    end.push(0);
    (key.to_vec(), Some(end))
}

/// Whether `key` falls inside span `(start, end)` (half-open: `start ≤ key`,
/// and `key < end` when `end` is `Some`).
fn span_contains(start: &[u8], end: &Option<Vec<u8>>, key: &[u8]) -> bool {
    key >= start
        && match end {
            Some(e) => key < e.as_slice(),
            None => true,
        }
}

/// The per-tablet read-timestamp cache. See the module doc for the design.
#[derive(Debug)]
pub(crate) struct TsCache {
    current: BTreeMap<Span, HlcTimestamp>,
    previous: BTreeMap<Span, HlcTimestamp>,
    low_water: HlcTimestamp,
}

impl Default for TsCache {
    fn default() -> Self {
        Self::new()
    }
}

impl TsCache {
    #[must_use]
    pub(crate) fn new() -> Self {
        TsCache {
            current: BTreeMap::new(),
            previous: BTreeMap::new(),
            low_water: HlcTimestamp::zero(),
        }
    }

    /// Record that the span `[start, end)` was read at `ts`: bumps the
    /// recorded timestamp for that exact span (never regresses it — a
    /// re-bump at a lower `ts` than already recorded is a no-op), then
    /// rotates generations if `current` has grown past [`ROTATION_BOUND`].
    pub(crate) fn bump(&mut self, start: Vec<u8>, end: Option<Vec<u8>>, ts: HlcTimestamp) {
        let slot = self.current.entry((start, end)).or_insert(ts);
        if ts > *slot {
            *slot = ts;
        }
        if self.current.len() > ROTATION_BOUND {
            self.rotate();
        }
    }

    /// Rotate: the current `previous` generation is about to be replaced and
    /// is gone for good, so fold its highest timestamp into `low_water`
    /// before dropping it — every read it ever recorded still pushes future
    /// writes above at least that floor, just no longer precisely scoped to
    /// its original span.
    fn rotate(&mut self) {
        let dropped_max = self.previous.values().copied().max();
        if let Some(m) = dropped_max
            && m > self.low_water
        {
            self.low_water = m;
        }
        self.previous = std::mem::take(&mut self.current);
    }

    /// The highest recorded read timestamp overlapping any of `keys` —
    /// `low_water` if none overlap. This is what a propose-time write-push
    /// check compares its candidate `ts` against (see `lib.rs`'s
    /// `write_push_floor`): a write whose own `ts` doesn't strictly exceed
    /// this floor must be pushed above it (witness the floor, re-mint).
    ///
    /// A straightforward linear scan of both generations per key — the cache
    /// is bounded (`ROTATION_BOUND` per generation), and this is a
    /// leader-local, in-memory check with no I/O, so this is not the
    /// bottleneck an interval-tree would be justified for; a future PR can
    /// swap the representation without changing this method's contract if it
    /// ever needs to be.
    #[must_use]
    pub(crate) fn max_overlapping<K: AsRef<[u8]>>(&self, keys: &[K]) -> HlcTimestamp {
        let mut floor = self.low_water;
        for key in keys {
            let key = key.as_ref();
            for generation in [&self.current, &self.previous] {
                for ((start, end), ts) in generation {
                    if *ts > floor && span_contains(start, end, key) {
                        floor = *ts;
                    }
                }
            }
        }
        floor
    }

    /// Raise `low_water` to at least `ts` — used to fold the group's
    /// **committed read ceiling** in as an additional, coarser floor (see
    /// `lib.rs`): every read this leader (or a predecessor whose ceiling it
    /// witnessed) has ever served was served below *some* committed ceiling,
    /// so a write pushed above the current ceiling is pushed above every
    /// read anyone could have served, even one this cache itself has no
    /// per-span record of (e.g. because a prior leader instance served it and
    /// this leader never rebuilt that leader-local cache). Never regresses.
    pub(crate) fn raise_low_water(&mut self, ts: HlcTimestamp) {
        if ts > self.low_water {
            self.low_water = ts;
        }
    }

    #[cfg(test)]
    pub(crate) fn low_water(&self) -> HlcTimestamp {
        self.low_water
    }

    #[cfg(test)]
    pub(crate) fn current_len(&self) -> usize {
        self.current.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(wall_ms: u64) -> HlcTimestamp {
        HlcTimestamp {
            wall_ms,
            logical: 0,
        }
    }

    #[test]
    fn point_span_contains_exactly_one_key() {
        let (start, end) = point_span(b"k");
        assert!(span_contains(&start, &end, b"k"));
        assert!(!span_contains(&start, &end, b"j"));
        assert!(!span_contains(&start, &end, b"k\x00"));
        assert!(!span_contains(&start, &end, b"ka"));
        assert!(!span_contains(&start, &end, b"l"));
    }

    #[test]
    fn point_span_handles_an_all_0xff_key() {
        // No special-casing needed: appending 0x00 is still the immediate
        // successor regardless of the key's own trailing bytes.
        let key = vec![0xFF, 0xFF];
        let (start, end) = point_span(&key);
        assert!(span_contains(&start, &end, &key));
        assert!(!span_contains(&start, &end, &[0xFF, 0xFF, 0x00]));
        assert!(!span_contains(&start, &end, &[0xFF, 0xFF, 0x01]));
    }

    #[test]
    fn bump_then_max_overlapping_sees_the_bumped_ts() {
        let mut cache = TsCache::new();
        assert_eq!(
            cache.max_overlapping(&[b"k".to_vec()]),
            HlcTimestamp::zero()
        );

        let (s, e) = point_span(b"k");
        cache.bump(s, e, ts(10));
        assert_eq!(cache.max_overlapping(&[b"k".to_vec()]), ts(10));
        // A disjoint key is unaffected.
        assert_eq!(
            cache.max_overlapping(&[b"other".to_vec()]),
            HlcTimestamp::zero()
        );
    }

    #[test]
    fn bump_never_regresses_an_existing_span() {
        let mut cache = TsCache::new();
        let (s, e) = point_span(b"k");
        cache.bump(s.clone(), e.clone(), ts(10));
        cache.bump(s.clone(), e.clone(), ts(5)); // lower — must not regress
        assert_eq!(cache.max_overlapping(&[b"k".to_vec()]), ts(10));
        cache.bump(s, e, ts(20)); // higher — must win
        assert_eq!(cache.max_overlapping(&[b"k".to_vec()]), ts(20));
    }

    #[test]
    fn range_span_overlaps_every_key_inside_it() {
        let mut cache = TsCache::new();
        cache.bump(b"a".to_vec(), Some(b"m".to_vec()), ts(7));
        assert_eq!(cache.max_overlapping(&[b"a".to_vec()]), ts(7));
        assert_eq!(cache.max_overlapping(&[b"f".to_vec()]), ts(7));
        assert_eq!(cache.max_overlapping(&[b"lz".to_vec()]), ts(7));
        assert_eq!(
            cache.max_overlapping(&[b"m".to_vec()]),
            HlcTimestamp::zero()
        );
        assert_eq!(
            cache.max_overlapping(&[b"z".to_vec()]),
            HlcTimestamp::zero()
        );
    }

    #[test]
    fn unbounded_span_overlaps_everything_from_start_onward() {
        let mut cache = TsCache::new();
        cache.bump(b"m".to_vec(), None, ts(3));
        assert_eq!(
            cache.max_overlapping(&[b"a".to_vec()]),
            HlcTimestamp::zero()
        );
        assert_eq!(cache.max_overlapping(&[b"m".to_vec()]), ts(3));
        assert_eq!(cache.max_overlapping(&[b"zzzz".to_vec()]), ts(3));
    }

    #[test]
    fn max_overlapping_takes_the_max_across_several_keys() {
        let mut cache = TsCache::new();
        cache.bump(point_span(b"a").0, point_span(b"a").1, ts(5));
        cache.bump(point_span(b"b").0, point_span(b"b").1, ts(9));
        assert_eq!(
            cache.max_overlapping(&[b"a".to_vec(), b"b".to_vec()]),
            ts(9)
        );
    }

    #[test]
    fn raise_low_water_floors_every_key_even_with_an_empty_cache() {
        let mut cache = TsCache::new();
        cache.raise_low_water(ts(42));
        assert_eq!(cache.max_overlapping(&[b"anything".to_vec()]), ts(42));
        cache.raise_low_water(ts(10)); // lower — must not regress
        assert_eq!(cache.max_overlapping(&[b"anything".to_vec()]), ts(42));
    }

    #[test]
    fn rotation_evicts_current_into_previous_and_low_water_never_loses_the_evicted_max() {
        let mut cache = TsCache::new();
        for i in 0..ROTATION_BOUND {
            // Two-byte key, big-endian-style split so every `i` in
            // `0..ROTATION_BOUND` (4096 = 256^2 / 16) gets a genuinely
            // distinct key: compute the modulo/division on the `usize`
            // *before* narrowing to `u8` (narrowing first, then taking a
            // modulo of the *truncated* byte, silently collides every 256
            // values — a real bug this comment exists to warn future edits
            // away from).
            cache.bump(
                vec![(i % 256) as u8, (i / 256) as u8],
                None,
                ts(i as u64 + 1),
            );
        }
        assert!(cache.current_len() <= ROTATION_BOUND);
        assert_eq!(cache.low_water(), HlcTimestamp::zero(), "not rotated yet");

        // One more bump pushes `current` past the bound, triggering a
        // rotation: `previous` was empty, so `low_water` stays zero, but
        // `current` (now `previous`) holds everything just inserted.
        cache.bump(vec![255, 255], None, ts(ROTATION_BOUND as u64 + 100));

        // Rotate again with a fresh full batch: this time `previous` (the
        // batch above) is dropped, and its max must land in `low_water` —
        // over-conservative eviction, but never a lost, unsafely-low floor.
        for i in 0..(ROTATION_BOUND + 1) {
            cache.bump(
                vec![(i % 200) as u8, (i / 200) as u8, 1],
                None,
                ts(i as u64 + 1),
            );
        }
        assert!(
            cache.low_water() > HlcTimestamp::zero(),
            "rotating away a generation must fold its max ts into low_water"
        );
        // Even a key never explicitly bumped is still pushed above the
        // rotated-away floor — the over-conservative-but-safe property.
        assert!(cache.max_overlapping(&[b"never-seen-key".to_vec()]) >= cache.low_water());
    }
}
