//! A tiny, hand-rolled Bloom filter for per-SSTable membership testing.
//!
//! On flush/compaction we build one of these over the table's distinct user
//! keys and persist it inside the manifest. On a point read, `may_contain` lets
//! us skip an SSTable whose filter proves the key is absent — strictly tighter
//! than the key-range gate (which only bounds `[min_key, max_key]`).
//!
//! ## Design (deterministic, dependency-light)
//!
//! - A bit vector of `m` bits (`m` rounded up to a multiple of 8, stored as
//!   `Vec<u8>`).
//! - `k` hash probes per key, derived by **double hashing** from two 64-bit base
//!   hashes: `g_i(x) = h1 + i*h2`. This is the standard Kirsch–Mitzenmacher
//!   construction, and it only holds its false-positive rate when `h1` and `h2`
//!   behave as independent uniform values.
//! - **Hash schemes** ([`HashScheme`], persisted per filter in manifest v2): the
//!   current scheme runs one FNV-1a pass and then two rounds of the SplitMix64
//!   output function over its state, giving two well-mixed, independent-looking
//!   words (no external dep, deterministic, endian- and platform-independent).
//!   The legacy scheme (manifest v1 filters) took two FNV-1a passes with
//!   different offset bases; those words are strongly correlated (see
//!   `docs/lessons`), which pushed the real false-positive rate to 3-4% against
//!   a 1% target. Filters written under it must still be probed under it (the
//!   bits were set by it), so the scheme travels with the filter until the table
//!   is compacted away.
//! - `m` and `k` are sized from the key count and a target false-positive rate
//!   so the filter stays small but useful.
//!
//! There is no randomness and no platform-dependent hashing, so the persisted
//! bytes are byte-identical for the same key set on every run (ADR 0003).

use serde::{Deserialize, Serialize};

/// Target false-positive probability used to size a freshly built filter.
const TARGET_FP_RATE: f64 = 0.01;
/// Never build a bit vector smaller than this many bits (avoids a uselessly
/// tiny filter for a handful of keys).
const MIN_BITS: usize = 64;

/// Which pair of base hashes a filter's bits were set with. Persisted per filter
/// (manifest v2); a probe must use the scheme the build used.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum HashScheme {
    /// Manifest v1 filters: two FNV-1a passes with different offset bases
    /// (correlated; ~3-4% false positives at the 1% target). Probe-only.
    #[default]
    LegacyFnv = 0,
    /// FNV-1a, then two SplitMix64 outputs as the independent `h1`, `h2`.
    SplitMix = 1,
}

impl HashScheme {
    /// The scheme every newly built filter uses.
    pub(super) const CURRENT: Self = Self::SplitMix;

    /// The persisted tag byte.
    pub(super) fn tag(self) -> u8 {
        self as u8
    }

    /// The scheme for a persisted tag byte, `None` if this binary does not know it.
    pub(super) fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::LegacyFnv),
            1 => Some(Self::SplitMix),
            _ => None,
        }
    }
}

/// A persisted Bloom filter: a bit vector, the probe count `k` and the
/// [`HashScheme`] its bits were set with.
///
/// Serialized inside [`SsTableMeta`](super::sstable::SsTableMeta) in the
/// manifest. An empty bit vector means the table is empty and
/// [`may_contain`](BloomFilter::may_contain) returns `false` for everything.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BloomFilter {
    /// The bit vector, 8 bits per byte (little-endian bit order within a byte).
    bits: Vec<u8>,
    /// Number of hash probes per key.
    k: u32,
    /// The base-hash scheme the bits were set with (`LegacyFnv` when absent,
    /// i.e. for anything written before schemes were tagged).
    #[serde(default)]
    scheme: HashScheme,
}

impl BloomFilter {
    /// Build a filter sized for `keys.len()` distinct keys at [`TARGET_FP_RATE`].
    ///
    /// Keys may repeat; duplicates are harmless (idempotent sets of the same
    /// bits). For an empty key set this returns an empty filter that answers
    /// `may_contain == false` for everything (an empty SSTable contains nothing).
    #[must_use]
    pub fn build(keys: &[&[u8]]) -> Self {
        Self::build_with(keys, HashScheme::CURRENT)
    }

    /// Build a filter under the frozen legacy hash scheme. Test only:
    /// production never writes the legacy scheme (writers write the current
    /// one, ADR 0073); this reproduces what a manifest-v1 binary persisted.
    #[cfg(test)]
    #[must_use]
    pub fn build_legacy(keys: &[&[u8]]) -> Self {
        Self::build_with(keys, HashScheme::LegacyFnv)
    }

    fn build_with(keys: &[&[u8]], scheme: HashScheme) -> Self {
        let n = keys.len();
        if n == 0 {
            return Self {
                bits: Vec::new(),
                k: 1,
                scheme,
            };
        }
        let (m_bits, k) = optimal_params(n);
        let mut bits = vec![0u8; m_bits.div_ceil(8)];
        let m = (bits.len() * 8) as u64;
        for key in keys {
            let (h1, h2) = base_hashes(scheme, key);
            for i in 0..k {
                let bit = probe(h1, h2, i, m);
                bits[(bit / 8) as usize] |= 1u8 << (bit % 8);
            }
        }
        Self { bits, k, scheme }
    }

    /// The raw `(bits, k, scheme)` parts, for the compact binary manifest codec.
    pub(super) fn as_parts(&self) -> (&[u8], u32, HashScheme) {
        (&self.bits, self.k, self.scheme)
    }

    /// Reconstruct a filter from its raw parts (binary manifest decode). The
    /// inverse of [`as_parts`](BloomFilter::as_parts).
    pub(super) fn from_parts(bits: Vec<u8>, k: u32, scheme: HashScheme) -> Self {
        Self { bits, k, scheme }
    }

    /// Whether `key` *might* be present. `false` is definitive (the key was
    /// never inserted); `true` may be a false positive. An empty bit vector
    /// (built from zero keys) answers `false` for everything.
    #[must_use]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.bits.is_empty() {
            return false;
        }
        let m = (self.bits.len() * 8) as u64;
        let (h1, h2) = base_hashes(self.scheme, key);
        for i in 0..self.k {
            let bit = probe(h1, h2, i, m);
            if self.bits[(bit / 8) as usize] & (1u8 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }
}

/// Optimal `(m_bits, k)` for `n` items at [`TARGET_FP_RATE`], using the standard
/// `m = -n ln p / (ln 2)^2`, `k = (m/n) ln 2` formulas, clamped to sane minima.
fn optimal_params(n: usize) -> (usize, u32) {
    let nf = n as f64;
    let ln2 = std::f64::consts::LN_2;
    let m = (-nf * TARGET_FP_RATE.ln() / (ln2 * ln2)).ceil() as usize;
    let m = m.max(MIN_BITS);
    let k = ((m as f64 / nf) * ln2).round() as u32;
    (m, k.clamp(1, 30))
}

/// FNV-1a offset basis (the standard one).
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
/// The second offset basis the legacy scheme used for `h2`.
const LEGACY_FNV_OFFSET_2: u64 = 0x9e37_79b9_7f4a_7c15;
/// SplitMix64 state increment (the golden-ratio constant).
const SPLITMIX_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;

/// Two 64-bit base hashes of `key` for double hashing, per `scheme`.
fn base_hashes(scheme: HashScheme, key: &[u8]) -> (u64, u64) {
    match scheme {
        HashScheme::LegacyFnv => {
            // Frozen: this is what manifest-v1 filters were built with.
            let h1 = fnv1a(key, FNV_OFFSET);
            let h2 = fnv1a(key, LEGACY_FNV_OFFSET_2);
            // Guard against h2 == 0, which would make all probes collide.
            (h1, if h2 == 0 { 0x1 } else { h2 })
        }
        HashScheme::SplitMix => {
            // One FNV-1a pass seeds a SplitMix64 generator; its first two
            // outputs are the two base hashes. SplitMix64's output function is
            // a bijective avalanche mixer, so the structured low-entropy FNV
            // state (sequential keys differ in a few bits) becomes two
            // independent-looking words.
            let mut state = fnv1a(key, FNV_OFFSET);
            let h1 = splitmix64_next(&mut state);
            let h2 = splitmix64_next(&mut state);
            // Odd step: coprime with the byte-multiple-of-8 bit count's power-of-two
            // factor, and never zero (zero would collapse all probes).
            (h1, h2 | 1)
        }
    }
}

/// One SplitMix64 step (Steele, Lea & Flood): advance `state` and return the
/// finalized output. Pure integer arithmetic, identical on every platform.
fn splitmix64_next(state: &mut u64) -> u64 {
    *state = state.wrapping_add(SPLITMIX_GAMMA);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// FNV-1a 64-bit over `bytes` with the given offset basis.
fn fnv1a(bytes: &[u8], offset_basis: u64) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = offset_basis;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// The `i`-th probe bit index in `[0, m)` via Kirsch–Mitzenmacher double hashing.
fn probe(h1: u64, h2: u64, i: u32, m: u64) -> u64 {
    h1.wrapping_add(u64::from(i).wrapping_mul(h2)) % m
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn present_keys_always_report_maybe() {
        let keys: Vec<Vec<u8>> = (0u32..500).map(|i| i.to_le_bytes().to_vec()).collect();
        let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        let bloom = BloomFilter::build(&refs);
        // No false negatives: every inserted key must report present.
        for k in &keys {
            assert!(bloom.may_contain(k), "false negative for {k:?}");
        }
    }

    #[test]
    fn absent_keys_mostly_report_absent() {
        let keys: Vec<Vec<u8>> = (0u32..1000).map(|i| i.to_le_bytes().to_vec()).collect();
        let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        let bloom = BloomFilter::build(&refs);
        // Probe 1000 keys that were never inserted; expect a low FP rate.
        let mut fps = 0;
        for i in 1000u32..2000 {
            if bloom.may_contain(&i.to_le_bytes()) {
                fps += 1;
            }
        }
        assert!(fps < 60, "false-positive rate too high: {fps}/1000");
    }

    #[test]
    fn empty_filter_reports_absent() {
        let bloom = BloomFilter::build(&[]);
        assert!(!bloom.may_contain(b"anything"));
    }

    #[test]
    fn deterministic_for_same_keys() {
        let keys: Vec<&[u8]> = vec![b"a", b"bb", b"ccc"];
        let a = BloomFilter::build(&keys);
        let b = BloomFilter::build(&keys);
        assert_eq!(a.bits, b.bits);
        assert_eq!(a.k, b.k);
    }

    /// Deterministic key generators for the false-positive measurements. The
    /// "hashed" family uses an xorshift64* stream (independent of the filter's
    /// own hashing) so the keys look random; the "sequential" family is the
    /// worst case for a weak hash (big-endian counters, long shared prefixes).
    fn hashed_key(stream: u64, i: u64) -> Vec<u8> {
        let mut x = (i + 1).wrapping_mul(0x2545_f491_4f6c_dd1d) ^ stream;
        let mut out = Vec::with_capacity(16);
        for _ in 0..2 {
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            out.extend_from_slice(&x.wrapping_mul(0x2545_f491_4f6c_dd1d).to_le_bytes());
        }
        out
    }

    fn sequential_key(base: u64, i: u64) -> Vec<u8> {
        format!("user#{:012}", base + i).into_bytes()
    }

    /// Empirical false-positive rate of `bloom` over `probes` absent keys.
    fn measured_fp(bloom: &BloomFilter, absent: impl Fn(u64) -> Vec<u8>, probes: u64) -> f64 {
        let fps = (0..probes)
            .filter(|&i| bloom.may_contain(&absent(i)))
            .count();
        fps as f64 / probes as f64
    }

    const N: u64 = 100_000;

    fn fp_rates(build: fn(&[&[u8]]) -> BloomFilter) -> (f64, f64) {
        let hashed: Vec<Vec<u8>> = (0..N).map(|i| hashed_key(1, i)).collect();
        let refs: Vec<&[u8]> = hashed.iter().map(Vec::as_slice).collect();
        let bloom = build(&refs);
        for k in hashed.iter().step_by(97) {
            assert!(bloom.may_contain(k), "false negative");
        }
        let hashed_fp = measured_fp(&bloom, |i| hashed_key(2, i), N);

        let seq: Vec<Vec<u8>> = (0..N).map(|i| sequential_key(0, i)).collect();
        let refs: Vec<&[u8]> = seq.iter().map(Vec::as_slice).collect();
        let bloom = build(&refs);
        for k in seq.iter().step_by(97) {
            assert!(bloom.may_contain(k), "false negative");
        }
        let seq_fp = measured_fp(&bloom, |i| sequential_key(N, i), N);
        (hashed_fp, seq_fp)
    }

    /// Issue #1389: the current scheme meets the 1% target within 1.5x on both
    /// random and sequential key sets.
    #[test]
    fn current_scheme_false_positive_rate_is_near_target() {
        let (hashed, seq) = fp_rates(BloomFilter::build);
        eprintln!("current scheme FP: hashed={hashed:.4} sequential={seq:.4}");
        assert!(hashed <= TARGET_FP_RATE * 1.5, "hashed FP {hashed}");
        assert!(seq <= TARGET_FP_RATE * 1.5, "sequential FP {seq}");
    }

    /// Negative control: the frozen legacy scheme does NOT meet that bound, so
    /// the test above is able to catch a regression to correlated base hashes.
    #[test]
    fn legacy_scheme_exceeds_the_bound() {
        let (hashed, seq) = fp_rates(BloomFilter::build_legacy);
        eprintln!("legacy scheme FP: hashed={hashed:.4} sequential={seq:.4}");
        assert!(
            hashed > TARGET_FP_RATE * 1.5 || seq > TARGET_FP_RATE * 1.5,
            "legacy scheme unexpectedly within bound: hashed={hashed} seq={seq}"
        );
    }

    /// A filter built under the legacy scheme is probed under it (no false
    /// negatives) after a `(bits, k, scheme)` round trip.
    #[test]
    fn legacy_filter_is_probed_with_the_legacy_scheme() {
        let keys: Vec<Vec<u8>> = (0u32..2000).map(|i| i.to_le_bytes().to_vec()).collect();
        let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        let legacy = BloomFilter::build_legacy(&refs);
        let (bits, k, scheme) = legacy.as_parts();
        assert_eq!(scheme, HashScheme::LegacyFnv);
        let back = BloomFilter::from_parts(bits.to_vec(), k, scheme);
        for key in &keys {
            assert!(back.may_contain(key));
        }
        assert_eq!(BloomFilter::build(&refs).as_parts().2, HashScheme::CURRENT);
    }

    /// Pinned base-hash vectors: any change to either scheme's arithmetic is a
    /// format break (bits already on disk would be probed wrongly).
    #[test]
    fn base_hash_vectors_are_pinned() {
        assert_eq!(
            base_hashes(HashScheme::LegacyFnv, b""),
            (0xcbf2_9ce4_8422_2325, 0x9e37_79b9_7f4a_7c15)
        );
        let (h1, h2) = base_hashes(HashScheme::SplitMix, b"");
        assert_eq!((h1, h2), SPLITMIX_EMPTY);
    }
    const SPLITMIX_EMPTY: (u64, u64) = (0xc3817c016ba4ff30, 0x100cdaacc0bc9317);
}
