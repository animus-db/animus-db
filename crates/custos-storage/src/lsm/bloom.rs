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
//!   hashes: `g_i(x) = h1 + i*h2`. The base hashes are FNV-1a over the key bytes
//!   (no external dep, fully deterministic, no `HashMap` randomized state). This
//!   is the standard Kirsch–Mitzenmacher construction.
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

/// A persisted Bloom filter: a bit vector plus the probe count `k`.
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
}

impl BloomFilter {
    /// Build a filter sized for `keys.len()` distinct keys at [`TARGET_FP_RATE`].
    ///
    /// Keys may repeat; duplicates are harmless (idempotent sets of the same
    /// bits). For an empty key set this returns an empty filter that answers
    /// `may_contain == false` for everything (an empty SSTable contains nothing).
    #[must_use]
    pub fn build(keys: &[&[u8]]) -> Self {
        let n = keys.len();
        if n == 0 {
            return Self {
                bits: Vec::new(),
                k: 1,
            };
        }
        let (m_bits, k) = optimal_params(n);
        let mut bits = vec![0u8; m_bits.div_ceil(8)];
        let m = (bits.len() * 8) as u64;
        for key in keys {
            let (h1, h2) = base_hashes(key);
            for i in 0..k {
                let bit = probe(h1, h2, i, m);
                bits[(bit / 8) as usize] |= 1u8 << (bit % 8);
            }
        }
        Self { bits, k }
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
        let (h1, h2) = base_hashes(key);
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

/// Two 64-bit base hashes of `key` for double hashing. Both are FNV-1a passes
/// with distinct offset bases, so they are deterministic and cheap.
fn base_hashes(key: &[u8]) -> (u64, u64) {
    let h1 = fnv1a(key, 0xcbf2_9ce4_8422_2325);
    let h2 = fnv1a(key, 0x9e37_79b9_7f4a_7c15);
    // Guard against h2 == 0, which would make all probes collide.
    (h1, if h2 == 0 { 0x1 } else { h2 })
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
}
