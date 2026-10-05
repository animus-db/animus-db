//! A seeded byte mutator for the stable-toolchain smoke. Deterministic (a
//! `SplitMix64` stream from an explicit seed — never `thread_rng`, per the
//! repo's determinism rule), so a smoke failure replays exactly.

/// `SplitMix64`: a tiny, well-mixed, seedable PRNG.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform-ish in `0..n` (`n > 0`).
    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// Values that sit on integer-width boundaries — the usual length-prefix and
/// offset-overflow suspects.
const INTERESTING: &[&[u8]] = &[
    &[0x00],
    &[0xff],
    &[0x7f],
    &[0x80],
    &[0x00, 0x00],
    &[0xff, 0xff],
    &[0x7f, 0xff],
    &[0x00, 0x00, 0x00, 0x00],
    &[0xff, 0xff, 0xff, 0xff],
    &[0x7f, 0xff, 0xff, 0xff],
    &[0x80, 0x00, 0x00, 0x00],
    &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
    &[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
    &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
    b"\n",
    b"\r\n\r\n",
    b":",
    b"\"",
    b"\\",
    b"{",
    b"}",
    b"[",
    b"]",
    b"(",
    b")",
    b"#",
    b"null",
    b"99999999999999999999",
    b"-1",
];

/// Apply 1..=4 random edits to a copy of `seed` and return it. Edits: bit
/// flip, byte overwrite, insert an interesting value, delete a range, truncate,
/// duplicate a range, splice a slice of `other` (a second seed), swap two bytes.
/// The result is capped at `max_len`.
#[must_use]
pub fn mutate(rng: &mut Rng, seed: &[u8], other: &[u8], max_len: usize) -> Vec<u8> {
    let mut v = seed.to_vec();
    for _ in 0..=rng.below(4) {
        match rng.below(9) {
            0 if !v.is_empty() => {
                let i = rng.below(v.len());
                v[i] ^= 1 << rng.below(8);
            }
            1 if !v.is_empty() => {
                let i = rng.below(v.len());
                v[i] = rng.next_u64() as u8;
            }
            2 => {
                let ins = INTERESTING[rng.below(INTERESTING.len())];
                let at = rng.below(v.len() + 1);
                v.splice(at..at, ins.iter().copied());
            }
            3 if !v.is_empty() => {
                let a = rng.below(v.len());
                let b = (a + 1 + rng.below(16)).min(v.len());
                v.drain(a..b);
            }
            4 if !v.is_empty() => {
                let keep = rng.below(v.len());
                v.truncate(keep);
            }
            5 if !v.is_empty() => {
                let a = rng.below(v.len());
                let b = (a + 1 + rng.below(32)).min(v.len());
                let chunk = v[a..b].to_vec();
                let at = rng.below(v.len() + 1);
                v.splice(at..at, chunk);
            }
            6 if !other.is_empty() => {
                let a = rng.below(other.len());
                let b = (a + 1 + rng.below(64)).min(other.len());
                let at = rng.below(v.len() + 1);
                v.splice(at..at, other[a..b].iter().copied());
            }
            7 if v.len() > 1 => {
                let i = rng.below(v.len());
                let j = rng.below(v.len());
                v.swap(i, j);
            }
            8 if !v.is_empty() => {
                // Overwrite a window with an interesting value (a "length" field).
                let ins = INTERESTING[rng.below(INTERESTING.len())];
                let at = rng.below(v.len());
                for (k, b) in ins.iter().enumerate() {
                    if at + k < v.len() {
                        v[at + k] = *b;
                    }
                }
            }
            _ => {}
        }
    }
    v.truncate(max_len);
    v
}
