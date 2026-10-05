//! Key-choice distributions (YCSB-style) — pure, driven by a caller-supplied
//! seeded RNG.
//!
//! - [`Zipfian`]: Gray et al.'s generator exactly as YCSB's
//!   `ZipfianGenerator` (theta = 0.99): rank 0 is the most popular.
//! - **Scrambled zipfian** ([`KeyChooser::Zipfian`]): YCSB's default
//!   `zipfian` request distribution — a zipfian *rank* hashed (FNV-64) onto
//!   the key space so the hot keys are scattered across the ring rather than
//!   clustered at the low key indices (which would put them all in one
//!   partition).
//! - **Latest** ([`KeyChooser::Latest`]): YCSB's `SkewedLatestGenerator`
//!   used by workload D — a zipfian *distance* back from the most recently
//!   inserted record.
//! - **Uniform**.

use rand::Rng;
use rand_chacha::ChaCha8Rng;
use serde::{Deserialize, Serialize};

/// YCSB's zipfian constant.
pub const ZIPFIAN_THETA: f64 = 0.99;

/// Which request distribution to draw record indices from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Distribution {
    Uniform,
    Zipfian,
}

impl Distribution {
    /// Parse `uniform` / `zipfian` (CLI).
    ///
    /// # Errors
    /// On an unknown name.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "uniform" => Ok(Self::Uniform),
            "zipfian" => Ok(Self::Zipfian),
            other => Err(format!("unknown distribution `{other}` (uniform|zipfian)")),
        }
    }
}

fn zeta(n: u64, theta: f64) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum()
}

/// FNV-1a 64 over the 8 little-endian bytes of `v` (YCSB's `fnvhash64`
/// scramble; any fixed bijective-ish mixer would do — fixed so a seed
/// reproduces across runs and hosts).
#[must_use]
pub fn fnv64(v: u64) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in v.to_le_bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Gray et al.'s zipfian rank generator over `[0, n)`.
#[derive(Clone, Debug)]
pub struct Zipfian {
    n: u64,
    alpha: f64,
    zetan: f64,
    eta: f64,
}

impl Zipfian {
    /// A generator over `n >= 1` items with [`ZIPFIAN_THETA`].
    #[must_use]
    pub fn new(n: u64) -> Self {
        let n = n.max(1);
        let theta = ZIPFIAN_THETA;
        let zetan = zeta(n, theta);
        let zeta2 = zeta(2, theta);
        #[allow(clippy::cast_precision_loss)]
        let eta = (1.0 - (2.0 / n as f64).powf(1.0 - theta)) / (1.0 - zeta2 / zetan);
        Self {
            n,
            alpha: 1.0 / (1.0 - theta),
            zetan,
            eta,
        }
    }

    /// The rank for a uniform `u` in `[0, 1)`; 0 is the most popular.
    #[must_use]
    pub fn rank(&self, u: f64) -> u64 {
        let uz = u * self.zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5f64.powf(ZIPFIAN_THETA) {
            return 1.min(self.n - 1);
        }
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        let r = (self.n as f64 * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64;
        r.min(self.n - 1)
    }
}

/// A record-index chooser over a (possibly growing) key space.
#[derive(Clone, Debug)]
pub enum KeyChooser {
    /// Uniform over the currently existing records.
    Uniform,
    /// Scrambled zipfian over the first `n` (initially loaded) records.
    Zipfian { zipf: Zipfian, n: u64 },
    /// Zipfian distance back from the newest record (workload D).
    Latest { zipf: Zipfian },
}

impl KeyChooser {
    /// Scrambled zipfian or uniform over `n` initially-loaded records.
    #[must_use]
    pub fn new(dist: Distribution, n: u64) -> Self {
        match dist {
            Distribution::Uniform => Self::Uniform,
            Distribution::Zipfian => Self::Zipfian {
                zipf: Zipfian::new(n),
                n: n.max(1),
            },
        }
    }

    /// Workload D's read-latest chooser over a `window`-record zipfian.
    #[must_use]
    pub fn latest(window: u64) -> Self {
        Self::Latest {
            zipf: Zipfian::new(window),
        }
    }

    /// Choose a record index in `[0, existing)` (`existing >= 1`).
    pub fn choose(&self, rng: &mut ChaCha8Rng, existing: u64) -> u64 {
        let existing = existing.max(1);
        match self {
            Self::Uniform => rng.gen_range(0..existing),
            Self::Zipfian { zipf, n } => {
                let rank = zipf.rank(rng.r#gen::<f64>());
                fnv64(rank) % (*n).min(existing)
            }
            Self::Latest { zipf } => {
                let back = zipf.rank(rng.r#gen::<f64>());
                (existing - 1).saturating_sub(back)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    fn rng(seed: u64) -> ChaCha8Rng {
        ChaCha8Rng::seed_from_u64(seed)
    }

    #[test]
    fn uniform_covers_the_space_evenly() {
        let c = KeyChooser::new(Distribution::Uniform, 100);
        let mut r = rng(1);
        let mut buckets = [0u32; 100];
        for _ in 0..100_000 {
            buckets[c.choose(&mut r, 100) as usize] += 1;
        }
        // Mean 1000/bucket; a loose +-35% band (seeded, so deterministic).
        for (i, &b) in buckets.iter().enumerate() {
            assert!((650..=1350).contains(&b), "bucket {i} = {b}");
        }
    }

    #[test]
    fn zipfian_rank_zero_dominates_and_the_head_is_heavy() {
        let z = Zipfian::new(1_000);
        let mut r = rng(2);
        let draws = 200_000u32;
        let (mut zero, mut head10) = (0u32, 0u32);
        for _ in 0..draws {
            let k = z.rank(r.r#gen::<f64>());
            assert!(k < 1_000);
            zero += u32::from(k == 0);
            head10 += u32::from(k < 10);
        }
        let p0 = f64::from(zero) / f64::from(draws);
        let p10 = f64::from(head10) / f64::from(draws);
        // zeta(1000, .99) ~ 7.2 -> P(rank 0) ~ 0.139, P(rank < 10) ~ 0.40.
        assert!((0.10..=0.18).contains(&p0), "p0 = {p0}");
        assert!((0.30..=0.50).contains(&p10), "p10 = {p10}");
    }

    #[test]
    fn scrambled_zipfian_scatters_the_hot_keys() {
        let n = 10_000u64;
        let c = KeyChooser::new(Distribution::Zipfian, n);
        let mut r = rng(3);
        let mut hits = vec![0u32; n as usize];
        for _ in 0..100_000 {
            let k = c.choose(&mut r, n);
            assert!(k < n);
            hits[k as usize] += 1;
        }
        // Still skewed: the single hottest key takes >5% of traffic...
        let (hot, &hot_n) = hits.iter().enumerate().max_by_key(|&(_, h)| *h).unwrap();
        assert!(hot_n > 5_000, "hottest key only {hot_n}");
        // ...it is the scrambled rank 0, not index 0...
        assert_eq!(hot as u64, fnv64(0) % n);
        assert_ne!(hot, 0);
        // ...and the 10 hottest keys are not the 10 lowest indices.
        let mut order: Vec<usize> = (0..n as usize).collect();
        order.sort_by_key(|&i| std::cmp::Reverse(hits[i]));
        assert!(order[..10].iter().any(|&i| i >= 10));
    }

    #[test]
    fn latest_favours_the_newest_records() {
        let c = KeyChooser::latest(1_000);
        let mut r = rng(4);
        let existing = 5_000u64;
        let mut recent = 0u32;
        for _ in 0..50_000 {
            let k = c.choose(&mut r, existing);
            assert!(k < existing);
            recent += u32::from(k >= existing - 10);
        }
        // ~40% of reads land in the newest 10 records.
        assert!(recent > 15_000, "recent = {recent}");
    }

    #[test]
    fn same_seed_same_stream() {
        let c = KeyChooser::new(Distribution::Zipfian, 1_000);
        let a: Vec<u64> = {
            let mut r = rng(9);
            (0..64).map(|_| c.choose(&mut r, 1_000)).collect()
        };
        let b: Vec<u64> = {
            let mut r = rng(9);
            (0..64).map(|_| c.choose(&mut r, 1_000)).collect()
        };
        assert_eq!(a, b);
    }
}
