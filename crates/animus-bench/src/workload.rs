//! YCSB workloads A-F as a pure, seeded operation stream — no I/O, no clock.
//!
//! The stream decides *what* to do (which record, which operation class);
//! [`crate::ycsb`] maps each [`Op`] onto DynamoDB requests. Given the same
//! [`WorkloadSpec`] and seed the stream is byte-for-byte reproducible.
//!
//! # Key layout (disclosed in every results file)
//!
//! One table per workload run, `pk` (S, HASH) + `sk` (N, RANGE), so a single
//! layout serves point reads, point updates, inserts and `Query` scans.
//! Record `i` lives at
//!
//! ```text
//! pk = "user" + zero-padded(i / 100, 10)     sk = i % 100
//! ```
//!
//! i.e. 100 consecutive records share a partition key (a "wide partition" of
//! 100 sort keys). Workloads A-D and F touch single items by `(pk, sk)`.
//! Workload E's scan is a `Query` on one partition: `pk = :p AND sk >= :s`
//! with `Limit` = the scan length (uniform in `1..=max_scan_len`, default
//! 100 as in YCSB). A DynamoDB `Query` cannot cross partition keys, so a
//! scan stops at the end of its 100-record partition — a deliberate
//! difference from YCSB's cross-key `scan`, disclosed rather than hidden.
//! Each item is `{pk, sk, version: N, data: S(value_bytes)}`.

use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde::{Deserialize, Serialize};

use crate::dist::{Distribution, KeyChooser};

/// Sort keys per partition key.
pub const SK_PER_PK: u64 = 100;
/// Hash-key attribute name.
pub const ATTR_PK: &str = "pk";
/// Range-key attribute name.
pub const ATTR_SK: &str = "sk";
/// The payload attribute (a string of `value_bytes`).
pub const ATTR_DATA: &str = "data";
/// The per-item version counter workload F's conditional update checks.
pub const ATTR_VERSION: &str = "version";

/// `(pk, sk)` of record `idx` under the layout above.
#[must_use]
pub fn key_for(idx: u64) -> (String, u64) {
    (format!("user{:010}", idx / SK_PER_PK), idx % SK_PER_PK)
}

/// A deterministic alphanumeric payload of exactly `n` bytes derived from
/// `nonce` (no characters needing JSON escaping).
#[must_use]
pub fn value_for(nonce: u64, n: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut x = nonce ^ 0x9e37_79b9_7f4a_7c15;
    let mut s = String::with_capacity(n);
    for _ in 0..n {
        x = x
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        s.push(ALPHABET[((x >> 33) as usize) % ALPHABET.len()] as char);
    }
    s
}

/// The six standard YCSB core workloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum WorkloadKind {
    A,
    B,
    C,
    D,
    E,
    F,
}

/// Operation-mix percentages (sum to 100).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mix {
    pub read: u32,
    pub update: u32,
    pub insert: u32,
    pub scan: u32,
    pub read_modify_write: u32,
}

impl WorkloadKind {
    /// All six, in order.
    pub const ALL: [Self; 6] = [Self::A, Self::B, Self::C, Self::D, Self::E, Self::F];

    /// Parse `A`..`F` (case-insensitive).
    ///
    /// # Errors
    /// On anything else.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_ascii_uppercase().as_str() {
            "A" => Ok(Self::A),
            "B" => Ok(Self::B),
            "C" => Ok(Self::C),
            "D" => Ok(Self::D),
            "E" => Ok(Self::E),
            "F" => Ok(Self::F),
            other => Err(format!("unknown workload `{other}` (A|B|C|D|E|F)")),
        }
    }

    /// The single-letter name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::A => "A",
            Self::B => "B",
            Self::C => "C",
            Self::D => "D",
            Self::E => "E",
            Self::F => "F",
        }
    }

    /// One-line description for the summary / disclosure.
    #[must_use]
    pub fn description(self) -> &'static str {
        match self {
            Self::A => "update heavy: 50% GetItem / 50% UpdateItem",
            Self::B => "read mostly: 95% GetItem / 5% UpdateItem",
            Self::C => "read only: 100% GetItem",
            Self::D => "read latest: 95% GetItem (latest-skewed) / 5% PutItem of new keys",
            Self::E => "short scans: 95% Query (sort-key range) / 5% PutItem of new keys",
            Self::F => "read-modify-write: 50% GetItem / 50% GetItem + conditional UpdateItem",
        }
    }

    /// The operation mix.
    #[must_use]
    pub fn mix(self) -> Mix {
        let m = Mix::default();
        match self {
            Self::A => Mix {
                read: 50,
                update: 50,
                ..m
            },
            Self::B => Mix {
                read: 95,
                update: 5,
                ..m
            },
            Self::C => Mix { read: 100, ..m },
            Self::D => Mix {
                read: 95,
                insert: 5,
                ..m
            },
            Self::E => Mix {
                scan: 95,
                insert: 5,
                ..m
            },
            Self::F => Mix {
                read: 50,
                read_modify_write: 50,
                ..m
            },
        }
    }

    /// Does this workload contain any read-class operation (and so honour
    /// the `ConsistentRead` setting)?
    #[must_use]
    pub fn has_reads(self) -> bool {
        let m = self.mix();
        m.read + m.scan + m.read_modify_write > 0
    }
}

/// What a workload run looks like, independent of seed and timing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadSpec {
    pub kind: WorkloadKind,
    /// Records loaded before the run.
    pub record_count: u64,
    /// Size of the `data` attribute in bytes.
    pub value_bytes: usize,
    /// Request distribution for choosing records (workload D always reads
    /// "latest"; its inserts are sequential).
    pub distribution: Distribution,
    /// Longest `Query` scan (workload E): lengths are uniform in `1..=max`.
    pub max_scan_len: u32,
}

/// One logical operation. `idx` is a record index (see [`key_for`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Read { idx: u64 },
    Update { idx: u64, nonce: u64 },
    Insert { idx: u64, nonce: u64 },
    Scan { idx: u64, len: u32 },
    ReadModifyWrite { idx: u64, nonce: u64 },
}

impl Op {
    /// The operation-class label results are bucketed by.
    #[must_use]
    pub fn class(&self) -> &'static str {
        match self {
            Self::Read { .. } => "read",
            Self::Update { .. } => "update",
            Self::Insert { .. } => "insert",
            Self::Scan { .. } => "scan",
            Self::ReadModifyWrite { .. } => "read_modify_write",
        }
    }
}

/// The seeded operation stream.
pub struct OpStream {
    spec: WorkloadSpec,
    rng: ChaCha8Rng,
    chooser: KeyChooser,
    /// Next record index an insert will create (== records existing so far).
    next_insert: u64,
}

impl OpStream {
    /// A stream for `spec` seeded with `seed`.
    #[must_use]
    pub fn new(spec: WorkloadSpec, seed: u64) -> Self {
        let n = spec.record_count.max(1);
        let chooser = if spec.kind == WorkloadKind::D {
            KeyChooser::latest(n)
        } else {
            KeyChooser::new(spec.distribution, n)
        };
        Self {
            next_insert: spec.record_count,
            spec,
            rng: ChaCha8Rng::seed_from_u64(seed),
            chooser,
        }
    }

    /// The next operation.
    pub fn next_op(&mut self) -> Op {
        let mix = self.spec.kind.mix();
        let roll = self.rng.gen_range(0..100u32);
        let existing = self.next_insert;
        let (r, u, i, s) = (
            mix.read,
            mix.read + mix.update,
            mix.read + mix.update + mix.insert,
            mix.read + mix.update + mix.insert + mix.scan,
        );
        if roll < r {
            Op::Read {
                idx: self.chooser.choose(&mut self.rng, existing),
            }
        } else if roll < u {
            Op::Update {
                idx: self.chooser.choose(&mut self.rng, existing),
                nonce: self.rng.next_u64(),
            }
        } else if roll < i {
            let idx = self.next_insert;
            self.next_insert += 1;
            Op::Insert {
                idx,
                nonce: self.rng.next_u64(),
            }
        } else if roll < s {
            Op::Scan {
                idx: self.chooser.choose(&mut self.rng, existing),
                len: self.rng.gen_range(1..=self.spec.max_scan_len.max(1)),
            }
        } else {
            Op::ReadModifyWrite {
                idx: self.chooser.choose(&mut self.rng, existing),
                nonce: self.rng.next_u64(),
            }
        }
    }
}

impl crate::engine::OpSource for OpStream {
    type Op = Op;
    fn next_op(&mut self) -> Op {
        OpStream::next_op(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(kind: WorkloadKind) -> WorkloadSpec {
        WorkloadSpec {
            kind,
            record_count: 10_000,
            value_bytes: 64,
            distribution: Distribution::Zipfian,
            max_scan_len: 100,
        }
    }

    #[test]
    fn mixes_sum_to_100() {
        for k in WorkloadKind::ALL {
            let m = k.mix();
            assert_eq!(
                m.read + m.update + m.insert + m.scan + m.read_modify_write,
                100
            );
        }
    }

    #[test]
    fn seeded_stream_matches_each_workloads_mix() {
        let n = 100_000u32;
        for kind in WorkloadKind::ALL {
            let mut s = OpStream::new(spec(kind), 42);
            let mut c = [0u32; 5];
            for _ in 0..n {
                match s.next_op() {
                    Op::Read { .. } => c[0] += 1,
                    Op::Update { .. } => c[1] += 1,
                    Op::Insert { .. } => c[2] += 1,
                    Op::Scan { .. } => c[3] += 1,
                    Op::ReadModifyWrite { .. } => c[4] += 1,
                }
            }
            let m = kind.mix();
            for (got, want) in
                c.iter()
                    .zip([m.read, m.update, m.insert, m.scan, m.read_modify_write])
            {
                let pct = f64::from(*got) * 100.0 / f64::from(n);
                assert!(
                    (pct - f64::from(want)).abs() < 1.0,
                    "workload {}: got {pct:.2}% want {want}%",
                    kind.name()
                );
            }
        }
    }

    #[test]
    fn same_seed_reproduces_the_stream_and_different_seeds_differ() {
        let take = |seed| {
            let mut s = OpStream::new(spec(WorkloadKind::A), seed);
            (0..200).map(|_| s.next_op()).collect::<Vec<_>>()
        };
        assert_eq!(take(7), take(7));
        assert_ne!(take(7), take(8));
    }

    #[test]
    fn inserts_are_sequential_new_keys_and_scans_are_bounded() {
        let mut s = OpStream::new(spec(WorkloadKind::E), 1);
        let mut next = 10_000;
        for _ in 0..5_000 {
            match s.next_op() {
                Op::Insert { idx, .. } => {
                    assert_eq!(idx, next);
                    next += 1;
                }
                Op::Scan { idx, len } => {
                    assert!(idx < next);
                    assert!((1..=100).contains(&len));
                }
                other => panic!("workload E produced {other:?}"),
            }
        }
        assert!(next > 10_000);
    }

    #[test]
    fn workload_d_reads_skew_to_the_latest_records() {
        let mut s = OpStream::new(spec(WorkloadKind::D), 5);
        let (mut reads, mut recent) = (0u32, 0u32);
        let mut newest = 10_000u64;
        for _ in 0..50_000 {
            match s.next_op() {
                Op::Insert { idx, .. } => newest = idx + 1,
                Op::Read { idx } => {
                    assert!(idx < newest);
                    reads += 1;
                    recent += u32::from(idx + 20 >= newest);
                }
                other => panic!("workload D produced {other:?}"),
            }
        }
        // P(zipfian distance < 20) ~ 0.37 over a ~10k window.
        assert!(f64::from(recent) / f64::from(reads) > 0.3);
    }

    #[test]
    fn key_layout_groups_100_records_per_partition() {
        assert_eq!(key_for(0), ("user0000000000".to_string(), 0));
        assert_eq!(key_for(99), ("user0000000000".to_string(), 99));
        assert_eq!(key_for(100), ("user0000000001".to_string(), 0));
        assert_eq!(key_for(12_345), ("user0000000123".to_string(), 45));
    }

    #[test]
    fn value_is_exact_length_and_deterministic() {
        assert_eq!(value_for(1, 256).len(), 256);
        assert_eq!(value_for(1, 32), value_for(1, 32));
        assert_ne!(value_for(1, 32), value_for(2, 32));
    }
}
