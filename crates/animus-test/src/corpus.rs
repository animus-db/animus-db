//! Shared seed-management scaffolding for this repo's fault-injection
//! corpora (ADR 0061 rung B1).
//!
//! Every corpus in this repo follows the same "house corpus doctrine"
//! (originally ADR 0014, generalized since): a **frozen, name-seeded
//! generator** — each scenario's seed is a stable hash of its own name, so a
//! suite run is the same set every time and a failure names one scenario +
//! seed, replayable via `ANIMUS_SEED` (see the root `CLAUDE.md`'s "Replaying
//! a failed simulation" and this crate's own `CLAUDE.md`). Depth is a knob
//! (`ANIMUS_<X>_SEEDS`): `K=1` reproduces the committed set byte-for-byte,
//! `K>1` additionally sweeps `K-1` fresh, name-derived seeds per cell.
//!
//! Before this module, `name_seed`/`seeds_per_cell`/`seed_expand` were
//! independently reimplemented in at least 11 corpus files across three
//! crates. This is the single copy; corpora migrate onto it rather than
//! rolling their own.
//!
//! **Two name-seed hash flavors exist in the wild, both preserved exactly**
//! (audited when this module was extracted — see the root `CLAUDE.md`'s
//! engineering-lessons log). Most corpora (`raftkv_linearizable`,
//! `txn_serializable`, `reconciler_corpus`, `inplace_split_reconciler`,
//! `learner_corpus`) use the plain FNV-1a hash, [`name_seed`]. Four corpora
//! (`backup_fault_corpus`, `backfill_fault_corpus`, `stream_lineage_corpus`,
//! `pitr_fault_corpus`) additionally force the low bit on, [`odd_name_seed`]
//! — no comment in their history explains why, and unifying the two would
//! silently change which seed an already-committed scenario name replays
//! to (exactly the hazard this module exists to avoid), so both are kept as
//! distinct functions rather than collapsed into one.
//!
//! Two expansion shapes are in use, matching two different corpus authoring
//! styles:
//!
//! - [`seed_expand`] — for corpora that build a `Vec<Scenario>` of cells up
//!   front, then run every cell. Generic over the corpus's own `Scenario`
//!   type via the [`SeedVariant`] trait, so the harness owns the expansion
//!   algorithm without forcing every corpus into one struct shape.
//! - [`for_each_seed`] — for corpora that drive one named scenario directly
//!   (no `Vec<Scenario>`), via a closure. Uses [`odd_name_seed`].

use std::env;

/// FNV-1a 64-bit hash of `name` into a seed. The repo's name→seed map: a
/// scenario's seed is always a deterministic function of its own name,
/// never `std::hash` (whose `SipHash` is randomized per-process, breaking
/// ADR 0003 reproducibility).
pub fn name_seed(name: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// [`name_seed`] with the low bit forced on — the second hash flavor used by
/// four corpora. See the module doc for why this is kept distinct from
/// [`name_seed`] rather than unified with it.
pub fn odd_name_seed(name: &str) -> u64 {
    name_seed(name) | 1
}

/// Depth knob: read an `ANIMUS_*_SEEDS`-shaped env var (`var_name`),
/// default `1`, clamped to a minimum of `1`. Every corpus's depth knob uses
/// this exact parse (see the root `CLAUDE.md`'s test-scaling table).
pub fn seeds_from_env(var_name: &str) -> usize {
    env::var(var_name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1)
        .max(1)
}

/// A scenario cell a corpus can expand into `k` seed variants. A corpus
/// implements this for its own `Scenario` type so [`seed_expand`] can own
/// the expansion algorithm generically.
pub trait SeedVariant: Clone {
    /// This cell's own (canonical/frozen, or already-expanded) name.
    fn scenario_name(&self) -> &str;

    /// A copy of `self` renamed `name` and reseeded `seed` — every other
    /// field (workload, faults, ...) carried over unchanged.
    fn reseeded(&self, name: String, seed: u64) -> Self;
}

/// Expand each cell into `k` seed variants (the house corpus-depth
/// convention): variant 0 keeps the cell's own canonical (frozen) name +
/// seed, so `k <= 1` returns `cells` unchanged — byte-identical to the
/// pre-expansion corpus. Variants `1..k` get a `_sNN`-suffixed name (via
/// [`name_seed`]) and a fresh, name-derived seed, exercising the same
/// scenario under different interleavings.
pub fn seed_expand<T: SeedVariant>(cells: Vec<T>, k: usize) -> Vec<T> {
    if k <= 1 {
        return cells;
    }
    let mut out = Vec::with_capacity(cells.len() * k);
    for cell in cells {
        for i in 0..k {
            if i == 0 {
                out.push(cell.clone());
            } else {
                let name = format!("{}_s{i:02}", cell.scenario_name());
                let seed = name_seed(&name);
                out.push(cell.reseeded(name, seed));
            }
        }
    }
    out
}

/// Run `body` once per seed variant of a scenario named `name`, `k` times
/// total — the closure-based sibling of [`seed_expand`], for corpora that
/// drive a scenario directly rather than building a `Vec<Scenario>` up
/// front. Variant 0 is `odd_name_seed(name)`; variants `1..k` are
/// `odd_name_seed("{name}_s{variant}")` (no zero-padding, matching this
/// shape's own existing convention — distinct from `seed_expand`'s
/// `_s{i:02}`).
pub fn for_each_seed(name: &str, k: usize, mut body: impl FnMut(u64)) {
    for variant in 0..k {
        let seed = if variant == 0 {
            odd_name_seed(name)
        } else {
            odd_name_seed(&format!("{name}_s{variant}"))
        };
        body(seed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reference vectors captured from the pre-refactor corpus files
    /// (raftkv_linearizable.rs / backup_fault_corpus.rs), before this module
    /// existed — pins both hash flavors against silent drift.
    #[test]
    fn name_seed_matches_captured_reference_vectors() {
        assert_eq!(name_seed("baseline_3"), 0xc3d1_5d41_8fef_a97c);
        assert_eq!(name_seed("leader_kill_early_3"), 0x177f_d1d4_d9a9_1f57);
        assert_eq!(odd_name_seed("baseline_3"), name_seed("baseline_3") | 1);
    }

    #[test]
    fn seeds_from_env_defaults_to_one_and_never_zero() {
        // No such var is ever set in this process's env by these tests.
        assert_eq!(seeds_from_env("ANIMUS_TEST_CORPUS_NONEXISTENT_VAR"), 1);
    }

    #[derive(Clone, Debug, PartialEq)]
    struct C {
        name: String,
        seed: u64,
    }

    impl SeedVariant for C {
        fn scenario_name(&self) -> &str {
            &self.name
        }
        fn reseeded(&self, name: String, seed: u64) -> Self {
            C { name, seed }
        }
    }

    #[test]
    fn seed_expand_k1_is_identity() {
        let cells = vec![
            C {
                name: "a".into(),
                seed: 1,
            },
            C {
                name: "b".into(),
                seed: 2,
            },
        ];
        assert_eq!(seed_expand(cells.clone(), 1), cells);
        assert_eq!(seed_expand(cells.clone(), 0), cells);
    }

    #[test]
    fn seed_expand_keeps_variant_zero_canonical_and_derives_the_rest() {
        let cells = vec![C {
            name: "cell".into(),
            seed: 42,
        }];
        let expanded = seed_expand(cells, 3);
        assert_eq!(expanded.len(), 3);
        assert_eq!(
            expanded[0],
            C {
                name: "cell".into(),
                seed: 42
            }
        );
        assert_eq!(expanded[1].name, "cell_s01");
        assert_eq!(expanded[1].seed, name_seed("cell_s01"));
        assert_eq!(expanded[2].name, "cell_s02");
        assert_eq!(expanded[2].seed, name_seed("cell_s02"));
    }

    #[test]
    fn for_each_seed_matches_cell_seed_shape() {
        let mut seeds = Vec::new();
        for_each_seed("widgets", 3, |s| seeds.push(s));
        assert_eq!(seeds.len(), 3);
        assert_eq!(seeds[0], odd_name_seed("widgets"));
        assert_eq!(seeds[1], odd_name_seed("widgets_s1"));
        assert_eq!(seeds[2], odd_name_seed("widgets_s2"));
    }
}
