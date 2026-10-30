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
//!
//! **Seed sharding (`ANIMUS_SEED_SHARD=i/n`, issue #1271).** A deep corpus can
//! be split across `n` parallel workers: shard `i` (0-based) runs only the
//! seed variants whose index `% n == i`, so the union of the `n` shards is
//! exactly the unsharded run and the shards are disjoint. Variant 0 (the
//! canonical, frozen cell) therefore lives in shard 0 alone. Unset, nothing
//! changes. The expansion helpers above apply it themselves; a corpus that
//! computes its own seed list or `0..k` loop MUST route it through
//! [`shard_retain`] / [`shard_range`] (or call [`reject_shard`] if it cannot
//! be split), so that no corpus ignores the knob silently. A malformed value
//! panics.

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

/// Env var carrying the seed shard, `i/n` (see the module doc).
pub const SHARD_VAR: &str = "ANIMUS_SEED_SHARD";

/// One worker's slice of the seed variants: variant `v` belongs to shard
/// `v % of`. Constructed by [`Shard::parse`] / [`shard_from_env`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shard {
    pub index: usize,
    pub of: usize,
}

impl Shard {
    /// Parse `i/n` with `n >= 1` and `i < n`.
    pub fn parse(s: &str) -> Result<Shard, String> {
        let bad = || format!("{SHARD_VAR}={s:?}: expected `i/n` with 0 <= i < n and n >= 1");
        let (i, n) = s.split_once('/').ok_or_else(bad)?;
        let index: usize = i.trim().parse().map_err(|_| bad())?;
        let of: usize = n.trim().parse().map_err(|_| bad())?;
        if of == 0 || index >= of {
            return Err(bad());
        }
        Ok(Shard { index, of })
    }

    /// Whether this shard runs seed variant `variant`.
    pub fn keeps(&self, variant: usize) -> bool {
        variant % self.of == self.index
    }
}

/// The shard from `ANIMUS_SEED_SHARD`, `None` when unset. Panics on a
/// malformed value: a typo must never silently run the whole corpus (or
/// nothing) on every worker.
pub fn shard_from_env() -> Option<Shard> {
    match env::var(SHARD_VAR) {
        Ok(v) => Some(Shard::parse(&v).unwrap_or_else(|e| panic!("{e}"))),
        Err(env::VarError::NotPresent) => None,
        Err(e) => panic!("{SHARD_VAR}: {e}"),
    }
}

/// Pure core of [`shard_retain`].
pub fn shard_retain_with<T>(items: Vec<T>, shard: Option<Shard>) -> Vec<T> {
    match shard {
        None => items,
        Some(sh) => {
            let total = items.len();
            let kept: Vec<T> = items
                .into_iter()
                .enumerate()
                .filter(|(v, _)| sh.keeps(*v))
                .map(|(_, t)| t)
                .collect();
            trace_shard(sh, total, (0..total).filter(|v| sh.keeps(*v)));
            kept
        }
    }
}

/// `ANIMUS_SHARD_TRACE=1` prints the variant indices a shard kept out of
/// `total`, so a run at small K for each shard shows that the shards' union is
/// the unsharded seed set (`sort -u` the `SHARD_TRACE` lines per shard).
fn trace_shard(sh: Shard, total: usize, kept: impl Iterator<Item = usize>) {
    if env::var_os("ANIMUS_SHARD_TRACE").is_some() {
        let kept: Vec<usize> = kept.collect();
        eprintln!("SHARD_TRACE {}/{} of {total}: {kept:?}", sh.index, sh.of);
    }
}

/// Keep only this shard's items of a seed list whose position is its variant
/// index (position 0 is the canonical variant). Identity when unset.
pub fn shard_retain<T>(items: Vec<T>) -> Vec<T> {
    shard_retain_with(items, shard_from_env())
}

/// `0..k`, restricted to this shard's variant indices (all of `0..k` when
/// unset). For a `for i in 0..k` seed loop.
pub fn shard_range(k: u64) -> impl Iterator<Item = u64> {
    let shard = shard_from_env();
    if let Some(sh) = shard {
        trace_shard(sh, k as usize, (0..k as usize).filter(|v| sh.keeps(*v)));
    }
    (0..k).filter(move |v| shard.is_none_or(|s| s.keeps(*v as usize)))
}

/// Whether this worker runs the corpus's *unsharded* tests: aggregate negative
/// controls ("at least one seed shows the failure") and meta-tests that
/// inspect the whole cell set cannot be split by seed, so they run once, in
/// full, on shard 0 (or always when no shard is set) and are skipped by the
/// other shards.
pub fn owns_unsharded() -> bool {
    shard_from_env().is_none_or(|s| s.index == 0)
}

/// [`shard_range`] for a `usize` loop bound.
pub fn shard_range_usize(k: usize) -> impl Iterator<Item = usize> {
    let shard = shard_from_env();
    (0..k).filter(move |v| shard.is_none_or(|s| s.keeps(*v)))
}

/// For a corpus that cannot be split by seed (a fixed-seed regression, an
/// index-addressed seed list): fail loudly if a shard was requested, rather
/// than silently running it in full on every worker.
pub fn reject_shard(corpus: &str) {
    assert!(
        shard_from_env().is_none(),
        "{corpus} cannot be sharded by seed; unset {SHARD_VAR}"
    );
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
    seed_expand_sharded(cells, k, shard_from_env())
}

/// [`seed_expand`] with the shard passed explicitly (the pure core). Variant
/// `v` of every cell is kept only by shard `v % of`; with `k <= 1` only
/// variant 0 exists, so only shard 0 runs anything.
pub fn seed_expand_sharded<T: SeedVariant>(
    cells: Vec<T>,
    k: usize,
    shard: Option<Shard>,
) -> Vec<T> {
    if k <= 1 {
        return shard_retain_with_variant0(cells, shard);
    }
    let mut out = Vec::with_capacity(cells.len() * k);
    for cell in cells {
        for i in 0..k {
            if !shard.is_none_or(|s| s.keeps(i)) {
                continue;
            }
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

fn shard_retain_with_variant0<T>(cells: Vec<T>, shard: Option<Shard>) -> Vec<T> {
    match shard {
        Some(s) if !s.keeps(0) => Vec::new(),
        _ => cells,
    }
}

/// The pure core of [`corpus_cells`]: with `replay = Some(seed)` every cell is
/// reseeded to `seed` under its own name (an `ANIMUS_SEED` replay, no depth
/// expansion); otherwise the cells are expanded `k` deep with [`seed_expand`].
/// Either way, `filter` keeps only cells whose name contains it.
pub fn expand_cells<T: SeedVariant>(
    cells: Vec<T>,
    replay: Option<u64>,
    k: usize,
    filter: Option<&str>,
) -> Vec<T> {
    expand_cells_sharded(cells, replay, k, filter, shard_from_env())
}

/// [`expand_cells`] with the shard passed explicitly (`None` = every variant).
pub fn expand_cells_sharded<T: SeedVariant>(
    cells: Vec<T>,
    replay: Option<u64>,
    k: usize,
    filter: Option<&str>,
    shard: Option<Shard>,
) -> Vec<T> {
    let expanded = if let Some(seed) = replay {
        cells
            .into_iter()
            .map(|c| {
                let name = c.scenario_name().to_owned();
                c.reseeded(name, seed)
            })
            .collect()
    } else {
        seed_expand_sharded(cells, k, shard)
    };
    expanded
        .into_iter()
        .filter(|c| filter.is_none_or(|f| c.scenario_name().contains(f)))
        .collect()
}

/// The cells a corpus runs, after the depth knob, `ANIMUS_SEED` replay and an
/// optional cell-name filter: `seeds_var` is the corpus's own depth knob (see
/// [`seeds_from_env`]) and `cell_var` its name-substring filter. An
/// `ANIMUS_SEED` that parses as a `u64` replays every cell at that seed;
/// see [`expand_cells`].
pub fn corpus_cells<T: SeedVariant>(cells: Vec<T>, seeds_var: &str, cell_var: &str) -> Vec<T> {
    let filter = env::var(cell_var).ok();
    let replay = env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok());
    expand_cells(cells, replay, seeds_from_env(seeds_var), filter.as_deref())
}

/// [`corpus_cells`] over *every* seed variant, ignoring `ANIMUS_SEED_SHARD`: for
/// an aggregate check pinned to shard 0 ([`owns_unsharded`]) that must see the
/// whole seed set.
pub fn corpus_cells_unsharded<T: SeedVariant>(
    cells: Vec<T>,
    seeds_var: &str,
    cell_var: &str,
) -> Vec<T> {
    let filter = env::var(cell_var).ok();
    let replay = env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok());
    expand_cells_sharded(
        cells,
        replay,
        seeds_from_env(seeds_var),
        filter.as_deref(),
        None,
    )
}

/// Run `body` once per seed variant of a scenario named `name`, `k` times
/// total — the closure-based sibling of [`seed_expand`], for corpora that
/// drive a scenario directly rather than building a `Vec<Scenario>` up
/// front. Variant 0 is `odd_name_seed(name)`; variants `1..k` are
/// `odd_name_seed("{name}_s{variant}")` (no zero-padding, matching this
/// shape's own existing convention — distinct from `seed_expand`'s
/// `_s{i:02}`).
pub fn for_each_seed(name: &str, k: usize, mut body: impl FnMut(u64)) {
    let shard = shard_from_env();
    for variant in 0..k {
        if !shard.is_none_or(|s| s.keeps(variant)) {
            continue;
        }
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
    fn expand_cells_replays_filters_and_expands() {
        let cells = vec![
            C {
                name: "alpha".into(),
                seed: 1,
            },
            C {
                name: "beta".into(),
                seed: 2,
            },
        ];
        // Replay: every cell keeps its own name, takes the replay seed, and
        // the depth knob is ignored.
        let replayed = expand_cells(cells.clone(), Some(7), 3, None);
        assert_eq!(
            replayed,
            vec![
                C {
                    name: "alpha".into(),
                    seed: 7
                },
                C {
                    name: "beta".into(),
                    seed: 7
                },
            ]
        );
        // No replay: plain seed_expand, then the name filter.
        assert_eq!(expand_cells(cells.clone(), None, 1, None), cells);
        assert_eq!(
            expand_cells(cells.clone(), None, 1, Some("bet")),
            cells[1..]
        );
        let deep = expand_cells(cells.clone(), None, 2, Some("alpha"));
        assert_eq!(deep.len(), 2);
        assert_eq!(deep[1].name, "alpha_s01");
        // The filter applies to a replay too.
        assert_eq!(expand_cells(cells, Some(9), 1, Some("alp")).len(), 1);
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

    fn cells3() -> Vec<C> {
        ["a", "b", "c"]
            .iter()
            .enumerate()
            .map(|(i, n)| C {
                name: (*n).into(),
                seed: i as u64,
            })
            .collect()
    }

    #[test]
    fn shard_parse_accepts_valid_and_rejects_malformed() {
        assert_eq!(Shard::parse("0/1"), Ok(Shard { index: 0, of: 1 }));
        assert_eq!(Shard::parse("2/4"), Ok(Shard { index: 2, of: 4 }));
        for bad in [
            "", "1", "1/", "/2", "a/b", "0/0", "2/2", "3/2", "-1/2", "1/2/3",
        ] {
            assert!(Shard::parse(bad).is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn unset_shard_leaves_expansion_unchanged() {
        for k in [0, 1, 2, 5] {
            let plain = seed_expand_sharded(cells3(), k, None);
            // Byte-for-byte what the pre-shard algorithm produced.
            let mut want = Vec::new();
            for c in cells3() {
                for i in 0..k.max(1) {
                    if i == 0 {
                        want.push(c.clone());
                    } else {
                        let n = format!("{}_s{i:02}", c.name);
                        want.push(C {
                            seed: name_seed(&n),
                            name: n,
                        });
                    }
                }
            }
            assert_eq!(plain, want, "k={k}");
        }
        assert_eq!(shard_retain_with(vec![1, 2, 3], None), vec![1, 2, 3]);
    }

    #[test]
    fn shards_partition_the_expansion_exactly() {
        for k in [0usize, 1, 2, 3, 7, 10] {
            for n in 1..=5usize {
                let full = seed_expand_sharded(cells3(), k, None);
                let mut union: Vec<C> = Vec::new();
                for i in 0..n {
                    let part = seed_expand_sharded(cells3(), k, Some(Shard { index: i, of: n }));
                    for c in &part {
                        assert!(!union.contains(c), "k={k} n={n}: {c:?} in two shards");
                    }
                    union.extend(part);
                }
                let mut a: Vec<String> = full.iter().map(|c| c.name.clone()).collect();
                let mut b: Vec<String> = union.iter().map(|c| c.name.clone()).collect();
                a.sort();
                b.sort();
                assert_eq!(a, b, "k={k} n={n}: union != unsharded run");
            }
        }
    }

    #[test]
    fn canonical_variant_zero_lands_in_exactly_one_shard() {
        for n in 1..=4usize {
            let owners: Vec<usize> = (0..n)
                .filter(|i| {
                    seed_expand_sharded(cells3(), 6, Some(Shard { index: *i, of: n }))
                        .iter()
                        .any(|c| c.name == "a")
                })
                .collect();
            assert_eq!(owners, vec![0], "n={n}");
        }
    }

    #[test]
    fn shard_retain_partitions_a_seed_list() {
        let all: Vec<u64> = (100..113).collect();
        for n in 1..=6usize {
            let mut union = Vec::new();
            for i in 0..n {
                union.extend(shard_retain_with(
                    all.clone(),
                    Some(Shard { index: i, of: n }),
                ));
            }
            union.sort();
            assert_eq!(union, all, "n={n}");
        }
    }
}
