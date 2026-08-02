//! **The frozen, generated scenario corpus** (deliverable 4 of the
//! consistency-testing milestone).
//!
//! [`support::corpus`] materializes a committed, deterministic, named set of
//! scenarios with combinatorial coverage of the fault matrix
//! (fault type × timing × workload shape × cluster shape), plus no-fault
//! baselines and compound (lossy / overlapping) scenarios. This file is the
//! **parametric runner**: it runs every scenario through the Elle checkers
//! (`check_cycles`/`check_durability`/`check_convergence`) and fails naming the
//! specific scenario and its replay seed.
//!
//! The corpus is *not* a live-random test — it runs the SAME scenarios every
//! time (regenerating/growing it is an explicit edit to `support::corpus`), so a
//! failure is reproducible and attributable, and any scenario that ever catches a
//! bug stays as a permanent regression.
//!
//! Each scenario points the serializability checker at Accord under **real
//! contention** (overlapping multi-key transactions), so a green run means the
//! checker actually had conflicts to chew on — see `support/mod.rs` for the
//! list-append-over-Accord modelling.

mod support;

use std::collections::BTreeSet;

use support::{
    NemesisAction, Topology, corpus, corpus_base, corpus_extended, run_scenario, run_scenario_with,
    seed_expand,
};

/// Run a single scenario against the **serialization-authoritative** topology
/// (pure Accord) and assert **serializability** — the property that layer claims.
///
/// It deliberately asserts **only `cycles`**, not convergence/durability. Pure
/// Accord guarantees the committed *order* among replicas that received the
/// commit; it does **not** guarantee every replica's local store holds every
/// committed write — backfilling a laggard that missed a `Commit` is the
/// **data-plane frontier's** job (anti-entropy), not the ordering layer's. So
/// reading convergence/durability off a single pure-Accord replica's store is
/// unsound under faults (a `stop_restart` can leave a non-quorum replica behind
/// forever) — exactly mirroring why `cycles` is unsound over the AP frontier.
/// Convergence + durability are checked where they hold:
/// [`frontier_corpus_converges_and_is_durable`]. (Found by the deep tier:
/// `ext_t_stop_restart_winddown` diverged on pure Accord but converges on the
/// frontier — see ADR 0014.)
fn assert_scenario_consistent(scenario: &support::Scenario) {
    let r = run_scenario(scenario);
    let name = &scenario.name;
    let seed = scenario.seed;
    assert!(
        r.cycles.ok,
        "[{name}] serializability cycle (seed={seed}): {:?}",
        r.cycles.violations
    );
}

/// The whole corpus is **serializable** under the authoritative topology. This is
/// the headline suite — it asserts `cycles` (Accord's serialization claim) on
/// every scenario; convergence + durability are the data plane's guarantees and
/// are checked separately by [`frontier_corpus_converges_and_is_durable`].
///
/// Size scales with the env knobs (`ANIMUS_CORPUS_SEEDS` / `ANIMUS_CORPUS_FULL`);
/// at their defaults this is the frozen base set. The structural guards below
/// deliberately run against [`corpus_base`] so they stay fast and env-independent.
#[test]
fn corpus_is_consistent() {
    let scenarios = corpus();
    assert!(
        scenarios.len() >= 60,
        "corpus shrank unexpectedly to {} scenarios",
        scenarios.len()
    );
    for scenario in &scenarios {
        assert_scenario_consistent(scenario);
    }
}

/// The **frontier corpus**: the same scenarios run against the AP data-plane
/// frontier ([`Topology::Frontier`]), checked for what that layer *offers* —
/// **convergence** (two final reads agree) and **durability** (every acked write
/// is in the final state). It deliberately does **not** assert `cycles`: a read
/// through the eventually-consistent quorum can transiently observe a torn/stale
/// multi-key write under a data-replica fault (it converges via anti-entropy), so
/// serializability is unsound to assert here — that is the authoritative topology's
/// job ([`corpus_is_consistent`]). This pairs with the repo principle: point the
/// serializability checker at Accord, check the AP plane for convergence/RYW.
///
/// Runs the **bounded base corpus** (`corpus_base`, 119), *not* the seed-expanded
/// / extended set — deliberately. Convergence + durability are **eventual**
/// properties (anti-entropy + coordinator retry), so "did it converge within the
/// runner's fixed post-heal drain?" is only a sound *hard* assertion on a bounded,
/// non-pathological set. At adversarial seed-depth a compound fault
/// (e.g. `lossy` + `stop_restart`) can legitimately leave convergence still in
/// flight when the drain ends — on **either** topology — so scaling this to depth
/// makes it flaky without revealing a safety bug. Serializability (a *safety*
/// property) is the lever that scales to depth ([`corpus_is_consistent`]); this
/// stays bounded. (Found by the deep tier: `lossy_stop_restart_mid_s36` diverged
/// here while pure Accord converged, and `ext_t_stop_restart_winddown_s39` did the
/// reverse — neither layer converges within a fixed bound under every compound
/// fault. See ADR 0014.)
#[test]
fn frontier_corpus_converges_and_is_durable() {
    for scenario in &corpus_base() {
        let r = run_scenario_with(scenario, Topology::Frontier);
        let name = &scenario.name;
        let seed = scenario.seed;
        assert!(
            r.convergence.ok,
            "[frontier {name}] final replica states diverged (seed={seed}): {:?}",
            r.convergence.violations
        );
        assert!(
            r.durability.ok,
            "[frontier {name}] acknowledged write lost from final state (seed={seed}): {:?}",
            r.durability.violations
        );
    }
}

/// Coverage guard: the corpus must actually exercise every fault type and both
/// cluster shapes — otherwise a dimension silently stopped being tested. This
/// keeps the matrix honest as the generator evolves.
#[test]
fn corpus_covers_the_fault_matrix() {
    let scenarios = corpus_base();

    // Every single-fault nemesis action appears somewhere.
    let mut seen_faults: BTreeSet<NemesisAction> = BTreeSet::new();
    let mut seen_accord_shapes: BTreeSet<usize> = BTreeSet::new();
    let mut seen_data_shapes: BTreeSet<usize> = BTreeSet::new();
    let mut compound = 0usize;
    for s in &scenarios {
        seen_accord_shapes.insert(s.cluster.accord_replicas);
        seen_data_shapes.insert(s.cluster.data_replicas);
        if s.faults.len() > 1 {
            compound += 1;
        }
        for (_, f) in &s.faults {
            seen_faults.insert(*f);
        }
        // Names are unique and stable.
    }

    for f in [
        NemesisAction::PartitionMinority,
        NemesisAction::PartitionMajority,
        NemesisAction::IsolateOne,
        NemesisAction::Crash,
        NemesisAction::StopRestart,
        NemesisAction::LeaderKill,
        NemesisAction::Lossy,
    ] {
        assert!(
            seen_faults.contains(&f),
            "fault {f:?} is not covered by any corpus scenario"
        );
    }
    assert!(
        seen_accord_shapes.contains(&3) && seen_accord_shapes.contains(&5),
        "both 3- and 5-replica Accord shapes must be covered: {seen_accord_shapes:?}"
    );
    assert!(
        seen_data_shapes.contains(&3) && seen_data_shapes.contains(&5),
        "both 3- and 5-replica data shapes must be covered: {seen_data_shapes:?}"
    );
    assert!(
        compound >= 2,
        "expected ≥ 2 compound (multi-fault) scenarios, found {compound}"
    );

    // Names are unique (so a failure unambiguously names one scenario).
    let names: BTreeSet<&str> = scenarios.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(
        names.len(),
        scenarios.len(),
        "corpus scenario names must be unique"
    );
    // Seeds are unique (so replay is unambiguous).
    let seeds: BTreeSet<u64> = scenarios.iter().map(|s| s.seed).collect();
    assert_eq!(
        seeds.len(),
        scenarios.len(),
        "corpus scenario seeds must be unique"
    );
}

/// Non-vacuity: a healthy fraction of corpus scenarios must genuinely contend
/// (a key written ≥ 2 times) and acknowledge writes — otherwise the green
/// `check_cycles` verdicts are meaningless. We sample the no-fault baselines and
/// the tight-contention scenarios, which should always contend.
#[test]
fn corpus_has_real_contention() {
    let scenarios = corpus_base();
    let mut contended = 0usize;
    let mut sampled = 0usize;
    for s in &scenarios {
        // Sample the baselines and tight-contention faulted scenarios (cheap
        // subset that must contend); skip the rest to keep this test fast.
        if s.name.contains("baseline") || s.name.contains("_tight_") {
            sampled += 1;
            let r = run_scenario(s);
            if r.contended && r.ok_writes >= 6 {
                contended += 1;
            }
        }
    }
    assert!(
        sampled >= 3,
        "expected to sample ≥ 3 scenarios, got {sampled}"
    );
    assert!(
        contended * 2 >= sampled,
        "fewer than half of sampled scenarios genuinely contended \
         ({contended}/{sampled}) — the checker may have nothing to chew on"
    );
}

/// The corpus is deterministic: running a representative scenario twice records
/// byte-identical histories (ADR 0003). A flaky corpus would make a failure
/// unreproducible.
#[test]
fn corpus_scenarios_are_deterministic() {
    // Pick a faulted scenario (more moving parts → a stronger determinism test).
    let scenarios = corpus_base();
    let scenario = scenarios
        .iter()
        .find(|s| s.name.contains("crash") && s.name.contains("mid"))
        .expect("a crash/mid scenario exists");
    let a = run_scenario(scenario);
    let b = run_scenario(scenario);
    assert_eq!(
        a.history.entries, b.history.entries,
        "[{}] scenario not deterministic from seed {}",
        scenario.name, scenario.seed
    );
}

/// Seed-depth lever (`ANIMUS_CORPUS_SEEDS`): expanding the base set by `k` yields
/// exactly `k×` scenarios, every name and seed stays unique, and **variant 0
/// preserves the canonical (frozen) name+seed** — so growing depth never moves an
/// existing regression seed. This is a pure structural check (no scenario runs).
#[test]
fn seed_expansion_is_additive_and_unique() {
    let base = corpus_base();
    let k = 3;
    let expanded = seed_expand(base.clone(), k);

    assert_eq!(
        expanded.len(),
        base.len() * k,
        "seed-expansion must yield exactly k× scenarios"
    );

    let names: BTreeSet<&str> = expanded.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names.len(), expanded.len(), "expanded names must be unique");
    let seeds: BTreeSet<u64> = expanded.iter().map(|s| s.seed).collect();
    assert_eq!(seeds.len(), expanded.len(), "expanded seeds must be unique");

    // Every base scenario survives byte-identical (canonical name + seed) — the
    // frozen-regression guarantee under growth.
    for b in &base {
        let kept = expanded
            .iter()
            .find(|s| s.name == b.name)
            .unwrap_or_else(|| panic!("base scenario {} missing after expansion", b.name));
        assert_eq!(
            kept.seed, b.seed,
            "base scenario {} seed must be preserved by expansion",
            b.name
        );
    }

    // k == 1 is the identity (the always-on default is byte-identical to base).
    assert_eq!(seed_expand(base.clone(), 1).len(), base.len());
}

/// Breadth lever (`ANIMUS_CORPUS_FULL`): the extended tier adds genuinely new
/// dimension values — the `SlowLinks` fault, a 7-replica shape, and an asymmetric
/// shape — without colliding with or perturbing any base name/seed. Structural
/// only (no scenario runs).
#[test]
fn extended_tier_adds_new_dimensions() {
    let base = corpus_base();
    let ext = corpus_extended();
    assert!(!ext.is_empty(), "extended tier must produce scenarios");

    // Names are all `ext_`-prefixed and disjoint from the base set, so merging the
    // tiers can never perturb a frozen base name/seed.
    let base_names: BTreeSet<&str> = base.iter().map(|s| s.name.as_str()).collect();
    for s in &ext {
        assert!(
            s.name.starts_with("ext_"),
            "extended scenario {} must be ext_-prefixed",
            s.name
        );
        assert!(
            !base_names.contains(s.name.as_str()),
            "extended scenario {} collides with a base name",
            s.name
        );
    }

    // New fault dimension: SlowLinks appears (it never does in the base set).
    let has_slow = ext
        .iter()
        .any(|s| s.faults.iter().any(|(_, f)| *f == NemesisAction::SlowLinks));
    assert!(has_slow, "extended tier must exercise SlowLinks");

    // New cluster dimensions: a 7-replica shape and an asymmetric shape.
    assert!(
        ext.iter().any(|s| s.cluster.accord_replicas == 7),
        "extended tier must exercise a 7-replica shape"
    );
    assert!(
        ext.iter()
            .any(|s| s.cluster.accord_replicas != s.cluster.data_replicas),
        "extended tier must exercise an asymmetric shape"
    );
}
