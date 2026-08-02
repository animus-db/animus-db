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

use support::{NemesisAction, corpus, run_scenario};

/// Run a single scenario and assert all three checkers pass, with the scenario
/// name + seed in every message for attributable, replayable failures.
fn assert_scenario_consistent(scenario: &support::Scenario) {
    let r = run_scenario(scenario);
    let name = &scenario.name;
    let seed = scenario.seed;
    assert!(
        r.cycles.ok,
        "[{name}] serializability cycle (seed={seed}): {:?}",
        r.cycles.violations
    );
    assert!(
        r.convergence.ok,
        "[{name}] final replica states diverged (seed={seed}): {:?}",
        r.convergence.violations
    );
    assert!(
        r.durability.ok,
        "[{name}] acknowledged write lost from final state (seed={seed}): {:?}",
        r.durability.violations
    );
}

/// The whole corpus passes every checker. This is the headline suite.
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

/// Coverage guard: the corpus must actually exercise every fault type and both
/// cluster shapes — otherwise a dimension silently stopped being tested. This
/// keeps the matrix honest as the generator evolves.
#[test]
fn corpus_covers_the_fault_matrix() {
    let scenarios = corpus();

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
    let scenarios = corpus();
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
    let scenarios = corpus();
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
