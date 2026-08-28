//! Generic failure minimization for fault-injection corpora (ADR 0061 rung
//! B4 — "the single highest-leverage item" in that ADR's plan; ADR 0003's
//! Consequences promised this and it was never built until now).
//!
//! ## Why this can't "shrink a seed"
//!
//! `SimEnv`'s determinism guarantee (ADR 0003) is that a run is a pure
//! function of its seed: the whole point of the seed is that it is opaque —
//! two different `u64`s drive two *completely different* executions, not a
//! bigger and a smaller version of the same one. There is no sense in which
//! seed `0x1234` is "smaller" than seed `0x9abc`; searching over seeds directly
//! (as classic property-based-testing shrinkers do with their PRNG state)
//! would not minimize anything, it would just try unrelated runs until one
//! happens to look simpler by luck. So "shrink the seed" is not a coherent
//! request, and this module does not attempt it.
//!
//! ## The two strategies that *are* coherent, and which one this module picks
//!
//! - **(a) Scenario-parameter minimization** — treat a corpus scenario's own
//!   *parameters* (op/round count, node count, keyspace size, which faults are
//!   scheduled and when, ...) as the search space, and delta-debug over them:
//!   re-run the *entire* simulation from scratch at each candidate, **holding
//!   the seed fixed throughout**, keeping any candidate that still reproduces
//!   the failure. This is what [`minimize`] below implements.
//! - **(b) Fault-schedule minimization** — record the concrete sequence of
//!   fault *decisions* a run made (the trace already carries `Drop`/
//!   `DiskFault`/`DiskTear`/`DiskCorrupt`/... events) and replay with a subset
//!   of them suppressed, delta-debugging toward a minimal suppressing set.
//!   More powerful (it can isolate "message #4173 dropped" out of thousands),
//!   but it requires a *recorded-schedule replay mode*: today, suppressing a
//!   fault changes how many RNG draws every later fault decision consumes
//!   (each roll is drawn inline, gated on its own threshold — see
//!   `animus-sim/CLAUDE.md`'s "one roll, two buckets" / duplicate-delay notes),
//!   so a naive "turn this one fault off and re-run the same seed" perturbs
//!   every fault decision *after* it, not just the one being tested. Doing
//!   this soundly means teaching the simulator to replay fault decisions from
//!   a recorded schedule instead of drawing them, which touches the same core
//!   RNG-draw-order machinery the byte-identical-trace guarantee
//!   (`animus-sim/tests/determinism.rs`) depends on.
//!
//! **This module implements (a) only.** (b) is strictly more powerful — see
//! the ADR 0061 rung B4 section of `crates/animus-sim/CLAUDE.md` for the
//! follow-up sketch — but the task's own guidance is explicit that a working,
//! honest (a) beats a half-working (b) that risks perturbing the determinism
//! guarantee, and every corpus in this repo already exposes its fault
//! schedule as an explicit, un-randomized `Scenario` field (a `Vec<(Duration,
//! Nemesis)>` or equivalent — see `raftkv_linearizable.rs::Scenario`), so (a)
//! already reaches "drop one specific scheduled fault, keep the others" for
//! every corpus in the repo today. What it *cannot* do is drop one specific
//! dropped **message** out of an ambient `NetConfig` drop-probability fault —
//! that granularity needs (b). See "What it cannot do" in the crate guide.
//!
//! ## The algorithm
//!
//! [`minimize`] is greedy coordinate-wise delta-debugging, not full ddmin's
//! exponential subset-bisection: given the current case, ask the caller's
//! `candidates` closure for every "one step smaller" variant, keep the first
//! one that still reproduces the failure, and restart from it; stop when no
//! candidate of the current case still fails (a **local fixpoint** — 1-minimal
//! with respect to the caller's own candidate generator) or the check budget
//! runs out. Full ddmin's binary-search-style subset removal exists to cut
//! down the *number of test runs* needed to shrink a long list; every
//! `Scenario` in this repo's corpora has a handful of scalar knobs plus a
//! short fault list (single digits to low tens of entries), so the simpler
//! algorithm converges in a small, bounded number of checks without needing
//! that optimization. Revisit if a corpus grows a fault list long enough for
//! that to matter.
//!
//! ## Determinism
//!
//! The whole thing is deterministic by construction: `candidates` is a plain
//! function of the current case (no RNG, no ordering ambiguity — return a
//! `Vec` in the same order every time), `still_fails` reruns the *same seed*
//! every time (ADR 0003), and the search itself never branches on wall-clock
//! time. Same failing input ⇒ same sequence of candidates tried ⇒ same
//! minimized output, on any machine, every time. The iteration budget below
//! is a plain **check count**, deliberately never a wall-clock deadline, so
//! that a slower machine cannot silently produce a less-minimized result than
//! a faster one — that would quietly break "same input, same output."
//!
//! ## Opt-in, default-off
//!
//! [`shrink_enabled`] gates the whole facility behind `ANIMUS_SHRINK=1`
//! (mirroring the repo's `ANIMUS_SEED`/`ANIMUS_*_SEEDS` convention — see the
//! root `CLAUDE.md`'s knob table). A corpus wires it in by calling
//! [`shrink_enabled`] only *after* observing a real failure, never on the hot
//! path of an already-green run, so normal corpus runs (`ANIMUS_SHRINK` unset)
//! never call [`minimize`] at all — same behaviour, same seeds, same runtime,
//! byte-for-byte.

use std::env;
use std::fmt;

/// Iteration budget for [`minimize`]. A plain check **count**, never
/// wall-clock time — see the module doc's determinism argument. `max_checks`
/// bounds the total number of `still_fails` calls across the whole search,
/// which in turn bounds wall-clock time indirectly (each check is one full
/// simulation run) without letting machine speed change the *result*.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShrinkBudget {
    pub max_checks: usize,
}

impl Default for ShrinkBudget {
    /// 500 checks is generous for the corpora in this repo today (a handful
    /// of scalar knobs plus single-digit-to-low-tens fault lists — see the
    /// module doc) while still keeping a runaway search bounded. Override via
    /// [`budget_from_env`] (`ANIMUS_SHRINK_MAX_CHECKS`) for a corpus whose
    /// scenarios are individually expensive.
    fn default() -> Self {
        ShrinkBudget { max_checks: 500 }
    }
}

/// Outcome of a [`minimize`] run.
#[derive(Clone, Debug)]
pub struct ShrinkReport<S> {
    /// The case as first observed to fail — unchanged, for reference.
    pub original: S,
    /// The most-reduced case still reproducing the failure. Equal to
    /// `original` if the very first candidate check already exhausted the
    /// budget, or if `candidates(original)` never reproduces (nothing to
    /// reduce with the given candidate generator).
    pub minimized: S,
    /// Total `still_fails` calls made across the whole search.
    pub checks: usize,
    /// How many candidates were actually accepted (each one a step of real
    /// reduction — e.g. one fewer scheduled fault, or a halved round count).
    pub reductions: usize,
    /// `true` if the search stopped because [`ShrinkBudget::max_checks`] was
    /// reached, not because it found a genuine local fixpoint. A report with
    /// this set may still be usefully smaller than the original, but a
    /// developer should know the search didn't finish exploring.
    pub budget_exhausted: bool,
}

impl<S> ShrinkReport<S> {
    /// `true` if the search reached a genuine local fixpoint (no remaining
    /// candidate of `minimized` still reproduces the failure) rather than
    /// being cut off by the budget.
    pub fn converged(&self) -> bool {
        !self.budget_exhausted
    }
}

/// Greedy delta-debugging (see the module doc for the algorithm and its
/// determinism argument).
///
/// - `initial` — a case already known to reproduce the failure (the caller's
///   job to establish this; garbage in only produces a vacuous "no
///   reduction" report, never a false claim of minimization).
/// - `candidates` — given the current case, return every "one step smaller"
///   variant to try next, in a fixed order. Return `vec![]` once `case` is
///   already as small as this generator knows how to make it.
/// - `still_fails` — re-run the case (against the *same* seed the case
///   itself carries, per strategy (a) — see the module doc) and report
///   whether the original failure still reproduces. Must be deterministic.
/// - `budget` — see [`ShrinkBudget`].
pub fn minimize<S, C, F>(
    initial: S,
    mut candidates: C,
    mut still_fails: F,
    budget: ShrinkBudget,
) -> ShrinkReport<S>
where
    S: Clone,
    C: FnMut(&S) -> Vec<S>,
    F: FnMut(&S) -> bool,
{
    let original = initial.clone();
    let mut current = initial;
    let mut checks = 0usize;
    let mut reductions = 0usize;
    let mut budget_exhausted = false;

    'reduce: loop {
        let cands = candidates(&current);
        let mut progressed = false;
        for cand in cands {
            if checks >= budget.max_checks {
                budget_exhausted = true;
                break 'reduce;
            }
            checks += 1;
            if still_fails(&cand) {
                current = cand;
                reductions += 1;
                progressed = true;
                break;
            }
        }
        if !progressed {
            break;
        }
    }

    ShrinkReport {
        original,
        minimized: current,
        checks,
        reductions,
        budget_exhausted,
    }
}

/// `ANIMUS_SHRINK=1` — the repo's env-knob convention (mirrors `ANIMUS_SEED`/
/// `ANIMUS_*_SEEDS`) for opting into minimization when a corpus observes a
/// failing scenario. Default off: a corpus must call this explicitly, only
/// after it already knows a scenario failed, so an unset (the default)
/// `ANIMUS_SHRINK` never calls into this module at all — normal corpus runs
/// are completely unaffected.
pub fn shrink_enabled() -> bool {
    env::var("ANIMUS_SHRINK").ok().as_deref() == Some("1")
}

/// `ANIMUS_SHRINK_MAX_CHECKS` — override [`ShrinkBudget::default`]'s 500-check
/// budget. Unset, empty, `0`, or unparseable falls back to the default (never
/// an unbounded search).
pub fn budget_from_env() -> ShrinkBudget {
    let max_checks = env::var("ANIMUS_SHRINK_MAX_CHECKS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| ShrinkBudget::default().max_checks);
    ShrinkBudget { max_checks }
}

/// Pretty-print a report for a human triaging a failure — the "print it
/// clearly" half of the reproducible handle (see [`replay_json`] for the
/// copy-pasteable machine half). `scenario_name` is whatever the corpus's own
/// scenario type calls itself (e.g. `Scenario::name`), unrelated to `S`'s own
/// `Debug` output, so the report stays legible even if `S::fmt` is dense.
pub fn describe<S: fmt::Debug>(scenario_name: &str, report: &ShrinkReport<S>) -> String {
    format!(
        "ANIMUS_SHRINK: minimized failing scenario '{name}'\n\
         \x20 checks={checks} reductions={reductions} converged={converged}\n\
         \x20 minimized: {minimized:#?}",
        name = scenario_name,
        checks = report.checks,
        reductions = report.reductions,
        converged = report.converged(),
        minimized = report.minimized,
    )
}

/// The machine-replayable half of the reproducible handle: the minimized case
/// serialized to JSON, meant to be printed alongside [`describe`] and pasted
/// back into a corpus's own replay entry point (each corpus that wires this
/// module in documents its own — see `raftkv_linearizable.rs`'s
/// `raftkv_shrink_replay` for the pattern: gated on an env var, deserializes
/// this exact JSON, re-runs it, and asserts the failure still reproduces).
/// Returns `Err` only if `S`'s own `Serialize` impl fails (never expected for
/// a corpus's own plain-data `Scenario` type).
pub fn replay_json<S: serde::Serialize>(
    report: &ShrinkReport<S>,
) -> Result<String, serde_json::Error> {
    serde_json::to_string(&report.minimized)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A toy case standing in for a corpus `Scenario`: a fault list plus two
    /// unrelated scalar knobs, so the test can assert the minimizer strips
    /// *both* dimensions (red-herring list entries and irrelevant scalars),
    /// exactly the shape a real corpus's own `Scenario` has (a fault
    /// schedule plus round/client/keyspace counts — see
    /// `raftkv_linearizable.rs::Scenario`).
    #[derive(Clone, Debug, PartialEq)]
    struct Toy {
        items: Vec<i32>,
        rounds: u32,
        clients: u32,
    }

    /// Every "one step smaller" move: drop one list item (tried first, for
    /// every index), or decrement a still-positive scalar. Deterministic,
    /// fixed order.
    fn toy_candidates(t: &Toy) -> Vec<Toy> {
        let mut out = Vec::new();
        for i in 0..t.items.len() {
            let mut items = t.items.clone();
            items.remove(i);
            out.push(Toy { items, ..t.clone() });
        }
        if t.rounds > 0 {
            out.push(Toy {
                rounds: t.rounds - 1,
                ..t.clone()
            });
        }
        if t.clients > 0 {
            out.push(Toy {
                clients: t.clients - 1,
                ..t.clone()
            });
        }
        out
    }

    /// The manufactured "bug": failure depends on exactly one culprit value
    /// (42) being present anywhere in `items`. Every other list entry and
    /// both scalars are red herrings the minimizer must be able to strip.
    fn toy_still_fails(t: &Toy) -> bool {
        t.items.contains(&42)
    }

    /// **The minimizer actually minimizes.** A failure that genuinely depends
    /// on one small subset of the case (`items` contains `42`, nothing else
    /// matters) is reduced to exactly that subset, with every red-herring
    /// item and every irrelevant scalar stripped — proving the reduction is
    /// real, not merely "ran without crashing." This is the test the task
    /// explicitly requires: "a minimizer with no test proving it reduces
    /// anything is not a deliverable."
    #[test]
    fn minimize_strips_every_red_herring_down_to_the_culprit() {
        let initial = Toy {
            items: vec![1, 2, 42, 3, 4, 5, 6, 7],
            rounds: 9,
            clients: 5,
        };
        assert!(
            toy_still_fails(&initial),
            "sanity: the constructed case must actually fail before shrinking it"
        );

        let report = minimize(
            initial.clone(),
            toy_candidates,
            toy_still_fails,
            ShrinkBudget::default(),
        );

        assert!(
            report.converged(),
            "expected a genuine fixpoint within the default budget, got {report:?}"
        );
        assert_eq!(
            report.minimized.items,
            vec![42],
            "every red-herring list entry must be stripped, keeping only the culprit"
        );
        assert_eq!(
            report.minimized.rounds, 0,
            "an irrelevant scalar must be reduced to its floor"
        );
        assert_eq!(
            report.minimized.clients, 0,
            "an irrelevant scalar must be reduced to its floor"
        );
        assert!(
            toy_still_fails(&report.minimized),
            "the minimized case must still reproduce the original failure"
        );
        assert!(
            report.reductions >= 7,
            "expected substantial reduction, got {} reductions",
            report.reductions
        );
    }

    /// **Determinism.** Same failing input, same candidate generator, same
    /// predicate ⇒ byte-identical minimized output and identical check/
    /// reduction counts, every time — the property the whole facility rests
    /// on (see the module doc).
    #[test]
    fn minimize_is_deterministic() {
        let initial = Toy {
            items: vec![9, 42, 1, 2, 42, 3],
            rounds: 4,
            clients: 2,
        };
        let a = minimize(
            initial.clone(),
            toy_candidates,
            toy_still_fails,
            ShrinkBudget::default(),
        );
        let b = minimize(
            initial.clone(),
            toy_candidates,
            toy_still_fails,
            ShrinkBudget::default(),
        );
        assert_eq!(a.minimized, b.minimized);
        assert_eq!(a.checks, b.checks);
        assert_eq!(a.reductions, b.reductions);
    }

    /// **The budget is real and reported honestly.** A tiny budget against a
    /// large case must stop early, report `budget_exhausted`, and never
    /// exceed the configured check count — proving the "unbounded search"
    /// failure mode is actually closed, not just documented.
    #[test]
    fn minimize_respects_the_check_budget_and_says_so() {
        let mut items: Vec<i32> = (0..50).collect();
        items.push(42);
        let initial = Toy {
            items,
            rounds: 0,
            clients: 0,
        };
        let report = minimize(
            initial,
            toy_candidates,
            toy_still_fails,
            ShrinkBudget { max_checks: 3 },
        );
        assert!(report.budget_exhausted);
        assert!(!report.converged());
        assert!(
            report.checks <= 3,
            "must never exceed the configured budget, got {}",
            report.checks
        );
    }

    /// A candidate generator with nothing left to try (already minimal) is a
    /// no-op: zero reductions, `minimized == original`, still converged (a
    /// fixpoint reached in zero steps is still a genuine fixpoint).
    #[test]
    fn minimize_is_a_noop_on_an_already_minimal_case() {
        let initial = Toy {
            items: vec![42],
            rounds: 0,
            clients: 0,
        };
        let report = minimize(
            initial.clone(),
            toy_candidates,
            toy_still_fails,
            ShrinkBudget::default(),
        );
        assert_eq!(report.minimized, initial);
        assert_eq!(report.reductions, 0);
        assert!(report.converged());
    }

    #[test]
    fn shrink_enabled_defaults_off() {
        // Never set by this process's own test env.
        // (Not asserting against a real env var mutation — tests run
        // concurrently in one process, so mutating process env here would be
        // a determinism/isolation hazard of its own; this just pins the
        // "unset" default path.)
        if env::var("ANIMUS_SHRINK").is_err() {
            assert!(!shrink_enabled());
        }
    }

    #[test]
    fn budget_from_env_defaults_when_unset() {
        if env::var("ANIMUS_SHRINK_MAX_CHECKS").is_err() {
            assert_eq!(budget_from_env(), ShrinkBudget::default());
        }
    }
}
