//! **Elle against Accord under contention** (deliverable 2 of the
//! consistency-testing milestone).
//!
//! Unlike the disjoint-key data-plane workload in `end_to_end.rs` (near-zero
//! contention, where the serializability checker can never form a cycle), this
//! points the checker at the layer that *claims* a consistent serialization
//! order — Accord — and drives **concurrent, conflicting multi-key transactions**
//! through it, then runs `check_cycles`/`check_durability`/`check_convergence`
//! over the recorded history.
//!
//! The conflicts are real: a small shared key space (4 keys) with several clients
//! each issuing 2-key read/write transactions means concurrent transactions
//! routinely overlap — the regime where a serializability bug would surface as a
//! dependency cycle. A correct Accord run must show **no cycle**.
//!
//! See `support/mod.rs` for the list-append-over-Accord modelling. The whole run
//! is a pure function of its seed.

mod support;

use std::time::Duration;

use support::{ClusterShape, Scenario, WorkloadSpec, run_scenario};

/// A no-fault, high-contention run: the checker must accept it (no cycle), and
/// the run must be non-vacuous — genuine contention (a key written ≥ 2 times)
/// and a healthy number of acknowledged writes, so the green verdict means the
/// checker actually had conflicting transactions to chew on.
#[test]
fn accord_serializable_under_contention() {
    let seed = 0xE11E_ACC0;
    let scenario = Scenario {
        name: "elle_accord_contended".into(),
        seed,
        cluster: ClusterShape::SMALL,
        workload: WorkloadSpec::CONTENDED,
        faults: Vec::new(),
    };
    let result = run_scenario(&scenario);

    assert!(
        result.cycles.ok,
        "Accord produced a serializability cycle under contention \
         (seed={seed}): {:?}",
        result.cycles.violations
    );
    assert!(
        result.convergence.ok,
        "final reads did not converge (seed={seed}): {:?}",
        result.convergence.violations
    );
    assert!(
        result.durability.ok,
        "an acknowledged write was lost (seed={seed}): {:?}",
        result.durability.violations
    );

    // Teeth guard: the run must have genuinely contended, or a green cycle check
    // is meaningless.
    assert!(
        result.ok_writes >= 8,
        "near-vacuous run: only {} acknowledged writes (seed={seed})",
        result.ok_writes
    );
    assert!(
        result.contended,
        "no key received ≥ 2 acknowledged writes — workload did not contend \
         (seed={seed})"
    );
    assert!(
        result.nonempty_reads >= 1,
        "no read ever observed a non-empty list — the checker saw no wr/rw \
         edges (seed={seed})"
    );
}

/// The Accord-contention run is consistent across a seed sweep: different
/// message-delay / arrival interleavings must all be serializable.
#[test]
fn accord_serializable_across_seeds() {
    for seed in 0xE11E_1000..0xE11E_1008 {
        let scenario = Scenario {
            name: format!("elle_accord_seed_{seed:x}"),
            seed,
            cluster: ClusterShape::SMALL,
            workload: WorkloadSpec::CONTENDED,
            faults: Vec::new(),
        };
        let result = run_scenario(&scenario);
        assert!(
            result.cycles.ok,
            "serializability cycle (seed={seed}): {:?}",
            result.cycles.violations
        );
        assert!(
            result.convergence.ok,
            "non-convergence (seed={seed}): {:?}",
            result.convergence.violations
        );
    }
}

/// The whole harness run is byte-reproducible from its seed (ADR 0003): two runs
/// at the same seed record identical histories.
#[test]
fn accord_run_is_deterministic_from_seed() {
    let scenario = || Scenario {
        name: "elle_accord_determinism".into(),
        seed: 0xE11E_DE77,
        cluster: ClusterShape::SMALL,
        workload: WorkloadSpec {
            clients: 3,
            rounds: 4,
            keyspace: 3,
            keys_per_txn: 2,
            read_pct: 40,
        },
        faults: vec![(Duration::from_secs(2), support::NemesisAction::Lossy)],
    };
    let a = run_scenario(&scenario());
    let b = run_scenario(&scenario());
    assert_eq!(
        a.history.entries, b.history.entries,
        "same seed must record an identical history"
    );
}
