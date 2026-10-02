//! **Negative control for the serializability checker** (deliverable 1 of the
//! consistency-testing milestone).
//!
//! A green corpus run is only meaningful if the checker can actually *reject* a
//! non-serializable history. These tests feed [`check_cycles`] hand-constructed
//! histories whose serializability status we know by construction, and assert
//! the verdict matches:
//!
//! - **Must reject** (cycle present): classic write skew (a rw/rw `G2`
//!   anti-dependency cycle), a `G1c` circular information flow (each transaction
//!   reads the other's write — wr/wr cycle), and a longer three-transaction
//!   cycle (so detection is not specialised to length-2).
//! - **Must accept** (serializable): a strict linear order, a read-only history,
//!   and a fan-out (one writer, many independent readers).
//!
//! This is the "does the smoke detector beep when there's smoke?" check. It
//! complements the existing `cycle_checker.rs` (which also covers export shape +
//! divergent reads) by being a focused, exhaustive teeth-proof for the corpus
//! milestone. Pure in-memory histories — no simulator, no seed dependence.

use animus_test::history::Mop;
use animus_test::{Recorder, check_cycles};

fn append(key: u64, value: u64) -> Mop {
    Mop::Append { key, value }
}
fn read(key: u64, observed: &[u64]) -> Mop {
    Mop::Read {
        key,
        observed: Some(observed.to_vec()),
    }
}

/// Record one transaction as invoke-then-ok at distinct times. The `process` and
/// `time` are bookkeeping; the checker reasons about `mops` order, not wall time.
fn txn(rec: &mut Recorder, process: u64, t: u64, mops: Vec<Mop>) {
    rec.invoke(process, t, mops.clone());
    rec.ok(process, t + 1, mops);
}

// ---------------------------------------------------------------------------
// Histories that MUST be rejected (a serializability cycle is present).
// ---------------------------------------------------------------------------

/// Write skew (`G2`): T0 reads key 1 (empty) and writes key 0; T1 reads key 0
/// (empty) and writes key 1. Each read precedes the other's write, so the two
/// `rw` anti-dependencies form a 2-cycle. No serial order explains both reads
/// observing the pre-state.
#[test]
fn write_skew_g2_is_rejected() {
    let seed = 0xDEAD_0001;
    let mut rec = Recorder::new(seed);
    txn(&mut rec, 0, 10, vec![read(1, &[]), append(0, 1)]);
    txn(&mut rec, 1, 20, vec![read(0, &[]), append(1, 2)]);

    let report = check_cycles(rec.history());
    assert!(
        !report.ok,
        "checker missed a write-skew G2 cycle (seed={seed})"
    );
    assert!(
        report.violations.iter().any(|v| v.contains("cycle")),
        "expected a cycle violation, got {:?} (seed={seed})",
        report.violations
    );
    assert_eq!(report.seed, seed, "report must carry the replay seed");
}

/// Circular information flow (`G1c`): T0 appends 1 to key 0 and reads key 1
/// observing T1's value; T1 appends 2 to key 1 and reads key 0 observing T0's
/// value. Each transaction read-depends (`wr`) on the other, so they cannot be
/// placed in any serial order — a 2-cycle of `wr` edges.
#[test]
fn circular_read_dependency_g1c_is_rejected() {
    let seed = 0xDEAD_0002;
    let mut rec = Recorder::new(seed);
    txn(&mut rec, 0, 10, vec![append(0, 1), read(1, &[2])]);
    txn(&mut rec, 1, 20, vec![append(1, 2), read(0, &[1])]);

    let report = check_cycles(rec.history());
    assert!(
        !report.ok,
        "checker missed a G1c circular read dependency (seed={seed}): {:?}",
        report.violations
    );
    assert!(
        report.violations.iter().any(|v| v.contains("cycle")),
        "expected a cycle violation, got {:?} (seed={seed})",
        report.violations
    );
}

/// A three-transaction cycle, so detection is not specialised to length-2.
/// T0 reads key 1 empty + writes key 0; T1 reads key 2 empty + writes key 1;
/// T2 reads key 0 empty + writes key 2. The three `rw` anti-dependencies chain
/// T0→T1→T2→T0.
#[test]
fn three_transaction_cycle_is_rejected() {
    let seed = 0xDEAD_0003;
    let mut rec = Recorder::new(seed);
    txn(&mut rec, 0, 10, vec![read(1, &[]), append(0, 10)]);
    txn(&mut rec, 1, 20, vec![read(2, &[]), append(1, 20)]);
    txn(&mut rec, 2, 30, vec![read(0, &[]), append(2, 30)]);

    let report = check_cycles(rec.history());
    assert!(
        !report.ok,
        "checker missed a 3-transaction cycle (seed={seed}): {:?}",
        report.violations
    );
    // The cycle description should name all three transactions.
    assert!(
        report
            .violations
            .iter()
            .any(|v| v.contains("cycle") && v.contains('0') && v.contains('1') && v.contains('2')),
        "expected a 3-txn cycle, got {:?} (seed={seed})",
        report.violations
    );
}

// ---------------------------------------------------------------------------
// Histories that MUST be accepted (a valid serial order exists).
// ---------------------------------------------------------------------------

/// A strict linear order: T0 writes key 0; T1 reads key 0 (sees T0) and writes
/// key 1; T2 reads both (sees both). The only dependencies are forward
/// (T0→T1→T2), so the graph is a DAG — serializable.
#[test]
fn strict_linear_order_is_accepted() {
    let mut rec = Recorder::new(1);
    txn(&mut rec, 0, 10, vec![append(0, 1)]);
    txn(&mut rec, 1, 20, vec![read(0, &[1]), append(1, 2)]);
    txn(&mut rec, 2, 30, vec![read(0, &[1]), read(1, &[2])]);

    let report = check_cycles(rec.history());
    assert!(
        report.ok,
        "a strictly-ordered serializable history was flagged: {:?}",
        report.violations
    );
}

/// A read-only history (no appends) can never cycle: there are no `ww`/`wr`/`rw`
/// edges with a write endpoint, so it is trivially serializable.
#[test]
fn read_only_history_is_accepted() {
    let mut rec = Recorder::new(2);
    txn(&mut rec, 0, 10, vec![read(0, &[]), read(1, &[])]);
    txn(&mut rec, 1, 20, vec![read(0, &[]), read(1, &[])]);

    let report = check_cycles(rec.history());
    assert!(
        report.ok,
        "a read-only history was flagged: {:?}",
        report.violations
    );
}

/// Fan-out: one writer, several independent readers that all observe the write.
/// Every reader read-depends on the writer (`wr`), but no reader writes, so no
/// edge ever points back to the writer — a star DAG, serializable.
#[test]
fn writer_with_many_readers_is_accepted() {
    let mut rec = Recorder::new(3);
    txn(&mut rec, 0, 10, vec![append(0, 1)]);
    for p in 1..=4 {
        txn(&mut rec, p, 10 + p * 10, vec![read(0, &[1])]);
    }
    let report = check_cycles(rec.history());
    assert!(
        report.ok,
        "a writer/many-readers fan-out was flagged: {:?}",
        report.violations
    );
}

/// The checker is **stable**: re-running it on the same history yields the same
/// verdict and the same violation set (Tarjan over `BTreeMap`/`BTreeSet` is
/// deterministic). A flaky verdict would make the corpus runner untrustworthy.
#[test]
fn verdict_is_stable_across_runs() {
    let mut rec = Recorder::new(0xDEAD_0004);
    txn(&mut rec, 0, 10, vec![read(1, &[]), append(0, 1)]);
    txn(&mut rec, 1, 20, vec![read(0, &[]), append(1, 2)]);
    let h = rec.history();
    let a = check_cycles(h);
    let b = check_cycles(h);
    assert_eq!(a.ok, b.ok, "verdict not stable");
    assert_eq!(a.violations, b.violations, "violation set not stable");
}
