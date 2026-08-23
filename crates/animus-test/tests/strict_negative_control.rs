//! **Negative control for the strict (linearizability) checker.**
//!
//! The sibling `negative_control.rs` proves [`check_cycles`] has teeth for
//! *serializability*. This file proves [`check_strict_cycles`] has teeth for the
//! strictly stronger property the CP data plane actually claims — linearizable
//! single-tablet reads (ADR 0016/0017 §3) — and, just as importantly, that it
//! does **not** reject histories linearizability permits.
//!
//! The headline pair is [`stale_prefix_read_is_invisible_to_check_cycles`] and
//! [`stale_prefix_read_is_rejected_by_the_strict_check`]: one history, two
//! verdicts. It is the exact anomaly a deposed leader serving a read without a
//! valid ReadIndex barrier produces, and it documents by construction why the
//! plain checker cannot be the oracle for a linearizable plane.
//!
//! Pure in-memory histories — no simulator, no seed dependence.

use animus_test::history::Mop;
use animus_test::{Recorder, check_cycles, check_strict_cycles, realtime_edge_count};

fn append(key: u64, value: u64) -> Mop {
    Mop::Append { key, value }
}
fn read(key: u64, observed: &[u64]) -> Mop {
    Mop::Read {
        key,
        observed: Some(observed.to_vec()),
    }
}

/// Record one transaction with an explicit real-time span `[invoke, complete]`.
/// Unlike `negative_control.rs`'s helper, the span is the point of these tests,
/// so both ends are caller-chosen.
fn span(rec: &mut Recorder, process: u64, invoke: u64, complete: u64, mops: Vec<Mop>) {
    rec.invoke(process, invoke, mops.clone());
    rec.ok(process, complete, mops);
}

/// The shared anomaly: p0's append is acknowledged and then *observed* by p1,
/// after which p2 — invoked strictly later than both completed — reads an empty
/// list. No linearization exists: a completed operation precedes one invoked
/// after it returned, so p2's read cannot be ordered before p0's append.
fn stale_prefix_history() -> animus_test::History {
    let mut rec = Recorder::new(0x5747_0001);
    span(&mut rec, 0, 10, 100, vec![append(0, 1)]);
    span(&mut rec, 1, 150, 200, vec![read(0, &[1])]);
    span(&mut rec, 2, 250, 300, vec![read(0, &[])]);
    // Ordinary traffic continues, so the history is not degenerate.
    span(&mut rec, 0, 350, 400, vec![append(0, 2)]);
    span(&mut rec, 1, 450, 500, vec![read(0, &[1, 2])]);
    rec.into_history()
}

// ---------------------------------------------------------------------------
// The gap, pinned from both sides.
// ---------------------------------------------------------------------------

/// Documents the limitation rather than asserting a bug: the stale read is
/// perfectly *serializable* (order it before the append), so the data-dependency
/// checker accepts it. If this ever starts failing, `check_cycles` grew real-time
/// awareness and this file's premise needs revisiting.
#[test]
fn stale_prefix_read_is_invisible_to_check_cycles() {
    let h = stale_prefix_history();
    let report = check_cycles(&h);
    assert!(
        report.ok,
        "premise changed: check_cycles now rejects a stale-prefix read ({:?})",
        report.violations
    );
}

/// The teeth-proof: with real-time edges the same history is rejected.
#[test]
fn stale_prefix_read_is_rejected_by_the_strict_check() {
    let h = stale_prefix_history();
    let report = check_strict_cycles(&h);
    assert!(
        !report.ok,
        "strict checker missed a non-linearizable stale read (seed={})",
        h.seed
    );
}

/// The read-vs-read shape of the same defect: p1 observes the append, then p2 —
/// invoked after p1 completed — observes less. Non-monotonic across processes,
/// with no append involved in the violation itself.
#[test]
fn a_read_that_goes_backwards_is_rejected_by_the_strict_check() {
    let mut rec = Recorder::new(0x5747_0002);
    span(&mut rec, 0, 10, 100, vec![append(0, 1)]);
    span(&mut rec, 1, 150, 200, vec![read(0, &[1])]);
    span(&mut rec, 2, 250, 300, vec![read(0, &[])]);
    let h = rec.into_history();

    assert!(check_cycles(&h).ok, "premise: serializable");
    assert!(
        !check_strict_cycles(&h).ok,
        "strict checker missed a backwards read"
    );
}

// ---------------------------------------------------------------------------
// Histories linearizability PERMITS — the strict check must not reject these.
// ---------------------------------------------------------------------------

/// A read that **overlaps** the append in real time may legally miss it: the two
/// are concurrent, so linearizability lets the read be ordered first. This is the
/// guard against the strict check degenerating into "any stale read is a bug".
#[test]
fn a_read_concurrent_with_the_append_may_miss_it() {
    let mut rec = Recorder::new(0x5747_0003);
    // The append is in flight from t=10 to t=100 ...
    span(&mut rec, 0, 10, 100, vec![append(0, 1)]);
    // ... and this read runs entirely inside that window, observing nothing.
    span(&mut rec, 1, 50, 60, vec![read(0, &[])]);
    // A later, non-overlapping read sees it.
    span(&mut rec, 2, 200, 210, vec![read(0, &[1])]);
    let h = rec.into_history();

    let report = check_strict_cycles(&h);
    assert!(
        report.ok,
        "strict checker rejected a legal concurrent-window read: {:?}",
        report.violations
    );
}

/// A plainly linearizable sequential history stays accepted.
#[test]
fn a_sequential_linearizable_history_is_accepted() {
    let mut rec = Recorder::new(0x5747_0004);
    span(&mut rec, 0, 10, 20, vec![append(0, 1)]);
    span(&mut rec, 1, 30, 40, vec![read(0, &[1])]);
    span(&mut rec, 0, 50, 60, vec![append(0, 2)]);
    span(&mut rec, 1, 70, 80, vec![read(0, &[1, 2])]);
    span(&mut rec, 2, 90, 100, vec![read(0, &[1, 2])]);
    let h = rec.into_history();

    let report = check_strict_cycles(&h);
    assert!(
        report.ok,
        "strict checker rejected a linearizable history: {:?}",
        report.violations
    );
}

/// Every history the plain checker rejects must still be rejected once real-time
/// edges are added — the strict check is a strengthening, never a replacement.
#[test]
fn strict_still_rejects_a_plain_serializability_cycle() {
    // Write skew: each transaction reads the key the other writes.
    let mut rec = Recorder::new(0x5747_0005);
    span(&mut rec, 0, 10, 20, vec![read(1, &[]), append(0, 1)]);
    span(&mut rec, 1, 11, 21, vec![read(0, &[]), append(1, 2)]);
    let h = rec.into_history();

    assert!(!check_cycles(&h).ok, "premise: a G2 cycle is present");
    assert!(
        !check_strict_cycles(&h).ok,
        "strict checker lost a cycle the plain checker finds"
    );
}

// ---------------------------------------------------------------------------
// Non-vacuity of the added edges.
// ---------------------------------------------------------------------------

/// The strict check only has teeth where operations do not overlap. A corpus
/// must therefore be able to *measure* how many real-time edges its histories
/// admit, or a green strict run proves nothing beyond a green plain run.
#[test]
fn realtime_edge_count_measures_what_the_strict_check_adds() {
    // Five strictly sequential ops: every earlier one precedes every later one.
    let h = stale_prefix_history();
    assert_eq!(
        realtime_edge_count(&h),
        10,
        "5 sequential ops should admit C(5,2) = 10 real-time edges"
    );

    // Fully overlapping ops admit none, so the strict check adds nothing.
    let mut rec = Recorder::new(0x5747_0006);
    span(&mut rec, 0, 10, 100, vec![append(0, 1)]);
    span(&mut rec, 1, 20, 110, vec![read(0, &[])]);
    span(&mut rec, 2, 30, 120, vec![read(0, &[])]);
    let overlapping = rec.into_history();
    assert_eq!(
        realtime_edge_count(&overlapping),
        0,
        "mutually overlapping ops are unordered by real time"
    );
}
