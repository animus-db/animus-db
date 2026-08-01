//! M5 (transactional path): the dependency-graph cycle checker passes on a
//! serializable history and flags a non-serializable one (write skew, G2),
//! reporting the offending transactions and the replay seed.

use custos_test::history::Mop;
use custos_test::{Recorder, check_cycles, export};

fn append(key: u64, value: u64) -> Mop {
    Mop::Append { key, value }
}
fn read(key: u64, observed: &[u64]) -> Mop {
    Mop::Read {
        key,
        observed: Some(observed.to_vec()),
    }
}

#[test]
fn serializable_history_passes() {
    // A runs first (sees y empty), then B (sees x = [1]). A → B only.
    let mut rec = Recorder::new(1);
    rec.invoke(0, 10, vec![append(0, 1), read(1, &[])]);
    rec.ok(0, 11, vec![append(0, 1), read(1, &[])]);
    rec.invoke(1, 20, vec![append(1, 2), read(0, &[1])]);
    rec.ok(1, 21, vec![append(1, 2), read(0, &[1])]);

    let report = check_cycles(rec.history());
    assert!(
        report.ok,
        "serializable history flagged: {:?}",
        report.violations
    );
}

#[test]
fn write_skew_cycle_is_flagged() {
    // Classic write skew: each transaction reads the other's key as empty and
    // writes its own. The two anti-dependencies form a cycle (Adya G2).
    let seed = 0x6_2A2;
    let mut rec = Recorder::new(seed);
    rec.invoke(0, 10, vec![append(0, 1), read(1, &[])]);
    rec.ok(0, 12, vec![append(0, 1), read(1, &[])]);
    rec.invoke(1, 11, vec![append(1, 2), read(0, &[])]);
    rec.ok(1, 13, vec![append(1, 2), read(0, &[])]);

    let report = check_cycles(rec.history());
    assert!(!report.ok, "write-skew cycle not detected");
    assert!(
        report.violations.iter().any(|v| v.contains("cycle")),
        "{:?}",
        report.violations
    );
    assert_eq!(report.seed, seed, "report should carry the replay seed");
}

#[test]
fn divergent_reads_are_flagged() {
    // Two reads of the same key observe incompatible lists ([1,2] vs [1,3]):
    // neither is a prefix of the other, so the value order cannot be recovered.
    let mut rec = Recorder::new(2);
    rec.ok(0, 1, vec![append(0, 1), append(0, 2), append(0, 3)]);
    rec.ok(1, 2, vec![read(0, &[1, 2])]);
    rec.ok(2, 3, vec![read(0, &[1, 3])]);

    let report = check_cycles(rec.history());
    assert!(!report.ok, "divergent reads should be flagged");
    assert!(
        report.violations.iter().any(|v| v.contains("divergent")),
        "{:?}",
        report.violations
    );
}

#[test]
fn edn_and_json_export_round_trip_shape() {
    let mut rec = Recorder::new(7);
    rec.invoke(
        0,
        5,
        vec![
            append(3, 9),
            Mop::Read {
                key: 3,
                observed: None,
            },
        ],
    );
    rec.ok(0, 6, vec![append(3, 9), read(3, &[9])]);

    let edn = export::to_edn(rec.history());
    assert!(edn.contains(":process 0"), "{edn}");
    assert!(edn.contains("[:append 3 9]"), "{edn}");
    assert!(edn.contains("[:r 3 nil]"), "{edn}");
    assert!(edn.contains("[:r 3 [9]]"), "{edn}");

    let json = export::to_json(rec.history());
    assert!(json.contains("\"seed\": 7"), "{json}");
    assert!(json.contains("Append"), "{json}");
}
