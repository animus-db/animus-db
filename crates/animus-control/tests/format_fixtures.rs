//! Golden fixture for the control-plane Raft WAL line format (ADR 0073
//! Phase 0 workstream B, `persist::CONTROL_WAL` — magic `CWL1`).
//!
//! One fixture per version under
//! `tests/fixtures/formats/control-wal/v<N>.bin`, decoded structurally
//! against a hand-written expected value (never just "decodes without
//! error" — the ADR 0073 Phase 0 conventions' own requirement). A round-trip
//! test separately proves encode-then-decode agrees with what this test
//! decodes the checked-in fixture into, catching an encoder/decoder
//! asymmetry a static fixture alone can't.
//!
//! **The `Metadata` inside this fixture is deliberately `Metadata::
//! default()`** — a later PR in this stack (workstream B PR 3) adds a
//! required `"v"` field to `Metadata`'s own JSON shape, which will change
//! the `Snapshot` record's payload bytes. Since a checked-in fixture may
//! never be edited once on `main` (`scripts/check-format-fixtures.sh`), but
//! this whole stack merges as one atomic `gh-stack` series, keeping the
//! embedded `Metadata` minimal here avoids that later PR needing to touch
//! (or, worse, feeling tempted to touch) this file's own committed bytes.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use animus_control::persist::{CONTROL_WAL, PersistedState, WalRecord};
use animus_control::raft::LogEntry;
use animus_control::{MetaCommand, Metadata, NodeStatus};
use animus_env::nid;

/// One of each [`WalRecord`] variant, built from fixed constants only (ADR
/// 0073 Phase 0's determinism rule — no wall-clock time, no unseeded
/// randomness, so the encoded bytes are stable across every future run).
/// Shared by the fixture generator, the decode test, and the round-trip
/// test, so all three stay in lockstep by construction.
fn v1_records() -> Vec<WalRecord<MetaCommand, Metadata>> {
    vec![
        WalRecord::Hard {
            term: 3,
            voted_for: Some(nid(1)),
        },
        WalRecord::Append(LogEntry {
            index: 1,
            term: 3,
            command: MetaCommand::UpsertMember {
                node: nid(2),
                labels: BTreeMap::new(),
                status: NodeStatus::Active,
            },
            config: None,
            learners: None,
        }),
        WalRecord::Truncate { keep: 1 },
        WalRecord::Snapshot {
            // Deliberately minimal — see this file's own module doc for why.
            metadata: Metadata::default(),
            last_index: 5,
            last_term: 3,
            config: Some(BTreeSet::from([nid(1), nid(2)])),
            learners: Some(BTreeSet::from([nid(3)])),
        },
    ]
}

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/formats/control-wal")
}

fn encode_all(records: &[WalRecord<MetaCommand, Metadata>]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for record in records {
        bytes.extend(PersistedState::<MetaCommand, Metadata>::encode_record(
            record,
        ));
    }
    bytes
}

/// Iterates every file under `tests/fixtures/formats/control-wal/` (never
/// naming `v1` literally, per the ADR 0073 Phase 0 conventions — a future
/// version's own fixture needs no test-code change) and asserts each
/// decodes, structurally, to the exact expected value for its version.
#[test]
fn decodes_every_checked_in_fixture_structurally() {
    let dir = fixtures_dir();
    let mut checked = 0usize;
    let entries = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("reading fixtures dir {}: {e}", dir.display()));
    for entry in entries {
        let entry = entry.expect("readable dir entry");
        let path = entry.path();
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_string();
        let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("reading {name}: {e}"));
        match name.as_str() {
            "v1.bin" => {
                let decoded = PersistedState::<MetaCommand, Metadata>::decode(&bytes)
                    .unwrap_or_else(|e| panic!("{name} failed to decode: {e}"));
                assert_eq!(
                    decoded,
                    v1_records(),
                    "{name} decoded to an unexpected value"
                );
                checked += 1;
            }
            other => panic!(
                "unrecognized control-wal fixture {other:?} — add a matching expected-value \
                 arm to this test before adding the fixture file"
            ),
        }
    }
    assert!(
        checked > 0,
        "no control-wal fixtures found under {}",
        dir.display()
    );
}

/// Encoding the same representative records with the *current* code and
/// decoding them back must reproduce the originals exactly — catches an
/// encoder/decoder asymmetry a static fixture decode alone can't.
#[test]
fn round_trips_through_encode_and_decode() {
    let records = v1_records();
    let bytes = encode_all(&records);
    let decoded = PersistedState::<MetaCommand, Metadata>::decode(&bytes).expect("decodes");
    assert_eq!(decoded, records);
}

/// The fixture's first line carries [`CONTROL_WAL`]'s own magic/version —
/// a cheap, direct sanity check independent of the structural decode above.
/// A line is `<crc32 8 hex>:<magic 4><version 2 hex><payload>\n`, so the
/// tag sits right after the 8-hex-digit checksum and its colon.
#[test]
fn fixture_starts_with_the_control_wal_tag() {
    let bytes = std::fs::read(fixtures_dir().join("v1.bin")).expect("v1.bin fixture is checked in");
    assert_eq!(&bytes[9..13], &CONTROL_WAL.magic);
    assert_eq!(
        &bytes[13..15],
        format!("{:02x}", CONTROL_WAL.version).as_bytes()
    );
}

/// Regenerates `v<CONTROL_WAL.version>.bin` from [`v1_records`] with the
/// *current* encoder. Run explicitly, never part of the default test run:
/// `cargo test -p animus-control --test format_fixtures generate_fixture_control_wal -- --ignored`.
///
/// Refuses to overwrite a fixture that already exists (ADR 0073 Phase 0
/// conventions) — bump [`CONTROL_WAL`]'s version and add a new file instead
/// of regenerating an existing one.
#[test]
#[ignore]
fn generate_fixture_control_wal() {
    let dir = fixtures_dir();
    std::fs::create_dir_all(&dir).expect("create fixtures dir");
    let path = dir.join(format!("v{}.bin", CONTROL_WAL.version));
    if std::fs::metadata(&path).is_ok() {
        panic!(
            "{} already exists — a checked-in fixture is never regenerated in place; \
             bump CONTROL_WAL's version and add a new fixture file instead",
            path.display()
        );
    }
    let bytes = encode_all(&v1_records());
    std::fs::write(&path, &bytes).unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
}
