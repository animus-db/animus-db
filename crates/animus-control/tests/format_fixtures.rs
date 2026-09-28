//! Golden fixtures for the control plane's ADR 0073 Phase 0 workstream B
//! formats: the Raft WAL line envelope (`persist::CONTROL_WAL`, magic
//! `CWL1`) and the snapshot/`InstallSnapshot` payload envelope
//! (`persist::CONTROL_SNAPSHOT`, magic `CSN1`).
//!
//! One fixture per version under
//! `tests/fixtures/formats/<format>/v<N>.bin`, decoded structurally
//! against a hand-written expected value (never just "decodes without
//! error" — the ADR 0073 Phase 0 conventions' own requirement). A round-trip
//! test separately proves encode-then-decode agrees with what this test
//! decodes the checked-in fixture into, catching an encoder/decoder
//! asymmetry a static fixture alone can't.
//!
//! **The `Metadata` inside the `control-wal` fixture is deliberately
//! `Metadata::default()`** — a later PR in this stack (workstream B PR 3)
//! adds a required `"v"` field to `Metadata`'s own JSON shape, which will
//! change the `Snapshot` record's payload bytes. Since a checked-in fixture
//! may never be edited once on `main` (`scripts/check-format-fixtures.sh`),
//! but this whole stack merges as one atomic `gh-stack` series, keeping the
//! embedded `Metadata` minimal here avoids that later PR needing to touch
//! (or, worse, feeling tempted to touch) this file's own committed bytes.
//!
//! **The `control-snapshot` fixture wraps a system-keyspace image
//! directly** (`node::encode_syskv_image_bytes`'s `Vec<(key,
//! value-or-tombstone, version)>` shape) rather than a `Metadata` value —
//! this is the real, `DRIVER_APPLIED` control plane's actual
//! `InstallSnapshot` transfer payload (`crate::node`'s own doc), never a
//! serialized `Metadata` blob. It is entirely unaffected by `Metadata`'s
//! future `"v"` field for the same reason.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use animus_control::node::{decode_syskv_image_bytes, encode_syskv_image_bytes};
use animus_control::persist::{CONTROL_SNAPSHOT, CONTROL_WAL, PersistedState, WalRecord};
use animus_control::raft::LogEntry;
use animus_control::{MetaCommand, Metadata, NodeStatus};
use animus_env::nid;
use animus_tablet::TabletId;

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

// ---------------------------------------------------------------------------
// `control-snapshot` (`CONTROL_SNAPSHOT`, magic `CSN1`) — the system-keyspace
// `InstallSnapshot` payload envelope (ADR 0073 Phase 0 workstream B).

/// A small, deterministic system-keyspace image: fixed keys (built from the
/// real `syskv` key helpers, not hand-rolled bytes, so this fixture stays
/// honest about the real key shape), fixed values, fixed versions, and one
/// tombstone (`None` value) — the shape [`decode_syskv_image_bytes`]'s own
/// doc says a receiver must handle. Shared by the fixture generator, the
/// decode test, and the round-trip test, so all three stay in lockstep by
/// construction.
fn v1_syskv_entries() -> Vec<(Vec<u8>, Option<Vec<u8>>, u64)> {
    vec![
        (
            animus_control::syskv::tablet_key(TabletId(1)),
            Some(b"{\"tablet\":1}".to_vec()),
            3,
        ),
        (
            animus_control::syskv::member_key(&nid(2)),
            Some(b"{\"status\":\"Active\"}".to_vec()),
            5,
        ),
        (
            animus_control::syskv::applied_index_key(),
            Some(7u64.to_be_bytes().to_vec()),
            1,
        ),
        // A tombstone: a schema row that was since dropped.
        (animus_control::syskv::schema_key("orders"), None, 2),
    ]
}

fn control_snapshot_fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/formats/control-snapshot")
}

/// Iterates every file under `tests/fixtures/formats/control-snapshot/`
/// (never naming `v1` literally, per the ADR 0073 Phase 0 conventions — a
/// future version's own fixture needs no test-code change) and asserts each
/// decodes, structurally, to the exact expected value for its version.
#[test]
fn decodes_every_checked_in_control_snapshot_fixture_structurally() {
    let dir = control_snapshot_fixtures_dir();
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
                let decoded = decode_syskv_image_bytes(&bytes)
                    .unwrap_or_else(|e| panic!("{name} failed to decode: {e}"));
                assert_eq!(
                    decoded,
                    v1_syskv_entries(),
                    "{name} decoded to an unexpected value"
                );
                checked += 1;
            }
            other => panic!(
                "unrecognized control-snapshot fixture {other:?} — add a matching expected-value \
                 arm to this test before adding the fixture file"
            ),
        }
    }
    assert!(
        checked > 0,
        "no control-snapshot fixtures found under {}",
        dir.display()
    );
}

/// Encoding the same representative entries with the *current* code and
/// decoding them back must reproduce the originals exactly — catches an
/// encoder/decoder asymmetry a static fixture decode alone can't.
#[test]
fn control_snapshot_round_trips_through_encode_and_decode() {
    let entries = v1_syskv_entries();
    let bytes = encode_syskv_image_bytes(&entries);
    let decoded = decode_syskv_image_bytes(&bytes).expect("decodes");
    assert_eq!(decoded, entries);
}

/// The fixture's own header carries [`CONTROL_SNAPSHOT`]'s magic/version —
/// a cheap, direct sanity check independent of the structural decode above.
/// [`format::wrap`](animus_control::format::wrap)'s binary envelope is
/// `magic(4) || version(u8) || payload`, with no line/checksum framing in
/// front of it (unlike `control-wal`'s line shape).
#[test]
fn control_snapshot_fixture_starts_with_the_control_snapshot_tag() {
    let bytes = std::fs::read(control_snapshot_fixtures_dir().join("v1.bin"))
        .expect("v1.bin fixture is checked in");
    assert_eq!(&bytes[..4], &CONTROL_SNAPSHOT.magic);
    assert_eq!(bytes[4], CONTROL_SNAPSHOT.version);
}

/// Regenerates `v<CONTROL_SNAPSHOT.version>.bin` from [`v1_syskv_entries`]
/// with the *current* encoder. Run explicitly, never part of the default
/// test run: `cargo test -p animus-control --test format_fixtures
/// generate_fixture_control_snapshot -- --ignored`.
///
/// Refuses to overwrite a fixture that already exists (ADR 0073 Phase 0
/// conventions) — bump [`CONTROL_SNAPSHOT`]'s version and add a new file
/// instead of regenerating an existing one.
#[test]
#[ignore]
fn generate_fixture_control_snapshot() {
    let dir = control_snapshot_fixtures_dir();
    std::fs::create_dir_all(&dir).expect("create fixtures dir");
    let path = dir.join(format!("v{}.bin", CONTROL_SNAPSHOT.version));
    if std::fs::metadata(&path).is_ok() {
        panic!(
            "{} already exists — a checked-in fixture is never regenerated in place; \
             bump CONTROL_SNAPSHOT's version and add a new fixture file instead",
            path.display()
        );
    }
    let bytes = encode_syskv_image_bytes(&v1_syskv_entries());
    std::fs::write(&path, &bytes).unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
}
