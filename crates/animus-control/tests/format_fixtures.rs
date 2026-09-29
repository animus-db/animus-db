//! Golden fixtures for the control plane's ADR 0073 Phase 0 workstream B
//! formats: the Raft WAL line envelope (`persist::CONTROL_WAL`, magic
//! `CWL1`) and the snapshot/`InstallSnapshot` payload envelope
//! (`persist::CONTROL_SNAPSHOT`, magic `CSN1`); plus, from workstream C, the
//! `SharedWal` outer line envelope (`persist::SHARED_WAL_TAG`, magic `SWL1`,
//! fixture dir `shared-wal`) — it lives in this crate, so its fixture does too.
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

//!
//! **`metadata` (`Metadata`'s own `"v"` field) is the third format this file
//! covers, added by workstream B PR 3** — a `serde_json` "top-level `\"v\"`
//! field" format per the ADR's Phase 0 conventions, not a binary/line
//! envelope, so it has no `format::wrap`/`encode_line` tag to check; its own
//! decode path is [`Metadata::from_json`]. **No separate `syskv-mirror`
//! fixture exists** — the system-keyspace mirror's own format-version row
//! (`mirror::SYSKV_FORMAT_VERSION_COUNTER`) is mirror-internal bookkeeping
//! that never rides a `Metadata` value at all (`Metadata` is `DRIVER_APPLIED`
//! — see `crates/animus-control/CLAUDE.md`'s "Versioned formats" section),
//! and it already has direct unit-test coverage in `mirror.rs`'s own test
//! module; the `control-snapshot` fixture above already covers a real
//! system-keyspace image's on-the-wire bytes structurally, which is as close
//! as that mirror-internal row gets to a "fixture" of its own.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use animus_control::node::{decode_syskv_image_bytes, encode_syskv_image_bytes};
use animus_control::persist::{
    CONTROL_SNAPSHOT, CONTROL_WAL, PersistedState, SHARED_WAL_TAG, WalRecord,
};
use animus_control::raft::LogEntry;
use animus_control::schema::{ColumnType, TableSchema};
use animus_control::{ApplyOutcome, MetaCommand, Metadata, NodeStatus, PlacementPolicy};
use animus_env::nid;
use animus_tablet::{KeyRange, TabletId};

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

// ---------------------------------------------------------------------------
// `metadata` (`Metadata`'s own top-level `"v"` field, ADR 0073 Phase 0
// workstream B PR 3) — the `serde_json` "Phase 0 conventions" shape, decoded
// via [`Metadata::from_json`].

/// A deterministic, non-trivial [`Metadata`] built by applying real
/// [`MetaCommand`]s through [`Metadata::apply`] — never hand-constructed
/// field-by-field, which could silently drift from what `apply` actually
/// produces. Two members, one tablet with a placement policy, and one table
/// schema; fixed constants only (ADR 0073 Phase 0's determinism rule).
/// Shared by the fixture generator, the decode test, and the round-trip
/// test, so all three stay in lockstep by construction.
fn v1_metadata() -> Metadata {
    let mut m = Metadata::default();
    let commands = [
        MetaCommand::UpsertMember {
            node: nid(1),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        },
        MetaCommand::UpsertMember {
            node: nid(2),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        },
        MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("orders".to_string()),
            range: KeyRange::whole(),
            replicas: vec![nid(1), nid(2)],
        },
        MetaCommand::SetTabletPolicy {
            tablet: TabletId(1),
            policy: Some(PlacementPolicy::simple("p", 2)),
        },
        MetaCommand::CreateTableSchema {
            table: "orders".to_string(),
            schema: TableSchema::simple("id", ColumnType::String),
        },
    ];
    for command in &commands {
        assert_eq!(
            m.apply(command),
            ApplyOutcome::Applied,
            "fixture premise: every command applies cleanly"
        );
    }
    m
}

fn metadata_fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/formats/metadata")
}

/// Iterates every file under `tests/fixtures/formats/metadata/` (never
/// naming `v1` literally, per the ADR 0073 Phase 0 conventions — a future
/// version's own fixture needs no test-code change) and asserts each
/// decodes, structurally, to the exact expected value for its version.
#[test]
fn decodes_every_checked_in_metadata_fixture_structurally() {
    let dir = metadata_fixtures_dir();
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
            "v1.json" => {
                let decoded = Metadata::from_json(&bytes)
                    .unwrap_or_else(|e| panic!("{name} failed to decode: {e}"));
                assert_eq!(
                    decoded,
                    v1_metadata(),
                    "{name} decoded to an unexpected value"
                );
                checked += 1;
            }
            other => panic!(
                "unrecognized metadata fixture {other:?} — add a matching expected-value \
                 arm to this test before adding the fixture file"
            ),
        }
    }
    assert!(
        checked > 0,
        "no metadata fixtures found under {}",
        dir.display()
    );
}

/// Encoding the same representative value with the *current* code and
/// decoding it back must reproduce the original exactly — catches an
/// encoder/decoder asymmetry a static fixture decode alone can't.
#[test]
fn metadata_round_trips_through_encode_and_decode() {
    let metadata = v1_metadata();
    let bytes = serde_json::to_vec(&metadata).expect("metadata serializes");
    let decoded = Metadata::from_json(&bytes).expect("decodes");
    assert_eq!(decoded, metadata);
}

/// The fixture carries the top-level `"v"` field the ADR 0073 Phase 0
/// `serde_json` convention requires — a cheap, direct sanity check
/// independent of the structural decode above.
#[test]
fn metadata_fixture_carries_the_v_field() {
    let bytes = std::fs::read(metadata_fixtures_dir().join("v1.json"))
        .expect("v1.json fixture is checked in");
    let value: serde_json::Value = serde_json::from_slice(&bytes).expect("fixture is valid JSON");
    assert_eq!(
        value.get("v"),
        Some(&serde_json::Value::from(
            animus_control::meta::METADATA_VERSION
        ))
    );
}

/// Regenerates `v<METADATA_VERSION>.json` from [`v1_metadata`] with the
/// *current* encoder. Run explicitly, never part of the default test run:
/// `cargo test -p animus-control --test format_fixtures generate_fixture_metadata -- --ignored`.
///
/// Refuses to overwrite a fixture that already exists (ADR 0073 Phase 0
/// conventions) — bump [`Metadata::version`]'s `METADATA_VERSION` and add a
/// new file instead of regenerating an existing one.
#[test]
#[ignore]
fn generate_fixture_metadata() {
    let dir = metadata_fixtures_dir();
    std::fs::create_dir_all(&dir).expect("create fixtures dir");
    let path = dir.join(format!("v{}.json", animus_control::meta::METADATA_VERSION));
    if std::fs::metadata(&path).is_ok() {
        panic!(
            "{} already exists — a checked-in fixture is never regenerated in place; \
             bump METADATA_VERSION and add a new fixture file instead",
            path.display()
        );
    }
    let bytes = serde_json::to_vec_pretty(&v1_metadata()).expect("metadata serializes");
    std::fs::write(&path, &bytes).unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
}

// ---------------------------------------------------------------------------
// `shared-wal` (`SHARED_WAL_TAG`, magic `SWL1`) — the `SharedWal` outer line
// envelope, `{"tablet","record"}` per line (ADR 0073 Phase 0 workstream C).
// Instantiated with the same `MetaCommand`/`Metadata` pair as `control-wal`
// (the inner `WalRecord` shape is generic; `Metadata::default()` for the
// same reason as above) — production instantiates `KvCommand`/`KvState`,
// whose JSON goes through the identical envelope code.

/// Lines from three tablets, interleaved, covering every [`WalRecord`]
/// variant. Fixed constants only.
fn v1_shared_wal_lines() -> Vec<(TabletId, WalRecord<MetaCommand, Metadata>)> {
    let entry = |index, term, node| {
        WalRecord::Append(LogEntry {
            index,
            term,
            command: MetaCommand::UpsertMember {
                node: nid(node),
                labels: BTreeMap::new(),
                status: NodeStatus::Active,
            },
            config: None,
            learners: None,
        })
    };
    vec![
        (
            TabletId(1),
            WalRecord::Hard {
                term: 2,
                voted_for: Some(nid(1)),
            },
        ),
        (
            TabletId(2),
            WalRecord::Hard {
                term: 4,
                voted_for: None,
            },
        ),
        (TabletId(1), entry(1, 2, 2)),
        (TabletId(7), entry(1, 4, 3)),
        (TabletId(2), entry(1, 4, 4)),
        (TabletId(1), entry(2, 2, 5)),
        (TabletId(1), WalRecord::Truncate { keep: 1 }),
        (
            TabletId(7),
            WalRecord::Snapshot {
                metadata: Metadata::default(),
                last_index: 5,
                last_term: 4,
                config: Some(BTreeSet::from([nid(1), nid(2)])),
                learners: Some(BTreeSet::from([nid(3)])),
            },
        ),
        (TabletId(2), entry(2, 4, 6)),
    ]
}

fn shared_wal_fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/formats/shared-wal")
}

fn encode_shared_wal(lines: &[(TabletId, WalRecord<MetaCommand, Metadata>)]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for (tablet, record) in lines {
        bytes
            .extend(PersistedState::<MetaCommand, Metadata>::encode_tagged_record(*tablet, record));
    }
    bytes
}

/// Iterates every file under `tests/fixtures/formats/shared-wal/` (never
/// naming `v1` literally) and asserts each decodes structurally.
#[test]
fn decodes_every_checked_in_shared_wal_fixture_structurally() {
    let dir = shared_wal_fixtures_dir();
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
                let decoded = PersistedState::<MetaCommand, Metadata>::decode_tagged(&bytes)
                    .unwrap_or_else(|e| panic!("{name} failed to decode: {e}"));
                assert_eq!(
                    decoded,
                    v1_shared_wal_lines(),
                    "{name} decoded to an unexpected value"
                );
                checked += 1;
            }
            other => panic!(
                "unrecognized shared-wal fixture {other:?} — add a matching expected-value \
                 arm to this test before adding the fixture file"
            ),
        }
    }
    assert!(
        checked > 0,
        "no shared-wal fixtures found under {}",
        dir.display()
    );
}

#[test]
fn shared_wal_round_trips_through_encode_and_decode() {
    let lines = v1_shared_wal_lines();
    let bytes = encode_shared_wal(&lines);
    let decoded = PersistedState::<MetaCommand, Metadata>::decode_tagged(&bytes).expect("decodes");
    assert_eq!(decoded, lines);
}

/// The fixture's first line carries [`SHARED_WAL_TAG`]'s magic/version
/// right after the 8-hex-digit checksum and its colon.
#[test]
fn shared_wal_fixture_starts_with_the_shared_wal_tag() {
    let bytes = std::fs::read(shared_wal_fixtures_dir().join("v1.bin"))
        .expect("v1.bin fixture is checked in");
    assert_eq!(&bytes[9..13], &SHARED_WAL_TAG.magic);
    assert_eq!(
        &bytes[13..15],
        format!("{:02x}", SHARED_WAL_TAG.version).as_bytes()
    );
}

/// Regenerates `v<SHARED_WAL_TAG.version>.bin`. Run explicitly:
/// `cargo test -p animus-control --test format_fixtures generate_fixture_shared_wal -- --ignored`.
/// Refuses to overwrite an existing fixture (ADR 0073 Phase 0 conventions).
#[test]
#[ignore]
fn generate_fixture_shared_wal() {
    let dir = shared_wal_fixtures_dir();
    std::fs::create_dir_all(&dir).expect("create fixtures dir");
    let path = dir.join(format!("v{}.bin", SHARED_WAL_TAG.version));
    if std::fs::metadata(&path).is_ok() {
        panic!(
            "{} already exists — a checked-in fixture is never regenerated in place; \
             bump SHARED_WAL_TAG's version and add a new fixture file instead",
            path.display()
        );
    }
    let bytes = encode_shared_wal(&v1_shared_wal_lines());
    std::fs::write(&path, &bytes).unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
}
