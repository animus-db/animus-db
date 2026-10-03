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
//! decode path is [`Metadata::from_json`]. The system-keyspace mirror's own
//! value fixtures (`mirror-entities`, `mirror-version`, ADR 0073 Phase 1
//! P1-C) live at the bottom of this file.

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

/// The exact bytes a current (v2) writer produces for [`v1_records`] as two
/// persist rounds: records 0..2 and a sync marker, then records 2.. and a
/// second marker — each marker appended after the round it covers, at the
/// file length it claims.
fn v2_wal_bytes() -> Vec<u8> {
    let records = v1_records();
    let mut bytes = Vec::new();
    for round in [&records[..2], &records[2..]] {
        bytes.extend(encode_all(round));
        let marker = animus_control::format::encode_sync_marker(&CONTROL_WAL, bytes.len() as u64);
        bytes.extend(marker);
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
            "v2.bin" => {
                // Same records as v1 (the payload shape did not change); the
                // sync markers are consumed by the decoder.
                let decoded = PersistedState::<MetaCommand, Metadata>::decode(&bytes)
                    .unwrap_or_else(|e| panic!("{name} failed to decode: {e}"));
                assert_eq!(
                    decoded,
                    v1_records(),
                    "{name} decoded to an unexpected value"
                );
                assert_eq!(
                    bytes,
                    v2_wal_bytes(),
                    "{name}: current writer emits the fixture"
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
    let bytes = std::fs::read(fixtures_dir().join(format!("v{}.bin", CONTROL_WAL.version)))
        .expect("the current version's fixture is checked in");
    assert_eq!(&bytes[9..13], &CONTROL_WAL.magic);
    assert_eq!(
        &bytes[13..15],
        format!("{:02x}", CONTROL_WAL.version).as_bytes()
    );
}

/// Regenerates `v<CONTROL_WAL.version>.bin` from [`v1_records`] with the
/// *current* encoder. Run explicitly, never part of the default test run:
/// `cargo test -p animus-control --test it format_fixtures::generate_fixture_control_wal -- --ignored`.
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
    let bytes = v2_wal_bytes();
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
/// test run: `cargo test -p animus-control --test it format_fixtures::
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

/// The era shape (ADR 0073 Phase 2, P2-A): two data members, one
/// control-only node (a `node_addrs` claim with no `Member` row), every node
/// reported `[1, 2]`, finalized to cluster version 2. Built through real
/// commands, like [`v1_metadata`].
fn v1_era_metadata() -> Metadata {
    use animus_control::meta::NodeAddrs;
    use animus_control::version::VersionRange;
    let mut m = Metadata::default();
    let mut commands = vec![
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
        MetaCommand::RegisterNode {
            node: nid(3),
            addrs: NodeAddrs {
                internal: "127.0.0.1:9303".to_string(),
                client: "127.0.0.1:9003".to_string(),
                intra: "127.0.0.1:9603".to_string(),
                admin: "127.0.0.1:9503".to_string(),
                role: "control".to_string(),
            },
            labels: BTreeMap::new(),
        },
    ];
    for (n, build) in [(1, "2.0.0"), (2, "2.0.0"), (3, "2.0.1")] {
        commands.push(MetaCommand::ReportNodeVersion {
            node: nid(n),
            range: VersionRange::new(1, 2),
            build: build.to_string(),
        });
    }
    commands.push(MetaCommand::FinalizeClusterVersion {
        expected: 1,
        target: 2,
    });
    for command in &commands {
        assert_eq!(
            m.apply(command),
            ApplyOutcome::Applied,
            "fixture premise: every command applies cleanly"
        );
    }
    assert!(m.versioning_active() && m.cluster_version() == 2);
    m
}

/// ADR 0073 Phase 2 (P2-A): era-0 serialization is byte-identical to Phase 1's.
/// `v1_metadata` carries no version record, so its current encoding must equal
/// the frozen `v1.json` (pretty-printed, as the generator wrote it) exactly,
/// and must not mention either new field.
#[test]
fn era_0_metadata_encoding_is_byte_identical_to_the_v1_fixture() {
    let fixture = std::fs::read(metadata_fixtures_dir().join("v1.json")).expect("v1.json");
    let encoded = serde_json::to_vec_pretty(&v1_metadata()).expect("serializes");
    assert_eq!(
        String::from_utf8(encoded).unwrap().trim_end(),
        String::from_utf8(fixture).unwrap().trim_end()
    );
    let compact = serde_json::to_string(&v1_metadata()).unwrap();
    assert!(!compact.contains("node_versions") && !compact.contains("cluster_version"));
    let era = serde_json::to_string(&v1_era_metadata()).unwrap();
    assert!(era.contains("node_versions") && era.contains("\"cluster_version\":2"));
}

/// A Phase 1 reader's view: the era fixture decodes fine with the new fields
/// absent from the document ignored by an old shape — modelled here by
/// stripping them and checking the rest still decodes to the era-0 reading.
#[test]
fn era_fixture_round_trips_and_an_absent_version_reads_as_one() {
    let era = v1_era_metadata();
    let bytes = serde_json::to_vec(&era).unwrap();
    assert_eq!(Metadata::from_json(&bytes).unwrap(), era);
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value.as_object_mut().unwrap().remove("node_versions");
    value.as_object_mut().unwrap().remove("cluster_version");
    let stripped: Metadata = serde_json::from_value(value).unwrap();
    assert!(!stripped.versioning_active());
    assert_eq!(stripped.cluster_version(), 1);
}

fn metadata_fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/formats/metadata")
}

/// Iterates every file under `tests/fixtures/formats/metadata/` and asserts
/// each decodes, via the version-dispatching [`Metadata::from_json`], to its
/// own per-version expected value (ADR 0073 Phase 1). The version comes from
/// the `vN.json` file name and must agree with the document's own `"v"`; an
/// unrecognised version panics, so a new fixture forces a new expectation.
#[test]
fn decodes_every_checked_in_metadata_fixture_to_its_per_version_value() {
    let dir = metadata_fixtures_dir();
    let mut seen = Vec::new();
    let entries = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("reading fixtures dir {}: {e}", dir.display()));
    for entry in entries {
        let path = entry.expect("readable dir entry").path();
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_string();
        // `vN.json` (the era-0 shape) or `vN-<shape>.json` (ADR 0073 Phase 2:
        // a gated additive field inside the same `"v": N`).
        let stem = name
            .strip_prefix('v')
            .and_then(|r| r.strip_suffix(".json"))
            .unwrap_or_else(|| panic!("{name}: fixture name is not vN[-shape].json"));
        let (num, shape) = match stem.split_once('-') {
            Some((n, shape)) => (n, Some(shape)),
            None => (stem, None),
        };
        let version: u32 = num
            .parse()
            .unwrap_or_else(|_| panic!("{name}: fixture name is not vN[-shape].json"));
        let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("reading {name}: {e}"));
        let decoded =
            Metadata::from_json(&bytes).unwrap_or_else(|e| panic!("{name} failed to decode: {e}"));
        assert_eq!(decoded.version, version, "{name}: file name vs decoded v");
        let expected = match (version, shape) {
            (1, None) => v1_metadata(),
            (1, Some("era")) => v1_era_metadata(),
            (other, _) => panic!(
                "{name}: no expected value for metadata v{other} shape {shape:?}; add a match arm (and a \
                 frozen legacy decoder) before adding the fixture file"
            ),
        };
        assert_eq!(decoded, expected, "{name} decoded to an unexpected value");
        seen.push((version, shape.map(str::to_owned)));
    }
    assert!(
        !seen.is_empty(),
        "no metadata fixtures under {}",
        dir.display()
    );
    assert!(
        seen.contains(&(animus_control::meta::METADATA_VERSION, None)),
        "no fixture for the current METADATA_VERSION: {seen:?}"
    );
}

/// `from_json` dispatches on the peeked version: `0` and future versions are
/// refused by name, a missing `"v"` is pre-baseline; plain serde (the
/// nested-in-an-envelope path) still reads a `"v"`-less document as v1, which
/// the frozen `control-wal`/`shared-wal` fixtures depend on.
#[test]
fn metadata_from_json_refuses_unknown_versions_and_serde_defaults_missing_v() {
    use animus_control::format::FormatError;
    let mut value = serde_json::to_value(v1_metadata()).unwrap();
    for bad in [0u8, 2, 99] {
        value["v"] = bad.into();
        let err = Metadata::from_json(value.to_string().as_bytes()).unwrap_err();
        assert_eq!(
            err,
            FormatError::UnsupportedFormatVersion {
                format: "metadata",
                found: bad,
                max_supported: 1
            },
            "v={bad}"
        );
    }
    value.as_object_mut().unwrap().remove("v");
    let bytes = value.to_string().into_bytes();
    assert!(matches!(
        Metadata::from_json(&bytes),
        Err(FormatError::PreBaselineFormat { .. })
    ));
    let nested: Metadata = serde_json::from_slice(&bytes).expect("serde defaults v to 1");
    assert_eq!(nested, v1_metadata());
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
/// `cargo test -p animus-control --test it format_fixtures::generate_fixture_metadata -- --ignored`.
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

/// Writes `v1-era.json` from [`v1_era_metadata`]; refuses to overwrite.
/// `cargo test -p animus-control --test it format_fixtures::generate_fixture_metadata_era -- --ignored`.
#[test]
#[ignore]
fn generate_fixture_metadata_era() {
    let path = metadata_fixtures_dir().join("v1-era.json");
    if std::fs::metadata(&path).is_ok() {
        panic!(
            "{} already exists — never regenerated in place",
            path.display()
        );
    }
    let bytes = serde_json::to_vec_pretty(&v1_era_metadata()).expect("metadata serializes");
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

/// The exact bytes a current (v2) `SharedWal` produces for
/// [`v1_shared_wal_lines`] as two flushes (a sync marker after each).
fn v2_shared_wal_bytes() -> Vec<u8> {
    let lines = v1_shared_wal_lines();
    let mid = lines.len() / 2;
    let mut bytes = Vec::new();
    for round in [&lines[..mid], &lines[mid..]] {
        bytes.extend(encode_shared_wal(round));
        bytes.extend(animus_control::format::encode_sync_marker(
            &SHARED_WAL_TAG,
            bytes.len() as u64,
        ));
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
            "v2.bin" => {
                let decoded = PersistedState::<MetaCommand, Metadata>::decode_tagged(&bytes)
                    .unwrap_or_else(|e| panic!("{name} failed to decode: {e}"));
                assert_eq!(
                    decoded,
                    v1_shared_wal_lines(),
                    "{name} decoded to an unexpected value (markers consumed)"
                );
                assert_eq!(
                    bytes,
                    v2_shared_wal_bytes(),
                    "{name}: current writer emits the fixture"
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
    let bytes =
        std::fs::read(shared_wal_fixtures_dir().join(format!("v{}.bin", SHARED_WAL_TAG.version)))
            .expect("the current version's fixture is checked in");
    assert_eq!(&bytes[9..13], &SHARED_WAL_TAG.magic);
    assert_eq!(
        &bytes[13..15],
        format!("{:02x}", SHARED_WAL_TAG.version).as_bytes()
    );
}

/// Regenerates `v<SHARED_WAL_TAG.version>.bin`. Run explicitly:
/// `cargo test -p animus-control --test it format_fixtures::generate_fixture_shared_wal -- --ignored`.
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
    let bytes = v2_shared_wal_bytes();
    std::fs::write(&path, &bytes).unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
}

// ---------------------------------------------------------------------------
// `mirror-entities` / `mirror-version` — the system-keyspace mirror's VALUE
// encodings (ADR 0073 Phase 1 workstream P1-C, layer 3).
//
// The mirror is `DRIVER_APPLIED` (ADR 0038): its per-entity values and its
// format-version row are the control plane's real durable state, and none of
// it is covered by the `metadata` fixture above (which versions
// `Metadata`'s own JSON, a shape that never reaches disk on the real path).
//
// Layout: `formats/mirror-entities/<EntityKind::as_str()>/v<N>.<ext>` holds
// exactly the value bytes the mirror encoder wrote for one representative
// entity of that kind (the key is re-derived from the real `syskv` key
// helpers, so the fixture stays honest about the key shape without
// duplicating it); `formats/mirror-version/v<N>.bin` holds the value bytes
// of the `SYSKV_FORMAT_VERSION_COUNTER` row. Every fixture decodes through
// the real mirror read path (`apply_key_write` / `rebuild_metadata_from_
// engine`) to a hand-derived expected value, selected by the `vN` in its
// file name; an unrecognised version panics so a new fixture forces a new
// arm.

use animus_control::mirror::{
    self, KeyWrite, NEXT_TABLET_ID_COUNTER, SYSKV_MIRROR_VERSION, apply_and_derive_mirror,
    apply_key_write, rebuild_metadata_from_engine,
};
use animus_control::syskv::{self, DecodedKey, EntityKind};
use animus_control::{
    ExportFormat, ExportType, InputCompressionType, InputFormat, NodeAddrs, OpClass, Policy,
    SecretKey, TableMatch,
};
use animus_storage::{MemoryEngine, MergeOp, StorageEngine};

/// Every [`EntityKind`]. The exhaustive `match` in [`kind_witness`] makes a
/// new variant a compile error there, and the scenario/fixture-directory
/// tests below fail until this list, [`v1_entity`], and a checked-in fixture
/// directory all cover it.
const ALL_KINDS: [EntityKind; 19] = [
    EntityKind::Tablet,
    EntityKind::Member,
    EntityKind::Schema,
    EntityKind::Policy,
    EntityKind::NodeAddrs,
    EntityKind::Counter,
    EntityKind::StreamShard,
    EntityKind::IndexBackfill,
    EntityKind::SplitLineage,
    EntityKind::SplitPlacing,
    EntityKind::Backup,
    EntityKind::BackupProgress,
    EntityKind::Restore,
    EntityKind::PitrSegment,
    EntityKind::PitrBaseBackup,
    EntityKind::Credential,
    EntityKind::Export,
    EntityKind::Import,
    EntityKind::NodeVersion,
];

/// Exhaustive over [`EntityKind`] (no wildcard).
fn kind_witness(kind: EntityKind) -> usize {
    match kind {
        EntityKind::Tablet => 0,
        EntityKind::Member => 1,
        EntityKind::Schema => 2,
        EntityKind::Policy => 3,
        EntityKind::NodeAddrs => 4,
        EntityKind::Counter => 5,
        EntityKind::StreamShard => 6,
        EntityKind::IndexBackfill => 7,
        EntityKind::SplitLineage => 8,
        EntityKind::SplitPlacing => 9,
        EntityKind::Backup => 10,
        EntityKind::BackupProgress => 11,
        EntityKind::Restore => 12,
        EntityKind::PitrSegment => 13,
        EntityKind::PitrBaseBackup => 14,
        EntityKind::Credential => 15,
        EntityKind::Export => 16,
        EntityKind::Import => 17,
        EntityKind::NodeVersion => 18,
    }
}

fn mirror_entities_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/formats/mirror-entities")
}

fn mirror_version_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/formats/mirror-version")
}

type Live = BTreeMap<Vec<u8>, Vec<u8>>;

fn run_mirror(meta: &mut Metadata, live: &mut Live, command: MetaCommand) {
    let (outcome, writes) = apply_and_derive_mirror(meta, &command);
    assert_eq!(
        outcome,
        ApplyOutcome::Applied,
        "fixture premise: {command:?} applies cleanly"
    );
    for w in writes {
        match w {
            KeyWrite::Put(k, v) => {
                live.insert(k, v);
            }
            KeyWrite::Delete(k) => {
                live.remove(&k);
            }
        }
    }
}

/// The `Metadata` a fixed, fully-deterministic command script produces, plus
/// the final (last-write-wins) system-keyspace value bytes the real mirror
/// encoder derived along the way. Every `EntityKind` ends up with at least
/// one live row (asserted by the callers).
fn v1_mirror_scenario() -> (Metadata, Live) {
    let mut meta = Metadata::default();
    let mut live = Live::new();
    let (m, l) = (&mut meta, &mut live);
    let labels = BTreeMap::from([("region".to_string(), "eu-west".to_string())]);
    for n in [1, 2] {
        run_mirror(
            m,
            l,
            MetaCommand::UpsertMember {
                node: nid(n),
                labels: labels.clone(),
                status: NodeStatus::Active,
            },
        );
    }
    run_mirror(
        m,
        l,
        MetaCommand::RegisterNodeAddrs {
            node: nid(1),
            addrs: NodeAddrs {
                internal: "10.0.0.1:7001".to_string(),
                client: "10.0.0.1:8001".to_string(),
                admin: "10.0.0.1:9001".to_string(),
                intra: "10.0.0.1:7501".to_string(),
                role: "combined".to_string(),
            },
        },
    );
    run_mirror(
        m,
        l,
        MetaCommand::CreateTableSchema {
            table: "orders".to_string(),
            schema: TableSchema::simple("id", ColumnType::String),
        },
    );
    run_mirror(
        m,
        l,
        MetaCommand::CreateTableIndex {
            table: "orders".to_string(),
            index: animus_control::IndexDef {
                name: "by_email".to_string(),
                kind: animus_control::IndexKind::Global,
                hash_attribute: "email".to_string(),
                sort_attribute: None,
                projection: animus_control::IndexProjection::All,
                status: animus_control::IndexStatus::Creating,
                hash_attribute_type: None,
                sort_attribute_type: None,
            },
        },
    );
    run_mirror(
        m,
        l,
        MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("orders".to_string()),
            range: KeyRange::whole(),
            replicas: vec![nid(1), nid(2)],
        },
    );
    run_mirror(
        m,
        l,
        MetaCommand::SetTabletPolicy {
            tablet: TabletId(1),
            policy: Some(PlacementPolicy::simple("p", 2)),
        },
    );
    run_mirror(
        m,
        l,
        MetaCommand::MarkIndexBackfilled {
            table: "orders".to_string(),
            index: "by_email".to_string(),
            tablet: TabletId(1),
        },
    );
    run_mirror(
        m,
        l,
        MetaCommand::SetTableStream {
            table: "orders".to_string(),
            spec: Some(animus_control::StreamSpec {
                view_type: animus_control::StreamViewType::NewAndOldImages,
                label: "L1".to_string(),
            }),
        },
    );
    run_mirror(
        m,
        l,
        MetaCommand::SealStreamShard {
            table: "orders".to_string(),
            label: "L1".to_string(),
            tablet: TabletId(1),
            epoch: 0,
            view_type: animus_control::StreamViewType::NewAndOldImages,
            hlc_range: (0, 100),
            count: 7,
            seal_wall_ms: 1_700_000_000_000,
            replicas: vec![nid(1), nid(2)],
            object_id: "orders/L1/1/0/obj".to_string(),
        },
    );
    run_mirror(
        m,
        l,
        MetaCommand::UpdateContinuousBackups {
            table: "orders".to_string(),
            enabled: true,
            wall_ms: 1_700_000_000_100,
        },
    );
    run_mirror(
        m,
        l,
        MetaCommand::SealPitrSegment {
            table: "orders".to_string(),
            generation: 1,
            tablet: TabletId(1),
            epoch: 0,
            hlc_range: (0, 100),
            count: 7,
            seal_wall_ms: 1_700_000_000_200,
            replicas: vec![nid(1), nid(2)],
            object_id: "backup/pitr/orders/1/0/obj".to_string(),
        },
    );
    run_mirror(
        m,
        l,
        MetaCommand::BeginBackup {
            backup_id: "b1".to_string(),
            table: "orders".to_string(),
            created_wall_ms: 1_700_000_000_300,
            backup_name: "nightly".to_string(),
            pitr_base: true,
        },
    );
    run_mirror(
        m,
        l,
        MetaCommand::RecordBackupTabletComplete {
            backup_id: "b1".to_string(),
            tablet: TabletId(1),
            cut_version: 42,
            bytes: 4096,
            chunk_count: 2,
        },
    );
    run_mirror(
        m,
        l,
        MetaCommand::PutCredential {
            id: "AKID1".to_string(),
            secret: SecretKey::new("s3cr3t"),
            policy: Policy {
                tables: TableMatch::Names(BTreeSet::from(["orders".to_string()])),
                ops: BTreeSet::from([OpClass::Read]),
            },
            enabled: true,
            now: 1_000,
        },
    );
    run_mirror(
        m,
        l,
        MetaCommand::BeginExport {
            export_id: "e1".to_string(),
            table: "orders".to_string(),
            table_arn: "arn:aws:dynamodb:animus:0:table/orders".to_string(),
            s3_bucket: "bucket".to_string(),
            s3_prefix: Some("exports/".to_string()),
            format: ExportFormat::DynamoDbJson,
            export_type: ExportType::Full,
            export_time_ms: None,
            client_token: Some("tok-e1".to_string()),
            created_wall_ms: 1_700_000_000_400,
        },
    );
    run_mirror(
        m,
        l,
        MetaCommand::CreateTableSchema {
            table: "restored".to_string(),
            schema: TableSchema::simple("id", ColumnType::String),
        },
    );
    run_mirror(
        m,
        l,
        MetaCommand::BeginRestore {
            restore_id: "r1".to_string(),
            backup_id: "b1".to_string(),
            source_table: "orders".to_string(),
            target_table: "restored".to_string(),
            tablet: TabletId(10),
            replicas: vec![nid(1)],
            gsi_defs: Vec::new(),
            pitr: None,
        },
    );
    run_mirror(
        m,
        l,
        MetaCommand::CreateTableSchema {
            table: "imported".to_string(),
            schema: TableSchema::simple("id", ColumnType::String),
        },
    );
    run_mirror(
        m,
        l,
        MetaCommand::BeginImport {
            import_id: "i1".to_string(),
            target_table: "imported".to_string(),
            target_table_arn: "arn:aws:dynamodb:animus:0:table/imported".to_string(),
            table_id: "table-id-1".to_string(),
            s3_bucket: "bucket".to_string(),
            s3_prefix: None,
            input_format: InputFormat::DynamoDbJson,
            input_compression: InputCompressionType::Gzip,
            base_schema: Box::new(TableSchema::simple("id", ColumnType::String)),
            key_types: vec![("id".to_string(), "S".to_string())],
            gsi_defs: Vec::new(),
            throughput: None,
            tablet: TabletId(11),
            replicas: vec![nid(1)],
            client_token: None,
            created_wall_ms: 1_700_000_000_500,
        },
    );
    // In-place split of tablet 1 into children 12 and 13. The children's
    // replica sets deliberately do not satisfy the RF-2 policy, so cutover
    // also writes a directed-Placing row.
    let epoch = m.tablets[&TabletId(1)].epoch;
    run_mirror(
        m,
        l,
        MetaCommand::BeginSplitInPlace {
            parent: TabletId(1),
            expected_epoch: epoch,
            split_key: vec![0x80, 0, 0, 0, 0, 0, 0, 0],
            children: [(TabletId(12), vec![nid(1)]), (TabletId(13), vec![nid(1)])],
        },
    );
    let epoch = m.tablets[&TabletId(1)].epoch;
    run_mirror(
        m,
        l,
        MetaCommand::CutoverSplit {
            parent: TabletId(1),
            expected_epoch: epoch,
            cutover_wall_ms: 1_700_000_000_600,
        },
    );
    // ADR 0073 Phase 2 (P2-A): the era-only `NodeVersion` kind.
    run_mirror(
        m,
        l,
        MetaCommand::ReportNodeVersion {
            node: nid(1),
            range: animus_control::version::VersionRange::new(1, 2),
            build: "2.0.0".to_string(),
        },
    );
    (meta, live)
}

/// For one [`EntityKind`]: the representative entity's system-keyspace key,
/// and the `Metadata` that decoding exactly that one row onto an empty
/// `Metadata` must produce (the expected value, derived from the scenario's
/// authoritative state — not from the fixture bytes under test). Exhaustive
/// over [`EntityKind`].
fn v1_entity(kind: EntityKind, full: &Metadata) -> (Vec<u8>, Metadata) {
    let mut m = Metadata::default();
    let key = match kind {
        EntityKind::Tablet => {
            let id = TabletId(12);
            m.tablets.insert(id, full.tablets[&id].clone());
            syskv::tablet_key(id)
        }
        EntityKind::Member => {
            m.members.insert(nid(1), full.members[&nid(1)].clone());
            syskv::member_key(&nid(1))
        }
        EntityKind::Schema => {
            // `SchemaCatalog::insert` is crate-private; the public path in is
            // the replicated command.
            assert_eq!(
                m.apply(&MetaCommand::CreateTableSchema {
                    table: "orders".to_string(),
                    schema: full.schemas.get("orders").unwrap().clone(),
                }),
                ApplyOutcome::Applied
            );
            syskv::schema_key("orders")
        }
        EntityKind::Policy => {
            let id = TabletId(12);
            m.policies.insert(id, full.policies[&id].clone());
            syskv::policy_key(id)
        }
        EntityKind::NodeAddrs => {
            m.node_addrs
                .insert(nid(1), full.node_addrs[&nid(1)].clone());
            syskv::node_addrs_key(&nid(1))
        }
        EntityKind::Counter => {
            m.next_tablet_id = full.next_tablet_id;
            syskv::counter_key(NEXT_TABLET_ID_COUNTER)
        }
        EntityKind::StreamShard => {
            let id = (TabletId(1), 0);
            m.stream_shards.insert(id, full.stream_shards[&id].clone());
            syskv::stream_shard_key(id.0, id.1)
        }
        EntityKind::IndexBackfill => {
            m.index_backfill
                .insert((TabletId(1), "by_email".to_string()), ());
            syskv::index_backfill_key(TabletId(1), "by_email")
        }
        EntityKind::SplitLineage => {
            let id = TabletId(12);
            m.split_lineage.insert(id, full.split_lineage[&id]);
            syskv::split_lineage_key(id)
        }
        EntityKind::SplitPlacing => {
            let id = *full
                .split_placing
                .keys()
                .next()
                .expect("scenario premise: cutover wrote a directed-Placing row");
            m.split_placing.insert(id, full.split_placing[&id].clone());
            syskv::split_placing_key(id)
        }
        EntityKind::Backup => {
            m.backups
                .insert("b1".to_string(), full.backups["b1"].clone());
            syskv::backup_key("b1")
        }
        EntityKind::BackupProgress => {
            let id = ("b1".to_string(), TabletId(1));
            m.backup_tablet_progress
                .insert(id.clone(), full.backup_tablet_progress[&id]);
            syskv::backup_progress_key("b1", TabletId(1))
        }
        EntityKind::Restore => {
            m.restores
                .insert("r1".to_string(), full.restores["r1"].clone());
            syskv::restore_key("r1")
        }
        EntityKind::PitrSegment => {
            let id = (TabletId(1), 0);
            m.pitr_segments.insert(id, full.pitr_segments[&id].clone());
            syskv::pitr_segment_key(id.0, id.1)
        }
        EntityKind::PitrBaseBackup => {
            m.pitr_base_backups.insert("b1".to_string());
            syskv::pitr_base_backup_key("b1")
        }
        EntityKind::Credential => {
            m.credentials
                .insert("AKID1".to_string(), full.credentials["AKID1"].clone());
            syskv::credential_key("AKID1")
        }
        EntityKind::Export => {
            m.exports
                .insert("e1".to_string(), full.exports["e1"].clone());
            syskv::export_key("e1")
        }
        EntityKind::Import => {
            m.imports
                .insert("i1".to_string(), full.imports["i1"].clone());
            syskv::import_key("i1")
        }
        EntityKind::NodeVersion => {
            m.node_versions
                .insert(nid(1), full.node_versions[&nid(1)].clone());
            syskv::node_version_key(&nid(1))
        }
    };
    (key, m)
}

/// Fixture extension per kind: JSON-valued kinds are `.json`; the kinds
/// whose value is not JSON (an 8-byte big-endian counter, an always-empty
/// presence marker) are `.bin`.
fn entity_ext(kind: EntityKind) -> &'static str {
    match kind {
        EntityKind::Counter | EntityKind::PitrBaseBackup | EntityKind::IndexBackfill => "bin",
        _ => "json",
    }
}

/// Parse `v<N>` out of a fixture file name (`v1.json` -> `1`).
fn fixture_version(path: &Path) -> u32 {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_else(|| panic!("fixture {} has no file stem", path.display()));
    stem.strip_prefix('v')
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("fixture {} is not named v<N>.<ext>", path.display()))
}

fn fixture_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("reading fixtures dir {}: {e}", dir.display()))
        .map(|e| e.expect("readable dir entry").path())
        .collect();
    files.sort();
    files
}

/// The scenario itself must exercise every kind — otherwise a kind could be
/// "covered" by a fixture the real encoder never actually produces.
#[test]
fn mirror_scenario_produces_a_live_row_of_every_entity_kind() {
    let (full, live) = v1_mirror_scenario();
    let mut seen: BTreeSet<usize> = BTreeSet::new();
    for kind in ALL_KINDS {
        assert!(seen.insert(kind_witness(kind)), "{kind:?} listed twice");
        let (key, _) = v1_entity(kind, &full);
        assert!(
            live.contains_key(&key),
            "scenario produced no live mirror row for {kind:?}"
        );
        match syskv::decode_key(&key) {
            Some(DecodedKey::Entity { kind: k, .. }) => assert_eq!(k, kind),
            other => panic!("{kind:?} key decodes to {other:?}"),
        }
    }
    assert_eq!(seen.len(), 19, "ALL_KINDS must list every EntityKind");
    // Any kind the scenario writes but ALL_KINDS forgot is caught here too.
    for key in live.keys() {
        if let Some(DecodedKey::Entity { kind, .. }) = syskv::decode_key(key) {
            assert!(ALL_KINDS.contains(&kind), "{kind:?} missing from ALL_KINDS");
        }
    }
}

/// Every `EntityKind` has a fixture directory, and that directory holds a
/// fixture for the current mirror version.
#[test]
fn every_entity_kind_has_a_fixture_for_the_current_version() {
    for kind in ALL_KINDS {
        let dir = mirror_entities_root().join(kind.as_str());
        assert!(
            dir.is_dir(),
            "EntityKind::{kind:?} has no fixture directory {} — add one (ADR 0073)",
            dir.display()
        );
        let versions: Vec<u32> = fixture_files(&dir)
            .iter()
            .map(|p| fixture_version(p))
            .collect();
        assert!(
            versions.contains(&SYSKV_MIRROR_VERSION),
            "{kind:?} has no fixture for the current mirror version v{SYSKV_MIRROR_VERSION}"
        );
    }
    // And no stray directory for a kind that does not exist.
    for entry in fixture_files(&mirror_entities_root()) {
        let name = entry.file_name().unwrap().to_str().unwrap().to_string();
        assert!(
            ALL_KINDS.iter().any(|k| k.as_str() == name),
            "unrecognized mirror-entities directory {name:?}"
        );
    }
}

/// Every checked-in entity fixture decodes, through the real mirror read
/// path, to the expected value for its version.
#[test]
fn decodes_every_checked_in_mirror_entity_fixture_structurally() {
    let (full, _) = v1_mirror_scenario();
    let mut checked = 0usize;
    for kind in ALL_KINDS {
        let dir = mirror_entities_root().join(kind.as_str());
        for path in fixture_files(&dir) {
            let version = fixture_version(&path);
            let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            match version {
                1 => {
                    let (key, expected) = v1_entity(kind, &full);
                    let mut decoded = Metadata::default();
                    apply_key_write(&mut decoded, &KeyWrite::Put(key, bytes));
                    assert_eq!(
                        decoded,
                        expected,
                        "{} decoded to an unexpected value",
                        path.display()
                    );
                    checked += 1;
                }
                other => panic!(
                    "unrecognized mirror-entities fixture version v{other} ({}) — add a \
                     matching expected-value arm",
                    path.display()
                ),
            }
        }
    }
    assert!(
        checked >= ALL_KINDS.len(),
        "too few fixtures checked: {checked}"
    );
}

/// The current encoder's bytes decode (through the same path) to the same
/// value the checked-in v1 fixture does — an encoder/decoder asymmetry check.
#[test]
fn mirror_entities_round_trip_through_encode_and_decode() {
    let (full, live) = v1_mirror_scenario();
    for kind in ALL_KINDS {
        let (key, expected) = v1_entity(kind, &full);
        let mut decoded = Metadata::default();
        apply_key_write(
            &mut decoded,
            &KeyWrite::Put(key.clone(), live[&key].clone()),
        );
        assert_eq!(decoded, expected, "{kind:?} round trip");
    }
}

/// Regenerates the `mirror-entities/<kind>/v<SYSKV_MIRROR_VERSION>.<ext>`
/// fixtures from the real mirror encoder. Run explicitly:
/// `cargo test -p animus-control --test it format_fixtures::generate_fixture_mirror_entities -- --ignored`.
/// Refuses to overwrite an existing fixture.
#[test]
#[ignore]
fn generate_fixture_mirror_entities() {
    let (full, live) = v1_mirror_scenario();
    for kind in ALL_KINDS {
        let (key, _) = v1_entity(kind, &full);
        let dir = mirror_entities_root().join(kind.as_str());
        std::fs::create_dir_all(&dir).expect("create fixtures dir");
        let path = dir.join(format!("v{SYSKV_MIRROR_VERSION}.{}", entity_ext(kind)));
        if std::fs::metadata(&path).is_ok() {
            panic!(
                "{} already exists — a checked-in fixture is never regenerated in place; \
                 bump SYSKV_MIRROR_VERSION and add a new fixture file instead",
                path.display()
            );
        }
        std::fs::write(&path, &live[&key])
            .unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
    }
}

/// Every checked-in `mirror-version` fixture (the bytes of the
/// `SYSKV_FORMAT_VERSION_COUNTER` row's value) is the version named by its
/// own file name, and an engine holding that row plus a real entity row
/// rebuilds cleanly through `rebuild_metadata_from_engine`.
#[tokio::test]
async fn decodes_every_checked_in_mirror_version_fixture_structurally() {
    let (full, live) = v1_mirror_scenario();
    let (member_key, member_expected) = v1_entity(EntityKind::Member, &full);
    let mut versions = Vec::new();
    for path in fixture_files(&mirror_version_dir()) {
        let version = fixture_version(&path);
        let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        match version {
            1 => {
                assert_eq!(
                    bytes,
                    1u64.to_be_bytes().to_vec(),
                    "{} is not the 8-byte big-endian encoding of 1",
                    path.display()
                );
                let engine = MemoryEngine::new();
                engine
                    .merge_batch(vec![
                        MergeOp::put(mirror::syskv_format_version_key(), bytes.clone(), 1),
                        MergeOp::put(member_key.clone(), live[&member_key].clone(), 1),
                    ])
                    .await
                    .expect("merge");
                let rebuilt = rebuild_metadata_from_engine(&engine)
                    .await
                    .unwrap_or_else(|e| panic!("{} did not rebuild: {e}", path.display()));
                assert_eq!(rebuilt.members, member_expected.members);
            }
            other => panic!(
                "unrecognized mirror-version fixture version v{other} ({}) — add a matching \
                 expected-value arm",
                path.display()
            ),
        }
        versions.push(version);
    }
    assert!(
        versions.contains(&SYSKV_MIRROR_VERSION),
        "no mirror-version fixture for the current version v{SYSKV_MIRROR_VERSION}"
    );
}

/// The current encoder writes exactly the current version's fixture bytes.
#[test]
fn mirror_version_row_matches_the_current_encoder() {
    match mirror::put_syskv_format_version() {
        KeyWrite::Put(key, value) => {
            assert_eq!(key, mirror::syskv_format_version_key());
            let path = mirror_version_dir().join(format!("v{SYSKV_MIRROR_VERSION}.bin"));
            let fixture =
                std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            assert_eq!(value, fixture);
        }
        KeyWrite::Delete(_) => panic!("format-version row is a Put"),
    }
}

/// Regenerates `mirror-version/v<SYSKV_MIRROR_VERSION>.bin`. Run explicitly:
/// `cargo test -p animus-control --test it format_fixtures::generate_fixture_mirror_version -- --ignored`.
#[test]
#[ignore]
fn generate_fixture_mirror_version() {
    let dir = mirror_version_dir();
    std::fs::create_dir_all(&dir).expect("create fixtures dir");
    let path = dir.join(format!("v{SYSKV_MIRROR_VERSION}.bin"));
    if std::fs::metadata(&path).is_ok() {
        panic!(
            "{} already exists — bump SYSKV_MIRROR_VERSION and add a new fixture instead",
            path.display()
        );
    }
    let KeyWrite::Put(_, value) = mirror::put_syskv_format_version() else {
        panic!("format-version row is a Put");
    };
    std::fs::write(&path, value).unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
}
