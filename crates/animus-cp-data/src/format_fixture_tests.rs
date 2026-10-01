//! Golden-fixture tests for the RaftKV formats (ADR 0073 Phase 0, workstream
//! C, layer 3): `raftkv-wire`, `raftkv-image`, and `raftkv-wal`.
//!
//! These live in-crate rather than in `tests/format_fixtures.rs` because the
//! wire/image codec is `pub(crate)` (an integration test cannot reach it) and
//! because the fixture content is built from the same `codec::tests::sample_*`
//! helpers the round-trip tests use, so there is exactly one construction of
//! "every `KvCommand`/`RaftMsg` variant". The layout convention is the same as
//! `tests/format_fixtures.rs`: `tests/fixtures/formats/<format>/v<N>.bin`,
//! decoded structurally against a hand-built expected value, round-tripped
//! byte-for-byte, with an `#[ignore]`d generator that refuses to overwrite.
//!
//! Fixture containers:
//! - `raftkv-wire/v1.bin`: a sequence of wire frames, each a `u32` big-endian
//!   length followed by that many bytes of one `encode_wire` output, to EOF.
//!   The frames are [`sample_wires`] in order (every `RaftMsg` variant, both
//!   probes, the heartbeat batches; the `AppendEntries` carries every
//!   `KvCommand` variant).
//! - `raftkv-image/v1.bin`: exactly one `encode_image` output.
//! - `cp-engine-layout/v1.bin`: the per-tablet engine layout marker (layer 4,
//!   `layout.rs`) for tablet `7`: a `u32` big-endian length then the marker
//!   **key** bytes, then a `u32` big-endian length then the marker **value**
//!   bytes (`b"KLY1" || epoch`), to EOF.
//! - `raftkv-wal/v1.bin`: `WalRecord<KvCommand, KvState>` lines in the
//!   per-group `CWL1` envelope (`PersistedState::encode_record`), the
//!   compatibility-relevant one: `KvCommand` is stored as `serde_json` here,
//!   not via the binary codec.

use std::path::{Path, PathBuf};

use animus_control::format::FormatError;
use animus_control::persist::{PersistedState, WalRecord};
use animus_env::nid;

use crate::codec::tests::{sample_entries, sample_wires};
use crate::codec::{self};
use crate::hlc::HlcTimestamp;
use crate::{ImageEntry, KvCommand, KvState, KvWire};

fn formats_dir(format: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/formats")
        .join(format)
}

/// Every `v<N>.bin` in `dir`, sorted, as `(version, bytes)`.
fn fixture_files(dir: &Path) -> Vec<(u8, Vec<u8>)> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()))
    {
        let path = entry.expect("dir entry").path();
        let stem = path.file_stem().and_then(|s| s.to_str()).expect("stem");
        let version: u8 = stem
            .strip_prefix('v')
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| panic!("fixture {} is not named v<N>", path.display()));
        out.push((version, std::fs::read(&path).expect("read fixture")));
    }
    out.sort();
    assert!(!out.is_empty(), "no fixtures in {}", dir.display());
    out
}

fn write_new_fixture(dir: &Path, version: u8, bytes: &[u8]) {
    std::fs::create_dir_all(dir).expect("create fixtures dir");
    let path = dir.join(format!("v{version}.bin"));
    if std::fs::metadata(&path).is_ok() {
        panic!(
            "{} already exists — a checked-in fixture is never regenerated in place; \
             bump the format's VERSION and add a new fixture file instead",
            path.display()
        );
    }
    std::fs::write(&path, bytes).unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
}

// ---------------------------------------------------------------------------
// raftkv-wire

fn pack_frames(frames: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for f in frames {
        out.extend_from_slice(&(f.len() as u32).to_be_bytes());
        out.extend_from_slice(f);
    }
    out
}

fn unpack_frames(mut bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    while !bytes.is_empty() {
        let len = u32::from_be_bytes(bytes[..4].try_into().expect("length prefix")) as usize;
        out.push(bytes[4..4 + len].to_vec());
        bytes = &bytes[4 + len..];
    }
    out
}

fn v1_wire_bytes() -> Vec<u8> {
    let frames: Vec<Vec<u8>> = sample_wires().iter().map(codec::encode_wire).collect();
    pack_frames(&frames)
}

#[test]
fn raftkv_wire_decodes_every_checked_in_fixture_structurally() {
    for (version, bytes) in fixture_files(&formats_dir("raftkv-wire")) {
        let frames = unpack_frames(&bytes);
        match version {
            1 => {
                let expected = sample_wires();
                assert_eq!(frames.len(), expected.len(), "v1 frame count");
                for (i, (frame, want)) in frames.iter().zip(&expected).enumerate() {
                    assert_eq!(frame[0], 0xCB, "v1 frame {i}: magic");
                    assert_eq!(frame[1], 1, "v1 frame {i}: version byte");
                    let got =
                        codec::decode_wire(frame).unwrap_or_else(|e| panic!("v1 frame {i}: {e}"));
                    // `KvWire` has no `PartialEq` (`RaftMsg` doesn't derive it);
                    // the `Debug` form covers every field.
                    assert_eq!(format!("{got:?}"), format!("{want:?}"), "v1 frame {i}");
                }
                // Every KvCommand variant is present inside an AppendEntries.
                let mut variants = std::collections::BTreeSet::new();
                for w in &expected {
                    if let KvWire::Raft(animus_control::raft::RaftMsg::AppendEntries {
                        entries,
                        ..
                    }) = w
                    {
                        for e in entries {
                            let dbg = format!("{:?}", e.command);
                            variants.insert(
                                dbg.split(|c: char| !c.is_alphanumeric())
                                    .next()
                                    .unwrap_or("")
                                    .to_owned(),
                            );
                        }
                    }
                }
                assert_eq!(variants.len(), 16, "all KvCommand variants: {variants:?}");
            }
            other => panic!(
                "raftkv-wire fixture v{other} has no expected value — add one (ADR 0073 checklist step 4)"
            ),
        }
    }
}

#[test]
fn raftkv_wire_round_trips_and_matches_the_fixture_bytes() {
    for (version, bytes) in fixture_files(&formats_dir("raftkv-wire")) {
        if version != 1 {
            continue;
        }
        let re: Vec<Vec<u8>> = unpack_frames(&bytes)
            .iter()
            .map(|f| codec::encode_wire(&codec::decode_wire(f).expect("decode")))
            .collect();
        assert_eq!(
            pack_frames(&re),
            bytes,
            "v1: decode -> encode reproduces the fixture"
        );
        assert_eq!(
            v1_wire_bytes(),
            bytes,
            "v1: the current encoder still emits the fixture"
        );
    }
}

#[test]
fn raftkv_wire_fixture_refuses_other_versions_by_name() {
    let frame = unpack_frames(&fixture_files(&formats_dir("raftkv-wire"))[0].1).remove(0);
    for bad in [0u8, 2, 32, 255] {
        let mut f = frame.clone();
        f[1] = bad;
        assert_eq!(
            codec::decode_wire(&f).unwrap_err(),
            FormatError::UnsupportedFormatVersion {
                format: "raftkv-wire",
                found: bad,
                max_supported: 1
            }
        );
    }
}

/// `cargo test -p animus-cp-data --lib generate_fixture_raftkv_wire -- --ignored`.
/// Refuses to overwrite an existing fixture (ADR 0073 Phase 0).
#[test]
#[ignore]
fn generate_fixture_raftkv_wire() {
    write_new_fixture(&formats_dir("raftkv-wire"), 1, &v1_wire_bytes());
}

// ---------------------------------------------------------------------------
// raftkv-image

fn v1_image() -> (Option<HlcTimestamp>, Vec<ImageEntry>) {
    let max_ts = Some(HlcTimestamp {
        wall_ms: 1_767_225_600_000,
        logical: 7,
    });
    let rows: Vec<ImageEntry> = vec![
        (
            crate::KIND_BASE,
            b"pk-a\x00sk".to_vec(),
            Some(vec![0, 1, 255]),
            300,
        ),
        (crate::KIND_BASE, b"pk-b".to_vec(), None, 900), // tombstone
        (crate::KIND_LSI, b"idx-a".to_vec(), Some(vec![7, 7]), 301),
        (
            crate::KIND_CHANGE,
            b"chg-a".to_vec(),
            Some(b"record".to_vec()),
            302,
        ),
        (crate::KIND_FOOTPRINT, Vec::new(), Some(Vec::new()), 0),
        (crate::KIND_CURSOR, b"cur".to_vec(), Some(vec![9]), u64::MAX),
    ];
    (max_ts, rows)
}

#[test]
fn raftkv_image_decodes_every_checked_in_fixture_structurally() {
    for (version, bytes) in fixture_files(&formats_dir("raftkv-image")) {
        assert_eq!(&bytes[..2], &[0xCB, version], "v{version}: magic + version");
        let got = codec::decode_image(&bytes).unwrap_or_else(|e| panic!("v{version}: {e}"));
        match version {
            1 => {
                assert_eq!(got, v1_image(), "v1 structural");
                assert!(got.0.is_some(), "nonzero max_ts header");
            }
            other => panic!(
                "raftkv-image fixture v{other} has no expected value — add one (ADR 0073 checklist step 4)"
            ),
        }
    }
}

#[test]
fn raftkv_image_round_trips_and_matches_the_fixture_bytes() {
    for (version, bytes) in fixture_files(&formats_dir("raftkv-image")) {
        if version != 1 {
            continue;
        }
        let (max_ts, rows) = codec::decode_image(&bytes).expect("decode");
        assert_eq!(codec::encode_image(&rows, max_ts), bytes, "v1 round trip");
        let (m, r) = v1_image();
        assert_eq!(
            codec::encode_image(&r, m),
            bytes,
            "v1: current encoder emits the fixture"
        );
    }
}

/// `cargo test -p animus-cp-data --lib generate_fixture_raftkv_image -- --ignored`.
#[test]
#[ignore]
fn generate_fixture_raftkv_image() {
    let (m, r) = v1_image();
    write_new_fixture(&formats_dir("raftkv-image"), 1, &codec::encode_image(&r, m));
}

// ---------------------------------------------------------------------------
// raftkv-wal

type Rec = WalRecord<KvCommand, KvState>;

/// Hard state, one `Append` per `KvCommand` variant, a `Truncate`, a
/// `Snapshot` with a voter/learner config, and a `Snapshot` without.
fn v1_wal_records() -> Vec<Rec> {
    let mut recs: Vec<Rec> = vec![Rec::Hard {
        term: 7,
        voted_for: Some(nid(2)),
    }];
    recs.extend(sample_entries().into_iter().map(Rec::Append));
    recs.push(Rec::Truncate { keep: 3 });
    recs.push(Rec::Snapshot {
        metadata: KvState,
        last_index: 20,
        last_term: 6,
        config: Some([1, 2, 3].into_iter().map(nid).collect()),
        learners: Some([4].into_iter().map(nid).collect()),
    });
    recs.push(Rec::Snapshot {
        metadata: KvState,
        last_index: 25,
        last_term: 7,
        config: None,
        learners: None,
    });
    recs.push(Rec::Hard {
        term: 8,
        voted_for: None,
    });
    recs
}

/// One `CWL1` **version 1** line, exactly as a v1 writer framed it (the
/// current `encode_record` now writes v2): the legacy encoder for this
/// fixture (ADR 0073 checklist step 7), anchored by the byte-equality test
/// below.
fn v1_wal_line(r: &Rec) -> Vec<u8> {
    const V1: animus_control::format::FormatTag = animus_control::format::FormatTag {
        magic: *b"CWL1",
        version: 1,
        name: "control-wal",
    };
    animus_control::format::encode_line(&V1, &serde_json::to_vec(r).expect("serializes"))
}

fn v1_wal_bytes() -> Vec<u8> {
    v1_wal_records().iter().flat_map(v1_wal_line).collect()
}

/// The v2 layout (#1132): the same records as two persist rounds, each
/// followed by its sync marker at the file length it claims.
fn v2_wal_bytes() -> Vec<u8> {
    let recs = v1_wal_records();
    let mid = recs.len() / 2;
    let mut bytes = Vec::new();
    for round in [&recs[..mid], &recs[mid..]] {
        for r in round {
            bytes.extend(PersistedState::<KvCommand, KvState>::encode_record(r));
        }
        bytes.extend(animus_control::format::encode_sync_marker(
            &animus_control::persist::CONTROL_WAL,
            bytes.len() as u64,
        ));
    }
    bytes
}

#[test]
fn raftkv_wal_decodes_every_checked_in_fixture_structurally() {
    for (version, bytes) in fixture_files(&formats_dir("raftkv-wal")) {
        let got = PersistedState::<KvCommand, KvState>::decode(&bytes)
            .unwrap_or_else(|e| panic!("v{version}: {e}"));
        match version {
            1 => {
                assert_eq!(got, v1_wal_records(), "v1 structural");
                let appended = got.iter().filter(|r| matches!(r, Rec::Append(_))).count();
                assert_eq!(appended, sample_entries().len());
            }
            2 => assert_eq!(got, v1_wal_records(), "v2 structural (markers consumed)"),
            other => panic!(
                "raftkv-wal fixture v{other} has no expected value — add one (ADR 0073 checklist step 4)"
            ),
        }
    }
}

#[test]
fn raftkv_wal_round_trips_and_matches_the_fixture_bytes() {
    for (version, bytes) in fixture_files(&formats_dir("raftkv-wal")) {
        match version {
            1 => {
                let recs = PersistedState::<KvCommand, KvState>::decode(&bytes).expect("decode");
                let re: Vec<u8> = recs.iter().flat_map(v1_wal_line).collect();
                assert_eq!(
                    re, bytes,
                    "v1: decode -> legacy encode reproduces the fixture"
                );
                assert_eq!(
                    v1_wal_bytes(),
                    bytes,
                    "v1: legacy encoder emits the fixture"
                );
            }
            2 => assert_eq!(
                v2_wal_bytes(),
                bytes,
                "v2: current encoder + markers emit the fixture"
            ),
            other => panic!("raftkv-wal v{other} fixture has no byte expectation yet"),
        }
    }
}

/// The required fields of `TxnWrite` (ADR 0073 Phase 0 dropped their
/// `#[serde(default)]`): a `TxnStage` JSON write missing `stage_marker` or
/// `pending` is a loud decode error, not a silent `None`.
#[test]
fn raftkv_wal_txn_write_fields_are_required() {
    let line = |v: &serde_json::Value| {
        animus_control::format::encode_line(
            &animus_control::persist::CONTROL_WAL,
            serde_json::to_vec(v).expect("json").as_slice(),
        )
    };
    let recs = v1_wal_records();
    let stage = recs
        .iter()
        .find(|r| matches!(r, Rec::Append(e) if matches!(e.command, KvCommand::TxnStage { .. })))
        .expect("a TxnStage record");
    for field in ["stage_marker", "pending"] {
        let mut v = serde_json::to_value(stage).expect("json");
        let writes = v["Append"]["command"]["TxnStage"]["writes"]
            .as_array_mut()
            .expect("writes");
        writes[0].as_object_mut().expect("obj").remove(field);
        let err = PersistedState::<KvCommand, KvState>::decode(&line(&v)).unwrap_err();
        assert!(
            matches!(err, FormatError::Malformed { .. }),
            "{field}: {err:?}"
        );
    }
}

/// `cargo test -p animus-cp-data --lib generate_fixture_raftkv_wal -- --ignored`.
#[test]
#[ignore]
fn generate_fixture_raftkv_wal() {
    write_new_fixture(
        &formats_dir("raftkv-wal"),
        animus_control::persist::CONTROL_WAL.version,
        &v2_wal_bytes(),
    );
}

// ---------------------------------------------------------------------------
// cp-engine-layout

const LAYOUT_FIXTURE_TABLET: u64 = 7;

fn v1_layout_bytes() -> Vec<u8> {
    pack_frames(&[
        crate::layout::layout_marker_key(LAYOUT_FIXTURE_TABLET),
        crate::layout::encode_layout_value(),
    ])
}

#[test]
fn cp_engine_layout_decodes_every_checked_in_fixture_structurally() {
    for (version, bytes) in fixture_files(&formats_dir("cp-engine-layout")) {
        let parts = unpack_frames(&bytes);
        assert_eq!(parts.len(), 2, "v{version}: key + value");
        let (key, value) = (&parts[0], &parts[1]);
        match version {
            1 => {
                // key = escape(RESERVED_NAMESPACE) || escape("cp_layout") || tablet_be
                let mut want =
                    animus_tablet::escape(animus_control::syskv::RESERVED_NAMESPACE.as_bytes());
                want.extend_from_slice(&animus_tablet::escape(b"cp_layout"));
                want.extend_from_slice(&LAYOUT_FIXTURE_TABLET.to_be_bytes());
                assert_eq!(key, &want, "v1 key layout");
                assert_eq!(value, &[b'K', b'L', b'Y', b'1', 1], "v1 value bytes");
                assert_eq!(crate::layout::decode_layout_value(value), Ok(1));
            }
            other => panic!(
                "cp-engine-layout fixture v{other} has no expected value — add one (ADR 0073 checklist step 4)"
            ),
        }
    }
}

#[test]
fn cp_engine_layout_round_trips_and_matches_the_fixture_bytes() {
    for (version, bytes) in fixture_files(&formats_dir("cp-engine-layout")) {
        if version != 1 {
            continue;
        }
        assert_eq!(
            v1_layout_bytes(),
            bytes,
            "v1: current encoder emits the fixture"
        );
    }
}

#[test]
fn cp_engine_layout_refuses_other_epochs_by_name() {
    let value = unpack_frames(&fixture_files(&formats_dir("cp-engine-layout"))[0].1).remove(1);
    for bad in [0u8, 2, 255] {
        let mut v = value.clone();
        v[4] = bad;
        assert_eq!(
            crate::layout::decode_layout_value(&v).unwrap_err(),
            FormatError::UnsupportedFormatVersion {
                format: "cp-engine-layout",
                found: bad,
                max_supported: 1
            }
        );
    }
}

/// `cargo test -p animus-cp-data --lib generate_fixture_cp_engine_layout -- --ignored`.
/// Refuses to overwrite an existing fixture (ADR 0073 Phase 0).
#[test]
#[ignore]
fn generate_fixture_cp_engine_layout() {
    write_new_fixture(&formats_dir("cp-engine-layout"), 1, &v1_layout_bytes());
}
