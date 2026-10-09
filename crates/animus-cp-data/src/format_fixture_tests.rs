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

use animus_control::Metadata;
use animus_control::format::FormatError;
use animus_control::persist::{PersistedState, WalRecord};
use animus_control::version::{ClusterFeatures, Gate};
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
        // `v<N>-<shape>.bin`: an additive shape inside the same version (ADR
        // 0073 Phase 2), read by its own test, never by a per-version loop.
        if stem.contains('-') {
            continue;
        }
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
    let frames: Vec<Vec<u8>> = sample_wires()
        .iter()
        .map(|w| codec::encode_wire(w, &ClusterFeatures::new()))
        .collect();
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
            .map(|f| {
                codec::encode_wire(
                    &codec::decode_wire(f).expect("decode"),
                    &ClusterFeatures::new(),
                )
            })
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

fn mrec_wire_bytes() -> Vec<u8> {
    let frames: Vec<Vec<u8>> = codec::tests::mrec_sample_wires()
        .iter()
        .map(|w| codec::encode_wire(w, &ClusterFeatures::new()))
        .collect();
    pack_frames(&frames)
}

/// ADR 0075 (G-01 stage G-d): `raftkv-wire/v1-mrec.bin` — the additive MREC
/// content (`WriteSchema.mrec`, `KindEvalOp::Replicate`) inside wire v1. It
/// decodes to the hand-built value, the current encoder reproduces its bytes,
/// and every frame is still a v1 frame (no version bump: the content is JSON
/// inside the envelope, gated by `Gate::MrecReplication` at the propose site).
#[test]
fn raftkv_wire_mrec_shape_fixture_decodes_and_round_trips() {
    let bytes = std::fs::read(formats_dir("raftkv-wire").join("v1-mrec.bin"))
        .expect("raftkv-wire/v1-mrec.bin is checked in");
    let expected = codec::tests::mrec_sample_wires();
    let frames = unpack_frames(&bytes);
    assert_eq!(frames.len(), expected.len());
    for (i, (frame, want)) in frames.iter().zip(&expected).enumerate() {
        assert_eq!((frame[0], frame[1]), (0xCB, 1), "frame {i}: still wire v1");
        let got = codec::decode_wire(frame).unwrap_or_else(|e| panic!("frame {i}: {e}"));
        assert_eq!(format!("{got:?}"), format!("{want:?}"), "frame {i}");
        // The content the gate keys on is really there.
        assert_eq!(got.required_gate(), Gate::MrecReplication, "frame {i}");
    }
    assert_eq!(
        mrec_wire_bytes(),
        bytes,
        "the current encoder emits the fixture"
    );
}

/// Old-input test: the pre-MREC v1 frames carry no MREC content, so every one
/// of them needs only `Base` (the content-dependent `required_gate` must not
/// change any existing entry's gate).
#[test]
fn pre_mrec_wire_fixture_needs_no_gate() {
    for (version, bytes) in fixture_files(&formats_dir("raftkv-wire")) {
        for frame in unpack_frames(&bytes) {
            let w = codec::decode_wire(&frame).expect("decodes");
            assert_eq!(w.required_gate(), Gate::Base, "v{version}");
        }
    }
}

/// `cargo test -p animus-cp-data --lib generate_fixture_raftkv_wire_mrec -- --ignored`.
/// Refuses to overwrite an existing fixture.
#[test]
#[ignore]
fn generate_fixture_raftkv_wire_mrec() {
    let path = formats_dir("raftkv-wire").join("v1-mrec.bin");
    if std::fs::metadata(&path).is_ok() {
        panic!(
            "{} already exists — never regenerated in place",
            path.display()
        );
    }
    std::fs::write(&path, mrec_wire_bytes()).expect("write fixture");
}

fn txn_sealed_wire_bytes() -> Vec<u8> {
    let frames: Vec<Vec<u8>> = codec::tests::txn_sealed_sample_wires()
        .iter()
        .map(|w| codec::encode_wire(w, &ClusterFeatures::new()))
        .collect();
    pack_frames(&frames)
}

/// ADR 0018 (2026-10-09, R-01): `raftkv-wire/v1-txnseal.bin` -- the additive
/// seal-checked decision variants (codec tags 18/19) inside wire v1. Decodes to
/// the hand-built value, the current encoder reproduces its bytes, every frame is
/// still v1, and the content-dependent gate classifies it `TxnSealChecked`.
#[test]
fn raftkv_wire_txn_sealed_shape_fixture_decodes_and_round_trips() {
    let bytes = std::fs::read(formats_dir("raftkv-wire").join("v1-txnseal.bin"))
        .expect("raftkv-wire/v1-txnseal.bin is checked in");
    let expected = codec::tests::txn_sealed_sample_wires();
    let frames = unpack_frames(&bytes);
    assert_eq!(frames.len(), expected.len());
    for (i, (frame, want)) in frames.iter().zip(&expected).enumerate() {
        assert_eq!((frame[0], frame[1]), (0xCB, 1), "frame {i}: still wire v1");
        let got = codec::decode_wire(frame).unwrap_or_else(|e| panic!("frame {i}: {e}"));
        assert_eq!(format!("{got:?}"), format!("{want:?}"), "frame {i}");
        assert_eq!(got.required_gate(), Gate::TxnSealChecked, "frame {i}");
    }
    assert_eq!(
        txn_sealed_wire_bytes(),
        bytes,
        "the current encoder emits the fixture"
    );
}

/// `cargo test -p animus-cp-data --lib generate_fixture_raftkv_wire_txn_sealed -- --ignored`.
/// Refuses to overwrite an existing fixture.
#[test]
#[ignore]
fn generate_fixture_raftkv_wire_txn_sealed() {
    let path = formats_dir("raftkv-wire").join("v1-txnseal.bin");
    if std::fs::metadata(&path).is_ok() {
        panic!(
            "{} already exists — never regenerated in place",
            path.display()
        );
    }
    std::fs::write(&path, txn_sealed_wire_bytes()).expect("write fixture");
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
        assert_eq!(
            codec::encode_image(&rows, max_ts, &ClusterFeatures::new()),
            bytes,
            "v1 round trip"
        );
        let (m, r) = v1_image();
        assert_eq!(
            codec::encode_image(&r, m, &ClusterFeatures::new()),
            bytes,
            "v1: current encoder emits the fixture"
        );
    }
}

/// ADR 0073 Phase 2 (P2-B) byte-identity proof: before the era, a B2 encoder
/// emits **exactly the Phase 1 bytes** for both gate-selected frame kinds, whatever
/// the sender's feature handle says. The handles here are the floor handle
/// (`ClusterFeatures::new()`, what a node holds before it first reads
/// `Metadata`) and one fed from an **era-0** `Metadata` (versioning off, cluster
/// version 1), plus one fed from an era-on `Metadata` (no frame version exists
/// beyond v1 yet, so even an open era must still select v1). The fixtures are the
/// Phase 0/1 `raftkv-wire/v1.bin` and `raftkv-image/v1.bin`, which no P2 change
/// may touch (`scripts/check-format-fixtures.sh`).
#[test]
fn pre_era_encoders_are_byte_identical_to_the_phase1_fixtures_under_every_handle() {
    let era0 = ClusterFeatures::new();
    era0.update(&Metadata::default());
    assert!(!era0.era_active() && era0.cluster_version() == 1);
    let era_on_meta = crate::gates::era_on_metadata();
    let era_on = ClusterFeatures::new();
    era_on.update(&era_on_meta);
    assert!(era_on.era_active());
    let handles = [
        ("floor", ClusterFeatures::new()),
        ("era-0 metadata", era0),
        ("era-on metadata", era_on),
    ];

    let wire_fixture = fixture_files(&formats_dir("raftkv-wire"))
        .into_iter()
        .find(|(v, _)| *v == 1)
        .expect("raftkv-wire/v1.bin")
        .1;
    let image_fixture = fixture_files(&formats_dir("raftkv-image"))
        .into_iter()
        .find(|(v, _)| *v == 1)
        .expect("raftkv-image/v1.bin")
        .1;
    let (max_ts, rows) = v1_image();
    for (name, f) in &handles {
        let frames: Vec<Vec<u8>> = sample_wires()
            .iter()
            .map(|w| codec::encode_wire(w, f))
            .collect();
        assert!(
            frames.iter().all(|fr| fr[0] == 0xCB && fr[1] == 1),
            "{name}"
        );
        assert_eq!(
            pack_frames(&frames),
            wire_fixture,
            "{name}: wire != Phase 1"
        );
        let image = codec::encode_image(&rows, max_ts, f);
        assert_eq!(&image[..2], &[0xCB, 1], "{name}");
        assert_eq!(image, image_fixture, "{name}: image != Phase 1");
    }
}

/// `cargo test -p animus-cp-data --lib generate_fixture_raftkv_image -- --ignored`.
#[test]
#[ignore]
fn generate_fixture_raftkv_image() {
    let (m, r) = v1_image();
    write_new_fixture(
        &formats_dir("raftkv-image"),
        1,
        &codec::encode_image(&r, m, &ClusterFeatures::new()),
    );
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

/// The MREC records of `raftkv-wal/v2-mrec.bin` (ADR 0075 G-d): the entries of
/// [`codec::tests::mrec_sample_wires`]' `AppendEntries` as `Append` records, in
/// one persist round plus its sync marker (the v2 layout). `KvCommand` is
/// stored as `serde_json` in the WAL, so this is where `WriteSchema.mrec` and
/// `KindEvalOp::Replicate` become durable.
fn mrec_wal_records() -> Vec<Rec> {
    let mut recs = vec![Rec::Hard {
        term: 9,
        voted_for: Some(nid(2)),
    }];
    for w in codec::tests::mrec_sample_wires() {
        let KvWire::Raft(animus_control::raft::RaftMsg::AppendEntries { entries, .. }) = w else {
            panic!("mrec_sample_wires is an AppendEntries frame");
        };
        recs.extend(entries.into_iter().map(Rec::Append));
    }
    recs
}

fn mrec_wal_bytes() -> Vec<u8> {
    let mut bytes = Vec::new();
    for r in &mrec_wal_records() {
        bytes.extend(PersistedState::<KvCommand, KvState>::encode_record(r));
    }
    bytes.extend(animus_control::format::encode_sync_marker(
        &animus_control::persist::CONTROL_WAL,
        bytes.len() as u64,
    ));
    bytes
}

#[test]
fn raftkv_wal_mrec_shape_fixture_decodes_and_round_trips() {
    let bytes = std::fs::read(formats_dir("raftkv-wal").join("v2-mrec.bin"))
        .expect("raftkv-wal/v2-mrec.bin is checked in");
    let got = PersistedState::<KvCommand, KvState>::decode(&bytes).expect("decodes");
    assert_eq!(got, mrec_wal_records());
    assert_eq!(
        mrec_wal_bytes(),
        bytes,
        "the current encoder emits the fixture"
    );
}

/// `cargo test -p animus-cp-data --lib generate_fixture_raftkv_wal_mrec -- --ignored`.
#[test]
#[ignore]
fn generate_fixture_raftkv_wal_mrec() {
    let path = formats_dir("raftkv-wal").join("v2-mrec.bin");
    if std::fs::metadata(&path).is_ok() {
        panic!(
            "{} already exists — never regenerated in place",
            path.display()
        );
    }
    std::fs::write(&path, mrec_wal_bytes()).expect("write fixture");
}

/// The seal-checked decision records of `raftkv-wal/v2-txnseal.bin` (ADR 0018,
/// 2026-10-09): the entries of [`codec::tests::txn_sealed_sample_wires`] as
/// `Append` records in one persist round plus its sync marker (the v2 layout).
fn txn_sealed_wal_records() -> Vec<Rec> {
    let mut recs = vec![Rec::Hard {
        term: 11,
        voted_for: Some(nid(2)),
    }];
    for w in codec::tests::txn_sealed_sample_wires() {
        let KvWire::Raft(animus_control::raft::RaftMsg::AppendEntries { entries, .. }) = w else {
            panic!("txn_sealed_sample_wires is an AppendEntries frame");
        };
        recs.extend(entries.into_iter().map(Rec::Append));
    }
    recs
}

fn txn_sealed_wal_bytes() -> Vec<u8> {
    let mut bytes = Vec::new();
    for r in &txn_sealed_wal_records() {
        bytes.extend(PersistedState::<KvCommand, KvState>::encode_record(r));
    }
    bytes.extend(animus_control::format::encode_sync_marker(
        &animus_control::persist::CONTROL_WAL,
        bytes.len() as u64,
    ));
    bytes
}

#[test]
fn raftkv_wal_txn_sealed_shape_fixture_decodes_and_round_trips() {
    let bytes = std::fs::read(formats_dir("raftkv-wal").join("v2-txnseal.bin"))
        .expect("raftkv-wal/v2-txnseal.bin is checked in");
    let got = PersistedState::<KvCommand, KvState>::decode(&bytes).expect("decodes");
    assert_eq!(got, txn_sealed_wal_records());
    assert_eq!(
        txn_sealed_wal_bytes(),
        bytes,
        "the current encoder emits the fixture"
    );
}

/// `cargo test -p animus-cp-data --lib generate_fixture_raftkv_wal_txn_sealed -- --ignored`.
#[test]
#[ignore]
fn generate_fixture_raftkv_wal_txn_sealed() {
    let path = formats_dir("raftkv-wal").join("v2-txnseal.bin");
    if std::fs::metadata(&path).is_ok() {
        panic!(
            "{} already exists — never regenerated in place",
            path.display()
        );
    }
    std::fs::write(&path, txn_sealed_wal_bytes()).expect("write fixture");
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

// ---------------------------------------------------------------------------
// txn-envelope (ADR 0018 §2's 2026-10-04 amendment)
//
// The 1-byte-tagged value envelope every base-row value is wrapped in
// (`txn.rs`). Tag `0` = committed (every version); tag `1` = a v1 intent
// (retired, `txn::legacy::v1`: no prior value); tag `2` = a v2 intent (the v1
// body plus a trailing `prior` — the committed value the intent shadows).
// Container: `u32`-BE-length-prefixed envelope values, to EOF.

fn txn_fixture_id() -> crate::txn::TxnId {
    crate::txn::TxnId {
        ts: HlcTimestamp {
            wall_ms: 1_700_000_000_123,
            logical: 7,
        },
        node: nid(3),
    }
}

fn txn_fixture_record_key() -> Vec<u8> {
    crate::txn::record_key(&[0xA5; animus_tablet::TOKEN_BYTES], &txn_fixture_id())
}

fn txn_fixture_kind_writes() -> Vec<crate::KindWrite> {
    vec![
        (1u8, b"lsi-key".to_vec(), Some(b"lsi-row".to_vec())),
        (1u8, b"lsi-old".to_vec(), None),
    ]
}

fn txn_fixture_change_log() -> (Vec<u8>, Vec<u8>) {
    (b"change-prefix".to_vec(), b"change-record".to_vec())
}

fn v1_txn_envelope_bytes() -> Vec<u8> {
    use crate::txn;
    let (id, rk) = (txn_fixture_id(), txn_fixture_record_key());
    pack_frames(&[
        txn::encode_committed(b"hello"),
        txn::encode_committed(b""),
        txn::legacy::v1::encode_intent(
            &id,
            &rk,
            "orders",
            Some(b"staged"),
            &txn_fixture_kind_writes(),
            Some(&txn_fixture_change_log()),
        ),
        txn::legacy::v1::encode_intent(&id, &rk, "orders", None, &[], None),
    ])
}

fn v2_txn_envelope_bytes() -> Vec<u8> {
    use crate::txn;
    let (id, rk) = (txn_fixture_id(), txn_fixture_record_key());
    pack_frames(&[
        txn::encode_committed(b"hello"),
        txn::encode_committed(b""),
        txn::encode_intent(
            &id,
            &rk,
            "orders",
            Some(b"staged"),
            &txn_fixture_kind_writes(),
            Some(&txn_fixture_change_log()),
            Some(b"was"),
        ),
        txn::encode_intent(&id, &rk, "orders", None, &[], None, None),
        txn::encode_intent(&id, &rk, "orders", Some(b"x"), &[], None, Some(b"")),
    ])
}

fn txn_intent(
    staged: Option<&[u8]>,
    with_derived: bool,
    prior: crate::txn::IntentPrior,
) -> crate::txn::Envelope {
    crate::txn::Envelope::Intent {
        txn_id: txn_fixture_id(),
        record_key: txn_fixture_record_key(),
        record_table: "orders".to_string(),
        staged_value: staged.map(<[u8]>::to_vec),
        kind_writes: if with_derived {
            txn_fixture_kind_writes()
        } else {
            Vec::new()
        },
        change_log: with_derived.then(txn_fixture_change_log),
        prior,
    }
}

#[test]
fn txn_envelope_decodes_every_checked_in_fixture_structurally() {
    use crate::txn::{Envelope, IntentPrior, decode_envelope};
    for (version, bytes) in fixture_files(&formats_dir("txn-envelope")) {
        let decoded: Vec<Envelope> = unpack_frames(&bytes)
            .iter()
            .map(|b| decode_envelope(b))
            .collect();
        let expected = match version {
            // A v1 intent translates to the current shape with an unknown
            // prior: the reader falls back to the MVCC lookback (step 6).
            1 => vec![
                Envelope::Committed(b"hello".to_vec()),
                Envelope::Committed(Vec::new()),
                txn_intent(Some(b"staged"), true, IntentPrior::Unknown),
                txn_intent(None, false, IntentPrior::Unknown),
            ],
            2 => vec![
                Envelope::Committed(b"hello".to_vec()),
                Envelope::Committed(Vec::new()),
                txn_intent(
                    Some(b"staged"),
                    true,
                    IntentPrior::Known(Some(b"was".to_vec())),
                ),
                txn_intent(None, false, IntentPrior::Known(None)),
                txn_intent(Some(b"x"), false, IntentPrior::Known(Some(Vec::new()))),
            ],
            other => panic!(
                "txn-envelope fixture v{other} has no expected value — add one (ADR 0073 checklist step 4)"
            ),
        };
        assert_eq!(decoded, expected, "txn-envelope v{version}");
    }
}

#[test]
fn txn_envelope_encoders_match_the_fixture_bytes() {
    for (version, bytes) in fixture_files(&formats_dir("txn-envelope")) {
        let want = match version {
            // Checklist step 7: the `legacy-encoders` v1 encoder reproduces v1.
            1 => v1_txn_envelope_bytes(),
            // Checklist step 5: the current writer reproduces v2.
            2 => v2_txn_envelope_bytes(),
            other => panic!("txn-envelope fixture v{other} has no encoder arm"),
        };
        assert_eq!(
            want, bytes,
            "txn-envelope v{version}: encoder emits the fixture"
        );
    }
}

/// `cargo test -p animus-cp-data --lib generate_fixture_txn_envelope -- --ignored`.
/// Refuses to overwrite an existing fixture (ADR 0073 Phase 0).
#[test]
#[ignore]
fn generate_fixture_txn_envelope() {
    let dir = formats_dir("txn-envelope");
    if !dir.join("v1.bin").exists() {
        write_new_fixture(&dir, 1, &v1_txn_envelope_bytes());
    }
    write_new_fixture(&dir, 2, &v2_txn_envelope_bytes());
}

/// The harness's engine-row transcode (`animus-test`'s `ROW_TABLE`): a v2 intent
/// down-converts to exactly the v1 bytes the `legacy-encoders` v1 encoder
/// writes for the same fields (which `txn_envelope_encoders_match_the_fixture_bytes`
/// anchors to `v1.bin`), and anything that is not exactly one v2 intent is left
/// alone.
#[test]
fn txn_envelope_v2_intents_downgrade_to_the_v1_fixture_bytes() {
    use crate::downgrade_txn_envelope_to_v1 as down;
    let v1 = unpack_frames(&std::fs::read(formats_dir("txn-envelope").join("v1.bin")).unwrap());
    let v2 = unpack_frames(&std::fs::read(formats_dir("txn-envelope").join("v2.bin")).unwrap());
    // Frames 0/1 are committed values: untouched. Frames 2/3 of v2 are the
    // v1 fixture's frames 2/3 plus a prior.
    assert_eq!(down(&v2[0]), None);
    assert_eq!(down(&v2[1]), None);
    assert_eq!(down(&v2[2]).as_deref(), Some(v1[2].as_slice()));
    assert_eq!(down(&v2[3]).as_deref(), Some(v1[3].as_slice()));
    // Frame 4 (staged `x`, prior empty) has no v1 fixture twin: it must equal
    // the v1 encoder over the same fields.
    let (id, rk) = (txn_fixture_id(), txn_fixture_record_key());
    let want = crate::txn::legacy::v1::encode_intent(&id, &rk, "orders", Some(b"x"), &[], None);
    assert_eq!(down(&v2[4]), Some(want));
    // Not a v2 intent: left alone (`None`), never rewritten or panicked on.
    for not_v2 in [&v1[2][..], &[2u8][..], &[2u8, 1, 2, 3][..], &[][..]] {
        assert_eq!(down(not_v2), None, "{not_v2:02x?}");
    }
    // A v2 intent with a byte appended or removed is not exactly one intent.
    let mut longer = v2[2].clone();
    longer.push(0);
    assert_eq!(down(&longer), None);
    assert_eq!(down(&v2[2][..v2[2].len() - 1]), None);
}

// ---------------------------------------------------------------------------
// txn-resolved-marker (issue #1243)
//
// The durable per-key row `TxnResolve`'s apply writes beside every resolved
// intent so `TxnStage`'s apply can reject a stale/duplicate stage
// deterministically (`txn::resolved_marker_key`). Container: two
// `u32`-BE-length-prefixed frames — the marker's logical key, then its value
// (`[0xA1] || txn_id`).

fn resolved_marker_base_key() -> Vec<u8> {
    let mut k = vec![0xA5; animus_tablet::TOKEN_BYTES];
    k.extend_from_slice(b"pk\x00\x00row");
    k
}

fn v1_resolved_marker_bytes() -> Vec<u8> {
    use crate::txn;
    pack_frames(&[
        txn::resolved_marker_key(&resolved_marker_base_key()),
        txn::encode_resolved_marker(&txn_fixture_id()),
    ])
}

#[test]
fn txn_resolved_marker_decodes_every_checked_in_fixture() {
    use crate::txn;
    for (version, bytes) in fixture_files(&formats_dir("txn-resolved-marker")) {
        let frames = unpack_frames(&bytes);
        match version {
            1 => {
                assert_eq!(frames.len(), 2, "txn-resolved-marker v1 frame count");
                assert_eq!(
                    frames[0],
                    txn::resolved_marker_key(&resolved_marker_base_key())
                );
                assert!(txn::is_resolved_marker_key(&frames[0]));
                assert!(!txn::is_record_key(&frames[0]));
                assert_eq!(
                    txn::decode_resolved_marker(&frames[1]),
                    Some(txn_fixture_id())
                );
            }
            other => panic!(
                "txn-resolved-marker fixture v{other} has no expected value — add one (ADR 0073 checklist step 4)"
            ),
        }
    }
}

#[test]
fn txn_resolved_marker_encoder_matches_the_fixture_bytes() {
    for (version, bytes) in fixture_files(&formats_dir("txn-resolved-marker")) {
        let want = match version {
            1 => v1_resolved_marker_bytes(),
            other => panic!("txn-resolved-marker fixture v{other} has no encoder arm"),
        };
        assert_eq!(
            want, bytes,
            "txn-resolved-marker v{version}: encoder emits the fixture"
        );
    }
}

/// `cargo test -p animus-cp-data --lib generate_fixture_txn_resolved_marker -- --ignored`.
/// Refuses to overwrite an existing fixture (ADR 0073 Phase 0).
#[test]
#[ignore]
fn generate_fixture_txn_resolved_marker() {
    write_new_fixture(
        &formats_dir("txn-resolved-marker"),
        1,
        &v1_resolved_marker_bytes(),
    );
}
