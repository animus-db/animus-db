//! Golden fixtures for `animus-cp-data`'s durable/wire formats (ADR 0073
//! Phase 0, workstream C). Later layers of workstream C (RaftKV codec,
//! `SharedWal` envelope, key-layout marker) add their own sections here.
//! The RaftKV wire/image/WAL fixtures (`raftkv-*`) are tested in-crate instead
//! (`src/format_fixture_tests.rs`): the wire/image codec is `pub(crate)`.
//!
//! One fixture per version under
//! `tests/fixtures/formats/<format>/v<N>.bin`, decoded structurally against
//! a hand-written expected value (never just "decodes without error"), plus
//! a round-trip test and an `#[ignore]`d generator that refuses to overwrite
//! an existing fixture. Convention: `animus-control/src/format.rs`.

use std::path::{Path, PathBuf};

use animus_control::format::FormatError;
use animus_cp_data::segment::{self, Segment, SegmentHeader, SegmentRecord};

fn formats_dir(format: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/formats")
        .join(format)
}

/// Every `v<N>.bin` in `dir`, sorted by name, as `(version, bytes)`.
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

/// Refuse-to-overwrite fixture writer shared by every generator.
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
// `segment` — the stream-shard segment codec (`SEGF`, `segment::VERSION`).

/// Fixed, hand-chosen content only (ADR 0073 determinism rule). Covers:
/// a `Some` parent shard id, a tied `packed_hlc` group with distinct
/// `ordinal`s (issue #852), a non-ASCII table/label, a binary
/// (non-UTF-8, token-leading) `source_key`, an empty and a non-empty
/// opaque `change_record`, and `u64`-extreme HLC values.
fn v1_segment() -> Segment {
    let records = vec![
        SegmentRecord {
            source_key: vec![
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, b'p', b'k', 0x00,
            ],
            packed_hlc: 1_000_000,
            ordinal: 0,
            change_record: b"change-record-1".to_vec(),
        },
        SegmentRecord {
            source_key: vec![0xFF, 0xFE, 0x80, b'k', b'2'],
            packed_hlc: 2_000_000,
            ordinal: 0,
            change_record: Vec::new(),
        },
        SegmentRecord {
            source_key: b"tied-a".to_vec(),
            packed_hlc: 2_000_000,
            ordinal: 1,
            change_record: vec![0, 1, 2, 3, 255],
        },
        SegmentRecord {
            source_key: b"tied-b".to_vec(),
            packed_hlc: 2_000_000,
            ordinal: 2,
            change_record: b"{\"json\":\"opaque\"}".to_vec(),
        },
        SegmentRecord {
            source_key: b"last".to_vec(),
            packed_hlc: u64::MAX - 1,
            ordinal: u32::MAX,
            change_record: b"end".to_vec(),
        },
    ];
    Segment {
        header: SegmentHeader {
            table: "orders-\u{00e9}".to_owned(),
            label: "2026-01-01T00:00:00Z".to_owned(),
            shard_id: segment::shard_id(42, 3),
            tablet: 42,
            epoch: 3,
            parent_shard_id: Some(segment::shard_id(42, 2)),
            hlc_range: (999_999, u64::MAX - 1),
            count: 5,
            seal_wall_ms: 1_767_225_600_000,
        },
        records,
    }
}

/// A second, minimal shape: no parent, empty body.
fn v1_root_segment() -> Segment {
    Segment {
        header: SegmentHeader {
            table: "t".to_owned(),
            label: "l".to_owned(),
            shard_id: segment::shard_id(1, 0),
            tablet: 1,
            epoch: 0,
            parent_shard_id: None,
            hlc_range: (0, 0),
            count: 0,
            seal_wall_ms: 0,
        },
        records: Vec::new(),
    }
}

#[test]
fn segment_decodes_every_checked_in_fixture_structurally() {
    for (version, bytes) in fixture_files(&formats_dir("segment")) {
        assert_eq!(&bytes[..4], b"SEGF", "v{version}: magic");
        assert_eq!(bytes[4], version, "v{version}: version byte matches name");
        let decoded = segment::decode(&bytes).unwrap_or_else(|e| panic!("v{version}: {e}"));
        match version {
            1 => {
                assert_eq!(decoded, v1_segment(), "v1 structural");
                let h = &decoded.header;
                assert_eq!(h.count as usize, decoded.records.len());
                assert_eq!(h.parent_shard_id.as_deref(), Some("shardId-42-2"));
                // The tied group is preserved in (packed_hlc, ordinal) order.
                let keys: Vec<_> = decoded
                    .records
                    .iter()
                    .map(|r| (r.packed_hlc, r.ordinal))
                    .collect();
                let mut sorted = keys.clone();
                sorted.sort();
                assert_eq!(keys, sorted);
                assert_eq!(decoded.records[1].change_record, Vec::<u8>::new());
                // The superset-slice rule over the fixture keeps everything
                // inside its own committed range.
                let (_, sliced) = segment::decode_and_slice(&bytes, h.hlc_range).expect("slice");
                assert_eq!(sliced, decoded.records);
            }
            other => panic!(
                "fixture v{other} has no expected value — add one (ADR 0073 checklist step 4)"
            ),
        }
    }
}

#[test]
fn segment_round_trips_through_encode_and_decode() {
    for seg in [v1_segment(), v1_root_segment()] {
        let bytes = segment::encode(&seg.header, &seg.records);
        assert_eq!(segment::decode(&bytes).expect("decode"), seg);
    }
    // decode -> encode reproduces the checked-in bytes exactly.
    for (version, bytes) in fixture_files(&formats_dir("segment")) {
        if version == segment::VERSION {
            let decoded = segment::decode(&bytes).expect("decode");
            assert_eq!(
                segment::encode(&decoded.header, &decoded.records),
                bytes,
                "v{version}: decode -> encode must reproduce the fixture"
            );
        }
    }
}

#[test]
fn segment_fixture_is_current_version_and_refuses_pre_baseline_shapes() {
    // A fixture for the current `VERSION` must exist (ADR 0073 checklist
    // step 3); this deliberately does not pin `VERSION == 1`, so a bump
    // only fails here until its new fixture is added.
    let (_, bytes) = fixture_files(&formats_dir("segment"))
        .into_iter()
        .find(|(v, _)| *v == segment::VERSION)
        .unwrap_or_else(|| panic!("no fixture for segment::VERSION {}", segment::VERSION));
    // A version above the current one is refused by name.
    let mut old = bytes.clone();
    old[4] = segment::VERSION + 1;
    assert_eq!(
        segment::decode(&old).expect_err("v2 refused"),
        FormatError::UnsupportedFormatVersion {
            format: "segment",
            found: segment::VERSION + 1,
            max_supported: segment::VERSION
        }
    );
    // Version 0 is never valid.
    let mut zero = bytes.clone();
    zero[4] = 0;
    assert_eq!(
        segment::decode(&zero).expect_err("v0 refused"),
        FormatError::UnsupportedFormatVersion {
            format: "segment",
            found: 0,
            max_supported: segment::VERSION
        }
    );
    // Untagged input (the body without magic+version) is pre-baseline.
    assert_eq!(
        segment::decode(&bytes[5..]).expect_err("untagged refused"),
        FormatError::PreBaselineFormat { format: "segment" }
    );
}

/// Regenerates `segment/v<VERSION>.bin` with the *current* encoder:
/// `cargo test -p animus-cp-data --test it format_fixtures::generate_fixture_segment -- --ignored`.
/// Refuses to overwrite an existing fixture (ADR 0073 Phase 0).
#[test]
#[ignore]
fn generate_fixture_segment() {
    let seg = v1_segment();
    let bytes = segment::encode(&seg.header, &seg.records);
    write_new_fixture(&formats_dir("segment"), segment::VERSION, &bytes);
}
