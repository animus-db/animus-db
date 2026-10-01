//! Golden fixtures for the backup formats (ADR 0073 Phase 0, workstream E
//! layer 3; ADR 0059 §2): the manifest object (`BKMF` + `u8` envelope, JSON
//! body — the envelope *is* the format tag, so the body carries no `"v"`)
//! and the data chunk (`BKDT` + `u8`, length-prefixed rows).
//!
//! Lives beside, not inside, `format_fixtures.rs` (workstream C's file).
//! One fixture per version under `tests/fixtures/formats/<format>/v<N>.bin`,
//! decoded structurally, plus round-trip, rejection, and `#[ignore]`d
//! generators that refuse to overwrite.

use std::path::{Path, PathBuf};

use animus_control::format::FormatError;
use animus_control::{
    BackupManifest, BackupPinnedTablet, BackupTabletProgress, ColumnType, TableSchema,
};
use animus_cp_data::backup::{
    BackupManifestObject, BackupManifestTabletEntry, DATA_VERSION, MANIFEST_VERSION,
    decode_data_chunk, decode_manifest_object, encode_data_chunk, encode_manifest_object,
};
use animus_cp_data::{KIND_BASE, KIND_FOOTPRINT, KIND_LSI, SeedRow};
use animus_tablet::{KeyRange, TabletId};

fn formats_dir(format: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/formats")
        .join(format)
}

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
             bump the format's version and add a new fixture file instead",
            path.display()
        );
    }
    std::fs::write(&path, bytes).unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
}

// --- backup-manifest ---------------------------------------------------------

/// Fixed content only: a bounded and a whole-keyspace pinned range (one with
/// non-UTF-8 bounds), a fixed wall-clock constant, `u64`-extreme progress.
fn v1_manifest() -> BackupManifestObject {
    BackupManifestObject {
        manifest: BackupManifest {
            schema: TableSchema::simple("pk", ColumnType::String),
            pinned_tablets: vec![
                BackupPinnedTablet {
                    tablet: TabletId(7),
                    range: KeyRange::new(vec![0x00, 0x01], Some(vec![0x80, 0xFF])),
                },
                BackupPinnedTablet {
                    tablet: TabletId(9),
                    range: KeyRange::whole(),
                },
            ],
            created_wall_ms: 1_790_000_000_000,
        },
        tablet_progress: vec![
            BackupManifestTabletEntry {
                tablet: TabletId(7),
                progress: BackupTabletProgress {
                    cut_version: 4_242,
                    bytes: 1_000,
                    chunk_count: 3,
                },
            },
            BackupManifestTabletEntry {
                tablet: TabletId(9),
                progress: BackupTabletProgress {
                    cut_version: u64::MAX,
                    bytes: u64::MAX / 2,
                    chunk_count: 0,
                },
            },
        ],
    }
}

#[test]
fn backup_manifest_fixtures_decode_structurally() {
    for (version, bytes) in fixture_files(&formats_dir("backup-manifest")) {
        assert_eq!(&bytes[..4], b"BKMF", "v{version} magic");
        assert_eq!(bytes[4], version, "v{version} envelope version byte");
        let obj = decode_manifest_object(&bytes).unwrap_or_else(|e| panic!("v{version}: {e}"));
        match version {
            1 => {
                let want = v1_manifest();
                assert_eq!(obj.manifest.created_wall_ms, 1_790_000_000_000);
                assert_eq!(obj.manifest.schema, want.manifest.schema);
                assert_eq!(obj.manifest.pinned_tablets.len(), 2);
                assert_eq!(obj.manifest.pinned_tablets[0].tablet, TabletId(7));
                assert_eq!(
                    obj.manifest.pinned_tablets[0].range,
                    KeyRange::new(vec![0x00, 0x01], Some(vec![0x80, 0xFF]))
                );
                assert_eq!(obj.manifest.pinned_tablets[1].range, KeyRange::whole());
                assert_eq!(obj.tablet_progress.len(), 2);
                assert_eq!(obj.tablet_progress[0].progress.cut_version, 4_242);
                assert_eq!(obj.tablet_progress[0].progress.chunk_count, 3);
                assert_eq!(obj.tablet_progress[1].progress.cut_version, u64::MAX);
                assert_eq!(obj.total_bytes(), 1_000 + u64::MAX / 2);
                assert_eq!(obj, want);
            }
            other => panic!(
                "fixture v{other} has no expected value — add one (ADR 0073 checklist step 4)"
            ),
        }
    }
}

#[test]
fn backup_manifest_round_trips_and_is_tagged_with_current_version() {
    let obj = v1_manifest();
    let bytes = encode_manifest_object(&obj);
    assert_eq!(&bytes[..4], b"BKMF");
    assert_eq!(bytes[4], MANIFEST_VERSION);
    assert_eq!(decode_manifest_object(&bytes).expect("decodes"), obj);
    // The current encoder's output for the baseline content is byte-identical
    // to the checked-in fixture (the on-disk bytes never change).
    if MANIFEST_VERSION == 1 {
        let fixture = std::fs::read(formats_dir("backup-manifest").join("v1.bin")).unwrap();
        assert_eq!(bytes, fixture);
    }
}

#[test]
fn backup_manifest_rejects_untagged_bad_version_and_malformed() {
    let pre = |r: Result<BackupManifestObject, FormatError>| {
        assert_eq!(
            r.expect_err("must reject"),
            FormatError::PreBaselineFormat {
                format: "backup-manifest"
            }
        );
    };
    // Raw JSON with no envelope (the shape a header-less writer would emit).
    let raw = serde_json::to_vec(&v1_manifest()).unwrap();
    pre(decode_manifest_object(&raw));
    pre(decode_manifest_object(&[]));
    pre(decode_manifest_object(b"BKMF")); // magic but no version byte
    pre(decode_manifest_object(b"BKDT\x01{}")); // a foreign magic

    let good = encode_manifest_object(&v1_manifest());
    for bad in [0u8, MANIFEST_VERSION + 1] {
        let mut b = good.clone();
        b[4] = bad;
        assert_eq!(
            decode_manifest_object(&b).expect_err("must reject"),
            FormatError::UnsupportedFormatVersion {
                format: "backup-manifest",
                found: bad,
                max_supported: MANIFEST_VERSION
            }
        );
    }

    let mut malformed = b"BKMF\x01".to_vec();
    malformed.extend_from_slice(b"not json");
    assert!(matches!(
        decode_manifest_object(&malformed),
        Err(FormatError::Malformed {
            format: "backup-manifest",
            ..
        })
    ));
    let mut trailing = good;
    trailing.extend_from_slice(b"garbage");
    assert!(matches!(
        decode_manifest_object(&trailing),
        Err(FormatError::Malformed { .. })
    ));
}

#[test]
#[ignore = "generator: run once by hand to (re)create a NEW fixture; refuses to overwrite"]
fn generate_backup_manifest_v1() {
    write_new_fixture(
        &formats_dir("backup-manifest"),
        1,
        &encode_manifest_object(&v1_manifest()),
    );
}

// --- backup-data -------------------------------------------------------------

/// Fixed rows: every kind a capture writes, a non-UTF-8 key, a tombstone
/// (`None` value), an empty value, and `u64::MAX` version.
fn v1_rows() -> Vec<SeedRow> {
    vec![
        (
            KIND_BASE,
            vec![0x00, 0x00, 0x00, 0x2A, b'p', b'k', 0x00],
            Some(b"value-1".to_vec()),
            100,
        ),
        (KIND_BASE, vec![0xFF, 0xFE, 0x80], None, 101),
        (KIND_LSI, b"lsi-key".to_vec(), Some(Vec::new()), 102),
        (
            KIND_FOOTPRINT,
            b"fp".to_vec(),
            Some(vec![0x00, 0xFF]),
            u64::MAX,
        ),
    ]
}

#[test]
fn backup_data_fixtures_decode_structurally() {
    for (version, bytes) in fixture_files(&formats_dir("backup-data")) {
        assert_eq!(&bytes[..4], b"BKDT", "v{version} magic");
        assert_eq!(bytes[4], version, "v{version} version byte");
        let rows = decode_data_chunk(&bytes).unwrap_or_else(|e| panic!("v{version}: {e}"));
        match version {
            1 => {
                assert_eq!(rows.len(), 4);
                assert_eq!(rows[0].0, KIND_BASE);
                assert_eq!(rows[0].1, vec![0x00, 0x00, 0x00, 0x2A, b'p', b'k', 0x00]);
                assert_eq!(rows[0].2.as_deref(), Some(&b"value-1"[..]));
                assert_eq!(rows[0].3, 100);
                assert_eq!(rows[1].1, vec![0xFF, 0xFE, 0x80]);
                assert!(std::str::from_utf8(&rows[1].1).is_err());
                assert_eq!(rows[1].2, None);
                assert_eq!(rows[2].0, KIND_LSI);
                assert_eq!(rows[2].2, Some(Vec::new()));
                assert_eq!(rows[3].0, KIND_FOOTPRINT);
                assert_eq!(rows[3].3, u64::MAX);
                assert_eq!(rows, v1_rows());
            }
            other => panic!(
                "fixture v{other} has no expected value — add one (ADR 0073 checklist step 4)"
            ),
        }
    }
}

#[test]
fn backup_data_round_trips_and_is_tagged_with_current_version() {
    let rows = v1_rows();
    let bytes = encode_data_chunk(&rows);
    assert_eq!(&bytes[..4], b"BKDT");
    assert_eq!(bytes[4], DATA_VERSION);
    assert_eq!(decode_data_chunk(&bytes).expect("decodes"), rows);
    if DATA_VERSION == 1 {
        let fixture = std::fs::read(formats_dir("backup-data").join("v1.bin")).unwrap();
        assert_eq!(bytes, fixture);
    }
}

#[test]
fn backup_data_rejects_untagged_bad_version_and_malformed() {
    let pre = |r: Result<Vec<SeedRow>, FormatError>| {
        assert_eq!(
            r.expect_err("must reject"),
            FormatError::PreBaselineFormat {
                format: "backup-data"
            }
        );
    };
    pre(decode_data_chunk(&[]));
    pre(decode_data_chunk(b"{\"rows\":[]}"));
    pre(decode_data_chunk(b"BKMF\x01\x00\x00\x00\x00"));

    let good = encode_data_chunk(&v1_rows());
    for bad in [0u8, DATA_VERSION + 1] {
        let mut b = good.clone();
        b[4] = bad;
        assert_eq!(
            decode_data_chunk(&b).expect_err("must reject"),
            FormatError::UnsupportedFormatVersion {
                format: "backup-data",
                found: bad,
                max_supported: DATA_VERSION
            }
        );
    }
    let malformed = |r: Result<Vec<SeedRow>, FormatError>| {
        assert!(
            matches!(
                r,
                Err(FormatError::Malformed {
                    format: "backup-data",
                    ..
                })
            ),
            "{r:?}"
        );
    };
    malformed(decode_data_chunk(&good[..good.len() - 1]));
    malformed(decode_data_chunk(&good[..7])); // header + partial count
    let mut trailing = good;
    trailing.push(0);
    malformed(decode_data_chunk(&trailing));
}

#[test]
#[ignore = "generator: run once by hand to (re)create a NEW fixture; refuses to overwrite"]
fn generate_backup_data_v1() {
    write_new_fixture(
        &formats_dir("backup-data"),
        1,
        &encode_data_chunk(&v1_rows()),
    );
}
