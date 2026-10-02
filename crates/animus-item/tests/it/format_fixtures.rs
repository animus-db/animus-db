//! Golden fixtures for the row-value formats (ADR 0073 Phase 1, workstream
//! P1-A): `stored-item` (the stored-item codec's bytes) and `change-record`
//! (the serialized `ChangeRecord`). Both are **untagged JSON** at v1 — see
//! `stored_item_version` / `ChangeRecord::version_of` — and their serde shape
//! (including `AttributeValue`/`Item`) is frozen: additive-only, each change
//! with a new fixture file. Layout and discipline follow ADR 0073's Phase 0
//! conventions: `tests/fixtures/formats/<format>/vN.json`, iterated (never
//! named literally), a hand-written expected value per version that panics on
//! a fixture with no expectation, a round trip, and an `#[ignore]`d
//! no-overwrite generator. `scripts/check-format-fixtures.sh` guards against
//! edits/deletions.

use animus_item::{
    AttributeValue, ChangeRecord, Item, decode_stored_item, encode_stored_item, encode_tombstone,
    stored::stored_item_version,
};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

fn fixtures_dir(format: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/formats")
        .join(format)
}

/// Every `vN.json` under `format`'s directory as `(N, bytes)`, sorted.
fn fixtures(format: &str) -> Vec<(u32, Vec<u8>)> {
    let dir = fixtures_dir(format);
    let mut out: Vec<(u32, Vec<u8>)> = fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("reading fixture dir {}: {e}", dir.display()))
        .map(|e| e.expect("dir entry").path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "json"))
        .map(|p| {
            let stem = p.file_stem().and_then(|s| s.to_str()).expect("utf8 name");
            let version = stem
                .strip_prefix('v')
                .and_then(|n| n.parse().ok())
                .unwrap_or_else(|| panic!("fixture {} is not named vN.json", p.display()));
            (
                version,
                fs::read(&p).unwrap_or_else(|e| panic!("reading {}: {e}", p.display())),
            )
        })
        .collect();
    out.sort();
    assert!(
        !out.is_empty(),
        "no vN.json fixtures under {}",
        dir.display()
    );
    out
}

#[allow(
    clippy::disallowed_methods,
    reason = "test-only fixture generator: std::fs is fine in tests (repo convention), and this \
              is the one sanctioned place a golden fixture is ever written"
)]
fn write_new_fixture(path: &Path, bytes: &[u8]) {
    if let Ok(meta) = fs::metadata(path) {
        panic!(
            "refusing to overwrite existing fixture {} ({} bytes on disk) — \
             add a new vN.json instead of regenerating this one",
            path.display(),
            meta.len()
        );
    }
    fs::create_dir_all(path.parent().expect("fixture path has a parent dir"))
        .unwrap_or_else(|e| panic!("creating fixture dir for {}: {e}", path.display()));
    fs::write(path, bytes).unwrap_or_else(|e| panic!("writing fixture {}: {e}", path.display()));
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.into())
}

/// The representative item: one attribute per `AttributeValue` variant, with
/// nesting (a map inside a list inside a map) and non-ASCII / empty / binary
/// edge values.
fn representative_item() -> Item {
    let mut inner = BTreeMap::new();
    inner.insert("k".to_string(), s("v"));
    inner.insert("n".to_string(), AttributeValue::N("-1.5E+3".into()));
    let mut item = Item::new();
    item.insert("s".into(), s("héllo \u{1F600}"));
    item.insert("n".into(), AttributeValue::N("123.456".into()));
    item.insert("b".into(), AttributeValue::B(vec![0, 1, 2, 255]));
    item.insert("bool".into(), AttributeValue::Bool(true));
    item.insert("null".into(), AttributeValue::Null);
    item.insert(
        "m".into(),
        AttributeValue::M(BTreeMap::from([(
            "nested".to_string(),
            AttributeValue::L(vec![
                s(""),
                AttributeValue::Bool(false),
                AttributeValue::M(inner),
            ]),
        )])),
    );
    item.insert(
        "l".into(),
        AttributeValue::L(vec![s("a"), AttributeValue::Null]),
    );
    item.insert(
        "ss".into(),
        AttributeValue::SS(vec!["a".into(), "b".into()]),
    );
    item.insert(
        "ns".into(),
        AttributeValue::NS(vec!["1".into(), "2.5".into()]),
    );
    item.insert("bs".into(), AttributeValue::BS(vec![vec![], vec![0, 255]]));
    item
}

/// The expected decoded value of each checked-in `stored-item` fixture. A
/// fixture version with no arm here panics: adding a version means adding its
/// expectation (ADR 0073 checklist step 4).
fn expected_stored_item(version: u32) -> Item {
    match version {
        1 => representative_item(),
        other => panic!("stored-item fixture v{other} has no hand-written expected value"),
    }
}

fn representative_change_record() -> ChangeRecord {
    let mut old = Item::new();
    old.insert("id".into(), s("u1"));
    old.insert("n".into(), AttributeValue::N("1".into()));
    ChangeRecord {
        base_sk: vec![0x00, 0x01, 0xff],
        old_image: Some(old),
        new_image: Some(representative_item()),
        seeded: true,
        marker: true,
        staged: true,
        ttl_expired: true,
    }
}

fn expected_change_record(version: u32) -> ChangeRecord {
    match version {
        1 => representative_change_record(),
        other => panic!("change-record fixture v{other} has no hand-written expected value"),
    }
}

#[test]
fn stored_item_fixtures_decode_to_the_expected_value() {
    for (version, bytes) in fixtures("stored-item") {
        assert_eq!(
            stored_item_version(&bytes),
            Some(version),
            "sniffed version must equal the fixture's version (v{version})"
        );
        let decoded = decode_stored_item(&bytes)
            .unwrap_or_else(|e| panic!("stored-item v{version} fails to decode: {e}"));
        assert_eq!(decoded, Some(expected_stored_item(version)), "v{version}");
    }
}

#[test]
fn stored_item_round_trips() {
    let item = representative_item();
    let bytes = encode_stored_item(&item);
    assert_eq!(decode_stored_item(&bytes).unwrap(), Some(item));
}

#[test]
fn stored_item_tombstone_v1_bytes_are_pinned() {
    // The tombstone is too small to earn its own fixture file; pin its exact
    // v1 bytes here instead.
    assert_eq!(encode_tombstone(), br#""tombstone""#);
    assert_eq!(decode_stored_item(br#""tombstone""#).unwrap(), None);
}

#[test]
fn change_record_fixtures_decode_to_the_expected_value() {
    for (version, bytes) in fixtures("change-record") {
        assert_eq!(
            ChangeRecord::version_of(&bytes),
            Some(version),
            "sniffed version must equal the fixture's version (v{version})"
        );
        let decoded = ChangeRecord::decode(&bytes)
            .unwrap_or_else(|| panic!("change-record v{version} fails to decode"));
        assert_eq!(decoded, expected_change_record(version), "v{version}");
    }
}

#[test]
fn change_record_round_trips() {
    let rec = representative_change_record();
    assert_eq!(ChangeRecord::decode(&rec.encode()), Some(rec));
}

#[test]
fn change_record_written_before_the_flag_fields_existed_decodes_as_a_real_write() {
    // The `#[serde(default)]` additions (seeded / marker / staged /
    // ttl_expired, and `base_sk`) must keep decoding an older record as a
    // plain client write — exactly what an old writer meant.
    let old = br#"{"old_image":null,"new_image":{"id":{"S":"u1"}}}"#;
    let rec = ChangeRecord::decode(old).expect("pre-flag record decodes");
    assert!(rec.base_sk.is_empty());
    assert!(!rec.seeded && !rec.marker && !rec.staged && !rec.ttl_expired);
    assert_eq!(rec.event_name(), "INSERT");
}

#[test]
fn unrecognised_encodings_are_not_v1() {
    assert_eq!(ChangeRecord::version_of(b"\x00CHR2{}"), None);
    assert_eq!(ChangeRecord::decode(b"\x00CHR2{}"), None);
    assert_eq!(ChangeRecord::version_of(b""), None);
}

#[test]
#[ignore = "run explicitly to (re)generate a fixture: cargo test -p animus-item --test it format_fixtures::generate_fixture_stored_item -- --ignored"]
fn generate_fixture_stored_item() {
    write_new_fixture(
        &fixtures_dir("stored-item").join("v1.json"),
        &encode_stored_item(&representative_item()),
    );
}

#[test]
#[ignore = "run explicitly to (re)generate a fixture: cargo test -p animus-item --test it format_fixtures::generate_fixture_change_record -- --ignored"]
fn generate_fixture_change_record() {
    write_new_fixture(
        &fixtures_dir("change-record").join("v1.json"),
        &representative_change_record().encode(),
    );
}
