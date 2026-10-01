//! Pinned `AttributeValue::key_bytes` vectors as a checked-in fixture
//! (`tests/fixtures/formats/key-bytes/vN.json`; ADR 0073 Phase 1, P1-A), so
//! `scripts/check-format-fixtures.sh` covers them. Lives in-crate because
//! `key_bytes` is `pub(crate)`. Not a version-tagged format: the encoding has
//! no tag by design and this fixture is its compatibility pin (see ADR 0073's
//! open question on the hash-ring/key-encoding layer). Entries are
//! `{"value": <AttributeValue serde JSON>, "bytes": "<hex>"}`; the `value`
//! side also pins `AttributeValue`'s frozen serde shape.

use crate::AttributeValue;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/formats/key-bytes")
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn inputs() -> Vec<AttributeValue> {
    use AttributeValue as A;
    vec![
        A::S(String::new()),
        A::S("abc".into()),
        A::S("h\u{e9}llo \u{1F600}".into()),
        A::S("nul\0inside".into()),
        A::N("0".into()),
        A::N("-0".into()),
        A::N("9".into()),
        A::N("15".into()),
        A::N("-10".into()),
        A::N("123.456".into()),
        A::N("1E-129".into()),
        A::N("-9.9999999999999999999999999999999999999E+125".into()),
        // Not a well-formed number: the documented defensive raw-text fallback.
        A::N("abc".into()),
        A::N(String::new()),
        A::B(Vec::new()),
        A::B(vec![0, 1, 2, 255]),
        A::Bool(false),
        A::Bool(true),
        // Non-key types encode empty.
        A::Null,
        A::M(BTreeMap::from([("k".to_string(), A::S("v".into()))])),
        A::L(vec![A::S("a".into())]),
        A::SS(vec!["a".into()]),
        A::NS(vec!["1".into()]),
        A::BS(vec![vec![1]]),
    ]
}

fn compute(v: &AttributeValue) -> Value {
    json!({"value": serde_json::to_value(v).expect("serializes"), "bytes": hex(&v.key_bytes())})
}

#[test]
fn key_bytes_vectors_match_the_checked_in_fixtures() {
    let dir = fixtures_dir();
    let mut seen = 0;
    for entry in fs::read_dir(&dir).unwrap_or_else(|e| panic!("reading {}: {e}", dir.display())) {
        let path = entry.expect("dir entry").path();
        if path.extension().is_none_or(|e| e != "json") {
            continue;
        }
        let stem = path.file_stem().and_then(|s| s.to_str()).expect("name");
        let version: u32 = stem
            .strip_prefix('v')
            .and_then(|n| n.parse().ok())
            .unwrap_or_else(|| panic!("{} is not named vN.json", path.display()));
        let doc: Value = serde_json::from_slice(&fs::read(&path).expect("read fixture"))
            .unwrap_or_else(|e| panic!("parsing {}: {e}", path.display()));
        let entries = doc.as_array().expect("fixture is a JSON array");
        match version {
            1 => {
                assert!(entries.len() >= 20, "v1 fixture lost vectors");
                for e in entries {
                    let value: AttributeValue =
                        serde_json::from_value(e["value"].clone()).expect("value decodes");
                    assert_eq!(&compute(&value), e, "key_bytes vector {value:?} changed");
                }
            }
            other => panic!("key-bytes fixture v{other} has no hand-written check"),
        }
        seen += 1;
    }
    assert!(seen > 0, "no key-bytes fixtures under {}", dir.display());
}

#[test]
#[allow(
    clippy::disallowed_methods,
    reason = "test-only fixture generator: std::fs is fine in tests (repo convention), and this \
              is the one sanctioned place a golden fixture is ever written"
)]
#[ignore = "run explicitly to (re)generate a fixture: cargo test -p animus-item --lib generate_fixture_key_bytes -- --ignored"]
fn generate_fixture_key_bytes() {
    let path = fixtures_dir().join("v1.json");
    assert!(
        fs::metadata(&path).is_err(),
        "refusing to overwrite existing fixture {} — add a new vN.json instead",
        path.display()
    );
    fs::create_dir_all(path.parent().expect("parent dir")).expect("create fixture dir");
    let doc: Vec<Value> = inputs().iter().map(compute).collect();
    let mut text = serde_json::to_string_pretty(&doc).expect("serialize");
    text.push('\n');
    fs::write(&path, text).expect("write fixture");
}
