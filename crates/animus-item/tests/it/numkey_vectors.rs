//! Pinned `numkey` vectors (ADR 0063 order-preserving `N` key encoding) as a
//! checked-in fixture, so `scripts/check-format-fixtures.sh` covers them
//! (ADR 0073 Phase 1, P1-A). This is **not** a version-tagged byte format:
//! the encoding has no tag by design, and the fixture *is* its compatibility
//! pin — a change that alters any vector here changes every stored key, and
//! is a breaking change needing an ADR amendment (see ADR 0073's open question
//! on the hash-ring/key-encoding layer).
//!
//! Layout: `tests/fixtures/formats/numkey/vN.json`, a JSON array of
//! `{"input", "encoded", "checked", "decoded"}` (hex for bytes, `null` where
//! the function returns `None`). The input list below is hand-written; the
//! outputs are recomputed with the current code and must equal the fixture.

use animus_item::numkey;
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/formats/numkey")
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Inputs: the unit tests' round-trip, ordering, zero-form and accepted-shape
/// cases, the range extremes, and malformed/over-long text.
fn inputs() -> Vec<String> {
    let mut v: Vec<String> = [
        "0",
        "-0",
        "0.000",
        "0e5",
        "-0.0E-3",
        "+0",
        "1",
        "-1",
        "0.5",
        "-0.5",
        "0.05",
        "123",
        "1230",
        "12",
        "-123",
        "-12",
        "9",
        "15",
        "-5",
        "-10",
        "5",
        "1E10",
        "1E11",
        "1.23E40",
        "-9.9999999999999999999999999999999999999E+125",
        "9.9999999999999999999999999999999999999E+125",
        "1E-129",
        " 42 ",
        "+42",
        "42.",
        "42",
        // 39 significant digits: `encode` accepts, `encode_checked` rejects.
        "1.23456789012345678901234567890123456789",
        // Malformed.
        "",
        " ",
        "abc",
        "1e",
        "--1",
        "1.2.3",
        "NaN",
        ".",
    ]
    .iter()
    .map(|s| (*s).to_owned())
    .collect();
    v.dedup();
    v
}

fn compute(input: &str) -> Value {
    let enc = numkey::encode(input);
    json!({
        "input": input,
        "encoded": enc.as_deref().map(hex),
        "checked": numkey::encode_checked(input).as_deref().map(hex),
        "decoded": enc.as_deref().and_then(numkey::decode),
    })
}

fn check_v1(entries: &[Value]) {
    assert!(entries.len() >= 30, "v1 fixture lost vectors");
    for e in entries {
        let input = e["input"].as_str().expect("input is a string");
        assert_eq!(&compute(input), e, "numkey vector for {input:?} changed");
    }
    // The fixture must still contain the inputs that pin the known failure
    // modes, so trimming it cannot silently weaken the pin.
    for must in ["0", "9", "15", "-10", "1E-129", ""] {
        assert!(
            entries.iter().any(|e| e["input"] == must),
            "v1 fixture missing the {must:?} vector"
        );
    }
}

#[test]
fn numkey_vectors_match_the_checked_in_fixtures() {
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
            1 => check_v1(entries),
            other => panic!("numkey fixture v{other} has no hand-written check"),
        }
        seen += 1;
    }
    assert!(seen > 0, "no numkey fixtures under {}", dir.display());
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
    fs::create_dir_all(path.parent().expect("parent dir")).expect("create fixture dir");
    fs::write(path, bytes).unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
}

#[test]
#[ignore = "run explicitly to (re)generate a fixture: cargo test -p animus-item --test it numkey_vectors::generate_fixture_numkey -- --ignored"]
fn generate_fixture_numkey() {
    let doc: Vec<Value> = inputs().iter().map(|i| compute(i)).collect();
    let mut text = serde_json::to_string_pretty(&doc).expect("serialize");
    text.push('\n');
    write_new_fixture(&fixtures_dir().join("v1.json"), text.as_bytes());
}
