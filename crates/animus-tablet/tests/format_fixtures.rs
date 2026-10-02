//! Pinned hash-ring vectors as checked-in fixtures (ADR 0073 Phase 1, P1-A),
//! so `scripts/check-format-fixtures.sh` covers them: `partition-token`
//! (`partition_token`, i.e. MurmurHash3 x64-128 seed 0, top 64 bits
//! big-endian) and `escape` (the prefix-free key escape the wire edges must
//! match byte-for-byte). Not version-tagged byte formats: the token has no
//! tag by design, and every stored key embeds it, so these fixtures are its
//! compatibility pin (see ADR 0073's open question on the hash-ring /
//! key-encoding layer). Layout `tests/fixtures/formats/<name>/vN.json`: a
//! JSON array of `{"input": "<hex>", "output": "<hex>"}`. Inputs are
//! hand-written below; outputs are recomputed and must equal the fixture.

use animus_tablet::{escape, partition_token};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};

fn fixtures_dir(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/formats")
        .join(name)
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn unhex(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd-length hex {s:?}");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

/// Inputs: empty, every tail length 1..=15 around the 16-byte block boundary
/// (the reference cases `animus-tablet`'s unit test already pins against
/// canonical MurmurHash3), multi-block inputs, embedded and trailing `0x00`s,
/// and realistic text keys.
fn inputs() -> Vec<Vec<u8>> {
    let seq = |n: u8| (1..=n).collect::<Vec<u8>>();
    let mut v: Vec<Vec<u8>> = vec![Vec::new(), vec![0], vec![0, 0, 0, 1, 2, 3]];
    for n in [1u8, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 48] {
        v.push(seq(n));
    }
    for s in ["user#1", "a", "abc", "h\u{e9}llo \u{1F600}", "a\0b", "a\0"] {
        v.push(s.as_bytes().to_vec());
    }
    v.push(vec![0xff; 40]);
    v
}

fn token_vector(input: &[u8]) -> Value {
    json!({"input": hex(input), "output": hex(&partition_token(input))})
}

fn escape_vector(input: &[u8]) -> Value {
    json!({"input": hex(input), "output": hex(&escape(input))})
}

fn check_all(name: &str, compute: fn(&[u8]) -> Value, must_contain: &[&[u8]]) {
    let dir = fixtures_dir(name);
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
                for e in entries {
                    let input = unhex(e["input"].as_str().expect("input hex"));
                    assert_eq!(&compute(&input), e, "{name} vector {} changed", hex(&input));
                }
                for m in must_contain {
                    assert!(
                        entries.iter().any(|e| e["input"] == hex(m)),
                        "{name} v1 fixture missing vector {}",
                        hex(m)
                    );
                }
            }
            other => panic!("{name} fixture v{other} has no hand-written check"),
        }
        seen += 1;
    }
    assert!(seen > 0, "no {name} fixtures under {}", dir.display());
}

#[test]
fn partition_token_vectors_match_the_checked_in_fixtures() {
    check_all("partition-token", token_vector, &[b"", b"\x01", b"user#1"]);
}

#[test]
fn escape_vectors_match_the_checked_in_fixtures() {
    check_all("escape", escape_vector, &[b"", b"a\0b"]);
}

#[allow(
    clippy::disallowed_methods,
    reason = "test-only fixture generator: std::fs is fine in tests (repo convention), and this \
              is the one sanctioned place a golden fixture is ever written"
)]
fn write_new_fixture(name: &str, compute: fn(&[u8]) -> Value) {
    let path = fixtures_dir(name).join("v1.json");
    assert!(
        fs::metadata(&path).is_err(),
        "refusing to overwrite existing fixture {} — add a new vN.json instead",
        path.display()
    );
    fs::create_dir_all(path.parent().expect("parent dir")).expect("create fixture dir");
    let doc: Vec<Value> = inputs().iter().map(|i| compute(i)).collect();
    let mut text = serde_json::to_string_pretty(&doc).expect("serialize");
    text.push('\n');
    fs::write(&path, text).expect("write fixture");
}

#[test]
#[ignore = "run explicitly to (re)generate a fixture: cargo test -p animus-tablet --test format_fixtures generate_fixture_partition_token -- --ignored"]
fn generate_fixture_partition_token() {
    write_new_fixture("partition-token", token_vector);
}

#[test]
#[ignore = "run explicitly to (re)generate a fixture: cargo test -p animus-tablet --test format_fixtures generate_fixture_escape -- --ignored"]
fn generate_fixture_escape() {
    write_new_fixture("escape", escape_vector);
}
