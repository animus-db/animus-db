//! Golden fixtures for the handshake preamble (ADR 0073 Phase 0, workstream
//! D) — see `docs/adr/0073-upgrade-compatibility.md`'s "Phase 0 conventions"
//! section for the shape this follows: one directory per format under
//! `tests/fixtures/formats/<format>/`, one `vN.bin` file per version,
//! iterated (never named literally) so a later version needs no test-code
//! change, only a new fixture file. `scripts/check-format-fixtures.sh`
//! enforces separately that a fixture file, once checked in, is never
//! edited or deleted.
//!
//! Two independent formats, matching `handshake.rs`'s two `ProtocolSpec`s:
//! `network-handshake` (`NETWORK_PROTOCOL`) and `client-handshake`
//! (`CLIENT_PROTOCOL`).

use animus_env::handshake::{
    CLIENT_PROTOCOL, NETWORK_PROTOCOL, Preamble, ProtocolSpec, decode, encode,
};
use std::fs;
use std::path::{Path, PathBuf};

fn fixtures_dir(format: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/formats")
        .join(format)
}

/// Decodes every `vN.bin` fixture under `format`'s directory with the
/// *current* code and asserts, field-by-field, that it decodes to exactly
/// the preamble `spec` describes (magic, version, empty extensions) — not
/// merely "decodes without error," which a decoder that silently dropped a
/// field would still pass.
fn assert_all_fixtures_decode_to(format: &str, spec: &ProtocolSpec) {
    let dir = fixtures_dir(format);
    let mut entries: Vec<PathBuf> = fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("reading fixture dir {}: {e}", dir.display()))
        .map(|e| e.expect("dir entry").path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "bin"))
        .collect();
    entries.sort();
    assert!(
        !entries.is_empty(),
        "no vN.bin fixtures found under {}",
        dir.display()
    );

    for path in entries {
        let bytes = fs::read(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
        let (preamble, consumed) =
            decode(&bytes).unwrap_or_else(|e| panic!("decoding fixture {}: {e}", path.display()));
        assert_eq!(
            consumed,
            bytes.len(),
            "fixture {} left unconsumed trailing bytes",
            path.display()
        );
        assert_eq!(preamble.magic, spec.magic, "fixture {}", path.display());
        assert_eq!(preamble.version, spec.version, "fixture {}", path.display());
        assert_eq!(
            preamble.extensions,
            Vec::<u8>::new(),
            "fixture {}: a v1 encoder always writes an empty ext area",
            path.display()
        );
    }
}

#[test]
fn network_handshake_fixtures_decode_structurally() {
    assert_all_fixtures_decode_to("network-handshake", &NETWORK_PROTOCOL);
}

#[test]
fn client_handshake_fixtures_decode_structurally() {
    assert_all_fixtures_decode_to("client-handshake", &CLIENT_PROTOCOL);
}

#[test]
fn network_handshake_round_trips() {
    let value = Preamble::for_protocol(&NETWORK_PROTOCOL);
    let bytes = encode(&value);
    let (decoded, consumed) = decode(&bytes).expect("round-trip decode");
    assert_eq!(consumed, bytes.len());
    assert_eq!(decoded, value);
}

#[test]
fn client_handshake_round_trips() {
    let value = Preamble::for_protocol(&CLIENT_PROTOCOL);
    let bytes = encode(&value);
    let (decoded, consumed) = decode(&bytes).expect("round-trip decode");
    assert_eq!(consumed, bytes.len());
    assert_eq!(decoded, value);
}

/// Writes `path` with `bytes`, refusing to overwrite a file that already
/// exists — the mechanism that makes "a fixture is regenerated only when a
/// version is deliberately bumped, never silently" enforced rather than a
/// convention. Run explicitly and only when deliberately adding a new
/// fixture version; never run as part of the normal test suite.
#[allow(
    clippy::disallowed_methods,
    reason = "test-only fixture generator: std::fs is fine in tests (repo convention), and this \
              is the one sanctioned place a golden fixture is ever written"
)]
fn write_new_fixture(path: &Path, bytes: &[u8]) {
    if let Ok(meta) = fs::metadata(path) {
        panic!(
            "refusing to overwrite existing fixture {} ({} bytes on disk) — \
             bump the format version and add a new vN.bin instead of regenerating this one",
            path.display(),
            meta.len()
        );
    }
    fs::create_dir_all(path.parent().expect("fixture path has a parent dir"))
        .unwrap_or_else(|e| panic!("creating fixture dir for {}: {e}", path.display()));
    fs::write(path, bytes).unwrap_or_else(|e| panic!("writing fixture {}: {e}", path.display()));
}

#[test]
#[ignore = "run explicitly to (re)generate a fixture: cargo test -p animus-env --test format_fixtures generate_fixture_network_handshake -- --ignored"]
fn generate_fixture_network_handshake() {
    let path = fixtures_dir("network-handshake").join(format!("v{}.bin", NETWORK_PROTOCOL.version));
    write_new_fixture(&path, &encode(&Preamble::for_protocol(&NETWORK_PROTOCOL)));
}

#[test]
#[ignore = "run explicitly to (re)generate a fixture: cargo test -p animus-env --test format_fixtures generate_fixture_client_handshake -- --ignored"]
fn generate_fixture_client_handshake() {
    let path = fixtures_dir("client-handshake").join(format!("v{}.bin", CLIENT_PROTOCOL.version));
    write_new_fixture(&path, &encode(&Preamble::for_protocol(&CLIENT_PROTOCOL)));
}
