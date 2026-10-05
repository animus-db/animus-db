//! Golden-fixture tests for `animusd`'s `ClusterConfig` JSON format
//! (ADR 0073 Phase 0, Workstream E): the top-level required `"v"` field.
//!
//! Fixtures live under `tests/fixtures/formats/cluster-config/`; a
//! checked-in fixture is never edited or deleted
//! (`scripts/check-format-fixtures.sh`).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use animus_control::format::FormatError;
use animus_env::NodeId;
use animusd::RoleAddrs;
use animusd::config::{
    CLUSTER_CONFIG_FORMAT, CLUSTER_CONFIG_VERSION, ClusterConfig, ClusterSettings, ConfigError,
    DynamoAuthConfig, NodeRole, TlsSection,
};

fn addr(s: &str) -> SocketAddr {
    s.parse().unwrap()
}

fn node(i: u16, role: NodeRole, tls: bool) -> RoleAddrs {
    let base = 9000 + i * 6;
    let a = |off: u16| addr(&format!("10.0.0.{}:{}", i + 1, base + off));
    RoleAddrs {
        id: NodeId::propose(&format!("n{i}")).expect("valid id"),
        role,
        internal: a(0),
        client: a(1),
        dynamo: a(2),
        admin: a(3),
        intra: a(4),
        console: a(5),
        advertise_host: (i == 1).then(|| "n1.example.internal".to_string()),
        tls: tls.then(|| TlsSection {
            cert_path: PathBuf::from("/etc/animus/tls/tls.crt"),
            key_path: PathBuf::from("/etc/animus/tls/tls.key"),
            ca_path: Some(PathBuf::from("/etc/animus/tls/ca.crt")),
        }),
        encryption_key_path: (i == 0).then(|| "/etc/animus/enc/key".to_string()),
        overload: None,
    }
}

/// The representative v1 value: mixed roles, TLS on every node, auth,
/// advertise host, encryption key path, and a partially-set settings section.
fn v1_config() -> ClusterConfig {
    ClusterConfig {
        version: CLUSTER_CONFIG_VERSION,
        nodes: vec![
            node(0, NodeRole::Control, true),
            node(1, NodeRole::Both, true),
            node(2, NodeRole::Data, true),
        ],
        dynamo_auth: Some(DynamoAuthConfig {
            credentials: BTreeMap::from([("AKIDEXAMPLE".to_string(), "secret".to_string())]),
        }),
        cluster_settings: Some(ClusterSettings {
            auto_split_bytes: Some(1_000_000),
            quiesce_after_secs: Some(5),
            ..ClusterSettings::default()
        }),
    }
}

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/formats/cluster-config")
}

fn assert_v1_structure(c: &ClusterConfig, name: &str) {
    assert_eq!(c.version, 1, "{name}");
    assert_eq!(c.nodes.len(), 3, "{name}");
    assert_eq!(c.nodes[0].role, NodeRole::Control, "{name}");
    assert_eq!(c.nodes[1].role, NodeRole::Both, "{name}");
    assert_eq!(c.nodes[2].role, NodeRole::Data, "{name}");
    assert_eq!(c.nodes[0].id.to_string(), "n0", "{name}");
    assert_eq!(c.nodes[0].internal, addr("10.0.0.1:9000"), "{name}");
    assert_eq!(c.nodes[2].console, addr("10.0.0.3:9017"), "{name}");
    assert_eq!(
        c.nodes[1].advertise_host.as_deref(),
        Some("n1.example.internal"),
        "{name}"
    );
    assert!(c.nodes.iter().all(|n| n.tls.is_some()), "{name}");
    assert_eq!(
        c.nodes[0].tls.as_ref().unwrap().ca_path.as_deref(),
        Some(Path::new("/etc/animus/tls/ca.crt")),
        "{name}"
    );
    assert_eq!(
        c.nodes[0].encryption_key_path.as_deref(),
        Some("/etc/animus/enc/key"),
        "{name}"
    );
    assert!(c.nodes[1].encryption_key_path.is_none(), "{name}");
    assert_eq!(
        c.dynamo_auth.as_ref().unwrap().credentials["AKIDEXAMPLE"],
        "secret",
        "{name}"
    );
    let s = c.cluster_settings.as_ref().expect("settings present");
    assert_eq!(s.auto_split_bytes, Some(1_000_000), "{name}");
    assert_eq!(s.quiesce_after_secs, Some(5), "{name}");
    assert_eq!(s.orphan_sweep_after_secs, None, "{name}");
}

/// Per-version expected value (ADR 0073 Phase 1): the version comes from the
/// `vN.json` file name and must agree with the decoded `"v"`; an unrecognised
/// version panics, so a new fixture forces a new expectation (and a frozen
/// legacy decoder in `ClusterConfig::from_json`'s dispatch).
#[test]
fn decodes_every_checked_in_cluster_config_fixture_to_its_per_version_value() {
    let dir = fixtures_dir();
    let mut seen = Vec::new();
    for entry in std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("reading fixtures dir {}: {e}", dir.display()))
    {
        let path = entry.expect("dir entry").path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let version: u32 = name
            .strip_prefix('v')
            .and_then(|r| r.strip_suffix(".json"))
            .and_then(|n| n.parse().ok())
            .unwrap_or_else(|| panic!("{name}: fixture name is not vN.json"));
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {name}: {e}"));
        let c = ClusterConfig::from_json(&text)
            .unwrap_or_else(|e| panic!("{name} failed to decode: {e}"));
        assert_eq!(c.version, version, "{name}: file name vs decoded v");
        match version {
            1 => {
                assert_v1_structure(&c, &name);
                assert_eq!(c.to_json(), v1_config().to_json(), "{name}");
            }
            other => panic!(
                "{name}: no expected value for cluster-config v{other}; add a match arm \
                 before adding the fixture file"
            ),
        }
        seen.push(version);
    }
    assert!(!seen.is_empty(), "no fixtures under {}", dir.display());
    assert!(
        seen.contains(&CLUSTER_CONFIG_VERSION),
        "no fixture for the current CLUSTER_CONFIG_VERSION: {seen:?}"
    );
}

#[test]
fn cluster_config_round_trips() {
    let cfg = v1_config();
    let back = ClusterConfig::from_json(&cfg.to_json()).expect("decodes");
    assert_v1_structure(&back, "round-trip");
    assert_eq!(back.to_json(), cfg.to_json());
}

#[test]
fn fixture_carries_the_v_field() {
    let text = std::fs::read_to_string(fixtures_dir().join("v1.json")).expect("v1.json");
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["v"], serde_json::Value::from(CLUSTER_CONFIG_VERSION));
}

fn with_v(v: Option<serde_json::Value>) -> String {
    let mut value: serde_json::Value = serde_json::from_str(&v1_config().to_json()).unwrap();
    let obj = value.as_object_mut().unwrap();
    match v {
        Some(v) => obj.insert("v".into(), v),
        None => obj.remove("v"),
    };
    value.to_string()
}

#[test]
fn missing_v_is_a_named_pre_baseline_error() {
    let err = ClusterConfig::from_json(&with_v(None)).expect_err("no v must be refused");
    assert_eq!(
        err,
        ConfigError::Format(FormatError::PreBaselineFormat {
            format: CLUSTER_CONFIG_FORMAT
        })
    );
    let msg = err.to_string();
    assert!(msg.contains("cluster-config"), "{msg}");
    assert!(msg.contains("\"v\": 1"), "{msg}");
    assert!(msg.contains("gen-config"), "{msg}");
}

#[test]
fn v_zero_and_future_v_are_unsupported_version_errors() {
    for found in [0u8, 2, 200] {
        let err = ClusterConfig::from_json(&with_v(Some(found.into())))
            .expect_err("unsupported v must be refused");
        assert_eq!(
            err,
            ConfigError::Format(FormatError::UnsupportedFormatVersion {
                format: CLUSTER_CONFIG_FORMAT,
                found,
                max_supported: 1
            }),
            "v={found}"
        );
    }
}

/// Regenerates `v1.json` from [`v1_config`]:
/// `cargo test -p animusd --test format_fixtures generate_fixture_cluster_config -- --ignored`.
/// Refuses to overwrite an existing fixture (ADR 0073).
#[test]
#[ignore]
fn generate_fixture_cluster_config() {
    let dir = fixtures_dir();
    std::fs::create_dir_all(&dir).expect("create fixtures dir");
    let path = dir.join(format!("v{CLUSTER_CONFIG_VERSION}.json"));
    if std::fs::metadata(&path).is_ok() {
        panic!(
            "{} already exists — a checked-in fixture is never regenerated in place; bump \
             CLUSTER_CONFIG_VERSION and add a new fixture file instead",
            path.display()
        );
    }
    std::fs::write(&path, v1_config().to_json()).unwrap();
}
