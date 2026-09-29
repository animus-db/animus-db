//! Golden-fixture tests for the `AnimusCluster` spec's internal content
//! schema version (ADR 0073 Phase 0, Workstream E): the required
//! `spec.schemaVersion`, distinct from the Kubernetes `v1alpha1` API version.
//!
//! Fixtures live under `tests/fixtures/formats/animuscluster-spec/` as full
//! `AnimusCluster` CRs; a checked-in fixture is never edited or deleted
//! (`scripts/check-format-fixtures.sh`).

use std::path::{Path, PathBuf};

use animus_operator::AnimusCluster;
use animus_operator::crd::{CONTENT_SCHEMA_VERSION, SPEC_FORMAT};
use animus_operator::validate::{validate_schema_version, validate_spec};
use kube::CustomResourceExt;

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/formats/animuscluster-spec")
}

fn assert_v1_structure(c: &AnimusCluster, name: &str) {
    let s = &c.spec;
    assert_eq!(s.schema_version, 1, "{name}");
    assert_eq!(c.metadata.name.as_deref(), Some("golden"), "{name}");
    assert_eq!(c.metadata.namespace.as_deref(), Some("animus"), "{name}");
    assert_eq!(
        s.image.as_deref(),
        Some("ghcr.io/animus-db/animusd:0.1.0"),
        "{name}"
    );
    assert_eq!(s.nodes, 5, "{name}");
    assert_eq!(s.control_nodes, Some(3), "{name}");
    assert_eq!(s.storage.size.as_deref(), Some("10Gi"), "{name}");
    assert_eq!(
        s.storage.storage_class_name.as_deref(),
        Some("fast-ssd"),
        "{name}"
    );
    assert_eq!(s.base_port, Some(14000), "{name}");
    assert_eq!(
        s.client_service.type_.as_deref(),
        Some("LoadBalancer"),
        "{name}"
    );
    assert_eq!(s.quiesce_after_secs, Some(5), "{name}");
    assert_eq!(s.auto_split_bytes, Some(1_000_000), "{name}");
    assert_eq!(
        s.dynamo_auth_secret_name.as_deref(),
        Some("dynamo-creds"),
        "{name}"
    );
    assert_eq!(
        s.tls.as_ref().and_then(|t| t.secret_name.as_deref()),
        Some("animus-tls"),
        "{name}"
    );
    assert_eq!(s.backup_store.as_deref(), Some("cluster"), "{name}");
    assert_eq!(
        s.segment_store.as_deref(),
        Some("dir:/var/lib/animus/segments"),
        "{name}"
    );
    assert_eq!(
        s.encryption_key_secret_name.as_deref(),
        Some("animus-enc-key"),
        "{name}"
    );
}

#[test]
fn every_checked_in_fixture_decodes_and_validates() {
    let mut seen = 0;
    for entry in std::fs::read_dir(fixtures_dir()).expect("fixtures dir") {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|e| e != "json") {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let text = std::fs::read_to_string(&path).unwrap();
        let cluster: AnimusCluster =
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_v1_structure(&cluster, &name);
        validate_spec(None, &cluster.spec).unwrap_or_else(|v| panic!("{name}: {v:?}"));
        seen += 1;
    }
    assert!(seen >= 1, "no fixtures found");
}

#[test]
fn v1_fixture_round_trips_losslessly() {
    let text = std::fs::read_to_string(fixtures_dir().join("v1.json")).unwrap();
    let original: serde_json::Value = serde_json::from_str(&text).unwrap();
    let cluster: AnimusCluster = serde_json::from_str(&text).unwrap();
    assert_eq!(serde_json::to_value(&cluster).unwrap(), original);
}

fn v1_value() -> serde_json::Value {
    let text = std::fs::read_to_string(fixtures_dir().join("v1.json")).unwrap();
    serde_json::from_str(&text).unwrap()
}

#[test]
fn missing_schema_version_is_rejected_at_decode() {
    let mut v = v1_value();
    v["spec"].as_object_mut().unwrap().remove("schemaVersion");
    let err = serde_json::from_value::<AnimusCluster>(v).unwrap_err();
    assert!(err.to_string().contains("schemaVersion"), "{err}");
}

#[test]
fn zero_and_future_schema_versions_are_refused_by_validation() {
    for bad in [0u32, 2] {
        let mut v = v1_value();
        v["spec"]["schemaVersion"] = bad.into();
        let cluster: AnimusCluster = serde_json::from_value(v).unwrap();
        let violation = validate_schema_version(&cluster.spec).expect("must be refused");
        assert!(violation.message.contains(SPEC_FORMAT), "{violation}");
        assert!(validate_spec(None, &cluster.spec).is_err());
    }
}

#[test]
fn crd_schema_lists_schema_version_as_required_with_a_minimum() {
    let crd = serde_json::to_value(AnimusCluster::crd()).unwrap();
    let spec = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"];
    let required = spec["required"].as_array().expect("required list");
    assert!(
        required.iter().any(|r| r == "schemaVersion"),
        "{required:?}"
    );
    assert_eq!(
        spec["properties"]["schemaVersion"]["minimum"].as_f64(),
        Some(1.0)
    );
}

/// Regenerates `v1.json`:
/// `cargo test -p animus-operator --test format_fixtures generate_fixture_animuscluster_spec -- --ignored`.
/// Refuses to overwrite an existing fixture (ADR 0073).
#[test]
#[ignore]
fn generate_fixture_animuscluster_spec() {
    let dir = fixtures_dir();
    std::fs::create_dir_all(&dir).expect("create fixtures dir");
    let path = dir.join(format!("v{CONTENT_SCHEMA_VERSION}.json"));
    if std::fs::metadata(&path).is_ok() {
        panic!(
            "{} already exists — a checked-in fixture is never regenerated in place; bump \
             CONTENT_SCHEMA_VERSION and add a new fixture file instead",
            path.display()
        );
    }
    let mut cluster = AnimusCluster::new(
        "golden",
        animus_operator::AnimusClusterSpec {
            image: Some("ghcr.io/animus-db/animusd:0.1.0".into()),
            nodes: 5,
            control_nodes: Some(3),
            base_port: Some(14000),
            quiesce_after_secs: Some(5),
            auto_split_bytes: Some(1_000_000),
            dynamo_auth_secret_name: Some("dynamo-creds".into()),
            tls: Some(animus_operator::crd::TlsSpec {
                secret_name: Some("animus-tls".into()),
                cert_manager: None,
            }),
            backup_store: Some("cluster".into()),
            segment_store: Some("dir:/var/lib/animus/segments".into()),
            encryption_key_secret_name: Some("animus-enc-key".into()),
            storage: animus_operator::crd::StorageSpec {
                size: Some("10Gi".into()),
                storage_class_name: Some("fast-ssd".into()),
                ..Default::default()
            },
            client_service: animus_operator::crd::ClientServiceSpec {
                type_: Some("LoadBalancer".into()),
            },
            ..Default::default()
        },
    );
    cluster.metadata.namespace = Some("animus".into());
    let mut text = serde_json::to_string_pretty(&cluster).unwrap();
    text.push('\n');
    std::fs::write(&path, text).unwrap();
}
