//! The committed `deploy/operator/crd.yaml` is what `scripts/e2e-kind.sh`
//! (and any operator user following `deploy/operator/README.md`) applies to
//! a real API server. It is *generated* from `AnimusCluster::crd()` by
//! `animus-operator crd`, so every change to the spec type must be followed
//! by regenerating it — the API server's strict decoding otherwise rejects a
//! CR that uses the new field ("unknown field spec.s3" was the first time
//! this drifted, S-04 PR 3). This test pins the committed file to the type.
//!
//! To refresh after a spec change:
//!
//! ```sh
//! cargo run -p animus-operator -- crd > deploy/operator/crd.yaml
//! ```

use kube::CustomResourceExt;

const COMMITTED: &str = include_str!("../../../deploy/operator/crd.yaml");

#[test]
fn committed_crd_manifest_matches_the_generated_one() {
    let generated = serde_yaml::to_string(&animus_operator::AnimusCluster::crd())
        .expect("CustomResourceDefinition serializes to YAML");
    assert!(
        COMMITTED == generated,
        "deploy/operator/crd.yaml is stale relative to `AnimusCluster::crd()`; regenerate it with \
         `cargo run -p animus-operator -- crd > deploy/operator/crd.yaml`"
    );
}
