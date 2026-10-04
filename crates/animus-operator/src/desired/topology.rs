//! Topology constants and the pure pod-annotation patch computation
//! (G-01 stage G-a, ADR 0060's 2026-10-04 amendment).
//!
//! The downward API cannot expose a *node's* labels, so the operator resolves
//! each scheduled pod's node itself and annotates the pod with the node's
//! `topology.kubernetes.io/{region,zone}` values; the `StatefulSet` projects
//! `metadata.annotations` into a file that `animusd --labels-file
//! --labels-file-annotations` reads (`animusd::node_labels`, which owns the
//! reverse annotation-key -> label-key mapping). The strings below must match
//! that module's; a test pins them against its source text.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Pod;

/// The node label read for a pod's region.
pub const NODE_REGION_LABEL: &str = "topology.kubernetes.io/region";
/// The node label read for a pod's zone; also the `topologySpreadConstraints`
/// `topologyKey`.
pub const NODE_ZONE_LABEL: &str = "topology.kubernetes.io/zone";
/// The node label identifying a host, the anti-affinity `topologyKey`.
pub const HOSTNAME_LABEL: &str = "kubernetes.io/hostname";

/// Pod annotation carrying the node's region.
pub const REGION_ANNOTATION: &str = "animus.io/topology-region";
/// Pod annotation carrying the node's zone.
pub const ZONE_ANNOTATION: &str = "animus.io/topology-zone";
/// Pod annotation set once the node has been resolved, even if the node has
/// no topology labels (what `animusd`'s startup wait polls for).
pub const RESOLVED_ANNOTATION: &str = "animus.io/topology-resolved";

/// Where the downward-API projection of the pod's annotations is mounted.
pub const TOPOLOGY_MOUNT_DIR: &str = "/etc/animus/topology";
/// The projected file's name within [`TOPOLOGY_MOUNT_DIR`].
pub const TOPOLOGY_FILE_NAME: &str = "annotations";
/// `--labels-wait-secs` the entrypoint passes: the kubelet refreshes a
/// downward-API volume on its own sync period (up to roughly a minute after
/// an annotation change), so the bound is generous; on timeout `animusd`
/// starts anyway with a warning.
pub const LABELS_WAIT_SECS: u64 = 180;

/// The pod's `spec.nodeName`, if scheduled.
#[must_use]
pub fn node_name(pod: &Pod) -> Option<&str> {
    pod.spec
        .as_ref()
        .and_then(|s| s.node_name.as_deref())
        .filter(|n| !n.is_empty())
}

/// The annotations that must be merged onto `pod` so it carries its node's
/// topology, given the node's labels (`None`: the node could not be read —
/// nothing is patched, so a transient failure is retried rather than
/// recorded as "resolved without labels"). Returns `None` when there is
/// nothing to do: the pod is unscheduled, the node is unknown, or the pod
/// already carries exactly these annotations.
///
/// A node with no region/zone labels still yields the
/// [`RESOLVED_ANNOTATION`] marker alone, so `animusd` stops waiting.
#[must_use]
pub fn pod_annotation_patch(
    pod: &Pod,
    node_labels: Option<&BTreeMap<String, String>>,
) -> Option<BTreeMap<String, String>> {
    node_name(pod)?;
    let node_labels = node_labels?;
    let mut want = BTreeMap::new();
    for (label, annotation) in [
        (NODE_REGION_LABEL, REGION_ANNOTATION),
        (NODE_ZONE_LABEL, ZONE_ANNOTATION),
    ] {
        if let Some(v) = node_labels.get(label).filter(|v| !v.is_empty()) {
            want.insert(annotation.to_owned(), v.clone());
        }
    }
    want.insert(RESOLVED_ANNOTATION.to_owned(), "true".to_owned());
    let have = pod.metadata.annotations.as_ref();
    let satisfied = want
        .iter()
        .all(|(k, v)| have.and_then(|h| h.get(k)) == Some(v));
    (!satisfied).then_some(want)
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::PodSpec;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn pod(node: Option<&str>, annotations: &[(&str, &str)]) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some("c-0".into()),
                annotations: (!annotations.is_empty()).then(|| {
                    annotations
                        .iter()
                        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                        .collect()
                }),
                ..Default::default()
            },
            spec: Some(PodSpec {
                node_name: node.map(str::to_owned),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn unscheduled_pod_or_unreadable_node_patches_nothing() {
        let l = labels(&[(NODE_ZONE_LABEL, "z1")]);
        assert_eq!(pod_annotation_patch(&pod(None, &[]), Some(&l)), None);
        assert_eq!(pod_annotation_patch(&pod(Some(""), &[]), Some(&l)), None);
        assert_eq!(pod_annotation_patch(&pod(Some("n1"), &[]), None), None);
    }

    #[test]
    fn patches_region_zone_and_marker() {
        let l = labels(&[
            (NODE_REGION_LABEL, "r1"),
            (NODE_ZONE_LABEL, "r1-a"),
            ("kubernetes.io/hostname", "n1"),
        ]);
        let got = pod_annotation_patch(&pod(Some("n1"), &[]), Some(&l)).unwrap();
        assert_eq!(
            got,
            labels(&[
                (REGION_ANNOTATION, "r1"),
                (ZONE_ANNOTATION, "r1-a"),
                (RESOLVED_ANNOTATION, "true"),
            ])
        );
    }

    #[test]
    fn node_without_topology_labels_still_gets_the_marker() {
        let got = pod_annotation_patch(&pod(Some("n1"), &[]), Some(&labels(&[]))).unwrap();
        assert_eq!(got, labels(&[(RESOLVED_ANNOTATION, "true")]));
    }

    #[test]
    fn already_resolved_pod_is_left_alone_but_a_changed_zone_repatches() {
        let l = labels(&[(NODE_ZONE_LABEL, "z1")]);
        let done = pod(
            Some("n1"),
            &[
                (ZONE_ANNOTATION, "z1"),
                (RESOLVED_ANNOTATION, "true"),
                ("other", "x"),
            ],
        );
        assert_eq!(pod_annotation_patch(&done, Some(&l)), None);
        let l2 = labels(&[(NODE_ZONE_LABEL, "z2")]);
        assert_eq!(
            pod_annotation_patch(&done, Some(&l2)).unwrap()[ZONE_ANNOTATION],
            "z2"
        );
    }

    /// Drift guard: the strings this module shares with `animusd`'s
    /// `node_labels` (which the operator does not depend on) must match.
    #[test]
    fn annotation_keys_match_animusd_node_labels() {
        let src = include_str!("../../../animusd/src/node_labels.rs");
        for s in [
            REGION_ANNOTATION,
            ZONE_ANNOTATION,
            RESOLVED_ANNOTATION,
            NODE_REGION_LABEL,
        ] {
            // region/zone label strings live in animus-placement.
            if s.starts_with("animus.io/") {
                assert!(src.contains(&format!("\"{s}\"")), "{s} missing in animusd");
            }
        }
        let placement = include_str!("../../../animus-placement/src/lib.rs");
        for s in [NODE_REGION_LABEL, NODE_ZONE_LABEL] {
            assert!(placement.contains(&format!("\"{s}\"")), "{s}");
        }
    }
}
