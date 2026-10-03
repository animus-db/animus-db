//! A pure, shared validator for [`AnimusClusterSpec`] (S-07e, ADR 0070).
//!
//! Every rule here is checked purely from the spec — no cluster access, no
//! `async`, no side effects — so it can run both inside the reconciler
//! (`crate::controller::reconcile`, which has run these same checks
//! ad hoc since before this crate had an admission webhook) and inside the
//! webhook server (`crate::webhook`, which has no cluster access of its own
//! to speak of beyond the two objects the API server hands it). **This is
//! the one place either caller checks any of these rules** — the reconciler
//! doesn't reimplement a parallel copy, it calls the exact functions this
//! module calls (`TlsSpec::validate`/`S3StoreSpec::validate`/
//! `AnimusClusterSpec::validate_store_spec` stay defined on their own types
//! in `crd.rs`, called from both here and there, and the two
//! `spec.controlNodes` rules below moved out of `controller.rs`'s own
//! inline arithmetic into the two small pure functions this module exports)
//! — so the two paths cannot silently drift apart.
//!
//! A **live** check — does a referenced `Secret` actually exist, is it
//! shaped right — is deliberately **not** here: `spec.encryptionKeySecretName`
//! is the one existing example (`crate::controller::
//! validate_encryption_key_secret`), and it needs a `ClusterApi` call the
//! webhook must never make (ADR 0070's own "fast and side-effect free"
//! requirement — an admission webhook blocking on a slow or unreachable API
//! call is exactly the kind of webhook `failurePolicy: Fail` turns into an
//! outage). Live checks stay exactly where they already were, in the
//! reconciler, with their own condition-based fallback.

use crate::crd::{AnimusClusterSpec, CONTENT_SCHEMA_VERSION, SPEC_FORMAT};

/// One rule this spec failed, naming the field it's about (a JSON-pointer-
/// ish dotted path, `spec.foo`/`spec.foo.bar` — not necessarily a single
/// leaf when a rule spans two fields, e.g. `spec.nodes`/`spec.controlNodes`)
/// and a human-readable message. `Display` renders `"{field}: {message}"`,
/// which is what both callers actually show an operator (a `ClusterCondition
/// .message`, or an `AdmissionResponse`'s denial reason).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub field: &'static str,
    pub message: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.field, self.message)
    }
}

/// `spec.schemaVersion` must be in `1..=`[`CONTENT_SCHEMA_VERSION`] (ADR 0073
/// Phase 0 E). `0` and a version newer than this operator understands are
/// both refused, with a message naming the format and the way out.
#[must_use]
pub fn validate_schema_version(new: &AnimusClusterSpec) -> Option<Violation> {
    let v = new.schema_version;
    if v == 0 {
        Some(Violation {
            field: "spec.schemaVersion",
            message: format!(
                "{SPEC_FORMAT} schemaVersion 0 is invalid; set schemaVersion: \
                 {CONTENT_SCHEMA_VERSION}"
            ),
        })
    } else if v > CONTENT_SCHEMA_VERSION {
        Some(Violation {
            field: "spec.schemaVersion",
            message: format!(
                "{SPEC_FORMAT} schemaVersion {v} unsupported (this operator supports up to \
                 {CONTENT_SCHEMA_VERSION}); upgrade the operator or set schemaVersion: \
                 {CONTENT_SCHEMA_VERSION}"
            ),
        })
    } else {
        None
    }
}

/// `spec.nodes` must be at least 1 — a `StatefulSet` with zero replicas is a
/// legal but useless object, and every other builder in this crate
/// (`desired::poddisruptionbudget` in particular) already documents that it
/// assumes at least one node and clamps rather than trusts this. Not
/// previously enforced anywhere in this crate (a real gap the CRD's own doc
/// comment on `nodes` already claimed was true) — closed here rather than
/// left for the webhook alone to discover, since the reconciler gains the
/// identical check for free (see [`validate_spec`]'s own doc for why the
/// reconciler surfaces it as a condition rather than refusing to reconcile).
#[must_use]
pub fn validate_nodes(new: &AnimusClusterSpec) -> Option<Violation> {
    if new.nodes < 1 {
        Some(Violation {
            field: "spec.nodes",
            message: format!("spec.nodes ({}) must be at least 1", new.nodes),
        })
    } else {
        None
    }
}

/// `spec.controlNodes` (resolved against its own default) must be at least
/// 1 and no more than `spec.nodes` — the identical rule
/// `crate::controller::reconcile`'s own `CONDITION_SCALE_BELOW_CONTROL_NODES_
/// REFUSED` check has always enforced (every control-role pod must fit
/// inside the total pod count). Message text matches that condition's own
/// wording so an operator sees the same sentence whether the webhook denied
/// the write or the reconciler is reporting on a cluster installed without
/// one.
#[must_use]
pub fn validate_control_nodes_within_nodes(new: &AnimusClusterSpec) -> Option<Violation> {
    let control_nodes = new.control_nodes_or_default();
    if control_nodes < 1 {
        Some(Violation {
            field: "spec.controlNodes",
            message: format!("spec.controlNodes ({control_nodes}) must be at least 1"),
        })
    } else if new.nodes >= 1 && control_nodes > new.nodes {
        Some(Violation {
            field: "spec.controlNodes",
            message: format!(
                "spec.nodes ({}) is below spec.controlNodes ({control_nodes})",
                new.nodes
            ),
        })
    } else {
        None
    }
}

/// `spec.controlNodes` (resolved) may only ever grow — `prior` is the
/// previously-accepted resolved value, `target` the newly-requested one.
/// Shared by [`validate_spec`] (fed `old.control_nodes_or_default()` on an
/// UPDATE review) and `crate::controller::reconcile`'s own shrink-rejection
/// check (fed the *actually-applied* value read back off the previous
/// `ConfigMap` — see `crate::controller::previous_applied_control_nodes`'s
/// own doc for why that, not the previous CR spec, is the reconciler's
/// source of truth: it must keep protecting a running cluster even when
/// installed without this webhook, or after an edit made before the webhook
/// existed).
#[must_use]
pub fn control_nodes_regression(prior: i32, target: i32) -> Option<Violation> {
    if target < prior {
        Some(Violation {
            field: "spec.controlNodes",
            message: format!(
                "spec.controlNodes decreased from {prior} to {target} — controlNodes can grow \
                 but never shrink once a cluster is running"
            ),
        })
    } else {
        None
    }
}

/// `spec.image` (resolved against [`AnimusClusterSpec::DEFAULT_IMAGE`], the
/// same resolution `desired::statefulset::build` renders with) may not
/// change on an existing cluster (ADR 0060 "Upgrades", 2026-10-03
/// amendment). `prior` is the previously-accepted *effective* image,
/// `target` the newly-requested one. Comparing effective values means
/// `None` -> `Some(<the default>)` (and back) is a no-op normalisation, not
/// a change.
///
/// Why: ADR 0073 Phase 1 supports only a whole-cluster stop -> upgrade ->
/// restart; mixed-version wire and rolling upgrades are not supported
/// (Phases 2/3), and a `StatefulSet`'s default `RollingUpdate` would roll a
/// new image one pod at a time, which is exactly that unsupported mixed
/// window. Shared by [`validate_spec`] (fed the old spec's effective image
/// on an UPDATE review) and `crate::controller::reconcile`'s fallback (fed
/// the image the live `StatefulSet` actually runs, for a cluster installed
/// without the webhook). Lifted by the follow-up that makes the operator
/// orchestrate a whole-cluster restart.
#[must_use]
pub fn image_change_rejection(prior: &str, target: &str) -> Option<Violation> {
    if prior == target {
        None
    } else {
        Some(Violation {
            field: "spec.image",
            message: format!(
                "spec.image changed from {prior} to {target} — changing the image of a running \
                 cluster is rejected: AnimusDB does not support mixed-version clusters or \
                 rolling upgrades yet (ADR 0073), and a StatefulSet rolling update would run \
                 two versions at once. Upgrade with a whole-cluster stop, upgrade and restart \
                 instead (ADR 0060, \"Upgrades\": delete the AnimusCluster, keeping its \
                 PersistentVolumeClaims, and re-create it with the new image)"
            ),
        })
    }
}

/// `spec.storage.ephemeral: true` together with a resolved `spec.
/// controlNodes` above `1` is rejected outright (issue #989, ADR 0060's
/// 2026-09-19 amendment to its own
/// `CONDITION_EPHEMERAL_VOTER_STORAGE_HAZARD` discussion — that condition
/// documents the still-allowed single-voter shape this function leaves
/// alone).
///
/// The mechanism (also spelled out in `crate::controller`'s matching
/// reconcile-time fallback and `crates/animus-operator/CLAUDE.md`'s issue
/// #864 section): an `emptyDir` data volume is wiped by an ordinary pod
/// recreation, and a `StatefulSet`'s pod-template config-hash annotation is
/// **one shared value for the whole StatefulSet**
/// (`desired::statefulset::restart_relevant_projection`), so *any*
/// config-affecting spec edit — not just `controlNodes` itself —
/// recreates every pod, including already-correct control voters whose own
/// role never changes. A recreated EXISTING voter then comes back with an
/// empty Raft WAL while its peers' committed config still names it with
/// real history — exactly the case issue #667's boot-time check refuses
/// **permanently**, on purpose (ADR 0009's 2026-09-15 amendment). Refuse
/// enough pre-existing voters this way and the control group loses quorum
/// for good: "one spec edit bricks the cluster."
///
/// A single voter (`spec.controlNodes` resolving to `1`) has no quorum to
/// lose beyond itself — a wipe just restarts it as a fresh single-voter
/// bootstrap, so a genuinely throwaway ephemeral dev cluster stays usable
/// and is left alone here (it still gets the informational
/// `CONDITION_EPHEMERAL_VOTER_STORAGE_HAZARD` condition from the
/// reconciler, since even a lone voter's *own data* — not quorum — is lost
/// on every recreation).
#[must_use]
pub fn validate_ephemeral_voters(new: &AnimusClusterSpec) -> Option<Violation> {
    let control_nodes = new.control_nodes_or_default();
    if new.storage.is_ephemeral() && control_nodes > 1 {
        Some(Violation {
            field: "spec.storage.ephemeral",
            message: format!(
                "spec.storage.ephemeral: true with spec.controlNodes resolving to \
                 {control_nodes} (more than one control voter) is rejected: an emptyDir data \
                 volume is wiped by any pod recreation a config-affecting spec edit triggers \
                 (a StatefulSet rolling update, not just a controlNodes change), and issue \
                 #667's boot-time check then permanently refuses each wiped EXISTING voter as \
                 unsafe to re-admit — enough refusals cost the control group its quorum for \
                 good. Use durable (PersistentVolumeClaim) storage, or set spec.controlNodes: 1."
            ),
        })
    } else {
        None
    }
}

/// Every CRD-shape rule this crate enforces purely from the spec (and, for
/// the grow-only rule, the previous spec) — the full rule list, run by both
/// the reconciler and the webhook:
///
/// - `spec.schemaVersion` is in `1..=CONTENT_SCHEMA_VERSION`
///   ([`validate_schema_version`]).
/// - `spec.nodes >= 1` ([`validate_nodes`]).
/// - `spec.controlNodes` (resolved) is `>= 1` and `<= spec.nodes`
///   ([`validate_control_nodes_within_nodes`]).
/// - `spec.controlNodes` (resolved) never decreases from `old`'s own
///   resolved value, when `old` is given ([`control_nodes_regression`]) —
///   `None` on a CREATE review, or when the reconciler has no previously-
///   applied `ConfigMap` yet.
/// - `spec.image` (resolved against its default) never changes from `old`'s
///   own resolved value, when `old` is given ([`image_change_rejection`]).
/// - `spec.storage.ephemeral: true` requires `spec.controlNodes` (resolved)
///   `<= 1` ([`validate_ephemeral_voters`]) — checked on every CREATE and
///   UPDATE, so flipping an existing multi-voter cluster to ephemeral, or
///   growing `controlNodes` past 1 on an already-ephemeral one, is rejected
///   exactly like starting out that way.
/// - `spec.tls` sets exactly one of `secretName`/`certManager`
///   ([`crate::crd::TlsSpec::validate`]).
/// - `spec.s3` is internally consistent ([`crate::crd::S3StoreSpec::
///   validate`]).
/// - `spec.backupStore`/`spec.segmentStore` are each a syntactically valid,
///   non-conflicting value ([`AnimusClusterSpec::validate_store_spec`]).
///
/// Collects **every** violation rather than stopping at the first — an
/// admission response should tell the caller everything wrong with the
/// write in one round trip, not make them fix one field at a time by
/// resubmitting. `Ok(())` iff every rule passes.
pub fn validate_spec(
    old: Option<&AnimusClusterSpec>,
    new: &AnimusClusterSpec,
) -> Result<(), Vec<Violation>> {
    let mut violations = Vec::new();

    violations.extend(validate_schema_version(new));
    violations.extend(validate_nodes(new));
    violations.extend(validate_control_nodes_within_nodes(new));
    if let Some(old) = old {
        violations.extend(control_nodes_regression(
            old.control_nodes_or_default(),
            new.control_nodes_or_default(),
        ));
        violations.extend(image_change_rejection(
            old.image_or_default(),
            new.image_or_default(),
        ));
    }
    violations.extend(validate_ephemeral_voters(new));

    if let Some(tls) = &new.tls
        && let Err(e) = tls.validate()
    {
        violations.push(Violation {
            field: "spec.tls",
            message: e,
        });
    }

    if let Some(s3) = &new.s3
        && let Err(e) = s3.validate()
    {
        violations.push(Violation {
            field: "spec.s3",
            message: e,
        });
    }

    if let Err(e) = new.validate_store_spec() {
        violations.push(Violation {
            field: "spec.backupStore/spec.segmentStore",
            message: e,
        });
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{CertManagerSpec, IssuerRef, S3StoreSpec, TlsSpec};

    fn base_spec(nodes: i32, control_nodes: Option<i32>) -> AnimusClusterSpec {
        AnimusClusterSpec {
            nodes,
            control_nodes,
            ..Default::default()
        }
    }

    #[test]
    fn schema_version_zero_and_future_are_violations_with_named_messages() {
        let mut spec = base_spec(3, None);
        spec.schema_version = 0;
        let v = validate_schema_version(&spec).expect("0 is invalid");
        assert_eq!(v.field, "spec.schemaVersion");
        assert_eq!(
            v.message,
            "animuscluster-spec schemaVersion 0 is invalid; set schemaVersion: 1"
        );
        assert!(validate_spec(None, &spec).is_err());

        spec.schema_version = 2;
        let v = validate_schema_version(&spec).expect("2 is unsupported");
        assert_eq!(
            v.message,
            "animuscluster-spec schemaVersion 2 unsupported (this operator supports up to 1); \
             upgrade the operator or set schemaVersion: 1"
        );
        let all = validate_spec(None, &spec).unwrap_err();
        assert!(
            all.iter().any(|x| x.field == "spec.schemaVersion"),
            "{all:?}"
        );

        spec.schema_version = 1;
        assert_eq!(validate_schema_version(&spec), None);
    }

    #[test]
    fn a_default_three_node_spec_is_valid() {
        let spec = base_spec(3, None);
        assert_eq!(validate_spec(None, &spec), Ok(()));
    }

    #[test]
    fn nodes_below_one_is_a_violation() {
        let spec = base_spec(0, None);
        let violations = validate_spec(None, &spec).unwrap_err();
        assert!(violations.iter().any(|v| v.field == "spec.nodes"));
    }

    #[test]
    fn control_nodes_below_one_is_a_violation() {
        let spec = base_spec(3, Some(0));
        let violations = validate_spec(None, &spec).unwrap_err();
        assert!(violations.iter().any(|v| v.field == "spec.controlNodes"));
    }

    #[test]
    fn control_nodes_above_nodes_is_a_violation() {
        let spec = base_spec(3, Some(5));
        let violations = validate_spec(None, &spec).unwrap_err();
        assert!(
            violations
                .iter()
                .any(|v| v.field == "spec.controlNodes" && v.message.contains("is below"))
        );
    }

    #[test]
    fn control_nodes_may_grow_on_update() {
        let old = base_spec(5, Some(3));
        let new = base_spec(5, Some(4));
        assert_eq!(validate_spec(Some(&old), &new), Ok(()));
    }

    #[test]
    fn control_nodes_may_not_shrink_on_update() {
        let old = base_spec(5, Some(3));
        let new = base_spec(5, Some(2));
        let violations = validate_spec(Some(&old), &new).unwrap_err();
        assert!(
            violations
                .iter()
                .any(|v| v.field == "spec.controlNodes" && v.message.contains("decreased"))
        );
    }

    #[test]
    fn control_nodes_regression_is_not_checked_on_create() {
        // No `old` at all (a CREATE review) — nothing to regress from.
        let new = base_spec(5, Some(2));
        assert_eq!(validate_spec(None, &new), Ok(()));
    }

    #[test]
    fn tls_both_shapes_set_is_a_violation() {
        let mut spec = base_spec(3, None);
        spec.tls = Some(TlsSpec {
            secret_name: Some("s".to_string()),
            cert_manager: Some(CertManagerSpec {
                issuer_ref: IssuerRef {
                    name: "i".to_string(),
                    kind: "Issuer".to_string(),
                    group: None,
                },
                duration: None,
                renew_before: None,
            }),
        });
        let violations = validate_spec(None, &spec).unwrap_err();
        assert!(violations.iter().any(|v| v.field == "spec.tls"));
    }

    #[test]
    fn tls_neither_shape_set_is_a_violation() {
        let mut spec = base_spec(3, None);
        spec.tls = Some(TlsSpec {
            secret_name: None,
            cert_manager: None,
        });
        let violations = validate_spec(None, &spec).unwrap_err();
        assert!(violations.iter().any(|v| v.field == "spec.tls"));
    }

    #[test]
    fn s3_with_no_store_set_is_a_violation() {
        let mut spec = base_spec(3, None);
        spec.s3 = Some(S3StoreSpec {
            credentials_secret_name: "creds".to_string(),
            ..Default::default()
        });
        let violations = validate_spec(None, &spec).unwrap_err();
        assert!(violations.iter().any(|v| v.field == "spec.s3"));
    }

    #[test]
    fn segment_store_cluster_keyword_is_a_violation() {
        let mut spec = base_spec(3, None);
        spec.segment_store = Some("cluster".to_string());
        let violations = validate_spec(None, &spec).unwrap_err();
        assert!(
            violations
                .iter()
                .any(|v| v.field == "spec.backupStore/spec.segmentStore")
        );
    }

    fn ephemeral_spec(nodes: i32, control_nodes: Option<i32>) -> AnimusClusterSpec {
        let mut spec = base_spec(nodes, control_nodes);
        spec.storage.ephemeral = Some(true);
        spec
    }

    #[test]
    fn ephemeral_with_one_control_node_is_allowed() {
        let spec = ephemeral_spec(3, Some(1));
        assert_eq!(validate_spec(None, &spec), Ok(()));
    }

    #[test]
    fn ephemeral_with_two_control_nodes_is_a_violation() {
        let spec = ephemeral_spec(3, Some(2));
        let violations = validate_spec(None, &spec).unwrap_err();
        assert!(
            violations
                .iter()
                .any(|v| v.field == "spec.storage.ephemeral")
        );
    }

    #[test]
    fn durable_with_two_control_nodes_is_allowed() {
        let spec = base_spec(3, Some(2));
        assert_eq!(validate_spec(None, &spec), Ok(()));
    }

    #[test]
    fn durable_with_one_control_node_is_allowed() {
        let spec = base_spec(3, Some(1));
        assert_eq!(validate_spec(None, &spec), Ok(()));
    }

    #[test]
    fn ephemeral_with_control_nodes_omitted_and_one_node_is_allowed() {
        // `control_nodes_or_default()` resolves to `min(3, nodes)` — a
        // single-node cluster with `controlNodes` omitted resolves to 1,
        // the still-allowed shape.
        let spec = ephemeral_spec(1, None);
        assert_eq!(validate_spec(None, &spec), Ok(()));
    }

    #[test]
    fn ephemeral_with_control_nodes_omitted_and_two_nodes_is_a_violation() {
        let spec = ephemeral_spec(2, None);
        let violations = validate_spec(None, &spec).unwrap_err();
        assert!(
            violations
                .iter()
                .any(|v| v.field == "spec.storage.ephemeral")
        );
    }

    #[test]
    fn ephemeral_with_control_nodes_omitted_and_three_nodes_is_a_violation() {
        // `nodes: 3` with `controlNodes` omitted resolves to `min(3, 3) ==
        // 3` — the default three-voter shape most clusters start from, and
        // exactly the combination issue #989 exists to catch.
        let spec = ephemeral_spec(3, None);
        let violations = validate_spec(None, &spec).unwrap_err();
        assert!(
            violations
                .iter()
                .any(|v| v.field == "spec.storage.ephemeral")
        );
    }

    #[test]
    fn update_flipping_an_existing_multi_voter_cluster_to_ephemeral_is_rejected() {
        let old = base_spec(3, Some(3));
        let new = ephemeral_spec(3, Some(3));
        let violations = validate_spec(Some(&old), &new).unwrap_err();
        assert!(
            violations
                .iter()
                .any(|v| v.field == "spec.storage.ephemeral")
        );
    }

    #[test]
    fn update_growing_control_nodes_past_one_on_an_already_ephemeral_cluster_is_rejected() {
        let old = ephemeral_spec(5, Some(1));
        let new = ephemeral_spec(5, Some(2));
        let violations = validate_spec(Some(&old), &new).unwrap_err();
        assert!(
            violations
                .iter()
                .any(|v| v.field == "spec.storage.ephemeral")
        );
    }

    #[test]
    fn update_keeping_a_single_ephemeral_voter_with_other_edits_is_allowed() {
        let old = ephemeral_spec(3, Some(1));
        let mut new = ephemeral_spec(5, Some(1));
        new.base_port = Some(9100);
        assert_eq!(validate_spec(Some(&old), &new), Ok(()));
    }

    #[test]
    fn update_changing_the_image_is_rejected_with_a_pointer_to_the_procedure() {
        let old = base_spec(3, None);
        let mut new = base_spec(3, None);
        new.image = Some("ghcr.io/animus-db/animusd:v2".to_string());
        let violations = validate_spec(Some(&old), &new).unwrap_err();
        assert_eq!(violations.len(), 1, "{violations:?}");
        assert_eq!(violations[0].field, "spec.image");
        let msg = &violations[0].message;
        assert!(msg.contains(AnimusClusterSpec::DEFAULT_IMAGE), "{msg}");
        assert!(msg.contains("ghcr.io/animus-db/animusd:v2"), "{msg}");
        assert!(msg.contains("whole-cluster stop"), "{msg}");
        assert!(msg.contains("ADR 0060"), "{msg}");
        assert!(msg.contains("Upgrades"), "{msg}");
    }

    #[test]
    fn update_between_two_explicit_images_is_rejected() {
        let mut old = base_spec(3, None);
        old.image = Some("example/animusd:v1".to_string());
        let mut new = base_spec(3, None);
        new.image = Some("example/animusd:v2".to_string());
        assert!(validate_spec(Some(&old), &new).is_err());
    }

    #[test]
    fn update_leaving_the_image_unchanged_is_allowed() {
        let mut old = base_spec(3, None);
        old.image = Some("example/animusd:v1".to_string());
        let mut new = base_spec(5, None);
        new.image = Some("example/animusd:v1".to_string());
        assert_eq!(validate_spec(Some(&old), &new), Ok(()));
    }

    #[test]
    fn update_normalising_none_to_the_explicit_default_image_is_allowed() {
        let old = base_spec(3, None);
        assert!(old.image.is_none());
        let mut new = base_spec(3, None);
        new.image = Some(AnimusClusterSpec::DEFAULT_IMAGE.to_string());
        assert_eq!(validate_spec(Some(&old), &new), Ok(()));
        // ... and back again.
        assert_eq!(validate_spec(Some(&new), &old), Ok(()));
    }

    #[test]
    fn create_with_any_image_is_unaffected_by_the_image_rule() {
        let mut spec = base_spec(3, None);
        spec.image = Some("example/animusd:v7".to_string());
        assert_eq!(validate_spec(None, &spec), Ok(()));
    }

    #[test]
    fn ephemeral_violation_collects_alongside_other_violations() {
        let mut spec = ephemeral_spec(0, Some(2));
        spec.segment_store = Some("cluster".to_string());
        let violations = validate_spec(None, &spec).unwrap_err();
        assert!(
            violations
                .iter()
                .any(|v| v.field == "spec.storage.ephemeral"),
            "{violations:?}"
        );
        assert!(
            violations
                .iter()
                .any(|v| v.field == "spec.backupStore/spec.segmentStore"),
            "{violations:?}"
        );
        assert!(
            violations.iter().any(|v| v.field == "spec.nodes"),
            "{violations:?}"
        );
        assert!(violations.len() >= 3, "{violations:?}");
    }

    #[test]
    fn every_rule_violated_at_once_is_reported_together() {
        let mut spec = base_spec(0, Some(0));
        spec.segment_store = Some("cluster".to_string());
        let violations = validate_spec(None, &spec).unwrap_err();
        // nodes, controlNodes (>=1 — the nodes<1 arm fires first since
        // control_nodes_or_default()=0 also fails the `< 1` check), and the
        // store conflict — at least three distinct problems reported in one
        // pass, not just the first one found.
        assert!(violations.len() >= 3, "{violations:?}");
    }
}
