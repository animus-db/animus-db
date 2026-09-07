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

use crate::crd::AnimusClusterSpec;

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

/// Every CRD-shape rule this crate enforces purely from the spec (and, for
/// the grow-only rule, the previous spec) — the full rule list, run by both
/// the reconciler and the webhook:
///
/// - `spec.nodes >= 1` ([`validate_nodes`]).
/// - `spec.controlNodes` (resolved) is `>= 1` and `<= spec.nodes`
///   ([`validate_control_nodes_within_nodes`]).
/// - `spec.controlNodes` (resolved) never decreases from `old`'s own
///   resolved value, when `old` is given ([`control_nodes_regression`]) —
///   `None` on a CREATE review, or when the reconciler has no previously-
///   applied `ConfigMap` yet.
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

    violations.extend(validate_nodes(new));
    violations.extend(validate_control_nodes_within_nodes(new));
    if let Some(old) = old {
        violations.extend(control_nodes_regression(
            old.control_nodes_or_default(),
            new.control_nodes_or_default(),
        ));
    }

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
