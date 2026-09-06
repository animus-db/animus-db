//! The `{name}-pdb` `PodDisruptionBudget` builder (S-07c, closing ADR
//! 0060's own deferred-list item: "`PodDisruptionBudget` tuning — the
//! StatefulSet ships with none beyond Kubernetes defaults; a
//! deliberately-tuned PDB is a follow-up").
//!
//! **The budget is derived from the cluster's own quorum math, never a
//! constant.** Two independent things must each keep a majority alive
//! under a voluntary eviction (a `kubectl drain`, a node-pool upgrade, …):
//!
//! - **The control-plane Raft group** — exactly `spec.controlNodes` pods
//!   (ordinals `0..controlNodes-1`) are voters (ADR 0060's own spec table;
//!   `controlNodes` is *not* capped the way data-plane replication is —
//!   every one of them is a real voter). A group of `controlNodes` voters
//!   tolerates `floor((controlNodes - 1) / 2)` simultaneous losses before
//!   losing majority.
//! - **Every data-plane tablet group** — `animusd` places each tablet's
//!   Raft group on the first `min(N, MAX_REPLICATION_FACTOR)` `Active`
//!   members it sees, where `MAX_REPLICATION_FACTOR = 3`
//!   (`crates/animusd/src/lib.rs`) is a fixed constant today, not a
//!   `spec`-level knob — this crate has no dependency on `animusd` (see
//!   this crate's own `CLAUDE.md`), so [`DATA_PLANE_MAX_REPLICATION_FACTOR`]
//!   mirrors it by hand, the same manual-sync posture `desired::
//!   cluster_config`'s `ClusterConfig`/`RoleAddrs` JSON mirror already
//!   established. The operator does not know *which* pods actually hold
//!   any given tablet's replicas (placement is the data plane's own
//!   runtime decision, not something this builder can see), so the only
//!   safe assumption is the worst case: any `nodes`-many pods could be
//!   asked to host one, capped at the target replication factor once
//!   `nodes >= MAX_REPLICATION_FACTOR`. With effective replication factor
//!   `rf = min(nodes, MAX_REPLICATION_FACTOR)`, a tablet group tolerates
//!   `floor((rf - 1) / 2)` simultaneous replica losses.
//!
//! `maxUnavailable` is the smaller of the two — the tighter constraint
//! always wins, since a single global `PodDisruptionBudget` selecting
//! every one of the cluster's own pods (`selector_labels`) caps
//! *simultaneous* voluntary evictions cluster-wide regardless of which
//! specific pods are chosen, which is exactly what bounds both risks at
//! once. See [`safe_max_unavailable`] for the exact arithmetic and its
//! degenerate cases (`nodes == 1`, `controlNodes == 1`, `nodes <
//! MAX_REPLICATION_FACTOR`) — most of them compute `0`, which blocks every
//! voluntary eviction outright. **That is the correct, intended outcome
//! for an under-replicated cluster, not a bug to work around**: a
//! single-voter control plane, or a tablet group with only one live
//! replica, genuinely cannot survive losing its only copy, voluntarily or
//! otherwise.
//!
//! **Expressed as `maxUnavailable`, never `minAvailable`.** The two are
//! mutually exclusive on a `PodDisruptionBudgetSpec`, and either can
//! express the identical constraint against a *known* total pod count —
//! but `minAvailable` would have to be `nodes - maxUnavailable`, recomputed
//! (and re-applied) every time `spec.nodes` changes. `maxUnavailable`
//! itself is scale-invariant across the entire range that matters in
//! practice: once `nodes` reaches `MAX_REPLICATION_FACTOR` (3) and
//! `controlNodes` reaches 3 (this operator's own default),
//! [`safe_max_unavailable`] returns the same value — `1` — for every
//! `nodes` from 3 to any larger count, since `controlNodes` is immutable
//! after creation (ADR 0060) and the effective replication factor plateaus
//! at `MAX_REPLICATION_FACTOR`. A scale-up/down within that range needs no
//! PDB change at all; `apply_children` still re-derives and re-applies it
//! every reconcile (the same unconditional-re-apply posture every other
//! child already has), always from `spec.nodes`/`spec.controlNodes` —
//! **never** the `StatefulSet`'s live/current replica count, which would
//! make the budget momentarily wrong mid-scale (see `crate::controller::
//! apply_children`'s own call site for why the desired spec, not observed
//! state, is the only correct input here).
//!
//! **No CRD field for this.** An override could only ever be asked to
//! (a) loosen the computed value, which is unsafe by construction and
//! must be rejected, or (b) tighten it, which achieves nothing a smaller
//! `spec.nodes`/`spec.controlNodes` doesn't already achieve directly, and
//! (c) disable the budget outright, which would make this the *first*
//! required child this operator ever stops applying once a spec says so —
//! every other required child (`ConfigMap`/`Service`/`NetworkPolicy`/
//! `StatefulSet`) is applied unconditionally forever, and there is no
//! finalizer or deletion path (`crate::controller`'s own "no finalizer in
//! v1" doc) for a child that used to be desired and no longer is. Since
//! the safe value is already a pure, fully-determined function of two
//! existing spec fields (`nodes`, immutable `controlNodes`), there is
//! nothing left for a CRD field to usefully express — an operator user
//! who wants a different budget already has the two levers that compute
//! it. If a real need for an override surfaces later, add it then, with
//! its own deletion story worked out at the same time.

use k8s_openapi::api::policy::v1::{PodDisruptionBudget, PodDisruptionBudgetSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use super::{common_labels, owner_reference, pod_disruption_budget_name, selector_labels};
use crate::crd::{AnimusCluster, AnimusClusterSpec};

/// Mirrors `animusd::MAX_REPLICATION_FACTOR` (`crates/animusd/src/lib.rs`)
/// — this crate has no dependency on `animusd` (see this crate's own
/// `CLAUDE.md`'s "does not depend on `animusd`" note), so, like `desired::
/// cluster_config`'s `ClusterConfig`/`RoleAddrs` JSON mirror, this constant
/// needs to be kept in sync by hand if that one ever changes. There is no
/// `spec`-level replication-factor override today; if one is ever added,
/// this builder will need to read it instead of assuming the fixed
/// default.
pub const DATA_PLANE_MAX_REPLICATION_FACTOR: i32 = 3;

/// The largest `maxUnavailable` that still guarantees both a control-plane
/// majority and every data-plane tablet group's own Raft majority survive
/// any single round of simultaneous voluntary evictions, for a cluster
/// shaped `nodes` total pods / `control_nodes` of them control-voters.
///
/// `nodes`/`control_nodes` are each clamped to at least `1` first — this
/// builder does not itself re-validate `spec.nodes >= 1`/`spec.controlNodes
/// <= spec.nodes` (`crate::controller::reconcile` already refuses a scale
/// below `controlNodes`, and `AnimusClusterSpec.nodes`'s own doc states
/// "must be at least 1", pre-existing and unenforced elsewhere — not this
/// PR's gap to close); the clamp just keeps this a total function that
/// never panics or returns a negative budget on a not-yet-valid or
/// momentarily-inconsistent input.
#[must_use]
pub fn safe_max_unavailable(nodes: i32, control_nodes: i32) -> i32 {
    let control_nodes = control_nodes.max(1);
    let effective_rf = nodes.clamp(1, DATA_PLANE_MAX_REPLICATION_FACTOR);
    let control_budget = (control_nodes - 1) / 2;
    let data_budget = (effective_rf - 1) / 2;
    control_budget.min(data_budget).max(0)
}

/// Build the `PodDisruptionBudget` for `cluster`.
#[must_use]
pub fn build(cluster: &AnimusCluster, spec: &AnimusClusterSpec) -> PodDisruptionBudget {
    let name = cluster
        .metadata
        .name
        .as_deref()
        .expect("AnimusCluster read from the API server always has a name");
    let ns = cluster
        .metadata
        .namespace
        .as_deref()
        .expect("AnimusCluster read from the API server always has a namespace");

    let max_unavailable = safe_max_unavailable(spec.nodes, spec.control_nodes_or_default());

    PodDisruptionBudget {
        metadata: ObjectMeta {
            name: Some(pod_disruption_budget_name(name)),
            namespace: Some(ns.to_string()),
            labels: Some(common_labels(name)),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(PodDisruptionBudgetSpec {
            max_unavailable: Some(IntOrString::Int(max_unavailable)),
            min_available: None,
            selector: Some(LabelSelector {
                match_labels: Some(selector_labels(name)),
                ..Default::default()
            }),
            unhealthy_pod_eviction_policy: None,
        }),
        status: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::desired::statefulset;
    use crate::desired::test_support::test_cluster;

    // --- `safe_max_unavailable` arithmetic, including every degenerate ---
    // --- shape the design brief calls out ---------------------------------

    #[test]
    fn three_and_three_tolerates_one() {
        // The operator's own default shape: 3 control voters, RF plateaued
        // at 3 — both budgets are floor((3-1)/2) = 1.
        assert_eq!(safe_max_unavailable(3, 3), 1);
    }

    #[test]
    fn five_and_five_still_tolerates_only_one_data_plane_rf_is_capped() {
        // The control budget alone would allow floor((5-1)/2) = 2, but the
        // data-plane budget stays capped at floor((3-1)/2) = 1 once
        // nodes >= MAX_REPLICATION_FACTOR — the smaller of the two always
        // wins.
        assert_eq!(safe_max_unavailable(5, 5), 1);
    }

    #[test]
    fn single_node_cluster_tolerates_none() {
        // nodes == 1: only one pod exists at all — evicting it can never
        // be declared safe.
        assert_eq!(safe_max_unavailable(1, 1), 0);
    }

    #[test]
    fn single_control_voter_tolerates_none_even_with_many_data_nodes() {
        // controlNodes == 1: the control-plane Raft group has exactly one
        // voter, so losing it is never survivable — regardless of how much
        // slack the data plane's own RF would otherwise allow.
        assert_eq!(safe_max_unavailable(10, 1), 0);
    }

    #[test]
    fn two_nodes_is_below_replication_factor_and_tolerates_none() {
        // nodes < MAX_REPLICATION_FACTOR: the effective (achievable) RF is
        // min(2, 3) = 2, so floor((2-1)/2) = 0 — same answer control_nodes
        // (also 2 by the min(3, nodes) default) gives independently.
        assert_eq!(safe_max_unavailable(2, 2), 0);
    }

    #[test]
    fn control_nodes_smaller_than_nodes_uses_the_smaller_control_budget() {
        // 10 total pods, 3 control voters: RF plateaus at 3
        // (floor((3-1)/2) = 1) but so does the control budget
        // (floor((3-1)/2) = 1) — same value here, but arrived at
        // independently; see the next test for a case where they diverge.
        assert_eq!(safe_max_unavailable(10, 3), 1);
    }

    #[test]
    fn a_larger_control_voter_count_can_exceed_the_data_plane_budget_but_is_capped_by_it() {
        // 10 total pods, 7 control voters: the control budget alone would
        // allow floor((7-1)/2) = 3, but the data-plane budget
        // (floor((3-1)/2) = 1, RF plateaued at 3) is smaller and wins.
        assert_eq!(safe_max_unavailable(10, 7), 1);
    }

    #[test]
    fn max_unavailable_is_scale_invariant_once_past_the_replication_factor() {
        // The whole point of choosing maxUnavailable over minAvailable:
        // once nodes >= RF and controlNodes >= 3, the value never changes
        // across a scale-up/down, since controlNodes is immutable and RF
        // is capped.
        for nodes in [3, 4, 5, 10, 50] {
            assert_eq!(
                safe_max_unavailable(nodes, 3),
                1,
                "nodes={nodes} should still tolerate exactly 1"
            );
        }
    }

    #[test]
    fn zero_or_negative_inputs_never_panic_and_never_go_negative() {
        // Defensive only — `nodes >= 1` is a pre-existing, unenforced
        // invariant elsewhere (see this function's own doc); this builder
        // must stay a total function regardless.
        assert_eq!(safe_max_unavailable(0, 0), 0);
        assert_eq!(safe_max_unavailable(-3, -1), 0);
    }

    // --- the built object --------------------------------------------------

    #[test]
    fn golden_shape_for_the_default_three_node_cluster() {
        let cluster = test_cluster("c", "ns", 3, None);
        let pdb = build(&cluster, &cluster.spec);
        let value = serde_json::to_value(&pdb).unwrap();
        let expected = serde_json::json!({
            "apiVersion": "policy/v1",
            "kind": "PodDisruptionBudget",
            "metadata": {
                "name": "c-pdb",
                "namespace": "ns",
                "labels": {
                    "app.kubernetes.io/name": "animusdb",
                    "app.kubernetes.io/instance": "c",
                    "app.kubernetes.io/managed-by": "animus-operator"
                },
                "ownerReferences": [{
                    "apiVersion": "animusdb.io/v1alpha1",
                    "kind": "AnimusCluster",
                    "name": "c",
                    "uid": "uid-c",
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {
                "maxUnavailable": 1,
                "selector": {
                    "matchLabels": {
                        "app.kubernetes.io/name": "animusdb",
                        "app.kubernetes.io/instance": "c"
                    }
                }
            }
        });
        assert_eq!(value, expected);
    }

    #[test]
    fn name_is_derived_from_the_cluster_name() {
        let cluster = test_cluster("demo", "ns1", 5, Some(5));
        let pdb = build(&cluster, &cluster.spec);
        assert_eq!(pdb.metadata.name.as_deref(), Some("demo-pdb"));
        assert_eq!(pod_disruption_budget_name("demo"), "demo-pdb");
    }

    #[test]
    fn owner_reference_points_at_the_cluster() {
        let cluster = test_cluster("c", "ns", 3, None);
        let pdb = build(&cluster, &cluster.spec);
        let owner = &pdb.metadata.owner_references.unwrap()[0];
        assert_eq!(owner.name, "c");
        assert_eq!(owner.uid, "uid-c");
        assert_eq!(owner.controller, Some(true));
    }

    #[test]
    fn max_unavailable_reflects_the_computed_value_for_a_single_node_cluster() {
        let cluster = test_cluster("solo", "ns", 1, Some(1));
        let pdb = build(&cluster, &cluster.spec);
        assert_eq!(
            pdb.spec.unwrap().max_unavailable,
            Some(IntOrString::Int(0)),
            "a single-replica cluster must block every voluntary eviction"
        );
    }

    #[test]
    fn min_available_is_never_set() {
        // maxUnavailable and minAvailable are mutually exclusive on a
        // PodDisruptionBudgetSpec; this builder always uses the former
        // (see the module doc for why).
        let cluster = test_cluster("c", "ns", 3, None);
        let pdb = build(&cluster, &cluster.spec);
        assert_eq!(pdb.spec.unwrap().min_available, None);
    }

    #[test]
    fn selector_matches_the_statefulsets_own_pod_template_labels() {
        // Assert against the actual `statefulset::build` output, not a
        // second call to `selector_labels` — that would only prove the two
        // call sites agree with each other, not that either actually
        // matches a real pod's labels.
        let cluster = test_cluster("c", "ns", 3, None);
        let pdb = build(&cluster, &cluster.spec);
        let sts = statefulset::build(&cluster, &cluster.spec);
        let pod_labels = sts.spec.unwrap().template.metadata.unwrap().labels.unwrap();
        let pdb_selector = pdb.spec.unwrap().selector.unwrap().match_labels.unwrap();
        for (k, v) in &pdb_selector {
            assert_eq!(
                pod_labels.get(k),
                Some(v),
                "PDB selector key {k:?} must match the StatefulSet pod template's own label"
            );
        }
    }
}
