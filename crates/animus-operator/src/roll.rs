//! The operator's rolling-upgrade driver (ADR 0073 Phase 3, workstream P3-D:
//! D7 `partition`, D8 no ungated window, D9 pause-and-surface).
//!
//! # The mechanism
//!
//! The `StatefulSet` uses `RollingUpdate` with **a `partition` the operator
//! owns** (never `OnDelete`: no pod-delete RBAC). Kubernetes updates only the
//! ordinals `>= partition`, highest first, one at a time. The operator
//!
//! 1. applies **every** changed pod template (image *and* the config-hash
//!    annotation, i.e. also a `spec.controlNodes` growth) together with
//!    `partition = replicas` in the *same* server-side apply, so there is no
//!    instant at which Kubernetes may roll a pod ungated ([`Stage::Start`]);
//! 2. then lowers the partition by one, only when `animus_roll::decide` says
//!    the gate is open (every member `Active`, every `GET /admin/roll-health`
//!    `ok`, nothing else in flight, the previous pod on the new revision,
//!    `Ready`, healthy and reported), after handing control leadership away
//!    from the next pod if it holds it ([`drive`]);
//! 3. at `partition 0` waits for the last pod, then finalizes (or offers to)
//!    per `spec.upgrade` ([`Effect::Finalize`], [`UpgradePhase`]).
//!
//! **The decisions are `animus-roll`'s** (the same crate `animus cluster roll`
//! uses); this module only supplies the observation (pods, admin endpoints)
//! and maps the machine's one-node-at-a-time answer onto "lower the partition
//! by one". The machine's own restart order (data nodes first, control leader
//! last) agrees with the `StatefulSet`'s highest-ordinal-first order except
//! for where the control leader sits, which is what the leadership transfer
//! is for.
//!
//! **Everything is derived from live truth every reconcile** (the
//! `StatefulSet`'s partition/revisions, the pods' `controller-revision-hash`
//! labels, the admin endpoints), so a restarted operator resumes mid-roll. The
//! only recorded state is `status.upgrade`'s clocks and image names (see
//! [`crate::crd::UpgradeStatus`]). The whole thing **fails closed**: a template
//! change whose state cannot be read holds `partition = replicas` (nothing
//! rolls) and surfaces `UpgradeBlocked`.
//!
//! # What is pure here
//!
//! [`stage`], [`platform_of`], [`drive`], [`hold_edits`], [`pdb_start_gate`]
//! take plain values and are unit-tested without a cluster; [`observe`],
//! [`execute`] and [`step`] are the imperative shell over `AdminOps` /
//! `ClusterApi`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use animus_roll::json::{Inputs, observation, parse_health};
use animus_roll::{Action, Block, Config, FinalizeMode, Health, Observation, Platform};
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Pod;
use serde_json::{Value, json};
use tracing::{info, warn};

use crate::admin_client::AdminOps;
use crate::cluster_api::ClusterApi;
use crate::controller::{Context, ReconcileError, admin_base_url};
use crate::crd::{
    AnimusCluster, AnimusClusterStatus, CONDITION_ROLL_COMPLETE, CONDITION_UPGRADE_BLOCKED,
    CONDITION_UPGRADE_FINALIZE_PENDING, CONDITION_UPGRADE_IN_PROGRESS, ClusterCondition,
    ConditionStatus, FinalizePolicy, UpgradePhase, UpgradeStatus,
};
use crate::desired;
use crate::desired::poddisruptionbudget::safe_max_unavailable;
use crate::desired::statefulset::{TEMPLATE_HASH_ANNOTATION, template_config_hash, template_image};

/// A node in flight (restarting, or on the new revision but not yet healthy and
/// reported) this long is `UpgradeBlocked` (a stall), not a plain wait (D9).
/// Covers the pod's termination grace (90 s) + boot + replica catch-up with a
/// wide margin; the roll never acts on it, it only surfaces it.
pub const STALL_AFTER: Duration = Duration::from_secs(900);

/// The label the `StatefulSet` controller stamps on every pod with the
/// `ControllerRevision` it was built from.
pub const REVISION_LABEL: &str = "controller-revision-hash";

/// The operator's own wall clock, in epoch seconds, for the roll's stall and
/// soak clocks (`status.upgrade.inFlightSince` / `settledSince`). **Not** the
/// `Env` seam: the operator is a real-socket process boundary outside it
/// (ADR 0003 / ADR 0061 Decision 4); tests substitute [`WallClock::fixed`] so
/// the clocks are exact.
#[derive(Clone, Default)]
pub struct WallClock {
    fixed: Option<Arc<AtomicI64>>,
}

impl WallClock {
    /// The real clock.
    #[must_use]
    pub fn real() -> Self {
        Self { fixed: None }
    }

    /// A settable clock for tests, starting at `secs`.
    #[must_use]
    pub fn fixed(secs: i64) -> Self {
        Self {
            fixed: Some(Arc::new(AtomicI64::new(secs))),
        }
    }

    /// Set a [`fixed`](Self::fixed) clock (no-op on the real one).
    pub fn set(&self, secs: i64) {
        if let Some(f) = &self.fixed {
            f.store(secs, Ordering::SeqCst);
        }
    }

    /// Epoch seconds now.
    #[must_use]
    pub fn now(&self) -> i64 {
        if let Some(f) = &self.fixed {
            return f.load(Ordering::SeqCst);
        }
        #[allow(
            clippy::disallowed_methods,
            reason = "animus-operator is a real-socket process boundary outside the Env seam (ADR 0003, ADR 0061 Decision 4); the roll's stall/soak clocks are the operator's own wall-clock measurements"
        )]
        let now = std::time::SystemTime::now();
        now.duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
    }
}

// ---------------------------------------------------------------------------
// Facts about the StatefulSet and its pods
// ---------------------------------------------------------------------------

/// What the live `StatefulSet` says, relative to the template this reconcile
/// is about to apply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StsView {
    pub replicas: i32,
    /// `spec.updateStrategy.rollingUpdate.partition` as stored (`0` when absent).
    pub partition: i32,
    pub update_revision: Option<String>,
    pub current_revision: Option<String>,
    /// The `StatefulSet` controller has observed the latest generation, so the
    /// revisions above describe the spec as stored. `false` right after an
    /// apply: **never** treat stale revisions as "the roll is done" (that would
    /// reset the partition to `0` and roll ungated).
    pub status_current: bool,
    /// Applying the desired template would change the pods (ADR 0073 D8).
    pub template_changed: bool,
    /// The container image in the live template.
    pub image: Option<String>,
}

impl StsView {
    /// A roll is in flight: some pod is held back by the partition, or the
    /// controller has an update revision the cluster has not fully reached.
    #[must_use]
    pub fn roll_in_flight(&self) -> bool {
        self.partition > 0
            || matches!((&self.current_revision, &self.update_revision),
                (Some(c), Some(u)) if c != u)
    }
}

/// Whether applying `desired` would change the pod template of `live`.
///
/// Compares the template fingerprint the operator stamped on the
/// `StatefulSet`'s metadata at its last apply ([`TEMPLATE_HASH_ANNOTATION`]).
/// A `StatefulSet` from an operator that predates the annotation has none: it
/// is compared by its image and config-hash annotation, and a template that
/// carries neither (nothing to compare) is *adopted* as unchanged rather than
/// guessed to differ.
#[must_use]
pub fn template_changed(live: &StatefulSet, desired: &StatefulSet) -> bool {
    let want = desired
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(TEMPLATE_HASH_ANNOTATION));
    let have = live
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(TEMPLATE_HASH_ANNOTATION));
    match (have, want) {
        (Some(h), Some(w)) => h != w,
        (Some(_), None) => false,
        (None, _) => {
            let (img, hash) = (template_image(live), template_config_hash(live));
            if img.is_none() && hash.is_none() {
                return false;
            }
            img != template_image(desired) || hash != template_config_hash(desired)
        }
    }
}

impl StsView {
    /// Read `live` against the template `desired` is about to apply.
    #[must_use]
    pub fn of(live: &StatefulSet, desired: &StatefulSet) -> Self {
        let spec = live.spec.as_ref();
        let status = live.status.as_ref();
        let generation = live.metadata.generation;
        let observed = status.and_then(|s| s.observed_generation);
        Self {
            replicas: spec.and_then(|s| s.replicas).unwrap_or(0),
            partition: spec
                .and_then(|s| s.update_strategy.as_ref())
                .and_then(|u| u.rolling_update.as_ref())
                .and_then(|r| r.partition)
                .unwrap_or(0),
            update_revision: status.and_then(|s| s.update_revision.clone()),
            current_revision: status.and_then(|s| s.current_revision.clone()),
            status_current: match (generation, observed) {
                (Some(g), Some(o)) => o >= g,
                (Some(_), None) => false,
                (None, _) => true,
            },
            template_changed: template_changed(live, desired),
            image: template_image(live),
        }
    }
}

/// One pod's roll-relevant facts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodFact {
    pub revision: Option<String>,
    pub ready: bool,
    pub terminating: bool,
}

/// The pods of cluster `name` by ordinal.
#[must_use]
pub fn pod_facts(name: &str, pods: &[Pod]) -> BTreeMap<i32, PodFact> {
    let prefix = format!("{name}-");
    pods.iter()
        .filter_map(|p| {
            let pod_name = p.metadata.name.as_deref()?;
            let ordinal: i32 = pod_name.strip_prefix(&prefix)?.parse().ok()?;
            let ready = p
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .is_some_and(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"));
            Some((
                ordinal,
                PodFact {
                    revision: p
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|l| l.get(REVISION_LABEL))
                        .cloned(),
                    ready,
                    terminating: p.metadata.deletion_timestamp.is_some(),
                },
            ))
        })
        .collect()
}

/// The platform's fact about ordinal `ordinal` (`animus_roll::Platform`):
///
/// - on the update revision and `Ready` -> `New`; on it but not `Ready` ->
///   `Restarting`;
/// - not on it: `Old` while the partition protects it (`ordinal <
///   partition`), `Restarting` once the partition admits it (the
///   `StatefulSet` controller is about to, or is, replacing it) or it is being
///   deleted;
/// - no pod at all: `Restarting` when the partition admits it, else `Old` (an
///   old pod the controller will bring back on the old revision; its health
///   is then `Unreachable`, which closes the gate).
#[must_use]
pub fn platform_of(
    fact: Option<&PodFact>,
    ordinal: i32,
    partition: i32,
    update_revision: Option<&str>,
) -> Platform {
    let admitted = ordinal >= partition;
    match fact {
        None => {
            if admitted {
                Platform::Restarting
            } else {
                Platform::Old
            }
        }
        Some(f) => {
            let on_update = update_revision.is_some()
                && f.revision.as_deref() == update_revision
                && !f.terminating;
            if on_update {
                if f.ready {
                    Platform::New
                } else {
                    Platform::Restarting
                }
            } else if f.terminating || admitted {
                Platform::Restarting
            } else {
                Platform::Old
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Stage: what kind of reconcile is this?
// ---------------------------------------------------------------------------

/// What the partition must do this reconcile, from the live `StatefulSet`
/// alone (no admin access needed).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// No `StatefulSet` yet: create it with `partition 0`.
    Fresh,
    /// Nothing is rolling: `partition 0`.
    Steady,
    /// The pod template is about to change (D8): apply it **with**
    /// `partition = replicas`, atomically. A roll already in flight is
    /// re-targeted the same way (fix forward): the partition goes back up so
    /// the new revision is gated from the top.
    Start { partition: i32 },
    /// The `StatefulSet` controller has not yet observed our last apply: keep
    /// the stored partition untouched and look again.
    Hold { partition: i32 },
    /// A roll is in flight: observe, decide, maybe lower the partition.
    Drive { partition: i32 },
}

/// Classify this reconcile. `upgrade` is `status.upgrade`: a recorded roll
/// that is not [`UpgradePhase::Complete`] keeps the driver running at
/// `partition 0` (the tail: last pod, era, finalize).
#[must_use]
pub fn stage(live: Option<&StsView>, upgrade: Option<&UpgradeStatus>) -> Stage {
    let Some(v) = live else {
        return Stage::Fresh;
    };
    if v.template_changed {
        return Stage::Start {
            partition: v.replicas,
        };
    }
    if !v.status_current {
        return Stage::Hold {
            partition: v.partition,
        };
    }
    let unfinished = upgrade.is_some_and(|u| u.phase != UpgradePhase::Complete);
    if v.roll_in_flight() || unfinished {
        Stage::Drive {
            partition: v.partition,
        }
    } else {
        Stage::Steady
    }
}

/// D7/maintainer decision 6: a roll on a shape whose PDB `maxUnavailable` is
/// `0` (one control node, fewer than three nodes) is an outage by
/// construction, so the operator refuses to *start* one: while the partition
/// still protects every pod, the answer is `Some(message)` and nothing is
/// observed or lowered. (The documented way through is the Phase 1
/// whole-cluster stop-upgrade-restart.)
#[must_use]
pub fn pdb_start_gate(partition: i32, replicas: i32, control_nodes: i32) -> Option<String> {
    if partition < replicas {
        return None;
    }
    let max_unavailable = safe_max_unavailable(replicas, control_nodes);
    (max_unavailable < 1).then(|| {
        format!(
            "cluster cannot tolerate losing a node (PDB maxUnavailable 0: {replicas} node(s), \
             {control_nodes} control node(s)); a rolling restart here is an outage by \
             construction, so the roll is not started. Use the whole-cluster \
             stop-upgrade-restart, or grow to at least 3 nodes with 3 control nodes first"
        )
    })
}

// ---------------------------------------------------------------------------
// Drive: one decision
// ---------------------------------------------------------------------------

/// An admin call the driver wants made after deciding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// `POST /admin/control/transfer {to}` on the current control leader (the
    /// next pod to be restarted).
    Transfer { leader_ordinal: i32, to: String },
    /// `POST /admin/cluster-version/finalize {to, expected}` on the control
    /// leader. Irreversible.
    Finalize {
        leader_ordinal: i32,
        to: u32,
        expected: u32,
    },
}

/// A condition to set (`Some(message)`) or clear (`None`).
pub type CondChange = (&'static str, Option<String>);

/// Everything one [`drive`] decision changes.
#[derive(Clone, Debug, PartialEq)]
pub struct Outcome {
    /// The partition to apply this reconcile.
    pub partition: i32,
    pub effect: Option<Effect>,
    pub phase: UpgradePhase,
    pub conds: Vec<CondChange>,
    pub in_flight: Option<(String, i64)>,
    pub settled_since: Option<i64>,
    pub from_version: Option<u32>,
    pub to_version: Option<u32>,
    pub active: Option<u32>,
    pub on_new: Option<i32>,
    pub total: Option<i32>,
}

/// The ordinal of node id `node` in cluster `name` (`{name}-{ordinal}`).
#[must_use]
pub fn ordinal_of(name: &str, node: &str) -> Option<i32> {
    node.strip_prefix(&format!("{name}-"))?.parse().ok()
}

fn blocked_conds(msg: String) -> Vec<CondChange> {
    vec![
        (
            CONDITION_UPGRADE_IN_PROGRESS,
            Some("roll paused; see UpgradeBlocked".to_string()),
        ),
        (CONDITION_UPGRADE_BLOCKED, Some(msg)),
        (CONDITION_UPGRADE_FINALIZE_PENDING, None),
        (CONDITION_ROLL_COMPLETE, None),
    ]
}

fn progress_conds(msg: String) -> Vec<CondChange> {
    vec![
        (CONDITION_UPGRADE_IN_PROGRESS, Some(msg)),
        (CONDITION_UPGRADE_BLOCKED, None),
        (CONDITION_UPGRADE_FINALIZE_PENDING, None),
        (CONDITION_ROLL_COMPLETE, None),
    ]
}

/// The outcome for a roll that cannot be evaluated (admin unreachable, no
/// verdict, PDB `0`): hold the partition exactly where it is, surface why.
/// **Fail closed.**
#[must_use]
pub fn blocked(partition: i32, prev: Option<&UpgradeStatus>, msg: String) -> Outcome {
    Outcome {
        partition,
        effect: None,
        phase: UpgradePhase::Blocked,
        conds: blocked_conds(msg),
        in_flight: prev.and_then(|p| Some((p.in_flight_node.clone()?, p.in_flight_since?))),
        settled_since: None,
        from_version: prev.and_then(|p| p.from_version),
        to_version: prev.and_then(|p| p.to_version),
        active: prev.and_then(|p| p.active_cluster_version),
        on_new: prev.and_then(|p| p.on_new),
        total: prev.and_then(|p| p.total),
    }
}

/// The inputs [`drive`] needs beyond the observation.
pub struct DriveInput<'a> {
    pub cluster: &'a str,
    pub finalize: FinalizePolicy,
    pub soak: Duration,
    pub partition: i32,
    pub replicas: i32,
    pub now: i64,
    pub prev: Option<&'a UpgradeStatus>,
}

fn secs_since(now: i64, since: i64) -> Duration {
    Duration::from_secs(u64::try_from(now.saturating_sub(since)).unwrap_or(0))
}

/// Decide this reconcile's step of the roll from `obs` (built without clocks)
/// and map it onto the partition.
///
/// `animus_roll::decide` says what the gate allows; the partition can only
/// move one ordinal at a time, in the `StatefulSet`'s own order:
///
/// - `Restart`/`TransferControlLeadership` (the gate is open): lower the
///   partition to `partition - 1`, **unless** that pod is the control leader,
///   in which case leadership is transferred first and the partition holds
///   (the next reconcile lowers it);
/// - `Wait`/`Blocked`/`Soak`/`AwaitEra`: hold;
/// - `ReadyToFinalize`/`Finalize`/`Complete`: every pod is done, `partition 0`.
#[must_use]
pub fn drive(input: &DriveInput<'_>, obs: &Observation) -> Outcome {
    let cfg = Config {
        finalize: match input.finalize {
            FinalizePolicy::Manual => FinalizeMode::Manual,
            FinalizePolicy::Auto => FinalizeMode::Auto { soak: input.soak },
        },
        stall_after: STALL_AFTER,
    };
    let prev = input.prev;
    let mut obs = obs.clone();
    // The StatefulSet replaces pods highest ordinal first, so the pod the
    // partition is about to admit is the highest ordinal still on the old
    // binary below it; the gate is judged for *that* node (its own verdict is
    // excluded, a control leader gets its transfer), not for the node the
    // machine's own order would prefer.
    let next_ord = obs
        .nodes
        .iter()
        .filter(|n| n.platform == Platform::Old)
        .filter_map(|n| ordinal_of(input.cluster, &n.id))
        .filter(|o| *o < input.partition)
        .max();
    let next_id = next_ord.map(|o| desired::cluster_config::node_id(input.cluster, o));
    let decide = |o: &Observation| animus_roll::decide_with_target(&cfg, o, next_id.as_deref());
    let mut act = decide(&obs);

    // The stall clock is ours: the machine takes "how long has the in-flight
    // node been in flight" as an input. First pass without it, then, if the
    // same node was already in flight last time, again with the elapsed time.
    if let Action::Wait { node, .. } = &act
        && let Some(p) = prev
        && p.in_flight_node.as_deref() == Some(node.as_str())
        && let Some(since) = p.in_flight_since
    {
        obs.in_flight_for = Some(secs_since(input.now, since));
        act = decide(&obs);
    }
    // Likewise the soak clock.
    if let Action::Soak { .. } = &act
        && let Some(since) = prev.and_then(|p| p.settled_since)
    {
        obs.settled_for = Some(secs_since(input.now, since));
        act = decide(&obs);
    }

    let in_flight = match &act {
        Action::Wait { node, .. } | Action::Blocked(Block::NodeStalled { node, .. }) => {
            let since = prev
                .filter(|p| p.in_flight_node.as_deref() == Some(node.as_str()))
                .and_then(|p| p.in_flight_since)
                .unwrap_or(input.now);
            Some((node.clone(), since))
        }
        _ => None,
    };
    let settled_since = match &act {
        Action::Soak { .. }
        | Action::Finalize { .. }
        | Action::ReadyToFinalize { .. }
        | Action::Complete => Some(prev.and_then(|p| p.settled_since).unwrap_or(input.now)),
        _ => None,
    };

    let from_version = prev.and_then(|p| p.from_version).or(Some(obs.active));
    let to_version = if obs.goal > from_version.unwrap_or(obs.active) {
        Some(obs.goal)
    } else {
        prev.and_then(|p| p.to_version)
    };
    let on_new = obs
        .nodes
        .iter()
        .filter(|n| n.platform == Platform::New)
        .count();
    let mut out = Outcome {
        partition: input.partition,
        effect: None,
        phase: UpgradePhase::InProgress,
        conds: Vec::new(),
        in_flight,
        settled_since,
        from_version,
        to_version,
        active: Some(obs.active),
        on_new: i32::try_from(on_new).ok(),
        total: i32::try_from(obs.nodes.len()).ok(),
    };

    match act {
        Action::Restart { node } => match next_ord {
            Some(next) if ordinal_of(input.cluster, &node) == Some(next) => {
                out.partition = next;
                out.conds = progress_conds(format!(
                    "gate open: lowered partition to {next}; pod ordinal {next} is being \
                     restarted on the new revision"
                ));
            }
            _ => {
                out.conds = progress_conds(format!(
                    "gate open for {node} but it is not the next pod the partition admits; \
                     waiting for the next safe step"
                ));
            }
        },
        Action::TransferControlLeadership { from, to } => match ordinal_of(input.cluster, &from) {
            Some(leader_ordinal) => {
                out.conds = progress_conds(format!(
                    "{from} is the control leader and is next: transferring leadership to \
                         {to} before restarting it"
                ));
                out.effect = Some(Effect::Transfer { leader_ordinal, to });
            }
            None => {
                out.conds = progress_conds(format!("cannot map control leader {from} to a pod"));
            }
        },
        Action::Wait { node, why } => {
            out.conds = progress_conds(format!("waiting for {node}: {}", why.join("; ")));
        }
        Action::Blocked(b) => {
            out.phase = UpgradePhase::Blocked;
            out.conds = blocked_conds(b.to_string());
        }
        Action::AwaitEra => {
            out.partition = 0;
            out.conds = progress_conds(
                "every pod is on the new binary; waiting for the versioning era to start"
                    .to_string(),
            );
        }
        Action::Soak { remaining } => {
            out.partition = 0;
            out.conds = progress_conds(format!(
                "every pod is on the new revision and healthy; soaking {}s before finalize",
                remaining.as_secs()
            ));
        }
        Action::ReadyToFinalize { to } => {
            out.partition = 0;
            out.phase = UpgradePhase::FinalizePending;
            out.conds = vec![
                (CONDITION_UPGRADE_IN_PROGRESS, None),
                (CONDITION_UPGRADE_BLOCKED, None),
                (
                    CONDITION_UPGRADE_FINALIZE_PENDING,
                    Some(format!(
                        "every pod runs the new binary and can_finalize holds: run `animus \
                         cluster finalize` to raise the cluster version to {to} (irreversible), \
                         or set spec.upgrade.finalize: Auto"
                    )),
                ),
                (CONDITION_ROLL_COMPLETE, None),
            ];
        }
        Action::Finalize { to, expected } => {
            out.partition = 0;
            out.phase = UpgradePhase::FinalizePending;
            match obs
                .control_leader
                .as_deref()
                .and_then(|l| ordinal_of(input.cluster, l))
            {
                Some(leader_ordinal) => {
                    out.conds = vec![
                        (CONDITION_UPGRADE_IN_PROGRESS, None),
                        (CONDITION_UPGRADE_BLOCKED, None),
                        (
                            CONDITION_UPGRADE_FINALIZE_PENDING,
                            Some(format!("finalizing cluster version {expected} -> {to}")),
                        ),
                        (CONDITION_ROLL_COMPLETE, None),
                    ];
                    out.effect = Some(Effect::Finalize {
                        leader_ordinal,
                        to,
                        expected,
                    });
                }
                None => {
                    out.phase = UpgradePhase::Blocked;
                    out.conds =
                        blocked_conds("cannot finalize: no control leader is known".to_string());
                }
            }
        }
        Action::Complete => {
            out.partition = 0;
            out.phase = UpgradePhase::Complete;
            let msg = if obs.active > from_version.unwrap_or(obs.active) {
                format!("roll complete: cluster version is now {}", obs.active)
            } else {
                "roll complete: every pod is on the new revision (no cluster-version change)"
                    .to_string()
            };
            out.conds = vec![
                (CONDITION_UPGRADE_IN_PROGRESS, None),
                (CONDITION_UPGRADE_BLOCKED, None),
                (CONDITION_UPGRADE_FINALIZE_PENDING, None),
                (CONDITION_ROLL_COMPLETE, Some(msg)),
            ];
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Holding other edits during a roll (D8 case 2, D9 revert)
// ---------------------------------------------------------------------------

/// The recorded roll refuses a revert: `desired_image` equals the pre-roll
/// image, the roll's own target differs from it, and a pod has already run the
/// new binary (so a revert would re-roll new-binary nodes onto the old binary,
/// whose formats may not read what the new one wrote: Option B). Returns the
/// image to keep (the roll's target). Shared by the webhook
/// (`crate::validate::validate_image_revert`) and the reconciler's own check.
#[must_use]
pub fn image_revert_target<'a>(
    upgrade: Option<&'a UpgradeStatus>,
    desired_image: &str,
) -> Option<&'a str> {
    let u = upgrade?;
    let (from, to) = (u.from_image.as_deref()?, u.to_image.as_deref()?);
    (u.phase != UpgradePhase::Complete
        && u.on_new.unwrap_or(0) > 0
        && from != to
        && desired_image == from)
        .then_some(to)
}

/// While a roll is in flight, pin edits the roll cannot absorb (ADR 0073 D8
/// case 2 and D9): a `spec.nodes` or `spec.controlNodes` change waits for
/// `RollComplete`, and an image revert after a pod reported the new range is
/// refused. Returns the effective cluster to reconcile and one note per held
/// edit (empty when nothing was held). `prior_control_nodes` is the value the
/// last reconcile applied.
#[must_use]
pub fn hold_edits(
    cluster: &AnimusCluster,
    live: Option<&StsView>,
    prior_control_nodes: Option<i32>,
) -> (AnimusCluster, Vec<String>) {
    let mut pinned = cluster.clone();
    let mut notes = Vec::new();
    let upgrade = cluster.status.as_ref().and_then(|s| s.upgrade.as_ref());

    if let Some(to) = image_revert_target(upgrade, cluster.spec.image_or_default()) {
        notes.push(format!(
            "refusing to revert spec.image to {}: a pod already runs the new binary, and \
             rolling it back onto the old one may not read what the new one wrote (fix \
             forward: set spec.image to a fixed image)",
            cluster.spec.image_or_default()
        ));
        pinned.spec.image = Some(to.to_string());
    }

    let Some(v) = live.filter(|v| v.roll_in_flight()) else {
        return (pinned, notes);
    };
    if cluster.spec.nodes != v.replicas && v.replicas > 0 {
        notes.push(format!(
            "holding spec.nodes {} -> {} until the roll completes (running {})",
            v.replicas, cluster.spec.nodes, v.replicas
        ));
        pinned.spec.nodes = v.replicas;
    }
    if let Some(prior) = prior_control_nodes
        && cluster.spec.control_nodes_or_default() != prior
    {
        notes.push(format!(
            "holding spec.controlNodes {} -> {} until the roll completes (running {})",
            prior,
            cluster.spec.control_nodes_or_default(),
            prior
        ));
        pinned.spec.control_nodes = Some(prior);
    }
    (pinned, notes)
}

// ---------------------------------------------------------------------------
// Status application
// ---------------------------------------------------------------------------

fn set_cond(status: &mut AnimusClusterStatus, type_: &str, message: String) {
    status.conditions.retain(|c| c.type_ != type_);
    status.conditions.push(ClusterCondition {
        type_: type_.to_string(),
        status: ConditionStatus::True,
        reason: Some(type_.to_string()),
        message: Some(message),
        last_transition_time: None,
    });
}

fn clear_cond(status: &mut AnimusClusterStatus, type_: &str) {
    status.conditions.retain(|c| c.type_ != type_);
}

/// Record `out` on `status`.
pub fn apply_outcome(status: &mut AnimusClusterStatus, out: &Outcome) {
    for (t, msg) in &out.conds {
        match msg {
            Some(m) => set_cond(status, t, m.clone()),
            None => clear_cond(status, t),
        }
    }
    let u = status
        .upgrade
        .get_or_insert_with(|| blank_upgrade(out.phase));
    u.phase = out.phase;
    u.from_version = out.from_version;
    u.to_version = out.to_version;
    u.active_cluster_version = out.active;
    u.on_new = out.on_new;
    u.total = out.total;
    u.in_flight_node = out.in_flight.as_ref().map(|(n, _)| n.clone());
    u.in_flight_since = out.in_flight.as_ref().map(|(_, s)| *s);
    u.settled_since = out.settled_since;
}

fn blank_upgrade(phase: UpgradePhase) -> UpgradeStatus {
    UpgradeStatus {
        phase,
        from_version: None,
        to_version: None,
        on_new: None,
        total: None,
        active_cluster_version: None,
        from_image: None,
        to_image: None,
        in_flight_node: None,
        in_flight_since: None,
        settled_since: None,
    }
}

/// Record a roll *start* (the template is changing, `partition = replicas`
/// applied atomically): a fresh `status.upgrade`, or a re-target of a roll
/// already in flight (the pre-roll image and cluster version are kept; the
/// clocks restart).
pub fn begin_roll(
    status: &mut AnimusClusterStatus,
    live_image: Option<String>,
    desired_image: Option<String>,
    replicas: i32,
    pdb_refusal: Option<&str>,
) {
    let prior = status
        .upgrade
        .take()
        .filter(|u| u.phase != UpgradePhase::Complete);
    let mut u = blank_upgrade(UpgradePhase::InProgress);
    u.from_image = prior
        .as_ref()
        .and_then(|p| p.from_image.clone())
        .or(live_image);
    u.to_image = desired_image;
    u.from_version = prior.as_ref().and_then(|p| p.from_version);
    u.to_version = prior.as_ref().and_then(|p| p.to_version);
    u.active_cluster_version = prior.as_ref().and_then(|p| p.active_cluster_version);
    u.on_new = Some(0);
    u.total = Some(replicas);
    let msg = format!(
        "pod template changed: applied with partition {replicas} (nothing rolls until the gate \
         opens)"
    );
    match pdb_refusal {
        Some(why) => {
            u.phase = UpgradePhase::Blocked;
            for (t, m) in blocked_conds(why.to_string()) {
                match m {
                    Some(m) => set_cond(status, t, m),
                    None => clear_cond(status, t),
                }
            }
        }
        None => {
            for (t, m) in progress_conds(msg) {
                match m {
                    Some(m) => set_cond(status, t, m),
                    None => clear_cond(status, t),
                }
            }
        }
    }
    status.upgrade = Some(u);
}

// ---------------------------------------------------------------------------
// The imperative shell
// ---------------------------------------------------------------------------

fn is_not_found(err: &str) -> bool {
    err.contains("status 404")
}

/// Where and how to reach the cluster's admin ports.
pub struct Target<'a> {
    pub name: &'a str,
    pub ns: &'a str,
    pub admin_port: i32,
    pub tls_ca: Option<&'a [u8]>,
}

impl Target<'_> {
    fn url(&self, ordinal: i32, path: &str) -> String {
        format!(
            "{}{path}",
            admin_base_url(
                self.name,
                self.ns,
                ordinal,
                self.admin_port,
                self.tls_ca.is_some()
            )
        )
    }
}

/// Build an [`Observation`] (without clocks) from the pods and the admin
/// endpoints. `Err` is "cannot evaluate the gate": the caller fails closed.
///
/// - per-pod platform from the pod's revision label ([`platform_of`]);
/// - per-`Ready`-pod `GET /admin/roll-health` (a `404` is `Unavailable`: a
///   previous-release node; anything else that fails is `Unreachable`); a pod
///   that is not `Ready` is `Unreachable` without a call;
/// - `GET /admin/cluster-version` from every new-binary pod and, until one
///   answers, from the others; **`goal` is the highest `own_range.max` a new
///   pod reports** (so a config-only roll has `goal == active` and waits for
///   no era), never recomputed from `active`;
/// - a first roll over Phase 1 binaries has no `cluster-version` anywhere: the
///   members are read from `GET /admin/status` and the era is reported
///   inactive (`animus-roll` then gates on platform + roll-health and ends in
///   `AwaitEra`);
/// - the control leader from `GET /admin/health` `is_control_leader` on the
///   control ordinals.
pub async fn observe<A: AdminOps>(
    admin: &A,
    t: &Target<'_>,
    replicas: i32,
    control_nodes: i32,
    partition: i32,
    update_revision: Option<&str>,
    pods: &BTreeMap<i32, PodFact>,
) -> Result<Observation, String> {
    let mut platform = BTreeMap::new();
    let mut health = BTreeMap::new();
    let mut views: Vec<(Platform, Value)> = Vec::new();
    let mut saw_not_found = false;
    let mut first_ready: Option<i32> = None;
    let mut new_own_max: Option<u32> = None;

    for i in 0..replicas {
        let id = desired::cluster_config::node_id(t.name, i);
        let fact = pods.get(&i);
        let pf = platform_of(fact, i, partition, update_revision);
        platform.insert(id.clone(), pf);
        if !fact.is_some_and(|f| f.ready && !f.terminating) {
            health.insert(id, Health::Unreachable);
            continue;
        }
        first_ready.get_or_insert(i);
        let h = match admin
            .get_json(&t.url(i, "/admin/roll-health"), t.tls_ca)
            .await
        {
            Ok(v) => parse_health(&v),
            Err(e) if is_not_found(&e) => Health::Unavailable,
            Err(_) => Health::Unreachable,
        };
        health.insert(id, h);
        if pf == Platform::New || views.is_empty() {
            match admin
                .get_json(&t.url(i, "/admin/cluster-version"), t.tls_ca)
                .await
            {
                Ok(v) => {
                    if pf == Platform::New
                        && let Some(m) = v
                            .get("own_range")
                            .and_then(|r| r.get("max"))
                            .and_then(Value::as_u64)
                    {
                        let m = u32::try_from(m).unwrap_or(u32::MAX);
                        new_own_max = Some(new_own_max.map_or(m, |x| x.max(m)));
                    }
                    views.push((pf, v));
                }
                Err(e) if is_not_found(&e) => saw_not_found = true,
                Err(_) => {}
            }
        }
    }

    let view = match views
        .iter()
        .find(|(p, _)| *p == Platform::New)
        .or_else(|| views.first())
    {
        Some((_, v)) => v.clone(),
        None if saw_not_found => {
            let i = first_ready.ok_or("no pod is Ready")?;
            phase1_view(admin, t, i, replicas, control_nodes).await?
        }
        None => {
            return Err("cannot read /admin/cluster-version from any Ready pod".to_string());
        }
    };

    let active = view.get("active").and_then(Value::as_u64).unwrap_or(0);
    let active = u32::try_from(active).unwrap_or(u32::MAX);
    let goal = new_own_max.map_or(active, |m| m.max(active));

    let mut control_leader = None;
    for i in 0..control_nodes.min(replicas) {
        if !pods.get(&i).is_some_and(|f| f.ready && !f.terminating) {
            continue;
        }
        if let Ok(v) = admin.get_json(&t.url(i, "/admin/health"), t.tls_ca).await
            && v.get("is_control_leader").and_then(Value::as_bool) == Some(true)
        {
            control_leader = Some(desired::cluster_config::node_id(t.name, i));
            break;
        }
    }

    observation(
        &view,
        &Inputs {
            goal,
            platform,
            health,
            control_leader,
            in_flight_for: None,
            settled_for: None,
        },
    )
}

/// The `cluster-version` view a Phase 1 cluster cannot serve, synthesized from
/// `GET /admin/status` on pod `ordinal`: era inactive, members and statuses
/// from the replicated metadata, roles from the ordinal split.
async fn phase1_view<A: AdminOps>(
    admin: &A,
    t: &Target<'_>,
    ordinal: i32,
    replicas: i32,
    control_nodes: i32,
) -> Result<Value, String> {
    let status = admin
        .get_json(&t.url(ordinal, "/admin/status"), t.tls_ca)
        .await
        .map_err(|e| format!("cannot read /admin/status from pod ordinal {ordinal}: {e}"))?;
    let members = status.get("members");
    let nodes: Vec<Value> = (0..replicas)
        .map(|i| {
            let id = desired::cluster_config::node_id(t.name, i);
            let st = members
                .and_then(|m| m.get(&id))
                .and_then(|m| m.get("status"))
                .and_then(Value::as_str)
                .map(str::to_string);
            json!({
                "node": id,
                "role": if i < control_nodes { "combined" } else { "data" },
                "status": st,
            })
        })
        .collect();
    Ok(json!({
        "era_active": false,
        "active": 0,
        "can_finalize": false,
        "blockers": [],
        "nodes": nodes,
    }))
}

/// Run `effect` against the control leader's admin port.
pub async fn execute<A: AdminOps>(
    admin: &A,
    t: &Target<'_>,
    effect: &Effect,
) -> Result<(), String> {
    match effect {
        Effect::Transfer { leader_ordinal, to } => admin
            .post_json(
                &t.url(*leader_ordinal, "/admin/control/transfer"),
                &json!({ "to": to }),
                t.tls_ca,
            )
            .await
            .map(|_| ())
            .map_err(|e| format!("transferring control leadership to {to}: {e}")),
        Effect::Finalize {
            leader_ordinal,
            to,
            expected,
        } => admin
            .post_json(
                &t.url(*leader_ordinal, "/admin/cluster-version/finalize"),
                &json!({ "to": to, "expected": expected }),
                t.tls_ca,
            )
            .await
            .map(|_| ())
            .map_err(|e| format!("finalizing cluster version {expected} -> {to}: {e}")),
    }
}

/// What a reconcile's roll step decided.
pub struct StepOut {
    /// The partition to build the `StatefulSet` with.
    pub partition: i32,
    /// A roll is active this reconcile: requeue quickly.
    pub active: bool,
}

/// The roll step of one reconcile, run **before** the `StatefulSet` is
/// applied: classify the reconcile ([`stage`]), and for a roll in flight
/// observe, decide ([`drive`]) and execute the one admin call it asks for.
/// Returns the partition to apply and records everything on `status`.
///
/// `desired` is the `StatefulSet` about to be applied (any partition),
/// `live` what is stored now.
///
/// # Errors
/// Only a Kubernetes API failure listing pods; admin failures never error,
/// they fail the gate closed ([`blocked`]).
pub async fn step<C: ClusterApi, A: AdminOps>(
    ctx: &Context<C, A>,
    cluster: &AnimusCluster,
    ns: &str,
    desired: &StatefulSet,
    live: Option<&StatefulSet>,
    tls_ca: Option<&[u8]>,
    status: &mut AnimusClusterStatus,
) -> Result<StepOut, ReconcileError> {
    let name = cluster
        .metadata
        .name
        .clone()
        .ok_or(ReconcileError::MissingName)?;
    let view = live.map(|l| StsView::of(l, desired));
    let control_nodes = cluster.spec.control_nodes_or_default();
    let st = stage(view.as_ref(), status.upgrade.as_ref());
    match st {
        Stage::Fresh | Stage::Steady => Ok(StepOut {
            partition: 0,
            active: false,
        }),
        Stage::Hold { partition } => Ok(StepOut {
            partition,
            active: true,
        }),
        Stage::Start { partition } => {
            let v = view.as_ref().expect("Start implies a live StatefulSet");
            let refusal = pdb_start_gate(partition, v.replicas, control_nodes);
            begin_roll(
                status,
                v.image.clone(),
                template_image(desired),
                v.replicas,
                refusal.as_deref(),
            );
            info!(cluster = %name, partition, "pod template changed: applying with partition = replicas");
            Ok(StepOut {
                partition,
                active: true,
            })
        }
        Stage::Drive { partition } => {
            let v = view.as_ref().expect("Drive implies a live StatefulSet");
            let prev = status.upgrade.clone();
            if let Some(why) = pdb_start_gate(partition, v.replicas, control_nodes) {
                apply_outcome(status, &blocked(partition, prev.as_ref(), why));
                return Ok(StepOut {
                    partition,
                    active: true,
                });
            }
            let pods = ctx
                .cluster_api
                .list_pods(ns, &desired::selector_labels(&name))
                .await?;
            let facts = pod_facts(&name, &pods);
            let target = Target {
                name: &name,
                ns,
                admin_port: cluster.spec.base_port_or_default()
                    + desired::cluster_config::PORT_ADMIN,
                tls_ca,
            };
            let obs = observe(
                &ctx.admin,
                &target,
                v.replicas,
                control_nodes,
                partition,
                v.update_revision.as_deref(),
                &facts,
            )
            .await;
            let mut out = match obs {
                Err(why) => {
                    warn!(cluster = %name, error = %why, "roll gate cannot be evaluated; holding the partition");
                    blocked(
                        partition,
                        prev.as_ref(),
                        format!("cannot evaluate the roll gate (failing closed): {why}"),
                    )
                }
                Ok(obs) => drive(
                    &DriveInput {
                        cluster: &name,
                        finalize: cluster.spec.finalize_policy(),
                        soak: Duration::from_secs(u64::from(
                            cluster.spec.soak_seconds_or_default(),
                        )),
                        partition,
                        replicas: v.replicas,
                        now: ctx.clock.now(),
                        prev: prev.as_ref(),
                    },
                    &obs,
                ),
            };
            let finalized_to = match &out.effect {
                Some(Effect::Finalize { to, .. }) => Some(*to),
                _ => None,
            };
            if let Some(effect) = out.effect.clone() {
                match execute(&ctx.admin, &target, &effect).await {
                    Ok(()) => {
                        info!(cluster = %name, ?effect, "roll effect executed");
                        if let Some(to) = finalized_to {
                            out.phase = UpgradePhase::Complete;
                            out.active = Some(to);
                            out.conds = vec![
                                (CONDITION_UPGRADE_IN_PROGRESS, None),
                                (CONDITION_UPGRADE_BLOCKED, None),
                                (CONDITION_UPGRADE_FINALIZE_PENDING, None),
                                (
                                    CONDITION_ROLL_COMPLETE,
                                    Some(format!(
                                        "roll complete: cluster version finalized to {to}"
                                    )),
                                ),
                            ];
                        }
                    }
                    Err(e) => {
                        warn!(cluster = %name, error = %e, "roll effect failed; retrying next reconcile");
                        let msg = format!("{e} (retried every reconcile, never forced)");
                        if finalized_to.is_some() {
                            out.conds
                                .retain(|(t, _)| *t != CONDITION_UPGRADE_FINALIZE_PENDING);
                            out.conds
                                .push((CONDITION_UPGRADE_FINALIZE_PENDING, Some(msg)));
                        } else {
                            out.phase = UpgradePhase::Blocked;
                            out.conds = blocked_conds(msg);
                        }
                    }
                }
            }
            let partition = out.partition;
            apply_outcome(status, &out);
            Ok(StepOut {
                partition,
                active: out.phase != UpgradePhase::Complete,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_roll::{NodeObs, Reason, Role};

    fn view(replicas: i32, partition: i32) -> StsView {
        StsView {
            replicas,
            partition,
            update_revision: Some("rev-2".into()),
            current_revision: Some("rev-1".into()),
            status_current: true,
            template_changed: false,
            image: Some("img:1".into()),
        }
    }

    fn node(id: &str, role: Role, platform: Platform) -> NodeObs {
        NodeObs {
            id: id.to_string(),
            role,
            status: Some("Active".to_string()),
            platform,
            reported_new: true,
            health: Health::Ok,
        }
    }

    /// `c-0..c-3`: three combined voters, one data node; era on, goal 2.
    fn obs(platforms: [Platform; 4], leader: Option<&str>) -> Observation {
        Observation {
            era_active: true,
            active: 1,
            goal: 2,
            can_finalize: false,
            finalize_blockers: vec![],
            nodes: vec![
                node("c-0", Role::Combined, platforms[0]),
                node("c-1", Role::Combined, platforms[1]),
                node("c-2", Role::Combined, platforms[2]),
                node("c-3", Role::Data, platforms[3]),
            ],
            control_leader: leader.map(str::to_string),
            in_flight_for: None,
            settled_for: None,
        }
    }

    fn input<'a>(partition: i32, now: i64, prev: Option<&'a UpgradeStatus>) -> DriveInput<'a> {
        DriveInput {
            cluster: "c",
            finalize: FinalizePolicy::Manual,
            soak: Duration::ZERO,
            partition,
            replicas: 4,
            now,
            prev,
        }
    }

    use Platform::{New, Old, Restarting};

    // --- stage -----------------------------------------------------------

    #[test]
    fn stage_classifies_every_live_shape() {
        assert_eq!(stage(None, None), Stage::Fresh);
        let mut v = view(4, 0);
        v.current_revision = v.update_revision.clone();
        assert_eq!(stage(Some(&v), None), Stage::Steady);
        // A template change always starts with partition = replicas, even
        // mid-roll (fix forward re-gates from the top).
        let mut changed = view(4, 2);
        changed.template_changed = true;
        assert_eq!(stage(Some(&changed), None), Stage::Start { partition: 4 });
        // The controller has not seen our apply yet: do not touch the partition.
        let mut stale = view(4, 4);
        stale.status_current = false;
        assert_eq!(stage(Some(&stale), None), Stage::Hold { partition: 4 });
        // Resume from the live partition (a restarted operator).
        assert_eq!(
            stage(Some(&view(4, 2)), None),
            Stage::Drive { partition: 2 }
        );
        // partition 0 but the controller is still converging: still driving.
        assert_eq!(
            stage(Some(&view(4, 0)), None),
            Stage::Drive { partition: 0 }
        );
        // The tail (finalize pending) keeps driving at a settled partition 0.
        let mut done = view(4, 0);
        done.current_revision = done.update_revision.clone();
        let mut up = blank_upgrade(UpgradePhase::FinalizePending);
        assert_eq!(stage(Some(&done), Some(&up)), Stage::Drive { partition: 0 });
        up.phase = UpgradePhase::Complete;
        assert_eq!(stage(Some(&done), Some(&up)), Stage::Steady);
    }

    #[test]
    fn stale_status_never_reads_as_a_finished_roll() {
        // Right after the atomic apply the stored generation is ahead of
        // `observedGeneration` and the revisions still describe the OLD spec
        // (equal). Treating that as steady would reset partition to 0.
        let mut live = StatefulSet::default();
        live.metadata.generation = Some(7);
        live.status = Some(k8s_openapi::api::apps::v1::StatefulSetStatus {
            observed_generation: Some(6),
            current_revision: Some("a".into()),
            update_revision: Some("a".into()),
            ..Default::default()
        });
        live.spec = Some(k8s_openapi::api::apps::v1::StatefulSetSpec {
            replicas: Some(3),
            ..Default::default()
        });
        let v = StsView::of(&live, &StatefulSet::default());
        assert!(!v.status_current);
        assert_eq!(stage(Some(&v), None), Stage::Hold { partition: 0 });
    }

    // --- platform_of -----------------------------------------------------

    fn fact(rev: &str, ready: bool) -> PodFact {
        PodFact {
            revision: Some(rev.into()),
            ready,
            terminating: false,
        }
    }

    #[test]
    fn platform_follows_revision_readiness_and_partition() {
        let upd = Some("rev-2");
        assert_eq!(platform_of(Some(&fact("rev-2", true)), 3, 3, upd), New);
        assert_eq!(
            platform_of(Some(&fact("rev-2", false)), 3, 3, upd),
            Restarting
        );
        // protected by the partition: still old, even if Ready
        assert_eq!(platform_of(Some(&fact("rev-1", true)), 2, 3, upd), Old);
        // admitted by the partition but not yet replaced: in flight
        assert_eq!(
            platform_of(Some(&fact("rev-1", true)), 3, 3, upd),
            Restarting
        );
        // gone: restarting if admitted, an old pod coming back otherwise
        assert_eq!(platform_of(None, 3, 3, upd), Restarting);
        assert_eq!(platform_of(None, 2, 3, upd), Old);
        let mut t = fact("rev-2", true);
        t.terminating = true;
        assert_eq!(platform_of(Some(&t), 3, 3, upd), Restarting);
        // unknown update revision: nothing is ever New
        assert_eq!(
            platform_of(Some(&fact("rev-2", true)), 3, 3, None),
            Restarting
        );
    }

    // --- template_changed -----------------------------------------------

    fn sts_with(hash: Option<&str>, image: Option<&str>) -> StatefulSet {
        let mut s = StatefulSet::default();
        if let Some(h) = hash {
            s.metadata.annotations = Some(BTreeMap::from([(
                TEMPLATE_HASH_ANNOTATION.to_string(),
                h.to_string(),
            )]));
        }
        if let Some(i) = image {
            s.spec = Some(k8s_openapi::api::apps::v1::StatefulSetSpec {
                template: k8s_openapi::api::core::v1::PodTemplateSpec {
                    spec: Some(k8s_openapi::api::core::v1::PodSpec {
                        containers: vec![k8s_openapi::api::core::v1::Container {
                            image: Some(i.to_string()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            });
        }
        s
    }

    #[test]
    fn template_change_uses_the_fingerprint_and_adopts_legacy_sets() {
        let want = sts_with(Some("bbb"), Some("img:2"));
        assert!(template_changed(&sts_with(Some("aaa"), None), &want));
        assert!(!template_changed(&sts_with(Some("bbb"), None), &want));
        // legacy (no annotation): compared by image
        assert!(template_changed(&sts_with(None, Some("img:1")), &want));
        assert!(!template_changed(
            &sts_with(None, Some("img:2")),
            &sts_with(None, Some("img:2"))
        ));
        // legacy and nothing to compare: adopted, not guessed
        assert!(!template_changed(&sts_with(None, None), &want));
    }

    // --- PDB refusal -----------------------------------------------------

    #[test]
    fn pdb_zero_refuses_to_start_only() {
        // 1 control node: maxUnavailable 0
        let msg = pdb_start_gate(3, 3, 1).expect("refuses");
        assert!(msg.contains("maxUnavailable 0"), "{msg}");
        // 2 nodes: rf 2 -> 0
        assert!(pdb_start_gate(2, 2, 2).is_some());
        // 3x3: fine
        assert_eq!(pdb_start_gate(3, 3, 3), None);
        // once the partition has moved the roll has started: never re-judged
        assert_eq!(pdb_start_gate(2, 3, 1), None);
    }

    // --- drive -----------------------------------------------------------

    #[test]
    fn open_gate_lowers_the_partition_by_exactly_one() {
        // c-3 (data) is next; nothing started: partition 4 -> 3.
        let o = obs([Old, Old, Old, Old], Some("c-0"));
        let out = drive(&input(4, 100, None), &o);
        assert_eq!(out.partition, 3);
        assert_eq!(out.effect, None);
        assert_eq!(out.phase, UpgradePhase::InProgress);
        // it was lowered by one, never straight to zero
        assert!(
            out.conds
                .iter()
                .any(|(t, m)| *t == CONDITION_UPGRADE_IN_PROGRESS && m.is_some())
        );
    }

    #[test]
    fn partition_waits_for_the_node_in_flight_then_lowers_once_per_ok() {
        // c-3 restarting: hold at 3.
        let o = obs([Old, Old, Old, Restarting], Some("c-0"));
        assert_eq!(drive(&input(3, 100, None), &o).partition, 3);
        // c-3 new but its roll-health is not ok yet: hold.
        let mut o = obs([Old, Old, Old, New], Some("c-0"));
        o.nodes[3].health = Health::NotOk(vec![Reason {
            kind: "local_group_behind".into(),
            node: None,
            tablet: None,
        }]);
        assert_eq!(drive(&input(3, 100, None), &o).partition, 3);
        // c-3 done and everything ok: lower to 2.
        let o = obs([Old, Old, Old, New], Some("c-0"));
        assert_eq!(drive(&input(3, 100, None), &o).partition, 2);
    }

    #[test]
    fn blocked_on_unhealthy_member_not_active_and_unobservable() {
        // roll-health not ok anywhere
        let mut o = obs([Old, Old, Old, New], Some("c-0"));
        o.nodes[0].health = Health::NotOk(vec![Reason {
            kind: "tablet_under_replicated".into(),
            node: None,
            tablet: Some(7),
        }]);
        let out = drive(&input(3, 100, None), &o);
        assert_eq!(out.partition, 3, "never lowered while a verdict is not ok");
        assert_eq!(out.phase, UpgradePhase::Blocked);
        let msg = blocked_msg(&out);
        assert!(msg.contains("tablet_under_replicated"), "{msg}");
        // a Down member
        let mut o = obs([Old, Old, Old, New], Some("c-0"));
        o.nodes[1].status = Some("Down".into());
        let out = drive(&input(3, 100, None), &o);
        assert_eq!(out.partition, 3);
        assert!(blocked_msg(&out).contains("Down"));
        // an unreachable node fails closed
        let mut o = obs([Old, Old, Old, New], Some("c-0"));
        o.nodes[1].health = Health::Unreachable;
        let out = drive(&input(3, 100, None), &o);
        assert_eq!(out.partition, 3);
        assert_eq!(out.phase, UpgradePhase::Blocked);
    }

    fn blocked_msg(out: &Outcome) -> String {
        out.conds
            .iter()
            .find(|(t, m)| *t == CONDITION_UPGRADE_BLOCKED && m.is_some())
            .and_then(|(_, m)| m.clone())
            .expect("an UpgradeBlocked message")
    }

    #[test]
    fn the_control_leader_gets_a_transfer_before_its_ordinal_is_admitted() {
        // c-3 done; next ordinal is 2 and c-2 leads: transfer first, hold.
        let o = obs([Old, Old, Old, New], Some("c-2"));
        let out = drive(&input(3, 100, None), &o);
        assert_eq!(
            out.partition, 3,
            "the partition must not move before the transfer"
        );
        let Some(Effect::Transfer { leader_ordinal, to }) = out.effect else {
            panic!("expected a transfer, got {:?}", out.effect);
        };
        assert_eq!(leader_ordinal, 2);
        assert_ne!(to, "c-2");
        assert!(to.starts_with("c-"));
        // Once leadership is elsewhere the very next observation lowers it.
        let o = obs([Old, Old, Old, New], Some("c-0"));
        assert_eq!(drive(&input(3, 101, None), &o).partition, 2);
    }

    #[test]
    fn a_transfer_prefers_a_node_already_on_the_new_binary() {
        // c-1 already New: leader c-2 hands to c-1, not c-0.
        let mut o = obs([Old, New, Old, New], Some("c-2"));
        o.nodes[1].reported_new = true;
        let out = drive(&input(3, 100, None), &o);
        assert_eq!(
            out.effect,
            Some(Effect::Transfer {
                leader_ordinal: 2,
                to: "c-1".into()
            })
        );
    }

    #[test]
    fn finish_manual_offers_finalize_and_auto_finalizes_only_with_can_finalize() {
        // every pod New, era on, can_finalize false: blocked, never finalizes.
        let mut o = obs([New, New, New, New], Some("c-0"));
        o.can_finalize = false;
        let mut i = input(0, 100, None);
        i.finalize = FinalizePolicy::Auto;
        let out = drive(&i, &o);
        assert_eq!(out.effect, None);
        assert_eq!(out.phase, UpgradePhase::Blocked);
        // can_finalize true, Manual: offered, partition 0, no effect.
        o.can_finalize = true;
        let out = drive(&input(0, 100, None), &o);
        assert_eq!(out.phase, UpgradePhase::FinalizePending);
        assert_eq!(out.effect, None);
        assert!(
            out.conds
                .iter()
                .any(|(t, m)| *t == CONDITION_UPGRADE_FINALIZE_PENDING && m.is_some())
        );
        // can_finalize true, Auto, soak 0: finalize 1 -> 2 on the leader.
        let out = drive(&i, &o);
        assert_eq!(
            out.effect,
            Some(Effect::Finalize {
                leader_ordinal: 0,
                to: 2,
                expected: 1
            })
        );
        // a finalize blocker never finalizes
        o.finalize_blockers = vec![animus_roll::Blocker {
            node: "c-1".into(),
            reason: "member is Down".into(),
        }];
        assert_eq!(drive(&i, &o).effect, None);
    }

    #[test]
    fn auto_finalize_soaks_on_the_operators_own_clock() {
        let mut o = obs([New, New, New, New], Some("c-0"));
        o.can_finalize = true;
        let mut i = input(0, 1_000, None);
        i.finalize = FinalizePolicy::Auto;
        i.soak = Duration::from_secs(60);
        let first = drive(&i, &o);
        assert_eq!(first.effect, None, "soak not elapsed");
        assert_eq!(
            first.settled_since,
            Some(1_000),
            "the soak clock starts now"
        );
        // 59 s later: still soaking; 60 s later: finalize.
        let mut st = blank_upgrade(UpgradePhase::InProgress);
        st.settled_since = Some(1_000);
        let mut i2 = input(0, 1_059, Some(&st));
        i2.finalize = FinalizePolicy::Auto;
        i2.soak = Duration::from_secs(60);
        assert_eq!(drive(&i2, &o).effect, None);
        let mut i3 = input(0, 1_060, Some(&st));
        i3.finalize = FinalizePolicy::Auto;
        i3.soak = Duration::from_secs(60);
        assert!(matches!(
            drive(&i3, &o).effect,
            Some(Effect::Finalize { .. })
        ));
        // a regression of health resets the soak clock
        o.nodes[0].health = Health::Unreachable;
        assert_eq!(drive(&i2, &o).settled_since, None);
    }

    #[test]
    fn a_stalled_node_is_surfaced_not_acted_on() {
        let o = obs([Old, Old, Old, Restarting], Some("c-0"));
        let mut st = blank_upgrade(UpgradePhase::InProgress);
        st.in_flight_node = Some("c-3".into());
        st.in_flight_since = Some(0);
        let out = drive(&input(3, 899, Some(&st)), &o);
        assert_eq!(
            out.phase,
            UpgradePhase::InProgress,
            "within the stall budget"
        );
        let out = drive(&input(3, 900, Some(&st)), &o);
        assert_eq!(out.phase, UpgradePhase::Blocked);
        assert_eq!(out.partition, 3, "a stall never moves the partition");
        assert!(blocked_msg(&out).contains("c-3"));
        // the clock is per node: a different node in flight restarts it
        let mut st2 = blank_upgrade(UpgradePhase::InProgress);
        st2.in_flight_node = Some("c-2".into());
        st2.in_flight_since = Some(0);
        let out = drive(&input(3, 5_000, Some(&st2)), &o);
        assert_eq!(out.in_flight, Some(("c-3".to_string(), 5_000)));
    }

    #[test]
    fn complete_without_a_version_change_ends_the_roll() {
        // config-only roll: goal == active, every pod New: Complete.
        let mut o = obs([New, New, New, New], Some("c-0"));
        o.goal = 1;
        let out = drive(&input(0, 100, None), &o);
        assert_eq!(out.phase, UpgradePhase::Complete);
        assert_eq!(out.partition, 0);
    }

    #[test]
    fn phase1_first_roll_awaits_the_era() {
        let mut o = obs([New, New, New, New], Some("c-0"));
        o.era_active = false;
        let out = drive(&input(0, 100, None), &o);
        assert_eq!(out.phase, UpgradePhase::InProgress);
        assert_eq!(out.effect, None);
    }

    // --- hold_edits / revert --------------------------------------------

    fn cluster(nodes: i32, control: Option<i32>, image: &str) -> AnimusCluster {
        let mut c = crate::desired::test_support::test_cluster("c", "ns", nodes, control);
        c.spec.image = Some(image.to_string());
        c
    }

    fn rolling(from: &str, to: &str, on_new: i32) -> UpgradeStatus {
        let mut u = blank_upgrade(UpgradePhase::InProgress);
        u.from_image = Some(from.into());
        u.to_image = Some(to.into());
        u.on_new = Some(on_new);
        u
    }

    #[test]
    fn revert_is_refused_only_after_a_pod_reported_the_new_range() {
        let u = rolling("img:1", "img:2", 1);
        assert_eq!(image_revert_target(Some(&u), "img:1"), Some("img:2"));
        // before the first pod is touched a revert is free
        let u0 = rolling("img:1", "img:2", 0);
        assert_eq!(image_revert_target(Some(&u0), "img:1"), None);
        // fix forward to a third image is fine
        assert_eq!(image_revert_target(Some(&u), "img:3"), None);
        // a finished roll has nothing to protect
        let mut done = rolling("img:1", "img:2", 4);
        done.phase = UpgradePhase::Complete;
        assert_eq!(image_revert_target(Some(&done), "img:1"), None);

        let mut c = cluster(3, Some(3), "img:1");
        c.status = Some(AnimusClusterStatus {
            upgrade: Some(u),
            ..Default::default()
        });
        let (pinned, notes) = hold_edits(&c, Some(&view(3, 1)), Some(3));
        assert_eq!(pinned.spec.image.as_deref(), Some("img:2"));
        assert_eq!(notes.len(), 1, "{notes:?}");
    }

    #[test]
    fn topology_edits_are_held_while_a_roll_is_in_flight() {
        let c = cluster(5, Some(3), "img:2");
        let (pinned, notes) = hold_edits(&c, Some(&view(3, 2)), Some(3));
        assert_eq!(pinned.spec.nodes, 3, "scale held to the running replicas");
        assert_eq!(notes.len(), 1);
        let c = cluster(3, Some(2), "img:2");
        let (pinned, _) = hold_edits(&c, Some(&view(3, 2)), Some(3));
        assert_eq!(pinned.spec.control_nodes, Some(3));
        // no roll in flight: nothing is held
        let mut calm = view(3, 0);
        calm.current_revision = calm.update_revision.clone();
        let c = cluster(5, Some(3), "img:2");
        let (pinned, notes) = hold_edits(&c, Some(&calm), Some(3));
        assert_eq!(pinned.spec.nodes, 5);
        assert!(notes.is_empty());
    }

    #[test]
    fn begin_roll_keeps_the_pre_roll_image_across_a_fix_forward() {
        let mut st = AnimusClusterStatus::default();
        begin_roll(&mut st, Some("img:1".into()), Some("img:2".into()), 3, None);
        let u = st.upgrade.clone().unwrap();
        assert_eq!(u.from_image.as_deref(), Some("img:1"));
        assert_eq!(u.to_image.as_deref(), Some("img:2"));
        // a fixed image mid-roll re-targets, remembering where we came from
        begin_roll(&mut st, Some("img:2".into()), Some("img:3".into()), 3, None);
        let u = st.upgrade.clone().unwrap();
        assert_eq!(u.from_image.as_deref(), Some("img:1"));
        assert_eq!(u.to_image.as_deref(), Some("img:3"));
        assert_eq!(u.on_new, Some(0));
    }

    #[test]
    fn begin_roll_with_a_pdb_refusal_is_blocked_not_in_progress() {
        let mut st = AnimusClusterStatus::default();
        begin_roll(
            &mut st,
            Some("a".into()),
            Some("b".into()),
            1,
            Some("PDB zero"),
        );
        assert_eq!(st.upgrade.unwrap().phase, UpgradePhase::Blocked);
        assert!(
            st.conditions
                .iter()
                .any(|c| c.type_ == CONDITION_UPGRADE_BLOCKED
                    && c.message.as_deref() == Some("PDB zero"))
        );
    }

    #[test]
    fn ordinal_of_parses_node_ids() {
        assert_eq!(ordinal_of("demo", "demo-12"), Some(12));
        assert_eq!(ordinal_of("demo", "other-1"), None);
    }
}
