//! The reconcile loop: a thin imperative shell over [`crate::desired`]'s
//! pure builders. All the interesting *shape* decisions live there and are
//! unit-tested without a cluster; this module's own job is just "build the
//! desired children, apply them, compute status, requeue" plus the two
//! stateful edges a pure function cannot express: the scale-down drain
//! sequence (talks to a real pod's admin port) and refusing an immutable
//! field change (reads the live object's own prior-applied state).
//!
//! **No finalizer in v1** — deletion relies entirely on Kubernetes garbage
//! collection following the `controller: true` owner references every
//! child carries (`crate::desired::owner_reference`). This is a deliberate
//! scope cut: nothing here needs pre-delete cleanup (an `AnimusCluster`
//! owns no external resource outside the Kubernetes API — no backup store,
//! no DNS record, nothing an orphaned finalizer could leak) and it keeps a
//! stuck-finalizer failure mode out of a v1 operator entirely.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use kube::{Api, Client, ResourceExt};
use serde_json::json;
use tracing::{error, info, warn};

use crate::admin_client::{AdminAccessMode, AdminOps, RealAdminClient};
use crate::cluster_api::{ClusterApi, RealClusterApi};
use crate::crd::{
    AnimusCluster, AnimusClusterStatus, CONDITION_CONTROL_NODES_GROWING,
    CONDITION_CONTROL_NODES_SHRINK_REJECTED, CONDITION_DRAIN_FAILED,
    CONDITION_ENCRYPTION_KEY_SECRET_INVALID, CONDITION_S3_SPEC_INVALID,
    CONDITION_SCALE_BELOW_CONTROL_NODES_REFUSED, CONDITION_STORE_SPEC_INVALID,
    CONDITION_TLS_SPEC_INVALID, ClusterCondition, ClusterPhase, ConditionStatus,
};
use crate::desired;

/// The field manager name every server-side-apply call uses
/// ([`crate::cluster_api::RealClusterApi`]'s own `PatchParams::apply`).
pub const FIELD_MANAGER: &str = "animus-operator";
/// Requeue interval after a clean reconcile.
const REQUEUE_OK: Duration = Duration::from_secs(30);
/// Requeue interval after a reconcile error (kube's `Controller` also
/// backs this off internally, but a fixed floor keeps a persistently
/// failing cluster from hot-looping the operator process).
const REQUEUE_ERR: Duration = Duration::from_secs(15);

/// Reconcile error type — every fallible step folds into this so the
/// `Controller`'s error hook can log it and set `Degraded` without a panic.
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("kube API error: {0}")]
    Kube(#[from] kube::Error),
    #[error("cluster object has no name")]
    MissingName,
    #[error("cluster object has no namespace")]
    MissingNamespace,
}

/// Shared context every reconcile call gets: the Kubernetes API seam and
/// the admin-port seam used for the scale-down drain sequence. Generic over
/// both so tests can substitute `crate::fakes::{FakeClusterApi,
/// FakeAdminClient}` for the real `kube`/HTTP implementors — see
/// `crate::cluster_api`/`crate::admin_client`'s own docs for why this is
/// the seam boundary.
pub struct Context<C: ClusterApi, A: AdminOps> {
    pub cluster_api: C,
    pub admin: A,
}

/// Apply every desired child for `cluster`, in a fixed order (`ConfigMap`
/// before `StatefulSet`, so a rolling pod never briefly reads a
/// `StatefulSet`-implied config that its `ConfigMap` doesn't have yet).
/// Every required child is applied unconditionally on every call — there
/// is no diff against the previously-applied object, so a reconcile of an
/// otherwise unchanged cluster still re-applies every one of them (an
/// idempotent re-apply, not a no-op; `crate::controller::tests` pins
/// this). `Certificate` (ADR 0064 commit 3) is the one *optional* child,
/// applied only for `spec.tls.certManager`; `PodDisruptionBudget` (S-07c)
/// is required like the rest — see `desired::poddisruptionbudget`'s own
/// module doc for why it carries no such toggle.
async fn apply_children<C: ClusterApi>(
    cluster_api: &C,
    cluster: &AnimusCluster,
    ns: &str,
    pdb_control_nodes: i32,
) -> Result<StatefulSet, ReconcileError> {
    let spec = &cluster.spec;

    let cm = desired::configmap::build(cluster, spec);
    cluster_api.apply_configmap(ns, &cm).await?;

    // ADR 0064 commit 3: a sixth child, applied only when `spec.tls.
    // certManager` is set (`build` returns `None` for the `secretName`
    // shape and for no TLS at all) — before the `StatefulSet` so the
    // `Secret` it names has a chance to exist by the time a pod starts.
    if let Some(cert) = desired::certificate::build(cluster, spec) {
        cluster_api.apply_certificate(ns, &cert).await?;
    }

    let internal_svc = desired::services::build_internal(cluster, spec);
    cluster_api.apply_service(ns, &internal_svc).await?;

    let client_svc = desired::services::build_client(cluster, spec);
    cluster_api.apply_service(ns, &client_svc).await?;

    let netpol = desired::networkpolicy::build(cluster, spec);
    cluster_api.apply_networkpolicy(ns, &netpol).await?;

    // S-07c: the quorum-derived PodDisruptionBudget, after the
    // NetworkPolicy and before the StatefulSet — a PDB that exists before
    // any pod does is harmless, and this keeps the ordering "things pods
    // depend on first, the StatefulSet itself last" that every other
    // child already follows. Applied unconditionally, from the *desired*
    // spec (`spec.nodes`/`spec.controlNodes`), never the StatefulSet's
    // live replica count — see `desired::poddisruptionbudget`'s own
    // module doc for why.
    //
    // **S-07d**: `pdb_control_nodes` is `spec.control_nodes_or_default()`
    // itself whenever no growth is in flight, but the *achieved* (live-
    // confirmed) voter count while a growth is still catching up — using
    // the full `spec.controlNodes` target here would grant a larger
    // disruption budget than the control group can actually survive right
    // now, since the newly-promoted ordinals are not yet real voters.
    // Never affects the `ConfigMap`/`StatefulSet` below, which must always
    // reflect the real `spec.controlNodes` target so the promoted ordinals
    // restart into role `Both` in the first place.
    let pdb_spec: std::borrow::Cow<'_, crate::crd::AnimusClusterSpec> =
        if pdb_control_nodes == spec.control_nodes_or_default() {
            std::borrow::Cow::Borrowed(spec)
        } else {
            let mut overridden = spec.clone();
            overridden.control_nodes = Some(pdb_control_nodes);
            std::borrow::Cow::Owned(overridden)
        };
    let pdb = desired::poddisruptionbudget::build(cluster, &pdb_spec);
    cluster_api.apply_poddisruptionbudget(ns, &pdb).await?;

    let sts = desired::statefulset::build(cluster, spec);
    let applied = cluster_api.apply_statefulset(ns, &sts).await?;

    Ok(applied)
}

/// The `spec.controlNodes` value the *previous* reconcile actually applied
/// (resolved against its own default at the time), or `None` on a fresh
/// cluster with no applied `ConfigMap` yet. With no admission webhook in
/// v1, the controller is the only thing that can catch a `controlNodes`
/// edit — so it is caught here, every reconcile, by comparing against the
/// live `StatefulSet`'s replica count is not enough (that only tells us
/// `nodes`, not `controlNodes`); instead this reads a dedicated status
/// annotation-free signal: the `ConfigMap`'s own already-applied config,
/// which is cheap to read back (server-side apply already wrote it) and is
/// the actual source of truth for which ordinals were minted `Both` vs
/// `Data` last time.
///
/// **S-07d**: this used to also compare against the *desired* value and
/// return `None` when unchanged (`control_nodes_changed`, hence the name);
/// it now always returns the prior value on its own, since both the
/// shrink-rejection check and the growth machinery below need it and
/// growth additionally needs to keep observing it every reconcile while a
/// growth is in flight, not just on the one reconcile where the spec edit
/// first lands.
async fn previous_applied_control_nodes<C: ClusterApi>(
    cluster_api: &C,
    ns: &str,
    cluster: &AnimusCluster,
) -> Result<Option<i32>, ReconcileError> {
    let name = cluster.name_any();
    let cm_name = desired::config_map_name(&name);
    let existing = cluster_api.get_configmap(ns, &cm_name).await?;
    let Some(existing) = existing else {
        return Ok(None);
    };
    let Some(data) = &existing.data else {
        return Ok(None);
    };
    let Some(json) = data.get(desired::cluster_config::CONFIG_FILE_NAME) else {
        return Ok(None);
    };
    let Ok(parsed) = serde_json::from_str::<desired::cluster_config::ClusterConfig>(json) else {
        return Ok(None);
    };
    if parsed.nodes.is_empty() {
        return Ok(None);
    }
    let previous_control_nodes = parsed
        .nodes
        .iter()
        .take_while(|n| matches!(n.role, desired::cluster_config::NodeRole::Both))
        .count() as i32;
    Ok(Some(previous_control_nodes))
}

// ---- S-07d: spec.controlNodes growth --------------------------------------
//
// `controlNodes` may now only ever *increase* — a decrease is still
// rejected outright (unchanged from the old "immutable" posture, just
// renamed: `CONDITION_CONTROL_NODES_SHRINK_REJECTED`). Growth is driven
// entirely from **live** `GET /admin/control/members` truth, one voter at a
// time, never from anything held only in this process's memory: a
// controller restart simply re-derives "which ordinal is next" from
// whatever the control group itself currently reports, which is what makes
// this resume-safe across a restart with no persisted growth state of its
// own (the `ControlNodesGrowing` status condition is a resume
// *optimization* — skip the live check once nothing is pending — never the
// source of truth).
//
// The sequence per reconcile (at most one ordinal advanced per call, so a
// single reconcile never blocks for the full multi-ordinal growth):
//  1. Ask an already-established control ordinal (0, falling back through
//     the rest) for its live voter set.
//  2. `next_growth_ordinal`/`achieved_control_nodes` (pure, unit-tested)
//     turn that into "how many of `0..target` are already confirmed".
//  3. If the next ordinal's own `GET /admin/config` doesn't yet report
//     `role: "combined"` (`animusd`'s own literal for a node running both
//     roles — `AdminInfo.role`, never `"both"`, see `ordinal_reports_role_both`'s
//     own doc for the pinned regression this once lacked), the promoted pod
//     hasn't restarted yet (the config-hash annotation drives that, see
//     `desired::statefulset`) — wait.
//  4. Otherwise, resolve that ordinal's live `status.podIP` (via the
//     Kubernetes API, not DNS — see `resolve_control_dial_addr`'s own doc
//     for why, including the pre-existing `animusd` admin-API gap this
//     works around) and `POST /admin/control/member/add` against each
//     already-confirmed voter ordinal in turn until one accepts (mirroring
//     "retry on the leader" — see `add_control_voter`'s own doc for why
//     this, not a parsed error-message address hint, is how that's done
//     here), then poll `GET /admin/control/members` (bounded) until the new
//     ordinal shows up before this reconcile returns.
//
// `advance_control_growth` also returns the control-voter count
// `apply_children`'s `PodDisruptionBudget` step should use this reconcile
// — see that function's own call site in `reconcile` for why this must be
// the *achieved*, not the *desired*, count while growth is still catching
// up.

/// The ordinal of the next control voter that still needs `POST
/// /admin/control/member/add`, given `target` (`spec.controlNodes`) and the
/// control group's own live voter-id set (`GET /admin/control/members`'s
/// `"voters"` field) — `None` once every ordinal `0..target` is already a
/// confirmed voter. A pure function of `(cluster_name, target, voters)`
/// precisely so a controller restart resumes from this exact truth, never
/// from anything held only in memory.
fn next_growth_ordinal(
    cluster_name: &str,
    target: i32,
    voters: &std::collections::BTreeSet<String>,
) -> Option<i32> {
    (0..target).find(|&i| !voters.contains(&desired::cluster_config::node_id(cluster_name, i)))
}

/// How many of the leading ordinals `0..target` are already confirmed
/// voters — `target` itself once growth has fully caught up.
fn achieved_control_nodes(
    cluster_name: &str,
    target: i32,
    voters: &std::collections::BTreeSet<String>,
) -> i32 {
    next_growth_ordinal(cluster_name, target, voters).unwrap_or(target)
}

/// Parse a `GET /admin/control/members` response body into its `"voters"`
/// set — `None` when the field is absent/`null` (a `Remote` handle that has
/// never observed a voter set at all, `control_members_view`'s own
/// documented "unknown vs. genuinely empty" distinction) or malformed,
/// which callers treat identically to "could not reach this node" rather
/// than "the group has zero voters".
fn parse_voters(body: &serde_json::Value) -> Option<std::collections::BTreeSet<String>> {
    body["voters"].as_array().map(|vs| {
        vs.iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect()
    })
}

/// `GET {admin_base_url(ordinal)}/admin/control/members`, parsed — `None`
/// on any transport error or an unparseable/unknown response.
async fn fetch_control_members<A: AdminOps>(
    admin: &A,
    name: &str,
    ns: &str,
    ordinal: i32,
    admin_port: i32,
    tls_ca: Option<&[u8]>,
) -> Option<std::collections::BTreeSet<String>> {
    let base = admin_base_url(name, ns, ordinal, admin_port, tls_ca.is_some());
    let body = admin
        .get_json(&format!("{base}/admin/control/members"), tls_ca)
        .await
        .ok()?;
    parse_voters(&body)
}

/// Ask each ordinal `0..target` in turn (stopping at the first that
/// answers) for the control group's own live voter set — `GET
/// /admin/control/members` is served on **any** node, control-voter or
/// not, so even a not-yet-promoted ordinal answers honestly (`admin.rs`'s
/// own doc on `control_members_view`). Bounded by `target`, which is the
/// control-voter count specifically (small in practice, not `spec.nodes`).
async fn discover_control_voters<A: AdminOps>(
    admin: &A,
    name: &str,
    ns: &str,
    target: i32,
    admin_port: i32,
    tls_ca: Option<&[u8]>,
) -> Option<std::collections::BTreeSet<String>> {
    for ordinal in 0..target {
        if let Some(voters) =
            fetch_control_members(admin, name, ns, ordinal, admin_port, tls_ca).await
        {
            return Some(voters);
        }
    }
    None
}

/// Whether ordinal `ordinal`'s own `GET /admin/config` currently reports
/// `role: "combined"` — the promoted pod has actually restarted into
/// combined mode (driven by `desired::statefulset`'s config-hash annotation)
/// and its own admin port is up enough to answer, regardless of
/// control-plane leadership (`config_view`'s own doc: a static
/// self-description, never gated on `is_leader`/`leader_recent`) — unlike
/// `GET /admin/health`, which would never report ready here (a
/// freshly-promoted, not-yet-a-voter control handle has no leader to be
/// recent about, so gating on health would deadlock this exact step). Any
/// transport error also reads `false` — the pod isn't ready to be added yet
/// either way.
///
/// **The literal is `"combined"`, never `"both"`** — `animusd`'s own
/// `AdminInfo.role` (`crates/animusd/src/admin.rs`'s `config_view`, stamped
/// at assembly time in `crates/animusd/src/lib.rs`) only ever takes the
/// values `"control"`/`"data"`/`"combined"`; pinned server-side by
/// `crates/animusd/tests/dashboard_endpoint.rs`'s own
/// `config_view["role"] == "combined"` assertion for a combined-mode node.
/// `"both"` is a *different* JSON shape entirely — the generated
/// `cluster.json`'s own per-node `role` field
/// (`desired::cluster_config::NodeRole::Both`, this crate's own dispatch
/// enum deciding which `animusd` subcommand a pod execs) — the two
/// vocabularies happen to describe the same real-world state (a pod running
/// both roles) but are unrelated fields on unrelated JSON documents; this
/// function reads the *runtime* one. Checking the wrong literal here used to
/// mean this always evaluated `false`, so growth waited forever for a
/// restart signal that could never arrive — closed by pinning the fake's own
/// literal to the same value in `crate::fakes::FakeAdminClient`, see that
/// module's own doc.
async fn ordinal_reports_role_both<A: AdminOps>(
    admin: &A,
    name: &str,
    ns: &str,
    ordinal: i32,
    admin_port: i32,
    tls_ca: Option<&[u8]>,
) -> bool {
    let base = admin_base_url(name, ns, ordinal, admin_port, tls_ca.is_some());
    match admin
        .get_json(&format!("{base}/admin/config"), tls_ca)
        .await
    {
        Ok(v) => v["role"].as_str() == Some("combined"),
        Err(_) => false,
    }
}

/// Resolve ordinal `ordinal`'s **current** internal-Raft dial address to a
/// literal `SocketAddr`, for `POST /admin/control/member/add`'s `addr`
/// field.
///
/// **Works around a real, pre-existing `animusd` admin-API gap**: that
/// field is typed `std::net::SocketAddr` server-side
/// (`admin::AddControlMemberReq`), which can only ever deserialize a
/// literal IP:port — never a DNS name. Every other address surface this
/// operator or `animusd` itself uses for a Kubernetes pod
/// (`RoleAddrs::advertise_host`, `ClientResponse::JoinInfo`, the peer book
/// `ProdEnv::merge_peer`/`ProdEnv::set_peers` populate) is deliberately
/// string/hostname-typed for exactly the reason a pod's IP is not stable
/// across a restart while its per-ordinal DNS name is. This reads the
/// pod's *current* `status.podIP` via the Kubernetes API (never a DNS
/// lookup — more immediately authoritative, and, unlike a raw
/// `tokio::net::lookup_host` call, goes through the already-testable
/// `ClusterApi` seam) purely as a one-time bootstrap value for the
/// leader's very first dial: the promoted node's own startup self-
/// registration (`spawn_common_tail`'s `register_node_addrs`,
/// unconditional on every combined-mode boot, per `animusd::lib`'s own
/// doc) republishes its real, DNS-name-based `advertised_addr` into the
/// replicated `Metadata.node_addrs` moments later, which every node's own
/// `peer_sync_loop` then adopts — so a resolved-IP staleness window here
/// is self-healing within moments of this call, not a permanent address
/// pin. See `crates/animus-operator/CLAUDE.md`'s S-07d section for the
/// full account and the animusd-side fix this should eventually get
/// (accepting a `String` addr the way `ProdEnv::merge_peer` already does).
async fn resolve_control_dial_addr<C: ClusterApi>(
    cluster_api: &C,
    name: &str,
    ns: &str,
    ordinal: i32,
    internal_port: i32,
) -> Result<std::net::SocketAddr, String> {
    let pod = desired::pod_name(name, ordinal);
    let ip = cluster_api
        .get_pod_ip(ns, &pod)
        .await
        .map_err(|e| format!("reading pod {pod}'s IP: {e}"))?
        .ok_or_else(|| format!("pod {pod} has no status.podIP yet"))?;
    format!("{ip}:{internal_port}")
        .parse()
        .map_err(|e| format!("pod {pod}'s podIP {ip:?} did not parse as an address: {e}"))
}

/// Add ordinal `ordinal` (already known to be missing from the live voter
/// set) as a control voter: resolves its dial address, then tries
/// `POST /admin/control/member/add` against each already-confirmed voter
/// ordinal `0..ordinal` in turn until one accepts. `POST /admin/control/
/// member/add` is **local-control-leader-only, not relayed**
/// (`admin::action_add_control_member`'s own doc) — and a `Local` control
/// handle's own `leader_addr_hint` is always `None` (unlike a `Remote`
/// data-only node's), so there is no address to parse out of a "not
/// leader" refusal the way a human operator's own runbook error message
/// might suggest. Trying every already-confirmed voter in turn instead
/// achieves the same "retry on the leader" outcome without needing one:
/// at most one of them can accept (the real leader), and `admin_add_control_
/// member`'s own doc states a retry of the whole call is always
/// safe/idempotent, so trying the others first costs nothing but a
/// harmless 409.
///
/// Returns `Ok(true)` once the group's own `GET /admin/control/members`
/// confirms `ordinal` as a voter (bounded poll), `Ok(false)` if the add
/// itself succeeded but confirmation didn't land within that bound (not a
/// failure — the next reconcile re-checks live truth and either finds it
/// already there or, since the add is idempotent, retries harmlessly), and
/// `Err` only when no already-confirmed voter accepted the add at all (or
/// the dial address couldn't be resolved).
async fn add_control_voter<C: ClusterApi, A: AdminOps>(
    ctx: &Context<C, A>,
    name: &str,
    ns: &str,
    ordinal: i32,
    admin_port: i32,
    internal_port: i32,
    tls_ca: Option<&[u8]>,
) -> Result<bool, String> {
    let node_id = desired::cluster_config::node_id(name, ordinal);
    let addr =
        resolve_control_dial_addr(&ctx.cluster_api, name, ns, ordinal, internal_port).await?;

    let mut last_err = "no already-confirmed control voter ordinal to ask".to_string();
    let mut added = false;
    for voter_ordinal in 0..ordinal {
        let base = admin_base_url(name, ns, voter_ordinal, admin_port, tls_ca.is_some());
        match ctx
            .admin
            .post_json(
                &format!("{base}/admin/control/member/add"),
                &json!({"node": node_id, "addr": addr.to_string()}),
                tls_ca,
            )
            .await
        {
            Ok(_) => {
                added = true;
                break;
            }
            Err(e) => last_err = format!("ordinal {voter_ordinal}: {e}"),
        }
    }
    if !added {
        return Err(format!("adding {node_id} as a control voter: {last_err}"));
    }

    const CONFIRM_POLLS: u32 = 15;
    const CONFIRM_INTERVAL: Duration = Duration::from_secs(2);
    for attempt in 0..CONFIRM_POLLS {
        if let Some(voters) =
            fetch_control_members(&ctx.admin, name, ns, 0, admin_port, tls_ca).await
            && voters.contains(&node_id)
        {
            return Ok(true);
        }
        if attempt + 1 == CONFIRM_POLLS {
            return Ok(false);
        }
        // ADR 0003 / ADR 0061 Decision 4 (rung B5): same real-wall-clock
        // allowance `drain_and_remove_node`'s own poll loop already carries
        // — this reconcile loop polls a real pod's admin port over a real
        // network, outside the Env seam.
        #[allow(
            clippy::disallowed_methods,
            reason = "animus-operator polls a real pod's admin port outside the Env seam, not system logic (ADR 0003); see ADR 0061 Decision 4"
        )]
        tokio::time::sleep(CONFIRM_INTERVAL).await;
    }
    Ok(false)
}

/// Advance `spec.controlNodes` growth by at most one ordinal this
/// reconcile — see this module's own "S-07d: spec.controlNodes growth"
/// section doc above for the full sequence. Returns the control-voter
/// count `apply_children`'s `PodDisruptionBudget` step should use this
/// reconcile: `target` once growth is confirmed complete, the live-
/// confirmed `achieved` count while it's still catching up, or
/// `previously_applied` (the last known-safe count) when live truth
/// couldn't be observed at all this reconcile.
#[allow(clippy::too_many_arguments)] // every argument is a distinct, already-resolved input; no natural grouping
async fn advance_control_growth<C: ClusterApi, A: AdminOps>(
    ctx: &Context<C, A>,
    name: &str,
    ns: &str,
    target: i32,
    previously_applied: i32,
    admin_port: i32,
    internal_port: i32,
    tls_ca: Option<&[u8]>,
    status: &mut AnimusClusterStatus,
) -> i32 {
    // Record that a growth is in progress *before* the first admin call —
    // a stall (this reconcile's own discovery/add call hanging, timing out,
    // or failing every reconcile in a row) must be visible from `kubectl get
    // animuscluster -o yaml` even if nothing below ever narrows the message
    // further. Every branch below either overwrites this with a more
    // specific message or clears the condition outright once growth
    // completes — this call never survives as the final message on a
    // reconcile that got further than this line.
    set_condition(
        status,
        CONDITION_CONTROL_NODES_GROWING,
        format!(
            "growing spec.controlNodes: {previously_applied}/{target} voters confirmed as of \
             the last successful check; discovering live control-voter truth"
        ),
    );
    let Some(voters) =
        discover_control_voters(&ctx.admin, name, ns, target, admin_port, tls_ca).await
    else {
        // Can't observe live truth this reconcile (every ordinal 0..target
        // refused/timed out — still bootstrapping, or a transient blip):
        // surface that a discovery attempt was made and failed, rather than
        // leaving only the generic "discovering" message above, and fall
        // back to the last confirmed-safe count for the PDB.
        set_condition(
            status,
            CONDITION_CONTROL_NODES_GROWING,
            format!(
                "growing spec.controlNodes: {previously_applied}/{target} voters confirmed as \
                 of the last successful check; could not reach any control ordinal in 0..{target} \
                 to discover live voter truth this reconcile"
            ),
        );
        return previously_applied;
    };
    let achieved = achieved_control_nodes(name, target, &voters);
    if achieved >= target {
        status
            .conditions
            .retain(|c| c.type_ != CONDITION_CONTROL_NODES_GROWING);
        return target;
    }
    if !ordinal_reports_role_both(&ctx.admin, name, ns, achieved, admin_port, tls_ca).await {
        set_condition(
            status,
            CONDITION_CONTROL_NODES_GROWING,
            format!(
                "growing spec.controlNodes: {achieved}/{target} voters confirmed; \
                 waiting for pod ordinal {achieved} to restart into role \"combined\""
            ),
        );
        return achieved;
    }
    match add_control_voter(ctx, name, ns, achieved, admin_port, internal_port, tls_ca).await {
        Ok(true) => {
            let now_achieved = achieved + 1;
            if now_achieved >= target {
                status
                    .conditions
                    .retain(|c| c.type_ != CONDITION_CONTROL_NODES_GROWING);
            } else {
                set_condition(
                    status,
                    CONDITION_CONTROL_NODES_GROWING,
                    format!("growing spec.controlNodes: {now_achieved}/{target} voters confirmed"),
                );
            }
            now_achieved
        }
        Ok(false) => {
            set_condition(
                status,
                CONDITION_CONTROL_NODES_GROWING,
                format!(
                    "growing spec.controlNodes: added ordinal {achieved}, waiting for it to be \
                     confirmed a voter ({achieved}/{target} confirmed so far)"
                ),
            );
            achieved
        }
        Err(e) => {
            warn!(
                cluster = %name,
                ordinal = achieved,
                error = %e,
                "control voter growth step failed; will retry next reconcile"
            );
            set_condition(
                status,
                CONDITION_CONTROL_NODES_GROWING,
                format!(
                    "growing spec.controlNodes: {achieved}/{target} voters confirmed; last \
                     attempt to add ordinal {achieved} failed: {e}"
                ),
            );
            achieved
        }
    }
}

/// Read `spec.tls`'s resolved cluster-CA `Secret` (if `spec.tls` is set),
/// for `AdminOps::get_json`/`post_json`'s `ca_pem` — shared by both the
/// scale-down drain sequence and S-07d's control-voter growth step, which
/// each dial a pod's admin port the identical way.
async fn resolve_tls_ca<C: ClusterApi>(
    cluster_api: &C,
    cluster: &AnimusCluster,
    ns: &str,
    name: &str,
) -> Result<Option<Vec<u8>>, ReconcileError> {
    match &cluster.spec.tls {
        Some(tls) => {
            let secret_name = tls.secret_name_or_default(name);
            Ok(cluster_api
                .get_secret(ns, &secret_name)
                .await?
                .and_then(|s| s.data)
                .and_then(|d| d.get("ca.crt").cloned())
                .map(|b| b.0))
        }
        None => Ok(None),
    }
}

/// Live-checks `spec.encryptionKeySecretName` (ADR 0069, S-03 PR 3) against
/// the API server: the named `Secret` must exist in `ns` and carry
/// [`desired::cluster_config::ENCRYPTION_KEY_SECRET_DATA_KEY`] as one of its
/// data keys. Returns `Ok(None)` when the reference is usable, `Ok(Some(
/// message))` naming exactly what's wrong otherwise (never `Err` for a
/// missing/malformed Secret — only a genuine API-server failure propagates
/// as [`ReconcileError`]).
///
/// Unlike [`crate::crd::TlsSpec::validate`]/[`crate::crd::S3StoreSpec::
/// validate`] (pure functions, no cluster access — `crd.rs`'s own "no
/// admission webhook in v1" posture), this genuinely needs a live read: a
/// Secret *reference*'s only checkable shape is its own presence (and, once
/// present, its own data keys) in the cluster, neither of which the spec
/// alone can ever say.
async fn validate_encryption_key_secret<C: ClusterApi>(
    cluster_api: &C,
    ns: &str,
    secret_name: &str,
) -> Result<Option<String>, ReconcileError> {
    let data_key = desired::cluster_config::ENCRYPTION_KEY_SECRET_DATA_KEY;
    let secret = cluster_api.get_secret(ns, secret_name).await?;
    Ok(match secret {
        None => Some(format!(
            "spec.encryptionKeySecretName names \"{secret_name}\", which does not exist in \
             namespace \"{ns}\" — create it with a \"{data_key}\" data key holding the raw \
             64-hex-character key (e.g. `openssl rand -hex 32 | kubectl create secret \
             generic {secret_name} --from-file={data_key}=/dev/stdin`), or point at an \
             existing one"
        )),
        Some(s) => {
            let has_key = s.data.as_ref().is_some_and(|d| d.contains_key(data_key));
            if has_key {
                None
            } else {
                Some(format!(
                    "Secret \"{secret_name}\" in namespace \"{ns}\" has no \"{data_key}\" data \
                     key — the encryption key must be stored under that exact key"
                ))
            }
        }
    })
}

/// The admin base URL for pod ordinal `ordinal` of cluster `name` in
/// namespace `ns` — the headless internal `Service`'s own per-pod DNS name.
/// `tls`: whether the admin port speaks TLS (ADR 0064 commit 3, server-only
/// — `animusd` serves `admin` that way whenever `spec.tls` is set), which
/// selects the URL scheme; the caller must pass the matching CA bytes to
/// `AdminOps::get_json`/`post_json` in that case (see [`drain_and_remove_node`]).
///
/// `pub(crate)`: `crate::admin_client`'s `ProxyAdminClient` parses this
/// exact URL shape back apart (`parse_admin_url`), and its own unit tests
/// build one through this function rather than hand-duplicating the format
/// string.
pub(crate) fn admin_base_url(
    name: &str,
    ns: &str,
    ordinal: i32,
    admin_port: i32,
    tls: bool,
) -> String {
    let scheme = if tls { "https" } else { "http" };
    format!(
        "{scheme}://{}:{admin_port}",
        desired::pod_fqdn(name, ns, ordinal)
    )
}

/// Drain and remove one pod ordinal before it is scaled away, via the
/// sequence `crate::CLAUDE.md`/the delivery brief document: `POST
/// /admin/drain {node}`, poll `GET /admin/member/drain-status?node=` to
/// completion, then `POST /admin/member/remove {node}`. `tls_ca` (ADR 0064
/// commit 3): `Some(pem)` dials the admin port over TLS trusting `pem` as
/// the cluster CA; `None` plain TCP — see `reconcile`'s own call site for
/// where this is read out of `spec.tls`'s resolved `Secret`.
async fn drain_and_remove_node<A: AdminOps>(
    admin: &A,
    name: &str,
    ns: &str,
    ordinal: i32,
    admin_port: i32,
    tls_ca: Option<&[u8]>,
) -> Result<(), String> {
    let node_id = desired::cluster_config::node_id(name, ordinal);
    let base = admin_base_url(name, ns, ordinal, admin_port, tls_ca.is_some());

    admin
        .post_json(
            &format!("{base}/admin/drain"),
            &json!({ "node": node_id }),
            tls_ca,
        )
        .await
        .map_err(|e| format!("draining {node_id}: {e}"))?;

    const MAX_POLLS: u32 = 120;
    const POLL_INTERVAL: Duration = Duration::from_secs(5);
    for attempt in 0..MAX_POLLS {
        let status: serde_json::Value = admin
            .get_json(
                &format!("{base}/admin/member/drain-status?node={node_id}"),
                tls_ca,
            )
            .await
            .map_err(|e| format!("polling drain-status for {node_id}: {e}"))?;
        let tablets_remaining = status["tablets_remaining"].as_u64().unwrap_or(u64::MAX);
        let member_status = status["status"].as_str().unwrap_or("");
        if tablets_remaining == 0 && member_status != "Active" {
            break;
        }
        if attempt + 1 == MAX_POLLS {
            return Err(format!(
                "{node_id} did not finish draining after {MAX_POLLS} polls \
                 ({tablets_remaining} tablets remaining, status {member_status:?})"
            ));
        }
        // ADR 0003 / ADR 0061 Decision 4 (rung B5): this reconcile loop polls a
        // real Kubernetes pod's admin port over a real network, outside the
        // Env seam (kube-rs, no SimEnv counterpart) — a real wall-clock wait
        // is the correct tool here, not a determinism hole.
        #[allow(
            clippy::disallowed_methods,
            reason = "animus-operator polls a real pod's admin port outside the Env seam, not system logic (ADR 0003); see ADR 0061 Decision 4"
        )]
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    admin
        .post_json(
            &format!("{base}/admin/member/remove"),
            &json!({ "node": node_id }),
            tls_ca,
        )
        .await
        .map_err(|e| format!("removing {node_id}: {e}"))?;
    Ok(())
}

/// Set (replacing any existing entry of the same `type`) one condition on
/// `status`.
fn set_condition(status: &mut AnimusClusterStatus, type_: &str, message: String) {
    status.conditions.retain(|c| c.type_ != type_);
    status.conditions.push(ClusterCondition {
        type_: type_.to_string(),
        status: ConditionStatus::True,
        reason: Some(type_.to_string()),
        message: Some(message),
        last_transition_time: None,
    });
}

async fn reconcile<C: ClusterApi, A: AdminOps>(
    cluster: Arc<AnimusCluster>,
    ctx: Arc<Context<C, A>>,
) -> Result<Action, ReconcileError> {
    let name = cluster.name_any();
    let ns = cluster
        .namespace()
        .ok_or(ReconcileError::MissingNamespace)?;
    info!(cluster = %name, namespace = %ns, "reconciling AnimusCluster");

    let mut status = cluster.status.clone().unwrap_or_default();
    status.observed_generation = cluster.metadata.generation;

    // Validate `spec.tls` (ADR 0064 commit 3): no admission webhook in v1
    // to reject the write itself, so — same posture as `controlNodes`'
    // immutability check above — this is the one place that can catch a
    // spec setting both or neither of `secretName`/`certManager`. Set a
    // condition and reconcile the rest of the spec with TLS stripped
    // (every other field — image, resources, scale — still deserves to
    // converge) rather than getting stuck entirely on one bad field; the
    // next reconcile (30s later, or sooner on a spec edit) retries the
    // validation once the spec is fixed.
    if let Some(tls) = &cluster.spec.tls
        && let Err(e) = tls.validate()
    {
        warn!(cluster = %name, error = %e, "refusing invalid spec.tls");
        set_condition(&mut status, CONDITION_TLS_SPEC_INVALID, e);
        let mut pinned = (*cluster).clone();
        pinned.spec.tls = None;
        let pdb_control_nodes = pinned.spec.control_nodes_or_default();
        return finish_reconcile(&pinned, &ctx, &ns, status, pdb_control_nodes).await;
    }
    status
        .conditions
        .retain(|c| c.type_ != CONDITION_TLS_SPEC_INVALID);

    // Validate `spec.encryptionKeySecretName` (ADR 0069, S-03 PR 3): unlike
    // every check above, this one is LIVE (a Secret reference's only
    // checkable shape is whether it actually exists — nothing in the spec
    // itself can say). On failure this deliberately does NOT strip the
    // field and reconcile the rest of the spec as if it were unset — see
    // `CONDITION_ENCRYPTION_KEY_SECRET_INVALID`'s own doc for why that
    // fallback would be actively dangerous here (it would regenerate a
    // plaintext `cluster.json` for a cluster whose data directory may
    // already be encrypted). The condition is purely informational; every
    // other child still reconciles normally either way.
    if let Some(secret_name) = &cluster.spec.encryption_key_secret_name {
        match validate_encryption_key_secret(&ctx.cluster_api, &ns, secret_name).await? {
            Some(e) => {
                warn!(
                    cluster = %name,
                    secret = %secret_name,
                    error = %e,
                    "spec.encryptionKeySecretName is not usable yet"
                );
                set_condition(&mut status, CONDITION_ENCRYPTION_KEY_SECRET_INVALID, e);
            }
            None => {
                status
                    .conditions
                    .retain(|c| c.type_ != CONDITION_ENCRYPTION_KEY_SECRET_INVALID);
            }
        }
    } else {
        status
            .conditions
            .retain(|c| c.type_ != CONDITION_ENCRYPTION_KEY_SECRET_INVALID);
    }

    // Validate `spec.s3` (S-04 PR 3): same "no admission webhook in v1"
    // posture as `spec.tls` above — set a condition and reconcile the rest
    // of the spec with `s3` stripped rather than getting stuck entirely.
    if let Some(s3) = &cluster.spec.s3
        && let Err(e) = s3.validate()
    {
        warn!(cluster = %name, error = %e, "refusing invalid spec.s3");
        set_condition(&mut status, CONDITION_S3_SPEC_INVALID, e);
        let mut pinned = (*cluster).clone();
        pinned.spec.s3 = None;
        let pdb_control_nodes = pinned.spec.control_nodes_or_default();
        return finish_reconcile(&pinned, &ctx, &ns, status, pdb_control_nodes).await;
    }
    status
        .conditions
        .retain(|c| c.type_ != CONDITION_S3_SPEC_INVALID);

    // Validate `spec.backupStore`/`spec.segmentStore` (S-07b): same "no
    // admission webhook in v1" posture as `spec.tls`/`spec.s3` above — set a
    // condition and reconcile the rest of the spec with both fields
    // stripped rather than getting stuck entirely. Checked *after*
    // `spec.s3` above (not before): an invalid `spec.s3` already returned
    // early, so by this point `cluster.spec.s3` is either `None` or valid,
    // which is what lets `validate_store_spec`'s own conflict check trust
    // it.
    if let Err(e) = cluster.spec.validate_store_spec() {
        warn!(
            cluster = %name,
            error = %e,
            "refusing invalid spec.backupStore/spec.segmentStore"
        );
        set_condition(&mut status, CONDITION_STORE_SPEC_INVALID, e);
        let mut pinned = (*cluster).clone();
        pinned.spec.backup_store = None;
        pinned.spec.segment_store = None;
        let pdb_control_nodes = pinned.spec.control_nodes_or_default();
        return finish_reconcile(&pinned, &ctx, &ns, status, pdb_control_nodes).await;
    }
    status
        .conditions
        .retain(|c| c.type_ != CONDITION_STORE_SPEC_INVALID);

    // S-07d: `spec.controlNodes` may only ever *increase* — a decrease is
    // still rejected outright (unchanged from the old "immutable" posture:
    // set a condition, keep going with every other field, but never
    // regenerate the config with the smaller value). An increase is
    // instead driven forward, one voter at a time, by
    // `advance_control_growth` — see this module's own "S-07d:
    // spec.controlNodes growth" section doc above for the full design.
    let target_control_nodes = cluster.spec.control_nodes_or_default();
    let prior_control_nodes =
        previous_applied_control_nodes(&ctx.cluster_api, &ns, &cluster).await?;
    let pdb_control_nodes = match prior_control_nodes {
        Some(prior) if target_control_nodes < prior => {
            warn!(
                cluster = %name,
                prior_control_nodes = prior,
                requested_control_nodes = target_control_nodes,
                "refusing controlNodes decrease"
            );
            set_condition(
                &mut status,
                CONDITION_CONTROL_NODES_SHRINK_REJECTED,
                format!(
                    "spec.controlNodes decreased from {prior} to {target_control_nodes} — \
                     ignored; controlNodes can grow but never shrink once a cluster is running"
                ),
            );
            // Reconcile with the *prior* control-node count so the running
            // cluster's own role split never actually changes underneath it.
            let mut pinned = (*cluster).clone();
            pinned.spec.control_nodes = Some(prior);
            return finish_reconcile(&pinned, &ctx, &ns, status, prior).await;
        }
        Some(prior) => {
            status
                .conditions
                .retain(|c| c.type_ != CONDITION_CONTROL_NODES_SHRINK_REJECTED);
            let already_growing = status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_CONTROL_NODES_GROWING);
            if target_control_nodes > prior || already_growing {
                let admin_port =
                    cluster.spec.base_port_or_default() + desired::cluster_config::PORT_ADMIN;
                let internal_port =
                    cluster.spec.base_port_or_default() + desired::cluster_config::PORT_INTERNAL;
                let tls_ca = resolve_tls_ca(&ctx.cluster_api, &cluster, &ns, &name).await?;
                advance_control_growth(
                    &ctx,
                    &name,
                    &ns,
                    target_control_nodes,
                    prior,
                    admin_port,
                    internal_port,
                    tls_ca.as_deref(),
                    &mut status,
                )
                .await
            } else {
                target_control_nodes
            }
        }
        None => {
            status
                .conditions
                .retain(|c| c.type_ != CONDITION_CONTROL_NODES_SHRINK_REJECTED);
            target_control_nodes
        }
    };

    // Refuse scaling below `controlNodes` — every control-role pod must
    // stay present (the control-plane Raft group needs its full voter
    // set); a data-only pod may always be removed.
    let control_nodes = target_control_nodes;
    if cluster.spec.nodes < control_nodes {
        warn!(
            cluster = %name,
            nodes = cluster.spec.nodes,
            control_nodes,
            "refusing scale below controlNodes"
        );
        set_condition(
            &mut status,
            CONDITION_SCALE_BELOW_CONTROL_NODES_REFUSED,
            format!(
                "spec.nodes ({}) is below spec.controlNodes ({control_nodes}) — ignored",
                cluster.spec.nodes
            ),
        );
        return finish_reconcile(&cluster, &ctx, &ns, status, pdb_control_nodes).await;
    }

    // Scale-down: drain+remove every pod ordinal being dropped, highest
    // first, before the `StatefulSet`'s own replica count goes down.
    if let Some(existing) = ctx.cluster_api.get_statefulset(&ns, &name).await? {
        let current_replicas = existing.spec.and_then(|s| s.replicas).unwrap_or(0);
        let target_replicas = cluster.spec.nodes;
        if target_replicas < current_replicas {
            let admin_port =
                cluster.spec.base_port_or_default() + desired::cluster_config::PORT_ADMIN;
            // ADR 0064 commit 3: the admin port speaks server-only TLS
            // whenever `spec.tls` is set (`animusd` serves it that way —
            // see `crd::TlsSpec`'s own doc); read the cluster CA out of
            // the resolved `Secret` once, up front, for every drain call
            // below. `None` here when `spec.tls` names a `Secret`
            // cert-manager hasn't finished issuing yet — the drain call
            // then fails against a TLS-only admin port (a real, surfaced
            // `DrainFailed` condition), retried on the next reconcile once
            // the `Secret` exists, rather than silently dialing plaintext
            // into a TLS listener.
            let tls_ca = resolve_tls_ca(&ctx.cluster_api, &cluster, &ns, &name).await?;
            for ordinal in (target_replicas..current_replicas).rev() {
                if let Err(e) = drain_and_remove_node(
                    &ctx.admin,
                    &name,
                    &ns,
                    ordinal,
                    admin_port,
                    tls_ca.as_deref(),
                )
                .await
                {
                    error!(cluster = %name, ordinal, error = %e, "scale-down drain failed");
                    set_condition(
                        &mut status,
                        CONDITION_DRAIN_FAILED,
                        format!("draining pod ordinal {ordinal} before scale-down: {e}"),
                    );
                    // Stop the drain sequence here — don't scale the
                    // StatefulSet down past a pod that never finished
                    // draining, and don't attempt a lower ordinal either
                    // (they must go highest-first).
                    return finish_reconcile(&cluster, &ctx, &ns, status, pdb_control_nodes).await;
                }
            }
            status
                .conditions
                .retain(|c| c.type_ != CONDITION_DRAIN_FAILED);
        }
    }

    finish_reconcile(&cluster, &ctx, &ns, status, pdb_control_nodes).await
}

async fn finish_reconcile<C: ClusterApi, A: AdminOps>(
    cluster: &AnimusCluster,
    ctx: &Context<C, A>,
    ns: &str,
    mut status: AnimusClusterStatus,
    pdb_control_nodes: i32,
) -> Result<Action, ReconcileError> {
    let name = cluster.name_any();
    let applied_sts = apply_children(&ctx.cluster_api, cluster, ns, pdb_control_nodes).await?;

    let desired_replicas = cluster.spec.nodes;
    let ready = applied_sts
        .status
        .as_ref()
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0);
    status.ready_nodes = Some(ready);

    let has_blocking_condition = status.conditions.iter().any(|c| {
        c.type_ == CONDITION_DRAIN_FAILED
            || c.type_ == CONDITION_CONTROL_NODES_SHRINK_REJECTED
            || c.type_ == CONDITION_SCALE_BELOW_CONTROL_NODES_REFUSED
    });
    status.phase = Some(if has_blocking_condition && ready < desired_replicas {
        ClusterPhase::Degraded
    } else if ready >= desired_replicas {
        ClusterPhase::Ready
    } else if ready > 0 {
        ClusterPhase::Bootstrapping
    } else {
        ClusterPhase::Pending
    });

    ctx.cluster_api
        .patch_cluster_status(ns, &name, &status)
        .await?;

    Ok(Action::requeue(REQUEUE_OK))
}

fn error_policy<C: ClusterApi, A: AdminOps>(
    cluster: Arc<AnimusCluster>,
    err: &ReconcileError,
    _ctx: Arc<Context<C, A>>,
) -> Action {
    error!(
        cluster = %cluster.name_any(),
        error = %err,
        "reconcile failed"
    );
    Action::requeue(REQUEUE_ERR)
}

/// Run the controller loop against `client` forever (until the process is
/// asked to stop). Watches `AnimusCluster` plus its five owned, typed
/// child kinds (the cert-manager `Certificate` is a `DynamicObject`, not
/// watched here) so an out-of-band edit to a child (e.g. `kubectl edit
/// statefulset`) triggers a reconcile that reverts the drift, not just a
/// spec change on the parent.
///
/// `admin_access` (`--admin-access {proxy,direct}`, `crate::main`) selects
/// how every admin-port call this loop makes — the scale-down drain
/// sequence today — reaches its target pod; see `crate::admin_client`'s
/// own doc for the trade-off. Defaults to `AdminAccessMode::Proxy`, which
/// works in every deployment shape this crate supports.
pub async fn run(client: Client, admin_access: AdminAccessMode) {
    let clusters = Api::<AnimusCluster>::all(client.clone());
    let ctx = Arc::new(Context {
        cluster_api: RealClusterApi::new(client.clone()),
        admin: RealAdminClient::new(admin_access, client.clone()),
    });

    Controller::new(clusters, watcher::Config::default())
        .owns(
            Api::<StatefulSet>::all(client.clone()),
            watcher::Config::default(),
        )
        .owns(
            Api::<ConfigMap>::all(client.clone()),
            watcher::Config::default(),
        )
        .owns(
            Api::<Service>::all(client.clone()),
            watcher::Config::default(),
        )
        .owns(
            Api::<NetworkPolicy>::all(client.clone()),
            watcher::Config::default(),
        )
        .owns(
            Api::<PodDisruptionBudget>::all(client),
            watcher::Config::default(),
        )
        .run(
            reconcile::<RealClusterApi, RealAdminClient>,
            error_policy::<RealClusterApi, RealAdminClient>,
            ctx,
        )
        .for_each(|res| async move {
            match res {
                Ok((obj, action)) => {
                    tracing::debug!(?obj, ?action, "reconciled");
                }
                Err(e) => {
                    tracing::warn!(error = %e, "reconcile stream error");
                }
            }
        })
        .await;
}

/// ADR 0061 rung E1: `reconcile`/`previous_applied_control_nodes`/
/// `drain_and_remove_node` exercised via `crate::fakes::{FakeClusterApi,
/// FakeAdminClient}` — no live API server, no real socket. See that
/// module's doc and `crates/animus-operator/CLAUDE.md`'s testing section
/// for what this harness does and does not prove.
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use k8s_openapi::api::core::v1::ConfigMap;
    use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

    use super::*;
    use crate::crd::{AnimusClusterSpec, CertManagerSpec, IssuerRef, S3StoreSpec, TlsSpec};
    use crate::desired::test_support::test_cluster;
    use crate::fakes::{AppliedKind, FakeAdminClient, FakeClusterApi};

    fn make_ctx(
        cluster_api: FakeClusterApi,
        admin: FakeAdminClient,
    ) -> Arc<Context<FakeClusterApi, FakeAdminClient>> {
        Arc::new(Context { cluster_api, admin })
    }

    /// The exact URL `admin_base_url` + a path suffix builds — used to
    /// assert on `FakeAdminClient::calls()` without duplicating
    /// `drain_and_remove_node`'s own URL-building logic.
    fn admin_url(name: &str, ns: &str, ordinal: i32, admin_port: i32, path: &str) -> String {
        format!(
            "{}{path}",
            admin_base_url(name, ns, ordinal, admin_port, false)
        )
    }

    /// A `ConfigMap` shaped exactly like the one a previous reconcile would
    /// have applied for `spec`, for seeding `FakeClusterApi::seed_configmap`
    /// in the `previous_applied_control_nodes`/shrink-rejection tests.
    fn prior_cluster_configmap(name: &str, ns: &str, spec: &AnimusClusterSpec) -> ConfigMap {
        let config = desired::cluster_config::build_cluster_config(name, ns, spec);
        let mut cm = ConfigMap::default();
        cm.metadata.name = Some(desired::config_map_name(name));
        cm.data = Some(BTreeMap::from([(
            desired::cluster_config::CONFIG_FILE_NAME.to_string(),
            desired::cluster_config::to_json(&config),
        )]));
        cm
    }

    // --- (1) a fresh cluster reconcile creates the expected children -----

    #[tokio::test]
    async fn reconcile_fresh_cluster_applies_all_six_children_in_order() {
        let cluster = Arc::new(test_cluster("demo", "ns1", 3, None));
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        let result = reconcile(Arc::clone(&cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        assert_eq!(
            ctx.cluster_api.applies(),
            vec![
                (AppliedKind::ConfigMap, desired::config_map_name("demo")),
                (AppliedKind::Service, desired::internal_service_name("demo")),
                (AppliedKind::Service, desired::client_service_name("demo")),
                (
                    AppliedKind::NetworkPolicy,
                    desired::network_policy_name("demo")
                ),
                (
                    AppliedKind::PodDisruptionBudget,
                    desired::pod_disruption_budget_name("demo")
                ),
                (AppliedKind::StatefulSet, "demo".to_string()),
            ]
        );

        // No `StatefulSet` status was ever reported ready (nothing seeded),
        // so a fresh cluster's own first reconcile lands in `Pending`.
        let status = ctx.cluster_api.last_status().expect("status was patched");
        assert_eq!(status.phase, Some(ClusterPhase::Pending));
        assert_eq!(status.ready_nodes, Some(0));
    }

    // --- (2) a reconcile of an unchanged cluster: pin the actual behavior -

    #[tokio::test]
    async fn reconcile_of_unchanged_cluster_reapplies_every_child_again() {
        // `apply_children` never diffs against what's already applied —
        // every reconcile unconditionally re-applies all six children, an
        // idempotent re-apply rather than a no-op. This test pins that
        // choice so a future change to the behavior is a deliberate,
        // visible diff here, not a silent regression.
        let cluster = Arc::new(test_cluster("demo", "ns1", 3, None));
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        reconcile(Arc::clone(&cluster), Arc::clone(&ctx))
            .await
            .unwrap();
        let first = ctx.cluster_api.applies();
        assert_eq!(first.len(), 6);

        reconcile(Arc::clone(&cluster), Arc::clone(&ctx))
            .await
            .unwrap();
        let second = ctx.cluster_api.applies();
        assert_eq!(second.len(), 12);
        assert_eq!(&second[..6], &first[..]);
        assert_eq!(&second[6..], &first[..]);
    }

    // --- (3) previous_applied_control_nodes reads the prior applied value -

    #[tokio::test]
    async fn previous_applied_control_nodes_reads_the_prior_configmaps_value() {
        let fake = FakeClusterApi::new();
        let prior_spec = AnimusClusterSpec {
            nodes: 5,
            control_nodes: Some(3),
            ..Default::default()
        };
        fake.seed_configmap(
            &desired::config_map_name("demo"),
            prior_cluster_configmap("demo", "ns1", &prior_spec),
        );

        let cluster = test_cluster("demo", "ns1", 5, Some(5));
        let result = previous_applied_control_nodes(&fake, "ns1", &cluster)
            .await
            .unwrap();
        assert_eq!(result, Some(3));
    }

    #[tokio::test]
    async fn previous_applied_control_nodes_still_returns_the_value_when_unchanged() {
        // S-07d: unlike the old `control_nodes_changed`, this no longer
        // compares against the spec's own desired value at all — it always
        // reports the prior applied value, changed or not, since the
        // growth machinery needs to keep observing it every reconcile
        // while a growth is in flight.
        let fake = FakeClusterApi::new();
        let prior_spec = AnimusClusterSpec {
            nodes: 5,
            control_nodes: Some(3),
            ..Default::default()
        };
        fake.seed_configmap(
            &desired::config_map_name("demo"),
            prior_cluster_configmap("demo", "ns1", &prior_spec),
        );

        let cluster = test_cluster("demo", "ns1", 5, Some(3));
        let result = previous_applied_control_nodes(&fake, "ns1", &cluster)
            .await
            .unwrap();
        assert_eq!(result, Some(3));
    }

    #[tokio::test]
    async fn previous_applied_control_nodes_is_none_when_no_prior_configmap() {
        // A fresh cluster (nothing applied yet): nothing to compare
        // against, so this must never look like a shrink attempt.
        let fake = FakeClusterApi::new();
        let cluster = test_cluster("demo", "ns1", 3, None);
        let result = previous_applied_control_nodes(&fake, "ns1", &cluster)
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    // --- S-07d: the pure growth-decision functions -------------------------

    #[test]
    fn next_growth_ordinal_finds_the_first_missing_voter() {
        let voters: std::collections::BTreeSet<String> =
            ["demo-0", "demo-1"].into_iter().map(String::from).collect();
        assert_eq!(next_growth_ordinal("demo", 5, &voters), Some(2));
    }

    #[test]
    fn next_growth_ordinal_is_none_once_every_ordinal_is_a_voter() {
        let voters: std::collections::BTreeSet<String> = ["demo-0", "demo-1", "demo-2"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(next_growth_ordinal("demo", 3, &voters), None);
    }

    #[test]
    fn next_growth_ordinal_ignores_an_unrelated_extra_voter() {
        // A voter set that (impossibly, but defensively) contains an id
        // this cluster never minted must not confuse the search — it looks
        // for `node_id(name, i)` specifically, not just "any 3 entries".
        let voters: std::collections::BTreeSet<String> = ["demo-0", "someone-else-7"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(next_growth_ordinal("demo", 3, &voters), Some(1));
    }

    #[test]
    fn achieved_control_nodes_is_the_leading_confirmed_prefix() {
        let voters: std::collections::BTreeSet<String> =
            ["demo-0", "demo-1"].into_iter().map(String::from).collect();
        assert_eq!(achieved_control_nodes("demo", 5, &voters), 2);
    }

    #[test]
    fn achieved_control_nodes_is_the_target_once_fully_caught_up() {
        let voters: std::collections::BTreeSet<String> = ["demo-0", "demo-1", "demo-2"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(achieved_control_nodes("demo", 3, &voters), 3);
    }

    #[test]
    fn achieved_control_nodes_is_zero_with_no_voters_observed() {
        assert_eq!(
            achieved_control_nodes("demo", 3, &std::collections::BTreeSet::new()),
            0
        );
    }

    #[test]
    fn parse_voters_reads_the_voters_array() {
        let body = serde_json::json!({"voters": ["demo-0", "demo-1"], "addrs": {}});
        let voters = parse_voters(&body).expect("voters array present");
        assert_eq!(
            voters,
            ["demo-0", "demo-1"].into_iter().map(String::from).collect()
        );
    }

    #[test]
    fn parse_voters_is_none_when_voters_is_null() {
        // `control_members_view`'s own documented "unknown, not empty"
        // shape for a `Remote` handle that has never observed a voter set.
        let body = serde_json::json!({"voters": null, "addrs": {}});
        assert_eq!(parse_voters(&body), None);
    }

    #[test]
    fn parse_voters_is_none_when_voters_is_absent() {
        let body = serde_json::json!({});
        assert_eq!(parse_voters(&body), None);
    }

    // --- (3b) `ordinal_reports_role_both` checks the real `animusd` -------
    // --- literal, never the unrelated `cluster.json` one ------------------

    /// A minimal `AdminOps` that always answers `GET /admin/config` with a
    /// fixed `role` value — used to pin exactly which JSON literal
    /// `ordinal_reports_role_both` treats as "this pod has restarted into
    /// combined mode", independent of `FakeAdminClient`'s own behavior
    /// (which is a mock this crate maintains by hand and could drift from
    /// `animusd`'s real shape the same way it once did — see this struct's
    /// own regression note).
    struct FixedRoleAdmin(&'static str);

    #[async_trait::async_trait]
    impl AdminOps for FixedRoleAdmin {
        async fn post_json(
            &self,
            _url: &str,
            _body: &serde_json::Value,
            _ca_pem: Option<&[u8]>,
        ) -> Result<serde_json::Value, String> {
            unreachable!("this test never posts")
        }
        async fn get_json(
            &self,
            _url: &str,
            _ca_pem: Option<&[u8]>,
        ) -> Result<serde_json::Value, String> {
            Ok(serde_json::json!({ "role": self.0 }))
        }
    }

    #[tokio::test]
    async fn ordinal_reports_role_both_accepts_animusds_real_combined_literal() {
        // Pinned against `AdminInfo.role`'s real value for a combined-mode
        // node (`crates/animusd/src/lib.rs`'s `role: "combined"` assembly,
        // server-side pinned by `crates/animusd/tests/dashboard_
        // endpoint.rs`'s `config_view["role"] == "combined"` assertion).
        // This is the literal comparison that once read `"both"` instead —
        // a JSON-shape mismatch with the real `animusd` that made this
        // function always return `false`, so growth waited forever for a
        // restart signal that could never arrive (root cause of the S-07d
        // e2e timeout this test closes).
        let admin = FixedRoleAdmin("combined");
        assert!(ordinal_reports_role_both(&admin, "demo", "ns1", 3, 14003, None).await);
    }

    #[tokio::test]
    async fn ordinal_reports_role_both_rejects_the_unrelated_cluster_json_literal() {
        // `"both"` is `desired::cluster_config::NodeRole::Both`'s own
        // spelling on a *different* JSON document (the generated
        // `cluster.json`, not the runtime `/admin/config` response) — it
        // must never be mistaken for the real `animusd` role literal.
        let admin = FixedRoleAdmin("both");
        assert!(!ordinal_reports_role_both(&admin, "demo", "ns1", 3, 14003, None).await);
    }

    #[tokio::test]
    async fn ordinal_reports_role_both_rejects_data_and_control() {
        for role in ["data", "control"] {
            let admin = FixedRoleAdmin(role);
            assert!(!ordinal_reports_role_both(&admin, "demo", "ns1", 3, 14003, None).await);
        }
    }

    // --- (4) drain_and_remove_node's sequence, including the bounded ------
    // --- never-completes failure path -------------------------------------

    #[tokio::test]
    async fn drain_and_remove_node_succeeds_when_drain_completes_immediately() {
        // No response queued: FakeAdminClient's default GET response is
        // "already fully drained", so the sequence completes in one poll.
        let admin = FakeAdminClient::new();
        let result = drain_and_remove_node(&admin, "demo", "ns1", 2, 14003, None).await;
        assert!(result.is_ok(), "{result:?}");

        assert_eq!(
            admin.calls(),
            vec![
                (
                    "POST".to_string(),
                    admin_url("demo", "ns1", 2, 14003, "/admin/drain")
                ),
                (
                    "GET".to_string(),
                    admin_url(
                        "demo",
                        "ns1",
                        2,
                        14003,
                        &format!("/admin/member/drain-status?node={}", node_id_for_test())
                    )
                ),
                (
                    "POST".to_string(),
                    admin_url("demo", "ns1", 2, 14003, "/admin/member/remove")
                ),
            ]
        );
    }

    /// `node_id("demo", 2)` — a tiny local mirror since `node_id` is
    /// private to `desired::cluster_config`.
    fn node_id_for_test() -> String {
        desired::cluster_config::node_id("demo", 2)
    }

    #[tokio::test(start_paused = true)]
    async fn drain_and_remove_node_is_bounded_when_drain_never_completes() {
        let admin = FakeAdminClient::new();
        // Exactly one queued response: it repeats forever (see
        // `FakeAdminClient`'s own doc), so the drain never satisfies
        // `drain_and_remove_node`'s completion check.
        admin.queue_drain_status(3, "Draining");

        let result = drain_and_remove_node(&admin, "demo", "ns1", 4, 14003, None).await;
        let err = result.expect_err("a drain that never completes must eventually give up");
        assert!(
            err.contains("did not finish draining after 120 polls"),
            "{err}"
        );

        let calls = admin.calls();
        // 1 drain POST + 120 status GETs, never reaching remove — bounded,
        // not a spin loop.
        assert_eq!(calls.len(), 1 + 120);
        assert!(calls.iter().all(|(_, url)| !url.contains("/member/remove")));
    }

    // --- reconcile-level scale-down sequencing, both the happy path and ---
    // --- the stop-on-first-failure path ------------------------------------

    #[tokio::test]
    async fn reconcile_scale_down_drains_removed_ordinals_highest_first() {
        let fake_cluster = FakeClusterApi::new();
        // A previous reconcile already scaled this cluster to 5 replicas.
        fake_cluster.seed_statefulset("demo", 5, 5);
        let ctx = make_ctx(fake_cluster, FakeAdminClient::new());

        // Target: 3 nodes — ordinals 3 and 4 must be drained+removed,
        // highest first, before anything else.
        let cluster = Arc::new(test_cluster("demo", "ns1", 3, None));
        let result = reconcile(Arc::clone(&cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let drain_posts: Vec<String> = ctx
            .admin
            .calls()
            .into_iter()
            .filter(|(m, u)| m == "POST" && u.ends_with("/admin/drain"))
            .map(|(_, u)| u)
            .collect();
        assert_eq!(
            drain_posts,
            vec![
                admin_url("demo", "ns1", 4, 14003, "/admin/drain"),
                admin_url("demo", "ns1", 3, 14003, "/admin/drain"),
            ]
        );
        let remove_posts: Vec<String> = ctx
            .admin
            .calls()
            .into_iter()
            .filter(|(m, u)| m == "POST" && u.ends_with("/admin/member/remove"))
            .map(|(_, u)| u)
            .collect();
        assert_eq!(
            remove_posts,
            vec![
                admin_url("demo", "ns1", 4, 14003, "/admin/member/remove"),
                admin_url("demo", "ns1", 3, 14003, "/admin/member/remove"),
            ]
        );

        // No blocking condition: the drain sequence succeeded.
        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            !status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_DRAIN_FAILED)
        );
    }

    #[tokio::test]
    async fn reconcile_scale_down_stops_on_first_drain_failure() {
        let fake_cluster = FakeClusterApi::new();
        fake_cluster.seed_statefulset("demo", 5, 5);
        let fake_admin = FakeAdminClient::new();
        fake_admin.fail_drain();
        let ctx = make_ctx(fake_cluster, fake_admin);

        let cluster = Arc::new(test_cluster("demo", "ns1", 3, None));
        let result = reconcile(Arc::clone(&cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_DRAIN_FAILED)
        );

        // Only the highest ordinal (4) was ever attempted — the sequence
        // must stop there, never touching ordinal 3, and never reaching
        // "remove" for anything.
        let calls = ctx.admin.calls();
        let drain_posts: Vec<&String> = calls
            .iter()
            .filter(|(m, u)| m == "POST" && u.ends_with("/admin/drain"))
            .map(|(_, u)| u)
            .collect();
        assert_eq!(
            drain_posts,
            vec![&admin_url("demo", "ns1", 4, 14003, "/admin/drain")]
        );
        assert!(!calls.iter().any(|(_, u)| u.contains("/member/remove")));
    }

    // --- controlNodes decrease is still refused (S-07d renamed this from
    // --- "immutable" to "shrink-rejected"; growth is now honored instead) -

    #[tokio::test]
    async fn reconcile_refuses_control_nodes_decrease() {
        let fake_cluster = FakeClusterApi::new();
        let prior_spec = AnimusClusterSpec {
            nodes: 5,
            control_nodes: Some(5),
            ..Default::default()
        };
        fake_cluster.seed_configmap(
            &desired::config_map_name("demo"),
            prior_cluster_configmap("demo", "ns1", &prior_spec),
        );
        let ctx = make_ctx(fake_cluster, FakeAdminClient::new());

        // The spec now asks for controlNodes: 3 — refused, since it
        // previously applied as 5; a decrease is never honored.
        let cluster = Arc::new(test_cluster("demo", "ns1", 5, Some(3)));
        let result = reconcile(Arc::clone(&cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_CONTROL_NODES_SHRINK_REJECTED)
        );
        assert!(
            !status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_CONTROL_NODES_GROWING),
            "a rejected decrease must never also look like a growth in progress"
        );

        // The re-applied ConfigMap must still reflect the *prior*
        // controlNodes value (5 "both" roles), never the refused smaller
        // one.
        let cm = ctx
            .cluster_api
            .configmap(&desired::config_map_name("demo"))
            .expect("ConfigMap re-applied");
        let json = cm
            .data
            .as_ref()
            .unwrap()
            .get(desired::cluster_config::CONFIG_FILE_NAME)
            .unwrap();
        let parsed: desired::cluster_config::ClusterConfig = serde_json::from_str(json).unwrap();
        let both_count = parsed
            .nodes
            .iter()
            .filter(|n| matches!(n.role, desired::cluster_config::NodeRole::Both))
            .count();
        assert_eq!(both_count, 5);
    }

    // --- S-07d: spec.controlNodes growth, end to end via FakeAdminClient --

    /// Seed everything a growth-in-progress test needs: a prior applied
    /// `ConfigMap` recording `control_nodes: 3` for a 5-node cluster, and
    /// the fake control group's own live voter set matching that same
    /// prior shape (`demo-0..2`) — the steady state right before an
    /// operator edits `spec.controlNodes` from 3 to 5.
    fn seed_pre_growth_state(fake_cluster: &FakeClusterApi, fake_admin: &FakeAdminClient) {
        let prior_spec = AnimusClusterSpec {
            nodes: 5,
            control_nodes: Some(3),
            ..Default::default()
        };
        fake_cluster.seed_configmap(
            &desired::config_map_name("demo"),
            prior_cluster_configmap("demo", "ns1", &prior_spec),
        );
        fake_admin.seed_control_voters(["demo-0", "demo-1", "demo-2"].map(String::from));
    }

    #[tokio::test]
    async fn reconcile_grows_regenerates_the_configmap_role_split_immediately() {
        // The ConfigMap/StatefulSet role split must reflect the full
        // *target* the moment the spec changes — the promoted ordinal
        // can't restart into role "both" at all otherwise — even though no
        // voter has actually caught up yet.
        let fake_cluster = FakeClusterApi::new();
        let fake_admin = FakeAdminClient::new();
        seed_pre_growth_state(&fake_cluster, &fake_admin);
        let ctx = make_ctx(fake_cluster, fake_admin);

        let cluster = Arc::new(test_cluster("demo", "ns1", 5, Some(5)));
        let result = reconcile(Arc::clone(&cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        assert!(
            !ctx.cluster_api
                .last_status()
                .unwrap()
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_CONTROL_NODES_SHRINK_REJECTED),
            "growth must never be treated as a rejected shrink"
        );

        let cm = ctx
            .cluster_api
            .configmap(&desired::config_map_name("demo"))
            .expect("ConfigMap re-applied");
        let json = cm
            .data
            .as_ref()
            .unwrap()
            .get(desired::cluster_config::CONFIG_FILE_NAME)
            .unwrap();
        let parsed: desired::cluster_config::ClusterConfig = serde_json::from_str(json).unwrap();
        let both_count = parsed
            .nodes
            .iter()
            .filter(|n| matches!(n.role, desired::cluster_config::NodeRole::Both))
            .count();
        assert_eq!(
            both_count, 5,
            "the ConfigMap must already show the full growth target"
        );
    }

    #[tokio::test]
    async fn reconcile_growth_waits_for_the_promoted_pod_before_adding_it() {
        let fake_cluster = FakeClusterApi::new();
        let fake_admin = FakeAdminClient::new();
        seed_pre_growth_state(&fake_cluster, &fake_admin);
        // Ordinal 3's own pod hasn't restarted into role "both" yet
        // (`mark_ordinal_ready_both` is never called) — no add attempted.
        let ctx = make_ctx(fake_cluster, fake_admin);

        let cluster = Arc::new(test_cluster("demo", "ns1", 5, Some(5)));
        reconcile(Arc::clone(&cluster), Arc::clone(&ctx))
            .await
            .unwrap();

        assert!(
            !ctx.admin
                .calls()
                .iter()
                .any(|(m, u)| m == "POST" && u.contains("/admin/control/member/add")),
            "must not attempt to add a voter before its pod reports role \"both\""
        );
        let status = ctx.cluster_api.last_status().unwrap();
        let growing = status
            .conditions
            .iter()
            .find(|c| c.type_ == CONDITION_CONTROL_NODES_GROWING)
            .expect("ControlNodesGrowing condition present");
        let msg = growing.message.as_deref().unwrap_or_default();
        assert!(msg.contains("3/5"), "{msg}");
        assert!(msg.contains("ordinal 3"), "{msg}");

        // The PDB must use the *achieved* count (3), not the full target
        // (5), while growth is still pending.
        let pdb = ctx
            .cluster_api
            .poddisruptionbudget(&desired::pod_disruption_budget_name("demo"))
            .unwrap();
        assert_eq!(
            pdb.spec.unwrap().max_unavailable,
            Some(IntOrString::Int(
                desired::poddisruptionbudget::safe_max_unavailable(5, 3)
            ))
        );
    }

    #[tokio::test]
    async fn reconcile_growth_adds_a_voter_once_its_pod_reports_role_both() {
        let fake_cluster = FakeClusterApi::new();
        let fake_admin = FakeAdminClient::new();
        seed_pre_growth_state(&fake_cluster, &fake_admin);
        fake_admin.mark_ordinal_ready_both(3);
        fake_cluster.seed_pod_ip("demo-3", "10.0.0.4");
        let ctx = make_ctx(fake_cluster, fake_admin);

        let cluster = Arc::new(test_cluster("demo", "ns1", 5, Some(5)));
        let result = reconcile(Arc::clone(&cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        assert!(
            ctx.admin.control_voters().contains("demo-3"),
            "ordinal 3 must have been added as a control voter"
        );
        let add_calls: Vec<String> = ctx
            .admin
            .calls()
            .into_iter()
            .filter(|(m, u)| m == "POST" && u.contains("/admin/control/member/add"))
            .map(|(_, u)| u)
            .collect();
        assert_eq!(
            add_calls,
            vec![admin_url(
                "demo",
                "ns1",
                0,
                14003,
                "/admin/control/member/add"
            )],
            "must try the first already-confirmed voter ordinal first"
        );

        let status = ctx.cluster_api.last_status().unwrap();
        let growing = status
            .conditions
            .iter()
            .find(|c| c.type_ == CONDITION_CONTROL_NODES_GROWING)
            .expect("still growing — only one of two missing voters was added");
        assert!(
            growing
                .message
                .as_deref()
                .unwrap_or_default()
                .contains("4/5"),
            "{:?}",
            growing.message
        );
    }

    #[tokio::test]
    async fn reconcile_growth_retries_a_different_voter_ordinal_when_the_first_refuses() {
        // Mirrors "retry on the leader" without an address hint (a `Local`
        // control handle's own `leader_addr_hint` is always `None`): every
        // already-confirmed voter ordinal is tried in turn, so a
        // not-currently-the-leader ordinal 0 doesn't block growth forever.
        let fake_cluster = FakeClusterApi::new();
        let fake_admin = FakeAdminClient::new();
        // Three already-confirmed voters this time, so there's a genuine
        // "second candidate" to fall back to.
        let prior_spec = AnimusClusterSpec {
            nodes: 6,
            control_nodes: Some(3),
            ..Default::default()
        };
        fake_cluster.seed_configmap(
            &desired::config_map_name("demo"),
            prior_cluster_configmap("demo", "ns1", &prior_spec),
        );
        fake_admin.seed_control_voters(["demo-0", "demo-1", "demo-2"].map(String::from));
        fake_admin.mark_ordinal_ready_both(3);
        fake_cluster.seed_pod_ip("demo-3", "10.0.0.4");
        let ctx = make_ctx(fake_cluster, fake_admin);
        ctx.admin.fail_add_control_member_for_ordinal(0);

        let cluster = Arc::new(test_cluster("demo", "ns1", 6, Some(4)));
        let result = reconcile(Arc::clone(&cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        assert!(ctx.admin.control_voters().contains("demo-3"));
        let add_calls: Vec<String> = ctx
            .admin
            .calls()
            .into_iter()
            .filter(|(m, u)| m == "POST" && u.contains("/admin/control/member/add"))
            .map(|(_, u)| u)
            .collect();
        assert_eq!(
            add_calls,
            vec![
                admin_url("demo", "ns1", 0, 14003, "/admin/control/member/add"),
                admin_url("demo", "ns1", 1, 14003, "/admin/control/member/add"),
            ],
            "ordinal 0 refused, so ordinal 1 must have been tried next"
        );
    }

    #[tokio::test]
    async fn reconcile_growth_completes_and_clears_the_condition() {
        // Live truth already shows every ordinal 0..5 as a confirmed
        // voter (as if a prior reconcile finished the job) — growth must
        // be recognized as complete and the PDB must use the full target.
        let fake_cluster = FakeClusterApi::new();
        let fake_admin = FakeAdminClient::new();
        seed_pre_growth_state(&fake_cluster, &fake_admin);
        fake_admin.seed_control_voters(
            ["demo-0", "demo-1", "demo-2", "demo-3", "demo-4"].map(String::from),
        );
        let ctx = make_ctx(fake_cluster, fake_admin);

        let cluster = Arc::new(test_cluster("demo", "ns1", 5, Some(5)));
        let result = reconcile(Arc::clone(&cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            !status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_CONTROL_NODES_GROWING),
            "{:?}",
            status.conditions
        );
        let pdb = ctx
            .cluster_api
            .poddisruptionbudget(&desired::pod_disruption_budget_name("demo"))
            .unwrap();
        assert_eq!(
            pdb.spec.unwrap().max_unavailable,
            Some(IntOrString::Int(
                desired::poddisruptionbudget::safe_max_unavailable(5, 5)
            ))
        );
    }

    #[tokio::test]
    async fn reconcile_growth_stall_is_visible_when_live_truth_is_unreachable() {
        // Diagnosability regression: before this fix, `advance_control_growth`
        // returning early on a failed `discover_control_voters` call left
        // `status.conditions` untouched — a stalled growth (every control
        // ordinal unreachable, every reconcile in a row) was invisible from
        // `kubectl get animuscluster -o yaml`, indistinguishable from a
        // reconcile that simply hadn't run yet. `CONDITION_CONTROL_NODES_
        // GROWING` must now be present and say so.
        let fake_cluster = FakeClusterApi::new();
        let fake_admin = FakeAdminClient::new();
        seed_pre_growth_state(&fake_cluster, &fake_admin);
        fake_admin.fail_control_members();
        let ctx = make_ctx(fake_cluster, fake_admin);

        let cluster = Arc::new(test_cluster("demo", "ns1", 5, Some(5)));
        let result = reconcile(Arc::clone(&cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let status = ctx.cluster_api.last_status().unwrap();
        let growing = status
            .conditions
            .iter()
            .find(|c| c.type_ == CONDITION_CONTROL_NODES_GROWING)
            .expect("a stalled growth must still record ControlNodesGrowing");
        let msg = growing.message.as_deref().unwrap_or_default();
        assert!(
            msg.contains("could not reach"),
            "message should explain the stall: {msg}"
        );

        // The PDB must fall back to the last confirmed-safe count (3), not
        // the unconfirmed full target (5).
        let pdb = ctx
            .cluster_api
            .poddisruptionbudget(&desired::pod_disruption_budget_name("demo"))
            .unwrap();
        assert_eq!(
            pdb.spec.unwrap().max_unavailable,
            Some(IntOrString::Int(
                desired::poddisruptionbudget::safe_max_unavailable(5, 3)
            ))
        );
    }

    #[tokio::test]
    async fn reconcile_grows_control_nodes_three_to_four_end_to_end() {
        // Mirrors `scripts/e2e-kind.sh`'s own S-07d phase exactly: a
        // 4-node cluster whose `controlNodes` is patched 3 -> 4. This is
        // the scenario that timed out in CI (issue root-caused to
        // `ordinal_reports_role_both` checking the wrong JSON literal,
        // "both" instead of `animusd`'s real "combined") — a full,
        // reconcile-level regression for that fix, independent of the
        // narrower `ordinal_reports_role_both_*` unit tests above.
        let fake_cluster = FakeClusterApi::new();
        let prior_spec = AnimusClusterSpec {
            nodes: 4,
            control_nodes: Some(3),
            ..Default::default()
        };
        fake_cluster.seed_configmap(
            &desired::config_map_name("e2e"),
            prior_cluster_configmap("e2e", "ns1", &prior_spec),
        );
        let fake_admin = FakeAdminClient::new();
        fake_admin.seed_control_voters(["e2e-0", "e2e-1", "e2e-2"].map(String::from));
        let ctx = make_ctx(fake_cluster, fake_admin);

        // First reconcile after the patch: ordinal 3 hasn't restarted yet
        // (still role "data") — the ConfigMap/StatefulSet regenerate to the
        // full target immediately, but no add is attempted, and the stall
        // is recorded.
        let cluster = Arc::new(test_cluster("e2e", "ns1", 4, Some(4)));
        reconcile(Arc::clone(&cluster), Arc::clone(&ctx))
            .await
            .unwrap();
        assert!(
            !ctx.admin
                .calls()
                .iter()
                .any(|(m, u)| m == "POST" && u.contains("/admin/control/member/add")),
            "must not add ordinal 3 before its pod actually reports role \"combined\""
        );
        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_CONTROL_NODES_GROWING),
        );

        // The promoted pod finishes restarting into combined mode (the
        // config-hash-triggered rolling restart, in the real cluster) —
        // its own `GET /admin/config` now reports the real `animusd`
        // literal.
        ctx.admin.mark_ordinal_ready_both(3);
        ctx.cluster_api.seed_pod_ip("e2e-3", "10.0.0.4");

        // Second reconcile: a real watch would deliver the object with the
        // status the first reconcile's own `patch_cluster_status` just
        // wrote (carrying the `ControlNodesGrowing` condition
        // `already_growing` needs, since the `ConfigMap` alone no longer
        // distinguishes "still growing" from "already at target" once it
        // regenerated to the full target on the first reconcile) — a fresh
        // `AnimusCluster` built from the same spec plus that status
        // reproduces that, unlike reusing `cluster` verbatim (an `Arc`'s
        // own `.status` never mutates in place).
        let mut cluster2 = test_cluster("e2e", "ns1", 4, Some(4));
        cluster2.status = Some(status.clone());
        reconcile(Arc::new(cluster2), Arc::clone(&ctx))
            .await
            .unwrap();
        assert!(ctx.admin.control_voters().contains("e2e-3"));
        assert_eq!(ctx.admin.control_voters().len(), 4);
        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            !status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_CONTROL_NODES_GROWING),
            "growth must be recognized as complete: {:?}",
            status.conditions
        );
    }

    #[tokio::test]
    async fn reconcile_resumes_growth_from_live_truth_after_a_simulated_restart() {
        // The ConfigMap already reflects the full target (as it would the
        // reconcile right after the spec edit landed — S-07d's own "P
        // becomes D the very next reconcile" property), so a bare
        // prior-vs-desired comparison could no longer tell growth is still
        // pending. What must carry it across is the `ControlNodesGrowing`
        // status condition surviving on the object itself (etcd-durable,
        // not this process's memory) — simulating exactly what a
        // controller restart sees.
        let fake_cluster = FakeClusterApi::new();
        let prior_spec = AnimusClusterSpec {
            nodes: 5,
            control_nodes: Some(5),
            ..Default::default()
        };
        fake_cluster.seed_configmap(
            &desired::config_map_name("demo"),
            prior_cluster_configmap("demo", "ns1", &prior_spec),
        );
        let fake_admin = FakeAdminClient::new();
        fake_admin.seed_control_voters(["demo-0", "demo-1", "demo-2"].map(String::from));
        fake_admin.mark_ordinal_ready_both(3);
        fake_cluster.seed_pod_ip("demo-3", "10.0.0.4");
        let ctx = make_ctx(fake_cluster, fake_admin);

        let mut cluster = test_cluster("demo", "ns1", 5, Some(5));
        cluster.status = Some(AnimusClusterStatus {
            conditions: vec![ClusterCondition {
                type_: CONDITION_CONTROL_NODES_GROWING.to_string(),
                status: ConditionStatus::True,
                reason: Some(CONDITION_CONTROL_NODES_GROWING.to_string()),
                message: Some("growing spec.controlNodes: 3/5 voters confirmed".to_string()),
                last_transition_time: None,
            }],
            ..Default::default()
        });

        let result = reconcile(Arc::new(cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());
        assert!(
            ctx.admin.control_voters().contains("demo-3"),
            "growth must resume from the surviving status condition, not stall forever \
             just because the ConfigMap already matches the target"
        );
    }

    #[tokio::test]
    async fn reconcile_refuses_control_nodes_increase_above_nodes() {
        // An increase above spec.nodes is rejected the same way a
        // spec.nodes decrease below controlNodes already is.
        let fake_cluster = FakeClusterApi::new();
        let fake_admin = FakeAdminClient::new();
        seed_pre_growth_state(&fake_cluster, &fake_admin);
        let ctx = make_ctx(fake_cluster, fake_admin);

        let cluster = Arc::new(test_cluster("demo", "ns1", 5, Some(6)));
        let result = reconcile(Arc::clone(&cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_SCALE_BELOW_CONTROL_NODES_REFUSED),
            "{:?}",
            status.conditions
        );
        assert!(
            !ctx.admin
                .calls()
                .iter()
                .any(|(m, u)| m == "POST" && u.contains("/admin/control/member/add")),
        );
    }

    // --- (6) spec.tls (ADR 0064 commit 3) --------------------------------

    fn cert_manager_tls() -> TlsSpec {
        TlsSpec {
            secret_name: None,
            cert_manager: Some(CertManagerSpec {
                issuer_ref: IssuerRef {
                    name: "i".to_string(),
                    kind: "ClusterIssuer".to_string(),
                    group: None,
                },
                duration: None,
                renew_before: None,
            }),
        }
    }

    #[tokio::test]
    async fn reconcile_applies_a_certificate_as_a_sixth_child_when_cert_manager_configured() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.tls = Some(cert_manager_tls());
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        let result = reconcile(Arc::new(cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let applies = ctx.cluster_api.applies();
        assert_eq!(applies.len(), 7, "{applies:?}");
        assert_eq!(
            applies[1],
            (AppliedKind::Certificate, "demo-tls".to_string())
        );
    }

    #[tokio::test]
    async fn reconcile_applies_no_certificate_for_the_secret_name_shape() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.tls = Some(TlsSpec {
            secret_name: Some("preexisting".to_string()),
            cert_manager: None,
        });
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        reconcile(Arc::new(cluster), Arc::clone(&ctx))
            .await
            .unwrap();

        let applies = ctx.cluster_api.applies();
        assert_eq!(applies.len(), 6, "{applies:?}");
        assert!(!applies.iter().any(|(k, _)| *k == AppliedKind::Certificate));
    }

    #[tokio::test]
    async fn reconcile_rejects_tls_spec_with_both_shapes_set() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.tls = Some(TlsSpec {
            secret_name: Some("preexisting".to_string()),
            cert_manager: Some(match cert_manager_tls().cert_manager {
                Some(cm) => cm,
                None => unreachable!(),
            }),
        });
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        let result = reconcile(Arc::new(cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_TLS_SPEC_INVALID),
            "{:?}",
            status.conditions
        );
        // Reconciled as if TLS were unset: no Certificate applied.
        assert!(
            !ctx.cluster_api
                .applies()
                .iter()
                .any(|(k, _)| *k == AppliedKind::Certificate)
        );
    }

    #[tokio::test]
    async fn reconcile_rejects_tls_spec_with_neither_shape_set() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.tls = Some(TlsSpec::default());
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        let result = reconcile(Arc::new(cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_TLS_SPEC_INVALID)
        );
    }

    #[tokio::test]
    async fn drain_and_remove_node_over_tls_dials_https_and_forwards_the_ca() {
        let admin = FakeAdminClient::new();
        let ca = b"fake-ca-pem";
        let result = drain_and_remove_node(&admin, "demo", "ns1", 2, 14003, Some(ca)).await;
        assert!(result.is_ok(), "{result:?}");
        let calls = admin.calls();
        assert!(
            calls.iter().all(|(_, url)| url.starts_with("https://")),
            "{calls:?}"
        );
    }

    #[tokio::test]
    async fn reconcile_scale_down_over_tls_reads_the_ca_from_the_resolved_secret() {
        use k8s_openapi::ByteString;
        use k8s_openapi::api::core::v1::Secret;

        let fake_cluster = FakeClusterApi::new();
        fake_cluster.seed_statefulset("demo", 5, 5);
        fake_cluster.seed_secret(
            "my-tls",
            Secret {
                data: Some(BTreeMap::from([(
                    "ca.crt".to_string(),
                    ByteString(b"fake-ca-pem".to_vec()),
                )])),
                ..Default::default()
            },
        );
        let ctx = make_ctx(fake_cluster, FakeAdminClient::new());

        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.tls = Some(TlsSpec {
            secret_name: Some("my-tls".to_string()),
            cert_manager: None,
        });
        let result = reconcile(Arc::new(cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let drain_posts: Vec<String> = ctx
            .admin
            .calls()
            .into_iter()
            .filter(|(m, u)| m == "POST" && u.ends_with("/admin/drain"))
            .map(|(_, u)| u)
            .collect();
        assert_eq!(
            drain_posts,
            vec![
                admin_url("demo", "ns1", 4, 14003, "/admin/drain")
                    .replacen("http://", "https://", 1),
                admin_url("demo", "ns1", 3, 14003, "/admin/drain")
                    .replacen("http://", "https://", 1),
            ]
        );
    }

    // --- spec.encryptionKeySecretName (ADR 0069, S-03 PR 3) ---------------

    fn parsed_cluster_config(
        cluster_api: &FakeClusterApi,
        name: &str,
    ) -> desired::cluster_config::ClusterConfig {
        let cm = cluster_api
            .configmap(&desired::config_map_name(name))
            .expect("ConfigMap applied");
        let json = cm
            .data
            .as_ref()
            .unwrap()
            .get(desired::cluster_config::CONFIG_FILE_NAME)
            .unwrap();
        serde_json::from_str(json).unwrap()
    }

    #[tokio::test]
    async fn reconcile_wires_encryption_key_path_when_secret_is_valid() {
        use k8s_openapi::ByteString;
        use k8s_openapi::api::core::v1::Secret;

        let fake_cluster = FakeClusterApi::new();
        fake_cluster.seed_secret(
            "my-encryption-key",
            Secret {
                data: Some(BTreeMap::from([(
                    desired::cluster_config::ENCRYPTION_KEY_SECRET_DATA_KEY.to_string(),
                    ByteString(vec![0u8; 32]),
                )])),
                ..Default::default()
            },
        );
        let ctx = make_ctx(fake_cluster, FakeAdminClient::new());

        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.encryption_key_secret_name = Some("my-encryption-key".to_string());
        let result = reconcile(Arc::new(cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            !status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_ENCRYPTION_KEY_SECRET_INVALID),
            "{:?}",
            status.conditions
        );

        let parsed = parsed_cluster_config(&ctx.cluster_api, "demo");
        assert!(
            parsed
                .nodes
                .iter()
                .all(|n| n.encryption_key_path.as_deref() == Some("/etc/animus/encryption/key")),
            "{parsed:?}"
        );

        // The StatefulSet's own pod template also carries the mount.
        let sts = ctx
            .cluster_api
            .get_statefulset("ns1", "demo")
            .await
            .unwrap()
            .expect("StatefulSet applied");
        let pod_spec = sts.spec.unwrap().template.spec.unwrap();
        assert!(
            pod_spec
                .volumes
                .unwrap()
                .iter()
                .any(|v| v.name == "encryption-key"),
            "encryption-key volume must be mounted once the secret is valid"
        );
    }

    #[tokio::test]
    async fn reconcile_sets_a_condition_when_encryption_key_secret_is_missing() {
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.encryption_key_secret_name = Some("does-not-exist".to_string());
        let result = reconcile(Arc::new(cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let status = ctx.cluster_api.last_status().unwrap();
        let condition = status
            .conditions
            .iter()
            .find(|c| c.type_ == CONDITION_ENCRYPTION_KEY_SECRET_INVALID)
            .expect("condition set");
        let message = condition.message.as_deref().unwrap_or_default();
        assert!(message.contains("does-not-exist"), "{message}");
        assert!(message.contains("does not exist"), "{message}");

        // Deliberately NOT stripped: the desired state still carries the
        // field (see the condition's own doc for why falling back to
        // plaintext here would be actively dangerous, not merely inert).
        let parsed = parsed_cluster_config(&ctx.cluster_api, "demo");
        assert!(
            parsed
                .nodes
                .iter()
                .all(|n| n.encryption_key_path.as_deref() == Some("/etc/animus/encryption/key")),
            "{parsed:?}"
        );
    }

    #[tokio::test]
    async fn reconcile_sets_a_condition_when_encryption_key_secret_has_no_data_key() {
        use k8s_openapi::api::core::v1::Secret;

        let fake_cluster = FakeClusterApi::new();
        fake_cluster.seed_secret(
            "my-encryption-key",
            Secret {
                data: Some(BTreeMap::from([(
                    "wrong-key".to_string(),
                    k8s_openapi::ByteString(vec![0u8; 32]),
                )])),
                ..Default::default()
            },
        );
        let ctx = make_ctx(fake_cluster, FakeAdminClient::new());

        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.encryption_key_secret_name = Some("my-encryption-key".to_string());
        let result = reconcile(Arc::new(cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let status = ctx.cluster_api.last_status().unwrap();
        let condition = status
            .conditions
            .iter()
            .find(|c| c.type_ == CONDITION_ENCRYPTION_KEY_SECRET_INVALID)
            .expect("condition set");
        let message = condition.message.as_deref().unwrap_or_default();
        assert!(message.contains("my-encryption-key"), "{message}");
        assert!(
            message.contains(desired::cluster_config::ENCRYPTION_KEY_SECRET_DATA_KEY),
            "{message}"
        );
    }

    #[tokio::test]
    async fn reconcile_clears_the_encryption_key_condition_once_the_secret_is_fixed() {
        use k8s_openapi::ByteString;
        use k8s_openapi::api::core::v1::Secret;

        let fake_cluster = FakeClusterApi::new();
        let ctx = make_ctx(fake_cluster, FakeAdminClient::new());

        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.encryption_key_secret_name = Some("my-encryption-key".to_string());
        reconcile(Arc::new(cluster.clone()), Arc::clone(&ctx))
            .await
            .unwrap();
        assert!(
            ctx.cluster_api
                .last_status()
                .unwrap()
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_ENCRYPTION_KEY_SECRET_INVALID)
        );

        ctx.cluster_api.seed_secret(
            "my-encryption-key",
            Secret {
                data: Some(BTreeMap::from([(
                    desired::cluster_config::ENCRYPTION_KEY_SECRET_DATA_KEY.to_string(),
                    ByteString(vec![0u8; 32]),
                )])),
                ..Default::default()
            },
        );
        reconcile(Arc::new(cluster), Arc::clone(&ctx))
            .await
            .unwrap();
        assert!(
            !ctx.cluster_api
                .last_status()
                .unwrap()
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_ENCRYPTION_KEY_SECRET_INVALID)
        );
    }

    #[tokio::test]
    async fn reconcile_never_touches_encryption_key_when_secret_name_is_unset() {
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());
        let cluster = test_cluster("demo", "ns1", 3, None);
        let result = reconcile(Arc::new(cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        assert!(
            !ctx.cluster_api
                .last_status()
                .unwrap()
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_ENCRYPTION_KEY_SECRET_INVALID)
        );
        let parsed = parsed_cluster_config(&ctx.cluster_api, "demo");
        assert!(parsed.nodes.iter().all(|n| n.encryption_key_path.is_none()));
    }

    // --- (7) spec.s3 (S-04 PR 3) ------------------------------------------

    fn valid_s3() -> S3StoreSpec {
        S3StoreSpec {
            backup_store: Some(
                "s3://my-bucket/backups?endpoint=https://s3.example.com".to_string(),
            ),
            segment_store: None,
            credentials_secret_name: "my-s3-creds".to_string(),
            allow_insecure_http: false,
            egress_cidrs: S3StoreSpec::default_egress_cidrs(),
        }
    }

    #[tokio::test]
    async fn reconcile_applies_the_same_six_children_with_a_valid_spec_s3() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.s3 = Some(valid_s3());
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        let result = reconcile(Arc::new(cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        // No new child kind — spec.s3 only changes the content of the
        // pre-existing ConfigMap/StatefulSet/NetworkPolicy, never adds a
        // seventh applied object the way spec.tls.certManager's
        // Certificate does.
        assert_eq!(
            ctx.cluster_api.applies(),
            vec![
                (AppliedKind::ConfigMap, desired::config_map_name("demo")),
                (AppliedKind::Service, desired::internal_service_name("demo")),
                (AppliedKind::Service, desired::client_service_name("demo")),
                (
                    AppliedKind::NetworkPolicy,
                    desired::network_policy_name("demo")
                ),
                (
                    AppliedKind::PodDisruptionBudget,
                    desired::pod_disruption_budget_name("demo")
                ),
                (AppliedKind::StatefulSet, "demo".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn reconcile_with_spec_s3_mounts_the_secret_and_sets_the_flags() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.s3 = Some(valid_s3());
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        reconcile(Arc::new(cluster), Arc::clone(&ctx))
            .await
            .unwrap();

        // StatefulSet: the s3 Secret volume/mount is present.
        let sts = ctx
            .cluster_api
            .get_statefulset("ns1", "demo")
            .await
            .unwrap()
            .expect("statefulset applied");
        let pod_spec = sts.spec.unwrap().template.spec.unwrap();
        let vol = pod_spec
            .volumes
            .unwrap()
            .into_iter()
            .find(|v| v.name == "s3")
            .expect("s3 volume present");
        assert_eq!(
            vol.secret.unwrap().secret_name.as_deref(),
            Some("my-s3-creds")
        );

        // ConfigMap: the entrypoint script carries the flags and the
        // credentials-JSON-writing preamble, never a literal secret value.
        let cm = ctx
            .cluster_api
            .configmap(&desired::config_map_name("demo"))
            .expect("configmap applied");
        let script = cm
            .data
            .as_ref()
            .unwrap()
            .get(desired::cluster_config::ENTRYPOINT_FILE_NAME)
            .unwrap();
        assert!(script.contains("--s3-credentials /tmp/animus-s3-credentials.json"));
        assert!(
            script.contains(
                "--backup-store 's3://my-bucket/backups?endpoint=https://s3.example.com'"
            )
        );
        assert!(script.contains("$(cat /etc/animus/s3/access_key_id)"));
        assert!(!script.contains("--allow-insecure-s3"));

        // NetworkPolicy: the S3 egress rule (443, the default CIDR) is
        // present alongside the two baseline rules.
        let np = ctx
            .cluster_api
            .networkpolicy(&desired::network_policy_name("demo"))
            .expect("networkpolicy applied");
        let egress = np.spec.unwrap().egress.unwrap();
        assert_eq!(egress.len(), 3, "{egress:?}");
    }

    #[tokio::test]
    async fn reconcile_rejects_s3_spec_with_neither_store_set() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.s3 = Some(S3StoreSpec {
            backup_store: None,
            segment_store: None,
            ..valid_s3()
        });
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        let result = reconcile(Arc::new(cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_S3_SPEC_INVALID),
            "{:?}",
            status.conditions
        );
        // Reconciled as if spec.s3 were unset: no s3 volume mounted.
        let sts = ctx
            .cluster_api
            .get_statefulset("ns1", "demo")
            .await
            .unwrap()
            .expect("statefulset applied");
        let pod_spec = sts.spec.unwrap().template.spec.unwrap();
        assert!(!pod_spec.volumes.unwrap().iter().any(|v| v.name == "s3"));
    }

    #[tokio::test]
    async fn reconcile_rejects_s3_spec_with_empty_credentials_secret_name() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.s3 = Some(S3StoreSpec {
            credentials_secret_name: String::new(),
            ..valid_s3()
        });
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        reconcile(Arc::new(cluster), Arc::clone(&ctx))
            .await
            .unwrap();

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_S3_SPEC_INVALID)
        );
    }

    #[tokio::test]
    async fn reconcile_rejects_s3_spec_with_insecure_http_not_allowed() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.s3 = Some(S3StoreSpec {
            backup_store: Some(
                "s3://bucket?endpoint=http://minio.ns.svc:9000&insecure_http=true".to_string(),
            ),
            ..valid_s3()
        });
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        reconcile(Arc::new(cluster), Arc::clone(&ctx))
            .await
            .unwrap();

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_S3_SPEC_INVALID)
        );
    }

    #[tokio::test]
    async fn reconcile_rejects_s3_spec_with_malformed_store_uri() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.s3 = Some(S3StoreSpec {
            backup_store: Some("not-a-valid-uri".to_string()),
            ..valid_s3()
        });
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        reconcile(Arc::new(cluster), Arc::clone(&ctx))
            .await
            .unwrap();

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_S3_SPEC_INVALID)
        );
    }

    #[tokio::test]
    async fn reconcile_accepts_insecure_http_s3_spec_when_allowed() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.s3 = Some(S3StoreSpec {
            backup_store: Some(
                "s3://bucket?endpoint=http://minio.ns.svc:9000&insecure_http=true".to_string(),
            ),
            allow_insecure_http: true,
            ..valid_s3()
        });
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        reconcile(Arc::new(cluster), Arc::clone(&ctx))
            .await
            .unwrap();

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            !status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_S3_SPEC_INVALID),
            "{:?}",
            status.conditions
        );
        let cm = ctx
            .cluster_api
            .configmap(&desired::config_map_name("demo"))
            .expect("configmap applied");
        let script = cm
            .data
            .as_ref()
            .unwrap()
            .get(desired::cluster_config::ENTRYPOINT_FILE_NAME)
            .unwrap();
        assert!(script.contains("--allow-insecure-s3"));
    }

    #[tokio::test]
    async fn reconcile_without_spec_s3_still_applies_baseline_egress_and_no_s3_volume() {
        let cluster = test_cluster("demo", "ns1", 3, None);
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        reconcile(Arc::new(cluster), Arc::clone(&ctx))
            .await
            .unwrap();

        let np = ctx
            .cluster_api
            .networkpolicy(&desired::network_policy_name("demo"))
            .expect("networkpolicy applied");
        let egress = np.spec.unwrap().egress.unwrap();
        assert_eq!(egress.len(), 2, "baseline-only egress: intra + DNS");

        let sts = ctx
            .cluster_api
            .get_statefulset("ns1", "demo")
            .await
            .unwrap()
            .expect("statefulset applied");
        let pod_spec = sts.spec.unwrap().template.spec.unwrap();
        assert!(!pod_spec.volumes.unwrap().iter().any(|v| v.name == "s3"));
    }

    // --- (8) spec.backupStore / spec.segmentStore (S-07b) ------------------

    #[tokio::test]
    async fn reconcile_applies_the_same_six_children_with_a_valid_non_s3_store_spec() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.backup_store = Some("fs:/var/lib/animus/backups".to_string());
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        let result = reconcile(Arc::new(cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        assert_eq!(
            ctx.cluster_api.applies(),
            vec![
                (AppliedKind::ConfigMap, desired::config_map_name("demo")),
                (AppliedKind::Service, desired::internal_service_name("demo")),
                (AppliedKind::Service, desired::client_service_name("demo")),
                (
                    AppliedKind::NetworkPolicy,
                    desired::network_policy_name("demo")
                ),
                (
                    AppliedKind::PodDisruptionBudget,
                    desired::pod_disruption_budget_name("demo")
                ),
                (AppliedKind::StatefulSet, "demo".to_string()),
            ]
        );

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            !status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_STORE_SPEC_INVALID),
            "{:?}",
            status.conditions
        );

        let cm = ctx
            .cluster_api
            .configmap(&desired::config_map_name("demo"))
            .expect("configmap applied");
        let script = cm
            .data
            .as_ref()
            .unwrap()
            .get(desired::cluster_config::ENTRYPOINT_FILE_NAME)
            .unwrap();
        assert!(script.contains("--backup-store 'fs:/var/lib/animus/backups'"));
        // No S3 credentials machinery for a non-S3 backupStore.
        assert!(!script.contains("--s3-credentials"));
    }

    #[tokio::test]
    async fn reconcile_rejects_segment_store_cluster_literal() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.segment_store = Some("cluster".to_string());
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        reconcile(Arc::new(cluster), Arc::clone(&ctx))
            .await
            .unwrap();

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_STORE_SPEC_INVALID),
            "{:?}",
            status.conditions
        );
        // Reconciled as if segmentStore were unset: no --segment-store flag.
        let cm = ctx
            .cluster_api
            .configmap(&desired::config_map_name("demo"))
            .expect("configmap applied");
        let script = cm
            .data
            .as_ref()
            .unwrap()
            .get(desired::cluster_config::ENTRYPOINT_FILE_NAME)
            .unwrap();
        assert!(!script.contains("--segment-store"));
    }

    #[tokio::test]
    async fn reconcile_rejects_backup_store_path_outside_data_dir() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.backup_store = Some("fs:/tmp/backups".to_string());
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        reconcile(Arc::new(cluster), Arc::clone(&ctx))
            .await
            .unwrap();

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_STORE_SPEC_INVALID)
        );
    }

    #[tokio::test]
    async fn reconcile_rejects_backup_store_conflicting_with_spec_s3() {
        let mut cluster = test_cluster("demo", "ns1", 3, None);
        cluster.spec.backup_store = Some("cluster".to_string());
        cluster.spec.s3 = Some(valid_s3());
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        reconcile(Arc::new(cluster), Arc::clone(&ctx))
            .await
            .unwrap();

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_STORE_SPEC_INVALID),
            "{:?}",
            status.conditions
        );
    }

    #[tokio::test]
    async fn reconcile_without_store_spec_still_applies_no_store_flags() {
        let cluster = test_cluster("demo", "ns1", 3, None);
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        reconcile(Arc::new(cluster), Arc::clone(&ctx))
            .await
            .unwrap();

        let status = ctx.cluster_api.last_status().unwrap();
        assert!(
            !status
                .conditions
                .iter()
                .any(|c| c.type_ == CONDITION_STORE_SPEC_INVALID)
        );
        let cm = ctx
            .cluster_api
            .configmap(&desired::config_map_name("demo"))
            .expect("configmap applied");
        let script = cm
            .data
            .as_ref()
            .unwrap()
            .get(desired::cluster_config::ENTRYPOINT_FILE_NAME)
            .unwrap();
        assert!(!script.contains("--backup-store"));
        assert!(!script.contains("--segment-store"));
    }

    // --- (9) PodDisruptionBudget (S-07c) -----------------------------------

    #[tokio::test]
    async fn reconcile_applies_a_poddisruptionbudget_owned_and_selecting_the_clusters_pods() {
        let cluster = test_cluster("demo", "ns1", 3, None);
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        reconcile(Arc::new(cluster), Arc::clone(&ctx))
            .await
            .unwrap();

        let pdb = ctx
            .cluster_api
            .poddisruptionbudget(&desired::pod_disruption_budget_name("demo"))
            .expect("poddisruptionbudget applied");

        let owner = &pdb.metadata.owner_references.as_ref().unwrap()[0];
        assert_eq!(owner.name, "demo");
        assert!(owner.controller.unwrap_or(false));

        // The default 3-node/3-controlNode shape tolerates exactly 1.
        let pdb_spec = pdb.spec.clone().unwrap();
        assert_eq!(
            pdb_spec.max_unavailable,
            Some(IntOrString::Int(1)),
            "{pdb_spec:?}"
        );
        assert_eq!(pdb_spec.min_available, None);

        // Selector matches the *actual* StatefulSet builder's own pod
        // template labels, not a second, independent call to the same
        // label helper.
        let cluster2 = test_cluster("demo", "ns1", 3, None);
        let sts = desired::statefulset::build(&cluster2, &cluster2.spec);
        let pod_labels = sts.spec.unwrap().template.metadata.unwrap().labels.unwrap();
        let pdb_selector = pdb_spec.selector.unwrap().match_labels.unwrap();
        for (k, v) in &pdb_selector {
            assert_eq!(pod_labels.get(k), Some(v), "selector key {k:?} mismatch");
        }
    }

    #[tokio::test]
    async fn reconcile_applies_exactly_one_poddisruptionbudget_per_reconcile() {
        let cluster = Arc::new(test_cluster("demo", "ns1", 3, None));
        let ctx = make_ctx(FakeClusterApi::new(), FakeAdminClient::new());

        reconcile(Arc::clone(&cluster), Arc::clone(&ctx))
            .await
            .unwrap();
        reconcile(Arc::clone(&cluster), Arc::clone(&ctx))
            .await
            .unwrap();

        let pdb_applies: Vec<_> = ctx
            .cluster_api
            .applies()
            .into_iter()
            .filter(|(k, _)| *k == AppliedKind::PodDisruptionBudget)
            .collect();
        assert_eq!(
            pdb_applies.len(),
            2,
            "one per reconcile, re-applied each time"
        );
    }

    #[tokio::test]
    async fn reconcile_scale_down_recomputes_the_poddisruptionbudget_from_the_desired_spec() {
        // A previous reconcile scaled this cluster to 5 replicas (RF
        // plateaus at 3, so its own PDB would have carried
        // maxUnavailable=1). Scaling down to 2 nodes/2 controlNodes — below
        // MAX_REPLICATION_FACTOR — must recompute a *stricter* budget (0)
        // from the new desired spec, never leave the stale, unsafe value
        // the old (higher) node count would have implied.
        let fake_cluster = FakeClusterApi::new();
        fake_cluster.seed_statefulset("demo", 5, 5);
        let ctx = make_ctx(fake_cluster, FakeAdminClient::new());

        let cluster = Arc::new(test_cluster("demo", "ns1", 2, Some(2)));
        let result = reconcile(Arc::clone(&cluster), Arc::clone(&ctx)).await;
        assert!(result.is_ok(), "{:?}", result.err());

        let pdb = ctx
            .cluster_api
            .poddisruptionbudget(&desired::pod_disruption_budget_name("demo"))
            .expect("poddisruptionbudget applied");
        assert_eq!(
            pdb.spec.unwrap().max_unavailable,
            Some(IntOrString::Int(0)),
            "a 2-node/2-controlNode cluster must block every voluntary eviction, \
             not inherit the prior 5-node shape's budget"
        );
    }
}
