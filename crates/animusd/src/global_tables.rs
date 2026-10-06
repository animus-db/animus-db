//! The wire edge of **MRSC global tables** (ADR 0075 sections 3.5/3.7/5.1,
//! G-01 stage G-c, milestone M3): `UpdateTable` `ReplicaUpdates` ->
//! [`MetaCommand::ConvertTableToGlobal`], the `DescribeTable` global fields,
//! and the MRSC feature restrictions.
//!
//! Everything here is `E: Env`-generic client path (no wall clock, no raw
//! spawn: the `#[deny(clippy::disallowed_methods)]` on this module's
//! declaration in `lib.rs` enforces it), shared by the production
//! `run_operation` arm and the `SimCluster` `dispatch_table_op` arm so both
//! take the identical code.
//!
//! ## Order of checks in [`update_table_global`]
//!
//! 1. **The gate.** `Gate::GlobalTables` closed -> the pre-G-c rejection text,
//!    byte for byte ([`GlobalTableUpdate::closed_gate_error`]). This is the
//!    emit-site check ADR 0073 requires: the proposer must never append a
//!    command whose gate is closed (the relay receiver re-checks it,
//!    `version_wiring::relay_gate_verdict`).
//! 2. The table exists (`ResourceNotFoundException`).
//! 3. The request-shape rules (pure, `animus_dynamo::global`).
//! 4. The cluster-dependent rules: not already global, this node's own Region
//!    label, every named Region is carried by an `Active` member, the table is
//!    `ACTIVE`, has no TTL and no LSI, and is **empty**.
//!
//! The emptiness check (plan decision D6) is AWS fidelity, not safety: a
//! write racing the conversion is safe because Raft membership change moves
//! data anyway. It is deliberately *not* a replicated precondition.

use std::collections::BTreeSet;

use animus_control::schema::{GlobalTableSpec, MultiRegionConsistency};
use animus_control::version::Gate;
use animus_control::{IndexKind, MetaCommand, Metadata, NodeStatus};
use animus_dynamo::global::{GlobalTableDescription, GlobalTableUpdate, RegionStatus};
use animus_dynamo::wire::{self, WireError};
use animus_env::Env;
use animus_node::host::RelayClient;
use animus_placement::REGION_LABEL;

use crate::dynamo::{
    SCHEMA_COMMIT_TIMEOUT, SCHEMA_POLL_INTERVAL, describe_table_wrapped, internal, registry_error,
    table_status,
};
use crate::{ClientCtx, ReadConsistency};

/// Rows fetched per page by the emptiness scan.
const EMPTY_CHECK_PAGE: usize = 64;

/// The `DescribeTable` global fields for `table`, or `None` for a regional
/// (or unknown) table — whose output is therefore byte-identical to before.
///
/// Replica status is **derived** (plan decision D4), never stored: a Region is
/// `ACTIVE` once every routable tablet of the table has a replica of its
/// desired set there, `CREATING` until then. The local Region is included in
/// `Replicas` (AWS-style); the witness is listed apart.
#[must_use]
pub(crate) fn global_description(meta: &Metadata, table: &str) -> Option<GlobalTableDescription> {
    let spec = meta.table_global(table).filter(|g| g.is_mrsc())?;
    let ready = meta.table_ready_regions(table);
    let status = |region: &String| {
        if ready.contains(region) {
            RegionStatus::Active
        } else {
            RegionStatus::Creating
        }
    };
    Some(GlobalTableDescription {
        replicas: spec
            .regions
            .iter()
            .filter(|r| spec.witness.as_ref() != Some(*r))
            .map(|r| (r.clone(), status(r)))
            .collect(),
        witness: spec.witness.as_ref().map(|r| (r.clone(), status(r))),
    })
}

/// MRSC restriction (ADR 0075 V8/3.7): the transaction APIs error on a global
/// table. Called by `TransactWriteItems`/`TransactGetItems` (and through them
/// `ExecuteTransaction`) for every table they touch.
///
/// # Errors
/// A `ValidationException` naming the API and the table.
pub(crate) fn reject_transaction_on_global(
    meta: &Metadata,
    table: &str,
    api: &str,
) -> Result<(), WireError> {
    if meta.table_global(table).is_some_and(|g| g.is_mrsc()) {
        return Err(WireError::validation(format!(
            "{api} is not supported on table `{table}`: transactions are not supported on a \
             multi-Region strongly consistent global table"
        )));
    }
    Ok(())
}

/// MRSC restriction (ADR 0075 V8/3.7): TTL cannot be enabled on a global
/// table. (Disabling is the no-op it always was, so it stays allowed.)
///
/// # Errors
/// A `ValidationException` naming the table.
pub(crate) fn reject_ttl_on_global(meta: &Metadata, table: &str) -> Result<(), WireError> {
    if meta.table_global(table).is_some_and(|g| g.is_mrsc()) {
        return Err(WireError::validation(format!(
            "UpdateTimeToLive cannot enable TTL on table `{table}`: TTL is not supported on a \
             multi-Region strongly consistent global table"
        )));
    }
    Ok(())
}

/// Whether `table` holds at least one live item (a DynamoDB tombstone row is
/// not an item). A quorum scan over the whole table, page by page, stopping at
/// the first live row.
async fn table_has_items<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    table: &str,
) -> Result<bool, WireError> {
    let mut cursor: Vec<u8> = Vec::new();
    loop {
        let rows = ctx
            .cp_scan(
                table,
                cursor.clone(),
                None,
                Some(EMPTY_CHECK_PAGE),
                false,
                ReadConsistency::Strong,
            )
            .await
            .map_err(|e| internal(&format!("scanning table `{table}` for emptiness: {e}")))?;
        for (_, value) in &rows {
            if wire::decode_stored_item(value)?.is_some() {
                return Ok(true);
            }
        }
        if rows.len() < EMPTY_CHECK_PAGE {
            return Ok(false);
        }
        let mut next = rows.last().expect("a full page is non-empty").0.clone();
        next.push(0x00);
        cursor = next;
    }
}

/// `UpdateTable` with `ReplicaUpdates` (+ optional witness) and
/// `MultiRegionConsistency: STRONG`: convert an **empty** regional table into
/// a multi-Region strongly consistent global table (ADR 0075 section 3.5).
///
/// Proposes one [`MetaCommand::ConvertTableToGlobal`] (set the spec and pin
/// every tablet's placement in one apply), waits for it to be visible, and
/// returns the `TableDescription`. The preferred-leader Region is **the
/// receiving node's own Region** (plan decision D3: AWS has no wire field to
/// choose it; `SetGlobalPreferredLeader` re-points it later).
///
/// # Errors
/// The named `ValidationException`s listed in the module doc;
/// `ResourceNotFoundException` for an unknown table.
pub(crate) async fn update_table_global<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    table: &str,
    update: GlobalTableUpdate,
) -> Result<String, WireError> {
    // 1. The gate (the emit-site check; see the module doc). A closed verdict
    // first re-reads this node's applied view, since the feeder runs on a tick
    // and a finalize this node already applied may not have reached the handle.
    let features = &ctx.edge.version().features;
    if !features.is_open(Gate::GlobalTables) {
        features.update(&ctx.effective_metadata());
        if !features.is_open(Gate::GlobalTables) {
            return Err(update.closed_gate_error());
        }
    }

    // 2. The table.
    let meta = ctx.metadata_fresh().await;
    if !meta.has_table_schema(table) {
        return Err(registry_error(animus_dynamo::RegistryError::NoSuchTable(
            table.to_owned(),
        )));
    }

    // 3. The request shape.
    let request = update.validate()?;

    // 4. The cluster-dependent rules.
    if meta.table_global(table).is_some() {
        return Err(WireError::validation(format!(
            "UpdateTable: table `{table}` is already a global table; the Regions of a \
             multi-Region strongly consistent table are fixed at creation and cannot be changed"
        )));
    }
    let me = ctx.env.node_id();
    let Some(local) = meta
        .members
        .get(&me)
        .and_then(|m| m.labels.get(REGION_LABEL))
        .cloned()
    else {
        return Err(WireError::validation(format!(
            "UpdateTable: this node carries no `{REGION_LABEL}` label, so it has no Region to \
             be the table's own Region"
        )));
    };
    let regions = request.regions(&local);
    let distinct: BTreeSet<&String> = regions.iter().collect();
    if distinct.len() != regions.len() {
        return Err(WireError::validation(format!(
            "UpdateTable: the table's own Region `{local}` (the Region of the node serving this \
             request) must not be named in ReplicaUpdates or GlobalTableWitnessUpdates"
        )));
    }
    let known: BTreeSet<String> = meta
        .members
        .values()
        .filter(|m| m.status == NodeStatus::Active)
        .filter_map(|m| m.labels.get(REGION_LABEL).cloned())
        .collect();
    for region in &regions {
        if !known.contains(region) {
            return Err(WireError::validation(format!(
                "UpdateTable: Region `{region}` is not a Region of this cluster (no active \
                 member carries `{REGION_LABEL}={region}`; known Regions: {})",
                known.iter().cloned().collect::<Vec<_>>().join(", ")
            )));
        }
    }
    if table_status(&meta, table) != "ACTIVE" {
        return Err(WireError::validation(format!(
            "UpdateTable: table `{table}` is not ACTIVE"
        )));
    }
    if meta.table_ttl(table).is_some() {
        return Err(WireError::validation(format!(
            "UpdateTable: table `{table}` has TTL enabled; a multi-Region strongly consistent \
             table does not support TTL (disable it first)"
        )));
    }
    if meta
        .table_indexes(table)
        .iter()
        .any(|d| d.kind == IndexKind::Local)
    {
        return Err(WireError::validation(format!(
            "UpdateTable: table `{table}` has a LocalSecondaryIndex; a multi-Region strongly \
             consistent table does not support local secondary indexes"
        )));
    }
    if table_has_items(ctx, table).await? {
        return Err(WireError::validation(format!(
            "UpdateTable: table `{table}` must be empty to become a multi-Region strongly \
             consistent global table"
        )));
    }

    // The conversion.
    let spec = GlobalTableSpec {
        consistency: MultiRegionConsistency::Strong,
        regions,
        witness: request.witness,
        preferred_leader_region: local,
        replicas: Vec::new(),
    };
    spec.validate()
        .map_err(|e| WireError::validation(format!("UpdateTable: {}", e.message())))?;
    let deadline = ctx.env.now().saturating_add(SCHEMA_COMMIT_TIMEOUT);
    loop {
        ctx.propose_schema(&MetaCommand::ConvertTableToGlobal {
            table: table.to_owned(),
            spec: spec.clone(),
        })
        .await;
        let fresh = ctx.metadata_fresh().await;
        match fresh.table_global(table) {
            Some(g) if *g == spec => {
                return describe_table_wrapped(&fresh, table, "TableDescription");
            }
            // A concurrent conversion (a different request) won the race.
            Some(_) => {
                return Err(WireError::validation(format!(
                    "UpdateTable: table `{table}` is already a global table; the Regions of a \
                     multi-Region strongly consistent table are fixed at creation and cannot be \
                     changed"
                )));
            }
            None => {}
        }
        if ctx.env.now() >= deadline {
            return Err(internal(
                "UpdateTable (ReplicaUpdates) did not commit to the control plane in time (no \
                 leader reachable, or the cluster refused the conversion)",
            ));
        }
        ctx.env.sleep(SCHEMA_POLL_INTERVAL).await;
    }
}

// ---- operations: decommission guard, admin view, preferred-leader action ----

/// The decommission guard (plan decision D10): draining `node` would strand a
/// global table when `node` is the **last `Active` member of a Region** the
/// table pins. The strict region pin (D8) never re-places a replica into
/// another Region, so the replica `node` holds could never leave it and the
/// drain would stall forever. Returns the first `(region, table)` so
/// affected, `None` when the drain is safe (or `node` carries no Region
/// label, or no global table pins its Region).
#[must_use]
pub(crate) fn drain_strands_region(
    meta: &Metadata,
    node: &animus_env::NodeId,
) -> Option<(String, String)> {
    let region = meta.members.get(node)?.labels.get(REGION_LABEL)?.clone();
    let other_active = meta.members.iter().any(|(id, m)| {
        id != node && m.status == NodeStatus::Active && m.labels.get(REGION_LABEL) == Some(&region)
    });
    if other_active {
        return None;
    }
    meta.schemas
        .iter()
        .find(|(_, s)| {
            s.global
                .as_ref()
                .is_some_and(|g| g.regions.contains(&region))
        })
        .map(|(table, _)| (region, table.clone()))
}

/// The refusal text of the decommission guard.
#[must_use]
pub(crate) fn drain_strands_region_error(
    node: &animus_env::NodeId,
    region: &str,
    table: &str,
) -> String {
    format!(
        "node {node} is the last Active member of Region `{region}`, which the multi-Region \
         strongly consistent table `{table}` pins a replica to: a drain could never re-place \
         that replica (placement never moves a replica to another Region) and would stall; add \
         another node in `{region}` first, or drain anyway with `force` (`animus admin drain <addr> \
         <node> --force`)"
    )
}

/// `GET /admin/global-tables` (ADR 0075 section 8): every global table's
/// Regions, witness, preferred-leader Region, derived replica status and
/// per-tablet placement by Region, plus the cluster's Active members per
/// Region and the warnings an operator needs (a pinned Region with no Active
/// member, a control quorum a single Region's loss would break). Leader
/// identity is **node-local**: a tablet this node hosts reports its known
/// leader and whether that leader sits off the preferred Region; any other
/// tablet reports `null` (a fleet view fans out over `/admin/global-tables`
/// on every node, like `/admin/raftkv`).
#[must_use]
pub(crate) fn admin_global_tables_view<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
) -> serde_json::Value {
    use serde_json::json;
    let meta = ctx.effective_metadata();
    let region_of = |n: &animus_env::NodeId| -> Option<String> {
        meta.members.get(n)?.labels.get(REGION_LABEL).cloned()
    };
    let hosted: std::collections::BTreeMap<_, _> = ctx.edge.hosted_groups().into_iter().collect();

    let mut active_by_region: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for (id, m) in &meta.members {
        if let Some(r) = m.labels.get(REGION_LABEL) {
            let e = active_by_region.entry(r.clone()).or_default();
            if m.status == NodeStatus::Active {
                e.push(id.to_string());
            }
        }
    }

    let mut warnings: Vec<String> = Vec::new();
    let mut tables = Vec::new();
    for (name, schema) in meta.schemas.iter() {
        let Some(spec) = schema.global.as_ref().filter(|g| g.is_mrsc()) else {
            continue;
        };
        let ready = meta.table_ready_regions(name);
        let mut tablets = Vec::new();
        for (id, t) in meta
            .tablets_for_table(name)
            .filter(|(_, t)| t.is_routable())
        {
            let replicas: Vec<_> = t
                .replicas
                .iter()
                .map(|n| json!({"node": n.to_string(), "region": region_of(n)}))
                .collect();
            let leader = hosted.get(id).and_then(|g| g.leader());
            let leader_region = leader.as_ref().and_then(&region_of);
            tablets.push(json!({
                "tablet": id.0,
                "state": format!("{:?}", t.state),
                "replicas": replicas,
                "leader": leader.map(|l| l.to_string()),
                "leader_region": leader_region,
                "leader_off_preferred": leader_region
                    .as_ref()
                    .map(|r| *r != spec.preferred_leader_region),
            }));
        }
        for r in &spec.regions {
            if active_by_region.get(r).is_none_or(Vec::is_empty) {
                warnings.push(format!(
                    "table `{name}`: Region `{r}` has no Active member; its replicas wait for the \
                     Region to return (the strict region pin never repairs across Regions)"
                ));
            }
        }
        tables.push(json!({
            "table": name,
            "consistency": "STRONG",
            "regions": spec.regions,
            "witness": spec.witness,
            "preferred_leader_region": spec.preferred_leader_region,
            "replica_status": spec
                .regions
                .iter()
                .map(|r| (r.clone(), if ready.contains(r) { "ACTIVE" } else { "CREATING" }))
                .collect::<std::collections::BTreeMap<_, _>>(),
            "tablets": tablets,
        }));
    }

    // The control quorum: losing the Region that holds the most voters must
    // leave a majority (ADR 0075 section 3.4).
    if !tables.is_empty()
        && let Some(voters) = ctx.control.config()
        && voters.len() >= 3
    {
        let mut per: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
        for v in &voters {
            if let Some(r) = region_of(v) {
                *per.entry(r).or_default() += 1;
            }
        }
        let quorum = voters.len() / 2 + 1;
        for (r, k) in per {
            if voters.len() - k < quorum {
                warnings.push(format!(
                    "control plane: Region `{r}` holds {k} of {} control voters; losing it loses \
                     the control quorum (no DDL or placement changes until it returns)",
                    voters.len()
                ));
            }
        }
    }

    json!({
        "enabled": ctx.edge.version().features.is_open(Gate::GlobalTables),
        "tables": tables,
        "regions": active_by_region
            .into_iter()
            .map(|(r, n)| (r, json!({"active_members": n})))
            .collect::<std::collections::BTreeMap<_, _>>(),
        "warnings": warnings,
    })
}

/// `POST /admin/table/preferred-leader {table, region}` (plan decision D3):
/// re-point a global table's preferred-leader Region (`MetaCommand::
/// SetGlobalPreferredLeader`). Refused with the same reasons the apply
/// rejects (not global, Region not one of the table's, the witness Region),
/// but **before** proposing, so the operator gets a named error rather than a
/// bare `Rejected`. Relayed like any schema proposal (`propose_schema`), then
/// confirmed by observing the new preferred Region in replicated `Metadata`.
pub(crate) async fn admin_set_preferred_leader<E: Env, R: RelayClient>(
    ctx: &ClientCtx<E, R>,
    table: &str,
    region: &str,
) -> (u16, serde_json::Value) {
    use serde_json::json;
    if !ctx.edge.version().features.is_open(Gate::GlobalTables) {
        return (
            409,
            json!({"error": "global tables are not enabled: finalize the cluster version to 2 \
                (`animus cluster finalize`) first"}),
        );
    }
    let meta = ctx.metadata_fresh().await;
    let Some(spec) = meta.table_global(table).filter(|g| g.is_mrsc()) else {
        return (
            404,
            json!({"error": format!("table `{table}` is not a global table")}),
        );
    };
    if !spec.regions.iter().any(|r| r == region) {
        return (
            400,
            json!({"error": format!(
                "Region `{region}` is not one of table `{table}`'s Regions {:?}",
                spec.regions
            )}),
        );
    }
    if spec.witness.as_deref() == Some(region) {
        return (
            400,
            json!({"error": format!(
                "Region `{region}` is the witness of table `{table}`; a witness never leads"
            )}),
        );
    }
    if spec.preferred_leader_region == region {
        return (
            200,
            json!({"ok": true, "table": table, "preferred_leader_region": region, "changed": false}),
        );
    }
    let deadline = ctx.env.now().saturating_add(SCHEMA_COMMIT_TIMEOUT);
    loop {
        ctx.propose_schema(&MetaCommand::SetGlobalPreferredLeader {
            table: table.to_owned(),
            region: region.to_owned(),
        })
        .await;
        let fresh = ctx.metadata_fresh().await;
        if fresh
            .table_global(table)
            .is_some_and(|g| g.preferred_leader_region == region)
        {
            return (
                200,
                json!({"ok": true, "table": table, "preferred_leader_region": region, "changed": true}),
            );
        }
        if ctx.env.now() >= deadline {
            return (
                504,
                json!({"error": "SetGlobalPreferredLeader did not commit to the control plane in \
                    time (no leader reachable, or the cluster refused it)"}),
            );
        }
        ctx.env.sleep(SCHEMA_POLL_INTERVAL).await;
    }
}
